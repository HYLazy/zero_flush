//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M3.1: PartitionTable / PartitionTableSet / KeySampler 实现。

#include "zeroflush/partition_table.h"

#include <algorithm>
#include <cassert>

#include "util/hash.h"
#include "util/mutexlock.h"

namespace zeroflush {

// ---------------------------------------------------------------------------
// PartitionTable
// ---------------------------------------------------------------------------

PartitionTable::PartitionTable(uint32_t version, bool is_hash_mode,
                               std::vector<std::string> boundaries,
                               std::vector<uint32_t> part_ids,
                               const rocksdb::Comparator* ucmp)
    : version_(version),
      is_hash_mode_(is_hash_mode),
      boundaries_(std::move(boundaries)),
      part_ids_(std::move(part_ids)),
      ucmp_(ucmp) {}

rocksdb::Status PartitionTable::Create(
    uint32_t version, std::vector<std::string> boundaries,
    const rocksdb::Comparator* ucmp,
    std::shared_ptr<PartitionTable>* out) {
  if (ucmp == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "PartitionTable: ucmp must not be null");
  }
  const uint32_t part_count = static_cast<uint32_t>(boundaries.size()) + 1;
  // 空边界时也允许（等价于 1 个分区，范围 [-∞, +∞)）。
  if (part_count == 0) {
    return rocksdb::Status::InvalidArgument(
        "PartitionTable: at least 1 partition required");
  }
  // 校验升序与无重复。
  for (size_t i = 1; i < boundaries.size(); ++i) {
    const int cmp =
        ucmp->Compare(rocksdb::Slice(boundaries[i - 1]),
                      rocksdb::Slice(boundaries[i]));
    if (cmp > 0) {
      return rocksdb::Status::InvalidArgument(
          "PartitionTable: boundaries not in ascending order");
    }
    if (cmp == 0) {
      return rocksdb::Status::InvalidArgument(
          "PartitionTable: duplicate boundary key");
    }
  }
  // 构造 part_ids = [0, 1, ..., part_count-1]（初始连续；分裂后由调用方
  // 传入自定义 part_ids，见 M3_DESIGN.md §5.5）。
  std::vector<uint32_t> part_ids;
  part_ids.reserve(part_count);
  for (uint32_t i = 0; i < part_count; ++i) {
    part_ids.push_back(i);
  }
  *out = std::shared_ptr<PartitionTable>(
      new PartitionTable(version, /*is_hash_mode=*/false, std::move(boundaries),
                         std::move(part_ids), ucmp));
  return rocksdb::Status::OK();
}

std::shared_ptr<PartitionTable> PartitionTable::CreateHash(
    uint32_t version, uint32_t partitions) {
  // hash 模式下 boundaries 为空，part_ids 为 [0, P)。
  std::vector<uint32_t> part_ids;
  part_ids.reserve(partitions);
  for (uint32_t i = 0; i < partitions; ++i) {
    part_ids.push_back(i);
  }
  return std::shared_ptr<PartitionTable>(
      new PartitionTable(version, /*is_hash_mode=*/true,
                         /*boundaries=*/{}, std::move(part_ids),
                         /*ucmp=*/nullptr));
}

uint32_t PartitionTable::Route(const rocksdb::Slice& user_key) const {
  if (is_hash_mode_) {
    const uint32_t h =
        rocksdb::Hash(user_key.data(), user_key.size(), /*seed=*/0);
    return h % partitions();
  }
  // 范围模式：upper_bound(boundaries_, user_key, ucmp)
  // 找到第一个 > user_key 的 boundary。
  auto it = std::upper_bound(
      boundaries_.begin(), boundaries_.end(), user_key,
      [this](const rocksdb::Slice& key, const std::string& b) {
        return this->ucmp_->Compare(key, rocksdb::Slice(b)) < 0;
      });
  return static_cast<uint32_t>(it - boundaries_.begin());
}

void PartitionTable::RangeOf(uint32_t p, rocksdb::Slice* lo,
                             rocksdb::Slice* hi) const {
  assert(p < partitions());
  if (p == 0) {
    *lo = rocksdb::Slice();  // -∞
  } else {
    *lo = rocksdb::Slice(boundaries_[p - 1]);
  }
  if (p >= boundaries_.size()) {
    *hi = rocksdb::Slice();  // +∞
  } else {
    *hi = rocksdb::Slice(boundaries_[p]);
  }
}

// ---------------------------------------------------------------------------
// PartitionTableSet
// ---------------------------------------------------------------------------

std::shared_ptr<PartitionTable> PartitionTableSet::current() const {
  const uint32_t v = current_version_.load(std::memory_order_acquire);
  rocksdb::MutexLock l(&mu_);
  if (v < versions_.size()) {
    return versions_[v];
  }
  return nullptr;
}

std::shared_ptr<PartitionTable> PartitionTableSet::Get(
    uint32_t version) const {
  rocksdb::MutexLock l(&mu_);
  if (version < versions_.size()) {
    return versions_[version];
  }
  return nullptr;
}

rocksdb::Status PartitionTableSet::Init(
    std::shared_ptr<PartitionTable> t) {
  if (t == nullptr) {
    return rocksdb::Status::InvalidArgument(
        "PartitionTableSet: table must not be null");
  }
  rocksdb::MutexLock l(&mu_);
  // 只允许第一次注册。
  if (!versions_.empty()) {
    return rocksdb::Status::InvalidArgument(
        "PartitionTableSet: already initialized");
  }
  versions_.push_back(std::move(t));
  current_version_.store(0, std::memory_order_release);
  return rocksdb::Status::OK();
}

void PartitionTableSet::InstallNewVersion(
    std::shared_ptr<PartitionTable> t) {
  // 必须持 DB mutex 调用（不在本方法内加锁：调用方保证排他）。
  const uint32_t new_version = static_cast<uint32_t>(versions_.size());
  assert(t->version() == new_version);
  versions_.push_back(std::move(t));
  current_version_.store(new_version, std::memory_order_release);
}

// ---------------------------------------------------------------------------
// KeySampler
// ---------------------------------------------------------------------------

KeySampler::KeySampler(uint32_t sample_every_n,
                       const rocksdb::Comparator* ucmp)
    : sample_every_n_(sample_every_n),
      ucmp_(ucmp) {
  if (sample_every_n_ == 0) sample_every_n_ = 1;
  samples_.reserve(64 * 1024);
}

void KeySampler::Sample(const rocksdb::Slice& user_key) {
  // M4.6c：写路径 16 线程并发调用（AddRecord 锁外）——加锁防 vector
  // 数据竞争（析构崩溃，见 partition_table.h 注释）。
  rocksdb::MutexLock l(&mu_);
  ++total_seen_;
  // 步长控制：不是每第 N 条才采样可以减少存储压力。
  if ((total_seen_ - 1) % sample_every_n_ != 0) {
    return;
  }
  if (samples_.size() < max_samples_) {
    // 容量未满：直接加入。
    samples_.emplace_back(user_key.data(), user_key.size());
  } else {
    // 蓄水池替换：以 max_samples_ / total_seen_ 的概率替换已有样本。
    uint64_t r = static_cast<uint64_t>(std::rand()) * max_samples_ / RAND_MAX;
    if (r < max_samples_) {
      samples_[r].assign(user_key.data(), user_key.size());
    }
  }
}

bool KeySampler::BuildBoundaries(
    uint32_t partitions,
    std::vector<std::string>* boundaries) const {
  rocksdb::MutexLock l(&mu_);
  boundaries->clear();
  if (partitions <= 1) {
    // 单分区不需要边界。
    return true;
  }
  // 需要至少 partitions 个样本才能选出有意义的 P-1 个分位点。
  if (samples_.size() < partitions) {
    return false;
  }
  // 排序（按 ucmp）。
  std::vector<std::string> sorted = samples_;
  std::sort(sorted.begin(), sorted.end(),
            [this](const std::string& a, const std::string& b) {
              return this->ucmp_->Compare(rocksdb::Slice(a),
                                          rocksdb::Slice(b)) < 0;
            });
  // 选出 P-1 个分位点：索引 = (i+1) * samples / partitions, i ∈ [0, P-2)。
  boundaries->reserve(partitions - 1);
  for (uint32_t i = 0; i < partitions - 1; ++i) {
    const size_t idx =
        static_cast<size_t>((i + 1) * sorted.size() / partitions);
    boundaries->push_back(sorted[idx]);
  }
  // 保证升序与无重复（QED：取的是已排序数组的分位点，自然升序、无重复
  // 当且仅当 sorted 中有重复键且分位点恰好命中了它们。
  // 若有重复，由 M3.1 的 PartitionTable::Create 校验拒绝，采样器不负责去重）。
  return true;
}

void KeySampler::Clear() {
  rocksdb::MutexLock l(&mu_);
  samples_.clear();
  total_seen_ = 0;
}

bool KeySampler::empty() const {
  rocksdb::MutexLock l(&mu_);
  return samples_.empty();
}

uint64_t KeySampler::total_seen() const {
  rocksdb::MutexLock l(&mu_);
  return total_seen_;
}

size_t KeySampler::sample_count() const {
  rocksdb::MutexLock l(&mu_);
  return samples_.size();
}

}  // namespace zeroflush