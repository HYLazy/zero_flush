//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M2: 封存代文件缓存实现。

#include "zeroflush/sealed_file_cache.h"

#include <algorithm>

#include "util/mutexlock.h"

namespace zeroflush {

SealedFileCache::SealedFileCache(rocksdb::Env* env, std::string dir,
                                 uint32_t capacity, bool reclaim_enabled)
    : env_(env),
      dir_(std::move(dir)),
      capacity_(capacity),
      reclaim_enabled_(reclaim_enabled) {}

SealedFileCache::~SealedFileCache() {
  // 析构时若还有未释放 epoch 不报错（DB 异常关闭路径）。
  // 不主动 unlink pending_unlink_（宿主可能仍持引用——POSIX 安全）。
}

std::string SealedFileCache::FileName(uint32_t part, uint32_t gen) const {
  return dir_ + "/zf-wal-" + std::to_string(part) + "-" +
         std::to_string(gen) + ".log";
}

void SealedFileCache::AddEpochWithRecoveryAdoption(const SealedEpoch& e,
                                                   uint32_t refcount) {
  rocksdb::MutexLock l(&mu_);
  auto it = epochs_.find(e.epoch);
  if (it != epochs_.end()) {
    // 重复登记（不应发生）——保持首次记录。
    return;
  }
  SealedEpoch merged = e;
  if (!recovery_gens_.empty()) {
    // M3.0 R1：同一持锁窗口内收养恢复期孤儿代并登记 epoch，保证读路径的
    // in_epoch 校验在收养期间恒成立（M3_DESIGN.md §8.1）。
    merged.gens.reserve(merged.gens.size() + recovery_gens_.size());
    for (ZfFileKey k : recovery_gens_) {
      merged.gens.emplace_back(static_cast<uint32_t>(k >> 32),
                               static_cast<uint32_t>(k));
    }
    merged.total_bytes += recovery_bytes_;
    merged.has_adopted_orphans = true;
    // M4.5b：合并 recovery 的 per-partition 字节（攒批后融合 ratio
    // 计算含全部待物化代）。
    for (const auto& [part, bytes] : recovery_part_bytes_) {
      merged.part_bytes[part] += bytes;
    }
    recovery_gens_.clear();
    recovery_bytes_ = 0;
    recovery_part_bytes_.clear();
  }
  merged.sealed_at_micros = env_->NowMicros();
  epochs_.emplace(merged.epoch, merged);
  // M3.4：多列族共享同一物理分区文件时 refcount = CF 个数。
  refs_[merged.epoch] = refcount;
  sealed_bytes_ += merged.total_bytes;
}

void SealedFileCache::AddRecoveryGens(
    const std::vector<std::pair<uint32_t, uint32_t>>& gens,
    uint64_t total_bytes) {
  rocksdb::MutexLock l(&mu_);
  for (const auto& [part, gen] : gens) {
    recovery_gens_.emplace(MakeFileKey(part, gen));
  }
  recovery_bytes_ += total_bytes;
}

void SealedFileCache::HandOffSkippedToRecovery(
    uint64_t epoch, const std::vector<std::pair<uint32_t, uint32_t>>& gens,
    const std::unordered_map<uint32_t, uint64_t>& part_bytes) {
  rocksdb::MutexLock l(&mu_);
  // 1) 从 epoch 移除跳过的 gens（ReleaseEpoch 不再 unlink 它们）。
  auto eit = epochs_.find(epoch);
  if (eit != epochs_.end()) {
    auto& egens = eit->second.gens;
    egens.erase(
        std::remove_if(egens.begin(), egens.end(),
                       [&gens](const std::pair<uint32_t, uint32_t>& g) {
                         return std::find(gens.begin(), gens.end(), g) !=
                                gens.end();
                       }),
        egens.end());
    for (const auto& [part, gen] : gens) {
      auto pb = eit->second.part_bytes.find(part);
      if (pb != eit->second.part_bytes.end()) {
        eit->second.total_bytes -= pb->second;
        eit->second.part_bytes.erase(pb);
      }
    }
  }
  // 2) 移交 recovery（可读、不回收；per-part 字节供收养时 ratio 合并）。
  for (const auto& [part, gen] : gens) {
    recovery_gens_.emplace(MakeFileKey(part, gen));
  }
  for (const auto& [part, bytes] : part_bytes) {
    recovery_part_bytes_[part] += bytes;
    recovery_bytes_ += bytes;
  }
}

uint64_t SealedFileCache::ReleaseEpoch(uint64_t epoch) {
  uint64_t released_bytes = 0;
  std::vector<std::string> to_unlink;
  {
    rocksdb::MutexLock l(&mu_);
    auto it = refs_.find(epoch);
    if (it == refs_.end()) {
      return 0;
    }
    if (--(it->second) > 0) {
      return 0;
    }
    refs_.erase(it);
    auto eit = epochs_.find(epoch);
    if (eit == epochs_.end()) {
      return 0;
    }
    released_bytes = eit->second.total_bytes;
    // M3.0：引用归零 = 该 epoch 已物化完成（old mem 已析构）。累计耗时
    // 与物化计数；入队 epoch 数供回收统计（reclaim_enabled_ 才真实 unlink）。
    ++materialized_epochs_;
    materialize_micros_total_ +=
        env_->NowMicros() - eit->second.sealed_at_micros;
    if (reclaim_enabled_) {
      ++pending_epochs_;
      to_unlink.reserve(eit->second.gens.size());
      for (const auto& [part, gen] : eit->second.gens) {
        to_unlink.push_back(FileName(part, gen));
      }
    }
    sealed_bytes_ -= eit->second.total_bytes;
    epochs_.erase(eit);
  }
  // 锁外追加到 pending_unlink_（避免持锁做 IO）。
  if (!to_unlink.empty()) {
    rocksdb::MutexLock l(&mu_);
    for (auto& name : to_unlink) {
      pending_unlink_.push_back(std::move(name));
    }
  }
  return released_bytes;
}

rocksdb::Status SealedFileCache::Get(
    uint32_t part, uint32_t gen,
    std::shared_ptr<rocksdb::RandomAccessFile>* out) {
  const ZfFileKey key = MakeFileKey(part, gen);
  rocksdb::MutexLock l(&mu_);
  // 校验该 gen 仍处于某个 epoch 中（未进入 pending_unlink），或属于
  // 恢复期孤儿代集合（M3.0 R1：Recover 到首次 Seal 之间的读窗口，
  // M3_DESIGN.md §8.1）。
  bool valid = recovery_gens_.count(key) == 1;
  if (!valid) {
    for (const auto& [e, se] : epochs_) {
      (void)e;
      for (const auto& [p, g] : se.gens) {
        if (p == part && g == gen) {
          valid = true;
          break;
        }
      }
      if (valid) break;
    }
  }
  if (!valid) {
    return rocksdb::Status::NotFound("ZF sealed gen not in any active epoch");
  }
  sealed_read_count_.fetch_add(1, std::memory_order_relaxed);
  auto it = handles_.find(key);
  if (it != handles_.end()) {
    *out = it->second;
    TouchLRU(key);
    return rocksdb::Status::OK();
  }
  // 未命中：按需打开。
  sealed_cache_miss_.fetch_add(1, std::memory_order_relaxed);
  rocksdb::EnvOptions opts;
  std::unique_ptr<rocksdb::RandomAccessFile> rf;
  rocksdb::Status s = env_->NewRandomAccessFile(FileName(part, gen), &rf, opts);
  if (!s.ok()) {
    return s;
  }
  auto sp = std::shared_ptr<rocksdb::RandomAccessFile>(std::move(rf));
  handles_.emplace(key, sp);
  lru_order_.push_front(key);
  // 容量超限：淘汰 LRU 末尾（注意：仍保留在 handles_，下次 Get 会再次打开，
  // 此为简化——LRU 仅用于控制本缓存内存，不主动 close）。
  while (lru_order_.size() > capacity_) {
    ZfFileKey evict = lru_order_.back();
    lru_order_.pop_back();
    handles_.erase(evict);
  }
  *out = sp;
  return rocksdb::Status::OK();
}

void SealedFileCache::TouchLRU(ZfFileKey key) {
  auto it = std::find(lru_order_.begin(), lru_order_.end(), key);
  if (it != lru_order_.end()) {
    lru_order_.erase(it);
  }
  lru_order_.push_front(key);
}

bool SealedFileCache::GetEpoch(uint64_t epoch, SealedEpoch* out) const {
  rocksdb::MutexLock l(&mu_);
  auto it = epochs_.find(epoch);
  if (it == epochs_.end()) {
    return false;
  }
  if (out != nullptr) {
    *out = it->second;
  }
  return true;
}

size_t SealedFileCache::PurgePending() {
  std::vector<std::string> to_unlink;
  {
    rocksdb::MutexLock l(&mu_);
    to_unlink.swap(pending_unlink_);
    // M3.0：真实 unlink 的 epoch 计数（pending_epochs_ 在 ReleaseEpoch
    // 入队时累计）。
    reclaimed_epochs_ += pending_epochs_;
    pending_epochs_ = 0;
  }
  for (const auto& name : to_unlink) {
    env_->DeleteFile(name).PermitUncheckedError();
  }
  return to_unlink.size();
}

uint64_t SealedFileCache::sealed_bytes() const {
  rocksdb::MutexLock l(&mu_);
  return sealed_bytes_;
}

uint64_t SealedFileCache::pending_count() const {
  rocksdb::MutexLock l(&mu_);
  return pending_unlink_.size();
}

size_t SealedFileCache::handle_count() const {
  rocksdb::MutexLock l(&mu_);
  return handles_.size();
}

uint64_t SealedFileCache::sealed_read_count() const {
  return sealed_read_count_.load(std::memory_order_relaxed);
}

uint64_t SealedFileCache::sealed_cache_miss() const {
  return sealed_cache_miss_.load(std::memory_order_relaxed);
}

uint64_t SealedFileCache::materialized_epochs() const {
  rocksdb::MutexLock l(&mu_);
  return materialized_epochs_;
}

uint64_t SealedFileCache::materialize_micros() const {
  rocksdb::MutexLock l(&mu_);
  return materialize_micros_total_;
}

uint64_t SealedFileCache::reclaimed_epochs() const {
  rocksdb::MutexLock l(&mu_);
  return reclaimed_epochs_;
}

size_t SealedFileCache::recovery_count() const {
  rocksdb::MutexLock l(&mu_);
  return recovery_gens_.size();
}

}  // namespace zeroflush
