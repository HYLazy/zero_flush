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
  if (!skip_gens_.empty()) {
    // M4.5b：收养 kSkip 跳过代（攒批）——并入 gens 供多代合并物化、
    // 并入 part_bytes 供融合 ratio 计算。seq 连续（未崩溃）→ 置
    // has_adopted_skips（物化可融合），区别于 has_adopted_orphans 的
    // 保守语义（崩溃孤儿 seq 可能交错）。
    merged.gens.reserve(merged.gens.size() + skip_gens_.size());
    for (ZfFileKey k : skip_gens_) {
      merged.gens.emplace_back(static_cast<uint32_t>(k >> 32),
                               static_cast<uint32_t>(k));
    }
    merged.total_bytes += skip_bytes_;
    merged.has_adopted_skips = true;
    for (const auto& [part, bytes] : skip_part_bytes_) {
      merged.part_bytes[part] += bytes;
    }
    skip_gens_.clear();
    skip_bytes_ = 0;
    skip_part_bytes_.clear();
  }
  merged.sealed_at_micros = env_->NowMicros();
  epochs_.emplace(merged.epoch, merged);
  // M4.8 回收分区化：引用计数从 epoch 粒度改为 (part, gen) 粒度——
  // 每个 WAL 段一引（M3.4 多列族共享时 refcount = CF 个数），可独立
  // 回收；epoch 引用 = 其全部 gens 引用之和（全空 = 完全回收）。
  auto& grefs = gen_refs_[merged.epoch];
  for (const auto& [part, gen] : merged.gens) {
    grefs[MakeFileKey(part, gen)] = refcount;
  }
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
  // 2) 移交 skip 集合（可读、不回收；per-part 字节供收养时 ratio 合并）。
  for (const auto& [part, gen] : gens) {
    skip_gens_.emplace(MakeFileKey(part, gen));
  }
  for (const auto& [part, bytes] : part_bytes) {
    skip_part_bytes_[part] += bytes;
    skip_bytes_ += bytes;
  }
  // M4.8 回收分区化：3) 同步移除 gen_refs_ 中跳过的 gens 引用——跳过代
  // 由 recovery 集合托管（可读、不回收），不再参与该 epoch 的引用计数
  // （否则 epoch 永不"完全回收"，物化统计停滞——R48 实测 sealed=42
  // materialized=41）。若 epoch 的全部 gens 都被跳过，引用集清空 →
  // epoch 完全回收（统计/封存字节结算，与 ReleaseGens 收尾一致）。
  auto git = gen_refs_.find(epoch);
  if (git != gen_refs_.end()) {
    for (const auto& [part, gen] : gens) {
      git->second.erase(MakeFileKey(part, gen));
    }
    if (git->second.empty()) {
      gen_refs_.erase(git);
      auto eit2 = epochs_.find(epoch);
      if (eit2 != epochs_.end()) {
        ++materialized_epochs_;
        materialize_micros_total_ +=
            env_->NowMicros() - eit2->second.sealed_at_micros;
        if (reclaim_enabled_) {
          ++pending_epochs_;
        }
        sealed_bytes_ -= eit2->second.total_bytes;
        epochs_.erase(eit2);
      }
    }
  }
}

uint64_t SealedFileCache::ReleaseEpoch(uint64_t epoch) {
  // M4.8：epoch 级入口 = 释放该 epoch 全部 gens（per-gen 独立回收）。
  // 单 flush 线程下 epoch 物化完成即全量释放，语义与迁移前一致。
  std::vector<std::pair<uint32_t, uint32_t>> gens;
  {
    rocksdb::MutexLock l(&mu_);
    auto eit = epochs_.find(epoch);
    if (eit != epochs_.end()) {
      gens = eit->second.gens;
    }
  }
  return ReleaseGens(epoch, gens);
}

uint64_t SealedFileCache::ReleaseGens(
    uint64_t epoch, const std::vector<std::pair<uint32_t, uint32_t>>& gens) {
  if (gens.empty()) {
    return 0;
  }
  uint64_t released_bytes = 0;
  std::vector<std::string> to_unlink;
  {
    rocksdb::MutexLock l(&mu_);
    auto git = gen_refs_.find(epoch);
    if (git == gen_refs_.end()) {
      return 0;  // 未登记或已完全回收
    }
    auto& grefs = git->second;
    auto eit = epochs_.find(epoch);
    for (const auto& [part, gen] : gens) {
      const ZfFileKey key = MakeFileKey(part, gen);
      auto rit = grefs.find(key);
      if (rit == grefs.end()) {
        continue;  // 已释放（幂等）
      }
      if (--(rit->second) > 0) {
        continue;  // 仍有其他 CF 引用，不回收
      }
      grefs.erase(rit);
      if (reclaim_enabled_) {
        to_unlink.push_back(FileName(part, gen));
      }
      // 从 epoch 登记中移除该 gen（Get 校验随之失效——文件已入
      // pending_unlink_）。part_bytes/total_bytes 保持（epoch 完全回收
      // 时统一扣减；中间态仅影响统计口径，不影响 Get 校验与物化决策
      // ——该 epoch 剩余 gens 由调用方保证已物化或正被物化）。
      if (eit != epochs_.end()) {
        auto& egens = eit->second.gens;
        egens.erase(
            std::remove(egens.begin(), egens.end(), std::make_pair(part, gen)),
            egens.end());
      }
    }
    if (grefs.empty()) {
      // M4.8：epoch 全部 gens 引用归零 = 完全回收。物化耗时/计数与
      // 封存字节在此时统一结算（与迁移前的 epoch 级语义一致）。
      gen_refs_.erase(git);
      if (eit != epochs_.end()) {
        released_bytes = eit->second.total_bytes;
        ++materialized_epochs_;
        materialize_micros_total_ +=
            env_->NowMicros() - eit->second.sealed_at_micros;
        if (reclaim_enabled_) {
          ++pending_epochs_;
        }
        sealed_bytes_ -= eit->second.total_bytes;
        epochs_.erase(eit);
      }
    }
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
  // M3_DESIGN.md §8.1），或属于 kSkip 攒批跳过代（M4.5b：跳过期间
  // 数据留在封存 WAL，Get/迭代器经 locator 定点读仍需可读）。
  bool valid = recovery_gens_.count(key) == 1 || skip_gens_.count(key) == 1;
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

uint64_t SealedFileCache::skipped_bytes() const {
  rocksdb::MutexLock l(&mu_);
  return skip_bytes_;
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
