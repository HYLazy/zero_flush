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
    static std::atomic<uint64_t> zf_adopt_dbg {0};
    if (zf_adopt_dbg.fetch_add(1, std::memory_order_relaxed) < 100) {
      fprintf(stderr, "ZFDBG-adopt skip_gens=%zu epoch=%llu\n",
              skip_gens_.size(), (unsigned long long)e.epoch);
    }
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

bool SealedFileCache::OldestPendingGen(uint32_t part, uint32_t* out) const {
  rocksdb::MutexLock l(&mu_);
  bool found = false;
  uint32_t best = 0;
  auto consider = [&](uint32_t p, uint32_t g) {
    if (p != part) {
      return;
    }
    auto git = gens_.find(MakeFileKey(p, g));
    if (git != gens_.end() && git->second.installed) {
      return;  // 已安装：不参与 FIFO（其数据已在 SST）
    }
    if (!found || g < best) {
      best = g;
      found = true;
    }
  };
  for (const auto& [ep, se] : epochs_) {
    (void)ep;
    for (const auto& [p, g] : se.gens) {
      consider(p, g);
    }
  }
  for (ZfFileKey k : skip_gens_) {
    consider(static_cast<uint32_t>(k >> 32), static_cast<uint32_t>(k));
  }
  for (ZfFileKey k : recovery_gens_) {
    consider(static_cast<uint32_t>(k >> 32), static_cast<uint32_t>(k));
  }
  if (found && out != nullptr) {
    *out = best;
  }
  return found;
}

void SealedFileCache::RegisterGen(uint32_t part, uint32_t gen, uint64_t bytes) {
  if (bytes == 0) {
    return;
  }
  rocksdb::MutexLock l(&mu_);
  const ZfFileKey key = MakeFileKey(part, gen);
  if (gens_.find(key) != gens_.end()) {
    return;  // 同一 (part,gen) 只注册一次（收养/重复封存路径不重复计账）
  }
  gens_[key] = GenInfo{bytes, false};
  const uint64_t now = pending_bytes_.fetch_add(bytes, std::memory_order_relaxed) + bytes;
  if (now > peak_pending_bytes_) {
    peak_pending_bytes_ = now;
  }
}

namespace {
}  // namespace

bool SealedFileCache::TryAcquireGenReadRef(uint32_t part, uint32_t gen) {
  rocksdb::MutexLock l(&mu_);
  const ZfFileKey key = MakeFileKey(part, gen);
  // retiring 中（回收已入队）⇒ 拒绝新引用：调用方必须重新选择。
  if (retiring_gens_.count(key) > 0) {
    acquire_rejected_retiring_.fetch_add(1, std::memory_order_relaxed);
    return false;  // 断言 retiring => no_new_ref
  }
  ++gen_read_refs_[key];
  gen_read_ref_total_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

bool SealedFileCache::GenRetiring(uint32_t part, uint32_t gen) const {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  return retiring_gens_.count(MakeFileKey(part, gen)) > 0;
}

bool SealedFileCache::GenRegistered(uint32_t part, uint32_t gen) const {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  return gens_.find(MakeFileKey(part, gen)) != gens_.end();
}

bool SealedFileCache::GenInstalledPublished(uint32_t part, uint32_t gen) const {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  auto it = gens_.find(MakeFileKey(part, gen));
  return it != gens_.end() && it->second.installed;
}

void SealedFileCache::ReleaseGenReadRef(uint32_t part, uint32_t gen) {
  std::vector<UnlinkRequest> to_unlink;
  {
    rocksdb::MutexLock l(&mu_);
    auto it = gen_read_refs_.find(MakeFileKey(part, gen));
    if (it == gen_read_refs_.end() || it->second == 0) {
      return;
    }
    if (--(it->second) == 0) {
      gen_read_refs_.erase(it);
      gen_read_ref_total_.fetch_sub(1, std::memory_order_relaxed);
      // 最后一个读引用释放：若该代已安装（数据在 LSM）且回收开启 ⇒ **标记 retiring**
      // 并此刻入队 unlink（此后新引用被拒）。只处理本 (part,gen) 的 pending。
      const ZfFileKey key = MakeFileKey(part, gen);
      auto pit = reclaim_pending_.find(key);
      const bool had_pending = (pit != reclaim_pending_.end());
      const bool pend_enabled = had_pending ? pit->second.reclaim_enabled : reclaim_enabled_;
      auto git = gens_.find(key);
      const bool inst = (git != gens_.end() && git->second.installed) ||
                        (had_pending && pit->second.installed);
      if (inst && pend_enabled) {
        EnqueueUnlinkLocked(part, gen, &to_unlink);
        if (had_pending) {
          reclaim_pending_.erase(pit);  // 已处理，仅限该 (part,gen)
        }
      } else if (had_pending) {
        // 回收失败/条件不足：**保留 pending 状态**（不丢记录），下次释放或再次请求时处理。
        reclaim_pending_[key].installed = inst;
      }
    } else {
      gen_read_ref_total_.fetch_sub(1, std::memory_order_relaxed);
    }
  }
  if (!to_unlink.empty()) {
    rocksdb::MutexLock l(&mu_);
    for (auto& req : to_unlink) {
      pending_unlink_.push_back(std::move(req));
    }
  }
}

uint64_t SealedFileCache::reclaim_pending_count() const {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  return reclaim_pending_.size();
}

size_t SealedFileCache::retiring_count() const {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  return retiring_gens_.size();
}

// D-38 诊断：列出**已注册但未安装**的代及其归属（epoch / recovery / skip），
// 用于回答"pending 为何不为 0"：哪些 (part,gen) 卡住、字节多少、归谁管。
bool SealedFileCache::GenReclaimedByFlow(uint32_t part, uint32_t gen) const {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  return reclaimed_by_flow_.count(MakeFileKey(part, gen)) > 0;
}

uint64_t SealedFileCache::UninstalledBytesOf(uint32_t part) const {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  uint64_t sum = 0;
  for (const auto& [key, info] : gens_) {
    if (info.installed) continue;
    if (static_cast<uint32_t>(key >> 32) != part) continue;
    sum += info.bytes;
  }
  return sum;
}

void SealedFileCache::ForEachRegisteredGen(
    const std::function<void(uint32_t, uint32_t, bool)>& fn) const {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  for (const auto& [key, info] : gens_) {
    fn(static_cast<uint32_t>(key >> 32), static_cast<uint32_t>(key),
       info.installed);
  }
}

std::string SealedFileCache::DescribeUninstalledGens() const {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  std::string out;
  uint64_t bytes = 0;
  size_t n = 0;
  for (const auto& [key, info] : gens_) {
    if (info.installed) {
      continue;
    }
    ++n;
    bytes += info.bytes;
    const uint32_t kp = static_cast<uint32_t>(key >> 32);
    const uint32_t kg = static_cast<uint32_t>(key);
    std::string owner = "NONE";
    for (const auto& [ep, se] : epochs_) {
      for (const auto& [pp, gg] : se.gens) {
        if (pp == kp && gg == kg) {
          owner = "epoch=" + std::to_string(ep);
        }
      }
    }
    if (recovery_gens_.count(key) > 0) {
      owner += "+recovery";
    }
    if (skip_gens_.count(key) > 0) {
      owner += "+skip";
    }
    if (gen_read_refs_.count(key) > 0) {
      owner += "+readrefs=" + std::to_string(gen_read_refs_.at(key));
    }
    out += std::to_string(kp) + ":" + std::to_string(kg) + "(" +
           std::to_string(info.bytes) + "B," + owner + ") ";
    if (n >= 24) {
      out += "...";
      break;
    }
  }
  return "n=" + std::to_string(n) + " bytes=" + std::to_string(bytes) + " [" +
         out + "]";
}

std::string SealedFileCache::DescribeReclaimPending() const {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  std::string out;
  for (const auto& [key, rp] : reclaim_pending_) {
    out += std::to_string(rp.part) + ":" + std::to_string(rp.gen) + "(inst=" +
           (rp.installed ? "1" : "0") + ",en=" +
           (rp.reclaim_enabled ? "1" : "0") + ",ep=" +
           std::to_string(rp.request_epoch) + " refs=" +
           std::to_string(gen_read_refs_.count(key) > 0
                              ? gen_read_refs_.at(key)
                              : 0) +
           (retiring_gens_.count(key) ? ",retiring=1" : "") + ") ";
  }
  return out;
}

uint64_t SealedFileCache::GenReadRefs(uint32_t part, uint32_t gen) const {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  auto it = gen_read_refs_.find(MakeFileKey(part, gen));
  return it == gen_read_refs_.end() ? 0 : it->second;
}

void SealedFileCache::MarkGensInstalled(
    const std::vector<std::pair<uint32_t, uint32_t>>& gens) {
  rocksdb::MutexLock l(&mu_);
  uint64_t freed = 0;
  for (const auto& [part, gen] : gens) {
    auto it = gens_.find(MakeFileKey(part, gen));
    if (it == gens_.end() || it->second.installed) {
      continue;  // 幂等：未登记或已安装
    }
    it->second.installed = true;
    freed += it->second.bytes;
  }
  if (freed > 0) {
    pending_bytes_.fetch_sub(freed, std::memory_order_relaxed);
    drained_bytes_ += freed;
    ++installed_epochs_;
  }
}

void SealedFileCache::MarkEpochInstalled(uint64_t epoch) {
  rocksdb::MutexLock l(&mu_);
  auto eit = epochs_.find(epoch);
  if (eit == epochs_.end()) {
    return;
  }
  uint64_t freed = 0;
  for (const auto& [part, gen] : eit->second.gens) {
    auto it = gens_.find(MakeFileKey(part, gen));
    if (it == gens_.end() || it->second.installed) {
      continue;
    }
    it->second.installed = true;
    freed += it->second.bytes;
  }
  if (freed > 0) {
    pending_bytes_.fetch_sub(freed, std::memory_order_relaxed);
    drained_bytes_ += freed;
    ++installed_epochs_;
  }
}

// step V0.6-A12（用户口径）：逐代回收分类——installed / epoch 归属 / 读引用数 /
// 是否已请求回收 / retiring / 是否在 purge 队列 / 文件是否存在。
// 只读诊断：不 unlink、不清 registry。
std::string SealedFileCache::DescribeGen(uint32_t part, uint32_t gen) const {
  rocksdb::MutexLock l(&mu_);
  const ZfFileKey k = MakeFileKey(part, gen);
  std::string out;
  // registry（唯一权威）
  auto git = gens_.find(k);
  if (git == gens_.end()) {
    out += "registry=absent; ";
  } else {
    out += "registry=present installed=" +
           std::string(git->second.installed ? "1" : "0") + " bytes=" +
           std::to_string(git->second.bytes) + "; ";
  }
  // 归属集合
  std::string owner;
  for (const auto& [ep, se] : epochs_) {
    for (const auto& [pp, gg] : se.gens) {
      if (pp == part && gg == gen) {
        owner += "epoch=" + std::to_string(ep) + " ";
      }
    }
  }
  if (skip_gens_.count(k) > 0) owner += "skip ";
  if (recovery_gens_.count(k) > 0) owner += "recovery ";
  out += "owner={" + (owner.empty() ? std::string("NONE") : owner) + "}; ";
  // 引用计数
  for (const auto& [ep, m] : gen_refs_) {
    auto it = m.find(k);
    if (it != m.end()) {
      out += "refs(epoch=" + std::to_string(ep) + ")=" +
             std::to_string(it->second) + " ";
    }
  }
  // V0.6-A12：回收分类字段（installed/归属见上；以下为回收相关证据）
  {
    out += "| read_refs=" +
           std::to_string(gen_read_refs_.count(k) > 0 ? gen_read_refs_.at(k) : 0);
    out += std::string(" retiring=") + (retiring_gens_.count(k) > 0 ? "1" : "0");
    out += std::string(" reclaim_requested=") +
           (reclaim_pending_.count(k) > 0 ? "1" : "0");
    const std::string fname = FileName(part, gen);
    bool queued = false;
    for (const auto& r : pending_unlink_) {
      if (r.name == fname) { queued = true; break; }
    }
    out += std::string(" in_unlink_queue=") + (queued ? "1" : "0");
    uint64_t fsz = 0;
    const rocksdb::Status fs = env_->GetFileSize(fname, &fsz);
    out += std::string(" file=") + ((fs.ok() && fsz > 0) ? "1" : "0");
  }
  return out;
}

uint64_t SealedFileCache::PendingBytesRecomputed() const {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  uint64_t sum = 0;
  for (const auto& [k, info] : gens_) {
    if (!info.installed) {
      sum += info.bytes;
    }
  }
  last_recomputed_pending_ = sum;
  const uint64_t mirror = pending_bytes_.load(std::memory_order_relaxed);
  if (sum != mirror) {
    ++pending_drift_events_;
  }
  return sum;
}

size_t SealedFileCache::registered_gens() const {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  return gens_.size();
}

// step V0.6-A10：把卡住的 epoch 的 gens 退回可采纳集合（skip），并从 epoch 登记中移除
// 该 epoch ⇒ 下一次封存会按既有机制采纳（不改变 pending/installed/drained）。
void SealedFileCache::ForEachEpoch(
    const std::function<void(
        uint64_t, const std::vector<std::pair<uint32_t, uint32_t>>&)>& fn) const {
  // step V0.6-A11 修复（gdb 栈实证）：**先在锁内做快照、释放锁后再回调**。
  // 原实现在持锁状态下调用 fn，而调用方（排空的"卡住 epoch"扫描）在回调里又调用
  // `GenInstalledPublished()`/`IsGenInstalled()` ⇒ 对非递归锁二次加锁 ⇒ 死锁
  // （实测：主线程查询 pending_gens 与 LOW worker 全部堵在 sealed_cache mu_ 上）。
  std::vector<std::pair<uint64_t, std::vector<std::pair<uint32_t, uint32_t>>>>
      snap;
  {
    rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
    rocksdb::MutexLock l(m);
    snap.reserve(epochs_.size());
    for (const auto& [ep, se] : epochs_) {
      snap.emplace_back(ep, se.gens);
    }
  }
  for (const auto& [ep, gens] : snap) {
    fn(ep, gens);
  }
}

size_t SealedFileCache::RequeueStuckEpochsForDrain(
    const std::vector<uint64_t>& stuck_epochs) {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  size_t moved = 0;
  for (uint64_t ep : stuck_epochs) {
    auto eit = epochs_.find(ep);
    if (eit == epochs_.end()) continue;
    for (const auto& [part, gen] : eit->second.gens) {
      const ZfFileKey key = MakeFileKey(part, gen);
      auto git = gens_.find(key);
      if (git == gens_.end() || git->second.installed) continue;  // 已安装/未登记 ⇒ 不动
      if (skip_gens_.insert(key).second) {
        skip_bytes_ += git->second.bytes;
        skip_part_bytes_[part] += git->second.bytes;
        ++moved;
      }
    }
    eit->second.gens.clear();  // 归属已退回可采纳集合（epoch 不再持有 gens）
  }
  return moved;
}

// step V0.6-A9：**一次完整的所有权交接**（锁内原子完成；不改变 pending/installed/drained）。
std::vector<std::pair<uint32_t, uint32_t>>
SealedFileCache::TakeSkippedGensForDrain(
    std::unordered_map<uint32_t, uint64_t>* part_bytes_out) {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  std::vector<std::pair<uint32_t, uint32_t>> out;
  out.reserve(skip_gens_.size());
  for (ZfFileKey k : skip_gens_) {
    out.emplace_back(static_cast<uint32_t>(k >> 32), static_cast<uint32_t>(k));
  }
  if (part_bytes_out != nullptr) {
    *part_bytes_out = skip_part_bytes_;
  }
  skip_gens_.clear();       // 归属转移：离开可重试集合（在飞期间不再被第二次收养）
  skip_bytes_ = 0;
  skip_part_bytes_.clear();
  return out;
}

void SealedFileCache::RestoreSkippedGens(
    const std::vector<std::pair<uint32_t, uint32_t>>& gens,
    const std::unordered_map<uint32_t, uint64_t>& part_bytes) {
  rocksdb::port::Mutex* m = const_cast<rocksdb::port::Mutex*>(&mu_);
  rocksdb::MutexLock l(m);
  for (const auto& [p, g] : gens) {
    const ZfFileKey key = MakeFileKey(p, g);
    if (skip_gens_.count(key) > 0) {
      adopt_double_return_.fetch_add(1, std::memory_order_relaxed);
      continue;  // 已在集合内（不应发生）——计数而不重复登记
    }
    skip_gens_.insert(key);
    const auto it = part_bytes.find(p);
    if (it != part_bytes.end()) {
      skip_part_bytes_[p] += it->second;
      skip_bytes_ += it->second;
    }
  }
  // 注意：**不触碰** ctx 侧的积压起始时间（backlog_since_micros_），
  // 退回后年龄继续累积 ⇒ 不会被无限推迟（用户契约）。
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
  // M5 诊断：epoch 14 的移交（Concurrent 定位——gen5 未移除疑点）。
  if (epoch == 14 || epoch == 13) {
    std::string gl;
    for (const auto& g : gens) {
      gl += "(" + std::to_string(g.first) + "," + std::to_string(g.second) +
            ")";
    }
    fprintf(stderr, "ZFDBG-handoff epoch=%llu gens=[%s]\n",
            (unsigned long long)epoch, gl.c_str());
  }
  // step 0b-3 修复：**已安装的 generation 不参与移交**。
  // 背景（0b-3 实测）：调用方可能带着过期 gens 列表调用移交（批次让位时按
  // job 全量移交，其中某些 epoch 的数据已由前一批经 LogAndApply 安装）。
  // 若不过滤，已安装数据会被重新塞进 skip/recovery 集合 → skip_bytes_ 虚高、
  // HasPendingGens() 恒真 → 关闭收敛判据永不满足（实测 skip=407.4MB 与
  // drained=407.4MB 相等，即同一批字节同时被判为"待物化"和"已安装"）。
  // 语义依据：installed 之后不得再被当作未安装数据（用户 2026-09-15 规则）。
  std::vector<std::pair<uint32_t, uint32_t>> feasible;
  feasible.reserve(gens.size());
  size_t skipped_installed = 0;
  uint64_t skipped_installed_bytes = 0;
  for (const auto& g : gens) {
    auto git = gens_.find(MakeFileKey(g.first, g.second));
    if (git != gens_.end() && git->second.installed) {
      ++skipped_installed;
      skipped_installed_bytes += git->second.bytes;
      continue;
    }
    feasible.push_back(g);
  }
  if (skipped_installed > 0) {
    handoff_installed_filtered_.fetch_add(skipped_installed,
                                          std::memory_order_relaxed);
    handoff_installed_filtered_bytes_.fetch_add(skipped_installed_bytes,
                                                std::memory_order_relaxed);
  }
  if (feasible.empty()) {
    return;  // 全部已安装：无需移交，也不得把它们重新标记为待物化
  }
  const std::vector<std::pair<uint32_t, uint32_t>>& gens2 = feasible;
  // 1) 从 epoch 移除跳过的 gens（ReleaseEpoch 不再 unlink 它们）。
  auto eit = epochs_.find(epoch);
  if (eit != epochs_.end()) {
    auto& egens = eit->second.gens;
    egens.erase(
        std::remove_if(egens.begin(), egens.end(),
                       [&gens2](const std::pair<uint32_t, uint32_t>& g) {
                         return std::find(gens2.begin(), gens2.end(), g) !=
                                gens2.end();
                       }),
        egens.end());
    for (const auto& [part, gen] : gens2) {
      auto pb = eit->second.part_bytes.find(part);
      if (pb != eit->second.part_bytes.end()) {
        eit->second.total_bytes -= pb->second;
        eit->second.part_bytes.erase(pb);
      }
    }
  }
  // 2) 移交 skip 集合（可读、不回收；per-part 字节供收养时 ratio 合并）。
  for (const auto& [part, gen] : gens2) {
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
    for (const auto& [part, gen] : gens2) {
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
  std::vector<UnlinkRequest> to_unlink;
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
      // step 1t（用户 2026-09-16）：**未安装的代不得回收/抹除**。
      // 实证：group5 失败键所在代已真正封存（sealed_meta=yes, 122 条），却因
      // epoch 释放走到这里被抹除 registry 条目并失去归属（owner=NONE），
      // 而文件仍在 ⇒ 读不到。规则：只有 `installed && 引用归零` 才允许
      // unlink 与移除登记；未安装时把归属移交 recovery（保留 registry 条目与
      // 可读性），让候选发现仍能看到它。
      {
        auto git2 = gens_.find(key);
        if (git2 != gens_.end() && !git2->second.installed) {
          recovery_gens_.emplace(key);
          gen_release_before_install_.fetch_add(1, std::memory_order_relaxed);
          if (eit != epochs_.end()) {
            auto& egens = eit->second.gens;
            egens.erase(std::remove(egens.begin(), egens.end(),
                                    std::make_pair(part, gen)),
                        egens.end());
          }
          continue;  // 不 unlink、不 erase registry 条目
        }
      }
      // step 1as（用户 2026-09-16）：**先判延迟，再做任何销账**。原顺序在
      // 延迟分支之前就 `gens_.erase()` + 计 reclaimed ⇒ 记进 pending 的
      // `installed` 恒为 false，最后一个引用释放时 `inst && …` 判假、retiring
      // 永不标记（case8b: deferred=1 但 retiring=0），且文件仍在却已计 reclaimed。
      // 现在：注册表条目与 reclaimed 记账都推迟到**真实 unlink 成功之后**
      // （PurgePending），延迟期间条目保留、可读、可再取引用。
      if (gen_read_refs_.find(key) != gen_read_refs_.end()) {
        reclaim_deferred_by_reader_.fetch_add(1, std::memory_order_relaxed);
        // 记录完整身份（幂等）：最后一个引用释放时据此处理本 (part,gen)。
        auto& rp = reclaim_pending_[key];
        rp.part = part;
        rp.gen = gen;
        {
          auto git3 = gens_.find(key);
          rp.installed = (git3 != gens_.end() && git3->second.installed);
        }
        rp.reclaim_enabled = reclaim_enabled_;
        rp.request_epoch = epoch;
      } else if (reclaim_enabled_) {
        EnqueueUnlinkLocked(part, gen, &to_unlink);
      }
      // 回收关闭（reclaim_sealed_files=false）或延迟中：**保留 registry 条目**
      // （数据在 LSM 已可读，但文件未删 ⇒ 不得算 reclaimed、不得抹除身份）。
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
    for (auto& req : to_unlink) {
      pending_unlink_.push_back(std::move(req));
    }
  }
  return released_bytes;
}

void SealedFileCache::EnqueueUnlinkLocked(uint32_t part, uint32_t gen,
                                          std::vector<UnlinkRequest>* out) {
  const ZfFileKey key = MakeFileKey(part, gen);
  // 与取得引用同锁互斥：入队后不再接受新引用（retiring => no_new_ref 断言）。
  if (gen_read_refs_.find(key) != gen_read_refs_.end()) {
    assert_retiring_new_ref_.fetch_add(1, std::memory_order_relaxed);
    return;  // 仍有读引用：绝不入队（由延迟路径接管）
  }
  retiring_gens_.insert(key);
  out->push_back(UnlinkRequest{FileName(part, gen), key});
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
    // M5 诊断：读路径 gen 不在册（Concurrent 用例 NotFound 定位）。
    static std::atomic<uint64_t> zf_getnf_dbg {0};
    if (zf_getnf_dbg.fetch_add(1, std::memory_order_relaxed) < 32) {
      fprintf(stderr, "ZFDBG-getnf part=%u gen=%u skip=%d rec=%d\n", part,
              gen, (int)skip_gens_.count(key),
              (int)recovery_gens_.count(key));
    }
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
  std::vector<UnlinkRequest> to_unlink;
  {
    rocksdb::MutexLock l(&mu_);
    to_unlink.swap(pending_unlink_);
    // M3.0：真实 unlink 的 epoch 计数（pending_epochs_ 在 ReleaseEpoch
    // 入队时累计）。
    reclaimed_epochs_ += pending_epochs_;
    pending_epochs_ = 0;
  }
  const size_t before = to_unlink.size();
  for (auto& req : to_unlink) {
    const rocksdb::Status s = env_->DeleteFile(req.name);
    if (s.ok() || s.IsNotFound()) {
      // step 1as：**unlink 完成后才计 reclaimed**，并在此刻移除 registry 条目
      // （延迟回收期间条目必须保留，否则身份与可读性一起丢失）。
      rocksdb::MutexLock l(&mu_);
      // 断言 `reclaimed => installed && refs == 0`。
      auto rit = gen_read_refs_.find(req.key);
      if (rit != gen_read_refs_.end() && rit->second > 0) {
        assert_reclaim_unsafe_.fetch_add(1, std::memory_order_relaxed);
      }
      auto git = gens_.find(req.key);
      if (git != gens_.end()) {
        if (!git->second.installed) {
          assert_reclaim_unsafe_.fetch_add(1, std::memory_order_relaxed);
        }
        reclaimed_bytes_ += git->second.bytes;
        ++reclaimed_gens_;
        gens_.erase(git);
      }
      // step V0.6-A1：留下**合法回收墓碑**（真实 unlink 成功 ⇒ 该代已回收）。
      // 有界保留最近 64K 条（超出清理一半，避免无界增长）。
      if (reclaimed_by_flow_.size() >= 65536) {
        reclaimed_by_flow_.clear();
      }
      reclaimed_by_flow_.insert(req.key);
    }
  }
  return before;
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

bool SealedFileCache::HasPendingGens() const {
  rocksdb::MutexLock l(&mu_);
  return !recovery_gens_.empty() || !skip_gens_.empty();
}

bool SealedFileCache::HasPendingPartGens(uint32_t part_id) const {
  rocksdb::MutexLock l(&mu_);
  const auto in_part = [part_id](ZfFileKey k) {
    return static_cast<uint32_t>(k >> 32) == part_id;
  };
  for (const auto& k : recovery_gens_) {
    if (in_part(k)) return true;
  }
  for (const auto& k : skip_gens_) {
    if (in_part(k)) return true;
  }
  for (const auto& [epoch, refs] : gen_refs_) {
    (void)epoch;
    for (const auto& [k, rc] : refs) {
      if (rc > 0 && in_part(k)) return true;
    }
  }
  return false;
}

size_t SealedFileCache::recovery_count() const {
  rocksdb::MutexLock l(&mu_);
  return recovery_gens_.size();
}

}  // namespace zeroflush
