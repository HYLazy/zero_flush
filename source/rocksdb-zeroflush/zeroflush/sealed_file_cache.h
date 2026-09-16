//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M2: 封存代文件缓存 + 引用计数 + 延迟 unlink。
//
// 设计要点（对应 M2_DESIGN.md §4.3 / §6.5 / I3）：
//  - 封存代文件由 EpochRef 引用计数管理。引用归零时文件移入 pending_unlink_，
//    不立即 unlink（POSIX 下 unlink 对已打开 fd 无害，但新开 fd 会失败，
//    与并发读者的 Get() 窗口有竞态）；
//  - unlink 推迟到 PurgePending() 调用（由 DBImpl::PurgeObsoleteFiles 触发，
//    与原生 SST 删除时机统一）；
//  - 读句柄用 LRU 缓存。未命中则按需打开。
//  - reclaim_sealed_files=false 时，ReleaseEpoch 只减引用不入队（调试用）。
//
// 线程安全：所有公开方法持 mu_；LRU 由 mu_ 保护（单线程访问）。

#pragma once

#include <atomic>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace zeroflush {

// (part, gen) → 物理路径
using ZfFileKey = uint64_t;

inline ZfFileKey MakeFileKey(uint32_t part, uint32_t gen) {
  return (static_cast<uint64_t>(part) << 32) | static_cast<uint64_t>(gen);
}

// 一个 Epoch 的封存文件集合。
struct SealedEpoch {
  uint64_t epoch = 0;
  std::vector<std::pair<uint32_t, uint32_t>> gens;  // (part_id, gen)
  uint64_t total_bytes = 0;                          // 所有 gens 文件大小之和
  // M3.3：per-partition 封存字节（融合归并触发判定 §7.2 用：
  // sealed_bytes[p] / overlap_bytes >= base_merge_min_ratio）。
  std::unordered_map<uint32_t, uint64_t> part_bytes;
  // M3.0 R1：本 epoch 是否收养了恢复期孤儿代（用于物化期的断言放宽，
  // 见 M3_DESIGN.md §7.4/§8.1）。
  bool has_adopted_orphans = false;
  // M4.5b：本 epoch 是否收养了 kSkip 跳过代（攒批）。与恢复期孤儿不同，
  // kSkip 跳过的数据 seq 与本 epoch 连续（未崩溃），可正常融合归并——
  // 物化决策据此区分保守（崩溃孤儿）与可融合（攒批跳过）收养。
  bool has_adopted_skips = false;
  // M5（zeroflush0.98）：本 epoch 封存是否含"自身新 gen0"（freeze 的
  // 分区 old_gen==0——学习批 hash 数据仍在分批封存）。物化判齐用：
  // gen0 覆盖 == partitions（全分区有 gen0）或（覆盖 >0 且本 epoch 无
  // 自身新 gen0——学习批已封完、覆盖不再增长）→ 单任务全量切片物化。
  bool has_fresh_gen0 = false;
  // M3.0：封存登记时刻（NowMicros），用于物化耗时统计。
  uint64_t sealed_at_micros = 0;
  // M3.1：该 epoch 写入时使用的 PartitionTable version（用于物化时取回
  // 同一张表做范围断言，见 M3_DESIGN.md §4.3）。
  uint32_t table_version = 0;
  // ZeroFlushV0 M4（活跃尾 split）：显式范围覆盖。切路由后冻结的父分区尾
  // epoch（split orphan）在新表中无此 part id，物化需按父原区间 [lo,hi) 做
  // 范围/重叠判定（而非孤儿兜底的全范围）——否则会与全区 base 文件互叠。
  bool has_range_override = false;
  std::string range_lo;  // 空 = -∞
  std::string range_hi;  // 空 = +∞
  // ZeroFlushV0 M5（活跃尾 split 跨批互斥）：本 epoch 是 split 尾 epoch 时
  // 记录两子分区 id（父 id 已不在表内）。物化期由 ctx 侧的 children hold
  // token 保证子分区批次让位（不在此并发安装 L1）。
  bool split_tail = false;
  uint32_t split_child_l = 0;
  uint32_t split_child_r = 0;
};

// LRU 节点。
struct SealedFileEntry {
  ZfFileKey key;
  std::shared_ptr<rocksdb::RandomAccessFile> file;
};

class SealedFileCache {
 public:
  SealedFileCache(rocksdb::Env* env, std::string dir, uint32_t capacity,
                  bool reclaim_enabled);
  ~SealedFileCache();

  SealedFileCache(const SealedFileCache&) = delete;
  SealedFileCache& operator=(const SealedFileCache&) = delete;

  // 登记一个 epoch 的封存文件集。与原生 AddEpoch 不同：
  // 会在同一个持锁窗口内先收养恢复期孤儿代（若有），保证读路径的
  // in_epoch 校验在收养期间恒成立（M3.0 R1，见 M3_DESIGN.md §8.1）。
  // M3.4：多列族共享同一物理分区文件时 refcount = CF 个数
  // （每个 CF 的 imm 各持一引，见 M3_DESIGN.md §9.1）。
  void AddEpochWithRecoveryAdoption(const SealedEpoch& e,
                                    uint32_t refcount = 1);

  // ---------------------------------------------------------------------
  // step 0b-3 第二阶段重构：generation 生命周期由本对象**单一权威**维护。
  // 用户（2026-09-15）指定的语义分离：
  //   pending_materialize_bytes : 仍需物化的 generation 字节之和
  //                               （state ∈ {sealed, queued, building,
  //                                 conflict, recovery}）
  //   drained_materialize_bytes : 仅在 LogAndApply 成功后增加（数据已进 L1）
  //   reclaimed_bytes           : 仅在引用全部释放（物理可回收）后增加
  // handoff / 收养 / conflict 重规划只是状态迁移或所有权变更，**不改变字节
  // 计数**；同一 (part, gen) 只注册一次。旧的"ledger 所有权镜像"已删除——
  // 并行维护第二份所有权必然漂移（0b-3 实测：持续空 edit 后 ledger 报 0 而
  // skip 集合仍有 407MB）。
  // ---------------------------------------------------------------------
  // 注册一个 (part, gen) 及其封存字节（幂等：仅首次生效）。
  void RegisterGen(uint32_t part, uint32_t gen, uint64_t bytes);

  // LogAndApply 成功、该 epoch 的数据真实进入 L1 后调用：把该 epoch 仍持有
  // 的 gens 标记为 installed 并计入 drained。已移交（不在 epochs_[e].gens
  // 中）的 gens 不受影响——这正是空 VersionEdit 批次的语义（其 gens 全部
  // 已移交 ⇒ pending 不变）。
  void MarkEpochInstalled(uint64_t epoch);

  // step FIFO：该 physical_part 仍 pending 的最老 generation
  // （epochs_ ∪ skip_gens_ ∪ recovery_gens_；installed 的不算）。
  // 用途：同分区 FIFO——若最老 pending 代早于本批将安装的最老代，本批不得安装。
  bool OldestPendingGen(uint32_t part, uint32_t* out) const;

  // step 1q（用户 2026-09-16 第 1 项）：pending 的**权威重算**——按 registry
  // 定义 `Σ_{sealed && !installed} bytes`。契约：
  //   pending = registry 中所有 sealed && !installed generation 的字节之和；
  //   handoff / 收养 / 让位只改变归属，**不改变 pending/drained**；
  //   只有成功的 AddFile+LogAndApply 才 pending→drained；
  //   物理回收只增加 reclaimed。
  // 用途：守恒校验（O(1) 镜像 vs 重算），任何漂移都必须暴露而不是被掩盖。
  // step 1r（用户 2026-09-16 第 3 项）：单代状态描述——归属集合（epoch/skip/
  // recovery）、installed、字节、引用计数。用于读失败时按证据分类
  // （索引撤除 / 读视图交接 / 候选发现），而不是猜测。
  std::string DescribeGen(uint32_t part, uint32_t gen) const;

  uint64_t PendingBytesRecomputed() const;
  uint64_t pending_drift_events() const { return pending_drift_events_; }
  // step 1t：epoch 释放时"引用归零但尚未安装"的代——**不得回收**，改为移交
  // recovery 并保留 registry 条目（应恒为 0 = 正常路径下 epoch 只在安装后释放）。
  uint64_t gen_release_before_install() const {
    return gen_release_before_install_.load(std::memory_order_relaxed);
  }

  uint64_t pending_materialize_bytes() const {
    return pending_bytes_.load(std::memory_order_relaxed);
  }
  uint64_t drained_materialize_bytes() const { return drained_bytes_; }
  uint64_t reclaimed_bytes() const { return reclaimed_bytes_; }
  uint64_t peak_pending_bytes() const { return peak_pending_bytes_; }
  uint64_t installed_epochs() const { return installed_epochs_; }
  uint64_t reclaimed_gens() const { return reclaimed_gens_; }
  size_t registered_gens() const;
  uint64_t handoff_installed_filtered() const {
    return handoff_installed_filtered_.load(std::memory_order_relaxed);
  }
  uint64_t handoff_installed_filtered_bytes() const {
    return handoff_installed_filtered_bytes_.load(std::memory_order_relaxed);
  }

  // M3.0 R1：登记恢复期孤儿代（Recover 时调用）。这些文件可读但不可
  // 回收、不占 refcount，直到被下一次 AddEpochWithRecoveryAdoption 收养。
  void AddRecoveryGens(const std::vector<std::pair<uint32_t, uint32_t>>& gens,
                       uint64_t total_bytes);

  // M4.5b 攒批：把指定 gens 从 epoch 中移除并移交 recovery 集合（可读、
  // 不回收）——物化跳过（kSkip）的分区数据保留在磁盘，待下个 epoch
  // 收养后多代合并物化。part_bytes 为该分区的封存字节（收养时合并进
  // 新 epoch 的 ratio 计算）。
  void HandOffSkippedToRecovery(uint64_t epoch,
                                const std::vector<std::pair<uint32_t, uint32_t>>& gens,
                                const std::unordered_map<uint32_t, uint64_t>& part_bytes);

  // 释放一个 epoch 的全部 gens 引用。引用归零（per-gen 计数）时把文件名
  // 移入 pending_unlink_，并返回该 epoch 的封存字节（用于物化统计）；
  // 未找到或未归零返回 0。reclaim_sealed_files == false 时只减引用不入队。
  // M4.8 回收分区化：epoch 引用 = 其全部 (part, gen) 引用之和，WAL 段按
  // (part, gen) 独立回收（本入口释放全部，等价 ReleaseGens(epoch, 全量)）。
  uint64_t ReleaseEpoch(uint64_t epoch);

  // M4.8 回收分区化：按 (part, gen) 子集独立释放引用——单个 WAL 段的
  // 引用归零即独立 unlink（无需等 epoch 其他分区物化完成）。全部 gens
  // 释放后 epoch 完全回收（物化统计/封存字节扣减）。未登记的 gens 幂等
  // 跳过。返回本次实际 unlink 的字节（epoch 完全回收时为其封存总字节）。
  uint64_t ReleaseGens(
      uint64_t epoch,
      const std::vector<std::pair<uint32_t, uint32_t>>& gens);

  // 取 (part, gen) 的只读句柄。LRU 命中直接返回；未命中则打开。
  // gen 不在 epochs_ 中且不在恢复期集合中（即未登记）返回 NotFound。
  rocksdb::Status Get(uint32_t part, uint32_t gen,
                      std::shared_ptr<rocksdb::RandomAccessFile>* out);

  // M3.2：取 epoch 的封存登记信息（物化用：gens/table_version/字节等）。
  // 未登记（未知 epoch 或已回收）返回 false。
  bool GetEpoch(uint64_t epoch, SealedEpoch* out) const;

  // 由 DBImpl::PurgeObsoleteFiles 调用，真实 unlink 移入队列的文件。
  // 调用后清空 pending_unlink_，返回实际 unlink 的文件数。
  size_t PurgePending();

  // 统计。
  uint64_t sealed_bytes() const;
  // M4.8：kSkip 攒批等待集合的字节（L0 遮蔽等待的上限判定用）。
  uint64_t skipped_bytes() const;
  uint64_t pending_count() const;
  size_t handle_count() const;
  // M3.0：封存代定点读次数 / LRU 未命中次数（打开文件才算 miss）。
  uint64_t sealed_read_count() const;
  uint64_t sealed_cache_miss() const;
  // M3.0：物化完成（引用归零）epoch 数 / 累计物化耗时（封存→归零）。
  uint64_t materialized_epochs() const;
  uint64_t materialize_micros() const;
  // M3.0：已真实 unlink 的 epoch 数。
  uint64_t reclaimed_epochs() const;
  // M3.0 R1：当前待收养的恢复期孤儿代文件数（诊断用）。
  size_t recovery_count() const;
  // M5（zeroflush0.98）§3.5-2：是否存在待收养数据（skip 攒批 gens /
  // 恢复期孤儿 gens 任一非空）——DB 关闭冲刷循环的终止判定。
  bool HasPendingGens() const;
  // ZeroFlushV0 P1：该分区是否仍有未回收的 WAL 段引用（epoch 引用 /
  // recovery 孤儿 / skip 攒批任一非空）——merge 的 retire 判定用。
  bool HasPendingPartGens(uint32_t part_id) const;

 private:
  std::string FileName(uint32_t part, uint32_t gen) const;
  void TouchLRU(ZfFileKey key);

  mutable rocksdb::port::Mutex mu_;
  rocksdb::Env* env_;
  std::string dir_;
  uint32_t capacity_;
  bool reclaim_enabled_;

  // epoch → SealedEpoch（持久保留以便 Get 校验）
  std::unordered_map<uint64_t, SealedEpoch> epochs_;
  // M4.8 回收分区化：epoch → ((part, gen) → 引用计数)。每 gen 一引
  // （多列族共享物理文件时每 CF 一引）；某 gen 归零即独立 unlink 该
  // WAL 段；epoch 的 gen 引用全空 = epoch 完全回收（统计/字节扣减）。
  std::unordered_map<uint64_t, std::unordered_map<ZfFileKey, uint32_t>>
      gen_refs_;

  // (part, gen) → 已打开句柄
  std::unordered_map<ZfFileKey, std::shared_ptr<rocksdb::RandomAccessFile>>
      handles_;
  // LRU 列表（front = 最新，back = 最久未用）
  std::list<ZfFileKey> lru_order_;

  // 待 unlink 的文件名（ReleaseEpoch 收集，PurgePending 真正删除）
  std::vector<std::string> pending_unlink_;

  // M3.0 R1：恢复期孤儿代集合（可读、不可回收、不占 refcount）。
  // 由 Recover() 登记，由 AddEpochWithRecoveryAdoption 收养（并入 epoch）。
  // 崩溃恢复孤儿：seq 与已物化数据可能交错 → 物化期保守（不融合）。
  std::unordered_set<ZfFileKey> recovery_gens_;
  uint64_t recovery_bytes_ = 0;
  // M4.5b：recovery 集合的 per-partition 字节（收养时合并进 part_bytes，
  // 供攒批后的融合 ratio 计算）。
  std::unordered_map<uint32_t, uint64_t> recovery_part_bytes_;
  // M4.5b：kSkip 攒批跳过代集合（可读、不可回收、不占 refcount）。
  // 由 HandOffSkippedToRecovery 登记，下个 epoch 封存时收养（多代合并
  // 物化）。与恢复期孤儿不同：seq 连续 → 可融合归并。
  std::unordered_set<ZfFileKey> skip_gens_;
  uint64_t skip_bytes_ = 0;
  std::unordered_map<uint32_t, uint64_t> skip_part_bytes_;

  // step 0b-3：generation registry（单一权威）
  struct GenInfo {
    uint64_t bytes = 0;
    bool installed = false;
  };
  std::unordered_map<ZfFileKey, GenInfo> gens_;
  std::atomic<uint64_t> pending_bytes_{0};   // O(1) 镜像（唯一写入方 = 本对象）
  // 移交时被过滤掉的"已安装"generation（诊断：应恒为 0；非 0 说明调用方
  // 带着过期 gens 列表移交）。
  std::atomic<uint64_t> handoff_installed_filtered_{0};
  std::atomic<uint64_t> handoff_installed_filtered_bytes_{0};
  uint64_t drained_bytes_ = 0;               // 单调：安装（LogAndApply）时增加
  uint64_t reclaimed_bytes_ = 0;             // 单调：引用归零、可物理回收时增加
  uint64_t peak_pending_bytes_ = 0;
  // pending 守恒校验：镜像与重算不一致的次数（应恒为 0）。
  mutable uint64_t pending_drift_events_ = 0;
  std::atomic<uint64_t> gen_release_before_install_{0};
  mutable uint64_t last_recomputed_pending_ = 0;
  uint64_t installed_epochs_ = 0;
  uint64_t reclaimed_gens_ = 0;

  // 累计封存字节（统计用）
  uint64_t sealed_bytes_ = 0;
  // M3.0 统计
  std::atomic<uint64_t> sealed_read_count_{0};
  std::atomic<uint64_t> sealed_cache_miss_{0};
  // M3.0：物化完成 epoch 数 / 累计物化耗时（mu_ 保护）
  uint64_t materialized_epochs_ = 0;
  uint64_t materialize_micros_total_ = 0;
  // M3.0：已真实 unlink 的 epoch 数 / 排队待 unlink 的 epoch 数（mu_ 保护）
  uint64_t reclaimed_epochs_ = 0;
  uint64_t pending_epochs_ = 0;
};

}  // namespace zeroflush
