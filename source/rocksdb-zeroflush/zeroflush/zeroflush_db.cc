//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M1: 全局上下文与 Open 入口实现。

#include "zeroflush/zeroflush_db.h"
#include "zeroflush/get_trace.h"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <optional>
#include <thread>
#include <unordered_set>

#include "db/column_family.h"
#include "db/compaction/zf_sink_diag.h"  // step 0a 诊断（临时）
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_thread.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "rocksdb/advanced_cache.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/write_batch.h"
#include "util/hash.h"
#include "zeroflush/partition_index.h"
#include "zeroflush/partition_table.h"
#include "zeroflush/sealed_file_cache.h"
#include "zeroflush/slim_memtable.h"
#include "zeroflush/wal_manager.h"

namespace zeroflush {

using ROCKSDB_NAMESPACE::InfoLogLevel;

namespace {

// M4.7b：value cache 条目释放（Cache 淘汰时删除堆上的 string）与 helper。
namespace {
void ValueCacheDeleter(ROCKSDB_NAMESPACE::Cache::ObjectPtr obj,
                       ROCKSDB_NAMESPACE::MemoryAllocator*) {
  delete static_cast<std::string*>(obj);
}
const ROCKSDB_NAMESPACE::Cache::CacheItemHelper* ValueCacheHelper() {
  static ROCKSDB_NAMESPACE::Cache::CacheItemHelper h(
      ROCKSDB_NAMESPACE::CacheEntryRole::kMisc, &ValueCacheDeleter);
  return &h;
}
}  // namespace

// WriteBatch 逐记录回调：路由 → 分区 WAL 追加 → Slim MemTable 插入。
// M2.3-2：touched_ 收集本 batch 实际触达的分区分区 ID，sync 阶段只
// fsync 这些分区（替代原 M1 的 SyncAll 全分区 fsync，64x 提升 sync=true
// 的吞吐）。
// M4.1c：本 handler 在 DB mutex 之外执行（写组 leader 锁外插入）。mem_
// 由调用方在锁内捕获并 Ref；post_map_ 按 mem 累计 MemTablePostProcessInfo
// （并发插入路径必须提供 ppi），组收尾统一 BatchPostProcess。
class ZfBatchHandler : public ROCKSDB_NAMESPACE::WriteBatch::Handler {
 public:
  ZfBatchHandler(ZeroFlushContext* ctx, ROCKSDB_NAMESPACE::MemTable* mem,
                 ROCKSDB_NAMESPACE::SequenceNumber* seq, uint64_t* touched,
                 std::map<ROCKSDB_NAMESPACE::MemTable*,
                          ROCKSDB_NAMESPACE::MemTablePostProcessInfo>* post_map,
                 uint32_t table_version)
      : ctx_(ctx), mem_(mem), seq_(seq), touched_(touched),
        table_version_(table_version), post_map_(post_map) {}

  ROCKSDB_NAMESPACE::Status PutCF(uint32_t column_family_id,
                                  const ROCKSDB_NAMESPACE::Slice& key,
                                  const ROCKSDB_NAMESPACE::Slice& value) override {
    if (column_family_id != 0) {
      return ROCKSDB_NAMESPACE::Status::NotSupported(
          "ZeroFlush M1: multi column family not supported");
    }
    ROCKSDB_NAMESPACE::Status s = ctx_->AddRecord(
        mem_, key, value, ROCKSDB_NAMESPACE::kTypeValue, *seq_,
        &(*post_map_)[mem_], table_version_);
    if (s.ok() && touched_ != nullptr) {
      *touched_ |= (uint64_t{1} << ctx_->RouteWithVersion(key, table_version_));
    }
    ++(*seq_);
    return s;
  }

  ROCKSDB_NAMESPACE::Status DeleteCF(uint32_t column_family_id,
                                     const ROCKSDB_NAMESPACE::Slice& key) override {
    if (column_family_id != 0) {
      return ROCKSDB_NAMESPACE::Status::NotSupported(
          "ZeroFlush M1: multi column family not supported");
    }
    ROCKSDB_NAMESPACE::Slice empty;
    ROCKSDB_NAMESPACE::Status s = ctx_->AddRecord(
        mem_, key, empty, ROCKSDB_NAMESPACE::kTypeDeletion, *seq_,
        &(*post_map_)[mem_], table_version_);
    if (s.ok() && touched_ != nullptr) {
      *touched_ |= (uint64_t{1} << ctx_->RouteWithVersion(key, table_version_));
    }
    ++(*seq_);
    return s;
  }

  ROCKSDB_NAMESPACE::Status SingleDeleteCF(
      uint32_t column_family_id,
      const ROCKSDB_NAMESPACE::Slice& key) override {
    if (column_family_id != 0) {
      return ROCKSDB_NAMESPACE::Status::NotSupported(
          "ZeroFlush M1: multi column family not supported");
    }
    ROCKSDB_NAMESPACE::Slice empty;
    ROCKSDB_NAMESPACE::Status s = ctx_->AddRecord(
        mem_, key, empty, ROCKSDB_NAMESPACE::kTypeSingleDeletion, *seq_,
        &(*post_map_)[mem_], table_version_);
    if (s.ok() && touched_ != nullptr) {
      *touched_ |= (uint64_t{1} << ctx_->RouteWithVersion(key, table_version_));
    }
    ++(*seq_);
    return s;
  }

  ROCKSDB_NAMESPACE::Status DeleteRangeCF(
      uint32_t /*column_family_id*/, const ROCKSDB_NAMESPACE::Slice& begin,
      const ROCKSDB_NAMESPACE::Slice& end) override {
    ROCKSDB_NAMESPACE::Status s = ctx_->AddRecord(
        mem_, begin, end, ROCKSDB_NAMESPACE::kTypeRangeDeletion, *seq_,
        &(*post_map_)[mem_], table_version_);
    if (s.ok() && touched_ != nullptr) {
      // kRangeDelPartId=0xFFFFFFFE 装不进 64 位位图 → 用位 63 映射
      *touched_ |= (uint64_t{1} << 63);
    }
    ++(*seq_);
    return s;
  }

  ROCKSDB_NAMESPACE::Status MergeCF(
      uint32_t /*column_family_id*/, const ROCKSDB_NAMESPACE::Slice& key,
      const ROCKSDB_NAMESPACE::Slice& value) override {
    ROCKSDB_NAMESPACE::Status s = ctx_->AddRecord(
        mem_, key, value, ROCKSDB_NAMESPACE::kTypeMerge, *seq_,
        &(*post_map_)[mem_], table_version_);
    if (s.ok() && touched_ != nullptr) {
      *touched_ |= (uint64_t{1} << ctx_->RouteWithVersion(key, table_version_));
    }
    ++(*seq_);
    return s;
  }

  void LogData(const ROCKSDB_NAMESPACE::Slice& /*blob*/) override {
    // no-op
  }

 private:
  ZeroFlushContext* ctx_;
  ROCKSDB_NAMESPACE::MemTable* mem_;
  ROCKSDB_NAMESPACE::SequenceNumber* seq_;
  uint64_t* touched_;  // 触达分区位图（分区数 ≤ 64；热路径省 hash）
  uint32_t table_version_;  // M4.10：写组绑定的路由表版本
  std::map<ROCKSDB_NAMESPACE::MemTable*,
           ROCKSDB_NAMESPACE::MemTablePostProcessInfo>* post_map_;
};

}  // namespace

// ---------------------------------------------------------------------------
// ZeroFlushContext
// ---------------------------------------------------------------------------

ZeroFlushContext::ZeroFlushContext(const ZeroFlushOptions& zfo,
                                   const std::string& wal_dir,
                                   ROCKSDB_NAMESPACE::Env* env)
    : zfo_(zfo), wal_dir_(wal_dir), env_(env) {
  wal_.reset(new PartitionedWalManager(env_, wal_dir_, zfo_.partitions,
                                       zfo_.partition_target_bytes));
  // M2：封存代文件缓存（持 wal_dir_，LRU 由 max_open_sealed_files 控制）。
  sealed_cache_.reset(new SealedFileCache(env_, wal_dir_,
                                          zfo_.max_open_sealed_files,
                                          zfo_.reclaim_sealed_files));
  // ---- ZeroFlushV0 M4：动态治理 DG 状态 ----
  rate_tracker_.reset(new PartitionRateTracker(zfo_.governance_ewma_fast_secs,
                                               zfo_.governance_ewma_slow_secs));
  GovernancePolicy::Config cfg;
  cfg.p_min = zfo_.governance_p_min;
  cfg.p_max = zfo_.governance_p_max;
  cfg.theta_split = zfo_.governance_split_theta;
  cfg.theta_merge = zfo_.governance_merge_theta;
  cfg.split_min_bytes = zfo_.governance_split_min_bytes;
  cfg.merge_max_bytes = zfo_.governance_merge_max_bytes;
  cfg.confirm_windows = std::max<uint32_t>(1, zfo_.governance_confirm_windows);
  cfg.cooldown_base_us = zfo_.governance_cooldown_micros;
  cfg.cooldown_max_us = zfo_.governance_cooldown_max_micros;
  cfg.max_actions_per_tick =
      std::max<uint32_t>(1, zfo_.governance_max_actions_per_tick);
  cfg.merge_idle_after_us = zfo_.governance_merge_idle_micros;
  governance_policy_.reset(new GovernancePolicy(cfg));
}

ZeroFlushContext::~ZeroFlushContext() = default;

ROCKSDB_NAMESPACE::Status ZeroFlushContext::Open() {
  const rocksdb::Status s = wal_->Open();
  if (s.ok()) {
    // ZeroFlushV0 P2（M2）：split 子分区（id > 初始 P-1）在 parts_ 中不存在
    // ——按当前路由表全部 part_ids 补齐并按磁盘文件初始化活跃代（幂等）。
    std::vector<uint32_t> ids;
    if (tables_ && tables_->current()) {
      ids = tables_->current()->part_ids();
    }
    wal_->InitPartitionsFromFiles(ids).PermitUncheckedError();
    LoadDrainingFile();  // ZeroFlushV0 P1c：崩溃后重建 DRAINING 注册表
  }
  return s;
}

bool ZeroFlushContext::ShouldSeal() const {
  // R57 诊断：限频打印三个触发源（每 4096 次取模 + 仅 index 超 1GB 时打印）。
  static std::atomic<uint64_t> zf_seal_calls{0};
  const uint64_t sc = zf_seal_calls.fetch_add(1, std::memory_order_relaxed);
  if ((sc & 0x1FFFFF) == 0) {
    const uint64_t im = index_set_ ? index_set_->total_mem_bytes() : 0;
    fprintf(stderr, "ZFDBG-seal active=%llu over=%d imem=%llu budget=%llu\n",
            (unsigned long long)wal_->TotalActiveBytes(),
            wal_->AnyPartitionOverTarget() ? 1 : 0,
            (unsigned long long)im,
            (unsigned long long)zfo_.index_mem_budget);
    if (im > (500ull << 20) && index_set_ != nullptr) {
      index_set_->DumpState();
    }
  }
  // M4.1a：O(1) 判定——总活跃字节与超限标志均由 Append/Freeze 以 relaxed
  // atomic 维护，写路径不再每写组遍历全部 P 分区（实测 ~0.3us×P 开销）。
  // 主触发：全分区活跃字节合计 ≥ epoch_target_bytes（原子读）。
  // 副触发：任一分区 ≥ partition_target_bytes（Append 时置位，防倾斜）。
  // M4.3d-2：终态路径追加内存预算背压——分区索引总内存 ≥ 预算即触发
  // freeze（索引是终态 L0 的内存驻留，需独立于 WAL 字节的背压）。
  if (index_set_ != nullptr && zfo_.index_mem_budget > 0 &&
      index_set_->total_mem_bytes() >= zfo_.index_mem_budget) {
    return true;
  }
  return wal_->TotalActiveBytes() >= zfo_.epoch_target_bytes ||
         wal_->AnyPartitionOverTarget();
}

bool ZeroFlushContext::BuildBaseAlignedTable(
    ROCKSDB_NAMESPACE::ColumnFamilyData* cfd,
    std::shared_ptr<PartitionTable>* out) const {
  assert(cfd != nullptr);
  assert(out != nullptr);
  // REQUIRES: DB mutex held（cfd->current()）。
  const auto* vstorage = cfd->current()->storage_info();
  // R59D：对齐对象 = base_level（ZF 物化实际直装层），而非硬编码 L1——
  // dynamic level 下空库 base=6、数据增长后下移（2.2GB 实测 L1/L2）；
  // 读错层使表桶与直装文件错位 → 桶偏斜/fallback 循环（研究结论）。
  const int base_lvl = vstorage->base_level();
  const auto& base_files = vstorage->LevelFiles(base_lvl);
  if (base_files.empty()) {
    return false;  // base 层为空：无法对齐，调用方保持当前表/采样兜底。
  }
  // 桶聚合：目标分区数 = zfo_.partitions；base 层文件数 ≤ 目标时每文件
  // 一桶。桶边界 = 桶末文件的 largest user key——精确落在文件边界上，
  // 保证物化输出文件键范围 ⊆ 单个 base 文件范围（1:1 归并/融合）。
  std::vector<std::string> boundaries;
  const uint32_t target = std::max<uint32_t>(1, zfo_.partitions);
  if (static_cast<size_t>(target) >= base_files.size()) {
    for (size_t i = 0; i + 1 < base_files.size(); ++i) {
      boundaries.push_back(base_files[i]->largest.user_key().ToString());
    }
  } else {
    uint64_t total = 0;
    for (const auto* f : base_files) {
      total += f->fd.GetFileSize();
    }
    const uint64_t target_bytes = total / target;
    uint64_t acc = 0;
    for (size_t i = 0; i < base_files.size(); ++i) {
      acc += base_files[i]->fd.GetFileSize();
      if (acc >= target_bytes && i + 1 < base_files.size() &&
          boundaries.size() + 1 < target) {
        boundaries.push_back(base_files[i]->largest.user_key().ToString());
        acc = 0;
      }
    }
  }
  if (boundaries.empty()) {
    return false;  // 单文件 base 层无法形成分区边界。
  }
  // 层内文件键范围严格升序 → boundaries 升序（PartitionTable::Create 校验）。
  return PartitionTable::Create(tables_->current_version() + 1,
                                std::move(boundaries), ucmp_, out)
      .ok();
}

ROCKSDB_NAMESPACE::Status ZeroFlushContext::SealEpochAndSwitch(
    ROCKSDB_NAMESPACE::DBImpl* impl,
    ROCKSDB_NAMESPACE::ColumnFamilyData* cfd) {
  assert(impl != nullptr);
  assert(cfd != nullptr);
  // 必须在 write thread 持 DB mutex 下调用。
  const uint64_t epoch = epoch_counter_.fetch_add(1) + 1;
  SealedEpoch se;
  se.epoch = epoch;
  // M3.1：记录当前使用的 PartitionTable version。
  se.table_version = tables_ ? tables_->current_version() : 0;
  // Step 1：封存全部分区。用 PartitionTable 中的 part_ids 而非 [0, P)。
  std::vector<uint32_t> ids;
  if (tables_ && tables_->current()) {
    ids = tables_->current()->part_ids();
  } else {
    ids.reserve(zfo_.partitions);
    for (uint32_t i = 0; i < zfo_.partitions; ++i) ids.push_back(i);
  }
  for (uint32_t pid : ids) {
    const FreezeResult fr = wal_->Freeze(pid);
    // step 1q（用户 2026-09-16 第 2 项）：Freeze 即登记——该代进入唯一
    // registry（sealed && !installed ⇒ 计入 pending）；此后 epoch 收养只是
    // 归属迁移。避免已封存但无主的代被 ReleaseGens 当可回收，也避免让位时
    // 无人接管（组 5 NotFound 的根因）。
    if (sealed_cache_ != nullptr && fr.sealed_bytes > 0) {
      sealed_cache_->RegisterGen(pid, fr.old_gen, fr.sealed_bytes);
    }
    // M3.2：只登记确有数据的分区（wfile 延迟创建，未写分区无物理文件；
    // 物化按 gens 重建路径 scan，空分区条目会导致 "scanner file not open"）。
    if (fr.sealed_bytes > 0) {
      se.gens.emplace_back(pid, fr.old_gen);
      // M3.3：per-partition 封存字节（融合归并触发判定 §7.2 用）。
      se.part_bytes[pid] = fr.sealed_bytes;
    }
    se.total_bytes += fr.sealed_bytes;
    (void)fr.sealed_path;  // SealedFileCache 自行重算
  }
  // M4.1a：全部封存完成，清超限标志（新代从 0 起，超限由后续 Append
  // 重新置位；不清会在下一写组触发一次多余的 ShouldSeal→空封存）。
  wal_->ClearOverTargetFlag();
  // M4.3a：终态路径——封存时冻结全部分区索引（active → frozen 链）。
  // 只冻结确有数据的分区（se.gens 只登记 sealed_bytes>0 的分区）。
  FreezeIndexes(se.gens);
  // M3.1：kSampled 模式：首个 epoch 封存时从采样器学习边界并安装新表。
  // 注意：本 epoch 的记录是用 hash 表（version 0）写的（M3_DESIGN §5.2
  // 步骤 3），se.table_version 必须保持 0——物化用它取回 hash 表并跳过
  // 范围断言；若错误地标成新表版本，物化会拿学习后的边界对 hash 路由
  // 的记录做范围断言，必然报 "materialized range outside table bounds"。
  if (zfo_.routing_mode == ZeroFlushOptions::RoutingMode::kSampled &&
      sampler_ && tables_ && tables_->current_version() == 0 &&
      epoch == 1 && !sampler_->empty()) {
    std::vector<std::string> boundaries;
    if (sampler_->BuildBoundaries(zfo_.partitions, &boundaries)) {
      std::shared_ptr<PartitionTable> new_table;
      rocksdb::Status cps = PartitionTable::Create(
          1, std::move(boundaries), ucmp_, &new_table);
      if (cps.ok()) {
        tables_->InstallNewVersion(std::move(new_table));
        // M4.10c：新表必须立即落盘 ZFPROPS——否则崩溃/重开后恢复旧表，
        // 路由错位使活跃段数据读不到（R52）。
        PersistZfProps().PermitUncheckedError();
        // 学习期 epoch 1 用 hash 写入：se.table_version 保持 0（上方初始化
        // 值），仅后续 epoch 使用 version 1。
      }
    }
    sampler_->Clear();
  }
  // M4.2b：kAlignL1 模式——每 epoch 封存时按当前 L1 文件边界重新对齐
  // 分区（compaction 感知分区）。语义与 kSampled 学习期一致：
  // 本 epoch 的记录用旧表写入（se.table_version 保持旧版本，物化按旧表
  // 断言）；新表经 InstallNewVersion 生效，供下一 epoch 写入使用。
  // L1 为空（空库/首轮，数据尚在 L0 未归并）或无法形成边界时保持当前
  // 表，下一 epoch 重试——收敛路径：hash 写（L0 交错回落）→ L0→L1
  // 归并出有序 L1 → 对齐表生效 → 后续 epoch 输出与 L1 1:1 对齐。
  if (zfo_.routing_mode == ZeroFlushOptions::RoutingMode::kAlignL1 &&
      tables_) {
    std::shared_ptr<PartitionTable> new_table;
    if (BuildBaseAlignedTable(cfd, &new_table)) {
      tables_->InstallNewVersion(std::move(new_table));
      // M4.10c：同 kSampled——新表立即落盘，重开路由一致。
      PersistZfProps().PermitUncheckedError();
    }
  }
  // Step 2：登记到 SealedFileCache（refcount=1）。M3.0 R1：若存在恢复期
  // 孤儿代（Recover 时 AddRecoveryGens 登记），会在同一持锁窗口内被收养
  // 并入本 epoch（M3_DESIGN.md §8.1），保证读路径的 in_epoch 校验在收养
  // 期间恒成立——不再需要调用方手动并入。
  RegisterSealedEpoch(se);
  // Step 3：把 epoch 标到旧 mem（即将被 SwitchMemtable 切换为 imm）。
  // 旧 mem 析构时通过 zf_ctx_->ReleaseEpoch(zf_epoch_) 归还 SealedFileCache
  // 引用；这是 I3 "iter 持有旧 SV 期间文件不删" 的内存安全基础。
  ROCKSDB_NAMESPACE::MemTable* old_mem = cfd->mem();
  if (old_mem != nullptr) {
    old_mem->SetZfEpoch(epoch);
  }
  // Step 4：SwitchMemtable（产生新的 mutable；旧 mem 进 imm）。
  // SwitchMemtable 是 DBImpl private，所以走公开包装 ZfSwitchMemtable。
  // SwitchMemtable 会调 CreateNewMemtable，cfd->zf_ctx_ 已被传播到新 mem。
  return impl->ZfSwitchMemtable(cfd);
}

uint64_t ZeroFlushContext::ReleaseEpoch(uint64_t epoch) {
  // step 1s（用户 2026-09-16 第 1/2/3 项）：**未安装的代不得随 epoch 释放而
  // 消失**。诊断实证（group5 iter=395）：非安装 epoch 释放时同时丢掉了
  // registry 条目与 frozen 索引段，而 WAL 文件仍在 ⇒ 索引 miss + LSM 无数据
  // = NotFound；pending 镜像还继续计着它（泄漏）。
  // 规则：
  //   · 已安装的代 → 释放 frozen 索引 + 归还缓存引用（可回收）；
  //   · 未安装的代 → 移交 recovery（保留归属与可读性），**保留** frozen 索引
  //     段与 registry 条目（pending 由 registry 定义，保持守恒）。
  if (sealed_cache_ != nullptr) {
    SealedEpoch se;
    std::vector<std::pair<uint32_t, uint32_t>> all_files_cache;
    if (wal_ != nullptr) {
      wal_->ListFiles(&all_files_cache);
    }
    if (GetSealedEpoch(epoch, &se)) {
      std::vector<std::pair<uint32_t, uint32_t>> uninstalled;
      for (const auto& [part, gen] : se.gens) {
        if (!IsGenInstalled(part, gen)) {
          uninstalled.emplace_back(part, gen);
        }
      }
      if (!uninstalled.empty()) {
        // 归属迁移（不改 pending/drained）；frozen 索引段保留 → 读仍可命中。
        // 索引段的最终释放发生在它真正被安装（MarkEpochInstalled）之后。
        ctx_stats_handoff_at_release_.fetch_add(1, std::memory_order_relaxed);
        std::unordered_map<uint32_t, uint64_t> per_part_bytes;
        for (const auto& [part, gen] : uninstalled) {
          per_part_bytes[part] +=
              wal_ != nullptr ? wal_->GetFileSize(part, gen, all_files_cache,
                                                  env_)
                              : 0;
        }
        sealed_cache_->HandOffSkippedToRecovery(epoch, uninstalled,
                                                per_part_bytes);
        ROCKS_LOG_WARN(wal_ != nullptr ? wal_->InfoLog() : nullptr,
                       "ZF epoch=%llu released with %zu uninstalled gens: "
                       "handed off to recovery (WAL retained, index kept)",
                       (unsigned long long)epoch, uninstalled.size());
      }
    }
  }
  ReleaseFrozenIndexes(epoch);
  if (sealed_cache_ != nullptr) {
    const uint64_t b = sealed_cache_->ReleaseEpoch(epoch);
    // ZeroFlushV0 P1b/M1a：epoch 回收是 DRAINING 分区的排空推进点——
    // 检查是否有分区已排空可 RETIRED。
    MaybeRetireDraining();
    // ZeroFlushV0 M4：split 孤儿尾 epoch 同点 retire（数据已进 SST）。
    MaybeRetireSplitOrphans();
    return b;
  }
  return 0;
}

size_t ZeroFlushContext::PurgeSealedFiles() {
  if (sealed_cache_ != nullptr) {
    return sealed_cache_->PurgePending();
  }
  return 0;
}

// ---- ZeroFlushV0 M4：动态治理 DG ----

// ---------------------------------------------------------------------------
// step 0b-3（重构）：pending 水位——单一权威 = SealedFileCache 的 generation
// registry。本层只转发，不再维护第二份所有权账目。
//   pending  : 仍需物化的 generation 字节（sealed/queued/building/conflict/
//              recovery 都算；只有 installed 移出）
//   drained  : LogAndApply 成功时增加（数据已进 L1）
//   reclaimed: 引用全部释放、物理可回收时增加
//   pending 归零 ≠ 文件已回收（旧读视图/snapshot 仍可能持有引用）。
// ---------------------------------------------------------------------------
uint64_t ZeroFlushContext::PendingMaterializeBytes() const {
  return sealed_cache_ != nullptr ? sealed_cache_->pending_materialize_bytes() : 0;
}
uint64_t ZeroFlushContext::DrainedMaterializeBytes() const {
  return sealed_cache_ != nullptr ? sealed_cache_->drained_materialize_bytes() : 0;
}
uint64_t ZeroFlushContext::HandoffInstalledFiltered() const {
  return sealed_cache_ != nullptr ? sealed_cache_->handoff_installed_filtered()
                                  : 0;
}
uint64_t ZeroFlushContext::ReclaimedBytes() const {
  return sealed_cache_ != nullptr ? sealed_cache_->reclaimed_bytes() : 0;
}
uint64_t ZeroFlushContext::PeakPendingBytes() const {
  return sealed_cache_ != nullptr ? sealed_cache_->peak_pending_bytes() : 0;
}
uint64_t ZeroFlushContext::InstalledEpochs() const {
  return sealed_cache_ != nullptr ? sealed_cache_->installed_epochs() : 0;
}
uint64_t ZeroFlushContext::ReclaimedGens() const {
  return sealed_cache_ != nullptr ? sealed_cache_->reclaimed_gens() : 0;
}
size_t ZeroFlushContext::RegisteredGens() const {
  return sealed_cache_ != nullptr ? sealed_cache_->registered_gens() : 0;
}
void ZeroFlushContext::MarkGensInstalled(
    const std::vector<std::pair<uint32_t, uint32_t>>& gens) {
  if (sealed_cache_ != nullptr) {
    sealed_cache_->MarkGensInstalled(gens);
  }
}

void ZeroFlushContext::MarkEpochInstalled(uint64_t epoch) {
  if (sealed_cache_ != nullptr) {
    sealed_cache_->MarkEpochInstalled(epoch);
  }
}

void ZeroFlushContext::NoteEmptyEdit(uint64_t pending_before, uint64_t lm_before,
                                    uint64_t lm_after) {
  const uint64_t pending_after = PendingMaterializeBytes();
  empty_edit_count_.fetch_add(1, std::memory_order_relaxed);
  empty_edit_pending_before_.store(pending_before, std::memory_order_relaxed);
  empty_edit_pending_after_.store(pending_after, std::memory_order_relaxed);
  empty_edit_lm_before_.store(lm_before, std::memory_order_relaxed);
  empty_edit_lm_after_.store(lm_after, std::memory_order_relaxed);
  if (pending_before == pending_after) {
    empty_edit_pending_unchanged_.fetch_add(1, std::memory_order_relaxed);
  }
}

void ZeroFlushContext::RegisterSealedEpoch(const SealedEpoch& se) {
  // @封存边界速率采样（不进写路径；封存为低频事件）。每分区本代封存字节 +
  // 当前时刻喂入速率 EWMA。随后做既有登记（收养 recovery/skip）。
  if (rate_tracker_ != nullptr) {
    const uint64_t now = env_ != nullptr ? env_->NowMicros() : 0;
    for (const auto& [pid, bytes] : se.part_bytes) {
      if (bytes > 0) {
        rate_tracker_->RecordSeal(pid, bytes, now);
      }
    }
  }
  // step 0b-3：注册本 epoch 新封存的 generations（幂等；收养/重复封存路径
  // 不会重复计账）。记账粒度 = 该分区本代封存字节（一次 freeze 一个分区一个
  // gen；part_bytes 是分区聚合值，按该分区在本 epoch 的 gen 数摊分）。
  {
    std::unordered_map<uint32_t, uint32_t> gens_per_part;
    for (const auto& [p, g] : se.gens) {
      (void)g;
      gens_per_part[p]++;
    }
    for (const auto& [p, g] : se.gens) {
      const auto bit = se.part_bytes.find(p);
      const uint64_t bytes =
          (bit != se.part_bytes.end() && gens_per_part[p] > 0)
              ? bit->second / gens_per_part[p]
              : 0;
      if (sealed_cache_ != nullptr) {
        sealed_cache_->RegisterGen(p, g, bytes);
      }
    }
  }
  if (sealed_cache_ != nullptr) {
    sealed_cache_->AddEpochWithRecoveryAdoption(se);
  }
}

bool ZeroFlushContext::HasPendingPartData(uint32_t part_id) const {
  // 与 MergePartitionLocked/SplitPartitionLocked 的 Busy 前置一致：
  // pending sealed WAL gens（含 skip/recovery 收养集合）或 frozen 分区索引。
  if (index_set_ != nullptr && index_set_->HasFrozen(part_id)) {
    return true;
  }
  if (sealed_cache_ != nullptr && sealed_cache_->HasPendingPartGens(part_id)) {
    return true;
  }
  return false;
}

void ZeroFlushContext::DrainingSnapshot(
    std::vector<DrainingPart>* out) const {
  out->clear();
  std::lock_guard<std::mutex> l(draining_mu_);
  out->reserve(draining_.size());
  for (const auto& d : draining_) {
    out->push_back(d);
  }
}

uint64_t ZeroFlushContext::sealed_bytes() const {
  return sealed_cache_ != nullptr ? sealed_cache_->sealed_bytes() : 0;
}

uint64_t ZeroFlushContext::skipped_bytes() const {
  return sealed_cache_ != nullptr ? sealed_cache_->skipped_bytes() : 0;
}

// ---------------------------------------------------------------------------
// M3.0：zf.* 统计指标（M3_DESIGN.md §13）
// ---------------------------------------------------------------------------

bool ZeroFlushContext::GetProperty(const std::string& prop,
                                   std::string* value) const {
  if (value == nullptr) {
    return false;
  }
  value->clear();
  if (prop == "rocksdb.zeroflush.epochs_sealed") {
    *value = std::to_string(epoch_counter());
  } else if (prop == "rocksdb.zeroflush.epochs_materialized") {
    *value = std::to_string(epochs_materialized());
  } else if (prop == "rocksdb.zeroflush.epochs_reclaimed") {
    *value = std::to_string(epochs_reclaimed());
  } else if (prop == "rocksdb.zeroflush.live_wal_bytes") {
    *value = std::to_string(live_wal_bytes());
  } else if (prop == "rocksdb.zeroflush.sealed_wal_bytes") {
    *value = std::to_string(sealed_bytes());
  } else if (prop == "rocksdb.zeroflush.materialize_micros") {
    *value = std::to_string(materialize_micros());
  } else if (prop == "rocksdb.zeroflush.index_parts_scanned") {
    *value = std::to_string(index_parts_scanned());
  } else if (prop == "rocksdb.zeroflush.read_short_read_retried") {
    *value = std::to_string(read_short_read_retried());
  } else if (prop == "rocksdb.zeroflush.read_readfail_propagated") {
    *value = std::to_string(read_readfail_propagated());
  } else if (prop == "rocksdb.zeroflush.read_readfail_fallen_back") {
    *value = std::to_string(read_readfail_fallen_back());
  } else if (prop.starts_with("rocksdb.zeroflush.describe_gen:") &&
             sealed_cache_ != nullptr) {
    unsigned p = 0, g = 0;
    sscanf(prop.c_str() + strlen("rocksdb.zeroflush.describe_gen:"), "%u:%u",
           &p, &g);
    *value = sealed_cache_->DescribeGen(p, g);
  } else if (prop == "rocksdb.zeroflush.pending_recomputed") {
    *value = std::to_string(sealed_cache_ != nullptr
                                ? sealed_cache_->PendingBytesRecomputed()
                                : 0);
  } else if (prop == "rocksdb.zeroflush.pending_drift_events") {
    *value = std::to_string(
        sealed_cache_ != nullptr ? sealed_cache_->pending_drift_events() : 0);
  } else if (prop == "rocksdb.zeroflush.forced_yield_batches") {
    *value = std::to_string(forced_yield_batches()) + "/" +
             std::to_string(forced_yield_drained_flat()) + "/" +
             std::to_string(forced_yield_pending_flat()) + " outputs=" +
             std::to_string(forced_yield_outputs());
  } else if (prop == "rocksdb.zeroflush.fence_no_committed_output") {
    *value = std::to_string(fence_no_committed_output());
  } else if (prop == "rocksdb.zeroflush.index_kept_uninstalled") {
    *value = std::to_string(index_kept_uninstalled());
  } else if (prop == "rocksdb.zeroflush.gen_release_before_install") {
    *value = std::to_string(sealed_cache_ != nullptr
                                ? sealed_cache_->gen_release_before_install()
                                : 0);
  } else if (prop == "rocksdb.zeroflush.handoff_at_release") {
    *value = std::to_string(handoff_at_release());
  } else if (prop == "rocksdb.zeroflush.gen_read_ref_total") {
    *value = std::to_string(sealed_cache_ != nullptr
                                ? sealed_cache_->gen_read_ref_total()
                                : 0);
  } else if (prop == "rocksdb.zeroflush.reclaim_deferred_by_reader") {
    *value = std::to_string(
        sealed_cache_ != nullptr ? sealed_cache_->reclaim_deferred_by_reader()
                                 : 0);
  } else if (prop == "rocksdb.zeroflush.acquire_rejected_retiring") {
    // step 1as：`retiring => no_new_ref` 的可观测证据（case8b 断言）。
    *value = std::to_string(sealed_cache_ != nullptr
                                ? sealed_cache_->acquire_rejected_retiring()
                                : 0);
  } else if (prop == "rocksdb.zeroflush.reclaim_pending_count") {
    *value = std::to_string(sealed_cache_ != nullptr
                                ? sealed_cache_->reclaim_pending_count()
                                : 0);
  } else if (prop == "rocksdb.zeroflush.assert_retiring_new_ref") {
    *value = std::to_string(sealed_cache_ != nullptr
                                ? sealed_cache_->assert_retiring_new_ref()
                                : 0);
  } else if (prop == "rocksdb.zeroflush.assert_reclaim_unsafe") {
    // 断言 `reclaimed => installed && refs == 0` 的反例计数（应恒为 0）。
    *value = std::to_string(
        sealed_cache_ != nullptr ? sealed_cache_->assert_reclaim_unsafe() : 0);
  } else if (prop == "rocksdb.zeroflush.retiring_count") {
    *value = std::to_string(sealed_cache_ != nullptr
                                ? sealed_cache_->retiring_count()
                                : 0);
  } else if (prop == "rocksdb.zeroflush.retry_read_view_needed") {
    // 因胜出代 retiring 而被拒 ⇒ 要求"刷新读视图后整次重试"的次数（step 1ar）。
    *value = std::to_string(
        zeroflush::get_trace::RetryReadViewNeeded().load(std::memory_order_relaxed));
  } else if (prop == "rocksdb.zeroflush.pending_unlink_count") {
    *value = std::to_string(sealed_cache_ != nullptr
                                ? sealed_cache_->pending_count()
                                : 0);
  } else if (prop == "rocksdb.zeroflush.reclaim_pending_list") {
    *value = sealed_cache_ != nullptr ? sealed_cache_->DescribeReclaimPending()
                                      : std::string();
  } else if (prop == "rocksdb.zeroflush.pending_gens") {
    // D-38 诊断：未安装的已注册代及其归属（回答"pending 为何不为 0"）。
    *value = sealed_cache_ != nullptr ? sealed_cache_->DescribeUninstalledGens()
                                      : std::string();
  } else if (prop.starts_with("rocksdb.zeroflush.gen_read_refs:") &&
             sealed_cache_ != nullptr) {
    unsigned p = 0, g = 0;
    sscanf(prop.c_str() + strlen("rocksdb.zeroflush.gen_read_refs:"), "%u:%u",
           &p, &g);
    *value = std::to_string(sealed_cache_->GenReadRefs(p, g));
  } else if (prop == "rocksdb.zeroflush.install_assert_file_missing") {
    *value = std::to_string(install_assert_file_missing());
  } else if (prop == "rocksdb.zeroflush.install_gens_withheld") {
    *value = std::to_string(install_gens_withheld());
  } else if (prop == "rocksdb.zeroflush.gen_handoff") {
    *value = std::to_string(gen_handoff());
  } else if (prop == "rocksdb.zeroflush.gen_lost_before_install") {
    *value = std::to_string(gen_lost_before_install());
  } else if (prop == "rocksdb.zeroflush.gen_unregistered_fd_fallback") {
    *value = std::to_string(gen_unregistered_fd_fallback());
  } else if (prop == "rocksdb.zeroflush.forbid_l0_install") {
    *value = std::to_string(zfo_.forbid_l0_install ? 1 : 0);
  } else if (prop == "rocksdb.zeroflush.l0_install_count") {
    *value = std::to_string(l0_install_count());
  } else if (prop == "rocksdb.zeroflush.l0_install_blocked") {
    *value = std::to_string(l0_install_blocked());
  } else if (prop == "rocksdb.zeroflush.wal_pin_acquired") {
    *value = std::to_string(wal_pin_acquired());
  } else if (prop == "rocksdb.zeroflush.wal_pin_active_unpinnable") {
    *value = std::to_string(wal_pin_active_unpinnable());
  } else if (prop == "rocksdb.zeroflush.read_union_overflow") {
    *value = std::to_string(read_union_overflow());
  } else if (prop == "rocksdb.zeroflush.active_wal_read_count") {
    *value = std::to_string(active_wal_read_count());
  } else if (prop == "rocksdb.zeroflush.sealed_read_count") {
    *value = std::to_string(sealed_read_count());
  } else if (prop == "rocksdb.zeroflush.sealed_cache_miss") {
    *value = std::to_string(sealed_cache_miss());
  } else if (prop == "rocksdb.zeroflush.recovery_count") {
    *value = std::to_string(recovery_count());
  } else if (prop == "rocksdb.zeroflush.partition_skew") {
    char buf[64];
    snprintf(buf, sizeof(buf), "%.4f", partition_skew());
    *value = buf;
  } else if (prop == "rocksdb.zeroflush.install_direct_base") {
    *value = std::to_string(install_direct_base());
  } else if (prop == "rocksdb.zeroflush.install_fallback_l0") {
    *value = std::to_string(install_fallback_l0());
  } else if (prop == "rocksdb.zeroflush.materialize_sort_micros") {
    *value = std::to_string(materialize_sort_micros());
  } else if (prop == "rocksdb.zeroflush.base_merge_count") {
    *value = std::to_string(base_merge_count());
  } else if (prop == "rocksdb.zeroflush.base_merge_rewritten_bytes") {
    *value = std::to_string(base_merge_rewritten_bytes());
  } else if (prop == "rocksdb.zeroflush.skip_count") {
    *value = std::to_string(skip_count());
  } else if (prop == "rocksdb.zeroflush.sink_requests") {
    *value = std::to_string(sink_request_count());
  } else if (prop == "rocksdb.zeroflush.pending_materialize_bytes") {
    *value = std::to_string(PendingMaterializeBytes());
  } else if (prop == "rocksdb.zeroflush.drained_materialize_bytes") {
    *value = std::to_string(DrainedMaterializeBytes());
  } else if (prop == "rocksdb.zeroflush.reclaimed_bytes") {
    *value = std::to_string(ReclaimedBytes());
  } else if (prop == "rocksdb.zeroflush.peak_pending_bytes") {
    *value = std::to_string(PeakPendingBytes());
  } else if (prop == "rocksdb.zeroflush.registered_gens") {
    *value = std::to_string(RegisteredGens());
  } else if (prop == "rocksdb.zeroflush.installed_epochs") {
    *value = std::to_string(InstalledEpochs());
  } else if (prop == "rocksdb.zeroflush.handoff_installed_filtered") {
    *value = std::to_string(HandoffInstalledFiltered());
  } else if (prop == "rocksdb.zeroflush.reclaimed_gens") {
    *value = std::to_string(ReclaimedGens());
  } else if (prop == "rocksdb.zeroflush.recovery_filtered_gens") {
    *value = std::to_string(recovery_filtered_gens());
  } else if (prop == "rocksdb.zeroflush.recovery_filtered_bytes") {
    *value = std::to_string(recovery_filtered_bytes());
  } else if (prop == "rocksdb.zeroflush.fence_advance_count") {
    *value = std::to_string(fence_advance_count());
  } else if (prop == "rocksdb.zeroflush.fifo_blocked") {
    *value = std::to_string(fifo_blocked());
  } else if (prop == "rocksdb.zeroflush.fifo_bypass") {
    *value = std::to_string(fifo_bypass());
  } else if (prop == "rocksdb.zeroflush.fence_hole_advance_count") {
    *value = std::to_string(fence_hole_advance_count());
  } else if (prop == "rocksdb.zeroflush.fence_gap_count") {
    *value = std::to_string(fence_gap_count());
  } else if (prop == "rocksdb.zeroflush.empty_edit_count") {
    *value = std::to_string(empty_edit_count());
  } else if (prop == "rocksdb.zeroflush.empty_edit_barrier_advance") {
    *value = std::to_string(empty_edit_barrier_advance());
  } else if (prop == "rocksdb.zeroflush.empty_edit_pending_before") {
    *value = std::to_string(empty_edit_pending_before());
  } else if (prop == "rocksdb.zeroflush.empty_edit_pending_after") {
    *value = std::to_string(empty_edit_pending_after());
  } else if (prop == "rocksdb.zeroflush.empty_edit_lm_before") {
    *value = std::to_string(empty_edit_lm_before());
  } else if (prop == "rocksdb.zeroflush.empty_edit_lm_after") {
    *value = std::to_string(empty_edit_lm_after());
  } else if (prop == "rocksdb.zeroflush.empty_edit_pending_unchanged") {
    *value = std::to_string(empty_edit_pending_unchanged());
  } else if (prop == "rocksdb.zeroflush.backpressure_waits") {
    *value = std::to_string(backpressure_waits());
  } else if (prop == "rocksdb.zeroflush.backpressure_timeouts") {
    *value = std::to_string(backpressure_timeouts());
  } else if (prop == "rocksdb.zeroflush.sink_diag") {
    // step 0a 诊断（临时）：sink 构造失败原因分类汇总，见 db/compaction/
    // zf_sink_diag.h。0a 结束后随该头文件一起删除。
    *value = ROCKSDB_NAMESPACE::zf_sink_diag::Summary();
  } else if (prop == "rocksdb.zeroflush.prof_scan_us") {
    *value = std::to_string(prof_scan_us_.load(std::memory_order_relaxed));
  } else if (prop == "rocksdb.zeroflush.prof_scan_bytes") {
    *value = std::to_string(prof_scan_bytes_.load(std::memory_order_relaxed));
  } else if (prop == "rocksdb.zeroflush.prof_build_us") {
    *value = std::to_string(prof_build_us_.load(std::memory_order_relaxed));
  } else if (prop == "rocksdb.zeroflush.prof_merge_us") {
    *value = std::to_string(prof_merge_us_.load(std::memory_order_relaxed));
  } else if (prop == "rocksdb.zeroflush.prof_output_bytes") {
    *value = std::to_string(prof_output_bytes_.load(std::memory_order_relaxed));
  } else if (prop == "rocksdb.zeroflush.prof_tasks") {
    *value = std::to_string(prof_tasks_.load(std::memory_order_relaxed));
  } else if (prop == "rocksdb.zeroflush.merges") {
    // ZeroFlushV0 P1：已成功合并（路由切换）的分区累计数。
    *value = std::to_string(MergeCount());
  } else if (prop == "rocksdb.zeroflush.splits") {
    // ZeroFlushV0 P2（M2）：已成功 split（路由切换）的分区累计数。
    *value = std::to_string(SplitCount());
  } else if (prop == "rocksdb.zeroflush.merge_retired") {
    // ZeroFlushV0 P1b/M1a：已 RETIRED（排空完成）的合并分区累计数。
    *value = std::to_string(MergeRetired());
  } else if (prop == "rocksdb.zeroflush.draining_partitions") {
    // ZeroFlushV0 P1b/M1a：当前 DRAINING（排空未完成）分区数。
    *value = std::to_string(DrainingPartitions());
  } else if (prop == "rocksdb.zeroflush.routing_version") {
    // ZeroFlushV0 P1：当前路由表版本（merge 每次切换 +1；重开经 ZFPROPS
    // 恢复）。诊断与重划分触发判据用。
    *value = std::to_string(tables_ ? tables_->current_version() : 0);
  } else if (prop == "rocksdb.zeroflush.partition_count") {
    // ZeroFlushV0 P1：当前路由表分区数（merge 后 -1；偏态重划分的
    // 分配/回收判据用）。
    auto cur = tables_ ? tables_->current() : nullptr;
    *value = std::to_string(cur != nullptr ? cur->partitions() : 0);
  } else if (prop == "rocksdb.zeroflush.rewrite_bytes") {
    // ZeroFlushV0 M4：type-3 rewrite 累计输出字节（治理带宽账本）。
    *value = std::to_string(rewrite_bytes());
  } else if (prop == "rocksdb.zeroflush.gov_ticks") {
    *value = std::to_string(governance_ticks());
  } else if (prop == "rocksdb.zeroflush.gov_splits") {
    *value = std::to_string(governance_splits());
  } else if (prop == "rocksdb.zeroflush.gov_merges") {
    *value = std::to_string(governance_merges());
  } else if (prop == "rocksdb.zeroflush.gov_in_flight") {
    *value = std::to_string(GovActionInFlight());
  } else if (prop == "rocksdb.zeroflush.gov_rewrite_gated") {
    *value = std::to_string(governance_rewrite_gated());
  } else if (prop == "rocksdb.zeroflush.gov_split_attempts") {
    *value = std::to_string(gov_split_attempts());
  } else if (prop == "rocksdb.zeroflush.gov_split_gate_base") {
    *value = std::to_string(gov_split_gate_base());
  } else if (prop == "rocksdb.zeroflush.gov_split_median_nofiles") {
    *value = std::to_string(gov_split_median_nofiles());
  } else if (prop == "rocksdb.zeroflush.gov_split_success") {
    *value = std::to_string(gov_split_success());
  } else if (prop == "rocksdb.zeroflush.gov_last_split_base") {
    *value = std::to_string(gov_last_split_base());
  } else if (prop == "rocksdb.zeroflush.gov_last_split_l0") {
    *value = std::to_string(gov_last_split_l0());
  } else if (prop == "rocksdb.zeroflush.gov_split_busy") {
    *value = std::to_string(gov_split_busy());
  } else if (prop == "rocksdb.zeroflush.gov_split_timeout") {
    *value = std::to_string(gov_split_timeout());
  } else if (prop == "rocksdb.zeroflush.gov_split_other") {
    *value = std::to_string(gov_split_other());
  } else if (prop == "rocksdb.zeroflush.gov_split_median_notfound") {
    *value = std::to_string(gov_split_median_notfound());
  } else if (prop == "rocksdb.zeroflush.gov_split_pos_gone") {
    *value = std::to_string(gov_split_pos_gone());
  } else if (prop == "rocksdb.zeroflush.gov_split_total0") {
    *value = std::to_string(gov_split_total0());
  } else if (prop == "rocksdb.zeroflush.gov_split_predrain") {
    *value = std::to_string(gov_split_predrain());
  } else if (prop == "rocksdb.zeroflush.index_insert_dropped") {
    *value = std::to_string(index_set_ != nullptr
                                ? index_set_->insert_dropped()
                                : 0);
  } else if (prop == "rocksdb.zeroflush.bloom_slowpath") {
    // M5：布隆假阴性慢路径次数（Get 布隆 miss 时改查索引链；正确性兜底）。
    *value = std::to_string(index_set_ != nullptr
                                ? index_set_->bloom_slowpath()
                                : 0);
  } else if (prop == "rocksdb.zeroflush.inflight_groups") {
    // 诊断：在途写组计数（治理路由切换屏障等待归零；泄漏会导致所有
    // merge/split 动作 TimedOut）。
    *value = std::to_string(InflightWriterGroups());
  } else {
    return false;
  }
  return true;
}

uint64_t ZeroFlushContext::epochs_materialized() const {
  return sealed_cache_ != nullptr ? sealed_cache_->materialized_epochs() : 0;
}

uint64_t ZeroFlushContext::epochs_reclaimed() const {
  return sealed_cache_ != nullptr ? sealed_cache_->reclaimed_epochs() : 0;
}

uint64_t ZeroFlushContext::live_wal_bytes() const {
  uint64_t sum = 0;
  const auto ids = wal_->AllPartitionIds();
  for (uint32_t pid : ids) {
    sum += wal_->ActiveSize(pid);
  }
  return sum;
}

uint64_t ZeroFlushContext::materialize_micros() const {
  return sealed_cache_ != nullptr ? sealed_cache_->materialize_micros() : 0;
}

uint64_t ZeroFlushContext::sealed_read_count() const {
  return sealed_cache_ != nullptr ? sealed_cache_->sealed_read_count() : 0;
}

uint64_t ZeroFlushContext::sealed_cache_miss() const {
  return sealed_cache_ != nullptr ? sealed_cache_->sealed_cache_miss() : 0;
}

size_t ZeroFlushContext::recovery_count() const {
  return sealed_cache_ != nullptr ? sealed_cache_->recovery_count() : 0;
}

bool ZeroFlushContext::HasPendingSealedData() const {
  if (sealed_cache_ != nullptr && sealed_cache_->HasPendingGens()) {
    return true;
  }
  // 未物化 epoch（imm 中的封存代）也须冲刷——recovery_count 只覆盖
  // 崩溃孤儿，skip 攒批与 imm 由调用方（DBImpl::Close）分别处理：
  // 本查询只回答"收养集合是否有货"；imm 存在性由调用方经
  // pending_epochs(cfd) 判断。
  return false;
}

double ZeroFlushContext::partition_skew() const {
  // 路由倾斜：max(ActiveSize) / avg(ActiveSize)。所有分区都为空时返回 0
  // （无倾斜可度量）；单分区活跃字节为 0 时按 1 计，避免除零。
  // M3.1：遍历实际的 part_id 集合。
  uint64_t sum = 0, max_one = 0, nonzero = 0;
  const auto ids = wal_->AllPartitionIds();
  for (uint32_t pid : ids) {
    const uint64_t sz = wal_->ActiveSize(pid);
    sum += sz;
    if (sz > max_one) max_one = sz;
    if (sz > 0) ++nonzero;
  }
  if (sum == 0 || nonzero == 0) {
    return 0.0;
  }
  const uint32_t n = static_cast<uint32_t>(ids.size());
  const double avg = n > 0 ? static_cast<double>(sum) / n : 0.0;
  return avg > 0.0 ? static_cast<double>(max_one) / avg : 0.0;
}

// ---------------------------------------------------------------------------
// M3.2：物化状态与统计访问器（M3_DESIGN.md §6/§13）
// ---------------------------------------------------------------------------

bool ZeroFlushContext::GetSealedEpoch(uint64_t epoch, SealedEpoch* out) const {
  return sealed_cache_ != nullptr && sealed_cache_->GetEpoch(epoch, out);
}

uint64_t ZeroFlushContext::last_materialized_epoch() const {
  return last_materialized_epoch_.load(std::memory_order_acquire);
}

void ZeroFlushContext::SetLastMaterializedEpoch(uint64_t e) {
  // M5P1b（多 flush 并行）：并发批推进不可回退——store 改 max 单调。
  // 失败批的"回滚到 last_before"调用在并发推进后自动 no-op（last 只降
  // 不升的反向操作被忽略）。数据侧安全：last 仅作批首 epoch 种子/按序
  // 断言的辅助，物化/释放以 imm 生命周期为准（失败批 imm 保留在队列，
  // 重试批次 first_epoch 放行不受 last 影响）。
  uint64_t cur = last_materialized_epoch_.load(std::memory_order_relaxed);
  while (e > cur &&
         !last_materialized_epoch_.compare_exchange_weak(
             cur, e, std::memory_order_release, std::memory_order_relaxed)) {
  }
}

uint64_t ZeroFlushContext::BeginMaterializeBatch() {
  std::lock_guard<std::mutex> l(mm_mu_);
  uint64_t token = mm_token_next_++;
  if (token == 0) {
    // 自增溢出回绕（不可达）：跳 0（0 = 不参与标记），从 1 重新计数。
    token = mm_token_next_ = 1;
  }
  mm_batch_parts_.emplace(token, std::unordered_set<uint32_t>());
  return token;
}

bool ZeroFlushContext::AddMaterializePart(uint64_t token, uint32_t part_id) {
  if (token == 0) {
    return true;  // 不参与跨批互斥（单 flush 串行等场景）
  }
  std::lock_guard<std::mutex> l(mm_mu_);
  auto it = mm_batch_parts_.find(token);
  if (it == mm_batch_parts_.end()) {
    // token 已 End（理论不可达——登记只在批活跃窗口内）：
    // 放行（不登记），避免死锁式拒绝。
    return true;
  }
  if (it->second.count(part_id) != 0) {
    return true;  // 本批已登记（幂等）
  }
  if (mm_active_parts_.count(part_id) != 0) {
    return false;  // 他批在产
  }
  it->second.insert(part_id);
  mm_active_parts_.insert(part_id);
  return true;
}

void ZeroFlushContext::EndMaterializeBatch(uint64_t token) {
  if (token == 0) {
    return;
  }
  std::lock_guard<std::mutex> l(mm_mu_);
  auto it = mm_batch_parts_.find(token);
  if (it == mm_batch_parts_.end()) {
    return;
  }
  for (uint32_t pid : it->second) {
    mm_active_parts_.erase(pid);
  }
  mm_batch_parts_.erase(it);
}

size_t ZeroFlushContext::ActiveMaterializeParts() const {
  std::lock_guard<std::mutex> l(mm_mu_);
  return mm_active_parts_.size();
}

// ---- M4.9：安装窗口串行化（zf_install_mu_）----
// 非阻塞获取：他批在飞 → 本批整批让位（调用方负责 gens 移交 recovery）。
// 持锁方在安装（TryInstall → LogAndApply）返回后释放；同一 FlushJob::Run
// 线程内成对出现，不跨线程。
// ZF_INSTALL_LOCK=0 可关闭（回退/对照实验用；默认开启）。
bool ZeroFlushContext::AcquireInstallLock() {
  // 默认开启（2026-09-14 决策）：死锁已修复——前序门保证持锁者不会在
  // TryInstall 里等待别人，等待前序时释放 gen0 槽避免"持槽等前序"自锁
  // （gdb 实证见 output/zeroflush_m5_perf/materialize_lag/
  //  PROTOCOL_FIX_ATTEMPT.md 附录 B）。
  // 验证：原 M4.9 崩溃配置（256 分区/16T/16 轮）3/3 通过、4M 写 13.5s
  // 零让位、zf_test ALL PASSED。
  // ZF_INSTALL_LOCK=0 可关闭（回退/对照实验用）。
  static const bool zf_install_lock_enabled = [] {
    const char* v = getenv("ZF_INSTALL_LOCK");
    return v == nullptr || std::string(v) != "0";
  }();
  if (!zf_install_lock_enabled) {
    return true;  // 关闭：不做串行化（仅回退/对照用）
  }
  install_lock_held_ = install_mu_.try_lock();
  if (install_lock_held_) {
    std::lock_guard<std::mutex> l(install_cv_mu_);
    install_mu_free_ = false;
  }
  return install_lock_held_;
}

void ZeroFlushContext::ReleaseInstallLock() {
  if (install_lock_held_) {
    install_lock_held_ = false;
    install_mu_.unlock();
    {
      std::lock_guard<std::mutex> l(install_cv_mu_);
      install_mu_free_ = true;
    }
    install_cv_.notify_all();
    NotifyInstallProgress();  // 安装完成/取消 → 唤醒所有等待方重新准入
  }
}

uint64_t ZeroFlushContext::InstallProgressEpoch() const {
  return install_progress_epoch_.load(std::memory_order_acquire);
}

void ZeroFlushContext::NotifyInstallProgress() {
  install_progress_epoch_.fetch_add(1, std::memory_order_acq_rel);
  installer_progress_cv_.notify_all();
}

bool ZeroFlushContext::WaitInstallProgress(uint64_t observed,
                                           uint64_t timeout_us) {
  std::unique_lock<std::mutex> l(install_progress_mu_);
  installer_progress_cv_.wait_for(
      l, std::chrono::microseconds(timeout_us), [&] {
        return install_progress_epoch_.load(std::memory_order_acquire) !=
               observed;
      });
  return install_progress_epoch_.load(std::memory_order_acquire) != observed;
}

bool ZeroFlushContext::TryAcquireGen0Slot() {
  std::lock_guard<std::mutex> l(g0_mu_);
  if (g0_running_ != 0) {
    return false;  // 学习批在飞：不等待，调用方释放资源后重试
  }
  ++g0_running_;
  ++g0_pending_;
  return true;
}

bool ZeroFlushContext::IsGen0WindowClosed() const {
  std::lock_guard<std::mutex> l(g0_mu_);
  return g0_pending_ == 0 && g0_waiters_ == 0;
}

void ZeroFlushContext::WaitInstallRelease(uint64_t max_wait_us) {
  std::unique_lock<std::mutex> l(install_cv_mu_);
  install_cv_.wait_for(l, std::chrono::microseconds(max_wait_us),
                       [this] { return install_mu_free_; });
}

void ZeroFlushContext::WaitForInstallLock() {
  // 阻塞直到拿到 install 锁。语义：调用点（规划之前、gen0 等待之后）已无
  // 其他依赖，持锁方必然完成安装并释放；本函数不改动本批的任何数据状态
  // （不移交 gens、不放弃计划），因此不会产生"让位→收养→再让位"循环。
  std::unique_lock<std::mutex> l(install_cv_mu_);
  install_cv_.wait(l, [this] { return install_mu_free_; });
  install_mu_.lock();
  install_lock_held_ = true;
  install_mu_free_ = false;
}

// ---- M5P1b：学习窗 gen0 批串行槽 ----
void ZeroFlushContext::AcquireGen0Slot() {
  std::unique_lock<std::mutex> l(g0_mu_);
  ++g0_waiters_;
  g0_cv_.wait(l, [this] { return g0_running_ == 0; });
  --g0_waiters_;
  ++g0_running_;
  ++g0_pending_;
  g0_cv_.notify_all();  // 稳态批可能在等 waiters==0
}
void ZeroFlushContext::WaitGen0WindowClosed() {
  std::unique_lock<std::mutex> l(g0_mu_);
  // 学习批优先：等全部学习批（在飞 + 等待取槽的）结束后稳态批才规划。
  g0_cv_.wait(l, [this] { return g0_pending_ == 0 && g0_waiters_ == 0; });
}
void ZeroFlushContext::ReleaseGen0Slot() {
  {
    std::lock_guard<std::mutex> l(g0_mu_);
    --g0_running_;
    --g0_pending_;
    g0_cv_.notify_all();
  }
  NotifyInstallProgress();  // 槽释放也是准入条件变化 → 唤醒等待方
}

uint64_t ZeroFlushContext::install_direct_base() const {
  return install_direct_base_.load(std::memory_order_relaxed);
}

uint64_t ZeroFlushContext::install_fallback_l0() const {
  return install_fallback_l0_.load(std::memory_order_relaxed);
}

uint64_t ZeroFlushContext::materialize_sort_micros() const {
  return materialize_sort_micros_.load(std::memory_order_relaxed);
}

uint64_t ZeroFlushContext::base_merge_count() const {
  return base_merge_count_.load(std::memory_order_relaxed);
}

uint64_t ZeroFlushContext::base_merge_rewritten_bytes() const {
  return base_merge_rewritten_bytes_.load(std::memory_order_relaxed);
}

uint64_t ZeroFlushContext::skip_count() const {
  return skip_count_.load(std::memory_order_relaxed);
}

void ZeroFlushContext::RequestRangeSink(
    const ROCKSDB_NAMESPACE::Slice& lo, const ROCKSDB_NAMESPACE::Slice& hi) {
  // 去重：同 range（lo/hi 全同）已有未消费请求则不重复入队——判据 3 每
  // epoch 都会对同一"满文件 range"重发请求，下沉完成前重复入队只会让
  // 消费点反复构造同一 compaction（首条成功后其余因 being_compacted
  // 构造失败丢弃——正确但浪费）。相邻 range 边界互斥（静态分区），
  // 无相交合并需求。lo/hi 必须拷贝：调用方的 Slice 指向路由表内部
  // 存储，表换代后悬垂。
  std::lock_guard<std::mutex> l(sink_mu_);
  const std::string lo_s(lo.data(), lo.size());
  const std::string hi_s(hi.data(), hi.size());
  for (const auto& r : sink_requests_) {
    if (r.first == lo_s && r.second == hi_s) {
      sink_request_count_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
  }
  sink_requests_.emplace_back(std::move(lo_s), std::move(hi_s));
  sink_request_count_.fetch_add(1, std::memory_order_relaxed);
}

std::vector<std::pair<std::string, std::string>>
ZeroFlushContext::TakeSinkRequests() {
  std::lock_guard<std::mutex> l(sink_mu_);
  std::vector<std::pair<std::string, std::string>> out;
  out.swap(sink_requests_);
  return out;
}

uint64_t ZeroFlushContext::sink_request_count() const {
  return sink_request_count_.load(std::memory_order_relaxed);
}

uint32_t ZeroFlushContext::pending_epochs(
    ROCKSDB_NAMESPACE::ColumnFamilyData* cfd) const {
  if (cfd == nullptr) return 0;
  // imm_ 大小即未物化 epoch 数（每个 imm 对应一次 SealEpochAndSwitch）。
  return static_cast<uint32_t>(cfd->imm()->NumNotFlushed());
}

std::shared_ptr<PartitionTable> ZeroFlushContext::current_table() const {
  return tables_ ? tables_->current() : nullptr;
}

uint32_t ZeroFlushContext::Route(const ROCKSDB_NAMESPACE::Slice& user_key) const {
  // M3.1：委派给当前 PartitionTable。
  auto tbl = current_table();
  if (tbl) {
    return tbl->Route(user_key);
  }
  // 回退：PartitionTable 尚未初始化时使用 hash（首次 Open 恢复路径）。
  uint32_t h = ROCKSDB_NAMESPACE::Hash(user_key.data(), user_key.size(), 0);
  return h % zfo_.partitions;
}

uint32_t ZeroFlushContext::RouteWithVersion(
    const ROCKSDB_NAMESPACE::Slice& user_key, uint32_t table_version) const {
  // M4.10：按指定表版本路由（写组绑定——封存换表期间组内版本一致，
  // 数据路由与物化（se.table_version）用同一表 → 范围断言一致）。
  auto tbl = tables_ ? tables_->Get(table_version) : nullptr;
  if (tbl) {
    return tbl->Route(user_key);
  }
  uint32_t h = ROCKSDB_NAMESPACE::Hash(user_key.data(), user_key.size(), 0);
  return h % zfo_.partitions;
}

uint32_t ZeroFlushContext::RoutePartId(
    const ROCKSDB_NAMESPACE::Slice& user_key) const {
  auto tbl = current_table();
  if (tbl && !tbl->IsHashMode()) {
    return tbl->part_id(tbl->Route(user_key));
  }
  // hash / 未初始化：下标即 id。
  return Route(user_key);
}

uint32_t ZeroFlushContext::RoutePartIdWithVersion(
    const ROCKSDB_NAMESPACE::Slice& user_key, uint32_t table_version) const {
  auto tbl = tables_ ? tables_->Get(table_version) : nullptr;
  if (tbl && !tbl->IsHashMode()) {
    return tbl->part_id(tbl->Route(user_key));
  }
  uint32_t h = ROCKSDB_NAMESPACE::Hash(user_key.data(), user_key.size(), 0);
  return h % zfo_.partitions;
}

ROCKSDB_NAMESPACE::Status ZeroFlushContext::AddRecord(
    ROCKSDB_NAMESPACE::MemTable* mem, const ROCKSDB_NAMESPACE::Slice& key,
    const ROCKSDB_NAMESPACE::Slice& value, ROCKSDB_NAMESPACE::ValueType type,
    ROCKSDB_NAMESPACE::SequenceNumber seq,
    ROCKSDB_NAMESPACE::MemTablePostProcessInfo* ppi,
    uint32_t table_version) {
  // 1) 路由 + 分区 WAL 追加（value 的唯一持久副本）
  const uint64_t wg_t0 = env_->NowMicros();
  // ZeroFlushV0 P1b：路由返回下标，此处映射为实际分区 id——merge 后
  // part_ids 稀疏（如 [0,2,3]），下标≠id，按 id 记账（WAL/索引/locator）。
  const uint32_t part =
      (type == ROCKSDB_NAMESPACE::kTypeRangeDeletion)
          ? kRangeDelPartId
          : RoutePartIdWithVersion(key, table_version);
  const uint64_t wg_t1 = env_->NowMicros();
  WalRecordRef ref;
  ROCKSDB_NAMESPACE::Status s =
      wal_->Append(part, key, value, static_cast<uint8_t>(type), seq, &ref);
  if (!s.ok()) {
    return s;
  }
  const uint64_t wg_t2 = env_->NowMicros();
  // 2) 索引插入
  SlimLocator loc;
  loc.part_id = ref.part_id;
  loc.gen = ref.gen;
  loc.wal_offset = ref.offset;
  const ROCKSDB_NAMESPACE::Slice loc_slice(reinterpret_cast<const char*>(&loc),
                                           sizeof(loc));
  // M4.3a/4.4b：终态路径——写分区索引（跳表，无 MemTable 外壳）。
  // 数据源仍是 WAL（AddRecord 先 Append）；分区索引提供未封存/未物化
  // 窗口的读。封存时冻结（FreezeIndexes），物化完成时释放
  // （ReleaseFrozenIndexes）——frozen 索引指向封存 WAL（SealedFileCache
  // 可读），物化后数据进 SST、Get 走原生 SST 查找。M4.4b：旧路径
  // （MemTable 外壳）已移除。
  // P4：复用 thread_local 编码缓冲（每记录堆分配的 string 是写路径
  // insert 段（14-18us/op）的固定浪费；Insert 同步完成，缓冲复用安全）。
  thread_local std::string ik_buf;
  ik_buf.clear();
  ik_buf.append(key.data(), key.size());
  ROCKSDB_NAMESPACE::PutFixed64(
      &ik_buf, ROCKSDB_NAMESPACE::PackSequenceAndType(
                   seq, static_cast<ROCKSDB_NAMESPACE::ValueType>(type)));
  const rocksdb::Slice ik(ik_buf);
  index_set_->Insert(part, ref.gen, ik, loc_slice);
  const uint64_t wg_t3 = env_->NowMicros();
  if (wg_t3 - wg_t0 > 200) {
    static std::atomic<uint64_t> zf_wg_dbg {0};
    if (zf_wg_dbg.fetch_add(1, std::memory_order_relaxed) < 64) {
      fprintf(stderr,
              "ZFDBG-slowop route_us=%llu wal_us=%llu ins_us=%llu part=%u "
              "gen=%u\n",
              (unsigned long long)(wg_t1 - wg_t0),
              (unsigned long long)(wg_t2 - wg_t1),
              (unsigned long long)(wg_t3 - wg_t2), part, ref.gen);
    }
  }
  // M3.1：kSampled 模式下记录采样（仅 epoch 1 学习期）。
  if (zfo_.routing_mode == ZeroFlushOptions::RoutingMode::kSampled &&
      sampler_ && epoch_counter_.load() == 0) {
    sampler_->Sample(key);
  }
  return ROCKSDB_NAMESPACE::Status::OK();
}

// M4.3d-1：批次封存——一次 epoch 冻结多个分区（超限优先，补充最大分区
// 至批次上限）。物化按 gens 循环（现有 ZfMaterializeJob 支持多分区并行
// 分片）——作业数 = 批次数，消除单分区小作业的固定开销。
ROCKSDB_NAMESPACE::Status ZeroFlushContext::FreezeBatchPartitions(
    ROCKSDB_NAMESPACE::DBImpl* impl,
    ROCKSDB_NAMESPACE::ColumnFamilyData* cfd) {
  assert(impl != nullptr);
  assert(cfd != nullptr);
  // 必须在 write thread 持 DB mutex 下调用。
  // ---- 选批次：超限分区优先 + 补充最大分区 ----
  const uint32_t kMaxBatch = 4;  // 批次分区数上限（调优参数）
  std::vector<uint32_t> batch;
  uint64_t batch_bytes = 0;
  const auto ids = wal_->AllPartitionIds();
  for (uint32_t p : ids) {
    const uint64_t sz = wal_->ActiveSize(p);
    if (sz >= zfo_.partition_target_bytes) {
      batch.push_back(p);
      batch_bytes += sz;
    }
  }
  if (batch.size() < kMaxBatch && batch_bytes < zfo_.epoch_target_bytes) {
    // 补充最大分区（未超限的），直到批次上限或字节目标。
    std::vector<std::pair<uint64_t, uint32_t>> rest;
    for (uint32_t p : ids) {
      if (std::find(batch.begin(), batch.end(), p) != batch.end()) {
        continue;
      }
      const uint64_t sz = wal_->ActiveSize(p);
      if (sz > 0) {
        rest.emplace_back(sz, p);
      }
    }
    std::sort(rest.begin(), rest.end(), std::greater<>());
    for (const auto& [sz, p] : rest) {
      if (batch.size() >= kMaxBatch ||
          batch_bytes >= zfo_.epoch_target_bytes) {
        break;
      }
      batch.push_back(p);
      batch_bytes += sz;
    }
  }
  if (batch.empty()) {
    wal_->ClearOverTargetFlag();
    return ROCKSDB_NAMESPACE::Status::OK();  // 无数据可封存
  }
  // ---- 一次 epoch 冻结整批 ----
  const uint64_t epoch = epoch_counter_.fetch_add(1) + 1;
  SealedEpoch se;
  se.epoch = epoch;
  se.table_version = tables_ ? tables_->current_version() : 0;
  // M3.1/M4.2b：采样学习 / L1 对齐（批次封存路径的接线缺口，R49——
  // 融合从未启用的根因）。epoch 1 封存时学习安装新表（版本 1+）；本
  // epoch 用旧表写/物化（写组表版本绑定保证路由一致）。
  if (zfo_.routing_mode == ZeroFlushOptions::RoutingMode::kSampled &&
      sampler_ && tables_ && tables_->current_version() == 0 &&
      epoch == 1 && !sampler_->empty()) {
    std::vector<std::string> boundaries;
    if (sampler_->BuildBoundaries(zfo_.partitions, &boundaries)) {
      std::shared_ptr<PartitionTable> new_table;
      rocksdb::Status cps = PartitionTable::Create(
          1, std::move(boundaries), ucmp_, &new_table);
      if (cps.ok()) {
        tables_->InstallNewVersion(std::move(new_table));
      }
    }
  } else if (zfo_.routing_mode == ZeroFlushOptions::RoutingMode::kAlignL1 &&
             tables_) {
    std::shared_ptr<PartitionTable> new_table;
    if (!BuildBaseAlignedTable(cfd, &new_table) && sampler_ &&
        tables_->current_version() == 0 && epoch == 1 &&
        !sampler_->empty()) {
      std::vector<std::string> boundaries;
      if (sampler_->BuildBoundaries(zfo_.partitions, &boundaries)) {
        rocksdb::Status cps = PartitionTable::Create(
            1, std::move(boundaries), ucmp_, &new_table);
        if (!cps.ok()) {
          new_table.reset();
        }
      }
    }
    if (new_table != nullptr) {
      tables_->InstallNewVersion(std::move(new_table));
    }
  }
  for (uint32_t p : batch) {
    const FreezeResult fr = wal_->Freeze(p);
    // step 1q（用户 2026-09-16 第 2 项）：Freeze 即登记——该代进入唯一
    // registry（sealed && !installed ⇒ 计入 pending）；此后 epoch 收养只是
    // 归属迁移。避免已封存但无主的代被 ReleaseGens 当可回收，也避免让位时
    // 无人接管（组 5 NotFound 的根因）。
    if (sealed_cache_ != nullptr && fr.sealed_bytes > 0) {
      sealed_cache_->RegisterGen(p, fr.old_gen, fr.sealed_bytes);
    }
    // 新活跃索引的 gen = 新 WAL 代（old_gen + 1）——否则 freeze 后写组的
    // 记录（ref.gen = 新代）Insert 时找不到匹配索引被丢弃（数据丢失）。
    if (fr.sealed_bytes > 0) {
      index_set_->Freeze(p, fr.old_gen + 1);
    }  // step 1ab：封存中止（不可见）时不换代
    if (fr.sealed_bytes > 0) {
      se.gens.emplace_back(p, fr.old_gen);
      se.part_bytes[p] = fr.sealed_bytes;
      se.total_bytes += fr.sealed_bytes;
    }
  }
  wal_->ClearOverTargetFlag();
  RegisterSealedEpoch(se);
  // 空 mem 切换 → imm → FlushJob 触发该 epoch 的物化（多分区 gens）。
  ROCKSDB_NAMESPACE::MemTable* old_mem = cfd->mem();
  if (old_mem != nullptr) {
    old_mem->SetZfEpoch(epoch);
  }
  return impl->ZfSwitchMemtable(cfd);
}

// M4.3c：终态路径的单分区封存。目标分区：超限（≥ partition_target）优先，
// 否则取活跃字节最大者（epoch_target 全局触发时按最大分区分批收敛）。
ROCKSDB_NAMESPACE::Status ZeroFlushContext::FreezeOnePartition(
    ROCKSDB_NAMESPACE::DBImpl* impl,
    ROCKSDB_NAMESPACE::ColumnFamilyData* cfd) {
  assert(impl != nullptr);
  assert(cfd != nullptr);
  // 必须在 write thread 持 DB mutex 下调用。
  // ---- 选目标分区 ----
  uint32_t target = UINT32_MAX;
  uint64_t max_bytes = 0;
  for (uint32_t p : wal_->AllPartitionIds()) {
    const uint64_t sz = wal_->ActiveSize(p);
    if (sz >= zfo_.partition_target_bytes) {
      target = p;  // 超限优先（单分区满即触发）
      max_bytes = sz;
      break;
    }
    if (sz > max_bytes) {
      max_bytes = sz;
      target = p;
    }
  }
  if (target == UINT32_MAX || max_bytes == 0) {
    wal_->ClearOverTargetFlag();
    return ROCKSDB_NAMESPACE::Status::OK();  // 无数据可封存
  }
  // ---- 单分区冻结 ----
  const uint64_t epoch = epoch_counter_.fetch_add(1) + 1;
  const FreezeResult fr = wal_->Freeze(target);
    // step 1q（用户 2026-09-16 第 2 项）：Freeze 即登记——该代进入唯一
    // registry（sealed && !installed ⇒ 计入 pending）；此后 epoch 收养只是
    // 归属迁移。避免已封存但无主的代被 ReleaseGens 当可回收，也避免让位时
    // 无人接管（组 5 NotFound 的根因）。
    if (sealed_cache_ != nullptr && fr.sealed_bytes > 0) {
      sealed_cache_->RegisterGen(target, fr.old_gen, fr.sealed_bytes);
    }
  index_set_->Freeze(target, fr.old_gen);
  SealedEpoch se;
  se.epoch = epoch;
  se.table_version = tables_ ? tables_->current_version() : 0;
  if (fr.sealed_bytes > 0) {
    se.gens.emplace_back(target, fr.old_gen);
    // M5：自身新 gen0 标记（学习批 hash 数据分批封存中）。
    if (fr.old_gen == 0) {
      se.has_fresh_gen0 = true;
    }
    se.part_bytes[target] = fr.sealed_bytes;
    se.total_bytes = fr.sealed_bytes;
  }
  // 清超限标志：该分区已换代（新代从 0 起）；其他分区超限由 Append 的
  // 原子检查重新置位（自愈，最多延迟一个写组）。
  wal_->ClearOverTargetFlag();
  RegisterSealedEpoch(se);
  // 空 mem 切换 → imm → FlushJob 触发该 epoch 的物化（单分区 gens）。
  ROCKSDB_NAMESPACE::MemTable* old_mem = cfd->mem();
  if (old_mem != nullptr) {
    old_mem->SetZfEpoch(epoch);
  }
  return impl->ZfSwitchMemtable(cfd);
}

// M4.5b：跳过分区的封存 WAL 移交 recovery 集合（攒批）。
void ZeroFlushContext::HandOffSkippedToRecovery(
    uint64_t epoch, const std::vector<std::pair<uint32_t, uint32_t>>& gens) {
  if (sealed_cache_ == nullptr || gens.empty()) {
    return;
  }
  // 取该 epoch 的 per-partition 封存字节（ratio 攒批合并用）。
  SealedEpoch se;
  std::unordered_map<uint32_t, uint64_t> part_bytes;
  bool se_ok = GetSealedEpoch(epoch, &se);
  if (se_ok) {
    for (const auto& [part, gen] : gens) {
      auto pb = se.part_bytes.find(part);
      if (pb != se.part_bytes.end()) {
        part_bytes[part] = pb->second;
      }
    }
  }
  static std::atomic<uint64_t> zf_ctxho_dbg {0};
  if (zf_ctxho_dbg.fetch_add(1, std::memory_order_relaxed) < 100) {
    fprintf(stderr,
            "ZFDBG-ctxhandoff epoch=%llu gens=%zu se_ok=%d se_pb=%zu out_pb=%zu\n",
            (unsigned long long)epoch, gens.size(), (int)se_ok,
            se.part_bytes.size(), part_bytes.size());
  }
  // step 0b-3：移交只是状态迁移（gens 移出 epoch、进入 recovery/skip 集合）
  // ——pending 总量不变、不重复计账（cache 为单一权威）。
  sealed_cache_->HandOffSkippedToRecovery(epoch, gens, part_bytes);
}

// M4.3a：封存时冻结全部分区索引（全局 epoch 粒度；M4.3c 改单分区触发）。
void ZeroFlushContext::FreezeIndexes(
    const std::vector<std::pair<uint32_t, uint32_t>>& gens) {
  for (const auto& [part, gen] : gens) {
    // 换代后新代号 = gen + 1（wal_manager::Freeze 内 ++p->gen）。
    index_set_->Freeze(part, gen + 1);
  }
}

// M4.3a：物化完成（epoch 回收）时释放该 epoch 的 frozen 索引。
// 数据已进 SST，Get 走原生 SST 查找；索引释放回收内存。
void ZeroFlushContext::ReleaseFrozenIndexes(uint64_t epoch) {
  SealedEpoch se;
  if (!GetSealedEpoch(epoch, &se)) {
    return;
  }
  for (const auto& [part, gen] : se.gens) {
    // step 1t（用户 2026-09-16）：**未安装的代不得撤除索引段**——段是读依赖
    // 的唯一载体（撤除后索引 miss → 唯一副本在 WAL 也读不到 = NotFound，
    // 实证 group5 失败键所在代：已封存未安装、registry/索引全丢）。
    // 只有已安装（数据在 LSM）才允许撤除。
    if (!IsGenInstalled(part, gen)) {
      index_kept_uninstalled_.fetch_add(1, std::memory_order_relaxed);
      continue;
    }
    // M5 诊断：frozen 索引释放（Concurrent 用例定位——释放后该代数据
    // 必须已在 SST）。part3 全打（关键分区）。
    if (part == 3) {
      fprintf(stderr, "ZFDBG-rfi epoch=%llu part=%u gen=%u\n",
              (unsigned long long)epoch, part, gen);
    } else {
      static std::atomic<uint64_t> zf_rfi_dbg {0};
      if (zf_rfi_dbg.fetch_add(1, std::memory_order_relaxed) < 8) {
        fprintf(stderr, "ZFDBG-rfi epoch=%llu part=%u gen=%u\n",
                (unsigned long long)epoch, part, gen);
      }
    }
    index_set_->ReleaseFrozen(part, gen);
  }
}

// M4.3a：Get 查分区索引（替代 mem/imm 链）。
// 命中返回 true（含 tombstone：type 为 Deletion 时置 NotFound）；未命中
// 返回 false（调用方继续走原生 SST 查找）。
static void ZfPinThunk(void* c, uint32_t part, uint32_t gen) {
  auto* slot = static_cast<ZeroFlushContext::ZfPinSlot*>(c);
  if (slot == nullptr) return;
  slot->part = part;
  slot->gen = gen;
  slot->pinned = false;
  slot->is_active = false;
  if (slot->ctx == nullptr) return;
  auto* wal = slot->ctx->wal();
  const std::optional<uint32_t> ag =
      wal != nullptr ? wal->ActiveGenOpt(part) : std::nullopt;
  slot->is_active = ag.has_value() && *ag == gen;
  slot->pinned = slot->ctx->PinWalGen(part, gen, &slot->handle);
}

// step 1i：候选选择（读路径与测试入口共用的**唯一**实现）。
//   候选 = 路由分区 ∪ 别名(DRAINING/split) ∪ 其它仍有索引段的分区；
//   跨候选按 seq 取最优（跨 physical part 唯一可比量），删除标记同等参与；
//   剪枝只允许用**可靠上界**（候选全部段的 max_seq 最大值，保守高估）：
//   上界 ≤ 当前最优 ⇒ 该候选不可能更新，之后的可停。
//   候选数上限 `zfo_.read_union_max_parts` **不截断**——超限扫全量并计数
//   （read_union_overflow），绝不静默丢候选（用户 2026-09-15）。
bool ZeroFlushContext::SelectBestIndexRecord(
    const ROCKSDB_NAMESPACE::Slice& user_key,
    ROCKSDB_NAMESPACE::SequenceNumber snapshot,
    const std::vector<uint32_t>* candidates, uint32_t* out_logical_part,
    uint32_t* out_phys_part, uint32_t* out_gen, uint64_t* out_offset,
    ROCKSDB_NAMESPACE::ValueType* out_type,
    ROCKSDB_NAMESPACE::SequenceNumber* out_seq, void* pin_slot) const {
  struct Cand {
    uint32_t part;
    uint64_t upper;
  };
  std::vector<Cand> cands;
  if (candidates != nullptr && !candidates->empty()) {
    // 测试入口：给定候选列表（每个候选都用**真实上界**参与剪枝）。
    for (uint32_t p : *candidates) {
      cands.push_back({p, index_set_->PartMaxSeqUpper(p)});
    }
  } else {
    const uint32_t part = RoutePartId(user_key);
    std::vector<uint32_t> extra;
    if (draining_active_.load(std::memory_order_acquire) != 0 &&
        ucmp_ != nullptr) {
      std::lock_guard<std::mutex> l(draining_mu_);
      for (const auto& d : draining_) {
        if (d.absorb != part) continue;
        if (d.lo.size() > 0 &&
            ucmp_->Compare(user_key, ROCKSDB_NAMESPACE::Slice(d.lo)) < 0) {
          continue;
        }
        if (d.hi.size() > 0 &&
            ucmp_->Compare(user_key, ROCKSDB_NAMESPACE::Slice(d.hi)) >= 0) {
          continue;
        }
        extra.push_back(d.part);
      }
    }
    if (split_orphan_active_.load(std::memory_order_acquire) != 0) {
      std::lock_guard<std::mutex> l(split_orphan_mu_);
      for (const auto& o : split_orphans_) {
        if (o.child_l == part || o.child_r == part) extra.push_back(o.part);
      }
    }
    cands.push_back({part, index_set_->PartMaxSeqUpper(part)});
    for (uint32_t p : extra) {
      cands.push_back({p, index_set_->PartMaxSeqUpper(p)});
    }
    std::vector<std::pair<uint32_t, uint64_t>> chain_parts;
    index_set_->ChainParts(&chain_parts);
    if (chain_parts.size() > zfo_.read_union_max_parts) {
      read_union_overflow_.fetch_add(1, std::memory_order_relaxed);
      ROCKS_LOG_WARN(wal_ != nullptr ? wal_->InfoLog() : nullptr,
                     "ZF read union candidates=%zu exceed cap=%u: scanning ALL "
                     "(no silent truncation)",
                     chain_parts.size(), zfo_.read_union_max_parts);
    }
    for (const auto& [p, ub] : chain_parts) {
      if (p == part) continue;
      if (std::find(extra.begin(), extra.end(), p) != extra.end()) continue;
      cands.push_back({p, ub});
      index_parts_scanned_.fetch_add(1, std::memory_order_relaxed);
    }
  }
  std::stable_sort(cands.begin(), cands.end(),
                   [](const Cand& a, const Cand& b) { return a.upper > b.upper; });
  bool have_any = false;
  ROCKSDB_NAMESPACE::ValueType best_type = ROCKSDB_NAMESPACE::kTypeValue;
  ROCKSDB_NAMESPACE::SequenceNumber best_seq = 0;
  uint32_t best_part = 0, best_phys = 0, best_gen = 0;
  uint64_t best_off = 0;
  // step 1aj（用户 2026-09-17，已证实的缺陷）：**locator 与读引用必须作为一份
  // 候选结果整体保存，只在候选胜出时整体替换**。
  // 旧实现把 pin 回调写进**单个共享槽**：每个命中的候选都会更新它，而 locator
  // 只在胜出时更新 ⇒ 后访问的落选候选（其段 max_seq 高、不被剪枝）会把 pin
  // 覆盖成自己的句柄，而 locator 仍属于胜出候选 ⇒ **locator 与读引用分离**
  // （实测：pin 路径读到 payload 中部，兜底路径在同 offset 读到合法记录头）。
  auto* out_slot = static_cast<ZfPinSlot*>(pin_slot);
  if (out_slot != nullptr) {
    *out_slot = ZfPinSlot();
    out_slot->ctx = this;
  }
  for (size_t ci = 0; ci < cands.size(); ++ci) {
    if (have_any && cands[ci].upper <= best_seq) break;  // 可靠剪枝
    std::string cand_loc;
    ROCKSDB_NAMESPACE::ValueType cand_type = ROCKSDB_NAMESPACE::kTypeValue;
    ROCKSDB_NAMESPACE::SequenceNumber cand_seq = 0;
    // 每个候选用**自己的** pin 槽：读引用在索引保护释放前取得（同一 GetCopy 调用内）。
    ZfPinSlot cand_pin;
    cand_pin.ctx = this;
    const bool cand_hit = index_set_->GetCopy(
        cands[ci].part, user_key, snapshot, &cand_loc, &cand_type, &cand_seq,
        out_slot != nullptr ? &ZfPinThunk : nullptr, &cand_pin);
    // step 1al（用户 2026-09-17）：**候选访问/命中/落选的执行记录**——上界关系
    // 只能说明"应被访问"，只有执行记录才证明缺陷路径真的被覆盖（case7 断言用）。
    if (zeroflush::get_trace::LocatorDiagEnabled()) {
      zeroflush::get_trace::NoteCandidateVisit(cands[ci].part, ci, cand_hit,
                                               cand_seq);
    }
    if (cand_hit) {
      if (!have_any || cand_seq > best_seq) {
        have_any = true;
        best_type = cand_type;
        best_seq = cand_seq;
        best_part = cands[ci].part;
        if (cand_loc.size() == sizeof(SlimLocator)) {
          const SlimLocator* sl =
              reinterpret_cast<const SlimLocator*>(cand_loc.data());
          best_phys = sl->part_id;
          best_gen = sl->gen;
          best_off = sl->wal_offset;
        }
        if (out_slot != nullptr) {
          // step 1aq：胜出候选**整体接管** locator + RAII 读引用（可移动、不可复制）。
          // 若该代已 retiring（回收已入队）⇒ 本候选失效，继续尝试其它候选（重选），
          // 不得静默返回旧值。
          if (sealed_cache_ != nullptr &&
              !sealed_cache_->TryAcquireGenReadRef(best_phys, best_gen)) {
            // 该代已 retiring（回收已入队）⇒ **整次选择作废**：交由调用方刷新读视图
            // 后完整重试（不得继续尝试其它候选而返回更低版本）。
            zeroflush::get_trace::RetryReadViewNeeded().fetch_add(
                1, std::memory_order_relaxed);
            zeroflush::get_trace::SetRetiringRejected(true);
            zeroflush::get_trace::NoteRejectedGen(best_phys, best_gen);
            have_any = false;
            if (out_slot != nullptr) {
              *out_slot = ZfPinSlot();
              out_slot->ctx = this;
            }
            return false;
          }
          *out_slot = std::move(cand_pin);  // 胜出候选：locator + 读引用整体替换
          out_slot->ctx = this;
          // step 1as 修正：上一步 TryAcquire 取得的引用必须**交给胜出槽**持有
          // （RAII：槽析构/被替换时释放）。此前该引用被丢弃 ⇒ 每次命中都泄漏
          // 一个 generation 读引用 ⇒ 该代永远"有读者"、回收被无限期延迟
          // （case5: file_after 恒为 1068、reclaim_pending 永不清空）。
          if (sealed_cache_ != nullptr) {
            out_slot->read_ref = SealedFileCache::GenReadRef(
                sealed_cache_.get(), best_phys, best_gen);
          }
          auto& pi = zeroflush::get_trace::LastPinIdentity();
          pi.set = true;
          pi.part = out_slot->part;
          pi.gen = out_slot->gen;
          pi.pinned = out_slot->pinned;
          pi.is_active = out_slot->is_active;
          // step 1ar：同一身份另存**全局**（测试主线程要读读者线程选中的 (part,gen)；
          // TLS 跨线程不可见）。
          auto& pig = zeroflush::get_trace::LastPinIdentityGlobal();
          pig.part.store(out_slot->part, std::memory_order_relaxed);
          pig.gen.store(out_slot->gen, std::memory_order_relaxed);
          pig.set.store(true, std::memory_order_relaxed);
        }
      }
      // 测试开关（仅诊断）：复现**旧的共享槽语义**——每个命中的候选都把 pin 写进
      // 输出槽（落选者会覆盖胜出者的句柄）⇒ 同一 fixture 下应当失败。
      if (out_slot != nullptr && getenv("ZF_TEST_LEGACY_PIN_SLOT") != nullptr) {
        *out_slot = std::move(cand_pin);
        out_slot->ctx = this;
        auto& pi = zeroflush::get_trace::LastPinIdentity();
        pi.set = true;
        pi.part = out_slot->part;
        pi.gen = out_slot->gen;
        pi.pinned = out_slot->pinned;
        pi.is_active = out_slot->is_active;
      }
    }
  }
  if (!have_any) {
    return false;
  }
  // step 1ag（C 段）：选择胜出后，记录/对照胜出 locator 的解码结果与 A 表。
  if (zeroflush::get_trace::LocatorDiagEnabled() &&
      out_phys_part != nullptr && out_gen != nullptr && out_offset != nullptr) {
    zeroflush::get_trace::AppendSite a;
    const bool found = zeroflush::get_trace::LookupAppendSite(
        *out_phys_part, *out_gen, *out_offset, &a);
    if (found) {
      const uint64_t kh = zeroflush::get_trace::KeyHash(user_key);
      if (a.key_hash != kh ||
          (out_seq != nullptr && a.seq != *out_seq)) {
        zeroflush::get_trace::LocatorCMismatch().fetch_add(
            1, std::memory_order_relaxed);
      }
    }
  }
  if (out_logical_part != nullptr) *out_logical_part = best_part;
  if (out_phys_part != nullptr) *out_phys_part = best_phys;
  if (out_gen != nullptr) *out_gen = best_gen;
  if (out_offset != nullptr) *out_offset = best_off;
  if (out_type != nullptr) *out_type = best_type;
  if (out_seq != nullptr) *out_seq = best_seq;
  return true;
}

bool ZeroFlushContext::GetFromPartitionIndex(
    const ROCKSDB_NAMESPACE::Slice& user_key,
    ROCKSDB_NAMESPACE::SequenceNumber snapshot,
    ROCKSDB_NAMESPACE::Status* s, std::string* value,
    ROCKSDB_NAMESPACE::MergeContext* merge_context,
    const ROCKSDB_NAMESPACE::MergeOperator* merge_op,
    bool* retry_requested, bool read_view_refreshed) const {
  // step 1ae：整次 Get 的关联标识。首次进入（attempt 0）生成 get_id；
  // SuperVersion 刷新后的重试递增 attempt_id。每个 attempt 开始时**清空**诊断
  // 状态，避免把上一次（可能成功）读取的线程局部残留当成本次失败证据。
  {
    using zeroflush::get_trace::BeginAttempt;
    using zeroflush::get_trace::BeginGet;
    using zeroflush::get_trace::CurAttemptId;
    if (CurAttemptId() == 0 && !read_view_refreshed) {
      BeginGet();  // 新的 Get：分配 get_id
    }
    BeginAttempt();  // 新 attempt：清空诊断状态
  }
  const uint32_t part = RoutePartId(user_key);
  // ZeroFlushV0 P1b/M1a：DRAINING 别名回查。merge 切路由后，被合并分区
  // 的 E_D 冻结索引在排空完成前仍持有其原区间内 key 的（最新）版本；该
  // 区间 key 现路由到吸收方——吸收方索引链 miss 后必须回查被合并分区的
  // 索引链，否则排空窗口内（未覆写的）旧 key 读缺失。序正确性：吸收方链
  // 优先（其版本更新）；E_D 先于吸收方任何 post-merge epoch 落地（FIFO +
  // 默认 merge-off 直装不攒批），故别名命中不可能盖过已落地的吸收方新值。
  // 热路径免锁门：无 DRAINING 登记时（常态）不取 draining_mu_。
  std::vector<uint32_t> extra;
  if (draining_active_.load(std::memory_order_acquire) != 0 && ucmp_ != nullptr) {
    std::lock_guard<std::mutex> l(draining_mu_);
    for (const auto& d : draining_) {
      if (d.absorb != part) {
        continue;
      }
      if (d.lo.size() > 0 &&
          ucmp_->Compare(user_key, ROCKSDB_NAMESPACE::Slice(d.lo)) < 0) {
        continue;
      }
      if (d.hi.size() > 0 &&
          ucmp_->Compare(user_key, ROCKSDB_NAMESPACE::Slice(d.hi)) >= 0) {
        continue;
      }
      extra.push_back(d.part);
    }
  }
  // ZeroFlushV0 M4（活跃尾 split）：孤儿尾 epoch 别名。key 路由到子分区，
  // 但父分区的冻结尾 epoch 仍持有 [父区间) 内同一 key 的旧版本——子链 miss
  // 后回查父链（子版本更新，子链优先即正确）。
  if (split_orphan_active_.load(std::memory_order_acquire) != 0) {
    std::lock_guard<std::mutex> l(split_orphan_mu_);
    for (const auto& o : split_orphans_) {
      if (o.child_l == part || o.child_r == part) {
        extra.push_back(o.part);
      }
    }
  }
  // step 1f：locator 由本次调用持有（拷贝语义）——不依赖索引段存活。
  std::string loc_buf;
  ROCKSDB_NAMESPACE::Slice loc;
  ROCKSDB_NAMESPACE::ValueType type;
  ROCKSDB_NAMESPACE::SequenceNumber seq;
  const bool zf_trace =
      zeroflush::get_trace::Enabled() && zeroflush::get_trace::Match(user_key);
  if (zf_trace) {
    fprintf(stderr,
            "ZFDBG-get-begin key_id=%lld read_seq=%llu routed_logical_part=%u "
            "alias_candidates=%zu\n",
            (long long)zeroflush::get_trace::KeyId(user_key),
            (unsigned long long)snapshot, part, extra.size());
  }
  ZfPinSlot best_pin;
  best_pin.ctx = this;
  uint32_t sel_part = 0, sel_phys = 0, sel_gen = 0;
  uint64_t sel_off = 0;
  ROCKSDB_NAMESPACE::ValueType sel_type = ROCKSDB_NAMESPACE::kTypeValue;
  ROCKSDB_NAMESPACE::SequenceNumber sel_seq = 0;
  zeroflush::get_trace::SetRetiringRejected(false);
  const bool have_any = SelectBestIndexRecord(
      user_key, snapshot, /*candidates=*/nullptr, &sel_part, &sel_phys, &sel_gen,
      &sel_off, &sel_type, &sel_seq, &best_pin);
  if (!have_any && zeroflush::get_trace::ConsumeRetiringRejected()) {
    // step 1ar（用户 2026-09-17）：胜出代已 retiring（回收入队）⇒ **整次读取重试**
    // （刷新读视图、保持 read_sequence）；重试若仍无法安全完成则明确报错——
    // **绝不**返回更低版本或 NotFound。
    if (retry_requested != nullptr && !read_view_refreshed) {
      *retry_requested = true;
      if (s != nullptr) {
        *s = ROCKSDB_NAMESPACE::Status::TryAgain();
      }
      return true;
    }
    // step 1as 修正（压力组 5 实证）：重取视图后仍被拒时，按**该代是否已安装**分流
    // ——同一个 true 结论不能既代表"数据已在 LSM（可回落取值）"又代表"唯一副本
    // 正在消失（必须报错）"：
    //   · 已安装 ⇒ 该代记录的同 seq 版本已在 LSM（发布证据），回落原生查找即取到
    //     **同一或更新版本**（不是降级）；
    //   · 未安装 ⇒ WAL 是唯一副本正在被回收 ⇒ 明确报错（不降级、不 NotFound）。
    // 判据用**发布证据**（cache 级 installed，随成功发布的 VersionEdit 置位），
    // 不用 fence：fence 还要求消费证明完整，可能滞后于发布（实测正是这一点让
    // 真实已安装的代被判成"不可回落"而产生 6 次 IO error）。
    {
      const auto& rej = zeroflush::get_trace::LastRejectedGenTls();
      const bool published =
          rej.set && sealed_cache_ != nullptr &&
          sealed_cache_->GenInstalledPublished(rej.part, rej.gen);
      if (published) {
        if (zeroflush::get_trace::LocatorDiagEnabled()) {
          fprintf(stderr,
                  "ZFDBG-get-final source=LSM_AFTER_RETIRE(installed_by_publish "
                  "part=%u gen=%u fence_covers=%d) → 回落原生查找\n",
                  rej.part, rej.gen, IsGenInstalled(rej.part, rej.gen) ? 1 : 0);
        }
        return false;  // 未命中索引层：交给调用方走 LSM（非降级）
      }
    }
    if (s != nullptr) {
      *s = ROCKSDB_NAMESPACE::Status::IOError(
          "ZF: generation retiring; read view refresh could not complete "
          "safely (no downgrade)");
    }
    return true;
  }
  if (have_any) {
    // step 1j：测试挂钩——选择完成（pin 已取）、ReadValue 之前暂停，供
    // "选中 locator 后触发安装/换代/回收"的确定性用例驱动（仅测试；
    // 未设 ZF_TEST_PAUSE_AFTER_SELECT 时零开销）。
    {
      const char* pause_path = getenv("ZF_TEST_PAUSE_AFTER_SELECT");
      if (pause_path != nullptr && *pause_path != '\0') {
        const uint64_t deadline =
            env_->NowMicros() + 15ull * 1000 * 1000;  // 15s 上限
        while (env_->NowMicros() < deadline) {
          if (env_->FileExists(pause_path).ok()) break;
          env_->SleepForMicroseconds(1000);
        }
      }
    }
    // locator 字节：从胜出字段重建（16B SlimLocator）。best_pin 已在该次
    // 选择内取得读句柄（缺口 3），并存活到本函数返回。
    SlimLocator sl{};
    sl.part_id = sel_phys;
    sl.gen = sel_gen;
    sl.wal_offset = sel_off;
    loc_buf.assign(reinterpret_cast<const char*>(&sl), sizeof(sl));
    loc = ROCKSDB_NAMESPACE::Slice(loc_buf);
    type = sel_type;
    seq = sel_seq;
    {
      const uint32_t cand = sel_part;  // 命中来源分区（诊断/日志）
      // M3.4：命中时也检查 tombstone 覆盖（DeleteRange 后写的 range 删除
      // 该 key——tombstone seq 更大则覆盖）。精细 seq 比较（数据后写覆盖
      // 旧 tombstone）留后续；当前按"tombstone ≤ snapshot 即覆盖"。
      if (CheckRangeDelCover(user_key, snapshot)) {
        zeroflush::get_trace::SetLastSource(
            zeroflush::get_trace::GetSource::kIndexRangeDel);
        if (zf_trace) {
          fprintf(stderr,
                  "ZFDBG-get-final source=INDEX(rangedel_cover) part=%u "
                  "hit_seq=%llu hit_type=RANGEDEL value=NOTFOUND\n",
                  cand, (unsigned long long)seq);
        }
        *s = ROCKSDB_NAMESPACE::Status::NotFound();
        return true;
      }
      if (type == ROCKSDB_NAMESPACE::kTypeDeletion ||
          type == ROCKSDB_NAMESPACE::kTypeSingleDeletion ||
          type == ROCKSDB_NAMESPACE::kTypeRangeDeletion) {
        zeroflush::get_trace::SetLastSource(
            zeroflush::get_trace::GetSource::kIndexTombstone);
        if (zf_trace) {
          // 删除标记：正确语义是 NotFound（不是"比较两个 value"）。
          fprintf(stderr,
                  "ZFDBG-get-final source=INDEX(tombstone) part=%u hit_seq=%llu "
                  "hit_type=%s value=NOTFOUND\n",
                  cand, (unsigned long long)seq,
                  zeroflush::get_trace::TypeName(static_cast<int>(type)));
        }
        *s = ROCKSDB_NAMESPACE::Status::NotFound();
        return true;
      }
      // 读句柄已在选择时（链引用释放前）取得并保留在 best_pin 中，
      // 覆盖"选择 → ReadValue"窗口（缺口 3）。
      ROCKSDB_NAMESPACE::Status rs =
          best_pin.handle != nullptr
              ? ReadValuePinned(loc, best_pin.handle.get(), value)
              : ReadValue(loc, value);
      // step 1aq：读引用由 best_pin.read_ref（RAII）在本函数返回时自动释放——
      // 成功/失败/提前 return 都恰好释放一次。
      if (!rs.ok()) {
        zeroflush::get_trace::SetLastSource(
            zeroflush::get_trace::GetSource::kIndexReadFailed);
        // step 1ai（用户 2026-09-17）：**pin 路径也要打印原始头副本**——此前只有
        // fd 兜底分支打印，导致"原始头 vs 兜底头"的对照其实来自两个不同失败点
        // （非严格配对）。此处与兜底分支使用同一份 `LastReadEvidence()`（在
        // ReadFromSealed 内**立即复制**的原始头），因此是同一个 read 的现场。
        if (zeroflush::get_trace::LocatorDiagEnabled()) {
          const auto& ev = PartitionedWalManager::LastReadEvidence();
          char oh[3 * 24 + 1] = {0};
          size_t o = 0;
          for (size_t i = 0; i < ev.original_header_len && i < 24; ++i) {
            o += snprintf(oh + o, 4, "%02x ",
                          static_cast<unsigned>(ev.original_header[i]));
          }
          uint32_t k_magic = 0, k_klen = 0, k_vlen = 0;
          if (ev.original_header_len >= 24) {
            std::memcpy(&k_magic, ev.original_header, 4);
            std::memcpy(&k_klen, ev.original_header + 8, 4);
            std::memcpy(&k_vlen, ev.original_header + 12, 4);
          }
          const auto& site = zeroflush::get_trace::LastReadError();
          fprintf(stderr,
                  "ZFDBG-hdrcmp pinned get_id=%llu attempt=%llu read_id=%llu "
                  "path=%s part=%u gen=%u off=%llu want=%llu got=%llu "
                  "header=[%s] decode: magic=0x%08x klen=%u vlen=%u "
                  "parse_rc=%d error_site=%d status=%s\n",
                  (unsigned long long)zeroflush::get_trace::CurGetId(),
                  (unsigned long long)zeroflush::get_trace::CurAttemptId(),
                  (unsigned long long)ev.read_id,
                  best_pin.handle != nullptr ? "PINNED" : "FD_FALLBACK",
                  site.part, site.gen, (unsigned long long)site.offset,
                  (unsigned long long)site.want, (unsigned long long)site.got,
                  oh, k_magic, k_klen, k_vlen, ev.orig_parse_rc, site.site,
                  rs.ToString().c_str());
        }
        // 判据（用户 2026-09-15 缺口 2 / step 1l）：**不能凭 fence 直接降级**
        // ——fence 只证明安装已提交，不证明本次 Get 的 SuperVersion 包含那次
        // 安装。协议：
        //   · 该代**未安装** → 唯一副本不可读，保留原始错误（传播）。
        //   · 该代**已安装** → 第一次请求调用方刷新读视图后重试（保持
        //     read_sequence）；刷新后（read_view_refreshed=true）仍不可读则
        //     回落 LSM——此时视图已包含该安装，回落是正确的。
        const bool installed_now = IsGenInstalled(best_pin.part, best_pin.gen);
        if (installed_now && !read_view_refreshed && retry_requested != nullptr) {
          *retry_requested = true;
          *s = ROCKSDB_NAMESPACE::Status::TryAgain();
          return true;  // 交由调用方刷新读视图后重试
        }
        if (installed_now && read_view_refreshed) {
          read_readfail_fallen_back_.fetch_add(1, std::memory_order_relaxed);
          if (zf_trace) {
            fprintf(stderr,
                    "ZFDBG-get-readfail phys_part=%u phys_gen=%u installed=1 "
                    "view_refreshed=1 status=%s -> fallthrough(LSM)\n",
                    best_pin.part, best_pin.gen, rs.ToString().c_str());
          }
          return false;  // 刷新后的视图包含该安装 → 由 LSM 服务
        }
        read_readfail_propagated_.fetch_add(1, std::memory_order_relaxed);
        {
          // step 1ae：**关联链证据**（产生点 → 传播 → 最终返回）。
          // 若产生点未登记（NOT_VISITED），说明错误来自本函数未覆盖的路径，
          // 先修诊断覆盖/核对二进制，而不是解释竞态。
          {
            using zeroflush::get_trace::CurAttemptId;
            using zeroflush::get_trace::CurGetId;
            using zeroflush::get_trace::LastReadError;
            // 打印**环形里所有属于本次 get_id 的**站点（不再只看"最新一条"）。
            bool any = false;
            {
              using zeroflush::get_trace::CurGetId;
              using zeroflush::get_trace::ErrorSiteRing;
              using zeroflush::get_trace::ErrorSiteRingPos;
              const auto* ring = ErrorSiteRing();
              const uint32_t pos = ErrorSiteRingPos();
              for (uint32_t i = 0; i < 8; ++i) {
                const auto& rr = ring[i];
                if (!rr.set || rr.get_id != CurGetId()) {
                  continue;
                }
                any = true;
                fprintf(stderr,
                        "ZFDBG-errchain get_id=%llu attempt=%llu thr=%llu "
                        "site=%d part=%u gen=%u off=%llu want=%llu got=%llu "
                        "parsed_total=%llu cond=%d status=%s => FINAL IOError\n",
                        (unsigned long long)rr.get_id,
                        (unsigned long long)rr.attempt_id,
                        (unsigned long long)rr.thread_hash, rr.site, rr.part,
                        rr.gen, (unsigned long long)rr.offset,
                        (unsigned long long)rr.want, (unsigned long long)rr.got,
                        (unsigned long long)rr.parsed_total, rr.cond, rr.status);
              }
              (void)pos;
            }
            if (!any) {
              fprintf(stderr,
                      "ZFDBG-errchain get_id=%llu attempt=%llu thr=%llu "
                      "site=NOT_VISITED => FINAL IOError "
                      "(错误产生点未被诊断覆盖)\n",
                      (unsigned long long)CurGetId(),
                      (unsigned long long)CurAttemptId(),
                      (unsigned long long)static_cast<uint64_t>(
                          std::hash<std::thread::id>{}(
                              std::this_thread::get_id())));
            }
          }
          *s = ROCKSDB_NAMESPACE::Status::IOError(
              "ZF: index hit but WAL record unreadable (phys_part=" +
              std::to_string(best_pin.part) + " gen=" +
              std::to_string(best_pin.gen) + " pinned=" +
              std::to_string(best_pin.pinned ? 1 : 0) + " active=" +
              std::to_string(best_pin.is_active ? 1 : 0) + " installed=" +
              std::to_string(IsGenInstalled(best_pin.part, best_pin.gen) ? 1 : 0) +
              "): " + rs.ToString());
          if (zf_trace) {
            fprintf(stderr,
                    "ZFDBG-get-readfail phys_part=%u phys_gen=%u pinned=%d "
                    "is_active=%d installed=%d status=%s -> PROPAGATE(error)\n",
                    best_pin.part, best_pin.gen, best_pin.pinned ? 1 : 0,
                    best_pin.is_active ? 1 : 0,
                    IsGenInstalled(best_pin.part, best_pin.gen) ? 1 : 0,
                    rs.ToString().c_str());
          }
          return true;  // 传播，不再回落 LSM
        }
        return false;
      }
      if (type == ROCKSDB_NAMESPACE::kTypeMerge && merge_context != nullptr) {
        // M3.4：收集同 key 全部版本（seq 降序）——分离 base 与 operands。
        std::vector<std::string> versions;
        std::vector<ROCKSDB_NAMESPACE::ValueType> vtypes;
        auto read_value = [this](const ROCKSDB_NAMESPACE::Slice& locator,
                                 std::string* out) {
          return ReadValue(locator, out);
        };
        index_set_->CollectVersions(cand, user_key, snapshot, read_value,
                                    &versions, &vtypes);
        std::string base;
        bool has_base = false;
        for (size_t i = 0; i < versions.size(); ++i) {
          if (vtypes[i] == ROCKSDB_NAMESPACE::kTypeValue && !has_base) {
            base = versions[i];
            has_base = true;
            break;
          }
          merge_context->PushOperand(versions[i]);
        }
        if (has_base && merge_context->GetNumOperands() > 0 &&
            merge_op != nullptr) {
          ROCKSDB_NAMESPACE::MergeOperator::MergeOperationInputV3 input(
              user_key,
              ROCKSDB_NAMESPACE::MergeOperator::MergeOperationInputV3::
                  ExistingValue(std::in_place_type<ROCKSDB_NAMESPACE::Slice>,
                                base),
              merge_context->GetOperands(), nullptr /* logger */);
          ROCKSDB_NAMESPACE::MergeOperator::MergeOperationOutputV3 output;
          if (merge_op->FullMergeV3(input, &output)) {
            if (auto* v = std::get_if<std::string>(&output.new_value)) {
              *value = *v;
              *s = ROCKSDB_NAMESPACE::Status::OK();
              return true;
            }
          }
          // FullMergeV3 失败/非字符串结果——回退 MergeInProgress（SST 兜底）。
        }
        *s = ROCKSDB_NAMESPACE::Status::MergeInProgress();
        return false;
      }
      zeroflush::get_trace::SetLastSource(
          zeroflush::get_trace::GetSource::kIndex);
      if (zf_trace) {
        fprintf(stderr,
                "ZFDBG-get-final source=INDEX part=%u hit_seq=%llu hit_type=%s "
                "value_len=%zu\n",
                cand, (unsigned long long)seq,
                zeroflush::get_trace::TypeName(static_cast<int>(type)),
                value != nullptr ? value->size() : 0);
      }
      *s = ROCKSDB_NAMESPACE::Status::OK();
      return true;
    }
  }
  // M5 诊断：全部候选未命中（key 不在任何链）。
  {
    static std::atomic<uint64_t> zf_ixmiss_dbg{0};
    if (zf_ixmiss_dbg.fetch_add(1, std::memory_order_relaxed) < 32) {
      fprintf(stderr, "ZFDBG-ixmiss routed_part=%u key=%s\n", part,
              user_key.ToString(true).c_str());
    }
  }
  // M3.4：未 compact 窗口的 range tombstone 覆盖检查（无索引命中时）。
  if (CheckRangeDelCover(user_key, snapshot)) {
    if (zf_trace) {
      fprintf(stderr,
              "ZFDBG-get-final source=INDEX(rangedel_cover,no_idx_hit) "
              "value=NOTFOUND\n");
    }
    *s = ROCKSDB_NAMESPACE::Status::NotFound();
    return true;
  }
  zeroflush::get_trace::SetLastSource(
      zeroflush::get_trace::GetSource::kNone);  // 索引未命中（来源由调用方定）
  if (zf_trace) {
    fprintf(stderr,
            "ZFDBG-get-fallthrough source=NONE(index_miss) -> LSM lookup\n");
  }
  return false;
}

// M3.4：未 compact 窗口的 range tombstone 覆盖检查。
bool ZeroFlushContext::CheckRangeDelCover(
    const ROCKSDB_NAMESPACE::Slice& user_key,
    ROCKSDB_NAMESPACE::SequenceNumber snapshot) const {
  if (index_set_ == nullptr) {
    return false;
  }
  auto read_value = [this](const ROCKSDB_NAMESPACE::Slice& loc,
                           std::string* out) {
    return ReadValue(loc, out);
  };
  return index_set_->GetRangeDelCover(kRangeDelPartId, user_key, snapshot,
                                      read_value);
}

ROCKSDB_NAMESPACE::Status ZeroFlushContext::WriteGroupToPartitionWal(
    ROCKSDB_NAMESPACE::WriteThread::WriteGroup& wg,
    ROCKSDB_NAMESPACE::SequenceNumber first_seq,
    ROCKSDB_NAMESPACE::MemTable* mem, std::vector<uint32_t>* touched) {
  ROCKSDB_NAMESPACE::SequenceNumber seq = first_seq;
  // M2.3-2：收集触达分区。write group leader 串行处理所有 writer，
  // handler 在 Put/Delete 时把 part_id 记入 touched（去重）。
  uint64_t touched_set = 0;  // 分区位图（M4.9：省每 op hash）
  // M4.1c：并发插入的 post-process 累计（按 mem，本组恒 1 个）。
  std::map<ROCKSDB_NAMESPACE::MemTable*,
           ROCKSDB_NAMESPACE::MemTablePostProcessInfo>
      post_map;
  for (auto* writer : wg) {
    assert(writer != nullptr);
    if (!writer->ShouldWriteToMemtable()) {
      continue;
    }
    ZfBatchHandler handler(this, mem, &seq, &touched_set, &post_map,
                           wg.zf_table_version);
    ROCKSDB_NAMESPACE::Status s = writer->batch->Iterate(&handler);
    if (!s.ok()) {
      return s;
    }
  }
  // 组收尾：应用 post-process（num_entries/data_size 计数 + UpdateFlushState，
  // 触发 mem 满时的 flush 调度）。与原生 MemTableInserter::PostProcess 同构。
  for (auto& entry : post_map) {
    entry.first->BatchPostProcess(entry.second);
  }
  // M3.0 R3：sync 移出本方法（与 DB mutex 解耦，见 M3_DESIGN.md §9）。
  // 本方法只负责追加 + 索引；调用方在释放 DB mutex 后按 touched 分区
  // 调 SyncTouchedPartitions 做 fdatasync。
  if (touched != nullptr) {
    touched->clear();
    const auto tbl_v = tables_ ? tables_->Get(wg.zf_table_version) : nullptr;
    for (uint32_t p = 0; p < 64 && touched_set != 0; ++p) {
      if (touched_set & (uint64_t{1} << p)) {
        uint32_t pid = zeroflush::kRangeDelPartId;
        if (p != 63) {
          pid = (tbl_v != nullptr && !tbl_v->IsHashMode())
                    ? tbl_v->part_id(p)  // ZeroFlushV0 P1b：下标→id
                    : p;
        }
        touched->push_back(pid);
        touched_set &= ~(uint64_t{1} << p);
      }
    }
  }
  return ROCKSDB_NAMESPACE::Status::OK();
}

ROCKSDB_NAMESPACE::Status ZeroFlushContext::InsertWriterToPartitionWal(
    ROCKSDB_NAMESPACE::WriteThread::Writer* w,
    ROCKSDB_NAMESPACE::MemTable* mem,
    const ROCKSDB_NAMESPACE::WriteOptions& write_options,
    uint32_t table_version) {
  assert(w != nullptr);
  ROCKSDB_NAMESPACE::SequenceNumber seq = w->sequence;
  uint64_t touched_set = 0;  // 分区位图（M4.9：省每 op hash）
  std::map<ROCKSDB_NAMESPACE::MemTable*,
           ROCKSDB_NAMESPACE::MemTablePostProcessInfo>
      post_map;
  ZfBatchHandler handler(this, mem, &seq, &touched_set, &post_map,
                         table_version);
  ROCKSDB_NAMESPACE::Status s = w->batch->Iterate(&handler);
  if (!s.ok()) {
    return s;
  }
  for (auto& entry : post_map) {
    entry.first->BatchPostProcess(entry.second);
  }
  if (write_options.sync && touched_set != 0) {
    std::vector<uint32_t> touched;
    const auto tbl_v = tables_ ? tables_->Get(table_version) : nullptr;
    for (uint32_t p = 0; p < 64 && touched_set != 0; ++p) {
      if (touched_set & (uint64_t{1} << p)) {
        uint32_t pid = zeroflush::kRangeDelPartId;
        if (p != 63) {
          pid = (tbl_v != nullptr && !tbl_v->IsHashMode())
                    ? tbl_v->part_id(p)  // ZeroFlushV0 P1b：下标→id
                    : p;
        }
        touched.push_back(pid);
        touched_set &= ~(uint64_t{1} << p);
      }
    }
    return SyncTouchedPartitions(touched);
  }
  return ROCKSDB_NAMESPACE::Status::OK();
}

ROCKSDB_NAMESPACE::Status ZeroFlushContext::SyncTouchedPartitions(
    const std::vector<uint32_t>& touched) {

  // 精准 fdatasync：仅触达分区。替代原 M1 的 SyncAll（fsync 全 P 分区）。
  // 性能特征：P=64 时单 put 的 sync 成本从 64x fsync 降到 1x。
  for (uint32_t part : touched) {
    ROCKSDB_NAMESPACE::Status s = wal_->Sync(part);
    if (!s.ok()) {
      return s;
    }
  }
  return ROCKSDB_NAMESPACE::Status::OK();
}

bool ZeroFlushContext::IsGenInstalled(uint32_t phys_part, uint32_t gen) const {
  if (cfd_ == nullptr || cfd_->current() == nullptr) {
    return false;
  }
  const auto* vs_fence = cfd_->current()->storage_info();
  ROCKSDB_NAMESPACE::VersionEdit::ZfInstallFence f;
  return vs_fence->GetZfInstallFence(phys_part, &f) &&
         gen <= f.last_generation;
}

bool ZeroFlushContext::PinWalGen(
    uint32_t phys_part, uint32_t gen,
    std::shared_ptr<ROCKSDB_NAMESPACE::RandomAccessFile>* out) const {
  if (wal_ == nullptr || !wal_->HasPartition(phys_part)) {
    return false;
  }
  // step 1aq：generation 读引用改由调用方用 RAII 取得（见 ZfPinSlot.read_ref），
  // 此处只负责句柄。
  const std::optional<uint32_t> active_gen = wal_->ActiveGenOpt(phys_part);
  if (active_gen.has_value() && gen == *active_gen) {
    // 活跃代：**不能**用独立 fd（数据可能还在写缓冲里，单开 fd 会读到不完整
    // 文件）。跨 Freeze 的读由 `ReadActiveRecordChecked` 保证（p->mu 下校验
    // 代号，换代则重解析→按已落盘的封存文件读）。
    NotePinActiveUnpinnable();
    return false;
  }
  if (sealed_cache_ == nullptr) {
    return false;
  }
  const bool ok = sealed_cache_->Get(phys_part, gen, out).ok();
  if (ok) {
    NotePinAcquired();
  }
  return ok;
}

// step 1k：用**选择时取得的句柄**直接读该记录（不再按 (part,gen) 重新解析，
// 因此不受 Freeze/换代/回收影响）。tombstone 语义与常规路径一致（空值）。
ROCKSDB_NAMESPACE::Status ZeroFlushContext::ReadValuePinned(
    const ROCKSDB_NAMESPACE::Slice& locator_slice,
    ROCKSDB_NAMESPACE::RandomAccessFile* pinned, std::string* out) const {
  if (pinned == nullptr) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument("null pinned handle");
  }
  if (locator_slice.size() != sizeof(SlimLocator)) {
    return ROCKSDB_NAMESPACE::Status::Corruption(
        "ZeroFlush: bad locator size (pinned read)");
  }
  const SlimLocator* loc =
      reinterpret_cast<const SlimLocator*>(locator_slice.data());
  WalRecordRef ref;
  ref.part_id = loc->part_id;
  ref.gen = loc->gen;
  ref.offset = loc->wal_offset;
  std::string buf;
  ROCKSDB_NAMESPACE::Slice val;
  rocksdb::Status st = PartitionedWalManager::ReadFromSealed(pinned, ref, &buf,
                                                             &val);
  if (!st.ok()) {
    return st;
  }
  out->assign(val.data(), val.size());
  return ROCKSDB_NAMESPACE::Status::OK();
}

ROCKSDB_NAMESPACE::Status ZeroFlushContext::ReadValue(
    const ROCKSDB_NAMESPACE::Slice& locator_slice,
    std::string* out) const {
  if (locator_slice.size() != sizeof(SlimLocator)) {
    return ROCKSDB_NAMESPACE::Status::Corruption(
        "ZeroFlush: bad locator size in slim memtable entry");
  }
  // M4.7b：value cache 命中直接返回（键 = locator——WAL 段不可变，精确）。
  if (value_cache_ != nullptr) {
    auto h = value_cache_->Lookup(locator_slice);
    if (h != nullptr) {
      auto* sv = static_cast<const std::string*>(value_cache_->Value(h));
      *out = *sv;
      value_cache_->Release(h);
      return ROCKSDB_NAMESPACE::Status::OK();
    }
  }
  const SlimLocator* loc =
      reinterpret_cast<const SlimLocator*>(locator_slice.data());
  WalRecordRef ref;
  ref.part_id = loc->part_id;
  ref.gen = loc->gen;
  ref.offset = loc->wal_offset;
  // M3.1：用 HasPartition 替代上界校验 part_id < partitions。
  if (!wal_->HasPartition(ref.part_id)) {
    return ROCKSDB_NAMESPACE::Status::Corruption("ZeroFlush: bad part_id");
  }
  const std::optional<uint32_t> active_gen = wal_->ActiveGenOpt(ref.part_id);
  if (active_gen.has_value() && ref.gen == *active_gen) {
    NoteActiveWalRead();
    // step 1k：换代原子化读；若期间发生 Freeze（gen_rotated）→ 旧代文件已
    // 完整落盘，改按封存文件读（cache 未登记时直接开独立 fd——此时安全）。
    std::string buf;
    rocksdb::Slice val;
    bool gen_rotated = false;
    rocksdb::Status st = wal_->ReadActiveRecordChecked(ref, &buf, &val, &gen_rotated);
    if (gen_rotated) {
      std::shared_ptr<rocksdb::RandomAccessFile> rf;
      rocksdb::Status gs = sealed_cache_ != nullptr
                               ? sealed_cache_->Get(ref.part_id, ref.gen, &rf)
                               : rocksdb::Status::NotFound();
      if (!gs.ok()) {
        gs = wal_->OpenGenForRead(ref.part_id, ref.gen, &rf);  // 落盘后不可变
      }
      if (!gs.ok()) {
        return gs;
      }
      st = PartitionedWalManager::ReadFromSealed(rf.get(), ref, &buf, &val);
      if (st.ok()) {
        out->assign(val.data(), val.size());
        return rocksdb::Status::OK();
      }
      return st;
    }
    if (st.ok()) {
      out->assign(val.data(), val.size());
      return rocksdb::Status::OK();
    }
    // 落到下面的常规路径（含 cache 命中等）——保持原有语义与统计。
    st = wal_->ReadRecord(ref, out);
    // M3.0 R4：调试输出改用 ROCKS_LOG_DEBUG（仅 use_logger 接线后生效）。
    if (zfo_.use_logger) {
      ROCKS_LOG_DEBUG(wal_->InfoLog(),
                      "ZF ReadValue ACTIVE: part=%u gen=%u off=%lu status=%s "
                      "val_len=%zu",
                      ref.part_id, ref.gen, (unsigned long)ref.offset,
                      st.ok() ? "OK" : st.ToString().c_str(), out->size());
    }
    if (st.ok() && value_cache_ != nullptr) {
      value_cache_->Insert(locator_slice, new std::string(*out),
                           ValueCacheHelper(), out->size());
    }
    return st;
  }
  // 封存代：优先 SealedFileCache 登记句柄；**登记缺失时用独立 fd 兜底**
  // （step 1p，用户 2026-09-16）：已封存文件一旦换代即不可变（Freeze 先
  // FlushBuf+Sync 再换代），只要文件还在磁盘上就可安全读——不再因为
  // "不在任何 epoch 登记"让唯一副本不可读。同时计数：
  //   · 无登记但文件在 → gen_unregistered_fd_fallback（登记缺口，可读）
  //   · 两者都不在     → gen_lost_before_install（保留契约破坏，应恒为 0）
  std::shared_ptr<rocksdb::RandomAccessFile> rf;
  rocksdb::Status s = sealed_cache_ != nullptr
                          ? sealed_cache_->Get(ref.part_id, ref.gen, &rf)
                          : rocksdb::Status::NotFound();
  if (!s.ok()) {
    rocksdb::Status fs = wal_->OpenGenForRead(ref.part_id, ref.gen, &rf);
    // step 1aa（用户 2026-09-17）：**字节级核对**——把"索引命中但读失败"的
    // 全部现场打出来，按证据区分 封存发布顺序 / 短读 / locator·文件身份 /
    // 扫描提前失败，而不是猜。
    {
      static std::atomic<int> zf_readfail_diag{0};
      if (zf_readfail_diag.fetch_add(1) < 6) {
        const std::string path = wal_->GenFilePath(ref.part_id, ref.gen);
        uint64_t fsize = 0;
        const rocksdb::Status gfs = env_->GetFileSize(path, &fsize);
        fprintf(stderr,
                "ZFDBG-readfail part=%u gen=%u off=%llu cache=%s fd=%s "
                "file=%s file_size=%llu(%s)\n",
                ref.part_id, ref.gen, (unsigned long long)ref.offset,
                s.ToString().c_str(),
                fs.ok() ? "ok" : fs.ToString().c_str(), path.c_str(),
                (unsigned long long)fsize,
                gfs.ok() ? "ok" : gfs.ToString().c_str());
        const std::string desc = wal_->DescribePartition(ref.part_id, ref.gen);
        fprintf(stderr, "ZFDBG-readfail state: %s\n", desc.c_str());
        // ①₀ step 1ah（用户 2026-09-17）：**原始头副本 vs 兜底头**的严格对照。
        // 原始头 = 失败那次 Read 返回的缓冲（在 ReadFromSealed 内立即复制，未重读）；
        // 兜底头 = 此处按路径新开的句柄。两者分别做**无 I/O 的独立解码**。
        if (zeroflush::get_trace::LocatorDiagEnabled()) {
          const auto& ev = PartitionedWalManager::LastReadEvidence();
          char oh[3 * 24 + 1] = {0};
          size_t o = 0;
          for (size_t i = 0; i < ev.original_header_len && i < 24; ++i) {
            o += snprintf(oh + o, 4, "%02x ",
                          static_cast<unsigned>(ev.original_header[i]));
          }
          uint32_t ok_klen = 0, ok_vlen = 0, ok_magic = 0;
          if (ev.original_header_len >= 24) {
            std::memcpy(&ok_magic, ev.original_header, 4);
            std::memcpy(&ok_klen, ev.original_header + 8, 4);
            std::memcpy(&ok_vlen, ev.original_header + 12, 4);
          }
          fprintf(stderr,
                  "ZFDBG-hdrcmp original get_id=%llu attempt=%llu read_id=%llu "
                  "result_addr=%p scratch=%p result_len=%zu header=[%s] "
                  "decode: magic=0x%08x klen=%u vlen=%u parse_rc=%d error_site=%d\n",
                  (unsigned long long)zeroflush::get_trace::CurGetId(),
                  (unsigned long long)zeroflush::get_trace::CurAttemptId(),
                  (unsigned long long)ev.read_id, ev.result_data_addr,
                  ev.scratch_addr, ev.result_size, oh, ok_magic, ok_klen,
                  ok_vlen, ev.orig_parse_rc,
                  zeroflush::get_trace::LastReadError().site);
        }
        // ①a step 1ag（D 段）：A 表对照——该 (part,gen,offset) 在 Append 时
        // 登记的是哪个 key/seq，与当前读取位置的内容是否一致。
        if (zeroflush::get_trace::LocatorDiagEnabled()) {
          zeroflush::get_trace::AppendSite a;
          const bool found = zeroflush::get_trace::LookupAppendSite(
              ref.part_id, ref.gen, ref.offset, &a);
          if (found) {
            fprintf(stderr,
                    "ZFDBG-loccmp D: part=%u gen=%u off=%llu append_id=%llu "
                    "append(key_hash=%llu seq=%llu type=%d)\n",
                    ref.part_id, ref.gen, (unsigned long long)ref.offset,
                    (unsigned long long)a.append_id,
                    (unsigned long long)a.key_hash,
                    (unsigned long long)a.seq, a.type);
          } else {
            fprintf(stderr,
                    "ZFDBG-loccmp D: part=%u gen=%u off=%llu NO_APPEND_RECORD "
                    "(locator 不来自本进程 Append 出口或超出诊断容量)\n",
                    ref.part_id, ref.gen, (unsigned long long)ref.offset);
          }
        }
        // ① **先打印失败那次底层 Read 的原始证据**（不依赖任何重读）。
        {
          const auto& ev = PartitionedWalManager::LastReadEvidence();
          fprintf(stderr,
                  "ZFDBG-readfail raw: read_id=%llu thr=%llu part=%u gen=%u "
                  "stage=%d second=%d off=%llu want=%zu got=%zu st=%s "
                  "result_size=%zu parsed(k=%u v=%u total=%u)\n",
                  (unsigned long long)ev.read_id,
                  (unsigned long long)ev.thread_hash, ev.part, ev.gen, ev.stage,
                  ev.second_read ? 1 : 0, (unsigned long long)ev.offset,
                  ev.requested, ev.returned, ev.ok ? "OK" : "ERR",
                  ev.result_size, ev.parsed_key_len, ev.parsed_val_len,
                  ev.parsed_total);
        }
        if (fs.ok()) {
          // ② 然后才做诊断重读（同一句柄、同一 offset 的独立头解析）。
          char scratch[24];
          rocksdb::Slice hdr;
          const rocksdb::Status rs = rf->Read(ref.offset, 24, &hdr, scratch);
          uint32_t klen = 0, vlen = 0;
          if (rs.ok() && hdr.size() == 24) {
            // 布局：magic(4) cf_id(1) type(1) flags(2) key_len(4) val_len(4) seq(8)
            std::memcpy(&klen, hdr.data() + 8, 4);
            std::memcpy(&vlen, hdr.data() + 12, 4);
            const uint32_t total = zeroflush::ZfRecordLength(klen, vlen);
            std::string rec;
            rec.resize(total);
            std::memcpy(&rec[0], hdr.data(), 24);
            rocksdb::Slice rest;
            const rocksdb::Status rs2 =
                rf->Read(ref.offset + 24, total - 24, &rest, &rec[0] + 24);
            zeroflush::ZfRecordHeader h;
            rocksdb::Slice k, v;
            const rocksdb::Status ds =
                rs2.ok() && rest.size() == total - 24
                    ? DecodeZfRecord(rec.data(), total, &h, &k, &v)
                    : rocksdb::Status::Corruption("short read in diag");
            // D 段补充（用户 2026-09-17）：**原始头字节 hex** + 字段值 +
            // 与 A 段（Append 时登记）的 seq 对照——直接判定"该 offset 处的
            // 内容是否还是写入时的那条记录"。
            {
              char hex[3 * 24 + 1];
              for (int i = 0; i < 24 && i < static_cast<int>(hdr.size()); ++i) {
                snprintf(hex + i * 3, 4, "%02x ", static_cast<unsigned char>(hdr.data()[i]));
              }
              uint32_t mg = 0, kl = 0, vl = 0;
              uint64_t sq = 0;
              if (hdr.size() >= 24) {
                std::memcpy(&mg, hdr.data(), 4);
                std::memcpy(&kl, hdr.data() + 8, 4);
                std::memcpy(&vl, hdr.data() + 12, 4);
                const uint64_t packed = 0;  // seq 在偏移 16..24
                (void)packed;
                std::memcpy(&sq, hdr.data() + 16, 8);
              }
              zeroflush::get_trace::AppendSite ap;
              const bool has_ap = zeroflush::get_trace::LookupAppendSite(
                  ref.part_id, ref.gen, ref.offset, &ap);
              fprintf(stderr,
                      "ZFDBG-readfail D: rf=%p hex=[%s] magic=0x%08x klen=%u "
                      "vlen=%u hdr_seq=%llu | A(append): %s seq=%llu "
                      "key_hash=%llu\n",
                      static_cast<const void*>(rf.get()), hex, mg, kl, vl,
                      (unsigned long long)sq,
                      has_ap ? "found" : "MISSING",
                      has_ap ? (unsigned long long)ap.seq : 0ULL,
                      has_ap ? (unsigned long long)ap.key_hash : 0ULL);
            }
            {
              char fh[3 * 24 + 1] = {0};
              for (int i = 0; i < 24 && i < static_cast<int>(hdr.size()); ++i) {
                snprintf(fh + i * 3, 4, "%02x ",
                         static_cast<unsigned char>(hdr.data()[i]));
              }
              fprintf(stderr,
                      "ZFDBG-hdrcmp fallback_header=[%s] (同一 offset、按路径"
                      "新开句柄)\n", fh);
            }
            fprintf(stderr,
                    "ZFDBG-readfail hdr: want=24 got=%zu st=%s | total=%u "
                    "want2=%u got2=%zu st2=%s | decode=%s key=%s seq=%llu\n",
                    hdr.size(), rs.ToString().c_str(), total, total - 24,
                    rest.size(), rs2.ToString().c_str(), ds.ToString().c_str(),
                    ds.ok() ? k.ToString(true).c_str() : "-",
                    ds.ok() ? (unsigned long long)h.seq : 0ULL);
          } else {
            fprintf(stderr,
                    "ZFDBG-readfail hdr: want=24 got=%zu st=%s (header read "
                    "itself short/failed)\n",
                    hdr.size(), rs.ToString().c_str());
          }
        }
      }
    }
    if (!fs.ok()) {
      NoteGenLostBeforeInstall();
      ROCKS_LOG_WARN(wal_->InfoLog(),
                     "ZF gen lost before install: part=%u gen=%u (cache=%s "
                     "fd=%s)",
                     ref.part_id, ref.gen, s.ToString().c_str(),
                     fs.ToString().c_str());
      return s;
    }
    NoteGenUnregisteredFdFallback();
    ROCKS_LOG_WARN(wal_->InfoLog(),
                   "ZF gen unregistered but file present: part=%u gen=%u "
                   "(fd fallback; cache=%s)",
                   ref.part_id, ref.gen, s.ToString().c_str());
  }
  // step 1ac（用户 2026-09-17）：**短读/截断的有界重试**。
  // 字节级实证：同一 fd、同一 offset 的读取"先失败后立刻成功"，说明失败发生在
  // "文件正在被追加/刷出"的窗口内（独立 fd 看到的是扩展中的文件），而不是数据
  // 从未落盘。这里只做**读可见性**等待（1ms×最多 50 次），不引入 fsync；
  // 超时仍失败则维持原有错误语义（不静默降级）。
  {
    rocksdb::Slice v_first;
    rocksdb::Status rs =
        PartitionedWalManager::ReadFromSealed(rf.get(), ref, out, &v_first);
    if (!rs.ok()) {
      // step 1ad：**默认关闭**（诊断选项 ZF_READ_SHORT_RETRY=<n>）。修复验收
      // 必须在关闭它时通过（1ms×50 未能消除失败）。
      const char* rr = getenv("ZF_READ_SHORT_RETRY");
      const int n_retry = rr != nullptr ? atoi(rr) : 0;
      for (int i = 0; i < n_retry && !rs.ok(); ++i) {
        env_->SleepForMicroseconds(1000);
        rs = PartitionedWalManager::ReadFromSealed(rf.get(), ref, out, &v_first);
      }
      if (rs.ok()) {
        NoteReadShortReadRetried();
        return rocksdb::Status::OK();
      }
      return rs;
    }
    // 首读即成功：沿用下方原有路径（value_cache 写入等）。
  }
  rocksdb::Slice val_slice;
  s = PartitionedWalManager::ReadFromSealed(rf.get(), ref, out, &val_slice);
  // ReadFromSealed 已把 value 拷进 *out（buf 形参），val_slice 仅用于引用。
  if (s.ok() && value_cache_ != nullptr) {
    value_cache_->Insert(locator_slice, new std::string(*out),
                         ValueCacheHelper(), out->size());
  }
  return s;
}



ROCKSDB_NAMESPACE::Status ZeroFlushContext::Recover(ROCKSDB_NAMESPACE::DBImpl* db) {
  std::vector<std::pair<uint32_t, uint32_t>> files;
  ROCKSDB_NAMESPACE::Status s = wal_->ListFiles(&files);
  if (!s.ok()) {
    return s;
  }
  // M3.1：探测孤儿封存代——遍历 ListFiles 发现的实际 part_id 集合，
  // 而非 [0, P)。part_set = { p : (p, ·) ∈ files } ∪ current_table->part_ids()。
  std::vector<uint32_t> known_parts;
  if (tables_ && tables_->current()) {
    known_parts = tables_->current()->part_ids();
  } else {
    known_parts.reserve(zfo_.partitions);
    for (uint32_t i = 0; i < zfo_.partitions; ++i) known_parts.push_back(i);
  }
  std::unordered_set<uint32_t> part_set(known_parts.begin(), known_parts.end());
  for (const auto& [pp, _gg] : files) {
    (void)_gg;
    part_set.insert(pp);
  }
  std::vector<uint32_t> all_parts(part_set.begin(), part_set.end());
  std::sort(all_parts.begin(), all_parts.end());

  // ---- 子提交 2：按 installed_fence 过滤已安装的 generation ----
  // 用户 2026-09-15 定稿：fence 按 **physical_wal_part** 判断（不能用逻辑索引
  // 分区或裸 generation 判断）；`gen <= fence` 的代数据已进 SST ⇒ 只保留物理
  // WAL 文件（审计/引用），**不注册到读索引、不计 pending**。
  // 依据：WAL 文件仍存在 ≠ WAL 数据仍需作为 L0 参与读取（文件存在只说明未回收）。
  ROCKSDB_NAMESPACE::ColumnFamilyData* cfd = db->GetDefaultColumnFamily();
  if (cfd == nullptr) {
    return ROCKSDB_NAMESPACE::Status::Corruption(
        "ZeroFlush: default column family missing during recovery");
  }
  const auto* vs_fence = cfd->current()->storage_info();
  auto zf_gen_installed = [&](uint32_t part, uint32_t gen) {
    ROCKSDB_NAMESPACE::VersionEdit::ZfInstallFence f;
    return vs_fence->GetZfInstallFence(part, &f) && gen <= f.last_generation;
  };
  uint64_t filtered_gens = 0;
  uint64_t filtered_bytes = 0;

  std::vector<std::pair<uint32_t, uint32_t>> orphans;
  uint64_t orphan_bytes = 0;
  for (uint32_t p : all_parts) {
    const uint32_t active = wal_->MaxGen(p, files);
    for (const auto& [pp, gg] : files) {
      if (pp == p && gg < active) {
        if (zf_gen_installed(pp, gg)) {
          ++filtered_gens;  // 已安装：不登记 recovery 孤儿、不计 pending
          filtered_bytes += wal_->GetFileSize(pp, gg, files, env_);
          continue;
        }
        orphans.emplace_back(pp, gg);
        orphan_bytes += wal_->GetFileSize(pp, gg, files, env_);
      }
    }
  }
  if (!orphans.empty()) {
    sealed_cache_->AddRecoveryGens(orphans, orphan_bytes);
    // step 0b-3：恢复期孤儿代也注册（待收养），否则重开后的水位会漏计
    // 已在磁盘上的未物化数据。
    for (const auto& [pp, gg] : orphans) {
      sealed_cache_->RegisterGen(pp, gg, wal_->GetFileSize(pp, gg, files, env_));
    }
  }
  ROCKSDB_NAMESPACE::SequenceNumber max_seq = 0;
  size_t recovered = 0;
  // M3.0：按最小 seq 排序文件，而非按 (part,gen)。序列号是跨分区
  // 交织分配的（如分区 0 的 seq 从 4 开始，分区 1 的 seq 从 1 开始），
  // 按 (part,gen) 顺序插入会导致 MemTable::Add 的 "s >= first_seqno_"
  // 断言失败（s=1 < first_seqno_=4）。见 M3_DESIGN.md §10.1。
  struct FileSortEntry {
    uint32_t part;
    uint32_t gen;
    ROCKSDB_NAMESPACE::SequenceNumber min_seq;
  };
  std::vector<FileSortEntry> sorted;
  sorted.reserve(files.size());
  for (const auto& [part, gen] : files) {
    WalScanner scanner(env_, wal_dir_, part, gen);
    ZfRecordHeader h;
    ROCKSDB_NAMESPACE::Slice key, value;
    ROCKSDB_NAMESPACE::SequenceNumber first_seq = 0;
    if (scanner.Next(&h, &key, &value)) {
      first_seq = h.seq;
    }
    sorted.push_back({part, gen, first_seq});
  }
  // ---- 直接证据插桩（ZF_DBG_GEN_ALLOC=1；用户 2026-09-15）----
  // 目的：区分"代内复用/追加语义"、"分配器回退"、"恢复资格/索引归属"三类假设。
  // 只读、无行为改动；拿到证据前不改号段分配。
  if (getenv("ZF_DBG_GEN_ALLOC") != nullptr) {
    for (uint32_t p : all_parts) {
      ROCKSDB_NAMESPACE::VersionEdit::ZfInstallFence fz;
      const bool has_f = vs_fence->GetZfInstallFence(p, &fz);
      const uint32_t mx = wal_->MaxGen(p, files);
      const std::optional<uint32_t> ag = wal_->ActiveGenOpt(p);
      std::string fl;
      uint32_t min_file_gen = 0;
      bool have_min = false;
      for (const auto& [pp, gg] : files) {
        if (pp != p) {
          continue;
        }
        if (!have_min || gg < min_file_gen) {
          min_file_gen = gg;
          have_min = true;
        }
        uint64_t fs = wal_->GetFileSize(pp, gg, files, env_);
        uint32_t fseq = 0;
        for (const auto& ent : sorted) {
          if (ent.part == pp && ent.gen == gg) {
            fseq = ent.min_seq;
            break;
          }
        }
        fl += " g" + std::to_string(gg) + "(size=" + std::to_string(fs) +
              ",first_seq=" + std::to_string(fseq) + ")";
      }
      fprintf(stderr,
              "ZFDBG-genalloc part=%u fence_present=%d fence_last=%u "
              "max_gen_from_files=%u min_file_gen=%u active=%s files=[%s]\n",
              p, (int)has_f, has_f ? fz.last_generation : 0u, mx,
              have_min ? min_file_gen : 0u,
              ag.has_value() ? std::to_string(*ag).c_str() : "nullopt",
              fl.empty() ? " none" : fl.c_str());
    }
  }
  std::sort(sorted.begin(), sorted.end(),
            [](const FileSortEntry& a, const FileSortEntry& b) {
              return a.min_seq < b.min_seq;
            });
  // M4.8：预创建活跃索引（MaxGen）——InsertCreate 的"active 缺失即创建"
  // 会把 sorted 首个（min_seq 最小）封存代误建为 active，Get 链中 active
  // 优先 → 旧代遮蔽新代（R48 phase3 get@3 读 'C' 而非 'D'）。
  // step 1（修复）：不再用 MaxGen(files) 定义 active 身份——该函数只能表示
  // "文件列表中的最大代"，且在没有文件时返回 0（与"首个有效代"歧义）。
  // 只有真正持有活跃 WAL 的分区才预建 active 索引；其余保持"无 active"
  // （显式 nullopt），重放记录由 InsertCreate 按显式身份归入 frozen。
  for (uint32_t p : all_parts) {
    const std::optional<uint32_t> ag = wal_->ActiveGenOpt(p);
    if (ag.has_value()) {
      index_set_->EnsureActive(p, *ag);
    }
  }
  for (const auto& entry : sorted) {
    // 子提交 2：已安装代（gen <= fence[physical_part]）不参与重放——
    // 其数据已由 install 成功提交进 SST；重放只会把陈旧版本重新挂进读索引，
    // 在读路径"命中即返回"下遮蔽同一 key 的更高 sequence 版本。
    if (zf_gen_installed(entry.part, entry.gen)) {
      ++filtered_gens;
      filtered_bytes += wal_->GetFileSize(entry.part, entry.gen, files, env_);
      continue;
    }
    const uint32_t part = entry.part;
    const uint32_t gen = entry.gen;
    WalScanner scanner(env_, wal_dir_, part, gen);
    ZfRecordHeader h;
    ROCKSDB_NAMESPACE::Slice key, value;
    while (scanner.Next(&h, &key, &value)) {
      // 重建 SlimLocator（恢复后 locator 指向分区 WAL 中的持久副本）
      SlimLocator loc;
      loc.part_id = part;
      loc.gen = gen;
      loc.wal_offset = scanner.offset() - ZfRecordLength(h.key_len, h.val_len);
      const ROCKSDB_NAMESPACE::Slice loc_slice(
          reinterpret_cast<const char*>(&loc), sizeof(loc));
      // M4.3a/4.4b：终态路径——重建分区索引（活跃 WAL 的全部代；locator
      // 指向持久 WAL）。索引 gen 字段不参与查找（Get 查 active+frozen 链），
      // 仅 frozen 索引按 gen 释放——恢复的活跃索引不释放，无碍。
      // M4.10c：索引分区按「当前表」路由（Get 同路由 → 一致）而非写时
      // 分区——ZFPROPS 表版本落后（旧版本 bug）或 hash 遗留时，写时分区
      // 与重开路由错位 → 活跃段数据读不到（R52）。locator.part_id 保持
      // 写时分区（WAL 段文件按写时分区定位，与路由无关）。
      std::string ik;
      ik.reserve(key.size() + 8);
      ik.append(key.data(), key.size());
      ROCKSDB_NAMESPACE::PutFixed64(
          &ik, ROCKSDB_NAMESPACE::PackSequenceAndType(
                   h.seq, static_cast<ROCKSDB_NAMESPACE::ValueType>(h.type)));
      const uint32_t rpart =
          RoutePartIdWithVersion(key, tables_->current_version());
      // 物理身份 = (entry.part, gen)（记录所在 WAL 文件）；索引身份 = rpart。
      // step 1 修复（实测驱动）：**只有记录本身属于本分区、且其代等于本分区
      // 的活跃代时，才允许进 active**；跨物理分区的 gen 号不可比较——
      // 复现实测：物理 part=4 gen=6 的记录被路由进索引 part=10，而 part=10
      // 的 active_gen=1，于是**另一条来自 part=4 gen=1 的旧记录**因"gen 号
      // 相等"被归入 active，读序 active 优先 → 遮蔽 frozen 链中 seq 更高的
      // 新版本（seed2 已在 L0 可见仍返回 seed1）。
      const bool same_phys_part = (entry.part == rpart);
      const std::optional<uint32_t> rec_active =
          same_phys_part ? wal_->ActiveGenOpt(rpart) : std::nullopt;
      index_set_->InsertCreate(rpart, gen, rec_active, ik, loc_slice);
      if (h.seq > max_seq) {
        max_seq = h.seq;
      }
      ++recovered;
    }
  }
  // I4：recovered seq 永远 ≥ 旧 LastSequence（物化路径只会更高），但取 max
  // 防止退化（如 SetLastSequence 之前有更晚的写入）。
  recovery_filtered_gens_ = filtered_gens;
  recovery_filtered_bytes_ = filtered_bytes;
  db->SetZeroFlushLastSequence(
      std::max(db->GetLatestSequenceNumber(), max_seq));
  return ROCKSDB_NAMESPACE::Status::OK();
}

// M4.10c：持久化 ZFPROPS v2（原子写：tmp → rename → SyncDir）。
ROCKSDB_NAMESPACE::Status ZeroFlushContext::PersistZfProps() {
  if (!zfo_.use_zfprops) {
    return ROCKSDB_NAMESPACE::Status::OK();
  }
  const std::string props_path = wal_dir_ + "/ZFPROPS";
  std::vector<ZfPropsTableInfo> tables_info;
  tables_info.reserve(1);
  ZfPropsTableInfo ti;
  ti.version = 0;
  ti.partitions = zfo_.partitions;
  ti.part_ids.clear();
  for (uint32_t i = 0; i < zfo_.partitions; ++i) ti.part_ids.push_back(i);
  ti.boundaries.clear();  // 初始 phase 0 完整信息由 ZFPROPS 全量写入
  tables_info.push_back(ti);
  // 若有更多表（采样后 InstallNewVersion 的），也编码进去。
  for (uint32_t v = 1; v <= tables_->current_version(); ++v) {
    auto tbl = tables_->Get(v);
    if (!tbl) continue;
    ZfPropsTableInfo tiv;
    tiv.version = v;
    tiv.partitions = tbl->partitions();
    tiv.part_ids = tbl->part_ids();
    tiv.boundaries.clear();
    if (!tbl->IsHashMode()) {
      // 从 table 中获得 boundaries（通过 RangeOf 获取每个分区的 hi 边界）。
      for (uint32_t i = 0; i + 1 < tbl->partitions(); ++i) {
        rocksdb::Slice lo, hi;
        tbl->RangeOf(i, &lo, &hi);
        (void)lo;
        tiv.boundaries.emplace_back(hi.data(), hi.size());
      }
    }
    tables_info.push_back(tiv);
  }
  std::string props_data;
  rocksdb::Status es = EncodeZfPropsV2(
      static_cast<uint8_t>(zfo_.routing_mode), ucmp_->Name(), tables_info,
      tables_->current_version(), &props_data);
  if (!es.ok()) {
    return ROCKSDB_NAMESPACE::Status::IOError(
        "ZeroFlush: failed to encode ZFPROPS v2: " + es.ToString());
  }
  const std::string tmp_path = props_path + ".tmp";
  std::unique_ptr<rocksdb::WritableFile> wf;
  rocksdb::Status ws =
      env_->NewWritableFile(tmp_path, &wf, rocksdb::EnvOptions());
  if (ws.ok()) {
    ws = wf->Append(props_data);
    if (ws.ok()) ws = wf->Sync();
    if (ws.ok()) ws = wf->Close();
  }
  if (ws.ok()) {
    ws = env_->RenameFile(tmp_path, props_path);
  }
  if (ws.ok()) {
    std::string dir = props_path.substr(0, props_path.rfind('/'));
    if (!dir.empty()) {
      std::unique_ptr<rocksdb::Directory> dir_obj;
      rocksdb::Status ds = env_->NewDirectory(dir, &dir_obj);
      if (ds.ok() && dir_obj) {
        ds = dir_obj->Fsync();
      }
      if (!ds.ok() && zfo_.use_logger && wal_->InfoLog() != nullptr) {
        ROCKS_LOG_WARN(wal_->InfoLog(), "ZeroFlush: SyncDir(%s) failed: %s",
                       dir.c_str(), ds.ToString().c_str());
      }
    }
  } else {
    env_->DeleteFile(tmp_path).PermitUncheckedError();
    return ROCKSDB_NAMESPACE::Status::IOError(
        "ZeroFlush: failed to write ZFPROPS v2: " + ws.ToString());
  }
  return ROCKSDB_NAMESPACE::Status::OK();
}

// ---------------------------------------------------------------------------
// ZeroFlushV0 P1（S4 merge：取消/合并冷分区——无在途数据版）
// ---------------------------------------------------------------------------

void ZeroFlushContext::EnterInflightGroup() {
  inflight_groups_.fetch_add(1, std::memory_order_acq_rel);
}

void ZeroFlushContext::ExitInflightGroup() {
  const uint32_t prev = inflight_groups_.fetch_sub(1, std::memory_order_acq_rel);
  assert(prev > 0);  // 组计数必须配对（Enter 一次/组，Exit 恰好一次/组）
  (void)prev;
}

uint32_t ZeroFlushContext::InflightWriterGroups() const {
  return inflight_groups_.load(std::memory_order_acquire);
}

ROCKSDB_NAMESPACE::Status ZeroFlushContext::MergePartitionLocked(
    ROCKSDB_NAMESPACE::DBImpl* impl, ROCKSDB_NAMESPACE::ColumnFamilyData* cfd,
    uint32_t drain_part, uint32_t* absorb_out) {
  // REQUIRES: DB mutex held（与 InstallNewVersion 的排他窗口一致）。
  auto tbl = tables_ ? tables_->current() : nullptr;
  if (tbl == nullptr || tbl->IsHashMode()) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "ZeroFlush: merge requires an active range-routing table "
        "(kStatic/kSampled/kAlignL1 已安装后)");
  }
  if (drain_part == zeroflush::kRangeDelPartId) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "ZeroFlush: cannot merge the range-deletion partition");
  }
  const auto& ids = tbl->part_ids();
  if (std::find(ids.begin(), ids.end(), drain_part) == ids.end()) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "ZeroFlush: drain partition not in current table");
  }
  // 写屏障：等待在途写组归零。在途写组（已在锁内捕获路由版本、正于锁外
  // 做分区 WAL 追加 + 索引插入）不再取 DB mutex，持锁自旋无死锁；新写组
  // 需 DB mutex 才能捕获版本 → 屏障后不存在仍按旧版本路由到 drain 的写者。
  // 防御性超时（正常窗口 µs–ms 级）：超时不合并、状态不变，可重试。
  const uint64_t kInflightWaitUs = 2ull * 1000 * 1000;
  const uint64_t deadline = env_->NowMicros() + kInflightWaitUs;
  while (InflightWriterGroups() != 0) {
    if (env_->NowMicros() > deadline) {
      return ROCKSDB_NAMESPACE::Status::TimedOut(
          "ZeroFlush: inflight writer groups did not drain");
    }
    std::this_thread::yield();
  }
  // ---- 前置（ZeroFlushV0 P1b/M1a）：drain 不得有"非活跃"的未物化残留 ----
  // 允许：(a) 完全排空（ActiveSize==0），或 (b) 活跃尾（ActiveSize>0，本次
  // 就地冻结为 E_D）。禁止：frozen 索引 / pending sealed WAL gens / skip·
  // recovery 残留——它们会延后进 post-merge epoch，其序保障需要 PlanLocked
  // 的 F1 强物化（merge_into_base_level=true 时），属 M1b 范围；默认
  // merge-off 下 drain 直装不攒批，无此残留即可保证排空先于吸收方。
  if (index_set_ != nullptr && index_set_->HasFrozen(drain_part)) {
    return ROCKSDB_NAMESPACE::Status::Busy(
        "ZeroFlush: drain partition has unflushed frozen indexes");
  }
  if (sealed_cache_ != nullptr &&
      sealed_cache_->HasPendingPartGens(drain_part)) {
    return ROCKSDB_NAMESPACE::Status::Busy(
        "ZeroFlush: drain partition has pending sealed WAL gens");
  }
  const uint64_t active_bytes =
      wal_ != nullptr ? wal_->ActiveSize(drain_part) : 0;

  // ZeroFlushV0 P1b/M1b：活跃尾合并的序倒置防护 = F1（PlanLocked 对
  // DRAINING 分区强制物化不攒批）——E_D 冻结代在切路由后的表中不存在
  // （孤儿直装），绝不被 kSkip 延后到吸收方更新版本之后。F1 已接线，
  // merge_into_base_level 任意取值均安全。

  // ---- 路由切换（先于冻结；纯内存+落盘，不释放 DB mutex）----
  // 派生去掉 drain 的新表版本并安装（对新写即时生效；drain 历史 L1 文件
  // 保留，由吸收方后续常规合并按 S1/S3 吸收收敛）。切路由必须先于冻结：
  // 冻结中的 ZfSwitchMemtable 会临时释放 DB mutex（M2.3：WAL 创建让锁），
  // 若冻结先行，让锁窗口内新写组按旧版本捕获 → 追加进 drain 新活跃代 →
  // 滞留（MergeActiveTail 实测 act 回涨）。先切表后，让锁窗口内的写组按
  // 新版本路由 → 全部归吸收方，drain 无新写。
  const auto& dids = tbl->part_ids();
  const size_t idx = static_cast<size_t>(
      std::find(dids.begin(), dids.end(), drain_part) - dids.begin());
  rocksdb::Slice lo_s, hi_s;
  tbl->RangeOf(static_cast<uint32_t>(idx), &lo_s, &hi_s);
  uint32_t absorb = 0;
  std::shared_ptr<PartitionTable> nt;
  rocksdb::Status s =
      tbl->DeriveMerge(drain_part, tbl->version() + 1, &absorb, &nt);
  if (!s.ok()) {
    return s;
  }
  tables_->InstallNewVersion(nt);
  // 持久化路由版本：重开后路由一致（同 kSampled/kAlignL1 学习安装）。
  s = PersistZfProps();
  if (!s.ok()) {
    // 新表已在内存生效（后续写组已按新版本捕获）；落盘失败不回滚——
    // 崩溃恢复一致性属 P5 恢复协议范畴（此处尽力而为并上抛）。
    return s;
  }

  // ---- 冻结活跃尾为 E_D（切路由后；se.table_version = 新表版本）----
  // 与 FreezeBatchPartitions 的单分区语义同构：wal Freeze + index
  // Freeze(gen+1) + epoch 登记（AddEpochWithRecoveryAdoption 顺带收养既有
  // skip/recovery——前置保证其中不含 drain 的，其他分区的照常收养无碍）+
  // 空 mem 切表触发该 epoch 的物化。drain 已不在新表中 → E_D 按孤儿路径
  // 直装（R59C：lo/hi 空 → 按实际数据范围建单文件，冲突落 L0，随后被
  // l0_overlap 融合吸收；读序由 L0 的'同 key 取最大 seq'保证）。
  if (active_bytes > 0) {
    const uint64_t epoch = epoch_counter_.fetch_add(1) + 1;
    const FreezeResult fr = wal_->Freeze(drain_part);
    // step 1q（用户 2026-09-16 第 2 项）：Freeze 即登记——该代进入唯一
    // registry（sealed && !installed ⇒ 计入 pending）；此后 epoch 收养只是
    // 归属迁移。避免已封存但无主的代被 ReleaseGens 当可回收，也避免让位时
    // 无人接管（组 5 NotFound 的根因）。
    if (sealed_cache_ != nullptr && fr.sealed_bytes > 0) {
      sealed_cache_->RegisterGen(drain_part, fr.old_gen, fr.sealed_bytes);
    }
    if (fr.sealed_bytes > 0) {
      index_set_->Freeze(drain_part, fr.old_gen + 1);
    }  // step 1ab：封存中止时不换代
    SealedEpoch se;
    se.epoch = epoch;
    se.table_version = tables_ ? tables_->current_version() : 0;
    if (fr.sealed_bytes > 0) {
      se.gens.emplace_back(drain_part, fr.old_gen);
      if (fr.old_gen == 0) {
        se.has_fresh_gen0 = true;
      }
      se.part_bytes[drain_part] = fr.sealed_bytes;
      se.total_bytes = fr.sealed_bytes;
    }
    wal_->ClearOverTargetFlag();
    RegisterSealedEpoch(se);
    ROCKSDB_NAMESPACE::MemTable* old_mem = cfd->mem();
    if (old_mem != nullptr) {
      old_mem->SetZfEpoch(epoch);
    }
    const rocksdb::Status ss = impl->ZfSwitchMemtable(cfd);
    if (!ss.ok()) {
      return ss;
    }
  }
  // 登记 DRAINING（仅活跃尾合并需要：E_D 排空完成前，drain 原区间 key 的
  // 别名读与 retire 判定依赖本记录）。完全排空的合并（无冻结）不登记。
  if (active_bytes > 0) {
    DrainingPart dp;
    dp.part = drain_part;
    dp.absorb = absorb;
    dp.lo.assign(lo_s.data(), lo_s.size());
    dp.hi.assign(hi_s.data(), hi_s.size());
    {
      std::lock_guard<std::mutex> l(draining_mu_);
      const bool exists =
          std::any_of(draining_.begin(), draining_.end(),
                      [drain_part](const DrainingPart& d) {
                        return d.part == drain_part;
                      });
      if (!exists) {
        draining_.push_back(std::move(dp));
      }
    }
    draining_active_.store(static_cast<uint32_t>(draining_.size()),
                           std::memory_order_release);
    SyncDrainingFile();  // ZeroFlushV0 P1c：登记落盘
  }
  merge_count_.fetch_add(1, std::memory_order_relaxed);
  if (absorb_out != nullptr) {
    *absorb_out = absorb;
  }
  return ROCKSDB_NAMESPACE::Status::OK();
}

ROCKSDB_NAMESPACE::Status ZeroFlushContext::SplitPartitionLocked(
    ROCKSDB_NAMESPACE::DBImpl* impl, ROCKSDB_NAMESPACE::ColumnFamilyData* cfd,
    uint32_t part, const ROCKSDB_NAMESPACE::Slice& split_key,
    uint32_t* child_l, uint32_t* child_r, uint64_t* tail_epoch_out,
    uint64_t* hold_token_out) {
  // REQUIRES: DB mutex held（与 InstallNewVersion 的排他窗口一致）。
  if (tail_epoch_out != nullptr) {
    *tail_epoch_out = 0;
  }
  if (hold_token_out != nullptr) {
    *hold_token_out = 0;
  }
  auto tbl = tables_ ? tables_->current() : nullptr;
  if (tbl == nullptr || tbl->IsHashMode()) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "ZeroFlush: split requires an active range-routing table "
        "(kStatic/kSampled/kAlignL1 已安装后)");
  }
  if (part == zeroflush::kRangeDelPartId) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "ZeroFlush: cannot split the range-deletion partition");
  }
  const auto& ids = tbl->part_ids();
  const auto id_it = std::find(ids.begin(), ids.end(), part);
  if (id_it == ids.end()) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "ZeroFlush: split partition not in current table");
  }
  // 父区间快照（活跃尾孤儿 epoch 的范围覆盖，必须在切路由前取得）。
  const size_t part_idx =
      static_cast<size_t>(id_it - ids.begin());
  rocksdb::Slice parent_lo, parent_hi;
  tbl->RangeOf(static_cast<uint32_t>(part_idx), &parent_lo, &parent_hi);
  const uint64_t active_bytes =
      wal_ != nullptr ? wal_->ActiveSize(part) : 0;
  // 写屏障：等待在途写组归零（同 MergePartitionLocked——切表后不存在仍按
  // 旧版本路由的写者）。
  const uint64_t kInflightWaitUs = 2ull * 1000 * 1000;
  const uint64_t deadline = env_->NowMicros() + kInflightWaitUs;
  while (InflightWriterGroups() != 0) {
    if (env_->NowMicros() > deadline) {
      return ROCKSDB_NAMESPACE::Status::TimedOut(
          "ZeroFlush: inflight writer groups did not drain");
    }
    std::this_thread::yield();
  }
  // ---- 前置 ----
  // 默认（governance_active_tail_split=false）：要求分区完全排空（quiescent
  // 子集，行为与 M2 一致）。实验开关开启时放宽为"活跃尾可拆"——活跃尾在
  // 切路由后冻结为带父区间覆盖的孤儿尾 epoch（见函数尾），由
  // split_orphans_ 别名读保读序、排空后 type-3 rewrite 两分。
  if (!zfo_.governance_active_tail_split) {
    if (index_set_ != nullptr && index_set_->HasFrozen(part)) {
      return ROCKSDB_NAMESPACE::Status::Busy(
          "ZeroFlush: split partition has unflushed frozen indexes");
    }
    if (sealed_cache_ != nullptr &&
        sealed_cache_->HasPendingPartGens(part)) {
      return ROCKSDB_NAMESPACE::Status::Busy(
          "ZeroFlush: split partition has pending sealed WAL gens");
    }
    if (wal_ != nullptr && wal_->ActiveSize(part) != 0) {
      return ROCKSDB_NAMESPACE::Status::Busy(
          "ZeroFlush: split partition must be fully drained (no active bytes)");
    }
  }
  // 分区不得是 DRAINING 方（排空未完成，其别名区间跨切点会错位）或别名
  // 目标（drain 区间 key 的别名回查依赖 absorb 单一 id）。
  {
    std::lock_guard<std::mutex> l(draining_mu_);
    for (const auto& d : draining_) {
      if (d.part == part || d.absorb == part) {
        return ROCKSDB_NAMESPACE::Status::Busy(
            "ZeroFlush: split partition involved in active drain");
      }
    }
  }
  // ---- 分配两子 id + 派生新表 + 切路由（对新写即时生效）----
  const uint32_t l = AllocatePartId();
  const uint32_t r = AllocatePartId();
  // ZeroFlushV0 M5（跨批互斥）：活跃尾将被冻结为宽区间孤儿 epoch，其融合
  // 替换安装必须与子分区批次互斥——先以独立 token 占住两子分区（任一被
  // 在飞批次占用则 Busy，策略退避后重试）；尾 epoch 排空+rewrite 完成后由
  // ZfSplitPartition 释放。期间子分区批次规划让位（kSkip→recovery，后续
  // epoch 收养，序正确）。
  // 无条件持有（quiescent 拆分同样需要：切路由后子分区批次若先安装 L1，
  // 随后的 type-3 rewrite 加子文件会与之重叠）。调用方（ZfSplitPartition）
  // 在 rewrite 完成后释放。
  uint64_t tail_hold_token = BeginMaterializeBatch();
  if (!AddMaterializePart(tail_hold_token, l) ||
      !AddMaterializePart(tail_hold_token, r)) {
    EndMaterializeBatch(tail_hold_token);
    return ROCKSDB_NAMESPACE::Status::Busy(
        "ZeroFlush: split children busy (materialize in flight)");
  }
  if (hold_token_out != nullptr) {
    *hold_token_out = tail_hold_token;
  }
  // 子分区 id 可能 > 初始 P-1——先建 WAL 分区对象（gen0；首写延迟开文件）。
  // 分区索引由写路径 Insert 首触达自建（active 索引 + bloom）。
  if (wal_ != nullptr) {
    wal_->EnsurePartition(l);
    wal_->EnsurePartition(r);
  }
  std::shared_ptr<PartitionTable> nt;
  rocksdb::Status s =
      tbl->DeriveSplit(part, split_key, tbl->version() + 1, l, r, &nt);
  if (!s.ok()) {
    return s;
  }
  tables_->InstallNewVersion(nt);
  // 持久化路由版本：重开后路由一致（同 kSampled/kAlignL1 学习安装）。
  s = PersistZfProps();
  if (!s.ok()) {
    // 新表已在内存生效；落盘失败不回滚（同 merge 的崩溃恢复一致性语义）。
    return s;
  }
  // ---- ZeroFlushV0 M4（活跃尾 split）：切路由后冻结父活跃尾为孤儿 epoch ----
  // 顺序说明：先切路由再冻结（同 merge）——冻结的 ZfSwitchMemtable 会为 WAL
  // 创建短暂释放 DB mutex，若先冻结，让锁窗口内新写仍按旧表路由进父分区
  // 的新活跃代 → 之后父不在表中、该代永不封存 → 数据丢失。切路由后让锁
  // 窗口内的写按子分区路由，父新代恒为空（ActiveSize 归零）。
  // 冻结代带父区间覆盖（has_range_override）：物化按父原区间落单文件，
  // 排空后由调用方 type-3 rewrite 在 split_key 处两分给两子分区。
  if (zfo_.governance_active_tail_split && active_bytes > 0 &&
      wal_ != nullptr) {
    const uint64_t epoch = epoch_counter_.fetch_add(1) + 1;
    const FreezeResult fr = wal_->Freeze(part);
    if (fr.sealed_bytes > 0) {  // step 1ab：封存中止时不换代
      index_set_->Freeze(part, fr.old_gen + 1);
    }
    SealedEpoch se;
    se.epoch = epoch;
    se.table_version = tables_ ? tables_->current_version() : 0;
    if (fr.sealed_bytes > 0) {
      se.gens.emplace_back(part, fr.old_gen);
      if (fr.old_gen == 0) {
        se.has_fresh_gen0 = true;
      }
      se.part_bytes[part] = fr.sealed_bytes;
      se.total_bytes = fr.sealed_bytes;
    }
    se.has_range_override = true;
    se.range_lo.assign(parent_lo.data(), parent_lo.size());
    se.range_hi.assign(parent_hi.data(), parent_hi.size());
    se.split_tail = tail_hold_token != 0;
    se.split_child_l = l;
    se.split_child_r = r;
    wal_->ClearOverTargetFlag();
    RegisterSealedEpoch(se);
    ROCKSDB_NAMESPACE::MemTable* old_mem = cfd->mem();
    if (old_mem != nullptr) {
      old_mem->SetZfEpoch(epoch);
    }
    const rocksdb::Status ss = impl->ZfSwitchMemtable(cfd);
    if (!ss.ok()) {
      return ss;
    }
    // 别名读登记（E 排空前，子分区 miss 的 key 回查父索引链）。
    {
      std::lock_guard<std::mutex> lk(split_orphan_mu_);
      SplitOrphanPart op;
      op.part = part;
      op.child_l = l;
      op.child_r = r;
      split_orphans_.push_back(op);
      split_orphan_active_.store(
          static_cast<uint32_t>(split_orphans_.size()),
          std::memory_order_release);
    }
    if (tail_epoch_out != nullptr) {
      *tail_epoch_out = epoch;
    }
  }
  split_count_.fetch_add(1, std::memory_order_relaxed);
  if (child_l != nullptr) {
    *child_l = l;
  }
  if (child_r != nullptr) {
    *child_r = r;
  }
  return ROCKSDB_NAMESPACE::Status::OK();
}

// step 1ar：指定代封存（测试适配层）——复用生产封存流程。
int ZeroFlushContext::ForceSealForTest(ROCKSDB_NAMESPACE::DBImpl* impl,
                                      ROCKSDB_NAMESPACE::ColumnFamilyData* cfd,
                                      uint32_t part, uint32_t expect_gen,
                                      uint32_t* sealed_gen_out) {
  if (impl == nullptr || cfd == nullptr || wal_ == nullptr) {
    return -1;
  }
  const std::optional<uint32_t> active = wal_->ActiveGenOpt(part);
  // 已换代：核对**原目标代**的封存状态即可（不必继续制造新代）。
  if (active.has_value() && *active != expect_gen) {
    GenSealMeta meta;
    const bool sealed = wal_->GetSealMeta(part, expect_gen, &meta);
    if (sealed_gen_out != nullptr) *sealed_gen_out = expect_gen;
    return sealed ? 1 : -1;
  }
  if (!active.has_value() || *active != expect_gen) {
    return -1;  // 无活跃代且非目标代
  }
  // 走完整生产封存流程：由 DBImpl 的公开包装持锁后调用（与写路径封存同一入口）。
  uint64_t epoch_out = 0;
  const ROCKSDB_NAMESPACE::Status st =
      impl->ZfForceSealPartition(this, cfd, part, &epoch_out);
  if (!st.ok()) {
    return -1;
  }
  GenSealMeta meta;
  const bool sealed = wal_->GetSealMeta(part, expect_gen, &meta);
  if (sealed_gen_out != nullptr) *sealed_gen_out = expect_gen;
  return sealed ? 0 : -1;
}

ROCKSDB_NAMESPACE::Status ZeroFlushContext::FreezePartitionTailLocked(
    ROCKSDB_NAMESPACE::DBImpl* impl, ROCKSDB_NAMESPACE::ColumnFamilyData* cfd,
    uint32_t part, uint64_t* epoch_out) {
  // REQUIRES: DB mutex held。镜像 MergePartitionLocked 的 E_D 冻结段
  //（单分区封存），但不切路由/不登记 DRAINING——供生长拆分前排空活跃尾。
  if (wal_ == nullptr || wal_->ActiveSize(part) == 0) {
    return ROCKSDB_NAMESPACE::Status::OK();  // 已排空，无需冻结
  }
  const uint64_t epoch = epoch_counter_.fetch_add(1) + 1;
  const FreezeResult fr = wal_->Freeze(part);
  if (fr.sealed_bytes == 0) {
    return ROCKSDB_NAMESPACE::Status::OK();  // step 1ab：封存中止（不可见）
  }
  index_set_->Freeze(part, fr.old_gen + 1);
  SealedEpoch se;
  se.epoch = epoch;
  se.table_version = tables_ ? tables_->current_version() : 0;
  if (fr.sealed_bytes > 0) {
    se.gens.emplace_back(part, fr.old_gen);
    if (fr.old_gen == 0) {
      se.has_fresh_gen0 = true;
    }
    se.part_bytes[part] = fr.sealed_bytes;
    se.total_bytes = fr.sealed_bytes;
  }
  wal_->ClearOverTargetFlag();
  RegisterSealedEpoch(se);
  ROCKSDB_NAMESPACE::MemTable* old_mem = cfd->mem();
  if (old_mem != nullptr) {
    old_mem->SetZfEpoch(epoch);
  }
  const rocksdb::Status ss = impl->ZfSwitchMemtable(cfd);
  if (epoch_out != nullptr) {
    *epoch_out = epoch;
  }
  return ss;
}

bool ZeroFlushContext::DrainingAbsorbOf(uint32_t part,
                                        uint32_t* absorb) const {
  if (absorb == nullptr) return false;
  std::lock_guard<std::mutex> l(draining_mu_);
  for (const auto& d : draining_) {
    if (d.part == part) {
      *absorb = d.absorb;
      return true;
    }
  }
  return false;
}

bool ZeroFlushContext::IsDrainingPartition(uint32_t part) const {
  std::lock_guard<std::mutex> l(draining_mu_);
  return std::any_of(draining_.begin(), draining_.end(),
                     [part](const DrainingPart& d) { return d.part == part; });
}

size_t ZeroFlushContext::DrainingPartitions() const {
  std::lock_guard<std::mutex> l(draining_mu_);
  return draining_.size();
}

namespace {
// DRAINING sidecar：每行 "part\tab absorb\tab hex(lo)\tab hex(hi)"。
std::string HexEncode(const std::string& in) {
  static const char* kHex = "0123456789abcdef";
  std::string out;
  out.reserve(in.size() * 2);
  for (unsigned char c : in) {
    out.push_back(kHex[c >> 4]);
    out.push_back(kHex[c & 0xf]);
  }
  return out;
}
bool HexDecode(const std::string& in, std::string* out) {
  if (in.size() % 2 != 0) {
    return false;
  }
  auto val = [](char c) -> int {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
  };
  out->clear();
  out->reserve(in.size() / 2);
  for (size_t i = 0; i < in.size(); i += 2) {
    const int hi = val(in[i]);
    const int lo = val(in[i + 1]);
    if (hi < 0 || lo < 0) {
      return false;
    }
    out->push_back(static_cast<char>((hi << 4) | lo));
  }
  return true;
}
}  // namespace

void ZeroFlushContext::SyncDrainingFile() {
  // best-effort：DRAINING 持久化只影响崩溃后 F1/retire 的延续，读正确性不
  // 依赖（恢复期重放按当前表路由）。写失败忽略（与学习安装的 ZFPROPS
  // best-effort 语义一致）。
  std::string body;
  {
    std::lock_guard<std::mutex> l(draining_mu_);
    for (const auto& d : draining_) {
      body += std::to_string(d.part) + "\t" + std::to_string(d.absorb) +
              "\t" + HexEncode(d.lo) + "\t" + HexEncode(d.hi) + "\n";
    }
  }
  const std::string path = wal_dir_ + "/ZF_DRAINING";
  const std::string tmp = path + ".tmp";
  {
    std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
    if (!f) {
      return;
    }
    f.write(body.data(), static_cast<std::streamsize>(body.size()));
    f.close();
    if (!f) {
      return;
    }
  }
  if (std::rename(tmp.c_str(), path.c_str()) != 0) {
    return;
  }
}

void ZeroFlushContext::LoadDrainingFile() {
  const std::string path = wal_dir_ + "/ZF_DRAINING";
  std::ifstream f(path, std::ios::binary);
  if (!f) {
    return;  // 无文件 = 空注册表（首次/无合并）
  }
  std::vector<DrainingPart> loaded;
  std::string line;
  while (std::getline(f, line)) {
    if (line.empty()) {
      continue;
    }
    // 解析 4 个 \t 分隔字段。
    std::vector<std::string> parts;
    size_t pos = 0;
    while (pos <= line.size()) {
      const size_t tab = line.find('\t', pos);
      parts.push_back(line.substr(pos, tab == std::string::npos
                                          ? std::string::npos
                                          : tab - pos));
      if (tab == std::string::npos) {
        break;
      }
      pos = tab + 1;
    }
    if (parts.size() != 4) {
      continue;  // 损坏行跳过
    }
    DrainingPart d;
    char* endp = nullptr;
    d.part = static_cast<uint32_t>(std::strtoul(parts[0].c_str(), &endp, 10));
    if (endp == parts[0].c_str()) {
      continue;
    }
    endp = nullptr;
    d.absorb = static_cast<uint32_t>(std::strtoul(parts[1].c_str(), &endp, 10));
    if (endp == parts[1].c_str()) {
      continue;
    }
    if (!HexDecode(parts[2], &d.lo) || !HexDecode(parts[3], &d.hi)) {
      continue;
    }
    loaded.push_back(std::move(d));
  }
  if (loaded.empty()) {
    return;
  }
  {
    std::lock_guard<std::mutex> l(draining_mu_);
    for (auto& d : loaded) {
      const bool exists =
          std::any_of(draining_.begin(), draining_.end(),
                      [&d](const DrainingPart& x) { return x.part == d.part; });
      if (!exists) {
        draining_.push_back(std::move(d));
      }
    }
  }
  draining_active_.store(static_cast<uint32_t>(draining_.size()),
                         std::memory_order_release);
}

void ZeroFlushContext::MaybeRetireDraining() {
  // DRAINING 分区排空完成（无未回收 WAL 段、无 frozen 索引、无活跃字节）
  // → RETIRED（移除登记）。在 ReleaseEpoch/ReleaseGens 等 epoch 回收点调用。
  if (draining_active_.load(std::memory_order_acquire) == 0) {
    return;
  }
  std::vector<uint32_t> retired;
  {
    std::lock_guard<std::mutex> l(draining_mu_);
    for (auto it = draining_.begin(); it != draining_.end();) {
      const uint32_t p = it->part;
      const bool frozen =
          index_set_ != nullptr && index_set_->HasFrozen(p);
      const bool pending =
          sealed_cache_ != nullptr && sealed_cache_->HasPendingPartGens(p);
      const uint64_t act = wal_ != nullptr ? wal_->ActiveSize(p) : 0;
      if (!frozen && !pending && act == 0) {
        retired.push_back(p);
        it = draining_.erase(it);
      } else {
        // M1c② 诊断：env 门控探针（ZF_DBG_RETIRE=1）——打印阻止 retire
        // 的判据（frozen/pending/act 哪一项为真）。
        if (getenv("ZF_DBG_RETIRE") != nullptr) {
          fprintf(stderr,
                  "ZFDBG-retire part=%u keep frozen=%d pending=%d act=%llu\n",
                  p, frozen ? 1 : 0, pending ? 1 : 0,
                  static_cast<unsigned long long>(act));
        }
        ++it;
      }
    }
  }
  if (!retired.empty()) {
    draining_active_.store(
        draining_active_.load(std::memory_order_relaxed) -
            static_cast<uint32_t>(retired.size()),
        std::memory_order_release);
    merge_retired_.fetch_add(retired.size(), std::memory_order_relaxed);
    SyncDrainingFile();  // ZeroFlushV0 P1c：retire 清盘
  }
}

void ZeroFlushContext::MaybeRetireSplitOrphans() {
  // ZeroFlushV0 M4（活跃尾 split）：孤儿尾 epoch 排空完成（无 frozen 索引、
  // 无 pending sealed WAL gens）→ 移除别名登记（数据已进 SST，读走原生
  // SST 查找；父 id 不再需要回查）。
  if (split_orphan_active_.load(std::memory_order_acquire) == 0) {
    return;
  }
  std::lock_guard<std::mutex> l(split_orphan_mu_);
  for (auto it = split_orphans_.begin(); it != split_orphans_.end();) {
    if (!HasPendingPartData(it->part)) {
      it = split_orphans_.erase(it);
    } else {
      ++it;
    }
  }
  split_orphan_active_.store(static_cast<uint32_t>(split_orphans_.size()),
                             std::memory_order_release);
}

size_t ZeroFlushContext::SplitOrphans() const {
  return split_orphan_active_.load(std::memory_order_acquire);
}

uint32_t ZeroFlushContext::AllocatePartId() {
  // REQUIRES: DB mutex held（Split/MergePartitionLocked 的分配窗口，与
  // InstallNewVersion 同互斥域——分配即消费 next_part_id_，无锁安全）。
  // kRangeDelPartId（0xFFFFFFFE）为 range-del 保留 id，单调分配正常生命周期
  // 不可能接近；仅防御。
  assert(next_part_id_ < kRangeDelPartId && "partition id space exhausted");
  return next_part_id_++;
}

void ZeroFlushContext::RebuildNextPartId() {
  // Open 时调用（Recover 后）。next_part_id_ = max(全部表版本 part_ids ∪
  // zfwal 残留文件 part) + 1。表版本在 ZFPROPS 中全量保留、文件列出实际
  // 分区的物理 id——任意分配过的 id（含已 merge 退役、崩溃时仍有残留 WAL 的
  // drain id）必在两者之一，故 max 重建可复现全部已用 id，无需持久化计数器。
  uint32_t mx = 0;
  const uint32_t cv = tables_ ? tables_->current_version() : 0;
  for (uint32_t v = 0; v <= cv; ++v) {
    auto t = tables_ ? tables_->Get(v) : nullptr;
    if (t == nullptr) {
      continue;
    }
    for (uint32_t pid : t->part_ids()) {
      mx = std::max(mx, pid);
    }
  }
  std::vector<std::pair<uint32_t, uint32_t>> files;
  if (wal_ != nullptr && wal_->ListFiles(&files).ok()) {
    for (const auto& f : files) {
      mx = std::max(mx, f.first);
    }
  }
  next_part_id_ = mx + 1;
}

ROCKSDB_NAMESPACE::Status MergePartition(ROCKSDB_NAMESPACE::DB* db,
                                         uint32_t drain_part,
                                         uint32_t* absorb_out) {
  if (db == nullptr) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "ZeroFlush::MergePartition: db must not be null");
  }
  auto* impl = static_cast<ROCKSDB_NAMESPACE::DBImpl*>(db);
  return impl->ZfMergePartition(drain_part, absorb_out);
}

ROCKSDB_NAMESPACE::Status SplitPartition(ROCKSDB_NAMESPACE::DB* db,
                                         uint32_t part,
                                         const ROCKSDB_NAMESPACE::Slice& split_key,
                                         uint32_t* child_l,
                                         uint32_t* child_r) {
  if (db == nullptr) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "ZeroFlush::SplitPartition: db must not be null");
  }
  auto* impl = static_cast<ROCKSDB_NAMESPACE::DBImpl*>(db);
  return impl->ZfSplitPartition(part, split_key, child_l, child_r);
}

ROCKSDB_NAMESPACE::Status GrowPartitions(ROCKSDB_NAMESPACE::DB* db,
                                         uint32_t* splits_out) {
  if (db == nullptr) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "ZeroFlush::GrowPartitions: db must not be null");
  }
  auto* impl = static_cast<ROCKSDB_NAMESPACE::DBImpl*>(db);
  return impl->ZfGrowPartitions(splits_out);
}

ROCKSDB_NAMESPACE::Status RunGovernanceTick(ROCKSDB_NAMESPACE::DB* db) {
  if (db == nullptr) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "ZeroFlush::RunGovernanceTick: db must not be null");
  }
  auto* impl = static_cast<ROCKSDB_NAMESPACE::DBImpl*>(db);
  return impl->ZfGovernanceTick();
}

ROCKSDB_NAMESPACE::Status StopGovernance(ROCKSDB_NAMESPACE::DB* db) {
  if (db == nullptr) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "ZeroFlush::StopGovernance: db must not be null");
  }
  auto* impl = static_cast<ROCKSDB_NAMESPACE::DBImpl*>(db);
  impl->ZfStopRepartitionController();
  return ROCKSDB_NAMESPACE::Status::OK();
}

// ---------------------------------------------------------------------------
// Open / Close
// ---------------------------------------------------------------------------

ROCKSDB_NAMESPACE::Status Open(const ROCKSDB_NAMESPACE::Options& opt,
                               const ZeroFlushOptions& zfo,
                               const std::string& dbname,
                               std::unique_ptr<ROCKSDB_NAMESPACE::DB>* db) {
  // 1) 替换 memtable factory（Slim，索引-only）
  ROCKSDB_NAMESPACE::Options zf_opt = opt;
  zf_opt.memtable_factory = std::make_shared<SlimMemTableRepFactory>();
  // flush 由封存替代：抑制自动 flush（侵入点②在 DBImpl 层兜底，
  // 这里把 memtable 预算放大避免 write_buffer 触发原生 flush）
  zf_opt.write_buffer_size = std::max(zf_opt.write_buffer_size, (size_t)256 << 20);
  // M3.2：内存上界校验。物化期并行排序的峰值内存 ≈ K × partition_target_bytes
  // （每 worker 同时持有至多 1 个分区的解码数据），须 ≤ 4 × write_buffer_size
  // （M3_DESIGN.md §6.1）。默认 8×64MB=512MB ≤ 4×256MB=1GB 通过。
  const uint32_t K = std::max<uint32_t>(1, zfo.materialize_parallelism);
  if (static_cast<uint64_t>(K) * zfo.partition_target_bytes >
      4ull * zf_opt.write_buffer_size) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument(
        "ZeroFlush: materialize memory bound exceeded (K * "
        "partition_target_bytes must be <= 4 * write_buffer_size)");
  }
  // M4.1c：SlimMemTableRep 已支持并发 memtable 写入（InlineSkipList
  // InsertConcurrently + ConcurrentArena），放开 allow_concurrent_memtable_write
  // 使写组走 parallel 路径（follower 并行插入，消除 M4.1 剖析发现的
  // 写组串行天花板）。pipelin 与 ZF 写路径仍不兼容，保持关闭。
  zf_opt.allow_concurrent_memtable_write = true;
  // pipelined_write 与 ZF 写路径不兼容（WriteGroup leader 串行）
  zf_opt.enable_pipelined_write = false;
  // M5P1b（多 flush 并行）：放宽 M3.2 的"单后台 flush 线程"串行约束。
  // 多 flush 线程并发时批次在 imm 堆积间自发流水（每封存一批分区即一次
  // imm → 可独立成批），物化吞吐不再被单批串行限制。正确性保障：
  //  1) 跨批同分区互斥——每批计划期登记产出分区（ctx mm_* 登记表），
  //     他批同分区 → kSkip 让位（数据转 recovery，批尾释放后下批收养），
  //     单任务批（学习齐批/hash 全范围）整批登记、任一冲突整批让位；
  //  2) last_materialized 推进 max 单调（并发批交错不回退；失败批回滚
  //     在他批推进后自动 no-op——数据由 imm 重试保全，重试批 first_epoch
  //     放行）；
  //  3) 批内 epoch 序断言放宽为单调升序（空洞 = 中间 epoch 在并发批
  //     手中，合法——同分区序由互斥保证）。
  // kAlignL1/kHash 除外（维持单 flush 串行——base 语义）：
  //  - kAlignL1：align 表每 epoch 按当前 L1 重建，分区 id 跨表不稳定
  //    （同 id 范围漂移、异 id 范围互叠）——分区粒度登记无法防跨批重叠
  //    （MemoryBudgetBackpressure 实测 L1 overlap 崩）；
  //  - kHash：全范围单任务 epoch，跨批互斥会整批 skip → recovery 收养链
  //    在并发窗口下放大数据滞留窗口（MultiEpoch 偶发率上升）。
  // 两者均为兼容/测试模式，非 P1 稳态性能路径（kSampled/kStatic 分区
  // 边界版本化后稳定 → 分区粒度互斥充分）。
  //
  // kSampled/kStatic 开启多 flush 并行；kAlignL1/kHash 维持单 flush
  // 串行（base 语义）：
  //  - kAlignL1：align 表每 epoch 按当前 L1 重建，分区 id 跨表不稳定
  //    （同 id 范围漂移、异 id 范围互叠）——分区粒度登记无法防跨批重叠
  //    （MemoryBudgetBackpressure 实测 L1 overlap 崩）；
  //  - kHash：全范围单任务 epoch，skip/recovery 收养链在并发窗口下放大
  //    数据滞留窗口（MultiEpoch 偶发率上升）。
  // kSampled/kStatic 的并行正确性由以下机制共同保证（M5P1b 系列）：
  //  1) 跨批分区互斥登记（ctx mm_*：Begin/Add/EndMaterializeBatch）——
  //     稳态批按分区登记；单任务批（学习齐批/hash 全范围）**全分区**
  //     登记 + 批级占位全分区（输出可覆盖任意分区范围，登记只覆盖自身
  //     gens 会让并发稳态批直装与其切片重叠，Release 实测 #197/#207）；
  //  2) 学习窗串行槽（AcquireGen0Slot/WaitGen0WindowClosed）——含 gen0
  //     代的学习批至多一个在飞（其切片输出依赖串行语义）；稳态批等
  //     学习窗关闭才规划（防直装落入学习批未安装输出分区 → 冲突让位
  //     → gen0 代进 recovery 循环振荡，Release 实测 81 次闸振荡）；
  //  3) last_materialized max 单调 + 批内 epoch 序断言放宽（> prev）；
  //  4) 批次登记延后到安装完成后释放（修 End→LogAndApply 窗口双批替换
  //     竞态，BulkLoadZeroL0 实测）；
  //  5) 规划基版本刷新（修 pick→plan 解锁窗口过期 base_，#180/#173 实测）。
  const bool zf_parallel_flush =
      zfo.routing_mode == zeroflush::ZeroFlushOptions::RoutingMode::kSampled ||
      zfo.routing_mode == zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  // 4 路流水上限：flush worker 批内分区任务各自占用物化线程（8 并发/
  // 批 × 4 批 = 32 ≤ 后台线程预算）。
  zf_opt.max_background_flushes = zf_parallel_flush ? 4 : 1;
  // M4.6：max_background_flushes（非 -1）使 GetBGJobLimits 走"兼容
  // 分支"（max_compactions = max(1, max_background_compactions)），而
  // max_background_compactions 默认 -1 → max(1,-1)=1 → L0 并行 job 被
  // 限制为 1（R41 实测 max_comp=1、num-running-compactions 恒 1——多
  // job 并行失效的根因；R36/R40 的 49.9K 仅来自单 job 的 subcompactions）。
  // 显式配对：compactions = jobs - flushes（R27 验证 max_comp=23 后
  // 连续调度多个 BGWorkCompaction、并行生效）。
  if (zf_opt.max_background_compactions <= 0) {
    zf_opt.max_background_compactions = std::max(
        1, static_cast<int>(zf_opt.max_background_jobs) -
               zf_opt.max_background_flushes);
  }
  // M2.3-1：写流控。设计要求 `max_write_buffer_number = max_pending_epochs + 1`。
  // M5P1b（2026-09-07）：有效水位——并行模式（kSampled/kStatic）用完整
  // 水位（默认 8 = 2× 并行 flush 数 + 余量：允许 imm 积压后 4 路并发
  // 排空——水位 2 会把流水线饿死，实测有效并行度 ~1.1×）；串行模式
  // （kAlignL1/kHash——多 flush 不启用）钳制回 2：align 的逐 epoch L1
  // 重对齐依赖低水位节拍（深 backlog 攒批处理 → align 回落 L0 失控，
  // SteadyStateControlledL0 实测 L0 93-99 vs ≤64）。
  // 原生默认 2 时：第一次封存后 imm=1（无 stall），第二次封存后 imm=2
  // 触发 kStopped → Recalc 创建 StopWriteToken → 第三次写进入 DelayWrite
  // 在 bg_cv 上等待。但 StopWriteToken 仅在 InstallSuperVersion 触发的
  // Recalc 中释放，InstallSuperVersion 只能由写路径触发，写路径又被
  // DelayWrite 卡住，形成不可解死锁。
  // 抬高到 max_write_buffer_number = 有效水位 + 1：让 imm 计数到水位才
  // 触发 stall，BG flush 完成时 imm 下降，InstallSuperVersion 释放 token。
  uint64_t zf_pending_epochs = zfo.max_pending_epochs;
  if (zfo.routing_mode ==
          zeroflush::ZeroFlushOptions::RoutingMode::kAlignL1 ||
      zfo.routing_mode == zeroflush::ZeroFlushOptions::RoutingMode::kHash) {
    zf_pending_epochs = std::min<uint64_t>(zf_pending_epochs, 2);
  }
  if (zf_opt.max_write_buffer_number <
      static_cast<int>(zf_pending_epochs) + 1) {
    zf_opt.max_write_buffer_number =
        static_cast<int>(zf_pending_epochs) + 1;
  }

  // ---- M5（zeroflush0.98）§3.6：原生介入禁用 ----
  // 稳态（P1 后）无 L0、L1→L2 只响应 ZF 手动 range 下沉：
  // 1) dynamic_level_bytes 恒 false——base_level 恒 1（本树默认 true 时
  //    空库 base=6=最底层，ZF 直装落底、无处下沉，M5 架构不成立）；
  // 2) L0 三触发 = 1e8——L0 永不产生也永不消费（装 L0 的决策分支已成
  //    防御/align 专属，P1 稳态配置不可达）；
  // 3) max_bytes_for_level_base 放大到 1GB——L1 score 触发的兜底阈值
  //    （手动下沉让位正常时 L1 稳态 << 1GB；L1 累积超限才原生全范围
  //    兜底——语义正确只是写放大退化）。L2 目标随之放大（multiplier
  //    = 10 → L2 目标 10GB）：下沉累积到 10GB 触发原生 L2→L3 清场
  //    （M5 §3.4"L2+ 完全交给原生"）。
  zf_opt.level_compaction_dynamic_level_bytes = false;
  zf_opt.level0_file_num_compaction_trigger = 100000000;
  zf_opt.level0_slowdown_writes_trigger = 100000000;
  zf_opt.level0_stop_writes_trigger = 100000000;
  // M5 基线结构修复：L0 消费路径。稳态（治理 ON）L0 恒空、无需消费；但基线
  // （governance=0/回退占多）L0 只增不消 → 空间放大（实测 44M 写积 42GB L0）。
  // ZF_L0_TRIGGER=N（N>0）时把 L0→L1 触发降为 N，交给原生 compaction 消费
  // L0（默认保持 1e8 = 不消费，零行为改动）。
  if (const char* env_l0 = getenv("ZF_L0_TRIGGER")) {
    const int v = atoi(env_l0);
    if (v > 0) {
      zf_opt.level0_file_num_compaction_trigger = v;
    }
  }
  // M5 归因实验：L1→L2 触发阈值可经 ZF_MAX_L1_BYTES 覆盖（默认 1GB）。
  // 背景：100GB 基线（governance=0）时 L1≈10GB → score≈10 → 原生 L1→L2
  // compaction 持续运行，与 ZF 物化抢文件（being_compacted 冲突 → kSkip），
  // 物化积压（sealed_wal 37.9GB）后写路径卡滞。调大该值可隔离验证。
  zf_opt.max_bytes_for_level_base = 1ull << 30;
  if (const char* env_l1 = getenv("ZF_MAX_L1_BYTES")) {
    const unsigned long long v = strtoull(env_l1, nullptr, 10);
    if (v > 0) {
      zf_opt.max_bytes_for_level_base = v;
    }
  }

  // 2) 原生 Open（MANIFEST/VersionSet/SuperVersion 全复用）
  // M4.6-2：CURRENT 一致性预修复——在原生 Recover 之前。Open/轮换竞态
  // 会使 CURRENT 偶发停留在旧 manifest（R28/R36 实测：CURRENT→000001
  // 空，编辑在轮换后的 000005），原生 Recover 读旧 manifest → 空版本 →
  // 全部 SST 被 PurgeObsoleteFiles 当孤儿删除（重开命中 0.3~35%）。
  // 修复必须在 Recover 之前（之后修复版本已加载，无效）。扫描
  // MANIFEST-* 取最新文件号，大于 CURRENT 指向则重写 CURRENT。
  {
    std::string cur;
    uint64_t cur_mfn = 0;
    std::ifstream cur_ifs(dbname + "/CURRENT");
    if (cur_ifs.good()) {
      std::getline(cur_ifs, cur);
      sscanf(cur.c_str(), "MANIFEST-%" SCNu64, &cur_mfn);
    }
    std::vector<std::string> children;
    auto gc_s = opt.env->GetChildren(dbname, &children);
    if (gc_s.ok()) {
      uint64_t max_mfn = 0;
      for (const auto& f : children) {
        uint64_t n = 0;
        if (sscanf(f.c_str(), "MANIFEST-%" SCNu64, &n) == 1) {
          max_mfn = std::max(max_mfn, n);
        }
      }
      if (max_mfn > cur_mfn) {
        ROCKSDB_NAMESPACE::SetCurrentFile(
            ROCKSDB_NAMESPACE::WriteOptions(), opt.env->GetFileSystem().get(),
            dbname, max_mfn, ROCKSDB_NAMESPACE::Temperature::kUnknown,
            nullptr /* dir_contains_current_file */);
      }
    }
  }
  std::unique_ptr<ROCKSDB_NAMESPACE::DB> opened;
  ROCKSDB_NAMESPACE::Status s =
      ROCKSDB_NAMESPACE::DB::Open(zf_opt, dbname, &opened);
  if (!s.ok()) {
    return s;
  }
  auto* impl = static_cast<ROCKSDB_NAMESPACE::DBImpl*>(opened.get());

  // 3) 创建上下文并挂到 DBImpl / MemTable
  const std::string wal_dir =
      (zf_opt.wal_dir.empty() ? dbname : zf_opt.wal_dir) + "/" + zfo.wal_subdir;
  auto ctx = std::make_shared<ZeroFlushContext>(zfo, wal_dir, impl->GetEnv());
  // ---- M3.1：初始化 PartitionTable ----
  // 获取 user comparator。
  auto* cfd = impl->GetDefaultColumnFamily();
  if (cfd == nullptr) {
    return ROCKSDB_NAMESPACE::Status::Corruption(
        "ZeroFlush: default column family missing");
  }
  const auto* ucmp = cfd->user_comparator();
  // M4.3a：创建分区索引（跳表比较器 = internal comparator）。M4.4b：
  // 终态路径为唯一路径（旧路径移除）。
  ctx->index_set_.reset(
      new PartitionIndexSet(ZfKeyComparator(cfd->internal_comparator())));
  ctx->SetCfd(cfd);  // step 1h：读路径查 installed fence
  ctx->set_ucmp(ucmp);
  // 创建 PartitionTableSet。
  auto table_set = std::unique_ptr<PartitionTableSet>(new PartitionTableSet());
  // 根据路由模式创建初始表（version 0）。
  if (zfo.routing_mode == ZeroFlushOptions::RoutingMode::kStatic) {
    // kStatic：用户提供 P-1 个分隔键。
    if (zfo.static_boundaries.size() != zfo.partitions - 1) {
      return ROCKSDB_NAMESPACE::Status::InvalidArgument(
          "ZeroFlush: static_boundaries size must be partitions - 1");
    }
    std::shared_ptr<PartitionTable> pt;
    ROCKSDB_NAMESPACE::Status ps = PartitionTable::Create(
        0, zfo.static_boundaries, ucmp, &pt);
    if (!ps.ok()) {
      return ps;
    }
    ps = table_set->Init(std::move(pt));
    if (!ps.ok()) return ps;
  } else {
    // kHash 或 kSampled：初始使用 hash 路由（version 0）。
    auto pt = PartitionTable::CreateHash(0, zfo.partitions);
    table_set->Init(std::move(pt)).PermitUncheckedError();
  }
  ctx->tables_ = std::move(table_set);

  // M3.1：kSampled / kAlignL1 模式：创建采样器（kAlignL1 在 L1 不足时
  // 用采样边界兜底，分区数恒定）。
  if (zfo.routing_mode == ZeroFlushOptions::RoutingMode::kSampled ||
      zfo.routing_mode == ZeroFlushOptions::RoutingMode::kAlignL1) {
    ctx->sampler_.reset(
        new KeySampler(zfo.sample_every_n_records, ucmp));
  }

  // ---- M3.1：ZFPROPS v2 读写（支持 v1→v2 原地升级） ----
  if (zfo.use_zfprops) {
    std::string props_path = wal_dir + "/ZFPROPS";
    rocksdb::Env* env = impl->GetEnv();
    // 确保目录存在（幂等）。
    rocksdb::Status ps = env->CreateDirIfMissing(wal_dir);
    if (!ps.ok()) return ps;

    if (env->FileExists(props_path).ok()) {
      // 读取整个文件（v2 变长，最多 4MB 保守上限）。
      std::unique_ptr<rocksdb::SequentialFile> rf;
      ps = env->NewSequentialFile(props_path, &rf, rocksdb::EnvOptions());
      if (!ps.ok()) return ps;
      std::string buf;
      buf.resize(4 * 1024 * 1024);
      rocksdb::Slice result;
      ps = rf->Read(buf.size(), &result, &buf[0]);
      if (!ps.ok()) return ps;
      buf.resize(result.size());
      ZfPropsV2 decoded;
      ps = DecodeZfPropsAuto(buf.data(), buf.size(), &decoded);
      if (!ps.ok()) {
        // ZFPROPS 损坏且 zfwal 非空 → Corruption。
        if (env->FileExists(wal_dir).ok()) {
          std::vector<std::pair<uint32_t, uint32_t>> files;
          ctx->wal()->ListFiles(&files);
          if (!files.empty()) {
            return ROCKSDB_NAMESPACE::Status::Corruption(
                "ZeroFlush: ZFPROPS corrupt but zfwal non-empty");
          }
        }
        // zfwal 空且 ZFPROPS 损坏：视为首次部署，允许覆盖。
      } else {
        // 校验 partitions 一致性。
        const uint32_t persisted_p =
            decoded.tables.empty() ? 0 : decoded.tables[0].partitions;
        if (persisted_p != zfo.partitions) {
          return ROCKSDB_NAMESPACE::Status::InvalidArgument(
              "ZeroFlush: ZFPROPS partitions mismatch (existing=" +
              std::to_string(persisted_p) + ", requested=" +
              std::to_string(zfo.partitions) +
              "); cannot change partitions across reopens");
        }
        // 校验 routing_mode 一致性。
        uint8_t persisted_mode = decoded.routing_mode;
        if (persisted_mode != static_cast<uint8_t>(zfo.routing_mode) &&
            !(persisted_mode == 0 &&
              (zfo.routing_mode == ZeroFlushOptions::RoutingMode::kSampled ||
               zfo.routing_mode == ZeroFlushOptions::RoutingMode::kAlignL1))) {
          // kHash→kSampled/kAlignL1 是安全的（两者首轮均用 hash 写），
          // 其余不匹配应拒绝。
          return ROCKSDB_NAMESPACE::Status::InvalidArgument(
              "ZeroFlush: ZFPROPS routing_mode mismatch");
        }
        // 校验 comparator name（v2 才有；v1 自动跳过）。
        if (decoded.format_version >= 2 &&
            !decoded.comparator_name.empty() &&
            decoded.comparator_name != ucmp->Name()) {
          return ROCKSDB_NAMESPACE::Status::InvalidArgument(
              "ZeroFlush: ZFPROPS comparator mismatch");
        }
        // 恢复边界表（若有已持久化的边界则替换初始表）。
        if (decoded.tables.size() > 1 ||
            (decoded.tables.size() == 1 && decoded.tables[0].version > 0)) {
          // 有多个表的场景需要重建 PartitionTableSet（M3.1b 分裂后）。
          // 此处只处理 version 0 的 hash 表（初始已有），其余由 M3.1b 处理。
          for (const auto& ti : decoded.tables) {
            if (ctx->tables_->Get(ti.version) != nullptr) continue;
            std::shared_ptr<PartitionTable> restored;
            if (ti.boundaries.empty()) {
              restored = PartitionTable::CreateHash(
                  ti.version, ti.partitions);
            } else {
              // ZeroFlushV0 M1c②：必须用持久化的稀疏 part_ids 重建——
              // Create 的默认 [0,P) 会把 merge 移除的 drain 分区 id 重新
              // 分配（CrashMidDrainReopen 实测：重开后新写落入 drain 的
              // WAL，排空永不完成）。part_ids 无法重建时回退默认连续 id
              // （兼容旧文件/损坏容忍，读路径由 SST 承载不受影响）。
              rocksdb::Status cs = PartitionTable::CreateWithPartIds(
                  ti.version, ti.boundaries, ti.part_ids, ucmp, &restored);
              if (!cs.ok()) {
                cs = PartitionTable::Create(ti.version, ti.boundaries, ucmp,
                                            &restored);
              }
              if (!cs.ok()) {
                // 边界无法重建（comparator 变更等）——记录日志但继续。
                if (zfo.use_logger) {
                  ROCKS_LOG_WARN(zf_opt.info_log.get(),
                                "ZeroFlush: cannot restore table v%u: %s",
                                ti.version, cs.ToString().c_str());
                }
                continue;
              }
            }
            ctx->tables_->InstallNewVersion(std::move(restored));
          }
        }
      }
    }

    // 原子写 ZFPROPS v2（tmp → rename）——M4.10c 提取为 ctx 方法，
    // 学习安装（运行中 InstallNewVersion）后同样调用。
    rocksdb::Status ps2 = ctx->PersistZfProps();
    if (!ps2.ok()) {
      return ps2;
    }
  }
  s = ctx->Open();
  if (!s.ok()) {
    return s;
  }
  // M3.0 R4：use_logger 时把 zf 日志接入 DB info log（ROCKS_LOG_* 生效；
  // 默认 nullptr 时 ROCKS_LOG_* 为 no-op）。
  if (zfo.use_logger && zf_opt.info_log != nullptr) {
    ctx->wal()->SetInfoLog(zf_opt.info_log.get());
  }
  // M4.7b：value cache（LRU，键 = locator 16B）
  if (zfo.value_cache_bytes > 0) {
    ctx->value_cache_ = ROCKSDB_NAMESPACE::NewLRUCache(zfo.value_cache_bytes);
  }
  impl->SetZeroFlushContext(ctx);

  // 4) 恢复：重放已有分区 WAL（崩溃恢复 / 重开场景）
  s = ctx->Recover(impl);
  if (!s.ok()) {
    return s;
  }
  // ZeroFlushV0 P2（M2）：分区 id 分配器初始化（Recover 列出全部文件后——
  // 表版本 + 文件 part 的 max 重建，见 RebuildNextPartId 注释）。
  ctx->RebuildNextPartId();
  // ZeroFlushV0 M4：动态治理后台控制器（zfo.dynamic_governance=true 时启
  // 后台线程按 governance_tick_interval_micros 周期自驱）。Close/析构时由
  // DBImpl::ZfStopRepartitionController join。
  if (zfo.dynamic_governance) {
    impl->ZfStartRepartitionController();
  }
  *db = std::move(opened);
  return ROCKSDB_NAMESPACE::Status::OK();
}

void Close(std::shared_ptr<ROCKSDB_NAMESPACE::DB>* db) {
  if (db && *db) {
    db->reset();
  }
}

// ---------------------------------------------------------------------------
// DestroyDB：原生 DestroyDB + 递归清理 zfwal 子目录
// ---------------------------------------------------------------------------

ROCKSDB_NAMESPACE::Status DumpKeyIndexChain(ROCKSDB_NAMESPACE::DB* db,
                                            const std::string& user_key,
                                            std::string* out) {
  auto* impl = static_cast<ROCKSDB_NAMESPACE::DBImpl*>(db);
  std::shared_ptr<ZeroFlushContext> ctx = impl->GetZeroFlushContext();
  if (ctx == nullptr || ctx->index_set() == nullptr) {
    return ROCKSDB_NAMESPACE::Status::InvalidArgument("not a ZeroFlush DB");
  }
  const ROCKSDB_NAMESPACE::Slice uk(user_key);
  const uint32_t part = ctx->RoutePartId(uk);
  char head[256];
  const std::optional<uint32_t> ag = ctx->wal()->ActiveGenOpt(part);
  std::vector<std::pair<uint32_t, uint32_t>> all_files;
  ctx->wal()->ListFiles(&all_files);
  std::string fl;
  for (const auto& [pp, gg] : all_files) {
    if (pp == part) {
      fl += " g" + std::to_string(gg);
    }
  }
  snprintf(head, sizeof(head),
           "  part=%u active_gen=%s files=[%s] ucmp=%s\n", part,
           ag.has_value() ? std::to_string(*ag).c_str() : "nullopt",
           fl.c_str(), ctx->ucmp() != nullptr ? ctx->ucmp()->Name() : "-");
  out->append(head);
  ctx->index_set()->DumpKeyChain(part, uk, out);
  return ROCKSDB_NAMESPACE::Status::OK();
}

ROCKSDB_NAMESPACE::Status DestroyDB(const std::string& dbname,
                                    const ROCKSDB_NAMESPACE::Options& options,
                                    const ZeroFlushOptions& zfo) {
  // Step 1：原生 DestroyDB 清理 manifest/sst/log/current 等。
  // 失败不直接返回 —— 仍要尝试清理 zfwal，避免半清理残留。
  ROCKSDB_NAMESPACE::Status s =
      ROCKSDB_NAMESPACE::DestroyDB(dbname, options);
  // Step 2：清理 wal_dir/zfwal 子目录（含 ZFPROPS + 各代分区文件）。
  // wal_dir 约定与 Open() 一致：options.wal_dir 非空则用之，否则用 dbname。
  ROCKSDB_NAMESPACE::Env* env = options.env;
  if (env == nullptr) {
    env = ROCKSDB_NAMESPACE::Env::Default();
  }
  const std::string wal_dir =
      (options.wal_dir.empty() ? dbname : options.wal_dir) + "/" +
      zfo.wal_subdir;
  std::vector<std::string> children;
  ROCKSDB_NAMESPACE::Status ls = env->GetChildren(wal_dir, &children);
  if (ls.ok()) {
    for (const auto& c : children) {
      if (c == "." || c == "..") continue;
      // zfwal 里只可能放文件（每个分区一个代文件 + ZFPROPS），
      // DeleteFile 对非空子目录会失败但我们不关心（M2 暂不嵌套）。
      env->DeleteFile(wal_dir + "/" + c).PermitUncheckedError();
    }
    // 删完文件后再删目录本身。PermitUncheckedError：zfwal 不存在时
    // 也要吞掉 NotFound，避免污染原 DestroyDB 的失败状态。
    env->DeleteDir(wal_dir).PermitUncheckedError();
  } else {
    // zfwal 不存在也是合法的（首次部署的 DB 还从未创建过 ZF WAL），
    // 此时 ls 返回 NotFound — 视为 OK。
  }
  // M3.0 修复：原生 DestroyDB 的 DeleteDir(dbname) 因 zfwal 子目录
  // 非空而失败（PermitUncheckedError 吞掉了错误），zfwal 清理完成后
  // 必须重试删除 dbname 目录本身，否则目录残留。
  env->DeleteDir(dbname).PermitUncheckedError();
  return s;
}

}  // namespace zeroflush
