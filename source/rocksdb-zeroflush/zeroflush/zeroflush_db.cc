//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M1: 全局上下文与 Open 入口实现。

#include "zeroflush/zeroflush_db.h"

#include <cassert>
#include <fstream>
#include <unordered_set>

#include "db/column_family.h"
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
}

ZeroFlushContext::~ZeroFlushContext() = default;

ROCKSDB_NAMESPACE::Status ZeroFlushContext::Open() { return wal_->Open(); }

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
  sealed_cache_->AddEpochWithRecoveryAdoption(se);
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
  // M4.3a：imm 析构（物化完成）时释放该 epoch 的 frozen 索引（数据已进
  // SST，Get 走原生 SST 查找）。须在 SealedFileCache unlink（WAL 删除）
  // 之前——ReleaseEpoch 内先释放索引再归还缓存引用。
  ReleaseFrozenIndexes(epoch);
  if (sealed_cache_ != nullptr) {
    return sealed_cache_->ReleaseEpoch(epoch);
  }
  return 0;
}

size_t ZeroFlushContext::PurgeSealedFiles() {
  if (sealed_cache_ != nullptr) {
    return sealed_cache_->PurgePending();
  }
  return 0;
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
  last_materialized_epoch_.store(e, std::memory_order_release);
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

ROCKSDB_NAMESPACE::Status ZeroFlushContext::AddRecord(
    ROCKSDB_NAMESPACE::MemTable* mem, const ROCKSDB_NAMESPACE::Slice& key,
    const ROCKSDB_NAMESPACE::Slice& value, ROCKSDB_NAMESPACE::ValueType type,
    ROCKSDB_NAMESPACE::SequenceNumber seq,
    ROCKSDB_NAMESPACE::MemTablePostProcessInfo* ppi,
    uint32_t table_version) {
  // 1) 路由 + 分区 WAL 追加（value 的唯一持久副本）
  const uint32_t part =
      (type == ROCKSDB_NAMESPACE::kTypeRangeDeletion)
          ? kRangeDelPartId
          : RouteWithVersion(key, table_version);
  WalRecordRef ref;
  ROCKSDB_NAMESPACE::Status s =
      wal_->Append(part, key, value, static_cast<uint8_t>(type), seq, &ref);
  if (!s.ok()) {
    return s;
  }
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
    // 新活跃索引的 gen = 新 WAL 代（old_gen + 1）——否则 freeze 后写组的
    // 记录（ref.gen = 新代）Insert 时找不到匹配索引被丢弃（数据丢失）。
    index_set_->Freeze(p, fr.old_gen + 1);
    if (fr.sealed_bytes > 0) {
      se.gens.emplace_back(p, fr.old_gen);
      se.part_bytes[p] = fr.sealed_bytes;
      se.total_bytes += fr.sealed_bytes;
    }
  }
  wal_->ClearOverTargetFlag();
  sealed_cache_->AddEpochWithRecoveryAdoption(se);
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
  index_set_->Freeze(target, fr.old_gen);
  SealedEpoch se;
  se.epoch = epoch;
  se.table_version = tables_ ? tables_->current_version() : 0;
  if (fr.sealed_bytes > 0) {
    se.gens.emplace_back(target, fr.old_gen);
    se.part_bytes[target] = fr.sealed_bytes;
    se.total_bytes = fr.sealed_bytes;
  }
  // 清超限标志：该分区已换代（新代从 0 起）；其他分区超限由 Append 的
  // 原子检查重新置位（自愈，最多延迟一个写组）。
  wal_->ClearOverTargetFlag();
  sealed_cache_->AddEpochWithRecoveryAdoption(se);
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
  if (GetSealedEpoch(epoch, &se)) {
    for (const auto& [part, gen] : gens) {
      auto pb = se.part_bytes.find(part);
      if (pb != se.part_bytes.end()) {
        part_bytes[part] = pb->second;
      }
    }
  }
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
    index_set_->ReleaseFrozen(part, gen);
  }
}

// M4.3a：Get 查分区索引（替代 mem/imm 链）。
// 命中返回 true（含 tombstone：type 为 Deletion 时置 NotFound）；未命中
// 返回 false（调用方继续走原生 SST 查找）。
bool ZeroFlushContext::GetFromPartitionIndex(
    const ROCKSDB_NAMESPACE::Slice& user_key,
    ROCKSDB_NAMESPACE::SequenceNumber snapshot,
    ROCKSDB_NAMESPACE::Status* s, std::string* value,
    ROCKSDB_NAMESPACE::MergeContext* merge_context,
    const ROCKSDB_NAMESPACE::MergeOperator* merge_op) const {
  const uint32_t part = Route(user_key);
  ROCKSDB_NAMESPACE::Slice loc;
  ROCKSDB_NAMESPACE::ValueType type;
  ROCKSDB_NAMESPACE::SequenceNumber seq;
  if (!index_set_->Get(part, user_key, snapshot, &loc, &type, &seq)) {
    if (CheckRangeDelCover(user_key, snapshot)) {
      *s = ROCKSDB_NAMESPACE::Status::NotFound();
      return true;
    }
    return false;
  }
  // M3.4：命中时也检查 tombstone 覆盖（DeleteRange 后写的 range 删除
  // 该 key——tombstone seq 更大则覆盖）。精细 seq 比较（数据后写覆盖
  // 旧 tombstone）留后续；当前按"tombstone ≤ snapshot 即覆盖"。
  if (CheckRangeDelCover(user_key, snapshot)) {
    *s = ROCKSDB_NAMESPACE::Status::NotFound();
    return true;
  }
  if (type == ROCKSDB_NAMESPACE::kTypeDeletion ||
      type == ROCKSDB_NAMESPACE::kTypeSingleDeletion ||
      type == ROCKSDB_NAMESPACE::kTypeRangeDeletion) {
    *s = ROCKSDB_NAMESPACE::Status::NotFound();
    return true;
  }
  ROCKSDB_NAMESPACE::Status rs = ReadValue(loc, value);
  if (!rs.ok()) {
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
    index_set_->CollectVersions(part, user_key, snapshot, read_value,
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
    if (has_base && merge_context->GetNumOperands() > 0 && merge_op != nullptr) {
      ROCKSDB_NAMESPACE::MergeOperator::MergeOperationInputV3 input(
          user_key,
          ROCKSDB_NAMESPACE::MergeOperator::MergeOperationInputV3::ExistingValue(
              std::in_place_type<ROCKSDB_NAMESPACE::Slice>, base),
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
  *s = ROCKSDB_NAMESPACE::Status::OK();
  return true;
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
    for (uint32_t p = 0; p < 64 && touched_set != 0; ++p) {
      if (touched_set & (uint64_t{1} << p)) {
        touched->push_back(p == 63 ? zeroflush::kRangeDelPartId : p);
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
    for (uint32_t p = 0; p < 64 && touched_set != 0; ++p) {
      if (touched_set & (uint64_t{1} << p)) {
        touched.push_back(p == 63 ? zeroflush::kRangeDelPartId : p);
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
  if (ref.gen == wal_->ActiveGen(ref.part_id)) {
    rocksdb::Status st = wal_->ReadRecord(ref, out);
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
  // 封存代：SealedFileCache.Get 必须已登记（M2.0 D1 修复后必走此路径）。
  std::shared_ptr<rocksdb::RandomAccessFile> rf;
  rocksdb::Status s = sealed_cache_->Get(ref.part_id, ref.gen, &rf);
  if (!s.ok()) {
    return s;
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

  std::vector<std::pair<uint32_t, uint32_t>> orphans;
  uint64_t orphan_bytes = 0;
  for (uint32_t p : all_parts) {
    const uint32_t active = wal_->MaxGen(p, files);
    for (const auto& [pp, gg] : files) {
      if (pp == p && gg < active) {
        orphans.emplace_back(pp, gg);
        orphan_bytes += wal_->GetFileSize(pp, gg, files, env_);
      }
    }
  }
  if (!orphans.empty()) {
    sealed_cache_->AddRecoveryGens(orphans, orphan_bytes);
  }
  ROCKSDB_NAMESPACE::ColumnFamilyData* cfd = db->GetDefaultColumnFamily();
  if (cfd == nullptr) {
    return ROCKSDB_NAMESPACE::Status::Corruption(
        "ZeroFlush: default column family missing during recovery");
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
  std::sort(sorted.begin(), sorted.end(),
            [](const FileSortEntry& a, const FileSortEntry& b) {
              return a.min_seq < b.min_seq;
            });
  // M4.8：预创建活跃索引（MaxGen）——InsertCreate 的"active 缺失即创建"
  // 会把 sorted 首个（min_seq 最小）封存代误建为 active，Get 链中 active
  // 优先 → 旧代遮蔽新代（R48 phase3 get@3 读 'C' 而非 'D'）。
  for (uint32_t p : all_parts) {
    index_set_->EnsureActive(p, wal_->MaxGen(p, files));
  }
  for (const auto& entry : sorted) {
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
      const uint32_t rpart = RouteWithVersion(key, tables_->current_version());
      index_set_->InsertCreate(rpart, gen, ik, loc_slice);
      if (h.seq > max_seq) {
        max_seq = h.seq;
      }
      ++recovered;
    }
  }
  // I4：recovered seq 永远 ≥ 旧 LastSequence（物化路径只会更高），但取 max
  // 防止退化（如 SetLastSequence 之前有更晚的写入）。
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
  // M3.2：物化按序前提（§6.2）——单后台 flush 线程。多 flush 线程并发时
  // ZfMaterializeJob 的按序断言不成立（epoch 顺序与 last 推进竞态）。
  zf_opt.max_background_flushes = 1;
  // M4.6：max_background_flushes=1（非 -1）使 GetBGJobLimits 走"兼容
  // 分支"（max_compactions = max(1, max_background_compactions)），而
  // max_background_compactions 默认 -1 → max(1,-1)=1 → L0 并行 job 被
  // 限制为 1（R41 实测 max_comp=1、num-running-compactions 恒 1——多
  // job 并行失效的根因；R36/R40 的 49.9K 仅来自单 job 的 subcompactions）。
  // 显式配对：compactions = jobs - flushes（R27 验证 max_comp=23 后
  // 连续调度多个 BGWorkCompaction、并行生效）。
  if (zf_opt.max_background_compactions <= 0) {
    zf_opt.max_background_compactions =
        std::max(1, zf_opt.max_background_jobs - 1);
  }
  // M2.3-1：写流控。设计要求 `max_write_buffer_number = max_pending_epochs + 1`。
  // 原生默认 2 时：第一次封存后 imm=1（无 stall），第二次封存后 imm=2
  // 触发 kStopped → Recalc 创建 StopWriteToken → 第三次写进入 DelayWrite
  // 在 bg_cv 上等待。但 StopWriteToken 仅在 InstallSuperVersion 触发的
  // Recalc 中释放，InstallSuperVersion 只能由写路径触发，写路径又被
  // DelayWrite 卡住，形成不可解死锁。
  // 抬高到 max_pending_epochs+1：让 imm 计数到 max_pending_epochs 才触发
  // stall，BG flush 完成时 imm 下降，InstallSuperVersion 释放 token。
  if (zf_opt.max_write_buffer_number <
      static_cast<int>(zfo.max_pending_epochs) + 1) {
    zf_opt.max_write_buffer_number =
        static_cast<int>(zfo.max_pending_epochs) + 1;
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
              rocksdb::Status cs = PartitionTable::Create(
                  ti.version, ti.boundaries, ucmp, &restored);
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
