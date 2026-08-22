//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M1: 全局上下文与 Open 入口实现。

#include "zeroflush/zeroflush_db.h"

#include <cassert>
#include <unordered_set>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_thread.h"
#include "logging/logging.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/write_batch.h"
#include "util/hash.h"
#include "zeroflush/partition_table.h"
#include "zeroflush/sealed_file_cache.h"
#include "zeroflush/slim_memtable.h"
#include "zeroflush/wal_manager.h"

namespace zeroflush {

using ROCKSDB_NAMESPACE::InfoLogLevel;

namespace {

// WriteBatch 逐记录回调：路由 → 分区 WAL 追加 → Slim MemTable 插入。
// M2.3-2：touched_ 收集本 batch 实际触达的分区分区 ID，sync 阶段只
// fsync 这些分区（替代原 M1 的 SyncAll 全分区 fsync，64x 提升 sync=true
// 的吞吐）。
class ZfBatchHandler : public ROCKSDB_NAMESPACE::WriteBatch::Handler {
 public:
  ZfBatchHandler(ZeroFlushContext* ctx, ROCKSDB_NAMESPACE::ColumnFamilyData* cfd,
                 ROCKSDB_NAMESPACE::SequenceNumber* seq,
                 std::unordered_set<uint32_t>* touched)
      : ctx_(ctx), cfd_(cfd), seq_(seq), touched_(touched) {}

  ROCKSDB_NAMESPACE::Status PutCF(uint32_t column_family_id,
                                  const ROCKSDB_NAMESPACE::Slice& key,
                                  const ROCKSDB_NAMESPACE::Slice& value) override {
    if (column_family_id != 0) {
      return ROCKSDB_NAMESPACE::Status::NotSupported(
          "ZeroFlush M1: multi column family not supported");
    }
    ROCKSDB_NAMESPACE::Status s =
        ctx_->AddRecord(cfd_, key, value, ROCKSDB_NAMESPACE::kTypeValue, *seq_);
    if (s.ok() && touched_ != nullptr) {
      touched_->insert(ctx_->Route(key));
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
    ROCKSDB_NAMESPACE::Status s =
        ctx_->AddRecord(cfd_, key, empty, ROCKSDB_NAMESPACE::kTypeDeletion, *seq_);
    if (s.ok() && touched_ != nullptr) {
      touched_->insert(ctx_->Route(key));
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
        cfd_, key, empty, ROCKSDB_NAMESPACE::kTypeSingleDeletion, *seq_);
    if (s.ok() && touched_ != nullptr) {
      touched_->insert(ctx_->Route(key));
    }
    ++(*seq_);
    return s;
  }

  ROCKSDB_NAMESPACE::Status DeleteRangeCF(
      uint32_t /*column_family_id*/, const ROCKSDB_NAMESPACE::Slice& /*begin*/,
      const ROCKSDB_NAMESPACE::Slice& /*end*/) override {
    return ROCKSDB_NAMESPACE::Status::NotSupported(
        "ZeroFlush M1: DeleteRange not supported");
  }

  ROCKSDB_NAMESPACE::Status MergeCF(
      uint32_t /*column_family_id*/, const ROCKSDB_NAMESPACE::Slice& /*key*/,
      const ROCKSDB_NAMESPACE::Slice& /*value*/) override {
    return ROCKSDB_NAMESPACE::Status::NotSupported(
        "ZeroFlush M1: Merge not supported");
  }

  void LogData(const ROCKSDB_NAMESPACE::Slice& /*blob*/) override {
    // no-op
  }

 private:
  ZeroFlushContext* ctx_;
  ROCKSDB_NAMESPACE::ColumnFamilyData* cfd_;
  ROCKSDB_NAMESPACE::SequenceNumber* seq_;
  std::unordered_set<uint32_t>* touched_;
};

}  // namespace

// ---------------------------------------------------------------------------
// ZeroFlushContext
// ---------------------------------------------------------------------------

ZeroFlushContext::ZeroFlushContext(const ZeroFlushOptions& zfo,
                                   const std::string& wal_dir,
                                   ROCKSDB_NAMESPACE::Env* env)
    : zfo_(zfo), wal_dir_(wal_dir), env_(env) {
  wal_.reset(new PartitionedWalManager(env_, wal_dir_, zfo_.partitions));
  // M2：封存代文件缓存（持 wal_dir_，LRU 由 max_open_sealed_files 控制）。
  sealed_cache_.reset(new SealedFileCache(env_, wal_dir_,
                                          zfo_.max_open_sealed_files,
                                          zfo_.reclaim_sealed_files));
}

ZeroFlushContext::~ZeroFlushContext() = default;

ROCKSDB_NAMESPACE::Status ZeroFlushContext::Open() { return wal_->Open(); }

bool ZeroFlushContext::ShouldSeal() const {
  // 主触发：全分区活跃字节合计 ≥ epoch_target_bytes。
  // 副触发：任一分区 ≥ partition_target_bytes（防倾斜）。
  // M3.1：遍历实际的 part_id 集合而非 [0, P)。
  uint64_t sum = 0, max_one = 0;
  const auto part_ids = wal_->AllPartitionIds();
  for (uint32_t pid : part_ids) {
    const uint64_t sz = wal_->ActiveSize(pid);
    sum += sz;
    if (sz > max_one) max_one = sz;
  }
  return sum >= zfo_.epoch_target_bytes ||
         max_one >= zfo_.partition_target_bytes;
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
  // M3.1：kSampled 模式：首个 epoch 封存时从采样器学习边界并安装新表。
  // 步骤 3），se.table_version 必须保持 0——物化用它取回 hash 表并跳过
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
        // 学习期 epoch 1 用 hash 写入：se.table_version 保持 0（上方初始化
      }
    }
    sampler_->Clear();
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

ROCKSDB_NAMESPACE::Status ZeroFlushContext::AddRecord(
    ROCKSDB_NAMESPACE::ColumnFamilyData* cfd,
    const ROCKSDB_NAMESPACE::Slice& key, const ROCKSDB_NAMESPACE::Slice& value,
    ROCKSDB_NAMESPACE::ValueType type, ROCKSDB_NAMESPACE::SequenceNumber seq) {
  // 1) 路由 + 分区 WAL 追加（value 的唯一持久副本）
  const uint32_t part = Route(key);
  WalRecordRef ref;
  ROCKSDB_NAMESPACE::Status s =
      wal_->Append(part, key, value, static_cast<uint8_t>(type), seq, &ref);
  if (!s.ok()) {
    return s;
  }
  // 2) Slim MemTable 索引插入（只写元数据，不复制 value）
  SlimLocator loc;
  loc.part_id = ref.part_id;
  loc.gen = ref.gen;
  loc.wal_offset = ref.offset;
  const ROCKSDB_NAMESPACE::Slice loc_slice(reinterpret_cast<const char*>(&loc),
                                           sizeof(loc));
  // kv_prot_info = nullptr：M1 关闭 protection（写入选项已在上层拒绝
  // protection_bytes_per_key > 0 的路径）。
  rocksdb::Status add_s = cfd->mem()->Add(seq, type, key, loc_slice,
                                         /*kv_prot_info=*/nullptr,
                                         /*allow_concurrent=*/false,
                                         /*post_process_info=*/nullptr,
                                         /*hint=*/nullptr);
  // M3.1：kSampled 模式下记录采样（仅 epoch 1 学习期）。
  if (add_s.ok() && zfo_.routing_mode == ZeroFlushOptions::RoutingMode::kSampled &&
      sampler_ && epoch_counter_.load() == 0) {
    sampler_->Sample(key);
  }
  return add_s;
}

ROCKSDB_NAMESPACE::Status ZeroFlushContext::WriteGroupToPartitionWal(
    ROCKSDB_NAMESPACE::WriteThread::WriteGroup& wg,
    ROCKSDB_NAMESPACE::SequenceNumber first_seq,
    ROCKSDB_NAMESPACE::ColumnFamilyData* cfd,
    std::vector<uint32_t>* touched) {
  ROCKSDB_NAMESPACE::SequenceNumber seq = first_seq;
  // M2.3-2：收集触达分区。write group leader 串行处理所有 writer，
  // handler 在 Put/Delete 时把 part_id 记入 touched（去重）。
  std::unordered_set<uint32_t> touched_set;
  for (auto* writer : wg) {
    assert(writer != nullptr);
    if (!writer->ShouldWriteToMemtable()) {
      continue;
    }
    ZfBatchHandler handler(this, cfd, &seq, &touched_set);
    ROCKSDB_NAMESPACE::Status s = writer->batch->Iterate(&handler);
    if (!s.ok()) {
      return s;
    }
  }
  // M3.0 R3：sync 移出本方法（与 DB mutex 解耦，见 M3_DESIGN.md §9）。
  // 本方法只负责追加 + 索引；调用方在释放 DB mutex 后按 touched 分区
  // 调 SyncTouchedPartitions 做 fdatasync。
  if (touched != nullptr) {
    touched->assign(touched_set.begin(), touched_set.end());
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
      s = cfd->mem()->Add(h.seq, static_cast<ROCKSDB_NAMESPACE::ValueType>(h.type),
                          key, loc_slice,
                          /*kv_prot_info=*/nullptr,
                          /*allow_concurrent=*/false,
                          /*post_process_info=*/nullptr,
                          /*hint=*/nullptr);
      if (!s.ok()) {
        return s;
      }
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
  // SlimMemTableRep 不支持并发 memtable 写入
  zf_opt.allow_concurrent_memtable_write = false;
  // pipelined_write 与 ZF 写路径不兼容（WriteGroup leader 串行）
  zf_opt.enable_pipelined_write = false;
  // M3.2：物化按序前提（§6.2）——单后台 flush 线程。多 flush 线程并发时
  // ZfMaterializeJob 的按序断言不成立（epoch 顺序与 last 推进竞态）。
  zf_opt.max_background_flushes = 1;
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

  // M3.1：kSampled 模式：创建采样器（学习期 epoch 1）。
  if (zfo.routing_mode == ZeroFlushOptions::RoutingMode::kSampled) {
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
              zfo.routing_mode == ZeroFlushOptions::RoutingMode::kSampled)) {
          // kHash→kSampled 是安全的（学习期用 hash 写），其余不匹配应拒绝。
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

    // 原子写 ZFPROPS v2（tmp → rename）。
    // 用 v2 格式总是向前兼容（即使 hash 模式也写 v2）。
    std::vector<ZfPropsTableInfo> tables_info;
    tables_info.reserve(1);
    ZfPropsTableInfo ti;
    ti.version = 0;
    ti.partitions = zfo.partitions;
    ti.part_ids.clear();
    for (uint32_t i = 0; i < zfo.partitions; ++i) ti.part_ids.push_back(i);
    ti.boundaries.clear();  // 初始 phase 0 完整信息由 ZFPROPS 全量写入
    tables_info.push_back(ti);
    // 若有更多表（采样后 InstallNewVersion 的），也编码进去。
    for (uint32_t v = 1; v <= ctx->tables_->current_version(); ++v) {
      auto tbl = ctx->tables_->Get(v);
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
        static_cast<uint8_t>(zfo.routing_mode),
        ucmp->Name(), tables_info, ctx->tables_->current_version(),
        &props_data);
    if (!es.ok()) {
      return ROCKSDB_NAMESPACE::Status::IOError(
          "ZeroFlush: failed to encode ZFPROPS v2: " + es.ToString());
    }
    // tmp → rename 原子写。
    const std::string tmp_path = props_path + ".tmp";
    std::unique_ptr<rocksdb::WritableFile> wf;
    rocksdb::Status ws = env->NewWritableFile(tmp_path, &wf, rocksdb::EnvOptions());
    if (ws.ok()) {
      ws = wf->Append(props_data);
      if (ws.ok()) ws = wf->Sync();
      if (ws.ok()) ws = wf->Close();
    }
    if (ws.ok()) {
      ws = env->RenameFile(tmp_path, props_path);
    }
    if (ws.ok()) {
      // SyncDir 保证 Rename 的目录元数据持久化。
      // 通过 NewDirectory + Fsync() 实现。
      std::string dir = props_path.substr(0, props_path.rfind('/'));
      if (!dir.empty()) {
        std::unique_ptr<rocksdb::Directory> dir_obj;
        rocksdb::Status ds = env->NewDirectory(dir, &dir_obj);
        if (ds.ok() && dir_obj) {
          ds = dir_obj->Fsync();
        }
        if (!ds.ok() && zfo.use_logger) {
          ROCKS_LOG_WARN(zf_opt.info_log.get(),
                        "ZeroFlush: SyncDir(%s) failed: %s",
                        dir.c_str(), ds.ToString().c_str());
        }
      }
    } else {
      // 清理 tmp 文件（best effort）。
      env->DeleteFile(tmp_path).PermitUncheckedError();
      return ROCKSDB_NAMESPACE::Status::IOError(
          "ZeroFlush: failed to write ZFPROPS v2: " + ws.ToString());
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
