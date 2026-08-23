//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M3.2: ZfMaterializeJob —— 一个 epoch 的物化（K 路并行）+ 层级下探直装。
//
//  对应 M3_DESIGN.md §6/§7：
//   - 输入：SealedEpoch（gens/table_version）+ 该 epoch 写入时的 PartitionTable；
//   - K = materialize_parallelism 个 worker 按 part_id % K 分片，每片负责若干
//     分区：WalScanner 顺序整读 → 按 InternalKeyComparator 排序（保留全部版本，
//     与原生 flush 一致）→ 范围断言（仅范围路由模式）→ BuildTable；
//   - 阶段 0（持锁）做融合归并触发判定（§7.2）并注册 Compaction（§7.3 互斥）；
//   - 阶段 2（持 DB mutex）逐文件定层/回填融合元信息，输出并入调用方的批次
//     集合，由调用方以单次 VersionEdit 安装（§6.3）。
//
//  线程安全：Run() 内部自建 K 个 worker；对象不跨调用共享。

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "db/compaction/compaction.h"
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "zeroflush/partition_table.h"
#include "zeroflush/sealed_file_cache.h"

namespace ROCKSDB_NAMESPACE {
class BlobFileCompletionCallback;
class ColumnFamilyData;
class CompactionPicker;
class EventLogger;
class FileMetaData;
class ImmutableDBOptions;
class InstrumentedMutex;
class IOTracer;
class JobContext;
class LogBuffer;
class MutableCFOptions;
class SeqnoToTimeMapping;
class Statistics;
class VersionSet;
struct FileOptions;
}  // namespace ROCKSDB_NAMESPACE

namespace zeroflush {

class ZeroFlushContext;

// 分区物化决策（M3_DESIGN.md §7.2/§7.3）：
//  - kDirect：base 层无重叠文件 → 单文件 + PickInstallLevel（可能直装）；
//  - kMergeBase：融合归并 → 与 base 层重叠文件归并，输出直装 base 层；
//  - kFallback：不融合（互斥冲突/比例不足/孤儿代）→ 单文件 + PickInstallLevel。
enum class MaterializeDecision : uint8_t { kDirect, kMergeBase, kFallback };

// 一个分区的物化输出：目标安装层 + FileMetaData + 融合归并元信息。
struct MaterializeOutput {
  int level = 0;  // 0 = L0（回落），>0 = 直装层
  ROCKSDB_NAMESPACE::FileMetaData meta;
  uint32_t part_id = 0;  // 来源分区（安装期按 part_id 关联决策）
  MaterializeDecision decision = MaterializeDecision::kDirect;
  // kMergeBase：被本输出替换的 base 层输入文件（existing，指针由
  // mc.base 引用保证存活；仅诊断用，安装循环用 replaced_file_numbers）。
  std::vector<ROCKSDB_NAMESPACE::FileMetaData*> replaced_inputs;
  uint64_t rewritten_bytes = 0;  // kMergeBase：被重写的 base 字节（指标）
  // kMergeBase：全部被替换文件号（existing + 批内前序融合输出）。
  // 安装循环先对每个文件号 edit_->DeleteFile(level, num) 再 AddFile。
  std::vector<uint64_t> replaced_file_numbers;
  // 批内链式替换：本输出已被同批次后序融合输出替代（不安装其文件，
  // 物理文件由替换者 Run() 返回前删除；M3.3 §7.4 批次内多 epoch 互斥）。
  bool superseded = false;
};

// FlushJob 把自身成员打包进本结构传给 ZfMaterializeJob（避免 20+ 参数构造）。
struct ZfMaterializeCtx {
  // DB 目录（BuildTable 的文件名/监听器用；VersionSet 无公开 dbname() 访问器）。
  std::string dbname;
  ROCKSDB_NAMESPACE::ColumnFamilyData* cfd = nullptr;
  const ROCKSDB_NAMESPACE::ImmutableDBOptions* db_options = nullptr;
  const ROCKSDB_NAMESPACE::MutableCFOptions* mutable_cf_options = nullptr;
  const ROCKSDB_NAMESPACE::FileOptions* file_options = nullptr;
  ROCKSDB_NAMESPACE::VersionSet* versions = nullptr;
  ROCKSDB_NAMESPACE::JobContext* job_context = nullptr;
  ROCKSDB_NAMESPACE::LogBuffer* log_buffer = nullptr;
  ROCKSDB_NAMESPACE::CompressionType output_compression;
  ROCKSDB_NAMESPACE::Env::IOPriority io_priority;
  ROCKSDB_NAMESPACE::Statistics* stats = nullptr;
  ROCKSDB_NAMESPACE::EventLogger* event_logger = nullptr;
  ROCKSDB_NAMESPACE::Env::Priority thread_pri;
  std::shared_ptr<ROCKSDB_NAMESPACE::IOTracer> io_tracer;
  std::string db_id;
  std::string db_session_id;
  uint64_t job_id = 0;
  ROCKSDB_NAMESPACE::SequenceNumber earliest_snapshot =
      ROCKSDB_NAMESPACE::kMaxSequenceNumber;
  std::shared_ptr<const ROCKSDB_NAMESPACE::SeqnoToTimeMapping>
      seqno_to_time_mapping;
  std::string full_history_ts_low;
  ROCKSDB_NAMESPACE::BlobFileCompletionCallback* blob_callback = nullptr;
  bool fast_sst_open = false;
  // 定层阶段须持 DB mutex（读 cfd->current()）；由 Run() 内部重取。
  ROCKSDB_NAMESPACE::InstrumentedMutex* db_mutex = nullptr;
  // M3.3：融合归并的基础版本（= FlushJob::base_，PickMemtable 已 Ref）。
  // 阶段 0 持锁时从 base->storage_info() 取 base 层重叠文件；FileMetaData
  // 指针的生存期由调用方的 base_ 引用计数保证（覆盖 Run() 全程）。
  ROCKSDB_NAMESPACE::Version* base = nullptr;
  // M3.3：CompactionPicker（cfd_->compaction_picker()），融合归并时
  // RegisterCompaction/UnregisterCompaction 用（防与原生 compaction 抢文件）。
  ROCKSDB_NAMESPACE::CompactionPicker* compaction_picker = nullptr;
};

// 物化一个 epoch：K 路并行 + 逐文件定层（安装由调用方执行，§6.3 单次 LogAndApply）。
class ZfMaterializeJob {
 public:
  // epoch：待物化 epoch 号（= mems_[i]->GetZfEpoch()）。
  // se：SealedFileCache 中该 epoch 的登记信息（GetSealedEpoch 取得）。
  // table：se.table_version 对应的路由表（范围断言用；hash 模式跳过断言）。
  // batch_outputs：本 FlushJob 批次已放置的输出（跨 epoch 共享，定层时避免
  //   同层重叠，见 M3_DESIGN.md §6.2 的 L0/更浅层检查）；由调用方持有，
  //   Run() 成功后把本 job 的输出（已定层）追加进去。
  ZfMaterializeJob(ZeroFlushContext* ctx, uint64_t epoch, const SealedEpoch& se,
                   std::shared_ptr<PartitionTable> table,
                   const ZfMaterializeCtx& mc,
                   std::vector<MaterializeOutput>* batch_outputs);

  ZfMaterializeJob(const ZfMaterializeJob&) = delete;
  ZfMaterializeJob& operator=(const ZfMaterializeJob&) = delete;

  // 执行 K 路并行物化。
  // 前提：单后台 flush 线程（或按 imm FIFO 串行化）下 epoch 按序物化
  // （M3_DESIGN.md §6.2）；入口做防御性断言。
  // 调用方必须释放 DB mutex；Run() 内部在定层阶段重取 DB mutex。
  // 任一路失败：清理全部已生成的临时 SST 并返回错误；批次输出不追加。
  ROCKSDB_NAMESPACE::Status Run();

  // 本 epoch 排序累计耗时（微秒），计入 zf.materialize_sort_micros。
  uint64_t sort_micros() const { return sort_micros_; }




 private:
  // 阶段 0（持锁）产生的分区决策，供阶段 1 worker 与阶段 2 安装消费。
  struct PartitionPlan {
    uint32_t part_id = 0;
    MaterializeDecision decision = MaterializeDecision::kDirect;
    // kMergeBase：base 层与 RangeOf(part_id) 重叠的文件（持锁取自
    // mc_.base->storage_info()；指针生存期由 mc_.base 引用保证）。
    std::vector<ROCKSDB_NAMESPACE::FileMetaData*> overlap;
    uint64_t overlap_bytes = 0;   // 重叠文件字节合计（rewritten_bytes 指标）
    // kMergeBase：已注册的 Compaction（构造时自动 MarkFilesBeingCompacted）。
    std::unique_ptr<ROCKSDB_NAMESPACE::Compaction> compaction;
    // 批内前序输出的 FileMetaData 副本（保持指针稳定；Compaction 与
    // worker 只经 overlap_all 引用，见下）。
    std::vector<ROCKSDB_NAMESPACE::FileMetaData> batch_meta_copies;
    // kMergeBase：B 侧全部文件 = existing 重叠文件 + 批内前序输出
    // （批内融合输出 ⊇ existing，取其最后一个即可；直装项与 existing
    // 不重叠，二者并存且有序）。指针集合供 Compaction inputs 与 worker。
    std::vector<ROCKSDB_NAMESPACE::FileMetaData*> overlap_all;
    ROCKSDB_NAMESPACE::Slice lo, hi;  // 分区边界（hash 模式为空）
  };

  // 阶段 0（须持 DB mutex）：逐分区做融合归并触发判定（§7.2）并构造/
  // 注册 Compaction（§7.3）。决策写入 plans_；任一冲突 → kFallback（不等待）。
  ROCKSDB_NAMESPACE::Status PlanLocked();

  // 阶段 2（须持 DB mutex）：kMergeBase 输出的 replaced_inputs/rewritten_
  // bytes 回填 + 释放全部已注册 Compaction（Unregister + unmark）。
  // 安装（AddFile/DeleteFile 进 VersionEdit）由调用方执行。
  void FinishPlansLocked();

  // 按 part_id 二分查找阶段 0 的分区决策（plans_ 与 part_ids_ 同序）。
  // 未找到（理论不可达）返回 nullptr。
  const PartitionPlan* FindPlan(uint32_t part_id) const;

  // 物化一个分区：WalScanner 顺序整读（含收养孤儿代的多 gen）→ 排序 →
  // 范围断言（范围模式）→ BuildTable。成功且非空时把 meta 追加到 outputs_。
  ROCKSDB_NAMESPACE::Status MaterializePartition(
      uint32_t part_id,
      const std::vector<std::pair<uint32_t, uint32_t>>& gens,
      const ROCKSDB_NAMESPACE::Slice& lo,
      const ROCKSDB_NAMESPACE::Slice& hi);

  // M3.3 融合归并一个分区（§7.4）：A 侧 = 封存代记录（排序后 VectorIterator），
  // B 侧 = base 层重叠文件的 TableIterator 串接；MergingIterator 归并 →
  // CompactionIterator（复用原生 snapshot/tombstone/merge 语义）→ 按
  // target_file_size_base 切分多文件。seq 前置断言：
  // A.smallest_seqno > B.largest_seqno（孤儿代 epoch 已降级 kFallback）。
  ROCKSDB_NAMESPACE::Status MaterializeMergePartition(
      uint32_t part_id,
      const std::vector<std::pair<uint32_t, uint32_t>>& gens,
      const std::vector<ROCKSDB_NAMESPACE::FileMetaData*>& overlap_all,
      const ROCKSDB_NAMESPACE::Compaction* compaction,
      const ROCKSDB_NAMESPACE::Slice& lo,
      const ROCKSDB_NAMESPACE::Slice& hi);

  // 定层（须持 DB mutex）：base_level 直装或回落 L0（M3.2 范围，§3.2 流图）。
  // 可安装 ⟺ L0..base_level 均无文件与本文件 user key 范围重叠
  // （含本批次已放置文件，跨 epoch 的 ABA 防护）。
  int PickInstallLevel(const ROCKSDB_NAMESPACE::InternalKey& smallest,
                       const ROCKSDB_NAMESPACE::InternalKey& largest) const;

  ZeroFlushContext* ctx_;
  uint64_t epoch_;
  SealedEpoch se_;
  std::shared_ptr<PartitionTable> table_;
  ZfMaterializeCtx mc_;
  std::vector<MaterializeOutput>* batch_outputs_;

  // worker 输出收集（out_mu_ 保护；主线程在 join 后读）。
  mutable ROCKSDB_NAMESPACE::port::Mutex out_mu_;
  std::vector<MaterializeOutput> outputs_;
  ROCKSDB_NAMESPACE::Status first_error_;
  std::atomic<bool> stop_{false};
  std::atomic<uint64_t> sort_micros_{0};

  // 批内被替换（superseded）输出的物理文件号；阶段 2 后、Run() 返回前
  // 删除（从未安装，仅本批内可见；不删会泄漏 SST）。
  std::vector<uint64_t> orphan_files_;

  // 本批次已注册融合 Compaction 的 existing 文件号（阶段 0 累积）。批内
  // 前序注册会把 existing 文件标记 being_compacted（vstorage 可见），
  // PlanLocked 遍历时必须识别并跳过，而非误判原生 compaction 冲突降级
  // kFallback（§7.4 批内链式替换的配套互斥识别）。
  std::unordered_set<uint64_t> batch_registered_files_;

  // 本 epoch 待物化分区（se.gens 去重排序；阶段 0 计算）。
  std::vector<uint32_t> part_ids_;
  // 阶段 0 决策结果（持锁写入，阶段 1/2 只读；Compaction 由 FinishPlansLocked
  // 在 Run() 返回前释放，worker 无锁期间仅经 compaction 指针做只读查询）。
  std::vector<PartitionPlan> plans_;
};

}  // namespace zeroflush
