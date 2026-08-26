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
//  - kFallback：不融合（互斥冲突/孤儿代）→ 单文件 + PickInstallLevel；
//  - kSkip（M4.5b 攒批）：比例不足（批次小不融合）→ 本 epoch 不产出，
//    封存 WAL 移交 recovery 集合，下个 epoch 收养后多代合并物化（数据
//    在 frozen 索引 + recovery WAL 保持可读，消除小批次的 L0 回落）。
enum class MaterializeDecision : uint8_t { kDirect, kMergeBase, kFallback, kSkip };

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
  // M4.9 L0 融合：被替换的 L0 文件号（安装循环 DeleteFile(0, num)——
  // 与 base 层替换分列，层级不同）。
  std::vector<uint64_t> replaced_l0_file_numbers;
  // 批内链式替换：本输出已被同批次后序融合输出替代（不安装其文件，
  // 物理文件由替换者 Run() 返回前删除；M3.3 §7.4 批次内多 epoch 互斥）。
  bool superseded = false;
};

class ZfMaterializeJob;

// M4.8 迁移路径：分区物化任务（job, part_id）——全局任务池的调度单元。
// 物化执行从「epoch 批次串行」改为「(part, gen) 任务池并行」：跨 epoch 的
// 不同分区自由并行，同分区 gen 序由阶段 0 的决策序（epoch 序）保证。
struct MaterializeTask {
  ZfMaterializeJob* job = nullptr;
  uint32_t part_id = 0;
};

// M4.8：全局分区任务池并行执行。workers 个线程抢任务；任一失败即停其余
// 并返回首个错误（调用方负责清理各 job 输出与释放 Compaction 注册）。
ROCKSDB_NAMESPACE::Status RunMaterializeTaskPool(
    std::vector<MaterializeTask>* tasks, uint32_t workers);

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

// 物化一个 epoch：M4.8 起由调用方（ZfMaterializeAllEpochs）按三阶段驱动，
// 跨 epoch 共享全局任务池并行（原 Run() 的「epoch 内 K 路并行 + epoch 间
// 串行」升级为「全部 epoch 分区任务统一并行」）：
//   - PlanLocked()      阶段 0（持 DB mutex）：逐 epoch 按序决策（epoch 序
//                       = 同分区 gen 序）+ Compaction 注册；
//   - CollectTasks()    收集本 epoch 的分区任务入全局任务池（kSkip 分区
//                       不收集）；
//   - ExecutePartition() 阶段 1（无锁）：执行单个分区任务（可由任意
//                       worker 线程调用，多 job 并发安全）；
//   - FinalizeLocked()  阶段 2（持 DB mutex）：定层 + 批内链式替换 +
//                       释放 Compaction 注册；输出追加 batch_outputs_；
//   - DeleteOrphanFiles() / HandOffSkipped()：安装前收尾（解锁后调用）。
// 安装（单次 VersionEdit）与 kSkip 移交由调用方执行。
class ZfMaterializeJob {
 public:
  // epoch：待物化 epoch 号（= mems_[i]->GetZfEpoch()）。
  // se：SealedFileCache 中该 epoch 的登记信息（GetSealedEpoch 取得）。
  // table：se.table_version 对应的路由表（范围断言用；hash 模式跳过断言）。
  // batch_outputs：本 FlushJob 批次已放置的输出（跨 epoch 共享，定层时避免
  //   同层重叠，见 M3_DESIGN.md §6.2 的 L0/更浅层检查）；由调用方持有，
  //   FinalizeLocked() 成功后把本 job 的输出（已定层）追加进去。
  ZfMaterializeJob(ZeroFlushContext* ctx, uint64_t epoch, const SealedEpoch& se,
                   std::shared_ptr<PartitionTable> table,
                   const ZfMaterializeCtx& mc,
                   std::vector<MaterializeOutput>* batch_outputs);

  ZfMaterializeJob(const ZfMaterializeJob&) = delete;
  ZfMaterializeJob& operator=(const ZfMaterializeJob&) = delete;

  uint64_t epoch() const { return epoch_; }

  // 阶段 0（须持 DB mutex）：逐分区做融合归并触发判定（§7.2）并构造/
  // 注册 Compaction（§7.3）。决策写入 plans_；冲突（being_compacted/
  // 边界越界）→ 直装优先（M4.8）：kSkip 等待（gen 上限强制落地兜底）。
  ROCKSDB_NAMESPACE::Status PlanLocked();

  // 阶段 1 任务收集（阶段 0 后调用）：本 epoch 全部待物化分区任务追加到
  // tasks（kSkip 攒批分区不收集——数据留 frozen 索引 + 封存 WAL）。
  void CollectTasks(std::vector<MaterializeTask>* tasks);

  // 阶段 1（无锁）：物化一个分区（WalScanner 顺序整读 → 排序 → 范围断言
  // → BuildTable / 融合归并）。由全局任务池的任意 worker 调用；多 job 的
  // 分区任务可自由并行（数据独立），同分区 gen 序由决策序保证。
  ROCKSDB_NAMESPACE::Status ExecutePartition(uint32_t part_id);

  // 阶段 2（须持 DB mutex）：逐文件定层 / 回填融合元信息 / 批内链式替换
  // / 释放全部已注册 Compaction。输出追加 batch_outputs_。
  ROCKSDB_NAMESPACE::Status FinalizeLocked();

  // 解锁后收尾：删除批内被替换（superseded）输出的物理文件；kSkip 分区
  // 的封存 WAL 移交 recovery 集合（可读、不 unlink）。
  void DeleteOrphanFiles();
  void HandOffSkipped();

  // 本 epoch 排序累计耗时（微秒），计入 zf.materialize_sort_micros。
  uint64_t sort_micros() const { return sort_micros_; }

  // 错误清理（任务池失败时调用）：取走全部已生成输出（调用方删除文件）。
  void DrainOutputs(std::vector<MaterializeOutput>* out);

  // 阶段 0 失败或任务池失败时释放已注册 Compaction（须持 DB mutex）。
  void FinishPlansLocked();

 private:
  // M4.5b：阶段 0 决策为 kSkip 的分区 gens（Run 尾部移交 recovery 集合）。
  std::vector<std::pair<uint32_t, uint32_t>> skipped_gens_;
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
    // M4.5b-2：孤儿收养 epoch 的强制分区直装替换——物化输出（单文件，
    // 含该分区全部待物化代）直装 base 层并替换 overlap 文件（DeleteFile
    // + AddFile），避免回落 L0 触发遮蔽链。与 kMergeBase 的区别：不做
    // 归并（A/B 侧合并），仅替换（输出范围 ⊇ overlap 文件范围）。
    bool force_replace = false;
    int base_level = 0;  // force_replace：目标替换层（PlanLocked 记录）
    // M4.9 L0 融合：L0（及更浅层）与本分区范围重叠的文件（非
    // being_compacted、完全包含）——物化输出合并这些文件（B 侧）后直装
    // base 并替换（DeleteFile(0) + AddFile(base)），L0 恒空、无遮蔽、
    // 无 compaction 压力（打破 fallback→L0→回落 循环）。
    std::vector<ROCKSDB_NAMESPACE::FileMetaData*> l0_overlap;
  };

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
      const std::vector<ROCKSDB_NAMESPACE::FileMetaData*>& l0_overlap,
      const ROCKSDB_NAMESPACE::Compaction* compaction,
      const ROCKSDB_NAMESPACE::Slice& lo,
      const ROCKSDB_NAMESPACE::Slice& hi);

  // 定层（须持 DB mutex）：base_level 直装或回落 L0（M3.2 范围，§3.2 流图）。
  // 可安装 ⟺ L0..base_level 均无文件与本文件 user key 范围重叠
  // （含本批次已放置文件，跨 epoch 的 ABA 防护）。
  int PickInstallLevel(const ROCKSDB_NAMESPACE::InternalKey& smallest,
                       const ROCKSDB_NAMESPACE::InternalKey& largest) const;

  // M4.5b：kSkip 攒批的上限——同一分区最多攒一代（≥上限强制物化，
  // 保证数据最终收敛到 SST，不被 ratio 拒绝无限推迟）。
  static constexpr uint32_t kMaxSkipGenerations = 2;

  // M4.5b：该分区在本 epoch 的待物化代数（含收养的恢复期孤儿代）。
  uint32_t PendingGenCount(uint32_t pid) const;

  ZeroFlushContext* ctx_;
  uint64_t epoch_;
  SealedEpoch se_;
  std::shared_ptr<PartitionTable> table_;
  ZfMaterializeCtx mc_;
  std::vector<MaterializeOutput>* batch_outputs_;

  // worker 输出收集（out_mu_ 保护；任务池 join 后由阶段 2 读取）。
  mutable ROCKSDB_NAMESPACE::port::Mutex out_mu_;
  std::vector<MaterializeOutput> outputs_;
  std::atomic<uint64_t> sort_micros_{0};

  // 批内被替换（superseded）输出的物理文件号；阶段 2 后由
  // DeleteOrphanFiles() 删除（从未安装，仅本批内可见；不删会泄漏 SST）。
  std::vector<uint64_t> orphan_files_;

  // 本批次已注册融合 Compaction 的 existing 文件号（阶段 0 累积）。批内
  // 前序注册会把 existing 文件标记 being_compacted（vstorage 可见），
  // PlanLocked 遍历时必须识别并跳过，而非误判原生 compaction 冲突降级
  // kFallback（§7.4 批内链式替换的配套互斥识别）。
  std::unordered_set<uint64_t> batch_registered_files_;

  // 本 epoch 待物化分区（se.gens 去重排序；阶段 0 计算）。
  std::vector<uint32_t> part_ids_;
  // 阶段 0 决策结果（持锁写入，阶段 1/2 只读；Compaction 由 FinalizeLocked
  // 释放，worker 无锁期间仅经 compaction 指针做只读查询）。
  std::vector<PartitionPlan> plans_;
};

}  // namespace zeroflush
