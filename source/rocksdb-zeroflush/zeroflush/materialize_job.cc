//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M3.2/M3.3: ZfMaterializeJob 实现 —— K 路并行物化 + 层级下探直装
//  + 融合归并（Materialize-into-BaseLevel）。
//
//  对应 M3_DESIGN.md §6/§7：
//   - 阶段 0（持 DB mutex）：逐分区做融合归并触发判定（§7.2）并注册
//     Compaction（§7.3，与原生 compaction 抢文件的互斥）；
//   - 阶段 1（无锁）：K = materialize_parallelism 个 worker 按 part_id % K
//     分片，每片顺序整读分区 WAL（含收养的恢复期孤儿代）→ 排序 →
//     范围断言（仅范围路由模式）→ 直装/回落路径 BuildTable，融合路径
//     MergingIterator + CompactionIterator 归并并按 target_file_size 切分；
//   - 阶段 2（持 DB mutex）：融合输出回填（level=base、replaced_inputs、
//     rewritten_bytes）与指标，直装/回落输出 PickInstallLevel，全部并入
//     调用方批次输出，由调用方以单次 VersionEdit 原子安装；最后统一释放
//     已注册 Compaction（UnregisterCompaction + MarkFilesBeingCompacted）。

#include "zeroflush/materialize_job.h"

#include <algorithm>
#include <limits>
#include <thread>
#include <utility>

#include "db/blob/blob_file_addition.h"
#include "db/builder.h"
#include "db/column_family.h"
#include "db/compaction/compaction_iterator.h"
#include "db/dbformat.h"
#include "db/job_context.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "file/file_util.h"
#include "file/read_write_util.h"
#include "file/filename.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "monitoring/histogram.h"
#include "monitoring/instrumented_mutex.h"
#include "options/cf_options.h"
#include "options/db_options.h"
#include "rocksdb/file_system.h"
#include "rocksdb/types.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "table/unique_id_impl.h"
#include "util/coding.h"
#include "util/vector_iterator.h"
#include "zeroflush/wal_format.h"
#include "zeroflush/wal_manager.h"
#include "zeroflush/zeroflush_db.h"

namespace zeroflush {

// ROCKS_LOG_* 宏在宏展开处要求 InfoLogLevel 可见（logging/logging.h 不
// include env.h）；与 wal_manager.cc 一致，这里显式引入。
using ROCKSDB_NAMESPACE::InfoLogLevel;

namespace {

// 由封存记录构造 internal key（user key + 8B seq/type 尾）。
std::string MakeInternalKey(const rocksdb::Slice& user_key, uint64_t seq,
                            uint8_t type) {
  std::string ik;
  ik.reserve(user_key.size() + 8);
  ik.append(user_key.data(), user_key.size());
  rocksdb::PutFixed64(
      &ik, rocksdb::PackSequenceAndType(seq, static_cast<rocksdb::ValueType>(type)));
  return ik;
}

// M4.6e：物化排序优化——Bytewise user comparator 时构造"memcmp 可排序"
// 的编码键（连续缓冲 + 偏移视图，无逐键堆分配——首版 string 编码因
// 堆分配/移动开销反更慢，R45 实测 684s vs 基线 596s），替代
// InternalKeyComparator 的逐次比较（每次比较都做 user key 提取 + seq
// 解码，是物化排序的大头：R42 实测 561s/50GB、占 wall 15.7%）。
// 编码 = 4B user key 长度（大端，保证变长 key 的"短者小"语义）+ user
// key 字节 + 8B ~(seq<<8|type)（从最高字节逆序取反，使 seq 降序语义变
// 为 memcmp 升序——低位优先会导致排序错误，R3 实测 CompactionIterator
// 顺序断言失败）。比较 = memcmp(pa, pb, min(la,lb))（长度前缀保证
// 长度序）。非 Bytewise comparator 返回 false（调用方走原路径）。
bool BuildSortKeys(const std::vector<std::string>& keys,
                   const rocksdb::Comparator* ucmp, std::string* buf,
                   std::vector<size_t>* off) {
  if (ucmp->Name() != rocksdb::BytewiseComparator()->Name()) {
    return false;
  }
  buf->clear();
  off->clear();
  off->reserve(keys.size());
  buf->reserve(keys.size() * 30);
  for (const auto& ik : keys) {
    const size_t n = ik.size() - 8;  // user key 长度（尾 8B 为 seq/type）
    off->push_back(buf->size());
    buf->push_back(static_cast<char>(n >> 24));
    buf->push_back(static_cast<char>(n >> 16));
    buf->push_back(static_cast<char>(n >> 8));
    buf->push_back(static_cast<char>(n));
    buf->append(ik.data(), n);
    // tail 为小端 (seq<<8|type)，数值比较高位优先——memcmp 需高位在前。
    for (int i = 7; i >= 0; --i) {
      buf->push_back(~ik[ik.size() - 8 + i]);
    }
  }
  return true;
}

// M4.6e：按缓冲编码键（memcmp 序）重排 keys/values（string 移动，O(n)），
// 返回可直接构造"不排序" VectorIterator 的已排序向量。
void ReorderBySortKeys(std::vector<std::string>* keys,
                       std::vector<std::string>* values,
                       const std::string& buf,
                       const std::vector<size_t>& off) {
  std::vector<size_t> idx(keys->size());
  std::iota(idx.begin(), idx.end(), 0);
  const size_t buf_size = buf.size();
  std::sort(idx.begin(), idx.end(), [&](size_t a, size_t b) {
    const char* pa = buf.data() + off[a];
    const char* pb = buf.data() + off[b];
    const size_t la = (a + 1 < off.size() ? off[a + 1] : buf_size) - off[a];
    const size_t lb = (b + 1 < off.size() ? off[b + 1] : buf_size) - off[b];
    const size_t m = la < lb ? la : lb;
    const int r = std::memcmp(pa, pb, m);
    return r < 0 || (r == 0 && la < lb);
  });
  std::vector<std::string> sk, sv;
  sk.reserve(keys->size());
  sv.reserve(values->size());
  for (size_t i : idx) {
    sk.push_back(std::move((*keys)[i]));
    sv.push_back(std::move((*values)[i]));
  }
  *keys = std::move(sk);
  *values = std::move(sv);
}

}  // namespace

ZfMaterializeJob::ZfMaterializeJob(
    ZeroFlushContext* ctx, uint64_t epoch, const SealedEpoch& se,
    std::shared_ptr<PartitionTable> table, const ZfMaterializeCtx& mc,
    std::vector<MaterializeOutput>* batch_outputs)
    : ctx_(ctx),
      epoch_(epoch),
      se_(se),
      table_(std::move(table)),
      mc_(mc),
      batch_outputs_(batch_outputs) {}

// M4.8 迁移路径：全局分区任务池并行执行（阶段 1）。
// 跨 job 的分区任务统一并行（数据独立——各分区读自己的 WAL 段，输出互
// 不依赖）；同分区 gen 序由阶段 0 的决策序（epoch 序）保证，安装序由调用
// 方保证。任一任务失败 → 停止其余 worker，返回首个错误（调用方清理各
// job 输出并释放 Compaction 注册）。
ROCKSDB_NAMESPACE::Status RunMaterializeTaskPool(
    std::vector<MaterializeTask>* tasks, uint32_t workers) {
  if (tasks == nullptr || tasks->empty()) {
    return ROCKSDB_NAMESPACE::Status::OK();
  }
  const uint32_t W =
      std::min<uint32_t>(std::max<uint32_t>(workers, 1), tasks->size());
  if (W <= 1) {
    for (const MaterializeTask& t : *tasks) {
      ROCKSDB_NAMESPACE::Status s = t.job->ExecutePartition(t.part_id);
      if (!s.ok()) {
        return s;
      }
    }
    return ROCKSDB_NAMESPACE::Status::OK();
  }
  std::atomic<size_t> next{0};
  std::atomic<bool> stop{false};
  ROCKSDB_NAMESPACE::port::Mutex err_mu;
  ROCKSDB_NAMESPACE::Status first_error;
  auto worker = [&] {
    while (!stop.load(std::memory_order_relaxed)) {
      const size_t i = next.fetch_add(1, std::memory_order_relaxed);
      if (i >= tasks->size()) {
        break;
      }
      const ROCKSDB_NAMESPACE::Status s =
          (*tasks)[i].job->ExecutePartition((*tasks)[i].part_id);
      if (!s.ok()) {
        bool first = false;
        {
          rocksdb::MutexLock l(&err_mu);
          if (first_error.ok()) {
            first_error = s;
            first = true;
          }
        }
        if (first) {
          stop.store(true, std::memory_order_relaxed);
        }
        return;
      }
    }
  };
  std::vector<std::thread> workers_vec;
  workers_vec.reserve(W);
  for (uint32_t w = 0; w < W; ++w) {
    workers_vec.emplace_back(worker);
  }
  for (auto& t : workers_vec) {
    t.join();
  }
  return first_error;
}

void ZfMaterializeJob::CollectTasks(std::vector<MaterializeTask>* tasks) {
  // kSkip 攒批分区不收集（数据留在 frozen 索引 + 封存 WAL，调用方在
  // imm 出链前 HandOff 移交 recovery 集合，下个 epoch 收养后多代合并）。
  for (uint32_t pid : part_ids_) {
    const PartitionPlan* plan = FindPlan(pid);
    if (plan != nullptr && plan->decision == MaterializeDecision::kSkip) {
      continue;
    }
    tasks->push_back({this, pid});
  }
}

ROCKSDB_NAMESPACE::Status ZfMaterializeJob::ExecutePartition(uint32_t part_id) {
  // 该 part 的全部 gen：正常封存 1 个 + 可能收养的恢复期孤儿/攒批代。
  std::vector<std::pair<uint32_t, uint32_t>> gens;
  for (const auto& g : se_.gens) {
    if (g.first == part_id) {
      gens.push_back(g);
    }
  }
  if (gens.empty()) {
    return ROCKSDB_NAMESPACE::Status::OK();
  }
  const PartitionPlan* plan = FindPlan(part_id);
  if (plan != nullptr && plan->decision == MaterializeDecision::kSkip) {
    return ROCKSDB_NAMESPACE::Status::OK();  // 双保险（CollectTasks 已过滤）
  }
  rocksdb::Slice lo, hi;
  // 仅范围路由模式查询分区边界（hash 模式 boundaries_ 为空，且范围
  // 断言本身也跳过 hash 模式；RangeOf 依赖 boundaries_ 会越界）。
  if (table_ != nullptr && !table_->IsHashMode()) {
    if (plan != nullptr && plan->decision == MaterializeDecision::kMergeBase) {
      lo = plan->lo;
      hi = plan->hi;
    } else {
      table_->RangeOf(part_id, &lo, &hi);
    }
  }
  return (plan != nullptr && plan->decision == MaterializeDecision::kMergeBase)
             ? MaterializeMergePartition(part_id, gens, plan->overlap_all,
                                         plan->l0_overlap,
                                         plan->compaction.get(), lo, hi)
             : MaterializePartition(part_id, gens, lo, hi);
}

ROCKSDB_NAMESPACE::Status ZfMaterializeJob::FinalizeLocked() {
  mc_.db_mutex->AssertHeld();
  // 逐文件定层 / 回填融合元信息（须持 DB mutex）。调用方保证本 job 的
  // worker 已 join（任务池完成），outputs_ 无并发写。
  // 关键：本批已定层的文件须立即写入 batch_outputs_——PickInstallLevel 的
  // 批内重叠检查只扫描 batch_outputs_（历史批次 + 本批已放置项）。若攒到
  // 最后统一追加，同批文件互相看不到对方：hash 模式下多个分区文件键范围
  // 交错重叠，会全部直装同一层 → VersionBuilder force_consistency_checks
  // 报 "L6 has overlapping ranges"（对应 M3_DESIGN.md §6.2 批内互斥）。
  std::vector<MaterializeOutput> outs = std::move(outputs_);
  for (auto& o : outs) {
    if (o.decision == MaterializeDecision::kMergeBase) {
      // 融合输出：直装阶段 0 决策的 base 层（替换 overlap 输入文件）。
      // replaced_inputs/rewritten_bytes 由调用方安装循环消费。
      const PartitionPlan* plan = FindPlan(o.part_id);
      assert(plan != nullptr &&
             plan->decision == MaterializeDecision::kMergeBase);
      o.level = plan->compaction->output_level();
      // M4.11b：安装前复查 compaction 冲突（同 PickInstallLevel）——冲突
      // 时回落 L0 且不替换（被占用文件不可删；L1 旧文件保留，数据 ⊇ 输出
      // 不丢，读走 L0 优先语义正确）。
      if (mc_.compaction_picker != nullptr &&
          mc_.compaction_picker->RangeOverlapWithCompaction(
              o.meta.smallest.user_key(), o.meta.largest.user_key(), o.level)) {
        o.level = 0;
        o.replaced_inputs.clear();
        o.replaced_file_numbers.clear();
        o.replaced_l0_file_numbers.clear();
      }
      o.replaced_inputs = plan->overlap;
      o.rewritten_bytes = plan->overlap_bytes;
      // 替换文件号：existing 重叠文件 + M4.9 L0 融合文件（DeleteFile(0)）。
      for (ROCKSDB_NAMESPACE::FileMetaData* r : plan->overlap) {
        o.replaced_file_numbers.push_back(r->fd.GetNumber());
      }
      for (ROCKSDB_NAMESPACE::FileMetaData* r : plan->l0_overlap) {
        o.replaced_l0_file_numbers.push_back(r->fd.GetNumber());
      }
      // 批内链式替换（§7.4 批次内多 epoch）：本输出与同批次前序输出
      // （level 相同、user key 范围重叠）合并后范围 ⊇ 前序 → 前序由本
      // 输出替代：标记 superseded（不安装）、继承其替换文件号、物理
      // 文件由 DeleteOrphanFiles() 删除。未安装文件无需 DeleteFile（不
      // 在任何已提交版本中），故只继承前序的 replaced_file_numbers。
      if (batch_outputs_ != nullptr) {
        const ROCKSDB_NAMESPACE::Comparator* ucmp2 = mc_.cfd->user_comparator();
        // M4.5b-3：只有融合输出（kMergeBase）能替换批内前序——融合输出
        // 的 B 侧包含前序输出（批内链式替换，§7.4），范围 ⊇ 前序；
        // 非融合输出（kDirect/kFallback/force_replace，含 kSkip 收养的
        // 多代数据）数据内容独立，范围重叠不代表包含 → 标记 superseded
        // 会误删前序数据（R23 实测：收养输出被后序单代输出替换 → 50GB
        // 重开后 99% 数据丢失）。
        if (o.decision != MaterializeDecision::kMergeBase) {
          continue;
        }
        for (MaterializeOutput& x : *batch_outputs_) {
          if (x.superseded || x.level != o.level) {
            continue;
          }
          const ROCKSDB_NAMESPACE::Slice x_smallest =
              x.meta.smallest.user_key();
          const ROCKSDB_NAMESPACE::Slice x_largest = x.meta.largest.user_key();
          if (ucmp2->Compare(o.meta.smallest.user_key(), x_largest) <= 0 &&
              ucmp2->Compare(x_smallest, o.meta.largest.user_key()) <= 0) {
            x.superseded = true;
            orphan_files_.push_back(x.meta.fd.GetNumber());
            for (uint64_t n : x.replaced_file_numbers) {
              o.replaced_file_numbers.push_back(n);
            }
          }
        }
        std::sort(o.replaced_file_numbers.begin(),
                  o.replaced_file_numbers.end());
        o.replaced_file_numbers.erase(
            std::unique(o.replaced_file_numbers.begin(),
                        o.replaced_file_numbers.end()),
            o.replaced_file_numbers.end());
      }
    } else {
      // M4.5b-2：孤儿直装替换——输出直装 base 层并替换 overlap 文件
      // （安装循环 DeleteFile + AddFile，复用 kMergeBase 的替换安装）。
      const PartitionPlan* rplan = FindPlan(o.part_id);
      if (rplan != nullptr && rplan->force_replace) {
        o.level = rplan->base_level;
        for (ROCKSDB_NAMESPACE::FileMetaData* r : rplan->overlap) {
          o.replaced_file_numbers.push_back(r->fd.GetNumber());
        }
        ctx_->install_direct_base_.fetch_add(1, std::memory_order_relaxed);
      } else {
        o.level = PickInstallLevel(o.meta.smallest, o.meta.largest);
        if (o.level == 0) {
          ctx_->install_fallback_l0_.fetch_add(1, std::memory_order_relaxed);
        } else {
          ctx_->install_direct_base_.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
    if (batch_outputs_ != nullptr) {
      batch_outputs_->push_back(o);
    }
  }
  // M3.3 指标：融合归并次数与重写字节（按分区计，§7.2/§13）。
  for (const PartitionPlan& p : plans_) {
    if (p.decision == MaterializeDecision::kMergeBase) {
      ctx_->base_merge_count_.fetch_add(1, std::memory_order_relaxed);
      ctx_->base_merge_rewritten_bytes_.fetch_add(
          p.overlap_bytes, std::memory_order_relaxed);
    }
  }
  FinishPlansLocked();
  return ROCKSDB_NAMESPACE::Status::OK();
}

void ZfMaterializeJob::DeleteOrphanFiles() {
  // 批内被替换输出的物理文件（从未安装，仅本批内可见；持锁 IO 不必要，
  // 由调用方在解锁后调用；不删会泄漏 SST）。
  for (uint64_t fn : orphan_files_) {
    const std::string fname = ROCKSDB_NAMESPACE::TableFileName(
        mc_.cfd->ioptions().cf_paths, fn, 0);
    mc_.db_options->env->DeleteFile(fname).PermitUncheckedError();
  }
}

void ZfMaterializeJob::HandOffSkipped() {
  // M4.5b：kSkip 分区的封存 WAL 移交 recovery 集合（可读、不 unlink）。
  // 必须在 imm 出链（ReleaseEpoch）前完成——出链时若该 epoch 的 gens
  // 未移除，被跳过的 WAL 会被 unlink 且 frozen 索引被释放（数据丢失）。
  // HandOff 后 GetSealedEpoch 不再含这些 gens → 索引保留。
  if (!skipped_gens_.empty()) {
    ctx_->HandOffSkippedToRecovery(epoch_, skipped_gens_);
    ROCKS_LOG_INFO(mc_.db_options->info_log,
                   "[JOB %d] ZeroFlush skipped %zu partition gens (batch)",
                   mc_.job_context->job_id, skipped_gens_.size());
  }
}

void ZfMaterializeJob::DrainOutputs(std::vector<MaterializeOutput>* out) {
  rocksdb::MutexLock l(&out_mu_);
  for (auto& o : outputs_) {
    out->push_back(std::move(o));
  }
  outputs_.clear();
}

ROCKSDB_NAMESPACE::Status ZfMaterializeJob::PlanLocked() {
  mc_.db_mutex->AssertHeld();
  // 分区集合以 se.gens 为准（收养的孤儿代 part 也在其中）。
  part_ids_.clear();
  part_ids_.reserve(se_.gens.size());
  for (const auto& [p, g] : se_.gens) {
    (void)g;
    part_ids_.push_back(p);
  }
  std::sort(part_ids_.begin(), part_ids_.end());
  part_ids_.erase(std::unique(part_ids_.begin(), part_ids_.end()),
                  part_ids_.end());
  plans_.clear();
  plans_.reserve(part_ids_.size());

  // 融合开关（§7.2）：merge_into_base_level 开启、非孤儿代 epoch（§8.1
  // 保守分支，孤儿代 A 侧 seq 与 base 不保证严格递增）、仅范围路由模式
  // （hash 模式分区键集交错，无法判定 base 文件与分区的隶属关系）。
  // M4.5b：kSkip 跳过代收养不置 has_adopted_orphans（seq 连续，可融合），
  // 故此处条件无需变化——只有崩溃恢复孤儿（Recover 登记的 recovery_gens_）
  // 触发保守。
  const bool merge_enabled = ctx_->zfo_.merge_into_base_level &&
                             !se_.has_adopted_orphans && table_ != nullptr &&
                             !table_->IsHashMode();
  ROCKSDB_NAMESPACE::VersionStorageInfo* vstorage =
      (mc_.base != nullptr) ? mc_.base->storage_info() : nullptr;
  const int base = (vstorage != nullptr) ? vstorage->base_level() : 0;
  const ROCKSDB_NAMESPACE::Comparator* ucmp = mc_.cfd->user_comparator();
  const ROCKSDB_NAMESPACE::MutableCFOptions& mcf = *mc_.mutable_cf_options;

  // base 层重叠文件扫描（半开区间 [lo, hi) 相交且完全包含；being_compacted
  // 冲突检测与批内注册识别）。供融合归并（merge_enabled）与孤儿直装替换
  // （M4.5b-2）共用。
  auto scan_overlap = [&](const rocksdb::Slice& lo, const rocksdb::Slice& hi,
                          std::vector<ROCKSDB_NAMESPACE::FileMetaData*>* out,
                          uint64_t* out_bytes, bool* ok_out,
                          bool* batch_skipped_out, bool* has_oob_out = nullptr) {
    out->clear();
    *out_bytes = 0;
    *ok_out = true;
    *batch_skipped_out = false;
    if (has_oob_out != nullptr) {
      *has_oob_out = false;
    }
    for (ROCKSDB_NAMESPACE::FileMetaData* f : vstorage->LevelFiles(base)) {
      const ROCKSDB_NAMESPACE::Slice f_lo = f->smallest.user_key();
      const ROCKSDB_NAMESPACE::Slice f_hi = f->largest.user_key();
      // 相交 ⟺ !(f_hi < lo || f_lo >= hi)。
      if (ucmp->Compare(f_hi, lo) < 0 || ucmp->Compare(f_lo, hi) >= 0) {
        continue;
      }
      // R59：越界文件（分区边界切割）不再拒绝——收进 overlap 作融合 B 侧
      // 整体重写（输出 ⊇ 被替换文件，替换安全）。
      if (ucmp->Compare(f_lo, lo) < 0 || ucmp->Compare(f_hi, hi) >= 0) {
        if (has_oob_out != nullptr) {
          *has_oob_out = true;
        }
      }
      if (f->being_compacted) {
        // 若为本批次前序融合注册所标记 → 跳过（由批内链式替换的
        // last_batch 提供 B 侧覆盖），否则为原生 compaction 冲突 → 降级。
        if (batch_registered_files_.count(f->fd.GetNumber()) != 0) {
          *batch_skipped_out = true;
          continue;
        }
        // 原生 compaction 正在使用该文件 → 冲突降级（§7.3 不等待）。
        *ok_out = false;
        return;
      }
      out->push_back(f);
      *out_bytes += f->fd.GetFileSize();
    }
  };

  for (uint32_t pid : part_ids_) {
    PartitionPlan plan;
    plan.part_id = pid;
    plan.decision = MaterializeDecision::kDirect;
    if (!merge_enabled) {
      // M4.5b：融合关闭（用户未开融合或孤儿收养 epoch）时，攒批仍适用
      // 于"待物化代 < 上限"的分区（数据留在 frozen 索引 + recovery WAL，
      // 下个 epoch 收养后多代合并）——避免孤儿收养 epoch 全量 kDirect
      // 物化 → 回落 L0 → 遮蔽链导致后续同分区连锁回落。孤儿分区本身
      // （待物化代 ≥ 上限）必须落地。用户未开融合（merge_into_base_level
      // =false）或关闭攒批（skip_batching=false）时不攒批（维持原行为）。
      if (ctx_->zfo_.merge_into_base_level && ctx_->zfo_.skip_batching &&
          PendingGenCount(pid) < kMaxSkipGenerations) {
        plan.decision = MaterializeDecision::kSkip;
        ctx_->skip_count_.fetch_add(1, std::memory_order_relaxed);
      } else if (ctx_->zfo_.merge_into_base_level && se_.has_adopted_orphans &&
                 table_ != nullptr && !table_->IsHashMode()) {
        // M4.5b-2：孤儿收养 epoch 的强制分区直装替换——物化输出（含该
        // 分区全部待物化代）替换 base 重叠文件（直装 base，不回落）。
        // 回落会产生 L0 遮蔽链：L0 旧数据（孤儿输出）遮蔽 base 更新的
        // 融合输出 → 读旧值（R22 实测用例 48 phase2 get 读到 'C'）。
        // 替换安全性：输出范围 ⊇ overlap 文件范围（全代数据），替换后
        // base 无重叠且数据最新。
        table_->RangeOf(pid, &plan.lo, &plan.hi);
        std::vector<ROCKSDB_NAMESPACE::FileMetaData*> ov;
        uint64_t ov_bytes = 0;
        bool ok = true, bskipped = false;
        bool oob3 = false;
        scan_overlap(plan.lo, plan.hi, &ov, &ov_bytes, &ok, &bskipped, &oob3);
        // M4.8 遮蔽防护：L0 有本分区范围文件 → 直装替换会被 L0 遮蔽
        // （L0 文件可能比孤儿输出旧）→ 放弃 force_replace，走默认 kDirect
        // → PickInstallLevel 回落 L0（L0 内按 file number 新→旧——读
        // 最新，语义正确）。
        bool l0_shadow = false;
        for (int l = 0; l < base && !l0_shadow; ++l) {
          for (ROCKSDB_NAMESPACE::FileMetaData* f : vstorage->LevelFiles(l)) {
            if (f->being_compacted) {
              continue;
            }
            const ROCKSDB_NAMESPACE::Slice f_lo = f->smallest.user_key();
            const ROCKSDB_NAMESPACE::Slice f_hi = f->largest.user_key();
            if (ucmp->Compare(f_hi, plan.lo) >= 0 &&
                ucmp->Compare(f_lo, plan.hi) < 0) {
              l0_shadow = true;
              break;
            }
          }
        }
        if (ok && !ov.empty() && !bskipped && !l0_shadow) {
          plan.force_replace = true;
          plan.base_level = base;
          plan.overlap = std::move(ov);
          plan.overlap_bytes = ov_bytes;
        }
      }
      plans_.push_back(std::move(plan));
      continue;
    }
    table_->RangeOf(pid, &plan.lo, &plan.hi);

    // 候选重叠文件：base 层与分区半开区间 [lo, hi) 相交且完全包含。
    // 手工遍历而非 GetOverlappingInputs：后者闭区间语义会把右邻居
    // （largest == lo）误收进来；完全包含要求保证输出范围 ⊆ [lo, hi)，
    // 从而替换后 base 层无残留重叠文件。
    std::vector<ROCKSDB_NAMESPACE::FileMetaData*> overlap;
    uint64_t overlap_bytes = 0;
    bool ok = true;
    bool batch_skipped = false;
    bool has_oob = false;
    scan_overlap(plan.lo, plan.hi, &overlap, &overlap_bytes, &ok,
                 &batch_skipped, &has_oob);
    if (!ok) {
      // M4.8 直装优先：base 层不可替换（being_compacted 冲突 / 边界越界）
      // → 不物化，等待（kSkip 攒批：数据留在 frozen 索引 + 封存 WAL，
      // 下个 epoch 收养后重试直装）。gen ≥ kMaxSkipGenerations 强制落地
      // （回落 L0 兜底，保证收敛；内存背压由 max_pending_epochs 流控）。
      if (ctx_->zfo_.skip_batching &&
          PendingGenCount(pid) < kMaxSkipGenerations) {
        plan.decision = MaterializeDecision::kSkip;
        ctx_->skip_count_.fetch_add(1, std::memory_order_relaxed);
      }
      plans_.push_back(std::move(plan));
      continue;
    }

    // M4.9 L0 融合：L0（及更浅层）有本分区范围的文件（非 being_compacted、
    // 完全包含）→ 物化输出合并这些文件（B 侧）后直装 base 并替换 L0
    // （DeleteFile(0) + AddFile(base)）——L0 恒空、无遮蔽、无 compaction
    // 压力（打破"fallback → L0 → 回落"循环；R49 实测 1KB/10GB 写 23K vs
    // 原生 110K，瓶颈即 L0 循环）。约束：L0 文件完全包含于分区范围（范围
    // 路由下 fallback 输出为单分区文件）；批内无本分区未安装的 L0 输出
    // （否则本输出不含其数据，替换会丢——等下一批，前序安装后 vstorage
    // 可见）。
    std::vector<ROCKSDB_NAMESPACE::FileMetaData*> l0_overlap;
    bool l0_busy = false;  // L0 有文件但被占用/越界（不可替换）
    for (int l = 0; l < base && !l0_busy; ++l) {
      for (ROCKSDB_NAMESPACE::FileMetaData* f : vstorage->LevelFiles(l)) {
        const ROCKSDB_NAMESPACE::Slice f_lo = f->smallest.user_key();
        const ROCKSDB_NAMESPACE::Slice f_hi = f->largest.user_key();
        if (ucmp->Compare(f_hi, plan.lo) < 0 ||
            ucmp->Compare(f_lo, plan.hi) >= 0) {
          continue;
        }
        // 完全包含（半开区间）：越界文件（分区边界切割）无法安全替换。
        if (ucmp->Compare(f_lo, plan.lo) < 0 ||
            ucmp->Compare(f_hi, plan.hi) >= 0) {
          l0_busy = true;
          break;
        }
        if (f->being_compacted) {
          l0_busy = true;
          break;
        }
        l0_overlap.push_back(f);
      }
    }
    if (!l0_overlap.empty()) {
      // 批内前序未安装的 L0 输出（level 0）：本输出不含其数据 → 不融合
      // （等待下一批，前序安装后 vstorage 可见 → 可替换）。
      bool batch_l0 = false;
      if (batch_outputs_ != nullptr) {
        for (auto rit = batch_outputs_->rbegin(); rit != batch_outputs_->rend();
             ++rit) {
          if (rit->superseded || rit->level != 0) {
            continue;
          }
          const ROCKSDB_NAMESPACE::Slice x_lo = rit->meta.smallest.user_key();
          const ROCKSDB_NAMESPACE::Slice x_hi = rit->meta.largest.user_key();
          if (ucmp->Compare(x_hi, plan.lo) >= 0 &&
              ucmp->Compare(x_lo, plan.hi) < 0) {
            batch_l0 = true;
            break;
          }
        }
      }
      if (!batch_l0) {
        // L0 融合：B 侧 = L0 文件 + base overlap。写放大防护（R49e 实测
        // 卡死根因）：小 epoch 无条件融合会每物化重写 L1 全量（32× 写放大
        // → 物化吞吐 < 写吞吐 → 写停卡死）。用（本 epoch sealed + 待融合
        // L0 字节）/ overlap（L1）的 ratio 门槛——不达标则回落 L0 累积
        // （L0 文件由后续融合清理），达标才融合（重写 L1 一次）。
        uint64_t l0_bytes = 0;
        for (ROCKSDB_NAMESPACE::FileMetaData* f : l0_overlap) {
          l0_bytes += f->fd.GetFileSize();
        }
        const auto pit = se_.part_bytes.find(pid);
        const uint64_t sealed =
            (pit != se_.part_bytes.end()) ? pit->second : 0;
        const double ratio =
            (overlap_bytes > 0)
                ? static_cast<double>(sealed + l0_bytes) /
                      static_cast<double>(overlap_bytes)
                : 1.0;
        if (ratio < ctx_->zfo_.base_merge_min_ratio) {
          // ratio 不足：回落 L0 累积（compaction 消费——滞留由后续
          // ratio 达标的融合清理；滞留有限且不阻断（越界跳过）。
          plan.decision = MaterializeDecision::kFallback;
          plans_.push_back(std::move(plan));
          continue;
        }
        plan.l0_overlap = std::move(l0_overlap);
      } else {
        // 批内有本分区未安装的 L0 输出（替换会丢其数据）：等待下一批
        // （前序安装后 vstorage 可见 → 可融合）。与 skip_batching 解耦
        // ——L0 场景的等待是必要等待（数据留 frozen + WAL 可读），否则
        // fallback→L0 循环（R49 实测）；skip 字节超阈值才强制回落兜底。
        const uint64_t shadow_limit =
            static_cast<uint64_t>(ctx_->zfo_.partition_target_bytes) * 32;
        if (ctx_->skipped_bytes() < shadow_limit) {
          plan.decision = MaterializeDecision::kSkip;
          ctx_->skip_count_.fetch_add(1, std::memory_order_relaxed);
        } else {
          plan.decision = MaterializeDecision::kFallback;
        }
        plans_.push_back(std::move(plan));
        continue;
      }
    } else if (l0_busy) {
      // L0 有文件但被 compaction 占用（消费中——替换不安全）/越界：等待
      // （compaction 消费后 L0 可替换 → 融合恢复）。同上与 skip_batching
      // 解耦；skip 字节超阈值才强制回落兜底（L0 增长触发 compaction
      // 消费 → 恢复融合）。
      const uint64_t shadow_limit =
          static_cast<uint64_t>(ctx_->zfo_.partition_target_bytes) * 32;
      if (ctx_->skipped_bytes() < shadow_limit) {
        plan.decision = MaterializeDecision::kSkip;
        ctx_->skip_count_.fetch_add(1, std::memory_order_relaxed);
      } else {
        plan.decision = MaterializeDecision::kFallback;
      }
      plans_.push_back(std::move(plan));
      continue;
    }

    // 批内前序输出（未安装，vstorage 不可见）：取最后一个与分区范围
    // 重叠的 base 层输出（在 batch_skipped/overlap.empty() 判定前查找，
    // 供后续判定引用）。融合输出 ⊇ 其 B 侧（= existing + 更早批内输出）
    // → 最后一个融合输出已覆盖全部前序 + existing；直装项与 existing
    // 不重叠，二者并存且按 smallest 有序。该输出由本输出在阶段 2 标记
    // superseded（批内链式替换，§7.4）。
    const MaterializeOutput* last_batch = nullptr;
    if (batch_outputs_ != nullptr) {
      for (auto rit = batch_outputs_->rbegin(); rit != batch_outputs_->rend();
           ++rit) {
        if (rit->superseded || rit->level != base) {
          continue;
        }
        const ROCKSDB_NAMESPACE::Slice x_lo = rit->meta.smallest.user_key();
        const ROCKSDB_NAMESPACE::Slice x_hi = rit->meta.largest.user_key();
        // 与分区半开区间 [lo, hi) 相交且完全包含（同 existing 判定）。
        if (ucmp->Compare(x_hi, plan.lo) >= 0 &&
            ucmp->Compare(x_lo, plan.hi) < 0 &&
            ucmp->Compare(x_lo, plan.lo) >= 0 &&
            ucmp->Compare(x_hi, plan.hi) < 0) {
          last_batch = &*rit;
          break;
        }
      }
    }

    // overlap 为空且无批内跳过 → existing 无相交文件 → 走直装路径。
    if (overlap.empty() && !batch_skipped && plan.l0_overlap.empty()) {
      // M4.10b：直装 L1 与运行中 compaction（L0→L1）的输出范围互斥——
      // 并发安装会使 L1 出现重叠文件（破坏非 L0 层无重叠不变量）。
      // 命中即降级：攒批开启时 kSkip 等待（compaction 完成后重试直装，
      // 数据留 frozen + WAL 可读）；关闭时保持 kDirect（PickInstallLevel
      // 因 L1 重叠回落 L0，由原生 L0 compaction 消费，语义正确）。
      if (mc_.compaction_picker != nullptr &&
          mc_.compaction_picker->RangeOverlapWithCompaction(plan.lo, plan.hi,
                                                            base)) {
        if (ctx_->zfo_.skip_batching &&
            PendingGenCount(pid) < kMaxSkipGenerations) {
          plan.decision = MaterializeDecision::kSkip;
          ctx_->skip_count_.fetch_add(1, std::memory_order_relaxed);
        }
      }
      plans_.push_back(std::move(plan));
      continue;
    }

    // 被批内前序注册标记的文件必须由批内融合输出覆盖（last_batch 为
    // kMergeBase），否则 B 侧缺数据 → 安全降级 kSkip（攒批，见下）。
    if (batch_skipped &&
        (last_batch == nullptr ||
         last_batch->decision != MaterializeDecision::kMergeBase)) {
      if (ctx_->zfo_.skip_batching &&
          PendingGenCount(pid) < kMaxSkipGenerations) {
        plan.decision = MaterializeDecision::kSkip;
        ctx_->skip_count_.fetch_add(1, std::memory_order_relaxed);
      }
      plans_.push_back(std::move(plan));
      continue;
    }

    // 触发比（§7.2）：sealed_bytes / overlap_bytes >= base_merge_min_ratio。
    // overlap 为空时（batch_skipped 场景）跳过比率检查——融合成本已被
    // 前序承担，B 侧全量由 last_batch（融合输出）提供。
    // M4.9：L0 融合（plan.l0_overlap 非空）跳过 ratio——L0 文件存在即
    // 合并（否则回落 L0 循环），重写代价 ≤ compaction 消费的代价。
    if (!overlap.empty() && plan.l0_overlap.empty()) {
      const auto it = se_.part_bytes.find(pid);
      const uint64_t sealed =
          (it != se_.part_bytes.end()) ? it->second : 0;
      const double ratio =
          static_cast<double>(sealed) / static_cast<double>(overlap_bytes);
      if (ratio < ctx_->zfo_.base_merge_min_ratio) {
        // M4.5b：比例不足（批次小不融合）→ kSkip 攒批：不产出 L0 回落
        // 文件，数据留在 frozen 索引 + 封存 WAL（skip 集合可读），
        // 下个 epoch 收养后多代合并一次物化。
        // 约束：该分区待物化代 < 上限（防永不收敛——攒一代即强制落地）。
        if (ctx_->zfo_.skip_batching &&
            PendingGenCount(pid) < kMaxSkipGenerations) {
          plan.decision = MaterializeDecision::kSkip;
          ctx_->skip_count_.fetch_add(1, std::memory_order_relaxed);
          plans_.push_back(std::move(plan));
          continue;
        }
        if (!ctx_->zfo_.skip_batching) {
          // 攒批关闭：维持原行为（kFallback 回落 L0，R20 基线）。
          plans_.push_back(std::move(plan));
          continue;
        }
        // M4.5b-2：攒一代后（待物化代 ≥ 上限）强制融合——无视 ratio，
        // 物化多代数据直装 base 层。回落会触发 L0 遮蔽链（后续同分区
        // 直装被 L0 重叠拒绝 → 连锁回落 → L0 堆积 → 停写；R21 实测
        // 50GB 17 分钟退化至 18K/51% stall）。强制融合的写放大 =
        // 重写 overlap 一次（~2MB vs 数据 ~300KB），换取零 L0 产出。
        // fallthrough：继续执行下方融合注册路径（kMergeBase）。
      }
    }

    // M4.5：删除 upper_conflict 检查——L0（或更浅层）与本分区范围重叠
    // 不再拒绝融合/直装。原逻辑（§7.2 保守分支）在 L0 堆积时导致"回落
    // L0 → L0 更多 → 更拒"的恶性循环（R8/R15 实测：50GB 下 L0 卡 75、
    // 停写 52%）。正确性论证：读路径自浅至深（L0 优先），L0 中的文件
    // 是更晚 epoch 的数据（seq 更新）→ 遮蔽 base 层的融合输出（本 epoch
    // 更旧）→ 语义正确；L0 文件由原生 compaction 消费，无新回落 →
    // 循环打破，稳态 L0 空 → 直装恢复。
    // 注意：批内多 epoch 的 ABA 防护由"epoch 按序物化 + 安装原子性"
    // 保证（单次 VersionEdit），L0 重叠不再作为拒绝理由。

    // 与运行中 compaction 的输出范围互斥（§7.3）：注册前检查，命中即
    // 降级不等待（防死锁）。分区范围 ⊇ 重叠文件范围，故该检查严格于
    // RegisterCompaction 内部 assert 的 FilesRangeOverlapWithCompaction。
    if (mc_.compaction_picker == nullptr ||
        mc_.compaction_picker->RangeOverlapWithCompaction(plan.lo, plan.hi,
                                                          base)) {
      plans_.push_back(std::move(plan));
      continue;
    }

    plan.overlap_all = overlap;  // existing 优先（无批内项时即 B 侧全集）
    if (last_batch != nullptr && last_batch->decision == MaterializeDecision::kMergeBase) {
      // 融合输出 ⊇ existing → 仅用它（避免与 existing 重叠破坏非 L0
      // 层 inputs 的有序/无重叠假设）。
      plan.batch_meta_copies.push_back(last_batch->meta);
      plan.overlap_all.clear();
      plan.overlap_all.push_back(&plan.batch_meta_copies.back());
    } else if (last_batch != nullptr) {
      // 直装输出：与 existing 不重叠 → 追加（保持有序）。
      plan.batch_meta_copies.push_back(last_batch->meta);
      plan.overlap_all.push_back(&plan.batch_meta_copies.back());
    }

    // 构造 Compaction：inputs[0] = L0 融合文件（M4.9；否则空）、
    // inputs[1] = overlap_all、output = base、kFlush。构造即
    // MarkFilesBeingCompacted(true)。
    std::vector<ROCKSDB_NAMESPACE::CompactionInputFiles> inputs(2);
    inputs[0].level = 0;
    inputs[0].files = plan.l0_overlap;
    inputs[1].level = base;
    inputs[1].files = plan.overlap_all;
    auto compaction = std::make_unique<ROCKSDB_NAMESPACE::Compaction>(
        vstorage, mc_.cfd->ioptions(), mcf, ROCKSDB_NAMESPACE::MutableDBOptions(),
        std::move(inputs), base, mcf.target_file_size_base,
        std::numeric_limits<uint64_t>::max() /* max_compaction_bytes */,
        0 /* output_path_id */, mc_.output_compression, mcf.compression_opts,
        ROCKSDB_NAMESPACE::Temperature::kUnknown,
        0 /* max_subcompactions */, std::vector<ROCKSDB_NAMESPACE::FileMetaData*>()
            /* grandparents */,
        std::nullopt /* earliest_snapshot */, nullptr /* snapshot_checker */,
        ROCKSDB_NAMESPACE::CompactionReason::kFlush, "" /* trim_ts */,
        -1 /* score */, !plan.l0_overlap.empty() /* l0_files_might_overlap */);
    // Proximal level 有效（preclude_last_level_data_seconds 场景）会触发
    // RegisterCompaction 的 debug assert → 放弃融合（保守降级）。
    if (compaction->GetProximalLevel() != ROCKSDB_NAMESPACE::Compaction::kInvalidLevel) {
      plans_.push_back(std::move(plan));
      continue;
    }
    mc_.compaction_picker->RegisterCompaction(compaction.get());
    // 记录被本注册标记 being_compacted 的 existing 文件号，供同批次
    // 后序 epoch 的 PlanLocked 识别并跳过（§7.4 批内链式替换互斥）。
    for (ROCKSDB_NAMESPACE::FileMetaData* r : overlap) {
      batch_registered_files_.insert(r->fd.GetNumber());
    }
    plan.decision = MaterializeDecision::kMergeBase;
    plan.overlap = std::move(overlap);
    plan.overlap_bytes = overlap_bytes;
    plan.compaction = std::move(compaction);
    plans_.push_back(std::move(plan));
  }
  // M4.5b：收集 kSkip 分区的全部待物化代（含收养孤儿代），供 Run 尾部
  // 移交 recovery 集合（同一持锁窗口内完成，与收养语义一致）。
  for (const auto& p : plans_) {
    if (p.decision != MaterializeDecision::kSkip) {
      continue;
    }
    for (const auto& [part, gen] : se_.gens) {
      if (part == p.part_id) {
        skipped_gens_.emplace_back(part, gen);
      }
    }
  }
  return ROCKSDB_NAMESPACE::Status::OK();
}

// M4.5b：该分区在本 epoch 的待物化代数（含收养的恢复期孤儿代）。
// kSkip 只允许攒一代（≥上限强制物化），保证数据最终收敛到 SST。
uint32_t ZfMaterializeJob::PendingGenCount(uint32_t pid) const {
  uint32_t n = 0;
  for (const auto& [part, gen] : se_.gens) {
    if (part == pid) {
      ++n;
    }
  }
  return n;
}

void ZfMaterializeJob::FinishPlansLocked() {
  mc_.db_mutex->AssertHeld();
  for (PartitionPlan& p : plans_) {
    if (p.decision != MaterializeDecision::kMergeBase) {
      continue;
    }
    // 释放注册 + 解除 being_compacted。不调 Compaction::ReleaseCompactionFiles：
    // 其走 cfd_->compaction_picker()（cfd_ 为 nullptr）且失败路径
    // ResetNextCompactionIndex 断言 input_version_ 非空（我们未调
    // FinalizeInputInfo）。二者均为 public，等价手动完成。
    mc_.compaction_picker->UnregisterCompaction(p.compaction.get());
    p.compaction->MarkFilesBeingCompacted(false);
    p.compaction.reset();
    p.decision = MaterializeDecision::kDirect;  // 防重复释放
  }
}

const ZfMaterializeJob::PartitionPlan* ZfMaterializeJob::FindPlan(
    uint32_t part_id) const {
  // plans_ 与 part_ids_ 同序（按 part_id 升序）。
  auto it = std::lower_bound(
      plans_.begin(), plans_.end(), part_id,
      [](const PartitionPlan& p, uint32_t id) { return p.part_id < id; });
  return (it != plans_.end() && it->part_id == part_id) ? &*it : nullptr;
}

ROCKSDB_NAMESPACE::Status ZfMaterializeJob::MaterializePartition(
    uint32_t part_id, const std::vector<std::pair<uint32_t, uint32_t>>& gens,
    const ROCKSDB_NAMESPACE::Slice& lo, const ROCKSDB_NAMESPACE::Slice& hi) {
  assert(!gens.empty());

  // 顺序整读该分区全部代（按 gen 升序 = 写入序）。
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (const auto& [p, gen] : gens) {
    WalScanner scanner(mc_.db_options->env, ctx_->wal_dir(), p, gen,
                       mc_.db_options->info_log.get());
    ZfRecordHeader h;
    rocksdb::Slice key, value;
    while (scanner.Next(&h, &key, &value)) {
      keys.push_back(MakeInternalKey(key, h.seq, h.type));
      values.emplace_back(value.data(), value.size());
    }
    // M3.2：物化严格要求已封存文件完整（区别于恢复路径的宽容语义）。
    if (!scanner.status().ok()) {
      return ROCKSDB_NAMESPACE::Status::Corruption(
          "ZF sealed WAL scan failed: " + scanner.status().ToString());
    }
  }
  if (keys.empty()) {
    return ROCKSDB_NAMESPACE::Status::OK();  // 空分区不产出 SST
  }

  // 排序（M4.6e：Bytewise 时编码排序键 + memcmp 索引排序——InternalKey
  // Comparator 的逐次解析是物化排序大头；非 Bytewise 走原 VectorIterator
  // 排序）。
  const uint64_t sort_start = mc_.db_options->clock->NowMicros();
  std::string sort_buf;
  std::vector<size_t> sort_off;
  if (BuildSortKeys(keys, mc_.cfd->user_comparator(), &sort_buf, &sort_off)) {
    ReorderBySortKeys(&keys, &values, sort_buf, sort_off);
  }
  sort_micros_.fetch_add(mc_.db_options->clock->NowMicros() - sort_start,
                         std::memory_order_relaxed);

  // M4.11：hash 遗留切片判定——必须在构造 VectorIterator（move keys）之前。
  // hash 遗留 = 分区数据 key 范围超出分区边界（epoch 1 hash 表写入的全
  // 范围交错数据；epoch 2+ 未封存分区的遗留数据被后续 epoch 收养物化）。
  // 单文件输出为全范围 → 直装 L1 与未替换文件重叠（VersionBuilder 一致性
  // 检查崩溃，R54 实测 5GB）或 L0→L1 compaction 合并出全范围 L1 → 所有
  // 后续物化 scan_overlap 越界 → 回落 L0 → 写放大无界（R50 链）。按切片
  // 表 RangeOf 切 P 片 → 输出分区范围文件 → L1 保持分区文件 → 直装/融合
  // 恢复。切片表：epoch 2+ 用 se 表（与物化分区一致）；epoch 1 用当前表
  // （封存时已安装学习表 v1）。
  const ROCKSDB_NAMESPACE::Comparator* ucmp_s = mc_.cfd->user_comparator();
  const rocksdb::Slice u_smallest = ROCKSDB_NAMESPACE::ExtractUserKey(keys.front());
  const rocksdb::Slice u_largest = ROCKSDB_NAMESPACE::ExtractUserKey(keys.back());
  const bool oob_data =
      (!lo.empty() && ucmp_s->Compare(u_smallest, lo) < 0) ||
      (!hi.empty() && ucmp_s->Compare(u_largest, hi) >= 0);
  const bool legacy_hash =
      table_ != nullptr && table_->IsHashMode() && lo.empty() && hi.empty();
  std::shared_ptr<zeroflush::PartitionTable> slice_table;
  if (table_ != nullptr && !table_->IsHashMode()) {
    slice_table = table_;
  } else {
    slice_table = ctx_->tables()->current();
  }
  const bool do_slice =
      (oob_data || legacy_hash) && slice_table != nullptr &&
      !slice_table->IsHashMode() && slice_table->partitions() > 1;

  // 范围断言（仅范围路由模式；hash 模式各分区输出范围可能交错，跳过）。
  // M4.10：学习过的表（se.table_version > 0）的分区可能含 hash 学习期
  // 遗留数据——越界数据回落 L0 承载（读走 L0 全范围查找，正确）→ 跳过
  // Corruption 并计数监控。版本 0（纯 hash 表，无遗留）保持断言。
  if (!do_slice && table_ != nullptr && !table_->IsHashMode()) {
    // [smallest, largest] ⊆ [lo, hi)，lo/hi 空 = -∞/+∞。
    if (oob_data) {
      if (se_.table_version > 0) {
        ctx_->materialize_oob_count_.fetch_add(1, std::memory_order_relaxed);
      } else {
        return ROCKSDB_NAMESPACE::Status::Corruption(
            "ZF partition " + std::to_string(part_id) +
            " materialized range outside table bounds");
      }
    }
  }

  // BuildTable（模板对齐 FlushJob::WriteLevel0Table；level=0 构建语义）。
  const ROCKSDB_NAMESPACE::MutableCFOptions& mcf = *mc_.mutable_cf_options;
  const std::string* const full_history_ts_low =
      (mc_.full_history_ts_low.empty()) ? nullptr : &mc_.full_history_ts_low;
  ROCKSDB_NAMESPACE::ReadOptions read_options(
      ROCKSDB_NAMESPACE::Env::IOActivity::kFlush);
  read_options.rate_limiter_priority = mc_.io_priority;
  const ROCKSDB_NAMESPACE::WriteOptions write_options(
      mc_.io_priority, ROCKSDB_NAMESPACE::Env::IOActivity::kFlush);
  int64_t _current_time = 0;
  ROCKSDB_NAMESPACE::Status ts =
      mc_.db_options->clock->GetCurrentTime(&_current_time);
  if (!ts.ok()) {
    _current_time = 0;
  }
  const uint64_t current_time = static_cast<uint64_t>(_current_time);
  const uint64_t oldest_key_time = current_time;

  // BuildTable 单文件 lambda（参数化迭代器；切片循环与整表共用）。
  auto build_one = [&](std::unique_ptr<ROCKSDB_NAMESPACE::InternalIteratorBase<
                           ROCKSDB_NAMESPACE::Slice>> vit)
      -> ROCKSDB_NAMESPACE::Status {
    ROCKSDB_NAMESPACE::FileMetaData meta;
    meta.fd = ROCKSDB_NAMESPACE::FileDescriptor(
        mc_.versions->NewFileNumber(), 0, 0);
    ROCKSDB_NAMESPACE::TableBuilderOptions tboptions(
        mc_.cfd->ioptions(), mcf, read_options, write_options,
        mc_.cfd->internal_comparator(),
        mc_.cfd->internal_tbl_prop_coll_factories(), mc_.output_compression,
        mcf.compression_opts, mc_.cfd->GetID(), mc_.cfd->GetName(),
        0 /* level */, current_time, false /* is_bottommost */,
        ROCKSDB_NAMESPACE::TableFileCreationReason::kFlush, oldest_key_time,
        current_time, mc_.db_id, mc_.db_session_id, 0 /* target_file_size */,
        meta.fd.GetNumber(),
        ROCKSDB_NAMESPACE::kMaxSequenceNumber /* preclude_last_level_min_seqno */);
    ROCKSDB_NAMESPACE::IOStatus io_s;
    std::vector<ROCKSDB_NAMESPACE::BlobFileAddition> blob_file_additions;
    uint64_t memtable_payload_bytes = 0;
    uint64_t memtable_garbage_bytes = 0;
    ROCKSDB_NAMESPACE::TableProperties table_properties;
    ROCKSDB_NAMESPACE::Status bs = ROCKSDB_NAMESPACE::BuildTable(
        mc_.dbname, mc_.versions, *mc_.db_options, tboptions, *mc_.file_options,
        mc_.cfd->table_cache(), vit.get(),
        std::vector<std::unique_ptr<
            ROCKSDB_NAMESPACE::FragmentedRangeTombstoneIterator>>(),
        &meta, &blob_file_additions, mc_.job_context->snapshot_seqs,
        mc_.earliest_snapshot,
        mc_.job_context->earliest_write_conflict_snapshot,
        mc_.job_context->GetJobSnapshotSequence(),
        mc_.job_context->snapshot_checker, mcf.paranoid_file_checks,
        mc_.cfd->internal_stats(), &io_s, mc_.io_tracer,
        ROCKSDB_NAMESPACE::BlobFileCreationReason::kFlush,
        mc_.seqno_to_time_mapping.get(), mc_.event_logger, mc_.job_id,
        &table_properties, ROCKSDB_NAMESPACE::Env::WLTH_NOT_SET,
        full_history_ts_low, mc_.blob_callback,
        nullptr /* version */, &memtable_payload_bytes, &memtable_garbage_bytes,
        nullptr /* flush_stats */, nullptr /* blob_file_garbages */,
        mc_.fast_sst_open);
    io_s.PermitUncheckedError();
    if (!bs.ok()) {
      return bs;
    }
    meta.epoch_number = mc_.cfd->NewEpochNumber();
    if (meta.fd.GetFileSize() == 0) {
      return ROCKSDB_NAMESPACE::Status::OK();  // 空表：BuildTable 已删除文件
    }
    {
      rocksdb::MutexLock l(&out_mu_);
      MaterializeOutput out;
      out.meta = std::move(meta);
      outputs_.push_back(std::move(out));
    }
    return ROCKSDB_NAMESPACE::Status::OK();
  };

  if (do_slice) {
    // 切片：按当前表分区范围二分排序后的 keys，逐片构建（lo/hi 为全范围
    // ——hash 遗留分区；切片边界 = 各分区 RangeOf 的 hi，升序）。
    const ROCKSDB_NAMESPACE::Comparator* ucmp = mc_.cfd->user_comparator();
    size_t begin = 0;
    for (uint32_t i = 0; i < slice_table->partitions(); ++i) {
      rocksdb::Slice p_lo, p_hi;
      slice_table->RangeOf(i, &p_lo, &p_hi);
      size_t end = begin;
      while (end < keys.size()) {
        const rocksdb::Slice uk = ROCKSDB_NAMESPACE::ExtractUserKey(keys[end]);
        if (ucmp->Compare(uk, p_hi) >= 0) {
          break;
        }
        ++end;
      }
      if (end > begin) {
        std::vector<std::string> k2(keys.begin() + begin, keys.begin() + end);
        std::vector<std::string> v2(values.begin() + begin,
                                    values.begin() + end);
        std::unique_ptr<ROCKSDB_NAMESPACE::VectorIterator> vit(
            new ROCKSDB_NAMESPACE::VectorIterator(
                std::move(k2), std::move(v2),
                sort_buf.empty() ? &mc_.cfd->internal_comparator() : nullptr));
        ROCKSDB_NAMESPACE::Status ss = build_one(std::move(vit));
        if (!ss.ok()) {
          return ss;
        }
        begin = end;
      }
    }
    // 尾部兜底（分区边界覆盖不全时剩余归入最后一片）。
    if (begin < keys.size()) {
      std::vector<std::string> k2(keys.begin() + begin, keys.end());
      std::vector<std::string> v2(values.begin() + begin, values.end());
      std::unique_ptr<ROCKSDB_NAMESPACE::VectorIterator> vit(
          new ROCKSDB_NAMESPACE::VectorIterator(
              std::move(k2), std::move(v2),
              sort_buf.empty() ? &mc_.cfd->internal_comparator() : nullptr));
      ROCKSDB_NAMESPACE::Status ss = build_one(std::move(vit));
      if (!ss.ok()) {
        return ss;
      }
    }
    return ROCKSDB_NAMESPACE::Status::OK();
  }

  // 非切片：整表输出（原逻辑，iter 持有已排序向量）。
  std::unique_ptr<ROCKSDB_NAMESPACE::VectorIterator> iter(
      new ROCKSDB_NAMESPACE::VectorIterator(
          std::move(keys), std::move(values),
          sort_buf.empty() ? &mc_.cfd->internal_comparator() : nullptr));
  iter->SeekToFirst();
  assert(iter->Valid());
  return build_one(std::move(iter));
}

ROCKSDB_NAMESPACE::Status ZfMaterializeJob::MaterializeMergePartition(
    uint32_t part_id, const std::vector<std::pair<uint32_t, uint32_t>>& gens,
    const std::vector<ROCKSDB_NAMESPACE::FileMetaData*>& overlap_all,
    const std::vector<ROCKSDB_NAMESPACE::FileMetaData*>& l0_overlap,
    const ROCKSDB_NAMESPACE::Compaction* compaction,
    const ROCKSDB_NAMESPACE::Slice& lo, const ROCKSDB_NAMESPACE::Slice& hi) {
  assert(!gens.empty());
  assert(!overlap_all.empty() || !l0_overlap.empty());
  assert(compaction != nullptr);
  const ROCKSDB_NAMESPACE::MutableCFOptions& mcf = *mc_.mutable_cf_options;

  // ---- A 侧：顺序整读 + 排序（同 MaterializePartition；记录最小 seq）----
  std::vector<std::string> keys;
  std::vector<std::string> values;
  uint64_t min_seq = ROCKSDB_NAMESPACE::kMaxSequenceNumber;
  for (const auto& [p, gen] : gens) {
    WalScanner scanner(mc_.db_options->env, ctx_->wal_dir(), p, gen,
                       mc_.db_options->info_log.get());
    ZfRecordHeader h;
    rocksdb::Slice key, value;
    while (scanner.Next(&h, &key, &value)) {
      keys.push_back(MakeInternalKey(key, h.seq, h.type));
      values.emplace_back(value.data(), value.size());
      if (h.seq < min_seq) {
        min_seq = h.seq;
      }
    }
    if (!scanner.status().ok()) {
      return ROCKSDB_NAMESPACE::Status::Corruption(
          "ZF sealed WAL scan failed: " + scanner.status().ToString());
    }
  }
  if (keys.empty()) {
    return ROCKSDB_NAMESPACE::Status::OK();  // 空分区不产出 SST
  }

  // M4.6e：同 MaterializePartition——Bytewise 时编码排序键 + memcmp 重排。
  const uint64_t sort_start = mc_.db_options->clock->NowMicros();
  std::string sort_buf;
  std::vector<size_t> sort_off;
  if (BuildSortKeys(keys, mc_.cfd->user_comparator(), &sort_buf, &sort_off)) {
    ReorderBySortKeys(&keys, &values, sort_buf, sort_off);
  }
  std::unique_ptr<ROCKSDB_NAMESPACE::VectorIterator> a_iter(
      new ROCKSDB_NAMESPACE::VectorIterator(
          std::move(keys), std::move(values),
          sort_buf.empty() ? &mc_.cfd->internal_comparator() : nullptr));
  sort_micros_.fetch_add(mc_.db_options->clock->NowMicros() - sort_start,
                         std::memory_order_relaxed);
  a_iter->SeekToFirst();
  assert(a_iter->Valid());

  // 范围断言（同 MaterializePartition）。M4.10：学习表（版本 >0）的分区
  // 可能含 hash 学习期遗留（越界——回落 L0 承载，读正确）→ 跳过并计数。
  if (table_ != nullptr && !table_->IsHashMode()) {
    a_iter->SeekToFirst();
    const rocksdb::Slice u_smallest =
        ROCKSDB_NAMESPACE::ExtractUserKey(a_iter->key());
    a_iter->SeekToLast();
    const rocksdb::Slice u_largest =
        ROCKSDB_NAMESPACE::ExtractUserKey(a_iter->key());
    a_iter->SeekToFirst();
    const ROCKSDB_NAMESPACE::Comparator* ucmp = mc_.cfd->user_comparator();
    if ((!lo.empty() && ucmp->Compare(u_smallest, lo) < 0) ||
        (!hi.empty() && ucmp->Compare(u_largest, hi) >= 0)) {
      if (se_.table_version > 0) {
        ctx_->materialize_oob_count_.fetch_add(1, std::memory_order_relaxed);
      } else {
        return ROCKSDB_NAMESPACE::Status::Corruption(
            "ZF partition " + std::to_string(part_id) +
            " materialized range outside table bounds");
      }
    }
  }

  // seq 前置断言（§7.4）：A 侧全部记录必须比 B 侧任何记录新。
  //  - 崩溃孤儿代 epoch（has_adopted_orphans）已在 PlanLocked 降级
  //    kSkip/force_replace，此处做运行时防御（孤儿 seq 与 base 交错
  //    会破坏快照语义）。
  //  - M4.8 放宽：kSkip 攒批收养（has_adopted_skips，seq 连续未崩溃）
  //    的 A 侧可含 base 前序更旧的代——任务池阶段 0 统一决策使"批内
  //    链式融合"退化为攒批，收养后的多代合并与 base 前序交错（全局
  //    min ≤ max），但 per-key 版本序仍正确（覆盖写 seq 递增），归并
  //    由 CompactionIterator 按 per-key seq + snapshot 裁决 → 放行。
  //  - M4.11 放宽：hash 遗留切片——epoch 1 分区数据（seq 从全局起点）
  //    按当前表切 P 片进 L1，后续 epoch 物化（A 侧含未封存分区 epoch 1
  //    遗留，min_seq 小）与 L1 片文件（不同 key 集合）seq 交错，全局
  //    min ≤ max 但 per-key 无重叠 → 放行（CompactionIterator 裁决）。
  //  - 孤儿 epoch（has_adopted_orphans）保留检查：崩溃恢复 seq 与 base
  //    交错会破坏快照语义（PlanLocked 已降级，此处运行时防御）。
  uint64_t max_b_seq = 0;
  for (const ROCKSDB_NAMESPACE::FileMetaData* f : overlap_all) {
    max_b_seq = std::max(max_b_seq, f->fd.largest_seqno);
  }
  for (const ROCKSDB_NAMESPACE::FileMetaData* f : l0_overlap) {
    max_b_seq = std::max(max_b_seq, f->fd.largest_seqno);
  }
  if (se_.has_adopted_orphans && min_seq <= max_b_seq) {
    return ROCKSDB_NAMESPACE::Status::Corruption(
        "ZF merge partition " + std::to_string(part_id) +
        " A-side seq not newer than B-side (min=" + std::to_string(min_seq) +
        ", max=" + std::to_string(max_b_seq) + ")");
  }

  // ---- B 侧：overlap_all（existing + 批内前序输出）TableIterator 串接 ----
  ROCKSDB_NAMESPACE::ReadOptions read_options(
      ROCKSDB_NAMESPACE::Env::IOActivity::kCompaction);
  read_options.rate_limiter_priority = mc_.io_priority;
  std::vector<ROCKSDB_NAMESPACE::InternalIterator*> children;
  children.reserve(overlap_all.size() + 1);
  // A 侧 VectorIterator 所有权移交给 MergingIterator。
  children.push_back(a_iter.release());
  ROCKSDB_NAMESPACE::Status s;
  for (const ROCKSDB_NAMESPACE::FileMetaData* f : l0_overlap) {
    ROCKSDB_NAMESPACE::InternalIterator* it =
        mc_.cfd->table_cache()->NewIterator(
            read_options, *mc_.file_options, mc_.cfd->internal_comparator(),
            *f, nullptr /* range_del_agg */, mcf, nullptr /* table_reader_ptr */,
            nullptr /* file_read_hist */, ROCKSDB_NAMESPACE::TableReaderCaller::kCompaction,
            nullptr /* arena */, false /* skip_filters */, 0 /* level */,
            ROCKSDB_NAMESPACE::MaxFileSizeForL0MetaPin(mcf),
            nullptr /* smallest_compaction_key */,
            nullptr /* largest_compaction_key */,
            false /* allow_unprepared_value */, nullptr /* range_del_read_seqno */,
            nullptr /* range_del_iter */, false /* maybe_pin_table_handle */,
            nullptr /* file_open_metadata */);
    if (!it->status().ok()) {
      s = it->status();
      delete it;
      break;
    }
    children.push_back(it);
  }
  if (!s.ok()) {
    for (ROCKSDB_NAMESPACE::InternalIterator* it : children) {
      delete it;
    }
    return s;
  }
  for (const ROCKSDB_NAMESPACE::FileMetaData* f : overlap_all) {
    ROCKSDB_NAMESPACE::InternalIterator* it =
        mc_.cfd->table_cache()->NewIterator(
            read_options, *mc_.file_options, mc_.cfd->internal_comparator(),
            *f, nullptr /* range_del_agg */, mcf, nullptr /* table_reader_ptr */,
            nullptr /* file_read_hist */, ROCKSDB_NAMESPACE::TableReaderCaller::kCompaction,
            nullptr /* arena */, false /* skip_filters */,
            compaction->output_level(),
            ROCKSDB_NAMESPACE::MaxFileSizeForL0MetaPin(mcf),
            nullptr /* smallest_compaction_key */,
            nullptr /* largest_compaction_key */,
            false /* allow_unprepared_value */, nullptr /* range_del_read_seqno */,
            nullptr /* range_del_iter */, false /* maybe_pin_table_handle */,
            nullptr /* file_open_metadata */);
    if (!it->status().ok()) {
      s = it->status();
      delete it;
      break;
    }
    children.push_back(it);
  }
  if (!s.ok()) {
    for (ROCKSDB_NAMESPACE::InternalIterator* it : children) {
      delete it;
    }
    return s;
  }

  // MergingIterator 接管 children 所有权（NewMergingIterator 的文档语义）。
  std::unique_ptr<ROCKSDB_NAMESPACE::InternalIterator> merge_iter(
      ROCKSDB_NAMESPACE::NewMergingIterator(
          &mc_.cfd->internal_comparator(), children.data(),
          static_cast<int>(children.size())));

  // ---- CompactionIterator（复用原生 snapshot/merge/tombstone 语义）----
  const std::string* const full_history_ts_low =
      (mc_.full_history_ts_low.empty()) ? nullptr : &mc_.full_history_ts_low;
  ROCKSDB_NAMESPACE::MergeHelper merge(
      mc_.db_options->env, mc_.cfd->user_comparator(),
      mc_.cfd->ioptions().merge_operator.get(),
      nullptr /* compaction_filter */, mc_.db_options->info_log.get(),
      true /* assert_valid_internal_key */,
      mc_.job_context->snapshot_seqs.empty()
          ? 0
          : mc_.job_context->snapshot_seqs.back(),
      mc_.job_context->snapshot_checker);
  // 对齐 builder.cc BuildTable：构造 CompactionIterator 前必须先对输入
  // 迭代器 SeekToFirst（CompactionIterator::SeekToFirst 不负责底层 seek）。
  merge_iter->SeekToFirst();
  std::atomic<bool> manual_canceled{false};
  ROCKSDB_NAMESPACE::CompactionRangeDelAggregator range_del_agg(
      &mc_.cfd->internal_comparator(), mc_.job_context->snapshot_seqs,
      full_history_ts_low, nullptr /* trim_ts */);
  ROCKSDB_NAMESPACE::CompactionIterator c_iter(
      merge_iter.get(), mc_.cfd->user_comparator(), &merge,
      ROCKSDB_NAMESPACE::kMaxSequenceNumber /* last_sequence（未使用） */,
      &mc_.job_context->snapshot_seqs, mc_.earliest_snapshot,
      mc_.job_context->earliest_write_conflict_snapshot,
      mc_.job_context->GetJobSnapshotSequence(),
      mc_.job_context->snapshot_checker, mc_.db_options->env,
      false /* report_detailed_time */, &range_del_agg,
      nullptr /* blob_file_builder */, mc_.db_options->allow_data_in_errors,
      mc_.db_options->enforce_single_del_contracts, manual_canceled,
      false /* must_count_input_entries */, nullptr /* compaction（无
              input_version_ 的 Compaction，传空走默认代理） */,
      nullptr /* compaction_filter */, nullptr /* shutting_down */,
      mc_.db_options->info_log, full_history_ts_low, std::nullopt,
      nullptr /* input_version */, read_options.io_activity);

  // ---- 输出循环：按 target_file_size 切分（同 user key 不跨文件）----
  const ROCKSDB_NAMESPACE::Comparator* ucmp = mc_.cfd->user_comparator();
  const uint64_t target_size = compaction->max_output_file_size();
  const ROCKSDB_NAMESPACE::WriteOptions write_options(
      mc_.io_priority, ROCKSDB_NAMESPACE::Env::IOActivity::kCompaction);

  int64_t _current_time = 0;
  s = mc_.db_options->clock->GetCurrentTime(&_current_time);
  if (!s.ok()) {
    ROCKS_LOG_WARN(mc_.db_options->info_log,
                   "[ZfMaterializeJob] GetCurrentTime failed: %s",
                   s.ToString().c_str());
    _current_time = 0;
  }
  const uint64_t current_time = static_cast<uint64_t>(_current_time);
  const uint64_t oldest_key_time = current_time;  // 简化：无 oldest key time

  ROCKSDB_NAMESPACE::FileOptions fo_copy = *mc_.file_options;
  fo_copy.write_hint = ROCKSDB_NAMESPACE::Env::WLTH_NOT_SET;
  std::string fname;
  uint64_t cur_file_number = 0;
  ROCKSDB_NAMESPACE::FileMetaData cur_meta;
  ROCKSDB_NAMESPACE::TableBuilder* builder = nullptr;
  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFileWriter> file_writer;
  std::vector<ROCKSDB_NAMESPACE::FileMetaData> outs;

  auto open_new = [&]() -> ROCKSDB_NAMESPACE::Status {
    cur_file_number = mc_.versions->NewFileNumber();
    fname = ROCKSDB_NAMESPACE::TableFileName(
        mc_.cfd->ioptions().cf_paths, cur_file_number, 0);
    std::unique_ptr<ROCKSDB_NAMESPACE::FSWritableFile> file;
    ROCKSDB_NAMESPACE::IOStatus io_s = ROCKSDB_NAMESPACE::NewWritableFile(
        mc_.db_options->fs.get(), fname, &file, fo_copy);
    if (!io_s.ok()) {
      mc_.db_options->env->DeleteFile(fname).PermitUncheckedError();
      return io_s;
    }
    file->SetIOPriority(mc_.io_priority);
    file->SetWriteLifeTimeHint(fo_copy.write_hint);
    file_writer.reset(new ROCKSDB_NAMESPACE::WritableFileWriter(
        std::move(file), fname, *mc_.file_options, mc_.db_options->clock,
        mc_.io_tracer, mc_.stats, ROCKSDB_NAMESPACE::Histograms::SST_WRITE_MICROS,
        mc_.cfd->ioptions().listeners,
        mc_.cfd->ioptions().file_checksum_gen_factory.get(),
        mc_.cfd->ioptions().checksum_handoff_file_types.Contains(
            ROCKSDB_NAMESPACE::FileType::kTableFile),
        false /* buffered_data_with_checksum */));
    cur_meta = ROCKSDB_NAMESPACE::FileMetaData();
    cur_meta.fd = ROCKSDB_NAMESPACE::FileDescriptor(cur_file_number, 0, 0);
    // TableBuilderOptions 按文件号构造（TableProperties.unique_id 由
    // db_id/db_session_id/file_number 推导，打开时校验；file_number=0
    // 会与 GetSstInternalUniqueId 计算值不匹配 → Corruption）。
    ROCKSDB_NAMESPACE::TableBuilderOptions tboptions(
        mc_.cfd->ioptions(), mcf, read_options, write_options,
        mc_.cfd->internal_comparator(),
        mc_.cfd->internal_tbl_prop_coll_factories(), mc_.output_compression,
        mcf.compression_opts, mc_.cfd->GetID(), mc_.cfd->GetName(),
        compaction->output_level() /* level */, current_time /* newest_key_time */,
        false /* is_bottommost */,
        ROCKSDB_NAMESPACE::TableFileCreationReason::kFlush, oldest_key_time,
        current_time, mc_.db_id, mc_.db_session_id, 0 /* target_file_size */,
        cur_file_number,
        ROCKSDB_NAMESPACE::kMaxSequenceNumber /* preclude_last_level_min_seqno */);
    builder = ROCKSDB_NAMESPACE::NewTableBuilder(tboptions, file_writer.get());
    return ROCKSDB_NAMESPACE::Status::OK();
  };

  // 收尾当前文件：Finish → Sync → Close → 填 meta → 入 outs；
  // 空文件/失败路径删除物理文件。与 builder.cc BuildTable 的收尾对齐。
  auto finish_cur = [&](ROCKSDB_NAMESPACE::Status status)
      -> ROCKSDB_NAMESPACE::Status {
    if (builder == nullptr) {
      return status;
    }
    ROCKSDB_NAMESPACE::Status ss = status;
    if (ss.ok() && builder->IsEmpty()) {
      builder->Abandon();
      builder = nullptr;
      file_writer.reset();
      mc_.db_options->env->DeleteFile(fname).PermitUncheckedError();
      return ss;
    }
    if (ss.ok()) {
      ss = builder->Finish();
    }
    if (ss.ok()) {
      ROCKSDB_NAMESPACE::IOOptions opts;
      ROCKSDB_NAMESPACE::IOStatus io_s =
          ROCKSDB_NAMESPACE::WritableFileWriter::PrepareIOOptions(write_options,
                                                                  opts);
      if (io_s.ok()) {
        io_s = file_writer->Sync(opts, mc_.cfd->ioptions().use_fsync);
      }
      if (io_s.ok()) {
        io_s = file_writer->Close(opts);
      }
      ss = io_s;
    }
    if (ss.ok()) {
      cur_meta.fd.file_size = builder->FileSize();
      cur_meta.tail_size = builder->GetTailSize();
      cur_meta.marked_for_compaction = builder->NeedCompact();
      cur_meta.user_defined_timestamps_persisted =
          mc_.cfd->ioptions().persist_user_defined_timestamps;
      cur_meta.file_checksum = file_writer->GetFileChecksum();
      cur_meta.file_checksum_func_name = file_writer->GetFileChecksumFuncName();
      if (!mc_.db_id.empty() && !mc_.db_session_id.empty()) {
        if (!ROCKSDB_NAMESPACE::GetSstInternalUniqueId(
                 mc_.db_id, mc_.db_session_id, cur_meta.fd.GetNumber(),
                 &cur_meta.unique_id)
                 .ok()) {
          cur_meta.unique_id = ROCKSDB_NAMESPACE::kNullUniqueId64x2;
        }
      }
      // manifest 编码要求 epoch_number != kUnknownEpochNumber。
      cur_meta.epoch_number = mc_.cfd->NewEpochNumber();
      outs.push_back(std::move(cur_meta));
    } else {
      mc_.db_options->env->DeleteFile(fname).PermitUncheckedError();
    }
    builder = nullptr;
    file_writer.reset();
    return ss;
  };

  std::string last_user_key_buf;
  bool have_last = false;
  c_iter.SeekToFirst();
  for (; c_iter.Valid(); c_iter.Next()) {
    const ROCKSDB_NAMESPACE::Slice& key = c_iter.key();
    const ROCKSDB_NAMESPACE::Slice& value = c_iter.value();
    const ROCKSDB_NAMESPACE::ParsedInternalKey ikey = c_iter.ikey();
    if (builder == nullptr) {
      s = open_new();
      if (!s.ok()) {
        break;
      }
    } else if (have_last && builder->EstimatedFileSize() >= target_size &&
               ucmp->Compare(ikey.user_key, ROCKSDB_NAMESPACE::Slice(last_user_key_buf)) != 0) {
      // 达到目标大小且 user key 变化 → 切分新文件。
      s = finish_cur(ROCKSDB_NAMESPACE::Status::OK());
      if (!s.ok()) {
        break;
      }
      s = open_new();
      if (!s.ok()) {
        break;
      }
    }
    builder->Add(key, value);
    if (!builder->status().ok()) {
      s = builder->status();
      break;
    }
    s = cur_meta.UpdateBoundaries(key, value, ikey.sequence, ikey.type);
    if (!s.ok()) {
      break;
    }
    last_user_key_buf.assign(ikey.user_key.data(), ikey.user_key.size());
    have_last = true;
  }
  if (s.ok() && !c_iter.status().ok()) {
    s = c_iter.status();
  }
  s = finish_cur(s);

  if (!s.ok()) {
    // 清理已产出文件（尚未进入 outputs_）。
    for (const ROCKSDB_NAMESPACE::FileMetaData& m : outs) {
      const std::string fn = ROCKSDB_NAMESPACE::TableFileName(
          mc_.cfd->ioptions().cf_paths, m.fd.GetNumber(), 0);
      mc_.db_options->env->DeleteFile(fn).PermitUncheckedError();
    }
    return s;
  }

  {
    rocksdb::MutexLock l(&out_mu_);
    for (ROCKSDB_NAMESPACE::FileMetaData& m : outs) {
      MaterializeOutput out;
      out.part_id = part_id;
      out.decision = MaterializeDecision::kMergeBase;
      out.meta = std::move(m);
      outputs_.push_back(std::move(out));
    }
  }
  return ROCKSDB_NAMESPACE::Status::OK();
}

int ZfMaterializeJob::PickInstallLevel(
    const ROCKSDB_NAMESPACE::InternalKey& smallest,
    const ROCKSDB_NAMESPACE::InternalKey& largest) const {
  // 调用方须持 DB mutex（读取 cfd->current()）。
  ROCKSDB_NAMESPACE::VersionStorageInfo* vstorage =
      mc_.cfd->current()->storage_info();
  const int base = vstorage->base_level();
  const ROCKSDB_NAMESPACE::Slice u_smallest = smallest.user_key();
  const ROCKSDB_NAMESPACE::Slice u_largest = largest.user_key();
  const ROCKSDB_NAMESPACE::Comparator* ucmp = mc_.cfd->user_comparator();

  // M4.11b：直装 base 前复查运行中 compaction 的输出范围冲突——阶段 0
  // 检查后 compaction 可能启动（L0→L1 输出与直装范围重叠 → L1 重叠文件
  // → VersionBuilder 一致性检查崩溃，R54 实测 5GB）。冲突 → 回落 L0
  // （L0 允许重叠；compaction 消费后后续批次直装恢复）。
  if (mc_.compaction_picker != nullptr &&
      mc_.compaction_picker->RangeOverlapWithCompaction(
          u_smallest, u_largest, base)) {
    return 0;
  }
  // 可安装 ⟺ L0..base_level 均无文件与 [smallest, largest] 重叠
  // （M3_DESIGN.md §6.2；base_level 为当前首个非空层）。
  for (int l = 0; l <= base; ++l) {
    if (vstorage->OverlapInLevel(l, &u_smallest, &u_largest)) {
      return 0;  // 回落 L0
    }
    // 本批次已放置到该层的文件（跨 epoch ABA 防护：同批文件尚未进
    // vstorage，但安装后会同层）。
    if (batch_outputs_ != nullptr) {
      for (const auto& placed : *batch_outputs_) {
        if (placed.level != l) {
          continue;
        }
        const ROCKSDB_NAMESPACE::Slice p_smallest =
            placed.meta.smallest.user_key();
        const ROCKSDB_NAMESPACE::Slice p_largest = placed.meta.largest.user_key();
        // 区间相交 ⟺ !(u_largest < p_smallest || p_largest < u_smallest)。
        if (ucmp->Compare(u_largest, p_smallest) >= 0 &&
            ucmp->Compare(p_largest, u_smallest) >= 0) {
          return 0;
        }
      }
    }
  }
  return base;  // 直装 base_level
}

}  // namespace zeroflush
