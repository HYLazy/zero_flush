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

ROCKSDB_NAMESPACE::Status ZfMaterializeJob::Run() {
  // 按序断言（单后台 flush 线程 + imm FIFO，M3_DESIGN.md §6.2）：
  //  - 正常按序：epoch == last + 1；
  //  - manifest 安装失败回滚后重试：epoch ≤ last（last 已回滚到批次前）。
  // 多 flush 线程并发由 ZF Open 的 max_background_flushes=1 排除。
  assert(epoch_ <= ctx_->last_materialized_epoch() + 1);
  if (se_.gens.empty()) {
    return ROCKSDB_NAMESPACE::Status::OK();
  }

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

  // ---- 阶段 0（持 DB mutex）：融合归并触发判定 + Compaction 注册 ----
  mc_.db_mutex->Lock();
  ROCKSDB_NAMESPACE::Status s = PlanLocked();
  if (!s.ok()) {
    // 阶段 0 失败不产生临时文件；释放已注册的部分 Compaction。
    FinishPlansLocked();
    mc_.db_mutex->Unlock();
    return s;
  }
  mc_.db_mutex->Unlock();

  // ---- 阶段 1：K 路并行物化（不持 DB mutex）----
  const uint32_t K =
      std::max<uint32_t>(1, ctx_->zfo_.materialize_parallelism);
  std::vector<std::vector<uint32_t>> shards(K);
  for (uint32_t i = 0; i < part_ids_.size(); ++i) {
    shards[i % K].push_back(part_ids_[i]);
  }

  auto worker = [this, &shards](uint32_t k) {
    for (uint32_t part_id : shards[k]) {
      if (stop_.load(std::memory_order_relaxed)) {
        return;
      }
      // 该 part 的全部 gen：正常封存 1 个 + 可能收养的恢复期孤儿代。
      std::vector<std::pair<uint32_t, uint32_t>> gens;
      for (const auto& g : se_.gens) {
        if (g.first == part_id) {
          gens.push_back(g);
        }
      }
      if (gens.empty()) {
        continue;
      }
      const PartitionPlan* plan = FindPlan(part_id);
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
      const ROCKSDB_NAMESPACE::Status ss =
          (plan != nullptr && plan->decision == MaterializeDecision::kMergeBase)
              ? MaterializeMergePartition(part_id, gens, plan->overlap_all,
                                          plan->compaction.get(), lo, hi)
              : MaterializePartition(part_id, gens, lo, hi);
      if (!ss.ok()) {
        // 记录首个错误并停掉其余 worker（已产出文件由 Run 统一清理）。
        bool expected = false;
        if (stop_.compare_exchange_strong(expected, true)) {
          rocksdb::MutexLock l(&out_mu_);
          first_error_ = ss;
        }
        return;
      }
    }
  };
  std::vector<std::thread> workers;
  workers.reserve(K);
  for (uint32_t k = 0; k < K; ++k) {
    workers.emplace_back(worker, k);
  }
  for (auto& t : workers) {
    t.join();
  }

  // 任一路失败：删除本 job 已生成的全部临时 SST，释放 Compaction 注册，
  // 批次输出不追加。
  {
    rocksdb::MutexLock l(&out_mu_);
    if (!first_error_.ok()) {
      std::vector<MaterializeOutput> outs = std::move(outputs_);
      outputs_.clear();
      for (auto& o : outs) {
        const std::string fname = ROCKSDB_NAMESPACE::TableFileName(
            mc_.cfd->ioptions().cf_paths, o.meta.fd.GetNumber(),
            o.meta.fd.GetPathId());
        mc_.db_options->env->DeleteFile(fname).PermitUncheckedError();
      }
      mc_.db_mutex->Lock();
      FinishPlansLocked();
      mc_.db_mutex->Unlock();
      return first_error_;
    }
  }

  // ---- 阶段 2：逐文件定层 / 回填融合元信息（须持 DB mutex）----
  // 关键：本批已定层的文件须立即写入 batch_outputs_——PickInstallLevel 的
  // 批内重叠检查只扫描 batch_outputs_（历史批次 + 本批已放置项）。若攒到
  // 最后统一追加，同批文件互相看不到对方：hash 模式下多个分区文件键范围
  // 交错重叠，会全部直装同一层 → VersionBuilder force_consistency_checks
  // 报 "L6 has overlapping ranges"（对应 M3_DESIGN.md §6.2 批内互斥）。
  mc_.db_mutex->Lock();
  std::vector<MaterializeOutput> outs = std::move(outputs_);
  for (auto& o : outs) {
    if (o.decision == MaterializeDecision::kMergeBase) {
      // 融合输出：直装阶段 0 决策的 base 层（替换 overlap 输入文件）。
      // replaced_inputs/rewritten_bytes 由调用方安装循环消费。
      const PartitionPlan* plan = FindPlan(o.part_id);
      assert(plan != nullptr &&
             plan->decision == MaterializeDecision::kMergeBase);
      o.level = plan->compaction->output_level();
      o.replaced_inputs = plan->overlap;
      o.rewritten_bytes = plan->overlap_bytes;
      // 替换文件号：existing 重叠文件。
      for (ROCKSDB_NAMESPACE::FileMetaData* r : plan->overlap) {
        o.replaced_file_numbers.push_back(r->fd.GetNumber());
      }
      // 批内链式替换（§7.4 批次内多 epoch）：本输出与同批次前序输出
      // （level 相同、user key 范围重叠）合并后范围 ⊇ 前序 → 前序由本
      // 输出替代：标记 superseded（不安装）、继承其替换文件号、物理
      // 文件由 Run() 返回前删除。未安装文件无需 DeleteFile（不在任何
      // 已提交版本中），故只继承前序的 replaced_file_numbers。
      if (batch_outputs_ != nullptr) {
        const ROCKSDB_NAMESPACE::Comparator* ucmp2 = mc_.cfd->user_comparator();
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
      o.level = PickInstallLevel(o.meta.smallest, o.meta.largest);
      if (o.level == 0) {
        ctx_->install_fallback_l0_.fetch_add(1, std::memory_order_relaxed);
      } else {
        ctx_->install_direct_base_.fetch_add(1, std::memory_order_relaxed);
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
  mc_.db_mutex->Unlock();
  // 删除批内被替换输出的物理文件（从未安装；持锁 IO 不必要，放解锁后）。
  for (uint64_t fn : orphan_files_) {
    const std::string fname = ROCKSDB_NAMESPACE::TableFileName(
        mc_.cfd->ioptions().cf_paths, fn, 0);
    mc_.db_options->env->DeleteFile(fname).PermitUncheckedError();
  }
  return ROCKSDB_NAMESPACE::Status::OK();
}

ROCKSDB_NAMESPACE::Status ZfMaterializeJob::PlanLocked() {
  mc_.db_mutex->AssertHeld();
  plans_.clear();
  plans_.reserve(part_ids_.size());

  // 融合开关（§7.2）：merge_into_base_level 开启、非孤儿代 epoch（§8.1
  // 保守分支，孤儿代 A 侧 seq 与 base 不保证严格递增）、仅范围路由模式
  // （hash 模式分区键集交错，无法判定 base 文件与分区的隶属关系）。
  const bool merge_enabled = ctx_->zfo_.merge_into_base_level &&
                             !se_.has_adopted_orphans && table_ != nullptr &&
                             !table_->IsHashMode();
  ROCKSDB_NAMESPACE::VersionStorageInfo* vstorage =
      (mc_.base != nullptr) ? mc_.base->storage_info() : nullptr;
  const int base = (vstorage != nullptr) ? vstorage->base_level() : 0;
  const ROCKSDB_NAMESPACE::Comparator* ucmp = mc_.cfd->user_comparator();
  const ROCKSDB_NAMESPACE::MutableCFOptions& mcf = *mc_.mutable_cf_options;

  for (uint32_t pid : part_ids_) {
    PartitionPlan plan;
    plan.part_id = pid;
    plan.decision = MaterializeDecision::kDirect;
    if (!merge_enabled) {
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
    for (ROCKSDB_NAMESPACE::FileMetaData* f : vstorage->LevelFiles(base)) {
      const ROCKSDB_NAMESPACE::Slice f_lo = f->smallest.user_key();
      const ROCKSDB_NAMESPACE::Slice f_hi = f->largest.user_key();
      // 相交 ⟺ !(f_hi < lo || f_lo >= hi)。
      if (ucmp->Compare(f_hi, plan.lo) < 0 ||
          ucmp->Compare(f_lo, plan.hi) >= 0) {
        continue;
      }
      // 完全包含：lo <= f_lo && f_hi < hi；越界文件（分区边界切割）
      // 无法安全替换 → 放弃融合。
      if (ucmp->Compare(f_lo, plan.lo) < 0 ||
          ucmp->Compare(f_hi, plan.hi) >= 0) {
        ok = false;
        break;
      }
      if (f->being_compacted) {
        // 若为本批次前序融合注册所标记 → 跳过（由批内链式替换的
        // last_batch 提供 B 侧覆盖），否则为原生 compaction 冲突 → 降级。
        if (batch_registered_files_.count(f->fd.GetNumber()) != 0) {
          batch_skipped = true;
          continue;
        }
        // 原生 compaction 正在使用该文件 → 冲突降级（§7.3 不等待）。
        ok = false;
        break;
      }
      overlap.push_back(f);
      overlap_bytes += f->fd.GetFileSize();
    }
    if (!ok) {
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
    if (overlap.empty() && !batch_skipped) {
      plans_.push_back(std::move(plan));
      continue;
    }

    // 被批内前序注册标记的文件必须由批内融合输出覆盖（last_batch 为
    // kMergeBase），否则 B 侧缺数据 → 安全降级 kFallback。
    if (batch_skipped &&
        (last_batch == nullptr ||
         last_batch->decision != MaterializeDecision::kMergeBase)) {
      plans_.push_back(std::move(plan));
      continue;
    }

    // 触发比（§7.2）：sealed_bytes / overlap_bytes >= base_merge_min_ratio。
    // overlap 为空时（batch_skipped 场景）跳过比率检查——融合成本已被
    // 前序承担，B 侧全量由 last_batch（融合输出）提供。
    if (!overlap.empty()) {
      const auto it = se_.part_bytes.find(pid);
      const uint64_t sealed =
          (it != se_.part_bytes.end()) ? it->second : 0;
      const double ratio =
          static_cast<double>(sealed) / static_cast<double>(overlap_bytes);
      if (ratio < ctx_->zfo_.base_merge_min_ratio) {
        plans_.push_back(std::move(plan));
        continue;
      }
    }

    // 上层（L0..base-1）与分区范围重叠：直装会被更旧的 L0 数据遮蔽
    // （读路径 L0 优先），且原生 L0→base compaction 可能并发产出同层
    // 重叠文件 → 放弃融合（§7.2 保守分支）。
    bool upper_conflict = false;
    for (int l = 0; l < base && !upper_conflict; ++l) {
      if (vstorage->OverlapInLevel(l, &plan.lo, &plan.hi)) {
        upper_conflict = true;
      }
    }
    // 批内已放置的 L0 文件（未安装，vstorage 不可见）与分区重叠时
    // 同样遮蔽直装输出 → 放弃融合（批次内多 epoch 的 ABA 防护）。
    if (!upper_conflict && batch_outputs_ != nullptr) {
      for (const MaterializeOutput& x : *batch_outputs_) {
        if (x.level != 0 || x.superseded) {
          continue;
        }
        const ROCKSDB_NAMESPACE::Slice x_lo = x.meta.smallest.user_key();
        const ROCKSDB_NAMESPACE::Slice x_hi = x.meta.largest.user_key();
        if (ucmp->Compare(x_hi, plan.lo) >= 0 &&
            ucmp->Compare(x_lo, plan.hi) < 0) {
          upper_conflict = true;
          break;
        }
      }
    }
    if (upper_conflict) {
      plans_.push_back(std::move(plan));
      continue;
    }

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

    // 构造 Compaction：inputs[0] 空（level=0）、inputs[1] = overlap_all、
    // output = base、kFlush。构造即 MarkFilesBeingCompacted(true)。
    std::vector<ROCKSDB_NAMESPACE::CompactionInputFiles> inputs(2);
    inputs[0].level = 0;
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
        -1 /* score */, false /* l0_files_might_overlap */);
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
  return ROCKSDB_NAMESPACE::Status::OK();
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

  // 排序：VectorIterator 构造时按 internal comparator 排 indices。
  const uint64_t sort_start = mc_.db_options->clock->NowMicros();
  std::unique_ptr<ROCKSDB_NAMESPACE::VectorIterator> iter(
      new ROCKSDB_NAMESPACE::VectorIterator(
          std::move(keys), std::move(values),
          &mc_.cfd->internal_comparator()));
  sort_micros_.fetch_add(mc_.db_options->clock->NowMicros() - sort_start,
                         std::memory_order_relaxed);

  iter->SeekToFirst();
  assert(iter->Valid());

  // 范围断言（仅范围路由模式；hash 模式各分区输出范围可能交错，跳过）。
  if (table_ != nullptr && !table_->IsHashMode()) {
    iter->SeekToFirst();
    const rocksdb::Slice u_smallest =
        ROCKSDB_NAMESPACE::ExtractUserKey(iter->key());
    iter->SeekToLast();
    const rocksdb::Slice u_largest =
        ROCKSDB_NAMESPACE::ExtractUserKey(iter->key());
    iter->SeekToFirst();
    const ROCKSDB_NAMESPACE::Comparator* ucmp = mc_.cfd->user_comparator();
    // [smallest, largest] ⊆ [lo, hi)，lo/hi 空 = -∞/+∞。
    if ((!lo.empty() && ucmp->Compare(u_smallest, lo) < 0) ||
        (!hi.empty() && ucmp->Compare(u_largest, hi) >= 0)) {
      return ROCKSDB_NAMESPACE::Status::Corruption(
          "ZF partition " + std::to_string(part_id) +
          " materialized range outside table bounds");
    }
  }

  // BuildTable（模板对齐 FlushJob::WriteLevel0Table；level=0 构建语义）。
  ROCKSDB_NAMESPACE::FileMetaData meta;
  meta.fd = ROCKSDB_NAMESPACE::FileDescriptor(
      mc_.versions->NewFileNumber(), 0, 0);
  const ROCKSDB_NAMESPACE::MutableCFOptions& mcf = *mc_.mutable_cf_options;

  const std::string* const full_history_ts_low =
      (mc_.full_history_ts_low.empty()) ? nullptr : &mc_.full_history_ts_low;
  ROCKSDB_NAMESPACE::ReadOptions read_options(
      ROCKSDB_NAMESPACE::Env::IOActivity::kFlush);
  read_options.rate_limiter_priority = mc_.io_priority;
  const ROCKSDB_NAMESPACE::WriteOptions write_options(
      mc_.io_priority, ROCKSDB_NAMESPACE::Env::IOActivity::kFlush);

  int64_t _current_time = 0;
  ROCKSDB_NAMESPACE::Status s =
      mc_.db_options->clock->GetCurrentTime(&_current_time);
  if (!s.ok()) {
    ROCKS_LOG_WARN(mc_.db_options->info_log,
                   "[ZfMaterializeJob] GetCurrentTime failed: %s",
                     s.ToString().c_str());
    _current_time = 0;
  }
  const uint64_t current_time = static_cast<uint64_t>(_current_time);
  const uint64_t oldest_key_time = current_time;  // 简化：无 oldest key time

  ROCKSDB_NAMESPACE::TableBuilderOptions tboptions(
      mc_.cfd->ioptions(), mcf, read_options, write_options,
      mc_.cfd->internal_comparator(), mc_.cfd->internal_tbl_prop_coll_factories(),
      mc_.output_compression, mcf.compression_opts, mc_.cfd->GetID(),
      mc_.cfd->GetName(), 0 /* level */, current_time /* newest_key_time */,
      false /* is_bottommost */, ROCKSDB_NAMESPACE::TableFileCreationReason::kFlush,
      oldest_key_time, current_time, mc_.db_id, mc_.db_session_id,
      0 /* target_file_size */, meta.fd.GetNumber(),
      ROCKSDB_NAMESPACE::kMaxSequenceNumber /* preclude_last_level_min_seqno */);

  ROCKSDB_NAMESPACE::IOStatus io_s;
  std::vector<ROCKSDB_NAMESPACE::BlobFileAddition> blob_file_additions;
  uint64_t memtable_payload_bytes = 0;
  uint64_t memtable_garbage_bytes = 0;
  ROCKSDB_NAMESPACE::TableProperties table_properties;
  s = ROCKSDB_NAMESPACE::BuildTable(
      mc_.dbname, mc_.versions, *mc_.db_options, tboptions, *mc_.file_options,
      mc_.cfd->table_cache(), iter.get(),
      std::vector<std::unique_ptr<
          ROCKSDB_NAMESPACE::FragmentedRangeTombstoneIterator>>(),
      &meta, &blob_file_additions, mc_.job_context->snapshot_seqs,
      mc_.earliest_snapshot, mc_.job_context->earliest_write_conflict_snapshot,
      mc_.job_context->GetJobSnapshotSequence(),
      mc_.job_context->snapshot_checker, mcf.paranoid_file_checks,
      mc_.cfd->internal_stats(), &io_s, mc_.io_tracer,
      ROCKSDB_NAMESPACE::BlobFileCreationReason::kFlush,
      mc_.seqno_to_time_mapping.get(), mc_.event_logger, mc_.job_id,
      &table_properties, ROCKSDB_NAMESPACE::Env::WLTH_NOT_SET,
      full_history_ts_low, mc_.blob_callback,
      nullptr /* version：无 blob/range tombstone，无需 base_ */,
      &memtable_payload_bytes, &memtable_garbage_bytes,
      nullptr /* flush_stats */, nullptr /* blob_file_garbages */,
      mc_.fast_sst_open);
  io_s.PermitUncheckedError();
  if (!s.ok()) {
    return s;
  }
  // 与原生 FlushJob 一致（flush_job.cc PickMemTable）：manifest 编码要求
  // epoch_number != kUnknownEpochNumber（version_edit.cc EncodeTo 校验）。
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
}

ROCKSDB_NAMESPACE::Status ZfMaterializeJob::MaterializeMergePartition(
    uint32_t part_id, const std::vector<std::pair<uint32_t, uint32_t>>& gens,
    const std::vector<ROCKSDB_NAMESPACE::FileMetaData*>& overlap_all,
    const ROCKSDB_NAMESPACE::Compaction* compaction,
    const ROCKSDB_NAMESPACE::Slice& lo, const ROCKSDB_NAMESPACE::Slice& hi) {
  assert(!gens.empty());
  assert(!overlap_all.empty());
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

  const uint64_t sort_start = mc_.db_options->clock->NowMicros();
  std::unique_ptr<ROCKSDB_NAMESPACE::VectorIterator> a_iter(
      new ROCKSDB_NAMESPACE::VectorIterator(
          std::move(keys), std::move(values),
          &mc_.cfd->internal_comparator()));
  sort_micros_.fetch_add(mc_.db_options->clock->NowMicros() - sort_start,
                         std::memory_order_relaxed);
  a_iter->SeekToFirst();
  assert(a_iter->Valid());

  // 范围断言（同 MaterializePartition）。
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
      return ROCKSDB_NAMESPACE::Status::Corruption(
          "ZF partition " + std::to_string(part_id) +
          " materialized range outside table bounds");
    }
  }

  // seq 前置断言（§7.4）：A 侧全部记录必须比 B 侧任何记录新。
  // 孤儿代 epoch 已在 PlanLocked 降级 kFallback；此处做运行时防御。
  uint64_t max_b_seq = 0;
  for (const ROCKSDB_NAMESPACE::FileMetaData* f : overlap_all) {
    max_b_seq = std::max(max_b_seq, f->fd.largest_seqno);
  }
  if (min_seq <= max_b_seq) {
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
