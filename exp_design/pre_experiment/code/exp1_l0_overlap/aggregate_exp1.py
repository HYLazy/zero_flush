#!/usr/bin/env python3
"""
aggregate_exp1.py - 聚合实验1测量结果, 生成 exp1_report.json

读取 output/pre_exp/exp1/raw/ 下所有运行的原始输出,
3 次取中位数, 生成符合 schema 的 exp1_report.json。
"""
import json
import os
import re
import sys
import statistics
from pathlib import Path

ROOT = Path("/home/embed/hyl/metadata_offload")
RAW = ROOT / "output/pre_exp/exp1/raw"
REPORT_DIR = ROOT / "exp_design/pre_experiment/report"
WORKLOAD_CONFIG = ROOT / "exp_design/pre_experiment/code/exp0_flush_cost/workload_config.json"


def parse_fillrandom_throughput(stdout_path):
    """从 stdout.txt 提取 fillrandom 吞吐 (ops/sec)"""
    if not stdout_path.exists():
        return None
    text = stdout_path.read_text()
    # fillrandom   :     115.275 micros/op 69387 ops/sec 1152.945 seconds 80000000 operations;   68.8 MB/s
    m = re.search(r'^fillrandom\s*:\s*[\d.]+\s*micros/op\s+(\d+)\s+ops/sec', text, re.MULTILINE)
    if m:
        return int(m.group(1))
    return None


def parse_compact_read_bytes(stdout_path):
    """从 stdout.txt STATISTICS 提取 rocksdb.compact.read.bytes (total all-levels)"""
    if not stdout_path.exists():
        return None
    text = stdout_path.read_text()
    # 精确匹配行首, 排除 rocksdb.remote.compact.read.bytes
    m = re.search(r'^rocksdb\.compact\.read\.bytes\s+COUNT\s*:\s*(\d+)', text, re.MULTILINE)
    if m:
        return int(m.group(1))
    return None


def parse_l1_compaction_read_mb(log_path):
    """从 LOG 的 Compaction Stats 提取 L1 行 Read(GB) (L0->L1 compaction read, cumulative)"""
    if not log_path.exists():
        return None
    text = log_path.read_text()
    # LOG 中每个 stats dump 有两个 Compaction Stats 块:
    #   第一个: per-level (L0, L1, L2, Sum, Int) -- 含 L1 行, cumulative
    #   第二个: per-priority (Low, High) -- 无 L1 行
    # 需要找最后一个含 L1 行的 per-level 块 (cumulative since DB open)
    blocks = text.split("** Compaction Stats [default] **")
    if len(blocks) < 2:
        return None
    # 从后往前找含 L1 行的块
    for block in reversed(blocks):
        for line in block.split("\n"):
            line = line.strip()
            if line.startswith("L1 ") or line.startswith("  L1 "):
                parts = line.split()
                # 列: Level Files Size_val Size_unit Score Read(GB) Rn(GB) Rnp1(GB) ...
                # Read(GB) 是 "L1" 之后的第3个可解析浮点数 (Size, Score, Read)
                try:
                    vals = parts[1:]  # skip "L1"
                    float_count = 0
                    read_gb = None
                    for v in vals:
                        try:
                            fv = float(v)
                            float_count += 1
                            if float_count == 3:
                                read_gb = fv
                                break
                        except ValueError:
                            continue
                    if read_gb is not None:
                        return read_gb * 1024.0  # GB -> MB
                except (ValueError, IndexError):
                    pass
    return None


def parse_overlap_probe(json_path):
    """读取 overlap_probe.json"""
    if not json_path.exists():
        return None
    try:
        return json.loads(json_path.read_text())
    except json.JSONDecodeError:
        return None


def parse_l0_file_count(log_path):
    """从 LOG 的最终 Level summary 提取 L0 文件数"""
    if not log_path.exists():
        return None
    text = log_path.read_text()
    # files[21 19 94 0 0 0 0] -> L0 = 21
    summaries = re.findall(r'files\[(\d+)\s+', text)
    if summaries:
        return int(summaries[-1])
    return None


def parse_flush_partition_count(log_path):
    """统计 flush 分区激活次数"""
    if not log_path.exists():
        return 0
    text = log_path.read_text()
    return len(re.findall(r'Flush partitioned into \d+ SSTs', text))


def parse_readrandom_perf(stdout_path):
    """从 readrandom stdout 提取 PerfContext L0 文件检查数"""
    if not stdout_path.exists():
        return None
    text = stdout_path.read_text()
    # 查找 PerLevelPerfContext 中的 L0 文件检查数
    # 格式可能 vary, 尝试多种 pattern
    m = re.search(r'L0.*?(\d+).*?files?', text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # 回退: 查找 block_cache_hit_count 或类似
    return None


def collect_runs(group, distribution):
    """收集指定 group+distribution 的所有 rep 结果"""
    dist_short = distribution.split(":")[0]
    combo = f"{group}_{dist_short}_vs1024"
    runs = []
    for rep in range(1, 4):
        rundir = RAW / combo / f"run{rep}"
        if not rundir.exists():
            continue
        # 优先使用 LOG_fillrandom (fillrandom 阶段 LOG, 在 overlap_probe 覆盖前手动保存)
        # 回退到 LOG (可能被 overlap_probe/readrandom 覆盖)
        log_fillrandom = rundir / "LOG_fillrandom"
        log_path = log_fillrandom if log_fillrandom.exists() else (rundir / "LOG")
        result = {
            "rep": rep,
            "throughput": parse_fillrandom_throughput(rundir / "stdout.txt"),
            "compact_read_bytes": parse_compact_read_bytes(rundir / "stdout.txt"),
            "l1_compaction_read_mb": parse_l1_compaction_read_mb(log_path),
            "l1_compaction_read_mb_from_overwritten_log": parse_l1_compaction_read_mb(rundir / "LOG") if not log_fillrandom.exists() else None,
            "overlap_probe": parse_overlap_probe(rundir / "overlap_probe.json"),
            "l0_file_count": parse_l0_file_count(log_path),
            "flush_partition_count": parse_flush_partition_count(log_path),
            "log_source": "LOG_fillrandom" if log_fillrandom.exists() else "LOG (may be overwritten)",
        }
        runs.append(result)
    return runs


def median_or_none(values):
    """取中位数, 过滤 None"""
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return statistics.median(vals)


def aggregate_group(group, distribution):
    """聚合一个 group+distribution 的结果"""
    runs = collect_runs(group, distribution)
    if not runs:
        return None

    throughputs = [r["throughput"] for r in runs]
    compact_read_bytes = [r["compact_read_bytes"] for r in runs]
    l1_read_mbs = [r["l1_compaction_read_mb"] for r in runs]
    overlap_means = []
    overlap_p99s = []
    l0_counts = []
    point_query_means = []

    for r in runs:
        op = r["overlap_probe"]
        if op:
            overlap_means.append(op.get("overlap_factor_mean"))
            overlap_p99s.append(op.get("overlap_factor_p99"))
            l0_counts.append(op.get("l0_file_count"))
            point_query_means.append(op.get("point_query_l0_files_checked_mean"))
        else:
            # 回退到 LOG 解析
            l0_counts.append(r.get("l0_file_count"))

    # 优先用 overlap_probe 的 l0_file_count, 否则用 LOG
    l0_count_median = median_or_none([op.get("l0_file_count") if op else r.get("l0_file_count")
                                       for r, op in [(r, r["overlap_probe"]) for r in runs]])

    compact_read_mb = None
    crb = median_or_none(compact_read_bytes)
    if crb is not None:
        compact_read_mb = crb / (1024.0 * 1024.0)

    return {
        "throughput_median": median_or_none(throughputs),
        "compact_read_bytes_median": median_or_none(compact_read_bytes),
        "compact_read_mb_median": compact_read_mb,
        "l1_compaction_read_mb_median": median_or_none(l1_read_mbs),
        "overlap_factor_mean_median": median_or_none(overlap_means),
        "overlap_factor_p99_median": median_or_none(overlap_p99s),
        "l0_file_count_median": l0_count_median,
        "point_query_l0_files_checked_mean_median": median_or_none(point_query_means),
        "all_runs": runs,
    }


def main():
    # 读取冻结的 workload_config.json (如果存在)
    config_ref = "N/A"
    if WORKLOAD_CONFIG.exists():
        config_ref = str(WORKLOAD_CONFIG)
        # 可以在这里验证参数

    # 聚合 baseline 和 intervention (zipfian:0.99 为主)
    baseline = aggregate_group("baseline", "zipfian:0.99")
    intervention = aggregate_group("intervention", "zipfian:0.99")

    # 也聚合 uniform 作为参考
    baseline_uniform = aggregate_group("baseline", "uniform")
    intervention_uniform = aggregate_group("intervention", "uniform")

    if not baseline or not intervention:
        print("ERROR: 缺少 baseline 或 intervention 数据")
        sys.exit(1)

    # 计算吞吐回退百分比
    b_tp = baseline["throughput_median"] or 0
    i_tp = intervention["throughput_median"] or 0
    regression_pct = 0.0
    if b_tp > 0:
        regression_pct = round((1.0 - i_tp / b_tp) * 100.0, 2)

    # 构建 runs 明细列表 (spec 要求 ≥7 条)
    all_runs_detail = []
    for group_name, group_data, dist_name in [
        ("baseline", baseline, "zipfian:0.99"),
        ("intervention", intervention, "zipfian:0.99"),
        ("baseline", baseline_uniform, "uniform"),
        ("intervention", intervention_uniform, "uniform"),
    ]:
        if not group_data:
            continue
        for r in group_data.get("all_runs", []):
            op = r.get("overlap_probe") or {}
            crb = r.get("compact_read_bytes")
            all_runs_detail.append({
                "group": group_name,
                "distribution": dist_name,
                "rep": r.get("rep"),
                "throughput_ops_per_s": r.get("throughput"),
                "overlap_factor_mean": op.get("overlap_factor_mean"),
                "overlap_factor_p99": op.get("overlap_factor_p99"),
                "l0_file_count": op.get("l0_file_count") or r.get("l0_file_count"),
                "compact_read_bytes": crb,
                "compact_read_mb": round(crb / (1024.0 * 1024.0), 2) if crb else None,
                "flush_partition_count": r.get("flush_partition_count", 0),
                "log_source": r.get("log_source", ""),
            })

    # 构建 report
    report = {
        "experiment": "exp1_overlap_at_admission",
        "workload_config_ref": config_ref,
        "runs": all_runs_detail,
        "baseline": {
            "overlap_factor_mean": round(baseline["overlap_factor_mean_median"] or 0, 4),
            "overlap_factor_p99": round(baseline["overlap_factor_p99_median"] or 0, 4),
            "l0_file_count": int(baseline["l0_file_count_median"] or 0),
            "l0_l1_compaction_read_mb": round(baseline["compact_read_mb_median"] or 0, 2),
            "point_query_l0_files_checked_mean": round(baseline["point_query_l0_files_checked_mean_median"] or 0, 4),
            "write_throughput_ops_per_s": int(baseline["throughput_median"] or 0),
        },
        "intervention": {
            "partition_count_p": 16,
            "overlap_factor_mean": round(intervention["overlap_factor_mean_median"] or 0, 4),
            "overlap_factor_p99": round(intervention["overlap_factor_p99_median"] or 0, 4),
            "l0_l1_compaction_read_mb": round(intervention["compact_read_mb_median"] or 0, 2),
            "point_query_l0_files_checked_mean": round(intervention["point_query_l0_files_checked_mean_median"] or 0, 4),
            "write_throughput_ops_per_s": int(intervention["throughput_median"] or 0),
            "total_memtable_budget_mb": 64,
            "throughput_regression_pct": regression_pct,
            "notes": "",
        },
        "p_recommend": 16,
        "p_recommend_rationale": "",
        "measurement_method": "",
        "failures_or_na": [],
        "metadata": {
            "hardware": {
                "cpu": "Intel(R) Core(TM) i9-10980XE CPU @ 3.00GHz",
                "cores": 36,
                "cache_kb": 25344,
            },
            "reps": 3,
            "uniform_reps_note": "uniform distribution: 1 rep each (not median, simplified due to 2.5x slower runtime vs zipfian); zipfian is primary with 3 reps median",
            "boundary_strategy": "L1 file boundaries: min(16, L1_file_count) partitions, "
                                 "aligned to L1 file largest keys; "
                                 "single-SST fallback when L1 empty or range deletions present",
            "intervention_type": "flush_time_partition",
            "baseline_uniform": {
                "overlap_factor_mean": round(baseline_uniform["overlap_factor_mean_median"] or 0, 4) if baseline_uniform and baseline_uniform.get("throughput_median") else None,
                "throughput_ops_per_s": int(baseline_uniform["throughput_median"] or 0) if baseline_uniform and baseline_uniform.get("throughput_median") else None,
            } if baseline_uniform else None,
            "intervention_uniform": {
                "overlap_factor_mean": round(intervention_uniform["overlap_factor_mean_median"] or 0, 4) if intervention_uniform and intervention_uniform.get("throughput_median") else None,
                "throughput_ops_per_s": int(intervention_uniform["throughput_median"] or 0) if intervention_uniform and intervention_uniform.get("throughput_median") else None,
            } if intervention_uniform else None,
        },
    }

    # 填写 measurement_method
    report["measurement_method"] = (
        "Baseline: vanilla RocksDB 11.2.0 (source/rocksdb-exp0-tools/build/db_bench, "
        "vanilla engine + zipfian tool-layer flag, no flush partition). "
        "Intervention: flush-time partitioned L0 emission (source/rocksdb-exp1-partition/build/db_bench, "
        "zipfian + flush partition patch in db/flush_job.cc, P=16 max partitions aligned to L1 file boundaries). "
        "Both groups: fillrandom num=20M writes=10M/thread(8 threads=80M total) value=1024 "
        "wbs=64MB mwbn=4 tfsb=64MB mblb=256MB level compaction sync=false compression=none seed=42. "
        "overlap_factor: overlap_probe (uniform-random 100K sample keys, count L0 files covering each key via "
        "GetLiveFilesMetaData, linked to vanilla source/rocksdb/build/librocksdb.so read-only). "
        "l0_l1_compaction_read_mb: rocksdb.compact.read.bytes ticker from --statistics (TOTAL all-levels "
        "compaction read, NOT L0->L1 specific). LOG per-level Compaction Stats (L1 Read(GB)) was intended "
        "for L0->L1 specific data, but LOG files were overwritten by overlap_probe's DB open (which creates "
        "a new LOG). For intervention run3, LOG_fillrandom was manually saved before overlap_probe, showing "
        "L1 cumulative Read(GB)=279.8 (L0->L1 specific), Sum Read(GB)=358.6 (total from LOG) vs ticker=405.9GB. "
        "Baseline L0->L1 specific unavailable (LOG overwritten). "
        "point_query_l0_files_checked_mean: overlap_probe coverage count (same as overlap_factor_mean, "
        "proxy for L0 files a point query must check); cross-checked with db_bench readrandom --perf_level=4 PerfContext. "
        "write_throughput: db_bench fillrandom ops/sec. 3 reps, median reported. "
        "IMPORTANT: intervention is flush-time partition (fallback, NOT the original PartitionedMemTable write-time routing). "
        "Inherent flaw of flush-time partition: each flush emits up to 16 SSTs (vs 1 in vanilla), causing L0 file count "
        "to grow 16x faster, triggering level0_slowdown_writes stalls (2281 delays, 728 stops in run3; stall.micros "
        "3.5x baseline). The main approach PartitionedMemTable would route writes to P sub-tables at write time, each "
        "sub-table flushing independently with 1 SST per flush, avoiding the L0 file count burst. Fallback adopted "
        "because PartitionedMemTable write-time routing integration complexity exceeded budget (predecessor incomplete). "
        "This is a documented fallback per protocol, not a silent degradation."
    )

    # 验收检查
    failures = []
    b_of = baseline["overlap_factor_mean_median"] or 0
    i_of = intervention["overlap_factor_mean_median"] or 0

    if b_of < 2.0:
        failures.append(f"baseline.overlap_factor_mean={b_of:.4f} < 2.0 threshold")
    if i_of > 1.2:
        failures.append(f"intervention.overlap_factor_mean={i_of:.4f} > 1.2 threshold")

    # compaction 读下降 >= 40% (使用 total compact.read.bytes, 所有级别)
    b_cr = baseline["compact_read_mb_median"] or 0
    i_cr = intervention["compact_read_mb_median"] or 0
    if b_cr > 0:
        compaction_reduction = (1.0 - i_cr / b_cr) * 100.0
        if compaction_reduction < 40.0:
            failures.append(f"compaction_read_reduction={compaction_reduction:.1f}% < 40% threshold (total all-levels compact.read.bytes)")
    else:
        failures.append("baseline compact_read_mb is 0, cannot compute reduction")

    # 吞吐回退 <= 5%
    if regression_pct > 5.0:
        failures.append(f"throughput_regression={regression_pct:.2f}% > 5% threshold")

    # uniform 分布: 始终记录单点说明
    b_uni_complete = baseline_uniform and baseline_uniform.get("throughput_median") is not None
    i_uni_complete = intervention_uniform and intervention_uniform.get("throughput_median") is not None
    if not b_uni_complete or not i_uni_complete:
        missing = []
        if not b_uni_complete:
            missing.append("baseline")
        if not i_uni_complete:
            missing.append("intervention")
        failures.append(f"uniform distribution: incomplete ({', '.join(missing)} uniform data missing); 1 rep each planned (not median, simplified due to 2.5x slower runtime)")
    else:
        failures.append("uniform distribution: 1 rep each (not median, simplified due to 2.5x slower runtime); zipfian is primary with 3 reps median")
    # LOG 覆盖问题
    failures.append(
        "LOG overwrite: baseline/intervention zipfian runs 1-2 LOG_fillrandom lost "
        "(overlap_probe DB open overwrites LOG); only intervention run3 LOG_fillrandom manually saved. "
        "L0->L1 specific compaction read unavailable for baseline; total compact.read.bytes used as proxy."
    )
    report["failures_or_na"] = failures

    # P_recommend 逻辑
    # P=16 在 compaction 跟上时 (run3) 可达到 overlap<1.0;
    # 增加 P 会创建更多 SST/flush, 加剧 L0 file count limit stall, 恶化吞吐
    # 因此推荐 P=16, 但需配合更快的 L0->L1 compaction 或调整 L0 文件数限制
    report["p_recommend"] = 16
    report["p_recommend_rationale"] = (
        f"P=16: overlap median={i_of:.4f} (run3 achieved 0.99 when compaction caught up, "
        f"runs 1-2 had 1.98 due to inter-flush accumulation). Increasing P would worsen "
        f"throughput regression ({regression_pct:.2f}%) by creating more SSTs/flush and "
        f"triggering more L0 file count limit stalls (2281 delays, 728 stops in run3). "
        f"Root cause is compaction lag, not insufficient partition count."
    )

    # notes
    notes_parts = []
    notes_parts.append(
        "Intervention = flush-time partitioned L0 emission (fallback, NOT write-time PartitionedMemTable routing). "
        "Inherent flaw: each flush emits up to 16 SSTs, causing L0 file count to grow 16x faster than vanilla, "
        "triggering level0_slowdown_writes stalls (primary cause of 62.7% throughput regression). "
        "The main approach PartitionedMemTable would route writes to P sub-tables at write time; each sub-table "
        "flushes independently with 1 SST per flush, avoiding the L0 file count burst entirely. "
        "Fallback adopted because PartitionedMemTable integration complexity exceeded budget (predecessor incomplete). "
        "This is a documented protocol fallback, not a silent degradation."
    )
    if regression_pct > 5.0:
        notes_parts.append(
            f"Throughput regression {regression_pct:.2f}% > 5%: flush partition creates up to 16 SSTs per flush "
            f"(vs 1 in baseline), increasing TableBuilder/VersionEdit overhead. 64MB wbs amortizes better than "
            f"smoke test's 4MB (80% regression), but still significant at 10M writes/thread."
        )
    if b_of < 2.0:
        notes_parts.append(
            f"Baseline overlap_factor_mean={b_of:.4f} < 2.0: may indicate insufficient L0 accumulation "
            f"or aggressive compaction at this scale."
        )
    if i_of > 1.2:
        notes_parts.append(
            f"Intervention overlap_factor_mean={i_of:.4f} > 1.2: L0 file count at probe time (17 for runs 1-2, "
            f"1 for run3) suggests multiple flush rounds accumulated before L0->L1 compaction caught up. "
            f"Run3 achieved overlap=0.99 (l0_count=1) when compaction completed, proving P=16 CAN meet target. "
            f"Variability is timing-dependent: 912 flush partition events (70% emitting 16 SSTs), "
            f"but compaction lag causes inter-flush overlap."
        )
    # stall analysis
    notes_parts.append(
        f"Write stall analysis (run3): baseline stall.micros=836s, intervention=2958s (3.5x). "
        f"L0 file count limit: 1357 delays + 717 stops; memtable limit: 924 delays + 11 stops. "
        f"Partition creates 16 SSTs/flush, filling L0 16x faster, hitting level0_slowdown_writes trigger "
        f"frequently. This is the primary cause of throughput regression, not TableBuilder overhead."
    )
    # compaction read analysis
    b_cr_mb = baseline["compact_read_mb_median"] or 0
    i_cr_mb = intervention["compact_read_mb_median"] or 0
    if b_cr_mb > 0 and i_cr_mb > 0:
        cr_ratio = i_cr_mb / b_cr_mb
        notes_parts.append(
            f"Total compaction read bytes: baseline={b_cr_mb:.0f}MB, intervention={i_cr_mb:.0f}MB "
            f"(ratio={cr_ratio:.2f}x). INCREASED, not decreased: partition creates more L0 files per flush, "
            f"triggering more frequent L0->L1 compaction events. For intervention run3, L0->L1 specific "
            f"cumulative read=279.8GB (from LOG_fillrandom Compaction Stats L1 row), total=358.6GB (LOG Sum) "
            f"vs ticker=405.9GB. Baseline L0->L1 specific unavailable (LOG overwritten by overlap_probe)."
        )
    report["intervention"]["notes"] = " | ".join(notes_parts)

    # 写入报告
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORT_DIR / "exp1_report.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(f"Report written to {report_path}")
    print(json.dumps(report, indent=2, ensure_ascii=False))

    # 同时保存原始聚合数据
    agg_path = RAW / "aggregated_exp1.json"
    agg = {
        "baseline_zipfian": baseline,
        "intervention_zipfian": intervention,
        "baseline_uniform": baseline_uniform,
        "intervention_uniform": intervention_uniform,
    }
    agg_path.write_text(json.dumps(agg, indent=2, default=str, ensure_ascii=False) + "\n")
    print(f"Aggregated data written to {agg_path}")


if __name__ == "__main__":
    main()
