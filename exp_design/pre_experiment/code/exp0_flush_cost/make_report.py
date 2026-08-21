#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""make_report.py - 组装 Exp0 最终报告 exp0_report.json 与冻结配置。

用法: make_report.py <out_dir> <report_out.json>
读取: out_dir/{aggregated.json, wa_summary.json, ycsb_result.json}
产出: report_out.json + code/exp0_flush_cost/workload_config.json (冻结配置)
"""

import datetime
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "common", "py"))
from exp_common import collect_hw_json  # noqa: E402

ROOT = "/home/embed/hyl/metadata_offload"
ROCKSDB_VERSION = "11.2.0"
# 源码树无 .git 目录, build_version.cc 显示 git_sha:0 → 无真实 commit hash
COMMIT_HASH = "N/A"

MB = 1024.0 * 1024.0

KEY_SIZE = 16
NUM = 20000000
WRITES = 10000000


def _mb(x):
    return None if x is None else round(x / MB, 3)


def build_workload_config():
    return {
        "engine": "RocksDB",
        "rocksdb_version": ROCKSDB_VERSION,
        "commit_hash": COMMIT_HASH,
        "build": {
            "source_tree": "source/rocksdb-exp0-tools (vanilla "
                           "source/rocksdb 的隔离副本, 仅 tools/db_bench_tool.cc "
                           "打 --key_distribution 工具层补丁, 引擎零改动)",
            "build_dir": "source/rocksdb-exp0-tools/build",
            "cmake": "-DCMAKE_BUILD_TYPE=Release -DWITH_GFLAGS=ON "
                     "-DWITH_JNI=OFF -DWITH_TESTS=OFF -DWITH_TOOLS=OFF "
                     "-DWITH_CORE_TOOLS=ON -DWITH_BENCHMARK_TOOLS=ON "
                     "-DWITH_SNAPPY=OFF -DWITH_ZLIB=OFF -DWITH_LZ4=OFF "
                     "-DWITH_ZSTD=OFF -DUSE_RTTI=AUTO -DPORTABLE=0",
            "ld_library_path": "<build_dir>:/usr/lib/x86_64-linux-gnu "
                               "(禁止 anaconda 路径)",
        },
        "key_size_bytes": KEY_SIZE,
        "value_sizes_bytes": [100, 1024, 4096],
        "num_keys": NUM,
        "ops_per_run": WRITES,
        "write_buffer_size_mb": 64,
        "max_write_buffer_number": 4,
        "compaction_style": "level",
        "target_file_size_base_mb": 64,
        "max_bytes_for_level_base_mb": 256,
        "distributions": ["zipfian(theta=0.99)", "uniform"],
        "benchmarks": ["fillrandom", "ycsb-a"],
        "sync_wal": False,
        "threads": 8,
        "seed": 42,
        "compression_type": "none",
        "repetitions_per_point": 3,
        "aggregation": "median",
    }


def fillrandom_run_entry(agg, wa):
    stall_slowdowns = agg.get("stalls_slowdowns")
    stall_stops = agg.get("stalls_stops")
    stall_count = None
    if stall_slowdowns is not None and stall_stops is not None:
        stall_count = int(stall_slowdowns + stall_stops)
    stall_ms = None
    if agg.get("stall_micros") is not None:
        stall_ms = round(agg["stall_micros"] / 1000.0, 3)

    disk_xcheck = (wa or {}).get("disk_crosscheck_residual_pct")
    notes = (
        f"writes={WRITES}/thread × 8 threads = {WRITES * 8} 总ops "
        f"(db_bench --writes 为每线程计数), seed=42, "
        f"compression=none(构建未含压缩库), "
        f"ops/sec={agg.get('ops_per_sec')}, "
        f"3次极差: wall={agg.get('wall_time_sec_range')}s, "
        f"wal={_mb(agg.get('wal_bytes_range'))}MB, "
        f"flush={_mb(agg.get('flush_bytes_range'))}MB, "
        f"compact={_mb(agg.get('compact_write_bytes_range'))}MB; "
        f"磁盘增量交叉核对残差={disk_xcheck}% "
        f"(口径: du(DB) vs flush+compact_write-compact_read+驻留WAL)")
    return {
        "benchmark": "fillrandom",
        "value_size_bytes": agg["value_size"],
        "distribution": agg["distribution"],
        "wall_time_sec": agg.get("wall_time_sec"),
        "cpu_time_sec": agg.get("cpu_time_sec"),
        "user_bytes_written_mb": _mb((wa or {}).get("user_bytes_written")),
        "wal_bytes_written_mb": _mb(agg.get("wal_bytes")),
        "flush_bytes_written_mb": _mb(agg.get("flush_bytes")),
        "compaction_bytes_written_mb": _mb(agg.get("compact_write_bytes")),
        "total_nand_write_mb": _mb((wa or {}).get("total_nand_write")),
        "stall_count": stall_count,
        "stall_total_ms": stall_ms,
        "notes": notes,
    }


def ycsb_run_entry(y):
    if not y:
        return None
    note = ("YCSB workloada (50% read/50% update, zipfian request "
            f"distribution), recordcount={y['recordcount']}, "
            f"operationcount={y['operationcount']}, threads=8, "
            f"value=1 field x 1024B; binding=rocksdbjni 6.2.2 (与本地 "
            "RocksDB 11.2 割裂, 仅作交叉验证); 引擎 Statistics 不经 JNI "
            "暴露 → WAL/flush/compaction 写字节为 N/A; "
            f"DB 最终磁盘占用={_mb(y.get('disk_final_bytes'))}MB; "
            f"吞吐={y.get('overall_throughput_ops_sec')} ops/sec, "
            f"latency={y.get('ops')}")
    return {
        "benchmark": "ycsb-a",
        "value_size_bytes": 1024,
        "distribution": "zipfian",
        "wall_time_sec": y.get("wall_time_sec"),
        "cpu_time_sec": y.get("cpu_time_sec"),
        "user_bytes_written_mb": _mb(
            y["operationcount"] * 0.5 * (10 + 1024))
        if y.get("operationcount") else None,
        "wal_bytes_written_mb": "N/A",
        "flush_bytes_written_mb": "N/A",
        "compaction_bytes_written_mb": "N/A",
        "total_nand_write_mb": "N/A",
        "stall_count": "N/A",
        "stall_total_ms": "N/A",
        "notes": note,
    }


def main():
    out_dir, report_path = sys.argv[1], sys.argv[2]

    with open(os.path.join(out_dir, "aggregated.json")) as f:
        aggs = json.load(f)
    wa_summary = {}
    wa_path = os.path.join(out_dir, "wa_summary.json")
    if os.path.exists(wa_path):
        with open(wa_path) as f:
            wa_summary = json.load(f)
    ycsb = None
    ycsb_path = os.path.join(out_dir, "ycsb_result.json")
    if os.path.exists(ycsb_path):
        with open(ycsb_path) as f:
            ycsb = json.load(f)

    wa_by_combo = {c["combo"]: c for c in wa_summary.get("checks", [])}
    fill_aggs = [a for a in aggs if a.get("benchmark") == "fillrandom"]

    runs = [fillrandom_run_entry(a, wa_by_combo.get(a["combo"]))
            for a in fill_aggs]
    ycsb_entry = ycsb_run_entry(ycsb)
    if ycsb_entry:
        runs.append(ycsb_entry)

    failures_or_na = []
    if COMMIT_HASH == "N/A":
        failures_or_na.append(
            "commit_hash=N/A: 源码树无 .git 目录, build_version.cc 显示 "
            "rocksdb_build_git_sha:0; 版本以 include/rocksdb/version.h "
            "的 11.2.0 为准")
    failures_or_na.append(
        "ycsb-a 的 wal/flush/compaction 字节与 stall 指标为 N/A: YCSB "
        "rocksdb binding (rocksdbjni 6.2.2) 不创建/暴露 RocksDB Statistics, "
        "该条仅作吞吐/延迟交叉验证")
    failures_or_na.append(
        "compression_type=none: 本机构建未启用任何压缩库 (与 vanilla build "
        "配置一致), db_bench 默认 snappy 不可用; 附录 A 未指定压缩, 如实记录")
    failures_or_na.append(
        "口径: db_bench --num/--writes 为每线程计数, 实际总写入 80M ops "
        "(10M×8 线程), keyspace=20M 全局共享; user_bytes_written 按总 ops "
        "计算 (已用 WAL 字节≈用户字节×1.05 验证)")

    expected_combos = {f"{d}_vs{vs}"
                       for d in ("zipfian", "uniform")
                       for vs in (100, 1024, 4096)}
    got_combos = {a["combo"] for a in fill_aggs if a.get("n_ok", 0) > 0}
    missing = expected_combos - got_combos
    if missing:
        failures_or_na.append(f"缺失组合: {sorted(missing)}")

    for a in fill_aggs:
        if a.get("n_ok", 0) < a.get("n_runs", 0):
            failures_or_na.append(
                f"{a['combo']}: {a['n_ok']}/{a['n_runs']} 次运行解析成功, "
                f"失败: {a.get('failed_runs')}")

    status = ("done"
              if not missing and len(fill_aggs) == 6 and ycsb_entry
              and not any("缺失" in x for x in failures_or_na)
              else "failed")

    summary = {
        "flush_share_of_nand_writes":
            wa_summary.get("flush_share_of_nand_writes_mean"),
        "wa_decomposition":
            wa_summary.get("wa_decomposition_mean", {}),
        "decomposition_residual_pct":
            wa_summary.get("decomposition_residual_pct_max"),
    }
    # 口径补充 (不影响 schema 必填字段)
    disk_xchecks = [c.get("disk_crosscheck_residual_pct")
                    for c in wa_summary.get("checks", [])
                    if c.get("disk_crosscheck_residual_pct") is not None]
    summary["notes"] = (
        "total_nand_write 按任务口径 = wal+flush+compaction 三项之和, "
        "故 decomposition_residual_pct 恒为 0 (守恒恒成立); 磁盘实际增量交叉"
        "核对口径为 du(DB) vs flush+compact_write-compact_read+驻留WAL "
        "(旧 WAL 在 flush 后即删除, wal_bytes 为累计写流量), 残差: "
        f"{[round(x, 2) for x in disk_xchecks]}% (逐组合)。"
        "wa_decomposition 为 6 组合 wa_x 的平均值。")

    report = {
        "experiment": "exp0_flush_cost_breakdown",
        "status": status,
        "rocksdb_version": ROCKSDB_VERSION,
        "commit_hash": COMMIT_HASH,
        "workload_config": build_workload_config(),
        "runs": runs,
        "summary": summary,
        "failures_or_na": failures_or_na,
        "metadata": {
            "hardware": collect_hw_json(),
            "reps": 3,
            "timestamp": datetime.datetime.now().isoformat(),
            "raw_outputs": "output/pre_exp/exp0/raw/<combo>/run<N>/"
                           "{stdout.txt,stderr.txt,time.txt,LOG,disk_bytes.txt}",
            "wa_summary_detail": "output/pre_exp/exp0/wa_summary.json",
            "ycsb_binding": "rocksdbjni 6.2.2 (source/YCSB), 与本地 RocksDB "
                            "11.2.0 版本割裂, 仅作交叉验证",
        },
    }

    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    # 冻结全局 workload 配置
    cfg_path = os.path.join(_HERE, "workload_config.json")
    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump({"workload_config": report["workload_config"]}, f,
                  indent=2, ensure_ascii=False)
    print(f"report -> {report_path} (status={status}, runs={len(runs)})")
    print(f"workload_config -> {cfg_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
