#!/usr/bin/env python3
"""ZeroFlush M1 性能对比脚本：ZeroFlush 修复后 vs 原生 RocksDB

跑 fillrandom / readrandom / readseq 三种工作负载，5 个 value_size，
输出 JSON 格式结果到 /home/embed/hyl/metadata_offload/output/zeroflush_m1_perf/。

使用：
  python run_perf_compare.py [--num 100000] [--runs 1]
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

DB_BENCH = "/home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/build/db_bench"
OUT_DIR = Path("/home/embed/hyl/metadata_offload/output/zeroflush_m1_perf")
TMP_DB_BASE = "/tmp/zf_perf_db"

# bench_name -> (value_size, zeroflush)
CONFIGS = [
    ("native",   False),
    ("zeroflush", True),
]

VALUE_SIZES = [32, 128, 512, 1024, 4096]
BENCHMARKS = "fillrandom,readrandom,readseq"


def parse_ops(line):
    """解析类似 'fillrandom : 1.234 micros/op 811003 ops/sec 0.123 seconds 100000 operations; 80.0 MB/s'"""
    m = re.search(
        r"(\S+)\s*:\s*([\d.]+)\s*micros/op\s*(\d+)\s*ops/sec\s*([\d.]+)\s*seconds\s*(\d+)\s*operations;\s*([\d.]+)\s*MB/s",
        line,
    )
    if not m:
        return None
    return {
        "name": m.group(1),
        "us_per_op": float(m.group(2)),
        "ops_per_sec": int(m.group(3)),
        "seconds": float(m.group(4)),
        "operations": int(m.group(5)),
        "mb_per_sec": float(m.group(6)),
    }


def run_one(num, value_size, zeroflush, run_id):
    """跑一次，返回解析后的指标列表"""
    db_dir = f"{TMP_DB_BASE}_{'zf' if zeroflush else 'native'}_vs{value_size}_r{run_id}"
    subprocess.run(["rm", "-rf", db_dir], check=False)

    cmd = [
        DB_BENCH,
        f"--benchmarks={BENCHMARKS}",
        f"--num={num}",
        f"--value_size={value_size}",
        f"--key_size=16",
        f"--db={db_dir}",
        "--compression_type=none",
        "--report_file_operations=false",
    ]
    if zeroflush:
        cmd.append("--zeroflush")

    env = os.environ.copy()
    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=600)
    elapsed = time.time() - t0

    if proc.returncode != 0:
        return {"error": f"db_bench exit {proc.returncode}", "stderr_tail": proc.stderr[-500:]}

    results = []
    for line in proc.stdout.splitlines():
        parsed = parse_ops(line)
        if parsed and parsed["name"] in ("fillrandom", "readrandom", "readseq"):
            results.append(parsed)

    # 清理
    subprocess.run(["rm", "-rf", db_dir], check=False)

    return {
        "wall_time_sec": round(elapsed, 2),
        "results": results,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--num", type=int, default=100000)
    ap.add_argument("--runs", type=int, default=1, help="每组重复运行次数取平均")
    ap.add_argument("--value-sizes", nargs="+", type=int, default=VALUE_SIZES)
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    overall = {
        "config": {
            "db_bench": DB_BENCH,
            "num_keys": args.num,
            "runs_per_config": args.runs,
            "benchmarks": BENCHMARKS,
            "key_size": 16,
        },
        "hardware": {
            "cpu_count": os.cpu_count(),
        },
        "data": [],
    }

    for vs in args.value_sizes:
        row = {"value_size": vs, "configs": {}}
        total_mb = args.num * (16 + vs) / (1024 * 1024)
        row["total_data_mb"] = round(total_mb, 1)
        print(f"\n[vs={vs}B, total={row['total_data_mb']}MB] running...", flush=True)

        for name, zf in CONFIGS:
            runs = []
            for r in range(args.runs):
                print(f"  - {name} run {r+1}/{args.runs}...", end=" ", flush=True)
                res = run_one(args.num, vs, zf, r)
                if "error" in res:
                    print(f"FAIL: {res['error']}")
                    runs.append(res)
                else:
                    print("OK")
                    runs.append(res)
            row["configs"][name] = runs

        overall["data"].append(row)

    out_path = OUT_DIR / f"perf_compare_num{args.num}_runs{args.runs}.json"
    with open(out_path, "w") as f:
        json.dump(overall, f, indent=2, ensure_ascii=False)
    print(f"\n→ Results saved to {out_path}")


if __name__ == "__main__":
    main()
