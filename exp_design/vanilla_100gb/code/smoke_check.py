#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""smoke_check.py - 端到端冒烟自检（~4GB，约 1 分钟）。

流程：小规模 run_exp（4M × 1KB，4 线程，stats 每 2s）→ parse → plot →
断言四项指标齐全且数值合理（有 compaction 事件、有周期 dump 块、带宽序列非空）。

用法：python3 smoke_check.py [--out <run_dir>] [--keep]
"""

import argparse
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = "/home/embed/hyl/metadata_offload"
OUT_DEFAULT = f"{ROOT}/output/vanilla_100gb/smoke_run"


def run(args_list):
    r = subprocess.run(args_list, capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stdout, r.stderr)
        sys.exit(f"命令失败: {' '.join(args_list)} rc={r.returncode}")
    return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=OUT_DEFAULT)
    ap.add_argument("--keep", action="store_true", help="保留 DB 目录")
    args = ap.parse_args()

    print("[smoke] 1/3 小规模运行 db_bench（4M×1KB，~20s）...", flush=True)
    run([sys.executable, os.path.join(HERE, "run_exp.py"), "--out", args.out,
         "--num", "4000000", "--value-size", "1024", "--threads", "4",
         "--stats-interval", "2", "--timeout-min", "10",
         "--no-peak-bw", "--force"]
        + (["--keep-db"] if args.keep else []))

    print("[smoke] 2/3 解析...", flush=True)
    run([sys.executable, os.path.join(HERE, "parse_exp.py"), args.out])

    print("[smoke] 3/3 出图...", flush=True)
    run([sys.executable, os.path.join(HERE, "plot_exp.py"), args.out])

    with open(os.path.join(args.out, "metrics.json"), encoding="utf-8") as f:
        m = json.load(f)
    errs = []
    src = m["meta"]["data_sources"]
    if src["blocks"] < 3:
        errs.append(f"周期 dump 块太少: {src['blocks']} < 3")
    if src["log_events"] < 4:
        errs.append(f"LOG 事件太少: {src['log_events']}")
    if not m["wa"]["per_pair_cnt"]:
        errs.append("无 compaction 层对计数")
    if not m["wa"]["per_level"]:
        errs.append("无逐层 compaction 统计")
    if not m["bandwidth"]["device_series"]:
        errs.append("设备带宽序列为空")
    if m["stalls"]["stall_reasons"] is None:
        errs.append("stall 分原因缺失")
    if len(errs) == 0:
        print("[smoke] PASS ✓")
        print(f"  blocks={src['blocks']} log_events={src['log_events']} "
              f"samples={src['samples']}")
        print(f"  per_pair={m['wa']['per_pair_cnt']}")
        print(f"  wa_device={m['wa']['wa_device']} "
              f"stall_pct={m['stalls']['stall_cum_pct']}%")
        return 0
    print("[smoke] FAIL ✗")
    for e in errs:
        print("  -", e)
    return 1


if __name__ == "__main__":
    sys.exit(main())
