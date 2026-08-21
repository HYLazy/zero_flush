#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""aggregate_exp2.py - 实验 2 结果聚合：每组合 3 次运行取中位数（记极差）。

输入：--raw-dir 下的原始 JSON
  range_p{P}_{impl}_rep{r}.json   （range_lookup_bench 单次运行）
  wal_p{P}_{A|B}_{true|false}_rep{r}.json （multi_wal_fsync_bench 单次运行）

输出：--out 指定的 aggregated.json（供 decision.py 与 build_report.py 使用）。

仅标准库 + common/py/exp_common.py。
"""

import argparse
import glob
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "common", "py"))
from exp_common import median_of_runs, collect_hw_json  # noqa: E402

MODE_NAMES = {"A": "independent_fsync", "B": "batched_fsync"}


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def aggregate_range(raw_dir, failures):
    results = []
    for p in (64, 128, 256):
        for impl in ("binary_search", "small_btree"):
            pat = os.path.join(raw_dir, f"range_p{p}_{impl}_rep*.json")
            files = sorted(glob.glob(pat))
            if len(files) < 3:
                failures.append(
                    f"range_lookup p={p} impl={impl}: only {len(files)}/3 runs")
            means, p99s = [], []
            reps = []
            for fp in files:
                try:
                    d = load_json(fp)
                    run = d["runs"][0]
                    means.append(run["mean_ns"])
                    p99s.append(run["p99_ns"])
                    reps.append({"file": os.path.basename(fp),
                                 "mean_ns": run["mean_ns"],
                                 "p99_ns": run["p99_ns"]})
                except Exception as e:  # noqa: BLE001
                    failures.append(f"range_lookup parse fail {fp}: {e}")
            if not means:
                continue
            med_mean, rng_mean = median_of_runs(means)
            med_p99, rng_p99 = median_of_runs(p99s)
            results.append({
                "p": p,
                "impl": impl,
                "mean_ns": round(med_mean, 3),
                "p99_ns": float(med_p99),
                "mean_ns_range": round(rng_mean, 3),
                "p99_ns_range": float(rng_p99),
                "reps": reps,
            })
    return results


def aggregate_wal(raw_dir, failures):
    results = []
    ops_per_combo = {}
    env = {"kernel": "", "mount_opts": "", "scheduler": "",
           "cpu_governor": "", "value_size": 0, "key_distribution": ""}
    for p in (1, 16, 64):
        for m in ("A", "B"):
            for sync in (True, False):
                tag = "true" if sync else "false"
                pat = os.path.join(
                    raw_dir, f"wal_p{p}_{m}_{tag}_rep*.json")
                files = sorted(glob.glob(pat))
                if len(files) < 3:
                    failures.append(
                        f"multi_wal p={p} mode={m} sync={sync}: "
                        f"only {len(files)}/3 runs")
                tputs, fops, wins = [], [], []
                reps = []
                for fp in files:
                    try:
                        d = load_json(fp)
                        tputs.append(d["throughput_ops_per_s"])
                        fops.append(d["fsync_count_per_op"])
                        wins.append(d["durability_window_ms"]["p99"])
                        reps.append({
                            "file": os.path.basename(fp),
                            "ops": d["ops"],
                            "elapsed_s": d["elapsed_s"],
                            "throughput_ops_per_s": d["throughput_ops_per_s"],
                            "fsync_count": d["fsync_count"],
                            "fsync_count_per_op": d["fsync_count_per_op"],
                            "durability_window_ms_p99":
                                d["durability_window_ms"]["p99"],
                            "commit_rounds": d["commit_rounds"],
                            "avg_dirty_files_per_round":
                                d["avg_dirty_files_per_round"],
                        })
                        # 环境信息取任一份（同机一致）
                        for k in ("kernel", "mount_opts", "scheduler",
                                  "cpu_governor", "value_size",
                                  "key_distribution"):
                            if d.get(k) and not env.get(k):
                                env[k] = d[k]
                        combo_key = f"p{p}_{MODE_NAMES[m]}_{tag}"
                        ops_per_combo[combo_key] = {
                            "ops_per_rep": [r["ops"] for r in reps],
                            "reason": d.get("ops_reason", ""),
                        }
                    except Exception as e:  # noqa: BLE001
                        failures.append(f"multi_wal parse fail {fp}: {e}")
                if not tputs:
                    continue
                mt, rt = median_of_runs(tputs)
                mf, rf = median_of_runs(fops)
                mw, rw = median_of_runs(wins)
                results.append({
                    "p": p,
                    "mode": MODE_NAMES[m],
                    "sync_wal": sync,
                    "throughput_ops_per_s": round(mt, 1),
                    "throughput_range": round(rt, 1),
                    "fsync_count_per_op": round(mf, 6),
                    "fsync_count_per_op_range": round(rf, 6),
                    "durability_window_ms": round(mw, 4),
                    "durability_window_ms_range": round(rw, 4),
                    "reps": reps,
                })
    return results, ops_per_combo, env


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    failures = []
    range_results = aggregate_range(args.raw_dir, failures)
    wal_results, ops_per_combo, env = aggregate_wal(args.raw_dir, failures)

    baseline = [
        {"sync_wal": r["sync_wal"],
         "throughput_ops_per_s": r["throughput_ops_per_s"],
         "fsync_count_per_op": r["fsync_count_per_op"]}
        for r in wal_results
        if r["p"] == 1 and r["mode"] == "independent_fsync"
    ]
    baseline.sort(key=lambda x: (not x["sync_wal"]))  # sync=true 在前

    hw = collect_hw_json()
    hw["cpu_governor"] = env.get("cpu_governor", "")

    agg = {
        "experiment": "exp2_partition_overhead_gate",
        "range_lookup": {"results": range_results},
        "multi_wal_fsync": {
            "results": wal_results,
            "baseline_single_wal": baseline,
        },
        "failures_or_na": failures,
        "metadata": {
            "hardware": hw,
            "kernel": env.get("kernel", ""),
            "mount_opts": env.get("mount_opts", ""),
            "scheduler": env.get("scheduler", ""),
            "reps": 3,
            "ops_per_combo": ops_per_combo,
            "value_size": env.get("value_size", 1024),
            "key_distribution": env.get("key_distribution", ""),
        },
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(agg, f, indent=2, ensure_ascii=False)
    print(f"[aggregate] range_combos={len(range_results)} "
          f"wal_combos={len(wal_results)} baseline={len(baseline)} "
          f"failures={len(failures)} -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
