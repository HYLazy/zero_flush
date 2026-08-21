#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""build_report.py - 汇总 aggregated.json + decision.json 为最终报告。

产出严格 schema 的 exp2_report.json，并做验收自检：
  - range_lookup 6 组合（mean_ns/p99_ns > 0）
  - multi_wal 12 组合 + baseline 2 档完整
  - decision 与 rationale 数据一致（decision.json 原样引用）
"""

import argparse
import json
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--agg", required=True)
    ap.add_argument("--decision", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    with open(args.agg, "r", encoding="utf-8") as f:
        agg = json.load(f)
    with open(args.decision, "r", encoding="utf-8") as f:
        dec = json.load(f)

    range_results = [
        {"p": r["p"], "impl": r["impl"],
         "mean_ns": r["mean_ns"], "p99_ns": r["p99_ns"]}
        for r in agg["range_lookup"]["results"]
    ]
    wal_results = [
        {"p": r["p"], "mode": r["mode"], "sync_wal": r["sync_wal"],
         "throughput_ops_per_s": r["throughput_ops_per_s"],
         "fsync_count_per_op": r["fsync_count_per_op"],
         "durability_window_ms": r["durability_window_ms"]}
        for r in agg["multi_wal_fsync"]["results"]
    ]
    baseline = agg["multi_wal_fsync"]["baseline_single_wal"]

    notes = (
        "CPU 调频器为 powersave（实测低频运行），延迟用 rdtsc cycles 经 "
        "CLOCK_MONOTONIC 校准换算 ns（invariant TSC，校准频率见原始 JSON "
        "tsc_freq_hz），调频器状态记录于 metadata.hardware.cpu_governor；"
        "multi_wal 写 key 采用均匀分布以制造 worst-case dirty 扇出"
        "（zipfian(0.99) 会把约 96% 质量压到 P=64 的 0 号分区，"
        "低估 fsync 扇出开销）；range_lookup 按任务要求使用 zipfian(0.99)，"
        "每组合丢弃 1,000,000 次预热后测量 10,000,000 次；"
        "durability_window_ms 取各轮 p99 的中位数；"
        "模式A commit 批=base_batch(512 ops)，模式B commit 批=base_batch*P，"
        "用更大的组提交摊销每文件 fsync。ops 自适应：sync=true 档按 "
        "pilot 速率×10s 目标迭代重标定（实测每组合 >=10s）；sync=false 档"
        "取 [2M,4M] ops（满足 spec >=2M ops；实测更大量会触发脏页回写"
        "限速并干扰后续组合，故该档以 ops 数而非时长为准）。")

    report = {
        "experiment": "exp2_partition_overhead_gate",
        "range_lookup": {"results": range_results},
        "multi_wal_fsync": {
            "results": wal_results,
            "baseline_single_wal": baseline,
        },
        "decision": dec["decision"],
        "decision_rationale": dec["decision_rationale"],
        "notes": notes,
        "failures_or_na": agg.get("failures_or_na", []),
        "metadata": agg["metadata"],
    }
    # 判定引用的具体数值一并放入 metadata，保证 decision 与数据一致可追溯
    report["metadata"]["decision_referenced_values"] = dec.get(
        "referenced_values", {})
    if dec.get("alternatives"):
        report["metadata"]["alternatives"] = dec["alternatives"]

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    # ---- 验收自检 ----
    errors = []
    if len(range_results) != 6:
        errors.append(f"range_lookup 组合数={len(range_results)} != 6")
    for r in range_results:
        if not (r["mean_ns"] > 0 and r["p99_ns"] > 0):
            errors.append(f"range_lookup 非正数值: {r}")
    if len(wal_results) != 12:
        errors.append(f"multi_wal 组合数={len(wal_results)} != 12")
    combos = {(r["p"], r["mode"], r["sync_wal"]) for r in wal_results}
    expect = {(p, m, s) for p in (1, 16, 64)
              for m in ("independent_fsync", "batched_fsync")
              for s in (True, False)}
    if combos != expect:
        errors.append(f"multi_wal 组合缺失: {expect - combos}")
    if len(baseline) != 2:
        errors.append(f"baseline 档数={len(baseline)} != 2")
    for r in wal_results:
        for k in ("throughput_ops_per_s", "fsync_count_per_op",
                  "durability_window_ms"):
            if r[k] is None or r[k] < 0:
                errors.append(f"multi_wal 非法数值 {k}: {r}")
    if report["decision"] not in ("PASS", "FAIL", "REDESIGN"):
        errors.append(f"decision 非法: {report['decision']}")
    if report["decision"] in ("FAIL", "REDESIGN"):
        if "替代设计" not in report["decision_rationale"]:
            errors.append("FAIL/REDESIGN 的 rationale 未列出替代设计")
    if report["failures_or_na"]:
        print(f"[selfcheck] WARNING failures_or_na 非空: "
              f"{report['failures_or_na']}")
    if errors:
        print("[selfcheck] FAILED:")
        for e in errors:
            print("  -", e)
        return 1
    print(f"[selfcheck] OK: range=6, wal=12, baseline=2, "
          f"decision={report['decision']} -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
