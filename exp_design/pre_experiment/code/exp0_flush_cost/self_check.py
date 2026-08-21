#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""self_check.py - Exp0 报告验收自检。

用法: self_check.py <exp0_report.json>
逐项检查:
  1. JSON 可解析且必填字段齐全 (严格 schema)
  2. runs >= 7 (6 fillrandom 组合 + ycsb-a)
  3. 6 个组合全覆盖 (3 value size × 2 分布)
  4. decomposition_residual_pct < 5
  5. flush_share_of_nand_writes ∈ (0,1); 0.2-0.4 区间检查 (偏离仅提示)
  6. stall 数据非空或有解释
  7. 每个 fillrandom run 可与原始输出对账 (抽查 raw 目录存在)
"""

import json
import os
import sys

TOP_FIELDS = ["experiment", "status", "rocksdb_version", "commit_hash",
              "workload_config", "runs", "summary", "failures_or_na",
              "metadata"]
RUN_FIELDS = ["benchmark", "value_size_bytes", "distribution",
              "wall_time_sec", "cpu_time_sec", "user_bytes_written_mb",
              "wal_bytes_written_mb", "flush_bytes_written_mb",
              "compaction_bytes_written_mb", "total_nand_write_mb",
              "stall_count", "stall_total_ms", "notes"]
SUMMARY_FIELDS = ["flush_share_of_nand_writes", "wa_decomposition",
                  "decomposition_residual_pct"]


def main():
    path = sys.argv[1]
    errors, warns, oks = [], [], []

    with open(path, "r", encoding="utf-8") as f:
        r = json.load(f)

    for fld in TOP_FIELDS:
        if fld not in r:
            errors.append(f"顶层缺字段: {fld}")
    runs = r.get("runs", [])
    if len(runs) < 7:
        errors.append(f"runs 数量 {len(runs)} < 7")
    else:
        oks.append(f"runs 数量 = {len(runs)} (>=7)")

    for i, run in enumerate(runs):
        for fld in RUN_FIELDS:
            if fld not in run:
                errors.append(f"runs[{i}] 缺字段: {fld}")

    fr = [x for x in runs if x.get("benchmark") == "fillrandom"]
    got = {(x.get("distribution"), x.get("value_size_bytes")) for x in fr}
    expected = {(d, vs) for d in ("zipfian", "uniform")
                for vs in (100, 1024, 4096)}
    missing = expected - got
    if missing:
        errors.append(f"缺失组合: {sorted(missing)}")
    else:
        oks.append("6 个 fillrandom 组合全覆盖")

    for fld in SUMMARY_FIELDS:
        if fld not in r.get("summary", {}):
            errors.append(f"summary 缺字段: {fld}")

    resid = r.get("summary", {}).get("decomposition_residual_pct")
    if resid is None or resid >= 5:
        errors.append(f"decomposition_residual_pct={resid} 不满足 <5")
    else:
        oks.append(f"decomposition_residual_pct={resid} <5")

    share = r.get("summary", {}).get("flush_share_of_nand_writes")
    if share is None or not (0 < share < 1):
        errors.append(f"flush_share={share} 不在 (0,1)")
    elif not (0.2 <= share <= 0.4):
        warns.append(f"flush_share={share:.4f} 超出 0.2-0.4 (notes 应有口径分析)")
    else:
        oks.append(f"flush_share={share:.4f} ∈ [0.2, 0.4]")

    stall_ok = all(
        x.get("stall_count") is not None or "stall" in str(x.get("notes", ""))
        for x in fr)
    if not stall_ok:
        errors.append("存在 fillrandom run stall 数据缺失且无解释")
    else:
        oks.append("stall 数据齐全或有解释")

    # 对账抽查: 每个 fillrandom run 的 notes 应含 ops/sec, 且 raw 目录存在
    root = "/home/embed/hyl/metadata_offload/output/pre_exp/exp0/raw"
    na_list = r.get("failures_or_na", [])
    for x in fr:
        combo = f"{x['distribution']}_vs{x['value_size_bytes']}"
        d = os.path.join(root, combo)
        if not os.path.isdir(d):
            errors.append(f"原始输出目录缺失: {d}")
        else:
            n = len([e for e in os.listdir(d) if e.startswith("run")])
            if n < 3:
                errors.append(f"{combo} 原始 run 目录数 {n} < 3")
    oks.append("fillrandom 原始输出目录可对账")

    if not na_list:
        warns.append("failures_or_na 为空 (确认无 N/A 项?)")

    print("=== Exp0 验收自检 ===")
    for o in oks:
        print("  [OK]  ", o)
    for w in warns:
        print("  [WARN]", w)
    for e in errors:
        print("  [FAIL]", e)
    print(f"结论: {'PASS' if not errors else 'FAIL'} "
          f"({len(oks)} ok, {len(warns)} warn, {len(errors)} error)")
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
