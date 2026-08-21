#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""wa_check.py - Exp0 写放大 (WA) 分解计算与守恒校验。

输入: parse_stats.py aggregate 产出的聚合 JSON (每组合中位数)。
输出: wa_summary.json, 内容含每组合 WA 分解、残差、flush 占比与验收判定。

口径说明 (重要):
  db_bench 的 --writes 为每线程计数, 实际总写入 = writes × threads
  (已用冒烟数据验证: WAL 字节 ≈ 总ops×(key+value)×1.05)。
  user_bytes_written  = writes × threads × (key_size + value_size)
  wa_x                = x 写字节 / user_bytes_written
  total_nand_write    = wal + flush + compaction (三项之和为基准, 按任务定义)
  decomposition_residual_pct = |total_nand_write − (wal+flush+compaction)|
                               / total_nand_write × 100
      由于 total_nand_write 按定义为三项之和, 该残差恒为 0; 真正的守恒
      交叉核对用"磁盘实际增量"口径:
          期望最终磁盘 ≈ flush + compact_write − compact_read + 驻留WAL
      (旧 WAL 在对应 memtable flush 后即被删除, 只驻留未 flush 部分;
       compaction 读走的旧 SST 字节不再驻盘), 记为 disk_crosscheck_residual_pct,
      二者均输出, notes 中说明口径。
"""

import json
import sys
import os

KEY_SIZE = 16          # 附录 A
WRITES = 10000000      # 附录 A (全量, 每线程); 冒烟由 --writes 覆盖经参数传入
THREADS = 8            # 附录 A; db_bench --writes 为每线程计数


def check_combo(agg, key_size=KEY_SIZE, writes=None, threads=THREADS):
    """对单个组合 (中位数指标) 计算 WA 分解与残差。返回 dict。"""
    vs = agg["value_size"]
    if writes is None:
        writes = agg.get("writes", WRITES)
    user_bytes = writes * threads * (key_size + vs)

    wal = agg.get("wal_bytes")
    flush = agg.get("flush_bytes")
    cw = agg.get("compact_write_bytes")
    cr = agg.get("compact_read_bytes")
    disk = agg.get("disk_final_bytes")
    wal_resident = agg.get("wal_resident_bytes") or 0.0

    out = {
        "combo": agg["combo"],
        "benchmark": agg["benchmark"],
        "value_size": vs,
        "distribution": agg["distribution"],
        "writes": writes,
        "user_bytes_written": user_bytes,
    }
    if wal is None or flush is None or cw is None:
        out["error"] = "missing ticker data (wal/flush/compact_write)"
        return out

    total_nand = wal + flush + cw
    out.update({
        "wal_bytes": wal,
        "flush_bytes": flush,
        "compaction_bytes": cw,
        "total_nand_write": total_nand,
        "wa_wal": wal / user_bytes,
        "wa_flush": flush / user_bytes,
        "wa_compaction": cw / user_bytes,
        "decomposition_residual_pct":
            abs(total_nand - (wal + flush + cw)) / total_nand * 100.0,
        "flush_share_of_nand_writes": flush / total_nand,
    })

    # 磁盘实际增量交叉核对: 期望 = flush + cw - cr + 驻留WAL
    # (旧 WAL 在 flush 后被删除, 累计 wal_bytes 是写流量而非驻留量)
    if disk and disk > 0 and cr is not None:
        expected = flush + cw - cr + wal_resident
        out["disk_expected_bytes"] = expected
        out["disk_crosscheck_residual_pct"] = (
            abs(disk - expected) / disk * 100.0)
    else:
        out["disk_crosscheck_residual_pct"] = None
    return out


def main():
    if len(sys.argv) < 3:
        print("usage: wa_check.py <aggregated.json> <out_wa_summary.json> "
              "[writes_override]")
        return 2
    with open(sys.argv[1], "r", encoding="utf-8") as f:
        aggs = json.load(f)
    writes_override = int(sys.argv[3]) if len(sys.argv) > 3 else None

    checks = []
    for agg in aggs:
        if agg.get("benchmark") != "fillrandom":
            continue  # ycsb-a 无引擎 ticker, 不参与 WA 分解
        checks.append(check_combo(agg, writes=writes_override))

    ok_checks = [c for c in checks if "error" not in c]
    summary = {"checks": checks, "failures": []}

    if ok_checks:
        shares = [c["flush_share_of_nand_writes"] for c in ok_checks]
        summary["flush_share_of_nand_writes_mean"] = sum(shares) / len(shares)
        n = len(ok_checks)
        summary["wa_decomposition_mean"] = {
            "wal": sum(c["wa_wal"] for c in ok_checks) / n,
            "flush": sum(c["wa_flush"] for c in ok_checks) / n,
            "compaction": sum(c["wa_compaction"] for c in ok_checks) / n,
        }
        summary["decomposition_residual_pct_max"] = max(
            c["decomposition_residual_pct"] for c in ok_checks)
        # 验收判定
        if summary["decomposition_residual_pct_max"] >= 5.0:
            summary["failures"].append(
                "decomposition_residual_pct >= 5, 需重跑")
        share = summary["flush_share_of_nand_writes_mean"]
        if not (0.2 <= share <= 0.4):
            summary["failures"].append(
                f"flush_share_of_nand_writes={share:.4f} 超出 0.2-0.4 区间, "
                "需在 notes 分析口径")
        for c in ok_checks:
            dc = c.get("disk_crosscheck_residual_pct")
            if dc is not None and dc >= 5.0:
                summary["failures"].append(
                    f"{c['combo']} 磁盘交叉核对残差 {dc:.2f}% >= 5 "
                    "(口径: flush+compact_write-compact_read+驻留WAL vs du)")

    with open(sys.argv[2], "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    for c in checks:
        if "error" in c:
            print(f"{c['combo']}: ERROR {c['error']}")
            continue
        print(f"{c['combo']}: wa_wal={c['wa_wal']:.3f} "
              f"wa_flush={c['wa_flush']:.3f} wa_comp={c['wa_compaction']:.3f} "
              f"flush_share={c['flush_share_of_nand_writes']:.3f} "
              f"residual={c['decomposition_residual_pct']:.4f}% "
              f"disk_xcheck={c.get('disk_crosscheck_residual_pct')}")
    if summary.get("failures"):
        print("FAILURES:")
        for fail in summary["failures"]:
            print("  -", fail)
        return 1
    print("wa_check: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
