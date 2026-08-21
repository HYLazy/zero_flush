#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""smoke_check.py - 冒烟阶段解析链验证。

用法: smoke_check.py <out_dir> <writes>
从 manifest_entries.jsonl 取 smoke_ 开头的条目, 聚合 + WA 校验,
任一环节失败则以非零码退出。
"""

import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
import parse_stats  # noqa: E402
import wa_check  # noqa: E402


def main():
    out_dir = sys.argv[1]
    writes = int(sys.argv[2])
    entries_path = os.path.join(out_dir, "manifest_entries.jsonl")
    entries = []
    with open(entries_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                e = json.loads(line)
                if e["combo"].startswith("smoke_"):
                    entries.append(e)
    if not entries:
        print("no smoke entries found")
        return 1

    combos = {}
    for e in entries:
        combos.setdefault(e["combo"], []).append(e["run_dir"])

    manifest = []
    for combo, runs in combos.items():
        with open(os.path.join(runs[0], "meta.json")) as f:
            meta = json.load(f)
        manifest.append({
            "combo": combo, "benchmark": meta["benchmark"],
            "value_size": meta["value_size"],
            "distribution": meta["distribution"], "runs": runs,
        })

    aggs = parse_stats.aggregate(manifest)
    for a in aggs:
        a["writes"] = writes
        if a["n_ok"] != a["n_runs"]:
            print(f"smoke combo {a['combo']} 解析失败 "
                  f"({a['n_ok']}/{a['n_runs']})")
            return 1
        print(f"smoke {a['combo']}: wall={a['wall_time_sec']:.2f}s "
              f"wal={a['wal_bytes']:.0f} flush={a['flush_bytes']:.0f} "
              f"compact_w={a['compact_write_bytes']:.0f} "
              f"stall_micros={a['stall_micros']}")

    checks = [wa_check.check_combo(a, writes=writes) for a in aggs]
    for c in checks:
        if "error" in c:
            print(f"smoke WA 校验失败: {c['combo']}: {c['error']}")
            return 1
        print(f"smoke WA {c['combo']}: wa_wal={c['wa_wal']:.3f} "
              f"wa_flush={c['wa_flush']:.3f} wa_comp={c['wa_compaction']:.3f} "
              f"disk_xcheck={c.get('disk_crosscheck_residual_pct')}")
    print("smoke_check: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
