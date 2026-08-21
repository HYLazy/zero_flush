#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""build_manifest.py - 由 manifest_entries.jsonl 生成聚合 manifest.json。

用法: build_manifest.py <entries.jsonl> <manifest.json>
排除 smoke_ 与 fallback 前缀的冒烟条目 (fallback_rww 若存在则保留,
由 make_report 单独处理)。
"""

import json
import os
import sys


def main():
    entries_path, out_path = sys.argv[1], sys.argv[2]
    entries = []
    with open(entries_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))

    combos = {}
    order = []
    for e in entries:
        c = e["combo"]
        if c.startswith("smoke_"):
            continue
        if c not in combos:
            combos[c] = []
            order.append(c)
        combos[c].append(e["run_dir"])

    manifest = []
    for c in order:
        runs = combos[c]
        with open(os.path.join(runs[0], "meta.json")) as f:
            meta = json.load(f)
        manifest.append({
            "combo": c,
            "benchmark": meta["benchmark"],
            "value_size": meta["value_size"],
            "distribution": meta["distribution"],
            "runs": runs,
        })

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    print(f"manifest: {len(manifest)} combos, "
          f"{sum(len(m['runs']) for m in manifest)} runs -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
