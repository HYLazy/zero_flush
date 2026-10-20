#!/usr/bin/env python3
"""从 db_bench fill.log / read.log 提取吞吐与延迟统计。

用法: python3 b4b17b3_stats.py <fill.log> [read.log ...]
输出: 每个文件一行汇总（吞吐、平均延迟、P50/P75/P99/P99.9/P99.99），
      并在末尾给出各文件延迟直方图的慢尾（最慢 5 个非空桶）。
"""
import re
import sys
import pathlib

SUMMARY = re.compile(
    r"^(?P<bench>fillrandom|readrandom|overwrite|readwhilewriting)\s*:\s*"
    r"(?P<us>[\d.]+)\s+micros/op\s+"
    r"(?P<ops>[\d,]+)\s+ops/sec\s+"
    r"(?P<sec>[\d.]+)\s+seconds\s+"
    r"(?P<n>\d+)\s+operations;\s+"
    r"(?P<mbs>[\d.]+)\s+MB/s(?P<found>\s*\(.*?\))?",
    re.M,
)
PCTL = re.compile(r"^Percentiles:\s*(?P<body>.+)$", re.M)
BUCKET = re.compile(r"^\(\s*(?P<lo>\d+),\s*(?P<hi>\d+)\s*\]\s+(?P<cnt>\d+)\s", re.M)


def parse(path):
    txt = pathlib.Path(path).read_text(errors="replace")
    rec = {"file": str(path)}
    m = SUMMARY.search(txt)
    if m:
        rec.update(
            bench=m.group("bench"),
            us_per_op=float(m.group("us")),
            ops_per_sec=int(m.group("ops").replace(",", "")),
            seconds=float(m.group("sec")),
            operations=int(m.group("n")),
            mb_per_s=float(m.group("mbs")),
            found=(m.group("found") or "").strip() or None,
        )
    p = PCTL.search(txt)
    if p:
        for k, v in re.findall(r"(P[\d.]+):\s*([\d.]+)", p.group("body")):
            rec["pctl_" + k] = float(v)
    rec["buckets"] = [(int(a), int(b), int(c)) for a, b, c in BUCKET.findall(txt)]
    return rec


def main(paths):
    rows = [parse(p) for p in paths]
    print("=" * 100)
    for r in rows:
        if "bench" not in r:
            print(f"{r['file']}: 无汇总行（未完成 / 无 db_bench 输出）")
            continue
        print(f"{r['file']}")
        print(
            f"  {r['bench']:<12} 吞吐={r['ops_per_sec']:>10,} ops/s  "
            f"平均延迟={r['us_per_op']:>9.3f} µs/op  "
            f"带宽={r['mb_per_s']:>7.1f} MB/s  用时={r['seconds']:.1f}s  "
            f"操作数={r['operations']:,}"
        )
        if r.get("found"):
            print(f"  命中: {r['found']}")
        pk = [(k, r[k]) for k in ("pctl_P50", "pctl_P75", "pctl_P99", "pctl_P99.9", "pctl_P99.99") if k in r]
        if pk:
            print("  延迟分位(µs): " + "  ".join(f"{k.replace('pctl_', '')}={v:g}" for k, v in pk))
        if r["buckets"]:
            total = sum(c for _, _, c in r["buckets"]) or 1
            slow = sorted(r["buckets"], key=lambda t: t[0])[-5:]
            print("  最慢桶(µs, 计数, 占比): " + "  ".join(f"[{lo},{hi}]={c}({100*c/total:.4f}%)" for lo, hi, c in slow))
    print("=" * 100)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    main(sys.argv[1:])
