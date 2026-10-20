#!/usr/bin/env python3
r"""Group a perf_v2 analysis report's arm table by an axis (a run-id regex or a table column).

The M1-M5 reports are per-arm tables; the questions the runbook asks ("does the write
path kneel at this offered load", "does the block cache move point reads") are contrasts
*between* arms. This reads one of those tables and prints, per axis value, the median and
the full range of a chosen metric, with the unresolved-effect rule applied: a difference
is only an effect when it is at least 1.5x AND larger than the between-rep spread.

Usage:
  matrix_synthesis.py --report out/perf_v2/m2/REPORT.md --metric ops_s \
      --axis 'm2_(deep|mixed|negative|recent)_t(\d+)_c(\d+)' --axis-name family,threads,cache
"""
import argparse
import collections
import re
import statistics
import sys

AXES = ("family", "threads", "cache", "frac", "read_pct", "K", "layout", "mode", "phase", "rep")


def parse_table(path):
    rows = []
    header = None
    for line in open(path, errors="replace"):
        line = line.rstrip("\n")
        if not line.startswith("|"):
            header = None
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if header is None:
            header = cells
            continue
        if set("".join(cells)) <= set("-: "):
            continue
        if cells[:1] and cells[0].startswith("m") and "_" in cells[0]:
            rows.append(dict(zip(header, cells)))
    return rows


def num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True)
    ap.add_argument("--metric", required=True)
    ap.add_argument("--axis", default=None, help="regex with one group per axis name")
    ap.add_argument("--group-by", default=None,
                    help="comma-separated table columns to group by, instead of --axis")
    ap.add_argument("--axis-name", default="family")
    ap.add_argument("--secondary", default=None)
    ap.add_argument("--min-rows", type=int, default=1)
    ap.add_argument("--only-rid", default=None, help="keep only rows whose rid matches")
    a = ap.parse_args()
    names = [n.strip() for n in a.axis_name.split(",")] if not a.group_by else \
        [n.strip() for n in a.group_by.split(",")]
    rows = parse_table(a.report)
    if a.only_rid:
        import re as _re
        rows = [r for r in rows if _re.search(a.only_rid, r.get("rid", ""))]
    if not rows:
        print("no arm rows parsed", file=sys.stderr)
        return 2
    groups = collections.defaultdict(list)
    for r in rows:
        if a.group_by:
            key = tuple(r.get(n, "?") for n in names)
        else:
            rid = r.get("rid", "")
            m = re.search(a.axis, rid)
            if not m:
                continue
            key = tuple(g if g is not None else "?" for g in m.groups())[:len(names)]
        groups[key].append(r)
    print(f"{a.report}: {len(rows)} arm rows, {len(groups)} groups by {names}")
    print()
    hdr = " | ".join(names) + f" | n | {a.metric} median | min..max | spread | effect |"
    print("| " + hdr)
    print("|" + "---|" * (len(names) + 5))
    medians = {}
    for key in sorted(groups):
        rs = groups[key]
        vals = [num(r.get(a.metric, "")) for r in rs]
        vals = [v for v in vals if v is not None]
        if not vals:
            continue
        med = statistics.median(vals)
        lo, hi = min(vals), max(vals)
        spread = (hi / lo) if lo and lo > 0 else float("inf")
        medians[key] = med
        # inside-bucket spread is the noise floor: an effect must exceed it
        within = spread
        print(f"| " + " | ".join(key) + f" | {len(vals)} | {med:.2f} | {lo:.2f}..{hi:.2f} | "
              f"{within:.2f}x | |")
    if medians:
        top = max(medians.values())
        bot = min(medians.values())
        ratio = (top / bot) if bot > 0 else float("inf")
        within_max = 0.0
        for key, med in medians.items():
            vals = [num(r.get(a.metric, "")) for r in groups[key]]
            vals = [v for v in vals if v is not None]
            if len(vals) > 1 and min(vals) > 0:
                within_max = max(within_max, max(vals) / min(vals))
        verdict = ("EFFECT" if (ratio >= 1.5 and ratio > within_max) else "unresolved")
        print()
        print(f"across-axis spread: {bot:.2f} .. {top:.2f} = {ratio:.2f}x; "
              f"largest within-bucket spread {within_max:.2f}x -> {verdict}")
    if a.secondary:
        print()
        print(f"secondary metric {a.secondary}:")
        for key in sorted(groups):
            vals = [num(r.get(a.secondary, "")) for r in groups[key]]
            vals = [v for v in vals if v is not None]
            if vals:
                print(f"  {'/'.join(key)}: median {statistics.median(vals):.2f} "
                      f"({min(vals):.2f}..{max(vals):.2f})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
