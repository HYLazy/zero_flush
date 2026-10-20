#!/usr/bin/env python3
"""Cross-check one arm between two backlog roots (the K-scan replication check).

The K scan's K=16 / B=2 GiB arms repeat arms that the first phase already ran. The two
sets differ in exactly two ways worth naming:

  * the base instance: the first phase ran on the 03:57 base, the K scan on the 20:30
    base. Both were built by the same tool revision with the same seed and parameters,
    but a fill's flush boundaries are not deterministic, so the two bases are the same
    data in a slightly different file layout (755 vs 777 SSTs out of ~750).
  * the tool revision: the first phase started before the 17:29 rebuild (which added two
    optional, defaulted-off flags) and finished after it.

So this is not a controlled A/B. It is a replication check: the same nominal arm, twice,
whose agreement bounds how much the "same" experiment moves on this host.

Usage:
  l0_kscan_replicate.py --a DIR --b DIR [--kind A|B|C] [--value 2147483648] [--k 16]
"""
import argparse
import importlib.util
import os
import statistics
import sys


def load_analyzer():
    here = os.path.dirname(os.path.abspath(__file__))
    spec = importlib.util.spec_from_file_location("lba", os.path.join(here, "l0_backlog_analyze.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def med(rs, f):
    v = [f(r) for r in rs if f(r) is not None]
    return statistics.median(v) if v else None


def chk(r):
    return (r["classes"].get("missing_in_range") or {}).get("bloom_checks_per_get")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True, help="first-phase root")
    ap.add_argument("--b", required=True, help="K-scan root")
    ap.add_argument("--value", type=int, default=2147483648)
    ap.add_argument("--k", type=int, default=16)
    ap.add_argument("--md", default=None)
    a = ap.parse_args()
    m = load_analyzer()

    out = ["# K=16 / B=2 GiB replication check", "",
           f"first phase `{a.a}` vs K scan `{a.b}`, arms with B={a.value} and K={a.k}.",
           "The two are not a controlled A/B (different base instance, different tool "
           "revision); they are the same nominal arm run twice, which is what bounds the "
           "run-to-run movement of this experiment.", ""]
    lines = []
    for kind in ("A", "B", "C"):
        for layout in ("narrow", "wide"):
            ra = [r for r in m.run_records(a.a) if r["valid"] and r["kind"] == kind
                  and r["layout"] == layout and r["B"] == a.value and r["K"] == a.k]
            rb = [r for r in m.run_records(a.b) if r["valid"] and r["kind"] == kind
                  and r["layout"] == layout and r["B"] == a.value and r["K"] == a.k]
            if not (ra and rb):
                continue
            rows = []
            sa, sb = med(ra, lambda r: r["svc"]), med(rb, lambda r: r["svc"])
            rows.append(("total ops/s", sa, sb, 0))
            rows.append(("CPU ns/Get", med(ra, lambda r: r["cpu_ns"]), med(rb, lambda r: r["cpu_ns"]), 1))
            rows.append(("checks/Get (miss-in)", med(ra, chk), med(rb, chk), 3))
            rows.append(("drain s", med(ra, lambda r: r["drain_s"]), med(rb, lambda r: r["drain_s"]), 0))
            rows.append(("peak GiB", med(ra, lambda r: r["peak_bytes"]), med(rb, lambda r: r["peak_bytes"]), 1))
            rows.append(("written GiB", med(ra, lambda r: m.level_sum(r, "write")),
                         med(rb, lambda r: m.level_sum(r, "write")), 1))
            for label, va, vb, nd in rows:
                if va is None and vb is None:
                    continue
                scale = 2**30 if label.endswith("GiB") else 1.0
                va = va / scale if va is not None else None
                vb = vb / scale if vb is not None else None
                if va and vb:
                    pct = 100.0 * (vb - va) / va
                    lines.append(f"| {kind} | {layout} | {label} | {va:.{nd}f} | {vb:.{nd}f} | {pct:+.1f}% |")
                else:
                    lines.append(f"| {kind} | {layout} | {label} | - | - | - |")
            # the per-run spread on each side, so a single number cannot hide a bimodal set
            for label, f, nd in (("ops/s", lambda r: r["svc"], 0),
                                 ("drain s", lambda r: r["drain_s"], 0)):
                va = [f(r) for r in ra if f(r) is not None]
                vb = [f(r) for r in rb if f(r) is not None]
                if va and vb:
                    lines.append(f"| {kind} | {layout} |   {label} range | "
                                 f"{min(va):.{nd}f}..{max(va):.{nd}f} (n={len(va)}) | "
                                 f"{min(vb):.{nd}f}..{max(vb):.{nd}f} (n={len(vb)}) | |")
    out += ["| kind | layout | metric | first phase | K scan | change |",
            "|---|---|---|---:|---:|---:|"] + lines
    text = "\n".join(out) + "\n"
    if a.md:
        os.makedirs(os.path.dirname(os.path.abspath(a.md)), exist_ok=True)
        open(a.md, "w").write(text)
        print(f"wrote {a.md}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
