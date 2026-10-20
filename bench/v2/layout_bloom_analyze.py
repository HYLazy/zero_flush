#!/usr/bin/env python3
"""Analysis for the L0 overlap x Bloom-cost campaign (bench/LAYOUT_BLOOM_PLAN.md §5-§7).

Everything is computed **by run**: five independent runs per group, so the confidence
interval is over run means, never over the millions of requests inside a run (the plan
is explicit that requests are not independent repeats).

Reported per group: Bloom checks per Get, false positive rate (absent queries only),
CPU ns/Get, cycles/Get, instructions/Get, Get p50/p95/p99 from the cohort histogram of
the queried class, throughput, block reads, cache accesses, device reads.

Verdicts follow the plan's table, with the pre-registered 5% engineering threshold:
the wide/narrow *performance* ratio is only called an effect if its interval lies
outside 0.95-1.05; a wide interval is reported as unresolved.

Usage:
  layout_bloom_analyze.py --out DIR [--phase first] [--charts DIR]
"""
import argparse
import csv
import glob
import json
import os
import statistics
import sys

SMALL, MANTB = 256, 4
GROUP = 1 << MANTB
BASEBIT = 8
BUCKETS = SMALL + (64 - BASEBIT) * GROUP


def lower(i):
    if i < SMALL:
        return i
    oct_, mant = divmod(i - SMALL, GROUP)
    return mant << ((BASEBIT + oct_) - MANTB)


def hist_percentiles(path, ps=(0.5, 0.95, 0.99)):
    """Percentiles from a cohort histogram file, in ns (bucket lower bounds)."""
    if not os.path.exists(path):
        return {}
    counts = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            try:
                counts.append((int(float(r["ns"])), int(r["engine_count"])))
            except (KeyError, ValueError):
                continue
    if not counts:
        return {}
    total = sum(c for _, c in counts)
    if total <= 0:
        return {}
    counts.sort()
    out, acc, ti = {}, 0, 0
    for ns, c in counts:
        acc += c
        while ti < len(ps) and acc >= ps[ti] * total:
            out[f"p{int(ps[ti]*100)}"] = ns
            ti += 1
    return out


def ci95(vals):
    """Mean and half-width of the 95% CI over run means (t for small n)."""
    if not vals:
        return None, None, None
    n = len(vals)
    m = statistics.mean(vals)
    if n < 2:
        return m, None, None
    sd = statistics.stdev(vals)
    t = {2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306}.get(n, 1.96)
    hw = t * sd / (n ** 0.5)
    return m, hw, sd


def load_runs(lb):
    rows = []
    if not os.path.exists(os.path.join(lb, "manifest.jsonl")):
        return rows
    for line in open(os.path.join(lb, "manifest.jsonl")):
        line = line.strip()
        if not line:
            continue
        try:
            m = json.loads(line)
        except Exception:
            continue
        # validity lives in the run's gate output; the manifest is written before the
        # gate runs, so it cannot carry the verdict
        vpath = os.path.join(lb, "runs", m["run_id"] + ".validity.txt")
        if os.path.exists(vpath) and "VALID=1" not in open(vpath).read():
            continue
        p = os.path.join(lb, "runs", m["run_id"])
        M = json.load(open(p + ".measure_phase.json"))
        S = json.load(open(p + ".summary.json"))
        pw = M.get("perf_window") or {}
        dw = M.get("diag_window") or {}
        cls_key = {"hit": "get_hit", "missing_inrange": "get_miss_in",
                   "missing_outrange": "get_miss_out"}[m["read_class"]]
        d = dw.get(cls_key) or {}
        # class counts: the class is assigned by the lookup outcome, so an in-envelope
        # miss lands in get_miss_in either way -- check it matches the arm's intent
        pct = hist_percentiles(os.path.join(lb, "runs", f"{m['run_id']}.hist_cohort_{cls_key}.csv"))
        rows.append(dict(rid=m["run_id"], K=int(m["K"]), layout=m["layout"],
                         bits=int(m["bloom_bits"]), cls=m["read_class"],
                         perf_ops=pw.get("ops"), diag_ops=dw.get("ops"),
                         cpu_ns=pw.get("cpu_ns_per_get"), cycles=pw.get("cycles_per_get"),
                         insn=pw.get("instructions_per_get"),
                         insn_errno=pw.get("instructions_unavailable_errno"),
                         checks=d.get("bloom_checks_per_get"), fpr=d.get("false_positive_rate"),
                         pos=d.get("bloom_positive"), neg=d.get("bloom_negative"),
                         cache_hits=d.get("cache_hits_per_get"),
                         block_reads=d.get("block_reads_per_get"),
                         filter_reads=d.get("filter_block_reads_per_get"),
                         index_reads=d.get("index_block_reads_per_get"),
                         cls_ops=d.get("ops"), opss=S.get("throughput_ops_s"),
                         dev_read=S.get("device_read_bytes", 0), rss=S.get("db_dir_total_bytes", 0),
                         p50=pct.get("p50"), p95=pct.get("p95"), p99=pct.get("p99"),
                         censored=M.get("censored")))
    return rows


def group(rows):
    g = {}
    for r in rows:
        g.setdefault((r["K"], r["layout"], r["bits"], r["cls"]), []).append(r)
    return g


def fmt(v, nd=2):
    if v is None:
        return "-"
    return f"{v:.{nd}f}"


def verdict_ratio(ratio, lo, hi, checks_ratio):
    """The plan's conclusion table, applied mechanically."""
    if lo is None or hi is None:
        return "insufficient reps for an interval"
    if lo > 0.95 and hi < 1.05:
        return ("performance ratio interval lies entirely inside 0.95-1.05: under these "
                "conditions the effect is below the preset 5%")
    if lo > 1.0 or hi < 1.0:
        if checks_ratio and checks_ratio > 1.2:
            return ("checks AND performance both move: a measurable read-path cost; "
                    "profiling is needed to attribute the part that is Bloom itself")
        return "performance difference resolvable (checks unchanged)"
    return "interval spans 1.0: effect not resolved at this rep count"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="layout_bloom output root")
    ap.add_argument("--phase", default="first")
    ap.add_argument("--charts", default=None)
    ap.add_argument("--md", default=None)
    a = ap.parse_args()
    lb = a.out
    rows = load_runs(lb)
    if not rows:
        print("no valid runs found", file=sys.stderr)
        return 2
    groups = group(rows)

    md = ["# L0 overlap x Bloom cost — results", "",
          f"Source `{lb}`, phase `{a.phase}`. Percentiles are bucket lower bounds from "
          f"each run's cohort histogram; CIs are over the run means (5 runs per group).", ""]
    md += ["## Per-group metrics", "",
           "| K | layout | bits | query | runs | checks/Get | FPR | cpu ns/Get | cycles/Get | "
           "insn/Get | p50 µs | p95 µs | p99 µs | ops/s | cache hits/Get | block reads/Get |",
           "|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    table = {}
    for k in sorted(groups):
        rs = groups[k]
        K, layout, bits, cls = k
        m_checks = statistics.mean([r["checks"] for r in rs if r["checks"] is not None] or [0])
        fprs = [r["fpr"] for r in rs if r["fpr"] is not None]
        m_cpu = statistics.mean([r["cpu_ns"] for r in rs if r["cpu_ns"] is not None] or [0])
        m_cyc = statistics.mean([r["cycles"] for r in rs if r["cycles"] is not None] or [0])
        insns = [r["insn"] for r in rs if r["insn"] is not None]
        p50s = [r["p50"] / 1000 for r in rs if r["p50"]]
        p95s = [r["p95"] / 1000 for r in rs if r["p95"]]
        p99s = [r["p99"] / 1000 for r in rs if r["p99"]]
        m_p50 = statistics.mean(p50s) if p50s else None
        m_p95 = statistics.mean(p95s) if p95s else None
        m_p99 = statistics.mean(p99s) if p99s else None
        m_ops = statistics.mean([r["opss"] for r in rs if r["opss"]] or [0])
        m_ch = statistics.mean([r["cache_hits"] for r in rs if r["cache_hits"] is not None] or [0])
        m_br = statistics.mean([r["block_reads"] for r in rs if r["block_reads"] is not None] or [0])
        table[k] = dict(checks=m_checks, fpr=statistics.mean(fprs) if fprs else None,
                        cpu=m_cpu, cycles=m_cyc,
                        insn=statistics.mean(insns) if insns else None,
                        p50=m_p50, p95=m_p95, p99=m_p99, ops=m_ops, cache=m_ch, br=m_br,
                        n=len(rs))
        md.append(f"| {K} | {layout} | {bits} | {cls} | {len(rs)} | {fmt(m_checks,3)} | "
                  f"{fmt(table[k]['fpr'],4) if table[k]['fpr'] is not None else 'N/A'} | "
                  f"{fmt(m_cpu,1)} | {fmt(m_cyc,1)} | "
                  f"{fmt(table[k]['insn'],1) if table[k]['insn'] else 'n/a'} | "
                  f"{fmt(m_p50,2)} | {fmt(m_p95,2)} | {fmt(m_p99,2)} | {fmt(m_ops,0)} | "
                  f"{fmt(m_ch,3)} | {fmt(m_br,4)} |")
    md.append("")

    # ---- the two contrasts the plan cares about -------------------------------------
    md += ["## Pre-registered contrasts (by run, 95% CI)", ""]
    for cls in ("hit", "missing_inrange"):
        md.append(f"### {cls}: wide vs narrow, and Bloom off vs on (K=16, 8 GiB cache, 1 thread)")
        md.append("")
        md.append("| contrast | metric | A | B | ratio B/A | 95% CI | verdict |")
        md.append("|---|---|---:|---:|---:|---|---|")
        for bits in (0, 10):
            A = [(r["K"], r["layout"], r["bits"], r["cls"]) for r in rows]
            ga = [r for r in rows if (r["K"], r["layout"], r["bits"], r["cls"]) == (16, "narrow", bits, cls)]
            gb = [r for r in rows if (r["K"], r["layout"], r["bits"], r["cls"]) == (16, "wide", bits, cls)]
            if not ga or not gb:
                continue
            for metric, get, nd in (("checks/Get", lambda r: r["checks"], 3),
                                    ("cpu ns/Get", lambda r: r["cpu_ns"], 1),
                                    ("p99 µs", lambda r: (r["p99"] / 1000 if r["p99"] else None), 2),
                                    ("ops/s", lambda r: r["opss"], 0)):
                va = [get(r) for r in ga if get(r) is not None]
                vb = [get(r) for r in gb if get(r) is not None]
                if not va or not vb:
                    continue
                ma, ha, _ = ci95(va)
                mb, hb, _ = ci95(vb)
                ratio = (mb / ma) if ma else None
                # CI of the ratio from the two intervals (conservative linear form)
                lo = hi = None
                if ratio and ha is not None and hb is not None and ma:
                    lo = (mb - hb) / (ma + ha)
                    hi = (mb + hb) / (ma - ha) if (ma - ha) > 0 else None
                v = verdict_ratio(ratio, lo, hi, ma and (mb / ma if ma else None))
                md.append(f"| K=16 bits={bits} {cls} | {metric} | {fmt(ma,nd)} | {fmt(mb,nd)} | "
                          f"{fmt(ratio,3)} | "
                          f"{fmt(lo,3) if lo is not None else '?'}..{fmt(hi,3) if hi is not None else '?'} | {v} |")
        # Bloom off vs on, per layout
        for layout in ("narrow", "wide"):
            g0 = [r for r in rows if (r["K"], r["layout"], r["bits"], r["cls"]) == (16, layout, 0, cls)]
            g1 = [r for r in rows if (r["K"], r["layout"], r["bits"], r["cls"]) == (16, layout, 10, cls)]
            if not g0 or not g1:
                continue
            for metric, get, nd in (("cpu ns/Get", lambda r: r["cpu_ns"], 1),
                                    ("cache hits/Get", lambda r: r["cache_hits"], 3),
                                    ("p99 µs", lambda r: (r["p99"] / 1000 if r["p99"] else None), 2),
                                    ("ops/s", lambda r: r["opss"], 0)):
                v0 = [get(r) for r in g0 if get(r) is not None]
                v1 = [get(r) for r in g1 if get(r) is not None]
                if not v0 or not v1:
                    continue
                m0, h0, _ = ci95(v0)
                m1, h1, _ = ci95(v1)
                ratio = (m1 / m0) if m0 else None
                lo = hi = None
                if ratio and h0 is not None and h1 is not None and m0:
                    lo = (m1 - h1) / (m0 + h0)
                    hi = (m1 + h1) / (m0 - h0) if (m0 - h0) > 0 else None
                v = verdict_ratio(ratio, lo, hi, None)
                md.append(f"| K=16 {layout} {cls} | {metric} | {fmt(m0,nd)} | {fmt(m1,nd)} | "
                          f"{fmt(ratio,3)} | "
                          f"{fmt(lo,3) if lo is not None else '?'}..{fmt(hi,3) if hi is not None else '?'} | {v} |")
        md.append("")

    md += ["## Notes on what is and is not claimable", "",
           "* `instructions/Get` is `n/a` when the hardware counter could not be opened "
           "(`instructions_unavailable_errno`, 13 = EACCES from `perf_event_paranoid`). "
           "The plan allows this; claiming that the Bloom function itself is expensive "
           "needs separate CPU profiling, which this environment cannot provide.",
           "* `cpu_ns/Get` is thread CPU time over the window divided by the window's "
           "operations, so on a saturated single-threaded arm it equals wall time per Get; "
           "it becomes an independent quantity only when the arm is not CPU-bound.",
           "* Device reads are reported separately: on this host a 2 GiB L0-only database "
           "is fully cache-resident, so `block reads` here are cache misses, not device I/O.",
           "* The false positive rate is only defined for queries that do not exist; it is "
           "`N/A` for hit arms and for arms with the filter switched off.", ""]

    if a.charts:
        make_charts(rows, table, a.charts)
        md.append(f"Charts: `{a.charts}` (checks vs K, CPU vs checks, bits/key facets).")
        md.append("")

    text = "\n".join(md)
    if a.md:
        os.makedirs(os.path.dirname(os.path.abspath(a.md)), exist_ok=True)
        open(a.md, "w").write(text)
        print(f"wrote {a.md}")
    else:
        print(text)
    return 0


def make_charts(rows, table, outdir):
    """The three charts the plan prioritises, plus the raw CSV they are drawn from."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "layout_bloom_points.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["K", "layout", "bits", "query", "checks_per_get", "fpr", "cpu_ns_per_get",
                    "cycles_per_get", "p50_us", "p95_us", "p99_us", "ops_s", "cache_hits_per_get",
                    "block_reads_per_get", "run"])
        for k, rs in sorted(group(rows).items()):
            K, layout, bits, cls = k
            for r in rs:
                w.writerow([K, layout, bits, cls, r["checks"], r["fpr"], r["cpu_ns"], r["cycles"],
                            (r["p50"] or 0) / 1000, (r["p95"] or 0) / 1000, (r["p99"] or 0) / 1000,
                            r["opss"], r["cache_hits"], r["block_reads"], r["rid"]])

    # 1. checks vs K, by layout (in-envelope missing, 10 bits)
    ks = sorted({k[0] for k in table if k[3] == "missing_inrange" and k[2] == 10})
    plt.figure(figsize=(6, 4))
    for layout, mk in (("narrow", "o-"), ("wide", "s--")):
        ys = [table[(k, layout, 10, "missing_inrange")]["checks"] for k in ks
              if (k, layout, 10, "missing_inrange") in table]
        xs = [k for k in ks if (k, layout, 10, "missing_inrange") in table]
        if xs:
            plt.plot(xs, ys, mk, label=layout)
    plt.xlabel("K (L0 files)"); plt.ylabel("Bloom checks per Get")
    plt.title("In-envelope missing: checks vs K (10 bits/key)")
    plt.legend(); plt.grid(alpha=0.3); plt.tight_layout()
    plt.savefig(os.path.join(outdir, "chart_checks_vs_K.png"), dpi=150); plt.close()

    # 2. CPU cost vs checks per Get
    plt.figure(figsize=(6, 4))
    for bits, mk in ((0, "o"), (10, "s"), (16, "^")):
        xs, ys, lab = [], [], []
        for (K, layout, b, cls), v in sorted(table.items()):
            if b != bits or cls != "missing_inrange":
                continue
            xs.append(v["checks"]); ys.append(v["cpu"]); lab.append(f"K{K} {layout}")
        if xs:
            plt.scatter(xs, ys, marker=mk, label=f"{bits} bits/key")
            for x, y, l in zip(xs, ys, lab):
                plt.annotate(l, (x, y), fontsize=6, alpha=0.7)
    plt.xlabel("Bloom checks per Get"); plt.ylabel("CPU ns per Get")
    plt.title("Cost vs checks (in-envelope missing)")
    plt.legend(); plt.grid(alpha=0.3); plt.tight_layout()
    plt.savefig(os.path.join(outdir, "chart_cpu_vs_checks.png"), dpi=150); plt.close()

    # 3. facets by bits/key: FPR, block reads, CPU
    fig, axes = plt.subplots(1, 3, figsize=(12, 4))
    for ax, metric, ylab in ((axes[0], "fpr", "Bloom false positive rate"),
                             (axes[1], "br", "block reads per Get"),
                             (axes[2], "cpu", "CPU ns per Get")):
        labels, vals = [], []
        for (K, layout, bits, cls), v in sorted(table.items()):
            if cls != "missing_inrange":
                continue
            labels.append(f"K{K}\n{layout}\n{bits}b")
            vals.append(v[metric] if v[metric] is not None else 0.0)
        ax.bar(range(len(labels)), vals)
        ax.set_xticks(range(len(labels))); ax.set_xticklabels(labels, fontsize=6)
        ax.set_ylabel(ylab); ax.grid(alpha=0.3, axis="y")
    fig.suptitle("Facets by layout / K / bits (in-envelope missing)")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "chart_facets_bits.png"), dpi=150); plt.close()


if __name__ == "__main__":
    sys.exit(main())
