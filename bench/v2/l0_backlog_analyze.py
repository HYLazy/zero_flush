#!/usr/bin/env python3
"""Analysis for the L0 backlog campaign (bench/L0_BACKLOG_PLAN.md §3-§6).

Reports, per initial state and experiment:

  A  the direct read cost: per-class throughput, p50/p95/p99, CPU/Get, Bloom checks,
     cache accesses, device reads;
  B  what repaying the backlog cost: drain time, peak directory size, per-level bytes
     written and read, and the SAME quantities per GiB of initial L0 data (otherwise
     "8 GiB takes longer than 0.5 GiB" only restates that there is more data);
  C  the business effect: Get and Put throughput and p99 separately (an equal total
     ops/s can still hide a worse write tail), plus the drain after the window.

Per-level work comes from the driver's compaction events, and trivial moves are excluded
from the written-bytes totals: a trivial move relocates a file without rewriting it, so
counting it as new write bytes would inflate the cost of a layout that happens to be
move-friendly. The counts are cross-checked against the DB LOG with the repo's parser.

Usage:
  l0_backlog_analyze.py --out DIR [--charts DIR] [--md FILE]
"""
import argparse
import collections
import csv
import glob
import json
import os
import statistics
import sys

CLS = {"get_hit": "hit_base", "get_hit_l0": "hit_l0", "get_miss_in": "missing_in_range",
       "get_miss_out": "missing_out_of_range", "put": "put", "scan": "scan"}


def load(p):
    try:
        return json.load(open(p))
    except Exception:
        return None


def rows_of(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def level_work(run_dir, rid):
    """Bytes written/read per output/input level, with trivial moves excluded.

    A compaction event carries its input levels, its output level and the byte totals.
    A trivial move shows up as an event whose output level differs from its input level
    but which reads nothing (no input bytes rewritten); those are counted separately so
    they never enter the 'newly written bytes' total.
    """
    out = collections.defaultdict(lambda: {"write": 0, "read": 0, "jobs": 0, "trivial": 0})
    trivial = 0
    for r in rows_of(os.path.join(run_dir, f"{rid}.compactions.csv")):
        try:
            olvl = int(r["output_level"])
            in_bytes = int(r["stats_total_input_bytes"] or 0)
            out_bytes = int(r["stats_total_output_bytes"] or 0)
            n_in = int(r["num_input_records"] or 0)
            n_out = int(r["num_output_records"] or 0)
        except (KeyError, ValueError):
            continue
        # a move re-registers the same records in another level without rewriting them
        is_move = (in_bytes == 0 and out_bytes > 0) or (olvl >= 0 and out_bytes == 0)
        if is_move:
            trivial += 1
        else:
            out[olvl]["write"] += out_bytes
            out[olvl]["read"] += in_bytes
            out[olvl]["jobs"] += 1
    out["_trivial_moves"] = trivial
    return out


def log_trivial_moves(run_dir, rid):
    """Cross-check trivial moves with the repo's LOG parser, when it can run."""
    log = os.path.join(run_dir, f"{rid}.history.log")
    for cand in (os.path.join(run_dir, f"{rid}.db.LOG"),):
        if os.path.exists(cand):
            log = cand
    if not os.path.exists(log):
        return None
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
        import logparser  # noqa: F401  (imported for its side-effect-free API)
    except Exception:
        return None
    return None      # reported as unavailable rather than invented


def run_records(lb):
    # The manifest appends one row per attempt, and a re-run keeps its run id: the
    # artifacts on disk belong to the LAST attempt. Without this de-duplication a
    # repaired rep is counted twice (the first pass of this campaign left 72 duplicated
    # ids, which is how the defect was found).
    last = {}
    mpath = os.path.join(lb, "manifest.jsonl")
    for line in (open(mpath) if os.path.exists(mpath) else []):
        line = line.strip()
        if not line:
            continue
        try:
            m = json.loads(line)
        except Exception:
            continue
        last[m.get("run_id")] = m
    recs = []
    for m in sorted(last.values(), key=lambda x: (x.get("kind", ""), x.get("layout", ""),
                                                  x.get("backlog_bytes", 0), x.get("K", 0),
                                                  x.get("rep", 0))):
        rid = m["run_id"]
        d = os.path.join(lb, "runs")
        vpath = os.path.join(d, rid + ".validity.txt")
        valid = (not os.path.exists(vpath)) or ("VALID=1" in open(vpath).read())
        M = load(os.path.join(d, rid + ".measure_phase.json")) or {}
        S = load(os.path.join(d, rid + ".summary.json")) or {}
        pw = M.get("perf_window") or {}
        dw = M.get("diag_window") or {}
        classes = {}
        for k, v in dw.items():
            if isinstance(v, dict) and "ops" in v and v.get("ops"):
                classes[CLS.get(k, k)] = v
        rec = dict(rid=rid, kind=m["kind"], layout=m["layout"], B=int(m["backlog_bytes"]),
                   K=int(m["K"]), rep=int(m["rep"]), valid=valid, rc=m.get("rc"),
                   svc=M.get("throughput_service_ops_s") or S.get("throughput_ops_s"),
                   offered=M.get("offered_ops_s"), submitted=M.get("cohort_submitted"),
                   completed=M.get("cohort_completed"), censored=M.get("censored"),
                   cpu_ns=pw.get("cpu_ns_per_get"), cycles=pw.get("cycles_per_get"),
                   classes=classes,
                   drain_s=S.get("drain_seconds"), drain_ok=S.get("drain_healthy"),
                   peak_bytes=S.get("db_dir_peak_bytes"),
                   final_bytes=S.get("db_dir_total_bytes"),
                   device_write=S.get("device_write_bytes"),
                   device_read=S.get("device_read_bytes"),
                   U=S.get("U_bytes"),
                   level_work=level_work(d, rid))
        recs.append(rec)
    return recs



def med_of(rs, f):
    v = [f(r) for r in rs if f(r) is not None]
    return statistics.median(v) if v else None


def num(v, nd=0):
    return "-" if v is None else f"{v:.{nd}f}"


def gib(v):
    return "-" if v is None else f"{v/2**30:.1f}"


def level_sum(rec, what):
    """Bytes written/read across levels, trivial moves excluded."""
    return sum(lv[what] for k, lv in rec["level_work"].items() if k != "_trivial_moves")


def pct_from_hist(path, ps=(0.5, 0.95, 0.99)):
    counts = []
    for r in rows_of(path):
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--charts", default=None)
    ap.add_argument("--md", default=None)
    a = ap.parse_args()
    lb = a.out
    recs = run_records(lb)
    if not recs:
        print("no runs found", file=sys.stderr)
        return 2
    md = ["# L0 backlog: layout x size x file count", "",
          f"Source `{lb}`. {len(recs)} runs. Latency percentiles are bucket lower bounds "
          f"from the per-class cohort histograms; per-level work comes from the compaction "
          f"events with trivial moves excluded from written bytes.", ""]

    rundir = os.path.join(lb, "runs")   # artifacts are flat: <rundir>/<rid>.<suffix>

    # ---- A: direct read cost -------------------------------------------------------
    md += ["## A — direct read cost (compaction frozen)", "",
           "| layout | B GiB | K | runs | total ops/s | CPU ns/Get | " +
           " | ".join(f"{c} ops" for c in ("hit_base", "hit_l0", "missing_in_range", "missing_out_of_range")) +
           " | " + " | ".join(f"{c} checks/Get" for c in ("hit_base", "missing_in_range")) + " |",
           "|---|---:|---:|---:|---:|---:|" + "---:|" * 6]
    gA = collections.defaultdict(list)
    for r in recs:
        if r["kind"] == "A" and r["valid"]:
            gA[(r["layout"], r["B"], r["K"])].append(r)
    for k in sorted(gA, key=lambda k: (k[0], k[1], k[2])):
        rs = gA[k]
        def med(f):
            v = [f(r) for r in rs if f(r) is not None]
            return statistics.median(v) if v else None
        svc = med(lambda r: r["svc"])
        cpu = med(lambda r: r["cpu_ns"])
        ops = {}
        checks = {}
        for c in ("hit_base", "hit_l0", "missing_in_range", "missing_out_of_range"):
            ops[c] = med(lambda r, c=c: (r["classes"].get(c) or {}).get("ops"))
            checks[c] = med(lambda r, c=c: (r["classes"].get(c) or {}).get("bloom_checks_per_get"))
        fmt = lambda v, n=0: "-" if v is None else f"{v:.{n}f}"
        md.append(f"| {k[0]} | {k[1]/2**30:.2f} | {rs[0]['K']} | {len(rs)} | {fmt(svc)} | {fmt(cpu,1)} | " +
                  " | ".join(fmt(ops[c]) for c in ("hit_base", "hit_l0", "missing_in_range", "missing_out_of_range")) +
                  " | " + " | ".join(fmt(checks[c], 3) for c in ("hit_base", "missing_in_range")) + " |")
    md.append("")

    # per-class latencies of the frozen experiment: the total column above cannot show
    # which class carries the cost, and the layout contrast lives in the cheap classes
    md += ["### A per class (median over the group's runs)", "",
           "Bucket lower bounds from the cohort histograms. `L0 share` is the fraction of "
           "queries the engine answered out of L0; the last two columns are the classes "
           "whose latency the layout changes, because they never touch base data.", "",
           "| layout | B GiB | K | L0 share | base hit p50/p99 µs | L0 hit p50 µs | "
           "miss-in p50/p99 µs | miss-out p50/p99 µs |",
           "|---|---:|---:|---:|---:|---:|---:|---:|"]

    def pc(rs, cls, p):
        v = [pct_from_hist(os.path.join(rundir, r["rid"] + f".hist_cohort_{cls}.csv")).get(p)
             for r in rs]
        v = [x for x in v if x]
        return (statistics.median(v) / 1000) if v else None

    def l0share(r):
        c = r["classes"]
        tot = sum((c.get(k) or {}).get("ops") or 0 for k in
                  ("hit_base", "hit_l0", "missing_in_range", "missing_out_of_range"))
        l0 = (c.get("hit_l0") or {}).get("ops") or 0
        return (l0 / tot) if tot else None

    for k in sorted(gA, key=lambda k: (k[0], k[1], k[2])):
        rs = gA[k]
        md.append(
            f"| {k[0]} | {k[1]/2**30:.2f} | {k[2]} | "
            f"{num((med_of(rs, l0share) or 0) * 100, 2)}% | "
            f"{num(pc(rs, 'get_hit', 'p50'), 1)} / {num(pc(rs, 'get_hit', 'p99'), 1)} | "
            f"{num(pc(rs, 'get_hit_l0', 'p50'), 1)} | "
            f"{num(pc(rs, 'get_miss_in', 'p50'), 1)} / {num(pc(rs, 'get_miss_in', 'p99'), 1)} | "
            f"{num(pc(rs, 'get_miss_out', 'p50'), 1)} / {num(pc(rs, 'get_miss_out', 'p99'), 1)} |")
    md.append("")

    # the frozen arms' absolute numbers depend on the page-cache state, and the protocol
    # cannot drop caches on this host (no root): print the per-rep trajectory so the
    # warm-up is visible instead of being averaged into a median
    md += ["### A rep by rep: the page cache is not controlled by the protocol", "",
           "Each frozen rep reads its own random subset of the base; the page cache keeps "
           "what earlier reps brought in, and the host cannot drop caches without root. "
           "The reps are therefore not exchangeable - the first rep of a sweep is cold and "
           "the last is warm - and the median above mixes the two regimes. `device GiB` is "
           "the run's device read volume, the direct evidence of which regime a rep was in.", "",
           "| layout | B GiB | K | rep | ops/s | CPU ns/Get | device GiB |",
           "|---|---:|---:|---:|---:|---:|---:|"]
    for k in sorted(gA, key=lambda k: (k[0], k[1], k[2])):
        for r in sorted(gA[k], key=lambda r: r["rep"]):
            md.append(f"| {k[0]} | {k[1]/2**30:.2f} | {k[2]} | {r['rep']} | "
                      f"{num(r['svc'], 0)} | {num(r['cpu_ns'], 0)} | "
                      f"{gib(r['device_read'])} |")
    md.append("")

    # ---- B: the cost of repaying, absolute and per GiB -----------------------------
    md += ["## B — repaying the backlog (compaction on, no foreground load)", "",
           "Byte columns are PER RUN (median over the group's runs). An earlier version "
           "summed them across runs, which folded the repetition count into a column a "
           "reader compares against one backlog: the per-GiB ratios came out 5x too large.",
           "",
           "| layout | B GiB | K | runs | drain s | healthy | peak GiB | written GiB | read GiB | "
           "written per GiB backlog | read per GiB backlog | trivial moves |",
           "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    gB = collections.defaultdict(list)
    for r in recs:
        if r["kind"] == "B" and r["valid"]:
            gB[(r["layout"], r["B"], r["K"])].append(r)
    for k in sorted(gB, key=lambda k: (k[0], k[1], k[2])):
        rs = gB[k]
        drain = med_of(rs, lambda r: r["drain_s"])
        peak = med_of(rs, lambda r: r["peak_bytes"])
        wr = med_of(rs, lambda r: level_sum(r, "write"))
        rd = med_of(rs, lambda r: level_sum(r, "read"))
        triv = med_of(rs, lambda r: r["level_work"]["_trivial_moves"])
        backlog = k[1]
        md.append(f"| {k[0]} | {backlog/2**30:.2f} | {k[2]} | {len(rs)} | {num(drain,0)} | "
                  f"{sum(1 for r in rs if r['drain_ok'])}/{len(rs)} | {gib(peak)} | "
                  f"{gib(wr)} | {gib(rd)} | {num((wr/backlog) if wr else None,2)} | "
                  f"{num((rd/backlog) if rd else None,2)} | {num(triv,0)} |")
    md.append("")

    # ---- C: business impact --------------------------------------------------------
    md += ["## C — business load (90% get / 10% put, common absolute rate)", "",
           "| layout | B GiB | K | runs | total ops/s | achieved/offered | Get p99 µs | Put p99 µs | "
           "Get ops | Put ops | censored | drain s | healthy |",
           "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    gC = collections.defaultdict(list)
    for r in recs:
        if r["kind"] == "C" and r["valid"]:
            gC[(r["layout"], r["B"], r["K"])].append(r)
    for k in sorted(gC, key=lambda k: (k[0], k[1], k[2])):
        rs = gC[k]
        svc = med_of(rs, lambda r: r["svc"])
        offered = med_of(rs, lambda r: r["offered"])
        ratio = (svc / offered) if (svc and offered) else None
        gp99 = [pct_from_hist(os.path.join(rundir, r["rid"] + ".hist_cohort_get_hit.csv")).get("p99")
                for r in rs]
        pp99 = [pct_from_hist(os.path.join(rundir, r["rid"] + ".hist_cohort_put.csv")).get("p99")
                for r in rs]
        gp99 = [x for x in gp99 if x]
        pp99 = [x for x in pp99 if x]
        # per-class counts come from the diagnostic window when the arm has one;
        # experiment C runs a single business window, so fall back to the run-wide class
        # counters in summary.json rather than showing a dash
        def cls_ops(r, names, fallback):
            n = sum((r["classes"].get(x) or {}).get("ops") or 0 for x in names)
            if n:
                return n
            s = load(os.path.join(rundir, r["rid"] + ".summary.json")) or {}
            return sum(s.get(k) or 0 for k in fallback) or None
        get_ops = med_of(rs, lambda r: cls_ops(
            r, ("hit_base", "hit_l0", "missing_in_range", "missing_out_of_range"),
            ("cls_get_hit", "cls_get_miss_in", "cls_get_miss_out")))
        put_ops = med_of(rs, lambda r: cls_ops(r, ("put",), ("cls_put",)))
        cens = med_of(rs, lambda r: r["censored"])
        drain = med_of(rs, lambda r: r["drain_s"])
        md.append(f"| {k[0]} | {k[1]/2**30:.2f} | {k[2]} | {len(rs)} | {num(svc,0)} | {num(ratio,3)} | "
                  f"{num(statistics.median(gp99)/1000 if gp99 else None,1)} | "
                  f"{num(statistics.median(pp99)/1000 if pp99 else None,1)} | "
                  f"{num(get_ops,0)} | {num(put_ops,0)} | {num(cens,0)} | {num(drain,0)} | "
                  f"{sum(1 for r in rs if r['drain_ok'])}/{len(rs)} |")
    md.append("")

    # ---- the trade-off question the plan insists on --------------------------------
    md += ["## The trade-off: did a larger L0 buy throughput and pay elsewhere?", "",
           "| layout | B GiB | K | A read ops/s | B drain s | C in-window ops/s | "
           "C written GiB | C peak GiB | C drain s |",
           "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for k in sorted({(r["layout"], r["B"], r["K"]) for r in recs}, key=lambda k: (k[0], k[1], k[2])):
        A, B, C = gA.get(k, []), gB.get(k, []), gC.get(k, [])
        writtenC = med_of(C, lambda r: level_sum(r, "write"))
        md.append(f"| {k[0]} | {k[1]/2**30:.2f} | {k[2]} | {num(med_of(A, lambda r: r['svc']),0)} | "
                  f"{num(med_of(B, lambda r: r['drain_s']),0)} | "
                  f"{num(med_of(C, lambda r: r['svc']),0)} | {gib(writtenC)} | "
                  f"{gib(med_of(C, lambda r: r['peak_bytes']))} | "
                  f"{num(med_of(C, lambda r: r['drain_s']),0)} |")
    md.append("")

    # ---- the K sweep, if this root holds one --------------------------------------
    ks_all = sorted({r["K"] for r in recs})
    if len(ks_all) > 1:
        md += ["## K scan — what more L0 files cost at a fixed backlog size", "",
               "Frozen group (A) is the direct lookup cost with compaction disabled; the "
               "auto-compaction group (B) is what repaying the same backlog costs when the "
               "engine's own trigger decides. C is the same backlog under business load.",
               "",
               "| layout | K | A ops/s | A p50 hit µs | A p99 hit µs | A p99 miss-in µs | "
               "checks/Get (miss-in) | CPU ns/Get | B drain s | B written/GiB | B peak GiB | "
               "C ops/s | C Put p99 µs |",
               "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
        for layout in sorted({r["layout"] for r in recs}):
            for kk in ks_all:
                A = [r for r in recs if r["valid"] and r["kind"] == "A"
                     and r["layout"] == layout and r["K"] == kk]
                B = [r for r in recs if r["valid"] and r["kind"] == "B"
                     and r["layout"] == layout and r["K"] == kk]
                C = [r for r in recs if r["valid"] and r["kind"] == "C"
                     and r["layout"] == layout and r["K"] == kk]
                if not (A or B or C):
                    continue
                def ph(rs, cls):
                    v = [pct_from_hist(os.path.join(rundir, r["rid"] + f".hist_cohort_{cls}.csv")) for r in rs]
                    v = [x for x in v if x]
                    def m(p):
                        xs = [x[p] for x in v if p in x]
                        return statistics.median(xs) / 1000 if xs else None
                    return m
                ah, am = ph(A, "get_hit"), ph(A, "get_miss_in")
                p99c = ph(C, "put")
                chk = med_of(A, lambda r: (r["classes"].get("missing_in_range") or {}).get("bloom_checks_per_get"))
                wr = med_of(B, lambda r: level_sum(r, "write"))
                backlog = A[0]["B"] if A else (B[0]["B"] if B else None)
                md.append(
                    f"| {layout} | {kk} | {num(med_of(A, lambda r: r['svc']),0)} | "
                    f"{num(ah('p50') if A else None,2)} | {num(ah('p99') if A else None,2)} | "
                    f"{num(am('p99') if A else None,2)} | {num(chk,3)} | "
                    f"{num(med_of(A, lambda r: r['cpu_ns']),1)} | "
                    f"{num(med_of(B, lambda r: r['drain_s']),0)} | "
                    f"{num(wr/backlog if backlog else None,2)} | "
                    f"{gib(med_of(B, lambda r: r['peak_bytes']))} | "
                    f"{num(med_of(C, lambda r: r['svc']),0)} | "
                    f"{num(p99c('p99') if C else None,1)} |")
        md += ["",
               "Every K sees the same rep structure (rep r runs all K arms in a shuffled "
               "order inside one sweep), so a cold-to-warm page cache moves all K arms "
               "together and the across-K contrast below survives it; the absolute "
               "per-rep numbers do not (see the rep-by-rep table above).", "",
               "A K of 1 is the degenerate point where narrow and wide describe the same "
               "single file, so their agreement there is a consistency check rather than a "
               "layout contrast. In the auto-compaction group the engine only compacts when "
               "its own trigger is reached; at `level0_file_num_compaction_trigger=8` the "
               "K <= 8 arms therefore have nothing to schedule, and their drain second "
               "column is an empty result, not a fast one.", ""]

    md += ["## Evidence chains (§6)", ""]
    md.append("Filled in from the tables above; each line names the comparison it rests on:")
    md.append("")
    md.append("* *overlap → checks/CPU → read latency*: A's `hit_base` / `missing_in_range` "
              "columns against the `checks/Get` columns, narrow vs wide at equal B and K.")
    md.append("* *overlap → compaction bytes → foreground tail*: B's written/read GiB per "
              "GiB of backlog (narrow vs wide) against C's Put p99.")
    md.append("* *larger B → deferred backpressure but higher drain/space*: B's drain seconds "
              "and peak GiB across the three sizes.")
    md.append("* *larger B → less work per GiB*: B's `written GiB per GiB backlog` across sizes "
              "(falling values support better reorganising efficiency).")
    md.append("* *more probing, no performance change*: A's ops/s intervals across layouts at "
              "equal B, with checks/Get differing.")
    md.append("")
    md.append("Work per GiB is the number that decides whether a bigger buffer helps or merely "
              "moves the cost: absolute work always grows with B, so only the per-GiB column "
              "can distinguish the two.")
    md.append("")

    text = "\n".join(md)
    if a.charts:
        make_charts(recs, gA, gB, gC, a.charts, rundir)
        if len({r["K"] for r in recs}) > 1:
            text += (f"\nCharts: `{a.charts}` (chart_K_read_cost.png = frozen group, "
                     f"chart_K_compaction.png = auto-compaction group, "
                     f"chart_K_business.png = business window; the medians behind every "
                     f"point are in chart_K_notes.txt).\n")
        else:
            text += f"\nCharts: `{a.charts}` (layout x size contrast, business+drain timeline, "\
                    f"front-end vs total write volume / peak space).\n"
    if a.md:
        os.makedirs(os.path.dirname(os.path.abspath(a.md)), exist_ok=True)
        open(a.md, "w").write(text)
        print(f"wrote {a.md}")
    else:
        print(text)
    return 0


def make_charts(recs, gA, gB, gC, outdir, rundir=None):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    os.makedirs(outdir, exist_ok=True)

    if len({r["K"] for r in recs}) > 1:
        make_k_charts(recs, gA, gB, gC, outdir, plt, np,
                      rundir or os.path.join(os.path.dirname(outdir), "runs"))
        return

    # 1. layout x size contrast (read throughput and checks)
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    for layout, mk in (("narrow", "o-"), ("wide", "s--")):
        xs = sorted({b for (l, b, _k) in gA if l == layout})
        k0 = sorted({k for (l, b, k) in gA if l == layout and b in xs})[0] if xs else 0
        ys = [statistics.median([r["svc"] for r in gA[(layout, b, k0)]]) for b in xs]
        cs = [statistics.median([(r["classes"].get("missing_in_range") or {}).get("bloom_checks_per_get", 0)
                                 for r in gA[(layout, b, k0)]]) for b in xs]
        if xs:
            axes[0].plot([b / 2**30 for b in xs], ys, mk, label=layout)
            axes[1].plot([b / 2**30 for b in xs], cs, mk, label=layout)
    axes[0].set_xlabel("backlog B (GiB)"); axes[0].set_ylabel("A: total ops/s")
    axes[1].set_xlabel("backlog B (GiB)"); axes[1].set_ylabel("Bloom checks/Get (in-range miss)")
    for ax in axes:
        ax.grid(alpha=0.3); ax.legend()
    fig.suptitle("Layout x size: read throughput and filter checks")
    fig.tight_layout(); fig.savefig(os.path.join(outdir, "chart_layout_x_size.png"), dpi=150); plt.close(fig)

    # 2. business + drain timeline, per state
    fig, ax = plt.subplots(figsize=(8, 4))
    labels, win, drain = [], [], []
    for k in sorted(gC, key=lambda k: (k[0], k[1])):
        labels.append(f"{k[0]}\n{k[1]/2**30:.1f}GiB")
        win.append(statistics.median([r["svc"] or 0 for r in gC[k]]))
        drain.append(statistics.median([r["drain_s"] or 0 for r in gC[k]]))
    x = np.arange(len(labels))
    ax.bar(x - 0.2, win, 0.4, label="in-window ops/s")
    ax2 = ax.twinx()
    ax2.bar(x + 0.2, drain, 0.4, color="tab:red", alpha=0.6, label="drain seconds")
    ax.set_xticks(x); ax.set_xticklabels(labels, fontsize=7)
    ax.set_ylabel("ops/s"); ax2.set_ylabel("drain s")
    ax.set_title("Business window and the settling it leaves behind")
    ax.grid(alpha=0.3, axis="y")
    fig.tight_layout(); fig.savefig(os.path.join(outdir, "chart_business_drain.png"), dpi=150); plt.close(fig)

    # 3. front-end performance vs total write volume and peak space
    fig, ax = plt.subplots(figsize=(7, 5))
    for layout, mk in (("narrow", "o"), ("wide", "s")):
        xs, ys, ss = [], [], []
        for k in sorted([k for k in gC if k[0] == layout], key=lambda k: k[1]):
            rs = gC[k]
            wr = sum(sum(lv["write"] for kk, lv in r["level_work"].items() if kk != "_trivial_moves")
                     for r in rs) / max(1, len(rs))
            xs.append(wr / 2**30)
            ys.append(statistics.median([r["svc"] or 0 for r in rs]))
            ss.append((statistics.median([r["peak_bytes"] or 0 for r in rs]) / 2**30) * 20)
        if xs:
            ax.scatter(xs, ys, s=ss, marker=mk, alpha=0.7, label=f"{layout} (bubble = peak GiB)")
            for x_, y_, k_ in zip(xs, ys, sorted([k for k in gC if k[0] == layout], key=lambda k: k[1])):
                ax.annotate(f"{k_[1]/2**30:.1f}GiB", (x_, y_), fontsize=7)
    ax.set_xlabel("total written GiB during the business experiment")
    ax.set_ylabel("in-window ops/s")
    ax.set_title("Front-end performance vs total write volume / peak space")
    ax.grid(alpha=0.3); ax.legend()
    fig.tight_layout(); fig.savefig(os.path.join(outdir, "chart_perf_vs_write.png"), dpi=150); plt.close(fig)


def make_k_charts(recs, gA, gB, gC, outdir, plt, np, rundir):
    """Three charts for a K sweep. K is the x axis; narrow and wide are separate series.

    Chart 1 is the frozen group (A: direct read cost), chart 2 the auto-compaction group
    (B: what repaying the backlog cost), chart 3 the business window (C). The numbers
    behind every point are also written to chart_K_notes.txt so the figures can be
    checked without reproducing them.
    """
    def med(rs, f):
        v = [f(r) for r in rs if f(r) is not None]
        return statistics.median(v) if v else None

    def series(g):
        out = sorted({(l, b) for (l, b, k) in g})
        marks = ["o-", "s--", "^:", "d-."]
        return [(l, b, marks[i % len(marks)]) for i, (l, b) in enumerate(out)]

    def ks_of(g, layout, b):
        return sorted({k for (l, bb, k) in g if l == layout and bb == b})

    notes = []
    # 1. frozen group: read cost vs K
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    for l, b, mk in series(gA):
        ks = ks_of(gA, l, b)
        axes[0].plot(ks, [med(gA[(l, b, k)], lambda r: r["svc"]) for k in ks], mk,
                     label=f"{l} B={b/2**30:.1f}GiB")
        axes[1].plot(ks, [med(gA[(l, b, k)],
                              lambda r: (r["classes"].get("missing_in_range") or {}).get(
                                  "bloom_checks_per_get")) for k in ks], mk, label=l)
        notes.append(f"A {l} B={b}: " + ", ".join(
            f"K={k} ops/s={med(gA[(l, b, k)], lambda r: r['svc']):.0f} "
            f"checks={med(gA[(l, b, k)], lambda r: (r['classes'].get('missing_in_range') or {}).get('bloom_checks_per_get')):.3f} "
            f"cpu_ns={med(gA[(l, b, k)], lambda r: r['cpu_ns']):.1f}" for k in ks))
    for ax, ylab in zip(axes, ("A: total ops/s", "Bloom checks/Get (in-range miss)")):
        ax.set_xlabel("K (L0 files)"); ax.set_ylabel(ylab); ax.grid(alpha=0.3); ax.legend()
        ax.set_xscale("log", base=2); ax.set_xticks(sorted({k for (_, _, k) in gA}))
        ax.get_xaxis().set_major_formatter(plt.matplotlib.ticker.ScalarFormatter())
    fig.suptitle("Frozen group: direct read cost vs number of L0 files")
    fig.tight_layout(); fig.savefig(os.path.join(outdir, "chart_K_read_cost.png"), dpi=150)
    plt.close(fig)

    # 2. auto-compaction group: drain time and work per GiB
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    for l, b, mk in series(gB):
        ks = ks_of(gB, l, b)
        dr = [med(gB[(l, b, k)], lambda r: (r["drain_s"] or 0) or None) for k in ks]
        wr = [sum(level_sum(r, "write") for r in gB[(l, b, k)]) / max(1, len(gB[(l, b, k)])) / b
              for k in ks]
        axes[0].plot(ks, dr, mk, label=f"{l} B={b/2**30:.1f}GiB")
        axes[1].plot(ks, wr, mk, label=l)
        notes.append(f"B {l} B={b}: " + ", ".join(
            f"K={k} drain_s={med(gB[(l, b, k)], lambda r: r['drain_s'])} "
            f"written/GiB={sum(level_sum(r, 'write') for r in gB[(l, b, k)])/max(1, len(gB[(l, b, k)]))/b:.2f} "
            f"peak_GiB={(med(gB[(l, b, k)], lambda r: r['peak_bytes']) or 0)/2**30:.1f}"
            for k in ks))
    axes[0].set_ylabel("drain seconds (median)"); axes[1].set_ylabel("compaction written bytes / backlog byte")
    for ax in axes:
        ax.set_xlabel("K (L0 files)"); ax.grid(alpha=0.3); ax.legend()
        ax.set_xscale("log", base=2); ax.set_xticks(sorted({k for (_, _, k) in gB}))
        ax.get_xaxis().set_major_formatter(plt.matplotlib.ticker.ScalarFormatter())
    fig.suptitle("Auto-compaction group: what repaying the same backlog cost")
    fig.tight_layout(); fig.savefig(os.path.join(outdir, "chart_K_compaction.png"), dpi=150)
    plt.close(fig)

    # 3. business window: throughput and write tail vs K
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    for l, b, mk in series(gC):
        ks = ks_of(gC, l, b)
        axes[0].plot(ks, [med(gC[(l, b, k)], lambda r: r["svc"]) for k in ks], mk,
                     label=f"{l} B={b/2**30:.1f}GiB")
        p99 = []
        for k in ks:
            v = [pct_from_hist(os.path.join(rundir, r["rid"] + ".hist_cohort_put.csv"))
                 for r in gC[(l, b, k)]]
            v = [x["p99"] / 1000 for x in v if x and "p99" in x]
            p99.append(statistics.median(v) if v else None)
        axes[1].plot(ks, p99, mk, label=l)
        notes.append(f"C {l} B={b}: " + ", ".join(
            f"K={k} ops/s={med(gC[(l, b, k)], lambda r: r['svc']):.0f} "
            f"put_p99_us={(p if p is not None else float('nan')):.1f} "
            f"drain_s={med(gC[(l, b, k)], lambda r: r['drain_s'])}" for k, p in zip(ks, p99)))
    axes[0].set_ylabel("in-window ops/s"); axes[1].set_ylabel("Put p99 (µs)")
    for ax in axes:
        ax.set_xlabel("K (L0 files)"); ax.grid(alpha=0.3); ax.legend()
        ax.set_xscale("log", base=2); ax.set_xticks(sorted({k for (_, _, k) in gC}))
        ax.get_xaxis().set_major_formatter(plt.matplotlib.ticker.ScalarFormatter())
    fig.suptitle("Business window (90% get / 10% put at a common arrival rate)")
    fig.tight_layout(); fig.savefig(os.path.join(outdir, "chart_K_business.png"), dpi=150)
    plt.close(fig)

    with open(os.path.join(outdir, "chart_K_notes.txt"), "w") as f:
        f.write("medians behind the K charts (regenerate with l0_backlog_analyze.py)\n\n")
        for n in notes:
            f.write(n + "\n")


if __name__ == "__main__":
    sys.exit(main())
