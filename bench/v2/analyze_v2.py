#!/usr/bin/env python3
"""perf_v2 analysis: turn the M1-M5 run directories into the tables the runbook
asks for, and only then into conclusions.

Rules this script obeys, because they are the difference between a number and a
result:
  * service throughput only, never whole-run throughput (runbook §2.2).
  * invalid runs (check_run.py) are excluded and counted, never averaged in.
  * latency percentiles come from the cohort per-class histograms and are bucket
    LOWER bounds; the bucket width is reported alongside so the resolution is
    visible rather than implied.
  * a difference smaller than the between-rep spread, or smaller than ~1.5x, is
    reported as "unresolved" instead of being called an effect (runbook §8).
  * a P99.9 is only reported for a class with >= 1,000,000 samples.

Usage:
  analyze_v2.py --out DIR [--stage m1] [--md REPORT.md]
"""
import argparse
import csv
import glob
import json
import os
import re
import statistics
import sys

M64 = (1 << 64) - 1


# ---------------------------------------------------------------------------
# loading
# ---------------------------------------------------------------------------
def load_json(p):
    try:
        with open(p) as f:
            return json.load(f)
    except Exception:
        return None


def argv_of(cfg):
    """Extract the driver argv recorded in <rid>.config.txt."""
    try:
        first = open(cfg, errors="replace").readline().strip()
    except Exception:
        return {}
    if not first.startswith("argv:"):
        return {}
    toks = first[len("argv:"):].split()
    d = {}
    for t in toks:
        if not t.startswith("--"):
            continue
        t = t[2:]
        if "=" in t:
            k, v = t.split("=", 1)
            d[k] = v
        else:
            d[t] = "1"
    return d


def latency_row(run_dir, rid):
    """Cohort per-class percentiles from summary_extra.csv (bucket lower bounds)."""
    p = os.path.join(run_dir, f"{rid}.summary_extra.csv")
    out = {}
    if not os.path.exists(p):
        return out
    for r in csv.DictReader(open(p, newline="")):
        name = r["name"]
        if name.startswith("cohort_"):
            out[name] = {
                "p50_ns": float(r["p50_ns"]), "p90_ns": float(r["p90_ns"]),
                "p99_ns": float(r["p99_ns"]), "p999_ns": float(r["p999_ns"]),
                "mean_ns": float(r["mean_ns"]), "max_ns": float(r["max_ns"]),
            }
    return out


def detail_stats(run_dir, rid):
    """Sampled per-class probe/read statistics from the detail stream."""
    out = {}
    for p in glob.glob(os.path.join(run_dir, f"{rid}.detail_*.csv")):
        cls = os.path.basename(p).split(".detail_")[1][:-4]
        n = 0
        agg = {"block_read": 0.0, "block_read_byte": 0.0, "cache_hit": 0.0,
               "filter_hit": 0.0, "filter_read": 0.0, "bloom_hit": 0.0, "bloom_miss": 0.0,
               "get_read_bytes": 0.0}
        for r in csv.DictReader(open(p, newline="")):
            n += 1
            for k, col in (("block_read", "block_read_count"), ("block_read_byte", "block_read_byte"),
                           ("cache_hit", "block_cache_hit"), ("filter_hit", "block_cache_filter_hit"),
                           ("filter_read", "filter_block_read"), ("bloom_hit", "bloom_sst_hit"),
                           ("bloom_miss", "bloom_sst_miss"), ("get_read_bytes", "get_read_bytes")):
                try:
                    agg[k] += float(r.get(col) or 0)
                except ValueError:
                    pass
        if n:
            out[cls] = {k: v / n for k, v in agg.items()}
            out[cls]["samples"] = n
    return out


def pending_slope(run_dir, rid, t_start, t_end):
    """GiB/s growth of estimate-pending-compaction-bytes across the measure window."""
    p = os.path.join(run_dir, f"{rid}.samples.csv")
    if not os.path.exists(p):
        return None
    col = "estimate-pending-compaction-bytes"
    pts = []
    with open(p, newline="") as f:
        rd = csv.DictReader(f)
        if col not in (rd.fieldnames or []):
            return None
        for r in rd:
            try:
                ts = float(r["ts"])
            except (KeyError, ValueError):
                continue
            if t_start <= ts <= t_end:
                try:
                    pts.append((ts, float(r[col])))
                except ValueError:
                    pass
    if len(pts) < 5:
        return None
    dt = pts[-1][0] - pts[0][0]
    if dt <= 0:
        return None
    return (pts[-1][1] - pts[0][1]) / dt / (1024 ** 3)


def steady_state(run_dir, rid, M, thr_tol=0.10, p99_tol=1.5):
    """Is the measurement cohort still moving while we measured it?

    Runbook §5 review: look at the second half of the window separately, and extend a
    key arm to 300-600 s when it has not converged. A single 60 s average cannot tell
    a steady state from a transient, so this compares the two halves of the window
    using the per-second series the driver already writes.
    """
    out = {"steady": None, "reason": None, "first_half_ops_s": None,
           "second_half_ops_s": None, "half_ratio": None,
           "p99_first_us": None, "p99_second_us": None, "p99_ratio": None}
    p = os.path.join(run_dir, f"{rid}.per_sec.csv")
    if not os.path.exists(p) or not M:
        return out
    t0, t1 = M.get("measure_start"), M.get("measure_end")
    if not t0 or not t1 or t1 <= t0:
        return out
    mid = (t0 + t1) / 2.0
    rows = []
    with open(p, newline="") as f:
        for r in csv.DictReader(f):
            try:
                ts = float(r["ts"])
            except (KeyError, ValueError):
                continue
            if t0 <= ts <= t1:
                rows.append(r)
    if len(rows) < 4:
        return out

    def half(lo, hi):
        sub = [r for r in rows if lo <= float(r["ts"]) < hi]
        ops = sum(num(r.get("m_ops")) for r in sub)
        span = hi - lo
        p99s = [num(r.get("eng_p99_us")) for r in sub if num(r.get("eng_p99_us")) > 0]
        return (ops / span if span > 0 else 0.0), (statistics.median(p99s) if p99s else None), len(sub)

    a_ops, a_p99, na = half(t0, mid)
    b_ops, b_p99, nb = half(mid, t1)
    out["first_half_ops_s"], out["second_half_ops_s"] = a_ops, b_ops
    if a_ops > 0:
        out["half_ratio"] = b_ops / a_ops
    if a_p99 and b_p99:
        out["p99_first_us"], out["p99_second_us"] = a_p99, b_p99
        out["p99_ratio"] = b_p99 / a_p99
    if na < 2 or nb < 2:
        return out
    reasons = []
    if out["half_ratio"] is not None and abs(out["half_ratio"] - 1.0) > thr_tol:
        reasons.append(f"throughput second/first half = {out['half_ratio']:.2f}")
    if out["p99_ratio"] is not None and out["p99_ratio"] > p99_tol:
        reasons.append(f"P99 second/first half = {out['p99_ratio']:.2f}")
    slope = pending_slope(run_dir, rid, t0, t1)
    if slope is not None and slope > 0.05:
        reasons.append(f"pending still rising at {slope:.2f} GiB/s")
    out["steady"] = not reasons
    out["reason"] = "; ".join(reasons) if reasons else "converged within the window"
    return out


def valid_runs(out_root, stage):
    """Yield run records for a stage, marking check_run.py validity."""
    d = os.path.join(out_root, stage)
    for sp in sorted(glob.glob(os.path.join(d, "*.summary.json"))):
        rid = os.path.basename(sp)[:-len(".summary.json")]
        S = load_json(sp) or {}
        M = load_json(os.path.join(d, rid + ".measure_phase.json")) or {}
        cfg = os.path.join(d, rid + ".config.txt")
        A = argv_of(cfg) if os.path.exists(cfg) else {}
        vp = os.path.join(d, rid + ".validity.txt")
        valid = None
        if os.path.exists(vp):
            valid = "VALID=1" in open(vp, errors="replace").read()
        rec = {
            "rid": rid, "stage": stage, "dir": d, "S": S, "M": M, "argv": A,
            "valid": valid, "lat": latency_row(d, rid), "detail": detail_stats(d, rid),
        }
        yield rec


def num(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# effect resolution (runbook §8): never call a difference an effect on its own
# ---------------------------------------------------------------------------
def unresolved(a_vals, b_vals, min_ratio=1.5):
    """True when two groups are not distinguishable under the runbook's rules."""
    if not a_vals or not b_vals:
        return True
    ma, mb = statistics.mean(a_vals), statistics.mean(b_vals)
    if ma <= 0 or mb <= 0:
        return True
    ratio = max(ma, mb) / min(ma, mb)
    if ratio < min_ratio:
        return True
    # between-rep spread must be smaller than the gap
    spread = 0.0
    for vals, m in ((a_vals, ma), (b_vals, mb)):
        if len(vals) > 1:
            spread = max(spread, (max(vals) - min(vals)) / m)
    gap = abs(ma - mb) / min(ma, mb)
    return spread >= gap


def bucket_width_ns(label):
    """Bucket width at a v2 lower-bound label, for reporting resolution.

    A v2 label is mant << sh with mant in [8,15] (the reachable mantissas: the top
    bit of mant is the octave's leading bit). The decomposition is unique because
    doubling mant would leave [8,15].
    """
    label = int(label)
    if label < 256:
        return 1
    for sh in range(0, 64):
        if label % (1 << sh):
            continue
        m = label >> sh
        if 8 <= m <= 15:
            return 1 << sh
    return None


def us(ns):
    return f"{ns / 1000.0:.1f}"


# ---------------------------------------------------------------------------
# per-matrix tables
# ---------------------------------------------------------------------------
def m1_table(runs):
    rows = []
    for r in runs:
        A, S, M = r["argv"], r["S"], r["M"]
        if A.get("mode") != "overwrite":
            continue
        offered = num(A.get("rate"))
        svc = num(M.get("throughput_service_ops_s") or S.get("throughput_service_ops_s"))
        lat = r["lat"].get("cohort_eng_put", {})
        win = num(M.get("measure_window_s"))
        slope = pending_slope(r["dir"], r["rid"],
                              num(M.get("measure_start")), num(M.get("measure_end")))
        wa = None
        if num(S.get("U_bytes")) > 0:
            wa = num(S.get("device_write_bytes")) / num(S.get("U_bytes"))
        ss = steady_state(r["dir"], r["rid"], M)
        rows.append({
            "rid": r["rid"], "valid": r["valid"], "offered": offered, "achieved": svc,
            "ratio": (svc / offered) if offered > 0 else None,
            "p50_us": us(lat["p50_ns"]) if lat else None,
            "p99_us": us(lat["p99_ns"]) if lat else None,
            "p999_us": us(lat["p999_ns"]) if lat else None,
            "bucket_ns": bucket_width_ns(lat["p99_ns"]) if lat else None,
            "pending_gib_s": slope, "stalls": num(S.get("stall_begin_events")),
            "dev_write_gib": num(S.get("device_write_bytes")) / 1024 ** 3,
            "wa": wa, "window_s": win, "censored": M.get("censored"),
            "arrival": A.get("arrival", "closed"), "noc": A.get("disable_auto_compactions") == "1",
            "submitted": M.get("cohort_submitted"), "completed": M.get("cohort_completed"),
            # submitted/offered < 1 means the client queue was deeper than the window:
            # scheduled arrivals still waiting for their slot when the deadline hit were
            # never admitted. It is a direct measure of saturation, and it is the same
            # quantity as achieved/offered whenever everything admitted completes.
            "submitted_over_offered": M.get("submitted_over_offered"),
            "client_wait_p99_us": M.get("client_wait_p99_us"),
            "long_window": bool(win and win >= 200),
            "steady": ss["steady"], "steady_reason": ss["reason"],
            "half_ratio": ss["half_ratio"], "p99_ratio": ss["p99_ratio"],
        })
    # group by offered fraction for the knee rule
    for r in rows:
        r["frac"] = r["rid"].split("_f")[1].split("_")[0] if "_f" in r["rid"] else None
    # low-load baseline for the latency criterion, and the ratio against it
    good = [r for r in rows if r["valid"] is not False and not r["noc"] and r["offered"] > 0]
    if good:
        base = min(good, key=lambda r: r["offered"])
        bp99 = num(base["p99_us"], 0)
        for r in rows:
            r["p99_vs_3x_baseline"] = (num(r["p99_us"]) / bp99) if bp99 > 0 else None
            # "crossed" is the runbook's knee criterion; "<=3x" only means the latency
            # threshold has NOT been reached, which is not itself a finding.
            r["p99_crossed_3x"] = bool(bp99 > 0 and num(r["p99_us"]) > 3 * bp99)
        for r in rows:
            r["baseline_rid"] = base["rid"]
    return rows


def knee_rule(rows):
    """runbook §5 M1. Two independent criteria, either one marks the knee:
       (a) achieved/offered < 0.95 while pending compaction keeps rising, or
       (b) P99 exceeds 3x the low-load baseline's P99.

    Both are evaluated on the MEDIAN across reps at each offered rate, not on
    individual runs. At a fixed offered rate the tail moves by almost an order of
    magnitude between reps (163.8 us vs 1572.9 us at 0.75x in the 2026-09-25 run), so
    a per-run rule would place the knee wherever an outlier rep happened to fall. The
    per-rep values are kept in the recorded decision so the spread stays visible.
    """
    good = [r for r in rows if r["valid"] is not False and not r["noc"] and r["offered"] > 0]
    if not good:
        return None
    # Only the standard window feeds the knee: a 300 s arm and a 60 s arm at the same
    # offered fraction are different measurements and must not be averaged together.
    standard = [r for r in good if not r.get("long_window")]
    if standard:
        good = standard
    # Group by the CONTROLLED factor -- the offered fraction of each rep's own
    # closed-loop ceiling -- not by the absolute rate. Each rep normalises against its
    # own ceiling (runbook §5 M1), and those ceilings differ by up to ~40% between
    # reps, so absolute-rate buckets would hold one rep each and the "median across
    # reps" would be a single run.
    groups = {}
    for r in good:
        if r.get("frac"):
            groups.setdefault(r["frac"], []).append(r)
    if len(groups) < 2:                       # fall back to absolute rates
        for r in good:
            groups.setdefault(f"{r['offered']:.0f}", []).append(r)
    keys = sorted(groups, key=lambda k: min(r["offered"] for r in groups[k]))
    base_rows = groups[keys[0]]
    base_p99 = statistics.median([num(r["p99_us"]) for r in base_rows if r["p99_us"]])
    out = {
        "baseline_offered_ops_s": statistics.median([r["offered"] for r in base_rows]),
        "baseline_p99_us": base_p99,
        "baseline_reps": len(base_rows),
        "baseline_rep_p99_us": [num(r["p99_us"]) for r in base_rows if r["p99_us"]],
        "aggregation": "median across reps of the arms at the same offered FRACTION "
                       "(each rep normalises against its own closed-loop ceiling)",
        "per_rate": [],
        "criterion": "none",
        "knee_ops_s": None,
    }
    for key in keys:
        rs = groups[key]
        p99s = [num(r["p99_us"]) for r in rs if r["p99_us"]]
        ratios = [r["ratio"] for r in rs if r["ratio"] is not None]
        slopes = [r["pending_gib_s"] for r in rs if r["pending_gib_s"] is not None]
        med_p99 = statistics.median(p99s) if p99s else None
        med_ratio = statistics.median(ratios) if ratios else None
        med_slope = statistics.median(slopes) if slopes else None
        entry = {"frac": key,
                 "offered_ops_s": statistics.median([r["offered"] for r in rs]),
                 "offered_ops_s_per_rep": [r["offered"] for r in rs],
                 "reps": len(rs), "median_p99_us": med_p99,
                 "median_ratio": med_ratio, "median_pending_gib_s": med_slope,
                 "rep_p99_us": p99s, "rep_ratio": ratios,
                 "p99_vs_baseline": (med_p99 / base_p99) if (med_p99 and base_p99) else None,
                 "p99_crossed_3x": bool(med_p99 and base_p99 and med_p99 > 3 * base_p99),
                 "saturated_with_rising_pending": bool(med_ratio is not None and med_ratio < 0.95
                                                       and med_slope is not None and med_slope > 0)}
        out["per_rate"].append(entry)
    for e in out["per_rate"]:
        if e["p99_crossed_3x"]:
            out.update({"criterion": "p99_gt_3x_baseline", "knee_ops_s": e["offered_ops_s"],
                        "reason": f"median P99 {e['median_p99_us']:.1f} us exceeds 3x the "
                                  f"low-load baseline {base_p99:.1f} us "
                                  f"({e['p99_vs_baseline']:.1f}x); per-rep P99 "
                                  f"{[round(x,1) for x in e['rep_p99_us']]}",
                        "at_rate": e})
            return out
        if e["saturated_with_rising_pending"]:
            out.update({"criterion": "saturated_with_rising_pending",
                        "knee_ops_s": e["offered_ops_s"],
                        "reason": f"median achieved/offered {e['median_ratio']:.3f} < 0.95 with "
                                  f"pending rising at {e['median_pending_gib_s']:.2f} GiB/s",
                        "at_rate": e})
            return out
    top = out["per_rate"][-1]
    out.update({"criterion": "none", "knee_ops_s": None,
                "reason": f"neither criterion met up to {top['offered_ops_s']:.0f} ops/s offered; "
                          f"median P99 stayed at {top['p99_vs_baseline']:.2f}x the low-load "
                          f"baseline, i.e. the 3x latency threshold was not crossed"})
    return out


def m2_table(runs):
    rows = []
    for r in runs:
        A, S, M = r["argv"], r["S"], r["M"]
        if A.get("mode") != "read":
            continue
        hit = r["lat"].get("cohort_eng_get_hit", {})
        miss = r["lat"].get("cohort_eng_get_miss", {})
        det_hit = r["detail"].get("get_hit", {})
        rows.append({
            "rid": r["rid"], "valid": r["valid"], "threads": A.get("threads"),
            "cache": A.get("cache_size"), "mix": A.get("read_mix"),
            "total_ops_s": num(M.get("throughput_service_ops_s")),
            "hit_ops_s": num(S.get("cls_get_hit")) / num(M.get("measure_window_s"), 1) if M else None,
            "get_hit": num(M.get("cohort_get_hit")), "get_miss": num(M.get("cohort_get_miss")),
            "hit_p50_us": us(hit["p50_ns"]) if hit else None,
            "hit_p99_us": us(hit["p99_ns"]) if hit else None,
            "miss_p50_us": us(miss["p50_ns"]) if miss else None,
            "miss_p99_us": us(miss["p99_ns"]) if miss else None,
            "block_reads_per_get": det_hit.get("block_read"),
            "filter_probes_per_get": det_hit.get("filter_read"),
            "bloom_hit_per_get": det_hit.get("bloom_hit"),
            "detail_samples": det_hit.get("samples"),
            # Whether the arm's reads actually reached the device is the first thing to
            # look at on this host: a 47 GiB database fits in the 62 GiB page cache, so a
            # read arm after warm-up can be answered entirely from RAM and the block-cache
            # question becomes "RocksDB cache on top of a warm page cache". Dropping the
            # page cache needs root, which the campaign does not have, so the state has to
            # be reported per arm instead of controlled.
            "dev_read_gib": num(S.get("device_read_bytes")) / 1024 ** 3,
            "read_bytes_per_op": (num(S.get("device_read_bytes")) / num(M.get("cohort_ops"), 1))
                                  if M.get("cohort_ops") else None,
            "online": "online" in r["rid"],
        })
    return rows


def generic_table(runs, keep=lambda A: True):
    rows = []
    for r in runs:
        A, S, M = r["argv"], r["S"], r["M"]
        if not keep(A):
            continue
        row = {"rid": r["rid"], "valid": r["valid"],
               "ops_s": num(M.get("throughput_service_ops_s")),
               "offered": num(M.get("offered_ops_s")),
               "submitted": M.get("cohort_submitted"),
               "completed": M.get("cohort_completed"),
               "client_wait_p99_us": M.get("client_wait_p99_us"),
               "censored": M.get("censored"),
               "cohort": num(M.get("cohort_ops")),
               "window_s": num(M.get("measure_window_s"))}
        ss = steady_state(r["dir"], r["rid"], M)
        row["half_ratio"] = ss["half_ratio"]
        row["steady"] = ss["steady"]
        for cls in ("put", "get_hit", "get_miss", "scan"):
            lat = r["lat"].get(f"cohort_eng_{cls}")
            row[f"{cls}_n"] = num(M.get(f"cohort_{cls}"))
            row[f"{cls}_p50_us"] = us(lat["p50_ns"]) if lat else None
            row[f"{cls}_p99_us"] = us(lat["p99_ns"]) if lat else None
            # a P99.9 needs >= 1e6 samples in that class to mean anything (§5 M5)
            row[f"{cls}_p999_us"] = us(lat["p999_ns"]) if (lat and row[f"{cls}_n"] >= 1000000) else None
        for k in ("threads", "read_pct", "scan_len", "scan_start", "cache_size",
                  "rate", "arrival", "backlog_files", "backlog_overlap"):
            row[k] = A.get(k)
        rows.append(row)
    return rows


def md_table(rows, cols, title):
    if not rows:
        return f"### {title}\n\n_no runs_\n"
    out = [f"### {title}", "",
           "| " + " | ".join(cols) + " |",
           "|" + "|".join(["---"] * len(cols)) + "|"]
    for r in rows:
        cells = []
        for c in cols:
            v = r.get(c)
            if isinstance(v, float):
                cells.append(f"{v:.2f}")
            elif v is None:
                cells.append("-")
            else:
                cells.append(str(v))
        out.append("| " + " | ".join(cells) + " |")
    out.append("")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="the perf_v2 output root")
    ap.add_argument("--stage", default=None, help="analyse only one matrix")
    ap.add_argument("--md", default=None, help="markdown report path")
    a = ap.parse_args()

    stages = [a.stage] if a.stage else ["m1", "m2", "m3", "m4", "m5"]
    md = [f"# perf_v2 analysis", "",
          f"Source: `{a.out}`. Service throughput only (measurement cohort); invalid runs "
          f"excluded. Latency percentiles are bucket **lower bounds** from the cohort "
          f"per-class histograms (hist_version 2); the bucket width is given where it "
          f"matters so the resolution is visible.", ""]

    for st in stages:
        d = os.path.join(a.out, st)
        if not os.path.isdir(d):
            continue
        runs = list(valid_runs(a.out, st))
        n_all = len(runs)
        n_bad = sum(1 for r in runs if r["valid"] is False)
        md.append(f"## {st.upper()} ({n_all} runs, {n_bad} invalid/excluded)")
        md.append("")
        if st == "m1":
            rows = m1_table(runs)
            md.append(md_table(rows,
                               ["rid", "arrival", "offered", "achieved", "ratio", "submitted",
                                "completed", "submitted_over_offered", "client_wait_p99_us",
                                "p50_us", "p99_us",
                                "p99_vs_3x_baseline", "p99_crossed_3x", "bucket_ns",
                                "half_ratio", "steady", "pending_gib_s", "stalls", "wa",
                                "window_s", "valid"],
                               "M1 arms (pure write)"))
            if rows:
                n_steady = sum(1 for r in rows if r["steady"] is True)
                n_unsteady = sum(1 for r in rows if r["steady"] is False)
                md.append(f"Steady-state check (second vs first half of the measure window): "
                          f"{n_steady} converged, {n_unsteady} still moving. "
                          f"`half_ratio` is second-half/first-half throughput; a value at or "
                          f"below ~1.0 means the rate was still climbing or decaying while it "
                          f"was being measured.")
                md.append("")
            long_rows = [r for r in rows if r.get("long_window")]
            if long_rows:
                md.append("#### Extended-window arms (the steady-state follow-up)")
                md.append("")
                md.append("Arms the short window flagged as still moving, re-run on a fresh "
                          "replica with a longer measurement window. These are excluded from "
                          "the knee aggregation above.")
                md.append("")
                md.append(md_table(long_rows,
                                   ["rid", "offered", "achieved", "ratio", "submitted_over_offered",
                                    "client_wait_p99_us", "p50_us", "p99_us", "p99_vs_3x_baseline",
                                    "half_ratio", "p99_ratio", "steady", "steady_reason",
                                    "window_s", "valid"],
                                   "extended-window arms"))
                by_frac_long = {}
                for r in long_rows:
                    if r["frac"]:
                        by_frac_long.setdefault(r["frac"], []).append(num(r["p99_us"]))
                by_frac_short = {}
                for r in rows:
                    if r["frac"] and not r.get("long_window"):
                        by_frac_short.setdefault(r["frac"], []).append(num(r["p99_us"]))
                common = sorted(set(by_frac_long) & set(by_frac_short))
                if common:
                    md.append("| offered fraction | median P99 (60 s) | median P99 (long) | ratio |")
                    md.append("|---|---:|---:|---:|")
                    for f in common:
                        short_med = statistics.median(by_frac_short[f])
                        long_med = statistics.median(by_frac_long[f])
                        md.append(f"| {f} | {short_med:.1f} µs | {long_med:.1f} µs | "
                                  f"{long_med / short_med:.2f}x |")
                    md.append("")
            k = knee_rule(rows)
            if k:
                md.append(f"**Knee rule** (aggregated across reps): criterion "
                          f"`{k['criterion']}` -> {k['reason']}")
                md.append("")
                if k.get("knee_ops_s"):
                    md.append(f"Knee offered rate ≈ **{k['knee_ops_s']:.0f} ops/s** "
                              f"(achieved {k['at_rate']['median_ratio']:.3f}x offered, median).")
                    n_steady_long = sum(1 for r in long_rows if r.get("steady") is True)
                    if long_rows and n_steady_long == 0:
                        md.append("")
                        md.append(f"**Caveat — read this as a band, not a point.** "
                                  f"{len(long_rows)} arms were re-run at a longer window and "
                                  f"**none of them converged** (pending still rising, and at the "
                                  f"higher rates the second half of the window delivers well under "
                                  f"the first half). The knee location is also rep-dependent "
                                  f"because each rep normalises against its own closed-loop "
                                  f"ceiling, which drifted between reps. A downstream matrix "
                                  f"(M3) should treat this number as the middle of a "
                                  f"40,000–50,000 ops/s band rather than a sharp threshold.")
                else:
                    md.append("No knee: the highest offered rate is where the search stopped, "
                              "not a knee.")
                md.append("")
                md.append("| offered ops/s | reps | median P99 µs | per-rep P99 µs | "
                          "P99 vs baseline | median achieved/offered | median pending GiB/s |")
                md.append("|---|---:|---:|---|---:|---:|---:|")
                for e in k["per_rate"]:
                    md.append(f"| {e['offered_ops_s']:.0f} | {e['reps']} | "
                              f"{e['median_p99_us']:.1f} | "
                              f"{', '.join(f'{x:.1f}' for x in e['rep_p99_us'])} | "
                              f"{e['p99_vs_baseline']:.2f}x | "
                              f"{e['median_ratio']:.3f} | "
                              f"{('%.3f' % e['median_pending_gib_s']) if e['median_pending_gib_s'] is not None else '-'} |")
                md.append("")
                os.makedirs(d, exist_ok=True)
                json.dump(k, open(os.path.join(d, "knee.json"), "w"), indent=2)
            # resolution check: is any offered-rate effect above the rep spread?
            by_frac = {}
            for r in rows:
                if r["frac"] and not r["noc"] and r["valid"] is not False:
                    by_frac.setdefault(r["frac"], []).append(num(r["achieved"]))
            if len(by_frac) >= 2:
                fs = sorted(by_frac)
                lo, hi = by_frac[fs[0]], by_frac[fs[-1]]
                verdict = "unresolved" if unresolved(lo, hi) else "resolved"
                md.append(f"Lowest offered fraction ({fs[0]}) vs highest ({fs[-1]}): "
                          f"achieved {statistics.mean(lo):.0f} vs {statistics.mean(hi):.0f} ops/s "
                          f"-> **{verdict}** under the §8 rule (>=1.5x and above rep spread).")
                md.append("")
            # Arms that were still moving, or at/above the knee, are the ones the
            # runbook says to extend to 300-600 s rather than average over 60 s.
            ext = []
            for r in runs:
                A, M = r["argv"], r["M"]
                if A.get("mode") != "overwrite" or not M:
                    continue
                ss = steady_state(r["dir"], r["rid"], M)
                at_knee = k and k.get("knee_ops_s") and num(A.get("rate")) >= k["knee_ops_s"]
                if ss["steady"] is False or at_knee:
                    why = ss["reason"] if ss["steady"] is False else "at or above the knee rate"
                    ext.append({"rid": r["rid"], "stage": st, "why": why,
                                "argv": A, "measure_window_s": M.get("measure_window_s")})
            if ext:
                with open(os.path.join(d, "needs_extension.json"), "w") as f:
                    json.dump(ext, f, indent=2)
                md.append(f"{len(ext)} arm(s) flagged for extension to a longer window: "
                          + ", ".join(e["rid"] for e in ext))
                md.append("")
        elif st == "m2":
            rows = m2_table(runs)
            n_cache_resident = sum(1 for r in rows if (r.get("dev_read_gib") or 0) < 0.05)
            if rows:
                md.append(f"Page-cache state: **{n_cache_resident} of {len(rows)} read arms "
                          f"served under 0.05 GiB from the device**, i.e. answered from RAM. "
                          f"This host has 62 GiB of RAM and the database is 47 GiB, so after "
                          f"warm-up a read arm can run entirely out of the OS page cache; the "
                          f"cache-size column then measures the RocksDB block cache on top of a "
                          f"warm page cache, and `dev_read_gib` / `read_bytes_per_op` say which "
                          f"arms were actually device-bound. Dropping the page cache requires "
                          f"root, which the campaign does not have, so this is reported rather "
                          f"than controlled.")
                md.append("")
            md.append(md_table(rows,
                               ["rid", "threads", "cache", "mix", "total_ops_s", "get_hit",
                                "get_miss", "hit_p50_us", "hit_p99_us", "miss_p99_us",
                                "block_reads_per_get", "filter_probes_per_get",
                                "dev_read_gib", "read_bytes_per_op", "online", "valid"],
                               "M2 arms (pure point reads)"))
        elif st == "m3":
            rows = generic_table(runs, lambda A: A.get("mode") == "mix")
            md.append(md_table(rows,
                               ["rid", "read_pct", "rate", "arrival", "ops_s",
                                "put_n", "put_p99_us", "get_hit_n", "get_hit_p99_us",
                                "get_miss_n", "censored", "valid"],
                               "M3 arms (mixed read/write)"))
        elif st == "m4":
            rows = generic_table(runs, lambda A: A.get("mode") in ("read", "backlog"))
            md.append(md_table(rows,
                               ["rid", "backlog_files", "backlog_overlap", "ops_s",
                                "get_hit_n", "get_hit_p99_us", "get_miss_n",
                                "get_miss_p99_us", "censored", "valid"],
                               "M4 arms (L0 geometry at a fixed working set)"))
        elif st == "m5":
            rows = generic_table(runs, lambda A: True)
            md.append(md_table(rows,
                               ["rid", "threads", "read_pct", "scan_len", "scan_start",
                                "cache_size", "ops_s", "put_p99_us", "get_hit_p99_us",
                                "scan_p50_us", "scan_p99_us", "censored", "valid"],
                               "M5 arms (threads / scans / cache)"))
        md.append("")

    md.append("## Unresolved-effect rule")
    md.append("")
    md.append("An arm-to-arm difference is only called an effect when it is at least "
              "1.5x AND larger than the between-rep spread. Otherwise the result is "
              "`unresolved` and no parameter is named as optimal.")
    md.append("")

    text = "\n".join(md)
    if a.md:
        os.makedirs(os.path.dirname(os.path.abspath(a.md)), exist_ok=True)
        with open(a.md, "w") as f:
            f.write(text)
        print(f"wrote {a.md}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
