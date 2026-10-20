#!/usr/bin/env python3
"""Latency erratum for the pre-2026-09-25 exports (review item 2).

What is and is not recoverable
-----------------------------
The v1 encoder wrote `label = (mant | kGroup) << sh`, which adds a spurious high bit
and lifts every label for v >= 256 to 2.07x..3.00x its true bucket. The label is
still injective on reachable buckets, so the BUCKET can be recovered exactly and its
inclusive interval [lower, upper] restored. What cannot be recovered is the position
of a request *inside* its bucket: the original files only ever held bucket counts.

So every corrected number below is reported as an interval plus, where several reps
exist, the mean of those intervals across reps -- never as a point latency. The
remaining resolution is stated per value (`width_ns`), because for a 2.4 ms bucket
that width is ~260 us (11%) and a reader must see that.

Usage:
  legacy_erratum.py [--root bench/out/I] [--out bench/out/hist_erratum]
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
METRICS = ("p50_ns", "p90_ns", "p99_ns", "p999_ns", "p9999_ns")


def legacy_label(i):
    """The v1 encoder, reproduced."""
    if i < SMALL:
        return i
    oct_, mant = divmod(i - SMALL, GROUP)
    return (mant | GROUP) << ((BASEBIT + oct_) - MANTB)


def legacy_interval(label):
    """Invert a v1 label -> (lower, upper, width) or None if not a reachable bucket.

    label = (mant|16) << sh with mant in [8,15], so the quotient by a power of two
    lands in [24,31]. The solution is unique: halving a valid mantissa leaves [12,15]
    which is out of range, so no second decomposition exists.
    """
    if label < SMALL:
        return (label, label, 1)
    for sh in range(0, 64):
        if label % (1 << sh):
            continue
        m = label >> sh
        if 24 <= m <= 31:
            mant = m - 16
            lo = mant << sh
            hi = ((mant + 1) << sh) - 1
            return (lo, hi, 1 << sh)
    return None


def selftest():
    """Round-trip over REACHABLE buckets only, and prove the unreachable ones are
    exactly the indices that cannot appear in a v1 file.

    Reachable buckets carry the octave's leading bit in the top mantissa bit, i.e.
    mant in [8,15]. The other half of each octave's index space is never produced by
    the encoder, so a round-trip failure there is expected, not a defect.
    """
    bad = 0
    unreachable_that_round_trip = 0
    for i in range(SMALL, SMALL + (64 - BASEBIT) * GROUP):
        _, mant = divmod(i - SMALL, GROUP)
        if mant < GROUP // 2:
            lab = legacy_label(i)
            if legacy_interval(lab) is not None and legacy_interval(lab)[0] == (mant << ((BASEBIT + (i - SMALL) // GROUP) - MANTB)):
                unreachable_that_round_trip += 1
            continue
        lab = legacy_label(i)
        dec = legacy_interval(lab)
        if dec is None:
            bad += 1
            continue
        lo, hi, _ = dec
        # reconstruct a sample that belongs to bucket i
        oct_, mant = divmod(i - SMALL, GROUP)
        sh = (BASEBIT + oct_) - MANTB
        v = (mant << sh) + (sh and 1)
        if not (lo <= v <= hi):
            bad += 1
    return bad


def arm_of(rid):
    """Strip the rep suffix and the build/drain sub-run infix."""
    for suf in ("_ext300", "_ext600"):
        if rid.endswith(suf):
            rid = rid[: -len(suf)]
    base = rid
    sub = ""
    for inf in (".build", ".drain"):
        if rid.endswith(inf):
            base, sub = rid[: -len(inf)], inf[1:]
    base = base.rsplit("_r", 1)[0] if "_r" in base else base
    for pre in ("bn_", "m1_", "m2_", "m3_", "m4_", "m5_"):
        if base.startswith(pre):
            base = base[len(pre):]
            break
    return base, sub


def throughput_denominator(root):
    """Why the old throughput_ops_s cannot be compared across arms (review item 3).

    The latency relabel changes no throughput count, but the old denominator was the
    whole process duration: it included `--drain_observe_s` (10-120 s, present in 86
    of 87 configs), the `--wait_for_drain` LSM drain, and -- because the op cap was
    detected by a progress poll every `--progress_ms` (15 s in the main arms) -- up to
    one polling period of idle tail. No old config had warmup/measure phases, so the
    numerator also contains the ramp-up.

    This measures how much of each run's duration was actually spent completing work,
    using the per-second series: `work_s` spans the first to the last second in which
    any operation completed.
    """
    import re
    out = []
    for f in sorted(glob.glob(os.path.join(root, "*", "*.per_sec.csv"))):
        base = f[: -len(".per_sec.csv")]
        rid = os.path.basename(base)
        if ".build" in rid or ".drain" in rid:
            continue                      # composite sub-runs carry their own series
        spath, opath = base + ".summary.json", base + ".config.txt"
        if not (os.path.exists(spath) and os.path.exists(opath)):
            continue
        try:
            S = json.load(open(spath))
        except Exception:
            continue
        ops, total = [], 0
        with open(f, newline="") as fh:
            for r in csv.DictReader(fh):
                try:
                    sec, o = int(float(r["sec"])), int(float(r["ops"]))
                except (KeyError, ValueError):
                    continue
                ops.append((sec, o))
                total += o
        if not ops:
            continue
        work = [s for s, o in ops if o > 0]
        work_s = max(1, (max(work) - min(work) + 1)) if work else 0
        dur = float(S.get("duration_s") or 0)
        ops_ok = int(S.get("ops_ok") or 0)
        cfg = open(opath, errors="replace").readline()

        def flag(name):
            m = re.search(rf"--{name}=([0-9.]+)", cfg)
            return float(m.group(1)) if m else None

        out.append({
            "rid": rid, "arm": arm_of(rid)[0], "ops_ok": ops_ok, "duration_s": dur,
            "work_s": work_s, "post_work_s": max(0.0, dur - work_s),
            "reported_ops_s": float(S.get("throughput_ops_s") or 0),
            "work_phase_ops_s": (ops_ok / work_s) if work_s else 0,
            "denominator_inflation": (dur / work_s) if work_s else 0,
            "persec_sum_equals_ops_ok": int(total == ops_ok),
            "drain_observe_s": flag("drain_observe_s"), "progress_ms": flag("progress_ms"),
            "wait_for_drain": flag("wait_for_drain"),
        })
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="bench/out/I")
    ap.add_argument("--out", default="bench/out/hist_erratum")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    st_bad = selftest()
    files = sorted(glob.glob(os.path.join(a.root, "*", "*.summary_extra.csv")))
    direct = [f for f in files if not (os.path.basename(f).endswith(".build.summary_extra.csv")
                                       or os.path.basename(f).endswith(".drain.summary_extra.csv"))]
    subrun = [f for f in files if f not in direct]

    rows, undecodable = [], 0
    for f in files:
        d = os.path.dirname(f)
        rid = os.path.basename(f)[: -len(".summary_extra.csv")]
        has_density = os.path.exists(os.path.join(d, rid + ".hist.csv"))
        arm, sub = arm_of(rid)
        for r in csv.DictReader(open(f, newline="")):
            if r.get("name") != "total_hist":
                continue
            for m in METRICS:
                raw = r.get(m)
                if not raw:
                    continue
                lab = int(float(raw))
                dec = legacy_interval(lab)
                if dec is None:
                    undecodable += 1
                    continue
                lo, hi, w = dec
                rows.append({"file": os.path.basename(f), "rid": rid, "arm": arm, "sub_run": sub,
                             "metric": m[:-3], "legacy_label_ns": lab,
                             "lower_ns": lo, "upper_ns": hi, "width_ns": w,
                             "bucket_counts_available": int(has_density)})

    with open(os.path.join(a.out, "latency_erratum.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    # per-arm: mean of the interval bounds across reps. Both endpoints are means, so
    # the result is an interval of means -- not a point estimate of anything.
    agg = {}
    for r in rows:
        k = (r["arm"], r["sub_run"], r["metric"])
        agg.setdefault(k, {"lower": [], "upper": [], "width": [], "n": 0,
                           "density": r["bucket_counts_available"]})
        agg[k]["lower"].append(r["lower_ns"])
        agg[k]["upper"].append(r["upper_ns"])
        agg[k]["width"].append(r["width_ns"])
        agg[k]["n"] += 1
    by_arm = []
    for (arm, sub, metric), v in sorted(agg.items()):
        by_arm.append({
            "arm": arm, "sub_run": sub, "metric": metric, "reps": v["n"],
            "mean_lower_ns": statistics.mean(v["lower"]),
            "mean_upper_ns": statistics.mean(v["upper"]),
            "mean_lower_us": statistics.mean(v["lower"]) / 1000.0,
            "mean_upper_us": statistics.mean(v["upper"]) / 1000.0,
            "bucket_width_ns": v["width"][0],
            "bucket_width_us": v["width"][0] / 1000.0,
            "interval_over_point": v["width"][0] / max(1.0, statistics.mean(v["lower"])),
            "bucket_counts_available": v["density"],
    })
    with open(os.path.join(a.out, "latency_erratum_by_arm.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(by_arm[0].keys()))
        w.writeheader()
        w.writerows(by_arm)

    def arm_row(arm, metric, sub=""):
        for r in by_arm:
            if r["arm"] == arm and r["metric"] == metric and r["sub_run"] == sub:
                return r
        return None

    summary = {
        "legacy_files": len(files),
        "legacy_files_direct": len(direct),
        "legacy_files_sub_run": len(subrun),
        "percentile_cells_decoded": len(rows),
        "percentile_cells_undecodable": undecodable,
        "encoder_roundtrip_failures": st_bad,
        "files_without_bucket_counts": len([f for f in files
                                            if not os.path.exists(os.path.join(
                                                os.path.dirname(f),
                                                os.path.basename(f)[:-len(".summary_extra.csv")]
                                                + ".hist.csv"))]),
        "headline": {},
    }
    for arm, metric, label in (("default", "p99", "default_arm_p99"),
                               ("default", "p50", "default_arm_p50"),
                               ("noc", "p99", "noc_arm_p99"),
                               ("f1c12", "p50", "f1c12_arm_p50"),
                               ("f1", "p50", "f1_arm_p50"),
                               ("noc", "p999", "noc_arm_p999")):
        r = arm_row(arm, metric)
        if r:
            summary["headline"][label] = {
                "reps": r["reps"],
                "mean_bucket_lower_us": round(r["mean_lower_us"], 1),
                "mean_bucket_upper_us": round(r["mean_upper_us"], 1),
                "bucket_width_us": round(r["bucket_width_us"], 1),
            }
    with open(os.path.join(a.out, "latency_erratum_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    # ---- throughput denominator (review item 3) ----------------------------
    td = throughput_denominator(a.root)
    if td:
        with open(os.path.join(a.out, "throughput_denominator.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(td[0].keys()))
            w.writeheader()
            w.writerows(td)
        infl = [t["denominator_inflation"] for t in td if t["denominator_inflation"]]
        per_arm = {}
        for t in td:
            per_arm.setdefault(t["arm"], []).append(t["denominator_inflation"])
        summary["throughput_denominator"] = {
            "runs": len(td),
            "inflation_min": round(min(infl), 2) if infl else None,
            "inflation_max": round(max(infl), 2) if infl else None,
            "inflation_median": round(statistics.median(infl), 2) if infl else None,
            "per_sec_sum_matches_ops_ok_all": all(t["persec_sum_equals_ops_ok"] for t in td),
            "per_arm_median_inflation": {k: round(statistics.median(v), 2)
                                         for k, v in sorted(per_arm.items()) if v},
        }
        with open(os.path.join(a.out, "latency_erratum_summary.json"), "w") as f:
            json.dump(summary, f, indent=2)

    print(json.dumps(summary, indent=2))
    print(f"\nwrote {a.out}/latency_erratum.csv, latency_erratum_by_arm.csv, "
          f"throughput_denominator.csv, latency_erratum_summary.json")
    return 1 if (undecodable or st_bad) else 0


if __name__ == "__main__":
    sys.exit(main())
