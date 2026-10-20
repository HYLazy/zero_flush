#!/usr/bin/env python3
"""Run validity gate + return-content inventory for the perf_v2 campaign.

Two jobs, both from the runbook:

  §6  decide whether a run may enter the campaign's means. A run is INVALID when
      counts do not conserve, verification fails, the tool reports a broken
      invariant, the box ran out of disk, the run was censored, or a background
      process interfered. Anything unproven is reported as a failure, not excused.

  §7  check that the per-run directory carries every file the runbook requires
      before it is shipped back.

Usage:
  check_run.py --run-dir DIR --run-id ID --rc N [--verify] [--json]

Prints a human-readable report and a final `VALID=1` / `VALID=0` line, which is
what bench/v2/lib.sh greps for. Exit status is 0 when valid, 1 otherwise.
"""
import argparse
import glob
import json
import os
import re
import sys

REQUIRED_GLOBS = [
    "*.config.txt",
    "*.measure_phase.json",
    "*.summary.json",
    "*.per_sec.csv",
    "*.hist_*.csv",        # per request class (runbook §2.3/§7); the aggregate
                           # <prefix>.hist.csv alone does not satisfy this
    "*.detail_*.csv",      # per request class, sampled raw timestamps
    "*.samples.csv",
    "*.events.csv",
    "*.verify.log",
]
MANIFEST_NAMES = ("manifest.jsonl",)


def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def find_one(rdir, rid, suffix):
    p = os.path.join(rdir, f"{rid}{suffix}")
    return p if os.path.exists(p) else None


def argv_flag_conflicts(cfg_path):
    """Flags given more than once with DIFFERENT values in one run's argv.

    The driver assigns on each occurrence, so the last one wins. A matrix that sweeps
    a parameter and then appends a fixed default silently replaces the swept value --
    which is exactly how the first formal M2 pass collapsed its thread and cache sweeps
    into a single configuration while every run still looked valid.
    """
    try:
        first = open(cfg_path, errors="replace").readline().strip()
    except Exception:
        return {}
    if not first.startswith("argv:"):
        return {}
    seen = {}
    for tok in first[len("argv:"):].split():
        if not tok.startswith("--") or "=" not in tok:
            continue
        k, v = tok[2:].split("=", 1)
        seen.setdefault(k, []).append(v)
    return {k: v for k, v in seen.items() if len(set(v)) > 1}


def find_manifest(rdir):
    for d in (rdir, os.path.dirname(rdir.rstrip("/")), os.path.dirname(os.path.dirname(rdir.rstrip("/")))):
        for n in MANIFEST_NAMES:
            p = os.path.join(d, n)
            if os.path.exists(p):
                return p
    return None


def parse_verify(vpath):
    """Return (rc, checked, ok, missing, badvalue) or None when not run."""
    if not vpath or not os.path.exists(vpath):
        return None
    # A run may verify in several passes (different regions carry different value salts),
    # so EVERY pass has to come back clean: keeping only the last one would hide a
    # failure in an earlier region.
    rcs, bads, missing_total, checked_total = [], [], 0, 0
    with open(vpath, errors="replace") as f:
        for line in f:
            line = line.strip()
            if re.match(r"^\w*_?rc=", line):
                try:
                    rcs.append(int(line.split("=", 1)[1]))
                except ValueError:
                    rcs.append(-1)
            elif line.startswith("VERIFY "):
                for tok in line.split()[1:]:
                    if "=" not in tok:
                        continue
                    k, v = tok.split("=", 1)
                    try:
                        iv = int(v)
                    except ValueError:
                        continue
                    if k == "checked":
                        checked_total += iv
                    elif k == "missing":
                        missing_total += iv
                    elif k == "badvalue":
                        bads.append(iv)
    rc = max(rcs) if rcs else None
    bad = sum(bads) if bads else None
    return (rc, checked_total or None, None, missing_total or None, bad)


def scan_stage(stage_dir):
    """Report conflicting duplicate flags across every run in a stage."""
    import glob as _glob
    bad, total, keys = [], 0, {}
    for cfg in sorted(_glob.glob(os.path.join(stage_dir, "*.config.txt"))):
        if ".verify.config.txt" in cfg:
            continue
        total += 1
        conf = argv_flag_conflicts(cfg)
        if conf:
            bad.append((os.path.basename(cfg), conf))
            for k in conf:
                keys[k] = keys.get(k, 0) + 1
    print(f"stage {stage_dir}: {total} run configs, {len(bad)} with conflicting flags")
    for k, c in sorted(keys.items(), key=lambda kv: -kv[1]):
        print(f"  --{k}: in {c} configs")
    for name, conf in bad[:10]:
        print(f"  e.g. {name}: " + ", ".join(f"--{k}={v}" for k, v in conf.items()))
    if len(bad) > 10:
        print(f"  ... and {len(bad) - 10} more")
    return 0 if not bad else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scan-stage", default=None,
                    help="scan a stage directory for conflicting duplicate flags and exit")
    ap.add_argument("--run-dir")
    ap.add_argument("--run-id")
    ap.add_argument("--rc", type=int, default=0)
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--no-cohort", action="store_true",
                    help="the run deliberately has no foreground measurement window "
                         "(e.g. experiment B: compaction enabled, no load), so the cohort "
                         "checks do not apply")
    ap.add_argument("--min-free-gb", type=float, default=150.0)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    if a.scan_stage:
        return scan_stage(a.scan_stage)
    if not a.run_dir or not a.run_id:
        ap.error("--run-dir and --run-id are required (or --scan-stage)")

    rdir, rid = a.run_dir, a.run_id
    reasons = []       # hard invalidity
    notes = []         # recorded, not disqualifying
    checks = {}

    def check(name, ok, why):
        checks[name] = bool(ok)
        if not ok:
            reasons.append(why)
        return ok

    # ---- §7 inventory ------------------------------------------------------
    missing = []
    req = list(REQUIRED_GLOBS)
    if a.no_cohort:
        # experiment B settles with compaction but issues no requests, so the artifacts
        # that only exist when something was measured are not part of its return content
        req = [g for g in req if g not in ("*.hist_*.csv", "*.detail_*.csv", "*.per_sec.csv")]
    for g in req:
        hits = glob.glob(os.path.join(rdir, g))
        if g == "*.hist_*.csv":
            # a legacy re-encode is not a measurement artifact
            hits = [h for h in hits if not h.endswith("_legacy.csv")]
        if not hits:
            missing.append(g)
    man = find_manifest(rdir)
    if not man:
        missing.append("manifest.jsonl")
    checks["files_present"] = not missing
    if missing:
        reasons.append("missing required return content: " + ", ".join(missing))

    # ---- driver restart / crash -------------------------------------------
    check("rc_zero", a.rc == 0, f"driver exit code {a.rc}")

    runlog = find_one(rdir, rid, ".run.log")
    if runlog:
        txt = open(runlog, errors="replace").read()
        check("no_oom", "bad_alloc" not in txt and "Out of memory" not in txt,
              "OOM/bad_alloc observed in run log")
        check("no_disk_low", "DISK-LOW" not in txt, "disk-low condition observed")

    # ---- measured summary and its invariants ------------------------------
    spath = find_one(rdir, rid, ".summary.json")
    S = load_json(spath) if spath else None
    if S is None:
        reasons.append("summary.json missing or unparsable")
        checks["summary_present"] = False
    else:
        checks["summary_present"] = True
        check("count_conservation",
              S.get("invariant_ops_done_eq_ok_plus_err") == 1,
              "ops_done != ops_ok + ops_err")
        check("read_write_split",
              S.get("invariant_read_plus_write_eq_total") == 1,
              "reads_done + writes_done != ops_done")
        check("timestamp_order",
              S.get("invariant_timestamp_order") == 1,
              f"timestamp ordering violated {S.get('invariant_ts_order_violations')} times")
        check("persec_cohort_sum",
              S.get("invariant_per_sec_sum_eq_cohort") == 1,
              "sum(per_sec cohort) != measurement cohort count")
        cens = S.get("censored", 0)
        if (S.get("hist_encoding") or "") != "lower_bound_v2":
            notes.append(f"hist_encoding={S.get('hist_encoding')!r} (not lower_bound_v2)")

    # ---- measurement window ----------------------------------------------
    mpath = find_one(rdir, rid, ".measure_phase.json")
    M = load_json(mpath) if mpath else None
    if M is None:
        reasons.append("measure_phase.json missing or unparsable")
        checks["measure_phase_present"] = False
    else:
        checks["measure_phase_present"] = True
        wl = M.get("measure_window_s") or 0
        if not a.no_cohort:
            check("measure_window_positive", wl > 0, "measurement window is not positive")
        if a.no_cohort:
            notes.append("no-cohort run: cohort checks skipped by request (settling run)")
        elif M.get("cohort_mode") == 1:
            check("cohort_nonempty", (M.get("cohort_ops") or 0) > 0, "cohort is empty")
        else:
            notes.append("cohort_mode=0 (legacy whole-run semantics; warmup/measure unused)")
        # A censored request is an ADMITTED request that did not finish before the drain
        # ended. Runbook §6 invalidates a run when in-flight requests are left
        # UNACCOUNTED, not when the deadline simply cut them off -- overload (queueing,
        # backpressure, a throughput plateau) is a result, not a defect. So the
        # requirement is an audit: everything admitted is either completed or
        # explicitly censored and reported.
        cens = M.get("censored")
        if a.no_cohort and (cens is None or cens < 0):
            notes.append("no-cohort run: censoring audit not applicable (no cutoff)")
        elif cens and cens > 0:
            sub, comp = M.get("cohort_submitted"), M.get("cohort_completed")
            if sub is not None and comp is not None:
                check("censored_accounted", sub == comp + cens,
                      f"cohort_submitted {sub} != cohort_completed {comp} + censored {cens}")
            else:
                check("censored_accounted", False,
                      "censored > 0 but cohort_submitted/cohort_completed are not reported")
            notes.append(f"censored={cens} (admitted, unfinished at drain end; "
                         f"inflight_at_cutoff={M.get('inflight_at_cutoff')})")
        # Overload evidence is reported, never used to reject a run.
        if M.get("offered_ops_s"):
            notes.append(f"offered={M['offered_ops_s']:.0f}/s submitted={M.get('cohort_submitted')} "
                         f"completed={M.get('cohort_completed')} "
                         f"client_wait_p99_us={M.get('client_wait_p99_us')}")

    # ---- data verification -------------------------------------------------
    if a.verify:
        v = parse_verify(find_one(rdir, rid, ".verify.log"))
        if v is None:
            reasons.append("--verify requested but verify.log missing")
            checks["verify_ran"] = False
        else:
            rc, checked, ok, missing_v, bad = v
            checks["verify_ran"] = rc == 0
            if rc != 0:
                reasons.append(f"verify exit code {rc}")
            if bad not in (None, 0):
                reasons.append(f"{bad} values failed content check")
            if missing_v not in (None, 0):
                notes.append(f"verify reported {missing_v} missing keys over the strided sweep")
            if checked:
                notes.append(f"verify checked={checked} ok={ok} missing={missing_v} bad={bad}")

    # ---- background interference marker ------------------------------------
    marker = os.path.join(rdir, "background_interference.txt")
    if os.path.exists(marker):
        reasons.append("background interference recorded during the run: "
                       + open(marker, errors="replace").read().strip())
    checks["no_background_interference"] = not os.path.exists(marker)

    # ---- the run must be the configuration its arm asked for -----------------
    cfgp = find_one(rdir, rid, ".config.txt")
    if cfgp:
        conf = argv_flag_conflicts(cfgp)
        # The offending flags belong in the notes, not in `checks`: every entry of
        # `checks` is printed as PASS/FAIL, so storing a list there renders an empty
        # list as "FAIL" and makes a valid run look broken.
        check("no_conflicting_flags", not conf,
              "flag(s) given twice with different values, last one silently wins: "
              + ", ".join(f"--{k}={v}" for k, v in conf.items()))
        if conf:
            notes.append("conflicting flags: " + ", ".join(f"--{k}={v}" for k, v in conf.items()))
    # ---- instrument identity must be stable across the run -----------------
    tool_changed = os.path.join(rdir, "tool_changed.txt")
    if os.path.exists(tool_changed):
        reasons.append("measurement instrument was rebuilt while the run was in "
                       "flight: " + open(tool_changed, errors="replace").read().replace("\n", " "))
    checks["tool_unchanged"] = not os.path.exists(tool_changed)

    valid = not reasons
    if a.json:
        json.dump({"run_id": rid, "valid": valid, "reasons": reasons, "notes": notes,
                   "checks": checks, "missing": missing}, sys.stdout, indent=2)
        print()
    else:
        print(f"run_id={rid}")
        print(f"rc={a.rc}")
        for k in sorted(checks):
            print(f"  check {k}: {'PASS' if checks[k] else 'FAIL'}")
        if missing:
            print(f"  missing files: {', '.join(missing)}")
        for n in notes:
            print(f"  note: {n}")
        for r in reasons:
            print(f"  REASON: {r}")
    print(f"VALID={1 if valid else 0}")
    return 0 if valid else 1


if __name__ == "__main__":
    sys.exit(main())
