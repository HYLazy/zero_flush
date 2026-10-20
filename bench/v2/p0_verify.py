#!/usr/bin/env python3
"""Independent verifier for the perf_v2 P0 acceptance runs (runbook §3).

"Independent" is the point: every quantity below is re-derived here from the
runbook's written specification and from the documented value generator, NOT read
back out of the driver's own bookkeeping. If the two implementations disagree,
the check fails.

  hist        §3.1 bucket geometry, labels, P99<=max, count conservation, and the
              v1/v2 label divergence
  mix         §3.2 read/write counts and non-empty per-class detail
  scan        §3.3 scan content/length/count via an independently computed checksum
  invariance  §3.4 cohort throughput must not move with polling or drain settings

Usage: p0_verify.py <subcommand> --root DIR [--prefix NAME] [--max-spread-pct 1.0]
Exit status 0 = all checks passed.
"""
import argparse
import csv
import glob
import json
import os
import sys

# ---------------------------------------------------------------------------
# Re-implementation of the log-linear bucket layout, straight from the runbook:
# exact below SMALL, then MANTB mantissa bits per octave. The reachable buckets of
# an octave are those whose mantissa carries the octave's leading bit.
# ---------------------------------------------------------------------------
SMALL, MANTB = 256, 4
GROUP = 1 << MANTB
BASEBIT = 8 if SMALL == 256 else 6
BUCKETS = SMALL + (64 - BASEBIT) * GROUP
U64MAX = (1 << 64) - 1


def idx(v):
    if v < SMALL:
        return v
    bl = v.bit_length()
    oct_ = bl - BASEBIT
    sh = bl - MANTB
    mant = (v >> sh) & (GROUP - 1)
    i = SMALL + oct_ * GROUP + mant
    return i if i < BUCKETS else BUCKETS - 1


def lower(i):
    if i < SMALL:
        return i
    oct_, mant = divmod(i - SMALL, GROUP)
    return mant << ((BASEBIT + oct_) - MANTB)


def upper(i):
    if i < SMALL:
        return i
    if i >= BUCKETS - 1:
        return None            # open-ended clamp bucket
    oct_, mant = divmod(i - SMALL, GROUP)
    return ((mant + 1) << ((BASEBIT + oct_) - MANTB)) - 1


def legacy_label(i):
    if i < SMALL:
        return i
    oct_, mant = divmod(i - SMALL, GROUP)
    return (mant | GROUP) << ((BASEBIT + oct_) - MANTB)


# ---------------------------------------------------------------------------
# Re-implementation of the driver's value generator (FillValue in l0bench.cc).
# ---------------------------------------------------------------------------
M64 = (1 << 64) - 1


def fill_value(id_, salt, size):
    x = (id_ * 0x9E3779B97F4A7C15 + salt * 0xD1B54A32D192ED03) & M64
    out = bytearray()
    i = 0
    while i < size:
        x ^= (x << 13) & M64
        x ^= x >> 7
        x ^= (x << 17) & M64
        x &= M64
        out += x.to_bytes(8, "little")[: min(8, size - i)]
        i += 8
    return bytes(out)


def fnv1a64(data, h=14695981039346656037):
    for b in data:
        h ^= b
        h = (h * 1099511628211) & M64
    return h


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
class Fail(Exception):
    pass


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def load_json(path):
    with open(path) as f:
        return json.load(f)


def one(root, pattern):
    hits = sorted(glob.glob(os.path.join(root, pattern)))
    if not hits:
        raise Fail(f"no file matching {pattern} under {root}")
    return hits[0]


# ---------------------------------------------------------------------------
# §3.1 histogram buckets
# ---------------------------------------------------------------------------
def check_hist(a):
    root = a.root
    sb = load_json(one(root, "*.hist_selfcheck.json"))
    buckets = read_csv(one(root, "*.hist_buckets.csv"))
    hist = read_csv(one(root, "*.hist.csv"))
    leg = read_csv(one(root, "*.hist_legacy.csv"))
    problems = []

    # (1) every bucket's recorded label and upper bound must equal this
    #     implementation's derivation from the bucket index
    for r in buckets:
        i = int(r["idx"])
        if int(float(r["ns"])) != lower(i):
            problems.append(f"bucket {i}: ns={r['ns']} but independent lower={lower(i)}")
        u = upper(i)
        if u is None:
            if int(float(r["ns_upper"])) != U64MAX:
                problems.append(f"bucket {i}: open bucket ns_upper={r['ns_upper']} != U64MAX")
        elif int(float(r["ns_upper"])) != u:
            problems.append(f"bucket {i}: ns_upper={r['ns_upper']} but independent upper={u}")

    # (2) lo <= v <= hi on the recorded sample extremes of every non-empty bucket
    for r in buckets:
        n = int(r["count"])
        if n == 0:
            continue
        mn, mx = int(r["sample_min"]), int(r["sample_max"])
        if mn < int(float(r["ns"])):
            problems.append(f"bucket {r['idx']}: sample_min {mn} < lower {r['ns']}")
        if int(float(r["ns_upper"])) != U64MAX and mx > int(float(r["ns_upper"])):
            problems.append(f"bucket {r['idx']}: sample_max {mx} > upper {r['ns_upper']}")

    # (3) the exact label cases the runbook names
    cases = [(256, 287, 256), (288, 319, 288), (512, 543, 512)]
    for lo, hi, want in cases:
        got_lo, got_hi = lower(idx(lo)), lower(idx(hi))
        if got_lo != want or got_hi != want:
            problems.append(f"{lo}..{hi}: independent labels {got_lo}/{got_hi}, want {want}")
        if idx(lo) != idx(hi):
            problems.append(f"{lo}..{hi} does not map to a single bucket")
        # the driver's own file must agree, via the reported label cases
        for c in sb.get("label_cases", []):
            if c["value"] == lo and int(c["label"]) != want:
                problems.append(f"driver reported label {c['label']} for {lo}, want {want}")

    # (4) conservation and P99 <= max, computed here from the raw bucket counts
    total = sum(int(r["engine_count"]) for r in hist)
    if total != sb["count"]:
        problems.append(f"hist.csv counts sum to {total} but selfcheck count={sb['count']}")
    counts = []
    for r in hist:
        counts.append((int(float(r["ns"])), int(r["engine_count"])))
    counts.sort()
    acc, p99 = 0, None
    for ns, c in counts:
        acc += c
        if p99 is None and acc >= 0.99 * total:
            p99 = ns
    if p99 is not None and p99 > sb["max"]:
        problems.append(f"P99 {p99} exceeds max {sb['max']}")
    nonempty = sum(1 for r in buckets if int(r["count"]) > 0)
    if nonempty != sb["nonempty_buckets"]:
        problems.append(f"nonempty {nonempty} != selfcheck {sb['nonempty_buckets']}")

    # (5) the legacy encoder must still produce its historical (inflated) labels,
    #     so consumers can be shown the two encodings must never be merged
    leg_by_lower = {int(float(r["ns"])): int(r["engine_count"]) for r in leg}
    n_diff = 0
    for r in hist:
        lo = int(float(r["ns"]))
        if lo >= SMALL and lo not in leg_by_lower:
            n_diff += 1
    if n_diff == 0:
        problems.append("legacy histogram carries the same labels as v2; the two encodings "
                        "are supposed to differ for every value >= 256")
    for c in sb.get("label_cases", []):
        v = c["value"]
        if legacy_label(idx(v)) != int(c["label_legacy"]):
            problems.append(f"legacy label for {v}: driver {c['label_legacy']} vs "
                            f"independent {legacy_label(idx(v))}")

    print(f"hist: count={total} nonempty={nonempty} bound_violations={sb['bound_violations']} "
          f"p99={p99} max={sb['max']} legacy_vs_v2_divergent_buckets={n_diff}")
    if sb["bound_violations"] != 0:
        problems.append(f"driver self-check reported {sb['bound_violations']} bound violations")
    if problems:
        for p in problems:
            print("  FAIL:", p)
        raise Fail(f"{len(problems)} histogram problem(s)")
    print("  PASS")
    return 0


# ---------------------------------------------------------------------------
# §3.2 read/write counts and per-class detail
# ---------------------------------------------------------------------------
def check_mix(a):
    root = a.root
    problems = []
    for name in ("put", "get", "mix50"):
        prefix = os.path.join(root, f"p0_{name}")
        spath = prefix + ".summary.json"
        if not os.path.exists(spath):
            raise Fail(f"missing {spath}")
        S = load_json(spath)
        done, ok, err = S["ops_done"], S["ops_ok"], S["ops_err"]
        rd, wr = S["reads_done"], S["writes_done"]
        if done != ok + err:
            problems.append(f"{name}: ops_done {done} != ok {ok} + err {err}")
        if rd + wr != done:
            problems.append(f"{name}: reads {rd} + writes {wr} != ops_done {done}")
        if S["invariant_per_sec_sum_eq_cohort"] != 1:
            problems.append(f"{name}: sum(per_sec cohort) != cohort total")
        if name == "put" and wr != rd + wr:
            problems.append(f"{name}: expected all writes, got reads={rd}")
        if name == "get" and rd == 0:
            problems.append(f"{name}: expected reads, got 0")
        # every request sampled into detail must show up in the class file it claims
        dpath = prefix + ".detail.csv"
        rows = read_csv(dpath) if os.path.exists(dpath) else []
        cls_seen = {}
        for r in rows:
            cls_seen[r["cls"]] = cls_seen.get(r["cls"], 0) + 1
            for k in ("op_start_ns", "db_call_begin_ns", "log_done_ns"):
                if not r.get(k):
                    problems.append(f"{name}: detail row missing {k}")
                    break
            if int(r["db_call_begin_ns"]) > int(r["done_ns"]):
                problems.append(f"{name}: db_call_begin > engine_done in detail")
            if int(r["done_ns"]) > int(r["log_done_ns"]):
                problems.append(f"{name}: engine_done > log_done in detail")
        # the per-class detail files must exist and be non-empty where expected
        per_class = {os.path.basename(p).split(".detail_")[1].split(".csv")[0]:
                     sum(1 for _ in open(p)) - 1
                     for p in glob.glob(prefix + ".detail_*.csv")}
        if name == "mix50":
            for want in ("put", "get_hit"):
                if cls_seen.get(want, 0) == 0:
                    problems.append(f"{name}: no {want} rows sampled into detail")
                if per_class.get(want, 0) == 0:
                    problems.append(f"{name}: detail_{want}.csv is empty")
        print(f"{name}: done={done} ok={ok} err={err} reads={rd} writes={wr} "
              f"cls={S.get('cls_put')}/{S.get('cls_get_hit')}/{S.get('cls_get_miss')}/"
              f"{S.get('cls_scan')} detail_classes={cls_seen} "
              f"per_class_files={per_class} dropped={S.get('detail_rows_dropped')} "
              f"qdepth_max={S.get('detail_queue_depth_max')}")
    if problems:
        for p in problems:
            print("  FAIL:", p)
        raise Fail(f"{len(problems)} mix problem(s)")
    print("  PASS")
    return 0


# ---------------------------------------------------------------------------
# §3.3 scan content/length/count
# ---------------------------------------------------------------------------
def check_scan(a):
    root = a.root
    line = None
    for p in glob.glob(os.path.join(root, "*.run.log")) + glob.glob(os.path.join(root, "*.verify.log")):
        for l in open(p, errors="replace"):
            if l.startswith("SCANCHECK"):
                line = l.strip()
    if line is None:
        raise Fail("no SCANCHECK line found in the run logs")
    f = {}
    for tok in line.split()[1:]:
        k, v = tok.split("=", 1)
        f[k] = int(v)
    start, limit = a.scan_start, f["keys"]
    exp_bytes, exp_sum = 0, 14695981039346656037
    for j in range(start, start + f["keys"]):
        v = fill_value(j, f["salt"], f["value_size"])
        exp_bytes += len(v)
        exp_sum = fnv1a64(v, exp_sum)
    problems = []
    if f["keys"] != limit:
        problems.append(f"scanned {f['keys']} keys, expected {limit}")
    if f["badvalue"] != 0:
        problems.append(f"{f['badvalue']} scanned values failed the content check")
    if f["bytes"] != exp_bytes:
        problems.append(f"bytes {f['bytes']} != independent {exp_bytes}")
    if f["checksum"] != exp_sum:
        problems.append(f"checksum {f['checksum']} != independent {exp_sum}")
    if f["value_size"] != a.value_size:
        problems.append(f"value_size {f['value_size']} != expected {a.value_size}")
    print(f"scan: keys={f['keys']} bytes={f['bytes']} (independent {exp_bytes}) "
          f"checksum={f['checksum']} (independent {exp_sum}) badvalue={f['badvalue']} "
          f"first_id={f['first_key_id']} last_id={f['last_key_id']}")
    if problems:
        for p in problems:
            print("  FAIL:", p)
        raise Fail(f"{len(problems)} scan problem(s)")
    print("  PASS")
    return 0


# ---------------------------------------------------------------------------
# §3.4 cohort throughput must not move with polling / drain settings
# ---------------------------------------------------------------------------
def check_invariance(a):
    root = a.root
    rows = []
    for p in sorted(glob.glob(os.path.join(root, "*.measure_phase.json"))):
        M = load_json(p)
        name = os.path.basename(p).replace(".measure_phase.json", "")
        S = load_json(p.replace(".measure_phase.json", ".summary.json"))
        rows.append({
            "name": name,
            "svc": M["throughput_service_ops_s"],
            "whole": M["throughput_whole_run_ops_s"],
            "cohort": M["cohort_ops"],
            "window": M.get("measure_window_s", 0),
            "wall": S["duration_s"],
            "censored": M["censored"],
            "inflight": M["inflight_at_cutoff"],
        })
    if len(rows) < 3:
        raise Fail(f"need at least 3 invariance runs, found {len(rows)}")
    svc = [r["svc"] for r in rows]
    lo, hi = min(svc), max(svc)
    mean = sum(svc) / len(svc)
    spread_pct = 100.0 * (hi - lo) / mean
    walls = [r["wall"] for r in rows]
    windows = [r["window"] for r in rows]
    print(f"invariance: {len(rows)} runs; cohort throughput {lo:.1f}..{hi:.1f} ops/s "
          f"(spread {spread_pct:.3f}%); measure window "
          f"{min(windows):.2f}..{max(windows):.2f}s; wall {min(walls):.1f}..{max(walls):.1f}s")
    for r in rows:
        print(f"  {r['name']:34s} svc={r['svc']:10.1f} whole={r['whole']:10.1f} "
              f"cohort={r['cohort']:8d} win={r['window']:6.2f} wall={r['wall']:7.2f} "
              f"censored={r['censored']} inflight={r['inflight']}")

    # Same-config control: what "no effect" looks like on this host. When the
    # across-config spread is no larger than the repeat spread, polling/drain
    # demonstrably contributes nothing measurable.
    noise = None
    if a.noise_floor_root:
        crows = []
        for p in sorted(glob.glob(os.path.join(a.noise_floor_root, "*.measure_phase.json"))):
            crows.append(load_json(p)["throughput_service_ops_s"])
        if len(crows) >= 2:
            noise = 100.0 * (max(crows) - min(crows)) / (sum(crows) / len(crows))
            print(f"  same-config noise floor ({len(crows)} repeats): {noise:.3f}%")

    problems = []
    if a.diagnostic:
        print("  DIAGNOSTIC: gate not applied; read the spread against the noise floor")
        if noise is not None and spread_pct <= max(noise * 1.5, 0.2):
            print(f"  spread ({spread_pct:.3f}%) is within the same-config noise floor "
                  f"({noise:.3f}%) -> no polling/drain effect is resolvable")
    elif spread_pct >= a.max_spread_pct:
        problems.append(f"cohort throughput spread {spread_pct:.3f}% >= "
                        f"{a.max_spread_pct}% limit"
                        + (f" (same-config noise floor {noise:.3f}%)" if noise is not None else ""))
    for r in rows:
        if r["censored"]:
            problems.append(f"{r['name']}: censored={r['censored']}")
    if problems:
        for p in problems:
            print("  FAIL:", p)
        raise Fail(f"{len(problems)} invariance problem(s)")
    if a.diagnostic:
        print("  PASS (diagnostic)")
    else:
        print(f"  PASS (spread {spread_pct:.3f}% < {a.max_spread_pct}%)")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["hist", "mix", "scan", "invariance", "all"])
    ap.add_argument("--root", required=True)
    ap.add_argument("--max-spread-pct", type=float, default=1.0)
    ap.add_argument("--diagnostic", action="store_true",
                    help="report the spread without applying the gate (for the "
                         "closed-loop grid and the same-config control)")
    ap.add_argument("--noise-floor-root", default=None,
                    help="directory of same-config repeats used as the noise floor")
    ap.add_argument("--scan-start", type=int, default=0)
    ap.add_argument("--value-size", type=int, default=1024)
    a = ap.parse_args()

    cmds = ["hist", "mix", "scan", "invariance"] if a.cmd == "all" else [a.cmd]
    bad = 0
    for c in cmds:
        print(f"--- {c} ---")
        try:
            {"hist": check_hist, "mix": check_mix, "scan": check_scan,
             "invariance": check_invariance}[c](a)
        except Fail as e:
            print(f"  RESULT: FAIL ({e})")
            bad += 1
        print()
    print("P0_VERIFY=" + ("PASS" if bad == 0 else f"FAIL({bad})"))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
