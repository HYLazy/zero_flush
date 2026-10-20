#!/usr/bin/env python3
"""Side-by-side comparison of two fillcpu 1KB/100GB runs (e.g. bg_jobs=2 vs 8).

Usage: python3 bench/fillcpu_compare_bg.py <prefix_A> <prefix_B> [labelA labelB]

Reports per run: throughput/wall/CPU by role, throttle-fraction of the wall time
(rate <= 16.5 MB/s = default delayed_write_rate cap), WAF, stall reasons, memory
peaks/components, per-op CPU, plus perf self-time tops if <prefix>/perf exists.
"""
import csv
import glob
import json
import os
import re
import sys

CAP_MBPS = 16.5


def load(prefix):
    with open(prefix + ".summary.json") as f:
        s = json.load(f)
    with open(prefix + ".samples.csv") as f:
        samples = list(csv.DictReader(f))
    cf = ""
    for k in ("cfstats_drainend", "cfstats_fillend"):
        p = prefix + "." + k + ".txt"
        if os.path.exists(p):
            cf += open(p).read()
    return {"prefix": prefix, "s": s, "samples": samples, "cf": cf}


def throttle_fraction(samples):
    rows = sorted((float(r["elapsed"]), int(r["ops_cum"])) for r in samples)
    t_lo = t_hi = b_lo = b_hi = 0.0
    for a, b in zip(rows, rows[1:]):
        dt = b[0] - a[0]
        if dt <= 0:
            continue
        by = (b[1] - a[1]) * 1040
        if by / dt / 1e6 <= CAP_MBPS:
            t_lo += dt
            b_lo += by
        else:
            t_hi += dt
            b_hi += by
    tot = t_lo + t_hi
    return (t_lo / tot if tot else 0.0, b_lo / (b_lo + b_hi) if b_lo + b_hi else 0.0)


def mem_peaks(samples):
    def g(r, k):
        try:
            return float(r[k])
        except (KeyError, TypeError, ValueError):
            return 0.0

    fill = [r for r in samples if int(r["phase"]) == 1]
    last = fill[-1] if fill else samples[-1]
    return {
        "rss_peak": max(g(r, "rss_kb") for r in samples) / 1024,
        "hwm": max(g(r, "hwm_kb") for r in samples) / 1024,
        "memtbl_peak": max(g(r, "mem_cur_all") for r in samples) / 1048576,
        "treaders_peak": max(g(r, "table_readers") for r in samples) / 1048576,
        "bcache_peak": max(g(r, "bcache_usage") for r in samples) / 1048576,
        "fillend_memtbl": g(last, "mem_cur_all") / 1048576,
        "fillend_treaders": g(last, "table_readers") / 1048576,
        "fillend_bcache": g(last, "bcache_usage") / 1048576,
        "fillend_mall_used": g(last, "mall_used") / 1048576,
        "fillend_arena": g(last, "mall_arena") / 1048576,
    }


def stall_reasons(cf):
    m = re.search(r"Write Stall \(count\): (.*)", cf)
    out = {}
    if not m:
        return out
    for part in m.group(1).split(","):
        if ":" in part:
            k, v = part.split(":")
            try:
                out[k.strip()] = int(v.strip())
            except ValueError:
                pass
    return out


def perf_top(prefix, role, n=8):
    cands = [os.path.join(prefix, "perf"), os.path.join(os.path.dirname(prefix), "perf")]
    files = []
    for c in cands:
        files += glob.glob(os.path.join(c, "perf_%s_r*.txt" % role))
    files = sorted(files)
    agg, tot = {}, 0.0
    for f in files:
        for line in open(f):
            m = re.match(r"\s+([0-9.]+)%\s+\S+\s+\S+\s+\[.\] (.*)", line)
            if not m:
                continue
            pct, sym = float(m.group(1)), m.group(2).strip()
            agg[sym] = agg.get(sym, 0.0) + pct
            tot += pct
    if not agg or tot <= 0:
        return []
    return sorted(((v / tot * 100, k) for k, v in agg.items()), reverse=True)[:n]


def main():
    pa, pb = sys.argv[1], sys.argv[2]
    la = sys.argv[3] if len(sys.argv) > 3 else "A"
    lb = sys.argv[4] if len(sys.argv) > 4 else "B"
    A, B = load(pa), load(pb)
    sa, sb = A["s"], B["s"]
    fa, fb = sa["phase_fill"], sb["phase_fill"]
    da, db = sa["phase_drain"], sb["phase_drain"]
    ja, jb = sa["jobs"], sb["jobs"]
    ops = max(sa["ops_ok"], 1)
    app_a = sa["workload"]["app_bytes"]
    app_b = sb["workload"]["app_bytes"]

    ta_share, da_share = throttle_fraction(A["samples"])
    tb_share, db_share = throttle_fraction(B["samples"])
    ma, mb = mem_peaks(A["samples"]), mem_peaks(B["samples"])
    ra, rb = stall_reasons(A["cf"]), stall_reasons(B["cf"])

    rows = []

    def add(name, va, vb, fmt="%.2f"):
        rows.append((name, fmt % va, fmt % vb))

    add("bg_jobs", float(sa.get("bg_jobs", 2) or 2), float(sb.get("bg_jobs", 2) or 2), "%d")
    add("fill wall (s)", sa["fill_wall_s"], sb["fill_wall_s"], "%.1f")
    add("fill rate (MB/s)", sa["fill_rate_mb_s"], sb["fill_rate_mb_s"], "%.1f")
    add("drain wall (s)", sa["drain_wall_s"], sb["drain_wall_s"], "%.1f")
    add("ops/s", sa["fill_rate_ops_s"], sb["fill_rate_ops_s"], "%.0f")
    for k in ("writer", "flush_pool", "compaction_pool", "driver", "other"):
        add("fill CPU %s (s)" % k, fa["cpu_s"][k], fb["cpu_s"][k], "%.1f")
    add("fill CPU total (s)", fa["cpu_s_total"], fb["cpu_s_total"], "%.1f")
    add("drain CPU total (s)", da["cpu_s_total"], db["cpu_s_total"], "%.1f")
    add("writer CPU us/op", fa["cpu_s"]["writer"] * 1e6 / ops, fb["cpu_s"]["writer"] * 1e6 / ops)
    add("flush CPU total (s)", fa["cpu_s"]["flush_pool"] + da["cpu_s"]["flush_pool"],
        fb["cpu_s"]["flush_pool"] + db["cpu_s"]["flush_pool"], "%.1f")
    add("flush jobs", float(ja["flush_jobs"]), float(jb["flush_jobs"]), "%d")
    add("flush job CPU (s)", ja["flush_cpu_s"], jb["flush_cpu_s"], "%.1f")
    add("comp jobs", float(ja["comp_jobs"]), float(jb["comp_jobs"]), "%d")
    add("comp CPU rdb (s)", ja["comp_cpu_s_rdb"], jb["comp_cpu_s_rdb"], "%.1f")
    add("comp in (GB)", ja["comp_in_bytes"] / 1e9, jb["comp_in_bytes"] / 1e9, "%.1f")
    add("WAF comp_in/app", ja["comp_in_bytes"] / app_a, jb["comp_in_bytes"] / app_b, "%.2fx")
    add("wall share <=16.5MB/s", ta_share * 100, tb_share * 100, "%.1f%%")
    add("data share <=16.5MB/s", da_share * 100, db_share * 100, "%.1f%%")
    add("stall events begin/end", float(sa["stalls"]["begin"]), float(sb["stalls"]["begin"]), "%d")
    for k in sorted(set(ra) | set(rb)):
        if k.startswith(("total", "memtable-limit", "l0-file-count", "pending-compaction")):
            add("stall: " + k, float(ra.get(k, 0)), float(rb.get(k, 0)), "%.0f")
    for k in ("rss_peak", "hwm", "memtbl_peak", "treaders_peak", "bcache_peak",
              "fillend_memtbl", "fillend_treaders", "fillend_bcache", "fillend_mall_used",
              "fillend_arena"):
        add("mem %s (MB)" % k, ma[k], mb[k], "%.0f")

    w = max(len(r[0]) for r in rows) + 2
    print("%-*s %15s %15s" % (w, "metric", la, lb))
    print("-" * (w + 34))
    for r in rows:
        print("%-*s %15s %15s" % (w, r[0], r[1], r[2]))

    for role in ("flush", "comp", "writer"):
        xa, xb = perf_top(A["prefix"], role), perf_top(B["prefix"], role)
        if not xa and not xb:
            continue
        print("\nperf self-time top-8 (%s), %% of samples:  A=%s  B=%s" % (role, la, lb))
        dda = {k: v for v, k in xa}
        ddb = {k: v for v, k in xb}
        for sym in list(dict.fromkeys([k for _, k in xa] + [k for _, k in xb])):
            print("  %-64.64s A=%6.2f B=%6.2f" % (sym, dda.get(sym, 0.0), ddb.get(sym, 0.0)))


if __name__ == "__main__":
    main()
