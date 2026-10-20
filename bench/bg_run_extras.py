#!/usr/bin/env python3
"""Extra metrics for a fillcpu 100GB run: thread-state mix, throttle fractions,
cfstats flush/compaction CPU accounting, and a flush-job reliability check.

Usage: python3 bench/bg_run_extras.py <prefix>
"""
import collections
import csv
import json
import re
import sys


def main():
    P = sys.argv[1]
    d = json.load(open(P + ".summary.json"))
    # ---- thread states (thread_cpu.csv has a state column since 2026-10-07)
    st = collections.Counter()
    thread_cpu = collections.defaultdict(lambda: [0.0, 0.0, collections.Counter()])
    first = {}
    last = {}
    for r in csv.DictReader(open(P + ".thread_cpu.csv")):
        name = r["name"]
        role = ("flush" if name.startswith("rocksdb:high") else
                "comp" if name.startswith("rocksdb:low") else
                "writer" if name.startswith("fcw") else
                "driver" if name.startswith("fc-") else "other")
        st[(role, r.get("state", "?"))] += 1
        tid = r["tid"]
        v = (float(r["utime_ticks"]), float(r["stime_ticks"]))
        if tid not in first:
            first[tid] = v
        last[tid] = v
        thread_cpu[role][0] += 0  # placeholder
    print("=== thread state mix (samples) ===")
    for role in ("flush", "comp", "writer", "driver"):
        tot = {k: v for (r, k), v in st.items() if r == role}
        s = sum(tot.values())
        if s:
            print("  %-7s n=%7d  %s" % (role, s, {k: "%.0f%%" % (100 * v / s) for k, v in sorted(tot.items())}))
        if role == "driver":
            tot_d = s
    # ---- per-thread CPU totals by role (utime+stime deltas per tid)
    role_cpu = collections.defaultdict(float)
    name_of = {}
    for r in csv.DictReader(open(P + ".thread_cpu.csv")):
        name_of[r["tid"]] = r["name"]
    for tid in first:
        nm = name_of[tid]
        role = ("flush" if nm.startswith("rocksdb:high") else
                "comp" if nm.startswith("rocksdb:low") else
                "writer" if nm.startswith("fcw") else "driver")
        role_cpu[role] += (last[tid][0] - first[tid][0] + last[tid][1] - first[tid][1]) / 100.0
    print("=== thread CPU by role over whole run (s) ===")
    for k, v in sorted(role_cpu.items(), key=lambda kv: -kv[1]):
        print("  %-7s %.1f" % (k, v))

    # ---- throttle fractions (default delayed_write_rate cap = 16 MB/s)
    rows = sorted((float(r["elapsed"]), int(r["ops_cum"])) for r in csv.DictReader(open(P + ".samples.csv")))
    t_lo = t_hi = b_lo = b_hi = 0.0
    for a, b in zip(rows, rows[1:]):
        dt = b[0] - a[0]
        if dt <= 0:
            continue
        by = (b[1] - a[1]) * 1040
        if by / dt / 1e6 <= 16.5:
            t_lo += dt
            b_lo += by
        else:
            t_hi += dt
            b_hi += by
    print("=== throttle (rate <= 16.5 MB/s) ===")
    print("  wall share %.1f%%   data share %.1f%%   throttled avg %.1f MB/s   fast avg %.1f MB/s" % (
        100 * t_lo / (t_lo + t_hi), 100 * b_lo / (b_lo + b_hi),
        b_lo / t_lo / 1e6 if t_lo else 0, b_hi / t_hi / 1e6 if t_hi else 0))

    # ---- cfstats: High row = flush only; Low row = compaction; L0 row may mix
    txt = open(P + ".cfstats_drainend.txt").read()
    for line in txt.splitlines():
        t = line.split()
        if not t:
            continue
        if t[0] in ("High", "Low") and len(t) > 12:
            try:
                print("=== cfstats %-4s: jobs=%s Comp(sec)=%.1f CompMergeCPU(sec)=%.2f read=%.1fGB write=%.1fGB" % (
                    t[0], t[-6], float(t[-8]), float(t[-7]), float(t[4]), float(t[7])))
            except (ValueError, IndexError):
                pass
    m = re.search(r"Write Stall \(count\): (.*)", txt)
    if m:
        print("=== stall reasons:", m.group(1)[:400])

    # ---- flush job reliability: per-job CPU must be <= wall; sum <= thread CPU
    jobs = list(csv.DictReader(open(P + ".flush_jobs.csv")))
    bad = [r for r in jobs if float(r["cpu_us"]) > float(r["wall_us"]) + 50e3]
    job_cpu = sum(float(r["cpu_us"]) for r in jobs) / 1e6
    wall = d["jobs"]["flush_wall_s"]
    print("=== flush jobs: n=%d  job-window CPU sum=%.1fs (thread total=%.1fs)  rows cpu>wall: %d" % (
        len(jobs), job_cpu, role_cpu.get("flush", 0), len(bad)))
    print("    per-job wall sum=%.1fs  median wall=%.3fs  median cpu=%.3fs" % (
        wall, sorted(float(r["wall_us"]) for r in jobs)[len(jobs) // 2] / 1e6,
        sorted(float(r["cpu_us"]) for r in jobs)[len(jobs) // 2] / 1e6))


if __name__ == "__main__":
    main()
