#!/usr/bin/env python3
"""Sample per-thread state/CPU of a running wpbench process.

Usage: wp_sample.py <logfile> <seconds> [wpbench args...]
Prints per-thread CPU accrual and the fraction of samples in each scheduler
state (R = running/spinning, S = sleeping), which separates userspace spin
from blocking waits.
"""
import os
import subprocess
import sys
import time

ROOT = "/home/embed/hyl/metadata_offload"


def parse_stat(path):
    try:
        s = open(path).read()
    except OSError:
        return None
    rp = s.rfind(")")
    lp = s.find("(")
    if rp < 0 or lp < 0:
        return None
    comm = s[lp + 1:rp]
    f = s[rp + 2:].split()
    return comm, f[0], int(f[11]), int(f[12])  # name, state, utime, stime


def main():
    logfile, seconds = sys.argv[1], float(sys.argv[2])
    args = sys.argv[3:]
    log = open(logfile, "w")
    p = subprocess.Popen([os.path.join(ROOT, "bench/tools/wpbench")] + args,
                         stdout=log, stderr=subprocess.STDOUT,
                         env=dict(os.environ, LD_LIBRARY_PATH=os.path.join(ROOT, "source/rocksdb/build")))
    t0 = time.time()
    firsts = {}
    lasts = {}
    states = {}
    samples = 0
    while time.time() - t0 < seconds and p.poll() is None:
        taskdir = "/proc/%d/task" % p.pid
        try:
            tids = [t for t in os.listdir(taskdir) if t.isdigit()]
        except OSError:
            break
        for t in tids:
            r = parse_stat(os.path.join(taskdir, t, "stat"))
            if not r:
                continue
            name, state, u, st = r
            states.setdefault(name, {}).setdefault(state, 0)
            states[name][state] += 1
            if t not in firsts:
                firsts[t] = (name, u, st)
            lasts[t] = (name, u, st)
        samples += 1
        time.sleep(0.2)
    if p.poll() is None:
        p.terminate()
        try:
            p.wait(5)
        except subprocess.TimeoutExpired:
            p.kill()
    dur = time.time() - t0
    print("wall=%.1fs samples=%d" % (dur, samples))
    # aggregate by name using the first and last sample captured while running
    first = {}
    last = {}
    for name, vals in firsts.items():
        pass
    agg = {}
    for t, (name, u0, s0) in firsts.items():
        if t not in lasts:
            continue
        u1, s1 = lasts[t][1], lasts[t][2]
        a = agg.setdefault(name, [0.0, 0.0, 0])
        a[0] += (u1 - u0) / 100.0
        a[1] += (s1 - s0) / 100.0
        a[2] += 1
    print("%-14s %6s %10s %10s %10s   state-mix (R=spin/S=block/D=io)" % (
        "thread-name", "count", "utime_s", "stime_s", "cpu_s"))
    for name in sorted(agg):
        u, s, n = agg[name]
        mix = ",".join("%s:%d" % (k, v) for k, v in sorted(states.get(name, {}).items()))
        print("%-14s %6d %10.2f %10.2f %10.2f   %s" % (name, n, u, s, u + s, mix))


if __name__ == "__main__":
    main()
