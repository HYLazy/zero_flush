#!/usr/bin/env python3
"""flushchar_analyze -- per-run budget table for the FLUSHCHAR campaign.

Usage: python3 bench/flushchar_analyze.py <out_prefix> [<out_prefix> ...]
       (e.g. bench/out/flushchar/r1_flushonly/run)

Prints, per run:
  * fill/drain wall, app rate, flush-pipeline utilization (is flush the
    binding constraint of the fill, or is the writer front end?)
  * per-flush-job budget: wall decomposed into thread CPU, fdatasync wait,
    write() wait, other; per-byte CPU intensity; implied per-thread capacity
  * device write/read bandwidth during fill + busy fraction + flush's share
    of device writes (needs the campaign's same-session ceiling for %)
  * memory: RSS/HWM, memtable peaks, table readers, page-cache Dirty/Writeback
  * stall accounting
"""
import csv
import json
import os
import statistics as st
import sys

SEC = 512  # diskstats sector bytes


def pct(v, p):
    s = sorted(v)
    return s[min(len(s) - 1, int(len(s) * p))]


def med(v):
    return st.median(v) if v else float("nan")


def analyze(prefix):
    name = os.path.basename(os.path.dirname(prefix.rstrip("/")))
    S = json.load(open(prefix + ".summary.json"))
    J = S["jobs"]
    F = S["phase_fill"]
    drain = S["phase_drain"]
    nflt = 1
    for tok in S["argv"].split():
        if tok.startswith("--max_bg_flushes="):
            nflt = int(tok.split("=")[1])

    # ---- flush job ledger (fill phase only; drain jobs counted separately)
    rows = [r for r in csv.DictReader(open(prefix + ".flush_jobs.csv"))]
    fill_jobs = [r for r in rows if r["phase"] == "1"]
    drain_jobs = [r for r in rows if r["phase"] == "2"]
    has_io = "io_fsync_ns" in rows[0] if rows else False

    def jobagg(jobs):
        out = {}
        n = len(jobs)
        out["n"] = n
        if not n:
            return out
        out["bytes"] = sum(float(r["data_bytes"]) for r in jobs)
        out["entries"] = sum(float(r["num_entries"]) for r in jobs)
        out["wall"] = sum(float(r["wall_us"]) for r in jobs) / 1e6
        out["cpu"] = sum(float(r["cpu_us"]) for r in jobs) / 1e6
        if has_io:
            # ~6% of multi-flush-thread jobs have wrapped (negative) io deltas:
            # the library resets the thread-local iostats_context mid-job.
            # Those rows are dropped; wall/cpu/bytes stay valid for all rows.
            sane = [r for r in jobs
                    if max(float(r["io_write_ns"]), float(r["io_fsync_ns"]),
                           float(r["io_cpu_write_ns"])) < (1 << 60)]
            out["io_dropped"] = n - len(sane)
            out["fsync"] = sum(float(r["io_fsync_ns"]) for r in sane) / 1e9
            out["write"] = sum(float(r["io_write_ns"]) for r in sane) / 1e9
            out["cpu_io"] = sum(float(r["io_cpu_write_ns"]) for r in sane) / 1e9
            # per-job clamped decomposition
            wait = cpuio = 0.0
            for r in sane:
                w = (float(r["io_write_ns"]) + float(r["io_fsync_ns"])) / 1e9
                c = float(r["io_cpu_write_ns"]) / 1e9
                wait += max(0.0, w - c)
                cpuio += min(c, w)
            out["io_wait"] = wait
            out["cpu_in_io"] = cpuio
            out["wall_io"] = sum(float(r["wall_us"]) for r in sane) / 1e6
        return out

    fj = jobagg(fill_jobs)
    dj = jobagg(drain_jobs)

    # ---- samples: device bandwidth, dirty pages, memory peaks (fill window)
    sm = list(csv.DictReader(open(prefix + ".samples.csv")))
    cols = sm[0].keys() if sm else {}
    fill_t0 = float(sm[0]["ts"])
    fill_t1 = fill_t0 + S["fill_wall_s"]
    fsm = [r for r in sm if float(r["ts"]) <= fill_t1 + 0.3]

    dev = []
    if "dev_wr_sectors" in cols:
        prev = None
        for r in fsm:
            t = float(r["ts"])
            if prev is not None:
                dt = t - prev[0]
                if dt > 0:
                    dev.append((
                        (float(r["dev_wr_sectors"]) - prev[1]) * SEC / dt / 1e6,
                        (float(r["dev_rd_sectors"]) - prev[2]) * SEC / dt / 1e6,
                        (float(r["dev_io_ticks_ms"]) - prev[3]) / (dt * 1000) * 100,
                    ))
            prev = (t, float(r["dev_wr_sectors"]), float(r["dev_rd_sectors"]),
                    float(r["dev_io_ticks_ms"]))
    wr = [d[0] for d in dev]
    rd = [d[1] for d in dev]
    busy = [d[2] for d in dev]

    dev_wr_bytes_total = ((float(sm[-1]["dev_wr_sectors"]) - float(sm[0]["dev_wr_sectors"])) * SEC
                          if "dev_wr_sectors" in cols else 0.0)
    dirty = [float(r["dirty_kb"]) for r in sm if r.get("dirty_kb") not in ("", None)]
    wback = [float(r["writeback_kb"]) for r in sm if r.get("writeback_kb") not in ("", None)]
    rss = [float(r["rss_kb"]) for r in sm]
    mcur = [float(r["mem_cur_all"]) for r in sm]
    msize = [float(r["mem_size_all"]) for r in sm]
    imm = [float(r["num_imm"]) for r in sm]
    tread = [float(r["table_readers"]) for r in sm]

    # ---- stalls
    states = {}
    try:
        for r in csv.DictReader(open(prefix + ".stalls.csv")):
            states[r["state"]] = states.get(r["state"], 0) + 1
    except FileNotFoundError:
        pass

    fill_wall = S["fill_wall_s"]
    fl_util = fj.get("wall", 0.0) / (fill_wall * nflt) if fj.get("wall") else 0.0

    print("=" * 100)
    print(f"RUN {name}   (fill {fill_wall:.1f}s @ {S['fill_rate_mb_s']:.0f} MB/s app, "
          f"drain {S['drain_wall_s']:.1f}s, {nflt} flush thread(s))")
    print(f"  flush jobs: fill={fj.get('n',0)} drain={dj.get('n',0)}  "
          f"flush bytes fill={fj.get('bytes',0)/1e9:.2f} GB  "
          f"stall_s={S['stalls']['stall_s']:.1f} ({S['stalls']['stall_s']/max(fill_wall,1e-9)*100:.0f}% of fill wall) "
          f"states={states}")
    print(f"  pipeline: flush-thread utilization = {fl_util*100:.0f}%  "
          f"({'FLUSH-LIMITED' if fl_util > 0.9 else 'WRITER/other-limited'})")
    if fj.get("n"):
        cpu_gb = fj["cpu"] / (fj["bytes"] / 1e9)
        med_bytes = med([float(r['data_bytes']) for r in fill_jobs])
        med_wall = med([float(r['wall_us']) for r in fill_jobs]) / 1e6
        med_cpu = med([float(r['cpu_us']) for r in fill_jobs]) / 1e6
        print(f"  per-job (median): {med_bytes/1048576:.1f} MB in {med_wall*1e3:.0f} ms "
              f"(cpu {med_cpu*1e3:.0f} ms = {med_cpu/med_wall*100:.0f}%)  "
              f"-> per-thread service rate {med_bytes/med_wall/1e6:.0f} MB/s")
        print(f"  aggregate budget of fill-phase flush wall ({fj['wall']:.1f} s):")
        if has_io:
            other_cpu = fj["cpu"] - fj.get("cpu_in_io", 0)
            other = fj["wall"] - fj["cpu"] - fj.get("io_wait", 0)
            wden = fj.get("wall_io", fj["wall"])  # wall of io-sane rows only
            note = (f"  [io columns on {fj.get('io_dropped',0)} dropped rows]"
                    if fj.get("io_dropped") else "")
            print(f"    thread CPU total      {fj['cpu']:7.1f} s  {fj['cpu']/fj['wall']*100:5.1f}%"
                  f"   (non-I/O compute {other_cpu:6.1f} s, inside write/fsync {fj.get('cpu_in_io',0):6.1f} s)")
            print(f"    fdatasync wall        {fj['fsync']:7.1f} s  {fj['fsync']/wden*100:5.1f}% of sane-job wall")
            print(f"    write() wall          {fj['write']:7.1f} s  {fj['write']/wden*100:5.1f}% of sane-job wall")
            print(f"    pure I/O wait (no CPU){fj.get('io_wait',0):7.1f} s  {fj.get('io_wait',0)/wden*100:5.1f}% of sane-job wall")
            print(f"    other (sched/locks)   {other:7.1f} s  {other/fj['wall']*100:5.1f}%{note}")
        print(f"  CPU intensity: {cpu_gb:.2f} s/GB  (= {1/cpu_gb:.2f} GB/s per core)  "
              f"flush CPU during fill = {F['cpu_s']['flush_pool']:.1f} s "
              f"({F['cpu_s']['flush_pool']/max(fill_wall,1e-9):.2f} cores)")
        print(f"  flush CPU share of process CPU: "
              f"{fj['cpu']/max(F['cpu_s_total'],1e-9)*100:.1f}%  "
              f"(process fill CPU {F['cpu_s_total']:.0f} s = "
              f"{F['cpu_s_total']/max(fill_wall,1e-9):.2f} cores of 36)")
    if wr:
        print(f"  device during fill: write mean/med/p95/max = {st.mean(wr):.0f}/"
              f"{med(wr):.0f}/{pct(wr,0.95):.0f}/{max(wr):.0f} MB/s   "
              f"read mean {st.mean(rd):.1f} MB/s   busy {st.mean(busy):.0f}%")
        if dev_wr_bytes_total > 0:
            print(f"  device writes total {dev_wr_bytes_total/1e9:.1f} GB; flush output share "
                  f"{J['flush_bytes']/max(dev_wr_bytes_total,1)*100:.1f}%")
    if dirty:
        print(f"  memory: RSS peak {max(rss)/1024:.0f} MB; memtable cur/size peak "
              f"{max(mcur)/1048576:.0f}/{max(msize)/1048576:.0f} MB; imm peak {max(imm):.0f}; "
              f"table readers {max(tread)/1048576:.0f} MB")
        print(f"  page cache: Dirty med/peak {med(dirty)/1024:.0f}/{max(dirty)/1024:.0f} MB; "
              f"Writeback med/peak {med(wback)/1024:.0f}/{max(wback)/1024:.0f} MB")
    else:
        print(f"  memory: RSS peak {max(rss)/1024:.0f} MB; memtable peak {max(msize)/1048576:.0f} MB "
              f"(no dirty_kb column)")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    for p in sys.argv[1:]:
        analyze(p)
