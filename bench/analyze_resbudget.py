#!/usr/bin/env python3
"""Summarize the resbudget arms: standard LSM path vs WAL-only + deferred replay."""
import csv
import json
import os
import re
import sys

ROOT = "/home/embed/hyl/metadata_offload"
OUT = os.path.join(ROOT, "bench/out/resbudget")
APP_BYTES_PER_OP = 16 + 1024


def rows(path):
    try:
        with open(path) as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return []


def load_summary(path):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def parse_wal_log(path):
    txt = ""
    try:
        txt = open(path).read()
    except FileNotFoundError:
        return {}
    m = re.search(r"wal done: bytes=(\d+) file_size=(\d+) wall=([0-9.]+)s rate=([0-9.]+) MB/s", txt)
    if not m:
        return {}
    return {
        "bytes": int(m.group(1)),
        "file_size": int(m.group(2)),
        "wall_s": float(m.group(3)),
        "rate_mb_s": float(m.group(4)),
    }


def parse_throttle(path):
    try:
        txt = open(path).read()
    except FileNotFoundError:
        return {}
    m = re.findall(r"\[iothrottle pid=\d+\] charged read=(\d+) MB write=(\d+) MB "
                   r"wait read=(\d+) ms write=(\d+) ms", txt)
    if not m:
        return {}
    r, w, rw, ww = m[-1]
    return {"charged_read_mb": int(r), "charged_write_mb": int(w),
            "wait_read_ms": int(rw), "wait_write_ms": int(ww)}


def parse_samples(path):
    try:
        f = open(path)
    except FileNotFoundError:
        return {}
    rows = list(csv.DictReader(f))
    f.close()
    if len(rows) < 2:
        return {}
    t0 = float(rows[0]["ts"])
    t1 = float(rows[-1]["ts"])
    dt = max(t1 - t0, 1e-9)
    wr = rd = 0.0
    busy = 0.0
    for a, b in zip(rows, rows[1:]):
        step = max(float(b["ts"]) - float(a["ts"]), 0.0)
        wr += float(b["dev_wr_mb_s"]) * step
        rd += float(b["dev_rd_mb_s"]) * step
        busy += float(b["dev_busy_pct"]) * step
    last = rows[-1]
    return {
        "duration_s": dt,
        "dev_write_mb_integral": wr,
        "dev_read_mb_integral": rd,
        "dev_busy_avg_pct": busy / dt if dt else 0,
        "dev_write_mean_mb_s": wr / dt if dt else 0,
        "dev_read_mean_mb_s": rd / dt if dt else 0,
        "dev_write_peak_mb_s": max(float(r["dev_wr_mb_s"]) for r in rows),
        "dev_read_peak_mb_s": max(float(r["dev_rd_mb_s"]) for r in rows),
        "proc_wchar_mb": float(last["proc_wchar_mb"]),
        "proc_rchar_mb": float(last["proc_rchar_mb"]),
        "proc_write_bytes_mb": float(last["proc_write_bytes_mb"]),
        "proc_read_bytes_mb": float(last["proc_read_bytes_mb"]),
        "proc_cpu_s": float(last["proc_cpu_s"]),
    }


def fill_profile(path):
    r = [x for x in rows(path) if x["phase"] == "1"]
    if not r:
        return {}
    stopped = sum(1 for x in r if x["write_stopped"] == "1")
    l0 = sorted(float(x["l0_files"]) for x in r)
    pend = sorted(float(x["pending_comp"]) for x in r)
    ops = [float(x["ops_cum"]) for x in r]
    t = [float(x["elapsed"]) for x in r]
    rates = [(ops[i] - ops[i - 1]) / (t[i] - t[i - 1]) for i in range(1, len(r))
             if t[i] > t[i - 1]]
    rates.sort()
    def q(a, frac):
        return a[min(len(a) - 1, int(frac * len(a)))] if a else 0
    return {
        "samples": len(r),
        "write_stopped_frac": stopped / len(r),
        "l0_median": q(l0, 0.5),
        "l0_max": l0[-1] if l0 else 0,
        "pending_median_gb": q(pend, 0.5) / 1e9,
        "pending_max_gb": pend[-1] / 1e9 if pend else 0,
        "ops_rate_median": q(rates, 0.5),
        "ops_rate_q1": q(rates, 0.25),
        "ops_rate_q3": q(rates, 0.75),
    }


def arm_a(name="A1"):
    d = os.path.join(OUT, name)
    s = load_summary(os.path.join(d, "fill.summary.json"))
    if not s:
        return {"status": "missing"}
    app = s.get("workload", {}).get("app_bytes", 0)
    fill = s.get("fill_wall_s", 0)
    drain = s.get("drain_wall_s", 0)
    stalls = s.get("stalls", {}).get("stall_s", 0)
    jobs = s.get("jobs", {})
    cpu = s.get("process_cpu_s_total", 0)
    sam = parse_samples(os.path.join(d, "samples.csv"))
    thr = parse_throttle(os.path.join(d, "run.log"))
    return {
        "status": "ok",
        "app_bytes": app,
        "ingest_wall_s": fill,
        "materialize_wall_s": drain,
        "total_wall_s": fill + drain,
        "ingest_rate_mb_s": s.get("fill_rate_mb_s", 0),
        "ingest_ops_s": s.get("fill_rate_ops_s", 0),
        "stall_s": stalls,
        "stall_frac": stalls / fill if fill else 0,
        "flush_jobs": jobs.get("flush_jobs", 0),
        "flush_bytes": jobs.get("flush_bytes", 0),
        "comp_jobs": jobs.get("comp_jobs", 0),
        "comp_in_bytes": jobs.get("comp_in_bytes", 0),
        "comp_out_bytes": jobs.get("comp_out_bytes", 0),
        "cpu_s": cpu,
        "cpu_cores_avg": cpu / (fill + drain) if fill + drain else 0,
        "samples": sam,
        "throttle": thr,
        "drain_hit_limit": s.get("drain_hit_limit", 0),
        "l0_end": s.get("l0_files_at_drain_end"),
        "fill_profile": fill_profile(os.path.join(d, "fill.samples.csv")),
    }


def arm_b():
    d = os.path.join(OUT, "B")
    ingest = parse_wal_log(os.path.join(d, "ingest/wal.log"))
    replay = load_summary(os.path.join(d, "replay/fill.summary.json"))
    if not ingest or not replay:
        return {"status": "missing", "ingest": ingest, "replay": bool(replay)}
    app = replay.get("workload", {}).get("app_bytes", 0)
    rfill = replay.get("fill_wall_s", 0)
    rdrain = replay.get("drain_wall_s", 0)
    stalls = replay.get("stalls", {}).get("stall_s", 0)
    jobs = replay.get("jobs", {})
    cpu = replay.get("process_cpu_s_total", 0)
    sam_i = parse_samples(os.path.join(d, "ingest/samples.csv"))
    sam_r = parse_samples(os.path.join(d, "replay/samples.csv"))
    thr_i = parse_throttle(os.path.join(d, "ingest/wal.log"))
    thr_r = parse_throttle(os.path.join(d, "replay/run.log"))
    total = ingest["wall_s"] + rfill + rdrain
    return {
        "status": "ok",
        "app_bytes": app,
        "ingest_wall_s": ingest["wall_s"],
        "ingest_rate_mb_s": ingest["rate_mb_s"],
        "ingest_ops_s": app / 1040.0 / ingest["wall_s"] if ingest["wall_s"] else 0,
        "materialize_wall_s": rfill + rdrain,
        "materialize_fill_wall_s": rfill,
        "materialize_drain_wall_s": rdrain,
        "total_wall_s": total,
        "replay_rate_mb_s": replay.get("fill_rate_mb_s", 0),
        "replay_ops_s": replay.get("fill_rate_ops_s", 0),
        "stall_s": stalls,
        "stall_frac_replay_fill": stalls / rfill if rfill else 0,
        "flush_jobs": jobs.get("flush_jobs", 0),
        "flush_bytes": jobs.get("flush_bytes", 0),
        "comp_jobs": jobs.get("comp_jobs", 0),
        "comp_in_bytes": jobs.get("comp_in_bytes", 0),
        "comp_out_bytes": jobs.get("comp_out_bytes", 0),
        "cpu_s": cpu,
        "cpu_cores_avg": cpu / total if total else 0,
        "samples_ingest": sam_i,
        "samples_replay": sam_r,
        "throttle_ingest": thr_i,
        "throttle_replay": thr_r,
        "drain_hit_limit": replay.get("drain_hit_limit", 0),
        "l0_end": replay.get("l0_files_at_drain_end"),
        "fill_profile": fill_profile(os.path.join(d, "replay/fill.samples.csv")),
        "b2_charged_write_mb": (thr_r.get("charged_write_mb", 0)
                                 - thr_i.get("charged_write_mb", 0)),
        "b2_charged_read_mb": (thr_r.get("charged_read_mb", 0)
                                - thr_i.get("charged_read_mb", 0)),
    }


def fmt_bytes(n):
    return "%.1f GiB" % (n / 1073741824.0) if n else "-"



def arm_bpipe():
    d = os.path.join(OUT, "Bpipe")
    ingest = parse_wal_log(os.path.join(d, "ingest/wal.log"))
    replay = load_summary(os.path.join(d, "replay/fill.summary.json"))
    try:
        done = open(os.path.join(d, "done.txt")).read()
    except FileNotFoundError:
        done = ""
    m = re.search(r"pipe_wall_s=(\d+)", done)
    pipe_wall = int(m.group(1)) if m else 0
    if not ingest or not replay or not pipe_wall:
        return {"status": "missing"}
    rfill = replay.get("fill_wall_s", 0)
    rdrain = replay.get("drain_wall_s", 0)
    jobs = replay.get("jobs", {})
    sam = parse_samples(os.path.join(d, "samples.csv"))
    thr_r = parse_throttle(os.path.join(d, "replay/run.log"))
    thr_i = parse_throttle(os.path.join(d, "ingest/wal.log"))
    return {
        "status": "ok",
        "app_bytes": replay.get("workload", {}).get("app_bytes", 0),
        "ingest_wall_s": ingest["wall_s"],
        "ingest_rate_mb_s": ingest["rate_mb_s"],
        "materialize_fill_wall_s": rfill,
        "materialize_drain_wall_s": rdrain,
        "materialize_wall_s": rfill + rdrain,
        "total_wall_s": pipe_wall,
        "replay_rate_mb_s": replay.get("fill_rate_mb_s", 0),
        "stall_s": replay.get("stalls", {}).get("stall_s", 0),
        "flush_jobs": jobs.get("flush_jobs", 0),
        "flush_bytes": jobs.get("flush_bytes", 0),
        "comp_jobs": jobs.get("comp_jobs", 0),
        "comp_in_bytes": jobs.get("comp_in_bytes", 0),
        "comp_out_bytes": jobs.get("comp_out_bytes", 0),
        "cpu_s": sam.get("proc_cpu_s", 0),
        "samples": sam,
        "throttle_replay": thr_r,
        "throttle_ingest": thr_i,
        "drain_hit_limit": replay.get("drain_hit_limit", 0),
        "fill_profile": fill_profile(os.path.join(d, "replay/fill.samples.csv")),
    }

def main():
    a = arm_a("A1")
    a2 = arm_a("A2")
    b = arm_b()
    bp = arm_bpipe()
    res = {"A1": a, "A2": a2, "B": b, "Bpipe": bp}
    with open(os.path.join(OUT, "summary.json"), "w") as f:
        json.dump(res, f, indent=2, sort_keys=True)

    print("A1.status", a.get("status"))
    print("A2.status", a2.get("status"))
    print("B.status", b.get("status"))
    print("Bpipe.status", bp.get("status"))
    if a.get("status") == "ok":
        print("A ingest %.1fs total %.1fs app %.1f MB/s stall %.1fs (%.1f%%)"
              % (a["ingest_wall_s"], a["total_wall_s"], a["ingest_rate_mb_s"],
                 a["stall_s"], 100 * a["stall_frac"]))
    if a2.get("status") == "ok":
        print("A2 ingest %.1fs total %.1fs app %.1f MB/s"
              % (a2["ingest_wall_s"], a2["total_wall_s"], a2["ingest_rate_mb_s"]))
    if b.get("status") == "ok":
        print("B ingest %.1fs materialize %.1fs total %.1fs ingest %.1f MB/s"
              % (b["ingest_wall_s"], b["materialize_wall_s"], b["total_wall_s"],
                 b["ingest_rate_mb_s"]))
    if bp.get("status") == "ok":
        print("Bpipe ingest %.1fs materialize %.1fs overlap-total %.1fs"
              % (bp["ingest_wall_s"], bp["materialize_wall_s"], bp["total_wall_s"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
