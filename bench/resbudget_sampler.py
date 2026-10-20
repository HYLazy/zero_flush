#!/usr/bin/env python3
"""Sample device and per-process I/O for a resbudget arm.

Writes one row per interval:
  ts, elapsed_s, dev_rd_mb_s, dev_wr_mb_s, dev_busy_pct,
  proc_rchar_mb, proc_wchar_mb, proc_read_bytes_mb, proc_write_bytes_mb,
  proc_cpu_s, pids
Device numbers come from /proc/diskstats for the whole NVMe device (all
partitions included).  Per-process numbers are the sum over the listed pids.
The sampler stops when --stop-file appears.
"""
import argparse
import os
import time

SECTOR = 512.0


def read_diskstats(dev):
    with open("/proc/diskstats", "r") as f:
        for line in f:
            p = line.split()
            if len(p) >= 14 and p[2] == dev:
                # fields: reads, reads_merged, sectors_read, ms_read,
                # writes, writes_merged, sectors_written, ms_write,
                # ios_in_progress, ms_io, ms_weighted
                return {
                    "rd_sectors": int(p[5]),
                    "wr_sectors": int(p[9]),
                    "io_ticks": int(p[12]),
                }
    raise SystemExit("device %s not found in /proc/diskstats" % dev)


def read_proc(pid):
    out = {"rchar": 0, "wchar": 0, "read_bytes": 0, "write_bytes": 0, "cpu": 0.0}
    try:
        with open("/proc/%d/io" % pid) as f:
            for line in f:
                k, _, v = line.partition(":")
                if k in out:
                    out[k] = int(v)
        with open("/proc/%d/stat" % pid) as f:
            p = f.read().rsplit(")", 1)[1].split()
        # after comm: state is p[0]; utime=p[11], stime=p[12]
        ticks = int(p[11]) + int(p[12])
        out["cpu"] = ticks / float(os.sysconf("SC_CLK_TCK"))
        out["alive"] = 1
    except (FileNotFoundError, ProcessLookupError, IndexError):
        out["alive"] = 0
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--dev", default="nvme0n1")
    ap.add_argument("--pids", default="")
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--stop-file", required=True)
    args = ap.parse_args()

    pids = [int(x) for x in args.pids.split(",") if x.strip()]
    f = open(args.out, "w")
    f.write("ts,elapsed_s,dev_rd_mb_s,dev_wr_mb_s,dev_busy_pct,"
            "proc_rchar_mb,proc_wchar_mb,proc_read_bytes_mb,proc_write_bytes_mb,"
            "proc_cpu_s,live_pids,pids\n")
    f.flush()

    t0 = time.time()
    prev_d = read_diskstats(args.dev)
    prev_t = t0
    # Per-process counters are cumulative and monotonic; keep the last value
    # for pids that exit so the total does not fall.
    last = {pid: {"rchar": 0, "wchar": 0, "read_bytes": 0, "write_bytes": 0, "cpu": 0.0}
            for pid in pids}
    while not os.path.exists(args.stop_file):
        now = time.time()
        try:
            d = read_diskstats(args.dev)
        except SystemExit:
            break
        dt = max(now - prev_t, 1e-6)
        rd = max(d["rd_sectors"] - prev_d["rd_sectors"], 0) * SECTOR / dt / 1048576.0
        wr = max(d["wr_sectors"] - prev_d["wr_sectors"], 0) * SECTOR / dt / 1048576.0
        busy = max(d["io_ticks"] - prev_d["io_ticks"], 0) / (dt * 1000.0) * 100.0
        live = 0
        tot = {"rchar": 0, "wchar": 0, "read_bytes": 0, "write_bytes": 0, "cpu": 0.0}
        for pid in pids:
            cur = read_proc(pid)
            if cur["alive"]:
                live += 1
                for k in ("rchar", "wchar", "read_bytes", "write_bytes"):
                    last[pid][k] = max(last[pid][k], cur[k])
                last[pid]["cpu"] = max(last[pid]["cpu"], cur["cpu"])
            for k in ("rchar", "wchar", "read_bytes", "write_bytes", "cpu"):
                tot[k] += last[pid][k]
        f.write("%.3f,%.3f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.2f,%d,%s\n" % (
            now, now - t0, rd, wr, busy,
            tot["rchar"] / 1048576.0, tot["wchar"] / 1048576.0,
            tot["read_bytes"] / 1048576.0, tot["write_bytes"] / 1048576.0,
            tot["cpu"], live,
            "|".join(str(p) for p in pids)))
        f.flush()
        prev_d, prev_t = d, now
        time.sleep(args.interval)
    f.close()


if __name__ == "__main__":
    main()
