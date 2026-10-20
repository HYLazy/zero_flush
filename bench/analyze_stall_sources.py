#!/usr/bin/env python3
"""Attribute the compactions that writers waited behind (pending-compaction-bytes
stall periods) by level pair, and — for a run with the enriched ledger — split
each job's wall into CPU (compute) vs read/write/fsync waits (transfer).

Usage: python3 bench/analyze_stall_sources.py <out_prefix> [--cap-gb 64]
"""
import csv
import json
import os
import re
import sys
from collections import defaultdict


def start_ts(prefix):
    with open(prefix + ".summary.json") as f:
        return json.load(f)["phase_mark"]["start_ts"]


def load_samples(prefix):
    rows = []
    for r in csv.DictReader(open(prefix + ".samples.csv")):
        try:
            rows.append({
                "elapsed": float(r["elapsed"]), "ops": int(r["ops_cum"]),
                "pending_gb": float(r["pending_comp"]) / 1e9,
                "l0": float(r["l0_files"]), "phase": int(r["phase"]),
                "ts": float(r["ts"]),
            })
        except (ValueError, KeyError):
            continue
    rows.sort(key=lambda x: x["elapsed"])
    return rows


def load_jobs(prefix):
    jobs = []
    for r in csv.DictReader(open(prefix + ".comp_jobs.csv")):
        def f(k, d=0.0):
            try:
                return float(r[k])
            except (ValueError, KeyError, TypeError):
                return d
        lvls = [i for i in range(7) if f("in_l%d" % i) > 0]
        out = int(f("out_level"))
        if not lvls:
            pair = "?"
        elif lvls[0] == 0:
            pair = "L0→L%d" % out
        elif len(lvls) == 1 and lvls[0] == out:
            pair = "L%d→L%d(trivial move)" % (lvls[0], out)
        elif len(lvls) == 1:
            pair = "L%d→L%d" % (lvls[0], out)
        else:
            pair = "mixed(%s)→L%d" % (",".join("L%d" % v for v in lvls), out)
        jobs.append({
            "t0": f("ts_begin"), "t1": f("ts_end"), "pair": pair,
            "in_bytes": f("in_bytes"), "out_bytes": f("out_bytes"),
            "cpu": f("cpu_us_self") / 1e6, "wall": f("wall_us") / 1e6,
            "rdb_cpu": f("cpu_us_rdb") / 1e6, "rdb_elapsed": f("rdb_elapsed_us") / 1e6,
            "io_read_ns": f("io_read_ns"), "io_write_ns": f("io_write_ns"),
            "io_fsync_ns": f("io_fsync_ns"), "io_rangesync_ns": f("io_range_sync_ns"),
            "io_cpu_read_ns": f("io_cpu_read_ns"), "io_cpu_write_ns": f("io_cpu_write_ns"),
            "file_write_ns": f("file_write_ns"), "phase": int(f("phase")),
            "out_files_n": f("out_files"),
        })
    jobs.sort(key=lambda j: j["t0"])
    return jobs


def f_out_files(job):
    return job.get("out_files_n", 0.0)


def main():
    prefix = sys.argv[1]
    cap_gb = 64.0
    if "--cap-gb" in sys.argv:
        cap_gb = float(sys.argv[sys.argv.index("--cap-gb") + 1])
    t0abs = start_ts(prefix)
    sm, jobs = load_samples(prefix), load_jobs(prefix)
    for j in jobs:   # jobs carry absolute wall-clock; convert to elapsed seconds
        j["t0"] -= t0abs
        j["t1"] -= t0abs
    if not sm or not jobs:
        print("no data"); return
    fill_end = max(r["elapsed"] for r in sm if r["phase"] == 1)
    # throttled intervals: pending bytes above the soft limit (the delay token's
    # cause), clipped to the fill phase
    thr, cur = [], None
    for a, b in zip(sm, sm[1:]):
        if a["phase"] != 1:
            continue
        above = a["pending_gb"] >= cap_gb
        if above and cur is None:
            cur = a["elapsed"]
        if not above and cur is not None:
            thr.append((cur, a["elapsed"])); cur = None
    if cur is not None:
        thr.append((cur, min(fill_end, sm[-1]["elapsed"])))
    thr_total = sum(b - a for a, b in thr)
    thr_share = thr_total / max(fill_end, 1e-9)
    print("== %s ==" % prefix)
    print("fill wall %.0f s；pending≥%.0fGB 的限速时段 %.0f s（%.1f%% 墙钟），共 %d 段" % (
        fill_end, cap_gb, thr_total, thr_share * 100, len(thr)))

    def overlap(j, a, b):
        return max(0.0, min(j["t1"], b) - max(j["t0"], a))

    by_pair = defaultdict(lambda: defaultdict(float))
    tot = defaultdict(float)
    n_by_pair = defaultdict(int)
    for j in jobs:
        w = 0.0
        for a, b in thr:
            w += overlap(j, a, b)
        tot["wall"] += j["wall"]; tot["in"] += j["in_bytes"]; tot["cpu"] += j["cpu"]
        tot["thr_wall"] += min(w, j["wall"])
        share = min(w, j["wall"]) / j["wall"] if j["wall"] > 0 else 0.0
        by_pair[j["pair"]]["jobs"] += 1
        by_pair[j["pair"]]["wall"] += j["wall"]
        by_pair[j["pair"]]["thr_wall"] += min(w, j["wall"])
        by_pair[j["pair"]]["in_bytes"] += j["in_bytes"]
        by_pair[j["pair"]]["cpu"] += j["cpu"]
        by_pair[j["pair"]]["thr_in"] += j["in_bytes"] * share
        by_pair[j["pair"]]["thr_cpu"] += j["cpu"] * share
        for k in ("io_read_ns", "io_write_ns", "io_fsync_ns", "io_cpu_read_ns",
                  "io_cpu_write_ns"):
            by_pair[j["pair"]][k] += j[k] * share
            by_pair[j["pair"]][k + "_tot"] += j[k]
        by_pair[j["pair"]]["out_files"] += f_out_files(j) * share
        by_pair[j["pair"]]["thr_cpu_io"] += (j["io_cpu_read_ns"] + j["io_cpu_write_ns"]) / 1e9 * share

    print("\n-- 全部 compaction 作业按层对 --")
    print("%-22s %5s %9s %9s %10s %10s" % ("层对", "作业", "输入GB", "占比", "CPU s", "CPU占比"))
    for pair, d in sorted(by_pair.items(), key=lambda kv: -kv[1]["in_bytes"]):
        print("%-22s %5d %9.1f %8.1f%% %10.1f %8.1f%%" % (
            pair, d["jobs"], d["in_bytes"] / 1e9, 100 * d["in_bytes"] / max(tot["in"], 1),
            d["cpu"], 100 * d["cpu"] / max(tot["cpu"], 1)))
    print("\n-- 其中落在限速时段内的部分（按墙钟重叠比例摊分） --")
    print("%-22s %9s %9s %10s %10s %11s %11s" % (
        "层对", "限速墙钟s", "占限速%", "摊分输入GB", "摊分CPU s", "读等待s", "fsync等待s"))
    tw = sum(d["thr_wall"] for d in by_pair.values())
    for pair, d in sorted(by_pair.items(), key=lambda kv: -kv[1]["thr_wall"]):
        print("%-22s %9.1f %8.1f%% %10.1f %10.1f %11.1f %11.1f" % (
            pair, d["thr_wall"], 100 * d["thr_wall"] / max(tw, 1e-9), d["thr_in"] / 1e9,
            d["thr_cpu"], d["io_read_ns"] / 1e9, d["io_fsync_ns"] / 1e9))
    print("限速期间 compaction 墙钟合计 %.0f s（占 fill 墙钟 %.1f%%）" % (tw, 100 * tw / max(fill_end, 1e-9)))
    if any(d["io_fsync_ns_tot"] > 0 for d in by_pair.values()):
        print("\n-- 限速期间：作业墙钟内的计算 vs I/O（按层对，摊分后占该层对限速墙钟的 %） --")
        print("%-12s %9s %7s %8s %8s %9s %10s %10s %9s" % (
            "层对", "墙钟s", "CPU%", "读等待%", "写等待%", "fsync%", "IO-cpu%", "非CPU(推定)%", "输出文件"))
        for pair, d in sorted(by_pair.items(), key=lambda kv: -kv[1]["thr_wall"]):
            w = d["thr_wall"]
            if w <= 0.5:
                continue
            cpu_io = d["thr_cpu_io"]
            wait = max(0.0, w - (d["thr_cpu"] + cpu_io))
            print("%-12s %9.1f %6.1f%% %7.1f%% %7.1f%% %8.1f%% %9.1f%% %10.1f%% %9.0f" % (
                pair, w, 100 * d["thr_cpu"] / w, 100 * d["io_read_ns"] / 1e9 / w,
                100 * d["io_write_ns"] / 1e9 / w, 100 * d["io_fsync_ns"] / 1e9 / w,
                100 * cpu_io / w, 100 * wait / w, d["out_files"]))

    # ---- time-bucketed level-pair shares (which level the waited work comes from) ----
    print("\n-- 按时间窗的层对输入占比（%） --")
    BUCKET = 600.0
    nb = int(fill_end // BUCKET) + 1
    grid = [defaultdict(float) for _ in range(nb)]
    for j in jobs:
        if j["t0"] > fill_end:
            continue
        b = min(nb - 1, int(j["t0"] // BUCKET))
        grid[b][j["pair"]] += j["in_bytes"]
    pairs = sorted({p for g in grid for p in g})
    print("%-12s %s" % ("时间窗(min)", "  ".join("%-10s" % p for p in pairs)))
    for b, g in enumerate(grid):
        tot_b = sum(g.values())
        if tot_b <= 0:
            continue
        print("%4d-%4d   %s" % (b * BUCKET / 60, (b + 1) * BUCKET / 60,
              "  ".join("%-10s" % ("%5.1f%%" % (100 * g.get(p, 0) / tot_b)) for p in pairs)))
    l0_extra = sum(1 for j in jobs if not j["pair"].startswith("L0"))
    print("非 L0 输入的作业数：%d（共 %d）" % (l0_extra, len(jobs)))

    # ---- debt attribution by level (cfstats snapshot at fill end) ----
    snap = prefix + ".cfstats_fillend.txt"
    if os.path.exists(snap):
        txt = open(snap).read()
        rows = []
        for line in txt.splitlines():
            t = line.split()
            if t and t[0] in ("L0", "L1", "L2", "L3", "L4", "L5", "L6") and len(t) > 12:
                try:
                    size_mb = float(t[2]) * (1024.0 if t[3] == "GB" else (1.0 if t[3] == "MB" else 1 / 1024.0))
                    score = float(t[4])
                    rows.append((t[0], size_mb, score))
                except (ValueError, IndexError):
                    pass
        print("\n-- 债务归属：cfstats(fill 末) 各层规模与目标（target = size/score） --")
        print("%-5s %12s %10s %12s %12s" % ("层", "规模MB", "Score", "目标MB", "超出目标MB"))
        tot_ex = 0.0
        for lv, size_mb, score in rows:
            target = size_mb / score if score > 0.01 else float("inf")
            ex = max(0.0, size_mb - target) if target != float("inf") else 0.0
            tot_ex += ex
            print("%-5s %12.1f %10.2f %12.1f %12.1f" % (
                lv, size_mb, score, target if target != float("inf") else -1, ex))
        print("各层超出目标合计 %.1f GB（对比 estimate-pending-compaction-bytes）" % (tot_ex / 1024))

    # I/O vs compute over ALL jobs (enriched ledger only)
    io_read = sum(j["io_read_ns"] for j in jobs) / 1e9
    io_write = sum(j["io_write_ns"] for j in jobs) / 1e9
    io_fsync = sum(j["io_fsync_ns"] for j in jobs) / 1e9
    cpu = sum(j["cpu"] for j in jobs)
    wall = sum(j["wall"] for j in jobs)
    if io_read or io_write or io_fsync:
        print("\n-- compaction 内部：计算 vs 传输（全部作业，含 drain） --")
        print("作业墙钟合计 %.0f s；线程 CPU %.0f s（%.0f%%）；" % (wall, cpu, 100 * cpu / max(wall, 1e-9)))
        print("read() 等待 %.1f s（%.0f%%）；write() 等待 %.1f s（%.0f%%）；fsync 等待 %.1f s（%.0f%%）" % (
            io_read, 100 * io_read / max(wall, 1e-9), io_write, 100 * io_write / max(wall, 1e-9),
            io_fsync, 100 * io_fsync / max(wall, 1e-9)))
        resid = wall - cpu - io_read - io_write - io_fsync
        print("其余（锁/调度/未计时）%.1f s（%.0f%%）" % (resid, 100 * resid / max(wall, 1e-9)))
    else:
        print("\n(该跑的台账未含 iostats 字段：读/写/fsync 拆分需要新驱动的跑)")


if __name__ == "__main__":
    main()
