#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""解析 profiling run 的 LOG / stdout / samples.csv → summary JSON + 文本摘要。

用法：python3 zf_parse_log.py [W1 [W2 ...]]（默认全部存在的变体）
输出：<variant>/parsed.json 与终端摘要表。
"""

import csv
import glob
import json
import os
import re
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))

RE_TS = re.compile(r"^(\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d{6})")
RE_MATERIALIZED = re.compile(
    r"ZeroFlush materialized (\d+) epochs \((\d+) files, (\d+) bytes\) "
    r"in (\d+) us")
RE_STALL_LINE = re.compile(r"Cumulative stall: (\d+):(\d+):([\d.]+) H:M:S, "
                           r"([\d.]+) percent")
RE_INTERVAL_WRITES = re.compile(
    r"Interval writes: (\d+) writes.*?ingest: ([\d.]+) ([KMG])B, "
    r"([\d.]+) ([KMG])B/s")
RE_MAIN = re.compile(
    r"^(\w+)\s*:\s*([\d.]+)\s*micros/op\s+(\d+)\s*ops/sec\s+-*([\d.]+)\s*"
    r"seconds\s+(\d+)\s*operations;\s*([\d.]+)\s*MB/s", re.M)
RE_PCT = re.compile(
    r"Percentiles:\s*P50:\s*([\d.]+)\s*P75:\s*([\d.]+)\s*P99:\s*([\d.]+)"
    r"\s*P99.9:\s*([\d.]+)\s*P99.99:\s*([\d.]+)")


def log_time_to_epoch(ts):
    return datetime.strptime(ts, "%Y/%m/%d-%H:%M:%S.%f").timestamp()


def parse_log(path):
    out = dict(materialize=[], compactions=[], flushes=[], stops=0,
               stall_dump=[], slowdown_lines=0)
    if not path or not os.path.exists(path):
        return out
    with open(path, errors="replace") as f:
        for line in f:
            m = RE_MATERIALIZED.search(line)
            if m:
                ts_m = RE_TS.match(line)
                end = log_time_to_epoch(ts_m.group(1)) if ts_m else None
                out["materialize"].append(dict(
                    end_ts=end,
                    epochs=int(m.group(1)), files=int(m.group(2)),
                    first_file_bytes=int(m.group(3)), micros=int(m.group(4))))
                continue
            if '"event": "compaction_finished"' in line or \
               '"event":"compaction_finished"' in line:
                js = line[line.find("{"):]
                try:
                    j = json.loads(js)
                    out["compactions"].append(dict(
                        time_micros=j.get("time_micros"),
                        job=j.get("job"),
                        wall_us=j.get("compaction_time_micros"),
                        cpu_us=j.get("compaction_time_cpu_micros"),
                        level=j.get("output_level"),
                        files=j.get("num_output_files"),
                        out_bytes=j.get("total_output_size"),
                        subcompactions=j.get("num_subcompactions"),
                        l0_after=(j.get("lsm_state") or [None])[0]))
                except json.JSONDecodeError:
                    pass
                continue
            if '"event": "flush_finished"' in line or \
               '"event":"flush_finished"' in line:
                js = line[line.find("{"):]
                try:
                    j = json.loads(js)
                    out["flushes"].append(dict(
                        time_micros=j.get("time_micros"),
                        job=j.get("job"),
                        l0=(j.get("lsm_state") or [None])[0],
                        imm=j.get("immutable_memtables")))
                except json.JSONDecodeError:
                    pass
                continue
            if "Stopping writes" in line:
                out["stops"] += 1
            if "Slowing down" in line or "stall" in line.lower() and \
               "Percentiles" not in line and "rocksdb." not in line:
                out["slowdown_lines"] += 1
            s = RE_STALL_LINE.search(line)
            if s:
                out["stall_dump"].append(
                    int(s.group(1)) * 3600 + int(s.group(2)) * 60 +
                    float(s.group(3)))
    return out


def mult(sym, val):
    return val * {"K": 1e3, "M": 1e6, "G": 1e9}[sym]


def parse_stdout(path):
    out = dict(interval_rates=[], stall_pct=[])
    if not os.path.exists(path):
        return out
    with open(path, errors="replace") as f:
        text = f.read()
    for m in RE_INTERVAL_WRITES.finditer(text):
        rate = mult(m.group(5), float(m.group(4)))
        out["interval_rates"].append(round(rate / 1e6, 2))  # MB/s
    for m in RE_STALL_LINE.finditer(text):
        out["stall_pct"].append(float(m.group(4)))
    # 终值：取最后一组（interval dump 中也有同名行）
    main = None
    for main in RE_MAIN.finditer(text):
        pass
    if main:
        out["final"] = dict(
            us_per_op=float(main.group(2)), ops_per_sec=int(main.group(3)),
            seconds=float(main.group(4)), operations=int(main.group(5)),
            mb_per_s=float(main.group(6)))
    pct = None
    for pct in RE_PCT.finditer(text):
        pass
    if pct:
        out["final"].update(p50=float(pct.group(1)), p99=float(pct.group(3)))
    out["zf_props"] = {
        k: float(v) for k, v in re.findall(
            r"^zf\.([a-z0-9_]+)\s*:\s*([\d.]+)", text, re.M)}
    return out


def parse_samples(path):
    out = {}
    if not os.path.exists(path):
        return out
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.append({k: v for k, v in r.items()
                         if k != "state"})
    if len(rows) < 2:
        return out
    def num(r, k):
        try:
            return float(r[k])
        except (KeyError, ValueError):
            return 0.0

    a, b = rows[0], rows[-1]
    dt = num(b, "t_s") - num(a, "t_s")
    out["duration_s"] = round(dt, 1)
    out["cpu_cores_avg"] = round(
        ((num(b, "utime_s") - num(a, "utime_s")) +
         (num(b, "stime_s") - num(a, "stime_s"))) / dt, 2)
    out["disk_wr_mb_s_avg"] = round(
        (num(b, "disk_w_mb") - num(a, "disk_w_mb")) / dt, 1)
    out["threads_max"] = int(max(num(r, "nthreads") for r in rows))
    return out


def summarize(variant):
    vdir = os.path.join(HERE, variant)
    res = {}
    with open(os.path.join(vdir, "result.json")) as f:
        res = json.load(f)
    log_files = res.get("log_files") or []
    log = parse_log(log_files[0] if log_files else None)
    stdout_an = parse_stdout(os.path.join(vdir, "stdout.txt"))
    samples = parse_samples(os.path.join(vdir, "samples.csv"))
    if "final" in stdout_an:
        res.update(stdout_an["final"])
    if stdout_an.get("zf_props"):
        res["zf_props"] = stdout_an["zf_props"]

    mat = log["materialize"]
    comp = log["compactions"]
    summary = dict(
        variant=variant,
        wall_sec=res.get("wall_sec"),
        ops_per_sec=res.get("ops_per_sec"),
        mb_per_s=res.get("mb_per_s"),
        p50_us=res.get("p50"), p99_us=res.get("p99"),
        zf_props=res.get("zf_props"),
        db_size_gb=round(res.get("db_size_bytes", 0) / 2**30, 2),
        epochs_materialized=len(mat),
        materialize_total_us=sum(m["micros"] for m in mat),
        materialize_max_us=max((m["micros"] for m in mat), default=0),
        materialize_files_total=sum(m["files"] for m in mat),
        compaction_jobs=len(comp),
        compaction_wall_us=sum(c["wall_us"] or 0 for c in comp),
        compaction_cpu_us=sum(c["cpu_us"] or 0 for c in comp),
        compaction_out_gb=round(
            sum(c["out_bytes"] or 0 for c in comp) / 2**30, 2),
        compaction_subcompactions=sorted(
            {c["subcompactions"] for c in comp}),
        l0_stop_lines=log["stops"],
        stall_dump_final_s=log["stall_dump"][-1] if log["stall_dump"] else 0,
        write_mb_s_intervals=stdout_an["interval_rates"][-8:],
        **samples)
    with open(os.path.join(vdir, "parsed.json"), "w") as f:
        json.dump(dict(summary=summary, materialize=mat, compactions=comp,
                       flushes=log["flushes"]), f, indent=2)
    return summary


def main():
    variants = sys.argv[1:] or sorted(
        d for d in glob.glob(os.path.join(HERE, "W*"))
        if os.path.isdir(d) and os.path.exists(os.path.join(d, "result.json")))
    variants = [os.path.basename(v) if os.path.isdir(v) else v
                for v in variants]
    rows = []
    for v in variants:
        try:
            rows.append(summarize(v))
        except FileNotFoundError as e:
            print(f"[{v}] 跳过：{e}")
    cols = ["variant", "wall_sec", "ops_per_sec", "mb_per_s", "p50_us",
            "p99_us", "epochs_materialized", "materialize_total_us",
            "compaction_jobs", "compaction_wall_us", "l0_stop_lines",
            "stall_dump_final_s", "cpu_cores_avg", "disk_wr_mb_s_avg",
            "db_size_gb"]
    print("\t".join(cols))
    for r in rows:
        print("\t".join(str(r.get(c)) for c in cols))
    for r in rows:
        zf = r.get("zf_props") or {}
        keys = ["epochs_sealed", "epochs_materialized", "epochs_reclaimed",
                "materialize_micros", "materialize_sort_micros",
                "install_direct_base", "install_fallback_l0",
                "base_merge_count", "sealed_read_count"]
        print(f'{r["variant"]} zf: ' + " ".join(
            f"{k}={zf.get(k)}" for k in keys))


if __name__ == "__main__":
    main()
