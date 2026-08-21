#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""parse_stats.py - Exp0 (Flush 成本分解) 单次运行解析与多轮聚合。

单次运行目录结构 (由 run_exp0.sh 生成):
  <run_dir>/stdout.txt      db_bench 完整 stdout (--statistics 输出)
  <run_dir>/stderr.txt      db_bench stderr
  <run_dir>/time.txt        /usr/bin/time -v 输出 (time -o)
  <run_dir>/LOG             运行结束后从 DB 目录复制的 RocksDB LOG
  <run_dir>/disk_bytes.txt  DB 目录最终 du -sb 值 (字节)
  <run_dir>/meta.json       {benchmark, value_size, distribution, run, seed,...}

用法:
  parse_stats.py run <run_dir>                 # 解析单次运行, JSON 输出到 stdout
  parse_stats.py aggregate <manifest.json> <out.json>
      manifest: [{"combo": "zipfian_vs100", "benchmark": "fillrandom",
                  "value_size": 100, "distribution": "zipfian",
                  "runs": [run_dir, ...]}, ...]
      out.json: 每组合 ≥3 次取中位数 (median_of_runs), 记录极差。

复用 ../common/py/exp_common.py 的解析器。
"""

import json
import os
import re
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "common", "py"))
from exp_common import (median_of_runs, parse_db_bench_statistics,  # noqa: E402
                        parse_stalls_count_line, parse_time_v)


def _read(path):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except OSError:
        return ""


# db_bench 输出行: fillrandom   :    6.218 micros/op 642568 ops/sec ...
_THROUGHPUT_RE = re.compile(
    r"^\s*(fillrandom|readwhilewriting|ycsb-a)\s*:\s*([\d.]+)\s+micros/op"
    r"\s+([\d.]+)\s+ops/sec", re.M)

# RocksDB 11.x LOG 新版 stall 行 (旧版 "Stalls(count)" 已弃用):
#   Write Stall (count): ..., total-delays: 13, total-stops: 0
_WRITE_STALL_RE = re.compile(
    r"total-delays:\s*(\d+),\s*total-stops:\s*(\d+)")

# 额外交叉核对用 ticker (exp_common.TICKER_NAMES 之外的)
EXTRA_TICKERS = ["rocksdb.compact.read.bytes"]


def parse_run_dir(run_dir):
    """解析单次运行目录, 返回指标 dict (缺失字段值为 None)。"""
    stdout = _read(os.path.join(run_dir, "stdout.txt"))
    time_txt = _read(os.path.join(run_dir, "time.txt"))
    log_txt = _read(os.path.join(run_dir, "LOG"))
    disk_txt = _read(os.path.join(run_dir, "disk_bytes.txt")).strip()
    meta = {}
    meta_txt = _read(os.path.join(run_dir, "meta.json"))
    if meta_txt:
        try:
            meta = json.loads(meta_txt)
        except ValueError:
            pass

    tickers = parse_db_bench_statistics(stdout)
    extra = parse_db_bench_statistics(stdout, tickers=EXTRA_TICKERS)
    tv = parse_time_v(time_txt)
    stalls = parse_stalls_count_line(log_txt)  # 旧版格式
    if stalls is None:
        # RocksDB 11.x 新版格式, 取最后一次周期性 dump (累计值)
        ws = _WRITE_STALL_RE.findall(log_txt)
        if ws:
            stalls = {"slowdowns": int(ws[-1][0]), "stops": int(ws[-1][1])}

    m = _THROUGHPUT_RE.search(stdout)

    disk_bytes = None
    if disk_txt:
        try:
            disk_bytes = float(disk_txt.split()[0])
        except (ValueError, IndexError):
            pass

    wal_resident_txt = _read(
        os.path.join(run_dir, "wal_resident_bytes.txt")).strip()
    wal_resident = None
    if wal_resident_txt:
        try:
            wal_resident = float(wal_resident_txt.split()[0])
        except (ValueError, IndexError):
            pass

    return {
        "run_dir": run_dir,
        "meta": meta,
        "wall_time_sec": tv.get("elapsed_wall_seconds"),
        "user_time_sec": tv.get("user_time_seconds"),
        "system_time_sec": tv.get("system_time_seconds"),
        "cpu_time_sec": (
            None if tv.get("user_time_seconds") is None
            or tv.get("system_time_seconds") is None
            else tv["user_time_seconds"] + tv["system_time_seconds"]),
        "wal_bytes": tickers.get("WAL_FILE_BYTES"),
        "flush_bytes": tickers.get("FLUSH_WRITE_BYTES"),
        "compact_write_bytes": tickers.get("COMPACT_WRITE_BYTES"),
        "compact_read_bytes": extra.get("rocksdb.compact.read.bytes"),
        "stall_micros": tickers.get("STALL_MICROS"),
        "stalls_slowdowns": stalls["slowdowns"] if stalls else None,
        "stalls_stops": stalls["stops"] if stalls else None,
        "micros_per_op": float(m.group(2)) if m else None,
        "ops_per_sec": float(m.group(3)) if m else None,
        "disk_final_bytes": disk_bytes,
        "wal_resident_bytes": wal_resident,
        "parsed_ok": bool(stdout) and bool(tickers),
    }


def _median_fields(run_results, fields):
    """对每个字段收集各 run 的非 None 值, 取中位数与极差。"""
    out = {}
    for f in fields:
        vals = [r[f] for r in run_results if r.get(f) is not None]
        if not vals:
            out[f] = None
            out[f + "_range"] = None
            continue
        med, rng = median_of_runs(vals)
        out[f] = med
        out[f + "_range"] = rng
    return out


MEDIAN_FIELDS = [
    "wall_time_sec", "cpu_time_sec", "wal_bytes", "flush_bytes",
    "compact_write_bytes", "compact_read_bytes", "stall_micros",
    "ops_per_sec", "disk_final_bytes", "wal_resident_bytes",
]


def aggregate(manifest):
    """manifest -> 每组合中位数聚合结果列表。"""
    results = []
    for combo in manifest:
        run_results = []
        failures = []
        for run_dir in combo["runs"]:
            r = parse_run_dir(run_dir)
            if not r["parsed_ok"]:
                failures.append(run_dir)
            run_results.append(r)
        agg = _median_fields(run_results, MEDIAN_FIELDS)
        stalls_sd = [r["stalls_slowdowns"] for r in run_results
                     if r["stalls_slowdowns"] is not None]
        stalls_st = [r["stalls_stops"] for r in run_results
                     if r["stalls_stops"] is not None]
        agg.update({
            "combo": combo["combo"],
            "benchmark": combo["benchmark"],
            "value_size": combo["value_size"],
            "distribution": combo["distribution"],
            "n_runs": len(run_results),
            "n_ok": sum(1 for r in run_results if r["parsed_ok"]),
            "failed_runs": failures,
            "stalls_slowdowns": median_of_runs(stalls_sd)[0] if stalls_sd else None,
            "stalls_stops": median_of_runs(stalls_st)[0] if stalls_st else None,
            "runs_detail": run_results,
        })
        results.append(agg)
    return results


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    mode = sys.argv[1]
    if mode == "run":
        if len(sys.argv) < 3:
            print("usage: parse_stats.py run <run_dir>")
            return 2
        print(json.dumps(parse_run_dir(sys.argv[2]), indent=2))
        return 0
    if mode == "aggregate":
        if len(sys.argv) < 4:
            print("usage: parse_stats.py aggregate <manifest.json> <out.json>")
            return 2
        with open(sys.argv[2], "r", encoding="utf-8") as f:
            manifest = json.load(f)
        results = aggregate(manifest)
        with open(sys.argv[3], "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
        for r in results:
            print(f"{r['combo']}: ok={r['n_ok']}/{r['n_runs']} "
                  f"wal={r['wal_bytes']} flush={r['flush_bytes']} "
                  f"compact_w={r['compact_write_bytes']} "
                  f"wall={r['wall_time_sec']}")
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
