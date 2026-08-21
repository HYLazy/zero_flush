#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""parse_ycsb.py - 解析 YCSB-A 交叉验证输出。

用法: parse_ycsb.py <ycsb_run_dir> <out.json> <recordcount> <operationcount>

目录内文件: run.txt (ycsb run stdout), time.txt (/usr/bin/time -v),
disk_bytes.txt (DB 目录最终大小), LOG (若 binding 生成)。
YCSB binding (rocksdbjni 6.2.2) 不暴露 RocksDB Statistics ticker,
引擎侧 WAL/flush/compaction 写字节为 N/A, 以磁盘最终大小作粗粒度交叉参考。
"""

import json
import os
import re
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "common", "py"))
from exp_common import parse_time_v  # noqa: E402


def _read(path):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except OSError:
        return ""


def main():
    ydir, out_path = sys.argv[1], sys.argv[2]
    recordcount, operationcount = int(sys.argv[3]), int(sys.argv[4])

    run_txt = _read(os.path.join(ydir, "run.txt"))
    time_txt = _read(os.path.join(ydir, "time.txt"))
    disk_txt = _read(os.path.join(ydir, "disk_bytes.txt")).strip()
    tv = parse_time_v(time_txt)

    def grab(name):
        m = re.search(rf"^\[{re.escape(name)}\],\s*([^,]+),\s*([\d.]+)",
                      run_txt, re.M)
        return float(m.group(2)) if m else None

    result = {
        "benchmark": "ycsb-a",
        "recordcount": recordcount,
        "operationcount": operationcount,
        "overall_throughput_ops_sec": grab("OVERALL, Throughput(ops/sec)"),
        "overall_runtime_sec": grab("OVERALL, RunTime(ms)"),
        "wall_time_sec": tv.get("elapsed_wall_seconds"),
        "cpu_time_sec": (
            None if tv.get("user_time_seconds") is None
            or tv.get("system_time_seconds") is None
            else tv["user_time_seconds"] + tv["system_time_seconds"]),
        "ops": {},
        "disk_final_bytes": (
            float(disk_txt.split()[0]) if disk_txt else None),
        "engine_statistics": "N/A (rocksdbjni binding 不暴露 Statistics)",
    }
    for op in ("READ", "UPDATE", "CLEANUP"):
        block = {}
        for field in ("Operations", "AverageLatency(us)", "95thPercentileLatency(us)",
                      "99thPercentileLatency(us)"):
            m = re.search(rf"^\[{op}\],\s*{re.escape(field)},\s*([\d.]+)",
                          run_txt, re.M)
            if m:
                block[field] = float(m.group(1))
        if block:
            result["ops"][op] = block

    if result["overall_throughput_ops_sec"] is None:
        print("WARN: 未解析到 YCSB OVERALL 吞吐")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))
    return 0 if result["overall_throughput_ops_sec"] is not None else 1


if __name__ == "__main__":
    sys.exit(main())
