#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""exp_common.py - ZeroFlush 前期实验 Python 公共库。

提供：
  - median_of_runs(values)          : 多轮结果取中位数 + 极差
  - parse_db_bench_statistics(text) : 解析 db_bench --statistics 输出 ticker
  - parse_stalls_count_line(log_text): 解析 RocksDB LOG 中 "Stalls(count)" 行
  - parse_time_v(text)              : 解析 /usr/bin/time -v 输出
  - collect_hw_json()               : 采集硬件信息 dict
  - validate_exp_json(schema, obj)  : 结果 JSON 必填字段校验框架

无第三方依赖（仅标准库），Python 3.8+。
"""

import os
import platform
import re
import subprocess
import sys

# ---------------------------------------------------------------------------
# 多轮统计
# ---------------------------------------------------------------------------

def median_of_runs(values):
    """返回 (median, range)。values 为非空数值序列。

    偶数个样本时中位数取中间两数平均；极差 = max - min。
    """
    if not values:
        raise ValueError("values must be non-empty")
    vals = sorted(float(v) for v in values)
    n = len(vals)
    if n % 2 == 1:
        median = vals[n // 2]
    else:
        median = (vals[n // 2 - 1] + vals[n // 2]) / 2.0
    return median, vals[-1] - vals[0]


# ---------------------------------------------------------------------------
# db_bench --statistics 输出解析
# ---------------------------------------------------------------------------

# ticker 枚举名 -> db_bench statistics 输出中的名字字符串
# 已对照 source/rocksdb/monitoring/statistics.cc 确认（本版本 RocksDB）：
#   WAL_FILE_BYTES       -> rocksdb.wal.bytes   （注意：不是 rocksdb.wal.file.bytes）
#   FLUSH_WRITE_BYTES    -> rocksdb.flush.write.bytes
#   COMPACT_WRITE_BYTES  -> rocksdb.compact.write.bytes
#   STALL_MICROS         -> rocksdb.stall.micros
TICKER_NAMES = {
    "WAL_FILE_BYTES": "rocksdb.wal.bytes",
    "FLUSH_WRITE_BYTES": "rocksdb.flush.write.bytes",
    "COMPACT_WRITE_BYTES": "rocksdb.compact.write.bytes",
    "STALL_MICROS": "rocksdb.stall.micros",
}

# db_bench statistics 行格式：<name> COUNT : <number>
_TICKER_LINE_RE = re.compile(r"^\s*([\w.]+)\s+COUNT\s+:\s+([\d.]+)", re.M)


def parse_db_bench_statistics(text, tickers=None):
    """从 db_bench --statistics 输出文本抽取 ticker 数值。

    参数:
      text    : db_bench 完整 stdout/stderr 文本
      tickers : 需要提取的 ticker 枚举名列表，默认取 TICKER_NAMES 全部。
    返回:
      dict: {ticker 枚举名: float 数值}；缺失的 ticker 不出现在结果中。
    """
    if tickers is None:
        tickers = list(TICKER_NAMES.keys())

    # 先建立 输出名 -> 数值 的映射
    found = {}
    for m in _TICKER_LINE_RE.finditer(text or ""):
        name, value = m.group(1), m.group(2)
        try:
            found[name] = float(value)
        except ValueError:
            pass

    result = {}
    for t in tickers:
        out_name = TICKER_NAMES.get(t, t)
        if out_name in found:
            result[t] = found[out_name]
    return result


# ---------------------------------------------------------------------------
# RocksDB LOG 解析
# ---------------------------------------------------------------------------

# LOG 中形如：
#   Stalls(count): 0 levels slow down, 0 stops
#   Stalls(count): 3 levels slow down, 1 stops; 0 batch
_STALLS_RE = re.compile(
    r"Stalls\(count\):\s*(\d+)\s+levels slow down,\s*(\d+)\s+stops")


def parse_stalls_count_line(log_text):
    """解析 RocksDB LOG 中 "Stalls(count)" 行。

    返回 dict: {"slowdowns": int, "stops": int}；未找到返回 None。
    存在多行时取最后一行（通常对应最终统计）。
    """
    matches = _STALLS_RE.findall(log_text or "")
    if not matches:
        return None
    slowdowns, stops = matches[-1]
    return {"slowdowns": int(slowdowns), "stops": int(stops)}


# ---------------------------------------------------------------------------
# /usr/bin/time -v 输出解析
# ---------------------------------------------------------------------------

_TIME_V_PATTERNS = {
    "elapsed_wall_seconds": re.compile(
        r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\): ([\d:.]+)"),
    "user_time_seconds": re.compile(r"User time \(seconds\): ([\d.]+)"),
    "system_time_seconds": re.compile(r"System time \(seconds\): ([\d.]+)"),
}


def _hms_to_seconds(s):
    """把 'h:mm:ss' / 'm:ss' / 'ss.ss' 转成秒。"""
    parts = s.split(":")
    parts = [float(p) for p in parts]
    sec = 0.0
    for p in parts:
        sec = sec * 60.0 + p
    return sec


def parse_time_v(text):
    """解析 /usr/bin/time -v 输出。

    返回 dict: {"elapsed_wall_seconds", "user_time_seconds",
                "system_time_seconds"}；缺失字段不出现在结果中。
    """
    result = {}
    for key, pat in _TIME_V_PATTERNS.items():
        m = pat.search(text or "")
        if not m:
            continue
        raw = m.group(1)
        result[key] = _hms_to_seconds(raw) if ":" in raw else float(raw)
    return result


# ---------------------------------------------------------------------------
# 硬件信息采集（字段风格参考 script/common.sh 的 collect_hw_json，只读参考）
# ---------------------------------------------------------------------------

def _read_file(path):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except OSError:
        return ""


def collect_hw_json():
    """采集硬件信息 dict（CPU 型号/核数、内存、磁盘、内核版本、uname）。"""
    cpu_model = ""
    cpu_cores = 0
    meminfo = _read_file("/proc/meminfo")
    cpuinfo = _read_file("/proc/cpuinfo")

    for line in cpuinfo.splitlines():
        if line.startswith("model name"):
            cpu_model = line.split(":", 1)[1].strip()
            break
    cpu_cores = sum(1 for line in cpuinfo.splitlines()
                    if line.startswith("processor"))

    memory_gb = 0.0
    m = re.search(r"MemTotal:\s+(\d+)\s+kB", meminfo)
    if m:
        memory_gb = round(int(m.group(1)) / 1024.0 / 1024.0, 2)

    # 磁盘：df / 所在文件系统
    disk = ""
    try:
        out = subprocess.run(
            ["df", "-h", "--output=target,size,fstype", "/"],
            capture_output=True, text=True, timeout=5)
        lines = [l.strip() for l in out.stdout.strip().splitlines()]
        disk = lines[1] if len(lines) > 1 else ""
    except Exception:
        pass

    kernel = platform.release()
    uname = " ".join(platform.uname())

    return {
        "cpu_model": cpu_model,
        "cpu_cores": cpu_cores,
        "memory_gb": memory_gb,
        "disk": disk,
        "kernel": kernel,
        "uname": uname,
        "os": platform.system(),
    }


# ---------------------------------------------------------------------------
# 结果 JSON 校验框架
# ---------------------------------------------------------------------------

# 各实验结果 JSON 的必填字段 schema
SCHEMAS = {
    # 通用实验运行记录
    "exp_run": ["metadata", "hardware", "runs"],
    # 每个 run 条目必填字段
    "exp_run_entry": ["value_size", "num_keys", "fill_ops_per_sec"],
    # 硬件信息
    "hardware": ["cpu_model", "cpu_cores", "memory_gb", "kernel"],
    # 元信息
    "metadata": ["timestamp", "engine", "value_size", "threads", "seed"],
}


def validate_exp_json(schema_name, obj):
    """简单必填字段校验。

    参数:
      schema_name : SCHEMAS 中的 schema 名；未知 schema 抛 KeyError
      obj         : 待校验 dict
    返回:
      list[str] 缺失/非法字段描述；空列表表示校验通过。
    """
    if schema_name not in SCHEMAS:
        raise KeyError(f"unknown schema: {schema_name}")
    required = SCHEMAS[schema_name]

    errors = []
    if not isinstance(obj, dict):
        return [f"expected object(dict), got {type(obj).__name__}"]
    for field in required:
        if field not in obj:
            errors.append(f"missing required field: {field}")
        elif obj[field] is None:
            errors.append(f"field is null: {field}")
    return errors


# ---------------------------------------------------------------------------
# CLI 自测入口
# ---------------------------------------------------------------------------

def _self_test():
    print("[exp_common] self test")

    med, rng = median_of_runs([3, 1, 2, 5, 4])
    assert med == 3.0 and rng == 4.0, (med, rng)
    med, rng = median_of_runs([1, 2, 3, 4])
    assert med == 2.5 and rng == 3.0, (med, rng)
    print(f"  median_of_runs: OK (odd->3.0/4.0, even->2.5/3.0)")

    stats_text = """
rocksdb.wal.bytes COUNT : 12345678
rocksdb.wal.synced COUNT : 12
rocksdb.flush.write.bytes COUNT : 67108864
rocksdb.compact.write.bytes COUNT : 134217728
rocksdb.stall.micros COUNT : 4567
"""
    stats = parse_db_bench_statistics(stats_text)
    assert stats["WAL_FILE_BYTES"] == 12345678.0, stats
    assert stats["FLUSH_WRITE_BYTES"] == 67108864.0, stats
    assert stats["COMPACT_WRITE_BYTES"] == 134217728.0, stats
    assert stats["STALL_MICROS"] == 4567.0, stats
    print(f"  parse_db_bench_statistics: OK {stats}")

    log_text = ("2024-01-01 blah\n"
                "Stalls(count): 3 levels slow down, 1 stops\n")
    stalls = parse_stalls_count_line(log_text)
    assert stalls == {"slowdowns": 3, "stops": 1}, stalls
    print(f"  parse_stalls_count_line: OK {stalls}")

    time_text = """\tCommand being timed: "./a.out"
\tUser time (seconds): 1.23
\tSystem time (seconds): 0.45
\tElapsed (wall clock) time (h:mm:ss or m:ss): 0:01.70
"""
    tv = parse_time_v(time_text)
    assert abs(tv["user_time_seconds"] - 1.23) < 1e-9, tv
    assert abs(tv["system_time_seconds"] - 0.45) < 1e-9, tv
    assert abs(tv["elapsed_wall_seconds"] - 1.70) < 1e-9, tv
    print(f"  parse_time_v: OK {tv}")

    hw = collect_hw_json()
    assert hw["cpu_cores"] > 0, hw
    print(f"  collect_hw_json: OK cores={hw['cpu_cores']} "
          f"mem={hw['memory_gb']}GB kernel={hw['kernel']}")

    errs = validate_exp_json("hardware", {"cpu_model": "x", "cpu_cores": 8,
                                          "memory_gb": 16.0,
                                          "kernel": "5.15"})
    assert errs == [], errs
    errs = validate_exp_json("hardware", {"cpu_model": "x"})
    assert len(errs) == 3, errs
    print(f"  validate_exp_json: OK (pass + {len(errs)} missing detected)")

    print("[exp_common] all python self tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(_self_test())
