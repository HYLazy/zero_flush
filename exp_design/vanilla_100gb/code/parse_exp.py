#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""parse_exp.py - 解析原版 RocksDB 100GB fillrandom 实验 run 目录 → metrics.json。

输入（run 目录，由 run_exp.py 生成）：
  cmd.json   完整命令与元信息
  stdout.txt db_bench 全量输出（30s 周期 stats dump + 最终 statistics tickers）
  LOG*       RocksDB 事件日志（compaction/flush 逐事件）
  samples.csv 1s 设备/进程采样
  peak_bw.json 设备峰值写带宽（可选）

输出 metrics.json（四项指标全量数据）：
  wa         各部分写放大（ticker 口径 + 设备口径 + 逐层 + 逐层对）
  stalls     累计停写/分原因/30s 时间序列
  compaction 按输出层与按层对次数/写量、60s 事件分布、并发序列
  bandwidth  设备 1s 带宽曲线、30s 组件带宽曲线、利用率

用法：python3 parse_exp.py <run_dir>
"""

import csv
import datetime
import json
import os
import re
import sys

MB = 2.0 ** 20
GB = 2.0 ** 30

# ---------------------------------------------------------------------------
# stdout：周期 stats dump 块切分
# ---------------------------------------------------------------------------
# 每个 30s 块以 ".... thread 0: (...) ops ... in (...)" 行开头
_BLOCK_MARK = re.compile(
    r"^\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\s+\.\.\. thread 0: .*? in \(", re.M)

_RE_UPTIME = re.compile(r"Uptime\(secs\): ([\d.]+) total, ([\d.]+) interval")
_RE_FLUSH_GB = re.compile(
    r"Flush\(GB\): cumulative ([\d.]+), interval ([\d.]+)")
_RE_COMPACTION_TOTAL = re.compile(
    r"(Cumulative|Interval) compaction: ([\d.]+) GB write, ([\d.]+) MB/s write, "
    r"([\d.]+) GB read, ([\d.]+) MB/s read, ([\d.]+) seconds")
_RE_STALL_CNT = re.compile(r"Write Stall \(count\): (.*)")
_RE_STALL_TIME = re.compile(
    r"(Cumulative|Interval) stall: (\d+):(\d+):([\d.]+) H:M:S, ([\d.]+) percent")
_RE_INGEST = re.compile(r"ingest: ([\d.]+) GB, ([\d.]+) MB/s")
_RE_WAL = re.compile(
    r"Interval WAL: \d+ writes, \d+ syncs, [\d.]+ writes per sync, "
    r"written: ([\d.]+) GB, ([\d.]+) MB/s")
_RE_PENDING = re.compile(r"Estimated pending compaction bytes: (\d+)")
_RE_RUNNING = re.compile(r"num-running-(compactions|flushes): (\d+)")
_RE_PCT = re.compile(
    r"Percentiles:\s*P50:\s*([\d.]+)\s*P75:\s*([\d.]+)\s*P99:\s*([\d.]+)"
    r"\s*P99.9:\s*([\d.]+)\s*P99.99:\s*([\d.]+)")
_RE_BENCH = re.compile(
    r"^\s*(fillrandom)\s*:\s*([\d.]+)\s+micros/op\s+([\d.]+)\s+ops/sec\s+"
    r"([\d.]+)\s+seconds\s+(\d+)\s+operations;\s*([\d.]+)\s*MB/s", re.M)
_RE_TICKER = re.compile(r"^\s*([\w.]+)\s+COUNT\s*:\s*([\d.]+)", re.M)

# Compaction Stats 表头与逐层行（Size 列带单位，如 "676.57 MB"，需按位对齐）
_RE_LEVEL_HEADER = re.compile(
    r"^\*\* Compaction Stats \[default\] \*\*\s*\n\s*Level\s+Files\s+.*$",
    re.M)
_RE_LEVEL_ROW = re.compile(
    r"^\s*(L\d+|Sum|Int)\s+(\d+)/(\d+)\s+([\d.]+)\s+(B|KB|MB|GB|TB)\s+(.*)$")


def _hms_to_sec(h, m, s):
    return int(h) * 3600 + int(m) * 60 + float(s)


def parse_stall_counts_line(text):
    """'k: v, k: v, ...' → dict[str, int]。"""
    out = {}
    for k, v in re.findall(r"([\w-]+): (\d+)", text):
        out[k] = int(v)
    return out


def parse_stdout_blocks(text):
    """stdout → (blocks, tickers, bench, pct)。

    blocks: [{t_uptime, per_level: {L0: {...cumulative...}}, sum: {...},
              int: {...}, flush_gb_cum, flush_gb_int, compact_cum, compact_int,
              stall_counts: {...}, stall_cum_sec, stall_cum_pct,
              stall_int_sec, ingest_int_mb_s, wal_int_mb_s,
              pending_bytes, running_compactions, running_flushes}]
    """
    marks = [m.start() for m in _BLOCK_MARK.finditer(text)]
    blocks = []
    for i, start in enumerate(marks):
        end = marks[i + 1] if i + 1 < len(marks) else len(text)
        chunk = text[start:end]

        uptime_m = _RE_UPTIME.search(chunk)
        if not uptime_m:  # 非周期 dump 的行误匹配，跳过
            continue
        blk = {"t_uptime": float(uptime_m.group(1)),
               "t_interval": float(uptime_m.group(2))}

        # 逐层 Compaction Stats（Size 列值带单位占 2 token，先抽出再对齐表头）
        hm = _RE_LEVEL_HEADER.search(chunk)
        if hm:
            header_tokens = chunk[hm.start():].splitlines()[1].split()
            header = ["Level", "Files"] + header_tokens[2:]
            rows = {}
            for line in chunk[hm.end():].splitlines():
                rm = _RE_LEVEL_ROW.match(line)
                if not rm:
                    continue
                parts = [rm.group(1), f"{rm.group(2)}/{rm.group(3)}",
                         f"{rm.group(4)} {rm.group(5)}"] + rm.group(6).split()
                if len(parts) != len(header):
                    continue
                rows[rm.group(1)] = {h: parts[i]
                                     for i, h in enumerate(header)}
            blk["per_level"] = rows

        fm = _RE_FLUSH_GB.search(chunk)
        if fm:
            blk["flush_gb_cum"] = float(fm.group(1))
            blk["flush_gb_int"] = float(fm.group(2))
        for kind, gb_w, mb_w, gb_r, mb_r, sec in _RE_COMPACTION_TOTAL.findall(chunk):
            blk[f"compact_{kind.lower()}_gb_w"] = float(gb_w)
            blk[f"compact_{kind.lower()}_mb_s_w"] = float(mb_w)
            blk[f"compact_{kind.lower()}_gb_r"] = float(gb_r)
            blk[f"compact_{kind.lower()}_mb_s_r"] = float(mb_r)
            blk[f"compact_{kind.lower()}_sec"] = float(sec)

        # 两处 Write Stall (count) 行合并（CF 原因 + write-buffer-manager）
        stall_counts = {}
        for m in _RE_STALL_CNT.finditer(chunk):
            stall_counts.update(parse_stall_counts_line(m.group(1)))
        blk["stall_counts"] = stall_counts

        for kind, h, m, s, pct in _RE_STALL_TIME.findall(chunk):
            blk[f"stall_{kind.lower()}_sec"] = _hms_to_sec(h, m, s)
            blk[f"stall_{kind.lower()}_pct"] = float(pct)

        ing = _RE_INGEST.findall(chunk)
        if len(ing) >= 2:
            blk["ingest_cum_mb_s"] = float(ing[0][1])
            blk["ingest_int_mb_s"] = float(ing[1][1])
        wm = _RE_WAL.search(chunk)
        if wm:
            blk["wal_int_mb_s"] = float(wm.group(2))
        pm = _RE_PENDING.search(chunk)
        if pm:
            blk["pending_bytes"] = int(pm.group(1))
        blk["running"] = {k: int(v) for k, v in _RE_RUNNING.findall(chunk)}
        blocks.append(blk)

    tickers = {}
    for m in _RE_TICKER.finditer(text):
        try:
            tickers[m.group(1)] = float(m.group(2))
        except ValueError:
            pass

    bench_m = _RE_BENCH.search(text)
    bench = None
    if bench_m:
        bench = dict(us_per_op=float(bench_m.group(2)),
                     ops_per_sec=float(bench_m.group(3)),
                     seconds=float(bench_m.group(4)),
                     operations=int(bench_m.group(5)),
                     mb_per_s=float(bench_m.group(6)))
    # 写延迟百分位：取最后一个 Percentiles 行（前面的属于周期 dump 中的
    # 文件读延迟直方图，最后一个才是结束后 "Microseconds per write" 的）
    pct_m = None
    for pct_m in _RE_PCT.finditer(text):
        pass
    pct = {f"p{n}": float(v) for n, v in zip(
        ["50", "75", "99", "999", "9999"], pct_m.groups())} if pct_m else None
    return blocks, tickers, bench, pct


# ---------------------------------------------------------------------------
# LOG：逐事件解析
# ---------------------------------------------------------------------------
_RE_LOG_TS = re.compile(r"^(\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2})\.\d+")
_RE_COMPACTING = re.compile(
    r"\[default\] \[JOB (\d+)\] Compacting (.+?) files to L(\d+), score ([\d.]+)")
_RE_COMPACTED = re.compile(
    r"\[default\] \[JOB (\d+)\] Compacted (.+?) => (\d+) bytes")
_RE_FLUSH_TABLE = re.compile(
    r"\[default\] \[JOB (\d+)\] Level-0 flush table #(\d+): started")
_RE_FLUSH_JOB = re.compile(
    r"\[default\] \[JOB (\d+)\] Flushing memtable id (\d+)")
_RE_FLUSH_LASTED = re.compile(
    r"\[default\] \[JOB (\d+)\] Flush lasted (\d+) microseconds")


def _pair_key(in_levels, out_level):
    return ",".join(str(x) for x in sorted(in_levels)) + f"->{out_level}"


def _parse_inputs(summary):
    """'7@0 + 1@1 files' → [0, 1]（输入层列表）。"""
    return sorted(int(lev) for _, lev in re.findall(r"(\d+)@(\d+)", summary))


def parse_log_files(paths, run_start_dt=None):
    """LOG* → events 列表与摘要。

    event: {t_s, job, kind: compact|flush, in_levels, out_level, pair,
            score, bytes, flush_table}
    t_s 相对 run 开始（缺 cmd.json 时相对首条事件）。
    """
    events = []
    for p in paths:
        try:
            with open(p, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
        except OSError:
            continue
        for line in lines:
            tm = _RE_LOG_TS.match(line)
            if not tm:
                continue
            t_abs = datetime.datetime.strptime(
                tm.group(1), "%Y/%m/%d-%H:%M:%S").timestamp()
            if (cm := _RE_COMPACTING.search(line)):
                in_levels = _parse_inputs(cm.group(2))
                events.append(dict(t_abs=t_abs, job=int(cm.group(1)),
                                   kind="compact", in_levels=in_levels,
                                   out_level=int(cm.group(3)),
                                   score=float(cm.group(4)),
                                   phase="start"))
            elif (dm := _RE_COMPACTED.search(line)):
                in_levels = _parse_inputs(dm.group(2))
                events.append(dict(t_abs=t_abs, job=int(dm.group(1)),
                                   kind="compact", in_levels=in_levels,
                                   out_level=int(re.search(
                                       r"to L(\d+)", dm.group(2)).group(1)),
                                   bytes=int(dm.group(3)), phase="done"))
            elif (fm := _RE_FLUSH_TABLE.search(line)):
                events.append(dict(t_abs=t_abs, job=int(fm.group(1)),
                                   kind="flush", phase="table",
                                   flush_table=int(fm.group(2))))
            elif (jm := _RE_FLUSH_JOB.search(line)):
                events.append(dict(t_abs=t_abs, job=int(jm.group(1)),
                                   kind="flush", phase="job",
                                   memtable_id=int(jm.group(2))))
            elif (lm := _RE_FLUSH_LASTED.search(line)):
                events.append(dict(t_abs=t_abs, job=int(lm.group(1)),
                                   kind="flush", phase="lasted",
                                   lasted_us=int(lm.group(2))))

    if not events:
        return [], {}
    t0 = run_start_dt.timestamp() if run_start_dt else min(e["t_abs"]
                                                           for e in events)
    for e in events:
        e["t_s"] = round(e["t_abs"] - t0, 1)
        if e["kind"] == "compact":
            e["pair"] = _pair_key(e["in_levels"], e["out_level"])
    return events, dict(t0_local=datetime.datetime.fromtimestamp(t0).strftime(
        "%F %T"))


def bin_events(events, bin_sec=60):
    """事件 → 时间桶 [{t_s, flush_jobs, flush_tables, compact: {pair: n},
    compact_bytes: {pair: n}}]。"""
    n_bins = int(max((e["t_s"] for e in events), default=0) // bin_sec) + 1
    bins = [dict(t_s=i * bin_sec, flush_jobs=0, flush_tables=0,
                 compact={}, compact_bytes={}) for i in range(n_bins)]
    for e in events:
        if e["t_s"] < 0:
            continue
        b = bins[min(int(e["t_s"] // bin_sec), n_bins - 1)]
        if e["kind"] == "flush":
            b["flush_jobs"] += e["phase"] == "lasted"
            b["flush_tables"] += e["phase"] == "table"
        else:
            if e["phase"] == "start":
                b["compact"][e["pair"]] = b["compact"].get(e["pair"], 0) + 1
            else:
                b["compact_bytes"][e["pair"]] = (
                    b["compact_bytes"].get(e["pair"], 0) + e["bytes"])
    return bins


# ---------------------------------------------------------------------------
# samples.csv → 带宽/占用时间序列
# ---------------------------------------------------------------------------
def parse_samples(path):
    rows = []
    num_cols = {"t_s", "nthreads", "utime_s", "stime_s",
                "disk_r_mb", "disk_w_mb", "disk_io_ms"}
    try:
        with open(path, newline="") as f:
            for r in csv.DictReader(f):
                rows.append({k: (float(v) if k in num_cols else v)
                             for k, v in r.items()})
    except OSError:
        return []
    series = []
    for i in range(1, len(rows)):
        a, b = rows[i - 1], rows[i]
        dt = max(b["t_s"] - a["t_s"], 0.001)
        series.append(dict(
            t_s=round(b["t_s"], 1),
            disk_w_mb=b["disk_w_mb"], disk_r_mb=b["disk_r_mb"],
            disk_w_mb_s=round(max(b["disk_w_mb"] - a["disk_w_mb"], 0) / dt, 2),
            disk_r_mb_s=round(max(b["disk_r_mb"] - a["disk_r_mb"], 0) / dt, 2),
            io_util_pct=round(min(max(
                (b["disk_io_ms"] - a["disk_io_ms"]) / (dt * 1000.0), 0), 1)
                * 100, 1),
            cpu_pct=round(min(max(
                (b["utime_s"] + b["stime_s"] - a["utime_s"] - a["stime_s"])
                / dt, 0), 1) * 100, 1),
            nthreads=int(b["nthreads"])))
    return series


# ---------------------------------------------------------------------------
# 组装 metrics
# ---------------------------------------------------------------------------
def build_metrics(run_dir):
    def _read(name):
        try:
            with open(os.path.join(run_dir, name), "r",
                      encoding="utf-8", errors="replace") as f:
                return f.read()
        except OSError:
            return ""

    cmd = {}
    try:
        with open(os.path.join(run_dir, "cmd.json")) as f:
            cmd = json.load(f)
    except OSError:
        pass
    result = {}
    try:
        with open(os.path.join(run_dir, "result.json")) as f:
            result = json.load(f)
    except OSError:
        pass
    peak_bw = {}
    try:
        with open(os.path.join(run_dir, "peak_bw.json")) as f:
            peak_bw = json.load(f)
    except OSError:
        pass

    stdout = _read("stdout.txt")
    blocks, tickers, bench, pct = parse_stdout_blocks(stdout)

    run_start_dt = None
    try:
        run_start_dt = datetime.datetime.strptime(
            cmd["started"], "%Y/%m/%d %H:%M:%S") if cmd.get("started") else None
    except (ValueError, KeyError):
        pass
    log_paths = sorted(os.path.join(run_dir, n) for n in os.listdir(run_dir)
                       if n.startswith("LOG"))
    events, t0_info = parse_log_files(log_paths, run_start_dt)
    bins = bin_events(events)
    samples = parse_samples(os.path.join(run_dir, "samples.csv"))

    # ---- 最终累计口径：取最后一个 dump 块 ----
    last = blocks[-1] if blocks else {}
    per_level = {}
    for lv, row in last.get("per_level", {}).items():
        if lv in ("Sum", "Int"):
            continue
        per_level[lv] = dict(
            write_gb=float(row["Write(GB)"]), read_gb=float(row["Read(GB)"]),
            cnt=int(float(row["Comp(cnt)"])),
            wamp=float(row["W-Amp"]), wr_mb_s=float(row["Wr(MB/s)"]))
    sum_row = last.get("per_level", {}).get("Sum", {})

    # ---- WA 分解 ----
    num = cmd.get("num") or 0
    vsz = cmd.get("value_size") or 0
    user_bytes = num * (16 + vsz) if num and vsz else None
    t = lambda k: tickers.get(k)  # noqa: E731
    wal_b = t("rocksdb.wal.bytes")
    flush_b = t("rocksdb.flush.write.bytes")
    compact_b = t("rocksdb.compact.write.bytes")
    compact_r = t("rocksdb.compact.read.bytes")
    stall_us = t("rocksdb.stall.micros")

    # 层对写量/次数（LOG）。单输入 n→n+1 可能是 trivial move（字节计入
    # Moved 而非真实写盘），单独打标
    pair_cnt, pair_bytes, pair_trivial = {}, {}, 0
    for e in events:
        if e["kind"] != "compact":
            continue
        if e["phase"] == "start":
            pair_cnt[e["pair"]] = pair_cnt.get(e["pair"], 0) + 1
            if (len(e["in_levels"]) == 1
                    and e["out_level"] == e["in_levels"][0] + 1):
                pair_trivial += 1
        else:
            pair_bytes[e["pair"]] = pair_bytes.get(e["pair"], 0) + e["bytes"]
    flush_tables = sum(1 for e in events
                       if e["kind"] == "flush" and e["phase"] == "table")
    flush_jobs = sum(1 for e in events
                     if e["kind"] == "flush" and e["phase"] == "lasted")
    compact_start_cnt = sum(pair_cnt.values())

    # 设备总写（samples 首尾差分）
    disk_w_bytes = None
    if len(samples) >= 2:
        disk_w_bytes = int(round(
            max(samples[-1]["disk_w_mb"] - samples[0]["disk_w_mb"], 0) * MB))

    def _div(x, y):
        return round(x / y, 3) if x is not None and y else None

    wa = dict(
        user_bytes=user_bytes,
        user_gb=round(user_bytes / GB, 2) if user_bytes else None,
        wal_bytes=wal_b, flush_bytes=flush_b, compact_bytes=compact_b,
        compact_read_bytes=compact_r,
        wa_wal=_div(wal_b, user_bytes),
        wa_flush=_div(flush_b, user_bytes),
        wa_compact=_div(compact_b, user_bytes),
        wa_total_ticker=_div((wal_b or 0) + (flush_b or 0)
                             + (compact_b or 0), user_bytes),
        disk_write_bytes=disk_w_bytes,
        wa_device=_div(disk_w_bytes, user_bytes),
        per_level=per_level,
        per_pair_cnt=pair_cnt,
        per_pair_bytes=pair_bytes,
        trivial_move_likely=pair_trivial,
        per_level_note=("L0 行 Write/Comp(cnt) 含 flush 输出（RocksDB 语义），"
                        "compaction-only 口径以 per_pair_* 为准"),
        compact_jobs=compact_start_cnt,
        flush_jobs=flush_jobs,
        flush_tables=flush_tables,
    )

    # ---- 停写 ----
    stalls = dict(
        stall_micros=stall_us,
        stall_sec=_div(stall_us, 1e6),
        stall_cum_sec=last.get("stall_cumulative_sec"),
        stall_cum_pct=last.get("stall_cumulative_pct"),
        stall_reasons=last.get("stall_counts", {}),
        series=[dict(t_s=round(b["t_uptime"], 1),
                     stall_int_sec=b.get("stall_interval_sec"),
                     stall_int_pct=b.get("stall_interval_pct"),
                     total_delays=b.get("stall_counts", {}).get(
                         "total-delays"),
                     total_stops=b.get("stall_counts", {}).get("total-stops"),
                     ingest_int_mb_s=b.get("ingest_int_mb_s"),
                     wal_int_mb_s=b.get("wal_int_mb_s"),
                     pending_bytes=b.get("pending_bytes"),
                     running_compactions=b.get("running", {}).get(
                         "compactions"),
                     running_flushes=b.get("running", {}).get("flushes"))
                for b in blocks],
    )

    # ---- compaction 位置/次数 ----
    compaction = dict(
        per_level_cnt={lv: v["cnt"] for lv, v in per_level.items()},
        per_pair_cnt=pair_cnt,
        per_pair_bytes=pair_bytes,
        flush_jobs=flush_jobs, flush_tables=flush_tables,
        compact_jobs=compact_start_cnt,
        sum_write_gb=float(sum_row["Write(GB)"]) if sum_row else None,
        sum_read_gb=float(sum_row["Read(GB)"]) if sum_row else None,
        sum_cnt=int(float(sum_row["Comp(cnt)"])) if sum_row else None,
        bins_60s=bins,
        series_running=[dict(t_s=round(b["t_uptime"], 1),
                             compactions=b.get("running", {}).get(
                                 "compactions"),
                             flushes=b.get("running", {}).get("flushes"))
                        for b in blocks],
    )

    # ---- 带宽 ----
    peak = peak_bw.get("peak_write_mb_s")
    disk_series = [dict(s) for s in samples]
    for s in disk_series:
        s["util_pct"] = round(s["disk_w_mb_s"] / peak * 100, 1) if peak else None
    # 30s 组件级：逐层 Write(GB) 累计差分 + flush/ingest 区间值
    comp_series = []
    prev_level = {}
    for i, b in enumerate(blocks):
        dt = b.get("t_interval") or 30.0
        row = dict(t_s=round(b["t_uptime"], 1),
                   flush_int_mb_s=round(
                       b.get("flush_gb_int", 0) * GB / dt / MB, 1),
                   compaction_int_mb_s=b.get("compact_interval_mb_s_w"),
                   ingest_int_mb_s=b.get("ingest_int_mb_s"),
                   wal_int_mb_s=b.get("wal_int_mb_s"),
                   per_level_mb_s={})
        for lv, v in b.get("per_level", {}).items():
            if lv in ("Sum", "Int"):
                continue
            cur = float(v["Write(GB)"])
            prev = prev_level.get(lv, 0.0)
            dt = b["t_interval"] or 30.0
            row["per_level_mb_s"][lv] = round(
                (cur - prev) * GB / dt / MB, 1) if cur >= prev else None
            prev_level[lv] = cur
        comp_series.append(row)
    bandwidth = dict(
        peak_write_mb_s=peak,
        peak_bw_note=peak_bw.get("note"),
        device_series=disk_series,
        component_series=comp_series,
        avg_disk_w_mb_s=round(
            sum(s["disk_w_mb_s"] for s in disk_series) / len(disk_series), 1)
        if disk_series else None,
        max_disk_w_mb_s=max((s["disk_w_mb_s"] for s in disk_series),
                            default=None),
        avg_util_pct=round(sum(s["util_pct"] for s in disk_series)
                           / len(disk_series), 1)
        if disk_series and peak else None,
        avg_io_busy_pct=round(sum(s["io_util_pct"] for s in disk_series)
                              / len(disk_series), 1) if disk_series else None,
    )

    # ---- 交叉核对 ----
    # Sum Write(GB) 的 L0 行含 flush 输出：flush ticker ↔ L0 行写量；
    # compaction ticker ↔ Sum − flush(最后 dump 累计)
    checks = {}
    l0_write_gb = per_level.get("L0", {}).get("write_gb")
    if flush_b is not None and l0_write_gb is not None:
        checks["flush_bytes_vs_l0_write_gb"] = round(
            flush_b / GB - l0_write_gb, 2)
    if compact_b and sum_row and last.get("flush_gb_cum") is not None:
        checks["compact_bytes_vs_sum_minus_flush_gb"] = round(
            compact_b / GB - (float(sum_row["Write(GB)"])
                              - last["flush_gb_cum"]), 2)
    if stall_us is not None and last.get("stall_cumulative_sec") is not None:
        checks["stall_us_vs_cum_sec"] = round(
            stall_us / 1e6 - last["stall_cumulative_sec"], 3)
    if pair_bytes and compact_b:
        checks["pair_bytes_vs_ticker_gb"] = round(
            (sum(pair_bytes.values()) - compact_b) / GB, 2)

    return dict(
        meta=dict(cmd=cmd, result=result, t0=t0_info,
                  rocksdb_version="11.2.0 (vanilla)",
                  data_sources=dict(
                      blocks=len(blocks), log_events=len(events),
                      log_files=log_paths, samples=len(samples))),
        throughput=dict(bench=bench, percentiles=pct),
        wa=wa, stalls=stalls, compaction=compaction, bandwidth=bandwidth,
        cross_checks=checks,
    )


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    run_dir = sys.argv[1]
    metrics = build_metrics(run_dir)
    out = os.path.join(run_dir, "metrics.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    m = metrics
    print(f"→ {out}")
    print(f"throughput: {m['throughput']['bench']}")
    print(f"WA: total(ticker)={m['wa']['wa_total_ticker']} "
          f"wal={m['wa']['wa_wal']} flush={m['wa']['wa_flush']} "
          f"compact={m['wa']['wa_compact']} "
          f"device={m['wa']['wa_device']}")
    print(f"stall: {m['stalls']['stall_cum_sec']}s "
          f"({m['stalls']['stall_cum_pct']}%) "
          f"reasons={m['stalls']['stall_reasons']}")
    print(f"compaction: {m['compaction']['compact_jobs']} jobs, "
          f"flush {m['compaction']['flush_jobs']} jobs, "
          f"per_pair={m['compaction']['per_pair_cnt']}")
    print(f"bandwidth: peak={m['bandwidth']['peak_write_mb_s']} "
          f"avg={m['bandwidth']['avg_disk_w_mb_s']} "
          f"util={m['bandwidth']['avg_util_pct']}%")
    print(f"cross_checks: {m['cross_checks']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
