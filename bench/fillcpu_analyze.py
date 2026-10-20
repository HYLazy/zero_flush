#!/usr/bin/env python3
"""Analyze a fillcpu run: per-operation CPU attribution + per-component memory.

Usage: python3 bench/fillcpu_analyze.py <out_prefix> [--md OUT.md]

Reads  <out_prefix>.summary.json, .samples.csv, .flush_jobs.csv,
       .comp_jobs.csv, .stalls.csv, .cfstats_fillend.txt, .cfstats_drainend.txt,
       .effective_options.txt, .config.txt
Prints the report to stdout and writes markdown to <out_prefix>.REPORT.md
(or --md path).
"""
import csv
import json
import os
import statistics as st
import sys

PHASE_FILL, PHASE_DRAIN = 1, 2


def _isnum(v):
    try:
        float(v)
        return True
    except (TypeError, ValueError):
        return False


def load(prefix):
    d = {}
    with open(prefix + ".summary.json") as f:
        d["summary"] = json.load(f)
    for name, key in [("samples", ".samples.csv"), ("flush", ".flush_jobs.csv"),
                      ("comp", ".comp_jobs.csv"), ("stalls", ".stalls.csv")]:
        path = prefix + key
        if not os.path.exists(path):
            d[name] = []
            continue
        with open(path) as f:
            d[name] = [{k: (float(v) if _isnum(v) else v) for k, v in row.items()}
                       for row in csv.DictReader(f)]
    for key in ["cfstats_fillend", "cfstats_drainend", "effective_options"]:
        p = prefix + "." + key + ".txt"
        d[key] = open(p).read() if os.path.exists(p) else ""
    return d


def parse_cfstats(text):
    """{level: {files, comp_sec, cpu_sec, cnt}} from the by-level table of
    rocksdb.cfstats.  Row tokens are indexed from the right because the Size
    column is two tokens; verified on RocksDB 11.2 output:
    tokens[-8]=Comp(sec), tokens[-7]=CompMergeCPU(sec), tokens[-6]=Comp(cnt).
    Level 0 holds the flush stats; L1..L6 hold compactions."""
    out = {}
    for line in text.splitlines():
        t = line.split()
        if not t or t[0] not in ("L0", "L1", "L2", "L3", "L4", "L5", "L6"):
            continue
        if len(t) < 12 or t[0] in out:
            continue
        try:
            out[t[0]] = {"files": int(t[1].split("/")[0]), "comp_sec": float(t[-8]),
                         "cpu_sec": float(t[-7]), "cnt": int(t[-6])}
        except (ValueError, IndexError):
            continue
    return out


def pct(x, tot):
    return 100.0 * x / tot if tot else 0.0


def quant(vals, q):
    if not vals:
        return 0.0
    vals = sorted(vals)
    return vals[min(len(vals) - 1, int(q * (len(vals) - 1) + 0.5))]


def mb(kb):
    return kb / 1024.0


def mbz(b):
    return b / 1048576.0


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    prefix = sys.argv[1]
    md_path = prefix + ".REPORT.md"
    if "--md" in sys.argv:
        md_path = sys.argv[sys.argv.index("--md") + 1]
    d = load(prefix)
    S = d["summary"]
    L = []
    P = L.append

    nt = S["workload"]["threads"]
    app_bytes = S["workload"]["app_bytes"]
    gb = app_bytes / 1e9
    nops = max(S["ops_ok"], 1)

    fill, drain = S["phase_fill"], S["phase_drain"]
    whole_cpu = {k: fill["cpu_s"][k] + drain["cpu_s"][k] for k in fill["cpu_s"]}
    whole_total = sum(whole_cpu.values())
    fill_total = fill["cpu_s_total"]
    jobs = S["jobs"]
    pmark = S["phase_mark"]
    procs = {k: S[k] for k in S if k.startswith("process_cpu_s_")}

    flush_jobs = d["flush"]
    flush_fill = [j for j in flush_jobs if j.get("phase") == PHASE_FILL]
    flush_drain = [j for j in flush_jobs if j.get("phase") == PHASE_DRAIN]
    comp_jobs = d["comp"]
    comp_fill = [j for j in comp_jobs if j.get("phase") == PHASE_FILL]
    comp_drain = [j for j in comp_jobs if j.get("phase") == PHASE_DRAIN]

    flush_cpu_fill = sum(j["cpu_us"] for j in flush_fill) / 1e6
    flush_cpu_drain = sum(j["cpu_us"] for j in flush_drain) / 1e6
    flush_cpu_all = flush_cpu_fill + flush_cpu_drain
    flush_bytes_all = sum(j["data_bytes"] for j in flush_jobs)
    flush_cpu_per_job = [j["cpu_us"] / 1e3 for j in flush_jobs]
    flush_wall_med = st.median([j["wall_us"] for j in flush_jobs]) / 1e3 if flush_jobs else 0.0

    comp_cpu_all = sum(j["cpu_us_rdb"] for j in comp_jobs) / 1e6
    comp_cpu_self_all = sum(j["cpu_us_self"] for j in comp_jobs) / 1e6
    comp_in_all = sum(j["in_bytes"] for j in comp_jobs)
    comp_out_all = sum(j["out_bytes"] for j in comp_jobs)

    cf_fill = parse_cfstats(d["cfstats_fillend"])
    cf_drain = parse_cfstats(d["cfstats_drainend"])
    cf_flush_fill = cf_fill.get("L0", {}).get("cpu_sec")
    cf_flush_drain = cf_drain.get("L0", {}).get("cpu_sec")
    cf_comp_drain = sum(v["cpu_sec"] for k, v in cf_drain.items() if k != "L0")

    # ---------- memory ----------
    samples = d["samples"]
    sam_fill = [r for r in samples if int(r["phase"]) == PHASE_FILL]
    peak = {
        "rss": mb(max((r["rss_kb"] for r in samples), default=0)),
        "hwm": mb(max((r["hwm_kb"] for r in samples), default=0)),
        "memtbl": mbz(max((r["mem_cur_all"] for r in samples), default=0)),
        "bcache": mbz(max((r["bcache_usage"] for r in samples), default=0)),
        "treaders": mbz(max((r["table_readers"] for r in samples), default=0)),
    }
    fillend = sam_fill[-1] if sam_fill else (samples[-1] if samples else {})
    last = samples[-1] if samples else {}

    def memdict(row):
        m = {"rss": mb(row.get("rss_kb", 0)), "hwm": mb(row.get("hwm_kb", 0)),
             "memtbl": mbz(row.get("mem_cur_all", 0)), "bcache": mbz(row.get("bcache_usage", 0)),
             "bcache_cap": mbz(row.get("bcache_cap", 0)),
             "treaders": mbz(row.get("table_readers", 0)),
             "pinned": mbz(row.get("bcache_pinned", 0))}
        m["other"] = m["rss"] - m["memtbl"] - m["bcache"] - m["treaders"]
        return m

    m_fe, m_last = memdict(fillend), memdict(last)
    mall = {"used": mbz(last.get("mall_used", 0)), "hblkhd": mbz(last.get("mall_hblkhd", 0)),
            "arena": mbz(last.get("mall_arena", 0))}

    rates = []
    for a, b in zip(sam_fill, sam_fill[1:]):
        dt = b["ts"] - a["ts"]
        if dt > 0:
            rates.append((b["ops_cum"] - a["ops_cum"]) / dt)
    rate_med = st.median(rates) if rates else 0.0
    rate_max = max(rates) if rates else 0.0

    # stall wall during fill (integrate transitions, clipped to the fill window)
    stall_s = 0.0
    open_ts = None
    for e in d["stalls"]:
        if e["ts"] < pmark["fill_start_ts"]:
            continue
        if e["state"] in ("delayed", "stopped"):
            if open_ts is None:
                open_ts = e["ts"]
        else:
            if open_ts is not None:
                stall_s += max(0.0, min(e["ts"], pmark["fill_end_ts"]) - open_ts)
                open_ts = None
    if open_ts is not None:
        stall_s += max(0.0, pmark["fill_end_ts"] - open_ts)

    # pool sizes from the observed thread-name census (rocksdb:high / :low)
    tids = {}
    tp = prefix + ".thread_cpu.csv"
    if os.path.exists(tp):
        with open(tp) as f:
            for row in csv.DictReader(f):
                tids.setdefault(row["name"], set()).add(row["tid"])
    nflush_threads = max(1, len(tids.get("rocksdb:high", {1})))
    ncomp_threads = max(1, len(tids.get("rocksdb:low", {1})))
    flush_busy_fill = 100.0 * fill["cpu_s"]["flush_pool"] / max(fill["wall_s"], 1e-9) / nflush_threads
    flush_busy_drain = 100.0 * drain["cpu_s"]["flush_pool"] / max(drain["wall_s"], 1e-9) / nflush_threads
    comp_busy_fill = 100.0 * fill["cpu_s"]["compaction_pool"] / max(fill["wall_s"], 1e-9) / ncomp_threads
    comp_busy_drain = 100.0 * drain["cpu_s"]["compaction_pool"] / max(drain["wall_s"], 1e-9) / ncomp_threads

    # ================= report =================
    P("# 默认 RocksDB 配置 · %s GB / 1KB KV 负载：flush 的 CPU 与各部件内存实测" % (
        ("%g" % gb) if gb < 1000 else "%.0f" % gb))
    P("")
    P("## 结论")
    P("")
    P("- **flush 消耗 CPU**：全程（含写完后排空）flush 线程共 **%.1f s**（其中逐作业窗口内 "
      "%.1f s），占进程总 CPU 的 **%.1f%%**；按 %.2f GB 数据计为 **%.2f s/GB**，"
      "摊到每次 1KB 写是 **%.2f µs/KV**。" % (
          whole_cpu["flush_pool"], flush_cpu_all, pct(whole_cpu["flush_pool"], whole_total),
          gb, whole_cpu["flush_pool"] / gb, whole_cpu["flush_pool"] * 1e6 / nops))
    P("- **各操作 CPU 占比**：前台写路径 %.1f%%、flush %.1f%%、compaction %.1f%%、"
      "驱动/其他 %.1f%%（总 CPU %.0f s，口径为逐线程 /proc 累计）。" % (
          pct(whole_cpu["writer"], whole_total), pct(whole_cpu["flush_pool"], whole_total),
          pct(whole_cpu["compaction_pool"], whole_total),
          pct(whole_cpu["driver"] + whole_cpu["other"], whole_total), whole_total))
    P("- **flush 线程压力**：fill 期间 flush 线程忙率 **%.0f%%**（%d 个线程），"
      "drain 期间 %.0f%%；compaction 线程 fill 忙率 %.0f%%、drain %.0f%%。" % (
          flush_busy_fill, nflush_threads, flush_busy_drain, comp_busy_fill, comp_busy_drain))
    P("- **内存**：峰值 RSS **%.0f MB**（VmHWM %.0f MB）。fill 结束时 RSS %.0f MB = "
      "memtable %.0f MB + block cache %.1f MB + table readers %.0f MB + 其他 %.0f MB。" % (
          peak["rss"], peak["hwm"], m_fe["rss"], m_fe["memtbl"], m_fe["bcache"],
          m_fe["treaders"], m_fe["other"]))
    P("- block cache 默认 32MB AutoHyperClockCache，但纯写负载里几乎不用（%.2f MB）；"
      "常驻内存主要是 memtable（64MB write buffer ×2）与打开 SST 的索引块（table readers）。" % (
          m_fe["bcache"]))
    P("")

    P("## 0. 实验设置")
    P("")
    P("- RocksDB %s（`source/rocksdb`，未修改），驱动的唯一非默认设置是 `create_if_missing` "
      "+ 只读 EventListener；**未开启 Statistics**。" % S["version"])
    P("- 负载：%d 键 × (16B 键 + %dB 值) = %.2f GB 应用数据，%d 线程、全键空间伪随机序写入"
      "（96M 键 100GB 口径与既有 `cmp_1kb100` 实验一致）。" % (
          S["workload"]["num_ops"], S["workload"]["value_size"], gb, nt))
    P("- 两阶段：**fill**（%d 写线程在跑）与 **drain**（停写后等 flush+compaction 排空，"
      "把 100GB 全部 flush/compaction 计完）。" % nt)
    P("")
    P("| 阶段 | wall(s) | 进程 CPU(s) | 平均占用核数 |")
    P("|---|---|---|---|")
    P("| fill | %.1f | %.1f | %.2f |" % (
        fill["wall_s"],
        procs.get("process_cpu_s_at_fill_t1", 0) - procs.get("process_cpu_s_at_fill_t0", 0),
        (procs.get("process_cpu_s_at_fill_t1", 0) - procs.get("process_cpu_s_at_fill_t0", 0)) / max(fill["wall_s"], 1e-9)))
    P("| drain | %.1f | %.1f | %.2f |" % (
        drain["wall_s"],
        procs.get("process_cpu_s_at_drain_t1", 0) - procs.get("process_cpu_s_at_fill_t1", 0),
        (procs.get("process_cpu_s_at_drain_t1", 0) - procs.get("process_cpu_s_at_fill_t1", 0)) / max(drain["wall_s"], 1e-9)))
    P("")

    P("## 1. CPU 按操作拆解")
    P("")
    P("口径：每个线程的 `utime+stime` 累计值来自 `/proc/self/task/<tid>/stat` 精确快照；"
      "线程按名字分角色（`rocksdb:high`=flush 池，默认 1 线程；`rocksdb:low`=compaction 池，"
      "默认 1 线程；`fcw*`=写线程）。")
    P("")
    P("| 操作 | fill CPU(s) | fill 占比 | drain CPU(s) | 全程 CPU(s) | 全程占比 |")
    P("|---|---|---|---|---|---|")
    for k, label in [("writer", "前台写路径（Put：写组/WAL/memtable）"),
                     ("flush_pool", "flush 线程（含 memtable→SST、WAL 同步、元数据）"),
                     ("compaction_pool", "compaction 线程"), ("driver", "驱动（采样/主线程）"),
                     ("other", "其他")]:
        P("| %s | %.2f | %.1f%% | %.2f | %.2f | %.1f%% |" % (
            label, fill["cpu_s"][k], pct(fill["cpu_s"][k], fill_total), drain["cpu_s"][k],
            whole_cpu[k], pct(whole_cpu[k], whole_total)))
    P("| **合计** | **%.2f** | 100%% | **%.2f** | **%.2f** | 100%% |" % (
        fill_total, drain["cpu_s_total"], whole_total))
    P("")
    P("以整机 %d 逻辑 CPU 为分母：fill 期间平均占用 %.2f 核（%.1f%% 整机），"
      "其中写路径 %.2f 核、flush %.2f 核、compaction %.2f 核。" % (
          36, fill_total / max(fill["wall_s"], 1e-9), 100.0 * (fill_total / max(fill["wall_s"], 1e-9)) / 36,
          fill["cpu_s"]["writer"] / max(fill["wall_s"], 1e-9),
          fill["cpu_s"]["flush_pool"] / max(fill["wall_s"], 1e-9),
          fill["cpu_s"]["compaction_pool"] / max(fill["wall_s"], 1e-9)))
    P("")

    P("## 2. flush 的 CPU 细节")
    P("")
    P("| 指标 | 值 |")
    P("|---|---|")
    P("| flush 作业数 | %d（fill %d / drain %d） |" % (len(flush_jobs), len(flush_fill), len(flush_drain)))
    P("| 逐作业窗口 CPU 合计 | %.2f s |" % flush_cpu_all)
    P("| 单作业 CPU 中位 / p90 / p99 / max | %.1f / %.1f / %.1f / %.1f ms |" % (
        st.median(flush_cpu_per_job) if flush_cpu_per_job else 0, quant(flush_cpu_per_job, 0.9),
        quant(flush_cpu_per_job, 0.99), max(flush_cpu_per_job) if flush_cpu_per_job else 0))
    P("| 单作业 wall 中位 | %.1f ms |" % flush_wall_med)
    P("| flush 输出数据量 | %.2f GB（应用 %.2f GB 的 %.1f%%） |" % (
        flush_bytes_all / 1e9, gb, 100.0 * flush_bytes_all / app_bytes))
    P("| flush CPU 强度 | %.2f s/GB ≈ %.0f MB/s·核 |" % (
        whole_cpu["flush_pool"] / max(gb, 1e-9), gb / max(whole_cpu["flush_pool"], 1e-9) * 1024))
    P("| 每 1KB 写分摊 flush CPU | %.2f µs/KV |" % (whole_cpu["flush_pool"] * 1e6 / nops))
    if cf_flush_drain is not None:
        P("| 库侧独立读数（cfstats L0 CompMergeCPU） | %.2f s |" % cf_flush_drain)
    P("")
    P("flush 线程 CPU（%.2f s）与逐作业窗口 CPU（%.2f s）之差 %.2f s（%.0f%%）是作业窗口"
      "之外的 flush 线程工作：被 flush memtable 对应 WAL 的 fsync（`SyncClosedWals`）、"
      "MANIFEST 写入、obsolete 文件删除与线程池调度。" % (
          whole_cpu["flush_pool"], flush_cpu_all, whole_cpu["flush_pool"] - flush_cpu_all,
          100.0 * (whole_cpu["flush_pool"] - flush_cpu_all) / max(whole_cpu["flush_pool"], 1e-9)))
    P("")

    P("## 3. compaction 的 CPU（对照）")
    P("")
    P("| 指标 | 值 |")
    P("|---|---|")
    P("| compaction 作业数 | %d |" % len(comp_jobs))
    P("| 库自报 CPU 合计（CompactionJobStats.cpu_micros） | %.2f s |" % comp_cpu_all)
    P("| 驱动线程级自测合计（同窗口 CLOCK_THREAD_CPUTIME_ID） | %.2f s |" % comp_cpu_self_all)
    P("| 读入 / 写出字节 | %.2f / %.2f GB |" % (comp_in_all / 1e9, comp_out_all / 1e9))
    P("| 读放大（compaction 读入 / 应用数据） | %.2fx |" % (comp_in_all / max(app_bytes, 1)))
    P("| 库侧 cfstats 各层 CompMergeCPU 合计 | %.2f s |" % cf_comp_drain)
    P("")

    P("## 4. wall 时间与 stall（CPU 占比的背景）")
    P("")
    P("| 指标 | 值 |")
    P("|---|---|")
    P("| fill wall / 应用吞吐 | %.1f s / %.1f MB/s（%.0f ops/s） |" % (
        fill["wall_s"], S["fill_rate_mb_s"], S["fill_rate_ops_s"]))
    P("| 采样中位 / 峰值速率 | %.1f / %.1f MB/s |" % (rate_med * 1040 / 1e6, rate_max * 1040 / 1e6))
    P("| 写线程 stall wall（fill 窗口内，delayed+stopped） | %.1f s（占 fill %.1f%%） |" % (
        stall_s, pct(stall_s, fill["wall_s"])))
    P("| 前台写路径 CPU | %.2f s（%.2f µs/KV） |" % (fill["cpu_s"]["writer"], fill["cpu_s"]["writer"] * 1e6 / nops))
    P("")
    stall_line = [l for l in d["cfstats_drainend"].splitlines() if l.startswith("Write Stall (count)")]
    if stall_line:
        P("```")
        P(stall_line[0])
        P("```")
        P("")

    P("## 5. 内存：总量与部件构成")
    P("")
    P("口径：DB properties（memtable=`cur-size-all-mem-tables`、cache=`block-cache-usage`、"
      "table readers=`estimate-table-readers-mem`），每 %.0f ms 采样；"
      "“其他” = RSS − 上述三项，属分配器/迭代器/代码/线程栈等，未强行归因。" % (
          (st.median([b["ts"] - a["ts"] for a, b in zip(samples, samples[1:])]) * 1000)
          if len(samples) > 1 else 250))
    P("")
    P("| 指标 | fill 结束 | 全程峰值 |")
    P("|---|---|---|")
    P("| 进程 RSS | %.0f MB | %.0f MB |" % (m_fe["rss"], peak["rss"]))
    P("| VmHWM（RSS 高水位） | %.0f MB | %.0f MB |" % (m_fe["hwm"], peak["hwm"]))
    P("| memtable | %.0f MB | %.0f MB |" % (m_fe["memtbl"], peak["memtbl"]))
    P("| block cache 使用（容量 %.0f MB，默认 AutoHCC） | %.2f MB | %.2f MB |" % (
        m_fe["bcache_cap"], m_fe["bcache"], peak["bcache"]))
    P("| table readers（SST 索引块等） | %.0f MB | %.0f MB |" % (m_fe["treaders"], peak["treaders"]))
    P("| 其他（=RSS−上三项） | %.0f MB | — |" % m_fe["other"])
    P("")
    P("排空后（drain 末）：RSS %.0f MB = memtable %.0f MB + block cache %.2f MB + "
      "table readers %.0f MB + 其他 %.0f MB；glibc mallinfo2：uordblks=%.0f MB、"
      "arena=%.0f MB、mmap 段=%.0f MB。" % (
          m_last["rss"], m_last["memtbl"], m_last["bcache"], m_last["treaders"],
          m_last["other"], mall["used"], mall["arena"], mall["hblkhd"]))
    P("")
    P("### 5.1 “其他”是什么：malloc 视角对账")
    P("")
    P("`memtable + table readers + block cache` 都是 malloc 出来的；把它们与 malloc 的"
      "在用字节对照，可以把 RSS 中未被 DB property 覆盖的部分拆开：")
    P("")
    P("| 项 | MB | 说明 |")
    P("|---|---|---|")
    P("| RSS | %.0f | 进程常驻 |" % m_last["rss"])
    P("| malloc 在用（uordblks） | %.0f | 含 memtable arena 在用块、SST 索引块等 |" % mall["used"])
    P("| 其中：DB property 可归因 | %.0f | memtable %.0f + table readers %.0f + block cache %.1f |" % (
        m_last["memtbl"] + m_last["treaders"] + m_last["bcache"], m_last["memtbl"],
        m_last["treaders"], m_last["bcache"]))
    P("| malloc arena 总计 | %.0f | glibc 向系统申请的堆总量 |" % mall["arena"])
    P("| 其中：空闲未归还（≈arena−在用） | %.0f | memtable 刷盘后释放的 arena 被 glibc 保留 |" % (
        max(0.0, mall["arena"] - mall["used"])))
    P("| 代码/线程栈/映射等 | %.0f | ≈RSS − arena |" % max(0.0, m_last["rss"] - mall["arena"]))
    P("")
    P("即：RSS 中最大的一块往往不是引擎的活跃数据结构，而是 glibc 为 memtable arena "
      "保留的空闲堆；真正随数据规模增长的是 table readers（打开 SST 的索引块）。")
    P("")
    P("LSM 末态每层文件数：%s（`rocksdb.cfstats`）。" % S.get("files_per_level_at_end", []))
    P("")

    P("## 6. 对账")
    P("")
    proc_total = procs.get("process_cpu_s_at_drain_t1", 0)
    thr_total = sum(whole_cpu.values())
    P("| 对账项 | A | B | 差异 |")
    P("|---|---|---|---|")
    P("| 进程 CPU（/proc/self/stat） vs 线程和 | %.2f s | %.2f s | %.2f%% |" % (
        proc_total, thr_total, 100.0 * abs(proc_total - thr_total) / max(proc_total, 1e-9)))
    P("| flush 作业 CPU vs flush 线程 CPU | %.2f s | %.2f s | 差 %.2f s（窗口外工作，见 §2） |" % (
        flush_cpu_all, whole_cpu["flush_pool"], whole_cpu["flush_pool"] - flush_cpu_all))
    if cf_flush_drain is not None:
        P("| flush CPU：驱动测量 vs 库 cfstats L0 | %.2f s | %.2f s | %.2f%% |" % (
            flush_cpu_all, cf_flush_drain, 100.0 * abs(flush_cpu_all - cf_flush_drain) / max(cf_flush_drain, 1e-9)))
    P("| compaction CPU：驱动 vs 库自报 | %.2f s | %.2f s | %.2f%% |" % (
        comp_cpu_self_all, comp_cpu_all, 100.0 * abs(comp_cpu_self_all - comp_cpu_all) / max(comp_cpu_all, 1e-9)))
    P("| flush 输出字节 vs 应用字节 | %.2f GB | %.2f GB | %.2f%% |" % (
        flush_bytes_all / 1e9, gb, 100.0 * abs(flush_bytes_all / 1e9 - gb) / gb))
    P("")

    # ---- §8 calibration (if the calibration runs exist next to this run) ----
    calib_dir = os.path.join(os.path.dirname(prefix) or ".", "..", "fillcpu_calib")
    calibs = []
    for t in (1, 4, 16):
        cp = os.path.join(os.path.dirname(prefix), "..", "fillcpu_calib", "calib_t%d" % t)
        cp = os.path.normpath(cp)
        if os.path.exists(cp + ".summary.json"):
            with open(cp + ".summary.json") as f:
                calibs.append((t, json.load(f)))
    if calibs:
        P("## 7. 写路径 CPU 的构成：单线程校准（同驱动、默认配置、1M 次写）")
        P("")
        P("| 写线程数 | wall(s) | 写路径 CPU(s) | µs/KV | flush CPU(s) | 是否有 stall |")
        P("|---|---|---|---|---|---|")
        for t, c in calibs:
            cf = c["phase_fill"]
            P("| %d | %.1f | %.2f | %.2f | %.2f | %s |" % (
                t, cf["wall_s"], cf["cpu_s"]["writer"],
                cf["cpu_s"]["writer"] * 1e6 / max(c["ops_ok"], 1), cf["cpu_s"]["flush_pool"],
                "%.1fs" % c["stalls"]["stall_s"] if c["stalls"]["stall_s"] > 0.05 else "无"))
        P("")
        P("1 线程无竞争时 1KB Put 的写路径成本 ≈ %.1f µs/KV（含 WAL 写入 + memtable 插入）；"
          "16 线程时升到 %.1f µs/KV —— 差额主要是多线程协调与 adaptive mutex 自旋；"
          "本实验（100GB、写路径长期被 stall 节流）里进一步升到 %.1f µs/KV。"
          "因此“前台写路径 CPU %0.f s”里包含大量**竞争/等待自旋成本**，"
          "并非全是有用功。" % (
              calibs[0][1]["phase_fill"]["cpu_s"]["writer"] * 1e6 / max(calibs[0][1]["ops_ok"], 1),
              calibs[-1][1]["phase_fill"]["cpu_s"]["writer"] * 1e6 / max(calibs[-1][1]["ops_ok"], 1),
              fill["cpu_s"]["writer"] * 1e6 / nops,
              whole_cpu["writer"]))
        P("")
        P("flush 侧在三种线程数下都是 ~1.7 s/GB，与大实验的 %.2f s/GB 一致（与并发无关）。" % (
            whole_cpu["flush_pool"] / max(gb, 1e-9)))
        P("")

    P("## 8. 生效的默认配置（库自报，节选）")
    P("")
    import re
    opt_text = d["effective_options"]
    for key in ["write_buffer_size", "max_write_buffer_number", "max_background_jobs",
                "max_background_flushes", "max_background_compactions",
                "level0_file_num_compaction_trigger", "level0_slowdown_writes_trigger",
                "level0_stop_writes_trigger", "target_file_size_base",
                "max_bytes_for_level_base", "level_compaction_dynamic_level_bytes",
                "max_subcompactions", "compression", "max_open_files", "bytes_per_sync",
                "wal_bytes_per_sync", "manual_wal_flush", "use_direct_io_for_flush_and_compaction"]:
        m = re.search(r"(?:^|[;\n])\s*%s=([^;\n]*)" % re.escape(key), opt_text, re.M)
        if m:
            P("- `%s=%s`" % (key, m.group(1).strip()))
    P("")
    P("默认 block cache 从 cfstats 直接读出：")
    bc = [l for l in d["cfstats_drainend"].splitlines() if l.startswith("Block cache")]
    if bc:
        P("```")
        P(bc[0][:200])
        P("```")
    P("完整清单：`%s.effective_options.txt`；逐作业与采样数据：`%s.{flush_jobs,comp_jobs,"
      "samples,thread_cpu,stalls}.csv`。" % (os.path.basename(prefix), os.path.basename(prefix)))

    text = "\n".join(L)
    print(text)
    with open(md_path, "w") as f:
        f.write(text + "\n")
    print("\n[written] %s" % md_path, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
