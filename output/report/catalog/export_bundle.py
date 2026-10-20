#!/usr/bin/env python3
"""export_bundle.py — export one experiment as a single self-contained file (zexp/1.0).

Design and field contract: output/report/catalog/EXPORT_FORMAT.md

    list     [--family X] [--min-bytes N]      what can be exported, and how big
    manifest <id>                              print the bundle's EXPERIMENT.json only
    export   <id...> | --family X | --all      write <slug>.zexp into --out (default output/bundles)
    verify   <bundle...>                       container + hash + contract checks
    show     <bundle>                          print README.md from inside (no extraction)
    extract  <bundle> <dir>                    unzip helper

Only reads the raw data in place and the metadata catalog; never modifies either.
"""
import argparse
import glob as globmod
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import zipfile
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone

ROOT = "/home/embed/hyl/metadata_offload"
CATALOG = os.path.join(ROOT, "output/report/catalog")            # active (mainline)
ARCHIVE_CATALOG = os.path.join(ROOT, "archive/catalog")   # archived (m-series)
SCOPE_CATALOG = {"active": CATALOG, "archived": ARCHIVE_CATALOG}
SCOPE_OUT = {"active": os.path.join(ROOT, "output/bundles"),
             "archived": os.path.join(ROOT, "archive/bundles")}
DEFAULT_OUT = SCOPE_OUT["active"]
BUNDLE_SCHEMA = "zexp/1.0"
EXPORTER_VERSION = "1.0"

# ---------------------------------------------------------------- payload selectors
# Dataset id -> glob patterns (repo-root relative) that make up its payload.
# Only needed where the dataset is narrower than its artifacts.dirs (the bench/I stage
# holds ~90 runs of ~15 experiments in one directory).  Everything else falls back to
# catalog artifacts.dirs.  Arm names follow bench/CAMPAIGN_BN.md §3/§3b/§3c.
I_DIR = "bench/out/I"
AN = "bench/out/analysis"
P2 = "bench/out/perf_v2"

# perf_v2 runs keep ~25 files each; the per-request detail tables and the 1 Hz sample
# series dominate (6.5 GB for m2 alone), so they are selected and then omitted by the
# bulk policy below with a registered reason - never silently dropped.
P2_RUN_KEEP = ["*.summary.json", "*.measure_phase.json", "*.validity.txt", "*.validity_on_export.txt",
               "*.config.txt", "*.effective.txt", "*.options.txt", "*.per_sec.csv", "*.stalls.csv",
               "*.layout_files.csv", "*.layout_levels.csv", "*.dirscan.csv", "*.read_keys.csv",
               "*.event_files.csv", "*.events.csv", "*.compactions.csv", "*.detail_meta.json",
               "*.detail.csv", "*.detail_get_*.csv", "*.detail_put.csv", "*.detail_scan.csv",
               "*.samples.csv", "*.hist.csv", "*.hist_*.csv", "*.hist_selfcheck.json",
               "*.hist_buckets.csv", "*.run.log", "*.history.log", "*.db.LOG", "*.mon/*"]

P2_COMMON = ["bench/out/perf_v2/RESULTS_SUMMARY.md", "bench/v2/IMPLEMENTATION_REPORT.md",
             "bench/v2/README.md", "bench/L0_BACKLOG_PLAN.md", "bench/LAYOUT_BLOOM_PLAN.md",
             "bench/l0bench.cc", "bench/layoutbench.cc", "bench/backlogbench.cc",
             "bench/tools/l0bench", "bench/tools/l0bench_layout", "bench/tools/l0bench_backlog",
             "bench/build.sh", "bench/build_layout.sh", "bench/build_backlog.sh", "bench/mkreplica.sh"]


def _p2(stage, runs=False, extra=()):
    d = f"{P2}/{stage}/runs" if runs else f"{P2}/{stage}"
    return [f"{d}/{s}" for s in P2_RUN_KEEP] + [f"{P2}/{stage}/{s}" for s in
            ("*.md", "*.json", "*.txt", "manifest.jsonl", "charts/*.png")] + list(extra)


P2_SELECTORS = {
    "perf_v2/P0": ([f"{P2}/p0/{s}" for s in P2_RUN_KEEP]
                   + [f"{P2}/p0/*.md", f"{P2}/p0/*.txt", f"{P2}/p0/*/*"],
                   ["bench/v2/p0_acceptance.sh", "bench/v2/p0_verify.py", "bench/v2/p0_explain.py"]),
    "perf_v2/M1": (_p2("m1"), [f"{P2}/synthesis/m1_offered_fractions.txt"]),
    "perf_v2/M2": (_p2("m2"), [f"{P2}/synthesis/m2_deep_reads.txt", f"{P2}/synthesis/m2_negative_reads.txt",
                               f"{P2}/synthesis/m2_all_families.txt"]),
    "perf_v2/M3": (_p2("m3"), [f"{P2}/synthesis/m3_mixed_load.txt"]),
    "perf_v2/M4": (_p2("m4"), [f"{P2}/synthesis/m4_geometry.txt", f"{P2}/synthesis/m4_deep_vs_recent.txt"]),
    "perf_v2/M5": (_p2("m5"), [f"{P2}/synthesis/m5_get_scale.txt", f"{P2}/synthesis/m5_get_cache.txt",
                               f"{P2}/synthesis/m5_put_scale.txt", f"{P2}/synthesis/m5_scans.txt"]),
    "perf_v2/LAYOUT_BLOOM": (_p2("layout_bloom", runs=True),
                             [f"{P2}/layout_bloom/db/*.done", "bench/v2/layout_bloom.sh",
                              "bench/v2/layout_bloom_analyze.py", "bench/v2/layout_show.py",
                              "bench/v2/layout_load_retry.sh"]),
    "perf_v2/BACKLOG": (_p2("l0_backlog", runs=True),
                        [f"{P2}/l0_backlog/db/*.done", f"{P2}/l0_backlog/logs/*",
                         "bench/v2/l0_backlog.sh", "bench/v2/l0_backlog_analyze.py"]),
    "perf_v2/KSWEEP": ([f"{P2}/l0_backlog_kscan/runs/*", f"{P2}/l0_backlog_kscan/*.md",
                        f"{P2}/l0_backlog_kscan/*.txt", f"{P2}/l0_backlog_kscan/manifest.jsonl",
                        f"{P2}/l0_backlog_kscan/charts/*.png",
                        f"{P2}/l0_backlog_kscan_K*/*.jsonl", f"{P2}/l0_backlog_kscan_K*/*.log"],
                       ["bench/v2/l0_kscan_chain.sh", "bench/v2/l0_kscan_merge.py",
                        "bench/v2/l0_kscan_replicate.py", "bench/v2/finalize_chain.sh",
                        "bench/v2/l0_backlog.sh", "bench/v2/l0_backlog_analyze.py"]),
}
SELECTORS = {
    "bench/I#flush_scan": [f"{I_DIR}/bn_default_r*/*", f"{I_DIR}/bn_x_f2c6_r*/*",
                           f"{I_DIR}/bn_f1_r*/*", f"{I_DIR}/bn_f4_r*/*",
                           f"{AN}/BN_criteria.txt", f"{AN}/BN_arm_table.txt",
                           f"{AN}/BN_paired_default.txt"],
    "bench/I#comp_scan": [f"{I_DIR}/bn_c2_r*/*", f"{I_DIR}/bn_c12_r*/*", f"{I_DIR}/bn_s1_r*/*",
                          f"{I_DIR}/bn_s16_r*/*", f"{I_DIR}/bn_f1c2_r*/*", f"{I_DIR}/bn_f4c2_r*/*",
                          f"{I_DIR}/bn_f1c12_r*/*", f"{I_DIR}/bn_f4c12s16_r*/*",
                          f"{AN}/BN_criteria.txt", f"{AN}/BN_arm_table.txt",
                          f"{AN}/BN_paired_default.txt"],
    "bench/I#noc": [f"{I_DIR}/bn_noc_r*/*", f"{AN}/BN_decisive.txt"],
    "bench/I#geom": [f"{I_DIR}/bn_mc8g_r*/*", f"{I_DIR}/bn_mult20_r*/*",
                     f"{AN}/BN_decisive.txt", f"{AN}/BN_levels.txt"],
    "bench/I#devload": [f"{I_DIR}/bn_devload2_r*/*", f"{AN}/BN_decisive.txt",
                        f"{AN}/BN_device_ceiling.txt"],
    "bench/I#devceil": [f"{AN}/BN_device_ceiling.txt"],
    "bench/I#zipf": [f"{I_DIR}/bn_zipf_r*/*"],
    "bench/I#iso": [f"{I_DIR}/bn_isoflush_*/*", f"{I_DIR}/bn_isol0_*/*",
                    f"{AN}/BN_isoflush.txt", f"{AN}/BN_isol0.txt"],
    "bench/I#profile": [f"{I_DIR}/bn_default_r1/*", f"{AN}/BN1_queue_ordering.txt",
                        f"{AN}/BN_criteria.txt"],
    "bench/I#ledger": [f"{I_DIR}/bn_default_r1/*", f"{I_DIR}/bn_noc_r1/*",
                       f"{AN}/BN_levels.txt", f"{AN}/BN_ledger_mainline.txt"],
    "bench/E10": ["bench/out/E/*/*"],
    **{k: v[0] + P2_COMMON + v[1] for k, v in P2_SELECTORS.items()},
    # datasets whose artifacts sit at the milestone root rather than in their own dir
    "m3/bench_matrix": ["archive/zeroflush_m3_perf/bench_matrix.json",
                        "archive/zeroflush_m3_perf/bench_matrix.py",
                        "archive/zeroflush_m3_perf/bench_matrix_report.md",
                        "archive/zeroflush_m3_perf/bench_matrix_report.py"],
    "m3/bench_matrix_v2": ["archive/zeroflush_m3_perf/bench_matrix_v2.json",
                           "archive/zeroflush_m3_perf/bench_matrix_v2.py"],
    "m3/benchmark_native_zeroflush": ["archive/zeroflush_m3_perf/benchmark_native.json",
                                      "archive/zeroflush_m3_perf/benchmark_zeroflush.json",
                                      "archive/zeroflush_m3_perf/run_benchmark.py"],
    "m3/rocksdb_vs_zeroflush_comparison": ["archive/zeroflush_m3_perf/rocksdb_vs_zeroflush_comparison.md"],
    "bench/CAMPAIGN": ["bench/CAMPAIGN.md", "bench/l0bench.cc", "bench/analyze.py",
                       "bench/logparser.py", "bench/build.sh", "bench/campaign.sh"],
    "bench/CAMPAIGN_BN": ["bench/CAMPAIGN_BN.md", "bench/bn_campaign.sh", "bench/bn_extra.sh",
                          "bench/bn_analyze.py", "bench/tools/devbench.c"],
    "m5/bench100g": ["archive/zeroflush_m5_perf/bench100g_*/*"],
    "m5/zf_50gb_noprobe": ["archive/zeroflush_m5_perf/zf_50gb_noprobe/*"],
    "m5/native_50gb_c59fc41": ["archive/zeroflush_m5_perf/native_50gb_c59fc41/*"],
    "m5/bpcase": ["archive/zeroflush_m5_perf/bpcase/*"],
}

# ---------------------------------------------------------------- data dictionary
# Column meanings for the file families we produced, so a recipient does not have to
# reverse-engineer them.  Keys are fnmatch patterns on the *source* path.
COLUMN_NOTES = {
    "*.samples.csv": {
        "elapsed": "秒，相对本次运行开始；采样间隔 --sample_ms（主战役 500 ms）",
        "ts": "unix 时间戳（秒）",
        "num-immutable-mem-table": "immutable memtable 个数（flush 队列长度）",
        "estimate-pending-compaction-bytes": "待压实字节（全局债务；软限 64 GiB 触发全局限速）",
        "num-files-at-level0..6": "各层文件数",
        "num-running-compactions": "当前运行中的 compaction job 数（LOW 池占用）",
        "num-running-flushes": "当前运行中的 flush job 数（HIGH 池占用）",
        "is-write-stopped": "1=前台被 StopWrite 停住",
        "actual-delayed-write-rate": "非 0 表示写控制器正在限速（单位 B/s）",
        "dev_wr_sectors / dev_rd_sectors": "设备写/读扇区累计数（×512 = 字节），来自 /proc/diskstats",
        "dev_io_ticks_ms": "设备处于在途 IO 的累计毫秒（忙率 = 本列斜率 / 1000）",
        "flush_time_cnt/sum_us/p50_us/p99_us": "FLUSH_TIME 直方图累计值（count/sum/分位，单位 µs）",
        "comp_time_cnt/sum_us/p50_us/p99_us": "COMPACTION_TIME 直方图累计值（单位 µs）",
        "u_ops / u_bytes": "应用确认的写操作数与逻辑字节（U，所有写放大比值的分母）",
        "ops_started / ops_done": "前台已发出/已完成的操作数",
        "rss_bytes / utime_ticks / stime_ticks": "进程 RSS 与用户/系统 CPU tick（100 tick = 1 s）",
        "block_cache_* / bloom_filter_*": "块缓存与布隆计数器（ticker 累计）",
    },
    "*.compactions.csv": {
        "_note": ("槽位语义陷阱：in_bytes_0 / in_bytes_1 是 **compaction 输入槽位**（0=起始层、1=输出层），"
                  "不是 LSM 层级；真实层级用 base_input_level / output_level 两列恢复。"
                  "这是 2026-09-23 勘误的核心（见 bench/BOTTLENECK_REPORT.md §1）。"),
        "ts_begin / ts_end": "job 起止 unix 时间戳（秒）",
        "job": "job id（与 events.csv / db.LOG 对应）",
        "reason": "compaction 触发原因（LevelL0FilesNum / LevelMaxLevelSize / …）",
        "num_input_files / total_input_bytes": "输入文件数与字节（含两侧）",
        "in_files_0..6 / in_bytes_0..6": "按输入槽位计的输入文件数与字节（见 _note）",
        "base_input_level / output_level": "真实起始层与输出层",
        "output_bytes / num_output_files / out_files_0..6": "输出字节、文件数（按输出层）",
        "num_input_records / num_output_records / num_records_replaced": "输入/输出记录数与被淘汰（覆盖）记录数",
        "elapsed_us / cpu_us": "job 挂钟与 CPU 时间（µs）；cpu/wall 低表示 IO 受限",
        "file_write_ns / file_fsync_ns": "文件写入与 fsync 耗时（ns）",
        "lsm_state_after": "job 结束时各层文件数",
    },
    "*.events.csv": {
        "_note": "EventListener 事件流；kind 取值：memtable_sealed / flush_begin / flush_installed / "
                 "compaction_begin / compaction_installed / subcompaction_begin / subcompaction_end / "
                 "file_deleted / set_option。flush 排队时间可用 memtable_sealed → flush_begin 配对求得。",
        "wall_us / cpu_us": "该事件对应 job 的挂钟/CPU 时间（µs）",
        "extra": "原始附加信息（reason、frontend_stop、seqno 等）",
    },
    "*.stalls.csv": {"condition": "normal / delayed / stopped 状态迁移", "condition_code": "0=delayed,2=normal"},
    "*.hist.csv": {"ns": "延迟桶下界（ns）", "engine_count/e2e_count": "累计计数（分位需自行累加，见 analyze.py）"},
    "*.config.txt": {"argv": "本次运行的完整命令行（含全部参数，可直接复现）",
                     "start_wall / pid / host": "起跑时间、进程号、主机"},
    "*.effective.txt": {"_note": "从打开后的 DB 读回的关键生效选项（argv 与实际生效可能不同，以本文件为准）"},
    "manifest.jsonl": {"_note": "每个运行一行 JSON：二进制 md5、git HEAD、host、阶段、配置、返回码 —— 可复现性的最小证据单元"},
    "*.LOG": {"_note": "RocksDB info log（事件文本流）。常用检索：'compacted to'（每次 compaction 的行级明细）、"
                       "'Stalling/Stopping writes because'（写停顿与原因）、'Level summary'（层级规模）"},
    "*.meta": {"_note": "从 .LOG 抽出的派生摘要行（如 Level summary）"},
    "*.stderr": {"_note": "进程 stderr：RocksDB 启动横幅（版本/编译选项/CPU）+ 告警/崩溃输出"},
}

# ---------------------------------------------------------------- helpers


def log(*a):
    print(*a, file=sys.stderr)


def slug(s):
    return re.sub(r"[^A-Za-z0-9._#-]+", "-", s.replace("/", "-")).strip("-")


def sha256(path, chunk=1 << 20):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def read_json(p):
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def load_catalog(scope="active"):
    cat = SCOPE_CATALOG[scope]
    return (read_json(os.path.join(cat, "experiments.json")),
            read_json(os.path.join(cat, "environment.json")),
            read_json(os.path.join(cat, "artifacts.json")),
            read_json(os.path.join(cat, "runs.json")))


def scope_of(eid):
    return "archived" if eid.split("/")[0] in ("m3", "m4", "m5") else "active"


def git(*args):
    try:
        r = subprocess.run(["git", "-C", ROOT] + list(args), capture_output=True, text=True)
        return r.stdout.strip() if r.returncode == 0 else None
    except Exception:
        return None


def fmt_of(name):
    for ext, tag in ((".tar.gz", "tar.gz"), (".log.gz", "log.gz")):
        if name.endswith(ext):
            return tag
    return {".md": "md", ".json": "json", ".jsonl": "jsonl", ".csv": "csv", ".txt": "txt",
            ".log": "log", ".LOG": "LOG", ".py": "py", ".sh": "sh", ".meta": "meta",
            ".stderr": "stderr", ".gz": "gz", ".html": "html"}.get(os.path.splitext(name)[1], "other")


BULK_RX = re.compile(r"(\.LOG$|\.workload$|^LOG\.old\.)")
BULK_MIN = 1 << 20
# perf_v2 bulk classes: per-request detail tables and the 1 Hz sample series are the only
# large files in a perf_v2 run, and the analyses do not read them (samples.csv is the
# time series behind the plots). Each has its own reason string so a reader can tell what
# was left out and how much it weighed.
# Scoped to bench/out/perf_v2/** on purpose: the v1 bundles were delivered with these
# files inside, and re-exporting them under a new policy would silently shrink packages
# that are already in circulation.
BULK2 = [(re.compile(r"\.detail(_[a-z_]+)?\.csv$"), 64 << 10, "bulk-detail"),
         (re.compile(r"\.samples\.csv$"), 64 << 10, "bulk-timeseries"),
         (re.compile(r"(\.mon/|\.run\.log$|history\.log$|\.db\.LOG$|hist_buckets\.csv$)"), 4 << 20,
          "bulk-large")]


def select_files(entry, artifacts):
    """Resolve a dataset's payload: SELECTORS first, else artifacts.dirs."""
    eid = entry["id"]
    if eid in SELECTORS:
        out = []
        for pat in SELECTORS[eid]:
            hits = sorted(globmod.glob(os.path.join(ROOT, pat)))
            if hits and not any(os.path.isdir(h) for h in hits):
                # a single-file pattern (analysis table)
                for h in hits:
                    if os.path.isfile(h):
                        out.append(h)
            elif hits:
                for h in hits:
                    if os.path.isfile(h):
                        out.append(h)
                    elif os.path.isdir(h):
                        for dp, _, fns in os.walk(h):
                            out += [os.path.join(dp, fn) for fn in fns]
        return out
    a = artifacts.get(eid)
    if not a:
        return []
    if a.get("derived_from") == "citations":
        # section-level entry: its payload is exactly the files its citations point at
        # (walking the parent directory would pull the whole milestone tree)
        return [os.path.join(ROOT, f["path"]) for f in a.get("files", [])]
    out = []
    for d in a["dirs"]:
        absd = os.path.join(ROOT, d)
        depth = 1 if d.startswith("bench/out") and d.count("/") == 2 else 2
        for dp, dns, fns in os.walk(absd):
            if dp[len(absd):].count(os.sep) >= depth:
                dns[:] = []
            out += [os.path.join(dp, fn) for fn in fns]
    return out


def plan_payload(files, full=False, max_total=None):
    """Apply the slimming policy; return (included, omitted). Never silent."""
    included, omitted = [], []
    for f in sorted(set(files)):
        rel = os.path.relpath(f, ROOT)
        try:
            b = os.path.getsize(f)
        except OSError:
            continue
        base = os.path.basename(f)
        reason = None
        if not full:
            if b == 0:
                reason = "empty-file"
            elif BULK_RX.search(base) and b >= BULK_MIN:
                reason = "bulk-log"
            elif rel.startswith("bench/out/perf_v2/"):
                for rx, lim, r in BULK2:
                    if b >= lim and (rx.search(base) or rx.search(rel)):
                        reason = r
                        break
        if reason:
            omitted.append({"source": rel, "bytes": b, "reason": reason})
        else:
            included.append({"source": rel, "abs": f, "bytes": b})
    if max_total:
        total = sum(x["bytes"] for x in included)
        for x in sorted(included, key=lambda y: -y["bytes"]):
            if total <= max_total:
                break
            included.remove(x)
            total -= x["bytes"]
            omitted.append({"source": x["source"], "bytes": x["bytes"], "reason": "over-budget"})
    included.sort(key=lambda x: x["source"])
    omitted.sort(key=lambda x: x["source"])
    return included, omitted


def bundle_path_of(source_rel):
    """data/<repo path> for run data, derived/<repo path> for analysis outputs."""
    top = source_rel.split("/")[0]
    kind = "derived" if ("/analysis/" in "/" + source_rel or source_rel.startswith("bench/out/analysis")) else "data"
    return f"{kind}/{source_rel}"


def data_dictionary(included):
    dd = []
    for x in included:
        src = x["source"]
        rec = OrderedDict([("bundle_path", bundle_path_of(src)), ("source", src),
                           ("format", fmt_of(src)), ("bytes", x["bytes"])])
        notes, cols, rows = None, None, None
        if src.endswith(".csv"):
            try:
                with open(x["abs"], encoding="utf-8", errors="replace") as f:
                    head = f.readline().rstrip("\n")
                cols = head.split(",")
                rows = sum(1 for _ in open(x["abs"], encoding="utf-8", errors="replace")) - 1
            except OSError:
                pass
            for pat, m in COLUMN_NOTES.items():
                if pat.endswith(".csv") and globmod.fnmatch.fnmatch(os.path.basename(src), pat):
                    notes = m
                    break
        elif src.endswith(".jsonl"):
            try:
                rows = sum(1 for _ in open(x["abs"], encoding="utf-8", errors="replace"))
            except OSError:
                pass
        elif src.endswith(".json"):
            try:
                d = read_json(x["abs"])
                cols = list(d.keys())[:30] if isinstance(d, dict) else None
            except Exception:
                pass
        for pat, m in COLUMN_NOTES.items():
            if globmod.fnmatch.fnmatch(os.path.basename(src), pat) and pat not in ("*.csv",):
                notes = m
                break
        if cols:
            rec["columns"] = cols
        if rows is not None:
            rec["rows"] = rows
        if notes:
            rec["column_notes"] = notes
        dd.append(rec)
    return dd


def reproduce_block(entry, included, runs):
    """Exact per-run commands from *.config.txt, plus the campaign invocation if derivable."""
    per_run, seen = [], set()
    for x in included:
        src = x["source"]
        if not src.endswith(".config.txt"):
            continue
        run_dir = os.path.dirname(src)
        try:
            first = open(os.path.join(ROOT, src), encoding="utf-8", errors="replace").readline().strip()
        except OSError:
            continue
        argv = first[len("argv:"):].strip() if first.startswith("argv:") else first
        if not argv:
            continue
        per_run.append(OrderedDict([("run_dir", run_dir), ("argv", argv),
                                    ("config_file", src)]))
    # run identity from the bench manifests (binary md5 / git head / rc)
    ident = []
    for stage, blk in (runs.get("bench_stages") or {}).items():
        for r in blk.get("runs", []):
            rid = r.get("run_id", "")
            if any(f"/{rid}/" in "/" + x["source"] for x in included):
                ident.append(OrderedDict([(k, r.get(k)) for k in
                                          ("run_id", "ts", "bin_md5", "git_head", "host", "exp", "arm", "rep", "rc")]))
    campaign = []
    stages = sorted({x["run_dir"].split("/")[2] for x in per_run
                     if x["run_dir"].startswith("bench/out/") and x["run_dir"].count("/") >= 2})
    for st in stages:
        if st != "I":
            campaign.append(f"bash bench/campaign.sh {st}    # stage {st}（按 manifest 的 rc 跳过已完成的运行）")
    if per_run and all(x["run_dir"].startswith("bench/out/I/bn_") for x in per_run):
        arms = sorted({os.path.basename(x["run_dir"]).rsplit("_r", 1)[0] for x in per_run})
        campaign.append("bash bench/bn_campaign.sh scan    # 容量扫描/矩阵臂；已完成的运行按 manifest 的 rc 自动跳过")
        if any(a.startswith("bn_noc") for a in arms):
            campaign.append("bash bench/bn_extra.sh noc      # 消融臂")
        if any(a.startswith("bn_iso") for a in arms):
            campaign.append("bash bench/bn_campaign.sh iso_flush   # 隔离实验")
            campaign.append("bash bench/bn_campaign.sh iso_l0base")
        if any(a.startswith(("bn_mc8g", "bn_mult20")) for a in arms):
            campaign.append("bash bench/bn_extra.sh geom     # 深层几何")
        if any(a.startswith("bn_devload") for a in arms):
            campaign.append("bash bench/bn_extra2.sh devload2")
        if any(a.startswith("bn_zipf") for a in arms):
            campaign.append("bash bench/bn_extra.sh zipf")
    return OrderedDict([
        ("per_run", per_run),
        ("campaign", campaign),
        ("notes", ("per_run.argv 为原始运行命令行（来自各运行的 .config.txt），可直接复现该次运行；"
                   "campaign 是整组编排脚本，重复执行时已记录的运行会被跳过。"
                   + ("本数据集的复现入口另见 environment.other / 引用文档。"
                      if not per_run else ""))),
        ("run_identity", ident),
    ])


def how_to_analyze(entry, included):
    kinds = {fmt_of(x["source"]) for x in included}
    tips = []
    if any(x["source"].endswith("samples.csv") for x in included):
        tips.append("时间序列先读 `data/**/<run>.samples.csv`：第 2 列 `elapsed` 为秒，"
                    "属性列是队列/层级状态，`dev_*` 是设备计数器（扇区×512），"
                    "`flush_time_*`/`comp_time_*` 是服务时间直方图累计值。")
    if any(x["source"].endswith("compactions.csv") for x in included):
        tips.append("逐 job 成本读 `*.compactions.csv`；注意 `in_bytes_0/1` 是按**输入槽位**而非层级计，"
                    "层级要用 `base_input_level`/`output_level` 还原（见该文件的 column_notes）。")
    if any(x["source"].endswith("events.csv") for x in included):
        tips.append("事件级时间线读 `*.events.csv`（flush 排队 = memtable_sealed → flush_begin 的间隔）。")
    if any(x["source"].endswith("manifest.jsonl") for x in included):
        tips.append("可复现性证据读 `manifest.jsonl`：每行的 bin_md5/git_head 决定这份数据出自哪个二进制与提交。")
    if any(x["source"].endswith(".LOG") for x in included):
        tips.append("RocksDB info log 用文本检索：`grep 'compacted to'`、`grep 'Stalling writes because'`。")
    if kinds & {"md"}:
        tips.append("报告类文件（REPORT/RESULTS/README）是结论与口径的来源；本包的 results/evaluation "
                    "即为其中的要点摘录，两者应一致。")
    tips.append("分析前先看 `environment_baseline.measurement_caveats`：本项目的运行间方差很大"
                "（同配置可达 1.9×），跨时间窗的绝对值不可直接比较。")
    if entry.get("caveats_zh"):
        tips.append("该数据集自述的限制见 `caveats_zh`；若有作废/更正的臂，不要引用其数字。")
    return tips


# ---------------------------------------------------------------- manifest / export


def build_manifest(eid, experiments, env, artifacts, runs, full=False, max_total=None):
    entry = next((e for e in experiments if e["id"] == eid), None)
    if not entry:
        raise SystemExit(f"unknown dataset id: {eid}")
    files = select_files(entry, artifacts)
    included, omitted = plan_payload(files, full=full, max_total=max_total)
    for x in included:
        x["sha256"] = sha256(x["abs"])
    for x in omitted:
        p = os.path.join(ROOT, x["source"])
        x["sha256"] = sha256(p) if os.path.isfile(p) and x["bytes"] <= (64 << 20) else None
    payload = [OrderedDict([("bundle_path", bundle_path_of(x["source"])), ("source", x["source"]),
                            ("bytes", x["bytes"]), ("sha256", x["sha256"]),
                            ("format", fmt_of(x["source"]))]) for x in included]
    total = sum(x["bytes"] for x in included)
    m = OrderedDict()
    m["bundle_schema"] = BUNDLE_SCHEMA
    m["exported_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    m["exporter"] = f"output/report/catalog/export_bundle.py v{EXPORTER_VERSION}"
    m["id"] = entry["id"]
    m["title_zh"] = entry["title_zh"]
    m["generation"] = entry.get("generation", "v1")
    m["engine"] = entry["engine"]
    m["category"] = entry["category"]
    m["purpose_zh"] = entry["purpose_zh"]
    m["environment"] = entry["environment"]
    m["environment_baseline"] = {k: v for k, v in env.items() if k != "schema_note"}
    m["environment_baseline_note"] = env.get("schema_note")
    m["results"] = entry["results"]
    m["evaluation_zh"] = entry.get("evaluation_zh")
    m["caveats_zh"] = entry.get("caveats_zh") or []
    m["citations"] = entry.get("sources") or []
    _cat = SCOPE_CATALOG[scope_of(eid)]
    m["annotation_gaps"] = [g for g in (read_json(os.path.join(_cat, "index.json")).get("annotation_gaps") or [])
                            if g["id"] == eid]
    m["scope"] = scope_of(eid)
    m["reproduce"] = reproduce_block(entry, included, runs)
    m["provenance"] = OrderedDict([
        ("git_head", git("rev-parse", "HEAD")),
        ("git_branch", git("rev-parse", "--abbrev-ref", "HEAD")),
        ("git_dirty", bool(git("status", "--porcelain"))),
        ("host", os.uname().nodename),
        ("exported_at", m["exported_at"]),
        ("catalog_schema", read_json(os.path.join(CATALOG, "index.json"))["schema_version"]),
        ("note", "citations 指向的仓库路径未随包发布；它们是事实出处标注，不是运行依赖。"),
    ])
    m["data_dictionary"] = data_dictionary(included)
    m["payload"] = payload
    m["omitted"] = omitted
    mode = ("metadata-only" if not included else
            "full" if not omitted else "partial")
    m["completeness"] = OrderedDict([
        ("mode", mode),
        ("has_payload", bool(included)),
        # `complete` means: this dataset's own payload is fully inside the bundle.
        # A metadata-only bundle is NOT complete in that sense (it has no data at all).
        ("complete", mode == "full"),
        ("included_files", len(included)),
        ("omitted_files", len(omitted)),
        ("included_bytes", total),
        ("omitted_bytes", sum(x["bytes"] for x in omitted)),
        ("criterion", ("默认排除 >1 MiB 的 RocksDB info log（*.LOG / LOG.old.* / *.workload）与空文件"
                       + ("；perf_v2 数据集另排除 >64 KiB 的 *.detail*.csv / *.samples.csv 与 >4 MiB 的"
                          "监控日志（仅限 bench/out/perf_v2/**，v1 包保持原样）"
                          if any(x["source"].startswith("bench/out/perf_v2/") for x in included + omitted)
                          else "")
                       + (f"，并把总量压到 {max_total} 字节内" if max_total else ""))
                      + ("；本次为 --full，未做任何省略。" if full else "。")),
    ])
    m["how_to_analyze"] = how_to_analyze(entry, included)
    return m, included


def render_readme(m):
    L = []
    A = L.append
    A(f"# 实验导出包：{m['title_zh']}")
    A("")
    gen = m.get("generation", "v1")
    gtxt = ("**v2（新版本）**：perf_v2/runbook 战役，2026-09-25 起，直方图编码 lower_bound_v2，"
            "逐运行 §6 有效性门" if gen == "v2" else
            "v1：早期战役（L0 调查 / 瓶颈判定 / ZeroFlush 里程碑），其延迟百分位为 v1 编码的**占位值**，"
            "引用前须按桶下界重标（见 perf_v2/P0 与 bench/out/hist_erratum/）")
    A(f"- **id**：`{m['id']}`　**引擎**：{', '.join(m['engine'])}　**类别**：{m['category']}")
    A(f"- **版本世代**：{gtxt}")
    A(f"- **导出时间**：{m['exported_at']}　**导出器**：{m['exporter']}")
    A(f"- **数据完整性**：包含 {m['completeness']['included_files']} 个文件 "
      f"（{m['completeness']['included_bytes']/2**20:.1f} MiB），"
      f"省略 {m['completeness']['omitted_files']} 个（{m['completeness']['omitted_bytes']/2**20:.1f} MiB）")
    if m["completeness"]["mode"] == "metadata-only":
        A("")
        A("> ⚠ **本包只含元数据，不含实验数据**：该实验的产物不在本仓库的常规数据目录内"
          "（或未在本机保留）。元数据（目的/环境/结果/评估/出处）齐备，"
          "数据位置见下方「数据在哪里」。")
    elif not m["completeness"]["complete"]:
        A("")
        A("> ⚠ **本包不是全部原始数据**：以下文件未打入，原因见 `EXPERIMENT.json.omitted`"
          "（含 sha256 以便核对）。判据：" + m["completeness"]["criterion"])
    A("")
    A("## 1. 实验目的")
    A("")
    A(m["purpose_zh"] or "_(来源未陈述)_")
    A("")
    A("## 2. 实验环境")
    A("")
    A("| 项 | 值 |")
    A("|---|---|")
    for k, v in (m["environment"] or {}).items():
        if v:
            A(f"| {k} | {str(v)[:400]} |")
    A("")
    A("共享基线（主机/设备/引擎/测量注意）见 `EXPERIMENT.json.environment_baseline`，要点：")
    hb = (m["environment_baseline"].get("host") or {})
    A(f"- 主机：{hb.get('cpu')}，{hb.get('cores_threads')}，{hb.get('ram')}；设备：{hb.get('device')}；"
      f"文件系统：{hb.get('filesystem')}")
    for c in (m["environment_baseline"].get("measurement_caveats") or [])[:3]:
        A(f"- 注意：{c}")
    A("")
    A("## 3. 结果")
    A("")
    if m["results"]:
        A("| 指标 | 值 | 单位 | 条件 | 出处 |")
        A("|---|---|---|---|---|")
        for r in m["results"]:
            A(f"| {r.get('metric')} | {r.get('value')} | {r.get('unit') or ''} | "
              f"{r.get('condition') or ''} | `{r.get('source')}` |")
    else:
        A("_(来源未给出结构化结果)_")
    A("")
    A("## 4. 结果评估（报告自身裁决）")
    A("")
    A(m.get("evaluation_zh") or "_(来源未陈述)_")
    A("")
    if m["caveats_zh"]:
        A("### 限制与作废")
        A("")
        for c in m["caveats_zh"]:
            A(f"- {c}")
        A("")
    A("## 5. 怎么用这些数据")
    A("")
    for t in m["how_to_analyze"]:
        A(f"- {t}")
    A("")
    A("## 6. 复现")
    A("")
    pr = m["reproduce"]["per_run"]
    if pr:
        groups = OrderedDict()
        for r in pr:
            # normalise so that commands differing only by repo root / run name collapse
            # (the exact argv is preserved in EXPERIMENT.json.reproduce.per_run)
            run = os.path.basename(r["run_dir"])
            norm = r["argv"].replace(ROOT, "<REPO>").replace(run, "<RUN>")
            groups.setdefault(norm, []).append(run)
        A(f"原始运行命令行（来自各运行的 `.config.txt`；{len(pr)} 次运行，"
          f"下按「仓库根 → <REPO>、运行名 → <RUN>」归一化后合并为 {len(groups)} 组；"
          "逐运行的原始 argv 见 `EXPERIMENT.json.reproduce.per_run`）：")
        A("")
        for argv, runs in list(groups.items())[:6]:
            label = f"{len(runs)} 次重复" if len(runs) > 1 else "1 次"
            A(f"- {label}：`{', '.join(runs[:8])}`" + ("…" if len(runs) > 8 else ""))
            A("  ```")
            A(f"  {argv}")
            A("  ```")
        if len(groups) > 6:
            A(f"- …另有 {len(groups)-6} 组，完整逐运行列表见 `EXPERIMENT.json.reproduce.per_run`")
    else:
        A("本数据集未附逐运行命令行；复现入口见 `EXPERIMENT.json.reproduce.notes` 与引用文档。")
    if m["reproduce"]["campaign"]:
        A("")
        A("整组编排：")
        A("")
        for c in m["reproduce"]["campaign"]:
            A(f"- `{c}`")
    A("")
    A(f"> {m['reproduce']['notes']}")
    A("")
    if m["completeness"]["mode"] == "metadata-only":
        A("## 7. 数据在哪里")
        A("")
        A("本包不含数据文件。该实验的数据在我方仓库的以下位置（可据此向你索取或按 §6 复现）：")
        A("")
        for c in m["citations"][:6]:
            A(f"- `{c}`")
        A("")
    A("## 7. 包里有什么")
    A("")
    A("```")
    A("EXPERIMENT.json      机器可读主清单（本 README 的全部内容 + 数据字典 + 文件清单 + 校验）")
    A("README.md            本文件")
    A("checksums.sha256     载荷校验（sha256sum -c checksums.sha256）")
    A("data/…               原始实验数据，路径镜像仓库布局")
    A("derived/…            分析产物（分析表等）")
    A("```")
    A("")
    if m["provenance"].get("git_head"):
        A(f"出处：仓库 `{m['provenance']['git_head'][:12]}`"
          f"（分支 {m['provenance'].get('git_branch')}，"
          f"{'有未提交改动' if m['provenance'].get('git_dirty') else '工作区干净'}）")
        A("")
    A("## 8. 数据字典（列含义）")
    A("")
    for d in m["data_dictionary"][:14]:
        A(f"- `{d['bundle_path']}` — {d['format']}, {d['bytes']} B"
          + (f", {d['rows']} 行" if d.get("rows") is not None else "")
          + (f", {len(d['columns'])} 列" if d.get("columns") else ""))
        if d.get("column_notes"):
            n = d["column_notes"]
            if "_note" in n:
                A(f"  - 说明：{n['_note']}")
            kv = ", ".join(f"`{k}`={v}" for k, v in list(n.items())[:6] if k != "_note")
            if kv:
                A(f"  - 列义：{kv}")
    if len(m["data_dictionary"]) > 14:
        A(f"- …另有 {len(m['data_dictionary'])-14} 个文件，完整字典见 `EXPERIMENT.json.data_dictionary`")
    A("")
    A("## 9. 事实出处")
    A("")
    A(f"本包的 目的/环境/结果/评估 共引用了 {len(m['citations'])} 处原始出处"
      "（`路径:行号`，指向我方仓库；未随包发布，仅供回溯）：")
    A("")
    for c in m["citations"][:10]:
        A(f"- `{c}`")
    if len(m["citations"]) > 10:
        A(f"- …另有 {len(m['citations'])-10} 处，完整列表见 `EXPERIMENT.json.citations`")
    if m["annotation_gaps"]:
        A("")
        A("来源未陈述（未补写）："
          + "；".join(f"{g['id']} 缺 {','.join(g['missing'])}" for g in m["annotation_gaps"]))
    A("")
    return "\n".join(L)


def write_bundle(m, included, outdir):
    os.makedirs(outdir, exist_ok=True)
    fname = slug(m["id"].replace("#", "-"))
    if m["completeness"]["mode"] == "metadata-only":
        fname += ".metaonly"
    path = os.path.join(outdir, f"{fname}.zexp")
    readme = render_readme(m)
    manifest_txt = json.dumps(m, ensure_ascii=False, indent=1) + "\n"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        z.writestr("EXPERIMENT.json", manifest_txt)
        z.writestr("README.md", readme)
        for x in included:
            z.write(x["abs"], bundle_path_of(x["source"]))
        sums = "".join(f"{x['sha256']}  {bundle_path_of(x['source'])}\n" for x in included)
        z.writestr("checksums.sha256", sums)
    return path


def update_index(outdir, records):
    p = os.path.join(outdir, "index.json")
    idx = read_json(p) if os.path.exists(p) else {"schema": BUNDLE_SCHEMA, "bundles": []}
    by_id = {b["id"]: b for b in idx["bundles"]}
    for r in records:
        by_id[r["id"]] = r
    idx["bundles"] = sorted(by_id.values(), key=lambda b: b["id"])
    idx["count"] = len(idx["bundles"])
    idx["total_bytes"] = sum(b["bytes"] for b in idx["bundles"])
    idx["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    idx["note"] = ("每个 .zexp 都是自包含的单文件实验包（数据 + 环境 + 目的 + 结果 + 评估 + 复现 + 数据字典）；"
                   "未打入的文件在包内 EXPERIMENT.json.omitted 里逐条登记。")
    with open(p, "w", encoding="utf-8") as f:
        json.dump(idx, f, ensure_ascii=False, indent=1)
        f.write("\n")


# ---------------------------------------------------------------- commands


def cmd_list(args, cat):
    experiments, env, artifacts, runs = cat
    label = cat[1].get("scope_note", "")
    rows = []
    for e in experiments:
        if args.family and not e["id"].startswith(args.family):
            continue
        files = select_files(e, artifacts)
        inc, om = plan_payload(files, full=args.full, max_total=args.max_total_bytes)
        b = sum(x["bytes"] for x in inc)
        if args.min_bytes and b < args.min_bytes:
            continue
        rows.append((e["id"], len(inc), len(om), b))
    rows.sort(key=lambda r: -r[3])
    print(f"[scope={cat[1].get('scope', '?')}] {label[:60]}")
    print(f"{'dataset id':<28} {'files':>6} {'omitted':>7} {'MiB':>9}")
    for i, n, o, b in rows:
        print(f"{i:<28} {n:>6} {o:>7} {b/2**20:>9.1f}")
    print(f"\n{len(rows)} datasets; total {sum(r[3] for r in rows)/2**20:.0f} MiB")
    return 0


def cmd_manifest(args, cat):
    experiments, env, artifacts, runs = cat
    m, _ = build_manifest(args.id, experiments, env, artifacts, runs,
                          full=args.full, max_total=args.max_total_bytes)
    print(json.dumps(m, ensure_ascii=False, indent=1))
    return 0


def cmd_export(args, cats):
    ids = list(args.ids)
    if args.all or args.family:
        for sc, (experiments, _, _, _) in cats.items():
            if args.all:
                ids += [e["id"] for e in experiments]
            if args.family:
                ids += [e["id"] for e in experiments if e["id"].startswith(args.family)]
    ids = [i for i in OrderedDict.fromkeys(ids)]
    if not ids:
        log("nothing to export: pass ids, --family or --all")
        return 2
    records = defaultdict(list)
    for i in ids:
        sc = scope_of(i)
        if sc not in cats:
            log(f"skip {i}: scope {sc} not selected (use --scope {sc} or --scope all)")
            continue
        experiments, env, artifacts, runs = cats[sc]
        try:
            m, included = build_manifest(i, experiments, env, artifacts, runs,
                                         full=args.full, max_total=args.max_total_bytes)
        except SystemExit as e:
            log(f"skip {i}: {e}")
            continue
        if not included and args.skip_empty:
            log(f"skip {i}: no local payload (metadata-only export suppressed by --skip-empty)")
            continue
        outdir = args.out or SCOPE_OUT[sc]
        p = write_bundle(m, included, outdir)
        rec = {"id": i, "scope": sc, "bundle": os.path.relpath(p, ROOT), "bytes": os.path.getsize(p),
               "sha256": sha256(p), "payload_files": m["completeness"]["included_files"],
               "omitted_files": m["completeness"]["omitted_files"],
               "payload_bytes": m["completeness"]["included_bytes"],
               "mode": m["completeness"]["mode"],
               "complete": m["completeness"]["complete"], "exported_at": m["exported_at"]}
        records[outdir].append(rec)
        print(f"{i:<28} -> {rec['bundle']:<52} {rec['bytes']/2**20:>7.2f} MiB "
              f"files={rec['payload_files']} omitted={rec['omitted_files']}")
    for outdir, recs in records.items():
        update_index(outdir, recs)
        print(f"\n{len(recs)} bundles in {os.path.relpath(outdir, ROOT)}; index.json updated")
    return 0


def cmd_verify(args, cat):
    ok = True
    for b in args.bundles:
        try:
            with zipfile.ZipFile(b) as z:
                bad = z.testzip()
                names = set(z.namelist())
                if bad:
                    print(f"FAIL {b}: corrupt entry {bad}")
                    ok = False
                    continue
                for req in ("EXPERIMENT.json", "README.md", "checksums.sha256"):
                    if req not in names:
                        print(f"FAIL {b}: missing {req}")
                        ok = False
                m = json.loads(z.read("EXPERIMENT.json").decode("utf-8"))
                for k in ("bundle_schema", "id", "purpose_zh", "environment", "results",
                          "evaluation_zh", "caveats_zh", "citations", "payload", "omitted",
                          "completeness", "data_dictionary", "provenance", "reproduce"):
                    if k not in m:
                        print(f"FAIL {b}: EXPERIMENT.json missing key {k}")
                        ok = False
                # hashes
                nhash = 0
                for x in m.get("payload", []):
                    if x["bundle_path"] not in names:
                        print(f"FAIL {b}: payload listed but absent: {x['bundle_path']}")
                        ok = False
                        continue
                    h = hashlib.sha256(z.read(x["bundle_path"])).hexdigest()
                    nhash += 1
                    if h != x["sha256"]:
                        print(f"FAIL {b}: hash mismatch {x['bundle_path']}")
                        ok = False
                # checksums file self-consistency
                cs = z.read("checksums.sha256").decode("utf-8")
                n_cs = sum(1 for ln in cs.splitlines() if ln.strip())
                # data dictionary covers payload
                dd = {d["bundle_path"] for d in m.get("data_dictionary", [])}
                missing_dd = [x["bundle_path"] for x in m.get("payload", [])
                              if x["bundle_path"] not in dd]
                if missing_dd:
                    print(f"WARN {b}: {len(missing_dd)} payload files lack a data-dictionary entry")
                comp = m["completeness"]
                print(f"OK   {b}: schema ok, mode={comp.get('mode')}, {nhash} hashes ok, "
                      f"checksums={n_cs}, payload={comp['included_files']} files/"
                      f"{comp['included_bytes']/2**20:.2f} MiB, omitted={comp['omitted_files']}, "
                      f"complete={comp['complete']}")
        except zipfile.BadZipFile:
            print(f"FAIL {b}: not a zip container")
            ok = False
    return 0 if ok else 1


def cmd_show(args, cat):
    with zipfile.ZipFile(args.bundle) as z:
        print(z.read("README.md").decode("utf-8"))
    return 0


def cmd_extract(args, cat):
    os.makedirs(args.dir, exist_ok=True)
    with zipfile.ZipFile(args.bundle) as z:
        z.extractall(args.dir)
    print(f"extracted {args.bundle} -> {args.dir}")
    return 0


def main():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--full", action="store_true", help="include bulk logs, no size cap")
    common.add_argument("--scope", choices=["active", "archived", "all"], default="active",
                        help="active=主线（bench，默认）；archived=m 系列存档（m3/m4/m5）；all=两者")
    common.add_argument("--max-total-bytes", type=int, default=64 << 20,
                        help="payload budget; largest files are dropped (and recorded) beyond it")
    common.add_argument("--skip-empty", action="store_true",
                        help="skip datasets with no local payload (default: export them as <slug>.metaonly.zexp)")
    ap = argparse.ArgumentParser(description="export one experiment as a single self-contained file",
                                 parents=[common])
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("list", parents=[common])
    p.add_argument("--family"); p.add_argument("--min-bytes", type=int, default=0)
    p = sub.add_parser("manifest", parents=[common]); p.add_argument("id")
    p = sub.add_parser("export", parents=[common])
    p.add_argument("ids", nargs="*")
    p.add_argument("--family"); p.add_argument("--all", action="store_true")
    p.add_argument("--out", default=None,
                   help="override output dir (default: scope's own dir)")
    p = sub.add_parser("verify", parents=[common]); p.add_argument("bundles", nargs="+")
    p = sub.add_parser("show", parents=[common]); p.add_argument("bundle")
    p = sub.add_parser("extract", parents=[common]); p.add_argument("bundle"); p.add_argument("dir")

    args = ap.parse_args()
    scopes = ["active", "archived"] if args.scope == "all" else [args.scope]
    cats = {s: load_catalog(s) for s in scopes}
    if args.cmd == "list":
        for s in scopes:
            cmd_list(args, cats[s])
            print()
        return 0
    if args.cmd == "manifest":
        return cmd_manifest(args, cats[scopes[0]])
    if args.cmd == "export":
        return cmd_export(args, cats)
    return {"verify": cmd_verify, "show": cmd_show, "extract": cmd_extract}[args.cmd](args, cats)


if __name__ == "__main__":
    sys.exit(main())
