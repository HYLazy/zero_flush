#!/usr/bin/env python3
"""build_catalog.py — assemble and validate the experiment-data JSON catalog.

Layers
------
annotated   : per-dataset entries (environment / purpose / results / evaluation / caveats),
              mined from the archived reports by 6 extraction passes, each claim carrying a
              "path:LINE" citation.  Source files: catalog/_src/*.json
machine     : everything derivable exactly from the working tree, generated here:
                environment.json  shared machine / RocksDB / driver reference
                runs.json         per-run index (bench/out/*/manifest.jsonl + report JSON runs[])
                artifacts.json    per-dataset file inventory (formats, sizes, tracked flag)
                documents.json    index of every report/protocol/design document
                index.json        master index + validation report
Validation
----------
* schema: required keys present, types correct, ids unique
* citations: every "source"/"sources" entry must resolve to an existing file and a
  line number inside it, and that line must be non-blank (catches off-by-one citations)
* coverage: per-entry and overall citation counts, plus annotation completeness

Run:  python3 output/report/catalog/build_catalog.py
"""
import json
import os
import re
import subprocess
import sys
from collections import Counter, OrderedDict

ROOT = "/home/embed/hyl/metadata_offload"
CAT = os.path.join(ROOT, "output/report/catalog")            # active scope (mainline)
ARCHIVE_CAT = os.path.join(ROOT, "archive/catalog")   # archived scope (m-series)
SRC = os.path.join(CAT, "_src")

# Two scopes.  The m-series milestones (m3/m4/m5 = the ZeroFlush V0 line) were archived
# on 2026-09-25: their data now lives under archive/, they are kept out of the
# mainline catalog, and export_bundle.py skips them unless --scope archived is asked for.
SCOPES = {
    "active":   {"entry_prefixes": ("bench/", "perf_v2/"), "path_prefix": "",
                 "catalog": CAT},
    "archived": {"entry_prefixes": ("m3/", "m4/", "m5/"), "path_prefix": "archive/",
                 "catalog": ARCHIVE_CAT},
}

SCHEMA_VERSION = "1.0"
REQUIRED_KEYS = ["id", "title_zh", "engine", "category", "purpose_zh",
                 "environment", "results", "evaluation_zh", "caveats_zh", "sources"]
VALID_CATEGORIES = {"benchmark", "diagnosis", "mechanism", "capacity_sweep",
                    "integration", "isolation", "protocol"}
ENV_KEYS = ["host", "cpu", "cores_threads", "ram", "device", "filesystem",
            "engine_version", "binary_path", "workload", "rocksdb_options", "other"]

# ---------------------------------------------------------------- helpers


def rel(p):
    return os.path.relpath(p, ROOT)


def read_json(p):
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def tracked(path):
    """True if the path is tracked by git (uses one batched query for speed)."""
    try:
        r = subprocess.run(["git", "-C", ROOT, "ls-files", "--error-unmatch", path],
                           capture_output=True)
        return r.returncode == 0
    except Exception:
        return None


def tracked_set(paths):
    """Batch-resolve tracked status for many paths via a single git ls-files call."""
    out = set()
    try:
        r = subprocess.run(["git", "-C", ROOT, "ls-files", "-z", "--"] + list(paths),
                           capture_output=True)
        if r.returncode == 0:
            for p in r.stdout.decode("utf-8", "replace").split("\0"):
                if p:
                    out.add(p)
    except Exception:
        pass
    return out


def dir_size(path):
    n = b = 0
    for dp, _, fns in os.walk(path):
        for fn in fns:
            fp = os.path.join(dp, fn)
            try:
                b += os.path.getsize(fp)
                n += 1
            except OSError:
                pass
    return n, b


FMT_OF = {".md": "md", ".json": "json", ".jsonl": "jsonl", ".csv": "csv", ".txt": "txt",
          ".log": "log", ".LOG": "LOG", ".py": "py", ".sh": "sh", ".meta": "meta",
          ".stderr": "stderr", ".gz": "gz", ".tar": "tar", ".sst": "sst", ".patch": "patch",
          ".html": "html", ".cc": "cc", ".h": "h", ".c": "c", ".pid": "pid",
          ".workload": "LOG", ".fragment": "fragment"}


def fmt_of(name):
    for ext, tag in ((".tar.gz", "tar.gz"), (".log.gz", "log.gz")):
        if name.endswith(ext):
            return tag
    return FMT_OF.get(os.path.splitext(name)[1], "other")


# ---------------------------------------------------------------- annotated layer


def load_annotated():
    entries, problems = [], []
    for fn in sorted(os.listdir(SRC)):
        if not fn.endswith(".json"):
            continue
        group = fn[:-5]
        data = read_json(os.path.join(SRC, fn))
        if not isinstance(data, list):
            problems.append(f"{fn}: not a JSON array")
            continue
        for e in data:
            e["_group"] = group
            entries.append(e)
    seen = {}
    for e in entries:
        eid = e.get("id", "<missing>")
        if eid in seen:
            problems.append(f"duplicate id: {eid} ({seen[eid]} and {e['_group']})")
        seen[eid] = e["_group"]
    return entries, problems


def validate(entries):
    """Schema + citation validation. Returns (problems, stats)."""
    problems, stale = [], []
    stats = dict(entries=0, results=0, sources=0, env_filled=0,
                 with_evaluation=0, with_caveats=0, citations_checked=0,
                 citations_bad_file=0, citations_bad_line=0, citations_blank_line=0)
    line_cache = {}
    for e in entries:
        stats["entries"] += 1
        for k in REQUIRED_KEYS:
            if k not in e:
                problems.append(f"{e.get('id')}: missing key {k}")
        if e.get("category") not in VALID_CATEGORIES:
            problems.append(f"{e.get('id')}: bad category {e.get('category')!r}")
        if not isinstance(e.get("engine"), list) or not e.get("engine"):
            problems.append(f"{e.get('id')}: engine must be a non-empty list")
        env = e.get("environment") or {}
        if isinstance(env, dict):
            filled = sum(1 for k in ENV_KEYS if env.get(k))
            stats["env_filled"] += 1 if filled else 0
            for k in env:
                if k not in ENV_KEYS:
                    problems.append(f"{e.get('id')}: unknown environment key {k}")
        else:
            problems.append(f"{e.get('id')}: environment must be an object")
        if not (e.get("purpose_zh") or "").strip():
            problems.append(f"{e.get('id')}: empty purpose_zh")
        if (e.get("evaluation_zh") or "").strip():
            stats["with_evaluation"] += 1
        if e.get("caveats_zh"):
            stats["with_caveats"] += 1

        cites = list(e.get("sources") or [])
        for r in e.get("results") or []:
            stats["results"] += 1
            if isinstance(r, dict) and r.get("source"):
                cites.append(r["source"])
            else:
                problems.append(f"{e.get('id')}: result without source: {str(r)[:60]}")
        for c in cites:
            stats["sources"] += 1
            stats["citations_checked"] += 1
            # a citation is "path:" followed by one or more line specs separated by
            # commas, each "N" or "A-B" (also en/em dash).  e.g. "f.md:12,20-24"
            m = re.match(r"^(?P<file>.*?):(?P<specs>\d+(?:[-–,]\d+)*)$", str(c))
            f = str(c)
            specs = []
            if m:
                f = m.group("file")
                for part in m.group("specs").split(","):
                    if "-" in part or "–" in part:
                        a, b = re.split(r"[-–]", part, 1)
                        specs.append((int(a), int(b)))
                    else:
                        specs.append((int(part), int(part)))
            fp = os.path.join(ROOT, f)
            if not os.path.exists(fp):
                stats["citations_bad_file"] += 1
                stale.append((e.get("id"), c))
                continue
            if not specs:
                continue
            if fp not in line_cache:
                with open(fp, encoding="utf-8", errors="replace") as fh:
                    line_cache[fp] = fh.read().split("\n")
            lines = line_cache[fp]
            bad = None
            for a, b in specs:
                if a < 1 or b > len(lines):
                    bad = f"line out of range ({a}-{b} of {len(lines)})"
                    break
                if not any(lines[i - 1].strip() for i in range(a, b + 1)):
                    bad = f"blank line ({a}-{b})"
                    break
            if bad:
                stats["citations_bad_line" if "range" in bad else "citations_blank_line"] += 1
                stale.append((e.get("id"), f"{c} ({bad})"))
    return problems, stale, stats


# ---------------------------------------------------------------- machine: environment


def build_environment(entries):
    """Shared reference environment, machine-extracted from protocols + benchmark JSONs."""
    def first_env(group_pred, key):
        for e in entries:
            if group_pred(e):
                v = (e.get("environment") or {}).get(key)
                if v:
                    return v
        return None
    bench = lambda e: e["id"].startswith("bench/")
    m3 = lambda e: e["id"].startswith("m3/")
    env = OrderedDict()
    env["schema_note"] = ("共享实验环境参考；各数据集的 environment 字段只在与本文不同处重复或补充。"
                          "字段来源：bench/CAMPAIGN.md（原生战役）与 archive/zeroflush_m3_perf/*.json 的 "
                          "hardware/config 块（ZeroFlush 里程碑测量）。")
    env["host"] = {
        "cpu": first_env(bench, "cpu") or "Intel(R) Core(TM) i9-10980XE CPU @ 3.00GHz",
        "cores_threads": first_env(bench, "cores_threads") or "18 cores / 36 threads",
        "ram": first_env(bench, "ram") or "62 GiB",
        "device": first_env(bench, "device") or "WD_BLACK SN750 2TB NVMe (/dev/nvme0n1p3)",
        "filesystem": first_env(bench, "filesystem") or "ext4, rw,relatime,commit=60",
        "kernel": "6.8.0-124-generic",
        "note": ("单机单设备单列族单进程；设备为消费级 NVMe（SMART：Percentage Used 13%、"
                 "已写 221TB、通电 30705h，见 m5/50gb_1kb_merge_off#ENV_DIAGNOSIS）；"
                 "盘温在重度负载下长期 79–86°C 并被判定为最可疑的剩余约束。"),
    }
    # engines actually built in this tree
    env["engines"] = {
        "native_rocksdb": {
            "tree": "source/rocksdb", "version": "11.2.0",
            "build_flags": "-O3 -DNDEBUG -march=native -fno-rtti -std=gnu++20",
            "artifacts": ["source/rocksdb/build/librocksdb.so.11", "source/rocksdb/build/db_bench"],
            "used_by": ["bench/*（原生战役全部运行）"],
            "note": "本机预编译库 WITH_LZ4/SNAPPY/ZLIB/ZSTD=OFF，故全部 byte-level 实验为 compression=none。",
        },
        "zeroflush": {
            "tree": "source/rocksdb-zeroflush",
            "worktrees": ["source/rocksdb-zeroflush（主）", "/home/embed/hyl/wt_b4b17b3（b4b17b3 独立 worktree）"],
            "build": "build_rel（cmake，RelWithDebInfo，PORTABLE=0，make -j20）",
            "artifacts": ["build_rel/db_bench", "build_rel/zf_valcheck", "tools/zf_govern_bench"],
            "used_by": ["m3/m4/m5 的 ZeroFlush 臂"],
        },
        "driver": {
            "l0bench": {"src": "bench/l0bench.cc", "built": "bench/tools/l0bench",
                        "role": "原生战役的事件/指标采集驱动（无源码补丁）"},
            "devbench": {"src": "bench/tools/devbench.c", "built": "bench/tools/devbench",
                         "role": "O_DIRECT 随机 IO 设备上限探针与争用负载"},
            "logparser": {"src": "bench/logparser.py", "role": "独立解析 DB LOG 事件流的交叉校验路径"},
            "analyzers": ["bench/analyze.py", "bench/bn_analyze.py"],
        },
    }
    env["instrumentation"] = {
        "zeroflush_flags": ["ZF_DBG_*（探针门控）", "ZF_INSTALL_LOCK", "ZF_L0_TRIGGER",
                            "ZF_MAX_L1_BYTES", "ZF_L1_BUDGET_MB", "ZF_SINK_*", "ZF_TRACE_IMM",
                            "ZF_FORCE_EMPTY_EDIT", "ZF_BENCH_VERBOSE"],
        "native_flags": ["--statistics", "--histogram", "stats_dump_period_sec=60",
                         "EventListener（flush/compaction/subcompaction/stall/file 创建）",
                         "PerfContext（采样 op）", "/proc/diskstats（sectors、io_ticks busy%）"],
        "note": "ZeroFlush 臂普遍以 env/job 探针计数（ZFDBG-*、zf.* 属性）而非重编译插桩；原生臂以公共 API 为主。",
    }
    env["measurement_caveats"] = [
        "回放方差大：原生战役同一配置两次独立运行可差 1.86×（工作量不变、设备交付带宽 1.7×）；"
        "ZeroFlush 侧同样记录 7%–20% 的运行间方差与 native 跨轮 ±50% 波动。",
        "系统常驻负载 5–45 波动（grafana/prometheus/ceph），故跨时间窗的绝对值不可直接比较，"
        "只有同窗背靠背运行内部可比。",
        "一次机器重启（2026-09-24）重置了设备内部状态，之后的重复系统性更快，列为未受控验证的候选原因。",
    ]
    return env


# ---------------------------------------------------------------- machine: runs


def build_runs(scope="active"):
    pre = SCOPES[scope]["path_prefix"]
    stages = {}
    total = 0
    for stage in sorted(os.listdir(os.path.join(ROOT, "bench/out"))):
        mf = os.path.join(ROOT, "bench/out", stage, "manifest.jsonl")
        if not os.path.exists(mf):
            continue
        runs = []
        for ln in open(mf, encoding="utf-8"):
            ln = ln.strip()
            if not ln:
                continue
            try:
                d = json.loads(ln)
            except Exception:
                continue
            runs.append(d)
        stages[stage] = runs
        total += len(runs)
    # machine-written benchmark result files with a runs[] array
    reports = []
    for dp, _, fns in os.walk(os.path.join(ROOT, "output/archive" if scope == "archived" else "output")):
        if scope == "active" and "/archive" in "/" + os.path.relpath(dp, ROOT):
            continue
        for fn in fns:
            if not fn.endswith(".json") or "/report/" in dp or "/catalog" in dp:
                continue
            fp = os.path.join(dp, fn)
            try:
                d = read_json(fp)
            except Exception:
                continue
            if isinstance(d, dict) and isinstance(d.get("runs"), list):
                reports.append({
                    "path": rel(fp),
                    "engine": d.get("engine"),
                    "timestamp": d.get("timestamp"),
                    "binary": d.get("db_bench_path"),
                    "workload_config": d.get("config"),
                    "hardware": d.get("hardware"),
                    "runs": d["runs"],
                    "tracked": rel(fp) in tracked_set([rel(fp)]),
                })
            elif isinstance(d, list) and d and isinstance(d[0], dict) and "engine" in d[0]:
                reports.append({"path": rel(fp), "shape": "array-of-arms",
                                "arms": len(d), "tracked": rel(fp) in tracked_set([rel(fp)])})
    return OrderedDict([
        ("schema_note", "逐运行身份索引。bench 战役的每次运行由 manifest.jsonl 记录"
                        "（二进制 md5 / git HEAD / host / 配置 / 返回码），这是可复现性的最小证据单元。"),
        ("bench_stages", {k: {
            "runs_file": f"bench/out/{k}/manifest.jsonl",
            "n_runs": len(v),
            "fields": sorted({f for r in v for f in r}),
            "runs": v,
        } for k, v in stages.items()}),
        ("bench_stage_totals", {"stages": len(stages), "runs": total}),
        ("report_result_files", reports),
    ])


# ---------------------------------------------------------------- machine: artifacts

# dataset id -> directories holding its raw artifacts.  Derived from CAMPAIGN.md §5
# (stage -> experiment mapping) and the per-milestone directory layout.
STAGE_OF_EXPERIMENT = {
    "bench/E1": ["bench/out/A"], "bench/E2": ["bench/out/A", "bench/out/B"],
    "bench/G#arrival": ["bench/out/G"], "bench/E3": ["bench/out/B"],
    "bench/E7": ["bench/out/B"], "bench/E8": ["bench/out/C"],
    "bench/E4": ["bench/out/D"], "bench/E6": ["bench/out/D"],
    "bench/E5": ["bench/out/A"], "bench/H#intervention": ["bench/out/H"],
    "bench/E9": ["bench/out/A"], "bench/E10": ["bench/out/E"],
}


# milestone -> directory holding its data, inside the scope's own prefix
MILESTONE_DIR = {"m3": "zeroflush_m3_perf", "m4": "zeroflush_m4_perf", "m5": "zeroflush_m5_perf"}
# entries whose id is a report basename rather than a sub-directory of the milestone dir
M5_REPORT_IN_ROOT = {"m5/B4B17B3_50GB_100GB_REPORT"}


PERF_V2_DIR = {
    "perf_v2/P0": "p0", "perf_v2/M1": "m1", "perf_v2/M2": "m2", "perf_v2/M3": "m3",
    "perf_v2/M4": "m4", "perf_v2/M5": "m5", "perf_v2/LAYOUT_BLOOM": "layout_bloom",
    "perf_v2/BACKLOG": "l0_backlog", "perf_v2/KSWEEP": "l0_backlog_kscan",
}


def artifacts_for(entry):
    eid = entry["id"]
    dirs = []
    if eid in PERF_V2_DIR:
        dirs = [f"bench/out/perf_v2/{PERF_V2_DIR[eid]}"]
    elif eid.startswith("bench/I#"):
        dirs = ["bench/out/I"]
    elif eid in STAGE_OF_EXPERIMENT:
        dirs = STAGE_OF_EXPERIMENT[eid]
    elif eid.startswith("bench/"):
        dirs = []
    else:
        ms = eid.split("/")[0]                    # m3 / m4 / m5
        if ms in MILESTONE_DIR:
            pre = SCOPES["archived"]["path_prefix"]
            base = eid.split("#")[0].split("/")[1]
            cand = (f"{pre}{MILESTONE_DIR[ms]}" if eid in M5_REPORT_IN_ROOT
                    else f"{pre}{MILESTONE_DIR[ms]}/{base}")
            dirs = [cand]
    dirs = [d for d in dirs if os.path.isdir(os.path.join(ROOT, d))]
    files, by_fmt, nbytes = [], Counter(), 0
    if dirs:
        derived_from = "directory"
        for d in dirs:
            absd = os.path.join(ROOT, d)
            depth = 1 if d.startswith("bench/out") and d.count("/") == 2 else 2
            for dp, dns, fns in os.walk(absd):
                if dp[len(absd):].count(os.sep) >= depth:
                    dns[:] = []
                for fn in fns:
                    fp = os.path.join(dp, fn)
                    try:
                        b = os.path.getsize(fp)
                    except OSError:
                        continue
                    t = fmt_of(fn)
                    by_fmt[t] += 1
                    nbytes += b
                    files.append({"path": rel(fp), "format": t, "bytes": b})
    else:
        # section-level entry (its subject is a section of a document, e.g. one experiment
        # inside archive/zeroflush_m4_perf/REPORT.md): its payload is exactly the set
        # of files its own citations point at — NOT the whole milestone tree.
        derived_from = "citations"
        cited = []
        for s in (entry.get("sources") or []):
            cited.append(str(s))
        for r in (entry.get("results") or []):
            if r.get("source"):
                cited.append(str(r["source"]))
        seen = set()
        for c in cited:
            f = c.split(":")[0]
            if f in seen:
                continue
            seen.add(f)
            fp = os.path.join(ROOT, f)
            if not os.path.isfile(fp):
                continue
            b = os.path.getsize(fp)
            t = fmt_of(f)
            by_fmt[t] += 1
            nbytes += b
            files.append({"path": f, "format": t, "bytes": b})
        dirs = sorted({os.path.dirname(f["path"]) for f in files})
    if not files:
        return None
    return {"dirs": dirs, "derived_from": derived_from, "n_files": len(files), "bytes": nbytes,
            "by_format": dict(sorted(by_fmt.items(), key=lambda kv: -kv[1])), "files": files}


# ---------------------------------------------------------------- machine: documents

DOC_CATEGORY = [
    ("protocol", re.compile(r"(CAMPAIGN|RUN_MANIFEST|PROTOCOL|DESIGN|PLAN|ROADMAP|NOTE_文件说明)")),
    ("report", re.compile(r"(REPORT|RESULTS|COMPARISON|DIAGNOSIS|AMPLIFICATION|progress|progress$|"
                          r"报告|BOTTLENECK|L0_INVESTIGATION|benchmark_analysis|bench_matrix_report|"
                          r"l0_bottleneck_report|l0l1_report|profiling_report|verification_report|"
                          r"comp_time_report|thread_scaling)")),
]


def classify_doc(path):
    name = os.path.basename(path)
    for cat, rx in DOC_CATEGORY:
        if rx.search(name):
            return cat
    return "note"


def build_documents(scope="active"):
    docs = []
    skip_dirs = {".git", "node_modules", ".zcode", ".qoder", "bench/db", "bench/base",
                 "source/rocksdb/third-party", "source/rocksdb-zeroflush/third-party"}
    for root in ["output", "bench", "exp_design", "."]:
        base = os.path.join(ROOT, root)
        for dp, dns, fns in os.walk(base):
            r = rel(dp)
            if any(r.startswith(s) for s in skip_dirs):
                dns[:] = []
                continue
            if r.startswith("output/report/catalog"):
                continue
            for fn in fns:
                if not fn.endswith(".md"):
                    continue
                fp = os.path.join(dp, fn)
                try:
                    txt = open(fp, encoding="utf-8", errors="replace").read()
                except OSError:
                    continue
                lines = txt.split("\n")
                title = next((l.lstrip("# ").strip() for l in lines if l.startswith("#")), "")
                r = rel(fp)
                if (scope == "archived") != r.startswith("archive/"):
                    continue
                if r.startswith("output/report/catalog/") or r.startswith("archive/catalog/"):
                    continue
                # keep our own docs; drop upstream RocksDB documentation (README/HISTORY/
                # docs/, java/, unreleased_history/ …) which would otherwise dominate the index
                if not (r.startswith(("bench/", "output/", "archive/", "exp_design/"))
                        or "/" not in r
                        or r.startswith("source/rocksdb-zeroflush/zeroflush/")):
                    continue
                docs.append({
                    "path": r,
                    "title": title[:160],
                    "category": classify_doc(rel(fp)),
                    "lines": len(lines),
                    "bytes": len(txt.encode()),
                    "milestone": ("m3" if "/zeroflush_m3_perf/" in rel(fp) else
                                  "m4" if "/zeroflush_m4_perf/" in rel(fp) else
                                  "m5" if "/zeroflush_m5_perf/" in rel(fp) else
                                  "bench" if rel(fp).startswith("bench/") else "repo"),
                })
    seen = set()
    dedup = []
    for d in docs:
        if d["path"] in seen:
            continue
        seen.add(d["path"])
        dedup.append(d)
    docs = dedup
    docs.sort(key=lambda d: (d["milestone"], d["category"], d["path"]))
    mt = tracked_set([d["path"] for d in docs])
    for d in docs:
        d["tracked"] = d["path"] in mt
    return docs


# ---------------------------------------------------------------- main


def build_scope(scope, entries_all, problems, stale, stats):
    """Build and write the 7 catalog files for one scope. Returns the index dict."""
    cat = SCOPES[scope]["catalog"]
    pre = SCOPES[scope]["path_prefix"]
    os.makedirs(cat, exist_ok=True)
    sel = [e for e in entries_all if any(e["id"].startswith(p) for p in SCOPES[scope]["entry_prefixes"])]
    sub_problems = [p for p in problems if any(p.startswith(e["id"]) for e in sel)]
    sub_stale = [list(x) for x in stale if any(x[0] == e["id"] for e in sel)]
    sub_stats = dict(stats)

    for e in sel:
        art = artifacts_for(e)
        if art:
            e["artifacts"] = {"dirs": art["dirs"], "derived_from": art["derived_from"],
                              "n_files": art["n_files"],
                              "bytes": art["bytes"], "by_format": art["by_format"]}
    sel.sort(key=lambda e: e["id"])

    env = build_environment(entries_all)
    env["scope_note"] = (f"scope={scope}；"
                         + ("本目录只含主线（原生 RocksDB 的 L0 调查与瓶颈判定战役）。"
                            if scope == "active" else
                            "本目录为存档作用域（ZeroFlush V0 的 m3/m4/m5 里程碑），"
                            "数据位于 archive/zeroflush_m*_perf/。"))
    runs = build_runs(scope)
    docs = build_documents(scope)

    artifacts = OrderedDict()
    for e in sel:
        art = artifacts_for(e)
        if art:
            artifacts[e["id"]] = art
    allpaths = sorted({f["path"] for a in artifacts.values() for f in a["files"]})
    tsp = tracked_set(allpaths)
    for a in artifacts.values():
        for f in a["files"]:
            f["tracked"] = f["path"] in tsp

    cat_counts = Counter(e["category"] for e in sel)
    ms_counts = Counter(e["id"].split("/")[0] for e in sel)
    gen_counts = Counter(e.get("generation", "v1") for e in sel)
    idx = OrderedDict()
    idx["schema_version"] = SCHEMA_VERSION
    idx["scope"] = scope
    idx["generation"] = {
        "v2": gen_counts.get("v2", 0), "v1": gen_counts.get("v1", 0),
        "note": ("v2 = perf_v2/*（2026-09-25 起的 runbook 战役，直方图编码 lower_bound_v2，"
                 "逐运行有效性门与工具 sha256）；v1 = 早期战役（其延迟百分位为 v1 编码占位值，"
                 "引用前须按桶下界重标）。逐数据集的 generation 字段见 experiments.json，"
                 "缺省表示 v1。"),
    }
    idx["generated_by"] = "output/report/catalog/build_catalog.py"
    idx["regenerate"] = "python3 output/report/catalog/build_catalog.py"
    idx["purpose"] = ("把仓库里的实验数据编成一份自描述的 JSON 目录：每个数据集带实验环境、"
                      "实验目的、结果条目、结果评估与限制，并且每条事实都带 file:LINE 出处。")
    if scope == "active":
        idx["other_scope"] = {"archived": "archive/catalog/index.json",
                              "note": "m3/m4/m5（ZeroFlush V0）已归档，不在主线目录与默认导出中。"}
    else:
        idx["other_scope"] = {"active": "output/report/catalog/index.json",
                              "note": "存档作用域；默认不参与导出，需要时用 export_bundle.py --scope archived。"}
    idx["files"] = OrderedDict([
        ("index.json", "本文件：总索引 + 校验报告"),
        ("experiments.json", "注解层：每个数据集的 环境/目的/结果/评估/限制/出处/产物概览"),
        ("environment.json", "共享环境参考：主机、设备、两套引擎与构建、仪器、测量注意"),
        ("runs.json", "逐运行身份索引（bench 各 stage 的 manifest.jsonl + 报告 JSON 的 runs[]）"),
        ("artifacts.json", "每个数据集的原始产物清单（格式、字节、是否入库）"),
        ("documents.json", "报告/协议/设计/说明类文档索引"),
        ("schema.json", "字段定义与填写约定"),
    ])
    idx["counts"] = OrderedDict([
        ("datasets", len(sel)),
        ("result_rows", sum(len(e.get("results") or []) for e in sel)),
        ("citations", sum(len(e.get("sources") or []) + len(e.get("results") or []) for e in sel)),
        ("machine_runs_bench", runs["bench_stage_totals"]["runs"]),
        ("documents", len(docs)),
        ("artifact_files_indexed", sum(a["n_files"] for a in artifacts.values())),
    ])
    idx["by_milestone"] = dict(sorted(ms_counts.items()))
    idx["by_category"] = dict(sorted(cat_counts.items()))
    n_eval = sum(1 for e in sel if (e.get("evaluation_zh") or "").strip())
    n_cav = sum(1 for e in sel if e.get("caveats_zh"))
    n_env = sum(1 for e in sel if any((e.get("environment") or {}).values()))
    idx["annotation_coverage"] = OrderedDict([
        ("entries_with_evaluation", n_eval),
        ("entries_with_caveats", n_cav),
        ("entries_with_nonempty_environment", n_env),
        ("citation_checks", idx["counts"]["citations"]),
        ("citation_failures", len(sub_stale)),
    ])
    idx["validation"] = OrderedDict([
        ("schema_problems", sub_problems),
        ("citation_problems", [{"id": i, "citation": c} for i, c in sub_stale][:40]),
        ("note", "schema_problems 为空且 citation_problems 为空即表示：所有必填键齐全、"
                 "所有出处都能解析到存在的文件与行、且该行非空。"),
    ])
    gaps = []
    for e in sel:
        missing = ([k for k in ("evaluation_zh",) if not (e.get(k) or "").strip()] +
                   [k for k in ("caveats_zh",) if not e.get(k)])
        if not missing:
            continue
        srcs = {str(s).split(":")[0] for s in (e.get("sources") or [])}
        srcs |= {str(r.get("source", "")).split(":")[0] for r in (e.get("results") or [])}
        machine_only = bool(srcs) and all(s.endswith(".json") for s in srcs)
        reason = ("来源为机器写出的裸测量文件（.json），文件内没有结论文本与假设判定，故留空而不补写。"
                  if machine_only else
                  "来源报告未陈述该项（限制/方差/裁决），故留空而不补写。")
        gaps.append({"id": e["id"], "missing": missing,
                     "source_kinds": sorted({os.path.splitext(s)[1] for s in srcs}), "reason": reason})
    idx["annotation_gaps"] = gaps
    idx["dataset_ids"] = [e["id"] for e in sel]
    idx["reading_guide"] = OrderedDict([
        ("稳定标识", "id 形如 <milestone>/<dataset>，如 m3/cmp_1kb50、bench/I#noc；与文件名/目录名对应。"),
        ("environment", "只写与 environment.json 基线不同处；未记录则 null（不猜）。"),
        ("results", "每条含 metric/value/unit/condition/source；value 为报告原文写法，未做换算。"),
        ("evaluation_zh", "报告自身的裁决（支持/否证/作废/已修正），不是本目录的再判断。"),
        ("caveats_zh", "报告自述的限制、方差、作废与更正；跨数据集的方法学限制见 environment.measurement_caveats。"),
        ("artifacts", "概览（目录/文件数/字节/格式分布）在 experiments.json；完整文件清单在 artifacts.json。"
                      "derived_from=directory 表示产物来自该数据集的目录；=citations 表示该条目是某文档的章节，"
                      "产物即其引用到的文件。"),
    ])

    schema = OrderedDict()
    schema["schema_version"] = SCHEMA_VERSION
    schema["dataset_entry"] = OrderedDict([
        ("generation", "str，v1|v2：该数据集属于哪一代实验（v2=perf_v2 战役，直方图编码 "
                       "lower_bound_v2、逐运行 §6 有效性门；缺省/\"v1\"=早期战役，"
                       "其延迟百分位为 v1 编码占位值，引用前须按桶下界重标）"),
        ("id", "str，稳定标识 <milestone>/<dataset>"),
        ("title_zh", "str，中文短标题"),
        ("engine", "list[str]，取值 native / zeroflush"),
        ("category", sorted(VALID_CATEGORIES)),
        ("purpose_zh", "str，实验目的（该实验回答什么问题）"),
        ("environment", {k: "str|null" for k in ENV_KEYS}),
        ("results", [{"metric": "str", "value": "str（原文写法）", "unit": "str",
                      "condition": "str（哪条臂/哪次运行）", "source": "str path:LINE"}]),
        ("evaluation_zh", "str，报告自身裁决"),
        ("caveats_zh", "list[str]，报告自述限制/方差/作废"),
        ("sources", "list[str] path:LINE"),
        ("artifacts", {"dirs": "list[str]", "n_files": "int", "bytes": "int",
                       "by_format": "dict[str,int]"}),
    ])
    schema["conventions"] = [
        "path 一律为仓库根相对路径。",
        "source 一律 'path:LINE'，LINE 为 1 基行号；允许 'path:A-B' 与 'path:A,B-C'。",
        "未在来源中写明的字段填 null 或省略，禁止推断。",
        "数值保持报告原文（含单位与千分位），目录不做单位换算。",
        "tracked=true 表示该产物已入库；false 表示被 .gitignore 排除、只在本机保留。",
    ]

    for name, obj in [("index.json", idx), ("experiments.json", sel),
                      ("environment.json", env), ("runs.json", runs),
                      ("artifacts.json", artifacts), ("documents.json", docs),
                      ("schema.json", schema)]:
        with open(os.path.join(cat, name), "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=1)
            f.write("\n")
    return idx


def main():
    entries, load_problems = load_annotated()
    problems, stale, stats = validate(entries)
    problems += load_problems
    out = {}
    for scope in ("active", "archived"):
        idx = build_scope(scope, entries, problems, stale, stats)
        out[scope] = idx
        c = idx["counts"]
        print(f"[{scope:8s}] datasets={c['datasets']:<4} results={c['result_rows']:<5} "
              f"citations={c['citations']:<5} docs={c['documents']:<4} "
              f"artifact_files={c['artifact_files_indexed']:<6} "
              f"schema_problems={len(idx['validation']['schema_problems'])} "
              f"citation_problems={len(idx['validation']['citation_problems'])} "
              f"-> {os.path.relpath(SCOPES[scope]['catalog'], ROOT)}")
    bad = sum(len(out[s]["validation"]["schema_problems"]) +
              len(out[s]["validation"]["citation_problems"]) for s in out)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
