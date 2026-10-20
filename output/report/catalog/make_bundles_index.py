#!/usr/bin/env python3
"""make_bundles_index.py — regenerate the "bundles 里有哪些实验" table in
output/report/README.md.

Start time comes from each run's own `<run>.config.txt` (`start_wall=<unix ts>`), which the
driver writes at process start — it is the authoritative run-start stamp.  Experiments with
no start stamp (protocol documents, single analysis tables) get an empty cell rather than a
guessed time.

Usage: python3 output/report/catalog/make_bundles_index.py [--check]
"""
import argparse
import glob
import json
import os
import sys
from collections import Counter
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import export_bundle as eb  # noqa: E402

ROOT = eb.ROOT
README = os.path.join(ROOT, "output/report/README.md")
BEGIN = "<!-- BUNDLES-INDEX:BEGIN -->"
END = "<!-- BUNDLES-INDEX:END -->"


def dataset_times(files):
    """(first_start, last_start, n_run_dirs) from the runs' config.txt files.

    One run directory can hold several config.txt files (a run with kill/recovery phases
    writes one per phase), so count distinct directories, not files.
    """
    stamps, dirs = [], set()
    for f in files:
        if not f.endswith(".config.txt"):
            continue
        try:
            with open(f, encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    if line.startswith("start_wall="):
                        stamps.append(float(line.split("=", 1)[1]))
                        dirs.add(os.path.dirname(f))
                        break
        except OSError:
            continue
    if not stamps:
        return None, None, 0
    return min(stamps), max(stamps), len(dirs)


def fmt(ts):
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def build():
    experiments, env, artifacts, runs = eb.load_catalog("active")
    idx = json.load(open(os.path.join(eb.SCOPE_CATALOG["active"], "index.json")))
    # bundle sizes / completeness from the exported index
    bpath = os.path.join(eb.SCOPE_OUT["active"], "index.json")
    bundles = {b["id"]: b for b in (json.load(open(bpath))["bundles"] if os.path.exists(bpath) else [])}

    # which payload directories are shared by several entries (CAMPAIGN.md §5 maps stages
    # to experiments many-to-many, so e.g. E1/E2/E5/E9 all live in stage A)
    dir_owner = {}
    for e in experiments:
        a = artifacts.get(e["id"]) or {}
        for d in a.get("dirs", []):
            dir_owner.setdefault(d, []).append(e["id"])
    shared = {d for d, ids in dir_owner.items() if len(ids) > 1}

    rows = []
    gen_of = lambda e: e.get("generation", "v1")
    # new-version datasets first: the point of this table is to make the v2 block obvious
    for e in sorted(experiments, key=lambda x: (0 if gen_of(x) == "v2" else 1, x["id"])):
        files = eb.select_files(e, artifacts)
        t0, t1, n = dataset_times(files)
        if t0 is None:
            when = "—"                                   # no start stamp found: leave empty
        elif t1 and (t1 - t0) > 3600:
            when = f"{fmt(t0)} 起（{n} 个运行目录，至 {fmt(t1)}）"
        elif n > 1:
            when = f"{fmt(t0)}（{n} 个运行目录）"
        else:
            when = fmt(t0)
        b = bundles.get(e["id"], {})
        a = artifacts.get(e["id"]) or {}
        mark = " †" if (n and any(d in shared and d.startswith("bench/out/")
                                  for d in a.get("dirs", []))) else ""
        rows.append((e["id"], e, when + mark, n, b, gen_of(e)))

    lines = []
    lines.append(BEGIN)
    lines.append("")
    n_v2 = sum(1 for r in rows if r[5] == "v2")
    lines.append(f"**{len(rows)} 个主线实验包**（`output/bundles/<id>.zexp`），其中 **{n_v2} 个是 v2 新版本**"
                 "（`perf_v2/*`，2026-09-25 起的 runbook 战役：直方图编码 lower_bound_v2、逐运行有效性门、"
                 "工具版本逐运行记录）。版本列标 `**v2**` 的即为新版本；其余为 v1 早期战役"
                 "（其延迟百分位是 v1 编码的占位值，引用前须按桶下界重标）。"
                 "起始时间取各运行自带的 `start_wall`（驱动进程启动时刻）；"
                 "没有该时间戳的实验（协议文档、单张分析表）留空。")
    lines.append("")
    lines.append("| # | 包（id → 文件） | 版本 | 做了什么实验 | 起始时间 | 运行目录 | 包大小 | 数据完整性 |")
    lines.append("|---:|---|---|---|---|---:|---:|---|")
    for i, (eid, e, when, n, b, gen) in enumerate(rows, 1):
        size = f"{b['bytes']/2**20:.1f} MiB" if b else "n/a"
        mode = {"full": "全部产物", "partial": "有省略（见包内 omitted）",
                "metadata-only": "仅元数据"}.get(b.get("mode"), "n/a") if b else "n/a"
        purpose = (e["purpose_zh"] or "").strip().replace("\n", " ")
        if len(purpose) > 96:
            purpose = purpose[:94].rstrip() + "…"
        gcell = "**v2**" if gen == "v2" else "v1"
        lines.append(f"| {i} | `{eid}` → `{eid.replace('#','-')}.zexp` | {gcell} | {e['title_zh']}。{purpose} "
                     f"| {when} | {n or '—'} | {size} | {mode} |")
    lines.append("")
    lines.append("† 该实验与其它实验**共用同一个 stage 目录**（`bench/out/{A,B,C,D}`）：起始时间与运行数"
                 "是那个 stage 的，不是该实验独有的。stage↔实验是多对多映射（见 `bench/CAMPAIGN.md` §5），"
                 "逐运行的实验归属由包内 `EXPERIMENT.json.reproduce` 与各 stage 的 `manifest.jsonl` 决定。")
    lines.append("")
    lines.append(END)
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="fail if the README is out of date")
    args = ap.parse_args()

    table = build()
    src = open(README, encoding="utf-8").read()
    if BEGIN in src and END in src:
        i, j = src.index(BEGIN), src.index(END) + len(END)
        new = src[:i] + table + src[j:]
    else:
        marker = "## JSON 数据目录"
        assert marker in src, "no insertion point in README"
        new = src.replace(marker, table + "\n\n" + marker, 1)

    if args.check:
        if new != src:
            print("README bundles index is OUT OF DATE (run make_bundles_index.py)")
            return 1
        print("README bundles index is up to date")
        return 0
    if new != src:
        open(README, "w", encoding="utf-8").write(new)
        print(f"README updated ({len(table.splitlines())} lines of index)")
    else:
        print("README already up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())
