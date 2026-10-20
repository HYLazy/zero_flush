#!/usr/bin/env python3
"""RocksDB compaction 耗时拆解饼图（图 14）。

三个维度，全部来自仓库内实测产物：
  ① 按层级对：compaction 墙钟花在哪一层。数据 = bench/out/fillcpu_1kb100_src2/run.comp_jobs.csv
     的 base_level（= base_input_level）→ out_level。口径与 bench/bn_analyze.py 的
     start->out 一致；trivial move（<1 ms 且输出≈输入）照旧计入，本跑仅 170 个、合计 1 s。
  ② 按活动：在算还是在等。同一 CSV：wall / cpu_us_self / io_cpu_{read,write}_ns / io_fsync_ns。
     io_* 是线程本地 iostats_context 的增量（见 bench/fillcpu.cc OnCompactionBegin/Completed）。
     四片按定义精确加和到 wall：cpu_self = 纯逻辑计算 + read/write 系统调用 CPU；
     wall − cpu_self − fsync = 其他等待。
  ③ 按函数类别：计算的内部构成。bench/out/fillcpu_1kb100_bg2matched/perf/*.flat.txt
     （perf report -g none，6 次采样共 20000 样本；该跑与 ① ② 的生效配置逐项一致）。
     分类表直接复用 bench/analyze_compaction_cost.py 的 CAT，只按用户态帧归一化
     （内核帧 49.3% 已由 ② 的系统调用 CPU 单独表达，不重复计）。

输出：report/charts/fig14_compaction_pie.{png,svg,pdf}，并打印切片审计表。
"""
import csv
import glob
import importlib.util
import json
import os
import re
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patches import Patch
import numpy as np

ROOT = "/home/embed/hyl/metadata_offload"
OUT = f"{ROOT}/report/charts"
SRC2 = f"{ROOT}/bench/out/fillcpu_1kb100_src2/run"
PERF = f"{ROOT}/bench/out/fillcpu_1kb100_bg2matched/perf"

# ── palette（与 make_charts.py / make_write_pie.py 同一套） ──
BORDER, ICON = "#c7ced2", "#3f6c83"
HEADER, BLOCK = "#375462", "#5b7887"
ACCENT, TEXT, MUTED = "#a14c2f", "#181a1b", "#848b8e"
SUCCESS, WARNING = "#489762", "#8a7446"
RUST_L, RUST_M = "#c98a6a", "#b96a45"
RAMP = ["#375462", "#4a6b7c", "#5b7887", "#6f8b98", "#8ba3ae", "#a8bcc4", "#c7ced2", "#dde3e6"]

font_manager.fontManager.addfont(f"{ROOT}/report/fonts/NotoSansSC-Regular.ttf")
font_manager.fontManager.addfont(f"{ROOT}/report/fonts/NotoSansSC-Bold.ttf")
plt.rcParams.update({
    "font.family": "Noto Sans SC", "font.size": 9, "axes.unicode_minus": False,
    "figure.facecolor": "white", "text.color": TEXT, "savefig.dpi": 300,
    "savefig.bbox": "tight", "savefig.pad_inches": 0.12,
})

rows = list(csv.DictReader(open(SRC2 + ".comp_jobs.csv")))


def g(r, k):
    return float(r[k]) if r.get(k) else 0.0


# ══ ① 按层级对 ═══════════════════════════════════════════════════════════
by_level = defaultdict(lambda: dict(n=0, wall=0.0, cpu=0.0, cout=0.0, triv=0))
for r in rows:
    start, outl = int(r["base_level"]), int(r["out_level"])
    key = "L0→base" if start == 0 else f"L{start}→L{outl}"
    a = by_level[key]
    a["n"] += 1
    a["wall"] += g(r, "wall_us") / 1e6
    a["cpu"] += g(r, "cpu_us_self") / 1e6
    a["cout"] += g(r, "out_bytes") / 1e9
    if g(r, "rdb_elapsed_us") < 1000 and g(r, "out_bytes") <= max(g(r, "in_bytes"), 1) * 1.001:
        a["triv"] += 1
DEEP = sum(by_level[k]["wall"] for k in by_level if k != "L0→base")
ORDER1 = [("L0→base", "L0→base 直装（base level 随库增长从 L3 移到 L6）", ICON),
          ("L3→L4", "L3→L4", RUST_L),
          ("L4→L5", "L4→L5", RUST_M),
          ("L5→L6", "L5→L6", ACCENT)]
V1 = [(lbl, by_level[k]["wall"], col, by_level[k]) for k, lbl, col in ORDER1]
TOT1 = sum(v for _, v, _, _ in V1)

# ══ ② 按活动 ═════════════════════════════════════════════════════════════
W = sum(g(r, "wall_us") for r in rows) / 1e6
C = sum(g(r, "cpu_us_self") for r in rows) / 1e6
SYSCPU = (sum(g(r, "io_cpu_read_ns") for r in rows) + sum(g(r, "io_cpu_write_ns") for r in rows)) / 1e9
FSYNC = sum(g(r, "io_fsync_ns") for r in rows) / 1e9
LOGIC = C - SYSCPU
OTHER_WAIT = W - C - FSYNC
V2 = [("纯逻辑计算（归并、编解码、键比较、校验和）", LOGIC, HEADER),
      ("read/write 系统调用内的 CPU（页缓存拷贝）", SYSCPU, ICON),
      (f"等待（fsync 落盘 {FSYNC:,.0f} s + 其他 {OTHER_WAIT:.0f} s）", FSYNC + OTHER_WAIT, ACCENT)]
TOT2 = W

# ══ ③ 按函数类别（用户态帧归一化） ═══════════════════════════════════════
spec = importlib.util.spec_from_file_location("acc", f"{ROOT}/bench/analyze_compaction_cost.py")
acc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(acc)
tot, NS = defaultdict(float), 0.0
for f in sorted(glob.glob(PERF + "/*.flat.txt")):
    txt = open(f).read()
    m = re.search(r"#\s*Samples:\s*([0-9.]+)\s*([KM]?)", txt)
    n = float(m.group(1)) * (1e3 if m.group(2) == "K" else 1e6) if m else 0.0
    # 每行只印两位小数，1000+ 行会累积约 ±1% 的舍入误差；先在文件内归一化，再按样本数加权
    per_file = defaultdict(float)
    for line in txt.splitlines():
        mm = re.match(r"\s+([0-9.]+)%\s+\S+\s+\S+\s+\[.\] (.*)", line)
        if mm:
            per_file[acc.cat(mm.group(2).strip())] += float(mm.group(1))
    scale = n / sum(per_file.values())
    NS += n
    for k, v in per_file.items():
        tot[k] += v * scale
KERNEL = "内核帧（系统调用/页缓存拷贝与写回/调度）"
USER = NS - tot[KERNEL]
SHORT = {
    "输入块读取与解析（BlockFetcher/DataBlockIter/IndexBlockIter/块缓存/文件读取封装）":
        "输入块读取与解析（BlockFetcher/DataBlockIter/块缓存）",
    "归并/迭代框架（CompactionIterator/MergingIterator/表迭代器/Seek-Next 调度）":
        "归并与迭代框架（CompactionIterator/MergingIterator）",
    "输出块构建与写出（BlockBuilder 前缀压缩/EmitBlock/表构建/WritableFileWriter）":
        "输出块构建与写出（BlockBuilder/EmitBlock/文件写入）",
    "数据搬运（memmove/memcpy）": "数据搬运（memmove/memcpy）",
    "键比较（memcmp/Comparator）": "键比较（memcmp/Comparator）",
    "校验和（XXH3/crc32）": "校验和（XXH3/crc32）",
    "内存分配（malloc/free/new/delete/Arena）": "内存分配（malloc/free/Arena）",
    "其他用户态（未归类小函数）": "其他用户态（长尾，最大单项仅 4.6%）",
}
usr = {k: v for k, v in tot.items() if k != KERNEL}
usr["其他用户态（未归类小函数）"] += usr.pop("TLS/动态链接/vdso")   # 并入长尾
V3 = sorted(usr.items(), key=lambda kv: -kv[1])
V3 = [(SHORT.get(k, k), v / USER, RAMP[i % len(RAMP)]) for i, (k, v) in enumerate(V3)]
TOT3 = 1.0

# ══ 画布：3 行 × 2 列（饼图 + 图例） ═════════════════════════════════════
fig = plt.figure(figsize=(7.8, 9.0))
gs = fig.add_gridspec(3, 2, height_ratios=[3.15, 1.0, 3.2], hspace=0.0, wspace=0.04,
                      left=0.02, right=0.98, top=0.862, bottom=0.075)


def draw_pie(ax, vals, cols, outside=None, inside_min=0.10, lim=(1.62, 1.52)):
    """扇区内只放百分比；< inside_min 的用引线把百分比放到图外（outside 给手工锚点）。"""
    wedges, _ = ax.pie(vals, startangle=90, counterclock=False, colors=cols,
                       wedgeprops=dict(edgecolor="white", lw=1.3))
    frac = np.array(vals) / np.sum(vals)
    for i, (w, f) in enumerate(zip(wedges, frac)):
        ang = np.deg2rad((w.theta1 + w.theta2) / 2.0)
        x, y = np.cos(ang), np.sin(ang)
        if f >= inside_min:
            ax.text(x * 0.58, y * 0.58, f"{f*100:.1f}%", ha="center", va="center",
                    fontsize=9.5, color="white", fontweight="bold")
        elif outside:
            tx, ty, ha = outside[i]
            ax.annotate(f"{f*100:.1f}%", xy=(x * 0.99, y * 0.99), xytext=(tx, ty),
                        ha=ha, va="center", fontsize=8.5, color=TEXT, fontweight="bold",
                        arrowprops=dict(arrowstyle="-", color=MUTED, lw=0.7,
                                        shrinkA=0, shrinkB=1.5))
    ax.set_xlim(-lim[0], lim[0])
    ax.set_ylim(-lim[1] * 0.95, lim[1])
    ax.set_axis_off()


def legend_below(ax, items, fmt, ncol=1, fs=7.5):
    ax.set_axis_off()
    handles = [Patch(facecolor=c, edgecolor="white", lw=0.8) for _, _, c in items]
    labels = [f"{n}　{fmt(v)}" for n, v, _ in items]
    ax.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 1.02),
              ncol=ncol, frameon=False, fontsize=fs, handlelength=1.1,
              labelspacing=0.36, borderpad=0.0, columnspacing=1.4)


# ① 层级对
ax1, lg1 = fig.add_subplot(gs[0, 0]), fig.add_subplot(gs[1, 0])
draw_pie(ax1, [v for _, v, _, _ in V1], [c for _, _, c, _ in V1], lim=(1.45, 1.40))
legend_below(lg1, [(n, v, c) for n, v, c, _ in V1],
             lambda v: f"{v:,.0f} s　{v/TOT1*100:.1f}%")
ax1.set_title(f"① compaction 墙钟花在哪一层　合计 {TOT1:,.0f} s\n"
              f"深层级联（L3→L4 起）合计 {DEEP/TOT1*100:.1f}%",
              loc="center", fontsize=9.2, fontweight="bold", pad=7, linespacing=1.5)

# ② 活动
ax2, lg2 = fig.add_subplot(gs[0, 1]), fig.add_subplot(gs[1, 1])
draw_pie(ax2, [v for _, v, _ in V2], [c for _, _, c in V2], lim=(1.45, 1.40))
legend_below(lg2, V2, lambda v: f"{v:,.0f} s　{v/TOT2*100:.1f}%")
ax2.set_title(f"② compaction 墙钟花在干什么　合计 {TOT2:,.0f} s\n"
              f"线程 CPU {C:,.0f} s + 等待 {TOT2-C:,.0f} s",
              loc="center", fontsize=9.2, fontweight="bold", pad=7, linespacing=1.5)

# ③ 函数类别
ax3, lg3 = fig.add_subplot(gs[2, 0]), fig.add_subplot(gs[2, 1])
draw_pie(ax3, [v for _, v, _ in V3], [c for _, _, c in V3],
         outside={4: (-1.34, 0.34, "right"), 5: (-1.30, 0.88, "right"),
                  6: (-0.82, 1.36, "right"), 7: (0.30, 1.46, "left")},
         inside_min=0.12, lim=(1.78, 1.62))
legend_below(lg3, V3, lambda v: f"{v*100:.1f}%", fs=7.3)
ax3.set_title("③ compaction 的计算内部构成\n"
              "perf 采样，占用户态样本（内核帧 49.3% 已计入 ② 的系统调用 CPU）",
              loc="center", fontsize=9.2, fontweight="bold", pad=7, linespacing=1.5)

# 状态行 + 脚注
st = json.load(open(SRC2 + ".summary.json"))
RUN_WALL = st["fill_wall_s"] + st["drain_wall_s"]
fig.text(0.02, 0.950,
         f"单线程 compaction：整个 {RUN_WALL:,.0f} s 的跑里，作业把线程占住 {TOT1:,.0f} s（{TOT1/RUN_WALL*100:.0f}%），"
         f"但真正花 CPU 只有 {C:,.0f} s（占墙钟 {C/TOT1*100:.0f}%）；{FSYNC/TOT1*100:.0f}% 的时间在等 fsync 落盘。"
         f"写放大 {st['jobs']['comp_in_bytes']/st['workload']['app_bytes']:.1f}×。",
         fontsize=8.3, color=TEXT, ha="left")
notes = [
    "数据源：bench/out/fillcpu_1kb100_src2/run.comp_jobs.csv（①②，默认配置 · 100 GB · 1 KB KV · 16 写线程 · "
    "3,381 个作业 · 输入 953 GB）；",
    "bench/out/fillcpu_1kb100_bg2matched/perf/*.flat.txt（③，与①②生效配置逐项一致的另一跑，6 次采样 20,000 样本）。",
    "口径：①② 的“墙钟”是各作业自身耗时之和（单线程串行，故≈该线程被占用时长）；③ 是函数级样本占比，不做秒数折算。",
]
y = 0.055
for txt in notes:
    fig.text(0.02, y, txt, fontsize=7.0, color=MUTED, ha="left")
    y -= 0.019
fig.suptitle("RocksDB compaction 耗时拆解：64% 花在深层级联，42% 在等 fsync 落盘",
             x=0.02, ha="left", fontsize=11.4, fontweight="bold", y=0.981)

os.makedirs(OUT, exist_ok=True)
for ext in ("png", "svg", "pdf"):
    fig.savefig(f"{OUT}/fig14_compaction_pie.{ext}")
plt.close(fig)

# ── 审计表 ──
print(f"① 按层级对（合计 {TOT1:,.1f} s）")
for n, v, _, a in V1:
    print(f"   {n:<26s} {v:8.0f} s {v/TOT1*100:6.1f}%  jobs {a['n']:5d}（trivial {a['triv']}）"
          f"  CPU {a['cpu']:7.0f} s  输出 {a['cout']:6.1f} GB")
print(f"   深层级联合计（L3→L4 + L4→L5 + L5→L6）= {DEEP:.0f} s（{DEEP/TOT1*100:.1f}%）")
print(f"② 按活动（合计 {TOT2:,.1f} s）")
for n, v, _ in V2:
    print(f"   {n:<42s} {v:8.0f} s {v/TOT2*100:6.1f}%")
print(f"   校验：{LOGIC:.0f} + {SYSCPU:.0f} + {FSYNC:.0f} + {OTHER_WAIT:.0f} = "
      f"{LOGIC+SYSCPU+FSYNC+OTHER_WAIT:.1f} s（wall {TOT2:.1f}）")
print(f"③ 计算的函数级构成（用户态 {USER:.0f} 样本 / 全部 {NS:.0f}）")
for n, v, _ in V3:
    print(f"   {n:<46s} {v*100:5.1f}%")
print(f"   用户态合计 {sum(v for _, v, _ in V3)*100:.1f}%")
print("written:", sorted(f for f in os.listdir(OUT) if f.startswith("fig14")))
