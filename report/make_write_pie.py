#!/usr/bin/env python3
"""RocksDB 写流程耗时拆解饼图（图 13）。

数据来源（全部为仓库内实测产物，经 report/data/data.json 中转）：
  * ①「单次 1 KB Put 的引擎内耗时」：bench/out/wpbench/wpbench_perf.csv 中 tag=quiet_t1
    的修正后一行（PerfContext kEnableTime，1 线程、无后台干扰、2M ops，取每 tag 最后一次出现）。
  * ②「100 GB 写入负载的全部 CPU 去向」：
      - 写路径/flush/compaction/驱动 四角色：bench/out/fillcpu_1kb100_default/run（fill+drain，
        逐线程 /proc utime+stime 累计），即 data.json 的 runs.bg2_arch。
      - 写路径内部的再拆解：有效写功取 wpbench.csv put_quiet_wal 的单线程 5.764 µs/KV；
        16 线程协调地板取 bench/out/wpbench/WRITEPATH_DECOMPOSITION.md §6 的 31.4 µs/op
        （quiet 16 线程实测 31.0、1 GB 默认配置 31.8，差 <1.5%）；
        限速/停车期自旋放大 = 写路径实测 − 地板。
  * 墙钟注记：同 run 的 .summary.json stalls.stall_s 与 fill_wall_s。

版式：扇区内只放百分比（短文本，不会溢出扇区），名称与绝对量由图例承载。
输出：report/charts/fig13_write_flow_pie.{png,svg,pdf}，并打印切片审计表。
"""
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patches import Patch
import numpy as np

ROOT = "/home/embed/hyl/metadata_offload"
OUT = f"{ROOT}/report/charts"
D = json.load(open(f"{ROOT}/report/data/data.json"))

# ── palette（与 make_charts.py 同一套） ──
BORDER, ICON = "#c7ced2", "#3f6c83"
HEADER, BLOCK = "#375462", "#5b7887"
ACCENT, TEXT, MUTED = "#a14c2f", "#181a1b", "#848b8e"
SUCCESS, WARNING = "#489762", "#8a7446"
GREEN_L, GREEN_XL = "#7cae8a", "#bcd3c2"

font_manager.fontManager.addfont(f"{ROOT}/report/fonts/NotoSansSC-Regular.ttf")
font_manager.fontManager.addfont(f"{ROOT}/report/fonts/NotoSansSC-Bold.ttf")
plt.rcParams.update({
    "font.family": "Noto Sans SC", "font.size": 9, "axes.unicode_minus": False,
    "figure.facecolor": "white", "text.color": TEXT, "savefig.dpi": 300,
    "savefig.bbox": "tight", "savefig.pad_inches": 0.12,
})

# ══ ① 单次 1 KB Put 的引擎内耗时（PerfContext，1 线程 quiet） ══════════════
t1 = [r for r in D["wpbench"]["wpbench_perf.csv"] if r["tag"] == "quiet_t1"][-1]  # 修正后那一轮
A_ITEMS = [
    ("memtable 插入", float(t1["write_memtable_time_per_op_ns"]), SUCCESS),
    ("WAL 写入", float(t1["write_wal_time_per_op_ns"]), GREEN_L),
    ("前后处理", float(t1["write_pre_and_post_process_time_per_op_ns"]), GREEN_XL),
]
A_TOT = sum(v for _, v, _ in A_ITEMS)

# ══ ② 100 GB 写入负载的全部 CPU 去向（fill + drain） ══════════════════════
run = D["runs"]["bg2_arch"]
OPS = 96_000_000                                           # 96M 键 × 1 KB = 99.84 GB
USEFUL_PER_OP = float([r for r in D["wpbench"]["wpbench.csv"]
                       if r["tag"] == "put_quiet_wal"][0]["cpu_us_per_op"])  # 5.764 µs
FLOOR_PER_OP = 31.4        # WRITEPATH_DECOMPOSITION.md §6（quiet 16 线程地板）


def role(r, k):
    return r["cpu_fill"][k] + r["cpu_drain"][k]


B_TOT = run["cpu_fill_total"] + run["cpu_drain_total"]
B_WRITER = role(run, "writer")
B_USEFUL = USEFUL_PER_OP * 1e-6 * OPS
B_FLOOR = FLOOR_PER_OP * 1e-6 * OPS
B_COORD, B_SPIN = B_FLOOR - B_USEFUL, B_WRITER - B_FLOOR
assert abs((B_USEFUL + B_COORD + B_SPIN) - B_WRITER) < 1e-6

B_ITEMS = [
    ("有效写功（写 WAL + 插 memtable + 驱动）", B_USEFUL, SUCCESS),
    ("写线程协调/自旋（16 线程地板 31.4 µs/op）", B_COORD, WARNING),
    ("限速/停车期自旋放大", B_SPIN, ACCENT),
    ("flush 线程", role(run, "flush_pool"), ICON),
    ("compaction 线程", role(run, "compaction_pool"), BLOCK),
    ("驱动/采样线程", role(run, "driver"), BORDER),
]
STALL_S = json.load(open(f"{ROOT}/bench/out/fillcpu_1kb100_default/run.summary.json"))["stalls"]["stall_s"]
FILL_WALL = run["fill_wall_s"]

# ══ 画布：每列 = 饼图 + 下方图例 ═════════════════════════════════════════
fig = plt.figure(figsize=(7.8, 6.2))
gs = fig.add_gridspec(2, 2, height_ratios=[3.15, 1.0], hspace=0.0, wspace=0.04,
                      left=0.015, right=0.985, top=0.795, bottom=0.075)
axA, axB = fig.add_subplot(gs[0, 0]), fig.add_subplot(gs[0, 1])
lgA, lgB = fig.add_subplot(gs[1, 0]), fig.add_subplot(gs[1, 1])


def draw_pie(ax, items, outside):
    """扇区内只放百分比；<inside_min 的扇区用引线把百分比放到图外。

    outside: {名称: (text_x, text_y, ha)} 手工锚定点，避免相邻小扇区标签相撞。
    """
    vals = [v for _, v, _ in items]
    cols = [c for _, _, c in items]
    wedges, _ = ax.pie(vals, startangle=90, counterclock=False, colors=cols,
                       wedgeprops=dict(edgecolor="white", lw=1.3))
    frac = np.array(vals) / np.sum(vals)
    for w, f, (name, val, col) in zip(wedges, frac, items):
        ang = np.deg2rad((w.theta1 + w.theta2) / 2.0)
        x, y = np.cos(ang), np.sin(ang)
        if f >= 0.10:
            ax.text(x * 0.60, y * 0.60, f"{f*100:.1f}%", ha="center", va="center",
                    fontsize=9.5, color="white", fontweight="bold")
        else:
            tx, ty, ha = outside[name]
            ax.annotate(f"{f*100:.1f}%", xy=(x * 0.99, y * 0.99), xytext=(tx, ty),
                        ha=ha, va="center", fontsize=8.5, color=TEXT, fontweight="bold",
                        arrowprops=dict(arrowstyle="-", color=MUTED, lw=0.7,
                                        shrinkA=0, shrinkB=1.5))
    ax.set_xlim(-1.62, 1.62)
    ax.set_ylim(-1.40, 1.52)
    ax.set_axis_off()


def legend_below(ax, items, total, fmt, ncol=1):
    ax.set_axis_off()
    handles = [Patch(facecolor=c, edgecolor="white", lw=0.8) for _, _, c in items]
    labels = [f"{n}　{fmt(v)}　{v/total*100:.1f}%" for n, v, _ in items]
    ax.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 1.02),
              ncol=ncol, frameon=False, fontsize=7.6, handlelength=1.1,
              labelspacing=0.34, borderpad=0.0, columnspacing=1.4)


draw_pie(axA, A_ITEMS, {"前后处理": (0.10, 1.32, "left")})
draw_pie(axB, B_ITEMS, {"有效写功（写 WAL + 插 memtable + 驱动）": (0.46, 1.28, "left"),
                        "驱动/采样线程": (-0.14, 1.40, "center"),
                        "flush 线程": (-1.20, -0.10, "right")})
legend_below(lgA, A_ITEMS, A_TOT, fmt=lambda v: f"{v/1000:.2f} µs")
legend_below(lgB, B_ITEMS, B_TOT, fmt=lambda v: f"{v:,.0f} 核·秒")

axA.set_title(f"① 单次 1 KB Put 的引擎内耗时　合计 {A_TOT/1000:.2f} µs\n"
              "PerfContext 实测 · 1 线程 · 无后台干扰 · 2 M ops",
              loc="center", fontsize=9.5, fontweight="bold", pad=8, linespacing=1.5)
axB.set_title(f"② 整个 100 GB 写入负载的全部 CPU 去向　合计 {B_TOT:,.0f} 核心·秒\n"
              "默认配置 · 16 写线程 · 96 M 次 1 KB 写 · 写路径实测 90.2 µs/op",
              loc="center", fontsize=9.5, fontweight="bold", pad=8, linespacing=1.5)

fig.text(0.015, 0.925,
         f"同期墙钟：fill 历时 {FILL_WALL:,.0f} s，其中写线程处于限速/停车状态 {STALL_S:,.0f} s"
         f"（≈{STALL_S/FILL_WALL*100:.0f}%）；真正在写数据的时间只占 {B_USEFUL/B_TOT*100:.1f}%。",
         fontsize=8.5, color=TEXT, ha="left")
notes = [
    "数据源：① bench/out/wpbench/wpbench_perf.csv（PerfContext, quiet t1, 2 M ops）；",
    "② bench/out/fillcpu_1kb100_default/run（fill + drain，逐线程 /proc 累计）与 bench/out/wpbench/{wpbench.csv,",
    "WRITEPATH_DECOMPOSITION.md}。口径：① 为 PerfContext 的 leader 墙钟相位（仅引擎内部，不含驱动与写组等待）；② 为线程 CPU 累计。",
]
y = 0.055
for txt in notes:
    fig.text(0.015, y, txt, fontsize=7.0, color=MUTED, ha="left")
    y -= 0.020
fig.suptitle("RocksDB 写流程耗时拆解：单次写仅 4.54 µs，整个负载 95% 的 CPU 不在写数据",
             x=0.015, ha="left", fontsize=11.4, fontweight="bold", y=0.985)

os.makedirs(OUT, exist_ok=True)
for ext in ("png", "svg", "pdf"):
    fig.savefig(f"{OUT}/fig13_write_flow_pie.{ext}")
plt.close(fig)

# ── 审计表 ──
print(f"① 引擎内（PerfContext, quiet t1, n={t1['ops']}）  合计 {A_TOT:.1f} ns")
for n, v, _ in A_ITEMS:
    print(f"   {n:<14s} {v:8.1f} ns  {v/A_TOT*100:5.1f}%")
print(f"② 全流程（100 GB fill+drain）  合计 {B_TOT:,.1f} 核心·秒")
for n, v, _ in B_ITEMS:
    print(f"   {n:<40s} {v:9.1f}   {v/B_TOT*100:5.2f}%")
print(f"   校验：写路径 {B_WRITER:.2f} = 有效 {B_USEFUL:.1f} + 协调 {B_COORD:.1f} + 放大 {B_SPIN:.1f}"
      f"（差 {B_WRITER-(B_USEFUL+B_COORD+B_SPIN):.4f}）")
print(f"   写路径实测 {run['writer_us_op']:.2f} µs/op；地板 {FLOOR_PER_OP} µs/op；有效 {USEFUL_PER_OP} µs/op")
print("written:", sorted(f for f in os.listdir(OUT) if f.startswith("fig13")))
