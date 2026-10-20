#!/usr/bin/env python3
"""100GB 写入流程的写入吞吐率时间线 + 写停顿区间（图 16）。

上：全程吞吐率（0–400 MB/s，原始 250 ms + 10 s 平滑），停顿区间着色
下：放大到 0–60 MB/s，叠加 pending compaction bytes（右轴）看"停在什么条件上"

数据
  * bench/out/pcie_bw/dev100.samples.csv   逐 250ms：ops_cum → 应用层吞吐；pending_comp / l0_files
  * bench/out/pcie_bw/dev100.stalls.csv    EventListener 的 WriteStallInfo 状态迁移（normal/delayed/stopped）
相位
  fill 0–4912.4 s（写线程在跑）；drain 4912.4–5232.9 s（停写等后台排空，吞吐为 0，非停顿）
"""
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patches import Patch
import numpy as np

ROOT = "/home/embed/hyl/metadata_offload"
BW = f"{ROOT}/bench/out/pcie_bw"
OUT = f"{ROOT}/report/charts"

BORDER, ICON = "#c7ced2", "#3f6c83"
HEADER, BLOCK = "#375462", "#5b7887"
ACCENT, TEXT, MUTED = "#a14c2f", "#181a1b", "#848b8e"
SUCCESS, WARNING = "#489762", "#8a7446"
DELAY_BG, STOP_BG, DRAIN_BG = "#f6e3c9", "#f2d5d0", "#e9ebec"

font_manager.fontManager.addfont(f"{ROOT}/report/fonts/NotoSansSC-Regular.ttf")
font_manager.fontManager.addfont(f"{ROOT}/report/fonts/NotoSansSC-Bold.ttf")
plt.rcParams.update({
    "font.family": "Noto Sans SC", "font.size": 9, "axes.unicode_minus": False,
    "figure.facecolor": "white", "axes.facecolor": "white", "axes.edgecolor": BORDER,
    "axes.labelcolor": TEXT, "text.color": TEXT, "xtick.color": MUTED, "ytick.color": MUTED,
    "grid.color": BORDER, "grid.linestyle": (0, (3, 3)), "grid.linewidth": 0.6,
    "grid.alpha": 0.55, "axes.grid": True, "savefig.dpi": 300,
    "savefig.bbox": "tight", "savefig.pad_inches": 0.12,
})

# ── 读数据 ─────────────────────────────────────────────────────────────
rows = list(csv.DictReader(open(f"{BW}/dev100.samples.csv")))
t0 = float(rows[0]["ts"]) - float(rows[0]["elapsed"])
win = []
for a, b in zip(rows, rows[1:]):
    dt = float(b["elapsed"]) - float(a["elapsed"])
    if dt <= 0:
        continue
    win.append((float(b["elapsed"]), (int(b["ops_cum"]) - int(a["ops_cum"])) * 1040 / 1e6 / dt,
                int(b["phase"]), int(b["l0_files"]), int(b["pending_comp"]) / 2**30))
t = np.array([x[0] for x in win])
y = np.array([x[1] for x in win])
phase = np.array([x[2] for x in win])
l0 = np.array([x[3] for x in win])
pend = np.array([x[4] for x in win])
FILL_S = t[phase == 1].max()
ys = np.array([y[max(0, np.searchsorted(t, x - 5)):max(1, np.searchsorted(t, x + 5))].mean() for x in t])

# 停顿区间（wall 时间 → elapsed）
ev = sorted((float(r["ts"]) - t0, r["state"]) for r in csv.DictReader(open(f"{BW}/dev100.stalls.csv")))
ival = {"delayed": [], "stopped": []}
cur = start = None
for tt, s in ev:
    if s == "normal":
        if cur:
            ival[cur].append((start, tt))
            cur = None
    elif cur != s:
        if cur:
            ival[cur].append((start, tt))
        cur, start = s, tt
if cur:
    ival[cur].append((start, t[-1]))
fill_ival = {k: [(a, min(b, FILL_S)) for a, b in v if a < FILL_S] for k, v in ival.items()}

# ── 画布 ───────────────────────────────────────────────────────────────
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(7.8, 6.5), sharex=True,
                               gridspec_kw=dict(height_ratios=[1.0, 1.0], hspace=0.34))


def shade(ax, ymax_frac=1.0):
    for a, b in fill_ival["delayed"]:
        ax.axvspan(a / 60, b / 60, color=DELAY_BG, lw=0, zorder=0)
    for a, b in fill_ival["stopped"]:
        ax.axvspan(a / 60, b / 60, color=STOP_BG, lw=0, zorder=0)
    ax.axvspan(FILL_S / 60, t[-1] / 60, color=DRAIN_BG, lw=0, zorder=0)


def tidy(ax):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.set_axisbelow(True)


# ① 全程
shade(ax1)
ax1.plot(t / 60, y, lw=0.45, color=ACCENT, alpha=0.45, zorder=2, label="原始 250 ms 窗口")
ax1.plot(t / 60, ys, lw=1.7, color=HEADER, zorder=4, label="10 s 平滑")
ax1.axvline(FILL_S / 60, color=MUTED, lw=0.9, ls="--", zorder=5)
ax1.text(FILL_S / 60 + 0.8, 205, "fill 结束 → drain", fontsize=7.6, color=MUTED,
         bbox=dict(facecolor="white", alpha=0.85, edgecolor="none", pad=1.4), zorder=6)
ax1.axhline(ys[phase == 1].mean(), color=HEADER, lw=0.8, ls=":", zorder=3)
ax1.text(45, ys[phase == 1].mean() + 12, f"fill 均值 {ys[phase==1].mean():.1f} MB/s",
         fontsize=8.2, color=HEADER, fontweight="bold",
         bbox=dict(facecolor="white", alpha=0.88, edgecolor="none", pad=1.6), zorder=6)
ax1.annotate("开跑 373 MB/s（10 s 平滑峰值）",
             xy=(1.0, 340), xytext=(9, 330), fontsize=7.8, color=TEXT,
             bbox=dict(facecolor="white", alpha=0.9, edgecolor=BORDER, lw=0.5, pad=2),
             arrowprops=dict(arrowstyle="->", color=MUTED, lw=0.8), zorder=7)
ax1.annotate("2 分钟内塌到 ~15 MB/s：L0 撞 21 文件、pending 冲到 17 GiB",
             xy=(2.2, 30), xytext=(13, 180), fontsize=7.8, color=TEXT,
             bbox=dict(facecolor="white", alpha=0.9, edgecolor=BORDER, lw=0.5, pad=2),
             arrowprops=dict(arrowstyle="->", color=MUTED, lw=0.8), zorder=7)
ax1.set_ylabel("写吞吐率（MB/s，应用层）")
ax1.set_ylim(0, 400)
ax1.legend(handles=[Patch(facecolor=ACCENT, alpha=0.45, label="写吞吐：原始 250 ms 窗口"),
                    Patch(facecolor=HEADER, label="写吞吐：10 s 平滑"),
                    Patch(facecolor=DELAY_BG, label="写停顿·delayed（限速）"),
                    Patch(facecolor=STOP_BG, label="写停顿·stopped（停车）"),
                    Patch(facecolor=DRAIN_BG, label="drain（停写等后台，非停顿）")],
           fontsize=7.6, loc="upper right", ncol=2, frameon=True, facecolor="white",
           framealpha=0.95, edgecolor=BORDER, borderpad=0.5, columnspacing=1.2, handlelength=1.6)
tidy(ax1)
ax1.set_title("① 写入吞吐率全程：开跑即塌，随后长期在 15–20 MB/s 平台反复停顿",
              loc="left", fontsize=10.2, fontweight="bold", pad=8)

# ② 放大 + pending
shade(ax2)
ax2.plot(t / 60, y, lw=0.4, color=ACCENT, alpha=0.35, zorder=2)
ax2.plot(t / 60, ys, lw=1.6, color=HEADER, zorder=4, label="写吞吐（10 s 平滑，左轴）")
ax2.set_ylabel("写吞吐率（MB/s）")
ax2.set_ylim(0, 70)
ax2.set_xlabel("fill 时间（分钟，含末尾 drain）")
ax2.set_xlim(0, t[-1] / 60)
ax2.legend(handles=[Patch(facecolor=ACCENT, alpha=0.35, label="写吞吐：原始 250 ms 窗口"),
                    Patch(facecolor=HEADER, label="写吞吐：10 s 平滑")],
           fontsize=7.8, loc="upper right", frameon=True, facecolor="white",
           framealpha=0.95, edgecolor=BORDER, borderpad=0.5)
ax2.axvline(FILL_S / 60, color=MUTED, lw=0.9, ls="--", zorder=5)
tidy(ax2)
_d = sum(b - a for a, b in fill_ival["delayed"])
_s = sum(b - a for a, b in fill_ival["stopped"])
_pm = np.median(pend[phase == 1])
ax2.set_title(f"② 放大到 0–70 MB/s：fill 期间 {(_d+_s)/FILL_S*100:.0f}% 的时间处于停顿状态"
              f"（delayed {_d/FILL_S*100:.0f}% / stopped {_s/FILL_S*100:.0f}%）\n"
              f"同期 pending compaction bytes 中位 {_pm:.0f} GiB、最大 {pend[phase==1].max():.0f} GiB"
              f"——长期钉在 64 GiB 软限上方，故限速持续不断",
              loc="left", fontsize=9.3, fontweight="bold", pad=7, linespacing=1.5)

fig.suptitle("100GB 写入流程的写入吞吐率与写停顿（fillcpu 默认配置 · 96M×1KB · 16 线程 · 4912 s fill + 320 s drain）",
             x=0.012, ha="left", fontsize=11.0, fontweight="bold", y=0.995)
NOTES = [
    "停顿口径：RocksDB EventListener 的 WriteStallInfo 状态迁移（写入器被限速 delayed / 被停车 stopped），着色为该状态的持续区间；",
    "同一时刻可能多个条件并存，此处按 DB 级状态机逐个区间着色（因此区间不重叠）。drain 段写线程已停，吞吐为 0 不是停顿。",
    "数据：bench/out/pcie_bw/{dev100.samples.csv（逐 250 ms）, dev100.stalls.csv（1344 条状态迁移）}。",
]
_y = -0.045
for _t in NOTES:
    fig.text(0.012, _y, _t, fontsize=7.1, color=MUTED, ha="left")
    _y -= 0.030

os.makedirs(OUT, exist_ok=True)
for ext in ("png", "svg", "pdf"):
    fig.savefig(f"{OUT}/fig16_throughput_stalls.{ext}")
plt.close(fig)

print(f"停顿：delayed {len(fill_ival['delayed'])} 段/{_d:.0f}s（{_d/FILL_S*100:.1f}%），"
      f"stopped {len(fill_ival['stopped'])} 段/{_s:.0f}s（{_s/FILL_S*100:.1f}%），"
      f"合计 {(_d+_s)/FILL_S*100:.1f}% 的 fill 时间")
print(f"吞吐：fill 均值 {ys[phase==1].mean():.1f} MB/s，10 s 平滑峰值 {ys[phase==1].max():.0f}，"
      f"最低 {ys[phase==1].min():.1f} MB/s")
print(f"最长段：delayed {max((b-a for a,b in fill_ival['delayed']), default=0):.0f}s，"
      f"stopped {max((b-a for a,b in fill_ival['stopped']), default=0):.0f}s")
print("written:", sorted(f for f in os.listdir(OUT) if f.startswith("fig16")))
