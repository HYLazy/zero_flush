#!/usr/bin/env python3
"""100GB 写入流程的使用率折线图（图 15）。

上：硬盘带宽使用率与设备忙率  下：PCIe 链路使用率
数据
  * bench/out/pcie_bw/dev100.samples.csv   逐 250ms：设备 sectors / io_ticks（块层）
  * bench/out/pcie_bw/iio_during_run.txt   逐 5s：uncore_iio_free_running port1（PCIe 计数）
分母（同会话同盘实测天花板；PCie 为链路协商值的 128b/130b 有效上限）
  * 顺序写 910 MB/s、顺序读 1700 MB/s
  * PCIe 3.0 x4 = 8.0 GT/s × 4 lane × 128/130 = 3.938 GB/s（单向）
对齐：perf 比 fillcpu 早启动 15 s（按两序列相关性最大化定标，15–120s 聚合 r=0.78–0.98）。
"""
import csv
import os
import re

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np

ROOT = "/home/embed/hyl/metadata_offload"
BW = f"{ROOT}/bench/out/pcie_bw"
OUT = f"{ROOT}/report/charts"

BORDER, ICON = "#c7ced2", "#3f6c83"
HEADER, BLOCK = "#375462", "#5b7887"
ACCENT, TEXT, MUTED = "#a14c2f", "#181a1b", "#848b8e"
SUCCESS, WARNING = "#489762", "#8a7446"
RUST_L = "#c98a6a"

CEIL_W, CEIL_R = 910.0, 1700.0        # MB/s，本会话实测
LINK = 8.0 * 4 * 128 / 130 / 8 * 1000  # MB/s，PCIe 3.0 x4 单向有效载荷
SMOOTH = 15.0                          # s，滚动平均窗
DELTA = 15.0                           # s，perf 起点相对 fillcpu 起点的提前量

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

# ── 块层：逐 250ms 速率 ─────────────────────────────────────────────────
rows = list(csv.DictReader(open(f"{BW}/dev100.samples.csv")))
S = lambda r, k: int(r[k])
dev = []
for a, b in zip(rows, rows[1:]):
    dt = float(b["elapsed"]) - float(a["elapsed"])
    if dt <= 0:
        continue
    dev.append((float(b["elapsed"]),
                (S(b, "dev_wr_sectors") - S(a, "dev_wr_sectors")) * 512 / 1e6 / dt,
                (S(b, "dev_rd_sectors") - S(a, "dev_rd_sectors")) * 512 / 1e6 / dt,
                (S(b, "dev_io_ticks_ms") - S(a, "dev_io_ticks_ms")) / dt / 10.0))
t_dev = np.array([x[0] for x in dev])
wr = np.array([x[1] for x in dev])
rd = np.array([x[2] for x in dev])
busy = np.array([x[3] for x in dev])
FILL_END = 4912.4 / 60.0        # min


def roll(y, t, win):
    out = np.empty_like(y)
    n = len(t)
    for i in range(n):
        lo, hi = np.searchsorted(t, t[i] - win / 2), np.searchsorted(t, t[i] + win / 2)
        out[i] = y[max(lo, 0):max(hi, 1)].mean()
    return out


wr_s, rd_s, busy_s = roll(wr, t_dev, SMOOTH), roll(rd, t_dev, SMOOTH), roll(busy, t_dev, SMOOTH)

# ── PCIe：逐 5s 计数（perf 时基 + DELTA 对齐到 fillcpu 时基） ────────────
iio = {}
for line in open(f"{BW}/iio_during_run.txt"):
    m = re.match(r"\s*([0-9.]+)\s+([0-9.]+) MiB\s+uncore_iio_free_running/bw_(in|out)_port1/", line)
    if m:
        iio.setdefault(m.group(3), []).append((float(m.group(1)) + DELTA, float(m.group(2)) * 1.048576 / 5.0))
t_iio = np.array([x[0] for x in iio["out"]])
iio_out = np.array([x[1] for x in iio["out"]])
iio_in = np.array([x[1] for x in iio["in"]])
iio_out_s, iio_in_s = roll(iio_out, t_iio, SMOOTH), roll(iio_in, t_iio, SMOOTH)
bl_in_t = np.interp(t_iio, t_dev, wr + rd)     # 块层换算（写+读，校验用）

step = 5.0
iis = np.searchsorted(t_iio, np.arange(0, t_iio[-1], step))
dds = np.searchsorted(t_dev, np.arange(0, t_dev[-1], step))

# ── 画布 ───────────────────────────────────────────────────────────────
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(7.8, 5.6), sharex=True,
                               gridspec_kw=dict(height_ratios=[1.15, 1.0], hspace=0.12))


def tidy(ax):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.set_axisbelow(True)


ddr = np.searchsorted(t_dev, np.arange(0, t_dev[-1], 1.0))
x1r = t_dev[ddr] / 60
x1 = t_dev[dds] / 60
_rawpk = (wr[ddr] / CEIL_W * 100).max()
ax1.plot(x1r, wr[ddr] / CEIL_W * 100, lw=0.45, color=RUST_L, alpha=0.55, zorder=1,
         label=f"写带宽：原始 250 ms 窗口（峰值 {_rawpk:.0f}%）")
ax1.plot(x1, wr_s[dds] / CEIL_W * 100, lw=1.8, color=ACCENT, zorder=4,
         label="写带宽：15 s 平滑 ÷ 顺序写天花板 910 MB/s")
ax1.plot(x1, busy_s[dds], lw=1.3, color=ICON, zorder=3, label="设备忙率（io_ticks：队列非空闲时间占比）")
ax1.plot(x1, rd_s[dds] / CEIL_R * 100, lw=1.3, color=WARNING, zorder=3, label="读带宽：15 s 平滑 ÷ 顺序读天花板 1700 MB/s")
ax1.axvline(FILL_END, color=MUTED, lw=0.9, ls="--")
ax1.text(FILL_END + 1.0, 103, "fill 结束 → drain", fontsize=7.8, color=MUTED)
for y, c, txt in ((wr_s[dds].mean() / CEIL_W * 100, ACCENT, None),
                  (busy_s[dds].mean(), ICON, None)):
    ax1.axhline(y, color=c, lw=0.8, ls=":", alpha=0.8)
_bb = dict(facecolor="white", alpha=0.88, edgecolor="none", pad=1.6)
ax1.text(1.0, wr_s[dds].mean() / CEIL_W * 100 + 2.2, f"写向均值 {wr_s[dds].mean()/CEIL_W*100:.1f}%",
         fontsize=8.2, color=ACCENT, fontweight="bold", bbox=_bb, zorder=6)
ax1.text(1.0, busy_s[dds].mean() + 2.2, f"忙率均值 {busy_s[dds].mean():.1f}%",
         fontsize=8.2, color=ICON, fontweight="bold", bbox=_bb, zorder=6)
ax1.set_ylabel("占各自天花板的百分比（%）")
ax1.set_ylim(0, 134)
ax1.set_xlim(0, t_dev[-1] / 60)
ax1.legend(fontsize=7.9, loc="upper center", ncol=2, frameon=True, facecolor="white",
           framealpha=0.95, edgecolor=BORDER, borderpad=0.5, columnspacing=1.2)
tidy(ax1)
_uw, _ub = wr_s[dds].mean() / CEIL_W * 100, busy_s[dds].mean()
ax1.set_title(f"① 硬盘带宽使用率与设备忙率：写向 {_uw:.1f}%、忙率 {_ub:.1f}%，盘未饱和",
              loc="left", fontsize=10.2, fontweight="bold", pad=8)

x2 = t_iio[iis] / 60
ax2.plot(x2, (iio_out_s + iio_in_s)[iis] / LINK * 100, lw=1.6, color=HEADER,
         label="PCIe 双向 ÷ 链路 3.94 GB/s（IIO 计数器实测）")
ax2.plot(x2, iio_out_s[iis] / LINK * 100, lw=1.3, color=BLOCK, label="PCIe 写向（IIO 实测）")
ax2.plot(x2, iio_in_s[iis] / LINK * 100, lw=1.2, color=RUST_L, label="PCIe 读向（IIO 实测）")
ax2.plot(t_iio[iis] / 60, bl_in_t[iis] / LINK * 100, lw=0.9, ls="--", color=SUCCESS,
         label="写向的块层换算（独立仪器，r=0.96）")
ax2.axvline(FILL_END, color=MUTED, lw=0.9, ls="--")
tot = (iio_out_s + iio_in_s)[iis].mean()
ax2.axhline(tot / LINK * 100, color=HEADER, lw=0.8, ls=":")
ax2.text(1.0, tot / LINK * 100 + 0.7, f"双向均值 {tot/LINK*100:.2f}%   15 s 平滑峰值 {(iio_out_s+iio_in_s)[iis].max()/LINK*100:.1f}%",
         fontsize=8.2, color=HEADER, fontweight="bold", bbox=_bb, zorder=6)
ax2.set_ylabel("占 PCIe 3.0 x4 链路的百分比（%）")
ax2.set_xlabel("fill 时间（分钟，含末尾 drain）")
ax2.set_ylim(0, 25)
ax2.legend(fontsize=7.9, loc="upper left", ncol=2, frameon=True, facecolor="white",
           framealpha=0.95, edgecolor=BORDER, borderpad=0.5, columnspacing=1.2)
tidy(ax2)
_ow, _ir = iio_out_s[iis].mean() / LINK * 100, iio_in_s[iis].mean() / LINK * 100
ax2.set_title(f"② PCIe 链路使用率：写向 {_ow:.1f}%、读向 {_ir:.1f}%，双向不足 6%",
              loc="left", fontsize=10.2, fontweight="bold", pad=8)

fig.suptitle("100GB 写入流程的使用率时间线（fillcpu 默认配置 · 96M×1KB · 16 线程 · 87 分钟）",
             x=0.012, ha="left", fontsize=11.2, fontweight="bold", y=0.995)
NOTES = [
    "分母均为同会话、机器空闲时在同盘实测的天花板：顺序写 910 MB/s、顺序读 1700 MB/s；PCIe 为协商链路",
    "PCIe 3.0 x4（8.0 GT/s×4）扣 128b/130b 后的单向有效上限 3.938 GB/s。①② 主曲线均为 15 s 滚动平均；",
    "① 另绘原始 250 ms 序列（细淡线）以显示突发——其峰值超过持续天花板，由盘的 SLC 缓存吸收。",
    "数据：bench/out/pcie_bw/{dev100.samples.csv（逐 250 ms 块层）, iio_during_run.txt（逐 5 s PCIe）}。",
    "IIO 计数器经孤立实验归因：写 6 GiB → out_port1 909 MiB/s，读 6 GiB → in_port1 1731 MiB/s。",
]
_y = -0.05
for _t in NOTES:
    fig.text(0.012, _y, _t, fontsize=7.1, color=MUTED, ha="left")
    _y -= 0.032

os.makedirs(OUT, exist_ok=True)
for ext in ("png", "svg", "pdf"):
    fig.savefig(f"{OUT}/fig15_util_timeline.{ext}")
plt.close(fig)

print(f"硬盘：写均值 {wr_s[dds].mean():.1f} MB/s = {wr_s[dds].mean()/CEIL_W*100:.1f}%  忙率均值 {busy_s[dds].mean():.1f}%  "
      f"读均值 {rd_s[dds].mean():.1f} MB/s = {rd_s[dds].mean()/CEIL_R*100:.2f}%")
print(f"PCIe：写向均值 {iio_out_s[iis].mean():.0f} MB/s = {iio_out_s[iis].mean()/LINK*100:.2f}%  "
      f"读向 {iio_in_s[iis].mean():.0f} = {iio_in_s[iis].mean()/LINK*100:.2f}%  "
      f"双向 {(iio_out_s+iio_in_s)[iis].mean()/LINK*100:.2f}%  峰值 {(iio_out_s+iio_in_s)[iis].max()/LINK*100:.1f}%")
print("written:", sorted(f for f in os.listdir(OUT) if f.startswith("fig15")))
