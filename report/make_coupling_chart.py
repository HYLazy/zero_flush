#!/usr/bin/env python3
"""250 ms 写吞吐 vs 硬盘带宽使用率：是否重叠 / 是否同周期（图 17）。

① 2 分钟放大：两条 250 ms 序列叠加 + flush 完成事件标记（看"不同步"）
② 相关系数随聚合尺度（0.5 s → 120 s）：什么时候才咬合
③ 事件锁定中位数：以 flush / compaction 完成为 0 点，归一化到各自基线

数据：bench/out/pcie_bw/{dev100.samples.csv, dev100.flush_jobs.csv, dev100.comp_jobs.csv}
分析口径：平台段（5–80 min，避开开跑塌陷的非平稳段）；dt 中位 300 ms 且不规则，
故相关系数用 0.5 s 均匀网格重采样后按窗聚合，周期用 Lomb-Scargle/ACF 交叉校验。
"""
import csv
import os

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

# ── 读 250 ms 序列 ─────────────────────────────────────────────────────
rows = list(csv.DictReader(open(f"{BW}/dev100.samples.csv")))
t0 = float(rows[0]["ts"]) - float(rows[0]["elapsed"])
S = lambda r, k: int(r[k])
T, APP, DW, TK = [], [], [], []
for a, b in zip(rows, rows[1:]):
    d = float(b["elapsed"]) - float(a["elapsed"])
    if d <= 0:
        continue
    T.append(float(b["elapsed"]))
    APP.append((S(b, "ops_cum") - S(a, "ops_cum")) * 1040 / 1e6 / d)
    DW.append((S(b, "dev_wr_sectors") - S(a, "dev_wr_sectors")) * 512 / 1e6 / d)
    TK.append((S(b, "dev_io_ticks_ms") - S(a, "dev_io_ticks_ms")) / d / 10.0)
T, APP, DW, TK = map(np.array, (T, APP, DW, TK))
pl = (T > 300) & (T < 4800)                      # 平台段
step = 0.5
grid = np.arange(T[pl][0], T[pl][-1], step)
g = lambda v: np.interp(grid, T[pl], v[pl])
a, d, bk = g(APP), g(DW), g(TK)

# ── ① 事件时间 ────────────────────────────────────────────────────────
def evts(fn, col):
    ts = np.sort([float(r[col]) - t0 for r in csv.DictReader(open(fn))])
    return ts[(ts > 300) & (ts < 4800)]
fe = evts(f"{BW}/dev100.flush_jobs.csv", "ts_end")
ce = evts(f"{BW}/dev100.comp_jobs.csv", "ts_end")

# ── ② 相关系数 vs 聚合尺度 ────────────────────────────────────────────
wins = [0.5, 1, 2, 3, 5, 10, 20, 30, 60, 120]
r_dw, r_tk = [], []
for w in wins:
    k = max(int(w / step), 1)
    n = len(a) // k * k
    A = a[:n].reshape(-1, k).mean(1)
    r_dw.append(np.corrcoef(A, d[:n].reshape(-1, k).mean(1))[0, 1])
    r_tk.append(np.corrcoef(A, bk[:n].reshape(-1, k).mean(1))[0, 1])

# ── ③ 事件锁定中位数（相对基线归一化）─────────────────────────────────
LAG = np.arange(-6, 6.01, 0.5)


def lock(evts_, series):
    out = []
    for L in LAG:
        v = [series[min(max(np.searchsorted(T, e + L) - 1, 0), len(T) - 1)]
             for e in evts_ if T[0] <= e + L <= T[-1]]
        out.append(np.median(v) if v else np.nan)
    out = np.array(out)
    base = np.nanmedian(out[np.abs(LAG) > 3])        # 用远端作基线
    return out / base


lo_fa, lo_fd = lock(fe, APP), lock(fe, DW)
lo_ca, lo_cd = lock(ce, APP), lock(ce, DW)

# ── 画布 ───────────────────────────────────────────────────────────────
fig = plt.figure(figsize=(7.8, 6.3))
gs = fig.add_gridspec(2, 2, height_ratios=[1.0, 1.0], hspace=0.42, wspace=0.30)
axA = fig.add_subplot(gs[0, :])
axB = fig.add_subplot(gs[1, 0])
axC = fig.add_subplot(gs[1, 1])


def tidy(ax):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.set_axisbelow(True)


# ① 2 分钟放大
W0, W1 = 30 * 60, 32 * 60
m = (T >= W0) & (T <= W1)
x = T[m] / 60.0
axA.plot(x, APP[m], lw=1.0, color=ACCENT, label="写吞吐（应用层，左轴）")
axA.set_ylabel("写吞吐（MB/s）", color=ACCENT)
axA.tick_params(axis="y", colors=ACCENT)
axA.set_ylim(0, max(APP[m].max() * 1.55, 10))
axD = axA.twinx()
axD.plot(x, DW[m], lw=1.0, color=BLOCK, label="设备写带宽（右轴）")
axD.set_ylabel("设备写带宽（MB/s）", color=BLOCK)
axD.tick_params(axis="y", colors=BLOCK)
axD.grid(False)
axD.set_ylim(0, max(DW[m].max() * 1.55, 10))
for s in ("top",):
    axD.spines[s].set_visible(False)
nf = ((fe >= W0) & (fe <= W1)).sum()
for e in fe[(fe >= W0) & (fe <= W1)]:
    axA.axvline(e / 60, color=SUCCESS, lw=0.8, alpha=0.55, zorder=1)
axA.set_xlim(W0 / 60, W1 / 60)
axA.set_xlabel("fill 时间（分钟）")
h1, l1 = axA.get_legend_handles_labels()
h2, l2 = axD.get_legend_handles_labels()
h1.append(plt.Line2D([0], [0], color=SUCCESS, lw=1.2))
l1.append(f"flush 完成（本窗 {nf} 次）")
axA.legend(h1 + h2, l1 + l2, fontsize=7.6, loc="upper left", ncol=2, frameon=True,
           facecolor="white", framealpha=0.95, edgecolor=BORDER, borderpad=0.45,
           columnspacing=1.0, handlelength=1.4)
tidy(axA)
axA.set_title("① 250 ms 原始序列（30–32 min 片段）：两条曲线各走各的，"
              "设备侧只在 flush 完成处出现瞬时小峰", loc="left", fontsize=9.8, fontweight="bold", pad=7)

# ② r vs 尺度
axB.semilogx(wins, r_dw, "-o", color=HEADER, lw=1.6, ms=4.5, label="写吞吐 ↔ 设备写带宽")
axB.semilogx(wins, r_tk, "-s", color=WARNING, lw=1.4, ms=4, label="写吞吐 ↔ 设备忙率")
axB.axhline(0, color=BORDER, lw=0.8)
axB.axhline(0.5, color=MUTED, lw=0.8, ls=":")
for w, rv in ((0.5, r_dw[0]), (120, r_dw[-1])):
    axB.annotate(f"{rv:+.2f}", (w, rv), textcoords="offset points", xytext=(0, 8),
                 ha="center", fontsize=8, color=HEADER, fontweight="bold")
axB.text(0.6, 0.53, "r = 0.5", fontsize=7.4, color=MUTED)
axB.set_xlabel("聚合窗宽（s，对数轴）")
axB.set_ylabel("皮尔逊相关系数 r")
axB.set_ylim(-0.05, 1.0)
axB.legend(fontsize=7.6, loc="upper left", frameon=True, facecolor="white",
           framealpha=0.95, edgecolor=BORDER, borderpad=0.45)
tidy(axB)
axB.set_title("② 250 ms 尺度上基本解耦（r=0.18）\n    要到 1–2 分钟才咬合（r=0.90）",
              loc="left", fontsize=9.8, fontweight="bold", pad=7, linespacing=1.5)

# ③ 事件锁定
axC.axhline(1.0, color=BORDER, lw=0.8)
axC.plot(LAG, lo_fd, "-", color=BLOCK, lw=1.8, label="flush 完成 → 设备写")
axC.plot(LAG, lo_fa, "--", color=ACCENT, lw=1.5, label="flush 完成 → 写吞吐")
axC.plot(LAG, lo_ca, "--", color=SUCCESS, lw=1.5, label="compaction 完成 → 写吞吐")
axC.axvline(0, color=MUTED, lw=0.9, ls="--")
_BB = dict(facecolor="white", alpha=0.92, edgecolor="none", pad=1.6)
axC.text(0.6, 1.44, f"设备写 瞬时 ×{lo_fd[LAG == 0][0]:.2f}", fontsize=7.2, color=BLOCK,
         fontweight="bold", bbox=_BB, zorder=7, va="center")
axC.text(0.6, 1.28, f"写吞吐 台阶 ×{np.median(lo_ca[LAG > 0]):.2f}", fontsize=7.2, color=SUCCESS,
         fontweight="bold", bbox=_BB, zorder=7, va="center")
axC.set_xlabel("相对事件完成时刻（s）")
axC.set_ylabel("相对各自基线的倍数")
axC.set_ylim(0.40, 1.96)
axC.legend(fontsize=6.8, loc="lower left", frameon=True, facecolor="white",
           framealpha=0.95, edgecolor=BORDER, borderpad=0.45, labelspacing=0.32)
tidy(axC)
axC.set_title("③ 节奏由事件驱动（非固定周期）：\n     flush 每 4.0 s 一次（中位），compaction 每 1.0 s 一次",
              loc="left", fontsize=9.8, fontweight="bold", pad=7, linespacing=1.5)

fig.suptitle("250 ms 写吞吐波动 与 硬盘带宽使用率：无共同周期，只在 ≥1 分钟尺度上重叠",
             x=0.012, ha="left", fontsize=11.2, fontweight="bold", y=0.995)
NOTES = [
    "口径：平台段（5–80 min）避开开跑塌陷的非平稳段；采样 dt 中位 300 ms 且不规则，故重采样到 0.5 s 均匀网格后聚合。",
    "自相关在 1.5–150 s 内的峰全部 ≤0.15，功率谱峰弱且两条序列几乎不共享 → 两侧都没有可用的固定周期。",
    "数据：bench/out/pcie_bw/{dev100.samples.csv, dev100.flush_jobs.csv, dev100.comp_jobs.csv}；脚本 report/make_coupling_chart.py。",
]
y = -0.045
for _t in NOTES:
    fig.text(0.012, y, _t, fontsize=7.1, color=MUTED, ha="left")
    y -= 0.030

os.makedirs(OUT, exist_ok=True)
for ext in ("png", "svg", "pdf"):
    fig.savefig(f"{OUT}/fig17_throughput_io_coupling.{ext}")
plt.close(fig)

# 自检：两个标注框是否落在坐标区像素范围内
fig.canvas.draw(); r_ = fig.canvas.get_renderer()
axbb = axC.get_window_extent(r_)
for t_ in axC.texts:
    bb = t_.get_window_extent(r_)
    ok = bb.x1 <= axbb.x1 and bb.x0 >= axbb.x0 and bb.y1 <= axbb.y1 and bb.y0 >= axbb.y0
    print(("  [OK] " if ok else "  [越界] ") + f"{t_.get_text()[:24]!r} px x {bb.x0:.0f}-{bb.x1:.0f} vs 坐标区 {axbb.x0:.0f}-{axbb.x1:.0f}")
lg = axC.get_legend().get_window_extent(r_)
print(f"  图例 px x {lg.x0:.0f}-{lg.x1:.0f}, y {lg.y0:.0f}-{lg.y1:.0f}")
for t_ in axC.texts:
    bb = t_.get_window_extent(r_)
    inter = not (bb.x1 < lg.x0 or bb.x0 > lg.x1 or bb.y1 < lg.y0 or bb.y0 > lg.y1)
    if inter: print(f"  [图例重叠] {t_.get_text()[:20]!r}")
_boxes = [t_.get_window_extent(r_) for t_ in axC.texts]
for _i in range(len(_boxes)):
    for _j in range(_i + 1, len(_boxes)):
        a_, b_ = _boxes[_i], _boxes[_j]
        if not (a_.x1 < b_.x0 or a_.x0 > b_.x1 or a_.y1 < b_.y0 or a_.y0 > b_.y1):
            print(f"  [标注框互叠] {_i} vs {_j}")
for t_ in axC.texts:
    bb = t_.get_window_extent(r_)
    inv = axC.transData.inverted()
    (x0, yb), (x1, _) = inv.transform((bb.x0, bb.y0)), inv.transform((bb.x1, bb.y1))
    m_ = (LAG >= x0) & (LAG <= x1)
    cmax = max(float(np.nanmax(v[m_])) for v in (lo_fd, lo_fa, lo_ca)) if m_.any() else 0
    print(("  [OK] " if cmax < yb else "  [压曲线] ") +
          f"框 {t_.get_text()[:16]!r} 底部 y={yb:.2f}，其 x 范围内曲线最高 ×{cmax:.2f}")
_lo = min(float(np.nanmin(lo_fd)), float(np.nanmin(lo_fa)), float(np.nanmin(lo_ca)))
_py = axbb.y0 + (_lo - axC.get_ylim()[0]) / (axC.get_ylim()[1] - axC.get_ylim()[0]) * (axbb.y1 - axbb.y0)
print(("  [OK] " if _py > lg.y1 else "  [图例压曲线] ") +
      f"曲线最低 ×{_lo:.3f} -> 像素 y={_py:.0f} vs 图例顶 {lg.y1:.0f}")
print("r(app,设备写):", " ".join(f"{w}s:{v:+.2f}" for w, v in zip(wins, r_dw)))
print("r(app,忙率)  :", " ".join(f"{w}s:{v:+.2f}" for w, v in zip(wins, r_tk)))
print(f"flush 完成锁定: 设备写 @0s ×{lo_fd[LAG==0][0]:.2f}；写吞吐 @-1..0s ×{lo_fa[(LAG>=-1)&(LAG<=0)].max():.2f}")
print(f"compaction 完成锁定: 写吞吐 @+1..+6s ×{np.median(lo_ca[LAG>0]):.2f}（前置 ×{np.median(lo_ca[LAG<0]):.2f}）")
print("written:", sorted(f for f in os.listdir(OUT) if f.startswith("fig17")))
