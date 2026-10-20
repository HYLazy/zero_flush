#!/usr/bin/env python3
"""Generate all charts for the RocksDB write-performance PDF report."""
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.patches import FancyBboxPatch
import numpy as np

ROOT = "/home/embed/hyl/metadata_offload"
OUT = f"{ROOT}/report/charts"
D = json.load(open(f"{ROOT}/report/data/data.json"))
R = D["runs"]

# ── palette (pdf.py palette.cascade, mode=minimal, intent=cold) ──
PAGE_BG, SECTION_BG, CARD_BG, STRIPE = "#f2f3f3", "#eaeced", "#e5e7e8", "#ebedef"
HEADER, BLOCK, BORDER, ICON = "#375462", "#5b7887", "#c7ced2", "#3f6c83"
ACCENT, ACCENT2 = "#a14c2f", "#5abb40"
TEXT, MUTED = "#181a1b", "#848b8e"
SUCCESS, WARNING, ERROR, INFO = "#489762", "#8a7446", "#a14c44", "#4b6884"
SERIES = {"bg2_arch": BORDER, "bg2": HEADER, "bg8": ACCENT}
LABEL = {"bg2_arch": "bg2·归档(load 2-3)", "bg2": "bg2·匹配(load≈3)", "bg8": "bg8(load 5-13)"}

font_manager.fontManager.addfont(f"{ROOT}/report/fonts/NotoSansSC-Regular.ttf")
font_manager.fontManager.addfont(f"{ROOT}/report/fonts/NotoSansSC-Bold.ttf")
plt.rcParams.update({
    "font.family": "Noto Sans SC", "font.size": 9, "axes.unicode_minus": False,
    "figure.facecolor": "white", "axes.facecolor": "white",
    "axes.edgecolor": BORDER, "axes.labelcolor": TEXT, "text.color": TEXT,
    "xtick.color": MUTED, "ytick.color": MUTED, "axes.titlecolor": TEXT,
    "grid.color": BORDER, "grid.linestyle": (0, (3, 3)), "grid.linewidth": 0.6,
    "grid.alpha": 0.55, "axes.grid": True, "savefig.dpi": 200,
    "savefig.bbox": "tight", "savefig.pad_inches": 0.14,
})


def tidy(ax, grid_axis="y"):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.spines["left"].set_color(BORDER)
    ax.spines["bottom"].set_color(BORDER)
    ax.set_axisbelow(True)
    ax.grid(axis=grid_axis)


def title(ax, t, sub=None):
    ax.set_title(t, loc="left", fontsize=11.5, fontweight="bold", pad=14 if sub else 8)
    if sub:
        ax.text(0, 1.02, sub, transform=ax.transAxes, fontsize=8.5, color=MUTED, va="bottom")


# ── fig1: 10s 写速率时间线 ─────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(7.4, 3.3))
for k in ("bg2", "bg8"):
    tl = R[k]["timeline"]
    x = [r[0] / 60 for r in tl]
    y = [min(r[1], 260) for r in tl]
    ax.plot(x, y, lw=1.0, color=SERIES[k], label=LABEL[k], alpha=0.9)
ax.axhline(16.5, color=ERROR, lw=1.2, ls="--")
ax.text(1.6, 20, "默认限速上限 16 MB/s（delayed_write_rate）", color=ERROR, fontsize=8,
        bbox=dict(facecolor="white", alpha=0.9, edgecolor="none", pad=1.5), zorder=5)
ax.set_xlabel("fill 时间（分钟）"); ax.set_ylabel("写速率（MB/s，10 s 窗口）")
ax.set_ylim(0, 270); ax.set_xlim(0, 70)
ax.annotate("爆发窗口可达 200–620 MB/s（图中截断到 260）", xy=(11.2, 166), xytext=(24, 232),
            fontsize=8, color=TEXT, zorder=6,
            bbox=dict(facecolor="white", alpha=0.92, edgecolor=BORDER, lw=0.5, pad=2.5),
            arrowprops=dict(arrowstyle="->", color=MUTED, lw=0.8))
ax.legend(frameon=False, loc="upper right", fontsize=8.5, bbox_to_anchor=(1.0, 1.0))
tidy(ax); title(ax, "图 1　100GB fill 的写速率时间线：限速涓流 + 周期性泄洪",
                "96M×1KB；bg2=默认 1+1 后台线程，bg8=2 flush + 6 compaction 线程")
fig.tight_layout(); fig.savefig(f"{OUT}/fig1_timeline.png"); plt.close(fig)

# ── fig2: 墙钟与吞吐柱状 ───────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(7.4, 2.9))
keys = ["bg2_arch", "bg2", "bg8"]
ys = np.arange(len(keys))[::-1]
ax = axes[0]
vals = [R[k]["fill_wall_s"] for k in keys]
bars = ax.barh(ys, vals, height=0.55, color=[SERIES[k] for k in keys])
for y, v in zip(ys, vals):
    ax.text(v + 60, y, f"{v:,.0f} s（{v/60:.0f} min）", va="center", fontsize=8.5)
ax.set_yticks(ys); ax.set_yticklabels([LABEL[k] for k in keys], fontsize=8.5)
ax.set_xlim(0, 5600); ax.set_xlabel("fill 墙钟（s）"); tidy(ax, "x")
title(ax, "fill 墙钟")
ax = axes[1]
vals = [R[k]["fill_mbps"] for k in keys]
ax.barh(ys, vals, height=0.55, color=[SERIES[k] for k in keys])
for y, v in zip(ys, vals):
    ax.text(v + 0.6, y, f"{v:.1f} MB/s", va="center", fontsize=8.5)
ax.set_yticks(ys); ax.set_yticklabels([]); ax.set_xlim(0, 38)
ax.set_xlabel("fill 吞吐（MB/s）"); tidy(ax, "x")
title(ax, "fill 吞吐")
fig.suptitle("图 2　100GB fill 的墙钟与吞吐：bg8 比匹配基线快 24%",
             x=0.012, ha="left", fontsize=11, fontweight="bold", y=1.06)
fig.tight_layout(); fig.savefig(f"{OUT}/fig2_wall_throughput.png"); plt.close(fig)

# ── fig3: 限速时间/数据占比 ────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(7.4, 2.5))
keys = ["bg2", "bg8"]
y = [1, 0]
for i, k in enumerate(keys):
    t = R[k]["throttle"]
    ax.barh(y[i], t["wall_share"] * 100, height=0.42, color=ERROR, alpha=0.85)
    ax.barh(y[i] - 0.0, t["data_share"] * 100, height=0.42, color=SUCCESS, alpha=0.85)
    ax.text(t["wall_share"] * 100 / 2, y[i] + 0.06, f"时间 {t['wall_share']*100:.1f}%",
            ha="center", va="bottom", fontsize=8, color="white", fontweight="bold")
    ax.text(t["data_share"] * 100 + 1.5, y[i] - 0.02, f"数据 {t['data_share']*100:.1f}%",
            va="center", fontsize=8.5, color=TEXT)
ax.set_yticks(y); ax.set_yticklabels([LABEL[k] for k in keys], fontsize=9)
ax.set_xlim(0, 100); ax.set_xlabel("占 fill 的比例（%）")
ax.text(76, 1.28, "慢区间平均 9.2 MB/s", fontsize=8, color=MUTED)
ax.text(76, 0.28, "慢区间平均 5.8 MB/s", fontsize=8, color=MUTED)
tidy(ax, "x")
title(ax, "图 3　限速区间（≤16.5 MB/s）的时间与数据占比",
      "红色=被限速的时间占比，绿色=在这段时间内写掉的数据占比")
fig.tight_layout(); fig.savefig(f"{OUT}/fig3_throttle_split.png"); plt.close(fig)

# ── fig4: CPU 归因 ───────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(7.4, 2.6))
roles = [("writer", "前台写路径"), ("compaction_pool", "compaction"), ("flush_pool", "flush"),
         ("driver", "驱动/采样")]
for i, k in enumerate(["bg2", "bg8"]):
    left = 0
    for key, name in roles:
        v = (R[k]["cpu_fill"][key] + R[k]["cpu_drain"][key]) / 1000
        ax.barh(1 - i, v, left=left, height=0.42, color=dict(writer=HEADER, compaction_pool=BLOCK,
                flush_pool=ICON, driver=BORDER)[key], edgecolor="white", lw=0.6)
        if v > 0.35:
            ax.text(left + v / 2, 1 - i, f"{name}\n{v:.1f}k ({v/((R[k]['cpu_fill_total']+R[k]['cpu_drain_total'])/1000)*100:.0f}%)",
                    ha="center", va="center", fontsize=7.5,
                    color="white" if key != "driver" else TEXT)
        left += v
    ax.text(left + 0.15, 1 - i, f"总 {left:.2f}k 核心·秒", va="center", fontsize=8.5)
ax.set_yticks([1, 0]); ax.set_yticklabels([LABEL["bg2"], LABEL["bg8"]], fontsize=9)
ax.set_xlim(0, 13.2); ax.set_xlabel("fill + drain 的 CPU（千核心·秒）")
tidy(ax, "x")
title(ax, "图 4　CPU 归因：写路径占近 7 成，flush 仅约 2%",
      "逐线程 /proc 累计口径；bg8 的写路径 CPU 随墙钟缩短而降 9%")
fig.tight_layout(); fig.savefig(f"{OUT}/fig4_cpu_attribution.png"); plt.close(fig)

# ── fig5: 写路径并发曲线（quiet, wpbench） ────────────────────────────
sweep = [r for r in D["wpbench"]["wpbench_sweep.csv"]]
thr = [int(r["threads"]) for r in sweep]
mbps = [float(r["mbps"]) for r in sweep]
us = [float(r["cpu_us_per_op"]) for r in sweep]
lat = [float(r["wall_s"]) / (int(r["ops"]) / int(r["threads"])) * 1e6 for r in sweep]
fig, ax = plt.subplots(figsize=(7.4, 3.0))
ax.bar(range(len(thr)), mbps, width=0.5, color=HEADER, alpha=0.9, label="吞吐（MB/s，左轴）")
for i, v in enumerate(mbps):
    if v > 120:
        ax.text(i, v - 22, f"{v:.0f}", ha="center", va="top", fontsize=8.5, color="white", zorder=5)
    else:
        ax.text(i, v + 8, f"{v:.0f}", ha="center", fontsize=8.5, zorder=5)
ax.set_xticks(range(len(thr))); ax.set_xticklabels([f"{t} 线程" for t in thr])
ax.set_ylabel("吞吐（MB/s）"); ax.set_ylim(0, 560)
ax2 = ax.twinx()
ax2.plot(range(len(thr)), us, "-o", color=ACCENT, lw=2, ms=5, label="CPU/op（µs，右轴）")
ax2.plot(range(len(thr)), lat, "--s", color=WARNING, lw=1.4, ms=4, label="每 op 延迟（µs，右轴）")
for i, v in enumerate(us):
    ax2.annotate(f"{v:.1f}", (i, v), textcoords="offset points", xytext=(0, 8), ha="center",
                 fontsize=8, color=ACCENT, zorder=6,
                 bbox=dict(facecolor="white", alpha=0.9, edgecolor="none", pad=1.0))
ax2.set_ylabel("µs / op"); ax2.set_ylim(0, 290); ax2.grid(False)
for s in ("top",):
    ax2.spines[s].set_visible(False)
h1, l1 = ax.get_legend_handles_labels(); h2, l2 = ax2.get_legend_handles_labels()
ax.legend(h1 + h2, l1 + l2, frameon=False, loc="upper left", fontsize=8)
tidy(ax)
title(ax, "图 5　写线程数扫描：吞吐 3.3× 封顶，CPU/op 与延迟持续上升",
      "1KB Put、无后台干扰（quiet）、2M ops；32 线程因超出物理核数而塌陷")
fig.tight_layout(); fig.savefig(f"{OUT}/fig5_write_scaling.png"); plt.close(fig)

# ── fig6: pending compaction bytes + L0 ──────────────────────────────
fig, ax = plt.subplots(figsize=(7.4, 3.0))
for k in ("bg2", "bg8"):
    tl = R[k]["timeline"]
    ax.plot([r[0] / 60 for r in tl], [r[2] for r in tl], lw=1.2, color=SERIES[k], label=LABEL[k] + " pending")
ax.axhline(64, color=ERROR, ls="--", lw=1.2)
ax.text(1.5, 66.5, "soft_pending_compaction_bytes_limit = 64 GB → 触发限速", color=ERROR, fontsize=8,
        bbox=dict(facecolor="white", alpha=0.9, edgecolor="none", pad=1.5), zorder=6)
ax.set_ylim(0, 86)
ax.set_xlabel("fill 时间（分钟）"); ax.set_ylabel("待压缩字节（GB）"); ax.set_xlim(0, 70); ax.set_ylim(0, 80)
ax3 = ax.twinx()
for k in ("bg2", "bg8"):
    tl = R[k]["timeline"]
    ax3.plot([r[0] / 60 for r in tl], [r[3] for r in tl], lw=0.9, ls=":", color=SERIES[k], alpha=0.75)
ax3.set_ylabel("L0 文件数（点线）", color=MUTED); ax3.set_ylim(0, 40); ax3.grid(False)
ax.legend(frameon=False, loc="lower left", fontsize=8)
tidy(ax)
title(ax, "图 6　compaction 积压（pending bytes）长期压在 64 GB 软限之上",
      "积压在线上时写路径被限速；bg8 的 6 个 compaction 线程把积压更快压回线下")
fig.tight_layout(); fig.savefig(f"{OUT}/fig6_pending.png"); plt.close(fig)

# ── fig7: 内存构成 ───────────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(7.4, 2.7))
parts = [("memtbl", "memtable", HEADER), ("bcache", "block cache", ICON),
         ("treaders", "table readers", BLOCK), ("other", "其他(分配器/迭代器)", BORDER)]
for ax, (k, lab) in zip(axes, (("bg2", LABEL["bg2"]), ("bg8", LABEL["bg8"]))):
    m = R[k]["mem_fillend"]
    left = 0
    for key, name, col in parts:
        v = m[key]
        ax.barh(0, v, left=left, height=0.4, color=col, edgecolor="white", lw=0.5)
        left += v
    ax.barh(-0.55, R[k]["mem"]["rss_peak"], height=0.4, color=ACCENT, alpha=0.85)
    ax.set_xlim(0, 1700); ax.set_ylim(-1.05, 0.45)
    ax.set_yticks([0, -0.55]); ax.set_yticklabels(["fill 末构成", "峰值"], fontsize=8.5)
    ax.set_xlabel("MB"); tidy(ax, "x"); ax.set_title(lab, loc="left", fontsize=10, fontweight="bold")
    ax.text(R[k]["mem"]["rss_peak"] * 0.5, -0.86, f"峰值 RSS {R[k]['mem']['rss_peak']:.0f} MB",
            ha="center", va="center", fontsize=8, color=ACCENT)
_h7 = [plt.Rectangle((0, 0), 1, 1, color=c) for _, _, c in parts]
fig.legend(_h7, [n for _, n, _ in parts], frameon=False, fontsize=8, ncol=4,
           loc="lower center", bbox_to_anchor=(0.5, -0.06), handlelength=1.2)
fig.suptitle("图 7　内存构成：table readers 是主体，bg8 因 6 路并发 compaction 峰值多 39%",
             x=0.012, ha="left", fontsize=11, fontweight="bold", y=1.10)
fig.tight_layout(); fig.savefig(f"{OUT}/fig7_memory.png"); plt.close(fig)

# ── fig8: 线程状态 ───────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(7.4, 2.4))
order = ["R", "S", "D"]
colmap = {"R": SUCCESS, "S": INFO, "D": WARNING}
lab = {"R": "R 运行", "S": "S 阻塞/睡", "D": "D IO 等待"}
rows = [("bg2", "comp"), ("bg8", "comp"), ("bg2", "flush"), ("bg8", "flush"),
        ("bg2", "writer"), ("bg8", "writer")]
ylabels = []
for i, (k, role) in enumerate(rows):
    d = R[k]["states"].get(role, {})
    tot = sum(d.values()) or 1
    left = 0
    for s in order:
        v = d.get(s, 0) / tot * 100
        ax.barh(len(rows) - 1 - i, v, left=left, height=0.62, color=colmap[s], edgecolor="white", lw=0.5)
        if v >= 7:
            ax.text(left + v / 2, len(rows) - 1 - i, f"{v:.0f}%", ha="center", va="center",
                    fontsize=7.5, color="white")
        left += v
    ylabels.append(f"{k}·{'flush' if role=='flush' else 'compaction' if role=='comp' else 'writer'}")
ax.set_yticks(range(len(rows))[::-1]); ax.set_yticklabels(ylabels, fontsize=8.5)
ax.set_xlim(0, 100); ax.set_xlabel("采样点占比（%）")
handles = [plt.Rectangle((0, 0), 1, 1, color=colmap[s]) for s in order]
ax.legend(handles, [lab[s] for s in order], frameon=False, ncol=3, loc="upper center", fontsize=8,
          bbox_to_anchor=(0.5, 1.0), handlelength=1.1)
ax.set_xlabel("采样点占比（%）", labelpad=4)
ax.set_ylim(-0.7, len(rows) + 0.95)
tidy(ax, "x")
title(ax, "图 8　线程状态构成：bg8 的 compaction 线程 65% 时间在等磁盘（D）",
      "R=运行/自旋、S=睡眠/阻塞、D=不可中断 IO 等待；写线程以睡眠为主")
fig.tight_layout(); fig.savefig(f"{OUT}/fig8_states.png"); plt.close(fig)

# ── fig9: stall 原因 ─────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(7.4, 2.6))
reasons = [("pending-compaction-bytes-delays", "限速：compaction 积压 ≥64GB"),
           ("memtable-limit-stops", "停车：memtable 槽位耗尽（等 flush）"),
           ("l0-file-count-limit-delays", "限速：L0 文件数 ≥20")]
keys = ["bg2", "bg8"]
h = 0.34
for i, (key, label) in enumerate(reasons):
    base = i
    for j, k in enumerate(keys):
        v = R[k]["stall_reasons"].get(key, 0)
        ax.barh(base + (0.19 if j == 0 else -0.19), v, height=h, color=SERIES[k],
                label=LABEL[k] if i == 0 else None)
        ax.text(v + 60, base + (0.19 if j == 0 else -0.19), f"{v:,}", va="center", fontsize=8,
                bbox=dict(facecolor="white", alpha=0.85, edgecolor="none", pad=1.0), zorder=5)
ax.set_yticks(range(len(reasons))); ax.set_yticklabels([r[1] for r in reasons], fontsize=8.5)
ax.set_xlim(0, 5600); ax.set_xlabel("事件计数（cfstats）")
ax.legend(frameon=False, loc="upper center", fontsize=8, ncol=2, bbox_to_anchor=(0.5, -0.26),
          handlelength=1.1)
ax.set_xlabel("事件计数（cfstats）", labelpad=22)
tidy(ax, "x")
title(ax, "图 9　stall 原因：bg8 把积压限速事件砍掉 45%\n但 memtable 停车增加 33%",
      "5000+ 次限速/停车事件说明写路径大部分时间处于被节流状态")
fig.tight_layout(); fig.savefig(f"{OUT}/fig9_stall.png"); plt.close(fig)

# ── fig10: perf 剖面 ─────────────────────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(7.6, 3.1))
roles = [("flush", "flush 线程（2 线程）"), ("comp", "compaction 线程（6 线程）"),
         ("writer", "writer 线程")]
SHORT = {
    "__memmove_evex_unaligned_erms": "memmove（1KB 值拷贝）",
    "rocksdb::MemTableIterator::key() const": "MemTableIterator::key",
    "rocksdb::BlockBuilder::AddWithLastKeyImpl(rocksdb::Slice const&, rocksdb::Slice const&, rocksdb::Slice const&, rocksdb::Slice const*, bool, unsigned long)": "BlockBuilder::Add",
    "rocksdb::CompactionIterator::NextFromInput()": "CompactionIterator::Next",
    "XXH3_hashLong_64b_default.constprop.0": "XXH3 块校验",
    "rocksdb::CompactionMergingIterator::Next()": "MergingIterator::Next",
    "rocksdb::WriteThread::AwaitState(rocksdb::WriteThread::Writer*, unsigned char, rocksdb::WriteThread::AdaptationContext*)": "WriteThread::AwaitState（自旋）",
    "__sched_yield": "__sched_yield",
    "rocksdb::clock_cache::(anonymous namespace)::TryInsert(rocksdb::clock_cache::ClockHandleBasicData const&, rocksdb::BlockContents const&, bool, rocksdb::Cache::Priority, unsigned long, rocksdb::CacheItemHelper const*, rocksdb::Cache::CreateCallback const&, rocksdb::CompressionType, rocksdb::MemoryAllocator*)": "clock_cache::TryInsert",
    "__memcmp_evex_movbe": "memcmp",
    "__tls_get_addr": "__tls_get_addr",
}


def short(sym):
    if sym in SHORT:
        return SHORT[sym]
    if sym.startswith("0x") or "ffffffff" in sym:
        return "内核帧（写回/调度）"
    if sym == "0000000000000000":
        return "内核帧"
    return sym[:34]


for ax, (role, t) in zip(axes, roles):
    b = D["perf"]["bg8"][role]
    names = [short(s) for _, s in b][:6]
    vals = [v for v, _ in b][:6]
    ax.barh(range(len(vals))[::-1], vals, height=0.55, color=[ACCENT if i == 0 else BLOCK for i in range(len(vals))])
    for y, v in zip(range(len(vals))[::-1], vals):
        ax.text(v + 0.3, y, f"{v:.1f}%", va="center", fontsize=8)
    ax.set_yticks(range(len(vals))[::-1]); ax.set_yticklabels(names, fontsize=8)
    ax.set_xlim(0, max(vals) * 1.35); ax.set_xlabel("% 采样"); tidy(ax, "x")
    ax.set_title(t, loc="left", fontsize=9.5, fontweight="bold")
fig.suptitle("图 10　perf 函数级剖面（bg8，fill 期间 6 轮 × 20 s 采样）\n"
             "flush / compaction 在搬数据与算校验，writer 在自旋等待",
             x=0.012, ha="left", fontsize=10.5, fontweight="bold", y=1.10)
fig.tight_layout(); fig.savefig(f"{OUT}/fig10_perf.png"); plt.close(fig)

# ── fig11: 写路径组件拆解 + 优化开关（重排：单条堆叠 + 图例在轴内下方） ──
fig, axes = plt.subplots(1, 2, figsize=(7.4, 2.6), gridspec_kw={"width_ratios": [1.05, 1.0]})
ax = axes[0]
comp = [("memtable 插入 + 批次 3.30", 3.30, HEADER), ("WAL 1.10", 1.10, BLOCK),
        ("驱动/校验 1.36", 1.36, BORDER)]
left = 0
for name, v, col in comp:
    ax.barh(0, v, left=left, height=0.42, color=col, edgecolor="white", lw=0.6)
    if v > 2.0:
        ax.text(left + v / 2, 0, f"{v:.2f} µs", ha="center", va="center", fontsize=8.5, color="white")
    left += v
handles = [plt.Rectangle((0, 0), 1, 1, color=c) for _, _, c in comp]
ax.legend(handles, [n for n, _, _ in comp], frameon=False, fontsize=6.8, ncol=1,
          loc="lower left", bbox_to_anchor=(0.0, 0.04), handlelength=1.0, labelspacing=0.35)
ax.set_yticks([0]); ax.set_yticklabels(["单线程 Put"], fontsize=8.5)
ax.set_xlim(0, 6.6); ax.set_ylim(-0.9, 0.42)
ax.set_xlabel("µs / 1KB Put", labelpad=3); tidy(ax, "x")
title(ax, "单线程固有成本 5.76 µs/KV")
ax = axes[1]
rows = [("默认（自旋等待）", 31.1), ("阻塞等待", 59.9), ("流水线写", 26.3)]
ys = np.arange(len(rows))[::-1]
ax.barh(ys, [v for _, v in rows], height=0.5, color=[ACCENT, WARNING, SUCCESS])
for y, (n, v) in zip(ys, rows):
    ax.text(v + 0.9, y, f"{v:.1f}", va="center", fontsize=8.5)
ax.axvline(5.76, color=MUTED, ls=":", lw=1.2)
ax.text(8.5, 0.72, "单线程基线 5.76 µs", fontsize=7.6, color=MUTED,
        bbox=dict(facecolor="white", alpha=0.9, edgecolor="none", pad=1.0), zorder=6)
ax.set_yticks(ys); ax.set_yticklabels([n for n, _ in rows], fontsize=8.5)
ax.set_xlim(0, 72); ax.set_ylim(-0.62, 2.72)
ax.set_xlabel("写路径 CPU（µs/op，16 线程）", labelpad=3); tidy(ax, "x")
title(ax, "16 线程的等待策略对比")
fig.suptitle("图 11　写路径开销拆解（wpbench，quiet）：有效功 5.76 µs（memtable 3.30 + WAL 1.10 + 驱动 1.36，浅色为 wal=0 对照 4.67）",
             x=0.012, ha="left", fontsize=10.5, fontweight="bold", y=1.04)
fig.tight_layout(); fig.savefig(f"{OUT}/fig11_writepath.png"); plt.close(fig)

# ── fig12: 后台线程配置扫描 ──────────────────────────────────────────
sw_all = [r for r in D["bg_sweep"]]
sw = [r for r in sw_all if not r["cfg"].endswith("32GB")]
order = ["bg2_sub1", "bg2_sub4", "bg4_sub1", "bg8_sub1", "bg8_sub4", "bg16_sub4"]
sw = sorted(sw, key=lambda r: order.index(r["cfg"]) if r["cfg"] in order else 99)
names = {"bg2_sub1": "默认 1+1", "bg2_sub4": "1+1 + subcomp4", "bg4_sub1": "3 compaction",
         "bg8_sub1": "6 compaction", "bg8_sub4": "6 compaction + subcomp4",
         "bg16_sub4": "14 compaction + subcomp4"}
fig, ax = plt.subplots(figsize=(7.4, 2.8))
ys = np.arange(len(sw))[::-1]
cols = [ACCENT if r["cfg"] == "bg2_sub1" else (SUCCESS if r["cfg"] in ("bg4_sub1", "bg8_sub1") else WARNING) for r in sw]
ax.barh(ys, [r["mbps"] for r in sw], height=0.55, color=cols)
for y, r in zip(ys, sw):
    ax.text(r["mbps"] + 2, y, f"{r['mbps']} MB/s", va="center", fontsize=8)
ax.set_yticks(ys); ax.set_yticklabels([names.get(r["cfg"], r["cfg"]) for r in sw], fontsize=8.5)
ax.set_xlim(0, 240); ax.set_ylim(-1.5, len(sw) - 0.35); ax.set_xlabel("fill 吞吐（MB/s，8.3GB / 16 线程 / 默认配置）")
tidy(ax, "x")
title(ax, "图 12　后台线程配置扫描\n加 compaction 线程有用但饱和，subcompaction 反而有害",
      "32GB 规模：默认 42 MB/s → 6 线程 66 MB/s（+57%）")
fig.tight_layout(); fig.savefig(f"{OUT}/fig12_bgthreads.png"); plt.close(fig)

print("charts written:", sorted(os.listdir(OUT)))
