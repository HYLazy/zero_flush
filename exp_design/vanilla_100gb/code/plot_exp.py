#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""plot_exp.py - 原版 RocksDB 100GB fillrandom 实验出图与报告。

输入：run_dir/metrics.json（parse_exp.py 产物）
输出：run_dir/charts/{wa_breakdown,stalls_timeline,compaction_map,
                      bandwidth_timeline}.png + report.md

用法：python3 plot_exp.py <run_dir>
"""

import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

plt.rcParams["font.family"] = ["Noto Sans CJK JP", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False

GB = 2.0 ** 30
MB = 2.0 ** 20


def _load(run_dir):
    with open(os.path.join(run_dir, "metrics.json"), encoding="utf-8") as f:
        return json.load(f)


def _hours(t_s):
    return t_s / 3600.0


def _hms(sec):
    if sec is None:
        return "N/A"
    sec = int(sec)
    return f"{sec // 3600}h{sec % 3600 // 60:02d}m{sec % 60:02d}s"


def plot_wa(m, out_dir):
    wa = m["wa"]
    user_gb = wa["user_gb"] or 0
    wal_gb = (wa["wal_bytes"] or 0) / GB
    flush_gb = (wa["flush_bytes"] or 0) / GB
    pb = wa["per_pair_bytes"] or {}
    pairs = sorted(pb, key=lambda k: -pb[k])
    comp_gb = [pb[k] / GB for k in pairs]

    fig, (ax1, ax2) = plt.subplots(
        1, 2, figsize=(11, max(4.5, 1.6 + 0.42 * len(pairs))),
        gridspec_kw={"width_ratios": [1.25, 1]})
    segs = [("WAL", wal_gb), ("Flush", flush_gb)] + [
        (f"Comp {k}", v) for k, v in zip(pairs, comp_gb)]
    left = 0.0
    cmap = plt.get_cmap("tab20")
    for i, (name, v) in enumerate(segs):
        ax1.barh([0], [v], left=[left], color=cmap(i % 20),
                 label=f"{name} ({v:.1f}GB, WA={v / user_gb:.2f})"
                 if user_gb else f"{name} ({v:.1f}GB)")
        left += v
    ax1.axvline(user_gb, color="k", ls="--", lw=1.2)
    ax1.text(user_gb, -0.4, f"用户数据 {user_gb:.1f}GB",
             ha="center", va="bottom", fontsize=9)
    ax1.set_yticks([])
    ax1.set_xlabel("写字节数 (GB)")
    ax1.set_title("各部分写量分解")
    ax1.legend(fontsize=8, loc="lower right")

    names = [("WAL", wa["wa_wal"]), ("Flush", wa["wa_flush"]),
             ("Compaction", wa["wa_compact"]),
             ("合计(ticker)", wa["wa_total_ticker"]),
             ("设备实测", wa["wa_device"])]
    y = range(len(names))
    ax2.barh(list(y), [v or 0 for _, v in names], color="#4C72B0")
    for yi, (n, v) in zip(y, names):
        ax2.text(v + 0.02, yi, f"{v:.2f}" if v is not None else "N/A",
                 va="center", fontsize=9)
    ax2.set_yticks(list(y))
    ax2.set_yticklabels([n for n, _ in names], fontsize=9)
    ax2.set_xlabel("写放大倍数 (分量/用户数据)")
    ax2.set_title("写放大 (WA)")
    ax2.set_xlim(0, max([v or 0 for _, v in names]) * 1.35 + 0.2)
    fig.suptitle(f"写放大分解（用户数据 {user_gb:.1f}GB，"
                 f"设备总写 {wa['wa_device']:.2f}×）", fontsize=11)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(os.path.join(out_dir, "wa_breakdown.png"), dpi=150)
    plt.close(fig)


def plot_stalls(m, out_dir):
    s = m["stalls"]["series"]
    fig, ax = plt.subplots(figsize=(10, 4.2))
    t = [_hours(x["t_s"]) for x in s]
    v = [x["stall_int_sec"] or 0 for x in s]
    ax.bar(t, v, width=30 / 3600 * 0.85, color="#C44E52",
           label="每 30s 停写秒数")
    ax.set_xlabel("时间 (h)")
    ax.set_ylabel("停写秒数 / 30s 区间", color="#C44E52")
    ax.tick_params(axis="y", labelcolor="#C44E52")
    ax2 = ax.twinx()
    int_pct = [x.get("stall_int_pct") for x in s]
    if any(v is not None for v in int_pct):
        ax2.plot(t, [v if v is not None else 0 for v in int_pct],
                 "o-", color="#4C72B0", ms=3, label="区间停写占比")
        ax2.set_ylabel("区间停写占比 (%)", color="#4C72B0")
        ax2.tick_params(axis="y", labelcolor="#4C72B0")
    ax.set_title(f"写停顿时间线（累计停写 "
                 f"{_hms(m['stalls']['stall_cum_sec'])}，"
                 f"占比 {m['stalls']['stall_cum_pct']}%；"
                 f"delay={m['stalls']['stall_reasons'].get('total-delays')} "
                 f"stop={m['stalls']['stall_reasons'].get('total-stops')}）")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "stalls_timeline.png"), dpi=150)
    plt.close(fig)


def plot_compaction(m, out_dir):
    c = m["compaction"]
    fig, (ax1, ax2) = plt.subplots(
        1, 2, figsize=(11, max(4.2, 1.6 + 0.42 * len(c["per_pair_cnt"]))))
    pc = c["per_pair_cnt"]
    pairs = sorted(pc, key=lambda k: -pc[k])
    ax1.barh(range(len(pairs)), [pc[k] for k in pairs], color="#55A868")
    ax1.set_yticks(range(len(pairs)))
    ax1.set_yticklabels(pairs, fontsize=9)
    for i, k in enumerate(pairs):
        ax1.text(pc[k] + max(pc.values()) * 0.01, i, str(pc[k]),
                 va="center", fontsize=8)
    ax1.set_xlabel("compaction 次数")
    trivial = c.get("trivial_move_likely")
    ax1.set_title(f"按层对次数（共 {c['compact_jobs']} 次；"
                  f"flush {c['flush_jobs']} 次"
                  + (f"；疑似 trivial move {trivial} 次" if trivial else "")
                  + "）")

    pb = c["per_pair_bytes"] or {}
    pairs_b = sorted(pb, key=lambda k: -pb[k])
    ax2.barh(range(len(pairs_b)), [pb[k] / GB for k in pairs_b],
             color="#8172B2")
    ax2.set_yticks(range(len(pairs_b)))
    ax2.set_yticklabels(pairs_b, fontsize=9)
    for i, k in enumerate(pairs_b):
        ax2.text(pb[k] / GB + max(pb.values()) / GB * 0.01, i,
                 f"{pb[k] / GB:.1f}GB", va="center", fontsize=8)
    ax2.set_xlabel("写字节数 (GB)")
    ax2.set_title("按层对写量")
    fig.suptitle(f"compaction 位置与次数（按输出层："
                 f"{c['per_level_cnt']}）", fontsize=11)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(os.path.join(out_dir, "compaction_map.png"), dpi=150)
    plt.close(fig)


def plot_bandwidth(m, out_dir):
    b = m["bandwidth"]
    fig, ax = plt.subplots(figsize=(12, 5))
    dev = b["device_series"]
    peak = b["peak_write_mb_s"]
    t_dev = [_hours(x["t_s"]) for x in dev]
    ax.plot(t_dev, [x["disk_w_mb_s"] for x in dev], lw=0.7, alpha=0.75,
            color="#4C72B0", label="设备写带宽 (1s)")
    ax.plot(t_dev, [x["disk_r_mb_s"] for x in dev], lw=0.5, alpha=0.4,
            color="#DD8452", label="设备读带宽 (1s)")
    if peak:
        ax.axhline(peak, color="k", ls="--", lw=1,
                   label=f"设备峰值 {peak} MB/s")
    cs = b["component_series"]
    if cs:
        t_c = [_hours(x["t_s"]) for x in cs]
        ax.plot(t_c, [x["compaction_int_mb_s"] or 0 for x in cs],
                "o-", ms=3, lw=1, color="#55A868",
                label="compaction 写 (30s)")
        ax.plot(t_c, [x["flush_int_mb_s"] or 0 for x in cs],
                "s-", ms=3, lw=1, color="#C44E52", label="flush 写 (30s)")
        ax.plot(t_c, [x["ingest_int_mb_s"] or 0 for x in cs],
                "^--", ms=3, lw=0.8, color="#8172B2", label="ingest (30s)")
    ax.set_xlabel("时间 (h)")
    ax.set_ylabel("带宽 (MB/s)")
    ax.legend(fontsize=8, ncol=3)
    ax.set_ylim(bottom=0)
    ax2 = ax.twinx()
    ax2.plot(t_dev, [x["util_pct"] or 0 for x in dev], lw=0.5, alpha=0.35,
             color="gray", label="带宽利用率")
    ax2.set_ylabel("利用率 (%)", color="gray")
    ax2.tick_params(axis="y", labelcolor="gray")
    ax.set_title(f"带宽使用率随时间变化（设备写均值 {b['avg_disk_w_mb_s']} "
                 f"MB/s，峰值 {b['max_disk_w_mb_s']} MB/s，"
                 f"利用率均值 {b['avg_util_pct']}%，"
                 f"设备忙均值 {b['avg_io_busy_pct']}%）")
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, "bandwidth_timeline.png"), dpi=150)
    plt.close(fig)


def write_report(m, out_dir):
    wa, st, cp, bw = m["wa"], m["stalls"], m["compaction"], m["bandwidth"]
    tp = m["throughput"]["bench"] or {}
    lines = [
        "# 原版 RocksDB 100GB fillrandom 基线实验报告",
        "",
        f"- 数据集：{wa['user_gb']}GiB（{m['meta']['cmd'].get('num')} × "
        f"{m['meta']['cmd'].get('value_size')}B），"
        f"线程 {m['meta']['cmd'].get('threads')}，"
        f"RocksDB {m['meta']['rocksdb_version']}",
        f"- 吞吐：{tp.get('ops_per_sec')} ops/s，{tp.get('mb_per_s')} MB/s，"
        f"{tp.get('us_per_op')} µs/op；P99 {m['throughput']['percentiles'] and m['throughput']['percentiles'].get('p99')} µs",
        f"- 墙钟：{m['meta']['result'].get('wall_sec')} s（超时："
        f"{m['meta']['result'].get('timeout')}）",
        "",
        "## 1. 写放大分解",
        f"- 用户数据 {wa['user_gb']}GB；设备实测总写 {wa['wa_device']}×",
        f"- WAL {wa['wa_wal']}×，Flush {wa['wa_flush']}×，"
        f"Compaction {wa['wa_compact']}×（ticker 合计 {wa['wa_total_ticker']}×）",
        f"- 逐层写量 (GB) / 次数 / W-Amp：{wa['per_level']}",
        f"- 逐层对写量 (GB)：{ {k: round(v / GB, 1) for k, v in (wa['per_pair_bytes'] or {}).items()} }",
        f"- flush 次数：{wa['flush_jobs']}（表 {wa['flush_tables']}）",
        "",
        "## 2. 写停顿",
        f"- 累计停写 {_hms(st['stall_cum_sec'])}，占比 {st['stall_cum_pct']}%",
        f"- ticker stall.micros = {st['stall_micros']} µs"
        f"（核对差 {m['cross_checks'].get('stall_us_vs_cum_sec')} s）",
        f"- 分原因计数：{st['stall_reasons']}",
        "",
        "## 3. Compaction 位置与次数",
        f"- 总次数 {cp['compact_jobs']}；按输出层 {cp['per_level_cnt']}",
        f"- 按层对：{cp['per_pair_cnt']}",
        f"- 60s 事件分布见 metrics.json `compaction.bins_60s`",
        "",
        "## 4. 带宽使用率",
        f"- 设备峰值写 {bw['peak_write_mb_s']} MB/s；"
        f"运行均值 {bw['avg_disk_w_mb_s']} MB/s（利用率 "
        f"{bw['avg_util_pct']}%），峰值 {bw['max_disk_w_mb_s']} MB/s；"
        f"设备忙均值 {bw['avg_io_busy_pct']}%",
        f"- 30s 组件带宽（compaction/flush/ingest）见 metrics.json "
        f"`bandwidth.component_series`",
        "",
        "## 交叉核对",
        f"- {m['cross_checks']}",
        "",
        f"- 生成时间：{__import__('datetime').datetime.now().strftime('%F %T')}",
    ]
    with open(os.path.join(out_dir, "report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    run_dir = sys.argv[1]
    m = _load(run_dir)
    out_dir = os.path.join(run_dir, "charts")
    os.makedirs(out_dir, exist_ok=True)
    plot_wa(m, out_dir)
    plot_stalls(m, out_dir)
    plot_compaction(m, out_dir)
    plot_bandwidth(m, out_dir)
    write_report(m, run_dir)
    print(f"→ {out_dir}/{{wa_breakdown,stalls_timeline,compaction_map,"
          f"bandwidth_timeline}}.png + report.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
