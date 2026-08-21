#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gen_exp0_charts.py - 生成实验0报告所需的4张matplotlib图表。

图表:
  1. exp0_wa_decomposition.png  - WA分解堆叠柱状图 (wa_wal/wa_flush/wa_compaction)
  2. exp0_flush_share.png       - flush占NAND写比例 (含0.2-0.4区间标注)
  3. exp0_throughput_vs_value.png - 吞吐vs value size (zipfian vs uniform)
  4. exp0_nand_write_comparison.png - NAND写三项对比 (WAL/flush/compaction)

数据来源: exp0_report.json + wa_summary.json (只读, 不修改)
"""

import json
import os
import sys

import matplotlib
matplotlib.use('Agg')  # 非交互后端
import matplotlib.pyplot as plt
import numpy as np

REPORT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHART_DIR = os.path.join(REPORT_DIR, "charts")
REPORT_JSON = os.path.join(REPORT_DIR, "exp0_report.json")
WA_JSON = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(REPORT_DIR))),
    "output", "pre_exp", "exp0", "wa_summary.json")

# ---- 颜色方案 ----
C_WAL = "#4C72B0"       # 蓝色 - WAL
C_FLUSH = "#DD8452"     # 橙色 - flush
C_COMP = "#55A868"      # 绿色 - compaction
C_ZIPF = "#C44E52"      # 红色 - zipfian
C_UNIF = "#4C72B0"      # 蓝色 - uniform
C_RANGE = "#55A868"     # 绿色 - 在区间内
C_OUT = "#C44E52"       # 红色 - 超出区间


def load_data():
    with open(REPORT_JSON, "r", encoding="utf-8") as f:
        report = json.load(f)
    with open(WA_JSON, "r", encoding="utf-8") as f:
        wa = json.load(f)
    return report, wa


def chart1_wa_decomposition(wa):
    """WA分解堆叠柱状图: wa_wal, wa_flush, wa_compaction 按组合。"""
    checks = wa["checks"]
    labels = [c["combo"] for c in checks]
    wa_wal = [c["wa_wal"] for c in checks]
    wa_flush = [c["wa_flush"] for c in checks]
    wa_comp = [c["wa_compaction"] for c in checks]

    x = np.arange(len(labels))
    width = 0.55

    fig, ax = plt.subplots(figsize=(10, 6))
    b1 = ax.bar(x, wa_wal, width, label='WA_WAL', color=C_WAL)
    b2 = ax.bar(x, wa_flush, width, bottom=wa_wal,
                label='WA_Flush', color=C_FLUSH)
    bottoms = [w + f for w, f in zip(wa_wal, wa_flush)]
    b3 = ax.bar(x, wa_comp, width, bottom=bottoms,
                label='WA_Compaction', color=C_COMP)

    # 在每段上标注数值
    for i in range(len(labels)):
        ax.text(i, wa_wal[i] / 2, f'{wa_wal[i]:.2f}',
                ha='center', va='center', fontsize=8, fontweight='bold',
                color='white')
        ax.text(i, wa_wal[i] + wa_flush[i] / 2, f'{wa_flush[i]:.2f}',
                ha='center', va='center', fontsize=8, fontweight='bold',
                color='white')
        ax.text(i, bottoms[i] + wa_comp[i] / 2, f'{wa_comp[i]:.2f}',
                ha='center', va='center', fontsize=8, fontweight='bold',
                color='white')
        # 总WA标注
        total = wa_wal[i] + wa_flush[i] + wa_comp[i]
        ax.text(i, total + 0.15, f'Σ={total:.2f}',
                ha='center', va='bottom', fontsize=8, color='#333333')

    ax.set_ylabel('Write Amplification (×)', fontsize=12)
    ax.set_title('Exp0: WA Decomposition by Combo\n'
                 '(wa_wal + wa_flush + wa_compaction, stacked)',
                 fontsize=13, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha='right', fontsize=10)
    ax.legend(loc='upper left', fontsize=10)
    ax.set_ylim(0, max(bottoms[i] + wa_comp[i] for i in range(len(labels)))
               * 1.15)
    ax.grid(axis='y', alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(CHART_DIR, "exp0_wa_decomposition.png"),
                dpi=150, bbox_inches='tight')
    plt.close(fig)
    print("  -> exp0_wa_decomposition.png")


def chart2_flush_share(wa):
    """flush占NAND写比例柱状图, 含0.2-0.4区间标注。"""
    checks = wa["checks"]
    labels = [c["combo"] for c in checks]
    shares = [c["flush_share_of_nand_writes"] for c in checks]
    mean_share = wa["flush_share_of_nand_writes_mean"]

    x = np.arange(len(labels))
    width = 0.55
    colors = [C_RANGE if 0.2 <= s <= 0.4 else C_OUT for s in shares]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(x, shares, width, color=colors, edgecolor='#333333',
                  linewidth=0.5)

    # 标注数值
    for i, (bar, s) in enumerate(zip(bars, shares)):
        ax.text(i, s + 0.005, f'{s:.3f}',
                ha='center', va='bottom', fontsize=9, fontweight='bold')

    # 期望区间 0.2-0.4
    ax.axhspan(0.2, 0.4, alpha=0.15, color=C_RANGE,
               label='Expected range [0.2, 0.4]')
    ax.axhline(y=0.2, color=C_RANGE, linestyle='--', linewidth=1, alpha=0.7)
    ax.axhline(y=0.4, color=C_RANGE, linestyle='--', linewidth=1, alpha=0.7)
    # 均值线
    ax.axhline(y=mean_share, color='#333333', linestyle='-.',
               linewidth=1.5, alpha=0.8,
               label=f'Mean = {mean_share:.4f}')

    ax.set_ylabel('flush_share_of_nand_writes', fontsize=12)
    ax.set_title('Exp0: Flush Share of NAND Writes by Combo\n'
                 '(Expected: 0.2–0.4, Mean shown as dash-dot)',
                 fontsize=13, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha='right', fontsize=10)
    ax.legend(loc='upper right', fontsize=9)
    ax.set_ylim(0, max(max(shares), 0.45) * 1.1)
    ax.grid(axis='y', alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(CHART_DIR, "exp0_flush_share.png"),
                dpi=150, bbox_inches='tight')
    plt.close(fig)
    print("  -> exp0_flush_share.png")


def chart3_throughput(report):
    """吞吐 vs value size (zipfian vs uniform, 双对数)。"""
    fillrandom = [r for r in report["runs"]
                  if r["benchmark"] == "fillrandom"]

    # 从 notes 中提取 ops/sec
    import re
    def extract_ops(notes):
        m = re.search(r'ops/sec=([\d.]+)', notes)
        return float(m.group(1)) if m else None

    # 按 distribution 分组
    zipf_data = {}  # vs -> ops/sec
    unif_data = {}
    for r in fillrandom:
        vs = r["value_size_bytes"]
        ops = extract_ops(r["notes"])
        if r["distribution"] == "zipfian":
            zipf_data[vs] = ops
        else:
            unif_data[vs] = ops

    vs_sizes = sorted(set(list(zipf_data.keys()) + list(unif_data.keys())))
    zipf_ops = [zipf_data.get(vs) for vs in vs_sizes]
    unif_ops = [unif_data.get(vs) for vs in vs_sizes]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(vs_sizes, zipf_ops, 'o-', color=C_ZIPF, linewidth=2,
            markersize=8, label='zipfian (θ=0.99)')
    ax.plot(vs_sizes, unif_ops, 's-', color=C_UNIF, linewidth=2,
            markersize=8, label='uniform')

    # 标注数值
    for vs, ops in zip(vs_sizes, zipf_ops):
        if ops:
            ax.annotate(f'{ops:,.0f}', (vs, ops),
                        textcoords="offset points", xytext=(10, 5),
                        fontsize=9, color=C_ZIPF)
    for vs, ops in zip(vs_sizes, unif_ops):
        if ops:
            ax.annotate(f'{ops:,.0f}', (vs, ops),
                        textcoords="offset points", xytext=(10, -12),
                        fontsize=9, color=C_UNIF)

    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlabel('Value Size (bytes)', fontsize=12)
    ax.set_ylabel('Throughput (ops/sec)', fontsize=12)
    ax.set_title('Exp0: Throughput vs Value Size\n'
                 '(fillrandom, 8 threads, 10M writes/thread, 80M total ops)',
                 fontsize=13, fontweight='bold')
    ax.set_xticks(vs_sizes)
    ax.set_xticklabels([str(v) for v in vs_sizes])
    ax.legend(fontsize=11)
    ax.grid(True, which='both', alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(CHART_DIR, "exp0_throughput_vs_value.png"),
                dpi=150, bbox_inches='tight')
    plt.close(fig)
    print("  -> exp0_throughput_vs_value.png")


def chart4_nand_write(report):
    """NAND写三项对比 (WAL/flush/compaction) 按组合, 对数Y轴。"""
    fillrandom = [r for r in report["runs"]
                  if r["benchmark"] == "fillrandom"]

    labels = [f"{r['distribution']}_vs{r['value_size_bytes']}"
              for r in fillrandom]
    wal_mb = [r["wal_bytes_written_mb"] for r in fillrandom]
    flush_mb = [r["flush_bytes_written_mb"] for r in fillrandom]
    comp_mb = [r["compaction_bytes_written_mb"] for r in fillrandom]
    total_mb = [r["total_nand_write_mb"] for r in fillrandom]

    x = np.arange(len(labels))
    width = 0.22

    fig, ax = plt.subplots(figsize=(12, 6))
    b1 = ax.bar(x - width, wal_mb, width, label='WAL', color=C_WAL)
    b2 = ax.bar(x, flush_mb, width, label='Flush', color=C_FLUSH)
    b3 = ax.bar(x + width, comp_mb, width, label='Compaction', color=C_COMP)

    # 标注数值 (简短格式)
    for bars, vals in [(b1, wal_mb), (b2, flush_mb), (b3, comp_mb)]:
        for bar, v in zip(bars, vals):
            if v and v > 0:
                label = f'{v/1024:.1f}GB' if v >= 1024 else f'{v:.0f}MB'
                ax.text(bar.get_x() + bar.get_width() / 2, v * 1.05,
                        label, ha='center', va='bottom', fontsize=7,
                        rotation=90)

    ax.set_yscale('log')
    ax.set_ylabel('Bytes Written (MB, log scale)', fontsize=12)
    ax.set_title('Exp0: NAND Write Breakdown — WAL vs Flush vs Compaction\n'
                 '(6 combos, median of 3 runs)', fontsize=13,
                 fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha='right', fontsize=10)
    ax.legend(fontsize=11, loc='upper left')
    ax.grid(axis='y', alpha=0.3, which='both')
    fig.tight_layout()
    fig.savefig(os.path.join(CHART_DIR, "exp0_nand_write_comparison.png"),
                dpi=150, bbox_inches='tight')
    plt.close(fig)
    print("  -> exp0_nand_write_comparison.png")


def main():
    os.makedirs(CHART_DIR, exist_ok=True)
    report, wa = load_data()
    print("Generating Exp0 charts...")
    chart1_wa_decomposition(wa)
    chart2_flush_share(wa)
    chart3_throughput(report)
    chart4_nand_write(report)
    print("All charts generated.")


if __name__ == "__main__":
    main()
