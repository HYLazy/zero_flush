#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gen_exp1_charts.py - 为实验 1 生成 SVG 矢量图表（纯标准库，无需 matplotlib）。

读取 exp_design/pre_experiment/report/exp1_report.json（权威数据源），
生成 4 张 SVG 图表到同目录 charts/：
  exp1_overlap_baseline_vs_intervention.svg   overlap baseline vs intervention（zipfian+uniform）
  exp1_throughput_baseline_vs_intervention.svg  throughput baseline vs intervention
  exp1_per_rep_intervention_overlap.svg        per-rep intervention overlap（zipfian run1-3 + uniform，含1.2目标线）
  exp1_l0_file_count.svg                       L0 file count baseline vs intervention

所有数值严格取自 exp1_report.json，禁止伪造或估算。
matplotlib 在本机不可用，故用纯 Python 生成 SVG 矢量图（浏览器原生支持 <img>）。
"""
import json
import os
import sys
import math

HERE = os.path.dirname(os.path.abspath(__file__))
REPORT = os.path.join(os.path.dirname(HERE), "exp1_report.json")
OUT_DIR = HERE


class SvgBarChart:
    """极简 SVG 分组柱状图，支持水平阈值线。"""

    def __init__(self, w=760, h=460, title="", y_label="", x_label=""):
        self.w = w
        self.h = h
        self.title = title
        self.y_label = y_label
        self.x_label = x_label
        self.groups = []   # [(label, [(bar_label, value, color), ...])]
        self.hlines = []   # (value, color, label, dashed)

    def add_group(self, label, bars):
        """bars = [(bar_label, value, color), ...]"""
        self.groups.append((label, bars))

    def add_hline(self, value, color, label, dashed=True):
        self.hlines.append((value, color, label, dashed))

    def _nice_ticks(self, lo, hi, n=5):
        if lo == hi:
            hi = lo + 1
        rng = hi - lo
        raw = rng / n
        mag = 10 ** math.floor(math.log10(raw)) if raw > 0 else 1
        norm = raw / mag
        s = 1 if norm <= 1.5 else (2 if norm <= 3 else (5 if norm <= 7 else 10))
        step = s * mag
        start = math.floor(lo / step) * step
        ticks = []
        v = start
        while v <= hi + step * 0.5:
            if v >= lo - step * 0.01:
                ticks.append(round(v, 10))
            v += step
        return ticks

    def _fmt(self, v):
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        if abs(v) >= 1:
            return f"{v:g}"
        return f"{v:g}"

    def render(self):
        W, H = self.w, self.h
        pad_l, pad_r, pad_t, pad_b = 78, 28, 56, 64
        plot_w = W - pad_l - pad_r
        plot_h = H - pad_t - pad_b

        all_y = []
        for _, bars in self.groups:
            for _, v, _ in bars:
                all_y.append(v)
        for hv, *_ in self.hlines:
            all_y.append(hv)
        if not all_y:
            all_y = [0, 1]

        y_min = 0
        y_max = max(all_y)
        span = y_max - y_min if y_max > y_min else 1
        y_max += span * 0.15

        def sy(y):
            return pad_t + (1 - (y - y_min) / (y_max - y_min)) * plot_h

        n_groups = len(self.groups)
        n_bars = max(len(bars) for _, bars in self.groups) if self.groups else 1
        group_w = plot_w / n_groups
        bar_w = group_w * 0.65 / n_bars
        gap = bar_w * 0.15

        p = []
        p.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
                  f'viewBox="0 0 {W} {H}" font-family="Helvetica,Arial,sans-serif">')
        p.append(f'<rect x="0" y="0" width="{W}" height="{H}" fill="#ffffff"/>')
        # title
        p.append(f'<text x="{W/2}" y="26" text-anchor="middle" '
                  f'font-size="16" font-weight="700" fill="#1f2937">{self.title}</text>')

        # grid + y ticks
        yticks = self._nice_ticks(y_min, y_max)
        for t in yticks:
            yy = sy(t)
            if pad_t <= yy <= pad_t + plot_h:
                p.append(f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{pad_l+plot_w}" '
                          f'y2="{yy:.1f}" stroke="#e5e7eb" stroke-width="1"/>')
                p.append(f'<text x="{pad_l-8}" y="{yy+4:.1f}" text-anchor="end" '
                          f'font-size="11" fill="#6b7280">{self._fmt(t)}</text>')
        # y label
        if self.y_label:
            p.append(f'<text x="18" y="{pad_t+plot_h/2}" text-anchor="middle" '
                      f'font-size="12" fill="#374151" transform="rotate(-90 18 '
                      f'{pad_t+plot_h/2})">{self.y_label}</text>')

        # threshold hlines
        for hv, color, label, dashed in self.hlines:
            yy = sy(hv)
            dash = 'stroke-dasharray="6 4"' if dashed else ''
            p.append(f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{pad_l+plot_w}" '
                      f'y2="{yy:.1f}" stroke="{color}" stroke-width="1.8" {dash}/>')

        # bars
        for gi, (glabel, bars) in enumerate(self.groups):
            gx = pad_l + gi * group_w + group_w * 0.175
            # group label
            cx = pad_l + gi * group_w + group_w / 2
            p.append(f'<text x="{cx:.1f}" y="{pad_t+plot_h+20}" text-anchor="middle" '
                      f'font-size="12" fill="#374151" font-weight="600">{glabel}</text>')
            for bi, (blabel, val, color) in enumerate(bars):
                bx = gx + bi * (bar_w + gap)
                by = sy(val)
                bh = pad_t + plot_h - by
                p.append(f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bar_w:.1f}" '
                          f'height="{bh:.1f}" fill="{color}" rx="2"/>')
                # value label on top
                p.append(f'<text x="{bx+bar_w/2:.1f}" y="{by-5:.1f}" text-anchor="middle" '
                          f'font-size="11" fill="#1f2937" font-weight="700">{self._fmt(val)}</text>')
                # bar label below
                p.append(f'<text x="{bx+bar_w/2:.1f}" y="{pad_t+plot_h+38}" text-anchor="middle" '
                          f'font-size="10" fill="#6b7280">{blabel}</text>')

        # axes
        p.append(f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" '
                  f'y2="{pad_t+plot_h}" stroke="#374151" stroke-width="1.5"/>')
        p.append(f'<line x1="{pad_l}" y1="{pad_t+plot_h}" x2="{pad_l+plot_w}" '
                  f'y2="{pad_t+plot_h}" stroke="#374151" stroke-width="1.5"/>')

        # legend
        lx = pad_l + 12
        ly = pad_t + 14
        seen = set()
        for _, bars in self.groups:
            for blabel, _, color in bars:
                key = (blabel, color)
                if key not in seen:
                    seen.add(key)
                    p.append(f'<rect x="{lx}" y="{ly-7}" width="14" height="14" '
                              f'fill="{color}" rx="2"/>')
                    p.append(f'<text x="{lx+18}" y="{ly+4}" font-size="11.5" '
                              f'fill="#1f2937">{blabel}</text>')
                    ly += 20
        for hv, color, label, dashed in self.hlines:
            dash = 'stroke-dasharray="6 3"' if dashed else ''
            p.append(f'<line x1="{lx}" y1="{ly}" x2="{lx+22}" y2="{ly}" '
                      f'stroke="{color}" stroke-width="2" {dash}/>')
            p.append(f'<text x="{lx+28}" y="{ly+4}" font-size="11.5" '
                      f'fill="{color}">{label}</text>')
            ly += 20

        # threshold labels at right
        for hv, color, label, dashed in self.hlines:
            yy = sy(hv)
            p.append(f'<text x="{pad_l+plot_w-4}" y="{yy-5:.1f}" text-anchor="end" '
                      f'font-size="10.5" fill="{color}" font-weight="600">{label}</text>')

        # x label
        if self.x_label:
            p.append(f'<text x="{pad_l+plot_w/2}" y="{H-12}" text-anchor="middle" '
                      f'font-size="12" fill="#374151">{self.x_label}</text>')

        p.append('</svg>')
        return "\n".join(p)


class SvgLineChart:
    """极简 SVG 折线/散点图（复用 exp2 风格）。"""

    def __init__(self, w=760, h=460, title="", y_label="", x_label=""):
        self.w = w
        self.h = h
        self.title = title
        self.y_label = y_label
        self.x_label = x_label
        self.series = []
        self.hlines = []

    def add_line(self, name, xs, ys, color, mark="o", dashed=False):
        self.series.append((name, xs, ys, color, mark, dashed))

    def add_hline(self, value, color, label, dashed=True):
        self.hlines.append((value, color, label, dashed))

    def _fmt(self, v):
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        return f"{v:g}"

    def render(self):
        W, H = self.w, self.h
        pad_l, pad_r, pad_t, pad_b = 78, 28, 56, 64
        plot_w = W - pad_l - pad_r
        plot_h = H - pad_t - pad_b

        all_x = []
        all_y = []
        for _, xs, ys, *_ in self.series:
            all_x.extend(xs)
            all_y.extend(ys)
        for hv, *_ in self.hlines:
            all_y.append(hv)
        if not all_x:
            all_x = [0, 1]
        if not all_y:
            all_y = [0, 1]

        x_min, x_max = min(all_x), max(all_x)
        y_min = 0
        y_max = max(all_y)
        span = y_max - y_min if y_max > y_min else 1
        y_max += span * 0.15

        xr = (x_max - x_min) or 1
        x_pad = xr * 0.08
        x_lo = x_min - x_pad
        x_hi = x_max + x_pad

        def sx(x):
            return pad_l + (x - x_lo) / (x_hi - x_lo) * plot_w

        def sy(y):
            return pad_t + (1 - (y - y_min) / (y_max - y_min)) * plot_h

        # nice y ticks
        rng = y_max - y_min
        raw = rng / 5
        mag = 10 ** math.floor(math.log10(raw)) if raw > 0 else 1
        norm = raw / mag
        s = 1 if norm <= 1.5 else (2 if norm <= 3 else (5 if norm <= 7 else 10))
        step = s * mag
        start = math.floor(y_min / step) * step
        yticks = []
        v = start
        while v <= y_max + step * 0.5:
            if v >= y_min - step * 0.01:
                yticks.append(round(v, 10))
            v += step

        p = []
        p.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
                  f'viewBox="0 0 {W} {H}" font-family="Helvetica,Arial,sans-serif">')
        p.append(f'<rect x="0" y="0" width="{W}" height="{H}" fill="#ffffff"/>')
        p.append(f'<text x="{W/2}" y="26" text-anchor="middle" '
                  f'font-size="16" font-weight="700" fill="#1f2937">{self.title}</text>')

        for t in yticks:
            yy = sy(t)
            if pad_t <= yy <= pad_t + plot_h:
                p.append(f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{pad_l+plot_w}" '
                          f'y2="{yy:.1f}" stroke="#e5e7eb" stroke-width="1"/>')
                p.append(f'<text x="{pad_l-8}" y="{yy+4:.1f}" text-anchor="end" '
                          f'font-size="11" fill="#6b7280">{self._fmt(t)}</text>')
        if self.y_label:
            p.append(f'<text x="18" y="{pad_t+plot_h/2}" text-anchor="middle" '
                      f'font-size="12" fill="#374151" transform="rotate(-90 18 '
                      f'{pad_t+plot_h/2})">{self.y_label}</text>')

        for t in sorted(set(all_x)):
            xx = sx(t)
            p.append(f'<line x1="{xx:.1f}" y1="{pad_t+plot_h}" x2="{xx:.1f}" '
                      f'y2="{pad_t+plot_h+5}" stroke="#9ca3af" stroke-width="1"/>')
            p.append(f'<text x="{xx:.1f}" y="{pad_t+plot_h+20}" text-anchor="middle" '
                      f'font-size="11" fill="#6b7280">{t}</text>')

        p.append(f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" '
                  f'y2="{pad_t+plot_h}" stroke="#374151" stroke-width="1.5"/>')
        p.append(f'<line x1="{pad_l}" y1="{pad_t+plot_h}" x2="{pad_l+plot_w}" '
                  f'y2="{pad_t+plot_h}" stroke="#374151" stroke-width="1.5"/>')

        for hv, color, label, dashed in self.hlines:
            yy = sy(hv)
            dash = 'stroke-dasharray="6 4"' if dashed else ''
            p.append(f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{pad_l+plot_w}" '
                      f'y2="{yy:.1f}" stroke="{color}" stroke-width="1.8" {dash}/>')

        for name, xs, ys, color, mark, dashed in self.series:
            pts = [(sx(x), sy(y)) for x, y in zip(xs, ys)]
            dash = 'stroke-dasharray="7 4"' if dashed else ''
            path = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
            p.append(f'<polyline points="{path}" fill="none" stroke="{color}" '
                      f'stroke-width="2.2" {dash}/>')
            r = 5
            for x, y in pts:
                if mark == "o":
                    p.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r}" fill="{color}"/>')
                elif mark == "s":
                    p.append(f'<rect x="{x-r:.1f}" y="{y-r:.1f}" width="{2*r}" height="{2*r}" fill="{color}"/>')
                # value label
                p.append(f'<text x="{x:.1f}" y="{y-10:.1f}" text-anchor="middle" '
                          f'font-size="10" fill="{color}" font-weight="600">{ys[xs.index(x)]:g}</text>')

        # legend
        lx = pad_l + 12
        ly = pad_t + 14
        for name, xs, ys, color, mark, dashed in self.series:
            dash = 'stroke-dasharray="6 3"' if dashed else ''
            p.append(f'<line x1="{lx}" y1="{ly}" x2="{lx+22}" y2="{ly}" '
                      f'stroke="{color}" stroke-width="2.4" {dash}/>')
            p.append(f'<circle cx="{lx+11}" cy="{ly}" r="3.5" fill="{color}"/>')
            p.append(f'<text x="{lx+28}" y="{ly+4}" font-size="11.5" '
                      f'fill="#1f2937">{name}</text>')
            ly += 18
        for hv, color, label, dashed in self.hlines:
            dash = 'stroke-dasharray="6 3"' if dashed else ''
            p.append(f'<line x1="{lx}" y1="{ly}" x2="{lx+22}" y2="{ly}" '
                      f'stroke="{color}" stroke-width="2" {dash}/>')
            p.append(f'<text x="{lx+28}" y="{ly+4}" font-size="11.5" '
                      f'fill="{color}">{label}</text>')
            ly += 18

        for hv, color, label, dashed in self.hlines:
            yy = sy(hv)
            p.append(f'<text x="{pad_l+plot_w-4}" y="{yy-5:.1f}" text-anchor="end" '
                      f'font-size="10.5" fill="{color}" font-weight="600">{label}</text>')

        if self.x_label:
            p.append(f'<text x="{pad_l+plot_w/2}" y="{H-12}" text-anchor="middle" '
                      f'font-size="12" fill="#374151">{self.x_label}</text>')

        p.append('</svg>')
        return "\n".join(p)


def main():
    with open(REPORT, "r", encoding="utf-8") as f:
        rep = json.load(f)

    runs = rep["runs"]
    baseline_zip = rep["baseline"]
    interv_zip = rep["intervention"]
    baseline_uni = rep["metadata"]["baseline_uniform"]
    interv_uni = rep["metadata"]["intervention_uniform"]

    # Colors
    C_BASE = "#3b82f6"   # blue for baseline
    C_INTERV = "#ef4444"  # red for intervention
    C_PASS = "#10b981"   # green for pass
    C_FAIL = "#ef4444"   # red for fail
    C_TARGET = "#059669" # green for target line
    C_AMBER = "#f59e0b"  # amber

    # ---- Chart 1: overlap baseline vs intervention ----
    c1 = SvgBarChart(
        title="L0 Overlap Factor：基线 vs 干预（中位数）",
        y_label="overlap_factor_mean",
        x_label="键分布"
    )
    c1.add_group("zipfian(θ=0.99)", [
        ("基线", baseline_zip["overlap_factor_mean"], C_BASE),
        ("干预", interv_zip["overlap_factor_mean"], C_INTERV),
    ])
    c1.add_group("uniform", [
        ("基线", baseline_uni["overlap_factor_mean"], C_BASE),
        ("干预", interv_uni["overlap_factor_mean"], C_INTERV),
    ])
    c1.add_hline(1.2, C_TARGET, "目标 ≤1.2", dashed=True)
    open(os.path.join(OUT_DIR, "exp1_overlap_baseline_vs_intervention.svg"), "w", encoding="utf-8").write(c1.render())
    print(f"[chart1] overlap: zip_base={baseline_zip['overlap_factor_mean']} zip_int={interv_zip['overlap_factor_mean']} "
          f"uni_base={baseline_uni['overlap_factor_mean']} uni_int={interv_uni['overlap_factor_mean']}")

    # ---- Chart 2: throughput baseline vs intervention ----
    c2 = SvgBarChart(
        title="写入吞吐：基线 vs 干预（中位数）",
        y_label="吞吐 (ops/s)",
        x_label="键分布"
    )
    c2.add_group("zipfian(θ=0.99)", [
        ("基线", baseline_zip["write_throughput_ops_per_s"], C_BASE),
        ("干预", interv_zip["write_throughput_ops_per_s"], C_INTERV),
    ])
    c2.add_group("uniform", [
        ("基线", baseline_uni["throughput_ops_per_s"], C_BASE),
        ("干预", interv_uni["throughput_ops_per_s"], C_INTERV),
    ])
    open(os.path.join(OUT_DIR, "exp1_throughput_baseline_vs_intervention.svg"), "w", encoding="utf-8").write(c2.render())
    print(f"[chart2] throughput: zip_base={baseline_zip['write_throughput_ops_per_s']} zip_int={interv_zip['write_throughput_ops_per_s']} "
          f"uni_base={baseline_uni['throughput_ops_per_s']} uni_int={interv_uni['throughput_ops_per_s']}")

    # ---- Chart 3: per-rep intervention overlap ----
    # zipfian intervention reps: run1=1.99766, run2=1.97978, run3=0.98773
    # uniform intervention: 0.23429
    interv_zip_reps = [r for r in runs if r["group"] == "intervention" and r["distribution"] == "zipfian:0.99"]
    interv_zip_reps.sort(key=lambda r: r["rep"])
    rep_labels = [f"zipfian\nrun{r['rep']}" for r in interv_zip_reps]
    rep_vals = [r["overlap_factor_mean"] for r in interv_zip_reps]
    rep_colors = [C_FAIL if v > 1.2 else C_PASS for v in rep_vals]

    c3 = SvgBarChart(
        title="逐 rep 干预 overlap（zipfian 3 rep + uniform 1 rep）",
        y_label="overlap_factor_mean",
        x_label="运行"
    )
    for i, (lbl, val, clr) in enumerate(zip(rep_labels, rep_vals, rep_colors)):
        c3.add_group(lbl, [(f"overlap={val:g}", val, clr)])
    c3.add_group("uniform\nrun1", [
        (f"overlap={interv_uni['overlap_factor_mean']:g}", interv_uni["overlap_factor_mean"], C_PASS),
    ])
    c3.add_hline(1.2, C_TARGET, "目标 ≤1.2", dashed=True)
    open(os.path.join(OUT_DIR, "exp1_per_rep_intervention_overlap.svg"), "w", encoding="utf-8").write(c3.render())
    print(f"[chart3] per-rep overlap: zip={[f'{v:g}' for v in rep_vals]} uni={interv_uni['overlap_factor_mean']:g}")

    # ---- Chart 4: L0 file count baseline vs intervention ----
    # l0_file_count not in intervention summary; compute median from runs
    def median(vals):
        s = sorted(vals)
        n = len(s)
        return s[n // 2] if n % 2 == 1 else (s[n // 2 - 1] + s[n // 2]) / 2

    base_zip_l0 = median([r["l0_file_count"] for r in runs if r["group"] == "baseline" and r["distribution"] == "zipfian:0.99"])
    interv_zip_l0 = median([r["l0_file_count"] for r in runs if r["group"] == "intervention" and r["distribution"] == "zipfian:0.99"])
    base_uni_l0 = next(r["l0_file_count"] for r in runs if r["group"] == "baseline" and r["distribution"] == "uniform")
    interv_uni_l0 = next(r["l0_file_count"] for r in runs if r["group"] == "intervention" and r["distribution"] == "uniform")

    c4 = SvgBarChart(
        title="L0 文件数：基线 vs 干预（中位数）",
        y_label="L0 file count",
        x_label="键分布"
    )
    c4.add_group("zipfian(θ=0.99)", [
        ("基线", base_zip_l0, C_BASE),
        ("干预", interv_zip_l0, C_INTERV),
    ])
    c4.add_group("uniform", [
        ("基线", base_uni_l0, C_BASE),
        ("干预", interv_uni_l0, C_INTERV),
    ])
    open(os.path.join(OUT_DIR, "exp1_l0_file_count.svg"), "w", encoding="utf-8").write(c4.render())
    print(f"[chart4] l0_file_count: zip_base={base_zip_l0} zip_int={interv_zip_l0} uni_base={base_uni_l0} uni_int={interv_uni_l0}")

    print("[done] 4 SVG charts written to", OUT_DIR)


if __name__ == "__main__":
    sys.exit(main())
