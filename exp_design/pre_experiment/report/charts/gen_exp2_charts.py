#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gen_exp2_charts.py - exp2 SVG charts (pure stdlib, no matplotlib)."""
import json, os, sys, math
HERE = os.path.dirname(os.path.abspath(__file__))
REPORT = os.path.join(os.path.dirname(HERE), "exp2_report.json")
OUT_DIR = HERE

class SvgChart:
    def __init__(self, w=760, h=460, title="", ylog=False, y_label="", x_label=""):
        self.w=w; self.h=h; self.title=title; self.ylog=ylog
        self.y_label=y_label; self.x_label=x_label
        self.series=[]; self.hlines=[]
    def add_line(self, name, xs, ys, color, mark="o", dashed=False):
        self.series.append((name, xs, ys, color, mark, dashed))
    def add_hline(self, value, color, label, dashed=True):
        self.hlines.append((value, color, label, dashed))
    def _yt(self, v):
        return math.log10(v) if self.ylog and v>0 else v
    def _yticks(self, lo, hi, n=5):
        if self.ylog:
            ll=math.floor(math.log10(lo)) if lo>0 else -1
            hl=math.ceil(math.log10(hi)) if hi>0 else 0
            return [10**e for e in range(ll, hl+1)]
        rng=hi-lo or 1; raw=rng/n; mag=10**math.floor(math.log10(raw)); norm=raw/mag
        s=1 if norm<=1.5 else (2 if norm<=3 else (5 if norm<=7 else 10)); step=s*mag
        start=math.floor(lo/step)*step; t=[]; v=start
        while v<=hi+step*0.5:
            if v>=lo-step*0.01: t.append(round(v,10))
            v+=step
        return t
    def _fmt(self, v):
        if self.ylog:
            if v>=1:
                e=int(round(math.log10(v)))
                if 10**e==v: return f"1e{e}" if e else "1"
            return f"{v:g}"
        return f"{v:,.0f}" if abs(v)>=1000 else f"{v:g}"
    def render(self):
        W,H=self.w,self.h; pl,pr,pt,pb=78,28,56,64; pw=W-pl-pr; ph=H-pt-pb
        ax=[]; ay=[]
        for _,xs,ys,*_ in self.series: ax.extend(xs); ay.extend(ys)
        for hv,*_ in self.hlines: ay.append(hv)
        if not ax: ax=[0,1]
        if not ay: ay=[0,1]
        x_min,x_max=min(ax),max(ax)
        yv=[v for v in ay if (not self.ylog or v>0)]
        if self.ylog:
            y_min=10**math.floor(math.log10(min(yv))); y_max=10**math.ceil(math.log10(max(yv)))
        else:
            y_min=min(yv); y_max=max(yv); span=(y_max-y_min) or abs(y_max)*0.1 or 1
            y_min-=span*0.08; y_max+=span*0.12
            if y_min>0 and y_min<span*0.5: y_min=0
        xr=(x_max-x_min) or 1; xp=xr*0.05; x_lo=x_min-xp; x_hi=x_max+xp
        def sx(x): return pl+(x-x_lo)/(x_hi-x_lo)*pw
        def sy(y):
            yy=self._yt(y); ylo=self._yt(y_min); yhi=self._yt(y_max)
            return pt+(1-(yy-ylo)/(yhi-ylo))*ph
        p=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" font-family="Helvetica,Arial,sans-serif">']
        p.append(f'<rect x="0" y="0" width="{W}" height="{H}" fill="#ffffff"/>')
        p.append(f'<text x="{W/2}" y="26" text-anchor="middle" font-size="16" font-weight="700" fill="#1f2937">{self.title}</text>')
        for t in self._yticks(y_min if not self.ylog else min(yv), y_max if not self.ylog else max(yv)):
            yy=sy(t)
            if pt<=yy<=pt+ph:
                p.append(f'<line x1="{pl}" y1="{yy:.1f}" x2="{pl+pw}" y2="{yy:.1f}" stroke="#e5e7eb" stroke-width="1"/>')
                p.append(f'<text x="{pl-8}" y="{yy+4:.1f}" text-anchor="end" font-size="11" fill="#6b7280">{self._fmt(t)}</text>')
        if self.y_label:
            p.append(f'<text x="18" y="{pt+ph/2}" text-anchor="middle" font-size="12" fill="#374151" transform="rotate(-90 18 {pt+ph/2})">{self.y_label}</text>')
        for t in sorted(set(ax)):
            xx=sx(t)
            p.append(f'<line x1="{xx:.1f}" y1="{pt+ph}" x2="{xx:.1f}" y2="{pt+ph+5}" stroke="#9ca3af" stroke-width="1"/>')
            p.append(f'<text x="{xx:.1f}" y="{pt+ph+20}" text-anchor="middle" font-size="11" fill="#6b7280">{t}</text>')
        if self.x_label:
            p.append(f'<text x="{pl+pw/2}" y="{H-16}" text-anchor="middle" font-size="12" fill="#374151">{self.x_label}</text>')
        p.append(f'<line x1="{pl}" y1="{pt}" x2="{pl}" y2="{pt+ph}" stroke="#374151" stroke-width="1.5"/>')
        p.append(f'<line x1="{pl}" y1="{pt+ph}" x2="{pl+pw}" y2="{pt+ph}" stroke="#374151" stroke-width="1.5"/>')
        for hv,color,label,dashed in self.hlines:
            yy=sy(hv); dash='stroke-dasharray="6 4"' if dashed else ''
            p.append(f'<line x1="{pl}" y1="{yy:.1f}" x2="{pl+pw}" y2="{yy:.1f}" stroke="{color}" stroke-width="1.6" {dash}/>')
            p.append(f'<text x="{pl+pw-4}" y="{yy-4:.1f}" text-anchor="end" font-size="10.5" fill="{color}" font-weight="600">{label}</text>')
        for name,xs,ys,color,mark,dashed in self.series:
            pts=[(sx(x),sy(y if (not self.ylog or y>0) else y_min)) for x,y in zip(xs,ys)]
            dash='stroke-dasharray="7 4"' if dashed else ''
            path=" ".join(f"{x:.1f},{y:.1f}" for x,y in pts)
            p.append(f'<polyline points="{path}" fill="none" stroke="{color}" stroke-width="2.2" {dash}/>')
            r=4.5
            for x,y in pts:
                if mark=="o": p.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r}" fill="{color}"/>')
                elif mark=="s": p.append(f'<rect x="{x-r:.1f}" y="{y-r:.1f}" width="{2*r}" height="{2*r}" fill="{color}"/>')
                elif mark=="D": p.append(f'<polygon points="{x},{y-r} {x+r},{y} {x},{y+r} {x-r},{y}" fill="{color}"/>')
        lx=pl+12; ly=pt+14
        for name,xs,ys,color,mark,dashed in self.series:
            dash='stroke-dasharray="6 3"' if dashed else ''
            p.append(f'<line x1="{lx}" y1="{ly}" x2="{lx+22}" y2="{ly}" stroke="{color}" stroke-width="2.4" {dash}/>')
            p.append(f'<circle cx="{lx+11}" cy="{ly}" r="3.5" fill="{color}"/>')
            p.append(f'<text x="{lx+28}" y="{ly+4}" font-size="11.5" fill="#1f2937">{name}</text>'); ly+=18
        for hv,color,label,dashed in self.hlines:
            dash='stroke-dasharray="6 3"' if dashed else ''
            p.append(f'<line x1="{lx}" y1="{ly}" x2="{lx+22}" y2="{ly}" stroke="{color}" stroke-width="2" {dash}/>')
            p.append(f'<text x="{lx+28}" y="{ly+4}" font-size="11.5" fill="{color}">{label}</text>'); ly+=18
        p.append('</svg>')
        return "\n".join(p)

def main():
    rep=json.load(open(REPORT,encoding="utf-8"))
    rl=rep["range_lookup"]["results"]; mw=rep["multi_wal_fsync"]["results"]
    rv=rep["metadata"]["decision_referenced_values"]; lim=rv["limits"]
    def wal(p,mode,sync):
        for r in mw:
            if r["p"]==p and r["mode"]==mode and r["sync_wal"]==sync: return r
        raise KeyError(f"missing {p}/{mode}/{sync}")
    ps=[1,16,64]
    # Chart1 throughput
    c1=SvgChart(title="吞吐 vs 分区数 P（sync_wal=true）", y_label="吞吐 (ops/s)", x_label="分区数 P")
    tA=[wal(p,"independent_fsync",True)["throughput_ops_per_s"] for p in ps]
    tB=[wal(p,"batched_fsync",True)["throughput_ops_per_s"] for p in ps]
    bt=rv["baseline_p1_throughput_ops_per_s"]; thr15=bt*(1-lim["regression_pct_max"]/100)
    c1.add_line("模式A independent_fsync",ps,tA,"#dc2626","o")
    c1.add_line("模式B batched_fsync",ps,tB,"#2563eb","s")
    c1.add_hline(thr15,"#059669",f"PASS 阈值(回退≤{lim['regression_pct_max']:g}%)={thr15:,.0f}")
    open(os.path.join(OUT_DIR,"exp2_throughput_vs_p.svg"),"w",encoding="utf-8").write(c1.render())
    # Chart2 fsync/op
    c2=SvgChart(title="fsync/op vs 分区数 P（sync_wal=true，对数轴）", y_label="fsync 次数/op", x_label="分区数 P", ylog=True)
    fA=[wal(p,"independent_fsync",True)["fsync_count_per_op"] for p in ps]
    fB=[wal(p,"batched_fsync",True)["fsync_count_per_op"] for p in ps]
    bf=rv["baseline_p1_fsync_count_per_op"]; f2x=bf*lim["fsync_ratio_max"]
    c2.add_line("模式A independent_fsync",ps,fA,"#dc2626","o")
    c2.add_line("模式B batched_fsync",ps,fB,"#2563eb","s")
    c2.add_hline(f2x,"#059669",f"PASS 阈值(≤2×基线)={f2x:g}")
    open(os.path.join(OUT_DIR,"exp2_fsync_per_op_vs_p.svg"),"w",encoding="utf-8").write(c2.render())
    # Chart3 durability
    c3=SvgChart(title="durability window vs 分区数 P（sync_wal=true，对数轴）", y_label="durability window p99 (ms)", x_label="分区数 P", ylog=True)
    dA=[wal(p,"independent_fsync",True)["durability_window_ms"] for p in ps]
    dB=[wal(p,"batched_fsync",True)["durability_window_ms"] for p in ps]
    c3.add_line("模式A independent_fsync",ps,dA,"#dc2626","o")
    c3.add_line("模式B batched_fsync",ps,dB,"#2563eb","s")
    c3.add_hline(lim["durability_window_ms_max"],"#059669",f"PASS 阈值(≤{lim['durability_window_ms_max']:g}ms)")
    open(os.path.join(OUT_DIR,"exp2_durability_vs_p.svg"),"w",encoding="utf-8").write(c3.render())
    # Chart4 range lookup
    c4=SvgChart(title="range lookup 延迟 vs 分区数 P", y_label="延迟 (ns)", x_label="分区数 P")
    rps=[64,128,256]
    g=lambda p,im,k: next(r[k] for r in rl if r["p"]==p and r["impl"]==im)
    bsm=[g(p,"binary_search","mean_ns") for p in rps]; bsp=[g(p,"binary_search","p99_ns") for p in rps]
    btm=[g(p,"small_btree","mean_ns") for p in rps]; btp=[g(p,"small_btree","p99_ns") for p in rps]
    c4.add_line("binary_search mean",rps,bsm,"#1d4ed8","o")
    c4.add_line("binary_search p99",rps,bsp,"#1d4ed8","o",True)
    c4.add_line("small_btree mean",rps,btm,"#dc2626","s")
    c4.add_line("small_btree p99",rps,btp,"#dc2626","s",True)
    c4.add_hline(100,"#059669","达标阈值 p99≤100ns")
    open(os.path.join(OUT_DIR,"exp2_range_lookup_vs_p.svg"),"w",encoding="utf-8").write(c4.render())
    print("[done] charts: tput A=%s B=%s thr15=%.1f | fsync A=%s B=%s 2x=%g | dur A=%s B=%s lim=%g | range bsm=%s bsp=%s btm=%s btp=%s"%(
        tA,tB,thr15,fA,fB,f2x,dA,dB,lim["durability_window_ms_max"],bsm,bsp,btm,btp))

if __name__=="__main__":
    sys.exit(main())
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gen_exp2_charts.py - 为实验 2 生成 SVG 矢量图表（纯标准库，无需 matplotlib）。

读取 exp_design/pre_experiment/report/exp2_report.json（权威数据源），
生成 4 张 SVG 图表到同目录 charts/：
  exp2_throughput_vs_p.svg      sync=true 档，模式 A/B 吞吐 vs P
  exp2_fsync_per_op_vs_p.svg    sync=true 档，模式 A/B fsync/op vs P（对数轴）
  exp2_durability_vs_p.svg       sync=true 档，模式 A/B durability window vs P（对数轴）
  exp2_range_lookup_vs_p.svg    range lookup mean/p99 vs P（binary_search / small_btree）

所有数值严格取自 exp2_report.json，禁止伪造或估算。
matplotlib 在本机不可用，故用纯 Python 生成 SVG 矢量图（浏览器原生支持 <img>）。
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPORT = os.path.join(os.path.dirname(HERE), "exp2_report.json")
OUT_DIR = HERE

# ---------- SVG 画布 ----------

class SvgChart:
    """极简 SVG 折线/散点图，支持线性或对数 Y 轴。"""

    def __init__(self, w=760, h=460, title="", ylog=False, y_label="",
                 x_label="", y_unit=""):
        self.w = w
        self.h = h
        self.title = title
        self.ylog = ylog
        self.y_label = y_label
        self.x_label = x_label
        self.y_unit = y_unit
        self.series = []   # list of (name, xs, ys, color, mark, dashed)
        self.hlines = []   # (value, color, label, dashed)
        self.parts = []

    def add_line(self, name, xs, ys, color, mark="o", dashed=False):
        self.series.append((name, xs, ys, color, mark, dashed))

    def add_hline(self, value, color, label, dashed=True):
        self.hlines.append((value, color, label, dashed))

    def _y_transform(self, v):
        if self.ylog:
            if v <= 0:
                v = 1e-12
            import math
            return math.log10(v)
        return v

    def _nice_ticks(self, lo, hi, n=5):
        if lo == hi:
            hi = lo + 1
        rng = hi - lo
        if self.ylog:
            import math
            lo_l = math.floor(math.log10(lo)) if lo > 0 else -1
            hi_l = math.ceil(math.log10(hi)) if hi > 0 else 0
            ticks = []
            e = lo_l
            while e <= hi_l:
                ticks.append(10 ** e)
                e += 1
            return ticks
        step = 10 ** (int(round(__import__('math').log10(rng / n))) ) if rng > 0 else 1
        import math
        raw = rng / n
        mag = 10 ** math.floor(math.log10(raw))
        norm = raw / mag
        if norm <= 1.5:
            s = 1
        elif norm <= 3:
            s = 2
        elif norm <= 7:
            s = 5
        else:
            s = 10
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
        if self.ylog:
            import math
            if v >= 1:
                e = int(round(math.log10(v)))
                if 10 ** e == v:
                    return f"1e{e}" if e != 0 else "1"
            if v >= 1000:
                return f"{v:,.0f}"
            if v >= 1:
                return f"{v:g}"
            return f"{v:g}"
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        if abs(v) >= 1:
            return f"{v:g}"
        return f"{v:g}"

    def render(self):
        import math
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
        yvals = all_y
        if self.ylog:
            yvals = [v for v in yvals if v > 0]
            y_min = min(yvals)
            y_max = max(yvals)
            # pad log range a bit
            y_min_l = math.floor(math.log10(y_min))
            y_max_l = math.ceil(math.log10(y_max))
            y_min = 10 ** y_min_l
            y_max = 10 ** y_max_l
        else:
            y_min = min(yvals)
            y_max = max(yvals)
            span = y_max - y_min if y_max > y_min else abs(y_max) * 0.1 or 1
            y_min -= span * 0.08
            y_max += span * 0.12
            if y_min > 0 and y_min < span * 0.5:
                y_min = 0

        xr = (x_max - x_min) or 1
        x_pad = xr * 0.05
        x_lo = x_min - x_pad
        x_hi = x_max + x_pad

        def sx(x):
            return pad_l + (x - x_lo) / (x_hi - x_lo) * plot_w

        def sy(y):
            if self.ylog:
                yy = self._y_transform(y)
                ylo = self._y_transform(y_min)
                yhi = self._y_transform(y_max)
            else:
                yy = y
                ylo = y_min
                yhi = y_max
            return pad_t + (1 - (yy - ylo) / (yhi - ylo)) * plot_h

        p = []
        p.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
                  f'viewBox="0 0 {W} {H}" font-family="Helvetica,Arial,sans-serif">')
        p.append(f'<rect x="0" y="0" width="{W}" height="{H}" fill="#ffffff"/>')
        # title
        p.append(f'<text x="{W/2}" y="26" text-anchor="middle" '
                  f'font-size="16" font-weight="700" fill="#1f2937">{self.title}</text>')

        # grid + y ticks
        yticks = self._nice_ticks(y_min if not self.ylog else min(yvals),
                                  y_max if not self.ylog else max(yvals))
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

        # x ticks
        xticks = sorted(set(all_x))
        for t in xticks:
            xx = sx(t)
            p.append(f'<line x1="{xx:.1f}" y1="{pad_t+plot_h}" x2="{xx:.1f}" '
                      f'y2="{pad_t+plot_h+5}" stroke="#9ca3af" stroke-width="1"/>')
            p.append(f'<text x="{xx:.1f}" y="{pad_t+plot_h+20}" text-anchor="middle" '
                      f'font-size="11" fill="#6b7280">{t}</text>')
        # x label
        if self.x_label:
            p.append(f'<text x="{pad_l+plot_w/2}" y="{H-16}" text-anchor="middle" '
                      f'font-size="12" fill="#374151">{self.x_label}</text>')

        # axes
        p.append(f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" '
                  f'y2="{pad_t+plot_h}" stroke="#374151" stroke-width="1.5"/>')
        p.append(f'<line x1="{pad_l}" y1="{pad_t+plot_h}" x2="{pad_l+plot_w}" '
                  f'y2="{pad_t+plot_h}" stroke="#374151" stroke-width="1.5"/>')

        # threshold hlines
        for hv, color, label, dashed in self.hlines:
            yy = sy(hv)
            dash = 'stroke-dasharray="6 4"' if dashed else ''
            p.append(f'<line x1="{pad_l}" y1="{yy:.1f}" x2="{pad_l+plot_w}" '
                      f'y2="{yy:.1f}" stroke="{color}" stroke-width="1.6" {dash}/>')
            p.append(f'<text x="{pad_l+plot_w-4}" y="{yy-4:.1f}" text-anchor="end" '
                      f'font-size="10.5" fill="{color}" font-weight="600">{label}</text>')

        # series
        marks = {"o": "circle", "s": "rect", "D": "diamond", "^": "triangle"}
        for name, xs, ys, color, mark, dashed in self.series:
            pts = []
            for x, y in zip(xs, ys):
                xx = sx(x)
                yy = sy(y if (not self.ylog or y > 0) else max(y, y_min))
                pts.append((xx, yy))
            dash = 'stroke-dasharray="7 4"' if dashed else ''
            path = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
            p.append(f'<polyline points="{path}" fill="none" stroke="{color}" '
                      f'stroke-width="2.2" {dash}/>')
            r = 4.5
            for x, y in pts:
                if mark == "o":
                    p.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r}" '
                              f'fill="{color}"/>')
                elif mark == "s":
                    p.append(f'<rect x="{x-r:.1f}" y="{y-r:.1f}" width="{2*r}" '
                              f'height="{2*r}" fill="{color}"/>')
                elif mark == "D":
                    p.append(f'<polygon points="{x},{y-r} {x+r},{y} {x},{y+r} '
                              f'{x-r},{y}" fill="{color}"/>')
                elif mark == "^":
                    p.append(f'<polygon points="{x},{y-r} {x+r},{y+r} {x-r},{y+r}" '
                              f'fill="{color}"/>')

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

        p.append('</svg>')
        return "\n".join(p)


def main():
    with open(REPORT, "r", encoding="utf-8") as f:
        rep = json.load(f)

    rl = rep["range_lookup"]["results"]
    mw = rep["multi_wal_fsync"]["results"]
    rv = rep["metadata"]["decision_referenced_values"]
    limits = rv["limits"]

    def wal(p, mode, sync):
        for r in mw:
            if r["p"] == p and r["mode"] == mode and r["sync_wal"] == sync:
                return r
        raise KeyError(f"missing {p}/{mode}/{sync}")

    ps = [1, 16, 64]
    # ---- Chart 1: throughput vs P (sync=true) ----
    c1 = SvgChart(title="吞吐 vs 分区数 P（sync_wal=true）",
                  y_label="吞吐 (ops/s)", x_label="分区数 P")
    tA = [wal(p, "independent_fsync", True)["throughput_ops_per_s"] for p in ps]
    tB = [wal(p, "batched_fsync", True)["throughput_ops_per_s"] for p in ps]
    base_t = rv["baseline_p1_throughput_ops_per_s"]
    reg_lim = limits["regression_pct_max"]
    thr_15 = base_t * (1 - reg_lim / 100.0)  # 15% regression threshold throughput
    c1.add_line("模式A independent_fsync", ps, tA, "#dc2626", "o")
    c1.add_line("模式B batched_fsync", ps, tB, "#2563eb", "s")
    c1.add_hline(thr_15, "#059669", f"PASS 阈值(回退≤{reg_lim:g}%)={thr_15:,.0f}")
    open(os.path.join(OUT_DIR, "exp2_throughput_vs_p.svg"), "w", encoding="utf-8").write(c1.render())
    print(f"[chart1] throughput: A={tA} B={tB} thr15={thr_15:.1f}")

    # ---- Chart 2: fsync/op vs P (sync=true, log Y) ----
    c2 = SvgChart(title="fsync/op vs 分区数 P（sync_wal=true，对数轴）",
                  y_label="fsync 次数/op", x_label="分区数 P", ylog=True)
    fA = [wal(p, "independent_fsync", True)["fsync_count_per_op"] for p in ps]
    fB = [wal(p, "batched_fsync", True)["fsync_count_per_op"] for p in ps]
    base_f = rv["baseline_p1_fsync_count_per_op"]
    fsync_2x = base_f * limits["fsync_ratio_max"]
    c2.add_line("模式A independent_fsync", ps, fA, "#dc2626", "o")
    c2.add_line("模式B batched_fsync", ps, fB, "#2563eb", "s")
    c2.add_hline(fsync_2x, "#059669", f"PASS 阈值(≤2×基线)={fsync_2x:g}")
    open(os.path.join(OUT_DIR, "exp2_fsync_per_op_vs_p.svg"), "w", encoding="utf-8").write(c2.render())
    print(f"[chart2] fsync: A={fA} B={fB} 2x={fsync_2x}")

    # ---- Chart 3: durability window vs P (sync=true, log Y) ----
    c3 = SvgChart(title="durability window vs 分区数 P（sync_wal=true，对数轴）",
                  y_label="durability window p99 (ms)", x_label="分区数 P", ylog=True)
    dA = [wal(p, "independent_fsync", True)["durability_window_ms"] for p in ps]
    dB = [wal(p, "batched_fsync", True)["durability_window_ms"] for p in ps]
    dur_lim = limits["durability_window_ms_max"]
    c3.add_line("模式A independent_fsync", ps, dA, "#dc2626", "o")
    c3.add_line("模式B batched_fsync", ps, dB, "#2563eb", "s")
    c3.add_hline(dur_lim, "#059669", f"PASS 阈值(≤{dur_lim:g}ms)")
    open(os.path.join(OUT_DIR, "exp2_durability_vs_p.svg"), "w", encoding="utf-8").write(c3.render())
    print(f"[chart3] durability: A={dA} B={dB} lim={dur_lim}")

    # ---- Chart 4: range lookup mean/p99 vs P ----
    c4 = SvgChart(title="range lookup 延迟 vs 分区数 P",
                  y_label="延迟 (ns)", x_label="分区数 P")
    rps = [64, 128, 256]
    bs_mean = [next(r["mean_ns"] for r in rl if r["p"] == p and r["impl"] == "binary_search") for p in rps]
    bs_p99 = [next(r["p99_ns"] for r in rl if r["p"] == p and r["impl"] == "binary_search") for p in rps]
    bt_mean = [next(r["mean_ns"] for r in rl if r["p"] == p and r["impl"] == "small_btree") for p in rps]
    bt_p99 = [next(r["p99_ns"] for r in rl if r["p"] == p and r["impl"] == "small_btree") for p in rps]
    c4.add_line("binary_search mean", rps, bs_mean, "#1d4ed8", "o")
    c4.add_line("binary_search p99", rps, bs_p99, "#1d4ed8", "o", dashed=True)
    c4.add_line("small_btree mean", rps, bt_mean, "#dc2626", "s")
    c4.add_line("small_btree p99", rps, bt_p99, "#dc2626", "s", dashed=True)
    c4.add_hline(100, "#059669", "达标阈值 p99≤100ns")
    open(os.path.join(OUT_DIR, "exp2_range_lookup_vs_p.svg"), "w", encoding="utf-8").write(c4.render())
    print(f"[chart4] range: bs_mean={bs_mean} bs_p99={bs_p99} bt_mean={bt_mean} bt_p99={bt_p99}")

    print("[done] 4 SVG charts written to", OUT_DIR)


if __name__ == "__main__":
    sys.exit(main())
