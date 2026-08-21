#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""verify_exp2.py - 数据一致性自检：exp2_report.json 关键值是否在 md/html 中出现。"""
import json, os, re
HERE = os.path.dirname(os.path.abspath(__file__))
REPORT = os.path.join(HERE, "exp2_report.json")
MD = os.path.join(HERE, "exp2_stage_report.md")
HTML = os.path.join(HERE, "exp2_presentation.html")

def norm(s):
    # 去掉千分位逗号、去掉 .0 尾缀统一，方便匹配
    return s.replace(",", "")

def check(label, needle, hay_md, hay_html):
    nm = norm(hay_md); nh = norm(hay_html); nn = norm(str(needle))
    in_md = nn in nm
    in_html = nn in nh
    status = "OK" if (in_md and in_html) else "MISS"
    print(f"  [{status}] {label}: '{needle}'  md={in_md} html={in_html}")
    return in_md and in_html

def main():
    rep = json.load(open(REPORT, encoding="utf-8"))
    md = open(MD, encoding="utf-8").read()
    html = open(HTML, encoding="utf-8").read()
    rl = rep["range_lookup"]["results"]
    mw = rep["multi_wal_fsync"]["results"]
    rv = rep["metadata"]["decision_referenced_values"]
    alts = rep["metadata"]["alternatives"]
    bl = rep["multi_wal_fsync"]["baseline_single_wal"]
    hw = rep["metadata"]["hardware"]

    ok = True
    print("== decision / rationale / alternatives ==")
    ok &= check("decision", rep["decision"], md, html)
    ok &= check("mode_b_regression_pct", rv["mode_b_regression_pct"], md, html)
    ok &= check("mode_b_fsync_ratio", rv["mode_b_fsync_ratio_vs_p1"], md, html)
    ok &= check("mode_a_regression_pct", rv["mode_a_regression_pct"], md, html)

    print("== range_lookup 6 组合 ==")
    for r in rl:
        ok &= check(f"range p={r['p']} {r['impl']} mean", r["mean_ns"], md, html)
        ok &= check(f"range p={r['p']} {r['impl']} p99", r["p99_ns"], md, html)

    print("== multi_wal 12 组合 ==")
    for r in mw:
        ok &= check(f"wal p={r['p']} {r['mode']} sync={r['sync_wal']} tput", r["throughput_ops_per_s"], md, html)
        ok &= check(f"wal p={r['p']} {r['mode']} sync={r['sync_wal']} fsync/op", r["fsync_count_per_op"], md, html)
        ok &= check(f"wal p={r['p']} {r['mode']} sync={r['sync_wal']} dur", r["durability_window_ms"], md, html)

    print("== baseline ==")
    for b in bl:
        ok &= check(f"baseline sync={b['sync_wal']} tput", b["throughput_ops_per_s"], md, html)
        ok &= check(f"baseline sync={b['sync_wal']} fsync", b["fsync_count_per_op"], md, html)

    print("== alternatives ==")
    for a in alts:
        # 截前 30 字符匹配
        ok &= check("alt[:30]", a[:30], md, html)

    print("== hardware ==")
    ok &= check("cpu_governor", hw["cpu_governor"], md, html)
    ok &= check("cpu_cores", hw["cpu_cores"], md, html)

    print("== 图表 SVG 引用 ==")
    for f in ["exp2_throughput_vs_p.svg","exp2_fsync_per_op_vs_p.svg",
              "exp2_durability_vs_p.svg","exp2_range_lookup_vs_p.svg"]:
        p = os.path.join(HERE, "charts", f)
        ex = os.path.exists(p) and os.path.getsize(p) > 0
        in_html = f in html
        in_md = f in md
        st = "OK" if (ex and in_html) else "MISS"
        print(f"  [{st}] chart {f}: exists={ex} html_ref={in_html} md_ref={in_md}")
        ok &= ex and in_html

    print("\n== 总结 ==")
    print("ALL CONSISTENT" if ok else "INCONSISTENCY FOUND")
    return 0 if ok else 1

if __name__ == "__main__":
    raise SystemExit(main())
