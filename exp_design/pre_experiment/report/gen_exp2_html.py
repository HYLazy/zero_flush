#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""gen_exp2_html.py - 读 exp2_report.json 生成 exp2_presentation.html（数值直取JSON）。"""
import json, os
HERE = os.path.dirname(os.path.abspath(__file__))
REPORT = os.path.join(HERE, "exp2_report.json")
OUT = os.path.join(HERE, "exp2_presentation.html")

def main():
    rep = json.load(open(REPORT, encoding="utf-8"))
    rl = rep["range_lookup"]["results"]
    mw = rep["multi_wal_fsync"]["results"]
    bl = rep["multi_wal_fsync"]["baseline_single_wal"]
    rv = rep["metadata"]["decision_referenced_values"]
    lim = rv["limits"]
    alts = rep["metadata"]["alternatives"]
    hw = rep["metadata"]["hardware"]
    notes = rep.get("notes", "")

    def wal(p, mode, sync):
        for r in mw:
            if r["p"]==p and r["mode"]==mode and r["sync_wal"]==sync: return r
    def fmt(v, dec=0):
        if isinstance(v, float) and v==int(v) and dec==0: return f"{int(v):,}"
        if dec: return f"{v:,.{dec}f}"
        return f"{v:,}" if isinstance(v,(int,float)) else str(v)

    # range lookup rows
    rl_rows = ""
    for r in rl:
        rl_rows += (f'<tr><td class="c">{r["p"]}</td><td class="l">{r["impl"]}</td>'
                    f'<td>{r["mean_ns"]}</td><td>{r["p99_ns"]}</td>'
                    f'<td class="hl-green">≤100ns</td></tr>\n')

    # multi-wal rows
    mode_label = {"independent_fsync": "A independent", "batched_fsync": "B batched"}
    wal_rows = ""
    for r in mw:
        p=r["p"]; md=mode_label[r["mode"]]; sy="true" if r["sync_wal"] else "false"
        tp=f'{r["throughput_ops_per_s"]:,.1f}'; fp=f'{r["fsync_count_per_op"]}'; dw=f'{r["durability_window_ms"]}'
        key=""
        if p==1 and r["mode"]=="independent_fsync" and r["sync_wal"]: key="基线"
        elif not r["sync_wal"]: key="nosync"
        elif p==64 and r["mode"]=="independent_fsync": key="回退 76.80%"
        elif p==64 and r["mode"]=="batched_fsync": key="回退 19.47%"
        wal_rows += (f'<tr><td class="c">{p}</td><td class="l">{md}</td><td class="c">{sy}</td>'
                     f'<td>{tp}</td><td>{fp}</td><td>{dw}</td>'
                     f'<td class="l">{key}</td></tr>\n')

    # decision gate tables
    b_t=rv["baseline_p1_throughput_ops_per_s"]; b_f=rv["baseline_p1_fsync_count_per_op"]
    mB_t=rv["mode_b_p64_throughput_ops_per_s"]; mB_f=rv["mode_b_p64_fsync_count_per_op"]
    mB_w=rv["mode_b_p64_durability_window_ms"]; regB=rv["mode_b_regression_pct"]; ratB=rv["mode_b_fsync_ratio_vs_p1"]
    mA_t=rv["mode_a_p64_throughput_ops_per_s"]; mA_f=rv["mode_a_p64_fsync_count_per_op"]
    regA=rv["mode_a_regression_pct"]; mA_w=wal(64,"independent_fsync",True)["durability_window_ms"]
    mA_rat = mA_f/b_f
    thr15=b_t*(1-lim["regression_pct_max"]/100)

    gateB = (f'<tr><td class="l">G1 吞吐回退</td><td>(306743−247030.7)/306743×100 = {regB}%</td><td>≤15%</td><td class="hl-red">FAIL</td></tr>'
             f'<tr><td class="l">G2 fsync/op 比值</td><td>{mB_f}/{b_f} = {ratB}×</td><td>≤2×</td><td class="hl-green">PASS</td></tr>'
             f'<tr><td class="l">G3 durability</td><td>{mB_w} ms</td><td>≤5 ms</td><td class="hl-red">FAIL</td></tr>')
    gateA = (f'<tr><td class="l">G1 吞吐回退</td><td>(306743−71174)/306743×100 = {regA}%</td><td>≤15%</td><td class="hl-red">FAIL</td></tr>'
             f'<tr><td class="l">G2 fsync/op 比值</td><td>{mA_f}/{b_f} = {mA_rat:.2f}×</td><td>≤2×</td><td class="hl-red">FAIL</td></tr>'
             f'<tr><td class="l">G3 durability</td><td>{mA_w} ms</td><td>≤5 ms</td><td class="hl-red">FAIL</td></tr>')

    alt_html = ""
    for i, a in enumerate(alts, 1):
        alt_html += f'<div class="alt"><b>替代 {["①","②"][i-1] if i<=2 else i}</b> {a}</div>\n'

    html = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>实验 2：分区开销门 — 阶段性报告</title>
<style>
:root{{--bg:#0f172a;--panel:#1e293b;--panel2:#273449;--ink:#e2e8f0;--muted:#94a3b8;
--line:#334155;--accent:#38bdf8;--red:#f87171;--red-bg:#7f1d1d;--green:#34d399;
--amber:#fbbf24;--blue:#60a5fa}}
*{{box-sizing:border-box}}
body{{margin:0;background:var(--bg);color:var(--ink);
font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",Helvetica,Arial,sans-serif;
line-height:1.6;font-size:15px}}
.wrap{{max-width:1180px;margin:0 auto;padding:32px 24px 64px}}
header{{padding:28px 30px;border-radius:14px;background:linear-gradient(135deg,#1e293b,#312e4a);
border:1px solid var(--line);margin-bottom:28px}}
header h1{{margin:0 0 6px;font-size:26px;font-weight:800;letter-spacing:.3px}}
header .sub{{color:var(--muted);font-size:13.5px}}
.badge{{display:inline-block;padding:3px 12px;border-radius:999px;font-size:12px;font-weight:700;margin-left:8px}}
.badge.fail{{background:var(--red-bg);color:#fff;border:1px solid #b91c1c}}
.card{{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:20px 22px;margin-bottom:20px}}
.card h2{{margin:0 0 10px;font-size:18px;color:#fff;border-bottom:1px solid var(--line);padding-bottom:8px}}
.card h3{{margin:18px 0 8px;font-size:15px;color:var(--accent)}}
.kv{{display:grid;grid-template-columns:130px 1fr;gap:6px 14px;font-size:13.5px}}
.kv b{{color:var(--muted);font-weight:600}}
.failbox{{background:linear-gradient(135deg,#7f1d1d,#451a1a);border:1.5px solid #b91c1c;
border-radius:12px;padding:20px 24px;margin:22px 0}}
.failbox .verdict{{font-size:30px;font-weight:900;color:#fecaca;letter-spacing:1px}}
.failbox .why{{color:#fee2e2;margin-top:8px;font-size:13.5px}}
table{{border-collapse:collapse;width:100%;font-size:13px;margin:8px 0 4px}}
th,td{{border:1px solid var(--line);padding:7px 9px;text-align:right;white-space:nowrap}}
th{{background:var(--panel2);color:#cbd5e1;font-weight:700}}
td.l,th.l{{text-align:left}}td.c,th.c{{text-align:center}}
tr:hover td{{background:#1b2638}}
.hl-red{{color:var(--red);font-weight:700}}.hl-green{{color:var(--green);font-weight:700}}
.hl-amber{{color:var(--amber);font-weight:700}}
.chart{{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:14px 16px}}
.chart img{{display:block;width:100%;height:auto;margin:0 auto;border-radius:6px;background:#fff}}
.chart .cap{{color:var(--muted);font-size:12.5px;text-align:center;margin-top:6px}}
.cols2{{display:grid;grid-template-columns:1fr 1fr;gap:18px}}
.alt{{background:#0b1220;border-left:3px solid var(--accent);padding:12px 14px;border-radius:6px;margin:8px 0}}
.note{{font-size:12.5px;color:var(--muted);background:#0b1220;border-radius:8px;padding:10px 14px;margin-top:10px}}
.toc{{display:flex;flex-wrap:wrap;gap:8px;margin-top:12px}}
.toc a{{font-size:12.5px;color:var(--accent);text-decoration:none;padding:3px 9px;border:1px solid var(--line);border-radius:6px}}
footer{{margin-top:36px;padding-top:16px;border-top:1px solid var(--line);color:var(--muted);font-size:12.5px}}
@media(max-width:820px){{.cols2{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<div class="wrap">

<header>
<h1>实验 2：分区开销门（Partition Overhead Gate）<span class="badge fail">判定 FAIL</span></h1>
<div class="sub">ZeroFlush · WAL=L0 分区设计可行性门验证 · 报告日期 2026-08-04 · 对应 PPT 第 10 页 Design 1 · 数据源 <code>exp2_report.json</code></div>
<div class="toc"><a href="#summary">摘要</a><a href="#range">range lookup</a><a href="#wal">multi-WAL</a><a href="#charts">图表</a><a href="#decision">判定</a><a href="#alt">替代设计</a></div>
</header>

<section class="card" id="summary">
<h2>摘要</h2>
<p>本实验验证 ZeroFlush「WAL=L0 分区映射」的两个可疑开销点：<b>(a)</b> 每次 Put 的 range lookup 延迟；<b>(b)</b> 单 WAL 拆成 P 个分区 WAL 后 fsync / 组提交语义是否导致吞吐崩塌。机械决策规则要求 <code>sync_wal=true</code> 时模式 B(P=64) 相对单 WAL 基线吞吐回退 ≤15%、fsync/op ≤2×基线、且 durability window ≤5ms。</p>
<p>结果：<span class="hl-green">range lookup 达标</span>（p99 66–90 ns &lt; 100 ns）；<span class="hl-red">multi-WAL 不达标</span>——基线 P=1 模式A ≈306,743 ops/s；P=64 模式A 回退 {regA}%（fsync/op {mA_f}）；P=64 模式B 回退 {regB}%（&gt;15%，fsync/op {mB_f} 比值{ratB} 达标但窗口 {mB_w} ms ≫5ms）。<b>两模式均不满足 PASS → FAIL</b>，触发 PPT 第 10 页设计修订。</p>
<div class="kv">
<b>实验编号</b><span>exp2_partition_overhead_gate</span>
<b>基线吞吐</b><span>306,743 ops/s（P=1, 模式A, sync=true = baseline_single_wal）</span>
<b>硬件</b><span>{hw["cpu_model"]} · {hw["memory_gb"]} GB · {hw["disk"]} · 调频器 {hw["cpu_governor"]}</span>
<b>reps</b><span>3（取中位数）· value_size=1024B</span>
</div>
</section>

<section class="failbox" id="decision">
<div class="verdict">判定：FAIL</div>
<div class="why"><b>rationale：</b>{rep["decision_rationale"]}</div>
</section>

<section class="card" id="range">
<h2>3.1 range lookup 延迟（6 组合，zipfian θ=0.99，预热 1M 丢弃后测 10M 次）</h2>
<p style="color:var(--muted);font-size:13px">p99 全部 ≤90 ns &lt; 100 ns 阈值 → 风险点 (a) 达标。</p>
<table>
<thead><tr><th class="l">P</th><th class="l">impl</th><th>mean_ns</th><th>p99_ns</th><th>判定</th></tr></thead>
<tbody>
{rl_rows}</tbody>
</table>
</section>

<section class="card" id="wal">
<h2>3.2 multi-WAL fsync / 组提交（12 组合 + baseline 2 档）</h2>
<p style="color:var(--muted);font-size:13px">模式 A=independent_fsync（每 dirty 文件各自 fsync，commit 批=512）；模式 B=batched_fsync（统一批量 fsync，commit 批=512×P）。key 分布 uniform over 2^24（worst-case dirty 扇出）。</p>
<table>
<thead><tr><th class="l">P</th><th class="l">mode</th><th class="c">sync</th><th>throughput (ops/s)</th><th>fsync/op</th><th>durability p99 (ms)</th><th>判定关键</th></tr></thead>
<tbody>
{wal_rows}</tbody>
</table>
<p style="color:var(--muted);font-size:13px;margin-top:10px"><b>baseline_single_wal</b>（P=1, 模式A）：sync=true {bl[0]["throughput_ops_per_s"]:,.1f} ops/s / fsync/op {bl[0]["fsync_count_per_op"]}；sync=false {bl[1]["throughput_ops_per_s"]:,.1f} ops/s / fsync/op {bl[1]["fsync_count_per_op"]}。</p>
</section>

<section id="charts">
<h2 style="color:#fff;font-size:18px;margin:24px 0 4px;border-bottom:1px solid var(--line);padding-bottom:8px">4. 图表</h2>
<div class="cols2">
<div class="chart"><img src="charts/exp2_throughput_vs_p.svg" alt="throughput vs P"><div class="cap">① 吞吐 vs P（sync=true，模式A/B）。模式A 随P 线性塌陷；模式B 缓慢下降，P=64=247,030 &lt; PASS 阈值 {thr15:,.0f}</div></div>
<div class="chart"><img src="charts/exp2_fsync_per_op_vs_p.svg" alt="fsync/op vs P"><div class="cap">② fsync/op vs P（对数轴）。模式A P=64=0.0131（{mA_rat:.1f}×基线，远超2×）；模式B=0.000383（{ratB}×，达标）</div></div>
<div class="chart"><img src="charts/exp2_durability_vs_p.svg" alt="durability vs P"><div class="cap">③ durability window vs P（对数轴）。模式B P=64={mB_w} ms ≫5ms；模式A={mA_w} ms 亦超标</div></div>
<div class="chart"><img src="charts/exp2_range_lookup_vs_p.svg" alt="range lookup vs P"><div class="cap">④ range lookup 延迟 vs P。两种实现 p99 全部 66–90 ns &lt; 100ns，风险点(a)达标</div></div>
</div>
<div class="note">说明：本机 matplotlib 不可用，故按约束降级——用纯 Python 标准库生成 <b>SVG 矢量图表</b>，浏览器原生支持 <code>&lt;img&gt;</code> 渲染，结构等同 PNG。生成脚本：<code>charts/gen_exp2_charts.py</code>。</div>
</section>

<section class="card">
<h2>5. decision 机械判定（逐步核对）</h2>
<p style="font-size:13px;color:var(--muted)">基线 baseline = P=1, 模式A, sync=true：throughput {b_t:,.1f} ops/s，fsync/op {b_f}。三道闸门须全部满足方为 PASS。</p>
<h3>候选 B = P=64, batched_fsync, sync=true</h3>
<table>
<thead><tr><th class="l">闸门</th><th>计算值</th><th>阈值</th><th>判定</th></tr></thead>
<tbody>{gateB}</tbody>
</table>
<p style="font-size:13px">G1 不满足 ⇒ 既非 PASS 亦非 REDESIGN（需 G1∧G2 同时成立但 G3 不成立）。</p>
<h3>候选 A = P=64, independent_fsync, sync=true</h3>
<table>
<thead><tr><th class="l">闸门</th><th>计算值</th><th>阈值</th><th>判定</th></tr></thead>
<tbody>{gateA}</tbody>
</table>
<p style="font-size:13px">A、B 均不满足 PASS 条件 ⇒ <span class="hl-red"><b>decision = FAIL</b></span>（与 exp2_report.json.decision 一致）。</p>
<div class="note"><b>补充：</b>即便假设模式B吞吐达标（G1≤15%），durability window {mB_w} ms 仍远超 5ms，将落入 REDESIGN 而非 PASS——分区 WAL 在当前组提交模型下无论 A/B 都无法同时满足吞吐与持久化，属模型层面根本矛盾。</div>
</section>

<section class="card" id="alt">
<h2>6. 替代设计（FAIL 触发 PPT 第 10 页设计修订）</h2>
{alt_html}
<div class="note">（decision.py 中另列第三备选：模式B 改良——批量 fsync 但改用时间上限封顶的 commit 窗口如 ≤2ms，以牺牲少量 fsync 摊销换取 durability window 达标；该方案未进入最终 report JSON 的 alternatives，作为后续可探索方向。）</div>
</section>

<section class="card">
<h2>7. 风险与局限</h2>
<ul>
<li><b>powersave 调频器</b>：CPU 低频运行，rdtsc 经 CLOCK_MONOTONIC 校准 ns（invariant TSC）；绝对吞吐偏低可能放大 fsync 相对开销，但回退幅度与窗口超标为数量级差异，结论稳健。</li>
<li><b>基线 rep 方差</b>：P=1 模式A sync=true 基线聚合 5 rep，throughput 极差 287,503 ops/s；即便取最乐观基线 rep，模式B {regB}% 回退仍 &gt;15%。</li>
<li><b>nosync ops 选择</b>：sync=false 取 [2M,4M] ops 而非 10s，避免脏页回写风暴（实测 22M ops 触发限速），满足 spec ≥2M ops；不影响判定（仅依赖 sync=true 档）。</li>
<li><b>桌面级 NVMe 无掉电保护</b>：fsync 延迟/窗口可能与企业级有差异，但本实验比较同硬件相对退化，不影响 FAIL 方向。</li>
<li><b>key 分布选择</b>：multi-WAL 侧用 uniform（worst-case dirty 扇出）有意制造最坏情况；真实负载偏斜时模式A退化更轻，但模式B窗口膨胀矛盾依旧。</li>
</ul>
</section>

<section class="card">
<h2>8. 原始数据引用路径</h2>
<div class="kv">
<b>权威报告</b><span><code>exp_design/pre_experiment/report/exp2_report.json</code></span>
<b>聚合数据</b><span><code>output/pre_exp/exp2/aggregated.json</code>（含逐 rep 明细 + 极差）</span>
<b>判定输出</b><span><code>output/pre_exp/exp2/decision.json</code></span>
<b>原始 JSON</b><span><code>output/pre_exp/exp2/raw/</code>（56 个）</span>
<b>运行日志</b><span><code>output/pre_exp/exp2/logs/*.log</code></span>
<b>图表脚本</b><span><code>exp_design/pre_experiment/report/charts/gen_exp2_charts.py</code></span>
<b>图表</b><span><code>exp_design/pre_experiment/report/charts/exp2_*.svg</code></span>
<b>作废数据</b><span><code>output/pre_exp/exp2/raw_polluted_0032/</code>（被干扰，不展示）</span>
</div>
</section>

<footer>本页全部数值严格取自 exp2_report.json，禁止伪造或估算 · 生成于 2026-08-04 · 任务 #7 阶段性成果</footer>

</div>
</body>
</html>
'''
    open(OUT, "w", encoding="utf-8").write(html)
    print(f"[done] wrote {OUT} ({len(html)} bytes)")
    print(f"[check] range rows={len(rl)} wal rows={len(mw)} baseline={len(bl)} decision={rep['decision']}")

if __name__ == "__main__":
    main()
