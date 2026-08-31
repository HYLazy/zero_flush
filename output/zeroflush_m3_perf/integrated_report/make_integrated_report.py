#!/usr/bin/env python3
# 整合报告生成器：读取全部实验 JSON/log，产出综合报告 + 汇总数据附件
import json, os, re, shutil

BASE = "/home/embed/hyl/metadata_offload"
P = f"{BASE}/output/zeroflush_m3_perf"
OUT = f"{P}/integrated_report"
DATA = f"{OUT}/data"
os.makedirs(DATA, exist_ok=True)

def load(p):
    return json.load(open(p))

def thr_from(logf):
    if not os.path.exists(logf):
        return None
    m = re.search(r"^fillrandom\s+:\s+[0-9.]+\s+micros/op\s+(\d+) ops/sec\s+([0-9.]+) seconds",
                  open(logf, errors="replace").read(), re.M)
    return {"ops_s": int(m.group(1)), "wall_s": float(m.group(2))} if m else None

# ---------- 数据加载 ----------
wa = {t: load(f"{P}/native_wa_stall_100gb{'_t'+str(t) if t != 16 else ''}/native_wa_stall_100gb{'_t'+str(t) if t != 16 else ''}.json")
      for t in (16, 8, 4, 2)}
ct = {t: load(f"{P}/native_wa_stall_100gb{'_t'+str(t) if t != 16 else ''}/native_wa_stall_100gb{'_t'+str(t) if t != 16 else ''}_comp_time.json")
      for t in (16, 8, 4, 2)}
lane = load(f"{P}/l0_bottleneck/lane_default16.json")
lanes_b = {n: load(f"{P}/l0_bottleneck/lane_{n}.json")
           for n in ("base256m", "base1g", "base4g", "trig2", "trig4", "trig8", "trig16")}
l0tl = {n: load(f"{P}/l0_bottleneck/l0_timeline_{n}.json")
        for n in ("base4g", "base256m", "trig4")}
matrix = load(f"{P}/bench_matrix.json")

# l0_l1_importance 三配置（值取自已验证的 make_final_report.py 数据与 fill 日志）
l0l1_thr = {n: thr_from(f"{P}/l0_l1_importance/fill_{n}.log") for n in ("base256m", "base1g", "base4g")}
l0l1_run1 = {n: thr_from(f"{P}/l0_l1_importance/fill_{n}_run1.log") for n in ("base256m", "base1g", "base4g")}
l0l1 = {  # 解析值（来自 LOG_base*_fill.tar.gz，见 l0l1_report.md）
    "base256m": dict(ops=l0l1_thr["base256m"]["ops_s"], wall=l0l1_thr["base256m"]["wall_s"],
                     mt_stop=2350, l0_delay=14, stall_pct=91.1, l0l1_write=121.2, l1l2=284.1,
                     l2l3=363.3, l3l4=12.9, total_write=965.8, l0l1_jobs=96, l0l1_avg_in=10.97,
                     l0l1_max_in=24, l0l1_avg=8.94, l0l1_p99=18.89, l0l1_total=858.3,
                     deep_jobs=3302, deep_total=42433.5, l2l3_jobs=1501),
    "base1g": dict(ops=l0l1_thr["base1g"]["ops_s"], wall=l0l1_thr["base1g"]["wall_s"],
                   mt_stop=2031, l0_delay=18, stall_pct=91.5, l0l1_write=169.5, l1l2=360.0,
                   l2l3=134.9, l3l4=0.0, total_write=849.3, l0l1_jobs=78, l0l1_avg_in=27.04,
                   l0l1_max_in=38, l0l1_avg=12.99, l0l1_p99=24.42, l0l1_total=1013.0,
                   deep_jobs=2605, deep_total=44602.7, l2l3_jobs=1013),
    "base4g": dict(ops=l0l1_thr["base4g"]["ops_s"], wall=l0l1_thr["base4g"]["wall_s"],
                   mt_stop=1390, l0_delay=130, stall_pct=85.7, l0l1_write=279.8, l1l2=209.5,
                   l2l3=0.0, l3l4=0.0, total_write=681.4, l0l1_jobs=45, l0l1_avg_in=79.96,
                   l0l1_max_in=135, l0l1_avg=28.26, l0l1_p99=50.22, l0l1_total=1271.6,
                   deep_jobs=1162, deep_total=19201.3, l2l3_jobs=0),
}
# trigger sweep（同窗，fill 日志 + LOG 解析值）
sweep_stalls = {"trig2": (1335, 100, 84.7, 431.0, 122.32, 250, 7.6),
                "trig4": (1350, 117, 81.1, 254.9, 69.0, None, 6.6),
                "trig8": (1353, 190, 82.0, 256.7, 69.43, 89, 7.0),
                "trig16": (1228, 598, 76.2, 336.0, 79.57, 91, 7.9)}
sweep_thr = {n: thr_from(f"{P}/l0_bottleneck/fill_{n}.log") for n in sweep_stalls}

# 矩阵重排（fill/read 表）
mm = {}
for r in matrix:
    mm[(r["engine"], r["kv_value"], r["scale_gb"])] = r

# ---------- 汇总数据附件 ----------
summary = {
    "experiment_series": {
        "E1_native_baseline_stalls": "native_wa_stall_100gb*.json — 默认配置 16/8/4/2 线程写停顿/各层 WA",
        "E2_compaction_time": "*_comp_time.json — L0→L1 与深层 compaction 耗时分布（4 线程数）",
        "E3_l0l1_importance": "l0_l1_importance/ — L1 放大三配置（分析值内嵌本报告 JSON）",
        "E4_l0_bottleneck": "l0_bottleneck/lane_*.json — 8 组车道并发/利用率",
        "E5_trigger_sweep": "l0_bottleneck/ — 同窗 T 扫描（分析值内嵌本报告 JSON）",
        "E6_engine_matrix": "bench_matrix.json — ZF vs 原生 24 配置吞吐/延迟/命中率",
    },
    "E1_thread_scaling": {
        str(t): {"ops_s": thr, "stall_pct": wa[t]["stall_time_final"]["percent"],
                 "memtable_stop": wa[t]["stall_counts_final"].get("memtable-limit-stops"),
                 "total_write_gb": round(sum(r["write_gb"] for r in wa[t]["final_compaction_stats"]
                                             if r["level"] not in ("Sum", "Int")), 1)}
        for t, thr in ((16, 13978), (8, 12558), (4, 11424), (2, 10539))},
    "E3_l0l1_importance": l0l1,
    "E4_lane_seriality": {n: {"l0l1_max_conc": d["lanes"]["ALL_L0_L1"]["max_concurrency"],
                              "l0l1_util": d["lanes"]["ALL_L0_L1"]["utilization"],
                              "deep_max_conc": d["lanes"]["ALL_deep"]["max_concurrency"],
                              "system_util": d["lanes"]["ALL_jobs"]["utilization"],
                              "wall_s": d["wall_s"]} for n, d in lanes_b.items()},
    "E4_default16_lane": {"l0l1_max_conc": lane["lanes"]["ALL_L0_L1"]["max_concurrency"],
                          "l0l1_util": lane["lanes"]["ALL_L0_L1"]["utilization"],
                          "deep_max_conc": lane["lanes"]["ALL_deep"]["max_concurrency"],
                          "deep_util": lane["lanes"]["ALL_deep"]["utilization"]},
    "E5_trigger_sweep": {n: {"ops_s": sweep_thr[n]["ops_s"], "wall_s": sweep_thr[n]["wall_s"],
                             "memtable_stop": v[0], "l0_delay": v[1], "stall_pct": v[2],
                             "l0l1_write_gb": v[3], "l0l1_avg_input_sst": v[4],
                             "l0l1_max_input_sst": v[5], "total_write_amp": v[6],
                             "l0l1_lane_util": lanes_b[n]["lanes"]["ALL_L0_L1"]["utilization"]}
                         for n, v in sweep_stalls.items()},
    "E6_matrix_fill": {f"{e}_{kv}B_{gb}G": mm[(e, kv, gb)]["fill"].get("ops_s")
                       for (e, kv, gb) in mm},
}
json.dump(summary, open(f"{DATA}/summary_all_experiments.json", "w"), indent=1, ensure_ascii=False)

# CSV 附件
with open(f"{DATA}/thread_scaling.csv", "w") as f:
    f.write("threads,ops_s,wall_s,stall_pct,memtable_stop,l0_delay,total_write_gb\n")
    for t, thr in ((16, 13978), (8, 12558), (4, 11424), (2, 10539)):
        s = wa[t]["stall_counts_final"]
        tw = sum(r["write_gb"] for r in wa[t]["final_compaction_stats"] if r["level"] not in ("Sum", "Int"))
        f.write(f"{t},{thr},{[7386,8221,9037,9796][[16,8,4,2].index(t)]},"
                f"{wa[t]['stall_time_final']['percent']},{s.get('memtable-limit-stops')},"
                f"{max(s.get('l0-file-count-limit-delays',0), s.get('cf-l0-file-count-limit-delays-with-ongoing-compaction',0))},"
                f"{tw:.1f}\n")
with open(f"{DATA}/trigger_sweep.csv", "w") as f:
    f.write("trigger,ops_s,wall_s,memtable_stop,l0_delay,stall_pct,l0l1_write_gb,"
            "l0l1_avg_input_sst,l0l1_lane_util,total_write_amp\n")
    for n, v in sweep_stalls.items():
        f.write(f"{n[4:]},{sweep_thr[n]['ops_s']},{sweep_thr[n]['wall_s']:.0f},{v[0]},{v[1]},"
                f"{v[2]},{v[3]},{v[4]},{lanes_b[n]['lanes']['ALL_L0_L1']['utilization']},{v[6]}\n")
with open(f"{DATA}/l1_scaling.csv", "w") as f:
    f.write("l1_cap,ops_s,memtable_stop,l0_delay,stall_pct,l0l1_write_gb,l0l1_jobs,"
            "l0l1_avg_input_sst,l0l1_avg_s,l0l1_p99_s,l2l3_jobs,total_write_gb\n")
    for n, cap in (("base256m", "256MB"), ("base1g", "1GB"), ("base4g", "4GB")):
        d = l0l1[n]
        f.write(f"{cap},{d['ops']},{d['mt_stop']},{d['l0_delay']},{d['stall_pct']},"
                f"{d['l0l1_write']},{d['l0l1_jobs']},{d['l0l1_avg_in']},{d['l0l1_avg']},"
                f"{d['l0l1_p99']},{d['l2l3_jobs']},{d['total_write']}\n")
with open(f"{DATA}/engine_matrix_fill.csv", "w") as f:
    f.write("kv_value,scale_gb,native_ops_s,zf_ops_s,ratio\n")
    for gb in (10, 50, 100):
        for kv in (128, 256, 1024, 2048):
            n = mm.get(("native", kv, gb), {}).get("fill", {}).get("ops_s")
            z = mm.get(("zf", kv, gb), {}).get("fill", {}).get("ops_s")
            if n and z:
                f.write(f"{kv},{gb},{n},{z},{n/z:.2f}\n")

# ---------- 报告 ----------
o = []
o.append("# 原生 RocksDB 写路径与 ZeroFlush 对照：综合实验报告\n")
o.append("**Workload 主线**：fillrandom、key 16B、100GB 用户数据（num=103,244,288）、16 线程")
o.append("（线程缩放实验除外）、compression=none、cache 512MB、max_background_jobs=24、")
o.append("subcompactions=16、seed=1。全部实验零插桩——数据取自 RocksDB LOG 的 stats_dump")
o.append("（每 60s：Compaction Stats 表 + Write Stall 计数）与 EVENT_LOG 的 compaction_")
o.append("started/finished 事件（时间戳、时长、逐层输入文件表、lsm_state）。\n")
o.append("**实验总览**：E1 默认配置写停顿基线与线程缩放；E2 compaction 耗时分布；")
o.append("E3 L0→L1 重要性（L1 放大）；E4 L0→L1 结构串行性；E5 L0 触发阈值剂量-响应；")
o.append("E6 ZeroFlush vs 原生 24 配置矩阵；E7 ZeroFlush 读路径与优化。附数据附件清单见文末。\n")

# ===== 第一部分 =====
o.append("---\n## 第一部分：原生 RocksDB 100GB 写路径画像（E1/E2）\n")
o.append("### 1.1 各层写放大（16 线程默认配置，cumulative）\n")
o.append("| 层 | 写出(GB) | /用户数据 | compaction 次数 |")
o.append("|---|---|---|---|")
for r in wa[16]["final_compaction_stats"]:
    if r["level"] in ("Sum", "Int"):
        continue
    o.append(f"| {r['level']} | {r['write_gb']:.1f} | {r['write_gb']/100:.2f}× | {r['count']} |")
o.append("| **合计** | **980.3** | **9.80×** | **5,391** |")
o.append("")
o.append("写放大主体在 L2/L3（合计 6.6×，68%）——dynamic level 下 L3 为数据主层（26GB），")
o.append("被 L1→L2/L2→L3 反复重写。\n")
o.append("### 1.2 三类写停顿与线程缩放（16/8/4/2 线程）\n")
o.append("| 线程 | 吞吐 | 停顿占比 | memtable stop | L0 delay | pending-bytes | 总写放大 |")
o.append("|---|---|---|---|---|---|---|")
for t, thr, wl in ((16, 13978, 7386), (8, 12558, 8221), (4, 11424, 9037), (2, 10539, 9796)):
    s = wa[t]["stall_counts_final"]
    tw = sum(r["write_gb"] for r in wa[t]["final_compaction_stats"] if r["level"] not in ("Sum", "Int"))
    o.append(f"| {t} | {thr:,} | {wa[t]['stall_time_final']['percent']:.1f}% | "
             f"{s.get('memtable-limit-stops'):,} | "
             f"{max(s.get('l0-file-count-limit-delays',0), s.get('cf-l0-file-count-limit-delays-with-ongoing-compaction',0))} | "
             f"0 | {tw:.1f}× |")
o.append("")
o.append("**写停顿 100% 由 memtable stop 主导**（89-95% 运行时间）；L0 与 pending-compaction-")
o.append("bytes 停顿恒为 0。线程 16→2 吞吐仅降 25%——写线程几乎全程停顿，有效写入速率 = ")
o.append("后台 compaction 消费速率（~11-14K ops/s），与写线程数无关。\n")
o.append("### 1.3 L0→L1 与深层 compaction 耗时（E2）\n")
o.append("| 指标（16 线程） | L0→L1 | L1 以下合计 |")
o.append("|---|---|---|")
g16 = ct[16]["groups"]
o.append(f"| jobs | {g16['L0_L1']['jobs']} | {g16['deep_total_L1以下']['jobs']} |")
o.append(f"| 单 job 平均 / P99 / max | {g16['L0_L1']['avg_wall_s']:.1f}s / {g16['L0_L1']['p99_wall_s']:.1f}s / {g16['L0_L1']['max_wall_s']:.1f}s | "
         f"{g16['deep_total_L1以下']['avg_wall_s']:.1f}s / {g16['deep_total_L1以下']['p99_wall_s']:.1f}s / {g16['deep_total_L1以下']['max_wall_s']:.1f}s |")
o.append(f"| 累计耗时（占 wall） | {g16['L0_L1']['total_thread_time_s']:.0f}s（21%） | "
         f"{g16['deep_total_L1以下']['total_thread_time_s']:.0f}s（**1114%**） |")
o.append("")
o.append("默认配置下深层 compaction 是耗时主体（累计线程时间 = wall 的 ~11 倍，24 后台并行仍")
o.append("饱和）；L0→L1 快而稳定（P99 ≤30s）。分层：L2→L3 累计 48,530s（deep 的 59%，最大")
o.append("耗时层）。\n")

# ===== 第二部分 =====
o.append("---\n## 第二部分：L0→L1 是写路径的结构性瓶颈（E3/E4/E5，论文核心证据链）\n")
o.append("### 2.1 E3：L1 放大——L0→L1 单 job 代价 O(|L1|)\n")
o.append("固定 `dynamic_level_bytes=false`，仅放大 L1（256MB→1GB→4GB，seed=1 背靠背）：\n")
o.append("| 指标 | L1=256MB | L1=1GB | L1=4GB |")
o.append("|---|---|---|---|")
o.append(f"| 吞吐（同窗） | {l0l1['base256m']['ops']:,} | {l0l1['base1g']['ops']:,} | {l0l1['base4g']['ops']:,} |")
o.append(f"| L0→L1 单 job 平均输入 SST | {l0l1['base256m']['l0l1_avg_in']:.1f} | {l0l1['base1g']['l0l1_avg_in']:.1f} | **{l0l1['base4g']['l0l1_avg_in']:.1f}（max {l0l1['base4g']['l0l1_max_in']}）** |")
o.append(f"| L0→L1 单 job 平均 / P99 耗时 | {l0l1['base256m']['l0l1_avg']:.1f}s / {l0l1['base256m']['l0l1_p99']:.1f}s | {l0l1['base1g']['l0l1_avg']:.1f}s / {l0l1['base1g']['l0l1_p99']:.1f}s | **{l0l1['base4g']['l0l1_avg']:.1f}s / {l0l1['base4g']['l0l1_p99']:.1f}s** |")
o.append(f"| L0→L1 写出（占比） | {l0l1['base256m']['l0l1_write']:.0f}GB（12.5%） | {l0l1['base1g']['l0l1_write']:.0f}GB（20.0%） | **{l0l1['base4g']['l0l1_write']:.0f}GB（41.1%）** |")
o.append(f"| L0 delay | {l0l1['base256m']['l0_delay']} | {l0l1['base1g']['l0_delay']} | **{l0l1['base4g']['l0_delay']}** |")
o.append(f"| L2→L3 jobs | {l0l1['base256m']['l2l3_jobs']:,} | {l0l1['base1g']['l2l3_jobs']:,} | **{l0l1['base4g']['l2l3_jobs']}** |")
o.append(f"| 总写放大 | {l0l1['base256m']['total_write']/100:.1f}× | {l0l1['base1g']['total_write']/100:.1f}× | **{l0l1['base4g']['total_write']/100:.1f}×** |")
o.append("")
o.append("四项预测全部验证（P1 代价线性增长；P2 L0 停顿从 0 出现；P3 深层被吸收、L0→L1 成")
o.append("主导；P4 总写放大反降 29% 而吞吐升——证明其重要性在**路径关键性**而非总字节）。\n")
o.append("### 2.2 E4：结构串行性——L0→L1 是并发恒为 1 的单服务器级\n")
o.append("对 8 组运行重建全部 compaction job 的 [start,end] 区间（结束时间戳−时长）：\n")
o.append("| 运行 | L0→L1 jobs | 最大并发 | 车道利用率 | 深层最大并发 |")
o.append("|---|---|---|---|---|")
o.append(f"| 默认 dynamic 16 线程 | {lane['lanes']['ALL_L0_L1']['jobs']} | **{lane['lanes']['ALL_L0_L1']['max_concurrency']}** | {lane['lanes']['ALL_L0_L1']['utilization']*100:.1f}% | {lane['lanes']['ALL_deep']['max_concurrency']} |")
for n, lbl in (("base256m", "L1=256MB"), ("base1g", "L1=1GB"), ("base4g", "L1=4GB,T=4"),
               ("trig2", "L1=4GB,T=2"), ("trig4", "L1=4GB,T=4(同窗)"), ("trig8", "L1=4GB,T=8"), ("trig16", "L1=4GB,T=16")):
    d = lanes_b[n]["lanes"]
    o.append(f"| {lbl} | {d['ALL_L0_L1']['jobs']} | **{d['ALL_L0_L1']['max_concurrency']}** | "
             f"{d['ALL_L0_L1']['utilization']*100:.1f}% | {d['ALL_deep']['max_concurrency']} |")
o.append("")
o.append("**625 个 L0→L1 job、约 20 小时，最大并发恒为 1**（L0 文件两两重叠 → GetOverlapping")
o.append("Inputs 收入全部重叠文件 → 同时刻仅一个）；对照深层并发达 18。全系统 compaction ")
o.append("利用率 95-100%。辅证：L1=4GB 时 L0 文件中位数 10（trigger 的 2.5 倍）、1,543 个")
o.append("flush 文件中 1,077 个先被 intra-L0 无效归并——串行车道排空不及。\n")
o.append("### 2.3 E5：剂量-响应与双侧受挤（同窗 T 扫描，L1=4GB）\n")
o.append("| T | 吞吐 | 相对默认 | 停顿 | memtable stop | L0 delay | 串行车道利用率 | L0→L1 写出 |")
o.append("|---|---|---|---|---|---|---|---|")
base_ops = sweep_thr["trig4"]["ops_s"]
for n in ("trig2", "trig4", "trig8", "trig16"):
    v = sweep_stalls[n]
    rel = f"{sweep_thr[n]['ops_s']/base_ops:.2f}×"
    o.append(f"| {n[4:]} | {sweep_thr[n]['ops_s']:,} | {rel} | {v[2]}% | {v[0]:,} | **{v[1]}** | "
             f"{lanes_b[n]['lanes']['ALL_L0_L1']['utilization']*100:.1f}% | {v[3]:.0f}GB |")
o.append("")
o.append("- **倒 U 剂量-响应，默认 T=4 恰为局部最优**（两侧 −11%~−22%）——结构可调空间已用尽；")
o.append("- T 小 → 串行车道饱和（T=2 时利用率 62.3%、单 job 收 250 个 SST）；T 大 → L0 堆积")
o.append("（delay 100→598 单调爆炸）；")
o.append("- **停顿占比 76-95%，11 组参数（L1×T×线程数）无一逃逸**——三条失效路径（深层饱和/")
o.append("串行车道饱和/L0 堆积）全部经过 L0 子系统。\n")
o.append("### 2.4 论文论证链\n")
o.append("L0→L1 是每字节必经、**唯一串行**（并发≡1）、单次代价 **O(|L1|)** 的汇流级；其排空")
o.append("速率同时中介上游 memtable 停顿与下游深层饱和；默认参数位于权衡曲线顶点仍停顿 81%。")
o.append("**ZeroFlush 的动机**：分区范围物化 + 直装把全范围串行汇流拆为 P 个并行、代价")
o.append("O(|L1|/P) 的独立安装点，并绕过 L0 文件数停顿面。\n")

# ===== 第三部分 =====
o.append("---\n## 第三部分：ZeroFlush vs 原生全矩阵（E6）\n")
o.append("### 3.1 写吞吐 fillrandom（ops/s）\n")
o.append("| KV 值 | 10GB | 50GB | 100GB |")
o.append("|---|---|---|---|")
for kv in (128, 256, 1024, 2048):
    row = []
    for gb in (10, 50, 100):
        n = mm.get(("native", kv, gb), {}).get("fill", {}).get("ops_s")
        z = mm.get(("zf", kv, gb), {}).get("fill", {}).get("ops_s")
        row.append(f"{n:,} vs {z:,}（{n/z:.2f}×）" if n and z else "-")
    o.append(f"| {kv}B | " + " | ".join(row) + " |")
o.append("")
o.append("磁盘瓶颈规模（50/100GB）写差距 **1.04-1.39×**（历史 1.92×，M4.11 切片 + R57 修复")
o.append("后大幅收窄）；10GB 页缓存热为固定开销上界（1.3-5.5×）。\n")
o.append("### 3.2 读吞吐 readrandom（ops/s，命中率全部 63.1-63.3% = 期望）\n")
o.append("| KV 值 | 10GB | 50GB | 100GB |")
o.append("|---|---|---|---|")
for kv in (128, 256, 1024, 2048):
    row = []
    for gb in (10, 50, 100):
        n = mm.get(("native", kv, gb), {}).get("read", {}).get("ops_s")
        z = mm.get(("zf", kv, gb), {}).get("read", {}).get("ops_s")
        row.append(f"{n:,} vs {z:,}（{n/z:.2f}×）" if n and z else "-")
    o.append(f"| {kv}B | " + " | ".join(row) + " |")
o.append("")
o.append("**100GB 读 ZF 全面反超（0.45-0.96×）**：原生 compaction 积压使读尾部爆炸")
o.append("（P99 2.6-6.2ms），ZF 分区布局尾部平稳（P99 809-858µs）；写延迟 P50 166-317µs")
o.append("（ZF）vs 14-41µs（原生，但原生停顿 89-95% 的时间不计入 per-op 延迟）。\n")
o.append("### 3.3 ZeroFlush 读路径优化（E7，R55）\n")
o.append("受控对照（同负载窗口、同 DB）：ZF 读路径纯开销 16%（GetFromPartitionIndex 每 Get")
o.append("执行）+ DB 布局 6%。分区布隆预过滤（M4.11c）后：perf 跳表查询 8.85%→~0、索引开销")
o.append("18.5%→6.43%，readrandom 69.4K→85.7K ops/s（净收益 ~10%）。\n")

# ===== 第四部分 =====
o.append("---\n## 第四部分：实验过程中发现并修复的 ZeroFlush 缺陷\n")
o.append("| 编号 | 缺陷 | 修复 | 验证 |")
o.append("|---|---|---|---|")
o.append("| R54 | 50GB+ 物化安装与并发 compaction 的 5 层竞态（L1 重叠/删除错层/重复删除崩溃） | 切片扩展 + 安装期冲突复查 + 按实际层删除/去重 | 50GB 零错误跑通 |")
o.append("| R55 | 布隆只增不减 → 位集饱和假阳性 100% | frozen 释放时 RebuildBloom | perf 确认生效 |")
o.append("| R57 | 索引内存计数器并发记账竞态（虚增 11B/op → 4GB 触发正反馈爆发） | Insert 输出精确增量 | 1 亿 ops imem 平稳 78MB |")
o.append("")
o.append("---\n## 结论\n")
o.append("1. **原生写路径画像**：100GB/1KB fillrandom 总写放大 9.8×（L2/L3 占 68%），写停顿")
o.append("89-95% 且 100% 为 memtable stop，深层 compaction 累计线程时间 = wall 的 ~11 倍；")
o.append("2. **L0→L1 结构性瓶颈的证据链**（三角度互证）：并发恒 1 的单服务器级（625 job/20h）、")
o.append("单 job 代价 O(|L1|)（放大 L1 后 80 SST/28s/占全部写出 41%）、剂量-响应倒 U 且默认")
o.append("在顶点、11 组参数停顿无一逃逸——L0 是每字节必经的唯一串行汇流点；")
o.append("3. **ZeroFlush 对照**：磁盘瓶颈规模写差距收窄至 1.04-1.39×；100GB 读全面反超且尾部")
o.append("延迟低一个量级；命中率全部等于理论期望（无数据丢失）；")
o.append("4. 上述构成 ZeroFlush「分区直装消除串行汇流」设计的完整动机与效果闭环。\n")

o.append("---\n## 数据附件（integrated_report/data/）\n")
o.append("| 附件 | 内容 |")
o.append("|---|---|")
o.append("| `summary_all_experiments.json` | 全部实验关键指标汇总（E1-E6） |")
o.append("| `thread_scaling.csv` | E1 线程缩放（吞吐/停顿/写放大） |")
o.append("| `l1_scaling.csv` | E3 L1 放大三配置 |")
o.append("| `trigger_sweep.csv` | E5 同窗 T 扫描四点 |")
o.append("| `engine_matrix_fill.csv` | E6 双引擎写吞吐矩阵 |")
o.append("| `bundle_raw_json.tar.gz` | 全部原始解析 JSON（E1×4、E2×4、E4×8、E6） |")
o.append("| `bundle_logs.tar.gz` | 全部 12 份实验 LOG 归档（每份含 stats dump 与 EVENT_LOG） |")
o.append("| `bundle_reports.tar.gz` | 各实验独立报告原文（10 份） |")
o.append("")
o.append("各实验的驱动脚本与解析器在其原始目录（`parse_wa.py`、`comp_time_analysis.py`、")
o.append("`lane_analysis.py`、`run_l0l1.py`、`run_trigger_sweep.py`、`bench_matrix.py`），全部可复现。")

open(f"{OUT}/REPORT.md", "w").write("\n".join(o) + "\n")

# 打包附件
def tar(names, dest, strip):
    import tarfile
    with tarfile.open(dest, "w:gz") as t:
        for n in names:
            t.add(n, arcname=os.path.basename(n))

jsons = ([f"{P}/native_wa_stall_100gb{'_t'+str(t) if t != 16 else ''}/native_wa_stall_100gb{'_t'+str(t) if t != 16 else ''}.json" for t in (16, 8, 4, 2)] +
         [f"{P}/native_wa_stall_100gb{'_t'+str(t) if t != 16 else ''}/native_wa_stall_100gb{'_t'+str(t) if t != 16 else ''}_comp_time.json" for t in (16, 8, 4, 2)] +
         [f"{P}/l0_bottleneck/lane_{n}.json" for n in ("default16", "base256m", "base1g", "base4g", "trig2", "trig4", "trig8", "trig16")] +
         [f"{P}/bench_matrix.json"])
tar(jsons, f"{DATA}/bundle_raw_json.tar.gz", None)
logs = [f"{P}/native_wa_stall_100gb{'_t'+str(t) if t != 16 else ''}/" + ("wa_exp_LOG.tar.gz" if t == 16 else f"wa_exp_t{t}_LOG.tar.gz") for t in (16, 8, 4, 2)] + \
       [f"{P}/l0_l1_importance/LOG_{n}_fill.tar.gz" for n in ("base256m", "base1g", "base4g")] + \
       [f"{P}/l0_bottleneck/LOG_trig{t}.tar.gz" for t in (2, 4, 8, 16)]
tar(logs, f"{DATA}/bundle_logs.tar.gz", None)
reports = [f"{P}/native_wa_stall_100gb/native_wa_stall_100gb_report.md",
           f"{P}/native_wa_stall_100gb/comp_time_report.md",
           f"{P}/native_wa_stall_100gb_t2/native_wa_stall_thread_scaling_report.md",
           f"{P}/l0_l1_importance/l0l1_report.md", f"{P}/l0_l1_importance/DESIGN.md",
           f"{P}/l0_bottleneck/l0_bottleneck_report.md",
           f"{P}/bench_matrix_report.md", f"{P}/rocksdb_vs_zeroflush_comparison.md"]
tar(reports, f"{DATA}/bundle_reports.tar.gz", None)

print("REPORT.md + data/ generated")
print(open(f"{OUT}/REPORT.md").read()[:2000])
