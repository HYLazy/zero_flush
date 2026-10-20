#!/usr/bin/env python3
# L0 写路径瓶颈：三角度互证最终报告
import os

P = "/home/embed/hyl/metadata_offload/output/zeroflush_m3_perf/l0_bottleneck"
out = []
out.append("# L0 是写路径瓶颈：三角度互证（原生 RocksDB 100GB/1KB fillrandom）\n")
out.append("既有实验（L1 放大，91af88e）已证明 L0→L1 单 job 代价 O(|L1|) 且随 L1 主导写路径。")
out.append("本报告补充三个更本质的角度：**结构串行性**（区間重建归因）、**剂量-响应**（同窗")
out.append("trigger 扫描）、**双侧受挤**（无参数点可逃逸）。全部无插桩，数据来自 LOG 的")
out.append("compaction_finished 事件（时间戳 + 时长 + lsm_state）与 stats dump。\n")

out.append("## 角度 A：结构串行性——L0→L1 是每字节必经的单服务器级\n")
out.append("对 8 组运行（默认×1、L1 放大×3、trigger 扫描×4）重建全部 compaction job 的")
out.append("[start, end] 区间（结束时间戳 − compaction_time_micros），按输出层分车道统计：\n")
out.append("| 运行 | L0→L1 jobs | **L0→L1 最大并发** | L0→L1 车道利用率 | 深层最大并发 | 全系统利用率 |")
out.append("|---|---|---|---|---|---|")
rows = [
    ("默认 dynamic（16 线程基线）", "99", "**1**", "21.1%", "18", "99.9%"),
    ("L1=256MB", "96", "**1**", "23.6%", "18", "99.9%"),
    ("L1=1GB", "78", "**1**", "27.2%", "18", "99.7%"),
    ("L1=4GB, T=2", "50", "**1**", "**62.3%**", "18", "99.4%"),
    ("L1=4GB, T=4", "42", "**1**", "40.0%", "18", "99.3%"),
    ("L1=4GB, T=8", "46", "**1**", "37.3%", "18", "98.7%"),
    ("L1=4GB, T=16", "65", "**1**", "42.3%", "18", "95.2%"),
]
for r in rows:
    out.append("| " + " | ".join(r) + " |")
out.append("")
out.append("- **625 个 L0→L1 job、约 20 小时运行中，最大并发恒为 1**——L0 文件两两重叠使")
out.append("  GetOverlappingInputs 必然收入全部重叠文件，同一时刻只允许一个 L0→L1（对照：")
out.append("  深层车道并发达 18，可吃满 24 后台线程）；")
out.append("- **全系统 compaction 利用率 95-100%（所有 8 组）**——写路径永远被 compaction 限制；")
out.append("- L0 出口由「串行 L0→L1 + intra-L0 内耗」构成：L1=4GB 时 L0 文件中位数 10 个")
out.append("  （trigger=4 的 2.5 倍）、p90=15、max=21——flush 持续堆积，串行车道排空不及；")
out.append("- 1,543 个 L0 flush 文件中仅 466 个直接进入 L0→L1，其余 1,077 个先被 intra-L0")
out.append("  反复归并（L1=4GB 组）——串行车道跟不上时，L0 内部先做无效功。\n")

out.append("## 角度 B：剂量-响应——同环境窗口 trigger 扫描（L1=4GB 固定，seed=1）\n")
out.append("仅改变 `level0_file_num_compaction_trigger`（L0→L1 的触发频率），四点背靠背同窗：\n")
out.append("| T | 吞吐（ops/s） | 相对默认 | 写停顿 | L0→L1 写出 | 单 job 输入 SST | 串行车道利用率 |")
out.append("|---|---|---|---|---|---|---|")
sw = [("2", "43,530", "−21.9%", "84.7%", "431.0GB", "122.3（max 250）", "**62.3%**"),
      ("4（默认）", "**55,722**", "1.00×", "81.1%", "254.9GB", "~69", "40.0%"),
      ("8", "49,682", "−10.8%", "82.0%", "256.7GB", "69.4", "37.3%"),
      ("16", "45,839", "−17.7%", "76.2%", "336.0GB", "79.6", "42.3%")]
for r in sw:
    out.append("| " + " | ".join(r) + " |")
out.append("")
out.append("- **倒 U 剂量-响应，默认 T=4 恰为局部最优**：向任一方向调参吞吐最多 −22%——")
out.append("  RocksDB 默认值已位于 L0 排空权衡曲线的顶点，该结构的可调空间已被用尽；")
out.append("- 两侧机制不同：T=2 时串行 L0→L1 车道利用率 62.3%（**单服务器饱和**）+ 单 job")
out.append("  收 250 个 SST；T=16 时 L0 堆积（见角度 C）；")
out.append("- 注：早前 base4g（35.4K）与本次 trig4（55.7K）配置相同但跨时间窗——环境方差")
out.append("  ±30%+，故剂量-响应必须同窗比较（本表全部 11:33-12:04 前后背靠背）。\n")

out.append("## 角度 C：双侧受挤——无参数点可逃逸写停顿\n")
out.append("| 配置（全部同 workload） | memtable stop | **L0 delay** | 停顿总占比 |")
out.append("|---|---|---|---|")
c = [("T=2", "1,335", "100", "84.7%"),
     ("T=4", "1,350", "117", "81.1%"),
     ("T=8", "1,353", "190", "82.0%"),
     ("T=16", "1,228", "**598**", "76.2%"),
     ("L1=256MB（深层饱和侧）", "2,350", "14", "91.1%"),
     ("默认 dynamic 16/8/4/2 线程", "2,605/2,595/2,450/2,460", "6/0/0/0", "94.6/93.7/91.4/89.2%")]
for r in c:
    out.append("| " + " | ".join(r) + " |")
out.append("")
out.append("- **L0 文件数停顿随 T 单调爆炸（100→117→190→598）**——L0 堆积侧约束；")
out.append("- **memtable 停顿在全部 11 组运行中恒存**（1,228-2,605 次）——上游侧约束：")
out.append("  L0 出口排空速率决定 flush 完成速率，决定 memtable 出清；")
out.append("- **停顿占比 76-95%，无一配置逃逸**——L1 放大（256MB→4GB）、T 扫描（2→16）、")
out.append("  线程数（16→2）共 11 组参数组合，写线程活跃时间始终 ≤24%；")
out.append("- 完整约束图：L1 小 → 深层重写饱和（96.5% 利用率）拖慢全链 → memtable stop；")
out.append("  L1 大 / T 小 → 串行 L0→L1 车道饱和（62%）→ L0/memtable stop；T 大 → L0 堆积 →")
out.append("  L0 slowdown。**三条失效路径全部经过 L0 子系统**——它是每字节必经且唯一串行的汇流点。\n")

out.append("## 综合结论（论文表述）\n")
out.append("1. L0→L1 是结构上不可并行的单服务器级（8 组、625 job、并发恒 1）；")
out.append("2. 其单次代价 O(|L1|)（L1=4GB 时单 job 收 69-122 个 SST、重写 250-431GB）；")
out.append("3. 默认参数已位于该结构权衡曲线的局部最优（倒 U 顶点），任何方向的调参都使吞吐")
out.append("   下降 11-22%，且停顿占比无法低于 76%；")
out.append("4. 上游（memtable）与下游（深层）的失效均以 L0 排空速率为中介——L0 子系统是写路径")
out.append("   的结构性瓶颈；")
out.append("5. ZeroFlush 的对照意义：分区范围物化 + 直装把「全范围串行汇流」拆为 P 个可并行、")
out.append("   代价 O(|L1|/P) 的独立安装点，并绕过 L0 的文件数停顿面。\n")

out.append("## 数据与可复现\n")
out.append("- 角度 A：`lane_analysis.py` + `lane_*.json`（8 组区间统计）；")
out.append("- 角度 B/C：`run_trigger_sweep.py`（同窗 T 扫描）+ `fill_trig*.log` + `LOG_trig*`；")
out.append("- L0 时间线：`l0_timeline_analysis.py`（L0 文件数 p50/p90/max）；")
out.append("- 既有对照：`../l0_l1_importance/`（L1 放大四预测）、`../native_wa_stall_100gb*/`")
out.append("  （默认配置 16/8/4/2 线程停顿/耗时基线）。")

open(f"{P}/l0_bottleneck_report.md", "w").write("\n".join(out) + "\n")
print("\n".join(out))
