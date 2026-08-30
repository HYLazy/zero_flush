#!/usr/bin/env python3
# L0→L1 与 L1 以下 compaction 耗时统计：16/8/4/2 线程对比报告
import json, os

BASE = "/home/embed/hyl/metadata_offload"
P = os.path.join(BASE, "output/zeroflush_m3_perf")
D = {
    "16": json.load(open(f"{P}/native_wa_stall_100gb/native_wa_stall_100gb_comp_time.json")),
    "8": json.load(open(f"{P}/native_wa_stall_100gb_t8/native_wa_stall_100gb_t8_comp_time.json")),
    "4": json.load(open(f"{P}/native_wa_stall_100gb_t4/native_wa_stall_100gb_t4_comp_time.json")),
    "2": json.load(open(f"{P}/native_wa_stall_100gb_t2/native_wa_stall_100gb_t2_comp_time.json")),
}
WALL = {"16": 7386, "8": 8221, "4": 9037, "2": 9796}

out = []
out.append("# 原生 RocksDB 100GB/1KB fillrandom：L0→L1 与 L1 以下 compaction 耗时统计\n")
out.append("数据源：四次线程缩放实验（16/8/4/2 线程，workload 恒定 100GB）LOG 的 compaction_finished")
out.append("事件（每逻辑 compaction 一条，compaction_time_micros 为该 job 的 wall 时间——subcompactions=16")
out.append("的子 job 在其内部并行）。统计维度：单 job wall 平均/P50/P95/P99/P99.9/max、全部 job 的")
out.append("累计耗时（total；同一时刻多个 compaction 并行执行，故累计值可超过 wall）。\n")

out.append("## 1. L0→L1 compaction（output_level=1）\n")
out.append("| 指标 | 16 线程 | 8 线程 | 4 线程 | 2 线程 |")
out.append("|---|---|---|---|---|")
def row(metric, fmt="{:.1f}", grp="L0_L1"):
    vals = [fmt.format(D[t]["groups"][grp][metric]) for t in ("16", "8", "4", "2")]
    return "| " + metric + " | " + " | ".join(vals) + " |"
out.append(row("jobs", "{:.0f}"))
out.append(row("avg_wall_s"))
out.append(row("p50_wall_s"))
out.append(row("p95_wall_s"))
out.append(row("p99_wall_s"))
out.append(row("p999_wall_s"))
out.append(row("max_wall_s"))
out.append(row("total_thread_time_s"))
out.append("| 占 wall 比例 | " + " | ".join(
    f"{D[t]['groups']['L0_L1']['total_thread_time_s']/WALL[t]*100:.1f}%" for t in ("16","8","4","2")) + " |")
out.append("")

out.append("## 2. L1 以下 compaction（L1→L2、L2→L3、L3→L4 合计，output_level≥2）\n")
out.append("| 指标 | 16 线程 | 8 线程 | 4 线程 | 2 线程 |")
out.append("|---|---|---|---|---|")
out.append(row("jobs", "{:.0f}", "deep_total_L1以下"))
out.append(row("avg_wall_s", "{:.1f}", "deep_total_L1以下"))
out.append(row("p50_wall_s", "{:.1f}", "deep_total_L1以下"))
out.append(row("p95_wall_s", "{:.1f}", "deep_total_L1以下"))
out.append(row("p99_wall_s", "{:.1f}", "deep_total_L1以下"))
out.append(row("p999_wall_s", "{:.1f}", "deep_total_L1以下"))
out.append(row("max_wall_s", "{:.1f}", "deep_total_L1以下"))
out.append(row("total_thread_time_s", "{:.0f}", "deep_total_L1以下"))
out.append("| 占 wall 比例 | " + " | ".join(
    f"{D[t]['groups']['deep_total_L1以下']['total_thread_time_s']/WALL[t]*100:.0f}%" for t in ("16","8","4","2")) + " |")
out.append("")

out.append("## 3. L1 以下分层的单 job 耗时（16 线程，代表分布）\n")
out.append("| 层 | jobs | 平均 | P50 | P95 | P99 | P99.9 | max | 累计耗时 |")
out.append("|---|---|---|---|---|---|---|---|---|")
g16 = D["16"]["groups"]
for k in ("L1_L2", "L2_L3", "L3_L4"):
    g = g16[k]
    out.append(f"| {k} | {g['jobs']} | {g['avg_wall_s']:.1f}s | {g['p50_wall_s']:.1f}s | {g['p95_wall_s']:.1f}s | "
               f"{g['p99_wall_s']:.1f}s | {g['p999_wall_s']:.1f}s | {g['max_wall_s']:.1f}s | {g['total_thread_time_s']:.0f}s |")
out.append("")

out.append("## 4. 结论\n")
out.append("1. **L0→L1 快而稳定**：单 job 平均 15.7-18.1s、P99 28-30s、max ≤31s——输入以小 L0 文件为主，")
out.append("   数据量小；累计耗时 1,558-2,294s，仅占 wall 的 21-23%；")
out.append("2. **L1 以下慢且长尾**：单 job 平均 24.4-29.3s（约为 L0→L1 的 1.6 倍），P99 68-78s、")
out.append("   最长 91-143s（P99.9 与 max 同量级——长尾真实存在）；累计 82,244-105,838s，")
out.append("   **是 L0→L1 的 45-53 倍、占 wall 的 9.8-10.8 倍**——即平均 10.8-11.1 个 compaction 并行运行仍消费不过来；")
out.append("3. **L2→L3 是最大耗时层**（16 线程：累计 48,530s，占 deep 的 59%；单 job 平均 32.1s、P99 72.9s、max 91.4s）")
out.append("   ——主层（L3 26GB）被反复重写；L1→L2 次之（30,200s，单 job 17.6s）；L3→L4 尚在初期（3,514s）；")
out.append("4. **线程数对单 job 耗时几乎无影响**（各分布 ±15% 内），但低线程数下单 job 略慢（burst 写更平缓 →")
out.append("   同层文件更多重叠）且 job 数更多 → 累计耗时随线程数下降而增加（82.2K → 105.8K s，+29%）；")
out.append("5. **与写停顿的直接联系**：memtable stop 占 wall 89-95%，而深层 compaction 累计线程时间达 wall 的")
out.append("   9.8-10.8 倍（并行 24 后台仍饱和）——深层 compaction 是唯一的系统性瓶颈，L0→L1 从不是瓶颈。")

txt = "\n".join(out) + "\n"
open(f"{P}/native_wa_stall_100gb/comp_time_report.md", "w").write(txt)
print(txt)
