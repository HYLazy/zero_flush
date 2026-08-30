#!/usr/bin/env python3
# 8 线程 vs 16 线程 100GB/1KB fillrandom 原生对比报告（三类写停顿为主）
import json, os

BASE = "/home/embed/hyl/metadata_offload"
T16 = json.load(open(os.path.join(BASE, "output/zeroflush_m3_perf/native_wa_stall_100gb/native_wa_stall_100gb.json")))
T8 = json.load(open(os.path.join(BASE, "output/zeroflush_m3_perf/native_wa_stall_100gb_t8/native_wa_stall_100gb_t8.json")))

out = []
out.append("# 原生 RocksDB 100GB/1KB fillrandom：8 线程 vs 16 线程（三类写停顿对比）\n")
out.append("唯一差异：写线程数（8 vs 16）；workload 同为 103,244,288 键 ≈ 100GB 用户数据；")
out.append("后台 compaction 能力不变（max_background_jobs=24、subcompactions=16）。\n")

# 汇总表
out.append("## 1. 总体结果\n")
out.append("| 项 | 16 线程 | 8 线程 |")
out.append("|---|---|---|")
out.append("| 吞吐 | 13,978 ops/s（13.9 MB/s） | 12,558 ops/s（12.5 MB/s） |")
out.append("| wall | 7,386s（123 分钟） | 8,221s（137 分钟） |")
out.append("| 停顿总时长 | 6,960s（94.6%） | 7,684s（93.7%） |")
out.append("| L0 flush 写出 | 184.8GB | 184.1GB |")
out.append("| SST 累计写出（总写放大） | 980.3GB（9.8×） | 1,007.0GB（10.0×） |")
out.append("| compaction jobs | 3,762 | 3,834 |")
out.append("")

# 三类停顿
s16, s8 = T16["stall_counts_final"], T8["stall_counts_final"]
t16, t8 = T16["stall_time_final"], T8["stall_time_final"]
out.append("## 2. 三类写停顿（WriteStallCause）\n")
out.append("| 停顿类型 | 16 线程 delayed | 16 线程 stopped | 8 线程 delayed | 8 线程 stopped |")
out.append("|---|---|---|---|---|")
out.append(f"| **内存（memtable-limit）** | {s16.get('memtable-limit-delays',0)} | **{s16.get('memtable-limit-stops',0)}** | {s8.get('memtable-limit-delays',0)} | **{s8.get('memtable-limit-stops',0)}** |")
l0d16 = max(s16.get('l0-file-count-limit-delays',0), s16.get('cf-l0-file-count-limit-delays-with-ongoing-compaction',0))
l0d8 = max(s8.get('l0-file-count-limit-delays',0), s8.get('cf-l0-file-count-limit-delays-with-ongoing-compaction',0))
out.append(f"| **L0 文件数** | {l0d16} | {s16.get('l0-file-count-limit-stops',0)} | {l0d8} | {s8.get('l0-file-count-limit-stops',0)} |")
out.append(f"| **L1 以下（pending compaction 字节）** | {s16.get('pending-compaction-bytes-delays',0)} | {s16.get('pending-compaction-bytes-stops',0)} | {s8.get('pending-compaction-bytes-delays',0)} | {s8.get('pending-compaction-bytes-stops',0)} |")
out.append(f"| 合计 | {s16.get('total-delays',0)} | {s16.get('total-stops',0)} | {s8.get('total-delays',0)} | {s8.get('total-stops',0)} |")
out.append("")
out.append(f"- 停顿占比几乎不变（94.6% → 93.7%），**memtable stop 绝对主导的模式完全一致**；")
out.append(f"- **L0 与 L1+ 停顿在两种线程数下都≈0**——停顿类型与线程数无关；")
out.append(f"- 8 线程单次停顿更长（总停顿 7,684s ÷ 2,595 次 ≈ 2.96s/次 vs 16 线程 2.67s/次）——")
out.append("  写组更小、突发写入更少，memtable 出清窗口内写入量更少；")
out.append("- 停顿模式：开头 ~30s 全速写满 ~2.7GB memtable 预算，此后 94% 时间在等 compaction 出清。")

# 各层 WA
out.append("\n## 3. 各层写放大对比（SST 写出字节 / 100GB 用户数据）\n")
out.append("| 层 | 16 线程 | 8 线程 |")
out.append("|---|---|---|")
fs16 = {r["level"]: r for r in T16["final_compaction_stats"] if r["level"] not in ("Sum", "Int")}
fs8 = {r["level"]: r for r in T8["final_compaction_stats"] if r["level"] not in ("Sum", "Int")}
for lvl in ("L0", "L1", "L2", "L3", "L4"):
    a = fs16.get(lvl, {}).get("write_gb", 0)
    b = fs8.get(lvl, {}).get("write_gb", 0)
    out.append(f"| {lvl} | {a:.1f}GB（{a/100:.2f}×） | {b:.1f}GB（{b/100:.2f}×） |")
out.append("| 合计 | 980.3GB（9.80×） | 1,007.0GB（10.07×） |")
out.append("")
out.append("- 各层写放大几乎一致（±3%）——写放大由 workload 与 LSM 参数决定，与写线程数无关；")
out.append("- 8 线程 L2 略高（303 vs 286GB）：写突发更平缓 → L0 小文件分布不同 → L1→L2 输入重叠略增。")

# Compaction 规模
out.append("\n## 4. Compaction SSTable 数量对比\n")
out.append("| 输出层 | 16 线程 jobs/输入SST/平均 | 8 线程 jobs/输入SST/平均 |")
out.append("|---|---|---|")
cb16 = T16["compactions_by_output_level"]
cb8 = T8["compactions_by_output_level"]
for ol in ("0", "1", "2", "3", "4"):
    a = cb16.get(ol, {})
    b = cb8.get(ol, {})
    out.append(f"| L{ol} | {a.get('jobs',0)} / {a.get('input_files',0)} / {a.get('avg_input',0)} | "
               f"{b.get('jobs',0)} / {b.get('input_files',0)} / {b.get('avg_input',0)} |")
out.append("| 合计 | 3,762 / 15,438 / 4.10 | 3,834 / 17,033 / 4.44 |")
out.append("")
out.append("- L0→L1 单次平均输入 SST 一致（10.6 vs 10.4，最大 21 vs 16）——由 L0 小文件合并特性决定；")
out.append("- 深层（L1→L2/L2→L3）单次平均 3.4-4.6 个，两配置一致；job 数相当——compaction 行为由数据与")
out.append("  LSM 参数驱动，与写线程数基本无关。")

out.append("\n## 5. 结论\n")
out.append("1. **写线程数 16→8 不改变停顿结构**：memtable stop 仍占运行时间 ~94%（2,605 vs 2,595 次），")
out.append("   L0/pending-bytes 停顿仍为 0——瓶颈在后台 compaction 消费能力（不变），不在写线程数；")
out.append("2. **吞吐仅降 10%**（13,978 → 12,558 ops/s）：写线程近乎全程停顿，有效写入由 compaction 出清节奏决定；")
out.append("3. **写放大与 compaction 规模与线程数无关**（9.8× vs 10.0×、L0→L1 平均 10.6 vs 10.4 个 SST）——")
out.append("   均由 workload 分布与 LSM 参数决定；")
out.append("4. 对 ZeroFlush 的意义不变：原生瓶颈 = 深层 compaction 消费 < flush 生产，memtable 出清为唯一节流阀；")
out.append("   写线程数只是改变每次停顿的粒度，不改变停顿本身。")

txt = "\n".join(out) + "\n"
open(os.path.join(BASE, "output/zeroflush_m3_perf/native_wa_stall_100gb_t8/native_wa_stall_100gb_t8_report.md"), "w").write(txt)
print(txt)
