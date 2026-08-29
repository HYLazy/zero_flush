#!/usr/bin/env python3
# 原生 RocksDB 100GB/1KB fillrandom 实验：报告生成
import json, os

BASE = "/home/embed/hyl/metadata_offload"
EXP = os.path.join(BASE, "output/zeroflush_m3_perf/native_wa_stall_100gb")
r = json.load(open(os.path.join(EXP, "native_wa_stall_100gb.json")))

USER_GB = 100.0
out = []
out.append("# 原生 RocksDB 100GB/1KB fillrandom：写放大 · 写停顿 · Compaction 规模\n")
out.append("配置：key 16B + value 1024B、16 线程、num=103,244,288（用户数据 100GB）、compression=none、")
out.append("cache 512MB、max_background_jobs=24、subcompactions=16、stats_dump 60s。")
out.append("结果：**13,978 ops/s（13.9 MB/s），wall 7,386s（123 分钟）**，Cumulative stall 94.7%。\n")

# 1) 各层写放大
fs = r["final_compaction_stats"]
out.append("## 1. 各层写放大（cumulative Compaction Stats，最终快照）\n")
out.append("W-Amp 列 = 该层 Write(GB) / Rn(GB)（db_bench 原生定义）；「/用户数据」= 该层写出字节 ÷ 100GB 用户写入。")
out.append("Rn = 下一层（更浅）输入、Rnp1 = 本层输入、Write = 本层输出。\n")
out.append("| 层 | 当前文件/大小 | Rn(GB) | Rnp1(GB) | Write(GB) | W-Amp | Write/用户数据 | Comp 次数 |")
out.append("|---|---|---|---|---|---|---|---|")
total_write = 0.0
for row in fs:
    if row["level"] in ("Sum", "Int"):
        continue
    total_write += row["write_gb"]
    out.append(f"| {row['level']} | {row['files']} / {row['size_mb']/1024:.1f}GB | {row['rn_gb']:.1f} | "
               f"{row['rnp1_gb']:.1f} | {row['write_gb']:.1f} | {row['w_amp']:.2f} | "
               f"{row['write_gb']/USER_GB:.2f}x | {row['count']} |")
out.append(f"| **合计（SST 写出）** | {sum(x['files'] for x in fs if x['level'] not in ('Sum','Int'))} / "
           f"{sum(x['size_mb'] for x in fs if x['level'] not in ('Sum','Int'))/1024:.1f}GB | "
           f"{sum(x['rn_gb'] for x in fs if x['level'] not in ('Sum','Int')):.1f} | "
           f"{sum(x['rnp1_gb'] for x in fs if x['level'] not in ('Sum','Int')):.1f} | "
           f"{total_write:.1f} | - | **{total_write/USER_GB:.2f}x** | "
           f"{sum(x['count'] for x in fs if x['level'] not in ('Sum','Int'))} |")
out.append("")
out.append(f"- **总写放大 ≈ {total_write/USER_GB:.1f}×**：写入 100GB 用户数据，SST 侧累计写出 {total_write:.0f}GB；")
out.append(f"- **L2/L3 是写放大主力**（L2 2.86×、L3 3.71×）——dynamic level 下 L3 为数据主层（26GB），"
           f"L2→L3 与 L1→L2 反复重写；")
out.append("- L0（flush）1.85×：100GB 用户数据 flush 出 184.8GB SST（覆盖写产生的多版本随 flush 落盘）；")
out.append("- L4 仅 0.17×（实验结束时深层下沉尚在初期）。")

# 2) 三类写停顿
st = r["stall_counts_final"]; tm = r["stall_time_final"]
out.append("\n## 2. 三类写停顿（WriteStallCause 计数 + 总时长）\n")
out.append("| 停顿类型 | cause | delayed | stopped |")
out.append("|---|---|---|---|")
out.append(f"| **内存（memtable 满）** | memtable-limit | {st.get('memtable-limit-delays',0)} | **{st.get('memtable-limit-stops',0)}** |")
out.append(f"| **L0 文件数** | L0-file-count-limit | {max(st.get('l0-file-count-limit-delays',0), st.get('cf-l0-file-count-limit-delays-with-ongoing-compaction',0))} | "
           f"{max(st.get('l0-file-count-limit-stops',0), st.get('cf-l0-file-count-limit-stops-with-ongoing-compaction',0))} |")
out.append(f"| **L1 以下（pending compaction 字节）** | pending-compaction-bytes | {st.get('pending-compaction-bytes-delays',0)} | {st.get('pending-compaction-bytes-stops',0)} |")
out.append(f"| 合计 | - | {st.get('total-delays',0)} | {st.get('total-stops',0)} |")
out.append(f"\n- **停顿总时长 {tm.get('secs',0)/60:.0f} 分钟 = 运行时间的 {tm.get('percent',0):.1f}%**；")
out.append(f"- **内存停顿（memtable stop）绝对主导（{st.get('memtable-limit-stops',0)} 次）**——compaction 消费速度低于 flush 速度，")
out.append("  L0 文件被并行 compaction（24 后台）及时消费（L0 停顿仅 6 次 delay），但 L2/L3 深层重写拖慢整体出清，")
out.append("  mutable memtable 迟迟无法切换 → 写线程在 memtable 满上全停；")
out.append("- pending-compaction-bytes 停顿为 0（L1+ 预估 compaction 字节未触发 soft/hard limit——")
out.append("  dynamic level bytes 下主层 26GB 远低于限制）。")

# 3) Compaction SSTable 数量
cb = r["compactions_by_output_level"]
out.append("\n## 3. Compaction 涉及的 SSTable 数量（compaction_started 输入文件，per subcompaction job）\n")
out.append("| 输出层 | 类型 | jobs | 输入 SST 累计 | L0 输入 | 更深输入 | 平均 | 中位 | 最大 |")
out.append("|---|---|---|---|---|---|---|---|---|")
def typ(ol):
    return {0: "Intra-L0", 1: "L0→L1"}.get(ol, f"L{ol-1}→L{ol}")
for ol, d in cb.items():
    out.append(f"| L{ol} | {typ(int(ol))} | {d['jobs']} | {d['input_files']} | {d['l0_inputs']} | "
               f"{d['nonl0_inputs']} | {d['avg_input']} | {d['median_input']} | {d['max_input']} |")
tot_jobs = sum(d['jobs'] for d in cb.values())
tot_files = sum(d['input_files'] for d in cb.values())
out.append(f"| 合计 | - | {tot_jobs} | {tot_files} | {sum(d['l0_inputs'] for d in cb.values())} | "
           f"{sum(d['nonl0_inputs'] for d in cb.values())} | {tot_files/tot_jobs:.2f} | - | - |")
out.append("")
out.append("- **L0→L1 仅 99 次 job、平均 10.6 个输入 SST**（561 个 L0 + 491 个 L1）——L0→L1 把 ~30 个小 L0 合并成")
out.append("  大 L1 输入；单次最大 21；")
out.append("- **深层 compaction（L1→L2/L2→L3）是 job 数主力**（1719 + 1513 次），单次小（平均 3.4-4.5 个 SST）——")
out.append("  L1/L2 文件大（target_file_size 64MB），每次下沉只涉及少数重叠文件；")
out.append("- Intra-L0 290 次（平均 4.8 个 L0 文件）——L0 小文件过多时先内部归并。")

# 4) 结论
out.append("\n## 4. 结论\n")
out.append("1. **原生 fillrandom 100GB 的写停顿 = memtable 停顿**（94.6% 时间在 stop 等待）：写停顿不是 L0 也不是")
out.append("   L1+ 字节限制，而是深层 compaction 消费不及导致 memtable 无法出清；")
out.append("2. **总写放大 9.8×**，其中 L2+L3 贡献 6.6×（68%）；ZF 的物化（无多版本重写）+ L1 直装正是绕开这段的开销；")
out.append("3. **L0→L1 compaction 单次平均 10.6 个 SST**（vs 深层 2-4.5 个）——L0 小文件合并特性；")
out.append("4. 13,978 ops/s 中实际写线程活跃仅 ~5%（停顿 94.6%）——compaction 消费能力（24 后台线程）是上限。")

txt = "\n".join(out) + "\n"
open(os.path.join(EXP, "native_wa_stall_100gb_report.md"), "w").write(txt)
print(txt)
