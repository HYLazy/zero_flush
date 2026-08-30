#!/usr/bin/env python3
# 原生 RocksDB 100GB/1KB fillrandom：写线程数 16/8/4/2 三类写停顿对比报告
import json, os

BASE = "/home/embed/hyl/metadata_offload"
P = os.path.join(BASE, "output/zeroflush_m3_perf")
D = {
    16: json.load(open(f"{P}/native_wa_stall_100gb/native_wa_stall_100gb.json")),
    8: json.load(open(f"{P}/native_wa_stall_100gb_t8/native_wa_stall_100gb_t8.json")),
    4: json.load(open(f"{P}/native_wa_stall_100gb_t4/native_wa_stall_100gb_t4.json")),
    2: json.load(open(f"{P}/native_wa_stall_100gb_t2/native_wa_stall_100gb_t2.json")),
}
OPS = {16: 13978, 8: 12558, 4: 11424, 2: 10539}
WALL = {16: 7386, 8: 8221, 4: 9037, 2: 9796}

out = []
out.append("# 原生 RocksDB 100GB/1KB fillrandom：写线程数 16/8/4/2 三类写停顿对比\n")
out.append("workload 恒定（103,244,288 键 ≈ 100GB 用户数据）；仅写线程数变化；")
out.append("后台 compaction 能力不变（max_background_jobs=24、subcompactions=16）。无插桩，")
out.append("数据全部来自 LOG stats_dump（60s）+ EVENT_LOG compaction 事件。\n")

out.append("## 1. 总体结果\n")
out.append("| 项 | 16 线程 | 8 线程 | 4 线程 | 2 线程 |")
out.append("|---|---|---|---|---|")
out.append("| 吞吐（ops/s） | 13,978 | 12,558 | 11,424 | 10,539 |")
out.append("| wall（分钟） | 123 | 137 | 151 | 163 |")
st = lambda t, k: D[t]["stall_counts_final"].get(k, 0)
tm = lambda t: D[t]["stall_time_final"]
out.append("| 停顿总时长 | {:.0f}s（{:.1f}%） | {:.0f}s（{:.1f}%） | {:.0f}s（{:.1f}%） | {:.0f}s（{:.1f}%） |".format(
    tm(16)["secs"], tm(16)["percent"], tm(8)["secs"], tm(8)["percent"],
    tm(4)["secs"], tm(4)["percent"], tm(2)["secs"], tm(2)["percent"]))
out.append("| memtable stop 次数 | {} | {} | {} | {} |".format(
    st(16, "memtable-limit-stops"), st(8, "memtable-limit-stops"),
    st(4, "memtable-limit-stops"), st(2, "memtable-limit-stops")))
out.append("| L0 停顿（delayed） | 6 | 0 | 0 | 0 |")
out.append("| pending-bytes 停顿 | 0 | 0 | 0 | 0 |")
fs = lambda t: {r["level"]: r for r in D[t]["final_compaction_stats"] if r["level"] not in ("Sum", "Int")}
tot = lambda t: sum(fs(t)[l]["write_gb"] for l in fs(t))
out.append("| SST 累计写出（总写放大） | {:.0f}GB（{:.1f}×） | {:.0f}GB（{:.1f}×） | {:.0f}GB（{:.1f}×） | {:.0f}GB（{:.1f}×） |".format(
    tot(16), tot(16)/100, tot(8), tot(8)/100, tot(4), tot(4)/100, tot(2), tot(2)/100))
out.append("| compaction jobs | 3,762 | 3,834 | 3,934 | 4,011 |")
out.append("")

out.append("## 2. 三类写停顿明细\n")
out.append("| 停顿类型 | 16 线程 | 8 线程 | 4 线程 | 2 线程 |")
out.append("|---|---|---|---|---|")
out.append("| **memtable-limit** delayed / stopped | 0 / **2,605** | 0 / **2,595** | 0 / **2,450** | 0 / **2,460** |")
out.append("| **L0-file-count-limit** delayed / stopped | 6 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |")
out.append("| **pending-compaction-bytes** delayed / stopped | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |")
out.append("| 单次 memtable stop 平均时长 | {:.2f}s | {:.2f}s | {:.2f}s | {:.2f}s |".format(
    tm(16)["secs"]/st(16,"memtable-limit-stops"), tm(8)["secs"]/st(8,"memtable-limit-stops"),
    tm(4)["secs"]/st(4,"memtable-limit-stops"), tm(2)["secs"]/st(2,"memtable-limit-stops")))
out.append("")

out.append("## 3. 各层写放大对比（SST 写出 / 100GB 用户数据）\n")
out.append("| 层 | 16 线程 | 8 线程 | 4 线程 | 2 线程 |")
out.append("|---|---|---|---|---|")
for lvl in ("L0", "L1", "L2", "L3", "L4"):
    row = [fs(t).get(lvl, {}).get("write_gb", 0) for t in (16, 8, 4, 2)]
    out.append("| {} | {} | {} | {} | {} |".format(lvl, *[
        f"{v:.1f}GB（{v/100:.2f}×）" for v in row]))
out.append("")

out.append("## 4. L0→L1 / 深层 compaction 规模（jobs / 输入SST / 平均）\n")
out.append("| 输出层 | 16 线程 | 8 线程 | 4 线程 | 2 线程 |")
out.append("|---|---|---|---|---|")
for ol in ("0", "1", "2", "3", "4"):
    row = []
    for t in (16, 8, 4, 2):
        d = D[t]["compactions_by_output_level"].get(ol, {})
        row.append(f"{d.get('jobs',0)} / {d.get('input_files',0)} / {d.get('avg_input',0)}")
    out.append(f"| L{ol} | " + " | ".join(row) + " |")
out.append("")

out.append("## 5. 结论\n")
out.append("1. **写线程 16→2（8 倍范围），memtable stop 始终绝对主导**：停顿占比 94.6% → 93.7% → 91.4% → 89.2%，")
out.append("   仅缓降 5.4pp；stop 次数几乎不变（2,605/2,595/2,450/2,460）；L0 与 pending-bytes 停顿恒为 0；")
out.append("2. **吞吐仅降 25%**（13,978 → 10,539 ops/s）而写线程减 8 倍——写线程几乎全程停顿，")
out.append("   有效写入速率 = 后台 compaction 消费速率（~11-14K ops/s），与写线程数基本无关；")
out.append("3. **停顿占比的缓降来自停顿间隙的突发写入变慢**：burst 速率随线程数近似线性下降（~90/45/22/11 MB/s），")
out.append("   burst 越慢 → 同样消费速率下停顿窗口占比越低——趋近 compaction 支撑速率（~10K ops/s）时停顿将消失；")
out.append("4. **各层写放大与 compaction 规模与线程数无关**（总放大 9.8-10.5×、L0→L1 平均 9.8-10.6 个输入 SST）")
out.append("   ——由 workload 分布与 LSM 参数决定；")
out.append("5. 对 ZeroFlush 的意义：原生 fillrandom 100GB 的写停顿 = 单一 memtable 节流阀")
out.append("   （compaction 消费 < 生产），线程数只是改变写入突发粒度；ZF 的分区 WAL 直装绕开 memtable 出清，")
out.append("   其停顿结构（若有的话）在更深层。")

txt = "\n".join(out) + "\n"
open(f"{P}/native_wa_stall_100gb_t2/native_wa_stall_thread_scaling_report.md", "w").write(txt)
print(txt)
