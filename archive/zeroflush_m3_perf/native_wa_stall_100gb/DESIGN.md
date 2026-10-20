# 原生 RocksDB 100GB/1KB fillrandom：写放大 · 写停顿 · Compaction 规模实验设计

## 目标
原生 RocksDB 在 100GB / 1KB KV / 16 线程 fillrandom 下的：
1. **各层写放大**（L0→L1、L1→L2、…、每层读入/写出字节与 W-Amp）；
2. **三类写停顿**（按 RocksDB 11.2 WriteStallCause）：
   - 内存（memtable-limit：write_buffer 满触发 delayed/stopped）
   - L0（L0-file-count-limit：level0_slowdown/stop 阈值）
   - L1 以下（pending-compaction-bytes：L1+ 预估 compaction 字节 soft/hard rate limit）
3. **L0→L1 compaction 涉及的 SSTable 数量**（每次 job 的 L0/L1 输入文件数、分布与累计）；
4. **深层 compaction（output_level ≥ 2）涉及的 SSTable 数量**（同上）。

## 工作负载
- `--benchmarks=fillrandom --num=103244288 --writes=6452768 --threads=16`
  （key 16B + value 1024B = 1040B/键，103,244,288 × 1040B ≈ 100GB = 所有线程写入总和；
  writes 6,452,768/线程 × 16）
- 公共参数与 24 项矩阵一致：compression=none、disable_wal=false、cache 512MB、
  max_background_jobs=24、subcompactions=16、histogram
- 观测开销：--statistics（ticker 终值）、--stats_dump_period_sec=60（LOG 每 60s 累计 stats：
  Compaction Stats 表 + Write Stall (count) 按原因）
- 预计时长 ≈ 2.3h（矩阵同参 12.4K ops/s）；负载背景 5-45 波动与矩阵一致

## 数据源（全部来自 LOG 文件，无需插桩）
| 指标 | 来源 |
|---|---|
| 各层写放大 | LOG 最终 cumulative "Compaction Stats" 表：每层 Rn(GB)/Rnp1(GB)/Write(GB)/W-Amp；逐 60s 快照构成时间线 |
| 三类写停顿 | LOG 每 60s "Write Stall (count): memtable-limit-delayed: N, memtable-limit-stopped: N, L0-file-count-limit-delayed: N, ... pending-compaction-bytes-delayed/stopped: N" + "Cumulative stall" 总时长 + "Interval stall" 时间线 |
| L0→L1 SSTable 数 | EVENT_LOG_v1 "compaction_started" 事件：`files_L0:[...]`/`files_L1:[...]` 数组长度（output_level=1 的 job，按 compaction_finished 的 output_level 关联） |
| 深层 SSTable 数 | 同上，output_level ≥ 2 |

## 产出
- `native_wa_stall_100gb.json`：全部解析结果（最终各层 WA、停顿计数/时长、
  compaction 规模分布、时间线）
- `native_wa_stall_100gb_report.md`：报告
- LOG 原件保留于实验目录（压缩）
