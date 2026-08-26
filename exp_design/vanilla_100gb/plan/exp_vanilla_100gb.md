# 实验方案：原版 RocksDB 100GB fillrandom 基线剖析

- 日期：2026-08-25
- 被测对象：`source/rocksdb`（RocksDB 11.2.0，原版，无 ZeroFlush 修改）
- 负载：fillrandom，100GB 数据集（1KB KV），16 线程
- 目的：量化原版 LSM 在 100GB 写入下的（1）各部分写放大（2）写停顿（3）compaction 发生位置与次数（4）带宽使用率随时间变化，作为 ZeroFlush 各阶段改造的对照基线。

---

## 1. 负载配置

与 ZeroFlush 50GB 终验（R42）对齐的非 ZF 参数，去掉全部 `--zf_*` 与零冲刷专属 flag，使基线可比：

| 参数 | 值 | 说明 |
|---|---|---|
| `--benchmarks` | `fillrandom` | 纯写 |
| `--num` | `100000000` | 100M × 1KB ≈ 100GiB 逻辑数据 |
| `--key_size` / `--value_size` | 16 / 1024 | 1KB KV |
| `--threads` | 16 | 与 R42 一致 |
| `--writes` | `num/threads` | 每线程配额 |
| `--compression_type` | none | 与 R42 一致 |
| `--disable_wal` | false | WAL 开启 |
| `--write_buffer_size` | 268435456 (256MB) | 与 R42 一致 |
| `--max_background_jobs` | 24 | 与 R42 一致 |
| `--subcompactions` | 16 | 与 R42 一致 |
| `--cache_size` | 536870912 (512MB) | 与 R42 一致 |
| `--histogram` / `--statistics` | true | 延迟百分位 + ticker |
| `--stats_interval_seconds` / `--stats_per_interval` | 30 / 1 | **核心：每 30s 一次全量 stats dump** |

L0/L1 触发阈值保持原版默认：`level0_file_num_compaction_trigger=4`、`level0_slowdown_writes_trigger=20`、`level0_stop_writes_trigger=36`（ZF 侧为 64/72，差值本身就是对照点）。

DB 目录：`/tmp/vanilla_db_<run>`（根分区 nvme0n1p3，与 R42 同盘）。超时 15h（R42 实测 50GB 约 1.9h，100GB 原版保守估计 3–8h）。

## 2. 四项指标：定义与数据来源

### 2.1 各部分的写放大（WA 分解）

| 分量 | 定义 | 数据来源 |
|---|---|---|
| 用户数据 | `num × (key+value)` = 100GiB | db_bench RawSize |
| WAL 写 | `rocksdb.wal.bytes` 最终 ticker | stdout 末尾 statistics 段 |
| Flush 写 | `rocksdb.flush.write.bytes` 最终 ticker；与最后 dump 的 `Flush(GB): cumulative` 交叉核对 | stdout |
| Compaction 写（总量） | `rocksdb.compact.write.bytes` 最终 ticker | stdout |
| Compaction 写（按输出层） | 最后 dump 中 Compaction Stats 逐层 `Write(GB)` 行（L0..Ln、Sum） | stdout 周期 dump |
| Compaction 写（按层对） | LOG 中每条 `Compacted 7@0 + 1@1 files to L1 => N bytes` 按（输入层集→输出层）聚合字节数 | LOG |
| 设备级总写 | 采样器 diskstats `w_sect` 全程增量 × 512B | samples.csv |

写放大定义：`WA_x = 分量写字节 / 用户数据字节`；总体 WA 同时给出 ticker 口径（WAL+Flush+Compact 之和）与设备口径（diskstats 实测，含 WAL/表/元数据全部落盘）。Flush 次数另计（LOG `Level-0 flush table #N: started`）。

### 2.2 写停顿

| 指标 | 来源 |
|---|---|
| 累计停写秒数与占比 | 最后 dump `Cumulative stall: H:M:S, X.X percent`；`rocksdb.stall.micros` ticker 交叉核对 |
| 分原因计数（delay/stop） | 每 dump 的 `Write Stall (count):` 行：`l0-file-count-limit-*`、`memtable-limit-*`、`pending-compaction-bytes-*`、`write-buffer-manager-limit-stops`、`total-delays/total-stops` |
| 时间序列 | 每 dump 的 `Interval stall: H:M:S`（30s 粒度停写秒数）；`total-delays/stops` 累计值差分 |

### 2.3 Compaction 发生位置与次数

| 指标 | 来源 |
|---|---|
| 按输出层次数 | 最后 dump 逐层 `Comp(cnt)`（L0 行含 L0→L0 内部合并） |
| 按层对次数与写量 | LOG `Compacting X@a + Y@b files to Ln, score s` → 层对（如 `0,1→1`、`0→0`、`1,2→2`）计数与触发 score；`Compacted ... => N bytes` 给每层对写字节 |
| Flush 次数 | LOG `Level-0 flush table #N: started` 计数 |
| 随时间分布 | 按 60s 分桶聚合 LOG 事件时间戳；30s dump 的 `num-running-compactions/flushes` 序列 |

### 2.4 带宽使用率随时间变化

| 指标 | 来源 |
|---|---|
| 设备级写/读带宽 | 1s 采样 diskstats 扇区差分 → MB/s 曲线 |
| 设备忙占比 | diskstats `io_ms` 差分 / 采样周期（≈设备占用率） |
| DB 组件级带宽 | 每 dump 的 `Interval compaction: X GB write, Y MB/s`、`Flush(GB): interval`、`Interval WAL: written`、`Interval writes: ingest`；逐层 Write(GB) 累计差分 → 各层写速率 |
| 带宽利用率 | 设备写 MB/s ÷ 标定峰值写带宽（见 §3.3） |

## 3. 采集机制

### 3.1 stdout 周期 dump（核心）

`--stats_interval_seconds=30 --stats_per_interval=1` 使 db_bench 每 30s 向 stdout 输出一个完整 stats 块，含：逐层 Compaction Stats（Write(GB)、W-Amp、Comp(cnt)、Wr(MB/s)）、Flush(GB) 累计/区间、Cumulative/Interval compaction 与 stall、分原因 Write Stall 计数、num-running-compactions/flushes、Interval WAL/ingest。30s 粒度足以支撑全部四项指标的时间序列。

### 3.2 LOG 事件流

db_bench 默认 INFO 级 LOG 每起一桩后台任务记一条（RocksDB 11.2 源码 `compaction_job.cc:2825`、`flush_job.cc:933/980/1153`，均 INFO 级）：
`[JOB 5] Compacting 7@0 + 1@1 files to L1, score 1.75`、`[JOB 5] Compacted 7@0 + 1@1 files to L1 => 141299142 bytes`、`[JOB 3] Level-0 flush table #14: started`。运行结束后把 DB 目录 `LOG*` 复制到 run 目录。

### 3.3 采样器与峰值标定

- 1s 轮询 `/proc/<pid>/stat`（状态/线程数/CPU）与 `/proc/diskstats`（读写扇区、io_ms）→ `samples.csv`（复用 `output/zeroflush_m3_perf/profiling/zf_profile.py` 的 Sampler 模式，磁盘设备由 DB 目录所在文件系统自动探测）。
- 峰值写带宽：正式运行前用 `dd` 写 8GB（`oflag=direct`），期间 0.2s 采样 diskstats，取 **2s 滑窗峰值**（总平均偏保守、瞬时易高估），结果缓存于 run 目录 `peak_bw.json`，供利用率计算。
- **口径注意**：数据盘为系统根分区（nvme0n1p3，共享设备），diskstats 设备口径含系统后台写噪声；小时级运行中占比可忽略，报告中已标注。

### 3.4 数据一致性

- **口径说明**：周期 dump 的 Compaction Stats 中，L0 行的 Write(GB)/Comp(cnt) 包含 memtable flush 输出（RocksDB 语义）；compaction-only 口径以 LOG 层对事件统计为准。单输入 `n→n+1` 的 compaction 可能是 trivial move（字节计入 Moved(GB)，未真实写盘），解析器对此打标 `trivial_move_likely`。
- `rocksdb.flush.write.bytes` ticker ↔ 最后 dump L0 行 Write(GB) 交叉核对；
- `rocksdb.compact.write.bytes` ticker ↔ 最后 dump Sum Write(GB) − Flush(GB) cumulative 交叉核对；
- `rocksdb.stall.micros` ↔ `Cumulative stall` 秒数交叉核对；
- LOG 层对写字节之和 ↔ ticker `COMPACT_WRITE_BYTES` 交叉核对（含 trivial move 高估与 dump 时点差，容差 ±5% 并在报告中标注）。

## 4. 运行流程与产物

```
exp_design/vanilla_100gb/
├── plan/exp_vanilla_100gb.md      # 本方案
└── code/
    ├── run_exp.py                 # 运行驱动（db_bench + 采样 + LOG 收集 + 峰值标定）
    ├── parse_exp.py               # stdout/LOG/samples → metrics.json
    ├── plot_exp.py                # 4 张图 + report.md
    └── smoke_check.py             # 小规模端到端自检（2–4GB，~1min）

output/vanilla_100gb/run_<ts>/
├── cmd.json / result.json         # 完整命令、硬件、基础结果
├── stdout.txt                     # db_bench 全量输出（周期 dump + 最终 tickers）
├── LOG*                           # RocksDB 事件日志
├── samples.csv                    # 1s 设备/进程采样
├── peak_bw.json                   # 设备峰值写带宽
├── metrics.json                   # 四项指标全量解析结果
└── charts/                        # wa_breakdown / stalls_timeline /
                                   # compaction_map / bandwidth_timeline + report.md
```

执行：`python3 code/run_exp.py --out <run_dir>`（默认 100GB/15h）；解析：`python3 code/parse_exp.py <run_dir>`；出图：`python3 code/plot_exp.py <run_dir>`。

## 5. 时间预估与风险

- 预估 3–8h（原版 fillrandom 受 L0/L1 停写节流；W0 小规模参考 1M ops/s @256B，1KB 值下按盘带宽折算 ~250K ops/s，100M keys ≈ 400–1000s 纯写，但停写显著拉长 wall time）。
- 风险：中途 OOM/断电 → 采样与 stdout 持续落盘，LOG 可续解析；`--statistics` 最终 ticker 仅运行正常结束时才有 → 解析器以最后 dump 为兜底累计口径。
- 磁盘容量：553GB 可用，原版 LSM 峰值占用 ≈ 1.5–2.5× 终态（终态 ~100GB），充足。
- 与 ZF 对比时的口径说明：本基线保留 WAL（R42 同），若对比目标为 ZF 的 WAL 减免收益，需另跑 `--disable_wal=true` 对照组，本实验不含。

## 6. 预期产出

- `metrics.json`：四项指标全量数据（WA 分解/层对矩阵、stall 分原因+时间序列、compaction 层对计数+60s 分布、带宽 1s 曲线+30s 组件曲线+利用率）；
- 4 张图：WA 分解堆叠图、停写时间线、compaction 位置/次数图、带宽利用率时间线；
- `report.md`：关键数字汇总（总 WA、停写占比、compaction 总次数、平均/峰值带宽利用率）。
