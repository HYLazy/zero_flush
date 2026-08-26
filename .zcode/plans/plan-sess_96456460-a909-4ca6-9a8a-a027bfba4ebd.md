# ZeroFlush 写路径剖析实验计划

## 目标
小规模复现 ZF fillrandom 慢 16~37× 的现象，把 wall time 分解为「停写等待 / 物化 / compaction / 有效写入」，验证或推翻 benchmark_analysis_report.md 发现 2 的 ②（随机定点读）③（单线程）——代码核查表明这两个描述与实际跑基准的二进制不符（K=8 并行 + WalScanner 顺序整读已存在），需要实测裁决真正瓶颈，修订 P3 优化方向。

## 四个判别性变体（vs256，key 16B，threads=16，fillrandom only）

| 变体 | 配置差异 | 回答的问题 |
|---|---|---|
| W0 原生 | 原生 rocksdb db_bench | 同规模参考吞吐/P50 |
| W1 ZF 基线 | 与 50GB 基准完全同参（P=64, stop=72, slowdown=64, K=8 默认） | 复现现象 + 全量时间线 |
| W2 ZF 无停写 | slowdown/stop trigger 抬到 10^6 | **W2−W1 吞吐差 = 停写等待占比**（根因①定量） |
| W3 ZF K=1 | `--zf_materialize_parallelism=1` | **W1 vs W3 物化批次耗时差 = 并行是否实际生效**（根因③定量） |

规模：num=8,000,000 keys（≈2.2GB 逻辑，~9 个 epoch @ epoch_target 256MB，L0 停写动态从第 2 个 epoch 即出现）。每变体超时 90 分钟，预计总运行 1.5~3 小时（后台执行）。readrandom 不在本实验范围。

## A. 插桩（小改动 + 增量编译，~30 分钟）
修改 `tools/db_bench_tool.cc`（仅此一个文件）：
1. 新增 `--zf_materialize_parallelism`（int，默认 8，写入 `zfo.materialize_parallelism`；W3 需要）；
2. benchmark 结束后若 zeroflush 开启，循环打印全部 15 个 `rocksdb.zeroflush.*` 属性（仿 `tools/zf_test.cc:125-129` 的 ZfProp）——epochs_sealed/materialized/reclaimed、install_direct_base/fallback_l0、materialize_sort_micros、sealed_read_count 等。
3. `make -j db_bench` 增量编译；用 30 秒微型 run 冒烟验证新 flag 与属性打印。

## B. 驱动脚本 `output/zeroflush_m3_perf/profiling/zf_profile.py`（新建）
- 顺序执行 W0→W1→W2→W3，DB 目录 `/tmp/zf_prof_db_<variant>`（**不删**，解析完 LOG 后再清理）；
- 公共 flag 与 50GB 基准一致（compression=none, disable_wal=false, cache 8MB, write_buffer 256MB, max_background_jobs 16, histogram, statistics），另加 `--stats_interval_seconds=30 --stats_per_interval=true`（stats dump 的 **Stalls 段**给出按原因的停写秒数——精确归因主通道）；
- 采样器线程：每秒读 `/proc/<pid>/stat`（utime/stime/线程数）+ `/proc/diskstats` → CSV（iostat/pidstat 未安装的替代）；
- W1 运行期间做两段 `perf record -F 199 -g -p <pid> -- sleep 60`（约一段落在停写期、一段落在流动期；perf_event_paranoid 若拒绝则记录并跳过，不影响主结论）；
- 每变体结果落 JSON。

## C. 解析器 `zf_parse_log.py`（新建）
从每个 run 的 LOG 提取：
- `ZeroFlush materialized N epochs (F files, B bytes) in T us`（flush_job.cc:1332）→ 物化批次时间线；
- `compaction_finished` 事件 JSON → compaction_time_micros / cpu_micros / output_level / bytes（含复核报告"job 81 cpu≈wall"到底量的是什么）；
- stats dump 的 Stalls 段 → 每原因 count/秒数随时间演化；
- `flush_finished` 的 lsm_state → L0 文件数轨迹；
- db_bench stdout → 吞吐/百分位；末尾 zf.* 属性值。

汇总输出每变体：wall time 分解（有效写入 / 停写等待 / 物化 / compaction，注意重叠关系按时间线对齐而非简单相加）、物化平均批次时长与字节率、L0 波动周期。

## D. 报告 `output/zeroflush_m3_perf/profiling/profiling_report.md`
- wall-time 分解表（4 变体对照）+ L0/物化/compaction 时间线图（文本表或简易 HTML）；
- 对报告根因 ②③ 的明确判定（已实现 or 仍瓶颈），及根因 ① 的定量占比；
- 修订版 P3 建议：若停写占主导 → 优先消费端（compaction/融合归并启用、范围路由验证）；若物化占主导 → 深挖 BuildTable fsync/单 flush 线程串行等新嫌疑；
- 顺手核验：install_direct_base 应为 0、install_fallback_l0 ≈ 64×epochs（hash 路由全回落的预期）。

## 验收标准（实验要能回答的三问）
1. 2.2GB 填充的 wall time 中，停写等待、物化、compaction 各占多少秒/百分比？
2. K=8 并行与顺序整读在真实负载下是否生效（W3 vs W1 物化批次时长、perf 热点）？
3. 下一个最值得做的代码优化是什么（修订 P3 排序）？

## 执行注意
- 所有修改只动 `tools/db_bench_tool.cc`（插桩）+ 新建 profiling 目录下脚本/报告，不触碰 zeroflush 核心逻辑；
- 长跑用后台执行并定期汇报进度；`/tmp` 空间充足（每变体峰值 <3GB）。