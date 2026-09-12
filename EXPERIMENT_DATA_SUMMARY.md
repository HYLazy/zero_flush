# ZeroFlush / metadata_offload 全项目实验数据汇总

> 盘点时间：2026-09-12（覆盖 2026-08-04 前期探针 → 2026-09-12 merge-off 中止）
> 数据来源：`output/**` 报告与原始日志、`exp_design/**` 报告与 JSON、`zeroflush/*_DESIGN.md`
> 与 `ZEROFLUSHV0_ROADMAP.md`、`git log` 里程碑。**所有数字均来自文件原文**；来源
> 路径逐条给出；冲突项在 §9 并列。可信度分级见 §10。

---

## 1. 总览：里程碑级对照（ZF vs 原生 RocksDB）

| 实验 | 日期 | 配置 | ZeroFlush | 原生 RocksDB | 来源 |
|---|---|---|---|---|---|
| exp0 flush 成本分解 | 08-05 | 80M ops/8T；100B·1K·4K × value-seq/uniform | — | 838,964 / 312,675 / 76,133 / 25,905 / 9,558 / 3,106 ops/s | `[已归档 /home/embed/hyl/metadata_offload-archive/pre-v0-experiments/]exp_design/pre_experiment/report/exp0_stage_report.md` |
| exp1 L0 overlap 干预 | 08-05 | 10M 写×8T/1KB，flush 按 L1 边界分区 | overlap 6.0→1.98（zipf）/7.0→0.23（unif）；吞吐 −62.7%/−68.8% | baseline 58,522（zipf） | `.../exp1_stage_report.md` |
| exp2 分区开销门 | 08-04 | range lookup P∈{64,128,256}；multi-WAL P∈{1,16,64}×{A,B} | lookup p99 ≤90ns；P=64 组提交回退 76.8%（A）/19.5%（B） | P=1 A sync 306,743 ops/s | `.../exp2_stage_report.md` |
| M3 早期 50GB 基准 | 08-20/21 | 50GB/16T，P=64 | 3,387（256B）/ 1,919（1KB）ops/s | 124,125 / 30,255 | `source/rocksdb-zeroflush/benchmark_analysis_report.md` |
| M3 写路径剖析 W0–W3 | 08-21 | 2.2GB/256B/16T | W1 10,898（后重跑 21,594） | W0 1,031,788 ops/s | `output/zeroflush_m3_perf/profiling/profiling_report.md` |
| M3 L0→L1 重要性 | — | 100GB/1KB/16T，L1=256M/1G/4G | 28,374 / 27,694 / 35,402 ops/s | — | `.../l0_l1_importance/l0l1_report.md` |
| M3 原生 100GB 停顿基线 | — | 100GiB/1KB，T=2/4/8/16 | — | 13,978 / 12,558 / 11,424 / 10,539 ops/s | `.../native_wa_stall_100gb*/` |
| M3 E6 矩阵 24 配置 | 08-27/29 | 10/50/100GB × 128B–2KB | 写落后 1.04–5.53×；100GB 读 0.45–0.96×（**反超**） | — | `.../bench_matrix_report.md` |
| M4.1 去串行化 | 08-21 | 2.2GB | 21,594（2.0×）；R3 38,129（3.2×） | — | `M4_DESIGN.md` §M4.1 |
| M4.2b→M4.6d 演进 | 08-22/25 | 50GB/16T | 22,751 → **28,812（1.9h 闭环）** | — | `M4_DESIGN.md` §M4.2b–§M4.6 |
| M4.6–M4.11 优化 | 08-25/29 | 50GB/16T | R43/R44 **60.0K/66.9K**；切片 78.6K | 88.8K（差距 11%） | `M4_DESIGN.md` §6.6–§6.15 |
| **M4 DG 50GB 验收** | 09-10 | gov_bench：51.2M 写×1KB=50GiB **写流量**；universe 100M、hot 85%/span 12M（活数据仅 19.0M 条） | **ON 40,196 / OFF 29,801 ops/s（+34.9%）**，命中 83.88%/83.89%，0 corruption | — | `output/zeroflush_m4_perf/REPORT.md` |
| **M4 DG 100GB 验收** | 09-10 | 104.86M 写≈100GiB | **ON 22,545 ops/s**，命中 86.04%，0 corruption；OFF 两次卡滞（~69% 处） | — | 同上 |
| M5 50GB（丢失修复后） | 09-07 | 50M/1KB/16T，base_merge | fill 15,452（0.74×）；read **188,086**；命中 63.26% | fill 20,752；read 137,972 | `.../m5_50gb_fix/RESULTS.md` |
| **cmp_1kb50 同窗对照** | 09-02 | 50GB/1KB/16T | **27,066 ops/s**（1,773s）；read 63,284 | 26,409（1,818s）；read 356,717 | `.../cmp_1kb50/RESULTS.md` |
| cmp_1kb100 | 09-03 | 100GB/1KB | 19,701（0.907×） | 21,725 | `.../cmp_1kb100/RESULTS.md` |
| zf09_100gb | 09-03 | 100GB/1KB | 17,098（0.79×）；写 P50 294.8µs/P99 5,034µs | 21,680 | `.../zf09_100gb_1kb/RESULTS.md` |
| **vanilla_100gb smoke** | 08-25 | 3.87GiB/4T/1KB | — | **50,943 ops/s**，P99 32.53µs，设备写放大 3.975× | `[已归档 /home/embed/hyl/metadata_offload-archive/abandoned-baselines/]output_vanilla_100gb/smoke_run/report.md` |

---

## 2. 前期探针 exp0–exp3（2026-08-04/05）

- **exp0（flush 成本分解，判定 H1 成立）**：吞吐随 value 尺寸与分布剧烈变化（见上表）；
  写放大 WA_WAL=1.0203 / WA_Flush=0.7670 / WA_Compaction=3.3673；flush 占比均值 **0.1668**
  （目标区间 0.2–0.4 未达，因 compaction 分母膨胀）；分解残差 0.0%；uniform-4KB 时
  WA_Comp=9.099、stall 占 wall **87.5%**。YCSB-A 补充：READ avg 33.57µs(p95/p99 93/111)，
  UPDATE avg 47.41µs(107/128)。
- **exp1（L0 overlap 源于准入，判定 3 FAIL/1 PASS）**：分区注入使 overlap 降 67–96.7%，
  但 compaction 读 +166%、吞吐 −63%；uniform 下吞吐 23,311→7,273（−68.8%）；结论：
  方向有效但 compaction lag 反噬，**P_recommend=16**。
- **exp2（分区开销门，判定 FAIL）**：range lookup 微秒级以下（p99 66–90ns，mean
  26.1–37.1ns）；multi-WAL 组提交两条路均不可行——A(independent_fsync) 吞吐回退
  **76.8%**、B(batched_fsync) durability 窗口 **1,902ms**（超标 380×）。据此提出
  "单 WAL + 后台分区线程" 替代。
- **exp3（Partition Index vs MemTable）**：因 exp2 FAIL **未执行**（status=aborted），H4 未验证。

---

## 3. M1–M2（2026-08-09/10）

- **M1（WAL 持久化修复）**：zf_test 7/7 PASS（析构未 flush、ReopenWritableFile 截断两 bug）；
  **无任何吞吐数字**；`output/zeroflush_m1_perf/` 从未入库（README 引用的 report.html 已随
  e3021f5 清理）。M1 遗留 5 项限制（WAL 无限增长、zfwal 不被 DestroyDB 清理等）。
- **M2（Epoch 封存/物化/回收）**：纯设计+实现里程碑，验收标准 G1–G6；**无独立实验数据**，
  G6（并行物化 K=8 ≥2×）在 M3.2 回填验证（K=8 1.53s/批 vs K=1 10.68s，7×）。

---

## 4. M3（范围路由 + 直装 base level，08-20 → 09-03）

**4.1 早期 50GB 基准（vs256/vs1024）**：写慢 36.6×/15.8×；但空间 ZF 35.16/35.20GB vs
原生 40.56/40.76GB（−13%）、256B 读 ZF 96,554 vs 11,599（8.3×，P99 248µs vs 14,433µs）；
读 1KB ZF 4,115 vs 30,250（落后）。写放大 ZF 0.70 vs 原生 0.81/0.82；fill P50 ZF
1,440.7/1,894.2µs vs 原生 14.3/20.4µs；总耗时 16.2h/7.5h。

**4.2 写路径剖析（W0–W3/W2b，2.2GB）**：W0 原生 1,031,788；W1/W2/W3/W2b ≈
10.9K/10.9K/10.9K/10.4K；瓶颈定位 = **DB mutex 下写路径全序列化**（~92µs/op、~11K
天花板）；物化并行与停写均非瓶颈（根因②③证伪）。单线程 P50：原生 5.35µs、
ZF P=64 52.0µs / P=1 32.3µs / P=16 44.9µs。

**4.3 L0→L1 重要性（100GB/1KB，L1=256MB/1GB/4GB）**：吞吐 28,374/27,694/**35,402**；
单 job 平均输入 SST 11/27/80 个、耗时 8.9/13.0/28.3s（P99 18.9/24.4/50.2s）；L0→L1
写出 121.2/169.5/279.8GB（占 12.5%/20.0%/41.1%）；总写放大 9.6/8.5/6.8×；停顿 91.1/91.5/85.7%。

**4.4 L0 结构性瓶颈（三角度）**：8 组运行区间重建 + T 扫描 + 参数扫描。625 个 L0→L1
job 中**最大并发恒 1**（系统利用率 95–100%）；T=2/4/8/16 → 43,530/55,722(默认)/49,682/
45,839（默认已是倒 U 顶点）；L0 delay 100/117/190/598；停顿占比 76–95% 无逃逸。

**4.5 原生 100GB 停顿基线（2/4/8/16T）**：吞吐 10,539–13,978 ops/s；wall 123–163min；
停顿 89.2–94.6%（memtable stop）；总写放大 9.8–10.5×（SST 写出 980–1,065GB）；
L0→L1 单 job avg 15.7–18.1s，深层累计为 wall 的 1080–1116%。

**4.6 E6 24 配置矩阵（10/50/100GB × 128/256/1024/2048B）**：写 ZF/原生差距随规模收窄
（10GB 1.31–5.53× → 50GB 1.20–1.27× → 100GB **1.04–1.39×**）；读 50GB 1.33–1.85×、
**100GB 0.45–0.96×（ZF 全面反超）**；命中率全部 63.1–63.3%（= 理论期望，零丢失）；
100GB 读 P99 ZF 809–858µs vs 原生 2,622–6,237µs。

**4.7 证伪类实验（09-02/03）**：`cmp_50gb_planA`（方案 A）20,030（−26%）；`cmp_50gb_tr32`
（trigger=32）19,056；`cmp_50gb_skipbatch`（skip_batching=true）22,012（−19%）——全部证伪；
`bench_matrix_v2.json` 为中断的中间产物（50GB 原生 128B 164,766 → 2048B 13,693 ops/s）。

---

## 5. M4 旧架构（M4.0–M4.11，08-21 → 08-29）

| 编号 | 实验 | 关键数字 |
|---|---|---|
| M4.0 | 路由预演（2.2GB） | W1 hash 10,898 / R1 直装 7,731(−29%) / R2 融合 7,608(−30%) / **R3 P=16 12,082(+11%)**；L1 重写 2.99/13.09/12.30/2.62GB |
| M4.1 | 去串行化（2.2GB） | W1 10,898→**21,594（2.0×）**、R3 12,082→**38,129（3.2×）**、P50 1,279→138µs、micro 110K+/T=32 147K；修复 follower 丢数据（100K ops 只落 3.8MB WAL，读回 11.7%） |
| M4.2b | L1 对齐 | R7（2.2GB）111,311（2.9×）、P50 129µs；R8（50GB）22,751 vs 3,387（6.7×），但回落 L0 3,199（99.7%）、停写 52%；R9 ratio=0.05 融合风暴终止 |
| M4.3–4.6 | 分区独立 compact | M4.3a 157K；R14 105.7K；R15/R17 50GB 中途停滞（0.03/0.12MB/s）；**R20 50GB 完整跑通 1.9h、28,812 ops/s**、写放大 0.73、命中 63.3% |
| M4.6c–4.6e | 并行消费调优 | R36/R40 49.1–49.9K（+70%）、停写 10.5–11%；R42 55.4K（+92%）、59.4min；R43/R44 60.0K/66.9K；sort 575.9s |
| M4.8–4.11 | 参数级优化 | M4.10c 索引序物化 +120%（回滚）；M4.10d 微优化全负；M4.11 切片 78.6K vs 原生 88.8K（差距 11%）；M4.11c 布隆净收益 ~10%（69.4K→85.7K） |

**M4.8/M4.9 对照（`rocksdb_vs_zeroflush_comparison.md`）**：2.2GB/1KB 251.6K vs 80.0K（3.1×）；
10GB/1KB 109.7K vs 32.3K（3.4×）；50GB/1KB 原生 14.0K vs ZF 12.3K（1.14×）、读 157.0K vs
74.6K；受控读 ZF 69.4K / native-on-zf 80.2K / native 85.4K（真实路径开销 16%）。

---

## 6. M4 DG（动态治理，09-09 → 09-11 验收）

- **首轮（gov_bench 偏斜负载）**：单线程 ~47MB：OFF 2,434.8 vs ON 2,436.6 ops/s（+0.07%）、
  P 8→6；8 线程 ~2.88M 写：46,649.4 vs 46,467.2（−0.39%）、iter 差 3 条。结论：无吞吐回归、P 有界。
  验收中发现并修复在途写组计数泄漏。
- **50GB 全量（51.2M ops≈50GiB）**：ON **40,196** vs OFF **29,801** ops/s（+34.9%）、
  wall 1,506/1,718s、终态 P 5/8、splits·merges 1·4 / 0·0、read 命中 83.88%/83.89%、
  0 Corruption、reopen 一致。
- **100GB 全量（104.86M 写）**：ON **22,545** ops/s、4,651s（77.5min）、命中 86.04%、
  0 Corruption；**OFF 两次卡滞**（~69% 处，L0=23,162 文件/11.25GB、L1 score≈10）；
  `ZF_MAX_L1_BYTES=1TB` 禁原生 L1→L2 后卡滞消失（~100K ops/s）但 66% 处磁盘耗尽。
- **基线变体（partitions=256）**：44.3M 写处 L0 78,926 文件/41.8GB、写未停 → 与分区数无关。
- **L0 消费三路径验证（09-11）**：`ZF_L0_TRIGGER=8/1` 均追不上到达率（1.6k 文件/百万写）；
  门控解耦引发 7 次 L1 重叠崩溃后撤销。
- **M4.9 merge_base=1 崩溃量化**：256 分区/16T：0.4–0.7M 写干净、**0.8M 写必崩**（8/8 复现）；
  8 分区 0.8M 崩；256B×3.2M（≈0.8GB）崩；1 线程 1.0M 干净。触发条件 = **merge_base=1 ×
  并发 × ≥0.8GB**；默认验收配置不触发。
- **M4.9 修复尝试（三次负结果，09-11）**：相邻分区互斥 8→11 次崩溃；安装期复检 0 触发；
  窗口标记仍 8/10 崩。登记探针证实后批在对方 `LogAndApply` 让锁窗口内规划 →
  方案 `zf_install_mu_`，**尚未落地**。
- **测试状态**：zf_test 53/53 ALL PASSED（09-11）；CrashMidActiveSplitReopen 3/3；
  活跃尾 split 单次 283k ops/s、默认开复跑 226–230k ops/s。

---

## 7. M5（收口，09-04 → 09-12）

- **设计验收目标（09-04，非实测）**：50GB 装 L0=0、命中 63.2%、写放大<9×、吞吐>27.1K。
- **50GB 数据丢失修复（09-07）**：根因 recovery 转换循环（命中 32.8% → 63.26% 零丢失）；
  fill 15,452 vs native 20,752（0.74×）；read ZF 188,086 vs 137,972（**反超**）；
  L0 终态 3 文件/0.02GB；物化占 wall 73%。
- **50GB/100GB 验收**：见 §6（40,196 / 22,545，0 corruption）。
- **merge-off 50GB DNF（09-11）**：5h07m 被杀；SST 70.8GB/2,227 文件、space amp 1.49×、
  long op 1,569 次（~1.6s）、epoch 1,121；install 4,536 次/110.3GB。
- **merge-off 复跑中止（09-12）**：2h1x 中止；install 11,312 次/**276.9GB（5.08×）**；
  **原生 compaction 输出 4,019.6GB（83.7×）**；设备 SMART +4.57TB（93.7×）；最大单次
  compaction ~2.98GB/3.05M 条（= 整分区）；`l1score` 6.08→7.52 全程钉住；user 吞吐 ~7k ops/s。
  对照 merge-ON（09-02）install 1,315 次/55.7GB（**1.17×**）、27,066 ops/s、30min 跑完。
  详见 `rerun_20260912/ABORTED_RUN_AMPLIFICATION.md`。
- **原生 50GB DNF（09-12）**：16h13m 未出结果行、DB 85GB（amp 1.79×）；环境根因 = SSD
  flush/fsync 延迟尖峰（并发 max 6,745ms vs 空闲 0.67ms）+ 盘温 84.8–85.8°C（阈值 83.8°C）
  + 830GiB 未 TRIM 空间；修复（fstrim/WBT=0/commit=60/风扇）后探针恢复（冷 228,154、
  热 177,095 ops/s）。详见 `50gb_1kb_merge_off/ENV_DIAGNOSIS.md`。
- **崩溃矩阵/带宽限流（09-10/11）**：CrashMidActiveSplitReopen 3/3（P∈{4,5}、种子零丢失）；
  rewrite 带宽 ≤25%（EWMA 双速率 + RewriteBudgetOk，无量化吞吐数字）；§5 文字对齐因文稿缺失阻塞。

---

## 8. 其他对照

- **vanilla_100gb 正式基线**：**从未产出**（`[已归档 /home/embed/hyl/metadata_offload-archive/abandoned-baselines/]output_vanilla_100gb/` 仅 smoke_run 3.87GiB）。
- **R47e 历史 1KB/100GB**（原始文件已删）：原生 23.8K vs ZF 12.4K（1.92×）。

---

## 9. 数字冲突与口径说明（并列保留）

1. W1 基线：`profiling_report.md` 10,898 ops/s / P50 1,511µs ↔ `W1/result.json` 21,594 /
   P50 3.70µs（后者为 M4.1 后重跑）。
2. 100GB OFF 卡滞写量：`REPORT.md` ~72.4M(69%) ↔ `ROADMAP.md` 73.9M(70%) / 72.5M(69%)。
3. 100GB ON splits/merges：REPORT 表内先写 1/4、后文更正为 **1/3**（以更正为准）。
4. M4.9 崩溃对手方：commit 1be7fff 断言"原生 compaction"、05c3fa9 更正为"两条均为 ZF L1
   输出"（以更正为准）。
5. 50GB merge-ON 吞吐：27,066（09-02 b2bd1fe）↔ 40,196（09-10 active-tail 修复后）——
   不同配置/日期，非同一实验。
6. M5_DESIGN 的 27.1K 目标来自旧配置时代；同窗 native 实为 20.75K（文档已注明）。

## 10. 数据完整度

- **有原始日志/JSON（可复核）**：exp0–exp2（阶段报告 + 权威 JSON，但 `output/pre_exp/` 原始
  run 目录缺失）；M3 profiling W0–W3/W2b（stdout+result.json+samples.csv）；cmp_1kb50/100、
  zf09_100gb、cmp_50gb_{planA,tr32,skipbatch}（fill/read 原始 log）；native_wa_stall 100GB 四线程
  （JSON，stdout/LOG 已删）；l0_l1_importance / l0_bottleneck（summary JSON/CSV）；E6 matrix.json；
  vanilla smoke；M4 DG 50/100GB 与 OFF 卡滞现场（`output/zeroflush_m4_perf/logs/*.log.gz` 5 份）；
  M5 merge-off（zf_fill.log / native_fill.log / run2_telemetry.log / DNF evidence / ENV_DIAGNOSIS.md）。
- **仅有结论/汇总数字**：M1（回归 PASS）、M2（设计）、M4 旧架构 R3/R7/R8/R15–R44 全部、
  M4.6/4.8/4.9 多数实验、M5 50GB 修复验收、崩溃矩阵与带宽限流。
- **缺失/不可用**：exp0–exp3 原始目录、`output/zeroflush_m1_perf`、`output/zeroflush_m2_perf`、
  vanilla 100GB 正式运行、R42/R47e 等 LOG 归档（ff0d060 清理）、bench_matrix_v2（中断）。

## 11. 结论性曲线（写/读随规模的收敛）

| 规模/值大小 | 写（ZF vs 原生） | 读（ZF vs 原生） | 出处 |
|---|---|---|---|
| 2.2GB/1KB（M3 早期） | 3.1× 落后 | — | rocksdb_vs_zeroflush |
| 10GB/1KB | 3.4× 落后 | — | 同上 |
| 10GB（E6 矩阵） | 1.31–5.53× 落后 | — | bench_matrix |
| 50GB/1KB（09-02 同窗） | **1.025× 领先** | 0.18×（63,284 vs 356,717 = 磁盘受限） | cmp_1kb50 |
| 50GB（E6） | 1.20–1.27× 落后 | 1.33–1.85× 落后 | bench_matrix |
| 50GB（M4 DG 09-10） | ON 40,196（+34.9% vs OFF） | 命中 83.88% | m4 REPORT |
| 100GB（E6） | **1.04–1.39× 落后（最优档）** | **0.45–0.96× 反超** | bench_matrix |
| 100GB（M4 DG 09-10） | ON 22,545、0 corruption | 命中 86.04% | m4 REPORT |
| merge-off 50GB | **不可收敛（5.08× 物化 + 83.7× compaction）** | — | ABORTED_RUN_AMPLIFICATION |

**一句话**：ZF 与原生写吞吐差距随规模放大而收窄（10GB 3× → 50GB 1.02–1.27× → 100GB 1.04–1.39×），
读在 100GB 反超；动态治理把 50GB 拉高 34.9%；**merge 关闭 + 50GB 全域随机键是当前唯一
不可完成的结构性问题**（L1 目标 1GB vs 分区 3GB → sink 整分区反复重写）。

> **⚠ 跨 harness 不可直接比**：§1 表中 gov_bench 的 40,196/29,801/22,545 与本表的
> db_bench 数字（27,066、19,701、3,387…）**不是同一负载**。gov_bench 口径 = "写**流量**
> 50GiB/100GiB"，其中 85% 写入集中在 12M/18M 键的 hot band（universe 的 12%），
> 覆盖写在 LSM 内折叠 —— 50GB 臂实测活数据仅 **19.0M 条（≈19GB）**、读命中 83.9%；
> 而 db_bench fillrandom 写 48M 个**互不相同**的随机键 = **48GB 活数据**、读命中 63.2%。
> 可比的只有：同 harness 的 ON/OFF A/B（+34.9%）、同 db_bench 协议的横向对照。
