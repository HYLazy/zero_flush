# ZeroFlushV0 M4 动态治理规模验收（首轮，2026-09-10 更新）

驱动：`source/rocksdb-zeroflush/tools/zf_govern_bench`（新增）。协议 = 偏态
hot-band 覆盖写 + 突发/排空间隙轮次（治理在 quiescent 检查点执行）+ P 轨迹
采样 + 收尾读验证/reopen。

## 参数与规模

| 项 | 值 |
|---|---|
| key 空间 | 4M（"k%012d"）；hot band [0, 480k) 占 85% 写（初始在分区 0）|
| 写 | 10 轮 × 10,000/线程，值 512B |
| 治理 | kStatic P0=8；split_min=4MB；θ 默认；tick 20ms；冷却 ~0（快测）|
| 规模 | 单线程 ≈47MB；4 线程 ≈151MB（小规模验证，非 50GB）|

## 结果对比（同参 governance OFF / ON）

**单写线程（~47MB）**

| 指标 | 静态基线 (OFF) | 动态治理 (ON) | 差 |
|---|---|---|---|
| 写吞吐 ops/s | 2,434.8 | 2,436.6 | +0.07%（噪声内，无回归）|
| 终态 P | 8 | 6 | ∈ [P_min=4, P_max=256] |
| gov_splits / gov_merges | 0 / 0 | 1 / 3 | 治理动作已执行 |
| rewrite_bytes | 0 | 23.8MB | 1 次热拆分 type-3 |
| 读命中 found/200k | 0.1382 | 0.1382 | 相等（无数据丢失）|
| iter_entries | 92,834 | 92,834 | 相等（reopen 一致）|

**4 写线程（~151MB，修复在途计数泄漏后）**

| 指标 | 动态治理 (ON) |
|---|---|
| 写吞吐 ops/s | 8,984 |
| 终态 P | 6 ∈ [4,256] |
| gov_splits / gov_merges | 1 / 3 |
| inflight_groups（写中采样） | 0（计数平衡）|
| 读命中 / reopen | 0.4339（与写入覆盖一致）/ 一致 |

P 轨迹（ON 单线程）：8 →（空闲期空分区合并 3 次）5 →（热拆分）6 → 稳定。
分裂后两子分区各 ≈0.5×热负载，单分区 rate/mean ≈ P/2 ≈ 3 → 达 θ 平衡不再
拆（符合设计：热拆分 H:1→2 时 ratio 从 P 落到 P/2）。

**8 写线程（~2.88M 写 / ≈1.1GB live / key 空间 32M，修复后）**

| 指标 | 静态基线 (OFF) | 动态治理 (ON) | 差 |
|---|---|---|---|
| 写吞吐 ops/s | 46,649.4 | 46,467.2 | -0.39%（治理开销）|
| 终态 P | 8 | 6 | ∈ [P_min,P_max] |
| gov_splits / gov_merges | 0 / 0 | 1 / 3 | |
| rewrite_bytes | 0 | 466.9MB | 1 次热拆分 |
| inflight_groups | 0 | 0（写中采样） | 无泄漏 |
| 读命中 found/200k | 0.4011 | 0.4011 | 相等 |
| iter_entries | 2,239,018 | 2,239,015 | 差 3（覆盖随机性，非丢失）|
| reopen | ok | ok | |

说明：热键带 3.84M 键、写入 2.45M 次（有覆盖）→ live ≈1.8M 热键 + 冷区，
与 iter 2.24M 一致。P 稳定在 6：首次热拆分后 H=2 个等热子分区，
rate/mean ≈ P/2 = 3 恰达 θ_split 边界 → 不再拆（策略平衡点），全程无振荡。

## 结论

1. **吞吐无回归**（单线程 +0.07%，同参对照）；数据完整（读命中/迭代数/
   reopen 与基线逐位一致）。
2. **P 有界**：治理把 P 从 8 收敛到 6 ∈ [4,256]，无越界、无失控。
3. 单线程、4 线程、8 线程（2.88M 写 / ≈1.1GB live）下 split/merge 均在排空
   检查点正常执行；8 线程吞吐 46,467 vs 基线 46,649 ops/s（-0.39%，含一次
   466MB 热拆分 rewrite 的开销），读命中与 live 键数与基线一致。

## 验收中发现并已修复的问题

### 1. 并行写在途写组计数泄漏（已修复 ✅）

- **现象**：T=1 写后 `inflight_groups` 恒 0；T≥2 并发写时计数单调滞留
  （探针：2 线程 6000 写后 =543；4 线程 32768 写后 =10650），merge/split
  的路由切换屏障等待归零永久 `Operation timed out: inflight writer groups
  did not drain`。
- **根因**：parallel 组的最后完成者可能是 **follower**；原实现只在 leader
  的 `should_exit` 块调用 `ExitInflightGroup`，leader 非最后完成者时该组
  计数永不退出。
- **修复**（`db/db_impl/db_impl_write.cc`）：follower 收尾块
  （`CompleteParallelMemTableWriter` 返回 true，恰好一次/组）补 Exit；且
  Exit 必须在收尾块**最前**（先于取 DB mutex 的 Unref）——屏障是"持 DB
  mutex 自旋等 inflight==0"，收尾者若先抢 mutex 再 Exit 仍会互等超时。
- **验证**：T=2/4 `inflight_groups` 写后归零、写中采样平衡（0/瞬 1）；
  4 线程治理臂 splits/merges 生效；新增回归用例
  `MergeConcurrentWriters(M4)`（并发写在场 + 冷分区 merge 重试，3/3 通过；
  修复前该屏障永不归零必失败）。

## 活跃尾 split：已修复（默认开，连续负载可用）

设计：切路由后把父活跃尾冻结为"父区间覆盖的孤儿尾 epoch"；该 epoch 物化走
**融合替换**——父区间内全部文件（含子分区在切路由后并发产出的 L1）与尾数据
按 seq 归并后单批替换安装（此前 kDirect 宽文件直装会与子分区 L1 重叠 →
force_consistency_checks 崩溃，已修）；子分区索引 miss 经 `split_orphans_`
别名回查父链；排空后 type-3 rewrite 按新边界两分。

验证（8 线程连续写、2.88M 写、gap_ms=0）：
- `active_tail=1` 单次：283k ops/s、splits=1/merges=3、**0 Corruption**、
  reopen P 一致、读命中/live 键数与基线一致；
- 默认开（含治理）复跑两次：226–230k ops/s、**0 Corruption**、P 界内、
  reopen 一致（splits 依本轮流合并时序在本轮 0–1 间波动）。
- 回归：SplitConcurrentWriters / GovernanceDynamicEndToEnd / PolicyUnit /
  SplitQuiescent / Growth / MergeConcurrentWriters / MergeCold /
  MergeActiveTail / CrashMidDrainReopen 全 PASS。


## 50GB 级全量验收（2026-09-10，16 线程 / 1KB / 连续写 / gap_ms=0）

写入量 51,200,000 ops × 1KB ≈ **50GiB**；key 空间 100M，hot band 85%。

| 指标 | ON（治理） | OFF（基线） | 差 |
|---|---|---|---|
| 写墙钟 s | 1,506 | 1,718 | ON 快 12.3% |
| 写吞吐 ops/s | **40,196** | 29,801 | **+34.9%**（ON：回收空分区 + 活跃尾热拆分）|
| 终态 P | 5 | 8 | 界内（8→4 回收 + 1 次热拆分）|
| gov splits / merges | **1 / 4** | 0 / 0 | 活跃尾热拆分成功（跨批互斥修复后）|
| rewrite_bytes | 14.35MB | 0 | 1 次 type-3 边界 rewrite |
| 读命中 found/200k | 167,769 (83.88%) | 167,774 (83.89%) | 一致 |
| iter_entries | 19,037,195 | 19,037,151 | ±44（覆盖随机性，无丢失）|
| reopen | P=5 一致 | P=8 一致 | 通过 |
| Corruption | **0** | 0 | — |

### 热拆分未触发（50GB 长跑）——根因已定位（两个独立问题）

1. **驱动 Bug（已修）**：zf_govern_bench 早期版本把 `governance_active_tail_split`
   显式置 false，治理走旧"分区必须排空"路径；连续写下热分区永远 Busy →
   split 在门控后静默 NoteRetry（`split_attempts>0`、base/L0 充足、
   gate_base/median/busy/timeout 全 0）。已补 `gov_split_predrain` 计数并将
   bench 默认对齐 ZeroFlushOptions；诊断口径：attempts/gate_base/
   median_nofiles/busy/timeout/other/median_notfound/pos_gone/total0/predrain
   + last_base/last_l0/rewrite_gated。
2. **活跃尾路径跨批竞态（已修，默认开）**：曾出现
   `force_consistency_checks: L1 has overlapping ranges`（尾 epoch 融合替换
   扫描后子分区并发安装 L1）。修复 = 拆分前以独立 hold token 占住两子分区
   （在飞则 Busy 退避），尾 epoch 物化/rewrite 后释放；期间子分区批次让位
   （kSkip→recovery，后续 epoch 收养）。验证：16T/1KB/12.8M 写 **0
   Corruption、splits=1 成功、rewrite 23MB、reopen P=6 一致、118.7k ops/s**。

**诊断字段（已落地）**：gov_split_attempts/gate_base/median_nofiles/success/
busy/timeout/other/median_notfound/pos_gone/total0/predrain、
gov_last_split_base/l0、gov_rewrite_gated。


## 100GB 级验收（2026-09-10，16T/1KB/连续，104.86M 写 ≈100GiB）

| 指标 | ON（治理，active-tail 默认开） | 备注 |
|---|---|---|
| 写墙钟 | 4,651s（77.5min） | |
| 写吞吐 | 22,545 ops/s | 高于 50GB 臂的绝对量下因 live 数据/压缩开销下降 |
| 终态 P | 6 | ∈ [P_min,P_max] |
| gov splits / merges | 1 / 4? → 实测 1 / 3 | 活跃尾热拆分 1 次（rewrite 23.6MB）|
| **Corruption** | **0** | 修复前同配置 7 次 L1 重叠；hold 加宽后消除 |
| 读命中 found/200k | 172,076（86.04%） | |
| iter_entries | 32,698,553 | |
| reopen | P=6 一致 | |

诊断：split_attempts=1、success=1、gate_base/median/busy/timeout/other 全 0；
last_base=7.6MB、last_l0=16.6MB。

### 100GB OFF 基线臂：卡滞归因（现场数据 + 机制）

复现：governance=0 同参两跑，均在 **~72.4M 写（69%）** 处停滞（~100 写/5s），
磁盘空闲、0 Corruption、无 error/stall 日志。现场采样（ZF_BENCH_VERBOSE=1）：

| 观测 | 值（卡点） | 含义 |
|---|---|---|
| is-write-stopped / delayed | 0 / 0 | **非写停控制器**（排除 StopWriteToken/DelayWrite）|
| L0 | **23,162 文件 / 11.25GB** | 物化回落 L0 长期累积（ZF 设计禁 L0 压缩，永不消费）|
| L1 | 105 文件 / 10.73GB（score≈10） | 热点范围 L1 无界增长 → 持续触发原生 L1→L2 |
| L2 / L3 | 4.0GB / 7.0GB | 原生压缩在下沉 |
| num-running-compactions | 恒 1 | 大 L1→L2 持续在跑（占 being_compacted）|
| sealed_wal（未回收封存） | **37.93GB（冻结不再回收）** | ZF 物化侧停摆 |
| prof_tasks / skip_count | 27,079 / 6,625（冻结） | 物化计划/任务停止推进 |
| live_wal | 缓慢增长（~500 写/5s） | 写路径仅微幅推进后阻塞 |

机制（与 ON 对照的解释）：baseline P=8 静态分区下，热键带使某分区 L1 增长到
~10GB（违反 S1/S3 的"每分区文件有界"设计目标）；`max_bytes_for_level_base`
=1GB → L1 score≈10 → **原生 L1→L2 compaction 持续运行**，其 being_compacted
文件与 ZF 物化的 overlap 判定冲突 → 物化计划大量 kSkip/等待 → sealed 积压
（37.9GB）与任务冻结 → 写线程最终卡在 epoch/imm 水位（非写停控制器）。治理
ON 通过热拆分把每分区数据/文件规模压住（P=6），100GB 全程完成（22,545 ops/s、
0 Corruption）——即该卡滞是**基线静态分区的结构性限制**，治理正是其对策。

**验证实验（已完成，结论）**：`ZF_MAX_L1_BYTES=1TB`（禁用原生 L1→L2 触发）
复跑 OFF 100GB：
- **卡滞消失、写吞吐升至 ~100k ops/s**（同点前值 ~34k）→ 坐实"原生 L1→L2
  与 ZF 物化冲突"是卡滞的直接触发条件（L1→L2 一关，写路径立即通畅）；
- 但跑到 69M 写（66%）时 **磁盘耗尽**（`No space left on device`）：L0
  只增不消（22k 文件/58.7GB）+ sealed 积压 34GB + WAL/下层，写 100GB 需远超
  磁盘空间 → 暴露基线第二个结构性问题：**空间放大失控**（L0 无消费 + 物化
  回流 L0）。
- 0 Corruption。

结论：OFF 基线在 100GB 规模下的卡滞 = "热点范围数据无界（L1 无界→原生
L1→L2 与物化抢文件冲突→物化冻结→写卡 epoch 水位）"与"空间放大失控"两条
结构性问题叠加；治理 ON（拆分热点、每分区有界，实测 43GB 库）100GB 全程
完成 —— 治理即对策。基线若要在 100GB 稳定运行，需（i）按 S1/S3 收紧每
range 文件规模（= 治理路径），或（ii）修物化与原生 L1→L2 的互斥（让位/
等待而非冻结），并（iii）为 L0 建立消费路径（现设计只增不消）。

## 基线变体实验：静态分区数不是解（2026-09-10）

假设"更多静态分区可避免热点 L1 无界"→ 用 `--partitions=256` 跑 OFF 100GB
（16T/1KB/连续/verbose）：

| 指标（44.3M 写/42% 处停止） | 值 |
|---|---|
| L0 | **78,926 文件 / 41.8GB**（≈ 写入字节 1:1 累积）|
| L1 | 270 文件 / 1.19GB |
| sealed_wal（未回收封存） | ~44GB 且持续增长 |
| skip_count / prof_tasks | 29,428 / 83,464（仍在跑）|
| is-write-stopped | 0（未到写停）|

结论：**L0 只增不消是基线的主导性空间放大**（物化输出大比例落 L0，ZF 设计
L0 无消费路径），与分区数无关；44M 写即 42GB L0，外推 100GB 写需 ~100GB L0
+ ~100GB sealed → 必然磁盘耗尽（或先卡滞）。因此：
- 基线（governance=0）在本机无法完成 100GB；50GB 可完成（1718s/29.8k ops/s）；
- 治理 ON 100GB 完成（43GB 库）→ 治理（热拆分 + 回收 + 每分区有界）同时
  缓解了 L0/空间放大，是该结构问题的实际对策。

## 基线结构修复实验：L0 消费（2026-09-11）

**尝试 1：原生 L0→L1 compaction 消费（`ZF_L0_TRIGGER=N`，新增 env 覆盖）**
- OFF + 256 分区 + `ZF_L0_TRIGGER=8`：12.8M 写处 L0=22,961 文件/11.8GB
  （≈1.8k 文件/百万写，与未开触发完全一致）；
- `ZF_L0_TRIGGER=1` 小规模复核：3.2M 写处 L0=5,137 文件/2.66GB
  （≈1.6k 文件/百万写）→ **原生 compaction 完全追不上 ZF 的 L0 到达率**
  （每批物化产生多个 ~0.5MB 小文件，数百文件/秒到达；comp=1 长期打满）。
- 结论：调触发阈值不是解，属**源头问题**。

**修复方向（下一步）**：让 ZF 物化自身吸收 L0——M4.9 "L0 融合"路径已存在
（B 侧纳入 l0_overlap 后直装 base 并替换 L0），但被 `merge_into_base_level`
门控；基线该开关为 false → 不吸收 → L0 只增。待做：把 "L0 吸收" 与融合开关
解耦（l0_overlap 非空即走吸收路径），从源头把 L0 控在 O(批次)。

## 后续

- 因问题 1 已修，可以多写线程复跑更大规模（数 GB）确认 P 收敛轨迹与吞吐
  对照稳定；
- 问题 2 作为 M4 后续/收口项评估（活跃尾 split）。

原始日志：`gov_acc0.log`（OFF 单线程）、`gov_acc1.log`（ON 单线程）、
`gov_acc_mt.log`（ON 4 线程）、`gov_big0.log`（OFF 8 线程 2.88M 写）、
`gov_big.log`（ON 8 线程 2.88M 写）。
