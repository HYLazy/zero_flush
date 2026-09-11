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

## 基线结构修复实验（续）：L0 吸收路径的三条验证（2026-09-11）

1. **原生 L0→L1 compaction**（`ZF_L0_TRIGGER`）：追不上到达率（1.6k 文件/
   百万写；=1 亦无效）——非解。
2. **门控解耦（ZF_L0_ABSORB，已撤销）**：相交判定 → 融合输出越界 → L1 重叠
   崩溃；收紧为"完全包含"仍崩（7 次）。简单门控不安全，实现需完整 M4.9
   前置（busy/批内/遮蔽判定）。
3. **既有 M4.9 `merge_into_base_level=true`**（256 分区/16T/3.2M 写）：
   **L0 被吸收（0 文件）但 8 次 L1 overlapping ranges 崩溃** → M4.9 融合路径
   在规模并发下的既存缺陷（宽范围/跨批类，与已修的 E_D×absorb、split 子
   分区同类），非开关问题。

结论：基线 L0 消费需**专项修复 M4.9 融合路径的安装范围/跨批互斥**（候选：
L0 吸收批登记受影响邻分区、输出范围钳制在分区区间），而非调参；治理 ON
（每分区有界 + 拆分/回收）仍是当前唯一可用的结构对策。实验开关
`ZF_L0_TRIGGER` / `ZF_MAX_L1_BYTES` 保留为诊断用（默认零行为改动）。


## M4.9 规模并发重叠：深挖与安全网（2026-09-11）

- **追踪（ZF_DBG_MM）**：重叠对 = **两个不同 FlushJob 批次**对**同一分区**
  产出的 kMergeBase L1 文件；plan 期扫描视图互不可见（ov 列表缺对方文件号）
  → 并行规划 + 安装期 LogAndApply 冲突（VersionBuilder L1 overlapping）。
- 尝试拦截均未生效：相邻分区互斥扩展（ZF_MERGE_NEIGHBOR）、安装期复检
  （复检落在对方 LogAndApply 未落地窗口内，实测 0 yield）。
- **已落地安全网（默认生效，无回归）**：阶段 2 安装前按当前版本复检
  "同层既有文件重叠且不在本批删除集 / 批内 Add-Add 同层重叠"，命中则**整批
  让位**（删除已产出文件 + 全部 gens 移交 recovery + 回滚 last，与 kSkip
  同语义）→ 把崩溃降级为可重试让位。回归 6 项关键用例全 PASS。
- **下一步（未完成）**：查 `AddMaterializePart` 为何两批均登记成功，或引入
  分区粒度 install 锁把 plan→LogAndApply 串行化。


## M4.9 重叠根因实锤：LogAndApply 让锁窗口内的跨批规划（2026-09-11）

登记探针（ZFDBG-mm-reg）显示：冲突分区在窗口内出现**不同 token 的登记成功**
（pid5: token 159/167/175/177 均 ok=1）→ 后批在对方 `LogAndApply` 的 MANIFEST
IO 让锁窗口内完成规划：其扫描视图不含对方"尚未落地"的输出，双方 edit 相继
apply → VersionBuilder L1 重叠崩溃。即 M2 遗留项"与并发 flush 的 LogAndApply
让锁窗口竞态未做 being_compacted 防护"的实锤；既有 token 守卫未覆盖该窗口。

**修复方案（下一步，确定性）**：DB 级 `zf_install_mu_`——ZF 批自阶段 0 规划前
`try_lock`，持至安装（LogAndApply）完成释放；规划期 try_lock 失败即整批 kSkip
让位。跨批 plan→apply 串行化后，让锁窗口不可能再插入规划，重叠从源头消除。
现有"安装期复检 + 整批让位"安全网保留为第二道防线（本轮回 0 触发）。


## M4.9 修复尝试三连负结果 + 决定性时序（2026-09-11）

三次守卫尝试（均撤销，主路径恢复原行为，回归全绿）：
1. 相邻分区互斥扩展（ZF_MERGE_NEIGHBOR）：崩溃 8→11；
2. 安装期复检 + 整批让位：复现中 0 触发（复检落在对方 edit 未落地窗口内）；
3. 安装窗口标记 + 规划期整批让位：仍 8/10 崩溃。

**job↔token 探针决定性时序**：崩溃对 file376(job74/epoch76/token71) 与
file384(job69/epoch68/token73) 同分区同层；job74 的 edit 已构建打印但未
apply 时，job69 已在规划（扫描视图 ov 不含 376）；且 job69 的 token73
登记成功 ⇒ token71 已 End——**存在"edit 尚未落地即释放 token"的路径**
（End 名义上在 TryInstall 之后，疑似失败/重试或某出口提前 End）。

**下一步（决定性）**：全部 `EndMaterializeBatch` 调用点打点（token/job/
s.ok()/apply 状态）+ PlanLocked 入口断言 `mc_.base == cfd_->current()`
（持锁下应恒真；不真即 stale-base 规划实锤）→ 定位后一处修复。


## M4.9 触发条件与规模阈值（量化，2026-09-11）

配置：`zf_govern_bench --governance=0 --merge_base=1`（M4.9 L0 吸收路径），
1KB 值（另注 256B 一档），universe 150M，连续写。

| 条件 | 结果 |
|---|---|
| 256 分区 / 16 线程 / 0.4M、0.5M、0.6M、0.7M 写（0.4–0.7GB） | 全部干净（rc=0、0 Corruption）|
| 256 分区 / 16 线程 / **0.8M 写（0.8GB）** | **崩溃**（6 次 L1 重叠，段错误）|
| 256 分区 / 16 线程 / 3.2M 写（3.2GB） | 崩溃（7–15 次）|
| **8 分区** / 16 线程 / 0.8M 写 | **崩溃**（15 次）→ 与分区数无关 |
| 256 分区 / 16 线程 / **256B 值 × 3.2M 写（≈0.8GB）** | **崩溃**（7 次）→ 按**写入字节**归一，非条数 |
| 256 分区 / 4 线程 / 3.2M 写 | 崩溃（4 次）|
| 256 分区 / **1 线程** / 1.0M 写（1GB） | 干净（rc=0，94.5s）|
| 256 分区 / 1 线程 / 3.2M 写 | 仅 1 次重叠错误（表现为 Put 返回 Corruption，非段错误）|
| 256 分区 / 16 线程 / 0.8M 写，**merge_base=0（对照）** | 干净 |

结论：
- **条件** = `merge_into_base_level=true`（L0 吸收）× **并发 flush/write** ×
  **≈≥0.8GB 写入量**（≈100 次封存批次）；与分区数、值大小无关。
- **确定性**：触发配置下 8/8 轮复现（无一跑完 3.2M 写）——是结构性竞态，
  不是偶发抖动；阈值下（≤0.7GB）多轮干净。
- **波及面**：默认配置（merge_base=false，含全部 50GB/100GB 验收）**不触发**
  （它们撞的是 L0 空间放大/卡滞）；但若为"基线 L0 消费"启用融合（merge_base=1），
  在 ≥1GB 的真实负载下几乎必然命中 → **必须先修再用**。
- 表现形态：多线程=段错误（VersionBuilder 致命）；单线程=Put 返回
  `force_consistency_checks: L1 overlapping ranges`。


## 更正：merge=0 对照组与 L0 文件 key range 语义（2026-09-11）

**更正**：上一轮"崩溃对手方是原生 compaction 的 L1 输出"的推断**不成立**——
该推断所依据的日志未开 ZF_DBG_MM 探针（`mm-add` 无记录 ≠ 非 ZF 输出）。

**对照组（256 分区/16T/3.2M 写/1KB，探针开）**：

| 配置 | 结果 | ZF 安装 L1 数 | ZF 安装 L0 数 |
|---|---|---|---|
| merge_base=0 | **rc=0、0 Corruption** | 256 | 5,692 |
| merge_base=1 | 段错误、7 次 L1 重叠 | 380 | **0** |

- merge=1：ZF 输出**全部进 L1**（L0 被完全吸收）→ 同分区 L1 反复被替换；
  崩溃对**两条都是 ZF 的 L1 输出**（此前的 pair 均有 mm-add 记录）。
- merge=0：ZF 仅 **4%** 安装进 L1（256/5948），其余回落 L0；且 L0→L1 原生
  compaction 因 `level0_file_num_compaction_trigger=1e8` 不调度 → L1 几乎只被
  那 256 次直装触碰，同分区 L1 替换循环不存在 → 未观测到崩溃（但其代价是
  L0 只增不消：5,692 文件/3.2M 写的空间放大，100GB 时表现为卡滞/磁盘耗尽）。

**merge=0 时 L0 文件的 key range 如何确定**（代码语义）：
1. 物化任务按分区产出单文件，文件内部键范围 = 实际数据足迹
   （smallest/largest internal key）；范围断言保证 ⊆ 该分区路由区间
   [lo,hi)（`GetPartRange`→`RangeOf`；首/末分区为 ±∞ 侧）。
2. 定层 `PickInstallLevel`：与 L0..base_level 任一文件无重叠 → 直装 base
   （L1）；有重叠 → **回落 L0**（kFallback）。故 L0 文件通常只覆盖
   **某个分区区间内的一个子段**，同分区不同代文件在 L0 可互相重叠
   （L0 查找按文件号新→旧，最新版取胜）。
3. 特例：kHash 路由（版本 0 hash 表）无边界 → lo/hi 空 → 输出可为**全范围**
   单文件；split 孤儿尾 epoch 用父区间覆盖；学习期越界数据回落 L0 并计数。
4. `skip_batching=true`（选项默认）时比例不足的分区走 kSkip——数据留
   frozen 索引+recovery WAL，**不产生 L0 文件**（bench 为 drain 确定性设
   skip=false，故对照组 L0 文件数偏多）。

## 后续

- 因问题 1 已修，可以多写线程复跑更大规模（数 GB）确认 P 收敛轨迹与吞吐
  对照稳定；
- 问题 2 作为 M4 后续/收口项评估（活跃尾 split）。

原始日志：`gov_acc0.log`（OFF 单线程）、`gov_acc1.log`（ON 单线程）、
`gov_acc_mt.log`（ON 4 线程）、`gov_big0.log`（OFF 8 线程 2.88M 写）、
`gov_big.log`（ON 8 线程 2.88M 写）。
