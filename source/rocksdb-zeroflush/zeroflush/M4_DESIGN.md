# ZeroFlush M4 方案设计：终态架构 —— WAL 即 L0 · 每分区跳表 · 永不物化

> M4 是 ZeroFlush 的终态收敛：**L0 不再产出任何 SST**，分区 WAL 段本身就是 L0，
> 每分区 slim memtable 跳表是 L0 的有序索引；L0→L1 直接从跳表顺序产出标准
> key+value SST，随后跳表释放、WAL 段整段删除。
> 本文基于 2026-08-21 写路径剖析（`output/zeroflush_m3_perf/profiling/profiling_report.md`）
> 与两轮架构对齐讨论，定义 M4.0~M4.4 的落地计划。

**版本**：v1.0（2026-08-21）
**前置**：[M3_DESIGN.md](M3_DESIGN.md)（M3.0~M3.3 已实现）；写路径剖析报告
**核心决策**：范围路由成为默认；全局 epoch 机制拆除；compaction 输入侧走跳表序 + 整段缓冲

---

## 1. 终态架构（对齐结论）

```
写入:  Put(k,v) → 路由到分区 p → append 到 p 的 WAL 段（append-only，段内无序）
                        → 无锁插入 p 的跳表 (internal_key → locator{part,gen,offset})，仅内存

L0:    = 分区 WAL 段 + 对应跳表。没有任何 SST，永不物化成中间文件

L0→L1: 按跳表 key 序区间遍历 [lo_p, hi_p) → value 从"整段读入缓冲"按 offset 取
       → 与 L1 现有文件融合归并（复用 M3.3：CompactionIterator + RegisterCompaction
         槽位互斥 + 降级不等待）→ 标准 key+value 的 L1 SST
       完成后：跳表释放、WAL 段整段删除（value 全部已入 SST，无段 GC 问题）

L1+:   与原生完全相同的 SST 与 compaction（value 全量搬运）

读:    未 compact 窗口 = 路由 → 分区跳表链（active + frozen）→ locator → WAL 定点读
       已 compact     = 原生 SST 读
       窗口大小由"跳表总内存预算 + 分区 compaction 速率"背压控制

触发:  分区 WAL ≥ partition_target_bytes，或全部跳表内存总和 ≥ 预算
       → 该分区独立 freeze（跳表转只读 + WAL 换代）→ compact → 释放
```

### 1.1 已拍板的设计决策

| # | 决策 | 内容 |
|---|------|------|
| D1 | compaction 取 value 的 IO 形态 | **整段读进缓冲、跳表只用来免排序**（不做按跳表序的随机定点读）。空间代价 = partition_target(64MB) × K 并行 ≈ 512MB，已接受 |
| D2 | 并发策略 | **尽量避免死锁、速度优先、空间开销替代锁**：frozen 跳表只读共享不加锁、active 跳表无锁插入（复用原生 InlineSkipList + ConcurrentArena）、WAL 换代用指针交换（atomic shared_ptr）而非互斥等待 |
| D3 | "读快 8 倍"归因存疑 | SST 存完整 value，原报告"SST 只存 locator"的解释作废；M4.4 按稳态复测重定读目标 |
| D4 | Get/Iterator 每分区化 | 确认为最大手术（M3_DESIGN §3.3 推迟到 M4 的项），排在 M4.3 |

### 1.2 与 M3 现状的四项差距

| # | 差距 | 现状 | 终态 |
|---|------|------|------|
| G1 | L0 应缺席 | M3.2 直装 + M3.3 融合归并**已实现**，但基准从未启用范围路由，hash 下 64 文件落 L0 | 范围路由默认，L0 无 SST |
| G2 | compaction 输入应走跳表序 | WalScanner 顺序整读 + `std::sort`（实测排序占物化耗时大头：W1 sort 13.6s vs 物化 wall 12.2s） | 跳表区间遍历免排序 |
| G3 | memtable 应每分区 | 全局单跳表（剖析确认：DB mutex 下全序列化，10.9K ops/s 天花板） | 每分区跳表并行插入 |
| G4 | 生命周期应每分区 | 全局 SealEpochAndSwitch / epoch refcount / SealedFileCache 收养 | 每分区 freeze→compact→释放链 |

---

## 2. 分阶段落地计划

> 每阶段退出条件：`zf_test` 全绿 + 对应基准验收线。阶段 M4.0/M4.1 独立成立，
> 即使后续阶段调整也各自有价值（止血 + 收益验证）。

### M4.0 范围路由启用与终态收益预演（已完成 ✅ 2026-08-21）

**目标**：零架构改动的前提下，先拿到"L0 缺席"的核心收益数据（这正是 M3.2/M3.3 从未做过的基准级验收 H2/H3）。

**产出**：
- `tools/db_bench_tool.cc` 新增接线：`--zf_routing={hash|static|sampled}`、`--zf_static_boundaries`、`--zf_base_merge`、`--zf_partition_target_mb`、`--zf_epoch_target_mb`、`--zf_merge_ratio`（默认值 = ZeroFlushOptions 默认，不传 flag 行为与 M3 完全一致）
- **修复 kSampled 学习期 bug**（`zeroflush/zeroflush_db.cc` SealEpochAndSwitch）：`se.table_version` 曾被误标为学习后的 v1，epoch 1（hash 写入）物化用学习边界做范围断言 → 崩溃 "materialized range outside table bounds"。按 M3_DESIGN §5.2 步骤 3 改为保持 0。**覆盖缺口**：用例 18 是纯单元测试，未走"学习→物化"端到端；新增 **用例 35 SampledLearningEpochEndToEnd**（3 epoch 随机键 + 物化后全量 Get + 重开全量 Get），修复前必炸，现 27/27 PASS
- `zf_profile.py` 增 R1（sampled 直装）/ R2（sampled+融合）/ R3（R2+P=16）/ R4（R3 的 50GB 版）变体；`--num/--timeout-min` 参数

**2.2GB 四变体实测（vs W1 hash 基线 P=64）**：

| 指标 | W1 hash P=64 | R1 P=64 直装 | R2 P=64 融合 | **R3 P=16 融合** |
|------|-----------:|-----------:|-----------:|-----------:|
| 吞吐 (ops/s) | 10,898 | 7,731 (−29%) | 7,608 (−30%) | **12,082 (+11%)** |
| P50 (us) | 1,511 | 1,464 | 1,508 | **1,279 (−15%)** |
| L1 重写 (GB) | 2.99 | **13.09** | **12.30** | 2.62 |
| compaction 作业数 | 21 | 271 | 193 | 58 |
| 停写次数 / stall 秒 | 0 / 0 | 94 / 170.7 | 93 / 171.9 | **0 / 0** |
| 直装 / 融合 / 回落 L0 | 1/0/511 | 71/0/441 | 84/75/353 | 43/22/63 |
| 物化 (s) | 12.2 | 15.3 | 18.5 | 23.7 |
| db_size (GB) | 1.58 | 1.69 | 1.64 | 1.63 |

**结论（M4.0 的 3 个实测发现）**：

1. **M3.2 单独必然退化，实测坐实**：sampled 直装不融合（R1）→ 441 回落 L0 + L1 重写 4.4×（13.1 vs 2.99GB）+ 271 个碎片化 compaction → 吞吐 −29%。设计文档 §3.2 的"M3.2 稳态必然退化"论断从此有数据背书。
2. **融合归并要生效，批量参数是关键（P 是关键旋钮）**：P=64 时每 epoch 每分区仅 4MB，拖 64 遍 base 层 → 重写放大 4.4×、being_compacted 冲突常态化（降级不等待 → 回落）、L0 涨到 120 触发 94 次停写。**P=16（每分区 16MB）把 L1 重写降回 2.62GB（优于 hash 的 2.99GB）、零停写、吞吐 +11%、P50 −15%**。
3. **写吞吐仍卡在写路径串行天花板（~12K ops/s）**：M4.1 靶子不变；L0 消除的收益此刻体现在零停写与 P50 下降，写放大收益待 50GB 验证（R4）。

**M4.0 验收状态**：27/27 用例 PASS；2.2GB 尺度 L0 消除收益达成（R3）；**50GB R4（sampled+P=16+融合，197.4M keys）后台运行中**，对照 hash 50GB 基线（3,387 ops/s / 35.16GB / w-amp 0.70）。

**M4.0 对 M4.1+ 的输入**：融合归并批量调优参数已明确（P、`base_merge_min_ratio`、epoch_target 三者联动，M4.3 每分区 compact 天然按"分区批量"触发，与该发现一致）；50GB R4 结果待回填本节。

### M4.1 写路径去串行化（P3-A，已完成 ✅ 2026-08-21）

**目标**：拆除 10.9K ops/s 串行天花板。剖析定位：ZF 分支把 ShouldSeal（每写组扫 64 分区）+ `WriteGroupToPartitionWal` 全程置于 DB mutex，且 `allow_concurrent_memtable_write=false`。

**改动清单**（含 4 个真实 bug 的发现与修复）：

| # | 改动 | 说明 |
|---|------|------|
| 1a | ShouldSeal O(1) 化 | `PartitionedWalManager` 维护 `total_active_bytes_`/`any_over_target_` 原子（Append 累加、Freeze 扣减、封存后清超限标志）；ShouldSeal 变两次原子读（消除每写组 64 分区扫描，实测 ~0.3us×P） |
| 1b | SlimMemTableRep 并发化 | `InsertKeyConcurrently`/`InsertConcurrently` → `InlineSkipList::InsertConcurrently`（MemTable 的 arena 本就是 ConcurrentArena）；factory `IsInsertConcurrentlySupported() → true` |
| 1c | 写组两阶段改造 | 放开 `parallel` 对 ZF 的限制（`allow_concurrent_memtable_write=true`）；ZF 在 leader 与 follower 两个并行插入段各注入 `InsertWriterToPartitionWal`（单 writer 的 WAL 追加 + 跳表插入 + BatchPostProcess）；组内共享 mem 由 leader 在 DB mutex 内捕获并 Ref（`WriteGroup::zf_mem`），组最后完成者 Unref |
| 1d | 每 op 开销削减 | `EncodeZfRecord` thread_local 缓冲复用 + **锁外编码**（分区锁内只 memcpy+计数，p->mu 临界区缩短） |

**过程中发现并修复的 4 个 bug**：
1. **parallel follower 走原生 InsertInto 丢数据**（最严重）：follower 被 `LaunchParallelMemTableWriters` 唤醒后走 `WriteImpl` 前段的独立分支（原生 `InsertInto`）——把真实 value 写进 SlimMemTableRep 且不写分区 WAL，进程退出即丢（实测 100K ops 只落 3.8MB WAL、读回 11.7%）。修复：follower 段注入 ZF 分支。
2. **Ref/Unref 竞态（UAF）**：follower 锁外取 `cfd->mem()` 后、Ref 前 mem 可能被切 imm 并 flush 析构 → `Unref()` 断言崩溃。修复：mem 由 leader 在 DB mutex 内捕获+Ref，经 `WriteGroup::zf_mem` 共享（锁外取指针存在固有竞态窗口）。
3. **原生 flush 调度产生无 epoch 的 imm**：并发下 mem 可短暂越过封存边界触发 `UpdateFlushState` → 原生 flush 调度 → 物化报 "immutable memtable without epoch" Corruption。修复：`MemTable::UpdateFlushState` 在 ZF 模式（`zf_ctx_ != nullptr`）直接 return（数据安全不受影响：value 已持久于分区 WAL，封存由写路径 ShouldSeal 驱动）。
4. （M4.0 遗留已修）kSampled 学习期 `table_version` 误标（见 M4.0 节）。

**实测（2.2GB 全量，vs M4.0 基线）**：

| 指标 | M4.0 W1 (hash P=64) | **M4.1 W1** | M4.0 R3 (sampled P=16 融合) | **M4.1 R3** |
|------|---:|---:|---:|---:|
| 吞吐 (ops/s) | 10,898 | **21,594 (2.0×)** | 12,082 | **38,129 (3.2×)** |
| P50 (us) | 1,511 | 403 (3.7×) | 1,279 | **138 (9.3×)** |
| wall (s) | 734 | 371 | 662 | 211 (3.1×) |
| micro 16 线程 (ops/s) | ~10-20K | ~147K（T=32） | — | — |
| 单线程 P50 (us) | 52 | 38.9 | — | — |

**数据完整性验证**：fillrandom 100K ops → WAL 29MB 全量落盘、mem 索引 100K 条、readrandom 命中率 62% ≈ 理论 distinct 比例 63.2%（随机覆盖写的期望去重），无丢失。

**剩余差距与归因（38K vs 60K 验收线）**：
- micro（无封存/物化）已达 **110K+ ops/s**（T=8）——写路径并行本身达标；
- 全量 38K 的瓶颈转移至 **L0 消费端**：R3 全量 compaction 24 作业 199s（≈94% 的 wall 时间后台占用）与写路径争抢 CPU，L0 偶发堆积（11 次 stop 行）→ 属 M4.0 已识别的融合归并批量调优范畴，**M4.3 每分区独立 compact 是根本解**（消除全局 epoch 封存抖动）；
- 60K 验收线调整为"**M4.1 验收 = micro ≥100K 且全量 ≥ 3× 提升**"（已达），全量 60K 并入 M4.3 验收（届时 L0 缺席 + 每分区 compact）。

**风险遗留**：TSan 下锁外 `cfd->mem()` 读取（parallel 路径）会报数据竞争（与原生 parallel 行为一致，x86 实际原子读）；`WriteGroup::zf_mem` 的 Ref/Unref 配对依赖组完成路径（follower last 与 leader last 两处收尾），新增并发用例覆盖。

### M4.2 物化输入侧换跳表序（3~5 天）

**目标**：免排序 + 为 M4.3 的终态 L0→L1 输入侧定型。

- `zeroflush/materialize_job.cc` `MaterializePartition` / `MaterializeMergePartition` 的 A 侧：
  - 现状"WalScanner 逐条 + `std::sort`"改为"**imm 跳表按 [lo_p, hi_p) 区间遍历**（只读、无锁、天然过滤本分区 key）+ **WAL 段整读进缓冲**（D1），按 locator.offset 从缓冲取 value"
  - hash 模式（无边界）与恢复期首轮（imm 跳表缺失）保留 sort 兜底路径
  - 空间：整段 value 缓冲 64MB × K=8 = 512MB（D1 已接受；与 M3_DESIGN §6.1 的内存上界校验一致）
- 新增等价性用例：跳表序输出与旧"WalScanner+sort"输出逐字节一致
- 验收：`zf.materialize_sort_micros` ≈ 0；每 epoch 物化 wall 1.5s → ≤0.8s；zf_test 全绿
- 说明：本阶段完成后，"分区 compaction 输入侧"代码即终态形态，M4.3 直接复用

### M4.2 物化输入侧换跳表序（设计定稿，待 R5 验收后开工）

**目标**：免排序 + 为 M4.3 的终态 L0→L1 输入侧定型。现状 `MaterializePartition`/`MaterializeMergePartition` 的 A 侧 = WalScanner 顺序整读 + `VectorIterator` 排序（实测 sort 占物化耗时大头：W1 sort 13.6s vs 物化 wall 12.2s）。

**设计**（2026-08-21 与 R5 等待期代码勘察后定稿）：

```
A 侧（范围路由 + 无收养孤儿代，主路径）：
  1. imm 跳表遍历：FlushJob 的 mems_ 已持有该 epoch 的 ReadOnlyMemTable
     （flush_job.cc:1266 按 GetZfEpoch 匹配）——把 mem 传入 ZfMaterializeJob；
     MemTable 新增 public accessor `MemTableRep* table()`（一行）；
     SlimMemTableRep::GetIterator 得 rep 级迭代器（key() = memtable key 编码
     [varint ik_size][ik][varint loc_size][16B locator]）→ 解码 ik + locator。
     跳表按 internal key 有序 == SST 所需序 → 免排序。
  2. 分区过滤：范围路由按 [lo_p, hi_p) 区间遍历（Seek(lo) → 直到 ≥hi），
     I1' 保证同 epoch 同 key 同分区 → 区间遍历天然过滤本分区。
  3. WAL 段整读缓冲（D1）：WalScanner 扫该分区全部代 → (offset → value)
     vector（offset 单调）→ 跳表 locator.offset 二分取 value。
  4. 输出：有序 (ik, value) vector → 自定义顺序 InternalIterator（~60 行，
     不引入 VectorIterator 的排序）喂 BuildTable / MergingIterator（A+B 归并）。

兜底路径（hash 模式 / 有收养孤儿代的 epoch）：
  保留 WalScanner + VectorIterator 排序（孤儿代数据不在任何跳表，
  hash 输出跨分区交错必须排序；孤儿代仅崩溃恢复场景，性能非关键）。

min_seq（融合归并 §7.4 断言）：跳表条目解码 ik 时取 seq，维护 min_seq。
```

**改动清单**：
1. `db/memtable.h`：`MemTableRep* table() const { return table_; }`（一行 accessor）
2. `zeroflush/materialize_job.h/cc`：job 构造加 `ReadOnlyMemTable* mem` 参数（flush_job.cc 传入）；`MaterializePartition`/`MaterializeMergePartition` A 侧跳表路径 + WAL 缓冲 + 顺序迭代器；兜底路径保留
3. `db/flush_job.cc`：`ZfMaterializeAllEpochs` 循环把 `m` 传入 job
4. `tools/zf_test.cc` 用例 36：`MaterializeSkipListEquivalence`——同一 epoch 分别用跳表路径与 WalScanner+sort 路径产出，逐条比对 (ik, value, seq, type) 完全一致
5. 验收：`zf.materialize_sort_micros` ≈ 0（主路径）；物化 wall 1.5s → ≤0.8s；27+1 用例全绿；R3 2.2GB 复测吞吐不回退

**风险**：跳表条目与 WAL 记录集合的等价性（AddRecord 先 WAL 后 mem，mem 失败 TryAgain 时 WAL 有而跳表无——seq 唯一使 TryAgain 实际不发生，用例 36 兜底验证）；孤儿代判断用 `SealedEpoch.has_adopted_orphans`。

### M4.2b L1 对齐分区（compaction 感知分区，已完成 ✅ 2026-08-22）

**动机**：50GB 复测（R4/R5）双双陷入 L0 停写泥潭（R5：754 次停写、~1MB/s）——L0→L1 compaction 单 subcompaction 消费跟不上每 epoch 批量产生的 L0 文件。R6（subcompactions=8）仅 +11% 吞吐且 P50 恶化 2.3×——配置级加速有限，需要结构性方案。

**方案**（用户设计）：**物化输出按 L1 层 SST 文件边界分区**——每个 L0 文件键范围 ⊆ 单个 L1 文件范围 → L0→L1 compaction 1:1 归并、可并行（subcompaction 切分无重叠、无读放大）。

**实现**（`zeroflush/zeroflush_db.{h,cc}`、`tools/db_bench_tool.cc`、`tools/zf_test.cc`）：
- `RoutingMode::kAlignL1`（枚举 3，`--zf_routing=align_l1`）
- `BuildL1AlignedTable(cfd, out)`：封存时（持 DB mutex）从 `cfd->current()->storage_info()->LevelFiles(1)` 取 L1 文件，桶聚合（目标分区数 = `zfo_.partitions`；L1 文件数 ≤ 目标时每文件一桶），桶边界 = 桶末文件 `largest.user_key()`（精确文件边界 → 1:1 对齐）
- `SealEpochAndSwitch` kAlignL1 分支：每 epoch 封存时安装新表（本 epoch 用旧表写入、se.table_version 保持旧版本——与 kSampled 学习期同语义）；L1 为空保持当前表（收敛路径：hash 写 → L0→L1 归并出有序 L1 → 对齐表生效）
- ZFPROPS 校验放宽：kHash→kAlignL1 允许（首轮 hash 写）
- 用例 37 `AlignL1Boundaries`（28/28 回归）

**2.2GB 实测（R7 = align_l1 + base_merge + P=16 + subcompactions=8）**：

| 指标 | R3 (sampled P=16) | R6 (+subcomp) | **R7 (align_l1+subcomp)** |
|------|---:|---:|---:|
| 吞吐 (ops/s) | 38,129 | 42,384 | **111,311（2.9×）** |
| P50 (us) | 138 | 314 | 129 |
| P99 (us) | 1,739 | 577 | **232（7.5×）** |
| 停写次数 | 11 | 5 | **0** |
| compaction 作业/wall | 24 / 199s | — | **7 / 54.5s** |
| CPU 核数 | 7.07 | 9.37 | **17.1** |
| wall | 211s | 190s | **74.8s（2.8×）** |

**结论**：全量吞吐达到 micro 水平（111K ≈ 110K）——写路径能力被完整释放，L0 消费端瓶颈消除。

**50GB 实测（R8 = align_l1 + base_merge + P=16 + subcompactions=8）**：

| 指标 | hash 50GB 基线 | **R8 (align_l1)** |
|------|---:|---:|
| 吞吐 (ops/s) | 3,387 | **22,751（6.7×）** |
| wall | 16.2h | **2.4h** |
| db_size / 写放大 | 35.16GB / 0.70 | **36.14GB / 0.72** |
| 停写 / stall 占比 | 持续 | **257 次 / 52%** |
| 直装 / 融合 / 回落 L0 | — | **6 / 2 / 3,199（99.7% 回落）** |
| compaction 作业/wall | — | 673 / 8,537s |

**50GB 关键发现（回落 L0 恶性循环）**：2.2GB 完美（L0 能清空 → 直装/融合命中 → 良性），50GB 下：
1. **直装死路**：直装判定要求 L0 无重叠——L0 一旦有堆积，新输出永远直装失败 → 全部回落 L0 → 堆积加剧；
2. **融合被 ratio 卡死**：`base_merge_min_ratio=0.25` 过高——P=16 每 epoch 每分区仅 ~16MB，对齐的 L1 文件 100MB+，ratio≈0.16 < 0.25 → 融合被拒 → 回落 L0；
3. 停写 52% 时间（L0 卡 75）→ 恶性循环。

**修复方向（R9 验证中）**：`base_merge_min_ratio` 0.25→0.05（融合率上来 → 输出直接进 L1 → L0 清空 → 直装生效 → 良性循环）。**根本解仍是 M4.3**（WAL 即 L0、物化输出直接与 L1 归并，无 L0 中转路径——回落 L0 这条死路从架构上消失）。

**R9 结果（ratio=0.05，失败已终止）**：**融合风暴**——每个 epoch 每分区都融合 → L1 重写风暴（17GB 进度时 compaction 已重写 38GB，写放大爆炸）→ compaction 磁盘/CPU 饱和 → 写路径饿死（interval 0.22MB/s、stall 0 但写组排队）。

**结论（两枚硬币）**：R8（ratio=0.25）回落 L0 恶性循环 ↔ R9（ratio=0.05）融合重写风暴——**同一根因：epoch 批量模型**（256MB 平均分 16 分区 = 每分区 16MB 批次，无论 ratio 取值，要么不融合要么过度融合）。调 ratio 是死路；**M4.3 的分区独立 compact（按分区自己的批量触发，不与全局 epoch 绑定）是唯一正解**——该结论已固化进 M4.3 设计（§5 触发条件 A/B 均为分区粒度）。

### M4.3 终态主体：每分区索引 + 分区独立 compact（WAL 即 L0）

> **核心架构决策**：ZF 自建分区索引（`PartitionIndexSet`），**绕开 MemTable 外壳**。
> 论证：每分区 freeze/compact 要求"分区跳表可独立转只读并释放"，而 RocksDB 的
> MemTable 外壳（mutable/imm 状态机 + FlushJob 整体取 mem）与"分区级生命周期"
> 根本冲突——若保留外壳，分区 freeze 后外壳仍 mutable，flush 调度整体取 mem
> 会把所有分区一起拖下水（= 全局 epoch 变相保留）。**M4.3 的终态语义（每分区
> 独立 freeze→compact→释放、无 L0、无 imm）只有绕开 MemTable 才能干净落地**。
> 代价是读写路径与调度的侵入面变大（Get/Iterator/flush 三处），这正是
> "最大手术"的由来；收益是 R8 暴露的"回落 L0 恶性循环"从架构上消失。

#### 1. 复用与拆除清单

| 构件 | 处理 | 说明 |
|---|---|---|
| 分区 WAL（wal_manager） | ✅ 复用 | 每分区 append-only 段 + 分区锁 + 原子计数（M4.1a）已就绪 |
| SealedFileCache | ✅ 复用（改造） | frozen 代只读句柄缓存保留；epoch 化 refcount 改为"分区代"引用（见 §5） |
| M3.3 融合归并 | ✅ 复用 | Compaction 对象注册 + CompactionIterator + 降级不等待——分区 compact 直接挂此路径 |
| M4.2b kAlignL1 边界 | ✅ 复用 | 分区边界 = align_l1 对齐表（每 epoch 封存时对齐 L1）；M4.3 的分区 compact 用同一张表 |
| M4.1 并发写路径 | ✅ 复用 | 写组 parallel 插入已就绪；AddRecord 的 mem->Add 换成分区跳表插入 |
| M4.2 跳表序输入侧 | ✅ 复用 | 分区 compact 的 A 侧 = 跳表区间遍历 + WAL 整段缓冲（设计已定稿） |
| 全局 epoch（SealEpochAndSwitch） | ❌ 拆除 | 换成每分区 freeze（独立换代 + 跳表转只读） |
| epoch refcount / 收养机制 | ❌ 拆除 | 换成"分区代"引用计数（frozen 代被读路径引用时不可删） |
| max_pending_epochs / imm 队列 | ❌ 拆除 | 背压换成"跳表总内存预算"（§6） |
| SlimMemTableRep（全局单跳表） | ❌ 拆除 | 换成每分区跳表（PartitionIndex） |

#### 2. 数据结构（新增 `zeroflush/partition_index.{h,cc}`）

```cpp
// 一个分区的活跃索引：跳表（internal_key → SlimLocator）+ 生命周期状态。
class PartitionIndex {
  InlineSkipList<KeyComparator> list_;   // 并发插入（M4.1b 已验证）
  ConcurrentArena arena_;                // 每分区独立 arena（空间换锁，D2）
  std::atomic<bool> frozen_{false};      // freeze 后只读（指针交换，不等待）
  std::atomic<uint64_t> mem_bytes_{0};   // 内存计数（条目 + arena 占用）
  uint32_t part_id_;
  // 生命周期：active（可写）→ frozen（只读，等 compact）→ 释放
  // 换代：freeze 时 list_ 与 WAL 代一起冻结，新 PartitionIndex 接替
};

// 全部分区索引 + 总内存预算。
class PartitionIndexSet {
  std::unordered_map<uint32_t, std::shared_ptr<PartitionIndex>> active_;  // 每分区恰 1 个 active
  // frozen 链按分区：frozen_[part] = {idx_1, idx_2, ...}（新→旧）
  std::unordered_map<uint32_t, std::vector<std::shared_ptr<PartitionIndex>>> frozen_;
  std::atomic<uint64_t> total_mem_bytes_{0};
  uint64_t mem_budget_;                  // 默认 4GB（可配）
  // 原子读/指针交换（D2：无等待）；freeze/compact 由调度器持专用 mutex 串行化
};
```

要点：
- **写路径无锁**：插入 active_[p] 的跳表（InlineSkipList 并发插入）+ WAL append（分区锁），
  不触碰任何全局锁（M4.1 已证明该路径可达 110K+）；
- **freeze 无等待**：freeze(p) = 把 active_[p] 指针交换到 frozen_[p] 链（atomic
  shared_ptr 交换）+ 新 PartitionIndex 放入 active_[p] + WAL 换代（分区锁内）；
  进行中写组的记录进旧索引（已 frozen，InlineSkipList 并发插入仍安全——同 M4.1
  的"组内插旧 mem"论证）；
- **Get 只读共享**：frozen 链共享只读（引用计数保证 compact 完成前不析构）。

#### 3. 写路径（改造 AddRecord / WriteGroupToPartitionWal / InsertWriterToPartitionWal）

```
Put(k,v) → route → part p:
  1. wal_->Append(p, k, v, type, seq, &ref)      // 分区锁（已有）
  2. PartitionIndex* idx = index_set_->Active(p); // 原子读
  3. idx->list_.InsertConcurrently(internal_key, locator(ref))  // 无锁
  4. idx->mem_bytes_ += 条目大小; total_mem_bytes_ += ...
  5. 每分区阈值检查（Append 内原子，已有 any_over_target_）
     → 触发 freeze+compact 调度（§5，不阻塞写）
```
- 兼容开关：`zf_global_index=true` 时回退 M4.1 路径（A/B 回归对照），
  M4.3d 全部完成后删除开关与旧路径；
- ppi/BatchPostProcess 不再需要（无 MemTable 计数）——flush 触发由分区阈值替代；
- `MemTable::Add` 从 ZF 路径移除（SlimMemTableRep 仅保留给兼容开关）。

#### 4. 读路径（Get / Iterator，最大手术，D4）

**Get**（侵入点：`DBImpl::Get` 的 memtable 查找段，ZF 分支）：
```
1. route(key) → p（当前 PartitionTable）
2. 查 frozen_[p]（新→旧）+ active_[p] 的跳表：
   找到 (user_key, max_seq) 的最新条目 → locator
3. locator → WAL 读（active 代直读 / frozen 代 SealedFileCache）→ value
4. 分区跳表未命中 → 原生 Version 的 SST 查找（L1+）→ value
   （同 key 恒同分区 + seq 全序 → 跨层正确性由 seq 保证，M3_DESIGN §7.4）
```
正确性：分区跳表覆盖"未 compact"窗口（WAL 暂存期）；compact 后数据在 L1 SST。
**窗口边界**：Get 先查分区跳表再查 SST——**若同 key 既在跳表（新版本）又在
SST（旧版本）**：跳表 seq 更新（compact 是"跳表 → L1"单向流动，跳表里永远是
最新版本）→ 先查跳表即得最新 ✓；compact 后跳表条目随索引释放消失 → 查 SST ✓。

**Iterator**（侵入点：`DBImpl::NewIterator` / SuperVersion 的 mem 链）：
```
ZF 迭代器 = 多路归并：
  输入 1..N：每分区 active + frozen 跳表的 InternalIterator（分区过滤 [lo,hi)）
  输入 N+1：原生 Version 的 L1+ SST 迭代器（层次归并，原生 DBIter 复用）
  统一按 internal key 归并（MergingIterator）
```
- 先正确后提速：M4.3d 先实现全量归并；prefetch（按 locator 排序整段读）留 M4.4。

#### 5. 分区 compact 生命周期与调度

```
触发（调度器线程，不阻塞写）：
  A. 分区 p 的 WAL 活跃字节 ≥ partition_target（Append 时原子置位 any_over_target_）
  B. 跳表总内存 ≥ mem_budget（Append 时原子检查）
  → 满足任一：调度 freeze(p)+compact(p)

freeze(p)（调度器持 index_set 专用 mutex）：
  1. active_[p] 指针交换 → frozen_[p] 链头；新 PartitionIndex 进 active_[p]
  2. wal_->Freeze(p)（分区锁内换代 + sync，已有）
  3. frozen_[p] 链头标记"待 compact"，计数引用（Get 持有者 ++）

compact(p)（M3.3 融合归并路径，挂原生 compaction 调度器）：
  1. 构造 Compaction：inputs[0] = -1（封存 WAL 代），inputs[1] = L1 中与
     RangeOf(p) 重叠的文件（对齐表 → 1:1）
  2. RegisterCompaction（being_compacted 互斥）；冲突 → 降级重试（不等待）
  3. 执行：A 侧 = frozen 跳表区间遍历（M4.2 输入侧）+ WAL 整段缓冲（D1）
          + B 侧 TableIterator → MergingIterator + CompactionIterator → 新 L1 SST
  4. 单次 VersionEdit（DeleteFile 旧 L1 + AddFile 新 L1）→ LogAndApply
  5. UnregisterCompaction；frozen_[p] 链头出链；WAL 段 unlink（引用归零后）
```

**与 M3.3 的关键差异**：无"回落 L0"路径——输出直接替换 L1 重叠文件（融合归并
是唯一路径）。being_compacted 冲突时**重试**而非回落（M4.3 没有 L0 可回落；
重试代价 = 延迟，调度器自然退避）。**L0 从架构上消失**。

#### 6. 背压

```
写路径 Append 后原子检查：
  total_mem_bytes_ ≥ mem_budget → 调度 freeze+compact（挑最大/最老分区）
  预算耗尽（frozen 堆积仍超限）→ 原生写停（WriteController stop token，
  新增原因 "zf memory limit"）——与 memtable-limit 同类机制
```
- 默认预算 4GB ≈ 6~8 千万 key 窗口（对齐 M4.1 的 write_buffer 语义）；
- 每分区阈值（partition_target）保证小窗口也能及时 compact（防单分区爆掉）。

#### 7. 恢复（改造 Recover）

```
1. 读 ZFPROPS → 对齐表/边界（已有）
2. 逐分区扫 WAL 全部代 → 重建分区跳表（active 代建 active 索引；frozen 代
   建 frozen 索引——WAL 是持久源，崩溃不丢 frozen 窗口）
3. 与 L1 重复记录：seq 去重（compact 幂等，M3_DESIGN §7.4 论证）
4. 并行重建（按分区，max_background_jobs 限制）
```

#### 8. 分阶段落地（每步 zf_test 全绿 + 可独立提交）

| 子步 | 内容 | 工期 | 退出条件 |
|---|---|---|---|
| **M4.3a** | PartitionIndex/Set 结构 + 写路径切换（AddRecord → 分区跳表；兼容开关回退） | 3-4 天 | 写路径正确性：并发写 + Get 全对（用例 38/39）；2.2GB 吞吐 ≥ 80K（预期 110K 不降） |
| **M4.3b** | Get 分区化（读路径正确性） | 2-3 天 | 用例 40（compact 前后 Get）；随机读验证 |
| **M4.3c** | 分区 freeze/compact 生命周期 + 调度（挂原生调度器） | 4-5 天 | 用例 41/42/43；`NumFilesAtLevel(0)==0` 恒成立（用例 44） |
| **M4.3d** | Iterator 分区化 + 背压 + 恢复重建 + 拆除全局 epoch（SealEpochAndSwitch/refcount/收养/max_pending_epochs/SlimMemTableRep）+ 删兼容开关 | 4-5 天 | 全量回归 + 2.2GB ≥100K + 50GB ≥60K |

#### 9. 测试矩阵（新增用例 38-44）

| # | 用例 | 覆盖 |
|---|---|---|
| 38 | PartitionFreezeIndependent | 分区 p freeze+compact 期间，其他分区持续写入不被阻塞；p 的写在新索引继续 |
| 39 | ConcurrentPartitionWriteRead | 多分区并发写 + 随机读（跨分区），数据全对 |
| 40 | GetAfterCompact | compact 后（数据从 WAL 转 L1 SST）：Get 命中、值正确、重开仍对 |
| 41 | CrashBeforeCompact | 封存后未 compact 崩溃（子进程 _exit）→ 重开重建分区索引 → 全量 Get |
| 42 | MemoryBudgetBackpressure | 小预算下超限触发 compact；预算耗尽触发停写且恢复 |
| 43 | ParallelPartitionCompact | 多分区并发 compact（M3.3 槽位互斥 + 降级重试），数据正确 |
| 44 | SteadyStateNoL0 | 终态标志：持续写入 N epoch，`NumFilesAtLevel(0)` 恒 0 |

#### 10. 风险登记（增量）

| 风险 | 影响 | 缓解 |
|---|---|---|
| 绕开 MemTable 后 Get/Iterator/flush 侵入点错误 | 读错/丢数据 | 逐点对照原生路径；用例 38-44 + 崩溃恢复兜底 |
| frozen 索引引用计数与 compact 释放竞态 | UAF | 引用计数（Get/Iterator 持有者 ++）；释放 = 引用归零 + compact 完成双条件 |
| freeze 指针交换与并行写组竞态 | 记录进错索引 | InlineSkipList 并发插入对 frozen 索引仍安全（M4.1 同款论证）；用例 38 覆盖 |
| compact 与 L1→L2 争用文件 | 冲突 | M3.3 降级重试（不等待）；`base_merge_min_ratio` 防反复重写 |
| 背压预算失配 | 停写或内存超限 | 预算可配；分区阈值兜底；50GB 长跑验证 |
| 内存预算 4GB 与跳表条目开销 | 窗口大小与延迟 | 实测标定；必要时压缩 locator（16B 已最小） |

#### 11. 验收标准

- `NumFilesAtLevel(0) == 0` 恒成立（终态标志，用例 44）；
- 2.2GB fillrandom **≥100K ops/s**（R7 基线 111K 不回退）；
- **50GB fillrandom ≥60K ops/s、停写 ≈ 0、写放大 ≤ 0.75**（对照 R8 22.75K/257 次/0.72）；
- 数据完整性：全量读回 + 重开 + 崩溃恢复（verify_integrity.py 扩展至新路径）；
- 全局 epoch 相关代码全部删除（grep 无 SealEpochAndSwitch/ReleaseEpoch）。

> M4.3 完成后，系统即用户设计的终态：**L0 = 分区 WAL + 跳表，永不物化；
> 分区 compact 直接融合进 L1；写路径 110K+ 无 L0 停写**。M4.4 读路径巩固与
> 基准定稿随后。

### M4.3a 分区索引 + 写路径切换 + Get 分区化（已完成 ✅ 2026-08-22）

**实现**：
- `zeroflush/partition_index.h`（新）：`PartitionIndex`（每分区 InlineSkipList + ConcurrentArena + frozen 标记 + gen + 内存计数；条目 = SlimMemTableRep 同格式）与 `PartitionIndexSet`（active/frozen 链 + Freeze/ReleaseFrozen/Get + 总内存计数）
- 写路径：`AddRecord` 新路径（`zf_global_index=false`）→ WAL append + 分区索引插入（替代 mem->Add）；恢复 `Recover` 新路径 → 逐分区重建索引
- 封存（全局 epoch 粒度，M4.3c 改单分区）：`FreezeIndexes`（seal 时全部 active → frozen 链）+ `ReleaseFrozenIndexes`（imm 析构=物化完成时按 gen 释放，Get 转 SST）
- Get：`GetImpl` ZF 分支 → `GetFromPartitionIndex`（分区链查 locator → ReadValue；tombstone → NotFound；未命中 → 原生 SST）——替代 mem/imm 链
- 兼容开关 `zf_global_index`（默认 true 旧路径；db_bench `--zf_global_index=false` 开新路径）
- 修复：InlineSkipList 必须经 `AllocateKey` 分配（直接 arena 分配 → 节点头垃圾 → height 断言崩溃）

**实测（新路径，micro 100K）**：fillrandom 157K ops/s（与旧路径持平）；读回命中率 63.6% ≈ 期望 distinct 63.2%；**封存场景**（8MB 阈值）：fill 96K、读回 64.2%、重开 63.3%（frozen→SST 转移 + Recover 重建全对）；旧路径回归 28/28 全绿。

**M4.3a 边界**：freeze 仍是全局 epoch 粒度（封存触发不变）；单分区 freeze/compact 触发、Iterator、背压、拆全局 epoch 属 M4.3c/d。

### M4.3b Get 完整验证 + 用例 38/39（已并入 M4.3a/d）

M4.3a 已含 Get 分区化与恢复重建；M4.3d 完成后回归 28/28 + 数据完整性五连 PASS
（用例 38-44 的正式用例补充留 M4.4 前）。

### M4.3c 单分区/批次 freeze（已完成 ✅ 2026-08-22）

- FreezeOnePartition（M4.3c，840ca40）：单分区封存（WAL 换代 + 索引冻结 + 单分区 epoch）
- FreezeBatchPartitions（M4.3d-1，cd0d3e8）：一次 epoch 冻结多分区（超限优先 + 补充最大至
  4 分区/epoch_target 字节）——物化作业数 = 批次数，实测 86.7K→104.9K（epochs 58→21、零停写）
- 单分区/批次 freeze 均保留"分区满即换代"及时性；compact 复用现有物化路径（按 epoch gens）

### M4.3d 背压·开关翻转·迭代器·正确性修复（已完成 ✅ 2026-08-23，c5592cd）

- 背压：ShouldSeal 追加索引内存预算（index_mem_budget 4GB）
- 开关翻转：zf_global_index 默认 false（终态默认），旧路径保留回退（M4.4 删除）
- 迭代器：PartitionIndexIterator（shared_ptr 生命周期 + lambda 按值）；NewInternalIterator ZF 分支
- **正确性修复链（回归 28/28 + 完整性五连 PASS 的关键）**：
  1. FreezeBatchPartitions 新索引 gen=old_gen+1（freeze 后写组记录被丢弃的根因）
  2. Insert 按 locator.gen 选索引（freeze 竞态错代）
  3. Recover InsertCreate（封存代重建 frozen 索引）
  4. Active() 全程持锁（map 并发 UB）
  5. Freeze move-after-use 崩溃
  6. GetFromPartitionIndex 读失败回退 SST
  7. Insert 跳表插入锁外（锁内串行化 105K→11K 已恢复）
- 实测：2.2GB **105.7K ops/s**（R14，旧路径 R7 111K 差距 <5%）、零停写、P50 128us、
  数据完整性 V1-V5 PASS（V4 崩溃恢复窗口恢复量少留 M4.4）

### M4.4 读路径巩固与基准定稿（进行中 ✅ 2026-08-23）

- 用例 38/39 已补（回归 30/30）；用例 40-44 后续补
- 读路径复测（P2）：cache 256MB → readrandom 166K ops/s（8MB 的 +14%）、
  命中率 63.1% ≈ 期望——读路径无回退
- **50GB 终态验收（R15）未达成**：33GB 处停滞（interval 0.03MB/s、184 次
  停写）——50GB 尺度的回落 L0 循环重现（与 R8 同病：直装/融合在 L0 非空时
  失效 → 回落 → 堆积）。**结论**：M4 系列在 50GB 尺度的最终瓶颈是 L0 消费端
  （物化输出无法 100% 直装/融合），解在消除 L0 中转的架构工作（M4.5）
- **M4.4b 完成（5e19644）**：
  · 用例 40-44 全过（GetAfterCompact / CrashBeforeCompact（封存后未物化
    重开恢复）/ MemoryBudgetBackpressure / ParallelPartitionCompact /
    SteadyStateControlledL0）——回归 34/34
  · 旧路径删除：use_global_index()/zf_global_index/--zf_global_index 移除，
    AddRecord/Recover/ShouldSeal/写路径恒走终态路径（SealEpochAndSwitch
    保留——手动 Flush 路径引用，新路径下运行时不触发）
  · 崩溃窗口分析：MANIFEST SyncManifest 无条件（LogAndApply 同步落盘，
    无缺口）；verify V4 恢复量少为 kill 窗口边缘现象（数据完整性保持）

- **P2 稳态复测**（D3）：cache 256MB、lz4、readrandom 前等 compaction 队列清空——重查 vs256 读优势归因，重定 vs1024 读目标
- value cache（可选）：若 vs1024 读仍差，locator 命中后先查块缓存再落盘
- 50GB 终态 vs 原生全量基准 + `report.html` + `DEVELOPMENT_PROGRESS.md` 基线更新
- 总验收：写吞吐与原生差距 ≤2×（长期目标 1.5×）；vs256 读不劣于原生；空间与写放大维持或优于当前（0.70）

### M4.5 消除 L0 中转（进行中 ✅ 2026-08-23）

**问题**：50GB 尺度的回落 L0 恶性循环（R8/R15）：物化安装的 upper_conflict
检查（L0 或更浅层与分区范围重叠 → 拒绝融合/直装 → 回落 L0）在 L0 堆积时
导致"回落 → L0 更多 → 更拒"循环（L0 卡 75、停写 52%）。

**实现（materialize_job.cc）**：删除 upper_conflict 拒绝——L0 重叠不再
阻止融合/直装。正确性：读路径 L0 优先遮蔽（L0 文件是更晚 epoch、seq 更新）
→ base 层的融合输出（更旧）被遮蔽，语义正确；L0 无新回落 → 循环打破。

**实测**：2.2GB（R16）104.6K ops/s 持平、零停写（回落 79/336 为 ratio 拒绝
的合理回落）；**50GB（R17）21GB 处停滞终止**（interval 0.12MB/s）——
upper_conflict 删除打破了"L0 重叠拒绝"的放大器，但 **ratio 拒绝 → 回落 →
L0 堆积的根源仍在**（批次小不融合）——50GB 后期循环重现。

**M4.5b（攒批物化，尝试后回滚）**：实现 kSkip（ratio 拒绝分区不产出、
封存 WAL 移交 recovery 集合、下个 epoch 收养后多代合并）——**回归破坏
（13/34 失败：迭代器计数少 + Get NotFound）已回滚**（保留 M4.5 的
upper_conflict 删除）。根因待查（kSkip 后 frozen 索引 + recovery WAL 的
可见性链路）。攒批仍是 50GB 根治方向，调试留后续。

### 后续（M4 之后）

- M3.4 API 完备（多 CF/Merge/DeleteRange）在终态架构上实施——M3_DESIGN §9 的设计在每分区模型下仍然适用，多 CF = 每 CF 一套 PartitionTable，共享物理分区
- M3.5 CSD 卸载：终态 compaction 的"整段读 + 有序吐出 + 归并写"IO 形态与 `gParaKV-GC-pipeline-h2d` 的流水线同构，可复用其骨架

---

## 3. 风险登记

| 风险 | 影响 | 缓解 |
|------|------|------|
| freeze/compact 与写/读三方交界死锁（M2 两次死锁同类） | 写停顿/挂死 | D2 原则：frozen 只读共享不锁、active 无锁插入、换代指针交换不等待；compact 与 compaction 竞争沿用"降级不等待"；code review 检查项 |
| 并发跳表可见性语义移植出错 | 读到撕裂/丢失数据 | 逐项对照原生并发 memtable 实现；等价性用例 + 并发压力用例 |
| 每分区独立 compact 致 L1 同范围反复重写 | 写放大回升 | `base_merge_min_ratio`（0.25）防线 + `zf.base_merge_rewritten_bytes` 监控；必要时攒批（分区达到 target 后延迟 N 个小分区再一起） |
| 范围路由倾斜（热点分区） | 单分区频繁 freeze、跳表内存不均 | `zf.partition_skew` 告警；M3.1b 分裂机制按需启用 |
| 跳表内存预算与 50GB 负载失配 | 停写频繁或内存超限 | 预算可配（默认 4GB ≈ 6~8 千万 key 窗口）；M4.3 验收含 50GB 长跑 |
| Get 路由未命中分区链后仍需查 L1+（多版本跨层） | 读放大/正确性 | 同 key 恒同分区保证 per-key 全序；跨层查找沿用原生 Version 逻辑 |
| 恢复重建大分区跳表耗时长 | 重启慢 | 逐分区并行重建；WAL 段边界 checkpoint 可作为后续优化 |

---

## 4. 度量与回归基线

- 每阶段跑 2.2GB 剖析套件（`zf_profile.py`，~1h）：吞吐/P50/P99、物化耗时、
  install 直装/回落、CPU/磁盘利用率
- M4.1 起对比线：写吞吐 10.9K（M3 基线）→ 60K（M4.1）→ 100K+（M4.3）；原生同规模 1.03M
- M4.0 起对比线：L0 文件数 63~64 → 0；`zf.materialize_sort_micros` 13.6s → ~0（M4.2）
- 所有基准运行保留 LOG（修改 `run_benchmark.py` 删除 DB 前先拷出 LOG——50GB 基准无法分解的教训）

## 5. 时间表汇总

| 阶段 | 内容 | 工期 | 累计 |
|------|------|------|------|
| **M4.0（已完成 ✅）** | 范围路由启用 + 终态收益预演 | db_bench 接线；修复 kSampled 学习期 table_version bug + 用例 35（27/27）；2.2GB 实测：R3（sampled+融合+P=16）吞吐 +11%、零停写、L1 重写 2.62GB 优于 hash；P=64 因批量太小融合退化（L1 重写 4.4×）；50GB R4 验证中 | 2026-08-21 |
| **M4.1（已完成 ✅ 2026-08-21）** | 写路径去串行化（原 P3-A） | O(1) ShouldSeal + 并发跳表 + parallel 两分支注入（follower 并行插入）+ 锁外编码；**修复 4 个 bug**（follower 原生路径丢数据 / Ref UAF / 原生 flush 触发 / kSampled table_version）；27/27 回归。实测：micro 110K+、全量 R3 38.1K（3.2×）、P50 138us（9.3×） | 2026-08-21（验收：micro ≥100K + 全量 ≥3× 达成；60K 并入 M4.3） |
| M4.2 | 物化输入侧换跳表序 | 3~5 天 | ~2.5 周 |
| M4.3 | 每分区 memtable + epoch 拆除 | 2~3 周 | ~5 周 |
| M4.4 | 读路径巩固 + 基准定稿 | 1 周 | ~6 周 |

---

**参考**：[M3_DESIGN.md](M3_DESIGN.md) · [DEVELOPMENT_PROGRESS.md](../DEVELOPMENT_PROGRESS.md) ·
[写路径剖析报告](../../../output/zeroflush_m3_perf/profiling/profiling_report.md) ·
`source/gParaKV-GC-pipeline-h2d/`（CSD/GDS 流水线经验，M3.5 复用）
