# ZeroFlushV0 现状 · 问题 · 研究方向（研究用文档）

> 编写时间：2026-09-12。基线：分支 `ZeroFlushV0`，HEAD = 05c3fa9。
> 目的：把"当前版本实现到什么程度、卡在哪些问题上、每个问题的证据与
> 机制、可能的解法空间"写成一份可供直接开工/交接的研究文档。
>
> **证据口径**：文中所有结论分三级标注——**【实测】**= 有原始日志/JSON/报告，
> 路径给出；**【代码】**= 读代码得出，行号给出；**【推断】**= 由前两者推演，
> 尚未直接验证。行号对应 HEAD 05c3fa9，代码改动后需重新核对。

---

## 0. TL;DR（给赶时间的人）

1. **功能完整、正确性收敛**：M1–M5 全部里程碑已实现并提交；`zf_test`
   全量 **53/53 PASS（首次全绿，89afc3d，2026-09-11）**【实测】。
2. **两个未修的结构性问题使大规模验收无法完成**：
   - **P1｜merge 关闭 + ≥50GB 全域随机键不可收敛**：数据全部经 `kDirect`
     落 L0/L1、L0 无 ZF 消费路径、原生层间 compaction 以"整分区"粒度反复
     归并 → 物化 5.08× + 原生 compaction 83.7× + 设备 93.7× 写放大，吞吐
     塌到 ~7k ops/s，2 小时无收敛被中止（**`zf_sink_request=0`：ZF 自己的
     下沉一次都没触发**）。这是**当前版本默认配置下的主路径**。
   - **P2｜M4.9 并发重叠崩溃**：`merge_into_base_level=true` × 并发 × ≥0.8GB
     必崩（8/8 复现）。决定性修复方案 `zf_install_mu_` 已定稿未落地；现有
     安全网 0 触发（第二道防线实际失效）。
3. **两项验收指标未达**：M5 P1 门（装 L0=0、写放大<9×、吞吐>27.1K）实测
   15,452 ops/s（0.74× native）、物化占墙钟 73%、L1 152 文件（设计 16）。
4. **环境不可信**：测试机系统盘 SN750 处于退化态（fsync 尖峰 5–8s、盘温
   贴 83.8°C 节流线）；fstrim 后有缓解但未根除。**任何新实验前必须过准入
   探针**，否则数据不可用。

---

## 1. 代码现状快照

| 维度 | 状态 | 证据 |
|---|---|---|
| 测试 | zf_test **53/53 ALL PASSED** | 89afc3d（09-11）；CrashMidActiveSplitReopen 3/3 |
| 里程碑 | M1(P1b 在途尾 merge)/M2(split+type-3 rewrite)/M3(起步生长)/M4(动态治理)/M5(收口) 均已实现提交 | `ZEROFLUSHV0_ROADMAP.md` |
| 默认配置 | `merge_into_base_level=false`（merge 关）、`dynamic_governance=false`、`skip_batching=true`、`routing_mode` 由 benchmark 显式指定 | `zeroflush_db.h:121,163,...` |
| 未完成项 | ① `zf_install_mu_` 未落地；② 意图持久化仅首版（decide 可重算）；③ §5 文字对齐阻塞（文稿不在仓库）；④ 活跃尾 split 已在 M4 实现并默认开 | ROADMAP §M4/§M5 |
| 最近工作 | 09-11 M4.9 根因深挖（探针+安全网，修复未落地）；09-12 merge-off 50GB 复跑中止 + 环境诊断 | git log / `output/zeroflush_m5_perf/` |

**一句话**：正确性工程已经收敛，性能/可扩展性工程卡在"merge 关闭时的下沉
管线"与"并发安装竞态"两处。

---

## 2. 实现走读（读问题前必读）

### 2.1 数据流总览

```
用户写 (Put)
  │  16B SlimLocator 写入 SlimMemtable（键+指向值的位置）
  │  值数据顺序追加到分区 WAL 段（zfwal，part×gen 组织）
  ▼
封存（Seal）—— 按 epoch 封 memtable，登记 SealedEpoch{part→gen 列表}
  │  路由表 = PartitionTable（kStatic/kSampled/kHash/kAlignL1）
  ▼
物化（Materialize）—— FlushJob::ZfMaterializeAllEpochs（db/flush_job.cc:1240）
  │  阶段 0  PlanLocked     持 DB mutex，逐 epoch 决策（materialize_job.cc:584）
  │  阶段 1  ExecutePartition 无锁，任务池并行读 WAL → 写 SST
  │  阶段 2  安装             单次 VersionEdit（持锁）→ FlushJob::Run → LogAndApply
  ▼
LSM：L1 直装/融合输出 ≤64MB/分区（设计），L1→L2 由 ZF 手动下沉请求驱动，
     L2→L3 由原生清场（阈值 4GB）
```

### 2.2 物化决策（M5 §3.2，PlanLocked 全文 materialize_job.cc:584–1534）

按分区扫描 base 层（=L1）重叠文件 `overlap`（`scan_overlap`，649+）与更浅层
（L0）重叠 `l0_overlap`（1142–1210），产出四类决策：

| 决策 | 条件（HEAD 行号） | 语义 |
|---|---|---|
| `kDirect` | `overlap.empty() && !batch_skipped && l0_overlap.empty()`（1281） | 直装 L1 单文件（写放大 1×） |
| `kMergeBase` | 判据 1：`pending + overlap_bytes ≤ kRangeMergeBytes`（1364 取反，即 1381 的 fallthrough） | 与 L1 既有文件按 seq 归并，输出单文件 ≤64MB |
| `kSkip` | 判据 2：`!overlap.empty() && l0_overlap.empty()` 且 `pending + overlap_bytes > 64MB`（1360–1380） | 请求下沉 F + 本批数据转 recovery（下批收养） |
| `kFallback` | align_l1 专属兜底（P1 稳态不可达） | 回落 L0 |

**注意判据 2 的前置**：`plan.l0_overlap.empty()`——只要本分区 L0 有文件，
判据 2 就不成立，会一路走到 `kDirect`（1281），再由 `PickInstallLevel`
因 L0 重叠而**落 L0**（materialize_job.cc:2336–2357 返回 0）。这是 P1
"数据不断落 L0"的代码根源。

另有两个前置分支：`base` 层不可替换（being_compacted/越界）→ 无条件 kSkip
（1117–1135）；孤儿收养 epoch 的 `force_replace`（1038–1072）。

关键常量：`kRangeMergeBytes = 64MB`（materialize_job.h:348，= L1 单文件上限）。

执行侧：`MaterializePartition`（直装，1571）/`MaterializeMergePartition`
（融合，1824）→ `MaterializeOutput{meta, level, decision, part_id, superseded,
replaced_file_numbers, ...}`；输出统一进 `zf_batch_outputs_`，由调用方以
**单次 VersionEdit 原子安装**（db/flush_job.h:140–147，安装代码
db/flush_job.cc:1634–1694）。

### 2.3 下沉（sink）—— 代码存在，但 merge 关闭时**未被调用**（db_impl_compaction_flush.cc:3307）

```
物化判据 2 命中 → ctx_->RequestRangeSink(lo, hi)   （zeroflush_db.cc:784）
  → 队列 sink_requests_（mutex 保护，去重）
DBImpl::MaybeScheduleZfSink()（每次 MaybeScheduleFlushOrCompaction 时调用）
  → L2 ≥ 4GB 时优先整层 L2→L3 清场（kSinkL2ClearBytes，3328）
  → 否则逐请求构造原生手动 Compaction：
       input_level = base_level()（=1，因 dynamic_level_bytes=false）
       begin/end   = 整个分区区间 [lo, hi)
       cfd->CompactRange(...)  → BGWorkCompaction 异步执行
```

**要点**：下沉是**原生 L1→L2 手动 compaction**，粒度 = **整个分区区间**。
输入选择由原生 picker 决定：L1 该区间文件 × L2 所有重叠文件。

> ⚠ **实测提醒（P1）**：这套下沉在 merge 关闭的 50GB 运行中**一次都没被
> 调用**（`zf_sink_request=0`，因为判据 2 的前置 `l0_overlap.empty()` 不成立）。
> 4TB 写放大来自**原生层间 compaction**，不是这条 ZF 路径。分析 P1 时不要
> 从 sink 入手。

### 2.4 LSM 层尺寸配置（zeroflush_db.cc:2464–2499）

| 配置 | 值 | 目的 / 副作用 |
|---|---|---|
| `level_compaction_dynamic_level_bytes` | false | base_level 恒 1 |
| `level0_file_num_compaction_trigger/slowdown/stop` | 1e8 | L0 永不产生也永不消费 |
| `max_bytes_for_level_base` | **1GB**（`ZF_MAX_L1_BYTES` 可覆盖） | L1 score 兜底阈值；**score = L1 字节 / 1GB** |
| `max_bytes_for_level_multiplier` | 未覆写（原生默认 10）→ L2 目标 10GB | 下沉累积到 10GB 触发原生 L2→L3 |
| 手动 L2 清场 | L2 ≥ **4GB** 即整层 L2→L3 | 与上面 10GB 目标不一致（4GB 更早） |

### 2.5 并发正确性机制（M5P1b，zeroflush_db.cc:2405–2425 注释 + 实现）

| # | 机制 | 代码位置 | 覆盖的窗口 |
|---|---|---|---|
| 1 | 跨批分区互斥登记（token） | `ctx mm_batch_parts_/mm_active_parts_`；`BeginMaterializeBatch`(685)/`AddMaterializePart`(696)/`EndMaterializeBatch`(718) | 规划期互相可见 |
| 2 | 学习窗 gen0 串行槽 | `AcquireGen0Slot`(739)/`WaitGen0WindowClosed`(748) | 学习批切片输出语义 |
| 3 | `last_materialized` max 单调 | `SetLastMaterializedEpoch`(672) | 并发批推进不回退 |
| 4 | 登记延后到安装完成后释放 | db/flush_job.cc:367–380 | End→LogAndApply 窗口 |
| 5 | 规划基版本刷新（学习窗等待之后） | db/flush_job.cc:1280–1300 | pick→plan 解锁窗口 |
| 6 | 安装期复检 + 整批让位（安全网） | db/flush_job.cc:1559–1633 | 规划→安装窗口（**设计如此，实际 0 触发**） |

### 2.6 type-3 rewrite / split / 治理

- `SplitPartition`（quiescent）+ `ZfSplitRewriteFile`（跨界 base 文件纯重编码
  二分）→ 单点 VersionEdit 删 F 加 F1/F2（materialize_job.cc:2392+）。
- 治理控制器 `repartition_controller.{h,cc}`：EWMA 速率 @封存边界 +
  `GovernancePolicy` 状态机 + `ZfGovernanceTick`（1s tick，O(P)）；活跃期
  只拆不合、空闲期冷回收；`governance_active_tail_split=true` 默认开。
- rewrite 带宽 ≤25%：EWMA 双速率 + `RewriteBudgetOk` 门控（89c722e）。

### 2.7 诊断开关（研究时直接用）

| 开关 | 作用 |
|---|---|
| `ZF_DBG_MM` | M4.9 探针族：`mm-plan/mm-reg/mm-end/mm-add/mm-del/mm-scan/mm-watch/mm-yield` |
| `ZF_DBG_SKIP` | skip 转移完整性（`skipx`：gens vs skipped，单任务模式） |
| `ZF_DBG_MERGE` | 融合叶子预检（乱序定位） |
| `ZF_MERGE_NEIGHBOR` | 相邻分区互斥实验（**负结果，勿启**） |
| `ZF_L0_TRIGGER` / `ZF_MAX_L1_BYTES` | L0 消费 / L1 阈值隔离实验（诊断用） |
| `ZF_STRESS_*` | MergeParallelStress harness（R0 乱序竞态复现） |
| `ZF_GOV_BG` / `ZF_DBG_GOV` | 后台治理线程 / 治理调试 |

---

## 3. 问题清单（按优先级）

| 编号 | 问题 | 严重度 | 状态 | 影响配置 |
|---|---|---|---|---|
| P1 | merge 关闭 + 50GB 全域随机键：数据经 kDirect 落 L0/L1，原生层间 compaction 整分区反复归并，不可收敛 | **致命** | 未解（有量化证据 + 机制已修正） | **默认配置** |
| P2 | M4.9 并发重叠崩溃（token 守卫覆盖不到 LogAndApply 让锁窗口） | **致命** | 根因实锤，修复方案定稿未落地 | `merge_base=1` |
| P3 | 验收性能门未达（吞吐 0.74× native、物化占 73%、L1 152 文件） | 高 | 未解 | base_merge 配置 |
| P4 | 长跑热拆分饥饿（50GB 全量 split=0/rewrite=0） | 中 | 原因待确认 | 治理 ON |
| P5 | 基线（governance=0）100GB 卡滞（热点 L1 无界 + L0 空间放大） | 中 | 已归因，靠治理规避 | governance=0 |
| P6 | 环境不可复现（SN750 退化、fsync 尖峰、盘温节流） | 中 | 部分缓解 | 全部实验 |
| P7 | 工程债：意图持久化首版、字节账目链断裂、代码小瑕疵 | 低 | 记录在案 | — |

---

### P1 — merge 关闭 + 50GB 全域随机键：L0 无消费 + 原生整分区归并，不可收敛

#### 现象【实测】

运行：`db_bench fillrandom --num=48M --value_size=1024 --threads=16`
（ZF: `--zf_partitions=16 --zf_routing=sampled`，merge 关闭=默认，治理默认关）。
13:00 开跑，15:1x 用户中止（≈2h1x），未产出结果行。

| 指标 | 数值 |
|---|---|
| DB 大小 / SST 文件 | 74GB / 1,881 |
| ZF 物化 install | **11,312 次 / 276.9GB（5.08×）**（lvl=0: 10,736 / lvl=1: 576；平均 25.1MB） |
| 原生 compaction | **4,773 次 / 输出 4,019.6GB（83.7×）**（→L1 2,506.4 / →L3 1,447.1 / →L2 66.0） |
| 最大单次 compaction | **~2.98GB、输入 ~3,051,462 条（= 一整个分区）** |
| LSM 末态 | [L0=358, L1=140, L2=100, L3=1,176, L4=0] |
| `ZFDBG-sched l1score` | **6.08→7.52 全程钉住**，`bgc` 最高 10 |
| 设备 | 562MB/s、4,761 写 IOPS、平均写延迟 788ms；SMART +4.57TB（**93.7×**） |
| 用户吞吐 | **~7k ops/s**（WAL 代际折算），无收敛迹象 |

对照【实测】：merge **ON**（09-02 cmp_1kb50）install 1,315 次 / 55.7GB
（1.17×）、27,066 ops/s、30 分钟跑完。

来源：`output/zeroflush_m5_perf/50gb_1kb_merge_off/rerun_20260912/ABORTED_RUN_AMPLIFICATION.md`。

#### 机制（实测日志反向核对，2026-09-12；**修正了既有文档的表述**）

用本轮 `zf_fill.log` 的 `ZFDBG-install` / `ZFDBG-sched` / `ZFDBG-skip` /
`ZFDBG-frz` 四类打点做全量统计，实际形态与 `ABORTED_RUN_AMPLIFICATION.md`
"ZF `sink` 管线反复整分区合并"的表述**不同**，必须修正：

1. **ZF 判决层完全没进入下沉路径**：`ZFDBG-skip` 汇总行（每次物化打印）
   末值 `sink=0`、`no_merge=0`、`busy=0`、`batchprev=0`、`cross=0`——
   **`RequestRangeSink` 全程 0 次**，`判据 2`（materialize_job.cc:1360–1380）
   从未命中。原因：判据 2 的前置是 `!overlap.empty() && plan.l0_overlap.empty()`
   ——本轮 **L0 始终有本分区文件**（安装绝大多数落 L0，见下），前置不成立，
   于是**每次物化都走 `kDirect`**（`dec=0` 占 11,532/11,532）。
2. **数据实际落点**：`ZFDBG-install` 统计 **lvl=0 ×10,939 / lvl=1 ×593**，
   文件尺寸 L0 ≈25.1MB（平均）、L1 仅 3.8–4.6MB。即 ZF 把数据不断装进 L0，
   L1 只有零星小文件。
3. **封存节拍**：`ZFDBG-frz` 稳态 ≈30MB/分区·gen（p50 30.03MB），16 分区
   → 每批 ≈480MB，约每 10–20s 一批。
4. **真正的放大器 = 原生 compaction**：ZF 侧 `sink=0` 的同时，
   `l1score` 中位 6.21 / p90 8.99 / **峰值 29.19**（n=265 采样），
   L1 文件峰值 **533**（设计 16）、L2 字节峰值 **11.94GB**（中位 4.25GB、
   p90 9.07GB），`bgc` 峰值 **20**。原生 compaction 累计输出 **4,019.6GB**，
   单次最大 **2.98GB / 3,051,462 条**。日志无 `L2 clear` 记录——手动清场
   阈值（4GB）从未真正生效。
5. **为什么原生 compaction 是"整分区"级**：写路径把键全域随机分布到各分区，
   同一分区内 L0/L1/L2 的文件键域**完全互相覆盖**（每个 L0 文件 ≈25MB 覆盖
   整个分区区间）→ 原生 picker 的 L0→L1 / L1→L2 归并一旦选中该分区，输入
   就是该分区在该层及以上的**全部历史数据**（实测 3.05M 条 ≈ 整分区），
   输出亦然。随机键 + 同区间文件互相重叠 ⇒ **每次归并都是全分区重写**，
   而 L1 长期超目标（score 6–29 倍）又使这种归并**持续被触发**。
6. **净效果**：2 小时内同一批数据在 L0→L1→L2→L3 之间被反复整分区重写，
   设备总写入 4.57TB（93.7×），平均写延迟 788ms，用户吞吐只剩 ~7k ops/s。

#### 与既有文档的两处修正（重要）

| 既有表述 | 本轮实测 | 结论 |
|---|---|---|
| "sink 管线反复调度整分区合并（`l1score` 钉住）" | **`sink=0`**：ZF sink 从未触发 | 4TB 是**原生** compaction（L0/L1/L2/L3 层间）造成，非 ZF 下沉 |
| "merge 关闭时数据先落 L0 由 sink 向 L1→L2 推" | L0 文件由**原生** L0 消费路径清空；ZF 无 sink | merge 关闭时 **L0 无 ZF 消费路径**才是"L0 只增"的机制（与报告 §基线 L0 消费三路径验证一致：`ZF_L0_TRIGGER` 追不上、门控解耦不安全） |

> 也就是说：**P1 的根因不在"下沉粒度"，而在"merge 关闭 ⇒ L0 无人吸收 ⇒
> 数据在原生 L0→L1→L2→L3 之间按"整分区"粒度反复归并"**。修复方向相应改变
> （见 §4.1）：核心是**给 L0 一个 ZF 侧消费路径（= `merge_base=1` 的 L0 吸收）**，
> 或在 merge 关闭时把落点从 L0 改回 L1（`kDirect` 直装 L1 的条件）。

#### 关键矛盾（一句话）

> **merge 关闭 ⇒ 本分区 L0 恒非空 ⇒ `kDirect` 落 L0 ⇒ L0 无人吸收（原生三触发
> 被禁用、`ZF_L0_TRIGGER` 实测追不上）⇒ 数据只能靠原生层间归并消化；
> 而全域随机键让每次归并都退化为整分区重写 ⇒ 4TB 写放大、吞吐塌到 7k。**

#### 证据锚点（本轮日志，可直接复核）

| 事实 | 命令 | 实测值 |
|---|---|---|
| **ZF sink 从未触发** | `grep -o "sink=[0-9]*"`（ZFDBG-skip 汇总行） | 全程 0 |
| 安装全部 kDirect、大多落 L0 | `grep -o "dec=[0-9]"` / `ZFDBG-install lvl=` | dec=0 ×11,532；lvl=0 ×10,939 / lvl=1 ×593 |
| L0/L1 文件尺寸 | `ZFDBG-install ... bytes=` | L0 ≈25MB；L1 3.8–4.6MB |
| 封存节拍 | `ZFDBG-frz size=` | p50 30.03MB/分区·gen |
| L1 score 失控 | `ZFDBG-sched l1score=` | p50 6.21 / p90 8.99 / max 29.19（n=265）|
| L1/L2 文件数 | `ZFDBG-sched l1n= / l2n=` | L1 峰值 533；L2 峰值 222 |
| L2 字节失控 | `ZFDBG-sched l2bytes=` | p50 4.25GB / p90 9.07GB / max 11.94GB；**无 L2 clear** |
| 原生归并规模 | 报告 §中止时刻 | 单次最大 ~2.98GB / 3,051,462 条；累计 4,019.6GB |
| 后台并发打满 | `ZFDBG-sched bgc=` | 峰值 20 |

#### 关键矛盾（一句话）

> **下沉粒度 = 分区（3GB）；而数据局部性粒度 = range（64MB）。**
> 分区内数据在 L2 全域重叠 → 每次下沉必须重写整个分区，写放大与分区
> 尺寸成正比、与增量大小无关。

#### 证据锚点

- 日志实锤：`最大单次 compaction ~2.98GB/3.05M 条 = 整分区`；`l1score 6.08→7.52`。
- 代码：判据 2（materialize_job.cc:1360）；sink 区间 = `[lo, hi)`（db_impl:3387–3400）；
  L1 目标 1GB（zeroflush_db.cc:2493）；L2 清场阈值 4GB（db_impl:3328）。
- 反证：merge ON 时数据经 `kMergeBase` 就地合并（增量级），不产生整分区下沉。

---

### P2 — M4.9 并发重叠崩溃

#### 现象【实测】

配置：`zf_govern_bench --governance=0 --merge_base=1`，1KB 值，universe 150M。

| 条件 | 结果 |
|---|---|
| 256 分区 / 16 线程 / 0.4–0.7M 写（0.4–0.7GB） | 全部干净 |
| 256 分区 / 16 线程 / **0.8M 写（0.8GB）** | **崩溃**（6 次 L1 重叠，段错误）|
| 8 分区 / 16 线程 / 0.8M 写 | **崩溃**（15 次）→ 与分区数无关 |
| 256 分区 / 16 线程 / 256B × 3.2M 写（≈0.8GB） | **崩溃** → 按**写入字节**归一 |
| 256 分区 / **1 线程** / 1.0M 写（1GB） | 干净（单线程 3.2M 仅 1 次非致命）|
| 256 分区 / 16 线程 / 0.8M 写，**merge_base=0** | 干净 |
| 触发配置重复 8 轮 | **8/8 复现**（结构性竞态，非抖动）|

来源：`output/zeroflush_m4_perf/REPORT.md`（§M4.9 各节）。

#### 根因链（已实锤，唯余一处未钉死）【实测 + 代码】

1. 重叠对 = **两个不同 FlushJob 批次**对**同一分区**产出的 `kMergeBase` L1
   输出（ZFDBG-mm 探针：【实测】两条 mm-add 均为 ZF 输出——commit 05c3fa9
   更正了早前"对手方是原生 compaction"的错误断言）。
2. 探针显示：规划期扫描视图互不可见（`ov` 列表缺对方文件号）；冲突分区在
   窗口内出现**不同 token 的登记成功**（pid5: token 159/167/175/177 均 ok=1）。
3. 即：后批在**对方 `LogAndApply` 的 MANIFEST IO 让锁窗口**内完成规划——该
   窗口 DB mutex 被释放，token 守卫不覆盖 → 双方 edit 相继 apply →
   VersionBuilder 报 `force_consistency_checks: L1 overlapping ranges`。
4. 决定性时序【实测】：job74 的 edit 已构建打印但未 apply 时，job69 已在规划
   且 `ov=[305]` 不含 376；**job69 的 token73 登记成功 ⇒ token71 已 End**，
   而 End 名义上在 TryInstall 之后 → **存在"edit 尚未落地即释放 token"的路径**
   （疑似 TryInstall 失败/重试或某错误出口提前 End）——**这一处尚未钉死**
   （第二轮回：加 `mc_.base == cfd_->current()` 断言恒真、End 全打点时序正常）。
5. M2 文档遗留项原文即"与并发 flush 的 LogAndApply 让锁窗口竞态未做
   being_compacted 防护"——本崩溃是其第一次被实锤。

#### 现状：安全网为什么没兜住

`db/flush_job.cc:1559–1633` 的安装期复检在**规划前后同一个持锁窗口**内读
`cfd_->current()`：如果对方 edit **尚未 apply**，当前版本里当然看不到它的
输出 → 复检通过 → 随后对方 apply、我方 apply，相隔极近，安全网**0 触发**
（实测）。它只在"对方已落地但我的规划视图过期"时生效——不是这个场景。

#### 已定稿修复（未落地）

`zf_install_mu_`（ROADMAP §M4.9）：DB 级互斥，ZF 批自阶段 0 **规划前
try_lock**、持至安装（LogAndApply）完成释放；try_lock 失败 → 整批 kSkip
让位。跨批 plan→apply 全串行化后，让锁窗口不可能再插入规划。保留安装期
复检为第二道防线。

#### 负面结果记录（避免重复踩坑）【实测】

| 尝试 | 结果 |
|---|---|
| 相邻分区互斥扩展（`ZF_MERGE_NEIGHBOR`） | 8→11 次崩溃（恶化） |
| 安装期复检 + 整批让位 | 复现中 0 触发 |
| 安装窗口标记 + 规划期整批让位 | 仍 8/10 崩 |
| 门控解耦 `ZF_L0_ABSORB`（L0 相交即吸收） | 输出越界 → L1 重叠，收紧为"完全包含"仍崩 7 次，已撤销 |

---

### P3 — 验收性能门未达

M5_DESIGN §5 P1 门：**装 L0 计数 = 0、读命中 63.2%、写放大 < 9×、吞吐 > 27.1K**。

实测（09-07，数据丢失修复后，50GB/1KB/16T/sampled/base_merge）【实测】：

| 指标 | ZF | native 同窗 | 差距 |
|---|---|---|---|
| fillrandom | **15,452 ops/s**（3,236s）| 20,752 ops/s（2,409s）| **0.74×** |
| readrandom | 188,086 ops/s | 137,972 ops/s | 1.36×（反超）|
| 命中 | 63.26%（= 期望，零丢失 ✓）| 63.27% | — |
| L0 终态 | 3 文件 / 0.02GB | — | 设计要求 0 |
| LSM 终态 | **L1 152×9.9GB** / L2 180×9.8GB / L3 352×18.2GB | — | 设计 16 文件（1/range）|

已知成因【实测/推断】：
- 物化占墙钟 **73%**（材料化流水是瓶颈）；
- L1 152 文件 vs 设计 16 —— 【推断】下沉节奏 + 合并输出切分导致每个
  range 多于 1 文件；文件数多又放大原生 L1→L2 的输入选择，形成相互放大；
- 27.1K 目标来自旧配置时代（文档已注明），当前同窗 native = 20.75K →
  以"≥ native"为新目标更合理。

**同窗 merge ON 对照（09-02 cmp_1kb50，`output/zeroflush_m3_perf/cmp_1kb50/RESULTS.md`）**：
fillrandom ZF **27,066** vs native 26,409 ops/s（各 1,773s/1,818s），命中 63.18%，
install **1,317 次 / 55.7GB（1.17×，原始日志 `zf_fill.log` 复核：lvl=0 1,185 /
lvl=1 130）**。⚠ 该轮 ZF 运行时 load 3–8、native 33，非严格同负载，需交替复测。
即：**同为 50GB/1KB，merge ON 是 1.17× 写放大、27K ops/s；merge OFF 是
93.7× 设备写入、7K ops/s**——这是 P1 最直接的对照。

---

### P4 — 长跑热拆分饥饿

50GB 全量验收（M4 DG，09-10）【实测】：**split=0、rewrite=0**（"长跑热拆分
未触发"），而短程 4M 写同参数 splits=1 成功。推断为规模下 base 门槛饥饿
（分区 base 字节 ≥ `growth_split_min_bytes=32MB` 难满足或治理 θ 判定在
大 P 下的 rate/mean 比不越界）——**未确认**，见 ROADMAP §M5 进度。

研究切入点：`repartition_controller.cc` 的 `GovernancePolicy` 判定与
`ZfGrowPartitions` 前置（排空检查点）在 16T 连续写下是否可达；建议加
"治理决策日志"（每个 tick 打印各分区 rate/mean/θ 比较与拒绝原因）。

---

### P5 — 基线（governance=0）100GB 卡滞

【实测】OFF 臂在 73.9M/104.9M（70%）处停滞（~48 ops/s，主线程 futex 阻塞，
无 error/corruption；清盘复现于 72.5M/69%）。归因：L0 积压 23k 文件/11.25GB、
L1 10.73GB（score≈10）→ 原生 L1→L2 与 ZF 物化 being_compacted 冲突 →
物化 kSkip 累积、sealed_wal 37.9GB 冻结 → 写路径堵在 epoch/imm 水位。
`ZF_MAX_L1_BYTES=1TB` 消除卡滞但 66% 处磁盘耗尽（L0 22k 文件/58.7GB 只增不消）。

这其实是 **P1 的同一根因在另一配置下的表现**：L1 目标 1GB 与分区尺寸失配 +
L0 无消费 → 双结构性问题。当前靠 governance ON 规避（拆分热点）。

---

### P6 — 环境（不是代码问题，但阻塞实验）

【实测】`output/zeroflush_m5_perf/50gb_1kb_merge_off/ENV_DIAGNOSIS.md`：
- 盘：`WD_BLACK SN750 2TB`（消费级），负载期 84.8–85.8°C（`high=83.8°C` ALARM）、
  fsync 并发尖峰 max 6,745ms、内核阻塞栈 = ext4 journal + WBT 节流；
- 诱因：830GiB 已删除空间未 TRIM（周级 fstrim.timer）→ FTL GC 放大 + 延迟尖峰；
- 已处置：`fstrim` 交还 830.2GiB、`wbt_lat_usec=0`、`mount commit=60`、风扇；
  复测 4M 探针 1,166 → ~5.1k ops/s，**long op 8.57s 仍在**；SMART 健康
  （Used 13%、Spare 100%、Media Errors 0）。
- 另：期间 3 次 OOM（db_bench ×2 各 54GB、node 58.7GB/62GB 内存）。

**准入 gate**：每次正式实验前跑 4M ops 探针（正常应 ≤3 分钟、0 long op），
并记录 fsync/温度遥测。

---

### P7 — 工程债（低优先，但会影响后续研究）

| 项 | 说明 | 位置 |
|---|---|---|
| 意图持久化首版 | decide→route-switch→retire 只落了路由快照，M1c 的 DRAINING sidecar 是唯一持久化意图 | ROADMAP §0 |
| **字节账目链断裂** | `pending = se_.part_bytes[pid]` 只含本批分区、收养字节未流转 → 判据 2 的 `pending` 恒为本批大小 → M5 §3.2 的"字节化攒批"被删掉绕过（注释自述） | materialize_job.cc:1348–1362 |
| 安全网 yield 路径 fprintf 格式串坏 | `"ZFDBG-mm-end site=%s job=%%d token=%%llu s=%%s\n"` 参数错配（调试输出，非功能） | db/flush_job.cc:1618 |
| kSkip 上限口径不一 | `kMaxSkipGenerations=2`（代数）仅剩孤儿/align 兜底路径；稳态走字节判据 | materialize_job.h:341–346 |
| MultiEpoch 既有偶发（kHash base） | M5 修复后未再复现，保留观察 | M5_DESIGN §待办 |

---

## 4. 解法空间（研究假设，按问题分组）

> 以下为**假设与推演**，多数未验证。每条给出机制、预期效果、代价、验证方法。

### 4.1 P1 下沉/归并放大（核心）

**根因重述**（基于 §P1 的实测反向核对）：不是"ZF 下沉粒度太大"，而是

- merge 关闭 ⇒ 本分区 L0 恒有文件 ⇒ `kDirect` 落 L0（判定链见 §P1）；
- L0 没有 ZF 侧消费路径（原生 L0 三触发被禁、`ZF_L0_TRIGGER` 实测追不上）；
- 数据只能靠**原生层间 compaction** 消化，而全域随机键使每次归并都退化为
  **整分区重写**，且 L1 长期超目标（score 6–29）使其持续触发。

因此解法分三层：**(a) 让数据别落 L0；(b) 给 L0 一个 ZF 消费路径；
(c) 降低原生归并的整分区退化和触发频率。**

| 方案 | 机制 | 预期 | 代价/风险 | 验证 |
|---|---|---|---|---|
| **H1.1 修 `kDirect` 落点**（改 (a)） | merge 关闭时若 L1 有本分区文件，当前直接判 kSkip/落 L0。改为**仍直装 L1 并替换旧文件**（等价 `kMergeBase` 的"无条件融合"语义），使 L1 保持单文件、数据不落 L0 | 数据不再进 L0 → 原生层间归并失去输入 | 与 merge 打开语义趋同（等于把"融合"变成默认路径）——需先修 P2 | 50GB fill：`ZFDBG-install lvl=` 分布、L0 计数、4TB 是否消失 |
| **H1.2 给 L0 一个 ZF 消费路径**（改 (b)） | 启用 A 路（原 L0 融合，`l0_overlap` 非空也走 `kMergeBase`）——这正是 `merge_base=1` 做的事；或修好 `ZF_L0_TRIGGER` 的原生消费（实测 1.6k 文件/百万写追不上，需并行消费 + 阈值联调） | L0 恒 0 | A 路 = P2 崩溃面；B 路已被三路径实验证伪 | `ZFDBG-skip sink/no_merge` 计数、L0 文件数 |
| **H1.3 降低原生归并的整分区退化**（改 (c)） | 在不改数据布局的前提下，限制单次 compaction 的输入：`max_compaction_bytes` / `target_file_size_base` 显式设小 + `subcompactions` 生效；或把 L0 文件键域切成子区间（结合 split） | 单次 compaction 从 3GB 降到 ~64–256MB；4TB 成比例下降 | 只降单次规模、不降次数；跨界文件边界吸附需调 | 单次 compaction 输入字节分布 + 累计输出 |
| **H1.4 提高 L1 目标/对齐尺寸** | `ZF_MAX_L1_BYTES` 抬到 ≈ 分区数据量，使 l1score<1，隔离"score 兜底"与"L0 推进"各自贡献 | 判定 4TB 里多少来自 score 兜底 | 治标；L0 空间放大仍在 | A/B：4TB 变化幅度 |
| **H1.5 默认改用增量融合**（=`merge_base=1`） | `kMergeBase` 是增量级（30MB+4MB→单文件），merge ON 实测 1.17× / 27,066 ops/s / 30min | 直接回到可用性能 | **必须先修 P2**（merge=1 在 ≥0.8GB 必崩） | 修 P2 后 50GB 双臂 |
| **H1.6 关闭冲刷期强制融合** | 当前 `closing_flush()` 时判据 2 被绕过（强制融合）——可检查长跑中该模式的影响 | 小 | 待确认是否有实际收益 | 代码审查 |

**建议主攻顺序**：**H1.4**（隔离变量，拿回可跑基线）→ **H1.1**（最小语义
修改，让数据不落 L0；需评估它与"融合开关"的关系）→ **H1.3**（降低原生归并
规模）→ **H1.5**（P2 修完后的正解）→ H1.2（并行消费 L0 的长期方案）。

### 4.2 P2 并发安装（确定性修复已定稿）
| 方案 | 机制 | 预期 | 风险 |
|---|---|---|---|
| **H2.1 `zf_install_mu_`**（推荐） | DB 级 mutex，plan 前 try_lock → apply 后释放；失败整批 kSkip 让位 | token 守卫覆盖面补齐，让锁窗口不再可插入规划 | 批吞吐略降（串行化 plan→apply）；需验证 let-lock 顺序（DB mutex ↔ install_mu）无死锁 |
| H2.2 钉死"提前 End"出口 | 给全部 `EndMaterializeBatch` 调用点打点（token/job/s.ok()/apply 状态），定位 token71 提前释放的出口后单点修 | 若真存在提前 End，修后 token 守卫即可覆盖 | 若提前 End 是表象、真实原因是 TryInstall 失败重试，仍需 H2.1 |
| H2.3 让 picker 感知"未安装输出" | 把批内/他批在飞输出登记进 VersionStorageInfo 级视图（类似 being_compacted 标记） | 规划扫描即可见 → 无需串行化 | 改动原生 Version 结构，风险高，不推荐 |

### 4.3 P3 性能门

- 物化占 73% 墙钟 → 需要 profile 分解（读 WAL / 归并 / 建 SST / 安装），
  历史 M4.6c 的并行消费调优可复用（R36–R44 路径）。
- L1 文件数 152 → 16：先查合并输出切分（`target_file_size` 与
  `kRangeMergeBytes` 是否一致）与下沉节奏（每次 sink 后 L1 是否真的空）。
- 目标口径建议改写为"≥ 同窗 native"，并把 27.1K 旧目标标注废弃。

### 4.4 P4/P5

- P4：治理决策日志（tick 级打印 rate/mean/θ/拒绝原因）+ 50GB 复跑采样。
- P5：与 P1 合并解决——L1 目标/下沉粒度对齐后，100GB OFF 卡滞预计同步缓解；
  可先用 `ZF_MAX_L1_BYTES` 做隔离实验验证。

---

## 5. 复现手册

### 5.1 环境准入（每次实验前）

```bash
cd /home/embed/hyl/metadata_offload
# 盘温 <74°C、4M 探针 ≤3 分钟、无 long op 才开跑
LD_LIBRARY_PATH=$PWD/source/rocksdb/build:/usr/lib/x86_64-linux-gnu \
  ./source/rocksdb/build/db_bench --benchmarks=fillrandom --key_size=16 \
  --value_size=1024 --num=4000000 --threads=16 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --use_existing_db=0 --db=/tmp/probe
sudo fstrim -v /    # 大删数据后必做
```

### 5.2 P1（merge-off 50GB）

```bash
./output/zeroflush_m5_perf/50gb_1kb_merge_off/rerun_20260912/run.sh
# 臂：ZF（--zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64）
#     → 原生同参；各含 fillrandom + readrandom
```

### 5.3 P2（M4.9 崩溃矩阵）

```bash
# 最小复现：256 分区 / 16T / 0.8M 写 / merge_base=1
./source/rocksdb-zeroflush/build_rel/zf_govern_bench \
  --governance=0 --merge_base=1 --partitions=256 --threads=16 --writes=800000
# 决定性探针（stderr）
ZF_DBG_MM=1 ZF_DBG_SKIP=1 <上述命令> 2> mm.log
```

### 5.4 关键日志/报告索引

| 内容 | 路径 |
|---|---|
| M4.9 全量证据（崩溃矩阵/根因/三连负结果/更正） | `output/zeroflush_m4_perf/REPORT.md` |
| merge-off 放大量化 | `output/zeroflush_m5_perf/50gb_1kb_merge_off/rerun_20260912/ABORTED_RUN_AMPLIFICATION.md` |
| 环境诊断 | `output/zeroflush_m5_perf/50gb_1kb_merge_off/ENV_DIAGNOSIS.md` |
| M5 50GB 验收（含数据丢失修复） | `output/zeroflush_m3_perf/m5_50gb_fix/RESULTS.md` |
| M5 设计（per-range 合并架构） | `zeroflush/M5_DESIGN.md` |
| 路线图/里程碑状态 | `zeroflush/ZEROFLUSHV0_ROADMAP.md` |
| 全项目数据汇总与冲突项 | `EXPERIMENT_DATA_SUMMARY.md`（仓库根） |

---

## 6. 给后续研究者的开放问题

1. **P1 的判据设计**：`kDirect` 为什么在"L1 有本分区文件、L0 也有本分区文件"
   时选择落 L0 而不是替换 L1 旧文件（即 `kMergeBase` 的无条件融合语义）？
   这是 M5 §3.2 的刻意设计（"L1 无文件才 kDirect"）还是实现偏差？——**这是
   决定 H1.1 是否需要修改设计文档的关键问题**。
2. **P1 的原生归并触发源**：`l1score` 6–29 的成因里，"L0 推进（L0→L1）"与
   "score 兜底（L1→L2）"各占多少？（无分开计数，需插桩或用 H1.4 隔离。）
3. **P2 的最后一环**：token71 提前 End 的确切出口在哪？如果是 `TryInstall`
   失败重试路径，`zf_install_mu_` 是否足够（还是连重试也要纳入锁窗口）？
4. **物化 73% 墙钟**：读 WAL / 归并 / 建 SST / 安装四段的占比是多少？（无
   按段计时数据，需插桩。）
5. **L1 文件 152 vs 16 的偏差**（P3）：是切分逻辑、下沉节奏，还是并发安装的
   superseded 链残留？（需对 L1 层做一次文件级归因 dump。）
6. **L0 该由谁消费**：三路径验证（原生 L0→L1 追不上 1.6k 文件/百万写；
   门控解耦不安全；M4.9 融合路径已崩）→ 唯一可行方向是 ZF 物化自身吸收
   L0（即 P2 修好后的 `merge_base=1`）。是否接受"merge 默认必须开"？

---

## 附录 A：数据可信度说明

- 本文档数字与 `EXPERIMENT_DATA_SUMMARY.md` §1/§6/§7/§9 一致；跨 harness
  （gov_bench vs db_bench）数字不可直接比。
- P1 的 4,019.6GB/276.9GB 来自运行中 DB 内部计数器（zf_fill.log /
  run2_telemetry.log），SMART 差值 4.57TB 为独立旁证——两者同量级，互证。
- M4.9 崩溃矩阵来自 `zf_govern_bench`（harness 与 db_bench 不同），
  但"与分区数/值大小无关、按写入字节归一"的结论与 db_bench 侧观察一致。

## 附录 B：与早期结论的冲突/更正

1. **P1 机制**：早期文档（`ABORTED_RUN_AMPLIFICATION.md` §机制）表述为
   "ZF 的 sink 管线反复调度整分区合并"；本文档用同一份 `zf_fill.log` 的
   `ZFDBG-skip`/`ZFDBG-install` 打点反向核对后更正为 **ZF sink 全程 0 次、
   放大来自原生层间 compaction**（依据：`sink=0`、`dec=0`×11,532、
   `lvl=0`×10,939）。旧文档的"2 小时内每分区被整份重写 ≈80 次"结论不变，
   但**执行者**由 ZF 下沉改为原生 compaction。
2. M4.9 对手方：早前判定"原生 compaction 的 L1 输出"→ **已更正**为"两条
   均为 ZF 输出"（未开 `ZF_DBG_MM` 的日志不能推断）。
3. 环境负载：早前"有外部负载拖慢"→ **已订正**为 load 22–26 全部来自本实验
   自身线程（OOM 快照佐证）。
4. 复核用命令（本文档所有 P1 数字均可重放）：
   ```bash
   cd output/zeroflush_m5_perf/50gb_1kb_merge_off/rerun_20260912
   tr '\r' '\n' < zf_fill.log > /tmp/zf.log
   grep -o "sink=[0-9]*" /tmp/zf.log | sort -n | tail -1        # → 0（ZF sink 从未触发）
   grep -o "dec=[0-9]" /tmp/zf.log | sort | uniq -c             # → 11532 dec=0
   grep -o "ZFDBG-install lvl=[0-9]" /tmp/zf.log | sort | uniq -c
   grep -o "l1score=[0-9.]*" /tmp/zf.log | sort -n | awk '{a[NR]=$1} END{print a[int(NR/2)], a[NR]}'
   grep -o "l2bytes=[0-9]*" /tmp/zf.log | sort -n | awk '{a[NR]=$1} END{print a[int(NR/2)], a[NR]}'
   ```
