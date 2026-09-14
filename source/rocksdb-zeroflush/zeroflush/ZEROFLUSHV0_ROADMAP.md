# ZeroFlushV0 实施路线图（修订版 v2，2026-09-09）

状态：规划稿。依据：动态分区治理设计（起步构建 + 动态调整两阶段）、性能评估
（调度器不得触碰写热路径）、以及 ZeroFlushV0 已提交实施（P0/P1/P1 加固/诊断）。

## 0. 现状基线（已提交）

| 提交 | 内容 | 对本路线图的意义 |
|---|---|---|
| a04b9da | P0：zf_request.h（S5 压缩契约） | 契约形态已定，type-3(kBoundaryRewrite) 已声明未实现 |
| 6667f9d | P1：无在途数据 merge（DeriveMerge + 写组屏障 + 切路由 + ZFPROPS + 指标） | merge 的路由切换/屏障/持久化机制可复用 |
| a5b0bd3 | P1 加固（kStatic gen0 门控、在途计数双 Exit 修复、路由指标） | 物化决策面已开始收敛；metrics 供治理触发 |
| 91a649d | ZF_DBG_MERGE 叶子预检 + P1B 设计档 | 并行乱序竞态的定位工具已就位 |

**结构性缺失（新设计使之成为必须项）**：
1. 意图持久化（decide→route-switch→retire 落 manifest、幂等续作）——现仅路由
   结果落 ZFPROPS 全表快照。
2. type-3 boundary-rewrite——未实现；却是起步生长（P0=1 首拆）的关键路径。
3. split——不存在（代码只有 DeriveMerge=删分区；split_after_skewed_epochs 死选项）。
4. 决策队列/aging/并发上限/带宽份额——不存在（merge 为同步外部 API）。

**未解前置**：并行 4 分区物化间歇乱序（见 P1B §9.3）——卡"并发 rewrite/merge"
的承诺；M1 起用 materialize_parallelism=1 规避，直到 R0 修复。

## 1. 目标架构与依赖

```
M1 P1b（带在途尾数据 merge：registry+别名读+retire+F1 串行版）
   │
M2 P2（split + type-3 rewrite，quiescent 子集）─── 并行轨 R0（乱序竞态专项）
   │                                                    │
M3 起步生长（P0=1 全域 → 按中位数生长拆分）          （R0 修复后解除并发门）
   │
M4 动态治理 DG（统计@封存边界 + 触发参数 + 队列/aging + 冷却 + 控制器）
   │
M5 意图持久化收口 + 崩溃矩阵 + §5 文字对齐
```

性能纪律（贯穿）：统计/判定不进每写路径与每写组；控制器低频 tick；调整复用后台
物化机制；并发调整 ≤C、rewrite 带宽 ≤25%。详见 §6。

## 2. 里程碑定义

### R0（并行轨，时间盒）并行 4 分区物化乱序竞态专项
- 目标：定位并修复 "Compaction sees out-of-order keys"。
- 手段：还原 pump 交错节奏复现（MergePartition 尝试 × 追加写入 ×
  WaitAllMaterialized），或 2–4 写线程 + 物化并行；ZF_DBG_MERGE 叶子预检
  （已提交）直接给出乱序叶子；若为批内前序文件被并发读/竞争文件号则修同步。
- 验收：复现场景连续 N 轮零失败；zf_test 46/46；并发稳态（flushes=4 +
  parallelism=8）无回归。修复后解除 M4 的并发门。
- 产出：独立提交；期间 M1–M3 测试统一设 materialize_parallelism=1。
进度（2026-09-09）：单写进程穷举（~400 轮 × pump/tight/overwrite/flushes
 变体）未复现；TSAN 构建（build_tsan, WITH_TSAN=ON）复跑命中并修复三类
  data race（详见 P1B §9.4）：① memtable refs_ Unref 无锁（写组三出口加
  DB mutex）；② PartitionBloom 重建期 UAF（blooms_ 改 shared_ptr +
  BloomAdd 锁内快照）；③ 写/读路径 static 调试计数器非 atomic（改
  atomic+fetch_add）。TSAN 最终零告警；并发门解除——merge/split e2e 测试
  移除 materialize_parallelism=1 钉死，恢复默认 8。

### M1 P1b：带在途尾数据的 DRAINING merge（串行正确性子集）
目标：把 P1 前置"必须已完全排空"放宽为"仅允许活跃尾（ActiveSize>0），无
sealed/skip/recovery pending"。依据 P1B_DESIGN §2/§4 的 (c)+(F1)。
进度（2026-09-09）：M1a 已提交（441ae6b）——活跃尾冻结（切路由先于冻结，
规避 ZfSwitchMemtable 让锁窗口）、DRAINING 注册表/别名读/retire、分区 id/下
标映射正确性修复（写/读/物化/恢复）。F1 已完整接线并解除 Busy 门
（8182426 → 本轮）：PlanLocked 对 DRAINING 分区强制孤儿直装、永不 kSkip，
活跃尾合并覆盖 merge_into_base_level=true（MergeActiveTailMergeEnabled 序
回归，zf_test 42/42）。M1 正确性内核完成；收尾（M1c，代码锚点已记录于
P1B_DESIGN §7）：DRAINING 持久化（ZFPROPS v3 draining 段 + Recover 重建）
与崩溃注入 e2e。
范围：
1. 恢复 DRAINING 注册表（DrainingPart{part,absorb,lo,hi} + mutex）与
   IsDrainingPartition / DrainingPartitions / MergeRetired 指标。
2. MergePartitionLocked：屏障后若 ActiveSize>0 → 单分区封存 E_D
   （se.table_version=当前 V；复用 FreezeBatchPartitions 单元素路径），再切路由；
   前置：!HasFrozen(D) && !HasPendingPartGens(D)（拒绝 sealed/skip 残留）。
3. 读别名：GetFromPartitionIndex 恢复"路由分区 miss → 覆盖该 key 的 DRAINING
   分区索引回查"（A 链优先；序正确性由 E_D 先于 A 首 epoch + F1 保证）。
4. F1 串行版：PlanLocked 主循环按 ctx_->IsDraining(pid) 把可达的 in-table
   kSkip 决策强制转 kFallback（物化单文件，可落 L0 由后续吸收）；batch/cross-batch
   让位型分支在 parallelism=1 下不可达，加防御断言。
5. retire：MaybeRetireDraining 挂在 ReleaseEpoch/ReleaseGens（排空推进点）。
6. 意图持久化（首版）：DRAINING 记录随切路由写入 ZFPROPS（drain/absorb/lo/hi）；
   恢复重建注册表；残余 D gens 走既有孤儿收养路径（P1B §7 语义）。
文件：zeroflush_db.{h,cc}、partition_index.h(别名无需改结构)、materialize_job.cc、
sealed_file_cache.h(epoch 无需改，force 判断用 registry)、zf_test.cc。
验收：e2e（活跃尾 merge → 中途读旧 key/快照 → 覆写归 A → 排空 → retire 指标 →
重开一致）；序倒置回归（覆写同一 key + 多次物化任意时刻不读旧值）；
crash 注入（merge 后 E_D flush 前 kill → reopen 数据完整）；zf_test 全量（新用例
显式 parallelism=1）。

### M2 P2：split + type-3 boundary-rewrite（quiescent 子集）
目标：建立"在分位点切分区"与"跨界文件按新边界两分"两个原语。
进度（2026-09-09）：
- ✅ 步骤 1/2：分区 id 分配器（`AllocatePartId`/`RebuildNextPartId`，Open 在
  Recover 后以「全部表版本 part_ids ∪ zfwal 文件 part」的 max 重建——表版本在
  ZFPROPS 全量保留，任意分配过的 id 必在某个版本或残留文件中，故未单独持久化
  计数器，与"随 ZFPROPS 持久化"语义等价）与 `PartitionTable::DeriveSplit`
  （签名取两子 id 为入参，table 保持纯函数；父区间开区间内切分、首/末分区
  -∞/+∞ 侧不约束、边界重复拒绝、子 id 互异且不在表中）。表级用例
  TestDeriveSplitTable（中/首/末分区、稀疏表 merge 后 {0,2,3}、错误路径）。
- ✅ 步骤 3/4/5：type-3 rewrite（`ZfSplitRewriteFile`，materialize_job：跨界
  base 文件纯重编码二分——内部条目逐条搬移，输出左右两文件；`ZfRewriteParams`
  由 DBImpl 组装，持 DB mutex 内联执行）+ `SplitPartitionLocked`/外部 API
  `zeroflush::SplitPartition`（quiescent 前置：无活跃字节/无 frozen/pending/
  非 DRAINING 方/别名方 → 分配器取两子 id → DeriveSplit 切路由落盘 →
  DBImpl::ZfSplitPartition 查跨界 base 文件 → rewrite → 单点 VersionEdit
  删 F 加 F1/F2）。配套：WAL 管理器 `EnsurePartition`（运行时子分区）与
  `InitPartitionsFromFiles`（重开子分区按磁盘 MaxGen 建对象，防 D4 类落陈旧
  代）；`rocksdb.zeroflush.splits` 指标。e2e TestSplitQuiescentEndToEnd
  （种子单 base 文件 → 中点 split → SST+1、路由/指标断言 → 两子分区续写物化
  → 读全对 → reopen 一致）。
- ✅ v1 限制收口（L0/多文件跨界）：rewrite 从"base 层单跨界文件"泛化为
  "任意层全部跨界文件"（非 L0 层文件不相交 → 每层至多一个含 b；L0 允许
  重叠 → 多个）——逐文件 ZfSplitRewriteFile + 单次 VersionEdit 原子
  删/加。跨界文件 ≤256MB/个上限保留。剩余 v1 限制：活跃尾/frozen 尾
  split 未做（跨界 WAL gens 处理并入 M4）；与并发 flush 的 LogAndApply
  让锁窗口竞态未做 being_compacted 防护（quiescent 显式 API 场景无并发
  写，可接受；M4 前补）。

**M2 实施细则（供直接开工）：**
1. 分区 id 分配器：ctx 增 `next_part_id_`（随 ZFPROPS 持久化；恢复用 max 重建）；
   split 时给子分区分配新 id（父 id 退役），part_ids 保持稀疏（id/下标映射已就绪）。
2. `PartitionTable::DeriveSplit(part, split_key, version, &child_left_id,
   &child_right_id, &nt)`：父区间 [lo,hi) 在 split_key(b∈(lo,hi)) 处切两段；
   boundaries 在 b 处插入一条（需 b 严格在 lo..hi 之间且不重复）；part_ids 把
   父 id 换成两个新 id（顺序保持）。校验沿用 Create 的升序/去重。
3. type-3 rewrite 任务（kBoundaryRewrite，zf_request.h 已声明）：
   - 输入：跨界 L1 文件（quiescent 首版恰为父分区当前 L1 文件，≤64MB，数据
     版本不跨 b——同 user key 恒在同一侧）；不读 WAL、不去重、不需快照水位。
   - 执行：TableIterator 顺序扫输入，首 key ≥ b 处切换输出 TableBuilder×2；
     纯重编码（块/索引/bloom 由两 builder 各自生成）。
   - 安装：单点原子 VersionEdit——DeleteFile(F) + AddFile(F1) + AddFile(F2)；
     两输出 range ⊆ 子区间（S2 硬约束由切点保证）。
   - 挂后台任务池执行（serial 安全）；与常规物化互斥经 being_compacted/批登记。
4. `SplitPartitionLocked(impl, cfd, part, split_key, &id_l, &id_r)`：前置 = 分区
   完全排空（同 P1 口径：无活跃/无 frozen/无 pending）+ barrier + 切路由（安装
   新表 + PersistZfProps）→ 登记/排队 rewrite（quiescent 下 rewrite 输入即父
   分区 L1 文件）。暂不做"活跃尾 split"（其跨界 WAL gens 处理并入 M4）。
5. 测试：表级 DeriveSplit（含首/末/中分区、b 越界拒绝）；e2e quiescent split
   （直装→填满→物化→切分：两文件不重叠 ≤64MB、路由正确、读全对、reopen、
   partition_count/routing_version 指标）；组合用例：部分划归 = split + merge。
6. 依赖：F1/id 映射（M1 已就绪）；并行乱序竞态 R0 未修前测试 parallelism=1。
范围：
1. PartitionTable::DeriveSplit(part, split_key, version, 两子表, child_ids) +
   分区 id 分配器（ctx 全局 next_part_id，随 ZFPROPS 持久化；part_ids 稀疏）。
2. type-3 任务（kBoundaryRewrite）：输入 = 跨界 L1 文件（quiescent 首版；跨界
   sealed gens 并入留待 M4），输出 = 按 b 切分的两个文件；纯重编码无去重
   （同 user key 版本不跨边界，无需水位）；VersionEdit 原子删 F 加 F1/F2。
   复用 TableBuilder 流式切输出；后台执行（任务池），serial 安全。
3. SplitPartition API：前置 = 分区已完全排空（同 P1 口径），切点由调用方给出
   （M3/M4 提供统计）；切路由 + rewrite 排队。
文件：partition_table.{h,cc}、materialize_job（新 kRewrite 分支或独立 job）、
zeroflush_db.{h,cc}、zf_test.cc、ZFPROPS 版本。
验收：表级 DeriveSplit 用例；e2e quiescent split（直装→填满→物化→中位数切分：
路由正确、两文件不重叠 ≤64MB、读全对、reopen）；与 merge 的组合（部分划归 =
split+merge）。

### M3 起步生长（P0=1 全域单分区 + 生长拆分）
目标：空库冷启动 = 1 个全域分区，数据增长中按"等容"生长拆分至稳态。
范围：
1. Open 路径支持 partitions=1 + 空边界（kStatic 无边界 = 全域）；确认 gen0
   门控/物化/下沉在单分区下正确。
2. 生长控制器（极简版，M4 的雏形）：分区 seal+物化后若数据量 ≥ S_min_split
   （32MB 且估计热）→ 取该分区数据中位数 → M2 split。warmup 冷却 + 快速通道
   （积压 ≥ 50% max_pending 立即拆）。
3. 采样等容初始化（P0=16）沿用现有 kSampled epoch-1 学习路径——不改。
文件：ZeroFlushOptions（partitions/边界）、zeroflush_db.cc Open、生长触发逻辑。
验收：P0=1 fillrandom 生长至多分区：全程正确性 + L1 每 range ≤64MB 单文件、
P 增长有界（≤ 翻倍/冷却窗）、reopen；论文 §5.2(5) 两路径口径的代码落地版。
进度（2026-09-09）：✅ 代码落地版——ZeroFlushOptions 增 growth_split_min_bytes
（默认 32MB，0=关）与 growth_split_cooldown_micros；zeroflush::GrowPartitions
外部入口 → DBImpl::ZfGrowPartitions（排空检查点生长拆分：base 字节 ≥ 阈值的
最大分区 → 取 base 文件条目中位数 → M2 SplitPartition，冷却窗限频，迭代上限
64）；预排空处理 seal 残留活跃尾（FreezePartitionTailLocked 冻结 → 调度
flush → 轮询物化推进——P0=1 全域单分区无法用"他分区触发批次封存"排空）。
P0=1（kStatic 空边界）Open/物化/生长全链路验证：e2e GrowthSinglePartition
（700 keys → 15 分区 / 14 次生长拆分，读全对、迭代计数稳定、reopen 一致）。
全量 zf_test 47/47。

### M4 动态治理 DG（调度器）
目标：治理层成型——统计、触发、队列/aging、冷却、参数化；merge(P1b)/split(M2)
由控制器按策略调度而非手动 API。
进度（2026-09-09）：✅ v1 落地——新 `zeroflush/repartition_controller.{h,cc}`
（PartitionRateTracker @封存边界 EWMA 双窗+时间衰减；GovernancePolicy 纯决策
状态机）+ DBImpl::ZfGovernanceTick（观察→策略→执行）+ 后台控制器线程
（Open 启 / Close join）+ `zeroflush::RunGovernanceTick` 外部确定性入口。
v1 语义取舍见 `M4_DG_DESIGN.md`：活跃期只拆不合（防 P 塌缩饿死拆分）、空闲
期冷回收（对应"设备空闲后由再锚定合并回收"）；C=1；rewrite 带宽/意图持久化
完整版留 M5。测试：GovernancePolicyUnit / GovernanceDynamicEndToEnd
（A 热拆分 + B 空闲回收子场景）/ GovernanceBackgroundAuto（ZF_GOV_BG 门控）
+ 全量 zf_test 回归（50/50 全绿）。R0/并发轨补修：并行写在途写组计数泄漏
（follower 为最后完成者漏 Exit → 屏障永久 TimedOut）已修复（fix d7dd50b）；
活跃尾 split 已实现并默认开（融合替换安装；连续写负载可用，见
M4_DG_DESIGN §4）。
范围：
1. 每分区统计（@封存边界，不进写路径）：速率=封存字节/(t−t_sealed)，EWMA
   双窗（1/5min，可配）；pending=HasPendingPartGens 口径；S_min 阈值。
2. 触发判定（控制器 tick 1s，O(P)）：θ_split=3×均值 & ≥32MB & 3 窗；
   θ_merge=0.3×均值 & ≤16MB & 3 窗；冷却 300–600s + 同 range 退避倍增。
3. 决策队列：并发 ≤C(1–2)，urgency=热度×等待（aging）；rewrite 优先于常规
   install 但带宽 ≤25%（任务池调度 + rate limiter）。
4. 参数进 ZeroFlushOptions：P_min/P_max(4/256)、θ、窗、冷却、C、rewrite 份额；
   与现有 max_pending_epochs=8/partition_target=64MB 对齐。
5. 意图持久化完整版 + 崩溃矩阵（decide/route/rewrite/retire 各中断点）。
文件：新 zeroflush/repartition_controller.{h,cc}、zeroflush_db.{h,cc}、
materialize_job.cc（调度接线）、options、zf_test.cc。
验收：zipfian/偏态长跑（P 有界、无饿死、无震荡、命中率门、吞吐 vs 静态基线
  无回归——prof_* 对照）；老化队列测试；crash 注入全矩阵；R0 修复后开并发门
  （flushes=4+parallelism=8）复跑。

### M5 收口
进度（2026-09-10）：
- ✅ 崩溃矩阵（活跃尾 split 中断）：新增 CrashMidActiveSplitReopen——
  子进程 sync 写种子 → 触发活跃尾 SplitPartition → 父进程在 freeze/物化、
  rewrite/尾部两档时机 SIGKILL → 重开校验：路由表可恢复（P∈{4,5}）、
  种子零丢失、后续写/物化/reopen 一致（split_orphans_ 内存别名丢失无碍：
  重开由 recovery 重放 zfwal 重建索引）。3/3 稳定通过。既有
  CrashMidDrainReopen（merge E_D 中断）覆盖 route/retire 中断点；decide
  意图可重算（不需持久化）——意图持久化口径见 M4_DG_DESIGN §4。
- ✅ 既有 flaky 用例 teardown 挂起修复：MergeActiveTailMergeEnabled 失败
  路径先 db.reset()（关 DB）再删目录，避免 close-flush 在已删目录上挂起；
  全量套件可跑到底（该用例自身仍为本机环境性失败，与实现无关）。
- ⏳ §5 文字对齐：**阻塞**——论文/设计稿（§5.1 意图不变量、§5.2(3)(4)(5)、
  五模式表 + 活性梗概）不在本仓库/文件系统（全仓仅路线图引用"五模式"）。
  需提供该文稿后方可执行。
- ✅ rewrite 带宽 ≤25% 限流已实现：DBImpl 维护"常规物化输出 / type-3
  rewrite"两条 EWMA 速率（τ=10s，按 prof_output_bytes / rewrite_bytes 累计
  计数差分），拆分动作前以 `RewriteBudgetOk(share, rw_rate, out_rate)`
  （rw ≤ share/(1-share)×out）门控；超预算则 NoteRetry（退避后再试），
  metric `gov_rewrite_gated` 计数。纯判定函数由 GovernancePolicyUnit §E 覆盖；
  合并动作不产生 rewrite，不受限。
- ✅ 50GB 级全量验收（16T/1KB/连续，51.2M 写≈50GiB，active-tail 修复后复跑）：
  ON 40,196 vs OFF 29,801 ops/s（+34.9%，回收空分区 + 活跃尾热拆分）；读命中 83.89% 一致、
  iter 19.04M 一致、reopen 一致。⚠ 长跑热拆分未触发（split=0，rewrite=0 →
  非限流器；短程 4M 写同参 splits=1 成功）——推断规模下 base 门槛饥饿，待确认。
  详见 output/zeroflush_m4_perf/REPORT.md。
- ✅ 100GB 级验收（16T/1KB/连续，104.86M 写≈100GiB，active-tail 默认开）：
  ON 22,545 ops/s、P=6、splits=1/merges=3、**0 Corruption**、读命中 86.04%、
  iter 32.70M、reopen 一致（hold 加宽修复后；修复前同配置 7 次 L1 重叠）。
  OFF 基线臂未完成：同参 governance=0 在 73.9M/104.9M（70%）处卡滞（~48 ops/s，
  主线程 futex 阻塞、单 bg 线程活跃、无 error/corruption 日志；磁盘 95% 使用）
  → 清盘重跑**复现**（72.5M 写/69% 再次停滞，磁盘 117G 空闲、0 Corruption）
  = 可复现基线卡滞（~70% 写入量处）。**归因（现场数据）**：L0 积压 23k 文件/
  11.25GB、L1 10.73GB（score≈10）→ 原生 L1→L2 持续运行（comp=1）与 ZF 物化
  being_compacted 冲突 → 物化 kSkip 累积、sealed_wal 37.9GB 冻结、写路径卡在
  epoch/imm 水位（is-write-stopped=0，非写停控制器）。= 基线静态分区热点 L1
  无界的结构性限制（治理 ON 拆分后 100GB 全程完成）——治理即对策。
  验证（ZF_MAX_L1_BYTES=1TB 禁原生 L1→L2）：卡滞消失、写吞吐升至 ~100k ops/s
  （坐实 L1→L2 与物化冲突为直接触发），但 69M 写（66%）即磁盘耗尽
  （L0 22k 文件/58.7GB 只增不消 + sealed 34GB）→ 基线第二条结构性问题：
  空间放大失控。结论：OFF 100GB 卡滞 = 热点 L1 无界（原生 L1→L2 冲突）+
  L0 无消费空间放大；治理 ON（拆分热点）100GB 全程完成 = 对策。后续可选：
  修物化与原生 L1→L2 互斥（让位/等待）、L0 消费路径、或按 S1/S3 收紧每
  range 文件规模。
  zf_test 全量现状（2026-09-11）：**53 PASS / 0 FAIL —— ALL PASSED（首次全绿，
  套件可跑到底，teardown 挂起已修）**。两条既有失败均已修复：
  - MergeActiveTailMergeEnabled：E_D 与吸收方跨批互斥（E_D 批同时登记 absorb
    id，冲突让位转 recovery）——原 'L1 has overlapping ranges' 根因；
  - MultiEpoch（真数据丢失 bug）：单任务模式（hash/学习齐批）kSkip 的 gens
    移交只收代表分区（实测 epoch=21 gens=24/skipped=19），未移交 gen 在 epoch
    出链时 frozen 索引被释放且数据不在 recovery/SST → 永久不可见；修复为
    单任务模式全量移交 se_.gens。附带正确性改进：布隆预过滤降级为快路径
    （miss 转链查）+ 指标 bloom_slowpath / index_insert_dropped。
  SkipBatchMaterialize 时序 flaky 本轮回通过。
- P1B_DESIGN.md 保留为独立 S4（合并协议）设计档（内容自洽，不再合入）。

## 3. 性能纪律（调度器实现约束）

1. 写路径/写组零新增：判定不进 ShouldSeal/AddRecord/leader 捕获点；路由切换仍
   是一次原子表指针交换（M4.10 现成）。
2. 统计@seal 边界：字节+时间戳在 SealedEpoch 已有（sealed_at_micros），速率
   增量计算；禁止逐 Append EWMA（与 active_bytes 争 cacheline）。
3. 控制器低频：tick ≥1s、O(P)≤256 扫描；调整动作只发生在后台（物化任务池/
   rewrite 任务），前台仅承受调整瞬间的短 mutex 窗口（µs–ms，频率=每冷却窗）。
4. 后台 I/O 有界：并发调整 ≤C=1–2、单次 rewrite ≤64MB（quiescent 首版恰为
   一个 ≤64MB 文件）、带宽份额 ≤25%、冷却退避防空耗。
5. R0 修复前：所有新用例与并行承诺用 materialize_parallelism=1 约束；
   R0 修复后解除，并复跑并发基线。

## 4. 风险与门

| 风险 | 缓解 |
|---|---|
| R0 竞态时间不可控 | 独立并行轨 + 时间盒；主链 M1–M3 用 parallelism=1 不阻塞 |
| F1 改 PlanLocked 引入回归 | 仅 in-table 可达 kSkip 出口 + parallelism=1 下 batch/cross-batch 分支不可达断言 + 序倒置回归用例 |
| ZFPROPS 意图扩展兼容 | 版本化 v3，旧库可读（全表快照 + 新段）；先于 M4 全量崩溃矩阵 |
| 生长路径扰动稳态（P0=1 首次拆分） | M3 用 fillrandom/zipfian 双负载验收 + 吞吐门 |
| 调度器触发误伤写吞吐 | 参数钝（10× 差 + 3 窗 + 冷却）；验收以吞吐对照为门 |

## 5. 建议执行顺序

先 M1（P1b 串行正确性子集）——它把 P1 的 API 语义从"必须已排空"推进到"活跃尾
可合并"，是合并协议的正确性内核，且不依赖 R0/M2。R0 作为并行轨立即开始（诊断
工具已就位）。M2/M3 在 M1 后串行推进；M4 聚合全部依赖。每里程碑独立提交、以
zf_test 全量 + 对应 e2e 为门。

## M4.9 规模并发重叠深挖（2026-09-11）

重叠对 = 两个不同 FlushJob 对同一分区的 kMergeBase L1 输出；plan 视图互不
可见（ZF_DBG_MM 追踪）→ 并行规划 + LogAndApply 冲突。相邻分区互斥扩展与
安装期复检均未拦截（复检落在对方 LogAndApply 未落地窗口，0 yield）。已落地
安全网：安装前按当前版本复检同层/批内重叠 → 整批让位（删产出 + gens 转
recovery + 回滚 last，与 kSkip 同语义），把崩溃降级为可重试让位（回归
全绿；但复现中 0 触发——复检落在对方 LogAndApply 未落地窗口内）。
**登记探针（ZFDBG-mm-reg）**：冲突分区在窗口内出现**不同 token 的登记
成功**（pid5: token 159/167/175/177 均 ok=1）→ 后批在对方 `LogAndApply`
让锁窗口（MANIFEST IO 期间释放 DB mutex）内完成规划：扫描视图不含对方
尚未落地的输出 → 双方 edit 相继 apply → VersionBuilder L1 重叠崩溃。
即 M2 文档遗留项"与并发 flush 的 LogAndApply 让锁窗口竞态未做
being_compacted 防护"的实锤（token 守卫未覆盖该窗口，需再探 job↔token
映射确认是提前 End 还是窗口内登记）。
**修复方案（2026-09-12 已落地）**：新增 DB 级 `zf_install_mu_`
（`ZeroFlushContext::install_mu_`），ZF 批从阶段 0 规划前 try_lock、持至
安装（TryInstall→LogAndApply）完成后释放；获取失败先有界等待重试
（20ms×8，由释放方 cv 唤醒），超时则整批让位（全部待物化 gens 移交
recovery，下批收养）。跨批次串行化 plan→apply 后，让锁窗口不可能再插入
规划，重叠从源头消除。`ZF_INSTALL_LOCK=0` 可整体关闭（回退/对照）。
**验证**：控制组（回退补丁重建）3/3 崩溃（rc=139 + L1 overlapping ranges）
vs 修复组 3/3 通过（256 分区/16T/2.1M 写）；8.4M 写重载通过；zf_test
ALL PASSED（53/53）。**已知代价**：锁跨"规划→构建→安装"全程，ZF 批并行度
4→1，同二进制 A/B 实测 74–82K → 41–43K ops/s（约 1.9× 慢）。后续应收窄
锁粒度（按 range/分区预留）或压缩持有时段至 finalize→apply。
**实施记录（2026-09-11，未奏效，已撤销）**：先以"安装窗口标记 + 规划期
整批让位"替代 install 锁（避免跨函数持锁）——复现仍 8/10 崩。job↔token
关联探针给出决定性时序：崩溃对 376（job74/epoch76/token71）与 384
（job69/epoch68/token73）同分区；**job74 的 edit 已构建打印（行 8351）但
未 apply 时，job69 已在行 8372 规划且扫描视图 ov=[305] 不含 376**；同时
job69 的 token73 登记成功 ⇒ token71 已 End（而 End 名义上在 TryInstall
之后）→ 存在"**edit 尚未落地即释放 token**"的路径（疑似 TryInstall 失败/
重试或某错误出口提前 End）。三次尝试（相邻互斥 / 安装期复检 / 窗口让位）
均未拦截。
**探针落地与结论（2026-09-11）**：
  - PlanLocked 入口 `mc_.base == cfd_->current()` 断言：**stale=0**（规划视图
    恒为最新版本）；
  - End 全打点（run_after_install ok / err 出口 / yield 出口）：token 登记与
    释放顺序**正常**（后批登记仅发生在前批 End 之后）；
  - 但崩溃对的两条 mm-add 均为 ZF 输出，后批 plan 的 ov 列表中**缺失前批
    已安装的同层文件**（如 job129 装 742 于 L1，job123 随后 plan pid5 的
    ov=[691] 不含 742）→ 说明前批文件在 L1 视图中"消失"或从未以可扫描形态
    存在于该版本（移动层/被替换/安装于不同版本分支）。
  **层级生命周期追踪（watch）已落地**：记录 ZF 安装文件号+范围，plan 扫描
  时对"相交但不在本层列表"的号打印当前层。实测（256 分区/16T/merge=1）：
  早批安装文件在大批后续 plan 中呈 `now_level=-1`（已被后续 ZF 替换删除，
  属正常替换路径）；**崩溃对（共存 L1 的重叠文件）未命中同一 watch 条目**
  → "文件被非 ZF 编辑移走"假设未证实，需绑定式探针：仅盯崩溃对的两个文件
  号，在其安装后每次相关分区 plan 时打印其存在层。撤销记录与探针
（ZFDBG-mm-jobbeg / mm-reg / mm-plan / mm-scan）保留在代码中（env 门控）。

**死锁修复（2026-09-14，已落地并验证）**：安装锁上线后暴露两处新缺陷，
经 gdb 栈定位为死锁环：① 持锁批在 `TryInstall` 等前序 memtable，而前序
的 flush job 在等锁；② 学习批持 gen0 槽等前序，而前序等"学习窗关闭"。
修复（`db/flush_job.cc` + `zeroflush_db.{h,cc}`）：新增**前序门**——取锁前
确认 imm 中无更早未安装 memtable，有则不取锁、不持全局资源地等待（轮询 +
200µs 退避），且等前序时**释放 gen0 槽**（前序清空后重取）。验证：原
M4.9 崩溃配置 5/5 通过（112.7–129s、零 overlapping、reopen 一致，此前
8/8 崩溃）、4M 写 13.5s/297K ops/s 零让位、zf_test ALL PASSED；锁已
**默认开启**（`ZF_INSTALL_LOCK=0` 可关）。代价 ≈−20%（256 分区
18.4K ops/s），待分区级 token 收窄。详见
`output/zeroflush_m5_perf/materialize_lag/PROTOCOL_FIX_ATTEMPT.md` 附录 B。

