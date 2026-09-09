# ZeroFlushV0 P1B 设计：带在途数据的 DRAINING merge（S4 完整版）

状态：设计档（2026-09-09）。P1（6667f9d）已交付"无在途数据"的冷分区合并；
本档定义把 merge 前置检查从"drain 必须已完全排空"放宽为"可冻结 + 异步排空"
所需的全部机制。分支 ZeroFlushV0。

## 1. 目标与范围

MergePartition(drain D) 允许 D 仍有未物化数据（活跃 WAL、frozen 索引、
pending sealed epochs、skip/recovery gens）。协议（§5.1 S4）：
切路由（新写即时归吸收方 A）→ D 进入 DRAINING（拒新写）→ 残余数据经既有
物化流水逐个安装 → 无 pending 后 RETIRED。

P1 已实现的路由切换机制（写组屏障、DeriveMerge、InstallNewVersion、
ZFPROPS）原样复用。P1b 增量 = 冻结 + 排空窗口读 + 防序倒置 + retire。

## 2. 冻结（merge 临界区内，屏障之后）

在切路由的同一个 DB-mutex 窗口中，先执行一次"单分区封存"（与
FreezeBatchPartitions 同构、只含 D）：
- `wal_->Freeze(D)` → 新 sealed gen (D, g)；`index_set_->Freeze(D, g+1)`；
- 新 epoch E_D，`se.table_version = 当前版本 V`（D 仍在 V 中 → E_D 的物化
  按 D 的原区间干净断言，不会走孤儿全范围路径）；
- `AddEpochWithRecoveryAdoption(E_D)`——顺带收养 D（及任何分区）既有的
  skip/recovery gens（它们并入 E_D 一次排空）；
- `SetZfEpoch(mem)` + `ZfSwitchMemtable`（imm 入列，flush 调度）；
- 然后 InstallNewVersion(V+1 无 D) + PersistZfProps + 登记 DRAINING。

关键性质：E_D 的 seal 时刻先于任何 A 的后 merge epoch（写组路由已切），
imm FIFO 保证 E_D 的 flush 先于 A 的首个 epoch——**只要 E_D 的 D 分区被
物化（不 skip），D 的尾数据在 A 的任何数据进 L1 之前已落地**，序不倒置。

## 3. 序倒置根因（为什么"别名读"单独不够）

DRAINING 窗口内读 key k（D 原区间）：路由到 A → A 的索引链（活跃+frozen）
miss → **别名回查 D 的索引链**（其 frozen/active 仍持有 D 的 pending 版本）。

层序读假设"索引窗口恒比 L1 新"。merge 打破它当且仅当：D 的旧版本仍在
pending（索引），而 A 对同一 k 的更新版本已物化进 L1。此时别名命中（旧值）
先于原生 L1（新值）返回 → 读旧值。

该窗口的充要条件是 **D 的 pending 存活晚于 A 的首次物化**。E_D 先于 A 的
epoch（FIFO）已排除"正常路径"；漏洞只剩 kSkip：
- E_D 的 D 分区在物化决策中被 kSkip → gens 移交 recovery/skip → 被下一个
  seal 收养——下一个 seal 很可能是 A 的 E_A1 → E_A1 的 flush 中 D 以
  "不在当前表"的孤儿身份被规划（R59C 全范围 lo/hi）→ 若 D 再次 skip 而 A
  物化 → 序倒置成立。
- 多 flush 并行（4 路）下 E_D 与 A 的 epoch 可并发 flush（batch 级登记只
  串行化同分区），同一漏洞的并发变体。

⇒ 充分条件（P1b 的核心）：
**(F1) 自 merge 起，任何 epoch 的 gens 中出现 DRAINING 分区 D 时，D 的
partition plan 一律物化（kSkip → 强制落地），直到 D 排空。**
F1 排除"E_D 被 skip"与"A 的 epoch 收养 D 后 D 再被 skip"两条路径；配合
FIFO（E_D 先于 A 首 epoch）→ 序倒置不可达。别名读随之安全：
- A 链命中 → A 版本更新（post-merge seq > M ≥ D 版本 seq）；
- A 链 miss → 该 key 无 post-merge 写 → D pending 即最新 → 别名正确。

## 4. 实现 F1：物化决策的收口点

materialize_job.cc `PlanLocked()` 中 kSkip 决策分散在十余处
（gen0 滞留 / batch_planned / cross-batch busy / merge-disabled 攒批 /
主路径小批 skip / sink 窗口 skip…），逐点打补丁不可维护。收口方案：

1. `SealedEpoch` 增字段 `bool force_materialize_parts`（或携带集合）：
   E_D 由 merge 封存时置位；AddEpochWithRecoveryAdoption 收养 skip/recovery
   gens 时把"DRAINING 分区"并入 force 集合（需要 ctx 侧查
   IsDraining(part)）。也可简化：force 集合 = 该 epoch 中所有
   ctx->IsDraining(p) 的 p（PlanLocked 内直接判定，不落 epoch 字段）。
2. `ZeroFlushContext::IsDraining(part)`：DRAINING 注册表（draining_mu_ +
   vector，P1 已删，P1b 恢复）的只读查询。
3. `PlanLocked()` 主循环开头为每个 pid 算 `force = ctx_->IsDraining(pid)`；
   各 skip 分支改为 `if (skip_ok && !force) {kSkip} else {kFallback 或强制}`。
   需要逐点核对 kFallback 的语义：kFallback = 单文件 + PickInstallLevel，
   可能落 L0——数据必达（读优先），由后续融合吸收，符合"强制落地"。
   对 batch_planned/cross-batch 这类"同批他 epoch/他批已规划 D"的让位型
   skip，force 语义应为"不 skip 也不重复产出"——它们依赖前序产出者已覆盖
   D 的**该批 gens**；E_D 若与 A 的 epoch 同批，batch 内按 epoch 序规划，
   D 出现在 E_D（更早 epoch）→ 先规划产出（force）→ A 的 epoch 因
   batch_planned 登记正常 skip（A 不是 DRAINING，skip 无碍；且其 D 关联
   数据已被 E_D 覆盖）。核对 batch_planned_parts 的登记语义与 D 跨批让位
   （cross-batch）在 F1 下是否可安全放宽为等待（AcquireGen0Slot 类机制）。
4. 多 flush 并行下保证"E_D 的 flush 先于 A 的首 epoch 完成"——若 F1 使得
   D 永不延后，A 的数据（只可能更新 k）不可能先于 D 进 L1（A 的首 epoch
   若与 E_D 并发 flush，二者输出安装点独立——A 装 L1 而 E_D 未装仍可能
   …）。为此需附加：**A 的首个 post-merge epoch（或其物化）在 D 排空前
   不产出覆盖 D 区间的新文件**？——进一步核对：A 的 E_A1 gens 若与 E_D
   并发 flush（不同 flush 线程各取 imm），E_A1 先安装 → A 的新 k（若有）
   进 L1 而 D 的旧 k 仍在 E_D pending → 序倒置！FIFO 只在单 flush 线程
   （imm 串行消费）成立；M5P1b 已放开 4 路并行（kSampled/kStatic）。
   解法候选：
   (a) DRAINING 期间把 A 纳入"等 D 排空的让位"（batch 级冲突登记的扩展
       版：D 排空前 A 的产出分区若与 D 区间相交则让位）——粒度粗；
   (b) 让 E_D 走 gen0 学习窗串行槽（g0_* 已有"单任务批串行"基础设施）：
       E_D 标记为串行批，flush 时与并发批互斥，保证 E_D 先于其后的 A 批
       完成安装（在 E_D 完成前 A 的 epoch 不被 flush 或让位）；
   (c) 关闭场景简化：P1b v1 用 max_background_flushes=1 验证正确性
       （FIFO 成立 → 只需 F1），4 路并行下加 (b)。
   推荐先 (c)+(F1) 落地正确性，再以 (b) 解除串行限制。

## 5. 排空窗口读（别名）

恢复 P1 曾设计后移除的 `GetFromPartitionIndex` 别名回查：候选链 = 路由分区
+ 覆盖该 key 的 DRAINING 分区（draining 注册表存 D 原区间 [lo,hi) 与吸收
方 A）。约束：仅在 A 链 miss 后回查（A 命中即最新）。迭代器无需改
（AddIterators 已含全部分区索引）。布隆不受影响（D 的 bloom 独立存在）。

## 6. Retire

`MaybeRetireDraining()`（P1 曾设计）：DRAINING 分区满足"无 pending WAL 段
（SealedFileCache::HasPendingPartGens==false）+ 无 frozen 索引
（PartitionIndexSet::HasFrozen==false）+ 活跃字节 0"→ 移除登记、
merge_retired++。调用点：ReleaseEpoch / ReleaseGens（epoch 回收 = 物化完成
推进点）；skip/recovery 中转由 F1 排除（D 不再被 skip），故只需 epoch 回收
钩子。D 的活跃代在 merge 时已冻结（E_D）且无新写 → 活跃字节恒 0。

## 7. 崩溃与恢复（P5 之前的行为）

- ZFPROPS 已持久化新路由表（V+1）→ 重开路由无 D，正确。
- D 的 E_D/pending 封存 WAL：Recover 重放所有分区 WAL 文件重建索引
  （InsertCreate 按 gen）→ D 的 sealed gens 以 recovery 孤儿登记
  （AddRecoveryGens）→ 下个 seal 收养物化（孤儿路径，R59C 容忍）。
- DRAINING 注册表是内存态：崩溃后丢失 → 别名查询失效窗口 = 孤儿被收养
  物化之前（期间 D pending 数据不可见）。P5 恢复协议把 DRAINING 状态与
  收养关系落盘（ZFPROPS/MANIFEST 扩展）后关闭该窗口。

## 8. 测试计划（P1b 验收）

- e2e：写 D 区间数据（部分已 flush、部分活跃/frozen）→ merge → 立即读
  D 旧 key（别名路径，含快照读旧版）→ 覆写 D key（归 A）→ 排空完成后
  retire（merge_retired==1）→ reopen 一致。
- 序倒置回归：D 有小尾数据（会触发 skip 的体积）merge，A 随后覆写同一
  key 并多次 flush；任意时刻 Get 不得返回 D 的旧值（F1 保证）。该用例在
  max_background_flushes=1 与 4 路下各跑。
- crash 注入：merge 后、E_D flush 前 kill → reopen 数据完整（§7 语义）。
- 全量 zf_test 回归 + 命中率门。

## 9. 加固中发现的问题（2026-09-09，ZeroFlushV0）

1. **kStatic 首封存 gen0 被误判为"学习齐批"**（已修）：`PlanLocked` 的
   gen0 学习机制只对"数据先于分区表写入"的模式（kSampled 首 epoch hash /
   kAlignL1 对齐前）有意义；kStatic 边界从 gen0 起即权威，首批含全分区
   gen0 时被误判为学习批 → 单任务整表切片或滞留等齐批。修复：`gen0_learning`
   增加 routing_mode ∈ {kSampled, kAlignL1} 门控。此前 kStatic 仅在 merge
   测试中出现，属潜在 bug。
2. **在途写组计数的双 Exit**（已修）：!parallel 组的 leader 会额外经过
   WriteImpl 共享尾部的 `should_exit_batch_group` 块——原实现在该块统一
   Exit，!parallel 每组长双 Exit → 计数 underflow 断言（MergeEdges 实测）。
   修复：!parallel 组在 `WriteGroupToPartitionWal` 返回处 Exit；并行组保留
   共享尾部 Exit（仅 `in_parallel_group && should_exit`，最后完成者恰好一次）。
3. **间歇性 "Compaction sees out-of-order keys"（未修，独立追踪）**：
   4 分区并行**合并**物化（kMergeBase，B 侧有 L1）场景下偶发——同批次大部
   分成功、个别乱序；复现：kStatic P=4 + 4KB 目标 + 四分区交替写入产生
   4 分区整批封存、连续多批 4 任务并行合并（MergeEdgesAndCascadeEndToEnd
   的填充模式，该用例已因触发此 bug 暂移除）。疑似并行分区任务的共享状态
   竞态（overlap 选择 / Compaction 注册 / 批次内链式替换）。与 merge 机制
   正交，需独立定位（建议先用 gtest 式最小复现 + TSAN）。

