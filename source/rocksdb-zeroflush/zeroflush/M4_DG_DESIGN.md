# ZeroFlushV0 M4 动态治理 DG 设计/状态档（v1）

状态：v1 已落地并 e2e 闭合（2026-09-09）。依据 ZEROFLUSHV0_ROADMAP.md
§2 M4；M4_DESIGN.md 为旧架构档，与修订版编号无关（本文件是新 M4 的设计档）。

## 0. 目标与总览

治理层成型：merge(P1b)/split(M2) 由控制器按策略调度而非手动 API。v1 落地：

1. **统计 @封存边界**（不进写路径）：每分区写入速率 EWMA 双窗
   （快 1min / 慢 5min，可配），采样点 = 分区 WAL 换代（epoch 登记）的
   封存字节 + 时刻；读取含时间衰减（空闲速率自然趋零）。
2. **触发判定**（控制器 tick，默认 1s，O(P)≤256）：热拆分 θ_split=3×均值
   & base ≥ S_min(32MB) & N 连续窗（默认 3）；冷合并 θ_merge=0.3×均值 &
   base ≤ 16MB & N 窗；冷却（默认 300s 基础，同 range 退避倍增封顶 600s）。
3. **决策队列**：Busy/TimedOut 意图保留为 pending，urgency=热度×等待
   （aging），重试退避调度（默认 1s 起、倍增封顶 30s，防活跃期空转/冻结
   风暴）；并发 ≤ C（v1 每 tick 至多 1 动作）。
4. **参数化**：全部入 ZeroFlushOptions（governance_*，P_min/P_max、
   θ、大小门槛、窗数、冷却、C、EWMA 窗、空闲期合并判定窗、rewrite 份额
   预留）。
5. **崩溃一致性**：复用既有持久化（ZFPROPS 全表版本化路由 + DRAINING
   sidecar）：路由切换原子、split 单点 VersionEdit，决策意图可重算（不需
   持久化）；崩溃矩阵/全量文字对齐按路线图归 M5 收口。

## 1. 关键语义决策（v1 设计取舍）

### 1.1 拆分-合并的"活跃期/空闲期"分工
- **活跃期只拆不合**：存在任一"速率>0 且封存样本新近"的分区时抑制合并。
  原因（实现期实测）：(a) 冷合并把 P 降到热分区 rate 不再 ≥θ_split×mean
  （1 热+1 冷 = 2×mean < 3×mean）→ 杀死后续拆分；(b) C=1 单动作/tick 下
  merge 抢占热拆分确认窗 → 拆分饿死（e2e 首轮实测 splits=1/0）。
- **空闲期回收**：负载回落后（无新近封存）放行冷合并，把 P 收到 P_min。
  对应原设计"设备空闲后由再锚定合并回收"。判定窗默认 120s（2×快窗）。
- 这使 merge 触发除"冷"（θ_merge 与 base ≤16MB）外还要求"空闲"；单元与
  e2e 均按此口径。

### 1.2 分切点
复用 M3 生长口径：base 文件条目中位数；若中位数等于既有分区边界
（DeriveSplit 拒绝），向后取第一个严格在 (lo,hi) 内的键。

### 1.3 子分区继承
split 成功后子分区继承父分区速率（双窗拷贝、末次采样时刻置为切分时刻），
父退避级 +1 下发子区间（冷却期内防止子分区被当"零速率"立即误合并/重拆）。

## 2. 代码落地清单

| 文件 | 内容 |
|---|---|
| `zeroflush/repartition_controller.{h,cc}`（新） | `PartitionRateTracker`
  （@seal EWMA+衰减）；`GovernancePolicy`（连续窗/冷却退避/P 界/pending
  aging/活跃期合并抑制/拆分保护，纯逻辑可单测） |
| `zeroflush/zeroflush_db.{h,cc}` | ZeroFlushOptions governance_* 参数；
  `RegisterSealedEpoch`（全部 epoch 创建点收敛：SealEpochAndSwitch/
  FreezeBatch/FreezeOne/E_D/排空冻结）；`HasPendingPartData`/`DrainingSnapshot`
  访问器；metrics：rewrite_bytes/gov_ticks/gov_splits/gov_merges；
  `zeroflush::RunGovernanceTick(db)` 外部入口 |
| `db/db_impl/db_impl.{h}` + `db_impl_write.cc` | DBImpl::ZfGovernanceTick
  （观察→策略→执行 merge/split：排空活跃尾+等物化+中位数切分）；
  ZfStartRepartitionController/ZfStopRepartitionController（后台线程，
  Open 启、Close/析构 join）；base 字节 TTL 缓存；ZF_DBG_GOV 诊断 |
| `db/db_impl/db_impl.cc` | Close() 与 ~DBImpl 在关闭冲刷前停止控制器 |
| `CMakeLists.txt` | 登记新源文件 |
| `tools/zf_test.cc` | GovernancePolicyUnit / GovernanceDynamicEndToEnd
  （A 热拆分子场景 + B 空闲回收子场景）/ GovernanceBackgroundAuto
  （ZF_GOV_BG 门控 smoke） |

## 3. 测试与验收

- GovernancePolicyUnit（纯策略，A-D）：连续窗确认触发热 split / 均匀负载
  零动作 / 空闲冷合并与 P 界 / Busy 退避调度与 aging。
- GovernanceDynamicEndToEnd：A) 偏态单调写 + 排空检查点 tick → gov_splits
  ≥2、P 界、数据全对、reopen 一致；B) 全空库空闲回收 → merge 收到 P_min。
- GovernanceBackgroundAuto（ZF_GOV_BG=1）：真实后台线程启停/Close join/
  reopen 一致 smoke。
- 全量 zf_test 回归通过（默认跑 bg 用例 skip）。

## 4. v1 限制与后续（M5）

- 决策意图不跨崩溃恢复（可重算——路由/排空一致性已由 ZFPROPS+sidecar 保证）；
  路线图 M4.5 的"完整意图持久化 + 全崩溃矩阵 + §5 文字对齐"由 M5 收口。
- rewrite 带宽份额 ≤25%：参数已预留（governance_rewrite_share）；v1 因
  C=1 且 split 低频、天然受限，严格 rate-limiter（任务池优先/带宽账本）
  并入 M5。
- 控制器合并"空闲期"判定为 v1 语义（原路线图仅写 θ_merge；无冲突——本档
  1.1 记录取舍）。
- EWMA 双窗快/慢默认 1/5min；测试用小窗（快测）。
