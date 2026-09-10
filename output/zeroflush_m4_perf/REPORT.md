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

## 结论

1. **吞吐无回归**（单线程 +0.07%，同参对照）；数据完整（读命中/迭代数/
   reopen 与基线逐位一致）。
2. **P 有界**：治理把 P 从 8 收敛到 6 ∈ [4,256]，无越界、无失控。
3. 单线程与 4 线程下 split/merge 均在排空检查点正常执行。

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

## 仍未解决（阻塞连续负载全规模验收）

### 2. 活跃尾 split 未实现（M2 遗留，路线图"并入 M4"）

v1 split 仅 quiescent（须完全排空）；持续写负载下分区无 quiescent 瞬间 →
split 只能在排空检查点（突发间隙）执行，故本验收用"突发+间隙"协议而非连续
fillrandom。要做连续负载热拆分需"活跃尾 split"（冻结尾 epoch + 跨界 WAL
gens 按新边界切分）。

## 后续

- 因问题 1 已修，可以多写线程复跑更大规模（数 GB）确认 P 收敛轨迹与吞吐
  对照稳定；
- 问题 2 作为 M4 后续/收口项评估（活跃尾 split）。

原始日志：`gov_acc0.log`（OFF）、`gov_acc1.log`（ON 单线程）、
`gov_acc_mt.log`（ON 4 线程）。
