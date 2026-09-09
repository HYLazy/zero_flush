# ZeroFlushV0 M4 动态治理规模验收（首轮，2026-09-09）

驱动：`source/rocksdb-zeroflush/tools/zf_govern_bench`（新增）。协议 = 偏态
hot-band 覆盖写 + 突发/排空间隙轮次（治理在 quiescent 检查点执行）+ P 轨迹
采样 + 收尾读验证/reopen。

## 参数与规模

| 项 | 值 |
|---|---|
| key 空间 | 4M（"k%012d"）；hot band [0, 480k) 占 85% 写（初始在分区 0）|
| 写 | 10 轮 × 10,000 = 100k 次，值 512B，单写线程 |
| 治理 | kStatic P0=8；split_min=4MB；θ 默认；tick 20ms；冷却 ~0（快测）|
| 规模 | 有效数据 ≈ 92,834 键 ≈ 47MB（本轮小规模验证，非 50GB）|

## 结果对比（同参 governance OFF / ON）

| 指标 | 静态基线 (OFF) | 动态治理 (ON) | 差 |
|---|---|---|---|
| 写吞吐 ops/s | 2,434.8 | 2,436.6 | +0.07%（噪声内，无回归）|
| 写墙钟 s | 41.07 | 41.04 | — |
| 终态 P | 8 | 6 | 治理在 [P_min=4, P_max=256] 内 |
| gov_splits / gov_merges | 0 / 0 | 1 / 3 | 治理动作已执行 |
| rewrite_bytes | 0 | 23.8MB | 1 次热拆分 type-3 |
| 读命中 found/200k | 0.1382 | 0.1382 | 相等（无数据丢失）|
| iter_entries | 92,834 | 92,834 | 相等（reopen 一致）|
| inflight_groups（写停后） | 0 | 0 | 无泄漏（单线程）|

P 轨迹（ON）：8 →（空闲期空分区合并 3 次）5 →（热拆分）6 → 稳定；
分裂后两子分区各≈0.5×热负载，ratio≈P/2≈3 → 达 θ 平衡不再拆（数值符合
设计：热拆把 H=1→2 时单分区 rate/mean 从 P 落到 P/2）。

## 结论

1. **吞吐无回归**（+0.07%，同参对照）；数据完整（读命中/迭代数/reopen 与
   基线逐位一致）。
2. **P 有界**：治理动作把 P 从 8 收敛到 6 ∈ [4,256]，无越界、无失控。
3. 单写线程下 split/merge 均正常执行（本轮分别 1/3 次）。

## 阻塞全规模（多线程长跑）验收的两个问题（本轮实测定位）

1. **并行写路径在途写组计数泄漏**（新定位，疑既有 bug）：
   T=1 写后 inflight_groups 恒 0；**T≥2 并发写时计数单调滞留**（探针：2 线程
   6000×1 轮后 inflight=543 不回零）。merge/split 的路由切换屏障等待该计数
   归零 → **并发写负载下所有 merge/split 一律 `Operation timed out:
   inflight writer groups did not drain`**（实测 split/merge 均如此）。这直接
   阻塞 M4/merge 的"多线程 + 治理"承诺（路线图验收 flushes=4+parallelism=8）。
   根因方向：db_impl_write.cc parallel 分支的组级 Exit 埋点（应恰好一次/组）
   在并发下未覆盖全部出口。**建议单开修复**（连同 M4 e2e 用多写线程复跑）。

2. **活跃尾 split 未实现（M2 遗留，路线图明确"并入 M4"）**：v1 split 仅
   quiescent（须完全排空）；持续写负载下分区无 quiescent 瞬间 → split 只能
   在排空检查点（突发间隙）执行。本轮验收因此用"突发+间隙"协议而非连续
   fillrandom。要做连续负载热拆分需"活跃尾 split"（冻结尾 epoch + 跨界 WAL
   gens 按新边界切分）。

## 后续

- 修复（1）后以多写线程复跑本驱动（threads≥4）+ 更大规模（数 GB）确认
  P 收敛轨迹与吞吐对照稳定；
- （2）作为 M4 后续/收口项评估（活跃尾 split）。

原始日志：`gov_acc0.log`（OFF）、`gov_acc1.log`（ON）。
