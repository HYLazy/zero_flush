# 启动期 L1/base 对齐方案研究（R59D 计划）

日期：2026-09-02 · 状态：研究结论

## 1. 启动期问题机制（实证澄清）

**关键事实 1：ZF 的直装层是 base_level（dynamic level 下动态），不是 L1。**
- 空库 base_level = 6（version_set.cc:5213，first_non_empty_level = max_level）；
- 数据增长后 base 下移（2.2GB sampled 实测 lsm_state [10,6,17]——直装 L1/L2）；
- **align_l1 的 BuildL1AlignedTable 读 LevelFiles(1)**——与 ZF 实际直装层（base_level）错位：表桶（L1 文件）≠ 直装目标（base 层文件）→ 对齐表与实际数据流脱节。

**关键事实 2：align_l1 已有 epoch 1 采样兜底**（FreezeBatchPartitions 668-677：L1 空且 epoch 1 且采样器非空 → 采样边界建表 T1）——但：
- epoch 1 封存时安装 T1 → epoch 1 物化（hash 写，se.table_version=0）经 M4.11 切片（slice_table = current = T1）→ 16 片直装 base → **启动期对齐本应建立**；
- 实测 2.2GB sampled fallback 248——回落仍大量发生（切片/直装后为何回落？见下）。

**关键事实 3：fallback 是漂移的唯一制造者**（L0 → 原生 L0→base compaction → 跨分区合并 → 漂移文件 → 越界 → 更多 fallback → 自增强）。2.2GB fallback 248 → L0 10 个 → 原生消费 → L1/L2 漂移文件（L1=6 可能部分是漂移产物）。

**启动期链条（当前 align_l1 的实际行为）**：
```
epoch 1：hash 写 → 封存时采样建 T1 → 物化切片直装 base（对齐）→ 但部分分区
        ratio/冲突回落 → L0 → 原生 L0→base 合并切片文件（跨分区）→ 漂移文件
epoch 2+：BuildL1AlignedTable 读 L1（漂移文件/空）→ 桶 = 漂移 → 偏斜循环
```

## 2. 三层方案（启动期对齐的完整闭环）

### 层 1：启动期建立对齐（本次研究核心）

**改 BuildL1AlignedTable → BuildBaseAlignedTable**：对齐对象从 L1 改为 **base_level 层**（ZF 实际直装层）：
- 空库（base 层空）→ 采样边界建表（现有兜底保留，条件从 L1 空改为 base 层空）；
- base 层有文件 → 按 base 层文件边界建表（桶 = base 层文件）→ 写入按桶路由 → 物化直装 base = 桶文件 1:1；
- 学习期（epoch 1）切片直装 base（M4.11 的 slice_table = current 已支持）。

**预期**：epoch 1 切片文件（T1 分区范围）直装 base → base 层 = 对齐文件 → epoch 2 表 = base 文件（1:1）→ 稳态无 fallback → 无漂移 → 无越界。

### 层 2：稳态保持（无 fallback 即自动对齐）

base 层对齐文件存在时：同分区新 epoch 融合 = overlap（前序对齐文件）1:1 → ratio = sealed/overlap ≈ 1（同量级）→ 达标 → 无 fallback → L0 空 → 原生 L0→base 不触发 → 无漂移。**sampled（表固定）在无 fallback 时 base 层自动对齐——动态表仅在漂移打破对齐后需要**。

### 层 3：漂移修复（fallback 打破对齐后）

漂移文件（跨分区）一旦产生：R59 越界融合维持其大范围（输出 = 文件范围，不收缩）→ 表跟随 = 大桶 → 偏斜。**根治需要融合输出按表边界切回**（R59B 对齐物化的 compaction 侧版本：融合时按分区边界切分输出，漂移文件 [a,c) 切回 [a,b)/[b,c)）。此为 R59B 未闭环的部分——层 1/2 建立后 fallback 大减，漂移罕见，层 3 的触发率低，可作为后续优化而非启动前提。

## 3. 实现路径（最小验证顺序）

1. **BuildBaseAlignedTable**：读 vstorage->base_level() 层文件（替代硬编码 L1）；空库采样兜底条件同步改 base 层空。~30 行改动；
2. **5GB align_l1 验证**：预期 fallback 248 → <50、merge 主导、lsm_state 首层 = 分区文件；
3. **50GB align_l1**：目标吞吐 ≥30K（vs sampled 22.6K——物化滞留应消失）；
4. 若桶内共享（base 层文件跨分区）仍出现 → 叠加 SafeRangeOf（已提交）+ 评估层 3。

## 4. 风险

| 风险 | 缓解 |
|---|---|
| base_level 随数据下移（L6→L1）→ 表每 epoch 变 → 孤儿跨表 | SafeRangeOf 已提交（全范围兜底）；孤儿只在 fallback 时产生（层 1/2 后罕见） |
| base 层文件被原生 L0→base 合并（fallback 期）| 层 1/2 消除 fallback 后 L0 空，原生不触发 |
| 采样边界在 epoch 1 时不准（数据分布未知）| sampled 同样依赖采样——1/64 采样足够（M3.1 验证） |
| dynamic base 迁移期（base 从 L6 到 L1 的下移 compaction）| 下移由原生处理（层间搬移），ZF 表跟随 base 层文件即可 |

## 5. 结论

启动期对齐的根因不是"L1 空"而是 **align_l1 对齐了错误的层（L1 而非 ZF 实际直装层 base_level）**。改对齐对象为 base_level + 保留采样启动兜底 + 现有 SafeRangeOf 孤儿防护，即可形成闭环：启动期切片直装 base（对齐）→ 稳态 1:1 融合（无 fallback 无漂移）→ base 层恒对齐。这是比"物化侧切分"（R59B）小一个量级的改动，且直接复用 align_l1 的现有框架。
