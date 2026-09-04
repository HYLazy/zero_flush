# M5 设计：Per-Range L1 合并架构（fillrandom 静态版）

状态：设计定稿（2026-09-04）。分支 zeroflush0.9（基线 cdcbdac）。
作者决策记录（用户拍板）：① range 尺寸按默认 SST 大小 64MB；② 安装期冲突用
"丢弃输出转 recovery"（弃 L0 兜底）；③ WAL→L1 合并自建、L1→L2 下沉用原生代码。

## 1. 背景与动机

现状（R59E 归因 + 多轮实验证伪）：小批数据撞大 L1 累积 → 融合 ratio 门槛拒融合
→ 装 L0 → 原生全范围消费或 ZF 兜底大重写 → 写放大 11.6× vs native 9.8×，
100GB 吞吐落后 native ~10-21%，L0 循环无参数解（P0a/R59X/trigger/skip_batching/
方案 A 全部证伪）。

本设计从结构上消除 L0 循环：**L1 每 range 至多 1 个 ≤64MB 文件、数据到量即与
自己的 L1 文件就地合并（无条件）、满了下沉 L2（原生执行）**。合并粒度与 L1
文件同量级 → 不存在"8MB 撞 3GB" → ratio 门槛、L0、fallback 全部不需要。

## 2. 架构总览

数据按 range（现有 16 分区静态等分，sampled 表初始化）路由。每 range 独立循环：

```
[WAL 段攒批(≤64MB)] → [L1 空?] ──是──→ kDirect 直装（≤64MB 单文件，写放大 1×）
                          └─否（L1 有旧文件 F≤64MB）──→
    ├ N+F ≤ 64MB → kMergeBase：WAL 数据与 F 归并 → 输出 1 文件（≤64MB）
    ├ N 不足（<64MB）→ kSkip 攒批（字节化，数据留 WAL + frozen 索引）
    └ N+F > 64MB（F 已满）→ 触发下沉 F（原生 L1→L2）→ N 转 recovery（下批收养）
```

稳态（fillrandom 均匀）：每 range 每 64MB 数据 = 一次原生下沉（异步，与写入
流水重叠）+ 一次 ≤64MB 直装/合并写。写放大 ≈ 物化 1× + 原生下沉归并（L2 相交
累积，被原生 L2→L3 清场截断）+ 原生下游 ~1-2× → 总 ~3-4×（推演，待实测）。

## 3. 关键机制定稿

### 3.1 range 尺寸（决策①）
- 攒批到量阈值 kRangeMergeBytes = 64MB（默认 SST 大小）
- L1 文件上限 kRangeMaxBytes = 64MB（输出恒 ≤64MB 单文件，不做多文件切分；
  合并输出可能超限的场景由"先下沉旧文件"消解）
- 后续可调（64/128/256），平衡写放大与下沉频率。

### 3.2 决策规则（PlanLocked 重写）
- L1 无该 range 文件 → kDirect（无论 N 大小——直装无重写成本，不必攒）。
- L1 有文件 F：
  - 待物化字节 N（本批 + 收养的 skip/recovery 代）< 64MB 且 N < F → kSkip；
  - 否则 N + |F| ≤ 64MB → kMergeBase（无条件，删 ratio 判据）；
  - 否则（F 已满/接近满）→ 标记下沉 F + 本批转 recovery（见 3.4）。
- kSkip 上限从代数（kMaxSkipGenerations=2）改为**字节**：range 待物化 < 64MB
  均可 skip；全局 skip 字节阈值（现有 partition_target_bytes×32 = 2GB）兜底。
- 删除：ratio 门槛分支、kFallback、l0_overlap/l0_busy/batch_l0 分支族、
  PickInstallLevel 回落路径、L0 安装。
- 保留：批内链式替换（superseded）、batch_skipped 互斥、任务池并行、
  单次 VersionEdit 原子安装、范围断言、SafeRangeOf。

### 3.3 合并执行（自建，决策③-a）
现有 MaterializeMergePartition（A 侧 WAL 多代 + B 侧文件 → CompactionIterator
→ 输出）整体保留；改动：
- 输出切分阈值 = kRangeMaxBytes（64MB），不再按 target_file_size 切多文件；
- 直装（MaterializePartition）输出不变（≤64MB 天然单文件）。

### 3.4 下沉（原生执行，决策③-b）
- 触发：ZF 侧（物化决策/收尾窗口）对"L1 文件已满且本 range 有新数据"的 range
  请求原生 L1→L2 compaction，**按 range 指定 key 边界**（内部手动 compaction，
  非 score 触发；execution 异步，与写入流水重叠）。
- 执行：原生 compaction（trivial move 自动生效——L2 无重叠时零重写移动；
  有重叠时原生归并，L2 不重叠不变量由原生维护；输出跨界吸附由原生 L2→L3
  定期清场截断——L2+ 完全交给原生，ZF 不介入）。
- 互斥：下沉进行中 L1 文件 being_compacted → 该 range 后续批自然走
  kSkip（现有 busy 检测复用），下沉完成（L1 空）后恢复直装/合并。
- 期间到量数据：移交 recovery 集合（与 kSkip 同路径），下批收养。

### 3.5 安装期冲突与数据滞留（决策②）
阶段 1（物化）后复查发现冲突 → **不装 L0**：丢弃输出文件（物理删除）、
数据转 recovery（HandOffSkipped 同一窗口内移交，下批收养重物化）。
铁律（P0a 教训）：
1. 移交必须在 imm 出链/ReleaseEpoch 之前完成（现有 HandOffSkipped 时序）；
2. **DB 关闭路径必须冲刷 recovery 集合**（flush 循环直到集合空），禁止
   skip/recovery 数据跨 DB 关闭滞留——readrandom 跨进程命中率（63.2%）为
   数据完整性验收门。

### 3.6 原生介入禁用（zeroflush_db.cc Open）
- level0_file_num_compaction_trigger / slowdown / stop = 1e8（L0 永不产生，
  原生永不消费）；
- 原生 L1→L2 score 触发禁用（max_bytes_for_level_base 放大或等价手段）——
  L1→L2 只响应 ZF 的手动 range 下沉；
- L2→L3 及以下保持原生默认（清场/分层）。

## 4. 改动清单（文件级）

| 文件 | 改动 |
|---|---|
| zeroflush_db.cc | Open 选项覆盖（§3.6）；关闭冲刷 recovery（§3.5-2） |
| materialize_job.cc | PlanLocked 决策重写（§3.2）；MaterializeMergePartition 输出阈值 =64MB（§3.3）；下沉请求出口（§3.4）；安装冲突 → 转 recovery（§3.5） |
| materialize_job.h | 参数（kRangeMergeBytes/kRangeMaxBytes）；下沉请求状态 |
| flush_job.cc | 安装循环：L0 分支删除；冲突处理路径改 recovery 移交；收尾冲刷调用 |
| sealed_file_cache / zeroflush_db | recovery 集合语义不变（复用）；关闭冲刷接口 |
| zf_test.cc | 用例适配：L0 语义断言（SteadyStateZeroL0/ControlledL0/SkipBatchMaterialize 等）改 per-range 语义 |

## 5. 实施阶段与验收

P1（决策+合并+禁用，无下沉）暂缓——无下沉时 L1 单文件无界增长，写放大随
累积上升，无法独立验收。改为：

**P1：核心闭环**（决策重写 + 单文件化 + 原生禁用 + 下沉触发 + 关闭冲刷）
- zf_test 回归全绿（适配用例）；50GB fillrandom：装 L0 计数 = 0、
  读命中 63.2%（零丢失）、写放大 < 9×、吞吐 > 27.1K。
- 100GB 同窗：吞吐 ≥ native（21.7K 基准）为最终验收。

P1 即含全部核心改动；后续（偏态 range 分裂/合并、边界校正、叠加 WAL 流水、
热 range 多文件组）为扩展项，机制已预留（表版本化 + 字节化 skip）。

## 6. 风险与回滚

1. 数据滞留/冲刷（§3.5 铁律）——最高风险，关闭冲刷单独用例验证
   （FreezeReopen / CrashBeforeCompact / 跨进程 readrandom 命中率）。
2. 下沉与合并的同 range 串行依赖 being_compacted 检测——回归用例覆盖
   （下沉中写入场景）。
3. 原生手动 compaction 的调度方式（内部 API 选择）——P1 第一个技术验证点。
4. 每阶段独立提交、可回滚（改动集中在 materialize_job 决策 + flush 收尾 +
   Open 选项三处，其余文件只读不动）。

## 7. 待细化（P1 开工前）

- 下沉触发 API：DBImpl 内部手动 compaction（RunManualCompaction 异步化）vs
  CompactRange 语义的持锁调度——flush 线程内触发不可同步等待（死锁风险），
  需"注册 pending manual compaction + 让出"或专用触发点。
- kSkip 字节化的内存账目（range 待物化字节的统计点：se.part_bytes + 收养）。
- 关闭冲刷的触发位置（DB 析构 / Close / 最后 flush 循环）。
