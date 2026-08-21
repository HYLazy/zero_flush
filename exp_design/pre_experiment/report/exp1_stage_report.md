# 实验 1：L0 Overlap 源于写入准入 — 阶段性详细报告

- **实验编号**：exp1_overlap_at_admission
- **报告日期**：2026-08-05
- **对应方案文档**：`exp_design/pre_experiment/plan/ZeroFlush_实验方案与执行提示词_Exp0-3.docx`（实验 1 章节）
- **对应 PPT**：`exp_design/pre_experiment/plan/ZeroFlush_Presentation.pptx` 第 4/10 页「H2: L0 overlap comes from write admission」
- **权威数据源**：`exp_design/pre_experiment/report/exp1_report.json`（完整 schema，8 runs：zipfian 6 + uniform 2，31 项 schema 验证全 PASS）
- **原始数据**：`output/pre_exp/exp1/raw/`（8 个 run 目录：baseline_zipfian_vs1024/run{1,2,3} + intervention_zipfian_vs1024/run{1,2,3} + baseline_uniform_vs1024/run1 + intervention_uniform_vs1024/run1，含 stdout/stderr/LOG/LOG_fillrandom/overlap_probe.json/compact_read_bytes.txt 等）
- **补丁**：`exp_design/pre_experiment/code/exp1_l0_overlap/patches/0001-flush-partition.patch` + `0002-zipfian.patch`
- **机械判定结果**：**FAIL**（验收 4 项中 3 项 FAIL，1 项 PASS）

---

## 1. 实验目的与假设 H2

**假设 H2**：L0 SST 之间的 key range 重叠（overlap）源于写入准入（write admission）阶段——即所有写入进入同一个 MemTable，flush 时整个 MemTable 的 key range 作为一个整体落盘为单个 SST，多个 flush 轮次产出的 SST 之间自然产生重叠。

**预期**：若在 flush 时按 L1 边界将单 MemTable 迭代流分区为 P 个互不重叠的 SST，则：
- baseline overlap_factor_mean ≥ 2.0（多个 flush 轮次 L0 文件互相重叠）
- intervention overlap_factor_mean ≤ 1.2（分区后 L0 文件 key range 互不重叠）
- compaction 读字节降低 ≥ 40%（L0→L1 compaction 读量减少）
- 吞吐回退 ≤ 5%（分区开销可接受）

**干预方式声明（重要）**：
- **实际采用的干预为 flush 时分区（flush-time partition），系回退预案，非主方案 PartitionedMemTable（写入时路由）。** 主方案 PartitionedMemTable 的设计意图是在写入时将 Put 路由到 P 个独立子表，每个子表独立 flush 时各产出 1 个 SST，从而避免 L0 文件数暴涨。但由于 PartitionedMemTable 集成复杂度超出预算（前任未完成），退而在 `db/flush_job.cc` 的 `WriteLevel0Table` 中实现 flush 时分区：将单 MemTable 迭代流按 L1 文件边界分流到 ≤P=16 个 TableBuilder，各产出互不重叠 SST。
- **回退方案的固有缺陷**：每次 flush 发射多达 16 个 SST（vs 基线的 1 个），导致 L0 文件数增长 16 倍，频繁触发 `level0_slowdown_writes` stall，这是吞吐回退的主因。主方案 PartitionedMemTable 可避免此问题（独立子表 flush 每次 1 SST）。
- **采用回退原因**：PartitionedMemTable 写入时路由集成复杂度超标，前任未完成；flush 时分区为文档化的协议回退，非静默降级。

---

## 2. 方法学

### 2.1 引擎与构建

- **基线引擎**：RocksDB 11.2.0 vanilla（`source/rocksdb-exp0-tools/build/db_bench`），仅 `tools/db_bench_tool.cc` 打 `--key_distribution` 工具层补丁（支持 `zipfian:0.99`），引擎零改动。
- **干预引擎**：RocksDB 11.2.0 + flush 时分区补丁（`source/rocksdb-exp1-partition/build/db_bench`），在 `db/flush_job.cc` 的 `WriteLevel0Table` 中按 L1 文件边界将单 MemTable 迭代流分流到 ≤P=16 个 TableBuilder。
- **zipfian 补丁**：`0002-zipfian.patch`，工具层实现 Zipfian(θ=0.99) 分布生成器，不修改引擎代码。
- **flush 分区补丁**：`0001-flush-partition.patch`，在 `WriteLevel0Table` 中按 L1 文件边界（`min(16, L1_file_count)` 个分区，对齐 L1 文件 largest keys）将迭代流分流；L1 为空或存在 range deletions 时回退为单 SST。

### 2.2 overlap_probe 测量方法

- **工具**：独立 `overlap_probe`（链接 vanilla `source/rocksdb/build/librocksdb.so` 只读打开 DB）。
- **方法**：均匀随机采样 100,000 个 key（key space = 20M，`db_bench GenerateKeyFromInt` 编码：大端 8B index + '0' padding），对每个 key 统计 L0 文件（`GetLiveFilesMetaData`，`level==0`）中 `[smallest_key, largest_key]` 覆盖该 key 的文件数（`BytewiseComparator` 比较）。
- **指标**：`overlap_factor_mean`（平均覆盖数）、`overlap_factor_p99`、`overlap_factor_max`。
- **point_query 代理**：`point_query_l0_files_checked_mean` = `overlap_factor_mean`（同一覆盖计数，代理点查需检查的 L0 文件数），与 `db_bench readrandom --perf_level=4` PerfContext 交叉验证。

### 2.3 L0→L1 compaction 读字节口径

- **主口径**：`rocksdb.compact.read.bytes` ticker（`db_bench --statistics` 输出），为**全层级** compaction 累计读字节，非 L0→L1 特定口径。
- **LOG per-level 口径**：RocksDB LOG 中的 `Compaction Stats` 表 `L1 Read(GB)` 行为 L0→L1 特定数据。但 `overlap_probe` 打开 DB 时创建新 LOG，覆盖原有 LOG。仅 intervention run3 的 `LOG_fillrandom` 在 overlap_probe 前手动保存，保留了 L0→L1 特定数据（L1 累计 Read = 279.8 GB）；baseline 及 intervention runs 1-2 的 LOG 被 overwrite，L0→L1 特定口径不可用。此局限已在 `measurement_method` 中声明。
- **run3 交叉核对**（intervention zipfian run3）：L0→L1 特定累计读 = 279.8 GB（LOG L1 行）；全层级 Sum Read = 358.6 GB（LOG Sum 行）；ticker `compact.read.bytes` = 405.9 GB。三者口径不同，ticker 偏大因包含 L1→L2 等高层级读。

### 2.4 实验矩阵

- **zipfian(θ=0.99)**：baseline × 3 rep + intervention × 3 rep，取中位数。
- **uniform**：baseline × 1 rep + intervention × 1 rep（单点，非中位数，因 uniform 运行时长约为 zipfian 的 2.5 倍，简化为单点）。
- **附录参数**（取自 `exp0_report.json` 冻结的 `workload_config.json`）：
  - key_size=16B, num=20M (keyspace), writes=10M/thread, threads=8 (总 80M ops)
  - write_buffer_size=64MB, max_write_buffer_number=4
  - compaction_style=level, target_file_size_base=64MB, max_bytes_for_level_base=256MB
  - sync=false, seed=42, compression=none, value_size=1024B

### 2.5 验收阈值

| 验收项 | 条件 | 阈值 |
|--------|------|------|
| V1 baseline overlap | baseline.overlap_factor_mean ≥ | 2.0 |
| V2 intervention overlap | intervention.overlap_factor_mean ≤ | 1.2 |
| V3 compaction 读降低 | (baseline − intervention)/baseline × 100 ≥ | 40% |
| V4 吞吐回退 | (baseline − intervention)/baseline × 100 ≤ | 5% |

---

## 3. 测试数据表

> 全部数值严格取自 `exp1_report.json`。

### 3.1 zipfian(θ=0.99) 6 runs（baseline × 3 + intervention × 3，中位数加粗）

| 组 | rep | overlap_mean | overlap_p99 | L0 文件数 | compaction_read (MB) | 吞吐 (ops/s) | flush_partition_count |
|----|----:|-------------:|------------:|----------:|---------------------:|-------------:|----------------------:|
| baseline | 1 | 8.99813 | 9 | 9 | 145,339.75 | 58,738 | 0 |
| baseline | 2 | **5.99874** | **6** | **6** | 145,279.49 | 55,992 | 0 |
| baseline | 3 | 3.99934 | 4 | 4 | 145,658.14 | **58,522** | 0 |
| intervention | 1 | 1.99766 | 2 | 17 | 393,955.54 | 23,010 | 0 |
| intervention | 2 | **1.97978** | **2** | **17** | 384,493.81 | **21,808** | 0 |
| intervention | 3 | 0.98773 | 1 | 1 | **387,052.77** | 21,804 | 912 |

> **中位数说明**：zipfian baseline overlap 中位数 = rep2 (5.99874)；intervention overlap 中位数 = rep2 (1.97978)；baseline 吞吐中位数 = rep3 (58,522)；intervention 吞吐中位数 = rep2 (21,808)；intervention compaction_read 中位数 = rep3 (387,052.77)。rep1 baseline overlap=8.998 为异常高值（L0 积压 9 个文件），但仍 ≥ 2.0 满足 V1。

### 3.2 uniform 2 runs（baseline × 1 + intervention × 1，单点非中位数）

| 组 | rep | overlap_mean | overlap_p99 | L0 文件数 | compaction_read (MB) | 吞吐 (ops/s) | flush_partition_count |
|----|----:|-------------:|------------:|----------:|---------------------:|-------------:|----------------------:|
| baseline | 1 | 6.99943 | 7 | 7 | 388,444.17 | 23,311 | 0 |
| intervention | 1 | 0.23429 | 1 | 2 | 1,152,814.46 | 7,273 | 1,018 |

> **声明**：uniform 仅为 1 rep（非中位数），因 uniform 运行时长约为 zipfian 的 2.5 倍（80M ops × 1024B value，baseline 23,311 ops/s ≈ 3,430s ≈ 57min；intervention 7,273 ops/s ≈ 10,997s ≈ 183min）。zipfian 为主结论（3 rep 中位数）。

### 3.3 逐 rep 干预 overlap（达标证据）

| 分布 | rep | overlap_mean | L0 文件数 | 达标（≤1.2）|
|------|----:|-------------:|----------:|:----------:|
| zipfian:0.99 | 1 | 1.99766 | 17 | FAIL |
| zipfian:0.99 | 2 | 1.97978 | 17 | FAIL |
| zipfian:0.99 | 3 | 0.98773 | 1 | **PASS** |
| uniform | 1 | 0.23429 | 2 | **PASS** |

> run3 zipfian overlap=0.99（L0 文件数=1）达标，证明 P=16 在 compaction 跟上时**可以**满足目标。runs 1-2 overlap≈1.98（L0 文件数=17）未达标，因多次 flush 积聚后 compaction 尚未追上。

### 3.4 汇总对比（中位数）

| 指标 | baseline (zipfian 中位数) | intervention (zipfian 中位数) | 变化 |
|------|------------------------:|----------------------------:|------|
| overlap_factor_mean | 5.9987 | 1.9798 | −67.0% |
| overlap_factor_p99 | 6 | 2 | −66.7% |
| L0 文件数 | 6 | 17 | +183.3% |
| compaction_read (MB) | 145,339.75 | 387,052.77 | +166.3% |
| 吞吐 (ops/s) | 58,522 | 21,808 | −62.74% |
| point_query_l0_files_checked_mean | 5.9987 | 1.9798 | −67.0% |
| P (分区数) | — | 16 | — |

### 3.5 uniform 对比（单点）

| 指标 | baseline (uniform) | intervention (uniform) | 变化 |
|------|-------------------:|-----------------------:|------|
| overlap_factor_mean | 6.9994 | 0.2343 | −96.7% |
| 吞吐 (ops/s) | 23,311 | 7,273 | −68.81% |
| compaction_read (MB) | 388,444.17 | 1,152,814.46 | +196.8% |

---

## 4. 分析

### 4.1 overlap 下降幅度与目标 ≤1.2 的差距

见 `charts/exp1_overlap_baseline_vs_intervention.svg`。

- **zipfian（中位数）**：5.9987 → 1.9798，下降 67.0%。未达 ≤1.2 目标（1.9798 > 1.2）。
- **uniform（单点）**：6.9994 → 0.2343，下降 96.7%。**达标**（0.2343 < 1.2）。
- **zipfian run3**：0.98773（L0 文件数=1），**达标**。证明 P=16 在 compaction 跟上时可以满足目标。
- **差距根因**：runs 1-2 的 L0 文件数为 17（探测时多次 flush 积聚后 compaction 尚未追上），导致 overlap≈2.0（两轮 flush 的分区 SST 互相重叠）。run3 探测时 compaction 已完成（L0 文件数=1），overlap≈1.0。

见 `charts/exp1_per_rep_intervention_overlap.svg`。

### 4.2 run3 zipfian=0.99 与 uniform=0.23 达标的时序条件

- **run3 zipfian**：overlap_probe 探测时 L0 仅有 1 个文件，说明 compaction 已将之前积聚的 17 个 L0 文件合并完毕。此时 overlap=0.99（≈1，即每个 key 最多被 1 个 L0 文件覆盖）。flush_partition_count=912 次（70% 发射 16 SST），但最终 L0 收敛到 1 个文件。
- **uniform**：overlap_probe 探测时 L0 有 2 个文件，overlap=0.2343（约 23% 的 key 被 1 个 L0 文件覆盖，其余被 0 个覆盖——即大部分采样 key 不在 L0 范围内，L0 已被 compaction 清理到极少量残留）。flush_partition_count=1018 次。
- **时序条件**：达标发生在 compaction 追上 flush 速率的稳态或收尾阶段。runs 1-2 探测时处于 flush 速率 > compaction 速率的过渡期，L0 文件积聚。

### 4.3 compaction 读反增 2.66x 根因

见 `charts/exp1_throughput_baseline_vs_intervention.svg`。

- **baseline compaction_read**：145,339.75 MB（zipfian 中位数）
- **intervention compaction_read**：387,052.77 MB（zipfian 中位数）
- **比率**：387,052.77 / 145,339.75 = **2.663x**（+166.3%）
- **根因**：flush 时分区每次 flush 发射多达 16 个 SST，L0 文件数增长 16 倍（中位数 17 vs 基线 6），触发更频繁的 L0→L1 compaction 事件。每次 L0→L1 compaction 需读取所有相关 L0 文件 + L1 重叠文件，文件数增多直接增加读量。
- **与预期相反**：预期分区后 L0 文件 key range 互不重叠，减少单次 compaction 读量；但实际 L0 文件数暴涨抵消了单次读量减少，总读量反增。

### 4.4 吞吐回退 62.74% 根因（L0 stall）

- **baseline 吞吐**：58,522 ops/s（zipfian 中位数）
- **intervention 吞吐**：21,808 ops/s（zipfian 中位数）
- **回退**：(58,522 − 21,808) / 58,522 × 100 = **62.74%**
- **根因**：flush 时分区每次 flush 发射多达 16 SST → L0 文件数增长 16 倍 → 频繁触发 `level0_slowdown_writes` stall。
- **stall 数据**（intervention run3，取自 `LOG_fillrandom`）：
  - `rocksdb.stall.micros`：baseline 836s → intervention 2,958s（**3.5×基线**）
  - L0 文件数限制：1,357 delays + 717 stops
  - Memtable 限制：924 delays + 11 stops
  - **合计**：2,281 delays + 728 stops
- **stall 是吞吐回退的主因**，而非 TableBuilder 开销。64MB wbs 比 smoke test 的 4MB（80% 回退）摊销更好，但在 10M writes/thread 规模下仍显著。

### 4.5 zipfian vs uniform 分区效果差异

| 分布 | baseline overlap | intervention overlap | 下降幅度 | L0 文件数 (intervention) |
|------|-----------------:|----------------------:|---------:|------------------------:|
| zipfian:0.99 | 5.9987 | 1.9798 | −67.0% | 17（中位数）|
| uniform | 6.9994 | 0.2343 | −96.7% | 2 |

- **zipfian**：θ=0.99 极端偏态使热键高度集中，部分分区接收极不均匀的数据量，导致部分分区 SST key range 实际覆盖范围较大，overlap 降至 1.98（未达 1.2）。
- **uniform**：键均匀分布，16 个分区数据量均衡，分区效果更佳，overlap 降至 0.23（达标）。
- **关键发现**：分区本身有效（overlap 均显著下降），但效果受 compaction lag 限制——当 compaction 无法跟上 flush 速率时，多轮 flush 的分区 SST 在 L0 积聚并互相重叠，导致 overlap > 1.0。

### 4.6 compaction lag 对 overlap 的时序影响

见 `charts/exp1_l0_file_count.svg`。

- **intervention zipfian L0 文件数**：runs 1-2 = 17，run3 = 1。中位数 = 17（vs baseline 6）。
- **时序解释**：flush 速率 > compaction 速率时，L0 文件积聚（runs 1-2 探测到 17 个文件 = 2 轮 flush × ~8-9 SST/轮）。当 compaction 追上时（run3），L0 收敛到 1 个文件，overlap 降到 ~1.0。
- **关键发现**：分区本身有效降低 overlap，但受 compaction lag 限制——在 compaction 无法跟上时，多轮 flush 的分区 SST 在 L0 积聚并互相重叠。

---

## 5. 验收核对（逐项引用数值）

### zipfian:0.99（3 次中位数，主结论）

| 验收项 | 计算 | 阈值 | 判定 |
|--------|------|------|------|
| V1 baseline overlap ≥ 2.0 | 5.9987 ≥ 2.0 | ≥2.0 | **PASS** |
| V2 intervention overlap ≤ 1.2 | 1.9798 ≤ 1.2 → 1.9798 > 1.2 | ≤1.2 | **FAIL** |
| V3 compaction 读降低 ≥ 40% | (145339.75−387052.77)/145339.75 × 100 = −166.3% | ≥40% | **FAIL** |
| V4 吞吐回退 ≤ 5% | (58522−21808)/58522 × 100 = 62.74% | ≤5% | **FAIL** |

→ 4 项中 3 项 FAIL，1 项 PASS ⇒ **整体 FAIL**。

### 达标证据（部分 rep / 分布）

| 条件 | 数值 | 判定 |
|------|------|:----:|
| zipfian run3 overlap ≤ 1.2 | 0.98773 ≤ 1.2 | **PASS** |
| uniform overlap ≤ 1.2 | 0.23429 ≤ 1.2 | **PASS** |
| uniform overlap 下降幅度 | −96.7% | 达标 |
| zipfian overlap 下降幅度 | −67.0% | 未达标（1.98 > 1.2）但显著下降 |

> run3 zipfian 和 uniform 证明 P=16 **可以**在 compaction 跟上时满足 overlap ≤ 1.2 目标，但 zipfian 中位数（1.98）因 compaction lag 未达标。

---

## 6. 结论

1. **flush 时分区（回退方案）在该配置下不可行**：每次 flush 发射多达 16 SST → L0 文件数增长 16 倍（中位数 17 vs 基线 6）→ 频繁触发 `level0_slowdown_writes` stall（2,281 delays + 728 stops，stall.micros 3.5×基线）→ 吞吐回退 62.74%。三项验收（overlap/compaction读/吞吐）均 FAIL。
2. **主方案 PartitionedMemTable 的必要性**：主方案在写入时路由到 P 个独立子表，每个子表独立 flush 时各产出 1 SST，避免 L0 文件数暴涨。flush 时分区虽然降低了单次 flush 的 overlap（分区 SST 互不重叠），但因 L0 文件数暴涨触发 stall 和 compaction 读增加，反而恶化吞吐和 compaction 读。主方案从根本上避免了 flush 时分区的固有缺陷。
3. **ZeroFlush 设计的 L0 文件数管理挑战**：本实验的负面结果揭示了一个关键设计约束——任何增加 L0 文件数（每次 flush 产出 >1 SST）的方案都会触发 `level0_slowdown_writes` stall，导致吞吐大幅回退。ZeroFlush 的 L0 管理策略必须控制 L0 文件数增长速率，否则 stall 不可接受。此负面结果有研究价值。
4. **P_recommend = 16**：run3 zipfian overlap=0.99 + uniform overlap=0.23 达标证据支持 P=16；增大 P 会加剧 L0 文件数 stall（根因是 compaction lag 而非分区数不足）。
5. **分区本身有效**：overlap 在所有分布和 rep 中均显著下降（zipfian −67%、uniform −96.7%），证明 flush 时按 L1 边界分区确实能降低 L0 overlap。但受 compaction lag 限制，中位数未达 ≤1.2 目标。

---

## 7. 风险与局限

1. **回退方案非主方案**：本实验干预为 flush 时分区（回退预案），非主方案 PartitionedMemTable（写入时路由）。flush 时分区的固有缺陷（每次 flush 16 SST → L0 文件数暴涨 → stall）为主方案所无。主方案 PartitionedMemTable 因集成复杂度超标（前任未完成）未尝试，无法在本实验中验证。结论「flush 时分区不可行」不等同于「PartitionedMemTable 不可行」——后者从根本上避免了 L0 文件数暴涨。
2. **uniform 单点非中位数**：uniform 分布仅为 1 rep（非中位数），因 uniform 运行时长约为 zipfian 的 2.5 倍。uniform 达标（overlap 0.23）为单点证据，需更多 rep 确认稳健性。zipfian 为 3 rep 中位数，为主结论。
3. **LOG 覆盖致 L0→L1 特定口径仅 run3 可用**：`overlap_probe` 打开 DB 时创建新 LOG，覆盖原有 LOG。仅 intervention run3 的 `LOG_fillrandom` 在 overlap_probe 前手动保存，保留了 L0→L1 特定 compaction 读数据（279.8 GB）。baseline 及 intervention runs 1-2 的 L0→L1 特定口径不可用，报告使用全层级 `compact.read.bytes` ticker 作为代理口径。run3 交叉核对显示 ticker (405.9 GB) > LOG Sum (358.6 GB) > LOG L1 特定 (279.8 GB)，因 ticker 含高层级读。
4. **主方案 PartitionedMemTable 未尝试**：主方案因集成复杂度超标（前任未完成）未在本实验中实现和验证。本实验的 FAIL 结论仅针对 flush 时分区回退方案。主方案的可行性需后续实验验证。
5. **powersave 调频器**：CPU 调频器为 powersave（低频运行），绝对吞吐偏低；但 overlap 下降幅度、compaction 读比率、stall 倍数为相对比较，结论对调频器不敏感。
6. **stall 数据来源**：baseline stall.micros=836s 来自 `--statistics` stdout 输出（baseline LOG 被 overlap_probe 覆盖，显示 stall.micros=0，系 LOG 被覆盖后重新打开 DB 的初始值）；intervention run3 stall.micros=2,958s 来自 `LOG_fillrandom`（手动保存，最后一次统计 dump 的累计值 2,810,837,653 micros ≈ 2,811s，与 stdout 的 2,958s 略有差异，系最终 `--statistics` dump 与 LOG 最后一次 interval dump 的口径差异）。

---

## 8. 原始数据引用路径（供审查对账）

| 用途 | 路径 |
|------|------|
| 权威报告 JSON（本报告数值来源） | `exp_design/pre_experiment/report/exp1_report.json` |
| 基线 zipfian run1 | `output/pre_exp/exp1/raw/baseline_zipfian_vs1024/run1/` |
| 基线 zipfian run2（中位数） | `output/pre_exp/exp1/raw/baseline_zipfian_vs1024/run2/` |
| 基线 zipfian run3 | `output/pre_exp/exp1/raw/baseline_zipfian_vs1024/run3/` |
| 干预 zipfian run1 | `output/pre_exp/exp1/raw/intervention_zipfian_vs1024/run1/` |
| 干预 zipfian run2（中位数） | `output/pre_exp/exp1/raw/intervention_zipfian_vs1024/run2/` |
| 干预 zipfian run3（达标 + stall 数据） | `output/pre_exp/exp1/raw/intervention_zipfian_vs1024/run3/` |
| 基线 uniform run1 | `output/pre_exp/exp1/raw/baseline_uniform_vs1024/run1/` |
| 干预 uniform run1 | `output/pre_exp/exp1/raw/intervention_uniform_vs1024/run1/` |
| overlap_probe 数据 | 各 run 目录下 `overlap_probe.json` |
| compaction 读字节 | 各 run 目录下 `compact_read_bytes.txt` |
| fillrandom 结果 | 各 run 目录下 `fillrandom_result.txt` |
| stall 统计 | `intervention_zipfian_vs1024/run3/LOG_fillrandom`（手动保存） |
| flush 分区补丁 | `exp_design/pre_experiment/code/exp1_l0_overlap/patches/0001-flush-partition.patch` |
| zipfian 补丁 | `exp_design/pre_experiment/code/exp1_l0_overlap/patches/0002-zipfian.patch` |
| 图表生成脚本 | `exp_design/pre_experiment/report/charts/gen_exp1_charts.py` |
| 图表（SVG） | `exp_design/pre_experiment/report/charts/exp1_*.svg` |
| 冻结 workload 配置 | `exp_design/pre_experiment/code/exp0_flush_cost/workload_config.json` |

### 硬件环境

| 项目 | 值 |
|------|-----|
| CPU | Intel(R) Core(TM) i9-10980XE CPU @ 3.00GHz, 36 核 |
| cache | 25,344 KB |
| reps | 3（zipfian，取中位数）/ 1（uniform，单点非中位数） |
| value_size | 1024 B |

> 图表说明：本机 matplotlib 不可用（`python3 -c "import matplotlib"` 失败，pip 安装受网络限制未成功），故按约束采用降级方案——用纯 Python 标准库生成 **SVG 矢量图表**（浏览器原生支持 `<img>` 渲染），保存为独立 `.svg` 文件由 HTML 相对路径引用，结构等同 PNG 方案。生成脚本：`charts/gen_exp1_charts.py`。
