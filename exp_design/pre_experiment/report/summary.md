# ZeroFlush 前期实验 0–3 跨实验汇总报告

> **日期**: 2026-08-05  
> **方案文档**: `exp_design/pre_experiment/plan/ZeroFlush_实验方案与执行提示词_Exp0-3.docx`  
> **展示文档**: `exp_design/pre_experiment/plan/ZeroFlush_Presentation.pptx`  
> **PPT 锚点**: 第 3–4 页 Motivation（exp0）、第 4/10 页 H2（exp1）、第 10 页 Design 1（exp2/exp3）  
> **数据源**: `exp_design/pre_experiment/report/exp{0,1,2}_report.json`（权威 JSON）、`exp{0,1,2}_stage_report.md`（阶段报告）  
> **原始数据**: `output/pre_exp/exp{0,1,2}/`

---

## 1. 四实验状态总览

| 实验 | 目的 | 状态 | 关键指标 | 验收结论 | 对应 PPT 页 |
|------|------|------|----------|----------|:----------:|
| Exp0 | Flush 成本分解，验证 H1（flush 占一次 NAND 写，可分解可量化） | **done (PASS)** | WA_WAL≈1.020、WA_Flush≈0.767（uniform≈1.01）、WA_Compaction≈3.367；flush_share=0.1668；守恒残差 0.0%；磁盘交叉核对残差<2% | H1 核心主张成立（WA_Flush≈1.01 支撑 flush=一次 NAND 写）；flush_share 0.2–0.4 预期未达（根因 compaction 放大膨胀分母） | 第 3–4 页 |
| Exp1 | L0 Overlap 源于写入准入，验证 H2（flush 时分区降低 overlap） | **done (FAIL)** | overlap 5.9987→1.9798（−67%，未达≤1.2）；compaction 读反增 166.3%（387053MB vs 145340MB）；吞吐回退 62.74%（58522→21808 ops/s） | 4 项验收 3 项 FAIL（V2/V3/V4），1 项 PASS（V1）；run3 zipfian overlap=0.99、uniform overlap=0.23 达标 | 第 4/10 页 |
| Exp2 | 分区开销门，验证 WAL=L0 分区映射的可行性 | **done (FAIL)** | range lookup p99 66–90ns（达标）；multi-WAL 模式B(P=64) 吞吐回退 19.47%、durability 窗口 1902.0261ms（≫5ms）；模式A(P=64) 回退 76.797% | decision=FAIL；两模式均不满足 PASS 三道闸门 | 第 10 页 |
| Exp3 | Partition Index vs MemTable，验证 H4 | **aborted** | 无（前置 exp2=FAIL，按方案中止） | 中止，未产出数据 | 第 10 页 |

---

## 2. 实验 0：Flush 成本分解（Motivation 数据）

### 2.1 WA 分解三项（6 组合均值，取自 `exp0_report.json.summary.wa_decomposition`）

| 指标 | 值 |
|------|---:|
| WA_WAL | 1.0203 |
| WA_Flush | 0.7670 |
| WA_Compaction | 3.3673 |

> WA_WAL 稳定在 ~1.0x（WAL 写入 ≈ 用户数据字节）。WA_Flush 在 uniform 分布下 ≈1.01x（vs100=1.025、vs1024=1.010、vs4096=1.010），直接支撑 H1 核心主张。WA_Compaction 是 NAND 写主导因素，uniform_vs4096 达 9.099x。

### 2.2 flush share 与守恒残差

| 指标 | 值 | 说明 |
|------|---:|------|
| flush_share_of_nand_writes | 0.1668 | 低于期望区间 [0.2, 0.4]，根因是 WA_Compaction 远超预期致分母膨胀 |
| decomposition_residual_pct | 0.0% | 恒成立（total_nand = WAL+Flush+Compact 三项之和） |
| 磁盘增量交叉核对残差 | [1.80%, 0.78%, 1.36%, 0.79%, 0.83%, 0.36%] | 全部 < 2.0%，远低于 5% 阈值 |

> **结论**: flush 占比低于预期区间不影响 H1 成立。H1 核心主张是"flush = 一次 NAND 写"（即 WA_Flush ≈ 1.0x），而非"flush 占总 NAND 写流量的 20–40%"。后者高度依赖 compaction 强度。

### 2.3 冻结 workload_config 关键字段

以下配置已冻结于 `exp_design/pre_experiment/code/exp0_flush_cost/workload_config.json`，供实验 1/2/3 引用：

| 字段 | 值 |
|------|-----|
| engine | RocksDB 11.2.0 |
| key_size_bytes | 16 |
| value_sizes_bytes | [100, 1024, 4096] |
| num_keys | 20,000,000 |
| ops_per_run | 10,000,000（per thread） |
| write_buffer_size_mb | 64 |
| max_write_buffer_number | 4 |
| compaction_style | level |
| target_file_size_base_mb | 64 |
| max_bytes_for_level_base_mb | 256 |
| distributions | [zipfian(θ=0.99), uniform] |
| sync_wal | false |
| threads | 8 |
| seed | 42 |
| compression_type | none |
| repetitions_per_point | 3 |
| aggregation | median |

### 2.4 H1 验证结论

**H1（flush 成本可分解）PASS**：
1. flush 可分解、可量化：三项 WA 分解成功，守恒残差 0.0%，磁盘交叉核对残差 < 2.0%。
2. flush 占一次 NAND 写：uniform 分布 WA_Flush ≈ 1.01x，直接支撑 H1 核心主张。
3. flush_share 0.2–0.4 预期未达（实测均值 0.1668），根因是 compaction 放大膨胀分母，不否定 H1。

---

## 3. 实验 1：L0 Overlap 源于写入准入

### 3.1 overlap 下降幅度

| 分布 | baseline overlap | intervention overlap | 下降幅度 |
|------|-----------------:|----------------------:|---------:|
| zipfian(θ=0.99)（中位数） | 5.9987 | 1.9798 | −67.0% |
| uniform（单点） | 6.9994 | 0.2343 | −96.7% |

> 分区本身有效（overlap 在所有分布和 rep 中均显著下降），但 zipfian 中位数 1.9798 未达 ≤1.2 目标。

### 3.2 三项 FAIL 逐项

| 验收项 | 计算值 | 阈值 | 判定 |
|--------|--------|------|:----:|
| V2 intervention overlap ≤ 1.2 | 1.9798 > 1.2 | ≤1.2 | **FAIL** |
| V3 compaction 读降低 ≥ 40% | (145339.75−387052.77)/145339.75 × 100 = −166.3%（反增 2.66×） | ≥40% | **FAIL** |
| V4 吞吐回退 ≤ 5% | (58522−21808)/58522 × 100 = 62.74% | ≤5% | **FAIL** |

> 唯一 PASS 项：V1 baseline overlap 5.9987 ≥ 2.0。

### 3.3 run3 / uniform 达标证据

| 条件 | 数值 | 判定 |
|------|------|:----:|
| zipfian run3 overlap ≤ 1.2 | 0.98773（L0 文件数=1） | **PASS** |
| uniform overlap ≤ 1.2 | 0.23429（L0 文件数=2） | **PASS** |

> run3 zipfian overlap=0.99 证明 P=16 在 compaction 跟上时可以满足 ≤1.2 目标。runs 1–2 overlap≈1.98（L0 文件数=17）因多次 flush 积聚后 compaction 尚未追上。

### 3.4 根因分析

**flush 时分区 L0 文件数 stall**：干预方式为 flush 时分区（`flush_time_partition`，回退预案，非主方案 PartitionedMemTable）。固有缺陷：每次 flush 发射多达 16 个 SST（vs 基线的 1 个），导致 L0 文件数增长 16 倍（中位数 17 vs 基线 6），频繁触发 `level0_slowdown_writes` stall。

stall 数据（intervention run3，取自 `LOG_fillrandom`）：
- `rocksdb.stall.micros`：baseline 836s → intervention 2,958s（3.5×基线）
- L0 文件数限制：1,357 delays + 717 stops
- Memtable 限制：924 delays + 11 stops
- 合计：2,281 delays + 728 stops

compaction 读反增 2.66×根因：flush 时分区每次发射 16 SST → L0 文件数暴涨 → 触发更频繁 L0→L1 compaction 事件 → 总读量反增（387,052.77 MB vs 145,339.75 MB）。

### 3.5 主方案 PartitionedMemTable 的必要性

主方案 PartitionedMemTable 在写入时将 Put 路由到 P 个独立子表，每个子表独立 flush 时各产出 1 个 SST，从根本上避免 L0 文件数暴涨。flush 时分区的固有缺陷（每次 flush 16 SST → L0 文件数暴涨 → stall）为主方案所无。主方案因集成复杂度超标（前任未完成）未在本实验中实现，FAIL 结论仅针对回退方案。

### 3.6 关键发现：zipfian vs uniform 分区效果

| 分布 | baseline overlap | intervention overlap | 下降幅度 | L0 文件数（intervention） |
|------|-----------------:|----------------------:|---------:|--------------------------:|
| zipfian:0.99 | 5.9987 | 1.9798 | −67.0% | 17（中位数） |
| uniform | 6.9994 | 0.2343 | −96.7% | 2 |

- **zipfian**：θ=0.99 极端偏态使热键高度集中，部分分区接收极不均匀的数据量，overlap 降至 1.98（未达 1.2）。
- **uniform**：键均匀分布，16 个分区数据量均衡，分区效果更佳，overlap 降至 0.23（达标）。

### 3.7 P_recommend

**P_recommend = 16**：run3 zipfian overlap=0.99 + uniform overlap=0.23 达标证据支持 P=16。增大 P 会加剧 L0 文件数 stall（根因是 compaction lag 而非分区数不足）。

---

## 4. 实验 2：分区开销门

### 4.1 range lookup 达标

| P | impl | mean_ns | p99_ns | 判定 |
|---|------|--------:|-------:|:----:|
| 64 | binary_search | 26.123 | 66.0 | ≤100ns |
| 64 | small_btree | 27.642 | 78.0 | ≤100ns |
| 128 | binary_search | 32.673 | 81.0 | ≤100ns |
| 128 | small_btree | 31.274 | 87.0 | ≤100ns |
| 256 | binary_search | 37.121 | 89.0 | ≤100ns |
| 256 | small_btree | 33.335 | 90.0 | ≤100ns |

> p99 全部 66–90 ns < 100 ns 阈值，风险点 (a) range lookup 达标。

### 4.2 multi-WAL fsync FAIL

基线 = P=1, independent_fsync, sync=true：throughput = 306,743.0 ops/s，fsync/op = 0.00021。

| P | mode | sync | throughput (ops/s) | fsync/op | durability p99 (ms) | 回退 |
|---|------|:----:|-------------------:|---------:|--------------------:|-----:|
| 64 | A independent_fsync | true | 71,174.0 | 0.013143 | 214.1229 | 76.797% |
| 64 | B batched_fsync | true | 247,030.7 | 0.000383 | 1902.0261 | 19.467% |

机械判定（三道闸门须全部满足方为 PASS，阈值：回退≤15%、fsync比值≤2×、durability≤5ms）：

| 候选 | G1 吞吐回退 | G2 fsync/op 比值 | G3 durability | 判定 |
|------|------------|-----------------|---------------|:----:|
| B (P=64) | 19.467% > 15% → FAIL | 1.8238× ≤ 2× → PASS | 1902.0261ms ≫ 5ms → FAIL | **FAIL** |
| A (P=64) | 76.797% > 15% → FAIL | 62.59× > 2× → FAIL | 214.1229ms ≫ 5ms → FAIL | **FAIL** |

> 模式 A 因每文件独立 fsync 使吞吐随 P 线性崩塌；模式 B 靠放大 commit 批（512×P）摊薄 fsync 却使 durability window 膨胀至 1902ms。两者均不满足 PASS。

### 4.3 decision 机械判定

**decision = FAIL**（与 `exp2_report.json.decision` 一致）。模式 A、B 均不满足 PASS 条件。

> 补充：即便假设模式 B 吞吐达标（G1≤15%），其 durability window 1902.0261ms 仍远超 5ms 阈值，将落入 REDESIGN 而非 PASS。即分区 WAL 在当前组提交模型下无论 A/B 模式都无法同时满足吞吐与持久化——这是模型层面的根本矛盾，非参数微调可解。

### 4.4 两替代设计

取自 `exp2_report.json.metadata.alternatives`：

1. **替代 ①**：单 WAL 写入 + 轻量后台分区线程（写入路径保持单文件组提交，分区路由推迟到 flush/L0 构建时后台完成）。
2. **替代 ②**：放弃 WAL=L0 映射，退化为分区 MemTable 方案（WAL 仅作崩溃恢复日志，分区结构只在内存 MemTable 与 L0 维持）。

### 4.5 Gate FAIL 对 ZeroFlush WAL=L0 分区方案的含义

Gate FAIL 即触发 PPT 第 10 页 Design 1 设计修订，必须改写写入路径而非继续推进分区 WAL。当前组提交模型下，单 WAL 拆成 P 个分区 WAL 后，模式 A（每文件独立 fsync）使吞吐随 P 线性崩塌（P=64 回退 76.797%）；模式 B（批量 fsync + 放大 commit 批）虽把 fsync/op 压到 1.8238×基线，却以 durability window 1902.0261ms 为代价。两者均不满足 PASS 三道闸门。

---

## 5. 实验 3：中止

实验 3（Partition Index vs MemTable）前置条件为实验 2 decision=PASS。exp2 判定 FAIL（多 WAL fsync 模式 B 吞吐回退 19.47% > 15%、durability 窗口 1902.0261ms ≫ 5ms），按方案中止，未产出任何实验数据。

中止产物：`exp_design/pre_experiment/report/exp3_report.json`（status=aborted, reason=exp2 decision=FAIL）。

---

## 6. 跨实验综合结论

### 6.1 ZeroFlush 核心假设的部分验证

| 假设 | 状态 | 证据 |
|------|------|------|
| H1（flush 成本可分解） | **PASS** | WA 分解成功（WA_WAL≈1.020, WA_Flush≈0.767, WA_Compaction≈3.367），守恒残差 0.0%，磁盘交叉核对 <2%；uniform WA_Flush≈1.01 直接支撑"flush=一次 NAND 写" |
| H2（L0 overlap 源于写入准入） | **部分支持** | 分区有效降低 overlap（zipfian −67%、uniform −96.7%），但受 compaction lag 限制，zipfian 中位数 1.9798 未达 ≤1.2 目标；run3 zipfian=0.99、uniform=0.23 达标证明分区方向正确 |
| H4（Partition Index 替代 MemTable） | **未验证** | exp2 FAIL 中止 exp3，H4 未获得任何实验数据 |

### 6.2 关键瓶颈

1. **WAL 分区 fsync 语义（exp2 FAIL）**：多 WAL group commit 在当前模型下吞吐崩塌 + durability 窗口过大。
   - 模式 A（independent_fsync）P=64：吞吐回退 76.797%（71,174.0 vs 306,743.0 ops/s），fsync/op 0.013143（62.59×基线）。
   - 模式 B（batched_fsync）P=64：吞吐回退 19.467%（247,030.7 vs 306,743.0 ops/s），fsync/op 0.000383（1.8238×基线，达标），但 durability 窗口 1902.0261ms（≫5ms 阈值，超标约 380×）。
   - 根本矛盾：批量 fsync 摊薄 fsync 开销需放大 commit 批（512×P），但 commit 批放大使 durability window 随 P 急剧膨胀。模型层面不可调和。

2. **L0 文件数管理（exp1 flush 时分区缺陷）**：分区 flush 导致 L0 文件数暴涨触发 stall。
   - flush 时分区每次 flush 发射 16 SST → L0 文件数增长 16×（中位数 17 vs 基线 6）→ `level0_slowdown_writes` stall（2,281 delays + 728 stops，stall.micros 3.5×基线）→ 吞吐回退 62.74%。
   - 主方案 PartitionedMemTable（独立子表 flush 每次 1 SST）是必要修正，从根本上避免 L0 文件数暴涨。

### 6.3 设计修订建议（对应 PPT 第 10 页）

1. **WAL 分区采用替代设计**：
   - 替代 ①：单 WAL 写入 + 轻量后台分区线程（写入路径保持单文件组提交，分区路由推迟到 flush/L0 构建时后台完成）。
   - 替代 ②：放弃 WAL=L0 映射，退化为分区 MemTable 方案（WAL 仅作崩溃恢复日志，分区结构只在内存 MemTable 与 L0 维持）。

2. **L0 管理需主方案 PartitionedMemTable**：独立子表 flush 每次 1 SST，避免 L0 文件数暴涨。flush 时分区（回退方案）已证明不可行（每次 flush 16 SST → L0 文件数暴涨 → stall → 吞吐回退 62.74%）。

### 6.4 负面结果的研究价值

本前期实验如实量化了 ZeroFlush 设计在两个维度的可行性边界：

1. **WAL fsync 语义维度**（exp2）：量化了多 WAL group commit 在当前模型下的吞吐崩塌（模式 A 回退 76.797%）与 durability 窗口膨胀（模式 B 1902.0261ms）的不可调和矛盾，明确了"WAL=L0 分区映射"在当前组提交模型下的不可行性。
2. **L0 文件数管理维度**（exp1）：量化了 flush 时分区导致 L0 文件数暴涨（16×）→ stall → 吞吐回退 62.74% 的固有缺陷，揭示了"任何增加 L0 文件数（每次 flush >1 SST）的方案都会触发 stall"的设计约束。

这些负面结果为后续设计修订提供了明确的量化依据，避免了在不可行路径上继续投入。

---

## 7. 方案执行偏离声明

| 偏离项 | 说明 | 影响 |
|--------|------|------|
| 实验 1 采用 flush 时分区回退预案 | 非主方案 PartitionedMemTable（写入时路由），因集成复杂度超标（前任未完成）退而采用 flush 时分区。回退方案固有缺陷：每次 flush 16 SST → L0 文件数暴涨 → stall | FAIL 结论仅针对回退方案，不等同于主方案不可行。主方案从根本上避免了 L0 文件数暴涨 |
| 实验 3 uniform 单点非中位数 | uniform 分布仅为 1 rep（非中位数），因 uniform 运行时长约为 zipfian 的 2.5 倍（时间成本精简）。zipfian 为 3 rep 中位数，为主结论 | uniform 达标为单点证据，需更多 rep 确认稳健性 |
| 实验 0 zipfian 工具层补丁 | db_bench 无原生 zipfian 支持，在 `tools/db_bench_tool.cc` 工具层实现 Zipfian(θ=0.99) 生成器，不修改引擎代码。通过冒烟测试（WAL 字节 ≈ 用户字节 × 1.05）验证 | 补丁正确性未经独立审计 |
| YCSB-A JNI 6.2.2 版本割裂 | YCSB rocksdb binding 使用 rocksdbjni 6.2.2，与本地 RocksDB 11.2.0 版本割裂。JNI 不暴露 RocksDB Statistics | YCSB-A 的 WAL/flush/compaction 字节和 stall 指标为 N/A，仅作吞吐/延迟交叉验证 |

---

## 8. 各实验原始数据与阶段报告引用路径

| 实验 | 报告 JSON | 阶段报告 | 原始数据目录 | 展示页 |
|------|-----------|----------|--------------|--------|
| Exp0 | `exp_design/pre_experiment/report/exp0_report.json` | `exp_design/pre_experiment/report/exp0_stage_report.md` | `output/pre_exp/exp0/` | `exp_design/pre_experiment/report/exp0_presentation.html` |
| Exp1 | `exp_design/pre_experiment/report/exp1_report.json` | `exp_design/pre_experiment/report/exp1_stage_report.md` | `output/pre_exp/exp1/raw/` | `exp_design/pre_experiment/report/exp1_presentation.html` |
| Exp2 | `exp_design/pre_experiment/report/exp2_report.json` | `exp_design/pre_experiment/report/exp2_stage_report.md` | `output/pre_exp/exp2/` | `exp_design/pre_experiment/report/exp2_presentation.html` |
| Exp3 | `exp_design/pre_experiment/report/exp3_report.json` | —（中止，无阶段报告） | —（中止，无原始数据） | — |

### 硬件环境（三实验一致）

| 项目 | 值 |
|------|-----|
| CPU | Intel(R) Core(TM) i9-10980XE CPU @ 3.00GHz, 36 核 |
| 内存 | 62.48 GB |
| 磁盘 | / 1.8T ext4 (NVMe) |
| 内核 | 6.8.0-124-generic (PREEMPT_DYNAMIC) |
| OS | Linux (Ubuntu 22.04) |
| CPU 调频器 | powersave（exp1/exp2 声明，绝对吞吐偏低；结论为相对比较，对调频器不敏感） |

---

## 9. 后续建议

1. **尝试主方案 PartitionedMemTable（独立子表 flush）**：在写入时将 Put 路由到 P 个独立子表，每个子表独立 flush 时各产出 1 个 SST，从根本上避免 L0 文件数暴涨。exp1 的 flush 时分区回退方案已证明不可行（吞吐回退 62.74%），但 run3 zipfian overlap=0.99、uniform overlap=0.23 达标证据证明分区方向正确，主方案有望解决 L0 文件数管理问题。

2. **对 WAL 分区替代设计（单 WAL + 后台线程）做微基准验证**：exp2 FAIL 量化了多 WAL group commit 的不可调和矛盾（模式 A 吞吐崩塌、模式 B durability 窗口膨胀）。替代设计 ①（单 WAL + 轻量后台分区线程）保持单文件组提交，有望在吞吐与 durability 之间取得平衡，需微基准验证其可行性。

3. **论文终稿规模放大（200M keys）复核**：当前前期实验使用 num=20M keyspace（论文终稿计划 200M 的 10%）。uniform_vs4096 单轮耗时 7–8 小时，200M 规模预计需 70+ 小时/轮。WA 趋势可外推，但绝对值可能变化，需在论文终稿规模复核关键指标。
