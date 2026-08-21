# 实验 2：分区开销门（Partition Overhead Gate）— 阶段性详细报告

- **实验编号**：exp2_partition_overhead_gate
- **报告日期**：2026-08-04
- **对应方案文档**：`exp_design/pre_experiment/plan/ZeroFlush_实验方案与执行提示词_Exp0-3.docx`（实验 2 章节）
- **对应 PPT**：`exp_design/pre_experiment/plan/ZeroFlush_Presentation.pptx` 第 10 页「Design 1」两个风险点
- **权威数据源**：`exp_design/pre_experiment/report/exp2_report.json`（严格 schema）
- **原始数据**：`output/pre_exp/exp2/`（raw/ 56 个原始 JSON、logs/、aggregated.json、decision.json；`raw_polluted_0032/` 为被干扰的作废数据，仅参考不展示）
- **机械判定结果**：**FAIL**

---

## 1. 实验目的与假设

ZeroFlush 的核心设计假设「WAL = L0 分区映射」要求把单个 WAL 文件拆成 P 个分区 WAL，每条 Put 在写入前需先做一次 L1 边界 range lookup 定位分区。本实验作为该设计路径的**可行性门**，量化两个被怀疑的开销点：

- **风险点 (a) — range lookup 延迟**：每次 Put 附加的分区定位查找是否会成为热路径瓶颈。预期在 P≤256、p99 延迟 ≤ 100 ns 时可接受（纳秒级、远小于一次 fsync）。
- **风险点 (b) — 多 WAL fsync / 组提交语义崩塌**：单 WAL 拆成 P 个分区 WAL 后，若每个 dirty 分区文件各自 fsync（模式 A），fsync 次数随 P 线性放大；若统一批量 fsync（模式 B），虽可摊薄每文件 fsync，但 commit 批放大为 `base_batch × P` 会使 durability window 膨胀。预期存在某种模式使 `sync_wal=true` 时吞吐回退可控且持久化窗口可接受。

**机械决策规则**（取自 `decision.py` 与 `exp2_report.json` 的 `metadata.decision_referenced_values.limits`，三道闸门须全部满足方为 PASS）：

| 闸门 | 条件 | 阈值 |
|------|------|------|
| G1 吞吐回退 | 模式 B(P=64) 相对单 WAL 基线回退 ≤ | 15% |
| G2 fsync/op | 模式 B(P=64) fsync/op ≤ | 2 × baseline |
| G3 持久化窗口 | 模式 B(P=64) durability_window ≤ | 5 ms |

- **PASS**：G1 ∧ G2 ∧ G3 同时成立。
- **REDESIGN**：G1 ∧ G2 成立但 G3 不成立（持久化窗口不可接受）；或边界情形（B 不达标但 A 达标）。
- **FAIL**：模式 A、B 均不满足 PASS 条件。

---

## 2. 方法学

### 2.1 Part 1 — range lookup 延迟测量（`range_lookup_bench.cc`）

- **两种实现**：
  - `binary_search`：P−1 个分隔边界存于连续数组，`upper_bound` 二分查找，cache-line 友好。
  - `small_btree`：fanout=8 的静态小 B-tree，节点 `alignas(64)`，P≤256 时树深 ≤3，叶子内 ≤8 键线性扫描（分支预测友好）。
- **key 空间**：16B 大端数值序 key，keyspace = 2^24，均匀划分为 P 个 range，分隔边界 = range j 起点（j=1..P−1），即 L1 边界模拟。
- **查询分布**：**zipfian(θ=0.99)**（按任务要求；range lookup 侧用 zipfian 制造热点边界访问）。
- **测量协议**：每组合预生成 `warmup(1,000,000) + queries(10,000,000)` 个 key 序列；前 1,000,000 次预热**丢弃不统计**，后 10,000,000 次统计 mean/p99。
- **计时**：`rdtsc` cycles 经 `CLOCK_MONOTONIC` 校准换算 ns（invariant TSC，校准频率 `tsc_freq_hz` 见原始 JSON）。
- **绑核**：测量线程 `pthread_setaffinity_np` 绑定 CPU 5。
- **干扰控制**：运行前采样 `/proc/stat` 两次（1 s 间隔）算 CPU busy 占比，且 `/proc/loadavg` load1 ≤ 阈值（取 `max(2.0, 0.15×核数)`）才启动。
- **组合**：P ∈ {64, 128, 256} × impl ∈ {binary_search, small_btree} = 6 组合，每组合 3 次（取中位数）。

### 2.2 Part 2 — multi-WAL fsync / 组提交测量（`multi_wal_fsync_bench.cc`）

- **harness**：P 个 WAL 文件（`fallocate` 预分配），每个 Put 先做 range lookup 定位分区再写入对应 WAL；4 个 writer 线程（绑核 6 起），commit 线程不绑核。
- **组提交模型**：
  - **模式 A — independent_fsync**：每个 dirty 分区文件独立参与组提交，每次 commit 轮（每 `base_batch=512` ops 一轮）对每个 dirty 文件各自 `fsync()`。
  - **模式 B — batched_fsync**：所有 dirty 分区文件统一批量 `fsync()`；commit 轮放大为 `base_batch × P` ops，用更大的组提交摊销每文件 fsync 开销；并记录 durability window（最早未落盘记录的写入时刻 → fsync 完成时刻）。
- **key 分布**：**uniform over 2^24**（刻意制造 worst-case dirty 扇出；若用 zipfian(0.99) 会把约 96% 质量压到 P=64 的 0 号分区，**低估** fsync 扇出开销）。
- **干扰控制**：每轮运行前 `sync()` 落盘 + 检查 `/proc/loadavg`、CPU busy、`/proc/meminfo` 的 `Dirty:` 脏页量（≤512 MB）及 nvme in-flight，确保系统空闲。
- **ops 自适应**：
  - `sync=true` 档：pilot 速率 × 10 s 目标迭代重标定，clamp 到 [50k, 8M]，保证每组合实测 ≥ 10 s。
  - `sync=false` 档：取 [2M, 4M] ops（满足 spec ≥2M ops；上限压低以避免脏页回写风暴——实测 22M ops 触发限速、单轮 30–90 s 且干扰后续组合，故该档以 ops 数而非 10 s 时长为准）。
- **durability_window_ms**：取各轮 p99 的中位数；统计时丢弃首轮（启动/预热效应）。
- **组合**：P ∈ {1, 16, 64} × mode ∈ {A, B} × sync ∈ {true, false} = 12 组合，每组合 3 次（取中位数）。其中 P=1 & mode=A 即 baseline_single_wal（sync true/false 两档）。运行顺序：sync=true 全部先跑（判定关键档），避免 nosync 大量脏页回写干扰。
- **复现性**：每组合 ≥3 次，报告中位数值；aggregated.json 另含每组合极差（range）与逐 rep 明细供对账。

### 2.3 硬件与环境（取自 `metadata.hardware`）

| 项 | 值 |
|----|----|
| CPU | Intel(R) Core(TM) i9-10980XE @ 3.00GHz, 36 核 |
| 内存 | 62.48 GB |
| 磁盘 | / 1.8T ext4（mount opts: rw,relatime,errors=remount-ro） |
| 内核 | 6.8.0-124-generic（PREEMPT_DYNAMIC） |
| CPU 调频器 | **powersave**（实测低频运行） |
| I/O 调度器 | none |
| reps | 3 |
| value_size | 1024 B |

---

## 3. 测试数据表

> 全部数值严格取自 `exp2_report.json`。`sync_wal=true` 档为判定关键档；`false` 档列关键值以对照。

### 3.1 range_lookup（6 组合）

| P | impl | mean_ns | p99_ns |
|----|------|--------:|------:|
| 64 | binary_search | 26.123 | 66.0 |
| 64 | small_btree | 27.642 | 78.0 |
| 128 | binary_search | 32.673 | 81.0 |
| 128 | small_btree | 31.274 | 87.0 |
| 256 | binary_search | 37.121 | 89.0 |
| 256 | small_btree | 33.335 | 90.0 |

（p99 全部 ≤ 90 ns < 100 ns 阈值；aggregated.json 另含 `mean_ns_range` / `p99_ns_range` 极差，最大 5.0 ns。）

### 3.2 multi_wal_fsync（12 组合 + baseline 2 档）

| P | mode | sync_wal | throughput (ops/s) | fsync/op | durability_window p99 (ms) |
|---|------|---------:|-------------------:|---------:|--------------------------:|
| 1 | A independent_fsync | true  | 306,743.0 | 0.00021   | 44.5179 |
| 1 | A independent_fsync | false | 2,185,463.8 | 0.0    | 4.9355 |
| 1 | B batched_fsync     | true  | 298,867.9 | 0.00022   | 55.9321 |
| 1 | B batched_fsync     | false | 2,099,587.5 | 0.0    | 5.9725 |
| 16 | A independent_fsync | true  | 135,256.5 | 0.003423  | 149.7172 |
| 16 | A independent_fsync | false | 2,424,634.6 | 0.0    | 5.2446 |
| 16 | B batched_fsync     | true  | 226,244.5 | 0.000387  | 583.5687 |
| 16 | B batched_fsync     | false | 1,890,751.4 | 0.0    | 41.6155 |
| 64 | A independent_fsync | true  | 71,174.0  | 0.013143  | 214.1229 |
| 64 | A independent_fsync | false | 2,284,206.6 | 0.0    | 4.1969 |
| 64 | B batched_fsync     | true  | 247,030.7 | 0.000383  | 1902.0261 |
| 64 | B batched_fsync     | false | 1,816,119.1 | 0.0    | 289.1641 |

**baseline_single_wal**（P=1, mode A）：

| sync_wal | throughput (ops/s) | fsync/op |
|----------|-------------------:|---------:|
| true     | 306,743.0          | 0.00021  |
| false    | 2,185,463.8        | 0.0      |

**nosync 档关键值对照**：P=1 两模式 nosync 吞吐 ≈2.10–2.19M ops/s、窗口 4.9–6.0 ms；P=64 A(false) 仍达 2.28M ops/s（无 fsync 时分区本身不损吞吐）；P=64 B(false) 降至 1.82M ops/s 且窗口 289.16 ms（commit 批=512×64=32768 ops，即便无 fsync 也使窗口膨胀）。

---

## 4. 分析

### 4.1 吞吐 vs P（模式 A/B，sync=true）

见 `charts/exp2_throughput_vs_p.svg`。

- 基线 P=1 模式 A sync=true = **306,743 ops/s**（= baseline_single_wal）。
- **模式 A（independent_fsync）**：随 P 近线性塌陷——P=16 → 135,256.5（−55.9%），P=64 → 71,174.0（−76.8%）。原因：每 commit 轮对每个 dirty 文件各自 fsync，fsync 次数随 P 与 dirty 扇出线性放大，fsync/op 从 0.00021（P=1）涨到 0.013143（P=64，约 62.6×基线）。
- **模式 B（batched_fsync）**：随 P 缓慢下降——P=16 → 226,244.5，P=64 → 247,030.7（回退 19.47%）。批量 fsync 把 fsync/op 压到 0.000383（P=64，仅 1.82×基线，达标），但代价是 commit 批放大为 `512×P`。
- PASS 吞吐阈值线（回退 ≤15%）= 306,743 × 0.85 = **260,731.5 ops/s**。模式 B P=64 = 247,030.7 < 260,731.5，**未达 G1**。

### 4.2 fsync/op vs P（模式 A/B，sync=true，对数轴）

见 `charts/exp2_fsync_per_op_vs_p.svg`。

- 模式 A fsync/op：0.00021 → 0.003423 → 0.013143，随 P 近线性增长（每多一倍 dirty 扇出即多一倍 fsync），P=64 时 0.013143 / 0.00021 ≈ **62.59×基线**，远超 2× 阈值（G2 FAIL）。
- 模式 B fsync/op：0.00022 → 0.000387 → 0.000383，P=64 时 1.82×基线 ≤ 2× 阈值（**G2 PASS**），证明批量 fsync 有效摊薄每文件 fsync 开销。
- 2× 基线阈值线 = 0.00042。模式 B 全档在其下，模式 A P=16 起远在其上。

### 4.3 durability window vs P（模式 A/B，sync=true，对数轴）

见 `charts/exp2_durability_vs_p.svg`。

- 模式 A：44.52 → 149.72 → 214.12 ms，随 P 上升但受限于 commit 批小（每轮 512 ops）。
- 模式 B：55.93 → 583.57 → **1902.03 ms**，commit 批 = 512×P 使最早未落盘记录等待时间随 P 急剧膨胀；P=64 时窗口 1902 ms ≫ 5 ms 阈值（**G3 FAIL**，超标约 380×）。
- 即便 nosync，模式 B P=64 窗口仍达 289.16 ms（commit 批放大本身即膨胀窗口，与 fsync 无关）。

### 4.4 range lookup 延迟 vs P（binary_search / small_btree）

见 `charts/exp2_range_lookup_vs_p.svg`。

- 两种实现 mean 随 P 缓慢上升（64→256）：binary_search 26.1→37.1 ns，small_btree 27.6→33.3 ns。
- p99 全部 66–90 ns，**均 < 100 ns 阈值**（风险点 a 达标）。
- P=128/256 时 small_btree 略优于 binary_search（树深浅、cache 局部性更好）；P=64 时 binary_search 略优（数组连续、二分分支少）。两者差距很小，非瓶颈。

### 4.5 与预期对比

| 风险点 | 预期 | 实测 | 结论 |
|--------|------|------|------|
| (a) range lookup p99 | < 100 ns | 66–90 ns | 达标 |
| (b) 模式 A 吞吐回退 | 可控 | −76.8%（P=64） | 远超 15%，崩塌 |
| (b) 模式 B 吞吐回退 | ≤15% | 19.47%（P=64） | 超 15%，不达标 |
| (b) 模式 B fsync/op | ≤2×基线 | 1.82× | 达标 |
| (b) 模式 B durability | ≤5 ms | 1902 ms | 严重超标 |

range lookup 风险消除，但 multi-WAL fsync 在两种组提交模型下均不满足 PASS：模式 A 因每文件独立 fsync 使吞吐崩塌；模式 B 靠放大 commit 批摊薄 fsync 却牺牲持久化窗口。

---

## 5. decision 机械判定（逐步核对）

基线 baseline = P=1, independent_fsync, sync=true：throughput = 306,743.0 ops/s，fsync/op = 0.00021。

### 候选 B = P=64, batched_fsync, sync=true

| 闸门 | 计算 | 阈值 | 判定 |
|------|------|------|------|
| G1 吞吐回退 | (306743−247030.7)/306743 ×100 = **19.467%** | ≤15% | **FAIL** |
| G2 fsync/op | 0.000383 / 0.00021 = **1.8238×** | ≤2× | PASS |
| G3 durability | **1902.0261 ms** | ≤5 ms | **FAIL** |

→ G1 不满足 ⇒ 既非 PASS（需 G1∧G2∧G3）亦非 REDESIGN（需 G1∧G2 同时成立但 G3 不成立）。模式 B 不通过。

### 候选 A = P=64, independent_fsync, sync=true

| 闸门 | 计算 | 阈值 | 判定 |
|------|------|------|------|
| G1 吞吐回退 | (306743−71174)/306743 ×100 = **76.797%** | ≤15% | **FAIL** |
| G2 fsync/op | 0.013143 / 0.00021 = **62.59×** | ≤2× | **FAIL** |
| G3 durability | **214.1229 ms** | ≤5 ms | **FAIL** |

→ 三闸全 FAIL。

### 结论

模式 A、B 均不满足 PASS 条件 ⇒ **decision = FAIL**（与 `exp2_report.json.decision` 一致）。

**补充**：即便假设模式 B 吞吐达标（G1 ≤15% 成立），其 durability window 1902.0261 ms 仍远超 5 ms 阈值，将落入 **REDESIGN** 而非 PASS。即在当前组提交模型下，分区 WAL 无论 A/B 模式都**无法同时满足吞吐与持久化**——这是模型层面的根本矛盾，非参数微调可解。

---

## 6. 结论

**Gate FAIL** 对 ZeroFlush「WAL = L0 分区映射」方案的含义：

1. **当前组提交模型下不可行**：单 WAL 拆成 P 个分区 WAL 后，模式 A（每文件独立 fsync）使吞吐随 P 线性崩塌（P=64 回退 76.8%）；模式 B（批量 fsync + 放大 commit 批）虽把 fsync/op 压到 1.82×基线，却以 durability window 1902 ms 为代价。两者均不满足 PASS 三道闸门。
2. **触发 PPT 第 10 页 Design 1 设计修订**：FAIL 即触发设计修订，必须改写写入路径而非继续推进分区 WAL。
3. **替代设计**（取自 `exp2_report.json.metadata.alternatives`）：
   - **替代 ①**：单 WAL 写入 + 轻量后台分区线程（写入路径保持单文件组提交，分区路由推迟到 flush/L0 构建时后台完成）。
   - **替代 ②**：放弃 WAL=L0 映射，退化为分区 MemTable 方案（WAL 仅作崩溃恢复日志，分区结构只在内存 MemTable 与 L0 维持）。
   - （`decision.py` 中另列第三备选：模式 B 改良——批量 fsync 但改用时间上限封顶的 commit 窗口如 ≤2 ms，以牺牲少量 fsync 摊销换取 durability window 达标；该方案未进入最终 report JSON 的 alternatives，作为后续可探索方向。）

---

## 7. 风险与局限

1. **powersave 调频器**：CPU 调频器为 `powersave`（实测低频运行），rdtsc 延迟经 `CLOCK_MONOTONIC` 校准换算 ns（invariant TSC），但绝对吞吐数值偏低且可能放大 fsync 相对开销；结论（FAIL）对调频器不敏感——回退幅度与 durability window 超标为数量级差异，非调频器可解释。
2. **基线 rep 方差**：P=1 模式 A sync=true 基线聚合用了 5 个 rep（见 `ops_per_combo.p1_independent_fsync_true.ops_per_rep`），throughput 极差 287,503.1 ops/s（rep1 仅 152k，rep3 达 439k），中位数 306,743.0。基线方差较大主要源于 sync=true 下 fsync 抖动；但即便取最乐观基线 rep，模式 B 19.47% 回退仍 >15%，结论稳健。
3. **nosync ops 数选择**：`sync=false` 档取 [2M, 4M] ops 而非 10 s 时长，是为避免脏页回写风暴（实测 22M ops 触发限速、单轮 30–90 s 并干扰后续组合）。该选择满足 spec ≥2M ops 硬性要求，但 nosync 绝对吞吐可能略低于「更长跑时长」估值；不影响判定（判定只依赖 sync=true 档）。
4. **桌面级 NVMe 无掉电保护**：测试盘为桌面级 NVMe（1.8T ext4），无企业级掉电保护电容，`fsync()` 实测延迟与 durability window 可能与企业级 NVMe 有差异；但本实验比较的是「同一硬件上单 WAL vs 多 WAL」的相对退化，硬件绝对性能不影响 FAIL 结论的方向。
5. **range lookup 仅测边界查找**：本实验只测 L1 边界 range lookup 延迟，未含实际 SST 元数据查找；但该项已达标（p99 ≤90 ns），非瓶颈，无需进一步细化。
6. **key 分布选择**：multi-WAL 侧用 uniform（worst-case dirty 扇出）而非 zipfian，是有意制造最坏情况；真实负载若偏斜，dirty 扇出更小、模式 A 退化更轻，但模式 B 的 durability window 膨胀与 commit 批放大的矛盾依旧存在。

---

## 8. 原始数据引用路径（供审查对账）

| 用途 | 路径 |
|------|------|
| 权威报告 JSON（本报告数值来源） | `exp_design/pre_experiment/report/exp2_report.json` |
| 聚合数据（含逐 rep 明细 + 极差） | `output/pre_exp/exp2/aggregated.json` |
| 机械判定输出 | `output/pre_exp/exp2/decision.json` |
| range_lookup 原始（18 个） | `output/pre_exp/exp2/raw/range_p{64,128,256}_{binary_search,small_btree}_rep{1,2,3}.json` |
| multi_wal 原始（38 个） | `output/pre_exp/exp2/raw/wal_p{1,16,64}_{A,B}_{true,false}_rep*.json` |
| 运行日志 | `output/pre_exp/exp2/logs/*.log` |
| 图表生成脚本 | `exp_design/pre_experiment/report/charts/gen_exp2_charts.py` |
| 图表（SVG） | `exp_design/pre_experiment/report/charts/exp2_*.svg` |
| 作废数据（被干扰，不展示） | `output/pre_exp/exp2/raw_polluted_0032/` |
| 实验代码 | `exp_design/pre_experiment/code/exp2_partition_gate/` |
| 公共库 | `exp_design/pre_experiment/code/common/py/exp_common.py` |

> 图表说明：本机 matplotlib 不可用（`python3 -c "import matplotlib"` 失败，pip 安装受网络限制未成功），故按约束采用降级方案——用纯 Python 标准库生成 **SVG 矢量图表**（浏览器原生支持 `<img>` 渲染），保存为独立 `.svg` 文件由 HTML 相对路径引用，结构等同 PNG 方案。
