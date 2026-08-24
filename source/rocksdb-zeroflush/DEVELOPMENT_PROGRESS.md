# ZeroFlush 开发进展文档

> **文档定位**：ZeroFlush 后续开发工作的参考基线，汇总截至 2026-08-23 的全部里程碑、基准数据与优化路线
> **代码仓库**：`source/rocksdb-zeroflush`（基于 RocksDB 9.x）
> **模块目录**：`zeroflush/`（核心实现）· `tools/zf_test.cc`（回归测试，35 例）

---

## 0. M4 系列完成状态（2026-08-23）

**终态架构已完整落地**（用户设计的"WAL 即 L0"）：写路径直写分区 WAL + 每分区跳表索引（绕开 MemTable）、L1 对齐分区、批次 freeze、内存预算背压、迭代器分区化、旧路径删除。

| 里程碑 | 内容 | 状态 |
|---|---|---|
| M4.0 | 范围路由接线 + kSampled 学习期修复 | ✅ 8df80a7 |
| M4.1 | 写路径去串行化（O(1) ShouldSeal、并发跳表、parallel 两分支、4 bug 修复） | ✅ 11a9ac9 |
| M4.2b | L1 对齐分区（compaction 感知分区） | ✅ 9897619 |
| M4.3a | 分区索引（PartitionIndexSet，绕开 MemTable） | ✅ b4d2cdf |
| M4.3c/d | 单分区→批次 freeze、背压、迭代器分区化、开关翻转、正确性修复链 | ✅ c5592cd 等 |
| M4.4 | 用例 38-44、读路径复测（cache 256MB） | ✅ 3a41963 等 |
| M4.5 | 消除 upper_conflict 回落放大器（2.2GB 104.6K/零停写） | ✅ cbb3c88 |
| M4.5b | 攒批物化（kSkip）——三次调试根因锁定（迭代器 SV 与物化安装竞态），三次回滚（回归 35/35 恢复） | ⏸ 待修复 |

**M4 基准（2.2GB，终态路径）**：fillrandom **105.7~111.6K ops/s**（cache 8MB→256MB）、零停写、P50 128us、P99 239us、写放大 0.72（与原生持平）、数据完整性五连验证 PASS、**回归 35/35**。

**50GB 尺度（R20 稳定性/完成度验收，2026-08-24）**：**完整跑通**（rc=0、1.9h）
——强化后台（subcompactions=16 + max_background_jobs=24 + cache 512MB）
加快 L0 消费，越过 R8/R19 的停滞点。**结果**：28,812 ops/s（R8 22.75K
+27%）、写放大 0.73（优于原生 0.81）、停写 247（未循环停滞）、RSS 稳定
1.2GB、**读回命中率 63.3% ≈ 期望**（数据完整）。回落 L0 循环的根治（攒批
物化）仍为后续方向（R19 34GB 停滞为历史对照）。

---

## 1. 项目概述

**ZeroFlush** 是基于 RocksDB 的存储引擎改造实验，核心设计：**memtable 仅存 key + SlimLocator（WAL 位置指针），value 原样驻留于 64 个分区 WAL；物化（materialization）时才从 WAL 读回 value 写入 SST**，从而消除 memtable flush 的全量重写路径。

**设计目标**：
- **降低写放大**：SST 侧每 key 仅 ~16B（key+locator），compaction 仅搬运轻量索引，value 无逐层重写
- **提升读性能**：SST 精简 + Bloom 快速定位，value 从 WAL 定点读回，读长尾收敛

**当前阶段定位**：M3 主线开发已推进至 **M3.3 完成、M3.4 待启动**；期间插入了两轮基准测试（M1 基准、M3 基准）与测试平台建设，最新 M3 基准确认了空间效率与 vs256 读路径优势，同时暴露写吞吐 16~37 倍的差距，确立了 P3 物化路径优化为最高优先级方向。

---

## 2. 已完成工作清单（按时间线）

### 2.1 M1 — WAL 持久化修复 ✅

| 项 | 内容 |
|----|------|
| 缺陷 1 | `PartitionedWalManager` 析构丢弃未同步缓冲数据 → 新增 `Close()` flush 全部分区缓冲并 sync，析构调用 |
| 缺陷 2 | `NewWritableFile` 重开时截断既有 WAL → 改用 `ReopenWritableFile`（O_APPEND）防截断 |
| 回归测试 | `tools/zf_test.cc` 7 例（WALBufferFlush / ReopenNoTruncate / SequentialKeys / RandomKeysUnique / RandomKeysWithDup / MultiPartition / LargeSequential）全 PASS；发现并修复 `DestroyDB` 不清理 `zfwal` 子目录的跨用例污染（改用自定义 CleanDB） |
| 文档 | `zeroflush/M1_WAL_PERSISTENCE_FIX.md`；M1 基准报告（写快 1.33~1.73×） |

### 2.2 M3 — 版本设计文档 ✅

- 产出 `zeroflush/M3_DESIGN.md`（1103 行，16 章）：定义 M3.0 清偿 M2 债务 → M3.1 PartitionTable 范围路由 → M3.2 并行物化+跳 L0 直装 → M3.3 Materialize-into-BaseLevel 融合归并 → M3.4 多列族/Merge/DeleteRange → M3.5 CSD 卸载后端
- 修正 M2_DESIGN §7.3 错误分析（孤儿代问题的真实缺陷为 Recover()→Seal() 之间未登记 SealedFileCache 的读窗口）
- **M3.0 债务清偿**（2026-08-10）：孤儿封存代登记、IteratorPins 修复、10 项 `zf.*` 指标经 `GetProperty("rocksdb.zeroflush.*")` 暴露、ZFPROPS 写目录时序 + `--zf_filter` 解析 bug 修复

### 2.3 M3.1 — 版本开发 ✅

- `partition_table.h/cc`：PartitionTable（Route/RangeOf）+ PartitionTableSet 版本化管理
- ZFPROPS v2 变长格式（CRC 校验、v1/v2 自动检测）
- kStatic / kSampled 采样路由策略；`wal_manager` parts_ 改 unordered_map
- 新增 6 例测试（#17~#22），全量回归 **21/21 PASS**

### 2.4 M3.2 — 物化路径接入原生 FlushJob（三缺陷修复）✅

| # | 缺陷 | 修复 |
|---|------|------|
| 1 | ZF 分派路径不配对 `PickMemtable` 的 `base_->Ref()`，Version 引用泄漏 → DB 析构 CF version list 断言崩溃 | `ZfMaterializeAllEpochs` 成功/失败分支均补 `base_->Unref()` |
| 2 | `PickInstallLevel` 批内重叠检查只扫 `batch_outputs_`，同批文件互相不可见 → hash 模式分区键范围交错重叠直装同层，VersionBuilder 报 "L6 has overlapping ranges" | 定层循环内每文件定层后立即 push_back 进 `batch_outputs_` |
| 3 | kHash 模式 `boundaries_` 为空，`PartitionTable::RangeOf` 空 vector 越界崩溃 | 仅 `!IsHashMode()` 时调用 RangeOf |

**方法论沉淀**：与原生路径平行的 FlushJob 分支必须逐项对照原生 `WriteLevel0Table` 的生命周期配对（Ref/Unref、锁）与层间不变量（同层不重叠）。

### 2.5 M3.3 — Materialize-into-BaseLevel（融合归并）✅

- 物化输出按封存字节/待重写 base-level 字节触发比融合归并进 base level，避免 L0 堆积
- 当前 ZF 每分区物化产生 1 个 L0 文件的批量行为即由此路径而来

### 2.6 P0 — db_bench 每线程配额修正 ✅（2026-08-20）

- **根因**：db_bench 的 `--num`/`--writes`/`--reads` 均为**每线程**配额语义（源码 `DoWrite`：`num_ops = writes_ == 0 ? num_ : writes_`，每线程独立 Duration）。旧脚本误传总量 → 16 线程实际写入 800GB（16 倍放大）
- **修正**：`--num` 保持 key 空间总量（KeyGenerator 范围），`--writes = total/16`、`--reads = 1M/16`
- **验证**：mini 实测 `threads=2, writes=50000 → 100,000 operations` 精确吻合；原生测试从 11.3h/场景 降至 26.5min/场景

### 2.7 P1 — ZF stall 触发器适配 ✅（2026-08-20）

- **根因**：`--zf_partitions=64` 使 L0 恒保持 ~64 文件，超默认 `level0_stop_writes_trigger=36` → 持续 write stop（有效吞吐仅 0.8MB/s）
- **修正**：`--level0_slowdown_writes_trigger=64 --level0_stop_writes_trigger=72`（≥ 分区数）
- **效果**：L0 在 64↔128 间周期波动（flush 产 64 文件/轮 → 触顶停写 → compaction 消化 → 恢复），吞吐提升 2~4 倍至 1.4~3.5MB/s，消除持续卡死

### 2.8 基准测试平台 ✅

- `output/zeroflush_m3_perf/run_benchmark.py`：两引擎（native/zeroflush）× 两 value size（256B/1024B）× fillrandom+readrandom 编排，50GB/16 线程，JSON 落盘（含 P50/P75/P99/P99.9/P99.99 全百分位）、`--engine report` 自动生成 Chart.js HTML 报告
- 沉淀监控规范：`setsid nohup` 后台脱离、LOG 中 `lsm_state`/`flush_finished`/`Stopping writes` 解析、参数生效性验证（grep LOG 中 Options 打印值）

---

## 3. 待完成工作清单（按优先级）

| 优先级 | 工作项 | 内容 | 状态 |
|--------|--------|------|------|
| **M4.0（已完成 ✅ 2026-08-21）** | 范围路由启用 + 终态收益预演 | db_bench 接线（`--zf_routing/--zf_base_merge/--zf_partition_target_mb` 等）；**修复 kSampled 学习期 `table_version` 误标 bug + 新增用例 35（27/27 PASS）**；2.2GB 实测：**R3（sampled+融合+P=16）12,082 ops/s（+11% vs hash）、零停写、L1 重写 2.62GB 优于 hash（2.99GB）**；P=64 时融合因批量太小退化（L1 重写 4.4×、94 次停写）——**批量参数是融合归并生效的关键旋钮**。50GB R4 验证后台运行中。详见 `zeroflush/M4_DESIGN.md` §M4.0 | 验收：50GB R4 写放大 vs 0.70 |
| **M4.1（已完成 ✅ 2026-08-21）** | 写路径去串行化（原 P3-A） | O(1) ShouldSeal + SlimMemTableRep 并发化 + parallel 两分支注入（follower 并行插 WAL+跳表）+ 锁外编码；**修复 4 个 bug**（parallel follower 走原生 InsertInto 丢数据、Ref UAF、原生 flush 触发无 epoch imm、kSampled table_version）；27/27 回归。实测：**micro 110K+ ops/s、R3 全量 38.1K（3.2×）、P50 138us（9.3×）**；全量剩余瓶颈=L0 消费端（compaction 全程争抢）→ M4.3 | 验收达成（micro ≥100K + 全量 ≥3×） |
| M4.2 | 物化输入侧换跳表序 | imm 跳表区间遍历免排序 + WAL 段整读缓冲（剖析实测排序占物化耗时大头）；此输入侧即终态 L0→L1 输入侧 | 待开发 |
| M4.3 | 每分区 memtable + 全局 epoch 拆除（终态主体） | WAL 即 L0、永不物化：每分区跳表（无锁插入）、每分区 freeze→compact→释放、背压改跳表内存预算、Get/Iterator 每分区化（最大手术） | 待开发 |
| M4.4 | 读路径巩固 + 基准定稿 | P2 稳态复测重查读优势归因（"SST 只存 locator"解释已证伪）、value cache 可选、50GB 终态 vs 原生全量基准 | 待开发 |
| P2 | 配置调优复测 | 并入 M4.4（剖析证明触发器类调参对写吞吐零效果） | 待执行 |
| — | **写入吞吐目标** | 相同参数（50GB/16 线程/zf_partitions=64）同负载下，fillrandom 吞吐**接近或超过原生**（当前慢 16~37 倍：vs256 3,387 vs 124,125 ops/s；vs1024 1,919 vs 30,255 ops/s） | 目标 |
| — | **读取性能巩固目标** | 保持 **vs256 读快 8 倍**（96,554 vs 11,599 ops/s，已达成）；**vs1024 读快 4 倍**（当前受 fill 后 compaction 积压干扰仅 4,115 ops/s，需优化至 ~120K ops/s） | 目标 |
| M3.4 | API 完备性 | 多列族 / Merge / DeleteRange 支持（m34-1~m34-7） | 待启动 |
| M3.5 | CSD 卸载后端 | GDS 读取与设备侧排序骨架 | 未开始 |

---

## 4. 对比测试结果摘要

> 完整数据：`output/zeroflush_m3_perf/benchmark_native.json` / `benchmark_zeroflush.json` / `report.html`
> 深度分析：`source/rocksdb-zeroflush/benchmark_analysis_report.md`
> **写路径剖析（2026-08-21）**：`output/zeroflush_m3_perf/profiling/profiling_report.md` —— 2.2GB 四变体对照 + 微实验，主瓶颈定位为写路径 DB mutex 全序列化（非物化/非停写），P3 优先级已据此修订
> 条件：50GB 逻辑数据、16 线程、key 16B、无压缩、WAL 开启、cache 8MB、write buffer 256MB

### 4.1 Fillrandom（写路径）

| 指标 | vs256 原生 | vs256 ZF | 差距 | vs1024 原生 | vs1024 ZF | 差距 |
|------|-----------:|---------:|-----:|------------:|----------:|-----:|
| QPS (ops/s) | 124,125 | 3,387 | 36.6× | 30,255 | 1,919 | 15.8× |
| MB/s | 32.2 | 0.9 | 35.8× | 30.0 | 1.9 | 15.8× |
| P99 (us) | 1,646 | 4,237 | 2.6× | 4,189 | 9,355 | 2.2× |
| P50 (us) | 14.3 | 1,441 | 101× | 20.4 | 1,894 | 93× |
| w-amp | 0.81 | **0.70** | **-13.6%** ✓ | 0.82 | **0.70** | **-14.6%** ✓ |
| 耗时 | 26.5 min | **16.2 h** | 36.6× | 28.4 min | **7.5 h** | 15.8× |

### 4.2 Readrandom（读路径）

| 指标 | vs256 原生 | vs256 ZF | 对比 | vs1024 原生 | vs1024 ZF | 对比 |
|------|-----------:|---------:|-----:|------------:|----------:|-----:|
| QPS (ops/s) | 11,599 | **96,554** | **ZF 快 8.3×** ✓ | **30,250** | 4,115 | ZF 慢 7.4×（时序干扰†） |
| MB/s | 1.9 | **15.8** | 8.3× | **19.0** | 2.6 | — |
| P99 (us) | 14,433 | **248** | **ZF 快 58×** ✓ | 3,783 | 6,086 | 1.6× |
| P99.9 (us) | 161,321 | **358** | 450× | 49,778 | 6,576 | — |

† readrandom 紧跟 fill 执行，7.5h 写入产生的大量 compaction 仍在后台争抢 IO/CPU；需按 P2（读前等 compaction 清空）复测后方可定论。

### 4.3 空间占用

| 指标 | vs256 | vs1024 |
|------|------:|-------:|
| 原生 DB | 40.56 GB | 40.76 GB |
| ZF DB | **35.16 GB（-13.3%）** | **35.20 GB（-13.6%）** |

---

## 5. 性能优化方案详述

### 5.1 写入瓶颈根因（三个叠加因素）

1. **L0 周期性 write stop**：ZF 每轮物化批量产生 64 个 L0 文件（每分区 1 个）；flush 产生速率 > compaction 消费速率（单次 L0→L1 compaction 实测 170+s），L0 在 64↔128 波动，触顶 stop=72 即停写等待；
2. **物化 WAL 随机读回**：memtable 仅存 key+locator，物化需从 64 个 WAL 分区**逐条随机定点** `ReadValue` 读回全部 value——这是 fill P50 高达原生 100 倍的主要来源；
3. **物化单线程**：LOG 显示 `compaction_time_cpu ≈ compaction_time`（job 81：172s wall / 165s CPU），未利用 `max_background_jobs=16`。

磁盘全程非瓶颈（NVMe 953MB/s 能力 vs 原生实际 79MB/s、ZF <5MB/s）——**优化空间在软件路径**。

### 5.2 优化方案矩阵

| 方案 | 针对根因 | 预期收益 | 实施难度 | 依赖 |
|------|----------|----------|----------|------|
| **P3-a 物化并行化**（按分区多线程物化） | 根因 3 | 物化吞吐 ×N（N=并行分区数），直接缩短 stop 等待 | 中：需处理多分区并行的锁与 VersionEdit 安装顺序（借鉴 M3.2 教训：Ref/Unref 配对、批内重叠可见性） | 无 |
| **P3-b 物化顺序预读**（按 WAL 段顺序批量读 value） | 根因 2 | WAL 读回从随机 IO 转 4K/64K 顺序 IO，NVMe 下单次读放大收益 10~50× | 中低：物化前按 locator 排序或按段聚合读 | 无（与 P3-a 正交互补） |
| **P3-c value cache**（物化前 value 先入块缓存） | 读路径 + 根因 2 部分缓解 | 改善 vs1024 读（P50 3,654us → 亚毫秒级） | 低 | cache 调大（P2-a） |
| **P2-a cache 8MB→256MB** | 读路径 | 索引层命中率提升，两引擎 readrandom 均受益 | 低（配置） | 无 |
| **P2-b 读前等 compaction 清空** | 测试方法学 | 消除 §4.2† 时序干扰，获得稳态读基线 | 低（脚本） | 无 |
| **P2-c lz4 压缩** | 磁盘压力 | 顺序读吞吐提升，对 ZF 物化读回亦有收益 | 低（配置，两引擎同开） | 无 |
| **P2-d disable_wal 对照** | 方法论 | 分离 WAL 开销占比，量化 ZF"以 WAL 为主存储"的真实代价 | 低（配置） | 无 |
| **P3-d 分区数调优**（zf_partitions 64→16/32） | 根因 1 | L0 文件基数线性下降，compaction 追赶压力减小；但读并行度与热点分散能力下降 | 低（配置） | 需 A/B 复测 |

### 5.3 推荐迭代顺序

**P1（已完成，已验证有效）→ P3 物化优化（根本解法）→ P2 配置调优（收尾固化）**

理由：P1 已消除持续卡死但吞吐仅 0.9~1.9MB/s，距原生 30MB/s 的差距根源在物化路径（P3），配置调优（P2）无法弥补 20~40 倍的软件路径差距；P3-a+P3-b 完成后 P2 作为同条件复测与固化手段，避免在未优化的代码上调参得出误导性结论。

---

## 6. 后续行动计划

### 第一步：P3-b 顺序预读（1~2 天）

- **改动**：`db/flush_job.cc` `ZfMaterializeAllEpochs` 物化循环——物化前按 WAL 段聚合 locator，段内顺序批量读；`zeroflush/wal_manager` 增加段级批量读接口
- **验证**：`zf_test` 全量回归 + `zf.materialize_*` 指标（M3.0 已暴露）对比
- **预期**：fill P50 显著下降（随机 IO 消除），吞吐 2~5×

### 第二步：P3-a 物化并行化（3~5 天）

- **改动**：按分区划分物化子任务，多线程执行（复用 `max_background_jobs`），安装阶段串行保序（严格对照 M3.2 三缺陷清单：Ref/Unref 配对、batch_outputs_ 即时可见、hash 模式边界）
- **风险**：VersionEdit 安装顺序与层间不变量；需新增并发物化回归用例
- **预期**：与 P3-b 叠加后写吞吐 5~10×（3,387 → 17K~34K ops/s 区间），与原生差距收窄至 4~7×

### 第三步：P2 配置复测 + 读路径巩固（1 天）

- cache 256MB / lz4 / 读前等 compaction 队列清空，同条件复测两引擎
- 验收读目标：vs256 保持 ≥8×（96K ops/s 级）；vs1024 达成 4×（~120K ops/s 级）
- 产出修订版 `benchmark_*.json` 与 `report.html`，更新本文档基线

### 第四步：回到 M3.4 主线（API 完备性）

- m34-1~m34-7：多列族 / Merge / DeleteRange
- P3 完成后物化路径已稳定，M3.4 的 API 扩展可直接复用优化后的物化骨架

### 所需资源

- 现有开发环境即可（i9-10980XE 36 核 / 62GB / NVMe 2TB，磁盘非瓶颈已验证）
- `zf_test` 回归套件（21+ 例）、`run_benchmark.py` 平台、`zf.*` GetProperty 指标均已就绪
- 每轮优化后跑一轮 50GB 对比基准（原生 ~1h + ZF 优化后预期 ≤3h）

---

## 附录：关键文件索引

| 文件 | 说明 |
|------|------|
| `zeroflush/M3_DESIGN.md` | M3 总体设计（1103 行，M3.0~M3.5） |
| `zeroflush/M1_WAL_PERSISTENCE_FIX.md` | M1 修复文档 |
| `zeroflush/partition_table.h/cc` | M3.1 范围路由 |
| `db/flush_job.cc` | 物化路径（`ZfMaterializeAllEpochs`，P3 主战场） |
| `db/memtable.cc` | key+locator 存储（`zf_ctx_->ReadValue`） |
| `zeroflush/wal_manager.h/cc` | 分区 WAL 管理（P3-b 段级读接口落点） |
| `tools/zf_test.cc` | 回归测试套件 |
| `output/zeroflush_m3_perf/` | 基准数据与报告（JSON + HTML + 脚本） |
| `benchmark_analysis_report.md` | 本轮对比测试深度分析（2026-08-21） |
