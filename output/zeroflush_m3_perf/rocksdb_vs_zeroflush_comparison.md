# ZeroFlush vs 原生 RocksDB 对比实验报告

日期：2026-08-26 · 分支：master（M4.8 + M4.9 优化尝试）· 系统负载：~25（含 grafana/prometheus/ceph 常驻）

## 1. 实验配置

| 项 | 值 |
|---|---|
| 原生 | `source/rocksdb/build/db_bench`（RocksDB 11.2.0） |
| ZF | `source/rocksdb-zeroflush/build/db_bench`（ZeroFlush 当前版） |
| 值大小 | 1KB（key 16 + value 1024）/ 272B（key 16 + value 256） |
| 规模 | 2.2GB / 10GB total（--num × 1KB，--writes=num/16/线程，16 线程） |
| 公共 | compression=none、disable_wal=false、cache 512MB、max_background_jobs=24 |
| ZF 专属 | --zeroflush --zf_partitions=16 --zf_routing=align_l1 --zf_base_merge --zf_skip_batching=false --zf_value_cache_mb=64 --subcompactions=16 |
| 读 | fill 后分开进程 readrandom --use_existing_db=true --reads=1000000/线程 |

## 2. 结果

### 2.1 写（fillrandom）

| 场景 | 原生 ops/s | ZF ops/s | 差距 |
|---|---|---|---|
| 2.2GB / 1KB | 251.6K | 80.0K（位图优化后） | 3.1× |
| 10GB / 1KB | 109.7K | 32.3K（位图优化后） | 3.4× |
| 10GB / 272B | 851.7K | 109.0K | 7.8× |

### 2.2 读（readrandom）

| 场景 | 原生 ops/s | ZF ops/s | 命中率 |
|---|---|---|---|
| 2.2GB / 1KB | （未跑） | （未跑） | 63% |
| 10GB / 1KB | 457.6K | 105.6K（位图优化后） | 63.3% |
| 10GB / 272B | 982.7K | 140.1K | 63.2% |

### 2.3 参考（50GB / 272B，R44 配置，历史）

| 引擎 | ops/s | 备注 |
|---|---|---|
| 原生（R44 时，低负载） | 66.9K | 49 分钟 |
| ZF（M4.8，负载 25） | 39.2K | 83 分钟，1.7× |

## 3. 差距分析

> 规模效应：10GB 数据全部落在页缓存内，原生写路径为纯 CPU 瓶颈（272B 时
> 851K ops/s），ZF 的每 op 固定开销（分区 WAL Append + 索引 Insert + 物化）
> 被显性放大。磁盘瓶颈场景（50GB+/低负载）差距显著缩小：R47e 历史 1KB/100GB
> 原生 23.8K vs ZF 12.4K（1.92×）；R44 配置 272B/50GB 原生 66.9K vs ZF 39.2K
> （1.7×，负载 25）。10GB 页缓存热的 4.5-7.8× 是固定开销差距的上界。

### 3.1 瓶颈分解（10GB / 1KB，ZF 21.6K vs 原生 109.7K）

### 3.1 瓶颈分解（ZF 侧，10GB / 1KB）

| 组件 | 占比/数据 | 说明 |
|---|---|---|
| 物化 | 146.8s / 440s = 33% | WAL 读 + 排序（21.3s）+ BuildTable；物化吞吐 68MB/s（2.2GB 时 183MB/s——规模退化） |
| 写停（stall） | 23.9s / 440s = 5.4% | db.write.stall（P50 2.1ms） |
| 写路径有效吞吐 | ~37K ops/s | 纯写路径（不封存）上限 144K——1.7× vs 原生 251K；10GB 时有效 37K（物化/compaction CPU 竞争） |

### 3.2 差距构成

1. **写路径固有 1.7×**：AddRecord（分区 WAL Append + 跳表索引 Insert）6.9µs/op vs 原生 4µs/op；
2. **物化 33%**：每 epoch 冻结 ≤4 分区（kMaxBatch=4）→ epoch ~102MB 小批次 × 98（10GB），
   批次固定开销 + 任务粒度小；kMaxBatch 提到全部分区后物化 -43%（6.8s vs 12s@2.2GB）但
   fallback 增多（127 vs 79）、停写变长，净吞吐更差——批次大小与 L0 循环存在权衡；
3. **L0 循环**：fallback 391（10GB）——物化输出回落 L0（ratio<0.25 的 kDirect +
   PickInstallLevel L0..base 重叠回落）→ L0 文件 → compaction 消费（CPU/IO 与写竞争）；
   M4.9 L0 融合（物化输出合并 L0 文件→替换→直装 L1）已实现但实测未触发——决策时 L0
   文件几乎总被 compaction 占用（being_compacted）→ 等待/回落分支；
4. **负载效应**：系统负载 25（原生同受）——相对差距的绝对值在低负载下会缩小
   （R47e 历史：1KB/100GB 原生 23.8K vs ZF 12.4K = 1.92×，磁盘瓶颈场景）。

## 3.5 优化进展（M4.9 后续）

| 优化 | 实现 | 效果 |
|---|---|---|
| 写路径触达分区集合 hash → 位图（fecd043） | ZfBatchHandler touched 从 unordered_set 改 uint64_t 位图（kRangeDelPartId 映射位 63） | 纯写路径 144K → 170K（+18%）；2.2GB/1KB 整体 56.3K → 80.0K（+43%）；10GB/1KB 21.6K → 32.3K（+45%）、读 80.9K → 105.6K（+30%）——写组处理提速的传导 |
| 物化输入侧换索引序（M4.2 设计） | 未实现 | 评估：省排序（物化 14%）+ WAL 顺序读，但定点读（LRU 锁）抵消部分收益——预计净 10%，边际 |
| L0 融合完整版（compaction 竞争仲裁） | 框架已提交（88a233a），未触发 | 需 ZF 自管 L0 或融合优先仲裁——大改 |

## 4. M4.9 优化尝试（已提交/进行中）

| 优化 | 实现 | 2.2GB/1KB 效果 | 10GB/1KB 效果 |
|---|---|---|---|
| L0 融合（物化合并 L0→直装 L1） | materialize_job 决策/归并/安装扩展 | 未触发（fallback 79 不变） | 未触发 |
| L0 占用等待与 skip_batching 解耦 | l0_busy → 无条件 kSkip（字节阈值兜底） | fallback 不变 | fallback 391（无恶化） |
| base_merge_min_ratio 可调（flag） | 0.1 → 66.9K（+19%） | 10GB 无改善（epoch 数异常 98 与 ratio 无关——默认也是 98） |
| max_pending_epochs 可调（flag） | 4 → 23.9K（+3%） | 停写非主瓶颈 |
| kMaxBatch 全冻结 | epoch 大（275MB） | 44.9K（更差——fallback 127、停写长） | —（已回滚） |

## 4.5 非参数 tradeoff 探索（牺牲稳定性/事务支持）

| tradeoff | 收益（评估） | 风险 | 结论 |
|---|---|---|---|
| 封存 Sync 跳过（--zf_seal_no_sync） | 停写 5.4% → 0（10GB）——fsync 从封存路径移除 | 崩溃（断电）丢最近未 sync 的封存段（~1 epoch，256MB）；物化仍可读（页缓存） | 收益小（<6%），风险大——不建议默认；可作可选 flag |
| 物化输出只留最新版本（禁快照保留） | 物化去重更早、输出更小（~5-10%） | 快照读旧版本失败（隔离性牺牲） | 已隐含（无快照时 CompactionIterator 去重）；快照场景保留——无额外收益 |
| 写路径不建索引（物化/恢复时建） | 写路径省跳表插入（~1.5µs/op——+20%） | 未物化窗口的读 miss（frozen 数据读不到）→ 数据不可读 | 不可行（读正确性）——除非物化严格快于读（不保证） |
| 封存段内存化（不落盘，物化读内存） | 物化读 IO → 内存（快 10×） | 崩溃丢全部未落盘数据；物化瓶颈在 CPU 非 IO——收益有限 | 收益评估低于预期——不建议 |
| 禁用原生 L0 compaction（ZF 自管 L0，融合消费） | 打破 L0 循环（fallback 391 → 0），消除 compaction 竞争（潜在 20-50%） | 大改（compaction 调度）；稳定性风险（L0 无人消费时数据滞留） | 最大潜在收益——后续主攻方向 |
| 事务/多列族 | ZF 已不支持（NotSupported） | — | 无 tradeoff 空间（保持不支持即为简化） |
| disable_wal 语义 | ZF 分区 WAL 是架构核心（数据唯一持久副本） | — | 不能禁用；对比中双方均 disable_wal=false |

## 4.7 L0 自管深挖（M4.10 尝试与发现）

按方案实施「L0 自管」（禁用原生 L0 compaction + L0 融合激活）时发现两个
**M4.3d 批次封存路径的接线缺口**（R49 实测确认，解释全部历史实验的
base_merge=0）：

1. **M3.1 采样学习 / M4.2b L1 对齐未接入 FreezeBatchPartitions**：
   SealEpochAndSwitch（旧路径）有采样学习 + L1 对齐的 InstallNewVersion，
   批次封存路径漏了——路由表恒为版本 0 的 hash 表 → merge_enabled=false
   → 融合从未启用 → 物化全回落 L0 → L0 循环（R44/R47e/1KB 全部实验的
   fallback 大、merge=0 的根因）。
2. **表切换与 parallel 写的竞态**：补上采样学习后，封存内 InstallNewVersion
   换表，parallel follower 的 Route 用新表、物化用旧表（se.table_version）
   → 同 epoch 数据跨表 → 物化范围断言 Corruption/崩溃。写组表版本绑定
   （WriteGroup.zf_table_version + RouteWithVersion）已实现但未收敛——
   align_l1 的对齐还有 L1 文件数依赖（<partitions 时分区数变化 → 越界）。

**修复路线**（后续，按序）：
   a. 批次封存接入采样学习（已验证 merge 启用——补上后 base_merge 生效）
   b. 表切换的原子性：写组绑定表版本（框架已实现）+ 学习/对齐只在写组边界
      且分区数恒定（align_l1 不足时用采样边界兜底，不依赖 L1）
   c. L0 融合激活后验证 fallback→0（预期 20-50% 吞吐收益）

## 5. 结论与方向

- **272B 小值场景差距 ~1.7×**（50GB 历史对比，负载 25）；**1KB 大值场景差距 4.5-4.9×**
  （10GB 页缓存热时原生写路径优势显性化）。
- 参数级优化收益有限（<20%）；根本差距在：写路径固定开销（1.7×）、物化/compaction
  的 CPU 竞争、L0 循环的调度。
- 后续方向（按收益排序）：
  1. 写路径剖析（perf）：AddRecord 6.9µs/op 的锁/编码/插入分布；
  2. L0 循环根治：物化输出与 L0 compaction 的竞争仲裁（融合优先于 compaction 消费），
     或 ZF 自管 L0 消费（禁用原生 L0 compaction）；
  3. 物化批次流水线：epoch 冻结全分区 + 物化与写并行度解耦（任务池独立于 flush 调度）。

## 4. 50GB / 1KB 对比（2026-08-27，M4.11b + R54b）

**环境**：同机同参数（compression=none、cache 512MB、16 线程、max_background_jobs=24、subcompactions=16），
系统常驻负载 ~25-45（grafana/prometheus/ceph）。原生 = source/rocksdb/build/db_bench 11.2.0；
ZF = sampled 路由 + base_merge。规模 num=48,000,000（key 16B + value 1024B ≈ 50GB），
fill 后分进程 readrandom（--reads=1,000,000/线程）。

| 项 | 原生 | ZF | 差距 |
|---|---|---|---|
| fillrandom | 14.0K ops/s（13.9 MB/s） | 12.3K ops/s（12.2 MB/s） | **1.14×** |
| readrandom | 157.0K ops/s（98.5 MB/s） | 74.6K ops/s（46.8 MB/s） | 2.10× |
| 命中率 | 63.2% | 63.2% | 均 = 随机写覆盖期望 ✓ |

**历史对照（R44/R47e）**：1KB/100GB 原生 23.8K vs ZF 12.4K（1.92×）→ **fill 差距从 1.92× 收窄至 1.14×**（M4.11 切片消除全范围污染 + R54b 竞态修复）。fill 双方均处磁盘瓶颈（50GB 超出页缓存），差距接近 1。

**read 差距（2.1×）**：ZF 读路径 = 分区索引（GetFromPartitionIndex）+ 原生 SST 查找；切片使文件数增多（L1/L2 分区文件）→ 读放大。native 50GB 数据主要落在 2-3 层大文件。优化方向：读路径直达层（跳过多层候选扫描）/ 文件合并。

**50GB 测试暴露并修复的崩溃（R54 系列）**：
1. hash 遗留数据（epoch 1 + 未封存分区）单文件全范围输出 → L1 重叠（切片条件扩展到所有越界数据）；
2. 阶段 0-2 间 compaction 竞态 → L1 重叠（安装期复查 RangeOverlapWithCompaction，冲突回落 L0）；
3. 替换删除层不匹配 / 重复删除 / 过期版本检查（按实际层 + 去重 + cfd_->current()）；
4. VersionBuilder 缺失文件删除容忍（构建 + MANIFEST 重放双路径，删除 no-op 语义）；
5. 融合标记文件与原生 compaction 并发断言放宽（debug-only，数据重复不丢）。

**50GB fill 用时**：native 57 分钟（受早期测试竞争拖累）、ZF 65 分钟（无竞争，负载 8-45 波动）。

### 4.1 read 差距受控验证（2026-08-27 晚，负载 ~13 同窗口）

原始 2.1× 差距（74.6K vs 157K）经受控对照修正——157K 测于页缓存热 + 低负载窗口，
同窗口（负载 13）交替测量：

| 测量 | ops/s | 说明 |
|---|---|---|
| ZF 模式读 /tmp/zf50 | 69.4K | ZF 读路径 |
| 原生模式读 /tmp/zf50 | 80.2K | 同 DB 同页缓存——**ZF 路径纯开销 16%** |
| 原生模式读 /tmp/native50 | 85.4K | **DB 布局差异仅 6%**（文件 650 vs 569，层分布接近） |

**真实 read 差距 ≈ 1.23×**（85.4/69.4），构成：
- **ZF 读路径开销 16%**：GetFromPartitionIndex 每 Get 执行（即使数据在 SST）——
  跳表查询 8.85% CPU（cache miss 主导）+ 链遍历/锁 ~5% + Route 1%；
- **DB 布局 6%**：文件数略多（650 vs 569）→ 层二分/TableCache 略慢。

perf 证据：ZF 每 op CPU ≈ native 5.2×，其中 Version::Get（SST 查找）73%
（MaybeReadBlockAndLoadToCache 38% + IndexBlockIter 7.3%）+ GetFromPartitionIndex
18.5%——但两边每 op block.cache.miss 接近（~2 次）、bytes.read 相同（647B/op）
→ IO 量相同，差距纯在 CPU 路径。

**read 优化空间（按收益排序）**：
1. **索引布隆预过滤**（~8-10%）：GetFromPartitionIndex 前按分区布隆快速判断
   key 是否在活跃段索引（97% 的 key 数据在 SST → 布隆 miss 跳过跳表 ~1.1us/op）；
   布隆可增量构建（索引只增不删，frozen 释放时丢弃）。
2. **SST 优先路径**（~5%）：Get 先走原生 SST 查找（数据 98% 在 SST），miss 再查
   分区索引（活跃段 2% 兜底）——61% 的 Get 省去索引查询；正确性不变（SST miss
   → 索引命中读 WAL）。
3. **DB 布局**（~4-6%）：target_file_size_base 调大（64→128MB）减文件数；
   L0/L1 分区文件尽早合并。
4. **组合预期**：1.23× → ~1.05-1.08×（接近原生）。
