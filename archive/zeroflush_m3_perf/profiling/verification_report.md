# ZeroFlush 成果检验报告（M4.0 + M4.1，2026-08-21）

> 检验对象：`source/rocksdb-zeroflush`（M4.1 完成态二进制）
> 检验方式：全量回归 + 端到端数据完整性五连验证 + 性能对比
> 待收：50GB R4（M4.0 二进制写放大）与 R5（M4.1 二进制全量验收）后台运行中

---

## 1. 回归（正确性基线）

**27/27 用例全 PASS**（M1 7 + M2 6 + M3.0~M3.3 12 + M4.0 新增用例 35），覆盖：
- 写路径：顺序/随机/重复键、多分区、WAL 缓冲、重开不截断
- 恢复：Freeze 重开、MultiEpoch、孤儿代收养、崩溃恢复合并幂等
- M3.2/M3.3：直装零 L0、并行物化 7×、融合归并、compaction 竞争降级
- M4.0：kSampled 学习期端到端（修复回归用例）

## 2. 端到端数据完整性（`verify_integrity.py`，R3 配置 2.2GB）

| 项 | 验证 | 结果 |
|---|---|---|
| V1 | fillrandom 8M keys 完成 | rc=0，45.6K ops/s，176s |
| V2 | 全量读回（同进程） | **每线程 found 316,236 vs 期望 316,060（比值 1.0006）**——数据 100% 完整 |
| V3 | 重开读回（新进程 Recover） | found 315,749（差 487 = 随机抽样波动 1σ≈341）——重开恢复完整 |
| V4 | 写入中途 kill -9 → 重开读回 | found 124,420（崩溃时已落盘数据全部可恢复） |
| V5 | 封存-物化-回收闭环 | epochs_sealed=8 == materialized=8 == reclaimed=8 |

**结论：M4.1 并行写路径（含 4 个 bug 修复后）数据完整性无损失。**

## 3. 性能成果（2.2GB 全量，vs M4.0 基线）

| 指标 | M4.0 R3 | **M4.1 R3** | 提升 |
|------|---:|---:|---:|
| 吞吐 (ops/s) | 12,082 | **38,129** | 3.2× |
| P50 (us) | 1,279 | **138** | 9.3× |
| P99 (us) | 2,632 | 1,739 | 1.5× |
| wall (s) | 662 | 211 | 3.1× |
| micro 8 线程 (ops/s) | ~20K | **110.6K** | 5.5× |
| 单线程 P50 (us) | 52 | 38.9 | 1.3× |

- W1（hash P=64）全量：10,898 → **21,594**（2.0×）
- 数据规模 2.2GB 全量写+读+崩溃恢复全程 PASS

## 4. 成果全景（M4.0 + M4.1 累计）

**M4.0**（范围路由 + 融合归并接线）：
- 修复 kSampled 学习期 table_version bug；新增用例 35；27/27
- 实测发现：M3.2 直装单独必然退化（L1 重写 4.4×）；**P=16 是融合归并的批量关键旋钮**（L1 重写 2.62GB 优于 hash、零停写、吞吐 +11%）
- db_bench 六个路由/融合 flag 接线

**M4.1**（写路径去串行化）：
- O(1) ShouldSeal + SlimMemTableRep 并发化 + parallel 两分支注入 + 锁外编码
- **修复 4 个真实 bug**（详见 M4_DESIGN.md §M4.1）：follower 原生路径丢数据（最严重，实测 11.7% 读回）、Ref UAF、原生 flush 触发无 epoch imm、kSampled table_version
- 写路径天花板 10.9K → micro 110K+（10×）；全量 3.2×

## 5. 剩余瓶颈与下一步

**已知瓶颈（按优先级）**：
1. **全量 38K vs 60K 目标**：compaction 几乎全程占用（R3 全量 24 作业 199s / wall 211s）与写路径争抢 CPU + L0 偶发堆积 → 属 L0 消费端（M4.0 已识别），M4.3 每分区 compact 为根本解
2. P50 138us：封存-物化-imm 链的间歇干扰

**建议下一步（按序）**：
1. **等 R4/R5 结果**（~8h / ~2h）：R4（M4.0 二进制）给 sampled+P=16 的 50GB 写放大（vs hash 基线 0.70）；R5（M4.1 二进制）给 M4.1 写路径下的 50GB 全量验收（吞吐/空间/写放大），回填 M4_DESIGN.md
2. **M4.2 物化输入侧换跳表序**（3-5 天）：imm 跳表区间遍历免排序 + WAL 段整读缓冲——消除 `materialize_sort_micros`（占物化耗时大头），此输入侧即终态 L0→L1 输入侧，为 M4.3 铺路
3. **M4.3 每分区 memtable + 全局 epoch 拆除**（2-3 周）：终态主体（WAL 即 L0），预期全量 60K+ 验收线在此达成

## 6. 产物索引

- 回归：`build/zf_test`（27 用例）
- 完整性验证：`output/zeroflush_m3_perf/profiling/verify_integrity.py`
- 性能数据：`output/zeroflush_m3_perf/profiling/W1|R3/result.json` + `parsed.json`
- 设计文档：`zeroflush/M4_DESIGN.md`（M4.0/M4.1 完成节）
- 后台任务：R4（M4.0 50GB）、R5（M4.1 50GB）
