# 1KB 100GB 同窗对比（2026-09-02，负载 3-30）

参数：16 线程、1KB 值、compression=none、cache 512MB、bg=24、subcompactions=16；
ZF = sampled + base_merge + value_cache 64MB（b2bd1fe + R59C/R59D）。

## fillrandom（ops/s）

| 引擎 | fill | MB/s | wall | readrandom 命中率 |
|---|---|---|---|---|
| ZeroFlush | 19,701 | 19.5 | 4,873s | 63.22% ✓ |
| **原生 RocksDB** | **21,725** | 21.5 | 4,419s | 63.11% ✓ |

**同窗 ZF 落后原生 0.907×（10%）**——负载差异小（ZF 期间 load 3-8，native 期间 19-30，
对 native 略不利但仍领先）。历史：ZF 矩阵旧值 9.7K（R59 前）→ b2bd1fe 14.0K → 本轮
19.7K（+41%，含负载）；native 9/1 同窗 24.6K → 本轮 21.7K（负载更高）。

## 50GB vs 100GB 对比（本轮同窗）

| 规模 | ZF | native | ZF/native |
|---|---|---|---|
| 50GB | 27,066 | 26,409 | **1.025×**（ZF 领先） |
| 100GB | 19,701 | 21,725 | **0.907×**（ZF 落后 10%） |

规模增大 ZF 相对退化：物化滞留（skip 6260、物化占 wall 78%）随规模线性放大——
100GB 时物化成为更紧的瓶颈；native 的深层 compaction 摊薄更充分。

## ZF 决策统计（100GB）

merge 484 / 直装 373 / fallback 1605 / skip 6260 / 物化 3,789s（78% wall）。

## 结论

1. ZF 100GB/1KB 零丢失（63.22% = 期望）；
2. 同窗 0.907×（落后 10%）——50GB 持平（1.025×）而 100GB 落后：物化滞留的
   规模放大是主因（skip 6260 → 多代收养物化），消除滞留（批内收养）是弥合
   100GB 差距的关键；
3. ZF 从 R59 前 9.7K → 19.7K（2.0×），native 同窗 21.7K。
