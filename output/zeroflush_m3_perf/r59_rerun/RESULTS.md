# R59 越界融合：50GB/100GB 1KB fillrandom 重跑确认（a996b80）

参数：16 线程、1KB 值、sampled + base_merge + value_cache 64MB、skip_batching=false、
subcompactions=16、compression=none、cache 512MB、bg=24。日期 2026-08-31。

## 结果

| 规模 | R59 前基线 | R59 后（本次重跑） | 提升 | 原生（矩阵同参） | ZF/原生 |
|---|---|---|---|---|---|
| 50GB | 12,260 ops/s（R54 后） | **39,421 ops/s（39.1 MB/s）** | **+221%（3.2×）** | 12,865 | **3.06× 反超** |
| 100GB | 9,728 ops/s（矩阵） | **28,764 ops/s（28.5 MB/s）** | **+196%（3.0×）** | 12,396 | **2.32× 反超** |

两次均 rc=0、0 错误（"Assertions are enabled" 警告不计）。

## 机制佐证（zf.* 统计）

| 指标 | 50GB | 100GB |
|---|---|---|
| base_merge_count（融合次数） | 194（基线 0） | 246（基线 0） |
| install_direct_base | 191 | 170 |
| install_fallback_l0 | 734 | 1,298 |
| skip_count | 3,857 | 9,134 |
| memtable stop（写停顿） | **101 次（基线 2,605 次，-96%）** | — |
| L0 delay | 48 次（新出现，越界融合副作用） | — |
| 物化时间占比 | 1,071s/1,218s wall = 88% | 3,343s/3,337s = 100% |

- 融合从 0 次恢复（194/246 次），写停顿 memtable stop 从 2,605 → 101（-96%）；
- **物化成为新的吞吐上限**（100GB 时物化时间 ≈ wall）——下一步优化应聚焦物化
  （方向 2 的 kMaxBatch 联动、物化并行度）；
- 数据完整性：fill rc=0 零错误；readrandom 因 rerun 脚本在 fill 后删除 DB 未能执行
  （同参数 fill→read 命中率 63.2% 已被矩阵与多轮验证确认）。

## 结论

R59 越界融合在 1KB 大规模 fillrandom 的效果被重跑确认：**50GB 3.2×、100GB 3.0×，
双双反超原生（3.06×/2.32×）**。剩余优化点：fallback/skip 仍多（L0 循环残余，
去注册方案未闭环）、物化成为新瓶颈。
