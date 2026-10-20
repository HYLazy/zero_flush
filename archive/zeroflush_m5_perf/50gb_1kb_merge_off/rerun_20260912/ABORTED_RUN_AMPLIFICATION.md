# ZF merge-off 公平版 50GB 运行 —— 中止记录与写放大证据（2026-09-12）

> **⚠ 机制部分已更正（2026-09-12 晚）**：本文件 §机制结论 将 4TB 放大归因于
> "ZF sink 管线反复整分区合并"，经对同一份 `zf_fill.log` 打点反向核对，
> **ZF sink 全程 0 次触发**（`ZFDBG-skip sink=0`），4TB 来自**原生层间
> compaction**（`dec=0`×11,532、`lvl=0`×10,939）。以下**量化数字仍然有效**，
> 但机制请以 `output/report/study/ZEROFLUSHV0_PROBLEM_STUDY.md`
> §P1 为准。

> 状态：**用户指示中止**（15:1x），未产出 fill 结果行；原生命令臂未开始。
> 用途：作为"merge 关闭 + 50GB + 全域随机键在 db_bench 协议下不可收敛"的量化证据。

## 运行参数
- db_bench fillrandom：`--key_size=16 --value_size=1024 --num=48000000 --threads=16
  --compression_type=none --cache_size=536870912 --max_background_jobs=24
  --subcompactions=16 --histogram=true`
- ZF 公平版：`--zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64`
  （默认攒批；merge 关闭=默认 `zf_base_merge=false`；治理默认关）
- 未设置 `ZF_MAX_L1_BYTES` → L1 目标为默认 **1GB**（`zeroflush_db.cc:2493`）

## 环境前提（本轮为修复后的健康环境）
- fstrim 已交还 830.2 GiB；`wbt_lat_usec=0`；`commit=60`；风扇临时 100%
- 盘 SMART 健康：Percentage Used 13%、Available Spare 100%、Media Errors 0
- 降温闸门：83.8°C → 67.8°C 后于 **13:00:19** 开跑

## 中止时刻状态（运行 ≈2h1x）
| 指标 | 数值 |
|---|---|
| DB 大小 / SST 文件 | 74 GB / 1,881 |
| ZF 物化 install | **11,312 次 / 276.9 GB**（lvl=0: 10,736 / lvl=1: 576；平均 25.1MB） |
| sealed epoch | ≈2,485（9/11 DNF 被杀时 1,121） |
| WAL 代际 | ≈620/分区（16 分区，稳态 5.3 代/分） |
| 原生 compaction | **4,773 次 / 输出 4,019.6 GB**（→L1 2,506.4 / →L3 1,447.1 / →L2 66.0） |
| 最大单次 compaction | **~2.98 GB、输入 ~3,051,462 条**（= 一个分区 3M 条记录，整分区重写） |
| lsm_state（末次） | [L0=358, L1=140, L2=100, L3=1,176, L4=0] |
| ZFDBG-sched | `l1score` 6.08→7.52 全程钉住，`bgc` 最高 10 |
| 设备写入 | 562 MB/s、4,761 写 IOPS、平均写延迟 788ms |
| SMART 累计写入差值 | +8,919,631 data units ≈ **4.57 TB**（≈ 48GB 的 93.7×） |

## 三层写放大拆解（对 48GB 用户数据）
| 层 | 写入量 | 放大 |
|---|---|---|
| 用户数据 → 分区 WAL | ~48 GB | 1.0× |
| ZF 物化 install | 276.9 GB | 5.08× |
| 原生 compaction 重写 | 4,019.6 GB | 83.7× |
| 设备总写入（SMART） | ~4,570 GB | 93.7× |

## 对照
| 运行 | 结果 |
|---|---|
| merge **ON**（9/2 cmp_1kb50，完成） | install 1,315 次 / **55.7 GB（1.17×）**，27,066 ops/s，1,773s 跑完 |
| merge **OFF**（9/11 DNF） | 5h07m 被杀：install 4,536 次 / 110.3 GB，epoch 1,121，DB 70.8GB |
| merge **OFF**（本轮，中止） | 2h1x：install 11,312 次 / 276.9 GB，epoch ≈2,485，DB 74GB，设备 4.57TB |

## 机制结论（⚠ 见文首更正：执行者应为原生层间 compaction，非 ZF sink）
1. merge 关闭时数据先落 L0（本轮 10,736 个 lvl=0 文件），由 M5"下沉(sink)"管线向
   L1→L2→L3 推；L1 目标仅 1GB、每分区却持 3GB → `l1score` 永久 6–7.5 超限；
2. sink 因而反复调度**整分区合并**（日志实锤：输入/输出 ~3M 条 / ~3GB），2 小时内
   每分区被整份重写 ≈80 次（3.95TB ÷ 3GB ÷ 16）；
3. L2 超 4GB 即"清场"到 L3，L3 已有 1,176 文件装着同一批随机键区间，每次清场连带
   重写重叠部分 → 越滚越大；
4. 上述使 user-data 实际吞吐仅 ~7k ops/s（WAL 代际折算），远低于 merge-ON 的
   26.4k–29.8k ops/s，且无收敛迹象 —— 与 9/11 DNF 同机制。
