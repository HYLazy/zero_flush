# Bottleneck campaign — run protocol (native RocksDB 11.2)

本文件固定"瓶颈判定"实验（bn_*）的协议。环境、负载、主配置 P、指标定义全部继承
`bench/CAMPAIGN.md`（同一台机器、同一 RocksDB 构建产物、同一 l0bench 驱动、同一
16 B 键 / 1 KiB 不可压缩值、同一 50 GB = 48 M 键宇宙）。本文件只记录差异与新增。

## 1. 驱动新增（仅客户端程序，未改 RocksDB）

- `--max_background_flushes=N --max_background_compactions=N`（默认 -1）。
  RocksDB `GetBGJobLimits`：两者都为 -1 时从 `max_background_jobs=8` 推导
  flush=max(1, jobs/4)=2、compaction=jobs-2=6；**任一显式 ≥1 即切换到独立分支**，
  HIGH 池（flush）与 LOW 池（compaction）分别按显式值设置。这就是计划 §四要求的
  "按阶段的资源控制"，用公共选项实现，无源码补丁。
- 采样列新增：`dev_io_ticks_ms`（/proc/diskstats 字段 13，设备忙时毫秒累计 → 设备 busy%）、
  `flush_time_*` / `comp_time_*`（`FLUSH_TIME`/`COMPACTION_TIME` 直方图：count/sum/p50/p99）。

## 2. 工作负载（干预实验的到达过程）

- 初始布局：`bench/base/base50` = 50 GB 填库（主配置 P、shuffle 到达）沉降后的硬链接副本
  （`mkreplica.sh`：硬链 `.sst`、复制其余），每臂每重复都从同一字节级布局出发。
- 轨迹：`--mode=overwrite --dist=uniform --order=shuffle --num_ops=12,800,000`
  （固定种子置换的前 12.8 M 项 = 12.40 GiB 应用确认字节，与 stage C 完全同一条轨迹）。
- 前台：16 线程闭环饱和写，`sync=0`，`compression=none`（主配置 P 不变）。
- 每臂 5 次独立重复（次要臂 3 次），按 rep-major 轮转交错运行顺序（bn_campaign.sh `scan`）。
- 运行结束等待自然排空（`--wait_for_drain=1`，上限 900 s），不强制 CompactRange。

## 3. 臂表（容量干预）

| 臂 | flushes(F) | compactions(C) | subcomp | 用途 |
|---|---:|---:|---:|---|
| `default` | 派生=2 | 派生=6 | 8 | 基线（jobs=8，无显式覆盖） |
| `x_f2c6` | 2 | 6 | 8 | 显式中档 ≙ default 的等价性检验 |
| `f1` | 1 | 6 | 8 | Flush 扫描·低 |
| `f4` | 4 | 6 | 8 | Flush 扫描·高 |
| `c2` | 2 | 2 | 8 | Compaction 扫描·低 |
| `c12` | 2 | 12 | 8 | Compaction 扫描·高 |
| `f1c12` | 1 | 12 | 8 | 矩阵：Flush低×Comp高 |
| `f4c2` | 4 | 2 | 8 | 矩阵：Flush高×Comp低 |
| `f4c12s16` | 4 | 12 | 16 | 矩阵：双高 + subcompaction 高（§五 配置4） |
| `f1c2` | 1 | 2 | 8 | 矩阵：双低（§五 配置5） |
| `s1` | 2 | 6 | 1 | subcompaction·低（3 次） |
| `s16` | 2 | 6 | 16 | subcompaction·高（3 次） |

判定按计划 §四.1/§四.2/§四.3 的支持条件逐条对照（队列先在哪里增长、停顿类别、
吞吐随干预的变化、平台期与瓶颈转移）。

## 3b. 决定性干预臂（`bn_extra.sh`：期望效应远大于回放噪声）

回放噪声实测为同配置 1.86×（§BN2），因此 ±20% 量级的容量旋钮效应无法在有限重复下分辨。
以下三条干预的期望效应为数量级级别，用于因果判定（计划 §七 第三条件）：

| 臂 | 干预 | 命令 | 重复 | 判定用途 |
|---|---|---|---:|---|
| `noc` | 移除压实产生的全部设备工作：`disable_auto_compactions=1`，`level0_trigger/slowdown/stop=100000`，`max_write_buffer_number=8` | `bn_extra.sh noc` | 5 | 纯写路径上限；若吞吐大幅上升且 device WA→1，则限制来自压实工作量而非写路径 |
| `deep nl5` / `deep nl4` | 结构性削减深层级联：`num_levels=5` / `4`（其余同主配置） | `bn_extra.sh deep` | 3+3 | 若级联重写字节随层数下降而吞吐按比例上升，则深层级联工作量是约束的**来源** |
| `devload` | 注入受控背景设备负载：`tools/devbench FILE 8 16 0`（O_DIRECT 4 KiB 随机写，16 线程，与压实设备流量 ~200 MB/s 同量级） | `bn_extra.sh devload` | 3 | 若前台吞吐按注入比例下降，则设备服务容量是**共享约束**；若不变，则设备不是约束 |

`devbench` 源码在 `bench/tools/devbench.c`（编译产物 `bench/tools/devbench`，不随仓库入库）。

## 3c. 敏感性臂

`zipf`：`--dist=zipf --zipf_theta=0.99`、同轨迹长度（12.8 M 次写）、默认容量，5 次重复。
注意计划 §一 的敏感性要求；zipf 到达会改变**分母含义**（内存表在 flush 时即消除大量旧版本），
分析时必须同时报告 flush 后条目数与 U（见 `L0_INVESTIGATION_REPORT.md` §4.3 的同类口径问题）。

## 3d. 实际执行的臂清单（最终）

| 阶段 | 臂 | 次数 | 记录字段 |
|---|---|---:|---|
| 基准 | `bn_base_fill`、`bn_base_snapshot` | 1+1 | `bn-base` |
| 容量扫描/矩阵（`bn_campaign.sh scan`） | `default` `x_f2c6` `f1` `f4` `c2` `c12` `f1c12` `f4c2` `f4c12s16` `f1c2` `s1` `s16` | 54 | `bn-scan` |
| 消融 | `noc` | 5 | `bn-noc` |
| 敏感性 | `zipf`（θ=0.99） | 5 | `bn-zipf` |
| 深层几何 | `mc8g`（`max_compaction_bytes=8G`）`mult20`（`max_bytes_for_level_multiplier=20`） | 6 | `bn-geom` |
| 设备争用 | `devload`（作废，见下）`devload2`（带周期速率输出） | 3+3 | `bn-devload` / `bn-devload2` |
| 隔离 | `bn_isoflush_{wb64m,wb256m,wb1g,wb8g}`、`bn_isol0_{k8narrow,k8wide,k8narrow_sc1,k16wide,k16wide_sc1}` | 9 | `bn-iso-*` |
| 设备探针 | `tools/devbench` read_pct ∈ {0,75,100} | 3×30 s | `out/analysis/BN_device_ceiling.txt` |

**作废记录**：`devload`（v1，3 次）——devbench 当时只在退出时打印速率，而它被 harness 提前 kill，
注入量无法核验，故该臂作废；结论以 `devload2` 为准。该记录保留在 manifest 中但不在报告中引用。

**被引擎拒绝的干预**（记录在案，避免重复尝试）：`num_levels=5/4` 施加于已存在的 7 层库被拒绝
（`Invalid argument: db has more levels than options.num_levels`）。层数只能在建库时决定。

**一次执行事故**（记录在案）：`bn_extra.sh` 在其一次调用执行期间被编辑，导致该调用在到达脚本尾部时
读到错位的文本而语法错退出（`dry` 阶段之后、`zipf` 之前），按旧报告 §14 的警告，本阶段的后续修改
全部放入独立文件（`bn_extra2.sh`）。另有一次机器重启中断（进程被杀、已完成的运行由 manifest
的 `done_rc` 判据自动跳过续跑）。

## 4. 隔离实验（计划 §六）

- `iso_flush`：单写线程、`disable_auto_compactions=1`、trigger=1000 —— 后台只剩 flush。
  `write_buffer_size ∈ {64M, 256M, 1G, 8G}` 扫描（8G 臂在 10 GB 宇宙几乎不 flush，
  给出"仅写路径"的 CPU 基线）。测量：每次 flush 的 wall/字节/条目、进程 CPU 核秒、
  flush 吞吐随输入规模的变化。
- `iso_l0base`：frozen 副本上构造 K 个 L0 文件（同一批键、`wide`/`narrow` 两种区间几何，
  复用 stage B 的构造器），然后以默认容量（F2/C6）打开、无前台写、让自动压实把
  恰好这批积压排空 —— 固定输入的 L0→base 重放。臂：K=8 narrow/wide × subcomp{1,8}、
  K=16 wide × subcomp{1,8}。测量：逐 job 输入/输出字节、wall/CPU、subcompaction 并行度、
  base 读/L0 读比。

## 5. 分析

`bench/bn_analyze.py`：`queue`（单运行：队列起始先后、增长斜率、稳态队列、
每阶段池利用率 = Σjob wall/(池大小×T)、服务率 μ、设备带宽/busy%、停顿类别、写放大）、
`arm`/`matrix`（跨重复聚合 mean±sd 与矩阵表）、`isoflush`、`isol0`。

## 6. 已知限制（在 CAMPAIGN.md §6 之上新增）

1. 所有 compaction 共用 LOW 池，"L0→base 专用容量"不可在线分离；本实验以
   显式 F×C 矩阵 + 固定输入重放（iso_l0base）回应计划 §四.2 的弱因果标注要求。
2. 每臂运行约 8–40 分钟，闭环饱和；到达率 λ 不是外生固定的（吞吐是响应变量），
   队列判据用"阶段池利用率≈100% + 队列不排空"表述。
3. 设备 busy% 依赖 io_ticks，仅新运行有；旧 A 阶段运行无此列（标 n/a）。
