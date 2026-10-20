# 固定资源预算下的 50 GiB LSM 写入：标准路径 vs 只写 WAL + 延迟物化

> 实验日期：2026-10-17/18　主机：i9-10980XE（18 物理核 / 36 逻辑核）、62 GiB、
> WD_BLACK SN750 2TB（`/dev/nvme0n1p3`，ext4）
> 协议（先于结果写定）：`bench/RESBUDGET_PLAN.md`
> 数据：`bench/out/resbudget/`（原始 CSV/日志按 `bench/.gitignore` 只留本机）；
> 汇总：`bench/out/resbudget/summary.json`；图：`bench/out/resbudget/fig_*.png/svg`

## 0. 结论

**在固定字节预算下，"只写 WAL、把 flush 推到余量"能把前台完成时间缩短 6.5–6.9×，
但无法缩短总完成时间；两种 B 排期都比标准路径更慢（+15%～+20%）。**

| 臂 | 前台 ingest | 完全物化 | 总时间 | 相对 A2 |
|---|---:|---:|---:|---:|
| A1 标准 | 1730.7 s | 514.3 s | **2245.0 s** | −5.4% |
| A2 标准（对照） | 1860.0 s | 513.3 s | **2373.2 s** | — |
| **B-seq**（先 WAL 全速，再物化） | **268.1 s** | 2455.5 s | **2723.6 s** | **+14.8%** |
| **B-pipe**（WAL 80MB/s 与物化重叠） | 1182.0 s（与物化并行） | 2783.5 s | **2850.0 s** | **+20.1%** |

原因是一条硬约束：**延迟物化本身就要 2455 s，已经和标准路径的全程 2245–2373 s 一样长。**
所以无论怎么排期，WAL 阶段都不可能"藏进余量"——预算里没有那份余量。
WAL 只有 50 GiB 写，而 flush + compaction 要写 50 GiB、重写 247–253 GiB，
并读回 224–232 GiB；同一份预算下把它们拆开，只会把关键路径暴露出来。

真正的收益在**延迟隔离**，不在吞吐：标准路径的前台有 24–25% 的采样秒处于写停止，
WAL-only 前台为 0（B1 全速 191 MB/s、零停顿）。

## 1. 实验设计

固定一份**留有大量余量**的资源预算，比较同一 50 GiB LSM 写入的两种调度：

* **A 标准路径**：RocksDB `fillcpu`，WAL + memtable flush + compaction 全在预算内，
  由 memtable/L0/pending-bytes 背压耦合。
* **B 只写 WAL + 延迟物化**：
  * **B1（前台）**：`walwriter` 顺序写 50 GiB WAL 等效日志（O_DIRECT，不建 LSM、不 flush）。
  * **B2（物化）**：同一 `fillcpu` 选项把同一批键值重放进新 DB，`disableWAL=1`，
    只产生 flush + compaction；同时用 O_DIRECT 顺序读扫过 50 GiB WAL 文件，计入读预算。
  * **B-seq** = B1 全速完成后再做 B2。
  * **B-pipe** = WAL 以 80 MiB/s 限速与 B2 同时跑，两者共享同一个写令牌桶
    （即"WAL 吃 80，flush/compaction 用剩下的"）。

资源预算：

| 资源 | 预算 | 机制 | 本机上限 | 占比 |
|---|---|---|---|---|
| 写带宽 | 200 MB/s | LD_PRELOAD 全局令牌桶（跨进程 POSIX shm） | 849 MB/s（8 GiB O_DIRECT） | 23.5% |
| 读带宽 | 400 MB/s | 同上（独立读桶） | 1707 MB/s | 23.4% |
| CPU | CPU 0–7（8 物理核） | `taskset` 硬亲和性 | 18 物理 / 36 逻辑核 | 44% 物理核 |

本机 cgroup v2 未向用户会话委派 `io`/`cpu`，因此用 `bench/tools/libiothrottle.so` 在
`write/pwrite/writev/read/pread/readv` 层做全局字节令牌桶。校验：单进程 `dd` 2 GiB
@200MB/s 用时 10.40 s（197 MB/s）；两进程各 1 GiB 共享预算，聚合 10.41 s（197 MB/s）；
读 @400MB/s 用时 5.20 s（394 MB/s）。

工作负载：50 GiB = 51,622,203 × (16B key + 1024B value)，16 写线程，伪随机键序，
`write_buffer_size=256MB`、`max_background_jobs=6`、`max_subcompactions=4`、
flush/compaction 用 direct IO。A1、A2、B、B-pipe 参数完全相同。

## 2. 结果

### 2.1 时间分解

| 臂 | 前台 ingest | 物化 fill | 物化排空 | 总时间 | 前台吞吐（MiB/s） |
|---|---:|---:|---:|---:|---:|
| A1 标准 | 1730.7 s | — | 514.3 s | 2245.0 s | 29.6（29,827 ops/s） |
| A2 标准（对照） | 1860.0 s | — | 513.3 s | 2373.2 s | 27.5 MB/s |
| B-seq | **268.1 s** | 1876.2 s | 579.3 s | 2723.6 s | **191.0（= 预算上限）** |
| B-pipe | 1182.0 s（与物化并行） | 2266.7 s | 516.8 s | 2850.0 s | 43.3（WAL 实际速率） |

* 前台加速：B-seq 相对 A1 **6.46×**、相对 A2 **6.94×**。
* 吞吐单位按工具原生口径为 MiB/s（1 MiB = 1.048576 MB）；预算 200 MB/s = 190.7 MiB/s，
  B-seq 的 191.0 MiB/s 即为把写预算打满。
* 但 B-seq 的物化 fill（1876 s）就已经和 A2 的全程（2373 s）同一量级，
  加上 50 GiB WAL 的串行写入（268 s）后总时间反超。
* B-pipe 证明"用余量"是无效的：WAL 限速到 80 MiB/s 后实际只跑 43.3 MiB/s
  （因为物化在抢同一个令牌桶），物化 fill 从 1876 s 拉长到 2267 s（+21%）；
  总时间 2850 s，比顺序 B 还差 4.6%。**共享固定预算不会产生时间，只会重新分配。**

### 2.2 工作量守恒

| 臂 | 应用字节 | flush 输出 | compaction 输出 | 进程写 | 进程读 | CPU 秒 | 平均核数 |
|---|---:|---:|---:|---:|---:|---:|---:|
| A1 | 50.00 GiB | 49.98 GiB | 253.11 GiB | 333.0 GiB | 232.7 GiB | 3990 | 1.78 / 8 |
| A2 | 50.00 GiB | 49.99 GiB | 253.30 GiB | 324.5 GiB | 224.2 GiB | 3962 | 1.67 / 8 |
| B-seq | 50.00 GiB | 50.21 GiB | 246.78 GiB | 327.2 GiB | 277.2 GiB | 3562 | 1.31 / 8 |
| B-pipe | 50.00 GiB | 50.21 GiB | 252.24 GiB | 323.9 GiB | 223.8 GiB | 3800 | 1.33 / 8 |

* flush 输出四臂都在 49.98–50.21 GiB = 应用数据 1.00×；compaction 输出 246.8–253.3 GiB。
* B-seq 的读比 A1 多 19%（277.2 vs 232.7 GiB），多出的约 50 GiB 正是延迟物化必须回读的 WAL；
  B-pipe 没有跑独立的 WAL 回读（见 §5 限制 9），因此其读字节（223.8 GiB）偏低、
  总时间略偏乐观。
* **总量守恒成立**：A 与 B 做的是同一份 flush + compaction，B 没有省掉工作。

### 2.3 关键路径是 pending compaction，不是前台 WAL

fill 期间 1 s 采样：

| 指标 | A1 | A2 | B2 物化 | B-pipe 物化 |
|---|---:|---:|---:|---:|
| 写停止采样占比 | 24.6% | 24.2% | 29.6% | 24.9% |
| L0 中位 / 最大 | 13 / 23 | 16 / 23 | 15 / 23 | 18 / 23 |
| pending compaction 中位 | 60.5 GiB | 54.9 GiB | 61.4 GiB | 55.2 GiB |
| 同时运行 compaction（中位/最大） | 5 / 5 | 5 / 5 | 5 / 5 | 5 / 5 |
| ops/s 中位（q1–q3） | 13.6K（6.5–15.9K） | 12.7K（6.5–15.9K） | 12.7K（3.7–15.9K） | 10.4K（3.0–15.3K） |

* A 的 fill 与 B2 的物化 fill 几乎同长（1731/1860 s vs 1876 s），尽管 B2 **不写 WAL**。
  前台 WAL 写不在关键路径上；瓶颈是 pending compaction bytes 钉在 55–61 GiB 附近、
  L0 维持 13–18 个文件、5 个 compaction 线程常驻。
* 标准路径的前台被这个关键路径直接反压（24.6% 采样写停止）；B2 的物化有 29.6% 写停止——
  把 flush 推到后台并不会让它不受同一套背压，它只是换了个被压的对象。

### 2.4 预算用满了吗？

| 臂 | 写预算利用（记账字节/墙钟） | 读预算利用 | 设备 busy 均值 | 设备写均值 | 设备读均值 |
|---|---:|---:|---:|---:|---:|
| A1 | 333.0 GiB / 2245 s = **151.9 MiB/s（80%）** | 106.1 MiB/s（28%） | 56.3% | 135.3 MB/s | 106.2 MB/s |
| A2 | 324.5 GiB / 2373 s = **140.0 MiB/s（73%）** | 96.7 MiB/s（25%） | 60.4% | 124.3 MB/s | 96.7 MB/s |
| B-seq | 327.2 GiB / 2724 s = **123.0 MiB/s（65%）** | 104.2 MiB/s（27%） | 17.6%（B1）/ 71.2%（B2） | 191.6 / 116.4 | 0.0 / 115.5 |
| B-pipe | 323.9 GiB / 2850 s = **116.4 MiB/s（61%）** | 80.4 MiB/s（21%） | 71.1% | 118.0 MB/s | 82.3 MB/s |

（`MB/s` 为设备 sampler 的十进制 MB；`MiB/s` 为进程记账的二进制 MiB。预算 200 MB/s ≈ 190.7 MiB/s。）

* **CPU 从不是约束**：均值 1.3–1.8 核（预算 8 核）。
* **写预算也没有被持续用满**：61–80%。但设备 busy 仍有 56–71%，慢窗口出现
  **90%+ busy 却只有 3–8 MB/s** 的状态——时间被低队列深度的 compaction I/O 和设备状态
  占住，而不是被字节配额卡住。
* 因此"再加带宽/CPU"不是显然的解；要么减少 compaction 工作量（写放大），
  要么提升 compaction 的服务率/队列深度。这与既有 M4 结论（merge-on 1.17× 放大 vs
  merge-off 5.08×/83.7×）同向。

### 2.5 事先写死的预测与实测

| 预测（`RESBUDGET_PLAN.md` §5） | 实测 | 判定 |
|---|---|---|
| P1：B1 前台 ≥ 3× 快于 A 的 fill | 6.46×（A1）/ 6.94×（A2） | **成立** |
| P2：A 的 fill 有 >10% 时间 stall，B1 stall=0 | A1 24.6%、A2 24.2% 采样写停止；B1 无 LSM、0 stall | **成立** |
| P3：总工作量守恒；B 完全物化时间与 A 相近或略差 | flush 49.98–50.21 GiB 四臂一致；B 总时间 +14.8%（对 A2） | **成立**（略差 15%） |
| P4：两臂均不超预算，A 的 CPU 均值 < 8 核 | 写利用 61–80%、CPU 1.3–1.8 核 | **成立** |
| （新增）用余量让 flush 与 WAL 重叠可收回 WAL 时间 | B-pipe 2850 s，比 B-seq 还慢 4.6% | **证伪** |

## 3. 对"flush 走余量资源"的判断

**成立的部分**：只关心**前台确认延迟**时，WAL-only 的价值是真实的。B1 用 268 s
确认全部 50 GiB（191 MB/s，0 flush、0 stall），标准路径要 1731–1860 s，
其中 24–25% 的采样处于写停止。延迟隔离成立。

**不成立的部分**：在固定总预算下，余量不能让物化变便宜。

* 延迟物化本身需要 **2455 s**（B2 fill+drain），已经 ≥ 标准路径全程 2245–2373 s。
  这是与排期无关的下界：即使 WAL 写入零成本，B 的最好结果也只是与 A 持平，
  而且还要额外付出 50 GiB 的 WAL 回读。
* 顺序 B 把 WAL 阶段串行加在物化之前 → +14.8%。
* 重叠 B 让 WAL 与物化共享同一令牌桶 → WAL 掉到 43 MiB/s、物化拉长 21% → +20.1%，
  比顺序 B 还差。**固定预算下的重叠不产生吞吐，只产生争用。**

**结论**：延迟物化是**延迟隔离**手段，不是**吞吐**手段。要赢总时间，必须减少
compaction 工作量（更低的写放大）或提高 compaction 的服务率；仅改变 flush 的调度位置，
总工作量守恒，总时间不会变短。若存在**真正独立的余量资源**（第二块盘、空闲时段、
或前台只占预算的一小部分），结论才是 `总时间 = max(ingest, 物化) ` 而不是相加——
本次实验的预算里没有这种独立余量。

## 4. 图表

![phases](out/resbudget/fig_phases.png)

![io](out/resbudget/fig_io.png)

![foreground](out/resbudget/fig_foreground.png)

## 5. 限制与偏差

1. **单次运行**：A1→A2 同配置漂移 +5.7%；B-seq 相对 A2 +14.8%、B-pipe +20.1% 超过漂移；
   B-pipe 相对 B-seq 的 +4.6% 落在漂移量级内，只能表述为"没有改善"。
2. **设备状态**：实验期间 NVMe 温度 78–83 °C（历史阈值 83.8 °C），宿主 load ~9；
   设备 sampler 含宿主其他进程 I/O。高 busy、低 MB/s 的脱节很可能包含热节流/SLC 折叠/
   其他进程因素，未做隔离。
3. **限流在应用层**：令牌桶限制进程发出的读写字节；页缓存回写、其他进程和内核预读
   可使设备级瞬时峰值超过预算（A1 峰值 667 MB/s）。平均记账速率始终 ≤ 预算。
4. **B 是模拟**：B1 是 WAL 等效顺序流（非 RocksDB WAL 格式），B2 用生成器重放而非
   解析 WAL；WAL 回读用独立 O_DIRECT 顺序读建模。B1 的 CPU（0.24 核）是真实 WAL
   写入的下界，B2 省掉了 WAL 解码 CPU，因此 B 略偏乐观——而结论是 B 更慢，
   偏乐观的方向不改变结论。
5. **WAL 持久化语义不同**：A 的 WAL 是 `sync=false` 缓冲写，B1 是 O_DIRECT。
6. **排空判据**：连续 5 次采样无 flush/compaction/immutable 即结束；四臂 `drain_hit_limit` 均为 0，
   终态 L0 = 0。
7. **无覆盖写**：50 GiB 唯一键；覆盖/更新负载的 compaction 量与结论可能不同。
8. **进程写字节与 flush+compaction 输出之和不严格相等**（差 5–10%）：WAL 旧文件在
   flush 后删除、页缓存记账、direct IO 统计口径不同。工作量守恒以 flush/compaction
   事件字节为准，预算利用以进程记账为准。
9. **B-pipe 少了 WAL 回读**：`run_bpipe` 只启动了 `walwriter` 与重放，没有像 B-seq 那样
   另起 O_DIRECT 读线程扫 WAL 文件。按读预算 400 MB/s、50 GiB 回读，理论上最多再增加
   ~128 s 的竞争；实际在 26% 的读利用率下影响很小，但这使 B-pipe 的"仍然更慢"成为
   偏保守的结论（补上回读只会更慢）。
10. **令牌桶统计是全局的**：`iothrottle` 退出时打印的是共享桶的累计值，不是该进程自己的
   读写量；因此 B-pipe 的 `throttle_ingest` 里包含了重放进程当时的读数。报告中的进程级
   读写字节一律取自 `resbudget_sampler.py` 对 pids 的采样，不使用该打印值。

## 6. 复现

```bash
bash bench/build_fillcpu.sh
gcc -O2 -fPIC -shared -o bench/tools/libiothrottle.so bench/tools/iothrottle.c -ldl -lpthread -lrt
gcc -O2 -o bench/tools/walwriter bench/tools/walwriter.c

bash bench/run_resbudget.sh smoke   # 2 GiB / 2M ops
bash bench/run_resbudget.sh A1
bash bench/run_resbudget.sh A2
bash bench/run_resbudget.sh B       # B1 + B2
bash bench/run_resbudget.sh Bpipe   # WAL 80MB/s 与物化重叠
python3 bench/analyze_resbudget.py
python3 bench/plot_resbudget.py
```
