# RESBUDGET — 固定资源预算下的标准 LSM 写 vs 只写 WAL + 延迟物化

> 状态：协议先于结果写定（2026-10-17）。正式结果见 `bench/out/resbudget/REPORT.md`。

## 1. 问题

在**同一份固定资源预算**（设备读写带宽 + CPU）并**留有余量**的条件下，50 GiB
LSM-tree 写入的两种调度方式差在哪里？

* **A 标准路径**：前台 WAL 写 + memtable flush + compaction 全部在同一个预算内运行，
  由 RocksDB 的 memtable/L0 背压机制耦合。
* **B 只写 WAL + 延迟物化**：前台只写 50 GiB 顺序 WAL 等效日志，不做 flush；随后用同一
  份预算的剩余部分做真实 LSM 物化重放（flush + compaction，重放时 `disableWAL`）。

这不是"省掉工作"的对比：A 与 B 的设备字节总量应当近似守恒（A = WAL + flush + compaction；
B = WAL + flush + compaction）。差异只来自**调度与耦合**，因此可以直接回答"把 flush 推到
余量资源里做，前台能快多少、总完成时间是否变差"。

## 2. 资源预算与执行方式

本机的 cgroup v2 只向用户会话委派了 `memory`/`pids`，没有委派 `io`/`cpu`，因此不能用
`io.max`/`cpu.max`。改用两个可核验的机制：

| 资源 | 预算 | 机制 | 相对本机上限 |
|---|---|---|---|
| 写带宽 | **200 MB/s** | `bench/tools/libiothrottle.so`（LD_PRELOAD 全局令牌桶，跨进程共享 POSIX shm） | 顺序写 849 MB/s 的 **23.5%** |
| 读带宽 | **400 MB/s** | 同上（独立读桶） | 顺序读 1707 MB/s 的 **23.4%** |
| CPU | **8 个物理核**（CPU 0–7） | `taskset -c 0-7`（硬亲和性；18 个物理核 / 36 逻辑核） | 物理核的 **44%**，逻辑核的 **22%** |

"留有余量"指：预算显著低于硬件上限，且实验中实际 CPU 占用约 3–4 核、写带宽约
190–200 MB/s，均未贴顶。A/B 两臂使用完全相同的工作负载、RocksDB 选项、预算和 CPU 集。

机制验证（同一会话）：

* `dd` 2 GiB O_DIRECT 写 @200MB/s：10.40s（197 MB/s）；无限制 2.26s（约 900 MB/s）。
* 两个进程共享同一 shm：各 1 GiB，聚合 10.41s（197 MB/s）——预算确实是全局的。
* `dd` 2 GiB O_DIRECT 读 @400MB/s：5.20s（394 MB/s）。
* 顺序写天花板 8 GiB：9.64s（849 MB/s）；顺序读：4.80s（1707 MB/s）。

## 3. 工作负载

* 50 GiB 应用字节 = **51,622,203** 次写 × (16B key + 1024B value)。
* 16 个写线程，Feistel 置换后的伪随机键序（与既有 `fillcpu` 基准一致），无覆盖。
* RocksDB 11.2.0（未修改的 `source/rocksdb`），选项：
  `write_buffer_size=256MB`、`max_background_jobs=6`、`max_subcompactions=4`、
  `use_direct_io_for_flush_and_compaction=1`、`compression=none`（库默认）、`sync=false`。
* A：`fillcpu`（`Put`，WAL 开）。B 重放：同一 `fillcpu` 加 `--disable_wal=1`
  （仅本实验新增的开关，写路径其余不变）。

## 4. 两臂

### A — 标准路径
`fillcpu` 写 50 GiB；前台确认写、后台 flush/compaction 全部在 200/400MB/s、8 核预算内。
记录 fill 墙钟（前台完成）、drain 墙钟（后台排空）、停顿、flush/compaction 字节与 CPU。

### B — 只写 WAL + 延迟物化
1. **B1（前台 ingest）**：`walwriter` 以 O_DIRECT 顺序写 50 GiB 的 WAL 等效日志
   （16B key + 1KB value 记录，1 MiB 写块）。不建 memtable、不 flush。仍在同一预算内。
2. **B2（物化）**：B1 完成后，用同一 `fillcpu` 选项和同一键序重放 50 GiB 到新 DB，
   `disableWAL=1`，因此只产生 flush + compaction；同时用一个 O_DIRECT 顺序读线程扫过
   50 GiB WAL 文件，计入读预算（模拟延迟物化必须回读 WAL）。
3. **总时间** = B1 + B2（fill + drain）。B2 的 fill 阶段是物化的一部分，不计为前台 ingest。

## 5. 主指标与判据

**主指标**
1. **前台 ingest 完成时间**：A 的 fill 墙钟 vs B1 的墙钟。
2. **完全物化完成时间**：A 的 fill+drain vs B 的 B1+B2 fill+drain。
3. **前台吞吐/停顿**：A 的 ops/s、stall 秒与占比；B1 的 MB/s（无 flush，因此理论上零停顿）。
4. **资源合规**：两臂设备写/读带宽、进程 CPU 是否始终 ≤ 预算；设备忙率与峰值。
5. **工作量守恒**：A 与 B 的 flush 字节、compaction 输入/输出、设备总写字节对比。

**先写死的预测（可被证伪）**

* P1：B1 的前台 ingest 时间显著短于 A 的 fill 时间（预期 ≥ 3×；B1 只受 200MB/s 写预算约束，
  理论下限 50GiB/200MB/s ≈ 4.3 min）。
* P2：A 的 fill 期间存在明显的 stall（占 fill 墙钟 > 10%），B1 无 LSM 背压、stall=0。
* P3：总工作量近似守恒：两臂 flush 字节 ≈ 50 GiB，compaction 输出字节同量级；
  B 的完全物化时间不会比 A 快一个数量级，最可能相近或略差（B 多了 WAL 顺序回读；
  A 则可能因停顿损失部分预算）。
* P4：两臂实际资源均不超预算；A 的 CPU 均值低于 8 核（预算留有余量）。
* **若 P1 不成立**：说明 WAL-only 前台并不快，问题不在 flush/compaction 耦合。
* **若 P3 中 B 的总时间远大于 A**：说明"flush 走余量"只在 flush 工作量能塞进余量时成立；
  本负载的 compaction 工作量远超前台 WAL 时间，延迟物化会把总时间拉长。

## 6. 测量与归因

* `fillcpu`：每作业 flush/compaction 的 CPU、墙钟、输入/输出字节、L0 轨迹、stall 事件、
  每线程 CPU、设备 `/proc/diskstats` 采样。
* `resbudget_sampler.py`：1s 采样整盘读写 MB/s、busy%、进程 `rchar/wchar/read_bytes/write_bytes`
  与 CPU 秒——独立于 RocksDB 统计。
* `iothrottle`：每个进程退出时报告令牌桶实际记账的读/写 MB 与等待时间。
* 设备级数字包含宿主其他进程的噪声；因此同时报告进程级 `write_bytes` 与 shim 记账，
  三者交叉核对。

## 7. 已知偏差与边界

1. **WAL 等效日志不是 RocksDB 真实 WAL 格式**：B1 只模拟顺序追加的字节流；不模拟 WAL
   记录头/校验和与 group commit。B1 的 CPU 因此偏低，但它只占 B 总时间的一小部分。
2. **B2 用生成器重放而非解析 WAL**：键序与字节量与 A 完全一致，但省掉了 WAL 解码 CPU；
   已用独立的 50 GiB 顺序读模拟回读 I/O。该偏差使 B 略偏乐观（CPU 侧）。
3. **A 的 WAL 是缓冲写**（sync=false）；B1 用 O_DIRECT，持久性语义更强。预算按应用
   发出字节计，二者可比；设备级数字以 sampler 为准。
4. **单次运行**：本环境存在已知 1.3–1.8× 的回放方差（同配置不同轮次）。本实验的 A/B
   差异若小于该量级则不宣称效应；若前台时间差达到数倍，则远超方差。
5. **宿主机噪声**：设备 sampler 含其他进程；A/B 串行执行、每臂前 `sync` + 静置 10s，
   并在报告中给出设备 busy% 与进程级占比。

## 8. 复现

```bash
bash bench/run_resbudget.sh smoke   # 2 GiB/2M ops 冒烟
bash bench/run_resbudget.sh A       # 标准路径 50 GiB
bash bench/run_resbudget.sh B       # WAL-only + 物化重放 50 GiB
python3 bench/analyze_resbudget.py  # 汇总
```

产物：`bench/out/resbudget/{A,B,smoke}/`、`summary.json`、`REPORT.md`。

## 9. 执行时的增补与偏差登记

正式执行时在原计划之外增加了两项，均在 `REPORT.md` 中单独标注：

* **A2 对照臂**：A1 与 B 不是同一时段的设备状态；A2 是在 B 之后用完全相同参数复跑
  的标准路径，用于把"排期效应"与"设备漂移"分开。A1→A2 的差（+5.7%）作为本实验的
  噪声带，所有 B 的结论以 A2 为对照、并同时给出对 A1 的值。
* **B-pipe 臂**：原计划的 B 是"先 WAL 再物化"的顺序排期，它无法回答"让 flush 用余量"
  这一说法；B-pipe 把 WAL 限速到 80 MiB/s 与物化同时跑、两者共享同一个写令牌桶，
  是对该说法的直接检验。它是在看到 B-seq 结果之后追加的，因此按"探索性"对待：
  其与 B-seq 的 4.6% 差落在噪声带内，只能表述为"没有改善"。

另有两处口径在报告中写清：所有吞吐统一按工具原生 MiB/s 表述（预算 200 MB/s ≈
190.7 MiB/s）；B-pipe 没有额外的 WAL 回读线程，因此其读字节偏低、结论偏保守。
