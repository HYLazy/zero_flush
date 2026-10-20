# FLUSHCHAR — flush 的资源特征判定实验计划

> 问题（本次要回答的唯一问题）：**flush（memtable→SST）的使用有什么特征？
> 是高计算占用、高带宽占用，还是高内存占用？**
> 对象：未修改的原生 RocksDB 11.2（`source/rocksdb`），默认配置为主，
> 1KB KV / 16 写线程 / 全键空间伪随机序（与 fillcpu 系列完全同口径）。
> 产出：`bench/out/flushchar/REPORT.md`（判定 + 证据链 + 复现命令）。

## 0. 已有证据（不重跑，直接引用）

之前的 fillcpu / pcie_bw 系列已经在**默认配置、100GB、compaction 与 flush 同场**的
条件下测过 flush 的量级（`bench/out/fillcpu_1kb100_default/run.REPORT.md`、
`bench/out/fillcpu_1kb100_src2/`、`bench/out/pcie_bw/REPORT.md`）：

| 维度 | 实测（默认配置，100GB fill） |
|---|---|
| CPU | flush 线程 CPU 206.9 s，占进程总 CPU **1.8%**；强度 **2.07 s/GB**（≈494 MB/s·核）；单作业 CPU 中位 111 ms |
| 带宽 | flush 输出 100.3 GB（=应用数据的 1.004×），占设备总写入 1041 GB 的 **9.6%**；平均需求 20.4 MB/s = 顺序写天花板（910 MB/s）的 **2.2%**；PCIe 双向合计 5.9% |
| 内存 | flush 的操作数（memtable）64 MB×2、块缓存 0.3 MB；进程 RSS 979 MB 里 643 MB 是 table readers、251 MB 是 glibc 保留堆——均与 flush 无关 |
| **墙钟分解**（src2 逐作业 iostats，1552 个作业聚合） | 作业 wall 合计 472.7 s：**fsync 等待 308.9 s（65.4%）**、线程 CPU 175.2 s（37.1%，含 I/O 提交 CPU）、write() 调用 wall 54.1 s（11.4%）、其余（调度/锁）2%。中位作业：wall 272 ms、CPU 111 ms、**fdatasync wall 168 ms** |

即在真实混合负载里，flush 三者都不"高"。但这是**伴随观测**：flush 的节奏被
compaction 停顿牵着走，无法区分"flush 本身的容量特征"与"被环境压制的表现"。
本计划补上**隔离 + 扰动**两类判别实验，把三个假设逐一证伪/证实。

## 1. 三个假设与证伪标准

先定义"高"的可判定门槛（都写成可测的量，报告里逐条对）：

| 假设 | 含义 | 证伪/证实标准 |
|---|---|---|
| **H1 高计算** | flush 的墙钟主要花在 CPU 上，CPU 是其吞吐的限制因素 | 作业 CPU/wall ≥ 80%；或 flush 满负荷时进程 CPU 饱和（36 逻辑核占满）；或限制 CPU（线程数不增、降配）时吞吐同比例下降。反之若 CPU/wall ≈ 35% 且有大量空闲核 → 证伪 |
| **H2 高带宽** | flush 需要的设备/链路带宽接近天花板，带宽是限制因素 | flush 满负荷时设备写带宽 ≥ 顺序写天花板的 ~70%；或并发吃掉一部分带宽时 flush 吞吐同比例下降。反之若满负荷带宽占用 < 30% → 证伪 |
| **H3 高内存** | flush 消耗的内存（RSS 增量 / 页缓存脏页）大到成为限制因素 | flush 使 Dirty+Writeback 逼近脏页上限并触发写回节流，或 RSS 增量达 GB 级并限制吞吐；write_buffer 扫描给出内存操作数的缩放律。反之若增量 ~100 MB 量级、Dirty 远低于 6.2 GB 背景阈值 → 证伪 |
| **H4（补设）高延迟** | flush 的墙钟由**低队列深度的同步 I/O（fdatasync）**主导 | 作业 fdatasync wall 占比 ≥ 50%；fdatasync 时延 ≈ 输出字节/设备速率 + 固定开销；多 flush 线程时吞吐近线性（等待并行化）而带宽仍不饱和 |

## 2. 判别实验矩阵

公共条件：`bench/tools/fillcpu`（只设 create_if_missing + 只读监听器，不开 Statistics），
16B key + 1KB value，16 写线程，`--sample_ms=250`，fill 后 drain 到静止；
DB 在 `/home/embed/hyl/metadata_offload/bench/db/`（即被测盘 nvme0n1p3）；
每跑独立 DB，跑完即删（本机盘仅剩 ~130 GB，需节制）。
"flush-only" = `disable_auto_compactions=true` 且把 l0 slowdown/stop 与
pending-compaction 软硬限抬到 10^6/2^42，使**后台只剩 flush 一种作业**；
此时 fill 吞吐 = flush 流水线的容量（写侧 16 线程本身容量 ≈366 MB/s，高于预期
flush 容量，故瓶颈落在 flush 上，且以 memtable-limit 停顿的形式显形）。

| 编号 | 配置 | 数据量 | 度量/回答 |
|---|---|---|---|
| **C0** | dd O_DIRECT 顺序写/读 16 GB；devbench 随机 4K | — | 本会话带宽天花板（分母，防盘态漂移；9-10 月两次会话已差 5%） |
| **C1** | `flushmicro`：①memcpy+XXH3 流送 ②写 64 MB 缓冲后 fdatasync ×20 | — | ①纯流送 CPU 参照（flush CPU 强度 / memcpy 速率 = flush CPU 的"贵"倍数）②**干净环境下 fdatasync(64MB) 的时延底价** = flush 每作业墙钟下限 |
| **R1** | flush-only，其余全默认（wb=64MB，1 flush 线程） | 20 GB | flush 满负荷容量；作业预算表（CPU/fdatasync/write/墙钟）；满负荷时设备带宽占用与 CPU 占用；Dirty 页轨迹 |
| **R2** | R1 + max_background_flushes=2 | 20 GB | 容量随 flush 线程数的缩放（等待并行化 ⇒ H4；带宽触顶 ⇒ H2；不缩放且 CPU 饱和 ⇒ H1） |
| **R3** | R1 + max_background_flushes=4 | 20 GB | 同上外推；若容量撞到写侧 366 MB/s 说明 4 线程已不再是被约束方 |
| **R4** | R1 + write_buffer_size=16MB | 10 GB | 内存操作数缩放律（RSS/memtable/Dirty）；小作业的每字节 CPU 与 fdatasync 次数变化 |
| **R5** | R1 + write_buffer_size=256MB | 20 GB | 同上（大作业摊薄效应） |
| **R6** | R1 + use_direct_io_for_flush_and_compaction=true | 20 GB | flush 绕过页缓存：速率变化（页缓存是否 H3 的隐形臂）与 Dirty=0 验证 |
| **R7** | R1 + 并发 dd 吞掉约一半设备写带宽 | 20 GB | **带宽饥饿扰动**：外部带宽被吃掉 ~50% 时 flush 容量掉多少（同比例 ⇒ H2；超比例 ⇒ 延迟敏感 ⇒ H4） |
| **R8** | R1 缩到 2 GB，包在 `strace -f -c` 里 | 2 GB | 每 flush 作业的 fdatasync/write 系统调用**次数**（时延分布已有，次数补齐） |

已有证据充当：默认混合负载下的伴随观测（第 0 节）与 bg2/bg8 的带宽竞争对照
（flush 单作业 wall 0.249→0.690 s，×2.8）。

## 3. 判读规则（先写死，防事后解释）

1. **容量点预算**：R1 稳态下，flush 流水线容量 R* = fill 吞吐。分解每作业墙钟为
   `CPU / fdatasync 等待 / write 调用 / 其他`，四个占比决定 H1/H4 的判定。
2. **资源占用率**（容量点）：设备写带宽占用 = 设备写速率（diskstats）÷ C0 顺序写
   天花板；CPU 占用 = flush 线程 CPU 速率 ÷ 1 核（并对照 36 核总量）。
   两者均 <30% 而 fdatasync 占比 ≥50% ⇒ H1、H2 同被证伪，H4 成立。
3. **缩放方向**：R2/R3 若吞吐 ~×2/×4 且带宽占用仍 <70% ⇒ 等待（延迟）并行化，
   H4 的旁证；若吞吐不增且 CPU 满 ⇒ H1；若吞吐增但带宽贴顶 ⇒ H2。
4. **内存臂**：R4/R5 给出 RSS ≈ f(write_buffer_size) 的缩放律（预期线性、量级百 MB）；
   R6 对比 R1：若 direct IO 下速率显著变化或 R1 的 Dirty 轨迹逼近 6.2 GB
   （dirty_background 10%×62 GB）并伴随写者节流 ⇒ H3 有实质分量；否则证伪。
5. **扰动**：R7 的容量下降幅度 vs 被吃带宽比例：>同比例 ⇒ 延迟敏感（排队），
   H4；≈同比例 ⇒ H2；≪ ⇒ 带宽无关。
6. **CPU 的"贵"度**（次要）：flush CPU 强度（s/GB）÷ C1 memcpy 流送速率
   （GB/s·核）⁻¹ ≈ flush 每 GB 的 CPU 是纯拷贝的几倍——回答"flush 的 CPU 都花在哪"
   （已有 perf 剖面：memmove/迭代器/块构建/XXH3）。

## 4. 交付物

- `bench/flushchar_analyze.py <run_prefix>`：单跑预算表 + 上述判据量。
- `bench/out/flushchar/REPORT.md`：结论表（三个假设各 PASS/FAIL + 数字）、
  预算表、缩放曲线、复现命令、限制。
- 驱动改动仅限 `bench/fillcpu.cc` 追加插桩/选项（--disable_compactions、
  --write_buffer_size、--max_bg_flushes、--direct_io、samples 加 meminfo
  Dirty/Writeback 列），不改 source/rocksdb。

## 5. 已知限制

- 单盘单机单次运行（判别实验均为 20 GB 级，量级差异远大于单跑噪声；
  关键跑 R1/R3 各复跑一次核对）。
- flush-only 是构造环境（无 compaction 竞争），回答"flush 本身的特征"；
  真实混合负载的表现以第 0 节已有数据为准，两者在报告中分开陈述。
- 默认配置 wb=64MB/1 flush 线程的"容量"受单线程串行 fdatasync 结构性限制，
  这正是要测的对象，不是缺陷。
- 盘剩余 130 GB：顺序执行 + 每跑删库；dd/随机探针文件用完即删。
