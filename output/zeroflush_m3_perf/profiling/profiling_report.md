# ZeroFlush 写路径剖析报告（profiling 2026-08-21）

> **目的**：在 2.2GB 小规模下复现 ZF fillrandom 慢的现象，分解 wall time，验证/推翻
> `benchmark_analysis_report.md` 发现 2 的三个根因（①L0 停写、②物化随机定点读、③物化单线程），
> 为 P3 优化方向提供实测依据。
> **方法**：4 个对照变体（W0 原生 / W1 ZF 基线 / W2 无停写 / W3 K=1）+ 5 组微实验
> （单线程 / 线程伸缩 1-16 / 分区数 1-16-64 / strace / 原生单线程参照）。
> **数据**：本目录 `W*/result.json`、`W*/parsed.json`、`W*/samples.csv`、`W*/stdout.txt`、LOG（`/tmp/zf_prof_db_W*`）。

---

## 1. 核心结论（TL;DR）

| # | 结论 | 证据 |
|---|------|------|
| **C1** | **主瓶颈是写路径全序列化，不是物化、不是停写、不是任何 L0 侧机制**：ZF 把整个 write group 的处理（ShouldSeal 检查 + 逐条 WAL 追加 + SlimMemTable 插入）放在 **DB mutex 临界区**内，且强制 `allow_concurrent_memtable_write=false` → 16 线程完全串行 | 单线程 P50 仅 **52us**（基础路径很快）；P50 随线程数线性增长 52→121→484→721→1511us；四个 ZF 变体（含摘除全部 L0 限速的 W2b）聚合全钉在 10.4~10.9K ops/s ≈ 1/92us（串行天花板）；全程**零停写、零 stall 计数**、物化仅占 wall 1.7% |
| **C2** | **物化路径又快又并行，benchmark 报告的根因 ②③ 不成立**：K=8 并行 + WalScanner 顺序整读早已实现且生效 | K=8 每 epoch 物化 64 文件 / ~256MB 仅 **1.4~1.6s**（≈170MB/s）；K=1 时 10.7s/epoch（**7× 差距证明并行真实生效**），而端到端吞吐两者相同（10,899 vs 10,898 ops/s）——物化与写吞吐瓶颈无关 |
| **C3** | 每条写有 **~47us 用户态固定开销**（原生 5.4us）+ 每 partition ~0.3us 的 ShouldSeal 扫描 | 原生单线程 P50 5.35us vs ZF 52us（P=64）；P=1 时 32us、P=16 时 45us（近似线性）；strace 排除系统调用（futex 0.84s 全在启动期） |
| **C4** | L0→L1 compaction **subcompactions=1 单线程**，消化每轮 64 个重叠 L0 文件需 14~30s；2.2GB 尺度下刚好跟上（L0 恒 63-64，从未触顶 stop=72）；50GB 尺度下跟上不（推定 10.9K→3.4K ops/s 的增量劣化来自债务限速与停写，**待 50GB 复测验证**——原 50GB run 的 LOG 已被基准脚本删除，无法直接分解） | 21 个 compaction 作业全部 sub=1，cpu≈wall；L0 轨迹 63↔64；W1 `Stopping writes`=0、stall=0.0% |
| **C5** | 小规模完美复现 50GB 基准的延迟特征：W1 P50=1511us vs 50GB 的 1440us | 延迟形态（P50~P99.99）与量级一致，证明 2.2GB 剖析结论可外推 |

**一句话**：ZF 写吞吐差距的第一性原因是**写路径在 DB mutex 下把 16 个写线程串成一条 ~92us/op 的单队列**（理论天花板 ~11K ops/s），物化与停写在此尺度几乎无贡献；50GB 尺度的进一步劣化（3.4K ops/s）来自 64 文件/epoch 的 L0 堆积在单线程 compaction 下的债务限速。**P3 的正确排序应改为：写路径去串行化 → L0 消费端 → 每写固定开销微优化**。

---

## 2. 实验配置

规模：`fillrandom`，vs256 / key 16B / num=8,000,000（≈2.2GB 逻辑 → 8 个 epoch @ 256MB）/ 16 线程 / 无压缩 / WAL 开 / cache 8MB / write_buffer 256MB / max_background_jobs 16。与 50GB 基准同参（见 `zf_profile.py`）。

| 变体 | 差异配置 | 目的 |
|---|---|---|
| W0 | 原生 rocksdb db_bench | 参照 |
| W1 | ZF：P=64, stop=72, slowdown=64, K=8 | 复现 + 全量时间线 |
| W2 | ZF：slowdown/stop=10^6 | 隔离停写贡献（预测无效：W1 本就零停写） |
| W3 | ZF：`--zf_materialize_parallelism=1` | 定量 K=8 并行收益 |
| W2b | ZF：compaction trigger 同抬 10^6 | 隔离 WriteController delay-token 限速 |

微实验（2000 ops、独立目录、干扰可忽略）：单线程 / 线程=2,4,8 / P=1,4,16 / strace -c / 原生单线程。

插桩：`tools/db_bench_tool.cc` 新增 `--zf_materialize_parallelism` 与结束时打印全部 15 个 `rocksdb.zeroflush.*` 属性（不触碰核心逻辑，增量编译）。

---

## 3. 主结果

### 3.1 变体对照

| 指标 | W0 原生 | W1 ZF 基线 (K=8) | W2 无停写 | W3 K=1 | W2b 全 L0 触发器抬升 |
|------|--------:|-----------:|----------:|-------:|-------:|
| wall (s) | 8.0 | 734.4 | 732.7 | 734.3 | 767.2 |
| 吞吐 (ops/s) | 1,031,788 | 10,898 | 10,926 | 10,899 | 10,431 |
| 吞吐 (MB/s) | 267.6 | 2.8 | 2.8 | 2.8 | 2.7 |
| P50 (us) | 13.7 | 1,511 | 1,483 | 1,503 | 1,552 |
| P99 (us) | 29.3 | 2,826 | 2,826 | 2,816 | 2,842 |
| epochs（封存=物化=回收） | — | 8/8/8 | 8/8/8 | 8/8/8 | 8/8/8 |
| 物化总耗时 (s) | — | **12.2（1.7% wall）** | 13.4 | **85.5（11.6% wall）** | 13.9 |
| 每物化批次均值 (s) | — | 1.53 | 1.68 | 10.68 | 1.73 |
| compaction 作业数 / 总 wall(s) | 3 / 5.4 | 21 / 331.8 | 21 / 322.6 | 22 / 324.0 | 11 / 227.6 |
| L0 停写行数 / stall 秒 | 2 / 0.08 | **0 / 0.0** | 0 / 0.0 | 0 / 0.0 | 0 / 0.0 |
| 平均 CPU 核数 | 16.1 | 2.71 | 2.69 | 2.65 | 2.62 |
| 磁盘写 (MB/s) | 370.7 | 13.3 | 12.8 | 12.8 | 10.3 |
| install 直装/回落 L0 | — | 1 / 511 | 1 / 511 | 1 / 511 | 1 / 511 |

**四个 ZF 变体端到端全部相同（10.4~10.9K ops/s）**，与唯一被测差异变量的关系：
- **W1 vs W3（K=8→K=1）**：物化总耗时×7（12.2→85.5s），吞吐/P50/P99 纹丝不动——**物化与写吞吐瓶颈彻底无关，且 K=8 并行真实生效（7× 加速）**。
- **W1 vs W2（slowdown/stop→10^6）**：所有指标不动——**P1 式触发器调参在此尺度零效果**（触发器本就从未触发）。
- **W1 vs W2b（连 compaction trigger→10^6，L0 侧所有限速摘除）**：吞吐 −4%（且 compaction 少了 10 个作业）——**L0 侧整体摘除后写吞吐仍钉在串行天花板上**。

### 3.2 微实验（基础写路径）

| 配置 | P50 (us) | 聚合 (ops/s) | 说明 |
|---|---:|---:|---|
| 原生 threads=1 | 5.35 | 89,162 | db_bench+原生 Put 全开销 |
| ZF threads=1, P=64 | **52.0** | ~19,000 | ZF 每 op 固定成本 ≈ 47us |
| ZF threads=2 | 121.4 | 13,953 | P50 ≈ 线程数 × 串行节拍 |
| ZF threads=4 | 483.9 | 7,881 | （4 线程受采样噪声，趋势一致） |
| ZF threads=8 | 721.5 | 11,085 | |
| ZF threads=16（smoke，无 epoch 封存、无 L0 文件） | 1,517 | ~10-20K | **无任何后台活动仍 1.5ms/op** |
| ZF threads=1, P=1 | 32.3 | 19,365 | ShouldSeal 扫描 ≈ 0.3us × P |
| ZF threads=1, P=16 | 44.9 | 16,187 | |

（strace：全进程 syscall 总时间 0.65s 且 futex 0.84s 集中于启动期 → 每 op 开销在用户态 CPU。）

### 3.3 W1 时间线特征

- 物化批次：每 epoch 1.43~1.62s / 64 文件，间隔 ~80s（= 256MB ÷ 2.8MB/s）
- L0 轨迹（flush_finished 快照）：恒 63~64 —— 每轮物化 +64，compaction 全量收走，从未越过 stop=72
- compaction：L0→L1 单作业 14~30s（subcompactions=1，cpu≈wall，如 job5：29.8s / 428MB / 1.69M records），另有 L1→L2
- 写线程全程状态 S、CPU ~0.5-1 核：16 线程在写队列 futex 上排队，不在任何 IO/stall 等待

---

## 4. 根因链（修订版）

```
ZF 写路径（db_impl_write.cc:1497-1541）
  mutex_.Lock()                          ← 每次 write group 都拿 DB mutex
    ShouldSeal()                          ← 扫描全部 P 分区（0.3us × P）
    WriteGroupToPartitionWal(group)       ← 逐条 Encode+Append+skiplist（~47us/op）
  mutex_.Unlock()
+ zeroflush_db.cc:640 强制 allow_concurrent_memtable_write=false
    ⇒ 16 写线程串成单队列，节拍 ~92us/op ⇒ 聚合上限 ~11K ops/s（2.8MB/s）
    （对照：原生 allow_concurrent=true 并行插 memtable ⇒ 1.03M ops/s）

每 epoch 物化 64 个 L0 文件（hash 路由 → 范围重叠 → 全部回落 L0，四个变体一致 511/512）
  ⇒ L0 常驻 63-64；L0→L1 compaction subcompactions=1（14~30s/轮）
  ⇒ 2.2GB：刚好消化（零停写，W2/W2b 摘除限速亦无变化）
  ⇒ 50GB（推定，需复测验证）：债务累积 → WriteController delay-token（threshold =
     2×compaction_trigger = 8，不进 stall 统计的隐形限速，抬 slowdown/stop 触不到）
     + write_rate 0.8×/轮衰减 + 周期 stop ⇒ 10.9K → 3.4K ops/s
```

对 `benchmark_analysis_report.md` 发现 2 的判定：
- **② 物化随机定点读 —— 不成立**：物化用 WalScanner 顺序整读封存代（`materialize_job.cc:534`），每 epoch 1.5s；`sealed_read_count=0`（fill 期间零定点读）。
- **③ 物化单线程 —— 不成立**：K=8 线程物化已实现（`materialize_job.cc:121-175`）且真实生效（W3 K=1 对照：物化 85.5s vs 12.2s，7×）；报告所引"job 81 cpu≈wall"实为 **compaction** 作业的单 subcompaction 特征（W1 的 21 个 compaction 全部 cpu≈wall，与物化无关）。
- **① L0 停写 —— 部分成立但机制错位**：停写与隐形限速真实存在于 50GB 尺度（推定），但 2.2GB 复现中 L0 从未触顶、W2b 摘除全部 L0 限速吞吐不变；且隐形限速走 `WriteController::GetDelay`（`column_family.cc SetupDelay`，threshold=2×compaction_trigger=8）路径，**抬 slowdown/stop（P1 修复）根本触不到它**。
- **发现 3"常态慢"的定性正确，归因错误**：P50 ~1.4ms 的来源是写队列排队（futex 等待），不是"每次写入常态性承受限速"。

---

## 5. 修订后的优化路线（P3 重排）

| 优先级 | 工作项 | 内容 | 预期收益 | 依据 |
|---|---|---|---|---|
| **P3-A（新，最高）** | **写路径去串行化** | ① `WriteGroupToPartitionWal` 移出 DB mutex 临界区（分区自有 `p->mu` 已可保护并发 Append；SlimMemTable skiplist 需评估 ConcurrentInlineArena/锁）；② ShouldSeal 改原子聚合计数（每 Append 原子累加，免逐分区扫描）；③ 评估放开 allow_concurrent_memtable_write | 串行节拍 92us → 目标 <15us ⇒ 写吞吐 5~15×（10.9K → 60~160K ops/s 区间），达到或接近原生 | C1/C3；微实验 52us@1 线程 |
| **P3-B** | **L0 消费端**（针对 50GB 尺度的增量劣化） | ① 验证范围路由（kStatic/kSampled）直装 base level（M3.2 代码已在，benchmark 从未启用）；② L0→L1 compaction 启用 subcompactions/多线程归并 | 消除 64 文件/轮的 L0 堆积 → 消除 50GB 尺度推定的隐形限速与停写（2.2GB 尺度已证明与写吞吐无关，此项须 50GB 复测验证） | C4；install 511/512 回落 |
| **P4** | 每 op 固定开销微优化 | EncodeZfRecord 复用 buffer、touched_set 改小数组、AddRecord 路径瘦身 | 47us → 目标 ~20us（叠加 P3-A 后仍有益） | C3；P=1 时 32us 下限 |
| ~~原 P3-a~~ | 物化并行化 | **已完成，无需再做** | — | C2 |
| ~~原 P3-b~~ | 物化顺序预读 | **已完成，无需再做** | — | C2 |
| P2 | 配置调优复测 | 不变，但在 P3-A 之后做才有意义 | — | — |

### 后续验证建议

1. **P3-A 完成后先跑 2.2GB 剖析套件**（`zf_profile.py`，~1h）验证串行节拍下降，再上 50GB。
2. **50GB 复测务必保留 LOG**（修改 `run_benchmark.py`：物化后解析再删除 DB 目录），否则无法分解 10.9K→3.4K 的增量劣化来源（债务限速 vs 停写 vs 深 LSM 的 mutex 争抢）。
3. 验收线：P3-A 后 16 线程 fillrandom 聚合 ≥60K ops/s（当前 10.9K）；若达成，物化（170MB/s）与 imm 队列（`max_pending_epochs=2` + 单 flush 线程）将成为下一瓶颈，届时再评估 P3-B 与 flush 并行化。

---

## 6. 产物索引与复现

- 驱动：`zf_profile.py`（`--variants W0,W1,W2,W3`）；解析：`zf_parse_log.py`
- 每变体：`result.json`（命令+终值）、`parsed.json`（物化/compaction/flush 事件）、`samples.csv`（每秒 CPU/线程/磁盘）、`stdout.txt`（interval stats + zf 属性）、LOG 在 `/tmp/zf_prof_db_W*`
- 关键代码位置：
  - 串行临界区：`db/db_impl/db_impl_write.cc:1497-1541`（`mutex_.Lock()` → ShouldSeal → `WriteGroupToPartitionWal` → Unlock）
  - 强制关并行：`zeroflush/zeroflush_db.cc:640`
  - K 路物化：`zeroflush/materialize_job.cc:121-175`；顺序整读：`materialize_job.cc:534`
  - 隐形限速：`db/column_family.cc` `SetupDelay` + `db/db_impl/db_impl_write.cc:2908-2945`（`WriteController::GetDelay` 睡眠不进 stall 统计）
  - 每分区扫描：`zeroflush/zeroflush_db.cc:136-149`（ShouldSeal 遍历 AllPartitionIds）

环境备注：perf_event_paranoid=4（perf 不可用）、yama ptrace_scope=1（不可 attach，可跟踪子进程 → strace 可用）。
