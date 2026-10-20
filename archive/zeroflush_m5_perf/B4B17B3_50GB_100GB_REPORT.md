# b4b17b3 分支：50GB / 100GB × 1KB KV 读写测试报告

> 执行时间：2026-09-19 02:02 – 03:47（CST）
> 目标提交：`b4b17b3ca14f054bb2a7ac5619c81c63b5081df5`
> （"候选线清洗——实验性积压排空/收益攒批移出候选（含全部 env 开关）"，2026-09-17 18:11，分支
> `fix/zeroflush-recovery-visibility-fences`）

## 0. 结论摘要

1. **50GB 与 100GB 的 ZF 读写测试在 b4b17b3 上均未能完成**：两臂的写入阶段都在 **ZF 账本背压处硬停滞**，
   被看门狗取证后终止。50GB 口径停在 **2.69GB / 2.41GB**（复跑复现，2/2），100GB 口径停在 **14.41GB**。
2. 因此 **50GB / 100GB 全量口径的写吞吐、写延迟、读吞吐、读延迟均无有效数据**（fill 未完成 ⇒ db_bench 未输出汇总）。
3. **停滞不是环境问题**：同窗口、同参数、同判据的 **native RocksDB 50GB 对照完整跑通**（fill+read 全 rc=0）。
4. b4b17b3 上能测到的只有：**停滞点以下的 ZF 写 smoke**（1GB 规模）与**部分库上的 ZF 读测试**——
   两者口径与 50GB/100GB 全量测试不同，**不可当作该测试的结果**（详见 §4、§5）。
5. 停滞签名与既有记录（`RUN_MANIFEST.md` D-129 / D-130）**一致**，且 b4b17b3 早于那些调查 ⇒
   两条停滞路径在候选线 09-17 的提交上就已存在。b4b17b3 的提交信息称"候选线上不存在可达的崩溃路径"——
   就**崩溃**而言成立，但**可达的停滞路径确实存在**，这是一条应予反馈的结论差异。

## 1. 隔离与构建（如何做到"不影响正在进行的修改工作"）

- **独立 worktree**：`/home/embed/hyl/wt_b4b17b3`（`git worktree add ... b4b17b3`）。
  主工作树 `/home/embed/hyl/metadata_offload` 的源文件、in-source 构建产物（`source/rocksdb-zeroflush/`
  的 `CMakeCache.txt`/`CMakeFiles/`/`db_bench`/`zf_valcheck`）**全程未被触碰**。
- **独立构建**：in-source、`-DCMAKE_BUILD_TYPE=RelWithDebInfo`、`PORTABLE=0`、`nice -n 10`、`make -j20`
  （留 16 核给用户构建）；与候选线 c59fc41 的构建配置一致。产物 `db_bench` **14,163,752 字节，与
  c59fc41 的产物字节数完全相同**，可作为同配置佐证。
- **运行期让位**：所有 db_bench 均以 `nice -n 5` 运行；跑测全程本机 load 仅来自本任务。
- **独立 DB 目录**：全部位于 `output/zeroflush_m5_perf/b4b17b3_*/db`（`.gitignore` 已覆盖 `output/**/db_*/`）。

**构建期踩到的坑（需记录）**：vendored RocksDB 的 `cmake/modules/*.cmake`（10 个文件）
被 `source/rocksdb-zeroflush/.gitignore:28` 的 `*.cmake` 规则忽略、**从未入库**，只存在于各工作树的本地文件里。
因此**任何新建的 worktree 都无法 `cmake` 配置**（报 `Unknown CMake command "get_rocksdb_version"`）。
解决办法：从主树只读复制 `cmake/modules/` 到 worktree（本报告即如此）。

## 2. 运行身份（可复现）

| 项 | 值 |
|---|---|
| b4b17b3 | `b4b17b3ca14f054bb2a7ac5619c81c63b5081df5` |
| `db_bench` md5 | `b29d439da6359757aa99d7df96badf49` |
| `librocksdb.so.11` md5 | `609c747930a331a4f163472efa30ae38` |
| 源码路径 | `/home/embed/hyl/wt_b4b17b3/source/rocksdb-zeroflush` |
| 公共参数 | `--key_size=16 --value_size=1024 --threads=16 --seed=20260914 --compression_type=none --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 --histogram=true --statistics=false` |
| ZF 参数 | `--zeroflush --zf_partitions=16 --zf_value_cache_mb=64 --zf_base_merge=true`，`ZF_L0_TRIGGER=100000000` |
| 50GB 口径 | 写 `--num=48000000 --writes=3000000`（16×3M=48M 写）、路由 `sampled`；读 `--reads=8000000`（128M 次） |
| 100GB 口径 | 写 `--num=100000000 --writes=6250000`（16×6.25M=100M 写）、路由 `static --zf_static_span=100000000`；读 `--reads=2000000`（32M 次） |
| native 对照 | `source/rocksdb/build/db_bench`（md5 `25c4356b789562db9b7ead087419e631`），参数与 50GB ZF 臂逐字相同 |

b4b17b3 上**不存在**的开关/环境（故未使用）：`--zf_stall_probe_ms`、`ZF_BENCH_ORPHAN_RECOVER`、`ZF_DBG_BUSY`。
历史脚本（`c59fc41_50gb_1kb`、`bench100g_full`）用到的其余 ZF 开关在 b4b17b3 上均存在。

## 3. 结果总表

| 运行 | 阶段 | 吞吐 | 平均延迟 | 带宽 | P50 | P99 | P99.9 | 用时 | 结果 |
|---|---|---|---|---|---|---|---|---|---|
| **native 50GB 同窗对照** | fill | **16,852 ops/s** | 949.4 µs/op | 16.7 MB/s | 26.14 | 84.33 | 454.57 | 2848 s | **PASS** |
| （同窗对照） | read | **613,681 ops/s** | 26.0 µs/op | 468.7 MB/s | 12.93 | 122.25 | 904.21 | 208.6 s | **PASS**（命中 77.0%） |
| ZF@b4b17b3 50GB #1 | fill | — | — | — | — | — | — | 02:07:17→02:09:18 | **INCOMPLETE**，停于 2.69GB |
| ZF@b4b17b3 50GB #2（复跑） | fill | — | — | — | — | — | — | 03:23:57→03:26:08 | **INCOMPLETE**，停于 2.41GB |
| ZF@b4b17b3 100GB | fill | — | — | — | — | — | — | 02:11:48→02:23:38 | **INCOMPLETE**，停于 14.41GB |
| ZF@b4b17b3 写 smoke（1GB，非对照口径） | fill | 339,729 ops/s | 47.1 µs/op | 337.0 MB/s | 29.3 | 74.77 | 360.65 | 2.8 s | PASS |
| ZF@b4b17b3 读 50GB 臂**部分库**（非对照口径） | read | 152,753 ops/s | 102.8 µs/op | 5.6 MB/s | 97.01 | 167.01 | 169.79 | 838 s | PASS（命中 3.7%） |
| ZF@b4b17b3 读 100GB 臂**部分库**（非对照口径） | read | 156,341 ops/s | 101.9 µs/op | 80.1 MB/s | 96.5 | 167.64 | 227.79 | 204.7 s | PASS（命中 51.6%） |

- 表中"—"= 该阶段未完成、db_bench 未输出汇总，**不是 0，也不是缺失测量**。
- 读臂的 MB/s 与命中率成正比（未命中的读不计字节），故读臂只应以 **ops/s** 比较。
- **native 自身跨轮波动很大**：同参数历史轮次 fill 为 47,875 / 47,385 / 23,343 ops/s，本轮 16,852 ops/s；
  read 为 798,878 / 391,061 / 877,373 ops/s，本轮 613,681 ops/s。因此单轮数字不可当稳定基线。

## 4. 写 smoke（停滞点以下规模）——仅说明"健康期"表现，不可与 50GB 数字对比

配置同 50GB 臂，仅把写量降到 `--writes=60000`（16×60K = 96 万写 ≈ 1GB），**完整跑通**：

```
fillrandom : 339,729 ops/s  47.085 micros/op  337.0 MB/s  2.8 s  960,000 operations
Percentiles: P50 29.3  P75 33.1  P99 74.77  P99.9 360.65  P99.99 86666.7 (µs)
```

**为什么不可对比**：该 run 只有 2.8 秒、总量 1GB，几乎不触发稳态压实与 sink/L2 清场管线；
而 50GB/100GB 的写入瓶颈正在那条管线（见 §6 的写放大证据）。把它当作"ZF 写吞吐 = native 的 20 倍"
是错误解读——历史同窗对照显示 ZF 的 50GB fill 反而是 native 的 **0.34–0.59×**。

## 5. 读测试（部分库）——同样非 50GB/100GB 口径

因为写阶段停滞，可读的库只有停滞现场留下的部分库：

| 库 | 数据量 | 命中率 | 吞吐 | 平均延迟 | P50 | P99 |
|---|---|---|---|---|---|---|
| 50GB 臂现场 | 2.69GB（SST 1.27GB） | 294,197 / 8,000,000 = **3.7%** | 152,753 ops/s | 102.8 µs/op | 97.01 | 167.01 |
| 100GB 臂现场 | 14.41GB | 1,032,970 / 2,000,000 = **51.6%** | 156,341 ops/s | 101.9 µs/op | 96.5 | 167.64 |
| （参考）native 满库 | 31.69GB | 6,161,212 / 8,000,000 = 77.0% | 613,681 ops/s | 26.0 µs/op | 12.93 | 122.25 |

命中率差异巨大（3.7% / 51.6% vs 满库 77.0%）⇒ 这批读延迟/吞吐**不能用于 ZF 的读性能结论**，
只能说明：在这些部分库上读路径**没有卡死**，且 P99 分布很窄（167 µs 一带）。

## 6. 停滞取证（决定性证据）

两臂的共同线程签名（`thread apply all bt`）：

```
15 × rocksdb::WriteThread::AwaitState      ← 写线程等组提交
 1 × rocksdb::DBImpl::DelayWrite           ← 组组长空转
26 × rocksdb::ThreadPoolImpl::Impl::BGThread ← 后台线程全部空闲
 1 × rocksdb::Timer::Run
```

写控制器计数全为 0（`unscheduled_flushes_=0 bg_flush_scheduled_=0 bg_compaction_scheduled_=0
total_stopped_=0 total_delayed_=0`）⇒ **不是 RocksDB 自身的写控制器在限流**（与 D-130 的
"needs_delay=0 is_stopped=0" 一致）。

**路径 A：50GB 口径（sampled 路由）——账本 stop 水位越线且无人排空**

```
LOG: ZeroFlush FIFO bypass blocked at install: part=0 oldest_pending=0 < install_min=2
LOG: [JOB 11] ZeroFlush empty-edit: submit VersionEdit without AddFile (pending=1083.1MB, lm=11)
LOG: materialize install overlap detected; batched yielded to recovery
LOG: flush_finished lsm_state=[0,16,0,0,0,0,0]    ← L0=0、L1=16 文件、L2 空
LOG: Level summary: files[0 16 0 0 0 0 0] max score 0.09, estimated pending compaction bytes 0
ZFDBG-skip  no_merge=0 busy=0 batch_l0=0 ... bytes=0 sink=0 cross=0 act=16
ZFDBG-sched unsched=0 bgc=0 l1score=0.00 l2score=0.00 l1n=16 l2n=0 l2bytes=0
```

`zeroflush/zeroflush_db.h:182` → `uint64_t ledger_stop_bytes = 1ull << 30;  // 1GiB（验证期）`。
实测停滞时账本 pending = **1083.1MB（#1）/ 1110.7MB（#2）**，均已越过 1GiB 停止水位；
而 `sink=0`、`bgc=0`、L2 为空、`estimated pending compaction bytes 0` ⇒ **没有任何在跑的任务会去排空该账本**。
即：写入被账本背压停下，而排空者不存在 → 硬停滞。

**路径 B：100GB 口径（static 路由）——规划侧被 `being_compacted` 持续让位（D-129 签名）**

```
ZFDBG-skip  no_merge=0 busy=130 batch_l0=0 l0busy=0 dirconf=0 batchprev=0 bytes=0 sink=356 cross=0 act=4
ZFDBG-skip  no_merge=0 busy=137 ...                                                sink=366        act=12
LOG: flush_finished lsm_state=[0,8,96,169,0,0,0]   ← L1=8、L2=96、L3=169 文件
LOG: Level summary: files[0 8 96 169 0 0 0] max score 0.31, estimated pending compaction bytes 0
```

`busy` 稳步上升（130→137）且 `sink` 计数在涨（356→366），但写入无进展、写线程停在 `AwaitState`/`DelayWrite`、
26 个后台线程全空闲 ⇒ 与 D-129 记录的"存在没有持有者的 `being_compacted` 注册，规划侧持续让位"同类。

**停滞前的写放大（100GB 口径）**：`compaction_finished` 累计 `→L2 55.86GB（1287 文件）`、
`→L3 32.10GB（724 文件）`，合计 **87.95GB** 的压实输出，而当时仅写入 14.41GB 数据（≈6.1×，
且过程中仍在持续输出）——这解释了为何管线会把写入拖停。

**与既有记录的交叉验证**：

| 来源 | 提交/线 | 口径 | 停滞点 |
|---|---|---|---|
| 本报告 | b4b17b3（09-17） | 50GB sampled | **2.69GB / 2.41GB（2/2 复现）** |
| 本报告 | b4b17b3（09-17） | 100GB static | **14.41GB** |
| `RUN_MANIFEST.md` D-129（09-18） | bench-100g 线 | 100GB | 12.8GB |
| `RUN_MANIFEST.md` D-130（09-18） | bench-100g 线 | 30GB 参数 | ~13.4GB |
| `output/.../c59fc41_50gb_1kb/`（09-18） | c59fc41 候选线 | 50GB sampled | 有 `hang.LOG`，未跑完 |

⇒ b4b17b3 已含这两条停滞路径，且 **50GB/sampled 口径停滞得更早**（2.4–2.7GB，比 09-18 记录的 12–13GB 更早）。

## 7. 方法论修正（本次执行中的一处失误，如实记录）

读阶段的第一次尝试（`b4b17b3_followups/readpartial50|100.*`）被看门狗判为"停滞"并终止，**这是误判**：

- 看门狗沿用了写阶段的判据——"DB 体积 + LOG 大小连续 3 次（30s）不变"。但**读负载不改变 DB 体积、
  LOG 也几乎不写**，该判据对读阶段无效。
- 误判的旁证：当时抓到的线程栈显示 16 个读线程正在 `DBImpl::GetImpl` / `InlineSkipList` /
  `Version::Get` / `TableCache::Get` 里**干活**，并非 futex 等待。
- 修正：改用**进程 CPU 时间**（`/proc/PID/stat` 的 utime+stime）作进展信号——真正卡死时 CPU 时间不增长。
  重跑（`read_retest.sh`）后两相均 **rc=0 正常完成**（`watch=0`），CPU ticks 每 30 秒稳定增长约 48K（≈16 线程满载）。
- 结论：`readpartial50.bt_all.txt` / `readpartial100.bt_all.txt` 是误判产物，**不可当作"读停滞"证据**；
  重跑结果见 §5。

## 8. 证据与复现路径

| 内容 | 路径 |
|---|---|
| 50GB ZF 跑法（含看门狗） | `output/zeroflush_m5_perf/b4b17b3_50gb_1kb/run.sh` |
| 100GB ZF 跑法 | `output/zeroflush_m5_perf/b4b17b3_100gb/run.sh` |
| native 同窗对照跑法 | `output/zeroflush_m5_perf/b4b17b3_50gb_native_ctrl/run.sh` |
| 后续取证（部分库读 / 写 smoke / 复跑） | `output/zeroflush_m5_perf/b4b17b3_followups/run.sh` |
| 读阶段重测（CPU 时间判据） | `output/zeroflush_m5_perf/b4b17b3_followups/read_retest.sh` |
| 停滞现场 LOG / 线程栈 | `b4b17b3_50gb_1kb/{fill.hang.LOG,hang_bt.txt,stall_cond.txt}`、`b4b17b3_100gb/` 同名文件、`b4b17b3_followups/repeat.LOG` |
| 延迟/吞吐统计脚本 | `output/zeroflush_m5_perf/b4b17b3_stats.py <fill.log|read.log> ...` |
| 各相 run.log | 各目录下 `run.log`、`*.watch.log` |

## 9. 清理

- 保留：两臂**停滞现场 DB**（`b4b17b3_50gb_1kb/db` ≈1.4GB、`b4b17b3_100gb/db` ≈14.2GB）及其 LOG/栈快照。
- 删除（本任务生成、可复现、无取证价值）：`b4b17b3_50gb_native_ctrl/db`（≈32GB，已完整跑通）、
  `b4b17b3_followups/db_repeat`、`b4b17b3_followups/db_smoke`。
- **未触碰**用户此前的任何实验目录（`bench100g_*`、`c59fc41_*`、`native_50gb_*` 等）。
