# fillcpu — 默认配置下的 CPU/内存归因驱动

回答的问题：**在 RocksDB 默认配置、1KB KV / 100GB 负载下，flush 用了多少 CPU、各操作
占多少 CPU、内存用了多少、各部件各占多少。**

## 组件

| 文件 | 作用 |
|---|---|
| `bench/fillcpu.cc` | 驱动：纯默认配置打开 DB（只设 `create_if_missing` + 只读 EventListener，**不开 Statistics**），16 线程随机序写 1KB KV，采样逐线程 CPU / 内存部件 / 逐作业 flush、compaction 记录 |
| `bench/build_fillcpu.sh` | 构建 `bench/tools/fillcpu`（链接 `source/rocksdb` 的 `librocksdb.so.11`，未修改库） |
| `bench/run_fillcpu_1kb100.sh` | 正式实验：96M 键 × (16B key + 1KB value) = 99.84GB，fill + drain |
| `bench/run_fillcpu_calib.sh` | 校准：1/4/16 线程小负载，量写路径固有 CPU/op（锁竞争对照） |
| `bench/fillcpu_analyze.py` | 生成报告：`python3 bench/fillcpu_analyze.py <out_prefix>` |

## 测量方法（为什么可信）

* **逐作业 CPU**：`OnFlushBegin → OnFlushCompleted` 之间用 `CLOCK_THREAD_CPUTIME_ID`
  夹逼；两个回调都在该 flush 自己的后台线程上（`DBImpl::FlushMemTableToOutputFile`）。
  compaction 同时记录库自报的 `CompactionJobStats.cpu_micros` 作为第二读数。
* **逐线程 CPU**：`/proc/self/task/<tid>/stat` 的 `utime+stime` 精确快照；线程按名字分角色
  （`rocksdb:high`=flush 池、`rocksdb:low`=compaction 池、`fcw*`=写线程）。
* **内存部件**：DB properties（`cur-size-all-mem-tables`、`block-cache-usage`、
  `estimate-table-readers-mem`）+ RSS/VmHWM + `mallinfo2`，每 250ms 采样。
* **第三方交叉验证**：库自己的 `rocksdb.cfstats`（L0 行 `CompMergeCPU(sec)` = flush CPU，
  L1+ 行为 compaction CPU），在 8GB 验证跑上与驱动测量相差 0.4%。

## 用法

```bash
bash bench/build_fillcpu.sh
bash bench/run_fillcpu_1kb100.sh          # ~1h（默认配置下 compaction 是瓶颈）
python3 bench/fillcpu_analyze.py bench/out/fillcpu_1kb100_default/run
```

输出（`<out_prefix>.*`）：`.summary.json`（两阶段角色 CPU、作业总账、内存末态）、
`.samples.csv`（时间线）、`.thread_cpu.csv`（逐线程累计 CPU）、`.flush_jobs.csv`、
`.comp_jobs.csv`、`.stalls.csv`、`.cfstats_{fillend,drainend}.txt`、
`.effective_options.txt`（库自报的生效默认值）、`.REPORT.md`（分析脚本生成）。

## 关键实现选择

* 键序用 28-bit Feistel + cycle walking 的 O(1) 内存置换（`l0bench` 的 `--order=shuffle`
  用 768MB 置换表，会污染内存测量）。
* 写线程在写完范围后阻塞在自旋等待里（不耗 CPU）直到 fill 末快照完成，否则
  `/proc` 里已退出的线程会让它们的 CPU 从角色归因里消失。
* drain 阶段（停写后等引擎静止）单独计时，用于把全部 flush/compaction 计完。
