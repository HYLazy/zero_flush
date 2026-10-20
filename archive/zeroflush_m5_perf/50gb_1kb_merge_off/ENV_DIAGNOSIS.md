# 50GB/1KB 对比 DNF 的环境诊断（2026-09-12）

> 目的：回答"机器上是否有其他负载拖慢了两次 fill"，并给出复跑前置条件。
> 结论：**无外部负载**（load 22–26 全部来自本实验自身线程）；真正的瓶颈在
> **系统盘 SSD 的 flush/fsync 延迟**（并发写下尖峰 5–8 秒），该状态今天已复现。

## 1. 时间线（9/11–9/12）

| 时刻 | 事件 |
|---|---|
| 9/11 14:30:49 | ZF merge-off fill 启动（db_bench 日志头时间） |
| 9/11 19:38 | ZF fill 被杀（5h07m，SST 70.8GB/2227 文件，epoch=1121） |
| 9/11 19:38:12 | native fill 启动（同 run.sh 串联） |
| 9/11 19:46:37 | **全局 OOM**：`node` 进程 56GB RSS/81GB VM 被杀（详见 §4） |
| 9/12 11:52 | native fill 被杀（16h13m，DB 85GB，未产出结果行） |

## 2. 证据 A：无外部负载

### 2.1 负载时间线与本实验完全同步
`/var/log/hwdiag/monitor_metrics.tsv`（每 5 秒采样）：

| 时刻 | load1 |
|---|---|
| 09-11 13:30（实验前，机器空闲） | 2.09 |
| 09-11 14:00（实验前） | 2.03 |
| 09-11 14:45（ZF fill 进行中） | 24.27 |
| 09-11 19:36（native fill 前段） | 26.57 |
| 09-12 11:52（native fill 末段） | 22.43 |
| 09-12 12:00（杀掉 bench 后） | 2.44（当前） |

即：load 随本实验启动而升起、随进程被杀而立刻回落。db_bench 为 16 写线程
+ `--max_background_jobs=24`（+`--subcompactions=16`），IO 阻塞线程计入
loadavg —— **load 22–26 是自造的**（此前 DNF evidence 中"非本实验进程贡献"
的判断错误，已订正）。

### 2.2 进程清单（当前 + OOM 快照）
- `who` 无登录用户；进程按用户统计仅 root/embed/nobody/998/167 + 系统账户；
  无其他人类用户作业，无 ablate 类外部仿真进程，无遗留 db_bench。
- 常驻非本实验消费者（合计 CPU <3%、内存 <1GB）：
  - Ceph 监控栈：ceph-mgr ×2（252MB+13MB）、ceph-exporter、ceph-crash
  - 监控：prometheus（65MB）、grafana（73MB）、alertmanager、node_exporter
  - `hwdiag.sh monitor`（PID 1855，常驻）：每 5s 写 metrics + `journalctl -k
    --since 1m` + `nvidia-smi -q`；`temp-monitor.sh` 每分钟 cron
- OOM 时刻（9/11 19:46:37）内核 dump 的 109 进程全表中，除下述 node 外最大
  者即本实验自身：db_bench 382MB、zcode-cli 3.1GB。

## 3. 证据 B：SSD 退化（真正的瓶颈）

### 3.1 复现探针（今天 12:20–12:36，同参数 4M ops=4GB）
| 指标 | 本次探针 | 历史基准（9/2 cmp_1kb50 同参数） |
|---|---|---|
| 完成量 | 15m44s 仅 1.1M ops（**1,166 ops/s**） | 48M ops/1817s = **26,409 ops/s** |
| 长停顿 | `long op: 8099635 micros`（**8.1s**） | 0 次 long op |
| CPU | 全程 ~0.8 核（线程全在 IO 等待） | — |

与两次 DNF 日志同一签名：ZF `long op: 8570647 micros`（8.5s）、native
`long op: 1858115 micros`（1.86s）、ZFDBG-slowtask `build_ms=8094`（28MB 的
SST 构建耗 8.1s）。

### 3.2 fsync/flush 延迟实测
| 场景 | p50 | 尾部 |
|---|---|---|
| 空闲 | 0.67ms | max 1.17ms |
| db_bench 探针并发 | 0.84ms | **max 6745ms**，mean 143ms |

### 3.3 盘温度与阈值
- 盘：`WD_BLACK SN750 2TB`（消费级，固件 112000WD）
- `sensors`：Composite 79.8–83.8°C，`high = +83.8°C`，到阈值即报 **ALARM**
- 9/11 14:30 起（fill 全程）钉在 84.8–85.8°C，至 9/12 11:52 结束才回落
- 历史对照：9/5、9/6、9/7、9/8、9/10 的负载时段同样到过 84.8–85.8°C
  → 高温是"重度负载日常"，非 9/11 新变量（故仅凭温度不能解释速度差异）

### 3.4 裸盘微基准（同为今天、机器空闲）
| 项目 | 实测 |
|---|---|
| 8GB 顺序直写（dd oflag=direct） | 958 MB/s |
| 30GB 持续直写 | 514 MB/s（首 2GB 仅 135MB/s，后段 ~1GB/s） |
| 4KB 随机写（预分配，8 线程 O_DIRECT） | 64,021 IOPS / 250 MB/s |
| 4KB 随机读（预分配，8 线程 O_DIRECT） | 70,338 IOPS / 275 MB/s |

→ 原始吞吐/低队列随机 IOPS 正常；**问题集中在 flush（fsync/fdatasync）与
混合写负载**，即 RocksDB 的 flush/compaction 完成路径。

### 3.5 可疑诱因：未 TRIM 的已删除空间
- 文件系统：ext4 `/dev/nvme0n1p3`，挂载项 **无 `discard`**，尺寸 1.8T/用 59%
- `fstrim.timer` 为**周级**：上次 ~9/7，下次 9/14 01:18
- 9/11 上午清理删除了大量实验数据（数百 GB，10:34 起盘温首次破 75°C），
  这些块**尚未交还 SSD** → FTL 在后续写入时被迫做额外 GC（写成倍放大+
  flush 延迟尖峰）。9/2 历史对照跑得快，与"删除累积后 FTL 状态劣化"时序吻合。

## 4. 期间的系统级事件：3 次 OOM（62GB 内存）
| 时间 | 被杀进程 | 峰值内存 |
|---|---|---|
| 9/6 13:49:35 | db_bench | anon-rss 54.1GB |
| 9/6 16:46:28 | db_bench | anon-rss 54.2GB |
| 9/11 19:46:37 | **node**（zcode 工具侧，uid=1000） | anon-rss 58.7GB / vm 85GB |

9/11 那次发生在 native fill 开跑 8 分钟后，触发全局 OOM；node 被杀后
zcode 服务在 19:47–19:48 重启。该事件与 fill 慢无直接因果（占窗口 10 分钟），
但说明 62GB 内存下有工具侧进程失控风险，跑长实验前应确认无 node 类进程膨胀。

## 5. 复跑前置条件（按优先级）

1. **`sudo fstrim -v /`**（需 root，本会话无免密 sudo）——把已删除块交还 SSD；
2. **SMART 体检**：`sudo apt install smartmontools && sudo smartctl -a
   /dev/nvme0n1`，看 Percentage_Used / Data Units Written / Media Errors /
   Critical_Warning（消费盘也可能因磨损进入劣化状态）；
3. **散热**：确认 SN750 有无散热片、机箱风道；环境温度偏高
   （systin 53–54°C、auxtin2 63°C、CPU 空闲 41–58°C）；
4. **准入 gate**：处理后先跑 4M ops 探针，正常应 ≤3 分钟、无 `long op`；
   达标再跑 50GB 双臂（native 预期 ~30 分钟/臂）；
5. 跑正式实验时记录 fsync/温度/吞吐遥测（本轮即因缺遥测而多花数小时）。

## 6. 处置与复测（2026-09-12 12:30–13:00，已获 root）

### 6.1 已执行
| 动作 | 结果 |
|---|---|
| `fstrim -v /` | **交还 830.2 GiB**（2m49s）——证实大量已删除空间未 TRIM |
| `smartctl -a /dev/nvme0n1` | 盘健康：Percentage Used **13%**、Available Spare **100%**、Media Errors **0**、Critical Warning 0；已写 221TB、通电 30705h——**非磨损问题** |
| `wbt_lat_usec: 2000 → 0` | 关 WBT 节流；复测延迟未见明显改善（901ms），保留（RocksDB 对 NVMe 的常规调优） |
| `mount -o remount,commit=60 /` | journal 提交间隔 5s→60s；复测后探针由"零进度"恢复推进 |

### 6.2 内核阻塞点（`/proc/<pid>/task/*/stack` 实测）
```
ext4_sync_file → ext4_fc_commit → jbd2_complete_transaction → jbd2_log_wait_commit   # fdatasync 等 journal 提交
ext4_dirty_inode → jbd2__journal_start → add_transaction_credits → wait_transaction_locked  # journal 额度耗尽
blk_mq_submit_bio → __rq_qos_throttle → wbt_wait → rq_qos_wait                        # WBT 写回节流
```
即：设备写延迟升高 → WBT 限流 → journal 提交变慢/额度耗尽 → **所有写者被同时卡住**
（这就是 1.8s / 8.5s 长停顿的同源机制；实测设备平均写延迟 99–499ms、在途 IO 300–5663）。

### 6.3 fstrim 后复测（4M ops 探针）
| | 修复前 | fstrim 后 |
|---|---|---|
| 完成量/速率 | 1.1M ops/15m44s = **1,166 ops/s** | 3.2M ops/10m27s ≈ **5.1k ops/s**（前 90s 达 ~11.5k） |
| long op | 1984 次（8.1–8.57s） | 仍 1984 次（8.57s），未根除 |

盘温在空转 10 分钟后仍 78.8–83.8°C（`high=+83.8°C`，ALARM 常亮）——**降温能力不足是当前
最可疑的剩余约束**（历史 9/5–9/10 负载时段同样到过 84.8–85.8°C，故高温非唯一变量）。

## 8. 收尾状态（2026-09-12 15:3x，用户指示停止测试后）

- **测试已停**：ZF merge-off 50GB 臂于 15:1x 中止（≈2h1x），原生臂未开始；
  其量化证据落档于 `rerun_20260912/ABORTED_RUN_AMPLIFICATION.md`
  （install 276.9GB=5.08×、原生 compaction 4,019.6GB=83.7×、设备 4.57TB=93.7×，
  整分区重写 ~3M 条/次、l1score 钉在 6–7.5）。
- **风扇已恢复自动**：`pwm1/2_enable=5`，RPM ~1100。
- **磁盘清理**：删除中止实验 DB（74GB）、/tmp 遗留实验库（~120GB）、
  pip/uv 缓存（8.7GB）→ 可用空间 665G → **884G**；`/tmp` 138G → 0.6G。
- **再次 fstrim**：交还 **690.6 GiB**（1m39s）——避免清理后重新积累未 TRIM 空间
  再次诱发 flush 延迟尖峰（本轮事故根因）。
- **运行时调优**（重启即失效，如需保留需写入 fstab/sysctl）：
  `wbt_lat_usec=0`、`mount commit=60`。

## 9. 复现命令

```bash
# 负载/温度时间线（每 5s 一条）
awk -F'\t' '$1>="2026-09-11 13:30"' /var/log/hwdiag/monitor_metrics.tsv | awk 'NR%120==1'
# NVMe 温度告警
grep -c NVMe /var/log/hw-monitor.log        # 9/12：715 条 >=75°C
# 4M ops 探针（同参数缩小版）
cd /home/embed/hyl/metadata_offload
LD_LIBRARY_PATH=$PWD/source/rocksdb/build:/usr/lib/x86_64-linux-gnu \
  ./source/rocksdb/build/db_bench --benchmarks=fillrandom --key_size=16 --value_size=1024 \
  --num=4000000 --threads=16 --compression_type=none --cache_size=536870912 \
  --max_background_jobs=24 --subcompactions=16 --histogram=true --use_existing_db=0 --db=/tmp/probe
# 内核阻塞点（跑负载时采样）
pid=$(pgrep -x db_bench); for t in /proc/$pid/task/*; do sudo cat $t/stack | head -3; done
# 盘健康
sudo smartctl -a /dev/nvme0n1 | grep -iE "Percentage Used|Available Spare|Media and Data|Critical Warning|Temperature"
```

