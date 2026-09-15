# 运行清单（配置 / 结果 / 复现）

> 原始 `.LOG`（DB info log，5–7MB/个）与实验 DB 目录**不入库**（见根 `.gitignore`：
> `output/**/*.LOG`、`output/**/fill_*.log`、`output/**/db_/`）。
> 本文件记录每次运行的配置、关键结果与复现命令；失败 key 清单单独入库
> （`ledger/triage_fail_keys.txt`）。
> 二进制：`source/rocksdb-zeroflush/build_rel/{db_bench,zf_valcheck}`（构建产物不入库：
> `mkdir -p build_rel && cd build_rel && cmake .. && make -j db_bench zf_valcheck`）。

## A. step 0a：sink 受控对照（dir: `sink_tuning/`）

公共参数（`run_arm.sh`）：16 线程 / 16 分区 / kSampled / `--num=48000000 --writes=1000000`
（每线程 1M，共 16M 写）/ 1KB value / `--zf_base_merge=true` / cache 512MB / L2 清场 4GB。

| 臂 | 环境 | 吞吐 ops/s | 总输出 GB | 备注 |
|---|---|---|---|---|
| base（历史，旧二进制） | `ZF_SINK_WHOLE_RANGE=1` | 38,692 | 105.9 | 1ec2f45 提交时的记录 |
| A（历史，含 UAF） | 默认（窄） | 249,430 | 0 | **作废**：UAF 致 picker 收到垃圾区间 |
| B（历史） | `ZF_SINK_WHOLE_RANGE=1 ZF_SINK_L2_CLEAR_MB=8192` | 43,350 | 119.5 | — |
| base2 / base2b / base2c | `ZF_SINK_WHOLE_RANGE=1`（同二进制复跑） | 46,479 / 43,667 / 34,814 | 118.6 / 118.0 / 85.6 | 末态 L0 = 3 / 38 / 40，方差源于"未完成工作量" |
| base3 | 默认（整分区）+ 排空 | 41,542 | 127.0 | `waitforcompaction` 后 remaining=0 |
| A0a / A0b / A0c / A0d | 窄下沉（`ZF_SINK_NARROW=1`） | 38,331 / 43,071 / 33,520 / 30,873 | 121.0 / 129.3 / 131.2 / 129.9 | 请求成功率 12%，clean-cut 失败 ≈1500/次 |
| B1 / B1b | `ZF_SINK_BATCH=1` | 35,372 / 32,240 | 118.2 / 126.9 | 合批判据 0 次生效（每分区仅 1–2 个 L1 文件） |

每臂实际输入构成（`built` 计数，稳定指标）：L1 输入 ≈52MB/次、L2 输入 ≈200MB/次、
L2/L1 ≈ 3.9–4.0、每次下沉取 1 个 L1 文件；下沉写入 L2 的文件中位 66.6MB。

复现：`sink_tuning/run_arm.sh <臂名> "<env>"`；矩阵 `run_matrix_0a2.sh`、`run_matrix_0a2b.sh`、`run_matrix_0b1.sh`；
分析 `analyze_0a2.py <臂名...>`。结论见 `sink_tuning/REPORT_0A.md` 与 `REPORT.md`（含 A/AB 臂作废更正）。

## B. step 0b-3：ledger / 背压 / 空 edit / 校验器（dir: `ledger/`）

公共诊断参数（与 0a 不同）：16 分区 / kSampled / `--zf_base_merge=true` /
封存阈值降到 `epoch_target=16MB`、`partition_target=4MB`（使小数据也走封存→物化路径）。

| 运行 | 配置 | 结果 |
|---|---|---|
| smoke A | 400K 写、默认水位 stop=1GiB | pending→0、entries=0、drained 1.45GB、峰值 332MB、waits=0 |
| smoke B | `--zf_ledger_stop_mb=32 --zf_ledger_resume_mb=24` | `backpressure_waits=14`、`timeouts=0`、运行正常结束 |
| smoke C | 同上 + `--zf_bp_timeout_ms=1` | `waits=1971 / timeouts=1964 / bp_retries=15642`、退出码 0、无死锁 |
| db_ee 系列 | `ZF_FORCE_EMPTY_EDIT=N`（N=2 / 1000） | 空 edit 路径：pending 不变、barrier 推进、`handoff_installed_filtered>0`（已安装代被拦下） |
| db_vc / db_tri 系列 | `zf_valcheck` + `ZF_VC_KEEP_WAL=1` | **triage：B1=0 / B2 / B3 分布见下** |

**校验器模式**（`zf_valcheck <dbdir> <mode> <N> [threads]`）：
`write`（只写不 Flush）/ `walhit`（物化前，断言 WAL 读>0）/ `l1hit`（Flush 后）/
`verify`（重启后）/ `scan`（全量扫描并分类不匹配）/ `probe`（取值诊断）/
`deep`（扫 WAL）/ `triage`（WAL + 全层 SST 证据，B3 再拆 L0 / L1+）。

**0b-3 基线验收（通过）**：build pass；smoke A/B/C pass；ledger accounting pass
（pending / drained_materialize / reclaimed 三者自洽，`registered_gens` 结清）。

**已知失败门（未修，故意保留）**：

```
triage（40K 键 / 保留 WAL）: B1_not_written=0  B2_in_wal=980  B3_visible_in_sst=2303
                             B3 拆分: in_L0=2303  in_L1plus=0   （seq2>seq1 = 2303/2303）
scan  : lost_overwrite≠0（实测 2227 / 3039 / 14053，随运行状态漂移）
```

复现：
```bash
ZF=source/rocksdb-zeroflush/build_rel; export LD_LIBRARY_PATH=$ZF:/usr/lib/x86_64-linux-gnu
cd output/zeroflush_m5_perf/ledger
ZF_VC_KEEP_WAL=1 $ZF/zf_valcheck db_vc write 40000 4     # 只写（阈值 16/4MB，触发封存）
ZF_VC_KEEP_WAL=1 $ZF/zf_valcheck db_vc scan  40000 4     # 期望 lost_overwrite=0 —— 当前失败
ZF_VC_KEEP_WAL=1 $ZF/zf_valcheck db_vc triage 40000 4    # 三桶判决 + 索引链
```

归因（已证）：恢复用"文件是否存在"推断"当前 active / 仍需物化"（R0）+ 索引命中即返回、
不与已安装层比较 sequence（R2）⇒ 恢复重放的旧 WAL 记录遮蔽 L0 中更高 seq 的新版本；
陈旧遮蔽 100% 发生在 L0（R3）；失败集随开合循环漂移（R4）。
修复顺序与回归门见 `ledger/REPORT_0B.md` 第 10 节。
