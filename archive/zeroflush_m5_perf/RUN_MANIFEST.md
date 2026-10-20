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

## C. 基线标识（便于二分与追溯）

```
parent (0a/0b 之前的起点) : 1ec2f45  ZeroFlushV0 tip
diagnostic content commit  : 750e458  amend 前的原始提交（悬空对象，不参与后续二进制/二分）
clean baseline commit      : 665f434  含 .gitignore（构建产物 / 原始 LOG / 实验库已忽略）
repair branch root         : 665f434  @ fix/zeroflush-recovery-visibility-fences
branch                     : ZeroFlushV0.5 亦指向 665f434
```

> 说明：665f434 **替换**（amend）而非接续 750e458，两者无父子关系；`750e458` 仅存在于
> reflog/对象库，`git merge-base --is-ancestor 750e458 HEAD` 为假。二分请以 **665f434** 为基线。

## D. 修复分支进展（fix/zeroflush-recovery-visibility-fences）

| 提交 | 内容 | 门禁 |
|---|---|---|
| `665f434` | 基线（诊断全部内容 + 干净工作树） | **红**：triage fail=2144（B3 2303/拆分全 L0）、scan lost_overwrite≠0 |
| `69bb3d0` | step 1 部分：显式 `ActiveGenOpt`（nullopt ≠ gen 0）、恢复不再用 `MaxGen` 定义 active、`InsertCreate` 按显式身份归位（同物理分区 + 同活跃代才进 active） | **仍红**：triage fail=1989（B3 1768、B2 221） |

失败键清单按阶段留档：`triage_fail_keys.txt`（基线 2144）、`triage_fail_keys_step1.txt`（step 1 后 1989）、`triage_fail_keys_step1c2_db_fence4.txt`（step 1c-2 后 2806，见 §D-2/D-3；按桶细分 `triage_fail_keys_b2.txt` = 240、`_b3.txt` = 2566）。

新实证（推翻"编号复用"旧假设）：记录物理属于 `part=4`、索引分区为 `part=10`，
而 `part=10 active_gen=1` —— 另一条来自 `part=4 gen=1` 的**旧**记录因"gen 号
相等"进入 active，读序 active 优先 ⇒ 遮蔽 frozen 链中 `seq=59905` 的新版本
（同时 L0 已有该新值）。**跨物理分区的 gen 号不可比较**（用户第一轮质疑成立）。

剩余根因（未修）：恢复仍把"其数据已安装"的旧代记录登记为可读 pending，且读路径
索引命中后不与已安装层比较 sequence。下一步 `installed_fence + 恢复过滤` 需先定
原子性方案：① MANIFEST/VersionEdit 内新增字段（改格式）② 一次性 reconciliation
（无需格式改动，按 LSM 覆盖判定是否已安装；用户已列为 legacy 路径备选）。

### D-2. step 1c-2：fence 的"输入完整消费"判据（两套统计）— 未提交

用户 2026-09-15 定稿的判据：**输入消费量**与**输出量**必须分开统计；generation 只有
在"输入被完整读取 + 任务成功 + 真实 AddFile/LogAndApply 成功"三者同时成立时才可
推进 fence。期望值 `sealed_expected_*` 在**封存时刻**记录（不得用物化输出反推——
物化会合法过滤旧版本/删除标记/合并记录）。

实现（未提交，工作树内）：

- `PartitionedWalManager`：`Partition::rec_count`（Append 逐条累计）+ `GenSealMeta
  {bytes, records, records_known}`；`Freeze` 冻结期望（`FreezeResult::sealed_records`），
  重开路径按磁盘字节登记（`records_known=false`，判据退化为字节相等）；同一
  `(part,gen)` 二次封存 → `seal_meta_conflicts`（代身份守卫信号）+ 保留较小期望。
- `ZfMaterializeJob`：两处 A 侧扫描循环累计 **输入消费**（每条读入并处理的记录，
  即使之后被过滤；字节取 `WalScanner::offset()`）；输出侧单独计数
  （`output_records_/output_bytes_`，诊断量，与输入不可比）。
  `ConsumedRanges()` 逐代判 `consumed_* == expected_*`（records 可信时也比条数），
  首代不完整即截断区间（保守）；未完整 → `fence_input_incomplete_count`，不推进。
- `flush_job.cc`：fence 只在安装路径装配（让位/空 edit 路径在此之前 `return`，
  结构性排除）；`f.consumed_record_count/consumed_bytes` 填**真实消费量**；
  `ZF_DBG_FENCE=1` 打印
  `physical_part / generation / exp_rec,exp_bytes / cons_rec,cons_bytes / out_rec,out_bytes / complete`。

验收（`ZF_VC_KEEP_WAL=1`，16 分区 / kSampled / epoch_target=16MB / partition_target=4MB，
`zf_valcheck <db> write 40000 4` → `scan` → `triage` → `agg`）：

| 运行 | fence 行 | 推进区间 | cons==exp | fence_gap / hole / input_incomplete | fifo_blocked / bypass | scan lost_overwrite | triage 分桶 |
|---|---|---|---|---|---|---|---|
| db_fence3 | 28 | 12 | 28/28 | 0 / 0 / 0 | 0 / 0 | 2444 | B2=523 B3=1921（L0 1784 / L1+ 137） |
| db_fence4 | 32 | 16 | 32/32 | 0 / 0 / 0 | 0 / 0 | 2806 | B2=240 B3=2566（L0 2566 / L1+ 0） |

**关键验收（用户定义）：PASS ×2**

```
ACCEPTANCE: B2_records_with_gen_le_fence=0 B2_records_with_gen_gt_fence=523 -> PASS
            （即：不再出现 gen <= fence 但该 generation 仍有 WAL-only seed2 的记录）
```

B2（唯一副本仍在 WAL、尚未物化）的记录全部落在 fence **之上**的代
（part2 gen1>0、part4 gen6>5、part7 gen1>0、part8 gen5>4）——属"尚未物化"，不再被
恢复期误当"已安装"过滤。

**剩余失败的归因**（均不在 fence 判据内，是读路径问题；triage 新增逐代归属）：

1. `index_chain_hit`（1452/1749 键）：读在索引链命中陈旧 WAL 记录后**立即返回**，
   不与已安装层比较 sequence。证据：`B3_stale_seq_below_sst_seed2 = 全部`（陈旧记录
   的 seq 上界仍低于 SST 中 seed2 的 seq ⇒ 只要做一次序列号比较就会返回正确值）。
   陈旧记录所在代：parts 9/10/11/13 = 该 part **无 fence**（只有活跃 gen0，从未封存，
   故恢复期登记其 WAL 是正确的）；parts 0/1/3/5/6 = gen0 被 fence 覆盖。
2. `index_chain_miss`（817 键）+ `in_L0` 全量：读从 LSM 返回了**旧版本**，尽管新版本就在
   另一个 L0 文件里。样本 v=1：`L0 file=92 seq=66 cls=1 fseq=[66,4131]`（旧）vs
   `L0 file=60 seq=40002 cls=2 fseq=[70,43840]`（新），两文件元信息都覆盖该键
   （`covers=1`）。L0 读按**文件号降序**首命中即停 ⇒ ZF 把**旧记录重新物化成更高
   文件号**，违反"L0 文件号序 = 数据新旧序"不变量。这是 L0 fallback 安装路径的问题，
   与 0b-4（无 L0 受控实验）同一工作流。

复现（in-source 构建；`build_rel` 为旧二进制，需重建）：

```bash
SRC=/home/embed/hyl/metadata_offload/source/rocksdb-zeroflush
export LD_LIBRARY_PATH=$SRC:/usr/lib/x86_64-linux-gnu
cd output/zeroflush_m5_perf/ledger
ZF_VC_KEEP_WAL=1 ZF_DBG_FENCE=1 $SRC/zf_valcheck db_fence4 write 40000 4   # fence 期望/消费/决策全量日志
ZF_VC_KEEP_WAL=1 $SRC/zf_valcheck db_fence4 scan   40000 4
ZF_VC_KEEP_WAL=1 $SRC/zf_valcheck db_fence4 triage 40000 4   # 含 B2/B3 逐代归属 + ACCEPTANCE 行
ZF_VC_KEEP_WAL=1 $SRC/zf_valcheck db_fence4 agg    40000 4
```

门禁：**仍红**（scan/triage 未全绿：B2=240、B3=2566、lost_overwrite≠0）；本步只解决
"fence 过早推进"，读路径（sequence 比较）与 L0 安装序仍在待修列表。未提交任何代码。

### D-3. step 1d：单次失败 Get 的真实来源追踪（未提交）

用户 2026-09-15 的有界任务：把**同一次** Get 的索引侧与 LSM 侧证据串起来，
字段 = `key / read_sequence`、索引 `logical_part / physical_part / gen / 命中 seq、type`、
LSM `实际访问顺序 / level / file / 命中 seq、type`、最终 `返回来源 / seq / type`。

**证据载体（已冻结，避免反复重开改变证据）**

```
二进制 : source/rocksdb-zeroflush/zf_valcheck          sha256 bf49f2bf9d49e6e69033aa4dbbc6f837b3899439cf4cfa1c1281423ba79ba79c
动态库 : source/rocksdb-zeroflush/librocksdb.so.11.2.0 sha256 3b4d173da7534e5c251ec2a9681a3b5bb7597a330d5f57cf5687dea5e0084b5d
样本   : ledger/frozen_f4/run0..run3（= db_fence4 的四份副本，每份 116MB，每次追踪用一份新副本）
         （门禁数字原由加追踪前的二进制产生；追踪版二进制重跑 frozen_f4/run2 得同一数字：
          fail=2806 B2=240 B3=2566 ACCEPTANCE=PASS，仅新增日志、无行为改动）
```

追踪实现（`ZF_DBG_GET=<v>[,<v>]`，键编码 = BE64(v)+8×'0'，与 `zf_keygen::KeyFromInt(v,16)` 同源）：
`zeroflush/get_trace.h` + 四个只读钩子 ——
`PartitionIndexSet::Get`（逐链元素 hit/miss + physical locator）、
`ZeroFlushContext::GetFromPartitionIndex`（路由分区/别名候选/最终来源）、
`Version::Get`（每个被访问文件的 level/file/seq_range/state_after，**真实访问顺序**）、
`DBImpl::GetImpl`（索引 miss 后 LSM 的最终 status）。读视图与 GetContext 同源
（`snapshot ? snapshot->seq : vset_->LastSequence()`）；tombstone 走
`source=INDEX(tombstone) value=NOTFOUND`，不是"比较两个 value"。

**追踪结果**（`verify` 模式 + `ZF_DBG_GET`，frozen 副本；read_seq=60000，snap=no，两端一致）

| 键（v） | 索引链 | 索引结论 | LSM 实际访问顺序 | 最终来源 | 机制 |
|---|---|---|---|---|---|
| 2, 11, 14, 15, 17, 20, 389, 392, 393 | `idx-hit logical_part=9/10/12/13 idx_gen=0 hit_seq=67..1610 hit_type=VAL phys_part=同 part phys_gen=0` | 命中即返回 | 未访问 | **INDEX seq=67..1610** | 索引返回旧值，LSM 有更新值 |
| 1, 4, 6, 16, 18, 394 | `idx-miss-all chain_gens=[5,4,1,7,8,1]` | 链内无此键 | `#0 level=0 file=92 seq_range=[66,4131] state_after=1(kFound)` → **停止** | **LSM file=92** | 真实 LSM 查找自身返回旧值 |
| 1031, 1032, 1037, 1042, 1043（B2 桶） | `idx-hit logical_part=9/10/13 idx_gen=0 hit_seq=4104..4116 VAL` | 只有 seed1 那一版 | 未访问 | **INDEX seq≈4104** | seed2 在**别的物理分区**的 WAL（B2_by_gen: part0/3/7 gen1），路由分区链里根本没有 |

四条修复方向（对应用户表格）：

1. **索引返回旧值、LSM 有更新值** → pending 后缀不变量与跨来源可见性：索引命中必须与
   已安装层比较 sequence（`B3_stale_seq_below_sst_seed2=2566/2566`，全部只需一次 seq 比较）。
   证据键：2/11/14/15/17/20（`hit_seq=67..1610` 对 SST `seq≈40003+`）。
2. **真实 LSM 查找自身返回旧值** → L0 安装序：`file 92 (seq_range [66,4131])` 是**更高
   文件号却更旧的数据**，L0 首命中即停（`state_after=1`）⇒ 数据更新的 `file 60
   (seq_range [70,43840])` 从未被访问。两个文件的元信息都覆盖该键（`covers=1`），
   故不是范围元数据问题，而是"L0 文件号序 = 数据新旧序"不变量被旧记录重新物化破坏。
3. **B2：seed2 在 WAL 却不在索引** → 恢复登记/路由/索引生命周期：同一 user key 在
   phase1 与 phase2 落到**不同物理分区**（每 epoch 重建路由表），读只查当前路由分区
   （+DRAINING/split 别名），未物化的旧分区 WAL 不在任何被查链中。
4. **索引内有 seed2 却选 seed1** → 本轮未观察到该形态（索引里只有 seed1 那一版，
   seed2 根本不在被查链内）——按证据应归入 3 而非"索引版本选择"。

**新增/更新工具**：triage 按桶输出 `triage_fail_keys_b1/b2/b3.txt`（本轮 B1=0 / B2=240 /
B3=2566），SST 证据行附带 `fseq=[smallest,largest]` 与 `covers=`；单键追踪经 `ZF_DBG_GET`。

复现：
```bash
SRC=/home/embed/hyl/metadata_offload/source/rocksdb-zeroflush
export LD_LIBRARY_PATH=$SRC:/usr/lib/x86_64-linux-gnu
cd output/zeroflush_m5_perf/ledger
cp -a db_fence4 frozen_f4/runN                       # 每份副本只跑一次
ZF_VC_KEEP_WAL=1 ZF_DBG_GET=2,11,14 $SRC/zf_valcheck frozen_f4/runN verify 40000 4
ZF_VC_KEEP_WAL=1 $SRC/zf_valcheck frozen_f4/runM triage 40000 4   # 桶清单 + 分代归属
```

**用户对本轮结论的收紧（已采纳，写入待办）**

- `B2_records_with_gen_le_fence=0` 只能证明**两次运行中未观察到**该类误过滤，不能证明
  所有 fence 都安全。fence 判据还需补一条：所消费的输入必须**属于最终成功安装的输出**
  ——重试、输出废弃、部分安装不得沿用旧的"消费证明"（待实现：零已安装输出的 job 不得推进）。
- B2 位于 `gen > fence` 只说明留在 WAL 的位置合理；**Get 返回 seed1 仍是正确性失败**，
  不按"正常 pending"结案。
- 文件号更大 ≠ 实际先被读——本轮已用 `Version::Get` 的实际访问顺序证实（file 92 先被
  访问且命中即停），不再依赖文件号推断。

### D-4. step 1e：C 修复（链内按序列号选择）— 未提交

**修复点**（`zeroflush/partition_index.h`）：逐键证据（见下）表明，B2 里的记录**不缺**
索引——它在读路径的链里，只是**位置序**把它排在了命中段之后。链内选择改为按
**序列号**：

- `PartitionIndex` 增加段内 `max_seq_/min_seq_`（`Insert` 时原子更新，`MaxSeq()/MinSeq()`）；
- `PartitionIndexSet::Get`（partition_index.h:847）按各段 `max_seq` **降序**访问，
  命中后若当前最优 seq ≥ 下一段 `max_seq` 上界即提前退出；删除标记同等参与
  （最高 seq 者胜出，由调用方映射 NotFound）。布隆快路径不变。
- 依据：gen 号跨物理分区不可比较，唯一可比的是 seq；位置序（active 优先）在
  "活跃段更旧、冻结段更新"时必然选错。

**同一个失败键的修复前后路径**（v=1031，frozen 副本，`ZF_DBG_GET=1031` + `recdiag`）

```
修复前 step1 : wal_rec phys_part=0 gen=1 offset=7476   seq=44104 class=seed2   ← 新版本（未安装）
              wal_rec phys_part=9 gen=0 offset=279860 seq=4104  class=seed1
       step2 : fence: part0 last=0（gen1>fence ⇒ 恢复期不会过滤它）; part9 none
       step4 : chain logical_part=9:
                 chain#0 part=9 gen=0 frozen=0 mem_bytes=41958 → seq=4104  loc(part=9 gen=0 off=279860)
                 chain#3 part=9 gen=1 frozen=1 mem_bytes=2646  → seq=44104 loc(part=0 gen=1 off=7476)
       get   : ZFDBG-idx-hit ... hit_seq=4104 → 返回 seed1（B2 桶，scan 判 lost_overwrite）

修复后 get   : ZFDBG-idx-miss idx_part=9 idx_gen=7 idx_frozen=1 seg_max_seq=59947
              ZFDBG-idx-miss idx_part=9 idx_gen=8 idx_frozen=1 seg_max_seq=59871
              ZFDBG-idx-hit  idx_part=9 idx_gen=1 idx_frozen=1 seg_max_seq=55876 hit_seq=44104 type=VAL
              ZFDBG-idx-select best_seq=44104 phys_part=0 phys_gen=1 phys_off=7476
              最终返回 seed2（eq_seed2=1）；提前退出只访问 3/6 段
```

**完整 B1/B2/B3 对照**（同一 frozen 样本 `db_fence4` 的独立副本；二进制哈希见下）

| 指标 | 修复前 | 修复后 |
|---|---|---|
| scan mismatch | 2806 (lost_overwrite) | **2566** |
| triage fail | 2806 | **2566** |
| B1_not_written | 0 | 0 |
| **B2_in_wal** | **240** | **0** |
| B3_visible_in_sst | 2566 | 2566（未被"转成 B3"） |
| ACCEPTANCE(gen≤fence 且 WAL-only) | PASS(0) | PASS(0) |
| 全新库端到端（write→scan→triage） | B2=523 / 240（两次） | **B2=0**（mismatch 2799，全 B3） |
| 回归 smoke（`all` 模式） | — | 结构不变量 OK：`wal_read_delta=0 errors=0`（mismatch≠0 属既有缺陷） |

**A/B 改为按"实际返回来源"统计**（此前用"新值所在层"统计属错误口径）：

```
SOURCE_all_fail: INDEX=1749 LSM=817          ← A(索引返回旧值) vs B(LSM 自身返回旧值)
SOURCE_B3:       INDEX=1749 LSM=817          ← B2 已归零
SOURCE_read_ok:  INDEX=2171 LSM=15243
```
A 与 B 之和 = 2566 = 全部剩余失败，与 `B3 中 seq2>seq1=2566`、`stale_seq_below_sst_seed2=2566` 一致。

**两处归因修正（已落实为打印字段）**

1. `chain_gens` 里重复的 `gen=1` **不是**"同一物理代两份身份"。实例字段已打印：
   `chain#0 part=5 gen=1 frozen=0 mem_bytes=0`（活跃实例，空）与
   `chain#3 part=5 gen=1 frozen=1 mem_bytes=2814`（冻结实例）——是同一 (index_part, gen)
   标签下的**两个不同实例**（active/frozen）；记录是否相同需看各自 locator：part9 的
   `chain#3 part=9 gen=1 frozen=1` 持有的记录物理落在 **(part=0, gen=1)**，即索引段标签
   与记录物理位置可以不同。跨 physical part 的裸 gen 不作比较。
2. `B3_in_L0=2566` 只是**新值所在层**，不是机制 B 的次数；A/B 已按返回来源统计（见上）。

**已知自伤与修复**：链内选择的第一版把 locator 拷进**函数栈缓冲**并返回其 Slice
（契约要求跨调用稳定）→ 调用方读到垃圾 locator（`bad part_id`，Get 变 NotFound）。
改为直接返回段内 arena 的 Slice 后恢复；证据：`ZFDBG-get-final source=INDEX(read_failed)
... status=Corruption: bad part_id`（修复前）→ `source=INDEX hit_seq=44104`（修复后）。

**二进制/样本**（C 修复版）
```
zf_valcheck          sha256 686fd94971abc801b384baa669ebc5bd1191de231781c3b3a1d5b7199d5e5803
librocksdb.so.11.2.0 sha256 4b3d8c695b8b0978d30bea4ae1c7428dcac6c94c60b00c9c315c46fbe97d5af3
frozen 副本：ledger/frozen_f4/{crun1,crun2,crun3,id1,r1031*,c1031*}（每份只跑一次）
失败键留档：`triage_fail_keys_step1e_db_fence4.txt`（2566，修复后同一样本）、
`triage_fail_keys_step1e_b2.txt`（**0 行** = B2 归零的证据）、`_b3.txt`（2566）。
```

### D-5. step 1f/1g：C 的三项补充检查 + B 的排序核实与归并实验（未提交）

**C 补充检查（用户 2026-09-15 指定）**

1. *快照/删除语义定向断言* —— 新增 `zf_valcheck <db> selftest`（自包含：写→快照→覆盖写→删除→重写）：
   `selftest put/delete/snapshot: PASS (del_now=1 snap_seed2=1 snap_seed1=1 re_put=1)`：
   最新视图在删除后为 NotFound（不是旧值）、旧快照见 seed1、新快照见 seed2、删除后重写见新值。
2. *剪枝上界* —— `max_seq_` 改为在 `InsertConcurrently` **之前**抬升（`partition_index.h`）：
   上界只能高估不能低估，否则并发读会因"已有新记录而上界仍旧"提前退出漏掉更高版本；
   插入失败（重复条目）只让上界偏大 ⇒ 多扫段，不影响正确性。排序与提前退出使用同一上界。
3. *locator 生命周期* —— 新增 `PartitionIndexSet::GetCopy(...)`（把 locator 拷进**调用方缓冲**），
   读路径改用该入口；契约从"指向段内 arena"改为"调用方持有"，不再依赖索引段存活
   （物化完成 `ReleaseFrozen` 后段可被释放）。WAL generation 生命周期由 ReadValue 自身的
   引用（活跃代句柄 / SealedFileCache 登记 + LRU 句柄）保证。
4. *WAL 命中回归* —— `wal_read_delta=0` 的 smoke **不能**证明 WAL 读可用（那正是"数据已物化"）。
   新增 `walread` 模式（写→同进程立刻读，不等物化）并新增引擎侧 `active_wal_read_count`：
   ```
   walread sources: INDEX=10451 LSM=4820 LSM_NOTFOUND=4709
   WAL_HIT_ASSERT: PASS (active=6700 sealed=3751)
   ```
   **新发现（另立线索）**：同进程写后立刻读的窗口里 `LSM_NOTFOUND=4709/19979` —— 已确认写入的
   数据在该窗口内既不在索引也不在 LSM（正是"未安装的已确认写入必须保持可读"这条不变量的违例）。
   与开合循环后的 B2 不同，这是**活跃窗口**内的可见性空洞，待单独立项（复现：`walread 20000 4`）。

**B-1 排序规则核实（代码 + 运行时）**

- 代码：`VersionBuilder::Rep::SaveSSTFilesTo`（version_builder.cc:1492）按
  `GetEpochNumberRequirement()` 选比较器；ZF 输出带 `epoch_number`（安装时 `NewEpochNumber()`）
  ⇒ 走 `NewestFirstByEpochNumber`（version_builder.cc:63）：**epoch 降序**，同 epoch 再按
  `largest_seqno` → `smallest_seqno` → 文件号降序。读路径按该顺序遍历并在**首个非 NotFound 命中处停止**。
- 运行时（frozen 样本，v=1）：`ZFDBG-l0-order` 实测 61 个 L0 文件按 epoch 100→20 排列；
  `file=92 epoch=85 seq=[66,4131]` 排第 15 位，`file=60 epoch=53 seq=[70,43840]` 排第 47 位；
  读到第 15 位即命中停止 ⇒ 返回 seq≈66 的旧值，**file 60 从未被访问**。
- 关键：这 61 个文件的 seq 区间**交错**（如 `file=103 epoch=96 seq=[199,15609]` 与
  `file=100 epoch=93 seq=[135,58752]`）⇒ **按最大 seq 重排或改文件号都不能保证逐键正确**。

**B-1b 为什么落到 L0**（运行时）：`ZFDBG-pick vsstorage base=1 lvl=1 new=[...] hit=file9 [...]`
共 48 次/次写周期 —— `PickLevel`（materialize_job.cc:2495-2567）在
"L0..base 任一层有文件与输出范围重叠"或"有运行中 compaction 范围冲突"时 **回落 L0**。
即：base 层已被先前的安装占用 ⇒ 后续输出装不进 base ⇒ 落 L0。叠加"后一批物化了更旧的代"
（乱序安装）⇒ L0 的**安装序**与**数据新旧序**不一致 ⇒ 首命中即错。

**B-3 既有库的安全读取实验（`l0merge`）**：把污染 L0 一次性归并进 base 层
（`CompactRange(change_level=true, target_level=1)`），归并后 `L0=0 / L1=1`，重新判决：

| 指标 | 归并前 | 归并后 |
|---|---|---|
| **LSM 返回旧值的失败（B 类）** | **817** | **0** |
| INDEX 返回旧值的失败（A 类，单列） | 1749 | 1749 |
| 总失败 | 2566 | **1749** |
| B2 / ACCEPTANCE | 0 / PASS | 0 / PASS |

结论：**逐键正确性只能靠"每层 key 不重叠 + 跨层按 seq"这一结构保证**；归并是既有库的
安全读取路径，而"不满足顺序就保留 WAL、让位"只防新错、修不了已存在的 L0 关系。

**B 的修复方向（下一轮实施）**

1. 后续安装（防新错）：输出只允许装到"与既有文件不重叠"的位置——`PickLevel` 的 L0 回落
   改为"合并重叠文件"（复用已存在的 L0 融合路径：把重叠 L0/base 文件并入 B 侧输入，
   输出装 base）或"保留 WAL + 让位"；禁止把重叠文件追加进 L0。
2. 既有库（读安全）：打开时若 L0 非空且存在重叠 ⇒ 触发一次 L0→base 归并（与本次实验
   相同的操作），完成后 L0 恒空。
3. 验收口径（用户指定）：`B2 = 0`、**LSM 返回旧值的失败 = 0**、INDEX 来源剩余失败单列。

**二进制/样本**
```
zf_valcheck          sha256 31d97fea46cb99da4e59634b3b45613147680fb1bfc4a86eb93a9634d1bfc4bb
librocksdb.so.11.2.0 sha256 60b97297ac3b4becb68673146a4a8d5730b94286105e254b17bdf7cee0afabad
frozen 副本：ledger/frozen_f4/{b1,b2,m1}（b1/b2 用于排序核实，m1 做了 l0merge 实验）
db_pick：`write 40000 4` 的 ZFDBG-pick 证据来源
```

### D-6. step 1h：活跃窗口 NotFound 的定位与修复（未提交）

**用户更正（已采纳，替换此前表述）**

1. RocksDB **不是**"跨层统一比较 sequence"：它靠安装与压实维持版本顺序，点读可以首命中即返回
   ⇒ **层内不重叠并不足以保证正确性**（B 的根因正是这条被破坏）。
2. `GetCopy` 只解决 locator **字节**的生命周期，**不解决它指向的 WAL 文件**的生命周期：
   从索引选中记录到 `ReadValue` 完成，必须持有 generation 引用或有效读句柄。
3. `l0merge` 保持为**显式修复模式**，不进自动打开流程：普通 RocksDB 的 L0 重叠本就合法，
   "L0 非空且重叠"不能判定污染；本次实验通过也不足以证明对删除/快照/历史库都安全。

**单键时序（活跃窗口内当场采集，v=10003，期望 seed1）**

```
LIVE_FAIL v=10003 expect=seed1 got_len=0 status=NOTFOUND read_seq=30000
          （Write 已返回 OK；read_seq >= write_seq ✓）
LIVE_FAIL wal_rec phys_part=9 gen=0 offset=0 seq=66 type=1 class=seed1   ← 记录确实在 WAL
LIVE_FAIL fences: … p3=[1,1] …（**没有 p9** ⇒ 该代未安装）
LIVE_FAIL chain_has_key logical_part=9
            chain#0 part=9 gen=0 frozen=0 mem_bytes=41034
              seq=66 type=1 loc(part=9 gen=0 off=0)                      ← 索引登记在 part 9
读路由     : routed_logical_part=8  （链长 1，仅活跃段；不含该键）
            → index_miss → LSM: L0 file=35 miss → L1 file=18 miss → NOTFOUND
```

**根因**：**写入时的路由表版本与读时的版本不同**（表每 epoch 重建）——未安装记录的索引
登记在写入时的分区（9），而读只查当前路由分区（8）+ DRAINING/split 别名（本次 0 个别名）
⇒ 记录不可见，LSM 里也还没有 ⇒ NotFound。此前 `walread` 的 4043/19979 全属这一类。

**修复（读候选并集 + 跨候选 seq 最优 + 失败不吞）**

1. `PartitionIndexSet::ChainParts()`：列出仍有索引段的分区；`GetFromPartitionIndex` 在
   **路由分区 miss 后**把并集作为候选（上限 `zfo.read_union_max_parts`，默认 64；**0 = 关闭
   并集，回到旧行为，供对照**），并在**全部候选中按 seq 取最优**（删除标记同等参与）。
   成本：仅 miss 时扩展，计数 `rocksdb.zeroflush.index_parts_scanned` 可观测。
2. **失败不吞**：索引命中后 `ReadValue` 失败时，按"该 (phys_part, gen) 是否已被 fence 声明
   安装"分流——已安装 ⇒ 回落 LSM 正确；**未安装 ⇒ 传播 `IOError`**，绝不静默返回 LSM 的
   旧值/NotFound（后者会把生命周期错误伪装成陈旧读）。
3. **WAL 句柄钉住**：`PinWalGen()` 在索引选中后、`ReadValue` 完成前持有该代的读句柄
   （sealed 代 = SealedFileCache 登记 + LRU 句柄；活跃代 = 常驻写句柄，无需钉住）。
4. *索引撤除只在安装/释放时发生*（本轮未改，留待 B 的安装防护一起收敛）。

**验证**

| 用例 | 修复前 | 修复后 |
|---|---|---|
| `walread`(20000,4) mismatch | 4043 / 4709 / 2563（三次） | **0** |
| `LIVE_WINDOW_ASSERT` | FAIL | **PASS** |
| `WAL_HIT_ASSERT`（明确 WAL 命中） | PASS | **PASS**（active=9361 sealed=7418） |
| `walread` 来源 | INDEX 10407 / LSM 7010 / LSM_NOTFOUND 2563 | **INDEX=13070 / LSM=6910 / NOTFOUND=0** |
| `all` 冒烟 | mismatch=425 **miss=5597** | mismatch=1689 **miss=0** |
| `selftest`（快照/删除） | PASS | PASS |
| `l1hit` | wal_read_delta=0 | wal_read_delta=0 |
| 冻结样本 scan/triage（A/B 分桶） | 2566（A=1749 B=817） | **2566（A=1749 B=817）**（无回归） |

**门禁口径更新（当前主门禁）**：`walread` 的 `LIVE_WINDOW_ASSERT`（逐键零错误，含 NotFound）
与 `WAL_HIT_ASSERT`（明确 WAL 命中）**进入主门禁**；此前的 `wal_read_delta=0` 口径作废。
当前状态：**C 的版本选择已验证；B 的归并实验通过（显式模式）；活跃读可见性已修复；
A（INDEX 来源 1749 键）与 B 的系统修复仍阻断整体验收。**

复现：`ZF_VC_KEEP_WAL=1 zf_valcheck <db> walread 20000 4`（自包含：写→同进程立刻读）；
对照实验加 `--zf_read_union_max_parts=0`。

**二进制（step 1h 修复版）**
```
zf_valcheck          sha256 ba3e4a8be1eb0727b32b7858213b67bcfed707ed7f163deab6a295e7bb282493
librocksdb.so.11.2.0 sha256 bd16b085bda06ef71e24624f59344613ea0671306da09fdc636145bd9d0e8738
```

### D-7. step 1i/1j：活跃读语义补全（三处缺口 + 四个确定性用例）— 未提交

**缺口 1：候选必须**总是**参与选择，且不得静默截断**

- 选择逻辑已抽成**唯一实现** `ZeroFlushContext::SelectBestIndexRecord`
  （读路径与测试入口共用）：
  候选 = 路由分区 ∪ 别名(DRAINING/split) ∪ 其它仍有索引段的分区；
  **跨全部候选按 seq 取最优**（跨 physical part 唯一可比量），删除标记同等参与；
  剪枝只允许用**可靠上界**：按上界降序访问，某候选上界 ≤ 当前最优 ⇒ 停。
- 上界 = 该候选全部段的 `max_seq` 最大值（`ChainParts` / `PartMaxSeqUpper`）。
  **修复过程中发现并修掉一个真缺口**：别名与显式候选此前上界填 0（占位），
  会被剪枝误跳过 —— 现在每个候选都取真实上界。这个缺口正是用例 1–3 抓出来的。
- `read_union_max_parts` **不再截断**：超限扫全量并计 `read_union_overflow` 并发 WARN。

**缺口 2：不得凭 fence 降级**

- 索引命中但 `ReadValue` 失败时**一律保留原始错误**（`IOError`，含
  phys_part/gen/pinned/is_active/installed 现场），不再回落 LSM —— fence 只证明安装已提交，
  不证明本次 Get 的 SuperVersion 包含该安装，且 LSM 侧错读（B）未修。计数
  `read_readfail_propagated`（理想 0）。后续按协议"重新取得包含该安装的读视图并保持
  read_sequence"再重试，届时才可回落。

**缺口 3：pin 覆盖"选择 → 读"窗口（含活跃代）**

- `GetCopy(..., pin_fn, pin_ctx)`：pin 在**选择完成的同一次调用内、链引用释放之前**取
  （不再是"先返回 locator 再 pin"）。pin 句柄（`ZfPinSlot`）存活到 `ReadValue` 完成。
- 活跃代**无法 pin**（Freeze 轮换写句柄）：记 `is_active` 并交给缺口 2 的错误协议
  （Freeze 后该代进入 SealedFileCache 仍可读；若已回收则**明确报错**而不是静默旧值）。

**四个确定性用例**（`zf_valcheck <db> semtest <n>`，写入受控合成条目走同一选择实现）

```
case1 routed_old_vs_other_new: PASS (sel_part=1001 sel_seq=200 phys=1001/2; 期望 part=1001 seq=200)
case2 routed_old_tombstone_vs_other_new_value: PASS (sel_seq=400 type=VAL)
case3 newest_tombstone_in_other_candidate: PASS (sel_seq=600 type=DEL ⇒ 调用方 NotFound)
case4a pause_then_resume: PASS (status=OK correct=1 silent_stale=0)
case4b pause_then_unlink: PASS (status=OK correct=1 silent_stale=0)   ← pin 持 fd，unlink 后仍读到正确值
semtest cases1-4: PASS (fails=0)
```

用例入口与挂钩：`SelectBestIndexRecord(candidates=显式列表)`；`ZF_TEST_PAUSE_AFTER_SELECT=<哨兵>`
在"选定 + pin 完成、ReadValue 之前"暂停（测试专用，未设置零开销）。

**回归（修复后）**

| 用例 | 结果 |
|---|---|
| `walread(20000,4)` | mismatch=**0**，`LIVE_WINDOW_ASSERT` PASS，`WAL_HIT_ASSERT` PASS（active=8879 sealed=10607） |
| `walread` 新计数 | `readfail_propagated=0`、`union_overflow=0`、`index_parts_scanned=299700`（19979 次读 → 约 15 个额外链查询/读，即并集慢路径的实测成本） |
| `all` | miss=0 errors=0，**mismatch=2172**（仍在漂移：1689→2172；**不能用 miss=0 代替正确性通过**） |
| `selftest` | PASS |
| 冻结样本 scan/triage | 2566（A=INDEX 1749 / B=LSM 817），B2=0，ACCEPTANCE PASS（无回归） |

**测试脚手架陷阱（记录）**：`PartitionIndexSet::Insert(part, gen, ...)` 只写入**已存在**的该代段，
无段则计入 `insert_dropped` 并丢弃 —— 用例 2/3 首轮 FAIL 即由此（合成条目 gen 不存在），
不是引擎缺陷。

**V0.6 方向（用户 2026-09-15 确定）**

固定范围分区：**新库应持久化固定边界**，让写入、读取、恢复使用同一映射；全分区候选扫描
只作为**历史库的正确性慢路径**，不作目标系统默认随机读路径。注意：**冻结历史库当前路由表
也不能消除已存在的跨分区记录**（那些记录的索引已登记在写时的分区上），因此历史库仍需要慢路径，
或按 B/迁移方案处理。

**当前状态（用户口径）**：路由漏查根因已证；活跃窗口用例通过；**完整活跃读语义（四用例）已通过**；
`all` 仍有 2172 错读，**不能以 miss=0 代替正确性通过**；B 的安全安装与排空、A 仍阻断整体验收。

**二进制（step 1i/1j 版）**
```
zf_valcheck          sha256 70ddfe3a5747875e161808bff5288d0c791062f58584fafcef6ebe42f9fc07ff
librocksdb.so.11.2.0 sha256 7c753b9a951bcf125be361d6f57406935f76ebd5637ed7653067c14140de0e98
```

### D-8. step 1k/1l：活跃代读引用跨 Freeze + 刷新重试协议（未提交）

**问题（用户 2026-09-15）**：活跃代无法 pin ⇒ 遇 Freeze 只能报错；这避免了静默错读，
却没保证正常并发下的可读性。报错不能当作正常换代的完成方案。

**实现**

1. **换代原子化的活跃代读**（`PartitionedWalManager::ReadActiveRecordChecked`）：
   在 `p->mu` 下校验"`ref.gen` 仍是当前活跃代"再读（缓冲内/文件内两种情形）。
   Freeze 持同一把锁完成"刷盘 + 关旧句柄 + 换代"，因此读要么完整读到旧代（含未刷盘
   缓冲），要么得到 `gen_rotated=true` —— 此时旧代文件**已完整落盘且不可变**，可安全
   按封存文件读。这修掉了"先解析活跃代、Freeze 后按旧代号读新文件"的错读窗口。
   *（先前用"独立 fd"的做法已废弃并说明原因：活跃代数据可能还在写缓冲，单开 fd 会读到
   不完整文件——实测 `Corruption: ZF sealed record header truncated`。）*
2. **刷新读视图并重试的协议**（缺口 2 的完成方案）：索引命中而记录不可读时——
   · 该代**未安装** → 传播错误（唯一副本不可读）；
   · 该代**已安装** → 第一次请求调用方刷新 SuperVersion 后重试（`DBImpl` 循环内
     `ReturnAndCleanupSuperVersion` + `GetAndRefSuperVersion`，**read_sequence 不变**，
     最多 2 次）；刷新后仍不可读 → 回落 LSM（此时视图已包含该安装，回落正确）。
   计数：`read_readfail_propagated` / `read_readfail_fallen_back`（均已导出为属性）。
3. 封存代仍走"选择时取句柄（pin）+ 句柄直读"，覆盖"选择 → 读"窗口。

**用例 5（新增，用户指定时序）**

```
case5 active_gen_survives_freeze: PASS (status=OK correct=1 read_error=0
      gen_before=0 gen_after=1 rotated=1 readfail_fellback=0 propagated=0)
```
构造：写键（落在活跃代）→ 读线程暂停在"已选择、未读" → 主线程写入**路由到该分区**的
6000 条（超 4MB 分区目标）触发 Freeze（实测 gen 0→1 已换代）→ 等**物化安装完成**
（`pending_materialize_bytes==0`）→ unlink 该代文件（模拟回收）→ 放行 → 读返回正确值、
无错误（走封存/LRU 句柄路径）。

四用例（1–4）结果不变：case1/2/3 PASS（跨候选按 seq 取最优、删除标记参与）、
case4a/4b PASS（OK correct=1 silent_stale=0）。**当前表述应为"五个定向用例通过"**，
其中 1–3 是选择逻辑，4/5 是读引用生命周期。

**新发现（重要，如实记录）**：`walread` 的 `LIVE_WINDOW_ASSERT` **与时序相关**。
同一代码下多次运行：0（窗口内无安装完成）→ 981 → 661（窗口内安装完成较多）。
失败形态是 **A 类**：`index_insert_dropped=0`（写入没丢索引），索引链返回旧值而 LSM 已有
新值——正是 A 类在活跃窗口内的暴露。因此：
- 此前的 `LIVE_WINDOW_ASSERT PASS (mismatch=0)` 属**时序侥幸**，不能当作活跃读语义通过；
- 该门禁在 A 修好前会间歇性 FAIL，应记录为"A 类错读的单列证据"。

**当前状态**：五个定向用例通过；活跃窗口不再有 NotFound 类不可读；**A 类（索引返回旧值）
仍是活跃窗口与冻结样本的共同失败源，B 的安全安装未做**。下一轮按用户限制：
B 的安全安装（重叠则融合或保留 WAL 让位、让位不生成 L0/不推进 fence、有明确进展来源、
最终可排空）。

**二进制（step 1k/1l 版）**
```
zf_valcheck          sha256 dc9b286664e652c12200665191ddaf1e40774513dd119a3d6c231efbd0d7f95f
librocksdb.so.11.2.0 sha256 325b68be83af481717213dc18672f682f130ed439fff76b96f1ee779a9839b19
```

### D-9. V0.6 固定路由：实现清单（用户 2026-09-15 要求，不能只留在待办）

1. **持久化固定边界**：`PartitionTable` 版本化写入 MANIFEST/ZFPROPS；每库一份"固定边界表"
   （version 0 由建库参数确定，之后不变），写入/读取/恢复三处都从同一份表取路由。
2. **取消每 epoch 重建表**：`align_l1` 的"按 L1 文件边界重采样"改为只用于**初始建表**；
   运行期不再改表（split/merge 走显式、带版本的边界变更，并记录在 MANIFEST）。
3. **读路径收敛**：固定边界 ⇒ 路由分区唯一 ⇒ **删除全候选并集慢路径**（仅保留 DRAINING/
   split 别名），随机读回到单分区链查询（消除当前 ~15 次额外链查询/读的成本）。
4. **历史库兼容**：并集慢路径保留为**显式开关**（`read_union_max_parts>0`），只对
   "跨分区记录已存在"的旧库开启；旧库迁移/归并（`l0merge` 同类操作）需在副本上按删除、
   快照、跨分区记录三项分别校验后再决定。
5. **验收**：新库上 `index_parts_scanned` 增量 ≈ 0（无并集），`read_union_overflow=0`，
   现有门禁（含 walread/scan/triage）不回归。

### D-10. step 1m：A 类的稳定复现用例（用户 2026-09-16 第 1 项）— 未提交

**用例形态**（`zf_valcheck <db> semtest <n>`，需 `ZF_VC_PARTITION_TARGET_MB=1`）
```
case6 A_CLASS_REPRO: BUG_PRESENT (index_seq=1 lsm_seq=20985 read_seq=20985 ins_ok=1
      返回来源=INDEX status=OK correct=0 got_len=1024 readfail_fellback=0)
```
三次独立运行**逐字一致**（run1/2/3 输出相同）⇒ 稳定可复现。

**构造**（把 A 类从"统计现象"变成"确定性用例"）

1. 写 K（新值）→ 大量写入触发封存 → `Flush()` + 等 `pending_materialize_bytes==0`
   ⇒ K 的新值进 LSM，其索引段被释放（索引里不再有 K 的版本）；
2. 写 K'（旧值）后只调 `wal()->Sync(part)`——**刷写缓冲但不换代、不安装**
   ⇒ 该记录**可读**且所在代**未安装**（这是复现的必要条件：先前用"完整封存"
   的版本读不到它，被"已安装 → 刷新视图 → 回落 LSM"协议拦成了良性路径）；
3. 扫 WAL 取 K' 的真实 locator；
4. 向索引注入 K 的**旧 seq（=1）**条目，locator 指向 K' 的记录；
5. 读 K。

**归因（三项证据同时给出，可判定错误发生在哪一环）**

| 环节 | 证据 | 结论 |
|---|---|---|
| 索引选择 | `返回来源=INDEX`、`index_seq=1 < lsm_seq=20985`、`correct=0` | **错在这里**：索引命中即返回，缺"与已安装层比较 seq" |
| 索引安装清理 | `ins_ok=1`（条目在索引里且未被清理） | 不是清理问题 |
| LSM 回落 | `readfail_fellback=0`（未走回落路径，读取成功） | 不是回落问题 |

**证据字段齐全**：`index_seq` / LSM 候选 seq（= 读序列，用例中同源）/ `read_sequence`
/ SuperVersion 刷新计数（`readfail_fellback`）/ 最终返回来源与状态。
该用例在 A 修复前应恒为 `BUG_PRESENT`，修复后应转为 `FIXED`——作为 A 的回归门。

**当前状态（用户口径）**

```
case1–5（选择逻辑 3 项 + 读引用生命周期 2 项）：PASS
case6（A 类）：BUG_PRESENT（稳定复现，作为 A 的失败用例）
walread：LIVE_WINDOW_ASSERT 间歇 FAIL（661–981，A 类；不再有 NotFound 类）
B2=0；冻结样本 2566（A=INDEX 1749 / B=LSM 817）
B 的安全安装：未实现（生产路径仍可能生成错误 L0）
```

**下一轮**（按用户顺序，本轮未做）：① `case5` 循环数百次的并发压力（覆盖读期间
Freeze / Freeze 后安装 / 安装后回收 / SuperVersion 刷新 / 删除标记与快照，任何
Corruption、静默旧值、NotFound 都算失败）；② B 的安全安装（不生成 L0、不写 AddFile、
不推进 fence/drained、保留 WAL pending、稳定重叠安排融合、运行中冲突等待后重规划，
并以累计计数证明 `install_fallback_l0=0`）。

**二进制（step 1m 版）**
```
c8c38f5e0a0f2b714b69a864a5bc7a96f72fd777056d393bdf3caa7f2c307790  source/rocksdb-zeroflush/zf_valcheck
325b68be83af481717213dc18672f682f130ed439fff76b96f1ee779a9839b19  source/rocksdb-zeroflush/librocksdb.so.11.2.0
```

### D-11. step 1n：case5 扩展为可重复压力测试（用户 2026-09-16 第 2 项）— 未提交

**实现**（`zf_valcheck <db> stress <group> <iterations>`，需 `ZF_VC_PARTITION_TARGET_KB`）

- 每轮 **独立新库**（`<db>/it_<group>_<i>`，建库即用即弃）⇒ 轮间无状态泄漏；
- 固定随机种子 `20260916`；**30s watchdog**（`wd_stage`/`wd_iter` 无进展即打印现场并
  `_Exit(3)`，避免死锁静默无输出）；
- 每轮严格执行：写活跃代记录 → 选择后暂停（`ZF_TEST_PAUSE_AFTER_SELECT`）→ 触发 Freeze
  且**等到该分区真的换代**（写路由≠读路由，固定条数不保证命中该分区；有界重试）→
  等安装完成（组 1/2/3；组 4/5 用 64MB epoch 目标保持"已封存未安装"）→（组 2、组 3 删除变体）
  unlink 回收 → 放行读；
- 逐轮输出：`iteration / variant / gen_before / gen_after / rotated / rseq / snap /
  src / status / got_len / exp / pin_ok / pin_active / propagated / fellback / unlink`。

**五组结果**（严格判定：`status!=OK`（删除变体期望 NotFound 除外）、静默旧值、NotFound、
Corruption/IOError、`readfail_propagated>0` 任一即失败）

| 组 | 覆盖 | 轮数 | 结果 |
|---|---|---:|---|
| 1 | 基础跨 Freeze（active→sealed 读转换） | 500 | `failures=0 silent_stale=0 notfound=0 corruption=0 rotations=500` **PASS**（src=INDEX 500） |
| 2 | 安装后回收（pin 与 unlink 生命周期） | 500 | 同上 **PASS**（src=LSM 499 / INDEX 1） |
| 3 | snapshot/delete 混合（值/删除/快照三变体） | 500 | 同上 **PASS** |
| 4 | 选择时活跃 + 暂停期间换代 + 不安装（读转换） | 500 | 同上 **PASS**（INDEX 490 / LSM 10） |
| 5 | **选择时已在 sealed 代**（pin 路径） | 300 | 同上 **PASS**；`pin_ok=218`（218 轮由 sealed 代句柄直接服务）、`pin_active=0` |

⇒ 判定条件（`failures=0 silent_stale=0 notfound=0 corruption=0 rotations>0`）四组 500 轮 +
一组 300 轮全部满足；**"活跃代读引用跨 Freeze"标记为压力验证通过**（含 sealed 代 locator、
删除标记、snapshot、读期间多次 Freeze、安装后回收）。

**过程中的两个真实发现（保留记录）**

1. 首版组 4 在**未安装**的 sealed 代上 unlink ⇒ 引擎返回
   `IO error: ZF: index hit but WAL record unreadable (installed=0)`、`propagated=1` ——
   即"删掉唯一副本"时**明确报错而非静默旧值**，是缺口 2 协议按预期工作的证据；
   但该构造不该出现在本组（回收只应在安装后），已改为组 2 负责 unlink。
2. 组 4 首轮 `rotations=0`（固定条数未命中该分区）⇒ 改为"写到达成换代"的有界循环，
   此后五组 `rotations` 均等于轮数。

**未覆盖/边界**：`case6`（A 类的确定性单元门禁）**不计入**本压力统计，保持独立；
压力测试均在同进程内完成（无重启），重启路径仍由 `scan/triage` 覆盖。

**二进制（step 1n 压力版）**
```
1b5ba6d0849040c16f33728431bf2096c3244a07a4e5167c605ace5219958997  source/rocksdb-zeroflush/zf_valcheck
8504ff6262fc5126983825cb7f0e670f876c3314d48ec27e7019cb566e6b1cfe  source/rocksdb-zeroflush/librocksdb.so.11.2.0
```

### D-12. step 1o：B——关闭目标模式的 L0 出口（用户 2026-09-16 第 1/2 项）— 未提交

**实现**

1. **L0 出口全部关闭**（`ZeroFlushOptions::forbid_l0_install`，**默认 true**）：
   - `PickInstallLevel` 的两处回落（运行中 compaction 占用 / L0..base 重叠）在目标模式
     下返回哨兵 `ZfMaterializeJob::kCannotInstall`（= 不可安全安装），不再返回 0；
   - **安装点兜底**（覆盖正常路径、fallback 与重试路径）：本批只要有输出 `level<=0` 或
     被标记不可安装 → **整批让位**——`DeleteAllOutputFiles` + `HandOffAllGens` +
     空 VersionEdit：**不写 AddFile、不推进 fence、不增加 drained**，WAL 保留；
   - 累计计数：`l0_install_count`（任何 L0 安装）/ `l0_install_blocked`（被拦下的批次），
     与既有 `install_fallback_l0` 一并导出为属性。
2. **稳定重叠 → 融合；临时占用 → 等待**：
   - `PlanLocked` 中"L0 融合 ratio 门槛不达标 ⇒ 回落 L0"改为：目标模式下**直接融合**
     （重叠不会因等待而消失）；
   - "批内有未安装输出"⇒ 目标模式下**一律等待**下一批（临时占用，前序安装后即可融合），
     不再回落 L0；
   - base 层 `being_compacted`/越界 ⇒ 既有的 kSkip 让位（等占用任务完成后重规划）不变。

**新增目标库验收（首次出现 scan/triage 双零）**

```
$ zf_valcheck db_tm3 write 20000 4
write-only l0_install=0 l0_blocked=0 fallback_l0=0 forbid_l0=1
$ zf_valcheck db_tm3 scan 20000 4
scan: mismatch=0 (lost_overwrite=0 unexpected_overwrite=0 other=0) / total=20000
$ zf_valcheck db_tm3 triage 20000 4
triage: fail=0 B1=0 B2=0 B3=0   |   SOURCE_B3: (空)
```
⇒ 目标模式（禁止 L0）下**新库读写全对**，且 `l0_install_count=0`；`all` 模式排空到
`pending=0.0MB gens=0`。对照：**冻结历史样本不变**（scan 2566 / triage 2566，A=1749
B=817，B2=0）——L0 关闭不回溯修复既有污染，符合"历史库用显式模式"的定位。

**新发现（目标模式引入，B 范围内必须修）**：压力组在目标模式下出现新的失败形态
（非 A 类）：

```
src=INDEX_READ_FAILED status=IO error: ZF: index hit but WAL record unreadable
   (phys_part=12 gen=0 pinned=0 active=1 installed=0):
   NotFound: ZF sealed gen not in any active epoch
```
即：选择时记录在**活跃代**（`active=1`）、未安装（`installed=0`）；暂停期间 Freeze 换代后，
该世代**既不在 SealedFileCache 的 epoch 登记里、WAL 文件也已不存在**（独立 fd 兜底也失败）。
出现率：组 1 前 8 轮 6 次、组 5 200 轮 3 次（`notfound`），组 2/3 同形态。
含义：目标模式下让位/融合变多后，**"未安装世代"的保留契约被打破**（应保留 WAL + 登记，
直到该代真的安装）。这正是用户第 3 项要解决的"消费证明/保留与安装结果绑定"，
已定为下一步首要修复项（压力组在此之前保持 FAIL，不掩饰）。

**未做**：① 强制一次安装冲突的确定性用例（需要注入钩子）——先用自然重叠观察
（`l0_blocked=0`，本次运行的规划阶段已把重叠走成融合，兜底未触发）；② 让位批次
AddFile=0/fence/drained 不变的逐项断言；③ 第 3 项"同一 VersionEdit 绑定消费证明"的
实现（当前 fence 仍与 AddFile 同 edit 原子提交，但"输出被撤销/部分废弃"的判定待补）。

**二进制（step 1o / B 首片）**
```
3344bae3fbf8fcef2ae07a38833254ac364f11cf30f9e298e402a3d87be87f2a  source/rocksdb-zeroflush/zf_valcheck
606cbc018b52affe718aa7fd2b4719f9403f82c62edfc4982eea71ebd47e6be1  source/rocksdb-zeroflush/librocksdb.so.11.2.0
```

### D-13. step 1p：未安装世代的保留契约（用户 2026-09-16 第 1/2/3/4 项）— 未提交

**根因（两条，均在"换代 + 让位"路径）**

1. `ReadValue` 的**封存分支**只查 SealedFileCache：换代后（原本记录在活跃代）
   该代若不在任何 epoch 登记，直接返回 `NotFound: ZF sealed gen not in any active epoch`
   ——而文件其实还在磁盘上（已封存文件不可变）。
2. **让位只移交了本 job 的 `se_.gens`**：目标模式下让位变多，"封存后尚未登记进任何
   epoch"的代不在该列表里 ⇒ 无人接管 ⇒ 落在两个集合之外。

**修复**

- 封存分支加 **独立 fd 兜底**：登记缺失但文件在 → 用 `OpenGenForRead` + `ReadFromSealed`
  读（已封存文件不可变，安全）；两者都不在才报错。
- `HandOffAllGens` 改为移交**所有未安装世代**：候选 = `se_.gens` ∪ 磁盘上全部
  `gen > fence_last(part)`（或无 fence）的代。
- 计数（均已导出属性）：
  `gen_lost_before_install`（无登记且无文件，**应恒为 0**）、
  `gen_unregistered_fd_fallback`（无登记但文件在，fd 兜底可读）、
  `gen_handoff`（补充移交的未登记世代数）；
  连同既有 `gen_lost` 打印点，读失败时记录 part/gen/登记/文件三态。
- 压力测试修正：**只有 fence 覆盖该代（installed）才允许 unlink**。注意
  `pending==0` **不能**当替代判据——让位/移交会把代移出 pending 记账（recovery 集合
  不计入），pending=0 不代表数据已在 LSM（首版据此 unlink，实测 group2 6/8 失败）。

**压力结果（目标模式，200–300 轮/组）**

| 组 | 结果 |
|---|---|
| 1 基础跨 Freeze | 200/200 **PASS**（修复前 6/8 失败） |
| 2 安装后回收 | 300/300 **PASS**（修正 unlink 判据后） |
| 3 snapshot/delete | 200/200 **PASS** |
| 4 选择时活跃+换代+不安装 | 200/200 **PASS** |
| 5 选择时已在 sealed 代 | 298/300，**notfound=2**（0.7%）← 唯一未过项 |

**目标模式新库验收（本次复测）**

```
write-only gen_lost_before_install=0 unregistered_fd_fallback=0 gen_handoff=0
write-only l0_install=0 l0_blocked=0 fallback_l0=0 forbid_l0=1
scan   : mismatch=0 / 20000
triage : fail=0 B1=0 B2=0 B3=0
l1hit  : checked=20064 mismatch=0 miss=0 errors=0（读全对）
```
冻结样本回归：`scan mismatch=2566`（与 D-12 一致，未受影响）。

**未过项（下一步，已取到失败现场）**：组 5 `notfound=4/300`，逐轮记录显示

```
iter=31/81: gen_before=1 gen_after=2 rotated=1 rseq=2001
            src=LSM_NOTFOUND status=NOTFOUND
            pin_ok=0 pin_active=0 propagated=0 fellback=0 unlink=0
```
`pin_active=0` 且 `pin_ok=0` ⇒ **索引侧一个候选都没命中**（pin 回调根本没触发），随后
LSM 也 NotFound。结合 `gen_lost_before_install=0`（文件仍在磁盘）可断定：
**该代的索引段已被撤除（或在候选并集之外），而数据尚未安装** ⇒ 索引未命中时没有任何
机制去查"未安装世代的 WAL 文件"。
修复方向（下一步）：索引未命中且 LSM 也未命中时，增加**未安装世代 WAL 的兜底查询**
（recovery/skip 集合 + 磁盘上 `gen > fence_last` 的文件），即"已确认写入必须保持可读"
的直接实现——与已加的"索引命中但无登记 → fd 兜底"互补。

**未做**：强制冲突确定性用例（注入钩子）、让位批次 AddFile=0/fence/drained 不变的逐项
断言、第 3 项"消费证明与安装结果原子绑定"的显式实现（当前 fence 仍与 AddFile 同 edit，
但"输出被撤销/部分废弃"的判定未补）。

**二进制（step 1p 保留契约版）**
```
3b5e00d7ce0ec83f1d36a5821a73da201cedd36f20b76acf003adf56be708fb1  source/rocksdb-zeroflush/zf_valcheck
88c321a43cc24f8ce3ad1f6b3b263c317c4e19cd0891d84fa1a277bd19c78858  source/rocksdb-zeroflush/librocksdb.so.11.2.0
```

### D-14. step 1q/1r/1s：pending 守恒、handoff 收紧、失败键完整状态（用户 2026-09-16 第 1/2/3 项）— 未提交

**先纠正我此前的一处错误结论**：manifest §D-13 写的"让位/移交会把代移出 pending 记账
（recovery 集合不计入）"**不成立**。代码里 pending 是 registry 驱动的：唯一减项在
`MarkEpochInstalled`（成功安装），handoff/收养/让位只改归属。已加**权威重算**做守恒校验：
```
PendingBytesRecomputed() = Σ_{registry 中 sealed && !installed} bytes
pending=15121316  pending_recomputed=15121316  drift_events=0   ← 目标模式实测守恒
```
即"pending=0 才是排空完成"这句话在代码层是成立的；此前我用 `pending==0` 当"已安装"的
替代判据是**测试侧**的错误（现已改成只看 fence）。

**第 2 项：handoff 收紧（已回退磁盘全局收养）**
- 回退了上一轮加的"让位时枚举磁盘上全部 `gen > fence` 并收养"——那会接管其他在飞任务的
  代、甚至当前活跃代；文件扫描不能替代所有权记录。
- `HandOffAllGens` 现在只移交**本任务拥有**的代（`se_.gens` ∪ `skipped_gens_`）。
- 真正的孤儿接管只保留在启动恢复路径；另有新增的 **epoch 释放守门**（step 1s）：
  `ReleaseEpoch` 若发现该 epoch 尚有**未安装**的代 → 先移交 recovery（保留归属与 frozen
  索引段、WAL 不动）再释放，并计 `handoff_at_release`。
- **Freeze 即登记**（4 个 `wal_->Freeze` 调用点全部补上 `RegisterGen`）：封存瞬间进入唯一
  registry（sealed && !installed ⇒ 计入 pending），此后 epoch 收养只是归属迁移。

**第 3 项：失败键的完整状态（同一时点采集，压力失败当场打印）**
`DiagKeyState` 输出：写序列/读序列、fences、`index_insert_dropped`、全链摘要
（每个 part 的 upper）、读路由分区、WAL 里该键的每一条记录（part/gen/off/seq/class/
decodable）、以及该代 registry 状态（`DescribeGen`：epoch/skip/recovery 归属 + installed
+ 引用 + 文件存在）。三例失败现场一致：

```
DIAG key_v=1031007 read_seq=2401
DIAG fences=p0=[3,3] p9=[1,1] p10=[1,1] p12=[1,1]        ← 无该记录所在 part 的 fence
DIAG index_insert_dropped=0                              ← 无"插入被丢"记录
DIAG chains=16: p0(upper=2401) p1(upper=1648) … p13(upper=0) …   ← 该 part 无活链
DIAG routed_part=0                                       ← 读路由到 p0
DIAG wal_rec phys_part=13 gen=0 off=0 seq=1 seed1 decodable=1     ← 记录在 p13，可解码
DIAG gen_state phys_part=13 gen=0 registry=absent; owner={NONE}; file_exists=1
```
**按证据归类**（用户表格）：属于"**条目仍在/从未进入候选**"一类 —— 写时路由把记录落到
p13，读时路由为 p0；p13 **没有活索引链**（upper=0）、registry 无该代、`insert_dropped=0`
（没有记录到"插入被丢"）。也就是说：**写时路由分区的索引/登记生命周期没有被保留**，
读的候选发现（step 1h 的并集）只能看到"仍有链"的分区，看不到已经无链的分区。
⇒ 修复方向：让"写时路由分区"的索引条目/登记在换代与释放过程中**不被丢弃**（或让候选
发现能覆盖写时分区），而不是加全 WAL 扫描兜底（**本轮按要求未加**）。

**当前状态（本轮交付）**
```
pending 守恒：drift_events=0（守恒校验已进属性，可随时复核）
目标模式新库：l0_install=0 / gen_lost_before_install=0 / scan=0 / triage=0(B1=B2=B3=0)
压力：组 1/2/3/4 各 200–300 轮 PASS；组 5 396–400 轮中 3–6 次 NotFound（上述形态）
未做：确定性回归（该失败是路由表切换 × 换代窗口的时序竞态，先把候选发现/登记护住再固化用例）；
      强制冲突用例；第 3 项"消费证明与安装结果原子绑定"
```

**二进制（step 1q-1s 版）**
```
36c47545ceb7c4e1a900aef4ce86aa95580c614936a4a5bb08d69e9bd85667f9  source/rocksdb-zeroflush/zf_valcheck
d28e9d5a8a287c949387be79ab6354b6cce046f7f650d82a37c6cb9f26db006b  source/rocksdb-zeroflush/librocksdb.so.11.2.0
```

### D-15. step 1s/1t：physical(13,gen0) 事件顺序 + 修复（用户 2026-09-16 指定追踪）— 未提交

**两处判读纠正（已采纳）**
1. `pending == pending_recomputed` 只证明 registry **内部**账目一致；**从未登记或被错误移除
   的代会同时漏出两边**。因此 `pending=0` 不能单独证明排空完成（active 本来也不计入）。
2. 先前"D-14：条目仍在、只是没进候选"的归类**不成立**：现场是 p13 既无可读索引、
   registry 也无该代。`index_insert_dropped=0` 只能排除那条计数路径。

**physical(13, gen0) 的完整事件顺序（失败当场采集）**
```
DIAG gen_state phys_part=13 gen=0 registry=absent; owner={NONE}; file_exists=1
     active_gen=2  sealed_meta=yes bytes=130296 records=122 records_known=1
DIAG wal_rec  phys_part=13 gen=0 off=0 seq=1 seed1 decodable=1
DIAG chains=16: p0(upper=2401) … p13(upper=0) …      ← 该分区无活链
DIAG routed_part=0                                    ← 读路由在别的分区
```
结论：**它确实真正封存过**（`sealed_meta` 在 Freeze 时刻记录，122 条；`active_gen=2` 已换代），
且**从未安装**（无该 part 的 fence）；随后两个状态转换把它抹掉：
- `SealedFileCache::ReleaseGens`：引用归零即抹除 registry 条目 —— **不检查 `installed`**；
- `ZeroFlushContext::ReleaseFrozenIndexes`：无条件撤除该代的 frozen 索引段 ——**读依赖随之消失**。
两者都在 **epoch 释放**发生，而让位/未安装的 epoch 同样会走到释放。

**修复**
1. `ReleaseGens`（step 1t）：`!installed` 时**不 unlink、不抹除 registry**，把归属移交
   recovery（保留可读性与候选发现），计 `gen_release_before_install`。
2. `ReleaseFrozenIndexes`（step 1t）：`!installed` 的代**保留索引段**（读依赖的唯一载体），
   计 `index_kept_uninstalled`。
3. `ReleaseEpoch`（step 1s）：释放前把未安装代移交 recovery，计 `handoff_at_release`。
4. Freeze 即登记（4 处）+ 回退"磁盘全局收养"（只移交本任务拥有的代）——见 D-14。

**验证（目标路径确实被走到，而非"未触发所以没失败"）**
```
stress gate: index_kept_uninstalled=11504  gen_release_before_install=0
             handoff_at_release=3314       gen_lost_before_install=0
stress group=5 rounds=400 failures=0 silent_stale=0 notfound=0 corruption=0 -> PASS
stress group=1/2/3/4 rounds=300各 failures=0 … rotations=300 -> PASS
```

**目标模式验收（本轮）**
```
write-only pending=13898412 pending_recomputed=13898412 drift_events=0
write-only gen_lost_before_install=0 unregistered_fd_fallback=0
write-only l0_install=0 l0_blocked=0 fallback_l0=0 forbid_l0=1
scan  : mismatch=0 / 20000          triage: fail=0 (B1=B2=B3=0)
all   : checked=30064 mismatch=0 miss=0 errors=0
        pending=0.0MB drained=17.4MB reclaimed=17.4MB gens=0      ← 排空归零
冻结样本：scan mismatch=2566（历史库仍走显式模式，未受影响）
```

**仍未完成**：① 最小确定性单元用例（当前用"守门计数 11504 + 400 轮严格通过"承担该职责，
比手工构造更强，但缺少单键最小复现）；② B 第 3 项消费证明与安装结果原子绑定；
③ 强制冲突用例与"让位批次 AddFile=0/fence/drained 不变"断言；④ A（case6 为回归门）。

**二进制（step 1t 索引/登记保留版）**
```
e9f0ddfe9909707a461a049bdeacca9a503e733b518036c9a898e215c298fc50  source/rocksdb-zeroflush/zf_valcheck
8743c1832891140da87a1545e1cce544a10208408ffa503220c2da7693b4cf3b  source/rocksdb-zeroflush/librocksdb.so.11.2.0
```

### D-16. step 1u/1v：消费证明绑定安装结果 + 强制冲突验收（用户 2026-09-16 第 2/3 步）— 见提交

**第 2 步：消费证明 ↔ 安装结果绑定**

- fence 候选新增判据（`flush_job.cc`，与既有规则叠加）：
  `输入完整消费`（step 1c-2）+ `同一 VersionEdit`（必要条件）+ **本次 attempt 的该
  physical_part 必须有已提交输出**（本批最终 batch 中非 superseded、且不是
  `kCannotInstall`）。被撤销/废弃/不可安装/让位的输出**不能为证明背书**
  → 计 `fence_no_committed_output`，保守不推进。
- `drained` 只在 `MarkEpochInstalled`（LogAndApply 成功点）增加 ✓；让位出口在其之前
  返回，不触碰 drained/fence ✓（现由逐批断言验证，见下）。
- `ReleaseFrozenIndexes` 复核：判据用的是记录的真实 `(physical_part, gen)`（遍历
  `se.gen.sgens` 的 pair），**逐代**判断 `IsGenInstalled(part, gen)`；混合来源的段
  不会因其中一个代已安装而整段撤除（未安装的代其段保留）。

**第 3 步：强制冲突验收（`zf_valcheck <db> baccept <n> [threads]`）**

测试钩子 `ZF_TEST_FORCE_INSTALL_CONFLICT=<n>`（负值 = 测试中途重新装载计数）在目标模式
安装点把整批判为不可安全安装 → 走空 edit 让位路径。**断言放在引擎侧逐批执行**（聚合窗口
会被"未被注入的后台批次"干扰，不能用作判据）：

```
baccept phase1 baseline: pending=0 drained=11697804 fences=p2=[0,0]b1081884r1013 sst=32
baccept phase2 forced-conflict: batches=5 drained_flat=5 pending_flat=5
                                outputs_discarded=50 (fences_unchanged=1) -> PASS
baccept phase3 install-after-release: pending=0 drained=41948904 (>baseline=1) -> PASS
baccept phase4 reclaim: registered_gens=0 reclaimed=41948904 -> PASS
baccept l0_install=0 fence_no_committed_output=3 gen_lost_before_install=0
exit=0
```
- phase2 = 5 个让位批次，**每批**都满足 `drained 不变 / pending 不减 / 丢弃了产出`
  （50 个输出未提交 ⇒ 确有产出却未 AddFile；空 edit 由该出口结构保证）；
- phase3 = 解除冲突后有限时间内安装完成、`pending=0`、`drained` 超过基线；
- phase4 = **保护不是永久保留**：安装后 `registered_gens=0`、`reclaimed=41.9MB`
  （索引段与 generation 最终释放并回收）。

**第 4 步：门禁回归（本轮）**
```
目标模式新库：scan=0 / triage=0(B1=B2=B3=0) / l0_install=0 / drift=0 /
              gen_lost_before_install=0 / pending 守恒（15041128 == recomputed）
压力：组 1/2/3/4 各 300 轮、组 5 400 轮 → failures/silent_stale/notfound/corruption 全 0
      守门计数 index_kept_uninstalled=13629 handoff_at_release=3715 gen_lost=0
```

**仍未完成**：最小确定性单元用例（当前由引擎侧逐批断言 + 400 轮压力承担）；
A 类（`case6` 为人工注入的防御性红灯；自然路径是否产生 A 尚无结论——不因此给每次 Get
加 LSM 查询）；V0.6 固定路由落地。

### D-17. step 1w/1x：安装集合判据收紧（尝试未通过，已回退为补丁）— 见 WIP_install_set.patch

**用户指出的实质缺口（成立）**：此前"同一 physical_part 有已提交输出"不足以证明该代
已安装——同分区可含多个 generation，部分输出撤销时"分区有输出"仍成立。更严重的是
**登记点本身的位置错了**：`MarkEpochInstalled/MarkGensInstalled` 原本在
`ZfMaterializeAllEpochs` **构建 edit_ 时**调用，而让位/空 edit/LogAndApply 失败都在其后
⇒ 出现"fence 保守不推进、账目与回收资格却已认定安装完成"（诊断实证：目标模式下
5 个批次 `outs=0/16`（全部 level=-1 不可安装）仍走到了登记点）。

**本轮实现（已写、已构建、未通过验收）**
1. 输出携带 `covered_gens`（该输出所属 job 消费的 gens，去掉 kSkip 部分）与 `src_epoch`；
2. 登记点从"构建 edit 时"**移到 LogAndApply 成功之后**（Run 的 success 路径），消费
   `zf_planned_installs_`（每 epoch：covered gens + 输出提交比）；要求
   **该 job 的全部输出都已提交**才 `MarkGensInstalled`，否则计 `install_gens_withheld`
   并保持 pending（`SealedFileCache::MarkGensInstalled` 为唯一权威入口）；
3. 新增负例钩子 `ZF_TEST_DROP_OUTPUT`（撤销一个输出且无替代，删除其物理文件）+
   baccept phase5 断言"未覆盖的代仍 pending、仍可读、不可回收"；
4. 新增"让位必须带来进展"：安装点让位的分区登记为 `force_merge_parts_`，下一轮规划对其
   强制 `kMergeBase`（否则 kDirect→让位 空转）。

**验收结果（红）**
```
phase1 baseline  PASS
phase2 forced-conflict PASS (batches=5 drained_flat=5 pending_flat=5 outputs_discarded=48)
phase3 install-after-release FAIL  (pending=38.4MB 不降、drained 停在 4.3MB)
phase4 reclaim FAIL (registered_gens=19)
phase5 partial-revocation FAIL (readable=0)
phase5b FAIL
```
诊断：整个验收里只有 **1 个批次**走通安装（`ZFDBG-installset2 epoch=.. outs=16/16
covered=4`），其余批次全部在安装点不可安装（`level=-1`）而让位；加了强制融合后仍未
改变 ⇒ 冲突的进展路径（kMergeBase 的实际生效条件）比"一个 flag"更深，需要按
"安装点冲突 → 重新规划并真的融合成功"端到端打通。

**处置**：工作树已**回退到上一个已验证提交**（`dfa8644`，其源码状态 = `6fbc63d` 的
已验证版本），本次尝试完整保存为
`output/zeroflush_m5_perf/WIP_install_set.patch`（15336 行 diff，可直接续做），
避免留下"排空停滞"的破损状态。回退后重建并复验：目标模式新库仍然
`l0_install=0 / drift=0 / gen_lost_before_install=0 / scan=0 / triage=0(B1=B2=B3=0)`。

**下一步（按用户顺序的第 1 项收尾）**：在补丁基础上把"安装点冲突 → 强制融合"打通
（确认 kMergeBase 在目标模式下的生效条件：overlap 扫描/Compaction 注册/输出定层），
使 phase3/4/5b 转绿；再进入合法路径 A 的结论与固定路由落地。

### D-18. step 1y（用户 2026-09-16 四步的第 1 步）：提交成功后才认定安装 — 见提交

**只提取"安装时点"这一项**（未整包恢复 D-17 的 15336 行补丁；补丁里 11845 行是 Makefile）。
改动面：`sealed_file_cache.{h,cc}`（唯一销账入口 `MarkGensInstalled`）、
`zeroflush_db.{h,cc}`（转发 + `install_gens_withheld` 计数）、
`materialize_job.{h,cc}`（输出携带**安装候选集合** `covered_gens`/`src_epoch`）、
`db/flush_job.{h,cc}`（计划集合 + 发布校验）。

**关键点（针对你的三点要求）**

1. **候选集合**：每个输出携带"本次 attempt 该 job 消费的 gens"（= `se_.gens` 去掉 kSkip），
   在搬进 batch 前填好，安装点据此构造销账集合。
2. **最终 VersionEdit 与候选的对应**：装配 `edit_` 后记录 `ZfPlannedInstall`
   （epoch / 候选 gens / 输出文件号+层）。**让位、空 edit、构建失败都在此之前 return
   ⇒ 集合为空 ⇒ 不可能提前销账**。
3. **"实际安装成功"不凭返回值判断**：空 edit 也会让 `Run()` 返回 OK。销账前逐文件确认
   **该输出的文件号出现在当前 Version**（`cfd_->current()->storage_info()` 各层查找），
   全部出现才 `MarkGensInstalled`（幂等，pending→drained）；任一缺失 → 不销账并计
   `install_gens_withheld`（相关代保持 pending、可读、不可回收）。

**验证（封存确实发生的负载）**
```
ZFDBG-publish epoch=1 committed=16 outs=16 published=1 first_file=9 lvl=1
write-only pending=13496844 pending_recomputed=13496844 drift_events=0
write-only gen_lost_before_install=0 / l0_install=0 / forbid_l0=1
drained=4.2MB（该批次销账）  pending=12.9MB（其余保持 pending，不再"提前归零"）
scan=0 / triage=0(B1=B2=B3=0)
```
即：**排空因此停住**（进展问题，符合预期），而不是靠提前标记 installed 让它归零。

**尚未完成（用户四步的第 2/3/4 步）**
- 第 2 步：撤销输出时**整个安装单元**撤销（不提交输入删除/输出新增/fence）+ 负例断言
  "撤销 → 空安装 → 释放 epoch ⇒ 数据仍可读、pending 不减、fence/drained 不推进"。
  当前的 `ZF_TEST_DROP_OUTPUT` 钩子只丢一个输出（**部分提交**）✗ 需改为整单元撤销。
- 第 3 步：只追踪一个反复 `level=-1` 的 job（attempt→规划原因→融合输入→输出范围→
  edit 的 DeleteFile/AddFile→拒绝的文件与原因），首要验证假设"**本次已融合、计划删除的
  输入文件被当成输出安装的重叠冲突**"。当前 `force_merge` 开关已暂停（本提交未含）。
- 第 4 步：单 job 跑通后再扩大验收（含安装失败注入 + 完整 baccept + 压力组）。
- 另记一条观察：`all` 模式在本提交下出现 `mismatch=1953 miss=1518`（数据未落地时经索引/WAL
  读取的路径），需在 A/排空相关工作中一并定位——不作为已完成项。

### D-19. step 1z（用户四步之第 1/2 步）：销账判据修正、整单元撤销钩子、`all` 缺失定位 — 见提交

**1. 销账判据修正（采纳你的意见）**
"输出仍在当前 Version"不作为判据——输出随后可能被压实替换，会误判未安装并造成 pending 永久滞留。
改为：**判据 = 本次 edit 已成功提交发布**（`outs_committed > 0` ⇒ edit 含 AddFile；
`TryInstallMemtableFlushResults` 返回 OK ⇒ 已 LogAndApply），在**同一把 DB mutex 之下、
紧随发布**执行幂等 `MarkGensInstalled`。文件成员检查**降级为当场断言**
（新增 `install_assert_file_missing`，理论恒 0）。

**2. 整单元撤销（首版保守做法）**
`ZF_TEST_DROP_OUTPUT=<n>`（负值=中途重新装载）：第 n 个待安装批次**整体撤销**——
删除全部产出物理文件，走既有让位出口 ⇒ **不提交输入删除、不提交 AddFile、不写 fence**，
`ZfPlannedInstall` 清空（不留待销账），WAL/registry/索引全部保持可读。
位置在**装配 edit_ 与写 fence 之前**（本函数上方即 return 出口），避免"内存不销账、
fence 已进 MANIFEST"。

**3. `all` 模式缺失的定位结论（真实读错误，不是排空停滞）**
期望存在却失败的首批三例状态一致：
```
ZFDBG-missdiag v=5001 read_seq=30000 expect=seed2
  status=IO error: ZF: index hit but WAL record unreadable
        (phys_part=5 gen=2 pinned=0 active=1 installed=0):
        Corruption: ZF sealed record header truncated
ZFDBG-missdiag v=794/804 ... (phys_part=4 gen=0 pinned=1 active=0 installed=0):
        Corruption: ZF sealed record truncated
```
且 `DiagKeyState` 的 WAL 扫描**在任何磁盘文件里都找不到该键的记录** ⇒ 该记录只存在于
"已换代/已封存"的代的**未刷盘尾部**（或其文件被读成不完整）。这是读依赖指向
"数据不在磁盘"的代的缺陷，与"排空停滞"是两码事（`scan/triage=0` 的另一组结果不能抵消它）。

**判定口径（用户第 2 步要求）**：本例属于"**未安装数据的读依赖被撤掉/指向不可读尾部**"，
不是"已安装数据不在当前读视图"（fence 无该分区条目、installed=0）。

**当前状态**：提前销账路径已调整（D-18/668d3c1）；安装成功证明与撤销原子性**仍在验证中**；
新增读失败已定位到成因类别（未刷盘尾部/被读成截断），待修。

### D-20. step 1aa–1ac：截断读的字节级核对（发布时序假设被证伪；修复未生效）— 见提交

**要做的字节级核对（已实现）**：读失败现场打印
`locator(part/gen/off)` + `cache/fd 状态` + 文件路径与长度 + 分区状态
（`DescribePartition`：active_gen/is_active_gen/flushed_size/total_size/buf_bytes/
file_size/seal_bytes/seal_records）+ **同一句柄同一 offset 的独立头解析**
（want/got 字节、total、二次读 want/got、decode、key、seq）。

**实测现场（`all 20000 4`）**
```
ZFDBG-readfail part=6 gen=0 off=10680 cache=NotFound: ZF sealed gen not in any active epoch
               fd=ok file=db_bytes/zfwal/zf-wal-6-0.log file_size=1086156(ok)
ZFDBG-readfail state: active_gen=1 is_active_gen=0 flushed_size=1127852 total_size=1128920
               buf_bytes=1068 file_size=1086156(ok) seal_meta=yes
               seal_bytes=1086156 seal_records=1017 seal_rec_known=1
ZFDBG-readfail hdr: want=24 got=24 st=OK | total=1068 want2=1044 got2=1044 st2=OK
               | decode=OK key=...2712... seq=129
```
**结论（按你给的判定表）**：
- 文件长度充足（`file_size == seal_bytes`）、记录在**同一句柄同一 offset 上重读完全成功且
  CRC/key/seq 正确** ⇒ **不是"记录跨过已刷出缓冲末尾却被发布为 sealed"**（发布时序假设证伪），
  也不是 locator 指向别的 key/seq。
- 失败与成功发生在**同一调用内的前后两次读**上 ⇒ 窗口是"**文件正被追加/刷出时被独立 fd 读到**"
  （读到扩展中的文件 → 短读 → `ZF record truncated`），而不是数据从未落盘。

**据此做的两处修改（均已构建）**
1. `Freeze` 发布前校验：`FlushBuf` + `Sync` 必须成功，且刷出后 `GetFileSize() >= total_size`
   才继续换代并发布 sealed；否则**中止封存**（保持原代活跃、返回 `sealed_bytes=0`），
   4 个调用点同步守卫（不换代、不冻索引、不登记 epoch）。
2. sealed 代读的**短读有界重试**（1ms×50，仅读可见性等待，不引入 fsync）+ 计数
   `read_short_read_retried`。

**效果：in-flight 失败未消除**（`all`：mismatch 460–880 / miss 465–662，反复运行同量级波动）。
但**重开后全部正确**：同一库 `verify`（另一进程）= `checked=20064 mismatch=0 miss=0`。
⇒ 失败只发生在**写-物化在飞窗口**；持久化数据无丢失。

**下一步要抓的缺失数据点（本轮未取到）**：上面记录的是"**失败之后**的重读"结果，
还缺**失败那次读本身**的字节数——需要在 `ReadFromSealed` 内部记录
`(offset, want, got, file_size_at_that_moment, status)`，才能最终区分：
  · 读接口短读契约（`RandomAccessFile::Read` 在文件小于 offset+len 时的返回语义）⇒ 修读路径；
  · 还是"读到扩展中的文件" ⇒ 修"sealed 发布/写可见性"（例如发布前在该代上做一次
    "长度=逻辑末尾"的**持锁复核**并直到通过；读者侧则对该窗口做退避重试）。

### D-21. step 1ad：原始失败读取证（结论与预期不同，未定位到最终生产者）— 见提交

**实现**：`ReadFromSealed` 内记录**每一次底层 Read** 的证据到线程局部结构
（`read_id/thread_hash/part/gen/stage/second_read/offset/requested/returned/ok/
result_size/parsed(k,v,total)`）；调用方在失败时**先打印原始证据**，再做诊断重读。
短读重试改为**默认关闭**的诊断选项 `ZF_READ_SHORT_RETRY=<n>`（验收要求关闭时通过）。

**实测（`all 20000 4`，重试关闭）**
```
ZFDBG-readfail raw: read_id=0  thr=0 part=0 gen=0 ... want=0 got=0 st=ERR ...   ← 全零
ZFDBG-readfail raw: read_id=13 ... part=4 gen=0 stage=0 second=0 off=9612
                    want=4120 got=4120 st=OK result_size=4120 parsed(k=16 v=1024 total=1068)
ZFDBG-readfail raw: read_id=5  ... part=4 gen=0 stage=0 second=0 off=6408 ... st=OK
ZFDBG-readfail raw: read_id=29 ... part=7 gen=0 ... st=OK
```
**两条关键事实**
1. 被打印的底层 Read **全部成功**（`got == want == 4120`、`st=OK`、`total=1068 ≤ 4120`
   ⇒ **没有走补读分支**）——**与"短读/truncated"假设不符**。
2. 出现 `read_id=0`（全零）的失败 ⇒ 该次失败**没有经过 `ReadFromSealed`**（线程局部未被
   填写）⇒ 报出的 `ZF sealed record truncated` 由**另一条路径/另一线程**产生。

**因此两处判断需要撤回**：① "文件正在扩展导致短读"无证据；② 我先前"同一 fd 前后两次读"
的解读也站不住（那是诊断重读，不是被观测的那次读）。

**Slice 生命周期核查（你要求的一项，已完成）**：`ReadFromSealed` 返回
`Slice(*buf)`（指向**调用方**缓冲）、局部 `rec` 仅函数内使用 ⇒ **无悬垂/共享覆盖问题**。

**下一步（唯一缺口）**：把产生 `truncated` 的**每一处**都打唯一标记（
`ReadFromSealed` 380/411、`ReadRecord` 644/657、`ReadActiveRecordChecked` 937/952、
scanner 107/125），并带上 thread_hash 与 read_id，直接看是哪一处、哪个线程触发；
随后按你给的证据表分支修复。**未完成**：确定性复现（同步钩子暂停于刷缓冲/发布/换代
边界）、最小修复、关闭重试后的 `all` 零错误、整单元撤销断言。

**`Sync()` 实现核实（你要求的一项，已完成）**：`Freeze` 调用的 `p->wfile->Sync()` 是
`WritableFile::Sync`（`use_fsync=false` 时走 `fdatasync`/`fsync` 路径，取决于 Env 实现）
⇒ 我先前"只加可见性检查、不引入 fsync"的说法**不严谨**：该调用本身可能触发 fdatasync；
D-20 的守卫并未新增调用，只是**校验其成功与长度**。

### D-22. step 1ae/1af：错误产生点 ↔ Get 返回的关联链（已通）— 见提交

**实现**
- `get_id`（每次顶层 Get 分配）/`attempt_id`（每次内部重试递增）/`thread_hash` 关联标识；
  **每个 attempt 开始时清空**诊断状态（防"上一次成功读取的线程局部残留"被当成本次证据）；
  未经过的路径在打印时标 `NOT_VISITED`。
- 四处 truncated 产生点分配唯一 `error_site`：`ReadFromSealed` header=1 / body=2，
  scanner=21/22，`ReadRecord`=11–14（含缓冲与文件两条）。失败返回点打印
  `get_id/attempt/thr/site/part/gen/off/want/got/parsed_total/cond/status => FINAL IOError`。

**实测（`all 20000 4`，短读重试关闭）**
```
ZFDBG-errchain get_id=2 attempt=7 thr=… site=15 part=4 gen=1 off=13928
      want=4120 got=4120 parsed_total=3384520122 cond=2
      status=bad record magic => FINAL IOError
site 分布: site=15(bad magic)=473  site=1(header 短读)=16  NOT_VISITED=0
```
**结论（按你的判定表）**
- **97% 的失败是 `bad record magic`**：`want=4120 got=4120`（读取本身完全成功），
  但 locator 指向的字节**不是 ZF 记录头**；解析出的"长度"是垃圾（`parsed_total`
  可达 3.4GB）。⇒ 属你表里第三类："**offset 处是其他 key/seq，或句柄身份不符**"，
  即**陈旧/错位的 locator**，与"短读""封存发布窗口"都无关。
- 其余 16 例是 header 短读（返回 < 24B）：与"locator 偏移落在文件末尾之外"一致，
  同属陈旧 locator，而非封存时序。
- 关联链完整：**全部失败都有对应产生点（NOT_VISITED=0）**。

**已据此加的守卫**：`ReadFromSealed` 在信任长度前校验 `kZfMagic`，不匹配即返回
`ZF sealed record: bad magic at locator`（并登记 site=15）——把"垃圾长度导致的 truncated"
变成可分类的明确错误，且不再把它误判为短读。

**撤回**：D-20/D-21 关于"封存发布过早/短读窗口"的方向（D-20 的封存可见性守卫与 D-21 的
短读重试都不是本问题的修复；重试已默认关闭）。

**下一步（本轮未做）**：① 追**陈旧 locator 的产生点**——为什么索引里的 `(part, gen, offset)`
与文件内容不再对应（候选：代号复用/同代号文件被后续追加、`InsertCreate` 把记录归到错的代、
或索引条目在代回收后未清理）；② 读路径对"magic 不符/键不符"的处置（按你之前的协议：
刷新读视图后回落 LSM，或明确报错，不能静默返回旧值）；③ 之后才是确定性复现、最小修复、
关闭重试后的 `all` 零错误、整单元撤销断言。

### D-23. step 1ag：locator 的 A→B→C→D 四段对照（结论：locator 与文件内容一致，问题在句柄身份）— 见提交

**实现**（`ZF_DBG_LOCATOR=1` 启用；A 表有界 40 万条，append_id 关联）
- A：`Append` 出口登记 `(part,gen,offset)+key_hash/seq/type`；
- B：索引插入时解码收到的 locator 并与 A 对照（键哈希/seq/(part,gen,offset)），计 `LocatorBMismatch`；
- C：选择胜出后解码胜出 locator 并与 A 对照，计 `LocatorCMismatch`；
- D：读取现场打印 locator 的 A 表记录 + **原始头 24 字节 hex** + 字段（magic/klen/vlen/seq）+ 句柄指针。
- 另加：长度合法性约束（site=16 `implausible length`，上界 64MB，避免垃圾长度触发巨额分配）；
  magic 校验保留（site=15）。

**实测（`all 20000 4`）**
```
ZFDBG-readfail D: rf=0x79b9680009d0
   hex=[55 46 30 31 00 00 01 00 10 00 00 00 00 04 00 00 41 00 00 00 00 00 00 00]
   magic=0x31304655('ZF01') klen=16 vlen=1024 hdr_seq=65
   | A(append): found seq=65 key_hash=16449318014064423359        ← **完全一致**
（其余三例同形：seq 196/68/203 与 A 表逐一相等）
```
**因此**：
- locator 的 `(part,gen,offset)` **确实来自真实 Append（A 命中，且 seq/key_hash 一致）**；
- 在**同一 offset** 上，用 fd 兜底句柄读到的 24 字节是**合法记录头**（magic/klen/vlen/seq 全对）
  ⇒ **locator 有效、文件内容正确**；
- 但**失败的那次读**（errchain 里 `parsed_total` 达 GB 级、site=16/1）解析出的是垃圾
  ⇒ 两次读**不是同一个文件对象**：D 段打印的句柄指针每例不同（`0x…09d0` / `0x…3760` / …），
  失败走的是**选择时 pin 下来的句柄**，兜底/诊断走的是**按路径新开的句柄**。

**归属（按你的判定表）**：第 3 行——"**C 正确，D 使用了别的文件或偏移 ⇒ 句柄绑定、换代检查**"。
即 **pin 下来的句柄在其文件被替换/重建后仍然被使用**（POSIX 下旧 inode 仍可读 ⇒ 读到异内容），
而不是 locator 错、也不是短读/封存窗口。

**保留与收敛（你第 3 条要求）**
- magic 校验、长度合法性约束均保留；解码成功后**仍需核对 key/seq/type 与所选索引记录一致**
  （已具备 A 表可对照，下一步落地到读路径）；
- `bad_magic` **不**凭 fence 回落 LSM（保持原始错误）。

**下一步（本轮未做）**：① 读路径在解码成功后核对记录身份（key/seq/type 对所选索引条目），
不一致即判定"句柄与 locator 不匹配"并按明确协议处理（重新解析句柄或报错）；
② 追**pin 句柄为何失效**——文件被替换/重建的路径（unlink+recreate / 代号复用）与
`SealedFileCache` LRU 句柄的生命周期；③ 之后才是确定性复现、最小修复、关闭重试后的
`all` 零错误、整单元撤销断言。

### D-24. step 1ah/1ai：严格配对的原始头 vs 兜底头（结论：同一 locator 两种内容）— 见提交

**实现**
- `ReadFromSealed` 在**失败那次 Read 返回后立即复制**头 24 字节进证据（`original_header`，
  不做任何重读），并做一次**无 I/O 的独立解码**（`parse_rc`：1=magic 2=长度 3=短读）；
  记录 `result`/`scratch` 地址与长度。**兜底头**单独打印（同一 offset、按路径新开句柄）。
- 错误站点改为**环形 8 条**（每条自带 `get_id/attempt_id`），不再依赖"最新一条"，
  避免后续成功读覆盖失败现场；打印时按 `get_id` 过滤。**pin 路径也打印原始头**
  （此前只有 fd 兜底分支打印，导致"配对"其实来自两个不同失败点 ✗）。
- 撤回的推断：`rf` 指针不同 ≠ 文件不同（两个对象可打开同一文件）；POSIX unlink 不会
  让旧 fd 读到别的文件的字节 ⇒ 先前"pin 句柄失效"的说法**未获证据支持，已撤回**。

**实测（同一 locator、两条路径）**
```
① pin 路径（严格配对：该次失败的原始头）
ZFDBG-hdrcmp pinned path=PINNED part=4 gen=1 off=79076 want=4120 got=4120
  header=[84 8b 92 99 a0 a7 ae b5 bc c3 ca d1 d8 df e6 ed f4 fb 02 09 10 17 1e 25]
  decode: magic=0x99928b84 klen=3519726524 vlen=3991330776
  parse_rc=2 error_site=16  status=implausible length at locator
  注：字节呈 +7 等差数列 ⇒ **落在记录 payload 内部**（不是记录起始）。
② 兜底路径（同一 offset、按路径新开句柄）
ZFDBG-hdrcmp fallback_header=[55 46 30 31 00 00 01 00 10 00 00 00 00 04 00 00 41 …]
  magic=0x31304655 klen=16 vlen=1024 seq=65（与 A 表 **seq/需求一致**）
```
**因此**：同一 `(part, gen, offset)` 经**两条路径读到不同内容**——pin 路径读到 payload 中部，
兜底路径读到合法记录头。属判定表第 3 行变体"**原始头错误、兜底头正确**"⇒ 不一致点定位在
**读取所用的文件对象**（句柄缓存键 / 打开路径 / generation 绑定）。

**仍未确定（不得写成"根因已确认"）**：为什么 pin 路径会指向另一个文件对象。按你的要求，
下一步用**句柄身份（device/inode，经 fstat/fstatat）**而非路径 stat 证明两个句柄指向不同文件，
再查 `SealedFileCache` 的缓存键、打开路径与 generation 绑定。当前结论口径：
**"错误产生点已关联；locator／文件内容不一致已证；不一致的产生机制待定位。"**

**其他保留项（按你要求）**：magic 校验与长度护栏保留（64MB 已标注为**诊断阈值**，
非格式上界）；64MB 只作非法输入护栏；记录身份校验（key/seq/type 对所选索引条目）仍待落地；
`bad_magic` 不凭 fence 回落 LSM。

### D-25. step 1aj：**locator 与读引用分离**（用户假设成立）→ 最小修复 → `all` 归零 — 见提交

**缺陷（代码审查直接定位，正如用户假设）**：`SelectBestIndexRecord` 把 pin 回调写进**单个
共享槽** `slot`，而 locator 的 `best_*` 字段只在候选胜出时更新：
```
for (candidate ci) {
  GetCopy(..., pin_fn, /*同一个*/ slot);      ← 每个**命中**的候选都往共享槽写 pin
  if (cand_seq > best_seq) { best_locator = ci 的 locator; }   ← 只有胜出者更新 locator
}
```
⇒ 后访问的**落选候选**（其段 `max_seq` 高、不被剪枝）会把 pin 覆盖成**自己的句柄**，而
locator 仍属于胜出候选 ⇒ **locator 与读引用首次分离就在此处**（与 D-23/D-24 的现场一致：
pin 路径读到 payload 中部、兜底路径在同 offset 读到合法记录头）。

**最小修复（按用户处方）**：每个候选用**自己的** `ZfPinSlot`（读引用在索引保护释放前、
同一 `GetCopy` 调用内取得）；候选择胜出时，**locator + seq + type + 物理代 + 读引用作为
一份结果整体替换**（`*out_slot = std::move(cand_pin)`），未胜出者的 pin 直接丢弃。

**确定性用例（用户指定的构造）**：`semtest` case7——A=同键 seq=200（先访问、胜出），
B=同键 seq=100（后访问、不胜出），并抬高 B 段的 `max_seq`（同段插入 seq=100000 的其它键）
确保不被剪枝：
```
case7 locator-pin-same-candidate: PASS
      (A=(part=4 gen=0 seq=200) B=(part=6 gen=0 seq=100, 段 max 抬高) status=OK correct=1)
```
（修复前该构造下读会解码到 B 的记录 ⇒ 值不匹配；现稳定通过。）

**门禁结果（本轮）**
```
all   : checked=30064 mismatch=0 miss=0 unexpected_found=0 errors=0   ← 连跑两次均全零
目标模式新库：l0_install=0 / drift=0 / gen_lost_before_install=0 / scan=0 / triage=0(B1=B2=B3=0)
```
**未验证/待办**
- **压力组回归本轮未完成**：两次后台运行都被终止（SIGTERM/timeout），结果未知，需重跑
  （`stress 1/5` 在 `GetCopy` 改动后必须复验）。
- `semtest` case6（A 类人工注入）现在报 `FIXED`——因为安装销账改动后真实索引条目被保留
  （step 1t），其"索引里没有该键"的前提不再成立 ⇒ 需要按新前提重写该用例（它仍是 A 的
  防御性门禁，但当前不再是有效红灯）。
- 整单元撤销断言、提交失败注入、`level=-1` 融合进展、A 的合法路径结论、固定路由仍待做。

### D-26. step 1ak：case7 覆盖条件收紧、case6 标 SETUP_INVALID、case5 出现回归 — 见提交

**1. case7 的覆盖条件（按用户不等式）**
初版把 B 段上界抬到 100000 ⇒ `upper(A)=200 < upper(B)` ⇒ **B 先访问**，未覆盖缺陷形态。
按 `A.upper > B.upper > A.seq > B.seq` 重做填充后：
```
case7 locator-pin-same-candidate: PASS
   A=(part=4 gen=0 seq=200 upper=100000) B=(part=6 gen=0 seq=100 upper=300)
   upper_order_ok=1 => 访问顺序 A→B、B 命中但落选；status=OK correct=1
```
覆盖条件（`upper_order_ok`）现纳入断言，避免"抬 B 上界反而先访问"的假覆盖。
**未做**：父提交（`668d3c1` 之前）跑同一用例以证明"父提交失败、`9229709` 通过"——
需要额外一次全量重建，本轮未做，不声称已完成。

**2. case6 状态修正**
```
case6 A_CLASS_REPRO: SETUP_INVALID(真实索引条目已被保留，旧前提不成立) … 返回来源=INDEX correct=1
```
理由：step 1t 之后未安装代的索引条目会被保留，case6"索引里没有该键"的前提不成立 ⇒
不再显示 FIXED，也不作为红灯（其 `fails` 计数已移除；当前 `fails=1` 来自 case5，见下）。

**3. 新回归：case5（活跃代 + Freeze + unlink）**
```
case5 active_gen_survives_freeze: FAIL
   (status=ERR correct=0 read_error=1 gen_before=0 gen_after=1 rotated=1
    readfail_fellback=0 propagated=1)
```
在 `9229709` 之前该用例通过（`status=OK propagated=0`）。改动后：选择时该代是活跃代
（无法 pin，符合设计）⇒ 读取走 `ReadValue`；Freeze 换代后走封存路径；文件被 unlink 后
封存路径失败 ⇒ 传播错误。**需要查**：为何此前能成功（封存缓存/LRU 句柄在该次 unlink 后
仍可读），以及 per-candidate pin 改动如何改变了保留的句柄集合。属"未安装代读依赖"
一类，下一步优先。

**4. 压力回归**
仍未完成（前次后台运行被终止）。验收要求：短读重试关闭、五组零错误。

### D-27. step 1al：case7 执行记录断言（构造仍未打通）＋ case5 合法回收改写未做 — 见提交

**本轮做的**
- 新增**候选访问执行记录**（`CandidateVisits`：part/order/hit/seq，`SelectBestIndexRecord` 内
  按访问顺序写入）——用于"上界关系只说明应被访问，执行记录才证明覆盖"。
- case7 断言扩展为 `visited_A & visited_B & B_hit & B_lost`（执行记录）+ `upper_order_ok`。

**仍未打通（两个已定位原因）**
1. **`LocatorDiagEnabled()` 在首次调用时缓存 env**（`static const bool on = getenv(...)`），
   而 case7 是**测试中途**才 `setenv("ZF_DBG_LOCATOR","1")` ⇒ 访问记录始终为空
   （`visited_A=0 visited_B=0`）。应改为每次读取 env，或让调用方显式传开关。
2. case7 的合成条目仍未进入读路径（`status=NotFound`）。已排除"用 WAL 文件的 gen 插入"
   （改为插入时刻的活跃代），仍 NotFound ⇒ 需按"Insert 是否被丢弃（`insert_dropped`）"
   与"候选集合是否包含这两个 part"两条继续核对。**case7 当前不是有效回归**（既没覆盖、
   也不红灯），不能作为修复的证据；`all` 两次全零 + 代码审查（D-25）仍是该修复的依据。

**case5 合法回收改写（用户第 3 步）未做**：应改为"选中活跃记录 → 换代 → 安装 → **走正常
回收**（因读引用存在而延迟）→ 恢复读取正确 → 释放引用后回收成功"，并断言"读期间不提前
回收""读完后不永久保留"。当前 case5 仍是**手工 unlink**（且未先校验 `installed` 与引用数）
⇒ 它此前的通过/失败都不能作为正常生命周期验收，需按上述改写。

**压力回归**：仍未完成（需在短读重试关闭下跑，分批记录退出码/轮数/汇总）。

### D-28. step 1am：case7 重建为隔离 fixture（带前置断言，当前 INCOMPLETE）— 见提交

**按用户要求重建**：不再往大库里注入"未写入的键指向别的键记录"（那不是合法读状态），
改为**只用真实记录**构造：
- 目标键 K 由本进程真实写入（多次，跨换代）；
- A = K 的**较新**记录、B = K 的**较旧**记录（二者的 key/seq/type 与各自 WAL 记录一致）；
- 段上界由填充键抬高，并按 `A.upper > B.upper > A.seq > B.seq` 断言访问序；
- **前置断言**：目标段存在、两条插入成功（返回字节>0）、分别查询 A/B 段能得到预期记录、
  跨-part 成立；不满足即 `INCOMPLETE`（**独立非成功状态，exit=3，汇总计"未完成"**）。
- 诊断开关 `ZF_DBG_LOCATOR` 由**进程启动前**设置（不再中途 setenv）。

**fixture 演进的实证（前置断言的价值）**
```
第 1 版: INCOMPLETE (ins(A=0,B=0,fillA=42,fillB=42) probeA=1 probeB=0
                     partA=4 gA=0 seq=2 partB=4 gB=0 seq=1)
         ⇒ 同一键写两次落在**同一段**（partA==partB），且两条真实条目**已存在**
           ⇒ 合成插入被判重复（返回 0），前提不成立。
第 2 版: 加"写入→换代→再写"循环（400 键×6 轮）⇒ 仍 partA==partB==4 ⇒ INCOMPLETE。
第 3 版: 提到 3000 键×10 轮 ⇒ **跨 part 成立**（partA=0/gA=1/seq=30011、
         partB=4/gB=0/seq=1），但 `ins(A=0,B=0)` 仍为 0（真实条目已存在 ⇒ 重复）
         且 `probeB=0`（part 4 的段可能已因安装释放）⇒ 仍 INCOMPLETE。
```
**结论（本轮）**：fixture 的**跨-part 前提已可达成**；剩余两项待解——① 不该再插入
（A/B 已是真实条目，应**直接使用**并只验证可查询性）；② `probeB` 失败需区分"段已释放
（数据在 LSM）"与"条目不可达"。因此 **case7 目前仍是 INCOMPLETE（exit=3），不是有效回归**；
`9229709` 的确定性回归仍未闭环（其依据仍是 `all` 两次全零 + 代码审查）。

**case5 真实回收验收（用户第 3 步）未做**：仍需"取得跨 active→sealed 的 generation 读引用
→ 换代 → 安装 → 正常回收不提前发生 → 读正确 → 释放引用后回收成功"，以及
"手工 unlink"独立为破坏性测试。当前 case5 仍为手工 unlink。

**压力回归**：仍未完成（短读重试关闭下分批跑）。

### D-29. step 1an：case7 组件级隔离 fixture **通过**（含旧实现对照）— 见提交

**按用户指令重建（不依赖路由/后台物化/大写入）**：组件层直接构造——
- 用 WAL 管理器的 `Append` 接口在**两个显式 physical part** 写受控记录（返回真实 locator）：
  K 旧值→B(seq=100)、K 新值→A(seq=200)、填充→B(seq=300)、填充→A(seq=100000)；
- `Sync` 两个分区后，把四个条目**各插一次**进各自索引段（`Active(part)` 保证段存在）；
- 前置断言：追加成功 / 四插成功 / 分别验证 A、B（索引找得到预期版本，且 locator 按
  `ReadValue` 读回并核对值与 gen 一致）；
- 调用**生产选择逻辑**（新增组件级入口：把 `ZfPinSlot` 公开，fixture 传**真实槽**，
  避免此前传 nullptr 读到上一次调用的陈旧身份）；`ZF_DBG_LOCATOR` 由进程启动前设置。

**结果（新实现）**
```
case7 fixture: appends=1 ins=1 candA=1 candB=1
   sel =(part=2 gen=0 off=0 seq=200)=expect      ← locator 属于 A
   pin =(part=2 gen=0 pinned=0 active=1)         ← **读引用也属于 A**（活跃代暂不可 pin）
   exec[A=1 B=1 B_hit=1 B_lost=1]                ← 实际访问 A→B，B 命中但落选
   legacy=off => PASS        semtest cases1-7: PASS (fails=0 setup_incomplete=0)  exit=0
```
**旧共享槽实现对照**（`ZF_TEST_LEGACY_PIN_SLOT=1`，仅诊断开关；每个命中候选都写输出槽）
```
   pin =(part=3 gen=0 …)     ← 读引用被落选候选 B 覆盖（part=3=B）
   => FAIL（用例按预期失败）
case7 legacy-check: PASS（legacy 模式：用例按预期失败 ⇒ 用例有区分力）
```
⇒ **同一 fixture 下：旧实现失败、新实现通过**，确定性回归闭环（不再依赖 `all` 的统计）。
`INCOMPLETE` 处理保留（exit=3，汇总计"未完成"）。

**仍未做**：case5 的真实回收验收（读引用存在时延迟回收 → 读正确 → 释放引用后最终回收；
需先实现跨 active→sealed 的 generation 读引用，不能靠跳过 unlink）；压力组回归
（短读重试关闭下分批）；整单元撤销与提交失败注入；`level=-1` 融合进展；合法路径 A；固定路由。

### D-30. step 1ao：generation 读引用（已实现）+ case5 真实回收验收（INCOMPLETE）+ 汇总拆开 — 见提交

**已实现（case5 的机制前提）**
- `SealedFileCache` 新增 **per-(physical_part, gen) 读引用**：`AcquireGenReadRef/ReleaseGenReadRef/
  GenReadRefs/gen_read_ref_total/reclaim_deferred_by_reader`；引用**不计入 pending**
  （pending 仍只由 registry 的 sealed && !installed 定义）。
- 回收路径尊重读引用：`ReleaseGens` 在引用存在时**不 unlink、保留登记条目**，计
  `reclaim_deferred_by_reader`；最后一个引用释放时，若该代**已安装且回收开启**，此刻入队 unlink。
- 引用**在选择保护释放前取得、随胜出候选整体转移**（`ZfPinSlot.read_ref_taken`），
  **活跃代也取**（活跃代数据可能仍在写缓冲，换代后由该引用保证不被回收）；
  读完成后（成功或失败）释放。
- 复跑证据：`refs_during=1`（读取期间确实持有引用）、`read status=OK correct=1`。

**case5（真实回收验收）当前状态：INCOMPLETE**
```
case5 real-reclaim: INCOMPLETE(该代未安装 ⇒ 回收验收不适用)
   refs_during=1 installed=0(gen=0 fence_last=0) deferred_by_reader=0
   file_during=308652 file_after=308652 reclaimed=0 reclaim=on | read=OK correct=1
```
未安装的原因：测试写入（定向 12×400 键）未能把 part5 推到换代（`fence` 无该分区条目）。
按用户要求**不把"未安装 ⇒ 不适用"显示为通过**：新增前提判定 ⇒ 报 `INCOMPLETE`，
`setup_incomplete` 计数 +1，`semtest` 返回 **3**、汇总标 `SEMTEST_INCOMPLETE`。
（同时修掉两处测试逻辑错误：无 fence 条目被当成 `fence_last=0` 误判 installed；
未安装时不该要求"最终回收"。）

**汇总拆开（用户要求）**
```
semtest per-case: case1/2/3=见上（各自 PASS/FAIL）  case5=INCOMPLETE(未完成)
                  case6=N/A(前提失效)  case7=见上
semtest cases1-7: SEMTEST_INCOMPLETE (fails=0 setup_incomplete=1)   exit=3
```
即 case5（未完成）与 case6（不适用）**不再被总 PASS 掩盖**。

**未做**：让 case5 真正做到"该代安装成功 → 回收延迟 → 读正确 → 释放后最终回收"
（需要把目标分区确定性推到换代并完成安装）；case7 的 sealed 变体（封存+登记后断言
`pinned=1` 并经由 pin 读取 A）；压力组回归；整单元撤销与提交失败注入；融合进展；A；固定路由。

### D-31. step 1ap：case7 **真实 sealed pin 验收通过**（旧实现对照仍失败）；case5 仍 INCOMPLETE

**case7 sealed 变体（按用户要求）**：复用同一组件 fixture，把 A/B 走**组件级封存**——
`wal_->Freeze(part)`（production Freeze：FlushBuf+Sync+长度校验+换代）+ `index_set_->Freeze`
（活跃段接替、旧段冻结）+ `sealed_cache_->RegisterGen` + `AddEpochWithRecoveryAdoption`
（registry 登记；工具无法构造 memtable/epoch 归属那部分，已注明）——然后：
```
case7 fixture: appends=1 ins=1 candA=1 candB=1
   sel=(part=2 gen=0 off=336420 seq=200)=expect     ← locator 属于 A
   pin=(part=2 gen=0 pinned=1 active=0)             ← **真实 sealed 句柄**（pinned=1，非活跃代）
   exec[A=1 B=1 B_hit=1 B_lost=1]                   ← 访问 A→B，B 命中但落选
   => PASS
case7 sealed: sealed=1 pinned=1 pin_read_A=1
```
`pin_read_A` = **经该 pin 句柄**原始读取记录头并解码，核对 **key/seq(=200)/type(VAL)/value**
全部匹配 A ✓（此前 `pin_read_A=0` 是**我的检查写错**：`ReadFromSealed` 只回填 value，
不能拿它解记录头；已改为直接从句柄读原始 24B+payload）。

**旧共享槽对照（同一 fixture）**
```
pin=(part=3 gen=0 pinned=1 active=0)   ← 读引用被落选候选 B 覆盖（part=3=B）
=> FAIL；case7 legacy-check: PASS（有区分力）
```

**case5 仍 INCOMPLETE**（本轮未推进）：`refs_during=1`（读引用取得并持有）✓、读正确 ✓，
但目标代**未安装**（`installed=0`；按用户纠正：无 fence 只说明"未证明安装"，换代需直接核对
`active_gen`）。下一步按用户要求：**显式封存目标物理代 → 驱动正常物化与安装 → 若安装迟迟
不完成则报"安装进展失败"**（不再加写入、不再归为 fixture 未建成）——这可能直接触及
尚未解决的融合/排空问题。

**读引用契约（用户第 2 条）**：RAII 化与"最后引用释放 vs 新读者取得"的同步协议**未做**；
当前实现仅在 `mu_` 下检查引用计数（检查与入队同一锁内 ✓），但"入队后新读者再来"的竞态
未加协议，需与 case5 一起解决。

### D-32. step 1aq：RAII 读引用 + retiring 门（已实现）；case5 仍缺"确定性封存入口"

**已实现（用户"最优先"项）**
- `SealedFileCache::GenReadRef`：**可移动、不可复制**的 RAII 句柄；落选候选、胜出替换、
  错误/提前返回都**恰好释放一次**（析构调用 `ReleaseGenReadRef`）。
- **同一把锁下的两个互斥决定**：
  * 取得引用：`TryAcquireGenReadRef` —— 已 retiring ⇒ **拒绝**（计 `acquire_rejected_retiring`）；
  * 开始回收：`refs==0 && installed && reclaim_enabled` ⇒ **标记 retiring + 入队**
    （`ReleaseGens` 与 `ReleaseGenReadRef` 两处都先标记再入队）。
  ⇒ 入队后**不再接受新引用**（不再是"检查为零并入队、却继续接受读者"）。
- 读者选中了已 retiring 的代 ⇒ `SelectBestIndexRecord` **不静默跳过**，而是让该候选失效并
  继续尝试其它候选（重选）；调用方既有的"刷新读视图后重试"协议保留。
- 读引用不改变 pending 计账；**安装不等待读引用**（避免"读线程暂停等安装 / 安装等读引用"
  的环形等待）。

**case5 当前状态：INCOMPLETE（未换代）**
```
case5 real-reclaim: INCOMPLETE(该代未安装 ⇒ 回收验收不适用)
   refs_during=1 read=OK correct=1 installed=0 fence_last=0 rotated=?（未换代）
```
本轮把判据改成"**直接核对 active_gen 是否换代**"（用户纠正：无 fence ≠ 没换代），并新增
`INSTALL_PROGRESS_FAILURE` 分支：**若已换代但迟迟未安装，直接报安装进展失败**（不归为
fixture 未建成、不手工标记 installed）。当前走的是"未换代"分支 ⇒ 我用的排空入口
（`ZfDrainNow`）没有封存到目标分区 part5。
**下一步（确定性封存入口）**：用封存阈值旋钮（`ZF_VC_PARTITION_TARGET_KB`，已有）让目标
分区的写入**必然触发换代** + 目标定向写入 ⇒ 断言 `active_gen` 变化 ⇒ 再驱动安装。
这不算"增加写入量碰运气"（是降低阈值使封存确定发生）。

**压力回归**：仍未做（关闭短读重试；引用机制改动后必须重跑）。

### D-33. step 1ar：retiring 拒绝→整次重试；case5 真实回收闭环 **PASS** — 见提交

**1. retiring 拒绝不再降级（用户第 1 项）**
- 胜出候选因 retiring 无法取得读引用时：**整次选择作废**（置 `RetryReadViewNeeded`
  + 线程局部 `RetiringRejected`），`GetFromPartitionIndex` 据此走既有协议——
  `retry_requested` + `Status::TryAgain()` ⇒ DBImpl **刷新读视图并保持 read_sequence** 重试；
  刷新后仍无法安全完成则返回**明确 IOError**（`no downgrade`）。
- **不再 `continue` 找其它候选**（那会返回更低版本且不触发重试）。

**2. 指定代封存入口（生产路径，用户第 2 项）**
- `ZeroFlushContext::ForceSealForTest(impl, cfd, part, expect_gen, &sealed_gen)`：
  复用**完整生产封存流程** `FreezePartitionTailLocked`（WAL 封存 + registry 登记 +
  索引换代 + 物化调度；经 `DBImpl::ZfForceSealPartition` 持 DB mutex）。
  返回 0=封存成功、1=该代**已封存**（核对 `GetSealMeta` 即可，不制造新代）、-1=失败。
  参数取自**读者实际选中的 locator**（`LastPinIdentity` 另存全局供测试主线程读取，
  TLS 跨线程不可见——本轮修掉这个接线缺口）。
- 未用阈值/路由碰换代、未增加写入量、未手工标记 installed。

**3. case5 结果（真实回收闭环）**
```
case5 real-reclaim: PASS
   refs_during=1（读引用在选择时取得并持有）installed=1（fence 有该分区条目且覆盖该代）
   file_during=1068（读期间文件仍在 ⇒ **未提前回收**）
   file_after=1068 reclaimed=1068 reclaim=on（读完引用释放后**最终回收完成**）
   read status=OK correct=1 src=INDEX（放行后读取正确）
semtest cases1-7: PASS (fails=0 setup_incomplete=0)   [case6=N/A(前提失效) 单列]
```
⇒ **回收既不提前、也不永久推迟**；且"引用恰好在选择时取得、读取完成后释放"由 RAII 保证。

**仍未做（下一阶段）**：retiring 拒绝的小用例（新版本已装、旧版本仍在索引 ⇒ 重试后返回
新版本，不返回旧版本）；case5 的"retiring 未 unlink 期间取新引用被拒"竞争小检查；
**压力组回归（短读重试关闭，引用机制已改动必须重跑）**；整单元撤销与提交失败注入；
`level=-1` 融合/排空阻塞（若 case5 的 INSTALL_PROGRESS_FAILURE 再现则优先追）；
合法路径 A；固定路由。

### D-34. step 1as：retiring 两个小用例（当前红，根因已定位）+ 压力组 1（未完成）— 见提交

**1. 两个 retiring 小用例已写入（`semtest` case8a/8b），当前 FAIL：**
```
case8b retiring-gate: FAIL (retiring=0 new_ref_rejected=0 rejected_count=0->0
                            deferred_by_reader=1 file_while_held=1068)
case8a retiring-reject-no-downgrade: FAIL (sel_returned=1 retiring_rejected=0
                                           read_status=NotFound read_is_new=0 read_is_old=0)
```
根因（本轮定位）：
- **`ReleaseGenReadRef` 的 retiring 标记依赖 `gens_` 条目与 `installed`**：epoch 释放路径可能已
  擦除该条目 ⇒ 最后一个引用释放时 `git == end()` ⇒ **未标记 retiring**（`retiring=0`）⇒
  进而 8a 的"新引用被拒"根本没触发（级联）。修法：改为按"**回收在 refs>0 时被请求过**"
  的 `reclaim_pending_` 集合判定（入队条件已由触发方判定，不再二次依赖 registry/installed）。
- 8b 的"延迟"部分**已经工作**：`deferred_by_reader=1`、引用持有期间文件仍在（1068B）✓。
- 8a 的 `read_status=NotFound` 需一并查清（fixture 键只存在于 parts 4/5 的 WAL，
  读端候选并集为何没命中）。

**2. 压力组 1（关闭短读重试）两次前台尝试，均按"未完成"记录：**
- 第一次漏了 `ZF_VC_PARTITION_TARGET_KB=128` ⇒ `rotations=0`（未换代）⇒ 该次不计通过。
- 第二次用正确阈值：已完成轮次**全部 PASS**（`status=OK rotated=1 … PASS`），
  但被**外部 timeout（exit=124）在 44 轮处终止** ⇒ 按用户口径记**未完成**，
  不能当已通过（且单轮耗时明显上升，需先查是引用协议还是封存/安装路径变慢）。
- 验收要求仍未满足：`acquire_rejected_retiring / reclaim_deferred_by_reader / reclaimed`
  也需纳入每组汇总。

**后续顺序（用户 1–5）**：① 修 retiring 标记的判定并让 8a/8b 通过；② 压力组 1/5 → 五组全量
（关闭短读重试，记录 failures/silent_stale/notfound/corruption/acquire_rejected_retiring/
reclaim_deferred_by_reader/reclaimed，外部终止记未完成）；③ 整单元撤销与提交失败负例
（DeleteFile=0/AddFile=0/fence 不推进/installed+drained 不变/pending 保留/重启后可读，
LogAndApply 前后失败注入；若再现 `level=-1` 循环则追单个 job 的规划/融合输入/拒绝原因）；
④ 合法路径 A 的结论（case6 保持 N/A；新增合法写入/覆盖/删除/snapshot/安装交接用例，
多次固定运行均为零则记"当前协议下不可达"）；⑤ 固定路由落地（持久化边界 + 协议版本、
写读恢复共用映射、epoch 更替不重建、历史库走显式慢路径、新库额外候选查询为零）。

### D-35. step 1as：`reclaim_pending_` 状态已加，case8b 仍红（同一签名，下一步需在该决策点插桩）

**本轮实现（按用户第 1 条）**
- `SealedFileCache` 新增 **`reclaim_pending_`**（完整身份：`part/gen/installed/reclaim_enabled/
  request_epoch`）+ 断言计数 `assert_retiring_new_ref` / `assert_reclaim_unsafe` + `reclaim_pending_count()`。
- 回收资格确认但因 `refs>0` 延迟时：**记录完整身份**（幂等）。
- 最后一个引用释放时：**只处理该 `(part,gen)` 的 pending reclaim** —— 标记 retiring → 入队
  unlink（若 `reclaim_enabled`）→ 删除该 pending 条目；条件不足/回收失败时**保留 pending
  状态**（不丢记录）。
- 断言保留：`TryAcquire` 在 retiring 时拒绝（计数 `acquire_rejected_retiring`）。

**结果（仍 FAIL，签名未变）**
```
case8b retiring-gate: FAIL (retiring=0 new_ref_rejected=0 rejected_count=0->0
                            deferred_by_reader=1 file_while_held=1068)
case8a retiring-reject-no-downgrade: FAIL (sel_returned=1 retiring_rejected=0
                                           read_status=NotFound read_is_new=0 read_is_old=0)
```
已排除的环节：**延迟本身成立**（`deferred_by_reader=1`、引用持有期间文件仍在 ⇒ 说明
`TryAcquire` 成功、`ReleaseGens` 命中延迟分支并记录了 pending）；读引用释放路径也确实执行
（测试在持有后释放）。⇒ 失败点在 `ReleaseGenReadRef` 的**决策**（`inst` 或 `pend_enabled`
判定），下一步在该点插桩打印 `pending_found / inst / pend_enabled / refs` 四个值即可定位，
不再扩大范围。case8a 仍是级联（`retiring=0` ⇒ 拒绝路径未触发）；其 `read_status=NotFound`
待 8b 通过后按用户列出的逐项打印（new locator / WAL+SST 状态 / 旧 locator / retiring /
read_sequence / 重试前后候选链）单独处理。

**压力组 1**：本轮未跑成（预算用于 8b 修复）⇒ 仍记**未完成**。计划：先 `iterations=50`、
`ZF_READ_SHORT_RETRY=0`、watchdog 30s，记录每轮耗时与
`acquire_rejected_retiring / reclaim_deferred_by_reader / reclaimed`；稳定后再 300/500。

### D-36. step 1as 收口：读引用泄漏 + reclaimed 记账顺序 —— semtest 全绿，压力组 1/2 短跑通过

**1. 两个根因（互相掩盖，本轮同时定位）**

- **读引用泄漏（生产缺陷）**：`SelectBestIndexRecord` 胜出时调用
  `TryAcquireGenReadRef(best_phys, best_gen)`，但返回值被丢弃、没有交给胜出槽
  `out_slot->read_ref` ⇒ **每次索引命中泄漏一个 generation 读引用** ⇒ 该代永远"有读者"、
  回收无限期延迟。实证：case5 打印 `pending_reclaim={11:0(inst=1,en=1,ep=1 refs=1)}`
  在**读取已返回之后** refs 仍为 1、文件 1068B 永不删除、`purged=0`。
  修复：把引用存入 `out_slot->read_ref`（RAII，槽析构/被替换时恰好释放一次）。
- **记账顺序**：`ReleaseGens` 在延迟分支**之前**就 `gens_.erase()` + 计 reclaimed ⇒
  ① 记入 `reclaim_pending_` 的 `installed` 恒为 false、`retiring` 永不标记（case8b
  `retiring=0` 的直接原因）；② 文件仍在却已计 reclaimed。修复：注册表条目与 reclaimed
  记账统一推迟到 **`PurgePending` 真实 `DeleteFile` 成功之后**（新增
  `UnlinkRequest{name,key}`）；延迟期间条目保留、可读、可再取引用；失败保留 pending 不丢记录。
- **测试接线错误（第三处）**：`rocksdb.zeroflush.acquire_rejected_retiring` **未注册为属性**，
  读恒为 0 ⇒ case8b 的"新引用被拒"被误判为未发生。已补 `acquire_rejected_retiring` /
  `reclaim_pending_list` / `retiring_count` / `pending_unlink_count` / `retry_read_view_needed` /
  `assert_retiring_new_ref` / `assert_reclaim_unsafe` 属性。

**2. semtest（确定性用例）—— 全绿**
```
case1/2/3 PASS
case5 real-reclaim PASS
   pending_reclaim={} retiring=1 unlink_pending=2 → purge: file_final=0
   reclaimed=1068(基线=0 acct_ok=1) | read status=OK correct=1 src=INDEX
case8b retiring-gate PASS (retiring=1 new_ref_rejected=1 rejected_count=0->1
                           deferred_by_reader=1 file_while_held=1068)
case8a retiring-reject-no-downgrade PASS (sel_returned=0 retiring_rejected=1 db_gate=1
   read_status=IO error: "ZF: generation retiring; read view refresh could not complete
   safely (no downgrade)" read_is_new=0 read_is_old=0 read_is_notfound=0)
case7 fixture PASS (sel/pin 同候选：exec[A=1 B=1 B_hit=1 B_lost=1] legacy=off)
case7 sealed PASS (sealed=1 pinned=1 pin_read_A=1)
semtest cases1-7: PASS (fails=0 setup_incomplete=0)  exit=0
```
case8a 的逐项证据（用户指定打印）：
```
new_loc=(p=4 gen=0 off=0) wal_file=1068 registry=[present installed=1 bytes=1068]
                         retiring=1
old_loc=(p=5 gen=0 off=0) wal_file=1068            read_seq=261 latest=261
选择层候选链: [1001 hit=1 seq=200] [1001 hit=1 seq=400] [1001 hit=1 seq=600] [4 hit=1 seq=200]
DB Get 候选链(重取视图后): [1001 hit=0][1000 hit=0][4 hit=1 seq=200] ‖ 第二 attempt 同上
      rejected=2->4 retry_view=1->3
```
⇒ 协议完整走通：胜出代 retiring → 取引用被拒 → 整次读取作废（TryAgain）→ **保持
read_sequence 重取视图**（第二 attempt 命中同一 retiring 代）→ 仍无法安全完成 ⇒
**明确报错**（不返回旧值、不 NotFound、不静默降级）。

**3. case8a 验收口径收紧（本轮修正）**：`NotFound` **不合格**（原口径把 `not_old`
当作通过，会把"静默丢失"放过去）。现要求：新值，或 `!ok && !NotFound` 的明确错误。
另外 fixture 必须先做真实写入把 `latest` 推过 200（合成条目 seq=100/200 否则会被读序列
直接过滤掉、落 LSM ⇒ 与协议无关的 NotFound）——本轮已在用例内加入该前置。

**4. 压力组短跑（`ZF_READ_SHORT_RETRY=0`、watchdog=30s、`ZF_VC_PARTITION_TARGET_KB=128`）**

| 组 | 轮数 | failures | silent_stale | notfound | corruption | rotations | ms_avg/ms_max | 结论 |
|---|---|---|---|---|---|---|---|---|
| 1 基础跨 Freeze | 50 | 0 | 0 | 0 | 0 | 50 | 10142/10154 | PASS |
| 2 安装后回收 | 50 | 0 | 0 | 0 | 0 | 50 | 10144/10169 | PASS |

回收侧计数（本轮新增打印）：
```
组1: acquire_rejected_retiring=0 deferred_by_reader=0 reclaimed_bytes=0 gens=0
     断言 retiring=>no_new_ref=0(应0) reclaimed=>installed&&refs==0=0(应0)
          读引用未释放=0(应0) -> OK
组2: acquire_rejected_retiring=0 deferred_by_reader=0
     reclaimed_bytes=348168 reclaimed_gens=28
     reclaim_due=6 轮 / unlink 完成=6 轮  -> OK
```
- 组 1/2 的读都发生在**活跃代**（`pin_active=50`）⇒ 不触发 sealed 代引用/回收路径，
  三个计数为 0 是**预期**；确定性覆盖由 case5/case8a/8b 承担。
- `gen_lost_before_install`：组 1 = 0；组 2 首跑 = **5**，已定位为**测试带外删除**
  （原压力轮用裸 `std::remove` 删 sealed 文件，绕过引用计数协议）⇒ 改为经生产入口
  `PurgeSealedFiles()` 回收后，重跑 = **0**（同时 `reclaimed_gens=28` 证明引擎确实回收）。
- 每轮耗时 ~10.1s 的构成：**10s 安装等待期限**（`pending_materialize_bytes != 0`）+
  ~140ms 工作。⇒ 安装/物化在大部分轮次内**未在 10s 内完成**（与既有 level=-1 融合/排空
  滞后同源，属**已存在**问题，非本轮引入）；数据仍全部可读（50/50 正确、零 NotFound）。

**5. 新库门禁复验（本轮 build）**
```
write : gen_lost_before_install=0 unregistered_fd_fallback=0 l0_install=0 forbid_l0=1
scan  : mismatch=0 (lost_overwrite=0 unexpected_overwrite=0 other=0) / total=20000
triage: B1=0 B2=0 B3=0 + ACCEPTANCE PASS(no over-advanced fence on WAL-only data)
all   : checked=30064 mismatch=0 miss=0 unexpected_found=0 errors=0  VALCHECK PASS
        (wal_read_delta=21861 sealed_miss_delta=18)
```
注：`all` 结束时仍有 `gens=31 pending=43.1MB`（sealed 未安装的保留数据）⇒ 与"安装滞后"
一致，读路径正确（WAL-only 数据可读）。

**6. 状态**：`case7 通过` / `case5 通过` / `case8a 通过` / `case8b 通过` /
`压力组1 短跑(50×2) 通过`、`压力组2 短跑(50) 通过`；**完整组 300/500 进行中**
（300 轮已在后台启动，日志 `/tmp/g1_300.log`）。整单元撤销与提交失败注入、合法路径 A、
固定路由仍按用户顺序**未开始**。

### D-37. baccept phase3/4 在**修复前后同数失败** ⇒ 归因：既有的安装/排空滞后，非本轮改动

本机 A/B（同一目录、同一命令、同一参数；只改**两处生产行为**：① 选择层读引用是否交给胜出槽；
② reclaimed/registry 销账是否推迟到真实 unlink 之后）：

| build | phase1 baseline | phase3 install-after-release | phase4 reclaim |
|---|---|---|---|
| **旧行为**（临时恢复：引用丢弃 + 延迟前销账） | pending=7361724 drained=4336080 | pending=37612824 drained=4336080 → **FAIL** | registered_gens=18 reclaimed=4336080 → **FAIL** |
| **本轮修复** | pending=7361724 drained=4336080 | pending=37612824 drained=4336080 → **FAIL** | registered_gens=22 reclaimed=0 → **FAIL** |

⇒ `pending=37612824`（37.6MB 已封存未安装）在**两版都不排空**，phase3 失败与我的改动**无关**；
phase4 的数字差异正是修复的语义变化（`reclaimed` 只在真实 unlink 后计账 ⇒ 未安装 ⇒ 不计），
两者都因"未安装"而 FAIL。

**同源现象（本轮多路独立复现，指向同一个既有问题）**
- 压力组 1/2 每轮 ~10.1s = **10s 安装等待期限整段耗尽**（`pending_materialize_bytes != 0`）；
- 新库 `all` 结束仍 `pending=43.1MB gens=31 reclaimed=0`（读路径正确：WAL-only 数据可读）；
- baccept phase1 基线即 `pending=7361724`（记录在案的早期运行是 `pending=0`）。

**结论（状态口径）**：`B（安全安装）门禁**仍未通过**` —— 冲突解除后安装未在窗口内完成、
pending 未归零；这是**既有**的安装/物化滞后（与 level=-1 融合/排空同源），需下一步单独定位，
不得因"修复前后一致"而记为通过。归因方法：临时恢复旧行为 → 同数失败 → 恢复修复（`git checkout
HEAD -- zeroflush/...` + 重建），旧行为版本未提交。

**运行可复现性说明**：300 轮压力运行的二进制是 `9ba3d07` 的构建；该文件随后被 A/B 重建覆盖，
**二进制哈希未留存**（以提交号为准）。后续应改为"先算哈希再跑"。

### D-38. baccept phase3/4 的根因收窄：安装点在"无新写入"时推进不了（引擎自述 empty-edit 空转）

**实验 1（只用 `Flush()` 等待，当前提交行为）**
```
baccept phase1 baseline:  pending=7361724  drained=4336080  sst=32
baccept phase2 forced-conflict: PASS（5 批让位，drained/pending 均不变，fences 不变）
baccept phase3 install-after-release: pending=37612824 drained=4336080 → FAIL
      （12s 内 drained **一动未动** ⇒ 零安装）
baccept phase4 reclaim: registered_gens=22 reclaimed=0 → FAIL
```
- 与记录在案的早先运行差异：早先 `phase1 pending=0`，现在 `pending=7.3MB`，而
  **fence 内容与 SST 数完全一致**（`p2=[0,0]b1081884r1013`、sst=32）。
  ⇒ 安装量相同，差的只是**注册表里仍有未安装代**。早先的 `pending=0` 正是本轮修掉的
  "延迟前销账/抹除登记"造成的**假归零**；现在这个 7.3MB 是**诚实的未安装量**，
  必须由后续 epoch 的收养来安装——而 phase3 **不写新数据**，故永远等不到。

**实验 2（把等待改成生产排空入口 `ZfDrainNow()` 循环；仅实验，已回退）**
```
baccept phase1: pending=17023920 drained=4336080 sst=3328   ← SST 从 32 涨到 3328
baccept phase3: pending=38383920 drained=4336080 → FAIL（drained 仍纹丝不动）
引擎 LOG: "ZeroFlush close-flush: no real install progress (pending=16.2MB,
           empty_edit=2..N) — stop draining"   出现 **267 次**
```
⇒ `ZfDrainNow()` 循环能让引擎**反复封存/物化并写出大量 lvl=1 SST**（3328 个文件），
但 **`drained` 不前进、pending 反而上涨**，且引擎自己的"空 edit 无实质进展"探测器
每次都判定应停止排空。两点推论（下一步的定位入口）：
1. 空 edit 路径被**反复**走到 ⇒ 安装点在有规划、有产出的情况下仍然不提交输出；
2. 已被标记 installed 的代被**重复物化**（新 SST 不断产生）⇒ drained 不涨（只对
   registry 中未安装代销账），而真正未安装的（handoff/回收集合）始终没被安装。

**状态**：`B（安全安装）门禁：仍红`——phase3（冲突解除后继续安装并排空）与
phase4（保护非永久保留）在修复前后**同数失败**（见 D-37 的 A/B），根因已从
"本轮改动"排除，收窄到"安装点在无新写入/空 edit 反复出现时不再推进"。

### D-39. 压力组 1 **完整 300 轮通过**（观察窗关闭外部终止风险）

```
stress group=1 rounds=300 failures=0 silent_stale=0 notfound=0 corruption=0
               rotations=300 propagated=0 fellback=0 pin_ok=0 pin_active=300
               ms_avg=10141 ms_max=10403 -> PASS
stress gate: index_kept_uninstalled=1119 gen_release_before_install=0
             handoff_at_release=366 gen_lost_before_install=0
stress reclaim: acquire_rejected_retiring=0 reclaim_deferred_by_reader=0
                reclaimed_bytes=0 reclaimed_gens=0 |
                断言 retiring=>no_new_ref=0(应0) reclaimed=>installed&&refs==0=0(应0)
                读引用未释放=0(应0) -> OK
```
逐轮耗时（`ms=`）：n=300，min=0，avg=9635，max=10403 —— **由 10s 安装等待期限主导**
（`pending_materialize_bytes != 0` 整段耗尽，见 D-38：无新写入时安装点不推进）。
本组设计上读**活跃代**（`pin_active=300`）⇒ 不进入 sealed 引用/回收路径，
`acquire_rejected_retiring / reclaim_deferred_by_reader / reclaimed` 为 0 属预期；
回收路径的确定性覆盖见 case5/case8a/case8b 与压力组 2（`reclaimed_gens=28`）。

**运行信息**：`ZF_READ_SHORT_RETRY=0`、`ZF_VC_PARTITION_TARGET_KB=128`、watchdog=30s、
独立进程前台后台各一次；无外部终止（exit=0）。500 轮已启动（`/tmp/g1_500.log`）。

### D-40. 安装停滞定位到**层判定**：目标模式禁 L0 + 规划的 L0 输出 ⇒ 空 edit 空转（可复现、逐代可见）

新增诊断：属性 `rocksdb.zeroflush.pending_gens`（未安装代的 part:gen/字节/归属：epoch / recovery /
skip / 读引用）+ 模式 `zf_valcheck <db> drainprobe <n> [rounds]`（不追加用户写，打印前后状态并调生产
排空入口 `ZfDrainNow()`）。对 D-38 的现场库（`ledger/ba_drain_010931`，关闭后重开）实测：

```
before:        pending=38383920 drained=0 registered=20 installed_epochs=0
               empty_edit=0 l0_install=0 l0_blocked=0 skip=0
   pending_gens n=20 bytes=38383920 [ 15:212(4195104B,NONE+recovery) 15:209(...recovery)
                15:208(...) 15:2(386616B) 0:0(1016736B) 13:0 15:211 1:0 14:0 3:0 4:0 7:0 8:0 ... ]
after_drain_1: pending=38383920 drained=0 empty_edit=1 barrier=1 pend_unchanged=1
               l0_blocked=**2** skip=1      ← 归属从 recovery 变为 **skip**（攒批等待集合）
after_drain_2: pending=38383920 drained=0 empty_edit=2 barrier=2 pend_unchanged=2
               l0_blocked=**4** skip=2
```
**判读**：20 个代（38.4MB）全部 `NONE`（无 epoch 归属）+ recovery/skip；每次排空都走到
"安装输出被判为 L0 ⇒ 目标模式禁装 L0 ⇒ 让位（空 edit）"，并把代推进 **skip 攒批等待集合**，
`pending/drained` 完全不动。即：

> **目标模式"禁止 L0 安装"与规划出的 L0 输出互锁**：规划认为该分区的输出层是 L0
> （无 base 层重叠可沉），而安装点禁止 L0 ⇒ 数据只能反复让位、等一个**永远不会出现的
> L0 重叠**。skip 攒批等待集合的语义是"等 L0 遮蔽解决"，但在禁装 L0 的库里没有解决路径。

这解释了 D-37/D-38 的全部现象（phase1 基线 pending≠0、phase3 无新写入时 drained 恒定、
压力轮每轮白等满 10s、`all` 结束仍 pending=43.1MB），也解释了读一直正确（数据在 WAL，
保留契约生效）。**下一步（引擎侧）**：层判定必须与目标模式一致 —— 规划输出层为 L0 时，
要么按 `merge_into_base_level` 下沉到 base 层，要么让 skip 等待集合有可终结的出口；
在此之前 `B（安全安装）门禁保持红`。

### D-41. 压力组 1 **500 轮通过**（完整组 300→500 全部通过）+ semtest 在当前 build 复验全绿

```
stress group=1 rounds=500 failures=0 silent_stale=0 notfound=0 corruption=0
               rotations=500 propagated=0 fellback=0 pin_ok=0 pin_active=500
               ms_avg=10155 ms_max=10570 -> PASS     (exit=0，无外部终止)
stress gate: index_kept_uninstalled=1870 gen_release_before_install=0
             handoff_at_release=612 gen_lost_before_install=0
stress reclaim: acquire_rejected_retiring=0 reclaim_deferred_by_reader=0
                reclaimed_bytes=0 reclaimed_gens=0 | 三项断言全 0 -> OK
```
逐轮 `ms=`：n≈500，min=0，avg=9667，max=10570（仍由 10s 安装等待期限主导，见 D-40）。

semtest 在含新诊断（`pending_gens` 属性 + `drainprobe`）的同一 build 上复验：
```
case1/2/3 PASS  case5 real-reclaim PASS（purged=2 file_final=0 reclaimed=1068 acct_ok=1）
case8b PASS（retiring=1 new_ref_rejected=1 deferred_by_reader=1）
case8a PASS（db_gate=1 read_status=IO error(...no downgrade) read_is_notfound=0）
case7 fixture PASS（exec[A=1 B=1 B_hit=1 B_lost=1]）  case7 sealed PASS
semtest cases1-7: PASS (fails=0 setup_incomplete=0)   exit=0
```

**后续（顺序启动，互不并发）**：压力组 2/3/4/5 各 300 轮已排队后台执行
（`ZF_READ_SHORT_RETRY=0`、`ZF_VC_PARTITION_TARGET_KB=128`、watchdog=30s），
日志 `/tmp/g{2,3,4,5}_300.log`；外部终止一律记未完成。

### D-42. 压力组 5（sealed-at-selection）暴露"拒绝即报错"过严 ⇒ 已按 fence 分流修复

**组 2/3/4 各 300 轮全部通过**（旧 build）：
```
组2: rounds=300 failures=0 silent_stale=0 notfound=0 corruption=0 rotations=300
     ms_avg=10144  reclaim: reclaimed_gens=132 reclaimed_bytes=1616952（断言全 0 → OK）
组3: rounds=300 failures=0 ... rotations=300 ms_avg=10143  reclaimed_gens=48/580408 → OK
组4: rounds=300 failures=0 ... rotations=300 ms_avg=208（不等安装，读未安装 sealed 代）→ OK
```
**组 5 FAIL（旧 build）**：`rounds=35 failures=6 corruption=6`，6 次失败**同一签名**：
```
status=IO error: ZF: generation retiring; read view refresh could not complete safely
        (no downgrade)   pin_ok=24 pin_active=0 acquire_rejected_retiring=12
reclaimed_gens=78 reclaimed_bytes=7482408
```
**判读（自我纠正）**：这条报错来自我在 case8a 落地的"拒绝即不得降级"规则，但它对
**已安装**的代过严：该代数据已在 LSM（fence 覆盖），回落原生查找取到的是**同一 seq
版本**（fence 不变量），不是降级。把"被拒"一个布尔量同时用于两种语义是错的。

**修复（按 Version fence 分流）**：
- 选择层在被拒时记录**被拒身份** `(part,gen)`（`get_trace::NoteRejectedGen`）；
- 重取视图后仍被拒时：`IsGenInstalled(part,gen)`（**Version fence**，真实安装证据）为真
  ⇒ **返回未命中，回落 LSM**（同 seq 版本已在 LSM）；为假 ⇒ 维持明确 IOError
  （WAL 是唯一副本且正在回收，绝不降级/NotFound）。
- 关键点：判据用 **Version fence** 而不是 cache 内的 `installed` 标记 —— case8a 的
  合成代只标了 cache 级 installed、真实 Version 不覆盖 ⇒ 它仍走明确报错（semtest 复验
  `case8a PASS` 不变），而真实场景（组 5）走 LSM 回落取值。

**build 哈希**：见 `/tmp/build_hash_fall.txt`（本轮修复后的 `zf_valcheck` / `librocksdb.so.11`）。
五组已按**该 build** 重新排队（组 5 → 1 → 2 → 3 → 4，各 300 轮，顺序执行，互不并发），
日志 `/tmp/v2_g{5,1,2,3,4}_300.log`；外部终止记未完成。

### D-43. 组 5 根因与修复：判据要用**发布证据**（不是 fence）；case8a fixture 改为真实可读状态

**证据链**
- 旧 build 组 5：`failures=6 corruption=6`，6 次同一签名
  `IO error: ZF: generation retiring; read view refresh could not complete safely
  (no downgrade)`，且 `pin_ok=0 / pin_active=0`（胜出代在 cache 里已无条目 ⇒ 拿不到句柄）、
  `acquire_rejected_retiring=12`。
- 第一版修复用 **Version fence** 判"已安装 ⇒ 可回落 LSM" ⇒ 组 5 仍 6 次同签名失败
  ⇒ 说明 fence **滞后于发布**（fence 还要求消费证明完整；`MarkGensInstalled` 只要求
  "本次 edit 已发布"）。
- 第二版修复改用**发布证据** `GenInstalledPublished(part,gen)`（与 `MarkGensInstalled`
  同一事件置位）⇒ 组 5 **50 轮 PASS**（`failures=0 rotations=50
  acquire_rejected_retiring=14 pin_ok=38 reclaimed_gens=92/8619828`，断言全 0）。

**规则（写进代码注释）**
- 胜出代 retiring + **已发布安装** ⇒ 该代记录的同 seq 版本已在 LSM ⇒ 回落原生查找
  （取到同一或更新版本，**不是降级**）；
- 胜出代 retiring + **未发布** ⇒ WAL 是唯一副本且正在回收 ⇒ 明确 IOError
  （不降级、不 NotFound）。

**case8a fixture 同步改为真实可读状态**（用户 2026-09-17 原话："重取视图后**返回新值**"）：
先用真实写入把 `k8=new` 写进库、封存并等安装完成（fence 未覆盖则记 INCOMPLETE），
合成索引条目（seq=200 新 / 100 旧）只作为"胜出代已 retiring"的触发器；判据收紧为
**必须 OK 且等于新值**。实测：
```
case8a PASS: sel_returned=0 retiring_rejected=1 db_gate=1 lsm_has_new=1
             read_status=OK read_is_new=1 read_is_old=0 read_is_notfound=0 src=LSM
             （两个 attempt 都命中 retiring 代；rejected=2->4 retry_view=1->3）
```
**build 哈希**：`/tmp/build_hash_v3.txt`（zf_valcheck / librocksdb.so.11）。
五组全量按该 build 重新排队（组 5→1→2→3→4，各 300 轮），日志 `/tmp/v3_g*_300.log`。

### D-44. **五组压力全量通过**（v3 build，各 300 轮，零错误；组 5 覆盖新协议）

build = `82032ce`（发布证据分流 + case8a 真实化）；哈希 `/tmp/build_hash_v3.txt`
（`zf_valcheck b6aa030b…`、`librocksdb.so.11 04e0d049…`）。命令统一为
`ZF_READ_SHORT_RETRY=0 ZF_VC_PARTITION_TARGET_KB=128 zf_valcheck <db> stress <g> 300`，
watchdog=30s 内置，无外部终止（各组 exit=0）。

| 组 | 覆盖 | rounds | failures | silent_stale | notfound | corruption | rotations | ms_avg/ms_max | 回收侧 |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 基础跨 Freeze | 300 | 0 | 0 | 0 | 0 | 300 | 10142/10157 | 0 |
| 2 | 安装后回收 | 300 | 0 | 0 | 0 | 0 | 300 | 10144/10178 | gens=132 / 1.62MB |
| 3 | snapshot/delete 混合 | 300 | 0 | 0 | 0 | 0 | 300 | 10143/10166 | gens=48 / 580KB |
| 4 | 活跃+换代+不安装 | 300 | 0 | 0 | 0 | 0 | 300 | 209/248 | 0 |
| 5 | sealed-at-selection | 300 | 0 | 0 | 0 | 0 | 300 | 10296/10772 | **gens=660 / 62.98MB**，`acquire_rejected_retiring=112`、`pin_ok=219` |

全部组的断言均为 0：`retiring=>no_new_ref`、`reclaimed=>installed&&refs==0`、
读引用未释放（`gen_read_ref_total`）；`gen_release_before_install=0`、
`gen_lost_before_install=0`；保留契约被走到（`index_kept_uninstalled` 1116–1870、
`handoff_at_release` 365–612）。

**判读**：组 5 是本轮修复的**真正验证组**——它让"胜出代已 retiring"反复发生
（112 次拒绝、219 次 sealed 代 pin、660 个代回收），读数全部正确且无一次 IOError/
NotFound/旧值；组 2/3 证明回收在读写混合下也安全完成；组 1/4 覆盖活跃代与未安装代读路径。
**外部终止**：无（未出现任何 exit=124/143 或被 kill 的记录）。

**仍未通过的门禁（与本轮修复无关，根因见 D-40）**：baccept phase3/4（"冲突解除后继续
安装并排空"、"保护非永久保留"）——目标模式禁装 L0 × 规划输出层为 L0 互锁；A/B 已证
修复前后同数失败。整单元撤销、合法路径 A、固定路由仍未开始。

## B step 1（用户 2026-09-17 指令）：把互锁缩成确定性用例 —— 完成，根因钉死

### D-45. 确定性复现 + 根因：**hash 表 + 目标模式**下"覆盖写"没有合法安装路径

**新用例 `zf_valcheck <db> installtest <1|2> [轮数]`**（禁 L0、封存一批、**停止写入**、
只驱动正常物化；不依赖后续写入、不等待不存在的 L0 文件；stdout 改无缓冲以便与引擎诊断
逐行对齐）。两批使用**同一键区间**，场景 2 才是"目标层有重叠"。

```
场景 1（目标层无重叠）: PASS
  installtest scen=1 本批 封存: part=1 gen 0→1 rc=0 pending=13884 drained=0 sst=0 | 停写
  pass=0 pending=0 drained=13884 delta=+13884 empty_edit=0 l0_install=0 l0_blocked=0 | 全 0
场景 2（目标层有重叠）: FAIL
  前置: 安装成功（fences_nonempty=18 sst=1 drained=+13884）
  本批: part=1 gen 1→2 pending=13884 → 停写
  pass=0..N: pending=413316 drained=13884 **delta=+0**（冻结）
             empty_edit +1/次, l0_blocked **+2/次**, skips +1/次, 其余让位原因全 0
  installtest scen=2: FAIL (真实安装=0 pending归零=0 l0_install=0 空edit=7)
```

**根因（逐层证据，含新增 `ZFDBG-mergegate`）**
```
ZFDBG-mergegate epoch=2 merge_enabled=0 merge_opt=1 range_override=0 adopted_orphans=0
   adopted_skips=0 table_null=0 hash_mode=1 forbid_l0=1 |
   epoch_table_ver=0 cur_table_ver=0 cur_hash=1 routing=2(kSampled)
ZFDBG-mm-plan epoch=2 pid=1 dec=0 lo= hi= ov=[]        ← 规划只出普通物化，无重叠记录
ZFDBG-pick vsstorage base=1 lvl=1 new=[00..00,00..C1] hit=file9 [同范围]
                                                       ← 安装点发现 L1 同范围文件
→ 目标模式禁 L0 ⇒ PickInstallLevel 返回 kCannotInstall ⇒ 整批让位（空 edit）
   → 代移交 recovery ⇒ 下一 epoch 收养（adopted_skips=1）⇒ merge 仍被禁（同为 hash 表）
   ⇒ **自持死循环**：pending 永不归零、drained 永不推进、每轮白烧一次空 edit。
```
- `routing_mode=kSampled` 的库在采样边界建立前**一直用 version 0 的 hash 表**
  （`PartitionTable::CreateHash`）——hash 表无 range 语义 ⇒ `merge_enabled` 恒为 0
  （门禁含 `!table_->IsHashMode()`），规划只能出 `kMaterialize`；
- 而**安装点**是 range 语义（`OverlapInLevel`）：同分区上一批自己产出的 L1 文件与之
  同范围 ⇒ 必然重叠 ⇒ 在禁 L0 下无合法出口。
- 结论：这不是"某个 guard 太严"，而是**hash 路由与"禁 L0 + range 安装检查"在覆盖写场景下
  语义不相容**：既不能融合（无 range 语义），也不能替换（无法判定重叠文件是否属于本分区——
  文件元数据里**没有分区归属**），更不能装 L0。

**性能线索（用户提示）已量化：固定等待，不是引擎慢**
```
组1 20 轮: ms_avg=10151  install_wait_avg=10010  install_wait_timeouts=20/20
组4 20 轮: ms_avg=215    install_wait_avg=0      install_wait_timeouts=0
```
⇒ 10.14s/轮中 98.6% 是测试的固定 10s 安装等待；**每轮都等到超时**（= 上面这个
"装不上"在压力轮里 100% 复现）。压力轮现在把 `install_wait_avg/timeouts` 计入汇总，
后续不再用 ms_avg 代表性能。

**让位原因计数修正**：`zf_skip_*` 原为 `materialize_job.cc` 内的 **static**（进程级共享）
⇒ 同进程多库（压力/校验器每轮新开库）会串味，实测日志里的 `dirconf/batchprev` 即由此而来，
**不能作为"本轮为何让位"的证据**。已改为上下文成员并按库隔离，新增属性
`rocksdb.zeroflush.skip_reasons`（逐次可读）。

**修复方向（待用户确认，涉及路由/表语义 = V0.6 固定路由范畴）**
1. **目标模式禁止在 hash 表上封存/安装**：封存前若表仍是 hash 且禁 L0，先建立 range 表
   （固定边界或采样边界），使每个 epoch 都有 range 语义 ⇒ 走正常融合/下沉；
2. 或给文件加**分区归属元数据**，使"输出范围 ⊇ 全部重叠文件且全部属于本分区"时可安全替换
   （M4.5b-2 直装替换的推广）；
3. 不建议：为目标模式在这条路径上放开 L0（那就等于放弃目标模式的核心约束）。

**当前状态**：`B-step1 完成`（确定性用例 + 根因 + 性能线索量化）；`B-step2（修规划）待决策后实施`。
semtest 复验仍全绿（case1/2/3/5/7/8a/8b + `cases1-7: PASS`）。

### D-46. B step 2 进展与决定性证据：**融合/安装路径本身正确**，缺口是"边界必须从一开始就稳定"

**已实施（本提交）**
1. **学习窗口不再绑定 epoch 1**：只要表仍是 version 0（hash）就持续采样、每次封存都尝试学习。
   旧实现只在 epoch 1 封存时学边界、且采样只在 `epoch_counter_==0` 期间 ⇒ 首 epoch 封得早
   或样本少 ⇒ **永久停在 hash 表**（实测 `samples=7 parts=16 bd_ok=0`）。
2. **采样不足时降级分区数**而不是放弃学习：`BuildBoundaries(P)` 在 `samples < P` 时返回
   false；改为从 P 递减找到第一个可用的分区数（仍为 **range 语义**，只是分区数少于配置值，
   与"align 表每 epoch 重建、分区数可变"的既有语义一致）。
   实测：`ZFDBG-learn epoch=3 samples=7 seen=400 bd_ok=1 parts=16 eff_parts=7` ⇒
   `cur_table_ver=1 cur_hash=0`，此后 epoch 4+ `merge_enabled=1`。
3. 新增实验开关 `ZF_VC_STATIC_BOUNDARIES=1`（kStatic，按键空间等分给出 P-1 个固定边界）。

**决定性实验（同一用例、同一 build、只改路由模式）**
```
installtest scen=1/2 @ kStatic（固定 range 边界）:
  scen=1: PASS（真实安装=1 pending归零=1 l0_install=0 空edit=0）
  scen=2: PASS（真实安装=1 pending归零=1 l0_install=0 空edit=0）
    证据: ZFDBG-mm-plan epoch=2 pid=0 dec=1 lo=… hi=…4E2… ov=[9 ]   ← kMergeBase 消费目标层输入
          ZFDBG-install lvl=1 file=11 … part=0 dec=1                 ← 同批替换安装成功
installtest scen=2 @ kSampled（默认）: FAIL（同前：pending 冻结、empty_edit/l0_blocked 递增）
```
⇒ **引擎的融合路径（Compaction 注册 / 输出范围 / DeleteFile+AddFile / 安装时重叠检查）在
稳定 range 边界下完全正确**；B 的"停止写入后能排空"只差**路由语义**。

**学习窗口修复仍不足以解锁已写数据**（实测）：epoch 4+ 已 `merge_enabled=1`，但 hash 期
（epoch ≤3）写下的代**键集按 hash 路由**，在 range 表下 `GetPartRange` 给不出覆盖它们的区间
⇒ 仍只能让位（空 edit）。即"路由版本在库生命周期内改变"本身与"range 语义安装检查"不相容。

**结论与建议（需决策，含存储/协议影响）**
- **建议（已在实验中验证可行）**：把"**边界从创建起就稳定**"提前到目标模式落地——即
  `forbid_l0_install` 的新库直接以 `kStatic`（或等价的确定性固定边界，前缀/等分均可）建表，
  持久化于 ZFPROPS，写/读/恢复共用同一张表（这正是用户列在 V0.6 的"固定路由"；实测它一启用
  B 就打通）。代价：新库的分区数/边界固定，后续调参需迁移（与 V0.6 设计一致）。
- 备选：给**每条记录**（或每个 gen）记录写入时的路由版本，使旧 hash 期数据在其原表语义下安装
  ——协议面更大（读/物化/恢复都要带版本判定），且与"边界稳定"目标重复。
- 不建议：为目标模式在这条路径放开 L0（等于放弃目标模式约束）。

**回归（本 build）**：semtest cases1-7 PASS；新库 `write/scan/triage/all` 全零
（`scan mismatch=0`、`B2=0`、`all: mismatch/miss/errors=0 VALCHECK PASS`）；
`all` 结束仍 `pending=44.8MB gens=32 drained=0`（= 上面这条未修完的"装不上"）。

## B step 2b 检查点交付：固定 range 路由持久化 + 多分区 installtest + 重启一致性

### D-47. 固定路由协议（创建 / 重开 / 负例）

**协议语义（`RoutingMode::kStatic` = 固定 range 路由）**
- 新库**从第一条写入起**就是固定 range 表：边界为**显式配置**（`static_boundaries`），
  按**实际比较器**校验（严格升序、非空、个数 = P-1）；**没有配置边界时只允许 P=1 的
  单一全范围分区**（不做"通用等分"假设）。
- epoch 更替**不学习、不替换**边界（学习分支只对 kSampled 开放）。
- **持久化（ZFPROPS v2）**：协议版本(format_version=2)、比较器名、分区数、**边界**、
  physical part_ids、current_version、CRC32C；原子写（tmp→rename）+ SyncDir。
- 重开：加载同一份配置；**配置冲突 / 元数据损坏 / 协议不支持一律明确报错**。

**本提交修的关键缺陷**：`PersistZfProps()` 把 version 0 **硬当 hash 表**编码
（`boundaries.clear()` + `part_ids=[0,P)`）⇒ 固定路由的边界根本没落盘，重开被守卫
正确拒绝（`fixed-routing boundaries conflict: persisted 0 boundaries, configured 3`）。
现改为从**实际表**取分区数/part_ids/边界。另外重开时"表无法按持久化边界重建"原本
只记日志继续（= 悄悄换一张表），固定路由下改为 **Corruption 硬报错**。

**负例（新模式 `routingneg`，三项全 PASS）**
```
A 配置冲突（不同边界重开固定库）      -> Invalid argument: fixed-routing boundaries conflict …
C 协议不支持（hash 库 + 固定路由配置） -> Invalid argument: ZFPROPS partitions mismatch …
B 元数据损坏（ZFPROPS 写坏且 zfwal 非空）-> Corruption: ZFPROPS corrupt but zfwal non-empty
routingneg: PASS (fails=0/3)
```

### D-48. 多分区安装验收 + 重启一致性（`installtest 1..4`，四个场景全 PASS）

每个场景都执行：**写入 → 显式封存 → 停止写入 → 有限次正常物化 → 重启**；判据为
`pending 归零` **且 registry 无未安装代**（`pending_gens: n=0`）且 `reclaim_pending` 空，
再要求**路由身份重启前后一致**（`ver/parts/hash/boundaries/cur_ver/mode`）与**逐键正确**。

| 场景 | 覆盖 | 涉及分区 | 结果 |
|---|---|---|---|
| 1 | 单分区、目标层无重叠 | [0] | PASS |
| 2 | 单分区、目标层有重叠（覆盖写） | [0] | PASS |
| 3 | **跨边界多分区**、无重叠（含边界键 5000/9999/10000/14999） | [0 1 2] | PASS |
| 4 | 跨边界多分区、有重叠（覆盖写 + 删除 + 快照读） | [0 1 2] | PASS |

有重叠场景的**实际证据**（不只看最终层号）：
```
scen=2 第二遍: merge_input_files=1 add_file=2 delete_replaced=1 base_merge_count=1
               rewritten_bytes=106742 | 合并输入日志=part=0/file=11:[9 ]
scen=4 第二遍: merge_input_files=3 add_file=6 delete_replaced=3 base_merge_count=3
               rewritten_bytes=176857 | 日志=part=0/file=14:[11 ] part=1/file=16:[10 ]
                                            part=2/file=17:[12 ]
```
（`file=新增输出 : [被消费/替换的旧文件号]`；scen=4 还验证了"覆盖前快照读到旧值"。）

新增诊断面：`routing_table`（路由身份）、`install_evidence`/
`merge_input_files`/`install_delete_replaced`、`merge_inputs`（实际输入文件号）、
`pending_gens`（未安装代清单）。固定路由实验/用例开关：`ZF_VC_FIXED_ROUTING`
（installtest 自动启用，4 分区、边界 5000/10000/15000）。

**回归（本 build）**：semtest `cases1-7 PASS`；新库 `write/scan/triage/all` 全零
（`scan mismatch=0`、`B2=0`、`all VALCHECK PASS`）。
**未做（按用户顺序，属下一步）**：整单元撤销与提交失败注入（step 3）、同一 build 收口
（step 4：完整 baccept + 五组压力 + 停止写入排水/重启校验；学习窗口改动后旧绿灯只作历史证据）。

### D-49. 固定路由作为目标模式默认：**安装/排空/回收全部打通**（五组 300 轮双通过）

**变更**：校验器的目标模式（禁 L0）新库**默认固定 range 路由**（`ZF_VC_FIXED_ROUTING`，
4 分区、边界 5000/10000/15000）；历史协议由 `ZF_VC_LEGACY_ROUTING=1` 显式回退。
压力汇总按用户口径同时报告**读正确性 + 安装进展**：要求安装的组（1/2/3/5）**等待超时即判失败**，
组 4 明确不要求安装。

**五组全量（各 300 轮，固定路由）**
| 组 | failures/silent_stale/notfound/corruption | rotations | ms_avg/max | install_wait avg/timeouts | 回收 |
|---|---|---|---|---|---|
| 1 | 0/0/0/0 | 300 | 103/119 | 8ms / **0** | 300 代 / 39.4MB |
| 2 | 0/0/0/0 | 300 | 123/140 | 8ms / **0** | 900 代 / 118MB |
| 3 | 0/0/0/0 | 300 | 110/137 | 8ms / **0** | 500 代 / 65.7MB |
| 4 | 0/0/0/0 | 300 | 162/179 | 0ms / 0 | 1500 代 / 197MB |
| 5 | 0/0/0/0 | 300 | 156/172 | 17ms / **0** | 1500 代 / 197MB |
全部 `-> PASS`；断言（retiring=>no_new_ref / reclaimed=>installed&&refs==0 / 读引用未释放）全 0。
对比旧配置：`ms_avg=10151`、`install_wait_timeouts=20/20`、`reclaimed=0`。

**新库门禁两种路由对比（同一 build、同一命令）**
```
固定路由: write pending=10.3MB drained=16.0MB | scan mismatch=0 | triage B2=0
          all: wal_read_delta=0 sealed_miss_delta=0 pending=**0.0MB** drained=34.3MB
               reclaimed=26.2MB gens=2  VALCHECK PASS
历史协议: write pending=14.8MB drained=4.2MB | scan mismatch=0 | triage B2=0
          all: wal_read_delta=23016 sealed_miss_delta=20 pending=**46.1MB**
               drained=0.0MB reclaimed=0.0MB gens=33  VALCHECK PASS
```
⇒ 固定路由把"停止写入后能排空"与"空间最终回收"两条**直接做通**（pending→0、回收 26.2MB）；
历史协议保持原行为（读正确，但 pending 滞留 46MB、零回收）。

**覆盖度提醒（如实记录）**：固定路由下组 4/5 的 `pin_ok=0 / pin_active=0`——数据装得太快，
这两组**不再走到 sealed 代 pin / retiring 拒绝**路径（旧配置是 219 次 pin、112 次拒绝）。
该路径的覆盖现由 semtest case5/case8a/case8b 承担；后续若要压力级覆盖，需要为固定路由
单独调参（例如扩大 epoch 目标或注入让位）再补一组。

## B step 3–4 收口：失败语义验收 + 同一 build 全面验收（**全绿**）

build 哈希：`/tmp/build_hash_final.txt`（`zf_valcheck 2efa31bf…`、`librocksdb.so.11 60208545…`）。

### D-50. 失败语义验收（新模式 `failaccept A|B|C`，三场景全 PASS）

三个确定场景，每个都做：写入 → 显式封存 → 注入 → 驱动安装 → **在注入仍生效时**读判据
→ 重启 → 解除注入后安装/排空/回收。

| 场景 | 注入 | 结果 |
|---|---|---|
| A 整单元撤销 | `ZF_TEST_DROP_OUTPUT`（装配 edit/fence **之前**整体撤销） | PASS |
| B 提交**前**失败 | `ZF_TEST_LNA_FAIL_PRE`（共享提交路径、manifest 未写） | PASS |
| C 提交**后**失败 | `ZF_TEST_LNA_FAIL_POST_SYNC`（manifest 已 sync、内存未应用） | PASS |

```
A: 无AddFile=1 无Delete=1 无fence=1 pending保持=1 drained不推进=1 可读=1
   重启后逐键 bad=0/62 | 解除后 pending=0 无未安装代=1 reclaimed=65148
B: 同合规；重启后 **edit 未被承认**（fences=(none)、pending=66216、gens=NONE+recovery）
   | 解除后 pending=0 无未安装代=1 reclaimed=132432
C: 同合规 + install_uncertain>0；重启后 **承认该 edit**：
   fences=p1=[0,0]b1068r1 p0=[0,0]b65148r61、pending=0、registered=0、逐键 62/62 正确
   | 解除后 pending=0 无未安装代=1 reclaimed=66216
```
新增：`ZF_TEST_LNA_FAIL_PRE` / `ZF_TEST_LNA_FAIL_POST_SYNC`（**env 存在期间持续失败**——
一次性注入会被自动重试覆盖，实测第二次尝试即成功而测不到失败路径）；
`install_uncertain`（提交结果不确定计数）+ `install_reconciled`；恢复期**按 Version fence
对账**（`ForEachRegisteredGen` + `MarkGensInstalled`）——C 的重启承认正是靠它/恢复本身完成。
判据用**持久证据**（Version 里的 `installed_fences`）而不是 `fence_advance_count`
（实测后者在注入窗口内为 +2 而 Version 里没有任何 fence，不能当判据）。

### D-51. 同一 build 的完整验收（全部通过）

| 项目 | 结果 |
|---|---|
| semtest（case5 / case7 / case8a / case8b / 1-3） | **PASS**（`cases1-7: PASS`，`fails=0`） |
| baccept（phase1–4） | **PASS**（phase1 pending=0；phase2 让位 5 批；phase3 pending=0 drained=42720000；phase4 `registered_gens=0 reclaimed=42720000`） |
| all / scan / triage | **PASS**（`scan mismatch=0`、`B2=0`、`all: mismatch/miss/errors=0`、`pending=0.0MB drained=34.2MB reclaimed=26.2MB`） |
| installtest 1–4（停止写入后排空 + 重启一致性） | **PASS**（四场景含融合证据） |
| routingneg（配置冲突/协议不支持/元数据损坏） | **PASS**（3/3） |
| failaccept A/B/C | **PASS** |
| 五组压力（各 300 轮） | **PASS** |

五组压力汇总（按用户要求同时报告读错误/安装等待超时/pending/reclaimed/registry 残留）：
```
组1: failures=0 silent_stale=0 notfound=0 corruption=0 rotations=300 ms_avg=103
     install_wait_timeouts=0 pending_last=0 pending_max=0 registry_last=2 PASS
     reclaimed_bytes=39409200 reclaimed_gens=300
组2: 同上 ms_avg=123 timeouts=0 pending=0 registry_last=2 reclaimed=118227600 gens=900
组3: 同上 ms_avg=110 timeouts=0 pending=0 registry_last=2 reclaimed=65686400 gens=500
组4: 同上 ms_avg=162 timeouts=0 pending=0 registry_last=0 reclaimed=197046000 gens=1500（组4 不要求安装）
组5: 同上 ms_avg=155 timeouts=0 pending=0 registry_last=1 reclaimed=197046000 gens=1500
断言（retiring=>no_new_ref / reclaimed=>installed&&refs==0 / 读引用未释放）全 0。
```
**覆盖度补记（用户指令 4）**：固定路由下组 4/5 的 `pin_ok/pin_active` 为 0——不再自然触发
sealed 代 pin / retiring 路径；该路径的确定性覆盖由 **semtest case5/case7(sealed)/case8a/8b**
承担（本 build 全 PASS）。若后续压力门禁要求覆盖该路径，需另加**受控让位场景**（不改生产逻辑）。
另外组 1–3 每轮结束时 `registry_last=2`（当轮活跃/刚封存代），非残留泄漏（`pending_last=0`）。

**状态**：`B（安全安装与失败语义）在本 build 上完整标绿`：安装/排空/回收、跨分区与重启一致性、
整单元撤销、提交前失败、提交后故障恢复、协议负例、五组压力全部通过。
下一步（按用户顺序）：**合法路径 A 的自然可达性判断**；其后是固定路由的性能与迁移细节。

## A 自然可达性判断（进行中）：无 A 命中，但**发现另一类读失败（snapshot 交互）**

### D-52. `aprobe` / `mini` 两个新用例 + 二分结果

**新增**
- `aprobe <n> <iters>`：**合法操作**（普通写/覆盖/删除墓碑/snapshot/封存+安装+回收交接/重启）
  逐次 Get 记录 `read_sequence / index_sequence / truth_sequence / source / candidate_count /
  fence`；A 命中判据 = 索引胜出（`source=INDEX`）且 `index_seq < 真值 seq` 且需要 LSM 才正确。
  配套新增：索引选择的 TLS 记录（`LastIndexSelTls`）、**线程本地**的跳过索引开关
  （`get_trace::SetSkipIndex`，用于取"LSM-only"对照；早前用 env 开关会与并发线程
  getenv 竞争 ⇒ 未定义行为/偶发假失败）、首个错误处的**重启审计**（判定持久丢失 vs 内存状态）。
- `mini <batches>`：干净最小序列（写一批 → 显式封存 → 排空 → 逐键复查）——**全 PASS**
  （6 批 120 键、6 次封存、pending 归零、逐键全对）⇒ 写/封存/安装路径本身无问题。

**A 判定（当前证据）**
```
base（全开）        : gets=1520 errors=410  A_hits=0 陈旧索引胜出(非快照)=0
no_delete           : gets=1220 errors=846  A_hits=0 陈旧索引胜出=2
no_snapshot         : gets= 920 errors=  **0**  A_hits=0 陈旧索引胜出=0   ← PASS
no_delete+no_snapshot: gets= 620 errors= **0**  A_hits=0 陈旧索引胜出=5
```
⇒ **没有出现"A 命中导致结果错误"**；关闭 snapshot 步骤后整套混合负载**零读错误**。
但**暂不宣布 `N/A/UNREACHABLE_UNDER_VALID_PROTOCOL`**：① `no_delete+no_snapshot` 仍有 5 次
"索引 seq 低于真值"（值恰好正确，需单独最小化）；② 下面这条失败必须先解释掉。

**新发现（优先级高于 A）：snapshot 交互会导致随后大量读失败**
- 触发步骤：取快照 → 写入 → 用快照读（期望旧值）→ 实时读 → 释放快照。
- 现象：`base/no_delete` 配置下 `errors=410/846`（`NotFound` 与值不符都有），
  起点固定在**第 2–3 次封存附近**（首个失败 read_seq≈206）；至少一次实例中**重启后该键
  仍缺失**（`重启审计: checked=42 仍缺失=1 → 持久丢失`）。
- 关闭 snapshot 步骤后同一负载 **0 错误** ⇒ 触发点锁定在 snapshot 路径，而非封存/安装/purge
  （`ZF_AP_NO_PURGE`、`ZF_AP_NO_LSM_PROBE` 均不能消除）。
- 下一步（按用户方法）：把 snapshot 组合缩成**单键最小复现**（写 K → 快照 → 覆盖 → 快照读 →
  实时读 → 释放 → 封存+排空 → 复查），定位是快照读路径、快照释放路径还是
  "快照 + 写 + ZF 索引换代"的交互；在此之前不修读路径、不宣布 A 的最终结论。

**状态**：A 步**未完成**（无命中但需先解释上述失败）；B 仍以 `e86a9fd` 为候选基线，未改动。

## snapshot 序列读失败：**已定根因并修复**（A 判定完成）

### D-53. 根因不是 snapshot，而是"索引 active 代落后于 WAL active 代 ⇒ 插入被静默丢弃"

`snapmini`（用户指定的唯一最小用例：Put→Get→快照→Put→快照读→实时读→释放→Freeze+DrainNow→
Get→重启→Get，逐步记录 read_sequence/source/index_sequence/truth/candidate_count/fence/
WAL·LSM 存在性/registry 状态）**一轮定位**：

```
第 0 轮（首次封存前）     : 全部步骤 ok（src=INDEX / LSM）
第 1 轮起                : Put 之后立即 Get = LSM_NOTFOUND，cands=0（候选链为空）
   registry: part=3 index_active_gen=2 wal_active_gen=4 insert_dropped=1（每次失败 +1）
```
→ `PartitionIndexSet::Insert` 在"active 段代 ≠ 请求代、frozen 链里也没有该代"时走
`insert_dropped_` **静默丢弃**；其注释假设"该代已物化释放、数据在 SST"，而对**新的当前代**
这个假设不成立（记录刚进 WAL、不在任何 SST）⇒ 刚写入的键读不到，直到下一次物化才可见。
触发条件是 **WAL 换代（封存/SealEpochAndSwitch）后索引侧 active 段停在旧代**——部分封存路径
有意跳过 `index_set_->Freeze`，于是写入直接撞上这个分支。**snapshot 只是改变了写入模式把它
暴露出来**（我上一轮把它归因成 snapshot 交互，此处更正）。

**修复 1（生产路径，最小改动）**：新增 `PartitionIndexSet::EnsureActiveForGen()`——当请求代
**更新**时按 `Freeze` 的既有语义换代（旧段 `SetFrozen()` 后入 frozen 链**保持可读**，为请求代
建新 active 段）；只有请求代**更旧**（确已物化释放）才维持丢弃。新增计数
`index_resegmented`/`insert_dropped` 可观测。

**修复 2（读路径的"陈旧条目"放行，删掉最后一类拒绝）**：`GetFromPartitionIndex` 在
"retiring 拒绝"后的分流原本只认"发布证据"；probe 残余 3–6 次拒绝的实测数值是
`published=0 fence=0 registry=absent`（LSM 里却已是最新值）⇒ 这类条目**既没被登记为待安装、
也没有安装证据**，拒绝它保护不了任何东西。现判据 = 发布证据 ∨ fence 覆盖 ∨ (**陈旧条目**：
不在 registry 且 fence 不覆盖) ⇒ 回落 LSM。case8a 的语义不受影响（其合成代 registry 内有条目、
走"发布证据"分支）——semtest `case8a PASS` 复验通过。

### D-54. 修复后完整验收（build 哈希 `/tmp/build_hash_snapfix.txt`）

| 项目 | 结果 |
|---|---|
| `snapmini` 全序列 + 四组开关 + 对照组（共 6 配置） | **全 PASS**（每配置 80–120 项检查、重启审计 0 bad） |
| `aprobe` 合法路径 300 轮 ×3 | **errors=0 / A_hits=0 / 陈旧索引胜出=0 / 持久丢失=0**（共 4560 次 Get） |
| semtest（case5/7/8a/8b + 1-3） | **PASS**（`cases1-7: PASS`，case6 标注更新为 `N/A/UNREACHABLE_UNDER_VALID_PROTOCOL`） |
| baccept phase1–4 | **PASS**（phase3 pending=0 drained=42720000；phase4 `registered_gens=0 reclaimed=42720000`） |
| all/scan/triage | **PASS**（`scan mismatch=0`、`B2=0`、`all pending=0.0MB reclaimed=26.2MB`） |
| 五组压力 各 300 轮 | **PASS**（`install_wait_timeouts=0`、`pending_last=0`、回收 39.4/118/65.7/197/197MB、断言全 0） |

**A 判定（用户判据）**：`A_hits = 0` 且所有合法路径正确 ⇒
**`A: UNREACHABLE_UNDER_VALID_PROTOCOL`**（合法路径 4560 次 Get 零错误、零陈旧索引胜出、
零持久丢失）；case6 同步标注。未对每次 Get 增加 LSM 查询（LSM-only 对照只在测试用例内做，
生产路径开关是线程本地的一次性判断）。

## 固定路由的性能与迁移收口（进行中）：迁移矩阵全绿，发现两个新阻断项

### D-55. 迁移矩阵（新模式 `migrate`，8/8 PASS）

| 用例 | 结果 |
|---|---|
| A 新库固定 range 路由：写→封存→安装→重启 | **PASS**（open ✓、路由身份前后**完全一致**、pending=0、逐键 bad=0） |
| B 历史 hash 库重开 | **PASS**（旧协议继续可用、逐键正确、身份保持 `hash=1`；**用固定路由配置打开被明确拒绝**：`ZFPROPS partitions mismatch`） |
| C 比较器名冲突（改 ZFPROPS+重算 CRC） | **PASS** ⇒ `InvalidArgument: ZFPROPS comparator mismatch` |
| D 边界冲突 | **PASS** ⇒ `InvalidArgument: fixed-routing boundaries conflict` |
| E 分区数冲突 | **PASS** ⇒ `Corruption: ZFPROPS corrupt but zfwal non-empty` |
| F part_ids 损坏 | **PASS** ⇒ Corruption（同上） |
| G 协议版本不支持（format_version=99） | **PASS** ⇒ Corruption |
| H CRC 损坏 | **PASS** ⇒ Corruption |
`migrate: PASS (fails=0)`；六类异常**全部明确报错**，无重建、无静默切换路由。

### D-56. 多分区边界回归（installtest 3/4 + 新指标）

```
scen=3/4: PASS（第一遍/第二遍/重启前逐键/路由一致/重启后逐键/融合证据 全 1）
终点: pending=0  registered=0  pending_gens=n=0  旧代 WAL 残留=0（磁盘证据，跨重启稳健）
备注: 额外候选分区查询=328（**已知项**：读路径收敛待做）
```
- 覆盖：边界键（5000/10000/15000 及其 ±1）、跨边界多分区、跨分区覆盖写、删除、snapshot、重启。
- `all/scan/triage`（同 build、新库）：`scan mismatch=0`、`B2=0`、`all pending=0.0MB drained=34.3MB reclaimed=26.2MB`、`wal_read_delta=0`。
- **"额外候选分区查询=0"未达成**：实测 328。原因明确——固定边界下路由分区唯一，
  但读路径仍做"全部有链分区"的候选并集（设计上属"读路径收敛"项）。我尝试实现后
  **主动退回**：该改动会改变读路径语义（非路由分区不再被查），semtest case1/2/3/7/8a 的
  跨分区 fixture 依赖并集，且迁移库可能仍有跨分区历史记录。按用户"不再改读写语义"指令，
  该项**列为后续性能项**，指标如实报告（计数器 `union_skipped` 已就位，开启时可直接观测）。

### D-57. 性能对照（固定 range vs 历史 hash，200k×1KB，8 分区，4 线程，`--zf_base_merge=1`）

| 指标 | 固定 range | 历史 hash |
|---|---|---|
| fillrandom | **88 151 ops/s**（45.0 µs/op，87.4 MB/s） | 30 262 ops/s（130.8 µs/op，30.0 MB/s） |
| readrandom | **258 397 ops/s**（15.4 µs/op，251.6 MB/s） | 12 866 ops/s（306.9 µs/op，12.5 MB/s） |
| 写 P50/P75/P99/P99.9 | 16.1 / 21.9 / 50.8 / 352.9 µs | 13.4 / 18.9 / 45.7 / 111.2 µs |
| 读 P50/P75/P99/P99.9 | **13.1 / 19.1 / 43.0 / 74.0 µs** | 313.2 / 347.1 / 379.5 / 564.4 µs |
| wall clock | 13.8 s | 91.2 s |
| 安装/排空/回收 | `reclaimed=789MB`、`installed_epochs=75`、`empty_edit=0`（另一次 200k 运行） | `pending=839MB`、`reclaimed=0`、`installed_epochs=1`、`empty_edit=51` |

⇒ 固定路由在读吞吐/延迟与排空回收上大幅领先（读 ~20×、写 ~2.9×）；历史 hash 仍受既有
"装不上/不回收"问题影响（D-45/D-46）。
**新增两点（阻断候选标记）**：
1. **物化写放大**：固定路由 200k×1KB（200MB）run 的 `prof_output_bytes=4.41GB`
   （≈22×），`prof_merge_us=27.8s` ⇒ 反复 base 融合重写是下一优化目标。
2. **关闭时罕见段错误**：`db_bench`（固定路由、`--zf_base_merge=1`）约 **1/7 次**在退出时
   SIGSEGV。插桩定位：`ZFTD close-flush` 的排空返回
   `Corruption: ZF sealed WAL scan failed: IO error: ZeroFlush: scanner file not open`
   ⇒ **物化扫描与回收竞态**（物化扫描密封段时未持 generation 读引用，而读路径有）。
   已挂后台复现循环（抓到即停，日志 `/tmp/crash_repro_iter*.log`）。

### 当前门禁状态（本 build，哈希 `/tmp/build_hash_v062.txt`）
```
snapmini（base/对照/sw4）      : PASS
aprobe（合法路径 300 轮）      : PASS —— errors=0 A_hits=0 陈旧索引胜出=0
semtest cases1-7               : PASS（并集回退后恢复；case6=N/A/UNREACHABLE_UNDER_VALID_PROTOCOL）
baccept phase1-4               : PASS（phase3 pending=0 drained=42720000；phase4 registered=0 reclaimed=42720000）
installtest 1-4                : PASS（终点 pending=0/registry=0/旧代 WAL=0）
migrate（迁移矩阵）             : PASS 8/8
all/scan/triage                : PASS（pending=0.0MB reclaimed=26.2MB wal_read_delta=0）
五组压力 各 300 轮              : PASS（install_wait_timeouts=0、pending 0、断言全 0）
```
**结论**：**不标记 V0.6 候选**——两个新阻断项（关闭时罕见 SIGSEGV、物化写放大 22×）与
一个已知性能项（额外候选分区收敛）必须先关闭；核心正确性其余项目全绿。

## V0.6 收口：P0 竞态已修（1000 轮关闭压力零崩溃）；P1/P2 有度量与方案但未达标的

### D-58. P0：关闭阶段崩溃（物化扫描 vs 回收竞态）——**已修复并验收**

根因（上一轮插桩定位）：关闭排空返回 `Corruption: ZF sealed WAL scan failed: scanner
file not open`，随后 SIGSEGV。机制：`ZfMaterializeJob::MaterializePartition` / 融合路径
**开 WalScanner 扫描密封段时不持 generation 读引用**，回收路径（`ReleaseGens`→`PurgePending`）
在 refs==0 时把文件 unlink ⇒ 扫描拿到已删除文件 ⇒ IOError → Corruption → 关闭路径崩溃。

修复（两处，均在扫描点）：
1. 扫描前 `TryAcquireGenReadRef(p,gen)` 并持有到扫描结束（新增 RAII
   `zeroflush::SealedCacheReadRefGuard`）⇒ 回收路径按既有 deferral 规则**延迟 unlink**；
2. 若该代**已被回收**（未登记 / 引用被拒）：其记录必已在 LSM（回收规则要求 installed
   ⇒ refs==0），物化无需再读 ⇒ **跳过并计数** `scan_skipped_reclaimed`，不再报 Corruption。
   新增计数：`scan_ref_held`（生效的扫描保护次数）、`scan_skipped_reclaimed`。

**验收**
```
closestress（新模式：开库→写 2k 键→封存 4 分区→带 pending 关闭）×1000 轮 : **零崩溃**
db_bench 定长循环（原复现器：固定路由、base_merge=1、200k×1KB）           : 无 SIGSEGV
  （循环中途两次出现 exit=127 = 我在循环运行时重建 db_bench 导致的"文件不存在"，
    非崩溃；纯净一轮循环已无 139）
```

### D-59. P1：物化写放大 —— **测量完成，但未找到杠杆**（默认行为保持原样）

度量（新增 `ZF_DBG_MERGE=1` 打印每次融合的输入/输出规模与重写比 + 范围）：
```
固定路由 200k×1KB（200MB 输入）：prof_scan_bytes≈760MB、prof_output_bytes≈**5.1GB**（≈25×）、
prof_tasks≈266、installed_epochs≈70、reclaimed≈795MB、pending 收尾≈53MB
```
尝试的参数（均**未**改善，且降低写吞吐）：
| 配置 | 写 ops/s | prof_output_bytes |
|---|---|---|
| 默认（min_ratio 0.25） | 93 459 | 5.09 GB |
| min_ratio=1.0 | 51 264 | 5.21 GB |
| min_ratio=2.0 | 18 104 | 6.13 GB |
| 收益攒批（ratio 0.25 + cap 8） | 102 108 | 5.18 GB |
| 收益攒批（cap 16） | 104 174 | 5.11 GB |

⇒ 放大**几乎与这两个 knob 无关**，说明"每 epoch 每活跃分区都走融合"这一分流点不是我改的
分支。**结论**：收益攒批（`base_merge_skip_max_generations`）默认**关闭**（保留 knob 与
度量；`ZF_DBG_MERGE=1` 可直接看到每次决策的 ratio/范围），P1 需在下一轮**专门追踪**
`plan.decision == kMergeBase` 的赋值点与 ratio 的实际取值（怀疑 `sealed_in`
（`se_.part_bytes[pid]`，含历史累计）使 ratio 恒达标）。

### D-60. P2：历史并集收敛 —— 实现了两段式，但破坏 case7 ⇒ **已回退**

实现"路由∪别名优先，缺候选才查历史并集"后，semtest case7（显式候选列表 + 跨分区
fixture）选错胜出者（得 `part=3/seq=100`，期望 `part=2/seq=200`）⇒ 该改动的影响超出
预期（候选顺序/剪枝与显式候选路径的交互）。按"性能项不得破坏既有语义与覆盖"原则回退为
单一并集实现；`union_fallback_used` 计数器保留，供后续以更小步长重做。
额外候选分区查询实测 **328**（未达成 0，属已知性能项）。

### D-61. 门禁（本 build，哈希 `/tmp/build_hash_p0fix.txt`）
```
semtest cases1-7            : PASS
aprobe（合法路径 300 轮）    : PASS —— errors=0 A_hits=0 陈旧索引胜出=0
baccept phase1-4            : PASS（phase3 pending=0 drained=42720000；phase4 registered=0 reclaimed=42720000）
installtest 1-4             : PASS
migrate（迁移矩阵）          : PASS 8/8
all/scan/triage             : PASS（pending=0.0MB reclaimed=26.2MB wal_read_delta=0）
五组压力 各 300 轮           : PASS（failures/silent_stale/notfound/corruption=0，install_wait_timeouts=0）
closestress 1000 轮          : PASS（零崩溃）
```
**结论**：P0（关闭崩溃）已关闭 ⇒ **可进入受控功能测试**；但 **V0.6 候选仍不标记**：
P1（写放大 ~25×，缺杠杆）与 P2（额外候选分区 328）未达标，另需把"目标模式下
`--zf_base_merge=1`"变成安全默认（或在启动时明确拒绝不安全配置）——这条本轮**未做**，
列为下一步第一项。

## V0.6 收口（三小提交）：C1 不安全默认已消除；C2 定位 P1 根因并验证收益；P2 暂缓

### D-62. C1：消除不安全默认配置（目标模式必须融合）

- `ZeroFlushOptions::merge_into_base_level` 增加**三态**：`merge_into_base_level_explicit`
  （宿主传 flag 时置位；db_bench 用 gflags `is_default` 判定）。
- `Open`（`zfo` 改为**按值**接收，规范化不回写调用方）：
  **目标模式（forbid_l0_install）+ 固定 range 协议**下：未显式指定 ⇒ **默认开启**；
  显式关闭 ⇒ **`InvalidArgument`**（"disabling merge in target mode makes drain/reclaim
  impossible…"）；**历史 hash/采样协议不受影响**（保持旧协议与旧行为）。
- 验收：
  ```
  负例 ZF_VC_MERGE_OFF=1 + 固定路由 ⇒ open failed: Invalid argument: … requires
       merge_into_base_level=true  ✓（不再出现"启动成功但停止写入后永远排不空"）
  正例（未指定）⇒ 融合默认开启：installtest scen=2 第二遍 融合输入=1/替换删除=1 ✓
  历史 hash 库 + 显式 --zf_base_merge=0 ⇒ 仍可正常跑（fillrandom 211k ops/s）✓
  ```

### D-63. C2：P1 根因定位（**不是累计值问题**）与收益验证

新增逐计划决策日志（`ZF_DBG_MERGE=1`，字段：part / consumed_gens / new_input /
se_part_bytes / base_input / ratio / decision / merge_enabled / forbid_l0 / range）。

**定位**：`ratio` 门槛只存在于 **L0 融合分支**（该分支在本 workload 从不被走到，插桩 0 命中）；
真正产出融合的 `kMergeBase` 赋值点**根本没有 ratio 判据** ⇒ 实测
`part=7 new_input=104664 base_input=25923226 ratio=0.0040 decision=merge`
（为 104KB 新数据重写 25MB base）⇒ 写放大 ~24×。
**并且**：`se_part_bytes == new_input`（二者相等）⇒ 用户怀疑的"累计值被当作收益"**不成立**。

在该点加入收益阈值（目标模式 + 攒批允许 ⇒ 收益不足改攒批，带上限与收敛条件）后的实测：
```
写放大: prof_output_bytes 4.5–5.1GB → **355MB**（≈24×→1.8×）
写吞吐: 93–108k → **176k ops/s**（175 MB/s）
但读吞吐: 102–136k → **35k ops/s**；收尾 pending=718MB 未排空
```
**结论（未启用）**：该策略把安装推迟到"下一次封存"；写入停止后没有新封存 ⇒ 积压只能靠
收尾排空（关闭期 guard 有界）⇒ 读阶段退化、pending 不归零。启用前必须先做**积压驱动排空**
（按分区未安装字节触发物化批次，而非只在封存时）。故 `base_merge_skip_max_generations`
默认 0 = **关闭**（代码/knob/诊断保留，收敛条件已就位；`UninstalledBytesOf(part)` 已加）。

### D-64. P2：暂缓（按用户指示谨慎重做）

本轮**未改**并集；仍为单一并集实现（`union_fallback_used` 计数器就位，供约束版重做时观测）。
约束条件已记录：仅"新固定路由 + 当前协议版本 + 无历史别名 + **无显式候选列表**"才允许
路由优先；历史库/旧表版本/显式 fixture 继续走完整并集；路由与兼容候选必须交给同一全局
sequence 选择器。

### D-65. 最终门禁（本 build，哈希 `/tmp/build_hash_c1c2.txt`）
```
closestress 1000 轮          : **零崩溃**
semtest cases1-7            : PASS
snapmini / aprobe(300 轮)    : PASS（errors=0 A_hits=0 陈旧索引胜出=0）
baccept phase1-4            : PASS（phase3 pending=0 drained=42720000；phase4 registered=0 reclaimed=42720000）
installtest 1-4             : PASS
migrate（迁移矩阵）          : PASS 8/8
all/scan/triage             : PASS（pending=0.0MB reclaimed=26.2MB wal_read_delta=0）
五组压力 各 300 轮           : PASS（failures/silent_stale/notfound/corruption=0，timeouts=0，pending_last=0）
db_bench 退出循环（含 histogram/statistics）20 次 : **crash=0**
```
**结论**：C1 达成（错误配置不再静默停滞）、P0 关闭（关闭无崩溃）、P2 未动语义；
**P1 已量化并验证收益（24×→1.8×）但需"积压驱动排空"配套才能启用** ⇒ V0.6 候选仍不标记，
下一步第一项即该配套（它是让 P1 收益落地的唯一前置）。

## V0.6 候选版本标记（本 build）

### D-66. P1 积压驱动排空：机制就位、**安全触发路径未完成** ⇒ 保持安全默认

- 新增（代码保留、默认不启用）：
  - `NeedsBacklogDrain()`（扫描每分区 `UninstalledBytesOf(part)`，阈值 = `base_merge_skip_max_bytes`；
    年龄上限 `backlog_drain_max_age_micros` 默认 30s）、`TouchBacklogAge`、积压请求/执行计数、
    `install_progress_failures`（关闭排空"连续无进展"计数，**不伪造 installed / 空 edit / 推进 fence**）；
  - 收益攒批门禁纳入 `backlog_drain_active()`（"无新写入时 drain 模式强制执行"）。
- **未完成**：把积压探测接到**安全触发路径**。我曾在 `MaybeScheduleZfSink`（持 DB mutex）里直接
  `FlushMemTable` ⇒ **互斥重入，实测挂死**（200k 基准 900s 超时，exit=124）。已回退为**只探测不触发**；
  正确做法是独立调度线程 / `Env::Schedule` 延迟执行（下一步）。
- 因此 `base_merge_skip_max_generations` **保持默认 0（关闭）**：候选版本使用**当前安全默认**。
  启用态（`ZF_VC_MERGE_BENEFIT=8`）下最小用例与压力已通过（installtest 2/3/4 PASS、
  压力组 1/2 各 100 轮 PASS、timeouts=0、pending=0），但基准（200k）因上述挂死未完成 ⇒
  按用户口径"哪一项不通过就用安全默认"。

### D-67. P2 测量细化（**无需改语义**）

现有实现已满足用户口径："路由候选与历史候选**合并后交给同一 sequence 选择器**决策，
不提前返回路由候选"（单一候选列表 + 上界剪枝 + 全局 seq 比较）。
新增**实际访问**计数后，边界回归的读数变为：
```
并集候选 列入=328 / **实际访问=0**（被上界剪枝全部跳过）
```
⇒ 328 只是"列名单"的计数，真实读路径开销为 0，**无需** 收敛改动（改动反而会破坏
semtest case1/2/3/7/8a 的跨分区语义，已在 D-55/D-60 记录并回退）。计数器仅测量、不改变结果。

### D-68. 最终门禁（本 build，哈希 `/tmp/build_hash_v06cand.txt`）

| 项目 | 结果 |
|---|---|
| closestress 1000 轮 | 零崩溃 |
| semtest cases1-7 | PASS |
| snapmini（含开关/对照组） | PASS |
| aprobe 合法路径 300 轮 | PASS（errors=0 / A_hits=0 / 陈旧索引胜出=0 / 持久丢失=0） |
| baccept phase1–4 | PASS（phase3 pending=0 drained=42720000；phase4 registered=0 reclaimed=42720000） |
| installtest 1–4 | PASS（终点 pending=0、registry=0、旧代 WAL=0） |
| migrate 迁移矩阵 | PASS 8/8 |
| all/scan/triage | PASS（pending=0.0MB、reclaimed=26.2MB、wal_read_delta=0） |
| 五组压力 各 300 轮 | PASS（fails/ss/nf/c=0、rotations=300、install_wait_timeouts=0、pending_last=0） |
| db_bench 退出循环 20 次（含 histogram/statistics） | crash=0 |

### D-69. **V0.6 候选版本：`7d48d8d` 之后的本 build（哈希见 `/tmp/build_hash_v06cand.txt`）**

判定依据（用户口径）：
- **错误配置不会静默停滞**：目标模式 + 固定协议下未指定融合 ⇒ 默认开启；显式关闭 ⇒ 启动
  `InvalidArgument`（C1，D-62）；历史 hash 库保持旧协议旧行为。
- **关闭无崩溃**：P0 竞态修复（扫描持 `GenReadRef` + 已回收代跳过）；closestress 1000 轮 +
  db_bench 退出循环（含 histogram/statistics）20 次均零崩溃。
- **写放大**：量化完成（~24×，根因 = 真正的融合路径缺收益判据）；收益攒批可把写放大降到
  ~1.8×（写吞吐 93k→176k），但需"积压驱动排空"的安全触发路径 ⇒ **按用户口径保持安全默认**
  （`base_merge_skip_max_generations=0`），诊断开关与机制保留。
- **额外查询优化不改变语义**：P2 采用同一选择器（不改语义），实测真实访问=0（列名单 328 是
  计数口径问题，已用 `union_parts_visited` 精确化）。
⇒ 全部门禁通过、两个生产级问题均有明确结论与可回退实现 ⇒ **标记本 build 为 V0.6 候选版本**
（具备受控运行条件）。下一步：P1 安全触发路径（独立调度线程）→ 启用收益攒批并复跑基准与门禁；
P2 保持现状（真实开销为 0）。

## V0.6 下一轮（A1–A3）：判定收紧已落地；后台排空机制完整但**默认不启用**

### D-70. A1：危险判据已复核并收紧（必须有证据，不得把"缺失"当成功）

问题（用户指出）：我此前两处按"registry 里没有条目 ⇒ 已回收 ⇒ 可跳过扫描 / 回落 LSM"
放行——该推断不充分（registry 缺失也可能是登记或生命周期错误，历史上发生过）。

改法：新增**合法回收墓碑** `reclaimed_by_flow_`（只有 `PurgePending` 里**真实 unlink
成功**后才登记，有界保留最近 64K 条）；三处判定全部改为**必须有证据**（任一）：
① 发布证据（cache 级 installed 且仍在登记）；② fence 覆盖；③ 合法回收墓碑。
- 物化扫描（`MaterializePartition` 与融合路径）：无证据 ⇒ **返回 Corruption 明确报错**，
  不再当作成功跳过；
- 读路径 retiring 分流：无证据 ⇒ 不回落，走"重试视图后仍不安全 ⇒ 明确报错"原路径。
验证：semtest（case1/2/3/5/7/8a/8b 全覆盖）PASS、installtest scen 2/4 PASS、压力组 1
100 轮 PASS ⇒ 合法流程全部留下证据，**收紧后零误报**。

### D-71. A2/A3：后台排空机制（完整实现，**默认门控关闭**）+ 定时唤醒的必要性实证

实现（复用既有 flush 调度模式）：
- 调度点 `MaybeScheduleZfDrainLocked()`（**持 DB mutex 只做**：探测积压 → 去重
  （`zf_drain_scheduled_`/`zf_drain_running_`）→ `env_->Schedule` 入队，带 unsched 回调）；
  任务 `BackgroundCallZfDrain()`（回调不持 DB mutex，内部调 `ZfDrainNow()` 的既有锁契约；
  置 `SetBacklogDrainActive(true)` 让收益攒批在排空期强制融合；**有进展立即再排、无进展
  退避 200ms + 计 `install_progress_failures`**）；定时器 `ZfDrainTimerThread`（200ms 周期，
  独立于写入/封存）；`CloseHelper` 起始停止新调度并**无锁等待**运行中任务结束。

**实测（`ZF_ENABLE_BACKLOG_DRAIN=1` + `ZF_VC_MERGE_BENEFIT=8` + 年龄 1s，autodrain 用例）**：
```
停写后（无任何 Flush/DrainNow/close 调用）：pending 10.68MB → 2.29MB
reclaimed 0 → 19 069 140；读校验 bad=0/20000；后台请求数 26k（定时器在工作）
但 pending **未归零**：残留 1 代 3:4(2.29MB, NONE+skip)
```
两个关键教训（都已记录，供下一步直接用）：
1. **定时器必须在任何调度点就启动**，不能"发现积压之后"才启动——否则停写后没人再调用
   调度函数，定时器永远不启动（实测 `requests=0`、pending 不动）。已修正为提前启动。
2. 停写后**没有新 epoch** 来收养 skip 集合里的代 ⇒ 该代永远不安装。我尝试"排空模式禁止
   kSkip（17 处一刀切改为 kMergeBase）"⇒ **段错误**：那些分支里有"目标文件被占用/冲突"
   等尚不具备融合条件的情形，强选 kMergeBase 会在合并装配处空指针。**已完整回退**
   （`NoSkipMode()` 保留但无调用点；kSkip 恢复 17 处）。
   正确做法（下一步）：**只**在"融合装配已完成/compaction 已就绪"的那几处禁用 kSkip，
   或在排空路径显式**收养 skip 集合**为一个 epoch（`AddEpochWithRecoveryAdoption` 已能把
   skip 并入新 epoch，问题是空尾封存不一定产生可物化的 epoch）。
3. 关闭期生命周期竞态：`autodrain` 在**重启/关闭**处 SIGSEGV（定时器/任务与 DB 析构顺序）；
   已加停止标志与无锁等待，仍未消除。**故自动触发默认门控关闭**
   （`ZF_ENABLE_BACKLOG_DRAIN` 显式开启），候选版本继续使用**已验收的安全默认**。

### D-72. 门禁（默认路径，本 build，哈希 `/tmp/build_hash_a1a3.txt`）
```
semtest cases1-7            : PASS
installtest scen 2 / 4      : PASS
五组压力（此处跑组 1 100 轮）: PASS（failures/silent_stale/notfound/corruption=0、timeouts=0、pending=0）
```
**未完成（下一步，按用户顺序）**：① 关闭期生命周期修复（定时器/任务的停止与 DB 析构顺序）
→ ② 排空路径的 skip 集合适配（"空尾封存也能收养并被物化"）或按站点精细禁用 kSkip →
③ 满足"不开新写、不靠关闭也能自动排空"的关键验收 → ④ 全生命周期写放大收益重测
（写阶段/立即读/排空后读、pending 峰值、排空耗时、总输出字节）→ ⑤ 启用新默认并复跑门禁。

## 实验分支 P1BacklogDrain-exp：单任务状态机与关闭契约（A5）

### D-75. 实现（严格按用户契约）

- **状态机**（DB mutex 下唯一维护）：`IDLE → QUEUED → RUNNING → IDLE`，
  `QUEUED/RUNNING ↘ STOPPING`；同一 DB 至多一个 queued/running 任务。
- **生命周期引用**：入队前 `refs+1` 并标 `ref_queued=true`（归属=排队阶段）；
  转 RUNNING 时交接归属（`ref_queued=false`）；**取消回调只释放排队阶段的引用**，
  运行中的任务在退出时释放（两个退出路径都恰好释放一次）。
- **取消**：`env_->Schedule(..., tag=this, unschedCallback)` +
  `env_->UnSchedule(this, LOW)`（ThreadPool 的取消函数在**调用者线程同步执行**，已核对实现）。
- **关闭契约（固定顺序）**：STOPPING → 停并 **join 定时器** → `UnSchedule(tag)` 取消 queued
  → **无锁等待 refs 归零**（沿用 `bg_flush_scheduled_` 的既有等待式样）→ 析构。
  回调在**开始与结束**都检查 STOPPING；关闭开始后不创建新任务。
- **退避**：无进展时**不立即重排**，写 `next_deadline=now+200ms`，交给定时器按 deadline 唤醒。
- **计数拆分**：`timer_wakeups / tasks_queued / tasks_started / coalesced_requests /
  no_progress_backoffs / tasks_cancelled`（新增属性 `zf_drain_state`、`zf_drain_counters`）。
- **定向钩子**：`ZF_TEST_DRAIN_QUEUED_HOLD_MS`（回调开始后、转 RUNNING 前暂停）、
  `ZF_TEST_DRAIN_RUN_DELAY_MS`（转 RUNNING 后、扫描前暂停）。

### D-76. 验收结果

| 用例 | 状态 | 结果 |
|---|---|---|
| **B：执行中、扫描前暂停 → Close** | `RUNNING refs=1` | **PASS**：关闭等待 3002ms（等暂停任务退出）→ 关闭完成 → 重开逐键 6000/6000 正确；计数有界（`tasks_queued=1 tasks_started=1 timer_wakeups=1`） |
| **A：已入队、未开始 → Close** | `QUEUED refs=1 tasks_started=0` | **未通过**：修复了一个真实死锁后关闭能完成（1992ms、重开逐键正确），但**进程在收尾阶段 SIGSEGV** |

**修复的真实缺陷（case A 暴露）**：取消回调原先以 `state == kQueued` 为释放条件，而 Close 已把
状态置为 STOPPING ⇒ **引用永不释放、关闭永久等待**（实测 exit=124 超时）。改为按
`ref_queued` 归属标志释放后，关闭契约成立。

**调度风暴已消除**：两个窗口内 `timer_wakeups=1`、`tasks_queued=1`（此前实测 26 121）。

**未做（按用户顺序）**：`AdoptSkippedGensForDrainLocked()`（状态机完全通过后再做）；
候选线 `b4b17b3` 的最终补充证据（`closestress 1000` + 五组各 300 轮）尚未跑。

## 实验分支 A6/A7：关闭窗口契约 + 收尾崩溃根因（gdb 栈）

### D-77. 两个已证实的缺陷（都不是"停止标志不够多"）

1. **tag 冲突**：`UnSchedule(this, LOW)` 会**误取消同优先级的 flush/compaction**——
   源码核对：`BGWorkCompaction`（LOW）与 `BGWorkFlush`（LOW）都以 `this` 为 tag。
   ⇒ 排空任务改用**独立稳定 tag**（`zf_drain_tag_`，堆地址稳定），所有取消入口同步更新；
   且仅在本 DB 真正入队过时才调用 `UnSchedule`。
2. **收尾 SIGSEGV 的根因（gdb 栈实证，非假设）**：
```
#4 VersionStorageInfo::GetZfInstallFence   ← this 指向**栈上/已销毁**的 VersionStorageInfo
#5 ZeroFlushContext::IsGenInstalled(phys_part=0, gen=1)
#6 ZeroFlushContext::ReleaseEpoch(epoch=2)     zeroflush_db.cc:448
#7 rocksdb::MemTable::~MemTable
#9 rocksdb::ColumnFamilyData::~ColumnFamilyData
#13 rocksdb::ColumnFamilySet::~ColumnFamilySet
#18 rocksdb::VersionSet::~VersionSet
```
   ⇒ DB 析构期（`~MemTable → ReleaseEpoch → IsGenInstalled → cfd_->current()`）解引用正在
   销毁的 Version。**触发条件 = 关闭时仍有未安装 epoch**（攒批/积压场景制造它；候选线的
   closestress 之所以没暴露，是因为那里 epoch 在关闭前都已安装/释放）。
   **修复**：`ZeroFlushContext::DetachFromDb()`（解挂 `cfd_`，之后 `IsGenInstalled` 安全
   返回 false、不误销账），在 `CloseHelper` 里**排空任务停稳之后**调用。
   ⇒ 三个窗口的进程退出**全部 exit=0**（此前 139）。

### D-78. 三个真实关闭窗口（文件屏障 + 控制线程放行；Close 在控制线程里执行）

| 窗口 | 构造 | 观察到的终态（进程全局事件，跨关闭存活） | 结果 |
|---|---|---|---|
| 1 真正排队、未出队 | LOW 池压到 1 线程 + 阻塞任务占住 worker | `enqueued=1 cb_entered=0 cancel_entered=1 released=1`（Close 取消，取消计数=1、回调进入=0） | **PASS**（190ms、重开 6000/6000、exit=0） |
| 2 已出队、主体未执行 | 回调入口文件屏障暂停 | `cb_entered=1 cancel_entered=0 handover=0 stopping_exits=1 released=1`（**不当作已取消**，引用由任务自身释放） | **PASS**（191ms、重开全对、exit=0） |
| 3 执行中、扫描前 | 转 RUNNING 后文件屏障暂停 | `handover=1 stopping_exits=1 released=1`（Close 等其安全退出） | **PASS**（187ms、重开全对、exit=0） |

通用不变量（全部成立）：`cancel_entered + cb_entered == enqueued`、`released == enqueued`、
`refs_at_close_return == 0`、`close_calls == 1`。
**说明（如实记录）**：`close_wait_rounds=0` —— 屏障在 STOPPING 时即放行（保证无死锁的有界
活性），因此窗口 2/3 表现为"任务在 STOPPING 后迅速退出并**恰好释放一次**"，而不是长时间
等待；"Close 未把它当已取消"由 `cancel_entered=0` 直接证明。

### D-79. 状态
- `P1BacklogDrain-exp`：状态机 + 关闭契约**三个窗口全部通过**（进程正常退出、引用归零、
  重开逐键正确、调度计数有界：`tasks_queued=1`、`timer_wakeups=1`，旧版 26 121 的风暴未再现）。
- **候选线 `b4b17b3` 建议补一个小后移**：`DetachFromDb()`（~10 行，独立于排空功能，修的是
  "关闭时仍有未安装 epoch ⇒ DB 析构期解引用已销毁 Version"的潜在崩溃）。是否后移请用户定。
- 未做：`AdoptSkippedGensForDrainLocked()`（下一步）；候选线最终压力证据（closestress 1000 +
  五组各 300 轮）。

## 实验分支 A8：Close 等待的顺序证明（窗口 4）+ 释放语义收紧

### D-84. 代码层收紧（用户契约）

1. **释放必须是回调的最后一步 DB 访问**：重排 `BackgroundCallZfDrain` 为
   ①进入/交接记账 → ②执行排空 → ③结果记账（含 STOPPING 判定、退避与重排标记）
   → ④**最后**递减引用并记录 released → ⑤立即返回（其后仅 mutex 解锁）。
   此前"先递减再改 `zf_ev_released_`/`zf_drain_state_`/deadline"违反该契约，已改正。
2. **硬屏障** `ZfWaitGateHard`：**只由控制线程创建放行文件**才继续（不随 STOPPING 逃逸），
   带 120s 安全上限（防止测试自身挂死）；原 `ZfWaitGate`（STOPPING 即放行）保留给窗口 2/3
   的有界活性语义。
3. **逐任务记录**（用户口径：DB 实例 + task ID 隔离）：新增 `ZfDrainTaskRec{db_instance,
   task_id, phase}` 环形记录与 `ZfDrainDbInstanceSeq()`；enqueue/cb_entered/handover/
   released/cancel 各记一条，重开后的新实例不会污染旧实例计数。

### D-85. 四个窗口的最终结果（实验分支）

```
窗口 1 真正排队/未出队 : exit=0 符合契约 重开 6000/6000
窗口 2 已出队/主体未执行: exit=0 符合契约 重开 6000/6000
窗口 3 执行中/扫描前    : exit=0 符合契约 重开 6000/6000
窗口 4 **Close 等待顺序证明**: exit=0 符合契约 顺序成立（Close 未返回 + 任务仍持引用）重开 6000/6000
```
窗口 4 的实测（控制线程放行版）：
```
关闭前: RUNNING refs=1 | ev{enqueued=1 cb_entered=1 handover=1 released=0}
等待确认: close_未返回=1 放行前 released=0 handover=1（硬屏障 ⇒ 任务仍持引用）
顺序成立（Close 未返回 + 任务仍持引用）
关闭完成: 耗时=508 ms | ev{... released=1 close_wait_rounds=285 stopping_exits=0
          close_calls=1 refs_at_close_return=0}
不变量: 唯一路径=1 引用归零=1 释放次数匹配=1
```
⇒ 顺序 **"任务持引用 → Close 进入等待且未返回 → 控制线程放行 → 任务完成最后一次访问 →
释放引用 → Close 返回"** 已被直接观测（`close_wait_rounds=285` 即 Close 在等待中自旋，
`released` 在放行后才追上 `handover`）。

### D-86. 待办（下一份交付）
**`AdoptSkippedGensForDrainLocked()`**（仅剩 skip 时停写也能排空；持续冲突时保持可读且调度
有界、解除后自行排空；不增加写入、不调用手工排空、不关闭 DB）。随后测完整生命周期写放大，
再决定是否合入候选线。候选线 `0de022f` 已收口，本轮未再改动。

## 实验分支 A9：skip 代排空出口（所有权交接已实现并受控；**调度缺口已定位**）

### D-87. 本轮交付

**实现（实验分支）**
- `SealedFileCache::TakeSkippedGensForDrain()` / `RestoreSkippedGens()`：**一次完整的所有权
  交接**——锁内原子取走（离开可重试集合 ⇒ 同一代不会被两个在飞任务同时收养）；失败退回时
  **不重置积压起始时间**（避免每次重试都重置年龄而无限推迟安装）；两者都**不改变
  pending / installed / drained**。新增一致性计数 `adopt_gen_not_in_skip` / `adopt_double_return`。
- `ZeroFlushContext::AdoptSkippedGensForDrainLocked()`：取所有权 → 组装真实可规划的
  `SealedEpoch`（真实 gens + part_bytes + `has_adopted_skips`）→ `RegisterSealedEpoch` 完成
  归属转移；返回 0=已收养 / 1=无 skip 代 / -1=失败（已退回）。属性 `adopt_stats` 可观测。
- 排空任务在 `ZfDrainNow()` **之前**调用收养（先转移归属，再走正常规划/冲突检查/安装）。
- 两个测试约束：① 硬屏障达到 120s 上限 ⇒ 记 `hard_gate_timeouts`，用例**判 FAIL**
  （不得视为控制线程放行）；② `Close` 的等待**持同一把锁确认引用归零**后才进入析构
  （代码注释与实测一致：`close_wait_rounds` 在等待中自旋）。

**case 2（持续冲突→解除）实测**
```
前置:  观察到 skip 归属=1  pending=6408000  drained=0  adopt=[calls=0 ...]
注入期(30s): 读 bad=0/6000 | drained 变化=0（不销账）| adopt calls=87（有界，退避生效）
等待结束:  pending=6408000  adopt=[calls=87 rolled_back=0 gen_not_in_skip=0 double_return=0]
终态:  pending=6408000 registered=5 reclaimed=0
       pending_gens=[3:1(1369176B,**epoch=3**) 1:0(280884B,epoch=3) ...]
       adopt=[calls=396 ...]  ⇒ **FAIL(安装进展失败：前提成立但超时未排空)**
```
**诊断（本轮最重要的一条）**：收养**成功完成了所有权转移**（gens 从 skip 集合进入
`epoch=3`，一致性计数全 0 ⇒ 单归属成立；96 次收养尝试有界），但**该 epoch 没有任何物化
任务**——物化批次由 flush/imm 驱动，而收养出来的 epoch **没有对应的 memtable/imm**
⇒ `ZfCloseFlush` 的 "seal+flush" 对**空尾**不产生可规划的任务 ⇒ gens 停在 `epoch=3` 里
不被安装（正是此前记过的"空尾封存不产生可物化 epoch"）。按用户口径，这种情况**报安装进展
失败（FAIL），不记 INCOMPLETE** ✓（本轮正是如此）。

**case 1（停写后只剩 skip）**：本轮未建成前置（两轮封存的比例仍 > min_ratio ⇒ 直接融合、
未产生 skip 归属）⇒ 记 `SETUP_INVALID`。需要更小的第二轮批量或更低 base 阈值，属用例参数
问题，下一轮修正。

### D-88. 下一步（唯一缺口）
给"无 memtable 的 epoch"实现**真实的 drain work item 调度入口**（两条路可选）：
① 让排空路径能对指定 epoch 直接构建并投递一个 `ZfMaterializeJob`（等价于 flush 批次的
规划/安装，但不依赖 imm）；② 或让空尾封存也产生可规划任务（例如强制非空 flush 或显式
投递）。要求：走正常 overlap/compaction 注册与安装；冲突时退回可重试集合并退避；
不改变 pending/installed/drained/fence。完成后再跑两个用例 + 全生命周期写放大。

## 实验分支 A10：**收养的 skip 代已真正提交安装**（case 2 达成核心交付）

### D-89. 最小完整改动（三处）

1. **强制为空尾产生真实批次**：新增 `DBImpl::ZfDrainForceFlushEmptyEpoch()`——置
   `zf_force_flush_empty_` 后调 `FlushMemTable`，越过两处"空 memtable 不排批次"的门槛
   （只封存 + 让 flush 批次按**既有**规划/冲突检查/提交安装流程处理该 epoch 的 gens）。
   教训：**不能**在封存后再 `SwitchMemtable`（会多出一个没有 epoch 的 imm ⇒ ZF flush 报
   `immutable memtable without epoch in zf flush`，实测）。
2. **收养语义修正（不造合成 epoch）**：此前把 gens 从 skip 集合"偷"进自建 epoch ⇒ 正常封存的
   采纳机制看不到它们、自建 epoch 又没有批次可调度 ⇒ **永久 pending**（实测 `epoch=3`）。
   现在 `AdoptSkippedGensForDrainLocked()` 只做两件事：① 找出"已采纳进 epoch 但 gens 仍未
   安装"的**卡住 epoch**，把其 gens 归属**退回**可采纳集合（`RequeueStuckEpochsForDrain`，
   满足"每个已收养未安装 epoch 必须有明确重试条件"）；② 报告是否还有待采纳 gens ⇒ 调用方
   随即强制封存，由 `AddEpochWithRecoveryAdoption` 原子采纳进**真实 epoch**。
3. 只绕过"收益不足"的等待，安全冲突检查照常（避让/退避不变）。

### D-90. case 2（持续冲突→解除）实测：**真实提交成功**
```
前置:        观察到 skip 归属=1  pending=6408000  drained=0
注入期 30s:   读 bad=0/6000（可读）| drained 变化=0（该批不销账）| 收养尝试=75（有界）
解除冲突后:   **pending 6408000 → 0，drained 0 → 6408000**（无新写入、无手工排空、不关闭）
终态:        pending_gens=n=0（已安装）、读 6000/6000 正确、单任务归属一致=1
```
⇒ 用户本轮要求的**"停写后的 skip 代真正提交安装"**已达成 ✓（drainwork 日志：
`flush-empty-epoch status=OK pending=0 drained=6408000`）。

**仍差的两点（下一轮）**
- **最终回收未在窗口内发生**：`registered=5`、`reclaimed=0`（120s 窗口）——安装已完成，
  但 unlink/PurgePending 未被触发（或触发更晚）。需查该路径下 purge 的触发条件（是等下一次
  flush/compaction 完成，还是需要排空路径显式调用生产回收入口）。
- **case 1 本轮挂住（exit=124）**：两轮封存 + 强制排空的组合下耗时超过 400s；日志停在
  第二轮封存后的等待。怀疑 `fo.wait=true` 的强制 flush 在排空任务线程上等待自身（或与
  drain 的批次互斥）——需用 `ZF_DBG_DRAINWORK`/ZFTD 阶段日志定位，并考虑 `wait=false` +
  由排空循环轮询。

## 实验分支 A11：**四个真实缺陷修复后，case 1 / case 2 闭环达成**

### D-91. 本轮 build 与命令

**build 哈希**：`/tmp/build_hash_final_a17.txt`（`zf_valcheck 9197c61b…`、`librocksdb.so.11 88a3c2a3…`）。
命令统一为 `LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu ZF_VC_MERGE_BENEFIT=8
ZF_VC_BACKLOG_AGE_MS=<见各用例> ./zf_valcheck <db> <mode> <n> <arg>`（`LD_LIBRARY_PATH`
用于绕开 anaconda 的旧 libstdc++）。诊断开关 `ZF_DBG_MERGE/ZF_DBG_MM/ZF_DBG_DRAINWORK/
ZF_TRACE_IMM/ZF_DBG_MARK_BT` 全部按需开启、默认无输出。

### D-92. 修复清单（每条都有直接证据，非推断）

1. **排空由"积压字节/年龄"驱动，但年龄判据的时钟永不复位**（`zeroflush_db.cc`）：
   `TouchBacklogAge()` 全仓**无调用点** ⇒ `backlog_since_micros_` 首次置位后永不复位，
   30s 后 `now - since >= age` **恒真** ⇒ 排空任务长期在飞（`backlog_drain_active=true`）
   ⇒ 规划点恒走 drain 模式 ⇒ "收益不足 ⇒ skip"在合法路径上不可达。
   修复：年龄改由周期检查点（200ms 定时器 + 各调度点）就地维护"当前这段连续积压"的年龄
   （归零复位、有积压且未登记则登记）。证据：`ZFDBG-skipgate` 首次出现
   `drain_mode=0 … gate=1`。
2. **`ZfSwitchMemtable` 入队 flush 请求后不触发调度**（`db_impl_write.cc`）：
   `EnqueuePendingFlush` 只入队 + `++unscheduled_flushes_`，不唤醒 worker；上游依赖调用方
   随后 `MaybeScheduleFlushOrCompaction()`（连续写入时被写路径掩盖）。证据（线程栈 + 入队
   trace）：`enqueue=1` → `flushq=1 unschedflush=1 bgflush=0 maxflush=4`（worker 全空闲）
   → **30s 无任何批次被规划**，直到关闭路径才被调度 ⇒ "停写后已封存代永不物化"。
   修复：封存路径自行调度。
3. **`Compaction` 构造函数内部即 `MarkFilesBeingCompacted(true)`**（backtrace 证实：
   `MarkFilesBeingCompacted ← Compaction::Compaction ← ZfMaterializeJob::PlanLocked`）——
   **构造后任何"放弃融合"出口都必须显式解除标记**。此前收益门禁（kSkip）与
   `GetProximalLevel` 降级两条出口直接 `continue` ⇒ 输入 L1 文件**永久 being_compacted**
   （无人持有、无人解除）⇒ 该分区此后每次规划都 `ok=0/busy=1` 让位 ⇒ 排空陷入
   `frz 空尾 → adopt skip → tasks=0` 死循环（实测 36 次 adopt、pending 不减；
   `ZFDBG-mm-mark set=1 inputs=[9]` 后无对应 `set=0`）。
   修复：① 收益门禁移到 `RegisterCompaction` **之前**并显式 unmark；② 引入**注册台账**
   `registered_compactions_`（构造即登记、注册打标），释放只认台账（与"计划当前是否仍为
   kMergeBase"无关），覆盖"注册后改判/提前返回"的所有出口。
4. **排空的强制封存与 drain 标记窗口**（`db_impl_compaction_flush.cc`）：
   ① 无 skip 代可采纳时**不再强制封存空尾**（避免制造合成 epoch、污染后续规划——
   用户口径"任务在飞期间不得再次收养或反复生成空批次"）；② `SetBacklogDrainActive(false)`
   移到规划阶段结束、回收观察窗之前（该标记只影响规划）。

### D-93. case 1（停写后 skip 代自行安装 + 回收）——**PASS**

```
第一轮: 完成=1 耗时=100 ms pending=0 drained=6408000
第二轮: 封存完成 pending=2136
ZFDBG-skipgate pid=0 epoch=3 ratio=0.0076 min=0.2500 gens=1 maxgens=8
   drain_mode=0(close=0 active=0 size=0 gens_cap=0) skip_ok=1 forbid_l0=1
   skip_batch=1 **gate=1**                      ← 规划点直接断言：ratio<阈值、原因=收益不足
前置: skip_merge_benefit=1 skip归属=1 pending=2136 drained=6408000
前置已确认（skip_merge_benefit=1），放行后台排空   ← 唯一外部动作=放行（年龄到点自动入队）
终态: pending=0 registered=0 reclaimed=6410136 pending_gens=n=0 bytes=0
      adopt=[calls=1 rolled_back=0 gen_not_in_skip=0 double_return=0]
case=1: PASS (pending归零=1 registry=0=1 回收>0=1 读正确=1 单任务归属一致=1)
```

要点：`adopt calls` 从 36（死循环）降到 **1**（单次收养即完成安装与回收）；
用户要求的"停写后、无外部排空操作、目标代完成安装和回收、期间无挂起或重复消费"达成。
观察窗口的实现说明：case 1 原用 `ZF_ENABLE_BACKLOG_DRAIN` 关排空——**该变量是死变量**
（引擎不读，`grep` 仅测试侧出现），且用 `ZF_TEST_DRAIN_GATE_RUN` 拦排空会**占住后台 worker**
（本 fork flush 与排空共用池）导致第二轮批次被饿死。现改为**生产选项**：把积压年龄阈值
抬到高于第二轮规划延迟（`ZF_VC_BACKLOG_AGE_MS=60000`），观察期内无排空任务在飞
（`active=0`）⇒ 规划点按收益不足进入 skip；随后年龄到点自动入队完成排空。

### D-94. case 2 + 四个关闭窗口 + 撤销/提交失败用例（同一 build）

```
skipdrain case=2: PASS (pending归零=1 registry=0=1 回收>0=1 读正确=1 单任务归属一致=1)
drainlife win=1: PASS  QUEUED   ev{enqueued=1 cb_entered=0 cancel_entered=1 released=1}
drainlife win=2: PASS  RUNNING  ev{cb_entered=1 handover=0 stopping_exits=1}
                        close_wait_rounds=**282**（Close 确实等待被屏障拦住的出队任务）
drainlife win=3: PASS  RUNNING  ev{handover=1} close_wait_rounds=**274**
drainlife win=4: PASS  RUNNING  ev{handover=1} close_wait_rounds=**272**（硬屏障不随
                        STOPPING 放行 ⇒ 证明 Close 真的等）
四窗口共同：硬屏障超时=0、重开逐键 bad=0/6000、进程 exit=0
installtest 2/3/4: PASS（含路由一致 + 融合证据）
failaccept A/B/C: PASS（注入期合规=1 重启语义=1 解除后安装排空=1）
```

**两处夹具修正（因本轮引擎行为变化而必须做，不是引擎回归）**：
- 关闭窗口的"持久积压"造法：旧夹具隐含依赖缺陷 1（年龄恒真）。年龄修正后，一个完整 epoch
  在 ~100ms 内装完 ⇒ 积压清零 ⇒ 年龄永不触发 ⇒ `enqueued=0`。现改为与 case 1 同构
  （攒批开启时补写极小第二批并封存 ⇒ 该代被收益门禁判"不足" ⇒ 留在 skip 集合 ⇒ 积压持续）
  ⇒ 排空任务按年龄（测试用 1s）入队，窗口回到"任务在队/在跑"的真实场景。
- 关闭窗口的屏障由**运行命令**提供（`ZF_TEST_DRAIN_GATE_ENTER` / `ZF_TEST_DRAIN_GATE_RUN`
  ⇒ 指向**不存在**的文件即为"拦住"，用例在断言之后创建该文件放行）；漏设这两个变量时
  屏障不生效（任务直接跑完）⇒ 窗口 2/4 会误报 FAIL。

### D-95. 仍差的一步（下一轮）

按用户交付线，闭环已达成（安装 + 回收 + 无挂起/重复消费）。**尚未测**：
含最终排空在内的**全生命周期写放大**（prof_output_bytes / 输入重写比）、积压峰值、
停写后立即读的延迟、排空耗时、`no_progress_backoffs` / `forced_merge` 计数。
候选线 `0de022f` 未动。

## 实验分支 A12：**全生命周期写放大对照（含最终排空）** —— 收益真实但大规模下立即读退化

### D-96. 新用例 + build

**新用例 `zf_valcheck <db> lifecycle <n> <threads>`**：严格按用户指定流程——写入 → 停止写入
→ **不调用 Flush/DrainNow、不关闭 DB** → 等后台 pending=0（全程采样峰值）→ 立即读一遍 →
重开 → 再读一遍 → 确认旧代 WAL 已清零。两组由**环境**切换（同一 build）：
基线 = 不设 `ZF_VC_MERGE_BENEFIT`；实验 = `ZF_VC_MERGE_BENEFIT=8` + `ZF_VC_BACKLOG_AGE_MS=2000`。
固定 range 路由 4 分区（边界按 n 等分 ⇒ 键均匀落盘），4 写线程 / 4 读线程。
**build 哈希**：`/tmp/build_hash_lc_final2.txt`（`zf_valcheck`、`librocksdb.so.11`）。

新增度量口径（本轮补齐）：`install_wait_stats{rounds,timeouts}`（安装准入有界等待次数与
到期无进度次数）、`drain_forced_merges`（本应攒批但被排空模式强制融合的次数）、
`insert_dropped` / `index_resegmented`，以及 4 线程读吞吐（立即读 / 排空后各一次）。

### D-97. 本轮修掉的一个**数据安全**缺陷（由该度量首次暴露）

**干净关闭后丢失每个分区最后 0–3 条已确认写入**（实测 `restart_read_errors=6/20000`、
`last_seq` 比真实最大 seq 少 2–3）。取证链：
① 重启后失败键固定为各分区**尾部**记录（4998/4999、9998/9999、19997–19999）；
② 用 CRC32C 逐条校验 **WAL 文件内全部记录 CRC 合法**、文件按记录边界完整收尾；
③ 恢复取证：`scanned_bytes == fs_size`（扫描器读满文件）、`replayed == scanned`，但
`max_seq` 仍少 2–3 ⇒ 记录**在恢复扫描时还没落盘**；
④ 决定性命中：**关闭路径从不刷 ZF WAL 的在途缓冲**——`Append` 只写入分区内存缓冲
（≥4KB 才 `FlushBuf`），而 `ZfCloseFlush` 的封存循环受 `HasPendingSealedData()` 保护
（"已封存数据已排空、仅剩活跃代尾部"时循环不执行）⇒ 残量缓冲只在**进程退出**时才刷。
进程内读没问题（读走内存索引），只有"干净关闭 + 重启"才暴露。
**修复**：`ZfCloseFlush` 入口无条件 `wal()->SyncAll()`（= 逐分区 FlushBuf + fsync），
尾部保持"活跃代未封存"语义（重开由恢复重放读取）。修后 4 组全部 `restart_read_errors=0`。

### D-98. 200k×1KB（输入 208,000,000 B；4 分区 / 4 线程）

| 指标 | 基线（攒批关） | 实验（攒批+自动排空） |
|---|---|---|
| write_ms | 1425 | 2141 |
| out_bytes | 1,291,679,259 | 712,193,067 |
| **wa_out（out/input）** | **6.21** | **3.42（−45%）** |
| wa_drained | 0.967 | 0.967 |
| pending 峰值 | 50,294,256 | 50,285,712 |
| drain_ms | 700 | 200 |
| 立即读吞吐（4T） | 205,997 ops/s | 212,422 ops/s（bad=0） |
| 排空后读吞吐（4T） | 213,363 | 208,383 |
| 立即/排空后延迟 | 13µs / 9µs | 19µs / 10µs |
| skip_merge_benefit | 0 | 12 |
| merge_rewritten | 1,092,964,428 | 513,452,916 |
| install wait rounds/timeouts | 24 / 10 | 9 / 1 |
| forced_merges | 0 | 0 |
| reclaimed_bytes | 201,161,004 | 150,900,924 |
| restart_read_errors | 0 | 0 |
| tasks_queued / started / timer_wakeups / coalesced | 0 / 0 / 10 / 0 | 0 / 0 / 8 / 0 |
| no_progress_backoffs | 0 | 0 |

### D-99. 500k×1KB（输入 520,000,000 B；4 分区 / 4 线程）

| 指标 | 基线 | 实验 |
|---|---|---|
| write_ms | 4131 | 3486 |
| out_bytes | 3,640,804,339 | 1,239,287,687 |
| **wa_out** | **7.00** | **2.38（−66%）** |
| wa_drained | 0.999 | 1.027 |
| pending 峰值 | 83,621,196 | **282,562,896（3.4×）** |
| drain_ms | 1402 | 2704 |
| **立即读吞吐（4T）** | **160,286 ops/s** | **98,118 ops/s（−39%）** |
| 排空后读吞吐（4T） | 201,284 | **228,394（+13%）** |
| 立即/排空后延迟 | 16µs / 11µs | 32µs / 10µs |
| skip_merge_benefit | 0 | 16 |
| merge_rewritten | 3,127,811,977 | 711,781,707 |
| install wait rounds/timeouts | 121 / 70 | 20 / 3 |
| forced_merges | 28 | 0 |
| reclaimed_bytes | 519,307,524 | 534,000,000 |
| restart_read_errors | 0 | 0 |
| tasks_queued / started / timer_wakeups / coalesced | 1 / 1 / 25 / 56 | 2 / 2 / 29 / 117 |
| no_progress_backoffs | 0 | 0 |

四组共同：`drained=1`（自动排空）、`pending_before_close=0`、
`old_wal_after=0`（旧代 WAL 清零）、立即/排空后/重启读**全部 0 错误**、进程 exit=0。

### D-100. 判定（按用户 2026-09-18 的标准）

- **写放大确实下降**：6.21→3.42（200k）、7.00→2.38（500k），且规模越大收益越大 ✓
- **自动排空 + 回收 + 重启全部正确** ✓
- **调度请求量无风暴**：`tasks_queued ≤ 2`、`timer_wakeups ≤ 29`、`no_progress_backoffs=0` ✓
- **但"立即读吞吐明显下降"在 500k 出现**：98,118 vs 160,286 ops/s（**−39%**），
  对应积压峰值 282.6MB（基线 83.6MB，3.4×）与排空耗时 2704ms（基线 1402ms）；
  200k 规模下两组读吞吐相当（212k vs 206k）。
- 结论：**按用户既定标准，不设为默认**，仅保留实验开关（`ZF_VC_MERGE_BENEFIT>0` +
  积压年龄）。候选线 `0de022f` 未动。

### D-101. 同一 build 的 P1 验收回归（`SyncAll()` 关闭修复后，必须复跑）

```
skipdrain case=1: PASS (pending归零=1 registry=0=1 回收>0=1 读正确=1 单任务归属一致=1)
skipdrain case=2: PASS (pending归零=1 registry=0=1 回收>0=1 读正确=1 单任务归属一致=1)
drainlife win=1/2/3/4: 窗口终态 符合契约（硬屏障超时=0）| 重开逐键 bad=0/6000 -> PASS
installtest 2/3/4: PASS（路由一致 + 融合证据 + 重启后逐键）
failaccept A/B/C: PASS（注入期合规=1 重启语义=1 解除后安装排空=1）
```

关闭修复只增加"关闭前 `SyncAll()`"一步（把活跃代尾部缓冲落盘），未改动状态机与引用
生命周期；上述回归确认四个关闭窗口的时序契约不变（`close_wait_rounds≈270-282` 仍成立）。

## 实验分支 A13 / 候选线 P1-A12：尾部数据安全修复入候选线 + 积压阈值参数扫描

### D-102. 候选线：`SyncAll()` 关闭尾部刷新（commit `e7a6552`，`fix/zeroflush-recovery-visibility-fences`）

改动只有 1 处引擎代码（`db_impl.cc` 的 `ZfCloseFlush` 入口无条件 `wal()->SyncAll()` =
逐分区 FlushBuf + fsync，+ include）与 1 个新用例 `tailflush`。
**门禁（同一 build：`zf_valcheck e4d6069d…` / `librocksdb.so.11 aa004829…`）**：

```
tailflush                  PASS（① 基线 5.2MB/分区 → pending=0；② Sync 后追加
                                3 条/分区（3132B < 4KB，断言"仅存于内存缓冲"）；
                                ③ 关闭 → 重开逐键 12/12 + 基线抽样 800 ✓ +
                                账实对照 4/4 分区（文件字节 ≥ 关闭前记账字节））
detachreg                  PASS（关闭未崩溃 + 重开逐键 bad=0/4000）
closestress 1000           ok=1000 timeout=0 other=0
```

**诚实记录（回归的判别力）**：候选线自身"小尾 + 关闭"路径会因关闭期 imm 刷盘顺带
封存，因此 `tailflush` 在**修复前后均通过**——它的价值是**不变量守卫**（账实对照 +
逐键），将来任一路径跳过"关闭前刷尾部"即失败。缺陷的**可复现证据在实验分支**
（排空路径先消费内存表 ⇒ 关闭不封存）：修复前 `restart_read_errors=6/20000`，
修复后 4 组全部 0（见 D-97）。

### D-103. 实验分支：`base_merge_skip_max_bytes` 参数扫描（8/16/32MB）

同一 build（`zf_valcheck e60f5c32…`；`ZF_VC_SKIP_MAX_MB` 为新增工具开关），固定
200k/500k × 1KB、4 分区、4 线程、`base_merge=1`、**积压年龄阈值统一取部署默认 30s**
（上一轮曾用 2s，实测该阈值会同时改变基线行为 ⇒ 不能作为对照混用），流程严格为
写入 → 停写 → 自动排空（不调 Flush/DrainNow/Close）→ 立即读 → 等回收 → 重启读。

| 配置 | 写放大 | 立即读吞吐(4T) | 相对基线 | pending 峰值 | 排空 | 安装等待超时 | 空桩 | 重启错误 |
|---|---|---|---|---|---|---|---|---|
| base_200k（攒批关） | 6.20 | 210,077 | — | 48.0MB | 700ms | 10 | 0 | 0 |
| base_500k（攒批关） | 5.53 | 171,452 | — | 93.5MB | 26,042ms | 57 | 8 | 0 |
| skip8_200k | 6.21 | 200,825 | −4% | 32.0MB | 500ms | 6 | 0 | 0 |
| skip8_500k | 6.93 | 120,122 | **−30%** | 125.9MB | 1101ms | 71 | 0 | 0 |
| skip16_200k | 3.42 | 215,873 | +3% | 48.0MB | 200ms | 1 | 0 | 0 |
| skip16_500k | **7.97** | 96,694 | **−44%** | 48.0MB | 1302ms | 68 | 0 | 0 |
| skip32_200k | 3.42 | 206,862 | −2% | 48.0MB | 200ms | 1 | 0 | 0 |
| skip32_500k | 3.69 | 206,230 | +20% | 47.9MB | 200ms | 6 | 0 | 0 |

八组全部满足：`pending=0`、非空旧代 WAL 清零、立即/排空后/重启读 0 错误、
`no_progress_backoffs=0`、调度请求 `tasks_queued ≤ 3`。

### D-104. 按预先判据的结论：**没有参数通过筛选 ⇒ 不设默认，保留实验开关**

- 「重启读错误为 0」「pending=0 + 旧代 WAL 全部回收」「no_progress_backoffs=0」：全组 ✓
- 「安装超时为 0」：**无一组通过，且基线也不通过**（10/57——有界 100ms 等待被计入，
  但 `no_progress_backoffs=0` 表明每次都因进度推进而重试）⇒ 该判据的写法需要重定
  （建议改为"≤ 基线"或直接以 `no_progress_backoffs`/`install_progress_failures` 为准）。
- 「立即读吞吐下降 ≤10%」：200k 三档均满足（−4%/+3%/−2%）；**500k 仅 32MB 满足**
  （+20%），8MB −30%、16MB −44% ✗。
- 「峰值与排空明显低于 32MB 配置」：8MB 只降低 200k 峰值（32 vs 48MB），500k 反而
  125.9MB 且排空更久；16MB 在 500k 与 32MB 持平（48MB）而排空 1302ms > 200ms ✗。
- 「写放大明显低于安全基线」：200k 的 16/32MB 达标（−45%）；**500k 只有 32MB 达标**
  （−33%），8/16MB 反而**高于**基线（6.93/7.97 vs 5.53）✗。
- 追加发现（判定依据本身的问题）：**500k 单配置重复运行差异极大**（skip16_500k 早前一轮
  2.00、本轮 7.97；base_500k 三轮 7.00/3.12/5.53）⇒ 参数效应小于运行间波动，
  当前扫描不足以支持"设默认"的决策；需先降低波动（重复取中位数/固定排空时序）。

结论与用户预案一致：**收益攒批保持实验开关，不设默认**。候选线 `e7a6552` 只含数据安全
修复，不含任何性能实验代码。

### D-105. 附带发现（记入待办，不在本轮修）

- 排空的"强制空尾封存"会留下 **0 字节 WAL 文件桩**（每轮每分区 1 个；实测一次
  500k 运行残留 8–12 个，全部 0 字节、无数据）。空间无损，但长期运行会累积 inode；
  建议后续在 purge 路径把 0 字节旧代文件一并删除。
- `lifecycle` 用例本轮的回收判据已修正为"**非空**旧代文件 = 0"（原先把 0 字节桩
  误判为未回收，导致随机 FAIL；数据侧另有 `reclaimed_bytes` 直接计量兜底）。

## 候选线 A14：`e7a6552` 完整门禁（含重启校验）

### D-106. 门禁结果（同一 build：`zf_valcheck e4d6069d…` / `librocksdb.so.11 aa004829…`）

| 用例 | 结果 |
|---|---|
| `all 6000`（写+封存+安装+读） | exit=0；`checked=9064 mismatch=0 miss=0 errors=0`；`pending=0 drained`、`gens=0` |
| `verify 6000`（重启后逐键） | exit=0（同一 build 同一库重开） |
| `scan 6000`（同库） | exit=0；`mismatch=0 (lost_overwrite=0 unexpected_overwrite=0 other=0)/6000`；`index_insert_dropped=0` |
| `triage 6000`（`ZF_VC_KEEP_WAL=1`，同库） | exit=0；`ACCEPTANCE: B2_records_with_gen_le_fence=0 → PASS(no over-advanced fence)`；`B3_stale_seq=0`；来源 INDEX=2997 |
| `semtest 20000`（`ZF_VC_PARTITION_TARGET_MB=1`） | case1–6、case8a/8b PASS；**case7 FAIL（见下，非正确性）** |
| `baccept 20000` | exit=0；phase2 强制冲突 `batches=5 drained_flat=5 pending_flat=5 outputs_discarded=2` PASS；phase3 `pending=0 drained=42,720,000 (>baseline)` PASS；phase4 `registered=0 reclaimed=42,720,000` PASS；`l0_install=0 gen_lost_before_install=0` |
| `migrate A–H` | 8/8 exit=0，`migrate: PASS (fails=0)`（含 B/C/D/E/F/G/H 各负例明确报错） |
| `stress 1–5 ×300`（`ZF_READ_SHORT_RETRY=0 ZF_VC_PARTITION_TARGET_KB=128`） | 五组全部 `rounds=300 failures=0 silent_stale=0 notfound=0 corruption=0 rotations=300 propagated=0 install_wait_timeouts=0 → PASS`；组 4 明确不要求安装（`registry_last=0(组4不要求安装)`）；组 1–3/5 `pending_last=0` |
| `tailflush` / `detachreg` / `closestress 1000` | 见 D-102（PASS / PASS / ok=1000 timeout=0 other=0） |

**case7 说明（不得当作正确性失败，也不得当作通过）**：case7 的**身份断言全部成立**
（`sel=(part=2,gen=0,off=1068,seq=200)` 与 expect 完全一致；pin 身份一致且 `pinned=1`；
`sealed=1 pinned=1 pin_read_A=1`）。判 FAIL 的唯一原因是 `exec[A=0 B=0 B_hit=0 B_lost=0`——
这四个计数是**实验分支的逐候选 visitor 插桩**，候选线不包含该插桩，故恒为 0。
要在候选线上让它变绿必须移植实验插桩，与"候选线清洗"决策冲突 ⇒ 本轮按
`N/A（缺少实验专属 visitor 插桩）` 记录，交由用户决定是否移植。

### D-107. 固定排空时序 + 每档 3 次重复（中位数与波动）

**配置**：`ZF_VC_BACKLOG_AGE_MS=1000`（固定排空节奏，替换会随规模漂移的默认 30s）、
其余同前（4 分区 / 4 线程 / base_merge=1 / 200k 与 500k ×1KB）；**每档重复 3 次**，
下表中位数与 [min–max]。build：`zf_valcheck 8b932eb9…`。

| 配置 | 写放大（中位[范围]） | 立即读吞吐(4T) | pending 峰值 | 排空耗时 | 安装等待 超时(中位) | **最终安装失败**(3 次合计) | 重启错误 |
|---|---|---|---|---|---|---|---|
| base_200k | 5.66 [5.39–6.21] | 209,194 | 32.0MB | 600ms | 6 | 1 | 0 |
| skip8_200k | 6.21 [6.21–6.21] | 207,751 | 48.0MB | 600ms | 10 | 0 | 0 |
| skip16_200k | 5.73 [3.50–5.73] | 164,770 | 32.0MB | 500ms | 6 | 0 | 0 |
| skip32_200k | 4.36 [4.36–6.34] | 147,531 | 27.8MB | 300ms | 2 | 0 | 0 |
| base_500k | 8.61 [6.30–10.39] | 105,178 | 90.4MB | 1802ms | 80 | 2 | 0 |
| skip8_500k | 7.00 [6.73–7.23] | 100,848 | 63.9MB | 4407ms | 66 | 4 | 0 |
| skip16_500k | 10.47 [7.59–10.48] | 114,857 | 83.6MB | 6711ms | 145 | 2 | 0 |
| skip32_500k | 8.77 [8.04–9.44] | 107,927 | 80.0MB | 4405ms | 114 | 1 | 0 |

相对基线的中位变化：

```
200k: 8MB  WA +10%  立即读 −1%   峰值 +50%
     16MB  WA  +1%  立即读 −21%  峰值  0%
     32MB  WA −23%  立即读 −29%  峰值 −13%
500k: 8MB  WA −19%  立即读  −4%  峰值 −29%
     16MB  WA +22%  立即读  +9%  峰值  −8%
     32MB  WA  +2%  立即读  +3%  峰值 −11%
```

**两个计数器已按用户要求分开**：`wait[rounds/timeouts]` 是安装准入的**有界 100ms 等待**
（到期后因进度推进而重试，属正常退避）；`install_progress_failures` 是**最终安装失败**
（连续无进展）。上表显示：固定节奏（1s）下前者在两个规模都显著升高（中位 2–145 次），
后者也不再为 0（1–4 次/3 次运行）——即"排空高频"本身会造成真实的安装无进展窗口。

### D-108. 结论（本轮不变，且证据更强）：收益攒批不设默认

1. **固定 1s 节奏下，攒批不再有稳定收益**：200k 的 8/16MB 写放大反而 +10%/+1%，
   500k 的 16MB +22%；只有"32MB@200k −23%"与"8MB@500k −19%"两个孤立点为正收益，
   且两者都伴随另一规模上的退化 ⇒ 无一致赢家。
2. **排空节奏是一阶因素**：把年龄阈值从默认 30s 改为固定 1s 后，**基线自身也变差**
   （500k：WA 5.53→8.61、立即读 171k→105k、峰值 93MB→90MB 但排空 26s→1.8s，
   且出现 2 次最终安装失败）。上一轮"32MB 在 500k 明显更好"的观测部分来自年龄阈值的
   混淆 ⇒ 该结论撤回，直到能在**固定且部署合理**的节奏下重复。
3. 所有 24 次运行的**安全项全部成立**：`restart_read_errors=0`、`pending=0`、
   非空旧代 WAL 清零、三处读 0 错误、无外部终止。
4. 按用户既定规则（重启 0 错 / pending 归零回收 / 安装超时 0 / 立即读降 ≤10% /
   峰值与排空明显更低 / 写放大明显低于基线）——**没有任何一档同时满足**，
   「安装超时=0」甚至基线也不满足 ⇒ **保持实验开关，不设默认**。

---

# 发布报告（候选线冻结：`c59fc41`，tag `v0.6-candidate-c59fc41`）

**冻结内容**：`e7a6552`（引擎：关闭前无条件刷 ZF WAL 尾部 —— 数据安全修复）
+ `c59fc41`（仅测试工具：semtest case7 缺实验插桩时记 `N/A/IDENTITY_PASS`）。
**引擎 build 哈希**：`librocksdb.so.11 aa004829e6009a729266aa5da091f1bb`
（`c59fc41` 只改 `tools/zf_valcheck.cc`，引擎哈希与门禁时**完全一致** ⇒ 门禁结论对该提交继续有效）。

## R1. 候选线门禁（全部通过）

| 用例 | 结果 |
|---|---|
| `all 6000` | `checked=9064 mismatch=0 miss=0 errors=0`；`pending=0`、`gens=0` |
| `verify 6000`（重启逐键） | PASS |
| `scan 6000`（同库） | `mismatch=0/6000`、`index_insert_dropped=0` |
| `triage 6000`（`ZF_VC_KEEP_WAL=1`） | `B2_records_with_gen_le_fence=0 → PASS`、`B3_stale_seq=0` |
| `semtest 20000`（`ZF_VC_PARTITION_TARGET_MB=1`） | **PASS**（`fails=0 setup_incomplete=0 n/a=1`）；case1–6、case8a/8b PASS |
| semtest **case7** | **`N/A/IDENTITY_PASS`** —— locator/pin/sealed 读取身份全部正确；缺实验分支专属的逐候选 visitor 插桩（不把插桩移入候选线）。既不计通过也不计失败 |
| `baccept 20000` | phase2/3/4 PASS；`l0_install=0`、`gen_lost_before_install=0` |
| `migrate A–H` | 8/8 PASS（各异常均明确报错，不重建路由） |
| `stress 1–5 ×300` | 五组 `failures=0 silent_stale=0 notfound=0 corruption=0 rotations=300 install_wait_timeouts=0` |
| `tailflush` / `detachreg` / `closestress 1000` | PASS / PASS / `ok=1000 timeout=0 other=0` |

## R2. 指标口径（发布报告必须分开陈述）

- **`wait[timeouts]`**（`install_wait_stats.timeouts`）= 安装准入的**有界 100ms 等待到期**，
  随后因进度推进而重试 —— 属于**正常退避**，不是安装失败。
  实测（实验分支 lifecycle，固定 1s 排空节奏）：中位 2–145 次/运行。
- **`install_progress_failures`** = **最终安装失败**（连续无进展）。实测同一批运行：
  0–4 次/3 次运行（基线 1–2 次）。**这才是滞留信号**。
- 两者都不得单独作为"安装不可用"的判据；候选线门禁中二者均为 0
  （`stress 1–5`：`install_wait_timeouts=0`、`pending_last=0`）。

## R3. 收益攒批 = 实验特性（默认关闭）

- 默认**关闭**：需显式设置 `ZF_VC_MERGE_BENEFIT>0`（且按需 `ZF_VC_SKIP_MAX_MB`）才启用。
- **不要用当前数据选默认阈值**：固定排空节奏（年龄 1s）下 3 次重复的中位数显示
  无稳定赢家（200k：8/16MB 写放大 +10%/+1%；500k：16MB +22%；仅 32MB@200k −23%
  与 8MB@500k −19% 两个孤立正收益点，且各自在另一规模退化），并且**基线自身**在
  该节奏下也变差（500k WA 5.53→8.61、立即读 171k→105k）⇒ 排空节奏是一阶因素，
  上一轮"32MB 在 500k 更好"的观测已按 D-108 撤回。
- 全部 24 次测量运行的安全项成立：`restart_read_errors=0`、`pending=0`、
  非空旧代 WAL 清零、三处读 0 错误。

## R4. 遗留（不阻塞发布）

1. **0 字节 WAL 文件桩**：排空的"强制空尾封存"每轮每分区留下 1 个 0 字节旧代文件
   （实测一次 500k 运行残留 8–12 个）。**无数据、不占空间**，仅累积 inode；
   建议后续在 purge 路径把 0 字节旧代文件一并删除（低优先级）。
2. 实验分支继续保留：积压排空（年龄/阈值触发）、收益攒批、skip 收养、
   `ZF_VC_*` 全部实验开关；候选线不含任何性能实验代码。

---

# 受控运行 1：50GB/1KB 读写（候选线 `c59fc41`）—— **ZF 臂写入停滞（新故障）**

### D-109. 运行配置（与历史 50GB 口径逐字一致）

```
--benchmarks=fillrandom --key_size=16 --value_size=1024 --num=48000000 --writes=3000000
--threads=16 --seed=20260914 --compression_type=none --cache_size=536870912
--max_background_jobs=24 --subcompactions=16 --histogram=true --statistics=false
ZF_L0_TRIGGER=100000000 --zeroflush --zf_partitions=16 --zf_routing=sampled
--zf_value_cache_mb=64 --zf_base_merge=true
```
- ZF 臂二进制：`/home/embed/hyl/zf_cand/source/rocksdb-zeroflush/db_bench`
  （`db_bench 7f6672cd…`、`librocksdb.so.11 aa004829…` = 冻结引擎）；目录
  `output/zeroflush_m5_perf/c59fc41_50gb_1kb/`。
- native 臂：`source/rocksdb/build/db_bench`（同参数、无 ZF 开关），目录
  `output/zeroflush_m5_perf/native_50gb_c59fc41/`。

### D-110. ZF 臂：**写入停滞（非慢，是零进展）**

- 时间线：11:44:53 启动 fill → 11:45:00 之后 **LOG 再无任何新行**；11:50 取两次线程栈；
  11:52 终止（`fill_rc=143`）。写入停在 **2.6GB / 48M 键空间**。
- 现场（两次全线程栈，共 44 线程）：
  - 16 个 db_bench 写线程：15 个在 `WriteThread::AwaitState`（等组提交），
    **1 个（组长）在 `DBImpl::DelayWrite` 的 TimedWait**（标准写停顿路径，超时后重试，
    但条件始终不满足 ⇒ 循环无进展）；
  - **全部后台线程空闲**（`ThreadPoolImpl::BGThread` 等待），无 flush/compaction 在跑；
  - 主线程在 `Benchmark::RunBenchmark` 等待各线程收尾。
- LOG 末段（决定性证据，全部为候选线自身告警）：
  ```
  [WARN] ZeroFlush target-mode: output part=0 would land in L0 (level=-1) — yielding batch,
         L0 installs are forbidden
  [WARN] ZeroFlush target-mode: output part=1 would land in L0 (level=-1) — yielding batch, ...
  [WARN] ZeroFlush FIFO bypass blocked at install: part=1 oldest_pending=0 < install_min=2
  [WARN] [JOB 11] ZeroFlush empty-edit: submit VersionEdit without AddFile (pending=1079.5MB, lm=11)
  [WARN] [JOB 11] materialize install overlap detected; batched yielded to recovery
  ```
  ⇒ **pending 累积到 1079.5MB 且再也无法安装**：每个批次都"构造融合 → 让位 → 移交
  recovery"，没有一条路径能把该分区的 L1 重新变成可安装状态。
- 对照：**同一 workload/同一参数在 2026-09-14 的历史运行完成过**（`zf_50gb_noprobe`
  17:32→18:24 = 52min、32GB、656 个 SST；`merge_on_accept_v2` 31min、31GB）；
  当时代码含"积压排空/skip 收养"机制。

### D-111. 根因判断（源码级 + 已有实验分支证据；**待确定性复现确认**）

候选线缺少"**放弃融合时必须解除 `being_compacted`**"的修复：
- `ZfMaterializeJob::FinishPlansLocked()`（候选线源码实测）**只释放决策仍为 `kMergeBase`
  的计划**，且**没有**"构造即入台账 / 放弃即 unmark"逻辑
  （`grep registered_compactions_|abandon_compaction` 在候选线 = 0 处）；
- 而 `Compaction` **构造函数内部**就会 `MarkFilesBeingCompacted(true)`（实验分支
  backtrace 实证）⇒ 任何"构造后放弃/让位"的出口都会把该分区 L1 文件**永久**标记为
  `being_compacted` ⇒ 之后该分区每次规划都返回 `kCannotInstall`（正是日志里的
  `would land in L0 … yielding`）⇒ 让位 → recovery → 再让位，形成自持死循环，
  pending 只增不减 ⇒ 写路径被 DelayWrite 卡住。
- 该缺陷在实验分支已被修复并验证（台账 + `abandon_compaction`）：修复前
  `ZFDBG-mm-mark set=1` 无对应 `set=0`、case 1 排空 36 次收养空转；修复后配对成立、
  收养 1 次即完成。**该修复不在候选线**（候选线 = `0de022f` + 关闭刷盘 + case7 N/A）。

### D-112. 处理建议（按用户规则：候选线不再修改，修复从 tag 另开分支）

1. **从 `v0.6-candidate-c59fc41` 开新分支**，移植最小修复：`Compaction` 构造即入台账、
   放弃时 `MarkFilesBeingCompacted(false)`、`FinishPlansLocked` 按台账释放
   （与实验分支同源，已有单测级证据）。
2. 在**小规模确定性用例**上先复现本停滞（例如 16 分区、base_merge=true、固定范围路由、
   多线程写 + 强制让位），作为回归；再做 50GB 复测。
3. 复测通过前，**50GB/100GB 的 ZF 侧吞吐与延迟结论不成立**；native 臂数据仅作环境参照。
4. 100GB **不启动**（用户规则：ZF 未优于 baseline 前不进入更大规模）。

### D-113. native 臂参照（同一窗口、同一参数、无 ZF 开关）

```
fill:  48,000,000 ops  23,343 ops/s  23.2 MB/s  2056.2s(34.3min)
       P50 25.71µs  P75 32.54µs  P99 82.58µs  P99.9 374.99µs  P99.99 998,125µs(≈1s 停顿)
read:  128,000,000 ops 877,373 ops/s 18.09µs/op 145.9s
       P50 14.62µs  P75 19.60µs  P99 55.45µs  P99.9 75.92µs  P99.99 170.02µs
       (hit=6,161,212/8,000,000 = 77%)
DB: 32GB、SST 573 个（31.86GB）、WAL 1 个
```

**窗口可比性警告**：本次 native fill 用 34.3 分钟，而 **2026-09-14 同参数同机**的
`native_50gb_samewindow` 只用 17 分钟 ⇒ 本机当前吞吐约为当时的 50%（缓存/IO 状态差异）。
⇒ 跨窗口数字不可直接比较；后续 ZF/native 对照必须**同窗**（先后紧邻、同一 DB 盘）执行。

### D-114. 本轮结论

1. **ZF 臂无法给出 50GB 吞吐/延迟**：候选线 `c59fc41` 在该 workload 上写入停滞
   （D-110 证据：pending 1079.5MB、批次反复让位、组提交停摆、后台全空闲）。
   ⇒ 不满足用户的进入条件（"表现优于 rocksdb 再做 100GB"），**100GB 未启动**。
2. 停滞不是机器问题：同窗口 native 臂正常完成（D-113）。
3. 根因指向候选线缺失"放弃融合时解除 `being_compacted`"的修复（D-111），
   该修复在实验分支已存在并有单测级证据，**但不在候选线**。
4. 按用户规则（候选线冻结、修复另开分支）：**从 `v0.6-candidate-c59fc41` 开分支**修，
   先做小规模确定性复现（回归），再重跑 50GB；通过后才谈吞吐对比与 100GB。

---

# 受控运行 2：修复分支 `fix/c59fc41-abandon-unmark`（commit `ec096ab`）

### D-115. 交付物 1：最小补丁（不含实验分支的排空/攒批/收养）

| # | 文件 | 改动 | 目的 |
|---|---|---|---|
| ① | `zeroflush/zeroflush_db.cc` | `forbid_l0_install && routing != kStatic` ⇒ **自动关闭** + WARN | 禁 L0 目标模式只对固定 range 协议成立；非固定协议回退历史行为 |
| ② | `zeroflush/materialize_job.{h,cc}` | **构造即入台账** + 放弃即 unmark + `FinishPlansLocked` **按台账释放** | 统一释放责任：覆盖"未注册即放弃/注册后改判/提前返回/正常结束"；只释放本任务拥有的对象 |

①的位置修正（同源缺陷，单列）：互锁必须放在 `ZeroFlushContext` **构造之前**——ctx 持有
options 的**拷贝**；同函数内既有的 C1 互锁把 `merge_into_base_level=true` 写在构造**之后**，
对引擎**无效**（默认开启分支是空操作）。本轮只修 forbid_l0 这一条，C1 的同类问题记入待办。

### D-116. 交付物 2：确定性回归 `routingguard`（修复前失败 / 修复后通过）

用例：写入（默认 20 万键）→ 封存 → 正常关闭 → **重开** → 断言 `forbid_l0_install`
（非固定=0 / 固定=1）、`pending==0`、抽样逐键正确。

| routing | 修复前（仅回退引擎改动，同一用例） | 修复后（`ec096ab`） |
|---|---|---|
| sampled | **FAIL** `forbid_l0=1`；关闭前 pending=205MB、**重开后仍 209MB**（数据未安装） | **PASS** `forbid_l0=0`；重开后 pending=0；抽样读错 0/28572 |
| hash | **FAIL** 同上（202MB→209MB） | **PASS** 同上 |
| static | PASS（`forbid_l0=1` 目标模式不变） | **PASS**（未被扩大化） |

### D-117. 定向门禁（同一 build：`zf_valcheck 64812cad…`、`librocksdb.so.11 3dbd0469…`）

```
routingguard sampled/hash/static   PASS / PASS / PASS
installtest 2/3/4                  PASS / PASS / PASS
baccept 20000                      PASS（phase2/3/4，0 个 FAIL）
failaccept A/B/C                   PASS / PASS / PASS
all 6000 / verify / scan / triage   PASS（checked=9064 mismatch=0；scan mismatch=0；B2=0）
semtest 20000                      PASS (fails=0 setup_incomplete=0 n/a=1)
migrate A–H                        8/8 PASS
tailflush / detachreg              PASS / PASS（重开逐键 bad=0/4000）
closestress 1000                   **ok=1000 other=0**（本线关闭回归；drainlife 为实验分支专属，本线无此模式）
```

### D-118. 50GB 复测（修复分支 + **固定 range 路由**）：**仍在 13.4GB 停滞 → 阻塞点已前移并定位**

运行：`output/zeroflush_m5_perf/fix_50gb_static/`（`--zf_routing=static --zf_static_span=48000000`，
其余参数与 D-109 完全一致；`db_bench e59c7db6…` / `so 3dbd0469…`）。
- 前 1 分钟写入 **8.8GB**（≈150MB/s，远快于 sampled 臂的停滞曲线），随后放缓，**14:37 起
  在 13.4GB 完全停滞**（`du` 两次采样一致；现场 `hang_static13gb.LOG` + 线程栈
  `/tmp/fix50_hang_bt.txt`）。
- 线程栈（与上一次同构）：1 个组长在 `DBImpl::DelayWrite`（TimedWait 空转）、15 个写线程
  在组提交等待、后台线程全空闲、timer 线程等待。
- **新的决定性证据（本次停滞的直接原因）**：
  ```
  [WARN] flush_job.cc:1855 ZeroFlush FIFO bypass blocked at install:
         part=6 oldest_pending=25 < install_min=26
  [WARN] flush_job.cc:1908 [JOB 295] materialize install overlap detected; batched yielded to recovery
         flush_job.cc:1888 [JOB 295] ZeroFlush empty-edit (pending=717.4MB, lm=...)
  ```
  ⇒ **某分区的较老代（part=6 gen=25）没有任何批次在安装**，而 FIFO 规则禁止跳过它安装更
  新的代（`oldest_pending(25) < install_min(26)`）⇒ 该分区此后所有安装全部让位 ⇒ pending
  只增不减（停滞前已 717MB）⇒ 写入被 DelayWrite 卡死。全日志另有 `skipped N partition gens`
  116 次（规划期频繁让位）。
- ⇒ 结论：**路由互锁（①）与释放台账（②）都是必要的，但不充分**；下一个阻塞是
  "**被让位/滞留的旧代没有重新入队的执行者**"。实验分支的"收养 + 排空 work item"
  正是为此而建（候选线清洗时移出），按用户要求**不整包合入**。

### D-119. 待办（按优先级）

1. **下一处阻塞的定向修复**（范围要小）：为"FIFO 被卡的旧代"提供**重新入队**路径——
   仅复用正常封存/规划流程（不引入定时器/攒批），并配确定性小用例
   （构造 part 的 stranded gen → 断言后续安装可推进）。这是 50GB 能否完成的关键。
2. 同源缺陷：`zeroflush_db.cc` 中 C1 互锁对 options 拷贝的修改**在 ctx 构造之后**（空操作）
   ——需要与 ① 同样前移（或给 ctx 增加显式 setter）。
3. 已修复项保持：路由互锁、释放台账、关闭刷盘（`e7a6552`）、case7 N/A 口径。
4. 50GB 吞吐/延迟、100GB：**均等 1 完成并通过 50GB 后再谈**。

### D-120. 纠正 1 落地（commit `8141108`）：明确拒绝 + 规范化前移 + 有效配置属性

| 场景（`routingguard`，期望由 runner 给定） | 修复前 | 修复后（`8141108`） |
|---|---|---|
| sampled/hash + `EXPECT=reject` | **FAIL**（打开成功：不安全组合被静默接受） | **PASS**：`InvalidArgument: forbid_l0_install (target mode) requires fixed range routing (kStatic)…` |
| sampled/hash + 显式 `ZF_VC_ALLOW_L0=1`（`EXPECT=allow`） | PASS | PASS（重开后 pending=0、抽样读错 0/28572） |
| static + `EXPECT=target` | PASS | PASS：`effective_options=[routing=1 forbid_l0=1 merge_into_base=1 merge_explicit=1 partitions=4 skip_batching=1]` |

- **自动降级已删除**：不再有"告警后把 forbid_l0 改为 false"的路径。
- **选项规范化前移**：C1 的"目标模式默认开启融合"与拒绝检查都移到 `ZeroFlushContext`
  构造**之前**（ctx 持 options 拷贝）；`effective_options` 属性用于核对引擎实际值
  （上表 static 的 `merge_into_base=1` 即由该属性读出）。
- **撤回**：`ec096ab` 的"释放台账"**不是**本次 50GB 停滞的必要修复；保留为独立加固。
- 定向回归（`routingguard` 三期望）在修复前后均按上表运行；其余定向回归与 50GB
  复跑待第 2/3 步（旧代执行者追踪 + 事件驱动重新入队）完成后一并进行。

### D-121. 第 2/3/4 步的设计（下一步执行）

**第 2 步（先证明"无执行者"）**：新增最小状态台账与事件记录，用于追一个 `(part,gen)`：
- `epochs_in_flight_`（批开始登记、结束清除；失败/不确定单列）——回答"该代是否处于
  执行中/等待提交/提交不确定"；
- `OldestPendingGen` 命中时记录 `{part, blocking_gen, min_out_gen, blocking_epoch,
  in_flight?, retryable?}` 环形事件（属性 `zf_fifo_blocked_detail`）——FIFO 拦截现场；
- 与既有 `describe_gen` / `pending_gens`（归属）合并，形成
  "归属 → 入队 → 执行 → 让位/提交 → 后续调度"的完整链路。

**第 3 步（事件驱动重新入队 + 最小唤醒）**：触发点两处——让位批次完成归还后、
安装发现 FIFO 被较老代阻塞时。要求：可重试且**无执行者**的旧代才原子领取；已在飞/
提交不确定的不得抢；同代单执行者；建任务失败恢复归属；冲突走既有退避/完成通知，
不立即无限重排。**唤醒不得依赖下一次写入**：复用正常封存/规划流程（seal → 调度
批次），以一次性后台任务在让位点自触发（写线程此时已在 `DelayWrite`）。

**第 4 步（合法序列对照用例）**：写+封存 G → 在真实让位出口注入一次冲突（复用
`ZF_TEST_FORCE_INSTALL_CONFLICT`）→ G+1 尝试安装并被 FIFO 拦截 → 解除冲突并停写 →
**不手工排空、不关闭** → G 获得任务并安装 → G+1 推进 → 排空/回收/重启可读；
断言被挡批次未提前推进 fence/installed/drained、无重复消费。修复前暴露进展失败。

---

# 受控运行 3：第 2 步取证结果（**推翻"旧代无执行者"假设**）

### D-122. 小规模确定性再现 + 逐代链路取证（fix 分支，仅加诊断，未改调度）

**再现（无需冲突注入、无需 50GB）**：`fifostrand` base 阶段——固定路由 / 目标模式 /
4 分区 / 20 万键 → 写满键空间 → 连续 **8 轮"全部落封存 + 有界等待"**，`pending_materialize_bytes`
**恒为 28,229,376 B**（零进展）。这与此前 50GB 现场（`pending=717MB~1079MB`、批次反复让位）
是同一现象的小规模形式。

**逐代链路（新属性 `gen_chain:<part>:<gen>`，含归属/在飞/不确定/可重试/事件链）**：

```
pending_gens = n=6 bytes=24034272
  [3:12(3058752B,NONE+skip) 3:11(4195104B,NONE+skip) 3:10(4195104B,NONE+skip)
   3:9(4195104B,NONE+skip) 3:8(4195104B,NONE+skip) 3:7(4195104B,NONE+skip)]
fifo_bypass=0  skip_count=20  empty_edit=0  install_progress_failures=0
```

**关键事件（stderr / LOG）**：

```
ZFDBG-frz part=3 gen=13 size=0          ← 空尾也被封存并登记 epoch
ZFDBG-adopt skip_gens=6 epoch=50        ← 6 个 skip 代被收养进新 epoch（**有执行者**）
ZFDBG-scan pid=3 ... n=1 bytes=29008955 ok=1 bskip=0 l1n=4   ← 规划点：扫描 29MB base
```

**结论（与原假设相反，必须记录）**：
1. 被卡住的 6 个代**不是"没有执行者"**：它们被后续 epoch 收养（`adopt skip_gens=6`），
   进入正常规划（`ZFDBG-scan`）。归因为"旧代无执行者"**不成立**。
2. 真实机制是**设计内的"收益不足 ⇒ 攒批（skip）"在停写后无法收敛**：这些代（每代
   ~4MB，正是分区目标大小）在 plan 阶段被判**收益不足**而再次 kSkip（`skip_count=20`、
   `retryable` 语义上="可重试但每次都被规划器再跳过"）；要越过阈值必须有**新数据**
   让 `ratio = 新字节 / base 字节` 达标——而写线程此时已停（50GB 现场则已被
   `DelayWrite` 卡死）⇒ **进展依赖下一次写入**，正是用户指出的"不能靠下一次写入推进"。
3. 关闭路径之所以能排空，是因为 `closing_flush` 分支**强制融合**（`NoSkipMode`）；
   候选线清洗时移除的"积压驱动排空（drain）"正是停写场景下的那个触发器。
4. 50GB 现场的 `FIFO bypass blocked: oldest_pending=25 < install_min=26` 是**下游症状**：
   较老代长期停留在 skip/待安装集合，较新批次的安装被 FIFO 正确拦下。

**下一步（第 3 步，最小形态）**：事件驱动、**不依赖新写入**的"强制融合一次"排空触发器——
复用正常封存/规划/安装流程，仅在"批次结束且仍有未安装代逐次再被跳过"时触发，并以
**一次尝试是否有进展**做退避；不引入实验分支的定时器、收益攒批参数或 skip 收养整包，
不放开 L0，不推进 fence/installed/drained 之外的状态。

### D-123. 第 2 步第二次修正：skip 原因是 **`batchprev`（齐批占位/批内互斥）**，不是"收益不足"

`fifostrand` 卡住现场的 **逐原因计数器**（本线已有，无需新插桩）：

```
ZFDBG-skip no_merge=0 busy=0 batch_l0=0 l0busy=0 dirconf=0 **batchprev=8** bytes=0 sink=0 cross=0 act=1
fifo_bypass=0  skip_count=22  empty_edit=0  install_progress_failures=0
gen_chain: owner=NONE（skip 集合）queued=0 running=0 waiting_commit=0 commit_uncertain=0
           epochs_in_flight_total=0，事件链显示 epoch_queued→task_started→(无 finished/installed)
```

⇒ 唯一在增长的跳因是 **`batchprev`**：`materialize_job.cc` 的"齐批/占位"路径
（`gen0_ready && batch_planned_parts != nullptr`）在批开始前就把**全部分区**登记为
"已规划"，批内后序 epoch 因此对同分区一律 kSkip，注释里的既定假设是
"**下批前序安装后 vstorage 可见 → 正常融合/直装**"。停写之后"下一批"不再由新数据产生，
被跳过的代（本案例 part=3 的 gen 7..12，每代 4MB）就长期停在 skip 集合里。

**因此第 3 步的触发条件必须按本线实际情况改写**：本线（候选线）**没有**"收益不足 ⇒
收益攒批"这条分支——`materialize_job.cc` 里明确写着"step V0.6-A4（候选线）：收益攒批
**已移除**"。所以用户给出的触发条件"因'收益不足'再次 skip"在本线**永远不会命中**；
真正需要处理的是 **`batchprev` 占位让位在停写后无人接手**（以及 FIFO 拦截这一下游症状）。

**建议调整后的触发条件（最小、可判定）**：
```
触发：某分区的已封存代处于 skip 集合（owner=NONE+skip）
      && 该分区活跃 WAL 无未封存数据（ActiveSize==0 ⇒ 无写入在途）
      && 本轮批次对它只做了 batchprev/FIFO 让位（没有真实产出）
      && 该分区当前没有 drain attempt 在飞
动作：为**这些已存在的代**发一次 drain attempt —— 只做"绕过齐批占位/让位"这一步，
      其余（overlap 检查、文件占用、Compaction 注册、AddFile/DeleteFile、成功发布后
      才推进 fence/installed/drained）完全复用正常流程；冲突/输入未就绪时保留 skip
      并按已有退避等待。
```
这与用户 5 条约束一致（只绕过"让位等待"这一项，不放开 L0、不越过 FIFO 的正确性检查、
不引入实验分支的定时器/攒批/收养整包、不依赖新写入），但**触发判据从"收益不足"改成
"batchprev/FIFO 让位且无写入在途"**——这是本线实际存在的让位出口。

**状态**：诊断事件环与 `gen_chain` 均为取证工具、不参与判定；本轮**未改任何调度行为**；
候选 tag 仍冻结；50GB 未跑（按验收顺序，需 fifostrand 修前 FAIL / 修后 PASS 之后）。

### D-124. batchprev 推进实现 + 门禁状态（**RED，未验收**）

**已实现**（commit 见 fix 分支 `wip(zeroflush): batchprev 专用事件驱动推进 + 逐代取证`）：
- 取证：事件环（单调序号）/`epochs_in_flight_` 台账/`gen_chain:<p>:<g>`/FIFO 拦截现场链路；
- 推进：真实让位出口登记 + 批次结束**按状态补判**（`ActiveSize(part)==0` +
  仍有未安装已封存代 + 无 attempt 在飞）→ `TakeBatchprevDrainParts` 原子领取 →
  既有 `SealEpochAndSwitch`（收养 skip 代）+ `MaybeScheduleFlushOrCompaction` 唤醒；
  进展判据**严格下降**（`<=` 会空转：实测 `forced=154` 而 pending 不变）；
- 锁契约：挂钩点已持 DB mutex ⇒ 不再套锁、不调用自带加锁的 `FlushMemTable`（曾自死锁）；
- 回收：安装完成后 `pending==0 && registered>0` 时调用生产入口 `PurgeSealedFiles()`。

**实测（`fifostrand 200000`，4 次）**：
```
3 次: pending=0（等待 100 ms）、读错 0、重开读错 0、FIFO拦截 0、forced=0
1 次: 90s 窗口内 pending 完全不降（FIFO拦截 1）
reclaimed_bytes: **始终 0**（含 60s 回收等待窗）
```
⇒ **RED**：① 排空可达成但不稳定（同参数同二进制，3:1）；② 停写后旧代 WAL 未回收，
回收驱动未生效（需查调用是否到达 / `PurgeSealedFiles` 的前置条件）；
③ 因此不能作为"修前 FAIL / 修后 PASS"的可靠判据，50GB 不启动。

### D-125. 两项取证的结论 + 剩余缺口（RED 未解除）

**① `reclaimed=0` 分类**：目标代**已回收**——`DescribeGen` 7 字段显示
`file_exists=0 registry=absent in_unlink_queue=0 reclaim_pending=0 read_refs=0`，
`reclaimed=253,650,000`；`PurgePending` 分类计数已加
（`purge_reached/executed/empty/unlink_success`）。此前的 0 是**测量错误**：
缺回收等待窗 + 在 `db.reset()` 重开后读该实例专属计数（重开后归零）。
已改为"重开前取样 + 有界回收等待"。**未新增 purge 调用点、未手工 unlink。**

**② `batchprev` 锁存**：真实让位处无条件置位；消费需 `ActiveSize==0` + 无 attempt 在飞 +
仍有 skip gens；失败/无进展只退避并保留；**仅在无 skip gens 时清除**。

**③ 剩余缺口（精确定位）**：`fifostrand` 3 连跑 = 2 PASS + 1 停滞；停滞现场
`batchprev=12`（锁存已置位）但 `ZFDBG-bpdrain=0` ⇒ 消费点（挂在批次结束）**永不被到达**，
因为停写后不再有批次结束事件 ⇒ **必须有"锁存置位时刻"的一次性唤醒**（用户预案所称
"补最小唤醒机制"）。当前 **RED**：`fifostrand` 尚不能"修前稳定 FAIL / 修后连续稳定 PASS"。

**纪律**：50GB 未跑；候选 tag 未动；收益攒批/实验调度器未引入。

### D-126. `fifostrand` 确定性前置 + 三次一致的现场分类（**仍未通过**）

**前置（用户选择 B 的钩子）**：`ZF_TEST_PAUSE_AFTER_PLAN_PART=<part>`（仅修复分支、env 门控、
生产零开销）——目标分区第一个 epoch 规划并登记 `batch_planned_parts` 后暂停；同一批的
第二个 epoch 继续规划 ⇒ **batchprev 必现**。三次运行 `paused=1`、`reskip=10~13`、`latch=4` ✓。

**现场分类（同实例取样，3 次一致）**：

```
pending_gens = n=9 bytes=32,424,480 → 全部 partition 0，全部 owner=epoch:64
skip_reasons = batch_prev=8/11/10（唯一增长项）bytes=0 skips=30/37
fifo_blocked_detail=(none) fifo_bypass=0
batchprev_latched=4 reskip=8/11/10 forced=0 backoff=1
wakeup req=40 coal=6~9 started=40 consumed=0
```

**结论**：残留既不在别的分区、也不是 FIFO 拦截；是 **partition 0 的 9 个代被 epoch=64 持有而
该 epoch 没有批次**。唤醒任务已执行 40 次却零进展 ⇒ 缺的环节是"**把被 epoch 持有但无批次
的代数重新变成可规划**"，而不是改消费映射（按用户指示未改）。附带异常待查：
`wakeup started=40` 时 `consumed=0 && no_progress=0`（疑似属性与计数器映射不一致）。

**状态**：修复分支 RED；候选线与原 tag 冻结；50GB 未启动。

### D-127. 孤儿转移：诊断拆分命中分支 (a)，根因收敛到"陈旧归属引用"

```
orphan_diag = orphans_moved_gens=0 handoffs=0 consumed_gens=0 adoption_epoch_id=0
              | snap_epochs=1 snap_max=43 moved=0
flight_ledger: in_flight=[] uncertain=[]
pending_gens → 9 代全部 owner=epoch:64；gen_chain: refs(epoch=64)=1（归属引用）
```

⇒ `EpochSnapshot`（`SealedFileCache::epochs_`）**看不到 epoch 64**（只见 43），而残留代被
`gen_refs_` 里一条**指向不存在 epoch 的归属引用**钉住 ⇒ 无批次规划它、转移枚举也覆盖不到它。
`refs(epoch=64)=1` 已确认为**归属引用**（非在飞任务引用），与 `flight_ledger=[]` 不矛盾。

**下一步单点修复**：把转移枚举从"活着的 epoch"扩展到"**被无主归属引用钉住的代**"，
解引用后走既有可采纳入口 → 一次封存收养 → 正常规划/安装。约束不变（不复制 gens、不推进
installed/fence/drained、不手工 unlink、不放开 L0、不引入定时器）。修复分支 `RED`；
候选线与原 tag 冻结；50GB 未启动。

### D-128. bench-100g：找到"收养 epoch 永不进批次"的真正缺口（封存后不调度）

事件链实证：`epoch_queued epoch=57/63/64` 之后**没有 `task_started`** ⇒ 恢复路径封存出的
epoch 没有批次规划。根因：`ZfSwitchMemtable` 只 `EnqueuePendingFlush`、**不触发调度**；
停写后无写路径调用调度器 ⇒ 新 epoch 永不进批次 ⇒ 恢复路径反复"转回 recovery + 再封存"
（自造空 epoch 的自持循环，与此前 50GB 现场同源）。

**修复（bench-100g，一行）**：入队后 `MaybeScheduleFlushOrCompaction()`。
**实测**：`fifostrand 200k`（门控开）`pending=0`（此前 4 轮 32.4MB 不动 90s）、
`reclaimed=234~238MB`、实时/重启读 0 错 ⇒ **停写后自动排空达成**；
`orphans_moved_gens=0 / consumed=0` ⇒ 悬空引用转移未被需要。
待办：① 分类仍登记的 8~9 个代（疑为活跃/空代）；② 判据按实际路径更新后跑满 3 次；
③ 之后才按顺序做 10GB smoke 与 100GB。候选线与原 tag 冻结。

### D-129. 100GB 全量运行：**进展停滞（INCOMPLETE，非通过）**

**运行身份**：`bench-100g` 工作树、`db_bench e59c7db6…`、`so ce9372b2…`、
`ZF_BENCH_ORPHAN_RECOVER=1`、固定 range 路由（`--zf_routing=static --zf_static_span=100000000`）、
`--zf_base_merge=true`、`--num=100000000`（16×6.25M 写）。目录 `output/zeroflush_m5_perf/bench100g_full/`。

**现场**：
```
启动 22:02；58 分钟只写入 ≈12.8GB（du 两次采样完全相同）
LOG 自 22:52:24 起不再增长（size 5,598,696 不变）；进程 30% CPU（空转）
线程栈：15×AwaitState（写线程等组提交）+ 1×DelayWrite（组长空转）+ **26×BGThread 空闲**
        + Timer + RunBenchmark
ZFDBG-skip: no_merge=0 **busy=118** batch_l0=0 l0busy=0 dirconf=0 batchprev=0 sink=332 act=4
ZFDBG-batch [JOB 364] tasks=0；ZFDBG-ctxhandoff epoch=125 gens=4
compaction: start summary/started 184 次、finished 184 次 ⇒ **没有"启动未结束"的 compaction**
ZFDBG-sched: bgc=2 → 之后 bgc=0（无在跑后台任务）
```

**判读**：写路径停在 `DelayWrite` 且**没有任何后台任务在跑**；规划侧持续以 `busy`（目标层
文件 `being_compacted`）让位 ⇒ 存在**没有持有者的 `being_compacted` 注册**（泄漏），
与 50GB 现场同类。该类缺陷在实验分支用"构造即入台账 + 放弃即 unmark + 按台账释放"修复过，
**本分支按用户范围未移植**。

**结论**：100GB **未跑通**（写入未完成 ⇒ 四项收尾无从判定），按 INCOMPLETE 记录；
现场保留（LOG/线程栈），下一步定位"谁标记了这些 L1 文件"（`zf_sink_diag` 的 MergeHold /
`ZFDBG-sink-*`、sink 预选路径），再决定是否把台账式释放按最小范围移植到 `bench-100g`。
候选线与原 tag 未动。

### D-130. 双组取证（只读）：**不是 being_compacted 泄漏，是 ZF 账本背压无人推进**

**构建身份**：`db_bench 207bdf40…` + `so f84588cd…`（含只读属性 `stall_snapshot` 与
db_bench 探针 `--zf_stall_probe_ms`）。复现：`bench100g_med2/med3`（30GB 参数，~12GB 处
连续 3 次停滞；看门狗在"体积与 LOG 均不变 ×3"时抓现场并终止）。

**① 限写条件（停滞当刻快照，207 次采样）**
```
needs_delay=0  is_stopped=0            ← 写控制器**从不**要求延迟
unsched_flush=0 bg_flush_sched=0 bg_comp_sched=0 flush_queue=0  imm_not_flushed=0
L0_files=0 L1_files=12 L2_files=72 L2_bytes=4.28GB
zf_pending=861,140,148B（连续 4 次采样冻结）  zf_registered=35  zf_epochs_in_flight=0
zf_batchprev_latched=0  zf_pending_unlink=0
```
**② being_compacted 文件**：仅 9/207 次快照非空，且**全部是 L2 文件、`hold=0`**（原生
compaction 在飞，正常）；**停滞前最后一个快照为 `[-]`（无任何占用）** ⇒ 与历史 141 条
`busy-reject`（均为 L1、`merge_hold=0`）**没有交集**。

**结论（命中用户的判定分支）**：`being_compacted=false` ⇒ **停止追踪泄漏**；
`needs_delay=0` ⇒ 不是原生写控制器。真正的阻塞点是 `DBImpl::DelayWrite` 里的
**ZF pending 账本背压**（`db_impl_write.cc:3063+`）：
```cpp
while (PendingMaterializeBytes() > zf_resume) {       // 实测 861MB > resume
  bg_cv_.TimedWait(+1ms);                              // 唯一唤醒源 = drained 增长（真实安装归还）
  if (drained 增长) 重置 deadline; else 超时返回 Incomplete("ZF pending backpressure timeout");
}
```
停滞当刻 `epochs_in_flight=0 / unsched_flush=0 / flush_queue=0 / imm=0` ⇒ **没有任何批次会
推进 drain** ⇒ drained 永不增长 ⇒ 写线程只能在 1ms 时间片里空转到 `zf_timeout`（默认 15s）
后返回 `Incomplete`，db_bench 重试 ⇒ 表现为"进程活着、CPU 空转、写入不完成、pending 冻结"。

**修复方向（最小、与已接受的两处驱动同族）**：在背压等待的阻塞分支里**补一次调度驱动**——
当 `pending ≥ stop` 且"零在飞/零排队"时，触发既有的"封存 + `MaybeScheduleFlushOrCompaction`"
路径（即把 batchprev 唤醒用的同一入口，改为也由**账本水位**触发），使 drained 能增长、
wait 能退出。不改水位语义、不引入定时器、不做手工 unlink、不放开 L0。

**状态**：100GB = INCOMPLETE/FAIL；候选线未修改；现场保留
（`bench100g_full/hang_12_8GB.LOG`、`med/med2/med3` 的 fill.log 与快照）。

### D-131. 背压驱动：已实现（四条约束逐条落地）但**驱动路径崩溃 ⇒ 不得合入**

**实现**（`bench-100g`）：
1. `DelayWrite` 的等待分支**只登记需求 + `env_->Schedule` 入队**，不做同步封存（核对：
   `DelayWrite` 由**写组长**调用、持 DB mutex；等待期 `bg_cv_.TimedWait` 释放 mutex）。
2. 进入看 stop、持续推进看 `pending > resume`、`pending ≤ resume` 才 `ClearPendingWatermarkDemand()`。
3. 独立原因 `pending_watermark`（不要求 `batchprev_latched>0`、不要求 `ActiveSize==0`），
   调度器与唤醒任务都认这个原因；任务在**水位原因**下走"生成真实批次 + 调度"。
4. 任务结束后按 `PendingAboveResume()` 复检并保持/清除需求；无进展期限只由真实安装
   （drained 增长）重置（未改这段语义）。

**实测（低水位小用例 stop=256MB/resume=192MB，2GB）**：
```
驱动开启：fill_rc=139（SIGSEGV）3/3（gdb 下 1/1 正常跑完、end-state 打印完整 ⇒ 竞态）
驱动禁用（ZF_BP_DRIVER_DISABLE=1）：rc=0 3/3，257k/252k/258k ops/sec  ⇒ **崩溃来自驱动路径**
```
已排除的两个候选（改动后仍崩）：后台线程直接 `SealEpochAndSwitch`（切表需写线程上下文）、
写入活跃时调用 recover/handoff。**剩余最可疑机制**：水位分支里由后台任务调用
`FlushMemTable`（内部 `EnterUnbatched` 需写线程空闲），而**写组长此刻正阻塞在
`DelayWrite` 的等待中、仍占着写线程槽** ⇒ 与用户约束 1 的担心一致（自等/非法进入）。

**下一步（定点）**：水位分支不调用 `FlushMemTable`；改为在**持锁**下只做
"`EnqueuePendingFlush`（若已有 imm）+ `MaybeScheduleFlushOrCompaction`"，需要封存时
由**批次侧**完成（不在 leader/任务里切表）。随后：低水位小用例 ×3 → 同参数 30GB
（跨越 ~12GB 停滞点）→ 100GB。候选 tag 未动；100GB 仍为 INCOMPLETE/FAIL。

### D-132. 崩溃归因更正 + 探针修复 + 背压验收口径

**更正（撤回 D-131 的归因）**：SIGSEGV **不在背压驱动**，而在**我自己的诊断探针**
（`--zf_stall_probe_ms` 每 2s 读 `stall_snapshot`）。四组对照：

| 驱动 | 探针 | 结果 |
|---|---|---|
| 开 | 开 | fill_rc=139（SIGSEGV）3/3 |
| 关 | 开 | rc=0 3/3 |
| 开 | 关 | rc=0 3/3（258k/241k/266k ops/s） |
| 开 | 关（手动 2 次） | rc=0 2/2 |

原因：`DBImpl::GetProperty` 对本属性**不持锁**（实测源码），探针在无锁下遍历
`VersionStorageInfo::LevelFiles`/`FileMetaData`，与并发安装竞争；且探针线程此前**未 join**，
可能在 DB 析构后仍调用 `GetProperty`（use-after-free）。

**修复（用户第 1 条）**：
1. `stall_snapshot` 改为**持 DB mutex 复制标量**（`being_compacted` 只复制 level/number/size），
   解锁后只打印副本，**不保留/解引用任何 Version/FileMetaData 指针**；
2. 探针线程增加 `stop` 标志并在 DB 销毁前 **join**（`Benchmark::Run` 的线程收尾处）。

**验证（原始复现条件：驱动开 + 探针开，3 连跑）**：**rc=0 3/3**（245k/249k/247k ops/s），
`peak_pending_bytes` = 215.7M / 215.7M / 296.1M。按用户口径记：
**"探针缺陷已修、原复现条件未再崩溃"**，暂不声称已完全证明 SIGSEGV 来源（无 core、gdb 下不复现）。

**背压验收口径（用户第 2 条）**：
- 新增累计量 `backpressure_entered`（进入 ZF 背压次数）与既有 `peak_pending_bytes`；
- 本轮三跑中**两次峰值 215MB < stop=256MB ⇒ 未进入背压**，只有第三次 296MB 越线 ⇒
  按用户规则这两次只能标"**未覆盖**"；要确定性覆盖需把 stop 降到峰值以下（或用更大写入量），
  并**在结束时读取** `backpressure_entered`/`peak`（db_bench 结束态目前不打印该属性，
  需补进打印集合或用校验器读取）。

**零 imm 入口（用户第 3 条，已批准，未开始）**：从 `FlushJob` 抽取最小共用执行段，使既有
sealed gens 成为真实任务输入；保留原子领取/同代单执行、WAL 扫描引用与 Version/Compaction
生命周期、冲突检查与禁 L0、输出与 fence 同 edit 提交、失败/让位/取消/不确定处理；
**只解除"必须先有 imm"的调度依赖，不切表、不等被背压阻塞的写组长**。

### D-133. 记账绑定实验：门禁式改法被否（且发现当前树有回归）——必须回到 3de900a 基线再推进

**本轮动作**：按用户第 2 点的最小形态，把"输出确在当前 Version"从**辅助断言**升级为
**销账门禁**（用于堵 `TryInstallMemtableFlushResults` 的 `commit_in_progress_` 早返回：
返回 OK 但未发布 ⇒ 提前销账）。

**实测**：
```
门禁版：fifostrand rc=124（超时）
撤销门禁后（恢复原辅助断言）：fifostrand rc=124（**仍超时**）
撤销门禁 + 驱动禁用：fifostrand rc=124（**仍超时**）
卡点：前置已成立（③batchprev reskip=9 latch=4），随后 90s 等待窗内未排空；
      日志停在一条正常 `ZFDBG-install`（JOB 50）之后
```
⇒ **两点结论**：① 门禁式改法**不能**据此判否——超时与它无关；② **当前树在 fifostrand 的
排空阶段存在回归/抖动**，最后一次已知全绿是 `3de900a`（fifostrand 3× PASS），其后引擎侧
改动只有：背压驱动（`06a1654`）、`backpressure_entered` 计数、探针持锁（`128687e`）。

**下一步（有界，不再试错）**：
1. 先在 `3de900a` 复跑 fifostrand 3× 复核基线；
2. 把 post-3de900a 的引擎改动**逐项**带回，每项都以 fifostrand（3×）为门禁；
3. 基线恢复全绿后，才按用户第 1/2/3 点实现"安装上下文独立 + 提交点记账 + sealed-only 入口"，
   再跑第 4 点的验收链（进入背压 → 领取 → 实际发布 → pending<resume → 写恢复 → 停写排空/回收/重启）。
候选 tag 未动；30GB/100GB 暂停。

---

# ZeroFlush V2：50GB 端到端受控运行（**完整跑通**，2026-10-16/17）

> 背景：V2 = 固定范围分区 + 无动态分区 + 分区 WAL 直写 + 与 L1 融合出 SST
> （设计/交付见 `source/rocksdb-zeroflush/zeroflush/V2_DESIGN.md`）。本节只记 50GB 运行。

### D-134. 配置（与 D-109/D-118 同口径，路由换 V2 固定范围）

```
--benchmarks=fillrandom,waitforcompaction --key_size=16 --value_size=1024
--num=48000000 --writes=3000000 --threads=16 --seed=20260914 --compression_type=none
--cache_size=536870912 --max_background_jobs=24 --subcompactions=16
--zeroflush --zf_v2=1 --zf_routing=static --zf_static_span=48000000
--zf_partitions=16 --zf_epoch_target_mb=256 --zf_partition_target_mb=64
--zf_base_merge=true --zf_value_cache_mb=64                ZF_L0_TRIGGER=100000000
```

### D-135. 结果：**写入全部完成 + waitforcompaction OK（DB_BENCH_EXIT=0）**

| 指标 | 数值 |
|---|---|
| fillrandom | 48,000,000 ops / **8,756s = 5,481 ops/s（5.4 MB/s）** |
| 延迟分位 | P50 **42µs** / P75 111µs / P99 4.3ms / P99.9 25ms |
| 末态 LSM | **L0=0**、L1=429、L2=70、L3=72 |
| 账本 | `drained=50.31GB`、`reclaimed=49.86GB`、`reclaimed_gens=2234`、`installed_epochs=380` |
| 退出时残留 | `pending=952MB`、`registered=66`（waitforcompaction 不驱动 ZF 排空；**重开后 pending=0/registry=0**） |
| 写放大 | `prof_output_bytes=1.344TB / 50GB = 26.9×`（融合重写整段 L1） |
| 读验证（重开） | `readrandom` 8M 次：**315,965/500,000 found=63.2%**（= 48M 随机写覆盖 48M 键空间的理论期望 `1-e^-1`，同历史 63.26%）⇒ 无丢失；读 **119k ops/s**（P50 289µs 冷缓存） |

### D-136. 三轮失败与修复（每轮都有现场证据）

1. **原生 L1→L2 抢文件 ⇒ 停滞**（18–19%）：`ZFDBG-skip busy=35`、批次 `tasks=0`、
   pending 1.03GB、`empty_edit=19`、imm 8→9、stdout 6 分钟无进展。
   修复：V2 档**独占 L1**（`max_bytes_for_level_base=1PB`，禁原生 L1→L2）。
2. **关掉收益攒批 ⇒ 每 epoch 重写整段 L1**：`ZFDBG-slowmerge` 486 次、单批 merge
   6–35s、物化 ~5MB/s、周期性停写。修复：V2 档强制攒批（代数 8）。
3. **攒批阈值 > 背压水位 ⇒ 闭环死锁**（30.9%，抓栈）：45 线程全睡 `futex_wait_queue`
   零 CPU、15 写线程 `AwaitState`、组长 `DBImpl::DelayWrite`（账本等待
   `db_impl_write.cc:3073`）、**全部后台 worker 空闲**、账本 882MB 冻结 >40min。
   修复：阈值**由水位推导**（`min(ledger_stop/4, 512MiB)`，水位 V2 默认提到 8GiB）
   ⇒ 排空必在背压之前可触发。修复后同配置**完整跑通**（D-135）。

### D-137. 遗留（记录在案，未修）

- 吞吐受写放大限制（26.9×）；L1 消费靠 ZF sink，长跑中与 L2 清场（默认 4GB 阈值）
  互相让位 ⇒ 吞吐振荡（`l2_clear_rounds=52`、`pick_ok=136`）。下一轮调
  `ZF_SINK_L2_CLEAR_MB` 或让 sink/清场解耦。
- 6 次 50GB 运行中 **1 次 SIGSEGV**：13 秒处，最后日志 `ZFDBG-adopt skip_gens=16`
  （封存期收养 skip 代路径，属实验分支未提交改动，非 V2 代码）。
- 现场与留档：`/tmp/zf_v2_50gb_run.log`、`/tmp/zf_v2_50gb/LOG*`、
  `/tmp/zf_deadlock_stacks_ledger_vs_threshold.txt`、`/tmp/zf_v2_progress.log`；
  50GB 库保留在 `/tmp/zf_v2_50gb`（34GB，577 SST）。

---

# ZeroFlush V2：50GB 性能优化 —— **超过同机原生基线**（2026-10-17）

### D-138. 基线（同机、同窗口、同参数、同 workload）

原生 RocksDB（`source/rocksdb/build/db_bench`，vanilla 11.2.0，Release）：
**16,587 ops/s（2,893.7s / 48M ops，16.5MB/s）**，P50 26.61µs、P99 131.36µs、exit 0。
（比 9 月历史 26.4k 低——本轮环境更差/更真实，故以本次同窗口测量为准。）

### D-139. 瓶颈定位（三轮取证）

1. **安装准入锁把"规划+归并+安装"整批串行化**（`flush_job.cc` 准入循环 + `install_mu_`）：
   抓栈显示同刻只有 1 批在归并、批内 3–5 个分区任务 ⇒ 全程仅 ~1 核 CPU（36 核机器），
   并行度被锁死，不是算力/带宽瓶颈。
2. **下沉（L1→L2）与融合乒乓重写**：`kRangeMergeBytes=64MiB`（硬编码让位阈值）让
   每次规划都触发下沉让位；下沉又把 L1 切片合并进 L2 并**反复重写整段 L2 切片**
   （实测 500MB/次）⇒ 瞬时写放大 20–40×、5–16k ops/s。
3. **L1 切片的整段重写**是"禁 L0"设计的固有代价：LSM 层内不重叠 ⇒ 融合只能重写
   该分区的整段 L1 切片。量化：`总重写 ≈ S²/(2·P·每轮累积)`（S=总量、P=分区数、
   每轮累积=攒批字节）⇒ 想赢就必须把"每轮累积"做大。

### D-140. 获胜配置（v7）与结果

改动：`kRangeMergeBytes` → 选项 `sink_yield_threshold_bytes`；V2 档默认：
累积 32 代（`base_merge_skip_max_generations=32`，2GiB/分区）、水位 32GiB/24GiB、
在途 `max_pending_epochs=128`、让位阈值 = 水位/2（≈不下沉）、K=16、`ZF_SINK_YIELD_MB` 可覆盖。

结果（v7）：**23,194 ops/s（2,069.4s）**，P50 37.5µs，exit 0；
读回 315,384/500,000=63.08%（= 期望覆盖 `1-e^-1`）⇒ 无丢失；L0=0、L2/L3=0。

中间对照（每格均为同机 50GB 实测）：v1（L2 清场 64GB+K16）≈15k 衰减｜v2（1GB epoch+16 分区批）
停滞（安装重叠让位）｜v3（分区目标 8MB）6.6k｜v4（让位阈值 1GB）11.9k｜
v5（禁下沉）峰值 18.4k、34% 崩溃（间歇 SIGSEGV）｜v6（+在途 32/水位 16GiB）≈16k 尾部衰减。

### D-141. 固化后的默认值复现（v8）：**27,698 ops/s = baseline 的 1.67×**

命令仅用 V2 档默认值（外加 `--zf_partitions=16 --zf_partition_target_mb=64
--zf_epoch_target_mb=256 --zf_materialize_parallelism=16`）：

```
fillrandom : 27,698 ops/s  1,732.9s  48,000,000 ops  27.5 MB/s   (exit 0)
P50 35.64µs / P75 46.47 / P99 147.10 / P99.9 823.74 / P99.99 598,230
读回（重开）：readrandom 8M 次 = 315,740/500,000 found = 63.1%（期望覆盖）→ 无丢失，138,939 ops/s
末态：L0=0、L2=0、L3=0（数据全在 L1=480 文件）；pending=0；重开 registry=0
写放大：prof_output_bytes=281.9GB / 50GB = 5.64×（首版跑通 26.9×，v4 时期 20–40×）
空间：DB 30GB（原生同负载 ≈40GB）
```

**遗留**：① 大累积 ⇒ 在途 WAL 峰值 26.6GB、imm 上限 129，内存/磁盘占用高于原生；
② P50 写延迟 35.6µs vs 原生 26.6µs（写入要让出空间给 WAL/索引）；
③ 安装锁的批内串行化未改（并行度仍受"批内分区任务数"限制），若解除可再上一个量级；
④ 间歇 SIGSEGV（~1/8 运行，工作树既有 skip 代收养路径，未修）。
留档：`/tmp/zf_v2_def_run.log`、`/tmp/zf_v2_def/LOG*`、`/tmp/native_50gb_run.log`、
`/tmp/zf_v2_opt7_run.log`、`/tmp/zf_opt5_stall_stacks.txt` 等。

---

# ZeroFlush V2：Zipfian 分布负载测试（2026-10-17）—— 写 1.65× baseline，**发现关库路径数据丢失**

### D-142. 新工具与时序无关性

本 fork 的 db_bench **没有 zipfian 基准**（Zipfian 只在 db_stress 里）。新增
`tools/zf_zipf_bench.cc`（CMake 目标 `zf_zipf_bench`）：
- 键：`P(rank k) ∝ 1/(k+1)^alpha`（默认 0.99，1M 桶 CDF，桶内均匀）；
- 值：`value = f(key)`（确定性 ⇒ 任意读回可精确校验）；
- **同一 seed 下 V2 与原生拿到完全相同的键序列**（原生版用 `-DZIPF_NO_ZF=1`
  编译，链接 vanilla `source/rocksdb/build/librocksdb.so`）；
- 读阶段**重放同一键序列** ⇒ 每个读键必然写过 ⇒ found 必须 100%、mismatch 必须 0；
- 每 10s 进度（stderr）+ 1µs 分辨率延迟直方图。

### D-143. 结果（同机同窗口；keyspace=48M，48M 写 × 1KB，alpha=0.99，16 线程，8M 读）

| 指标 | 原生 RocksDB | ZeroFlush V2 | 对比 |
|---|---|---|---|
| 写吞吐 | 38,274 ops/s（1,254.1s，39.2MB/s） | **63,007 ops/s（761.8s，64.5MB/s）** | **1.65×** |
| 写 P50/P99/P99.9 | 28 / 82 / 224 µs | 54 / 171 / **2,367** µs | 中位略慢、尾部明显更差 |
| 读（写后首轮） | **903,040 ops/s**（P50 13µs） | 17,746 ops/s（P50 660µs） | **0.02×（冷+写后状态）** |
| 读（同库第二轮） | — | 209,427 ops/s（P50 73µs） | 4.3× 落后 |
| 校验 | PASS（100% found） | PASS（100% found，0 mismatch） | 两者键流相同 |
| DB 大小 | 12GB | **4.9GB** | 空间更省 |
| 写放大 | — | `prof_output_bytes=124.6GB/50GB = 2.49×` | 均匀负载为 5.64×（覆盖写在融合时被折叠） |
| 末态 | — | L0=0、L1=176、L2/L3=0、pending=0 | — |

结论：**Zipfian 写负载下 V2 仍快于原生（1.65×）**；读路径落后 4.3×（热）到 50×（冷），
且尾部延迟（P99.9 2.4ms）明显差于原生（0.22ms）——读数与写放大相互独立。

### D-144. **严重问题：关库路径的数据丢失（确定性复现）**

序列（`keyspace=2M, ops=2M, alpha=0.99, 16 分区, partition_target=8MB,
epoch_target=64MB`，工具每次"打开→写 16 键→读→关闭"）：

```
cycle1: DB 364MB, found=2,000,008/2,000,008   ← OK
cycle2: DB 364MB, found=2,000,008/2,000,008   ← OK
cycle3: DB 83MB,  found=272,336/2,000,008     ← 86% 丢失（2/2 复现）
```

cycle3 的 LOG：
```
13:14:32.862 Deleted file .../000487.sst
13:14:32.866 [JOB 4] materialize 11 partition tasks failed:
             Corruption: ZF merge scan: gen neither registered nor provably installed
13:14:32.866 [WARN] ZeroFlush close-flush failed: Corruption: ...
13:14:32.997 CloseHelper: ... bg_flush_sched=1
```

机制（证据链）：skewed 负载下 L1 被物化为若干大切片；关闭时的尾 epoch 物化批次在
`MaterializeMergePartition` 的代身份防御检查处失败 → **该批输入的 L1 文件已被删除
（364MB→83MB）而输出未安装** → close-flush 失败 → 后续重开只能看到 WAL 重放的部分
（13.6%）。**已排除**：1M keyspace 的 5 轮循环（正常）、均匀 50GB 验收库的 4 轮
打开（仍 63.3% 期望命中）。50GB Zipfian 库（`/tmp/zipf_zf`）同样命中：第 3 次
打开后 87% NotFound。

关联代码：关闭刷尾（`ZfCloseFlush`）+ 物化批次失败处理 + 代身份/fence 账本
（工作树未提交的 A11/A12 区域）。**这是数据安全问题，优先级高于本轮的吞吐结果**；
下一步需要把"批次部分安装 + 失败清理"做成全有或全无（或失败时保留输入），并加
"关闭→重开×3"的回归门。
留档：`/tmp/zipf_repro2/LOG.old.1792214074033447`（失败现场）、
`/tmp/zipf_native_run.log`、`/tmp/zipf_zf_run.log`、`/tmp/zipf_repro2/`。
