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
