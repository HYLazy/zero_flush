# step 0b-3（第一阶段）：pending ledger 水位背压

> 目标：把"已封存未安装"的数据纳入可观测账目，并在写路径（seq 分配前）用
> native `DelayWrite`/`bg_cv_` 通道施加水位背压，超时可重试逃生。
> 状态：**第一阶段完成并冒烟验证**；空 edit（第二阶段）与 9 项故障用例
> （第三阶段）待做。

## 1. 落地内容

| 位置 | 改动 |
|---|---|
| `zeroflush_db.{h,cc}` | `ZeroFlushContext` 新增 pending ledger：`ledger_`（键 = part<<32\|gen，值 = {bytes, epoch, state}）＋ `ledger_pending_bytes_` / `ledger_drained_bytes_`（单调）/ `ledger_max_pending_bytes_`（峰值）/ `ledger_orphan_bytes_` / `ledger_installed_epochs_`；API `LedgerSeal` / `LedgerHandOff` / `LedgerAttachOrphans` / `LedgerReownEpoch` / `LedgerInstallEpoch` / `LedgerDescribePending` |
| `zeroflush_db.cc` 挂钩 | 封存登记（`RegisterSealedEpoch`：逐 (part,gen) 只计一次 → 收养 → cache 登记）、移交（`HandOffSkippedToRecovery` 前 `LedgerHandOff`）、恢复（`Recover` 的孤儿代入账，epoch=0 待收养）、释放（`ReleaseEpoch` 兜底） |
| `db/flush_job.cc` | 安装成功点（`SetLastMaterializedEpoch` 同处）调 `LedgerInstallEpoch`；每个 epoch 规划前调 `LedgerReownEpoch` |
| `db/db_impl_write.cc` | `PreprocessWrite` 把 ZF 水位并入"是否需要 DelayWrite"判定；`DelayWrite` 顶部新增水位等待块（1ms 时间片、`bg_cv_.TimedWait`、进展判据 = 单调 `ledger_drained_bytes`、超时返回 `Status::Incomplete`） |
| `ZeroFlushOptions` | `ledger_stop_bytes=1GiB` / `ledger_resume_bytes=0.75GiB` / `backpressure_timeout_micros=15s`（机制验证值；15s = max(15s, 2×实测 drain 5.5–6.3s)，采纳用户修正——5s 会把正常排空误判为超时） |
| db_bench | 新增 `--zf_ledger_stop_mb` / `--zf_ledger_resume_mb` / `--zf_bp_timeout_ms`；打印 ledger 各项与 `zf.bp_retries`；**fill 写调用把 `Status::Incomplete` 当可重试**（重试 + 计数） |

## 2. 实现中发现并修掉的四处账目缺陷（都不是设计问题，是挂载点错位）

1. **收养未回挂**（冒烟残留 246MB）：被移交的代标记为"待重规划"后，新 epoch
   收养它们时没有重新挂账 → 其所属 epoch 安装时清不到，永久滞留。
   → 修：`RegisterSealedEpoch` 内 `LedgerAttachOrphans(se.epoch)`，与 cache 的
   `AddEpochWithRecoveryAdoption` 同一语义窗口。
2. **让位后同 epoch 重试未重新认领**（残留 27.5MB）：让位批次回滚后由同一
   epoch 重试并成功安装，但代的归属仍是"已移交"状态。
   → 修：规划每个 epoch 前 `LedgerReownEpoch(epoch)`。
3. **安装点选错**（残留 267MB，最关键）：最初把"已安装"挂在
   `ZeroFlushContext::ReleaseEpoch`（imm 析构）。但 imm 析构依赖 SuperVersion
   换代，**关闭前可能永不发生**——于是已装进 L1 的数据仍被计为 pending。
   → 修：安装点改到 `flush_job.cc` 的安装成功处（`LogAndApply` 之后）。
   这条也直接决定了水位语义：pending = "尚未进 L1"，与该定义一致。
4. **恢复期孤儿未入账**：重开后磁盘上已有的未物化代不在账目里。
   → 修：`Recover` 登记 recovery 孤儿时同步 `LedgerSeal(..., epoch=0)`。

## 3. 冒烟验证（1.6M 写 / 8 线程 / 16 分区 / 14 亿… 见下）

| 用例 | 配置 | 结果 |
|---|---|---|
| A 默认水位 | stop=1GiB | pending **0** / entries 0 / orphan 0 / drained 1.45GB / 峰值 332MB；`waits=0`；253K ops/s |
| B 低水位 | stop=32MB, resume=24MB | **`waits=14`，`timeouts=0`**，pending 归零，运行正常结束；165K ops/s（背压生效的代价） |
| C 超时逃生 | stop=32MB + **timeout=1ms** | **`waits=1971`、`timeouts=1964`、`bp_retries=15642`**，运行**正常结束（退出码 0）**、无死锁、pending 归零 |

用例 C 之前的版本会 core dump（`free(): corrupted unsorted chunks`）：根因是
db_bench 的写错误路径 `ErrorExit()` 在 16 线程在飞时退出，**不是**引擎缺陷。
按用户要求把 `Status::Incomplete` 改为可重试后消失——这也顺带复现并印证了
"5s/1ms 这类过小 timeout 会制造重试风暴"的判断（15,642 次重试）。

## 4. 与设计稿的一致性

- pending 严格为"已封存未安装"，**不含 active**；active 只喂 `ShouldSeal`
  （`ledger_pending_bytes` 与 `wal_.TotalActiveBytes()` 互不影响）。
- 阻塞点只在 `PreprocessWrite`→`DelayWrite`（seq 分配与 WAL append 之前）；
  seal 临界区内的检查未加等待（保持"只记账"）。
- 等待复用 native `bg_cv_`，未新建条件变量；唤醒源只认"drained 计数增加"
  （installed）；空 edit/barrier/入队都不会增加该计数，因此不会提前放行。
- 超时返回与 native `no_slowdown` 同款的 `Status::Incomplete`。

## 5. 复现

```bash
ZF=/home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/build_rel
cd /home/embed/hyl/metadata_offload/output/zeroflush_m5_perf/ledger
LD_LIBRARY_PATH=$ZF:/usr/lib/x86_64-linux-gnu $ZF/db_bench \
  --benchmarks=fillrandom,waitforcompaction --key_size=16 --value_size=1024 \
  --num=48000000 --writes=200000 --threads=8 --seed=20260915 \
  --compression_type=none --cache_size=536870912 --max_background_jobs=24 \
  --statistics=false --use_existing_db=0 --zeroflush --zf_partitions=16 \
  --zf_routing=sampled --zf_base_merge=true \
  --zf_ledger_stop_mb=32 --zf_ledger_resume_mb=24 [--zf_bp_timeout_ms=1] \
  --db=db_smokeC
```

## 6. 待做（第二阶段/第三阶段）

- **第二阶段：空 edit**（老批次冲突且无可安装输出时）：
  `HandOffAllGens → 空 VersionEdit → 空壳 memtable 出链 → 推进 barrier`，
  且约束 `last_materialized_epoch` 不变、`pending_wal_bytes` 不变、WAL 不
  unlink、目录仍可读；部分分区成功仍走 `kSkip`。ledger 的状态机已预留
  `conflict` 状态位，`LedgerInstallEpoch` 已按"只清非 queued 归属"实现，
  空 edit 批次的 gens 会自然保留在 pending 中。
- **第三阶段**：用户列的 9 项用例（含"ledger 超 stop 时 native imm 未进 stop"
  与"同分区 FIFO 不被冲突重排破坏"）——建议落在 `tools/zf_test.cc`。

---

# 7. 第二阶段：空 VersionEdit（进行中，发现一个账目缺陷）

## 7.1 机制：让位路径本身就是空 edit

安装期冲突的让位路径（`flush_job.cc`）已经是规范里的形态：

```
安装期复检命中冲突
  → DeleteAllOutputFiles + HandOffAllGens（每 job）
  → batch outputs 清空 ⇒ 随后 TryInstall 提交的 VersionEdit 不含 AddFile
  → 空壳 memtable 出链、推进 install barrier
  → SetLastMaterializedEpoch(last_before)（不变）
  → 下一轮 epoch 收养（LedgerAttachOrphans）重新认领
```

用户列的禁止项在实现上天然成立：该路径不调用 `LedgerInstallEpoch`；gens 被
`HandOffAllGens` 标为 queued 后由 `LedgerInstallEpoch` 的"只清非 queued"规则
跳过；`pending_bytes` 不变；WAL 交给 recovery/skip 集合、不 unlink。

本阶段新增：诊断字段（`empty_edit_count / barrier_advance / pending_before|after /
lm_before|after / pending_unchanged`）、`ZF_FORCE_EMPTY_EDIT=N` 测试钩子
（install 冲突是竞态，难以确定性复现，钩子强制前 N 次安装让位）、
`ZfCloseFlush` 的"无进展即停"（drained 与 pending 双不变即结束，不再烧满 guard）。

## 7.2 已验证（强制 2 次空 edit，400K 写）

| 不变量 | 结果 |
|---|---|
| 1 空 edit 推进 barrier、不死锁 | ✅ `empty_edit_barrier_advance=2`，运行完成，随后 `install_direct_base=48` 正常安装 |
| 3 `last_materialized_epoch` 不变 | ✅ 该路径只做回滚（`SetLastMaterializedEpoch(last_before)`）；日志里的 `lm_before/after` 是**全局水位**，并发批推进会让它前进，不代表本批 |
| 4 `pending` 与 entry 数不变 | ✅ `empty_edit_pending_unchanged=2` |
| 6 下一轮重新认领、不重复记账 | ✅ 运行末 `pending=0/entries=0`，`drained=259.6MB` **小于**写入量（≈427MB，差额是结束时未封存的 active，按定义不计） |
| 8 连续空 edit 不空转 close-flush | ✅ guard 已实现（见 7.3 的分歧场景下 `rounds=2` 即停） |

## 7.3 ⚠ 未通过：重复空 edit 下 ledger 与 cache 分歧（阻塞 0b-4）

强制**持续**让位（`ZF_FORCE_EMPTY_EDIT=1000`）后，关闭时：

```
drain: rounds=2 has_pending=1->1 skip=407.4MB     ← cache 认为还有 407MB 待物化
ledger after drain: pending=0.0MB entries=0       ← ledger 认为已清空
```

即 **watermark 低计了卡住的数据**。定位证据（`ZF_DBG_LEDGER=1`）：

```
ZFDBG-ledger-handoff-miss ep=1 part=6 gen=0 ledger_entries=4
    keys_sample=(14/0/ep2) (1/0/ep2) (7/0/ep2) (13/0/ep2)
zf.ledger_handoff_hit : 0
zf.ledger_handoff_miss : 16
```

**移交时传入的 (part,gen) 在 ledger 中已不存在**（请求 ep1 的 part 0/2/6/12，
而 ledger 里只剩 ep2 的 part 1/7/13/14）。`LedgerHandOff` 只"标记已存在条目"，
找不到就跳过 → 这些代从未被标成 queued → 其 epoch 安装时 `LedgerInstallEpoch`
按 `state != queued` 把它们当"已安装"清掉 → ledger 少计、cache 仍算 pending。

根因类别：**ledger 用一张并行 map"镜像"cache 的 generation 所有权**，而 cache
才是"哪些 gen 仍在盘上待物化"的唯一权威（skip ∪ recovery ∪ 已登记未释放 epoch）。
镜像在多次移交/收养/安装交错下必然漂移——本阶段已经为此修了 4 处挂载点，这是
第 5 处。

### 建议的修法（下一轮）

不再镜像所有权，改为**从 cache 派生 pending**：

```
pending_bytes = Σ 文件字节( gens ∈ skip_gens_ ∪ recovery_gens_ ∪ 未释放的 epoch.gens )
drained_bytes = 单调计数，仅在 epoch/gens 真正释放（ReleaseGens 归零）时增加
```

即 ledger 只保留"单调 drained + 峰值"两个计数器，pending 由 `SealedFileCache`
按需计算（它已有 `skip_bytes_/recovery_bytes_/epochs_[].gens` 与文件大小）。
这样账目正确性由单一权威决定，不再有第二次记账。代价是每次读 pending 需要
遍历已登记 gens（数量级 = P × 在飞 epoch，~百级），对水位判定（写路径每写组
一次）完全可接受；若需要 O(1)，可由 cache 在 Add/Release 时维护一个求和，
但仍以 cache 为唯一写入方。

在此之前 **0b-4（无 L0 受控实验）不应开始**——那正是空 edit 高频出现的场景。

---

# 8. 第二阶段重构：单一权威账目（已完成）

按用户 2026-09-15 的意见，**删除 ledger 的 generation 所有权镜像**，改由
`SealedFileCache` 作为 generation 生命周期的唯一权威，并分离三个量：

| 量 | 语义 | 更新点 |
|---|---|---|
| `pending_materialize_bytes` | 仍需物化的 generation 字节（sealed/queued/building/conflict/recovery） | `RegisterGen`（每 (part,gen) 只注册一次）/ `MarkEpochInstalled` |
| `drained_materialize_bytes` | 数据真实进入 L1 | 仅在 `LogAndApply` 成功后（`FlushJob` 的 `MarkEpochInstalled`） |
| `reclaimed_bytes` | 物理可回收（引用全部释放） | `ReleaseGens` 中 per-gen 引用归零处 |

handoff / 收养 / conflict 重规划只是**状态迁移**，不再触碰字节计数
（`LedgerSeal/HandOff/AttachOrphans/ReownEpoch/InstallEpoch` 全部删除）。
实现位置：`SealedFileCache::gens_`（key=(part,gen) → {bytes, installed}）+
`registered_gens()/installed_epochs()/reclaimed_gens()`。

## 8.1 重构过程中发现的第二个实质缺陷（已修）

**移交被调用时携带已安装的 generation**。诊断（`ZF_DBG_LEDGER=1`）：

```
ZFDBG-handoff-already-installed ep=4 gens=28(407.4MB) / 28
```

即让位批次按 job 全量移交时，其中某些 epoch 的数据**已由前一批经 LogAndApply
安装**；`HandOffSkippedToRecovery` 未过滤 → 已安装数据被重新塞回 skip 集合 →
`skip_bytes_` 虚高 407.4MB、`HasPendingGens()` 恒真 → 关闭收敛判据永不满足。
（旁证：修复前 `drained=407.4MB` 与 `skip=407.4MB` 数值相等，同一批字节同时被
判为"已安装"和"待物化"。）

修法（一行语义守卫 + 诊断计数）：`HandOffSkippedToRecovery` 只接收 registry
中**未标记 installed** 的 generation；全部已安装则直接返回。计数
`handoff_installed_filtered`（应视为健康指标：非 0 说明调用方用了过期 gens 列表）。

## 8.2 持续空 edit 场景复测（`ZF_FORCE_EMPTY_EDIT=1000`，400K 写）

| 指标 | 修复前 | 修复后 |
|---|---|---|
| 关闭时 `skip` | 407.4 MB（虚高） | **0.0 MB** |
| `HasPendingGens` | 恒真（`has_pending=1->1`） | **0->0**，`rounds=0` |
| `pending` vs `drained` vs `reclaimed` | pending=0 且 skip=407MB（互相矛盾） | **0 / 257.2MB / 257.2MB**（自洽） |
| `registered_gens` | 28（悬挂） | **0**（全部结清） |
| `handoff_installed_filtered` | — | 12（守卫拦截） |
| 空 edit 计数 | 3 | 3（`barrier_advance=3`，pending 每次不变） |

不变量覆盖（本次）：1 ✅ barrier 推进；3 ✅ lm 不被空 edit 推进；4 ✅ pending
与 entry 数不变；6 ✅ 重新认领且不重复记账（`registered_gens` 归零、drained
≤ 写入量）；8 ✅ close-flush 无进展即停。
重开后（`readrandom`）：`recovery_count=0`、`pending=0`、`registered_gens=0`，
读有命中 → 数据落盘、无遗留待恢复数据（覆盖 2/5/9 的**主要方面**）。

## 8.3 仍未验证（诚实清单）

- 4 项：`readrandom` 的命中率（20400/200000）受 db_bench 键分布影响，**不能**
  作为完整性判据；要严格验证需要与写入端一致的键集合校验器（`zf_valcheck`
  的 `MakeKey` 与本轮 `--key_size=16` 的键格式不一致，未使用）。
- 7 项：部分 `kSkip` 批次不丢其他分区输出——本轮未构造该场景（需要单批多分区
  且其中部分被 skip）。
- 10 项：同分区 FIFO 与读后缀不变量——需专门用例（旧代冲突期间写新代并读）。
- 用户点名的"1ms timeout 重试风暴"已记录：`backpressure_timeouts` /
  `bp_retries` 分离计数，正式驱动需退避（harness 目前是 1ms 固定退避）。

---

# 9. 严格校验器（已完成）与其发现的正确性缺陷（阻塞 0b-4）

## 9.1 共享键生成 + 严格校验器

- 新增 `tools/zf_keygen.h`：键生成（`BE64(v)` + `'0'` 填充）与参考值模型，
  与 db_bench 的 `GenerateKeyFromInt` 共用（db_bench 已改为调用它，杜绝漂移）。
  此前 `zf_valcheck` 自带一套 `MakeKey`，与 `--key_size=16` 实际产生的键不一致，
  根本无法校验真实数据。
- `tools/zf_valcheck` 重写为多模式校验器（自带参考值模型，能证明"读到正确版本"）：

| 模式 | 覆盖检查项 |
|---|---|
| `write` | 只写不 Flush（含删除、覆盖）——为 walhit 准备"物化前"状态 |
| `walhit` | 物化前校验 + **断言 WAL 读数 > 0**（否则"WAL 命中"未被真正验证） |
| `l1hit` | Flush 物化后校验（L1/SST 路径）+ 报告残留 WAL 读数 |
| `verify` | 进程级重启后校验 |
| `scan` | 全量扫描并把不匹配分类：丢覆盖 / 不该覆盖 / 值损坏 |
| `probe` | 指定键的取值诊断（对照 seed1/seed2） |

覆盖：写入后随机 Get、重启后 Get、WAL 命中、L1 命中、Delete、覆盖写、
snapshot 可见性、NotFound、两条命中路径结果一致。
（首版期望模型写错——把"覆盖半个区间"当成"每 100 键覆盖一次"——`probe`
模式证实数据本身正确后修正；校验器自身的模型也由此得到验证。）

## 9.2 ⚠ 缺陷：覆盖写在读路径上丢失（可复现）

**现象**（`scan` 模式，40000 键，阈值降至 16MB/4MB 以触发封存）：

```
index_insert_dropped=0 bloom_slowpath=30110
scan: mismatch=2227 (lost_overwrite=2227 unexpected_overwrite=0 other=0) / total=40000
MISMATCH v=2 want=seed2 got_len=1024 eq1=1 eq2=0 st=OK
MISMATCH v=11 ... v=14 v=15 v=17 v=20 v=22 v=23 ...
```

- **`lost_overwrite=2227`（占被覆盖区间 20000 的 11%）**：读回的是**旧版本**
  （完整的 seed1 值，非损坏、非丢失），即第二阶段覆盖写在该键上不可见。
- 分位无关：不匹配键散布在 [0,20000)（v=2 到 19999），不是分区/范围边界效应。
- 与封存强相关：**不触发封存的小规模运行（20MB、20000 键）三次全绿**；
  一旦走到"封存→物化"路径（40000 键）即复现（三种模式一致复现 2227）。
- `index_insert_dropped=0` → **不是**已知的"索引插入按 gen 丢弃"路径。
- 本轮运行的配置**未触发 0b-3 的任何新机制**（无 `ZF_FORCE_EMPTY_EDIT`、
  无背压：pending 峰值 32MB < stop 1GiB、无让位）→ 高度指向**既有缺陷**，
  而非本次重构引入；但需用 0b-3 之前的构建做一次二分确认。

**影响**：这是"命中率"类检查永远发现不了的一类缺陷（此前 63%/83% 命中的
验收口径只证明"请求返回了结果"）。它直接阻断 0b-4——无 L0 模式会让更多数据
经由"封存→物化"路径。

**复现**：

```bash
ZF=/home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/build_rel
cd /home/embed/hyl/metadata_offload/output/zeroflush_m5_perf/ledger
LD_LIBRARY_PATH=$ZF:/usr/lib/x86_64-linux-gnu $ZF/zf_valcheck db_vc3 write 40000 4
LD_LIBRARY_PATH=$ZF:/usr/lib/x86_64-linux-gnu $ZF/zf_valcheck db_vc3 scan  40000 4
# 预期 mismatch=0；实测 lost_overwrite=2227
```

**下一步（按优先级）**：
1. 二分确认是否既有（用 0b-3 之前的提交构建同一校验器）。
2. 定位读路径：`lost_overwrite` 说明读取命中 L1/SST 的旧版本而**没有看到
   pending 索引/WAL 里的新版本**——检查封存窗口内"插入落到哪条索引链"
   （active vs 被冻结后又被释放的）以及 gen 归属；`ZFDBG-ixmiss` 日志
   （如 `part=4 key=...2D...`）可作切入点。
3. 修好并把 `scan` 模式纳入回归门（mismatch 必须为 0），再启动 0b-4。

---

# 10. 归因定稿（收紧）+ 修复顺序 + 回归门

## 10.1 已证事实（不再含推测）

| 编号 | 事实 | 证据 |
|---|---|---|
| R0 | **恢复用"文件是否存在"推断"当前 active / 仍需物化"** —— 该信号不可靠：文件存在只说明未回收，不说明未安装 | `EnsureActive(p, wal_->MaxGen(p, files))` 在文件被物化 unlink 后塌缩为 gen 0；`DumpKeyIndexChain` 实测 active=gen0 持有旧记录（`loc(part=8 gen=0 off=262728) seq=3906`） |
| R2 | **索引命中后直接返回，不与已安装层比较 sequence** | 读序 active 优先 + `GetFromPartitionIndex` 命中即 `return true`；`read_seq=60000 > seq2=43906` 排除 snapshot 因素 |
| R3 | **陈旧遮蔽 100% 发生在 L0** | `triage: B3_visible_in_sst=2144 (seq2>seq1=2144) | B3 拆分: in_L0=2144 in_L1plus=0` |
| R4 | **失败集随开合循环漂移** | 同一库两次 triage：fail 3283 → 2144，B2 980 → 0（每次重开都跑 recovery） |

**降级为风险（未证实）**：R1 身份模型混用（逻辑路由结果 `rpart` 被当作 generation 身份）。静态分区下 `rpart` 很可能仍等于源 `part`；已证的是 **generation 身份与 active 身份被混用**，不是发生了实际错路由。
**废弃**："generation 编号复用"不是主因。

**核心判据**（用户 2026-09-15）：
```
WAL 文件仍存在  ≠  WAL 数据仍需作为 L0 参与读取
当前恢复协议（文件存在 ⟹ 未安装）是错的
```

## 10.2 修复顺序（按用户指定，禁止跳步）

```
1. 显式 active_gen（optional<Generation>，空 ≠ gen 0）
   + 保留双身份：PhysicalWalId = (physical_part, generation) 与 IndexPart = 路由结果
   InsertCreate 必须同时接收两者，不能用裸 gen 判断 active/frozen
2. 连续 installed_fence[part]（不是"某 gen 曾出现在 SST"）：
   仅当该 gen 的全部 WAL 记录都被物化消费 且 AddFile 的 LogAndApply 成功后推进；
   与 SST 安装处于同一持久化提交（part/generation/consumed_records/consumed_bytes/install_seq）；
   空 edit 不推进 fence
   legacy（无 fence 的旧库）二选一：
     严格：拒绝按新协议打开
     兼容：一次性 SST+WAL reconciliation，且必须标记 legacy recovery，不得静默混入正常路径
3. 同分区 FIFO：旧 gen conflict/recovery 时新 gen 不得越过安装；
   空 edit 只推进全局 barrier，不推进该分区 fence
4. debug 读保护（保留到回归完成）：索引命中时校验
   index_seq vs live SST seq（L0/L1+ 分开），index_seq < sst_seq 则报错并返回最高可见版本
5. 回归：triage（含 B3 拆分）/ scan / write / walhit / l1hit / verify
```

## 10.3 回归门与两个必跑场景

```
回归门：
  triage: B1_not_written=0  B2_in_wal=0  B3_in_L0=0  B3_in_L1plus=0
  scan:   lost_overwrite=0  other=0
  write/walhit/l1hit/verify: mismatch=0

必跑场景（分开处理 baseline 与目标模式）：
  A. 保留 WAL + 允许旧 L0 fallback + 重启
  B. 保留 WAL + 禁止 L0 fallback + 空 edit      ← 目标模式
决定性验收：seed2 已进入 L1 或仍在 pending WAL → 重启 → 不得返回 seed1
```

## 10.4 当前代码状态

- 引擎与工具均已构建通过、工作树自洽；**未写入任何修复补丁**（避免未验证即改协议）。
- 诊断资产：`tools/zf_valcheck.cc`（`write/walhit/l1hit/verify/scan/probe/deep/triage`，triage 含 L0/L1+ 拆分）、`tools/zf_keygen.h`（与 db_bench 共用键生成）、`zeroflush::DumpKeyIndexChain`（只读索引链导出）。
- 待补：debug 读保护需要"LSM 侧命中的 sequence"——本版本 `GetContext` 不在 `db/*.h`（`db/table_cache.h` 仅前向声明），下一轮需先定位其定义与可用的 seq 统计接口（或退化为"索引命中时并行做一次 LSM 读并比较版本"）。
