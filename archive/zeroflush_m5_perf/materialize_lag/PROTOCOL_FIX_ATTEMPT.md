# install 锁协议修复尝试记录（2026-09-14）

> 目标（用户方案 commit 1）：锁竞争失败时**保留输入在 imm/retry 队列、返回
> 可重试状态**，不再"gens 移交 recovery + 返回 OK"。
>
> 结果：**部分成功，但未达到可上线标准**。下面是完整过程与结论，供下一轮
> 直接接手（避免重复踩坑）。

## 1. 已确认的改进（4M 写复现案例）

在 4M 键 / 4M 写（4GB）/ merge ON / 16 分区 / 锁开启下：

| 指标 | 旧实现（让位→recovery） | 新协议（保留 pending） |
|---|---|---|
| fill 完成 | ❌ 停滞（300s 超时） | ✅ **13.3s / 300K ops/s** |
| `ctxhandoff`↔`adopt` 循环 | 49/48 次、skip_gens 4→148 | **2/2 次（收敛）** |
| 安装次数 | 68（停摆） | **199**（随输入增长） |
| 存储形态 | WAL 3.45GB / SST 0.24GB | **WAL 0.23GB / SST 0.25GB** |

**结论：协议方向正确**——"保留输入 + 返回可重试"确实消除了 recovery 收养
循环，WAL 能回收、安装数随输入增长。

## 2. 达成上述结果的关键修正（两个）

### 2.1 前序检查必须用"本批最小 ID"

初版判据写成"imm 最早 ID < 本批任一 memtable ID"：

```cpp
for (ReadOnlyMemTable* m : mems_) {
  if (earliest < m->GetID()) { has_earlier = true; break; }   // ✗
}
```

当本批**自身含最早项**（`earliest == m->GetID()`）时仍可能被判定为"有前序"
（因为比较的是任一元素），导致**全体批次互相让位、无一能推进**（实测：
imm 堆积到 9 个、`Stopping writes`、flush 全停）。

正确判据 = 用本批最小 ID：

```cpp
uint64_t my_min = min over mems_;
if (earliest < my_min) { has_earlier = true; }                 // ✓
```

修正后 4M 案例即从"停滞"变为"13.3s 完成"。

### 2.2 让位前必须释放已持有的锁

后序批抢到锁后才发现"有前序"，此时若**持锁让位**，最早的批次永远拿不到锁
（锁被反复占用又释放给下一个后序批）。必须"先释放锁、再让位"，使锁在批次
间公平轮转。未做此修正时观测到：256 分区配置下 imm 堆积 9 个、
`Stopping writes because we have 9 immutable memtables`。

## 3. 未解决的问题：256 分区配置仍会停摆

M4.9 压力配置（256 分区 / 16 线程 / 2.1M 写 / merge ON / 锁开启）：

| 轮数 | 旧实现 | 新协议 |
|---|---|---|
| 4 轮 | 完成（崩溃） | 完成（无崩溃，reopen 一致） |
| 16 轮 | 崩溃或停滞 | **约 6 分钟后停摆**：写完 144 万/210 万后卡住，`dirconf=249→251`、imm=8-9、`Stopping writes`、安装停在 192 次 |

停摆机制（探针实测）：
1. 批次 R 抢到锁 → 发现"有前序未完成" → 释放锁让位（3.33 节语义）；
2. 该 memtable 被回滚到 imm，等下一次 flush 调度挑选；
3. 但下一次调度选中的批次同样发现"有前序"→ 再让位；
4. 若"最早未完成 memtable"始终没被任何批次带去安装，会让**所有**批次
   永远处于"有前序"状态 → imm 只增不减 → 写路径停止。

**根因判断**：让位（回滚 + 重新挑选）与 RocksDB 的 flush 队列语义存在
竞争——`PickMemtablesToFlush` 从 imm 尾部（最旧）开始挑，但"已经
`flush_in_progress_` 的 memtable"会中断挑选（`memtable_list.cc:438`），
因此被同批覆盖的最旧 memtable 可能长期拿不到新的 flush job。

## 4. 回退状态与建议

- **代码已回退**到 `495b278`（instal 锁默认关闭、`zf_test` ALL PASSED、
  M4.9 崩溃修复代码保留但默认不生效）。
- **默认行为不受影响**：`ZF_INSTALL_LOCK=0`，与修复前一致。

### 下一轮建议（按优先级）

1. **不要用"回滚重试"实现让位**。更稳的做法是把让位做成**调度信号**：
   让位批把"最早未完成 memtable"标记为优先，并确保它一定被下一个 flush
   job 携带（例如：让位时不做 Rollback，而是保持 `flush_in_progress_` 由
   本批继续持有，直到真正完成——即"锁等待"而不是"锁让位"）。
2. **锁等待的正确形态**：等锁期间**不持 DB mutex、不占 flush 线程**，且
   等待的批次必须是"imm 中最旧的那一个"（保证无环）。即：让位只在
   "本批不是最旧"时发生，且此时**不做回滚**（memtable 保持 in-flight，
   由调度器稍后重派同批）。
3. **或者放弃"整批串行"**：直接做分区级 token（用户方案 commit 3），
   从源头减少锁竞争——M4.9 冲突只发生在**同分区**，256 分区下同分区冲突
   概率远低于全局锁。
4. 确定性交错测试（用户方案 commit 2）应覆盖：
   - A 持锁构建、B 抢锁失败 → B 保留输入、A 完成后 B 重新规划并安装；
   - 反序就绪（B 先 build 完成、A 后到）；
   - imm 中有更早 memtable 时，锁必须在批次间轮转（本记录的 2.2 节）。

## 5. 证据文件

- `diag/p9.stderr`：修正判据后的成功案例（4M 写 13.3s、defer=2、installs=199）
- `diag/p3/p4/p5/p6/p7/p8.stderr`：各阶段探针（含停摆现场）
- `/tmp/m49v_long.log`：256 分区停摆现场（imm=9、Stopping writes、dirconf 递增）
- 关键代码位置：`db/flush_job.cc`（`zf_has_earlier_pending` 判据、让位分支）、
  `db/memtable_list.cc:409`（PickMemtablesToFlush 的挑选规则）

---

# 附录：最旧 imm 生命周期定位（2026-09-14，按用户方法）

> 目标：定位"最旧 imm 卡在哪个生命周期状态"，**不预设 owner 泄漏**。
> 方法：三处白盒探针（入队 / Pick / Rollback）+ 实际完成与安装端
> （TryInstall 标记完成、提交循环推进），统一走 `ZF_TRACE_IMM=1` 门控，
> 事件带全局序号。

## A.1 停摆现场（256 分区 / 16 线程 / merge ON / 锁开启）

```
ZFTRACE seq=26 site=PickMemtablesToFlush action=picked_done ids=[10]
ZFTRACE seq=27 site=TryInstall action=commit_loop commit_stop
        oldest_id=10 inprog=1 completed=0 n=1      ← 最旧 imm
（此后 memtable 11/12/13/14 照常 pick+完成：
  ？  enqueue notflushed=2 / 3 / 4 / 5   ← imm 持续增长）
（写路径：Stalling(8) → Stopping(9) → 全部线程 futex_wait，LOG 停止增长）
```

**最旧 memtable 10 的状态组合** = `flush_in_progress=1, flush_completed=0`，
即用户表格中的第一行：**"标记 in-progress、无完成"**。没有 rollback 事件
（`RollbackMemtableFlush` 0 次）、没有让位记录（`kept pending` 0 次）。

## A.2 该 memtable 的 owner 轨迹（job 绑定探针）

```
site=ZfMaterialize job=10 action=enter mems=[9]    → exit  → Run_postbuild s=OK
                                                   → Run_preinstall → flush_finished ✅
site=ZfMaterialize job=11 action=enter mems=[10]   → exit  → Run_postbuild s=OK
                                                   → Run_preinstall → ✗ 无后续
```

**JOB 11 = memtable 10 的 owner**：物化成功（`s=OK`、产出 4 文件）、
已进入安装路径（`Run_preinstall` 打印），但**没有 `TryInstall mark_completed`、
没有 `flush_finished`、没有错误、没有回滚**。JOB 12/13/14 均正常完成。

即：**构建完成 → 移交安装 → 安装未完成**（用户表格第二行：completed=0
但有明确 owner，且 owner 卡在安装路径内部）。这是与"owner 退出/泄漏"
不同的一类断裂。

## A.3 卡点范围（已缩小到函数级）

`TryInstallMemtableFlushResults` 内两条可能路径：

1. **标记完成前早退**：`if (commit_in_progress_) return s;`（handler 内
   首段）——但探针显示 `mark_completed` 未打印，说明卡在更早的位置；
2. **MANIFEST 提交**：`while (s.ok())` 循环 → `LogAndApply`（其间释放
   DB mutex）→ `RemoveMemTablesOrRestoreFlags`。

当前证据（JOB 11 未走到标记完成、也未打印任何 LogAndApply/commit 日志）
指向 **进入 `TryInstallMemtableFlushResults` 后、首个状态写入之前**，
最可能是阻塞在函数开头的 `mu->AssertHeld()` 语义外的 DB mutex 获取，
或 `LogAndApply` 内部等待 MANIFEST writer 队列。

**最终一步（需要 sudo 或允许 ptrace）**：停摆时抓该线程栈：

```bash
echo 0 | sudo tee /proc/sys/kernel/yama/ptrace_scope   # 或直接 sudo gdb
sudo gdb -p $(pgrep -x zf_govern_bench) -batch -ex "thread apply all bt 12"
```

本机 `ptrace_scope=1` 且无免密 sudo，gdb attach 被拒绝（已实测
`ptrace: Inappropriate ioctl for device`），所以栈未取到。

## A.4 探针清单（已入库，`ZF_TRACE_IMM=1` 门控）

| 位置 | 文件 | 记录内容 |
|---|---|---|
| `EnqueuePendingFlush` | `db_impl_compaction_flush.cc` | queued 标志 / IsFlushPending / notflushed / 是否入队 |
| `PickMemtablesToFlush` | `memtable_list.cc` | 选中完整 ID 集合；中断点及该 memtable 的两个标志 |
| `RollbackMemtableFlush` | `memtable_list.cc` | 请求集合 vs 实际重置集合（前后状态） |
| `TryInstallMemtableFlushResults` | `memtable_list.cc` | `mark_completed`（含 file_number）、commit 循环停止点（最旧 ID + 标志） |
| `ZfMaterializeAllEpochs` | `flush_job.cc` | 每批 enter/exit + memtable ID 集合（最小 ID 判据依据） |
| `Run` | `flush_job.cc` | postbuild（s 状态、产出数）、preinstall |

## A.5 分流结论（按用户表格）

| 现场 | 判断 |
|---|---|
| `in_progress=1, completed=0`，**有明确 owner（JOB 11）且 owner 未退出** | ✅ 本案例 |
| owner 卡在安装路径内（进入 `TryInstall`、无状态写入、无错误） | ✅ 本案例 |
| queued 标志与队列不一致 | ❌ 无证据（enqueue 探针显示正常入队） |
| 回滚集合错误 | ❌ 无证据（0 次回滚） |
| 错误未传播 | ❌ 无证据（LOG 无 error、无 TryAgain） |

**下一步（局部补丁方向）**：在 `TryInstallMemtableFlushResults` 入口与
`LogAndApply` 前后各加一个状态点（或直接在停摆时取栈），确定是在
"等 DB mutex" 还是 "等 MANIFEST writer"。然后只修该断点——
按用户要求，不以"加了更多探针"作为完成。

---

# 附录 B：死锁根因与修复（2026-09-14，gdb 栈实证，**已修复**）

## B.1 gdb 栈揭示的完整死锁环

放开 `ptrace_scope` 后抓栈（256 分区 / 16 轮 / 锁开启）得到决定性证据：

```
Thread 6/4 (rocksdb:high): WaitGen0WindowClosed()  ← 等学习窗关闭
Thread 3   (rocksdb:high): AcquireGen0Slot()       ← 等 gen0 槽
Thread 5   (rocksdb:high): [sleep] ZfMaterializeAllEpochs  ← 我的前序等待门
```

即：**学习批持有 gen0 槽，却卡在"等前序 memtable 完成"**；而前序的
flush job **卡在"等学习窗关闭"**（`WaitGen0WindowClosed`）→ **自锁**。
早期另一次抓栈（阻塞等待版）显示 4 个 flush 线程全部停在
`WaitForInstallLock`，而持锁者卡在 `TryInstallMemtableFlushResults`
等前序 → 同类环（持锁等前序 × 前序等锁）。

## B.2 修复（两处，缺一不可）

1. **前序门（取锁之前）**：本批若在 imm 中仍有更早未安装的 memtable，
   则**不取锁**、不持有任何全局资源地等待前序完成（轮询
   `GetEarliestMemTableID` + 200µs 退避），前序清空后才取锁。
   这样持锁者进入 `TryInstall` 时一定不会等待别人。
2. **等前序时释放 gen0 槽**：等待循环内若持槽则先 `ReleaseGen0Slot()`，
   前序清空后重新 `AcquireGen0Slot()`（保持"含 gen0 的批至多一个在飞"）。
   这是打破"持槽等前序 × 前序等窗"自锁的关键。

对应代码：`db/flush_job.cc`（`install_predecessor_wait` 区块）
+ `zeroflush/zeroflush_db.{h,cc}`（`WaitInstallRelease`）。

## B.3 验证结果（锁开启 = `ZF_INSTALL_LOCK=1`）

| 场景 | 结果 |
|---|---|
| 原 M4.9 崩溃配置（256 分区/16T/16 轮）| **3/3 通过**（120/129/126s），零 overlapping/corruption，read 命中与 reopen 一致 |
| 同配置（另一次 2 连跑）| 2/2 通过（112.7/114.0s，18.4K/18.4K ops/s） |
| 4M 写 / 16 分区 / merge ON | 13.5s 完成（297K ops/s），**零让位**，安装 200 次，WAL 0.23GB / SST 0.26GB（可回收） |
| zf_test 全量 | **ALL PASSED** |
| 对照（锁关闭）| 本批抽样未复现崩溃（该崩溃为概率性），但历史 3/3 崩溃记录在案 |

吞吐代价：256 分区配置下锁开启 ≈18.4K ops/s（此前锁关闭同配置约 23K 量级），
约 −20%，来源于安装窗口串行化 + 前序轮询退避。可后续用分区级 token 收窄。

## B.4 关键探针（`ZF_TRACE_IMM=1` 门控，保留在代码中）

`ZFTRACE site=install_predecessor_wait job=.. my_min=.. earliest_imm=.. wait_n=..`
——出现此行的批即为"为等前序而等待"的批；正常运行应为 0 次（4M 案例实测 0）。
