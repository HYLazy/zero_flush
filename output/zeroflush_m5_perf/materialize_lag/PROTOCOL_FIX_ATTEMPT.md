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
