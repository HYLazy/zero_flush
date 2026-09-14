# 50GB merge-ON 验收 + 严格最旧准入尝试（2026-09-14）

## 1. 50GB 验收（cf5608e/8fb5bab，锁开启）：**未通过**

在 2.7GB / 每线程 15 万写处停摆。gdb 栈（4 个 flush 线程）：

| 线程 | 等待点 |
|---|---|
| Thread 27 | `InstrumentedMutex::Lock`（想进 ZfMaterializeAllEpochs，入口要 DB mutex）|
| Thread 26 | `WaitInstallRelease`（前序等待退避）——**持 DB mutex 空转** |
| Thread 3 | `nanosleep`（同一等待循环）|

修复（已提交 `8fb5bab`）：退避期间放开 DB mutex。
**但复测仍在 2.7GB 同一处停摆**（这次前序的 flush job 卡在 gen0 窗口
等待）——说明"批级全局串行 + 前序等待"框架下等待环是层出不穷的。

## 2. 严格最旧准入尝试（本次，**已回退**）

按用户方案实现"最旧可处理批次准入"：

- 新增 `MemTableList::ZfEarliestUncompleted()`（imm 中最旧未完成项）；
- `FlushJob::PickMemTable` 入口加准入门：更旧批未完成 → 不 Pick、不设
  `flush_in_progress_`、不拿锁/gen0，直接返回；
- `ZfMaterializeAllEpochs` 的前序等待循环替换为"断言 + 让位"。

结果：**段错误（rc=139）**。日志显示 `defer_in_materialize=1`，即仍有
批次在 materialize 阶段才发现"有更旧未完成项"——准入门判据与
materialize 内的判据不一致：Pick 时用
`ZfEarliestUncompleted() < max_memtable_id`（只看未完成项），
而 materialize 内用 `GetEarliestMemTableID() < my_min`（含已完成待提交项）。
更旧的**已完成未提交**项不会被准入门拦住，却会让后续批次的
TryInstall 提交链断裂（`commit_stop oldest_id=..`），最终崩溃。

**结论**：准入门与前序检查必须使用同一判据（"最早未完成项"），
且该判据必须前移到 **PickMemtable 之后、创建 FlushJob 之前**的调度点
（用户方案的第一点要求），否则 Pick 与准入之间仍有竞态窗口。
这已超出"FlushJob 内部补丁"的范围，需要改调度入口。

## 3. 当前状态（回退后）

- 代码回到 `8fb5bab`（锁默认开启；M4.9 与 4M 写、zf_test 已验证；
  50GB 未验证）；
- 工作树干净；
- 下一步按用户方案：把准入判断前移到调度入口（Pick 之前），
  并统一"最旧"判据为 `ZfEarliestUncompleted`。
