# 物化滞后专题分析（2026-09-12）

> 现象：50GB merge-ON 验收中 fill 完成后 **47.3GB 数据仍滞留分区 WAL**，
> SST 仅 0.4GB；`ZFDBG-install` 仅 68 次（对 48M 写/16 分区），
> 原生 compaction 0 次。读写数据正确（readrandom 8M 全命中），
> 但读性能退化（77K vs 历史 188K）且 WAL 空间不回收。
>
> 本文记录逐层定位过程与最终结论（含一次自纠：初判"物化管线滞后"，
> 实测证明是 **install 锁引入的让位循环**）。

## 1. 复现（最小规模）

4M 键 / 4M 写（4GB）/ merge ON / 16 分区 / 锁开启：

| 指标 | 实测 |
|---|---|
| 安装事件 | **68 次**（全部集中在日志前 246 行 = 启动后 ~1s 内） |
| 封存事件 | 26 次（其中 15 次 size=0） |
| `ctxhandoff`（gens 移交 recovery） | **49 次** |
| `adopt`（下批收养） | **48 次**，`skip_gens` 4 → 148 **单调增长** |
| 决策计数 | `ZFDBG-skip` 全为 0（无 kSkip），但仍发生移交 |
| DB LOG | **49 次 `install lock busy; batch deferred`** |
| 终态 | WAL 3.45GB / SST 0.24GB |

**关键时序**（LOG 原始顺序）：

```
[JOB 2..5] ZeroFlush materialized 1 epochs (16 files)   ← 正常运行
[JOB 7]    ZeroFlush materialized 1 epochs (4 files)    ← 学习批切片
[JOB 6]    install lock busy; batch deferred (1 epochs handed to recovery)
[JOB 8..N] install lock busy; batch deferred ...        ← 连续 45+ 次
```

## 2. 定位过程

### 2.1 排除"决策层"与"数据流"

- `ZFDBG-skip` 计数全 0 → 不是 `kSkip` 攒批；
- `adopt` 与 `ctxhandoff` 成对（48/49）→ 数据在 recovery ↔ 收养之间往返，
  但不落盘；
- 安装 68 次全在启动瞬间 → 之后**没有任何一次物化产出被安装**。

### 2.2 锁持有者追踪（探针）

在 `AcquireInstallLock/ReleaseInstallLock/让位点` 加探针（`ZF_DBG_MM` 门控）：

```
ZFDBG-mm-defer job=6  wait_us=60388556 holder=149613... stage=0
ZFDBG-mm-defer job=8  wait_us=60373711 holder=149613... stage=0   ← 同一 holder
...
acq ok=0: 47,093 次        ← 失败重试次数
acq ok=1: 5 次             ← 成功获取（job 2..6）
release held=1: 17 次      ← 正常释放
```

结论：**锁的获取/释放是配对的**（release held=1 出现 17 次，无泄漏），
但让位路径在 60s 窗口内反复超时——每次 `WaitInstallLockFor` 返回 false
即让位。

### 2.3 循环的成因（实锤）

```
批次 B 在规划前 try_lock 失败
  → 等待 20ms×8（或 20ms×3000）后超时
  → 让位：把 B 的待物化 gens 移交 recovery + 释放 memtable
  → B 返回 OK（flush 视为完成，imm 出链）
  → 下一批 C 封存时收养 recovery 的 gens（skip_gens 增长）
  → C 规划前 try_lock 同样失败（holder 仍在构建）
  → C 也让位 …… 循环
```

**循环的驱动力 = 等待窗口 < 持锁批次的构建耗时**。实测首次物化
（学习批切片）耗时 300–500ms，而窗口 160ms → 必然超时。
把窗口放大到 60s 后，让位减少但**未消失**（仍 12–16 次），且出现
`acq ok=0` 达 47,093 次的密集重试——说明持锁批次有时长时间不释放
（超出 60s；120s 版本仍超时）。

## 3. 尝试过的修法与结果

| 方案 | 结果 |
|---|---|
| 短超时（160ms）让位 | 45+ 连击、物化停滞（原始状态） |
| 长超时（60s / 120s） | 让位减少到 12–16，但仍有超时；`acq ok=0` 47K 次；fill 13K ops/s |
| 阻塞等待（`WaitForInstallLock`，不移交数据） | `defer=0 / handoff=0`（循环消除），但 **300s 超时仍未完成 fill**——瓶颈转移到 install 路径本身 |
| 锁关闭（`ZF_INSTALL_LOCK=0`） | 正常：764 次原生 compaction、WAL 清空、3.6GB SST |

**自纠记录**：初版分析把现象归因为"物化管线滞后"（决策层不物化）。
实测证明决策层没有 kSkip；真正原因是**新增的 install 锁与让位语义叠加**
造成的循环，属本次修复引入的回归。

## 4. 结论与后续

### 4.1 对 50GB 验收结果的影响

- 那次验收的 327K ops/s 与 47GB WAL 留存，**是让位循环下的产物**：
  批次因锁冲突不断让位，物化几乎不发生，数据全留在 WAL（写入路径极快），
  安装停在 68 次。
- 因此验收中"锁开启比锁关闭快 4.1×"的结论**需要重新解读**：
  快的原因是"物化被让位循环抑制"，而非锁本身高效。
- **崩溃修复本身仍然成立**（控制组 3/3 崩 vs 修复组 3/3 过），但代价是
  物化停滞——这比原先估计的 1.9× 变慢严重得多。

### 4.2 剩余问题与建议方向

1. **install 锁必须换粒度**：当前"整批串行 plan→build→apply"在
   `max_background_flushes=4` 下把流水串行化，且其等待/让位路径与
   recovery 收养机制相互作用产生循环。建议按 **range/分区粒度**预留
   （与 token 机制合并），只串行化键域真正冲突的批次。
2. **或压缩持有时段**：锁只覆盖 finalize→apply，构建不持锁；构建完成后
   若版本已变（输入文件集合变化）则该分区重建。这消除长持有与让位需求。
3. **让位语义需重新设计**：现有让位 = "gens 移交 recovery"，而 recovery
   收养是全局的（不分分区），导致任何让位都会拖慢整库物化。若保留让位，
   应改为**保留在 imm 队列重试**（返回可重试状态，不清数据）。
4. **短期建议**：在 1–3 落地前，merge ON 默认保持 `ZF_INSTALL_LOCK=0`
   （崩溃风险仅存在于高并发 + ≥0.8GB 的 merge_basen 场景；生产配置需评估），
   或仅在高并发场景启用锁并接受物化滞后。

## 5. 证据文件

- `diag/fill.stderr`、`diag/fill.LOG`：4M 写复现（锁开启、160ms 窗口）
- `diag/probe2.stderr`：生命周期探针（lockacq/builddone/installed）
- `diag/stage.stderr`：持锁者阶段探针（stage=0 + 60s 超时）
- `diag/pair.stderr`：acq/release 配对探针（47K 次失败重试）
- `diag/wait.stderr`：阻塞等待版（defer=0 但 fill 未完成）
- `diag/fix60.stderr`、`diag/fix.stderr`：长窗口版（仍让位）
- 复现：`./run_diag.sh`（4M 键，约 1–6 分钟/次）
