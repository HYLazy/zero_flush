# 任务进度存档：b4b17b3 分支 50GB/1KB KV 读写测试

> ⚠️ **本文件已被取代**：任务已于 2026-09-19 执行完毕，结果见
> [`B4B17B3_50GB_100GB_REPORT.md`](B4B17B3_50GB_100GB_REPORT.md)。
> 下方内容保留为当时的勘察记录与计划（其中 §4/§5 的待定项已在正式报告中给出结论）。

> 状态：**已暂停（用户指示），未执行任何构建或测试**。存档时间 2026-09-17 19:0x。
> 本文件仅记录勘察结论与既定执行计划，便于之后直接恢复。

## 1. 任务

在不影响正在进行中的修改工作的前提下，对分支/提交 **b4b17b3** 运行 50GB / 1KB KV 读写测试。

## 2. 勘察结论（已完成，均为只读操作）

### 2.1 目标提交

- `b4b17b3ca14f054bb2a7ac5619c81c63b5081df5`
  "chore(zeroflush): 候选线清洗——实验性积压排空/收益攒批移出候选（含全部 env 开关）"
  Hyl，2026-09-17 18:11:57。
- 归属分支：仅 `fix/zeroflush-recovery-visibility-fences` 包含它，且该分支 tip 即此次提交。
- **与当前 HEAD 是兄弟关系**：`b4b17b3` 与 `677d753`（P1BacklogDrain-exp）共同父提交为 `debdbad`。
  ⇒ 二者代码不同，**不能复用主工作树已有的构建产物**，必须独立构建。
- 该提交移除了实验性开关：`ZF_VC_MERGE_BENEFIT`、`--zf_base_merge_skip_max_generations`
  （候选线上不存在可达崩溃路径）。

### 2.2 既有 50GB/1KB 跑法（可直接复用，无需重新设计）

两臂脚本已存在于仓库，参数一致、仅二进制与 ZF 参数不同：

| 臂 | 脚本 | 二进制 |
|---|---|---|
| ZF（本次目标） | `output/zeroflush_m5_perf/zf_50gb_noprobe/run.sh` | `source/rocksdb-zeroflush/build_rel/db_bench` |
| native 对照 | `output/zeroflush_m5_perf/native_50gb_samewindow/run.sh` | `source/rocksdb/build/db_bench` |

公共参数（两臂完全相同，保证同窗对照）：

```
--benchmarks=fillrandom --key_size=16 --value_size=1024 --num=48000000 \
--writes=3000000 --threads=16 --seed=20260914 --compression_type=none \
--cache_size=536870912 --max_background_jobs=24 --subcompactions=16
# 读阶段：--benchmarks=readrandom --reads=8000000（其余同上，--use_existing_db=1）
```

ZF 臂附加：`ZF_L0_TRIGGER=100000000`（写阶段）+
`--zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64 --zf_base_merge=true`。

规模口径：`--num=48000000` 键空间 × 16 线程 × `--writes=3000000` = 全键空间覆盖，
48M × (1KB value + 16B key) ≈ **50GB** 数据集，即"50GB/1KB KV"。

构建口径（历史一致）：ZF 侧 `CMAKE_BUILD_TYPE=RelWithDebInfo`、`PORTABLE=0`；
native 侧 `Release`（历史即为此不对称，沿用以免破坏可比性）。

### 2.3 环境资源

- CPU 36 核；内存 62GB（available ≈58GB）。
- `/home/embed/hyl` 所在 `/dev/nvme0n1p3`：1.8T，已用 927G，**可用 813G**（50GB 数据集 + 压实放大足够）。
- **无 ccache**，worktree 全新构建需全量编译（RocksDB 全量）。
- 无残留 db_bench 进程。

### 2.4 隔离约束（关键）

- 主工作树 `/home/embed/hyl/metadata_offload` 的 `source/rocksdb-zeroflush/` 是 **in-source cmake 构建**
  （`CMakeCache.txt`/`CMakeFiles/`/`db_bench` 均在 `.gitignore` 中）。**该目录及其构建产物不得触碰**。
- 会话期间用户修改工作持续推进：源文件改动由会话开始时的 6 个 M 增至 8 个 M
  （新增 `zeroflush/zeroflush_db.cc`、`zeroflush/zeroflush_db.h`），且 `make -j16 zf_valcheck` 正在编译中。
- ⇒ 本任务若落地，须全程在**独立 worktree + 独立 build 目录 + 独立 DB 目录**中进行，
  且并发编译会污染 NVMe 吞吐数值（见 §4）。

## 3. 已执行动作与副作用

- 仅执行只读勘察命令（`git log/branch/worktree list`、`ps`、`df/free`、`cat/ls`）。
- **未创建 worktree**（`git worktree add` 调用被用户中止，未落地）。
- **未构建、未跑测试、未删除任何文件**。
- 主工作树文件状态与进程状态未被本任务改变（`git worktree list` 仍只有主树）。

## 4. 恢复时的执行计划（尚未执行）

1. 独立检出：
   `git worktree add /home/embed/hyl/wt_b4b17b3 b4b17b3`
2. 独立构建（**刻意用 `-j12`**，为用户的 `-j16` 构建留出 CPU）：
   ```
   cd /home/embed/hyl/wt_b4b17b3/source/rocksdb-zeroflush
   mkdir -p build_rel && cd build_rel
   cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo && make -j12 db_bench
   ```
3. 结果目录按既有惯例放主仓库：
   `output/zeroflush_m5_perf/b4b17b3_50gb_1kb/`（DB 目录已被 `.gitignore` 的 `output/**/db_*/` 覆盖）。
   以 `zf_50gb_noprobe/run.sh` 为模板改 `BIN`/`RUN`/`DB` 路径后运行。
4. 采集：fill/read 的 `run.log`、`du -sh`、sst/log 计数与体积、$DB/LOG → fill.LOG/read.LOG。
5. 与 `native_50gb_samewindow/` 同窗对照；产出结果记录（建议覆盖进 `RUN_MANIFEST.md`）。
6. 清理：删除 50GB 级 DB 目录释放磁盘；若确认不再复用，`git worktree remove /home/embed/hyl/wt_b4b17b3`。

## 5. 遗留判断点（恢复前需确认）

1. **是否需要干净吞吐数值**：与用户的 `make -j16` 并发跑，会同时争抢 36 核与单块 NVMe，
   吞吐/延迟数据不可作为验收依据（仅可作"功能是否跑通、数据是否正确"的通过性证据）。
   若要可比数值，应选用户修改/构建窗口的空档，或明确接受污染并标注。
2. **臂的选择**：仅跑 ZF 臂（b4b17b3），还是同时重跑 native 同窗对照？历史已有 native 数据，
   但若要与本次同环境对齐，需一并重跑。
3. **是否追加正确性校验**（`zf_valcheck`）在 50GB 库上，或只做 db_bench 读写吞吐。
