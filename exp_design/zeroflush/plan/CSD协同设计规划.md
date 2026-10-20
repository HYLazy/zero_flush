# ZeroFlush × CSD 协同设计规划

> 参照实现：`source/CSD-CoKV/`（Samsung SmartSSD，Xilinx XRT + OpenCL，Int8/kernel 侧做整段压缩）
> 目标代码：`source/rocksdb-zeroflush/`
> 撰写时间：2026-09-19。**本文件是规划，未改动任何代码。**

---

## 0. 结论先行

1. **接口形态不必重新发明**：本仓 `zeroflush/M3_DESIGN.md` §10 已经定义过
   `MaterializeBackend`（`Probe()`/`Run()`）、两级卸载（L1 排序卸载 / L2 完整卸载）、
   `materialize_backend` 选项与 `ZfBackendEquivalence` 验收用例；`zeroflush/zf_request.h`
   已把设备契约（S1/S2/S5）写成**可编译但未接线**的 C++ 类型，并在文件头注明
   "P3 接线时消费本契约"。**规划的主干就是落地 M3.5，而不是新开一套。**
2. **可以加参数开关，而且很便宜**：`zf_materialize_backend = host|csd` 的端到端接线约半天
   （见 §5）。唯一非机械的部分是：若开关改变**落盘语义**，必须同步写进 ZFPROPS v2 并在重开时校验。
3. **但"照抄 CoKV"只能拿到一半收益，方向要选对**：CoKV 的切分是
   **设备只出算力、主机仍做全部磁盘 I/O**。而 ZeroFlush 当前的主要亏空是
   **物理写放大**（同窗 31GB 数据压实输出 921GB，23–31× vs native 12×），那是**字节量**问题、
   不是**算力**问题。算力卸载不减少写入字节。因此：
   - **低风险第一步**：materialize 的算力卸载（顺序读封存 WAL → 排序/归并），
     用于打掉"排序 + 全量值搬运"的主机 CPU；
   - **真正攻写放大的一步**：把 **L1→L2 sink / L2 清场**这条 ZF 独有的重写管线
     交给"能读自己盘、就地重写"的设备（CoKV 的 SmartSSD 正好是这个形态）。
     这一条要求数据在设备侧，属于 M3_DESIGN §10.2 的 "L2 卸载" 及其超集。
4. **前置条件（硬性）**：候选线目前连 50GB 写都跑不完（账本背压无人排空，见
   `B4B17B3_50GB_100GB_REPORT.md`）。**在停滞修复落地前不要启动物化卸载**——
   否则设备路径会把"无人推进"的问题搬到新的一层，且无法区分是设备慢还是账本停。

---

## 1. 从 CoKV 能移植什么、不能移植什么

### 1.1 可移植的切分原则（这是 CoKV 最有价值的部分）

| 原则 | CoKV 的做法 | ZF 对应 |
|---|---|---|
| **设备出算力，主机管持久化** | 设备输出 data+index 区，主机写 SST 尾部（properties/metaindex/footer）并 `Sync()` | 完全一致；ZF 只需在 `BuildTable`/`TableBuilder` 出口改为"注册设备产物" |
| **设备无状态** | 设备只加载 xclbin；持久化全在主机，输出仅通过 `VersionEdit` 可见 | 一致；ZF 还必须让设备产物通过 fence/账本（见 §6） |
| **一个 Compaction 一个内核** | `ShouldFormSubcompactions()` 对 CSD 返回 false | 一致；ZF 的 `materialize_parallelism` 是**任务级**并行，天然可映射为"多设备槽位" |
| **准入判定前置** | picker 之后、执行之前判断（输入文件数 ≤4、层策略、设备空闲） | 对应 `ZfMaterializeJob::ExecutePartition` 前的 `PlanLocked` 判定 |
| **失败即回落** | 10ms 内拿不到设备槽位就静默跑 CPU | ZF 应显式回退 + 计数 + 属性，不要静默 |

### 1.2 不可移植的部分（CoKV 的坑，别抄）

| CoKV 现状 | 为什么不能抄 | ZF 已有/应做的替代 |
|---|---|---|
| **快照无感知去重**：内核 keep-newest，从不查 `existing_snapshots_` | 有快照时会把旧版本删掉 → 读到错版本 | `zf_request.h` 已带 `earliest_snapshot` 水位（S5），设备必须保留 `seq > 水位` 的全部版本 |
| 不处理 RangeTombstone / BlobDB 引用 | 输出会丢删除语义 | ZF 契约已规定"墓碑按最底层语义保留"，设备不得自行决定 |
| **无输出校验**：结果状态被强制为 OK（`compaction_service_job.cc:1478`） | 内核算错会被静默安装 | 安装前必须走 §6 的校验门；A1 判定收紧（发布/fence/墓碑证据）不能因设备而放宽 |
| SST 格式假设硬编码：4KiB 块、restart=16、无 filter、checksum=0、写死 properties 块（含 `db_host_id`/`column_family_name="defalut"`） | 与带 comparator/压缩/bloom 的库不兼容 | ZF 侧应**显式拒绝**而非硬编码：`compression=kNoCompression` + `BytewiseComparator` + 固定 `format_version`，不满足就回退 host |
| 选项**未注册进 options 反射**（`OptionTypeInfo`），只能靠 db_bench 分支里写死 kernel 路径（且 fillseq/fillrandom/readrandom 用**不同** kernel 与策略） | 无法用 OPTIONS 文件/SetOptionsFromString 配置，行为随 benchmark 分支漂移 | ZF 的开关必须走正规 options 反射 + 单一配置入口（见 §5） |
| `column_family.h:32-37` **无条件** include `<CL/cl2.hpp>` | 无 XRT 的机器**编译不过** | ZF 设备代码必须放在编译开关之后，默认构建保持零设备依赖 |
| 设备槽位靠 `CSD_cv.wait_for(10ms)` 静默放弃；CSD↔路径靠 `cf_paths` 顺序与 `Compaction_accelerator_id` 手工对齐、无越界检查 | 调度行为不可观测、易错配 | ZF 用显式 `zf_csd_devices` 列表 + 槽位计数 + 属性暴露 |

### 1.3 ZF 相对 CoKV 的结构性优势（为什么这件事在 ZF 上更值得做）

1. **输入格式天生对设备友好**：CoKV 的设备必须实现 RocksDB block 格式解码（varint 重启点、
   footer、magic 校验、key 重建）。ZF 的输入是**自己的追加式 WAL**，
   顺序整读、自描述——`zeroflush/wal_format.h:5` 明确写了这个格式决策就是**为 CSD 做的**
   （"partition files are only sequentially read in full (CSD)"）。设备内核因此轻得多。
2. **输出约束比 CoKV 更紧、更易校验**：`zf_request.h` 的 S1/S2 规定
   常规任务输出恒 1 个文件、boundary rewrite 恒 2 个，且输出范围必须 ⊆ 发起分区的路由区间。
   CoKV 允许 ≤4 个任意不相交输出，校验面更大。**约束越紧，越容易在主机侧做廉价强校验。**
3. **契约与执行体已经分离**：`ZfCompactionRequest`/`ZfCompactionResult` 已存在且与
   `MaterializeDecision`（策略层）显式解耦。接线不需要重新设计数据结构。
4. **已有可判收益的指标**：`zf.materialize_micros` / `zf.materialize_sort_micros`
   就是为"判断是否值得卸载"而埋的（`M3_DESIGN.md:409`）。

---

## 2. 三条协同路径（先选路，再谈改代码）

| 路径 | 设备做什么 | 数据放哪 | 需要什么硬件 | 对 ZF 的攻击点 | 风险 |
|---|---|---|---|---|---|
| **C. GDS/cuFile 旁路**（推荐起步） | 排序 + 归并（可复用 `gParaKV-GC-pipeline-h2d` 的 CUDA+cuFile 骨架） | 主机 NVMe，cuFile 直读进设备内存 | GPU + 支持 GDS 的 NVMe | materialize 的 CPU 与搬运 | 低；不动持久化语义 |
| **A. 纯算力卸载**（= CoKV 原样） | 解码 + 4 路归并 + 去重 + 重编码 | 主机，且**必须经主机 PCIe 暂存**（CoKV 用 `pread` 灌进设备映射缓冲） | 任意加速器（含 SmartSSD） | 同上，但输入要过两遍 PCIe | 中；PCIe 搬运可能吃掉收益 |
| **B. 设备驻留 + 设备内计算**（价值最高） | 直接读**自己盘上**的封存 WAL / L1 文件，就地归并写回 | 设备自身 namespace | 真 CSD（可自读盘的 SmartSSD 类） | **L1→L2 sink / L2 清场的写放大** | 高；要动 WAL 落盘位置与主机读路径 |

**判断依据（用现有数据）**：

- ZF 的 materialize 是"顺序整读一批文件 → 排序 → 顺序写出"，与 CoKV 的 compaction
  在 IO 形态上同构（`M3_DESIGN.md:801-857`），所以路径 A/C 的工程映射是直接的。
- 但同窗对照显示 ZF fill 只有 native 的 0.34–0.59×，且差在
  **L1→L2 sink（ZF 248–476GB vs native 127GB）与 L2 清场（401–445GB vs 123GB）**，
  即**字节量**。路径 A/C 不减少字节 ⇒ **不能指望它解决 fill 落后**；
  它能解决的是"主机为排序/搬运花掉的 CPU 与内存带宽"。
- **只有路径 B 能减少主机侧重写字节**（设备就地重写，字节不出盘）。
  所以如果目标是"让 fill 追上 native"，主战场是 sink/L2clear，而不是 materialize。

> 建议的叙事与排期：**先用 C 打通契约与等价性（低风险、可回退），
> 再把 sink 管线作为 B 的首个目标**；A 只在设备是 SmartSSD 且 PCIe 不是瓶颈时才单独评估。

---

## 3. 要改哪些路径（按层列出，带位置）

### 3.1 新增文件

| 文件 | 内容 | 依据 |
|---|---|---|
| `zeroflush/materialize_backend.h` | `MaterializeBackend{Probe(), Run()}` + `MaterializeRequest/Output/Result` | `M3_DESIGN.md:801-857` 已给出草案；字段应与 `zf_request.h` 对齐（后者更严谨） |
| `zeroflush/backend_host.{h,cc}` | 把现有 `MaterializePartition`/`MaterializeMergePartition` 就地重构为 HostBackend 的执行体 | 等价性参照物（`ZfBackendEquivalence`） |
| `zeroflush/backend_csd.{h,cc}` | 设备句柄生命周期（CoKV 的 `column_family.cc:633-737` 是 ~120 行的模板）、槽位管理、结果回读 | 新 |
| `zeroflush/zf_wire.{h,cc}` | `ZfCompactionRequest/Result` ↔ 设备控制块 + 字节流的序列化（**当前契约只是 C++ 对象，没有线格式**） | 新；这是接线时最容易被低估的工作量 |

### 3.2 改造点（执行路径）

| # | 位置 | 改什么 |
|---|---|---|
| 1 | `zeroflush/materialize_job.cc:268` `ExecutePartition` | 加后端分支：`backend->Run(req)` 或走本地。**首选注入点** |
| 2 | `zeroflush/materialize_job.cc:2095` / `:2373` | 成为 HostBackend 的两个执行体；签名不变以便逐字节对照 |
| 3 | `zeroflush/materialize_job.cc:161` `RunMaterializeTaskPool` | 设备提交与槽位核算（替代 `std::thread` worker 池的本地线程）；设备忙 → 回落 host 并计数 |
| 4 | `zeroflush/materialize_job.cc:2249`（`BuildTable`）、`:2715-2747`（`NewWritableFile`+`TableBuilder`）、`:3053-3080`（边界重写） | 输出三处出口：改为"注册设备产物 + 主机补 SST 尾部" |
| 5 | `zeroflush/materialize_job.cc:2893` `ZfPickInstallLevel` | 不变。**设备永不安装**，层级仍由主机决定 |
| 6 | `db/flush_job.cc:1792`/`:2184`/`:2305` | 单次 VersionEdit 组装不变；但要在同批内加"设备产物 ⊆ 请求区间"的校验（S2） |
| 7 | `zeroflush/materialize_job.cc:2114-2122` `TryAcquireGenReadRef` + `SealedCacheReadRefGuard` | **必须保留并延伸到设备持有期**：设备正在读的 gen 不能被回收（这是 P0 关闭竞态修复的保护，不能因卸载而失效） |
| 8 | `db/flush_job.cc:2180-2311` | **唯一的 fence 推进点**，语义是"输入被完整消费 + 同批有提交的输出"。设备路径必须给出同等的"输入已消费"证据，否则 fence/账本会错 |
| 9 | `zeroflush/sealed_file_cache.{h,cc}`（`:102-140` 三本账；`:858-870` `PurgePending`） | 设备持有的段要计入 pending，且卸载前不得 unlink；需要"设备 pin"这一角色。`reclaimed_bytes` 仍只认真正 unlink |
| 10 | `zeroflush/zeroflush_db.cc:2694/2721` `ReadValue`/`ReadValuePinned` + `sealed_file_cache.cc:777/820` `Get` | **仅在路径 B（设备驻留数据）时需要**：读路径要新增设备句柄类型；路径 A/C 下读路径零改动 |
| 11 | `zeroflush/zeroflush_db.cc:3022` `Recover` | 孤儿代是否交给设备后端要明确（建议：**孤儿代一律走 host**，直到设备有等价性证明） |

### 3.3 策略层（不在首个版本动）

- `ZfMaterializeJob::PlanLocked`（`materialize_job.cc:652`）的 `MaterializeDecision`
  保持主机侧决策；`kSkip`/`kFallback` 语义不变。设备只是"另一种执行体"，
  **不参与"该不该做这个任务"的判断**（与 `zf_request.h` 文件头的分层约定一致）。
- L1→L2 sink 触发（`zeroflush_db.cc:1409` `RequestRangeSink` → 消费点
  `db_impl_compaction_flush.cc:3443`）属于**路径 B 的第二阶段**，首个版本不动。

### 3.4 存储放置（路径 B 才需要）

- WAL 落盘位置由 `PartitionedWalManager`（`zeroflush/wal_manager.cc:245` `MakeFileName`，
  `wal_dir/zfwal/zf-wal-<part>-<gen>.log`）决定；设备驻留需要能把这批文件放到设备 namespace。
  现有可用的低侵入手段：db_bench 已有 `--env_uri`/`--fs_uri` → `Env::CreateFromUri`
  （`tools/db_bench_tool.cc:9798-9813`），自定义 `FileSystem` 注册进 `ObjectRegistry`
  就能拦到**全部** ZF I/O——因为除三处 `std::fstream`（`zeroflush_db.cc:3912/3929/4302`）外，
  ZF 的读写全部经 `Env`/`FileSystem`。
- 这三处旁路（`ZF_DRAINING` 读写与 `CURRENT` 修复）在路径 B 下会成为**漏网**，需一并收口。

---

## 4. 关键契约点（比 CoKV 更严的要求）

1. **快照安全**：请求必须带 `earliest_snapshot`，设备保留 `seq > 水位` 的全部版本；
   `kMaxSequenceNumber` = 无约束。**禁止**采用 CoKV 的"无快照假设"。
2. **输出有界（S1/S2）**：常规任务 1 个输出、boundary rewrite 2 个；
   输出 `user_key` 范围必须 ⊆ `output_range`。**越界即判定任务失败**，不安装。
3. **墓碑语义**：设备按"是否最底层合并"保留墓碑，**不得自行决定**。
4. **安装原子性**：`DeleteFile(replaced_file_numbers) + AddFile(outputs)` 同一 VersionEdit；
   设备产物在通过校验前不得进入 VersionEdit。
5. **失败传播**：设备失败/超时必须回落 host 并在**同一批内**完成，不能留下"没有持有者的任务"
   ——当前 `D-129/D-130` 的停滞根因正是"账本没人推进"。设备槽位占用必须进入背压决策的可见范围。
6. **校验门**（推荐最小集合）：
   - 结构：输出区间 ⊆ 请求区间；`num_entries`/`smallest/largest`/`min/max_seqno` 自洽；
   - 交叉：设备产物与 host 直算结果在**调试模式**下逐字节比对（`ZfBackendEquivalence`）；
   - 兜底：`zf_valcheck` 在设备模式下必须能独立跑通（它按 offset 回读 WAL 校验值）。

---

## 5. 参数开关：可以加，这样加

### 5.1 建议的开关集合

| 开关 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `zf_materialize_backend` | string `host\|csd` | `host` | 主开关（M3_DESIGN §10.1 已定的名字） |
| `zf_csd_devices` | string（逗号分隔） | 空 | 设备列表；**不抄 CoKV 的"靠 cf_paths 顺序隐式对齐"** |
| `zf_csd_kernel_path` | string | 空 | xclbin / 内核产物路径（CoKV 是写死在 db_bench 分支里） |
| `zf_csd_policy` | enum | `kFreeSlot` | 槽位策略：空闲槽 / 指定槽 / 全空闲才做（对应 CoKV 三种 policy） |
| `zf_csd_max_input_files` | int32 | 4 | 设备端口上限，超出即回退 host |
| `zf_csd_fallback_policy` | enum `host\|fail` | `host` | `Probe()` 失败时：回退并 WARN（M3_DESIGN 要求"绝不因设备不可用而拒绝打开"） |

### 5.2 接线成本（逐点）

| 步骤 | 位置 | 量级 |
|---|---|---|
| 1 个字段（含注释） | `zeroflush/zeroflush_db.h:78-236`（`ZeroFlushOptions`） | ~15 行 |
| 1 个 flag | `tools/db_bench_tool.cc:836-893`（`DEFINE_*` zf_ 块） | 1 行 ×6 |
| 映射到 `zfo` | 同文件 `:5549-5645` 映射块 | ~6 行 |
| 校验 | `zeroflush_db.cc:4145-4290`（`Open`） | 少量 |
| 可观测 | `zeroflush_db.cc:738` `GetProperty`（建议 `zf.csd_tasks`/`zf.csd_fallbacks`/`zf.csd_micros`） | 少量 |
| **落盘语义（唯一非机械项）** | 若开关改变落盘形态：写入 ZFPROPS v2（`wal_format.h:97-120`，`EncodeZfPropsV2`/`DecodeZfPropsAuto`）并在 `zeroflush_db.cc:4471-4481`（现有 routing 校验旁）加失配拒绝 | 半天 |

**注意**：`ZeroFlushOptions` 里目前有 20+ 字段（如 `forbid_l0_install`、`use_zfprops`、
`dynamic_governance` 及全部 `governance_*`）**根本没有 db_bench flag**，只能写 C++ 才能设置。
如果 CSD 模式要能被 db_bench 与 `zf_valcheck` 直接验证，**必须补 flag**，否则会重演 CoKV
"配置只在 benchmark 分支里写死"的问题。

### 5.3 编译开关（别学 CoKV）

- 默认构建**不得**依赖 XRT/OpenCL。设备代码放 `#ifdef ROCKSDB_USE_CSD` 之后，
  运行期再用 `zf_materialize_backend` 选择。
- `Probe()` 的语义：设备不可用 → WARN + 回退 host + 计数；**不拒绝打开 DB**。

---

## 6. 分阶段落地与验收

| 阶段 | 内容 | 退出条件 | 依赖 |
|---|---|---|---|
| **S0（先决）** | 修掉账本背压停滞（候选线 50GB 口径 2.4–2.7GB 停、100GB 14.4GB 停） | 50GB/100GB fill 完整跑通，`pending` 能自然回落到 resume 以下 | 当前 P1 工作 |
| **S1** | `MaterializeBackend` 抽象 + `HostBackend` 重构 + flag（默认 host） | **零行为改动**：所有既有测试逐指标一致；`zf.materialize_micros` 不变 | — |
| **S2** | `ZfBackendEquivalence`：同输入 host vs 参照实现，逐字节比对输出 SST | 用例通过；`Probe()` 失败自动回退且 DB 可用 | S1 |
| **S3** | 路径 C：GDS/cuFile 的 L1 排序卸载（**只做无 L1 重叠的直装任务**：纯 WAL 扫描+排序，设备不解码 SST） | materialize 吞吐 ≥ host 的阈值（先用 `materialize_sort_micros` 占比定标）；结果与 host 等价 | S2 |
| **S4** | L2 完整卸载（设备产 data+index，主机写尾部） | 强制 `NoCompression` + `BytewiseComparator` + 固定 format_version；不满足即拒绝并回退 | S3 |
| **S5** | 路径 B 第一阶段：**L1→L2 sink / L2 清场**交给设备（攻写放大） | 同窗 50GB fill 的对 31GB 写入放大从 23–31× 显著下降；吞吐追上 native | S4 + 设备可读自身盘 |
| **S6** | 路径 B 完整：设备驻留 WAL + 主机读路径设备句柄 | 读延迟不劣化；恢复/孤儿代语义明确 | S5 |

**每阶段的硬性验收**：`zf_test` 全绿、`zf_valcheck` 在设备模式下可跑、崩溃用例
（封存未物化即崩、物化未回收即崩）行为不变。

---

## 7. 明确不做（Non-goals）

- ❌ 设备侧实现压缩 / bloom / 非字节序比较器（CoKV L2 都做不到，属同一个坑）。
- ❌ 设备参与"该不该做这个任务"的决策（保持 `zf_request.h` 的策略/契约分层）。
- ❌ 设备侧保存持久状态或推进 fence/账本（持久化权威必须唯一且在主机）。
- ❌ 在账本停滞修复前启动物化卸载（会把停滞问题叠加到新层，无法归因）。
- ❌ 抄 CoKV 的"靠 cf_paths 顺序/benchmark 分支隐式配置"。

---

## 8. 建议的第一步（可立即做，低风险）

**只做 S1**：引入 `MaterializeBackend` + `HostBackend`，`zf_materialize_backend` 默认 `host`，
行为零改动。收益：
1. 把 `MaterializePartition`/`MaterializeMergePartition` 变成可替换执行体，后续所有设备工作都只是"再写一个 backend"；
2. 立刻获得"后端等价性"的测试骨架（S2 的前置），这是设备路径唯一可靠的正确性保障；
3. 不引入任何硬件依赖、不影响候选线现状。
