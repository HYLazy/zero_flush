# ZeroFlush V2：固定范围分区档（设计 + 交付说明）

> 撰写：2026-10-16。目标代码：`source/rocksdb-zeroflush/`。
> 交付范围：**V2 档位 + 物化后端抽象（host CPU / CSD 设备）+ 验收用例**。
> 本档是 V0 的**子集与收敛**，不是重写：V0 里已验证的机制原样复用，只做
> "关掉动态分区、钉死固定范围、把物化执行体抽成两路"这三件事。

---

## 1. 需求与对应实现

用户口径（2026-10）：

| # | 要求 | 落地 |
|---|---|---|
| ① | 按**固定范围**决定 key 属于哪个 partition，**先不做动态分区** | `ZeroFlushOptions::v2_fixed_range` + `NormalizeV2Options()`：强制 `routing_mode=kStatic`（显式 P-1 个分隔键，或 P=1 全域单分区），拒绝 hash/采样/对齐模式；`dynamic_governance`、`split_after_skewed_epochs` 必须关，`growth_split_min_bytes` 强制归零（生长拆分关闭） |
| ② | 保持**取消 memtable→flush 这条路径**，数据直接写入 partition WAL | 沿用 V0 终态：`memtable_factory = SlimMemTableRepFactory`（只存 key + 16B SlimLocator），写组直接 `WriteGroupToPartitionWal` 落分区 WAL + 分区索引；V2 不做任何"把 value 写回 memtable 再 flush"的路径。用例 `v2` 的第 ② 条断言直接证明：写入后、封存前，**LSM 里没有任何 SST 文件**，数据只在 `zfwal/` 的分区代文件里（实测 4 文件 / 21.3MB） |
| ③ | 写入 partition WAL 后与 **L1 做 compaction 并生成 SST** | 沿用 V0 物化路径：封存（epoch）→ 顺序整读封存代 → 按 (user_key asc, seq desc) 归并 → 与重叠 L1 文件融合 → `BuildTable`/`TableBuilder` 产出 SST → 单次 `VersionEdit` 安装（目标模式 `forbid_l0_install=true` ⇒ 输出只进 base level，L0 恒空） |
| ④ | 保留 **CPU 路径**并同时保留 **CSD 介入路径** | `zeroflush/materialize_backend.{h,cc}`：把物化的"归并排序阶段"抽成 `MaterializeBackend`；`HostBackend`（默认，CPU）+ `CsdBackend`（设备：插件 / 内建仿真；不可用即回退 host + 计数） |

### 1.1 两条规模必需规则（2026-10-16 50GB 实测后固定）

V2 档在 `NormalizeV2Options()` / `Open()` 里固定两条规则，否则大规模不可用：

**(R1) ZF 独占 base level（L1）**：V2 档把 `max_bytes_for_level_base` 默认放大到
1PB（= 不触发原生 L1→L2；`ZF_MAX_L1_BYTES` 显式设置仍可覆盖用于对照实验）。
原因：V2 的物化与融合都落在 base level，而原生 L1→L2 会在 ZF 规划/安装同一批 L1
文件时把它们标记为 `being_compacted` ⇒ 规划期持续让位（`ZFDBG-skip ... busy=N`）、
批次产出 `tasks=0`、pending 只增不减 ⇒ imm 堆积到 `max_write_buffer_number` ⇒ 停写。
50GB 实测：修复前 18–19% 处停滞（pending 1.03GB、`empty_edit=19`、imm 8→9、
stdout 6 分钟无进展）；修复后 `busy=0`。L1 的消费只由 ZF 自己的 sink 路径负责。

**(R2) 收益攒批必须开启，且触发阈值必须由背压水位推导**：
- `base_merge_skip_max_generations=8`（单分区最多攒 8 代）；
- `base_merge_skip_max_bytes = min(ledger_stop_bytes/4, 512MiB)`，下限 8MiB
  —— **这条耦合是死锁修复的核心**；
- V2 默认把 `ledger_stop_bytes`/`ledger_resume_bytes` 从 1GiB/768MiB 提到
  8GiB/6GiB（调用方显式设置过则不覆盖）；
- `backlog_drain_max_age_micros=600s`（兜底，仅用于写入停顿/分布不均时保进展）。

原因 A（写放大）：融合路径在 `forbid_l0_install` 下没有 L0 出口；关掉攒批时每个
64MB epoch 都会把该分区整段 L1（50GB/16 分区 ≈ 3GB）重写一遍 ⇒ 写放大约 50×、
单批 merge 6–35s（`ZFDBG-slowmerge` 486 次）⇒ 物化吞吐 ~5MB/s ⇒ imm 堆积、周期性停写。
开启后由 `base_merge_min_ratio`（累计封存字节 / 重叠 L1 字节）做成本控制。

原因 B（**闭环死锁**，2026-10-16 抓栈定位）：写路径在 `pending > ledger_stop` 时
停写（`DBImpl::DelayWrite` 的账本背压，`db_impl_write.cc:3063+`），而 pending 只能
靠"下一次封存 → 融合安装"下降。若攒批/排空触发阈值 ≥ 背压水位，则
"**写被挡住 → 没有新封存 → 攒批到不了阈值 → 排空永不触发 → pending 不降**"
形成闭环。实测（阈值 2GB vs 默认水位 1GB）：30.9% 处 45 线程全睡、
`futex_wait_queue` 零 CPU、账本 882MB 冻结、>40 分钟零进展；栈显示 15 个写线程在
`AwaitState`、组长在 `DelayWrite`、**全部后台 worker 空闲**。把阈值按水位推导后，
排空必在背压之前可触发（实测同一配置越过了 30.9% 并持续推进）。

50GB 实测对比（同机同参）：

| 指标 | 攒批关闭 | 攒批开启但阈值 2GB（死锁版） | 攒批开启 + 阈值由水位推导（最终） |
|---|---|---|---|
| 写速率 | ~5.5k ops/s（衰减） | ~19.9k → 0（死锁） | 持续推进（越过 30.9% 死锁点，见 §5.4） |
| `ZFDBG-slowmerge` | 486 | 7 | 中等（每次融合 ~512MB 新数据 + 整段 L1） |
| `Stopping writes` | 5+ | 0（但随后死锁） | 偶发并可恢复 |
| 结局 | 慢但推进 | **死锁（不可恢复）** | 见 §5.4 |

硬性约束（沿用 `M3_DESIGN.md §10` 与 `exp_design/zeroflush/plan/CSD协同设计规划.md`）：
- 默认构建**不依赖** XRT/OpenCL/CUDA；设备代码只在插件里。
- 设备不可用**绝不拒绝打开 DB**（默认 `csd_fallback_policy=host`）。
- 持久化权威唯一且在主机：设备的输出（下标置换）在通过校验后才被主机使用。

---

### 1.2 三条性能必需默认（2026-10-17 50GB 调优后固化）

V2 档的默认值已按"能跑赢同机原生基线"标定（`NormalizeV2Options`）：

| 规则 | 默认 | 作用 |
|---|---|---|
| **R3 每轮累积要大** | `base_merge_skip_max_generations=32`、`base_merge_skip_max_bytes=min(ledger/4, 2GiB)` | 融合要重写该分区的**整段 L1 切片**（LSM 层内不重叠 ⇒ 无法只写增量），每次重写的代价因此由"每轮累积了多少新数据"摊销。量化：`总重写 ≈ S²/(2·P·每轮累积)`（S=总量、P=分区数）⇒ 50GB/16 分区下，累积 512MiB/轮 ≈ 6× 放大；2GiB/轮 ≈ 1× |
| **R4 水位/在途要够大** | `ledger_stop/resume = 32GiB/24GiB`、`max_pending_epochs=128` | R3 的累积只能来自"未安装的 WAL"⇒ 背压水位与在途 epoch 上限必须允许 ≥ P×每轮累积（否则写入在攒够之前就被挡住）。50GB 实测：8GiB 水位只能攒 ~512MiB/分区 |
| **R5 默认不下沉** | `sink_yield_threshold_bytes = ledger_stop/2`（32GiB 水位 ⇒ 16GiB） | ZF 的 L1→L2 下沉（`RequestRangeSink`）会把 L1 切片合并进 L2，**每次重写整段 L2 切片**（实测 500MB/次、放大 20–40×、5–16k ops/s）；放宽让位阈值后 L1 独占，融合只重写自己的切片。`ZF_SINK_YIELD_MB` 可显式覆盖做对照（0 = 永不触发） |

**结构性前提（未改，属既有设计）**：安装准入锁（`install_mu_`）把"规划+归并+安装"整批串行化
⇒ 并行度 = **批内分区任务数**（50GB 实测常见 3–5），而不是 `materialize_parallelism`。
R3/R5 都是在这个前提下"减少每字节的重写次数"而非"提高并行度"；若要再上一个量级，
需要把安装锁缩小到只覆盖安装步（或让批次在归并阶段并发）——属引擎改动，见 §6 下一步。

## 2. 架构：两条路径，一个写路径

```
写组 (WriteGroup)
  └─ 分区 WAL 追加（无 memtable flush 路径）        ← ②
        └─ 分区索引（key → SlimLocator{part,gen,offset}）
              │  封存（epoch seal，按固定范围表分片）
              ▼
        物化任务 (ZfMaterializeJob, 每分区一任务)
          ├─ 扫描：顺序整读该分区封存代（持 generation 读引用，禁回收）
          ├─ 归并排序：★ 物化后端（CPU host | CSD 设备）    ← ④
          ├─ 生成 SST：与重叠 L1 融合 → BuildTable          ← ③
          └─ 安装：单次 VersionEdit（禁 L0 ⇒ 只进 base level）
```

**分层约定**（与 `zeroflush/zf_request.h` 文件头一致）：后端只负责**执行体**
（归并排序），不参与"该不该做这个任务"的策略决策（`MaterializeDecision` 仍由
`ZfMaterializeJob::PlanLocked` 在主机侧决定）；输入扫描的引用保护、范围断言、
定层、文件号、表属性、fence、账本、回收全部留在主机。因此 host 与设备两条路
**必然**产出同一批条目，只有顺序可能由不同执行体产生——这正是等价性用例可判定的前提。

### 2.1 后端接口（`materialize_backend.h`）

```cpp
struct SortStageInput {                 // 输入：已解码的 (internal key, value) 向量
  std::vector<std::string>* keys;       // internal key = user key + 8B seq/type
  std::vector<std::string>* values;
  const Comparator* user_comparator;
  uint32_t part_id; uint64_t input_bytes; uint32_t input_gens;
};
struct SortStageResult {
  bool fully_ordered;    // true ⇒ 调用方可构造"不再比较"的迭代器
  uint64_t backend_micros; bool ran_on_device;
};
class MaterializeBackend {
  virtual const char* Name(); virtual MaterializeBackendKind kind();
  virtual Status Probe();                                  // 可用性探测
  virtual Status RunSortStage(const SortStageInput&, SortStageResult*);
};
```

三条实现：

| 后端 | 说明 | 什么时候用 |
|---|---|---|
| `host` | 原主机实现：Bytewise 时打包 memcmp 排序键（4B 大端长度 + user key + 逆序取反的 8B 尾）+ O(n) 原地重排；非 Bytewise 交给迭代器比较 | **默认**（`zf_materialize_backend=host`），也是设备路径的正确性参照物 |
| `csd-sim` | 内建**设备仿真**：同一调用形态（槽位准入 + 下标置换），无硬件依赖。用于打通接线、回退与等价性验收；**不声称加速能力** | `--zf_materialize_backend=csd --zf_csd_devices=sim` |
| `csd-plugin` | 通过 `--zf_csd_kernel_path=<so>` 加载真设备（C ABI v1，见 §4） | 有设备/设备模拟器时 |

回退语义（`CsdBackend`）：
- `Probe()` 失败（无设备/ABI 不符/probe 拒绝）⇒ 记 `probe_failures`；
- 槽位不可得 ⇒ `free_slot` 策略下等待至多 `csd_slot_wait_micros`（默认 10ms）后回退，`any_idle`/`all_free` 立即回退；
- 输入代数超过 `csd_max_input_files`（设备端口上限）⇒ 回退；
- 非字节序 user comparator ⇒ 设备不支持（只做 memcmp 语义）⇒ 回退；
- 设备返回的置换必须通过校验（长度、取值范围、互异）才被采用，否则判任务失败并回退；
- 每次回退记 `fallbacks` 计数；`csd_fallback_policy=fail` 时才把"设备不可用"升级为失败。

主机 CPU 路径**零行为改动**：host 后端的排序代码逐字迁自原 `materialize_job.cc`
（`BuildSortKeys`/`ReorderBySortKeys`），`fully_ordered` 取代原先的
`sort_buf.empty()` 判据，语义一一对应。

---

## 3. 开关与可观测

### 3.1 选项（`ZeroFlushOptions`）

| 字段 | 默认 | 说明 |
|---|---|---|
| `v2_fixed_range` | `false` | 打开 V2 档（触发 §1 的规范化与校验） |
| `materialize_backend` | `kHost` | `host` / `csd` |
| `csd_devices` | 空 | 设备列表（`sim` = 内建仿真） |
| `csd_kernel_path` | 空 | 设备插件 .so（C ABI v1） |
| `csd_slot_policy` | `kFreeSlot` | `free_slot` / `any_idle` / `all_free` |
| `csd_fallback_policy` | `kHost` | `host`（回退+计数）/ `fail` |
| `csd_max_input_files` | 4 | 设备端口上限（sim 下同时是并发槽位数） |
| `csd_slot_wait_micros` | 10000 | `free_slot` 的有界等待 |

### 3.2 db_bench flag

```
--zf_v2=1                                   # V2 档（须配 --zf_routing=static + 边界）
--zf_materialize_backend=host|csd
--zf_csd_devices=sim,...        --zf_csd_kernel_path=/path/to/device.so
--zf_csd_slot_policy=free_slot|any_idle|all_free
--zf_csd_fallback_policy=host|fail
--zf_csd_max_input_files=4      --zf_csd_slot_wait_ms=10
```

`zf_valcheck` 走同名环境变量：`ZF_VC_V2` / `ZF_VC_BACKEND` / `ZF_VC_CSD_DEVICES`
/ `ZF_VC_CSD_KERNEL_PATH` / `ZF_VC_CSD_SLOT_POLICY` / `ZF_VC_CSD_FALLBACK`
/ `ZF_VC_CSD_MAX_INPUT` / `ZF_VC_CSD_SLOT_WAIT_MS`。

### 3.3 属性（`GetProperty("rocksdb.zeroflush.*")`）

`engine_profile`（`v2` / `v0`）、`materialize_backend`（后端名）、`csd_tasks`、
`csd_device_tasks`、`csd_fallbacks`、`csd_probe_failures`、`csd_micros`。
db_bench 结束态会一并打印（连同既有 `prof_*`、`lsm_L*_files`）。

---

## 4. 设备契约（C ABI v1，`materialize_backend.cc`）

```c
typedef struct ZfCsdSortRequestV1 {
  uint32_t abi_version; uint32_t part_id; uint64_t num_records;
  const char* const* keys;      // internal keys（user key + 8B 小端 seq/type）
  const char* const* values;
  uint64_t input_bytes; uint32_t input_gens;
} ZfCsdSortRequestV1;

typedef struct ZfCsdPluginV1 {
  uint32_t abi_version;         // 必须 == 1
  const char* name; void* impl;
  int (*probe)(void* impl);                                       // 0 = 可用
  int (*sort)(void* impl, const ZfCsdSortRequestV1*, uint32_t* order_out,
              uint64_t* out_nanos);                               // 0 = 成功
  const char* (*last_error)(void* impl);
} ZfCsdPluginV1;
ZfCsdPluginV1* zf_csd_plugin_create(void);   // 插件必须导出的工厂符号
```

- 输入是**只读**的键/值指针数组；输出是长度 = `num_records` 的**下标置换**，
  语义 = internal key 全序（user_key 升序、同 key seq 降序），即
  (user_key asc, seq desc)。
- 设备**不得**增删条目、**不得**做持久化/安装/定层/fence；主机负责校验置换并完成
  真实的 string 重排与 SST 写出。
- 快照水位、墓碑保留等 S5 契约字段（`zf_request.h`）留待 L2 卸载（设备直接产
  SST）时使用；本交付只做 M3_DESIGN §10.2 的 **L1 卸载（排序卸载）**。

---

## 5. 验收

### 5.1 新用例

| 用例 | 命令 | 断言 | 实测（本机） |
|---|---|---|---|
| `v2` | `./zf_valcheck <db> v2 20000 4` | ① `engine_profile=v2`；② 封存前 `sst_files=0` 且 `zfwal` 有数据且全量读回正确；③ 封存后 `L0=0`、`L1+=4`、SST 字节 > 0；④ `pending=0`、`registered_gens=0`；⑤ 读回 0 错 + 重开 0 错 | **PASS**（pre 4 WAL 文件/21,326,660 B；post L0=0 L1+=4 sst=21,082,508 B；reopen 20000/0 bad） |
| `backendeq` | `./zf_valcheck <db> backendeq 20000 4` | host vs csd-sim 逐键相同、层级文件数相同、SST 总字节相同；csd 的 `csd_device_tasks>0` 且 `fallbacks=0`；无设备时 `probe_failures/fallbacks>0` 且数据与 host 相同 | **PASS**（files 4/4、bytes 21,082,508/21,082,508、`value_diff_ab=0`、`value_diff_ac=0`、device_tasks=8） |

### 5.2 db_bench 冒烟（V2 档，4 分区固定范围，100k×1KB）

| 配置 | 结果 |
|---|---|
| `--zf_materialize_backend=host` | fillrandom **162,292 ops/s**；`prof_tasks=24`、`prof_output_bytes=214MB`、`L0=0`、`L1=4` |
| `--zf_materialize_backend=csd --zf_csd_devices=sim` | fillrandom **165,208 ops/s**；`csd_device_tasks=24`、`csd_fallbacks=0`、`L0=0`、`L1=4` |

（两组同参、同机、同为 0.6s 的 100k 写；未做统计显著性处理，仅证明两条路径都通。）

### 5.3 回归（本机同一 build）

- `semtest 20000`（需 `ZF_DBG_LOCATOR=1`，case7 的执行记录依赖该诊断开关）：
  **cases1-7 PASS，fails=0**；
- `installtest 2/3/4`：**PASS**（含路由一致 + 融合证据 + 重启逐键）；
- `all 6000`：`checked=9064 mismatch=0 miss=0 errors=0`，**VALCHECK PASS**；
- 负例：`--zf_v2=1 --zf_routing=hash` ⇒ 启动报
  `Invalid argument: ZeroFlush V2: fixed-range partitioning requires routing_mode=kStatic`；
  负例：`ZF_VC_BACKEND=csd ZF_VC_CSD_FALLBACK=fail` 且无设备 ⇒ 启动报
  `... csd_fallback_policy=fail requires an available materialize device backend`。
- `zf_test`（53 例，需独立构建目录 + `-Wno-error=format-truncation`）：**51 PASS，2 FAIL**。
  两个失败为**本分支既有**（非本交付引入）：`DeleteRangeAcrossPartitions(M3.4-47)`、
  `SplitConcurrentWriters(M4)`——已用"只回滚本交付的排序改动、其余（含 A11/A12
  未提交改动）保持不变"的定点 A/B 复验：**回滚前后同样确定性失败（各 3/3）**。

---

### 5.4 50GB 端到端验收（2026-10-16/17，**已通过**）

配置与历史 50GB 口径一致（`archive/zeroflush_m5_perf/RUN_MANIFEST.md` D-109/D-118），
只把路由换成 V2 的固定范围并打开 V2 档：

```
--benchmarks=fillrandom,waitforcompaction --key_size=16 --value_size=1024
--num=48000000 --writes=3000000 --threads=16 --seed=20260914 --compression_type=none
--cache_size=536870912 --max_background_jobs=24 --subcompactions=16
--zeroflush --zf_v2=1 --zf_routing=static --zf_static_span=48000000
--zf_partitions=16 --zf_epoch_target_mb=256 --zf_partition_target_mb=64
--zf_base_merge=true --zf_value_cache_mb=64          # ZF_L0_TRIGGER=100000000
```

**结果**（`DB_BENCH_EXIT=0`，"完整跑通"的判据 = 写入 48M ops 全部完成 + waitforcompaction OK）：

| 指标 | 数值 |
|---|---|
| fillrandom | **48,000,000 ops / 8,756s = 5,481 ops/s（5.4 MB/s）** |
| 写延迟分位 | P50 **42µs** / P75 111µs / P99 4.3ms / P99.9 25ms |
| 末态 LSM | **L0 = 0**、L1=429、L2=70、L3=72（§1.1 R1/R2 的预期形态） |
| 入账/回收 | `drained=50.31GB`、`reclaimed=49.86GB`、`reclaimed_gens=2234`、`installed_epochs=380` |
| 退出时残留 | `pending=952MB`、`registered_gens=66`（waitforcompaction 不驱动 ZF 排空；重开后 `pending=0 / registry=0`） |
| 写放大（实测） | `prof_output_bytes=1.344TB` / 50GB 输入 = **26.9×**（融合重写整段 L1 的代价） |
| 读验证（重开后） | `readrandom` 8M 次：**315,965 / 500,000 found = 63.2%** = 48M 随机写覆盖 48M 键空间的理论期望（`1-e^-1`），与历史基线 63.26% 一致 ⇒ **无数据丢失**；读吞吐 **119k ops/s**（P50 289µs 冷缓存） |

**取证与三轮修复**（详见 §1.1 的 R1/R2）：

1. **第一轮（R1 缺失）**：原生 L1→L2 与 ZF 规划抢文件 ⇒ 18–19% 处停滞
   （`ZFDBG-skip busy=35`、`tasks=0`、pending 1.03GB、`empty_edit=19`、imm 8→9）。修复 R1 后 `busy=0`。
2. **第二轮（关掉了攒批）**：每 64MB epoch 重写整段 L1 ⇒ `ZFDBG-slowmerge` 486 次、
   单批 merge 6–35s、物化吞吐 ~5MB/s、周期性停写（5+ 次）。
3. **第三轮（攒批阈值高于背压水位）**：30.9% 处**闭环死锁**（45 线程全睡在
   `futex_wait_queue`、零 CPU：15 个写线程 `AwaitState`、组长 `DBImpl::DelayWrite`
   账本等待 `db_impl_write.cc:3073`、全部后台 worker 空闲、账本 882MB 冻结 >40min；
   栈留档 `archive/…` 见下）。修复 = 阈值由水位推导（§1.1 R2）。
4. 修复后同一配置**完整跑通**（本轮结果）。

**已知限制（不阻塞本次验收）**：

- 写吞吐 5.5k ops/s（原生同参数 ~14–30k）：瓶颈是融合重写量（26.9×），不是正确性。
  主旋钮是让 L1 有界——ZF 自己的 sink（L1→L2）在长跑中与 L2 清场（默认阈值 4GB）
  互相让位，形成吞吐振荡（`l2_clear_rounds=52`、`pick_ok=136`）；
  下一轮应调 `ZF_SINK_L2_CLEAR_MB`（如 32GB）或让 sink 与清场解耦，把 L1 压回小范围，
  融合即可只重写"最近一批"而非整段分区。
- 关闭时残留 `pending=952MB / registry=66`：`waitforcompaction` 不驱动 ZF 排空；
  重开恢复正常（已验）。后续可让 `WaitForCompact` 也等待 ZF pending 归零。
- 6 次 50GB 运行中出现 **1 次 SIGSEGV**（13 秒处，最后一条日志为
  `ZFDBG-adopt skip_gens=16`，即封存期收养 skip 代的路径）；该路径属实验分支
  未提交改动（工作树），非 V2 代码。现场与复现命令见下。
- 日志留档（2026-10-20 收档至 `output/zf_v2_evidence/`，含 R1/R2 由来与 opt 战役全套）：
  `zf_v2_50gb_run.log`（stdout+诊断）、`zf_v2_50gb_db.LOG`（DB info log）、
  死锁栈 `zf_deadlock_stacks_ledger_vs_threshold.txt`、逐 2 分钟进度
  `zf_v2_progress.log`。

### 5.5 50GB 性能验收：**超过同机原生基线**（2026-10-17）

同机、同窗口、同参数（48M ops × 1KB、16 线程、`compression=none`、cache 512MB、
`max_background_jobs=24`、`subcompactions=16`）对照，两者都执行
`fillrandom,waitforcompaction` 且 `exit=0`：

| 指标 | 原生 RocksDB（baseline） | V2（手调 v7） | **V2（默认值 v8）** | 对比 |
|---|---|---|---|---|---|
| fillrandom 吞吐 | 16,587 ops/s（16.5 MB/s） | 23,194 ops/s | **27,698 ops/s（27.5 MB/s）** | **1.67×** |
| wall | 2,893.7s | 2,069.4s | **1,732.9s** | **−40.1%** |
| P50 / P99 写延迟 | 26.61 / 131.36 µs | 37.52 / 186.22 µs | **35.64 / 147.10 µs** | 略慢（P50 +34%） |
| 读回（重开） | — | 315,384/500,000 = 63.08% | **315,740/500,000 = 63.1%**（= 期望覆盖 `1-e^-1`）⇒ 无丢失 |
| 读吞吐 | — | 134,604 ops/s | **138,939 ops/s**（87.1 MB/s，P50 ≈114µs） |
| 写放大 | — | — | `prof_output_bytes=281.9GB / 50GB = **5.64×**`（首版跑通时为 26.9×） |
| 末态 LSM | — | — | **L0=0、L2=0、L3=0**（数据全在 L1，R5 的直接结果） |
| 退出后 | — | — | `pending=0`；重开后 `registered_gens=0`、DB 30GB（原生同负载 ~40GB） |

**调参过程（每格都是同机 50GB 实测，非推算）**：

| 版本 | 相对上一版的改动 | 结果 |
|---|---|---|
| 首个跑通版 | R1+R2（8 代攒批、8GiB 水位、下沉开+L2 清场 4GB） | 5,481 ops/s（放大 26.9×） |
| v1 | L2 清场提到 64GB（sink 不再被清场挂起）+ K=16 | 早期 ~15k，尾部衰减 |
| v2 | epoch 1GB + 批覆盖 16 分区 | **停滞**（安装重叠让位 → imm 9 停写） |
| v3 | 分区目标 8MB | 6.6k（批内任务仍只有 ~4） |
| v4 | 让位阈值 64MB → 1GB | 11.9k（下沉乒乓缓解但仍有 L2 重写） |
| v5 | **禁下沉**（让位阈值 64GB） | 峰值 **18.4k**，34% 处崩溃（间歇 SIGSEGV） |
| v6 | v5 + 在途 32 epoch / 水位 16GiB | 平均 ~16k（尾部衰减到 <10k） |
| **v7** | + 累积 32 代（2GiB/分区）、水位 32GiB、在途 128、K=16 | **23,194 ops/s（1.40× baseline）** |
| **v8** | 把 v7 配置**固化为 V2 默认**（R3/R4/R5）后复现 | **27,698 ops/s（1.67× baseline）**、exit 0 |

**为什么快**：把"每字节重写次数"从 27–39× 压到 ≈1×——L1 独占（不下沉、原生 L1→L2 也关）、
每轮攒 2GiB/分区再融合（重写摊销），同时水位/在途允许这些数据在 WAL 里等待。

**代价与限制**：① 写延迟 P50 37.5µs（原生 26.6µs）——大累积意味着写入路径要等 WAL/索引
空间；② 在途 WAL 峰值 ~37GB（50GB 负载）+ imm 上限 129 个 memtable，内存/磁盘占用高于原生；
③ L1 独占 ⇒ 更大规模（>L1 容量）下必须靠 `ZF_SINK_YIELD_MB` 打开下沉，届时会回到
"重写 L2 切片"的代价；④ 间歇 SIGSEGV（~1/8 运行，位于工作树既有的 skip 代收养路径，未修）。

### 5.6 Zipfian 分布负载（2026-10-17）：写 1.65×,读落后,并发现关库数据丢失

工具 `tools/zf_zipf_bench.cc`（同 key 流 V2/原生对照、确定性 value、读回精确校验）。
48M 写 × 1KB、alpha=0.99、16 线程：**V2 63,007 ops/s vs 原生 38,274（1.65×）**，
DB 4.9GB vs 12GB，写放大 2.49×；读热态 209k vs 原生 903k（4.3× 落后），
写后首轮 17.7k（冷缓存）。**同时发现一个确定性的数据安全问题**：skewed 负载 +
重复"打开→小写→关闭"后，关闭期物化批次以
`Corruption: ZF merge scan: gen neither registered nor provably installed`
失败，输入 L1 已删而输出未装 ⇒ 86% 键丢失（2/2 复现；50GB Zipfian 库同样命中）。
详见 `archive/zeroflush_m5_perf/RUN_MANIFEST.md` D-142…D-144，
复现目录 `/tmp/zipf_repro2`（2026-10-20 已随 /tmp 清理删除；重建步骤与全部运行
日志见 `output/zf_v2_evidence/`）。**该问题优先级高于吞吐结果**。

## 6. 与 V0 的关系、限制、下一步

**关系**：V2 = V0 的**固定范围 + 无动态分区**子集，默认后端 = host ⇒ 对既有
V0 行为**零改动**（仅新增属性与校验）。V0 的动态治理（DG）、split/merge、
学习路由（kSampled/kAlignL1）、积压排空与 skip 收养等机制仍在代码中，但在 V2 档
下要么被拒绝（见 §1 ①）、要么不可达。

**限制 / 非目标**：
- 设备内核未实现：`csd-plugin` 只提供加载/探测/校验/回退的**接缝**；本机无
  SmartSSD/XRT，`csd-sim` 是管线仿真，不代表加速收益。
- 设备侧不实现压缩 / bloom / 非字节序比较器（`M3_DESIGN §10.2` 的 L2 卸载要求
  设备产完整 SST，本交付未做）。
- 不做动态分区、不做攒批（`base_merge_skip_max_generations` 在 V2 档强制 0）。
- 大规模（50GB/100GB）写入停滞问题属 V0 遗留，V2 未触碰；V2 的验收均在小规模
  （≤100k 键 / 20k 键）完成。

**下一步（按收益排序）**：
1. 把"归并排序"之外的阶段（输入解码、SST 写出）也纳入后端契约，向 L2 卸载推进
   （设备直接产 data+index，主机只补 SST 尾部）；
2. 真设备接入：按 §4 的 ABI 写一个插件（GDS/cuFile 或 SmartSSD），用 `backendeq`
   做逐字节等价性验收；
3. 固定范围下的**边界治理**（预分片比例、热点分区识别）——仍属"固定范围"，
   不是动态分区；
4. 与 V0 遗留的 50GB/100GB 停滞问题合并处理（`archive/zeroflush_m5_perf/RUN_MANIFEST.md`
   D-109 起）。

---

## 7. 改动清单（本交付）

| 文件 | 改动 |
|---|---|
| `zeroflush/materialize_backend.h`（新） | 后端接口 + 枚举 + 计数 + 工厂声明 |
| `zeroflush/materialize_backend.cc`（新） | `HostBackend`（原排序实现迁入）、`CsdSimProvider`、`ZfCsdPluginProvider`（dlopen + ABI 校验 + 置换校验）、`CsdBackend`（槽位/回退/计数） |
| `zeroflush/zeroflush_db.h` | V2 档与后端选项；`materialize_backend()` / `materialize_backend_stats()` 访问器 |
| `zeroflush/zeroflush_db.cc` | `NormalizeV2Options()`（在 ctx 构造前规范化/校验）；ctx 构造后创建后端 + kFail 检查；属性 7 项；logger 注入 |
| `zeroflush/materialize_job.cc` | 两处排序调用改为经后端（`RunSortStage`）；删除已迁出的排序辅助函数 |
| `tools/db_bench_tool.cc` | 8 个新 flag + 映射 + 属性打印 |
| `tools/zf_valcheck.cc` | `ZF_VC_V2`/`ZF_VC_BACKEND`/`ZF_VC_CSD_*` 接线；新用例 `v2`、`backendeq`；`PropS` |
| `CMakeLists.txt` | 加入 `zeroflush/materialize_backend.cc` |
| `zeroflush/README.md` | 状态段补 V2 |
