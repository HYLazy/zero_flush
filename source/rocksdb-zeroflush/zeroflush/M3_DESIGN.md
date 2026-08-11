# ZeroFlush M3 方案设计：范围路由 · 消除 L0 · API 完备

> M2 把 ZeroFlush 从"不可持续的永不 flush"变成"封存—物化—回收闭环"。
> M3 要回答的是**下一个层级的问题**：物化产出的 SSTable 为什么必须先落 L0、
> 再被 L0→L1 compaction 重写一遍？以及 ZeroFlush 何时能承载真实工作负载
> （多列族、Merge、DeleteRange）。

**版本**：v1.0
**状态**：M3.0 已实现（13 例全绿，2026-08-10），M3.1+ 待实现
**前置**：[M2_DESIGN.md](M2_DESIGN.md)（M2 债务已由 M3.0 清偿）
**核心决策**：路由函数从 hash 换成 **PartitionTable 范围路由**，使物化输出的 P 个
SSTable 键范围两两无交集，从而可以**跳过 L0 直接安装到 base level**；稳态下由
**Materialize-into-L1 融合归并**彻底消除 L0

---

## 1. 目标与非目标

### 1.1 目标

| # | 目标 | 验收标准 |
|---|---|---|
| H1 | **范围路由正确** | 同一 epoch 内任意两分区的输出 SST，按 `Options::comparator` 判定键范围无交集（断言 + 测试强制） |
| H2 | **批量装载零 L0** | 空库连续写入 10 个 epoch，`base_level` 从空开始被逐段填充，全过程 L0 文件数恒为 0，L0→L1 compaction 字节数为 0 |
| H3 | **稳态零 L0**（M3.3） | 稳态持续写入下 L0 文件数 ≤ `l0_fallback_tolerance`（默认 0）；总 compaction 写字节相对 M2 下降 ≥ 30% |
| H4 | **多列族** | N 个 CF 并发写入/读取正确；一个 epoch 覆盖全部 CF，回收在最后一个 CF 的 imm 析构后发生 |
| H5 | **Merge / DeleteRange** | `Merge` operand 链正确（含跨 epoch）；`DeleteRange` 跨分区语义正确、重开后仍生效 |
| H6 | **卸载后端可插拔** | `MaterializeBackend` 抽象下 host 后端通过全部用例；切换到 csd 后端时不可用可自动回退且有日志 |
| H7 | **可观测** | `zf.*` 指标经 `GetProperty` 暴露，覆盖封存/物化/回收/路由倾斜四类 |

### 1.2 非目标（明确推迟）

| 项 | 推迟到 | 理由 |
|---|---|---|
| 事务（`two_write_queues` / `seq_per_batch` / 2PC） | M4 | 与写路径分流强耦合，见 §9.4 只给接口约束不做实现 |
| 宽列（`kTypeWideColumnEntity`）与 BlobDB 共存 | M4 | value 已被 offload，再叠一层间接寻址收益不明 |
| 每分区独立 MemTable | M4 | 见 §3.3 —— M3 的范围路由**不需要**它；它换来的是并行插入，属于另一条优化轴 |
| 在线改 P（不停机 reshard） | M4 | M3 只支持"分裂时 P 单调增长"，且只在 epoch 边界发生（§5.5） |
| 压缩（compression）下的 CSD 卸载 | M4 | 设备侧压缩库可用性依赖硬件平台 |

---

## 2. 准入门槛：M2 债务清偿（M3.0 已完成）

**M3 第一行代码之前必须先把 M2 收干净。** M3.0 已清偿全部 M2 债务：

```
2026-08-10 最终回归（./zf_test）:
13/13 PASS, 0 FAIL, 45 行输出（0 DEBUG）
```

| # | 缺陷 | 根因 | 修复 |
|---|---|---|---|
| **R1** | `FreezeReopen` / `MultiEpoch` 重开后 Get 返回空 | 孤儿封存代在 `Recover()` 后未登记进 `SealedFileCache`，首次 `Seal()` 之前的读窗口内 Get 必然 NotFound | `AddRecoveryGens()` 登记待收养集合 + `AdoptRecoveryGensInto()` 在 Seal 原子窗口内收养（[zeroflush_db.cc](zeroflush_db.cc) / [sealed_file_cache.cc](sealed_file_cache.cc)） |
| **R2** | `IteratorPins` 迭代器可见条目数偏差 | 用例 bug：旧迭代器固定旧 SuperVersion，新建 iter 后计数正确 | 修复用例：非共享迭代器（[zf_test.cc](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/tools/zf_test.cc)） |
| **R3** | `SyncSemantics` 挂起（超时 600s 未返回） | ① 持 DB mutex 做 `fdatasync` 阻塞后台线程；② `need_wal_sync=true` 时 `PreprocessWrite` 对原生 WAL 调 `PrepareForSync()` 但 ZF 路径跳过 `WriteGroupToWAL`，标记永不清除 → 下一个 sync 写死锁在 `wal_sync_cv_.Wait()` | ① sync 移出 mutex 临界区（`SyncTouchedPartitions` 锁外调用）；② ZF 模式下 `wal_context.need_wal_sync = false`（[db_impl_write.cc](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/db/db_impl/db_impl_write.cc)） |
| **R4** | 调试代码污染 | 写路径/flush 路径大量 `fprintf(stderr, "DEBUG …")`，单次 `zf_test` 输出 **207 万行**，改变时序掩盖竞态 | 全部替换为 `ROCKS_LOG_DEBUG`（`use_logger` 条件门控）；过程跟踪删除；memtable.cc backtrace 还原为原生 assert（涉及 5 个文件 22 处） |
| **R5** | 未实现项 | `ZfMaterializeJob` 不存在（改由 M3.2 实现）；`zf.*` 指标未做；`ApproximateMemoryUsage` 未确认 | ① 10 项 `zf.*` 指标经 `GetProperty("rocksdb.zeroflush.*")` 暴露；② `SlimMemTableRep::Allocate` 原子累计条目载荷返回真实用量；③ 额外修复：ZFPROPS 写入目录时序 + `--zf_filter` 参数解析 bug |

> M3.0 验收标准已全部满足：13 例全绿 + 调试输出 45 行（< 200） + `GetProperty("rocksdb.zeroflush.epochs_sealed")` 可用。
> 可开工 **M3.1**（PartitionTable 范围路由）。

---

## 3. 总体架构：从"消除 Flush"到"消除 L0"

### 3.1 M2 的剩余浪费

M2 物化后的数据路径是：

```
分区 WAL ──物化──► L0 SSTable ──L0→L1 compaction──► L1 SSTable
             写 1 遍            读 1 遍 + 写 1 遍
```

L0→L1 这一趟是**纯粹的重排序开销**：它存在的唯一理由是 L0 文件之间键范围重叠，
必须归并去重才能进入"层内无重叠"的 L1。

而 ZeroFlush 有一个原生 RocksDB 没有的结构优势：**它自己决定数据落在哪个分区**。
若分区按键范围划分，那么"P 个分区的物化输出"天然就是"P 个键范围无交集的文件"
—— 这正是 L1 的层内不变式。L0 的存在理由被消解。

### 3.2 M3 的两级实现

```
   Epoch E 封存完成
        │
        ▼
   ZfMaterializeJob（P 路并行，每路 = 1 分区）
        │  每路输出 1 个 SST，键范围 ⊆ PartitionTable::RangeOf(p)
        ▼
   ZfPickInstallLevel(smallest, largest)
        │
   ┌────┴─────────────────────────────────────────────┐
   │ 目标层及所有更浅层与该范围无文件重叠？             │
   └────┬──────────────────────────┬──────────────────┘
      是│                        否│
        ▼                          ▼
  【M3.2】直接 AddFile 到       ┌──────────────────────────────┐
   base_level（跳过 L0）        │【M3.3】Materialize-into-L1： │
   —— 空库/批量装载稳定命中      │ 输入 = 封存 WAL + 该范围现有  │
                                │ L1 文件；输出替换之           │
                                │ —— 稳态命中，彻底消除 L0      │
                                └──────────────────────────────┘
                                          │ 二者都不可行（并发冲突/错误）
                                          ▼
                                    回落 L0（与 M2 行为一致）
```

**为什么 M3.2 单独存在还有意义？** 因为它的正确性只依赖"无重叠"这一条原生
LSM 不变式（与 `ExternalSstFileIngestionJob` 同款判定），实现风险极低，且在
批量装载（`fillseq`/`fillrandom` 冷启动/`bulk load`）这一类真实场景下能100%命中。
而 M3.3 需要与 compaction 调度器争用文件所有权，是 M3 最重的一环。
把二者分阶段可以先把收益落地。

**为什么 M3.2 稳态一定会退化？** L1 被 P 个分区文件占满后，下一个 epoch 的分区 p
输出与 L1 中同范围的文件必然重叠 → 判定失败 → 回落 L0。这不是缺陷而是逻辑必然，
也正是 M3.3 存在的理由。M3.2 的实现必须把这一退化路径做成"可观测的正常行为"
（指标 `zf.install_fallback_l0`），而不是错误。

### 3.3 为什么 M3 不需要每分区 MemTable

M2 §3 曾把"每分区 MemTable"与范围路由绑在一起留给 M3。**本设计解绑二者。**

范围路由改变的只有 `Route()` 的实现和"输出文件范围无交集"这条性质，
它作用在**物化侧**。而每分区 MemTable 改变的是 `Get`（变路由查询）和
`Iterator`（变 P 路归并），作用在**读侧**，收益是并行插入与更小的 skiplist 高度。
两者无依赖。M3 保持单 MemTable + 全分区同步封存（M2 的 Epoch 等式不变），
把每分区 MemTable 推到 M4。这让 M3 的改动面收敛在"路由 + 物化 + 安装"三处。

---

## 4. 数据结构

### 4.1 `ZeroFlushOptions` 新增字段

```cpp
struct ZeroFlushOptions {
  // ---- M1/M2 既有 ----
  uint32_t partitions = 64;
  uint64_t partition_target_bytes = 64 << 20;
  std::string wal_subdir = "zfwal";
  bool use_logger = false;
  uint64_t epoch_target_bytes = 256u << 20;
  uint64_t max_pending_epochs = 2;
  uint32_t max_open_sealed_files = 256;
  bool reclaim_sealed_files = true;
  bool use_zfprops = true;

  // ---- M3.1 路由 ----
  enum class RoutingMode : uint8_t {
    kHash = 0,        // M2 行为：Hash(user_key) % P（兼容既有 DB）
    kStatic = 1,      // 用户提供 P-1 个分隔键
    kSampled = 2,     // 首个 epoch 用 hash，封存时采样学习边界后固定
  };
  RoutingMode routing_mode = RoutingMode::kHash;
  std::vector<std::string> static_boundaries;   // kStatic：升序，size == P-1
  uint32_t sample_every_n_records = 64;         // kSampled：采样步长
  // 连续 k 个 epoch 超 partition_target_bytes 才允许分裂（0 = 禁用分裂）
  uint32_t split_after_skewed_epochs = 0;

  // ---- M3.2/M3.3 物化与安装 ----
  uint32_t materialize_parallelism = 8;     // K：并行归并 worker 数
  bool install_below_l0 = true;             // M3.2 直装开关
  bool merge_into_base_level = false;       // M3.3 融合归并开关（默认关，逐步放开）
  // M3.3 触发比：封存字节 / 待重写 base-level 字节 ≥ 该值才融合，否则落 L0
  double base_merge_min_ratio = 0.25;
  uint32_t l0_fallback_tolerance = 0;       // 允许的 L0 回落文件数（超出告警）

  // ---- M3.5 卸载后端 ----
  std::string materialize_backend = "host";  // "host" | "csd"
};
```

**`routing_mode` 默认仍是 `kHash`**：M3 的路由改造对既有 DB 必须是可选升级，
而不是打开就换语义。`kHash` 下 §6/§7 的直装/融合判定仍会执行，只是几乎必然
因范围重叠而回落 L0 —— 行为等于 M2，可作为 A/B 对照组。

### 4.2 `PartitionTable`

```cpp
// zeroflush/partition_table.h（新增）
// 一个不可变的边界快照。Route 是 user_key 的纯函数。
class PartitionTable {
 public:
  // boundaries 为升序的 P-1 个分隔键（按 ucmp）；partitions = boundaries.size()+1
  static rocksdb::Status Create(
      uint32_t version, std::vector<std::string> boundaries,
      const rocksdb::Comparator* ucmp, std::shared_ptr<PartitionTable>* out);
  // hash 兼容模式的工厂（无边界）
  static std::shared_ptr<PartitionTable> CreateHash(uint32_t version,
                                                    uint32_t partitions);

  uint32_t Route(const rocksdb::Slice& user_key) const;
  // 分区 p 的键区间 [lo, hi)；p==0 时 lo 为 -∞，p==P-1 时 hi 为 +∞
  void RangeOf(uint32_t p, rocksdb::Slice* lo, rocksdb::Slice* hi) const;
  bool IsHashMode() const;
  uint32_t version() const;
  uint32_t partitions() const;
  // 该表的分区在全局 part_id 空间中的编号（支持分裂后稀疏，见 §5.5）
  const std::vector<uint32_t>& part_ids() const;
 private:
  uint32_t version_;
  std::vector<std::string> boundaries_;        // 升序
  std::vector<uint32_t> part_ids_;             // 与 [0, P) 一一对应的全局 id
  const rocksdb::Comparator* ucmp_;            // 必须是 DB 的 user comparator
};
```

`Route` = `upper_bound(boundaries_, user_key, ucmp)` 的下标。

> **必须用 `Options::comparator` 而非 `memcmp`。** 分区无交集这条性质要被 LSM 的
> 层内不变式承认，判定用的比较器就必须与 SSTable 排序用的比较器是同一个。
> M2 的 hash 路由不涉及顺序，因此从未依赖 comparator；M3.1 起
> `ZeroFlushContext` 必须持有 `const Comparator*`（`Open()` 时从
> `cfd->user_comparator()` 取，并把 `ucmp->Name()` 写入 ZFPROPS 做重开校验）。

### 4.3 `PartitionTableSet`：版本化容器

```cpp
// 全部历史边界版本。写路径只用 current()；物化用该 epoch 记录的版本。
class PartitionTableSet {
 public:
  std::shared_ptr<PartitionTable> current() const;             // 原子读
  std::shared_ptr<PartitionTable> Get(uint32_t version) const;
  // 仅允许在 Seal 的原子窗口内调用（持 DB mutex + write thread 独占）
  void InstallNewVersion(std::shared_ptr<PartitionTable> t);
 private:
  mutable rocksdb::port::Mutex mu_;
  std::vector<std::shared_ptr<PartitionTable>> versions_;  // 下标 = version
  std::atomic<uint32_t> current_version_{0};
};
```

`SealedEpoch` 增加 `uint32_t table_version`：记录该 epoch 写入时使用的边界版本，
物化时据此取回同一张表做范围断言。

### 4.4 `ZFPROPS` v2（变长格式）

v1 是定长 16B（magic/version/partitions/crc，见
[wal_format.h:78-93](wal_format.h)）。v2 改为变长：

```
magic 'ZFP2'(4B) | format_version=2 (4B) | routing_mode (1B) | pad(3B)
comparator_name_len(4B) | comparator_name(...)
table_count(4B)
  ┌ per table: version(4B) | partitions(4B)
  │            part_ids: P × 4B
  │            boundary_count(4B) = P-1
  │              ┌ per boundary: len(4B) | bytes(...)
  └ …
current_version(4B)
crc32c(4B)   // 覆盖前面全部字节
```

**读写规则**：
- 读到 `'ZFP1'` → 视为 v1：`routing_mode=kHash`、单个 version 0 的 hash 表、
  comparator 名未知（跳过校验）。首次以 v2 写回时原地升级。
- `partitions` 不匹配仍返回 `InvalidArgument`（M2 §4.5 语义保留）。
- `routing_mode` 不匹配返回 `InvalidArgument`：hash 库不能直接当范围库打开
  （已有 locator 的 part_id 是 hash 分配的，与新边界无关，会破坏 §5.4 的 I6）。
  迁移路径见 §5.6。
- `comparator_name` 不匹配返回 `InvalidArgument`（换 comparator 会让已持久化的
  边界顺序失效）。
- 写入必须原子：写 `ZFPROPS.tmp` → `Sync` → `RenameFile` 到 `ZFPROPS` → `SyncDir`。
  M2 当前是直接 `NewWritableFile` 覆盖写（[zeroflush_db.cc:468-475](zeroflush_db.cc)），
  在写入中途崩溃会留下截断的 ZFPROPS；v2 的内容变长，这个窗口必须关掉。

### 4.5 `MaterializeRequest` / `MaterializeBackend`

见 §10（CSD 卸载后端）。

---

## 5. M3.1：PartitionTable 范围路由

### 5.1 三种边界来源

| 模式 | 边界怎么来 | 适用 |
|---|---|---|
| `kHash` | 无边界，`Hash(key) % P` | 兼容既有 DB；A/B 对照组 |
| `kStatic` | `static_boundaries`（用户给 P-1 个键） | key 空间已知（`db_bench` 的 `%016d`、时间序列、租户前缀） |
| `kSampled` | 第一个 epoch 用 hash，封存瞬间从采样键算分位点 | 通用负载，无需先验知识 |

`kStatic` 的校验：升序（按 ucmp）、无重复、`size() == partitions - 1`；
不满足返回 `InvalidArgument`，不做静默修正。

### 5.2 `kSampled` 的采样与切换

```
Epoch 1（学习期）：routing = hash 表（version 0）
  写路径：AddRecord 每 sample_every_n_records 条把 user_key 存入 sampler_
          （上界 max_samples = 64K 条，超出用蓄水池替换，内存上界 O(64K × keylen)）
Seal(E=1) 的原子窗口内：
  1. sampler_ 排序（ucmp）→ 取 P-1 个分位点 → PartitionTable version 1
  2. tables_.InstallNewVersion(v1)
  3. 该 epoch 的 SealedEpoch.table_version = 0（它是用 hash 写的）
  4. 清空 sampler_
Epoch 2 起：写路径用 version 1，SealedEpoch.table_version = 1
```

学习期的 epoch 1 用 hash 路由，其输出文件范围重叠 → 走 L0 回落。
即"第一个 epoch 有一次 L0→L1"，之后稳态无 L0。这个代价是可接受且可解释的。

### 5.3 不变式修订

M2 的 I1"同一 user_key 的所有版本恒在同一分区"在 M3 下**过强**。改为：

| ID | 不变式 | 保障方式 |
|---|---|---|
| **I1'** | 在**同一 epoch 内**，同一 user_key 的所有版本恒在同一分区 | 边界版本只在 Seal 的原子窗口内切换；epoch 期间 `tables_.current()` 不变 |
| **I5** | 读路径不依赖 `Route()` | `SlimLocator` 自带 `part_id`；`ReadValue` 只用 locator，从不重算路由 |
| **I6** | 一个 epoch 的 P 个物化输出，键范围按 ucmp 两两无交集 | I1' + 输出范围 ⊆ `RangeOf(p)`（物化时断言）；hash 模式下不成立，故不启用直装 |

I1' 比 I1 弱，但 I6 只需要 I1'：物化是**按 epoch 独立进行**的，跨 epoch 的同 key
分布在不同分区完全无害 —— 它们进的是不同批次的 SST，由 seq 决定新旧，与分区无关。

I5 是 M2 §4.5 那段论证需要修正的地方：M2 认为"P 变化会破坏正确性"，真实原因不是
路由本身，而是 `ReadValue` 的 `ref.part_id >= zfo_.partitions` 上界校验和
`Recover` 里对 `[0, P)` 的枚举（[zeroflush_db.cc:303](zeroflush_db.cc)、
[zeroflush_db.cc:331](zeroflush_db.cc)）。M3 把这两处改成"按 `ListFiles()` 实际
发现的 part_id 集合"驱动后，P 增长（分裂）就成为安全操作。

### 5.4 倾斜的度量与应对

范围路由必然面对倾斜（hash 路由天然均匀，这是它唯一的优势）。

**度量**：每个 epoch 封存时记录
`skew = max_p(sealed_bytes[p]) / avg_p(sealed_bytes[p])`，暴露为
`zf.partition_skew`。

**应对（按代价递增）**：
1. **副触发条件**（M2 已有）：`max ActiveSize ≥ partition_target_bytes` 就整体封存。
   倾斜时 epoch 变短、物化变频，但正确性与空间上界都不受影响。这是 M3.1 的默认行为。
2. **分裂**（M3.1b，`split_after_skewed_epochs > 0` 才启用）：见 §5.5。

### 5.5 分裂（M3.1b，可选）

若分区 p 连续 `split_after_skewed_epochs` 个 epoch 都是"超 `partition_target_bytes`
且封存字节 > 2× 平均"，在下一次 Seal 的原子窗口内：

```
1. 取 p 的采样键中位数 m（分裂期需对 p 单独采样）
2. 新表 version+1：boundaries 插入 m；part_ids 中 p 的位置变成 {p, new_id}
   new_id = ++max_part_id_（全局单调，不复用）
3. wal_->EnsurePartition(new_id)（延迟建文件，gen 从 0 开始）
4. InstallNewVersion；P += 1
```

分裂后：
- 旧分区 p 的**已封存**文件不受影响（I5：读走 locator）；
- p 与 new_id 的范围合起来等于原 p 的范围 → I6 仍成立；
- `partitions` 字段在 ZFPROPS 中随 current table 变化，重开校验改为"请求的
  `partitions` 必须等于 current table 的 partitions，或调用方不指定（0 = 沿用持久化值）"。

**分裂不做合并（merge）**：分区数只增不减。理由：合并要求两个相邻分区的存量
locator 可互换，而 locator 的 part_id 是物理文件号，不可重映射。若 P 增长失控，
运维手段是"导出 + 重建"，而不是在线合并。这一限制必须写进文档而不是留给发现。

### 5.6 从 hash 库迁移到范围库

不支持原地切换（§4.4）。提供离线工具路径：

```
1. 以 kHash 打开旧库，Flush() 直到 imm 为空、zfwal 只剩空的活跃代
   （此时全部数据已在 SSTable 中，无任何存活 locator）
2. 关闭；删除 zfwal/ZFPROPS
3. 以 kStatic/kSampled 重开 —— 空 zfwal + 无存量 locator，切换安全
```

`zeroflush::ConvertRouting(dbname, options, from_zfo, to_zfo)` 把上述三步封装并
在第 1 步后校验"zfwal 无存活封存代"，不满足则返回 `TryAgain`。

---

## 6. M3.2：`ZfMaterializeJob` + 层级下探直装

### 6.1 Job 结构

```
输入：SealedEpoch se（含 gens、table_version、max_seq）
      table = tables_.Get(se.table_version)
      cfd（M3.4 起为 CF 集合）

K = materialize_parallelism 个 worker，P 个分区按 part_id 取模分片
worker 处理分区 p：
  1. 对 se.gens 中所有属于 p 的代（正常 1 个；收养了孤儿代时可能多个）：
     WalScanner 顺序整读 → 解码 (user_key, seq, type, value)
  2. 按 InternalKeyComparator 排序（ucmp(user_key) 升序，seq 降序）
     —— 保留全部版本，不做 snapshot 裁剪（与原生 flush 一致）
  3. 断言：ucmp(smallest.user_key, lo) >= 0 且 ucmp(largest.user_key, hi) < 0
     失败 → Corruption（说明 I1'/I6 被破坏，宁可失败也不产出错文件）
  4. 追加本分区范围内的 range tombstone（§9.3）
  5. BuildTable() → FileMetaData（含 smallest/largest/seq 区间/文件大小）
主线程：K 路全部成功 → §6.2 逐文件定层 → 单次 VersionEdit → LogAndApply
        → imm 释放 → Reclaim（M2 §6.5 路径不变）
任一路失败 → 删除已生成的临时 SST，整个 epoch 物化失败；imm 保留，下轮重试
```

**内存上界** = `K × partition_target_bytes`（默认 8 × 64MB = 512MB）。
`Open()` 中校验该乘积 ≤ `db_write_buffer_size` 或显式上限，超出返回
`InvalidArgument` 并在错误信息里给出建议值 —— M2 §6.3 只把这一点写进文档，
M3 要把它变成选项校验。

排序的替代方案：分区内**按 key 顺序插入的负载**（时间序列、`fillseq`）扫描出来
已近乎有序，用 `std::sort` 的最坏情况仍是 O(n log n)。若实测排序成为瓶颈，
可改为"扫描时直接 emplace 到 `std::map`"或对已有序输入快速路径检测。
M3.2 先按 `std::sort` 实现，把排序耗时单独计入 `zf.materialize_sort_micros`
以便判断是否值得优化 —— 它也正是 §10 CSD 卸载的第一目标。

### 6.2 `ZfPickInstallLevel`

```cpp
// 返回可安装的最浅"安全层"，失败返回 0（L0）
int ZfPickInstallLevel(const ColumnFamilyData* cfd,
                       const VersionStorageInfo* vstorage,
                       const InternalKey& smallest,
                       const InternalKey& largest);
```

判定规则（从 `vstorage->base_level()` 往深处试，取最深的可行层）：

> 令候选层 `L_k`。可安装 ⟺ **L0 以及 L1..L_k 中都没有文件与
> `[smallest.user_key, largest.user_key]` 重叠**。

**为什么不需要检查更深层？** 更深层与该范围重叠的文件里，只可能是同 user_key 的
**更旧**版本（它们更早被写入、更早被物化）。LSM 查找自浅至深，先命中我们的新文件
→ 语义正确。这与 `ExternalSstFileIngestionJob::IngestedFileFitInLevel` 的判定
（[db/external_sst_file_ingestion_job.cc:1375](../db/external_sst_file_ingestion_job.cc)）
是同一条规则。

**为什么必须检查 L0 与更浅层？** 若 L0 中存在与该范围重叠的文件，它可能来自
**更晚**的 epoch（前一个 epoch 物化被延迟而回落 L0，后一个 epoch 抢先直装 L1）。
此时把较旧数据放到较浅位置会读到旧值。检查更浅层同时也把这一 ABA 风险排除。

与 ingestion 的关键差异：**我们不改 seq**。ingestion 给文件分配
`global_seqno = last_sequence` 以获得"最新"身份；ZeroFlush 的记录携带真实 seq
（这正是 WAL 显式落 seq 的设计目的），因此不需要也不允许改写。这也意味着
`FileMetaData` 的 `fd.smallest_seqno/largest_seqno` 必须取自真实记录，
`VersionEdit::AddFile` 才能维持 seq 区间不变式。

**epoch 必须按序物化**：`ZfMaterializeJob` 对同一 CF 一次只跑一个 epoch，且按
epoch 号升序。RocksDB 的 `imm()` 列表本就是 FIFO 且 `PickMemTable` 按序取，
只要不启用 `atomic_flush` 的乱序路径即自然满足；M3.2 需在 job 入口
`assert(epoch == last_materialized_epoch_ + 1)` 固化这条前提。

### 6.3 安装

```cpp
VersionEdit edit;
for (auto& [level, meta] : outputs) {
  edit.AddFile(level, meta.fd.GetNumber(), meta.fd.GetPathId(),
               meta.fd.GetFileSize(), meta.smallest, meta.largest,
               meta.fd.smallest_seqno, meta.fd.largest_seqno, /*marked=*/false,
               …);
}
versions_->LogAndApply(cfd, mutable_cf_options, read_options, write_options,
                       &edit, &mutex_, directories_.GetDbDir());
```

**单次 `LogAndApply` 覆盖全部 P 个输出**，保证"一个 epoch 的物化是原子的"——
崩溃后要么全部可见、要么全部不可见，配合 M2 §7.2 的"文件还在就重放"规则即得到
崩溃一致性（G4 在 M3 下继续成立，无需新增持久化结构）。

不同分区可能被定到**不同层**（有的分区范围 L1 空、有的不空），这是允许的：
`AddFile` 的 level 是逐文件参数。

安装完成后：
- `cfd->imm()->Remove(...)` 走原生 flush 完成路径 → MemTable 析构 → `ReleaseEpoch`
- `MaybeScheduleFlushOrCompaction()`：直装到 base level 会改变各层字节数，
  必须重算 compaction 分数（否则 L1 超容不会触发 L1→L2）

### 6.4 与原生 FlushJob 的关系

M3.2 起 ZF 模式的物化**完全不走 `FlushJob`**：

| 方面 | M2.1（原生 FlushJob） | M3.2（ZfMaterializeJob） |
|---|---|---|
| 输入 | imm 的 MemTableIterator，逐条 `ReadValue` 随机 pread | 分区文件顺序整读 |
| 并行 | 单线程 | K 路 |
| 输出层 | 恒 L0 | base level 优先 |
| 代码路径 | `FlushJob::WriteLevel0Table` | 新增 `materialize_job.cc` |

但**调度、错误处理、SuperVersion 安装、`imm` 生命周期全部复用**
`BackgroundCallFlush` 那一套：`ZfMaterializeJob` 被 `FlushJob` 在 ZF 模式下持有并
在 `Run()` 内分派，对上层仍是"一次 flush"。这样 `Flush()`、
`WaitForFlushMemTable`、`FlushOptions::wait`、`bg_flush_scheduled_` 计数、
write stall 的解除时机全部无需改动 —— 这是 M2 用血换来的经验（那两个死锁都源于
绕开原生 mutex 约定），M3 不再重复。

---

## 7. M3.3：Materialize-into-BaseLevel（融合归并）

### 7.1 动机

M3.2 稳态必然回落 L0（§3.2）。要真正消除 L0，就必须让物化本身承担"与
base level 现有文件归并"的工作 —— 即把原生的 `flush` 与 `L0→L1 compaction`
**合成一个操作**。

```
原生 / M2：   WAL ─写─► L0 ─读─┐
                             ├─► L1（读 L1 + 写 L1）
                       L1 ─读─┘
              总计：写 L0 + 读 L0 + 读 L1 + 写 L1

M3.3：        WAL ─读─┐
                     ├─► L1（写 L1）
              L1 ─读─┘
              总计：读 WAL + 读 L1 + 写 L1     ← 省掉一次 L0 写 + 一次 L0 读
```

注意 "读 WAL" 不是新增开销：M3.2 也要读它。真正省下的是 **L0 文件的一次完整
写入与一次完整读取**。

### 7.2 触发条件

融合归并会重写 base level 中该分区范围的全部文件。若封存量远小于被重写量，
写放大反而更差。因此：

```
对分区 p：
  overlap_bytes = Σ{ base level 中与 RangeOf(p) 重叠的文件大小 }
  if overlap_bytes == 0                                  → M3.2 直装（最优）
  elif sealed_bytes[p] / overlap_bytes >= base_merge_min_ratio → M3.3 融合
  else                                                   → 回落 L0
```

`base_merge_min_ratio` 默认 0.25：即封存量至少是待重写量的 1/4 才融合。
更小的批次交给 L0 + 原生 compaction 去攒批，这恰好是 L0 的正当用途 ——
**M3 的目标不是"L0 文件数恒为 0"，而是"L0 只承担攒批，不承担重排序"**。
H3 的验收阈值 `l0_fallback_tolerance` 因此应按负载设定，而不是教条地取 0。

### 7.3 并发所有权：必须占用 compaction 槽位

这是 M3.3 最重的一环。融合归并要删除 base level 的现有文件，而
`CompactionPicker` 可能同时选中同一批文件做 L1→L2。二者都做 `LogAndApply`
删除同一文件 → 后者失败或产生悬空引用。

**方案**：把融合归并建模为一个真实的 `Compaction` 对象并注册进
`files_being_compacted`：

```
1. 在 DB mutex 下，为分区 p 构造 Compaction：
     inputs[0] = {} （level = -1，表示"来自封存 WAL"，无 SST 输入）
     inputs[1] = base level 中与 RangeOf(p) 重叠的文件
     output_level = base level
   若 inputs[1] 中任一文件已 being_compacted → 本分区降级为回落 L0
     （不等待：等待会把物化整体卡住，进而卡住 write stall 的解除）
2. compaction_picker->RegisterCompaction(c)  → 标记 being_compacted
3. 释放 DB mutex，worker 执行归并 IO
4. 重获 DB mutex，单次 VersionEdit：
     DeleteFile(base_level, 被替换的每个文件)
     AddFile(base_level, 新输出文件)
   与其它分区的输出合并进**同一个** VersionEdit（保持 epoch 原子性，§6.3）
5. compaction_picker->UnregisterCompaction(c)
```

**降级而不等待**是关键决策：物化是 write stall 解除的必经路径，任何在物化里
等待 compaction 的设计都会把"写入停顿"与"compaction 进度"耦合成一个新的死锁面。
M2 已经在这个坑上付出过代价（两次死锁都源于锁序与等待），M3 显式禁止此类等待。

`inputs[0].level = -1` 需要检查 `Compaction` 与统计路径对 level 的假设；
若 `-1` 在既有代码里被广泛假设为非法，退路是用 `level = 0` 且 `inputs[0]` 为空，
在 `Compaction` 上加 `bool is_zf_materialize_` 标记以区分。实现时以能编译通过
且不污染 compaction 统计为准则选择其一。

### 7.4 归并算法

```
worker(p)：
  输入迭代器 A = 封存代记录（顺序整读 + 排序，同 §6.1 步骤 1-3）
  输入迭代器 B = base level 重叠文件的 TableIterator（各文件范围无交集 →
                 简单串接即可得有序流，无需堆）
  归并 = MergingIterator(A, B) + CompactionIterator（复用原生：处理
         snapshot 裁剪、tombstone 下沉、merge operand 合并、SingleDelete 语义）
  输出 = TableBuilder，按 target_file_size_base 切分成多个文件
         （切分点必须在 RangeOf(p) 内 → 输出文件仍与其它分区无交集）
```

复用 `CompactionIterator` 是本节的核心简化：**tombstone/merge/snapshot 这三类
最容易写错的语义完全不自己实现**。代价是需要构造 `CompactionIterator` 所需的
上下文（snapshot 列表、`earliest_write_conflict_snapshot`、
`CompactionRangeDelAggregator`），这部分照抄 `CompactionJob::ProcessKeyValueCompaction`
的准备段。

**A 侧的 seq 正确性**：封存记录的 seq 严格大于 base level 中同 key 的 seq
（前者是刚写入的，后者是历史物化的）。`CompactionIterator` 依赖这条性质做
去重与 tombstone 下沉，它自然成立 —— 但必须在归并入口 `assert` 校验
`A.smallest_seqno > B.largest_seqno`，否则宁可失败也不产出错误结果。

> 例外：**收养了孤儿代的 epoch**（§2 R1、§8.1）里，孤儿代记录可能已经被物化进
> base level（M2 §7.2 的幂等重放）。此时 A 与 B 会出现 **完全相同的 (user_key, seq,
> type, value)**。`CompactionIterator` 对相同 internal key 的处理是保留第一个（更浅侧，
> 即 A），语义正确。但 `assert(A.smallest_seqno > B.largest_seqno)` 会被触发 →
> 该断言必须放宽为 `A.smallest_seqno >= B.largest_seqno`，并在该 epoch 上跳过严格
> 校验（用 `SealedEpoch.has_adopted_orphans` 标记）。

### 7.5 与 `IngestExternalFile` 的边界

`IngestExternalFile` 也能把文件放进任意层，但它**不能归并**：重叠时只能退到 L0
或失败。M3.3 与它的区别正是"能归并"。因此 M3.3 不复用 ingestion 路径，只借用它的
定层判定逻辑（§6.2）。

---

## 8. 恢复与崩溃一致性增量

M2 §7 的核心规则 **"文件还在 ⟹ 重放它"** 在 M3 下继续成立，无需 ZF 版 MANIFEST。
M3 只在其上补三处：

### 8.1 孤儿代的读窗口（补 M2 §7.3 的登记）

见 §2 R1 —— 收养语义保留，补上 `Recover()` 到首次 `Seal()` 之间的可读性。

```
Recover() 结束时：
  if (存在孤儿代) {
    sealed_cache_->AddRecoveryGens(pending_orphan_gens_, orphan_total_bytes);
    // 可读、不可回收、不占 refcount
  }

Seal 的原子窗口内（M2 §6.2 第 4 步之后）：
  se.gens += std::move(pending_orphan_gens_);
  se.has_adopted_orphans = !收养集合为空;
  sealed_cache_->AdoptRecoveryGensInto(se);   // 转入 epoch E，清空恢复集合
  sealed_cache_->AddEpoch(se);                // refcount = 1，由 old_mem 持有
```

两个不变式因此在恢复路径上都成立：
- **I3**（可删 ⟺ 已物化且无引用）：孤儿代在收养前不可回收（不在任何 epoch 里，
  `ReleaseEpoch` 触不到它）；收养后与 `old_mem` 同生命周期。
- **读可达**：`Get` 的 in_epoch 校验放宽为 `in_epoch || in_recovery_set`，
  窗口内外都可读。

`has_adopted_orphans` 标记的两个用途：① §7.4 的断言放宽；② 该 epoch 的物化输出
可能与 base level 已有内容重叠（部分记录已被上次崩溃前的物化写入）→ 直接跳过
§6.2 的直装判定，走 §7 融合或 L0 回落。

> **`AdoptRecoveryGensInto` 必须在 `AddEpoch` 之前、且与之在同一个持锁窗口内。**
> 否则存在一个瞬间：孤儿代既已从恢复集合移除、又未进入 epoch → 读路径 `NotFound`。
> 这正是 R1 的同类错误，只是窗口从"整个 epoch 时长"缩成"两行代码"——依然要关掉。

### 8.2 part_id 集合驱动的恢复

M2 的 `Recover()` 与 `ReadValue` 都假设 part_id ∈ [0, P)
（[zeroflush_db.cc:303](zeroflush_db.cc)、[zeroflush_db.cc:331](zeroflush_db.cc)）。
分裂（§5.5）后 part_id 空间稀疏，改为：

```
ListFiles() → 得到实际存在的 (part, gen) 全集
part_set = { p : (p, ·) ∈ files } ∪ current_table->part_ids()
for p in part_set: active_gen[p] = max gen；打开活跃代
ReadValue 的上界校验改为 "part_id 必须在 wal_ 已知分区集合内"
```

`PartitionedWalManager` 的 `parts_` 从 `vector<unique_ptr<Partition>>`（下标=part_id）
改为 `unordered_map<uint32_t, unique_ptr<Partition>>` + `EnsurePartition(id)`。
`ShouldSeal` 的 `[0, P)` 循环同步改为遍历 map。

### 8.3 边界表恢复

ZFPROPS v2 的边界表在 `ctx->Open()` 中解析，`current_version` 决定写路径用哪张表。
若 ZFPROPS 缺失但 zfwal 存在多代文件 → 返回 `Corruption`（不能猜边界：猜错会让
新写入落错分区，破坏 I1' 而无任何报错）。M2 当前对"ZFPROPS 缺失"是宽容的
（视为首次部署，[zeroflush_db.cc:441-464](zeroflush_db.cc)），M3 必须收紧为
"zfwal 为空才允许缺失"。

---

## 9. M3.4：API 完备性

三项之间无依赖，也与 §5–§7 无依赖，可并行推进。

### 9.1 多列族

**分区模型**：所有 CF **共享物理分区文件**，靠 `ZfRecordHeader.cf_id`（已预留 2B，
[wal_format.h:34](wal_format.h)）区分；每个 CF 有独立的 `PartitionTable`，
但分区号在**全局 part_id 空间**中分配：

```
CF "default"(id=0)  → part_id [0, 64)
CF "cf1"(id=1)      → part_id [64, 128)
分裂产生的新分区    → part_id 从 max_part_id_+1 起，与 CF 的区间不必连续
```

`ZeroFlushContext` 持有 `unordered_map<uint32_t /*cf_id*/, CfRouting>`，
`CfRouting = { PartitionTableSet tables; std::vector<uint32_t> part_ids; }`。

**为什么共享文件而不是每 CF 一套文件？** 每 CF 一套 → 文件数 = CF × P
（10 个 CF × 64 分区 = 640 个活跃文件 + 各代封存文件），fd 与 fsync 成本都不可接受。
共享文件的代价是物化时顺序扫描要按 `cf_id` 过滤 —— 一次顺序读的 CPU 过滤成本
远低于 640 个文件句柄。

**Epoch 语义**：一个 epoch 仍是"一次全局封存"，但对应 **N 个 imm**（每 CF 一个）：

```
SealEpochAndSwitch(所有 CF)：
  1. E = ++epoch_counter_
  2. ∀p ∈ 全部 part_id: Freeze(p)           // 物理封存与 CF 无关
  3. sealed_cache_->AddEpoch(se, /*refcount=*/参与 CF 数 N)
  4. ∀cf: cf->mem()->SetZfEpoch(E); ZfSwitchMemtable(cf)
回收：N 个 imm 各自析构 → N 次 ReleaseEpoch(E) → refcount 归零 → unlink
```

`SealedFileCache::AddEpoch` 需增加 refcount 参数（当前硬编码 1，
[sealed_file_cache.cc:40](sealed_file_cache.cc)）。这是多 CF 唯一需要动
SealedFileCache 的地方 —— refcount 机制本就为此准备好了。

**物化**：`ZfMaterializeJob` 的一路 worker = (cf_id, part_id) 对。不同 CF 的输出进
各自 CF 的 VersionSet，因此 **VersionEdit 不再能合并成一个** → 每 CF 一次
`LogAndApply`。原子性因此降级为"每 CF 原子"。这是可接受的：CF 之间没有跨 CF 的
一致性要求（除 `atomic_flush`，M3 明确不支持 `atomic_flush` + ZF 组合，
`Open()` 校验并返回 `InvalidArgument`）。

**恢复**：按记录的 `cf_id` 查 cfd 插入。因此 `zeroflush::Open` 需新增重载：

```cpp
Status Open(const DBOptions& db_options, const ZeroFlushOptions& zfo,
            const std::string& dbname,
            const std::vector<ColumnFamilyDescriptor>& cfds,
            std::vector<ColumnFamilyHandle*>* handles,
            std::unique_ptr<DB>* db);
```

内部对每个 descriptor 的 `ColumnFamilyOptions` 替换 `memtable_factory` 并做
同样的 `write_buffer_size` / `max_write_buffer_number` 调整。
遇到 WAL 中的 `cf_id` 在本次 Open 的 CF 集合里不存在 → 返回 `Corruption`
（CF 被 drop 但 WAL 未回收；正确处理需要在 drop 时同步封存，留 M4）。

**写路径**：`ZfBatchHandler` 删除三处 `column_family_id != 0` 的 `NotSupported`
（[zeroflush_db.cc:39/54/71](zeroflush_db.cc)），改为按 cf_id 查 cfd 与路由表；
`AddRecord` 与 `PartitionedWalManager::Append` 增加 `cf_id` 参数并写进帧头
（当前帧头 cf_id 恒 0）。

### 9.2 Merge

改动比看起来小，因为 offload 与 merge 是正交的：

| 位置 | 改动 |
|---|---|
| `ZfBatchHandler::MergeCF` | 删除 `NotSupported`（[zeroflush_db.cc:92-97](zeroflush_db.cc)），改为 `AddRecord(..., kTypeMerge, seq)` |
| WAL 帧 | `type = kTypeMerge`，operand 当作 value 内联 —— 格式无需改动 |
| `MemTable::SaveValue` | **必改**：当前只有 `case kTypeValue` 有 zf 分支（[db/memtable.cc:1400-1425](../db/memtable.cc)），`case kTypeMerge` 仍走 `GetLengthPrefixedSlice` → 会把 16B locator 当 operand 喂给 MergeOperator（静默错误）。需在 merge 分支同样调 `zf_ctx->ReadValue` |
| `MemTableIterator::value()` | 无需改（已按 zf 分支统一解引用，[db/memtable.cc:660-667](../db/memtable.cc)） |
| 物化 | §7.4 复用 `CompactionIterator` → merge operand 的部分合并/全量合并自然正确；§6.2 的纯直装路径**不做**任何 merge 合并（保留全部 operand），与原生 flush 语义一致 |
| `MemTable::Add` 统计 | `num_deletes_`/`num_entries_`/`UpdateFlushState` 的 merge 计数路径复核 |

> `kTypeMerge` 的 zf 分支缺失是一个**当前已存在的静默错误**：M2 用
> `NotSupported` 在写入侧挡住了它，一旦 M3.4 放开写入，读侧就会返回垃圾 operand。
> 二者必须同一个 commit 内落地。同类风险还有 `kTypeWideColumnEntity`
> （[db/memtable.cc:1435](../db/memtable.cc)）与 `kTypeBlobIndex`
> （[db/memtable.cc:1385](../db/memtable.cc)）—— M3 不支持它们，需在
> `ZfBatchHandler` 与 `Open()` 选项校验处显式拒绝，而不是留在读侧静默出错。

### 9.3 DeleteRange

**关键观察**：range tombstone 不走 `SlimMemTableRep`。`MemTable` 为它维护了
**独立的 rep** `range_del_table_`（[db/memtable.h:914](../db/memtable.h)），
由 `NewRangeTombstoneIterator` 消费。而 `memtable_factory` 只替换主 rep —— 也就是说
`range_del_table_` 在 ZF 模式下**依然是原生实现，value（end key）内联在 arena 里**。

因此设计极简：**range tombstone 不做 offload。**

| 方面 | 处理 |
|---|---|
| MemTable | 完全不改。`Add(kTypeRangeDeletion, begin, end)` 走原生路径，end key 内联 |
| WAL | 新增一个**专用 range-del 分区**：`part_id = kRangeDelPartId`（保留号，不参与路由），随 epoch 一起换代。记录 `type=kTypeRangeDeletion`、`key=begin`、`value=end` |
| 为什么要专用分区 | 一个 range 覆盖多个分区，无法路由到某一个；写进被覆盖的每个分区会重复放大。单独一个分区最简单，且 tombstone 数量级远小于 KV |
| `ShouldSeal` | 把 range-del 分区大小计入 `sum`（不计入 `max_one`，避免它触发副条件） |
| 恢复 | 重放该分区 → `mem->Add(kTypeRangeDeletion, begin, end)`。**顺序要求**：range-del 记录与 KV 记录的相对顺序由各自的 seq 决定，与重放顺序无关，故可在 KV 重放之后统一重放 |
| 物化（§6.1 步骤 4） | 每个分区 worker 都要把**与本分区范围有交集的** tombstone 加入输出，并按 `RangeOf(p)` 裁剪端点（`max(begin, lo)`、`min(end, hi)`）。裁剪是必须的：不裁剪会让输出文件的 range 超出分区范围，破坏 I6 |
| 物化（§7.4 融合） | `CompactionRangeDelAggregator` 原生处理，无需自己实现下沉 |
| 空间回收 | range-del 分区与 KV 分区同代同 epoch → 随 epoch 一起 unlink，无额外机制 |

裁剪带来的语义问题：一个 tombstone 被裁成 P 段分布在 P 个文件里。这与原生
flush 的行为一致（原生也会在 SST 边界处裁剪 tombstone），`RangeDelAggregator`
按 seq 聚合，分段不影响正确性。

### 9.4 事务（M3+，仅约束不实现）

`TransactionDB`/`OptimisticTransactionDB` 依赖 `two_write_queues`、
`seq_per_batch`、`WriteBatchWithIndex`、以及 2PC 的 `MarkBeginPrepare`/
`MarkEndPrepare`/`MarkCommit` 回调。当前 `ZfBatchHandler` 未覆盖这些回调，
基类默认实现会静默忽略或报错。

M3 的义务是**显式拒绝而不是静默失败**：`zeroflush::Open()` 校验

```cpp
if (db_options.two_write_queues || db_options.unordered_write ||
    db_options.enable_pipelined_write || db_options.allow_2pc /*等价开关*/) {
  return Status::NotSupported("ZeroFlush: <opt> not supported (M3)");
}
```

并在 `ZfBatchHandler` 中 override `MarkBeginPrepare` 等回调返回 `NotSupported`，
让走到该路径的写入**失败而不是丢数据**。

---

## 10. M3.5：卸载后端接口（CSD）

### 10.1 抽象

```cpp
// zeroflush/materialize_backend.h（新增）
struct MaterializeRequest {
  uint64_t epoch;
  uint32_t cf_id;
  uint32_t part_id;
  std::vector<std::string> input_wal_paths;   // 该分区本 epoch 的封存代文件
  std::vector<std::string> input_sst_paths;   // M3.3 融合时的 base level 文件
  std::string range_lo, range_hi;             // 分区键范围（空 = 无界）
  std::vector<RangeTombstoneRecord> tombstones;
  // 输出约束（必须完整传递，设备侧不得自行决定）
  std::string comparator_name;
  CompressionType compression;
  uint64_t target_file_size;
  uint32_t format_version;
  std::string output_dir;
};

struct MaterializeOutput {
  std::string path;
  uint64_t file_size;
  std::string smallest_key, largest_key;      // internal key 编码
  SequenceNumber smallest_seqno, largest_seqno;
  uint64_t num_entries, num_deletions;
};

struct MaterializeResult {
  std::vector<MaterializeOutput> outputs;
  uint64_t bytes_read = 0, bytes_written = 0, micros = 0;
};

class MaterializeBackend {
 public:
  virtual ~MaterializeBackend() = default;
  virtual const char* Name() const = 0;
  // 设备/后端是否可用（不可用时调用方回退 host 并打日志）
  virtual Status Probe() = 0;
  virtual Status Run(const MaterializeRequest&, MaterializeResult*) = 0;
};
```

### 10.2 两级卸载

要求设备侧产出**完全 RocksDB 兼容的 SST** 是很强的约束（需要在设备上实现
block based table builder、压缩、bloom、properties block）。因此定义两级：

| 级别 | 设备承担 | 主机承担 | 现实性 |
|---|---|---|---|
| **L1 卸载**（排序卸载） | 顺序读封存文件 + 解码 + 按 ucmp 排序，输出**已排序的 KV 流** | 用该流驱动 `TableBuilder` 建 SST | 高。设备只需 memcmp 类比较器；`Options::comparator` 非字节序时自动回退 host |
| **L2 卸载**（完整卸载） | 排序 + `TableBuilder` 等价实现，直接产 SST | 只校验并注册 `FileMetaData` | 低。需设备侧实现 SST 格式；建议先只支持 `compression=kNoCompression` + `BytewiseComparator` |

`HostBackend` 即 §6.1/§7.4 的实现，也是 L1/L2 卸载的正确性参照物：
测试用例对同一输入分别跑 host 与 csd 后端，逐字节比对输出 SST 的
`num_entries`/`smallest`/`largest` 与全量扫描结果（`ZfBackendEquivalence` 用例）。

### 10.3 与 gParaKV 的关系

同仓 `source/gParaKV-GC-pipeline-h2d/` 已有基于 CUDA + GDS 的并行 GC 流水线经验
（H2D 传输、cuFile 直读）。ZeroFlush 的物化在 IO 形态上与之同构：
**顺序整读一批文件 → 排序 → 顺序写出**。`CsdBackend` 的第一个实现应复用其
GDS 读取与设备侧排序骨架，而非从零搭建。这一条是 M3.5 排在最后的原因：
它的收益依赖硬件平台，而前面四个阶段的收益不依赖任何硬件。

`materialize_backend` 选项在 `Probe()` 失败时**自动回退 host**并
`ROCKS_LOG_WARN`，绝不因设备不可用而拒绝打开 DB。

---

## 11. 分阶段落地计划

每阶段结束时 `zf_test` 必须全绿且可独立提交。

| 阶段 | 内容 | 退出条件 |
|---|---|---|
| **M3.0** | R1（孤儿代读窗口）+ R2 定位 + R3（sync 移出 DB mutex）+ R4（清调试代码）+ `zf.*` 指标 + `ApproximateMemoryUsage` | 现有 13 例全绿；`zf_test` 输出行数 < 200；`GetProperty("rocksdb.zeroflush.epochs_sealed")` 可用 |
| **M3.1** | `PartitionTable` + `PartitionTableSet` + ZFPROPS v2 + `kStatic`/`kSampled` + part_id 集合化（§8.2） | H1 达标（用例 20/21/22）；`kHash` 模式行为与 M3.0 逐指标一致（回归对照） |
| **M3.2** | `ZfMaterializeJob`（K 路并行）+ `ZfPickInstallLevel` + 单次 VersionEdit 安装 | H2 达标；物化吞吐相对 M2.1 的原生 FlushJob 提升 ≥ 2×（K=8） |
| **M3.3** | Materialize-into-BaseLevel + compaction 槽位互斥 + `CompactionIterator` 复用 | H3 达标；`zf.install_fallback_l0` 在稳态写入下 ≤ 阈值 |
| **M3.4** | 多 CF + Merge + DeleteRange + 不支持项显式拒绝 | H4/H5 达标；`kTypeMerge` 的 zf 分支补齐 |
| **M3.5** | `MaterializeBackend` 抽象 + `HostBackend` 重构 + `CsdBackend` 骨架 | H6 达标；`ZfBackendEquivalence` 通过 |
| **M3.1b** | 分裂（可选，`split_after_skewed_epochs > 0`） | 用例 27 通过；`zf.partition_skew` 在倾斜负载下收敛 |

**建议提交粒度**：M3.0 单独一个 commit（纯债务清偿，无新特性，可独立回溯）。
M3.1 + M3.2 是 M3 的价值交付点（"批量装载零 L0"是可独立发布的成果）。
M3.3 风险最高，应在 M3.2 稳定运行一段时间后单独推进，且默认
`merge_into_base_level = false`，靠开关灰度。
M3.4 与前面无依赖，人力允许时并行。

---

## 12. 测试矩阵

在现有 13 例（M1 7 例 + M2 6 例）基础上新增：

| # | 用例 | 阶段 | 覆盖 |
|---|---|---|---|
| 14 | `RecoveryEpochRegistered` | M3.0 | R1；构造孤儿代后重开，**逐 key `Get` 全部成功**（不只数迭代器条数） |
| 15 | `SyncNotHoldingDbMutex` | M3.0 | R3；`sync=true` 连续写 1k 条不挂起，且后台 flush 可并发推进 |
| 16 | `ZfPropertiesExposed` | M3.0 | H7；全部 `zf.*` 指标可读且单调性正确 |
| 17 | `StaticBoundariesRoute` | M3.1 | §5.1；给定边界后 `Route` 与 `RangeOf` 互为逆；越界键落首/末分区 |
| 18 | `SampledBoundariesConverge` | M3.1 | §5.2；随机键负载学习出的边界使 `skew < 1.5` |
| 19 | `NonBytewiseComparator` | M3.1 | §4.2；reverse comparator 下 I6 仍成立（用 `ReverseBytewiseComparator`） |
| 20 | `PartitionOutputsDisjoint` | M3.1/3.2 | **H1**；物化后逐对校验 P 个输出 SST 范围无交集（读 `sst_dump` 级元数据或内部 `FileMetaData`） |
| 21 | `ComparatorNameMismatchRejected` | M3.1 | §4.4；换 comparator 重开返回 `InvalidArgument` |
| 22 | `RoutingModeMismatchRejected` | M3.1 | §4.4；hash 库以 `kStatic` 重开被拒 |
| 23 | `BulkLoadZeroL0` | M3.2 | **H2**；空库写 10 epoch，全程 `NumFilesAtLevel(0) == 0` |
| 24 | `MaterializeParallelSpeedup` | M3.2 | §6.1；K=1 vs K=8 的 `zf.materialize_micros` 比值 ≥ 2 |
| 25 | `InstallFallbackToL0` | M3.2 | §3.2；人为让 base level 满，验证回落 L0 且数据正确、指标计数 |
| 26 | `SteadyStateZeroL0` | M3.3 | **H3**；持续写 20 epoch，L0 文件数 ≤ 阈值，compaction 写字节 < M2 基线 70% |
| 27 | `MaterializeVsCompactionRace` | M3.3 | §7.3；人为触发 L1→L2 与融合归并竞争同一文件，验证降级而非死锁/损坏 |
| 28 | `RecoveryEpochMergeIdempotent` | M3.3 | §7.4 例外；孤儿代与 base level 有重复记录时融合结果不翻倍 |
| 29 | `SkewedSplit` | M3.1b | §5.5；单调递增键导致末分区倾斜 → 分裂后 `skew` 下降，全 key 可读 |
| 30 | `MultiColumnFamily` | M3.4 | **H4**；3 个 CF 交叉写读；epoch 回收在最后一个 imm 析构后（查 `zf.sealed_wal_bytes`） |
| 31 | `MergeOperandChain` | M3.4 | **H5**；`Merge` 链跨 2 个 epoch，`Get` 结果正确（覆盖 `SaveValue` 的 merge zf 分支） |
| 32 | `DeleteRangeAcrossPartitions` | M3.4 | **H5**；一个 range 覆盖全部分区，物化后仍生效；重开后仍生效 |
| 33 | `UnsupportedOptionsRejected` | M3.4 | §9.4；`two_write_queues` / `unordered_write` / `atomic_flush` 逐项被拒 |
| 34 | `ZfBackendEquivalence` | M3.5 | H6；host 与 csd 后端输出等价；`Probe()` 失败时自动回退且 DB 可用 |

**崩溃用例**（沿用 M2 §10 的子进程 `_exit(0)` 手法，避免依赖外部 kill）：
用例 14/28 分别覆盖"封存后未物化即崩溃"与"物化后未回收即崩溃"。

**基线对照**：用例 23/26 的"M2 基线"数字必须来自同一台机器同一次跑批，
以 `routing_mode=kHash` + `install_below_l0=false` + `merge_into_base_level=false`
作为 M2 等价配置，避免拿历史数字做对比。

---

## 13. 统计指标与可观测性

经 `GetProperty("rocksdb.zeroflush.<name>")` 暴露（M3.0 落地）：

| 指标 | 含义 | 阶段 |
|---|---|---|
| `epochs_sealed` / `epochs_materialized` / `epochs_reclaimed` | 三阶段计数（差值即在途 epoch 数） | M3.0 |
| `live_wal_bytes` / `sealed_wal_bytes` | 活跃代 / 已封存未回收字节（G1 告警源） | M3.0 |
| `materialize_micros` / `materialize_sort_micros` | 物化总耗时 / 其中排序耗时（判断是否值得卸载） | M3.0/3.2 |
| `sealed_read_count` / `sealed_cache_miss` | 封存代定点读次数 / LRU 未命中 | M3.0 |
| `partition_skew` | `max/avg` 封存字节比 | M3.1 |
| `routing_table_version` / `partitions_current` | 当前边界版本 / 当前 P | M3.1 |
| `install_direct_base` / `install_fallback_l0` | 直装成功 / 回落 L0 的文件数 | M3.2 |
| `base_merge_count` / `base_merge_rewritten_bytes` | 融合归并次数 / 被重写的 base level 字节 | M3.3 |
| `backend_name` / `backend_fallback_count` | 生效后端 / 回退次数 | M3.5 |

**替代 R4 的调试手段**：所有原 `fprintf(stderr, "DEBUG …")` 位点改为
`ROCKS_LOG_DEBUG(immutable_db_options_.info_log, …)`，并在
`ZeroFlushOptions::use_logger` 为 true 时才生效。时序敏感的诊断改用
`SyncPoint`（`TEST_SYNC_POINT`），而不是 `stderr` 打印 —— 后者本身会改变时序，
M2 的死锁排查已充分暴露这个问题。

---

## 14. 性能预期

以 M2 完成后的数字为基线（M2 的基线本身待 M3.0 全绿后重跑）。

| 指标 | 预期 | 原因 |
|---|---|---|
| `fillrandom` 吞吐 | M3.1/3.2 持平，M3.3 略升 | 写路径不变；M3.3 减少后台 IO 争用 |
| **compaction 写字节** | **显著下降**（H2 场景 → 0；H3 稳态 → −30% 以上） | 跳过 L0 或把 flush+L0→L1 合成一次 |
| 写放大 WA | 下降 | 同上 |
| 物化吞吐 | ≥ 2×（K=8） | P 路并行顺序读替代单线程随机 pread |
| `readrandom` | 略升 | L0 文件数下降 → 每次 Get 少查若干 L0 文件 |
| `readseq` | 明显上升 | 数据集中在层内无重叠的 base level，顺序读不再跨多个 L0 文件做归并 |
| p99 写延迟 | 下降 | L0 文件数受控 → `level0_slowdown_writes_trigger` 触发概率下降 |
| 倾斜负载吞吐 | **可能下降**（范围路由的代价） | 单分区打满触发频繁短 epoch；由 §5.4/§5.5 缓解 |

最后一行是本设计必须诚实标注的**回归风险**：hash 路由天然均匀，范围路由用
"输出可直装"换掉了"分布均匀"。因此 `routing_mode` 默认保持 `kHash`，
范围路由是显式选择而非默认升级。

---

## 15. 风险登记

| 风险 | 影响 | 缓解 |
|---|---|---|
| M2 债务（R1–R3）未清就开 M3 | 新老缺陷交织，无法二分定位 | §2 门槛判定：13 例全绿才开工 M3.1 |
| 范围路由倾斜 | 单分区打满 → epoch 变短、物化变频 | 副触发条件兜底；`zf.partition_skew` 告警；M3.1b 分裂 |
| `Route` 用错比较器 | I6 静默失效 → 直装产生错误可见性 | `ucmp` 从 cfd 取；ZFPROPS 存 comparator name 并校验；用例 19/20 |
| 直装定层判定漏检更浅层 | 读到旧值（静默数据错误） | 判定逻辑与 `IngestedFileFitInLevel` 对齐；用例 25 显式覆盖回落分支 |
| 融合归并与 compaction 争用文件 | 悬空引用 / `LogAndApply` 失败 | `RegisterCompaction` 互斥 + **降级不等待**（§7.3）；用例 27 |
| 融合归并等待 compaction | 新死锁面（write stall 与 compaction 互锁） | 设计上禁止等待；code review 检查项 |
| `K × partition_target_bytes` 内存上界 | OOM | `Open()` 选项校验（M2 只写了文档，M3 落成校验） |
| 多 CF 下 epoch refcount 算错 | 文件早删（数据丢失）或永不删（空间泄漏） | `AddEpoch(refcount=N)` + 用例 30 查 `sealed_wal_bytes` 归零 |
| `kTypeMerge`/`kTypeWideColumnEntity` 的 zf 分支缺失 | 把 16B locator 当 value 返回（静默错误） | M3.4 补 merge 分支；其余类型在写入侧显式 `NotSupported` |
| CSD 后端输出不兼容 | 静默损坏 SST | `ZfBackendEquivalence` 逐字段比对；`Probe()` 失败即回退 host |
| ZFPROPS v2 变长写入中途崩溃 | 边界表丢失 → 无法安全打开 | tmp + rename + SyncDir 原子写；缺失且 zfwal 非空时返回 `Corruption` 而不是猜 |

---

## 16. 受影响文件清单

| 文件 | 改动 | 阶段 |
|---|---|---|
| `zeroflush/partition_table.{h,cc}` | **新增**：`PartitionTable` / `PartitionTableSet` / 采样器 | M3.1 |
| `zeroflush/materialize_job.{h,cc}` | **新增**：`ZfMaterializeJob`（K 路并行 + 定层 + 安装） | M3.2/3.3 |
| `zeroflush/materialize_backend.{h,cc}` | **新增**：`MaterializeBackend` / `HostBackend` / `CsdBackend` 骨架 | M3.5 |
| `zeroflush/wal_format.{h,cc}` | ZFPROPS v2 变长编解码；帧头 `cf_id` 实际写入 | M3.1/3.4 |
| `zeroflush/wal_manager.{h,cc}` | `parts_` 改 map + `EnsurePartition`；`Append` 增 `cf_id`；range-del 专用分区；`Sync` 移出 DB mutex 配合 | M3.0/3.1/3.4 |
| `zeroflush/sealed_file_cache.{h,cc}` | `AddRecoveryGens`/`AdoptRecoveryGensInto` + `Get` 放宽校验（R1）；`AddEpoch` 增 refcount 参数；`SealedEpoch` 增 `table_version`/`has_adopted_orphans` | M3.0/3.4 |
| `zeroflush/zeroflush_db.{h,cc}` | 孤儿代登记（R1）；`Route` 委派 `PartitionTable`；多 CF 路由表；`Open` 多 CF 重载；选项校验；`ucmp` 持有；指标导出 | 全阶段 |
| `zeroflush/slim_memtable.cc` | `ApproximateMemoryUsage` 返回真实 arena 用量 | M3.0 |
| `db/memtable.cc` | `SaveValue` 的 `kTypeMerge` 补 zf 分支；构造处 ZF 模式 `assert(zf_ctx != nullptr)` | M3.0/3.4 |
| `db/version_set.{h,cc}` | 暴露 `ZfPickInstallLevel` 所需的重叠查询（或复用既有 `GetOverlappingInputs`） | M3.2 |
| `db/compaction/compaction_picker.{h,cc}` | 允许注册 `input_level = -1` 的 ZF 物化 compaction | M3.3 |
| `db/flush_job.{h,cc}` | ZF 模式下把 `Run()` 分派给 `ZfMaterializeJob` | M3.2 |
| `db/db_impl/db_impl_write.cc` | 移除调试代码；sync 移出 DB mutex 临界区 | M3.0 |
| `db/db_impl/db_impl.cc` / `db_impl_compaction_flush.cc` / `column_family.cc` | 移除调试代码；`GetProperty` 接 `zf.*` | M3.0 |
| `tools/zf_test.cc` | 移除调试打印；新增用例 14–34 | 全阶段 |
| `CMakeLists.txt` | 新增 3 个源文件 | M3.1/3.2/3.5 |

---

**参考**：[M2_DESIGN.md](M2_DESIGN.md) ·
[M1_WAL_PERSISTENCE_FIX.md](M1_WAL_PERSISTENCE_FIX.md) ·
[README.md](README.md) ·
[性能基线报告](../../../output/zeroflush_m1_perf/PROJECT_SUMMARY.md) ·
`source/gParaKV-GC-pipeline-h2d/`（CSD/GDS 流水线经验）
