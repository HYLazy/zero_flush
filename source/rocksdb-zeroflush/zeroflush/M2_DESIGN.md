# ZeroFlush M2 方案设计：封存 · 物化 · 回收

**版本**：v1.0
**状态**：✅ **已实现，M3.0 已完成债务清偿**（2026-08-10 `zf_test` 13 例全绿）
- M2.0（修 D1–D4 + ZFPROPS）✅ 已实现
- M2.1（Epoch 封存/物化/回收）✅ 已实现
- M2.2（`ZfMaterializeJob` P 路并行归并）❌ 未实现（并入 M3.2）
- M2.3 ✅ 已实现：sync 语义 + DestroyDB + ZFPROPS + `zf.*` 统计指标

**遗留缺陷**：M2 期间发现的 R1–R5 已在 **M3.0** 全部修复。
详见 [M3_DESIGN.md §2「准入门槛：M2 债务清偿」](M3_DESIGN.md)。

**前置**：M1 已完成并通过 7/7 回归（见 [M1_WAL_PERSISTENCE_FIX.md](M1_WAL_PERSISTENCE_FIX.md)）
**后续**：[M3_DESIGN.md](M3_DESIGN.md)（范围路由 · 消除 L0 · API 完备）
**核心决策**：封存后的旧代 WAL 由 **SealWorker 归并成 SSTable** 后回收（而非 vLog 式不物化）

---

## 1. 目标与非目标

### 1.1 目标

| # | 目标 | 验收标准 |
|---|---|---|
| G1 | **WAL 空间有界** | 持续写入 10× `epoch_target_bytes` 后，`zfwal/` 目录总大小稳定在 `O(max_pending_epochs × epoch_target_bytes)` |
| G2 | **value 物化进 SSTable** | 封存代经归并后，L0 中的 SSTable 内联 value；对应 WAL 文件被删除；数据仍可读 |
| G3 | **恢复量有界** | 重开耗时与恢复内存只与"存活未物化代"成正比，不随历史写入总量增长 |
| G4 | **崩溃一致性** | 任意时刻 kill -9，重开后所有已确认写入可读，无重复无丢失 |
| G5 | **语义对齐** | `Flush()` 真实生效；`WriteOptions::sync` 触发 fsync；`DestroyDB` 清理 `zfwal` |
| G6 | **并行物化** | P 路并行顺序读归并，物化吞吐随 `materialize_parallelism` 近线性提升 |

### 1.2 非目标（明确推迟）

| 项 | 推迟到 | 理由 |
|---|---|---|
| 多列族（cf_id ≠ 0） | M3 | `ZfRecordHeader.cf_id` 已预留 2B，写路径改造独立于 M2 主线 |
| `Merge` / `DeleteRange` | M3 | 需要 WAL 记录类型扩展 + 归并期语义处理 |
| 事务（`two_write_queues` / `seq_per_batch`） | M3+ | 与写路径分流强耦合 |
| **PartitionTable 范围路由** | M3 | 见 §11.2 —— 范围路由的真正收益是"输出直接落 L1"，是独立里程碑，不应与 M2 混做 |
| CSD 卸载归并 | M3 | M2.2 的 `ZfMaterializeJob` 即其主机侧 fallback 形态，接口预留 |

---

## 2. 前置：必须先修的 M1 遗留缺陷

`Freeze()` 在 M1 从未被调用，因此以下缺陷全部**休眠未暴露**。M2 一旦启用封存，它们立刻变成静默数据损坏。**修完这四项才能开始 M2.1。**

### D1 `ReadRecord` 完全忽略 `ref.gen`（严重：静默读到错误数据）

```cpp
// wal_manager.cc — 现状
Partition* p = parts_[ref.part_id].get();
// ...直接用 p->rfile / p->buf，从未比较 ref.gen 与 p->gen
```

封存后 `p->gen` 递增、`p->rfile` 指向新代文件，而 MemTable 中存量 locator 仍带旧 `gen`。
读旧 locator 会**用旧 offset 去读新代文件** → 返回错误 value 或 CRC 失败。

**修复**：`ReadRecord` 按 `ref.gen` 三路分派（§6.4）。

### D2 `Freeze` 未重置分区偏移（严重：新代 offset 全错）

```cpp
const uint32_t old_gen = p->gen;
++p->gen;
OpenGen(p);          // 新文件，物理大小 0
// 但 p->flushed_size / p->total_size 仍是旧代的累计值（如 64MB）
```

`Append` 用 `offset = p->total_size` 作为返回的 locator 偏移，而新文件从 0 写起
→ locator 偏移与文件实际偏移相差一整个旧代长度。

**修复**：`Freeze` 中 `p->flushed_size = p->total_size = 0;`。

### D3 `Freeze` 对未写过的分区裸解引用 `wfile`（崩溃）

```cpp
p->wfile->Sync();   // wfile 是延迟打开的；从未 Append 过的分区此处为 nullptr
```

P=64 且写入量小时，多数分区从未被路由到 → 必然段错误。

**修复**：`if (p->wfile) { FlushBuf(p); p->wfile->Sync(); }`。

### D4 `Open()` 只探测 gen 0（严重：重开后追加进已封存文件）

```cpp
std::string fname = FileName(i, p->gen);   // p->gen 初值恒为 0
```

存在多代文件时，重开会把 `p->gen` 留在 0，随后 `Append` 追加到**已封存的 gen 0 文件尾部**，
而该文件可能已被归并并即将删除 → 新写入丢失。

**修复**：`Open()` 先 `ListFiles()`，对每个分区取 `max(gen)` 作为活跃代，并按该代文件大小
初始化 `flushed_size`/`total_size`；`gen < max(gen)` 的存活文件登记为"孤儿封存代"（§7.3）。

> 另：`OpenGen` 仍用 `NewWritableFile`（`O_TRUNC`）。新建代时正确，但为与 M1 的
> `ReopenWritableFile` 修复保持单一语义，统一改为 `ReopenWritableFile`。

---

## 3. 总体架构：Epoch 模型

M2 引入 **Epoch（纪元）** 作为封存/物化/回收的原子单位。

> **一个 Epoch = 全部 P 个分区各封存一代 + 一次 MemTable 切换。**

这条等式是整个 M2 的基石：它把"P 个分散的 WAL 文件"与"一个不可变 MemTable"绑成
**1:1 生命周期**，从而让"何时可以删 WAL"退化为一个已被 RocksDB 解决的问题
——"何时可以析构 immutable memtable"。

```
                        写入
                         │
                         ▼
   ┌──────────────────────────────────────────────────────┐
   │  活跃期 (Epoch E)                                     │
   │   mutable MemTable ──── locator ────► zf-wal-p-gE.log │  p = 0..P-1
   └──────────────────────────────────────────────────────┘
                         │  ShouldSeal() == true
                         │  【DB mutex + write thread 独占】
                         ▼
   ┌──────────────────────────────────────────────────────┐
   │  Seal(E)  原子三步：                                   │
   │   1. ∀p: Freeze(p)   gE → gE+1（旧代转只读）           │
   │   2. SwitchMemtable  mutable → immutable               │
   │   3. imm 绑定 EpochRef(E) = {(p, gE)} 全集             │
   └──────────────────────────────────────────────────────┘
                         │  后台调度
                         ▼
   ┌──────────────────────────────────────────────────────┐
   │  Materialize(E)                                       │
   │   M2.1: 原生 FlushJob（读 imm，locator 解引用取 value） │
   │   M2.2: ZfMaterializeJob（P 路并行顺序读封存文件）      │
   │   → 生成 L0 SSTable（value 内联）→ 单次 VersionEdit    │
   └──────────────────────────────────────────────────────┘
                         │  imm 引用归零（含所有存活迭代器）
                         ▼
   ┌──────────────────────────────────────────────────────┐
   │  Reclaim(E)  unlink {(p, gE)} 全部文件                 │
   └──────────────────────────────────────────────────────┘
```

**为什么"全分区同时封存"而不是"单分区独立封存"？**

单分区独立封存需要**每分区一个 MemTable**（否则无法只释放该分区的 MemTable 条目），
这会把 Get 变成路由查询、把 Iterator 变成 P 路归并，是一次大得多的改造。
全分区同步封存复用原生 `SwitchMemtable` + `imm()` 列表 + FlushJob 安装路径，
**M2 主线几乎不新增并发状态机**。代价是分区大小倾斜时（某分区先满）需等齐，
由副触发条件（§5.2）兜底。每分区 MemTable 留给 M3（与范围路由一并做）。

---

## 4. 数据结构

### 4.1 `ZeroFlushOptions` 新增字段

```cpp
struct ZeroFlushOptions {
  uint32_t partitions = 64;
  uint64_t partition_target_bytes = 64 << 20;   // 副触发：单分区上限
  std::string wal_subdir = "zfwal";
  bool use_logger = false;

  // ---- M2 新增 ----
  uint64_t epoch_target_bytes = 256 << 20;  // 主触发：全分区活跃字节合计上限
  uint32_t max_pending_epochs = 2;          // 未物化 epoch 上限，超出则写流控
  uint32_t materialize_parallelism = 8;     // M2.2：并行归并线程数
  uint32_t max_open_sealed_files = 256;     // 封存代读句柄 LRU 容量
  bool reclaim_sealed_files = true;         // false = 只封存不删（调试/取证用）
};
```

`epoch_target_bytes` 默认 256MB，与 M1 `Open()` 里强制放大的 `write_buffer_size`
一致，便于两者行为对齐。

### 4.2 `SealedEpoch` / `EpochRef`

```cpp
// 一个 epoch 的封存文件全集（不可变）
struct SealedEpoch {
  uint64_t epoch;
  std::vector<std::pair<uint32_t, uint32_t>> gens;  // (part_id, gen)
  rocksdb::SequenceNumber max_seq;                  // 封存瞬间的 last_sequence
  uint64_t total_bytes;                             // 统计用
};

// RAII 引用；由 immutable MemTable 持有，析构即 Release
class EpochRef {
 public:
  EpochRef() = default;
  EpochRef(ZeroFlushContext* ctx, uint64_t epoch);
  ~EpochRef();                       // → ctx->ReleaseEpoch(epoch_)
  EpochRef(EpochRef&&) noexcept;
  EpochRef(const EpochRef&) = delete;
 private:
  ZeroFlushContext* ctx_ = nullptr;
  uint64_t epoch_ = 0;
};
```

### 4.3 `SealedFileCache`

封存代文件的只读句柄缓存 + 引用计数 + 延迟 unlink。

```cpp
class SealedFileCache {
 public:
  SealedFileCache(rocksdb::Env* env, std::string dir, uint32_t capacity);

  // 登记一个 epoch 的文件集，refcount = 1
  void AddEpoch(const SealedEpoch& e);
  // refcount-- ；归零则把文件名移入 pending_unlink_
  void ReleaseEpoch(uint64_t epoch);
  // 取 (part, gen) 的只读句柄（LRU；未命中则打开）
  rocksdb::Status Get(uint32_t part, uint32_t gen,
                      std::shared_ptr<rocksdb::RandomAccessFile>* out);
  // 由 DBImpl::PurgeObsoleteFiles 调用，真正 unlink
  void PurgePending();

  uint64_t sealed_bytes() const;   // 统计：未回收封存字节
 private:
  // key = ((uint64_t)part << 32) | gen
  mutable rocksdb::port::Mutex mu_;
  std::unordered_map<uint64_t, SealedEpoch> epochs_;
  std::unordered_map<uint64_t, uint32_t> refs_;      // epoch → refcount
  /* LRU<uint64_t, shared_ptr<RandomAccessFile>> handles_; */
  std::vector<std::string> pending_unlink_;
};
```

**为什么不在 refcount 归零时立刻 unlink？**
POSIX 下 unlink 对已打开 fd 无害，但**新打开会失败**，而并发读者可能正好在
`Get()` 的窗口内。把 unlink 推迟到 `PurgeObsoleteFiles`（原生已保证此处
SuperVersion 已清理）可彻底消除该竞态，且与原生 SST 删除时机统一。

### 4.4 `MemTable` 侵入点

```cpp
// memtable.h 新增
void SetZfEpochRef(zeroflush::EpochRef ref) { zf_epoch_ref_ = std::move(ref); }
private:
  zeroflush::EpochRef zf_epoch_ref_;   // 析构 → ReleaseEpoch
```

### 4.5 `zfwal/ZFPROPS`（新增元数据文件）

单行文本，首次 `Open()` 写入，之后只校验：

```
format_version=1
partitions=64
```

**必要性**：`partitions` 决定 `Route()` 结果。若重开时 P 变化，同一 user_key 会落到
不同分区，破坏"同 key 不跨分区"不变式（§6.1），进而破坏 M2.2 的输出无交集前提。
P 不匹配时 `Open()` 返回 `InvalidArgument`，而不是静默产生错误结果。

---

## 5. 触发与流控

### 5.1 判定函数

```cpp
bool ZeroFlushContext::ShouldSeal() const {
  uint64_t sum = 0, max_one = 0;
  for (uint32_t p = 0; p < zfo_.partitions; ++p) {
    uint64_t sz = wal_->ActiveSize(p);
    sum += sz;
    max_one = std::max(max_one, sz);
  }
  return sum >= zfo_.epoch_target_bytes ||
         max_one >= zfo_.partition_target_bytes;
}
```

- **主条件** `Σ ActiveSize ≥ epoch_target_bytes`：控制总内存/恢复量。
- **副条件** `max ActiveSize ≥ partition_target_bytes`：防分区倾斜下单文件无限增长
  （倾斜时其余分区可能几乎为空，主条件迟迟不满足）。

> 不用 MemTable 内存量做触发：`SlimMemTableRep::ApproximateMemoryUsage()` 当前返回 0，
> 且 slim 条目大小与 value 大小无关，MemTable 内存不再是有意义的水位。
> （仍应把该函数改为返回真实 arena 用量，供 `GetProperty` 与统计使用。）

### 5.2 触发点

`DBImpl::PreprocessWrite`（write thread leader，持 DB mutex）——原生检查
"memtable 是否已满"的同一位置：

```cpp
if (zf_ctx_ != nullptr && zf_ctx_->ShouldSeal()) {
  status = ZfSealEpochAndSwitch(cfd);   // §6.2
}
```

### 5.3 写流控（复用原生机制，不新增）

未物化 epoch 数 = `cfd->imm()->NumNotFlushed()`。让原生
`max_write_buffer_number` 机制直接承担 stall：`zeroflush::Open()` 中设置

```cpp
zf_opt.max_write_buffer_number =
    static_cast<int>(zfo.max_pending_epochs) + 1;   // +1 = 当前 mutable
```

于此，原生 `WriteController` 的 delay/stop 逻辑无需改动即可对 ZF 生效。

---

## 6. 关键流程

### 6.1 正确性不变式

整个 M2 依赖以下四条不变式，任何实现改动都必须逐条复核：

| ID | 不变式 | 保障方式 |
|---|---|---|
| **I1** | 同一 user_key 的所有版本恒在同一分区 | `Route` 为 user_key 的纯函数；P 由 `ZFPROPS` 固定 |
| **I2** | locator `(part, gen, offset)` 在被引用期间恒指向同一字节区间 | 封存代只读；活跃代只追加（`ReopenWritableFile`/`O_APPEND`）；`Freeze` 重置偏移（修 D2） |
| **I3** | `(p, g)` 文件可删 ⟺ 其全部记录已进入已持久化 Version 的 SSTable **且** 无任何存活 MemTable/迭代器引用它 | epoch 与 imm 1:1 绑定；`EpochRef` 随 imm 析构释放；unlink 在 `PurgeObsoleteFiles` |
| **I4** | 恢复后 `last_sequence ≥` 任何已持久化记录的 seq | `SetZeroFlushLastSequence` 取 `max(versions_->LastSequence(), replayed_max)` |

> I3 中"且"的后半段是 M1 从未面对的问题：迭代器持有旧 SuperVersion → 旧 imm →
> 旧 locator。若在 FlushJob 完成时就删文件，长事务迭代器会读到已删文件。
> `EpochRef` 挂在 MemTable 而非 FlushJob 上，正是为了让 RocksDB 现成的
> MemTable 引用计数替我们回答这个问题。

### 6.2 Seal：`ZfSealEpochAndSwitch`

调用约束：**持 DB mutex，且在 write thread 内独占**（与原生 `SwitchMemtable` 相同）。

```
1. E = ++epoch_counter_
2. SealedEpoch se; se.epoch = E; se.max_seq = versions_->LastSequence()
3. for p in [0, P):
       old_gen = wal_->Freeze(p)          // 已修 D2/D3
       se.gens.push_back({p, old_gen})
       se.total_bytes += <旧代文件大小>
4. se.gens += std::move(pending_orphan_gens_)   // 恢复期遗留的孤儿封存代，§7.3
5. sealed_cache_->AddEpoch(se)                  // refcount = 1
6. old_mem = cfd->mem()
   s = SwitchMemtable(cfd, &context)            // 原生；产生新 mutable
7. old_mem->SetZfEpochRef(EpochRef(this, E))    // 转移 refcount 所有权
8. SchedulePendingFlush(...) ; MaybeScheduleFlushOrCompaction()
```

**第 3 步失败怎么办？** `Freeze` 部分成功会留下"一半分区已换代"的状态。
处理：把 `Freeze` 的错误累积但**继续对剩余分区执行**，最终把 DB 置为
只读错误态（`SetBGError`）。理由：偏移已经推进，回滚不可能；而
"已换代 + 记录已在 se.gens 中"仍满足 I2/I3，数据可读，只是不再接受写入。

**新 MemTable 必须带上 zf_ctx**。M1 只在 `Open()` 时给 default CF 的首个 MemTable
设过 ctx；`SwitchMemtable` 造出的新 MemTable 会**没有 ctx**，读路径退化为把 16B
locator 当 value 返回。修复：把 ctx 存到 `ColumnFamilyData`，由
`ColumnFamilyData::ConstructNewMemtable` 传给 `MemTable` 构造函数。
—— 这是 M2 必改项，也是 M1 从未切换 MemTable 才得以隐藏的第五个休眠缺陷。

### 6.3 Materialize

#### M2.1：复用原生 FlushJob（打通闭环）

关键观察：`MemTableIterator::value()` **已经**在 M1 里做了 locator → value 解引用。
因此原生 `FlushJob` 遍历 imm 时拿到的就是真实 value，`BuildTable()` 产出的就是
value 内联的普通 SSTable。**M2.1 的"物化"实现 = 删掉 M1 的 flush 抑制**：

```cpp
// db_impl_compaction_flush.cc:2549 —— 删除以下三行
if (zf_ctx_ != nullptr) {
  return Status::OK();
}
```

外加：`FlushMemTable` 在 ZF 模式下先进 write thread 做 `ZfSealEpochAndSwitch`
（这样手动 `Flush()`、`db_bench` 的 flush、`WaitForFlushMemTable` 全部自然生效）。

IO 形态：按 user_key 有序遍历 → 对 P 个封存文件做**随机 pread**。
文件刚写完、页缓存命中率高，但这是 M2.1 的已知短板，由 M2.2 解决。

#### M2.2：`ZfMaterializeJob`（P 路并行顺序归并）

```
并发度 K = materialize_parallelism；P 个分区分片给 K 个 worker
worker 处理分区 p：
  1. 顺序整读 zf-wal-p-gE.log（≤ partition_target_bytes，内存可控）
  2. 解码为 (InternalKey(user_key, seq, type), value_slice) 向量
  3. 按 InternalKeyComparator 排序（保留全部版本，不做 snapshot 裁剪）
  4. BuildTable() → 一个 L0 SSTable
主线程：K 路全部成功 → 汇总 P 个 FileMetaData → 单次 VersionEdit → LogAndApply
        → 释放 imm（触发 Reclaim）
任一路失败 → 删除已生成的临时 SST，整个 epoch 物化失败并重试（imm 保留）
```

**为什么 P 个输出文件可以同时进 L0 而无需去重？**
由 I1，分区间 user_key 集合**无交集**，故 P 个输出文件不含相同 user_key。
L0 允许文件间范围重叠，只要同 user_key 的新旧版本次序由 seq 决定即可 —— 而
同 user_key 只可能出现在一个文件里，条件自动满足。
（这也解释了为什么 P 必须固定：见 §4.5。）

**内存上界** = `K × partition_target_bytes`（默认 8 × 64MB = 512MB）。
若过大，可把"整读"降级为分块读 + 外部归并；M2.2 先按整读实现并把该上界写进文档与选项校验。

### 6.4 读路径：按 gen 分派

```cpp
Status PartitionedWalManager::ReadRecord(const WalRecordRef& ref,
                                         std::string* buf, Slice* value) const {
  if (ref.part_id >= partitions_) return Status::Corruption("bad part_id");
  Partition* p = parts_[ref.part_id].get();

  uint32_t cur_gen; uint64_t flushed; std::string buf_copy; bool in_buf = false;
  {   // 只在锁内取快照，绝不持锁做 IO
    MutexLock l(&p->mu);
    cur_gen = p->gen; flushed = p->flushed_size;
    if (ref.gen == cur_gen && ref.offset >= flushed) {
      buf_copy = p->buf; in_buf = true;      // 拷贝未刷盘缓冲
    }
  }
  if (ref.gen > cur_gen) return Status::Corruption("ZF locator from future gen");
  if (in_buf)          return DecodeFromBuffer(buf_copy, ref.offset - flushed, buf, value);
  if (ref.gen == cur_gen) return PreadFrom(p->rfile.get(), ref, buf, value);   // 活跃代
  // 封存代
  std::shared_ptr<RandomAccessFile> f;
  Status s = sealed_cache_->Get(ref.part_id, ref.gen, &f);
  if (!s.ok()) return s;
  return PreadFrom(f.get(), ref, buf, value);
}
```

两点改进同时落地：

1. **gen 分派**（修 D1）。
2. **不再持 `p->mu` 做 IO**。M1 在整个 pread 期间持分区锁，同分区读者完全串行 ——
   这与性能报告里 readseq/readrandom 偏弱高度相关。改为锁内取快照、锁外 IO。
   代价是命中未刷盘缓冲时多一次 `buf` 拷贝（≤4KB），只影响刚写入的少量记录。

### 6.5 Reclaim

```
imm 被 FlushJob 安装后 → cfd->imm() 移除 → MemTable 引用归零 → ~MemTable()
  → ~EpochRef() → ctx->ReleaseEpoch(E)
      → refs_[E]-- ；归零则 epochs_[E].gens 的文件名进 pending_unlink_
DBImpl::PurgeObsoleteFiles（原生已在 flush/compaction 后调用）
  → zf_ctx_->PurgeSealedFiles() → 逐个 DeleteFile + 从 LRU 摘除句柄
```

若 `reclaim_sealed_files == false`，`ReleaseEpoch` 只减引用不入队（调试用）。

---

## 7. 恢复

### 7.1 `Open()`：发现活跃代

```
1. CreateDirIfMissing(dir) ；读/写 ZFPROPS 并校验 partitions（§4.5）
2. ListFiles() → all_gens: vector<(part, gen)>
3. for p in [0, P):
       active_gen[p] = max{ g : (p, g) ∈ all_gens }（无文件则 0）
       p->gen = active_gen[p]
       p->flushed_size = p->total_size = FileSize(p, active_gen[p])
       打开活跃代 rfile
4. orphan = { (p, g) ∈ all_gens : g < active_gen[p] }
   → pending_orphan_gens_ = orphan（等待下次 Seal 收养，§7.3）
```

### 7.2 `Recover()`：重放存活代

重放 `all_gens` 中**全部**文件（含孤儿封存代）到 mutable MemTable，然后：

```cpp
db->SetZeroFlushLastSequence(
    std::max(db->GetLatestSequenceNumber(), replayed_max_seq));   // 修 I4
```

**孤儿封存代为什么必须重放？——幂等性论证**

崩溃可能落在"SSTable 已安装、WAL 文件未删"之间。此时该 gen 的记录同时存在于
SSTable 与待重放的 WAL 中。重放后：

- MemTable 与 SSTable 中该记录的 **(user_key, seq, type, value) 完全一致**
  （物化只搬运，不改写 seq）；
- `Get`/`DBIter` 按 seq 降序取第一个可见版本，MemTable 版本优先命中，值相同；
- 因此重放是**幂等**的，唯一代价是这部分记录多占一份 MemTable 内存，直到下一个
  epoch 物化后释放。

结论：**M2 不需要任何 ZF 专属的"已物化"持久化标记**，也不需要 ZF 版 MANIFEST。
"文件还在 ⟹ 重放它"这一条规则即可保证崩溃一致性。这是本设计相对
"引入 ZFMANIFEST 记录 epoch 状态"方案的关键简化 —— 少一个需要自己保证原子性的
持久化结构，就少一类崩溃窗口。

反向的丢失风险则由 I3 排除：文件在物化 **且** 引用归零前绝不会被删。

### 7.3 孤儿封存代的收养

孤儿代的记录已重放进当前 mutable MemTable，其 locator 指向孤儿文件本身。
因此这些文件必须与"当前 mutable MemTable"同生命周期 —— 恰好等于下一个 epoch
的生命周期。故在 `ZfSealEpochAndSwitch` 第 4 步把 `pending_orphan_gens_` 并入
本次 `SealedEpoch`（§6.2），随该 epoch 一并物化、一并回收。

> ⚠️ **实现遗漏（R1）**：本节的**生命周期绑定是正确的**，但实现漏了一件事 ——
> 从 `Recover()` 返回到下一次 Seal 之间，孤儿代**没有在 `SealedFileCache` 中登记**，
> 而重放出的 locator 带着 `gen < ActiveGen(p)`、读路径会走封存分支，于是
> `SealedFileCache::Get` 的 in_epoch 校验必然失败 → `NotFound` → 重开后 `Get` 拿不到
> value（`FreezeReopen` / `MultiEpoch` FAIL 的根因）。
> 修复方案（保留本节收养语义，只补窗口内的可读性）见
> [M3_DESIGN.md §2 R1 与 §8.1](M3_DESIGN.md)。

### 7.4 恢复量上界

存活字节 ≤ `(max_pending_epochs + 1) × epoch_target_bytes`（正常关闭时孤儿代为空，
上界降为 `epoch_target_bytes`）。达成 G3。

---

## 8. 语义对齐（G5）

### 8.1 `WriteOptions::sync`

`WriteGroupToPartitionWal` 目前忽略 `sync`。改为：`ZfBatchHandler` 记录本次
write group 触达的分区集合 `touched`，收尾时

```cpp
if (sync) { for (uint32_t p : touched) s = wal_->Sync(p); }
```

只 sync 触达分区，而非 `SyncAll()` —— P=64 时后者会产生 64 次 fdatasync，
把 group commit 的收益全部抵消。

### 8.2 `zeroflush::DestroyDB`

```cpp
Status DestroyDB(const std::string& dbname, const Options& options,
                 const ZeroFlushOptions& zfo);
```

实现：先递归清理 `wal_dir/zfo.wal_subdir`（`GetChildren` → `DeleteFile` → `DeleteDir`），
再调原生 `rocksdb::DestroyDB`。落地后 `zf_test.cc` 的 `CleanDB()`（`system("rm -rf")`）
可替换为该接口 —— 让"测试能正确清理"变成产品能力而非测试技巧。

### 8.3 `Flush()` 真实生效

见 §6.3（M2.1）。同时 `FlushOptions::wait == true` 必须真正等到物化完成，
使 db_bench 的 `flush` benchmark 与 `WaitForFlushMemTable` 语义正确。

---

## 9. 分阶段落地计划

每阶段结束时 `zf_test` 必须全绿，且可独立提交。

| 阶段 | 内容 | 退出条件 |
|---|---|---|
| **M2.0** | 修 D1–D4 + `SwitchMemtable` 的 ctx 传播（§6.2 末）+ `ReadRecord` 锁外 IO + `ZFPROPS` | 新增 `TestFreezeCorrectness`（手工调 `Freeze` 后旧 locator 仍可读）通过；原 7 用例不回退 |
| **M2.1** | Epoch 模型 + `SealedFileCache` + `EpochRef` + 解除 flush 抑制 + 原生 FlushJob 物化 + Reclaim | G1/G2/G3/G4 达标；`zfwal` 大小稳定；kill -9 恢复正确 |
| **M2.2** | `ZfMaterializeJob`（P 路并行顺序归并） | G6 达标；物化吞吐相对 M2.1 提升 ≥ 2×（K=8） |
| **M2.3** | `WriteOptions::sync` + `zeroflush::DestroyDB` + 统计指标 + 文档/报告更新 | G5 达标；性能报告重跑并对比 M1 |

**建议提交粒度**：M2.0 单独一个 commit（纯缺陷修复，可独立回溯）；
M2.1 是 M2 的价值交付点，若时间紧张可先只交付到 M2.1。

---

## 10. 测试矩阵

在 `tools/zf_test.cc` 现有 7 例基础上新增：

| # | 用例 | 覆盖 |
|---|---|---|
| 8 | `FreezeKeepsOldLocatorReadable` | D1/D2/D3；手工 `Freeze` 后旧 key 全部可读 |
| 9 | `ReopenWithMultipleGens` | D4；构造多代文件后重开，写入不进封存文件 |
| 10 | `SingleEpochSealAndMaterialize` | §6.2/6.3；封存一次后 L0 出现 SSTable，WAL 被删，全 key 可读 |
| 11 | `MultiEpochWalBounded` | G1；写 10× `epoch_target_bytes`，`zfwal` 大小 ≤ 上界 |
| 12 | `IteratorPinsSealedFiles` | I3；迭代器跨 Seal+Flush 存活，期间不得读失败 |
| 13 | `CrashBeforeReclaimIdempotent` | §7.2；物化后跳过删除直接重开，条目数与值均正确（不翻倍） |
| 14 | `RecoverBoundedByLiveGens` | G3；历史写入 10×，恢复重放量只与存活代成正比 |
| 15 | `SyncSemantics` | §8.1；`sync=true` 写入后 kill -9，数据不丢 |
| 16 | `DestroyDBRemovesZfwal` | §8.2；`zeroflush::DestroyDB` 后目录不存在 |
| 17 | `ManualFlushWorks` | §8.3；`Flush()` 后 `imm` 为空、L0 非空 |
| 18 | `PartitionsMismatchRejected` | §4.5；改 P 重开返回 `InvalidArgument` |
| 19 | `SkewedPartitionTriggersSeal` | §5.1 副条件；单分区打满触发封存 |

崩溃测试（13/15）用子进程 `_exit(0)` 跳过析构模拟，避免依赖外部 kill。

---

## 11. 性能预期与后续

### 11.1 预期变化（相对 M1 基线）

| 指标 | 预期 | 原因 |
|---|---|---|
| `fillrandom` | 略降（M1 的 133%~173% → 约 110%~150%） | M2 恢复了物化开销；M1 的领先部分来自"从不 flush"这一不可持续的优势 |
| `readrandom` | 提升 | 锁外 IO（§6.4）消除同分区串行；老数据进 SSTable 后走 block cache |
| `readseq`（小 value） | 明显提升 | M1 最弱项（32B 时仅原生 15%）主因是逐条 locator 解引用 + 持锁；物化后大部分数据从 SSTable 顺序读 |
| 空间 | 大幅下降 | WAL 不再无限增长 |

**M1 的写性能领先必须重新解读**：它建立在"永不 flush、WAL 永不回收"之上，
不是可持续的对比。M2 完成后的数字才是 ZeroFlush 的真实收益。
性能报告应据此重写结论段。

### 11.2 M3 展望：PartitionTable 的真正价值

M2 保留 hash 路由。改成 **PartitionTable 范围路由**后，各分区的 user_key
**范围互不重叠** → `ZfMaterializeJob` 的 P 个输出文件范围互不重叠 →
可以**直接安装到 L1，完全跳过 L0→L1 compaction**。
这是范围路由的核心收益，也是它应作为独立里程碑而非 M2 子项的原因：
它改变的是 compaction 拓扑，验证成本和收益都远大于"换一个路由函数"。

### 11.3 统计指标（M2.3）

`zf.epochs_sealed` / `zf.epochs_materialized` / `zf.live_wal_bytes` /
`zf.sealed_wal_bytes` / `zf.materialize_micros` / `zf.sealed_read_count` /
`zf.sealed_cache_miss`，经 `GetProperty("rocksdb.zeroflush.*")` 暴露。

---

## 12. 风险登记

| 风险 | 影响 | 缓解 |
|---|---|---|
| `Freeze` 中途失败导致分区代际不齐 | 无法继续写 | 累积错误后继续封存剩余分区 → `SetBGError` 转只读；数据仍可读（§6.2） |
| M2.2 内存上界 `K × partition_target_bytes` 过大 | OOM | 选项校验 + 文档标注；必要时降级为分块外部归并 |
| 迭代器长期持有旧 imm 阻塞回收 | WAL 超出上界 | 指标 `zf.sealed_wal_bytes` 告警；超阈值时写流控（复用 §5.3） |
| 物化持续跟不上写入 | 写 stall | 与原生 `max_write_buffer_number` stall 行为一致，可解释可调 |
| `SwitchMemtable` 的 ctx 传播漏改某条路径 | 读到 16B locator 当 value（静默错误） | 在 `MemTable` 构造处 `assert(zf_ctx != nullptr)`（ZF 模式下）使其快速失败 |

---

## 13. 附：受影响文件清单

| 文件 | 改动 |
|---|---|
| `zeroflush/wal_manager.{h,cc}` | 修 D1–D4；`ReadRecord` gen 分派 + 锁外 IO；`Freeze` 返回封存文件大小 |
| `zeroflush/sealed_file_cache.{h,cc}` | **新增** §4.3 |
| `zeroflush/materialize_job.{h,cc}` | **新增** §6.3 M2.2（CSD 卸载的主机 fallback 形态） |
| `zeroflush/zeroflush_db.{h,cc}` | Epoch 计数与表；`ShouldSeal`；`ReleaseEpoch`/`PurgeSealedFiles`；`ZFPROPS`；`DestroyDB`；`Recover` 取 max seq；`sync` 语义 |
| `zeroflush/slim_memtable.cc` | `ApproximateMemoryUsage` 返回真实 arena 用量 |
| `db/memtable.{h,cc}` | 持有 `EpochRef`；构造期接收 ctx |
| `db/column_family.{h,cc}` | 保存 zf_ctx 并在 `ConstructNewMemtable` 传播 |
| `db/db_impl/db_impl_write.cc` | `PreprocessWrite` 触发 `ZfSealEpochAndSwitch` |
| `db/db_impl/db_impl_compaction_flush.cc` | 删除 flush 抑制（L2549）；`FlushMemTable` 走 ZF 封存路径 |
| `db/db_impl/db_impl_files.cc` | `PurgeObsoleteFiles` 调 `PurgeSealedFiles` |
| `tools/zf_test.cc` | 新增用例 8–19 |
| `CMakeLists.txt` | 新增两个源文件 |

---

**参考**：[M1_WAL_PERSISTENCE_FIX.md](M1_WAL_PERSISTENCE_FIX.md) ·
[README.md](README.md) ·
[性能基线报告](../../../output/zeroflush_m1_perf/PROJECT_SUMMARY.md)
