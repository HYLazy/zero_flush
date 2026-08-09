# ZeroFlush M1 → M2：WAL 持久化修复与改进方向

> 状态：M1 修复已完成并通过 zf_test 回归套件验证；本文件记录 M1 的 WAL 持久化
> bug 根因、修复方案，以及 M2 落地时需要解决的 M1 已知限制。

---

## 1. 背景

ZeroFlush M1 将 MemTable 改造为**仅存 SlimLocator（16 字节）**，value 的唯一持久副本
落在分区 WAL 中（`dbname/zfwal/zf-wal-{part_id}-{gen}.log`）。这一架构带来两个
新的持久化不变量：

1. **WAL 必须在关闭前完整刷盘**，否则未满 4KB 的缓冲数据会全部丢失，
   而这些数据是部分记录的**唯一**持久副本（M1 无 flush 兜底）。
2. **重开时必须不截断已有 WAL**，否则 `SlimLocator` 指向的 value 偏移量会失效。

本次会话发现并修复了破坏这两个不变量的两个 bug。

---

## 2. Bug 1：析构函数未 flush 缓冲

### 2.1 现象

- zf_test 写入 1000 条记录后立即关闭（`db.reset()`）
- 重开 Get 任意 key → `NotFound`
- zfwal 文件存在但大小为 0 字节（写入的数据全在内存缓冲 `p->buf` 中，未刷盘）

### 2.2 根因

`PartitionedWalManager` 的析构函数是 `= default`：

```cpp
class PartitionedWalManager {
  // ...
  ~PartitionedWalManager() = default;  // 不调用 FlushBuf / Sync
};
```

写路径（[wal_manager.cc:Append](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/zeroflush/wal_manager.cc)）采用
**批缓冲**优化：先写入 `p->buf`（4KB），满了才调用 `FlushBuf` 落盘到 `wfile`。
但 `FlushBuf` 内部仅写文件，**不调用 `Sync`**；`Sync` 由调用方（如 db_bench）显式
或 DBImpl 关闭流程触发。

`DBImpl` 关闭流程在 ZeroFlush 路径上不调用 `PartitionedWalManager::Sync`（M1 未实现
native WAL → zfwal 的 sync hook），因此 `p->buf` 中的未满 4KB 数据在析构时静默丢失。

### 2.3 修复

新增 `Close()` 方法，flush 所有分区缓冲并 sync：

```cpp
Status PartitionedWalManager::Close() {
  Status s;
  for (uint32_t i = 0; i < partitions_; ++i) {
    Partition* p = parts_[i].get();
    MutexLock l(&p->mu);
    s = FlushBuf(p);  // 把 p->buf 写入 p->wfile
    if (!s.ok()) return s;
    if (p->wfile) {
      s = p->wfile->Sync();  // fsync 到磁盘
      if (!s.ok()) return s;
    }
  }
  return Status::OK();
}
```

将析构函数改为非 default：

```cpp
PartitionedWalManager::~PartitionedWalManager() {
  Close().PermitUncheckedError();  // 析构中无法传播 Status
}
```

并在头文件声明 `Status Close()`。

---

## 3. Bug 2：`NewWritableFile` 截断已有 WAL

### 3.1 现象

zf_test 在 Bug 1 修复后仍偶发 Get 失败：
- 写入 1000 条 → 关闭 → 重开 → Get 部分 key 失败
- 检查 zfwal 文件大小正常，**但读出的 value 是脏数据或 NotFound**

### 3.2 根因

`EnsureOpenForWrite` 在首次打开 `wfile` 时使用 `NewWritableFile`：

```cpp
env_->NewWritableFile(FileName(p->part_id, p->gen), &p->wfile, opts);
```

RocksDB POSIX 实现中 `NewWritableFile` 使用 `O_CREAT | O_TRUNC`（[env/fs_posix.cc:OpenWritableFile](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/env/fs_posix.cc)），
**会截断已有文件**。

但首次打开 `wfile` 的时机是 `EnsureOpenForWrite`，即 `Append` 首次写某分区时调用。
如果该分区在重开时已存在（`zfwal` 文件残留），`NewWritableFile` 会把它清空，
导致已恢复的 1000 条记录的 `SlimLocator.wal_offset` 指向的位置全部失效。

### 3.3 修复

改用 `ReopenWritableFile`（POSIX 实现使用 `O_CREAT | O_APPEND`）：

```cpp
env_->ReopenWritableFile(FileName(p->part_id, p->gen), &p->wfile, opts);
```

`O_APPEND` 保证：写入从文件末尾开始，**不修改已有内容**，新数据追加在已恢复
数据之后。`SlimLocator.wal_offset` 仍指向已恢复数据的正确位置。

---

## 4. 验证

`tools/zf_test.cc` 提供了 7 个回归用例，覆盖上述两个修复点以及读写正确性：

| 用例 | 验证目标 | 状态 |
|---|---|---|
| WALBufferFlush | Bug 1：单条记录 < 4KB → 关闭 → 重开 → 命中 | ✅ |
| ReopenNoTruncate | Bug 2：写 100 → 重开 → 写另 100 → 重开 → 200 全在 | ✅ |
| SequentialKeys | 基础顺序键 1k，写/迭代器/Get/重开 | ✅ |
| RandomKeysUnique | 随机键（无重复）1k | ✅ |
| RandomKeysWithDup | 随机键（有放回）10k，验证唯一键数 ≈ 632 × (writes/1000) | ✅ |
| MultiPartition | partitions ∈ {1, 4, 16, 64} 各跑 500 条 | ✅ |
| LargeSequential | 10 万顺序键写/迭代器/Get/重开 | ✅ |

执行：

```bash
cd build && cmake --build . --target zf_test && ./zf_test
# 期望输出：7 PASSED，退出码 0
```

注意：每个用例用独立 dbname，且通过 `CleanDB`（rm -rf）清理整个目录以避免
`zfwal` 子目录跨用例累积（见 §5.2）。

---

## 5. M1 已知限制（M2 改进方向）

### 5.1 无 flush 机制 → WAL 无限增长

**现象**：M1 写路径不调用 MemTable flush，分区 WAL 永远不封存、不删除。
重开时全量重放 zfwal 到 MemTable。

**影响**：
- 长时间运行后 zfwal 占用空间线性增长
- 重开时间随历史写入量线性增长
- MemTable 中保留全部历史版本（DBIter 去重后正确，但占内存）

**M2 方案**：使用 `ZeroFlushOptions::partition_target_bytes`（默认 64MB）作为封存阈值。
分区 WAL 达到阈值后：

1. 关闭当前 gen 的 `wfile` 并 sync
2. 在 MemTable 中将该分区对应的 SlimLocator 标记为「封存」（`gen++` 时无需
   再写新记录时即可淘汰）
3. 重开时只重放 `gen` 之后的 WAL（增量恢复）

实现位置：参考 `ZeroFlushOptions::partition_target_bytes` 字段（在
[zeroflush_db.h](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/zeroflush/zeroflush_db.h) 已声明但未使用）。

### 5.2 `zfwal` 不被 `DestroyDB` 清理

**现象**：RocksDB 的 `DestroyDB` 只删除由 DB 创建的原生文件（MANIFEST、OPTIONS、
*.log、*.sst），**不删除自定义子目录**。`zfwal` 是 ZeroFlush 自定义的子目录，
`DestroyDB` 完全无视它。

**影响**：
- 测试间需要显式 `rm -rf dbname` 才能保证隔离（zf_test 的 `CleanDB` 正是为此而设）
- 用户文档需要明确提示：删除 DB 时必须同步删除 `dbname/zfwal`

**M2 方案**：
- 提供 `zeroflush::DestroyDB(dbname)` 重载，递归删除 `dbname` 整棵子树
- 或在 `Open` 时检测 `zfwal` 目录是否存在并提供清理选项

### 5.3 Recover 增量重放

**现象**：`ZeroFlushContext::Recover`（[zeroflush_db.cc:184](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/zeroflush/zeroflush_db.cc#L184)）**全量重放 zfwal 所有 gen** 到 MemTable。
但若 §5.1 封存机制落地，MemTable 中应只保留 **unfrozen 分区** 的最新 locator，
已封存分区的 locator 可从对应 gen 的 WAL 中按需 lazy load。

**M2 方案**：
1. `Recover` 只重放 last_seq 之后的 WAL（即上次关闭时未封存的最新一段）
2. 已封存分区的 `SlimLocator` 持久化到 manifest（而非内存 MemTable）
3. `ReadValue` 在 locator 指向封存分区时按需解析（已有路径，只需 locator 中
   携带 `gen` 字段——M1 的 SlimLocator 已包含 `gen`，但 Recover 路径未利用）

### 5.4 写路径的 fsync 频率

**现象**：当前 `Close()` 才 sync 一次。正常 db_bench 运行中，
`wal_->Sync()` 是否被 DBImpl 触发？目前未确认。

**验证方法**：

```bash
./db_bench --benchmarks=fillrandom --zeroflush --sync=1 --num=1000
# 观察崩溃恢复后数据完整性
```

**M2 方案**：在 `WriteGroupToPartitionWal` 入口（`zeroflush_db.cc:WriteGroupToPartitionWal`）
中根据 `WriteOptions::sync` 调用 `wal_->Sync()`，对齐原生 RocksDB 的语义。

---

## 6. 文件清单

本次修复涉及的文件：

- [zeroflush/wal_manager.h](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/zeroflush/wal_manager.h)：新增 `Status Close()` 声明
- [zeroflush/wal_manager.cc](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/zeroflush/wal_manager.cc)：
  - `~PartitionedWalManager()` 改为非 default，调用 `Close()`
  - 新增 `Close()` 实现：flush + sync 所有分区
  - `EnsureOpenForWrite()` 改用 `ReopenWritableFile` 替代 `NewWritableFile`
- [tools/zf_test.cc](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/tools/zf_test.cc)：新增 7 个回归用例
- [CMakeLists.txt](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/CMakeLists.txt)：新增 `zf_test` 可执行目标

---

## 7. 验证时间线

| 验证项 | 结果 |
|---|---|
| zf_test 7 用例 | 全 PASS |
| db_bench fillseq + readseq | 100000/100000 遍历（100%） |
| db_bench fillrandom + readrandom | 命中率 ≈ 唯一键占比 ≈ 63.2%（fillrandom 随机键有放回采样的数学必然） |
