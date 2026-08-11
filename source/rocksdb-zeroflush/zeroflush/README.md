# ZeroFlush M1（zero flush LSM-tree 优化）

> 本目录实现 ZeroFlush 论文第一阶段（M1）的实验方案，将 MemTable 改造为
> **仅存 SlimLocator（16 字节）**，value 的唯一持久副本落在分区 WAL 中。
> 此方案从根本上消除了原生 RocksDB 的 memtable flush 开销。

## 模块结构

```
zeroflush/
├── README.md                   # 本文件（模块入口）
├── M1_WAL_PERSISTENCE_FIX.md   # M1 修复与 M2 改进方向技术文档
├── M2_DESIGN.md                # M2 方案设计：封存 · 物化 · 回收（Epoch 模型）
├── M3_DESIGN.md                # M3 方案设计：范围路由 · 消除 L0 · API 完备
├── slim_memtable.{h,cc}        # SlimMemTableRep：仅存 16B SlimLocator
├── wal_format.{h,cc}           # ZfRecord 记录格式 / WalScanner 重放 / ZFPROPS
├── wal_manager.{h,cc}          # PartitionedWalManager：分区 WAL 写/读/封存
├── sealed_file_cache.{h,cc}    # 封存代只读句柄缓存 + epoch 引用计数 + 延迟 unlink
└── zeroflush_db.{h,cc}         # ZeroFlushContext / Open / Recover / Epoch 管理
```

## 核心改动

| 文件 | 作用 |
|---|---|
| `wal_manager.cc` | 分区 WAL 写句柄：每分区独立文件，4KB 缓冲批写入 |
| `wal_format.cc` | WAL 记录格式：`[header(8B)][key][value][seq:8B]` |
| `slim_memtable.cc` | 替换原生 memtable 工厂，value 字段替换为 SlimLocator |
| `zeroflush_db.cc` | `Open` 入口；`Recover` 重放 WAL 到 Slim MemTable；`WriteGroupToPartitionWal` 写路径 |

## 构建

```bash
cd /home/embed/hyl/metadata_offload/source/rocksdb-zeroflush
mkdir -p build && cd build
cmake .. && make -j$(nproc) db_bench zf_test
```

## 运行

### 回归测试（强烈建议每次提交前跑一遍）

```bash
cd build && ./zf_test
```

预期：13 个用例全部 PASS，退出码 0。每个用例用独立 dbname 且通过 `rm -rf`
完全清理（含 `zfwal` 子目录），保证测试间隔离。

### 性能对比（vs 原生 RocksDB）

```bash
cd /home/embed/hyl/metadata_offload/output/zeroflush_m1_perf
python3 run_perf_compare.py        # 跑 fillrandom/readrandom/readseq
python3 generate_html_report.py    # 生成 report.html
```

最新报告：
[`/home/embed/hyl/metadata_offload/output/zeroflush_m1_perf/report.html`](/home/embed/hyl/metadata_offload/output/zeroflush_m1_perf/report.html)

### db_bench 手动跑

```bash
# ZeroFlush 模式
./db_bench --benchmarks=fillrandom,readrandom,readseq \
           --zeroflush --num=100000 --value_size=128 \
           --compression_type=none --db=/tmp/zf_bench

# 对照：原生 RocksDB（去掉 --zeroflush）
./db_bench --benchmarks=fillrandom,readrandom,readseq \
           --num=100000 --value_size=128 \
           --compression_type=none --db=/tmp/native_bench
```

## 已知 M1 限制（详见 [M1_WAL_PERSISTENCE_FIX.md §5](M1_WAL_PERSISTENCE_FIX.md)）

1. **WAL 无限增长**：M1 无 flush/封存机制，分区 WAL 永不删除
2. **`zfwal` 不被 `DestroyDB` 清理**：测试与生产部署需显式 `rm -rf` 整个 dbname
3. **Recover 全量重放**：每次重开把 zfwal 全部重放到 MemTable，无增量
4. **小 value 读性能较弱**：每次 ReadValue 走 pwrite+pread，无 block cache 加速
5. **写路径 fsync 频率未对齐原生语义**：`WriteOptions::sync` 当前不触发 fsync

## 当前状态

- ✅ WAL 持久化两处 bug 已修复（析构 flush + ReopenWritableFile 防截断）
- ✅ M1 回归 7/7 PASS
- ✅ 与原生 RocksDB 性能对比报告已生成
- ✅ **M2 方案已实现**（见 [M2_DESIGN.md](M2_DESIGN.md)）：
  - M2.0：Freeze 路径缺陷修复 + ZFPROPS 分区校验
  - M2.1：Epoch 封存/物化/回收闭环
  - M2.3：sync 语义 + DestroyDB 清理 + ZFPROPS 拒绝
- ✅ **M3.0 已全部完成**（2026-08-10）:
  - R1：修复孤儿封存代未登记 SealedFileCache（恢复期 epoch 机制）
  - R2：IteratorPins 测试缺陷修复
  - R3：sync 移出 DB mutex 临界区，消除持锁 fsync 死锁
  - R4：清理约 207 万行 `fprintf(stderr, "DEBUG …")`，替换为 `ROCKS_LOG_*`（`use_logger` 条件门控）
  - R5：`zf.*` 统计指标（10 项，经 `GetProperty("rocksdb.zeroflush.*")` 暴露）+ `SlimMemTableRep::ApproximateMemoryUsage` 真实内存统计
  - `zf_test` 13 例全绿，输出 45 行（DEBUG 0 行），可开工 M3.1
- ✅ **M3 方案设计已定稿**（见 [M3_DESIGN.md](M3_DESIGN.md)）：
  M3.0 清偿 M2 债务 → M3.1 PartitionTable 范围路由 → M3.2 并行物化 + 跳过 L0
  直装 base level → M3.3 Materialize-into-BaseLevel 融合归并 →
  M3.4 多列族/Merge/DeleteRange → M3.5 CSD 卸载后端
- ⏳ **下一步**：M3.1（PartitionTable 范围路由），在 [M3_DESIGN.md](M3_DESIGN.md) 中由 13 例全绿准入门槛保护

## 参考

- 论文：Clara — ADOC, Automatically Harmonizing Dataflow Between Components in
  Log-Structured Key-Value Stores（见 `doc/`）
- 设计文档：`exp_design/raw/plan/ZeroFlush_RocksDB实现设计文档.docx`
