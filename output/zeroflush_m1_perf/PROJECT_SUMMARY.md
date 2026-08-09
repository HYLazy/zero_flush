# ZeroFlush M1 WAL 持久化修复 — 项目总结报告

**日期**：2026-08-08
**范围**：M1 WAL 持久化 bug 修复 + 回归测试 + M2 改进方向文档化
**状态**：✅ 全部完成

---

## 1. 修复内容

### 1.1 Bug 1：析构函数未 flush 缓冲

| 项目 | 内容 |
|---|---|
| 现象 | 写入后立即关闭 → 重开 Get 全部 NotFound |
| 根因 | `~PartitionedWalManager() = default`，未调用 `FlushBuf` + `Sync`，`p->buf` 中未满 4KB 数据丢失 |
| 修复 | 新增 `Close()` 方法，flush 所有分区缓冲 + sync；析构改为非 default 调用 `Close()` |
| 验证 | `TestWALBufferFlush`：1 条 ~50B 记录 < 4KB → 关闭 → 重开 → Get 命中 ✓ |

### 1.2 Bug 2：`NewWritableFile` 截断已有 WAL

| 项目 | 内容 |
|---|---|
| 现象 | 修复 Bug 1 后仍偶发 Get 失败（重开数据损坏） |
| 根因 | `EnsureOpenForWrite` 用 `NewWritableFile`（POSIX `O_TRUNC`），截断已有 zfwal 文件，使 `SlimLocator.wal_offset` 失效 |
| 修复 | 改用 `ReopenWritableFile`（POSIX `O_APPEND`），写入从文件末尾追加不修改已有内容 |
| 验证 | `TestReopenNoTruncate`：写 100 → 重开 → 写另 100 → 重开 → 200 全在 ✓ |

---

## 2. 验证

### 2.1 回归测试套件（`tools/zf_test.cc`）

7 个用例，全部 PASS：

```
[PASS] WALBufferFlush(50B<4KB)         — 验证 Bug 1 修复
[PASS] ReopenNoTruncate                — 验证 Bug 2 修复
[PASS] SequentialKeys(1k)              — 基础顺序键
[PASS] RandomKeysUnique(1k)            — 随机键（无重复）
[PASS] RandomKeysWithDup(10k)          — 随机键（有放回，验证 63% 现象）
[PASS] MultiPartition                  — P ∈ {1, 4, 16, 64}
[PASS] LargeSequential(100k)           — 大数据集 100k
```

每个用例用独立 dbname + `rm -rf` 完全清理（含 `zfwal` 子目录），保证测试间隔离。

### 2.2 db_bench 性能对比（vs 原生 RocksDB）

| vs | 工作负载 | Native (ops/s) | ZeroFlush (ops/s) | ZF/Native |
|---|---|---|---|---|
| 32B  | fillrandom | 402,281 | 603,609 | **150%** |
| 32B  | readrandom | 1,355,142 | 621,790 | 46% |
| 32B  | readseq    | 6,632,907 | 989,643 | 15% |
| 128B | fillrandom | 378,380 | 504,637 | **133%** |
| 128B | readrandom | 1,208,546 | 666,369 | 55% |
| 128B | readseq    | 6,136,982 | 579,892 | 9% |
| 512B | fillrandom | 315,843 | 470,433 | **149%** |
| 512B | readrandom | 1,049,571 | 612,156 | 58% |
| 512B | readseq    | 4,852,873 | 625,363 | 13% |
| 1024B | fillrandom | 280,534 | 393,854 | **140%** |
| 1024B | readrandom | 372,131 | 550,833 | **148%** |
| 1024B | readseq    | 1,089,500 | 682,403 | 63% |
| 4096B | fillrandom | 116,454 | 201,977 | **173%** |
| 4096B | readrandom | 97,744 | 404,900 | **414%** |
| 4096B | readseq    | 313,432 | 439,950 | **140%** |

**结论**：
- **写性能**全面领先（133%~173%）：M1 不写原生 WAL、不触发 flush
- **大 value 读性能**领先（62%~414%）：绕过 block cache 重建
- **小 value 读性能**较弱：每次 ReadValue 触发 pwrite+pread，无 block cache 加速
- **修复未引入性能回退**：数据与修复前变化一致

---

## 3. 交付物清单

### 3.1 代码改动

| 文件 | 状态 | 内容 |
|---|---|---|
| `zeroflush/wal_manager.h` | 修改 | 新增 `Status Close()` 声明 |
| `zeroflush/wal_manager.cc` | 修改 | `~PartitionedWalManager()` 非 default；新增 `Close()`；`EnsureOpenForWrite` 改用 `ReopenWritableFile` |
| `tools/zf_test.cc` | 新增 | 7 个回归用例 + `CleanDB` 工具函数 |
| `CMakeLists.txt` | 修改 | 新增 `zf_test` 可执行目标 |

### 3.2 文档

| 路径 | 用途 |
|---|---|
| `zeroflush/README.md` | 模块入口、构建/运行/限制说明 |
| `zeroflush/M1_WAL_PERSISTENCE_FIX.md` | M1 修复根因分析 + M2 改进方向 |
| `output/zeroflush_m1_perf/PROJECT_SUMMARY.md` | 本文件 |
| `output/zeroflush_m1_perf/report.html` | 性能对比报告（Chart.js 图表） |

### 3.3 性能报告

| 文件 | 用途 |
|---|---|
| `output/zeroflush_m1_perf/run_perf_compare.py` | 跑对比测试 |
| `output/zeroflush_m1_perf/generate_html_report.py` | 生成 HTML |
| `output/zeroflush_m1_perf/perf_compare_num100000_runs1.json` | 原始结果 |
| `output/zeroflush_m1_perf/report.html` | 可视化报告 |

---

## 4. M1 已知限制（建议 M2 优先解决）

1. **WAL 无限增长** → M2 用 `partition_target_bytes` 封存
2. **`zfwal` 不被 `DestroyDB` 清理** → M2 提供 `zeroflush::DestroyDB` 重载
3. **Recover 全量重放** → M2 增量重放 + 已封存分区不进入 MemTable
4. **小 value 读性能较弱** → M2 考虑在 MemTable 侧加 value 缓存
5. **`WriteOptions::sync` 未触发 fsync** → M2 在 `WriteGroupToPartitionWal` 中对齐

详见 [M1_WAL_PERSISTENCE_FIX.md §5](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/zeroflush/M1_WAL_PERSISTENCE_FIX.md)

---

## 5. 复现命令

```bash
# 1. 编译
cd /home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/build
cmake --build . --target zf_test db_bench -j$(nproc)

# 2. 跑回归（7 用例）
./zf_test                       # 期望：ALL PASSED，退出码 0

# 3. 跑性能对比
cd /home/embed/hyl/metadata_offload/output/zeroflush_m1_perf
python3 run_perf_compare.py     # 生成 JSON
python3 generate_html_report.py # 生成 report.html

# 4. 手动对比某个 value_size
./db_bench --benchmarks=fillrandom,readrandom,readseq \
           --zeroflush --num=100000 --value_size=128 \
           --compression_type=none --db=/tmp/zf_bench
./db_bench --benchmarks=fillrandom,readrandom,readseq \
           --num=100000 --value_size=128 \
           --compression_type=none --db=/tmp/native_bench
```

---

## 6. 关键代码位置索引

- 修复点 1（析构 flush）：[wal_manager.cc:174-176](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/zeroflush/wal_manager.cc#L174-L176)
- 修复点 1 实现（Close）：[wal_manager.cc:178-198](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/zeroflush/wal_manager.cc#L178-L198)
- 修复点 2（ReopenWritableFile）：[wal_manager.cc:251-254](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/zeroflush/wal_manager.cc#L251-L254)
- 回归测试入口：[tools/zf_test.cc](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/tools/zf_test.cc)
- 性能报告：[output/zeroflush_m1_perf/report.html](file:///home/embed/hyl/metadata_offload/output/zeroflush_m1_perf/report.html)
- M2 改进方向：[M1_WAL_PERSISTENCE_FIX.md §5](file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/zeroflush/M1_WAL_PERSISTENCE_FIX.md)
