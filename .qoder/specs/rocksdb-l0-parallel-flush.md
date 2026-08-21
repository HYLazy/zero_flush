# RocksDB L0 多SST Flush + 并行Compaction 实现方案

## Context

当前RocksDB的flush流程将每个immutable memtable刷新为**单个**L0 SST文件，导致L0层文件之间存在key范围重叠。这使得L0->L1 compaction必须串行化执行（同时最多一个），成为写入密集型场景下的性能瓶颈。

本方案修改flush流程，使其根据L1层现有SST的键值范围将memtable数据拆分为多个小SST文件。由于这些小SST的key范围与L1 SST对齐且不重叠，L0->L1 compaction可以按key范围并行执行，显著提高compaction吞吐量。

---

## 总体架构

```
Flush前:                          Flush后:
┌──────────────────┐              ┌──────────────────────────────────┐
│ Immutable Memtable│              │ L0: [sst-A] [sst-B] [sst-C]     │
│  key1..key100    │  ──split──>  │     对齐L1范围, 不重叠           │
└──────────────────┘              ├──────────────────────────────────┤
                                  │ L1: [SST-0] [SST-1] [SST-2]     │
                                  │     [k1..k30] [k31..k60] [k61..k90]│
                                  └──────────────────────────────────┘

并行Compaction:
  Thread-0: L0 sst-A + L1 SST-0 -> new L1
  Thread-1: L0 sst-B + L1 SST-1 -> new L1
  Thread-2: L0 sst-C + L1 SST-2 -> new L1
```

---

## 第一部分：Flush流程修改 — 生成多个小SST

### 1.1 FlushJob 头文件修改

**文件:** `db/flush_job.h`

在private区域(line ~234后)新增:
```cpp
// 多SST输出模式下的所有输出文件元数据
std::vector<FileMetaData> output_metas_;

// 新增方法：多SST flush逻辑
Status WriteLevel0TableMulti();

// 获取L1层SST的key范围边界
struct KeyRange {
  InternalKey smallest;
  InternalKey largest;
  uint64_t file_number;
};
std::vector<KeyRange> GetL1KeyRanges();
```

### 1.2 GetL1KeyRanges() 实现

**文件:** `db/flush_job.cc` (新增函数)

```cpp
std::vector<FlushJob::KeyRange> FlushJob::GetL1KeyRanges() {
  db_mutex_->AssertHeld();
  const int base_level = 1;  // L0的下一层固定为level 1
  const auto& l1_files = base_->storage_info()->LevelFiles(base_level);
  
  std::vector<KeyRange> ranges;
  for (const auto* f : l1_files) {
    if (f->being_compacted) continue;
    ranges.push_back({f->smallest, f->largest, f->fd.GetNumber()});
  }
  // L1文件已按smallest key排序(RocksDB不变式)
  return ranges;
}
```

### 1.3 WriteLevel0TableMulti() 核心实现

**文件:** `db/flush_job.cc` (新增函数，替代WriteLevel0Table的核心逻辑)

**整体流程:**

```
WriteLevel0TableMulti()
  │
  ├── 1. 获取L1 key范围 → GetL1KeyRanges()
  │     └── 若L1为空 → 回退到原始 WriteLevel0Table()
  │
  ├── 2. 为每个L1范围分配 FileMetaData + 文件编号
  │     └── versions_->NewFileNumber() (N个)
  │
  ├── 3. 创建N个 WritableFile + TableBuilder
  │     └── 复用BuildTable()中的文件创建模式
  │
  ├── 4. 单次遍历memtable (merging iterator)
  │     └── 对每个key: 二分查找目标bucket → builder->Add()
  │
  ├── 5. 处理range deletion (添加到所有覆盖的builder)
  │
  ├── 6. Finish每个builder → sync → 校验
  │     └── 空bucket: Abandon + 删除文件
  │
  └── 7. 对每个非空输出: edit_->AddFile(0, output_metas_[i])
```

**关键设计细节:**

#### (A) Bucket分配策略

L1有N个SST文件，创建N个bucket:
- Bucket i: 对应L1[i]的key范围 `[L1[i].smallest, L1[i].largest]`
- **范围外key处理** (归入最近范围):
  - key < L1[0].smallest → bucket 0 (最左)
  - key > L1[N-1].largest → bucket N-1 (最右)
  - L1[i].largest < key < L1[i+1].smallest (gap区域) → 比较与两边的距离，归入较近的bucket
- 由于memtable iterator已排序，遍历是有序的，bucket切换是单调递增的，可以用**线性扫描**代替二分查找（性能更好）

#### (B) 多Builder管理

不复用`BuildTable()`函数（它只接受单个FileMetaData），而是在`WriteLevel0TableMulti()`中**内联**关键的builder管理逻辑:

```cpp
// 伪代码 - 核心结构
struct BuilderContext {
  FileMetaData meta;
  std::unique_ptr<WritableFileWriter> file_writer;
  TableBuilder* builder;
  OutputValidator validator;
  bool has_output = false;
};

std::vector<BuilderContext> builders(N);
// 初始化每个builder (参考 builder.cc:150-211)
for (int i = 0; i < N; i++) {
  builders[i].meta.fd = FileDescriptor(versions_->NewFileNumber(), 0, 0);
  builders[i].meta.epoch_number = cfd_->NewEpochNumber();
  // 创建文件、builder等...
}

// 单次遍历 (参考 builder.cc:258-326)
c_iter.SeekToFirst();
int current_bucket = 0;
for (; c_iter.Valid(); c_iter.Next()) {
  const Slice& key = c_iter.key();
  // 找到目标bucket (由于有序遍历，current_bucket单调递增)
  while (current_bucket < N-1 && key > builders[current_bucket].range_end) {
    current_bucket++;
  }
  // 检查gap区域，可能需要归入current_bucket或current_bucket+1
  builders[current_bucket].builder->Add(key, value);
  builders[current_bucket].meta.UpdateBoundaries(key, value, seqno, type);
}
```

#### (C) Range Deletion处理

每个range tombstone `[start, end)` 需要添加到**所有**key范围与之重叠的builder:
```cpp
for (auto& tombstone : range_tombstones) {
  for (int i = 0; i < N; i++) {
    if (tombstone范围与bucket_i范围重叠) {
      auto kv = tombstone.Serialize();
      builders[i].builder->Add(kv.first.Encode(), kv.second);
      builders[i].meta.UpdateBoundariesForRange(...);
    }
  }
}
```

#### (D) VersionEdit更新

每个非空输出SST独立调用`edit_->AddFile(0, meta)`:
```cpp
for (int i = 0; i < N; i++) {
  if (builders[i].has_output) {
    edit_->AddFile(0 /* level */, builders[i].meta);
    flush_stats.bytes_written += builders[i].meta.fd.GetFileSize();
    flush_stats.num_output_files++;
  } else {
    // 空文件: Abandon + 删除
    builders[i].builder->Abandon();
    fs->DeleteFile(fname, ...);
  }
}
```

### 1.4 Run() 流程修改

**文件:** `db/flush_job.cc` line 294

```cpp
// 原来:
s = WriteLevel0Table();

// 改为:
if (enable_multi_sst_flush) {
  s = WriteLevel0TableMulti();
} else {
  s = WriteLevel0Table();
}
```

### 1.5 TryInstallMemtableFlushResults 兼容性

**文件:** `db/flush_job.cc` line 330

`TryInstallMemtableFlushResults()` 接受 `meta_.fd.GetNumber()` 参数用于memtable排序。多输出模式下，`meta_` 仍作为第一个文件的meta保留（在PickMemTable中分配），其他文件使用`output_metas_`。这确保了与现有flush排序逻辑的兼容性。实际的L0文件列表来自`edit_->new_files_`，已包含所有输出文件。

### 1.6 L1为空时的回退策略

当`GetL1KeyRanges()`返回空时:
1. **回退到单SST flush**: 调用原始`WriteLevel0Table()`生成单个L0 SST
2. L0 score机制会自动触发L0->L1 compaction来填充L1
3. 后续flush看到L1有文件后，自动切换到多SST模式

---

## 第二部分：Compaction触发条件修改

### 2.1 新增数据结构: L0BucketIndex

**文件:** 新建 `db/compaction/l0_bucket_index.h`

```cpp
struct L0Bucket {
  int bucket_id;                          // 对应L1 SST索引
  InternalKey smallest_key;               // bucket key范围起始
  InternalKey largest_key;                // bucket key范围结束
  std::vector<FileMetaData*> l0_files;    // 属于此bucket的L0文件
  uint64_t accumulated_size;              // 非compacting文件的累计大小
  int non_compacting_file_count;          // 非compacting文件数
};

class L0BucketIndex {
public:
  void Build(const VersionStorageInfo* vstorage,
             const InternalKeyComparator& icmp);
  
  // 返回累积大小 >= target_file_size_base 的bucket
  std::vector<const L0Bucket*> GetBucketsNeedingCompaction(
      uint64_t target_file_size_base) const;
  
  // 获取某个L0文件所属的bucket
  int GetBucketForFile(const FileMetaData* f) const;
  
  const std::vector<L0Bucket>& GetBuckets() const { return buckets_; }

private:
  std::vector<L0Bucket> buckets_;
};
```

### 2.2 Bucket分配算法

在`L0BucketIndex::Build()`中:

1. 获取L1文件列表（已按key排序）
2. 遍历所有L0文件，根据smallest/largest key确定所属bucket:
   - 用二分查找定位重叠的L1 SST
   - 由于flush阶段已对齐L1范围，大部分L0文件会精确落入一个bucket
   - 跨越多个bucket的文件归入重叠最大的bucket
3. 计算每个bucket的`accumulated_size`和`non_compacting_file_count`

### 2.3 VersionStorageInfo 集成

**文件:** `db/version_set.h` (line ~740后)

```cpp
// 新增成员
std::unique_ptr<L0BucketIndex> l0_bucket_index_;

// 新增getter
const L0BucketIndex* GetL0BucketIndex() const {
  return l0_bucket_index_.get();
}
```

**文件:** `db/version_set.cc` — `ComputeCompactionScore()` (line ~3911后)

在现有L0 score计算之后，新增per-bucket score计算:

```cpp
// 构建bucket索引
l0_bucket_index_ = std::make_unique<L0BucketIndex>();
l0_bucket_index_->Build(this, icmp_);

// 计算per-bucket score
double max_bucket_score = 0;
auto buckets_needing = l0_bucket_index_->GetBucketsNeedingCompaction(
    mutable_cf_options.target_file_size_base);

for (const auto* bucket : buckets_needing) {
  double bucket_score = static_cast<double>(bucket->accumulated_size) /
                        mutable_cf_options.target_file_size_base;
  max_bucket_score = std::max(max_bucket_score, bucket_score);
}

// L0 score取bucket score和原有file count score的最大值
score = std::max(score, max_bucket_score);
```

### 2.4 触发条件总结

| 条件 | 原来 | 修改后 |
|------|------|--------|
| L0 compaction触发 | L0文件数 >= `level0_file_num_compaction_trigger` | **任一bucket的累积大小 >= `target_file_size_base`** (或保留原条件作为兜底) |
| Write slowdown | L0文件数 >= `level0_slowdown_writes_trigger` | 保持不变 (文件数触发) |
| Write stop | L0文件数 >= `level0_stop_writes_trigger` | 保持不变 (文件数触发) |

---

## 第三部分：L0 Compaction并行化

### 3.1 解除串行化限制

**文件:** `db/compaction/compaction_picker_level.cc` line 811-823

**当前代码 (瓶颈):**
```cpp
if (start_level_ == 0 &&
    !compaction_picker_->level0_compactions_in_progress()->empty()) {
  // 有任何L0 compaction在进行 → 拒绝新的L0→base compaction
  if (PickSizeBasedIntraL0Compaction()) return true;
  return false;
}
```

**修改为:**
```cpp
if (start_level_ == 0 &&
    !compaction_picker_->level0_compactions_in_progress()->empty()) {
  // 尝试pick一个不重叠bucket的并行L0→L1 compaction
  if (PickParallelL0Compaction()) return true;
  // 回退到intra-L0
  if (PickSizeBasedIntraL0Compaction()) return true;
  return false;
}
```

### 3.2 新增 PickParallelL0Compaction()

**文件:** `db/compaction/compaction_picker_level.cc` (新增函数)

```cpp
bool LevelCompactionBuilder::PickParallelL0Compaction() {
  const L0BucketIndex* bucket_index = vstorage_->GetL0BucketIndex();
  if (!bucket_index) return false;
  
  auto candidates = bucket_index->GetBucketsNeedingCompaction(
      mutable_cf_options_.target_file_size_base);
  
  for (const auto* bucket : candidates) {
    // 检查此bucket的key范围是否与正在进行的L0 compaction重叠
    if (compaction_picker_->RangeOverlapWithCompaction(
            bucket->smallest_key, bucket->largest_key,
            /*output_level=*/1, /*parent_index=*/nullptr)) {
      continue;  // 重叠，跳过
    }
    
    // 收集此bucket中未被compacting的L0文件
    start_level_inputs_.clear();
    start_level_inputs_.level = 0;
    for (auto* f : bucket->l0_files) {
      if (!f->being_compacted) {
        start_level_inputs_.files.push_back(f);
      }
    }
    if (start_level_inputs_.empty()) continue;
    
    // 获取output level的重叠文件
    output_level_ = 1;  // L0→L1
    is_parallel_l0_compaction_ = true;
    return true;
  }
  return false;
}
```

### 3.3 SetupOtherL0FilesIfNeeded() 修改

**文件:** `db/compaction/compaction_picker_level.cc` line 341-347

```cpp
bool LevelCompactionBuilder::SetupOtherL0FilesIfNeeded() {
  if (start_level_ == 0 && output_level_ != 0 && !is_l0_trivial_move_) {
    // 并行compaction模式下，不扩展L0文件列表
    // bucket分配已确保选择了正确的文件
    if (is_parallel_l0_compaction_) {
      return true;  // 跳过GetOverlappingL0Files()
    }
    return compaction_picker_->GetOverlappingL0Files(
        vstorage_, &start_level_inputs_, output_level_, &parent_index_);
  }
  return true;
}
```

**关键点:** `GetOverlappingL0Files()` (line 1296) 中有 `assert(level0_compactions_in_progress()->empty())` 断言。并行模式下跳过此函数调用，避免断言失败。

### 3.4 RegisterCompaction() 修改

**文件:** `db/compaction/compaction_picker.cc` line 1214-1233

当前line 1218-1221的assert需要修改以允许非重叠的并行L0 compaction:

```cpp
void CompactionPicker::RegisterCompaction(Compaction* c) {
  if (c == nullptr) return;
  
  // 修改: 对并行L0 compaction放宽断言
  // 原来的assert检查所有compaction不重叠 → 改为仅检查同level不同bucket
  if (ioptions_.compaction_style == kCompactionStyleLevel &&
      c->start_level() == 0 && !is_parallel_l0_compaction(c)) {
    assert(c->output_level() == 0 ||
           !FilesRangeOverlapWithCompaction(...));
  }
  
  // 仍然添加到 level0_compactions_in_progress_
  if (c->start_level() == 0 && ...) {
    level0_compactions_in_progress_.insert(c);
  }
  compactions_in_progress_.insert(c);
}
```

### 3.5 PickFilesMarkedForCompaction 修改

**文件:** `db/compaction/compaction_picker.cc` line 1266

```cpp
// 当前: L0 compaction在进行中时直接跳过
if (*start_level == 0 && !level0_compactions_in_progress()->empty()) {
  return false;
}

// 改为: 允许并行，检查范围重叠
if (*start_level == 0 && !level0_compactions_in_progress()->empty()) {
  if (RangeOverlapWithCompaction(...)) {
    return false;
  }
}
```

### 3.6 并行安全保证

并行compaction的安全性由以下机制保证:
1. **Bucket边界来自L1 SST边界** — 不同bucket的L0文件key范围不重叠
2. **`FilesRangeOverlapWithCompaction()` 检查** — 已有的range overlap检查防止output level冲突
3. **`being_compacted` 标记** — `MarkFilesBeingCompacted()` 防止同一文件被多个compaction选中
4. **VersionEdit原子性** — 每个compaction独立提交edit到MANIFEST

---

## 第四部分：配置选项

### 4.1 新增选项

**文件:** `include/rocksdb/advanced_options.h`

```cpp
// 启用多SST flush模式：flush时按L1 key范围拆分生成多个小SST
bool enable_multi_sst_flush = false;
```

**文件:** `options/cf_options.h` — MutableCFOptions 中新增对应字段

当`false`时，走原始单SST flush路径（零开销）。当`true`时，激活多SST flush + 并行compaction。

---

## 关键修改文件清单

| 文件 | 修改类型 | 修改内容 |
|------|----------|----------|
| `db/flush_job.h` | 修改 | 新增 `output_metas_`, `WriteLevel0TableMulti()`, `GetL1KeyRanges()`, `KeyRange` 结构 |
| `db/flush_job.cc` | **核心修改** | 实现 `WriteLevel0TableMulti()` (~200行新代码)，修改 `Run()` 分支 |
| `db/version_set.h` | 修改 | `VersionStorageInfo` 新增 `l0_bucket_index_` 成员和getter |
| `db/version_set.cc` | 修改 | `ComputeCompactionScore()` 中新增per-bucket score计算 (line ~3911) |
| `db/compaction/compaction_picker_level.h` | 修改 | 新增 `PickParallelL0Compaction()` 声明，`is_parallel_l0_compaction_` 标志 |
| `db/compaction/compaction_picker_level.cc` | **核心修改** | 修改 `PickFileToCompact()` (line 811), `SetupOtherL0FilesIfNeeded()` (line 341), `SetupInitialFiles()` (line 207)，实现 `PickParallelL0Compaction()` |
| `db/compaction/compaction_picker.cc` | 修改 | 修改 `RegisterCompaction()` (line 1214) 断言，`PickFilesMarkedForCompaction()` (line 1266) |
| `db/compaction/l0_bucket_index.h` | **新建** | `L0Bucket` 和 `L0BucketIndex` 类定义 |
| `db/compaction/l0_bucket_index.cc` | **新建** | bucket构建算法实现 |
| `include/rocksdb/advanced_options.h` | 修改 | 新增 `enable_multi_sst_flush` 选项 |
| `options/cf_options.h` | 修改 | MutableCFOptions 新增字段 |
| `CMakeLists.txt` | 修改 | 添加 `l0_bucket_index.cc` 到编译 |

---

## 实施步骤

### Step 1: 基础设施
1. 新建 `db/compaction/l0_bucket_index.h` 和 `.cc`
2. 在 `include/rocksdb/advanced_options.h` 和 `options/cf_options.h` 添加 `enable_multi_sst_flush` 选项
3. 更新 `CMakeLists.txt` 添加新源文件

### Step 2: Flush流程修改
1. 修改 `db/flush_job.h` — 新增成员和方法声明
2. 在 `db/flush_job.cc` 实现 `GetL1KeyRanges()`
3. 在 `db/flush_job.cc` 实现 `WriteLevel0TableMulti()` — 这是最核心的函数，包含:
   - L1范围获取和L1为空回退
   - N个Builder的创建和管理
   - 单次memtable遍历 + bucket分配
   - Range deletion处理
   - 多文件Finish和VersionEdit更新
4. 修改 `FlushJob::Run()` — 添加分支调用 `WriteLevel0TableMulti()`

### Step 3: Compaction Score修改
1. 修改 `db/version_set.h` — `VersionStorageInfo` 新增 `l0_bucket_index_`
2. 修改 `db/version_set.cc` — `ComputeCompactionScore()` 中构建bucket索引和计算per-bucket score

### Step 4: Compaction Picker修改
1. 修改 `db/compaction/compaction_picker_level.h` — 新增声明
2. 修改 `db/compaction/compaction_picker_level.cc`:
   - 修改 `PickFileToCompact()` line 811-823 — 允许并行pick
   - 实现 `PickParallelL0Compaction()`
   - 修改 `SetupOtherL0FilesIfNeeded()` line 341-347 — 跳过L0扩展
3. 修改 `db/compaction/compaction_picker.cc`:
   - 修改 `RegisterCompaction()` line 1218-1221 — 放宽断言
   - 修改 `PickFilesMarkedForCompaction()` line 1266 — 允许并行

### Step 5: 测试和验证

---

## 验证方案

### 编译验证
```bash
cd rocksdb-flush
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug
make -j$(nproc) rocksdb
```

### 单元测试
1. **Flush多SST输出测试**: 修改 `db/flush_job_test.cc`
   - 构造L1有3个SST的场景
   - 触发flush，验证生成了3个小SST
   - 验证每个SST的key范围与对应L1 SST对齐
   - 验证所有memtable KV都被正确写入

2. **L1为空回退测试**: 
   - L1为空时触发flush，验证回退到单SST模式

3. **Range deletion分配测试**:
   - 构造跨多个bucket的range deletion
   - 验证range deletion被正确写入所有覆盖的SST

4. **并行compaction测试**: 修改 `db/compaction/compaction_picker_test.cc`
   - 构造多个bucket累积大小超阈值的场景
   - 验证PickCompaction()能pick多个并行的L0→L1 compaction
   - 验证不同compaction的key范围不重叠

5. **Compaction触发条件测试**:
   - 验证bucket累积大小 >= target_file_size_base时触发compaction
   - 验证未达阈值时不触发

### 集成测试
```bash
# 使用db_bench验证
./db_bench --benchmarks=fillrandom --num=1000000 --value_size=1024 \
  --enable_multi_sst_flush=true --target_file_size_base=67108864 \
  --write_buffer_size=67108864 --max_background_compactions=4
```

### 正确性验证
- 运行现有测试套件: `make -j$(nproc) check`
- 特别关注 `db_flush_test`, `db_compaction_test`, `compaction_picker_test`

### 边界条件
- L1为空 → 回退单SST
- L1只有1个SST → 生成1个SST（等同原始行为）
- memtable为空 → 所有builder为空，不生成任何SST
- 所有key集中在一个bucket → 其他bucket为空，只生成1个SST
- key均匀分布 → 每个bucket都有输出
