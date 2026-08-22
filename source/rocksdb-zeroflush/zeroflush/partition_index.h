//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M4.3: PartitionIndex / PartitionIndexSet —— 终态分区索引。
//
//  终态架构（用户设计）：L0 = 分区 WAL 段 + 每分区跳表（有序索引）。
//  跳表条目 = [varint ik_size][internal key][varint loc_size=16][SlimLocator]，
//  与 SlimMemTableRep 同格式（MemTable::Add 编码）。value 唯一副本在分区
//  WAL，由 locator 定点引用；分区满（WAL 字节阈值）→ 该分区独立 freeze →
//  compact（与 L1 融合归并）→ 释放索引与 WAL 段。
//
//  并发模型（D2：空间换锁、指针交换不等待）：
//   - 写：InlineSkipList 并发插入（M4.1b 已验证）+ ConcurrentArena（每分区
//     独立 arena，无跨分区锁）；
//   - freeze：active → frozen 链的 atomic shared_ptr 交换 + WAL 换代
//     （分区锁内），进行中写组记录进旧索引（并发插入对 frozen 索引仍安全）；
//   - 读：frozen 链只读共享（引用计数保证 compact 完成前不析构）。

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "memtable/inlineskiplist.h"
#include "memory/concurrent_arena.h"
#include "rocksdb/memtablerep.h"
#include "table/internal_iterator.h"
#include "table/merging_iterator.h"
#include "util/coding.h"
#include "zeroflush/wal_format.h"  // SlimLocator

namespace zeroflush {

// 跳表比较器：与 MemTable::KeyComparator 相同语义
// （internal key 按 (user_key 升序, seq 降序) 排序——同 user_key 时
// 大 seq 在前，Get Seek 到 (user_key, snapshot) 即得最新 ≤ snapshot 版本）。
class ZfKeyComparator final : public ROCKSDB_NAMESPACE::MemTableRep::KeyComparator {
 public:
  explicit ZfKeyComparator(const ROCKSDB_NAMESPACE::InternalKeyComparator& c)
      : comparator(c) {}

  int operator()(const char* prefix_len_key1,
                 const char* prefix_len_key2) const override {
    return comparator.CompareKeySeq(ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(prefix_len_key1),
                                    ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(prefix_len_key2));
  }
  int operator()(const char* prefix_len_key,
                 const ROCKSDB_NAMESPACE::Slice& key) const override {
    return comparator.CompareKeySeq(ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(prefix_len_key), key);
  }

  ROCKSDB_NAMESPACE::InternalKeyComparator comparator;
};

// 一个分区的活跃/冻结索引。
class PartitionIndex {
 public:
  PartitionIndex(uint32_t part_id, uint32_t gen, const ZfKeyComparator& cmp)
      : part_id_(part_id), gen_(gen), cmp_(cmp), list_(cmp, &arena_) {}

  // 插入（并发安全）。internal_key 含 seq/type 尾；locator 为 16B SlimLocator。
  // 返回 false 表示重复条目（key+seq 已存在，理论上不发生——seq 唯一）。
  bool Insert(const ROCKSDB_NAMESPACE::Slice& internal_key,
              const ROCKSDB_NAMESPACE::Slice& locator) {
    const uint32_t ik_len = static_cast<uint32_t>(internal_key.size());
    const uint32_t loc_len = static_cast<uint32_t>(locator.size());
    const size_t total =
        ROCKSDB_NAMESPACE::VarintLength(ik_len) + ik_len +
        ROCKSDB_NAMESPACE::VarintLength(loc_len) + loc_len;
    // 必须经 AllocateKey 分配：InlineSkipList 在 key 前预留节点头
    // （随机高度等），直接 arena 分配会读到垃圾头导致断言失败。
    char* buf = list_.AllocateKey(total);
    char* p = ROCKSDB_NAMESPACE::EncodeVarint32(buf, ik_len);
    memcpy(p, internal_key.data(), ik_len);
    p += ik_len;
    p = ROCKSDB_NAMESPACE::EncodeVarint32(p, loc_len);
    memcpy(p, locator.data(), loc_len);
    mem_bytes_.fetch_add(total, std::memory_order_relaxed);
    return list_.InsertConcurrently(buf);
  }

  // Get：user_key 在 snapshot 下的最新版本。命中返回 true（含 tombstone，
  // type 由调用方判断）；未命中返回 false。
  bool Get(const ROCKSDB_NAMESPACE::Slice& user_key,
           ROCKSDB_NAMESPACE::SequenceNumber snapshot,
           ROCKSDB_NAMESPACE::Slice* locator_out,
           ROCKSDB_NAMESPACE::ValueType* type_out,
           ROCKSDB_NAMESPACE::SequenceNumber* seq_out) const {
    // 构造 (user_key, snapshot) 的 internal key 并 Seek：跳表按
    // (user_key asc, seq desc) 排序，Seek 到的第一个 ≥ 位置若 user_key
    // 匹配即最新 ≤ snapshot 版本（与原生 LookupKey 逻辑一致）。
    std::string target;
    target.reserve(user_key.size() + 8);
    target.append(user_key.data(), user_key.size());
    ROCKSDB_NAMESPACE::PutFixed64(
        &target, ROCKSDB_NAMESPACE::PackSequenceAndType(
                     snapshot, ROCKSDB_NAMESPACE::kTypeValue));
    // 编码为长度前缀格式（跳表条目格式）
    std::string encoded;
    ROCKSDB_NAMESPACE::PutVarint32(
        &encoded, static_cast<uint32_t>(target.size()));
    encoded.append(target);

    Iterator iter(&list_);
    iter.Seek(encoded.data());
    if (!iter.Valid()) {
      return false;
    }
    const char* entry = iter.key();
    const ROCKSDB_NAMESPACE::Slice ik =
        ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(entry);
    if (ik.size() < 8 ||
        user_key.compare(ROCKSDB_NAMESPACE::Slice(ik.data(), ik.size() - 8)) !=
            0) {
      return false;  // user_key 不匹配（Seek 越过目标或无此 key）
    }
    // 解码 seq/type 与 locator
    const uint64_t packed =
        ROCKSDB_NAMESPACE::DecodeFixed64(ik.data() + ik.size() - 8);
    *seq_out = packed >> 8;
    *type_out = static_cast<ROCKSDB_NAMESPACE::ValueType>(packed & 0xff);
    // 条目布局：[varint ik_size][ik][varint loc_size][loc]——
    // 前缀长度 = varint(ik_size) 的编码字节数。
    uint32_t ik_size = 0;
    const char* loc_pos = ROCKSDB_NAMESPACE::GetVarint32Ptr(
        entry, entry + 5, &ik_size);
    loc_pos += ik_size;
    // 条目布局：[varint ik_size][ik][varint loc_size][loc]
    // GetLengthPrefixedSliceSize(entry) = varint 前缀 + ik_size
    const ROCKSDB_NAMESPACE::Slice loc =
        ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(loc_pos);
    if (loc.size() != sizeof(SlimLocator)) {
      return false;  // 损坏条目
    }
    *locator_out = loc;
    return true;
  }

  uint64_t mem_bytes() const { return mem_bytes_.load(std::memory_order_relaxed); }
  uint32_t part_id() const { return part_id_; }
  uint32_t gen() const { return gen_; }
  bool frozen() const { return frozen_.load(std::memory_order_relaxed); }
  void SetFrozen() { frozen_.store(true, std::memory_order_relaxed); }

  using Iterator = ROCKSDB_NAMESPACE::InlineSkipList<ZfKeyComparator>::Iterator;

  // 条目解码（M4.3d Iterator 用）：从跳表条目取 internal key。
  static ROCKSDB_NAMESPACE::Slice DecodeInternalKey(const char* entry) {
    return ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(entry);
  }

 private:
  uint32_t part_id_;
  uint32_t gen_;  // freeze 时对应的 WAL 代（物化完成按 gen 释放）
  ZfKeyComparator cmp_;
  ROCKSDB_NAMESPACE::ConcurrentArena arena_;
  ROCKSDB_NAMESPACE::InlineSkipList<ZfKeyComparator> list_;
  std::atomic<uint64_t> mem_bytes_{0};
  std::atomic<bool> frozen_{false};
  friend class PartitionIndexIterator;  // M4.3d-3：迭代器访问跳表
};

class PartitionIndexIterator;  // M4.3d-3：前向声明（定义在文件尾）

// 全部分区索引集合（终态 L0 索引）。
class PartitionIndexSet {
 public:
  explicit PartitionIndexSet(const ZfKeyComparator& cmp) : cmp_(cmp) {}

  // 获取分区 p 的活跃索引（不存在则创建——写路径首次触达时）。
  std::shared_ptr<PartitionIndex> Active(uint32_t part_id) {
    {
      std::lock_guard<std::mutex> l(mu_);
      auto it = active_.find(part_id);
      if (it != active_.end()) {
        return it->second;
      }
    }
    auto idx = std::make_shared<PartitionIndex>(part_id, 0, cmp_);
    std::lock_guard<std::mutex> l(mu_);
    auto [it, inserted] = active_.emplace(part_id, idx);
    (void)inserted;
    return it->second;
  }

  // 插入（AddRecord 调用；写路径并发安全）。返回新增内存字节（0 = 重复）。
  uint64_t Insert(uint32_t part_id, const ROCKSDB_NAMESPACE::Slice& internal_key,
                  const ROCKSDB_NAMESPACE::Slice& locator) {
    auto idx = Active(part_id);
    const uint64_t before = idx->mem_bytes();
    if (!idx->Insert(internal_key, locator)) {
      return 0;
    }
    const uint64_t added = idx->mem_bytes() - before;
    total_mem_bytes_.fetch_add(added, std::memory_order_relaxed);
    return added;
  }

  // M4.3a：封存时冻结分区 p 的活跃索引（gen = 换代的 WAL 代）。
  // active → frozen 链头（新→旧）；新活跃索引接替（gen+1）。
  void Freeze(uint32_t part_id, uint32_t new_gen) {
    std::shared_ptr<PartitionIndex> old;
    {
      std::lock_guard<std::mutex> l(mu_);
      auto it = active_.find(part_id);
      if (it != active_.end()) {
        old = it->second;
        it->second = std::make_shared<PartitionIndex>(part_id, new_gen, cmp_);
      } else {
        active_[part_id] =
            std::make_shared<PartitionIndex>(part_id, new_gen, cmp_);
        return;
      }
    }
    if (old != nullptr && old->mem_bytes() > 0) {
      old->SetFrozen();
      std::lock_guard<std::mutex> l(mu_);
      frozen_[part_id].push_back(std::move(old));  // 新→旧（push_back = 链尾最旧）
    }
  }

  // M4.3a：物化完成（epoch 回收）时释放指定 (part, gen) 的 frozen 索引。
  void ReleaseFrozen(uint32_t part_id, uint32_t gen) {
    std::lock_guard<std::mutex> l(mu_);
    auto it = frozen_.find(part_id);
    if (it == frozen_.end()) {
      return;
    }
    auto& chain = it->second;
    for (auto c = chain.begin(); c != chain.end();) {
      if ((*c)->gen() == gen) {
        total_mem_bytes_.fetch_sub((*c)->mem_bytes(),
                                   std::memory_order_relaxed);
        c = chain.erase(c);
      } else {
        ++c;
      }
    }
  }

  // Get：查分区 p 的 active + frozen 链（新→旧，第一个命中即最新版本）。
  bool Get(uint32_t part_id, const ROCKSDB_NAMESPACE::Slice& user_key,
           ROCKSDB_NAMESPACE::SequenceNumber snapshot,
           ROCKSDB_NAMESPACE::Slice* locator_out,
           ROCKSDB_NAMESPACE::ValueType* type_out,
           ROCKSDB_NAMESPACE::SequenceNumber* seq_out) const {
    std::vector<std::shared_ptr<PartitionIndex>> chain;
    {
      std::lock_guard<std::mutex> l(mu_);
      auto fit = frozen_.find(part_id);
      if (fit != frozen_.end()) {
        chain = fit->second;  // 拷贝（shared_ptr 引用计数保护释放竞态）
      }
      auto ait = active_.find(part_id);
      if (ait != active_.end()) {
        chain.push_back(ait->second);
      }
    }
    // frozen 链：新→旧（vector 尾部最旧——push_back 语义）。
    // 需从"最新 frozen"到"最旧 frozen"再到 active 的顺序查——frozen 链
    // 存为 [最旧...最新]？Freeze 用 push_back → 链尾最新。Get 从链尾往前。
    for (auto c = chain.rbegin(); c != chain.rend(); ++c) {
      if ((*c)->Get(user_key, snapshot, locator_out, type_out, seq_out)) {
        return true;
      }
    }
    return false;
  }

  uint64_t total_mem_bytes() const {
    return total_mem_bytes_.load(std::memory_order_relaxed);
  }

  // 遍历全部 frozen 索引（M4.3c 分区 compact 输入侧用）。
  void ForEachFrozen(uint32_t part_id,
                     const std::function<void(const std::shared_ptr<PartitionIndex>&)>& fn) const {
    std::lock_guard<std::mutex> l(mu_);
    auto it = frozen_.find(part_id);
    if (it == frozen_.end()) {
      return;
    }
    for (const auto& idx : it->second) {
      fn(idx);
    }
  }

  // M4.3d-3：迭代器支持——遍历全部分区的 active + frozen 索引，
  // 为每个索引构造 PartitionIndexIterator 加入归并构建器。
  // value 解析（ReadValue）由迭代器按需执行（照抄 MemTableIterator zf 分支）。
  // 定义见文件尾（PartitionIndexIterator 之后）。
  void AddIterators(
      ROCKSDB_NAMESPACE::MergeIteratorBuilder* builder,
      const std::function<ROCKSDB_NAMESPACE::Status(const ROCKSDB_NAMESPACE::Slice&, std::string*)>& read_value,
      ROCKSDB_NAMESPACE::Arena* arena) const;

 private:
  ZfKeyComparator cmp_;
  mutable std::mutex mu_;  // 保护 map 与 frozen 链（写路径热路径不持锁：
                           // Active() 已用"先无锁读、有锁创建"降低竞争；
                           // Insert 对跳表本身无锁）
  std::unordered_map<uint32_t, std::shared_ptr<PartitionIndex>> active_;
  // frozen 链：每分区一个 vector，push_back 追加（链尾最新）。
  std::unordered_map<uint32_t, std::vector<std::shared_ptr<PartitionIndex>>>
      frozen_;
  std::atomic<uint64_t> total_mem_bytes_{0};
};

// M4.3d-3：分区索引的 InternalIterator（终态 L0 窗口的迭代器）。
// key = 条目内 internal key；value = 按 locator 定点读 WAL（read_value
// 回调，ZeroFlushContext::ReadValue 语义——与 MemTableIterator zf 分支一致）。
class PartitionIndexIterator : public ROCKSDB_NAMESPACE::InternalIterator {
 public:
  // 持 shared_ptr：AddIterators 返回后索引可能被 ReleaseFrozen 释放
  // （物化完成），迭代器必须延长索引生命周期（与 SuperVersion 语义一致）。
  PartitionIndexIterator(
      std::shared_ptr<const PartitionIndex> idx,
      const std::function<ROCKSDB_NAMESPACE::Status(const ROCKSDB_NAMESPACE::Slice&, std::string*)>& read_value)
      : idx_(std::move(idx)), iter_(&idx_->list_), read_value_(read_value) {}

  bool Valid() const override { return iter_.Valid(); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override {
    assert(Valid());
    iter_.Next();
  }
  void Prev() override {
    assert(Valid());
    iter_.Prev();
  }
  void Seek(const ROCKSDB_NAMESPACE::Slice& target) override {
    // target 是 internal key（8B seq/type 尾）——编码为长度前缀格式后 Seek。
    std::string encoded;
    ROCKSDB_NAMESPACE::PutVarint32(
        &encoded, static_cast<uint32_t>(target.size()));
    encoded.append(target.data(), target.size());
    iter_.Seek(encoded.data());
  }
  void SeekForPrev(const ROCKSDB_NAMESPACE::Slice& target) override {
    std::string encoded;
    ROCKSDB_NAMESPACE::PutVarint32(
        &encoded, static_cast<uint32_t>(target.size()));
    encoded.append(target.data(), target.size());
    iter_.SeekForPrev(encoded.data());
  }
  ROCKSDB_NAMESPACE::Slice key() const override {
    assert(Valid());
    return ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(iter_.key());
  }
  ROCKSDB_NAMESPACE::Slice value() const override {
    assert(Valid());
    const char* entry = iter_.key();
    uint32_t ik_size = 0;
    const char* loc_pos = ROCKSDB_NAMESPACE::GetVarint32Ptr(
        entry, entry + 5, &ik_size);
    loc_pos += ik_size;
    const ROCKSDB_NAMESPACE::Slice loc =
        ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(loc_pos);
    value_buf_.clear();
    if (!read_value_(loc, &value_buf_).ok()) {
      value_buf_.clear();  // 定点读失败视为损坏：返回空 value
    }
    return ROCKSDB_NAMESPACE::Slice(value_buf_);
  }
  ROCKSDB_NAMESPACE::Status status() const override {
    return ROCKSDB_NAMESPACE::Status::OK();
  }
  bool IsKeyPinned() const override { return true; }
  bool IsValuePinned() const override { return false; }

 private:
  std::shared_ptr<const PartitionIndex> idx_;
  ROCKSDB_NAMESPACE::InlineSkipList<ZfKeyComparator>::Iterator iter_;
  // 按值持有（lambda 生命周期归迭代器——引用会悬垂导致崩溃）
  std::function<ROCKSDB_NAMESPACE::Status(const ROCKSDB_NAMESPACE::Slice&, std::string*)> read_value_;
  mutable std::string value_buf_;
};

inline void PartitionIndexSet::AddIterators(
    ROCKSDB_NAMESPACE::MergeIteratorBuilder* builder,
    const std::function<ROCKSDB_NAMESPACE::Status(const ROCKSDB_NAMESPACE::Slice&, std::string*)>& read_value,
    ROCKSDB_NAMESPACE::Arena* arena) const {
  // 收集全部索引（active + frozen 链，全部分区）——拷贝 shared_ptr 保护
  // 释放竞态。
  std::vector<std::shared_ptr<PartitionIndex>> all;
  {
    std::lock_guard<std::mutex> l(mu_);
    for (const auto& [part, chain] : frozen_) {
      for (const auto& idx : chain) {
        all.push_back(idx);
      }
    }
    for (const auto& [part, idx] : active_) {
      all.push_back(idx);
    }
  }
  for (const auto& idx : all) {
    if (arena != nullptr) {
      void* mem = arena->AllocateAligned(sizeof(PartitionIndexIterator));
      builder->AddIterator(new (mem) PartitionIndexIterator(idx, read_value));
    } else {
      builder->AddIterator(new PartitionIndexIterator(idx, read_value));
    }
  }
}

}  // namespace zeroflush
