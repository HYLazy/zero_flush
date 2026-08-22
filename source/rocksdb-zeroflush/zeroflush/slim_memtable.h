//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M1: Slim MemTable 表示（索引-only）。
//
// 对应设计文档 §2.2 / §4.2：MemTable 只保存
// [varint ik_size][internal key][varint loc_size=16][SlimLocator 16B]，
// value 的唯一持久副本位于分区 WAL，由 SlimLocator 定点引用。
//
// 与原生 SkipListRep 的关系：底层同为 InlineSkipList，key 排序语义完全一致
// （MemTable 外壳的 comparator/seq 过滤/snapshot 机制原样复用）；差异仅在
// 条目体积（key+16B 而非 key+value）与 value 的解析位置（WAL 而非 arena）。
// value 解析由 MemTable::Get（SaveValue）与 MemTableIterator 按 zf 分支完成，
// 本 rep 不感知 value 语义 —— 因此 M1 的 rep 是 SkipListRep 的等价实现，
// 独立命名以表达"索引-only"契约，并为 M2 per-partition skiplist 留接口。

#pragma once

#include <atomic>
#include <memory>

#include "memtable/inlineskiplist.h"
#include "rocksdb/memtablerep.h"

namespace zeroflush {

// M1 实现与 SkipListRep 相同（InlineSkipList 后端）。
// M4.1b：启用并发插入（IsInsertConcurrentlySupported() = true）——写组
// leader 在 DB mutex 外执行 WAL 追加 + 索引插入，多个写组可并发插入同一
// memtable；InlineSkipList 支持并发插入 + 并发读（物化/flush 读 imm 期间
// 仍可能有写组向该 mem 补插，见 M4_DESIGN.md §M4.1）。
class SlimMemTableRep : public ROCKSDB_NAMESPACE::MemTableRep {
 public:
  SlimMemTableRep(const ROCKSDB_NAMESPACE::MemTableRep::KeyComparator& compare,
                  ROCKSDB_NAMESPACE::Allocator* allocator,
                  const ROCKSDB_NAMESPACE::SliceTransform* transform);

  ROCKSDB_NAMESPACE::KeyHandle Allocate(const size_t len,
                                        char** buf) override;
  void Insert(ROCKSDB_NAMESPACE::KeyHandle handle) override;
  bool InsertKey(ROCKSDB_NAMESPACE::KeyHandle handle) override;
  void InsertConcurrently(ROCKSDB_NAMESPACE::KeyHandle handle) override;
  bool InsertKeyConcurrently(ROCKSDB_NAMESPACE::KeyHandle handle) override;
  bool Contains(const char* key) const override;
  size_t ApproximateMemoryUsage() override;
  ROCKSDB_NAMESPACE::MemTableRep::Iterator* GetIterator(
      ROCKSDB_NAMESPACE::Arena* arena = nullptr) override;
  ~SlimMemTableRep() override = default;

  // 迭代器：与 SkipListRep::Iterator 相同（value 解析在 MemTableIterator
  // 层按 zf 分支完成，本迭代器只负责 key 有序遍历）。
  class Iterator : public ROCKSDB_NAMESPACE::MemTableRep::Iterator {
   public:
    explicit Iterator(
        const ROCKSDB_NAMESPACE::InlineSkipList<const ROCKSDB_NAMESPACE::MemTableRep::KeyComparator&>*
            list);
    ~Iterator() override = default;
    bool Valid() const override;
    const char* key() const override;
    void Next() override;
    void Prev() override;
    void Seek(const ROCKSDB_NAMESPACE::Slice& internal_key,
              const char* memtable_key) override;
    void SeekForPrev(const ROCKSDB_NAMESPACE::Slice& internal_key,
                     const char* memtable_key) override;
    void SeekToFirst() override;
    void SeekToLast() override;

   private:
    ROCKSDB_NAMESPACE::InlineSkipList<
        const ROCKSDB_NAMESPACE::MemTableRep::KeyComparator&>::Iterator iter_;
    std::string tmp_;  // EncodeKey 用 scratch
  };

 private:
  ROCKSDB_NAMESPACE::InlineSkipList<const ROCKSDB_NAMESPACE::MemTableRep::KeyComparator&>
      skip_list_;
  const ROCKSDB_NAMESPACE::MemTableRep::KeyComparator& cmp_;
  const ROCKSDB_NAMESPACE::SliceTransform* transform_;
  // M3.0：累计条目有效载荷字节（Allocate 时累加 len）。节点指针与
  // arena 块由 MemTable::ApproximateMemoryUsage 的 arena 项记账，这里
  // 补上条目本身 —— 二者相加即真实内存用量。
  std::atomic<size_t> approx_mem_{0};
};

// 工厂：通过 opt.memtable_factory 注入（zeroflush::Open 中设置）。
// M1 无状态（rep 不持有上下文，value 解析在 MemTable 层按 zf_ctx 分支）。
class SlimMemTableRepFactory : public ROCKSDB_NAMESPACE::MemTableRepFactory {
 public:
  static const char* kClassName() { return "SlimMemTableRepFactory"; }
  static const char* kNickName() { return "slim_memtable"; }

  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

  using MemTableRepFactory::CreateMemTableRep;
  ROCKSDB_NAMESPACE::MemTableRep* CreateMemTableRep(
      const ROCKSDB_NAMESPACE::MemTableRep::KeyComparator& compare,
      ROCKSDB_NAMESPACE::Allocator* allocator,
      const ROCKSDB_NAMESPACE::SliceTransform* slice_transform,
      ROCKSDB_NAMESPACE::Logger* /*logger*/) override {
    return new SlimMemTableRep(compare, allocator, slice_transform);
  }

  // M4.1b：并发插入由写组 leader 在 DB mutex 外执行（多组并发）。
  bool IsInsertConcurrentlySupported() const override { return true; }
};

}  // namespace zeroflush
