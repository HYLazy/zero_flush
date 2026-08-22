//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M1: Slim MemTable 表示实现（InlineSkipList 后端，参考
//  memtable/skiplistrep.cc 的结构，仅保留 M1 所需接口）。

#include "zeroflush/slim_memtable.h"

#include <cassert>

#include "memory/arena.h"
#include "rocksdb/memtablerep.h"
#include "util/coding.h"

namespace zeroflush {

namespace {
using ROCKSDB_NAMESPACE::Allocator;
using ROCKSDB_NAMESPACE::Arena;
using ROCKSDB_NAMESPACE::KeyHandle;
using ROCKSDB_NAMESPACE::MemTableRep;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::SliceTransform;

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  ROCKSDB_NAMESPACE::PutVarint32(scratch,
                                 static_cast<uint32_t>(target.size()));
  scratch->append(target.data(), target.size());
  return scratch->data();
}
}  // namespace

SlimMemTableRep::SlimMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* transform)
    : MemTableRep(allocator),
      skip_list_(compare, allocator),
      cmp_(compare),
      transform_(transform) {}

KeyHandle SlimMemTableRep::Allocate(const size_t len, char** buf) {
  *buf = skip_list_.AllocateKey(len);
  // M3.0：累计条目有效载荷字节（key + locator），供 ApproximateMemoryUsage
  // 返回真实用量。只增不减：MemTable 析构时整体释放，无需精确回退。
  approx_mem_.fetch_add(len, std::memory_order_relaxed);
  return static_cast<KeyHandle>(*buf);
}

void SlimMemTableRep::Insert(KeyHandle handle) {
  skip_list_.Insert(static_cast<char*>(handle));
}

bool SlimMemTableRep::InsertKey(KeyHandle handle) {
  return skip_list_.Insert(static_cast<char*>(handle));
}

void SlimMemTableRep::InsertConcurrently(KeyHandle handle) {
  skip_list_.InsertConcurrently(static_cast<char*>(handle));
}

bool SlimMemTableRep::InsertKeyConcurrently(KeyHandle handle) {
  return skip_list_.InsertConcurrently(static_cast<char*>(handle));
}

bool SlimMemTableRep::Contains(const char* key) const {
  return skip_list_.Contains(key);
}

size_t SlimMemTableRep::ApproximateMemoryUsage() {
  // M3.0：返回条目有效载荷累计字节（Allocate 时统计）。节点指针与
  // arena 块由 MemTable 的 arena 项统一记账，此处只补条目本身。
  return approx_mem_.load(std::memory_order_relaxed);
}

MemTableRep::Iterator* SlimMemTableRep::GetIterator(Arena* arena) {
  if (arena != nullptr) {
    void* mem = arena->AllocateAligned(sizeof(Iterator));
    return new (mem) Iterator(&skip_list_);
  }
  return new Iterator(&skip_list_);
}

SlimMemTableRep::Iterator::Iterator(
    const ROCKSDB_NAMESPACE::InlineSkipList<const MemTableRep::KeyComparator&>* list)
    : iter_(list) {}

bool SlimMemTableRep::Iterator::Valid() const { return iter_.Valid(); }

const char* SlimMemTableRep::Iterator::key() const {
  assert(Valid());
  return iter_.key();
}

void SlimMemTableRep::Iterator::Next() {
  assert(Valid());
  iter_.Next();
}

void SlimMemTableRep::Iterator::Prev() {
  assert(Valid());
  iter_.Prev();
}

void SlimMemTableRep::Iterator::Seek(const Slice& user_key,
                                     const char* memtable_key) {
  if (memtable_key != nullptr) {
    iter_.Seek(memtable_key);
  } else {
    iter_.Seek(EncodeKey(&tmp_, user_key));
  }
}

void SlimMemTableRep::Iterator::SeekForPrev(const Slice& user_key,
                                            const char* memtable_key) {
  if (memtable_key != nullptr) {
    iter_.SeekForPrev(memtable_key);
  } else {
    iter_.SeekForPrev(EncodeKey(&tmp_, user_key));
  }
}

void SlimMemTableRep::Iterator::SeekToFirst() { iter_.SeekToFirst(); }

void SlimMemTableRep::Iterator::SeekToLast() { iter_.SeekToLast(); }

}  // namespace zeroflush
