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
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "db/dbformat.h"
#include "memtable/inlineskiplist.h"
#include "memory/concurrent_arena.h"
#include "rocksdb/memtablerep.h"
#include "table/internal_iterator.h"
#include "util/hash.h"
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
              const ROCKSDB_NAMESPACE::Slice& locator,
              uint64_t* bytes_added = nullptr) {
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
    // R57：成功后计数（失败时节点未入表，计 total 会虚增 mem_bytes）。
    if (!list_.InsertConcurrently(buf)) {
      return false;
    }
    mem_bytes_.fetch_add(total, std::memory_order_relaxed);
    if (bytes_added != nullptr) {
      *bytes_added = total;
    }
    return true;
  }

  // M4.11c：遍历全部条目的 internal key（布隆重建用）。跳表条目格式：
  // varint ik_len + ik + varint loc_len + loc。并发插入下迭代器可能漏
  // 正在插入的节点（概率低）——重建后新插入由 Insert 的 Add 补上。
  void ForEachKey(const std::function<void(const ROCKSDB_NAMESPACE::Slice&)>& fn) const {
    Iterator iter(&list_);
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
      const char* entry = iter.key();
      uint32_t ik_len = 0;
      const char* p = ROCKSDB_NAMESPACE::GetVarint32Ptr(entry, entry + 5, &ik_len);
      if (p == nullptr || ik_len == 0) {
        continue;
      }
      fn(ROCKSDB_NAMESPACE::Slice(p, ik_len));
    }
  }

  // M3.4：range tombstone 覆盖——找 ≤ user_key 的最大 begin 条目。
  // end 从 WAL 读（read_value 回调——条目的 value 字段是 16B locator，
  // 不是 end；end 是 AddRecord 的 value 存于 WAL）。
  bool GetRangeDelCover(
      const ROCKSDB_NAMESPACE::Slice& user_key,
      ROCKSDB_NAMESPACE::SequenceNumber snapshot,
      const std::function<ROCKSDB_NAMESPACE::Status(const ROCKSDB_NAMESPACE::Slice&, std::string*)>& read_value) const {
    std::string target;
    target.reserve(user_key.size() + 8);
    target.append(user_key.data(), user_key.size());
    ROCKSDB_NAMESPACE::PutFixed64(
        &target, ROCKSDB_NAMESPACE::PackSequenceAndType(
                     snapshot, ROCKSDB_NAMESPACE::kTypeValue));
    std::string encoded;
    ROCKSDB_NAMESPACE::PutVarint32(
        &encoded, static_cast<uint32_t>(target.size()));
    encoded.append(target);
    Iterator iter(&list_);
    iter.SeekForPrev(encoded.data());
    if (!iter.Valid()) {
      return false;
    }
    const char* entry = iter.key();
    const ROCKSDB_NAMESPACE::Slice ik =
        ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(entry);
    if (ik.size() < 8) {
      return false;
    }
    const ROCKSDB_NAMESPACE::Slice ukey(ik.data(), ik.size() - 8);
    if (cmp_.comparator.user_comparator()->Compare(ukey, user_key) > 0) {
      return false;
    }
    const uint64_t packed =
        ROCKSDB_NAMESPACE::DecodeFixed64(ik.data() + ik.size() - 8);
    const auto type = static_cast<ROCKSDB_NAMESPACE::ValueType>(packed & 0xff);
    if (type != ROCKSDB_NAMESPACE::kTypeRangeDeletion) {
      return false;
    }
    uint32_t ik_size = 0;
    const char* loc_pos = ROCKSDB_NAMESPACE::GetVarint32Ptr(
        entry, entry + 5, &ik_size);
    loc_pos += ik_size;
    const ROCKSDB_NAMESPACE::Slice loc =
        ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(loc_pos);
    std::string end_buf;
    if (!read_value(loc, &end_buf).ok()) {
      return false;
    }
    const ROCKSDB_NAMESPACE::Slice end(end_buf);
    return cmp_.comparator.user_comparator()->Compare(user_key, end) < 0;
  }

  // M3.4：收集同 user_key 在 snapshot 下的全部版本（seq 降序）——
  // merge 链需要 base + 全部 operand。
  void CollectVersions(
      const ROCKSDB_NAMESPACE::Slice& user_key,
      ROCKSDB_NAMESPACE::SequenceNumber snapshot,
      const std::function<ROCKSDB_NAMESPACE::Status(const ROCKSDB_NAMESPACE::Slice&, std::string*)>& read_value,
      std::vector<std::string>* values,
      std::vector<ROCKSDB_NAMESPACE::ValueType>* types) const {
    std::string target;
    target.reserve(user_key.size() + 8);
    target.append(user_key.data(), user_key.size());
    ROCKSDB_NAMESPACE::PutFixed64(
        &target, ROCKSDB_NAMESPACE::PackSequenceAndType(
                     snapshot, ROCKSDB_NAMESPACE::kTypeValue));
    std::string encoded;
    ROCKSDB_NAMESPACE::PutVarint32(
        &encoded, static_cast<uint32_t>(target.size()));
    encoded.append(target);
    Iterator iter(&list_);
    iter.Seek(encoded.data());
    const auto* ucmp = cmp_.comparator.user_comparator();
    while (iter.Valid()) {
      const char* entry = iter.key();
      const ROCKSDB_NAMESPACE::Slice ik =
          ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(entry);
      if (ik.size() < 8) {
        break;
      }
      const ROCKSDB_NAMESPACE::Slice ukey(ik.data(), ik.size() - 8);
      if (ucmp->Compare(ukey, user_key) != 0) {
        break;  // 越过该 user_key
      }
      const uint64_t packed =
          ROCKSDB_NAMESPACE::DecodeFixed64(ik.data() + ik.size() - 8);
      const auto type = static_cast<ROCKSDB_NAMESPACE::ValueType>(packed & 0xff);
      uint32_t ik_size = 0;
      const char* loc_pos = ROCKSDB_NAMESPACE::GetVarint32Ptr(
          entry, entry + 5, &ik_size);
      loc_pos += ik_size;
      const ROCKSDB_NAMESPACE::Slice loc =
          ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(loc_pos);
      std::string val;
      if (read_value(loc, &val).ok()) {
        values->push_back(std::move(val));
        types->push_back(type);
      }
      iter.Next();
    }
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

// M4.11c：分区布隆过滤器——GetFromPartitionIndex 前的快速预过滤。
// 数据 97%+ 已物化进 SST（原生查找承载），未物化（活跃段 + frozen）的
// key 才在分区索引中；布隆 miss（~50-100ns）直接跳过跳表查询（~1.2us，
// cache miss 主导）。布隆只增不减（frozen 释放后假阳性上升——多走一次
// 跳表 miss，正确性不变）。位集为原子（写路径并发插入，fetch_or 防
// 丢失更新导致假阴性漏读）。
struct PartitionBloom {
  static constexpr uint64_t kNumBits = 8ull << 20;  // 1MB/分区 ≈ 100 万条容量
  std::vector<std::atomic<uint64_t>> bits_;
  PartitionBloom() : bits_(kNumBits / 64) {
    for (auto& b : bits_) {
      b.store(0, std::memory_order_relaxed);
    }
  }

  void Add(const ROCKSDB_NAMESPACE::Slice& key) {
    const uint32_t h1 = ROCKSDB_NAMESPACE::Hash(key.data(), key.size(), 0x5f10f1);
    const uint32_t h2 = ROCKSDB_NAMESPACE::Hash(key.data(), key.size(), 0x5f10f2);
    for (uint32_t i = 0; i < 4; ++i) {
      const uint64_t bit = (static_cast<uint64_t>(h1) + i * h2) % kNumBits;
      bits_[bit / 64].fetch_or(1ull << (bit % 64), std::memory_order_relaxed);
    }
  }

  bool MayContain(const ROCKSDB_NAMESPACE::Slice& key) const {
    const uint32_t h1 = ROCKSDB_NAMESPACE::Hash(key.data(), key.size(), 0x5f10f1);
    const uint32_t h2 = ROCKSDB_NAMESPACE::Hash(key.data(), key.size(), 0x5f10f2);
    for (uint32_t i = 0; i < 4; ++i) {
      const uint64_t bit = (static_cast<uint64_t>(h1) + i * h2) % kNumBits;
      if ((bits_[bit / 64].load(std::memory_order_relaxed) &
           (1ull << (bit % 64))) == 0) {
        return false;
      }
    }
    return true;
  }
};

// 全部分区索引集合（终态 L0 索引）。
class PartitionIndexSet {
 public:
  explicit PartitionIndexSet(const ZfKeyComparator& cmp) : cmp_(cmp) {}

  // 获取分区 p 的活跃索引（不存在则创建——写路径首次触达时）。
  // M4.3 修复：全程持锁——无锁 map 读与并发 emplace/Insert 竞争是 UB
  // （unordered_map 结构变更），曾导致段错误。跳表操作在锁外。
  std::shared_ptr<PartitionIndex> Active(uint32_t part_id) {
    std::unique_lock<std::shared_mutex> l(mu_);
    auto it = active_.find(part_id);
    if (it != active_.end()) {
      return it->second;
    }
    auto idx = std::make_shared<PartitionIndex>(part_id, 0, cmp_);
    active_.emplace(part_id, idx);
    blooms_.emplace(part_id, PartitionBloom());
    return idx;
  }

  // 插入（AddRecord 调用；写路径并发安全）。返回新增内存字节（0 = 重复）。
  // M4.3 修复：必须按 locator 的 gen 选索引——WAL Append（拿 gen）与索引
  // Insert 之间 freeze 可能发生（Active 换代），现取 Active 会把旧 gen 的
  // 条目插进新 gen 索引，物化后该条目丢失（locator 指向已回收 WAL）。
  uint64_t Insert(uint32_t part_id, uint32_t gen,
                  const ROCKSDB_NAMESPACE::Slice& internal_key,
                  const ROCKSDB_NAMESPACE::Slice& locator) {
    std::shared_ptr<PartitionIndex> idx;
    {
      auto it = active_.find(part_id);
      if (it != active_.end() && it->second->gen() == gen) {
        idx = it->second;
      } else if (it == active_.end()) {
        // 首次触达：双检锁内创建——无锁 emplace 在 16 写线程并发首触达
        // 不同分区时是 unordered_map 数据竞争（rehash 损坏，R41/gdb 栈
        // _M_find_before_node 读垃圾指针）。
        std::unique_lock<std::shared_mutex> l(mu_);
        it = active_.find(part_id);
        if (it != active_.end()) {
          idx = it->second;
        } else {
          auto ni = std::make_shared<PartitionIndex>(part_id, gen, cmp_);
          active_.emplace(part_id, ni);
          blooms_.emplace(part_id, PartitionBloom());
          idx = ni;
        }
      } else {
        // active 存在但 gen 不匹配（freeze 后写入旧代）：frozen 链找。
        auto fit = frozen_.find(part_id);
        if (fit != frozen_.end()) {
          // frozen 链：push_back 追加（链尾最新）——倒序找 gen 匹配。
          for (auto c = fit->second.rbegin(); c != fit->second.rend(); ++c) {
            if ((*c)->gen() == gen) {
              idx = *c;
              break;
            }
          }
        }
      }
    }
    if (idx == nullptr) {
      // gen 已物化并释放（数据已进 SST）——索引插入丢弃，数据由 SST 承载。
      return 0;
    }
    // M4.3 性能修复：跳表插入在锁外（InlineSkipList 并发插入）——锁内
    // 插入使全局 mu_ 串行化写路径（实测 105K → 11K）。freeze 竞态（锁内
    // mem_bytes 检查可能看到未更新的旧值而丢弃空索引）的后果是条目进
    // "游离索引"——数据仍在 WAL/物化 SST，Get 读游离条目失败时回退
    // SST（M4.3 回退逻辑），正确性保持。
    // R57：用 Insert 输出的精确增量——"after - before" 在并发插入下
    // 重复计数（两线程同读 before，各加一次 total，后算者得 2×total），
    // 计数器虚增 ~11B/op → 350M ops 时虚增 4GB 触发 index_mem_budget
    // 背压 → 封存正反馈爆发（R57）。实际内存始终有界（Dump 实测
    // 计数器 800MB vs 实际 109MB）。
    uint64_t added = 0;
    if (!idx->Insert(internal_key, locator, &added)) {
      return 0;
    }
    blooms_[part_id].Add(ROCKSDB_NAMESPACE::ExtractUserKey(internal_key));
    total_mem_bytes_.fetch_add(added, std::memory_order_relaxed);
    return added;
  }

  // M4.3a：封存时冻结分区 p 的活跃索引（gen = 换代的 WAL 代）。
  // active → frozen 链头（新→旧）；新活跃索引接替（gen+1）。
  void Freeze(uint32_t part_id, uint32_t new_gen) {
    std::shared_ptr<PartitionIndex> old;
    {
      std::unique_lock<std::shared_mutex> l(mu_);
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
      std::unique_lock<std::shared_mutex> l(mu_);
      frozen_[part_id].push_back(std::move(old));  // 新→旧（push_back = 链尾最旧）
    }
  }

  // M4.3a：物化完成（epoch 回收）时释放指定 (part, gen) 的 frozen 索引。
  void ReleaseFrozen(uint32_t part_id, uint32_t gen) {
    bool removed = false;
    {
      std::unique_lock<std::shared_mutex> l(mu_);
      auto it = frozen_.find(part_id);
      if (it != frozen_.end()) {
        auto& chain = it->second;
        for (auto c = chain.begin(); c != chain.end();) {
          if ((*c)->gen() == gen) {
            total_mem_bytes_.fetch_sub((*c)->mem_bytes(),
                                       std::memory_order_relaxed);
            c = chain.erase(c);
            removed = true;
          } else {
            ++c;
          }
        }
      }
    }
    if (removed) {
      // M4.11c：释放后重建布隆（去掉已物化代的 key，防位集饱和）。
      // 锁外调用（RebuildBloom 内部取锁）。
      RebuildBloom(part_id);
    }
  }

  // M4.11c：frozen 释放后重建分区布隆——只增不减会使位集被历史 key 饱和
  // （假阳性 100% → 预过滤失效）。遍历剩余 active + frozen 重建。
  // 持锁收集链（shared_ptr 拷贝）→ 无锁遍历（跳表迭代与并发插入共存，
  // 重建期间插入的 key 由 Insert 的 Add 补上——Add 在重建替换布隆后
  // 执行时生效；替换前执行的 Add 落在旧布隆（丢弃）→ 极小概率漏 key，
  // 保守场景：重建在物化完成时（写路径窗口小），记录为已知竞态）。
  void RebuildBloom(uint32_t part_id) {
    std::vector<std::shared_ptr<PartitionIndex>> chain;
    {
      std::shared_lock<std::shared_mutex> l(mu_);
      auto ait = active_.find(part_id);
      if (ait != active_.end()) {
        chain.push_back(ait->second);
      }
      auto fit = frozen_.find(part_id);
      if (fit != frozen_.end()) {
        for (const auto& idx : fit->second) {
          chain.push_back(idx);
        }
      }
    }
    PartitionBloom nb;
    for (const auto& idx : chain) {
      idx->ForEachKey([&nb](const ROCKSDB_NAMESPACE::Slice& ik) {
        nb.Add(ROCKSDB_NAMESPACE::ExtractUserKey(ik));
      });
    }
    std::unique_lock<std::shared_mutex> l(mu_);
    blooms_[part_id] = std::move(nb);
  }

  // M4.8：确保 active 索引存在且 gen 匹配（Recover 预创建用——防止
  // InsertCreate 把 min_seq 最小的封存代误建为 active，导致 Get 链中
  // active（旧代）优先遮蔽新代（R48 phase3 读旧值 'C'））。
  void EnsureActive(uint32_t part_id, uint32_t gen) {
    std::unique_lock<std::shared_mutex> l(mu_);
    auto it = active_.find(part_id);
    if (it != active_.end()) {
      if (it->second->gen() != gen) {
        it->second = std::make_shared<PartitionIndex>(part_id, gen, cmp_);
      }
      return;
    }
    active_.emplace(part_id,
                    std::make_shared<PartitionIndex>(part_id, gen, cmp_));
    blooms_.emplace(part_id, PartitionBloom());
  }

  // M4.3：Recover 专用插入——找不到 gen 匹配索引时创建（重开时所有 WAL
  // 代都是活的：活跃代建 active，封存代建 frozen）。写路径的 Insert 保持
  // "找不到即丢弃"（物化后迟到的插入由 SST 承载）。
  uint64_t InsertCreate(uint32_t part_id, uint32_t gen,
                        const ROCKSDB_NAMESPACE::Slice& internal_key,
                        const ROCKSDB_NAMESPACE::Slice& locator) {
    std::shared_ptr<PartitionIndex> idx;
    {
      std::unique_lock<std::shared_mutex> l(mu_);
      auto it = active_.find(part_id);
      if (it != active_.end() && it->second->gen() == gen) {
        idx = it->second;
      } else if (it == active_.end()) {
        auto ni = std::make_shared<PartitionIndex>(part_id, gen, cmp_);
        active_.emplace(part_id, ni);
        blooms_.emplace(part_id, PartitionBloom());
        idx = ni;
      } else {
        auto fit = frozen_.find(part_id);
        bool found = false;
        if (fit != frozen_.end()) {
          for (auto c = fit->second.rbegin(); c != fit->second.rend(); ++c) {
            if ((*c)->gen() == gen) {
              idx = *c;
              found = true;
              break;
            }
          }
        }
        if (!found) {
          // 封存代索引不存在：创建并加入 frozen 链（Recover 重建场景）。
          auto ni = std::make_shared<PartitionIndex>(part_id, gen, cmp_);
          ni->SetFrozen();
          frozen_[part_id].push_back(ni);
          idx = ni;
        }
      }
    }
    uint64_t added = 0;
    if (!idx->Insert(internal_key, locator, &added)) {
      return 0;
    }
    // M4.11c：布隆更新（Recover 重建路径——漏加会导致重开后活跃段数据
    // 被布隆预过滤误判 miss → 读不到）。
    blooms_[part_id].Add(ROCKSDB_NAMESPACE::ExtractUserKey(internal_key));
    // R57：精确增量（同上——并发读算重复计数）。
    total_mem_bytes_.fetch_add(added, std::memory_order_relaxed);
    return added;
  }


  // M3.4：range tombstone 覆盖（专用分区链，新→旧）。
  bool GetRangeDelCover(
      uint32_t part_id, const ROCKSDB_NAMESPACE::Slice& user_key,
      ROCKSDB_NAMESPACE::SequenceNumber snapshot,
      const std::function<ROCKSDB_NAMESPACE::Status(const ROCKSDB_NAMESPACE::Slice&, std::string*)>& read_value) const {
    std::vector<std::shared_ptr<PartitionIndex>> chain;
    {
      std::shared_lock<std::shared_mutex> l(mu_);
      auto fit = frozen_.find(part_id);
      if (fit != frozen_.end()) {
        chain = fit->second;
      }
      auto ait = active_.find(part_id);
      if (ait != active_.end()) {
        chain.push_back(ait->second);
      }
    }
    for (auto c = chain.rbegin(); c != chain.rend(); ++c) {
      if ((*c)->GetRangeDelCover(user_key, snapshot, read_value)) {
        return true;
      }
    }
    return false;
  }

  // M3.4：收集同 key 全部版本（seq 降序：active 先 + frozen 新→旧）。
  // 用于 merge 链（base + operands 的完整累积）。
  void CollectVersions(
      uint32_t part_id, const ROCKSDB_NAMESPACE::Slice& user_key,
      ROCKSDB_NAMESPACE::SequenceNumber snapshot,
      const std::function<ROCKSDB_NAMESPACE::Status(const ROCKSDB_NAMESPACE::Slice&, std::string*)>& read_value,
      std::vector<std::string>* values,
      std::vector<ROCKSDB_NAMESPACE::ValueType>* types) const {
    std::vector<std::shared_ptr<PartitionIndex>> chain;
    {
      std::unique_lock<std::shared_mutex> l(mu_);
      auto ait = active_.find(part_id);
      if (ait != active_.end()) {
        chain.push_back(ait->second);  // active 最先（最新 seq）
      }
      auto fit = frozen_.find(part_id);
      if (fit != frozen_.end()) {
        for (auto c = fit->second.rbegin(); c != fit->second.rend(); ++c) {
          chain.push_back(*c);  // frozen 新→旧
        }
      }
    }
    for (const auto& idx : chain) {
      idx->CollectVersions(user_key, snapshot, read_value, values, types);
    }
  }

  // Get：查分区 p 的 active + frozen 链（新→旧，第一个命中即最新版本）。
  // M4.11c：布隆预过滤——key 不在本分区未物化索引 → 直接 miss（数据在
  // SST，由原生查找承载；省跳表查询 ~1.2us/op）。
  bool Get(uint32_t part_id, const ROCKSDB_NAMESPACE::Slice& user_key,
           ROCKSDB_NAMESPACE::SequenceNumber snapshot,
           ROCKSDB_NAMESPACE::Slice* locator_out,
           ROCKSDB_NAMESPACE::ValueType* type_out,
           ROCKSDB_NAMESPACE::SequenceNumber* seq_out) const {
    {
      std::shared_lock<std::shared_mutex> l(mu_);
      auto bit = blooms_.find(part_id);
      if (bit == blooms_.end() || !bit->second.MayContain(user_key)) {
        return false;
      }
    }
    std::vector<std::shared_ptr<PartitionIndex>> chain;
    {
      std::shared_lock<std::shared_mutex> l(mu_);
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

  // R57 诊断：dump 每分区 active/frozen 索引字节与 gen（定位索引泄漏）。
  void DumpState() {
    std::shared_lock<std::shared_mutex> l(mu_);
    uint64_t act = 0, frz = 0;
    for (const auto& kv : active_) {
      act += kv.second->mem_bytes();
    }
    for (const auto& kv : frozen_) {
      for (const auto& idx : kv.second) {
        frz += idx->mem_bytes();
      }
    }
    fprintf(stderr, "ZFDBG-imem active_total=%llu frozen_total=%llu\n",
            (unsigned long long)act, (unsigned long long)frz);
    for (const auto& kv : frozen_) {
      fprintf(stderr, "ZFDBG-imem part=%u frozen=[", kv.first);
      for (const auto& idx : kv.second) {
        fprintf(stderr, "g%u:%llu,", idx->gen(), (unsigned long long)idx->mem_bytes());
      }
      fprintf(stderr, "]\n");
    }
  }


  // 遍历全部 frozen 索引（M4.3c 分区 compact 输入侧用）。
  void ForEachFrozen(uint32_t part_id,
                     const std::function<void(const std::shared_ptr<PartitionIndex>&)>& fn) const {
    std::shared_lock<std::shared_mutex> l(mu_);
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
  mutable std::shared_mutex mu_;  // 保护 map 与 frozen 链（写路径热路径不持锁：
                           // Active() 已用"先无锁读、有锁创建"降低竞争；
                           // Insert 对跳表本身无锁）
  std::unordered_map<uint32_t, std::shared_ptr<PartitionIndex>> active_;
  // frozen 链：每分区一个 vector，push_back 追加（链尾最新）。
  std::unordered_map<uint32_t, std::vector<std::shared_ptr<PartitionIndex>>>
      frozen_;
  // M4.11c：每分区布隆（懒创建，与 active 索引同生命周期；mu_ 保护）。
  std::unordered_map<uint32_t, PartitionBloom> blooms_;
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
    std::shared_lock<std::shared_mutex> l(mu_);
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
    // 堆分配（非 arena）：PartitionIndexIterator 含 std::string/std::function，
    // arena 迭代器不析构会泄漏且内存语义与归并迭代器 delete 约定冲突；
    // 迭代器按次创建，堆开销可忽略。
    builder->AddIterator(new PartitionIndexIterator(idx, read_value));
  }
}

}  // namespace zeroflush
