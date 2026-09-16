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

#include <algorithm>
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
#include "zeroflush/get_trace.h"

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
    // step 1e：段内 seq 边界（链内选择按 seq，不按位置——gen 号跨物理
    // 分区不可比较，位置序会在"活跃段更旧、冻结段更新"时选出旧版本）。
    //
    // **顺序要求**：max_seq 是供读路径剪枝用的**上界**，必须在节点对本
    // 段可见（InsertConcurrently）**之前**抬升；否则并发读可能看到"已有
    // 新记录但上界仍是旧值"而提前退出，漏掉更高版本（低估 = 不安全）。
    // 反过来，插入失败（重复条目）会让上界偏大——只多扫段，不影响正确性。
    {
      const uint64_t packed = ROCKSDB_NAMESPACE::DecodeFixed64(
          internal_key.data() + internal_key.size() - 8);
      const uint64_t seq = packed >> 8;
      uint64_t cur = max_seq_.load(std::memory_order_relaxed);
      while (seq > cur &&
             !max_seq_.compare_exchange_weak(cur, seq,
                                             std::memory_order_relaxed)) {
      }
      cur = min_seq_.load(std::memory_order_relaxed);
      while (seq < cur &&
             !min_seq_.compare_exchange_weak(cur, seq,
                                             std::memory_order_relaxed)) {
      }
    }
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

  // step 1e：段内 seq 边界（0 = 空段；空段不参与链内选择）。
  uint64_t MaxSeq() const { return max_seq_.load(std::memory_order_relaxed); }
  uint64_t MinSeq() const { return min_seq_.load(std::memory_order_relaxed); }

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

  // step 0b-3 诊断：打印本索引中该 user key 的全部版本（seq/type/locator）。
  // 只读；用于把"读回旧值"归因到索引链（active/frozen 各代）而不是猜。
  void DumpKeyVersions(const ROCKSDB_NAMESPACE::Slice& user_key,
                       std::string* out) const {
    std::string target;
    target.reserve(user_key.size() + 8);
    target.append(user_key.data(), user_key.size());
    ROCKSDB_NAMESPACE::PutFixed64(
        &target, ROCKSDB_NAMESPACE::PackSequenceAndType(
                     ROCKSDB_NAMESPACE::kMaxSequenceNumber,
                     ROCKSDB_NAMESPACE::kValueTypeForSeek));
    std::string encoded;
    ROCKSDB_NAMESPACE::PutVarint32(&encoded,
                                   static_cast<uint32_t>(target.size()));
    encoded.append(target);
    Iterator iter(&list_);
    iter.Seek(encoded.data());
    int n = 0;
    for (; iter.Valid() && n < 8; iter.Next(), ++n) {
      const char* entry = iter.key();
      const ROCKSDB_NAMESPACE::Slice ik =
          ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(entry);
      if (ik.size() < 8) {
        break;
      }
      if (user_key.compare(
              ROCKSDB_NAMESPACE::Slice(ik.data(), ik.size() - 8)) != 0) {
        break;  // 该 key 的版本已列完
      }
      const uint64_t packed =
          ROCKSDB_NAMESPACE::DecodeFixed64(ik.data() + ik.size() - 8);
      // PackSequenceAndType(seq,type) = (seq << 8) | type —— 手工解包，
      // 避免依赖本版本 UnPackSequenceAndType 的重载形态。
      const uint64_t sq = packed >> 8;
      const uint64_t vt = packed & 0xFF;
      const char* p = entry;
      uint32_t ik_len = 0;
      p = ROCKSDB_NAMESPACE::GetVarint32Ptr(p, p + 5, &ik_len);
      p += ik_len;
      const ROCKSDB_NAMESPACE::Slice loc =
          ROCKSDB_NAMESPACE::GetLengthPrefixedSlice(p);
      uint32_t lp = 0, lg = 0;
      uint64_t loff = 0;
      if (loc.size() == 16) {
        memcpy(&lp, loc.data(), 4);
        memcpy(&lg, loc.data() + 4, 4);
        memcpy(&loff, loc.data() + 8, 8);
      }
      char buf[192];
      snprintf(buf, sizeof(buf),
               "    seq=%llu type=%u loc(part=%u gen=%u off=%llu)\n",
               (unsigned long long)sq, (unsigned)vt, lp, lg,
               (unsigned long long)loff);
      out->append(buf);
    }
    if (n == 0) {
      out->append("    (no version)\n");
    }
  }

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
  // step 1e：本段已插入记录的最大/最小 seq（链内选择与提前退出用）。
  // 初值：max=0（任何插入都更大）、min=UINT64_MAX（任何插入都更小）。
  std::atomic<uint64_t> max_seq_{0};
  std::atomic<uint64_t> min_seq_{~static_cast<uint64_t>(0)};
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
    blooms_.emplace(part_id, std::make_shared<PartitionBloom>());
    return idx;
  }

  // 插入（AddRecord 调用；写路径并发安全）。返回新增内存字节（0 = 重复）。
  // M4.3 修复：必须按 locator 的 gen 选索引——WAL Append（拿 gen）与索引
  // Insert 之间 freeze 可能发生（Active 换代），现取 Active 会把旧 gen 的
  // 条目插进新 gen 索引，物化后该条目丢失（locator 指向已回收 WAL）。
  uint64_t Insert(uint32_t part_id, uint32_t gen,
                  const ROCKSDB_NAMESPACE::Slice& internal_key,
                  const ROCKSDB_NAMESPACE::Slice& locator) {
    // M5 诊断：写路由 vs 读路由一致性（Concurrent 用例定位）。
    {
      static std::atomic<uint64_t> zf_ins_dbg {0};
      if (zf_ins_dbg.fetch_add(1, std::memory_order_relaxed) < 2000) {
        const auto uk = ROCKSDB_NAMESPACE::ExtractUserKey(internal_key);
        if (uk.size() >= 9 && memcmp(uk.data(), "k00000035", 9) == 0) {
          uint64_t off = 0;
          if (locator.size() >= 16) {
            memcpy(&off, locator.data() + 8, 8);
          }
          fprintf(stderr, "ZFDBG-ins part=%u gen=%u off=%llu key=%s\n",
                  part_id, gen, (unsigned long long)off,
                  uk.ToString(true).c_str());
        }
      }
    }
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
          blooms_.emplace(part_id, std::make_shared<PartitionBloom>());
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
        // M5（zeroflush0.98）修复：无锁链查找可能落在 freeze 的
        // "活跃替换后、frozen push 前"窗口（freeze 持写锁 push——本
        // 查找无锁——暂未见 gen → 误判丢弃 → 在途条目游离（读不到；
        // §3.5 转 recovery 后无 SST 回退——Concurrent 用例 NotFound
        // 实锤）。持锁重查一次（freeze push 完成后可见）。
        if (idx == nullptr) {
          std::shared_lock<std::shared_mutex> l(mu_);
          auto fit2 = frozen_.find(part_id);
          if (fit2 != frozen_.end()) {
            for (auto c = fit2->second.rbegin(); c != fit2->second.rend();
                 ++c) {
              if ((*c)->gen() == gen) {
                idx = *c;
                break;
              }
            }
          }
        }
      }
    }
    if (idx == nullptr) {
      insert_dropped_.fetch_add(1, std::memory_order_relaxed);
      // gen 已物化并释放（数据已进 SST）——索引插入丢弃，数据由 SST 承载。
      // M5 诊断：插入丢弃（Concurrent 定位）。
      {
        static std::atomic<uint64_t> zf_insdrop_dbg {0};
        if (zf_insdrop_dbg.fetch_add(1, std::memory_order_relaxed) < 64) {
          const auto uk = ROCKSDB_NAMESPACE::ExtractUserKey(internal_key);
          if (uk.size() >= 9 && memcmp(uk.data(), "k00000035", 9) == 0) {
            fprintf(stderr, "ZFDBG-insdrop part=%u gen=%u\n", part_id, gen);
          }
        }
      }
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
    BloomAdd(part_id, ROCKSDB_NAMESPACE::ExtractUserKey(internal_key));
    total_mem_bytes_.fetch_add(added, std::memory_order_relaxed);
    return added;
  }

  // M4.3a：封存时冻结分区 p 的活跃索引（gen = 换代的 WAL 代）。
  // active → frozen 链头（新→旧）；新活跃索引接替（gen+1）。
  // M5（zeroflush0.98）修复：无条件转 frozen（原 mem_bytes()>0 条件）——
  // freeze 的 mem 检查与在途写组的锁外 Insert 竞态（检查见旧值 0 →
  // 丢弃活跃索引）→ 在途条目落"游离索引"（不在 active/frozen——读不
  // 到）。旧设计靠"游离失败回退 SST"兜底（数据已物化）；§3.5 冲突转
  // recovery 后数据不在 SST → 读不到（Concurrent 用例 NotFound 实锤）。
  // 无条件转后：在途 Insert 按 gen 找 frozen 链命中 → 可读。空占位
  // （freeze 时真无数据）仅链中占位（mem=0 无条目），量级可忽略。
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
    if (old != nullptr) {
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
    auto nb = std::make_shared<PartitionBloom>();
    for (const auto& idx : chain) {
      idx->ForEachKey([&nb](const ROCKSDB_NAMESPACE::Slice& ik) {
        nb->Add(ROCKSDB_NAMESPACE::ExtractUserKey(ik));
      });
    }
    std::unique_lock<std::shared_mutex> l(mu_);
    blooms_[part_id] = std::move(nb);
  }

  // R0（TSAN 实测 data race）：布隆更新——持 shared 锁快照当前 bloom 对象
  // （shared_ptr 保活，重建期旧对象不被释放），锁外 fetch_or。写路径热路径
  // 不加锁的代价：shared_mutex 读锁 ~10-30ns/条；重建（ReleaseFrozen）与
  // 写并发时在锁上短暂互斥（重建持 unique 锁替换 map 值）。替换前的在途
  // Add 落旧对象（丢弃）——该 key 同时已入 active 索引，其布隆位由后续
  // 任何一次 Add/Rebuild 补入前可能假阴性；保守窗口与 M4.11c 注释一致
  // （重建在物化完成时、写路径窗口小），消除的是内存不安全（vector 释放后
  // fetch_or 的 UAF）而非该逻辑窗口。
  void BloomAdd(uint32_t part_id, const ROCKSDB_NAMESPACE::Slice& user_key) {
    std::shared_ptr<PartitionBloom> b;
    {
      std::shared_lock<std::shared_mutex> l(mu_);
      auto it = blooms_.find(part_id);
      if (it == blooms_.end()) {
        return;
      }
      b = it->second;
    }
    if (b != nullptr) {
      b->Add(user_key);
    }
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
    blooms_.emplace(part_id, std::make_shared<PartitionBloom>());
  }

  // M4.3：Recover 专用插入——找不到 gen 匹配索引时创建（重开时所有 WAL
  // 代都是活的：活跃代建 active，封存代建 frozen）。写路径的 Insert 保持
  // "找不到即丢弃"（物化后迟到的插入由 SST 承载）。
  // step 1（修复）：接收**显式活跃代身份**（active_gen）。只有记录所属代
  // 等于该分区当前的活跃代时，才允许进入/创建 active 索引；否则一律进
  // frozen 链。
  // 旧实现的问题（本项目已实测）：`active 缺失即创建` 分支会把"第一条重放
  // 记录"（往往是旧代）直接建成 ACTIVE —— 恢复后 active=旧代、frozen=新代，
  // 读路径 active 优先 ⇒ 旧值遮蔽 SST 中更高 sequence 的新版本。
  uint64_t InsertCreate(uint32_t part_id, uint32_t gen,
                        const std::optional<uint32_t>& active_gen,
                        const ROCKSDB_NAMESPACE::Slice& internal_key,
                        const ROCKSDB_NAMESPACE::Slice& locator) {
    std::shared_ptr<PartitionIndex> idx;
    {
      std::unique_lock<std::shared_mutex> l(mu_);
      auto it = active_.find(part_id);
      const bool is_active_gen = active_gen.has_value() && gen == *active_gen;
      if (is_active_gen && it != active_.end() && it->second->gen() == gen) {
        idx = it->second;
      } else if (is_active_gen) {
        // 属于活跃代：创建/替换 active（缺失或换代）。
        auto ni = std::make_shared<PartitionIndex>(part_id, gen, cmp_);
        if (it == active_.end()) {
          active_.emplace(part_id, ni);
          blooms_.emplace(part_id, std::make_shared<PartitionBloom>());
        } else {
          it->second = ni;  // 换代：旧 active 由调用方 Freeze 处理
        }
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
    BloomAdd(part_id, ROCKSDB_NAMESPACE::ExtractUserKey(internal_key));
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
      if (bit == blooms_.end() || !bit->second->MayContain(user_key)) {
        // M5 修复（MultiEpoch 读路径）：布隆**仅作快路径**——未命中不再短路
        // 返回 false，而是继续查索引链（慢路径）。原因：M4.11c "frozen 释放
        // 后重建布隆" 存在已知竞态（重建替换布隆前执行的写路径 Add 落旧布隆
        // 被丢弃 → 假阴性）；大量 epoch 反复 freeze/release 时该窗口被放大，
        // 假阴性 → 未物化 key 直接 NotFound（实测 Get 0/500、iter 455）。
        // 布隆命中的快路径不变；miss 才走链（正确性兜底，计入诊断计数）。
        bloom_slowpath_.fetch_add(1, std::memory_order_relaxed);
        static std::atomic<uint64_t> zf_bmiss_dbg {0};
        if (zf_bmiss_dbg.fetch_add(1, std::memory_order_relaxed) < 32 && user_key.size() >= 9 &&
            memcmp(user_key.data(), "k00000035", 9) == 0) {
          fprintf(stderr, "ZFDBG-bmiss part=%u key=%s\n", part_id,
                  user_key.ToString(true).c_str());
        }
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
      // M5 诊断：part3 链状态（Concurrent 定位——列出 frozen 各 gen）。
      static std::atomic<uint64_t> zf_chain_dbg {0};
      if (zf_chain_dbg.fetch_add(1, std::memory_order_relaxed) < 40 && part_id == 3 && user_key.size() >= 9 &&
          memcmp(user_key.data(), "k00000035", 9) == 0) {
        std::string gens;
        for (const auto& ci : chain) {
          gens += std::to_string(ci->gen()) + ",";
        }
        fprintf(stderr, "ZFDBG-chain part=%u chain_gens=[%s]\n", part_id,
                gens.c_str());
      }
    }
    // frozen 链：新→旧（vector 尾部最旧——push_back 语义）。
    // 需从"最新 frozen"到"最旧 frozen"再到 active 的顺序查——frozen 链
    // 存为 [最旧...最新]？Freeze 用 push_back → 链尾最新。Get 从链尾往前。
    const bool zf_trace =
        zeroflush::get_trace::Enabled() && zeroflush::get_trace::Match(user_key);
    // step 1e（用户 2026-09-15，C 修复）：链内选择按**序列号**而非位置。
    // 依据（逐键证据 v=1031）：同一 logical_part 的链里，"活跃段 gen0" 命中
    // seq=4104，而"冻结段 gen1"持有 seq=44104（物理落在 part0/gen1）——位置序
    // （active 优先）选出旧版本。gen 号跨物理分区不可比较，唯一可比的是 seq。
    // 规则：按各段 max_seq 降序访问，命中后若当前最优 seq ≥ 下一段 max_seq
    // 上界即可提前退出；删除标记同样参与（最高 seq 者胜出，由调用方映射
    // NotFound）。布隆快路径在其上不变。
    {
      std::vector<std::pair<PartitionIndex*, uint64_t>> cands;
      cands.reserve(chain.size());
      for (auto& c : chain) {
        cands.emplace_back(c.get(), c->MaxSeq());
      }
      std::stable_sort(cands.begin(), cands.end(),
                       [](const std::pair<PartitionIndex*, uint64_t>& a,
                          const std::pair<PartitionIndex*, uint64_t>& b) {
                         return a.second > b.second;
                       });
      bool have_best = false;
      // 注意：locator_out 的契约是"指向**跨调用稳定**的内存"（段内跳表
      // arena，随段对象存活），调用方会在返回后继续使用。因此这里必须直接
      // 返回段的 Slice，**不得**拷进本函数栈上的缓冲（栈帧返回即失效 →
      // 实测读出垃圾 locator → bad part_id）。
      ROCKSDB_NAMESPACE::Slice best_loc;
      ROCKSDB_NAMESPACE::ValueType best_type =
          ROCKSDB_NAMESPACE::kTypeValue;
      ROCKSDB_NAMESPACE::SequenceNumber best_seq = 0;
      uint32_t zf_visit2 = 0;
      for (size_t ci = 0; ci < cands.size(); ++ci) {
        ++zf_visit2;
        PartitionIndex* seg = cands[ci].first;
        ROCKSDB_NAMESPACE::Slice l;
        ROCKSDB_NAMESPACE::ValueType t = ROCKSDB_NAMESPACE::kTypeValue;
        ROCKSDB_NAMESPACE::SequenceNumber sq = 0;
        const bool hit = seg->Get(user_key, snapshot, &l, &t, &sq);
        if (zf_trace) {
          fprintf(stderr,
                  "ZFDBG-idx-%s logical_part=%u idx_part=%u idx_gen=%u "
                  "idx_frozen=%d seg_max_seq=%llu visit=%u/%zu snap=%llu%s%llu"
                  "%s\n",
                  hit ? "hit" : "miss", part_id, seg->part_id(), seg->gen(),
                  (int)seg->frozen(), (unsigned long long)cands[ci].second,
                  zf_visit2, cands.size(), (unsigned long long)snapshot,
                  hit ? " hit_seq=" : " hit_seq=-",
                  (unsigned long long)sq,
                  hit ? (t == ROCKSDB_NAMESPACE::kTypeDeletion ? " type=DEL"
                      : t == ROCKSDB_NAMESPACE::kTypeSingleDeletion
                          ? " type=SINGLE_DEL"
                          : t == ROCKSDB_NAMESPACE::kTypeRangeDeletion
                              ? " type=RANGEDEL"
                              : " type=VAL")
                      : "");
        }
        if (hit && (!have_best || sq > best_seq)) {
          best_loc = l;  // 段内 arena 内存，跨调用稳定
          best_type = t;
          best_seq = sq;
          have_best = true;
        }
        if (have_best && (ci + 1 >= cands.size() ||
                          cands[ci + 1].second <= best_seq)) {
          break;  // 后续段 max_seq 上界不超过当前最优 → 不可能被超过
        }
      }
      if (have_best) {
        if (locator_out != nullptr) *locator_out = best_loc;
        if (type_out != nullptr) *type_out = best_type;
        if (seq_out != nullptr) *seq_out = best_seq;
        if (zf_trace) {
          uint32_t phys_part = 0, phys_gen = 0;
          uint64_t phys_off = 0;
          if (best_loc.size() == 16) {
            const SlimLocator* sl =
                reinterpret_cast<const SlimLocator*>(best_loc.data());
            phys_part = sl->part_id;
            phys_gen = sl->gen;
            phys_off = sl->wal_offset;
          }
          fprintf(stderr,
                  "ZFDBG-idx-select logical_part=%u best_seq=%llu "
                  "phys_part=%u phys_gen=%u phys_off=%llu\n",
                  part_id, (unsigned long long)best_seq, phys_part, phys_gen,
                  (unsigned long long)phys_off);
        }
        return true;
      }
    }
    if (zf_trace) {
      std::string gens;
      for (const auto& ci : chain) {
        gens += std::to_string(ci->gen());
        gens += ",";
      }
      fprintf(stderr,
              "ZFDBG-idx-miss-all logical_part=%u chain_len=%zu chain_gens=[%s] "
              "snap=%llu\n",
              part_id, chain.size(), gens.c_str(),
              (unsigned long long)snapshot);
    }
    return false;
  }

  // step 1h：列出仍有索引段（活跃或冻结）的分区及其 **seq 上界**（该分区
  // 全部段的 max_seq 最大值，保守高估）。用途：跨分区候选选择——写入时与
  // 读时的路由表版本可能不同（每 epoch 重建表），未安装记录的索引登记在
  // 写入时的分区上；读必须在**所有可能持有该键的候选**间按 seq 取最优，
  // 上界用于**可靠剪枝**（上界 ≤ 当前最优 ⇒ 该候选不可能更新）。
  void ChainParts(std::vector<std::pair<uint32_t, uint64_t>>* out) const {
    out->clear();
    std::unordered_map<uint32_t, uint64_t> best;
    {
      std::shared_lock<std::shared_mutex> l(mu_);
      for (const auto& [p, segs] : frozen_) {
        for (const auto& sg : segs) {
          if (sg == nullptr) continue;
          uint64_t& b = best[p];
          if (sg->MaxSeq() > b) b = sg->MaxSeq();
        }
      }
      for (const auto& [p, seg] : active_) {
        if (seg == nullptr) continue;
        uint64_t& b = best[p];
        if (seg->MaxSeq() > b) b = seg->MaxSeq();
      }
    }
    out->reserve(best.size());
    for (const auto& [p, b] : best) out->emplace_back(p, b);
    std::sort(out->begin(), out->end());
  }

  // step 1i：单个分区的 seq 上界（其全部段的 max_seq 最大值，保守高估）。
  // 剪枝必须对**每一个**候选都用真实上界——用 0 当占位会让该候选被误跳过
  // （别名/DRAINING 候选尤其如此）。返回 0 表示该分区没有索引段。
  uint64_t PartMaxSeqUpper(uint32_t part_id) const {
    uint64_t best = 0;
    std::shared_lock<std::shared_mutex> l(mu_);
    auto fit = frozen_.find(part_id);
    if (fit != frozen_.end()) {
      for (const auto& sg : fit->second) {
        if (sg != nullptr && sg->MaxSeq() > best) best = sg->MaxSeq();
      }
    }
    auto ait = active_.find(part_id);
    if (ait != active_.end() && ait->second != nullptr &&
        ait->second->MaxSeq() > best) {
      best = ait->second->MaxSeq();
    }
    return best;
  }

  // step 1f（用户 2026-09-15，C 补充检查 3）：拷贝版 Get——locator 拷进
  // **调用方提供的缓冲**。Get() 返回的 Slice 指向段内 arena，段一旦被
  // 释放（物化完成 ReleaseFrozen）即失效：只保证"指针指向 arena"不足以
  // 保证生命周期。读路径改用本入口，把 locator 的所有权收进调用方栈变量，
  // 直到 ReadValue 完成；WAL generation 的生命周期由 ReadValue 自身的
  // 引用（活跃代句柄 / SealedFileCache 登记+LRU 句柄）保证。
  // pin_fn/pin_ctx：可选回调，在本函数**返回之前**（链的 shared_ptr 仍在
  // 作用域内）以选中的 (physical_part, generation) 调用一次——调用方在此取
  // WAL 读句柄，覆盖"选择 → ReadValue"之间的窗口（用户 2026-09-15：先返回
  // locator 再 pin 会留出安装/回收窗口）。活跃代无法 pin（Freeze 轮换写
  // 句柄），由调用方按错误协议处理。
  bool GetCopy(uint32_t part_id, const ROCKSDB_NAMESPACE::Slice& user_key,
               ROCKSDB_NAMESPACE::SequenceNumber snapshot,
               std::string* locator_buf,
               ROCKSDB_NAMESPACE::ValueType* type_out,
               ROCKSDB_NAMESPACE::SequenceNumber* seq_out,
               void (*pin_fn)(void*, uint32_t, uint32_t) = nullptr,
               void* pin_ctx = nullptr) const {
    ROCKSDB_NAMESPACE::Slice l;
    ROCKSDB_NAMESPACE::ValueType t = ROCKSDB_NAMESPACE::kTypeValue;
    ROCKSDB_NAMESPACE::SequenceNumber sq = 0;
    if (!Get(part_id, user_key, snapshot, &l, &t, &sq)) {
      return false;
    }
    locator_buf->assign(l.data(), l.size());
    if (type_out != nullptr) *type_out = t;
    if (seq_out != nullptr) *seq_out = sq;
    if (pin_fn != nullptr && locator_buf->size() == sizeof(SlimLocator)) {
      const SlimLocator* sl =
          reinterpret_cast<const SlimLocator*>(locator_buf->data());
      pin_fn(pin_ctx, sl->part_id, sl->gen);
    }
    return true;
  }

  // ZeroFlushV0 P1：该分区是否仍有 frozen 索引对象（merge retire 判定用；
  // 物化完成后 ReleaseFrozen 会清空链，此时返回 false）。
  bool HasFrozen(uint32_t part_id) const {
    std::shared_lock<std::shared_mutex> l(mu_);
    auto it = frozen_.find(part_id);
    return it != frozen_.end() && !it->second.empty();
  }

  uint64_t total_mem_bytes() const {
    return total_mem_bytes_.load(std::memory_order_relaxed);
  }

  // M5：布隆假阴性慢路径计数（正确性兜底次数；诊断/调优用）。
  uint64_t bloom_slowpath() const {
    return bloom_slowpath_.load(std::memory_order_relaxed);
  }
  // M5：索引插入因 gen 无匹配被丢弃的次数（诊断：读不到 = 索引无 key）。
  uint64_t insert_dropped() const {
    return insert_dropped_.load(std::memory_order_relaxed);
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
  // step 0b-3 诊断：按**读路径的实际遍历顺序**打印该分区的索引链。
  void DumpKeyChain(uint32_t part_id, const ROCKSDB_NAMESPACE::Slice& user_key,
                    std::string* out) const {
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
      char buf[96];
      snprintf(buf, sizeof(buf), "  chain_len=%zu (读序：rbegin→rend)\n",
               chain.size());
      out->append(buf);
    }
    int pos = 0;
    for (auto c = chain.rbegin(); c != chain.rend(); ++c, ++pos) {
      char buf[176];
      snprintf(buf, sizeof(buf),
               "  chain#%d part=%u gen=%u frozen=%d mem_bytes=%llu\n", pos,
               (*c)->part_id(), (*c)->gen(), (int)(*c)->frozen(),
               (unsigned long long)(*c)->mem_bytes());
      out->append(buf);
      if (pos < 4) {
        (*c)->DumpKeyVersions(user_key, out);
      }
    }
  }

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
  // R0（TSAN 实测 data race）：重建期旧 bloom 不可释放——值存 shared_ptr，
  // BloomAdd 先持 shared 锁快照、锁外 fetch_or（旧对象由快照保活到重建
  // 完成后的在途 Add 结束）；RebuildBloom 以新对象替换 map 值。
  std::unordered_map<uint32_t, std::shared_ptr<PartitionBloom>> blooms_;
  std::atomic<uint64_t> total_mem_bytes_{0};
  // M5：布隆假阴性慢路径计数（正确性兜底；Get 布隆 miss 时查链）。
  mutable std::atomic<uint64_t> bloom_slowpath_{0};
  std::atomic<uint64_t> insert_dropped_{0};
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
