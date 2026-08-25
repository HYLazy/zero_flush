//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M3.1: PartitionTable 范围路由 + PartitionTableSet 版本化容器 + 采样器。
//
// 对应 M3_DESIGN.md §4.2 / §4.3 / §5.2。

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace zeroflush {

// ---------------------------------------------------------------------------
// PartitionTable：不可变的边界快照。Route 是 user_key 的纯函数。
// ---------------------------------------------------------------------------
class PartitionTable {
 public:
  // boundaries 为升序的 P-1 个分隔键（按 ucmp）；out 为输出。
  // 校验：升序、无重复、size() == 0 或 partitions - 1。
  static rocksdb::Status Create(uint32_t version,
                                std::vector<std::string> boundaries,
                                const rocksdb::Comparator* ucmp,
                                std::shared_ptr<PartitionTable>* out);

  // hash 兼容模式：无边界，Route 用 Hash(key) % partitions。
  static std::shared_ptr<PartitionTable> CreateHash(uint32_t version,
                                                    uint32_t partitions);

  // 路由：hash 模式返回 hash(key) % P；范围模式返回 upper_bound 下标。
  uint32_t Route(const rocksdb::Slice& user_key) const;

  // 分区 p 的键区间 [lo, hi)；p==0 时 lo 为 -∞（空 Slice），
  // p==P-1 时 hi 为 +∞（空 Slice）。
  void RangeOf(uint32_t p, rocksdb::Slice* lo, rocksdb::Slice* hi) const;

  bool IsHashMode() const { return is_hash_mode_; }
  uint32_t version() const { return version_; }
  uint32_t partitions() const {
    return static_cast<uint32_t>(part_ids_.size());
  }
  const rocksdb::Comparator* ucmp() const { return ucmp_; }

  // 分区在全局 part_id 空间中的编号（初始为 [0, P)），分裂后可能稀疏。
  const std::vector<uint32_t>& part_ids() const { return part_ids_; }

  // 该表中第 i 个分区对应的全局 part_id (i ∈ [0, partitions))。
  uint32_t part_id(uint32_t i) const { return part_ids_[i]; }

 private:
  PartitionTable(uint32_t version, bool is_hash_mode,
                 std::vector<std::string> boundaries,
                 std::vector<uint32_t> part_ids,
                 const rocksdb::Comparator* ucmp);

  uint32_t version_;
  bool is_hash_mode_;
  std::vector<std::string> boundaries_;    // 升序，size = P-1（hash 模式为空）
  std::vector<uint32_t> part_ids_;         // 与 [0, P) 一一对应的全局 id
  const rocksdb::Comparator* ucmp_;        // 必须是 DB 的 user comparator
};

// ---------------------------------------------------------------------------
// PartitionTableSet：全部历史边界版本。写路径只用 current()。
// ---------------------------------------------------------------------------
class PartitionTableSet {
 public:
  PartitionTableSet() = default;

  std::shared_ptr<PartitionTable> current() const;
  std::shared_ptr<PartitionTable> Get(uint32_t version) const;

  // 仅允许在 Seal 的原子窗口内调用（持 DB mutex + write thread 独占）。
  void InstallNewVersion(std::shared_ptr<PartitionTable> t);

  // 注册初始版本 0（Open 时调用；不可重复）。
  rocksdb::Status Init(std::shared_ptr<PartitionTable> t);

  uint32_t current_version() const {
    return current_version_.load(std::memory_order_acquire);
  }

 private:
  mutable rocksdb::port::Mutex mu_;
  std::vector<std::shared_ptr<PartitionTable>> versions_;  // 下标 = version
  std::atomic<uint32_t> current_version_{0};
};

// ---------------------------------------------------------------------------
// KeySampler：kSampled 模式的学习期采样器。
// 蓄水池算法：前 max_samples 条全收，之后按概率替换。
// ---------------------------------------------------------------------------
class KeySampler {
 public:
  explicit KeySampler(uint32_t sample_every_n,
                      const rocksdb::Comparator* ucmp);

  // 按 sample_every_n 步长采样 user_key（蓄水池算法）。
  void Sample(const rocksdb::Slice& user_key);

  // 从已采样键中选取 P-1 个分位点作为边界。返回 false 表示采样不足。
  // 成功时 boundaries 为升序 P-1 个 key（按 ucmp）。
  bool BuildBoundaries(uint32_t partitions,
                       std::vector<std::string>* boundaries) const;

  void Clear();
  bool empty() const;
  uint64_t total_seen() const;
  size_t sample_count() const;

 private:
  uint32_t sample_every_n_;
  const rocksdb::Comparator* ucmp_;
  std::vector<std::string> samples_;       // 蓄水池
  uint64_t total_seen_{0};
  uint64_t max_samples_{64 * 1024};   // 上界 64K 条
  // M4.6c：学习期写路径 16 线程并发 Sample（AddRecord 锁外调用）——
  // vector 无锁并发写是数据竞争（析构时 string 损坏崩溃：R41/gdb 栈
  // KeySampler::~KeySampler → vector<string> 析构 → free(垃圾指针)）。
  // 采样仅 epoch 1 学习期，加锁开销可忽略。
  mutable rocksdb::port::Mutex mu_;
};

}  // namespace zeroflush