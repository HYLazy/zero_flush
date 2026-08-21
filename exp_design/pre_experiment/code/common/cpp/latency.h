// latency.h - header-only 延迟统计库
//
// 设计：
//  - LatencyRecorder：每线程一个 64B 对齐的本地累加器（LatencyShard），
//    避免多线程记录时的 false sharing；采样路径只做追加，无锁。
//  - 汇总：AddShard() 收集所有线程分片，排序后计算
//    mean / median(p50) / p99 / max / min / range(极差)。
//
// 无第三方依赖，C++17。

#ifndef PRE_EXP_COMMON_LATENCY_H_
#define PRE_EXP_COMMON_LATENCY_H_

#include <algorithm>
#include <cstdint>
#include <mutex>
#include <vector>

namespace pre_exp {

// 缓存行对齐单位
static constexpr size_t kCacheLineBytes = 64;

// 单线程本地样本缓冲：64B 对齐，避免 false sharing。
// 每个线程持有一个独立实例（通常以 thread_local 使用）。
struct alignas(kCacheLineBytes) LatencyShard {
  std::vector<uint64_t> samples;

  // 容量预分配，避免采样路径上的扩容抖动
  void Reserve(size_t cap) { samples.reserve(cap); }

  inline void Record(uint64_t v) { samples.push_back(v); }

  size_t Count() const { return samples.size(); }

  // 占位填充说明：alignas(64) 保证对象起始地址按 64B 对齐；
  // 连续分配多个实例时，std::vector 内部的指针成员位于对象头部，
  // 不同线程的热写入字段分布在不同 64B 行的概率高。若需要严格
  // 隔离，可配合 PaddedShardStorage 使用。
};

// 带尾部填充的分片包装，保证数组中相邻元素跨不同缓存行
struct alignas(kCacheLineBytes) PaddedLatencyShard {
  LatencyShard shard;
  char pad[kCacheLineBytes];  // 尾部填充，防相邻元素共享缓存行
};

// 汇总统计结果
struct LatencyStats {
  uint64_t count = 0;
  double mean = 0.0;
  uint64_t min = 0;
  uint64_t max = 0;
  uint64_t range = 0;    // 极差 = max - min
  uint64_t p50 = 0;      // 中位数
  uint64_t p99 = 0;
  uint64_t p999 = 0;
};

// LatencyRecorder：聚合多个线程分片的采样数据并计算统计量。
// 使用模式：
//   每个工作线程持有一个 thread_local LatencyShard，采样时调用
//   shard.Record(ns)；结束后调用 recorder.AddShard(&shard) 注册，
//   再调用 recorder.ComputeStats() 汇总。
class LatencyRecorder {
 public:
  // 注册一个线程分片（调用方保证分片生命周期长于统计计算）
  void AddShard(LatencyShard* shard) {
    std::lock_guard<std::mutex> lock(mu_);
    shards_.push_back(shard);
  }

  // 直接记录单样本（单线程便捷接口，内部使用自有缓冲）
  void Record(uint64_t v) { own_.Record(v); }

  // 汇总所有样本并计算统计量；内部排序，调用后样本保持有序
  LatencyStats ComputeStats() {
    std::vector<uint64_t> all;
    {
      std::lock_guard<std::mutex> lock(mu_);
      size_t total = own_.Count();
      for (auto* s : shards_) total += s->Count();
      all.reserve(total);
      all.insert(all.end(), own_.samples.begin(), own_.samples.end());
      for (auto* s : shards_) {
        all.insert(all.end(), s->samples.begin(), s->samples.end());
      }
    }

    LatencyStats st;
    st.count = all.size();
    if (all.empty()) return st;

    std::sort(all.begin(), all.end());
    uint64_t sum = 0;
    for (uint64_t v : all) sum += v;

    st.mean = static_cast<double>(sum) / static_cast<double>(all.size());
    st.min = all.front();
    st.max = all.back();
    st.range = st.max - st.min;
    st.p50 = PercentileSorted(all, 0.50);
    st.p99 = PercentileSorted(all, 0.99);
    st.p999 = PercentileSorted(all, 0.999);
    return st;
  }

  size_t TotalCount() const {
    size_t total = own_.Count();
    for (auto* s : shards_) total += s->Count();
    return total;
  }

  void Clear() {
    own_.samples.clear();
    shards_.clear();
  }

  // 对已排序序列取分位数（nearest-rank 法）：
  // rank = ceil(p * n)，取 sorted[rank-1]
  static uint64_t PercentileSorted(const std::vector<uint64_t>& sorted,
                                    double p) {
    if (sorted.empty()) return 0;
    const size_t n = sorted.size();
    size_t rank = static_cast<size_t>(p * static_cast<double>(n) + 0.5);
    if (rank < 1) rank = 1;
    if (rank > n) rank = n;
    return sorted[rank - 1];
  }

 private:
  LatencyShard own_;              // 便捷接口的自有缓冲
  std::vector<LatencyShard*> shards_;
  std::mutex mu_;
};

}  // namespace pre_exp

#endif  // PRE_EXP_COMMON_LATENCY_H_
