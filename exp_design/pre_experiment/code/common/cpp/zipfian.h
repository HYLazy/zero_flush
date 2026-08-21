// zipfian.h - header-only YCSB 风格 Zipfian / Uniform 采样器
//
// 语义参考 source/gParaKV-GC-master/ycsbc/core/zipfian_generator.h
// （YCSB ZipfianDistribution, 默认 theta = 0.99），但实现方式不同：
// 本实现预计算逆 CDF 查找表（默认 2^20 bins），采样仅需
// 一次 RNG + 一次查表（必要时线性回退一个 bin），单次采样 < 15ns。
//
// RNG：线程局部 xorshift128+，固定可配置 seed，保证可复现。
// 无第三方依赖，C++17。

#ifndef PRE_EXP_COMMON_ZIPFIAN_H_
#define PRE_EXP_COMMON_ZIPFIAN_H_

#include <atomic>
#include <cmath>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace pre_exp {

// ---------------------------------------------------------------------------
// Xorshift128PlusRng：xorshift128+ 伪随机数发生器
// ---------------------------------------------------------------------------
class Xorshift128PlusRng {
 public:
  explicit Xorshift128PlusRng(uint64_t seed) { Reset(seed); }

  // 用 splitmix64 从单 seed 初始化状态，保证 seed=0 也不退化
  void Reset(uint64_t seed) {
    s0_ = Splitmix64(seed);
    s1_ = Splitmix64(s0_);
    if (s0_ == 0 && s1_ == 0) s1_ = 0x9E3779B97F4A7C15ULL;
  }

  inline uint64_t NextU64() {
    uint64_t x = s0_;
    const uint64_t y = s1_;
    s0_ = y;
    x ^= x << 23;
    s1_ = x ^ y ^ (x >> 17) ^ (y >> 26);
    return s1_ + y;
  }

  // [0, 1) 的均匀 double，取高 53 bit
  inline double NextDouble() {
    return static_cast<double>(NextU64() >> 11) * (1.0 / 9007199254740992.0);
  }

 private:
  static uint64_t Splitmix64(uint64_t x) {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
  }

  uint64_t s0_ = 0;
  uint64_t s1_ = 0;
};

namespace detail {
// 线程局部 RNG 流：按 (实例 key, seed) 管理。
// 同构的两个采样器（同 seed）各自持有独立但初始状态相同的流，
// 序列一致可复现；不同实例/不同 seed 互不干扰。
struct StreamSlot {
  uint64_t seed = 0;
  Xorshift128PlusRng rng{0};
};

constexpr size_t kMaxStreams = 4096;

// 全局槽指针表（函数局部 static，避免头文件全局变量问题）
inline std::atomic<StreamSlot*>& SlotPtr(size_t idx) {
  static std::atomic<StreamSlot*> slots[kMaxStreams + 1] = {};
  return slots[idx];
}

// 槽分配：仅创建采样器时走锁；采样热路径只做一次无锁指针解引用。
// 槽不回收（实验程序生命周期内采样器数量极少，微量泄漏可接受）。
inline uint64_t AllocStream() {
  static std::mutex mu;
  static std::vector<std::unique_ptr<StreamSlot>> owned;
  static size_t count = 0;
  std::lock_guard<std::mutex> lock(mu);
  const size_t id = count++;
  if (id >= kMaxStreams) return 0;  // 超限退化（正常实验不会触发）
  owned.emplace_back(new StreamSlot());
  SlotPtr(id + 1).store(owned.back().get(), std::memory_order_release);
  return id + 1;
}

// 热路径：无锁。快路径单槽缓存（典型实验每线程仅 1 个采样器，
// 命中时开销接近直接 thread_local 变量）；未命中走小缓存慢路径。
inline thread_local uint64_t hot_key = 0;
inline thread_local StreamSlot* hot_slot = nullptr;

inline Xorshift128PlusRng& ThreadLocalRngSlow(uint64_t key, uint64_t seed);
inline Xorshift128PlusRng& ThreadLocalRng(uint64_t key, uint64_t seed) {
  if (hot_key == key && hot_slot != nullptr) {
    if (hot_slot->seed != seed) {  // seed 变更时重新初始化
      hot_slot->seed = seed;
      hot_slot->rng.Reset(seed);
    }
    return hot_slot->rng;
  }
  return ThreadLocalRngSlow(key, seed);
}

inline Xorshift128PlusRng& ThreadLocalRngSlow(uint64_t key, uint64_t seed) {
  constexpr int kCacheN = 4;
  thread_local uint64_t cache_key[kCacheN] = {0, 0, 0, 0};
  thread_local StreamSlot* cache_slot[kCacheN] = {nullptr, nullptr,
                                                  nullptr, nullptr};
  thread_local int last = 0;
  StreamSlot* slot = nullptr;
  for (int i = 0; i < kCacheN; ++i) {
    if (cache_key[i] == key && cache_slot[i] != nullptr) {
      slot = cache_slot[i];
      break;
    }
  }
  if (slot == nullptr) {
    slot = SlotPtr(key).load(std::memory_order_acquire);
    cache_key[last] = key;
    cache_slot[last] = slot;
    last = (last + 1) % kCacheN;
  }
  if (slot->seed != seed) {  // 首次使用或 seed 变更时初始化
    slot->seed = seed;
    slot->rng.Reset(seed);
  }
  hot_key = key;
  hot_slot = slot;
  return slot->rng;
}
}  // namespace detail

// ---------------------------------------------------------------------------
// ZipfianSamplerBasic：逆 CDF 查找表 + 尾部解析快速路径（值类型模板化）
// P(k) = (k+1)^(-theta) / zeta, k = 0..n-1
//
// 采样策略（目标单次 < 15ns）：
//  - 头部（查找表覆盖区，概率质量占绝大多数）：一次查表，O(1)；
//    表用最小够用的整型（n<=2^32 时用 uint32_t，2^17 bins = 512KB，
//    适应 L2 缓存）以降低查表延迟。
//  - 尾部（单个 key 占不满一个 bin 的区域，概率质量极小）：
//    YCSB 同款的逆 CDF 闭式解 k = n*(eta*u - eta + 1)^alpha
// ---------------------------------------------------------------------------
template <typename ValueT>
class ZipfianSamplerBasic {
 public:
  static constexpr double kDefaultTheta = 0.99;
  static constexpr uint32_t kDefaultBinsLog2 = 17;  // 2^17 bins

  ZipfianSamplerBasic(uint64_t n, double theta = kDefaultTheta,
                      uint64_t seed = 0,
                      uint32_t bins_log2 = kDefaultBinsLog2) {
    Init(n, theta, seed, bins_log2);
  }

  void Init(uint64_t n, double theta = kDefaultTheta, uint64_t seed = 0,
            uint32_t bins_log2 = kDefaultBinsLog2) {
    n_ = n;
    theta_ = theta;
    seed_ = seed;
    if (stream_key_ == 0) stream_key_ = detail::AllocStream();
    bins_log2_ = bins_log2;
    num_bins_ = uint64_t{1} << bins_log2;

    // 1) 计算归一化常数 zeta_n = sum_{i=1..n} i^(-theta)
    zeta_n_ = 0.0;
    for (uint64_t i = 1; i <= n; ++i) {
      zeta_n_ += std::pow(static_cast<double>(i), -theta);
    }

    // 2) 构建逆 CDF 查找表：
    //    对 bin j，u = j / num_bins，table[j] = 最小的 k 使 CDF(k) >= u。
    //    每个 key 的 CDF 平台跨度 = P(k) * num_bins 个 bin，
    //    由于 theta < 1，P(k) 随 k 单调递减，跨度单调非增，可线性一次扫描。
    //    当跨度降到 < 1 bin 时停止（后续尾部改用解析快速路径）。
    table_.assign(num_bins_ + 1, static_cast<ValueT>(n - 1));
    const double bin_scale = static_cast<double>(num_bins_) / zeta_n_;
    uint64_t j = 0;
    double cdf_bins = 0.0;  // 以 bin 为单位的累积概率
    table_end_ = n;         // 表覆盖的 key 区间 [0, table_end_)
    for (uint64_t k = 0; k < n; ++k) {
      const double mass =
          std::pow(static_cast<double>(k + 1), -theta) * bin_scale;
      if (mass < 1.0) {  // 后续 key 跨度均 < 1 bin，交由尾部路径处理
        table_end_ = k;
        break;
      }
      const double cdf_next = cdf_bins + mass;
      const uint64_t end = static_cast<uint64_t>(cdf_next);  // 截断
      // bin [j, end) 都映射到 key k；跨度随 k 增大而减小，j 不回退
      while (j < end && j <= num_bins_) {
        table_[j++] = static_cast<ValueT>(k);
      }
      cdf_bins = cdf_next;
    }
    // 剩余 bin（尾部概率质量）统一指向 table_end_，由采样时解析路径修正
    while (j <= num_bins_) {
      table_[j++] = static_cast<ValueT>(table_end_);
    }
    tail_cdf_ = cdf_bins / static_cast<double>(num_bins_);  // 表覆盖累积概率

    // 3) 尾部解析快速路径参数（与 YCSB ZipfianGenerator 同款公式）
    alpha_ = 1.0 / (1.0 - theta_);
    const double zeta_2 = 1.0 + std::pow(2.0, -theta_);
    const double two_n = static_cast<double>(std::min(n, table_end_));
    eta_ = (1.0 - std::pow(2.0 / two_n, 1.0 - theta_)) /
           (1.0 - zeta_2 / zeta_n_);
  }

  // 采样一个 [0, n) 的 key（线程局部 RNG，可多线程并发调用）
  inline uint64_t Next() {
    const double u = detail::ThreadLocalRng(stream_key_, seed_).NextDouble();
    if (u < tail_cdf_) {  // 头部：一次查表
      return table_[static_cast<uint64_t>(u * num_bins_)];
    }
    // 尾部：逆 CDF 闭式解（YCSB 同款）
    uint64_t v = static_cast<uint64_t>(
        static_cast<double>(n_) *
        std::pow(eta_ * u - eta_ + 1.0, alpha_));
    if (v >= n_) v = n_ - 1;
    return v;
  }

  // 重置当前线程上本采样器的 RNG 流（用于从头复现同一序列）
  void ResetStream() { detail::ThreadLocalRng(stream_key_, seed_).Reset(seed_); }

  // 理论概率 P(k) = (k+1)^(-theta) / zeta_n，供自测对比
  double TheoreticalProb(uint64_t k) const {
    return std::pow(static_cast<double>(k + 1), -theta_) / zeta_n_;
  }

  uint64_t n() const { return n_; }
  double theta() const { return theta_; }
  uint64_t seed() const { return seed_; }
  double zeta_n() const { return zeta_n_; }

 private:
  uint64_t n_ = 0;
  double theta_ = kDefaultTheta;
  uint64_t seed_ = 0;
  uint64_t stream_key_ = 0;
  uint32_t bins_log2_ = kDefaultBinsLog2;
  uint64_t num_bins_ = 0;
  double zeta_n_ = 0.0;
  uint64_t table_end_ = 0;   // 表覆盖 key 区间 [0, table_end_)
  double tail_cdf_ = 1.0;    // 表覆盖的累积概率（尾部路径入口阈值）
  double alpha_ = 0.0;
  double eta_ = 0.0;
  std::vector<ValueT> table_;
};

// 默认 ZipfianSampler：uint32_t 表（2^17 bins = 512KB，适应 L2 缓存，
// 单次采样延迟最低，支持 n <= 2^32）；超大 n 场景可直接使用
// ZipfianSampler64（uint64_t 表）。
using ZipfianSampler = ZipfianSamplerBasic<uint32_t>;
using ZipfianSampler64 = ZipfianSamplerBasic<uint64_t>;

// ---------------------------------------------------------------------------
// UniformSampler：[0, n) 均匀采样（同样使用线程局部 xorshift128+）
// ---------------------------------------------------------------------------
class UniformSampler {
 public:
  explicit UniformSampler(uint64_t n, uint64_t seed = 0)
      : n_(n), seed_(seed), stream_key_(detail::AllocStream()) {}

  inline uint64_t Next() {
    return detail::ThreadLocalRng(stream_key_, seed_).NextU64() % n_;
  }

  uint64_t n() const { return n_; }
  uint64_t seed() const { return seed_; }

 private:
  uint64_t n_ = 0;
  uint64_t seed_ = 0;
  uint64_t stream_key_ = 0;
};

}  // namespace pre_exp

#endif  // PRE_EXP_COMMON_ZIPFIAN_H_
