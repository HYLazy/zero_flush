// timing.h - header-only 高精度计时工具
//
// 提供：
//  - rdtsc()       : __rdtsc 内联读取 TSC 计数
//  - CalibrateTsc(): 启动时用 clock_gettime(CLOCK_MONOTONIC) 间隔校准
//                    TSC 频率，得到 cycles/ns 换算系数
//  - tsc_to_ns()   : 将 TSC 差值换算为纳秒
//  - clock_gettime_ns(): CLOCK_MONOTONIC 纳秒便捷函数
//
// 注意：校准假设 TSC 为恒定频率（invariant TSC，现代 x86 服务器普遍具备）。
// 无第三方依赖，C++17。

#ifndef PRE_EXP_COMMON_TIMING_H_
#define PRE_EXP_COMMON_TIMING_H_

#include <cstdint>
#include <ctime>
#include <unistd.h>

#if defined(__x86_64__) || defined(__i386__)
#include <x86intrin.h>
#endif

namespace pre_exp {

// 读取 TSC 计数器
inline uint64_t rdtsc() {
#if defined(__x86_64__) || defined(__i386__)
  return static_cast<uint64_t>(__rdtsc());
#else
  // 非 x86 平台退化：直接用单调时钟的 ns 计数
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + ts.tv_nsec;
#endif
}

// CLOCK_MONOTONIC 纳秒便捷函数
inline uint64_t clock_gettime_ns() {
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL +
         static_cast<uint64_t>(ts.tv_nsec);
}

namespace detail {

// TSC 频率校准结果（cycles per nanosecond）
// 用函数内 static 保证进程内只校准一次（线程安全，C++11 magic statics）
struct TscCalibration {
  double cycles_per_ns = 1.0;  // 保守默认：1 cycle = 1 ns
  uint64_t freq_hz = 0;
  bool calibrated = false;

  TscCalibration() {
    // 用多轮 50ms sleep 采样，sleep 只会睡多不会睡少，单轮结果受调度
    // 抖动影响，故采样 5 轮取中位数，稳健估计 cycles/ns。
    constexpr int kRounds = 5;
    double samples[kRounds];
    for (int r = 0; r < kRounds; ++r) {
      const uint64_t ns0 = clock_gettime_ns();
      const uint64_t c0 = rdtsc();
      ::usleep(50 * 1000);  // 50ms
      const uint64_t c1 = rdtsc();
      const uint64_t ns1 = clock_gettime_ns();
      const uint64_t dns = ns1 - ns0;
      if (dns == 0) {
        samples[r] = 0.0;
        continue;
      }
      samples[r] = static_cast<double>(c1 - c0) / static_cast<double>(dns);
    }
    // 插入排序取中位数
    for (int i = 1; i < kRounds; ++i) {
      for (int k = i; k > 0 && samples[k] < samples[k - 1]; --k) {
        const double tmp = samples[k];
        samples[k] = samples[k - 1];
        samples[k - 1] = tmp;
      }
    }
    cycles_per_ns = samples[kRounds / 2];
    if (cycles_per_ns > 0.0) {
      freq_hz = static_cast<uint64_t>(cycles_per_ns * 1e9);
      calibrated = true;
    }
  }
};

inline const TscCalibration& GetTscCalibration() {
  static const TscCalibration cal;
  return cal;
}

}  // namespace detail

// 显式触发校准（首次调用 tsc_to_ns 时也会自动校准）
inline void CalibrateTsc() { (void)detail::GetTscCalibration(); }

// 校准得到的 TSC 频率（Hz）；未校准时返回 0
inline uint64_t tsc_freq_hz() { return detail::GetTscCalibration().freq_hz; }

// 将 TSC 计数差换算为纳秒
inline uint64_t tsc_to_ns(uint64_t delta_cycles) {
  const auto& cal = detail::GetTscCalibration();
  return static_cast<uint64_t>(
      static_cast<double>(delta_cycles) / cal.cycles_per_ns);
}

// 便捷：直接返回两个 TSC 时刻之间的纳秒差
inline uint64_t tsc_delta_ns(uint64_t t0, uint64_t t1) {
  return tsc_to_ns(t1 - t0);
}

}  // namespace pre_exp

#endif  // PRE_EXP_COMMON_TIMING_H_
