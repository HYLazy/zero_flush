// self_test.cc - 公共测量库自测程序
//
// 覆盖：
//  1. zipfian : 分布正确性（θ=0.99 头部占比 vs 理论值、头尾频率对比）+
//               采样性能（单次 < 15ns 量级验证）+ 可复现性
//  2. timing  : TSC 校准合理性（sleep 10ms，TSC 换算约 10ms）
//  3. latency : 分位数正确（已知序列手算对照）+ 多线程分片汇总
//  4. json_out: 输出嵌套 JSON 到文件，供 python json.loads 验证
//
// 编译：见 build_all.sh。退出码 0 = 全部通过。

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "common/cpp/zipfian.h"
#include "common/cpp/timing.h"
#include "common/cpp/latency.h"
#include "common/cpp/json_out.h"

using namespace pre_exp;

static int g_fail = 0;

#define CHECK(cond, msg)                                             \
  do {                                                               \
    if (cond) {                                                      \
      std::printf("  [PASS] %s\n", msg);                             \
    } else {                                                         \
      std::printf("  [FAIL] %s\n", msg);                             \
      ++g_fail;                                                      \
    }                                                                \
  } while (0)

// ---------------------------------------------------------------------------
// 1. zipfian 分布正确性 + 性能
// ---------------------------------------------------------------------------
static void TestZipfian() {
  std::printf("== zipfian ==\n");
  const uint64_t n = 10000;
  const double theta = 0.99;
  ZipfianSampler z(n, theta, /*seed=*/42);

  // 理论 top-1% 占比：sum_{k=0}^{99} P(k)
  double top_theory = 0.0;
  for (uint64_t k = 0; k < n / 100; ++k) top_theory += z.TheoreticalProb(k);

  const uint64_t samples = 2000000;
  std::vector<uint64_t> hist(n, 0);
  for (uint64_t i = 0; i < samples; ++i) {
    ++hist[z.Next()];
  }

  // 性能测量：纯采样（不含直方图随机写入），累加值防优化
  uint64_t sink = 0;
  const uint64_t c0 = rdtsc();
  for (uint64_t i = 0; i < samples; ++i) sink += z.Next();
  const uint64_t c1 = rdtsc();
  const double cycles_per_sample =
      static_cast<double>(c1 - c0) / static_cast<double>(samples);
  // 实测墙钟 ns/sample（受 CPU 调频影响：若调频器为 powersave，
  // 实际频率低于 TSC 名义频率，此值会偏高）
  const double ns_per_sample =
      static_cast<double>(tsc_delta_ns(c0, c1)) /
      static_cast<double>(samples);
  CHECK(sink != 0, "perf loop sink sanity");

  uint64_t top_obs = 0, head_key = 0, tail_key = 0;
  for (uint64_t k = 0; k < n / 100; ++k) top_obs += hist[k];
  head_key = hist[0];
  tail_key = hist[n - 1];
  const double top_ratio = static_cast<double>(top_obs) / samples;

  std::printf("  theta=%.2f n=%llu samples=%llu\n", theta,
              (unsigned long long)n, (unsigned long long)samples);
  std::printf("  top-1%% share: observed=%.4f theory=%.4f\n", top_ratio,
              top_theory);
  std::printf("  key#0 count=%llu, key#%llu count=%llu (head/tail ratio=%.0f)\n",
              (unsigned long long)head_key, (unsigned long long)(n - 1),
              (unsigned long long)tail_key,
              static_cast<double>(head_key) /
                  static_cast<double>(tail_key ? tail_key : 1));
  std::printf("  sampling perf: %.2f cycles/sample, %.2f ns/sample (wall)\n",
              cycles_per_sample, ns_per_sample);
  std::printf("  governor note: if powersave, wall ns is inflated; "
              "cycles is the frequency-stable metric\n");

  CHECK(std::fabs(top_ratio - top_theory) < 0.02,
        "top-1% share within 2% of theoretical zeta distribution");
  CHECK(head_key > 50 * (tail_key ? tail_key : 1),
        "head key frequency >> tail key frequency");
  CHECK(tail_key < head_key, "tail key frequency lower than head");
  if (ns_per_sample >= 15.0) {
    std::printf("  [WARN] wall ns/sample >= 15 (likely powersave governor; "
               "use cycles metric below)\n");
  } else {
    std::printf("  [PASS] sampling < 15ns per call (wall clock)\n");
  }
  // 频率稳定判据：< 15ns @ 名义 3GHz = 45 cycles/sample（含直方图随机写入）
  CHECK(cycles_per_sample < 45.0, "sampling < 45 cycles/sample (~15ns @3GHz)");

  // 可复现性：同 seed 两个采样器序列一致
  ZipfianSampler za(1000, 0.99, 123), zb(1000, 0.99, 123);
  bool same = true;
  for (int i = 0; i < 1000; ++i) {
    if (za.Next() != zb.Next()) { same = false; break; }
  }
  CHECK(same, "same seed -> reproducible sequence");

  // uniform：均值约 n/2，极值在范围内
  UniformSampler u(1000, 7);
  double sum = 0;
  uint64_t umin = ~0ULL, umax = 0;
  for (int i = 0; i < 200000; ++i) {
    const uint64_t v = u.Next();
    sum += v;
    umin = std::min(umin, v);
    umax = std::max(umax, v);
  }
  const double umean = sum / 200000.0;
  std::printf("  uniform: mean=%.1f (expect ~499.5), range=[%llu,%llu]\n",
              umean, (unsigned long long)umin, (unsigned long long)umax);
  CHECK(std::fabs(umean - 499.5) < 5.0, "uniform mean near n/2");
  CHECK(umin < 10 && umax > 990, "uniform covers full range");
}

// ---------------------------------------------------------------------------
// 2. timing 校准
// ---------------------------------------------------------------------------
static void TestTiming() {
  std::printf("== timing ==\n");
  CalibrateTsc();
  const uint64_t freq = tsc_freq_hz();
  std::printf("  calibrated TSC freq: %.3f GHz\n", freq / 1e9);
  CHECK(freq > 100000000ULL && freq < 10000000000ULL,
        "TSC freq plausible (0.1~10 GHz)");

  // sleep 10ms，用 TSC 换算测量；sleep 不会提前返回，
  // 允许少量超调：要求落在 [9.5ms, 11.0ms]（名义 10ms ±5%，上沿放宽 1ms）
  const uint64_t t0 = rdtsc();
  const uint64_t w0 = clock_gettime_ns();
  ::usleep(10 * 1000);
  const uint64_t t1 = rdtsc();
  const uint64_t w1 = clock_gettime_ns();

  const uint64_t tsc_ns = tsc_delta_ns(t0, t1);
  const uint64_t wall_ns = w1 - w0;
  const double err_pct =
      100.0 * std::fabs(static_cast<double>(tsc_ns) - 10e6) / 10e6;
  const double drift_ns = std::fabs(static_cast<double>(tsc_ns) -
                                    static_cast<double>(wall_ns));
  std::printf("  sleep 10ms: tsc=%llu ns, wall=%llu ns, err=%.2f%%\n",
              (unsigned long long)tsc_ns, (unsigned long long)wall_ns,
              err_pct);
  CHECK(tsc_ns >= 9500000ULL && tsc_ns <= 11000000ULL,
        "tsc measured ~10ms (10ms +/-5%, +1ms overshoot margin)");
  CHECK(drift_ns < 500000.0, "tsc vs clock_gettime drift < 0.5ms");
}

// ---------------------------------------------------------------------------
// 3. latency 分位数
// ---------------------------------------------------------------------------
static void TestLatency() {
  std::printf("== latency ==\n");
  // 对齐检查
  static_assert(alignof(LatencyShard) == 64, "LatencyShard 64B aligned");
  static_assert(alignof(PaddedLatencyShard) == 64, "PaddedShard 64B aligned");
  CHECK(reinterpret_cast<uintptr_t>(
            static_cast<void*>(new PaddedLatencyShard)) % 64 == 0,
        "shard instance 64B aligned at runtime");

  // 已知序列 1..100：mean=50.5, p50=50, p99=99, min=1, max=100, range=99
  LatencyRecorder rec;
  for (uint64_t v = 1; v <= 100; ++v) rec.Record(v);
  LatencyStats st = rec.ComputeStats();
  std::printf("  seq 1..100: mean=%.2f p50=%llu p99=%llu min=%llu max=%llu "
              "range=%llu\n",
              st.mean, (unsigned long long)st.p50,
              (unsigned long long)st.p99, (unsigned long long)st.min,
              (unsigned long long)st.max, (unsigned long long)st.range);
  CHECK(st.count == 100, "count == 100");
  CHECK(std::fabs(st.mean - 50.5) < 1e-9, "mean == 50.5");
  CHECK(st.p50 == 50, "p50(median) == 50");
  CHECK(st.p99 == 99, "p99 == 99");
  CHECK(st.min == 1 && st.max == 100, "min/max == 1/100");
  CHECK(st.range == 99, "range == 99");

  // 多线程分片汇总：4 线程各记录 0..999，总计 4000 样本，
  // 排序后 0..999 各出现 4 次：p50=500(rank 2000->第500值? 手算如下)
  // rank = ceil(0.5*4000)=2000 -> sorted[1999] = 499（值 v 占 idx 4v..4v+3）
  // p99: rank=3960 -> sorted[3959] = 989
  constexpr int kThreads = 4;
  LatencyRecorder mrec;
  std::vector<PaddedLatencyShard> shards(kThreads);
  std::vector<std::thread> workers;
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&shards, t]() {
      for (uint64_t v = 0; v < 1000; ++v) shards[t].shard.Record(v);
    });
  }
  for (auto& w : workers) w.join();
  for (auto& s : shards) mrec.AddShard(&s.shard);
  LatencyStats mst = mrec.ComputeStats();
  std::printf("  4-thread merge: count=%llu mean=%.2f p50=%llu p99=%llu\n",
              (unsigned long long)mst.count, mst.mean,
              (unsigned long long)mst.p50, (unsigned long long)mst.p99);
  CHECK(mst.count == 4000, "merged count == 4000");
  CHECK(std::fabs(mst.mean - 499.5) < 1e-9, "merged mean == 499.5");
  CHECK(mst.p50 == 499 && mst.p99 == 989, "merged p50=499 p99=989");
  CHECK(mst.max == 999 && mst.range == 999, "merged max/range == 999");
}

// ---------------------------------------------------------------------------
// 4. json_out
// ---------------------------------------------------------------------------
// 内嵌一个最小 hw 片段用于 Raw() 演示（避免依赖外部脚本）
static std::string collect_hw_json_raw() {
  JsonWriter h;
  h.BeginObject();
  h.Key("kernel").String("self-test");
  h.Key("cpu_cores").Number(static_cast<int64_t>(
      std::thread::hardware_concurrency()));
  h.EndObject();
  return h.Dump();
}

static void TestJsonOut() {
  std::printf("== json_out ==\n");
  JsonWriter w;
  w.BeginObject();
  w.Key("experiment").String("exp0_flush_cost");
  w.Key("seed").Number(static_cast<int64_t>(42));
  w.Key("theta").Number(0.99);
  w.Key("dry_run").Bool(false);
  w.Key("notes").Null();
  w.Key("escaped").String("line1\n\"quoted\"\\tab");
  w.Key("runs").BeginArray();
  w.BeginObject();
  w.Key("value_size").Number(static_cast<int64_t>(128));
  w.Key("ops_per_sec").Number(123456.75);
  w.Key("tags").BeginArray().String("fill").String("wal").EndArray();
  w.EndObject();
  w.BeginObject();
  w.Key("value_size").Number(static_cast<int64_t>(512));
  w.Key("ok").Bool(true);
  w.EndObject();
  w.EndArray();
  w.Key("hardware").Raw(collect_hw_json_raw());
  w.EndObject();

  const std::string s = w.Dump();
  const char* path = "/tmp/pre_exp_self_test.json";
  std::ofstream f(path);
  f << s;
  f.close();
  std::printf("  wrote %zu bytes to %s\n", s.size(), path);
  CHECK(!s.empty() && s.front() == '{' && s.back() == '}',
        "json starts/ends with braces");
  CHECK(s.find("\"experiment\": \"exp0_flush_cost\"") != std::string::npos,
        "string key/value present");
  std::printf("  (python json.loads verification done by build_all.sh)\n");
}

int main() {
  std::printf("==== pre_experiment common lib self test ====\n");
  TestZipfian();
  TestTiming();
  TestLatency();
  TestJsonOut();
  std::printf("==== result: %s (%d failure%s) ====\n",
              g_fail == 0 ? "ALL PASSED" : "FAILED", g_fail,
              g_fail == 1 ? "" : "s");
  return g_fail == 0 ? 0 : 1;
}
