//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  Zipfian 负载压测（V2 vs 原生同 key 流对照）
//
//  背景：本 fork 的 db_bench 没有 zipfian 基准（Zipfian 只存在于 db_stress）。
//  本工具提供**确定性、可精确校验**的 Zipfian 写/读负载：
//    · 键分布：P(rank k) ∝ 1/(k+1)^alpha（alpha 默认 0.99，桶化 CDF，
//      桶内均匀）——同一 seed 下 V2 与原生拿到**完全相同的键序列**；
//    · 值：value = f(key)（确定性），因此任意读回都能精确判定对错；
//    · 读阶段**重放同一键序列**（同 seed）⇒ 每个读键都必然写过，
//      found 必须 100%、mismatch 必须 0（任何其它结果都是数据问题）；
//    · 延迟：每线程 1µs 分辨率直方图（尾部分辨率 64µs），最后合并报分位。
//
//  两种构建：
//    - ZeroFlush 构建（默认）：支持 --zeroflush + V2 档（固定范围 16 分区，
//      边界按 keyspace 等分）。
//    - 原生构建：`-DZIPF_NO_ZF=1`，只做 DB::Open（用于同 key 流的基线对照）。

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

#ifndef ZIPF_NO_ZF
#include "zeroflush/zeroflush_db.h"
#endif

namespace {

// ---- 键/值编码（与 tools/zf_keygen.h 一致；此处自带以支持原生构建）----
std::string KeyFromInt(uint64_t v, int key_size = 16) {
  std::string key(static_cast<size_t>(key_size), '\0');
  const int n = std::min(key_size, 8);
  for (int i = 0; i < n; ++i) {
    key[i] = static_cast<char>((v >> ((n - i - 1) << 3)) & 0xFF);
  }
  if (key_size > n) memset(&key[n], '0', static_cast<size_t>(key_size - n));
  return key;
}

std::string ValueFor(uint64_t v, int value_size = 1024) {
  std::string value(static_cast<size_t>(value_size), '\0');
  for (int i = 0; i < value_size; ++i) {
    value[i] = static_cast<char>(
        (static_cast<uint64_t>(v) * 131 + static_cast<uint64_t>(i) * 7) & 0xFF);
  }
  return value;
}

// ---- Zipfian：桶化 CDF（P(rank) ∝ 1/(rank+1)^alpha）----
constexpr size_t kZipfBuckets = 1u << 20;
struct Zipf {
  std::vector<double> cdf;  // 归一化累计概率
  uint64_t keyspace = 0;
  uint64_t per_bucket = 1;

  void Build(uint64_t ks, double alpha) {
    keyspace = ks;
    const size_t n = static_cast<size_t>(std::min<uint64_t>(ks, kZipfBuckets));
    per_bucket = std::max<uint64_t>(1, ks / n);
    std::vector<double> w(n);
    double tot = 0.0;
    for (size_t i = 0; i < n; ++i) {
      w[i] = 1.0 / std::pow(static_cast<double>(i) + 1.0, alpha);
      tot += w[i];
    }
    cdf.resize(n);
    double acc = 0.0;
    for (size_t i = 0; i < n; ++i) {
      acc += w[i];
      cdf[i] = acc / tot;
    }
    cdf.back() = 1.0;
  }
  // u ∈ [0,1) → key 整数（桶内均匀）
  uint64_t Sample(double u, std::mt19937_64* rng) const {
    const auto it = std::lower_bound(cdf.begin(), cdf.end(), u);
    const size_t b = static_cast<size_t>(it - cdf.begin());
    const uint64_t base = static_cast<uint64_t>(b) * per_bucket;
    const uint64_t off = (*rng)() % per_bucket;
    const uint64_t v = base + off;
    return v < keyspace ? v : keyspace - 1;
  }
};

double Uniform(std::mt19937_64* rng) {
  return static_cast<double>((*rng)() >> 11) * 0x1.0p-53;
}

// ---- 延迟直方图：<4096µs 用 1µs 分辨率，尾部 64µs 分辨率 ----
constexpr int kHistN = 8192;
struct Hist {
  std::vector<uint64_t> h = std::vector<uint64_t>(kHistN, 0);
  void Add(uint64_t us) {
    size_t idx;
    if (us < 4096) {
      idx = static_cast<size_t>(us);
    } else {
      idx = 4096 + static_cast<size_t>(std::min<uint64_t>(4095, (us - 4096) / 64));
    }
    ++h[idx];
  }
  uint64_t Pct(double p) const {
    uint64_t tot = 0;
    for (auto v : h) tot += v;
    const uint64_t target = static_cast<uint64_t>(p * static_cast<double>(tot));
    uint64_t acc = 0;
    for (size_t i = 0; i < h.size(); ++i) {
      acc += h[i];
      if (acc >= target) {
        return i < 4096 ? i : 4096 + (i - 4096) * 64;
      }
    }
    return 0;
  }
};

struct Flags {
  std::string db = "/tmp/zipf_bench";
  uint64_t keyspace = 48000000;
  uint64_t ops = 48000000;   // 总写 ops
  uint64_t reads = 0;        // 读阶段 ops（0 = 不读）
  int threads = 16;
  int value_size = 1024;
  double alpha = 0.99;
  uint64_t seed = 20261017;
  bool zeroflush = false;
  uint32_t partitions = 16;
  uint64_t partition_target_mb = 64;
  uint64_t epoch_target_mb = 256;
  uint32_t materialize_parallelism = 16;
  int64_t cache_size = 512ll << 20;
  int max_background_jobs = 24;
  int subcompactions = 16;
  bool histogram = true;
};

bool Parse(int argc, char** argv, Flags* f) {
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto eq = a.find('=');
    if (eq == std::string::npos) {
      fprintf(stderr, "bad arg: %s (expect --name=value)\n", a.c_str());
      return false;
    }
    std::string k = a.substr(0, eq), v = a.substr(eq + 1);
    if (k == "--db") f->db = v;
    else if (k == "--keyspace") f->keyspace = strtoull(v.c_str(), nullptr, 10);
    else if (k == "--ops") f->ops = strtoull(v.c_str(), nullptr, 10);
    else if (k == "--reads") f->reads = strtoull(v.c_str(), nullptr, 10);
    else if (k == "--threads") f->threads = atoi(v.c_str());
    else if (k == "--value_size") f->value_size = atoi(v.c_str());
    else if (k == "--zipf_alpha") f->alpha = atof(v.c_str());
    else if (k == "--seed") f->seed = strtoull(v.c_str(), nullptr, 10);
    else if (k == "--zeroflush") f->zeroflush = (v == "1" || v == "true");
    else if (k == "--partitions") f->partitions = atoi(v.c_str());
    else if (k == "--partition_target_mb") f->partition_target_mb = strtoull(v.c_str(), nullptr, 10);
    else if (k == "--epoch_target_mb") f->epoch_target_mb = strtoull(v.c_str(), nullptr, 10);
    else if (k == "--materialize_parallelism") f->materialize_parallelism = atoi(v.c_str());
    else if (k == "--cache_size") f->cache_size = strtoll(v.c_str(), nullptr, 10);
    else if (k == "--max_background_jobs") f->max_background_jobs = atoi(v.c_str());
    else if (k == "--subcompactions") f->subcompactions = atoi(v.c_str());
    else {
      fprintf(stderr, "unknown flag: %s\n", k.c_str());
      return false;
    }
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  Flags f;
  if (!Parse(argc, argv, &f)) return 1;
  if (f.keyspace == 0 || f.ops == 0 || f.threads <= 0) {
    fprintf(stderr, "invalid --keyspace/--ops/--threads\n");
    return 1;
  }
#ifndef ZIPF_NO_ZF
  if (!f.zeroflush) {
    // 允许在 ZF 构建里跑原生对照（DB::Open）。
  }
#else
  if (f.zeroflush) {
    fprintf(stderr, "this build has no ZeroFlush support (ZIPF_NO_ZF)\n");
    return 1;
  }
#endif

  Zipf zipf;
  zipf.Build(f.keyspace, f.alpha);

  rocksdb::Options opt;
  opt.create_if_missing = true;
  opt.compression = rocksdb::kNoCompression;
  opt.max_background_jobs = f.max_background_jobs;
  opt.max_subcompactions = static_cast<uint32_t>(std::max(1, f.subcompactions));
  opt.statistics = nullptr;
  if (f.cache_size > 0) {
    rocksdb::BlockBasedTableOptions tbo;
    tbo.block_cache = rocksdb::NewLRUCache(static_cast<size_t>(f.cache_size));
    opt.table_factory.reset(rocksdb::NewBlockBasedTableFactory(tbo));
  }

  std::unique_ptr<rocksdb::DB> db;
  rocksdb::Status s;
  if (f.zeroflush) {
#ifndef ZIPF_NO_ZF
    zeroflush::ZeroFlushOptions zfo;
    zfo.v2_fixed_range = true;
    zfo.partitions = f.partitions;
    zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
    for (uint32_t i = 1; i < f.partitions; ++i) {
      zfo.static_boundaries.push_back(
          KeyFromInt(f.keyspace * i / f.partitions, 16));
    }
    zfo.partition_target_bytes = f.partition_target_mb << 20;
    zfo.epoch_target_bytes = f.epoch_target_mb << 20;
    zfo.materialize_parallelism = f.materialize_parallelism;
    zfo.merge_into_base_level = true;
    zfo.value_cache_bytes = 64ull << 20;
    s = zeroflush::Open(opt, zfo, f.db, &db);
#else
    return 1;
#endif
  } else {
    s = rocksdb::DB::Open(opt, f.db, &db);
  }
  if (!s.ok()) {
    fprintf(stderr, "open failed: %s\n", s.ToString().c_str());
    return 1;
  }

  printf("== zipf_bench: engine=%s keyspace=%llu ops=%llu reads=%llu threads=%d "
         "value=%dB alpha=%.3f seed=%llu ==\n",
#ifdef ZIPF_NO_ZF
         "native",
#else
         f.zeroflush ? "zeroflush-v2" : "native(zf-fork)",
#endif
         (unsigned long long)f.keyspace, (unsigned long long)f.ops,
         (unsigned long long)f.reads, f.threads, f.value_size, f.alpha,
         (unsigned long long)f.seed);

  const uint64_t per_thread = f.ops / static_cast<uint64_t>(f.threads) + 1;
  const uint64_t reads_pt = f.reads / static_cast<uint64_t>(f.threads) + 1;
  std::atomic<uint64_t> done{0};
  std::vector<Hist> hists(static_cast<size_t>(f.threads));
  // 进度线程（stderr，每 10s）：长跑可观测（stdout 只在结束时汇总）。
  std::atomic<bool> stop_progress{false};
  std::thread progress([&] {
    const auto start = std::chrono::steady_clock::now();
    uint64_t last = 0;
    while (!stop_progress.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(10));
      if (stop_progress.load()) break;
      const uint64_t cur = done.load();
      const double secs =
          std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
              .count();
      fprintf(stderr,
              "[progress] %.1fs ops=%llu (%.1f%%) avg=%.0f ops/s recent=%.0f "
              "ops/s\n",
              secs, (unsigned long long)cur,
              100.0 * static_cast<double>(cur) / static_cast<double>(f.ops),
              static_cast<double>(cur) / secs,
              static_cast<double>(cur - last) / 10.0);
      last = cur;
    }
  });

  // ---------------- 写阶段（Zipfian） ----------------
  const uint64_t t0 = [] {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
  }();
  {
    std::vector<std::thread> ts;
    for (int t = 0; t < f.threads; ++t) {
      ts.emplace_back([&, t] {
        std::mt19937_64 rng(f.seed * 1000003ull + static_cast<uint64_t>(t));
        rocksdb::WriteOptions wo;
        Hist* h = &hists[static_cast<size_t>(t)];
        std::string val_cache;  // 复用：同一 key 的 value 每次重算代价低
        for (uint64_t i = 0; i < per_thread; ++i) {
          const uint64_t v = zipf.Sample(Uniform(&rng), &rng);
          const std::string key = KeyFromInt(v, 16);
          const std::string val = ValueFor(v, f.value_size);
          auto b = std::chrono::steady_clock::now();
          const rocksdb::Status st = db->Put(wo, key, val);
          auto e = std::chrono::steady_clock::now();
          if (f.histogram) {
            h->Add(static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(e - b)
                    .count()));
          }
          if (!st.ok() && !st.IsIncomplete()) {
            fprintf(stderr, "put error: %s\n", st.ToString().c_str());
            break;
          }
          done.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }
    for (auto& t : ts) t.join();
  }
  const uint64_t t1 = [] {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
  }();
  stop_progress.store(true);
  if (progress.joinable()) progress.join();
  const uint64_t wrote = done.load();
  const double write_secs = static_cast<double>(t1 - t0) / 1e6;
  printf("write: %llu ops in %.3fs = %.0f ops/s (%.1f MB/s)\n",
         (unsigned long long)wrote, write_secs,
         static_cast<double>(wrote) / write_secs,
         static_cast<double>(wrote) * f.value_size / write_secs / 1e6);
  if (f.histogram) {
    Hist all;
    for (auto& h : hists) {
      for (size_t i = 0; i < all.h.size(); ++i) all.h[i] += h.h[i];
    }
    printf("write percentiles (us): P50=%llu P75=%llu P99=%llu P99.9=%llu "
           "P99.99=%llu\n",
           (unsigned long long)all.Pct(0.50), (unsigned long long)all.Pct(0.75),
           (unsigned long long)all.Pct(0.99), (unsigned long long)all.Pct(0.999),
           (unsigned long long)all.Pct(0.9999));
  }

  // ---------------- 读阶段（重放同一键序列 ⇒ 必须全命中且值正确） ----------------
  if (f.reads > 0) {
    std::atomic<uint64_t> found{0}, notfound{0}, mismatch{0}, errs{0};
    std::vector<Hist> rh(static_cast<size_t>(f.threads));
    const uint64_t r0 = [] {
      return static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now().time_since_epoch())
              .count());
    }();
    {
      std::vector<std::thread> ts;
      for (int t = 0; t < f.threads; ++t) {
        ts.emplace_back([&, t] {
          std::mt19937_64 rng(f.seed * 1000003ull + static_cast<uint64_t>(t));
          rocksdb::ReadOptions ro;
          Hist* h = &rh[static_cast<size_t>(t)];
          for (uint64_t i = 0; i < reads_pt; ++i) {
            const uint64_t v = zipf.Sample(Uniform(&rng), &rng);
            const std::string key = KeyFromInt(v, 16);
            std::string got;
            auto b = std::chrono::steady_clock::now();
            const rocksdb::Status st = db->Get(ro, key, &got);
            auto e = std::chrono::steady_clock::now();
            if (f.histogram) {
              h->Add(static_cast<uint64_t>(
                  std::chrono::duration_cast<std::chrono::microseconds>(e - b)
                      .count()));
            }
            if (st.ok()) {
              found.fetch_add(1, std::memory_order_relaxed);
              if (got != ValueFor(v, f.value_size)) {
                mismatch.fetch_add(1, std::memory_order_relaxed);
              }
            } else if (st.IsNotFound()) {
              notfound.fetch_add(1, std::memory_order_relaxed);
            } else {
              errs.fetch_add(1, std::memory_order_relaxed);
            }
          }
        });
      }
      for (auto& t : ts) t.join();
    }
    const uint64_t r1 = [] {
      return static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now().time_since_epoch())
              .count());
    }();
    const uint64_t total_r = found + notfound + mismatch + errs;
    const double rsecs = static_cast<double>(r1 - r0) / 1e6;
    printf("read: %llu ops in %.3fs = %.0f ops/s | found=%llu notfound=%llu "
           "mismatch=%llu errors=%llu\n",
           (unsigned long long)total_r, rsecs,
           static_cast<double>(total_r) / rsecs,
           (unsigned long long)found.load(), (unsigned long long)notfound.load(),
           (unsigned long long)mismatch.load(),
           (unsigned long long)errs.load());
    if (f.histogram) {
      Hist all;
      for (auto& h : rh) {
        for (size_t i = 0; i < all.h.size(); ++i) all.h[i] += h.h[i];
      }
      printf("read percentiles (us): P50=%llu P75=%llu P99=%llu P99.9=%llu\n",
             (unsigned long long)all.Pct(0.50), (unsigned long long)all.Pct(0.75),
             (unsigned long long)all.Pct(0.99),
             (unsigned long long)all.Pct(0.999));
    }
    if (notfound.load() != 0 || mismatch.load() != 0 || errs.load() != 0) {
      printf("VERIFY: FAIL (notfound/mismatch/errors 必须为 0)\n");
    } else {
      printf("VERIFY: PASS (same-key-stream 全命中且值正确)\n");
    }
  }

#ifndef ZIPF_NO_ZF
  if (f.zeroflush) {
    static const char* props[] = {"engine_profile", "pending_materialize_bytes",
                                  "registered_gens", "reclaimed_bytes",
                                  "prof_output_bytes", "prof_tasks",
                                  "prof_merge_us", "csd_device_tasks",
                                  "csd_fallbacks"};
    printf("--- zeroflush props ---\n");
    for (const char* p : props) {
      std::string v;
      if (db->GetProperty(std::string("rocksdb.zeroflush.") + p, &v)) {
        printf("zf.%s : %s\n", p, v.c_str());
      }
    }
    for (int lv = 0; lv <= 3; ++lv) {
      std::string v;
      if (db->GetProperty("rocksdb.num-files-at-level" + std::to_string(lv), &v)) {
        printf("zf.lsm_L%d_files : %s\n", lv, v.c_str());
      }
    }
  }
#endif
  db.reset();
  return 0;
}
