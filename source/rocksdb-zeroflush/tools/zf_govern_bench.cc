//  ZeroFlushV0 M4 动态治理规模验收驱动（zf_govern_bench）。
//
//  目的：偏态（hot-band 前缀倾斜）长跑下对比 dynamic_governance ON/OFF：
//   - P 轨迹采样（partition_count/gov_ticks/gov_splits/gov_merges）；
//   - 写入吞吐（ops/s，墙钟计量）+ 读验证命中率；
//   - 结束时数据完整（迭代计数、抽查读、reopen 一致）。
//
//  用法：
//   zf_govern_bench --db=/tmp/x --governance=0|1
//                   [--rounds=R --writes_per_round=M --value=V --threads=T
//                    --hot=H(0..1) --tick_us=U --seed=S]
//   key 空间 = "k%012d"（0..universe-1），universe = rounds*writes_per_round 的
//   固定上界（写入只覆盖 hot band 与冷区采样，见 Readme/报告）。
//
//  偏态模型：总写入 WRITES = rounds*writes_per_round（可重复命中同 key →
//   覆盖写）。hot 写：P(hot) 的写落在 [0, hot_span)（初始恰在分区 0 区间内，
//   制造速率偏斜）；其余落在 [hot_span, universe)。

#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "zeroflush/zeroflush_db.h"

namespace {

struct Args {
  std::string db;
  bool governance = false;
  uint64_t universe = 8u << 20;      // key 空间大小（写入下标上界）
  uint64_t writes_per_round = 8192;  // 每轮每线程写入数
  int rounds = 16;                   // 轮数
  int threads = 1;                   // 单写线程（见报告：T≥2 并行写触发
                                     // 在途写组计数泄漏，merge/split 屏障超时）
  size_t value_bytes = 512;
  double hot = 0.85;      // hot 写占比
  double hot_span = 0.12; // hot 键带宽度（universe 比例，初始在分区 0 内）
  uint64_t tick_us = 20 * 1000;  // 后台控制器 tick（governance=1）
  uint64_t seed = 20260909;
  int sample_ms = 250;  // P 轨迹采样间隔
  int gap_ms = 4000;    // 排空检查点间隙（0 = 连续写，验证活跃尾 split）
  uint32_t partitions = 8;   // kStatic 初始分区数（等宽边界）
  bool active_tail = true;   // 活跃尾 split（与 ZeroFlushOptions 默认一致）
};

std::string Key(uint64_t id) {
  char buf[32];
  snprintf(buf, sizeof(buf), "k%012" PRIu64, id);
  return std::string(buf);
}

std::string MakeValue(uint64_t seed, size_t bytes) {
  std::string v(bytes, 'x');
  snprintf(&v[0], 24, "gb%018" PRIu64, seed);
  return v;
}

uint64_t ZfMetric(rocksdb::DB* db, const std::string& name) {
  std::string val;
  if (!db->GetProperty("rocksdb.zeroflush." + name, &val)) return 0;
  return strtoull(val.c_str(), nullptr, 10);
}

}  // namespace

int main(int argc, char** argv) {
  Args a;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto val = [&](const std::string& k) {
      return arg.compare(0, k.size() + 1, k + "=") == 0
                 ? arg.substr(k.size() + 1)
                 : std::string();
    };
    if (arg.rfind("--db=", 0) == 0) a.db = val("--db");
    else if (arg.rfind("--governance=", 0) == 0) a.governance = val("--governance") == "1";
    else if (arg.rfind("--universe=", 0) == 0) a.universe = strtoull(val("--universe").c_str(), nullptr, 10);
    else if (arg.rfind("--writes_per_round=", 0) == 0) a.writes_per_round = strtoull(val("--writes_per_round").c_str(), nullptr, 10);
    else if (arg.rfind("--rounds=", 0) == 0) a.rounds = atoi(val("--rounds").c_str());
    else if (arg.rfind("--threads=", 0) == 0) a.threads = atoi(val("--threads").c_str());
    else if (arg.rfind("--value=", 0) == 0) a.value_bytes = static_cast<size_t>(atol(val("--value").c_str()));
    else if (arg.rfind("--hot=", 0) == 0) a.hot = atof(val("--hot").c_str());
    else if (arg.rfind("--hot_span=", 0) == 0) a.hot_span = atof(val("--hot_span").c_str());
    else if (arg.rfind("--tick_us=", 0) == 0) a.tick_us = strtoull(val("--tick_us").c_str(), nullptr, 10);
    else if (arg.rfind("--seed=", 0) == 0) a.seed = strtoull(val("--seed").c_str(), nullptr, 10);
    else if (arg.rfind("--sample_ms=", 0) == 0) a.sample_ms = atoi(val("--sample_ms").c_str());
    else if (arg.rfind("--gap_ms=", 0) == 0) a.gap_ms = atoi(val("--gap_ms").c_str());
    else if (arg.rfind("--partitions=", 0) == 0) a.partitions = static_cast<uint32_t>(atoi(val("--partitions").c_str()));
    else if (arg.rfind("--active_tail=", 0) == 0) a.active_tail = val("--active_tail") == "1";
  }
  if (a.db.empty()) {
    fprintf(stderr, "usage: zf_govern_bench --db=PATH --governance=0|1 [opts]\n");
    return 2;
  }
  const uint64_t hot_span = static_cast<uint64_t>(a.universe * a.hot_span);
  // 等宽 kStatic 边界（8 分区；hot band [0, hot_span) 初始落在分区 0）。
  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = a.partitions;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  for (uint32_t i = 1; i < a.partitions; ++i) {
    char b[32];
    snprintf(b, sizeof(b), "k%012" PRIu64, a.universe * i / a.partitions);
    zfo.static_boundaries.emplace_back(b);
  }
  zfo.partition_target_bytes = 8 << 20;   // 8MB/分区封存
  zfo.epoch_target_bytes = 64u << 20;
  zfo.skip_batching = false;  // 物化每 epoch 直装，无 skip/recovery 滞留
                              //（排空检查点确定性；与历史 perf 口径一致）
  zfo.dynamic_governance = a.governance;
  zfo.governance_tick_interval_micros = a.tick_us;
  zfo.governance_p_min = 4;
  zfo.governance_p_max = 256;
  zfo.governance_split_min_bytes = 4u << 20;   // 4MB base 即拆（规模验收口径）
  zfo.governance_merge_max_bytes = 4u << 20;
  zfo.governance_confirm_windows = 3;
  zfo.governance_cooldown_micros = 1;          // 快测（验收意图=调度收敛性）
  zfo.governance_cooldown_max_micros = 1;
  zfo.governance_ewma_fast_secs = 5;
  zfo.governance_ewma_slow_secs = 15;
  zfo.governance_merge_idle_micros = 100ull << 30;  // 本验收只观察活跃期拆分
  zfo.governance_active_tail_split = a.active_tail;

  rocksdb::Options opt;
  opt.create_if_missing = true;
  opt.compression = rocksdb::kNoCompression;
  // 清库
  std::string rm = "rm -rf '" + a.db + "'";
  std::system(rm.c_str());

  std::unique_ptr<rocksdb::DB> db;
  rocksdb::Status s = zeroflush::Open(opt, zfo, a.db, &db);
  if (!s.ok()) {
    fprintf(stderr, "open failed: %s\n", s.ToString().c_str());
    return 1;
  }

  const uint64_t rounds_total = static_cast<uint64_t>(a.rounds) * a.writes_per_round * a.threads;
  fprintf(stderr,
          "[zf_govern_bench] gov=%d db=%s threads=%d writes=%" PRIu64
          " value=%zu universe=%" PRIu64 " hot=%.2f span=%" PRIu64 "\n",
          a.governance ? 1 : 0, a.db.c_str(), a.threads, rounds_total,
          a.value_bytes, a.universe, a.hot, hot_span);

  // 采样线程：P 轨迹 + gov 计数（每 sample_ms）。
  std::atomic<bool> done{false};
  std::atomic<uint64_t> total_writes{0};
  std::thread sampler([&] {
    while (!done.load(std::memory_order_relaxed)) {
      const uint64_t pc = ZfMetric(db.get(), "partition_count");
      const uint64_t tk = ZfMetric(db.get(), "gov_ticks");
      const uint64_t sp = ZfMetric(db.get(), "gov_splits");
      const uint64_t mg = ZfMetric(db.get(), "gov_merges");
      const uint64_t w = total_writes.load(std::memory_order_relaxed);
      fprintf(stderr,
              "[sampler] w=%" PRIu64 " P=%" PRIu64 " ticks=%" PRIu64
              " splits=%" PRIu64 " merges=%" PRIu64
              " inflight=%" PRIu64 "\n",
              w, pc, tk, sp, mg, ZfMetric(db.get(), "inflight_groups"));
      // ZF_BENCH_VERBOSE=1：卡滞现场诊断（写停/后台作业/层级/物化积压）。
      if (getenv("ZF_BENCH_VERBOSE") != nullptr) {
        auto prop = [&](const char* name) {
          std::string v;
          db->GetProperty(name, &v);
          if (v.size() > 220) v.resize(220);
          return v;
        };
        fprintf(stderr,
                "[v] stopped=%s delayed=%llu comp=%s flush=%s "
                "live_wal=%llu sealed_wal=%llu skip=%llu base_rw=%llu "
                "merge_us=%llu out_bytes=%llu tasks=%llu\n",
                prop("rocksdb.is-write-stopped").c_str(),
                (unsigned long long)ZfMetric(db.get(), "delayed_write_rate"),
                prop("rocksdb.num-running-compactions").c_str(),
                prop("rocksdb.num-running-flushes").c_str(),
                (unsigned long long)ZfMetric(db.get(), "live_wal_bytes"),
                (unsigned long long)ZfMetric(db.get(), "sealed_wal_bytes"),
                (unsigned long long)ZfMetric(db.get(), "skip_count"),
                (unsigned long long)ZfMetric(db.get(), "base_merge_rewritten_bytes"),
                (unsigned long long)ZfMetric(db.get(), "prof_merge_us"),
                (unsigned long long)ZfMetric(db.get(), "prof_output_bytes"),
                (unsigned long long)ZfMetric(db.get(), "prof_tasks"));
        std::string ls;
        if (db->GetProperty("rocksdb.levelstats", &ls)) {
          std::string one;
          for (char c : ls) {
            if (c == '\n') {
              fprintf(stderr, "[level] %s\n", one.c_str());
              one.clear();
            } else {
              one.push_back(c);
            }
          }
          if (!one.empty()) fprintf(stderr, "[level] %s\n", one.c_str());
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(a.sample_ms));
    }
  });

  // 物化收敛等待（突发写间隙的排空检查点：sealed==materialized 稳定）。
  auto wait_settled = [&]() {
    int stable = 0;
    uint64_t last_m = 0, last_s = 0;
    for (int i = 0; i < 4000; ++i) {
      const uint64_t m = ZfMetric(db.get(), "epochs_materialized");
      const uint64_t s = ZfMetric(db.get(), "epochs_sealed");
      if (m == last_m && s == last_s && m == s) {
        if (++stable >= 3) return true;
      } else {
        stable = 0;
      }
      last_m = m;
      last_s = s;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
  };

  // 突发写 + 排空间隙（rounds 轮）：写 phase 全部并发达标后进入检查点间隙，
  // 让后台治理在 quiescent 窗口内完成活跃尾冻结→排空→中位数切分。
  std::atomic<uint64_t> next{0};
  const uint64_t per_round = a.writes_per_round * static_cast<uint64_t>(a.threads);
  const auto t0 = std::chrono::steady_clock::now();
  for (int round = 0; round < a.rounds; ++round) {
    next.store(round * per_round, std::memory_order_relaxed);
    std::vector<std::thread> writers;
    for (int t = 0; t < a.threads; ++t) {
      writers.emplace_back([&, tid = static_cast<uint64_t>(t)] {
        std::mt19937_64 rng(a.seed + tid * 7919 + round * 104729);
        std::uniform_real_distribution<double> ur(0.0, 1.0);
        rocksdb::WriteOptions wo;
        while (true) {
          const uint64_t idx =
              next.fetch_add(1, std::memory_order_relaxed);
          if (idx >= (round + 1) * per_round) break;
          uint64_t id;
          if (ur(rng) < a.hot) {
            id = static_cast<uint64_t>(rng() % hot_span);  // 热键带
          } else {
            id = hot_span +
                 static_cast<uint64_t>(rng() % (a.universe - hot_span));
          }
          const std::string k = Key(id);
          const std::string v =
              MakeValue((tid << 56) ^ (static_cast<uint64_t>(round) << 40) ^ idx,
                        a.value_bytes);
          rocksdb::Status ps = db->Put(wo, k, v);
          if (!ps.ok()) {
            fprintf(stderr, "put failed @%s: %s\n", k.c_str(),
                    ps.ToString().c_str());
            std::exit(3);
          }
          total_writes.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }
    for (auto& t : writers) t.join();
    // 排空检查点：等封存全部物化（消除 pending）→ quiescent 间隙供治理执行。
    if (!wait_settled()) {
      fprintf(stderr, "[zf_govern_bench] WARN: materialization did not settle "
                      "after round %d\n", round);
    }
    if (a.gap_ms > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(a.gap_ms));
    }
  }
  const auto t1 = std::chrono::steady_clock::now();
  const double wall_s = std::chrono::duration<double>(t1 - t0).count();

  // 让后台物化/治理收敛：P/splits/merges/rewrite 连续 2s 不变（上限 20s）。
  {
    uint64_t lp = ~0ull, ls = ~0ull, lm = ~0ull, lw = ~0ull;
    int stable = 0;
    for (int i = 0; i < 200 && stable < 10; ++i) {
      const uint64_t pc = ZfMetric(db.get(), "partition_count");
      const uint64_t sp = ZfMetric(db.get(), "gov_splits");
      const uint64_t mg = ZfMetric(db.get(), "gov_merges");
      const uint64_t rw = ZfMetric(db.get(), "rewrite_bytes");
      const uint64_t iaf = ZfMetric(db.get(), "gov_in_flight");
      if (iaf != 0) {
        stable = 0;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        continue;
      }
      if (pc == lp && sp == ls && mg == lm && rw == lw) {
        ++stable;
      } else {
        stable = 0;
      }
      lp = pc;
      ls = sp;
      lm = mg;
      lw = rw;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
  // 停止后台控制器（保持 DB 打开）→ 指标/P 冻结，后续测量不再被改动。
  const rocksdb::Status stop_s = zeroflush::StopGovernance(db.get());
  if (!stop_s.ok()) {
    fprintf(stderr, "[result] StopGovernance failed: %s\n",
            stop_s.ToString().c_str());
  }
  done.store(true, std::memory_order_relaxed);
  sampler.join();

  const uint64_t pc_end = ZfMetric(db.get(), "partition_count");
  const uint64_t splits = ZfMetric(db.get(), "gov_splits");
  const uint64_t merges = ZfMetric(db.get(), "gov_merges");
  const uint64_t ticks = ZfMetric(db.get(), "gov_ticks");
  const uint64_t rw = ZfMetric(db.get(), "rewrite_bytes");
  fprintf(stderr,
          "[result] wall_s=%.2f writes=%" PRIu64 " ops_s=%.1f P=%" PRIu64
          " ticks=%" PRIu64 " splits=%" PRIu64 " merges=%" PRIu64
          " rewrite_bytes=%" PRIu64 "\n",
          wall_s, rounds_total, static_cast<double>(rounds_total) / wall_s,
          pc_end, ticks, splits, merges, rw);
  // M5 诊断：拆分准入分解（attempts/gate_base/median_nofiles/success +
  // 最近候选 base/L0 字节）。
  fprintf(stderr,
          "[diag] split_attempts=%" PRIu64 " gate_base=%" PRIu64
          " median_nofiles=%" PRIu64 " success=%" PRIu64
          " last_base=%" PRIu64 " last_l0=%" PRIu64
          " rewrite_gated=%" PRIu64 "\n",
          ZfMetric(db.get(), "gov_split_attempts"),
          ZfMetric(db.get(), "gov_split_gate_base"),
          ZfMetric(db.get(), "gov_split_median_nofiles"),
          ZfMetric(db.get(), "gov_split_success"),
          ZfMetric(db.get(), "gov_last_split_base"),
          ZfMetric(db.get(), "gov_last_split_l0"),
          ZfMetric(db.get(), "gov_rewrite_gated"));
  fprintf(stderr,
          "[diag2] busy=%" PRIu64 " timeout=%" PRIu64 " other=%" PRIu64
          " median_notfound=%" PRIu64 "\n",
          ZfMetric(db.get(), "gov_split_busy"),
          ZfMetric(db.get(), "gov_split_timeout"),
          ZfMetric(db.get(), "gov_split_other"),
          ZfMetric(db.get(), "gov_split_median_notfound"));
  fprintf(stderr, "[diag3] pos_gone=%" PRIu64 " total0=%" PRIu64 "\n",
          ZfMetric(db.get(), "gov_split_pos_gone"),
          ZfMetric(db.get(), "gov_split_total0"));
  fprintf(stderr, "[diag4] predrain=%" PRIu64 "\n",
          ZfMetric(db.get(), "gov_split_predrain"));

  // 读验证：热/冷两区各采样（读自身写入过的值域）。
  uint64_t found = 0, read_total = 0;
  std::mt19937_64 rrng(a.seed + 1234567);
  std::uniform_real_distribution<double> ur2(0.0, 1.0);
  rocksdb::ReadOptions ro;
  for (uint64_t i = 0; i < 200000; ++i) {
    uint64_t id;
    if (ur2(rrng) < a.hot) {
      id = static_cast<uint64_t>(rrng() % hot_span);
    } else {
      id = hot_span + static_cast<uint64_t>(rrng() % (a.universe - hot_span));
    }
    std::string out;
    if (db->Get(ro, Key(id), &out).ok()) {
      ++found;
    }
    ++read_total;
  }
  fprintf(stderr, "[result] reads=%" PRIu64 " found=%" PRIu64 " hit=%.4f\n",
          read_total, found, static_cast<double>(found) / read_total);

  // 迭代计数（live 键近似：冷区有部分未写）。
  int64_t iter_cnt = 0;
  {
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(ro));
    it->SeekToFirst();
    for (; it->Valid(); it->Next()) ++iter_cnt;
  }
  fprintf(stderr, "[result] iter_entries=%" PRId64 "\n", iter_cnt);

  db.reset();
  // 校验用 reopen：关闭后台治理（否则重启的控制器会在读数前改动 P）。
  zeroflush::ZeroFlushOptions zfo_verify = zfo;
  zfo_verify.dynamic_governance = false;
  s = zeroflush::Open(opt, zfo_verify, a.db, &db);
  if (!s.ok()) {
    fprintf(stderr, "reopen failed: %s\n", s.ToString().c_str());
    return 1;
  }
  const uint64_t pc_reopen = ZfMetric(db.get(), "partition_count");
  if (pc_reopen < zfo.governance_p_min || pc_reopen > zfo.governance_p_max) {
    fprintf(stderr, "[result] reopen P out of bounds (%" PRIu64 ")\n",
            pc_reopen);
    return 1;
  }
  fprintf(stderr, "[result] reopen_ok P=%" PRIu64 " (pre_close P=%" PRIu64
                  ")\n",
          pc_reopen, pc_end);
  db.reset();
  return 0;
}
