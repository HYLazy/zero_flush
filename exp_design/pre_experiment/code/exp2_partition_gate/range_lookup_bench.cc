// range_lookup_bench.cc - ZeroFlush 前期实验 2：分区开销门（Part 1）
//
// 测量 L1 边界 range lookup 的两种实现延迟：
//   - binary_search : 有序边界数组上的二分查找（连续内存，cache-line 友好）
//   - small_btree   : fanout=8 的小 B-tree，节点 alignas(64)
//
// key 空间：16B key（解释为大端数值序），均匀划分为 P 个 range，
// 边界数组即 L1 边界模拟。查询 key 由 zipfian(theta=0.99) 生成。
// 每组合执行 --queries 次测量查询，前 --warmup 次为预热（丢弃不统计）。
// rdtsc 计时（timing.h 校准 cycles->ns），latency.h 统计 mean/p99。
// 测量线程用 pthread_setaffinity_np 绑核。
//
// 纯 POSIX + 标准库，不链接 RocksDB。
//
// 用法：
//   range_lookup_bench --p 64 --impl binary_search --reps 3
//       --queries 10000000 --warmup 1000000 --cpu 3 --out run.json

#include <pthread.h>
#include <sched.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/cpp/json_out.h"
#include "common/cpp/latency.h"
#include "common/cpp/timing.h"
#include "common/cpp/zipfian.h"

using pre_exp::JsonWriter;
using pre_exp::LatencyRecorder;
using pre_exp::LatencyShard;
using pre_exp::ZipfianSampler;

namespace {

// ---------------- 16B key（大端数值序） ----------------
struct Key16 {
  uint8_t b[16];
};

// 将数值 v 编码为 16B 大端 key（v 放在低 8 字节，大端序 -> memcmp 序 == 数值序）
inline Key16 MakeKey(uint64_t v) {
  Key16 k{};
  for (int i = 0; i < 8; ++i) {
    k.b[8 + i] = static_cast<uint8_t>(v >> (56 - 8 * i));
  }
  return k;
}

inline int CmpKey16(const Key16& a, const Key16& b) {
  return std::memcmp(a.b, b.b, 16);
}

// ---------------- binary_search 实现 ----------------
// 连续数组上的二分：boundaries 为 P-1 个分隔边界（range j+1 的起点）。
// 返回 key 所属分区下标 [0, P)。
class BinarySearchIndex {
 public:
  explicit BinarySearchIndex(std::vector<Key16> seps)
      : seps_(std::move(seps)) {}

  inline uint64_t Lookup(const Key16& k) const {
    // upper_bound：第一个 > k 的分隔边界位置 = 分区下标
    size_t lo = 0, hi = seps_.size();
    while (lo < hi) {
      const size_t mid = lo + (hi - lo) / 2;
      if (CmpKey16(seps_[mid], k) <= 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

 private:
  std::vector<Key16> seps_;
};

// ---------------- small_btree 实现 ----------------
// fanout=8 的静态 B-tree，节点 64B 对齐。叶子保存一段连续分隔边界，
// 内部节点用组间最大边界路由。P<=256 时树深 <=3。
class SmallBtreeIndex {
 public:
  static constexpr int kFanout = 8;

  struct alignas(64) Node {
    int nkeys = 0;             // 内部节点: 路由键数；叶子: 边界数
    bool is_leaf = false;
    uint32_t base = 0;         // 叶子: 该叶首边界的全局分区下标
    Key16 keys[kFanout];
    Node* children[kFanout + 1] = {nullptr};
  };

  explicit SmallBtreeIndex(const std::vector<Key16>& seps) {
    root_ = Build(seps, 0, seps.size(), 0);
  }

  ~SmallBtreeIndex() { Free(root_); }

  inline uint64_t Lookup(const Key16& k) const {
    const Node* n = root_;
    while (!n->is_leaf) {
      int i = 0;
      while (i < n->nkeys && CmpKey16(n->keys[i], k) <= 0) ++i;
      n = n->children[i];
    }
    // 叶子内线性 upper_bound（<=8 个键，分支预测友好）
    int i = 0;
    while (i < n->nkeys && CmpKey16(n->keys[i], k) <= 0) ++i;
    return n->base + static_cast<uint32_t>(i);
  }

  int Depth() const {
    int d = 1;
    for (const Node* n = root_; n && !n->is_leaf; n = n->children[0]) ++d;
    return d;
  }

 private:
  Node* Build(const std::vector<Key16>& seps, size_t lo, size_t hi,
              uint32_t base) {
    Node* node = new Node();
    const size_t len = hi - lo;
    if (len <= static_cast<size_t>(kFanout)) {
      node->is_leaf = true;
      node->base = base;
      node->nkeys = static_cast<int>(len);
      for (size_t i = 0; i < len; ++i) node->keys[i] = seps[lo + i];
      return node;
    }
    // 均分为 kFanout 组，递归建子树；组间分隔 = 各组末边界
    node->is_leaf = false;
    const size_t chunk = (len + kFanout - 1) / kFanout;
    int g = 0;
    for (size_t off = lo; off < hi; off += chunk, ++g) {
      const size_t end = std::min(off + chunk, hi);
      node->children[g] = Build(seps, off, end, base + (off - lo));
      if (g < kFanout - 1 && end < hi) {
        node->keys[node->nkeys++] = seps[end - 1];
      }
    }
    return node;
  }

  void Free(Node* n) {
    if (!n) return;
    if (!n->is_leaf) {
      for (int i = 0; i <= n->nkeys; ++i) Free(n->children[i]);
    }
    delete n;
  }

  Node* root_ = nullptr;
};

// ---------------- 工具 ----------------
std::string ReadFirstLine(const std::string& path) {
  std::ifstream f(path);
  std::string s;
  if (f) std::getline(f, s);
  return s;
}

bool PinToCpu(int cpu) {
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
}

// 干扰控制：等待系统 CPU 空闲（load1 阈值）后再测延迟，最多等 cap_s
// 采样 /proc/stat 两次（间隔 1s），返回整机 CPU busy 占比
// （可捕捉 loadavg 尚未衰减的间隙期其他基准任务）
double CpuBusyFraction() {
  auto read_cpu = [](uint64_t* total, uint64_t* idle) -> bool {
    std::ifstream f("/proc/stat");
    std::string tag;
    if (!(f >> tag) || tag != "cpu") return false;
    uint64_t v = 0, sum = 0, idle_v = 0;
    int idx = 0;
    while (f >> v) {
      sum += v;
      if (idx == 3) idle_v = v;  // user nice system idle ...
      ++idx;
    }
    *total = sum;
    *idle = idle_v;
    return true;
  };
  uint64_t t1 = 0, i1 = 0, t2 = 0, i2 = 0;
  if (!read_cpu(&t1, &i1)) return 0.0;
  ::usleep(1000 * 1000);
  if (!read_cpu(&t2, &i2)) return 0.0;
  if (t2 <= t1) return 0.0;
  double busy = 1.0 - static_cast<double>(i2 - i1) /
                          static_cast<double>(t2 - t1);
  if (busy < 0.0) busy = 0.0;
  return busy;
}

void WaitForCpuIdle(double load_thr, uint64_t cap_s) {
  const uint64_t t0 = pre_exp::clock_gettime_ns();
  for (;;) {
    std::ifstream f("/proc/loadavg");
    double load1 = 0.0;
    if (f) f >> load1;
    const double busy = CpuBusyFraction();  // 内含 1s 采样窗口
    if (load1 <= load_thr && busy <= 0.10) break;
    const uint64_t waited_s =
        (pre_exp::clock_gettime_ns() - t0) / 1000000000ULL;
    if (waited_s >= cap_s) {
      std::fprintf(stderr,
                   "[wait] cpu-idle wait capped at %llu s "
                   "(load=%.2f busy=%.2f)\n",
                   (unsigned long long)waited_s, load1, busy);
      break;
    }
    ::usleep(200 * 1000);
  }
}

struct Args {
  int p = 64;
  std::string impl = "binary_search";
  int reps = 3;
  uint64_t queries = 10000000;
  uint64_t warmup = 1000000;
  int cpu = 3;
  uint64_t seed = 42;
  std::string out;
  bool no_wait = false;
  uint64_t wait_cap_s = 7200;
};

void ParseArgs(int argc, char** argv, Args* a) {
  for (int i = 1; i < argc; ++i) {
    std::string k = argv[i];
    auto next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : ""; };
    if (k == "--p") a->p = std::atoi(next().c_str());
    else if (k == "--impl") a->impl = next();
    else if (k == "--reps") a->reps = std::atoi(next().c_str());
    else if (k == "--queries") a->queries = std::strtoull(next().c_str(), nullptr, 10);
    else if (k == "--warmup") a->warmup = std::strtoull(next().c_str(), nullptr, 10);
    else if (k == "--cpu") a->cpu = std::atoi(next().c_str());
    else if (k == "--seed") a->seed = std::strtoull(next().c_str(), nullptr, 10);
    else if (k == "--out") a->out = next();
    else if (k == "--no-wait") a->no_wait = true;
    else if (k == "--wait-cap") a->wait_cap_s = std::strtoull(next().c_str(), nullptr, 10);
    else { std::fprintf(stderr, "unknown arg: %s\n", k.c_str()); std::exit(2); }
  }
}

}  // namespace

int main(int argc, char** argv) {
  Args args;
  ParseArgs(argc, argv, &args);
  if (args.out.empty()) {
    std::fprintf(stderr, "--out required\n");
    return 2;
  }
  if (args.p < 2) { std::fprintf(stderr, "p must >= 2\n"); return 2; }

  pre_exp::CalibrateTsc();
  if (!PinToCpu(args.cpu)) {
    std::fprintf(stderr, "warn: pin to cpu %d failed\n", args.cpu);
  }
  if (!args.no_wait) {
    WaitForCpuIdle(std::max(2.0, 0.15 * std::thread::hardware_concurrency()),
                   args.wait_cap_s);
  }

  // 边界生成：key 空间 [0, 2^24) 均匀划分为 P 个 range，
  // 分隔边界 = range j 的起点（j=1..P-1），即 L1 边界模拟。
  constexpr uint64_t kKeyspaceN = 1ULL << 24;
  std::vector<Key16> seps;
  seps.reserve(args.p - 1);
  for (int j = 1; j < args.p; ++j) {
    seps.push_back(MakeKey(kKeyspaceN * static_cast<uint64_t>(j) /
                           static_cast<uint64_t>(args.p)));
  }

  BinarySearchIndex bs_idx(seps);
  SmallBtreeIndex bt_idx(seps);

  // 预生成查询序列（zipfian theta=0.99）：warmup + queries 个 key 数值。
  // 预生成使测量循环只包含 range lookup 本身，不混入采样开销。
  const uint64_t total_ops = args.warmup + args.queries;
  std::vector<uint64_t> key_seq(total_ops);
  {
    ZipfianSampler sampler(kKeyspaceN, 0.99, args.seed);
    for (uint64_t i = 0; i < total_ops; ++i) key_seq[i] = sampler.Next();
  }

  const bool use_btree = (args.impl == "small_btree");
  if (args.impl != "binary_search" && !use_btree) {
    std::fprintf(stderr, "unknown impl: %s\n", args.impl.c_str());
    return 2;
  }

  JsonWriter w;
  w.BeginObject();
  w.Key("experiment").String("exp2_partition_gate_range_lookup");
  w.Key("p").Number(static_cast<int64_t>(args.p));
  w.Key("impl").String(args.impl);
  w.Key("queries").Number(args.queries);
  w.Key("warmup").Number(args.warmup);
  w.Key("reps").Number(static_cast<int64_t>(args.reps));
  w.Key("seed").Number(args.seed);
  w.Key("keyspace_n").Number(kKeyspaceN);
  w.Key("theta").Number(0.99);
  w.Key("cpu_pinned").Number(static_cast<int64_t>(args.cpu));
  w.Key("tsc_freq_hz").Number(pre_exp::tsc_freq_hz());
  w.Key("cpu_governor").String(ReadFirstLine(
      "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"));
  if (use_btree) w.Key("btree_depth").Number(static_cast<int64_t>(bt_idx.Depth()));

  w.Key("runs").BeginArray();
  uint64_t sink = 0;
  for (int rep = 0; rep < args.reps; ++rep) {
    LatencyRecorder rec;
    LatencyShard shard;
    shard.Reserve(args.queries);

    for (uint64_t i = 0; i < total_ops; ++i) {
      const Key16 k = MakeKey(key_seq[i]);
      const uint64_t t0 = pre_exp::rdtsc();
      const uint64_t idx =
          use_btree ? bt_idx.Lookup(k) : bs_idx.Lookup(k);
      const uint64_t t1 = pre_exp::rdtsc();
      sink ^= idx;
      if (i >= args.warmup) {
        shard.Record(pre_exp::tsc_to_ns(t1 - t0));
      }
    }
    rec.AddShard(&shard);
    auto st = rec.ComputeStats();

    w.BeginObject();
    w.Key("rep").Number(static_cast<int64_t>(rep));
    w.Key("count").Number(st.count);
    w.Key("mean_ns").Number(st.mean);
    w.Key("p50_ns").Number(st.p50);
    w.Key("p99_ns").Number(st.p99);
    w.Key("min_ns").Number(st.min);
    w.Key("max_ns").Number(st.max);
    w.EndObject();
    std::fprintf(stderr,
                 "[range_lookup] p=%d impl=%s rep=%d mean=%.2fns p99=%lluns\n",
                 args.p, args.impl.c_str(), rep, st.mean,
                 (unsigned long long)st.p99);
  }
  w.EndArray();
  w.Key("sink").Number(sink);  // 防止查找结果被优化掉
  w.EndObject();

  std::ofstream f(args.out);
  if (!f) {
    std::fprintf(stderr, "cannot open out: %s\n", args.out.c_str());
    return 1;
  }
  f << w.Dump() << "\n";
  return 0;
}
