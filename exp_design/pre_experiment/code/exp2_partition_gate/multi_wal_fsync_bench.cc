// multi_wal_fsync_bench.cc - ZeroFlush 前期实验 2：分区开销门（Part 2）
//
// harness：P 个 WAL 文件（fallocate 预分配），每个 Put 先做 range lookup
// 路由到对应分区文件，然后追加写入（buffered write + fsync，与 RocksDB
// WAL 语义一致）。记录格式：定长 record = [uint32 长度头][16B key][value]。
//
// 组提交模型：W 个写线程（per-thread buffer）+ 单一 fsync/commit 线程，
// fsync 串行开销集中在 commit 线程，不直接计入写线程延迟。
//
// 模式：
//   A (independent_fsync)：每个分区文件独立参与组提交，每次 commit 轮
//       （每 base_batch ops 一轮）对每个 dirty 文件各自 fsync。
//   B (batched_fsync)：所有 dirty 分区文件统一批量 fsync；commit 轮放大
//       为 base_batch*P ops，用更大的组摊销每文件 fsync 开销；记录
//       durability_window（最早未落盘记录的写入时刻 -> fsync 完成时刻）。
//
// ops 数量自适应：先 pilot 估速率，再按目标时长 >=10s 推算 ops
// （sync=true: [5万, 50万]；sync=false: [200万, 500万]），理由记入 JSON。
//
// 干扰控制：运行前 sync() 落盘 + 检查 /proc/loadavg 与 nvme0n1 in-flight
// IO，忙则等待；记录内核版本、挂载选项、IO 调度器。
//
// 纯 POSIX + 标准库，不链接 RocksDB。
//
// 用法：
//   multi_wal_fsync_bench --p 64 --mode B --sync true --outdir DIR
//       --out run.json [--ops N] [--writers 4] [--base-batch 512]
//       [--cpu-start 4] [--seed 42] [--no-wait]

#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "common/cpp/json_out.h"
#include "common/cpp/latency.h"
#include "common/cpp/timing.h"
#include "common/cpp/zipfian.h"

using pre_exp::JsonWriter;

namespace {

constexpr uint64_t kKeyspaceN = 1ULL << 24;  // 与 range_lookup_bench 一致

// ---------------- 16B key 与路由（binary search，独立实现） ---------------
struct Key16 {
  uint8_t b[16];
};

inline Key16 MakeKey(uint64_t v) {
  Key16 k{};
  for (int i = 0; i < 8; ++i) {
    k.b[8 + i] = static_cast<uint8_t>(v >> (56 - 8 * i));
  }
  return k;
}

class RangeRouter {
 public:
  explicit RangeRouter(int p) {
    for (int j = 1; j < p; ++j) {
      seps_.push_back(MakeKey(kKeyspaceN * static_cast<uint64_t>(j) /
                              static_cast<uint64_t>(p)));
    }
  }
  inline uint32_t Route(const Key16& k) const {
    size_t lo = 0, hi = seps_.size();
    while (lo < hi) {
      const size_t mid = lo + (hi - lo) / 2;
      if (std::memcmp(seps_[mid].b, k.b, 16) <= 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return static_cast<uint32_t>(lo);
  }

 private:
  std::vector<Key16> seps_;
};

// ---------------- 系统信息 ----------------
std::string ReadFirstLine(const std::string& path) {
  std::ifstream f(path);
  std::string s;
  if (f) std::getline(f, s);
  return s;
}

std::string KernelVersion() {
  struct utsname u {};
  if (uname(&u) == 0) return u.release;
  return "";
}

std::string MountOptionsOf(const std::string& dir) {
  std::string cmd = "findmnt -n -o OPTIONS -T '" + dir + "' 2>/dev/null";
  FILE* fp = ::popen(cmd.c_str(), "r");
  if (!fp) return "";
  std::string out;
  char buf[512];
  while (fgets(buf, sizeof(buf), fp)) out += buf;
  ::pclose(fp);
  while (!out.empty() && (out.back() == '\n' || out.back() == ' '))
    out.pop_back();
  return out;
}

std::string IoScheduler() {
  std::string s = ReadFirstLine("/sys/block/nvme0n1/queue/scheduler");
  // 形如 "[none] mq-deadline"，提取 [..] 中当前值
  auto l = s.find('['), r = s.find(']');
  if (l != std::string::npos && r != std::string::npos && r > l)
    return s.substr(l + 1, r - l - 1);
  return s;
}

bool PinToCpu(int cpu) {
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
}

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

// 干扰控制：等待系统空闲（load1 阈值 + CPU busy 占比 + nvme in-flight
// + 脏页量），最多等 wait_cap_ms
uint64_t WaitForIdle(int ncpu, uint64_t wait_cap_ms) {
  const double load_thr = std::max(2.0, 0.15 * ncpu);
  const uint64_t t0 = pre_exp::clock_gettime_ns();
  for (;;) {
    std::ifstream f("/proc/loadavg");
    double load1 = 0.0;
    if (f) f >> load1;
    const double busy = CpuBusyFraction();  // 内含 1s 采样窗口
    bool io_busy = false;
    {
      std::ifstream ds("/proc/diskstats");
      std::string line;
      while (std::getline(ds, line)) {
        if (line.find("nvme0n1 ") == std::string::npos) continue;
        std::istringstream iss(line);
        // 字段 9（第 9 个数值）为 in-flight IO
        std::string tok;
        int col = 0;
        long inflight = 0;
        while (iss >> tok) {
          ++col;
          if (col == 12) {  // major minor name 之后第 9 个数值列
            inflight = std::atol(tok.c_str());
            break;
          }
        }
        if (inflight > 8) io_busy = true;
        break;
      }
    }
    // 脏页量：其他任务遗留的回写会直接干扰 fsync 延迟测量
    uint64_t dirty_kb = 0;
    {
      std::ifstream mi("/proc/meminfo");
      std::string line;
      while (std::getline(mi, line)) {
        if (line.rfind("Dirty:", 0) == 0) {
          dirty_kb = std::strtoull(line.c_str() + 6, nullptr, 10);
          break;
        }
      }
    }
    if (load1 <= load_thr && busy <= 0.10 && !io_busy &&
        dirty_kb <= 512 * 1024)
      break;
    const uint64_t waited_ms =
        (pre_exp::clock_gettime_ns() - t0) / 1000000ULL;
    if (waited_ms >= wait_cap_ms) {
      std::fprintf(stderr,
                   "[wait] idle-wait capped at %llu ms "
                   "(load=%.2f busy=%.2f)\n",
                   (unsigned long long)waited_ms, load1, busy);
      break;
    }
    ::usleep(200 * 1000);
  }
  return (pre_exp::clock_gettime_ns() - t0) / 1000000ULL;
}

// ---------------- 共享状态 ----------------
// 每个写线程一个独立槽位（自己的锁 + 分区缓冲）；“缓冲追加 + pending
// 计数”在同一把锁内完成，commit 线程用同一把锁换出缓冲，保证
// pending_ops 与实际缓冲条数一致，避免计数下溢导致死锁。
struct WriterSlot {
  std::mutex mu;
  std::vector<std::string> parts;  // 每分区一个追加缓冲
};

struct Shared {
  int p = 1;
  std::vector<std::unique_ptr<WriterSlot>> slots;  // 每写线程一个槽位
  std::atomic<uint64_t> pending_ops{0};
  std::atomic<uint64_t> earliest_ns{0};  // 当前未落盘最早记录的写入时刻
  std::atomic<bool> writers_done{false};
  std::atomic<uint64_t> total_fsyncs{0};
  uint64_t max_pending_ops = 0;  // 背压上限
};

ssize_t WriteAll(int fd, const char* data, size_t len) {
  size_t off = 0;
  while (off < len) {
    const ssize_t n = ::write(fd, data + off, len - off);
    if (n < 0) {
      if (errno == EINTR) continue;
      return -1;
    }
    off += static_cast<size_t>(n);
  }
  return static_cast<ssize_t>(off);
}

// ---------------- commit 线程 ----------------
struct CommitStats {
  uint64_t rounds = 0;
  uint64_t dirty_sum = 0;
  std::vector<uint64_t> window_ns;  // durability window 样本
};

void CommitThread(Shared* sh, const std::vector<int>& fds, bool do_sync,
                  uint64_t commit_batch, size_t recsize, CommitStats* cs) {
  const int P = sh->p;
  std::vector<std::string> merged(P);
  for (;;) {
    const uint64_t pend = sh->pending_ops.load(std::memory_order_acquire);
    const bool done = sh->writers_done.load(std::memory_order_acquire);
    if (pend == 0 && done) break;
    if (pend < commit_batch && !done) {
#if defined(__x86_64__)
      __builtin_ia32_pause();
#endif
      continue;
    }
    // ---- 交换出所有写线程缓冲（逐槽位加锁）----
    for (int i = 0; i < P; ++i) merged[i].clear();
    for (auto& sp : sh->slots) {
      std::lock_guard<std::mutex> lock(sp->mu);
      for (int i = 0; i < P; ++i) {
        if (!sp->parts[i].empty()) {
          merged[i].append(sp->parts[i]);
          sp->parts[i].clear();
        }
      }
    }
    const uint64_t e = sh->earliest_ns.exchange(0);
    uint64_t drained_bytes = 0;
    std::vector<int> dirty;
    // ---- 写入文件 ----
    for (int i = 0; i < P; ++i) {
      if (merged[i].empty()) continue;
      if (WriteAll(fds[i], merged[i].data(), merged[i].size()) < 0) {
        std::fprintf(stderr, "[commit] write failed on partition %d\n", i);
        std::exit(1);
      }
      drained_bytes += merged[i].size();
      dirty.push_back(i);
    }
    // ---- fsync ----
    uint64_t fs = 0;
    if (do_sync) {
      for (int idx : dirty) {
        if (::fsync(fds[idx]) != 0) {
          std::fprintf(stderr, "[commit] fsync failed on partition %d\n", idx);
          std::exit(1);
        }
        ++fs;
      }
    }
    sh->total_fsyncs.fetch_add(fs, std::memory_order_relaxed);
    if (e != 0) {
      const uint64_t now = pre_exp::clock_gettime_ns();
      cs->window_ns.push_back(now >= e ? now - e : 0);
    }
    cs->rounds++;
    cs->dirty_sum += dirty.size();
    // record 定长：按单条长度把字节数换算回 ops
    sh->pending_ops.fetch_sub(drained_bytes / recsize,
                              std::memory_order_release);
  }
}

// ---------------- 参数 ----------------
struct Args {
  int p = 1;
  std::string mode = "A";  // A=independent_fsync, B=batched_fsync
  bool sync = true;
  uint64_t ops = 0;  // 0 = 自适应
  int writers = 4;
  uint64_t base_batch = 512;
  size_t value_size = 1024;
  int cpu_start = 4;
  uint64_t seed = 42;
  std::string outdir = ".";
  std::string out;
  bool no_wait = false;
  uint64_t wait_cap_s = 7200;  // 等待系统空闲的上限（秒）
};

void ParseArgs(int argc, char** argv, Args* a) {
  for (int i = 1; i < argc; ++i) {
    std::string k = argv[i];
    auto next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : ""; };
    if (k == "--p") a->p = std::atoi(next().c_str());
    else if (k == "--mode") a->mode = next();
    else if (k == "--sync") a->sync = (next() == "true");
    else if (k == "--ops") a->ops = std::strtoull(next().c_str(), nullptr, 10);
    else if (k == "--writers") a->writers = std::atoi(next().c_str());
    else if (k == "--base-batch") a->base_batch = std::strtoull(next().c_str(), nullptr, 10);
    else if (k == "--value-size") a->value_size = std::strtoull(next().c_str(), nullptr, 10);
    else if (k == "--cpu-start") a->cpu_start = std::atoi(next().c_str());
    else if (k == "--seed") a->seed = std::strtoull(next().c_str(), nullptr, 10);
    else if (k == "--outdir") a->outdir = next();
    else if (k == "--out") a->out = next();
    else if (k == "--no-wait") a->no_wait = true;
    else if (k == "--wait-cap") a->wait_cap_s = std::strtoull(next().c_str(), nullptr, 10);
    else { std::fprintf(stderr, "unknown arg: %s\n", k.c_str()); std::exit(2); }
  }
}

// ---------------- 单次运行 ----------------
struct RunResult {
  bool ok = false;
  uint64_t ops = 0;
  double elapsed_s = 0.0;
  double throughput = 0.0;
  uint64_t fsync_count = 0;
  double fsync_per_op = 0.0;
  double win_mean_ms = 0.0;
  double win_p99_ms = 0.0;
  double win_max_ms = 0.0;
  uint64_t rounds = 0;
  double avg_dirty = 0.0;
};

bool RunOnce(const Args& args, const RangeRouter& router,
             const std::vector<int>& fds, uint64_t ops, uint64_t seed,
             RunResult* r) {
  const int P = args.p;
  const size_t recsize = 4 + 16 + args.value_size;
  const bool mode_b = (args.mode == "B");
  const uint64_t commit_batch =
      mode_b ? args.base_batch * static_cast<uint64_t>(P) : args.base_batch;

  std::string value(args.value_size, 'v');

  Shared sh;
  sh.p = P;
  sh.slots.reserve(args.writers);
  for (int i = 0; i < args.writers; ++i) {
    sh.slots.emplace_back(new WriterSlot());
    sh.slots.back()->parts.assign(P, std::string());
    for (auto& s : sh.slots.back()->parts) s.reserve(1u << 20);
  }
  // 背压上限：8 个 commit 批量（字节换算成 ops）
  sh.max_pending_ops =
      std::max<uint64_t>(commit_batch * 8, args.base_batch * 16);

  CommitStats cs;
  std::thread commit(CommitThread, &sh, std::cref(fds), args.sync,
                     commit_batch, recsize, &cs);

  // 启动写线程：每线程使用自己的槽位（自己的锁），互不竞争
  std::vector<std::thread> writers;
  const uint64_t per = ops / static_cast<uint64_t>(args.writers);
  uint64_t assigned = 0;
  auto make_writer = [&](int slot_idx, uint64_t nops, uint64_t wseed) {
    return std::thread([&sh, &router, &value, slot_idx, nops, wseed,
                        cpu = args.cpu_start + slot_idx]() {
      PinToCpu(cpu);
      pre_exp::UniformSampler sampler(kKeyspaceN, wseed);
      WriterSlot& slot = *sh.slots[slot_idx];
      Key16 k{};
      for (uint64_t i = 0; i < nops; ++i) {
        k = MakeKey(sampler.Next());
        const uint32_t part = router.Route(k);
        {
          std::lock_guard<std::mutex> lock(slot.mu);
          std::string& b = slot.parts[part];
          if (b.empty()) {
            uint64_t expect = 0;
            const uint64_t now = pre_exp::clock_gettime_ns();
            sh.earliest_ns.compare_exchange_strong(expect, now);
          }
          const uint32_t len = static_cast<uint32_t>(16 + value.size());
          b.append(reinterpret_cast<const char*>(&len), 4);
          b.append(reinterpret_cast<const char*>(k.b), 16);
          b.append(value);
          sh.pending_ops.fetch_add(1, std::memory_order_release);
        }
        // 背压：等待 commit 线程消化，避免无界缓冲
        while (sh.pending_ops.load(std::memory_order_acquire) >
               sh.max_pending_ops) {
#if defined(__x86_64__)
          __builtin_ia32_pause();
#endif
        }
      }
    });
  };
  const uint64_t t0 = pre_exp::clock_gettime_ns();
  for (int w = 0; w < args.writers; ++w) {
    const uint64_t nops =
        (w == args.writers - 1) ? (ops - assigned) : per;
    assigned += nops;
    writers.emplace_back(
        make_writer(w, nops, seed + 0x9E3779B97F4A7C15ULL * (w + 1)));
  }
  for (auto& t : writers) t.join();
  sh.writers_done.store(true, std::memory_order_release);
  commit.join();
  const uint64_t t1 = pre_exp::clock_gettime_ns();

  r->ok = true;
  r->ops = ops;
  r->elapsed_s = static_cast<double>(t1 - t0) / 1e9;
  r->throughput = r->elapsed_s > 0 ? ops / r->elapsed_s : 0.0;
  r->fsync_count = sh.total_fsyncs.load();
  r->fsync_per_op = ops > 0 ? static_cast<double>(r->fsync_count) / ops : 0.0;
  r->rounds = cs.rounds;
  r->avg_dirty = cs.rounds > 0
                     ? static_cast<double>(cs.dirty_sum) / cs.rounds
                     : 0.0;
  // durability window 统计：丢弃首轮（启动/预热效应；pilot 运行已作为
  // 整体预热被丢弃），其余排序取 mean/p99/max
  const size_t warm = cs.window_ns.size() >= 2 ? 1 : 0;
  std::vector<uint64_t> wins(cs.window_ns.begin() + warm, cs.window_ns.end());
  if (!wins.empty()) {
    std::sort(wins.begin(), wins.end());
    uint64_t sum = 0;
    for (uint64_t v : wins) sum += v;
    r->win_mean_ms = static_cast<double>(sum) / wins.size() / 1e6;
    r->win_p99_ms =
        static_cast<double>(wins[std::min<size_t>(
            wins.size() - 1, static_cast<size_t>(0.99 * wins.size()))]) /
        1e6;
    r->win_max_ms = static_cast<double>(wins.back()) / 1e6;
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  Args args;
  ParseArgs(argc, argv, &args);
  if (args.out.empty()) {
    std::fprintf(stderr, "--out required\n");
    return 2;
  }
  if (args.mode != "A" && args.mode != "B") {
    std::fprintf(stderr, "--mode must be A or B\n");
    return 2;
  }
  pre_exp::CalibrateTsc();
  const int ncpu = static_cast<int>(std::thread::hardware_concurrency());

  // 干扰控制：等待系统空闲 + 运行前 sync 落盘
  uint64_t idle_wait_ms = 0;
  if (!args.no_wait) idle_wait_ms = WaitForIdle(ncpu, args.wait_cap_s * 1000);
  ::sync();

  // 目录与文件准备（fallocate 预分配）
  if (::mkdir(args.outdir.c_str(), 0755) != 0 && errno != EEXIST) {
    std::fprintf(stderr, "mkdir %s failed: %s\n", args.outdir.c_str(),
                 strerror(errno));
    return 1;
  }
  const size_t recsize = 4 + 16 + args.value_size;
  RangeRouter router(args.p);
  std::vector<int> fds(args.p, -1);
  std::vector<std::string> paths(args.p);
  for (int i = 0; i < args.p; ++i) {
    char name[32];
    std::snprintf(name, sizeof(name), "/wal_%05d.log", i);
    paths[i] = args.outdir + name;
    const int fd = ::open(paths[i].c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
      std::fprintf(stderr, "open %s failed: %s\n", paths[i].c_str(),
                   strerror(errno));
      return 1;
    }
    fds[i] = fd;
  }

  auto fallocate_for = [&](uint64_t ops) {
    for (int i = 0; i < args.p; ++i) {
      // 均匀分布下每分区期望字节数，加 20% 余量，最小 8MB
      uint64_t est = ops * recsize / static_cast<uint64_t>(args.p);
      est = est + est / 5 + (8u << 20);
      ::fallocate(fds[i], 0, 0, static_cast<off_t>(est));  // 失败不致命
    }
  };

  const std::string mode_name =
      (args.mode == "A") ? "independent_fsync" : "batched_fsync";

  // ---- pilot / ops 自适应（含迭代重标定，保证正式运行 >=10s）----
  uint64_t ops = args.ops;
  uint64_t pilot_ops = 0;
  double pilot_rate = 0.0;
  std::string ops_reason;
  RunResult r;
  const double target_s = 10.0;
  // nosync 档位上限压到 4M ops：更大量会触发脏页回写限速（实测
  // 22M ops 时吞吐从 ~2M/s 跌到 ~300k/s、单轮 30-90s），既拖慢运行
  // 又干扰后续组合；spec 对 nosync 的硬性要求是 >=2M ops。
  const uint64_t ops_lo = args.sync ? 50000ULL : 2000000ULL;
  const uint64_t ops_hi = args.sync ? 8000000ULL : 4000000ULL;

  auto reset_files = [&]() -> bool {
    for (int i = 0; i < args.p; ++i) {
      if (::ftruncate(fds[i], 0) != 0) {
        std::fprintf(stderr, "ftruncate failed\n");
        return false;
      }
      ::lseek(fds[i], 0, SEEK_SET);
    }
    return true;
  };

  if (ops == 0) {
    pilot_ops = std::max<uint64_t>(
        args.base_batch * 8,
        (args.mode == "B") ? args.base_batch * args.p * 4 : 2048);
    fallocate_for(pilot_ops);
    RunResult pr;
    if (!RunOnce(args, router, fds, pilot_ops, args.seed ^ 0xA5A5A5A5, &pr)) {
      std::fprintf(stderr, "pilot run failed\n");
      return 1;
    }
    pilot_rate = pr.throughput;
    if (!reset_files()) return 1;  // pilot 数据作废
    ops = static_cast<uint64_t>(pilot_rate * target_s);
    ops = std::max<uint64_t>(ops, ops_lo);
    ops = std::min<uint64_t>(ops, ops_hi);
    ops_reason = args.sync
        ? "sync=true: pilot_rate*10s target clamped to [50000,8000000], "
          "迭代重标定保证 >=10s"
        : "sync=false: ops clamped to [2000000,4000000]，满足 spec >=2M ops；"
          "上限压低以避免脏页回写风暴（实测 22M ops 触发限速，单轮 30-90s "
          "且干扰后续组合），故该档时长以 ops 数而非 10s 为准";
    // pilot 短促会低估稳态速率：正式运行不足 target 时按比例放大 ops
    // 重跑（最多 3 次），直到达标或触及 ops 上限。
    for (int attempt = 0; attempt < 3; ++attempt) {
      fallocate_for(ops);
      if (!RunOnce(args, router, fds, ops, args.seed, &r)) {
        std::fprintf(stderr, "main run failed\n");
        return 1;
      }
      if (r.elapsed_s >= target_s || ops >= ops_hi) break;
      const double scale = target_s / std::max(r.elapsed_s, 0.05);
      const uint64_t prev = ops;
      double nf = static_cast<double>(ops) * scale * 1.1;  // 10% 余量
      ops = static_cast<uint64_t>(nf);
      ops = std::max<uint64_t>(ops, ops_lo);
      ops = std::min<uint64_t>(ops, ops_hi);
      if (ops <= prev) break;
      char extra[128];
      std::snprintf(extra, sizeof(extra),
                    "; rescale#%d: elapsed %.1fs < 10s -> ops x%.2f",
                    attempt + 1, r.elapsed_s, scale);
      ops_reason += extra;
      if (!reset_files()) return 1;
    }
  } else {
    ops_reason = "fixed by --ops";
    fallocate_for(ops);
    if (!RunOnce(args, router, fds, ops, args.seed, &r)) {
      std::fprintf(stderr, "main run failed\n");
      return 1;
    }
  }
  for (int fd : fds) ::close(fd);

  // ---- 输出 JSON ----
  JsonWriter w;
  w.BeginObject();
  w.Key("experiment").String("exp2_partition_gate_multi_wal_fsync");
  w.Key("p").Number(static_cast<int64_t>(args.p));
  w.Key("mode").String(mode_name);
  w.Key("sync_wal").Bool(args.sync);
  w.Key("writers").Number(static_cast<int64_t>(args.writers));
  w.Key("base_batch_ops").Number(args.base_batch);
  w.Key("commit_batch_ops").Number(
      (args.mode == "B") ? args.base_batch * args.p : args.base_batch);
  w.Key("value_size").Number(static_cast<uint64_t>(args.value_size));
  w.Key("record_size").Number(static_cast<uint64_t>(recsize));
  w.Key("key_distribution").String(
      "uniform over 2^24 (worst-case dirty fan-out; zipfian(0.99) would put "
      "~96% mass into partition 0 of 64 and underestimate fsync fan-out)");
  w.Key("wal_dir").String(args.outdir);
  w.Key("wal_path_note").String("NVMe-backed partition /home (ext4 on nvme0n1p3)");
  w.Key("ops").Number(r.ops);
  w.Key("ops_reason").String(ops_reason);
  w.Key("pilot_ops").Number(pilot_ops);
  w.Key("pilot_rate_ops_per_s").Number(pilot_rate);
  w.Key("elapsed_s").Number(r.elapsed_s);
  w.Key("throughput_ops_per_s").Number(r.throughput);
  w.Key("fsync_count").Number(r.fsync_count);
  w.Key("fsync_count_per_op").Number(r.fsync_per_op);
  w.Key("durability_window_ms").BeginObject();
  w.Key("mean").Number(r.win_mean_ms);
  w.Key("p99").Number(r.win_p99_ms);
  w.Key("max").Number(r.win_max_ms);
  w.EndObject();
  w.Key("durability_window_ms_p99").Number(r.win_p99_ms);
  w.Key("commit_rounds").Number(r.rounds);
  w.Key("avg_dirty_files_per_round").Number(r.avg_dirty);
  w.Key("idle_wait_ms").Number(idle_wait_ms);
  w.Key("kernel").String(KernelVersion());
  w.Key("mount_opts").String(MountOptionsOf(args.outdir));
  w.Key("scheduler").String(IoScheduler());
  w.Key("cpu_governor").String(ReadFirstLine(
      "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"));
  w.Key("ncpu").Number(static_cast<int64_t>(ncpu));
  w.Key("seed").Number(args.seed);
  w.Key("tsc_freq_hz").Number(pre_exp::tsc_freq_hz());
  w.Key("timestamp_ns").Number(pre_exp::clock_gettime_ns());
  w.EndObject();

  std::ofstream f(args.out);
  if (!f) {
    std::fprintf(stderr, "cannot open out: %s\n", args.out.c_str());
    return 1;
  }
  f << w.Dump() << "\n";
  std::fprintf(stderr,
               "[multi_wal] p=%d mode=%s sync=%d ops=%llu tput=%.0f ops/s "
               "fsync/op=%.4f win_p99=%.2fms\n",
               args.p, mode_name.c_str(), args.sync ? 1 : 0,
               (unsigned long long)r.ops, r.throughput, r.fsync_per_op,
               r.win_p99_ms);
  return 0;
}
