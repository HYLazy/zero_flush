// wpbench -- write-path decomposition benchmark for default-config RocksDB.
//
// Question it answers: the foreground Put path's CPU (74% of the 100GB run)
// looks huge.  How much of it is useful work (WAL write, memtable insert,
// value generation) and how much is multi-thread coordination (follower
// spin-wait in WriteThread::AwaitState, whose adaptive yield can spin up to
// write_thread_max_yield_usec=100us per wait)?
//
// Each invocation measures ONE configuration and appends one CSV row:
//   * per-thread CPU from CLOCK_THREAD_CPUTIME_ID (exact, taken in-thread
//     before exit) plus per-thread utime/stime from /proc/self/task/<tid>/stat
//     (stime share exposes sched_yield() spinning).
//   * optional PerfContext (kEnableTime) per-op wall components of the write
//     path: write_thread_wait_nanos, write_wal_time, write_memtable_time,
//     write_delay_time, write_scheduling_flushes_compactions_time,
//     write_pre_and_post_process_time.  These are the leader's own work; the
//     difference to the fleet CPU/op is coordination/spin.
//   * wall, ops/s, MB/s, stalls.
//
// Variants (all clearly reported in the CSV "variants" column):
//   --mode=gen            pure key+value generation, no DB (driver cost)
//   --mode=put            db->Put with a fresh WriteBatch per op (driver's Put)
//   --mode=put_nogen      Put with a constant value (no value generation)
//   --mode=batch          WriteBatch of --batch puts per Write call
//   --wal=0               WriteOptions.disableWAL=true        (isolates WAL)
//   --sync=1              WriteOptions.sync=true              (adds fsync)
//   --quiet=1             huge write buffer + auto compactions off + L0
//                         triggers widened (isolates the write path from
//                         flush/compaction/stall pressure; NON-DEFAULT)
//   --adaptive_yield=0    DBOptions.enable_write_thread_adaptive_yield=false
//                         (NON-DEFAULT; the control experiment for spin cost)
//   --wb_mb=N             write_buffer_size override (default: library default)
//
// Build: bench/build_wpbench.sh

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <string>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/perf_level.h"

using namespace ROCKSDB_NAMESPACE;

namespace {

double WallSec() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}
uint64_t CpuUs() {
  struct timespec ts;
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
  return (uint64_t)ts.tv_sec * 1000000ull + (uint64_t)ts.tv_nsec / 1000ull;
}
long ClockTicksPerSec() {
  static long v = sysconf(_SC_CLK_TCK);
  return v > 0 ? v : 100;
}
std::vector<std::string> Split(const std::string& s, char d) {
  std::vector<std::string> out;
  size_t p = 0;
  while (p <= s.size()) {
    size_t q = s.find(d, p);
    if (q == std::string::npos) q = s.size();
    out.push_back(s.substr(p, q - p));
    p = q + 1;
  }
  return out;
}
bool ReadFile(const std::string& path, std::string* out) {
  FILE* f = fopen(path.c_str(), "r");
  if (!f) return false;
  char buf[4096];
  size_t n;
  out->clear();
  while ((n = fread(buf, 1, sizeof(buf), f)) > 0) out->append(buf, n);
  fclose(f);
  return true;
}
// utime/stime (clock ticks) of the calling thread from /proc/self/task/<tid>/stat
void SelfUtimeStime(uint64_t* utime, uint64_t* stime) {
  *utime = *stime = 0;
  std::string s;
  if (!ReadFile("/proc/self/task/" + std::to_string((long)syscall(SYS_gettid)) + "/stat", &s))
    return;
  size_t rp = s.rfind(')');
  if (rp == std::string::npos) return;
  auto f = Split(s.substr(rp + 2), ' ');
  if (f.size() < 13) return;
  *utime = strtoull(f[11].c_str(), nullptr, 10);
  *stime = strtoull(f[12].c_str(), nullptr, 10);
}

// ---- key/value generation (identical to fillcpu) ----
inline uint32_t Mix32(uint32_t x, uint32_t salt) {
  x += salt * 0x9E3779B9u;
  x ^= x >> 16; x *= 0x7feb352du;
  x ^= x >> 15; x *= 0x846ca68bu;
  x ^= x >> 16;
  return x;
}
inline uint32_t Feistel28(uint32_t x, uint32_t seed) {
  uint32_t l = x >> 14, r = x & 0x3FFFu;
  for (uint32_t i = 0; i < 4; i++) {
    uint32_t f = Mix32(r, seed + 0x1111u * i) & 0x3FFFu;
    uint32_t nl = r, nr = l ^ f;
    l = nl; r = nr;
  }
  return (l << 14) | r;
}
inline uint32_t Permute(uint32_t x, uint32_t seed, uint32_t n) {
  uint32_t v = Feistel28(x, seed);
  while (v >= n) v = Feistel28(v, seed);
  return v;
}
inline void EncodeKey(uint64_t id, char* out) {
  for (int i = 0; i < 8; i++) out[i] = (char)0x01;
  for (int i = 0; i < 8; i++) out[8 + i] = (char)((id >> (56 - 8 * i)) & 0xFF);
}
void FillValue(uint64_t id, uint64_t salt, int size, std::string* v) {
  v->resize((size_t)size);
  uint64_t x = id * 0x9E3779B97F4A7C15ull + salt * 0xD1B54A32D192ED03ull;
  char* p = &(*v)[0];
  for (int i = 0; i < size; i += 8) {
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    int n = std::min(8, size - i);
    memcpy(p + i, &x, n);
  }
}

// ---- config ----
struct Cfg {
  std::string db_path = "./dbwp";
  std::string out = "wpbench";
  std::string mode = "put";   // gen | put | put_nogen | batch
  std::string tag = "";       // free-form label recorded in the CSV
  int64_t ops = 2000000;
  int threads = 1;
  int value_size = 1024;
  int batch = 1;
  int wal = 1, sync = 0, perf = 0, quiet = 0, nogen = 0, adaptive_yield = 1;
  int bg_jobs = 0, subcomp = 0;   // 0 = library default
  int pipelined = 0;              // enable_pipelined_write (non-default)
  int64_t wb_mb = 0;
  int64_t universe = 0;  // 0 -> use ops
  uint32_t seed = 7;
  bool keep = false;
} g;

const char* kUsage =
    "Usage: wpbench --db=P --out=O --mode=gen|put|put_nogen|batch [--threads=N]\n"
    "               [--ops=N] [--value_size=B] [--batch=N] [--wal=0|1] [--sync=0|1]\n"
    "               [--perf=0|1] [--quiet=0|1] [--adaptive_yield=0|1] [--wb_mb=N]\n"
    "               [--bg_jobs=N] [--subcomp=N]\n"
    "               [--tag=LABEL] [--keep=0|1]\n";

void ParseArgs(int argc, char** argv) {
  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    if (a == "--help" || a == "-h") { printf("%s", kUsage); exit(0); }
    if (a.rfind("--", 0) != 0) { fprintf(stderr, "bad arg %s\n", a.c_str()); exit(2); }
    a = a.substr(2);
    std::string k, v;
    size_t eq = a.find('=');
    if (eq == std::string::npos) { k = a; v = "1"; } else { k = a.substr(0, eq); v = a.substr(eq + 1); }
    auto B = [&]() { return v == "1" || v == "true"; };
    if (k == "db") g.db_path = v;
    else if (k == "out") g.out = v;
    else if (k == "mode") g.mode = v;
    else if (k == "tag") g.tag = v;
    else if (k == "ops") g.ops = strtoll(v.c_str(), nullptr, 10);
    else if (k == "threads") g.threads = atoi(v.c_str());
    else if (k == "value_size") g.value_size = atoi(v.c_str());
    else if (k == "batch") g.batch = atoi(v.c_str());
    else if (k == "wal") g.wal = B();
    else if (k == "sync") g.sync = B();
    else if (k == "perf") g.perf = B();
    else if (k == "quiet") g.quiet = B();
    else if (k == "adaptive_yield") g.adaptive_yield = B();
    else if (k == "wb_mb") g.wb_mb = strtoll(v.c_str(), nullptr, 10);
    else if (k == "bg_jobs") g.bg_jobs = atoi(v.c_str());
    else if (k == "subcomp") g.subcomp = atoi(v.c_str());
    else if (k == "pipelined") g.pipelined = atoi(v.c_str());
    else if (k == "keep") g.keep = B();
    else { fprintf(stderr, "unknown flag %s\n", k.c_str()); exit(2); }
  }
}

// ---- perf context component indices ----
enum { PC_WAIT = 0, PC_WAL, PC_MEM, PC_DELAY, PC_SCHED, PC_PREPOST, PC_N };
const char* kPcNames[PC_N] = {"write_thread_wait_nanos", "write_wal_time",
                              "write_memtable_time", "write_delay_time",
                              "write_scheduling_flushes_compactions_time",
                              "write_pre_and_post_process_time"};

DB* g_db = nullptr;

struct ThreadRes {
  double cpu_s = 0;
  uint64_t utime = 0, stime = 0;
  int64_t ops = 0;
  double wall_s = 0;
  uint64_t pc[PC_N] = {0, 0, 0, 0, 0, 0};
  int64_t stalls = 0;
};

void RunThread(int idx, int64_t begin, int64_t end, ThreadRes* res) {
  pthread_setname_np(pthread_self(), ("wp" + std::to_string(idx)).c_str());
  std::string key(16, '\0');
  std::string value;
  value.reserve((size_t)g.value_size);
  if (g.mode == "put_nogen" || g.mode == "batch") FillValue(12345, 1, g.value_size, &value);
  WriteOptions wo;
  wo.disableWAL = (g.wal == 0);
  wo.sync = (g.sync == 1);
  // NOTE: perf_level is thread_local in RocksDB -- it must be set inside the
  // thread, not by main(), or every write_* timer stays disabled here.
  if (g.perf) SetPerfLevel(PerfLevel::kEnableTime);
  PerfContext* pc = get_perf_context();
  uint64_t t0 = CpuUs();
  double w0 = WallSec();
  int64_t n = 0;
  const int64_t kBlock = 4096;
  if (g.mode == "batch") {
    int64_t next_block = kBlock;
    for (int64_t x = begin; x < end; x += g.batch) {
      WriteBatch wb;
      int64_t m = std::min<int64_t>(g.batch, end - x);
      for (int64_t j = 0; j < m; j++) {
        uint32_t id = Permute((uint32_t)(x + j), g.seed, (uint32_t)g.universe);
        EncodeKey(id, &key[0]);
        if (!g.nogen) FillValue(id, 1, g.value_size, &value);
        wb.Put(Slice(key), Slice(value));
      }
      Status s = g_db->Write(wo, &wb);
      if (s.ok()) n += m;
      if (g.perf && n >= next_block) {
        res->pc[PC_WAIT] += pc->write_thread_wait_nanos;
        res->pc[PC_WAL] += pc->write_wal_time;
        res->pc[PC_MEM] += pc->write_memtable_time;
        res->pc[PC_DELAY] += pc->write_delay_time;
        res->pc[PC_SCHED] += pc->write_scheduling_flushes_compactions_time;
        res->pc[PC_PREPOST] += pc->write_pre_and_post_process_time;
        pc->Reset();
        next_block += kBlock;
      }
    }
  } else {
    for (int64_t x = begin; x < end; x++) {
      uint32_t id = Permute((uint32_t)x, g.seed, (uint32_t)g.universe);
      EncodeKey(id, &key[0]);
      if (!g.nogen) FillValue(id, 1, g.value_size, &value);
      // PerfContext counters accumulate per thread: read-and-reset exactly once
      // per block, otherwise the running total gets added again on every op.
      if (g.perf && (n % kBlock) == 0) {
        res->pc[PC_WAIT] += pc->write_thread_wait_nanos;
        res->pc[PC_WAL] += pc->write_wal_time;
        res->pc[PC_MEM] += pc->write_memtable_time;
        res->pc[PC_DELAY] += pc->write_delay_time;
        res->pc[PC_SCHED] += pc->write_scheduling_flushes_compactions_time;
        res->pc[PC_PREPOST] += pc->write_pre_and_post_process_time;
        pc->Reset();
      }
      Status s = g_db->Put(wo, Slice(key), Slice(value));
      if (s.ok()) n++;
    }
  }
  res->wall_s = WallSec() - w0;
  res->cpu_s = (double)(CpuUs() - t0) / 1e6;
  res->ops = n;
  SelfUtimeStime(&res->utime, &res->stime);
}

// gen-only loop: no DB at all
void GenLoop(int64_t begin, int64_t end, ThreadRes* res) {
  std::string key(16, '\0'), value;
  value.reserve((size_t)g.value_size);
  uint64_t t0 = CpuUs();
  double w0 = WallSec();
  int64_t n = 0;
  for (int64_t x = begin; x < end; x++) {
    uint32_t id = Permute((uint32_t)x, g.seed, (uint32_t)g.universe);
    EncodeKey(id, &key[0]);
    FillValue(id, 1, g.value_size, &value);
    n++;
  }
  res->wall_s = WallSec() - w0;
  res->cpu_s = (double)(CpuUs() - t0) / 1e6;
  res->ops = n;
  SelfUtimeStime(&res->utime, &res->stime);
}

}  // namespace

int main(int argc, char** argv) {
  ParseArgs(argc, argv);
  pthread_setname_np(pthread_self(), "wp-main");
  // allow same-uid stack sampling (gdb -p) even with yama ptrace_scope=1
  prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
  double start = WallSec();

  if (g.universe <= 0) g.universe = g.ops;
  if (g.universe > (int64_t)0xFFFFFFF) { fprintf(stderr, "universe too large\n"); return 2; }
  int64_t chunk = g.ops / g.threads;
  std::vector<ThreadRes> res((size_t)g.threads);
  std::vector<std::thread> th;
  std::vector<std::string> variants;
  if (g.quiet) variants.push_back("quiet(wb,huge;auto_compact=off;L0_trig=big)");
  if (!g.adaptive_yield) variants.push_back("adaptive_yield=off");
  if (g.wb_mb > 0) variants.push_back("wb_mb=" + std::to_string(g.wb_mb));
  if (g.wal == 0) variants.push_back("wal=off");
  if (g.sync) variants.push_back("sync=on");
  if (g.batch > 1) variants.push_back("batch=" + std::to_string(g.batch));
  if (g.bg_jobs > 0) variants.push_back("bg_jobs=" + std::to_string(g.bg_jobs));
  if (g.subcomp > 0) variants.push_back("subcomp=" + std::to_string(g.subcomp));
  if (g.pipelined) variants.push_back("pipelined_write=on");
  std::string variant_str;
  for (size_t i = 0; i < variants.size(); i++) variant_str += (i ? ";" : "") + variants[i];

  if (g.mode == "gen") {
    for (int i = 0; i < g.threads; i++) {
      int64_t b = (int64_t)i * chunk;
      int64_t e = (i == g.threads - 1) ? g.ops : (int64_t)(i + 1) * chunk;
      th.emplace_back([&, i, b, e]() { GenLoop(b, e, &res[i]); });
    }
    for (auto& t : th) t.join();
  } else {
    Options opt;
    opt.create_if_missing = true;
    if (g.quiet) {
      opt.disable_auto_compactions = true;
      opt.level0_file_num_compaction_trigger = 1000000;
      opt.level0_slowdown_writes_trigger = 1000000;
      opt.level0_stop_writes_trigger = 1000000;
      opt.soft_pending_compaction_bytes_limit = 0;
      opt.hard_pending_compaction_bytes_limit = 0;
    }
    opt.enable_write_thread_adaptive_yield = g.adaptive_yield != 0;
    if (g.bg_jobs > 0) opt.max_background_jobs = g.bg_jobs;
    if (g.subcomp > 0) opt.max_subcompactions = g.subcomp;
    if (g.pipelined) opt.enable_pipelined_write = true;
    if (g.wb_mb > 0) opt.write_buffer_size = (size_t)g.wb_mb << 20;
    if (g.quiet) {
      opt.write_buffer_size = (size_t)8 << 30;  // avoid memtable switch inside the run
      opt.max_write_buffer_number = 3;
    }
    if (g.perf) SetPerfLevel(PerfLevel::kEnableTime);
    DestroyDB(g.db_path, Options());
    std::unique_ptr<DB> db;
    Status s = DB::Open(opt, g.db_path, &db);
    if (!s.ok()) { fprintf(stderr, "open failed: %s\n", s.ToString().c_str()); return 3; }
    g_db = db.get();
    auto opts_eff = db->GetOptions();
    for (int i = 0; i < g.threads; i++) {
      int64_t b = (int64_t)i * chunk;
      int64_t e = (i == g.threads - 1) ? g.ops : (int64_t)(i + 1) * chunk;
      th.emplace_back([&, i, b, e]() { RunThread(i, b, e, &res[i]); });
    }
    for (auto& t : th) t.join();
    // stalls observed (only meaningful in non-quiet runs)
    std::string v;
    if (db->GetProperty("rocksdb.cfstats", &v)) {
      size_t p = v.find("Write Stall (count)");
      if (p != std::string::npos) {
        size_t e = v.find('\n', p);
        printf("  %s\n", v.substr(p, std::min<size_t>(e - p, 700)).c_str());
      }
    }
    db->Close();
  }

  double wall = WallSec() - start;
  double cpu = 0, u = 0, st = 0;
  int64_t ops = 0;
  uint64_t pc[PC_N] = {0, 0, 0, 0, 0, 0};
  for (auto& r : res) {
    cpu += r.cpu_s;
    u += (double)r.utime / ClockTicksPerSec();
    st += (double)r.stime / ClockTicksPerSec();
    ops += r.ops;
    for (int i = 0; i < PC_N; i++) pc[i] += r.pc[i];
  }
  double mbps = (double)ops * (g.value_size + 16) / wall / 1048576.0;
  double cpu_us_op = cpu * 1e6 / std::max<int64_t>(ops, 1);
  printf("%-10s thr=%2d batch=%-3d wal=%d sync=%d perf=%d %-28s ops=%lld wall=%.2fs "
         "cpu=%.2fs (%.2f us/op) utime=%.2fs stime=%.2fs stime%%=%.1f MB/s=%.0f\n",
         g.mode.c_str(), g.threads, g.batch, g.wal, g.sync, g.perf, variant_str.c_str(),
         (long long)ops, wall, cpu, cpu_us_op, u, st, 100.0 * st / std::max(u + st, 1e-9), mbps);
  if (g.perf && ops > 0) {
    printf("           perf/op ns:");
    for (int i = 0; i < PC_N; i++) printf(" %s=%.0f", kPcNames[i], (double)pc[i] / ops);
    double lead = (double)(pc[PC_WAL] + pc[PC_MEM] + pc[PC_PREPOST]) / ops;
    printf("  [leader wal+mem+prepost=%.0f ns/op; fleet cpu=%.0f ns/op; diff=%.0f ns/op]",
           lead, cpu_us_op * 1000.0, cpu_us_op * 1000.0 - lead);
    printf("\n");
  }
  {
    std::string path = g.out + ".csv";
    bool exists = false;
    { FILE* f = fopen(path.c_str(), "r"); if (f) { exists = true; fclose(f); } }
    FILE* f = fopen(path.c_str(), "a");
    if (f) {
      if (!exists) {
        fprintf(f, "mode,threads,ops,batch,wal,sync,perf,value_size,wall_s,cpu_s,cpu_us_per_op,"
                   "utime_s,stime_s,stime_share_pct,mbps");
        for (int i = 0; i < PC_N; i++) fprintf(f, ",%s_per_op_ns", kPcNames[i]);
        fprintf(f, ",leader_wal_mem_prepost_ns_per_op,tag\n");
      }
      fprintf(f, "%s,%d,%lld,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.1f,%.1f",
              g.mode.c_str(), g.threads, (long long)ops, g.batch, g.wal, g.sync, g.perf,
              g.value_size, wall, cpu, cpu_us_op, u, st, 100.0 * st / std::max(u + st, 1e-9), mbps);
      for (int i = 0; i < PC_N; i++) fprintf(f, ",%.1f", (double)pc[i] / std::max<int64_t>(ops, 1));
      double lead = (double)(pc[PC_WAL] + pc[PC_MEM] + pc[PC_PREPOST]) / std::max<int64_t>(ops, 1);
      fprintf(f, ",%.1f,%s\n", lead, g.tag.c_str());
      fclose(f);
    }
  }
  return 0;
}
