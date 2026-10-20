// fillcpu -- default-config RocksDB fill driver with per-operation CPU and
//            per-component memory attribution.
//
// This is a CLIENT program linking an UNMODIFIED RocksDB shared library.  The
// DB is opened with *library defaults*: the only Options field set by this
// driver is create_if_missing (plus the observation-only EventListener).
// Statistics are deliberately NOT enabled, so no measurement-induced option
// change can shift the CPU cost being measured.
//
// Measurement surface (all measured, nothing inferred from the LOG):
//   * per-job CPU: CLOCK_THREAD_CPUTIME_ID delta around each flush job
//     (OnFlushBegin -> OnFlushCompleted).  Both callbacks run on the job's own
//     background thread (DBImpl::FlushMemTableToOutputFile), so the delta is
//     the CPU that thread burned on that flush (memtable -> SST + install +
//     manifest edit).
//     For compaction the same thread-level delta is recorded, and RocksDB's
//     own CompactionJobStats::cpu_micros (sum over subcompactions of the CPU
//     of the thread that ran each subcompaction, measured by the library with
//     clock->CPUMicros()) is recorded next to it as an independent reading.
//   * per-thread cumulative CPU: /proc/self/task/<tid>/stat snapshots.  Threads
//     are classified by name: "rocksdb:high" = flush thread pool (default
//     max_background_flushes = 1), "rocksdb:low" = compaction pool (default
//     max_background_compactions = 1), "fcw*" = our writer threads,
//     "fc-*" = driver threads.  The sum of per-thread deltas over a phase
//     equals the process CPU delta (checked against /proc/self/stat).
//   * memory: DB properties (memtables / block cache / table readers) + RSS,
//     VmHWM, VmSize, thread count and mallinfo2, sampled every --sample_ms.
//     "Other" is reported as RSS minus the accounted RocksDB components and is
//     labelled as allocator/engine overhead, not silently attributed.
//   * job ledger: every flush/compaction job row (CPU, wall, bytes, records,
//     level inputs/outputs) and every write-stall transition; plus the
//     library's own "rocksdb.cfstats" table captured at phase boundaries, which
//     reports CompMergeCPU(sec) per level (level 0 = flush).
//   * page cache: /proc/meminfo Dirty+Writeback sampled with the rest (the
//     buffered flush output lives here before kernel writeback).
//   * flush-characterization overrides (all default-off): --disable_compactions
//     (flush-only run), --write_buffer_size, --max_bg_flushes, --direct_io.
//
// Two phases are measured with exact thread-CPU snapshots around them:
//   fill  : writer threads running (16 threads in flight)
//   drain : writers done, engine left to quiesce (flush + compaction only)
// so "flush CPU while loading" and "flush CPU for the whole 100GB of data" are
// both answerable and separable.
//
// Build: bench/build_fillcpu.sh      Usage: --help

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <malloc.h>
#include <map>
#include <mutex>
#include <pthread.h>
#include <string>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/listener.h"
#include "rocksdb/iostats_context.h"
#include "monitoring/iostats_context_imp.h"  // thread_local iostats_context (in-tree build)
#include "rocksdb/options.h"
#include "rocksdb/perf_level.h"

using namespace ROCKSDB_NAMESPACE;

namespace {

double g_start_ts = 0;

// ---------------------------------------------------------------------------
// small utilities
// ---------------------------------------------------------------------------
double WallSec() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

// CPU consumed by the *calling thread* since thread start (microseconds).
uint64_t CpuUs() {
  struct timespec ts;
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
  return (uint64_t)ts.tv_sec * 1000000ull + (uint64_t)ts.tv_nsec / 1000ull;
}

pid_t GetTid() { return (pid_t)syscall(SYS_gettid); }

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
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return false;
  char buf[4096];
  ssize_t n = 0, r;
  out->clear();
  while ((r = ::read(fd, buf, sizeof(buf))) > 0) { out->append(buf, (size_t)r); n += r; }
  ::close(fd);
  return n >= 0;
}

std::string Fmt(const char* f, ...) {
  char buf[4096];
  va_list ap;
  va_start(ap, f);
  vsnprintf(buf, sizeof(buf), f, ap);
  va_end(ap);
  return std::string(buf);
}

// ---------------------------------------------------------------------------
// /proc readers
// ---------------------------------------------------------------------------
struct TStat {
  std::string name;
  char state = '?';               // R/S/D/... from /proc/<tid>/stat
  uint64_t utime = 0, stime = 0;  // clock ticks
  uint64_t cpu_ticks() const { return utime + stime; }
};

// Parse /proc/<pid>/task/<tid>/stat.  Fields 14/15 = utime/stime; comm is
// inside parens and may contain spaces, so parse from the last ')'.
bool ParseProcStat(const std::string& s, std::string* comm, uint64_t* utime,
                   uint64_t* stime, char* state = nullptr) {
  size_t lp = s.find('('), rp = s.rfind(')');
  if (lp == std::string::npos || rp == std::string::npos || rp < lp) return false;
  *comm = s.substr(lp + 1, rp - lp - 1);
  std::vector<std::string> f = Split(s.substr(rp + 2), ' ');
  if (f.size() < 13) return false;
  if (state && !f[0].empty()) *state = f[0][0];
  *utime = strtoull(f[11].c_str(), nullptr, 10);  // field 14
  *stime = strtoull(f[12].c_str(), nullptr, 10);  // field 15
  return true;
}

bool ReadThreadStat(pid_t tid, TStat* out) {
  std::string s;
  if (!ReadFile(Fmt("/proc/self/task/%d/stat", (int)tid), &s)) return false;
  return ParseProcStat(s, &out->name, &out->utime, &out->stime, &out->state);
}

int64_t ReadProcSelfCpuTicks() {
  std::string s;
  if (!ReadFile("/proc/self/stat", &s)) return -1;
  std::string comm;
  uint64_t u = 0, st = 0;
  if (!ParseProcStat(s, &comm, &u, &st)) return -1;
  return (int64_t)(u + st);
}

using ThreadMap = std::map<pid_t, TStat>;

void SnapshotThreads(ThreadMap* out) {
  out->clear();
  DIR* d = opendir("/proc/self/task");
  if (!d) return;
  struct dirent* e;
  while ((e = readdir(d)) != nullptr) {
    if (e->d_name[0] < '0' || e->d_name[0] > '9') continue;
    pid_t tid = (pid_t)atoi(e->d_name);
    TStat st;
    if (ReadThreadStat(tid, &st)) (*out)[tid] = st;
  }
  closedir(d);
}

struct Snap {
  ThreadMap tm;
  int64_t proc_ticks = 0;
  double ts = 0;
};

void TakeSnap(Snap* s) {
  s->proc_ticks = ReadProcSelfCpuTicks();
  SnapshotThreads(&s->tm);
  s->ts = WallSec();
}

// thread roles
enum Role { kRoleWriter = 0, kRoleFlush, kRoleCompaction, kRoleDriver, kRoleOther, kNumRoles };
const char* kRoleNames[kNumRoles] = {"writer", "flush_pool", "compaction_pool", "driver", "other"};

Role Classify(const std::string& name) {
  if (name.compare(0, 3, "fcw") == 0) return kRoleWriter;       // fcw00..fcw15
  if (name == "rocksdb:high") return kRoleFlush;
  if (name == "rocksdb:low") return kRoleCompaction;
  if (name.compare(0, 3, "fc-") == 0) return kRoleDriver;       // fc-main / fc-samp
  return kRoleOther;
}

struct RoleCpu {
  double cpu_s[kNumRoles] = {0, 0, 0, 0, 0};
  std::map<std::string, double> other_detail;
  double total() const {
    double t = 0;
    for (int i = 0; i < kNumRoles; i++) t += cpu_s[i];
    return t;
  }
};

RoleCpu DiffRoles(const ThreadMap& a, const ThreadMap& b) {
  RoleCpu r;
  for (const auto& kv : b) {
    auto it = a.find(kv.first);
    uint64_t prev = (it == a.end()) ? 0 : it->second.cpu_ticks();
    uint64_t cur = kv.second.cpu_ticks();
    if (cur < prev) prev = 0;
    double sec = (double)(cur - prev) / (double)ClockTicksPerSec();
    Role role = Classify(kv.second.name);
    r.cpu_s[role] += sec;
    if (role == kRoleOther) r.other_detail[kv.second.name] += sec;
  }
  return r;
}

int64_t ReadStatusKb(const char* key) {
  std::string s;
  if (!ReadFile("/proc/self/status", &s)) return -1;
  std::string pat = std::string(key) + ":";
  size_t p = s.find(pat);
  if (p == std::string::npos) return -1;
  return strtoll(s.c_str() + p + pat.size(), nullptr, 10);
}

// ---------------------------------------------------------------------------
// configuration
// ---------------------------------------------------------------------------
struct Cfg {
  std::string db_path = "./dbcpu";
  std::string out_prefix = "fillcpu";
  int64_t num_ops = 96000000;  // 96M x (16B key + 1024B value) = 99.84GB = "100GB"
  int64_t universe = 96000000;
  int threads = 16;
  int key_size = 16;
  int value_size = 1024;
  int sample_ms = 200;
  double progress_ms = 10000;
  std::string dev_name = "nvme0n1";  // /proc/diskstats row sampled for device bandwidth
  uint32_t seed = 7;
  int bg_jobs = 0;         // 0 = library default (2 -> 1 flush + 1 compaction)
  int subcomp = 0;         // 0 = library default (1)
  double drain_max_s = 0;  // >0: after writers finish, wait (max this long)
  bool drain_require_l0_zero = false;  // L0<trigger stays by design; non-zero only for special runs
  double disk_guard_gb = 60;
  bool destroy_first = false;
  bool perm_check = false;
  // --- flush-characterization overrides (all default-off: run IS the library
  // default unless explicitly set; each override is recorded in .config.txt) ---
  bool disable_compactions = false;  // flush-only: no auto compaction, stall triggers lifted
  bool disable_wal = false;          // materialization replay: no WAL write
  int64_t write_buffer_size = 0;     // 0 = library default (64MB)
  int max_bg_flushes = 0;            // 0 = library default (-1 => derived from max_background_jobs)
  bool direct_io = false;            // use_direct_io_for_flush_and_compaction
} g;

const char* kUsage =
    "Usage: fillcpu --db=PATH --out=PREFIX [options]\n"
    "  --num_ops=N          total writes (default 96000000 = 99.84GB of KV)\n"
    "  --universe=N         key id space (default 96000000)\n"
    "  --threads=N          writer threads (default 16)\n"
    "  --value_size=B       value bytes (default 1024)\n"
    "  --sample_ms=N        memory/thread sampling period (default 200)\n"
    "  --dev=NAME           /proc/diskstats device sampled for bandwidth/busy\n"
    "                       (default nvme0n1; empty string disables)\n"
    "  --progress_ms=N      progress print period (default 10000)\n"
    "  --seed=N             permutation seed\n"
    "  --bg_jobs=N          max_background_jobs override (default: library 2)\n"
    "  --subcomp=N          max_subcompactions override (default: library 1)\n"
    "  --drain_max_s=S      after fill, wait up to S s for flush+compaction to\n"
    "                       quiesce and measure that phase too (0 = skip)\n"
    "  --drain_l0_zero=0|1  also require L0==0 to end drain (default 0)\n"
    "  --disk_guard_gb=G    abort writers if free space < G (default 60)\n"
    "  --destroy_first=0|1  DestroyDB before open (default 0)\n"
    "  --perm_check=0|1     verify the key permutation is a bijection, exit\n"
    "  --disable_compactions=1  flush-only run: disable_auto_compactions + lift\n"
    "                       l0 slowdown/stop and pending-compaction stall triggers\n"
    "  --disable_wal=1      no WAL write (materialization-replay arm)\n"
    "  --write_buffer_size=B  write_buffer_size override (0 = library 64MB)\n"
    "  --max_bg_flushes=N   max_background_flushes override (0 = library -1)\n"
    "  --direct_io=1        use_direct_io_for_flush_and_compaction\n"
    "\n"
    "NOTE: no RocksDB option other than create_if_missing is set unless one of\n"
    "the override flags above is given: without them this run IS the library\n"
    "default configuration.\n";

uint64_t ParseU64(const std::string& s) {
  uint64_t mul = 1;
  std::string t = s;
  if (!t.empty()) {
    char c = t.back();
    if (c == 'k' || c == 'K') { mul = 1ull << 10; t.pop_back(); }
    else if (c == 'm' || c == 'M') { mul = 1ull << 20; t.pop_back(); }
    else if (c == 'g' || c == 'G') { mul = 1ull << 30; t.pop_back(); }
  }
  return strtoull(t.c_str(), nullptr, 10) * mul;
}

void ParseArgs(int argc, char** argv) {
  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    if (a == "--help" || a == "-h") { printf("%s", kUsage); exit(0); }
    if (a.rfind("--", 0) != 0) { fprintf(stderr, "bad arg: %s\n", a.c_str()); exit(2); }
    a = a.substr(2);
    std::string k, v;
    size_t eq = a.find('=');
    if (eq == std::string::npos) { k = a; v = "1"; } else { k = a.substr(0, eq); v = a.substr(eq + 1); }
    auto B = [&]() { return v == "1" || v == "true"; };
    if (k == "db") g.db_path = v;
    else if (k == "out") g.out_prefix = v;
    else if (k == "num_ops") g.num_ops = (int64_t)ParseU64(v);
    else if (k == "universe") g.universe = (int64_t)ParseU64(v);
    else if (k == "threads") g.threads = atoi(v.c_str());
    else if (k == "value_size") g.value_size = atoi(v.c_str());
    else if (k == "sample_ms") g.sample_ms = atoi(v.c_str());
    else if (k == "dev") g.dev_name = v;
    else if (k == "progress_ms") g.progress_ms = atof(v.c_str());
    else if (k == "seed") g.seed = (uint32_t)ParseU64(v);
    else if (k == "bg_jobs") g.bg_jobs = atoi(v.c_str());
    else if (k == "subcomp") g.subcomp = atoi(v.c_str());
    else if (k == "drain_max_s") g.drain_max_s = atof(v.c_str());
    else if (k == "drain_l0_zero") g.drain_require_l0_zero = B();
    else if (k == "disk_guard_gb") g.disk_guard_gb = atof(v.c_str());
    else if (k == "destroy_first") g.destroy_first = B();
    else if (k == "perm_check") g.perm_check = B();
    else if (k == "disable_compactions") g.disable_compactions = B();
    else if (k == "disable_wal") g.disable_wal = B();
    else if (k == "write_buffer_size") g.write_buffer_size = (int64_t)ParseU64(v);
    else if (k == "max_bg_flushes") g.max_bg_flushes = atoi(v.c_str());
    else if (k == "direct_io") g.direct_io = B();
    else { fprintf(stderr, "unknown flag: --%s\n", k.c_str()); exit(2); }
  }
}

// ---------------------------------------------------------------------------
// key id permutation: 28-bit Feistel + cycle walking -> bijection on [0,N),
// O(1) memory and pseudo-random order (no 768MB permutation table that would
// pollute the process memory measurement).
// ---------------------------------------------------------------------------
inline uint32_t Mix32(uint32_t x, uint32_t salt) {
  x += salt * 0x9E3779B9u;
  x ^= x >> 16; x *= 0x7feb352du;
  x ^= x >> 15; x *= 0x846ca68bu;
  x ^= x >> 16;
  return x;
}

inline uint32_t Feistel28(uint32_t x, uint32_t seed) {
  uint32_t l = x >> 14, r = x & 0x3FFFu;  // 14/14 bit halves
  for (uint32_t i = 0; i < 4; i++) {
    uint32_t f = Mix32(r, seed + 0x1111u * i) & 0x3FFFu;
    uint32_t nl = r;
    uint32_t nr = l ^ f;
    l = nl; r = nr;
  }
  return (l << 14) | r;
}

inline uint32_t Permute(uint32_t x, uint32_t seed, uint32_t n) {
  uint32_t v = Feistel28(x, seed);
  while (v >= n) v = Feistel28(v, seed);
  return v;
}

// 16B key = 8B space tag (0x01..) + 8B big-endian id -- same layout as the
// existing bench harness so keys are byte-comparable with prior runs.
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

// ---------------------------------------------------------------------------
// ledger / listener
// ---------------------------------------------------------------------------
struct JobAgg {
  std::atomic<int64_t> flush_jobs{0}, comp_jobs{0}, subcomp_jobs{0};
  std::atomic<int64_t> flush_cpu_us{0}, flush_wall_us{0}, flush_bytes{0}, flush_entries{0};
  std::atomic<int64_t> comp_cpu_us_self{0}, comp_cpu_us_rdb{0}, comp_wall_us{0};
  std::atomic<int64_t> comp_in_bytes{0}, comp_out_bytes{0}, comp_in_records{0}, comp_out_records{0};
  std::atomic<int64_t> stall_begin{0}, stall_end{0}, memtable_sealed{0}, memtable_sealed_entries{0};
  std::atomic<int64_t> stall_us_total{0};
  std::atomic<double> stall_open_ts{-1};
  std::atomic<int> phase{0};  // 0=open,1=fill,2=drain
};

JobAgg A;
std::mutex g_log_mu;
FILE* f_flush_jobs = nullptr;
FILE* f_comp_jobs = nullptr;
FILE* f_stalls = nullptr;

struct FlushStart {
  double ts = 0;
  uint64_t cpu = 0;
  int job_id = -1;
  IOStatsContext io{};
};

class Ledger : public EventListener {
 public:
  void OnFlushBegin(DB*, const FlushJobInfo& info) override {
    // IOSTATS/perf timers are gated by the (thread_local) perf level; the
    // callback runs on the flush thread before flush_job.Run(), so enabling
    // here covers the whole job.
    if (GetPerfLevel() < PerfLevel::kEnableTime) SetPerfLevel(PerfLevel::kEnableTime);
    FlushStart s;
    s.ts = WallSec();
    s.cpu = CpuUs();
    s.job_id = info.job_id;
    s.io = iostats_context;
    std::lock_guard<std::mutex> lk(mu_);
    flush_start_[std::make_pair(info.thread_id, info.job_id)] = s;
  }

  void OnFlushCompleted(DB*, const FlushJobInfo& info) override {
    double ts_end = WallSec();
    uint64_t cpu_end = CpuUs();
    FlushStart s;
    {
      std::lock_guard<std::mutex> lk(mu_);
      auto it = flush_start_.find(std::make_pair(info.thread_id, info.job_id));
      if (it != flush_start_.end()) { s = it->second; flush_start_.erase(it); }
      else { s.ts = ts_end; s.cpu = cpu_end; s.job_id = info.job_id; }
    }
    int64_t cpu = cpu_end > s.cpu ? (int64_t)(cpu_end - s.cpu) : 0;
    int64_t data_size = (int64_t)info.table_properties.data_size;
    int64_t entries = (int64_t)info.table_properties.num_entries;
    A.flush_jobs.fetch_add(1);
    A.flush_cpu_us.fetch_add(cpu);
    if (ts_end > s.ts) A.flush_wall_us.fetch_add((int64_t)((ts_end - s.ts) * 1e6));
    A.flush_bytes.fetch_add(data_size);
    A.flush_entries.fetch_add(entries);
    IOStatsContext io_now = iostats_context;
    std::lock_guard<std::mutex> lk(g_log_mu);
    if (f_flush_jobs) {
      fprintf(f_flush_jobs, "%.6f,%.6f,%lld,%llu,%d,%lld,%lld,%d,%d,%d,%.1f,"
                            "%llu,%llu,%llu,%llu,%llu\n",
              s.ts, ts_end, (long long)cpu, (unsigned long long)info.thread_id, s.job_id,
              (long long)data_size, (long long)entries,
              (int)info.triggered_writes_slowdown, (int)info.triggered_writes_stop,
              (int)A.phase.load(), (ts_end - s.ts) * 1e6,
              (unsigned long long)(io_now.bytes_written - s.io.bytes_written),
              (unsigned long long)(io_now.write_nanos - s.io.write_nanos),
              (unsigned long long)(io_now.fsync_nanos - s.io.fsync_nanos),
              (unsigned long long)(io_now.range_sync_nanos - s.io.range_sync_nanos),
              (unsigned long long)(io_now.cpu_write_nanos - s.io.cpu_write_nanos));
      fflush(f_flush_jobs);
    }
  }

  void OnCompactionBegin(DB*, const CompactionJobInfo& ci) override {
    if (GetPerfLevel() < PerfLevel::kEnableTime) SetPerfLevel(PerfLevel::kEnableTime);
    CompStart s;
    s.ts = WallSec();
    s.cpu = CpuUs();
    s.io = iostats_context;  // thread_local: this callback runs on the compaction thread
    std::lock_guard<std::mutex> lk(mu_);
    comp_start_[ci.thread_id] = s;
  }

  void OnCompactionCompleted(DB*, const CompactionJobInfo& ci) override {
    double ts_end = WallSec();
    uint64_t cpu_end = CpuUs();
    CompStart s;
    {
      std::lock_guard<std::mutex> lk(mu_);
      auto it = comp_start_.find(ci.thread_id);
      if (it != comp_start_.end()) { s = it->second; comp_start_.erase(it); }
      else { s.ts = ts_end; s.cpu = cpu_end; }
    }
    int64_t cpu_self = cpu_end > s.cpu ? (int64_t)(cpu_end - s.cpu) : 0;
    int64_t cpu_rdb = (int64_t)ci.stats.cpu_micros;
    A.comp_jobs.fetch_add(1);
    A.comp_cpu_us_self.fetch_add(cpu_self);
    A.comp_cpu_us_rdb.fetch_add(cpu_rdb);
    if (ts_end > s.ts) A.comp_wall_us.fetch_add((int64_t)((ts_end - s.ts) * 1e6));
    A.comp_in_records.fetch_add((int64_t)ci.stats.num_input_records);
    A.comp_out_records.fetch_add((int64_t)ci.stats.num_output_records);

    int64_t in_bytes = 0, out_bytes = 0;
    long long lvl_files[7] = {0, 0, 0, 0, 0, 0, 0};
    for (size_t i = 0; i < ci.input_file_infos.size(); i++) {
      int lv = ci.input_file_infos[i].level;
      auto it = ci.table_properties.find(ci.input_files[i]);
      in_bytes += (int64_t)((it != ci.table_properties.end()) ? it->second->data_size : 0);
      if (lv >= 0 && lv < 7) lvl_files[lv]++;
    }
    for (size_t i = 0; i < ci.output_file_infos.size(); i++) {
      auto it = ci.table_properties.find(ci.output_files[i]);
      out_bytes += (int64_t)((it != ci.table_properties.end()) ? it->second->data_size : 0);
    }
    A.comp_in_bytes.fetch_add(in_bytes);
    A.comp_out_bytes.fetch_add(out_bytes);
    IOStatsContext io_now = iostats_context;
    const auto& st = ci.stats;
    std::lock_guard<std::mutex> lk(g_log_mu);
    if (f_comp_jobs) {
      fprintf(f_comp_jobs,
              "%.6f,%.6f,%lld,%lld,%.1f,%lld,%lld,%lld,%lld,%lld,%.6f,"
              "%d,%d,%d,%d,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%d,%llu,"
              "%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,"
              "%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu\n",
              s.ts, ts_end, (long long)cpu_self, (long long)cpu_rdb, (ts_end - s.ts) * 1e6,
              (long long)in_bytes, (long long)out_bytes,
              (long long)st.num_input_records, (long long)st.num_output_records,
              (long long)st.elapsed_micros, (double)st.cpu_micros / 1e6,
              ci.base_input_level, ci.output_level, (int)ci.input_file_infos.size(),
              (int)ci.output_file_infos.size(), lvl_files[0], lvl_files[1], lvl_files[2],
              lvl_files[3], lvl_files[4], lvl_files[5], lvl_files[6],
              (int)A.phase.load(),
              (unsigned long long)ci.thread_id,
              (unsigned long long)st.file_write_nanos, (unsigned long long)st.file_fsync_nanos,
              (unsigned long long)st.file_range_sync_nanos,
              (unsigned long long)st.file_prepare_write_nanos,
              (unsigned long long)st.total_input_bytes,
              (unsigned long long)st.total_output_bytes,
              (unsigned long long)st.total_skipped_input_bytes,
              (unsigned long long)st.num_records_replaced,
              (unsigned long long)(io_now.bytes_read - s.io.bytes_read),
              (unsigned long long)(io_now.bytes_written - s.io.bytes_written),
              (unsigned long long)(io_now.read_nanos - s.io.read_nanos),
              (unsigned long long)(io_now.write_nanos - s.io.write_nanos),
              (unsigned long long)(io_now.fsync_nanos - s.io.fsync_nanos),
              (unsigned long long)(io_now.range_sync_nanos - s.io.range_sync_nanos),
              (unsigned long long)(io_now.cpu_read_nanos - s.io.cpu_read_nanos),
              (unsigned long long)(io_now.cpu_write_nanos - s.io.cpu_write_nanos));
      fflush(f_comp_jobs);
    }
  }

  void OnSubcompactionCompleted(const SubcompactionJobInfo& si) override {
    A.subcomp_jobs.fetch_add(1);
    (void)si;
  }

  void OnStallConditionsChanged(const WriteStallInfo& info) override {
    double ts = WallSec();
    const char* st = "?";
    switch (info.condition.cur) {
      case WriteStallCondition::kNormal: st = "normal"; break;
      case WriteStallCondition::kDelayed: st = "delayed"; break;
      case WriteStallCondition::kStopped: st = "stopped"; break;
    }
    if (info.condition.cur == WriteStallCondition::kNormal) {
      A.stall_end.fetch_add(1);
      double b = A.stall_open_ts.load();
      if (b >= 0) {
        A.stall_us_total.fetch_add((int64_t)((ts - b) * 1e6));
        A.stall_open_ts.store(-1);
      }
    } else {
      A.stall_begin.fetch_add(1);
      double expect = -1;
      A.stall_open_ts.compare_exchange_strong(expect, ts);
    }
    std::lock_guard<std::mutex> lk(g_log_mu);
    if (f_stalls) {
      fprintf(f_stalls, "%.6f,%s,%s,%d,%d\n", ts, info.cf_name.c_str(), st,
              (int)info.condition.cur, (int)A.phase.load());
      fflush(f_stalls);
    }
  }

  void OnMemTableSealed(const MemTableInfo& info) override {
    A.memtable_sealed.fetch_add(1);
    A.memtable_sealed_entries.fetch_add((int64_t)info.num_entries);
  }

 private:
  struct CompStart {
    double ts = 0;
    uint64_t cpu = 0;
    IOStatsContext io{};
  };
  std::mutex mu_;
  std::map<std::pair<uint64_t, int>, FlushStart> flush_start_;  // key: (thread_id, job_id)
  std::map<uint64_t, CompStart> comp_start_;    // key: thread_id
};

// ---------------------------------------------------------------------------
// memory / progress sampling
// ---------------------------------------------------------------------------
DB* g_db = nullptr;
std::atomic<bool> g_stop_sampler{false};
std::atomic<int64_t> g_ops{0}, g_ops_err{0}, g_bytes{0};
std::atomic<int> g_writers_done{0};
std::atomic<int> g_phase{0};

FILE* f_samples = nullptr;
FILE* f_threads = nullptr;
FILE* f_cfstats_tl = nullptr;
long cfstats_samples = 0;
std::mutex g_samples_mu;

int64_t PropI(DB* db, const char* name) {
  uint64_t v = 0;
  if (db && db->GetIntProperty(name, &v)) return (int64_t)v;
  return -1;
}

int64_t PropS(DB* db, const char* name) {
  std::string s;
  if (db && db->GetProperty(name, &s)) return strtoll(s.c_str(), nullptr, 10);
  return -1;
}

// /proc/meminfo page-cache counters for one keyword (kB), -1 if absent.
int64_t ReadMeminfoKb(const char* keyword) {
  FILE* f = fopen("/proc/meminfo", "r");
  if (!f) return -1;
  char line[256];
  int64_t out = -1;
  size_t kl = strlen(keyword);
  while (fgets(line, sizeof(line), f)) {
    if (strncmp(line, keyword, kl) == 0 && line[kl] == ':') {
      out = strtoll(line + kl + 1, nullptr, 10);
      break;
    }
  }
  fclose(f);
  return out;
}

// /proc/diskstats counters for one device: sectors (512B units), completed I/Os,
// and io_ticks (ms the block layer had I/O in flight -> device busy fraction).
// Field order per Documentation/admin-guide/iostats.rst; same parse as l0bench.
void ReadDiskStats(uint64_t* wr_sect, uint64_t* wr_ios, uint64_t* rd_sect,
                   uint64_t* rd_ios, uint64_t* io_ticks_ms) {
  *wr_sect = *wr_ios = *rd_sect = *rd_ios = *io_ticks_ms = 0;
  if (g.dev_name.empty()) return;
  FILE* f = fopen("/proc/diskstats", "r");
  if (!f) return;
  char line[512];
  while (fgets(line, sizeof(line), f)) {
    char dev[64];
    unsigned long long maj, mi, rc, rm, sr, mr, wc, wm, sw, mw, infl, ticks;
    int n = sscanf(line, "%llu %llu %63s %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
                   &maj, &mi, dev, &rc, &rm, &sr, &mr, &wc, &wm, &sw, &mw, &infl, &ticks);
    if (n < 11) continue;
    if (g.dev_name == dev) {
      *rd_ios = rc; *rd_sect = sr; *wr_ios = wc; *wr_sect = sw;
      if (n >= 13) *io_ticks_ms = ticks;
      break;
    }
  }
  fclose(f);
}

void SampleOnce() {
  double ts = WallSec();
  int phase = g_phase.load();
  int64_t rss_kb = ReadStatusKb("VmRSS");
  int64_t hwm_kb = ReadStatusKb("VmHWM");
  int64_t vsize_kb = ReadStatusKb("VmSize");
  int64_t nthreads = ReadStatusKb("Threads");
  int64_t mall_used = -1, mall_hblkhd = -1, mall_arena = -1;
#if defined(__GLIBC__) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 33)
  struct mallinfo2 mi = mallinfo2();
  mall_used = (int64_t)mi.uordblks;
  mall_hblkhd = (int64_t)mi.hblkhd;
  mall_arena = (int64_t)mi.arena;
#endif
#endif
  DB* db = g_db;
  int64_t mem_active = PropI(db, "rocksdb.cur-size-active-mem-table");
  int64_t mem_cur_all = PropI(db, "rocksdb.cur-size-all-mem-tables");
  int64_t mem_size_all = PropI(db, "rocksdb.size-all-mem-tables");
  int64_t num_imm = PropI(db, "rocksdb.num-immutable-mem-table");
  int64_t table_readers = PropI(db, "rocksdb.estimate-table-readers-mem");
  int64_t bcache_usage = PropI(db, "rocksdb.block-cache-usage");
  int64_t bcache_pinned = PropI(db, "rocksdb.block-cache-pinned-usage");
  int64_t bcache_cap = PropI(db, "rocksdb.block-cache-capacity");
  int64_t l0_files = PropS(db, "rocksdb.num-files-at-level0");
  int64_t running_flushes = PropI(db, "rocksdb.num-running-flushes");
  int64_t running_comps = PropI(db, "rocksdb.num-running-compactions");
  int64_t pending_comp = PropI(db, "rocksdb.estimate-pending-compaction-bytes");
  int64_t write_stopped = PropI(db, "rocksdb.is-write-stopped");
  int64_t newest_uln = PropS(db, "rocksdb.latest-sequence-number");
  int64_t ops_cum = g_ops.load();
  int64_t bytes_cum = g_bytes.load();
  int64_t flush_jobs = A.flush_jobs.load();
  int64_t flush_bytes = A.flush_bytes.load();
  int64_t comp_jobs = A.comp_jobs.load();
  double flush_cpu_s = (double)A.flush_cpu_us.load() / 1e6;
  double comp_cpu_s = (double)A.comp_cpu_us_rdb.load() / 1e6;
  double stall = (double)A.stall_us_total.load() / 1e6;
  double open_ts = A.stall_open_ts.load();
  if (open_ts >= 0 && phase == 1) stall += (ts - open_ts);  // writers-active stall only
  int64_t free_gb = -1;
  struct statvfs vfs;
  if (statvfs(g.db_path.c_str(), &vfs) == 0) {
    free_gb = (int64_t)((double)vfs.f_bavail * vfs.f_frsize / 1073741824.0);
  }
  uint64_t dev_wr_sect = 0, dev_wr_ios = 0, dev_rd_sect = 0, dev_rd_ios = 0, dev_ticks = 0;
  ReadDiskStats(&dev_wr_sect, &dev_wr_ios, &dev_rd_sect, &dev_rd_ios, &dev_ticks);
  int64_t dirty_kb = ReadMeminfoKb("Dirty");
  int64_t writeback_kb = ReadMeminfoKb("Writeback");
  std::lock_guard<std::mutex> lk(g_samples_mu);
  if (f_samples) {
    fprintf(f_samples,
            "%.6f,%.1f,%d,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,"
            "%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,"
            "%.6f,%.6f,%.6f,%lld,%llu,%llu,%llu,%llu,%llu,%lld,%lld\n",
            ts, ts - g_start_ts, phase, (long long)rss_kb, (long long)hwm_kb,
            (long long)vsize_kb, (long long)nthreads, (long long)mall_used,
            (long long)mall_hblkhd, (long long)mall_arena, (long long)mem_active,
            (long long)mem_cur_all, (long long)mem_size_all, (long long)num_imm,
            (long long)table_readers, (long long)bcache_usage, (long long)bcache_pinned,
            (long long)bcache_cap, (long long)l0_files, (long long)running_flushes,
            (long long)running_comps, (long long)pending_comp, (long long)write_stopped,
            (long long)newest_uln, (long long)ops_cum, (long long)g_ops_err.load(),
            (long long)bytes_cum, flush_cpu_s, comp_cpu_s, stall, (long long)free_gb,
            (unsigned long long)dev_wr_sect, (unsigned long long)dev_wr_ios,
            (unsigned long long)dev_rd_sect, (unsigned long long)dev_rd_ios,
            (unsigned long long)dev_ticks, (long long)dirty_kb, (long long)writeback_kb);
    fflush(f_samples);
  }
}

void SamplerLoop() {
  pthread_setname_np(pthread_self(), "fc-samp");
  double last_progress_ts = WallSec();
  int64_t last_ops = 0;
  while (!g_stop_sampler.load()) {
    double t0 = WallSec();
    SampleOnce();
    {
      ThreadMap tm;
      SnapshotThreads(&tm);
      std::lock_guard<std::mutex> lk(g_samples_mu);
      if (f_threads) {
        for (const auto& kv : tm) {
          fprintf(f_threads, "%.6f,%d,%s,%c,%llu,%llu\n", WallSec(), (int)kv.first,
                  kv.second.name.c_str(), kv.second.state, (unsigned long long)kv.second.utime,
                  (unsigned long long)kv.second.stime);
        }
        fflush(f_threads);
      }
    }
    if (f_cfstats_tl && (int)cfstats_samples % 40 == 0) {
      double t = WallSec();
      std::string v;
      if (g_db->GetProperty("rocksdb.cfstats", &v)) {
        std::lock_guard<std::mutex> lk(g_samples_mu);
        fprintf(f_cfstats_tl, "### ts=%.6f elapsed=%.3f\n%s\n", t, t - g_start_ts, v.c_str());
        fflush(f_cfstats_tl);
      }
    }
    cfstats_samples++;
    double now = WallSec();
    if (now - last_progress_ts >= g.progress_ms / 1000.0) {
      int64_t ops = g_ops.load();
      double rate = (double)(ops - last_ops) / (now - last_progress_ts);
      last_progress_ts = now;
      last_ops = ops;
      printf("[%.1fs] phase=%d ops=%lld (%.0f/s) flush=%lld jobs %.1fs cpu "
             "comp=%lld jobs %.1fs cpu rss=%.0fMB memtbl=%.0fMB bcache=%.0fMB "
             "l0=%lld fl=%lld comp=%lld stall=%.0fs\n",
             now - g_start_ts, g_phase.load(), (long long)ops, rate,
             (long long)A.flush_jobs.load(), (double)A.flush_cpu_us.load() / 1e6,
             (long long)A.comp_jobs.load(), (double)A.comp_cpu_us_rdb.load() / 1e6,
             (double)ReadStatusKb("VmRSS") / 1024.0,
             (double)PropI(g_db, "rocksdb.cur-size-all-mem-tables") / 1048576.0,
             (double)PropI(g_db, "rocksdb.block-cache-usage") / 1048576.0,
             (long long)PropS(g_db, "rocksdb.num-files-at-level0"),
             (long long)PropI(g_db, "rocksdb.num-running-flushes"),
             (long long)PropI(g_db, "rocksdb.num-running-compactions"),
             (double)A.stall_us_total.load() / 1e6);
      fflush(stdout);
    }
    double dt = (WallSec() - t0) * 1000.0;
    double wait = g.sample_ms - dt;
    if (wait > 1) std::this_thread::sleep_for(std::chrono::microseconds((long)(wait * 1000)));
  }
}

// ---------------------------------------------------------------------------
// writers
// ---------------------------------------------------------------------------
struct WriterCtx {
  int idx = 0;
  int64_t begin = 0, end = 0;
  std::atomic<bool>* go = nullptr;
  std::atomic<bool>* disk_stop = nullptr;
  std::atomic<bool>* release = nullptr;
};

void WriterLoop(WriterCtx* c) {
  pthread_setname_np(pthread_self(), Fmt("fcw%02d", c->idx).c_str());
  std::string key((size_t)g.key_size, '\0');
  std::string value;
  value.reserve((size_t)g.value_size);
  WriteOptions wo;  // library default: sync=false, WAL enabled
  wo.disableWAL = g.disable_wal;
  while (!c->go->load()) std::this_thread::yield();
  int64_t n = 0;
  for (int64_t x = c->begin; x < c->end; x++) {
    uint32_t id = Permute((uint32_t)x, g.seed, (uint32_t)g.universe);
    EncodeKey(id, &key[0]);
    FillValue(id, 1, g.value_size, &value);
    Status s = g_db->Put(wo, Slice(key), Slice(value));
    if (!s.ok()) { g_ops_err.fetch_add(1); continue; }
    n++;
    g_ops.fetch_add(1);
    g_bytes.fetch_add((int64_t)g.key_size + g.value_size);
    if ((n & 0x3FF) == 0 && c->disk_stop->load()) break;
  }
  // Signal completion, then stay alive (idle, no CPU) until the fill-end
  // thread snapshots are taken: /proc hides exited threads and their CPU
  // would be dropped from the per-role attribution.
  g_writers_done.fetch_add(1);
  while (!c->release->load()) std::this_thread::sleep_for(std::chrono::milliseconds(2));
}

// ---------------------------------------------------------------------------
int RunPermCheck() {
  uint32_t n = (uint32_t)g.universe;
  std::vector<uint8_t> seen(n, 0);
  for (uint32_t x = 0; x < n; x++) {
    uint32_t v = Permute(x, g.seed, n);
    if (v >= n) { fprintf(stderr, "perm out of range: %u\n", v); return 1; }
    if (seen[v]) { fprintf(stderr, "perm collision at %u\n", v); return 1; }
    seen[v] = 1;
  }
  printf("perm_check OK: bijection over [0,%u)\n", n);
  return 0;
}

void DumpCfStats(DB* db, const std::string& path) {
  std::string v;
  if (db->GetProperty("rocksdb.cfstats", &v)) {
    FILE* f = fopen(path.c_str(), "w");
    if (f) { fputs(v.c_str(), f); fclose(f); }
  }
}

}  // namespace

int main(int argc, char** argv) {
  ParseArgs(argc, argv);
  if (g.perm_check) return RunPermCheck();
  if (g.key_size != 16) { fprintf(stderr, "key_size must be 16\n"); return 2; }
  if (g.universe > (int64_t)0xFFFFFFF) {
    fprintf(stderr, "universe must be < 2^28 for the Feistel permutation\n");
    return 2;
  }
  if (g.num_ops > g.universe) { fprintf(stderr, "num_ops > universe\n"); return 2; }

  pthread_setname_np(pthread_self(), "fc-main");
  g_start_ts = WallSec();
  char host[256] = {0};
  gethostname(host, sizeof(host) - 1);

  {
    size_t dot = g.out_prefix.find_last_of('/');
    if (dot != std::string::npos) {
      std::string dir = g.out_prefix.substr(0, dot);
      std::string cmd = "mkdir -p '" + dir + "'";
      if (system(cmd.c_str()) != 0) { /* best effort */ }
    }
  }

  FILE* cfg = fopen((g.out_prefix + ".config.txt").c_str(), "w");
  if (cfg) {
    fprintf(cfg, "argv:");
    for (int i = 0; i < argc; i++) fprintf(cfg, " %s", argv[i]);
    fprintf(cfg, "\nrocksdb_version=%d.%d.%d\nstart_wall=%.6f\npid=%d\nhost=%s\n",
            ROCKSDB_MAJOR, ROCKSDB_MINOR, ROCKSDB_PATCH, g_start_ts, (int)getpid(), host);
    fclose(cfg);
  }

  f_flush_jobs = fopen((g.out_prefix + ".flush_jobs.csv").c_str(), "w");
  f_comp_jobs = fopen((g.out_prefix + ".comp_jobs.csv").c_str(), "w");
  f_stalls = fopen((g.out_prefix + ".stalls.csv").c_str(), "w");
  f_samples = fopen((g.out_prefix + ".samples.csv").c_str(), "w");
  f_threads = fopen((g.out_prefix + ".thread_cpu.csv").c_str(), "w");
  f_cfstats_tl = fopen((g.out_prefix + ".cfstats_timeline.txt").c_str(), "w");
  if (!f_flush_jobs || !f_comp_jobs || !f_stalls || !f_samples || !f_threads) {
    fprintf(stderr, "cannot open output CSVs under %s\n", g.out_prefix.c_str());
    return 2;
  }
  fprintf(f_flush_jobs,
          "ts_begin,ts_end,cpu_us,thread_id,job_id,data_bytes,num_entries,slowdown,stop,phase,wall_us,"
          "io_bytes_written,io_write_ns,io_fsync_ns,io_range_sync_ns,io_cpu_write_ns\n");
  fprintf(f_comp_jobs,
          "ts_begin,ts_end,cpu_us_self,cpu_us_rdb,wall_us,in_bytes,out_bytes,in_records,"
          "out_records,rdb_elapsed_us,rdb_cpu_s,base_level,out_level,in_files,out_files,"
          "in_l0,in_l1,in_l2,in_l3,in_l4,in_l5,in_l6,phase,thread_id,"
          "file_write_ns,file_fsync_ns,file_range_sync_ns,file_prepare_write_ns,"
          "total_in_bytes,total_out_bytes,skipped_in_bytes,replaced_records,"
          "io_bytes_read,io_bytes_written,io_read_ns,io_write_ns,io_fsync_ns,"
          "io_range_sync_ns,io_cpu_read_ns,io_cpu_write_ns\n");
  fprintf(f_stalls, "ts,cf,state,code,phase\n");
  fprintf(f_samples,
          "ts,elapsed,phase,rss_kb,hwm_kb,vsize_kb,nthreads,mall_used,mall_hblkhd,mall_arena,"
          "mem_active,mem_cur_all,mem_size_all,num_imm,table_readers,bcache_usage,"
          "bcache_pinned,bcache_cap,l0_files,running_flushes,running_comps,pending_comp,"
          "write_stopped,newest_seq,ops_cum,ops_err,bytes_cum,flush_cpu_s,comp_cpu_s,"
          "stall_s,free_gb,dev_wr_sectors,dev_wr_ios,dev_rd_sectors,dev_rd_ios,"
          "dev_io_ticks_ms,dirty_kb,writeback_kb\n");
  fprintf(f_threads, "ts,tid,name,state,utime_ticks,stime_ticks\n");

  auto listener = std::make_shared<Ledger>();

  if (g.destroy_first) {
    Options o;
    DestroyDB(g.db_path, o);
  }

  // ---- the only non-default things: create_if_missing + an observation-only
  //      listener.  Every other field is the library default unless one of the
  //      flush-characterization override flags was given.
  Options opt;
  opt.create_if_missing = true;
  if (g.bg_jobs > 0) opt.max_background_jobs = g.bg_jobs;
  if (g.subcomp > 0) opt.max_subcompactions = g.subcomp;
  if (g.write_buffer_size > 0) opt.write_buffer_size = (size_t)g.write_buffer_size;
  if (g.max_bg_flushes > 0) opt.max_background_flushes = g.max_bg_flushes;
  if (g.direct_io) opt.use_direct_io_for_flush_and_compaction = true;
  if (g.disable_compactions) {
    // flush-only: background work = memtable->SST flush only.  Every stall
    // trigger that would otherwise deadlock on "compaction will fix it" is
    // lifted; memtable-limit remains as the pipeline-capacity signal.
    opt.disable_auto_compactions = true;
    opt.level0_slowdown_writes_trigger = 1000000;
    opt.level0_stop_writes_trigger = 1000000;
    opt.soft_pending_compaction_bytes_limit = 1ull << 42;
    opt.hard_pending_compaction_bytes_limit = 1ull << 42;
  }
  opt.listeners.push_back(listener);

  std::unique_ptr<DB> db;
  Status s = DB::Open(opt, g.db_path, &db);
  if (!s.ok()) { fprintf(stderr, "Open failed: %s\n", s.ToString().c_str()); return 3; }
  g_db = db.get();
  DB* raw = db.get();  // convenience raw pointer for the main-flow reads
  {
    std::string dbo, cfo;
    GetStringFromDBOptions(&dbo, db->GetDBOptions());
    GetStringFromColumnFamilyOptions(&cfo, db->GetOptions());
    FILE* f = fopen((g.out_prefix + ".effective_options.txt").c_str(), "w");
    if (f) {
      fprintf(f, "# effective DBOptions (library defaults, as reported by the library)\n%s\n\n",
              dbo.c_str());
      fprintf(f, "# effective ColumnFamilyOptions (library defaults)\n%s\n", cfo.c_str());
      fclose(f);
    }
    printf("opened db=%s write_buffer_size=%zu max_write_buffer_number=%d\n",
           g.db_path.c_str(), opt.write_buffer_size, opt.max_write_buffer_number);
    fflush(stdout);
  }

  Snap s_open;
  TakeSnap(&s_open);

  std::thread sampler(SamplerLoop);

  // ---- fill phase
  g_phase.store(1);
  A.phase.store(1);
  std::atomic<bool> go{false};
  std::atomic<bool> disk_stop{false};
  std::atomic<bool> release{false};
  int64_t chunk = g.num_ops / g.threads;
  std::vector<WriterCtx> ctxs((size_t)g.threads);
  std::vector<std::thread> writers;
  for (int i = 0; i < g.threads; i++) {
    ctxs[i].idx = i;
    ctxs[i].begin = (int64_t)i * chunk;
    ctxs[i].end = (i == g.threads - 1) ? g.num_ops : (int64_t)(i + 1) * chunk;
    ctxs[i].go = &go;
    ctxs[i].disk_stop = &disk_stop;
    ctxs[i].release = &release;
  }

  Snap s_fill_t0, s_fill_t1;
  double fill_t0 = 0, fill_t1 = 0;
  TakeSnap(&s_fill_t0);
  fill_t0 = s_fill_t0.ts;
  for (int i = 0; i < g.threads; i++) writers.emplace_back(WriterLoop, &ctxs[i]);
  go.store(true);

  std::thread guard([&]() {
    pthread_setname_np(pthread_self(), "fc-guard");
    while (!disk_stop.load() && g_phase.load() == 1) {
      struct statvfs vfs;
      if (statvfs(g.db_path.c_str(), &vfs) == 0) {
        int64_t free_gb = (int64_t)((double)vfs.f_bavail * vfs.f_frsize / 1073741824.0);
        if (free_gb >= 0 && free_gb < (int64_t)g.disk_guard_gb) {
          fprintf(stderr, "DISK GUARD: free=%lldGB < %lldGB, stopping writers\n",
                  (long long)free_gb, (long long)g.disk_guard_gb);
          disk_stop.store(true);
          break;
        }
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  });

  // wait for all writers to run out of work, then snapshot while they are
  // still alive (idle).  They are released right after the snapshot.
  while (g_writers_done.load() < g.threads) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  fill_t1 = WallSec();
  TakeSnap(&s_fill_t1);
  release.store(true);
  for (auto& w : writers) w.join();
  disk_stop.store(true);
  guard.join();
  printf("fill done: ops=%lld err=%lld wall=%.1fs (%.1f MB/s app)\n",
         (long long)g_ops.load(), (long long)g_ops_err.load(), fill_t1 - fill_t0,
         (double)g_bytes.load() / (fill_t1 - fill_t0) / 1048576.0);
  fflush(stdout);
  SampleOnce();  // memory snapshot exactly at end of fill
  DumpCfStats(raw, g.out_prefix + ".cfstats_fillend.txt");

  // ---- drain phase
  double drain_t0 = fill_t1, drain_t1 = fill_t1;
  Snap s_drain_t0 = s_fill_t1, s_drain_t1 = s_fill_t1;
  int64_t l0_at_drain_start = PropS(raw, "rocksdb.num-files-at-level0");
  bool drain_hit_limit = false;
  if (g.drain_max_s > 0) {
    g_phase.store(2);
    A.phase.store(2);
    int quiet = 0;
    double last_log = 0;
    while (WallSec() - drain_t0 < g.drain_max_s) {
      int64_t fl = PropI(raw, "rocksdb.num-running-flushes");
      int64_t cp = PropI(raw, "rocksdb.num-running-compactions");
      int64_t imm = PropI(raw, "rocksdb.num-immutable-mem-table");
      int64_t l0 = PropS(raw, "rocksdb.num-files-at-level0");
      bool q = (fl == 0) && (cp == 0) && (imm == 0);
      if (g.drain_require_l0_zero && l0 != 0) q = false;
      quiet = q ? quiet + 1 : 0;
      if (quiet >= 5) break;
      if (WallSec() - last_log >= 10) {
        last_log = WallSec();
        printf("[drain %.0fs] fl=%lld comp=%lld imm=%lld l0=%lld rss=%.0fMB\n",
               WallSec() - drain_t0, (long long)fl, (long long)cp, (long long)imm,
               (long long)l0, (double)ReadStatusKb("VmRSS") / 1024.0);
        fflush(stdout);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    drain_t1 = WallSec();
    if (drain_t1 - drain_t0 >= g.drain_max_s) drain_hit_limit = true;
    TakeSnap(&s_drain_t1);
  }
  DumpCfStats(raw, g.out_prefix + ".cfstats_drainend.txt");

  g_stop_sampler.store(true);
  sampler.join();
  SampleOnce();

  RoleCpu cpu_open = DiffRoles(s_open.tm, s_fill_t0.tm);
  RoleCpu cpu_fill = DiffRoles(s_fill_t0.tm, s_fill_t1.tm);
  RoleCpu cpu_drain = DiffRoles(s_drain_t0.tm, s_drain_t1.tm);

  // ---- final memory readings before close (threads still alive)
  int64_t rss_end_kb = ReadStatusKb("VmRSS");
  int64_t hwm_end_kb = ReadStatusKb("VmHWM");
  int64_t mem_cur_all_end = PropI(raw, "rocksdb.cur-size-all-mem-tables");
  int64_t mem_size_all_end = PropI(raw, "rocksdb.size-all-mem-tables");
  int64_t table_readers_end = PropI(raw, "rocksdb.estimate-table-readers-mem");
  int64_t bcache_usage_end = PropI(raw, "rocksdb.block-cache-usage");
  int64_t bcache_pinned_end = PropI(raw, "rocksdb.block-cache-pinned-usage");
  int64_t l0_end = PropS(raw, "rocksdb.num-files-at-level0");
  int64_t lvl_end[7];
  for (int i = 0; i < 7; i++) lvl_end[i] = PropS(raw, Fmt("rocksdb.num-files-at-level%d", i).c_str());
  int64_t proc_ticks_end = ReadProcSelfCpuTicks();

  // ---- close
  double close_t0 = WallSec();
  uint64_t close_cpu0 = CpuUs();
  Status cs = db->Close();
  int64_t close_cpu = (int64_t)(CpuUs() - close_cpu0);
  double close_wall = WallSec() - close_t0;

  // ---- summary
  FILE* f = fopen((g.out_prefix + ".summary.json").c_str(), "w");
  if (f) {
    auto role_json = [&](const char* phase, const RoleCpu& c, double wall) {
      fprintf(f,
              "  \"%s\": {\"wall_s\": %.3f, \"cpu_s_total\": %.3f,"
              " \"cpu_s\": {\"writer\": %.3f, \"flush_pool\": %.3f,"
              " \"compaction_pool\": %.3f, \"driver\": %.3f, \"other\": %.3f},"
              " \"other_detail\": {",
              phase, wall, c.total(), c.cpu_s[kRoleWriter], c.cpu_s[kRoleFlush],
              c.cpu_s[kRoleCompaction], c.cpu_s[kRoleDriver], c.cpu_s[kRoleOther]);
      bool first = true;
      for (const auto& kv : c.other_detail) {
        fprintf(f, "%s\"%s\": %.3f", first ? "" : ", ", kv.first.c_str(), kv.second);
        first = false;
      }
      fprintf(f, "}},\n");
    };
    auto ticks_s = [&](int64_t t) { return (double)t / (double)ClockTicksPerSec(); };
    fprintf(f, "{\n");
    fprintf(f, "  \"version\": \"%d.%d.%d\",\n", ROCKSDB_MAJOR, ROCKSDB_MINOR, ROCKSDB_PATCH);
    fprintf(f, "  \"pid\": %d, \"host\": \"%s\",\n", (int)getpid(), host);
    fprintf(f, "  \"argv\": \"");
    for (int i = 1; i < argc; i++) fprintf(f, "%s%s", i > 1 ? " " : "", argv[i]);
    fprintf(f, "\",\n");
    fprintf(f, "  \"bg_jobs\": %d, \"subcomp\": %d,\n", g.bg_jobs, g.subcomp);
    fprintf(f, "  \"workload\": {\"num_ops\": %lld, \"universe\": %lld, \"threads\": %d,"
               " \"key_size\": %d, \"value_size\": %d, \"app_bytes\": %lld},\n",
            (long long)g.num_ops, (long long)g.universe, g.threads, g.key_size,
            g.value_size, (long long)g_bytes.load());
    fprintf(f, "  \"ops_ok\": %lld, \"ops_err\": %lld,\n", (long long)g_ops.load(),
            (long long)g_ops_err.load());
    fprintf(f, "  \"fill_wall_s\": %.3f, \"fill_rate_ops_s\": %.1f, \"fill_rate_mb_s\": %.1f,\n",
            fill_t1 - fill_t0,
            (double)g_ops.load() / std::max(0.001, fill_t1 - fill_t0),
            (double)g_bytes.load() / std::max(0.001, fill_t1 - fill_t0) / 1048576.0);
    fprintf(f, "  \"drain_wall_s\": %.3f, \"drain_hit_limit\": %d,"
               " \"l0_files_at_drain_start\": %lld, \"l0_files_at_drain_end\": %lld,\n",
            drain_t1 - drain_t0, (int)drain_hit_limit, (long long)l0_at_drain_start,
            (long long)l0_end);
    role_json("phase_open", cpu_open, fill_t0 - g_start_ts);
    role_json("phase_fill", cpu_fill, fill_t1 - fill_t0);
    role_json("phase_drain", cpu_drain, drain_t1 - drain_t0);
    fprintf(f, "  \"process_cpu_s_total\": %.3f,\n", ticks_s(proc_ticks_end));
    fprintf(f, "  \"process_cpu_s_at_fill_t0\": %.3f,\n", ticks_s(s_fill_t0.proc_ticks));
    fprintf(f, "  \"process_cpu_s_at_fill_t1\": %.3f,\n", ticks_s(s_fill_t1.proc_ticks));
    fprintf(f, "  \"process_cpu_s_at_drain_t1\": %.3f,\n", ticks_s(s_drain_t1.proc_ticks));
    fprintf(f, "  \"jobs\": {\"flush_jobs\": %lld, \"flush_cpu_s\": %.3f,"
               " \"flush_wall_s\": %.3f, \"flush_bytes\": %lld, \"flush_entries\": %lld,"
               " \"comp_jobs\": %lld, \"comp_cpu_s_self\": %.3f, \"comp_cpu_s_rdb\": %.3f,"
               " \"comp_wall_s\": %.3f, \"comp_in_bytes\": %lld, \"comp_out_bytes\": %lld,"
               " \"subcomp_jobs\": %lld},\n",
            (long long)A.flush_jobs.load(), (double)A.flush_cpu_us.load() / 1e6,
            (double)A.flush_wall_us.load() / 1e6, (long long)A.flush_bytes.load(),
            (long long)A.flush_entries.load(), (long long)A.comp_jobs.load(),
            (double)A.comp_cpu_us_self.load() / 1e6, (double)A.comp_cpu_us_rdb.load() / 1e6,
            (double)A.comp_wall_us.load() / 1e6, (long long)A.comp_in_bytes.load(),
            (long long)A.comp_out_bytes.load(), (long long)A.subcomp_jobs.load());
    fprintf(f, "  \"stalls\": {\"begin\": %lld, \"end\": %lld, \"stall_s\": %.3f},\n",
            (long long)A.stall_begin.load(), (long long)A.stall_end.load(),
            (double)A.stall_us_total.load() / 1e6);
    fprintf(f, "  \"memtable_sealed\": %lld, \"memtable_sealed_entries\": %lld,\n",
            (long long)A.memtable_sealed.load(), (long long)A.memtable_sealed_entries.load());
    fprintf(f, "  \"mem_at_end\": {\"rss_kb\": %lld, \"hwm_kb\": %lld,"
               " \"mem_cur_all\": %lld, \"mem_size_all\": %lld, \"table_readers\": %lld,"
               " \"bcache_usage\": %lld, \"bcache_pinned\": %lld},\n",
            (long long)rss_end_kb, (long long)hwm_end_kb, (long long)mem_cur_all_end,
            (long long)mem_size_all_end, (long long)table_readers_end,
            (long long)bcache_usage_end, (long long)bcache_pinned_end);
    fprintf(f, "  \"files_per_level_at_end\": [%lld,%lld,%lld,%lld,%lld,%lld,%lld],\n",
            (long long)lvl_end[0], (long long)lvl_end[1], (long long)lvl_end[2],
            (long long)lvl_end[3], (long long)lvl_end[4], (long long)lvl_end[5],
            (long long)lvl_end[6]);
    fprintf(f, "  \"close\": {\"wall_s\": %.3f, \"main_cpu_s\": %.3f, \"status\": \"%s\"},\n",
            close_wall, (double)close_cpu / 1e6, cs.ToString().c_str());
    fprintf(f, "  \"phase_mark\": {\"start_ts\": %.6f, \"fill_start_ts\": %.6f,"
               " \"fill_end_ts\": %.6f, \"drain_end_ts\": %.6f}\n",
            g_start_ts, fill_t0, fill_t1, drain_t1);
    fprintf(f, "}\n");
    fclose(f);
  }
  printf("done. summary=%s\n", (g.out_prefix + ".summary.json").c_str());
  return 0;
}
