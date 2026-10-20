// l0bench -- instrumented workload driver for the L0 investigation (native RocksDB).
//
// This is a CLIENT program that links against an UNMODIFIED RocksDB shared library.
// It does not patch or recompile RocksDB: every measurement below comes from public
// APIs (Options/EventListener/Properties/Statistics/PerfContext/ColumnFamilyMetaData)
// plus the driver's own clocks and the OS.
//
// Measurement surface (see bench/CAMPAIGN.md for which experiment uses what):
//   * closed-loop (saturation) and open-loop (fixed arrival rate, with bursts)
//     request generation, so queueing/end-to-end latency is separated from engine
//     latency: two latency histograms per op, sched->done and submit->done.
//   * exact accounting of application-confirmed write bytes (U), so every write
//     amplification number has a real denominator.
//   * per-op PerfContext deltas on a sampled subset (or all ops) -- the only way to
//     see actual data block reads / filter probes per request without patching.
//   * EventListener capture: flush/compaction/subcompaction/stall/memtable-sealed,
//     with per-level input file and byte breakdown and per-output-file rows.
//   * periodic sampling: LSM layout per level and per file (key ranges, size,
//     being_compacted), DB properties, ~65 statistics tickers, process RSS/CPU,
//     block-device counters, and the DB directory's allocated blocks.
//
// Build: bench/build.sh      Usage: --help
//
// NOTE ON HONESTY: the summary JSON distinguishes measured from unmeasured. Fields
// that RocksDB does not expose through public APIs (e.g. exact per-level probe counts
// inside a Get) are NOT invented here; they are reconstructed analytically in the
// analysis scripts from the sampled layout, and that reconstruction is labelled.

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdarg>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <dirent.h>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/version.h"
#include "rocksdb/write_batch.h"

using namespace rocksdb;

// ---------------------------------------------------------------------------
// utilities
// ---------------------------------------------------------------------------
static uint64_t NowNs() {
  return (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch()).count();
}
static double WallSec() {
  return std::chrono::duration<double>(
             std::chrono::system_clock::now().time_since_epoch()).count();
}
static std::string Fmt(const char* fmt, ...) {
  char buf[8192];
  va_list ap; va_start(ap, fmt);
  vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  return std::string(buf);
}
static std::string NS(uint64_t v) { return std::to_string(v); }
static std::string NS(int v) { return std::to_string(v); }
static std::string NS(int64_t v) { return std::to_string(v); }
static std::string NS(double v) { return Fmt("%.6f", v); }
static uint64_t ParseU64(const std::string& s) {
  if (s.empty()) return 0;
  char suf = s.back();
  double mult = 1.0;
  std::string num = s;
  if (isalpha((unsigned char)suf)) {
    num = s.substr(0, s.size() - 1);
    switch (toupper((unsigned char)suf)) {
      case 'K': mult = 1024.0; break;
      case 'M': mult = 1024.0 * 1024; break;
      case 'G': mult = 1024.0 * 1024 * 1024; break;
      case 'T': mult = 1024.0 * 1024 * 1024 * 1024; break;
      default: mult = 1.0;
    }
  }
  return (uint64_t)(strtod(num.c_str(), nullptr) * mult);
}
static std::vector<std::string> Split(const std::string& s, char d) {
  std::vector<std::string> out; size_t p = 0;
  while (true) {
    size_t q = s.find(d, p);
    if (q == std::string::npos) { out.push_back(s.substr(p)); break; }
    out.push_back(s.substr(p, q - p)); p = q + 1;
  }
  return out;
}
static uint64_t DirSizeBytes(const std::string& dir) {
  DIR* d = opendir(dir.c_str());
  if (!d) return 0;
  uint64_t total = 0; struct dirent* e;
  while ((e = readdir(d)) != nullptr) {
    if (e->d_name[0] == '.') continue;
    struct stat st;
    if (stat((dir + "/" + e->d_name).c_str(), &st) == 0 && S_ISREG(st.st_mode))
      total += (uint64_t)st.st_size;
  }
  closedir(d);
  return total;
}

// ---------------------------------------------------------------------------
// log-linear histogram: exact below `SMALL`, then MANTB mantissa bits per octave
//
// BUCKET LABELS (two encodings; the file/JSON consumers MUST NOT mix them):
//
//   lower_bound_v2 (hist_version 2, the default, `--hist_encoding=lower_bound_v2`):
//     Val(i)   = mant << sh  = the bucket's inclusive LOWER bound
//     Upper(i) = ((mant+1) << sh) - 1 = the bucket's inclusive UPPER bound
//     so lo <= v <= hi holds for every sample v.
//
//   legacy_v1 (`--hist_encoding=legacy`, hist_version 1):
//     Val(i) = (mant | kGroup) << sh. `mant` already carries the leading bit of the
//     octave, so OR-ing kGroup in adds a spurious high bit and labels every value
//     >= SMALL at (kGroup+mant)/mant = 2.07x .. 3.00x its true bucket. That is the
//     encoder behind every export produced before 2026-09-25, and it is kept ONLY so
//     those exports can be reproduced and re-labelled offline (see
//     bench/v2/relabel_legacy_hist.py). Never merge v1 labels with v2 labels.
// ---------------------------------------------------------------------------
static constexpr int kHistVersionV2 = 2, kHistVersionLegacy = 1;
static const char* kHistEncV2 = "lower_bound_v2";
static const char* kHistEncLegacy = "legacy_v1";
static inline int g_hist_enc = 0;           // 0 = lower_bound_v2, 1 = legacy_v1
static inline bool g_hist_track_bounds = false;  // record per-bucket sample min/max

template <int SMALL, int MANTB>
struct HistT {
  static constexpr int kBits = MANTB;
  static constexpr int kGroup = 1 << MANTB;
  static constexpr int kBaseBit = (SMALL == 256) ? 8 : 6;
  static constexpr int kBuckets = SMALL + (64 - kBaseBit) * kGroup;
  std::vector<uint64_t> b;
  std::vector<uint64_t> bmin, bmax;   // only populated when g_hist_track_bounds
  uint64_t count = 0, sum = 0, mn = UINT64_MAX, mx = 0;
  HistT() : b(kBuckets, 0) {
    if (g_hist_track_bounds) { bmin.assign(kBuckets, UINT64_MAX); bmax.assign(kBuckets, 0); }
  }
  static inline int Idx(uint64_t v) {
    if (v < (uint64_t)SMALL) return (int)v;
    int bl = 64 - __builtin_clzll(v);
    int oct = bl - kBaseBit;
    int sh = bl - MANTB;
    int mant = (int)((v >> sh) & (uint64_t)(kGroup - 1));
    int i = SMALL + oct * kGroup + mant;
    return i < kBuckets ? i : kBuckets - 1;
  }
  static inline double Val(int i) {
    if (i < SMALL) return (double)i;
    int k = i - SMALL;
    int oct = k / kGroup, mant = k % kGroup;
    int sh = (kBaseBit + oct) - MANTB;
    return (double)((uint64_t)mant << sh);
  }
  static inline double ValLegacy(int i) {
    if (i < SMALL) return (double)i;
    int k = i - SMALL;
    int oct = k / kGroup, mant = k % kGroup;
    int sh = (kBaseBit + oct) - MANTB;
    return (double)((uint64_t)(mant | kGroup) << sh);
  }
  static inline double ValEnc(int i, int enc) { return enc ? ValLegacy(i) : Val(i); }
  static inline double Upper(int i) {
    if (i < SMALL) return (double)i;
    // the top bucket is a clamp: it absorbs everything past the last representable
    // octave, so it has no finite upper bound.
    if (i >= kBuckets - 1) return (double)UINT64_MAX;
    int k = i - SMALL;
    int oct = k / kGroup, mant = k % kGroup;
    int sh = (kBaseBit + oct) - MANTB;
    return (double)((((uint64_t)mant + 1) << sh) - 1);
  }
  inline void Add(uint64_t v) {
    int i = Idx(v);
    b[i]++; count++; sum += v;
    if (v < mn) mn = v;
    if (v > mx) mx = v;
    if (!bmin.empty()) {
      if (v < bmin[i]) bmin[i] = v;
      if (v > bmax[i]) bmax[i] = v;
    }
  }
  void Merge(const HistT& o) {
    for (int i = 0; i < kBuckets; i++) b[i] += o.b[i];
    count += o.count; sum += o.sum;
    if (o.mn < mn) mn = o.mn;
    if (o.mx > mx) mx = o.mx;
    if (!bmin.empty() && !o.bmin.empty()) {
      for (int i = 0; i < kBuckets; i++) {
        if (o.bmin[i] < bmin[i]) bmin[i] = o.bmin[i];
        if (o.bmax[i] > bmax[i]) bmax[i] = o.bmax[i];
      }
    }
  }
  double Pct(double p, int enc = -1) const {
    if (enc < 0) enc = g_hist_enc;
    if (count == 0) return 0;
    uint64_t target = (uint64_t)(p * (double)count);
    if (target == 0) target = 1;
    uint64_t acc = 0;
    for (int i = 0; i < kBuckets; i++) { acc += b[i]; if (acc >= target) return ValEnc(i, enc); }
    return ValEnc(kBuckets - 1, enc);
  }
  void Reset() {
    std::fill(b.begin(), b.end(), 0); count = sum = mx = 0; mn = UINT64_MAX;
    if (!bmin.empty()) { std::fill(bmin.begin(), bmin.end(), UINT64_MAX); std::fill(bmax.begin(), bmax.end(), 0); }
  }
};
using Hist = HistT<256, 4>;
using Coarse = HistT<64, 2>;

// ---------------------------------------------------------------------------
// keys: 16 bytes = 8-byte big-endian space tag + 8-byte big-endian id
// ---------------------------------------------------------------------------
static constexpr uint64_t kTagMain = 0x0101010101010101ull;
static constexpr uint64_t kTagNeg = 0x0202020202020202ull;
static inline void EncodeKey(uint64_t tag, uint64_t id, char* out) {
  for (int i = 0; i < 8; i++) out[i] = (char)((tag >> (56 - 8 * i)) & 0xFF);
  for (int i = 0; i < 8; i++) out[8 + i] = (char)((id >> (56 - 8 * i)) & 0xFF);
}
static inline uint64_t DecodeKeyId(const char* k) {
  uint64_t v = 0;
  for (int i = 8; i < 16; i++) v = (v << 8) | (unsigned char)k[i];
  return v;
}
static inline uint64_t DecodeKeyTag(const char* k) {
  uint64_t v = 0;
  for (int i = 0; i < 8; i++) v = (v << 8) | (unsigned char)k[i];
  return v;
}
static std::string HexKey(const std::string& k) {
  static const char* h = "0123456789abcdef";
  std::string o; o.reserve(k.size() * 2);
  for (unsigned char c : k) { o.push_back(h[c >> 4]); o.push_back(h[c & 15]); }
  return o;
}
static void FillValue(uint64_t id, uint64_t salt, int size, std::string* v) {
  v->resize(size);
  uint64_t x = id * 0x9E3779B97F4A7C15ull + salt * 0xD1B54A32D192ED03ull;
  char* p = &(*v)[0];
  for (int i = 0; i < size; i += 8) {
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    int n = std::min(8, size - i);
    memcpy(p + i, &x, n);
  }
}
static bool CheckValue(uint64_t id, uint64_t salt, const Slice& got) {
  std::string want;
  FillValue(id, salt, (int)got.size(), &want);
  return want.compare(got.ToString()) == 0;
}

// ---------------------------------------------------------------------------
// configuration
// ---------------------------------------------------------------------------
struct Burst { double start, end, mult; };
struct Cfg {
  std::string db_path = "./db";
  std::string out_prefix = "run";
  std::string mode = "fill";
  int64_t universe = 48000000;
  int64_t num_ops = -1;
  double duration_s = 0;
  int threads = 16;
  int key_size = 16;
  int value_size = 1024;
  bool use_existing_db = false;
  std::string dist = "uniform";
  std::string order = "identity";
  double zipf_theta = 0.99;
  double hot_fraction = 0.10, hot_share = 0.80;
  int64_t locality_window = 1000000;
  std::string read_mix = "recent:0.4,deep:0.4,neg:0.2";
  int64_t recent_span = 0, deep_span = 0, scan_len = 10;
  std::string scan_start = "random";   // random | sequential (runbook §5 M5)
  std::string arrival = "closed";
  double rate = 0;
  std::vector<Burst> bursts;
  bool sync = false;
  bool perf_detail = false;
  double detail_rate = 0.0;
  int64_t detail_max_rows = 2000000;
  double read_log_rate = 0.0;
  int sample_ms = 1000, layout_ms = 10000, dirscan_ms = 10000, progress_ms = 2000;
  int read_pct = -1;               // mix ratio for --mode=mix
  // rocksdb options
  uint64_t write_buffer_size = 256ull << 20;
  int max_write_buffer_number = 4;
  int level0_trigger = 8, level0_slowdown = 20, level0_stop = 36;
  uint64_t target_file_size_base = 64ull << 20;
  int max_background_jobs = 8, max_subcompactions = 8;
  // Per-stage capacity control (bottleneck campaign). Defaults -1 = derive from
  // max_background_jobs (flush = max(1, jobs/4), compaction = jobs - flush).
  // Setting either to >= 1 switches RocksDB to the explicit branch where both
  // pools are sized independently (GetBGJobLimits compatibility path).
  int max_background_flushes = -1, max_background_compactions = -1;
  bool dynamic_level_bytes = true;
  uint64_t cache_size = 8ull << 30;
  int bloom_bits = 10;
  std::string compression = "none";
  bool disable_auto_compactions = false;
  uint64_t max_total_wal_size = 0, max_compaction_bytes = 0;
  int num_levels = 0;
  uint64_t bytes_per_sync = 0;
  bool use_direct_io = false;
  bool disable_wal = false;
  bool manual_wal_flush = false;
  uint64_t max_bytes_for_level_base = 0;
  int stats_dump_period_sec = 60;   // periodic full statistics block in the DB LOG
  // phases
  // open -> warmup -> measure -> drain (bench/CAMPAIGN_V2.md §2.2).
  // measure_s > 0 switches on cohort accounting: submissions stop at the measurement
  // deadline, already-submitted requests are allowed to finish, and throughput is
  // reported for the measurement cohort only. measure_s == 0 keeps the legacy
  // whole-run semantics (cohort == whole run) so old invocations are unchanged.
  double warmup_s = 0;
  double measure_s = 0;
  double measure_drain_max_s = 300;
  double drain_observe_s = 0;
  bool final_flush = false;
  double final_flush_observe_s = 0;
  bool wait_for_drain = false;
  double wait_for_drain_max_s = 1800;
  bool drain_require_l0_zero = false;   // L0==0 as an additional physical condition
  // measurement-method switches (see CAMPAIGN_V2.md §2.1/§2.3)
  std::string hist_encoding = "lower_bound_v2";   // lower_bound_v2 | legacy
  bool hist_track_bounds = false;                 // per-bucket sample min/max + bound check
  std::string mix_sampling = "random";            // random | legacy (op_id hash)
  std::string detail_sampling = "random";         // random | stride
  int64_t detail_ring_rows = 65536;               // async detail ring capacity
  // fault injection
  int64_t kill_after_ops = -1;
  double kill_at_s = -1;
  std::string kill_at_event;
  int64_t kill_after_event_n = 1;
  // backlog mode
  int backlog_files = 16;
  int64_t backlog_keys_per_file = 131072;
  std::string backlog_overlap = "wide";
  int64_t backlog_region = 0;
  // verify
  int64_t verify_step = 99991;
  uint64_t verify_salt = 1;
  int64_t verify_from = -1, verify_to = -1;
  int seed = 20260914;
  // Runtime option interventions: --set_opt=AT_S:name=value[,...]
  struct SetOpt { double at; std::string name, value; };
  std::vector<SetOpt> set_opts;
};

static Cfg g;

static const char* kUsage =
"l0bench -- instrumented RocksDB workload driver (native RocksDB, no source patches)\n"
"\n"
"General:  --db=PATH --out=PREFIX --mode=MODE --universe=N [--num_ops=N | --duration_s=S]\n"
"          --threads=T --value_size=B --use_existing_db=0|1 --seed=N\n"
"  modes:  fill overwrite read mix scan backlog open verify compact flush histcheck scancheck\n"
"Workload: --dist=uniform|monotonic|locality|zipf|hotspot   --order=identity|shuffle\n"
"          --read_mix=recent:w,deep:w,neg:w,uniform:w,scan:w\n"
"          --recent_span=N --deep_span=N --scan_len=N --read_pct=0..100\n"
"          --scan_start=random|sequential\n"
"          --zipf_theta=F --hot_fraction=F --hot_share=F --locality_window=N\n"
"          --read_log_rate=F (0=off) --mix_sampling=random|legacy\n"
"Arrival:  --arrival=closed|open --rate=OPS_S --burst=S:E:MULT[,...] --sync=0|1\n"
"Measure:  --warmup_s=S --measure_s=S --measure_drain_max_s=S\n"
"          --perf_detail=0|1 --detail_rate=F --detail_max_rows=N\n"
"          --detail_sampling=random|stride --detail_ring_rows=N\n"
"          --sample_ms=N --layout_ms=N --dirscan_ms=N --progress_ms=N\n"
"          --drain_observe_s=S --final_flush=0|1 --final_flush_observe_s=S\n"
"          --wait_for_drain=0|1 --wait_for_drain_max_s=S --drain_require_l0_zero=0|1\n"
"          --hist_encoding=lower_bound_v2|legacy --hist_track_bounds=0|1\n"
"Fault:    --kill_after_ops=N --kill_at_s=S --kill_at_event=flush_installed|compaction_installed --kill_after_event_n=N\n"
"Options:  --write_buffer_size=B --max_write_buffer_number=N --level0_trigger=N\n"
"          --level0_slowdown=N --level0_stop=N --target_file_size_base=B\n"
"          --max_background_jobs=N --max_subcompactions=N --dynamic_level_bytes=0|1\n"
"          --max_background_flushes=N --max_background_compactions=N   (-1 = derive from jobs)\n"
"          --cache_size=B --bloom_bits=N --compression=none|lz4|zstd|snappy\n"
"          --disable_auto_compactions=0|1 --max_total_wal_size=B --max_compaction_bytes=B\n"
"          --num_levels=N --max_bytes_for_level_base=B --bytes_per_sync=B\n"
"          --use_direct_io=0|1 --disable_wal=0|1 --manual_wal_flush=0|1\n"
"Backlog:  --backlog_files=K --backlog_keys_per_file=N --backlog_overlap=wide|narrow --backlog_region=N\n"
"Verify:   --verify_step=N --verify_salt=S\n";

static void ParseArgs(int argc, char** argv) {
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
    else if (k == "mode") g.mode = v;
    else if (k == "universe") g.universe = (int64_t)ParseU64(v);
    else if (k == "num_ops") g.num_ops = (int64_t)ParseU64(v);
    else if (k == "duration_s") g.duration_s = atof(v.c_str());
    else if (k == "threads") g.threads = atoi(v.c_str());
    else if (k == "key_size") g.key_size = atoi(v.c_str());
    else if (k == "value_size") g.value_size = atoi(v.c_str());
    else if (k == "use_existing_db") g.use_existing_db = B();
    else if (k == "seed") g.seed = (int)ParseU64(v);
    else if (k == "dist") g.dist = v;
    else if (k == "order") g.order = v;
    else if (k == "zipf_theta") g.zipf_theta = atof(v.c_str());
    else if (k == "hot_fraction") g.hot_fraction = atof(v.c_str());
    else if (k == "hot_share") g.hot_share = atof(v.c_str());
    else if (k == "locality_window") g.locality_window = (int64_t)ParseU64(v);
    else if (k == "read_mix") g.read_mix = v;
    else if (k == "recent_span") g.recent_span = (int64_t)ParseU64(v);
    else if (k == "deep_span") g.deep_span = (int64_t)ParseU64(v);
    else if (k == "scan_len") g.scan_len = (int64_t)ParseU64(v);
    else if (k == "scan_start") g.scan_start = v;
    else if (k == "read_pct") g.read_pct = atoi(v.c_str());
    else if (k == "arrival") g.arrival = v;
    else if (k == "rate") g.rate = atof(v.c_str());
    else if (k == "burst") {
      for (auto& part : Split(v, ',')) {
        auto f = Split(part, ':');
        if (f.size() == 3) g.bursts.push_back({atof(f[0].c_str()), atof(f[1].c_str()), atof(f[2].c_str())});
      }
    }
    else if (k == "sync") g.sync = B();
    else if (k == "perf_detail") g.perf_detail = B();
    else if (k == "detail_rate") g.detail_rate = atof(v.c_str());
    else if (k == "detail_max_rows") g.detail_max_rows = (int64_t)ParseU64(v);
    else if (k == "read_log_rate") g.read_log_rate = atof(v.c_str());
    else if (k == "mix_sampling") g.mix_sampling = v;
    else if (k == "detail_sampling") g.detail_sampling = v;
    else if (k == "detail_ring_rows") g.detail_ring_rows = (int64_t)ParseU64(v);
    else if (k == "hist_encoding") g.hist_encoding = v;
    else if (k == "hist_track_bounds") g.hist_track_bounds = B();
    else if (k == "warmup_s") g.warmup_s = atof(v.c_str());
    else if (k == "measure_s") g.measure_s = atof(v.c_str());
    else if (k == "measure_drain_max_s") g.measure_drain_max_s = atof(v.c_str());
    else if (k == "sample_ms") g.sample_ms = atoi(v.c_str());
    else if (k == "layout_ms") g.layout_ms = atoi(v.c_str());
    else if (k == "dirscan_ms") g.dirscan_ms = atoi(v.c_str());
    else if (k == "progress_ms") g.progress_ms = atoi(v.c_str());
    else if (k == "write_buffer_size") g.write_buffer_size = ParseU64(v);
    else if (k == "max_write_buffer_number") g.max_write_buffer_number = atoi(v.c_str());
    else if (k == "level0_trigger") g.level0_trigger = atoi(v.c_str());
    else if (k == "level0_slowdown") g.level0_slowdown = atoi(v.c_str());
    else if (k == "level0_stop") g.level0_stop = atoi(v.c_str());
    else if (k == "target_file_size_base") g.target_file_size_base = ParseU64(v);
    else if (k == "max_background_jobs") g.max_background_jobs = atoi(v.c_str());
    else if (k == "max_subcompactions") g.max_subcompactions = atoi(v.c_str());
    else if (k == "max_background_flushes") g.max_background_flushes = atoi(v.c_str());
    else if (k == "max_background_compactions") g.max_background_compactions = atoi(v.c_str());
    else if (k == "dynamic_level_bytes") g.dynamic_level_bytes = B();
    else if (k == "cache_size") g.cache_size = ParseU64(v);
    else if (k == "bloom_bits") g.bloom_bits = atoi(v.c_str());
    else if (k == "compression") g.compression = v;
    else if (k == "disable_auto_compactions") g.disable_auto_compactions = B();
    else if (k == "max_total_wal_size") g.max_total_wal_size = ParseU64(v);
    else if (k == "max_compaction_bytes") g.max_compaction_bytes = ParseU64(v);
    else if (k == "num_levels") g.num_levels = atoi(v.c_str());
    else if (k == "max_bytes_for_level_base") g.max_bytes_for_level_base = ParseU64(v);
    else if (k == "stats_dump_period_sec") g.stats_dump_period_sec = atoi(v.c_str());
    else if (k == "bytes_per_sync") g.bytes_per_sync = ParseU64(v);
    else if (k == "use_direct_io") g.use_direct_io = B();
    else if (k == "disable_wal") g.disable_wal = B();
    else if (k == "manual_wal_flush") g.manual_wal_flush = B();
    else if (k == "drain_observe_s") g.drain_observe_s = atof(v.c_str());
    else if (k == "final_flush") g.final_flush = B();
    else if (k == "final_flush_observe_s") g.final_flush_observe_s = atof(v.c_str());
    else if (k == "wait_for_drain") g.wait_for_drain = B();
    else if (k == "wait_for_drain_max_s") g.wait_for_drain_max_s = atof(v.c_str());
    else if (k == "drain_require_l0_zero") g.drain_require_l0_zero = B();
    else if (k == "kill_after_ops") g.kill_after_ops = (int64_t)ParseU64(v);
    else if (k == "kill_at_s") g.kill_at_s = atof(v.c_str());
    else if (k == "kill_at_event") g.kill_at_event = v;
    else if (k == "kill_after_event_n") g.kill_after_event_n = (int64_t)ParseU64(v);
    else if (k == "backlog_files") g.backlog_files = atoi(v.c_str());
    else if (k == "backlog_keys_per_file") g.backlog_keys_per_file = (int64_t)ParseU64(v);
    else if (k == "backlog_overlap") g.backlog_overlap = v;
    else if (k == "backlog_region") g.backlog_region = (int64_t)ParseU64(v);
    else if (k == "verify_step") g.verify_step = (int64_t)ParseU64(v);
    else if (k == "verify_salt") g.verify_salt = ParseU64(v);
    else if (k == "verify_from") g.verify_from = (int64_t)ParseU64(v);
    else if (k == "verify_to") g.verify_to = (int64_t)ParseU64(v);
    else if (k == "set_opt") {
      for (auto& part : Split(v, ',')) {
        size_t c1 = part.find(':'), c2 = part.find('=', c1 + 1);
        if (c1 == std::string::npos || c2 == std::string::npos) { fprintf(stderr, "bad --set_opt %s\n", part.c_str()); exit(2); }
        g.set_opts.push_back({atof(part.substr(0, c1).c_str()),
                              part.substr(c1 + 1, c2 - c1 - 1), part.substr(c2 + 1)});
      }
    }
    else { fprintf(stderr, "unknown flag: --%s\n", k.c_str()); exit(2); }
  }
  if (g.recent_span == 0) g.recent_span = std::max<int64_t>(1000, g.universe / 20);
  if (g.deep_span == 0) g.deep_span = std::max<int64_t>(1000, g.universe / 5);
  if (g.mode == "fill" || g.mode == "overwrite") {
    // Implicit "write the whole universe" only applies to a build-style run. If a
    // measurement window was asked for, the run is time-bounded by that window:
    // otherwise the implicit op cap silently truncates the run before warmup ends
    // and the cohort comes back empty.
    if (g.num_ops < 0 && g.duration_s <= 0 && g.measure_s <= 0) g.num_ops = g.universe;
  }
  if (g.hist_encoding == "lower_bound_v2") g_hist_enc = 0;
  else if (g.hist_encoding == "legacy") g_hist_enc = 1;
  else { fprintf(stderr, "unknown --hist_encoding=%s (lower_bound_v2|legacy)\n", g.hist_encoding.c_str()); exit(2); }
  g_hist_track_bounds = g.hist_track_bounds;
  if (g.mix_sampling != "random" && g.mix_sampling != "legacy") {
    fprintf(stderr, "unknown --mix_sampling=%s (random|legacy)\n", g.mix_sampling.c_str()); exit(2);
  }
  if (g.detail_sampling != "random" && g.detail_sampling != "stride") {
    fprintf(stderr, "unknown --detail_sampling=%s (random|stride)\n", g.detail_sampling.c_str()); exit(2);
  }
  if (g.warmup_s < 0 || g.measure_s < 0) { fprintf(stderr, "warmup_s/measure_s must be >= 0\n"); exit(2); }
  if (g.detail_ring_rows < 1024) g.detail_ring_rows = 1024;
  if (g.scan_start != "random" && g.scan_start != "sequential") {
    fprintf(stderr, "unknown --scan_start=%s (random|sequential)\n", g.scan_start.c_str()); exit(2);
  }
}

// ---------------------------------------------------------------------------
// CSV sinks
// ---------------------------------------------------------------------------
// Set once the DB is being torn down. RocksDB's background threads can outlive the
// workload and still fire EventListener callbacks; without this they would write
// into sinks (and their mutexes) that global destruction has already reclaimed,
// which showed up as an intermittent SIGSEGV *after* a run's results were printed.
static std::atomic<bool> g_closing{false};

struct Csv {
  FILE* f = nullptr;
  std::mutex mu;
  void Open(const std::string& path, const std::string& header) {
    f = fopen(path.c_str(), "w");
    if (!f) { fprintf(stderr, "cannot open %s\n", path.c_str()); exit(1); }
    fprintf(f, "%s\n", header.c_str());
  }
  void Row(const std::string& s) {
    if (g_closing.load()) return;
    std::lock_guard<std::mutex> lk(mu);
    if (f) { fputs(s.c_str(), f); fputc('\n', f); }
  }
  void Close() { std::lock_guard<std::mutex> lk(mu); if (f) { fclose(f); f = nullptr; } }
};
static Csv c_persec, c_samples, c_layout, c_layout_levels, c_events,
    c_event_files, c_reads, c_dirscan, c_stalls, c_compactions, c_summary_extra;

// ---------------------------------------------------------------------------
// request classes (CAMPAIGN_V2.md §2.3): put / get_hit / get_miss / scan each get
// their own engine and E2E latency histogram. A single merged histogram cannot
// stand in for these: the three read classes have different cost structure.
// ---------------------------------------------------------------------------
enum OpClass { kClsPut = 0, kClsGetHit = 1, kClsGetMiss = 2, kClsScan = 3, kClsCount = 4 };
static const char* kClsNames[kClsCount] = {"put", "get_hit", "get_miss", "scan"};

// ---------------------------------------------------------------------------
// asynchronous detail sink (§2.3).
// The request path only pushes a formatted row into a bounded ring; a writer
// thread drains it to disk. When the ring is full the row is DROPPED and counted
// (the request path never blocks on I/O). Sampling rate, rows written, rows
// dropped and the peak queue depth are all reported, so a consumer can tell
// whether the detail stream is representative.
// ---------------------------------------------------------------------------
class DetailRing {
 public:
  void Open(const std::string& prefix, const std::string& header) {
    prefix_ = prefix;
    buf_.resize((size_t)g.detail_ring_rows);
    f_all_.Open(prefix + ".detail.csv", header);
    for (int c = 0; c < kClsCount; c++)
      fc_[c].Open(prefix + ".detail_" + kClsNames[c] + ".csv", header);
  }
  void Start() {
    stop_ = false;      // set before the writer thread exists
    th_ = std::thread([this] { Run(); });
  }
  void Push(std::string row, int cls) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (head_ - tail_ >= buf_.size()) { dropped_.fetch_add(1); return; }
      uint64_t h = head_++;
      Slot& s = buf_[(size_t)(h % buf_.size())];
      s.row = std::move(row);
      s.cls = cls;
      uint64_t d = head_ - tail_;
      uint64_t md = max_depth_.load();
      while (d > md && !max_depth_.compare_exchange_weak(md, d)) {}
      pushed_.fetch_add(1);
    }
    cv_.notify_one();
  }
  // Drain everything currently queued (used by Stop; also lets a caller flush
  // mid-run without stopping the writer).
  void Drain() {
    std::unique_lock<std::mutex> lk(mu_);
    while (tail_ < head_) FlushLocked();
  }
  void Stop() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      stop_ = true;
    }
    cv_.notify_all();
    if (th_.joinable()) th_.join();
    Drain();
  }
  int64_t pushed() const { return pushed_.load(); }
  int64_t written() const { return written_.load(); }
  int64_t dropped() const { return dropped_.load(); }
  int64_t max_depth() const { return (int64_t)max_depth_.load(); }
  int64_t capacity() const { return (int64_t)buf_.size(); }

 private:
  struct Slot { std::string row; int cls = 0; };
  void Run() {
    std::unique_lock<std::mutex> lk(mu_);
    while (true) {
      if (tail_ < head_) { FlushLocked(); continue; }
      if (stop_ && tail_ >= head_) return;
      cv_.wait_for(lk, std::chrono::milliseconds(50));
    }
  }
  void FlushLocked() {
    while (tail_ < head_) {
      Slot& s = buf_[(size_t)(tail_ % buf_.size())];
      f_all_.Row(s.row);
      if (s.cls >= 0 && s.cls < kClsCount) fc_[s.cls].Row(s.row);
      s.row.clear();
      tail_++;
      written_.fetch_add(1);
    }
  }
  std::string prefix_;
  std::vector<Slot> buf_;
  uint64_t head_ = 0, tail_ = 0;           // guarded by mu_
  bool stop_ = false;                      // guarded by mu_
  std::mutex mu_;
  std::condition_variable cv_;
  std::thread th_;
  Csv f_all_, fc_[kClsCount];
  std::atomic<int64_t> pushed_{0}, written_{0}, dropped_{0};
  std::atomic<uint64_t> max_depth_{0};
};
static DetailRing DETAIL;

// ---------------------------------------------------------------------------
// shared run state
// ---------------------------------------------------------------------------
struct Shared {
  std::atomic<int64_t> ops_started{0}, ops_done{0}, ops_ok{0}, ops_err{0};
  std::atomic<int64_t> reads_issued{0}, reads_hit{0}, reads_notfound{0}, writes_ok{0};
  std::atomic<int64_t> bytes_written_logical{0}, bytes_read_returned{0};
  std::atomic<int64_t> detail_rows{0}, read_rows{0}, detail_cap_hits{0};
  std::atomic<int64_t> stall_begin_events{0}, stall_end_events{0};
  std::atomic<int64_t> flush_installed{0}, compaction_installed{0};
  std::atomic<bool> stop{false};
  std::atomic<int64_t> killed_at_ops{-1};
  // ---- measurement cohort (§2.2) -------------------------------------------
  // Counters are incremented only for requests SUBMITTED inside the measurement
  // window, so they are independent of progress polling and of the drain.
  std::atomic<int64_t> m_submitted{0}, m_done{0}, m_ok{0}, m_err{0};
  std::atomic<int64_t> m_reads{0}, m_writes{0}, m_notfound{0};
  std::atomic<int64_t> m_cls[kClsCount] = {};
  // whole-run read/write split and per-class success counts
  std::atomic<int64_t> reads_done{0}, writes_done{0};
  std::atomic<int64_t> c_cls[kClsCount] = {};
  std::atomic<int64_t> ts_order_violations{0};
  // phase boundaries, WallSec() scale
  std::atomic<double> t_open_start{0}, t_open_end{0};
  std::atomic<double> t_warmup_start{0}, t_warmup_end{0};
  std::atomic<double> t_measure_start{0}, t_measure_end{0}, t_measure_end_planned{0};
  std::atomic<double> t_work_end{0};
  // Raw monotonic (steady_clock) nanoseconds for the phase boundaries we observe
  // directly, kept alongside the wall-clock values so a reviewer can recompute a
  // throughput from the clock the driver actually used for latencies.
  std::atomic<uint64_t> n_t0{0}, n_warmup_end{0}, n_cutoff{0}, n_drain_end{0};
  std::atomic<double> t_drain_start{0}, t_drain_end{0};
  std::atomic<double> t_final_flush_start{0}, t_final_flush_end{0};
  std::atomic<int64_t> inflight_at_cutoff{-1}, censored{-1};
  std::atomic<bool> cutoff{false};
  double t0 = 0;
};
static Shared S;
static uint64_t g_dev_wr0 = 0, g_dev_rd0 = 0;

struct PerSec {
  std::mutex mu;
  std::vector<Hist> h_eng, h_e2e;      // engine latency (submit->done) and e2e (sched->done)
  std::vector<uint64_t> ops, bytes, wops, rops, rhit;
  // measurement-cohort counters per wall second, so that
  //   sum(per_sec.m_ops) == measure_phase.cohort_ops
  // is a checkable invariant (CAMPAIGN_V2.md §3).
  std::vector<uint64_t> m_ops, m_rops, m_wops, m_bytes, m_rhit;
  void Ensure(int64_t sec) {
    while ((int64_t)h_eng.size() <= sec) {
      h_eng.emplace_back(); h_e2e.emplace_back();
      ops.push_back(0); bytes.push_back(0); wops.push_back(0); rops.push_back(0); rhit.push_back(0);
      m_ops.push_back(0); m_rops.push_back(0); m_wops.push_back(0);
      m_bytes.push_back(0); m_rhit.push_back(0);
    }
  }
};
static PerSec PS;

// ---------------------------------------------------------------------------
// request generators
// ---------------------------------------------------------------------------
struct Gen {
  std::vector<uint64_t> perm;
  bool has_perm = false;
  std::vector<double> zipf_cdf;
  void Build() {
    uint64_t U = (uint64_t)g.universe;
    if (g.dist == "uniform" || g.dist == "hotspot" ||
        (g.dist == "monotonic" && g.order == "shuffle")) {
      perm.resize(U);
      for (uint64_t i = 0; i < U; i++) perm[i] = i;
      std::mt19937_64 rng((uint64_t)g.seed * 2654435761ull + 7);
      for (uint64_t i = U; i > 1; i--) { uint64_t j = rng() % i; std::swap(perm[i - 1], perm[j]); }
      has_perm = true;
    }
    if (g.dist == "zipf") {
      size_t n = (size_t)std::min<uint64_t>(U, 1000000);
      zipf_cdf.resize(n);
      double s = 0;
      for (size_t i = 0; i < n; i++) { s += 1.0 / std::pow((double)(i + 1), g.zipf_theta); zipf_cdf[i] = s; }
      for (auto& x : zipf_cdf) x /= s;
    }
  }
  inline uint64_t Uniform(uint64_t lo, uint64_t hi, std::mt19937_64& rng) {
    if (hi <= lo) return lo;
    return lo + rng() % (hi - lo);
  }
  inline uint64_t Zipf(std::mt19937_64& rng) {
    if (zipf_cdf.empty()) return 0;
    double u = (rng() >> 11) * (1.0 / 9007199254740992.0);
    auto it = std::lower_bound(zipf_cdf.begin(), zipf_cdf.end(), u);
    uint64_t idx = (uint64_t)(it - zipf_cdf.begin());
    return idx * (uint64_t)g.universe / std::max<size_t>(1, zipf_cdf.size());
  }
  inline uint64_t WriteKey(int64_t i, std::mt19937_64& rng) {
    const uint64_t U = (uint64_t)g.universe;
    if (g.dist == "monotonic") return has_perm ? perm[(uint64_t)i % U] : (uint64_t)i % U;
    if (g.dist == "locality") {
      uint64_t w = (uint64_t)std::max<int64_t>(1, g.locality_window);
      uint64_t blk = (uint64_t)i / w, off = (uint64_t)i % w;
      uint64_t span = U > w ? U - w : 1;
      uint64_t start = (blk * 0x9E3779B97F4A7C15ull) % span;
      return start + off;
    }
    if (g.dist == "zipf") return Zipf(rng);
    if (g.dist == "hotspot") {
      double u = (rng() >> 11) * (1.0 / 9007199254740992.0);
      uint64_t hot = (uint64_t)((double)U * g.hot_fraction);
      if (u < g.hot_share && hot > 0) return Uniform(0, hot, rng);
      return Uniform(0, U, rng);
    }
    if (has_perm) return perm[(uint64_t)i % U];
    return Uniform(0, U, rng);
  }
};
static Gen G;

struct MixPart { std::string name; double w; };
static std::vector<MixPart> g_read_mix;
static void ParseReadMix() {
  g_read_mix.clear();
  double tot = 0;
  for (auto& p : Split(g.read_mix, ',')) {
    auto f = Split(p, ':');
    if (f.size() != 2) continue;
    MixPart m{f[0], atof(f[1].c_str())};
    tot += m.w;
    g_read_mix.push_back(m);
  }
  if (tot > 0) for (auto& m : g_read_mix) m.w /= tot;
}

// open-loop pacer with burst schedule
struct Pacer {
  std::mutex mu;
  uint64_t next_ns = 0;
  double base = 0, t0_mono = 0;
  void Init() { base = g.rate; t0_mono = (double)NowNs(); next_ns = (uint64_t)(t0_mono + 1e9 / g.rate); }
  double MultAt(double elapsed) const {
    double m = 1.0;
    for (auto& b : g.bursts) if (elapsed >= b.start && elapsed < b.end) m = b.mult;
    return m;
  }
  uint64_t Next() {
    std::lock_guard<std::mutex> lk(mu);
    uint64_t t = next_ns;
    double elapsed = ((double)t - t0_mono) / 1e9;
    double m = MultAt(elapsed);
    double per = 1e9 / (base * (m > 0 ? m : 1e-9));
    next_ns = t + (uint64_t)std::max(1.0, per);
    return t;
  }
};
static Pacer PACE;

// ---------------------------------------------------------------------------
// EventListener
// ---------------------------------------------------------------------------
static const char* FlushReasonStr(FlushReason r) {
  switch (r) {
    case FlushReason::kOthers: return "Others";
    case FlushReason::kGetLiveFiles: return "GetLiveFiles";
    case FlushReason::kShutDown: return "ShutDown";
    case FlushReason::kExternalFileIngestion: return "ExternalFileIngestion";
    case FlushReason::kManualCompaction: return "ManualCompaction";
    case FlushReason::kWriteBufferManager: return "WriteBufferManager";
    case FlushReason::kWriteBufferFull: return "WriteBufferFull";
    case FlushReason::kTest: return "Test";
    case FlushReason::kDeleteFiles: return "DeleteFiles";
    case FlushReason::kAutoCompaction: return "AutoCompaction";
    case FlushReason::kManualFlush: return "ManualFlush";
    case FlushReason::kErrorRecovery: return "ErrorRecovery";
    case FlushReason::kErrorRecoveryRetryFlush: return "ErrorRecoveryRetryFlush";
    case FlushReason::kWalFull: return "WalFull";
    default: return "Unknown";
  }
}
static const char* CompactionReasonStr(CompactionReason r) {
  switch (r) {
    case CompactionReason::kUnknown: return "Unknown";
    case CompactionReason::kLevelL0FilesNum: return "LevelL0FilesNum";
    case CompactionReason::kLevelMaxLevelSize: return "LevelMaxLevelSize";
    case CompactionReason::kUniversalSizeAmplification: return "UniversalSizeAmplification";
    case CompactionReason::kUniversalSizeRatio: return "UniversalSizeRatio";
    case CompactionReason::kUniversalSortedRunNum: return "UniversalSortedRunNum";
    case CompactionReason::kFIFOMaxSize: return "FIFOMaxSize";
    case CompactionReason::kFIFOReduceNumFiles: return "FIFOReduceNumFiles";
    case CompactionReason::kFIFOTtl: return "FIFOTtl";
    case CompactionReason::kManualCompaction: return "ManualCompaction";
    case CompactionReason::kFilesMarkedForCompaction: return "FilesMarkedForCompaction";
    case CompactionReason::kBottommostFiles: return "BottommostFiles";
    case CompactionReason::kTtl: return "Ttl";
    case CompactionReason::kFlush: return "Flush";
    case CompactionReason::kExternalSstIngestion: return "ExternalSstIngestion";
    case CompactionReason::kPeriodicCompaction: return "PeriodicCompaction";
    case CompactionReason::kChangeTemperature: return "ChangeTemperature";
    case CompactionReason::kForcedBlobGC: return "ForcedBlobGC";
    case CompactionReason::kRoundRobinTtl: return "RoundRobinTtl";
    case CompactionReason::kRefitLevel: return "RefitLevel";
    default: return "Other";
  }
}
static constexpr int kMaxLvl = 7;
static std::string JoinInts(const std::vector<int>& v) {
  std::string s;
  for (size_t i = 0; i < v.size(); i++) { if (i) s += "|"; s += std::to_string(v[i]); }
  return s;
}
// fixed-width event row (17 columns)
static void Ev(const char* kind, int job, const std::string& cf,
               const std::string& ilev = "", const std::string& ninf = "",
               const std::string& inb = "", const std::string& olvl = "",
               const std::string& outb = "", const std::string& noutf = "",
               const std::string& wall = "", const std::string& cpu = "",
               const std::string& ninr = "", const std::string& nour = "",
               const std::string& inbs = "", const std::string& outbs = "",
               const std::string& extra = "") {
  c_events.Row(Fmt("%.6f,%s,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", WallSec(), kind, job,
                   cf.c_str(), ilev.c_str(), ninf.c_str(), inb.c_str(), olvl.c_str(), outb.c_str(),
                   noutf.c_str(), wall.c_str(), cpu.c_str(), ninr.c_str(), nour.c_str(),
                   inbs.c_str(), outbs.c_str(), extra.c_str()));
}

struct CompStart { double ts; std::string reason; double score; uint64_t in_files[kMaxLvl]; uint64_t in_bytes[kMaxLvl]; std::string trigger; };

class L0Listener : public EventListener {
 public:
  void OnFlushBegin(DB* db, const FlushJobInfo& info) override {
    { std::lock_guard<std::mutex> lk(map_mu_); flush_begin_ts_[info.job_id] = WallSec(); }
    Ev("flush_begin", info.job_id, info.cf_name, "", "", "", "", "", "", "", "", "", "", "", "",
       Fmt("reason=%s file=%llu", FlushReasonStr(info.flush_reason),
           (unsigned long long)info.file_number));
  }
  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
    double t0 = WallSec();
    { std::lock_guard<std::mutex> lk(map_mu_); auto itf = flush_begin_ts_.find(info.job_id); if (itf != flush_begin_ts_.end()) t0 = itf->second; }
    uint64_t l0 = 0;  // filled by the sampler, not from the callback thread
    (void)db;
    Ev("flush_installed", info.job_id, info.cf_name, "", "1", "",
       "0", NS((uint64_t)info.table_properties.data_size), "1",
       Fmt("%.0f", (WallSec() - t0) * 1e6), "", NS((uint64_t)info.table_properties.num_entries), "",
       "", "", Fmt("reason=%s frontend_slowdown=%d frontend_stop=%d L0_after=%llu bytes_relaxed=%llu seqno=[%llu,%llu]",
                   FlushReasonStr(info.flush_reason), (int)info.triggered_writes_slowdown,
                   (int)info.triggered_writes_stop, (unsigned long long)l0,
                   (unsigned long long)info.table_properties.data_size,
                   (unsigned long long)info.smallest_seqno, (unsigned long long)info.largest_seqno));
    S.flush_installed.fetch_add(1);
    MaybeKill("flush_installed");
  }
  void OnCompactionBegin(DB* db, const CompactionJobInfo& ci) override {
    CompStart cs;
    cs.ts = WallSec();
    cs.reason = CompactionReasonStr(ci.compaction_reason);
    cs.score = 0;
    for (int i = 0; i < kMaxLvl; i++) { cs.in_files[i] = 0; cs.in_bytes[i] = 0; }
    for (size_t i = 0; i < ci.input_file_infos.size(); i++) {
      int lv = ci.input_file_infos[i].level;
      if (lv < 0 || lv >= kMaxLvl) continue;
      cs.in_files[lv]++;
      auto it = ci.table_properties.find(ci.input_files[i]);
      if (it != ci.table_properties.end()) cs.in_bytes[lv] += it->second->data_size;
    }
    cs.trigger = "";
    { std::lock_guard<std::mutex> lk(map_mu_); comp_starts_[ci.job_id] = cs; }
    std::vector<int> ilev;
    for (auto& f : ci.input_file_infos) ilev.push_back(f.level);
    std::sort(ilev.begin(), ilev.end());
    ilev.erase(std::unique(ilev.begin(), ilev.end()), ilev.end());
    Ev("compaction_begin", ci.job_id, ci.cf_name, JoinInts(ilev), NS(ci.input_file_infos.size()),
       "", "", "", "", "", "", "", "", "", "", Fmt("reason=%s", cs.reason.c_str()));
  }
  void OnCompactionCompleted(DB* db, const CompactionJobInfo& ci) override {
    CompStart cs{};
    { std::lock_guard<std::mutex> lk(map_mu_); auto itc = comp_starts_.find(ci.job_id); if (itc != comp_starts_.end()) cs = itc->second; }
    const CompactionJobStats& st = ci.stats;
    uint64_t out_files[kMaxLvl];
    for (int i = 0; i < kMaxLvl; i++) out_files[i] = 0;
    uint64_t out_bytes = 0;
    for (size_t i = 0; i < ci.output_file_infos.size(); i++) {
      int lv = ci.output_file_infos[i].level;
      if (lv >= 0 && lv < kMaxLvl) out_files[lv]++;
      auto it = ci.table_properties.find(ci.output_files[i]);
      uint64_t fsz = (it != ci.table_properties.end()) ? it->second->data_size : 0;
      out_bytes += fsz;
      c_event_files.Row(Fmt("%.6f,%d,out,%d,%llu,%llu,%llu", WallSec(), ci.job_id, lv,
                            (unsigned long long)ci.output_file_infos[i].file_number,
                            (unsigned long long)fsz,
                            (unsigned long long)(it != ci.table_properties.end() ? it->second->num_entries : 0)));
    }
    for (size_t i = 0; i < ci.input_file_infos.size(); i++) {
      auto it = ci.table_properties.find(ci.input_files[i]);
      c_event_files.Row(Fmt("%.6f,%d,in,%d,%llu,%llu,%llu", WallSec(), ci.job_id,
                            ci.input_file_infos[i].level,
                            (unsigned long long)ci.input_file_infos[i].file_number,
                            (unsigned long long)(it != ci.table_properties.end() ? it->second->data_size : 0),
                            (unsigned long long)(it != ci.table_properties.end() ? it->second->num_entries : 0)));
    }
    (void)db;
    std::string lsm = "NA";   // per-level file counts come from the sampler
    // rich per-job row (E1/E2/E4/E8 core table)
    {
      std::string row = Fmt("%.6f,%.6f,%d,%s,%s,", cs.ts, WallSec(), ci.job_id, ci.cf_name.c_str(),
                            cs.reason.c_str());
      uint64_t tot_in_files = 0, tot_in_bytes = 0;
      for (int lv = 0; lv < kMaxLvl; lv++) { tot_in_files += cs.in_files[lv]; tot_in_bytes += cs.in_bytes[lv]; }
      row += Fmt("%llu,%llu,", (unsigned long long)tot_in_files, (unsigned long long)tot_in_bytes);
      for (int lv = 0; lv < kMaxLvl; lv++) row += Fmt("%llu,", (unsigned long long)cs.in_files[lv]);
      for (int lv = 0; lv < kMaxLvl; lv++) row += Fmt("%llu,", (unsigned long long)cs.in_bytes[lv]);
      row += Fmt("%d,%zu,%llu,", ci.output_level, ci.output_file_infos.size(),
                 (unsigned long long)out_bytes);
      for (int lv = 0; lv < kMaxLvl; lv++) row += Fmt("%llu,", (unsigned long long)out_files[lv]);
      row += Fmt("%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%s,%s",
                 (unsigned long long)st.num_input_records, (unsigned long long)st.num_output_records,
                 (unsigned long long)st.num_records_replaced,
                 (unsigned long long)st.total_skipped_input_bytes,
                 (unsigned long long)st.total_input_bytes,
                 (unsigned long long)st.total_output_bytes,
                 (unsigned long long)st.elapsed_micros, (unsigned long long)st.cpu_micros,
                 (unsigned long long)st.file_write_nanos, (unsigned long long)st.file_fsync_nanos,
                 (unsigned long long)st.num_input_deletion_records,
                 lsm.c_str(),
                 ci.base_input_level >= 0 ? std::to_string(ci.base_input_level).c_str() : "NA");
      c_compactions.Row(row);
    }
    Ev("compaction_installed", ci.job_id, ci.cf_name, "", NS(ci.input_file_infos.size()),
       NS(st.total_input_bytes), NS(ci.output_level), NS(out_bytes),
       NS((uint64_t)ci.output_file_infos.size()), NS(st.elapsed_micros), NS(st.cpu_micros),
       NS(st.num_input_records), NS(st.num_output_records),
       NS(st.num_records_replaced), NS(st.total_skipped_input_bytes),
       Fmt("reason=%s base_input_level=%d subcompactions=%zu lsm=%s aborted=%d",
           cs.reason.c_str(), ci.base_input_level, ci.output_file_infos.size(), lsm.c_str(),
           (int)ci.aborted));
    S.compaction_installed.fetch_add(1);
    MaybeKill("compaction_installed");
  }
  void OnSubcompactionBegin(const SubcompactionJobInfo& si) override {
    Ev("subcompaction_begin", si.job_id, si.cf_name, "", "", "", "", "", "", "", "", "", "", "", "",
       Fmt("sub=%d base_in=%d out=%d reason=%s", si.subcompaction_job_id, si.base_input_level,
           si.output_level, CompactionReasonStr(si.compaction_reason)));
    (void)si;
  }
  void OnSubcompactionCompleted(const SubcompactionJobInfo& si) override {
    Ev("subcompaction_end", si.job_id, si.cf_name, "", "", "", NS(si.output_level), "", "", 
       NS(si.stats.elapsed_micros), NS(si.stats.cpu_micros), NS(si.stats.num_input_records),
       NS(si.stats.num_output_records), NS(si.stats.total_input_bytes), NS(si.stats.total_output_bytes),
       Fmt("sub=%d base_in=%d reason=%s", si.subcompaction_job_id, si.base_input_level,
           CompactionReasonStr(si.compaction_reason)));
  }
  void OnStallConditionsChanged(const WriteStallInfo& info) override {
    const char* st = "?";
    bool begin = false;
    switch (info.condition.cur) {
      case WriteStallCondition::kNormal: st = "normal"; break;
      case WriteStallCondition::kDelayed: st = "delayed"; begin = true; break;
      case WriteStallCondition::kStopped: st = "stopped"; begin = true; break;
    }
    if (begin) S.stall_begin_events.fetch_add(1); else S.stall_end_events.fetch_add(1);
    c_stalls.Row(Fmt("%.6f,%s,%s,%d", WallSec(), info.cf_name.c_str(), st, (int)info.condition.cur));
  }
  void OnTableFileCreated(const TableFileCreationInfo& info) override {
    std::string base = info.file_path;
    size_t slash = base.find_last_of('/');
    if (slash != std::string::npos) base = base.substr(slash + 1);
    uint64_t num = strtoull(base.c_str(), nullptr, 10);
    c_event_files.Row(Fmt("%.6f,%d,created,%d,%llu,%llu,%llu", WallSec(), info.job_id, -1,
                          (unsigned long long)num, (unsigned long long)info.file_size,
                          (unsigned long long)info.table_properties.num_entries));
  }
  void OnTableFileDeleted(const TableFileDeletionInfo& info) override {
    // FileDeletionInfo carries only the path; recover the file number from it.
    std::string base = info.file_path;
    size_t slash = base.find_last_of('/');
    if (slash != std::string::npos) base = base.substr(slash + 1);
    uint64_t num = strtoull(base.c_str(), nullptr, 10);
    Ev("file_deleted", info.job_id, "", "", "", "", "", "", "", "", "", "", "", "", "",
       Fmt("file=%llu path=%s status=%s", (unsigned long long)num, base.c_str(),
           info.status.ToString().c_str()));
  }
  void OnMemTableSealed(const MemTableInfo& info) override {
    Ev("memtable_sealed", -1, info.cf_name, "", "", "", "", "", "", "", "", NS(info.num_entries),
       "", "", "", Fmt("first_seq=%llu earliest_seq=%llu",
                       (unsigned long long)info.first_seqno, (unsigned long long)info.earliest_seqno));
    MaybeKill("memtable_sealed");
  }
  void OnBackgroundError(BackgroundErrorReason reason, Status*) override {
    Ev("background_error", -1, "", "", "", "", "", "", "", "", "", "", "", "", "",
       Fmt("reason=%d", (int)reason));
  }

 private:
  void MaybeKill(const char* ev) {
    if (g.kill_at_event.empty() || g.kill_at_event != ev) return;
    static std::atomic<int64_t> n{0};
    int64_t k = n.fetch_add(1) + 1;
    if (k == g.kill_after_event_n) {
      int64_t confirmed = S.ops_ok.load();
      int64_t done_ = S.ops_done.load();
      S.killed_at_ops.store(confirmed);
      // The confirmation record: with a single writer thread the confirmed key set is
      // exactly [0, confirmed). Flush everything before dying so it survives SIGKILL.
      c_summary_extra.Row(Fmt("kill_confirmation,%lld,%lld,%.6f", (long long)confirmed,
                              (long long)done_, WallSec()));
      Ev("kill_triggered", -1, "", "", "", "", "", "", "", "", "", "", "", "", "",
         Fmt("%s confirmed_ops=%lld ops_done=%lld wall=%.3f", ev, (long long)confirmed,
             (long long)done_, WallSec()));
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      fflush(nullptr);
      kill(getpid(), SIGKILL);
    }
  }
  std::mutex map_mu_;
  std::map<int, double> flush_begin_ts_;
  std::map<int, CompStart> comp_starts_;
};

// ---------------------------------------------------------------------------
// sampled tickers / properties
// ---------------------------------------------------------------------------
struct TickerDef { const char* name; Tickers t; };
static const TickerDef kTickers[] = {
  {"stall_micros", STALL_MICROS},
  {"db_mutex_wait_micros", DB_MUTEX_WAIT_MICROS},
  {"memtable_hit", MEMTABLE_HIT}, {"memtable_miss", MEMTABLE_MISS},
  {"keys_written", NUMBER_KEYS_WRITTEN}, {"keys_read", NUMBER_KEYS_READ},
  {"keys_updated", NUMBER_KEYS_UPDATED},
  {"bytes_written", BYTES_WRITTEN}, {"bytes_read", BYTES_READ},
  {"iter_bytes_read", ITER_BYTES_READ},
  {"db_seek", NUMBER_DB_SEEK}, {"db_next", NUMBER_DB_NEXT},
  {"wal_file_bytes", WAL_FILE_BYTES}, {"wal_file_synced", WAL_FILE_SYNCED},
  {"write_done_by_self", WRITE_DONE_BY_SELF}, {"write_done_by_other", WRITE_DONE_BY_OTHER},
  {"write_with_wal", WRITE_WITH_WAL},
  {"compact_read_bytes", COMPACT_READ_BYTES}, {"compact_write_bytes", COMPACT_WRITE_BYTES},
  {"flush_write_bytes", FLUSH_WRITE_BYTES},
  {"compact_read_bytes_marked", COMPACT_READ_BYTES_MARKED},
  {"compact_write_bytes_marked", COMPACT_WRITE_BYTES_MARKED},
  {"compact_read_bytes_periodic", COMPACT_READ_BYTES_PERIODIC},
  {"compact_write_bytes_periodic", COMPACT_WRITE_BYTES_PERIODIC},
  {"compact_read_bytes_ttl", COMPACT_READ_BYTES_TTL},
  {"compact_write_bytes_ttl", COMPACT_WRITE_BYTES_TTL},
  {"compaction_cpu_total_time", COMPACTION_CPU_TOTAL_TIME},
  {"key_drop_newer_entry", COMPACTION_KEY_DROP_NEWER_ENTRY},
  {"key_drop_obsolete", COMPACTION_KEY_DROP_OBSOLETE},
  {"key_drop_range_del", COMPACTION_KEY_DROP_RANGE_DEL},
  {"key_drop_user", COMPACTION_KEY_DROP_USER},
  {"range_del_drop_obsolete", COMPACTION_RANGE_DEL_DROP_OBSOLETE},
  {"block_cache_miss", BLOCK_CACHE_MISS}, {"block_cache_hit", BLOCK_CACHE_HIT},
  {"block_cache_index_miss", BLOCK_CACHE_INDEX_MISS}, {"block_cache_index_hit", BLOCK_CACHE_INDEX_HIT},
  {"block_cache_filter_miss", BLOCK_CACHE_FILTER_MISS}, {"block_cache_filter_hit", BLOCK_CACHE_FILTER_HIT},
  {"block_cache_data_miss", BLOCK_CACHE_DATA_MISS}, {"block_cache_data_hit", BLOCK_CACHE_DATA_HIT},
  {"block_cache_bytes_read", BLOCK_CACHE_BYTES_READ}, {"block_cache_bytes_write", BLOCK_CACHE_BYTES_WRITE},
  {"bloom_filter_useful", BLOOM_FILTER_USEFUL},
  {"bloom_filter_full_positive", BLOOM_FILTER_FULL_POSITIVE},
  {"bloom_filter_full_true_positive", BLOOM_FILTER_FULL_TRUE_POSITIVE},
  {"no_file_opens", NO_FILE_OPENS}, {"no_file_errors", NO_FILE_ERRORS},
  {"read_amp_estimate_useful_bytes", READ_AMP_ESTIMATE_USEFUL_BYTES},
  {"read_amp_total_read_bytes", READ_AMP_TOTAL_READ_BYTES},
  {"last_level_read_bytes", LAST_LEVEL_READ_BYTES}, {"last_level_read_count", LAST_LEVEL_READ_COUNT},
  {"non_last_level_read_bytes", NON_LAST_LEVEL_READ_BYTES},
  {"non_last_level_read_count", NON_LAST_LEVEL_READ_COUNT},
  {"last_level_seek_data", LAST_LEVEL_SEEK_DATA},
  {"non_last_level_seek_data", NON_LAST_LEVEL_SEEK_DATA},
  {"block_compressed", NUMBER_BLOCK_COMPRESSED}, {"block_decompressed", NUMBER_BLOCK_DECOMPRESSED},
  {"bytes_compressed_from", BYTES_COMPRESSED_FROM}, {"bytes_compressed_to", BYTES_COMPRESSED_TO},
  {"num_iterators_new", NO_ITERATOR_CREATED}, {"num_iterators_del", NO_ITERATOR_DELETED},
  {"number_multiget_calls", NUMBER_MULTIGET_CALLS},
  {"number_multiget_keys_read", NUMBER_MULTIGET_KEYS_READ},
  {"number_multiget_bytes_read", NUMBER_MULTIGET_BYTES_READ},
  {"number_superversion_acquires", NUMBER_SUPERVERSION_ACQUIRES},
  {"number_superversion_cleanups", NUMBER_SUPERVERSION_CLEANUPS},
  {"number_direct_load_table_properties", NUMBER_DIRECT_LOAD_TABLE_PROPERTIES},
  {"files_deleted_immediately", FILES_DELETED_IMMEDIATELY},
};
static const size_t kNumTickers = sizeof(kTickers) / sizeof(kTickers[0]);
static const char* kProps[] = {
  "rocksdb.num-files-at-level0", "rocksdb.num-files-at-level1", "rocksdb.num-files-at-level2",
  "rocksdb.num-files-at-level3", "rocksdb.num-files-at-level4", "rocksdb.num-files-at-level5",
  "rocksdb.num-files-at-level6",
  "rocksdb.estimate-pending-compaction-bytes", "rocksdb.num-immutable-mem-table",
  "rocksdb.mem-table-flush-pending", "rocksdb.compaction-pending",
  "rocksdb.cur-size-all-mem-tables", "rocksdb.size-all-mem-tables",
  "rocksdb.num-entries-active-mem-table", "rocksdb.num-entries-imm-mem-tables",
  "rocksdb.total-sst-files-size", "rocksdb.estimate-live-data-size", "rocksdb.estimate-num-keys",
  "rocksdb.base-level", "rocksdb.num-live-versions", "rocksdb.num-snapshots",
  "rocksdb.is-write-stopped", "rocksdb.actual-delayed-write-rate", "rocksdb.background-errors",
  "rocksdb.oldest-snapshot-time",
  "rocksdb.estimate-table-readers-mem", "rocksdb.block-cache-usage",
  "rocksdb.size-all-mem-tables", "rocksdb.num-running-compactions",
  "rocksdb.num-running-flushes", "rocksdb.estimate-oldest-key-time",
};
static const size_t kNumProps = sizeof(kProps) / sizeof(kProps[0]);

// ---------------------------------------------------------------------------
// process / device counters
// ---------------------------------------------------------------------------
static void ReadProcStat(uint64_t* ut, uint64_t* st_) {
  *ut = *st_ = 0;
  FILE* f = fopen("/proc/self/stat", "r");
  if (!f) return;
  char buf[4096];
  size_t n = fread(buf, 1, sizeof(buf) - 1, f);
  fclose(f);
  buf[n] = 0;
  char* p = strrchr(buf, ')');
  if (!p) return;
  p++;
  int field = 2;
  char* tok = strtok(p, " ");
  while (tok) {
    field++;
    if (field == 14) *ut = strtoull(tok, nullptr, 10);
    if (field == 15) { *st_ = strtoull(tok, nullptr, 10); break; }
    tok = strtok(nullptr, " ");
  }
}
static uint64_t ReadRssBytes() {
  FILE* f = fopen("/proc/self/status", "r");
  if (!f) return 0;
  char line[512];
  uint64_t v = 0;
  while (fgets(line, sizeof(line), f))
    if (strncmp(line, "VmRSS:", 6) == 0) { v = strtoull(line + 6, nullptr, 10) * 1024ull; break; }
  fclose(f);
  return v;
}
static void ReadDiskStats(uint64_t* wr_sect, uint64_t* wr_ios, uint64_t* rd_sect, uint64_t* rd_ios,
                          uint64_t* io_ticks_ms) {
  *wr_sect = *wr_ios = *rd_sect = *rd_ios = 0;
  if (io_ticks_ms) *io_ticks_ms = 0;
  FILE* f = fopen("/proc/diskstats", "r");
  if (!f) return;
  char line[512];
  while (fgets(line, sizeof(line), f)) {
    char dev[64];
    // /proc/diskstats: major minor name reads_completed reads_merged sectors_read
    // ms_reading writes_completed writes_merged sectors_written ms_writing ...
    // field 13 (io_ticks, ms doing I/O) needs the two extra %llu slots below.
    unsigned long long maj, mi, rc, rm, sr, mr, wc, wm, sw, mw, infl, ticks;
    int n = sscanf(line, "%llu %llu %63s %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
                   &maj, &mi, dev, &rc, &rm, &sr, &mr, &wc, &wm, &sw, &mw, &infl, &ticks);
    if (n < 11) continue;
    if (strcmp(dev, "nvme0n1") == 0) {
      *rd_ios = rc; *rd_sect = sr; *wr_ios = wc; *wr_sect = sw;
      if (io_ticks_ms && n >= 13) *io_ticks_ms = ticks;
      break;
    }
  }
  fclose(f);
}

// ---------------------------------------------------------------------------
// sampler
// ---------------------------------------------------------------------------
static DB* g_db = nullptr;
static Statistics* g_stats = nullptr;
static Options g_eff;   // effective options as read back from the opened DB

struct Sampler {
  std::atomic<bool> stop{false};
  void Run() {
    uint64_t ut = 0, st_ = 0;
    double last_layout = 0, last_dir = 0;
    double t_start = WallSec();
    while (!stop.load()) {
      double now = WallSec();
      std::string row = Fmt("%.6f,%.3f", now, now - t_start);
      for (size_t i = 0; i < kNumProps; i++) {
        uint64_t v = 0;
        if (g_db->GetIntProperty(kProps[i], &v)) {
          row += Fmt(",%llu", (unsigned long long)v);
        } else {
          // some properties (notably rocksdb.num-files-at-level<N>) are registered
          // as *string* properties; read them and parse.
          std::string sv;
          if (g_db->GetProperty(kProps[i], &sv) && !sv.empty()) {
            char* end = nullptr;
            unsigned long long pv = strtoull(sv.c_str(), &end, 10);
            row += (end && end != sv.c_str()) ? Fmt(",%llu", pv) : ",-1";
          } else {
            row += ",-1";
          }
        }
      }
      for (size_t i = 0; i < kNumTickers; i++)
        row += Fmt(",%llu", (unsigned long long)g_stats->getTickerCount(kTickers[i].t));
      ReadProcStat(&ut, &st_);
      row += Fmt(",%llu,%llu,%llu", (unsigned long long)ReadRssBytes(),
                 (unsigned long long)ut, (unsigned long long)st_);
      uint64_t ws, wi, rs, ri, iot;
      ReadDiskStats(&ws, &wi, &rs, &ri, &iot);
      row += Fmt(",%llu,%llu,%llu,%llu,%llu", (unsigned long long)ws, (unsigned long long)wi,
                 (unsigned long long)rs, (unsigned long long)ri, (unsigned long long)iot);
      // per-stage service histograms (wall time per job; cumulative since open)
      HistogramData fh{}, ch{};
      g_stats->histogramData(FLUSH_TIME, &fh);
      g_stats->histogramData(COMPACTION_TIME, &ch);
      row += Fmt(",%llu,%llu,%.1f,%.1f,%llu,%llu,%.1f,%.1f",
                 (unsigned long long)fh.count, (unsigned long long)fh.sum,
                 fh.median, fh.percentile99,
                 (unsigned long long)ch.count, (unsigned long long)ch.sum,
                 ch.median, ch.percentile99);
      row += Fmt(",%lld,%lld,%lld,%lld,%lld,%lld,%.6f", (long long)S.ops_ok.load(),
                 (long long)S.ops_err.load(), (long long)S.bytes_written_logical.load(),
                 (long long)S.bytes_read_returned.load(), (long long)S.ops_started.load(),
                 (long long)S.ops_done.load(), now);
      c_samples.Row(row);
      if (now - last_layout >= g.layout_ms / 1000.0) { last_layout = now; SampleLayout(now); }
      if (now - last_dir >= g.dirscan_ms / 1000.0) { last_dir = now; SampleDir(now); }
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
  }
  void SampleLayout(double now) {
    ColumnFamilyMetaData cf;
    g_db->GetColumnFamilyMetaData(&cf);
    std::string base = Fmt("%.6f", now);
    for (auto& lv : cf.levels) {
      c_layout_levels.Row(Fmt("%s,%d,%zu,%llu,%llu", base.c_str(), lv.level, lv.files.size(),
                              (unsigned long long)lv.size, (unsigned long long)cf.size));
      if (lv.level <= 2) {
        for (auto& f : lv.files) {
          c_layout.Row(Fmt("%s,%d,%llu,%llu,%llu,%d,%llu,%s,%s", base.c_str(), lv.level,
                           (unsigned long long)f.file_number, (unsigned long long)f.size,
                           (unsigned long long)f.num_entries, f.being_compacted ? 1 : 0,
                           (unsigned long long)f.file_creation_time,
                           HexKey(f.smallestkey).c_str(), HexKey(f.largestkey).c_str()));
        }
      }
    }
  }
  void SampleDir(double now) {
    uint64_t sst_b = 0, sst_a = 0, wal_b = 0, wal_a = 0, oth_b = 0, oth_a = 0;
    uint64_t n_sst = 0, n_wal = 0, n_oth = 0;
    DIR* d = opendir(g.db_path.c_str());
    if (d) {
      struct dirent* e;
      while ((e = readdir(d)) != nullptr) {
        std::string name = e->d_name;
        if (name == "." || name == "..") continue;
        struct stat stx;
        if (stat((g.db_path + "/" + name).c_str(), &stx) != 0 || !S_ISREG(stx.st_mode)) continue;
        uint64_t alloc = (uint64_t)stx.st_blocks * 512;
        bool is_sst = name.size() > 4 && name.compare(name.size() - 4, 4, ".sst") == 0;
        bool is_log = name.size() > 4 && name.compare(name.size() - 4, 4, ".log") == 0;
        if (is_sst) { sst_b += stx.st_size; sst_a += alloc; n_sst++; }
        else if (is_log) { wal_b += stx.st_size; wal_a += alloc; n_wal++; }
        else { oth_b += stx.st_size; oth_a += alloc; n_oth++; }
      }
      closedir(d);
    }
    c_dirscan.Row(Fmt("%.6f,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu", now,
                      (unsigned long long)sst_b, (unsigned long long)sst_a, (unsigned long long)n_sst,
                      (unsigned long long)wal_b, (unsigned long long)wal_a, (unsigned long long)n_wal,
                      (unsigned long long)oth_b, (unsigned long long)oth_a, (unsigned long long)n_oth,
                      (unsigned long long)DirSizeBytes(g.db_path)));
  }
};
static Sampler SAMPLER;

// ---------------------------------------------------------------------------
// DB open
// ---------------------------------------------------------------------------
static DB* OpenDb(Status* st_out) {
  Options o;
  o.create_if_missing = !g.use_existing_db;
  o.write_buffer_size = (size_t)g.write_buffer_size;
  o.max_write_buffer_number = g.max_write_buffer_number;
  o.level0_file_num_compaction_trigger = g.level0_trigger;
  o.level0_slowdown_writes_trigger = g.level0_slowdown;
  o.level0_stop_writes_trigger = g.level0_stop;
  o.target_file_size_base = g.target_file_size_base;
  o.max_background_jobs = g.max_background_jobs;
  o.max_subcompactions = g.max_subcompactions;
  if (g.max_background_flushes >= 0) o.max_background_flushes = g.max_background_flushes;
  if (g.max_background_compactions >= 0) o.max_background_compactions = g.max_background_compactions;
  o.level_compaction_dynamic_level_bytes = g.dynamic_level_bytes;
  o.disable_auto_compactions = g.disable_auto_compactions;
  if (g.max_total_wal_size) o.max_total_wal_size = g.max_total_wal_size;
  if (g.max_compaction_bytes) o.max_compaction_bytes = g.max_compaction_bytes;
  if (g.num_levels) o.num_levels = g.num_levels;
  if (g.max_bytes_for_level_base) o.max_bytes_for_level_base = g.max_bytes_for_level_base;
  if (g.bytes_per_sync) o.bytes_per_sync = g.bytes_per_sync;
  o.use_direct_io_for_flush_and_compaction = g.use_direct_io;
  o.manual_wal_flush = g.manual_wal_flush;
  if (g.compression == "none") o.compression = kNoCompression;
  else if (g.compression == "lz4") o.compression = kLZ4Compression;
  else if (g.compression == "zstd") o.compression = kZSTD;
  else if (g.compression == "snappy") o.compression = kSnappyCompression;
  else { fprintf(stderr, "unknown compression %s\n", g.compression.c_str()); exit(2); }
  o.bottommost_compression = o.compression;
  BlockBasedTableOptions t;
  t.block_cache = NewLRUCache((size_t)g.cache_size);
  if (g.bloom_bits > 0) t.filter_policy.reset(NewBloomFilterPolicy(g.bloom_bits, false));
  o.table_factory.reset(NewBlockBasedTableFactory(t));
  std::shared_ptr<Statistics> stats = CreateDBStatistics();
  stats->set_stats_level(StatsLevel::kAll);
  o.statistics = stats;
  o.stats_dump_period_sec = g.stats_dump_period_sec;
  o.listeners.push_back(std::shared_ptr<EventListener>(new L0Listener()));
  o.info_log_level = InfoLogLevel::INFO_LEVEL;
  std::vector<ColumnFamilyDescriptor> cfd{ColumnFamilyDescriptor(kDefaultColumnFamilyName, o)};
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> dbptr;
  Status st = DB::Open(DBOptions(o), g.db_path, cfd, &handles, &dbptr);
  *st_out = st;
  if (st.ok()) {
    g_db = dbptr.release();
    g_stats = stats.get();
    std::shared_ptr<Logger> logger;
    Env::Default()->NewLogger((g.out_prefix + ".options.txt").c_str(), &logger);
    if (logger) {
      Options eff = g_db->GetOptions();
      eff.Dump(logger.get());
    }
    g_eff = g_db->GetOptions();
    {  // explicit, guaranteed-nonempty record of the settings that matter here
      FILE* f = fopen((g.out_prefix + ".effective.txt").c_str(), "w");
      if (f) {
        const Options* e = &g_eff;
        fprintf(f, "write_buffer_size=%zu\nmax_write_buffer_number=%d\n", e->write_buffer_size, e->max_write_buffer_number);
        fprintf(f, "level0_file_num_compaction_trigger=%d\nlevel0_slowdown=%d\nlevel0_stop=%d\n",
                e->level0_file_num_compaction_trigger, e->level0_slowdown_writes_trigger, e->level0_stop_writes_trigger);
        fprintf(f, "target_file_size_base=%llu\nmax_background_jobs=%d\nmax_subcompactions=%d\n"
                   "max_background_flushes=%d\nmax_background_compactions=%d\n",
                (unsigned long long)e->target_file_size_base, e->max_background_jobs, e->max_subcompactions,
                e->max_background_flushes, e->max_background_compactions);
        fprintf(f, "level_compaction_dynamic_level_bytes=%d\ndisable_auto_compactions=%d\n",
                (int)e->level_compaction_dynamic_level_bytes, (int)e->disable_auto_compactions);
        fprintf(f, "compression=%d\nnum_levels=%d\nmax_bytes_for_level_base=%llu\n",
                (int)e->compression, e->num_levels, (unsigned long long)e->max_bytes_for_level_base);
        fprintf(f, "max_total_wal_size=%llu\nmax_compaction_bytes=%llu\nbytes_per_sync=%llu\n",
                (unsigned long long)e->max_total_wal_size, (unsigned long long)e->max_compaction_bytes,
                (unsigned long long)e->bytes_per_sync);
        fprintf(f, "max_open_files=%d\nuse_direct_io=%d\nmanual_wal_flush=%d\n",
                e->max_open_files, (int)e->use_direct_io_for_flush_and_compaction, (int)e->manual_wal_flush);
        fclose(f);
      }
    }
  }
  return g_db;
}

// ---------------------------------------------------------------------------
// workers
// ---------------------------------------------------------------------------
struct WorkerStat {
  Hist h_eng, h_e2e;
  Hist h_eng_cls[kClsCount], h_e2e_cls[kClsCount];
  // client-side queueing: scheduled arrival -> entry into the DB call. Only defined
  // for open-loop arms (closed loop has no scheduled arrival). This is the "client
  // queueing" the runbook's overload review asks to be reported alongside offered /
  // submitted / completed.
  Hist h_cli_wait;
  // measurement cohort (§2.2): only requests SUBMITTED inside the measure window
  Hist m_eng, m_e2e;
  Hist m_eng_cls[kClsCount], m_e2e_cls[kClsCount];
  Hist m_cli_wait;
  double w_first_submit = 0, w_last_submit = 0, w_first_eng_done = 0, w_last_eng_done = 0;
  // the same bounds on the raw monotonic clock the latencies are measured on
  uint64_t n_first_submit = 0, n_last_submit = 0, n_first_eng_done = 0, n_last_eng_done = 0;
  int64_t ops = 0, ok = 0, err = 0, reads = 0, hits = 0, writes = 0, details = 0;
  int64_t bytes_w = 0, bytes_r = 0;
  int64_t m_ops = 0, m_ok = 0, m_err = 0, m_reads = 0, m_writes = 0;
  int64_t m_cls[kClsCount] = {};
  int64_t scan_seq = 0;   // per-worker sequential scan cursor (§5 M5)
};

// The first 20 columns are byte-identical to the pre-2026-09-25 detail schema so
// existing consumers keep working; the five appended columns are the new
// timestamps/classification required by CAMPAIGN_V2.md §2.3.
//   log_done_ns >= engine_done_ns by construction. With the WAL enabled and sync=0
//   the WAL append happens inside the db call, so the engine_done->log_done gap is
//   NOT a separate durability wait -- it is the driver's own post-call bookkeeping.
static const char* kDetailHeader =
    "op,key_id,key_tag,sched_ns,submit_ns,done_ns,status,value_len,block_read_count,"
    "block_read_byte,block_cache_hit,block_cache_filter_hit,filter_block_read,"
    "index_block_read,bloom_sst_hit,bloom_sst_miss,get_from_memtable_count,"
    "internal_key_skipped,get_read_bytes,user_key_cmp,op_start_ns,db_call_begin_ns,"
    "log_done_ns,cls,in_cohort";

static void PushDetail(const char* op, int cls, bool in_cohort, uint64_t id, uint64_t tag,
                       uint64_t sched, uint64_t op_start, uint64_t submit, uint64_t done,
                       uint64_t log_done, const std::string& status, int64_t vlen,
                       PerfContext* pc) {
  DETAIL.Push(Fmt("%s,%llu,%llu,%llu,%llu,%llu,%s,%lld,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,"
                  "%llu,%llu,%llu,%llu,%llu,%llu,%llu,%s,%d",
                  op, (unsigned long long)id, (unsigned long long)tag,
                  (unsigned long long)sched, (unsigned long long)submit,
                  (unsigned long long)done, status.c_str(), (long long)vlen,
                  (unsigned long long)(pc ? pc->block_read_count : 0),
                  (unsigned long long)(pc ? pc->block_read_byte : 0),
                  (unsigned long long)(pc ? pc->block_cache_hit_count : 0),
                  (unsigned long long)(pc ? pc->block_cache_filter_hit_count : 0),
                  (unsigned long long)(pc ? pc->filter_block_read_count : 0),
                  (unsigned long long)(pc ? pc->index_block_read_count : 0),
                  (unsigned long long)(pc ? pc->bloom_sst_hit_count : 0),
                  (unsigned long long)(pc ? pc->bloom_sst_miss_count : 0),
                  (unsigned long long)(pc ? pc->get_from_memtable_count : 0),
                  (unsigned long long)(pc ? pc->internal_key_skipped_count : 0),
                  (unsigned long long)(pc ? pc->get_read_bytes : 0),
                  (unsigned long long)(pc ? pc->user_key_comparison_count : 0),
                  (unsigned long long)op_start, (unsigned long long)submit,
                  (unsigned long long)log_done, kClsNames[cls], in_cohort ? 1 : 0),
              cls);
}

// Shut the DB down explicitly before main returns. The DB pointer is intentionally
// not owned by a smart pointer, so without this the DB (and its background threads)
// is never closed: the process exits with flush/compaction threads alive, and their
// listener callbacks race global destruction. Closing here also means the strided
// verification sees a settled WAL instead of one another process must replay.
static void CloseDb() {
  if (!g_db) return;
  g_closing.store(true);
  g_db->Close();
  delete g_db;
  g_db = nullptr;
}

// rocksdb.num-files-at-level<N> is a *string* property in this RocksDB version.
static uint64_t LevelFileCount(DB* db, int level) {
  std::string v;
  if (!db->GetProperty("rocksdb.num-files-at-level" + std::to_string(level), &v)) return 0;
  return strtoull(v.c_str(), nullptr, 10);
}

static void MaybeKillLoops() {
  if (g.kill_after_ops > 0 && S.ops_ok.load() >= g.kill_after_ops) {
    c_summary_extra.Row(Fmt("kill_confirmation,%lld,%lld,%.6f", (long long)S.ops_ok.load(),
                            (long long)S.ops_done.load(), WallSec()));
    Ev("kill_after_ops", -1, "", "", "", "", "", "", "", "", "", "", "", "", "",
       Fmt("confirmed_ops=%lld", (long long)S.ops_ok.load()));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    fflush(nullptr);
    kill(getpid(), SIGKILL);
  }
  if (g.kill_at_s > 0 && (WallSec() - S.t0) >= g.kill_at_s) {
    c_summary_extra.Row(Fmt("kill_confirmation,%lld,%lld,%.6f", (long long)S.ops_ok.load(),
                            (long long)S.ops_done.load(), WallSec()));
    Ev("kill_at_s", -1, "", "", "", "", "", "", "", "", "", "", "", "", "",
       Fmt("wall=%.3f confirmed_ops=%lld", WallSec() - S.t0, (long long)S.ops_ok.load()));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    fflush(nullptr);
    kill(getpid(), SIGKILL);
  }
}

static void WorkLoop(int tid, WorkerStat* ws) {
  std::mt19937_64 rng((uint64_t)g.seed * 131 + 977 * (tid + 1));
  const bool open_loop = (g.arrival == "open" && g.rate > 0);
  DB* db = g_db;
  int read_pct = g.mode == "read" ? 100 : (g.mode == "mix" ? (g.read_pct >= 0 ? g.read_pct : 50) : 0);
  const bool mix_random = (g.mix_sampling != "legacy");
  const bool detail_random = (g.detail_sampling != "stride");
  int64_t stride = g.detail_rate > 0 ? std::max<int64_t>(1, (int64_t)llround(1.0 / g.detail_rate)) : 0;
  int64_t rstride = g.read_log_rate > 0 ? std::max<int64_t>(1, (int64_t)llround(1.0 / g.read_log_rate)) : 0;
  const double detail_p = g.detail_rate, rlog_p = g.read_log_rate;
  auto DrawFrac = [&rng]() { return (rng() >> 11) * (1.0 / 9007199254740992.0); };
  auto Sample = [&](double p, bool random_mode, int64_t st, int64_t i) {
    return random_mode ? (DrawFrac() < p) : (st > 0 && (i % st) == 0);
  };
  char key[16];
  std::string val;
  const uint64_t U = (uint64_t)g.universe;
  const double deadline = g.duration_s > 0 ? S.t0 + g.duration_s : 0;
  // open -> warmup -> measure -> drain (CAMPAIGN_V2.md §2.2). With measure_s == 0 the
  // whole run is the cohort, which is the legacy semantic.
  const bool cohort_mode = (g.measure_s > 0);
  const double t_measure_end = S.t_measure_end_planned.load();
  const double t_warmup_end = S.t_warmup_end.load();
  // One monotonic<->wall anchor per worker: converting per-op monotonic timestamps
  // to wall time this way costs no extra clock reads in the request path.
  const uint64_t anchor_mono = NowNs();
  const double anchor_wall = WallSec();
  auto ToWall = [&](uint64_t ns) { return anchor_wall + (double)((int64_t)ns - (int64_t)anchor_mono) / 1e9; };

  while (!S.stop.load()) {
    // The measurement deadline stops new submissions. Requests already submitted
    // are allowed to complete (that is the drain), so the cohort is fixed here.
    if (cohort_mode && WallSec() >= t_measure_end) break;
    uint64_t sched = 0;
    if (open_loop) {
      sched = PACE.Next();
      while (!S.stop.load() && NowNs() < sched)
        std::this_thread::sleep_for(std::chrono::microseconds(20));
      if (cohort_mode && WallSec() >= t_measure_end) break;
    }
    int64_t i = S.ops_started.fetch_add(1);
    if (g.num_ops > 0 && i >= g.num_ops) { S.ops_started.fetch_sub(1); break; }
    if (deadline > 0 && WallSec() > deadline) { S.ops_started.fetch_sub(1); break; }
    MaybeKillLoops();

    uint64_t op_start = NowNs();
    const bool in_cohort = !cohort_mode || (WallSec() >= t_warmup_end);
    if (in_cohort) S.m_submitted.fetch_add(1);

    bool is_read = read_pct > 0 && (mix_random ? (DrawFrac() * 100.0 < (double)read_pct)
                                               : ((i * 2654435761u) % 100) < (uint64_t)read_pct);
    bool sampled = g.detail_rate > 0 && Sample(detail_p, detail_random, stride, i);
    bool want_detail = sampled;
    if (sampled && S.detail_rows.load() >= g.detail_max_rows) {
      S.detail_cap_hits.fetch_add(1);
      want_detail = false;
    }
    PerfContext* pc = nullptr;
    if (want_detail && g.perf_detail) {
      SetPerfLevel(PerfLevel::kEnableCount);
      get_perf_context()->Reset();
      pc = get_perf_context();
    }
    uint64_t id = 0, tag = kTagMain;
    std::string status = "OK";
    int64_t vlen = 0;
    bool op_ok = true;
    int cls = kClsPut;
    uint64_t submit, engine_done, log_done;
    submit = NowNs();                    // db_call_begin
    if (!is_read) {
      id = G.WriteKey(i, rng);
      EncodeKey(kTagMain, id, key);
      FillValue(id, 1, g.value_size, &val);
      WriteOptions wo;
      wo.sync = g.sync;
      wo.disableWAL = g.disable_wal;
      Status st = db->Put(wo, Slice(key, 16), Slice(val));
      vlen = g.value_size;
      cls = kClsPut;
      if (st.ok()) {
        ws->writes++; S.writes_ok.fetch_add(1);
        ws->bytes_w += g.value_size + 16; S.bytes_written_logical.fetch_add(g.value_size + 16);
        ws->m_cls[kClsPut]++; S.c_cls[kClsPut].fetch_add(1);
      } else { op_ok = false; status = "ERR:" + st.ToString(); }
    } else {
      double u = DrawFrac();
      double acc = 0;
      std::string comp = g_read_mix.empty() ? "uniform" : g_read_mix.back().name;
      for (auto& m : g_read_mix) { acc += m.w; if (u <= acc) { comp = m.name; break; } }
      if (comp == "scan" && g.scan_start == "sequential") {
        // walk the key space in scan_len strides: every scan starts where the last
        // one ended (the "continuous start" arm of runbook §5 M5)
        const int64_t step = std::max<int64_t>(1, g.scan_len);
        id = (uint64_t)(ws->scan_seq++ * step) % U;
        EncodeKey(kTagMain, id, key);
      } else if (comp == "recent") {
        uint64_t lo = U > (uint64_t)g.recent_span ? U - (uint64_t)g.recent_span : 0;
        id = G.Uniform(lo, U, rng);
        EncodeKey(kTagMain, id, key);
      } else if (comp == "deep") {
        id = G.Uniform(0, (uint64_t)g.deep_span, rng);
        EncodeKey(kTagMain, id, key);
      } else if (comp == "neg") {
        id = G.Uniform(0, U, rng);
        EncodeKey(kTagNeg, id, key);
      } else {
        id = G.Uniform(0, U, rng);
        EncodeKey(kTagMain, id, key);
      }
      ReadOptions ro;
      if (comp == "scan") {
        auto* it = db->NewIterator(ro);
        it->Seek(Slice(key, 16));
        int64_t n = 0;
        while (it->Valid() && n < g.scan_len) { vlen += (int64_t)it->value().size(); n++; it->Next(); }
        delete it;
        ws->reads++; S.reads_issued.fetch_add(1);
        cls = kClsScan;
        if (n > 0) { ws->hits++; S.reads_hit.fetch_add(1); }
        status = "SCAN";
        vlen = n * 1000 + (vlen % 1000);
        S.c_cls[kClsScan].fetch_add(1);
      } else {
        std::string out;
        Status st = db->Get(ro, Slice(key, 16), &out);
        ws->reads++; S.reads_issued.fetch_add(1);
        if (st.ok()) {
          ws->hits++; S.reads_hit.fetch_add(1);
          vlen = (int64_t)out.size();
          ws->bytes_r += vlen; S.bytes_read_returned.fetch_add(vlen);
          cls = kClsGetHit;
          S.c_cls[kClsGetHit].fetch_add(1);
        } else if (st.IsNotFound()) {
          // NotFound is a successfully completed read, counted as such but tracked
          // separately from a hit (CAMPAIGN_V2.md §2.3).
          S.reads_notfound.fetch_add(1); vlen = -1; status = "NOTFOUND";
          cls = kClsGetMiss;
          S.c_cls[kClsGetMiss].fetch_add(1);
        } else { op_ok = false; status = "ERR:" + st.ToString(); cls = kClsGetHit; }
      }
      if (Sample(rlog_p, detail_random, rstride, i) && S.read_rows.load() < g.detail_max_rows) {
        S.read_rows.fetch_add(1);
        c_reads.Row(Fmt("%.6f,%llu,%llu,%s,%s,%lld", WallSec(), (unsigned long long)id,
                        (unsigned long long)tag, comp.c_str(), status.c_str(), (long long)vlen));
      }
    }
    engine_done = NowNs();
    // ---- accounting (single place, so the counters cannot disagree) ---------
    const uint64_t e_eng = engine_done - submit;
    const uint64_t e_cli = sched ? (submit - sched) : 0;
    const uint64_t e_e2e = sched ? engine_done - sched : e_eng;
    if (op_ok) { ws->ok++; S.ops_ok.fetch_add(1); } else { ws->err++; S.ops_err.fetch_add(1); }
    S.ops_done.fetch_add(1);
    if (is_read) S.reads_done.fetch_add(1); else S.writes_done.fetch_add(1);
    if (submit < op_start || engine_done < submit) S.ts_order_violations.fetch_add(1);
    ws->ops++;
    ws->h_eng.Add(e_eng); ws->h_e2e.Add(e_e2e);
    ws->h_eng_cls[cls].Add(e_eng); ws->h_e2e_cls[cls].Add(e_e2e);
    if (sched) ws->h_cli_wait.Add(e_cli);
    if (in_cohort) {
      ws->m_ops++;
      if (op_ok) ws->m_ok++; else ws->m_err++;
      ws->m_cls[cls]++;
      ws->m_eng.Add(e_eng); ws->m_e2e.Add(e_e2e);
      ws->m_eng_cls[cls].Add(e_eng); ws->m_e2e_cls[cls].Add(e_e2e);
      if (sched) ws->m_cli_wait.Add(e_cli);
      const double w_submit = ToWall(op_start), w_done = ToWall(engine_done);
      if (ws->w_first_submit == 0) ws->w_first_submit = w_submit;
      ws->w_last_submit = w_submit;
      if (ws->w_first_eng_done == 0) ws->w_first_eng_done = w_done;
      ws->w_last_eng_done = w_done;
      if (ws->n_first_submit == 0) ws->n_first_submit = op_start;
      ws->n_last_submit = op_start;
      if (ws->n_first_eng_done == 0) ws->n_first_eng_done = engine_done;
      ws->n_last_eng_done = engine_done;
      S.m_done.fetch_add(1);
      if (op_ok) S.m_ok.fetch_add(1); else S.m_err.fetch_add(1);
      if (is_read) { S.m_reads.fetch_add(1); if (cls == kClsGetMiss) S.m_notfound.fetch_add(1); }
      else S.m_writes.fetch_add(1);
      S.m_cls[cls].fetch_add(1);
    }
    log_done = NowNs();
    if (log_done < engine_done) S.ts_order_violations.fetch_add(1);
    if (want_detail) {
      ws->details++;
      PushDetail(is_read ? "read" : "write", cls, in_cohort, id, tag, sched, op_start, submit,
                 engine_done, log_done, status, vlen, pc);
      if (g.perf_detail) SetPerfLevel(PerfLevel::kDisable);
    }
    {
      std::lock_guard<std::mutex> lk(PS.mu);
      int64_t sec = (int64_t)(WallSec() - S.t0);
      if (sec >= 0) {
        PS.Ensure(sec);
        PS.h_eng[sec].Add(e_eng);
        PS.h_e2e[sec].Add(e_e2e);
        PS.ops[sec]++;
        if (is_read) { PS.rops[sec]++; if (op_ok) PS.rhit[sec]++; }
        else { PS.wops[sec]++; PS.bytes[sec] += (uint64_t)(g.value_size + 16); }
        if (in_cohort) {
          PS.m_ops[sec]++;
          if (is_read) { PS.m_rops[sec]++; if (op_ok) PS.m_rhit[sec]++; }
          else { PS.m_wops[sec]++; PS.m_bytes[sec] += (uint64_t)(g.value_size + 16); }
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// modes
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// histograms merged out of the workers (whole run and measurement cohort)
// ---------------------------------------------------------------------------
struct Cohort {
  Hist eng, e2e, cli_wait;
  Hist eng_cls[kClsCount], e2e_cls[kClsCount];
  int64_t ops = 0, ok = 0, err = 0, reads = 0, writes = 0, notfound = 0;
  int64_t cls[kClsCount] = {};
  double first_submit = 0, last_submit = 0, first_eng_done = 0, last_eng_done = 0;
  // raw monotonic bounds of the cohort, and the driver's monotonic anchor
  uint64_t n_first_submit = 0, n_last_submit = 0, n_first_eng_done = 0, n_last_eng_done = 0;
  uint64_t n_anchor_mono = 0;
  double n_anchor_wall = 0;
  bool active = false;   // true when measure_s > 0 (explicit measurement window)
};
static Cohort COH;
static Hist W_ENG, W_E2E;
static Hist W_ENG_CLS[kClsCount], W_E2E_CLS[kClsCount];
static int64_t W_CLS[kClsCount] = {};
static int64_t W_WORKER_OPS = 0;

// One histogram file per (scope, class). `ns` is the bucket label under the encoding
// named in the trailing columns (so a legacy file reproduces the historical labels
// byte-for-byte), and ns_upper is the inclusive upper bound of that bucket.
static void WriteHistFile(const std::string& path, const Hist* eng, const Hist* e2e) {
  FILE* f = fopen(path.c_str(), "w");
  if (!f) { fprintf(stderr, "cannot open %s\n", path.c_str()); return; }
  const bool tb = g_hist_track_bounds && eng && !eng->bmin.empty();
  fprintf(f, "ns,engine_count,e2e_count,ns_upper,hist_version,hist_encoding");
  if (tb) fprintf(f, ",sample_min,sample_max,bounds_ok");
  fprintf(f, "\n");
  for (int i = 0; i < Hist::kBuckets; i++) {
    uint64_t ec = eng ? eng->b[i] : 0, sc = e2e ? e2e->b[i] : 0;
    if (!ec && !sc) continue;
    fprintf(f, "%.0f,%llu,%llu,%.0f,%d,%s", Hist::ValEnc(i, g_hist_enc),
            (unsigned long long)ec, (unsigned long long)sc, Hist::Upper(i),
            g_hist_enc ? kHistVersionLegacy : kHistVersionV2,
            g_hist_enc ? kHistEncLegacy : kHistEncV2);
    if (tb) {
      uint64_t mn = eng->bmin[i], mx = eng->bmax[i];
      const bool ok = (mn >= (uint64_t)Hist::Val(i)) && (mx <= (uint64_t)Hist::Upper(i) ||
                      Hist::Upper(i) == (double)UINT64_MAX);
      fprintf(f, ",%llu,%llu,%d", (unsigned long long)(mn == UINT64_MAX ? 0 : mn),
              (unsigned long long)mx, ok ? 1 : 0);
    }
    fprintf(f, "\n");
  }
  fclose(f);
}

static std::string HistPctRow(const char* name, const Hist& h) {
  return Fmt("%s,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%llu", name, h.Pct(0.5), h.Pct(0.9), h.Pct(0.99),
             h.Pct(0.999), h.Pct(0.9999), h.count ? (double)h.sum / h.count : 0.0,
             (unsigned long long)h.mx);
}

// §3.1 acceptance record for the bucket geometry: proves lo <= v <= hi on every
// non-empty bucket and pins the labels the runbook names explicitly.
static void WriteHistSelfCheck(const std::string& path) {
  FILE* f = fopen(path.c_str(), "w");
  if (!f) return;
  uint64_t nonempty = 0, viol = 0;
  for (int i = 0; i < Hist::kBuckets; i++) {
    if (!W_ENG.b[i] && !W_E2E.b[i]) continue;
    nonempty++;
    if (!W_ENG.bmin.empty() && W_ENG.b[i]) {
      const uint64_t lo = (uint64_t)Hist::Val(i), hi = (uint64_t)Hist::Upper(i);
      if (W_ENG.bmin[i] < lo) viol++;
      if (hi != UINT64_MAX && W_ENG.bmax[i] > hi) viol++;
    }
  }
  const uint64_t labels[4] = {256, 288, 512, 1024};
  fprintf(f, "{\n");
  fprintf(f, "  \"hist_version\": %d,\n  \"hist_encoding\": \"%s\",\n",
          g_hist_enc ? kHistVersionLegacy : kHistVersionV2,
          g_hist_enc ? kHistEncLegacy : kHistEncV2);
  fprintf(f, "  \"count\": %llu,\n  \"nonempty_buckets\": %llu,\n  \"bound_violations\": %llu,\n",
          (unsigned long long)W_ENG.count, (unsigned long long)nonempty, (unsigned long long)viol);
  fprintf(f, "  \"p99_le_max\": %d,\n", (W_ENG.count == 0 || W_ENG.Pct(0.99) <= (double)W_ENG.mx) ? 1 : 0);
  fprintf(f, "  \"label_cases\": [\n");
  for (int i = 0; i < 4; i++) {
    const uint64_t v = labels[i];
    const int idx = Hist::Idx(v);
    fprintf(f, "    {\"value\": %llu, \"idx\": %d, \"label\": %.0f, \"label_legacy\": %.0f, "
               "\"upper\": %.0f}%s\n",
            (unsigned long long)v, idx, Hist::Val(idx), Hist::ValLegacy(idx), Hist::Upper(idx),
            i < 3 ? "," : "");
  }
  fprintf(f, "  ],\n");
  fprintf(f, "  \"total_count_conserved\": %d,\n",
          (W_ENG.count == W_E2E.count) ? 1 : 0);
  fprintf(f, "  \"p50\": %.0f,\n  \"p99\": %.0f,\n  \"p999\": %.0f,\n  \"max\": %llu\n",
          W_ENG.Pct(0.5), W_ENG.Pct(0.99), W_ENG.Pct(0.999), (unsigned long long)W_ENG.mx);
  fprintf(f, "}\n");
  fclose(f);
}

static void RunWorkload() {
  S.t0 = WallSec();
  S.n_t0.store(NowNs());   // monotonic anchor for the same instant as S.t0
  const bool cohort_mode = (g.measure_s > 0);
  if (cohort_mode) {
    S.t_warmup_start.store(S.t0);
    S.t_warmup_end.store(S.t0 + g.warmup_s);
    S.t_measure_start.store(S.t0 + g.warmup_s);
    S.t_measure_end_planned.store(S.t0 + g.warmup_s + g.measure_s);
    S.t_measure_end.store(S.t_measure_end_planned.load());
    S.n_warmup_end.store(S.n_t0.load() + (uint64_t)(g.warmup_s * 1e9));
  } else {
    S.t_warmup_start.store(S.t0);
    S.t_warmup_end.store(S.t0);
    S.t_measure_start.store(S.t0);
    S.t_measure_end_planned.store(0);
    S.t_measure_end.store(0);   // open-ended: closed at the cutoff below
    S.n_warmup_end.store(S.n_t0.load());
  }
  if (g.arrival == "open" && g.rate > 0) PACE.Init();
  std::vector<WorkerStat> ws(g.threads);
  std::vector<std::thread> th;
  for (int t = 0; t < g.threads; t++) th.emplace_back(WorkLoop, t, &ws[t]);
  double last = WallSec();
  int64_t last_ops = 0;
  // The cutoff is detected at a cadence that does NOT depend on --progress_ms:
  // --progress_ms controls only how often the progress line is printed. Otherwise
  // a large progress_ms delays detection of the stop condition, which moves the
  // cutoff, which moves the measurement window -- i.e. polling would leak into the
  // reported throughput (exactly what CAMPAIGN_V2.md §3.4 forbids).
  const int detect_ms = (g.progress_ms > 0) ? std::min(g.progress_ms, 50) : 0;
  const double print_period = (g.progress_ms > 0) ? g.progress_ms / 1000.0 : 0.001;
  double last_print = last;
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(detect_ms));
    double now = WallSec();
    int64_t ops = S.ops_done.load();
    if (now - last_print >= print_period) {
      uint64_t imm = 0, l0 = 0, pend = 0, stopw = 0;
      g_db->GetIntProperty("rocksdb.num-immutable-mem-table", &imm);
      l0 = LevelFileCount(g_db, 0);
      g_db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes", &pend);
      g_db->GetIntProperty("rocksdb.is-write-stopped", &stopw);
      const char* phase = cohort_mode
                              ? (now < S.t_warmup_end.load() ? "warmup" : "measure")
                              : "run";
      fprintf(stderr,
              "PROGRESS[%s] %.1fs ops=%lld (%.0f/s) cohort=%lld U=%.2fGB L0=%llu imm=%llu "
              "pend=%.2fGB reads=%lld hit=%.1f%% stall_events=%lld\n",
              phase, now - S.t0, (long long)ops, (ops - last_ops) / (now - last),
              (long long)S.m_done.load(), S.bytes_written_logical.load() / 1073741824.0,
              (unsigned long long)l0, (unsigned long long)imm, pend / 1073741824.0,
              (long long)S.reads_issued.load(),
              S.reads_issued.load() ? 100.0 * S.reads_hit.load() / S.reads_issued.load() : 0.0,
              (long long)S.stall_begin_events.load());
      last = now; last_ops = ops; last_print = now;
    }
    // scheduled runtime option interventions (E5/E6 targeted mitigation)
    for (auto& so : g.set_opts) {
      if (!so.name.empty() && (now - S.t0) >= so.at) {
        // A few knobs (max_background_jobs, ...) live in DBOptions and are only mutable
        // through SetDBOptions; most per-CF knobs go through SetOptions. Try the DB-level
        // call first and fall back, so the intervention cannot silently no-op.
        Status ss = g_db->SetDBOptions({{so.name, so.value}});
        const char* via = "SetDBOptions";
        if (!ss.ok()) {
          Status s2 = g_db->SetOptions({{so.name, so.value}});
          if (s2.ok()) { ss = s2; via = "SetOptions"; }
          else { via = "REJECTED"; ss = s2; }
        }
        Ev("set_option", -1, "", "", "", "", "", "", "", "", "", "", "", "", "",
           Fmt("at=%.1f %s=%s via=%s %s", now - S.t0, so.name.c_str(), so.value.c_str(), via,
               ss.ToString().c_str()));
        so.name.clear();
      }
    }
    last = now; last_ops = ops;
    if (g.num_ops > 0 && S.ops_started.load() >= g.num_ops) break;
    if (g.duration_s > 0 && (now - S.t0) >= g.duration_s) break;
    // measurement deadline: stop issuing new work, then drain what is in flight
    if (cohort_mode && now >= S.t_measure_end.load()) break;
  }  // ---- cutoff / drain ------------------------------------------------------
  // The submission window closes HERE, not at the planned deadline: a run can also
  // end early on --num_ops/--duration_s, and the service throughput denominator
  // must be the window the cohort was actually admitted over.
  const double t_cutoff = WallSec();
  S.cutoff.store(true);
  S.n_cutoff.store(NowNs());
  S.t_measure_end.store(t_cutoff);
  S.t_work_end.store(t_cutoff);
  S.t_drain_start.store(t_cutoff);
  {
    const int64_t inflight = S.m_submitted.load() - S.m_done.load();
    S.inflight_at_cutoff.store(inflight < 0 ? 0 : inflight);
    fprintf(stderr, "CUTOFF at %.3f (cohort submitted=%lld done=%lld inflight=%lld)\n",
            t_cutoff - S.t0, (long long)S.m_submitted.load(), (long long)S.m_done.load(),
            (long long)S.inflight_at_cutoff.load());
  }
  S.stop.store(true);
  for (auto& x : th) if (x.joinable()) x.join();   // drain: every in-flight request done
  S.t_drain_end.store(WallSec());
  S.n_drain_end.store(NowNs());
  {
    const int64_t censored = S.m_submitted.load() - S.m_done.load();
    S.censored.store(censored < 0 ? 0 : censored);
    if (S.censored.load() > 0)
      fprintf(stderr, "CENSORED %lld cohort requests did not complete before drain end\n",
              (long long)S.censored.load());
  }
  // ---- merge ---------------------------------------------------------------
  for (auto& x : ws) {
    W_ENG.Merge(x.h_eng); W_E2E.Merge(x.h_e2e);
    for (int c = 0; c < kClsCount; c++) {
      W_ENG_CLS[c].Merge(x.h_eng_cls[c]);
      W_E2E_CLS[c].Merge(x.h_e2e_cls[c]);
      W_CLS[c] += x.m_cls[c];
    }
    W_WORKER_OPS += x.ops;
    COH.eng.Merge(x.m_eng); COH.e2e.Merge(x.m_e2e); COH.cli_wait.Merge(x.m_cli_wait);
    for (int c = 0; c < kClsCount; c++) {
      COH.eng_cls[c].Merge(x.m_eng_cls[c]);
      COH.e2e_cls[c].Merge(x.m_e2e_cls[c]);
      COH.cls[c] += x.m_cls[c];
    }
    COH.ops += x.m_ops; COH.ok += x.m_ok; COH.err += x.m_err;
    COH.reads += x.m_reads; COH.writes += x.m_writes;
    if (x.w_first_submit && (COH.first_submit == 0 || x.w_first_submit < COH.first_submit))
      COH.first_submit = x.w_first_submit;
    if (x.w_last_submit > COH.last_submit) COH.last_submit = x.w_last_submit;
    if (x.w_first_eng_done && (COH.first_eng_done == 0 || x.w_first_eng_done < COH.first_eng_done))
      COH.first_eng_done = x.w_first_eng_done;
    if (x.w_last_eng_done > COH.last_eng_done) COH.last_eng_done = x.w_last_eng_done;
    if (x.n_first_submit && (COH.n_first_submit == 0 || x.n_first_submit < COH.n_first_submit))
      COH.n_first_submit = x.n_first_submit;
    if (x.n_last_submit > COH.n_last_submit) COH.n_last_submit = x.n_last_submit;
    if (x.n_first_eng_done && (COH.n_first_eng_done == 0 ||
                               x.n_first_eng_done < COH.n_first_eng_done))
      COH.n_first_eng_done = x.n_first_eng_done;
    if (x.n_last_eng_done > COH.n_last_eng_done) COH.n_last_eng_done = x.n_last_eng_done;
  }
  COH.notfound = S.m_notfound.load();
  COH.active = cohort_mode;
  COH.n_anchor_mono = S.n_t0.load();
  COH.n_anchor_wall = S.t0;
  // ---- per-class and aggregate histograms ----------------------------------
  c_summary_extra.Row(HistPctRow("total_hist", W_ENG));
  c_summary_extra.Row(HistPctRow("e2e_hist", W_E2E));
  for (int c = 0; c < kClsCount; c++) {
    c_summary_extra.Row(HistPctRow(Fmt("eng_hist_%s", kClsNames[c]).c_str(), W_ENG_CLS[c]));
    c_summary_extra.Row(HistPctRow(Fmt("e2e_hist_%s", kClsNames[c]).c_str(), W_E2E_CLS[c]));
  }
  c_summary_extra.Row(HistPctRow("cohort_hist", COH.eng));
  c_summary_extra.Row(HistPctRow("cohort_e2e_hist", COH.e2e));
  if (COH.cli_wait.count) c_summary_extra.Row(HistPctRow("cohort_client_wait", COH.cli_wait));
  for (int c = 0; c < kClsCount; c++) {
    c_summary_extra.Row(HistPctRow(Fmt("cohort_eng_%s", kClsNames[c]).c_str(), COH.eng_cls[c]));
    c_summary_extra.Row(HistPctRow(Fmt("cohort_e2e_%s", kClsNames[c]).c_str(), COH.e2e_cls[c]));
  }
  WriteHistFile(g.out_prefix + ".hist.csv", &W_ENG, &W_E2E);
  for (int c = 0; c < kClsCount; c++)
    WriteHistFile(g.out_prefix + ".hist_" + kClsNames[c] + ".csv", &W_ENG_CLS[c], &W_E2E_CLS[c]);
  WriteHistFile(g.out_prefix + ".hist_cohort.csv", &COH.eng, &COH.e2e);
  if (COH.cli_wait.count)
    WriteHistFile(g.out_prefix + ".hist_cohort_client_wait.csv", &COH.cli_wait, nullptr);
  for (int c = 0; c < kClsCount; c++)
    WriteHistFile(g.out_prefix + ".hist_cohort_" + kClsNames[c] + ".csv", &COH.eng_cls[c],
                  &COH.e2e_cls[c]);
  if (g.hist_track_bounds) WriteHistSelfCheck(g.out_prefix + ".hist_selfcheck.json");
}

static void RunBacklog() {
  const uint64_t K = (uint64_t)g.backlog_files;
  const uint64_t per = (uint64_t)g.backlog_keys_per_file;
  const uint64_t M = g.backlog_region > 0 ? (uint64_t)g.backlog_region : K * per;
  const uint64_t U = (uint64_t)g.universe;
  if (M > U) { fprintf(stderr, "backlog region > universe\n"); exit(2); }
  DB* db = g_db;
  S.t0 = WallSec();
  fprintf(stderr, "BACKLOG start K=%llu keys_per_file=%llu M=%llu region=[%llu,%llu) overlap=%s\n",
          (unsigned long long)K, (unsigned long long)per, (unsigned long long)M,
          (unsigned long long)(U - M), (unsigned long long)U, g.backlog_overlap.c_str());
  std::vector<uint64_t> ids(M);
  for (uint64_t j = 0; j < M; j++) ids[j] = U - M + j;
  std::string val;
  for (uint64_t f = 0; f < K; f++) {
    WriteOptions wo;
    wo.sync = g.sync;
    int64_t n = 0;
    uint64_t first = 0, last = 0;
    const bool narrow = (g.backlog_overlap == "narrow");
    uint64_t j0 = narrow ? f * per : f;
    uint64_t j1 = narrow ? std::min<uint64_t>((f + 1) * per, M) : M;
    for (uint64_t j = j0; j < j1; j += (narrow ? 1 : K)) {
      uint64_t id = ids[j];
      if (n == 0) first = id;
      last = id;
      char key[16];
      EncodeKey(kTagMain, id, key);
      FillValue(id, 2, g.value_size, &val);
      Status st = db->Put(wo, Slice(key, 16), Slice(val));
      // ops_done is what every other mode counts, so the per-run invariant
      // ops_done == ops_ok + ops_err has to hold here too; a build that only bumped
      // ops_ok made the accounting look broken for a perfectly good run.
      S.ops_done.fetch_add(1);
      S.writes_done.fetch_add(1);
      if (st.ok()) { S.bytes_written_logical.fetch_add(g.value_size + 16); S.ops_ok.fetch_add(1); }
      else { S.ops_err.fetch_add(1); fprintf(stderr, "put error: %s\n", st.ToString().c_str()); }
      n++;
    }
    FlushOptions fo;
    fo.wait = true;
    double t0 = WallSec();
    Status fs = db->Flush(fo);
    uint64_t l0 = 0;
    l0 = LevelFileCount(db, 0);
    fprintf(stderr, "BACKLOG file %llu/%llu keys=%lld id_range=[%llu,%llu] flush=%.3fs %s L0=%llu\n",
            (unsigned long long)(f + 1), (unsigned long long)K, (long long)n,
            (unsigned long long)first, (unsigned long long)last, WallSec() - t0,
            fs.ToString().c_str(), (unsigned long long)l0);
  }
  double hold = std::max(3.0, g.layout_ms / 1000.0 * 2);
  fprintf(stderr, "BACKLOG frozen; holding %.1fs\n", hold);
  double t = WallSec();
  while (WallSec() - t < hold) std::this_thread::sleep_for(std::chrono::milliseconds(300));
}

static void RunCompact() {
  S.t0 = WallSec();
  if (g.mode == "flush") {
    FlushOptions fo; fo.wait = true;
    Status st = g_db->Flush(fo);
    fprintf(stderr, "FLUSH %s\n", st.ToString().c_str());
  } else {
    CompactRangeOptions cro;
    cro.exclusive_manual_compaction = false;
    Status st = g_db->CompactRange(cro, nullptr, nullptr);
    fprintf(stderr, "COMPACTRANGE %s\n", st.ToString().c_str());
  }
}

static int RunVerify() {
  Status st;
  DB* db = OpenDb(&st);
  if (!st.ok()) { fprintf(stderr, "open failed: %s\n", st.ToString().c_str()); return 1; }
  int64_t checked = 0, ok = 0, missing = 0, bad = 0;
  std::string v;
  const uint64_t U = (uint64_t)g.universe;
  const uint64_t vf = g.verify_from >= 0 ? (uint64_t)g.verify_from : 0;
  const uint64_t vt = g.verify_to >= 0 ? (uint64_t)g.verify_to : U;
  ReadOptions ro;
  for (uint64_t id = vf; id < vt; id += (uint64_t)g.verify_step) {
    char key[16];
    EncodeKey(kTagMain, id, key);
    checked++;
    Status s = db->Get(ro, Slice(key, 16), &v);
    if (s.ok()) { if (CheckValue(id, g.verify_salt, Slice(v))) ok++; else bad++; }
    else missing++;
  }
  printf("VERIFY checked=%lld ok=%lld missing=%lld badvalue=%lld salt=%llu\n", (long long)checked,
         (long long)ok, (long long)missing, (long long)bad, (unsigned long long)g.verify_salt);
  fflush(stdout);
  CloseDb();
  return bad == 0 ? 0 : 1;
}

// ---------------------------------------------------------------------------
// §3.1 latency-bucket acceptance, offline (no DB).
// Feeds a fixed sample set (the exact ranges the runbook names, plus >= 1e6
// pseudo-random values), then emits:
//   <out>.hist.csv          v2 labels + upper bounds
//   <out>.hist_legacy.csv   the v1 (legacy) labels, for parser validation
//   <out>.hist_buckets.csv  idx + label + upper + count + sample_min/max per bucket,
//                           so an independent implementation can re-derive the
//                           expected label from the bucket index and compare
//   <out>.hist_selfcheck.json  violations, P99<=max, count conservation, label cases
// ---------------------------------------------------------------------------
static int RunHistCheck() {
  g_hist_track_bounds = true;
  g.hist_track_bounds = true;
  Hist h;
  for (uint64_t v = 256; v <= 287; v++) h.Add(v);      // must label 256
  for (uint64_t v = 288; v <= 319; v++) h.Add(v);      // must label 288
  for (uint64_t v = 512; v <= 543; v++) h.Add(v);      // must label 512
  std::mt19937_64 rng((uint64_t)g.seed);
  for (int i = 0; i < 1000000; i++) h.Add(1 + rng() % 100000000ull);
  const uint64_t rand_n = 1000000;
  W_ENG = h;
  W_E2E = h;
  WriteHistFile(g.out_prefix + ".hist.csv", &W_ENG, &W_E2E);
  {
    FILE* f = fopen((g.out_prefix + ".hist_buckets.csv").c_str(), "w");
    if (f) {
      fprintf(f, "idx,ns,ns_upper,count,sample_min,sample_max\n");
      for (int i = 0; i < Hist::kBuckets; i++) {
        if (!h.b[i]) continue;
        fprintf(f, "%d,%.0f,%.0f,%llu,%llu,%llu\n", i, Hist::Val(i), Hist::Upper(i),
                (unsigned long long)h.b[i], (unsigned long long)h.bmin[i],
                (unsigned long long)h.bmax[i]);
      }
      fclose(f);
    }
  }
  const int saved_enc = g_hist_enc;
  g_hist_enc = 1;
  WriteHistFile(g.out_prefix + ".hist_legacy.csv", &W_ENG, &W_E2E);
  g_hist_enc = saved_enc;
  WriteHistSelfCheck(g.out_prefix + ".hist_selfcheck.json");
  // independent in-process re-derivation of the required labels
  uint64_t viol = 0, bad_label = 0, nonempty = 0;
  for (int i = 0; i < Hist::kBuckets; i++) {
    if (!h.b[i]) continue;
    nonempty++;
    const uint64_t lo = (uint64_t)Hist::Val(i);
    const uint64_t hi = (uint64_t)Hist::Upper(i);
    if (h.bmin[i] < lo || (hi != UINT64_MAX && h.bmax[i] > hi)) viol++;
  }
  struct { uint64_t lo, hi, want; } cases[] = {{256, 287, 256}, {288, 319, 288}, {512, 543, 512}};
  for (auto& c : cases) {
    if ((uint64_t)Hist::Val(Hist::Idx(c.lo)) != c.want) bad_label++;
    if ((uint64_t)Hist::Val(Hist::Idx(c.hi)) != c.want) bad_label++;
  }
  const bool p99_ok = (h.count == 0) || (h.Pct(0.99) <= (double)h.mx);
  const uint64_t expect = 3 * 32 + rand_n;
  const bool cnt_ok = (h.count == expect);
  printf("HISTCHECK count=%llu expected=%llu nonempty=%llu bound_violations=%llu "
         "label_mismatches=%llu p99=%.0f max=%llu p99_le_max=%d count_ok=%d encoding=%s\n",
         (unsigned long long)h.count, (unsigned long long)expect, (unsigned long long)nonempty,
         (unsigned long long)viol, (unsigned long long)bad_label, h.Pct(0.99),
         (unsigned long long)h.mx, p99_ok ? 1 : 0, cnt_ok ? 1 : 0, kHistEncV2);
  fflush(stdout);
  return (viol == 0 && bad_label == 0 && p99_ok && cnt_ok) ? 0 : 1;
}

// ---------------------------------------------------------------------------
// §3.3 scan acceptance: scan a fixed range from a fixed start and report the key
// count, the total value bytes and an FNV-1a-64 checksum over the values in scan
// order. An independent implementation (bench/v2/p0_verify.py) recomputes all
// three from the value generator, so this checks content, length and count.
// ---------------------------------------------------------------------------
static uint64_t Fnv1a64(const char* p, size_t n, uint64_t h = 14695981039346656037ull) {
  for (size_t i = 0; i < n; i++) { h ^= (unsigned char)p[i]; h *= 1099511628211ull; }
  return h;
}
static int RunScanCheck() {
  Status st;
  DB* db = OpenDb(&st);
  if (!st.ok()) { fprintf(stderr, "open failed: %s\n", st.ToString().c_str()); return 1; }
  const uint64_t start = g.verify_from >= 0 ? (uint64_t)g.verify_from : 0;
  const int64_t limit = g.scan_len > 0 ? g.scan_len : 1000;
  char key[16];
  EncodeKey(kTagMain, start, key);
  ReadOptions ro;
  auto* it = db->NewIterator(ro);
  int64_t n = 0, badvalue = 0;
  uint64_t bytes = 0, checksum = 14695981039346656037ull;
  uint64_t first_id = 0, last_id = 0;
  for (it->Seek(Slice(key, 16)); it->Valid() && n < limit; it->Next()) {
    const uint64_t id = DecodeKeyId(it->key().data());
    const Slice v = it->value();
    if (n == 0) first_id = id;
    last_id = id;
    if (!CheckValue(id, g.verify_salt, v)) badvalue++;
    bytes += v.size();
    checksum = Fnv1a64(v.data(), v.size(), checksum);
    n++;
  }
  delete it;
  printf("SCANCHECK keys=%lld badvalue=%lld bytes=%llu checksum=%llu first_key_id=%llu "
         "last_key_id=%llu value_size=%d salt=%llu\n",
         (long long)n, (long long)badvalue, (unsigned long long)bytes,
         (unsigned long long)checksum, (unsigned long long)first_id, (unsigned long long)last_id,
         g.value_size, (unsigned long long)g.verify_salt);
  fflush(stdout);
  CloseDb();
  return (badvalue == 0 && n == limit) ? 0 : 1;
}

int main(int argc, char** argv) {
  ParseArgs(argc, argv);
  ParseReadMix();
  if (g.key_size != 16) { fprintf(stderr, "only key_size=16 supported\n"); return 2; }
  G.Build();   // permutation / zipf tables referenced by every key selection
  char hostbuf[256] = {0};
  if (gethostname(hostbuf, sizeof(hostbuf) - 1) != 0) snprintf(hostbuf, sizeof(hostbuf), "unknown");
  FILE* cfgf = fopen((g.out_prefix + ".config.txt").c_str(), "w");
  if (cfgf) {
    fprintf(cfgf, "argv:");
    for (int i = 0; i < argc; i++) fprintf(cfgf, " %s", argv[i]);
    fprintf(cfgf, "\nrocksdb_version=%d.%d.%d\n", ROCKSDB_MAJOR, ROCKSDB_MINOR, ROCKSDB_PATCH);
    fprintf(cfgf, "start_wall=%.6f\npid=%d\nhost=%s\n", WallSec(), (int)getpid(), hostbuf);
    fprintf(cfgf, "hist_version=%d\nhist_encoding=%s\n",
            g_hist_enc ? kHistVersionLegacy : kHistVersionV2,
            g_hist_enc ? kHistEncLegacy : kHistEncV2);
    fprintf(cfgf, "warmup_s=%.6f\nmeasure_s=%.6f\nmix_sampling=%s\ndetail_sampling=%s\n",
            g.warmup_s, g.measure_s, g.mix_sampling.c_str(), g.detail_sampling.c_str());
  }
  if (g.mode == "verify") return RunVerify();
  // Offline self-tests of the measurement instrument itself (§3.1 buckets, §3.3 scans).
  // Neither touches a DB, so they can run on any host.
  if (g.mode == "histcheck") return RunHistCheck();
  if (g.mode == "scancheck") return RunScanCheck();

  std::string hdr_props, hdr_tick;
  for (size_t i = 0; i < kNumProps; i++) hdr_props += "," + std::string(kProps[i]).substr(8);
  for (size_t i = 0; i < kNumTickers; i++) hdr_tick += "," + std::string(kTickers[i].name);
  c_samples.Open(g.out_prefix + ".samples.csv",
                 "ts,elapsed" + hdr_props + hdr_tick +
                 ",rss_bytes,utime_ticks,stime_ticks,dev_wr_sectors,dev_wr_ios,dev_rd_sectors,"
                 "dev_rd_ios,dev_io_ticks_ms,flush_time_cnt,flush_time_sum_us,flush_time_p50_us,"
                 "flush_time_p99_us,comp_time_cnt,comp_time_sum_us,comp_time_p50_us,"
                 "comp_time_p99_us,u_ops,u_err,u_bytes,u_read_bytes,ops_started,ops_done,ts_dup");
  c_layout.Open(g.out_prefix + ".layout_files.csv",
                "ts,level,file_number,file_size,num_entries,being_compacted,file_creation_time,"
                "smallest_hex,largest_hex");
  c_layout_levels.Open(g.out_prefix + ".layout_levels.csv",
                       "ts,level,num_files,total_bytes,cf_total_bytes");
  c_dirscan.Open(g.out_prefix + ".dirscan.csv",
                 "ts,sst_bytes,sst_alloc_bytes,n_sst,wal_bytes,wal_alloc_bytes,n_wal,other_bytes,"
                 "other_alloc_bytes,n_other,db_total_bytes");
  // async detail sink: the request path only pushes into a bounded ring; a writer
  // thread drains it. Sampling rate / dropped / queue depth are all reported.
  DETAIL.Open(g.out_prefix, kDetailHeader);
  c_persec.Open(g.out_prefix + ".per_sec.csv",
                "ts,sec,ops,wops,rops,rhit,bytes,eng_p50_us,eng_p90_us,eng_p99_us,eng_p999_us,"
                "eng_max_us,e2e_p50_us,e2e_p90_us,e2e_p99_us,e2e_p999_us,e2e_max_us,"
                "phase,m_ops,m_rops,m_wops,m_rhit,m_bytes");
  c_reads.Open(g.out_prefix + ".read_keys.csv", "ts,key_id,key_tag,component,status,value_len");
  c_events.Open(g.out_prefix + ".events.csv",
                "ts,kind,job,cf,input_levels,num_input_files,input_bytes,output_level,output_bytes,"
                "num_output_files,wall_us,cpu_us,num_input_records,num_output_records,"
                "input_bytes_stats,output_bytes_stats,extra");
  c_event_files.Open(g.out_prefix + ".event_files.csv",
                     "ts,job,dir,level,file_number,data_size,num_entries");
  c_stalls.Open(g.out_prefix + ".stalls.csv", "ts,cf,condition,condition_code");
  {
    std::string h = "ts_begin,ts_end,job,cf,reason,num_input_files,total_input_bytes,";
    for (int l = 0; l < kMaxLvl; l++) h += Fmt("in_files_%d,", l);
    for (int l = 0; l < kMaxLvl; l++) h += Fmt("in_bytes_%d,", l);
    h += "output_level,num_output_files,output_bytes,";
    for (int l = 0; l < kMaxLvl; l++) h += Fmt("out_files_%d,", l);
    h += "num_input_records,num_output_records,num_records_replaced,skipped_input_bytes,"
         "stats_total_input_bytes,stats_total_output_bytes,elapsed_us,cpu_us,file_write_ns,"
         "file_fsync_ns,num_input_deletion_records,lsm_state_after,base_input_level";
    c_compactions.Open(g.out_prefix + ".compactions.csv", h);
  }
  c_summary_extra.Open(g.out_prefix + ".summary_extra.csv", "name,p50_ns,p90_ns,p99_ns,p999_ns,p9999_ns,mean_ns,max_ns");

  { uint64_t wi0, rs0; ReadDiskStats(&g_dev_wr0, &wi0, &g_dev_rd0, &rs0, nullptr); }
  S.t0 = WallSec();
  S.t_open_start.store(S.t0);
  double t_open0 = WallSec();
  Status st;
  DB* db = OpenDb(&st);
  double open_seconds = WallSec() - t_open0;
  S.t_open_end.store(WallSec());
  if (!st.ok()) { fprintf(stderr, "OPEN FAILED: %s\n", st.ToString().c_str()); return 1; }
  if (strcmp(g.mode.c_str(), "open") == 0 || strcmp(g.mode.c_str(), "verify") == 0) {
    printf("OPEN_OK=1 open_seconds=%.6f\n", open_seconds);
    c_summary_extra.Row(Fmt("open_seconds,%.6f", open_seconds));
    fflush(stdout);
  }
  DETAIL.Start();
  std::thread sampler([]() { SAMPLER.Run(); });

  const char* m = g.mode.c_str();
  if (strcmp(m, "backlog") == 0) RunBacklog();
  else if (strcmp(m, "compact") == 0 || strcmp(m, "flush") == 0) RunCompact();
  else if (strcmp(m, "open") == 0) { std::this_thread::sleep_for(std::chrono::milliseconds(500)); }
  else RunWorkload();

  if (g.drain_observe_s > 0) {
    double t = WallSec();
    fprintf(stderr, "DRAIN observing %.1fs\n", g.drain_observe_s);
    while (WallSec() - t < g.drain_observe_s) std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  if (g.wait_for_drain) {
    // Logical drain (runbook §6): no pending compulsive work -- no pending
    // compaction bytes, no immutable memtable, no pending or running compaction.
    // L0==0 is a PHYSICAL condition and is deliberately NOT required by default: a
    // level can legitimately hold a file while deeper compaction settles, and
    // demanding it only burns the timeout. Ask for it explicitly with
    // --drain_require_l0_zero=1 when a physically empty L0 is the point.
    double t = WallSec();
    uint64_t l0 = 0, pend = 0, imm = 0, cp = 0, running = 0;
    while (WallSec() - t < g.wait_for_drain_max_s) {
      l0 = LevelFileCount(g_db, 0);
      g_db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes", &pend);
      g_db->GetIntProperty("rocksdb.num-immutable-mem-table", &imm);
      g_db->GetIntProperty("rocksdb.compaction-pending", &cp);
      g_db->GetIntProperty("rocksdb.num-running-compactions", &running);
      const bool logical = (pend == 0 && imm == 0 && cp == 0 && running == 0);
      if (logical && (!g.drain_require_l0_zero || l0 == 0)) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    fprintf(stderr, "DRAIN waited %.1fs (l0=%llu pend=%llu imm=%llu comp_pending=%llu "
                    "running=%llu require_l0_zero=%d)\n",
            WallSec() - t, (unsigned long long)l0, (unsigned long long)pend,
            (unsigned long long)imm, (unsigned long long)cp, (unsigned long long)running,
            g.drain_require_l0_zero ? 1 : 0);
  }
  if (g.final_flush) {
    FlushOptions fo; fo.wait = true;
    S.t_final_flush_start.store(WallSec());
    double t0 = WallSec();
    Status fs = g_db->Flush(fo);
    S.t_final_flush_end.store(WallSec());
    fprintf(stderr, "FINAL_FLUSH %s in %.3fs\n", fs.ToString().c_str(), WallSec() - t0);
    if (g.final_flush_observe_s > 0) {
      double t = WallSec();
      while (WallSec() - t < g.final_flush_observe_s) std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
  }
  double t_end = WallSec();
  SAMPLER.SampleLayout(t_end);
  SAMPLER.SampleDir(t_end);
  SAMPLER.stop.store(true);
  sampler.join();
  // Drain and close the detail sink before reporting its counters.
  DETAIL.Stop();

  uint64_t per_sec_cohort_sum = 0;
  {
    std::lock_guard<std::mutex> lk(PS.mu);
    const double warmup_len = COH.active ? (S.t_warmup_end.load() - S.t0) : 0;
    const double measure_len = COH.active ? (S.t_measure_end.load() - S.t_warmup_end.load()) : 0;
    for (size_t i = 0; i < PS.h_eng.size(); i++) {
      const char* phase = "run";
      if (COH.active)
        phase = ((double)i < warmup_len) ? "warmup"
                                         : (((double)i < warmup_len + measure_len) ? "measure" : "drain");
      per_sec_cohort_sum += PS.m_ops[i];
      c_persec.Row(Fmt("%.6f,%zu,%llu,%llu,%llu,%llu,%llu,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,"
                       "%s,%llu,%llu,%llu,%llu,%llu",
                       S.t0 + (double)i, i, (unsigned long long)PS.ops[i], (unsigned long long)PS.wops[i],
                       (unsigned long long)PS.rops[i], (unsigned long long)PS.rhit[i],
                       (unsigned long long)PS.bytes[i],
                       PS.h_eng[i].Pct(0.5) / 1000.0, PS.h_eng[i].Pct(0.9) / 1000.0,
                       PS.h_eng[i].Pct(0.99) / 1000.0, PS.h_eng[i].Pct(0.999) / 1000.0,
                       PS.h_eng[i].mx / 1000.0,
                       PS.h_e2e[i].Pct(0.5) / 1000.0, PS.h_e2e[i].Pct(0.9) / 1000.0,
                       PS.h_e2e[i].Pct(0.99) / 1000.0, PS.h_e2e[i].Pct(0.999) / 1000.0,
                       PS.h_e2e[i].mx / 1000.0, phase,
                       (unsigned long long)PS.m_ops[i], (unsigned long long)PS.m_rops[i],
                       (unsigned long long)PS.m_wops[i], (unsigned long long)PS.m_rhit[i],
                       (unsigned long long)PS.m_bytes[i]));
    }
  }
  double dur = t_end - S.t0;
  uint64_t ws_, wi, rs, ri, iot_;
  ReadDiskStats(&ws_, &wi, &rs, &ri, &iot_);
  // ---- derived quantities and invariants (CAMPAIGN_V2.md §2.2/§2.3) ---------
  // The cohort was admitted over [measure_start, last_submit]. Using the last actual
  // submission (rather than the instant the main thread noticed the stop condition)
  // makes the throughput denominator independent of polling cadence: an idle tail
  // between the final submission and the cutoff cannot dilute service throughput.
  const double win_start = COH.active ? S.t_measure_start.load() : S.t0;
  const bool have_last_submit = (COH.last_submit > win_start);
  const double win_end = have_last_submit
                             ? COH.last_submit
                             : (COH.active ? S.t_measure_end.load() : S.t_work_end.load());
  const char* win_end_src = have_last_submit ? "last_submit" : "cutoff";
  const double win_len = win_end - win_start;
  const double svc_throughput = (win_len > 0) ? (double)COH.ops / win_len : 0.0;
  const double whole_throughput = (dur > 0) ? (double)S.ops_ok.load() / dur : 0.0;
  const int64_t ops_balance = S.ops_ok.load() + S.ops_err.load();
  const int64_t rw_sum = S.reads_done.load() + S.writes_done.load();
  const bool inv_ops_balance = (S.ops_done.load() == ops_balance);
  const bool inv_rw_sum = (rw_sum == S.ops_done.load());
  const bool inv_ts_order = (S.ts_order_violations.load() == 0);
  const bool inv_persec_cohort = (per_sec_cohort_sum == (uint64_t)COH.ops);
  {
    FILE* f = fopen((g.out_prefix + ".detail_meta.json").c_str(), "w");
    if (f) {
      fprintf(f, "{\n  \"detail_sampling\": \"%s\",\n  \"detail_rate\": %.8f,\n"
                 "  \"perf_detail\": %d,\n  \"ring_capacity_rows\": %lld,\n"
                 "  \"rows_written\": %lld,\n  \"rows_dropped_ring_full\": %lld,\n"
                 "  \"rows_skipped_max_rows\": %lld,\n  \"queue_depth_max\": %lld,\n"
                 "  \"sampled_rows_requested\": %lld\n}\n",
              g.detail_sampling.c_str(), g.detail_rate, g.perf_detail ? 1 : 0,
              (long long)DETAIL.capacity(), (long long)DETAIL.written(), (long long)DETAIL.dropped(),
              (long long)S.detail_cap_hits.load(), (long long)DETAIL.max_depth(),
              (long long)S.detail_rows.load());
      fclose(f);
    }
  }
  {
    FILE* f = fopen((g.out_prefix + ".measure_phase.json").c_str(), "w");
    if (f) {
      fprintf(f, "{\n");
      fprintf(f, "  \"phase_order\": [\"open\", \"warmup\", \"measure\", \"drain\"],\n");
      fprintf(f, "  \"cohort_mode\": %d,\n", COH.active ? 1 : 0);
      fprintf(f, "  \"open_start\": %.6f,\n  \"open_end\": %.6f,\n", S.t_open_start.load(), S.t_open_end.load());
      fprintf(f, "  \"warmup_start\": %.6f,\n  \"warmup_end\": %.6f,\n", S.t_warmup_start.load(), S.t_warmup_end.load());
      fprintf(f, "  \"measure_start\": %.6f,\n  \"measure_end\": %.6f,\n  \"measure_window_s\": %.6f,\n"
                 "  \"measure_end_planned\": %.6f,\n"
                 "  \"throughput_window_s\": %.6f,\n  \"throughput_window_end_source\": \"%s\",\n",
              win_start, win_end, win_len, S.t_measure_end_planned.load(), win_len, win_end_src);
      fprintf(f, "  \"first_submit\": %.6f,\n  \"last_submit\": %.6f,\n", COH.first_submit, COH.last_submit);
      fprintf(f, "  \"first_engine_done\": %.6f,\n  \"last_engine_done\": %.6f,\n",
              COH.first_eng_done, COH.last_eng_done);
      fprintf(f, "  \"drain_start\": %.6f,\n  \"drain_end\": %.6f,\n  \"drain_seconds\": %.6f,\n",
              S.t_drain_start.load(), S.t_drain_end.load(),
              S.t_drain_end.load() - S.t_drain_start.load());
      fprintf(f, "  \"final_flush_start\": %.6f,\n  \"final_flush_end\": %.6f,\n",
              S.t_final_flush_start.load(), S.t_final_flush_end.load());
      fprintf(f, "  \"inflight_at_cutoff\": %lld,\n  \"censored\": %lld,\n",
              (long long)S.inflight_at_cutoff.load(), (long long)S.censored.load());
      fprintf(f, "  \"cohort_ops\": %lld,\n  \"cohort_ok\": %lld,\n  \"cohort_err\": %lld,\n"
                 "  \"cohort_reads\": %lld,\n  \"cohort_writes\": %lld,\n  \"cohort_notfound\": %lld,\n",
              (long long)COH.ops, (long long)COH.ok, (long long)COH.err, (long long)COH.reads,
              (long long)COH.writes, (long long)COH.notfound);
      fprintf(f, "  \"cohort_put\": %lld,\n  \"cohort_get_hit\": %lld,\n  \"cohort_get_miss\": %lld,\n"
                 "  \"cohort_scan\": %lld,\n",
              (long long)COH.cls[kClsPut], (long long)COH.cls[kClsGetHit],
              (long long)COH.cls[kClsGetMiss], (long long)COH.cls[kClsScan]);
      // Overload review (runbook §5 M1/M3): offered, admitted, completed and the
      // client-side queueing delay have to be visible together, or a saturated arm
      // cannot be told apart from a broken one.
      fprintf(f, "  \"offered_ops_s\": %.6f,\n  \"cohort_submitted\": %lld,\n"
                 "  \"cohort_completed\": %lld,\n  \"submitted_over_offered\": %.6f,\n",
              g.rate, (long long)S.m_submitted.load(), (long long)S.m_done.load(),
              (g.rate > 0 && win_len > 0)
                  ? (double)S.m_submitted.load() / (g.rate * win_len) : 0.0);
      if (COH.cli_wait.count) {
        fprintf(f, "  \"client_wait_p50_us\": %.3f,\n  \"client_wait_p99_us\": %.3f,\n"
                   "  \"client_wait_max_us\": %.3f,\n  \"client_wait_samples\": %llu,\n",
                COH.cli_wait.Pct(0.5) / 1000.0, COH.cli_wait.Pct(0.99) / 1000.0,
                (double)COH.cli_wait.mx / 1000.0, (unsigned long long)COH.cli_wait.count);
      } else {
        fprintf(f, "  \"client_wait_p50_us\": null,\n  \"client_wait_p99_us\": null,\n"
                   "  \"client_wait_max_us\": null,\n  \"client_wait_samples\": 0,\n");
      }
      fprintf(f, "  \"throughput_service_ops_s\": %.6f,\n", svc_throughput);
      fprintf(f, "  \"throughput_whole_run_ops_s\": %.6f,\n", whole_throughput);
      fprintf(f, "  \"per_sec_cohort_sum\": %llu,\n  \"per_sec_cohort_sum_ok\": %d,\n",
              (unsigned long long)per_sec_cohort_sum, inv_persec_cohort ? 1 : 0);
      fprintf(f, "  \"hist_version\": %d,\n  \"hist_encoding\": \"%s\",\n",
              g_hist_enc ? kHistVersionLegacy : kHistVersionV2,
              g_hist_enc ? kHistEncLegacy : kHistEncV2);
      // Raw clock values, so a reviewer can recompute the service throughput from the
      // clock the latencies were measured on rather than trusting this summary.
      // `*_observed` marks values the driver read from the clock directly; the warmup
      // boundary is the one derived quantity (anchor + warmup_s), because nothing
      // happens at that instant that the driver could sample.
      {
        const uint64_t n_warm = S.n_warmup_end.load();
        const uint64_t n_last = COH.n_last_submit;
        fprintf(f, "  \"monotonic_ns\": {\n");
        fprintf(f, "    \"clock\": \"steady_clock (CLOCK_MONOTONIC)\",\n");
        fprintf(f, "    \"anchor_mono\": %llu,\n    \"anchor_wall\": %.6f,\n",
                (unsigned long long)COH.n_anchor_mono, COH.n_anchor_wall);
        fprintf(f, "    \"t0\": %llu,\n", (unsigned long long)S.n_t0.load());
        fprintf(f, "    \"warmup_end\": %llu,\n    \"warmup_end_observed\": false,\n",
                (unsigned long long)n_warm);
        fprintf(f, "    \"cutoff\": %llu,\n    \"cutoff_observed\": true,\n",
                (unsigned long long)S.n_cutoff.load());
        fprintf(f, "    \"drain_end\": %llu,\n    \"drain_end_observed\": true,\n",
                (unsigned long long)S.n_drain_end.load());
        fprintf(f, "    \"first_submit\": %llu,\n    \"last_submit\": %llu,\n",
                (unsigned long long)COH.n_first_submit, (unsigned long long)n_last);
        fprintf(f, "    \"first_engine_done\": %llu,\n    \"last_engine_done\": %llu,\n",
                (unsigned long long)COH.n_first_eng_done,
                (unsigned long long)COH.n_last_eng_done);
        fprintf(f, "    \"cohort_admission_window_ns\": %lld,\n",
                (long long)((n_last > n_warm) ? (n_last - n_warm) : 0));
        fprintf(f, "    \"recomputed_service_ops_s\": %.6f\n",
                (n_last > n_warm) ? (double)S.m_done.load() * 1e9 / (double)(n_last - n_warm) : 0.0);
        fprintf(f, "  }\n");
      }
      fprintf(f, "}\n");
      fclose(f);
    }
  }
  {
    FILE* f = fopen((g.out_prefix + ".summary.json").c_str(), "w");
    if (f) {
      fprintf(f, "{\n");
      fprintf(f, "  \"mode\": \"%s\",\n  \"duration_s\": %.6f,\n", g.mode.c_str(), dur);
      fprintf(f, "  \"ops_started\": %lld,\n  \"ops_done\": %lld,\n  \"ops_ok\": %lld,\n  \"ops_err\": %lld,\n",
              (long long)S.ops_started.load(), (long long)S.ops_done.load(),
              (long long)S.ops_ok.load(), (long long)S.ops_err.load());
      fprintf(f, "  \"throughput_ops_s\": %.6f,\n", whole_throughput);
      fprintf(f, "  \"throughput_service_ops_s\": %.6f,\n", svc_throughput);
      fprintf(f, "  \"throughput_whole_run_ops_s\": %.6f,\n", whole_throughput);
      fprintf(f, "  \"measure_window_s\": %.6f,\n", win_len);
      fprintf(f, "  \"reads_issued\": %lld,\n  \"reads_hit\": %lld,\n  \"reads_notfound\": %lld,\n",
              (long long)S.reads_issued.load(), (long long)S.reads_hit.load(),
              (long long)S.reads_notfound.load());
      fprintf(f, "  \"reads_done\": %lld,\n  \"writes_done\": %lld,\n",
              (long long)S.reads_done.load(), (long long)S.writes_done.load());
      fprintf(f, "  \"cls_put\": %lld,\n  \"cls_get_hit\": %lld,\n  \"cls_get_miss\": %lld,\n"
                 "  \"cls_scan\": %lld,\n",
              (long long)S.c_cls[kClsPut].load(), (long long)S.c_cls[kClsGetHit].load(),
              (long long)S.c_cls[kClsGetMiss].load(), (long long)S.c_cls[kClsScan].load());
      fprintf(f, "  \"writes_ok\": %lld,\n  \"U_bytes\": %lld,\n  \"read_return_bytes\": %lld,\n",
              (long long)S.writes_ok.load(), (long long)S.bytes_written_logical.load(),
              (long long)S.bytes_read_returned.load());
      fprintf(f, "  \"flush_installed\": %lld,\n  \"compaction_installed\": %lld,\n",
              (long long)S.flush_installed.load(), (long long)S.compaction_installed.load());
      fprintf(f, "  \"stall_begin_events\": %lld,\n  \"stall_end_events\": %lld,\n",
              (long long)S.stall_begin_events.load(), (long long)S.stall_end_events.load());
      fprintf(f, "  \"device_write_bytes\": %llu,\n  \"device_read_bytes\": %llu,\n",
              (unsigned long long)(ws_ - g_dev_wr0) * 512ull,
              (unsigned long long)(rs - g_dev_rd0) * 512ull);
      fprintf(f, "  \"db_dir_total_bytes\": %llu,\n", (unsigned long long)DirSizeBytes(g.db_path));
      fprintf(f, "  \"killed_at_ops\": %lld,\n", (long long)S.killed_at_ops.load());
      // measurement-method identity + the invariants the acceptance tests check
      fprintf(f, "  \"hist_version\": %d,\n  \"hist_encoding\": \"%s\",\n",
              g_hist_enc ? kHistVersionLegacy : kHistVersionV2,
              g_hist_enc ? kHistEncLegacy : kHistEncV2);
      fprintf(f, "  \"mix_sampling\": \"%s\",\n  \"detail_sampling\": \"%s\",\n  \"detail_rate\": %.8f,\n",
              g.mix_sampling.c_str(), g.detail_sampling.c_str(), g.detail_rate);
      fprintf(f, "  \"detail_rows_written\": %lld,\n  \"detail_rows_dropped\": %lld,\n"
                 "  \"detail_queue_depth_max\": %lld,\n",
              (long long)DETAIL.written(), (long long)DETAIL.dropped(), (long long)DETAIL.max_depth());
      fprintf(f, "  \"inflight_at_cutoff\": %lld,\n  \"censored\": %lld,\n",
              (long long)S.inflight_at_cutoff.load(), (long long)S.censored.load());
      fprintf(f, "  \"invariant_ops_done_eq_ok_plus_err\": %d,\n", inv_ops_balance ? 1 : 0);
      fprintf(f, "  \"invariant_read_plus_write_eq_total\": %d,\n", inv_rw_sum ? 1 : 0);
      fprintf(f, "  \"invariant_timestamp_order\": %d,\n", inv_ts_order ? 1 : 0);
      fprintf(f, "  \"invariant_ts_order_violations\": %lld,\n", (long long)S.ts_order_violations.load());
      fprintf(f, "  \"invariant_per_sec_sum_eq_cohort\": %d,\n", inv_persec_cohort ? 1 : 0);
      fprintf(f, "  \"per_sec_cohort_sum\": %llu,\n", (unsigned long long)per_sec_cohort_sum);
      fprintf(f, "  \"cohort_ops\": %lld,\n  \"cohort_ok\": %lld,\n  \"cohort_err\": %lld\n",
              (long long)COH.ops, (long long)COH.ok, (long long)COH.err);
      fprintf(f, "}\n");
      fclose(f);
    }
  }
  fprintf(stderr, "DONE ops=%lld ok=%lld err=%lld dur=%.1fs thr=%.0f/s svc_thr=%.0f/s "
                  "U=%.2fGB devwrite=%.2fGB invariants=%d%d%d%d censored=%lld\n",
          (long long)S.ops_started.load(), (long long)S.ops_ok.load(), (long long)S.ops_err.load(),
          dur, whole_throughput, svc_throughput,
          S.bytes_written_logical.load() / 1073741824.0,
          (ws_ - g_dev_wr0) * 512.0 / 1073741824.0,
          inv_ops_balance ? 1 : 0, inv_rw_sum ? 1 : 0, inv_ts_order ? 1 : 0,
          inv_persec_cohort ? 1 : 0, (long long)S.censored.load());
  fflush(nullptr);
  // Last step: close the DB while every sink is still alive, so RocksDB's
  // background threads are joined here rather than racing global destruction.
  CloseDb();
  c_persec.Close(); c_samples.Close(); c_layout.Close();
  c_layout_levels.Close(); c_events.Close(); c_event_files.Close(); c_reads.Close();
  c_dirscan.Close(); c_stalls.Close(); c_compactions.Close(); c_summary_extra.Close();
  printf("done\n");
  return 0;
}
