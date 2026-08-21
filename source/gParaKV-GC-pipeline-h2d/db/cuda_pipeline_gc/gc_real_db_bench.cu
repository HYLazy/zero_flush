// gc_real_db_bench.cu — 端到端真实LevelDB环境的GC Pipeline性能测试
//
// 使用真实LevelDB实例: DB::Open → DB::Put → CompactRange → Mark() → GC
// 通过BitmapSnapshot在Serial GC和Pipeline GC之间保持bitmap状态一致
//
// Benchmarks: serial_gc, pipeline_gc, gc_comparison

#include "db/cuda/gpu_struct.cuh"
#include "db/cuda_pipeline_gc/gc_pipeline_vlog.h"
#include "db/db_impl.h"
#include "db/gpu_gc.h"
#include "db/my_stats.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cuda_runtime.h>
#include <dirent.h>
#include <fcntl.h>
#include <random>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "leveldb/db.h"

#include "util/histogram.h"

// ============================================================================
//  CLI Flags
// ============================================================================
static const char* FLAGS_benchmarks = "gc_comparison";
static int FLAGS_num = 100000;
static int FLAGS_value_size = 100;
static int FLAGS_num_vlogs = 4;
static double FLAGS_invalid_ratio = 0.3;
static int FLAGS_record_size = 128;
static int FLAGS_repeat = 1;
static const char* FLAGS_db = "/tmp/gc_e2e_bench";
static bool FLAGS_histogram = true;
static double FLAGS_overwrite_ratio = 1.0;

static void ParseFlags(int argc, char** argv) {
  for (int i = 1; i < argc; i++) {
    std::string arg(argv[i]);
    if (arg.substr(0, 13) == "--benchmarks=") {
      FLAGS_benchmarks = argv[i] + 13;
    } else if (arg.substr(0, 6) == "--num=") {
      FLAGS_num = std::atoi(arg.c_str() + 6);
    } else if (arg.substr(0, 13) == "--value_size=") {
      FLAGS_value_size = std::atoi(arg.c_str() + 13);
    } else if (arg.substr(0, 12) == "--num_vlogs=") {
      FLAGS_num_vlogs = std::atoi(arg.c_str() + 12);
    } else if (arg.substr(0, 16) == "--invalid_ratio=") {
      FLAGS_invalid_ratio = std::atof(arg.c_str() + 16);
    } else if (arg.substr(0, 14) == "--record_size=") {
      FLAGS_record_size = std::atoi(arg.c_str() + 14);
    } else if (arg.substr(0, 9) == "--repeat=") {
      FLAGS_repeat = std::atoi(arg.c_str() + 9);
    } else if (arg.substr(0, 5) == "--db=") {
      FLAGS_db = argv[i] + 5;
    } else if (arg.substr(0, 12) == "--histogram=") {
      FLAGS_histogram = (std::atoi(arg.c_str() + 12) != 0);
    } else if (arg.substr(0, 18) == "--overwrite_ratio=") {
      FLAGS_overwrite_ratio = std::atof(arg.c_str() + 18);
    }
  }
}

// ============================================================================
//  Utility
// ============================================================================
static double NowMicros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::high_resolution_clock::now().time_since_epoch())
      .count();
}

// ============================================================================
//  Stats — db_bench风格的统计信息类
// ============================================================================
class Stats {
 public:
  double start_;
  double finish_;
  double seconds_;
  int64_t done_;
  int64_t bytes_;
  double last_op_finish_;
  leveldb::Histogram hist_;
  std::string message_;
  double io_time_us_;
  double compute_time_us_;
  double transfer_time_us_;

  Stats() { Clear(); }

  void Clear() {
    hist_.Clear();
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = finish_ = last_op_finish_ = NowMicros();
    message_.clear();
    io_time_us_ = 0;
    compute_time_us_ = 0;
    transfer_time_us_ = 0;
  }

  void Start() {
    start_ = finish_ = last_op_finish_ = NowMicros();
    done_ = 0;
    bytes_ = 0;
    io_time_us_ = 0;
    compute_time_us_ = 0;
    transfer_time_us_ = 0;
  }

  void Stop() {
    finish_ = NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void FinishedSingleOp(int64_t ops = 1) {
    if (FLAGS_histogram) {
      double now = NowMicros();
      hist_.Add(now - last_op_finish_);
      last_op_finish_ = now;
    }
    done_ += ops;
  }

  void AddBytes(int64_t n) { bytes_ += n; }

  void AddMessage(const std::string& msg) {
    if (!message_.empty()) message_ += " ";
    message_ += msg;
  }

  void Report(const char* name) {
    if (done_ < 1) done_ = 1;
    double elapsed = (finish_ - start_) * 1e-6;
    if (elapsed < 1e-9) elapsed = 1e-9;

    std::string extra;
    if (bytes_ > 0) {
      char rate[100];
      std::snprintf(rate, sizeof(rate), "%6.1f MB/s",
                    (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    if (!message_.empty()) {
      if (!extra.empty()) extra += " ";
      extra += message_;
    }
    double throughput = static_cast<double>(done_) / elapsed;

    std::fprintf(stdout,
                 "%-16s : %11.3f micros/op %ld ops/sec %.3f seconds %ld"
                 " operations;%s%s\n",
                 name, seconds_ * 1e6 / static_cast<double>(done_),
                 static_cast<long>(throughput), elapsed,
                 static_cast<long>(done_), (extra.empty() ? "" : " "),
                 extra.c_str());

    if (io_time_us_ > 0 || compute_time_us_ > 0) {
      std::fprintf(stdout,
                   "  [I/O: %.3f ms | Compute: %.3f ms | Transfer: %.3f ms]\n",
                   io_time_us_ / 1000.0, compute_time_us_ / 1000.0,
                   transfer_time_us_ / 1000.0);
    }

    if (FLAGS_histogram) {
      std::fprintf(stdout, "Microseconds per op:\n%s\n",
                   hist_.ToString().c_str());
    }
    std::fflush(stdout);
  }
};

// ============================================================================
//  BitmapSnapshot — GPU bitmap 状态快照/恢复工具
// ============================================================================
struct BitmapSnapshot {
  std::vector<uint8_t> host_flags;
  std::vector<uint32_t> host_invalid_counts;

  void Capture(GPUGC* gc, uint32_t num_vlogs, uint32_t mli) {
    size_t flag_bytes = static_cast<size_t>(num_vlogs) * mli;
    host_flags.resize(flag_bytes);
    host_invalid_counts.resize(num_vlogs);

    CHECK(cudaMemcpy(host_flags.data(), gc->gpu_flags, flag_bytes,
                     cudaMemcpyDeviceToHost));
    CHECK(cudaMemcpy(host_invalid_counts.data(), gc->invalid_count,
                     num_vlogs * sizeof(uint32_t), cudaMemcpyDeviceToHost));
    CHECK(cudaStreamSynchronize(gc->stream));
  }

  void Restore(GPUGC* gc, uint32_t num_vlogs, uint32_t mli) {
    size_t flag_bytes = static_cast<size_t>(num_vlogs) * mli;
    CHECK(cudaMemcpy(gc->gpu_flags, host_flags.data(), flag_bytes,
                     cudaMemcpyHostToDevice));
    CHECK(cudaMemcpy(gc->invalid_count, host_invalid_counts.data(),
                     num_vlogs * sizeof(uint32_t), cudaMemcpyHostToDevice));
    CHECK(cudaStreamSynchronize(gc->stream));
  }
};

// ============================================================================
//  VLog Discovery — 扫描目录查找 .vlog 文件
// ============================================================================
struct VLogFileInfo {
  uint32_t vlog_num;
  std::string path;
  size_t file_size;
};

static std::vector<VLogFileInfo> DiscoverVLogFiles(const std::string& dir) {
  std::vector<VLogFileInfo> results;

  DIR* dp = opendir(dir.c_str());
  if (dp == nullptr) {
    std::fprintf(stderr, "Warning: cannot open directory %s\n", dir.c_str());
    return results;
  }

  struct dirent* entry;
  while ((entry = readdir(dp)) != nullptr) {
    std::string name(entry->d_name);
    // Match NNNNNN.vlog pattern
    if (name.size() >= 11 && name.substr(name.size() - 5) == ".vlog") {
      std::string num_str = name.substr(0, name.size() - 5);
      // Verify all digits
      bool all_digits = true;
      for (char c : num_str) {
        if (c < '0' || c > '9') {
          all_digits = false;
          break;
        }
      }
      if (all_digits) {
        uint32_t vlog_num = static_cast<uint32_t>(std::stoul(num_str));
        std::string path = dir + "/" + name;
        struct stat st;
        if (stat(path.c_str(), &st) == 0 && st.st_size > 0) {
          results.push_back({vlog_num, path, static_cast<size_t>(st.st_size)});
        }
      }
    }
  }
  closedir(dp);

  std::sort(results.begin(), results.end(),
            [](const VLogFileInfo& a, const VLogFileInfo& b) {
              return a.vlog_num < b.vlog_num;
            });
  return results;
}

// ============================================================================
//  RealDBGCBenchmark — 端到端真实DB环境的GC性能测试
// ============================================================================
class RealDBGCBenchmark {
 public:
  RealDBGCBenchmark() : db_(nullptr), impl_(nullptr), gpu_gc_(nullptr) {}

  ~RealDBGCBenchmark() { Cleanup(); }

  // Phase 1: 配置 my_stats 参数
  void Setup() {
    record_size_ = FLAGS_record_size;
    uint32_t var_kv_size = record_size_ - 12;
    leveldb::my_stats.var_key_value_size = var_kv_size;

    uint32_t log_item_size = record_size_;
    uint64_t total_data = static_cast<uint64_t>(FLAGS_num) * log_item_size;
    uint64_t target_vlog_bytes = total_data / FLAGS_num_vlogs;

    uint32_t max_items =
        static_cast<uint32_t>((target_vlog_bytes / log_item_size + 100) / 100) *
        100;
    if (max_items < 100) max_items = 100;

    leveldb::my_stats.max_num_log_item = max_items;
    leveldb::my_stats.max_num_log =
        static_cast<uint32_t>(FLAGS_num) / max_items + 2;
    leveldb::my_stats.max_vlog_size =
        static_cast<uint64_t>(max_items) * log_item_size;
    leveldb::my_stats.clean_threshold =
        static_cast<uint32_t>(std::ceil(max_items * FLAGS_invalid_ratio * 0.8));

    mli_ = max_items;
    max_vlog_count_ = leveldb::my_stats.max_num_log;

    std::fprintf(stdout, "=== E2E Real DB GC Pipeline Benchmark ===\n");
    std::fprintf(stdout, "Entries:           %d\n", FLAGS_num);
    std::fprintf(stdout, "Value size:        %d bytes\n", FLAGS_value_size);
    std::fprintf(stdout,
                 "Record size:       %d bytes (12 header + %d payload)\n",
                 record_size_, var_kv_size);
    std::fprintf(stdout, "Target VLogs:      %d\n", FLAGS_num_vlogs);
    std::fprintf(stdout, "Max items/VLog:    %u\n", mli_);
    std::fprintf(stdout, "Max VLog size:     %lu bytes\n",
                 leveldb::my_stats.max_vlog_size);
    std::fprintf(stdout, "Clean threshold:   %u\n",
                 leveldb::my_stats.clean_threshold);
    std::fprintf(stdout, "Invalid ratio:     %.1f%%\n",
                 FLAGS_invalid_ratio * 100.0);
    std::fprintf(stdout, "Overwrite ratio:   %.1f%%\n",
                 FLAGS_overwrite_ratio * 100.0);
    std::fprintf(stdout, "Repeat:            %d\n", FLAGS_repeat);
    std::fprintf(stdout, "DB path:           %s\n\n", FLAGS_db);
  }

  // Phase 2: 打开真实LevelDB数据库
  void OpenDatabase() {
    std::fprintf(stdout, "[Phase 2] Opening database...\n");
    leveldb::Options options;
    options.create_if_missing = true;
    options.write_buffer_size =
        2048ULL * 1048576ULL;  // 2GB: avoid memtable flush during writes

    leveldb::Status s = leveldb::DB::Open(options, FLAGS_db, FLAGS_db, &db_);
    if (!s.ok()) {
      std::fprintf(stderr, "DB::Open error: %s\n", s.ToString().c_str());
      std::exit(1);
    }

    impl_ = static_cast<leveldb::DBImpl*>(db_);
    gpu_gc_ = impl_->TEST_GetGPUGC();
    vlog_dir_ = impl_->TEST_GetVLogName();

    std::fprintf(stdout, "  DB opened, VLog dir: %s\n\n", vlog_dir_.c_str());
  }

  // Phase 3: 写入数据, 自动创建多个VLog文件
  void WriteData() {
    std::fprintf(stdout, "[Phase 3] Writing %d entries...\n", FLAGS_num);
    auto t0 = NowMicros();

    std::mt19937 rng(42);
    int errors = 0;
    char key_buf[32];

    leveldb::WriteOptions wo;
    for (int i = 0; i < FLAGS_num; i++) {
      std::snprintf(key_buf, sizeof(key_buf), "%016d", i);
      leveldb::Slice key(key_buf, 16);

      // Generate pseudo-random value
      std::string value(FLAGS_value_size, '\0');
      for (int j = 0; j < FLAGS_value_size; j++) {
        value[j] = static_cast<char>((rng() >> 8) & 0xFF);
      }

      leveldb::Status s = db_->Put(wo, key, value);
      if (!s.ok()) errors++;
    }

    auto t1 = NowMicros();
    double elapsed_ms = (t1 - t0) / 1000.0;

    std::fprintf(stdout, "  Write: %d entries in %.1f ms (%.1f entries/sec)",
                 FLAGS_num, elapsed_ms, FLAGS_num / (elapsed_ms / 1000.0));
    if (errors > 0) {
      std::fprintf(stdout, " [%d errors]", errors);
    }
    std::fprintf(stdout, "\n\n");
  }

  // Phase 4: 覆写部分key, 创建旧版本
  void OverwriteData() {
    int num_overwrite = static_cast<int>(FLAGS_num * FLAGS_overwrite_ratio);
    if (num_overwrite <= 0) {
      std::fprintf(stdout, "[Phase 4] Skipping overwrite (ratio=0)\n\n");
      return;
    }

    std::fprintf(stdout, "[Phase 4] Overwriting %d entries...\n",
                 num_overwrite);
    auto t0 = NowMicros();

    std::mt19937 rng(123);  // Different seed for different values
    int errors = 0;
    char key_buf[32];

    leveldb::WriteOptions wo;
    for (int i = 0; i < num_overwrite; i++) {
      std::snprintf(key_buf, sizeof(key_buf), "%016d", i);
      leveldb::Slice key(key_buf, 16);

      std::string value(FLAGS_value_size, '\0');
      for (int j = 0; j < FLAGS_value_size; j++) {
        value[j] = static_cast<char>((rng() >> 8) & 0xFF);
      }

      leveldb::Status s = db_->Put(wo, key, value);
      if (!s.ok()) errors++;
    }

    auto t1 = NowMicros();
    double elapsed_ms = (t1 - t0) / 1000.0;

    std::fprintf(
        stdout, "  Overwrite: %d entries in %.1f ms (%.1f entries/sec)",
        num_overwrite, elapsed_ms, num_overwrite / (elapsed_ms / 1000.0));
    if (errors > 0) std::fprintf(stdout, " [%d errors]", errors);
    std::fprintf(stdout, "\n\n");
  }

  // Phase 5: Flush memtable (CPU-only, avoids GPU compaction path)
  void ForceCompaction() {
    std::fprintf(stdout, "[Phase 5] Flushing memtable (CPU path)...\n");
    auto t0 = NowMicros();

    // Use TEST_CompactMemTable to flush without triggering GPU L0->L1
    // compaction (GPU DecodeSSTables has compatibility issues with CPU-built
    // SSTables)
    impl_->TEST_CompactMemTable();

    auto t1 = NowMicros();
    double elapsed_ms = (t1 - t0) / 1000.0;
    std::fprintf(stdout, "  Memtable flush completed in %.1f ms\n", elapsed_ms);
    std::fprintf(stdout,
                 "  Note: Using bitmap injection for Mark() (GPU compaction "
                 "path skipped)\n\n");
  }

  // Phase 6: 等待后台工作完成
  void WaitForQuiescence() {
    std::fprintf(stdout, "[Phase 6] Waiting for background work...\n");
    // Sleep in 100ms intervals, max 30s
    for (int i = 0; i < 10; i++) {
      usleep(100000);  // 100ms
    }
    std::fprintf(stdout, "  Waited 1 second for quiescence\n\n");
  }

  // Phase 7: 填充bitmap (VLog文件来自真实DB, bitmap通过注入设置)
  void CheckAndMaybeInject() {
    std::fprintf(stdout, "[Phase 7] Setting up bitmap state...\n");

    // Copy invalid_count to host
    std::vector<uint32_t> host_counts(max_vlog_count_);
    CHECK(cudaMemcpy(host_counts.data(), gpu_gc_->invalid_count,
                     max_vlog_count_ * sizeof(uint32_t),
                     cudaMemcpyDeviceToHost));
    CHECK(cudaStreamSynchronize(gpu_gc_->stream));

    uint32_t threshold = leveldb::my_stats.clean_threshold;
    int triggered_count = 0;
    for (uint32_t v = 0; v < max_vlog_count_; v++) {
      if (host_counts[v] >= threshold) triggered_count++;
    }

    std::fprintf(stdout, "  VLogs with natural Mark: %d (target: %d)\n",
                 triggered_count, FLAGS_num_vlogs);

    // Always inject bitmap entries for reliable benchmarking
    // (GPU compaction DecodeSSTables may not work with CPU-built SSTables)
    {
      std::fprintf(stdout, "  [INJECT] Setting up bitmap for %d VLogs\n",
                   FLAGS_num_vlogs);

      // Copy gpu_flags to host for inspection
      size_t total_flags = static_cast<size_t>(max_vlog_count_) * mli_;
      std::vector<uint8_t> host_flags(total_flags);
      CHECK(cudaMemcpy(host_flags.data(), gpu_gc_->gpu_flags, total_flags,
                       cudaMemcpyDeviceToHost));

      // Find VLogs below threshold and inject invalid entries
      int needed = FLAGS_num_vlogs - triggered_count;
      for (uint32_t v = 0; v < max_vlog_count_ && needed > 0; v++) {
        if (host_counts[v] >= threshold) continue;

        // Inject enough invalid entries to reach threshold
        uint32_t to_inject = threshold - host_counts[v] + 10;
        uint32_t injected = 0;
        for (uint32_t e = 0; e < mli_ && injected < to_inject; e++) {
          size_t idx = static_cast<size_t>(v) * mli_ + e;
          if (host_flags[idx] == 1) {
            host_flags[idx] = 0;
            injected++;
          }
        }
        host_counts[v] += injected;
        needed--;
        std::fprintf(stdout,
                     "    VLog %u: injected %u invalid entries (total: %u)\n",
                     v + 1, injected, host_counts[v]);
      }

      // Upload modified bitmap back to GPU
      CHECK(cudaMemcpy(gpu_gc_->gpu_flags, host_flags.data(), total_flags,
                       cudaMemcpyHostToDevice));
      CHECK(cudaMemcpy(gpu_gc_->invalid_count, host_counts.data(),
                       max_vlog_count_ * sizeof(uint32_t),
                       cudaMemcpyHostToDevice));
      CHECK(cudaStreamSynchronize(gpu_gc_->stream));
    }
    std::fprintf(stdout, "\n");
  }

  // Phase 8: 快照bitmap状态
  void SnapshotBitmap() {
    std::fprintf(stdout, "[Phase 8] Snapshotting bitmap state...\n");
    snapshot_.Capture(gpu_gc_, max_vlog_count_, mli_);
    std::fprintf(stdout, "  Bitmap captured: %u VLogs x %u entries\n\n",
                 max_vlog_count_, mli_);
  }

  // Phase 9: 发现VLog文件
  void DiscoverVLogs() {
    std::fprintf(stdout, "[Phase 9] Discovering VLog files...\n");

    auto all_files = DiscoverVLogFiles(vlog_dir_);
    std::fprintf(stdout, "  Found %zu VLog files on disk\n", all_files.size());

    // Copy invalid_count for filtering
    std::vector<uint32_t> host_counts(max_vlog_count_);
    CHECK(cudaMemcpy(host_counts.data(), gpu_gc_->invalid_count,
                     max_vlog_count_ * sizeof(uint32_t),
                     cudaMemcpyDeviceToHost));

    uint32_t threshold = leveldb::my_stats.clean_threshold;
    vlog_candidates_.clear();

    for (const auto& vf : all_files) {
      // VLog numbers are 1-indexed in GPUGC, array is 0-indexed
      if (vf.vlog_num > 0 && vf.vlog_num <= max_vlog_count_) {
        uint32_t inv_count = host_counts[vf.vlog_num - 1];
        if (inv_count >= threshold) {
          vlog_candidates_.push_back(vf);
          std::fprintf(stdout, "    VLog %u: %zu bytes, invalid_count=%u\n",
                       vf.vlog_num, vf.file_size, inv_count);
        }
      }
      if (static_cast<int>(vlog_candidates_.size()) >= FLAGS_num_vlogs) break;
    }

    std::fprintf(stdout,
                 "  Selected %zu VLog candidates for GC benchmarking\n\n",
                 vlog_candidates_.size());

    if (vlog_candidates_.empty()) {
      std::fprintf(stderr,
                   "ERROR: No VLog candidates found. "
                   "Try increasing --num or --invalid_ratio.\n");
      std::exit(1);
    }
  }

  // Phase 10: 运行基准测试
  void Run() {
    std::string bm = FLAGS_benchmarks;
    const char* ptr = bm.c_str();

    while (ptr != nullptr) {
      const char* comma = strchr(ptr, ',');
      std::string name;
      if (comma != nullptr) {
        name = std::string(ptr, comma - ptr);
        ptr = comma + 1;
      } else {
        name = std::string(ptr);
        ptr = nullptr;
      }

      if (name == "serial_gc") {
        BenchSerialGC();
      } else if (name == "pipeline_gc") {
        BenchPipelineGC();
      } else if (name == "gc_comparison") {
        BenchComparison();
      } else {
        std::fprintf(stderr, "Unknown benchmark: %s\n", name.c_str());
      }
    }
  }

  // Phase 13: 清理
  void Cleanup() {
    if (db_ != nullptr) {
      delete db_;
      db_ = nullptr;
      impl_ = nullptr;
      gpu_gc_ = nullptr;

      // DestroyDB removes SSTables, MANIFEST, LOCK, etc.
      leveldb::Options options;
      leveldb::DestroyDB(FLAGS_db, options);

      // Remove remaining .vlog files (DestroyDB doesn't handle them)
      auto vlogs = DiscoverVLogFiles(FLAGS_db);
      for (const auto& vf : vlogs) {
        unlink(vf.path.c_str());
      }
      rmdir(FLAGS_db);

      std::fprintf(stdout, "  Cleanup completed\n");
    }
  }

 private:
  // Phase 10a: Serial GC benchmark
  void BenchSerialGC() {
    std::fprintf(stdout, "--- Benchmark: serial_gc ---\n");
    Stats stats;
    int nv = static_cast<int>(vlog_candidates_.size());

    for (int r = 0; r < FLAGS_repeat; r++) {
      // Restore bitmap to captured state
      snapshot_.Restore(gpu_gc_, max_vlog_count_, mli_);

      // Reset transfer time counter
      leveldb::my_stats.data_transfer_time = 0;

      stats.Start();
      for (int v = 0; v < nv; v++) {
        const auto& vc = vlog_candidates_[v];

        // Set triggered VLog
        gpu_gc_->triggered_vlog_num = vc.vlog_num;

        // Read VLog file from disk (with page cache invalidation for fair
        // comparison)
        FILE* f = fopen(vc.path.c_str(), "rb");
        if (f == nullptr) {
          std::fprintf(stderr, "  Warning: cannot open %s, skipping\n",
                       vc.path.c_str());
          continue;
        }
        // Drop page cache so fread hits real disk I/O (fair vs GDS O_DIRECT)
        posix_fadvise(fileno(f), 0, 0, POSIX_FADV_DONTNEED);
        std::vector<char> vlog_data(vc.file_size);
        auto rd_start = NowMicros();
        size_t nread = fread(vlog_data.data(), 1, vc.file_size, f);
        auto rd_end = NowMicros();
        fclose(f);
        stats.io_time_us_ += (rd_end - rd_start) / 1000.0;  // fread disk I/O

        if (nread != vc.file_size) {
          std::fprintf(stderr, "  Warning: short read on %s\n",
                       vc.path.c_str());
          continue;
        }

        // Run GC
        char* output = nullptr;
        gpu_gc_->BeginGPUGCOptimized(vlog_data.data(), vc.file_size, &output);

        stats.AddBytes(vc.file_size);
        stats.FinishedSingleOp(1);

        delete[] output;
        gpu_gc_->CleanGC();
      }
      stats.Stop();

      stats.transfer_time_us_ += leveldb::my_stats.data_transfer_time / 1000.0;
    }

    stats.AddMessage(std::to_string(nv) + " VLogs x " +
                     std::to_string(FLAGS_repeat) + " rounds");
    stats.Report("serial_gc");
    std::fprintf(stdout, "\n");
  }

  // Phase 11: Pipeline GC benchmark
  void BenchPipelineGC() {
    std::fprintf(stdout, "--- Benchmark: pipeline_gc ---\n");
    Stats stats;
    int nv = static_cast<int>(vlog_candidates_.size());

    leveldb::GCVLogPipeline::InitGDS();

    for (int r = 0; r < FLAGS_repeat; r++) {
      // Restore bitmap
      snapshot_.Restore(gpu_gc_, max_vlog_count_, mli_);

      stats.Start();

      leveldb::GCVLogPipeline pipeline(gpu_gc_, nv);

      // Add all VLog candidates
      for (int v = 0; v < nv; v++) {
        const auto& vc = vlog_candidates_[v];
        pipeline.AddVLog(vc.vlog_num, vc.path, vc.file_size);
        stats.AddBytes(vc.file_size);
      }

      // Run pipeline
      cudaError_t err = pipeline.RunAsync();
      if (err != cudaSuccess) {
        std::fprintf(stderr, "  Pipeline RunAsync error: %s\n",
                     cudaGetErrorString(err));
        pipeline.Destroy();
        stats.Stop();
        continue;
      }

      err = pipeline.Synchronize();
      if (err != cudaSuccess) {
        std::fprintf(stderr, "  Pipeline Synchronize error: %s\n",
                     cudaGetErrorString(err));
      }

      stats.Stop();

      // Collect stats
      const auto& gs = pipeline.GetStats();
      stats.io_time_us_ += gs.io_time_us / 1000.0;
      stats.compute_time_us_ += gs.compute_time_us / 1000.0;

      stats.FinishedSingleOp(nv);

      pipeline.BatchCleanGC();
      pipeline.Destroy();
    }

    leveldb::GCVLogPipeline::DestroyGDS();

    stats.AddMessage(std::to_string(nv) + " VLogs x " +
                     std::to_string(FLAGS_repeat) + " rounds");
    stats.Report("pipeline_gc");
    std::fprintf(stdout, "\n");
  }

  // Phase 12: Comparison benchmark
  void BenchComparison() {
    std::fprintf(stdout, "--- Benchmark: gc_comparison ---\n\n");
    int nv = static_cast<int>(vlog_candidates_.size());

    // Accumulate over repeat rounds
    double sa_total = 0, sa_io = 0, sa_compute = 0, sa_bytes = 0;
    double pa_total = 0, pa_io = 0, pa_compute = 0, pa_bytes = 0;

    leveldb::GCVLogPipeline::InitGDS();

    for (int r = 0; r < FLAGS_repeat; r++) {
      std::fprintf(stdout, "--- Round %d/%d ---\n", r + 1, FLAGS_repeat);

      // === Serial GC ===
      {
        snapshot_.Restore(gpu_gc_, max_vlog_count_, mli_);
        leveldb::my_stats.data_transfer_time = 0;

        double t0 = NowMicros();
        double fread_time_us = 0;
        for (int v = 0; v < nv; v++) {
          const auto& vc = vlog_candidates_[v];
          gpu_gc_->triggered_vlog_num = vc.vlog_num;

          FILE* f = fopen(vc.path.c_str(), "rb");
          if (!f) continue;
          // Drop page cache so fread hits real disk I/O (fair vs GDS O_DIRECT)
          posix_fadvise(fileno(f), 0, 0, POSIX_FADV_DONTNEED);
          std::vector<char> vd(vc.file_size);
          auto rd_start = NowMicros();
          size_t nr = fread(vd.data(), 1, vc.file_size, f);
          (void)nr;
          auto rd_end = NowMicros();
          fread_time_us += (rd_end - rd_start);
          fclose(f);

          char* output = nullptr;
          gpu_gc_->BeginGPUGCOptimized(vd.data(), vc.file_size, &output);
          sa_bytes += vc.file_size;
          delete[] output;
          gpu_gc_->CleanGC();
        }
        double t1 = NowMicros();

        sa_total += (t1 - t0) / 1000.0;
        sa_io += (leveldb::my_stats.data_transfer_time + fread_time_us) / 1000.0;
        sa_compute += ((t1 - t0) - leveldb::my_stats.data_transfer_time - fread_time_us) / 1000.0;
      }

      // === Pipeline GC ===
      {
        snapshot_.Restore(gpu_gc_, max_vlog_count_, mli_);

        double t0 = NowMicros();
        leveldb::GCVLogPipeline pipeline(gpu_gc_, nv);
        for (int v = 0; v < nv; v++) {
          const auto& vc = vlog_candidates_[v];
          pipeline.AddVLog(vc.vlog_num, vc.path, vc.file_size);
          pa_bytes += vc.file_size;
        }
        pipeline.RunAsync();
        pipeline.Synchronize();
        double t1 = NowMicros();

        const auto& gs = pipeline.GetStats();
        pa_total += (t1 - t0) / 1000.0;
        pa_io += gs.io_time_us / 1000.0;
        pa_compute += gs.compute_time_us / 1000.0;

        pipeline.BatchCleanGC();
        pipeline.Destroy();
      }
    }

    leveldb::GCVLogPipeline::DestroyGDS();

    // === Report ===
    int n = FLAGS_repeat;
    double s_avg = sa_total / n;
    double p_avg = pa_total / n;
    double s_io = sa_io / n;
    double p_io = pa_io / n;
    double s_compute = sa_compute / n;
    double p_compute = pa_compute / n;
    double s_bytes = sa_bytes / n;
    double p_bytes = pa_bytes / n;

    double s_tp = (s_bytes / 1048576.0) / (s_avg / 1000.0);
    double p_tp = (p_bytes / 1048576.0) / (p_avg / 1000.0);
    double speedup = (p_avg > 0) ? s_avg / p_avg : 0;

    std::fprintf(stdout,
                 "\n"
                 "=== GC Comparison Results (%d VLogs, %d rounds) ===\n"
                 "Invalid ratio: %.1f%% | Record size: %d bytes\n\n",
                 nv, n, FLAGS_invalid_ratio * 100.0, record_size_);

    std::fprintf(stdout, "%-20s %12s %12s %12s %12s\n", "Method", "Total(ms)",
                 "I/O(ms)", "Compute(ms)", "MB/s");
    std::fprintf(stdout, "%-20s %12s %12s %12s %12s\n", "--------------------",
                 "------------", "------------", "------------",
                 "------------");
    std::fprintf(stdout, "%-20s %12.3f %12.3f %12.3f %12.1f\n", "Serial GC",
                 s_avg, s_io, s_compute, s_tp);
    std::fprintf(stdout, "%-20s %12.3f %12.3f %12.3f %12.1f\n", "Pipeline GC",
                 p_avg, p_io, p_compute, p_tp);

    std::fprintf(stdout, "\nSpeedup: %.2fx | Throughput gain: %.2fx\n", speedup,
                 p_tp / s_tp);
    std::fflush(stdout);
  }

  // Members
  leveldb::DB* db_;
  leveldb::DBImpl* impl_;
  GPUGC* gpu_gc_;
  std::string vlog_dir_;
  uint32_t mli_;
  uint32_t max_vlog_count_;
  int record_size_;

  BitmapSnapshot snapshot_;
  std::vector<VLogFileInfo> vlog_candidates_;
};

// ============================================================================
//  main
// ============================================================================
int main(int argc, char** argv) {
  ParseFlags(argc, argv);

  // Initialize CUDA
  cudaFree(nullptr);

  // Create and run benchmark
  RealDBGCBenchmark bench;
  bench.Setup();
  bench.OpenDatabase();
  bench.WriteData();
  bench.OverwriteData();
  bench.ForceCompaction();
  bench.WaitForQuiescence();
  bench.CheckAndMaybeInject();
  bench.SnapshotBitmap();
  bench.DiscoverVLogs();
  bench.Run();

  std::fprintf(stdout, "\nDone.\n");
  return 0;
}
