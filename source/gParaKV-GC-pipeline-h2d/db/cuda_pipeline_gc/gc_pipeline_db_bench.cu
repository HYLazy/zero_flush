//
// [文件说明] GC Pipeline DB Benchmark — db_bench风格的GC流水线性能对比
// [新增] gParaKV-GC新增。模拟数据库GC工作负载, 对比两种GC路径:
//   serial_gc:    串行GC — 逐个VLog调用BeginGPUGCOptimized (现有方式)
//   pipeline_gc:  流水线GC — GCVLogPipeline多流并行 + GDS零拷贝
//   gc_comparison: 同时运行两者并输出详细对比报告
//
// 工作负载模拟:
//   1. 生成VLog文件 (模拟compaction后的VLog数据)
//   2. 初始化GPUGC bitmap (模拟Mark阶段标记的无效条目)
//   3. 运行GC并收集细粒度计时数据
//   4. 输出db_bench风格的性能报告
//
// 使用示例:
//   gc_pipeline_db_bench --benchmarks=gc_comparison
//   gc_pipeline_db_bench --benchmarks=serial_gc,pipeline_gc --num_vlogs=8
//   gc_pipeline_db_bench --benchmarks=gc_comparison --entries=50000 --repeat=3
//

#include "db/cuda_pipeline_gc/gc_pipeline_vlog.h"
#include "db/gpu_gc.h"
#include "db/my_stats.h"
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cuda_runtime.h>
#include <fcntl.h>
#include <random>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "util/histogram.h"

// ============================================================================
//  命令行参数 (db_bench风格)
// ============================================================================
static const char* FLAGS_benchmarks = "gc_comparison";
static int FLAGS_entries = 20000;
static int FLAGS_num_vlogs = 6;
static double FLAGS_invalid_ratio = 0.5;
static int FLAGS_record_size = 128;
static int FLAGS_repeat = 1;
static bool FLAGS_histogram = true;
static const char* FLAGS_db = "/tmp/gc_pipeline_db_bench";

// ============================================================================
//  命令行解析
// ============================================================================
static void ParseCommandLine(int argc, char** argv) {
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg.find("--benchmarks=") == 0) {
      FLAGS_benchmarks = argv[i] + 13;
    } else if (arg.find("--entries=") == 0) {
      FLAGS_entries = std::atoi(arg.c_str() + 10);
    } else if (arg.find("--num_vlogs=") == 0) {
      FLAGS_num_vlogs = std::atoi(arg.c_str() + 12);
    } else if (arg.find("--invalid_ratio=") == 0) {
      FLAGS_invalid_ratio = std::atof(arg.c_str() + 16);
    } else if (arg.find("--record_size=") == 0) {
      FLAGS_record_size = std::atoi(arg.c_str() + 14);
    } else if (arg.find("--repeat=") == 0) {
      FLAGS_repeat = std::atoi(arg.c_str() + 9);
    } else if (arg.find("--histogram=") == 0) {
      FLAGS_histogram = (std::atoi(arg.c_str() + 12) != 0);
    } else if (arg.find("--db=") == 0) {
      FLAGS_db = argv[i] + 5;
    } else if (arg == "--help") {
      std::printf(
          "Usage: gc_pipeline_db_bench [options]\n"
          "  --benchmarks=LIST    serial_gc,pipeline_gc,gc_comparison\n"
          "  --entries=N          Entries per VLog (default: 20000)\n"
          "  --num_vlogs=N        Number of VLogs (default: 6)\n"
          "  --invalid_ratio=F    Invalid ratio 0.0-1.0 (default: 0.5)\n"
          "  --record_size=N      Record bytes (default: 128)\n"
          "  --repeat=N           Repeat count (default: 1)\n"
          "  --histogram=N        Print histogram 0/1 (default: 1)\n"
          "  --db=PATH            Temp directory\n");
      std::exit(0);
    }
  }
}

// ============================================================================
//  计时工具
// ============================================================================
static double NowMicros() {
  auto now = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::microseconds>(
             now.time_since_epoch())
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
//  VLog数据生成器
// ============================================================================
static bool GenerateVLogFile(const std::string& path, uint32_t vlog_num,
                             int entries, int record_size) {
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return false;

  std::vector<char> buf(record_size, 0);
  for (int e = 0; e < entries; e++) {
    uint32_t h1 = vlog_num;
    uint32_t h2 = static_cast<uint32_t>(e * record_size + 12);
    std::memcpy(buf.data(), &h1, 4);
    std::memcpy(buf.data() + 4, &h2, 4);
    for (int i = 12; i < record_size; i++)
      buf[i] = static_cast<char>((e * 7 + i) & 0xFF);

    if (write(fd, buf.data(), record_size) != record_size) {
      close(fd);
      return false;
    }
  }
  close(fd);
  return true;
}

// ============================================================================
//  GPUGC Bitmap初始化
// ============================================================================
static void SetupGPUGCBitmap(GPUGC& gpu_gc, int num_vlogs, int entries,
                             double invalid_ratio,
                             std::vector<uint32_t>& out_counts) {
  uint32_t mli = gpu_gc.max_num_log_item;
  std::vector<uint8_t> hf(static_cast<size_t>(num_vlogs) * mli, 1);
  out_counts.resize(num_vlogs, 0);

  std::mt19937 rng(42);
  std::uniform_real_distribution<double> dist(0.0, 1.0);

  for (int v = 0; v < num_vlogs; v++)
    for (int e = 0; e < entries; e++)
      if (dist(rng) < invalid_ratio) {
        hf[v * mli + e] = 0;
        out_counts[v]++;
      }

  for (int v = 0; v < num_vlogs; v++) {
    CHECK(cudaMemcpy(gpu_gc.gpu_flags + v * mli, hf.data() + v * mli, mli,
                     cudaMemcpyHostToDevice));
    CHECK(cudaMemcpy(&gpu_gc.invalid_count[v], &out_counts[v], sizeof(uint32_t),
                     cudaMemcpyHostToDevice));
  }
  CHECK(cudaStreamSynchronize(gpu_gc.stream));
}

// ============================================================================
//  GCPipelineDBBenchmark — 主基准测试类
// ============================================================================
class GCPipelineDBBenchmark {
 public:
  GCPipelineDBBenchmark()
      : entries_(FLAGS_entries),
        num_vlogs_(FLAGS_num_vlogs),
        record_size_(FLAGS_record_size) {
    mkdir(FLAGS_db, 0755);
    vlog_paths_.resize(num_vlogs_);
    vlog_sizes_.resize(num_vlogs_);
    for (int v = 0; v < num_vlogs_; v++) {
      char buf[512];
      std::snprintf(buf, sizeof(buf), "%s/%06d.vlog", FLAGS_db, v + 1);
      vlog_paths_[v] = buf;
      vlog_sizes_[v] = static_cast<size_t>(entries_) * record_size_;
    }
  }

  ~GCPipelineDBBenchmark() {
    for (const auto& p : vlog_paths_) unlink(p.c_str());
    rmdir(FLAGS_db);
  }

  void Run() {
    std::fprintf(stdout, "=== GC Pipeline DB Benchmark ===\n");
    std::fprintf(stdout, "VLogs:           %d\n", num_vlogs_);
    std::fprintf(stdout, "Entries/VLog:    %d\n", entries_);
    std::fprintf(stdout, "Record size:     %d bytes\n", record_size_);
    std::fprintf(stdout, "Invalid ratio:   %.1f%%\n",
                 FLAGS_invalid_ratio * 100.0);
    std::fprintf(
        stdout, "Total data:      %.1f MB\n",
        static_cast<double>(num_vlogs_) * entries_ * record_size_ / 1048576.0);
    std::fprintf(stdout, "Repeat:          %d\n", FLAGS_repeat);
    std::fprintf(stdout, "DB path:         %s\n\n", FLAGS_db);

    std::fprintf(stdout, "Generating VLog files...\n");
    for (int v = 0; v < num_vlogs_; v++)
      if (!GenerateVLogFile(vlog_paths_[v], v + 1, entries_, record_size_)) {
        std::fprintf(stderr, "Failed to generate VLog %d\n", v + 1);
        return;
      }
    std::fprintf(stdout, "VLog files generated.\n\n");

    std::string bm = FLAGS_benchmarks;
    const char* ptr = bm.c_str();
    while (ptr) {
      const char* comma = strchr(ptr, ',');
      std::string name;
      if (comma) {
        name = std::string(ptr, comma - ptr);
        ptr = comma + 1;
      } else {
        name = ptr;
        ptr = nullptr;
      }
      while (!name.empty() && name[0] == ' ') name.erase(name.begin());

      if (name == "serial_gc")
        BenchSerialGC();
      else if (name == "pipeline_gc")
        BenchPipelineGC();
      else if (name == "gc_comparison")
        BenchComparison();
      else
        std::fprintf(stderr, "Unknown benchmark: %s\n", name.c_str());
    }
  }

 private:
  void BenchSerialGC() {
    std::fprintf(
        stdout,
        "------------------------------------------------------------\n");
    std::fprintf(stdout, "Benchmark: serial_gc\n");
    std::fprintf(
        stdout,
        "------------------------------------------------------------\n");

    Stats stats;
    stats.Start();

    for (int r = 0; r < FLAGS_repeat; r++) {
      GPUGC gpu_gc(num_vlogs_, entries_);
      gpu_gc.MallocMemory();
      leveldb::my_stats.data_transfer_time = 0;

      std::vector<uint32_t> ic;
      SetupGPUGCBitmap(gpu_gc, num_vlogs_, entries_, FLAGS_invalid_ratio, ic);

      for (int v = 0; v < num_vlogs_; v++) {
        gpu_gc.triggered_vlog_num = v + 1;

        FILE* f = fopen(vlog_paths_[v].c_str(), "rb");
        if (!f) continue;
        fseek(f, 0, SEEK_END);
        size_t fs = ftell(f);
        fseek(f, 0, SEEK_SET);
        std::vector<char> vd(fs);
        fread(vd.data(), 1, fs, f);
        fclose(f);

        stats.AddBytes(fs);
        char* output = nullptr;
        gpu_gc.BeginGPUGCOptimized(vd.data(), fs, &output);
        stats.FinishedSingleOp(1);
        delete[] output;
        gpu_gc.CleanGC();
      }
      stats.transfer_time_us_ += leveldb::my_stats.data_transfer_time;
    }

    stats.Stop();
    stats.AddMessage(std::to_string(num_vlogs_) + " VLogs x " +
                     std::to_string(FLAGS_repeat) + " rounds");
    stats.Report("serial_gc");
    std::fprintf(stdout, "\n");
  }

  void BenchPipelineGC() {
    std::fprintf(
        stdout,
        "------------------------------------------------------------\n");
    std::fprintf(stdout, "Benchmark: pipeline_gc\n");
    std::fprintf(
        stdout,
        "------------------------------------------------------------\n");

    leveldb::GCVLogPipeline::InitGDS();

    Stats stats;
    stats.Start();

    for (int r = 0; r < FLAGS_repeat; r++) {
      GPUGC gpu_gc(num_vlogs_, entries_);
      gpu_gc.MallocMemory();
      std::vector<uint32_t> ic;
      SetupGPUGCBitmap(gpu_gc, num_vlogs_, entries_, FLAGS_invalid_ratio, ic);

      leveldb::GCVLogPipeline pipeline(&gpu_gc, num_vlogs_);
      for (int v = 0; v < num_vlogs_; v++) {
        pipeline.AddVLog(v + 1, vlog_paths_[v], vlog_sizes_[v]);
        stats.AddBytes(vlog_sizes_[v]);
      }

      pipeline.RunAsync();
      pipeline.Synchronize();

      const auto& gs = pipeline.GetStats();
      stats.io_time_us_ += gs.io_time_us;
      stats.compute_time_us_ += gs.compute_time_us;
      stats.FinishedSingleOp(num_vlogs_);

      pipeline.BatchCleanGC();
      pipeline.Destroy();
    }

    stats.Stop();
    stats.AddMessage(std::to_string(num_vlogs_) + " VLogs x " +
                     std::to_string(FLAGS_repeat) + " rounds");
    stats.Report("pipeline_gc");
    std::fprintf(stdout, "\n");

    leveldb::GCVLogPipeline::DestroyGDS();
  }

  void BenchComparison() {
    std::fprintf(
        stdout,
        "============================================================\n");
    std::fprintf(stdout,
                 " Benchmark: gc_comparison (Serial GC vs Pipeline GC)\n");
    std::fprintf(
        stdout,
        "============================================================\n\n");

    leveldb::GCVLogPipeline::InitGDS();

    struct Acc {
      double total_ms = 0, io_ms = 0, compute_ms = 0, transfer_ms = 0;
      size_t bytes = 0;
    };
    Acc sa, pa;

    for (int r = 0; r < FLAGS_repeat; r++) {
      std::fprintf(stdout, "--- Round %d/%d ---\n", r + 1, FLAGS_repeat);

      // === Serial ===
      {
        GPUGC gc(num_vlogs_, entries_);
        gc.MallocMemory();
        leveldb::my_stats.data_transfer_time = 0;
        std::vector<uint32_t> ic;
        SetupGPUGCBitmap(gc, num_vlogs_, entries_, FLAGS_invalid_ratio, ic);

        double t0 = NowMicros();
        for (int v = 0; v < num_vlogs_; v++) {
          gc.triggered_vlog_num = v + 1;
          FILE* f = fopen(vlog_paths_[v].c_str(), "rb");
          if (!f) continue;
          fseek(f, 0, SEEK_END);
          size_t fs = ftell(f);
          fseek(f, 0, SEEK_SET);
          std::vector<char> vd(fs);
          fread(vd.data(), 1, fs, f);
          fclose(f);
          char* out = nullptr;
          gc.BeginGPUGCOptimized(vd.data(), fs, &out);
          sa.bytes += fs;
          delete[] out;
          gc.CleanGC();
        }
        double t1 = NowMicros();
        sa.total_ms += (t1 - t0) / 1000.0;
        sa.transfer_ms += leveldb::my_stats.data_transfer_time / 1000.0;
      }

      // === Pipeline ===
      {
        GPUGC gc(num_vlogs_, entries_);
        gc.MallocMemory();
        std::vector<uint32_t> ic;
        SetupGPUGCBitmap(gc, num_vlogs_, entries_, FLAGS_invalid_ratio, ic);

        leveldb::GCVLogPipeline pl(&gc, num_vlogs_);
        for (int v = 0; v < num_vlogs_; v++)
          pl.AddVLog(v + 1, vlog_paths_[v], vlog_sizes_[v]);

        double t0 = NowMicros();
        pl.RunAsync();
        pl.Synchronize();
        double t1 = NowMicros();

        const auto& gs = pl.GetStats();
        pa.total_ms += (t1 - t0) / 1000.0;
        pa.io_ms += gs.io_time_us / 1000.0;
        pa.compute_ms += gs.compute_time_us / 1000.0;
        pa.bytes += gs.total_bytes_read;

        pl.BatchCleanGC();
        pl.Destroy();
      }
    }

    int n = FLAGS_repeat;
    double s_avg = sa.total_ms / n, p_avg = pa.total_ms / n;
    double s_tp =
        (s_avg > 0) ? (sa.bytes / n) / 1048576.0 / (s_avg / 1000.0) : 0;
    double p_tp =
        (p_avg > 0) ? (pa.bytes / n) / 1048576.0 / (p_avg / 1000.0) : 0;
    double speedup = (p_avg > 0) ? s_avg / p_avg : 0;

    std::fprintf(stdout, "\n");
    std::fprintf(
        stdout,
        "============================================================\n");
    std::fprintf(stdout, " Results (averaged over %d rounds)\n", n);
    std::fprintf(
        stdout,
        "============================================================\n\n");
    std::fprintf(stdout, "%-20s %12s %12s %12s %12s\n", "", "Total(ms)",
                 "I/O(ms)", "Compute(ms)", "MB/s");
    std::fprintf(stdout, "%-20s %12s %12s %12s %12s\n", "----", "--------",
                 "------", "----------", "----");
    std::fprintf(stdout, "%-20s %12.2f %12.2f %12.2f %12.2f\n", "Serial GC",
                 s_avg, sa.transfer_ms / n, s_avg - sa.transfer_ms / n, s_tp);
    std::fprintf(stdout, "%-20s %12.2f %12.2f %12.2f %12.2f\n", "Pipeline GC",
                 p_avg, pa.io_ms / n, pa.compute_ms / n, p_tp);
    std::fprintf(stdout, "\n  Speedup:         %.2fx\n", speedup);
    std::fprintf(stdout, "  Throughput gain: %.2fx\n",
                 (s_tp > 0) ? p_tp / s_tp : 0.0);
    std::fprintf(stdout, "\n  Data processed:  %.1f MB per round\n",
                 static_cast<double>(sa.bytes / n) / 1048576.0);
    std::fprintf(stdout, "  VLogs/round:     %d\n", num_vlogs_);
    std::fprintf(stdout, "  Entries/VLog:    %d\n", entries_);
    std::fprintf(stdout, "  Invalid ratio:   %.1f%%\n",
                 FLAGS_invalid_ratio * 100.0);
    std::fprintf(stdout, "\n");

    // 直方图: per-VLog延迟分布
    if (FLAGS_histogram) {
      std::fprintf(
          stdout,
          "============================================================\n");
      std::fprintf(stdout, " Per-VLog Latency Distribution\n");
      std::fprintf(
          stdout,
          "============================================================\n\n");

      leveldb::Histogram sh, ph;

      // Serial per-vlog
      {
        GPUGC gc(num_vlogs_, entries_);
        gc.MallocMemory();
        std::vector<uint32_t> ic;
        SetupGPUGCBitmap(gc, num_vlogs_, entries_, FLAGS_invalid_ratio, ic);

        for (int v = 0; v < num_vlogs_; v++) {
          gc.triggered_vlog_num = v + 1;
          FILE* f = fopen(vlog_paths_[v].c_str(), "rb");
          if (!f) continue;
          fseek(f, 0, SEEK_END);
          size_t fs = ftell(f);
          fseek(f, 0, SEEK_SET);
          std::vector<char> vd(fs);
          fread(vd.data(), 1, fs, f);
          fclose(f);

          double t0 = NowMicros();
          char* out = nullptr;
          gc.BeginGPUGCOptimized(vd.data(), fs, &out);
          sh.Add(NowMicros() - t0);
          delete[] out;
          gc.CleanGC();
        }
      }

      // Pipeline total
      {
        GPUGC gc(num_vlogs_, entries_);
        gc.MallocMemory();
        std::vector<uint32_t> ic;
        SetupGPUGCBitmap(gc, num_vlogs_, entries_, FLAGS_invalid_ratio, ic);

        leveldb::GCVLogPipeline pl(&gc, num_vlogs_);
        for (int v = 0; v < num_vlogs_; v++)
          pl.AddVLog(v + 1, vlog_paths_[v], vlog_sizes_[v]);

        double t0 = NowMicros();
        pl.RunAsync();
        pl.Synchronize();
        ph.Add(NowMicros() - t0);
        pl.BatchCleanGC();
        pl.Destroy();
      }

      std::fprintf(stdout, "Serial GC (per-VLog, us):\n%s\n",
                   sh.ToString().c_str());
      std::fprintf(stdout, "Pipeline GC (all VLogs total, us):\n%s\n",
                   ph.ToString().c_str());
    }

    std::fprintf(
        stdout,
        "============================================================\n");
    leveldb::GCVLogPipeline::DestroyGDS();
  }

  int entries_;
  int num_vlogs_;
  int record_size_;
  std::vector<std::string> vlog_paths_;
  std::vector<size_t> vlog_sizes_;
};

// ============================================================================
//  主函数
// ============================================================================
int main(int argc, char** argv) {
  ParseCommandLine(argc, argv);

  leveldb::my_stats.var_key_value_size = FLAGS_record_size - 12;
  leveldb::my_stats.max_num_log_item = FLAGS_entries;
  leveldb::my_stats.max_num_log = FLAGS_num_vlogs;
  leveldb::my_stats.clean_threshold =
      static_cast<uint32_t>(FLAGS_entries * FLAGS_invalid_ratio * 0.8);

  GCPipelineDBBenchmark bench;
  bench.Run();
  return 0;
}
const char* comma = strchr(ptr, ',');
std::string name;
if (comma) {
  name = std::string(ptr, comma - ptr);
  ptr = comma + 1;
} else {
  name = ptr;
  ptr = nullptr;
}

// 去除空格
while (!name.empty() && name[0] == ' ') name.erase(name.begin());

if (name == "serial_gc") {
  RunBenchmarkSerialGC();
} else if (name == "pipeline_gc") {
  RunBenchmarkPipelineGC();
} else if (name == "gc_comparison") {
  RunBenchmarkComparison();
} else {
  std::fprintf(stderr, "Unknown benchmark: %s\n", name.c_str());
}
}
}

private:
// ---- 场景A: 串行GC ----
void RunBenchmarkSerialGC() {
  std::fprintf(
      stdout, "------------------------------------------------------------\n");
  std::fprintf(stdout, "Benchmark: serial_gc (传统串行GC)\n");
  std::fprintf(
      stdout, "------------------------------------------------------------\n");

  Stats stats;
  stats.Start();

  for (int r = 0; r < FLAGS_repeat; r++) {
    // 初始化GPUGC
    GPUGC gpu_gc(num_vlogs_, entries_);
    gpu_gc.MallocMemory();
    leveldb::my_stats.data_transfer_time = 0;

    std::vector<uint32_t> invalid_counts;
    SetupGPUGCBitmap(gpu_gc, num_vlogs_, entries_, FLAGS_invalid_ratio,
                     invalid_counts);

    // 逐个VLog串行处理
    for (int v = 0; v < num_vlogs_; v++) {
      gpu_gc.triggered_vlog_num = v + 1;

      // 读取VLog文件到主机内存
      FILE* f = fopen(vlog_paths_[v].c_str(), "rb");
      if (!f) continue;
      fseek(f, 0, SEEK_END);
      size_t file_size = ftell(f);
      fseek(f, 0, SEEK_SET);
      std::vector<char> vlog_data(file_size);
      size_t nread = fread(vlog_data.data(), 1, file_size, f);
      fclose(f);
      if (nread != file_size) continue;

      stats.AddBytes(file_size);

      // 执行GC
      char* output = nullptr;
      gpu_gc.BeginGPUGCOptimized(vlog_data.data(), file_size, &output);

      stats.FinishedSingleOp(1);

      // 清理
      delete[] output;
      gpu_gc.CleanGC();
    }

    stats.transfer_time_us_ += leveldb::my_stats.data_transfer_time;
  }

  stats.Stop();
  stats.AddMessage(std::to_string(num_vlogs_) + " VLogs x " +
                   std::to_string(FLAGS_repeat) + " rounds");
  stats.Report("serial_gc");
  std::fprintf(stdout, "\n");
}

// ---- 场景B: 流水线GC ----
void RunBenchmarkPipelineGC() {
  std::fprintf(
      stdout, "------------------------------------------------------------\n");
  std::fprintf(stdout, "Benchmark: pipeline_gc (流水线GC)\n");
  std::fprintf(
      stdout, "------------------------------------------------------------\n");

  // 初始化GDS
  leveldb::GCVLogPipeline::InitGDS();

  Stats stats;
  stats.Start();

  for (int r = 0; r < FLAGS_repeat; r++) {
    GPUGC gpu_gc(num_vlogs_, entries_);
    gpu_gc.MallocMemory();

    std::vector<uint32_t> invalid_counts;
    SetupGPUGCBitmap(gpu_gc, num_vlogs_, entries_, FLAGS_invalid_ratio,
                     invalid_counts);

    // 创建流水线并添加所有VLog
    leveldb::GCVLogPipeline pipeline(&gpu_gc, num_vlogs_);
    for (int v = 0; v < num_vlogs_; v++) {
      pipeline.AddVLog(v + 1, vlog_paths_[v], vlog_sizes_[v]);
      stats.AddBytes(vlog_sizes_[v]);
    }

    // 执行流水线GC
    pipeline.RunAsync();
    pipeline.Synchronize();

    // 收集统计
    const auto& gc_stats = pipeline.GetStats();
    stats.io_time_us_ += gc_stats.io_time_us;
    stats.compute_time_us_ += gc_stats.compute_time_us;

    stats.FinishedSingleOp(num_vlogs_);

    pipeline.BatchCleanGC();
    pipeline.Destroy();
  }

  stats.Stop();
  stats.AddMessage(std::to_string(num_vlogs_) + " VLogs x " +
                   std::to_string(FLAGS_repeat) + " rounds");
  stats.Report("pipeline_gc");
  std::fprintf(stdout, "\n");

  leveldb::GCVLogPipeline::DestroyGDS();
}

// ---- 对比测试: Serial vs Pipeline ----
void RunBenchmarkComparison() {
  std::fprintf(
      stdout, "============================================================\n");
  std::fprintf(stdout,
               " Benchmark: gc_comparison (Serial GC vs Pipeline GC)\n");
  std::fprintf(
      stdout,
      "============================================================\n\n");

  // 初始化GDS
  leveldb::GCVLogPipeline::InitGDS();

  // 累积多次repeat的统计
  struct AccumStats {
    double total_time_ms = 0;
    double io_time_ms = 0;
    double compute_time_ms = 0;
    double transfer_time_ms = 0;
    size_t total_bytes = 0;
  };
  AccumStats serial_acc, pipeline_acc;

  for (int r = 0; r < FLAGS_repeat; r++) {
    std::fprintf(stdout, "--- Round %d/%d ---\n", r + 1, FLAGS_repeat);

    // ======== 场景A: Serial GC ========
    {
      GPUGC gpu_gc(num_vlogs_, entries_);
      gpu_gc.MallocMemory();
      leveldb::my_stats.data_transfer_time = 0;

      std::vector<uint32_t> invalid_counts;
      SetupGPUGCBitmap(gpu_gc, num_vlogs_, entries_, FLAGS_invalid_ratio,
                       invalid_counts);

      auto t0 = NowMicros();

      for (int v = 0; v < num_vlogs_; v++) {
        gpu_gc.triggered_vlog_num = v + 1;

        FILE* f = fopen(vlog_paths_[v].c_str(), "rb");
        if (!f) continue;
        fseek(f, 0, SEEK_END);
        size_t file_size = ftell(f);
        fseek(f, 0, SEEK_SET);
        std::vector<char> vlog_data(file_size);
        fread(vlog_data.data(), 1, file_size, f);
        fclose(f);

        char* output = nullptr;
        gpu_gc.BeginGPUGCOptimized(vlog_data.data(), file_size, &output);

        serial_acc.total_bytes += file_size;
        delete[] output;
        gpu_gc.CleanGC();
      }

      auto t1 = NowMicros();
      serial_acc.total_time_ms += (t1 - t0) / 1000.0;
      serial_acc.transfer_time_ms +=
          leveldb::my_stats.data_transfer_time / 1000.0;
    }

    // ======== 场景B: Pipeline GC ========
    {
      GPUGC gpu_gc(num_vlogs_, entries_);
      gpu_gc.MallocMemory();

      std::vector<uint32_t> invalid_counts;
      SetupGPUGCBitmap(gpu_gc, num_vlogs_, entries_, FLAGS_invalid_ratio,
                       invalid_counts);

      leveldb::GCVLogPipeline pipeline(&gpu_gc, num_vlogs_);
      for (int v = 0; v < num_vlogs_; v++) {
        pipeline.AddVLog(v + 1, vlog_paths_[v], vlog_sizes_[v]);
      }

      auto t0 = NowMicros();
      pipeline.RunAsync();
      pipeline.Synchronize();
      auto t1 = NowMicros();

      const auto& gc_stats = pipeline.GetStats();
      pipeline_acc.total_time_ms += (t1 - t0) / 1000.0;
      pipeline_acc.io_time_ms += gc_stats.io_time_us / 1000.0;
      pipeline_acc.compute_time_ms += gc_stats.compute_time_us / 1000.0;
      pipeline_acc.total_bytes += gc_stats.total_bytes_read;

      pipeline.BatchCleanGC();
      pipeline.Destroy();
    }
  }

  // 计算平均值
  int n = FLAGS_repeat;
  double serial_avg_ms = serial_acc.total_time_ms / n;
  double pipeline_avg_ms = pipeline_acc.total_time_ms / n;
  double serial_throughput =
      (serial_avg_ms > 0)
          ? (serial_acc.total_bytes / n) / 1048576.0 / (serial_avg_ms / 1000.0)
          : 0;
  double pipeline_throughput = (pipeline_avg_ms > 0)
                                   ? (pipeline_acc.total_bytes / n) /
                                         1048576.0 / (pipeline_avg_ms / 1000.0)
                                   : 0;
  double speedup = (pipeline_avg_ms > 0) ? serial_avg_ms / pipeline_avg_ms : 0;

  // ======== 输出对比报告 ========
  std::fprintf(stdout, "\n");
  std::fprintf(
      stdout, "============================================================\n");
  std::fprintf(stdout, " Results (averaged over %d rounds)\n", n);
  std::fprintf(
      stdout, "============================================================\n");
  std::fprintf(stdout, "\n");

  // 表头
  std::fprintf(stdout, "%-20s %12s %12s %12s %12s\n", "", "Total(ms)",
               "I/O(ms)", "Compute(ms)", "MB/s");
  std::fprintf(stdout, "%-20s %12s %12s %12s %12s\n", "----", "--------",
               "------", "----------", "----");

  // Serial GC
  std::fprintf(stdout, "%-20s %12.2f %12.2f %12.2f %12.2f\n", "Serial GC",
               serial_avg_ms, serial_acc.transfer_time_ms / n,
               (serial_avg_ms - serial_acc.transfer_time_ms / n),
               serial_throughput);

  // Pipeline GC
  std::fprintf(stdout, "%-20s %12.2f %12.2f %12.2f %12.2f\n", "Pipeline GC",
               pipeline_avg_ms, pipeline_acc.io_time_ms / n,
               pipeline_acc.compute_time_ms / n, pipeline_throughput);

  // 加速比
  std::fprintf(stdout, "\n");
  std::fprintf(stdout, "  Speedup:         %.2fx\n", speedup);
  std::fprintf(
      stdout, "  Throughput gain: %.2fx\n",
      (serial_throughput > 0) ? pipeline_throughput / serial_throughput : 0.0);
  std::fprintf(stdout, "\n");

  // 数据量统计
  std::fprintf(stdout, "  Data processed:  %.1f MB per round\n",
               static_cast<double>(serial_acc.total_bytes / n) / 1048576.0);
  std::fprintf(stdout, "  VLogs/round:     %d\n", num_vlogs_);
  std::fprintf(stdout, "  Entries/VLog:    %d\n", entries_);
  std::fprintf(stdout, "  Invalid ratio:   %.1f%%\n",
               FLAGS_invalid_ratio * 100.0);
  std::fprintf(stdout, "\n");

  // 延迟直方图 (如果启用)
  if (FLAGS_histogram) {
    std::fprintf(
        stdout,
        "============================================================\n");
    std::fprintf(stdout, " Per-VLog Latency Distribution\n");
    std::fprintf(
        stdout,
        "============================================================\n");

    // Serial GC per-vlog histogram
    leveldb::Histogram serial_hist;
    leveldb::Histogram pipeline_hist;

    // 重新运行一次以收集per-vlog数据
    {
      GPUGC gpu_gc(num_vlogs_, entries_);
      gpu_gc.MallocMemory();
      leveldb::my_stats.data_transfer_time = 0;

      std::vector<uint32_t> ic;
      SetupGPUGCBitmap(gpu_gc, num_vlogs_, entries_, FLAGS_invalid_ratio, ic);

      for (int v = 0; v < num_vlogs_; v++) {
        gpu_gc.triggered_vlog_num = v + 1;
        FILE* f = fopen(vlog_paths_[v].c_str(), "rb");
        if (!f) continue;
        fseek(f, 0, SEEK_END);
        size_t fs = ftell(f);
        fseek(f, 0, SEEK_SET);
        std::vector<char> vd(fs);
        fread(vd.data(), 1, fs, f);
        fclose(f);

        double t0 = NowMicros();
        char* output = nullptr;
        gpu_gc.BeginGPUGCOptimized(vd.data(), fs, &output);
        double t1 = NowMicros();
        serial_hist.Add(t1 - t0);

        delete[] output;
        gpu_gc.CleanGC();
      }
    }

    {
      GPUGC gpu_gc(num_vlogs_, entries_);
      gpu_gc.MallocMemory();
      std::vector<uint32_t> ic;
      SetupGPUGCBitmap(gpu_gc, num_vlogs_, entries_, FLAGS_invalid_ratio, ic);

      leveldb::GCVLogPipeline pipeline(&gpu_gc, num_vlogs_);
      for (int v = 0; v < num_vlogs_; v++) {
        pipeline.AddVLog(v + 1, vlog_paths_[v], vlog_sizes_[v]);
      }

      double t0 = NowMicros();
      pipeline.RunAsync();
      pipeline.Synchronize();
      double t1 = NowMicros();
      // Pipeline processes all VLogs in parallel, record total
      pipeline_hist.Add(t1 - t0);

      pipeline.BatchCleanGC();
      pipeline.Destroy();
    }

    std::fprintf(stdout, "\nSerial GC (per-VLog, us):\n%s\n",
                 serial_hist.ToString().c_str());
    std::fprintf(stdout, "Pipeline GC (all VLogs, us):\n%s\n",
                 pipeline_hist.ToString().c_str());
  }

  std::fprintf(
      stdout, "============================================================\n");

  leveldb::GCVLogPipeline::DestroyGDS();
}

// ============ 成员变量 ============
int entries_;
int num_vlogs_;
int record_size_;
std::vector<std::string> vlog_paths_;
std::vector<size_t> vlog_sizes_;
}
;

// ============================================================================
//  主函数
// ============================================================================
int main(int argc, char** argv) {
  ParseCommandLine(argc, argv);

  // 初始化my_stats
  leveldb::my_stats.var_key_value_size = FLAGS_record_size - 12;
  leveldb::my_stats.max_num_log_item = FLAGS_entries;
  leveldb::my_stats.max_num_log = FLAGS_num_vlogs;
  leveldb::my_stats.clean_threshold =
      static_cast<uint32_t>(FLAGS_entries * FLAGS_invalid_ratio * 0.8);

  GCPipelineDBBenchmark bench;
  bench.Run();

  return 0;
}
