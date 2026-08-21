//
// [文件说明] GC流水线基准测试 — Serial vs Pipeline GC对比
// [新增] gParaKV-GC新增。对比两种GC路径:
//   Serial:   逐个VLog调用BeginGPUGCOptimized (现有方式)
//   Pipeline: GCVLogPipeline多流并行 (新方式)
//
// 测试内容:
//   1. 生成合成VLog文件 (可控条目数和无效比例)
//   2. 初始化GPUGC bitmap状态
//   3. 运行Serial GC并计时
//   4. 运行Pipeline GC并计时
//   5. 输出对比报告
//
// 使用示例:
//   gc_pipeline_bench --num_vlogs=4 --entries=10000 --invalid_ratio=0.5
//   gc_pipeline_bench --num_vlogs=8 --entries=50000 --invalid_ratio=0.3
//   --repeat=3
//

#include "db/cuda_pipeline_gc/gc_pipeline_vlog.h"
#include "db/gpu_gc.h"
#include "db/my_stats.h"
#include <chrono>
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

// ============================================================================
//  命令行参数
// ============================================================================
static int FLAGS_num_vlogs = 4;           // VLog数量
static int FLAGS_entries = 10000;         // 每个VLog的条目数
static double FLAGS_invalid_ratio = 0.5;  // 无效条目比例 (0.0~1.0)
static int FLAGS_repeat = 1;              // 重复执行次数
static int FLAGS_record_size = 128;  // 每条VLog记录大小(字节), 含12字节头部
static const char* FLAGS_tmpdir = "/tmp/gc_bench";  // 临时文件目录

// ============================================================================
//  命令行解析
// ============================================================================
static void ParseCommandLine(int argc, char** argv) {
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg.substr(0, 13) == "--num_vlogs=") {
      FLAGS_num_vlogs = std::atoi(arg.c_str() + 13);
    } else if (arg.substr(0, 10) == "--entries=") {
      FLAGS_entries = std::atoi(arg.c_str() + 10);
    } else if (arg.substr(0, 17) == "--invalid_ratio=") {
      FLAGS_invalid_ratio = std::atof(arg.c_str() + 17);
    } else if (arg.substr(0, 9) == "--repeat=") {
      FLAGS_repeat = std::atoi(arg.c_str() + 9);
    } else if (arg.substr(0, 15) == "--record_size=") {
      FLAGS_record_size = std::atoi(arg.c_str() + 15);
    } else if (arg.substr(0, 10) == "--tmpdir=") {
      FLAGS_tmpdir = argv[i] + 10;
    } else if (arg == "--help") {
      std::printf(
          "Usage: gc_pipeline_bench [options]\n"
          "  --num_vlogs=N       Number of VLogs to process (default: 4)\n"
          "  --entries=N         Entries per VLog (default: 10000)\n"
          "  --invalid_ratio=F   Ratio of invalid entries 0.0-1.0 "
          "(default: 0.5)\n"
          "  --repeat=N          Repeat count (default: 1)\n"
          "  --record_size=N     Record size in bytes (default: 128)\n"
          "  --tmpdir=PATH       Temp directory for VLog files\n");
      std::exit(0);
    }
  }
}

// ============================================================================
//  合成VLog数据生成
// ============================================================================

// 生成VLog文件: 每个条目为record_size字节的固定数据
// 文件总大小 = entries * record_size
static bool GenerateVLogFile(const std::string& path, int entries,
                             int record_size) {
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    std::fprintf(stderr, "Failed to create %s: %s\n", path.c_str(),
                 strerror(errno));
    return false;
  }

  // 使用写入缓冲区
  std::vector<char> buf(record_size, 0);
  // 填充一些可识别的数据模式
  for (int i = 0; i < record_size; i++) {
    buf[i] = static_cast<char>(i & 0xFF);
  }

  for (int e = 0; e < entries; e++) {
    // 在每条记录的头部写入条目编号 (用于验证)
    uint32_t entry_id = static_cast<uint32_t>(e);
    std::memcpy(buf.data(), &entry_id, sizeof(uint32_t));

    ssize_t written = write(fd, buf.data(), record_size);
    if (written != record_size) {
      std::fprintf(stderr, "Write failed at entry %d\n", e);
      close(fd);
      return false;
    }
  }

  close(fd);
  return true;
}

// ============================================================================
//  初始化GPUGC bitmap: 按指定invalid_ratio设置无效条目
// ============================================================================
static void SetupGPUGCBitmap(GPUGC& gpu_gc, int num_vlogs, int entries,
                             double invalid_ratio) {
  uint32_t max_num_log_item = gpu_gc.max_num_log_item;

  // 主机端构造bitmap数据
  std::vector<uint8_t> host_flags(
      static_cast<size_t>(num_vlogs) * max_num_log_item, 1);
  std::vector<uint32_t> host_invalid_count(num_vlogs, 0);

  std::mt19937 rng(42);  // 固定种子确保可重复
  std::uniform_real_distribution<double> dist(0.0, 1.0);

  for (int v = 0; v < num_vlogs; v++) {
    for (int e = 0; e < entries; e++) {
      if (dist(rng) < invalid_ratio) {
        host_flags[v * max_num_log_item + e] = 0;  // 标记为无效
        host_invalid_count[v]++;
      }
    }
  }

  // H2D拷贝bitmap
  for (int v = 0; v < num_vlogs; v++) {
    CHECK(cudaMemcpy(gpu_gc.gpu_flags + v * max_num_log_item,
                     host_flags.data() + v * max_num_log_item,
                     max_num_log_item * sizeof(uint8_t),
                     cudaMemcpyHostToDevice));
    CHECK(cudaMemcpy(&gpu_gc.invalid_count[v], &host_invalid_count[v],
                     sizeof(uint32_t), cudaMemcpyHostToDevice));
  }
  CHECK(cudaStreamSynchronize(gpu_gc.stream));

  std::printf("Bitmap setup: %d VLogs, %d entries each, %.1f%% invalid\n",
              num_vlogs, entries, invalid_ratio * 100.0);
}

// ============================================================================
//  测试结果结构
// ============================================================================
struct BenchResult {
  double total_time_ms = 0;
  double io_time_ms = 0;
  double compute_time_ms = 0;
  size_t total_bytes = 0;
  size_t total_valid = 0;
};

// ============================================================================
//  Serial GC基准测试
// ============================================================================
static BenchResult RunSerialGC(GPUGC& gpu_gc, int num_vlogs,
                               const std::vector<std::string>& vlog_paths,
                               const std::vector<size_t>& vlog_sizes) {
  BenchResult result;
  auto total_start = std::chrono::high_resolution_clock::now();

  for (int v = 0; v < num_vlogs; v++) {
    // 设置触发状态 (模拟TriggerGC选择了这个VLog)
    gpu_gc.triggered_vlog_num = v + 1;  // 1-indexed

    // 读取VLog文件到主机内存
    FILE* f = fopen(vlog_paths[v].c_str(), "rb");
    if (!f) {
      std::fprintf(stderr, "Cannot open %s\n", vlog_paths[v].c_str());
      continue;
    }
    fseek(f, 0, SEEK_END);
    size_t file_size = ftell(f);
    fseek(f, 0, SEEK_SET);
    std::vector<char> vlog_data(file_size);
    size_t nread = fread(vlog_data.data(), 1, file_size, f);
    fclose(f);
    if (nread != file_size) {
      std::fprintf(stderr, "Read mismatch for vlog %d\n", v + 1);
      continue;
    }

    // 运行BeginGPUGCOptimized
    char* output = nullptr;
    gpu_gc.BeginGPUGCOptimized(vlog_data.data(), file_size, &output);

    result.total_bytes += file_size;
    result.total_valid +=
        gpu_gc.max_num_log_item - gpu_gc.triggered_invalid_count;

    // 清理
    delete[] output;
    gpu_gc.CleanGC();
  }

  auto total_end = std::chrono::high_resolution_clock::now();
  result.total_time_ms = std::chrono::duration_cast<std::chrono::microseconds>(
                             total_end - total_start)
                             .count() /
                         1000.0;
  result.io_time_ms =
      static_cast<double>(leveldb::my_stats.data_transfer_time) / 1000.0;

  return result;
}

// ============================================================================
//  Pipeline GC基准测试
// ============================================================================
static BenchResult RunPipelineGC(GPUGC& gpu_gc, int num_vlogs,
                                 const std::vector<std::string>& vlog_paths,
                                 const std::vector<size_t>& vlog_sizes) {
  BenchResult result;
  auto total_start = std::chrono::high_resolution_clock::now();

  leveldb::GCVLogPipeline pipeline(&gpu_gc, num_vlogs);

  // 手动添加所有VLog
  for (int v = 0; v < num_vlogs; v++) {
    pipeline.AddVLog(v + 1, vlog_paths[v], vlog_sizes[v]);
  }

  // 启动并等待
  pipeline.RunAsync();
  pipeline.Synchronize();

  auto total_end = std::chrono::high_resolution_clock::now();
  result.total_time_ms = std::chrono::duration_cast<std::chrono::microseconds>(
                             total_end - total_start)
                             .count() /
                         1000.0;

  const auto& stats = pipeline.GetStats();
  result.io_time_ms = static_cast<double>(stats.io_time_us) / 1000.0;
  result.compute_time_ms = static_cast<double>(stats.compute_time_us) / 1000.0;
  result.total_bytes = stats.total_bytes_read;
  result.total_valid = stats.total_valid_entries;

  // 清理bitmap状态
  pipeline.BatchCleanGC();
  pipeline.Destroy();

  return result;
}

// ============================================================================
//  主函数
// ============================================================================
int main(int argc, char** argv) {
  ParseCommandLine(argc, argv);

  std::printf("=== GC Pipeline Benchmark ===\n");
  std::printf("  VLogs: %d\n", FLAGS_num_vlogs);
  std::printf("  Entries per VLog: %d\n", FLAGS_entries);
  std::printf("  Invalid ratio: %.1f%%\n", FLAGS_invalid_ratio * 100.0);
  std::printf("  Record size: %d bytes\n", FLAGS_record_size);
  std::printf("  Repeat: %d\n\n", FLAGS_repeat);

  // 初始化my_stats
  leveldb::my_stats.var_key_value_size = FLAGS_record_size - 12;
  leveldb::my_stats.max_num_log_item = FLAGS_entries;
  leveldb::my_stats.max_num_log = FLAGS_num_vlogs;
  leveldb::my_stats.clean_threshold =
      static_cast<uint32_t>(FLAGS_entries * FLAGS_invalid_ratio * 0.8);

  // 创建临时目录
  mkdir(FLAGS_tmpdir, 0755);

  // 生成VLog文件
  std::vector<std::string> vlog_paths;
  std::vector<size_t> vlog_sizes;
  for (int v = 0; v < FLAGS_num_vlogs; v++) {
    char path_buf[512];
    std::snprintf(path_buf, sizeof(path_buf), "%s/%06d.vlog", FLAGS_tmpdir,
                  v + 1);
    std::string path(path_buf);
    vlog_paths.push_back(path);

    size_t file_size = static_cast<size_t>(FLAGS_entries) * FLAGS_record_size;
    vlog_sizes.push_back(file_size);

    std::printf("Generating VLog %d: %s (%zu bytes)\n", v + 1, path.c_str(),
                file_size);
    if (!GenerateVLogFile(path, FLAGS_entries, FLAGS_record_size)) {
      std::fprintf(stderr, "Failed to generate VLog file\n");
      return 1;
    }
  }

  // 初始化GPUGC
  GPUGC gpu_gc(FLAGS_num_vlogs, FLAGS_entries);
  gpu_gc.MallocMemory();

  // 初始化GDS驱动
  leveldb::GCVLogPipeline::InitGDS();

  std::printf("\n");

  // 运行基准测试
  for (int r = 0; r < FLAGS_repeat; r++) {
    std::printf("--- Repeat %d/%d ---\n", r + 1, FLAGS_repeat);

    // 设置bitmap
    SetupGPUGCBitmap(gpu_gc, FLAGS_num_vlogs, FLAGS_entries,
                     FLAGS_invalid_ratio);

    // Serial GC
    leveldb::my_stats.data_transfer_time = 0;
    BenchResult serial =
        RunSerialGC(gpu_gc, FLAGS_num_vlogs, vlog_paths, vlog_sizes);

    // 重新设置bitmap (因为Serial GC已清理)
    SetupGPUGCBitmap(gpu_gc, FLAGS_num_vlogs, FLAGS_entries,
                     FLAGS_invalid_ratio);

    // Pipeline GC
    BenchResult pipeline =
        RunPipelineGC(gpu_gc, FLAGS_num_vlogs, vlog_paths, vlog_sizes);

    // 输出对比报告
    double speedup = (serial.total_time_ms > 0)
                         ? serial.total_time_ms / pipeline.total_time_ms
                         : 0.0;
    double serial_throughput = (serial.total_time_ms > 0)
                                   ? static_cast<double>(serial.total_bytes) /
                                         1024.0 / 1024.0 /
                                         (serial.total_time_ms / 1000.0)
                                   : 0.0;
    double pipeline_throughput =
        (pipeline.total_time_ms > 0)
            ? static_cast<double>(pipeline.total_bytes) / 1024.0 / 1024.0 /
                  (pipeline.total_time_ms / 1000.0)
            : 0.0;

    std::printf("\n");
    std::printf("=== Results (Repeat %d) ===\n", r + 1);
    std::printf("%-20s %12s %12s %12s %12s\n", "", "Total(ms)", "I/O(ms)",
                "Compute(ms)", "MB/s");
    std::printf("%-20s %12.2f %12.2f %12.2f %12.2f\n", "Serial GC",
                serial.total_time_ms, serial.io_time_ms,
                serial.total_time_ms - serial.io_time_ms, serial_throughput);
    std::printf("%-20s %12.2f %12.2f %12.2f %12.2f\n", "Pipeline GC",
                pipeline.total_time_ms, pipeline.io_time_ms,
                pipeline.compute_time_ms, pipeline_throughput);
    std::printf("  Speedup: %.2fx\n\n", speedup);
  }

  // 清理
  leveldb::GCVLogPipeline::DestroyGDS();

  // 删除临时文件
  for (const auto& path : vlog_paths) {
    unlink(path.c_str());
  }
  rmdir(FLAGS_tmpdir);

  std::printf("Benchmark complete.\n");
  return 0;
}
