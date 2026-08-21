//
// [文件说明] GCVLogPipeline — VLog级流水线垃圾回收器 (cudaMemcpy H2D 版本)
// [基于] gParaKV-GC-pipeline 的 GDS 版本改造而来
// [差异] 使用传统 cudaMemcpy HostToDevice 替代 cuFileReadAsync GDS 零拷贝,
//        不依赖 GPUDirect Storage 驱动, 兼容性更好。
//        保持3路CUDA worker流的流水线并行架构。
//
// 执行流程 (每个VLog):
//   1. open(O_RDONLY) + read() 读取VLog到主机内存
//   2. cudaMemcpyAsync H2D: 主机内存 → GPU显存
//   3. Adjustment内核: 统计bitmap中flag=0的数量
//   4. cudaStreamSynchronize: 获取flag_count(必须, 用于计算输出大小)
//   5. GPUGCOptimizedKernel: 压缩有效条目到输出缓冲区
//   6. cudaMemcpyAsync D2H: 压缩结果回主机端
//   7. cudaEventRecord: 标记完成
//

#include "db/cuda/gpu_coding.cuh"
#include "db/cuda_pipeline_gc/gc_pipeline_vlog.h"
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

// ============================================================================
//  前向声明gpu_gc.cu中的GPU内核 (CUDA分离编译支持跨TU链接)
// ============================================================================
__global__ void Adjustment(const uint8_t* flags, uint32_t max_num_log_item,
                           uint32_t* count);

__global__ void GPUGCOptimizedKernel(char* vlog_d, char* output_d,
                                     const uint8_t* flags,
                                     uint32_t var_key_value_size,
                                     uint32_t total_thread_num,
                                     uint32_t process_num_per_thread,
                                     uint32_t* global_count);

namespace leveldb {

// GDS驱动初始化状态 (H2D版本不使用GDS, 保留接口兼容性)
bool GCVLogPipeline::gds_initialized_ = false;

// ============================================================================
//  静态方法: GDS驱动管理 (H2D版本: 空实现, 保持接口兼容)
// ============================================================================

bool GCVLogPipeline::InitGDS() {
  // H2D版本不需要GDS驱动, 直接返回成功
  gds_initialized_ = true;
  return true;
}

void GCVLogPipeline::DestroyGDS() { gds_initialized_ = false; }

// ============================================================================
//  构造/析构
// ============================================================================

GCVLogPipeline::GCVLogPipeline(GPUGC* gpu_gc, size_t max_vlog_count)
    : gpu_gc_(gpu_gc), max_vlog_count_(max_vlog_count) {
  // 创建3路worker流
  for (size_t i = 0; i < NUM_GC_WORKER_STREAMS; ++i) {
    CHECK(cudaStreamCreate(&worker_streams_[i]));
  }
  // 同步流
  CHECK(cudaStreamCreate(&sync_stream_));

  // 预分配
  pending_vlogs_.reserve(max_vlog_count);
  per_vlog_buffers_.reserve(max_vlog_count);
  gds_handles_.reserve(max_vlog_count);
  vlog_complete_events_.reserve(max_vlog_count);
  results_.reserve(max_vlog_count);
}

GCVLogPipeline::~GCVLogPipeline() { Destroy(); }

// ============================================================================
//  收集触发GC的VLog (主机端扫描invalid_count[])
// ============================================================================

void GCVLogPipeline::CollectTriggeredVLogs(const std::string& vlog_dir) {
  uint32_t max_num_log = gpu_gc_->max_num_log;

  // D2H拷贝invalid_count数组到主机端
  std::vector<uint32_t> host_invalid_count(max_num_log);
  CHECK(cudaMemcpy(host_invalid_count.data(), gpu_gc_->invalid_count,
                   max_num_log * sizeof(uint32_t), cudaMemcpyDeviceToHost));

  uint32_t threshold = my_stats.clean_threshold;

  // 扫描所有VLog, 找出超过阈值的
  for (uint32_t i = 0; i < max_num_log; ++i) {
    if (host_invalid_count[i] >= threshold) {
      uint32_t vlog_num = i + 1;  // 1-indexed
      char path_buf[512];
      std::snprintf(path_buf, sizeof(path_buf), "%s/%06u.vlog",
                    vlog_dir.c_str(), vlog_num);
      std::string file_path(path_buf);

      struct stat st;
      if (stat(file_path.c_str(), &st) != 0) {
        std::fprintf(stderr,
                     "[GCVLogPipeline-H2D] Warning: cannot stat %s "
                     "(errno=%d: %s), skipping vlog %u\n",
                     file_path.c_str(), errno, strerror(errno), vlog_num);
        continue;
      }
      size_t file_size = static_cast<size_t>(st.st_size);
      if (file_size == 0) continue;

      AddVLog(vlog_num, file_path, file_size);
    }
  }

  std::fprintf(stdout,
               "[GCVLogPipeline-H2D] Collected %zu triggered VLogs "
               "(threshold=%u)\n",
               pending_vlogs_.size(), threshold);
}

// ============================================================================
//  手动添加VLog
// ============================================================================

void GCVLogPipeline::AddVLog(uint32_t vlog_num, const std::string& file_path,
                             size_t file_size) {
  size_t idx = pending_vlogs_.size();
  size_t aligned = GCAlignUp(file_size);

  pending_vlogs_.push_back({vlog_num, file_path, file_size, aligned});

  if (per_vlog_buffers_.size() <= idx) {
    per_vlog_buffers_.resize(idx + 1);
  }
  if (vlog_complete_events_.size() <= idx) {
    vlog_complete_events_.resize(idx + 1, nullptr);
  }
  if (gds_handles_.size() <= idx) {
    gds_handles_.resize(idx + 1);
  }
  if (results_.size() <= idx) {
    results_.resize(idx + 1);
  }
}

// ============================================================================
//  内部方法: 打开文件 (H2D版本: 不需要GDS注册, 仅打开文件描述符)
// ============================================================================

cudaError_t GCVLogPipeline::OpenAndRegisterFile(size_t vlog_idx) {
  if (vlog_idx >= pending_vlogs_.size()) return cudaErrorInvalidValue;

  const GCVLogDescriptor& desc = pending_vlogs_[vlog_idx];
  GC_GDSFileHandle& fh = gds_handles_[vlog_idx];

  if (fh.fd >= 0) return cudaSuccess;

  // H2D版本: 仅以O_RDONLY打开文件 (不需要O_DIRECT, 不需要GDS注册)
  fh.fd = open(desc.file_path.c_str(), O_RDONLY);
  if (fh.fd < 0) {
    std::fprintf(stderr,
                 "[GCVLogPipeline-H2D] Failed to open file: %s "
                 "(errno=%d: %s)\n",
                 desc.file_path.c_str(), errno, strerror(errno));
    return cudaErrorFileNotFound;
  }

  return cudaSuccess;
}

// ============================================================================
//  内部方法: 在指定流上处理单个VLog
//  H2D版本流程: POSIX read → cudaMemcpyAsync H2D → Adjustment → sync
//               → Compact → D2H
// ============================================================================

cudaError_t GCVLogPipeline::ProcessSingleVLogAsync(size_t vlog_idx,
                                                   int stream_idx) {
  if (vlog_idx >= pending_vlogs_.size()) return cudaErrorInvalidValue;

  cudaStream_t stream = worker_streams_[stream_idx];
  const GCVLogDescriptor& desc = pending_vlogs_[vlog_idx];
  GC_GDSFileHandle& fh = gds_handles_[vlog_idx];
  PerVLogBuffers& buf = per_vlog_buffers_[vlog_idx];
  GCVLogResult& result = results_[vlog_idx];

  uint32_t max_num_log_item = gpu_gc_->max_num_log_item;
  uint32_t record_size = my_stats.var_key_value_size + 12;

  // --- 步骤1: 分配主机端缓冲区并读取VLog文件 ---
  auto io_start = std::chrono::high_resolution_clock::now();

  char* host_buf = new char[desc.file_size];
  ssize_t bytes_read_total = 0;
  while (bytes_read_total < static_cast<ssize_t>(desc.file_size)) {
    ssize_t n = read(fh.fd, host_buf + bytes_read_total,
                     desc.file_size - bytes_read_total);
    if (n <= 0) {
      std::fprintf(stderr,
                   "[GCVLogPipeline-H2D] read() failed for vlog %u: "
                   "errno=%d: %s\n",
                   desc.vlog_num, errno, strerror(errno));
      delete[] host_buf;
      return cudaErrorUnknown;
    }
    bytes_read_total += n;
  }

  // --- 步骤2: 分配GPU缓冲区 ---
  CHECK(cudaMalloc(&buf.vlog_d, desc.file_size));

  // --- 步骤3: cudaMemcpyAsync H2D — 主机内存 → GPU显存 ---
  CHECK(cudaMemcpyAsync(buf.vlog_d, host_buf, desc.file_size,
                        cudaMemcpyHostToDevice, stream));

  // 主机缓冲区可以释放 (数据已排队传输到GPU)
  delete[] host_buf;

  // --- 步骤4: Adjustment内核 — 统计无效flag数量 ---
  CHECK(cudaMalloc(&buf.flag_count_d, sizeof(uint32_t)));
  CHECK(cudaMemsetAsync(buf.flag_count_d, 0, sizeof(uint32_t), stream));

  size_t adj_block = 256;
  size_t adj_grid = (max_num_log_item + adj_block - 1) / adj_block;
  Adjustment<<<adj_grid, adj_block, 0, stream>>>(
      gpu_gc_->gpu_flags + (desc.vlog_num - 1) * max_num_log_item,
      max_num_log_item, buf.flag_count_d);

  // --- 步骤5: 中间同步 — 获取flag_count (必须, 用于计算输出大小) ---
  CHECK(cudaStreamSynchronize(stream));

  auto io_end = std::chrono::high_resolution_clock::now();
  auto io_duration =
      std::chrono::duration_cast<std::chrono::microseconds>(io_end - io_start);
  stats_.io_time_us += io_duration.count();
  stats_.total_bytes_read += desc.file_size;

  // D2H拷贝flag_count
  CHECK(cudaMemcpy(&buf.flag_count, buf.flag_count_d, sizeof(uint32_t),
                   cudaMemcpyDeviceToHost));

  // --- 步骤6: 计算输出大小并分配输出缓冲区 ---
  buf.output_size = desc.file_size - buf.flag_count * record_size;

  if (buf.output_size > desc.file_size) {
    std::fprintf(stderr,
                 "[GCVLogPipeline-H2D] Warning: output_size (%zu) > file_size "
                 "(%zu) for vlog %u, flag_count=%u. Setting output_size=0.\n",
                 buf.output_size, desc.file_size, desc.vlog_num,
                 buf.flag_count);
    buf.output_size = 0;
    return cudaSuccess;
  }

  if (buf.output_size == 0) {
    result.vlog_num = desc.vlog_num;
    result.output_h = nullptr;
    result.output_size = 0;
    result.num_valid = 0;
    return cudaSuccess;
  }

  CHECK(cudaMalloc(&buf.output_d, buf.output_size));
  CHECK(cudaMalloc(&buf.global_count_d, sizeof(uint32_t)));
  CHECK(cudaMemsetAsync(buf.global_count_d, 0, sizeof(uint32_t), stream));

  // --- 步骤7: GPUGCOptimizedKernel — 压缩有效条目 ---
  auto compute_start = std::chrono::high_resolution_clock::now();

  uint32_t process_num_per_thread = 100;
  uint32_t total_thread_num = max_num_log_item / process_num_per_thread;
  size_t compact_block = 1024;
  size_t compact_grid = (total_thread_num + compact_block - 1) / compact_block;

  GPUGCOptimizedKernel<<<compact_grid, compact_block, 0, stream>>>(
      buf.vlog_d, buf.output_d,
      gpu_gc_->gpu_flags + (desc.vlog_num - 1) * max_num_log_item, record_size,
      total_thread_num, process_num_per_thread, buf.global_count_d);

  // --- 步骤8: D2H拷贝 — 压缩结果回主机端 ---
  result.vlog_num = desc.vlog_num;
  result.output_size = buf.output_size;
  result.output_h = new char[buf.output_size];
  result.num_valid = max_num_log_item - buf.flag_count;

  CHECK(cudaMemcpyAsync(result.output_h, buf.output_d, buf.output_size,
                        cudaMemcpyDeviceToHost, stream));

  // --- 步骤9: 记录完成事件 ---
  if (vlog_complete_events_[vlog_idx] == nullptr) {
    CHECK(cudaEventCreate(&vlog_complete_events_[vlog_idx]));
  }
  CHECK(cudaEventRecord(vlog_complete_events_[vlog_idx], stream));

  auto compute_end = std::chrono::high_resolution_clock::now();
  auto compute_duration = std::chrono::duration_cast<std::chrono::microseconds>(
      compute_end - compute_start);
  stats_.compute_time_us += compute_duration.count();
  stats_.total_valid_entries += result.num_valid;

  return cudaSuccess;
}

// ============================================================================
//  启动流水线执行
// ============================================================================

cudaError_t GCVLogPipeline::RunAsync() {
  if (pending_vlogs_.empty()) {
    std::fprintf(stdout, "[GCVLogPipeline-H2D] No VLogs to process.\n");
    return cudaSuccess;
  }

  if (running_.load()) {
    std::fprintf(stderr, "[GCVLogPipeline-H2D] Pipeline already running.\n");
    return cudaErrorInvalidValue;
  }
  running_.store(true);

  stats_ = GCStats{};

  std::fprintf(stdout,
               "[GCVLogPipeline-H2D] Starting pipeline for %zu VLogs "
               "across %zu worker streams (cudaMemcpy H2D mode).\n",
               pending_vlogs_.size(), NUM_GC_WORKER_STREAMS);

  // 轮询分配VLog到worker流
  for (size_t i = 0; i < pending_vlogs_.size(); ++i) {
    int stream_idx = i % NUM_GC_WORKER_STREAMS;

    cudaError_t err = OpenAndRegisterFile(i);
    if (err != cudaSuccess) {
      std::fprintf(stderr,
                   "[GCVLogPipeline-H2D] Failed to open vlog %u, skipping.\n",
                   pending_vlogs_[i].vlog_num);
      continue;
    }

    err = ProcessSingleVLogAsync(i, stream_idx);
    if (err != cudaSuccess) {
      std::fprintf(stderr,
                   "[GCVLogPipeline-H2D] Failed to process vlog %u, "
                   "skipping.\n",
                   pending_vlogs_[i].vlog_num);
    }
  }

  stats_.total_time_us = 0;

  return cudaSuccess;
}

// ============================================================================
//  同步等待所有VLog完成
// ============================================================================

cudaError_t GCVLogPipeline::Synchronize() {
  for (size_t i = 0; i < NUM_GC_WORKER_STREAMS; ++i) {
    CHECK(cudaStreamSynchronize(worker_streams_[i]));
  }

  running_.store(false);

  std::fprintf(stdout,
               "[GCVLogPipeline-H2D] Pipeline completed.\n"
               "  VLogs processed: %zu\n"
               "  I/O time: %lu us\n"
               "  Compute time: %lu us\n"
               "  Total bytes read: %zu\n"
               "  Total valid entries: %zu\n",
               pending_vlogs_.size(), stats_.io_time_us, stats_.compute_time_us,
               stats_.total_bytes_read, stats_.total_valid_entries);

  return cudaSuccess;
}

// ============================================================================
//  批量清理已处理VLog的bitmap和invalid_count
// ============================================================================

void GCVLogPipeline::BatchCleanGC() {
  uint32_t max_num_log_item = gpu_gc_->max_num_log_item;

  for (const auto& desc : pending_vlogs_) {
    uint32_t vlog_num = desc.vlog_num;

    CHECK(
        cudaMemset(&gpu_gc_->invalid_count[vlog_num - 1], 0, sizeof(uint32_t)));
    CHECK(cudaMemset(gpu_gc_->gpu_flags + (vlog_num - 1) * max_num_log_item, 1,
                     max_num_log_item));
  }

  gpu_gc_->triggered_vlog_num = 0;
  gpu_gc_->triggered_invalid_count = 0;

  CHECK(cudaStreamSynchronize(gpu_gc_->stream));

  std::fprintf(stdout,
               "[GCVLogPipeline-H2D] BatchCleanGC completed for %zu VLogs.\n",
               pending_vlogs_.size());
}

// ============================================================================
//  内部方法: 关闭文件 (H2D版本: 仅close, 无需GDS注销)
// ============================================================================

cudaError_t GCVLogPipeline::CloseAndDeregisterFiles() {
  for (auto& fh : gds_handles_) {
    if (fh.fd >= 0) {
      close(fh.fd);
      fh.fd = -1;
    }
  }
  gds_handles_.clear();
  return cudaSuccess;
}

// ============================================================================
//  内部方法: 清理所有per-vlog缓冲区
// ============================================================================

cudaError_t GCVLogPipeline::CleanupBuffers() {
  for (auto& buf : per_vlog_buffers_) {
    if (buf.vlog_d) cudaFree(buf.vlog_d);
    if (buf.flag_count_d) cudaFree(buf.flag_count_d);
    if (buf.global_count_d) cudaFree(buf.global_count_d);
    if (buf.output_d) cudaFree(buf.output_d);
  }
  per_vlog_buffers_.clear();

  for (auto& event : vlog_complete_events_) {
    if (event) cudaEventDestroy(event);
  }
  vlog_complete_events_.clear();

  for (auto& result : results_) {
    if (result.output_h) {
      delete[] result.output_h;
      result.output_h = nullptr;
    }
  }
  results_.clear();

  return cudaSuccess;
}

// ============================================================================
//  释放所有资源
// ============================================================================

void GCVLogPipeline::Destroy() {
  CloseAndDeregisterFiles();
  CleanupBuffers();
  pending_vlogs_.clear();

  for (size_t i = 0; i < NUM_GC_WORKER_STREAMS; ++i) {
    if (worker_streams_[i]) {
      cudaStreamDestroy(worker_streams_[i]);
      worker_streams_[i] = nullptr;
    }
  }
  if (sync_stream_) {
    cudaStreamDestroy(sync_stream_);
    sync_stream_ = nullptr;
  }
}

}  // namespace leveldb
