//
// [文件说明] GCVLogPipeline — VLog级流水线垃圾回收器 (cudaMemcpy H2D 版本)
// [基于] gParaKV-GC-pipeline 的 GDS 版本改造而来
// [差异] 使用 cudaMemcpy HostToDevice 替代 cuFileReadAsync GDS 零拷贝,
//        不依赖 GPUDirect Storage 驱动, 兼容性更好。
//        保持3路CUDA worker流的流水线并行架构。
//

#pragma once

#include "db/gpu_gc.h"
#include "db/my_stats.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cuda_runtime.h>
#include <string>
#include <vector>

// H2D版本: 不需要GDS头文件

namespace leveldb {

// [常量] GC流水线中worker流的数量
// 与PipelineManagerGDS保持一致, 使用3路流实现VLog间重叠
constexpr size_t NUM_GC_WORKER_STREAMS = 3;

// [常量] 对齐要求 (4KB, 保留用于兼容)
constexpr size_t GC_GDS_ALIGNMENT = 4096;

// [工具函数] 向上对齐到4KB
inline size_t GCAlignUp(size_t x, size_t alignment = GC_GDS_ALIGNMENT) {
  return (x + alignment - 1) & ~(alignment - 1);
}

// [结构] GCVLogDescriptor — 待处理VLog的元数据
struct GCVLogDescriptor {
  uint32_t vlog_num;      // VLog编号 (1-indexed, 与GPUGC中一致)
  std::string file_path;  // VLog文件的磁盘路径
  size_t file_size;       // VLog文件实际大小(字节)
  size_t aligned_size;    // 4KB对齐后的I/O大小
};

// [结构] PerVLogBuffers — 每个VLog在GPU端的缓冲区集合
struct PerVLogBuffers {
  char* vlog_d = nullptr;            // GPU端VLog原始数据 (GDS写入目标)
  uint32_t* flag_count_d = nullptr;  // Adjustment内核输出: 无效flag计数
  uint32_t* global_count_d = nullptr;  // Compact内核的atomicAdd计数器
  char* output_d = nullptr;  // Compact内核输出: 压缩后的有效条目
  size_t output_size = 0;    // 压缩后输出大小(字节)
  uint32_t flag_count = 0;   // 主机端拷贝的flag_count值
};

// [结构] GCVLogResult — 单个VLog的GC处理结果
struct GCVLogResult {
  uint32_t vlog_num;  // VLog编号
  char* output_h;  // 主机端压缩结果(有效条目数据, 由GCVLogPipeline管理生命周期)
  size_t output_size;  // 压缩后大小(字节)
  uint32_t num_valid;  // 有效条目数量
};

// [结构] GCStats — 流水线GC的统计信息
struct GCStats {
  uint64_t io_time_us = 0;       // I/O(GDS读取+D2H)总耗时(微秒)
  uint64_t compute_time_us = 0;  // GPU内核(Adjustment+Compact)总耗时(微秒)
  uint64_t total_time_us = 0;    // 端到端总耗时(微秒)
  size_t total_bytes_read = 0;     // GDS读取的总字节数
  size_t total_valid_entries = 0;  // 所有VLog的有效条目总数
};

// [结构] GC_GDSFileHandle — 文件句柄 (H2D版本: 仅保留fd)
struct GC_GDSFileHandle {
  int fd = -1;  // 文件描述符 (O_RDONLY)
  // H2D版本: 不需要GDS句柄
};

// [类] GCVLogPipeline — VLog级流水线垃圾回收器 (cudaMemcpy H2D 版本)
// 使用传统 cudaMemcpy H2D 从主机内存传输VLog数据到GPU,
// 通过多CUDA流实现多个VLog的并行垃圾回收处理。
// 不依赖GPUDirect Storage, 兼容性更好。
//
// 生命周期:
//   1. InitGDS() (H2D版本: 空操作, 保持接口兼容)
//   2. 构造GCVLogPipeline, 传入GPUGC指针
//   3. CollectTriggeredVLogs() 或 AddVLog() 添加待处理VLog
//   4. RunAsync() 启动流水线
//   5. Synchronize() 等待完成
//   6. GetResults() 获取结果, BatchCleanGC() 清理bitmap
//   7. Destroy() 释放资源
//   8. DestroyGDS() (H2D版本: 空操作)
class GCVLogPipeline {
 public:
  // [构造函数] 创建CUDA流并初始化状态
  // gpu_gc: 指向现有GPUGC实例的指针(非所有权, 仅借用)
  // max_vlog_count: 最大预期VLog数(用于预分配)
  GCVLogPipeline(GPUGC* gpu_gc, size_t max_vlog_count);

  // [析构函数] 自动释放资源
  ~GCVLogPipeline();

  // [禁用拷贝]
  GCVLogPipeline(const GCVLogPipeline&) = delete;
  GCVLogPipeline& operator=(const GCVLogPipeline&) = delete;

  // [静态方法] 初始化GDS驱动 (进程级别, 全局仅调用一次)
  // 如果PipelineManagerGDS已经初始化过GDS, 本方法会安全地返回true
  static bool InitGDS();

  // [静态方法] 销毁GDS驱动 (进程退出前调用)
  static void DestroyGDS();

  // [功能] 自动收集所有触发GC的VLog
  // 从gpu_gc->invalid_count[]数组D2H拷贝到主机,
  // 扫描所有超过clean_threshold的VLog, 使用vlog_dir和VLogFileName构造文件路径,
  // 添加到待处理队列 vlog_dir: VLog文件所在目录名 (对应DBImpl::vlog_name_)
  void CollectTriggeredVLogs(const std::string& vlog_dir);

  // [功能] 手动添加一个VLog到流水线队列
  // vlog_num: VLog编号 (1-indexed)
  // file_path: VLog文件的磁盘路径
  // file_size: VLog文件大小(字节)
  void AddVLog(uint32_t vlog_num, const std::string& file_path,
               size_t file_size);

  // [功能] 启动流水线执行 (异步, 主机端立即返回)
  // 内部流程 (每个VLog):
  //   1. 打开文件(O_DIRECT) + 注册GDS句柄
  //   2. 分配GPU缓冲区(含64KB padding workaround)
  //   3. cuFileReadAsync: NVMe到GPU直通读取
  //   4. Adjustment内核: 统计无效flag数量
  //   5. 中间同步: 获取flag_count计算输出大小
  //   6. GPUGCOptimizedKernel: 压缩有效条目
  //   7. D2H拷贝: 压缩结果回主机端
  //   8. 记录完成事件
  cudaError_t RunAsync();

  // [功能] 同步等待所有VLog处理完成
  cudaError_t Synchronize();

  // [功能] 获取所有VLog的GC处理结果
  const std::vector<GCVLogResult>& GetResults() const { return results_; }

  // [功能] 获取统计信息
  const GCStats& GetStats() const { return stats_; }

  // [功能] 批量清理所有已处理VLog的bitmap和invalid_count
  // 对每个已处理的VLog:
  //   - cudaMemset invalid_count[vlog_num-1] = 0
  //   - cudaMemset gpu_flags段 = 1 (全部有效)
  // 重置gpu_gc->triggered_vlog_num = 0
  void BatchCleanGC();

  // [功能] 释放所有GPU和CPU资源
  void Destroy();

  // [功能] 获取待处理VLog数量
  size_t GetPendingCount() const { return pending_vlogs_.size(); }

 private:
  // [内部方法] 打开文件并以O_DIRECT模式注册到GDS
  cudaError_t OpenAndRegisterFile(size_t vlog_idx);

  // [内部方法] 在指定流上处理单个VLog
  cudaError_t ProcessSingleVLogAsync(size_t vlog_idx, int stream_idx);

  // [内部方法] 关闭并注销所有GDS文件
  cudaError_t CloseAndDeregisterFiles();

  // [内部方法] 清理所有per-vlog缓冲区
  cudaError_t CleanupBuffers();

  // ============ CUDA流 ============
  cudaStream_t worker_streams_[NUM_GC_WORKER_STREAMS];
  cudaStream_t sync_stream_;

  // ============ GPUGC引用 (非所有权) ============
  GPUGC* gpu_gc_;

  // ============ VLog队列 ============
  std::vector<GCVLogDescriptor> pending_vlogs_;

  // ============ Per-VLog缓冲区 ============
  std::vector<PerVLogBuffers> per_vlog_buffers_;

  // ============ 处理结果 ============
  std::vector<GCVLogResult> results_;

  // ============ GDS文件句柄 ============
  std::vector<GC_GDSFileHandle> gds_handles_;

  // ============ 完成事件(per-vlog) ============
  std::vector<cudaEvent_t> vlog_complete_events_;

  // ============ 统计信息 ============
  GCStats stats_;

  // ============ 状态 ============
  size_t max_vlog_count_;
  std::atomic<bool> running_{false};

  // ============ GDS状态 ============
  static bool gds_initialized_;
};

}  // namespace leveldb
