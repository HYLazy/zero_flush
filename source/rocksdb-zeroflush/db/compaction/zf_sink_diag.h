// ZeroFlush step 0a 诊断（临时，仅用于定位"窄区间 sink 构造失败"）。
//
// 背景：受控实验（archive/zeroflush_m5_perf/sink_tuning/REPORT.md）发现
// 窄区间下沉臂的 compaction 完成数为 0，而失败日志是 DEBUG 级所以静默。
// 本文件把 picker 的 nullptr 返回点分类回传给调用方（MaybeScheduleZfSink
// 与 picker 在同一线程同步调用，故用 thread_local 传递，不改任何签名），
// 并由调用方计数、限频打印、经 GetProperty 暴露给 db_bench 汇总。
//
// 无行为改动：只写一个 thread_local int 与若干原子计数。
// 0a 结束后可整体删除（连同 picker / db_impl 中的调用点）。
#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <set>
#include <string>

namespace ROCKSDB_NAMESPACE {
namespace zf_sink_diag {

enum Reason : int {
  kOk = 0,             // 未失败（或 picker 未走到已知返回点）
  kInputsEmpty,        // 输入层该 range 无重叠文件
  kShrunkEmpty,        // bottommost 过滤后输入集为空
  kExpandCleanCut,     // ExpandInputsToCleanCut 失败（相邻文件被占）
  kSetupOtherInputs,   // SetupOtherInputs 失败（输出层扩展被占）
  kFilesInCompaction,  // AreFilesInCompaction（输入/输出文件正在 compaction）
  kRangeOverlap,       // FilesRangeOverlapWithCompaction（输出范围与在跑任务重叠）
};

inline const char* ReasonName(int r) {
  switch (r) {
    case kOk: return "ok/unknown";
    case kInputsEmpty: return "inputs_empty";
    case kShrunkEmpty: return "shrunk_empty";
    case kExpandCleanCut: return "expand_clean_cut_fail";
    case kSetupOtherInputs: return "setup_other_inputs_fail";
    case kFilesInCompaction: return "files_in_compaction";
    case kRangeOverlap: return "output_range_overlap";
    default: return "?";
  }
}

// picker 写入 / 调用方读取（同一线程，调用栈内传递）。
inline int& TReason() {
  static thread_local int r = kOk;
  return r;
}

struct Counters {
  std::atomic<uint64_t> calls{0};      // MaybeScheduleZfSink 进入次数
  std::atomic<uint64_t> taken{0};      // 取出的请求数
  std::atomic<uint64_t> no_l1_file{0}; // 窄模式下 L1 无相交文件（原静默 continue）
  std::atomic<uint64_t> pick_ok{0};    // 构造成功并已调度
  std::atomic<uint64_t> pick_inputs_empty{0};
  std::atomic<uint64_t> pick_shrunk_empty{0};
  std::atomic<uint64_t> pick_expand_clean_cut{0};
  std::atomic<uint64_t> pick_setup_other_inputs{0};
  std::atomic<uint64_t> pick_files_in_compaction{0};
  std::atomic<uint64_t> pick_range_overlap{0};
  std::atomic<uint64_t> pick_unknown{0};
  std::atomic<uint64_t> l2_clear_rounds{0};
  std::atomic<uint64_t> level_guard{0};
  std::atomic<uint64_t> requeue_left{0};
  std::atomic<uint64_t> requeue_right{0};
  // 构造失败时调用方观察到的上下文（累加量 / 样本数）
  std::atomic<uint64_t> fail_l1_files{0};       // 相交 L1 文件数累加
  std::atomic<uint64_t> fail_l1_busy_files{0};  // 其中 being_compacted 累加
  std::atomic<uint64_t> fail_l1_busy_cases{0};  // 至少一个相交文件被占的次数
  std::atomic<uint64_t> fail_samples{0};
  // 占用者归属：被 ZF 融合（kMergeBase，注册期持 being_compacted）占用 vs
  // 其它（ZF 模式下 L1 层只可能是另一条在飞的下沉）。
  std::atomic<uint64_t> fail_holder_merge{0};
  std::atomic<uint64_t> fail_holder_sink{0};
  // 合批策略（ZF_SINK_BATCH=1）：每轮选择的批次构成与估算成本。
  std::atomic<uint64_t> batch_rounds{0};
  std::atomic<uint64_t> batch_l1_files{0};
  std::atomic<uint64_t> batch_l1_bytes{0};
  std::atomic<uint64_t> batch_l2_bytes{0};
  // 构造成功的任务：真实输入集合（原生 clean-cut 之后），用于核对"请求
  // 区间 → 实际参与集合"的扩展幅度。
  std::atomic<uint64_t> built_n{0};
  std::atomic<uint64_t> built_l1_files{0};
  std::atomic<uint64_t> built_l1_bytes{0};
  std::atomic<uint64_t> built_l2_files{0};
  std::atomic<uint64_t> built_l2_bytes{0};
  // 输入放大（step 0b-2）：请求区间的初始 L2 overlap vs 实际 L2 输入集合。
  std::atomic<uint64_t> built_req_l2_bytes{0};
  std::atomic<uint64_t> built_req_l2_files{0};
};

inline Counters& C() {
  static Counters c;
  return c;
}

// 融合（kMergeBase）当前持有 being_compacted 的 existing 文件号登记：
// 注册在 RegisterCompaction 之后，解除在 MarkFilesBeingCompacted(false)
// 之后（unregister 不解除 mark，故以 mark 的窗口为准）。
inline std::mutex& MergeHoldMu() {
  static std::mutex m;
  return m;
}
inline std::set<uint64_t>& MergeHoldSet() {
  static std::set<uint64_t> s;
  return s;
}
inline void MergeHoldAdd(uint64_t n) {
  std::lock_guard<std::mutex> l(MergeHoldMu());
  MergeHoldSet().insert(n);
}
inline void MergeHoldDel(uint64_t n) {
  std::lock_guard<std::mutex> l(MergeHoldMu());
  MergeHoldSet().erase(n);
}
inline bool MergeHolds(uint64_t n) {
  std::lock_guard<std::mutex> l(MergeHoldMu());
  return MergeHoldSet().count(n) > 0;
}
inline size_t MergeHoldSize() {
  std::lock_guard<std::mutex> l(MergeHoldMu());
  return MergeHoldSet().size();
}

// 构造成功时记录真实输入集合（原生 clean-cut 之后）。
inline void NoteSinkOverlap(uint64_t req_l2b, uint64_t req_l2n, uint64_t in_l2b,
                            uint64_t in_l2n) {
  Counters& c = C();
  c.built_req_l2_bytes.fetch_add(req_l2b, std::memory_order_relaxed);
  c.built_req_l2_files.fetch_add(req_l2n, std::memory_order_relaxed);
}

inline void NoteSinkBuilt(uint64_t l1n, uint64_t l1b, uint64_t l2n, uint64_t l2b) {
  Counters& c = C();
  c.built_n.fetch_add(1, std::memory_order_relaxed);
  c.built_l1_files.fetch_add(l1n, std::memory_order_relaxed);
  c.built_l1_bytes.fetch_add(l1b, std::memory_order_relaxed);
  c.built_l2_files.fetch_add(l2n, std::memory_order_relaxed);
  c.built_l2_bytes.fetch_add(l2b, std::memory_order_relaxed);
}

inline void NotePickFail(int reason) {
  switch (reason) {
    case kInputsEmpty:
      C().pick_inputs_empty.fetch_add(1, std::memory_order_relaxed);
      break;
    case kShrunkEmpty:
      C().pick_shrunk_empty.fetch_add(1, std::memory_order_relaxed);
      break;
    case kExpandCleanCut:
      C().pick_expand_clean_cut.fetch_add(1, std::memory_order_relaxed);
      break;
    case kSetupOtherInputs:
      C().pick_setup_other_inputs.fetch_add(1, std::memory_order_relaxed);
      break;
    case kFilesInCompaction:
      C().pick_files_in_compaction.fetch_add(1, std::memory_order_relaxed);
      break;
    case kRangeOverlap:
      C().pick_range_overlap.fetch_add(1, std::memory_order_relaxed);
      break;
    default:
      C().pick_unknown.fetch_add(1, std::memory_order_relaxed);
      break;
  }
}

inline uint64_t Load(const std::atomic<uint64_t>& a) {
  return a.load(std::memory_order_relaxed);
}

// 单行摘要（经 GetProperty("rocksdb.zeroflush.sink_diag") 暴露）。
inline std::string Summary() {
  Counters& c = C();
  char buf[640];
  snprintf(buf, sizeof(buf),
           "calls=%llu taken=%llu no_l1_file=%llu pick_ok=%llu | fail: "
           "inputs_empty=%llu shrunk_empty=%llu expand_clean_cut=%llu "
           "setup_other=%llu in_compaction=%llu range_overlap=%llu "
           "unknown=%llu | l2_clear_rounds=%llu level_guard=%llu "
           "requeue_l=%llu requeue_r=%llu | fail_ctx: samples=%llu "
           "l1_files=%llu l1_busy_files=%llu l1_busy_cases=%llu | holder: "
           "merge=%llu sink=%llu merge_hold_now=%zu | batch: rounds=%llu "
           "l1_files=%llu l1_bytes=%llu l2_bytes=%llu | built: n=%llu "
           "l1=%llu(%llu MB) l2=%llu(%llu MB) l2_req=%llu(%llu MB)",
           (unsigned long long)Load(c.calls), (unsigned long long)Load(c.taken),
           (unsigned long long)Load(c.no_l1_file),
           (unsigned long long)Load(c.pick_ok),
           (unsigned long long)Load(c.pick_inputs_empty),
           (unsigned long long)Load(c.pick_shrunk_empty),
           (unsigned long long)Load(c.pick_expand_clean_cut),
           (unsigned long long)Load(c.pick_setup_other_inputs),
           (unsigned long long)Load(c.pick_files_in_compaction),
           (unsigned long long)Load(c.pick_range_overlap),
           (unsigned long long)Load(c.pick_unknown),
           (unsigned long long)Load(c.l2_clear_rounds),
           (unsigned long long)Load(c.level_guard),
           (unsigned long long)Load(c.requeue_left),
           (unsigned long long)Load(c.requeue_right),
           (unsigned long long)Load(c.fail_samples),
           (unsigned long long)Load(c.fail_l1_files),
           (unsigned long long)Load(c.fail_l1_busy_files),
           (unsigned long long)Load(c.fail_l1_busy_cases),
           (unsigned long long)Load(c.fail_holder_merge),
           (unsigned long long)Load(c.fail_holder_sink), MergeHoldSize(),
           (unsigned long long)Load(c.batch_rounds),
           (unsigned long long)Load(c.batch_l1_files),
           (unsigned long long)Load(c.batch_l1_bytes),
           (unsigned long long)Load(c.batch_l2_bytes),
           (unsigned long long)Load(c.built_n),
           (unsigned long long)Load(c.built_l1_files),
           (unsigned long long)(Load(c.built_l1_bytes) >> 20),
           (unsigned long long)Load(c.built_l2_files),
           (unsigned long long)(Load(c.built_l2_bytes) >> 20),
           (unsigned long long)Load(c.built_req_l2_files),
           (unsigned long long)(Load(c.built_req_l2_bytes) >> 20));
  return std::string(buf);
}

}  // namespace zf_sink_diag
}  // namespace ROCKSDB_NAMESPACE
