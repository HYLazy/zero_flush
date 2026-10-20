//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush V2：物化后端（materialize backend）抽象。
//
//  背景：V2 的写路径固定为「写组 → 分区 WAL（无 memtable flush 路径）」，
//  数据落盘后的唯一出路是**物化**：顺序整读该分区的封存代 → 按
//  (user_key asc, seq desc) 归并 → 生成 SST（与重叠 L1 融合）→ 单次
//  VersionEdit 安装。其中「归并排序」是主机 CPU 的大头
//  （`zf.materialize_sort_micros` 可观测），也是设备侧唯一需要承担的计算。
//
//  本文件把这**一个阶段**抽成可替换执行体：
//    - HostBackend：现有主机实现（memcmp 打包排序 + 原地重排），V2 默认；
//    - CsdBackend ：设备/加速器路径。设备只做排序（数据由主机喂入、输出流
//      由主机写成 SST），与 M3_DESIGN §10.2 的 "L1 卸载（排序卸载）" 一致；
//      设备不可用/不支持时**回退 host** 并计数，绝不因设备而拒绝打开 DB。
//
//  分层约定（与 zf_request.h 文件头一致）：本抽象只描述**执行体**，
//  不参与「该不该做这个任务」的策略决策（MaterializeDecision 仍由
//  ZfMaterializeJob::PlanLocked 在主机侧决定）；持久化权威（VersionEdit /
//  fence / 账本 / 回收）全部留在主机。
//
//  输入扫描（含 generation 读引用、WAL 完整性校验）、范围断言、定层、
//  输出文件号、表属性与 vlog 语义都不在后端内——后端只重排给定的
//  (internal key, value) 向量，因此 host 与设备两条路**必然**产出同一批
//  条目，只有顺序可能由不同执行体产生。

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace rocksdb {
class Comparator;
class InternalKeyComparator;
class Logger;
}  // namespace rocksdb

namespace zeroflush {

class ZeroFlushOptions;  // zeroflush_db.h（避免头文件环）

// 选项取值（字符串在 Open 时规范化成枚举；未知取值 ⇒ 启动报 InvalidArgument）。
enum class MaterializeBackendKind : uint8_t {
  kHost = 0,  // 主机 CPU 路径（默认）
  kCsd = 1,   // 设备/加速器路径（不可用 ⇒ 回退 host + 计数）
};

// 设备槽位策略（对应 CSD-CoKV 的三种 policy；ZF 用显式设备列表，不靠
// cf_paths 顺序隐式对齐）。
enum class CsdSlotPolicy : uint8_t {
  kFreeSlot = 0,  // 有任一空闲槽即提交（其余任务排队）
  kAnyIdle = 1,   // 任意设备空闲即可
  kAllFree = 2,   // 全部设备空闲才提交（否则回退 host）
};

enum class CsdFallbackPolicy : uint8_t {
  kHost = 0,  // 设备不可用/失败 ⇒ 回退 host（默认；M3_DESIGN 要求）
  kFail = 1,  // 设备不可用 ⇒ 直接失败（仅用于设备路径的严格验收）
};

// 归并排序阶段的输入。keys/values 由调用方（ZfMaterializeJob）持有，
// 后端**就地重排**（move 语义；value 不拷贝）。
struct SortStageInput {
  std::vector<std::string>* keys = nullptr;    // internal key（user key + 8B 尾）
  std::vector<std::string>* values = nullptr;  // 与 keys 一一对应
  const rocksdb::Comparator* user_comparator = nullptr;
  const rocksdb::InternalKeyComparator* internal_comparator = nullptr;
  uint32_t part_id = 0;
  uint64_t input_bytes = 0;  // 该分区本批扫描到的 key+value 字节（诊断/计费）
  uint32_t input_gens = 0;   // 参与本批归并的封存代数
};

struct SortStageResult {
  // true = keys/values 已按 internal key 全序排好，调用方可构造**不再比较**
  // 的迭代器（host 的打包 memcmp 路径 / 设备返回的全序流）；
  // false = 调用方须让迭代器自行按 internal comparator 比较（如非字节序
  // user comparator，或后端只做了部分排序）。
  bool fully_ordered = false;
  // 本阶段在后端内的耗时（设备路径用于计费；host 路径由调用方的
  // zf.materialize_sort_micros 统计，此处填 0 不改既有指标）。
  uint64_t backend_micros = 0;
  // 是否由设备/加速器执行（诊断用：区分"选了 csd 但回退了 host"）。
  bool ran_on_device = false;
};

class MaterializeBackend {
 public:
  virtual ~MaterializeBackend() = default;

  virtual const char* Name() const = 0;
  virtual MaterializeBackendKind kind() const = 0;

  // 日志句柄（DB 的 info log；Open 之后注入，设备回退/探测失败时打 WARN）。
  virtual void SetLogger(rocksdb::Logger* log) { (void)log; }

  // 后端可用性探测。失败语义由调用方的 csd_fallback_policy 决定；
  // 默认（kHost）是"回退 host + 计数 + WARN"，不拒绝打开 DB。
  virtual rocksdb::Status Probe() = 0;

  // 归并排序阶段。**必须**要么完整完成、要么返回非 OK（调用方据此回退
  // host 重做，绝不接受半排序的结果）。
  virtual rocksdb::Status RunSortStage(const SortStageInput& in,
                                      SortStageResult* out) = 0;
};

// 后端计数（GetProperty 暴露；进程内累计，DB 级由 ZeroFlushContext 持有）。
struct MaterializeBackendStats {
  std::atomic<uint64_t> tasks{0};           // 后端执行成功的排序任务数
  std::atomic<uint64_t> device_tasks{0};    // 其中真正跑在设备上的
  std::atomic<uint64_t> micros{0};          // 后端内累计耗时
  std::atomic<uint64_t> bytes{0};           // 累计输入字节
  std::atomic<uint64_t> fallbacks{0};       // 回退 host 次数（含探测失败）
  std::atomic<uint64_t> probe_failures{0};  // Probe() 失败次数
};

// 工厂：按选项构造后端。无论选了什么，返回的对象都可直接使用：
//   - kind=kHost ⇒ HostBackend；
//   - kind=kCsd 且设备可用 ⇒ CsdBackend；
//   - kind=kCsd 且设备不可用 ⇒ HostBackend + probe_failures/fallbacks 计数
//     （fallback_policy=kFail 时返回 nullptr 并由调用方报错）。
// stats 为 nullptr 表示不计数。
std::shared_ptr<MaterializeBackend> CreateMaterializeBackend(
    const ZeroFlushOptions& zfo, MaterializeBackendStats* stats);

// 字符串 ⇄ 枚举（选项规范化用；未知取值返回 false）。
bool ParseMaterializeBackendKind(const std::string& s,
                                 MaterializeBackendKind* out);
bool ParseCsdSlotPolicy(const std::string& s, CsdSlotPolicy* out);
bool ParseCsdFallbackPolicy(const std::string& s, CsdFallbackPolicy* out);

}  // namespace zeroflush
