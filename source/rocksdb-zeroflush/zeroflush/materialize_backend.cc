//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush V2：物化后端实现（host CPU 路径 + CSD 设备路径接缝）。
//
//  三条执行路径（都只覆盖"归并排序"阶段，见 materialize_backend.h 头注释）：
//   1) HostBackend    —— 现有主机实现：Bytewise 时打包 memcmp 排序键 +
//                        O(n) 原地重排；非 Bytewise 由调用方迭代器比较。
//                        这是 V2 默认路径，也是设备路径的正确性参照物。
//   2) CsdSimBackend  —— 内建设备**仿真**（无硬件依赖）：把排序交给一个
//                        "设备槽位"计数 + 下标置换接口（与真设备同一
//                        ABI/调用形态），用于打通设备路径的接线、回退与
//                        等价性验收。它**不**声明加速能力（ran_on_device
//                         =true 仅表示"走了设备接口"）。
//   3) CsdPluginBackend —— 通过 `zf_csd_kernel_path` 指向的共享库加载真设备
//                        （C ABI，v1）。任何加载/探测/执行失败 ⇒ 回退 host
//                        + 计数（csd_fallback_policy=kFail 时才失败）。
//  默认构建不依赖 XRT/OpenCL/CUDA：真设备代码只出现在插件里（设计文档
//  §5.3 的硬性要求）。

#include "zeroflush/materialize_backend.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <numeric>
#include <sstream>
#include <thread>

#include "logging/logging.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "zeroflush/zeroflush_db.h"

#ifndef _WIN32
#include <dlfcn.h>
#endif

namespace zeroflush {

// ROCKS_LOG_* 宏在宏展开处要求 InfoLogLevel 可见（logging/logging.h 不
// include env.h）；与 wal_manager.cc 一致，这里显式引入。
using ROCKSDB_NAMESPACE::InfoLogLevel;

namespace {

uint64_t NowMicros() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

// ---------------------------------------------------------------------------
// 主机排序实现（自 materialize_job.cc 原样迁入；语义逐字不变）
// ---------------------------------------------------------------------------
// M4.6e：物化排序优化——Bytewise user comparator 时构造"memcmp 可排序"的
// 编码键（连续缓冲 + 偏移视图，无逐键堆分配）。编码 = 4B user key 长度
// （大端，保证变长 key 的"短者小"语义）+ user key + 8B ~(seq<<8|type)
// （逆序取反，使 seq 降序语义变为 memcmp 升序）。非 Bytewise 返回 false
// （调用方走 internal comparator 比较）。
bool BuildSortKeys(const std::vector<std::string>& keys,
                   const rocksdb::Comparator* ucmp, std::string* buf,
                   std::vector<size_t>* off) {
  if (ucmp->Name() != rocksdb::BytewiseComparator()->Name()) {
    return false;
  }
  buf->clear();
  off->clear();
  off->reserve(keys.size());
  buf->reserve(keys.size() * 30);
  for (const auto& ik : keys) {
    const size_t n = ik.size() - 8;  // user key 长度（尾 8B 为 seq/type）
    off->push_back(buf->size());
    buf->push_back(static_cast<char>(n >> 24));
    buf->push_back(static_cast<char>(n >> 16));
    buf->push_back(static_cast<char>(n >> 8));
    buf->push_back(static_cast<char>(n));
    buf->append(ik.data(), n);
    for (int i = 7; i >= 0; --i) {
      buf->push_back(~ik[ik.size() - 8 + i]);
    }
  }
  return true;
}

// 按打包编码键（memcmp 序）重排 keys/values（string 移动，O(n)）。
void ApplyPermutation(std::vector<std::string>* keys,
                      std::vector<std::string>* values,
                      const std::vector<uint32_t>& order) {
  std::vector<std::string> sk, sv;
  sk.reserve(keys->size());
  sv.reserve(values->size());
  for (uint32_t i : order) {
    sk.push_back(std::move((*keys)[i]));
    sv.push_back(std::move((*values)[i]));
  }
  *keys = std::move(sk);
  *values = std::move(sv);
}

// 打包键的全序（返回下标置换）；供 host 与设备仿真共用同一排序语义。
std::vector<uint32_t> PackedSortOrder(const std::vector<std::string>& keys,
                                      const std::string& buf,
                                      const std::vector<size_t>& off) {
  std::vector<uint32_t> idx(keys.size());
  std::iota(idx.begin(), idx.end(), 0u);
  const size_t buf_size = buf.size();
  std::sort(idx.begin(), idx.end(), [&](uint32_t a, uint32_t b) {
    const char* pa = buf.data() + off[a];
    const char* pb = buf.data() + off[b];
    const size_t la = (static_cast<size_t>(a) + 1 < off.size() ? off[a + 1]
                                                              : buf_size) -
                      off[a];
    const size_t lb = (static_cast<size_t>(b) + 1 < off.size() ? off[b + 1]
                                                              : buf_size) -
                      off[b];
    const size_t m = la < lb ? la : lb;
    const int r = std::memcmp(pa, pb, m);
    return r < 0 || (r == 0 && la < lb);
  });
  return idx;
}

// ---------------------------------------------------------------------------
// 设备（CSD）侧 C ABI：v1
// ---------------------------------------------------------------------------
// 契约（S1/S2/S5 的最小子集）：
//   - 输入：internal key 数组（user key + 8B 小端 seq/type 尾）与 value 数组，
//     集合**不得被设备增删**（只允许重排）；
//   - 输出：order_out = 下标置换（长度 num_records），语义
//     (user_key asc, seq desc) —— 即 internal key 全序；
//   - 设备**不**做持久化、不产出 SST、不决定层（主机负责）；
//   - 非字节序 user comparator 不进入设备路径（主机侧回退）。
extern "C" {
#define ZF_CSD_PLUGIN_ABI_VERSION 1u

typedef struct ZfCsdSortRequestV1 {
  uint32_t abi_version;
  uint32_t part_id;
  uint64_t num_records;
  const char* const* keys;    // internal keys
  const char* const* values;  // 与 keys 一一对应（可为 nullptr）
  uint64_t input_bytes;
  uint32_t input_gens;
} ZfCsdSortRequestV1;

typedef struct ZfCsdPluginV1 {
  uint32_t abi_version;  // 必须 == ZF_CSD_PLUGIN_ABI_VERSION
  const char* name;
  void* impl;
  int (*probe)(void* impl);  // 0 = 可用
  int (*sort)(void* impl, const ZfCsdSortRequestV1* req, uint32_t* order_out,
              uint64_t* out_nanos);  // 0 = 成功
  const char* (*last_error)(void* impl);
} ZfCsdPluginV1;

typedef ZfCsdPluginV1* (*ZfCsdPluginCreateFn)(void);
}  // extern "C"

// 设备提供者（主机侧视图）：Plugin 与内建仿真实现同一接口。
class ZfCsdProvider {
 public:
  virtual ~ZfCsdProvider() = default;
  virtual const char* Name() const = 0;
  virtual bool Probe(std::string* why) = 0;
  virtual bool Sort(const SortStageInput& in, std::vector<uint32_t>* order,
                    std::string* why) = 0;
};

// 内建仿真：同一 ABI 形态（下标置换），无硬件依赖。
class ZfCsdSimProvider : public ZfCsdProvider {
 public:
  explicit ZfCsdSimProvider(uint32_t max_concurrent)
      : max_concurrent_(std::max<uint32_t>(1, max_concurrent)) {}
  const char* Name() const override { return "csd-sim"; }
  bool Probe(std::string* why) override {
    (void)why;
    return true;
  }
  bool Sort(const SortStageInput& in, std::vector<uint32_t>* order,
            std::string* why) override {
    // 槽位（与真设备一致的准入形态；kFreeSlot 下等待由调用方计）。
    const uint32_t busy = busy_.fetch_add(1, std::memory_order_acq_rel);
    if (busy >= max_concurrent_) {
      busy_.fetch_sub(1, std::memory_order_acq_rel);
      if (why != nullptr) {
        *why = "no free device slot (sim)";
      }
      return false;
    }
    struct SlotGuard {
      std::atomic<uint32_t>* b;
      ~SlotGuard() { b->fetch_sub(1, std::memory_order_acq_rel); }
    } guard{&busy_};
    std::string buf;
    std::vector<size_t> off;
    if (!BuildSortKeys(*in.keys, in.user_comparator, &buf, &off)) {
      if (why != nullptr) {
        *why = "non-bytewise comparator unsupported on device";
      }
      return false;
    }
    *order = PackedSortOrder(*in.keys, buf, off);
    return true;
  }

 private:
  std::atomic<uint32_t> busy_{0};
  uint32_t max_concurrent_ = 1;
};

// 共享库插件（真设备）：dlopen + ABI 校验 + probe + sort。
class ZfCsdPluginProvider : public ZfCsdProvider {
 public:
  explicit ZfCsdPluginProvider(const std::string& path) : path_(path) {}
  ~ZfCsdPluginProvider() override {
#ifndef _WIN32
    if (plugin_.handle != nullptr) {
      dlclose(plugin_.handle);
    }
#endif
  }
  const char* Name() const override {
    return plugin_.v1 != nullptr && plugin_.v1->name != nullptr
               ? plugin_.v1->name
               : "csd-plugin";
  }
  bool Probe(std::string* why) {
#ifndef _WIN32
    void* h = dlopen(path_.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (h == nullptr) {
      if (why != nullptr) {
        *why = std::string("dlopen failed: ") +
               (dlerror() != nullptr ? dlerror() : "unknown");
      }
      return false;
    }
    auto* create =
        reinterpret_cast<ZfCsdPluginCreateFn>(dlsym(h, "zf_csd_plugin_create"));
    if (create == nullptr) {
      if (why != nullptr) *why = "missing symbol zf_csd_plugin_create";
      dlclose(h);
      return false;
    }
    ZfCsdPluginV1* v1 = create();
    if (v1 == nullptr || v1->abi_version != ZF_CSD_PLUGIN_ABI_VERSION) {
      if (why != nullptr) *why = "plugin ABI version mismatch";
      dlclose(h);
      return false;
    }
    plugin_.handle = h;
    plugin_.v1 = v1;
    const int rc = v1->probe != nullptr ? v1->probe(v1->impl) : 0;
    if (rc != 0) {
      if (why != nullptr) {
        *why = "plugin probe() returned " + std::to_string(rc);
      }
      return false;
    }
    return true;
#else
    if (why != nullptr) *why = "plugin backend unsupported on this platform";
    return false;
#endif
  }
  bool Sort(const SortStageInput& in, std::vector<uint32_t>* order,
            std::string* why) {
    if (plugin_.v1 == nullptr || plugin_.v1->sort == nullptr) {
      if (why != nullptr) *why = "plugin sort() missing";
      return false;
    }
    std::vector<const char*> kptr, vptr;
    kptr.reserve(in.keys->size());
    for (const auto& k : *in.keys) kptr.push_back(k.c_str());
    if (in.values != nullptr) {
      vptr.reserve(in.values->size());
      for (const auto& v : *in.values) vptr.push_back(v.c_str());
    }
    ZfCsdSortRequestV1 req;
    req.abi_version = ZF_CSD_PLUGIN_ABI_VERSION;
    req.part_id = in.part_id;
    req.num_records = kptr.size();
    req.keys = kptr.data();
    req.values = vptr.empty() ? nullptr : vptr.data();
    req.input_bytes = in.input_bytes;
    req.input_gens = in.input_gens;
    order->assign(kptr.size(), 0u);
    uint64_t nanos = 0;
    const int rc = plugin_.v1->sort(plugin_.v1->impl, &req, order->data(), &nanos);
    if (rc != 0) {
      if (why != nullptr) {
        *why = std::string("plugin sort() failed rc=") + std::to_string(rc);
        if (plugin_.v1->last_error != nullptr) {
          const char* e = plugin_.v1->last_error(plugin_.v1->impl);
          if (e != nullptr) *why += std::string(": ") + e;
        }
      }
      return false;
    }
    // 置换合法性校验（设备产物必须通过校验才可使用）：长度、取值范围、
    // 互异（O(n) 位图）。任何越界/重复 ⇒ 判任务失败 ⇒ 回退 host。
    if (order->size() != kptr.size()) {
      if (why != nullptr) *why = "plugin order length mismatch";
      return false;
    }
    {
      std::vector<bool> seen(kptr.size(), false);
      for (uint32_t i : *order) {
        if (i >= kptr.size() || seen[i]) {
          if (why != nullptr) *why = "plugin order is not a permutation";
          return false;
        }
        seen[i] = true;
      }
    }
    return true;
  }

 private:
  std::string path_;
  struct {
    void* handle = nullptr;
    ZfCsdPluginV1* v1 = nullptr;
  } plugin_;
};

// ---------------------------------------------------------------------------
// 后端：host / csd
// ---------------------------------------------------------------------------
class HostBackend : public MaterializeBackend {
 public:
  const char* Name() const override { return "host"; }
  MaterializeBackendKind kind() const override {
    return MaterializeBackendKind::kHost;
  }
  rocksdb::Status Probe() override { return rocksdb::Status::OK(); }
  rocksdb::Status RunSortStage(const SortStageInput& in,
                               SortStageResult* out) override {
    out->fully_ordered = false;
    out->ran_on_device = false;
    out->backend_micros = 0;  // host 耗时由调用方 zf.materialize_sort_micros 统计
    std::string buf;
    std::vector<size_t> off;
    if (!BuildSortKeys(*in.keys, in.user_comparator, &buf, &off)) {
      return rocksdb::Status::OK();  // 非 Bytewise：迭代器自行比较
    }
    ApplyPermutation(in.keys, in.values, PackedSortOrder(*in.keys, buf, off));
    out->fully_ordered = true;
    return rocksdb::Status::OK();
  }
};

// CSD 后端：把排序阶段交给设备；设备不可用/失败 ⇒ 记一次回退并交回 host。
// 策略语义：
//   - Probe() 失败（设备缺失/ABI 不符/probe 拒绝）⇒ 每次任务都尝试一次、
//     每任务记一次 fallback（fail 策略下 Probe 失败让 Open 直接失败）；
//   - 槽位不可得 ⇒ kFreeSlot 等待至多 slot_wait_us 后回退；kAnyIdle/kAllFree
//     立即回退（计数）；
//   - 非 Bytewise comparator ⇒ 设备路径不支持 ⇒ 回退。
class CsdBackend : public MaterializeBackend {
 public:
  CsdBackend(std::unique_ptr<ZfCsdProvider> provider, CsdSlotPolicy slot_policy,
             CsdFallbackPolicy fallback_policy, uint64_t slot_wait_us,
             uint32_t max_input_gens,
             std::shared_ptr<std::atomic<bool>> device_ok,
             MaterializeBackendStats* stats, rocksdb::Logger* log)
      : provider_(std::move(provider)),
        slot_policy_(slot_policy),
        fallback_policy_(fallback_policy),
        slot_wait_us_(slot_wait_us),
        max_input_gens_(max_input_gens),
        device_ok_(std::move(device_ok)),
        stats_(stats),
        log_(log) {}

  const char* Name() const override { return provider_->Name(); }
  MaterializeBackendKind kind() const override {
    return MaterializeBackendKind::kCsd;
  }
  void SetLogger(rocksdb::Logger* log) override { log_ = log; }
  rocksdb::Status Probe() override {
    std::string why;
    if (!provider_->Probe(&why)) {
      if (stats_ != nullptr) {
        stats_->probe_failures.fetch_add(1, std::memory_order_relaxed);
      }
      if (log_ != nullptr) {
        ROCKS_LOG_WARN(log_,
                       "ZeroFlush CSD backend '%s' unavailable (%s); "
                       "falling back to host materialize backend",
                       provider_->Name(), why.c_str());
      }
      return rocksdb::Status::NotSupported(why);
    }
    return rocksdb::Status::OK();
  }

  rocksdb::Status RunSortStage(const SortStageInput& in,
                               SortStageResult* out) override {
    out->fully_ordered = false;
    out->ran_on_device = false;
    out->backend_micros = 0;
    // 设备端口上限：输入代数超过设备可接收的文件数 ⇒ 回退 host（对应
    // 设计文档 §1.1 "准入判定前置"：输入文件数 ≤ max_input_files）。
    if (max_input_gens_ > 0 && in.input_gens > max_input_gens_) {
      return Fallback(in, out, "input gens " + std::to_string(in.input_gens) +
                                   " exceeds device port limit " +
                                   std::to_string(max_input_gens_));
    }
    std::string why;
    if (!device_ok_->load(std::memory_order_acquire) ||
        !provider_->Probe(&why)) {
      if (stats_ != nullptr) {
        stats_->probe_failures.fetch_add(1, std::memory_order_relaxed);
      }
      return Fallback(in, out, "probe failed: " + why);
    }
    const uint64_t t0 = NowMicros();
    std::vector<uint32_t> order;
    bool ok = provider_->Sort(in, &order, &why);
    if (!ok && slot_policy_ == CsdSlotPolicy::kFreeSlot && slot_wait_us_ > 0) {
      // 有界等待：设备槽位释放后重试（CoKV 用 10ms 静默放弃；这里显式
      // 等待 + 显式计数，行为可观测）。
      const uint64_t deadline = t0 + slot_wait_us_;
      while (!ok && NowMicros() < deadline) {
        std::this_thread::sleep_for(std::chrono::microseconds(200));
        ok = provider_->Sort(in, &order, &why);
      }
    }
    if (!ok) {
      return Fallback(in, out, "device sort failed: " + why);
    }
    ApplyPermutation(in.keys, in.values, order);
    out->fully_ordered = true;
    out->ran_on_device = true;
    out->backend_micros = NowMicros() - t0;
    if (stats_ != nullptr) {
      stats_->tasks.fetch_add(1, std::memory_order_relaxed);
      stats_->device_tasks.fetch_add(1, std::memory_order_relaxed);
      stats_->micros.fetch_add(out->backend_micros, std::memory_order_relaxed);
      stats_->bytes.fetch_add(in.input_bytes, std::memory_order_relaxed);
    }
    return rocksdb::Status::OK();
  }

 private:
  // 回退 host：按 host 语义完成排序（调用方看到的是成功；设备侧缺口由
  // fallbacks/probe_failures 计数与日志暴露）。fallback_policy=kFail 时
  // 才把设备不可用升级为任务失败。
  rocksdb::Status Fallback(const SortStageInput& in, SortStageResult* out,
                           const std::string& why) {
    if (stats_ != nullptr) {
      stats_->fallbacks.fetch_add(1, std::memory_order_relaxed);
    }
    if (fallback_policy_ == CsdFallbackPolicy::kFail) {
      return rocksdb::Status::NotSupported(
          "ZeroFlush CSD backend required but unavailable: " + why);
    }
    if (log_ != nullptr) {
      ROCKS_LOG_WARN(log_, "ZeroFlush CSD backend fallback to host: %s",
                     why.c_str());
    }
    HostBackend host;
    return host.RunSortStage(in, out);
  }

  std::unique_ptr<ZfCsdProvider> provider_;
  CsdSlotPolicy slot_policy_;
  CsdFallbackPolicy fallback_policy_;
  uint64_t slot_wait_us_ = 0;
  uint32_t max_input_gens_ = 0;  // 0 = 不限
  std::shared_ptr<std::atomic<bool>> device_ok_;
  MaterializeBackendStats* stats_ = nullptr;
  rocksdb::Logger* log_;
};

}  // namespace

bool ParseMaterializeBackendKind(const std::string& s,
                                 MaterializeBackendKind* out) {
  if (s == "host") {
    *out = MaterializeBackendKind::kHost;
    return true;
  }
  if (s == "csd") {
    *out = MaterializeBackendKind::kCsd;
    return true;
  }
  return false;
}

bool ParseCsdSlotPolicy(const std::string& s, CsdSlotPolicy* out) {
  if (s == "free_slot") {
    *out = CsdSlotPolicy::kFreeSlot;
    return true;
  }
  if (s == "any_idle") {
    *out = CsdSlotPolicy::kAnyIdle;
    return true;
  }
  if (s == "all_free") {
    *out = CsdSlotPolicy::kAllFree;
    return true;
  }
  return false;
}

bool ParseCsdFallbackPolicy(const std::string& s, CsdFallbackPolicy* out) {
  if (s == "host") {
    *out = CsdFallbackPolicy::kHost;
    return true;
  }
  if (s == "fail") {
    *out = CsdFallbackPolicy::kFail;
    return true;
  }
  return false;
}

std::shared_ptr<MaterializeBackend> CreateMaterializeBackend(
    const ZeroFlushOptions& zfo, MaterializeBackendStats* stats) {
  if (zfo.materialize_backend != MaterializeBackendKind::kCsd) {
    return std::make_shared<HostBackend>();
  }
  // 设备来源二选一（显式，不隐式对齐）：
  //   ① `zf_csd_kernel_path` 指向的共享库（真设备，C ABI v1）；
  //   ② 设备列表含 "sim" ⇒ 内建仿真（打通接线/回退/等价性，不含加速承诺）。
  bool want_sim = false;
  for (const auto& d : zfo.csd_devices) {
    if (d == "sim") {
      want_sim = true;
    }
  }
  std::unique_ptr<ZfCsdProvider> provider;
  if (!zfo.csd_kernel_path.empty()) {
    provider.reset(new ZfCsdPluginProvider(zfo.csd_kernel_path));
  } else if (want_sim) {
    provider.reset(new ZfCsdSimProvider(zfo.csd_max_input_files));
  }
  auto device_ok = std::make_shared<std::atomic<bool>>(false);
  if (provider == nullptr) {
    if (stats != nullptr) {
      stats->probe_failures.fetch_add(1, std::memory_order_relaxed);
    }
    if (zfo.csd_fallback_policy == CsdFallbackPolicy::kFail) {
      return nullptr;  // 调用方（Open）报错：设备路径被显式要求但不可用
    }
    // 无可用设备：仍返回 host 后端（属性可见 probe_failure 计数）。
    if (stats != nullptr) {
      stats->fallbacks.fetch_add(1, std::memory_order_relaxed);
    }
    return std::make_shared<HostBackend>();
  }
  auto backend = std::make_shared<CsdBackend>(
      std::move(provider), zfo.csd_slot_policy, zfo.csd_fallback_policy,
      zfo.csd_slot_wait_micros, zfo.csd_max_input_files, device_ok, stats,
      nullptr /* log */);
  if (!backend->Probe().ok()) {
    if (zfo.csd_fallback_policy == CsdFallbackPolicy::kFail) {
      return nullptr;
    }
    return std::make_shared<HostBackend>();
  }
  device_ok->store(true, std::memory_order_release);
  return backend;
}

}  // namespace zeroflush
