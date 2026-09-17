//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  step 1d：单次失败 Get 的真实来源追踪（用户 2026-09-15 的有界任务）。
//
//  目标：把**同一次** Get 的索引侧与 LSM 侧证据串起来，字段对齐用户要求：
//    key / read_sequence
//    索引：logical_part(路由分区) / physical_part / gen / 命中 seq、type
//    LSM ：实际访问顺序 / level / file / 命中 seq、type
//    最终：返回来源 / seq / type
//
//  开关：ZF_DBG_GET=<v>[,<v>...]（user key 整数，键编码 = BE64(v) + 8×'0'，
//  与 tools/zf_keygen.h 的 KeyFromInt(v,16) 一致）。未设置时 Enabled()=false，
//  所有钩子零开销（一次静态初始化 + 一次比较）。
//
//  只读、无行为改动：不改变任何查找顺序、不改变返回结果。

#pragma once

#include <cstdio>
#include <string_view>
#include <mutex>
#include <unordered_map>
#include <functional>
#include <thread>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "rocksdb/slice.h"

namespace zeroflush {
namespace get_trace {

// 追踪目标键集合（惰性解析一次）。键编码与校验器/db_bench 一致：
// 前 8 字节大端整数，后 8 字节 ASCII '0' 填充。
inline const std::vector<std::string>& Keys() {
  static const std::vector<std::string> keys = [] {
    std::vector<std::string> out;
    const char* e = getenv("ZF_DBG_GET");
    if (e == nullptr || *e == '\0') {
      return out;
    }
    std::string s(e);
    size_t pos = 0;
    while (pos <= s.size()) {
      const size_t c = s.find(',', pos);
      const std::string tok =
          s.substr(pos, c == std::string::npos ? std::string::npos : c - pos);
      if (!tok.empty()) {
        const unsigned long long v = strtoull(tok.c_str(), nullptr, 10);
        std::string k(16, '0');
        for (int i = 0; i < 8; ++i) {
          k[7 - i] = static_cast<char>((v >> (8 * i)) & 0xFF);
        }
        out.push_back(std::move(k));
      }
      if (c == std::string::npos) {
        break;
      }
      pos = c + 1;
    }
    fprintf(stderr, "ZFDBG-get enabled keys=%zu (16B BE64+'0' 编码)\n",
            out.size());
    return out;
  }();
  return keys;
}

// 动态追踪键（校验器在**运行中**发现失败键时设置——活跃窗口的失败无法
// 预先用环境变量指定）。空 = 未设置。捕获失败那一刻的引擎内部状态。
inline std::string& DynamicKey() {
  static thread_local std::string k;
  return k;
}

inline void SetDynamicKey(const std::string& k) { DynamicKey() = k; }

inline bool Enabled() { return !Keys().empty() || !DynamicKey().empty(); }

inline bool Match(const ROCKSDB_NAMESPACE::Slice& user_key) {
  const std::string& dk = DynamicKey();
  if (!dk.empty() && user_key.size() == dk.size() &&
      memcmp(user_key.data(), dk.data(), dk.size()) == 0) {
    return true;
  }
  const auto& ks = Keys();
  if (ks.empty()) {
    return false;
  }
  for (const auto& k : ks) {
    if (user_key.size() == k.size() &&
        memcmp(user_key.data(), k.data(), k.size()) == 0) {
      return true;
    }
  }
  return false;
}

// 键的十进制标识（日志可读性：v=1234）。非本追踪键返回 -1。
inline long long KeyId(const ROCKSDB_NAMESPACE::Slice& user_key) {
  const auto& ks = Keys();
  for (size_t i = 0; i < ks.size(); ++i) {
    if (user_key.size() == ks[i].size() &&
        memcmp(user_key.data(), ks[i].data(), ks[i].size()) == 0) {
      return static_cast<long long>(i);  // 序号；真实 v 由调用方另行打印
    }
  }
  return -1;
}

// step 1ae（用户 2026-09-17）：**整次 Get 的关联标识 + 错误站点**。
// 顶层 Get 入口生成 get_id；每次内部重试（SuperVersion 刷新）递增 attempt_id；
// 每个 attempt 开始**清空**诊断状态（避免把上一次成功读取的线程局部日志当成
// 本次失败证据）；未经过的路径标 NOT_VISITED。
struct ReadErrorRecord {
  bool set = false;        // 本 attempt 是否记录过错误
  uint64_t get_id = 0;
  uint64_t attempt_id = 0;
  uint64_t thread_hash = 0;
  int site = -1;           // 唯一 error_site 编号（见各产生点）
  uint32_t part = 0;
  uint32_t gen = 0;
  uint64_t offset = 0;
  uint64_t want = 0;
  uint64_t got = 0;
  uint64_t parsed_total = 0;
  int cond = -1;           // 触发该判定的条件编号（0=读失败 1=长度不足 2=解析失败…）
  const char* status = "";
};

inline uint64_t& CurGetId() {
  static thread_local uint64_t v = 0;
  return v;
}
inline uint64_t& CurAttemptId() {
  static thread_local uint64_t v = 0;
  return v;
}
inline ReadErrorRecord& LastReadError() {
  static thread_local ReadErrorRecord r;
  return r;
}
inline uint64_t NextGetId() {
  static std::atomic<uint64_t> n{0};
  return n.fetch_add(1) + 1;
}
inline void BeginGet() {
  CurGetId() = NextGetId();
  CurAttemptId() = 0;
  LastReadError() = ReadErrorRecord();
}
inline void BeginAttempt() {
  ++CurAttemptId();
  LastReadError() = ReadErrorRecord();  // 每 attempt 清空（防跨次残留）
}
// step 1ai（用户 2026-09-17）：**错误站点环形记录**（8 条）——不再依赖"最新一条"，
// 避免后续成功读把失败现场覆盖掉。每条自带 get_id/attempt_id，可按 get_id 过滤打印。
inline ReadErrorRecord* ErrorSiteRing() {
  static thread_local ReadErrorRecord ring[8];
  return ring;
}
inline uint32_t& ErrorSiteRingPos() {
  static thread_local uint32_t p = 0;
  return p;
}
inline void NoteReadErrorSite(int site, uint32_t part, uint32_t gen,
                              uint64_t offset, uint64_t want, uint64_t got,
                              uint64_t parsed_total, int cond,
                              const char* status) {
  ReadErrorRecord& r = LastReadError();
  if (r.set) {
    return;  // 只记**首个**产生点（错误向上传播的源头）
  }
  r.set = true;
  r.get_id = CurGetId();
  r.attempt_id = CurAttemptId();
  r.thread_hash = static_cast<uint64_t>(
      std::hash<std::thread::id>{}(std::this_thread::get_id()));
  r.site = site;
  r.part = part;
  r.gen = gen;
  r.offset = offset;
  r.want = want;
  r.got = got;
  r.parsed_total = parsed_total;
  r.cond = cond;
  r.status = status;
  ReadErrorRecord* ring = ErrorSiteRing();
  uint32_t& pos = ErrorSiteRingPos();
  ring[pos % 8] = r;
  ++pos;
}

// step 1ag（用户 2026-09-17）：locator 的 A→B→C→D 四段对照。
//   A = Append 返回的 (part,gen,offset) + key/seq/type（append_id 关联）
//   B = 索引插入时收到的 locator 原始字节解码结果
//   C = 索引选择胜出后、调用方保存的 locator 字节解码结果
//   D = 实际读取使用的 (part,gen,offset) + 原始头字节 + 文件身份
// 仅诊断（ZF_DBG_LOCATOR=1 时启用），A 表有容量上限。
struct AppendSite {
  uint64_t append_id = 0;
  uint32_t part = 0;
  uint32_t gen = 0;
  uint64_t offset = 0;
  uint64_t seq = 0;
  int type = 0;
  uint64_t key_hash = 0;
};

inline bool LocatorDiagEnabled() {
  static const bool on = getenv("ZF_DBG_LOCATOR") != nullptr;
  return on;
}
inline std::unordered_map<uint64_t, AppendSite>& AppendSites() {
  static std::unordered_map<uint64_t, AppendSite> m;
  return m;
}
inline std::mutex& AppendSitesMu() {
  static std::mutex m;
  return m;
}
inline uint64_t& AppendIdSeq() {
  static std::atomic<uint64_t> n{0};
  return *reinterpret_cast<uint64_t*>(&n);  // 占位，下面用 NextAppendId
}
inline uint64_t NextAppendId() {
  static std::atomic<uint64_t> n{0};
  return n.fetch_add(1) + 1;
}
inline uint64_t KeyHash(const ROCKSDB_NAMESPACE::Slice& k) {
  return static_cast<uint64_t>(std::hash<std::string_view>{}(
      std::string_view(k.data(), k.size())));
}
inline uint64_t LocatorKey(uint32_t part, uint32_t gen, uint64_t off) {
  return (static_cast<uint64_t>(part) << 48) ^ (static_cast<uint64_t>(gen) << 32) ^
         off;
}
inline void NoteAppendSite(uint32_t part, uint32_t gen, uint64_t offset,
                           uint64_t seq, int type,
                           const ROCKSDB_NAMESPACE::Slice& key) {
  if (!LocatorDiagEnabled()) return;
  std::lock_guard<std::mutex> l(AppendSitesMu());
  auto& m = AppendSites();
  if (m.size() > 400000) return;  // 有界
  AppendSite a;
  a.append_id = NextAppendId();
  a.part = part;
  a.gen = gen;
  a.offset = offset;
  a.seq = seq;
  a.type = type;
  a.key_hash = KeyHash(key);
  m[LocatorKey(part, gen, offset)] = a;
}
inline bool LookupAppendSite(uint32_t part, uint32_t gen, uint64_t offset,
                             AppendSite* out) {
  std::lock_guard<std::mutex> l(AppendSitesMu());
  auto it = AppendSites().find(LocatorKey(part, gen, offset));
  if (it == AppendSites().end()) return false;
  if (out != nullptr) *out = it->second;
  return true;
}
// step 1al：候选访问执行记录（每个 attempt 清空）。
struct CandidateVisit {
  uint32_t part = 0;
  uint32_t order = 0;   // 访问序号（0 = 首个访问）
  bool hit = false;     // 该候选是否命中
  uint64_t seq = 0;     // 命中时的 seq
};
inline std::vector<CandidateVisit>& CandidateVisits() {
  static thread_local std::vector<CandidateVisit> v;
  return v;
}
inline void ClearCandidateVisits() { CandidateVisits().clear(); }

// step 1ar（用户 2026-09-17）：胜出候选因 **retiring** 无法取得读引用 ⇒ 整次选择作废、
// 需要**刷新读视图后完整重试**（不得继续尝试其它候选而返回更低版本）。
inline bool& RetiringRejectedTls() {
  static thread_local bool b = false;
  return b;
}
inline void SetRetiringRejected(bool v) { RetiringRejectedTls() = v; }
inline bool ConsumeRetiringRejected() {
  const bool b = RetiringRejectedTls();
  RetiringRejectedTls() = false;
  return b;
}

// step A-probe（测试专用）：**线程本地**跳过索引层开关——用 env 做开关时
// 每次 Get 前后 setenv/unsetenv 会与并发线程的 getenv 竞争（未定义行为，
// 实测表现为偶发读错误）。TLS 只影响调用线程，且生产路径仅一次 thread_local 读。
inline bool& SkipIndexTls() {
  static thread_local bool b = false;
  return b;
}
inline void SetSkipIndex(bool v) { SkipIndexTls() = v; }

inline std::atomic<uint64_t>& RetryReadViewNeeded() {
  static std::atomic<uint64_t> n{0};
  return n;
}
// step 1as 修正（压力组 5）：被拒候选的**身份**。只有"是否被拒"不足以决定后续行为：
//   · 该代**已安装**（Version fence 覆盖）⇒ LSM 就是同一 seq 数据的正确来源，
//     读取应回落 LSM 取值（不是降级；这是 fence 不变量的直接结论）；
//   · 该代**未安装** ⇒ WAL 是唯一副本且正在被回收，只能明确报错，
//     绝不能静默降级或 NotFound。
// step A-probe（用户 2026-09-17）：最近一次**索引选择**的胜出信息——用于
// 逐次 Get 记录 `index_sequence` / 候选数 / 来源，与 LSM 侧最新 seq 对照。
struct IndexSel {
  bool set = false;
  bool hit = false;         // 索引是否命中（false = 未命中，读走 LSM）
  uint32_t sel_part = 0;    // 命中来源的逻辑分区
  uint32_t phys = 0;        // locator 的 physical part
  uint32_t gen = 0;
  uint64_t seq = 0;         // 胜出条目的 seq
  uint32_t candidates = 0;  // 参与选择的候选数
  bool tombstone = false;
};
inline IndexSel& LastIndexSelTls() {
  static thread_local IndexSel s;
  return s;
}

struct RejectedGenIdentity {
  bool set = false;
  uint32_t part = 0;
  uint32_t gen = 0;
};
inline RejectedGenIdentity& LastRejectedGenTls() {
  static thread_local RejectedGenIdentity r;
  return r;
}
inline void NoteRejectedGen(uint32_t part, uint32_t gen) {
  auto& r = LastRejectedGenTls();
  r.set = true;
  r.part = part;
  r.gen = gen;
}
// 供测试主线程读取读者线程选中的 (part, gen)（TLS 不可跨线程 ⇒ 另存全局）。
struct PinIdentityGlobal {
  std::atomic<uint32_t> part{0};
  std::atomic<uint32_t> gen{0};
  std::atomic<bool> set{false};
};
inline PinIdentityGlobal& LastPinIdentityGlobal() {
  static PinIdentityGlobal g;
  return g;
}
inline void NoteCandidateVisit(uint32_t part, uint32_t order, bool hit,
                               uint64_t seq) {
  CandidateVisits().push_back(CandidateVisit{part, order, hit, seq});
}

// step 1an（用户 2026-09-17）：**胜出候选的读引用身份**（选择结束后由实现写入，
// 测试据此断言 pin 属于胜出候选，而不是被落选候选覆盖）。
struct PinIdentity {
  bool set = false;
  uint32_t part = 0;   // 读引用请求的 physical_part
  uint32_t gen = 0;
  bool pinned = false;      // 是否真的取得句柄
  bool is_active = false;   // 请求时该代是否活跃（活跃代当前无法 pin）
};
inline PinIdentity& LastPinIdentity() {
  static thread_local PinIdentity p;
  return p;
}

// B/C 不一致计数（索引插入 / 选择胜出，与 A 表对照）
inline std::atomic<uint64_t>& LocatorBMismatch() {
  static std::atomic<uint64_t> n{0};
  return n;
}
inline std::atomic<uint64_t>& LocatorCMismatch() {
  static std::atomic<uint64_t> n{0};
  return n;
}

// 读**实际返回来源**（用户 2026-09-15：A/B 必须按来源统计，不能按新值的
// 文件位置统计）。由引擎在返回点写入 thread-local，校验器在 Get 之后读取。
enum class GetSource : int {
  kNone = 0,           // 未经过 ZF 路径（非 ZF 库/未启用）
  kIndex = 1,          // 索引命中并返回值（A 桶：索引返回旧值）
  kIndexTombstone = 2, // 索引命中删除标记 → NotFound
  kIndexReadFailed = 3,// 索引命中但 WAL 读失败 → 回落 LSM
  kIndexRangeDel = 4,  // 范围删除覆盖 → NotFound
  kLsm = 5,            // 索引未命中，LSM 返回了值（B 桶：LSM 自身返回旧值）
  kLsmNotFound = 6,    // 索引未命中，LSM 也 NotFound
};

inline GetSource& LastSourceTls() {
  static thread_local GetSource src = GetSource::kNone;
  return src;
}

inline void SetLastSource(GetSource s) { LastSourceTls() = s; }

inline const char* SourceName(GetSource s) {
  switch (s) {
    case GetSource::kIndex:
      return "INDEX";
    case GetSource::kIndexTombstone:
      return "INDEX_TOMBSTONE";
    case GetSource::kIndexReadFailed:
      return "INDEX_READ_FAILED";
    case GetSource::kIndexRangeDel:
      return "INDEX_RANGEDEL";
    case GetSource::kLsm:
      return "LSM";
    case GetSource::kLsmNotFound:
      return "LSM_NOTFOUND";
    default:
      return "NONE";
  }
}

// 读视图（LSM 侧有效读序号）：快照存在则用快照 seq，否则用 LastSequence。
// 与 GetContext 的可见性判定同源，保证"同一读序列和读视图"。
inline const char* TypeName(int type) {
  switch (type) {
    case 0:
      return "DEL";
    case 1:
      return "VAL";
    case 2:
      return "MERGE";
    case 7:
      return "SINGLE_DEL";
    case 15:
      return "RANGEDEL";
    default:
      return "OTHER";
  }
}

}  // namespace get_trace
}  // namespace zeroflush
