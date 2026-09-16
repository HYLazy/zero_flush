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
