// 键生成：与 db_bench 的 ThreadState::GenerateKeyFromInt **完全一致**，
// 抽出来供 db_bench / zf_valcheck / 测试 reference model 共用，避免校验器
// 与压测端的键格式漂移（此前 zf_valcheck 自带一套 MakeKey，与
// `--key_size=16` 实际产生的键不一致，导致无法校验真实数据）。
//
// 语义（key_size=16，无 prefix 时）：
//   前 8 字节 = big-endian 的 v；其余字节填 ASCII '0'（0x30）。
//   例：v=0xB3B3C2 → 00 00 00 00 00 B3 B3 C2 '0'×8
// 字节序比较与数值序一致（同长度、等宽编码）。
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>

namespace zf_keygen {

// 把 v 写入 pos 起、长度为 size 的键缓冲（尾部用 '0' 填充）。
inline void FillKeyTail(uint64_t v, int size, char* pos) {
  const int bytes_to_fill = std::min(size, 8);
  for (int i = 0; i < bytes_to_fill; ++i) {
    pos[i] = static_cast<char>((v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF);
  }
  if (size > bytes_to_fill) {
    memset(pos + bytes_to_fill, '0', static_cast<size_t>(size - bytes_to_fill));
  }
}

inline std::string KeyFromInt(uint64_t v, int key_size = 16) {
  std::string key(static_cast<size_t>(key_size), '\0');
  if (key_size > 0) {
    FillKeyTail(v, key_size, &key[0]);
  }
  return key;
}

// 参考值模型：写入端与校验端共用，保证可复算。
//   seed   = 版本号（首次写 1，覆盖写 2、3……）
//   value[i] = (uint8)((v * 131 + seed * 17 + i * 7) & 0xFF)
inline std::string ValueFor(uint64_t v, uint32_t seed, int value_size = 1024) {
  std::string value(static_cast<size_t>(value_size), '\0');
  for (int i = 0; i < value_size; ++i) {
    value[i] = static_cast<char>(
        (static_cast<uint64_t>(v) * 131 + static_cast<uint64_t>(seed) * 17 +
         static_cast<uint64_t>(i) * 7) &
        0xFF);
  }
  return value;
}

}  // namespace zf_keygen
