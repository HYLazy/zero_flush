//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M1: WAL 分区记录帧格式（ZF01）。
//
// 设计要点（对应设计文档 §4.1）：
//  - 无 32KB 物理块分层：分区文件只被顺序整读（CSD）或定点随机读（Get 按 offset），
//    简化帧格式、降低写放大；
//  - seq 显式落盘：恢复时无需依赖跨分区顺序即可重建全局序；
//  - value 永久内联：val_len 4B 覆盖任意大小，不设 blob 指针；
//  - 记录可跨界（4KB 对齐非强制），索引记录的 offset 为精确字节偏移。
//
// Record 布局（小端）：
//   header (24B): magic 'ZF01'(4B) | cf_id(2B) | type(1B) | flags(1B)
//                 | key_len(4B) | val_len(4B) | seq(8B)
//   body        : key(key_len B) | value(val_len B)
//   trailer     : crc32c(4B)，覆盖 header+body

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace zeroflush {

constexpr uint32_t kZfMagic = 0x31304655;  // 'ZF01'
constexpr uint32_t kZfHeaderSize = 24;
constexpr uint32_t kZfCrcSize = 4;

// 帧头，24B 紧密打包（小端）。
struct ZfRecordHeader {
  uint32_t magic;   // kZfMagic
  uint16_t cf_id;   // v1 恒为 0，多 CF 预留
  uint8_t type;     // rocksdb ValueType: kTypeValue=0x1 / kTypeDeletion=0x0
  uint8_t flags;    // bit0 保留（未来：key 前缀压缩）
  uint32_t key_len;
  uint32_t val_len;
  uint64_t seq;     // 全局单调递增，显式落盘
};
static_assert(sizeof(ZfRecordHeader) == kZfHeaderSize,
              "ZfRecordHeader must be 24 bytes");

// Slim MemTable 的"value 区域"定位符（16B）：指向分区 WAL 中的唯一持久副本。
// 布局 [varint loc_size=16][locator 16B]，与原生 memtable 的
// [varint val_size][value] 结构完全同构 —— 这样 MemTable::Add/SaveValue/
// MemTableIterator 的既有解析逻辑无需改变，只需在取值处按 zf 分支解引用。
// M3.4：range tombstone 专用分区号（不参与路由；覆盖多分区的删除范围）。
constexpr uint32_t kRangeDelPartId = 0xFFFFFFFEu;

struct SlimLocator {
  uint32_t part_id;   // 所在 WAL 分区
  uint32_t gen;       // 代际（M1 恒为 0，Freeze 后递增）
  uint64_t wal_offset;  // 记录在分区文件中的精确字节偏移
};
static_assert(sizeof(SlimLocator) == 16, "SlimLocator must be 16 bytes");

// 编码一条记录到 out（header+body+crc，小端）。
void EncodeZfRecord(const ZfRecordHeader& h, const rocksdb::Slice& key,
                    const rocksdb::Slice& value, std::string* out);

// 从 data[0..len) 解码并校验 magic + crc32c。
// 成功时 h/key/value 指向 data 内部（无拷贝）。
rocksdb::Status DecodeZfRecord(const char* data, size_t len, ZfRecordHeader* h,
                               rocksdb::Slice* key, rocksdb::Slice* value);

// 单条记录的总字节数（header + key + value + crc）。
uint32_t ZfRecordLength(uint32_t key_len, uint32_t val_len);

// ---------------------------------------------------------------------------
// ZFPROPS v1：ZeroFlush 持久化元数据（位于 zfwal/ZFPROPS）
//
// v1 16B 定长布局（小端）：
//   magic  (4B) = 'ZFP1' = 0x3150465A
//   version(4B) = 1
//   partitions (4B)
//   crc32c (4B) — 覆盖前 12B
// ---------------------------------------------------------------------------
constexpr uint32_t kZfPropsMagic = 0x3150465A;  // 'ZFP1'
constexpr uint32_t kZfPropsSize = 16;

struct ZfProps {
  uint32_t magic;
  uint32_t version;
  uint32_t partitions;
  uint32_t crc;
};
static_assert(sizeof(ZfProps) == kZfPropsSize, "ZfProps must be 16 bytes");

// 编码 ZFPROPS v1 到 out（固定 16B）。
void EncodeZfProps(uint32_t partitions, std::string* out);

// 解码 + 校验 ZFPROPS v1。返回 OK 表示 magic/version/crc 全部通过。
rocksdb::Status DecodeZfProps(const char* data, size_t len, ZfProps* out);

// ---------------------------------------------------------------------------
// ZFPROPS v2（M3.1）：变长格式，支持 routing_mode / comparator / 边界表
//
// 布局（小端）：
//   magic 'ZFP2'(4B) | format_version=2 (4B) | routing_mode(1B) | pad(3B)
//   comparator_name_len(4B) | comparator_name(...)
//   table_count(4B)
//     ┌ per table: version(4B) | partitions(4B)
//     │            part_ids: P × 4B
//     │            boundary_count(4B) = P-1
//     │              ┌ per boundary: len(4B) | bytes(...)
//     └ …
//   current_version(4B)
//   crc32c(4B)   // 覆盖前面全部字节
// ---------------------------------------------------------------------------
constexpr uint32_t kZfPropsMagicV2 = 0x3250465A;  // 'ZFP2'
constexpr uint32_t kZfPropsV2FixedSize = 32;  // magic+version+rmode+pad+cnameLen
                                              // +tableCount + curVer + crc

// 每个 table 在 ZFPROPS v2 中的信息。
struct ZfPropsTableInfo {
  uint32_t version;
  uint32_t partitions;
  std::vector<uint32_t> part_ids;       // P 个全局 id
  std::vector<std::string> boundaries;  // P-1 个分隔键（空向量 = hash 或无边界）
};

// v2 解码结果。
struct ZfPropsV2 {
  uint32_t magic;                     // kZfPropsMagicV2
  uint32_t format_version;            // 2
  uint8_t routing_mode;               // 0=kHash, 1=kStatic, 2=kSampled
  std::string comparator_name;
  std::vector<ZfPropsTableInfo> tables;
  uint32_t current_version;           // 当前使用的 table version
  uint32_t crc;
};

// 编码 ZFPROPS v2。写入必须原子（tmp → rename）。
rocksdb::Status EncodeZfPropsV2(uint8_t routing_mode,
                                const std::string& comparator_name,
                                const std::vector<ZfPropsTableInfo>& tables,
                                uint32_t current_version, std::string* out);

// 自动检测 v1 / v2。v1 被包装为 v2 结构（routing_mode=0, comparator 未知）。
rocksdb::Status DecodeZfPropsAuto(const char* data, size_t len,
                                  ZfPropsV2* out);

}  // namespace zeroflush
