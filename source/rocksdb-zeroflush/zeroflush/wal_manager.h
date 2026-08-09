//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M1: 分区 WAL 管理器（PartitionedWalManager）。
//
// 对应设计文档 §2.2 / §4.1 / §5：P 个分区，每分区一个活跃写文件
// `zf-wal-<part>-<gen>.log`（存放于 wal_dir 下独立子目录，避免与原生 WAL
// 文件命名冲突）。写路径：记录编码为 ZF01 帧 → 分区 4KB 对齐缓冲 → 刷盘；
// 读路径：按 (part, gen, offset) 定点随机读（Get / 迭代器取值）。
//
// M1 说明：
//  - 无 SealWorker/Freeze 调用方：每个分区仅 gen 0 活跃文件，读写句柄常驻；
//  - Freeze() 已实现（供 M2 封存接入）：flush + 关旧文件 + 开新代文件；
//  - 记录缓冲只保证正确性，4KB 对齐刷盘为后续优化点（对齐非强制，可跨界）。

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "zeroflush/wal_format.h"

namespace zeroflush {

// 一次追加返回的引用：value 的唯一持久副本位置。
struct WalRecordRef {
  uint32_t part_id;
  uint32_t gen;
  uint64_t offset;  // 记录在分区文件中的精确字节偏移
};

// 顺序扫描器：恢复（重放）与 M2 CSD/fallback 归并使用。
class WalScanner {
 public:
  // 扫描 dir/zf-wal-<part>-<gen>.log 的全部记录。
  WalScanner(rocksdb::Env* env, const std::string& dir, uint32_t part,
             uint32_t gen, rocksdb::Logger* info_log = nullptr);
  ~WalScanner();

  WalScanner(const WalScanner&) = delete;
  WalScanner& operator=(const WalScanner&) = delete;

  // 取下一条记录；返回 false 表示扫描结束（或损坏）。CRC 校验失败返回
  // Corruption 状态（尾部损坏按截断处理，与原生 kTolerateCorruptedTailRecords
  // 语义一致）。
  bool Next(ZfRecordHeader* h, rocksdb::Slice* key, rocksdb::Slice* value);

  uint64_t offset() const { return offset_; }

 private:
  rocksdb::Env* env_;
  std::string path_;
  std::unique_ptr<rocksdb::SequentialFile> file_;
  std::string buf_;
  size_t buf_pos_ = 0;
  uint64_t offset_ = 0;
  rocksdb::Logger* info_log_;
};

// 分区 WAL 管理器：追加 / 同步 / 冻结 / 定点读 / 扫描。
class PartitionedWalManager {
 public:
  PartitionedWalManager(rocksdb::Env* env, const std::string& dir,
                        uint32_t partitions);
  ~PartitionedWalManager();
  
    // 关闭时刷新所有分区缓冲并同步到磁盘。
    rocksdb::Status Close();

  // 创建目录并打开各分区写文件（幂等；重开时继续追加已有文件）。
  rocksdb::Status Open();

  // 向分区 part 追加一条记录。value 内联（val_len 4B 覆盖任意大小）。
  rocksdb::Status Append(uint32_t part, const rocksdb::Slice& key,
                         const rocksdb::Slice& value,
                         uint8_t type, uint64_t seq, WalRecordRef* out);

  // fdatasync 单个/所有分区（group commit 统一下刷）。
  rocksdb::Status Sync(uint32_t part);
  rocksdb::Status SyncAll();

  // 活跃分区的逻辑大小（含未刷盘缓冲），用于封存大小上限判断。
  uint64_t ActiveSize(uint32_t part) const;

  // 封存：刷盘缓冲、关闭旧代文件、开新代文件。返回旧 gen。
  // M1 暂无调用方（SealWorker 在 M2 接入）；实现保证旧代文件只读可查。
  uint32_t Freeze(uint32_t part);

  // 按引用定点读 value（Get / 迭代器取值路径）。
  rocksdb::Status ReadRecord(const WalRecordRef& ref,
                             std::string* value) const;

  // 按引用定点读 value 到调用方缓冲（迭代器复用 buffer 用）。
  rocksdb::Status ReadRecord(const WalRecordRef& ref, std::string* buf,
                             rocksdb::Slice* value) const;

  // 列出目录中全部 (part, gen) 文件（恢复用）。
  rocksdb::Status ListFiles(std::vector<std::pair<uint32_t, uint32_t>>* out) const;

  const std::string& dir() const { return dir_; }
  uint32_t partitions() const { return partitions_; }

 private:
  struct Partition {
    uint32_t part_id = 0;               // 分区号（Open 时初始化）
    mutable rocksdb::port::Mutex mu;
    std::unique_ptr<rocksdb::WritableFile> wfile;   // 当前代写句柄
    std::unique_ptr<rocksdb::RandomAccessFile> rfile;  // 当前代读句柄
    std::string buf;          // 未刷盘记录缓冲
    uint64_t flushed_size = 0;  // 已刷盘字节数
    uint64_t total_size = 0;    // 逻辑大小 = flushed + buf
    uint32_t gen = 0;
  };

  std::string FileName(uint32_t part, uint32_t gen) const;
  rocksdb::Status FlushBuf(Partition* p) const;  // buf → 文件
  rocksdb::Status OpenGen(Partition* p) const;   // 打开 p->gen 代读写句柄（追加模式）
  rocksdb::Status EnsureOpenForWrite(Partition* p) const;  // 延迟打开写句柄

  rocksdb::Env* env_;
  std::string dir_;
  uint32_t partitions_;
  std::vector<std::unique_ptr<Partition>> parts_;
  rocksdb::Logger* info_log_;
};

}  // namespace zeroflush
