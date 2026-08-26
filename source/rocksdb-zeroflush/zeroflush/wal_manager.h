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
#include <unordered_map>
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

// Freeze 结果：旧代号、旧代封存字节数、对应文件名。
struct FreezeResult {
  uint32_t old_gen;       // 刚被封存的代号
  uint64_t sealed_bytes;  // 旧代封存时的逻辑大小（=p->total_size 旧值）
  std::string sealed_path;  // 旧代文件路径（绝对 or 相对 env 基址）
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

  // M3.2：区分"干净 EOF"与"中途损坏"。Next() 返回 false 后调用：
  //  - OK：正常扫完（EOF 或尾部截断——截断对物化同样视为脏数据）；
  //  - Corruption：CRC/解码失败（头部 magic 不对或 CRC 校验不过）；
  //  - IOError：底层读失败或文件打开失败。
  // 恢复路径（Recover）沿用"false 即停"的宽容语义，不受影响。
  rocksdb::Status status() const { return status_; }

  uint64_t offset() const { return offset_; }

 private:
  rocksdb::Env* env_;
  std::string path_;
  std::unique_ptr<rocksdb::SequentialFile> file_;
  std::string buf_;
  size_t buf_pos_ = 0;
  uint64_t offset_ = 0;
  rocksdb::Logger* info_log_;
  // M4.7：块读——缓冲不足时整段读入（1MB），避免逐条 2 次 Read 的
  // syscall 开销（1KB 记录 24MB 需 48K 次 Read；块读后 ~24 次）。
  bool Refill();
  static constexpr size_t kScanChunkSize = 1 << 20;
  // M3.2：扫描终态。OK = 干净 EOF（或尾部截断）；Corruption/IOError = 中途失败。
  rocksdb::Status status_ = rocksdb::Status::OK();
};

// 分区 WAL 管理器：追加 / 同步 / 冻结 / 定点读 / 扫描。
class PartitionedWalManager {
 public:
  PartitionedWalManager(rocksdb::Env* env, const std::string& dir,
                        uint32_t partitions, uint64_t partition_target_bytes);
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

  // M4.1a：O(1) 封存判定聚合计数。
  // Append 成功以 relaxed atomic 累加；Freeze 时按旧代字节扣减/清零。
  uint64_t TotalActiveBytes() const {
    return total_active_bytes_.load(std::memory_order_relaxed);
  }
  // 任一分区活跃字节 ≥ partition_target_bytes（Append 时置位，Freeze 清零）。
  bool AnyPartitionOverTarget() const {
    return any_over_target_.load(std::memory_order_relaxed);
  }
  // 封存全部分区后调用：清超限标志（新代从 0 起，超限由后续 Append 重新置位）。
  void ClearOverTargetFlag() {
    any_over_target_.store(false, std::memory_order_relaxed);
  }

  // 封存：刷盘缓冲、关闭旧代文件、开新代文件。返回旧代信息。
  // 修 D2：分代字段已重置，新代从 0 写起。
  // 修 D3：未写过的分区不会触发 wfile->Sync() 段错误。
  FreezeResult Freeze(uint32_t part);

  // 按引用定点读 value（Get / 迭代器取值路径）。
  rocksdb::Status ReadRecord(const WalRecordRef& ref,
                             std::string* value) const;

  // 按引用定点读 value 到调用方缓冲（迭代器复用 buffer 用）。
  rocksdb::Status ReadRecord(const WalRecordRef& ref, std::string* buf,
                             rocksdb::Slice* value) const;

  // M2.1：把 ref 的代际转成"当前活跃 gen"，用于在 ReadRecord 之前由
  // ZeroFlushContext 判断走"活跃代 IO"还是"SealedFileCache IO"。
  uint32_t ActiveGen(uint32_t part) const;

  // M2.1：从一个已打开的封存代 RandomAccessFile 读 value（无锁 IO）。
  // ZeroFlushContext::ReadValue 拿到 SealedFileCache 句柄后调用此方法。
  static rocksdb::Status ReadFromSealed(
      rocksdb::RandomAccessFile* rf, const WalRecordRef& ref,
      std::string* buf, rocksdb::Slice* value);

  // 列出目录中全部 (part, gen) 文件（恢复用）。
  rocksdb::Status ListFiles(std::vector<std::pair<uint32_t, uint32_t>>* out) const;

  // M2.1：发现一个分区当前最大活跃代（用于 Open() 中按 max(gen) 探测，
  // 修 D4——避免重开后 Append 追加到已封存的旧代文件）。
  // 若该分区无任何代文件返回 0。
  uint32_t MaxGen(uint32_t part,
                  const std::vector<std::pair<uint32_t, uint32_t>>& all) const;

  // M2.1：获取该分区某代文件的物理大小（用于 Open 初始化 flushed_size）。
  // 若 (part, gen) 不在 all 中返回 0。
  uint64_t GetFileSize(uint32_t part, uint32_t gen,
                       const std::vector<std::pair<uint32_t, uint32_t>>& all,
                       rocksdb::Env* env) const;

  const std::string& dir() const { return dir_; }
  uint32_t partitions() const { return partitions_; }

  // M3.0：接入 DB info log（zeroflush::Open 在 use_logger 时调用；
  // nullptr = 静默，ROCKS_LOG_* 对 nullptr 安全）。
  void SetInfoLog(rocksdb::Logger* l) { info_log_ = l; }
  rocksdb::Logger* InfoLog() const { return info_log_; }

  // M3.1：确保分区 id 存在（初始化或分裂后延迟创建）。已存在则 no-op。
  // 返回指向该分区的指针（保证非空）。
  void EnsurePartition(uint32_t part_id);

  // M3.1：检查分区 id 是否已被管理（用于 ReadValue 的上界校验替代
  // "part_id < partitions_"）。
  bool HasPartition(uint32_t part_id) const;

  // M3.1：返回管理的全部分区 ID 列表（用于遍历）。
  std::vector<uint32_t> AllPartitionIds() const;

 private:
  struct Partition {
    uint32_t part_id = 0;               // 分区号（Open 时初始化）
    mutable rocksdb::port::Mutex mu;
    std::unique_ptr<rocksdb::WritableFile> wfile;   // 当前代写句柄
    std::unique_ptr<rocksdb::RandomAccessFile> rfile;  // 当前代读句柄
    std::string buf;          // 未刷盘记录缓冲
    uint64_t flushed_size = 0;  // 已刷盘字节数
    uint64_t total_size = 0;    // 逻辑大小 = flushed + buf
    // M4.1a：活跃字节的并发计数（Append 累加 / Freeze 清零），供
    // ShouldSeal 的 O(1) 副触发与 ActiveSize 无锁读取。
    std::atomic<uint64_t> active_bytes{0};
    uint32_t gen = 0;
  };

  std::string FileName(uint32_t part, uint32_t gen) const;
  rocksdb::Status FlushBuf(Partition* p) const;  // buf → 文件
  rocksdb::Status OpenGen(Partition* p) const;   // 打开 p->gen 代读写句柄（追加模式）
  rocksdb::Status EnsureOpenForWrite(Partition* p) const;  // 延迟打开写句柄

  // M3.1：parts_ 从 vector 改为 unordered_map，支持稀疏 part_id 空间
  // （分裂后 part_ids 不再连续[0,P)，见 M3_DESIGN.md §8.2）。
  rocksdb::Env* env_;
  std::string dir_;
  uint32_t partitions_;  // 初始 P 值（实际分区数由 parts_.size() 反映）
  // M4.1a：单分区封存阈值（Append 时置位 any_over_target_ 用）。
  uint64_t partition_target_bytes_;
  std::unordered_map<uint32_t, std::unique_ptr<Partition>> parts_;
  // M3.4：保护 parts_ map 结构（range-del 分区动态创建与并发 Append/遍历）。
  mutable rocksdb::port::Mutex parts_mu_;
  // M4.1a：O(1) 封存判定聚合计数（见 TotalActiveBytes/AnyPartitionOverTarget）。
  std::atomic<uint64_t> total_active_bytes_{0};
  std::atomic<bool> any_over_target_{false};
  // M3.0 R4：M2 遗留 bug——该成员从未初始化（构造器未赋值）。
  // 现默认 nullptr（ROCKS_LOG_* 安全），由 SetInfoLog 接线。
  rocksdb::Logger* info_log_ = nullptr;
};

}  // namespace zeroflush
