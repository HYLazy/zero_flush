//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlushV0 P0（S 架构化）：S5 设备压缩契约 —— 请求/结果结构具象化。
//
//  本文件只落地契约形态（对象 + 约束注释），不接入执行路径（零行为改动）。
//  对应论文 §5.1 S1/S2/S5 与 §5.3 的设备视图：
//    S1 输出有界：常规任务（直装/合并）输出恒 1 个文件且 ≤ 目标大小；
//       type-3（boundary rewrite）为唯一多输出任务，输出恒 2 个。
//    S2 输出硬约束：任何输出文件 range ⊆ 发起分区的路由区间（越界 = 任务失败）。
//    S5 请求契约：WAL 区域描述 + 按 (key, seq desc) 的 per-record 条目流 +
//       快照水位 + 重叠 L1 文件描述；value 不在流中（按 offset 从 WAL 读取）；
//       cache slot 等主机读视图字段不出现在设备视图。
//
//  与实现侧的映射（P3 接线时消费本契约）：
//    - 直装/合并当前执行体：ZfMaterializeJob::MaterializePartition /
//      MaterializeMergePartition（materialize_job.h）；决策（MaterializeDecision：
//      kDirect/kMergeBase/kFallback/kSkip）属策略层，不进入本契约——契约只描述
//      "物理任务"，策略（是否攒批/收养/回落 L0）在决策层独立演进。
//    - 请求的 wal_regions/l1_inputs 对应 PartitionPlan 的 gens/overlap_all；
//      replaced_file_numbers 对应 MaterializeOutput::replaced_file_numbers。
//    - type-3（kBoundaryRewrite）为 S4 拆分过渡期任务，P2 实现。

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "rocksdb/types.h"

namespace zeroflush {

// S5 任务类型。与实现侧 MaterializeDecision 解耦命名（见文件头注释）。
enum class ZfJobKind : uint8_t {
  kDirectInstall = 0,   // WAL→L1 直装：区间内无重叠 L1 文件，输出 1 文件
  kMergeInstall = 1,    // WAL→L1 常规合并：输入含重叠 L1 文件，输出 1 文件
  kBoundaryRewrite = 2, // type-3：过渡期跨界数据按新边界切分，输出恒 2 文件
};

// 输出边界（S2）。lo/hi 为 owned 字节（请求可能跨 job / 未来跨 host-device
// 边界存续，不能借用路由表内部 Slice）；空串表示 -∞ / +∞。区间语义与
// PartitionTable::RangeOf 一致：[lo, hi)。
struct ZfKeyRange {
  std::string lo;
  std::string hi;
};

// 一段 WAL 输入 = 一个 (分区, 代际) 封存文件及其足迹。
struct ZfWalRegion {
  uint32_t part_id = 0;
  uint32_t gen = 0;
  uint64_t file_bytes = 0;      // 诊断/进度用（可不填）
  uint64_t entry_count = 0;     // 该代记录条数（0 = 未知）
  // 该代文件的 user_key 足迹（实际最小/最大，空 = ±∞）——overlap 判定用，
  // 不等于分区路由区间（S3：对齐仅为性能，足迹可小于区间）。
  std::string smallest_user_key;
  std::string largest_user_key;
};

// 条目流条目（S5 设备视图投影）：{key, seq, wal offset, type}。
// 仅借用的视图（不持有数据）——P0/P3 的 host 执行路径直接以 frozen 分区索引
// 或排序后 WAL 记录的内存序提供同序条目，无需物化副本；未来真 CSD 路径由
// 序列化层按同一排序语义产出字节流替换本视图。value 不在流中。
struct ZfEntryView {
  rocksdb::Slice key;                    // user key（排序键）
  rocksdb::SequenceNumber seq = 0;       // 版本序（全局）
  uint64_t wal_offset = 0;               // 相对所属 ZfWalRegion 文件头偏移
  uint8_t type = 0;                      // rocksdb ValueType：kTypeValue=0x1 /
                                         // kTypeDeletion=0x0（wal_format 同款）
};

// S5 压缩请求。一次请求 = 一个物理任务，输出边界内单点安装。
struct ZfCompactionRequest {
  ZfJobKind kind = ZfJobKind::kMergeInstall;

  // (i) WAL 区域描述：A 侧输入，按代际序（同分区代际序 = seq 单调）。
  std::vector<ZfWalRegion> wal_regions;

  // (ii) 条目流：per-WAL-record（含被覆盖旧版本与 tombstone），排序 =
  //      (user_key asc, seq desc)（与 InternalKey 序一致）。流与 wal_regions
  //      一一对应（每区域一段）；合并输入顺序由此流决定（设备单遍归并）。
  //      条目流按需以 ZfEntryView 迭代提供，不在请求内嵌拷贝。

  // (iii) L1 文件描述：B 侧输入 = 与 wal 足迹重叠的 L1 sstable（直装为空）。
  std::vector<rocksdb::FileMetaData> l1_inputs;

  // (iv) 快照水位（S5）：seq > earliest_snapshot 的版本全保留；≤ 水位的
  //      每 key 保留最新一条。kMaxSequenceNumber = 无快照约束。
  rocksdb::SequenceNumber earliest_snapshot = rocksdb::kMaxSequenceNumber;

  // 输出边界（S2）：常规任务 = 发起分区的路由区间；kBoundaryRewrite =
  // 拆分后的子区间（跨界数据按此边界切分）。输出必须 ⊆ 本区间。
  ZfKeyRange output_range;

  // 墓碑保留（S5）：本任务不是最底层合并 → tombstone 一律保留在输出中，
  // 由下游原生压缩在成为最底层合并时裁减。无需选项，设备按层语义执行。
};

// S5 压缩结果。
struct ZfCompactionResult {
  // 输出文件：常规任务 1 个；kBoundaryRewrite 恒 2 个（S1）。全部输出的
  // user_key 范围必须 ⊆ 请求 output_range（S2）——越界 = 结果无效。
  std::vector<rocksdb::FileMetaData> outputs;

  // 被本输出消费的 L1 文件号。安装 = 单点原子 VersionEdit：
  // DeleteFile(replaced_file_numbers) + AddFile(outputs) 同批生效。
  std::vector<uint64_t> replaced_file_numbers;

  // 实际输出覆盖（懒校验/断言用；应为输出 union 的足迹）。
  ZfKeyRange output_range;
};

}  // namespace zeroflush
