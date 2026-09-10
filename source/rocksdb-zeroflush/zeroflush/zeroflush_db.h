//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M1: 全局上下文与 Open 入口。
//
// ZeroFlushContext 是 ZF 写路径/读路径共享的运行时上下文：
//  - 持有分区 WAL 管理器与路由配置（M1：hash 路由，PartitionTable 在 M2 接入）；
//  - 提供 WriteGroup → 分区 WAL + Slim MemTable 的写实现；
//  - 提供 SlimLocator → value 的定点解析（Get / 迭代器共用）；
//  - 提供恢复（重放分区 WAL 重建索引）。
//
// 生命周期：由 zeroflush::Open 创建，以 shared_ptr 挂在 DBImpl 与 MemTable
// 上（侵入点：DBImpl::SetZeroFlushContext），随 DB 关闭而释放。

#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "db/write_thread.h"
#include "zeroflush/repartition_controller.h"  // M4: 每分区统计 + 治理决策策略

namespace ROCKSDB_NAMESPACE {
class ColumnFamilyData;
class DBImpl;
class Env;
class MemTable;
struct MemTablePostProcessInfo;
class MergeContext;
class MergeOperator;
struct ReadOptions;
struct WriteOptions;
}  // namespace ROCKSDB_NAMESPACE

namespace zeroflush {

class PartitionTable;  // M4.2b：BuildL1AlignedTable 输出（完整定义在
                       // partition_table.h，此处仅前向声明）
class PartitionIndexSet;  // M4.3：终态分区索引（partition_index.h）
class PartitionedWalManager;
struct SealedEpoch;

// ZeroFlushV0 P1b（M1）：DRAINING 分区登记（S4 merge 的排空窗口态）。
// merge 把 drain_part 的路由区间并入相邻分区（absorb_part）后：drain 不再
// 收新写（路由表已不含它），其封存数据（merge 时冻结的活跃尾 E_D + 此前已
// 物化进 L1 的历史文件）由既有 epoch 流水排空；E_D 排空完成前，drain 原
// 区间内 key 路由到吸收方，读路径需在吸收方 miss 后回查 drain 的分区索引
// （别名查找，见 GetFromPartitionIndex）。排空完成（无未回收 WAL 段、无
// frozen 索引、无活跃字节）→ MaybeRetireDraining 移除登记。
struct DrainingPart {
  uint32_t part = 0;    // 被合并（DRAINING）分区
  uint32_t absorb = 0;  // 吸收方分区（drain 区间 key 的新路由宿主）
  std::string lo;       // drain 原路由区间 [lo, hi)（合并时刻快照，别名判定用）
  std::string hi;
};

// ZeroFlushV0 M4（活跃尾 split）：父分区尾 epoch 的读别名登记。split 切路由
// 后父分区退役，但其冻结尾 epoch E（父原区间 [lo,hi) 的数据）在新表中不
// 存在；E 排空前，子分区索引链 miss 的 key 需回查父分区索引链（E 与子分区
// 新写可能同 key：E 版本更旧——子链优先即正确）。E 物化完成（ReleaseEpoch）
// 后移除登记。
struct SplitOrphanPart {
  uint32_t part = 0;  // 父分区 id（已退役，仅冻结尾 epoch 存活）
  uint32_t child_l = 0;
  uint32_t child_r = 0;
};

// ZeroFlush 扩展选项（对应设计文档 §10.1 zf 命名空间；M1 仅路由相关）。
struct ZeroFlushOptions {
  uint32_t partitions = 64;  // P：分区数（M1 hash 路由；M2 起由 PartitionTable 接替）
  uint64_t partition_target_bytes = 64 << 20;  // 封存大小上限（M2 使用）
  std::string wal_subdir = "zfwal";  // wal_dir 下独立子目录，避免与原生 WAL 冲突
  bool use_logger = false;           // 是否打印 zf 日志到 DB info log

  // ---- M2 新增字段 ----
  // 主触发：全分区活跃字节合计上限（达到即封存全部 P 个分区）。
  // 默认 256MB，与 M1 Open() 中强制放大的 write_buffer_size 对齐。
  uint64_t epoch_target_bytes = 256u << 20;
  // 副触发：单分区活跃字节上限（防分区倾斜时主条件迟迟不满足）。
  // M5P1b（2026-09-07 剖析结论）：未物化 epoch 上限（写停水位）。默认
  // 2 会饿死多 flush 流水线——写停在 2 个 pending epoch，imm 永远攒不起
  // backlog，4 路 flush 有效并行度 ~1.1×（实测 25GB：任务时间总和 ≈ 墙钟、
  // 全体线程 95% futex 等待；水位 8 后 stall=0、早期吞吐 +55%）。抬到 8
  // （= 2× 并行 flush 数 + 余量）：允许流水线积压后并发排空；内存代价 =
  // 至多 8 个未物化 epoch 的封存 WAL/索引（~1-2GB，index_mem_budget 兜底）。
  uint64_t max_pending_epochs = 8;
  uint32_t max_open_sealed_files = 256;  // SealedFileCache LRU 容量
  bool reclaim_sealed_files = true;     // false=只封存不删（调试/取证用）
  // 是否在 zeroflush::Open() 中写/校验 ZFPROPS 元数据文件（保护 partitions
  // 一致性；首次部署的 DB 也会自动创建）。
  bool use_zfprops = true;

  // ---- M3.1 路由 ----
  enum class RoutingMode : uint8_t {
    kHash = 0,        // M2 行为：Hash(user_key) % P（兼容既有 DB）
    kStatic = 1,      // 用户提供 P-1 个分隔键
    kSampled = 2,     // 首个 epoch 用 hash，封存时采样学习边界后固定
    // M4.2b：每 epoch 封存时按 L1 层 SST 文件边界重新对齐分区（compaction
    // 感知分区）。物化输出的 L0 文件键范围 ⊆ 单个 L1 文件范围 → L0→L1
    // compaction 1:1 归并、可并行（配合 subcompactions 无读放大）。
    // 本 epoch 数据用旧表写入（se.table_version 保持旧版本），新表给
    // 下一 epoch 使用——与 kSampled 学习期同语义。L1 为空时保持当前表。
    kAlignL1 = 3,
  };
  RoutingMode routing_mode = RoutingMode::kHash;
  std::vector<std::string> static_boundaries;   // kStatic：升序，size == P-1
  uint32_t sample_every_n_records = 64;         // kSampled：采样步长
  // 连续 k 个 epoch 超 partition_target_bytes 才允许分裂（0 = 禁用分裂）
  uint32_t split_after_skewed_epochs = 0;

  // ---- M3.2/M3.3 物化与安装（M3_DESIGN.md §4.1）----
  uint32_t materialize_parallelism = 8;  // K：并行归并 worker 数
  bool install_below_l0 = true;          // M3.2 层级下探直装开关
  bool merge_into_base_level = false;    // M3.3 融合归并开关（默认关，逐步放开）
  // M3.3 触发比：封存字节 / 待重写 base-level 字节 ≥ 该值才融合，否则落 L0
  double base_merge_min_ratio = 0.25;
  uint32_t l0_fallback_tolerance = 0;    // 允许的 L0 回落文件数（超出告警）
  // M4.5b：kSkip 攒批物化开关（默认开）。ratio 拒绝的分区跳过物化、
  // 下个 epoch 收养后多代合并——M4.6c 重测（2026-08-25）确认所有历史
  // 风险已清除：数据竞争崩溃（KeySampler/Insert 并发）已修、'重开丢
  // 数据'为 db_bench DestroyDB 误判（--use_existing_db 63.1% 完整）、
  // CURRENT 防御修复已加。实测：2.2GB +28%、50GB 60.0K ops/s（关
  // 55.4K，+8.4%）、fallback 减半、停写 1.3%。
  bool skip_batching = true;

  // ---- M4.3 终态 ----
  // M4.3d-2：分区索引总内存预算（背压触发 freeze；默认 4GB ≈ 6~8 千万
  // key 窗口，与 M4.1 的 write_buffer 语义对齐）。M4.4b：旧路径
  // （zf_global_index/MemTable 外壳）已移除。
  uint64_t index_mem_budget = 4ull << 30;
  // ---- M4.7b：value cache ----
  // WAL 定点读的 value 缓存（键 = locator（part,gen,offset）——WAL 段
  // 不可变，精确命中）。1KB 场景读路径瓶颈（65K vs 原生 312K）的根治。
  // 0 = 关闭（默认）。
  uint64_t value_cache_bytes = 0;
  // ---- M4.6：L0 消费端并行化 ----
  // L0→L1 compaction 的同 CF 并行 job 数（EnqueuePendingCompaction 重复
  // 入队）。align_l1 下 16 分区范围不相交 → 并行消费安全（Register-
  // Compaction 互斥自动排除重叠）。R20 基线（单 job 串行）= 1。
  uint32_t l0_parallelism = 8;
  // R59：批次封存分区数上限（0 = 默认 4）。与 epoch_target_bytes 联动：
  // 批次增大时调用方应同步放大 epoch_target 保持每分区大小（融合 ratio
  // 不受影响）。
  uint32_t max_batch_partitions = 4;

  // ---- M3 起步生长（P0=1 全域 → 等容生长拆分）----
  // 生长拆分阈值：分区 base 字节 ≥ 该值 → 由 zeroflush::GrowPartitions
  // 取该分区条目中位数调用 M2 split（排空检查点；每次切分约减半）。
  // 0 = 关闭生长。默认 32MB（路线图 S_min_split）。
  uint64_t growth_split_min_bytes = 32u << 20;
  // 两次生长拆分的最小间隔（warmup 冷却，微秒）。0 = 无冷却。
  uint64_t growth_split_cooldown_micros = 0;

  // ---- ZeroFlushV0 M4：动态治理 DG（调度器）----
  // 对应路线图 §2 M4。默认全关（v1 显式启用；既有行为零改动）。
  bool dynamic_governance = false;  // 自动后台控制器开关（Open 时启线程）
  // 控制器 tick 周期（微秒；路线图默认 1s）。
  uint64_t governance_tick_interval_micros = 1000ull * 1000;
  // P 界：合并后分区数须 > P_min 才允许；拆分前分区数须 < P_max。
  uint32_t governance_p_min = 4;
  uint32_t governance_p_max = 256;
  // θ_split / θ_merge（rate 相对均值的倍数门槛）。
  double governance_split_theta = 3.0;
  double governance_merge_theta = 0.3;
  // 大小门槛：热拆分要求 base ≥ split_min（默认 32MB）；冷合并要求
  // base ≤ merge_max（默认 16MB）。
  uint64_t governance_split_min_bytes = 32u << 20;
  uint64_t governance_merge_max_bytes = 16u << 20;
  // 连续窗确认数（默认 3 窗——防抖动）。
  uint32_t governance_confirm_windows = 3;
  // 冷却（微秒）：动作成功后同 range 冷却 base，退避倍增封顶 max。
  uint64_t governance_cooldown_micros = 300ull * 1000 * 1000;
  uint64_t governance_cooldown_max_micros = 600ull * 1000 * 1000;
  // 决策队列并发上限 C（v1：每 tick 至多一个动作）。
  uint32_t governance_max_actions_per_tick = 1;
  // 速率 EWMA 双窗（秒）：快窗 1min / 慢窗 5min。
  uint64_t governance_ewma_fast_secs = 60;
  uint64_t governance_ewma_slow_secs = 300;
  // rewrite 带宽份额上限（0.25 = 后台 rewrite 至多占 25% 写 IO 预算；
  // v1 作为动作调度限制的记录/预留参数，见 ZEROFLUSHV0_ROADMAP §2 M4）。
  double governance_rewrite_share = 0.25;
  // 合并"空闲期"判定窗口（µs）：存在速率>0 且距上次封存 ≤ 该值的分区时
  // 抑制合并（活跃期 P 预算让给热拆分）；负载回落/空闲后放行回收。默认
  // 120s（= 2× 快窗）。0 = 任何正速率都算活跃。
  uint64_t governance_merge_idle_micros = 120ull * 1000 * 1000;
  // ZeroFlushV0 M4：活跃尾 split（**实验，默认关**）。SplitPartition 不再要求分区
  // 完全排空——切路由后把父活跃尾冻结为"父区间覆盖的孤儿尾 epoch"；该
  // epoch 物化走**融合替换**（父区间内全部文件，含子分区在切路由后并发
  // 产出的 L1，与尾数据按 seq 归并后单批替换安装——避免宽文件与子分区
  // L1 重叠），子分区索引 miss 时经 split_orphans_ 别名回查父链，排空后
  // 由 type-3 rewrite 按新边界两分。关闭则回退 M2 quiescent 语义（活跃尾
  // 返回 Busy）。
  // ⚠ 残留竞态（实测 16T/1KB/12.8M 写必现）：尾 epoch 的"父区间融合替换"
  // 扫描后，子分区批次可能并发安装新 L1 文件 → VersionBuilder L1 重叠崩溃。
  // 根治需让尾 epoch 同时按两子分区做跨批互斥登记（AddMaterializePart），
  // 并在子分区在飞时让位；当前默认关，待实现后再开。
  bool governance_active_tail_split = false;
};

class ZeroFlushContext {
 public:
  ZeroFlushContext(const ZeroFlushOptions& zfo, const std::string& wal_dir,
                   ROCKSDB_NAMESPACE::Env* env);
  ~ZeroFlushContext();

  // 创建 wal_dir/zfwal 并打开分区写文件。
  ROCKSDB_NAMESPACE::Status Open();

  // ---- 写路径（WriteGroup leader 调用）----
  // 将 write_group 中全部 batch 的记录写入分区 WAL 并插入 Slim MemTable
  // （替代 WriteGroupToWAL + WriteBatchInternal::InsertInto）。
  // seq 区间 [first_seq, first_seq + 已写记录数 - 1] 由调用方保证全局连续。
  // M3.0 R3：本方法只写不 sync；实际触达分区由 touched 输出，sync 由
  // 调用方在释放 DB mutex 后通过 SyncTouchedPartitions 执行（避免持锁
  // fsync，见 M3_DESIGN.md §9）。
  // M4.1c：在调用方 DB mutex 之外执行（多个写组 leader 并发插入同一
  // memtable）。mem 由调用方在锁内捕获并 Ref，组内全部记录进该 mem——
  // 即使中途该 mem 被切为 imm，插入仍合法（InlineSkipList 并发插入），
  // 且本组 seq 更早，Get 先查 active 后查 imm 语义正确。
  ROCKSDB_NAMESPACE::Status WriteGroupToPartitionWal(
      ROCKSDB_NAMESPACE::WriteThread::WriteGroup& wg,
      ROCKSDB_NAMESPACE::SequenceNumber first_seq,
      ROCKSDB_NAMESPACE::MemTable* mem, std::vector<uint32_t>* touched);

  // M4.1c：单个 writer 的并行插入（parallel 路径，follower/leader 各执行
  // 自己的 batch）。seq 已由调用方在锁内分配（w->sequence）；mem 由调用
  // 方捕获并 Ref（锁外读 cfd->mem() 与原生 parallel 一致）。
  // 内部完成本 writer 的 BatchPostProcess（计数 + UpdateFlushState）。
  ROCKSDB_NAMESPACE::Status InsertWriterToPartitionWal(
      ROCKSDB_NAMESPACE::WriteThread::Writer* w,
      ROCKSDB_NAMESPACE::MemTable* mem, const ROCKSDB_NAMESPACE::WriteOptions& write_options,
      uint32_t table_version);

  // M3.0 R3：fdatasync 指定分区（必须在不持 DB mutex 时调用）。
  ROCKSDB_NAMESPACE::Status SyncTouchedPartitions(
      const std::vector<uint32_t>& touched);

  // ---- 读路径（value 解析）----
  // locator_slice 为 16B SlimLocator；value 写入 out。
  ROCKSDB_NAMESPACE::Status ReadValue(const ROCKSDB_NAMESPACE::Slice& locator_slice,
                                      std::string* out) const;

  // ---- 恢复 ----
  // 重放 wal_dir 下全部分区 WAL 到 default CF 的 Slim MemTable，
  // 并设置 last sequence（重放的最大 seq）。
  ROCKSDB_NAMESPACE::Status Recover(ROCKSDB_NAMESPACE::DBImpl* db);

  // M4.10c：持久化 ZFPROPS（全量表版本）。学习安装（kSampled epoch 1 /
  // kAlignL1 每 epoch）后调用——否则运行中安装的新表不落盘，重开恢复
  // 旧表 → 路由错位 → 活跃段数据读不到（R52 实测重开命中 46% vs 63%）。
  ROCKSDB_NAMESPACE::Status PersistZfProps();

  PartitionedWalManager* wal() { return wal_.get(); }
  const ZeroFlushOptions& options() const { return zfo_; }
  const std::string& wal_dir() const { return wal_dir_; }

  // M4.3a：分区索引（终态 L0 索引；Open 时创建，需要 internal comparator）。
  PartitionIndexSet* index_set() { return index_set_.get(); }
  const PartitionIndexSet* index_set() const { return index_set_.get(); }

  // M4.3a：封存时冻结全部分区索引（全局 epoch 粒度；M4.3c 改单分区）。
  void FreezeIndexes(const std::vector<std::pair<uint32_t, uint32_t>>& gens);
  // M4.3a：物化完成（epoch 回收）时释放该 epoch 的 frozen 索引。
  void ReleaseFrozenIndexes(uint64_t epoch);
  // M4.5b：跳过分区的封存 WAL 移交 recovery（攒批——下个 epoch 收养后
  // 多代合并物化）。由 FlushJob 在物化完成后、epoch 回收前调用。
  void HandOffSkippedToRecovery(
      uint64_t epoch, const std::vector<std::pair<uint32_t, uint32_t>>& gens);
  // M4.3a：Get 查分区索引（替代 mem/imm 链）。命中返回 true（含 tombstone）。
  bool GetFromPartitionIndex(
      const ROCKSDB_NAMESPACE::Slice& user_key,
      ROCKSDB_NAMESPACE::SequenceNumber snapshot,
      ROCKSDB_NAMESPACE::Status* s, std::string* value,
      ROCKSDB_NAMESPACE::MergeContext* merge_context,
      const ROCKSDB_NAMESPACE::MergeOperator* merge_op) const;
  bool CheckRangeDelCover(const ROCKSDB_NAMESPACE::Slice& user_key,
                          ROCKSDB_NAMESPACE::SequenceNumber snapshot) const;

  // 路由：M1 用 key 哈希取模（确定性，同 key 同分区 → 正确性不变式成立）；
  // M3.1 由 PartitionTable 的边界二分接替。
  uint32_t Route(const ROCKSDB_NAMESPACE::Slice& user_key) const;
  // M4.10：按指定表版本路由（写组绑定——封存换表期间组内版本一致，
  // 数据路由与物化（se.table_version）用同一表 → 范围断言一致）。
  uint32_t RouteWithVersion(const ROCKSDB_NAMESPACE::Slice& user_key,
                            uint32_t table_version) const;
  // ZeroFlushV0 P1b：Route 下标 → 实际分区 id（合并使 part_ids 稀疏后
  // 下标≠id；写/读路径按 id 记账，必须映射）。hash/无表回退 = 原下标。
  uint32_t RoutePartId(const ROCKSDB_NAMESPACE::Slice& user_key) const;
  uint32_t RoutePartIdWithVersion(const ROCKSDB_NAMESPACE::Slice& user_key,
                                  uint32_t table_version) const;

  // 单条记录：分区 WAL 追加 + Slim MemTable 索引插入（ZfBatchHandler 调用）。
  // M4.1c：并发插入路径（allow_concurrent=true），ppi 按 mem 累计，组
  // 收尾由调用方（WriteGroupToPartitionWal）统一 BatchPostProcess。
  ROCKSDB_NAMESPACE::Status AddRecord(
      ROCKSDB_NAMESPACE::MemTable* mem, const ROCKSDB_NAMESPACE::Slice& key,
      const ROCKSDB_NAMESPACE::Slice& value, ROCKSDB_NAMESPACE::ValueType type,
      ROCKSDB_NAMESPACE::SequenceNumber seq,
      ROCKSDB_NAMESPACE::MemTablePostProcessInfo* ppi,
      uint32_t table_version);

  // ---- M2 新增：Epoch 管理 ----

  // 判定是否需要封存当前活跃代并切换 MemTable。
  // 主条件：∑ActiveSize ≥ epoch_target_bytes；副条件：max ≥ partition_target_bytes。
  bool ShouldSeal() const;

  // 同步执行封存 + 切表（必须持 DB mutex；write thread 独占路径）。
  // 必须由 DBImpl 在 write thread 中调用（impl 持有 DB mutex + 是 leader）。
  // 成功时新 mutable mem 由 DBImpl::SwitchMemtable 接管；旧 mem 进 imm。
  ROCKSDB_NAMESPACE::Status SealEpochAndSwitch(
      ROCKSDB_NAMESPACE::DBImpl* impl,
      ROCKSDB_NAMESPACE::ColumnFamilyData* cfd);

  // M4.3c：终态路径的单分区封存——仅冻结目标分区（WAL 换代 + 索引冻结 +
  // 单分区 epoch 登记 + 空 mem 切换触发物化），其余分区写入不受影响。
  // 目标分区选择：超限（≥ partition_target）优先，否则活跃字节最大。
  // 必须持 DB mutex；由写路径 leader 在 ShouldSeal() 时调用。
  ROCKSDB_NAMESPACE::Status FreezeOnePartition(
      ROCKSDB_NAMESPACE::DBImpl* impl,
      ROCKSDB_NAMESPACE::ColumnFamilyData* cfd);

  // M4.3d-1：批次封存——一次 epoch 冻结多个分区（超限分区优先，补充最大
  // 分区至批次上限），物化作业数 = 批次频率（避免单分区小作业的物化
  // 串行开销），同时保留"分区满即换代"的及时性（WAL 不再增长）。
  // 必须持 DB mutex；由写路径 leader 在 ShouldSeal() 时调用（终态路径）。
  ROCKSDB_NAMESPACE::Status FreezeBatchPartitions(
      ROCKSDB_NAMESPACE::DBImpl* impl,
      ROCKSDB_NAMESPACE::ColumnFamilyData* cfd);

  // M4.2b：按当前 L1 层 SST 文件边界构建对齐分区表（compaction 感知分区）。
  // 桶聚合：目标分区数 = zfo_.partitions；L1 文件数 ≤ 目标时每文件一桶；
  // 桶边界 = 桶末文件的 largest user key（精确文件边界 → L0/L1 1:1 对齐）。
  // L1 为空或无法形成边界时返回 false（调用方保持当前表）。
  // REQUIRES: DB mutex held（cfd->current() 访问）。
  bool BuildBaseAlignedTable(
      ROCKSDB_NAMESPACE::ColumnFamilyData* cfd,
      std::shared_ptr<PartitionTable>* out) const;

  // 释放一个 epoch 的封存文件引用。引用归零时由 SealedFileCache 排队
  // 等待 PurgeSealedFiles() unlink，并返回该 epoch 的封存字节
  // （M3.0：物化完成统计，>0 表示该 epoch 已物化且引用归零）。
  uint64_t ReleaseEpoch(uint64_t epoch);

  // 由 DBImpl::PurgeObsoleteFiles 周期调用，真实 unlink 封存代文件。
  // 返回实际 unlink 的文件数（M3.0 统计用）。
  size_t PurgeSealedFiles();

  // 当前 epoch 计数（仅递增）。用于诊断与测试。
  uint64_t epoch_counter() const { return epoch_counter_.load(); }

  // 已物化/已封存但未回收字节（统计）。
  uint64_t sealed_bytes() const;

  // M4.8：kSkip 攒批等待集合字节（L0 遮蔽等待上限判定）。
  uint64_t skipped_bytes() const;

  // 当前未物化 epoch 数（cfd->imm() 大小）— 用于写流控。
  uint32_t pending_epochs(ROCKSDB_NAMESPACE::ColumnFamilyData* cfd) const;

  // ---- M3.1 路由访问器 ----

  // 当前最新 PartitionTable 的引用（写路径 Route 的目标）。
  // 线程安全：读写 atomic current_version，写时在 Seal 独占窗口内。
  std::shared_ptr<class PartitionTable> current_table() const;

  // PartitionTableSet 容器（供 Seal 写入新版本、物化时取历史版本）。
  class PartitionTableSet* tables() const { return tables_.get(); }

  // 用户比较器（从 cfd->user_comparator() 取得，ucmp->Name() 写入 ZFPROPS）。
  const ROCKSDB_NAMESPACE::Comparator* ucmp() const { return ucmp_; }
  void set_ucmp(const ROCKSDB_NAMESPACE::Comparator* c) { ucmp_ = c; }

  // kSampled 采样器。
  class KeySampler* sampler() const { return sampler_.get(); }

  // ---- M3.0：zf.* 统计指标（经 DBImpl::GetProperty 暴露）----

  // 解析 "rocksdb.zeroflush.<name>"；未知 name 返回 false。
  // 已知 name 见 M3_DESIGN.md §13 表。
  bool GetProperty(const std::string& prop, std::string* value) const;

  // 单指标访问器（测试与诊断直接调用）。
  uint64_t epochs_materialized() const;
  uint64_t epochs_reclaimed() const;
  uint64_t live_wal_bytes() const;    // 全部活跃代字节（未封存）
  uint64_t materialize_micros() const;  // 封存→物化完成墙钟耗时累计
  uint64_t sealed_read_count() const;
  uint64_t sealed_cache_miss() const;
  size_t recovery_count() const;      // 待收养恢复期孤儿代数
  // M5 §3.5-2：待物化数据存在性（skip 攒批 / 恢复孤儿 / 未物化 epoch）——
  // 关闭冲刷循环用。skip 数据跨 DB 关闭滞留 = 数据完整性违约（读命中门）。
  bool HasPendingSealedData() const;
  double partition_skew() const;      // max(ActiveSize) / avg(ActiveSize)

  // ---- M3.2：物化状态与统计（M3_DESIGN.md §6/§13）----

  // 取 epoch 的封存登记信息（物化用：gens/table_version/字节等）。
  // 未登记（未知 epoch 或已回收）返回 false。
  bool GetSealedEpoch(uint64_t epoch, SealedEpoch* out) const;

  // 已物化完成的最后一个 epoch 号（FlushJob 成功后推进；0 = 尚无）。
  // ZfMaterializeJob::Run 入口用它做按序物化断言。
  uint64_t last_materialized_epoch() const;
  void SetLastMaterializedEpoch(uint64_t e);

  // M3.2 指标：直装 base_level / 回落 L0 的文件数；物化排序累计耗时。
  uint64_t install_direct_base() const;
  uint64_t install_fallback_l0() const;
  uint64_t materialize_sort_micros() const;

  // M3.3 指标：融合归并次数（按分区计）与被重写的 base 层字节
  // （M3_DESIGN.md §7.2/§13）。
  uint64_t base_merge_count() const;
  uint64_t base_merge_rewritten_bytes() const;

  // M4.5b 指标：kSkip 攒批跳过的分区次数（决策时累计）。
  uint64_t skip_count() const;

  // ---- M5（zeroflush0.98）下沉请求 ----
  // 物化决策（PlanLocked 判据 3：L1 文件已满/接近满，merge 输出必超
  // kRangeMergeBytes）请求原生 L1→L2 下沉。请求按分区 key 边界记录
  // （lo/hi 为半开区间 [lo,hi) 的拷贝——GetPartRange 的 Slice 指向路由表
  // 内部存储，须拷贝存续），由 DBImpl 在 bg 调度点消费（MaybeSchedule
  // 内构造 prepicked compaction 交 bg 执行，异步、不等待）。
  // 请求方在持 DB mutex 的决策窗口调用（不取本锁——锁内仅 O(n) 去重
  // 扫描，n = 待处理 range 数 ≤ 分区数）。
  void RequestRangeSink(const ROCKSDB_NAMESPACE::Slice& lo,
                        const ROCKSDB_NAMESPACE::Slice& hi);
  // 消费：取走全部未处理请求（内部锁；DBImpl 在 MaybeSchedule 持 DB
  // mutex 时调用——下沉请求入队自物化决策（同持锁窗口），无死锁序）。
  std::vector<std::pair<std::string, std::string>> TakeSinkRequests();
  // 累计下沉请求次数（诊断/统计：rocksdb.zeroflush.sink_requests）。
  uint64_t sink_request_count() const;

  // ---- M5P1b：多 flush 并行——跨批分区互斥登记（flush 批次级）----
  // max_background_flushes>1 后多个批次可同时在飞（imm 堆积流水）。同
  // 分区跨批并发产出破坏"每 range 单文件"不变量与 gen 序（后批决策看
  // 不到前批未安装输出）。批在计划阶段（持 DB mutex）登记产出分区；他
  // 批计划同分区 → 不产出（kSkip 转 recovery，本批结束释放后下批收养
  // 重物化）。登记随批次结束（安装/失败出口）统一 End 释放。
  // 单任务批（学习齐批/hash 全范围全量读取）登记全部分区，任一被占 →
  // 整批 kSkip（单任务无法只产出部分分区）。
  uint64_t BeginMaterializeBatch();
  // 登记 part 到 token 批。false = 分区正被他批（不同 token）持有；
  // 同 token 重复登记幂等（返回 true）。
  bool AddMaterializePart(uint64_t token, uint32_t part_id);
  void EndMaterializeBatch(uint64_t token);

  // ---- M5P1b：学习窗 gen0 批串行槽 ----  // 含 gen0 代（学习期 hash 数据）的 flush 批必须串行执行（同一时刻至多
  // 一个在飞）：学习批的切片输出依赖串行语义（各批按自身 gens 分区产出、
  // 互不重叠、L1 为空前提），并发会让切片与稳态安装/他批切片重叠（Release
  // 实测 #197/#207 等）。稳态批不受限（与在飞学习批的冲突由全分区登记
  // 转 kSkip 让位，数据走 recovery 下批收养合并——学习批自身不 skip，
  // gen0 代不进入 recovery 循环）。AcquireGen0Slot 在批计划前调用（可能
  // 等待在飞学习批完成；等待期间释放 DB mutex）；Release 在批结束（安装
  // 或失败）后调用。单 flush（串行模式）下无并发批，槽位恒可获取。
  void AcquireGen0Slot();
  void ReleaseGen0Slot();
  // 稳态批等待学习窗关闭（无 gen0 批在飞/等待）后才开始规划。
  void WaitGen0WindowClosed();
  // 诊断：当前被他批登记占用的分区数（skip 循环定位）。
  size_t ActiveMaterializeParts() const;

  // ---- M5 §3.5-2：关闭冲刷模式 ----
  // DB 关闭冲刷期间置位：物化决策跳过 kSkip 判据 2/3（F 满也强制融合，
  // 输出按 target_file_size 切分——单文件 ≤64MB 约束为稳态性能目标，
  // 关闭一次性场景放宽；否则 F 满分区每轮冲刷都判据 3 转 recovery →
  // 循环永不收敛，跨关闭滞留违约）。冲刷由 DBImpl::ZfCloseFlush 驱动
  // （Seal 收养 + Flush 物化循环）。
  void SetClosingFlush(bool on) {
    closing_flush_.store(on, std::memory_order_relaxed);
  }
  bool closing_flush() const {
    return closing_flush_.load(std::memory_order_relaxed);
  }

  // ---- ZeroFlushV0 P1/P1b（S4 merge：取消/合并分区）----
  // 把分区 drain_part 的路由区间并入相邻分区（优先左邻）。调用前提：
  // 持 DB mutex、调用线程不是写组成员。过程：(1) 等待在途写组归零；
  // (2) 前置检查：drain 不得有 pending sealed/skip/recovery 残留（frozen
  // 索引、未回收 WAL 段必须为空）——仅允许"活跃尾"（ActiveSize>0，本次
  // 就地冻结）或完全排空的冷分区；(3) 若有活跃尾：单分区封存为 epoch E_D
  //（se.table_version = 当前表 V，drain 仍在 V 中 → E_D 按 drain 原区间
  // 干净排空，先于吸收方任何 post-merge epoch 的 FIFO 保证读序正确）；(4)
  // 派生并安装去掉 drain 的新表版本（路由切换，对新写即时生效）；(5) 登记
  // DRAINING（排空窗口别名读 + retire）；(6) ZFPROPS 落盘（重开路由一致）。
  // 正确性：drain 旧版本只在 E_D（及其后无新写）中，先于吸收方更新版本
  // 落地（FIFO + 本版默认 merge-off 下 drain 直装不攒批，F1 平凡满足）；
  // merge_into_base_level=true 的 F1 强制收口见 M1b（PlanLocked）。
  // absorb_out 返回实际吸收方（左邻优先，drain 为首分区时右邻）。
  ROCKSDB_NAMESPACE::Status MergePartitionLocked(
      ROCKSDB_NAMESPACE::DBImpl* impl, ROCKSDB_NAMESPACE::ColumnFamilyData* cfd,
      uint32_t drain_part, uint32_t* absorb_out);

  // ---- ZeroFlushV0 P2（M2）：split（quiescent 子集）----
  // 把分区 part 的路由区间在 split_key 处一分为二。调用前提：持 DB mutex、
  // 调用线程不是写组成员。过程：(1) 等待在途写组归零；(2) 前置检查：分区
  // 完全排空（无活跃字节、无 frozen 索引、无 pending sealed/skip/recovery
  // 残留）且不处于 DRAINING/别名方；(3) 分配两个子分区 id（父 id 退役）；
  // (4) DeriveSplit 派生新表版本并安装（路由切换，对新写即时生效——两子
  // 分区收新写）；(5) ZFPROPS 落盘（重开路由一致）。返回两子分区 id。
  // 物理层（跨界 base 文件按 b 二分 = type-3 rewrite）由调用方
  // （DBImpl::ZfSplitPartition）在返回后执行——本方法不触碰 SST；若 rewrite
  // 失败路由已切换但数据仍完整（文件未对齐仅影响后续压缩效率，读不受影响）。
  // ZeroFlushV0 M4（活跃尾 split）：活跃尾（ActiveSize>0）不再拒绝——切路由
  // 后把父活跃尾冻结为带父区间覆盖的孤儿尾 epoch（尾数据排空前由
  // split_orphans_ 别名读 + 排空后 type-3 rewrite 两分），*tail_epoch_out
  // 返回该 epoch（0 = 无活跃尾/纯路由 split）。
  ROCKSDB_NAMESPACE::Status SplitPartitionLocked(
      ROCKSDB_NAMESPACE::DBImpl* impl, ROCKSDB_NAMESPACE::ColumnFamilyData* cfd,
      uint32_t part, const ROCKSDB_NAMESPACE::Slice& split_key,
      uint32_t* child_l, uint32_t* child_r,
      uint64_t* tail_epoch_out = nullptr);

  // M3 起步生长：把分区 part 的活跃尾冻结为单分区 epoch（镜像 merge E_D 的
  // 冻结段，不做路由切换/DRAINING——用于生长拆分前的排空：冻结后调用方
  // Flush(wait) 把该 epoch 物化进 base，分区即 quiescent，可走正常 split）。
  // 持 DB mutex（写路径 seal 同窗口）。ActiveSize(part)==0 时 no-op OK。
  ROCKSDB_NAMESPACE::Status FreezePartitionTailLocked(
      ROCKSDB_NAMESPACE::DBImpl* impl, ROCKSDB_NAMESPACE::ColumnFamilyData* cfd,
      uint32_t part, uint64_t* epoch_out);

  // 分区是否处于 DRAINING（被合并、排空未完成）。PlanLocked F1 与读别名共用。
  bool IsDrainingPartition(uint32_t part) const;
  // DRAINING 分区数（诊断/测试：rocksdb.zeroflush.draining_partitions）。
  size_t DrainingPartitions() const;
  // 已 RETIRED（排空完成移除登记）的合并分区累计数。
  uint64_t MergeRetired() const {
    return merge_retired_.load(std::memory_order_relaxed);
  }
  // 排空完成检查：DRAINING 分区无未回收 WAL 段、无 frozen 索引、无活跃
  // 字节 → RETIRED（移除登记）。在 ReleaseEpoch/ReleaseGens 等回收点调用。
  void MaybeRetireDraining();
  // ZeroFlushV0 M4（活跃尾 split）：孤儿尾 epoch 排空完成 → 移除别名登记
  // （数据已进 SST，读走原生 SST 查找）。同回收点调用。
  void MaybeRetireSplitOrphans();
  size_t SplitOrphans() const;
  // ZeroFlushV0 P1c（M1c①）：DRAINING 记录持久化（独立 sidecar 文件
  // wal_dir/ZF_DRAINING，避开 ZFPROPS 格式改动）。登记/retire 时调用写全量
  // 快照（原子 tmp+rename）；ZeroFlushContext::Open 时 LoadDrainingFile 重建
  // 注册表——崩溃后 F1/retire 得以跨重启延续。读正确性不依赖它（恢复期重放
  // 已按当前表路由落吸收方链）；文件损坏/缺失按空注册表容忍（best-effort）。
  void SyncDrainingFile();
  void LoadDrainingFile();

  // ---- ZeroFlushV0 P2（M2）：分区 id 分配器（split 用）----
  // split 时给两个子分区分配新的全局 part id（父 id 退役）。id 单调不回退：
  // merge 移除的分区 id 保持空缺、split 分配的 id 不复用——历史 WAL gens /
  // 分区索引 / DRAINING 别名按原 id 记账的前提。初始化：Open（Recover 后）
  // 调 RebuildNextPartId——按「全部持久化表版本 part_ids ∪ zfwal 文件 part」
  // 的 max 重建（表版本全量落 ZFPROPS，任意分配过的 id 必在某个表版本或残留
  // 文件中，故无需单独持久化计数器）。分配只发生在持 DB mutex 的
  // Split/MergePartitionLocked 窗口（无锁，调用方保证互斥）。
  uint32_t AllocatePartId();
  void RebuildNextPartId();

  // 在途写组计数（路由切换的写屏障）：写组 leader 在"路由表版本捕获"点
  // +1（持 DB mutex）；组内全部记录追加+索引插入完成后 -1（恰好一次/组，
  // 见 db_impl_write.cc 埋点）。MergePartitionLocked 切换前等待归零——切表
  // 后不存在仍按旧版本路由、会把记录追加进已被移除分区的在途写组。
  void EnterInflightGroup();
  void ExitInflightGroup();
  uint32_t InflightWriterGroups() const;

  // 已成功合并（路由切换）的分区累计数（诊断/测试）。
  uint64_t MergeCount() const {
    return merge_count_.load(std::memory_order_relaxed);
  }

  // ZeroFlushV0 P2（M2）：已成功 split（路由切换）的分区累计数（诊断/测试：
  // rocksdb.zeroflush.splits）。
  uint64_t SplitCount() const {
    return split_count_.load(std::memory_order_relaxed);
  }

  // ---- M3 起步生长：生长拆分冷却 ----
  // warmup 冷却：距上次生长 split 已过 growth_split_cooldown_micros 才允许
  // 再次生长拆分（0 = 无冷却恒真）。GrowPartitions 成功 split 后 Touch。
  bool GrowthCooldownElapsed(uint64_t now_micros) const {
    const uint64_t last =
        growth_last_split_micros_.load(std::memory_order_relaxed);
    const uint64_t cd = zfo_.growth_split_cooldown_micros;
    return cd == 0 || last == 0 || now_micros >= last + cd;
  }
  void TouchGrowthSplit(uint64_t now_micros) {
    growth_last_split_micros_.store(now_micros, std::memory_order_relaxed);
  }

  // ---- ZeroFlushV0 M4（动态治理 DG）：统计与决策 ----
  // 每分区速率 EWMA 追踪器（@封存边界采样，见 repartition_controller.h）。
  PartitionRateTracker* rate_tracker() const { return rate_tracker_.get(); }
  // 治理决策策略（连续窗/冷却退避/pending aging；DBImpl tick 持有）。
  GovernancePolicy* governance_policy() const { return governance_policy_.get(); }
  // 封存登记统一入口：记录每分区速率样本后转 sealed_cache 登记。全部
  // epoch 创建点（批次/单分区/全域/E_D/排空冻结）经此——速率统计不进写路径。
  void RegisterSealedEpoch(const SealedEpoch& se);
  // 该分区是否有未物化/未回收数据（执行 Busy 预判）：pending sealed WAL
  // gens / frozen 分区索引任一存在。
  bool HasPendingPartData(uint32_t part_id) const;
  // DRAINING 注册表快照（控制器判断 DRAINING 方与别名吸收方）。
  void DrainingSnapshot(std::vector<DrainingPart>* out) const;
  // M4 诊断计数（治理 tick / 治理触发的 split / merge）。
  void NoteGovernanceTick() {
    gov_ticks_.fetch_add(1, std::memory_order_relaxed);
  }
  void NoteGovernanceSplit() {
    gov_splits_.fetch_add(1, std::memory_order_relaxed);
  }
  void NoteGovernanceMerge() {
    gov_merges_.fetch_add(1, std::memory_order_relaxed);
  }
  uint64_t governance_ticks() const {
    return gov_ticks_.load(std::memory_order_relaxed);
  }
  uint64_t governance_splits() const {
    return gov_splits_.load(std::memory_order_relaxed);
  }
  uint64_t governance_merges() const {
    return gov_merges_.load(std::memory_order_relaxed);
  }
  // type-3 rewrite 累计输出字节（治理带宽账本/诊断）。
  void AddRewriteBytes(uint64_t b) {
    rewrite_bytes_.fetch_add(b, std::memory_order_relaxed);
  }
  uint64_t rewrite_bytes() const {
    return rewrite_bytes_.load(std::memory_order_relaxed);
  }
  // M5：常规物化输出累计字节（rewrite 带宽账本的分母；prof_output_bytes_）。
  uint64_t prof_output_bytes_total() const {
    return prof_output_bytes_.load(std::memory_order_relaxed);
  }
  void NoteGovernanceRewriteGated() {
    gov_rewrite_gated_.fetch_add(1, std::memory_order_relaxed);
  }
  uint64_t governance_rewrite_gated() const {
    return gov_rewrite_gated_.load(std::memory_order_relaxed);
  }
  // M5 诊断：拆分准入结果分解（attempts = 进入门控的候选次数；
  // gate_base = base 字节 < split_min；median_nofiles = base 层无本分区文件
  // 可切；success = 成功）。gov_last_split_base/l0_bytes 为最近一次候选的
  // base 层/L0 字节（gauge，定位"规模下热拆分饥饿"）。
  void NoteGovSplitAttempt(uint64_t base_b, uint64_t l0_b) {
    gov_split_attempts_.fetch_add(1, std::memory_order_relaxed);
    gov_last_split_base_.store(base_b, std::memory_order_relaxed);
    gov_last_split_l0_.store(l0_b, std::memory_order_relaxed);
  }
  void NoteGovSplitGateBase() {
    gov_split_gate_base_.fetch_add(1, std::memory_order_relaxed);
  }
  void NoteGovSplitMedianNoFiles() {
    gov_split_median_nofiles_.fetch_add(1, std::memory_order_relaxed);
  }
  void NoteGovSplitSuccess() {
    gov_split_success_.fetch_add(1, std::memory_order_relaxed);
  }
  uint64_t gov_split_attempts() const {
    return gov_split_attempts_.load(std::memory_order_relaxed);
  }
  uint64_t gov_split_gate_base() const {
    return gov_split_gate_base_.load(std::memory_order_relaxed);
  }
  uint64_t gov_split_median_nofiles() const {
    return gov_split_median_nofiles_.load(std::memory_order_relaxed);
  }
  uint64_t gov_split_success() const {
    return gov_split_success_.load(std::memory_order_relaxed);
  }
  uint64_t gov_last_split_base() const {
    return gov_last_split_base_.load(std::memory_order_relaxed);
  }
  uint64_t gov_last_split_l0() const {
    return gov_last_split_l0_.load(std::memory_order_relaxed);
  }
  void NoteGovSplitFail(int kind) {  // 0=busy 1=timeout 2=other 3=median-notfound
    if (kind == 0) gov_split_busy_.fetch_add(1, std::memory_order_relaxed);
    else if (kind == 1) gov_split_timeout_.fetch_add(1, std::memory_order_relaxed);
    else if (kind == 2) gov_split_other_.fetch_add(1, std::memory_order_relaxed);
    else gov_split_median_notfound_.fetch_add(1, std::memory_order_relaxed);
  }
  uint64_t gov_split_busy() const { return gov_split_busy_.load(std::memory_order_relaxed); }
  uint64_t gov_split_timeout() const { return gov_split_timeout_.load(std::memory_order_relaxed); }
  uint64_t gov_split_other() const { return gov_split_other_.load(std::memory_order_relaxed); }
  uint64_t gov_split_median_notfound() const { return gov_split_median_notfound_.load(std::memory_order_relaxed); }
  void NoteGovSplitSilent(int kind) {  // 0=pos_gone 1=total0
    if (kind == 0) gov_split_pos_gone_.fetch_add(1, std::memory_order_relaxed);
    else gov_split_total0_.fetch_add(1, std::memory_order_relaxed);
  }
  uint64_t gov_split_pos_gone() const { return gov_split_pos_gone_.load(std::memory_order_relaxed); }
  uint64_t gov_split_total0() const { return gov_split_total0_.load(std::memory_order_relaxed); }
  void NoteGovSplitPreDrain() { gov_split_predrain_.fetch_add(1, std::memory_order_relaxed); }
  uint64_t gov_split_predrain() const { return gov_split_predrain_.load(std::memory_order_relaxed); }
  // 治理动作在飞（merge/split 执行中，含排空等待）——收敛/稳定判据用。
  void EnterGovAction() {
    gov_action_in_flight_.fetch_add(1, std::memory_order_relaxed);
  }
  void ExitGovAction() {
    gov_action_in_flight_.fetch_sub(1, std::memory_order_relaxed);
  }
  uint32_t GovActionInFlight() const {
    return gov_action_in_flight_.load(std::memory_order_relaxed);
  }

  ZeroFlushOptions zfo_;
  std::string wal_dir_;       // wal_dir/zfwal
  ROCKSDB_NAMESPACE::Env* env_;
  std::unique_ptr<PartitionedWalManager> wal_;
  // ---- M2 成员 ----
  std::unique_ptr<class SealedFileCache> sealed_cache_;
  std::atomic<uint64_t> epoch_counter_{0};
  // ---- M3.1 路由 ----
  std::unique_ptr<class PartitionTableSet> tables_;
  const ROCKSDB_NAMESPACE::Comparator* ucmp_ = nullptr;  // user comparator
  std::unique_ptr<class KeySampler> sampler_;  // kSampled/kAlignL1 学习期采样器
  // M4.10：学习表物化越界计数（hash 学习期遗留——监控告警用）。
  std::atomic<uint64_t> materialize_oob_count_{0};
  // ---- M4.3 终态 ----
  std::unique_ptr<class PartitionIndexSet> index_set_;  // 分区索引（L0 索引）
  // M4.7b：value cache（LRU；键 = locator 16B，值 = std::string*）。
  mutable std::shared_ptr<ROCKSDB_NAMESPACE::Cache> value_cache_;
  // ---- M3.2 物化状态与统计 ----
  // 物化按序推进（imm FIFO 单后台线程）；由 FlushJob 成功后更新。
  std::atomic<uint64_t> last_materialized_epoch_{0};
  std::atomic<uint64_t> install_direct_base_{0};    // 直装 base_level 文件数
  std::atomic<uint64_t> install_fallback_l0_{0};    // 回落 L0 文件数
  std::atomic<uint64_t> materialize_sort_micros_{0};  // 物化排序累计耗时
  // ---- M3.3 融合归并统计 ----
  std::atomic<uint64_t> base_merge_count_{0};       // 融合归并次数（按分区）
  std::atomic<uint64_t> base_merge_rewritten_bytes_{0};  // 被重写 base 字节
  // ---- M4.5b 攒批统计 ----
  std::atomic<uint64_t> skip_count_{0};  // kSkip 攒批跳过的分区次数
  // ---- M5 下沉请求状态 ----
  std::atomic<bool> closing_flush_{false};  // 关闭冲刷模式（强制物化）
  mutable std::mutex sink_mu_;
  std::vector<std::pair<std::string, std::string>> sink_requests_;  // (lo, hi)
  std::atomic<uint64_t> sink_request_count_{0};  // 累计请求次数（诊断）
  // ---- M5P1b 跨批互斥登记状态（mm_*：materialize mutex 缩写）----
  // 持锁方：登记在 PlanLocked（持 DB mutex）窗口、释放/End 在批次出口
  // （持 DB mutex）——锁序恒 DB mutex → mm_mu_（无反向获取），无死锁。
  mutable std::mutex mm_mu_;
  uint64_t mm_token_next_ = 1;
  std::unordered_map<uint64_t, std::unordered_set<uint32_t>> mm_batch_parts_;
  std::unordered_set<uint32_t> mm_active_parts_;
  // ---- 物化分相剖析（2026-09-07 吞吐归因）----
  // 每分区任务的阶段耗时/字节累积：scan = A 侧封存 WAL 顺序读；
  // build = 直装/切片建表（含排序）；merge = 融合路径 B 侧迭代器 +
  // CompactionIterator + 输出建表（含排序）。output_bytes = 建表输出。
  std::atomic<uint64_t> prof_scan_us_{0};
  std::atomic<uint64_t> prof_scan_bytes_{0};
  std::atomic<uint64_t> prof_build_us_{0};
  std::atomic<uint64_t> prof_merge_us_{0};
  std::atomic<uint64_t> prof_output_bytes_{0};
  std::atomic<uint64_t> prof_tasks_{0};

  // M5P1b：学习窗 gen0 批串行槽（g0_* 保护；Acquire 等待时释放 DB mutex，
  // 锁序 DB mutex → g0_mu_ 恒单向）。
  //  g0_running_：在飞学习批数（0/1——学习批互斥执行）；
  //  g0_pending_：已取槽未释放的学习批数（稳态批等它归零 = 学习窗关闭）；
  //  g0_waiters_：等待取槽的学习批数——稳态批必须等它归零（学习批优先：
  //  防「稳态批先规划占分区 → 学习批冲突让位 → gen0 代进 recovery 循环」）。
  mutable std::mutex g0_mu_;
  std::condition_variable g0_cv_;
  uint32_t g0_running_ = 0;
  uint32_t g0_pending_ = 0;
  uint32_t g0_waiters_ = 0;
  // ---- ZeroFlushV0 P1/P1b 成员 ----
  // 在途写组计数（见 EnterInflightGroup 注释）。
  std::atomic<uint32_t> inflight_groups_{0};
  std::atomic<uint64_t> merge_count_{0};
  // ZeroFlushV0 P2（M2）：已成功 split 的分区累计数（SplitCount）。
  std::atomic<uint64_t> split_count_{0};
  // M3 起步生长：上次生长 split 时刻（NowMicros；0 = 尚未生长拆分）。
  std::atomic<uint64_t> growth_last_split_micros_{0};
  // DRAINING 分区登记（别名读回查 + retire 判定）。
  mutable std::mutex draining_mu_;
  std::vector<DrainingPart> draining_;
  // 登记数快照（别名读热路径的免锁门：为 0 时 Get 不取 draining_mu_）。
  std::atomic<uint32_t> draining_active_{0};
  std::atomic<uint64_t> merge_retired_{0};
  // ZeroFlushV0 M4（活跃尾 split）：孤儿尾 epoch 别名登记（读回查 + retire）。
  mutable std::mutex split_orphan_mu_;
  std::vector<SplitOrphanPart> split_orphans_;
  std::atomic<uint32_t> split_orphan_active_{0};
  // ---- ZeroFlushV0 P2（M2）：分区 id 分配器状态 ----
  // 下一个可分配的分区全局 id（见 AllocatePartId）。仅分配窗口（持 DB
  // mutex）读写；Open 时 RebuildNextPartId 初始化。
  uint32_t next_part_id_ = 0;
  // ---- ZeroFlushV0 M4（动态治理 DG）成员 ----
  std::unique_ptr<PartitionRateTracker> rate_tracker_;  // @封存边界速率 EWMA
  std::unique_ptr<GovernancePolicy> governance_policy_;  // 决策策略状态
  std::atomic<uint64_t> gov_ticks_{0};
  std::atomic<uint64_t> gov_splits_{0};
  std::atomic<uint64_t> gov_merges_{0};
  std::atomic<uint64_t> rewrite_bytes_{0};
  std::atomic<uint32_t> gov_action_in_flight_{0};
  std::atomic<uint64_t> gov_rewrite_gated_{0};
  std::atomic<uint64_t> gov_split_attempts_{0};
  std::atomic<uint64_t> gov_split_gate_base_{0};
  std::atomic<uint64_t> gov_split_median_nofiles_{0};
  std::atomic<uint64_t> gov_split_success_{0};
  std::atomic<uint64_t> gov_last_split_base_{0};
  std::atomic<uint64_t> gov_last_split_l0_{0};
  std::atomic<uint64_t> gov_split_busy_{0};
  std::atomic<uint64_t> gov_split_timeout_{0};
  std::atomic<uint64_t> gov_split_other_{0};
  std::atomic<uint64_t> gov_split_median_notfound_{0};
  std::atomic<uint64_t> gov_split_pos_gone_{0};
  std::atomic<uint64_t> gov_split_total0_{0};
  std::atomic<uint64_t> gov_split_predrain_{0};
};

// 打开 ZeroFlush DB：
//  - 拷贝 opt 并把 memtable_factory 替换为 SlimMemTableRepFactory；
//  - 以原生 DB::Open 打开（MANIFEST/VersionSet 全复用）；
//  - 创建 ZeroFlushContext 挂到 DBImpl；
//  - 重放已有分区 WAL（恢复）。
// opt 按值传入（内部拷贝修改 memtable_factory，不污染调用方选项）。
ROCKSDB_NAMESPACE::Status Open(const ROCKSDB_NAMESPACE::Options& opt,
                               const ZeroFlushOptions& zfo,
                               const std::string& dbname,
                               std::unique_ptr<ROCKSDB_NAMESPACE::DB>* db);

// 关闭（DBImpl 析构时自动释放 ctx；此处仅显式解挂，供测试用）。
void Close(std::shared_ptr<ROCKSDB_NAMESPACE::DB>* db);

// ZeroFlushV0 P1：外部触发"取消/合并冷分区"（S4 merge 入口）。
// 把分区 drain_part 合并进相邻分区（优先左邻），路由切换对新写即时生效；
// drain 的历史 L1 文件保留、由吸收方后续常规合并吸收。absorb_out 返回
// 实际吸收方。适用范围：范围路由模式（kStatic/kSampled/kAlignL1 已安装
// 范围表后）；hash 模式或 drain 仍有未物化数据（P1b 前的限制）返回错误。
// 线程安全：内部取 DB mutex 并等待在途写组归零；调用线程不得是写组成员
// （正常前台/管理线程即可）。
ROCKSDB_NAMESPACE::Status MergePartition(ROCKSDB_NAMESPACE::DB* db,
                                         uint32_t drain_part,
                                         uint32_t* absorb_out);

// ZeroFlushV0 P2（M2）：外部触发"切分分区"（split 入口，quiescent 子集 +
// type-3 rewrite）。把分区 part 的路由区间在 split_key 处一分为二：分区须
// 完全排空（数据全在 SST）且不处于 DRAINING/别名窗口；跨界 base 文件按
// split_key 二分（单文件替换为两子文件）。child_l/child_r 返回两子分区 id。
// 适用范围：范围路由模式。线程安全：内部取 DB mutex 并等待在途写组归零；
// 调用线程不得是写组成员（正常前台/管理线程即可）。注意 rewrite 为持锁
// 内联执行（v1），跨界文件 ≤256MB。
ROCKSDB_NAMESPACE::Status SplitPartition(
    ROCKSDB_NAMESPACE::DB* db, uint32_t part,
    const ROCKSDB_NAMESPACE::Slice& split_key, uint32_t* child_l,
    uint32_t* child_r);

// M3 起步生长入口：排空检查点生长拆分。循环把 base 字节 ≥
// zfo.growth_split_min_bytes 的分区按条目中位数切分（≈减半），冷却窗限频；
// 无可生长分区或分区未排空（Busy）时停止。splits_out 返回本次生长 split
// 数。调用线程不得是写组成员。
ROCKSDB_NAMESPACE::Status GrowPartitions(ROCKSDB_NAMESPACE::DB* db,
                                         uint32_t* splits_out);

// ZeroFlushV0 M4：运行一次动态治理 tick——统计/决策/至多执行一个
// merge 或 split 动作（控制器调度而非手动 API）。后台自动模式由
// zfo.dynamic_governance 开启（Open 时启控制器线程，按
// governance_tick_interval_micros 周期自驱）；本入口供测试与外部低频
// 驱动确定性调用。调用线程不得是写组成员。返回动作尝试结果（Busy 由
// 策略 aging 自动重试，不视为失败）。
ROCKSDB_NAMESPACE::Status RunGovernanceTick(ROCKSDB_NAMESPACE::DB* db);

// ZeroFlushV0 M4：停止后台动态治理控制器（join 后台线程；DB 保持打开）。
// 用于验收/测试在测量前冻结治理状态（metrics/P 不再被后台动作改变）。
// 幂等；停止后仍可 RunGovernanceTick 手动驱动。
ROCKSDB_NAMESPACE::Status StopGovernance(ROCKSDB_NAMESPACE::DB* db);

// 销毁 ZeroFlush DB：先调原生 DestroyDB 清理 manifest/sst/log，再递归
// 删除 wal_dir/zfwal 子目录（含 ZFPROPS 与全部代文件）。即使原生 DB
// 已不存在，zfwal 也保证被清理。原 DestroyDB 状态作为返回值传递，
// zfwal 清理异常用 PermitUncheckedError 吞掉（best-effort）。
ROCKSDB_NAMESPACE::Status DestroyDB(const std::string& dbname,
                                    const ROCKSDB_NAMESPACE::Options& options,
                                    const ZeroFlushOptions& zfo);

}  // namespace zeroflush
