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

#include <memory>
#include <string>

#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "db/write_thread.h"

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
  uint64_t max_pending_epochs = 2;     // 未物化 epoch 上限（用于流控）
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
  bool BuildL1AlignedTable(
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

// 销毁 ZeroFlush DB：先调原生 DestroyDB 清理 manifest/sst/log，再递归
// 删除 wal_dir/zfwal 子目录（含 ZFPROPS 与全部代文件）。即使原生 DB
// 已不存在，zfwal 也保证被清理。原 DestroyDB 状态作为返回值传递，
// zfwal 清理异常用 PermitUncheckedError 吞掉（best-effort）。
ROCKSDB_NAMESPACE::Status DestroyDB(const std::string& dbname,
                                    const ROCKSDB_NAMESPACE::Options& options,
                                    const ZeroFlushOptions& zfo);

}  // namespace zeroflush
