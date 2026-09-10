//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlushV0 M4：动态治理 DG——每分区统计（@封存边界）+ 治理决策策略。
//
//  对应 ZEROFLUSHV0_ROADMAP.md §2 M4：
//   - PartitionRateTracker：每分区写入速率 EWMA（快窗 1min / 慢窗 5min），
//     采样点 = 封存边界（分区 WAL 换代时的封存字节 + 时刻），不进写路径；
//   - GovernancePolicy：低频 tick（默认 1s）的纯决策状态机——连续窗确认
//     （θ_split=3×均值 & ≥S_min & 3 窗 / θ_merge=0.3×均值 & ≤S_min 合并 & 3
//     窗）、冷却 + 同 range 退避倍增、P 界 [P_min,P_max]、Busy 重试 aging
//     （urgency = 热度 × 等待）。本文件不含执行（执行在 DBImpl，见
//     DBImpl::ZfGovernanceTick / db_impl_write.cc）。

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace zeroflush {

// ---------------------------------------------------------------------------
// PartitionRateTracker：每分区写入速率 EWMA（双窗），@封存边界采样。
// ---------------------------------------------------------------------------
// 采样语义（不触碰写路径）：每次分区封存（epoch 登记）携带该分区本代封存
// 字节 bytes 与当前时刻 now_us。瞬时速率 = bytes / (now − 上次封存时刻)
// （近似该分区本代写入的平均速率）；按间隔 dt 计算指数衰减系数
// α = 1 − exp(−dt/τ) 滚入快/慢两个 EWMA（τ_fast=1min，τ_slow=5min）。
// 读取时对未采样间隔做时间衰减（exp(−Δ/τ)）——分区停止写入后速率自然趋零，
// 供"变冷 → 合并"判定。线程安全：全部公开方法持内部锁（封存低频，无热路径
// 开销）。
class PartitionRateTracker {
 public:
  PartitionRateTracker(uint64_t fast_window_secs, uint64_t slow_window_secs);
  ~PartitionRateTracker() = default;
  PartitionRateTracker(const PartitionRateTracker&) = delete;
  PartitionRateTracker& operator=(const PartitionRateTracker&) = delete;

  // 封存事件：分区 part_id 本代封存 bytes 字节（bytes > 0）。
  void RecordSeal(uint32_t part_id, uint64_t bytes, uint64_t now_us);

  // 读（含衰减到 now_us）。无样本返回 0。
  double RateFast(uint32_t part_id, uint64_t now_us) const;
  double RateSlow(uint32_t part_id, uint64_t now_us) const;
  // 是否已有可用的速率样本（≥1 次封存，瞬时速率可算）。
  bool HasSample(uint32_t part_id, uint64_t now_us) const;
  uint64_t LastSealMicros(uint32_t part_id) const;

  // 分区 id 从路由表消失后清理（merge 移除/split 退役父 id）。
  void Forget(uint32_t part_id);
  // split 后子分区继承父分区速率（快/慢窗 + 末次采样时刻置为 now——
  // 子分区下一封存样本的间隔从切分时刻起算）。父条目由调用方 Forget。
  void InheritRate(uint32_t child_part, uint32_t parent_part,
                   uint64_t now_us);
  size_t Size() const;

 private:
  struct Entry {
    double fast = 0.0;
    double slow = 0.0;
    uint64_t last_seal_us = 0;   // 上次封存时刻（速率间隔基准）
    uint64_t last_decay_us = 0;  // 上次衰减/更新时刻
  };
  void DecayLocked(Entry& e, uint64_t now_us) const;

  mutable std::mutex mu_;
  uint64_t fast_tau_us_;
  uint64_t slow_tau_us_;
  std::unordered_map<uint32_t, Entry> entries_;
};

// ---------------------------------------------------------------------------
// 治理观察与决策（纯逻辑，可单测）
// ---------------------------------------------------------------------------

// 一次 tick 对一个分区的观察快照。rate_* 由 PartitionRateTracker 提供
// （已含衰减）；base_bytes 为 base 层落在该分区路由区间的文件字节合计
// （执行器在持 DB mutex 时计算；观察阶段可为 0——大小门槛在动作执行前
// 复核，不入窗口计数）。
struct GovernanceObs {
  uint32_t part_id = 0;
  double rate_fast = 0.0;
  double rate_slow = 0.0;
  uint64_t base_bytes = 0;    // base 层字节（决策大小门槛；可缺省，见 base_known）
  uint64_t active_bytes = 0;  // 未封存活跃字节
  bool base_known = false;    // base_bytes 是否已由执行器实测（否则门槛不阻断
                              // 窗口累积，动作执行前复核）
  bool busy = false;          // 有 pending sealed/frozen/recovery（动作会 Busy）
  bool draining = false;      // DRAINING 方或别名吸收方（禁止治理）
  std::string lo;             // 路由区间 [lo,hi)（冷却"同 range"判定用）
  std::string hi;
  // 距该分区最近一次封存（速率样本）的时长（µs）。> merge_idle_after 且
  // 速率仍为正 = "曾热现已空闲"（合并回收候选）；新近有封存 = 有活跃写。
  uint64_t last_seal_age_us = 0;
};

enum class GovernanceKind : uint8_t { kNone = 0, kSplit = 1, kMerge = 2 };

struct GovernanceAction {
  GovernanceKind kind = GovernanceKind::kNone;
  uint32_t part_id = 0;
  double urgency = 0.0;  // 热度倍数 × 等待倍率（决策队列排序键）
};

// 治理决策策略（无状态子，除下述窗口/冷却状态）。一个 tick 调一次
// Decide(obs, now)：更新连续窗计数（rate 规则）、冷却过期、pending aging，
// 输出本轮建议尝试的动作（≤ max_actions_per_tick，v1 固定 ≤1 的调用方
// 语义由执行器保证——每个动作可能持锁执行 + 排空等待）。
class GovernancePolicy {
 public:
  using RangeKey = std::pair<std::string, std::string>;  // (lo, hi)

  struct Config {
    uint32_t p_min = 4;             // P_min：低于该值不做冷合并
    uint32_t p_max = 256;           // P_max：达到该值不再热拆分
    double theta_split = 3.0;       // θ_split：rate ≥ θ_split × mean
    double theta_merge = 0.3;       // θ_merge：rate ≤ θ_merge × mean
    uint64_t split_min_bytes = 32ull << 20;  // 热拆分大小门槛（S_min 口径）
    uint64_t merge_max_bytes = 16ull << 20;  // 冷合并大小门槛
    uint32_t confirm_windows = 3;   // 连续窗数
    uint64_t cooldown_base_us = 300ull * 1000 * 1000;  // 冷却基础（300s）
    uint64_t cooldown_max_us = 600ull * 1000 * 1000;   // 退避倍增上限（600s）
    uint32_t max_actions_per_tick = 1;  // C：每 tick 至多建议动作数
    double aging_per_tick = 0.1;    // Busy 重试每 tick 的 urgency 加成
    // Busy/TimedOut 重试退避（避免对持续未排空分区每 tick 空转/冻结风暴）：
    // 重试间隔 = retry_backoff_base << tries，封顶 retry_backoff_max。
    uint64_t retry_backoff_base_us = 1000ull * 1000;
    uint64_t retry_backoff_max_us = 30ull * 1000 * 1000;
    // 合并仅在"空闲期"放行：存在任一速率>0 且封存样本新近
    // （age ≤ merge_idle_after_us）的分区时抑制合并（活跃期 P 预算让给
    // 热拆分；空闲回收符合设计"设备空闲后由再锚定合并回收"）。0 =
    // 任何正速率都算活跃（严格禁止 active 期合并）。
    uint64_t merge_idle_after_us = 120ull * 1000 * 1000;
  };

  explicit GovernancePolicy(const Config& cfg);
  GovernancePolicy(const GovernancePolicy&) = delete;
  GovernancePolicy& operator=(const GovernancePolicy&) = delete;

  // 一轮观察。P = obs.size()（调用方保证 = 当前表分区数）。
  // 输出本轮建议动作（urgency 降序，≤ max_actions_per_tick）。
  std::vector<GovernanceAction> Decide(const std::vector<GovernanceObs>& obs,
                                       uint64_t now_us);

  // 执行反馈：
  //  - NoteSuccess：动作成功（merge 的 affected = 吸收方新区间；split 的
  //    affected = 两子分区区间，均需执行器实测传入）。冷却登记到每个
  //    affected range：既有未过期冷却 level+1（同 range 退避倍增），否则用
  //    seed_level（split 时执行器传父区间旧 level+1 使子区间继承退避）。
  //  - NoteRetry：动作返回 Busy/TimedOut——保留为 pending（aging：urgency
  //    随等待 tick 增长），下轮 Decide 优先重试。
  void NoteSuccess(const GovernanceAction& a, uint64_t now_us,
                   const std::vector<RangeKey>& affected_ranges,
                   uint32_t seed_level);
  void NoteRetry(const GovernanceAction& a, uint64_t now_us);

  const Config& config() const { return cfg_; }
  // 诊断。
  uint64_t LastActionMicros() const { return last_action_us_; }
  // (lo,hi) 当前冷却退避级（未冷却/无记录返回 0）。
  uint32_t CooldownLevelOf(const std::string& lo, const std::string& hi,
                           uint64_t now_us) const;
  uint32_t NextBackoffLevel(const std::string& lo, const std::string& hi,
                            uint64_t now_us) const;

 private:
  struct Cool {
    uint64_t until_us = 0;
    uint32_t level = 0;  // 退避级（冷却 = base << level，cap = max）
  };
  struct Pending {
    GovernanceAction action;
    uint64_t first_us = 0;
    uint32_t tries = 0;
    uint64_t retry_at_us = 0;  // 下次重试时刻（退避调度）
  };

  bool RangeCooling(const RangeKey& k, uint64_t now_us, uint32_t* level);
  void SetCooldown(const RangeKey& k, uint32_t level, uint64_t now_us);
  double MeanRateFast(const std::vector<GovernanceObs>& obs) const;
  double MeanRateSlow(const std::vector<GovernanceObs>& obs) const;
  // 返回 rate 是否满足热（split）/冷（merge）规则（未含 streak/冷却/P 界）。
  // 双窗口径：拆分看快窗（近期负载增长——慢窗在短时高偏态/不等封存节奏
  // 下不可靠，实测会饿死热拆分）；合并看快窗 θ_merge + 慢窗不低于均值
  // （更长确认，防瞬时波动误回收）。
  bool HotByRate(const GovernanceObs& o, double mean_fast) const;
  bool ColdByRate(const GovernanceObs& o, double mean_fast,
                  double mean_slow) const;

  Config cfg_;
  // 连续窗计数（rate 规则命中/未命中），key = part_id。
  std::unordered_map<uint32_t, uint32_t> split_streak_;
  std::unordered_map<uint32_t, uint32_t> merge_streak_;
  std::map<RangeKey, Cool> cooldown_;
  std::vector<Pending> pending_;
  uint64_t last_action_us_ = 0;
};

// ZeroFlushV0 M5：rewrite 带宽预算判定（严格 ≤ share）。
// rewrite_rate/output_rate 为滑窗速率（bytes/s）。share<=0 视为不限；
// output_rate==0 时仅在 rewrite_rate==0 时放行（无产出即无 rewrite 预算）。
// 语义：rewrite ≤ share/(1-share) × 常规物化输出（即 rewrite 占总后台写 IO
// 份额 ≤ share）。
bool RewriteBudgetOk(double share, double rewrite_rate, double output_rate);

}  // namespace zeroflush
