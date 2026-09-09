//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlushV0 M4：动态治理 DG——统计与决策策略实现（见 repartition_controller.h）。

#include "zeroflush/repartition_controller.h"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iterator>
#include <unordered_set>

namespace zeroflush {

// ---------------------------------------------------------------------------
// PartitionRateTracker
// ---------------------------------------------------------------------------

PartitionRateTracker::PartitionRateTracker(uint64_t fast_window_secs,
                                           uint64_t slow_window_secs)
    : fast_tau_us_(std::max<uint64_t>(1, fast_window_secs) * 1000 * 1000),
      slow_tau_us_(std::max<uint64_t>(1, slow_window_secs) * 1000 * 1000) {}

void PartitionRateTracker::RecordSeal(uint32_t part_id, uint64_t bytes,
                                      uint64_t now_us) {
  std::lock_guard<std::mutex> l(mu_);
  Entry& e = entries_[part_id];
  DecayLocked(e, now_us);
  if (e.last_seal_us != 0 && now_us > e.last_seal_us) {
    // 瞬时速率 = 本代封存字节 / 距上次封存间隔（该分区本代的平均写入速率）。
    const double dt = static_cast<double>(now_us - e.last_seal_us);
    const double inst = static_cast<double>(bytes) * 1e6 / dt;  // bytes/sec
    const double af = 1.0 - std::exp(-dt / static_cast<double>(fast_tau_us_));
    const double as = 1.0 - std::exp(-dt / static_cast<double>(slow_tau_us_));
    e.fast = af * inst + (1.0 - af) * e.fast;
    e.slow = as * inst + (1.0 - as) * e.slow;
  }
  // 首个样本无前序间隔：速率保持 0，下一封存才有可算间隔（保守——新分区/
  // 新 id 前两代不参与"热拆分"，由 streak 窗数自然延后）。
  e.last_seal_us = now_us;
}

void PartitionRateTracker::DecayLocked(Entry& e, uint64_t now_us) const {
  if (e.last_decay_us == 0) {
    e.last_decay_us = now_us;
    return;
  }
  if (now_us <= e.last_decay_us) {
    return;
  }
  const double dt = static_cast<double>(now_us - e.last_decay_us);
  e.fast *= std::exp(-dt / static_cast<double>(fast_tau_us_));
  e.slow *= std::exp(-dt / static_cast<double>(slow_tau_us_));
  e.last_decay_us = now_us;
}

double PartitionRateTracker::RateFast(uint32_t part_id, uint64_t now_us) const {
  std::lock_guard<std::mutex> l(mu_);
  auto it = entries_.find(part_id);
  if (it == entries_.end()) {
    return 0.0;
  }
  Entry e = it->second;  // 拷贝后衰减（不污染 last_seal_us）
  DecayLocked(e, now_us);
  return e.fast;
}

double PartitionRateTracker::RateSlow(uint32_t part_id, uint64_t now_us) const {
  std::lock_guard<std::mutex> l(mu_);
  auto it = entries_.find(part_id);
  if (it == entries_.end()) {
    return 0.0;
  }
  Entry e = it->second;
  DecayLocked(e, now_us);
  return e.slow;
}

bool PartitionRateTracker::HasSample(uint32_t part_id, uint64_t now_us) const {
  std::lock_guard<std::mutex> l(mu_);
  auto it = entries_.find(part_id);
  if (it == entries_.end()) {
    return false;
  }
  return it->second.last_seal_us != 0;
}

uint64_t PartitionRateTracker::LastSealMicros(uint32_t part_id) const {
  std::lock_guard<std::mutex> l(mu_);
  auto it = entries_.find(part_id);
  return it == entries_.end() ? 0 : it->second.last_seal_us;
}

void PartitionRateTracker::Forget(uint32_t part_id) {
  std::lock_guard<std::mutex> l(mu_);
  entries_.erase(part_id);
}

void PartitionRateTracker::InheritRate(uint32_t child_part,
                                       uint32_t parent_part,
                                       uint64_t now_us) {
  std::lock_guard<std::mutex> l(mu_);
  auto it = entries_.find(parent_part);
  if (it == entries_.end()) {
    return;  // 父无速率样本（理论不达——能 split 必曾热）：不播种
  }
  Entry& ce = entries_[child_part];
  Entry e = it->second;
  DecayLocked(e, now_us);
  ce.fast = e.fast;
  ce.slow = e.slow;
  ce.last_seal_us = now_us;  // 子分区下个样本的间隔自切分时刻起算
  ce.last_decay_us = now_us;
}

size_t PartitionRateTracker::Size() const {
  std::lock_guard<std::mutex> l(mu_);
  return entries_.size();
}

// ---------------------------------------------------------------------------
// GovernancePolicy
// ---------------------------------------------------------------------------

GovernancePolicy::GovernancePolicy(const Config& cfg) : cfg_(cfg) {}

double GovernancePolicy::MeanRate(const std::vector<GovernanceObs>& obs) const {
  double sum = 0.0;
  for (const auto& o : obs) {
    sum += o.rate_fast;
  }
  return obs.empty() ? 0.0 : sum / static_cast<double>(obs.size());
}

bool GovernancePolicy::HotByRate(const GovernanceObs& o, double mean) const {
  if (mean <= 0.0) {
    return false;  // 无正均值（全空闲/冷启动）→ 无热分区
  }
  const double th = cfg_.theta_split * mean;
  return o.rate_fast >= th && o.rate_slow >= th;
}

bool GovernancePolicy::ColdByRate(const GovernanceObs& o, double mean) const {
  if (mean <= 0.0) {
    // 全空闲：速率≈0 的分区视为冷（回收小分区直到 P_min）。
    return o.rate_fast <= 0.0 && o.rate_slow <= 0.0;
  }
  const double th = cfg_.theta_merge * mean;
  return o.rate_fast <= th && o.rate_slow <= th;
}

bool GovernancePolicy::RangeCooling(const RangeKey& k, uint64_t now_us,
                                    uint32_t* level) {
  auto it = cooldown_.find(k);
  if (it == cooldown_.end()) {
    if (level != nullptr) {
      *level = 0;
    }
    return false;
  }
  if (now_us < it->second.until_us) {
    if (level != nullptr) {
      *level = it->second.level;
    }
    return true;
  }
  cooldown_.erase(it);  // 过期清理
  if (level != nullptr) {
    *level = 0;
  }
  return false;
}

void GovernancePolicy::SetCooldown(const RangeKey& k, uint32_t level,
                                   uint64_t now_us) {
  uint64_t dur = cfg_.cooldown_base_us;
  // 退避倍增：base << level，封顶 cooldown_max_us（防溢出用逐次×2 而非移位）。
  for (uint32_t i = 0; i < level; ++i) {
    dur *= 2;
    if (dur >= cfg_.cooldown_max_us) {
      dur = cfg_.cooldown_max_us;
      break;
    }
  }
  if (dur > cfg_.cooldown_max_us) {
    dur = cfg_.cooldown_max_us;
  }
  Cool c;
  c.until_us = now_us + dur;
  c.level = level;
  cooldown_[k] = c;
}

uint32_t GovernancePolicy::NextBackoffLevel(const std::string& lo,
                                            const std::string& hi,
                                            uint64_t now_us) const {
  uint32_t level = 0;
  // const 访问：RangeCooling 会 erase 过期项——此处只读，复制判定。
  auto it = cooldown_.find(RangeKey(lo, hi));
  if (it != cooldown_.end() && now_us < it->second.until_us) {
    level = it->second.level;
  }
  return level;
}

uint32_t GovernancePolicy::CooldownLevelOf(const std::string& lo,
                                           const std::string& hi,
                                           uint64_t now_us) const {
  return NextBackoffLevel(lo, hi, now_us);
}

std::vector<GovernanceAction> GovernancePolicy::Decide(
    const std::vector<GovernanceObs>& obs, uint64_t now_us) {
  std::vector<GovernanceAction> out;
  if (obs.size() < 2) {
    // P=1（或空）：无可合并邻居；单分区均值 = 自身速率，热拆分规则恒不满足
    //（rate ≥ 3×rate 假）。P=1 的生长拆分走 M3 GrowPartitions（排空检查点
    // 等容拆分），不属本策略。仍做 streak 清理。
    std::unordered_set<uint32_t> keep;
    return out;
  }
  const uint32_t P = static_cast<uint32_t>(obs.size());
  std::unordered_set<uint32_t> present;
  present.reserve(P);
  for (const auto& o : obs) {
    present.insert(o.part_id);
  }
  // 清理已不在表中的分区状态（merge 移除 / split 退役父 id）。
  for (auto it = split_streak_.begin(); it != split_streak_.end();) {
    it = present.count(it->first) ? std::next(it) : split_streak_.erase(it);
  }
  for (auto it = merge_streak_.begin(); it != merge_streak_.end();) {
    it = present.count(it->first) ? std::next(it) : merge_streak_.erase(it);
  }
  for (auto it = pending_.begin(); it != pending_.end();) {
    if (!present.count(it->action.part_id)) {
      it = pending_.erase(it);
    } else {
      ++it;
    }
  }

  const double mean = MeanRate(obs);

  // ---- 1) 更新连续窗计数（rate 规则） ----
  std::unordered_set<uint32_t> hot_ids;
  std::unordered_set<uint32_t> cold_ids;
  for (const auto& o : obs) {
    const bool hot = HotByRate(o, mean);
    const bool cold = !hot && ColdByRate(o, mean);
    uint32_t& hs = split_streak_[o.part_id];
    uint32_t& ms = merge_streak_[o.part_id];
    if (hot) {
      ++hs;
      ms = 0;
      hot_ids.insert(o.part_id);
    } else if (cold) {
      ++ms;
      hs = 0;
      cold_ids.insert(o.part_id);
    } else {
      hs = 0;
      ms = 0;
    }
  }

  // ---- 2) 收集候选 ----
  struct Cand {
    GovernanceAction action;
    bool is_pending = false;
  };
  std::vector<Cand> cands;
  auto has_pending_for = [&](uint32_t pid, GovernanceKind k) {
    for (const auto& p : pending_) {
      if (p.action.part_id == pid && p.action.kind == k) {
        return true;
      }
    }
    return false;
  };
  auto add_fresh = [&](const GovernanceObs& o, GovernanceKind kind,
                       double urgency) {
    GovernanceAction a;
    a.kind = kind;
    a.part_id = o.part_id;
    a.urgency = urgency;
    Cand c;
    c.action = a;
    c.is_pending = false;
    cands.push_back(c);
  };

  for (const auto& o : obs) {
    if (o.draining) {
      continue;  // DRAINING 方/别名吸收方不治理
    }
    const bool cooling =
        RangeCooling(RangeKey(o.lo, o.hi), now_us, nullptr);
    if (hot_ids.count(o.part_id) &&
        split_streak_[o.part_id] >= cfg_.confirm_windows && P < cfg_.p_max &&
        !cooling && !has_pending_for(o.part_id, GovernanceKind::kSplit)) {
      const bool base_ok =
          !o.base_known || o.base_bytes >= cfg_.split_min_bytes;
      if (base_ok) {
        double urgency = 1.0;
        if (mean > 0.0) {
          urgency = o.rate_fast / std::max(cfg_.theta_split * mean, 1e-9);
        }
        add_fresh(o, GovernanceKind::kSplit, urgency);
      }
    } else if (cold_ids.count(o.part_id) &&
               merge_streak_[o.part_id] >= cfg_.confirm_windows &&
               P > cfg_.p_min && !cooling &&
               !has_pending_for(o.part_id, GovernanceKind::kMerge)) {
      const bool base_ok =
          !o.base_known || o.base_bytes <= cfg_.merge_max_bytes;
      if (base_ok) {
        double urgency = 1.0;
        if (mean > 0.0 && o.rate_fast > 0.0) {
          urgency = (cfg_.theta_merge * mean) / o.rate_fast;
        }
        // 全空闲（rate≈0）：优先回收更小的分区（base 越小越优先）。
        if (o.base_known && o.base_bytes <= cfg_.merge_max_bytes) {
          urgency += 1.0 -
                     static_cast<double>(o.base_bytes) /
                         static_cast<double>(cfg_.merge_max_bytes);
        }
        add_fresh(o, GovernanceKind::kMerge, urgency);
      }
    }
  }

  // ---- 3) pending（Busy 重试）并入：未到期（退避调度）不参与 ----
  for (auto it = pending_.begin(); it != pending_.end();) {
    const Pending& p = *it;
    if (now_us < p.retry_at_us) {
      ++it;  // 退避中，本 tick 不动作
      continue;
    }
    // 分区仍在表中且其分类未反转 → 保留；否则丢弃。
    const bool still_split =
        p.action.kind == GovernanceKind::kSplit && hot_ids.count(p.action.part_id);
    const bool still_merge =
        p.action.kind == GovernanceKind::kMerge && cold_ids.count(p.action.part_id);
    if (!still_split && !still_merge) {
      it = pending_.erase(it);
      continue;
    }
    GovernanceAction a = p.action;
    a.urgency *= (1.0 + cfg_.aging_per_tick * static_cast<double>(p.tries));
    Cand c;
    c.action = a;
    c.is_pending = true;
    cands.push_back(c);
    ++it;
  }

  // ---- 4) 去重（fresh 与 pending 同 part+kind 取 pending——保留 aging） ----
  std::vector<Cand> merged;
  for (const auto& c : cands) {
    bool dup = false;
    for (auto& m : merged) {
      if (m.action.part_id == c.action.part_id &&
          m.action.kind == c.action.kind) {
        if (c.is_pending) {
          m = c;
        }
        dup = true;
        break;
      }
    }
    if (!dup) {
      merged.push_back(c);
    }
  }

  // ---- 5) 合并保护：活跃期不做冷合并 ----
  // 原因有二：
  //  (a) 冷合并会把 P 降到"热分区的 rate 相对均值不再 ≥ θ_split×mean"
  //      的程度（1 热 + 1 冷：rate=2×mean < 3×mean）——直接杀死后续拆分；
  //  (b) 每 tick 单动作（C=1）下，merge 会抢占热拆分确认窗——拆分饿死。
  // 判定"活跃"= 存在任一速率>0 且封存样本新近（age ≤ merge_idle_after_us）
  // 的分区；活跃期抑制全部 merge（冷分区回收延后到负载回落/空闲期——
  // 对应设计"设备空闲后由再锚定合并回收"）。拆分候选存在同样抑制合并。
  bool recent_activity = false;
  for (const auto& o : obs) {
    if ((o.rate_fast > 0.0 || o.rate_slow > 0.0) &&
        o.last_seal_age_us <= cfg_.merge_idle_after_us) {
      recent_activity = true;
      break;
    }
  }
  const bool any_split =
      std::any_of(merged.begin(), merged.end(), [](const Cand& c) {
        return c.action.kind == GovernanceKind::kSplit;
      });
  const bool suppress_merges = recent_activity || any_split;
  if (getenv("ZF_DBG_GOV") != nullptr) {
    fprintf(stderr,
            "ZFDBG-gov policy recent=%d any_split=%d suppress=%d "
            "cands=%zu idle=%llu\n",
            recent_activity ? 1 : 0, any_split ? 1 : 0,
            suppress_merges ? 1 : 0, merged.size(),
            (unsigned long long)cfg_.merge_idle_after_us);
  }
  if (suppress_merges) {
    merged.erase(std::remove_if(merged.begin(), merged.end(),
                                [](const Cand& c) {
                                  return c.action.kind == GovernanceKind::kMerge;
                                }),
                 merged.end());
  }

  // ---- 6) urgency 降序（tie：part_id 升序，保持确定性） ----
  std::sort(merged.begin(), merged.end(),
            [](const Cand& a, const Cand& b) {
              if (a.action.urgency != b.action.urgency) {
                return a.action.urgency > b.action.urgency;
              }
              if (a.action.part_id != b.action.part_id) {
                return a.action.part_id < b.action.part_id;
              }
              return a.action.kind < b.action.kind;
            });
  const size_t n = std::min<size_t>(cfg_.max_actions_per_tick, merged.size());
  for (size_t i = 0; i < n; ++i) {
    out.push_back(merged[i].action);
  }
  return out;
}

void GovernancePolicy::NoteSuccess(
    const GovernanceAction& a, uint64_t now_us,
    const std::vector<std::pair<std::string, std::string>>& affected_ranges,
    uint32_t seed_level) {
  // 清理 streak 与 pending（动作目标已变化）。
  split_streak_.erase(a.part_id);
  merge_streak_.erase(a.part_id);
  for (auto it = pending_.begin(); it != pending_.end();) {
    if (it->action.part_id == a.part_id &&
        it->action.kind == a.kind) {
      it = pending_.erase(it);
    } else {
      ++it;
    }
  }
  last_action_us_ = now_us;
  for (const auto& r : affected_ranges) {
    // 同 range 退避倍增：新登记在既有 level（seed 之上）再加一档。
    uint32_t level = seed_level;
    auto it = cooldown_.find(r);
    if (it != cooldown_.end() && now_us < it->second.until_us) {
      level = std::max(level, it->second.level + 1);
    }
    SetCooldown(r, level, now_us);
  }
}

void GovernancePolicy::NoteRetry(const GovernanceAction& a, uint64_t now_us) {
  for (auto& p : pending_) {
    if (p.action.part_id == a.part_id && p.action.kind == a.kind) {
      ++p.tries;
      uint64_t backoff = cfg_.retry_backoff_base_us;
      for (uint32_t i = 1; i < p.tries; ++i) {
        backoff *= 2;
        if (backoff >= cfg_.retry_backoff_max_us) {
          backoff = cfg_.retry_backoff_max_us;
          break;
        }
      }
      backoff = std::min(backoff, cfg_.retry_backoff_max_us);
      p.retry_at_us = now_us + backoff;
      return;
    }
  }
  Pending p;
  p.action = a;
  p.first_us = now_us;
  p.tries = 1;
  p.retry_at_us = now_us + cfg_.retry_backoff_base_us;
  pending_.push_back(std::move(p));
}

}  // namespace zeroflush
