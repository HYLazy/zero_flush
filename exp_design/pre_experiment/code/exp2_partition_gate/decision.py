#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""decision.py - 实验 2 分区开销门机械判定。

读取 aggregate_exp2.py 产出的 aggregated.json，按预设规则机械推出
PASS / FAIL / REDESIGN，输出 decision.json。

判定规则（sync_wal=true 档位）：
  baseline = 单 WAL 基线（P=1, independent_fsync）
  候选 B   = P=64, batched_fsync
  regression_pct = (baseline_tput - B_tput) / baseline_tput * 100

  PASS     : regression_pct <= 15 且 B.fsync_count_per_op <= 2*baseline，
             且 B.durability_window_ms <= 5
  REDESIGN : B 满足吞吐与 fsync 条件但 durability_window_ms > 5；
             或边界情形（B 不满足但 A(independent_fsync, P=64) 满足）
  FAIL     : A、B 均不满足 PASS 条件；或数据不完整（禁止给 PASS）

FAIL / REDESIGN 的 decision_rationale 必须列出 >=1 个替代设计。
"""

import argparse
import json
import sys

REGRESSION_LIMIT_PCT = 15.0
FSYNC_RATIO_LIMIT = 2.0
DURABILITY_LIMIT_MS = 5.0

ALTERNATIVES = [
    "单 WAL 写入 + 轻量后台分区线程（写入路径保持单文件组提交，"
    "分区路由推迟到 flush/L0 构建时后台完成）",
    "放弃 WAL=L0 映射，退化为分区 MemTable 方案"
    "（WAL 仅作崩溃恢复日志，分区结构只在内存 MemTable 与 L0 维持）",
    "模式 B 改良：批量 fsync 但改用时间上限封顶的 commit 窗口"
    "（如 <=2ms），以牺牲少量 fsync 摊销换取 durability window 达标",
]


def find_combo(results, p, mode, sync):
    for r in results:
        if r["p"] == p and r["mode"] == mode and r["sync_wal"] == sync:
            return r
    return None


def fmt(v, nd=4):
    return f"{v:.{nd}f}" if isinstance(v, (int, float)) else str(v)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--agg", required=True, help="aggregated.json 路径")
    ap.add_argument("--out", required=True, help="decision.json 输出路径")
    args = ap.parse_args()

    with open(args.agg, "r", encoding="utf-8") as f:
        agg = json.load(f)

    results = agg.get("multi_wal_fsync", {}).get("results", [])
    failures = agg.get("failures_or_na", [])

    base = find_combo(results, 1, "independent_fsync", True)
    cand_b = find_combo(results, 64, "batched_fsync", True)
    cand_a = find_combo(results, 64, "independent_fsync", True)

    referenced = {}
    decision = "FAIL"
    rationale = ""
    alternatives = []

    missing = []
    if base is None:
        missing.append("baseline(P=1 independent_fsync sync=true)")
    if cand_b is None:
        missing.append("candidate B(P=64 batched_fsync sync=true)")
    if cand_a is None:
        missing.append("candidate A(P=64 independent_fsync sync=true)")
    if failures:
        missing.append(f"存在 {len(failures)} 条运行/解析失败记录")

    if missing:
        decision = "FAIL"
        rationale = (
            "数据不完整，禁止给 PASS。缺失/异常: " + "; ".join(missing) +
            "。替代设计: " + "；".join(ALTERNATIVES[:2]))
        alternatives = ALTERNATIVES[:2]
    else:
        base_t = base["throughput_ops_per_s"]
        base_f = base["fsync_count_per_op"]
        b_t = cand_b["throughput_ops_per_s"]
        b_f = cand_b["fsync_count_per_op"]
        b_w = cand_b["durability_window_ms"]
        a_t = cand_a["throughput_ops_per_s"]
        a_f = cand_a["fsync_count_per_op"]

        reg_b = (base_t - b_t) / base_t * 100.0 if base_t > 0 else 1e9
        reg_a = (base_t - a_t) / base_t * 100.0 if base_t > 0 else 1e9
        b_thru_ok = reg_b <= REGRESSION_LIMIT_PCT
        b_fsync_ok = b_f <= FSYNC_RATIO_LIMIT * base_f
        a_thru_ok = reg_a <= REGRESSION_LIMIT_PCT
        a_fsync_ok = a_f <= FSYNC_RATIO_LIMIT * base_f

        referenced = {
            "baseline_p1_throughput_ops_per_s": base_t,
            "baseline_p1_fsync_count_per_op": base_f,
            "mode_b_p64_throughput_ops_per_s": b_t,
            "mode_b_p64_fsync_count_per_op": b_f,
            "mode_b_p64_durability_window_ms": b_w,
            "mode_b_regression_pct": round(reg_b, 3),
            "mode_b_fsync_ratio_vs_p1": round(b_f / base_f, 4)
            if base_f > 0 else None,
            "mode_a_p64_throughput_ops_per_s": a_t,
            "mode_a_p64_fsync_count_per_op": a_f,
            "mode_a_regression_pct": round(reg_a, 3),
            "limits": {
                "regression_pct_max": REGRESSION_LIMIT_PCT,
                "fsync_ratio_max": FSYNC_RATIO_LIMIT,
                "durability_window_ms_max": DURABILITY_LIMIT_MS,
            },
        }

        if b_thru_ok and b_fsync_ok:
            if b_w <= DURABILITY_LIMIT_MS:
                decision = "PASS"
                rationale = (
                    f"模式B(P=64) 相对单WAL基线吞吐回退 {fmt(reg_b,2)}% "
                    f"(<= {REGRESSION_LIMIT_PCT}%)："
                    f"baseline={fmt(base_t,0)} ops/s, B={fmt(b_t,0)} ops/s；"
                    f"fsync_count_per_op B={fmt(b_f,6)} <= 2x baseline "
                    f"{fmt(base_f,6)}（比值 {fmt(b_f/base_f,3) if base_f>0 else 'NA'}）；"
                    f"durability_window p99={fmt(b_w,3)}ms "
                    f"(<= {DURABILITY_LIMIT_MS}ms)。")
            else:
                decision = "REDESIGN"
                rationale = (
                    f"模式B(P=64) 吞吐与 fsync 条件达标（回退 {fmt(reg_b,2)}%，"
                    f"fsync/op {fmt(b_f,6)} vs 基线 {fmt(base_f,6)}），"
                    f"但 durability_window p99={fmt(b_w,3)}ms > "
                    f"{DURABILITY_LIMIT_MS}ms，持久化窗口不可接受。"
                    f"替代设计: " + "；".join(ALTERNATIVES))
                alternatives = ALTERNATIVES
        elif a_thru_ok and a_fsync_ok:
            decision = "REDESIGN"
            rationale = (
                f"边界情形：模式B(P=64) 不达标（回退 {fmt(reg_b,2)}%，"
                f"fsync/op {fmt(b_f,6)}，窗口 {fmt(b_w,3)}ms），"
                f"但模式A(P=64) 达标（回退 {fmt(reg_a,2)}%，"
                f"fsync/op {fmt(a_f,6)}）。需重新设计批量提交策略而非"
                f"直接采用模式B。替代设计: " + "；".join(ALTERNATIVES))
            alternatives = ALTERNATIVES
        else:
            decision = "FAIL"
            rationale = (
                f"模式A、B 均不满足 PASS 条件："
                f"A(P=64) 回退 {fmt(reg_a,2)}%、fsync/op {fmt(a_f,6)}；"
                f"B(P=64) 回退 {fmt(reg_b,2)}%、fsync/op {fmt(b_f,6)}、"
                f"窗口 {fmt(b_w,3)}ms；基线 P=1 吞吐 {fmt(base_t,0)} ops/s、"
                f"fsync/op {fmt(base_f,6)}。"
                f"替代设计: " + "；".join(ALTERNATIVES[:2]))
            alternatives = ALTERNATIVES[:2]

    out = {
        "decision": decision,
        "decision_rationale": rationale,
        "alternatives": alternatives,
        "referenced_values": referenced,
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"[decision] {decision}: {rationale[:160]}...")
    return 0


if __name__ == "__main__":
    sys.exit(main())
