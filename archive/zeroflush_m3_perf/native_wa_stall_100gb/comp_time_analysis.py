#!/usr/bin/env python3
# 从 LOG 提取每次 compaction 的耗时（compaction_finished 事件），按输出层分组：
#   L0→L1（output_level=1）、L1 以下（output_level>=2，另拆 L2/L3/L4）、Intra-L0（=0）。
# 注意 subcompactions=16：同一逻辑 compaction 的多个子 job 共享 job id（并行执行），
# 逻辑 wall = max(子 job micros)，线程时间 = sum(子 job micros)。
# 用法: python3 comp_time_analysis.py <log_file> <out_json> <label>
import json, os, re, sys

def percentile(sorted_v, p):
    if not sorted_v:
        return 0
    i = min(len(sorted_v) - 1, int(len(sorted_v) * p / 100.0))
    return sorted_v[i]

def parse(logf, label):
    logical = {}   # job_id -> {level, wall_max, thread_sum, cpu_sum, sub, sizes}
    events = 0
    started = 0
    for line in open(logf, errors="replace"):
        if '"event": "compaction_finished"' not in line:
            if '"event": "compaction_started"' in line:
                started += 1
            continue
        i = line.find("{")
        j = line.rfind("}")
        if i < 0 or j < 0:
            continue
        try:
            e = json.loads(line[i:j+1])
        except json.JSONDecodeError:
            continue
        events += 1
        jid = str(e.get("job"))
        lvl = e.get("output_level")
        micros = e.get("compaction_time_micros", 0)
        cpu = e.get("compaction_time_cpu_micros", 0)
        d = logical.setdefault(jid, {"level": lvl, "wall": 0, "thread": 0, "cpu": 0,
                                     "sub": e.get("num_subcompactions", 1),
                                     "out_bytes": 0})
        d["wall"] = max(d["wall"], micros)
        d["thread"] += micros
        d["cpu"] += cpu
        d["out_bytes"] += e.get("total_output_size", 0)

    groups = {}
    for jid, d in logical.items():
        key = "intra_L0" if d["level"] == 0 else ("L0_L1" if d["level"] == 1 else f"L{d['level']-1}_L{d['level']}")
        g = groups.setdefault(key, {"jobs": 0, "subjobs": 0, "wall_list": [], "thread_us": 0,
                                    "cpu_us": 0, "out_bytes": 0, "thread_sub_list": []})
        g["jobs"] += 1
        g["subjobs"] += d["sub"]
        g["wall_list"].append(d["wall"])
        g["thread_sub_list"].append(d["thread"])
        g["thread_us"] += d["thread"]
        g["cpu_us"] += d["cpu"]
        g["out_bytes"] += d["out_bytes"]

    result = {"label": label, "finished_events": events, "started_events": started,
              "logical_jobs": len(logical), "groups": {}}
    deep_wall = []; deep_thread = []; deep_jobs = 0; deep_sub = 0
    deep_thread_us = 0; deep_out = 0
    for key, g in groups.items():
        wl = sorted(g["wall_list"])
        sl = sorted(g["thread_sub_list"])
        gb = g["out_bytes"] / 1024**3
        item = {
            "jobs": g["jobs"], "subjobs": g["subjobs"],
            "total_thread_time_s": round(g["thread_us"] / 1e6, 1),
            "total_cpu_s": round(g["cpu_us"] / 1e6, 1),
            "out_gb": round(gb, 1),
            "avg_wall_s": round(sum(wl) / len(wl) / 1e6, 3),
            "p50_wall_s": round(percentile(wl, 50) / 1e6, 3),
            "p95_wall_s": round(percentile(wl, 95) / 1e6, 3),
            "p99_wall_s": round(percentile(wl, 99) / 1e6, 3),
            "p999_wall_s": round(percentile(wl, 99.9) / 1e6, 3),
            "max_wall_s": round(wl[-1] / 1e6, 3),
            "avg_subjob_thread_s": round(sum(sl) / len(sl) / 1e6, 3),
            "mb_s_per_subjob": round(gb * 1024 / (g["thread_us"] / 1e6), 1) if g["thread_us"] else 0,
        }
        result["groups"][key] = item
        if key.startswith("L") and key != "intra_L0" and key != "L0_L1":
            deep_wall += g["wall_list"]; deep_thread += g["thread_sub_list"]
            deep_jobs += g["jobs"]; deep_sub += g["subjobs"]
            deep_thread_us += g["thread_us"]; deep_out += g["out_bytes"]
    if deep_jobs:
        dwl = sorted(deep_wall)
        result["groups"]["deep_total_L1以下"] = {
            "jobs": deep_jobs, "subjobs": deep_sub,
            "total_thread_time_s": round(deep_thread_us / 1e6, 1),
            "out_gb": round(deep_out / 1024**3, 1),
            "avg_wall_s": round(sum(dwl) / len(dwl) / 1e6, 3),
            "p50_wall_s": round(percentile(dwl, 50) / 1e6, 3),
            "p95_wall_s": round(percentile(dwl, 95) / 1e6, 3),
            "p99_wall_s": round(percentile(dwl, 99) / 1e6, 3),
            "p999_wall_s": round(percentile(dwl, 99.9) / 1e6, 3),
            "max_wall_s": round(dwl[-1] / 1e6, 3),
        }
    return result

def main():
    logf, outj, label = sys.argv[1], sys.argv[2], sys.argv[3]
    r = parse(logf, label)
    json.dump(r, open(outj, "w"), indent=1)
    print(f"== {label}: events={r['finished_events']} started={r['started_events']} logical={r['logical_jobs']}")
    for k, g in r["groups"].items():
        print(f"  {k:>10}: jobs={g['jobs']:>5} wall avg={g['avg_wall_s']:>7.3f}s p50={g['p50_wall_s']:>7.3f} "
              f"p95={g['p95_wall_s']:>7.3f} p99={g['p99_wall_s']:>8.3f} max={g['max_wall_s']:>8.3f} "
              f"total_thread={g['total_thread_time_s']:>8.1f}s")

if __name__ == "__main__":
    main()
