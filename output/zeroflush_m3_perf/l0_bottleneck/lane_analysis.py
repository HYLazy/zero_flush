#!/usr/bin/env python3
# 角度 A：compaction 车道并发度/利用率归因。
# 从 LOG 的 compaction_finished 事件（行首时间戳 = 结束时间，事件内
# compaction_time_micros = 时长）重建每个 job 的 [start, end] 区间，按输出层
# 分车道统计：最大并发、忙时利用率（Σ活跃时长/wall）、与其它车道的重叠。
# 用法: python3 lane_analysis.py <LOG> <out_json> <label>
import json, os, re, sys
from datetime import datetime

def ts_epoch(ts_str):
    return datetime.strptime(ts_str, "%Y/%m/%d-%H:%M:%S.%f").timestamp()

def parse(logf, label):
    jobs = []  # (start, end, level)
    t0, t1 = None, None
    pat_ts = re.compile(r"^(\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d+)")
    for line in open(logf, errors="replace"):
        m = pat_ts.match(line)
        if not m:
            continue
        ts = ts_epoch(m.group(1))
        t0 = ts if t0 is None else t0
        t1 = ts
        if '"event": "compaction_finished"' not in line:
            continue
        i, j = line.find("{"), line.rfind("}")
        try:
            e = json.loads(line[i:j+1])
        except json.JSONDecodeError:
            continue
        micros = e.get("compaction_time_micros", 0)
        lvl = e.get("output_level")
        if lvl is None:
            continue
        jobs.append((ts - micros / 1e6, ts, lvl))
    wall = (t1 - t0) if t1 and t0 else 0

    lanes = {}
    for s, e_, lvl in jobs:
        key = ("intra_L0" if lvl == 0 else "L0_L1" if lvl == 1 else f"deep_L{lvl-1}_L{lvl}")
        lanes.setdefault(key, []).append((s, e_))

    def lane_stats(intervals):
        intervals = sorted(intervals)
        # 最大并发（扫描线）
        events = []
        for s, e_ in intervals:
            events.append((s, 1))
            events.append((e_, -1))
        events.sort()
        cur = mx = 0
        for _, d in events:
            cur += d
            mx = max(mx, cur)
        busy = sum(e_ - s for s, e_ in intervals)
        # 合并重叠后的真实占用时长
        merged = 0
        cs, ce = None, None
        for s, e_ in intervals:
            if cs is None:
                cs, ce = s, e_
            elif s <= ce:
                ce = max(ce, e_)
            else:
                merged += ce - cs
                cs, ce = s, e_
        if cs is not None:
            merged += ce - cs
        return {"jobs": len(intervals), "max_concurrency": mx,
                "sum_wall_s": round(busy, 1), "merged_busy_s": round(merged, 1),
                "utilization": round(merged / wall, 3) if wall else 0}

    result = {"label": label, "wall_s": round(wall, 1), "lanes": {}}
    for key, iv in lanes.items():
        result["lanes"][key] = lane_stats(iv)
    # 汇总车道：全部 L0→L1（应为串行）、全部深层、全部（总并发）
    l01 = [iv for k, ivs in lanes.items() if k == "L0_L1" for iv in ivs]
    deep = [iv for k, ivs in lanes.items() if k.startswith("deep") for iv in ivs]
    allj = [iv for ivs in lanes.values() for iv in ivs]
    result["lanes"]["ALL_L0_L1"] = lane_stats(l01) if l01 else None
    result["lanes"]["ALL_deep"] = lane_stats(deep) if deep else None
    result["lanes"]["ALL_jobs"] = lane_stats(allj)
    return result

def main():
    logf, outj, label = sys.argv[1], sys.argv[2], sys.argv[3]
    r = parse(logf, label)
    json.dump(r, open(outj, "w"), indent=1)
    print(f"== {label} wall={r['wall_s']:.0f}s")
    for k in ("ALL_L0_L1", "ALL_deep", "ALL_jobs", "intra_L0"):
        g = r["lanes"].get(k)
        if g:
            print(f"  {k:>12}: jobs={g['jobs']:>5} max_conc={g['max_concurrency']:>3} "
                  f"busy={g['merged_busy_s']:>8.0f}s util={g['utilization']*100:>5.1f}%")

if __name__ == "__main__":
    main()
