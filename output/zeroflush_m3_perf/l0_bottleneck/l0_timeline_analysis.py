#!/usr/bin/env python3
# 角度 A 补充：L0 文件数时间线 × L0→L1 忙窗口重合分析。
# 每条 compaction_finished 携带 lsm_state（各层文件数）与时间戳 + 时长 →
# (1) L0 文件数随时间轨迹；(2) L0→L1 忙区间；(3) L0 文件数高位（≥slowdown=20）
#     的时间落在 L0→L1 忙窗口内的比例。
import json, re, sys
from datetime import datetime

def main():
    logf = sys.argv[1]
    pat_ts = re.compile(r"^(\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d+)")
    l0_samples = []   # (t, l0_count)
    l01_windows = []  # (start, end)
    for line in open(logf, errors="replace"):
        m = pat_ts.match(line)
        if not m or '"event": "compaction_finished"' not in line:
            continue
        ts = datetime.strptime(m.group(1), "%Y/%m/%d-%H:%M:%S.%f").timestamp()
        i, j = line.find("{"), line.rfind("}")
        try:
            e = json.loads(line[i:j+1])
        except json.JSONDecodeError:
            continue
        lsm = e.get("lsm_state")
        if lsm:
            l0_samples.append((ts, lsm[0]))
        if e.get("output_level") == 1:
            micros = e.get("compaction_time_micros", 0)
            l01_windows.append((ts - micros / 1e6, ts))
    t0 = min(t for t, _ in l0_samples)
    samples = [(t - t0, c) for t, c in l0_samples]
    windows = [(s - t0, e_ - t0) for s, e_ in l01_windows]
    wall = max(t for t, _ in samples)

    def in_window(t):
        return any(s <= t <= e_ for s, e_ in windows)

    high = [(t, c) for t, c in samples if c >= 20]
    high_in = sum(1 for t, c in high if in_window(t))
    busy_total = sum(e_ - s for s, e_ in windows)
    print(f"wall={wall:.0f}s samples={len(samples)} L0→L1 jobs={len(windows)} busy={busy_total:.0f}s")
    print(f"L0≥20 样本: {len(high)}（{len(high)/len(samples)*100:.1f}%），其中落在 L0→L1 忙窗口: "
          f"{high_in}（{high_in/max(1,len(high))*100:.1f}%）")
    cs = [c for _, c in samples]
    cs.sort()
    print(f"L0 文件数: min={cs[0]} p50={cs[len(cs)//2]} p90={cs[int(len(cs)*0.9)]} max={cs[-1]}")
    json.dump({"wall": wall, "l0_timeline": samples, "l01_windows": windows,
               "high_in_window_pct": high_in / max(1, len(high)) * 100},
              open(sys.argv[2], "w"))

if __name__ == "__main__":
    main()
