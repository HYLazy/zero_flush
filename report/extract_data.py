#!/usr/bin/env python3
"""Extract all report numbers from the experiment artifacts into report/data/data.json."""
import csv, glob, json, os, re, statistics as st

ROOT = "/home/embed/hyl/metadata_offload"
RUNS = {
    "bg2_arch": f"{ROOT}/bench/out/fillcpu_1kb100_default/run",     # default, load 2-3
    "bg2":      f"{ROOT}/bench/out/fillcpu_1kb100_bg2matched/run",  # matched baseline
    "bg8":      f"{ROOT}/bench/out/fillcpu_1kb100_bg8/run",
}
CAP = 16.5  # MB/s


def num(v, d=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def run_metrics(prefix):
    s = json.load(open(prefix + ".summary.json"))
    j = s["jobs"]
    app = s["workload"]["app_bytes"]
    out = {
        "bg_jobs": s.get("bg_jobs", 2), "fill_wall_s": s["fill_wall_s"],
        "fill_mbps": s["fill_rate_mb_s"], "fill_ops_s": s["fill_rate_ops_s"],
        "drain_wall_s": s["drain_wall_s"], "app_gb": app / 1e9,
        "cpu_fill": s["phase_fill"]["cpu_s"], "cpu_drain": s["phase_drain"]["cpu_s"],
        "cpu_fill_total": s["phase_fill"]["cpu_s_total"],
        "cpu_drain_total": s["phase_drain"]["cpu_s_total"],
        "flush_jobs": j["flush_jobs"], "flush_cpu_window_s": j["flush_cpu_s"],
        "flush_wall_s": j["flush_wall_s"], "flush_bytes_gb": j["flush_bytes"] / 1e9,
        "comp_jobs": j["comp_jobs"], "comp_cpu_rdb_s": j["comp_cpu_s_rdb"],
        "comp_cpu_self_s": j["comp_cpu_s_self"],
        "comp_in_gb": j["comp_in_bytes"] / 1e9, "comp_out_gb": j["comp_out_bytes"] / 1e9,
        "waf": j["comp_in_bytes"] / app,
        "writer_us_op": s["phase_fill"]["cpu_s"]["writer"] * 1e6 / max(s["ops_ok"], 1),
        "stall_begin": s["stalls"]["begin"], "stall_end": s["stalls"]["end"],
        "memtable_sealed": s["memtable_sealed"],
        "files_per_level": s["files_per_level_at_end"],
    }
    # samples-derived: throttle split, memory, timelines
    rows = list(csv.DictReader(open(prefix + ".samples.csv")))
    tl, tl10 = [], []
    t_lo = t_hi = b_lo = b_hi = 0.0
    for a, b in zip(rows, rows[1:]):
        dt = num(b["elapsed"]) - num(a["elapsed"])
        if dt <= 0:
            continue
        by = (int(b["ops_cum"]) - int(a["ops_cum"])) * 1040
        r = by / dt / 1e6
        (t_lo, b_lo) = (t_lo + dt, b_lo + by) if r <= CAP else (t_lo, b_lo)
        (t_hi, b_hi) = (t_hi + dt, b_hi + by) if r > CAP else (t_hi, b_hi)
        tl.append((num(a["elapsed"]), r, num(a["pending_comp"]) / 1e9, num(a["l0_files"]),
                   num(a["rss_kb"]) / 1024, num(a["table_readers"]) / 1048576,
                   num(a["mem_cur_all"]) / 1048576))
    tot = t_lo + t_hi
    out["throttle"] = {
        "wall_share": t_lo / tot if tot else 0, "data_share": b_lo / (b_lo + b_hi) if b_lo + b_hi else 0,
        "slow_mbps": b_lo / t_lo / 1e6 if t_lo else 0, "fast_mbps": b_hi / t_hi / 1e6 if t_hi else 0,
    }
    def g(r, k):
        return num(r.get(k))
    out["mem"] = {
        "rss_peak": max(g(r, "rss_kb") for r in rows) / 1024,
        "memtbl_peak": max(g(r, "mem_cur_all") for r in rows) / 1048576,
        "treaders_peak": max(g(r, "table_readers") for r in rows) / 1048576,
        "bcache_peak": max(g(r, "bcache_usage") for r in rows) / 1048576,
        "pending_peak": max(g(r, "pending_comp") for r in rows) / 1e9,
    }
    fill = [r for r in rows if int(r["phase"]) == 1]
    last = fill[-1] if fill else rows[-1]
    out["mem_fillend"] = {
        "rss": g(last, "rss_kb") / 1024, "memtbl": g(last, "mem_cur_all") / 1048576,
        "treaders": g(last, "table_readers") / 1048576, "bcache": g(last, "bcache_usage") / 1048576,
        "arena": g(last, "mall_arena") / 1048576, "mall_used": g(last, "mall_used") / 1048576,
    }
    out["mem_fillend"]["other"] = (out["mem_fillend"]["rss"] - out["mem_fillend"]["memtbl"]
                                   - out["mem_fillend"]["treaders"] - out["mem_fillend"]["bcache"])
    # 10s-smoothed rate timeline for charts
    step = 10.0
    sm = [(num(r["elapsed"]), int(r["ops_cum"]), num(r["pending_comp"]) / 1e9, num(r["l0_files"]),
           num(r["rss_kb"]), num(r["table_readers"]), num(r["mem_cur_all"])) for r in rows]
    sm.sort()
    tl10 = []
    j = 0
    for i in range(len(sm)):
        while sm[i][0] - sm[j][0] > step:
            j += 1
        dt = sm[i][0] - sm[j][0]
        if dt >= step * 0.9:
            tl10.append([sm[i][0], (sm[i][1] - sm[j][1]) * 1040 / dt / 1e6,
                         sm[i][2], sm[i][3], sm[i][4] / 1024, sm[i][5] / 1048576, sm[i][6] / 1048576])
    out["timeline"] = [[round(x, 3) if isinstance(x, float) else x for x in row] for row in tl10]
    # thread states by role
    stt = {}
    for r in csv.DictReader(open(prefix + ".thread_cpu.csv")):
        nm = r["name"]
        role = ("flush" if nm.startswith("rocksdb:high") else "comp" if nm.startswith("rocksdb:low")
                else "writer" if nm.startswith("fcw") else "driver")
        state_ch = r.get("state") or "?"
        if state_ch == "?":
            continue  # archived runs predate the state column
        stt.setdefault(role, {})
        stt[role][state_ch] = stt[role].get(state_ch, 0) + 1
    out["states"] = {k: {s: v / sum(d.values()) for s, v in d.items()} for k, d in stt.items() if d}
    # cfstats: High row = flush, Low row = compaction
    txt = open(prefix + ".cfstats_drainend.txt").read()
    for line in txt.splitlines():
        t = line.split()
        if t and t[0] in ("High", "Low") and len(t) > 12:
            try:
                out["cfstats_" + t[0]] = {"jobs": int(t[-6]), "comp_sec": float(t[-8]), "cpu_sec": float(t[-7])}
            except (ValueError, IndexError):
                pass
    m = re.search(r"Write Stall \(count\): (.*)", txt)
    if m:
        sr = {}
        for part in m.group(1).split(","):
            if ":" in part:
                k, v = part.split(":")
                try: sr[k.strip()] = int(v.strip())
                except ValueError: pass
        out["stall_reasons"] = sr
    # flush job stats (wall median)
    fj = list(csv.DictReader(open(prefix + ".flush_jobs.csv")))
    if fj:
        out["flush_job_median_wall_s"] = st.median(num(r["wall_us"]) for r in fj) / 1e6
        out["flush_job_median_cpu_s"] = st.median(num(r["cpu_us"]) for r in fj) / 1e6
    return out


def wpbench_rows():
    out = {}
    for f in ("wpbench_sweep.csv", "wpbench.csv", "wpbench_extra.csv", "wpbench_perf.csv",
              "wpbench_perf_default8g.csv"):
        p = f"{ROOT}/bench/out/wpbench/{f}"
        if os.path.exists(p):
            out[f] = list(csv.DictReader(open(p)))
    return out


def bg_sweep():
    rows = []
    for f in ("bgthreads.csv",):
        p = f"{ROOT}/bench/out/wpbench/{f}"
        for line in open(p):
            m = re.match(r"(\S+) (put\s+thr=\s*\d+.*?) MB/s=(\d+)", line)
            if m:
                cfg, _, mbps = m.groups()
                cpu = re.search(r"\(([\d.]+) us/op\)", line)
                rows.append({"cfg": cfg, "mbps": int(mbps), "us_op": float(cpu.group(1)) if cpu else None})
    for line in open("/tmp/big_bg.out", errors="ignore"):
        m = re.match(r"(\S+) 32GB: put.*MB/s=(\d+)", line)
        if m:
            rows.append({"cfg": m.group(1) + "_32GB", "mbps": int(m.group(2)), "us_op": None})
    return rows


def perf_top(prefix, role, n=8):
    files = sorted(glob.glob(os.path.join(os.path.dirname(prefix), "perf", f"perf_{role}_r*.txt")))
    agg, tot = {}, 0.0
    for f in files:
        for line in open(f):
            m = re.match(r"\s+([0-9.]+)%\s+\S+\s+\S+\s+\[.\] (.*)", line)
            if m:
                pct, sym = float(m.group(1)), m.group(2).strip()
                agg[sym] = agg.get(sym, 0.0) + pct
                tot += pct
    return [[round(v / tot * 100, 2), k] for k, v in sorted(agg.items(), key=lambda kv: -kv[1])[:n]] if tot else []


data = {"runs": {k: run_metrics(v) for k, v in RUNS.items()}, "wpbench": wpbench_rows(),
        "bg_sweep": bg_sweep(),
        "perf": {k: {role: perf_top(v, role) for role in ("flush", "comp", "writer")}
                 for k, v in RUNS.items()}}
os.makedirs(f"{ROOT}/report/data", exist_ok=True)
json.dump(data, open(f"{ROOT}/report/data/data.json", "w"), ensure_ascii=False, indent=1)
print("wrote report/data/data.json")
for k in RUNS:
    r = data["runs"][k]
    print(f"  {k}: fill={r['fill_wall_s']:.0f}s {r['fill_mbps']:.1f}MB/s WAF={r['waf']:.2f} "
          f"throttle_wall={r['throttle']['wall_share']*100:.1f}% rss_peak={r['mem']['rss_peak']:.0f}MB "
          f"states={ {s: round(v*100) for s, v in r['states'].get('comp', {}).items()} }")
print("  perf symbols:", {k: {ro: len(v) for ro, v in d.items()} for k, d in data["perf"].items()})
