#!/usr/bin/env python3
# 解析原生 RocksDB 100GB fillrandom 实验输出（RocksDB 11.2 stats 格式）：
# 1) 各层写放大（cumulative Compaction Stats 表）
# 2) 三类写停顿（Write Stall (count) 按原因 + Cumulative stall 时长 + interval 时间线）
# 3) L0→L1 / intra-L0 / 深层 compaction 的 SSTable 数量（compaction_started files_L* 数组）
import json, os, re

BASE = "/home/embed/hyl/metadata_offload"
EXP = os.path.join(BASE, "output/zeroflush_m3_perf/native_wa_stall_100gb_t8")
LOG = __import__("sys").argv[1] if len(__import__("sys").argv) > 1 else "/tmp/wa_exp_t8/LOG"
OUT = os.path.join(EXP, "native_wa_stall_100gb_t8.json")

def parse_size(tok):
    m = re.match(r"([0-9.]+)([KMG]?B|)", tok)
    if not m:
        return 0.0
    v = float(m.group(1))
    mult = {"KB": 1.0/1024, "MB": 1.0, "GB": 1024.0}.get(m.group(2), 1.0/1024/1024)
    return v * mult  # MB

def parse_compaction_tables(txt):
    """全部 'L0/L1/.../Sum' 行（每次 stats dump 一组；cumulative 表含全部列）。"""
    tables = []
    for m in re.finditer(r"\*\* Compaction Stats \[default\] \*\*\n.*?\n-+\n(.*?)\n\n", txt, re.S):
        rows = []
        for line in m.group(1).splitlines():
            tok = line.split()
            if len(tok) < 20 or not re.match(r"^(L\d+|Sum|Int)$", tok[0]):
                continue
            try:
                rows.append({
                    "level": tok[0],
                    "files": int(tok[1].split("/")[0]),
                    "size_mb": parse_size(tok[2] + tok[3]),
                    "score": float(tok[4]),
                    "read_gb": float(tok[5]),
                    "rn_gb": float(tok[6]),
                    "rnp1_gb": float(tok[7]),
                    "write_gb": float(tok[8]),
                    "moved_gb": float(tok[11]),
                    "w_amp": float(tok[12]),
                    "comp_sec": float(tok[15]),
                    "count": int(tok[17]),
                })
            except (ValueError, IndexError):
                continue
        if rows:
            tables.append(rows)
    return tables

def parse_stalls(txt):
    stalls = []
    for m in re.finditer(r"Write Stall \(count\): (.*)", txt):
        entry = {}
        for kv in re.finditer(r"([a-zA-Z0-9\-]+): (\d+)", m.group(1)):
            entry[kv.group(1)] = int(kv.group(2))
        if entry:
            stalls.append(entry)
    return stalls

def parse_stall_time(txt):
    out = []
    for m in re.finditer(r"Cumulative stall: ([0-9:.]+) H:M:S, ([0-9.]+) percent", txt):
        h, mn, rest = m.group(1).split(":")
        out.append({"secs": int(h) * 3600 + int(mn) * 60 + float(rest),
                    "percent": float(m.group(2))})
    return out

def parse_interval_stalls(txt):
    out = []
    for m in re.finditer(r"Interval stall: ([0-9:.]+) H:M:S, ([0-9.]+) percent", txt):
        h, mn, rest = m.group(1).split(":")
        out.append({"secs": int(h) * 3600 + int(mn) * 60 + float(rest),
                    "percent": float(m.group(2))})
    return out

def parse_compaction_events(txt):
    jobs = {}
    for m in re.finditer(r'EVENT_LOG_v1 \{[^}]*"job": (\d+)[^}]*"event": "compaction_started"', txt):
        seg = txt[m.end():txt.find("}\n", m.end())]
        files_by_level = {}
        for lm in re.finditer(r'"files_(L\d+)": \[([^\]]*)\]', seg):
            files_by_level[lm.group(1)] = len([x for x in lm.group(2).split(",") if x.strip()])
        jobs[m.group(1)] = {"files_by_level": files_by_level, "output_level": None}
    for m in re.finditer(r'EVENT_LOG_v1 \{[^}]*"job": (\d+)[^}]*"event": "compaction_finished"[^}]*"output_level": (\d+)', txt):
        if m.group(1) in jobs:
            jobs[m.group(1)]["output_level"] = int(m.group(2))
    return {k: v for k, v in jobs.items() if v["output_level"] is not None}

def summarize(jobs):
    by_level = {}
    for j in jobs.values():
        ol = j["output_level"]
        n_in = sum(j["files_by_level"].values())
        d = by_level.setdefault(ol, {"jobs": 0, "input_files": 0, "l0_inputs": 0,
                                     "nonl0_inputs": 0, "sizes": []})
        d["jobs"] += 1
        d["input_files"] += n_in
        d["sizes"].append(n_in)
        d["l0_inputs"] += j["files_by_level"].get("L0", 0)
        d["nonl0_inputs"] += sum(n for l, n in j["files_by_level"].items() if l != "L0")
    for ol, d in by_level.items():
        s = sorted(d.pop("sizes"))
        d["avg_input"] = round(d["input_files"] / d["jobs"], 2)
        d["max_input"] = s[-1]
        d["median_input"] = s[len(s)//2]
    return by_level

def main():
    txt = open(LOG, errors="replace").read()
    tables = parse_compaction_tables(txt)
    final = tables[-1] if tables else []
    timeline = [{r["level"]: r["write_gb"] for r in t if r["level"] != "Sum"} for t in tables]
    stalls = parse_stalls(txt)
    times = parse_stall_time(txt)
    jobs = parse_compaction_events(txt)

    result = {
        "experiment": "native rocksdb 100GB/1KB fillrandom (8 threads)",
        "num_stats_dumps": len(tables),
        "final_compaction_stats": final,
        "write_gb_timeline": timeline,
        "stall_counts_final": stalls[-1] if stalls else {},
        "stall_time_final": times[-1] if times else {},
        "interval_stall_timeline": parse_interval_stalls(txt),
        "compaction_jobs_matched": len(jobs),
        "compactions_by_output_level": {str(k): v for k, v in sorted(summarize(jobs).items())},
    }
    json.dump(result, open(OUT, "w"), indent=1)
    print("dumps:", result["num_stats_dumps"], "jobs:", len(jobs))
    print("stalls:", json.dumps(result["stall_counts_final"]))
    print("stall_time:", result["stall_time_final"])
    for ol, d in result["compactions_by_output_level"].items():
        print(f"  out L{ol}: jobs={d['jobs']} in_files={d['input_files']} "
              f"(L0={d['l0_inputs']}, other={d['nonl0_inputs']}) avg={d['avg_input']} max={d['max_input']}")
    print("final compaction stats:")
    for r in final:
        print(f"  {r['level']:>4} files={r['files']:>4} size={r['size_mb']:>8.1f}MB rn={r['rn_gb']:.2f}GB "
              f"rnp1={r['rnp1_gb']:.2f}GB write={r['write_gb']:.2f}GB w_amp={r['w_amp']:.2f} cnt={r['count']}")

if __name__ == "__main__":
    main()
