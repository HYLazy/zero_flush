#!/usr/bin/env python3
"""step 0a 三臂对照分析：解析 fill_<arm>.log/.LOG，输出统一指标表。

用法: python3 analyze_0a2.py base2 A0c B1
指标口径：
  吞吐        db_bench fillrandom 行
  →L2/→L3     由 EVENT_LOG compaction_finished 的 output_level + 关联
              compaction_started 的 compaction_reason 归因（ManualCompaction
              = ZF 自己的下沉/清场；其余为原生 score 触发）
  trivial     EVENT_LOG trivial_move 次数与 total_files_size
  下沉输入    "Compaction start summary" 的 inputs 文件尺寸（L1/L2 分开）
  sink_diag   ZEROFLUSH PROPERTIES 里的 zf.sink_diag 单行
"""
import json
import re
import sys
import collections

G = 1e9


def parse(arm):
    log = f"fill_{arm}.LOG"
    txt = open(log, errors="replace").read().splitlines()
    out = {"arm": arm, "l2": 0, "l3": 0, "n2": 0, "n3": 0, "native": 0,
           "triv_n": 0, "triv_b": 0, "l1_in": 0, "l2_in": 0, "sink_n": 0,
           "lsm": None}
    start = {}
    for ln in txt:
        m = re.search(r"EVENT_LOG_v1 (\{.*\})", ln)
        if m:
            try:
                e = json.loads(m.group(1))
            except Exception:
                continue
            if e.get("event") == "compaction_started":
                start[e.get("job")] = e.get("compaction_reason", "?")
            elif e.get("event") == "compaction_finished":
                lvl = e.get("output_level")
                sz = e.get("total_output_size") or 0
                reason = start.get(e.get("job"), "?")
                if reason != "ManualCompaction":
                    out["native"] += sz
                elif lvl == 2:
                    out["l2"] += sz
                    out["n2"] += 1
                elif lvl == 3:
                    out["l3"] += sz
                    out["n3"] += 1
                if e.get("lsm_state"):
                    out["lsm"] = e["lsm_state"]
            elif e.get("event") == "trivial_move":
                out["triv_n"] += 1
                out["triv_b"] += e.get("total_files_size") or 0
        m2 = re.search(r"Compacting \d+@\d+ \+ \d+@\d+ files to L(\d+)", ln)
        if m2:
            out["_lvl"] = int(m2.group(1))
        m3 = re.search(r"Compaction start summary:.*inputs: (\[[^\]]*\]), (\[[^\]]*\])", ln)
        if m3 and out.get("_lvl") == 2:
            out["l1_in"] += sum(int(x) for x in re.findall(r"\((\d+)MB\)", m3.group(1))) << 20
            out["l2_in"] += sum(int(x) for x in re.findall(r"\((\d+)MB\)", m3.group(2))) << 20
            out["sink_n"] += 1
            out["_lvl"] = None
    # 吞吐与 sink_diag
    for ln in open(f"fill_{arm}.log", errors="replace"):
        m = re.search(r"fillrandom\s+:\s+([\d.]+) micros/op\s+(\d+) ops/sec", ln)
        if m:
            out["us"] = float(m.group(1))
            out["ops"] = int(m.group(2))
        if "zf.sink_diag : " in ln:
            out["diag"] = ln.split("zf.sink_diag : ", 1)[1].strip()
    return out


rows = [parse(a) for a in sys.argv[1:]]
print(f"{'arm':8s} {'ops/s':>8s} {'总输出GB':>9s} {'→L2':>7s} {'→L3':>7s} "
      f"{'原生GB':>7s} {'triv':>5s} {'trivGB':>7s} {'下沉次':>7s} "
      f"{'均L1MB':>7s} {'均L2MB':>7s} {'L2/L1':>6s}  LSM")
for r in rows:
    tot = r["l2"] + r["l3"]
    l1m = r["l1_in"] / max(r["sink_n"], 1) / 1e6
    l2m = r["l2_in"] / max(r["sink_n"], 1) / 1e6
    print(f"{r['arm']:8s} {r.get('ops',0):8d} {tot/G:9.1f} {r['l2']/G:7.1f} "
          f"{r['l3']/G:7.1f} {r['native']/G:7.1f} {r['triv_n']:5d} "
          f"{r['triv_b']/G:7.2f} {r['sink_n']:7d} {l1m:7.0f} {l2m:7.0f} "
          f"{r['l2_in']/max(r['l1_in'],1):6.1f}  {r['lsm']}")
print()
for r in rows:
    print(f"--- {r['arm']} sink_diag ---")
    for part in r.get("diag", "(none)").split("|"):
        print("   " + part.strip())
