#!/usr/bin/env python3
import re, os
def parse(path, bench):
    d={"ops_s":None,"mbps":None,"wall":None,"ops":None,"found":None}
    lat=None
    if not os.path.exists(path): return d, lat
    t=open(path,errors="replace").read()
    m=re.search(rf"^{bench}\s+:\s+([0-9.]+) micros/op ([0-9]+) ops/sec ([0-9.]+) seconds ([0-9]+) operations;\s*([0-9.]+) MB/s(?: \(([0-9]+) of ([0-9]+) found\))?", t, re.M)
    if m:
        d={"ops_s":int(m.group(2)),"mbps":float(m.group(5)),"wall":float(m.group(3)),
           "ops":int(m.group(4)),"found":(f"{m.group(6)}/{m.group(7)}" if m.group(6) else None)}
    p=re.search(r"Percentiles:\s*P50:?\s*([0-9.]+)\s+P75:?\s*([0-9.]+)\s+P99:?\s*([0-9.]+)\s+P99\.9:?\s*([0-9.]+)\s+P99\.99:?\s*([0-9.]+)", t)
    if p: lat=[float(x) for x in p.groups()]
    return d,p

rows=[]
for tag,fill,read in (("ZeroFlush(merge=off)","zf_fill.log","zf_read.log"),("原生 RocksDB","native_fill.log","native_read.log")):
    f,fl=parse(fill,"fillrandom"); r,rl=parse(read,"readrandom")
    rows.append((tag,f,r,fl,rl))

out=["# 50GB / 1KB 对比（merge 关闭，公平版）vs 原生 RocksDB",""]
out.append("参数：16 线程、1KB 值、compression=none、cache 512MB、bg=24、subcompactions=16、")
out.append("num=48,000,000、readrandom 8,000,000 reads、histogram 延迟；ZF：sampled/16 分区、")
out.append("value_cache 64MB、**默认攒批（不显式 skip_batching）**、**merge 关闭**（zf_base_merge 默认 false）、治理默认关。")
out.append("")
out.append("> 环境注记（2026-09-12）：系统盘 WD_BLACK SN750 处于退化状态（flush 延迟尖峰达")
out.append("> 8.5s、盘温贴 83.8°C 节流线、fstrim 交还 830GiB 后仍不稳定），已做 fstrim +")
out.append("> wbt_lat_usec=0 + commit=60；每臂前有降温闸门、全程遥测。绝对吞吐低于 9/2 历史")
out.append("> 同参数据（详见 ../ENV_DIAGNOSIS.md 与本目录 run2_telemetry.log）。")
out.append("")
out.append("## 吞吐")
out.append("")
out.append("| 引擎 | fill ops/s | fill MB/s | fill wall | read ops/s | read MB/s | read 命中 |")
out.append("|---|---|---|---|---|---|---|")
for tag,f,r,fl,rl in rows:
    out.append(f"| {tag} | {f['ops_s']} | {f['mbps']} | {f['wall']}s | {r['ops_s']} | {r['mbps']} | {r['found']} |")
out.append("")
out.append("## 延迟（µs，histogram）")
out.append("")
out.append("| 引擎 | 阶段 | P50 | P75 | P99 | P99.9 | P99.99 |")
out.append("|---|---|---|---|---|---|---|")
for tag,f,r,fl,rl in rows:
    if fl: out.append(f"| {tag} | fill | "+" | ".join(str(x) for x in fl)+" |")
    if rl: out.append(f"| {tag} | read | "+" | ".join(str(x) for x in rl)+" |")
open("RESULTS.md","w").write("\n".join(out)+"\n")
print("\n".join(out))
