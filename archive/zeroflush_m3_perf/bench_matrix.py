#!/usr/bin/env python3
# ZeroFlush vs 原生 RocksDB 完整 db_bench 矩阵
# KV 大小（value）：128/256/1024/2048（key=16）→ kv 总 144/272/1040/2064B
# workload（所有线程写入总和）：10/50/100 GB
# 每项：fillrandom(num = GB/(key+value), 16 整除) → readrandom(16M 读, histogram) → 清理
# 穿插：同 (kv, scale) 先 native 再 zf（环境漂移减半）
import json, os, re, subprocess, sys, time

BASE = "/home/embed/hyl/metadata_offload"
ZF_BIN = f"{BASE}/source/rocksdb-zeroflush/build/db_bench"
NAT_BIN = f"{BASE}/source/rocksdb/build/db_bench"
RESULT_F = os.path.join(BASE, "output/zeroflush_m3_perf/bench_matrix.json")

KV_SIZES = [128, 256, 1024, 2048]
WORKLOADS = [10, 50, 100]  # GB
KEY = 16
THREADS = 16

def run(cmd, timeout, logf):
    env = {**os.environ}
    if os.path.basename(cmd[0]) == "db_bench" and "rocksdb/build" in cmd[0]:
        # 原生二进制依赖 librocksdb.so.11（构建目录），不能污染全局（ZF 会混库）。
        env["LD_LIBRARY_PATH"] = "/home/embed/hyl/metadata_offload/source/rocksdb/build:/usr/lib/x86_64-linux-gnu"
    with open(logf, "w") as f:
        try:
            pc = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, timeout=timeout,
                                env=env, cwd=BASE)
            return pc.returncode
        except subprocess.TimeoutExpired:
            return 124

def parse(logf):
    out = {"fill": {}, "read": {}}
    try:
        txt = open(logf, errors="replace").read()
    except FileNotFoundError:
        return out
    # 结果行： "fillrandom   :  123.456 micros/op 78901 ops/sec 12.345 seconds 48000000 operations; 98.5 MB/s (X of Y found)"
    for sec, pat in (("fill", r"^fillrandom\s+:\s+([0-9.]+) micros/op ([0-9]+) ops/sec [0-9.]+ seconds ([0-9]+) operations;\s*([0-9.]+) MB/s"),
                     ("read", r"^readrandom\s+:\s+([0-9.]+) micros/op ([0-9]+) ops/sec [0-9.]+ seconds ([0-9]+) operations;\s*([0-9.]+) MB/s(?: \(([0-9]+) of ([0-9]+) found\))?")):
        m = re.search(pat, txt, re.M)
        if m:
            out[sec] = {"micros_op": float(m.group(1)), "ops_s": int(m.group(2)),
                        "ops": int(m.group(3)), "mbps": float(m.group(4))}
            if sec == "read" and m.group(5):
                out[sec]["found"] = int(m.group(5))
                out[sec]["read"] = int(m.group(6))
    # 延迟百分位（histogram）
    hist = {}
    for m in re.finditer(r"^\s*([0-9]+\.?[0-9]*)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)", txt, re.M):
        pass
    # db_bench histogram 段：
    hm = re.search(r"Latency Percentiles.*?50\n.*?(\d+)", txt, re.S)
    # 精确格式：
    hseg = re.search(r"P50:?\s*([0-9.]+)\s+P95:?\s*([0-9.]+)\s+P99:?\s*([0-9.]+)\s+P99\.9:?\s*([0-9.]+)\s+P99\.99:?\s*([0-9.]+)", txt)
    if hseg:
        out["percentiles"] = {"p50": float(hseg.group(1)), "p95": float(hseg.group(2)),
                              "p99": float(hseg.group(3)), "p999": float(hseg.group(4))}
    # 常见格式： "Latency Percentiles" 段行 "P50 = xxx"
    pm = re.search(r"P50\s*=\s*([0-9.]+) us.*?P95\s*=\s*([0-9.]+) us.*?P99\s*=\s*([0-9.]+) us.*?P99\.9\s*=\s*([0-9.]+) us", txt, re.S)
    if pm:
        out["percentiles"] = {"p50": float(pm.group(1)), "p95": float(pm.group(2)),
                              "p99": float(pm.group(3)), "p999": float(pm.group(4))}
    # 错误检测
    out["errors"] = (txt.count("put error") + txt.count("Assertion '") + txt.count("Check failed") + txt.count("Segmentation")) if txt else 0
    out["has_result"] = bool(out["fill"] or out["read"])
    return out

def one(engine, kv, gb, results, idx, total):
    nat = engine == "native"
    binp = NAT_BIN if nat else ZF_BIN
    bytes_ = gb * 1024**3
    kv_total = KEY + kv
    num = (bytes_ // kv_total // THREADS) * THREADS
    writes = num // THREADS
    db = f"/tmp/bench_{engine}_{kv}B_{gb}GB"
    logf = f"/tmp/bench_{engine}_{kv}B_{gb}GB.log"
    common = [binp, f"--num={num}", f"--writes={writes}", f"--threads={THREADS}",
              f"--key_size={KEY}", f"--value_size={kv}", "--compression_type=none",
              "--disable_wal=false", "--cache_size=536870912", "--max_background_jobs=24",
              "--subcompactions=16", "--histogram"]
    zflags = ["--zeroflush", "--zf_partitions=16", "--zf_routing=sampled", "--zf_base_merge",
              "--zf_skip_batching=false", "--zf_value_cache_mb=64"] if not nat else []
    read_extra = ["--reads=1000000", "--use_existing_db=true"]
    print(f"[{idx}/{total}] {engine} kv={kv} {gb}GB num={num} start {time.strftime('%H:%M:%S')}", flush=True)
    os.system(f"rm -rf {db}")
    rc = run(common + ["--benchmarks=fillrandom", f"--db={db}"] + zflags, 14400, logf)
    print(f"  fill rc={rc} {time.strftime('%H:%M:%S')}", flush=True)
    d1 = parse(logf)
    rc2 = None
    if rc == 0:
        rc2 = run(common + ["--benchmarks=readrandom", f"--db={db}"] + zflags + read_extra, 1800, logf + ".read")
        print(f"  read rc={rc2} {time.strftime('%H:%M:%S')}", flush=True)
        d2 = parse(logf + ".read")
    else:
        d2 = {}
    item = {"engine": engine, "kv_value": kv, "kv_total": kv_total, "scale_gb": gb,
            "num": num, "fill_rc": rc, "read_rc": rc2, "fill": d1.get("fill", {}),
            "read": d2.get("read", {}), "fill_percentiles": d1.get("percentiles", {}),
            "read_percentiles": d2.get("percentiles", {}), "errors": d1.get("errors", 0) + d2.get("errors", 0)}
    results.append(item)
    with open(RESULT_F, "w") as f:
        json.dump(results, f, indent=1)
    os.system(f"rm -rf {db}")
    return item

def main():
    results = []
    if os.path.exists(RESULT_F):
        results = json.load(open(RESULT_F))
    done = {(r["engine"], r["kv_value"], r["scale_gb"]) for r in results
            if r["fill_rc"] == 0 and r["fill"]}
    total = len(KV_SIZES) * len(WORKLOADS) * 2
    idx = len(results)
    # 穿插顺序：小规模先（快速验证），大规模后；每规模按 kv 从大到小（大写多、稳定）
    for gb in WORKLOADS:
        for kv in reversed(KV_SIZES):
            for engine in ("native", "zf"):
                if (engine, kv, gb) in done:
                    continue
                one(engine, kv, gb, results, idx + 1, total)
                idx += 1

if __name__ == "__main__":
    main()
