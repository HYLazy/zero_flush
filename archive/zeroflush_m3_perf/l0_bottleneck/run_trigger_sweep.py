#!/usr/bin/env python3
# 角度 B/C：L0 触发阈值剂量-响应实验（L1=4GB 固定，仅变 level0_file_num_compaction_trigger）
# 模型预测：每次 L0→L1 重写 |L1| + T·F 字节 → 用户吞叶 ∝ 1/(1 + |L1|/(T·F))
# T ∈ {2,8,16}；T=4 复用既有 base4g 运行（同 seed/参数）。
import os, subprocess, sys, time

BASE = "/home/embed/hyl/metadata_offload"
BIN = f"{BASE}/source/rocksdb/build/db_bench"
EXP = f"{BASE}/output/zeroflush_m3_perf/l0_bottleneck"
ENV = {**os.environ, "LD_LIBRARY_PATH": f"{BASE}/source/rocksdb/build:/usr/lib/x86_64-linux-gnu"}

COMMON = ["--num=103244288", "--writes=6452768", "--threads=16", "--key_size=16",
          "--value_size=1024", "--compression_type=none", "--disable_wal=false",
          "--cache_size=536870912", "--max_background_jobs=24", "--subcompactions=16",
          "--histogram", "--statistics", "--stats_dump_period_sec=60",
          "--stats_interval_seconds=30", "--stats_per_interval=1", "--seed=1"]
FIXED = "--level_compaction_dynamic_level_bytes=false --max_bytes_for_level_base=4294967296"

CONFIGS = [("trig4", "--level0_file_num_compaction_trigger=4")]

def run(args, logf, timeout):
    with open(logf, "w") as f:
        try:
            return subprocess.run(args, stdout=f, stderr=subprocess.STDOUT, timeout=timeout,
                                  env=ENV, cwd=BASE).returncode
        except subprocess.TimeoutExpired:
            return 124

def main():
    only = sys.argv[1:] if len(sys.argv) > 1 else None
    for name, extra in CONFIGS:
        if only and name not in only:
            continue
        db = f"/tmp/l0trig_{name}"
        os.system(f"rm -rf {db}")
        print(f"[{name}] fill start {time.strftime('%H:%M:%S')}", flush=True)
        rc = run([BIN, "--benchmarks=fillrandom", f"--db={db}"] + COMMON +
                 (FIXED + " " + extra).split(), f"{EXP}/fill_{name}.log", 36000)
        print(f"[{name}] fill rc={rc} {time.strftime('%H:%M:%S')}", flush=True)
        # fill LOG 在 reopen 前归档（R58）
        os.system(f"cp {db}/LOG {EXP}/LOG_{name} 2>/dev/null; rm -rf {db}")

if __name__ == "__main__":
    main()
