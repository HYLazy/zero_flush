#!/usr/bin/env python3
# L0→L1 重要性实验驱动：三配置顺序执行（fill 100GB + readrandom），逐配置解析
import json, os, subprocess, sys, time

BASE = "/home/embed/hyl/metadata_offload"
BIN = f"{BASE}/source/rocksdb/build/db_bench"
EXP = f"{BASE}/output/zeroflush_m3_perf/l0_l1_importance"
ENV = {**os.environ, "LD_LIBRARY_PATH": f"{BASE}/source/rocksdb/build:/usr/lib/x86_64-linux-gnu"}

CONFIGS = [
    ("base256m", "--level_compaction_dynamic_level_bytes=false --max_bytes_for_level_base=268435456"),
    ("base1g",   "--level_compaction_dynamic_level_bytes=false --max_bytes_for_level_base=1073741824"),
    ("base4g",   "--level_compaction_dynamic_level_bytes=false --max_bytes_for_level_base=4294967296"),
]
COMMON = ["--num=103244288", "--writes=6452768", "--threads=16", "--key_size=16",
          "--value_size=1024", "--compression_type=none", "--disable_wal=false",
          "--cache_size=536870912", "--max_background_jobs=24", "--subcompactions=16",
          "--histogram", "--statistics", "--stats_dump_period_sec=60",
          "--stats_interval_seconds=30", "--stats_per_interval=1", "--seed=1"]

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
        db = f"/tmp/l0exp_{name}"
        if os.path.exists(db):
            os.system(f"rm -rf {db}")
        print(f"[{name}] fill start {time.strftime('%H:%M:%S')}", flush=True)
        rc = run([BIN, "--benchmarks=fillrandom", f"--db={db}"] + COMMON + extra.split(),
                 f"{EXP}/fill_{name}.log", 36000)
        print(f"[{name}] fill rc={rc} {time.strftime('%H:%M:%S')}", flush=True)
        # fill 的 stats dump 在 LOG；reopen（readrandom）会把它轮转到 LOG.old.*——
        # 必须在 reopen 前归档全部 LOG*（R58 教训：先 read 后 cp 丢失 fill stats）。
        os.system(f"cp {db}/LOG* {EXP}/ 2>/dev/null; rm -rf {db}")

if __name__ == "__main__":
    main()
