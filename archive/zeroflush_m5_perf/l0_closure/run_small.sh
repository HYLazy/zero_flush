#!/bin/bash
# 小规模 L0 闭包实验：--num 是每线程，故总 ops = num × threads
# 臂 A: trigger=1e8（字节项主导）  臂 B: trigger=4（文件数主导）
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/l0_closure
BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb-zeroflush/build_rel:/usr/lib/x86_64-linux-gnu
# 250K 唯一键 × 1KB；每线程 500K ops → 总 8M ops
COMMON="--benchmarks=fillrandom --key_size=16 --value_size=1024 --num=500000 \
--threads=16 --compression_type=none --cache_size=536870912 --max_background_jobs=24 \
--subcompactions=16 --histogram=true --statistics=false --use_existing_db=0"
ZF="--zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64"

run_arm() {
  local name="$1" trig="$2"
  echo "=== $name (ZF_L0_TRIGGER=$trig) $(date '+%T') ==="
  rm -rf "$RUN/db_$name"
  local t0=$(date +%s)
  ZF_L0_TRIGGER=$trig $BIN $COMMON $ZF --db="$RUN/db_$name" \
      > "$RUN/results/$name.stderr" 2>&1
  local rc=$?
  local t1=$(date +%s)
  cp "$RUN/db_$name/LOG" "$RUN/results/$name.LOG" 2>/dev/null
  echo "$name rc=$rc wall=$((t1-t0))s" >> "$RUN/results/summary.txt"
  grep -E "fillrandom.*ops/sec" "$RUN/results/$name.stderr" | tail -1
}
: > "$RUN/results/summary.txt"
run_arm A_trig1e8 100000000
run_arm B_trig4 4
echo DONE >> "$RUN/results/summary.txt"
