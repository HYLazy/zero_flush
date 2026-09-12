#!/bin/bash
# 4 分区 × 4M 键 → 每分区 ~1M 键（≈1GB live），观察 L1 驻留变大时闭包是否随之放大
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/l0_closure
BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb-zeroflush/build_rel:/usr/lib/x86_64-linux-gnu
COMMON="--benchmarks=fillrandom --key_size=16 --value_size=1024 --num=2000000 \
--threads=16 --compression_type=none --cache_size=536870912 --max_background_jobs=24 \
--subcompactions=16 --histogram=true --statistics=false --use_existing_db=0"
echo "=== P4_part4 (trig=1e8, partitions=4) $(date '+%T') ==="
rm -rf "$RUN/db_P4"
ZF_L0_TRIGGER=100000000 $BIN $COMMON --zeroflush --zf_partitions=4 --zf_routing=sampled \
  --zf_value_cache_mb=64 --db="$RUN/db_P4" > "$RUN/results/P4_part4.stderr" 2>&1
cp "$RUN/db_P4/LOG" "$RUN/results/P4_part4.LOG" 2>/dev/null
grep -E "fillrandom.*ops/sec" "$RUN/results/P4_part4.stderr" | tail -1
echo P4_DONE >> "$RUN/results/summary.txt"
