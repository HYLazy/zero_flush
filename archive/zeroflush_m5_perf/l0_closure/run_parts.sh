#!/bin/bash
# 分区数扫描：同 H16 形态（16M 键 / 1× 覆盖 / trig=1e8 / 1 分区/16 分区已跑过）
# 新跑 16 / 64 / 256 分区，与 H16(4 分区) 对照
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/l0_closure
BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb-zeroflush/build_rel:/usr/lib/x86_64-linux-gnu
COMMON="--benchmarks=fillrandom --key_size=16 --value_size=1024 --num=1000000 \
--threads=16 --compression_type=none --cache_size=536870912 --max_background_jobs=24 \
--subcompactions=16 --histogram=true --statistics=false --use_existing_db=0"
for P in 16 64 256; do
  name="P${P}_parts"
  echo "=== $name (partitions=$P) $(date '+%T') ==="
  rm -rf "$RUN/db_$name"
  t0=$(date +%s)
  ZF_L0_TRIGGER=100000000 $BIN $COMMON --zeroflush --zf_partitions=$P \
    --zf_routing=sampled --zf_value_cache_mb=64 --db="$RUN/db_$name" \
    > "$RUN/results/$name.stderr" 2>&1
  rc=$?; t1=$(date +%s)
  cp "$RUN/db_$name/LOG" "$RUN/results/$name.LOG" 2>/dev/null
  grep -E "fillrandom.*ops/sec" "$RUN/results/$name.stderr" | tail -1
  grep "files\[" "$RUN/db_$name/LOG" | tail -1 | grep -o "files\[[0-9 ]*\]" > "$RUN/results/$name.meta"
  echo "$name rc=$rc wall=$((t1-t0))s" >> "$RUN/results/summary.txt"
done
echo PARTS_DONE
