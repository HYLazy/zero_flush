#!/bin/bash
# 中规模：4M 键 / 每线程 1M ops → 总 16M ops（≈16GB API 字节，churn 4×）
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/l0_closure
BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb-zeroflush/build_rel:/usr/lib/x86_64-linux-gnu
COMMON="--benchmarks=fillrandom --key_size=16 --value_size=1024 --num=1000000 \
--threads=16 --compression_type=none --cache_size=536870912 --max_background_jobs=24 \
--subcompactions=16 --histogram=true --statistics=false --use_existing_db=0"
ZF="--zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64"
for arm in "M_trig1e8:100000000" "M_trig4:4"; do
  name="${arm%%:*}"; trig="${arm##*:}"
  echo "=== $name (trig=$trig) $(date '+%T') ==="
  rm -rf "$RUN/db_$name"
  t0=$(date +%s)
  ZF_L0_TRIGGER=$trig $BIN $COMMON $ZF --db="$RUN/db_$name" > "$RUN/results/$name.stderr" 2>&1
  rc=$?; t1=$(date +%s)
  cp "$RUN/db_$name/LOG" "$RUN/results/$name.LOG" 2>/dev/null
  echo "$name rc=$rc wall=$((t1-t0))s" >> "$RUN/results/summary.txt"
  grep -E "fillrandom.*ops/sec" "$RUN/results/$name.stderr" | tail -1
done
echo MID_DONE >> "$RUN/results/summary.txt"
