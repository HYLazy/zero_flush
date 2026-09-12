#!/bin/bash
# R 实验：L1 驻留预算覆盖（ZF_L1_BUDGET_MB）对闭包的影响
# 形态 = H2（1 分区 / 16M 键 / 1× 覆盖 / trig=1e8 / merge 关 / 治理关）
# 对照：H2 基线 = 1GB L1 目标（无覆盖，已跑过：闭包 p50 2075MB / 事件 14 / 输出 13.3GiB）
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/l0_closure
BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb-zeroflush/build_rel:/usr/lib/x86_64-linux-gnu
COMMON="--benchmarks=fillrandom --key_size=16 --value_size=1024 --num=1000000 \
--threads=16 --compression_type=none --cache_size=536870912 --max_background_jobs=24 \
--subcompactions=16 --histogram=true --statistics=false --use_existing_db=0"
for budget in 256 64; do
  name="R_budget${budget}"
  echo "=== $name (ZF_L1_BUDGET_MB=$budget) $(date '+%T') ==="
  rm -rf "$RUN/db_$name"
  t0=$(date +%s)
  ZF_L1_BUDGET_MB=$budget $BIN $COMMON --zeroflush --zf_partitions=1 --zf_routing=sampled \
    --zf_value_cache_mb=64 --db="$RUN/db_$name" > "$RUN/results/$name.stderr" 2>&1
  rc=$?; t1=$(date +%s)
  cp "$RUN/db_$name/LOG" "$RUN/results/$name.LOG" 2>/dev/null
  grep -E "fillrandom.*ops/sec" "$RUN/results/$name.stderr" | tail -1
  grep "files\[" "$RUN/db_$name/LOG" | tail -1 | grep -o "files\[[0-9 ]*\]" > "$RUN/results/$name.meta"
  echo "$name rc=$rc wall=$((t1-t0))s" >> "$RUN/results/summary.txt"
done
echo BUDGET_DONE
