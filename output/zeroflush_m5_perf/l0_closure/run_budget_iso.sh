#!/bin/bash
# 隔离臂：L0 字节阈值固定 1GB（ZF_MAX_L1_BYTES 显式设回），仅 L1 预算降到 256MB
# 对照 H2 基线（1GB L1 目标）：闭包 p50 2075MB / 事件 14 / L1输入 1006MB / 输出 0.87×
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/l0_closure
BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb-zeroflush/build_rel:/usr/lib/x86_64-linux-gnu
COMMON="--benchmarks=fillrandom --key_size=16 --value_size=1024 --num=1000000 \
--threads=16 --compression_type=none --cache_size=536870912 --max_background_jobs=24 \
--subcompactions=16 --histogram=true --statistics=false --use_existing_db=0"
name="R_iso256"
echo "=== $name (L0 阈值=1GB, L1 预算=256MB) $(date '+%T') ==="
rm -rf "$RUN/db_$name"
t0=$(date +%s)
ZF_MAX_L1_BYTES=1073741824 ZF_L1_BUDGET_MB=256 $BIN $COMMON --zeroflush \
  --zf_partitions=1 --zf_routing=sampled --zf_value_cache_mb=64 \
  --db="$RUN/db_$name" > "$RUN/results/$name.stderr" 2>&1
rc=$?; t1=$(date +%s)
cp "$RUN/db_$name/LOG" "$RUN/results/$name.LOG" 2>/dev/null
grep -E "fillrandom.*ops/sec" "$RUN/results/$name.stderr" | tail -1
grep "files\[" "$RUN/db_$name/LOG" | tail -1 | grep -o "files\[[0-9 ]*\]" > "$RUN/results/$name.meta"
echo "$name rc=$rc wall=$((t1-t0))s" >> "$RUN/results/summary.txt"
echo ISO_DONE
