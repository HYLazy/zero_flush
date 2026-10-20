#!/bin/bash
# H2 臂：1 个分区（单分区 = 全键域）→ 单分区 live = 16GB，检验 L1 输入是否随
# 「分区承载数据量」线性放大。其余与 H16 相同（16M 键 / 1x 覆盖 / trig=1e8）。
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/l0_closure
BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb-zeroflush/build_rel:/usr/lib/x86_64-linux-gnu
COMMON="--benchmarks=fillrandom --key_size=16 --value_size=1024 --num=1000000 \
--threads=16 --compression_type=none --cache_size=536870912 --max_background_jobs=24 \
--subcompactions=16 --histogram=true --statistics=false --use_existing_db=0"
echo "=== H2t4 (1 分区 trig=4 / 16M 键 / 1x 覆盖 / trig=1e8) $(date '+%T') ==="
rm -rf "$RUN/db_H2t4"
t0=$(date +%s)
ZF_L0_TRIGGER=4 $BIN $COMMON --zeroflush --zf_partitions=1 --zf_routing=sampled \
  --zf_value_cache_mb=64 --db="$RUN/db_H2t4" > "$RUN/results/H2t4.stderr" 2>&1
rc=$?; t1=$(date +%s)
cp "$RUN/db_H2t4/LOG" "$RUN/results/H2t4.LOG" 2>/dev/null
grep -E "fillrandom.*ops/sec" "$RUN/results/H2t4.stderr" | tail -1
echo "H2t4 rc=$rc wall=$((t1-t0))s" >> "$RUN/results/summary.txt"
grep "files\[" "$RUN/db_H2t4/LOG" | tail -1 > "$RUN/results/H2t4.meta" 2>/dev/null
echo H2t4_DONE
