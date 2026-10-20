#!/bin/bash
# H16 臂：4 分区 × 16M 键域 × 16M ops 总写（≈1× 覆盖，接近 50GB 运行形态）
# 目标：验证闭包是否随「分区 live 数据量」增长到 GB 级；L1 驻留轨迹
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/l0_closure
BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb-zeroflush/build_rel:/usr/lib/x86_64-linux-gnu
# num 为每线程 op 数：16 线程 × 1M = 16M 总写（1× 覆盖 16M 键域）
COMMON="--benchmarks=fillrandom --key_size=16 --value_size=1024 --num=1000000 \
--threads=16 --compression_type=none --cache_size=536870912 --max_background_jobs=24 \
--subcompactions=16 --histogram=true --statistics=false --use_existing_db=0"
echo "=== H16 (4 分区 / 16M 键 / 1x 覆盖 / trig=1e8) $(date '+%T') ==="
rm -rf "$RUN/db_H16"
t0=$(date +%s)
ZF_L0_TRIGGER=100000000 $BIN $COMMON --zeroflush --zf_partitions=4 --zf_routing=sampled \
  --zf_value_cache_mb=64 --db="$RUN/db_H16" > "$RUN/results/H16.stderr" 2>&1
rc=$?; t1=$(date +%s)
cp "$RUN/db_H16/LOG" "$RUN/results/H16.LOG" 2>/dev/null
grep -E "fillrandom.*ops/sec" "$RUN/results/H16.stderr" | tail -1
echo "H16 rc=$rc wall=$((t1-t0))s" >> "$RUN/results/summary.txt"
# 保留 LSM 终态与 L1 文件尺寸分布
{ echo "=== final files[] ==="; grep "files\[" "$RUN/db_H16/LOG" | tail -1;
  echo "=== L1/L2 sizes ==="; du -sh "$RUN/db_H16" 2>/dev/null; } > "$RUN/results/H16.meta" 2>/dev/null
echo H16_DONE
