#!/bin/bash
# R47e：1KB / 100GB（总写入）读写对比（原生 RocksDB vs ZeroFlush 当前版）。
# 总共 100GB = num=100M keys × 1KB，--writes=6,250,000/线程（总 100M ops，
# 非每线程 100GB）。fill 后分开进程 readrandom（use_existing_db，物化完成
# 后读——避免 ZF frozen 窗口的时序偏差）。每引擎测完清除 DB。
set -u
NATIVE=/home/embed/hyl/metadata_offload/source/rocksdb/build/db_bench
ZF=/home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/build/db_bench
OUT=/home/embed/hyl/metadata_offload/output/zeroflush_m3_perf/profiling/R47e
mkdir -p "$OUT"

COMMON="--num=100000000 --writes=6250000 --threads=16 --key_size=16 \
--value_size=1024 --compression_type=none --disable_wal=false \
--histogram=true --statistics=true --cache_size=536870912 \
--max_background_jobs=24"

echo "=== [R47e] native fill start $(date) ===" | tee -a "$OUT/run.log"
rm -rf /tmp/zf_cmp_native
LD_LIBRARY_PATH=/home/embed/hyl/metadata_offload/source/rocksdb/build:/usr/lib/x86_64-linux-gnu \
  "$NATIVE" --benchmarks=fillrandom $COMMON --db=/tmp/zf_cmp_native \
  > "$OUT/native_fill.txt" 2>&1
echo "native fill rc=$?" >> "$OUT/native_fill.txt"
echo "=== [R47e] native read start $(date) ===" | tee -a "$OUT/run.log"
LD_LIBRARY_PATH=/home/embed/hyl/metadata_offload/source/rocksdb/build:/usr/lib/x86_64-linux-gnu \
  "$NATIVE" --benchmarks=readrandom --use_existing_db $COMMON --reads=1000000 \
  --db=/tmp/zf_cmp_native > "$OUT/native_read.txt" 2>&1
echo "native read rc=$?" >> "$OUT/native_read.txt"
rm -rf /tmp/zf_cmp_native
echo "=== [R47e] native done $(date) ===" | tee -a "$OUT/run.log"

echo "=== [R47e] zf fill start $(date) ===" | tee -a "$OUT/run.log"
rm -rf /tmp/zf_cmp_zf
"$ZF" --benchmarks=fillrandom $COMMON --db=/tmp/zf_cmp_zf --zeroflush \
  --zf_partitions=16 --zf_routing=align_l1 --zf_base_merge \
  --zf_skip_batching=false --zf_value_cache_mb=64 --subcompactions=16 \
  > "$OUT/zf_fill.txt" 2>&1
echo "zf fill rc=$?" >> "$OUT/zf_fill.txt"
echo "=== [R47e] zf read start $(date) ===" | tee -a "$OUT/run.log"
"$ZF" --benchmarks=readrandom --use_existing_db $COMMON --reads=1000000 \
  --db=/tmp/zf_cmp_zf --zeroflush --zf_partitions=16 --zf_routing=align_l1 \
  --zf_base_merge --zf_skip_batching=false --zf_value_cache_mb=64 \
  --subcompactions=16 > "$OUT/zf_read.txt" 2>&1
echo "zf read rc=$?" >> "$OUT/zf_read.txt"
rm -rf /tmp/zf_cmp_zf
echo "=== [R47e] zf done $(date) ===" | tee -a "$OUT/run.log"
echo "ALL DONE $(date)" >> "$OUT/run.log"
