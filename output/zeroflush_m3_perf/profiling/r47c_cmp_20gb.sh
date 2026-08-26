#!/bin/bash
# R47b：50GB / 1KB（100GB 两引擎均受写放大停写卡死，降规模） KV 读写对比（原生 RocksDB vs ZeroFlush 当前版）。
# 公平同参：key 16B、value 1024B、num=100M（≈100GB）、16 线程、
# compression none、WAL 开、cache 512MB、max_background_jobs=24。
# 每引擎 fillrandom,readrandom（同进程，1M 次随机点读）后立即清除 DB。
set -u
NATIVE=/home/embed/hyl/metadata_offload/source/rocksdb/build/db_bench
ZF=/home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/build/db_bench
OUT=/home/embed/hyl/metadata_offload/output/zeroflush_m3_perf/profiling/R47c
mkdir -p "$OUT"

COMMON="--benchmarks=fillrandom,readrandom --num=20000000 --reads=1000000 \
--threads=16 --key_size=16 --value_size=1024 --compression_type=none \
--disable_wal=false --histogram=true --statistics=true \
--cache_size=536870912 --max_background_jobs=24"

echo "=== [R47] native start $(date) ===" | tee -a "$OUT/run.log"
rm -rf /tmp/zf_cmp_native
LD_LIBRARY_PATH=/home/embed/hyl/metadata_offload/source/rocksdb/build:/usr/lib/x86_64-linux-gnu "$NATIVE" $COMMON --db=/tmp/zf_cmp_native > "$OUT/native.txt" 2>&1
echo "native rc=$?" >> "$OUT/native.txt"
rm -rf /tmp/zf_cmp_native
echo "=== [R47] native done $(date) ===" | tee -a "$OUT/run.log"

echo "=== [R47] zf start $(date) ===" | tee -a "$OUT/run.log"
rm -rf /tmp/zf_cmp_zf
"$ZF" $COMMON --db=/tmp/zf_cmp_zf --zeroflush --zf_partitions=16 \
    --zf_routing=align_l1 --zf_base_merge --zf_skip_batching=false \
    --subcompactions=16 \
    > "$OUT/zf.txt" 2>&1
echo "zf rc=$?" >> "$OUT/zf.txt"
rm -rf /tmp/zf_cmp_zf
echo "=== [R47] zf done $(date) ===" | tee -a "$OUT/run.log"

echo "ALL DONE $(date)" >> "$OUT/run.log"
