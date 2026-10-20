#!/bin/bash
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/bench100g_full
BIN=/home/embed/hyl/zf_fix/source/rocksdb-zeroflush/db_bench
export LD_LIBRARY_PATH=/home/embed/hyl/zf_fix/source/rocksdb-zeroflush:/usr/lib/x86_64-linux-gnu
DB=$RUN/db
cd "$RUN"
rm -rf "$DB"
echo "start $(date '+%F %T')" > run.log
echo "bin=$(md5sum $BIN|cut -d' ' -f1) so=$(md5sum /home/embed/hyl/zf_fix/source/rocksdb-zeroflush/librocksdb.so.11|cut -d' ' -f1)" >> run.log
ZF_BENCH_ORPHAN_RECOVER=1 ZF_L0_TRIGGER=100000000 $BIN \
  --benchmarks=fillrandom --key_size=16 --value_size=1024 --num=100000000 \
  --writes=6250000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=0 \
  --zeroflush --zf_partitions=16 --zf_routing=static --zf_static_span=100000000 \
  --zf_value_cache_mb=64 --zf_base_merge=true --db="$DB" > fill.log 2>&1
echo "fill_rc=$? fill_end=$(date '+%F %T')" >> run.log
cp "$DB/LOG" fill.LOG 2>/dev/null
ZF_BENCH_ORPHAN_RECOVER=1 $BIN --benchmarks=readrandom --key_size=16 --value_size=1024 \
  --num=100000000 --reads=2000000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=1 \
  --zeroflush --zf_partitions=16 --zf_routing=static --zf_static_span=100000000 \
  --zf_value_cache_mb=64 --zf_base_merge=true --db="$DB" > read.log 2>&1
echo "read_rc=$? read_end=$(date '+%F %T')" >> run.log
cp "$DB/LOG" read.LOG 2>/dev/null
du -sh "$DB" >> run.log
echo ALL_DONE >> run.log
