#!/bin/bash
# 50GB / 1KB —— 修复分支 ec096ab，**固定 range 路由**（目标模式要求的协议）
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/fix_50gb_static
BIN=/home/embed/hyl/zf_fix/source/rocksdb-zeroflush/db_bench
export LD_LIBRARY_PATH=/home/embed/hyl/zf_fix/source/rocksdb-zeroflush:/usr/lib/x86_64-linux-gnu
DB=$RUN/db
rm -rf "$DB"
cd "$RUN"
echo "start $(date '+%F %T')" > run.log
echo "bin=$(md5sum $BIN | cut -d' ' -f1) so=$(md5sum /home/embed/hyl/zf_fix/source/rocksdb-zeroflush/librocksdb.so.11 | cut -d' ' -f1)" >> run.log
ZF_L0_TRIGGER=100000000 $BIN \
  --benchmarks=fillrandom --key_size=16 --value_size=1024 --num=48000000 \
  --writes=3000000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=0 \
  --zeroflush --zf_partitions=16 --zf_routing=static --zf_static_span=48000000 \
  --zf_value_cache_mb=64 --zf_base_merge=true --db="$DB" > fill.log 2>&1
echo "fill_rc=$? fill_end=$(date '+%F %T')" >> run.log
cp "$DB/LOG" fill.LOG 2>/dev/null
$BIN --benchmarks=readrandom --key_size=16 --value_size=1024 --num=48000000 \
  --reads=8000000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=1 \
  --zeroflush --zf_partitions=16 --zf_routing=static --zf_static_span=48000000 \
  --zf_value_cache_mb=64 --zf_base_merge=true --db="$DB" > read.log 2>&1
echo "read_rc=$? read_end=$(date '+%F %T')" >> run.log
cp "$DB/LOG" read.LOG 2>/dev/null
du -sh "$DB" >> run.log
for ext in sst log; do n=$(find "$DB" -name "*.$ext" | wc -l); s=$(find "$DB" -name "*.$ext" -printf "%s\n" | awk '{a+=$1} END {printf "%.2f", a/1073741824}'); echo "$ext n=$n size=${s}GB" >> run.log; done
echo ALL_DONE >> run.log
