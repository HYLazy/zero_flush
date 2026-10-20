#!/bin/bash
# native 同窗对照（与 merge_on_accept_v2 同参数、同窗口、同盘）
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/native_50gb_samewindow
BIN=$BASE/source/rocksdb/build/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb/build:/usr/lib/x86_64-linux-gnu
DB=$RUN/db_repeat
rm -rf "$DB"
echo "start $(date '+%F %T')" > run.log
$BIN --benchmarks=fillrandom --key_size=16 --value_size=1024 --num=48000000 \
  --writes=3000000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=0 \
  --db="$DB" > fill.log 2>&1
echo "fill_rc=$? fill_end=$(date '+%F %T')" >> run.log
cp "$DB/LOG" fill.LOG 2>/dev/null
$BIN --benchmarks=readrandom --key_size=16 --value_size=1024 --num=48000000 \
  --reads=8000000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=1 \
  --db="$DB" > read.log 2>&1
echo "read_rc=$? read_end=$(date '+%F %T')" >> run.log
cp "$DB/LOG" read.LOG 2>/dev/null
du -sh "$DB" >> run.log
for ext in sst log; do n=$(find "$DB" -name "*.$ext" | wc -l); s=$(find "$DB" -name "*.$ext" -printf "%s\n" | awk '{a+=$1} END {printf "%.2f", a/1073741824}'); echo "$ext n=$n size=${s}GB" >> run.log; done
echo ALL_DONE >> run.log
