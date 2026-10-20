#!/bin/bash
# 50GB merge-ON 验收（cf5608e+6dc6449，锁默认开启）
# --num = 键空间上限（48M → 活数据 ~30GB）；--writes/--reads = 每线程操作数
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/zf_50gb_noprobe
BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb-zeroflush/build_rel:/usr/lib/x86_64-linux-gnu
DB=$RUN/db
rm -rf "$DB"
echo "start $(date '+%F %T')" > run.log
ZF_L0_TRIGGER=100000000 $BIN \
  --benchmarks=fillrandom --key_size=16 --value_size=1024 --num=48000000 \
  --writes=3000000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=0 \
  --zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64 \
  --zf_base_merge=true --db="$DB" > fill.log 2>&1
echo "fill_rc=$? fill_end=$(date '+%F %T')" >> run.log
cp "$DB/LOG" fill.LOG 2>/dev/null
$BIN --benchmarks=readrandom --key_size=16 --value_size=1024 --num=48000000 \
  --reads=8000000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=1 \
  --zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64 \
  --zf_base_merge=true --db="$DB" > read.log 2>&1
echo "read_rc=$? read_end=$(date '+%F %T')" >> run.log
cp "$DB/LOG" read.LOG 2>/dev/null
du -sh "$DB" >> run.log
for ext in sst log; do n=$(find "$DB" -name "*.$ext" | wc -l); s=$(find "$DB" -name "*.$ext" -printf "%s\n" | awk '{a+=$1} END {printf "%.2f", a/1073741824}'); echo "$ext n=$n size=${s}GB" >> run.log; done
echo ALL_DONE >> run.log
