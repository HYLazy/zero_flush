#!/bin/bash
# 50GB merge-ON 验收（cf5608e，锁默认开启）
# 配置：16T / 1KB / 48M 键空间 / 16 分区 / sampled / merge ON / governance OFF
#       / compression=none / bg=24 / subcompactions=16 / seed 固定
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/merge_on_accept_v2
BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb-zeroflush/build_rel:/usr/lib/x86_64-linux-gnu
DB=$RUN/db
rm -rf "$DB"
echo "start $(date '+%F %T')" > run.log
# 前台阶段：48M 唯一键 = 48GB 用户数据（--num 为每线程 op 数）
ZF_DBG_MM=1 ZF_TRACE_IMM=1 ZF_L0_TRIGGER=100000000 $BIN \
  --benchmarks=fillrandom --key_size=16 --value_size=1024 --num=3000000 \
  --threads=16 --seed=20260914 --compression_type=none --cache_size=536870912 \
  --max_background_jobs=24 --subcompactions=16 --histogram=true \
  --statistics=false --use_existing_db=0 \
  --zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64 \
  --zf_base_merge=true --db="$DB" > fill.log 2>&1
echo "fill_rc=$? fill_end=$(date '+%F %T')" >> run.log
cp "$DB/LOG" fill.LOG 2>/dev/null
# 排空/校验阶段：8M 随机读（进程存活期间后台继续推进；同时校验数据完整）
$BIN --benchmarks=readrandom --key_size=16 --value_size=1024 --num=3000000 \
  --threads=16 --seed=20260914 --compression_type=none --cache_size=536870912 \
  --max_background_jobs=24 --subcompactions=16 --histogram=true \
  --statistics=false --use_existing_db=1 --reads=8000000 \
  --zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64 \
  --zf_base_merge=true --db="$DB" > read.log 2>&1
echo "read_rc=$? read_end=$(date '+%F %T')" >> run.log
cp "$DB/LOG" read.LOG 2>/dev/null
du -sh "$DB" >> run.log
for ext in sst log; do n=$(find "$DB" -name "*.$ext" | wc -l); s=$(find "$DB" -name "*.$ext" -printf "%s\n" | awk '{a+=$1} END {printf "%.2f", a/1073741824}'); echo "$ext n=$n size=${s}GB" >> run.log; done
echo ALL_DONE >> run.log
