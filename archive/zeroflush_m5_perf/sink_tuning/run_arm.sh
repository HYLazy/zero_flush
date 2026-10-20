#!/bin/bash
# 受控对照单臂：$1=arm 名，$2=env 前缀（如 "ZF_SINK_WHOLE_RANGE=1 "）
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/sink_tuning
BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb-zeroflush/build_rel:/usr/lib/x86_64-linux-gnu
ARM=$1; ENVS=${2:-}
DB=$RUN/db_$ARM
rm -rf "$DB"
echo "[$ARM] env='$ENVS' start $(date '+%T')"
# step 0b-1：写量结束后追加排空阶段（waitforcompaction = DB::WaitForCompact：
# 等 native 后台任务与 imm 排空），使"总输出/末态"可跨运行比较；排空耗时由
# db_bench 的 waitforcompaction: started/finished 行 + DB LOG 的 ZeroFlush
# drain 行给出。
env $ENVS $BIN --benchmarks=fillrandom,waitforcompaction --key_size=16 --value_size=1024 \
  --num=48000000 --writes=1000000 --threads=16 --seed=20260914 \
  --compression_type=none --cache_size=536870912 --max_background_jobs=24 \
  --subcompactions=16 --statistics=false --use_existing_db=0 \
  --zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64 \
  --zf_base_merge=true --db="$DB" > fill_$ARM.log 2>&1
echo "[$ARM] rc=$? end $(date '+%T')"
cp "$DB/LOG" fill_$ARM.LOG 2>/dev/null
