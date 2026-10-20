#!/bin/bash
# ZF merge-off 公平版：默认攒批（skip_batching=true 默认，不显式关闭）、
# merge 关闭（zf_base_merge=false 默认，不显式开启）、治理默认关。
set -x
BASE=/home/embed/hyl/metadata_offload
ZF_BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
DB=$BASE/output/zeroflush_m5_perf/db_zf_fair_50gb
COMMON="--key_size=16 --value_size=1024 --num=48000000 --threads=16 \
--compression_type=none --cache_size=536870912 --max_background_jobs=24 \
--subcompactions=16 --histogram=true --use_existing_db=0"
ZF_FLAGS="--zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64"
COMMON_R="--key_size=16 --value_size=1024 --num=48000000 --threads=16 \
--compression_type=none --cache_size=536870912 --max_background_jobs=24 \
--subcompactions=16 --histogram=true --reads=8000000 --use_existing_db=1"

rm -rf $DB
LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu $ZF_BIN --benchmarks=fillrandom $COMMON $ZF_FLAGS --db=$DB > zf_fair_fill.log 2>&1
echo "ZF_FAIR_FILL_RC=$?" >> zf_fair_run.log
LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu $ZF_BIN --benchmarks=readrandom $COMMON_R $ZF_FLAGS --db=$DB > zf_fair_read.log 2>&1
echo "ZF_FAIR_READ_RC=$?" >> zf_fair_run.log
echo "ZF_FAIR_ALL_DONE" >> zf_fair_run.log
