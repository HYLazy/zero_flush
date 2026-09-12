#!/bin/bash
# 50GB / 1KB 对比：ZeroFlush（merge 关闭，governance 默认关）vs 原生 RocksDB
# 协议与 cmp_1kb50 历史一致：16 线程、1KB 值、no compression、cache 512MB、
# bg=24、subcompactions=16、num=48,000,000、readrandom 8,000,000 reads、
# histogram 延迟统计；fill 与 read 分两次调用（read 用 use_existing_db）。
set -x
BASE=/home/embed/hyl/metadata_offload
ZF_BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
NAT_BIN=$BASE/source/rocksdb/build/db_bench
COMMON="--key_size=16 --value_size=1024 --num=48000000 --threads=16 \
--compression_type=none --cache_size=536870912 --max_background_jobs=24 \
--subcompactions=16 --histogram=true --statistics=false --use_existing_db=0"
ZF_FLAGS="--zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64 \
--zf_skip_batching=false --zf_base_merge=false"
COMMON_R="--key_size=16 --value_size=1024 --num=48000000 --threads=16 \
--compression_type=none --cache_size=536870912 --max_background_jobs=24 \
--subcompactions=16 --histogram=true --reads=8000000 --use_existing_db=1"

# ---- ZeroFlush（merge OFF）----
rm -rf $BASE/output/zeroflush_m5_perf/db_zf_50gb
LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu $ZF_BIN --benchmarks=fillrandom $COMMON $ZF_FLAGS \
  --db=$BASE/output/zeroflush_m5_perf/db_zf_50gb > zf_fill.log 2>&1
echo "ZF_FILL_RC=$?"
LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu $ZF_BIN --benchmarks=readrandom $COMMON_R $ZF_FLAGS \
  --db=$BASE/output/zeroflush_m5_perf/db_zf_50gb > zf_read.log 2>&1
echo "ZF_READ_RC=$?"

# ---- 原生 RocksDB ----
rm -rf $BASE/output/zeroflush_m5_perf/db_nat_50gb
LD_LIBRARY_PATH=$BASE/source/rocksdb/build:/usr/lib/x86_64-linux-gnu $NAT_BIN --benchmarks=fillrandom $COMMON \
  --db=$BASE/output/zeroflush_m5_perf/db_nat_50gb > native_fill.log 2>&1
echo "NAT_FILL_RC=$?"
LD_LIBRARY_PATH=$BASE/source/rocksdb/build:/usr/lib/x86_64-linux-gnu $NAT_BIN --benchmarks=readrandom $COMMON_R \
  --db=$BASE/output/zeroflush_m5_perf/db_nat_50gb > native_read.log 2>&1
echo "NAT_READ_RC=$?"
echo "ALL_DONE"
