#!/bin/bash
# 等 native fill 真出结果 → 跑 native read → 跑 ZF 公平版（默认攒批）
set -x
cd "$(dirname "$0")"
for i in $(seq 1 720); do grep -q "fillrandom   :" native_fill.log && break; sleep 30; done
BASE=/home/embed/hyl/metadata_offload
NAT_BIN=$BASE/source/rocksdb/build/db_bench
COMMON_R="--key_size=16 --value_size=1024 --num=48000000 --threads=16 --compression_type=none --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 --histogram=true --reads=8000000 --use_existing_db=1"
LD_LIBRARY_PATH=$BASE/source/rocksdb/build:/usr/lib/x86_64-linux-gnu $NAT_BIN --benchmarks=readrandom $COMMON_R --db=$BASE/output/zeroflush_m5_perf/db_nat_50gb > native_read.log 2>&1
echo "NAT_READ_DONE rc=$?" > native_chain_done.txt
./run_zf_fair.sh > zf_fair_run.log 2>&1
echo "ZF_FAIR_DONE" >> native_chain_done.txt
