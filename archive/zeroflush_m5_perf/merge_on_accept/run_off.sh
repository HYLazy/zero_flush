#!/bin/bash
# 对照组：锁关闭（ZF_INSTALL_LOCK=0）的 50GB merge-ON fill，与锁开启臂同参数
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/merge_on_accept
BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb-zeroflush/build_rel:/usr/lib/x86_64-linux-gnu
DB=$RUN/db_zf_lockoff
rm -rf "$DB"
ZF_INSTALL_LOCK=0 ZF_L0_TRIGGER=100000000 $BIN \
  --benchmarks=fillrandom --key_size=16 --value_size=1024 --num=3000000 \
  --threads=16 --compression_type=none --cache_size=536870912 \
  --max_background_jobs=24 --subcompactions=16 --histogram=true \
  --statistics=false --use_existing_db=0 \
  --zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64 \
  --zf_base_merge=true --db="$DB" > "$RUN/zf_fill_lockoff.log" 2>&1
echo "lockoff_fill_rc=$?" >> "$RUN/run_off.log"
cp "$DB/LOG" "$RUN/zf_fill_lockoff.LOG" 2>/dev/null
echo LOCKOFF_DONE >> "$RUN/run_off.log"
