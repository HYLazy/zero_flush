#!/bin/bash
# 50GB merge-ON 验收（zf_install_mu_ 修复后，2026-09-12）
# 参数与 09-02 cmp_1kb50 基线同构：16T / 1KB / sampled / 16 分区 / base_merge
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/merge_on_accept
BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb-zeroflush/build_rel:/usr/lib/x86_64-linux-gnu
DB=$RUN/db_zf_merge_on
rm -rf "$DB"
echo "start $(date '+%F %T')" > run.log
ZF_L0_TRIGGER=100000000 $BIN \
  --benchmarks=fillrandom --key_size=16 --value_size=1024 --num=3000000 \
  --threads=16 --compression_type=none --cache_size=536870912 \
  --max_background_jobs=24 --subcompactions=16 --histogram=true \
  --statistics=false --use_existing_db=0 \
  --zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64 \
  --zf_base_merge=true \
  --db="$DB" > zf_fill.log 2>&1
echo "fill_rc=$? end $(date '+%F %T')" >> run.log
cp "$DB/LOG" zf_fill.LOG 2>/dev/null
$BIN --benchmarks=readrandom --key_size=16 --value_size=1024 --num=3000000 \
  --threads=16 --compression_type=none --cache_size=536870912 \
  --max_background_jobs=24 --subcompactions=16 --histogram=true \
  --statistics=false --use_existing_db=1 --reads=8000000 \
  --zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64 \
  --zf_base_merge=true --db="$DB" > zf_read.log 2>&1
echo "read_rc=$? end $(date '+%F %T')" >> run.log
echo ALL_DONE >> run.log
