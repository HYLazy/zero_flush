#!/bin/bash
set -u
cd /home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/build_rel
export LD_LIBRARY_PATH=$PWD:/usr/lib/x86_64-linux-gnu
export ZF_L0_TRIGGER=100000000
DB=/tmp/zipf_zf
rm -rf "$DB"
/usr/bin/time -v ./zf_zipf_bench \
  --db="$DB" --zeroflush=1 --keyspace=48000000 --ops=48000000 --reads=8000000 \
  --threads=16 --value_size=1024 --zipf_alpha=0.99 --seed=20261017 \
  --partitions=16 --partition_target_mb=64 --epoch_target_mb=256 \
  --materialize_parallelism=16 --cache_size=536870912 \
  --max_background_jobs=24 --subcompactions=16 \
  > /tmp/zipf_zf_run.log 2>/tmp/zipf_zf_progress.log
echo "ZIPF_ZF_EXIT=$?" >> /tmp/zipf_zf_run.log
