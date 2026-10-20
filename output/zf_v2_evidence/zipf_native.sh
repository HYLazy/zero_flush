#!/bin/bash
set -u
export LD_LIBRARY_PATH=/home/embed/hyl/metadata_offload/source/rocksdb/build:/usr/lib/x86_64-linux-gnu
DB=/tmp/zipf_native
rm -rf "$DB"
/usr/bin/time -v /tmp/native_zipf_bench \
  --db="$DB" --keyspace=48000000 --ops=48000000 --reads=8000000 \
  --threads=16 --value_size=1024 --zipf_alpha=0.99 --seed=20261017 \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  > /tmp/zipf_native_run.log 2>&1
echo "ZIPF_NATIVE_EXIT=$?" >> /tmp/zipf_native_run.log
