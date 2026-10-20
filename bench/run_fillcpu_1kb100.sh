#!/bin/bash
# 1KB-KV / 100GB workload on DEFAULT RocksDB configuration.
#
# Answers: how much CPU does flush use, what is the CPU share of each
# operation, how much memory is used and how is it split across engine
# components.  Driver: bench/tools/fillcpu (bench/fillcpu.cc); the driver sets
# no RocksDB option except create_if_missing, so this is the library default
# configuration (RocksDB 11.2.0, no compression library linked -> defaults
# resolve to no compression).
#
# 96,000,000 keys x (16B key + 1024B value) = 99.84 GB of application KV
# -- same convention as the existing 1KB/100GB experiments (cmp_1kb100).
set -euo pipefail
ROOT=/home/embed/hyl/metadata_offload
OUTDIR=$ROOT/bench/out/fillcpu_1kb100_default
DB=$ROOT/bench/db/fillcpu_1kb100_default
mkdir -p "$OUTDIR"
cd "$ROOT"

LD_LIBRARY_PATH=$ROOT/source/rocksdb/build \
  "$ROOT/bench/tools/fillcpu" \
  --db="$DB" \
  --out="$OUTDIR/run" \
  --num_ops=96000000 \
  --universe=96000000 \
  --threads=16 \
  --value_size=1024 \
  --sample_ms=250 \
  --progress_ms=15000 \
  --drain_max_s=3600 \
  --disk_guard_gb=80 \
  --destroy_first=1 \
  > "$OUTDIR/run.log" 2>&1
echo "exit=$?"
