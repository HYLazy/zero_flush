#!/bin/bash
# 100GB / 1KB, default config (max_background_jobs=2), enriched per-compaction I/O
# instrumentation: iostats deltas (read/write/fsync nanos), CompactionJobStats,
# periodic cfstats snapshots.  Used to decompose "who the writers wait for".
set -euo pipefail
ROOT=/home/embed/hyl/metadata_offload
OUTDIR=$ROOT/bench/out/fillcpu_1kb100_src2
DB=$ROOT/bench/db/fillcpu_1kb100_src2
mkdir -p "$OUTDIR"
cd "$ROOT"
LD_LIBRARY_PATH=$ROOT/source/rocksdb/build \
  "$ROOT/bench/tools/fillcpu" \
  --db="$DB" --out="$OUTDIR/run" \
  --num_ops=96000000 --universe=96000000 --threads=16 --value_size=1024 \
  --sample_ms=250 --progress_ms=15000 --drain_max_s=3600 --disk_guard_gb=80 \
  --destroy_first=1 > "$OUTDIR/run.log" 2>&1
echo "exit=$?"
