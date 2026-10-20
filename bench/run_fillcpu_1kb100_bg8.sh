#!/bin/bash
# 1KB-KV / 100GB workload with max_background_jobs=8 (2 flush + 6 compaction threads).
# Everything else is the library default -- same command shape as
# bench/run_fillcpu_1kb100.sh so the two runs are directly comparable.
set -euo pipefail
ROOT=/home/embed/hyl/metadata_offload
OUTDIR=$ROOT/bench/out/fillcpu_1kb100_bg8
DB=$ROOT/bench/db/fillcpu_1kb100_bg8
mkdir -p "$OUTDIR"
cd "$ROOT"

# resource timeline (load + device IO) alongside the run
(
  while pgrep -f "fillcpu --db=$DB" >/dev/null; do
    echo "$(date +%s) load=$(cut -d' ' -f1 /proc/loadavg) $(awk '/nvme0n1 /{print "rd_sect="$6" wr_sect="$10}' /proc/diskstats) $(awk '/^MemAvailable/{print "memavail_kb="$2}' /proc/meminfo)"
    sleep 10
  done
) > "$OUTDIR/resources.log" 2>&1 &
RESMON=$!

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
  --bg_jobs=8 \
  > "$OUTDIR/run.log" 2>&1
kill $RESMON 2>/dev/null || true
echo "exit=$?"
