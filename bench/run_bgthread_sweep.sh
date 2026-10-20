#!/bin/bash
# Compaction-thread sweep: how does background compaction/flush thread count
# affect foreground write throughput?  (default options otherwise; 16 writer
# threads; 8M x 1KB = 8.3GB per run.)
#
# For each config: run wpbench under wp_sample.py (per-thread state + utime/stime
# per thread name), print the run summary and the per-role CPU.
set -uo pipefail
ROOT=/home/embed/hyl/metadata_offload
OUT=$ROOT/bench/out/wpbench
mkdir -p "$OUT"
rm -f "$OUT/bgthreads.csv"

for cfg in "bg2_sub1:--bg_jobs=2 --subcomp=1" \
           "bg4_sub1:--bg_jobs=4 --subcomp=1" \
           "bg8_sub1:--bg_jobs=8 --subcomp=1" \
           "bg8_sub4:--bg_jobs=8 --subcomp=4" \
           "bg2_sub4:--bg_jobs=2 --subcomp=4" \
           "bg16_sub4:--bg_jobs=16 --subcomp=4"; do
  name=${cfg%%:*}; args=${cfg#*:}
  echo "================ $name  (16 writers, 8.3GB) ================"
  rm -rf "$ROOT/bench/db/wp_bg"
  python3 "$ROOT/bench/wp_sample.py" "/tmp/bg_$name.log" 400 \
      --db="$ROOT/bench/db/wp_bg" --out="$OUT/bgthreads_sub" --tag="$name" \
      --mode=put --ops=8000000 --threads=16 $args 2>&1 \
    | awk '/^(wall|put|rocksdb|wp-main|wp0|fc)/{print}'
  grep -E "^put" "/tmp/bg_$name.log" | sed "s/^/$name /" >> "$OUT/bgthreads.csv"
done
echo "==== done ===="
cat "$OUT/bgthreads.csv"
