#!/bin/bash
# FLUSHCHAR campaign -- device/compute/sync reference probes (C0/C1).
# Ceilings are measured in the SAME session as the R-runs: the disk state
# (93% full, temperature) drifts between sessions, so old denominators
# must not be reused (see bench/out/pcie_bw/REPORT.md section 5).
set -euo pipefail
ROOT=/home/embed/hyl/metadata_offload
OUT=$ROOT/bench/out/flushchar/probes
mkdir -p "$OUT"
cd "$ROOT"
LOG=$OUT/probes.log
{
echo "=== $(date -Is) host=$(hostname) load=$(cut -d' ' -f1 /proc/loadavg) ==="
echo "--- disk free before: $(df -BG --output=avail /home | tail -1) ---"

echo "=== C0a: sequential write ceiling (dd O_DIRECT 1MiB x 16GiB) ==="
dd if=/dev/zero of=$OUT/seq16.bin bs=1M count=16384 oflag=direct 2>&1 | tail -1

echo "=== C0b: sequential read ceiling (dd O_DIRECT 1MiB x 16GiB) ==="
dd if=$OUT/seq16.bin of=/dev/null bs=1M iflag=direct 2>&1 | tail -1

echo "=== C0c: random 4K write (devbench 16GiB file, 16 threads, 30 s) ==="
$ROOT/bench/tools/devbench $OUT/seq16.bin 16 16 0 30 2>&1 | tail -3

echo "=== C0d: random 4K read (devbench, 30 s) ==="
$ROOT/bench/tools/devbench $OUT/seq16.bin 16 16 100 30 2>&1 | tail -3

rm -f $OUT/seq16.bin
echo "--- disk free after: $(df -BG --output=avail /home | tail -1) ---"

echo "=== C1a: fdatasync floor, 64 MiB payload (default write buffer) x20 ==="
LD_LIBRARY_PATH=$ROOT/source/rocksdb/build $ROOT/bench/tools/fillmicro \
  fdatasync $OUT/sync64.bin $((64*1024*1024)) 20

echo "=== C1b: fdatasync floor, 16 MiB payload x20 ==="
LD_LIBRARY_PATH=$ROOT/source/rocksdb/build $ROOT/bench/tools/fillmicro \
  fdatasync $OUT/sync16.bin $((16*1024*1024)) 20

echo "=== C1c: fdatasync floor, 1 MiB payload x50 (fixed cost) ==="
LD_LIBRARY_PATH=$ROOT/source/rocksdb/build $ROOT/bench/tools/fillmicro \
  fdatasync $OUT/sync1.bin $((1024*1024)) 50

echo "=== C1d: single-core stream rate (memcpy+XXH3, 64MiB source) ==="
LD_LIBRARY_PATH=$ROOT/source/rocksdb/build $ROOT/bench/tools/fillmicro stream

echo "=== done $(date -Is) ==="
} 2>&1 | tee "$LOG"
