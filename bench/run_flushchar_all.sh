#!/bin/bash
# FLUSHCHAR campaign orchestrator -- runs probes + R1..R8 strictly sequentially
# (single shared NVMe device: concurrent runs would contaminate each other).
# See bench/FLUSHCHAR_PLAN.md for what each run decides.
set -uo pipefail
ROOT=/home/embed/hyl/metadata_offload
cd "$ROOT"
LOG=bench/out/flushchar/campaign.log
mkdir -p bench/out/flushchar
exec >>"$LOG" 2>&1
echo "############ FLUSHCHAR campaign start $(date -Is) load=$(cut -d' ' -f1 /proc/loadavg) free=$(df -BG --output=avail /home | tail -1)"

step() { echo; echo "######## [$1] $(date -Is) ########"; }

step "probes C0/C1"
bash bench/run_flushchar_probes.sh

step "R1 flush-only default (wb=64MB, 1 flush thread), 20GB"
bash bench/run_flushchar.sh r1_flushonly --disable_compactions=1

step "R2 flush-only 2 flush threads"
bash bench/run_flushchar.sh r2_flt2 --disable_compactions=1 --max_bg_flushes=2

step "R3 flush-only 4 flush threads"
bash bench/run_flushchar.sh r3_flt4 --disable_compactions=1 --max_bg_flushes=4

step "R4 flush-only write_buffer=16MB, 10GB"
FC_OPS=9600000 FC_UNIVERSE=9600000 bash bench/run_flushchar.sh r4_wb16 --disable_compactions=1 --write_buffer_size=16m

step "R5 flush-only write_buffer=256MB"
bash bench/run_flushchar.sh r5_wb256 --disable_compactions=1 --write_buffer_size=256m

step "R6 flush-only direct IO for flush output"
bash bench/run_flushchar.sh r6_directio --disable_compactions=1 --direct_io=1

# --- R7: bandwidth-starvation perturbation. Calibrate a dd hog that eats
# roughly half of the sequential-write ceiling, then rerun R1 under it.
step "R7 hog calibration (dd O_DIRECT, candidate block sizes, 8 s each)"
mkdir -p bench/out/flushchar/r7_hog
for BS in 64k 128k 256k; do
  echo "--- bs=$BS ---"
  timeout -s INT 8 dd if=/dev/zero of=bench/out/flushchar/r7_hog/hog.bin bs=$BS oflag=direct 2>&1 | tail -1 || true
done
rm -f bench/out/flushchar/r7_hog/hog.bin

step "R7 flush-only + concurrent dd hog (bs=128k), 20GB"
HOG=bench/out/flushchar/r7_hog/hog.bin
( timeout -s INT 600 dd if=/dev/zero of=$HOG bs=128k oflag=direct 2> bench/out/flushchar/r7_hog/hog_dd.log || true ) &
HOGPID=$!
bash bench/run_flushchar.sh r7_hog --disable_compactions=1
kill -INT $HOGPID 2>/dev/null || true
wait $HOGPID 2>/dev/null || true
rm -f $HOG
echo "hog dd:"; cat bench/out/flushchar/r7_hog/hog_dd.log

step "R8 strace syscall-count probe (2GB flush-only)"
OUT=bench/out/flushchar/r8_strace
mkdir -p $OUT
strace -f -c -e trace=fdatasync,fsync,sync_file_range,write,pwrite64,pread64 \
  -o $OUT/strace_summary.txt \
  env LD_LIBRARY_PATH=$ROOT/source/rocksdb/build \
  bench/tools/fillcpu \
  --db=$ROOT/bench/db/fc_r8 --out=$OUT/run \
  --num_ops=1920000 --universe=1920000 --threads=16 --value_size=1024 \
  --sample_ms=250 --progress_ms=10000 --drain_max_s=600 \
  --disk_guard_gb=40 --destroy_first=1 --disable_compactions=1 \
  > $OUT/run.log 2>&1 || true
rm -rf $ROOT/bench/db/fc_r8
cat $OUT/strace_summary.txt || true

step "R1 repeat (variance check)"
bash bench/run_flushchar.sh r1b_flushonly --disable_compactions=1

echo "############ FLUSHCHAR campaign done $(date -Is) free=$(df -BG --output=avail /home | tail -1)"
