#!/bin/bash
# Write-path decomposition runs (see bench/wpbench.cc).
# Machine should be otherwise idle; each run recreates its DB.
set -euo pipefail
ROOT=/home/embed/hyl/metadata_offload
OUT=$ROOT/bench/out/wpbench
BIN=$ROOT/bench/tools/wpbench
DB=$ROOT/bench/db/wpbench
mkdir -p "$OUT"
rm -f "$OUT/wpbench.csv"
export LD_LIBRARY_PATH=$ROOT/source/rocksdb/build

run() {  # run <tag> <args...>
  local tag=$1; shift
  "$BIN" --db="$DB" --out="$OUT/wpbench" --tag="$tag" "$@"
}

echo "===== A. single-thread component split ====="
run gen_only                     --mode=gen --ops=4000000
run put_nogen_quiet              --mode=put_nogen --ops=4000000 --quiet=1
run put_quiet_wal                --mode=put --ops=4000000 --quiet=1
run put_quiet_nowal              --mode=put --ops=4000000 --quiet=1 --wal=0
run put_default_1t               --mode=put --ops=2000000
run batch8_quiet                 --mode=batch --batch=8 --ops=4000000 --quiet=1
run put_quiet_wal_perf           --mode=put --ops=4000000 --quiet=1 --perf=1
run put_default_1t_perf          --mode=put --ops=2000000 --perf=1

echo "===== B. thread sweep (quiet: write path only, no stalls) ====="
for T in 2 4 8 16 32; do
  run put_quiet_t$T --mode=put --ops=2000000 --threads=$T --quiet=1 --perf=1
done
run put_quiet_t16_noadapt --mode=put --ops=2000000 --threads=16 --quiet=1 --perf=1 --adaptive_yield=0

echo "===== C. default options (flush/compaction/stall active) ====="
run put_default_t16_perf      --mode=put --ops=2000000 --threads=16 --perf=1
run put_default_t16_noadapt   --mode=put --ops=2000000 --threads=16 --perf=1 --adaptive_yield=0
run put_default_t16_noperf    --mode=put --ops=2000000 --threads=16
run put_default_t16_noadapt_noperf --mode=put --ops=2000000 --threads=16 --adaptive_yield=0

echo "===== done: $OUT/wpbench.csv ====="
