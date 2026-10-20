#!/bin/bash
# FLUSHCHAR campaign -- one fillcpu run with the campaign-common parameters.
#   usage: run_flushchar.sh <run_name> [extra fillcpu flags...]
# 16B key + 1KB value, 16 writer threads, 250 ms sampling, drain to quiesce,
# fresh DB destroyed first and deleted after (space-constrained disk).
# The DB lives on the measured device (nvme0n1p3, same as every fillcpu run).
set -euo pipefail
ROOT=/home/embed/hyl/metadata_offload
NAME=$1; shift
OUT=$ROOT/bench/out/flushchar/$NAME
DB=$ROOT/bench/db/fc_$NAME
OPS=${FC_OPS:-19200000}          # 20 GB of application bytes
UNIVERSE=${FC_UNIVERSE:-19200000}
mkdir -p "$OUT"
cd "$ROOT"

echo "=== run $NAME args: $* ops=$OPS ==="
LD_LIBRARY_PATH=$ROOT/source/rocksdb/build \
  "$ROOT/bench/tools/fillcpu" \
  --db="$DB" --out="$OUT/run" \
  --num_ops=$OPS --universe=$UNIVERSE --threads=16 --value_size=1024 \
  --sample_ms=250 --progress_ms=10000 \
  --drain_max_s=1800 --disk_guard_gb=40 --destroy_first=1 \
  "$@" > "$OUT/run.log" 2>&1
rc=$?
tail -3 "$OUT/run.log" || true
if [ "${KEEP_DB:-0}" != "1" ]; then rm -rf "$DB"; fi
echo "=== run $NAME exit=$rc (db kept: ${KEEP_DB:-0}) free=$(df -BG --output=avail /home | tail -1)"
exit $rc
