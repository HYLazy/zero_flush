#!/bin/bash
# resbudget -- fixed-resource comparison: standard LSM path vs WAL-only ingest
# followed by deferred materialization.
#
#   bash bench/run_resbudget.sh A     # standard RocksDB fill (WAL+flush+compaction)
#   bash bench/run_resbudget.sh B1    # WAL-equivalent sequential ingest only
#   bash bench/run_resbudget.sh B2    # deferred materialization replay (no WAL)
#   bash bench/run_resbudget.sh B     # B1 then B2, total = both
#   bash bench/run_resbudget.sh smoke # 2M-op smoke of B1/B2
#
# Caps (overridable): write 200 MB/s, read 400 MB/s, CPUs 0-7 (8 physical cores).
# The host does not delegate cgroup v2 io/cpu, so the bandwidth cap is enforced
# by bench/tools/libiothrottle.so across processes sharing one POSIX shm bucket.
set -euo pipefail

ROOT=/home/embed/hyl/metadata_offload
OUT=$ROOT/bench/out/resbudget
DBROOT=$ROOT/bench/db
RS=$ROOT/source/rocksdb
CAPW=${CAPW:-200000000}
CAPR=${CAPR:-400000000}
CPUS=${CPUS:-0-7}
THROTTLE=$ROOT/bench/tools/libiothrottle.so
CAPPED=$ROOT/bench/run_capped.sh
FILLCPU=$ROOT/bench/tools/fillcpu
WALWRITER=$ROOT/bench/tools/walwriter
SAMPLER=$ROOT/bench/resbudget_sampler.py
# 50 GiB of 16B key + 1KB value records.
NUM_OPS=${NUM_OPS:-51622203}
UNIVERSE=${UNIVERSE:-51622203}
VALUE=${VALUE:-1024}
BG=${BG:-6}
SUB=${SUB:-4}
WB=${WB:-256M}
DRAIN=${DRAIN:-900}
DISK_GUARD=${DISK_GUARD:-100}
SAMPLE_MS=${SAMPLE_MS:-1000}
PROGRESS_MS=${PROGRESS_MS:-30000}
SYNC_SETTLE=${SYNC_SETTLE:-10}
ARM=${1:-}

mkdir -p "$OUT"
SHM="/iot_resbudget_$$_${ARM}"
rm -f "/dev/shm/$SHM"
cleanup() { rm -f "/dev/shm/$SHM"; }
trap cleanup EXIT
export SHM CAPW CAPR CPUS
export IOTHROTTLE_ROOT="$OUT"

meta() {
  local d=$1
  mkdir -p "$d"
  {
    echo "date=$(date -Is)"
    echo "arm_$ARM=1"
    echo "cap_write_bytes_s=$CAPW cap_read_bytes_s=$CAPR cpus=$CPUS"
    echo "num_ops=$NUM_OPS universe=$UNIVERSE value=$VALUE bg=$BG sub=$SUB wb=$WB"
    echo "git=$(git -C "$ROOT" rev-parse --short HEAD)"
    echo "fillcpu_md5=$(md5sum "$FILLCPU" | cut -d' ' -f1)"
    echo "throttle_md5=$(md5sum "$THROTTLE" | cut -d' ' -f1)"
    echo "walwriter_md5=$(md5sum "$WALWRITER" | cut -d' ' -f1)"
    echo "host=$(hostname)"
    echo "--- df ---"; df -h "$ROOT"
    echo "--- free ---"; free -h
    echo "--- loadavg ---"; cat /proc/loadavg
  } > "$d/meta.txt"
  printf '%s\n' "$*" > "$d/cmd.txt"
}

SAMPLER_PID=0
start_sampler() {
  local out=$1 pids=$2 stop=$3
  rm -f "$stop"
  python3 "$SAMPLER" --out "$out" --pids "$pids" --interval 1.0 --stop-file "$stop" \
    >/dev/null 2>&1 &
  SAMPLER_PID=$!
}

stop_sampler() {
  local spid=$1 stop=$2
  touch "$stop"
  wait "$spid" || true
}

run_a() {
  local d=$OUT/$ARM
  local db=$DBROOT/resbudget_$ARM
  mkdir -p "$d"
  rm -rf "$db"
  meta "$d"
  sync; sleep "$SYNC_SETTLE"
  local t0=$(date +%s)
  "$CAPPED" "$FILLCPU" --db="$db" --out="$d/fill" \
    --num_ops="$NUM_OPS" --universe="$UNIVERSE" --threads=16 --value_size="$VALUE" \
    --sample_ms="$SAMPLE_MS" --progress_ms="$PROGRESS_MS" --drain_max_s="$DRAIN" \
    --disk_guard_gb="$DISK_GUARD" --destroy_first=1 \
    --bg_jobs="$BG" --subcomp="$SUB" --write_buffer_size="$WB" --direct_io=1 \
    > "$d/run.log" 2>&1 &
  local p=$!
  start_sampler "$d/samples.csv" "$p" "$d/.stop"
  local sp=$SAMPLER_PID
  local rc=0; wait "$p" || rc=$?
  stop_sampler "$sp" "$d/.stop"
  local t1=$(date +%s)
  echo "arm_wall_s=$((t1-t0)) rc=$rc" | tee "$d/done.txt"
  df -h "$ROOT" >> "$d/done.txt"
  return $rc
}

run_wal() {
  local d=$1 bytes=$2 rate=${3:-0} wal=$4
  mkdir -p "$d"
  mkdir -p "$(dirname "$wal")"
  sync; sleep "$SYNC_SETTLE"
  local t0=$(date +%s)
  "$CAPPED" "$WALWRITER" --path="$wal" --bytes="$bytes" --rate_mb_s="$rate" \
    --progress_ms=10000 > "$d/wal.log" 2>&1 &
  local p=$!
  start_sampler "$d/samples.csv" "$p" "$d/.stop"
  local sp=$SAMPLER_PID
  local rc=0; wait "$p" || rc=$?
  stop_sampler "$sp" "$d/.stop"
  local t1=$(date +%s)
  echo "wal_wall_s=$((t1-t0)) rc=$rc" | tee "$d/done.txt"
  df -h "$ROOT" >> "$d/done.txt"
  return $rc
}

run_b1() {
  local d=$OUT/B/ingest
  mkdir -p "$d"
  meta "$d"
  local bytes=$((NUM_OPS * (16 + VALUE)))
  run_wal "$d" "$bytes" 0 "$OUT/B/wal.bin"
}

run_b2() {
  local d=$OUT/B/replay
  local db=$DBROOT/resbudget_B
  local wal=$OUT/B/wal.bin
  mkdir -p "$d"
  rm -rf "$db"
  meta "$d"
  sync; sleep "$SYNC_SETTLE"
  local t0=$(date +%s)
  "$CAPPED" "$FILLCPU" --db="$db" --out="$d/fill" \
    --num_ops="$NUM_OPS" --universe="$UNIVERSE" --threads=16 --value_size="$VALUE" \
    --sample_ms="$SAMPLE_MS" --progress_ms="$PROGRESS_MS" --drain_max_s="$DRAIN" \
    --disk_guard_gb="$DISK_GUARD" --destroy_first=1 --disable_wal=1 \
    --bg_jobs="$BG" --subcomp="$SUB" --write_buffer_size="$WB" --direct_io=1 \
    > "$d/run.log" 2>&1 &
  local p1=$!
  # model the sequential WAL scan a deferred materializer has to perform
  local p2=0
  if [ -f "$wal" ]; then
    "$CAPPED" dd if="$wal" of=/dev/null bs=1M iflag=direct status=none \
        > "$d/wal_read.log" 2>&1 &
    p2=$!
  fi
  start_sampler "$d/samples.csv" "$p1,$p2" "$d/.stop"
  local sp=$SAMPLER_PID
  local rc=0; wait "$p1" || rc=$?
  [ "$p2" -ne 0 ] && wait "$p2" || true
  stop_sampler "$sp" "$d/.stop"
  local t1=$(date +%s)
  echo "replay_wall_s=$((t1-t0)) rc=$rc" | tee "$d/done.txt"
  df -h "$ROOT" >> "$d/done.txt"
  return $rc
}


# B-pipe: WAL ingest and deferred materialization overlap.  The WAL writer is
# paced to WPIPE_RATE MB/s, leaving the rest of the 200 MB/s write budget for
# the replay (which shares one token bucket with the WAL writer).  This is the
# direct test of "let the flush use the leftover resources".
run_bpipe() {
  local d=$OUT/Bpipe
  local db=$DBROOT/resbudget_Bpipe
  local wal=$d/wal.bin
  local rate=${WPIPE_RATE:-80}
  local head_gb=${WPIPE_HEAD_GB:-5}
  local bytes=$((NUM_OPS * (16 + VALUE)))
  local head_bytes=$((head_gb * 1024 * 1024 * 1024))
  mkdir -p "$d/ingest" "$d/replay"
  rm -rf "$db" "$wal"
  meta "$d"
  sync; sleep "$SYNC_SETTLE"
  local t0=$(date +%s)
  "$CAPPED" "$WALWRITER" --path="$wal" --bytes="$bytes" --rate_mb_s="$rate" \
    --progress_ms=10000 > "$d/ingest/wal.log" 2>&1 &
  local p1=$!
  while true; do
    local sz=$(stat -c%s "$wal" 2>/dev/null || echo 0)
    [ "$sz" -ge "$head_bytes" ] && break
    kill -0 "$p1" 2>/dev/null || break
    sleep 2
  done
  "$CAPPED" "$FILLCPU" --db="$db" --out="$d/replay/fill" \
    --num_ops="$NUM_OPS" --universe="$UNIVERSE" --threads=16 --value_size="$VALUE" \
    --sample_ms="$SAMPLE_MS" --progress_ms="$PROGRESS_MS" --drain_max_s="$DRAIN" \
    --disk_guard_gb="$DISK_GUARD" --destroy_first=1 --disable_wal=1 \
    --bg_jobs="$BG" --subcomp="$SUB" --write_buffer_size="$WB" --direct_io=1 \
    > "$d/replay/run.log" 2>&1 &
  local p2=$!
  start_sampler "$d/samples.csv" "$p1,$p2" "$d/.stop"
  local sp=$SAMPLER_PID
  local rc1=0 rc2=0
  wait "$p1" || rc1=$?
  wait "$p2" || rc2=$?
  stop_sampler "$sp" "$d/.stop"
  local t1=$(date +%s)
  echo "pipe_wall_s=$((t1-t0)) rc_wal=$rc1 rc_replay=$rc2 wal_rate_mb_s=$rate head_gb=$head_gb" \
    | tee "$d/done.txt"
  df -h "$ROOT" >> "$d/done.txt"
  return $((rc1 + rc2))
}

run_smoke() {
  local n=2000000
  local d=$OUT/smoke
  mkdir -p "$d"
  meta "$d"
  # WAL 2 GiB
  run_wal "$d/wal" 2147483648 0 "$d/wal.bin"
  # replay without WAL
  local db=$DBROOT/resbudget_smoke
  rm -rf "$db"
  "$CAPPED" "$FILLCPU" --db="$db" --out="$d/replay" \
    --num_ops=$n --universe=$n --threads=16 --value_size="$VALUE" \
    --sample_ms=500 --progress_ms=5000 --drain_max_s=120 --disk_guard_gb=60 \
    --destroy_first=1 --disable_wal=1 --bg_jobs="$BG" --subcomp="$SUB" \
    --write_buffer_size="$WB" --direct_io=1 > "$d/replay.log" 2>&1
}

case "$ARM" in
  A|A1|A2) run_a ;;
  B1) run_b1 ;;
  B2) run_b2 ;;
  B) run_b1; run_b2 ;;
  Bpipe|BPIPE) run_bpipe ;;
  smoke) run_smoke ;;
  *) echo "usage: $0 {A|A1|A2|B1|B2|B|Bpipe|smoke}" >&2; exit 2 ;;
esac
