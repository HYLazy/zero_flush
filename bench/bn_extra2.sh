#!/bin/bash
# bn_extra.sh -- post-matrix sensitivity arms (separate file so bn_campaign.sh
# is never edited while executing). Same helpers as bn_campaign.sh.
set -uo pipefail
ROOT=/home/embed/hyl/metadata_offload
BENCH=$ROOT/bench
BIN=$BENCH/tools/l0bench
MKREP=$BENCH/mkreplica.sh
export LD_LIBRARY_PATH=$ROOT/source/rocksdb/build:/usr/lib/x86_64-linux-gnu
THREADS=${THREADS:-16}; WB=${WB:-256M}; CACHE=${CACHE:-8G}
STAGE=I; OUT=$BENCH/out/$STAGE; mkdir -p "$OUT"
BASE=$BENCH/base/base50; U50=48000000; UPDATES=12800000
free_gb() { df -BG --output=avail "$ROOT" | tail -1 | tr -dc '0-9'; }
record() { local rid=$1 extra=$2
  printf '{"stage":"%s","run_id":"%s","ts":"%s","host":"%s","bin_md5":"%s","git_head":"%s",%s}\n' \
    "$STAGE" "$rid" "$(date '+%F %T')" "$(hostname)" \
    "$(md5sum $BIN | cut -d' ' -f1)" "$(git -C $ROOT rev-parse --short HEAD 2>/dev/null)" "$extra" >> "$OUT/manifest.jsonl"; }
done_rc() { grep -F "\"run_id\":\"$1\"" "$OUT/manifest.jsonl" 2>/dev/null | tail -1 | grep -o '"rc":[0-9]*' | cut -d: -f2; }
launch() { local rdir=$1 dbdir=$2 tmo=$3 stall_limit=$4; shift 4; [ "$1" = "--" ] && shift
  local log=$rdir/run.log; mkdir -p "$rdir"
  { echo "### CMD: $BIN $*"; echo "### START $(date '+%F %T') freeGB=$(free_gb)"; } >> "$log"
  setsid nice -n 5 "$BIN" "$@" >> "$log" 2>&1 &
  local pid=$! waited=0 last_ops=-1 last_cpu=-1 stalled=0
  while kill -0 "$pid" 2>/dev/null; do
    sleep 20; waited=$((waited+20))
    local ops cpu fg
    ops=$(grep -o 'PROGRESS [0-9.]*s ops=[0-9]*' "$log" | tail -1 | sed 's/.*ops=//')
    cpu=$(awk '{print $14+$15}' /proc/$pid/stat 2>/dev/null); cpu=${cpu:-0}
    if [ "${ops:-}" = "${last_ops}" ] && [ "$cpu" = "$last_cpu" ]; then stalled=$((stalled+20)); else stalled=0; fi
    last_ops=${ops:-$last_ops}; last_cpu=$cpu
    fg=$(free_gb)
    if [ "$fg" -lt 150 ]; then echo "### ABORT_DISK_LOW freeGB=$fg" >> "$log"; pkill -KILL -s "$pid" 2>/dev/null; return 90; fi
    if [ "$tmo" -gt 0 ] && [ "$waited" -ge "$tmo" ]; then echo "### TIMEOUT" >> "$log"; pkill -KILL -s "$pid" 2>/dev/null; return 92; fi
    if [ $((waited % 300)) -eq 0 ]; then echo "### alive ${waited}s freeGB=$fg ops=${ops:-?}" >> "$log"; fi
  done
  wait "$pid"; local rc=$?; echo "### END $(date '+%F %T') rc=$rc" >> "$log"; return $rc; }
finish_run() { local rdir=$1 dbdir=$2
  [ -f "$dbdir/LOG" ] && cp "$dbdir/LOG" "$rdir/db.LOG"
  [ -d "$dbdir" ] && ls -la "$dbdir" > "$rdir/dir_after.txt" 2>&1
  rm -rf "$dbdir"; }


# devload2: same as devload but with the instrumented devbench (periodic rate lines),
# so the injected load is verifiable from the run directory even if devbench is killed.
run_devload2() {
  local rep=$1
  local rid="bn_devload2_r${rep}"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
  local prev; prev=$(done_rc "$rid"); [ -n "$prev" ] && { echo "== $rid done, skip"; return 0; }
  [ -d "$BASE" ] || { echo "missing base" >&2; return 9; }
  rm -rf "$db" "$rdir"; mkdir -p "$rdir"
  bash "$MKREP" "$BASE" "$db" > "$rdir/replica.log" 2>&1 || return 8
  local loadf=$BENCH/db/devload2_file.bin
  setsid nice -n 0 "$BENCH/tools/devbench" "$loadf" 8 16 0 900 > "$rdir/devload.log" 2>&1 &
  local lp=$!
  sleep 5
  echo "### injected: devbench 8GiB 16thr read_pct=0 (O_DIRECT 4KiB random writes)" >> "$rdir/run.log"
  launch "$rdir" "$db" 7200 600 -- --db=$db --out=$rdir/$rid --mode=overwrite --universe=$U50 \
    --num_ops=$UPDATES --use_existing_db=1 --dist=uniform --order=shuffle \
    --threads=$THREADS --value_size=1024 --write_buffer_size=$WB --max_write_buffer_number=4 \
    --level0_trigger=8 --level0_slowdown=20 --level0_stop=36 --target_file_size_base=64M \
    --max_background_jobs=8 --max_background_flushes=2 --max_background_compactions=6 \
    --max_subcompactions=8 --dynamic_level_bytes=1 --cache_size=$CACHE --bloom_bits=10 \
    --compression=none --stats_dump_period_sec=60 --sample_ms=500 --layout_ms=5000 \
    --dirscan_ms=10000 --progress_ms=15000 --drain_observe_s=30 --wait_for_drain=1 --wait_for_drain_max_s=300
  local rc=$?
  kill -9 $lp 2>/dev/null; pkill -9 -f "devbench .*devload2_file" 2>/dev/null
  rm -f "$loadf"
  finish_run "$rdir" "$db"
  record "$rid" "\"exp\":\"bn-devload2\",\"arm\":\"devload2\",\"rep\":$rep,\"rc\":$rc"
  echo "== $rid rc=$rc"
}
case "${1:-}" in
  devload2) for r in 1 2 3; do run_devload2 $r; done ;;
  *) echo "usage: bn_extra2.sh devload2"; exit 2 ;;
esac
