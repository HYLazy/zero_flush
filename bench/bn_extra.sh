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

# zipf sensitivity: same trajectory length, skewed arrivals, default capacity
run_zipf() {
  local rep=$1
  local rid="bn_zipf_r${rep}"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
  local prev; prev=$(done_rc "$rid"); [ -n "$prev" ] && { echo "== $rid done, skip"; return 0; }
  [ -d "$BASE" ] || { echo "missing base" >&2; return 9; }
  rm -rf "$db" "$rdir"; mkdir -p "$rdir"
  bash "$MKREP" "$BASE" "$db" > "$rdir/replica.log" 2>&1 || return 8
  launch "$rdir" "$db" 14400 600 -- --db=$db --out=$rdir/$rid --mode=overwrite --universe=$U50 \
    --num_ops=$UPDATES --use_existing_db=1 --dist=zipf --zipf_theta=0.99 \
    --threads=$THREADS --value_size=1024 --write_buffer_size=$WB --max_write_buffer_number=4 \
    --level0_trigger=8 --level0_slowdown=20 --level0_stop=36 --target_file_size_base=64M \
    --max_background_flushes=2 --max_background_compactions=6 --max_subcompactions=8 \
    --dynamic_level_bytes=1 --cache_size=$CACHE --bloom_bits=10 --compression=none \
    --stats_dump_period_sec=60 --sample_ms=500 --layout_ms=5000 --dirscan_ms=10000 \
    --progress_ms=15000 --drain_observe_s=60 --wait_for_drain=1 --wait_for_drain_max_s=900
  local rc=$?
  finish_run "$rdir" "$db"
  record "$rid" "\"exp\":\"bn-zipf\",\"arm\":\"zipf\",\"rep\":$rep,\"theta\":0.99,\"rc\":$rc"
  echo "== $rid rc=$rc"
}
# deep-stage structural intervention (plan 四.3): fewer LSM levels => fewer cascade
# rewrite steps for the same data. If the device service capacity is the binding
# constraint, cutting cascade work must raise foreground throughput.
run_deep() {
  local arm=$1 levels=$2 rep=$3
  local rid="bn_${arm}_r${rep}"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
  local prev; prev=$(done_rc "$rid"); [ -n "$prev" ] && { echo "== $rid done, skip"; return 0; }
  [ -d "$BASE" ] || { echo "missing base" >&2; return 9; }
  rm -rf "$db" "$rdir"; mkdir -p "$rdir"
  bash "$MKREP" "$BASE" "$db" > "$rdir/replica.log" 2>&1 || return 8
  launch "$rdir" "$db" 14400 600 -- --db=$db --out=$rdir/$rid --mode=overwrite --universe=$U50 \
    --num_ops=$UPDATES --use_existing_db=1 --dist=uniform --order=shuffle \
    --threads=$THREADS --value_size=1024 --write_buffer_size=$WB --max_write_buffer_number=4 \
    --level0_trigger=8 --level0_slowdown=20 --level0_stop=36 --target_file_size_base=64M \
    --max_background_jobs=8 --max_subcompactions=8 --num_levels=$levels \
    --dynamic_level_bytes=1 --cache_size=$CACHE --bloom_bits=10 --compression=none \
    --stats_dump_period_sec=60 --sample_ms=500 --layout_ms=5000 --dirscan_ms=10000 \
    --progress_ms=15000 --drain_observe_s=60 --wait_for_drain=1 --wait_for_drain_max_s=300
  local rc=$?
  finish_run "$rdir" "$db"
  record "$rid" "\"exp\":\"bn-deep\",\"arm\":\"$arm\",\"num_levels\":$levels,\"rep\":$rep,\"rc\":$rc"
  echo "== $rid rc=$rc"
}
# ablation arm `noc`: compaction OFF and L0 throttles lifted => the device carries
# only WAL+flush (~1.01xU). This is the upper bound of the write path when the
# compaction-generated device work is removed. Decisive for "the constraint is the
# device service capacity for compaction work", as opposed to any stage's workers.
run_noc() {
  local rep=$1
  local rid="bn_noc_r${rep}"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
  local prev; prev=$(done_rc "$rid"); [ -n "$prev" ] && { echo "== $rid done, skip"; return 0; }
  [ -d "$BASE" ] || { echo "missing base" >&2; return 9; }
  rm -rf "$db" "$rdir"; mkdir -p "$rdir"
  bash "$MKREP" "$BASE" "$db" > "$rdir/replica.log" 2>&1 || return 8
  launch "$rdir" "$db" 7200 600 -- --db=$db --out=$rdir/$rid --mode=overwrite --universe=$U50 \
    --num_ops=$UPDATES --use_existing_db=1 --dist=uniform --order=shuffle \
    --threads=$THREADS --value_size=1024 --write_buffer_size=$WB --max_write_buffer_number=8 \
    --level0_trigger=100000 --level0_slowdown=100000 --level0_stop=100000 \
    --disable_auto_compactions=1 --target_file_size_base=64M \
    --max_background_jobs=8 --max_background_flushes=2 --max_background_compactions=6 \
    --max_subcompactions=8 --dynamic_level_bytes=1 --cache_size=$CACHE --bloom_bits=10 \
    --compression=none --stats_dump_period_sec=60 --sample_ms=500 --layout_ms=5000 \
    --dirscan_ms=10000 --progress_ms=15000 --drain_observe_s=30
  local rc=$?
  finish_run "$rdir" "$db"
  record "$rid" "\"exp\":\"bn-noc\",\"arm\":\"noc\",\"rep\":$rep,\"rc\":$rc"
  echo "== $rid rc=$rc"
}
# decisive arm `devload`: run the standard workload while a controlled background
# device load (devbench, O_DIRECT 4 KiB random writes, 16 threads) consumes the same
# device. If the device's service capacity is the shared binding constraint, the
# foreground throughput must fall by roughly the injected fraction; if it does not,
# the device was not binding and some stage-internal limit is.
run_devload() {
  local rep=$1
  local rid="bn_devload_r${rep}"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
  local prev; prev=$(done_rc "$rid"); [ -n "$prev" ] && { echo "== $rid done, skip"; return 0; }
  [ -d "$BASE" ] || { echo "missing base" >&2; return 9; }
  rm -rf "$db" "$rdir"; mkdir -p "$rdir"
  bash "$MKREP" "$BASE" "$db" > "$rdir/replica.log" 2>&1 || return 8
  local loadf=$BENCH/db/devload_file.bin
  setsid nice -n 0 "$BENCH/tools/devbench" "$loadf" 8 16 0 900 > "$rdir/devload.log" 2>&1 &
  local lp=$!
  sleep 3
  launch "$rdir" "$db" 7200 600 -- --db=$db --out=$rdir/$rid --mode=overwrite --universe=$U50 \
    --num_ops=$UPDATES --use_existing_db=1 --dist=uniform --order=shuffle \
    --threads=$THREADS --value_size=1024 --write_buffer_size=$WB --max_write_buffer_number=4 \
    --level0_trigger=8 --level0_slowdown=20 --level0_stop=36 --target_file_size_base=64M \
    --max_background_jobs=8 --max_background_flushes=2 --max_background_compactions=6 \
    --max_subcompactions=8 --dynamic_level_bytes=1 --cache_size=$CACHE --bloom_bits=10 \
    --compression=none --stats_dump_period_sec=60 --sample_ms=500 --layout_ms=5000 \
    --dirscan_ms=10000 --progress_ms=15000 --drain_observe_s=30
  local rc=$?
  kill -9 $lp 2>/dev/null; pkill -9 -f "devbench .*devload_file" 2>/dev/null
  rm -f "$loadf"
  finish_run "$rdir" "$db"
  record "$rid" "\"exp\":\"bn-devload\",\"arm\":\"devload\",\"rep\":$rep,\"load_rc\":$?,\"rc\":$rc"
  echo "== $rid rc=$rc"
}
# deep-work geometry interventions (feasible on an existing DB, unlike num_levels):
#   mc8g : max_compaction_bytes=8G (default ~1.6G) -- let deep compactions take a much
#          larger input batch, which the E8 evidence says cuts repeated base rewrite.
#   mult20: max_bytes_for_level_multiplier=20 (default 10, set at t=1s via SetOptions)
#          -- wider levels => the data occupies fewer intermediate levels => shorter
#          cascade. Both aim at the same thing: reduce the deep cascade rewrite bytes.
run_geom() {
  local arm=$1 rep=$2
  local rid="bn_${arm}_r${rep}"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
  local prev; prev=$(done_rc "$rid"); [ -n "$prev" ] && { echo "== $rid done, skip"; return 0; }
  [ -d "$BASE" ] || { echo "missing base" >&2; return 9; }
  rm -rf "$db" "$rdir"; mkdir -p "$rdir"
  bash "$MKREP" "$BASE" "$db" > "$rdir/replica.log" 2>&1 || return 8
  local gargs=""
  case "$arm" in
    mc8g)   gargs="--max_compaction_bytes=8589934592" ;;
    mc32g)  gargs="--max_compaction_bytes=34359738368" ;;
    mult20) gargs="--set_opt=1:max_bytes_for_level_multiplier=20" ;;
  esac
  launch "$rdir" "$db" 7200 600 -- --db=$db --out=$rdir/$rid --mode=overwrite --universe=$U50 \
    --num_ops=$UPDATES --use_existing_db=1 --dist=uniform --order=shuffle \
    --threads=$THREADS --value_size=1024 --write_buffer_size=$WB --max_write_buffer_number=4 \
    --level0_trigger=8 --level0_slowdown=20 --level0_stop=36 --target_file_size_base=64M \
    --max_background_jobs=8 --max_background_flushes=2 --max_background_compactions=6 \
    --max_subcompactions=8 --dynamic_level_bytes=1 $gargs \
    --cache_size=$CACHE --bloom_bits=10 --compression=none --stats_dump_period_sec=60 \
    --sample_ms=500 --layout_ms=5000 --dirscan_ms=10000 --progress_ms=15000 \
    --drain_observe_s=30 --wait_for_drain=1 --wait_for_drain_max_s=300
  local rc=$?
  finish_run "$rdir" "$db"
  record "$rid" "\"exp\":\"bn-geom\",\"arm\":\"$arm\",\"rep\":$rep,\"rc\":$rc"
  echo "== $rid rc=$rc"
}
case "${1:-}" in
  zipf) for r in 1 2 3 4 5; do run_zipf $r; done ;;
  noc) for r in 1 2 3 4 5; do run_noc $r; done ;;
  devload) for r in 1 2 3; do run_devload $r; done ;;
  geom) for r in 1 2 3; do run_geom mc8g $r; run_geom mult20 $r; done ;;
  geom2) for r in 1 2 3; do run_geom mc32g $r; done ;;
  *) echo "usage: bn_extra.sh zipf|noc|devload|geom|geom2"; exit 2 ;;
esac
