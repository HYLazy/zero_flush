#!/bin/bash
# Campaign driver for the L0 investigation (native RocksDB 11.2, no source patches).
#
#   bench/campaign.sh A [run_index ...]     # E1/E5/E9 main line at 50GB / 100GB
#   bench/campaign.sh B [arm ...]           # E2/E3/E7 controlled recent-layer arms
#   bench/campaign.sh C [arm ...]           # E8 batching amortisation (trigger sweep)
#   bench/campaign.sh D [arm ...]           # E4 parallelism sweep, E6 bursts, E5 interventions
#   bench/campaign.sh E [arm ...]           # E10 recovery / WAL-retention boundaries
#   bench/campaign.sh status                # show what has completed
#
# Layout: bench/out/<stage>/<run_id>/{*.csv,*.json,run.log,run.json,db.LOG}
#         bench/db/<run_id>                 (deleted after the run unless KEEP_DB=1)
#         bench/out/<stage>/manifest.jsonl  (one JSON line per run: exact identity)
# Env: THREADS WB CACHE KEEP_DB DRY FREE_MIN_GB
set -uo pipefail
# STAGE_G_APPLIED

ROOT=/home/embed/hyl/metadata_offload
BENCH=$ROOT/bench
BIN=$BENCH/tools/l0bench
MKREP=$BENCH/mkreplica.sh
export LD_LIBRARY_PATH=$ROOT/source/rocksdb/build:/usr/lib/x86_64-linux-gnu

THREADS=${THREADS:-16}
WB=${WB:-256M}
CACHE=${CACHE:-8G}
KEEP_DB=${KEEP_DB:-0}
DRY=${DRY:-0}
FREE_MIN_GB=${FREE_MIN_GB:-150}

STAGE=${1:-status}
shift || true
ONLY=("$@")

OUT=$BENCH/out/$STAGE
mkdir -p "$OUT"

free_gb() { df -BG --output=avail "$ROOT" | tail -1 | tr -dc '0-9'; }

record() {   # record <run_id> <extra-json-fields>
  local rid=$1 extra=$2
  printf '{"stage":"%s","run_id":"%s","ts":"%s","host":"%s","bin_md5":"%s","git_head":"%s",%s}\n' \
    "$STAGE" "$rid" "$(date '+%F %T')" "$(hostname)" \
    "$(md5sum $BIN | cut -d' ' -f1)" "$(git -C $ROOT rev-parse --short HEAD 2>/dev/null)" \
    "$extra" >> "$BENCH/out/$STAGE/manifest.jsonl"
}

wants() { local i=$1; [ ${#ONLY[@]} -eq 0 ] && return 0
  for x in "${ONLY[@]}"; do [ "$x" = "$i" ] && return 0; done; return 1; }

require_db() { # require_db <dbdir> : fail loudly rather than silently measuring nothing
  if [ ! -d "$1" ]; then echo "### MISSING DB $1 -- skipping (run the producer first)" >&2; return 1; fi
  return 0
}

# launch <rdir> <dbdir> <timeout_s> <stall_secs> -- <l0bench args>
launch() {
  local rdir=$1 dbdir=$2 tmo=$3 stall_limit=$4; shift 4
  [ "$1" = "--" ] && shift
  local log=$rdir/run.log
  mkdir -p "$rdir"
  {
    echo "### CMD: $BIN $*"
    echo "### START $(date '+%F %T') freeGB=$(free_gb)"
    if [ -d "$dbdir" ]; then
      ls -la "$dbdir" > "$rdir/dir_before.txt" 2>&1
      echo "### DB_BEFORE bytes=$(du -sb "$dbdir" 2>/dev/null | cut -f1) sst=$(du -cb "$dbdir"/*.sst 2>/dev/null | tail -1 | cut -f1) wal=$(du -cb "$dbdir"/*.log 2>/dev/null | tail -1 | cut -f1)"
    fi
  } >> "$log"
  if [ "$DRY" = "1" ]; then echo "### DRY RUN (not executed)" >> "$log"; return 0; fi

  setsid nice -n 5 "$BIN" "$@" >> "$log" 2>&1 &
  local pid=$! waited=0 last_ops=-1 last_cpu=-1 stalled=0
  while kill -0 "$pid" 2>/dev/null; do
    sleep 20; waited=$((waited+20))
    local ops cpu fg
    ops=$(grep -o 'PROGRESS [0-9.]*s ops=[0-9]*' "$log" | tail -1 | sed 's/.*ops=//')
    cpu=$(awk '{print $14+$15}' /proc/$pid/stat 2>/dev/null); cpu=${cpu:-0}
    if [ "${ops:-}" = "${last_ops}" ] && [ "$cpu" = "${last_cpu}" ]; then
      stalled=$((stalled+20))
    else
      stalled=0
    fi
    last_ops=${ops:-$last_ops}; last_cpu=$cpu
    fg=$(free_gb)
    if [ "$fg" -lt "$FREE_MIN_GB" ]; then
      echo "### ABORT_DISK_LOW freeGB=$fg $(date '+%F %T')" >> "$log"
      pkill -KILL -s "$pid" 2>/dev/null; echo "ABORT_DISK_LOW" >> "$log"; return 90
    fi
    if [ "$stalled" -ge "$stall_limit" ] && [ "$stall_limit" -gt 0 ]; then
      echo "### STALL ${stalled}s (no op progress, no CPU growth) $(date '+%F %T')" >> "$log"
      timeout 240 gdb -p "$pid" -batch -ex "set pagination off" -ex "thread apply all bt 20" \
        > "$rdir/stall_bt.txt" 2>&1
      cp "$dbdir/LOG" "$rdir/stall.LOG" 2>/dev/null
      pkill -KILL -s "$pid" 2>/dev/null
      echo "STALL" >> "$log"; return 91
    fi
    if [ "$tmo" -gt 0 ] && [ "$waited" -ge "$tmo" ]; then
      echo "### TIMEOUT ${waited}s $(date '+%F %T')" >> "$log"
      pkill -KILL -s "$pid" 2>/dev/null
      echo "TIMEOUT" >> "$log"; return 92
    fi
    if [ $((waited % 300)) -eq 0 ]; then echo "### alive ${waited}s freeGB=$fg ops=${ops:-?}" >> "$log"; fi
  done
  wait "$pid"; local rc=$?
  echo "### END $(date '+%F %T') rc=$rc freeGB=$(free_gb)" >> "$log"
  return $rc
}

finish_run() { # finish_run <rdir> <dbdir> [keep]
  local rdir=$1 dbdir=$2 keep=${3:-$KEEP_DB}
  [ -f "$dbdir/LOG" ] && cp "$dbdir/LOG" "$rdir/db.LOG"
  [ -d "$dbdir" ] && ls -la "$dbdir" > "$rdir/dir_after.txt" 2>&1
  if [ "$keep" != "1" ] && [ -d "$dbdir" ]; then rm -rf "$dbdir"; fi
}

# ---------------------------------------------------------------------------
# shared option blocks
# ---------------------------------------------------------------------------
fill_common() {   # primary configuration, documented in CAMPAIGN.md
  echo "--threads=$THREADS --value_size=1024 --write_buffer_size=$WB \
--max_write_buffer_number=4 --level0_trigger=8 --level0_slowdown=20 --level0_stop=36 \
--target_file_size_base=64M --max_background_jobs=8 --max_subcompactions=8 \
--dynamic_level_bytes=1 --cache_size=$CACHE --bloom_bits=10 --compression=none \
--stats_dump_period_sec=60 --sample_ms=1000 --layout_ms=10000 --dirscan_ms=10000 \
--progress_ms=15000"
}

# ---------------------------------------------------------------------------
# Stage A -- E1/E5/E9 main line
# ---------------------------------------------------------------------------
stage_A() {
  local U50=48000000 U100=96000000
  local C; C=$(fill_common)
  local B50=50gb_fill_r1; local B100=100gb_fill_r5

  if wants 1; then
    local rid db rdir
    local rid=$B50; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    rm -rf "$db"; mkdir -p "$rdir"
    launch "$rdir" "$db" 21600 300 -- --db=$db --out=$rdir/$rid --mode=fill --universe=$U50 \
      --num_ops=$U50 --dist=uniform --order=shuffle $C \
      --drain_observe_s=120 --wait_for_drain=1 --wait_for_drain_max_s=900 \
      --final_flush=1 --final_flush_observe_s=60
    local rc=$?; finish_run "$rdir" "$db" 1          # KEEP: base for later stages
    record "$rid" "\"phase\":\"fill\",\"scale\":\"50gb\",\"universe\":$U50,\"rc\":$rc"
  fi
  if wants 10; then   # snapshot the settled fill as the shared base layout for stages B/C/D
    require_db "$BENCH/db/$B50" || return 0
    local rid="base50_snapshot"; local db=$BENCH/db/$B50; local rdir=$OUT/$rid
    mkdir -p "$rdir" "$BENCH/base"
    rm -rf "$BENCH/base/base50"
    bash "$MKREP" "$BENCH/db/$B50" "$BENCH/base/base50" > "$rdir/replica.log" 2>&1
    local rrc=$?
    "$BIN" --db=$BENCH/base/base50 --out=$rdir/$rid.verify --mode=verify --universe=$U50 \
      --verify_step=4999 --verify_salt=1 > "$rdir/verify.log" 2>&1
    local vrc=$?
    record "$rid" "\"replica_rc\":$rrc,\"verify_rc\":$vrc"
  fi
  if wants 2; then
    require_db "$BENCH/db/$B50" || return 0
    local rid=50gb_read; local db=$BENCH/db/$B50; local rdir=$OUT/$rid
    mkdir -p "$rdir"
    launch "$rdir" "$db" 3600 300 -- --db=$db --out=$rdir/$rid --mode=read --universe=$U50 \
      --num_ops=8000000 --use_existing_db=1 --read_mix=recent:0.4,deep:0.4,neg:0.2 $C \
      --perf_detail=1 --detail_rate=0.001 --read_log_rate=0.002 --layout_ms=5000
    local rc=$?; record "$rid" "\"phase\":\"read\",\"scale\":\"50gb\",\"rc\":$rc"
  fi
  if wants 3; then
    require_db "$BENCH/db/$B50" || return 0
    local rid=50gb_overwrite; local db=$BENCH/db/$B50; local rdir=$OUT/$rid
    mkdir -p "$rdir"
    launch "$rdir" "$db" 10800 300 -- --db=$db --out=$rdir/$rid --mode=overwrite --universe=$U50 \
      --num_ops=$U50 --use_existing_db=1 --dist=uniform $C \
      --drain_observe_s=120 --wait_for_drain=1 --wait_for_drain_max_s=900
    local rc=$?; finish_run "$rdir" "$db" 1
    record "$rid" "\"phase\":\"overwrite\",\"scale\":\"50gb\",\"rc\":$rc"
  fi
  if wants 11; then   # repeat of fill (independent repetition), own DB, not kept
    local rid=50gb_fill_r11; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    rm -rf "$db"; mkdir -p "$rdir"
    launch "$rdir" "$db" 21600 300 -- --db=$db --out=$rdir/$rid --mode=fill --universe=$U50 \
      --num_ops=$U50 --dist=uniform --order=shuffle $C \
      --drain_observe_s=120 --wait_for_drain=1 --wait_for_drain_max_s=900
    local rc=$?; finish_run "$rdir" "$db" 0
    record "$rid" "\"phase\":\"fill\",\"scale\":\"50gb\",\"repetition\":2,\"rc\":$rc"
  fi
  if wants 12; then
    local rid=50gb_fill_r12; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    rm -rf "$db"; mkdir -p "$rdir"
    launch "$rdir" "$db" 21600 300 -- --db=$db --out=$rdir/$rid --mode=fill --universe=$U50 \
      --num_ops=$U50 --dist=uniform --order=shuffle $C \
      --drain_observe_s=120 --wait_for_drain=1 --wait_for_drain_max_s=900
    local rc=$?; finish_run "$rdir" "$db" 0
    record "$rid" "\"phase\":\"fill\",\"scale\":\"50gb\",\"repetition\":3,\"rc\":$rc"
  fi
  if wants 5; then
    local rid=$B100; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    rm -rf "$db"; mkdir -p "$rdir"
    launch "$rdir" "$db" 28800 300 -- --db=$db --out=$rdir/$rid --mode=fill --universe=$U100 \
      --num_ops=$U100 --dist=uniform --order=shuffle $C \
      --drain_observe_s=120 --wait_for_drain=1 --wait_for_drain_max_s=1200 \
      --final_flush=1 --final_flush_observe_s=60
    local rc=$?; finish_run "$rdir" "$db" 1
    record "$rid" "\"phase\":\"fill\",\"scale\":\"100gb\",\"universe\":$U100,\"rc\":$rc"
  fi
  if wants 6; then
    require_db "$BENCH/db/$B100" || return 0
    local rid=100gb_read; local db=$BENCH/db/$B100; local rdir=$OUT/$rid
    mkdir -p "$rdir"
    launch "$rdir" "$db" 3600 300 -- --db=$db --out=$rdir/$rid --mode=read --universe=$U100 \
      --num_ops=8000000 --use_existing_db=1 --read_mix=recent:0.4,deep:0.4,neg:0.2 $C \
      --perf_detail=1 --detail_rate=0.001 --read_log_rate=0.002 --layout_ms=5000
    local rc=$?; record "$rid" "\"phase\":\"read\",\"scale\":\"100gb\",\"rc\":$rc"
  fi
  if wants 7; then
    require_db "$BENCH/db/$B100" || return 0
    local rid=100gb_overwrite; local db=$BENCH/db/$B100; local rdir=$OUT/$rid
    mkdir -p "$rdir"
    launch "$rdir" "$db" 14400 300 -- --db=$db --out=$rdir/$rid --mode=overwrite --universe=$U100 \
      --num_ops=$U100 --use_existing_db=1 --dist=uniform $C \
      --drain_observe_s=120 --wait_for_drain=1 --wait_for_drain_max_s=1200
    local rc=$?; finish_run "$rdir" "$db" 0
    record "$rid" "\"phase\":\"overwrite\",\"scale\":\"100gb\",\"rc\":$rc"
  fi
  if wants 4; then   # online mixed window at 50GB (read+write interleaved)
    local rid=50gb_mix; local rdir=$OUT/$rid
    local db=$BENCH/db/$B50
    if [ ! -d "$db" ]; then
      [ -d "$BENCH/base/base50" ] || { echo "### skip $rid (no base50)" >&2; return 0; }
      echo "### mix: working DB gone, using a replica of base50" >&2
      db=$BENCH/db/50gb_mix_w
      rm -rf "$db"
      mkdir -p "$rdir"
      bash "$MKREP" "$BENCH/base/base50" "$db" > "$rdir/replica.log" 2>&1 || continue
    fi
    mkdir -p "$rdir"
    launch "$rdir" "$db" 10800 300 -- --db=$db --out=$rdir/$rid --mode=mix --universe=$U50 \
      --num_ops=32000000 --read_pct=50 --use_existing_db=1 --dist=uniform $C \
      --read_mix=recent:0.5,deep:0.5 --perf_detail=1 --detail_rate=0.0005 --read_log_rate=0.001
    local rc=$?; finish_run "$rdir" "$db" 0
    record "$rid" "\"phase\":\"mix\",\"scale\":\"50gb\",\"rc\":$rc"
  fi
# Fragment: additional stage-A runs (cold reads, scans, 100GB snapshot).
# Applied to campaign.sh AFTER the running driver exits (bash reads a running script
# lazily, so editing it mid-run can corrupt execution).
#
# Runs added:
#   24  base100 snapshot   : hardlink replica of the settled 100GB fill + verify
#   21  50gb_read_cold     : settled 50GB layout, 64 MiB block cache (device reads forced)
#   22  100gb_read_cold    : settled 100GB layout, 64 MiB block cache
#   23  50gb_scan          : settled 50GB layout, iterator scans (records + bytes returned)

if wants 24; then
  require_db "$BENCH/db/$B100" || return 0
  local rid="base100_snapshot"; local db=$BENCH/db/$B100; local rdir=$OUT/$rid
  mkdir -p "$rdir" "$BENCH/base"
  rm -rf "$BENCH/base/base100"
  bash "$MKREP" "$BENCH/db/$B100" "$BENCH/base/base100" > "$rdir/replica.log" 2>&1
  local rrc=$?
  "$BIN" --db=$BENCH/base/base100 --out=$rdir/$rid.verify --mode=verify --universe=$U100 \
    --verify_step=19997 --verify_salt=1 > "$rdir/verify.log" 2>&1
  local vrc=$?
  record "$rid" "\"replica_rc\":$rrc,\"verify_rc\":$vrc"
fi
if wants 21; then
  local rid=50gb_read_cold; local db=$BENCH/db/${rid}_w; local rdir=$OUT/$rid
  [ -d "$BENCH/base/base50" ] || continue
  rm -rf "$db" "$rdir"; mkdir -p "$rdir"
  bash "$MKREP" "$BENCH/base/base50" "$db" > "$rdir/replica.log" 2>&1 || continue
  launch "$rdir" "$db" 3600 300 -- --db=$db --out=$rdir/$rid --mode=read --universe=$U50 \
    --num_ops=8000000 --use_existing_db=1 --read_mix=recent:0.4,deep:0.4,neg:0.2 \
    ${C/--cache_size=$CACHE/--cache_size=64M} --perf_detail=1 --detail_rate=0.001 \
    --read_log_rate=0.002 --layout_ms=5000
  local rc=$?; finish_run "$rdir" "$db" 0
  record "$rid" "\"phase\":\"read\",\"scale\":\"50gb\",\"variant\":\"cold-cache\",\"rc\":$rc"
fi
if wants 22; then
  local rid=100gb_read_cold; local db=$BENCH/db/${rid}_w; local rdir=$OUT/$rid
  [ -d "$BENCH/base/base100" ] || continue
  rm -rf "$db" "$rdir"; mkdir -p "$rdir"
  bash "$MKREP" "$BENCH/base/base100" "$db" > "$rdir/replica.log" 2>&1 || continue
  launch "$rdir" "$db" 3600 300 -- --db=$db --out=$rdir/$rid --mode=read --universe=$U100 \
    --num_ops=8000000 --use_existing_db=1 --read_mix=recent:0.4,deep:0.4,neg:0.2 \
    ${C/--cache_size=$CACHE/--cache_size=64M} --perf_detail=1 --detail_rate=0.001 \
    --read_log_rate=0.002 --layout_ms=5000
  local rc=$?; finish_run "$rdir" "$db" 0
  record "$rid" "\"phase\":\"read\",\"scale\":\"100gb\",\"variant\":\"cold-cache\",\"rc\":$rc"
fi
if wants 23; then
  local rid=50gb_scan; local db=$BENCH/db/${rid}_w; local rdir=$OUT/$rid
  [ -d "$BENCH/base/base50" ] || continue
  rm -rf "$db" "$rdir"; mkdir -p "$rdir"
  bash "$MKREP" "$BENCH/base/base50" "$db" > "$rdir/replica.log" 2>&1 || continue
  launch "$rdir" "$db" 3600 300 -- --db=$db --out=$rdir/$rid --mode=read --universe=$U50 \
    --num_ops=200000 --use_existing_db=1 --read_mix=scan:1.0 --scan_len=10 \
    --perf_detail=1 --detail_rate=0.01 --read_log_rate=0.01 --layout_ms=10000
  local rc=$?; finish_run "$rdir" "$db" 0
  record "$rid" "\"phase\":\"scan\",\"scale\":\"50gb\",\"scan_len\":10,\"rc\":$rc"
fi
  if wants 8; then   # variant: static levelled
    local rid=50gb_fill_static; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    rm -rf "$db"; mkdir -p "$rdir"
    launch "$rdir" "$db" 21600 300 -- --db=$db --out=$rdir/$rid --mode=fill --universe=$U50 \
      --num_ops=$U50 --dist=uniform --order=shuffle \
      ${C/--dynamic_level_bytes=1/--dynamic_level_bytes=0} \
      --drain_observe_s=120 --wait_for_drain=1 --wait_for_drain_max_s=900
    local rc=$?; finish_run "$rdir" "$db" 0
    record "$rid" "\"phase\":\"fill\",\"scale\":\"50gb\",\"variant\":\"static-levels\",\"rc\":$rc"
  fi
  if wants 9; then   # variant: lz4 compression (compressed byte accounting)
    local rid=50gb_fill_lz4; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    rm -rf "$db"; mkdir -p "$rdir"
    launch "$rdir" "$db" 21600 300 -- --db=$db --out=$rdir/$rid --mode=fill --universe=$U50 \
      --num_ops=$U50 --dist=uniform --order=shuffle \
      ${C/--compression=none/--compression=lz4} \
      --drain_observe_s=120 --wait_for_drain=1 --wait_for_drain_max_s=900
    local rc=$?; finish_run "$rdir" "$db" 0
    record "$rid" "\"phase\":\"fill\",\"scale\":\"50gb\",\"variant\":\"lz4\",\"rc\":$rc"
  fi
}

# ---------------------------------------------------------------------------
# Stage B -- E2 (overlap) / E3 (read cost) / E7 (memory vs backlog)
# Controlled recent layer on replicas of one identical 50GB base layout.
# ---------------------------------------------------------------------------
stage_B() {
  local BASE=$BENCH/base/base50
  local U50=48000000
  local KPF=131072                      # keys per recent-layer file (128 MiB at 1KiB)
  # arm: K files x overlap x cache regime
  local ARMS=("1:narrow" "1:wide" "4:narrow" "4:wide" "8:narrow" "8:wide" "16:narrow" "16:wide")
  local i=0
  for arm in "${ARMS[@]}"; do
    i=$((i+1)); wants "$i" || continue
    local K=${arm%%:*} OV=${arm##*:}
    local rid="b_k${K}_${OV}"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    local db=$BENCH/db/$rid rdir=$OUT/$rid
    if [ ! -d "$BASE" ]; then echo "### missing base $BASE" >&2; return 0; fi
    rm -rf "$db" "$rdir"; mkdir -p "$rdir"
    bash "$MKREP" "$BASE" "$db" > "$rdir/replica.log" 2>&1 || { echo "replica failed"; continue; }
    # 1) build the recent layer (frozen: auto compactions off)
    launch "$rdir" "$db" 3600 0 -- --db=$db --out=$rdir/$rid.build --mode=backlog --universe=$U50 \
      --use_existing_db=1 --disable_auto_compactions=1 --backlog_files=$K \
      --backlog_keys_per_file=$KPF --backlog_overlap=$OV \
      --write_buffer_size=512M --level0_trigger=1000 --cache_size=$CACHE --layout_ms=2000 \
      --sample_ms=1000 --dirscan_ms=5000
    local rc1=$?
    # 2) frozen-layout read microbench, cache-resident regime
    local lo=$((U50 - K*KPF))
    launch "$rdir" "$db" 1800 0 -- --db=$db --out=$rdir/$rid.read_hot --mode=read --universe=$U50 \
      --num_ops=2000000 --use_existing_db=1 --disable_auto_compactions=1 \
      --recent_span=$((K*KPF)) --deep_span=$((U50/5)) \
      --read_mix=recent:0.4,deep:0.4,neg:0.2 --cache_size=$CACHE --bloom_bits=10 \
      --perf_detail=1 --detail_rate=0.01 --read_log_rate=0.01 --layout_ms=5000
    local rc2=$?
    # 3) same but with a small block cache: forces device reads
    launch "$rdir" "$db" 1800 0 -- --db=$db --out=$rdir/$rid.read_cold --mode=read --universe=$U50 \
      --num_ops=2000000 --use_existing_db=1 --disable_auto_compactions=1 \
      --recent_span=$((K*KPF)) --deep_span=$((U50/5)) \
      --read_mix=recent:0.4,deep:0.4,neg:0.2 --cache_size=64M --bloom_bits=10 \
      --perf_detail=1 --detail_rate=0.01 --read_log_rate=0.01 --layout_ms=5000
    local rc3=$?
    # 4) same frozen layout, but with auto compactions re-enabled: cost while L0 drains
    launch "$rdir" "$db" 1800 0 -- --db=$db --out=$rdir/$rid.read_online --mode=read --universe=$U50 \
      --num_ops=2000000 --use_existing_db=1 \
      --recent_span=$((K*KPF)) --deep_span=$((U50/5)) \
      --read_mix=recent:0.4,deep:0.4,neg:0.2 --cache_size=$CACHE --bloom_bits=10 \
      --perf_detail=1 --detail_rate=0.01 --read_log_rate=0.01 --layout_ms=5000
    local rc4=$?
    finish_run "$rdir" "$db" 0
    record "$rid" "\"K\":$K,\"overlap\":\"$OV\",\"keys_per_file\":$KPF,\"recent_lo\":$lo,\"rc\":[$rc1,$rc2,$rc3,$rc4]"
  done
}

# ---------------------------------------------------------------------------
# Stage C -- E8: does batching amortise target-level rewrite?
# Same initial layout replica + identical update trace, only the L0->base batch
# size (level0_file_num_compaction_trigger) changes.
# ---------------------------------------------------------------------------
stage_C() {
  local BASE=$BENCH/base/base50
  local U50=48000000
  local UPDATES=12800000        # 12.8M updates = 12.8 GB of new writes per arm
  local TRIGS=(2 4 8 16 32)
  local i=0
  for t in "${TRIGS[@]}"; do
    i=$((i+1)); wants "$i" || continue
    local rid="c_trig$t"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    [ -d "$BASE" ] || { echo "### missing base $BASE" >&2; return 0; }
    rm -rf "$db" "$rdir"; mkdir -p "$rdir"
    bash "$MKREP" "$BASE" "$db" > "$rdir/replica.log" 2>&1 || continue
    launch "$rdir" "$db" 7200 300 -- --db=$db --out=$rdir/$rid --mode=overwrite --universe=$U50 \
      --num_ops=$UPDATES --use_existing_db=1 --dist=uniform --level0_trigger=$t \
      --level0_slowdown=$((t*3)) --level0_stop=$((t*5)) \
      --threads=$THREADS --value_size=1024 --write_buffer_size=$WB --max_write_buffer_number=4 \
      --target_file_size_base=64M --max_background_jobs=8 --max_subcompactions=8 \
      --dynamic_level_bytes=1 --cache_size=$CACHE --bloom_bits=10 --compression=none \
      --stats_dump_period_sec=60 --sample_ms=1000 --layout_ms=10000 --dirscan_ms=10000 \
      --progress_ms=15000 --drain_observe_s=60 --wait_for_drain=1 --wait_for_drain_max_s=900
    local rc=$?
    # frozen check: same logical state regardless of batch size
    local vlog=$rdir/$rid.verify.log
    "$BIN" --db=$db --out=$rdir/$rid.verify --mode=verify --universe=$U50 --verify_step=19997 \
      --verify_salt=1 > "$vlog" 2>&1; local vrc=$?
    finish_run "$rdir" "$db" 0
    record "$rid" "\"trigger\":$t,\"updates\":$UPDATES,\"rc\":$rc,\"verify_rc\":$vrc"
  done
}

# ---------------------------------------------------------------------------
# Stage D -- E4 (job/subcompaction parallelism), E6 (burst buffering),
#            E5 (targeted interventions)
# ---------------------------------------------------------------------------
stage_D() {
  local BASE=$BENCH/base/base50
  local U10=9600000             # 10 GB scale for the sweeps (config scans, not the main line)
  # --- E4a: vary subcompaction limit, jobs fixed at 8 ---
  local i=0
  for sc in 1 4 8 16; do
    i=$((i+1)); wants "$i" || continue
    local rid="d_e4_sc$sc"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    rm -rf "$db" "$rdir"; mkdir -p "$rdir"
    launch "$rdir" "$db" 3600 300 -- --db=$db --out=$rdir/$rid --mode=fill --universe=$U10 \
      --num_ops=$U10 --dist=uniform --order=shuffle --threads=$THREADS --value_size=1024 \
      --write_buffer_size=128M --max_write_buffer_number=4 --level0_trigger=4 \
      --target_file_size_base=64M --max_background_jobs=8 --max_subcompactions=$sc \
      --dynamic_level_bytes=1 --cache_size=4G --bloom_bits=10 --compression=none \
      --sample_ms=1000 --layout_ms=5000 --dirscan_ms=10000 --progress_ms=5000 \
      --stats_dump_period_sec=30
    local rc=$?; finish_run "$rdir" "$db" 0
    record "$rid" "\"exp\":\"E4-subcompactions\",\"subcompactions\":$sc,\"jobs\":8,\"rc\":$rc"
  done
  # --- E4b: vary background jobs, subcompactions fixed at 8 ---
  for jb in 2 4 8 16; do
    i=$((i+1)); wants "$i" || continue
    local rid="d_e4_job$jb"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    rm -rf "$db" "$rdir"; mkdir -p "$rdir"
    launch "$rdir" "$db" 3600 300 -- --db=$db --out=$rdir/$rid --mode=fill --universe=$U10 \
      --num_ops=$U10 --dist=uniform --order=shuffle --threads=$THREADS --value_size=1024 \
      --write_buffer_size=128M --max_write_buffer_number=4 --level0_trigger=4 \
      --target_file_size_base=64M --max_background_jobs=$jb --max_subcompactions=8 \
      --dynamic_level_bytes=1 --cache_size=4G --bloom_bits=10 --compression=none \
      --sample_ms=1000 --layout_ms=5000 --dirscan_ms=10000 --progress_ms=5000 \
      --stats_dump_period_sec=30
    local rc=$?; finish_run "$rdir" "$db" 0
    record "$rid" "\"exp\":\"E4-jobs\",\"subcompactions\":8,\"jobs\":$jb,\"rc\":$rc"
  done
  # --- E6: open-loop burst on a replica of the settled 50GB base ---
  # 600 s at 4000 ops/s (4 MB/s), plus a 60 s burst at 8x from t=180
  local e6=0
  for spec in "3000:180:240:8" "6000:180:240:8" "4000:120:180:4"; do
    e6=$((e6+1)); i=$((i+1)); wants "$i" || continue
    local rate=${spec%%:*} rest=${spec#*:}
    local bs=${rest%%:*} rest2=${rest#*:}; local be=${rest2%%:*}; local mult=${rest2##*:}
    local rid="d_e6_r${rate}_x${mult}"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    [ -d "$BASE" ] || { echo "### missing base" >&2; return 0; }
    rm -rf "$db" "$rdir"; mkdir -p "$rdir"
    bash "$MKREP" "$BASE" "$db" > "$rdir/replica.log" 2>&1 || continue
    launch "$rdir" "$db" 2400 0 -- --db=$db --out=$rdir/$rid --mode=overwrite --universe=48000000 \
      --use_existing_db=1 --dist=uniform --arrival=open --rate=$rate --burst=$bs:$be:$mult \
      --duration_s=600 --threads=32 --value_size=1024 --write_buffer_size=$WB \
      --max_write_buffer_number=4 --level0_trigger=8 --level0_slowdown=20 --level0_stop=36 \
      --target_file_size_base=64M --max_background_jobs=8 --max_subcompactions=8 \
      --dynamic_level_bytes=1 --cache_size=$CACHE --bloom_bits=10 --compression=none \
      --sample_ms=500 --layout_ms=2000 --dirscan_ms=10000 --progress_ms=5000 \
      --drain_observe_s=180 --wait_for_drain=1 --wait_for_drain_max_s=900
    local rc=$?; finish_run "$rdir" "$db" 0
    record "$rid" "\"exp\":\"E6-burst\",\"rate\":$rate,\"burst\":\"$bs:$be:$mult\",\"rc\":$rc"
  done
  # --- E5: targeted intervention -- throttle background service mid-run ---
  for scen in "bg1" "bg1_restore"; do
    i=$((i+1)); wants "$i" || continue
    local rid="d_e5_$scen"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    rm -rf "$db" "$rdir"; mkdir -p "$rdir"
    local iv
    if [ "$scen" = "bg1" ]; then iv="60:max_background_jobs=1"; else iv="60:max_background_jobs=1,240:max_background_jobs=8"; fi
    launch "$rdir" "$db" 3600 300 -- --db=$db --out=$rdir/$rid --mode=fill --universe=$U10 \
      --num_ops=$U10 --dist=uniform --order=shuffle --threads=$THREADS --value_size=1024 \
      --write_buffer_size=64M --max_write_buffer_number=4 --level0_trigger=4 \
      --target_file_size_base=64M --max_background_jobs=8 --max_subcompactions=8 \
      --dynamic_level_bytes=1 --cache_size=4G --bloom_bits=10 --compression=none \
      --set_opt="$iv" --sample_ms=500 --layout_ms=2000 --dirscan_ms=10000 --progress_ms=5000 \
      --stats_dump_period_sec=30 --drain_observe_s=60
    local rc=$?; finish_run "$rdir" "$db" 0
    record "$rid" "\"exp\":\"E5-intervention\",\"scenario\":\"$scen\",\"intervention\":\"$iv\",\"rc\":$rc"
  done
}

# Stage G -- E2 arrival order at 50GB: identical inserted key multiset (for monotonic and
# locality, each of which inserts every key exactly once), different arrival order, and
# therefore different L0 file key ranges. This is the mechanism test for H2.
# ---------------------------------------------------------------------------
stage_G() {
  local U50=48000000
  local C; C=$(fill_common)
  local i=0
  for spec in "monotonic:--dist=monotonic --order=identity" \
              "locality:--dist=locality --locality_window=1000000" \
              "zipf:--dist=zipf --zipf_theta=0.99" \
              "hotspot:--dist=hotspot --hot_fraction=0.10 --hot_share=0.80"; do
    i=$((i+1)); wants "$i" || continue
    local name=${spec%%:*}; local dargs=${spec#*:}
    local rid="g_${name}"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    rm -rf "$db"; mkdir -p "$rdir"
    launch "$rdir" "$db" 21600 300 -- --db=$db --out=$rdir/$rid --mode=fill --universe=$U50 \
      --num_ops=$U50 $dargs $C \
      --drain_observe_s=60 --wait_for_drain=1 --wait_for_drain_max_s=900
    local rc=$?; finish_run "$rdir" "$db" 0
    record "$rid" "\"arrival\":\"$name\",\"scale\":\"50gb\",\"rc\":$rc"
  done
}

# ---------------------------------------------------------------------------
# Stage E -- E10: recovery / WAL-retention boundaries (single-threaded so the
# confirmed-op count maps exactly onto a key prefix)
# ---------------------------------------------------------------------------
stage_E() {
  local U=20000000          # ids never wrap before the kill: confirmed set is a prefix
  local i=0
  # arms: kill after a wall-clock delay, after a flush installs, after a compaction
  # installs, with and without WAL fsync
  for spec in "kseal:memtable_sealed:3:0" "kflush:flush_installed:1:0" \
              "kflush_sync:flush_installed:1:1" "kcomp:compaction_installed:1:0" \
              "ktime:time:8:0" "ktime_sync:time:8:1"; do
    i=$((i+1)); wants "$i" || continue
    local name=${spec%%:*}; rest=${spec#*:}; local ev=${rest%%:*}
    rest=${rest#*:}; local n=${rest%%:*}; local sy=${rest##*:}
    local rid="e_$name"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    rm -rf "$db"; mkdir -p "$rdir"
    local kargs=""
    case "$ev" in
      flush_installed|compaction_installed|memtable_sealed) kargs="--kill_at_event=$ev --kill_after_event_n=$n" ;;
      time) kargs="--kill_at_s=$n" ;;
    esac
    launch "$rdir" "$db" 1800 300 -- --db=$db --out=$rdir/$rid --mode=fill --universe=$U \
      --num_ops=20000000 --dist=monotonic --order=identity --threads=1 --value_size=1024 \
      --write_buffer_size=16M \
      --max_write_buffer_number=4 --level0_trigger=4 --target_file_size_base=32M \
      --max_background_jobs=4 --max_subcompactions=4 --dynamic_level_bytes=1 \
      --cache_size=1G --bloom_bits=10 --compression=none --sync=$sy \
      --sample_ms=500 --layout_ms=2000 --dirscan_ms=5000 --progress_ms=5000 \
      --stats_dump_period_sec=30 $kargs
    local rc=$?
    local killed; killed=$(python3 - "$rdir/$rid.summary.json" <<'PY' 2>/dev/null || echo -1
import json,sys
try:
    print(json.load(open(sys.argv[1]))["killed_at_ops"])
except Exception:
    print(-1)
PY
)
    # snapshot the WAL set BEFORE the recovery open (it is consumed by recovery)
    { echo "### WAL before recovery open:"; ls -la "$db"/*.log 2>/dev/null | wc -l;
      du -cb "$db"/*.log 2>/dev/null | tail -1; } > "$rdir/recovery_pre.txt"
    # recovery open (measures open time; the LOG records WAL replay)
    "$BIN" --db=$db --out=$rdir/$rid.reopen --mode=open --universe=$U --use_existing_db=1 \
      --cache_size=1G --stats_dump_period_sec=0 > "$rdir/reopen.log" 2>&1
    cp "$db/LOG" "$rdir/reopen.LOG" 2>/dev/null
    # durability check: with threads=1 keys are written in op order, so the set of
    # keys that MUST exist is exactly [0, killed_at_ops)
    local vlog=$rdir/$rid.verify.log
    "$BIN" --db=$db --out=$rdir/$rid.verify --mode=verify --universe=$U --verify_step=1 \
      --verify_from=0 --verify_to=$((killed > 0 ? killed : 0)) --verify_salt=1 > "$vlog" 2>&1
    local vrc=$?
    local vlog2=$rdir/$rid.verify_unconfirmed.log
    "$BIN" --db=$db --out=$rdir/$rid.verify_unconfirmed --mode=verify --universe=$U --verify_step=1 \
      --verify_from=$((killed > 0 ? killed : 0)) --verify_to=$U --verify_salt=1 > "$vlog2" 2>&1
    local vrc2=$?
    finish_run "$rdir" "$db" 0
    record "$rid" "\"exp\":\"E10-recovery\",\"kill_event\":\"$ev\",\"delay_or_n\":$n,\"sync\":$sy,\"killed_at_ops\":$killed,\"rc\":$rc,\"verify_confirmed_rc\":$vrc,\"verify_unconfirmed_rc\":$vrc2"
  done
}

# ---------------------------------------------------------------------------
# Stage H -- E5 targeted interventions (causality, not correlation)
#   H1: throttle the background service mid-run. max_background_jobs lives in DBOptions,
#       so it must go through SetDBOptions; the driver tries that first and records which
#       API succeeded in the `set_option` event.
#   H2: lift soft_pending_compaction_bytes_limit to 1 TiB at t=60 on a 50 GB fill. This is
#       the decisive test of H5: if the measured 16 MiB/s write cap (§3.2) is what binds,
#       removing the limit should restore the write rate; if the rate stays put, the cap was
#       not binding and the service rate is the real limit. Control arms are the three
#       unmodified 50 GB fills (50gb_fill_r1/r11/r12, same configuration).
# ---------------------------------------------------------------------------
stage_H() {
  local U10=9600000 U50=48000000
  local C; C=$(fill_common)
  local i=0
  for spec in "bg1:20:max_background_jobs=1" \
              "bg1_restore:20:max_background_jobs=1,60:max_background_jobs=8"; do
    i=$((i+1)); wants "$i" || continue
    local name=${spec%%:*}; local iv=${spec#*:}
    local rid="h_${name}"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    rm -rf "$db" "$rdir"; mkdir -p "$rdir"
    launch "$rdir" "$db" 3600 300 -- --db=$db --out=$rdir/$rid --mode=fill --universe=$U10 \
      --num_ops=$U10 --dist=uniform --order=shuffle --threads=$THREADS --value_size=1024 \
      --write_buffer_size=64M --max_write_buffer_number=4 --level0_trigger=4 \
      --target_file_size_base=64M --max_background_jobs=8 --max_subcompactions=8 \
      --dynamic_level_bytes=1 --cache_size=4G --bloom_bits=10 --compression=none \
      --set_opt="$iv" --sample_ms=500 --layout_ms=2000 --dirscan_ms=10000 --progress_ms=5000 \
      --stats_dump_period_sec=30 --drain_observe_s=90
    local rc=$?; finish_run "$rdir" "$db" 0
    record "$rid" "\"exp\":\"E5-intervention\",\"scenario\":\"$name\",\"intervention\":\"$iv\",\"rc\":$rc"
  done
  i=$((i+1))
  if wants "$i"; then
    local rid="h_lift_pending_limit"
    local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    rm -rf "$db" "$rdir"; mkdir -p "$rdir"
    launch "$rdir" "$db" 21600 300 -- --db=$db --out=$rdir/$rid --mode=fill --universe=$U50 \
      --num_ops=$U50 --dist=uniform --order=shuffle \
      --set_opt=60:soft_pending_compaction_bytes_limit=1099511627776 $C \
      --drain_observe_s=120 --wait_for_drain=1 --wait_for_drain_max_s=900
    local rc=$?; finish_run "$rdir" "$db" 0
    record "$rid" "\"exp\":\"E5-intervention\",\"scenario\":\"lift-soft-pending-limit\",\"scale\":\"50gb\",\"rc\":$rc"
  fi
}

case "$STAGE" in
  A) stage_A ;;
  B) stage_B ;;
  C) stage_C ;;
  D) stage_D ;;
  E) stage_E ;;
  F) stage_A ;;
  G) stage_G ;;
  H) stage_H ;;
  status)
    for st in A B C D E F G H; do
      printf "== stage %s ==\n" "$st"
      [ -f "$BENCH/out/$st/manifest.jsonl" ] && python3 -c "
import json,sys
for l in open('$BENCH/out/$st/manifest.jsonl'):
    d=json.loads(l)
    print('  %-22s rc=%s %s' % (d['run_id'], d.get('rc','?'), {k:v for k,v in d.items() if k in ('phase','scale','K','overlap','trigger','exp','subcompactions','jobs','variant','killed_at_ops')}))
" || echo "  (none)"
    done
    echo "free GB: $(free_gb)"
    ;;
  *) echo "unknown stage $STAGE" >&2; exit 2 ;;
esac
