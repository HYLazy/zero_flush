#!/bin/bash
# bn_campaign.sh -- bottleneck campaign (native RocksDB 11.2, no source patches).
#
# Implements the service-capacity intervention plan (瓶颈判定):
#   §四.1 Flush capacity scan        : flush worker count at fixed compaction capacity
#   §四.2/四.3 compaction scan       : LOW-pool (compaction) worker count at fixed flush
#   §五   2D matrix Flush x L0->base : first-round cells incl. both-low / both-high
#   §六   isolation                   : flush cost vs memtable size; L0->base fixed-input replay
#
# Per-stage capacity control uses PUBLIC options only: max_background_flushes sizes the
# HIGH pool, max_background_compactions sizes the LOW pool (GetBGJobLimits explicit
# branch). Default config derives flush=max(1,jobs/4)=2, compaction=jobs-2=6 from
# max_background_jobs=8.
#
# Usage:
#   bench/bn_campaign.sh base                 # rebuild bench/base/base50 (fill+settle+replica)
#   bench/bn_campaign.sh scan [arm ...]       # capacity scan + matrix, 5 reps interleaved
#   bench/bn_campaign.sh iso_flush            # flush isolation (memtable size sweep, no compaction)
#   bench/bn_campaign.sh iso_l0base           # L0->base fixed-input replay (frozen replica drain)
#   bench/bn_campaign.sh status               # what has completed
# Env: THREADS WB CACHE KEEP_DB DRY FREE_MIN_GB REPS (default 5)
set -uo pipefail

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
REPS=${REPS:-5}

STAGE=I
SUB=${1:-status}
shift || true
ONLY=("$@")
OUT=$BENCH/out/$STAGE
mkdir -p "$OUT"
LOG=$OUT/bn_campaign.log
BASE=$BENCH/base/base50
U50=48000000
UPDATES=12800000        # 12.8M updates = 12.4 GiB confirmed per arm (same trace as stage C)

free_gb() { df -BG --output=avail "$ROOT" | tail -1 | tr -dc '0-9'; }

record() {   # record <run_id> <extra-json-fields>
  local rid=$1 extra=$2
  printf '{"stage":"%s","run_id":"%s","ts":"%s","host":"%s","bin_md5":"%s","git_head":"%s",%s}\n' \
    "$STAGE" "$rid" "$(date '+%F %T')" "$(hostname)" \
    "$(md5sum $BIN | cut -d' ' -f1)" "$(git -C $ROOT rev-parse --short HEAD 2>/dev/null)" \
    "$extra" >> "$OUT/manifest.jsonl"
}

done_rc() { # done_rc <run_id> -> rc recorded in manifest, else ""
  grep -F "\"run_id\":\"$1\"" "$OUT/manifest.jsonl" 2>/dev/null | tail -1 | grep -o '"rc":[0-9]*' | cut -d: -f2
}

wants() { local i=$1; [ ${#ONLY[@]} -eq 0 ] && return 0
  for x in "${ONLY[@]}"; do [ "$x" = "$i" ] && return 0; done; return 1; }

# launch <rdir> <dbdir> <timeout_s> <stall_secs> -- <l0bench args>  (same watchdog as campaign.sh)
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
      echo "### DB_BEFORE bytes=$(du -sb "$dbdir" 2>/dev/null | cut -f1)"
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
    if [ "${ops:-}" = "${last_ops}" ] && [ "$cpu" = "$last_cpu" ]; then
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
      pkill -KILL -s "$pid" 2>/dev/null; echo "TIMEOUT" >> "$log"; return 92
    fi
    if [ $((waited % 300)) -eq 0 ]; then echo "### alive ${waited}s freeGB=$fg ops=${ops:-?}" >> "$log"; fi
  done
  wait "$pid"; local rc=$?
  echo "### END $(date '+%F %T') rc=$rc freeGB=$(free_gb)" >> "$log"
  return $rc
}

finish_run() { # finish_run <rdir> <dbdir> [keep]
  local rdir=$1 dbdir=$2 keep=${3:-$KEEP_DB}
  [ -f "$dbdir/LOG" ] && cp "$dbdir/LOG" "$rdir/db_close.LOG"
  [ -d "$dbdir" ] && ls -la "$dbdir" > "$rdir/dir_after.txt" 2>&1
  if [ "$keep" != "1" ] && [ -d "$dbdir" ]; then rm -rf "$dbdir"; fi
}

# ---------------------------------------------------------------------------
# base: rebuild bench/base/base50 (settled 50 GiB fill under primary config P)
# ---------------------------------------------------------------------------
cmd_base() {
  local rid=bn_base_fill; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
  rm -rf "$db"; mkdir -p "$rdir"
  launch "$rdir" "$db" 21600 300 -- --db=$db --out=$rdir/$rid --mode=fill --universe=$U50 \
    --num_ops=$U50 --dist=uniform --order=shuffle \
    --threads=$THREADS --value_size=1024 --write_buffer_size=$WB --max_write_buffer_number=4 \
    --level0_trigger=8 --level0_slowdown=20 --level0_stop=36 --target_file_size_base=64M \
    --max_background_jobs=8 --max_subcompactions=8 --dynamic_level_bytes=1 \
    --cache_size=$CACHE --bloom_bits=10 --compression=none --stats_dump_period_sec=60 \
    --sample_ms=500 --layout_ms=5000 --dirscan_ms=10000 --progress_ms=15000 \
    --drain_observe_s=120 --wait_for_drain=1 --wait_for_drain_max_s=1800 \
    --final_flush=1 --final_flush_observe_s=60
  local rc=$?
  finish_run "$rdir" "$db" 1          # KEEP: it is the base for every replica below
  record "$rid" "\"exp\":\"bn-base\",\"phase\":\"fill\",\"scale\":\"50gb\",\"universe\":$U50,\"rc\":$rc"
  if [ "$rc" != "0" ]; then echo "base fill rc=$rc -- aborting" >&2; return 1; fi
  # snapshot the settled layout as the shared base
  local srid=bn_base_snapshot; local srdir=$OUT/$srid
  mkdir -p "$srdir" "$BENCH/base"; rm -rf "$BASE"
  bash "$MKREP" "$db" "$BASE" > "$srdir/replica.log" 2>&1
  local rrc=$?
  "$BIN" --db=$BASE --out=$srdir/$srid.verify --mode=verify --universe=$U50 \
    --verify_step=4999 --verify_salt=1 > "$srdir/verify.log" 2>&1
  local vrc=$?
  record "$srid" "\"replica_rc\":$rrc,\"verify_rc\":$vrc"
  echo "base ready: replica_rc=$rrc verify_rc=$vrc"
}

# ---------------------------------------------------------------------------
# scan: capacity scan + 2D matrix on identical replicas, identical trace.
#   arm spec: name:flushes:compactions:subcompactions  (flushes/compactions -1 = unset)
# Interleaved rep-major with rotating order (计划 §一: 交错运行顺序).
# ---------------------------------------------------------------------------
ARMS=(
  "default:-1:-1:8"
  "x_f2c6:2:6:8"
  "f1:1:6:8"
  "f4:4:6:8"
  "c2:2:2:8"
  "c12:2:12:8"
  "f1c12:1:12:8"
  "f4c2:4:2:8"
  "f4c12s16:4:12:16"
  "f1c2:1:2:8"
  "s1:2:6:1"
  "s16:2:6:16"
)
arm_reps() { case "$1" in x_f2c6|s1|s16) echo 3 ;; *) echo "$REPS" ;; esac; }

run_one() { # run_one <arm> <flushes> <compactions> <subcompactions> <rep>
  local arm=$1 F=$2 C=$3 SC=$4 rep=$5
  local rid="bn_${arm}_r${rep}"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
  local prev; prev=$(done_rc "$rid")
  [ -n "$prev" ] && { echo "== $rid already done (rc=$prev), skip"; return 0; }
  [ -d "$BASE" ] || { echo "### missing base $BASE -- run 'bn_campaign.sh base' first" >&2; return 9; }
  rm -rf "$db" "$rdir"; mkdir -p "$rdir"
  bash "$MKREP" "$BASE" "$db" > "$rdir/replica.log" 2>&1 || { echo "replica failed: $rid"; return 8; }
  local capargs=""
  [ "$F" != "-1" ] && capargs="$capargs --max_background_flushes=$F"
  [ "$C" != "-1" ] && capargs="$capargs --max_background_compactions=$C"
  echo "== $rid start $(date '+%F %T')"
  launch "$rdir" "$db" 14400 600 -- --db=$db --out=$rdir/$rid --mode=overwrite --universe=$U50 \
    --num_ops=$UPDATES --use_existing_db=1 --dist=uniform --order=shuffle \
    --threads=$THREADS --value_size=1024 --write_buffer_size=$WB --max_write_buffer_number=4 \
    --level0_trigger=8 --level0_slowdown=20 --level0_stop=36 --target_file_size_base=64M \
    --max_background_jobs=8 --max_subcompactions=$SC --dynamic_level_bytes=1 $capargs \
    --cache_size=$CACHE --bloom_bits=10 --compression=none --stats_dump_period_sec=60 \
    --sample_ms=500 --layout_ms=5000 --dirscan_ms=10000 --progress_ms=15000 \
    --drain_observe_s=60 --wait_for_drain=1 --wait_for_drain_max_s=300
  local rc=$?
  # preserve the workload-phase DB LOG BEFORE the verify pass opens (and rotates) it;
  # the verify opener renames LOG -> LOG.old.*, so grab both.
  for lf in "$db/LOG" "$db"/LOG.old.*; do
    [ -f "$lf" ] && cp "$lf" "$rdir/$(basename "$lf").workload" 2>/dev/null
  done
  [ -f "$db/LOG" ] && cp "$db/LOG" "$rdir/db.LOG"
  local vlog=$rdir/$rid.verify.log
  "$BIN" --db=$db --out=$rdir/$rid.verify --mode=verify --universe=$U50 --verify_step=19997 \
    --verify_salt=1 > "$vlog" 2>&1; local vrc=$?
  finish_run "$rdir" "$db" 0
  record "$rid" "\"exp\":\"bn-scan\",\"arm\":\"$arm\",\"flushes\":$F,\"compactions\":$C,\"subcompactions\":$SC,\"rep\":$rep,\"updates\":$UPDATES,\"rc\":$rc,\"verify_rc\":$vrc"
  echo "== $rid end rc=$rc verify_rc=$vrc ($(date '+%F %T'))"
  return 0
}

cmd_scan() {
  local maxrep=0 a r
  for a in "${ARMS[@]}"; do a=${a%%:*}; local n; n=$(arm_reps "$a"); [ "$n" -gt "$maxrep" ] && maxrep=$n; done
  # Interleave across arms, rotating the start offset each rep (计划 §一: 交错运行顺序).
  for ((r=1; r<=maxrep; r++)); do
    local n_arms=${#ARMS[@]}
    for ((j=0; j<n_arms; j++)); do
      local idx=$(( (j + r - 1) % n_arms ))
      local a=${ARMS[$idx]}
      local name=${a%%:*}; local rest=${a#*:}
      local F=${rest%%:*}; rest=${rest#*:}
      local C=${rest%%:*}; local SC=${rest#*:}
      local n; n=$(arm_reps "$name"); [ "$r" -le "$n" ] || continue
      run_one "$name" "$F" "$C" "$SC" "$r"
    done
  done
}

# ---------------------------------------------------------------------------
# iso_flush: flush cost in isolation (计划 §六). Single writer thread,
# auto compactions OFF, L0 trigger far away: the only background work is flush.
# Sweeping write_buffer_size changes flush input size; the WB=8G arm performs
# almost no flush on a 10 GB universe and gives the writer-only CPU baseline.
# ---------------------------------------------------------------------------
cmd_iso_flush() {
  local U10=9600000
  local i=0
  for spec in "wb64m:64M" "wb256m:256M" "wb1g:1G" "wb8g:8G"; do
    i=$((i+1)); wants "$i" || continue
    local name=${spec%%:*} WBSize=${spec#*:}
    local rid="bn_isoflush_${name}"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    local prev; prev=$(done_rc "$rid"); [ -n "$prev" ] && { echo "== $rid done (rc=$prev), skip"; continue; }
    rm -rf "$db" "$rdir"; mkdir -p "$rdir"
    launch "$rdir" "$db" 7200 300 -- --db=$db --out=$rdir/$rid --mode=fill --universe=$U10 \
      --num_ops=$U10 --dist=uniform --order=shuffle --threads=1 --value_size=1024 \
      --write_buffer_size=$WBSize --max_write_buffer_number=4 \
      --level0_trigger=1000 --level0_slowdown=2000 --level0_stop=4000 \
      --disable_auto_compactions=1 --target_file_size_base=64M \
      --max_background_jobs=8 --max_subcompactions=8 --dynamic_level_bytes=1 \
      --cache_size=$CACHE --bloom_bits=10 --compression=none --stats_dump_period_sec=60 \
      --sample_ms=200 --layout_ms=5000 --dirscan_ms=10000 --progress_ms=5000 \
      --drain_observe_s=10
    local rc=$?
    finish_run "$rdir" "$db" 0
    record "$rid" "\"exp\":\"bn-iso-flush\",\"wb\":\"$WBSize\",\"universe\":$U10,\"rc\":$rc"
    echo "== $rid rc=$rc"
  done
}

# ---------------------------------------------------------------------------
# iso_l0base: L0->base cost by fixed-input replay (计划 §六). On a frozen
# replica, construct K L0 files over the SAME keys (wide vs narrow ranges),
# then re-enable auto compaction and let exactly this backlog drain; per-job
# records (inputs/bytes/wall/cpu/subcompactions) give the replay cost.
# ---------------------------------------------------------------------------
cmd_iso_l0base() {
  local KPF=131072
  local i=0
  for spec in "k8narrow:8:narrow:8" "k8wide:8:wide:8" "k8narrow_sc1:8:narrow:1" "k16wide:16:wide:8" "k16wide_sc1:16:wide:1"; do
    i=$((i+1)); wants "$i" || continue
    local name=$(echo "$spec" | cut -d: -f1) K=$(echo "$spec" | cut -d: -f2) OV=$(echo "$spec" | cut -d: -f3) SC=$(echo "$spec" | cut -d: -f4)
    local rid="bn_isol0_${name}"; local db=$BENCH/db/$rid; local rdir=$OUT/$rid
    local prev; prev=$(done_rc "$rid"); [ -n "$prev" ] && { echo "== $rid done (rc=$prev), skip"; continue; }
    [ -d "$BASE" ] || { echo "### missing base $BASE" >&2; return 9; }
    rm -rf "$db" "$rdir"; mkdir -p "$rdir"
    bash "$MKREP" "$BASE" "$db" > "$rdir/replica.log" 2>&1 || continue
    # 1) frozen construction of the identical K-file recent layer
    launch "$rdir" "$db" 3600 0 -- --db=$db --out=$rdir/$rid.build --mode=backlog --universe=$U50 \
      --use_existing_db=1 --disable_auto_compactions=1 --backlog_files=$K \
      --backlog_keys_per_file=$KPF --backlog_overlap=$OV \
      --write_buffer_size=512M --level0_trigger=1000 --cache_size=$CACHE --layout_ms=2000 \
      --sample_ms=500 --dirscan_ms=5000 --progress_ms=10000
    local rc1=$?
    # 2) drain exactly this backlog: open with auto compactions ENABLED (default).
    #    The scheduler sees L0=K>=trigger and runs the L0->base jobs immediately;
    #    no foreground writes, so every job works on the fixed constructed input.
    #    mode=flush just nudges one final memtable flush before the drain wait.
    launch "$rdir" "$db" 7200 900 -- --db=$db --out=$rdir/$rid.drain --mode=flush --universe=$U50 \
      --use_existing_db=1 \
      --max_background_flushes=2 --max_background_compactions=6 --max_subcompactions=$SC \
      --write_buffer_size=$WB --level0_trigger=8 --cache_size=$CACHE --sample_ms=500 \
      --layout_ms=5000 --dirscan_ms=10000 --progress_ms=10000 \
      --drain_observe_s=30 --wait_for_drain=1 --wait_for_drain_max_s=3600
    local rc2=$?
    finish_run "$rdir" "$db" 0
    record "$rid" "\"exp\":\"bn-iso-l0base\",\"K\":$K,\"overlap\":\"$OV\",\"subcompactions\":$SC,\"build_rc\":$rc1,\"drain_rc\":$rc2"
    echo "== $rid build_rc=$rc1 drain_rc=$rc2"
  done
}

cmd_status() {
  for f in "$OUT"/manifest.jsonl; do
    [ -f "$f" ] || continue
    echo "== stage I =="
    python3 - "$f" <<'EOF'
import json, sys
for line in open(sys.argv[1]):
    d = json.loads(line)
    print(f"  {d['run_id']:<28} rc={d.get('rc', d.get('drain_rc', '?'))} "
          f"arm={d.get('arm', d.get('exp', ''))} rep={d.get('rep', '')}")
EOF
  done
}

case "$SUB" in
  base) cmd_base ;;
  scan) cmd_scan ;;
  iso_flush) cmd_iso_flush ;;
  iso_l0base) cmd_iso_l0base ;;
  status) cmd_status ;;
  *) echo "usage: $0 base|scan|iso_flush|iso_l0base|status"; exit 2 ;;
esac
