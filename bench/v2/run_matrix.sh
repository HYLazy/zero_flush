#!/bin/bash
# §5 experiment matrix. One script, one arm runner: every arm is a fresh replica
# of a frozen snapshot, every run is gated by check_run.py, and the rep loop is
# rep-major so drift shows up as between-rep variance instead of being confounded
# with the arm variable.
#
#   bash bench/v2/run_matrix.sh smoke  m1      # end-to-end pipeline check, minutes
#   bash bench/v2/run_matrix.sh formal m1      # the runbook's parameters, hours
#   bash bench/v2/run_matrix.sh formal all
#
# Targets: m1 pure-write offered-rate sweep, m2 pure point reads, m3 mixed
# read/write, m4 L0 geometry at a fixed working set, m5 threads/scan/cache.
set -uo pipefail

# ---------------------------------------------------------------------------
# Immunity to mid-flight edits. bash reads a script incrementally, so editing this
# file while it runs can make it resume at a spliced byte offset and execute arbitrary
# text -- observed 2026-09-28, when a running M2 matrix jumped into the M1 sweep after
# run_matrix.sh was edited ("line 336: t: command not found"). The runbook forbids
# editing scripts during a campaign; this makes the rule structural instead of
# relying on discipline: the run continues from a private copy taken at startup.
# ---------------------------------------------------------------------------
if [ "${V2_SELF_FROZEN:-0}" != "1" ]; then
  export V2_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  V2_SELF_DIR=$(mktemp -d "${TMPDIR:-/tmp}/v2run.XXXXXX")
  cp -p "$(dirname "${BASH_SOURCE[0]}")/lib.sh" "$V2_SELF_DIR/" 2>/dev/null || true
  cp -p "${BASH_SOURCE[0]}" "$V2_SELF_DIR/$(basename "${BASH_SOURCE[0]}")"
  export V2_SELF_FROZEN=1
  exec bash "$V2_SELF_DIR/$(basename "${BASH_SOURCE[0]}")" "$@"
fi
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
v2_setup_paths

MODE=${1:-smoke}
TARGET=${2:-m1}

# The "default" configuration (runbook §0): identical to the §4.1 snapshot build.
# NOTE: this list deliberately excludes anything an arm sweeps (--threads,
# --cache_size). The driver assigns on every occurrence, so a trailing default would
# silently replace the arm's own value -- which is how the first M2 pass collapsed both
# sweeps into "16 threads, 8 GiB" (219 configs, caught by the duplicate-flag scan).
# Arms that want the default state it themselves.
OPT_COMMON=(
  --value_size=1024 --write_buffer_size=256M
  --max_write_buffer_number=4 --level0_trigger=8 --level0_slowdown=20 --level0_stop=36
  --target_file_size_base=64M --max_background_jobs=8 --max_subcompactions=8
  --dynamic_level_bytes=1 --bloom_bits=10 --compression=none
  --stats_dump_period_sec=60 --sample_ms=500 --layout_ms=5000 --dirscan_ms=10000
  --progress_ms=1000 --sync=0 --seed=20260925
  --detail_rate=0.001 --perf_detail=1 --detail_sampling=random
)

case "$MODE" in
  smoke)
    SNAP=${V2_SNAP:-smoke}
    REPS=${V2_REPS:-2}
    WARMUP=${V2_WARMUP:-3}; MEASURE=${V2_MEASURE:-8}
    UNIVERSE=200000
    M4_REGION=${V2_M4_REGION:-32768}
    SCAN_LENS=(100 1000)
    THREADS_SET=(1 4 16)
    RATES="0.25 0.50 0.75 0.90 1.00 1.10"
    NEED_GB=5
    ;;
  formal)
    SNAP=${V2_SNAP:-50}
    REPS=${V2_REPS:-3}
    WARMUP=30; MEASURE=60
    UNIVERSE=${V2_UNIVERSE:-48000000}
    M4_REGION=2097152
    SCAN_LENS=(1000 10000 100000 1000000)
    THREADS_SET=(1 2 4 8 16 32)
    RATES="0.25 0.50 0.75 0.90 1.00 1.10"
    NEED_GB=200
    ;;
  *) echo "usage: run_matrix.sh smoke|formal m1|m2|m3|m4|m5|all" >&2; exit 2 ;;
esac
echo "mode=$MODE target=$TARGET snapshot=$SNAP reps=$REPS window=${WARMUP}s+${MEASURE}s universe=$UNIVERSE"

if ! v2_disk_gate "$NEED_GB"; then exit 125; fi

# ---------------------------------------------------------------------------
# arm helpers
# ---------------------------------------------------------------------------
# Fresh replica + run, for arms that write (overwrite/mix/backlog).
arm_rw() {  # <stage> <rid> <snap> -- <args...>
  local stage=$1 rid=$2 snap=$3; shift 3; [ "$1" = "--" ] && shift
  local db
  db=$(v2_prep_replica "$snap" "$rid") || return 1
  V2_STAGE="$stage" v2_run "$rid" --db="$db" --use_existing_db=1 --universe="$UNIVERSE" "$@"
  local rc=$?
  # A write arm's replica is single-use: the next arm starts from the frozen snapshot
  # again (§0), so keeping 47 GiB per arm would exhaust the volume for nothing. The
  # run's evidence -- reports, replica identity, manifest line -- is already on disk.
  # Set V2_KEEP_DB=1 to keep the database (e.g. to inspect a failure).
  if [ "${V2_KEEP_DB:-0}" != "1" ]; then
    du -sb "$db" 2>/dev/null | sed "s|^|db_bytes_after_run=|" >> "${db}.replica_check.txt"
    rm -rf "$db"
    echo "db_removed_after_run=1" >> "${db}.replica_check.txt"
  fi
  return $rc
}

# Shared read-only replica, for arms that only read the frozen state.
READ_DB=""
arm_read() {  # <stage> <rid> -- <args...>
  local stage=$1 rid=$2; shift 2; [ "$1" = "--" ] && shift
  if [ -z "$READ_DB" ] || [ ! -d "$READ_DB" ]; then
    READ_DB=$(v2_prep_replica "$SNAP" "readonly_${SNAP}_$$") || return 1
  fi
  V2_STAGE="$stage" v2_run "$rid" --db="$READ_DB" --use_existing_db=1 --universe="$UNIVERSE" "$@"
}

# Every rate in the matrices is a fraction of THIS snapshot's closed-loop ceiling,
# measured on its own fresh replica (runbook §5 M1/M2: rates must not be shared
# across configurations).
CLOSED_LOOP_RATE=""
measure_closed_loop() {  # <stage> <rid> <snap> -- <args...>
  local stage=$1 rid=$2 snap=$3; shift 3; [ "$1" = "--" ] && shift
  arm_rw "$stage" "$rid" "$snap" -- --warmup_s="$WARMUP" --measure_s="$MEASURE" \
         --wait_for_drain=0 --progress_ms=1000 "$@"
  CLOSED_LOOP_RATE=$(v2_baseline_ops_s "$OUT/$stage/$rid.summary.json")
  echo "closed-loop ceiling ($rid) = ${CLOSED_LOOP_RATE:-0} ops/s"
}

# "Online compaction" arms: the background write stream and the read stream MUST be
# the same process. Two processes cannot open one DB directory -- RocksDB takes an
# exclusive LOCK, so the second open fails with "Resource temporarily unavailable"
# (verified on this host) -- and runbook §0 requires one measurement process at a
# time regardless. A 50/50 mix at total offered rate R drives writes at R/2 while
# the reads land in their own per-class histograms, which is exactly the comparison
# the M2/M5 online arms want. The achieved write rate is reported per run, so the
# "0.5x / 1.0x of the knee" label is checkable rather than assumed.
ONLINE_READ_PCT=${V2_ONLINE_READ_PCT:-50}
online_total_rate() {   # <desired_background_write_rate> -> total offered rate
  python3 -c "
w = ${1:-0}
p = $ONLINE_READ_PCT / 100.0
print(int(w / (1.0 - p)) if p < 1.0 else 0)"
}

# ---------------------------------------------------------------------------
# M1: pure-write open-loop offered-rate sweep (first priority)
# ---------------------------------------------------------------------------
run_m1() {
  local stage=m1
  echo "### M1 pure-write offered-rate sweep (overwrite from $SNAP, default config)"
  for rep in $(seq 1 "$REPS"); do
    measure_closed_loop "$stage" "m1_cb_r${rep}" "$SNAP" -- \
      --mode=overwrite --order=shuffle --dist=uniform
    local R=$CLOSED_LOOP_RATE
    if awk -v r="${R:-0}" 'BEGIN{exit !(r < 1000)}'; then echo "closed loop gave ${R}; aborting M1" >&2; return 1; fi
    for fr in $RATES; do
      local rate; rate=$(v2_frac "$R" "$fr")
      local rid="m1_ov_f${fr}_r${rep}"
      arm_rw "$stage" "$rid" "$SNAP" -- --mode=overwrite --order=shuffle --dist=uniform \
        --arrival=open --rate="$rate" --warmup_s="$WARMUP" --measure_s="$MEASURE" \
        --wait_for_drain=0 --threads=16 --cache_size=8G "${OPT_COMMON[@]}"
    done
    # noc ablation: a short causal upper bound, never a deployment candidate (§5 M1)
    arm_rw "$stage" "m1_noc10_r${rep}" "$SNAP" -- --mode=overwrite --order=shuffle --dist=uniform \
      --arrival=open --rate="$(v2_frac "$R" 1.0)" --disable_auto_compactions=1 \
      --warmup_s="$WARMUP" --measure_s="$MEASURE" --wait_for_drain=0 --threads=16 \
      --cache_size=8G "${OPT_COMMON[@]}"
    # empty-DB fill control, measured over the SAME window and offered rate as the
    # overwrite arms: the contrast is "settled snapshot vs empty DB", so the window
    # must not be truncated by an op cap (hence --num_ops=0).
    local fdb; fdb="$(v2_dbroot)/m1_fill_r${rep}"
    V2_STAGE="$stage" v2_run "m1_fill_ctrl_r${rep}" --mode=fill --db="$fdb" \
      --universe="$UNIVERSE" --num_ops=0 --dist=uniform --order=shuffle \
      --arrival=open --rate="$(v2_frac "$R" 1.0)" --warmup_s="$WARMUP" --measure_s="$MEASURE" \
      --wait_for_drain=0 --threads=16 --cache_size=8G "${OPT_COMMON[@]}"
    # the fill control writes a whole universe into a throwaway DB; keep the run's
    # evidence, not the 47 GiB
    [ "${V2_KEEP_DB:-0}" != "1" ] && rm -rf "$fdb"
  done
}

# ---------------------------------------------------------------------------
# M2: pure point reads (second priority). 50 GB first; 100 GB only if stable.
# ---------------------------------------------------------------------------
run_m2() {
  local stage=m2
  echo "### M2 pure point reads (frozen $SNAP snapshot, shared read-only replica)"
  local -A MIX=( [recent]="recent:1" [deep]="deep:1" [negative]="neg:1" [mixed]="recent:0.4,deep:0.4,neg:0.2" )
  for rep in $(seq 1 "$REPS"); do
    # per-thread-count open-loop rates must be normalised against that thread count's
    # own closed-loop baseline (§5 M2), so measure one ceiling per thread count.
    for th in "${THREADS_SET[@]}"; do
      measure_closed_loop "$stage" "m2_cb_t${th}_r${rep}" "$SNAP" -- \
        --mode=read --threads="$th" --cache_size=8G --read_mix="recent:0.4,deep:0.4,neg:0.2"
      local R=$CLOSED_LOOP_RATE
      for cache in 8G 512M 64M; do
        for mix in recent deep negative mixed; do
          arm_read "$stage" "m2_${mix}_t${th}_c${cache}_r${rep}" -- \
            --mode=read --threads="$th" --cache_size="$cache" --read_mix="${MIX[$mix]}" \
            --warmup_s="$WARMUP" --measure_s="$MEASURE" --wait_for_drain=0 "${OPT_COMMON[@]}"
        done
      done
      # online-compaction contrast (reads while a write stream keeps compaction busy)
      local thalf; thalf=$(v2_frac "$R" 0.5)
      arm_rw "$stage" "m2_online50_t${th}_r${rep}" "$SNAP" -- \
        --mode=mix --read_pct="$ONLINE_READ_PCT" --threads="$th" --cache_size=8G \
        --arrival=open --rate="$(online_total_rate "$thalf")" \
        --read_mix="recent:0.4,deep:0.4,neg:0.2" \
        --warmup_s="$WARMUP" --measure_s="$MEASURE" --wait_for_drain=0 "${OPT_COMMON[@]}"
    done
  done
}

# ---------------------------------------------------------------------------
# M3: mixed read/write. Rates are fractions of the M1 write knee (recorded by
# analyze_v2.py in m1/knee.json); falls back to the M1 closed-loop ceiling.
# ---------------------------------------------------------------------------
m3_base_rate() {
  # a dry run reads the knee from the real output root while writing its stubs elsewhere
  local k="${V2_KNEE_LOCATION:-$OUT}/m1/knee.json"
  if [ -f "$k" ]; then
    python3 -c "
import json;d=json.load(open('$k'))
print(int(d.get('knee_ops_s') or d.get('closed_loop_ops_s') or 0))"
  else
    echo 0
  fi
}

run_m3() {
  local stage=m3
  echo "### M3 mixed read/write"
  local base; base=$(m3_base_rate)
  if awk -v b="${base:-0}" 'BEGIN{exit !(b < 1000)}'; then
    echo "no M1 knee available; measuring a closed-loop ceiling instead"
    measure_closed_loop "$stage" "m3_cb_r1" "$SNAP" -- --mode=mix --read_pct=50
    base=$CLOSED_LOOP_RATE
  fi
  echo "M3 base rate = $base ops/s"
  if awk -v b="${base:-0}" 'BEGIN{exit !(b < 1000)}'; then echo "M3 base rate unusable" >&2; return 1; fi
  for rep in $(seq 1 "$REPS"); do
    for rp in 25 50 75; do
      for fr in 0.25 0.50 0.75 1.00; do
        arm_rw "$stage" "m3_r${rp}_f${fr}_r${rep}" "$SNAP" -- --mode=mix --read_pct="$rp" \
          --arrival=open --rate="$(v2_frac "$base" "$fr")" --warmup_s="$WARMUP" \
          --measure_s="$MEASURE" --wait_for_drain=0 --threads=16 --cache_size=8G \
          "${OPT_COMMON[@]}"
      done
      if [ "$rp" = 50 ]; then
        # frozen-compaction contrast at 50/50
        arm_rw "$stage" "m3_r50_f1.00_frozen_r${rep}" "$SNAP" -- --mode=mix --read_pct=50 \
          --arrival=open --rate="$base" --disable_auto_compactions=1 \
          --warmup_s="$WARMUP" --measure_s="$MEASURE" --wait_for_drain=0 --threads=16 \
          --cache_size=8G "${OPT_COMMON[@]}"
      fi
    done
  done
}

# ---------------------------------------------------------------------------
# M4: L0 geometry at a FIXED total working set. Only K, overlap shape and
# compaction state change; the working set M is constant so that "more L0 files"
# is not confounded with "larger recent set".
# ---------------------------------------------------------------------------
# M4's database holds base keys (salt=1) and backlog keys (salt=2), so no single-salt
# strided sweep can pass. Verify the two regions separately; the gate requires every
# pass to be clean.
m4_verify_db() {  # <db> <out-prefix> <log>
  local db=$1 pre=$2 log=$3
  local region_start=$(( UNIVERSE - M4_REGION ))
  {
    echo "== pass 1: base region [0,$region_start) salt=1"
    "$BIN" --mode=verify --db="$db" --out="${pre}.base" --universe="$UNIVERSE" \
           --use_existing_db=1 --verify_step=99992 --verify_salt=1 --verify_to="$region_start" \
           --threads=16 --cache_size=8G --disable_auto_compactions=1 --level0_stop=100000
    echo "base_pass_rc=$?"
    echo "== pass 2: backlog region [$region_start,$UNIVERSE) salt=2"
    "$BIN" --mode=verify --db="$db" --out="${pre}.backlog" --universe="$UNIVERSE" \
           --use_existing_db=1 --verify_step=99992 --verify_salt=2 --verify_from="$region_start" \
           --threads=16 --cache_size=8G --disable_auto_compactions=1 --level0_stop=100000
    echo "backlog_pass_rc=$?"
  } > "$log" 2>&1
}

run_m4() {
  local stage=m4
  echo "### M4 fixed working set M=$M4_REGION keys, K in {1,8,16}, narrow/wide, frozen/online"
  # §5 M4: three screening reps per arm, rep-major like the other matrices
  for rep in $(seq 1 "$REPS"); do
  for K in 1 8 16; do
    local per=$(( M4_REGION / K ))
    for ov in narrow wide; do
      for cg in frozen online; do
        local extra=()
        [ "$cg" = frozen ] && extra=(--disable_auto_compactions=1)
        local rid="m4_k${K}_${ov}_${cg}_r${rep}"
        local db
        db=$(v2_prep_replica "$SNAP" "$rid") || continue
        V2_STAGE="$stage" V2_POST_VERIFY_FN=m4_verify_db V2_GATE_ARGS="--no-cohort" \
          v2_run "${rid}_build" --no-verify --mode=backlog --db="$db" \
          --universe="$UNIVERSE" --use_existing_db=1 --threads=16 --cache_size=8G \
          --backlog_files="$K" --backlog_keys_per_file="$per" --backlog_region="$M4_REGION" \
          --backlog_overlap="$ov" "${OPT_COMMON[@]}" "${extra[@]}"
        for rmix in recent deep; do
          # the backlog build wrote salt=2, so the read arms' frozen-content check must
          # use the same salt or it reports every backlog key as corrupt
          V2_STAGE="$stage" V2_POST_VERIFY_FN=m4_verify_db \
            v2_run "${rid}_read_${rmix}" --no-verify --mode=read --db="$db" --universe="$UNIVERSE" \
            --use_existing_db=1 --threads=16 --cache_size=8G --read_mix="${rmix}:1" \
            --warmup_s="$WARMUP" --measure_s="$MEASURE" --wait_for_drain=0 \
            "${OPT_COMMON[@]}" "${extra[@]}"
        done
        # build+read pair is complete: the geometry DB is single-use
        [ "${V2_KEEP_DB:-0}" != "1" ] && rm -rf "$db"
      done
    done
  done
  done
}

# ---------------------------------------------------------------------------
# M5: threads, scans and cache -- only after M1-M4 have produced stable effects.
# ---------------------------------------------------------------------------
run_m5() {
  local stage=m5
  echo "### M5 thread scaling, scan length/start, cache"
  for rep in $(seq 1 "$REPS"); do
    for th in "${THREADS_SET[@]}"; do
      arm_rw "$stage" "m5_put_t${th}_r${rep}" "$SNAP" -- --mode=overwrite --threads="$th" \
        --order=shuffle --dist=uniform --warmup_s="$WARMUP" --measure_s="$MEASURE" \
        --wait_for_drain=0 "${OPT_COMMON[@]}"
      arm_read "$stage" "m5_get_t${th}_r${rep}" -- --mode=read --threads="$th" \
        --read_mix="recent:0.4,deep:0.4,neg:0.2" --warmup_s="$WARMUP" --measure_s="$MEASURE" \
        --wait_for_drain=0 "${OPT_COMMON[@]}"
    done
    for len in "${SCAN_LENS[@]}"; do
      for st in random sequential; do
        arm_read "$stage" "m5_scan_${len}_${st}_r${rep}" -- --mode=read --read_mix="scan:1" \
          --scan_len="$len" --scan_start="$st" --warmup_s="$WARMUP" --measure_s="$MEASURE" \
          --wait_for_drain=0 --threads=16 --cache_size=8G "${OPT_COMMON[@]}"
      done
    done
    for cache in 8G 512M 64M; do
      arm_read "$stage" "m5_get_c${cache}_r${rep}" -- --mode=read \
        --read_mix="recent:0.4,deep:0.4,neg:0.2" --cache_size="$cache" --threads=16 \
        --warmup_s="$WARMUP" --measure_s="$MEASURE" --wait_for_drain=0 "${OPT_COMMON[@]}"
    done
    # online compaction background at 0.5x and 1.0x the write knee (single process)
    local base; base=$(m3_base_rate)
    if awk -v b="${base:-0}" 'BEGIN{exit !(b >= 1000)}'; then
      for fr in 0.5 1.0; do
        arm_rw "$stage" "m5_online_f${fr}_r${rep}" "$SNAP" -- \
          --mode=mix --read_pct="$ONLINE_READ_PCT" \
          --arrival=open --rate="$(online_total_rate "$(v2_frac "$base" "$fr")")" \
          --read_mix="recent:0.4,deep:0.4,neg:0.2" \
          --warmup_s="$WARMUP" --measure_s="$MEASURE" --wait_for_drain=0 --threads=16 \
          --cache_size=8G "${OPT_COMMON[@]}"
      done
    fi
  done
}

run_one() {
  case "$1" in
    m1) run_m1 ;;
    m2) run_m2 ;;
    m3) run_m3 ;;
    m4) run_m4 ;;
    m5) run_m5 ;;
    *) echo "unknown target $1" >&2; return 2 ;;
  esac
}

rc=0
if [ "$TARGET" = all ]; then
  # runbook §7: only continue past M1/M2 once the P0 tool acceptance and the first
  # two matrices hold up. The order below is that dependency chain.
  for t in m1 m2 m3 m4 m5; do
    run_one "$t" || { echo "=== $t FAILED rc=$? ===" | tee -a "$LOGS/driver.log"; rc=1; break; }
  done
else
  run_one "$TARGET" || rc=$?
fi

echo
echo "analysis:  python3 $BENCH/v2/analyze_v2.py --out $OUT"
echo "matrix exit rc=$rc"
exit $rc
