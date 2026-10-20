#!/bin/bash
# L0 backlog campaign: layout x size B x file count K (bench/L0_BACKLOG_PLAN.md).
#
#   bash bench/v2/l0_backlog.sh smoke     # tiny scale, validates the whole chain
#   bash bench/v2/l0_backlog.sh first     # 6 states x 3 experiments x 5 reps = 90 runs
#   bash bench/v2/l0_backlog.sh prep      # (re)build the holey base only
#
# Design points enforced here, from the plan:
#   * the base has HOLES (the tail of the shuffled permutation is never written), because
#     an in-envelope missing query cannot exist on a fully dense base;
#   * the base's L0 and memtable are empty before staging: fill with level0_trigger=1 so
#     every L0 file compacts away by itself, then a second drain pass clears whatever the
#     final flush produced -- verified L0 == 0, and no forced CompactRange is used;
#   * the backlog overwrites EXISTING keys with the value they already hold, so all
#     groups present identical user-visible data; narrow/wide use the same key set at the
#     same B and differ only in grouping;
#   * each experiment (A frozen, B compaction only, C mixed) runs on its OWN copy;
#   * A's single run covers hit / in-envelope missing / out-of-envelope at once, with the
#     per-class counters reported separately.
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
v2_setup_paths

PHASE=${1:-smoke}
LB=$OUT/l0_backlog${V2_LB_SUFFIX:-}
DBDIR=$LB/db
BIN=${LB_BIN:-$BENCH/tools/l0bench_backlog}
[ -x "$BIN" ] || { echo "missing $BIN; run bench/build_backlog.sh" >&2; exit 2; }
mkdir -p "$LB" "$DBDIR" "$LB/logs"
touch "$LB/manifest.jsonl"

case "$PHASE" in
  smoke)
    UNIVERSE=200000; BASE_KEYS=180000
    BSIZES="16777216"            # one size: 16 MiB
    K=4; REPS=1
    WARMUP=4; PERF_S=6; DIAG_S=4
    DRAIN_MAX=300
    ;;
  kscan)
    # §5: fix B ~= 2 GiB and scan K alone. Two separate groups per K: compaction frozen
    # (file selection, Bloom, scans, metadata) and auto-compaction (scheduling and the
    # slowdown=20 / stop=36 thresholds, reported separately from lookup cost).
    # V2_K selects the K for this invocation; the output root is per-K so runs of
    # different K never share a directory.
    UNIVERSE=48000000; BASE_KEYS=44040192
    BSIZES="${V2_BSIZES:-2147483648}"
    K=${V2_K:-1}; REPS=${V2_REPS:-5}
    KINDS="A B"
    KLIST=${V2_KLIST:-1 2 4 8 16}     # the sweep itself; each item carries its own K
    WARMUP=30; PERF_S=60; DIAG_S=20
    DRAIN_MAX=1800
    ;;
  first|prep)
    UNIVERSE=48000000; BASE_KEYS=44040192      # ~44 GiB of 1 KiB values, 8% holes
    BSIZES="536870912 2147483648 8589934592"   # 0.5, 2, 8 GiB
    K=16; REPS=5
    WARMUP=30; PERF_S=60; DIAG_S=20
    DRAIN_MAX=1800
    ;;
  *) echo "usage: l0_backlog.sh smoke|first|prep" >&2; exit 2 ;;
esac
SEED=20260925
BSIZES_PY=${BSIZES// /,}
KLIST=${V2_KLIST:-}                # non-empty = sweep K; each order item carries its own K
KINDS_LIST=$(python3 -c "print(repr('${KINDS:-A B C}'.split()))")          # python tuple body for the ordering shuffle
# KLIST non-empty switches the phase into a K sweep (one K per item); KLIST_PY is the
# python list literal the ordering generator below iterates. Both must be defined before
# the loop even when empty, or `set -u` kills the run.
KLIST_PY=$(python3 -c "print(repr([int(x) for x in '${KLIST:-}'.split()]))")
SNAP=${LB_SNAP:-50}          # which snapshot the holey base derives from (only for disk)

# ---------------------------------------------------------------------------
# 1. the holey base, prepared once
# ---------------------------------------------------------------------------
# The base is shared by every phase and every K: it is ~44 GiB and takes about an hour to
# build, so it lives beside the campaign rather than inside a per-phase output root.
BASE=${LB_BASE:-$OUT/l0_backlog/db/base_holes}
prepare_base() {
  [ -f "$BASE.done" ] && { echo "  reuse $BASE"; return 0; }
  if [ "${V2_DRYRUN:-0}" = "1" ]; then echo "  DRYRUN: would prepare $BASE"; return 0; fi
  rm -rf "$BASE"
  rm -rf "$DBDIR/base_build"
  echo "== preparing the holey base: universe=$UNIVERSE written=$BASE_KEYS =="
  "$BIN" --mode=fill --db="$BASE" --out="$LB/logs/base_build" \
         --universe="$UNIVERSE" --num_ops="$BASE_KEYS" --dist=uniform --order=shuffle \
         --seed="$SEED" --value_size=1024 --threads=16 \
         --write_buffer_size=$(( BASE_KEYS > 1000000 ? 268435456 : 67108864 )) \
         --level0_trigger=1 --disable_auto_compactions=0 --max_background_jobs=8 \
         --wait_for_drain=1 --wait_for_drain_max_s=3600 --drain_stable_samples=2 \
         --final_flush=1 --progress_ms=5000 >> "$LB/logs/base_build.log" 2>&1
  # second pass: the final flush leaves one L0 file; let it compact away naturally
  "$BIN" --mode=open --db="$BASE" --out="$LB/logs/base_settle" \
         --universe="$UNIVERSE" --use_existing_db=1 --level0_trigger=1 --value_size=1024 \
         --threads=1 --wait_for_drain=1 --wait_for_drain_max_s=3600 \
         --drain_stable_samples=2 --drain_require_l0_zero=1 --progress_ms=5000 \
         >> "$LB/logs/base_settle.log" 2>&1
  local line; line=$(grep -E "^DRAIN (HEALTHY|TIMEOUT)" "$LB/logs/base_settle.log" | tail -1)
  echo "  base settle: $line"
  case "$line" in *HEALTHY*) ;; *) echo "base did not reach L0==0; aborting" >&2; return 1 ;; esac
  # the base's own key count and base level, recorded for the report
  "$BIN" --mode=layoutcheck --db="$BASE" --out="$LB/logs/base_check" --layout=narrow \
         --layout_files=1 --universe="$UNIVERSE" --use_existing_db=1 --threads=1 \
         --disable_auto_compactions=1 >> "$LB/logs/base_check.log" 2>&1
  echo "base_l0_empty=1" > "$BASE.done"
  echo "base_keys=$BASE_KEYS universe=$UNIVERSE k=$K seed=$SEED bin_sha256=$(sha256sum "$BIN" | cut -d' ' -f1)" >> "$BASE.done"
  return 0
}

# 2. one initial state per (layout, B, rep), then three independent copies
state_of() { echo "$DBDIR/state_$1_B$2_K${K}_r$3"; }        # <layout> <bytes> <rep>

build_state() {  # <layout> <bytes> <rep> -> echoes the state db
  local layout=$1 bytes=$2 rep=$3
  if [ "${V2_DRYRUN:-0}" = "1" ]; then echo "$(state_of "$layout" "$bytes" "$rep")"; return 0; fi
  local st; st=$(state_of "$layout" "$bytes" "$rep")
  local keys=$(( bytes / 1040 ))
  rm -rf "$st"
  # mkreplica.sh, not `cp -al`: a plain hardlink copy shares the WAL/MANIFEST inodes with
  # the base, and the accumulate run then truncates the base's WAL through that link (the
  # 09-29 matrix shows 002824.log touched at 18:21). SSTs stay hardlinked either way.
  bash "$BENCH/mkreplica.sh" "$BASE" "$st" >&2
  local wb; wb=$(python3 -c "print(max(1<<29, int($keys/$K*(1024+64)*2)))")
  "$BIN" --mode=accumulate --db="$st" --out="$LB/logs/state_${layout}_B${bytes}_r${rep}" \
         --layout="$layout" --layout_files="$K" --universe="$UNIVERSE" \
         --base_written="$BASE_KEYS" --backlog_keys="$keys" --value_size=1024 \
         --disable_auto_compactions=1 --level0_stop=100000 --write_buffer_size="$wb" \
         --use_existing_db=1 --threads=1 --dist=uniform --order=shuffle --seed="$SEED" \
         --progress_ms=5000 > "$LB/logs/state_${layout}_B${bytes}_r${rep}.log" 2>&1
  local rc=$?
  local line; line=$(grep -E "^ACCUMCHECK" "$LB/logs/state_${layout}_B${bytes}_r${rep}.log" | tail -1)
  # stdout of this function is the STATE PATH and nothing else: the caller captures it
  # with $( ), so a log line here would become part of the path
  echo "  state $layout B=$bytes rep=$rep rc=$rc :: $line" | tee -a "$LB/logs/states.log" >&2
  case "$line" in *"ok=1"*"base_l0_files=0"*) ;; *) echo "  state build not clean" >&2; return 1 ;; esac
  echo "$st"
}

copy_state() {  # <state> <suffix> -> echoes the copy path
  local st=$1 suffix=$2
  local dst="${st}_$suffix"
  rm -rf "$dst"
  bash "$BENCH/mkreplica.sh" "$st" "$dst" >&2
  echo "$dst"
}

run_experiment() {  # <state_copy> <A|B|C> <layout> <bytes> <rep> <rate>
  local db=$1 kind=$2 layout=$3 bytes=$4 rep=$5 rate=$6
  local rid="lb_${kind}_${layout}${V2_RID_K:-}_B${bytes}_r${rep}"
  local common=(--db="$db" --out="$LB/runs/$rid" --universe="$UNIVERSE" --value_size=1024
                --use_existing_db=1 --threads=1)
  mkdir -p "$LB/runs"
  case "$kind" in
    A)  # frozen compaction, read only: one run covers all three query classes
        "$BIN" --mode=read "${common[@]}" --layout="$layout" --layout_files="$K" \
               --bloom_bits=10 --base_written="$BASE_KEYS" \
               --l0_keys=$(( bytes / 1040 / K * K )) \
               --read_class=mixed --class_weights=hit:0.4,missing_inrange:0.4,missing_outrange:0.2 \
               --disable_auto_compactions=1 --level0_stop=100000 --cache_size=8G \
               --warmup_s="$WARMUP" --perf_window_s="$PERF_S" --diag_window_s="$DIAG_S" \
               --progress_ms=10000 --seed="$SEED" > "$LB/runs/$rid.run.log" 2>&1 ;;
    B)  # compaction enabled, no foreground load: settle and report what it cost
        "$BIN" --mode=open "${common[@]}" --cache_size=8G --level0_trigger=8 \
               --wait_for_drain=1 --wait_for_drain_max_s="$DRAIN_MAX" \
               --drain_stable_samples=4 --progress_ms=5000 --seed="$SEED" \
               > "$LB/runs/$rid.run.log" 2>&1 ;;
    C)  # business load (90% get / 10% put) at a common absolute rate, then settle
        "$BIN" --mode=mix --read_pct=90 "${common[@]}" --cache_size=8G --level0_trigger=8 \
               --arrival=open --rate="$rate" --warmup_s="$WARMUP" --measure_s="$PERF_S" \
               --wait_for_drain=1 \
               --wait_for_drain_max_s="$DRAIN_MAX" --drain_stable_samples=4 \
               --progress_ms=10000 --seed="$SEED" > "$LB/runs/$rid.run.log" 2>&1 ;;
  esac
  local rc=$?
  "$BIN" --mode=verify --db="$db" --out="$LB/runs/$rid.verify" --universe="$UNIVERSE" \
         --use_existing_db=1 --verify_step=99992 --verify_salt=1 --value_size=1024 \
         --threads=1 --cache_size=8G --disable_auto_compactions=1 \
         --level0_stop=100000 > "$LB/runs/$rid.verify.log" 2>&1
  echo "verify_rc=$?" >> "$LB/runs/$rid.verify.log"
  # experiment B is a settling run with no foreground window by design: the cohort checks
  # do not apply to it (everything else in the gate still does)
  local extra=()
  [ "$kind" = "B" ] && extra=(--no-cohort)
  python3 "$BENCH/v2/check_run.py" --run-dir "$LB/runs" --run-id "$rid" --rc "$rc" --verify \
          "${extra[@]}" > "$LB/runs/$rid.validity.txt" 2>&1 || true
  printf '{"run_id":"%s","kind":"%s","layout":"%s","backlog_bytes":%s,"K":%s,"rep":%s,"rc":%s,"bin_sha256":"%s","start":"%s"}\n' \
    "$rid" "$kind" "$layout" "$bytes" "$K" "$rep" "$rc" "$(sha256sum "$BIN" | cut -d' ' -f1)" \
    "$(date -Is)" >> "$LB/manifest.jsonl"
  local ok=0; grep -q '^VALID=1$' "$LB/runs/$rid.validity.txt" && ok=1
  printf '  %-28s rc=%s valid=%s\n' "$rid" "$rc" "$ok"
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
prepare_base || exit 1
[ "$PHASE" = prep ] && exit 0

# a common arrival rate for experiment C: measured once on the UN-backlogged base, so
# every group faces the same absolute rate instead of "80% of its own capacity"
RATE_FILE=$OUT/l0_backlog/common_rate.txt
if [ "${V2_DRYRUN:-0}" = "1" ]; then
  RATE=$(cat "$RATE_FILE" 2>/dev/null || echo 100000)
else
  RATE=""
fi
if [ "${V2_DRYRUN:-0}" = "1" ]; then
  echo "$RATE" > /dev/null 2>&1 || true
fi
if [ "${V2_DRYRUN:-0}" != "1" ] && [ ! -f "$RATE_FILE" ]; then
  echo "== measuring the common business rate on the un-backlogged base =="
  cp_rate_db=$(copy_state "$BASE" ratebase)
  "$BIN" --mode=mix --read_pct=90 --db="$cp_rate_db" --out="$LB/logs/ratebase" \
         --universe="$UNIVERSE" --value_size=1024 --use_existing_db=1 --threads=1 \
         --cache_size=8G --level0_trigger=8 --warmup_s="$WARMUP" --measure_s="$PERF_S" \
         --progress_ms=10000 --seed="$SEED" > "$LB/logs/ratebase.log" 2>&1
  echo "$(python3 -c "
import json
print(int(json.load(open('$LB/logs/ratebase.measure_phase.json'))['throughput_service_ops_s'] * 0.5))")" > "$RATE_FILE"
  rm -rf "$cp_rate_db"
fi
[ -n "$RATE" ] || RATE=$(cat "$RATE_FILE")
echo "common business rate for experiment C: $RATE ops/s (0.5x the base's mixed ceiling)"

# V2_BACKLOG_ONLY_REP=<n> runs a single rep: useful when a rep has to be redone without
# invalidating the ones already on disk.
REP_LIST=${V2_BACKLOG_ONLY_REP:-$(seq 1 "$REPS")}
for rep in $REP_LIST; do
  echo "-- rep $rep/$REPS --"
  # paired interleaving: shuffle (state x experiment) inside the rep
  mapfile -t ORDER < <(python3 -c "
import random
items=[]
for layout in ('narrow','wide'):
    for b in (${BSIZES_PY},):
        for kind in ${KINDS_LIST}:
            for k in (${KLIST_PY} if '${KLIST:-}' else (0,)):
                items.append(f'{layout} {b} {kind} {k}')
random.Random(20260925+$rep).shuffle(items)
for i in items: print(i)")
  # `declare -A BUILT` on an already-declared array does NOT clear it, so without this
  # reps 2..N reuse the previous rep's state paths -- which were deleted at the end of
  # that rep, leaving every later run pointing at a database that no longer exists.
  unset BUILT; declare -A BUILT
  if [ "${V2_DRYRUN:-0}" = "1" ]; then
    for item in "${ORDER[@]}"; do read -r l b kd kk <<< "$item"; echo "  DRYRUN item: $item"; done
    continue
  fi
  for item in "${ORDER[@]}"; do
    read -r layout bytes kind kk <<< "$item"
    if [ -n "${KLIST:-}" ]; then K=$kk; V2_RID_K="_K$kk"; fi
    key="$layout $bytes $K"
    if [ -z "${BUILT[$key]:-}" ]; then
      st=$(build_state "$layout" "$bytes" "$rep") || { echo "  state failed; skipping $kind" >&2; continue; }
      BUILT[$key]="$st"
    fi
    st=${BUILT[$key]}
    cdb=$(copy_state "$st" "$kind") || continue
    run_experiment "$cdb" "$kind" "$layout" "$bytes" "$rep" "$RATE"
    rm -rf "$cdb"         # the run's evidence is on disk; the database is not needed
  done
  # the state itself can go once its three experiments are done
  for key in "${!BUILT[@]}"; do rm -rf "${BUILT[$key]}"; done
done

echo
echo "analysis: python3 $BENCH/v2/l0_backlog_analyze.py --out $LB --charts $LB/charts"
