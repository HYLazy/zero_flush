#!/bin/bash
# L0 overlap x Bloom-cost campaign (bench/LAYOUT_BLOOM_PLAN.md).
#
#   bash bench/v2/layout_bloom.sh first     # 80 runs: the first-round matrix + controls
#   bash bench/v2/layout_bloom.sh extend    # K=8/bits=10 and K=16/bits=16 (40 runs)
#   bash bench/v2/layout_bloom.sh build     # (re)build the layout databases only
#
# Properties the plan fixes, all enforced here:
#   * L0-only database, M = 2,097,152 keys, ONLY the even ordinals 2i written, K files,
#     one explicit flush per file, auto-compaction off -- verified after every build
#     (exactly K L0 SSTs, other levels empty, memtable empty).
#   * Bloom configuration takes effect AT BUILD TIME: off / 10 / 16 bits each get their
#     own database, never just a different open option.
#   * single thread, 8 GiB block cache, no writes during the read phase.
#   * per run: 30 s warmup, 60 s performance window (PerfContext off), 20 s diagnostic
#     window (counters on), same query trace and seed for paired arms.
#   * rep-major with the paired arms shuffled inside each rep, so the narrow/wide and
#     off/on pairs are interleaved rather than blocked in time.
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

PHASE=${1:-first}
M=${M:-2097152}
VALUE=${VALUE:-1024}
LB=$OUT/layout_bloom
DBDIR=$LB/db
BIN=${LB_BIN:-$BENCH/tools/l0bench_layout}
WARMUP=${WARMUP:-30}
PERF_S=${PERF_S:-60}
DIAG_S=${DIAG_S:-20}
CACHE=${CACHE:-8G}
REPS=${REPS:-5}
[ -x "$BIN" ] || { echo "missing $BIN; run bench/build_layout.sh" >&2; exit 2; }
mkdir -p "$LB" "$DBDIR"

# one file must fit in one memtable, or a flush can split it and K SSTs become more
wb_for() {  # <K>
  python3 -c "
M=$M; K=$1; v=$VALUE
per = M // K
need = int(per * (v + 64) * 2.0)
print(max(1 << 29, need))"
}

db_name() { echo "$DBDIR/K$1_$2_b$3"; }   # K layout bits

build_db() {  # <K> <layout> <bits>
  local K=$1 layout=$2 bits=$3
  local db; db=$(db_name "$K" "$layout" "$bits")
  local wb; wb=$(wb_for "$K")
  if [ -f "$db.done" ]; then echo "  reuse $db"; return 0; fi
  rm -rf "$db"
  echo "  build $db (K=$K layout=$layout bloom_bits=$bits write_buffer_size=$wb)"
  "$BIN" --mode=layout --db="$db" --out="$LB/build_K${K}_${layout}_b${bits}" \
         --layout="$layout" --layout_files="$K" --layout_keys="$M" \
         --bloom_bits="$bits" --disable_auto_compactions=1 --level0_stop=100000 \
         --write_buffer_size="$wb" --max_write_buffer_number=2 --use_existing_db=0 \
         --value_size="$VALUE" --threads=1 --progress_ms=2000 --seed=20260925 \
         > "$LB/build_K${K}_${layout}_b${bits}.log" 2>&1
  local rc=$?
  grep -E "^LAYOUTCHECK" "$LB/build_K${K}_${layout}_b${bits}.log" | tail -1
  if [ "$rc" != 0 ] || ! grep -q "LAYOUTCHECK ok=1" "$LB/build_K${K}_${layout}_b${bits}.log"; then
    echo "  BUILD FAILED for K=$K $layout bits=$bits; see $LB/build_K${K}_${layout}_b${bits}.log" >&2
    return 1
  fi
  touch "$db.done"
  # layoutcheck: candidate file counts per query class, the mechanism the run depends on
  "$BIN" --mode=layoutcheck --db="$db" --out="$LB/chk_K${K}_${layout}_b${bits}" \
         --layout="$layout" --layout_files="$K" --layout_keys="$M" --bloom_bits="$bits" \
         --disable_auto_compactions=1 --use_existing_db=1 --threads=1 --seed=20260925 \
         > "$LB/chk_K${K}_${layout}_b${bits}.log" 2>&1
  grep -E "CANDIDATES|LAYOUTREAD" "$LB/chk_K${K}_${layout}_b${bits}.log" >> "$LB/layoutcheck_summary.txt"
  return 0
}

run_arm() {  # <rid> <K> <layout> <bits> <class>
  local rid=$1 K=$2 layout=$3 bits=$4 cls=$5
  local db; db=$(db_name "$K" "$layout" "$bits")
  local rdir=$LB/runs
  mkdir -p "$rdir"
  "$BIN" --mode=read --db="$db" --out="$rdir/$rid" \
         --layout="$layout" --layout_files="$K" --layout_keys="$M" --bloom_bits="$bits" \
         --read_class="$cls" --disable_auto_compactions=1 --level0_stop=100000 \
         --use_existing_db=1 --cache_size="$CACHE" --threads=1 --value_size="$VALUE" \
         --warmup_s="$WARMUP" --perf_window_s="$PERF_S" --diag_window_s="$DIAG_S" \
         --progress_ms=10000 --seed=20260925 --hist_encoding=lower_bound_v2 \
         > "$rdir/$rid.run.log" 2>&1
  local rc=$?
  # Frozen-content check, as a separate process after the measured windows (§7 wants a
  # verify.log per run). The stride is EVEN so every sampled ordinal 2i exists in this
  # database: an odd stride would report ~half the sample as missing and drown the
  # signal. `universe` is the full id space 2M, since keys are the even ordinals.
  "$BIN" --mode=verify --db="$db" --out="$rdir/$rid.verify" --universe=$((2 * M)) \
         --use_existing_db=1 --verify_step=99992 --verify_salt=1 --value_size="$VALUE" \
         --threads=1 --cache_size="$CACHE" --disable_auto_compactions=1 \
         --level0_stop=100000 > "$rdir/$rid.verify.log" 2>&1
  echo "verify_rc=$?" >> "$rdir/$rid.verify.log"
  # the gate looks for a manifest near the run dir, so record the run BEFORE gating it
  printf '{"run_id":"%s","K":%s,"layout":"%s","bloom_bits":%s,"read_class":"%s","rc":%s,"start":"%s","bin_sha256":"%s"}\n' \
    "$rid" "$K" "$layout" "$bits" "$cls" "$rc" "$(date -Is)" \
    "$(sha256sum "$BIN" | cut -d' ' -f1)" >> "$LB/manifest.jsonl"
  python3 "$BENCH/v2/check_run.py" --run-dir "$rdir" --run-id "$rid" --rc "$rc" --verify \
          > "$rdir/$rid.validity.txt" 2>&1 || true
  local ok=0; grep -q '^VALID=1$' "$rdir/$rid.validity.txt" && ok=1
  printf '%-34s rc=%s valid=%s  ' "$rid" "$rc" "$ok"
  python3 -c "
import json,sys
try:
    d=json.load(open('$rdir/$rid.measure_phase.json'))
    w=(d.get('diag_window') or {})
    k=[x for x in w if isinstance(w[x],dict) and 'bloom_checks_per_get' in w[x]]
    c=w[k[0]] if k else {}
    print('checks/Get=%.3f fpr=%s cpu_ns/Get=%.0f' % (c.get('bloom_checks_per_get',0), c.get('false_positive_rate'), (d.get('perf_window') or {}).get('cpu_ns_per_get',0)))
except Exception as e:
    print('(no summary: %s)' % e)"
}

# ---------------------------------------------------------------------------
# load extensions (§6): once the first round shows an effect, test whether the per-Get
# CPU cost reaches throughput. Same absolute arrival rate for both layouts -- a rate that
# is "80% of each layout's own ceiling" is not the same load. Also a cache-limited pass
# and a direct-read comparison (a small block cache does not by itself mean cold device
# reads, and use_direct_io for flush/compaction is NOT use_direct_reads).
# ---------------------------------------------------------------------------
run_load() {
  local stage=load
  echo "### load extensions on K=16 / 10 bits states"
  # one common rate: the lower of the two layouts' closed-loop ceilings, halved.
  # V2_LOAD_RATE freezes the rate measured by an earlier pass, which is what makes a
  # re-run of the load block paired with it instead of merely similar.
  local rate
  if [ -n "${V2_LOAD_RATE:-}" ]; then
    rate=$V2_LOAD_RATE
    echo "common open-loop rate (frozen from the first load pass): $rate ops/s"
  else
    local rmin=""
    for th in 1 4 16; do
      for layout in narrow wide; do
        arm_read_ceiling "$stage" "load_cb_${layout}_t${th}" "$layout" "$th"
        local r=$CLOSED_LOOP_RATE
        if [ -n "$r" ]; then
          if [ -z "$rmin" ] || awk -v a="$r" -v b="$rmin" 'BEGIN{exit !(a<b)}'; then rmin=$r; fi
        fi
      done
    done
    rate=$(v2_frac "$rmin" 0.5)
    echo "common open-loop rate for both layouts: $rate ops/s (0.5 x the lower ceiling $rmin)"
  fi
  for rep in $(seq 1 "$LOAD_REPS"); do
    for layout in narrow wide; do
      for th in 1 4 16; do
        arm_bloom "$stage" "load_threads_${layout}_t${th}_r${rep}" "$layout" 10 \
          --threads="$th" --warmup_s="$WARMUP" --measure_s="$PERF_S"
      done
      for fr in 0.5 1.0; do
        arm_bloom "$stage" "load_open_${layout}_f${fr}_r${rep}" "$layout" 10 \
          --arrival=open --rate="$(v2_frac "$rate" "$fr")" \
          --warmup_s="$WARMUP" --measure_s="$PERF_S"
      done
      for cache in 512M 64M; do
        arm_bloom "$stage" "load_cache_${layout}_c${cache}_r${rep}" "$layout" 10 \
          --cache_size="$cache" --warmup_s="$WARMUP" --measure_s="$PERF_S"
      done
      for dr in 0 1; do
        arm_bloom "$stage" "load_direct_${layout}_d${dr}_r${rep}" "$layout" 10 \
          --use_direct_reads="$dr" --warmup_s="$WARMUP" --measure_s="$PERF_S"
      done
    done
  done
}

# one arm against an existing K=16 state database (the layouts built by the first phase)
arm_bloom() {  # <stage> <rid> <layout> <bits> -- <extra args>
  local stage=$1 rid=$2 layout=$3 bits=$4; shift 4; [ "$1" = "--" ] && shift
  local db; db=$(db_name 16 "$layout" "$bits")
  local rdir=$LB/runs; mkdir -p "$rdir"
  # A swept flag must be passed ONCE, with the swept value. This arm used to hard-code
  # --threads=16/--cache_size=$CACHE and then append the caller's value, so the cache and
  # thread arms carried e.g. --cache_size=['8G','512M'] in their own config; the gate
  # refused them, correctly, because "last one wins" is not something a reader should
  # have to assume. The caller's value now replaces the default.
  local th=16 cache=$CACHE dr=() extra=()
  for a in "$@"; do
    case "$a" in
      --threads=*)          th=${a#--threads=} ;;
      --cache_size=*)       cache=${a#--cache_size=} ;;
      --use_direct_reads=*) dr=("$a") ;;
      *)                    extra+=("$a") ;;
    esac
  done
  "$BIN" --mode=read --db="$db" --out="$rdir/$rid" \
         --layout="$layout" --layout_files=16 --layout_keys="$M" --bloom_bits="$bits" \
         --read_class=mixed --class_weights=hit:0.4,missing_inrange:0.4,missing_outrange:0.2 \
         --disable_auto_compactions=1 --level0_stop=100000 --use_existing_db=1 \
         --threads="$th" --value_size="$VALUE" --cache_size="$cache" \
         ${dr[@]+"${dr[@]}"} \
         --diag_window_s="$DIAG_S" --progress_ms=10000 --seed=20260925 \
         ${extra[@]+"${extra[@]}"} \
         > "$rdir/$rid.run.log" 2>&1
  local rc=$?
  python3 "$BENCH/v2/check_run.py" --run-dir "$rdir" --run-id "$rid" --rc "$rc" --no-cohort \
          > "$rdir/$rid.validity.txt" 2>&1 || true
  printf '{"run_id":"%s","phase":"load","layout":"%s","rc":%s,"start":"%s"}
' \
    "$rid" "$layout" "$rc" "$(date -Is)" >> "$LB/manifest.jsonl"
  local ok=0; grep -q '^VALID=1$' "$rdir/$rid.validity.txt" && ok=1
  printf '  %-34s rc=%s valid=%s
' "$rid" "$rc" "$ok"
}

# closed-loop ceiling for a layout at a given thread count, on the K=16/10-bit state
arm_read_ceiling() {  # <stage> <rid> <layout> <threads>
  local stage=$1 rid=$2 layout=$3 th=$4
  local db; db=$(db_name 16 "$layout" 10)
  local rdir=$LB/runs; mkdir -p "$rdir"
  "$BIN" --mode=read --db="$db" --out="$rdir/$rid" --layout="$layout" --layout_files=16 \
         --layout_keys="$M" --bloom_bits=10 --read_class=mixed \
         --class_weights=hit:0.4,missing_inrange:0.4,missing_outrange:0.2 \
         --disable_auto_compactions=1 --level0_stop=100000 --use_existing_db=1 \
         --threads="$th" --value_size="$VALUE" --cache_size="$CACHE" \
         --warmup_s="$WARMUP" --measure_s="$PERF_S" --progress_ms=10000 --seed=20260925 \
         > "$rdir/$rid.run.log" 2>&1
  CLOSED_LOOP_RATE=$(python3 -c "
import json
try: print(int(json.load(open('$rdir/$rid.measure_phase.json'))['throughput_service_ops_s']))
except Exception: print(0)")
  echo "  ceiling $rid = $CLOSED_LOOP_RATE ops/s"
}

# ---------------------------------------------------------------------------
# builds
# ---------------------------------------------------------------------------
echo "== building layout databases (M=$M, value=$VALUE) =="
case "$PHASE" in
  load)
    LOAD_REPS=${V2_REPS:-3}
    ;;
  first)
    for bits in 0 10; do for layout in narrow wide; do build_db 16 "$layout" "$bits" || exit 1; done; done
    for bits in 0 10; do build_db 1 narrow "$bits" || exit 1; done
    ;;
  extend)
    for bits in 10; do for layout in narrow wide; do build_db 8 "$layout" "$bits" || exit 1; done; done
    for bits in 16; do for layout in narrow wide; do build_db 16 "$layout" "$bits" || exit 1; done; done
    ;;
  build) exit 0 ;;
  *) echo "usage: layout_bloom.sh first|extend|build" >&2; exit 2 ;;
esac

if [ "$PHASE" = load ]; then
  run_load
  echo
  echo "analysis: python3 $BENCH/v2/layout_bloom_analyze.py --out $LB --phase load"
  exit 0
fi

# ---------------------------------------------------------------------------
# arms
# ---------------------------------------------------------------------------
arm_list() {   # "<K> <layout> <bits> <class>" lines
  case "$PHASE" in
    first)
      # core matrix: 2 layouts x 2 Bloom x 2 classes (+ out-of-envelope control) at K=16
      for layout in narrow wide; do for bits in 0 10; do
        for cls in hit missing_inrange; do echo "16 $layout $bits $cls"; done
        echo "16 $layout $bits missing_outrange"
      done; done
      # K=1 control: narrow == wide, so one layout only
      for bits in 0 10; do for cls in hit missing_inrange; do echo "1 narrow $bits $cls"; done; done
      ;;
    extend)
      for layout in narrow wide; do
        for cls in hit missing_inrange; do
          echo "8 $layout 10 $cls"
          echo "16 $layout 16 $cls"
        done
      done
      ;;
  esac
}

echo "== running $PHASE arms (reps=$REPS, warmup=${WARMUP}s perf=${PERF_S}s diag=${DIAG_S}s) =="
mapfile -t ARMS < <(arm_list)
echo "   ${#ARMS[@]} distinct arms x $REPS reps = $(( ${#ARMS[@]} * REPS )) runs"

# rep-major, arms shuffled inside a rep so paired arms interleave (plan §7)
for rep in $(seq 1 "$REPS"); do
  echo "-- rep $rep/$REPS --"
  mapfile -t ORDER < <(python3 -c "
import random,sys
arms=[l.rstrip('\n') for l in sys.stdin if l.strip()]
random.Random(20260925 + $rep).shuffle(arms)
for a in arms: print(a)" <<< "$(printf '%s\n' "${ARMS[@]}")")
  for arm in "${ORDER[@]}"; do
    read -r K layout bits cls <<< "$arm"
    run_arm "lb_${PHASE}_K${K}_${layout}_b${bits}_${cls}_r${rep}" "$K" "$layout" "$bits" "$cls"
  done
done

echo
echo "analysis: python3 $BENCH/v2/layout_bloom_analyze.py --out $LB --phase $PHASE"
