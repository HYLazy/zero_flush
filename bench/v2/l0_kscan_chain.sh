#!/bin/bash
# L0 backlog plan §5: fix B ~= 2 GiB and scan the L0 file count K alone.
#
# One invocation per K, all of them reading ONE base instance (44 GiB, built by the tool
# revision recorded in that base's .done). A per-K base would make K and the base
# indistinguishable; the script's LB_BASE override exists for exactly this.
#
# Each K writes into its own output root (V2_LB_SUFFIX=_kscan_K<k>) so a failure cannot
# corrupt another K's evidence, and every run id carries _K<k>_ because the run ids
# would otherwise collide across K (same layout, B and rep).
#
#   bash bench/v2/l0_kscan_chain.sh            # K = 1 2 4 8 16, 5 reps each
#   KSCAN_K="4" bash bench/v2/l0_kscan_chain.sh
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
v2_setup_paths

KSCAN_K=${KSCAN_K:-"1 2 4 8 16"}
REPS=${KSCAN_REPS:-5}
SHARED_BASE=${KSCAN_BASE:-$OUT/l0_backlog_kscan_K16/db/base_holes}
STATUS=${KSCAN_LOG:-/tmp/v2smoke/KSCAN_STATUS.log}
[ -f "$SHARED_BASE.done" ] || { echo "no shared base at $SHARED_BASE" >&2; exit 2; }

log() { printf '%s %s\n' "$(date -Is)" "$*" | tee -a "$STATUS"; }

log "=== K scan chain: K=[$KSCAN_K] reps=$REPS base=$SHARED_BASE ==="
log "base identity: $(tr '\n' ' ' < "$SHARED_BASE.done")"
log "tool sha256:   $(sha256sum "$BENCH/tools/l0bench_backlog" | cut -c1-24)"
log "common rate:   $(cat "$OUT/l0_backlog/common_rate.txt" 2>/dev/null || echo missing)"

for k in $KSCAN_K; do
  root=$OUT/l0_backlog_kscan_K$k
  mkdir -p "$root"
  log "--- K=$k START (root $root) ---"
  t0=$(date +%s)
  V2_KLIST="$k" V2_REPS="$REPS" V2_LB_SUFFIX="_kscan_K$k" LB_BASE="$SHARED_BASE" \
    bash "$BENCH/v2/l0_backlog.sh" kscan > "$root/kscan.log" 2>&1
  rc=$?
  t1=$(date +%s)
  nvalid=$(grep -l '^VALID=1$' "$root"/runs/*.validity.txt 2>/dev/null | wc -l)
  ntot=$(ls "$root"/runs/*.validity.txt 2>/dev/null | wc -l)
  nman=$(wc -l < "$root/manifest.jsonl" 2>/dev/null || echo 0)
  log "--- K=$k END rc=$rc runs=$ntot valid=$nvalid manifest=$nman elapsed=$((t1-t0))s ---"
  if [ "$rc" != 0 ]; then log "K=$k aborted; chain stops so a broken stage is not measured again"; exit "$rc"; fi
  if [ "$ntot" != 20 ]; then log "K=$k produced $ntot run records, expected 20"; fi
  if [ "$nvalid" != "$ntot" ]; then
    log "K=$k has $((ntot-nvalid)) invalid runs; continuing (the gate's verdicts stay on disk)"
  fi
done
log "=== K scan chain complete ==="
