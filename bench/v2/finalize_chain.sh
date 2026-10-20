#!/bin/bash
# Finish the campaign after the K sweep: analyse it, cross-check it, re-run the rejected
# layout/Bloom load block, and re-export a bundle that contains all of it.
#
# Serialised on purpose: the load re-run measures, so it must not overlap the K sweep or
# the compiling step that precedes it.
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
v2_setup_paths

STATUS=${FINAL_LOG:-/tmp/v2smoke/FINALIZE_STATUS.log}
KSCAN_STATUS=/tmp/v2smoke/KSCAN_STATUS.log
MERGED=$OUT/l0_backlog_kscan

log() { printf '%s %s\n' "$(date -Is)" "$*" | tee -a "$STATUS"; }

log "=== finalize chain start ==="
# 1. wait for the K sweep
if [ "${FINAL_SKIP_WAIT:-0}" != 1 ]; then
  for i in $(seq 1 720); do
    grep -q "=== K scan chain complete ===" "$KSCAN_STATUS" 2>/dev/null && break
    if [ $((i % 10)) = 0 ]; then log "waiting for the K sweep ($i min)"; fi
    sleep 60
  done
  grep -q "=== K scan chain complete ===" "$KSCAN_STATUS" 2>/dev/null \
    && log "K sweep complete" || log "K sweep did NOT report completion; continuing anyway"
fi

# the status log is evidence; keep it with the data (the bundle mirrors repo paths)
mkdir -p "$OUT/l0_backlog_kscan"
cp -f "$KSCAN_STATUS" "$OUT/l0_backlog_kscan/KSCAN_STATUS.log" 2>/dev/null || true

# 2. merge the per-K roots and analyse
python3 "$BENCH/v2/l0_kscan_merge.py" --out "$MERGED" --force \
  --from "$OUT"/l0_backlog_kscan_K1 "$OUT"/l0_backlog_kscan_K2 "$OUT"/l0_backlog_kscan_K4 \
         "$OUT"/l0_backlog_kscan_K8 "$OUT"/l0_backlog_kscan_K16 >> "$STATUS" 2>&1
python3 "$BENCH/v2/l0_backlog_analyze.py" --out "$MERGED" --charts "$MERGED/charts" \
        --md "$MERGED/REPORT.md" > "$STATUS.kmerge" 2>&1
log "K sweep analysis: $(grep -c '' "$MERGED/REPORT.md" 2>/dev/null || echo 0) lines in $MERGED/REPORT.md"

# 3. replication check against the K=16 arms the backlog matrix already ran
python3 "$BENCH/v2/l0_kscan_replicate.py" --a "$OUT/l0_backlog" \
        --b "$OUT/l0_backlog_kscan_K16" --md "$MERGED/REPLICATION_K16.md" >> "$STATUS" 2>&1
log "replication check: $MERGED/REPLICATION_K16.md"

# 4. the layout/Bloom load block, re-run on one instrument revision
if [ "${FINAL_SKIP_LOAD:-0}" != 1 ]; then
  bash "$BENCH/v2/layout_load_retry.sh" >> "$STATUS" 2>&1
  log "load re-run: $(tail -1 "$STATUS")"
fi

# 5. bundle everything (the export now includes the follow-on roots and the tools)
bash "$BENCH/v2/export_run.sh" --all >> "$STATUS" 2>&1
log "bundle: $(ls -t "$OUT"/bundles/*.tar.gz | head -1)"
cp -f "$KSCAN_STATUS" "$OUT/l0_backlog_kscan/KSCAN_STATUS.log" 2>/dev/null || true

# 6. refresh the output/ integration: catalog -> per-dataset .zexp bundles -> README index.
#    These are the "new version (v2)" deliverables; running them here is what makes the
#    output/ tree track the data instead of being a one-off snapshot.
CAT=$V2_REPO/output/report/catalog
python3 "$CAT/build_catalog.py" >> "$STATUS" 2>&1
python3 "$CAT/export_bundle.py" export --scope active perf_v2/P0 perf_v2/M1 perf_v2/M2 \
        perf_v2/M3 perf_v2/M4 perf_v2/M5 perf_v2/LAYOUT_BLOOM perf_v2/BACKLOG perf_v2/KSWEEP \
        >> "$STATUS" 2>&1
python3 "$CAT/make_bundles_index.py" >> "$STATUS" 2>&1
python3 "$CAT/export_bundle.py" verify "$V2_REPO"/output/bundles/perf_v2-*.zexp >> "$STATUS" 2>&1
log "output/ refreshed: catalog + 9 v2 bundles + README index (see $STATUS)"
log "=== finalize chain complete ==="
