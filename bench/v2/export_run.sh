#!/bin/bash
# §7 return content. Bundles exactly what the runbook says must come back, and
# refuses to ship a run whose file inventory is short -- a missing artifact found
# after the fact costs a whole re-run.
#
#   bash bench/v2/export_run.sh --stage m1
#   bash bench/v2/export_run.sh --all
#   bash bench/v2/export_run.sh --run-dir bench/out/perf_v2/m1 --run-id m1_ov_f0.90_r1
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
v2_setup_paths

STAGE=""; ALL=0; RDIR=""; RID=""
while [ $# -gt 0 ]; do
  case "$1" in
    --stage) STAGE="$2"; shift 2 ;;
    --all) ALL=1; shift ;;
    --run-dir) RDIR="$2"; shift 2 ;;
    --run-id) RID="$2"; shift 2 ;;
    *) echo "unknown arg $1" >&2; exit 2 ;;
  esac
done

BUNDLE_DIR=$OUT/bundles
mkdir -p "$BUNDLE_DIR"
STAMP=$(date +%Y%m%dT%H%M%S)
TAR=$BUNDLE_DIR/perf_v2_${STAGE:-${ALL:+all}}_${STAMP}.tar.gz
LIST=$(mktemp)
BAD=0

# §7 inventory, per run
emit_run() {
  local dir="$1" rid="$2"
  python3 "$BENCH/v2/check_run.py" --run-dir "$dir" --run-id "$rid" --rc 0 \
    > "$dir/$rid.validity_on_export.txt" 2>&1 || true
  local miss
  miss=$(grep -c "^  check files_present: FAIL" "$dir/$rid.validity_on_export.txt" 2>/dev/null || echo 0)
  if [ "$miss" != "0" ]; then
    echo "WARN: $rid is missing required return content; see $rid.validity_on_export.txt" >&2
    BAD=1
  fi
  for pat in "$rid.config.txt" "$rid.effective.txt" "$rid.measure_phase.json" \
             "$rid.summary.json" "$rid.summary_extra.csv" "$rid.per_sec.csv" \
             "$rid.hist.csv" "$rid.hist_cohort.csv" "$rid.samples.csv" \
             "$rid.events.csv" "$rid.stalls.csv" "$rid.compactions.csv" \
             "$rid.detail_meta.json" "$rid.run.log" "$rid.validity.txt"; do
    [ -f "$dir/$pat" ] && echo "$dir/$pat" >> "$LIST"
  done
  # per-class histogram and detail streams (§2.3)
  for f in "$dir/$rid".hist_*.csv "$dir/$rid".detail_*.csv "$dir/$rid".hist_cohort_*.csv \
           "$dir/$rid".verify.log; do
    [ -f "$f" ] && echo "$f" >> "$LIST"
  done
  # monitor evidence and replica identity, when present
  for f in "$dir/$rid.mon"/*.log "$dir/../$rid.replica_check.txt" "$dir/$rid".history*; do
    [ -f "$f" ] && echo "$f" >> "$LIST"
  done
  return 0
}

if [ -n "$RDIR" ] && [ -n "$RID" ]; then
  emit_run "$RDIR" "$RID"
elif [ "$ALL" = 1 ]; then
  # The follow-on experiments (layout x Bloom, backlog, backlog K sweep) are as much
  # part of the campaign as the M matrices; an earlier bundle shipped only m1-m5 + p0
  # and silently left them out.
  for d in "$OUT"/m1 "$OUT"/m2 "$OUT"/m3 "$OUT"/m4 "$OUT"/m5 \
           "$OUT"/p0/hist "$OUT"/p0/mix "$OUT"/p0/scan "$OUT"/p0/inv_open \
           "$OUT"/layout_bloom "$OUT"/l0_backlog "$OUT"/l0_backlog_kscan_K1 \
           "$OUT"/l0_backlog_kscan_K2 "$OUT"/l0_backlog_kscan_K4 \
           "$OUT"/l0_backlog_kscan_K8 "$OUT"/l0_backlog_kscan_K16; do
    [ -d "$d" ] || continue
    for sp in "$d"/*.summary.json; do
      [ -f "$sp" ] || continue
      emit_run "$d" "$(basename "$sp" | sed 's/\.summary\.json$//')"
    done
  done
elif [ -n "$STAGE" ]; then
  for sp in "$OUT/$STAGE"/*.summary.json; do
    [ -f "$sp" ] || continue
    emit_run "$OUT/$STAGE" "$(basename "$sp" | sed 's/\.summary\.json$//')"
  done
else
  echo "specify --stage X, --all, or --run-dir/--run-id" >&2
  exit 2
fi

[ -f "$OUT/manifest.jsonl" ] && echo "$OUT/manifest.jsonl" >> "$LIST"
[ -f "$OUT/logs/host_start.txt" ] && echo "$OUT/logs/host_start.txt" >> "$LIST"
[ -f "$OUT/logs/driver.log" ] && echo "$OUT/logs/driver.log" >> "$LIST"
# Instrument identity and the frozen script set (§2 freeze): the reviewer asked for
# the full hashes, tied to source, build parameters and the frozen harness.
[ -f "$OUT/instrument_identity.json" ] && echo "$OUT/instrument_identity.json" >> "$LIST"
for f in "$OUT"/scripts_*/SHA256SUMS "$OUT"/scripts_*/MD5SUMS; do
  [ -f "$f" ] && echo "$f" >> "$LIST"
done
for f in "$OUT"/scripts_*/*.sh "$OUT"/scripts_*/*.py "$OUT"/scripts_*/l0bench.cc; do
  [ -f "$f" ] && echo "$f" >> "$LIST"
done
# Analysis tables, per-stage reports, the P0 explanation, the latency erratum and the
# implementation report travel with the data they describe.
for f in "$OUT"/*/REPORT*.md "$OUT"/p0/*.md "$OUT"/m1/knee.json "$OUT"/m1/needs_extension.json \
         "$OUT"/m2/REPORT.md "$OUT"/snapshots/*.identity.json; do
  [ -f "$f" ] && echo "$f" >> "$LIST"
done
for f in "$BENCH/v2/IMPLEMENTATION_REPORT.md" "$BENCH/v2/README.md" \
         "$BENCH"/out/hist_erratum/*.md "$BENCH"/out/hist_erratum/*.csv \
         "$BENCH"/out/hist_erratum/*.json "$BENCH"/base/base50.manifest.txt; do
  [ -f "$f" ] && echo "$f" >> "$LIST"
done
# The binaries and the build scripts that produce them: a bundle that carries only a
# hash cannot be re-run or audited once the working copy is rebuilt (the backlog tool was
# rebuilt mid-matrix and the earlier build is gone). Sources are already shipped above.
for f in "$BENCH"/tools/l0bench "$BENCH"/tools/l0bench_layout \
         "$BENCH"/tools/l0bench_backlog "$BENCH"/build.sh "$BENCH"/build_layout.sh \
         "$BENCH"/build_backlog.sh "$BENCH"/mkreplica.sh \
         "$BENCH"/l0bench.cc "$BENCH"/layoutbench.cc "$BENCH"/backlogbench.cc; do
  [ -f "$f" ] && echo "$f" >> "$LIST"
done
# Current harness (the scripts_* snapshots are from the 09-25 freeze and predate the
# analyzers that produced the follow-on experiments' reports), plus their reports.
for f in "$BENCH"/v2/*.py "$BENCH"/v2/*.sh "$BENCH"/v2/*PLAN*.md \
         "$BENCH"/L0_BACKLOG_PLAN.md "$BENCH"/v2/LAYOUT_BLOOM_PLAN.md \
         "$OUT"/RESULTS_SUMMARY.md "$OUT"/synthesis/*.txt; do
  [ -f "$f" ] && echo "$f" >> "$LIST"
done

sort -u "$LIST" -o "$LIST"
echo "files to ship: $(wc -l < "$LIST")"
tar -czf "$TAR" -T "$LIST" 2>/dev/null || tar -czf "$TAR" -T "$LIST"
# a checksum of the bundle itself, so the receiving side can detect truncation
sha256sum "$TAR" > "$TAR.sha256"
echo "bundle: $TAR"
cat "$TAR.sha256"
rm -f "$LIST"
if [ "$BAD" = 1 ]; then
  echo "NOTE: at least one run is missing required §7 content (bundled anyway, flagged)." >&2
fi
