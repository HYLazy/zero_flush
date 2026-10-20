#!/bin/bash
# Re-run the layout/Bloom load block (plan §6 extensions) after two harness defects were
# found in its first pass:
#
#   1. `arm_bloom` hard-coded --threads=16/--cache_size=8G and then appended the swept
#      value, so 24 arms carried a flag twice (--cache_size=['8G','512M'] ...) and the
#      validity gate marked them VALID=0. The gate was right: the run's own config was
#      ambiguous. Fixed in arm_bloom (a swept flag now replaces the default).
#   2. the tool had no --use_direct_reads at all, so all 12 direct-read arms died with
#      rc=2 before measuring anything (Options::use_direct_io_for_flush_and_compaction is
#      not the read switch the plan asks about). Added to layoutbench.cc.
#
# Because (2) changes the instrument, the whole load block is re-run with ONE binary
# revision instead of mixing revisions inside one comparison. The rate is frozen to the
# one the first pass measured (35955 ops/s), so the open-loop arms are paired.
#
# The first pass's artifacts are MOVED to $LB/load_rev1/ rather than deleted: its 18 valid
# arms (6 ceilings, 6 threads_t16, 12 open) are the cross-revision control.
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
v2_setup_paths

LB=$OUT/layout_bloom
REV1=$LB/load_rev1
RATE=${V2_LOAD_RATE:-35955}
REPS=${V2_REPS:-3}
STATUS=${LOAD_LOG:-/tmp/v2smoke/LOAD_RETRY_STATUS.log}

log() { printf '%s %s\n' "$(date -Is)" "$*" | tee -a "$STATUS"; }

log "=== layout/Bloom load-block re-run ==="
OLD_MD5=$(md5sum "$BENCH/tools/l0bench_layout" | cut -d' ' -f1)
log "tool before build: $OLD_MD5"
bash "$BENCH/build_layout.sh" >> "$STATUS" 2>&1 || { log "build failed"; exit 2; }
NEW_MD5=$(md5sum "$BENCH/tools/l0bench_layout" | cut -d' ' -f1)
NEW_SHA=$(sha256sum "$BENCH/tools/l0bench_layout" | cut -c1-24)
log "tool after build:  $NEW_MD5 (sha256 $NEW_SHA)"
[ "$NEW_MD5" != "$OLD_MD5" ] || { log "binary unchanged: the flag was not compiled in"; exit 2; }
# prove the new flag is accepted before spending an hour of arms on it
"$BENCH/tools/l0bench_layout" --use_direct_reads=1 --help >/dev/null 2>&1
log "flag probe: --use_direct_reads accepted (rc=$?)"

if [ ! -d "$REV1" ]; then
  mkdir -p "$REV1"
  n=0
  for f in "$LB"/runs/load_*; do [ -e "$f" ] || continue; mv "$f" "$REV1/"; n=$((n+1)); done
  log "archived $n first-pass artifacts to $REV1"
  grep -h '"phase":"load"' "$LB/manifest.jsonl" > "$LB/manifest_load_rev1.jsonl" 2>/dev/null || true
  log "first-pass manifest lines kept in manifest_load_rev1.jsonl ($(wc -l < "$LB/manifest_load_rev1.jsonl" 2>/dev/null || echo 0))"
  grep -v '"phase":"load"' "$LB/manifest.jsonl" > "$LB/manifest_main.jsonl" 2>/dev/null || true
  mv "$LB/manifest.jsonl" "$LB/manifest_with_load_rev1.jsonl"
  cp "$LB/manifest_main.jsonl" "$LB/manifest.jsonl"
else
  log "archive already present at $REV1; refusing to overwrite it"
  exit 2
fi

t0=$(date +%s)
V2_LOAD_RATE="$RATE" V2_REPS="$REPS" bash "$BENCH/v2/layout_bloom.sh" load > "$LB/load_rev2.log" 2>&1
rc=$?
t1=$(date +%s)
nvalid=$(grep -l '^VALID=1$' "$LB"/runs/load_*.validity.txt 2>/dev/null | wc -l)
ntot=$(ls "$LB"/runs/load_*.validity.txt 2>/dev/null | wc -l)
log "load re-run rc=$rc arms=$ntot valid=$nvalid elapsed=$((t1-t0))s"
python3 "$BENCH/v2/layout_bloom_analyze.py" --out "$LB" --phase load > "$LB/load_rev2_analysis.txt" 2>&1
log "analysis written to load_rev2_analysis.txt"
log "=== load re-run complete ==="
