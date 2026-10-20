#!/bin/bash
# Top-level campaign driver for the server-side read/write experiment.
#
# Implements the runbook's ordering and its gates:
#   §1 preflight
#   §2 freeze the instrument (immutable copies + md5), build
#   §3 P0 tool acceptance            <-- hard gate: no science runs until this passes
#   §4 snapshots (50 GB, then 100 GB)
#   §5 M1, M2                        <-- §7: ship P0+M1+M2 first
#          then M3-M5, only after the first three hold up
#   §6 validity gate on every run (inside lib.sh v2_run)
#   §7 export
#
#   bash bench/v2/campaign_v2.sh smoke          # whole chain at toy scale, minutes
#   bash bench/v2/campaign_v2.sh formal         # the runbook's parameters
#   bash bench/v2/campaign_v2.sh formal --to-m2 # stop after M2 (the §7 staging point)
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
STOP_AFTER=""
[ "${2:-}" = "--to-m2" ] && STOP_AFTER=m2

echo "=== perf_v2 campaign mode=$MODE stop_after=${STOP_AFTER:-none} $(date -Is) ==="
# §7 manifest.jsonl is the identity record of every run attempt, so it is appended
# to and never truncated; a new campaign rotates the previous one aside instead of
# discarding it.
if [ -s "$OUT/manifest.jsonl" ]; then
  mv "$OUT/manifest.jsonl" "$OUT/manifest.$(date +%Y%m%dT%H%M%S).jsonl"
fi
touch "$OUT/manifest.jsonl"

# ---------------------------------------------------------------------------
# §2 freeze the instrument BEFORE anything runs. The original campaign was hurt
# by scripts edited mid-flight; the answer is an immutable snapshot plus hashes.
# ---------------------------------------------------------------------------
FREEZE=$OUT/scripts_$(date +%Y%m%dT%H%M%S)
mkdir -p "$FREEZE"
cp -a "$BENCH/v2/"*.sh "$BENCH/v2/"*.py "$FREEZE/" 2>/dev/null || true
cp -a "$BENCH/l0bench.cc" "$FREEZE/" 2>/dev/null || true
( cd "$FREEZE" && sha256sum ./* > SHA256SUMS && md5sum ./* > MD5SUMS )
export V2_FREEZE_DIR="$FREEZE"
echo "frozen scripts + driver source in $FREEZE"
cat "$FREEZE/SHA256SUMS" | sed 's/^/  /'

echo
echo "--- §1 preflight ---"
bash "$BENCH/v2/preflight.sh" > "$LOGS/preflight.out" 2>&1 || true
tail -5 "$LOGS/preflight.out"

echo
echo "--- §2 build ---"
if ! bash "$BENCH/build.sh" > "$LOGS/build_v2.log" 2>&1; then
  echo "BUILD FAILED; see $LOGS/build_v2.log" >&2
  tail -20 "$LOGS/build_v2.log" >&2
  exit 1
fi
tail -2 "$LOGS/build_v2.log"
BIN_MD5=$(v2_bin_md5)
echo "driver md5=$BIN_MD5"
# Full identity of everything that can move a measurement (runbook §2 freeze).
v2_identity_json | python3 -m json.tool > "$OUT/instrument_identity.json"
v2_identity_json > "$OUT/driver_md5.txt"
echo "instrument identity:"; sed 's/^/  /' "$OUT/instrument_identity.json"

# ---------------------------------------------------------------------------
# §3 P0 acceptance gate
# ---------------------------------------------------------------------------
echo
echo "--- §3 P0 tool acceptance (gate) ---"
if ! bash "$BENCH/v2/p0_acceptance.sh" > "$LOGS/p0.log" 2>&1; then
  echo "P0 ACCEPTANCE FAILED -- the instrument is not trusted, campaign stops here." >&2
  echo "see $LOGS/p0.log" >&2
  grep -E "^(  FAIL|P0_VERIFY|HISTCHECK)" "$LOGS/p0.log" >&2 || true
  exit 1
fi
grep -E "^P0_VERIFY" "$LOGS/p0.log" | sed 's/^/  /'
echo "P0 gate: PASS"

# ---------------------------------------------------------------------------
# §4 snapshots
# ---------------------------------------------------------------------------
echo
echo "--- §4 snapshots ($MODE) ---"
if [ "$MODE" = smoke ]; then
  bash "$BENCH/v2/snapshot.sh" smoke > "$LOGS/snapshot.log" 2>&1 || {
    echo "snapshot build failed; see $LOGS/snapshot.log" >&2; exit 1; }
else
  bash "$BENCH/v2/snapshot.sh" 50 > "$LOGS/snapshot50.log" 2>&1 || {
    echo "base50 failed; see $LOGS/snapshot50.log" >&2; exit 1; }
  grep -E "^VERIFY|^snapshot=" "$LOGS/snapshot50.log" | sed 's/^/  /'
  # base100 is DEFERRED by default. §4.2 says the 100 GB work happens only after the
  # 50 GB round passes, and M1/M2 need only base50, so filling 96M keys up front would
  # delay the first shipment by an hour for nothing. Request it explicitly with
  # V2_BASE100=1.
  if [ "${V2_BASE100:-0}" = "1" ]; then
    bash "$BENCH/v2/snapshot.sh" 100 > "$LOGS/snapshot100.log" 2>&1 || {
      echo "base100 failed; see $LOGS/snapshot100.log" >&2; exit 1; }
    grep -E "^VERIFY|^snapshot=" "$LOGS/snapshot100.log" | sed 's/^/  /'
  else
    echo "base100 deferred (set V2_BASE100=1 to build it; §4.2 wants it after the 50 GB round)"
  fi
fi

# ---------------------------------------------------------------------------
# §5 matrix, in the runbook's dependency order
# ---------------------------------------------------------------------------
run_stage() {
  local m=$1
  echo
  echo "--- §5 $m ---"
  if ! bash "$BENCH/v2/run_matrix.sh" "$MODE" "$m" > "$LOGS/$m.log" 2>&1; then
    echo "$m reported a failure; see $LOGS/$m.log" >&2
    tail -5 "$LOGS/$m.log" >&2
    return 1
  fi
  python3 "$BENCH/v2/analyze_v2.py" --out "$OUT" --stage "$m" \
          --md "$OUT/$m/REPORT.md" > "$LOGS/analyze_$m.log" 2>&1 || true
  [ -f "$OUT/$m/REPORT.md" ] && sed -n '1,12p' "$OUT/$m/REPORT.md" | sed 's/^/  /'
  return 0
}

run_stage m1 || exit 1
run_stage m2 || exit 1

echo
echo "--- §7 first shipment (P0 + M1 + M2) ---"
bash "$BENCH/v2/export_run.sh" --all > "$LOGS/export_m1m2.log" 2>&1 || true
grep -E "^(bundle|files to ship)" "$LOGS/export_m1m2.log" | sed 's/^/  /'

if [ "$STOP_AFTER" = m2 ]; then
  echo
  echo "stopping after M2 as requested (runbook §7 staging point)."
  exit 0
fi

for m in m3 m4 m5; do
  run_stage "$m" || { echo "campaign stopped at $m"; exit 1; }
done

echo
echo "--- §7 full export ---"
bash "$BENCH/v2/export_run.sh" --all > "$LOGS/export_all.log" 2>&1 || true
grep -E "^(bundle|files to ship)" "$LOGS/export_all.log" | sed 's/^/  /'
echo
echo "campaign complete $(date -Is)"
