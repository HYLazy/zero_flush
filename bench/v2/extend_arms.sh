#!/bin/bash
# Extend the arms that had not converged inside the standard window (runbook §5
# review: "60 秒仍在持续变化的关键臂延长到 300–600 秒").
#
# analyze_v2.py writes <stage>/needs_extension.json listing the arms whose second
# half still differed from the first, plus the arms at or above the knee rate. This
# script replays exactly those arms from their recorded argv, changing only the
# measurement window -- and on a FRESH replica, because overwrite arms mutate the DB
# they started from (§0: one starting state per arm).
#
#   bash bench/v2/extend_arms.sh m1                 # 300 s window
#   bash bench/v2/extend_arms.sh m1 --measure-s=600
#   bash bench/v2/extend_arms.sh m1 --snap=smoke    # smoke-scale snapshot
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
v2_setup_paths

STAGE=${1:?usage: extend_arms.sh <stage> [--measure-s=N] [--snap=50|smoke]}; shift || true
MEASURE=300
SNAP=${V2_SNAP:-50}
while [ $# -gt 0 ]; do
  case "$1" in
    --measure-s=*) MEASURE="${1#*=}"; shift ;;
    --snap=*) SNAP="${1#*=}"; shift ;;
    *) echo "unknown arg $1" >&2; exit 2 ;;
  esac
done

LIST=$OUT/$STAGE/needs_extension.json
[ -f "$LIST" ] || { echo "no $LIST -- run analyze_v2.py on stage $STAGE first" >&2; exit 2; }
# --only takes a comma-separated list of substrings, so a targeted pass can extend
# just the arms a decision depends on (the low-load P99 reference and everything at
# or above the knee) instead of every arm the steady-state check flagged.
FILTERED=$OUT/$STAGE/needs_extension.filtered.json
if [ -n "${ONLY:-}" ]; then
  python3 - "$LIST" "$FILTERED" "$ONLY" <<'PY'
import json, sys
src, dst, pats = sys.argv[1], sys.argv[2], sys.argv[3].split(",")
rows = [e for e in json.load(open(src)) if any(p in e["rid"] for p in pats)]
json.dump(rows, open(dst, "w"), indent=2)
print(f"filtered extension set: {len(rows)} of {len(json.load(open(src)))} flagged arms")
PY
  LIST=$FILTERED
fi
N=$(python3 -c "import json;print(len(json.load(open('$LIST'))))")
echo "extending $N arm(s) from $STAGE to a ${MEASURE}s measurement window on snapshot $SNAP"
[ "$N" = 0 ] && exit 0

# One arm at a time, appended to the campaign manifest so the extended runs carry
# the same identity record as everything else.
python3 - "$LIST" > "$OUT/$STAGE/extension_plan.txt" <<'PY'
import json, sys
for e in json.load(open(sys.argv[1])):
    argv = e["argv"]
    skip = {"db", "out", "measure_s", "warmup_s", "num_ops", "wait_for_drain",
            "wait_for_drain_max_s", "seed", "drain_observe_s", "final_flush"}
    args = [f"--{k}={v}" for k, v in argv.items() if k not in skip]
    print(e["rid"] + "\t" + e.get("why", "") + "\t" + " ".join(args))
PY

FAILED=0
while IFS=$'\t' read -r rid why args; do
  [ -n "$rid" ] || continue
  ext_rid="${rid}_ext${MEASURE}"
  # keep the seed stable across the short and long runs of the same arm
  seed=$(python3 -c "
import json;d=json.load(open('$LIST'))
print(next((e['argv'].get('seed','20260925') for e in d if e['rid']=='$rid'), '20260925'))")
  echo "== extend $rid -> $ext_rid  (reason: $why)"
  db=$(v2_prep_replica "$SNAP" "$ext_rid") || { echo "  replica prep failed"; FAILED=$((FAILED+1)); continue; }
  # shellcheck disable=SC2086
  V2_STAGE="$STAGE" v2_run "$ext_rid" --db="$db" --universe="${V2_UNIVERSE:-48000000}" \
      --use_existing_db=1 --num_ops=0 --measure_s="$MEASURE" --wait_for_drain=0 \
      --seed="$seed" $args
  rc=$?
  # single-use replica: drop it once the run and its verification are done, or a
  # 12-arm extension pass leaves ~600 GB of dead databases behind.
  if [ "${V2_KEEP_DB:-0}" != "1" ] && [ -d "$db" ]; then
    du -sb "$db" 2>/dev/null | sed 's|^|db_bytes_after_run=|' >> "${db}.replica_check.txt"
    echo "db_removed_after_run=1" >> "${db}.replica_check.txt"
    rm -rf "$db"
  fi
  [ "$rc" != 0 ] && FAILED=$((FAILED+1))
done < "$OUT/$STAGE/extension_plan.txt"

echo "extension pass done for $STAGE (failures: $FAILED)"
python3 "$BENCH/v2/analyze_v2.py" --out "$OUT" --stage "$STAGE" \
        --md "$OUT/$STAGE/REPORT_EXTENDED.md" >/dev/null 2>&1 || true
echo "re-analysis: $OUT/$STAGE/REPORT_EXTENDED.md"
exit $([ "$FAILED" = 0 ] && echo 0 || echo 1)
