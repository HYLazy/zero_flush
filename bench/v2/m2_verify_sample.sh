#!/bin/bash
# Sample-verify the M2 sweep that ran under a mid-flight-edited script (2026-09-28).
#
# The sweep's own output looked self-consistent (thread and cache sweeps vary in the
# right direction, 0 conflicting flags), but "looked consistent" is not evidence that a
# process which executed spliced text produced the numbers it claims. This re-runs three
# M2 arms with the SAME frozen binary and the SAME flag set the sweep used, and prints
# both values so they can be compared directly.
#
#   bash bench/v2/m2_verify_sample.sh
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
v2_setup_paths

BIN=${BIN:-$BENCH/tools/l0bench}
OUTR=$OUT
CACHE=8G
# the flag set from run_matrix.sh's OPT_COMMON plus the read-arm specifics
COMMON=(--value_size=1024 --write_buffer_size=256M --max_write_buffer_number=4
        --level0_trigger=8 --level0_slowdown=20 --level0_stop=36 --target_file_size_base=64M
        --max_background_jobs=8 --max_subcompactions=8 --dynamic_level_bytes=1
        --bloom_bits=10 --compression=none --stats_dump_period_sec=60 --sample_ms=500
        --layout_ms=5000 --dirscan_ms=10000 --progress_ms=1000 --sync=0 --seed=20260925
        --detail_rate=0.001 --perf_detail=1 --detail_sampling=random)

echo "== prepping one shared read-only replica =="
DB=$(v2_prep_replica 50 readonly_50_verify) || exit 1
echo "   db=$DB"

printf '%-26s %14s %14s %8s\n' arm sweep ops/s verify ops/s ratio
for spec in "recent 1 8G" "deep 16 64M" "recent 4 512M"; do
  read -r mix th cache <<< "$spec"
  rid="m2_${mix}_t${th}_c${cache}_verify1"
  "$BIN" --mode=read --db="$DB" --out="$OUTR/m2/$rid" --universe=48000000 \
         --use_existing_db=1 --threads="$th" --cache_size="$cache" \
         --read_mix="recent:0.4,deep:0.4,neg:0.2" \
         --warmup_s=30 --measure_s=60 --wait_for_drain=0 "${COMMON[@]}" \
         > "$OUTR/m2/$rid.run.log" 2>&1
  rc=$?
  python3 - "$OUTR" "m2_${mix}_t${th}_c${cache}_r1" "$rid" "$rc" <<'PY'
import json, sys, os
outr, sweep_rid, verify_rid, rc = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
def svc(d, rid):
    p = os.path.join(d, "m2", rid + ".measure_phase.json")
    if not os.path.exists(p):
        return None
    return json.load(open(p))["throughput_service_ops_s"]
a, b = svc(outr, sweep_rid), svc(outr, verify_rid)
ratio = (b / a) if (a and b) else None
sweep_hist = os.path.join(outr, "m2", sweep_rid + ".hist_cohort_get_hit.csv")
ver_hist = os.path.join(outr, "m2", verify_rid + ".hist_cohort_get_hit.csv")
print(f"{verify_rid:26s} {a if a else 0:14.0f} {b if b else 0:14.0f} "
      f"{(f'{ratio:.3f}' if ratio else 'n/a'):>8s}  rc={rc}")
PY
done
rm -rf "$DB"
echo "compare the sweep's own reps too:"
python3 - "$OUTR" <<'PY'
import json, glob, os, statistics, re, sys
outr = sys.argv[1]
for spec in ("m2_recent_t1_c8G", "m2_deep_t16_c64M", "m2_recent_t4_c512M"):
    vals = []
    for p in sorted(glob.glob(os.path.join(outr, "m2", spec + "_r[0-9].measure_phase.json"))):
        vals.append(json.load(open(p))["throughput_service_ops_s"])
    if vals:
        print(f"  {spec:24s} sweep reps: " + ", ".join(f"{v:.0f}" for v in vals) +
              f"   mean={statistics.mean(vals):.0f}")
PY
