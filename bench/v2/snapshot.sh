#!/bin/bash
# §4 snapshot construction. Produces the two settled states every later arm starts
# from, and refuses to hand them on until they have been verified and copied.
#
#   4.1  base50  : 48,000,000 keys, 1 KiB values, ~50 GB of live data
#   4.2  base100 : 96,000,000 keys, ~100 GB (only after base50 is accepted)
#
# The fill is a measurement-free build step: it uses mode=fill with a fixed seed and
# a shuffled key order, then drains, then flushes, then verifies. The settled result
# is copied to $BASE so that fill/overwrite arms always start from an identical
# frozen state (runbook §0: fill and overwrite must never share a starting point).
#
#   bash bench/v2/snapshot.sh 50            # build base50
#   bash bench/v2/snapshot.sh 100           # build base100 (needs >= 300 GB free)
#   bash bench/v2/snapshot.sh verify 50     # re-verify an existing snapshot
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
v2_setup_paths

BASE=${BASE:-$BENCH/base}
mkdir -p "$BASE" "$LOGS"
ACTION=${1:-50}

# The common option set from runbook §4.1. Kept in one place so every arm that
# claims to start from "the same settled state" really does.
v2_common_options() {
  printf '%s' "--threads=16 --value_size=1024 --write_buffer_size=256M \
--max_write_buffer_number=4 --level0_trigger=8 --level0_slowdown=20 --level0_stop=36 \
--target_file_size_base=64M --max_background_jobs=8 --max_subcompactions=8 \
--dynamic_level_bytes=1 --cache_size=8G --bloom_bits=10 --compression=none \
--stats_dump_period_sec=60 --sample_ms=500 --layout_ms=5000 --dirscan_ms=10000 \
--progress_ms=1000 --sync=0 --seed=20260925"
}

build_snapshot() {
  local n="$1" universe="$2" need_gb="$3"
  local name="base${n}"
  local db="$BENCH/db/${name}_build"
  local stage="snapshot/$name"
  echo "=== building $name: universe=$universe keys (~$((universe / 1000000)) GB of live data) ==="
  if ! v2_disk_gate "$need_gb"; then
    echo "refusing: need ${need_gb}G free" >&2
    return 125
  fi
  # Reuse/adopt an already-settled snapshot rather than destroying it. §4.2 wants every
  # replica run to start from the SAME frozen state, §1 warns against deleting
  # artifacts that have not been exported yet, and a rebuild costs hours of NVMe
  # write. This applies to ANY existing directory, manifest or not: an inherited
  # snapshot is verified and then documented, never silently replaced.
  # Set V2_REBUILD=1 to force a fresh fill.
  if [ -z "${V2_REBUILD:-}" ] && [ -d "$BASE/$name" ]; then
    echo "existing $BASE/$name found: adopting (verify + manifest) instead of rebuilding"
    [ -f "$BASE/$name.manifest.txt" ] && cat "$BASE/$name.manifest.txt"
    adopt_snapshot "$n" "$universe"
    return $?
  fi
  rm -rf "$db"
  V2_STAGE="$stage" \
    v2_run "${name}_build" --mode=fill --db="$db" --universe="$universe" --num_ops="$universe" \
           $(v2_common_options) --dist=uniform --order=shuffle \
           --warmup_s=0 --measure_s=0 --wait_for_drain=1 --wait_for_drain_max_s=900 \
           --drain_require_l0_zero=1 --final_flush=1 || return $?
  # A snapshot is the one place the physical condition IS wanted: a frozen starting
  # state should have an empty L0, which is what the adopted base50 has. Everywhere
  # else the logical drain conditions alone are the criterion (§6).
  # strided content check before the snapshot is frozen
  V2_STAGE="$stage" \
    v2_run "${name}_verify" --mode=verify --db="$db" --universe="$universe" \
           --use_existing_db=1 --verify_step=99991 --verify_salt=1 --value_size=1024 \
           --threads=16 --cache_size=8G --bloom_bits=10 --compression=none \
           --progress_ms=1000 || return $?
  grep -h "^VERIFY" "$OUT/$stage/${name}_verify".*.log "$OUT/$stage/${name}_verify.run.log" 2>/dev/null | tail -1
  local bad
  bad=$(python3 - "$OUT/$stage/${name}_verify.run.log" <<'PY'
import re,sys
txt=open(sys.argv[1],errors="replace").read()
m=re.search(r"VERIFY checked=(\d+) ok=(\d+) missing=(\d+) badvalue=(\d+)",txt)
print(-1 if not m or int(m.group(4))!=0 else int(m.group(3)))
PY
)
  if [ "${bad:-1}" != "0" ]; then
    echo "verify did not come back clean (missing/bad=$bad); NOT freezing $name" >&2
    return 1
  fi
  # freeze: the replica is what every later arm is copied from
  rm -rf "$BASE/$name"
  echo "copying $db -> $BASE/$name (this is the frozen identity)"
  cp -a "$db" "$BASE/$name"
  {
    echo "snapshot=$name"
    echo "universe=$universe"
    echo "live_bytes=$(du -sb "$BASE/$name" | cut -f1)"
    echo "sst_files=$(find "$BASE/$name" -name '*.sst' | wc -l)"
    echo "created=$(date -Is)"
    echo "source_db=$db"
    echo "binary_md5=$(v2_bin_md5)"
    echo "git_head=$(v2_git_head)"
    echo "L0_files_at_freeze=$(find "$BASE/$name" -name '*.sst' -printf '%f\n' | wc -l)"
  } > "$BASE/$name.manifest.txt"
  cat "$BASE/$name.manifest.txt"
}

# Verify an existing settled snapshot in place and document it. Used for a snapshot
# this pipeline did not build itself (an inherited base50, for instance): it gets the
# same manifest and the same strided content check as a freshly built one, without
# paying for a 48M-key refill.
adopt_snapshot() {
  local n="$1" universe="$2"
  local name
  name=$(v2_replica_snapshot_name "$n")
  local dir="$BASE/$name"
  if [ ! -d "$dir" ]; then echo "no such snapshot: $dir" >&2; return 2; fi
  echo "=== adopting $name: verifying the existing state (universe=$universe) ==="
  V2_STAGE="snapshot/$name" \
    v2_run "${name}_reverify" --mode=verify --db="$dir" --universe="$universe" \
           --use_existing_db=1 --verify_step="${V2_VERIFY_STEP:-99991}" --verify_salt=1 \
           --value_size=1024 --threads=16 --cache_size=8G --bloom_bits=10 \
           --compression=none --progress_ms=1000
  local rc=$?
  local line
  line=$(grep -h "^VERIFY" "$OUT/snapshot/$name/${name}_reverify.run.log" 2>/dev/null | tail -1)
  echo "$line"
  local missing bad
  missing=$(printf '%s' "$line" | sed -n 's/.*missing=\([0-9]*\).*/\1/p')
  bad=$(printf '%s' "$line" | sed -n 's/.*badvalue=\([0-9]*\).*/\1/p')
  {
    echo "snapshot=$name"
    echo "universe=$universe"
    echo "origin=adopted"
    echo "live_bytes=$(du -sb "$dir" | cut -f1)"
    echo "sst_files=$(find "$dir" -name '*.sst' | wc -l)"
    echo "sst_bytes=$(find "$dir" -name '*.sst' -printf '%s\n' | awk '{s+=$1} END{print s+0}')"
    echo "wal_files=$(find "$dir" -name '*.log' | wc -l)"
    echo "adopted=$(date -Is)"
    echo "verify_line=$line"
    echo "verify_missing=$missing"
    echo "verify_badvalue=$bad"
    echo "verify_rc=$rc"
    echo "binary_md5=$(v2_bin_md5)"
    echo "git_head=$(v2_git_head)"
  } > "$BASE/$name.manifest.txt"
  cat "$BASE/$name.manifest.txt"
  if [ "${bad:-1}" != "0" ] || [ "$rc" != 0 ]; then
    echo "adopt FAILED for $name (verify_rc=$rc badvalue=${bad:-?}); do not use this snapshot" >&2
    return 1
  fi
  echo "adopted $name: verified clean over a $(awk -v u="$universe" -v s="${V2_VERIFY_STEP:-99991}" 'BEGIN{printf "%d", u/s}')-key stride"
  return 0
}

verify_snapshot() {
  local n="$1" universe db
  case "$n" in
    50) universe=48000000; db="$BASE/base50" ;;
    100) universe=96000000; db="$BASE/base100" ;;
    smoke) universe=200000; db="$BASE/base_smoke" ;;
    *) echo "unknown snapshot $n" >&2; return 2 ;;
  esac
  V2_STAGE="snapshot/$(basename "$db")" \
    v2_run "$(basename "$db")_reverify" --mode=verify --db="$db" --universe="$universe" \
           --use_existing_db=1 --verify_step=99991 --verify_salt=1 --value_size=1024 \
           --threads=16 --cache_size=8G --bloom_bits=10 --compression=none --progress_ms=1000
  grep -h "^VERIFY" "$OUT/snapshot/$(basename "$db")/$(basename "$db")_reverify.run.log" 2>/dev/null | tail -1
}

case "$ACTION" in
  50)    build_snapshot 50 48000000 200 ;;
  100)   build_snapshot 100 96000000 300 ;;
  # Small snapshot so the whole M1-M5 pipeline can be exercised end to end in
  # minutes before committing hours of NVMe time to the formal arms. It is NOT a
  # result: effects at this scale are dominated by cache residency.
  smoke) build_snapshot _smoke 200000 5 ;;
  adopt) adopt_snapshot "${2:-50}" "$(case "${2:-50}" in 100) echo 96000000 ;; smoke) echo 200000 ;; *) echo 48000000 ;; esac)" ;;
  verify) verify_snapshot "${2:-50}" ;;
  *) echo "usage: snapshot.sh 50|100|smoke|adopt <50|100|smoke>|verify <50|100>" >&2; exit 2 ;;
esac
