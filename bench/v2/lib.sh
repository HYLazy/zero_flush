#!/bin/bash
# Shared helpers for the perf_v2 campaign (SERVER_EXPERIMENT_RUNBOOK.md).
#
# Layout follows the runbook: ROOT/{source/rocksdb,bench}, results under
# $BENCH/out/perf_v2. If ROOT is unset the repo containing this file is used, so
# the same scripts work in-place and in a restored deployment.
#
# Source this file; do not execute it.
#   source bench/v2/lib.sh
set -uo pipefail

# ---------------------------------------------------------------------------
# paths
# ---------------------------------------------------------------------------
v2_setup_paths() {
  local here
  # When a runner re-execs itself from a private copy (the mid-flight-edit guard), the
  # real script directory is carried in V2_SCRIPT_DIR: the copy's own ../.. is /tmp/..
  here=${V2_SCRIPT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}
  local repo
  repo=$(cd "$here/../.." && pwd)
  if [ -n "${ROOT:-}" ] && [ -d "${ROOT}/source/rocksdb" ]; then
    RS=${RS:-$ROOT/source/rocksdb}
    BENCH=${BENCH:-$ROOT/bench}
  else
    RS=${RS:-$repo/source/rocksdb}
    BENCH=${BENCH:-$repo/bench}
  fi
  OUT=${OUT:-$BENCH/out/perf_v2}
  BIN=${BIN:-$BENCH/tools/l0bench}
  BASE=${BASE:-$BENCH/base}
  LOGS=$OUT/logs
  export LD_LIBRARY_PATH="$RS/build:/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  export V2_REPO=$repo
  mkdir -p "$OUT" "$LOGS"
}

# bytes -> GiB with 1 decimal
v2_gib() { awk -v b="${1:-0}" 'BEGIN{printf "%.1f", b/1073741824}'; }

v2_free_gb() { df -B1 --output=avail "$1" 2>/dev/null | tail -1 | awk '{printf "%.1f", $1/1000000000}'; }

v2_host() { hostname 2>/dev/null || echo unknown; }

v2_git_head() { git -C "${V2_REPO:-.}" rev-parse HEAD 2>/dev/null || echo nogit; }

v2_bin_md5() { md5sum "$BIN" 2>/dev/null | cut -d' ' -f1; }

# Refuse to start a run when the volume is too tight (runbook §1: stop below 150 GB).
v2_disk_gate() {
  local need_gb="${1:-150}" fs
  fs=$(df -B1 --output=avail "$OUT" 2>/dev/null | tail -1)
  local gb
  gb=$(awk -v b="$fs" 'BEGIN{printf "%.1f", b/1000000000}')
  if awk -v g="$gb" -v n="$need_gb" 'BEGIN{exit !(g < n)}'; then
    echo "DISK-LOW: only ${gb}G free at $OUT (need >= ${need_gb}G); refusing to run" >&2
    return 1
  fi
  return 0
}

# ---------------------------------------------------------------------------
# monitoring (runbook §6). iostat/vmstat/pidstat when present, else a /proc
# sampler -- either way the fact is recorded so nobody has to guess later.
# ---------------------------------------------------------------------------
# A run's monitor directories are tracked so an abnormal exit cannot leave stray
# sampler processes behind (they would keep writing into a later run's directory).
V2_MON_DIRS=${V2_MON_DIRS:-}

v2_monitors_start() {
  local rdir="$1" pid="${2:-0}"
  mkdir -p "$rdir"
  V2_MON_DIRS="$V2_MON_DIRS $rdir"
  : > "$rdir/monitor_available.txt"
  if command -v iostat >/dev/null 2>&1; then
    iostat -xm 1 > "$rdir/iostat.log" 2>&1 & echo $! > "$rdir/iostat.pid"
    echo "iostat=yes" >> "$rdir/monitor_available.txt"
  else echo "iostat=missing" >> "$rdir/monitor_available.txt"; fi
  if command -v vmstat >/dev/null 2>&1; then
    vmstat 1 > "$rdir/vmstat.log" 2>&1 & echo $! > "$rdir/vmstat.pid"
    echo "vmstat=yes" >> "$rdir/monitor_available.txt"
  else echo "vmstat=missing" >> "$rdir/monitor_available.txt"; fi
  if command -v pidstat >/dev/null 2>&1; then
    ( [ "$pid" != "0" ] && pidstat -dur -p "$pid" 1 || pidstat -dur 1 ) > "$rdir/pidstat.log" 2>&1 &
    echo $! > "$rdir/pidstat.pid"
    echo "pidstat=yes" >> "$rdir/monitor_available.txt"
  else echo "pidstat=missing" >> "$rdir/monitor_available.txt"; fi
  # Always-available fallback: raw /proc counters (iostat/pidstat are absent on
  # this host, so this is the primary device-side evidence here).
  (
    while :; do
      echo "ts=$(date +%s.%N) load=$(cut -d' ' -f1-3 /proc/loadavg) mem=$(awk '/MemAvailable/{print $2}' /proc/meminfo)"
      awk '$3=="nvme0n1"{print "diskstats wr_sect="$8" wr_ios="$5" rd_sect="$4" io_ticks="$13}' /proc/diskstats
      sleep 1
    done
  ) > "$rdir/proc_sampler.log" 2>&1 &
  echo $! > "$rdir/proc_sampler.pid"
  echo "proc_sampler=yes" >> "$rdir/monitor_available.txt"
}

v2_monitors_stop() {
  local rdir="$1"
  for p in iostat vmstat pidstat proc_sampler; do
    if [ -f "$rdir/$p.pid" ]; then
      kill "$(cat "$rdir/$p.pid")" 2>/dev/null || true
      rm -f "$rdir/$p.pid"
    fi
  done
}

# All tracked monitors, for an EXIT trap. Idempotent.
v2_monitors_stop_all() {
  local d
  for d in ${V2_MON_DIRS:-}; do v2_monitors_stop "$d"; done
}

# Render an integer to a shell-safe number.
v2_int() { python3 -c "import sys;print(int(float(sys.argv[1] or 0)))" "${1:-0}" 2>/dev/null || echo 0; }

# ---------------------------------------------------------------------------
# instrument identity (runbook §2/§6): the full hash of every artifact that can
# change a measurement. A truncated md5 is a display convenience, not an identity,
# so the manifest carries the full SHA-256 of the binary, the driver source, the
# build script and the frozen harness scripts, plus the exact build invocation.
# ---------------------------------------------------------------------------
v2_file_sha256() { sha256sum "$1" 2>/dev/null | cut -d' ' -f1; }

v2_identity_json() {
  local bin_src="$BENCH/l0bench.cc" build="$BENCH/build.sh"
  local scripts_sha
  if [ -d "${V2_FREEZE_DIR:-}" ]; then
    scripts_sha=$(cd "$V2_FREEZE_DIR" && sha256sum ./*.sh ./*.py 2>/dev/null | sha256sum | cut -d' ' -f1)
  else
    scripts_sha=$(cd "$BENCH/v2" && sha256sum ./*.sh ./*.py 2>/dev/null | sha256sum | cut -d' ' -f1)
  fi
  printf '{"bin_sha256":"%s","bin_md5":"%s","src_sha256":"%s","build_sha256":"%s","scripts_sha256":"%s","build_cmd":"%s","compiler":"%s","rocksdb_lib_sha256":"%s","freeze_dir":"%s"}' \
    "$(v2_file_sha256 "$BIN")" "$(v2_bin_md5)" \
    "$(v2_file_sha256 "$bin_src")" "$(v2_file_sha256 "$build")" "$scripts_sha" \
    "bash bench/build.sh" "$(g++ --version 2>/dev/null | head -1 | tr -d '"')" \
    "$(v2_file_sha256 "$RS/build/librocksdb.so")" "${V2_FREEZE_DIR:-<none>}"
}

# ---------------------------------------------------------------------------
# manifest (runbook §6/§7): one JSON line per invocation attempt.
# ---------------------------------------------------------------------------
v2_manifest() {  # run_id rc censored valid note free_start free_end extra_json
  local rid="$1" rc="$2" cens="$3" valid="$4" note="$5" f0="$6" f1="$7" extra="${8:-{}}"
  printf '{"run_id":"%s","rc":%s,"censored":%s,"valid":%s,"host":"%s","git_head":"%s","bin_md5":"%s","sha256":{"bin":"%s","src":"%s"},"free_gb_start":%s,"free_gb_end":%s,"start_wall":"%s","end_wall":"%s","note":"%s","identity":%s,"extra":%s}\n' \
    "$rid" "$rc" "$cens" "$valid" "$(v2_host)" "$(v2_git_head)" "$(v2_bin_md5)" \
    "$(v2_file_sha256 "$BIN")" "$(v2_file_sha256 "$BENCH/l0bench.cc")" \
    "${f0:-null}" "${f1:-null}" "${V2_RUN_START:-}" "$(date -Is)" "$note" \
    "$(v2_identity_json)" "$extra" \
    >> "${V2_MANIFEST:-$OUT/manifest.jsonl}"
}

# ---------------------------------------------------------------------------
# run one arm
#   v2_run <run_id> [--verify] [--need-gb=N] -- <l0bench args...>
# Returns the driver's rc. Records disk/identity before and after, starts and
# stops monitors, optionally runs the frozen-state verification, then applies the
# §6 validity gate plus the §7 file inventory.
# ---------------------------------------------------------------------------
v2_run() {
  local rid="$1"; shift
  # Verification is ON by default: runbook §7 requires a per-run verify.log, and a
  # data-integrity check that only happens sometimes is not a check. Use
  # --no-verify for arms where a strided content check cannot apply.
  local do_verify=1 need_gb=150
  while [ $# -gt 0 ]; do
    case "$1" in
      --verify) do_verify=1; shift ;;
      --no-verify) do_verify=0; shift ;;
      --need-gb=*) need_gb="${1#*=}"; shift ;;
      --) shift; break ;;
      *) break ;;
    esac
  done
  local ARGS=("$@")
  local stage="${V2_STAGE:-misc}"
  local rdir="$OUT/$stage"
  mkdir -p "$rdir"
  if [ "${V2_DRYRUN:-0}" = "1" ]; then
    # Print the exact command and record a config the conflict scanner can read, so a
    # matrix can be checked for "the arm's own value vs a trailing default" BEFORE
    # hours of runs -- the defect that silently collapsed the first M2 pass.
    printf 'argv: %s --out=%s/%s %s\n' "$BIN" "$rdir" "$rid" "${ARGS[*]}" > "$rdir/$rid.config.txt"
    echo "DRYRUN $rid: $BIN --out=$rdir/$rid ${ARGS[*]}"
    return 0
  fi
  local f0 f1
  f0=$(v2_free_gb "$OUT")
  if ! v2_disk_gate "$need_gb"; then
    V2_MANIFEST="${V2_MANIFEST:-$OUT/manifest.jsonl}" v2_manifest "$rid" 125 0 false "disk-low" "$f0" "$f0"
    return 125
  fi
  echo "== RUN $rid ($(date -Is)) ==" | tee -a "$LOGS/driver.log"
  echo "   $BIN ${ARGS[*]}" >> "$LOGS/driver.log"
  V2_RUN_START=$(date -Is)
  # Runbook §2: the tool must not be edited during a campaign. Rebuilding the driver
  # mid-flight is exactly the accident that once corrupted the original campaign, so
  # the binary identity is captured before the run and re-checked after it.
  local md5_before md5_after
  md5_before=$(v2_bin_md5)
  v2_monitors_start "$rdir/$rid.mon" 0
  "$BIN" --out="$rdir/$rid" "${ARGS[@]}" > "$rdir/$rid.run.log" 2>&1
  local rc=$?
  v2_monitors_stop "$rdir/$rid.mon"
  md5_after=$(v2_bin_md5)
  f1=$(v2_free_gb "$OUT")
  if [ "$md5_before" != "$md5_after" ]; then
    echo "TOOL-CHANGED: binary md5 went $md5_before -> $md5_after during $rid; run is void" \
      | tee -a "$rdir/$rid.run.log"
    { echo "binary_md5_before=$md5_before"; echo "binary_md5_after=$md5_after";
      echo "TOOL-CHANGED: the measurement instrument was rebuilt while this run was in flight"; } \
      > "$rdir/tool_changed.txt"
  fi

  local vlog="$rdir/$rid.verify.log"
  if [ -n "${V2_POST_VERIFY_FN:-}" ] && [ "$rc" = 0 ]; then
    # the arm's content check needs more than one pass (e.g. M4's database holds base
    # keys with salt=1 and backlog keys with salt=2, and no single-salt sweep can pass)
    local vdb=""
    for a in "${ARGS[@]}"; do case "$a" in --db=*) vdb="${a#*=}" ;; esac; done
    "$V2_POST_VERIFY_FN" "$vdb" "$rdir/$rid" "$vlog" || true
  elif [ "$do_verify" = 1 ] && [ "$rc" = 0 ]; then
    local vdb="" vuni="48000000"
    for a in "${ARGS[@]}"; do
      case "$a" in
        --db=*) vdb="${a#*=}" ;;
        --universe=*) vuni="${a#*=}" ;;
      esac
    done
    if [ -n "$vdb" ]; then
      # Frozen-state check, run as a separate process AFTER the measured window so
      # its reads never land inside it (runbook §2.2).
      "$BIN" --mode=verify --out="$rdir/$rid.verify" --db="$vdb" --universe="$vuni" \
             --use_existing_db=1 --verify_step="${V2_VERIFY_STEP:-99991}" \
             --verify_salt="${V2_VERIFY_SALT:-1}" > "$vlog" 2>&1
      echo "verify_rc=$?" >> "$vlog"
    else
      echo "verify_rc=skipped (no --db in run args)" > "$vlog"
    fi
  fi

  local censored="null"
  if [ -f "$rdir/$rid.summary.json" ]; then
    censored=$(python3 -c "import json;print(json.load(open('$rdir/$rid.summary.json')).get('censored',0))" 2>/dev/null || echo null)
  fi
  local note="ok"
  [ "$rc" != 0 ] && note="rc=$rc"
  local vflags=()
  [ "$do_verify" = 1 ] && vflags+=(--verify)
  # shellcheck disable=SC2206
  [ -n "${V2_GATE_ARGS:-}" ] && vflags+=(${V2_GATE_ARGS})
  local valid="false"
  if [ "${V2_SKIP_GATE:-0}" = "1" ]; then
    # Self-test modes (histcheck/scancheck) are not workload runs and do not carry
    # the §7 workload inventory; the gate does not apply to them.
    echo "VALID=n/a (V2_SKIP_GATE=1: self-test mode, §7 inventory not applicable)" \
      > "$rdir/$rid.validity.txt"
    valid="null"
  else
    python3 "$BENCH/v2/check_run.py" --run-dir "$rdir" --run-id "$rid" --rc "$rc" "${vflags[@]}" \
            > "$rdir/$rid.validity.txt" 2>&1 || true
    grep -q '^VALID=1$' "$rdir/$rid.validity.txt" 2>/dev/null && valid="true"
  fi
  V2_MANIFEST="${V2_MANIFEST:-$OUT/manifest.jsonl}" \
    v2_manifest "$rid" "$rc" "$censored" "$valid" "$note" "$f0" "$f1"
  return "$rc"
}

# Pull --db= out of a recorded config.txt (the argv line is authoritative).
v2_db_from_config() {
  local cfg="$1"
  sed -n 's/.*--db=\([^ ]*\).*/\1/p' "$cfg" | head -1
}

# Closed-loop baseline measurement (used by M1/M3 to derive offered-rate fractions).
v2_baseline_ops_s() {  # <summary.json>
  python3 -c "
import json,sys
d=json.load(open('$1'))
print(d.get('throughput_service_ops_s') or d.get('throughput_ops_s') or 0)
" 2>/dev/null || echo 0
}

# ---------------------------------------------------------------------------
# replicas (runbook §4.2): every overwrite arm starts from a fresh copy of the
# frozen snapshot, and the copy is checked against the snapshot manifest before
# the run. A DB that has already been written to is never reused as a starting
# state, so "same initial state" is a property of the data, not a promise.
# ---------------------------------------------------------------------------
v2_dbroot() { echo "${V2_DBROOT:-$OUT/db}"; }

v2_replica_snapshot_name() {  # <50|100|smoke> -> snapshot dir name
  case "$1" in
    smoke) echo "base_smoke" ;;
    *) echo "base$1" ;;
  esac
}

v2_prep_replica() {  # <snap> <run_id> -> echoes the DB path, or returns non-zero
  local snap="$1" rid="$2"
  local src="$BASE/$(v2_replica_snapshot_name "$snap")"
  local dst="$(v2_dbroot)/$rid"
  if [ ! -d "$src" ]; then
    echo "snapshot $src missing; build it with snapshot.sh $snap" >&2
    return 1
  fi
  if [ "${V2_DRYRUN:-0}" = "1" ]; then
    mkdir -p "$dst"; echo "$dst"; return 0      # no clone, no identity check
  fi
  rm -rf "$dst"; mkdir -p "$(dirname "$dst")"
  # Hard-link the SSTs, copy the mutable files. A replica of a 47 GiB snapshot costs
  # its small metadata only, and RocksDB never modifies an SST in place, so the
  # snapshot cannot be corrupted through the links. `cp -a` here would need ~47 GiB
  # per arm, which is how a 30-arm matrix turns into 1.4 TB -- the original campaign
  # used hard links for exactly this reason (bench/mkreplica.sh).
  if [ -x "$BENCH/mkreplica.sh" ]; then
    # stdout of this function is the DB PATH and nothing else, so the helper's own
    # progress line goes to stderr where the caller's logs can still see it.
    bash "$BENCH/mkreplica.sh" "$src" "$dst" >&2
  else
    echo "WARNING: $BENCH/mkreplica.sh missing; falling back to a full copy" >&2
    cp -a "$src" "$dst"
  fi
  if [ ! -d "$dst" ] || [ ! -f "$dst/CURRENT" ]; then
    echo "replica $dst is not a usable DB (missing CURRENT)" >&2
    return 1
  fi
  # §4.2 pre-run identity checks, recorded next to the run
  local chk="$(v2_dbroot)/$rid.replica_check.txt"
  {
    echo "snapshot=$(v2_replica_snapshot_name "$snap")"
    echo "source=$src"
    echo "dest=$dst"
    echo "method=$( [ -x "$BENCH/mkreplica.sh" ] && echo hardlink_sst || echo full_copy )"
    echo "src_sst_bytes=$(find "$src" -name '*.sst' -printf '%s\n' 2>/dev/null | awk '{s+=$1} END{print s+0}')"
    echo "dst_sst_bytes=$(find "$dst" -name '*.sst' -printf '%s\n' 2>/dev/null | awk '{s+=$1} END{print s+0}')"
    echo "src_n_sst=$(find "$src" -name '*.sst' | wc -l)"
    echo "dst_n_sst=$(find "$dst" -name '*.sst' | wc -l)"
    echo "hardlinked_sst=$(find "$dst" -name '*.sst' -links +1 2>/dev/null | wc -l)"
    echo "extra_bytes_on_disk=$(du -sb --exclude='*.sst' "$dst" 2>/dev/null | cut -f1)"
    echo "src_manifest=$(cat "$BASE/$(v2_replica_snapshot_name "$snap").manifest.txt" 2>/dev/null | tr '\n' ' ')"
  } > "$chk"
  local a b
  a=$(sed -n 's/^src_sst_bytes=//p' "$chk"); b=$(sed -n 's/^dst_sst_bytes=//p' "$chk")
  if [ "$a" != "$b" ]; then
    echo "replica check FAILED: SST bytes differ ($a vs $b)" | tee -a "$chk" >&2
    return 1
  fi
  echo "$dst"
  return 0
}

# Offered-rate helpers for the open-loop arms (runbook §5 M1/M3).
v2_frac() { python3 -c "print(int(${1:-0} * $2))"; }

# Arm bookkeeping: the runbook asks for rep-major interleaving, i.e. loop reps
# outermost so that drift shows up as between-rep variance rather than being
# confounded with the arm variable.
v2_rep_label() { printf 'r%s' "$1"; }

