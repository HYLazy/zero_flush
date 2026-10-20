#!/bin/bash
# perf_windows.sh <outdir> [rounds] [secs_per_thread] [interval_s]
# Profiles the running fillcpu process (bg_jobs run) thread-by-thread with perf:
#   rocksdb:high = flush pool, rocksdb:low = compaction pool, fcw00 = a writer.
# Each round writes perf_<role>_r<k>.data/.txt under <outdir>.
set -uo pipefail
OUT=${1:?outdir}
ROUNDS=${2:-3}
SECS=${3:-20}
INTERVAL=${4:-480}
mkdir -p "$OUT"

find_pid() { ps -eo pid,comm,args | awk '$2=="fc-main" && /tools\/fillcpu/ {print $1}' | head -1; }
tid_of() {  # $1=pid $2=comm  -> first tid with that name
  for t in /proc/$1/task/*/comm; do
    [ -f "$t" ] || continue
    if [ "$(cat "$t" 2>/dev/null)" = "$2" ]; then basename "$(dirname "$t")"; return; fi
  done
}

for i in $(seq 1 "$ROUNDS"); do
  PID=$(find_pid)
  [ -z "$PID" ] && { echo "round $i: process gone"; break; }
  echo "round $i: pid=$PID $(date +%H:%M:%S) load=$(cut -d' ' -f1 /proc/loadavg)"
  for role in "rocksdb:high:flush" "rocksdb:low:comp" "fcw00:writer"; do
    name=${role%:*}; label=${role##*:}
    TID=$(tid_of "$PID" "$name")
    [ -z "$TID" ] && { echo "  $label: no thread"; continue; }
    perf record -t "$TID" -F 299 --call-graph dwarf -o "$OUT/perf_${label}_r$i.data" \
        -- sleep "$SECS" >/dev/null 2>&1
    perf report -i "$OUT/perf_${label}_r$i.data" --stdio --sort symbol --percent-limit 0.3 \
        2>/dev/null > "$OUT/perf_${label}_r$i.txt"
    echo "  $label tid=$TID -> $(grep -c . "$OUT/perf_${label}_r$i.txt" 2>/dev/null) report lines"
  done
  [ "$i" -lt "$ROUNDS" ] && sleep "$INTERVAL"
done
echo "perf windows done"
