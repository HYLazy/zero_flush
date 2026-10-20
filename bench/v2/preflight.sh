#!/bin/bash
# §1 server preflight. Records host identity, device, filesystem mount options,
# disk budget and the current background-task situation BEFORE any run starts, so
# every later claim about "this was the same machine" is checkable.
#
#   bash bench/v2/preflight.sh            # write $OUT/logs/host_start.txt
#   ROOT=/srv/rocksdbexp bash bench/v2/preflight.sh
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
v2_setup_paths

OUTF=$LOGS/host_start.txt
{
  echo "=== perf_v2 preflight $(date -Is) ==="
  echo "root=${ROOT:-<repo-in-place>}"
  echo "RS=$RS"
  echo "BENCH=$BENCH"
  echo "OUT=$OUT"
  echo
  echo "--- uname ---";      uname -a
  echo "--- lscpu ---";      lscpu
  echo "--- free ---";       free -h
  echo "--- df OUT ---";     df -h "$OUT"
  echo "--- findmnt ---";    findmnt -T "$OUT" || true
  echo "--- git HEAD ---";   v2_git_head
  echo "--- binary ---";     ls -l "$BIN" 2>/dev/null; v2_bin_md5
  echo "--- rocksdb lib ---"; sha256sum "$RS/build/librocksdb.so" 2>/dev/null || echo "missing"
  echo
  echo "--- device model ---"
  for d in /sys/block/nvme0n1 /sys/block/sda; do
    [ -e "$d/device/model" ] && echo "$d: $(cat "$d/device/model")"
  done
  echo "--- device write cache / scheduler ---"
  cat /sys/block/nvme0n1/queue/scheduler 2>/dev/null
  cat /sys/block/nvme0n1/queue/write_cache 2>/dev/null
  echo "--- ext4 mount options ---"
  findmnt -T "$OUT" -o TARGET,SOURCE,FSTYPE,OPTIONS -n 2>/dev/null || true
  echo
  echo "--- temperature (hddtemp/smartctl, best effort) ---"
  if command -v smartctl >/dev/null 2>&1; then
    smartctl -A /dev/nvme0 2>/dev/null | grep -i -E "temperature|percentage|data units written" || echo "smartctl: no data"
  else
    echo "smartctl: not installed"
  fi
  echo
  echo "--- background tasks that could interfere (runbook §1) ---"
  ps -eo pid,pcpu,pmem,etime,comm --sort=-pcpu | head -20
  echo "--- cron/systemd timers (names only) ---"
  ls /etc/cron.d /etc/cron.daily 2>/dev/null | head -20
  systemctl list-timers --no-pager 2>/dev/null | head -10 || echo "systemctl unavailable"
  echo
  echo "--- monitoring tools present ---"
  for t in iostat vmstat pidstat smartctl fstrim; do
    printf '%s=%s\n' "$t" "$(command -v $t >/dev/null 2>&1 && echo yes || echo missing)"
  done
  echo
  echo "--- blkdiscard/TRIM status ---"
  fstrim -v "$OUT" 2>&1 | head -2 || echo "fstrim failed"
} | tee "$OUTF"

echo
echo "preflight written to $OUTF"
# §1 hard gates, reported up front rather than discovered mid-campaign.
free_gb=$(v2_free_gb "$OUT")
echo "free at OUT: ${free_gb} GB (runbook wants >= 150 GB before any run)"
if awk -v g="$free_gb" 'BEGIN{exit !(g < 150)}'; then
  echo "WARNING: below the 150 GB floor; the 50/100 GB arms cannot run safely." >&2
fi
