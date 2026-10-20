#!/bin/bash
# Execute one workload under the shared bandwidth budget and CPU affinity.
# Kept as a separate script (rather than a bash function) so that exec preserves
# the workload's PID: the sampler can then attribute /proc/<pid>/io correctly.
set -euo pipefail
ROOT=/home/embed/hyl/metadata_offload
CAPW=${CAPW:-200000000}
CAPR=${CAPR:-400000000}
CPUS=${CPUS:-0-7}
THROTTLE=$ROOT/bench/tools/libiothrottle.so
OUT=${IOTHROTTLE_ROOT:-$ROOT/bench/out/resbudget}
exec env LD_PRELOAD="$THROTTLE" \
  IOTHROTTLE_WBPS="$CAPW" IOTHROTTLE_RBPS="$CAPR" \
  IOTHROTTLE_BURST=67108864 IOTHROTTLE_ROOT="$OUT" \
  IOTHROTTLE_SHM="${SHM:-}" \
  LD_LIBRARY_PATH="$ROOT/source/rocksdb/build" \
  taskset -c "$CPUS" "$@"
