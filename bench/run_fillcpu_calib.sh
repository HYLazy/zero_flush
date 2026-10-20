#!/bin/bash
# Calibration: intrinsic write-path CPU per 1KB Put, default config.
# Single writer thread, small DB (no compaction pressure, no 16-way contention)
# vs the 16-thread 100GB run.  Same driver, same defaults.
set -euo pipefail
ROOT=/home/embed/hyl/metadata_offload
OUT=$ROOT/bench/out/fillcpu_calib
mkdir -p "$OUT"
cd "$ROOT"
for T in 1 4 16; do
  rm -rf "$ROOT/bench/db/calib_$T"
  LD_LIBRARY_PATH=$ROOT/source/rocksdb/build \
    "$ROOT/bench/tools/fillcpu" \
    --db="$ROOT/bench/db/calib_$T" \
    --out="$OUT/calib_t$T" \
    --num_ops=1000000 --universe=1000000 --threads=$T \
    --sample_ms=200 --progress_ms=5000 \
    --drain_max_s=120 --disk_guard_gb=60 --destroy_first=1 \
    > "$OUT/calib_t$T.log" 2>&1
  python3 - "$OUT/calib_t$T" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]+".summary.json"))
f=d["phase_fill"]; n=d["ops_ok"]; b=d["workload"]["app_bytes"]
print("threads=%d ops=%d wall=%.1fs  writer_cpu=%.2fs (%.2f us/op)  fill_cpu_total=%.2fs  "
      "flush=%.2fs comp=%.2fs  stall=%.1fs" % (
      d["workload"]["threads"], n, f["wall_s"], f["cpu_s"]["writer"],
      f["cpu_s"]["writer"]*1e6/n, f["cpu_s_total"], f["cpu_s"]["flush_pool"],
      f["cpu_s"]["compaction_pool"], d["stalls"]["stall_s"]))
PY
done
