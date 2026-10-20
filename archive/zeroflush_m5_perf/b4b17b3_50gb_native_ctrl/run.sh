#!/bin/bash
# 同窗控制：native RocksDB 50GB/1KB（与 b4b17b3_50gb_1kb **逐字同参数**）
# 目的：排除"本机/磁盘此刻异常"这一解释——若 native 能跑完而 ZF@b4b17b3 停住，
#       则停滞归因于 b4b17b3 代码路径。看门狗与 ZF 臂同款，保证判据一致。
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/b4b17b3_50gb_native_ctrl
BIN=$BASE/source/rocksdb/build/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb/build:/usr/lib/x86_64-linux-gnu
DB=$RUN/db
mkdir -p "$RUN"; cd "$RUN"
rm -rf "$DB"
rm -f bt_all.txt hang_bt.txt stall_marker.txt fill.log read.log
echo "start $(date '+%F %T')" > run.log
echo "bin=$(md5sum $BIN | cut -d' ' -f1)" >> run.log

STALL_LIMIT=3; MAX_WATCH_ITERS=320; MIN_FREE_GB=150
watch_phase() {
  local pid=$1 phase=$2 still=0 last_sz=0 last_log=0 it=0 sz lg free
  while kill -0 "$pid" 2>/dev/null; do
    sleep 30; it=$((it+1))
    sz=$(du -sb "$DB" 2>/dev/null | cut -f1); lg=$(stat -c%s "$DB/LOG" 2>/dev/null || echo 0)
    free=$(df -BG --output=avail "$RUN" | tail -1 | tr -dc '0-9')
    if [ "${sz:-0}" -gt 0 ] && [ "$sz" = "$last_sz" ] && [ "$lg" = "$last_log" ]; then
      still=$((still+1)); else still=0; fi
    last_sz=${sz:-0}; last_log=${lg:-0}
    echo "watch[$phase] $(date '+%T') size=$sz log=$lg stall=$still freeGB=$free" >> run.log
    if [ "${free:-9999}" -lt "$MIN_FREE_GB" ]; then
      echo "ABORT_DISK_LOW $(date '+%F %T') freeGB=$free" >> run.log; kill -KILL "$pid" 2>/dev/null; exit 90
    fi
    if [ "$still" -ge "$STALL_LIMIT" ] || [ "$it" -ge "$MAX_WATCH_ITERS" ]; then
      echo "STALL/TIMEOUT on $phase $(date '+%F %T') iters=$it" >> run.log
      echo "STALL on $phase" > stall_marker.txt
      cp "$DB/LOG" "$phase.hang.LOG" 2>/dev/null
      timeout 240 gdb -p "$pid" -batch -ex "set pagination off" -ex "thread apply all bt 24" > bt_all.txt 2>&1
      kill -KILL "$pid" 2>/dev/null
      echo "STALL_CAPTURED" >> run.log; exit 91
    fi
  done
  return 0
}

nice -n 5 $BIN --benchmarks=fillrandom --key_size=16 --value_size=1024 --num=48000000 \
  --writes=3000000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=0 \
  --db="$DB" > fill.log 2>&1 &
FILL=$!
echo "fill_pid=$FILL" >> run.log
watch_phase "$FILL" fill || true
wait $FILL; rc=$?
echo "fill_rc=$rc fill_end=$(date '+%F %T')" >> run.log
[ -f stall_marker.txt ] && { echo "ALL_DONE(INCOMPLETE)" >> run.log; exit 91; }
cp "$DB/LOG" fill.LOG 2>/dev/null

nice -n 5 $BIN --benchmarks=readrandom --key_size=16 --value_size=1024 --num=48000000 \
  --reads=8000000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=1 \
  --db="$DB" > read.log 2>&1 &
READ=$!
echo "read_pid=$READ" >> run.log
watch_phase "$READ" read || true
wait $READ; rc=$?
echo "read_rc=$rc read_end=$(date '+%F %T')" >> run.log
[ -f stall_marker.txt ] && { echo "ALL_DONE(INCOMPLETE)" >> run.log; exit 91; }
cp "$DB/LOG" read.LOG 2>/dev/null

du -sh "$DB" >> run.log
for ext in sst log; do
  n=$(find "$DB" -name "*.$ext" | wc -l)
  s=$(find "$DB" -name "*.$ext" -printf "%s\n" | awk '{a+=$1} END {printf "%.2f", a/1073741824}')
  echo "$ext n=$n size=${s}GB" >> run.log
done
echo "ALL_DONE" >> run.log
