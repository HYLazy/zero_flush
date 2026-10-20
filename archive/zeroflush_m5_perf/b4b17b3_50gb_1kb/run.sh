#!/bin/bash
# 50GB / 1KB KV 读写测试 —— **候选线提交 b4b17b3**（worktree /home/embed/hyl/wt_b4b17b3）
# 参数与 c59fc41_50gb_1kb / native_50gb_c59fc41 / zf_50gb_noprobe 逐字一致（sampled 路由，同窗可对照）。
# 加看门狗：连续 3 次采样（30s）DB 体积与 LOG 均不变 ⇒ 判停滞，抓线程栈与条件后终止（不空等）。
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/b4b17b3_50gb_1kb
SO=/home/embed/hyl/wt_b4b17b3/source/rocksdb-zeroflush
BIN=$SO/db_bench
export LD_LIBRARY_PATH=$SO:/usr/lib/x86_64-linux-gnu
DB=$RUN/db
mkdir -p "$RUN"; cd "$RUN"
rm -rf "$DB"
rm -f bt_all.txt hang_bt.txt stall_cond.txt t_frames.txt fill.log read.log hang.LOG stall_marker.txt
echo "start $(date '+%F %T')" > run.log
echo "bin=$(md5sum $BIN | cut -d' ' -f1) so=$(md5sum $SO/librocksdb.so.11 | cut -d' ' -f1)" >> run.log
git -C /home/embed/hyl/metadata_offload log --oneline -1 b4b17b3 >> run.log 2>/dev/null

STALL_LIMIT=3          # 连续 3 次无进展判停滞
MAX_WATCH_ITERS=320    # 硬上限 ≈160min
MIN_FREE_GB=150        # 磁盘余量下限，低于即中止保护
watch_phase() {  # $1=pid  $2=phase
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
      echo "ABORT_DISK_LOW $(date '+%F %T') freeGB=$free" >> run.log
      cp "$DB/LOG" "$phase.hang.LOG" 2>/dev/null; kill -KILL "$pid" 2>/dev/null; exit 90
    fi
    if [ "$still" -ge "$STALL_LIMIT" ] || [ "$it" -ge "$MAX_WATCH_ITERS" ]; then
      if [ "$it" -ge "$MAX_WATCH_ITERS" ]; then echo "TIMEOUT $(date '+%F %T') iters=$it" >> run.log
      else echo "STALL detected $(date '+%F %T') size=$sz log=$lg" >> run.log; fi
      echo "STALL/limit on $phase" > stall_marker.txt
      cp "$DB/LOG" "$phase.hang.LOG" 2>/dev/null
      timeout 240 gdb -p "$pid" -batch -ex "set pagination off" -ex "thread apply all bt 24" > bt_all.txt 2>&1
      T=$(awk '/^Thread/{t=$2} /DelayWrite/{print t; exit}' bt_all.txt)
      [ -z "${T:-}" ] && T=$(awk '/^Thread/{t=$2} /InstrumentedCondVar::TimedWait/{print t; exit}' bt_all.txt)
      if [ -n "${T:-}" ]; then
        timeout 120 gdb -p "$pid" -batch -ex "set pagination off" -ex "thread $T" -ex "bt 24" > t_frames.txt 2>&1
        F=$(awk '/^#/{n=$1; sub("#","",n); if (/DelayWrite/ || /PreprocessWrite/) {print n; exit}}' t_frames.txt)
        [ -z "${F:-}" ] && F=0
        echo "leader thread=$T frame=$F" >> run.log
        timeout 180 gdb -p "$pid" -batch -ex "set pagination off" \
          -ex "thread $T" -ex "frame $F" -ex "info frame" \
          -ex "p this->unscheduled_flushes_" -ex "p this->bg_flush_scheduled_" \
          -ex "p this->bg_compaction_scheduled_" -ex "p this->write_controller_.stoppable_count_" \
          -ex "p this->write_controller_.total_stopped_" -ex "p this->write_controller_.total_delayed_" \
          > stall_cond.txt 2>&1
      fi
      cp bt_all.txt hang_bt.txt 2>/dev/null
      kill -KILL "$pid" 2>/dev/null
      echo "STALL_CAPTURED" >> run.log
      exit 91
    fi
  done
  return 0
}

# ---------------- 写阶段：48M 键 × 1KB ≈ 50GB ----------------
ZF_L0_TRIGGER=100000000 nice -n 5 $BIN \
  --benchmarks=fillrandom --key_size=16 --value_size=1024 --num=48000000 \
  --writes=3000000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=0 \
  --zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64 \
  --zf_base_merge=true --db="$DB" > fill.log 2>&1 &
FILL=$!
echo "fill_pid=$FILL" >> run.log
watch_phase "$FILL" fill || true
wait $FILL; rc=$?
echo "fill_rc=$rc fill_end=$(date '+%F %T')" >> run.log
[ -f stall_marker.txt ] && { echo "ALL_DONE(INCOMPLETE)"; exit 91; }
cp "$DB/LOG" fill.LOG 2>/dev/null

# ---------------- 读阶段：8M/线程 = 128M 次点读 ----------------
nice -n 5 $BIN --benchmarks=readrandom --key_size=16 --value_size=1024 --num=48000000 \
  --reads=8000000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=1 \
  --zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64 \
  --zf_base_merge=true --db="$DB" > read.log 2>&1 &
READ=$!
echo "read_pid=$READ" >> run.log
watch_phase "$READ" read || true
wait $READ; rc=$?
echo "read_rc=$rc read_end=$(date '+%F %T')" >> run.log
[ -f stall_marker.txt ] && { echo "ALL_DONE(INCOMPLETE)"; exit 91; }
cp "$DB/LOG" read.LOG 2>/dev/null

du -sh "$DB" >> run.log
for ext in sst log; do
  n=$(find "$DB" -name "*.$ext" | wc -l)
  s=$(find "$DB" -name "*.$ext" -printf "%s\n" | awk '{a+=$1} END {printf "%.2f", a/1073741824}')
  echo "$ext n=$n size=${s}GB" >> run.log
done
echo "ALL_DONE" >> run.log
