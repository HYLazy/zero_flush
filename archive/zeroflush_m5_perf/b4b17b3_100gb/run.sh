#!/bin/bash
# 100GB / 1KB KV 读写测试 —— **候选线提交 b4b17b3**（worktree /home/embed/hyl/wt_b4b17b3）
# 参数与 bench100g_full（bench-100g 线）逐字一致：num=100M、16×6.25M 写、**固定 range 路由**。
# 差异（b4b17b3 上不存在，故删除）：`--zf_stall_probe_ms`、`ZF_BENCH_ORPHAN_RECOVER`、`ZF_DBG_BUSY`。
# 看门狗同上（30s 采样，连续 3 次无进展即抓现场终止），另加磁盘余量保护。
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/b4b17b3_100gb
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

STALL_LIMIT=3
MAX_WATCH_ITERS=320     # 硬上限 ≈160min
MIN_FREE_GB=200         # 100GB 规模下磁盘余量下限
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

# ---------------- 写阶段：100M 键 × 1KB ≈ 100GB ----------------
ZF_L0_TRIGGER=100000000 nice -n 5 $BIN \
  --benchmarks=fillrandom --key_size=16 --value_size=1024 --num=100000000 \
  --writes=6250000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=0 \
  --zeroflush --zf_partitions=16 --zf_routing=static --zf_static_span=100000000 \
  --zf_value_cache_mb=64 --zf_base_merge=true --db="$DB" > fill.log 2>&1 &
FILL=$!
echo "fill_pid=$FILL" >> run.log
watch_phase "$FILL" fill || true
wait $FILL; rc=$?
echo "fill_rc=$rc fill_end=$(date '+%F %T')" >> run.log
[ -f stall_marker.txt ] && { echo "ALL_DONE(INCOMPLETE)"; exit 91; }
cp "$DB/LOG" fill.LOG 2>/dev/null

# ---------------- 读阶段：2M/线程 = 32M 次点读 ----------------
nice -n 5 $BIN --benchmarks=readrandom --key_size=16 --value_size=1024 --num=100000000 \
  --reads=2000000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=1 \
  --zeroflush --zf_partitions=16 --zf_routing=static --zf_static_span=100000000 \
  --zf_value_cache_mb=64 --zf_base_merge=true --db="$DB" > read.log 2>&1 &
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
