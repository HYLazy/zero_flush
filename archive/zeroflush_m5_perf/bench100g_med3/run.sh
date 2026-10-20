#!/bin/bash
# 受监测复现：同 100GB 参数、规模降到 30GB；停滞即抓现场（DelayWrite 条件 + busy 文件），
# 不再空等。
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/bench100g_med3
BIN=/home/embed/hyl/zf_fix/source/rocksdb-zeroflush/db_bench
SO=/home/embed/hyl/zf_fix/source/rocksdb-zeroflush
export LD_LIBRARY_PATH=$SO:/usr/lib/x86_64-linux-gnu
DB=$RUN/db
cd "$RUN"
rm -rf "$DB"
echo "start $(date '+%F %T')" > run.log
echo "bin=$(md5sum $BIN|cut -d' ' -f1) so=$(md5sum $SO/librocksdb.so.11|cut -d' ' -f1)" >> run.log
ZF_BENCH_ORPHAN_RECOVER=1 ZF_L0_TRIGGER=100000000 ZF_DBG_BUSY=1 $BIN \
  --benchmarks=fillrandom --key_size=16 --value_size=1024 --num=30000000 \
  --writes=1875000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=0 \
  --zeroflush --zf_partitions=16 --zf_routing=static --zf_static_span=30000000 \
  --zf_value_cache_mb=64 --zf_base_merge=true --zf_stall_probe_ms=2000 --db="$DB" > fill.log 2>&1 &
FILL=$!
echo "fill_pid=$FILL" >> run.log
# ---- 看门狗：连续 3 次采样（每 30s）体积与 LOG 均不变 ⇒ 判停滞并抓现场 ----
still=0; last_sz=0; last_log=0
while kill -0 $FILL 2>/dev/null; do
  sleep 30
  sz=$(du -sb "$DB" 2>/dev/null | cut -f1); lg=$(stat -c%s "$DB/LOG" 2>/dev/null || echo 0)
  if [ "$sz" = "$last_sz" ] && [ "$lg" = "$last_log" ]; then still=$((still+1)); else still=0; fi
  last_sz=$sz; last_log=$lg
  echo "watch $(date '+%T') size=$sz log=$lg stall=$still" >> run.log
  if [ $still -ge 3 ]; then
    echo "STALL detected $(date '+%F %T') size=$sz log=$lg" >> run.log
    cp "$DB/LOG" hang.LOG 2>/dev/null
    timeout 180 gdb -p $FILL -batch -ex "set pagination off" -ex "thread apply all bt 24" > bt_all.txt 2>&1
    T=$(awk '/^Thread/{t=$2} /DelayWrite/{print t; exit}' bt_all.txt)
    if [ -z "${T:-}" ]; then T=$(awk '/^Thread/{t=$2} /InstrumentedCondVar::TimedWait/{print t; exit}' bt_all.txt); fi
    if [ -n "${T:-}" ]; then
      # 先取该线程完整帧表，定位 DelayWrite 所在帧号（不硬编码）
      timeout 120 gdb -p $FILL -batch -ex "set pagination off" -ex "thread $T" -ex "bt 24" > t_frames.txt 2>&1
      F=$(awk '/^#/{n=$1; sub("#","",n); if (/DelayWrite/ || /PreprocessWrite/) {print n; exit}}' t_frames.txt)
      [ -z "${F:-}" ] && F=0
      echo "leader thread=$T frame=$F" >> run.log
      # 在该 leader 帧内直接读成员（**不调用可能加锁的函数**）
      timeout 180 gdb -p $FILL -batch -ex "set pagination off" \
        -ex "thread $T" -ex "frame $F" \
        -ex "info frame" \
        -ex "p this->unscheduled_flushes_" \
        -ex "p this->bg_flush_scheduled_" \
        -ex "p this->bg_compaction_scheduled_" \
        -ex "p this->immutable_db_options_.max_write_buffer_number" \
        -ex "p this->default_cf_handle_->cfd()->imm()->NumNotFlushed()" \
        -ex "p this->default_cf_handle_->cfd()->current()->storage_info()->LevelFiles(0).size()" \
        -ex "p this->write_controller_.stoppable_count_" \
        -ex "p this->write_controller_.total_stopped_" \
        -ex "p this->write_controller_.total_delayed_" > stall_cond.txt 2>&1
      echo "captured thread=$T" >> run.log
    fi
    cp bt_all.txt hang_bt.txt 2>/dev/null
    kill -KILL $FILL 2>/dev/null
    echo "STALL_CAPTURED" >> run.log
    exit 0
  fi
done
wait $FILL; rc=$?
echo "fill_rc=$rc fill_end=$(date '+%F %T')" >> run.log
cp "$DB/LOG" fill.LOG 2>/dev/null
echo ALL_DONE >> run.log
