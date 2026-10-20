#!/bin/bash
# b4b17b3 后续取证（四相，顺序执行，避免 NVMe 争用）：
#   A. 读 50GB 臂的**部分库**（2.69GB，停滞现场）——读延迟/吞吐（非 50GB 口径）
#   B. 读 100GB 臂的部分库（14.41GB）——读延迟/吞吐（非 100GB 口径）
#   C. ZF 写 smoke（同配置、规模降到停滞点以下 ~1GB）——取得**健康期**写延迟/吞吐
#   D. ZF 50GB 配置**复跑**——判定停滞是否可复现
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/b4b17b3_followups
SO=/home/embed/hyl/wt_b4b17b3/source/rocksdb-zeroflush
BIN=$SO/db_bench
export LD_LIBRARY_PATH=$SO:/usr/lib/x86_64-linux-gnu
mkdir -p "$RUN"; cd "$RUN"
echo "start $(date '+%F %T')" > run.log
echo "bin=$(md5sum $BIN | cut -d' ' -f1) so=$(md5sum $SO/librocksdb.so.11 | cut -d' ' -f1)" >> run.log

STALL_LIMIT=3; MAX_WATCH_ITERS=320; MIN_FREE_GB=150
watch_phase() {  # $1=pid $2=tag $3=dbdir $4=logfile
  local pid=$1 tag=$2 dbdir=$3 lf=$4 still=0 last_sz=0 last_log=0 it=0 sz lg free
  while kill -0 "$pid" 2>/dev/null; do
    sleep 30; it=$((it+1))
    sz=$(du -sb "$dbdir" 2>/dev/null | cut -f1); lg=$(stat -c%s "$dbdir/LOG" 2>/dev/null || echo 0)
    free=$(df -BG --output=avail "$RUN" | tail -1 | tr -dc '0-9')
    if [ "${sz:-0}" -gt 0 ] && [ "$sz" = "$last_sz" ] && [ "$lg" = "$last_log" ]; then
      still=$((still+1)); else still=0; fi
    last_sz=${sz:-0}; last_log=${lg:-0}
    echo "watch[$tag] $(date '+%T') size=$sz log=$lg stall=$still freeGB=$free" >> "$lf"
    if [ "${free:-9999}" -lt "$MIN_FREE_GB" ]; then
      echo "ABORT_DISK_LOW" >> "$lf"; kill -KILL "$pid" 2>/dev/null; return 90; fi
    if [ "$still" -ge "$STALL_LIMIT" ] || [ "$it" -ge "$MAX_WATCH_ITERS" ]; then
      echo "STALL/TIMEOUT on $tag $(date '+%F %T') iters=$it" >> "$lf"
      cp "$dbdir/LOG" "$RUN/$tag.hang.LOG" 2>/dev/null
      timeout 240 gdb -p "$pid" -batch -ex "set pagination off" -ex "thread apply all bt 24" > "$RUN/$tag.bt_all.txt" 2>&1
      kill -KILL "$pid" 2>/dev/null
      return 91
    fi
  done
  return 0
}

wf() {  # 记录一相的起止与退出码: $1=tag $2=...cmd
  local tag=$1; shift
  "$@" & local pid=$!
  echo "${tag}_pid=$pid start=$(date '+%T')" >> run.log
  watch_phase "$pid" "$tag" "${WATCH_DB:-$RUN/db}" "$RUN/$tag.watch.log"
  local w=$?
  wait $pid; local rc=$?
  echo "${tag}_watch=$w ${tag}_rc=$rc ${tag}_end=$(date '+%F %T')" >> run.log
  return $rc
}

ZF_COMMON="--key_size=16 --value_size=1024 --threads=16 --seed=20260914 --compression_type=none \
--cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
--histogram=true --statistics=false \
--zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64 --zf_base_merge=true"

# ---------- A. 读 50GB 臂部分库 ----------
DB50=$BASE/output/zeroflush_m5_perf/b4b17b3_50gb_1kb/db
WATCH_DB=$DB50 wf readpartial50 nice -n 5 $BIN --benchmarks=readrandom --num=48000000 \
  --reads=8000000 --use_existing_db=1 $ZF_COMMON --db="$DB50" > read_partial_50.log 2>&1

# ---------- B. 读 100GB 臂部分库 ----------
DB100=$BASE/output/zeroflush_m5_perf/b4b17b3_100gb/db
WATCH_DB=$DB100 wf readpartial100 nice -n 5 $BIN --benchmarks=readrandom --num=100000000 \
  --reads=2000000 --use_existing_db=1 \
  --key_size=16 --value_size=1024 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false \
  --zeroflush --zf_partitions=16 --zf_routing=static --zf_static_span=100000000 \
  --zf_value_cache_mb=64 --zf_base_merge=true --db="$DB100" > read_partial_100.log 2>&1

# ---------- C. ZF 写 smoke（停滞点以下 ~1GB）----------
DB_SM=$RUN/db_smoke; rm -rf "$DB_SM"
WATCH_DB=$DB_SM wf wsmoke env ZF_L0_TRIGGER=100000000 nice -n 5 $BIN \
  --benchmarks=fillrandom --num=48000000 --writes=60000 --use_existing_db=0 \
  $ZF_COMMON --db="$DB_SM" > fill_smoke.log 2>&1
cp "$DB_SM/LOG" smoke.LOG 2>/dev/null

# ---------- D. ZF 50GB 配置复跑（停滞可复现性）----------
DB_R2=$RUN/db_repeat; rm -rf "$DB_R2"
WATCH_DB=$DB_R2 wf wrepeat env ZF_L0_TRIGGER=100000000 nice -n 5 $BIN \
  --benchmarks=fillrandom --num=48000000 --writes=3000000 --use_existing_db=0 \
  $ZF_COMMON --db="$DB_R2" > fill_repeat.log 2>&1
cp "$DB_R2/LOG" repeat.LOG 2>/dev/null

echo "ALL_DONE" >> run.log
