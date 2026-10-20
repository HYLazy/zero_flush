#!/bin/bash
# 读阶段重测（修正看门狗判据）：
#   读负载不改变 DB 体积、LOG 也几乎不写 ⇒ 原"体积+LOG 静止"判据会**误杀**正常读阶段。
#   改用 **进程 CPU 时间**（/proc/PID/stat utime+stime）作进展信号：真正卡死（futex 等待）时
#   CPU 时间不再增长；正常读则持续增长。
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/b4b17b3_followups
SO=/home/embed/hyl/wt_b4b17b3/source/rocksdb-zeroflush
BIN=$SO/db_bench
export LD_LIBRARY_PATH=$SO:/usr/lib/x86_64-linux-gnu
cd "$RUN"
echo "read-retest start $(date '+%F %T')" >> run.log

WATCH_DB=${WATCH_DB:-}   # 未用，仅保持接口一致
cputime() { awk '{print $14+$15}' /proc/$1/stat 2>/dev/null || echo -1; }

watch_cpu() {  # $1=pid $2=tag $3=logfile
  local pid=$1 tag=$2 lf=$3 still=0 last=-1 cur it=0
  while kill -0 "$pid" 2>/dev/null; do
    sleep 30; it=$((it+1))
    cur=$(cputime "$pid")
    if [ "$cur" = "$last" ]; then still=$((still+1)); else still=0; fi
    last=$cur
    echo "watchcpu[$tag] $(date '+%T') cpu_ticks=$cur stall=$still" >> "$lf"
    if [ "$still" -ge 3 ]; then
      echo "CPU_STALL on $tag $(date '+%F %T')" >> "$lf"
      timeout 240 gdb -p "$pid" -batch -ex "set pagination off" -ex "thread apply all bt 24" > "$RUN/$tag.bt_all.txt" 2>&1
      kill -KILL "$pid" 2>/dev/null
      return 91
    fi
    if [ "$it" -ge 60 ]; then   # 硬上限 30min
      echo "TIMEOUT on $tag" >> "$lf"; kill -KILL "$pid" 2>/dev/null; return 92
    fi
  done
  return 0
}

run_read() {  # $1=tag $2=dbdir $3=num $4=reads $5=extra
  local tag=$1 dbdir=$2 num=$3 reads=$4 extra=$5
  nice -n 5 $BIN --benchmarks=readrandom --num=$num --reads=$reads --use_existing_db=1 \
    $extra > "$tag.log" 2>&1 &
  local pid=$!
  echo "${tag}_pid=$pid start=$(date '+%T')" >> run.log
  watch_cpu "$pid" "$tag" "$RUN/$tag.watch.log"; local w=$?
  wait $pid; local rc=$?
  echo "${tag}_watch=$w ${tag}_rc=$rc ${tag}_end=$(date '+%F %T')" >> run.log
  rm -f "$RUN/$tag.watch.log.tmp"
}

echo "--- A': 读 50GB 臂部分库（2.69GB 现场）---" >> run.log
run_read reads50_retest "$BASE/output/zeroflush_m5_perf/b4b17b3_50gb_1kb/db" 48000000 8000000 \
  "--key_size=16 --value_size=1024 --threads=16 --seed=20260914 --compression_type=none \
--cache_size=536870912 --max_background_jobs=24 --subcompactions=16 --histogram=true --statistics=false \
--zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64 --zf_base_merge=true \
--db=$BASE/output/zeroflush_m5_perf/b4b17b3_50gb_1kb/db"

echo "--- B': 读 100GB 臂部分库（14.41GB 现场）---" >> run.log
run_read reads100_retest "$BASE/output/zeroflush_m5_perf/b4b17b3_100gb/db" 100000000 2000000 \
  "--key_size=16 --value_size=1024 --threads=16 --seed=20260914 --compression_type=none \
--cache_size=536870912 --max_background_jobs=24 --subcompactions=16 --histogram=true --statistics=false \
--zeroflush --zf_partitions=16 --zf_routing=static --zf_static_span=100000000 --zf_value_cache_mb=64 \
--zf_base_merge=true --db=$BASE/output/zeroflush_m5_perf/b4b17b3_100gb/db"

echo "READ_RETEST_ALL_DONE" >> run.log
