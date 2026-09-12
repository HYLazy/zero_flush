#!/bin/bash
# 50GB / 1KB merge 关闭（公平版）vs 原生 RocksDB —— 带环境闸门与遥测
#
# 背景（2026-09-12）：此前两轮 DNF 系系统盘 WD_BLACK SN750 处于退化状态
# （flush 延迟尖峰 8.5s、盘温贴 83.8C 节流线、830GiB 未 TRIM 空间）。
# 已执行 fstrim（交还 830GiB）+ wbt_lat_usec=0 + commit=60。
# 本脚本增加：
#   1) 每臂开跑前的降温闸门（等盘温 <= COOL_TARGET，最多等 COOL_MAX_WAIT）
#   2) 30s 遥测：load / 盘温 / 吞吐 / 平均写延迟 / 进度
#   3) DNF 守卫：单臂 fill 超过 FILL_TIMEOUT 或 10 分钟无进度 → 杀掉并落 DNF 证据
#
# 顺序：ZF merge-off 公平版（默认攒批） → 原生（同参数），各含 fillrandom + readrandom
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/50gb_1kb_merge_off/rerun_20260912
ZF_BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
NAT_BIN=$BASE/source/rocksdb/build/db_bench
DB_ZF=$BASE/output/zeroflush_m5_perf/db_zf_50gb
DB_NAT=$BASE/output/zeroflush_m5_perf/db_nat_50gb

NUM=48000000
READS=8000000
COOL_TARGET=${COOL_TARGET:-74}      # 摄氏：低于此温度才开跑
COOL_MAX_WAIT=${COOL_MAX_WAIT:-1800} # 降温最多等 30 分钟
FILL_TIMEOUT=${FILL_TIMEOUT:-21600}  # 单臂 fill 上限 6 小时
STALL_LIMIT=${STALL_LIMIT:-900}      # 900s 无进度视为停摆

COMMON="--key_size=16 --value_size=1024 --num=$NUM --threads=16 --compression_type=none \
--cache_size=536870912 --max_background_jobs=24 --subcompactions=16 --histogram=true --statistics=false"
COMMON_R="--key_size=16 --value_size=1024 --num=$NUM --threads=16 --compression_type=none \
--cache_size=536870912 --max_background_jobs=24 --subcompactions=16 --histogram=true --reads=$READS"
# 公平版：不显式设置 skip_batching（默认 true=攒批）、不显式设置 base_merge（默认 false=merge 关闭）
ZF_FLAGS="--zeroflush --zf_partitions=16 --zf_routing=sampled --zf_value_cache_mb=64"

cd "$RUN"
echo "run start: $(date '+%F %T')" >> run2.log

nvme_temp() { sensors 2>/dev/null | awk '/Composite:/{for(i=1;i<=NF;i++) if ($i ~ /^\+[0-9.]+/) {gsub(/[+°C]/,"",$i); print $i; exit}}'; }

cooldown() {
  local t0=$(date +%s)
  while :; do
    local tp=$(nvme_temp); tp=${tp:-999}
    echo "  cooldown: temp=${tp}C elapsed=$(( $(date +%s)-t0 ))s" >> run2.log
    awk -v a="$tp" -v b="$COOL_TARGET" 'BEGIN{exit !(a+0<=b+0)}' && return 0
    [ $(( $(date +%s)-t0 )) -ge "$COOL_MAX_WAIT" ] && { echo "  cooldown timeout, 以 ${tp}C 开跑" >> run2.log; return 0; }
    sleep 60
  done
}

# 遥测 + 停摆守卫：$1=tag $2=logfile
telemetry() {
  local tag=$1 log=$2
  local last=0 last_change=$(date +%s) stalled=0
  while :; do
    sleep 30
    local tp=$(nvme_temp); tp=${tp:-NA}
    local prog=$(tr '\r' '\n' < "$log" 2>/dev/null | grep -o 'finished [0-9]* ops' | awk '{print $2}' | sort -n | tail -1)
    prog=${prog:-0}
    local lat=$(awk '{print $8" "$5}' /sys/block/nvme0n1/stat)
    echo "$(date '+%F %T') [$tag] temp=${tp}C load=$(cut -d' ' -f1 /proc/loadavg) ops=$prog inflight=$(awk '{print $9}' /sys/block/nvme0n1/stat)" >> run2_telemetry.log
    if [ "$prog" -gt "$last" ]; then last_change=$(date +%s); stalled=0; else stalled=$(( $(date +%s) - last_change )); fi
    last=$prog
    if [ "$stalled" -ge "$STALL_LIMIT" ]; then
      echo "$(date '+%F %T') [$tag] STALL GUARD: ${stalled}s 无进度，杀 db_bench" >> run2.log
      pkill -x db_bench
      break
    fi
  done
}

run_arm() {
  local tag=$1 bin=$2 db=$3 flags=$4 logpfx=$5
  echo "[$tag] $(date '+%F %T') cooldown..." >> run2.log
  cooldown
  local t=$(nvme_temp)
  echo "[$tag] $(date '+%F %T') fill 开始 temp=${t}C" >> run2.log
  telemetry "$tag" "${logpfx}_fill.log" &
  local tel=$!
  rm -rf "$db"
  local t0=$(date +%s)
  timeout -k 60 "$FILL_TIMEOUT" env LD_LIBRARY_PATH="${LD_LIBRARY_PATH_BIN}" "$bin" --benchmarks=fillrandom $COMMON $flags --use_existing_db=0 --db="$db" > "${logpfx}_fill.log" 2>&1
  local rc=$?
  local t1=$(date +%s)
  kill $tel 2>/dev/null; wait $tel 2>/dev/null
  echo "${tag}_FILL_RC=$rc wall=$((t1-t0))s temp_end=$(nvme_temp)C" >> run2.log
  echo "[$tag] fill 结束 rc=$rc wall=$((t1-t0))s temp=$(nvme_temp)C" >> run2.log

  echo "[$tag] $(date '+%F %T') read 开始" >> run2.log
  local r0=$(date +%s)
  LD_LIBRARY_PATH="${LD_LIBRARY_PATH_BIN}" "$bin" --benchmarks=readrandom $COMMON_R $flags --use_existing_db=1 --db="$db" > "${logpfx}_read.log" 2>&1
  echo "${tag}_READ_RC=$? wall=$(( $(date +%s)-r0 ))s" >> run2.log
  echo "[$tag] read 结束 $(date '+%F %T')" >> run2.log
}

# ---- ZF merge-off 公平版 ----
LD_LIBRARY_PATH_BIN=/home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/build_rel:/usr/lib/x86_64-linux-gnu
run_arm "ZF" "$ZF_BIN" "$DB_ZF" "$ZF_FLAGS" "zf"
# ---- 原生 RocksDB ----
LD_LIBRARY_PATH_BIN=$BASE/source/rocksdb/build:/usr/lib/x86_64-linux-gnu
run_arm "NAT" "$NAT_BIN" "$DB_NAT" "" "native"

python3 parse_results.py >> run2.log 2>&1
echo "ALL_DONE $(date '+%F %T')" >> run2.log
