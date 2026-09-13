#!/bin/bash
# 物化滞后专项：4M 键 / 4M 写（4GB 用户数据）/ merge ON / 锁开
# 完整捕获 stderr（不截断），用于决策计数与安装时序分析
set -u
BASE=/home/embed/hyl/metadata_offload
OUT=$BASE/output/zeroflush_m5_perf/materialize_lag
BIN=$BASE/source/rocksdb-zeroflush/build_rel/db_bench
export LD_LIBRARY_PATH=$BASE/source/rocksdb-zeroflush/build_rel:/usr/lib/x86_64-linux-gnu
mkdir -p "$OUT/diag"
DB=$OUT/diag/db_4m
rm -rf "$DB"
ZF_L0_TRIGGER=100000000 $BIN \
  --benchmarks=fillrandom --key_size=16 --value_size=1024 --num=250000 \
  --threads=16 --compression_type=none --cache_size=536870912 \
  --max_background_jobs=24 --subcompactions=16 --statistics=false \
  --use_existing_db=0 --zeroflush --zf_partitions=16 --zf_routing=sampled \
  --zf_value_cache_mb=64 --zf_base_merge=true --db="$DB" \
  > "$OUT/diag/fill.stderr" 2>&1
echo "fill_rc=$?" > "$OUT/diag/run.txt"
cp "$DB/LOG" "$OUT/diag/fill.LOG" 2>/dev/null
# 统计：安装事件、封存事件、决策分布
echo "install_total=$(grep -c 'ZFDBG-install' "$OUT/diag/fill.stderr")" >> "$OUT/diag/run.txt"
echo "frz_total=$(grep -c 'ZFDBG-frz' "$OUT/diag/fill.stderr")" >> "$OUT/diag/run.txt"
echo "frz_zero=$(grep 'ZFDBG-frz' "$OUT/diag/fill.stderr" | grep -c 'size=0')" >> "$OUT/diag/run.txt"
echo "last_install_line=$(grep -n 'ZFDBG-install' "$OUT/diag/fill.stderr" | tail -1 | cut -d: -f1)" >> "$OUT/diag/run.txt"
echo "total_lines=$(wc -l < "$OUT/diag/fill.stderr")" >> "$OUT/diag/run.txt"
echo "last_frz_line=$(grep -n 'ZFDBG-frz' "$OUT/diag/fill.stderr" | tail -1 | cut -d: -f1)" >> "$OUT/diag/run.txt"
du -sh "$DB" >> "$OUT/diag/run.txt"
for ext in sst log; do n=$(find "$DB" -name "*.$ext" | wc -l); s=$(find "$DB" -name "*.$ext" -printf "%s\n" | awk '{a+=$1} END {printf "%.2f", a/1073741824}'); echo "$ext n=$n size=${s}GB" >> "$OUT/diag/run.txt"; done
cat "$OUT/diag/run.txt"
