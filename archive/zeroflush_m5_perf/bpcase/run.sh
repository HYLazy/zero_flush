#!/bin/bash
# 背压驱动小用例：低水位（stop=256MB/resume=192MB）迫使 pending 越过 stop，
# 观察：① 从 stop 以上降到 (resume,stop) 仍继续推进并恢复写入；② 停写后最终排空/回收/重开读。
set -u
BASE=/home/embed/hyl/metadata_offload
RUN=$BASE/output/zeroflush_m5_perf/bpcase
BIN=/home/embed/hyl/zf_fix/source/rocksdb-zeroflush/db_bench
export LD_LIBRARY_PATH=/home/embed/hyl/zf_fix/source/rocksdb-zeroflush:/usr/lib/x86_64-linux-gnu
DB=$RUN/db
cd "$RUN"; rm -rf "$DB"
echo "start $(date '+%F %T')" > run.log
echo "bin=$(md5sum $BIN|cut -d' ' -f1) so=$(md5sum /home/embed/hyl/zf_fix/source/rocksdb-zeroflush/librocksdb.so.11|cut -d' ' -f1)" >> run.log
ZF_BENCH_ORPHAN_RECOVER=1 ZF_L0_TRIGGER=100000000 $BIN \
  --benchmarks=fillrandom --key_size=16 --value_size=1024 --num=2000000 \
  --writes=125000 --threads=16 --seed=20260914 --compression_type=none \
  --cache_size=536870912 --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false --use_existing_db=0 \
  --zeroflush --zf_partitions=16 --zf_routing=static --zf_static_span=2000000 \
  --zf_value_cache_mb=64 --zf_base_merge=true --zf_stall_probe_ms=2000 \
  --zf_ledger_stop_mb=256 --zf_ledger_resume_mb=192 \
  --db="$DB" > fill.log 2>&1 &
FILL=$!
# 看门狗：不推进即抓现场（体积/LOG 双不变 ×3）
still=0; ls_=0; ll=0
while kill -0 $FILL 2>/dev/null; do
  sleep 20
  sz=$(du -sb "$DB" 2>/dev/null|cut -f1); lg=$(stat -c%s "$DB/LOG" 2>/dev/null||echo 0)
  [ "$sz" = "$ls_" ] && [ "$lg" = "$ll" ] && still=$((still+1)) || still=0
  ls_=$sz; ll=$lg
  echo "watch $(date '+%T') size=$sz log=$lg stall=$still" >> run.log
  if [ $still -ge 3 ]; then cp "$DB/LOG" hang.LOG 2>/dev/null; kill -KILL $FILL 2>/dev/null; echo "STALL" >> run.log; exit 1; fi
done
wait $FILL; echo "fill_rc=$? fill_end=$(date '+%F %T')" >> run.log
cp "$DB/LOG" fill.LOG 2>/dev/null
$BIN --benchmarks=readrandom --key_size=16 --value_size=1024 --num=2000000 --reads=200000 \
  --threads=16 --seed=20260914 --compression_type=none --use_existing_db=1 \
  --zeroflush --zf_partitions=16 --zf_routing=static --zf_static_span=2000000 \
  --zf_value_cache_mb=64 --zf_base_merge=true --db="$DB" > read.log 2>&1
echo "read_rc=$? read_end=$(date '+%F %T')" >> run.log
echo ALL_DONE >> run.log
