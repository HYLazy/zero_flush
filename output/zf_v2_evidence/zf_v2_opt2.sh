#!/bin/bash
# V2 50GB 优化版 v2：批次覆盖全部分区（16 任务/批）+ sink 持续 + 水位放大
# 依据：v1 抓栈显示安装锁串行化导致同刻只有 ~1 批（3–5 个分区任务）在归并、
# 进程仅用 0.84 核。把「批内并行度」提到 16（= 分区数）即可在不改协议的前提下
# 把并行度提高 ~4–16×。
set -u
cd /home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/build_rel
export LD_LIBRARY_PATH=$PWD:/usr/lib/x86_64-linux-gnu
export ZF_L0_TRIGGER=100000000
export ZF_SINK_L2_CLEAR_MB=65536         # 64GB：50GB 运行内不触发 L2 清场
export ZF_DBG_DRAINWORK=1

DB=/tmp/zf_v2_opt2
rm -rf "$DB"
/usr/bin/time -v ./db_bench \
  --benchmarks=fillrandom,waitforcompaction \
  --key_size=16 --value_size=1024 \
  --num=48000000 --writes=3000000 --threads=16 --seed=20260914 \
  --compression_type=none --cache_size=536870912 \
  --max_background_jobs=24 --subcompactions=16 \
  --histogram=true --statistics=false \
  --stats_interval_seconds=30 --stats_per_interval=1 \
  --zeroflush --zf_v2=1 --zf_routing=static --zf_static_span=48000000 \
  --zf_partitions=16 --zf_partition_target_mb=64 \
  --zf_epoch_target_mb=1024 --zf_max_batch_partitions=16 \
  --zf_materialize_parallelism=16 \
  --zf_ledger_stop_mb=16384 --zf_ledger_resume_mb=12288 \
  --zf_base_merge=true --zf_value_cache_mb=64 \
  --db="$DB" > /tmp/zf_v2_opt2_run.log 2>&1
echo "ZF_V2_OPT2_EXIT=$?" >> /tmp/zf_v2_opt2_run.log
