#!/bin/bash
# V2 50GB 优化版 v1：抬 L2 清场阈值（让 sink 持续流）+ 提高物化并行度
# 目标：超过同机原生基线（/tmp/native_50gb_run.log）
set -u
cd /home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/build_rel
export LD_LIBRARY_PATH=$PWD:/usr/lib/x86_64-linux-gnu
export ZF_L0_TRIGGER=100000000
export ZF_MAX_L1_BYTES=1000000000000     # 显式覆盖：V2 独占 L1（等同 V2 默认）
export ZF_SINK_L2_CLEAR_MB=65536         # 64GB：50GB 运行内不再触发 L2 清场
export ZF_DBG_DRAINWORK=1
export ZF_SINK_YIELD_MB=65536   # 64GB：永不触发下沉让位（L1 独占）

DB=/tmp/zf_v2_opt
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
  --zf_partitions=16 --zf_epoch_target_mb=256 --zf_partition_target_mb=64 \
  --zf_materialize_parallelism=16 --zf_max_pending_epochs=128 --zf_ledger_stop_mb=32768 --zf_ledger_resume_mb=24576 --zf_base_merge_skip_max_generations=32 \
  --zf_base_merge=true --zf_value_cache_mb=64 \
  --db="$DB" > /tmp/zf_v2_opt7_run.log 2>&1
echo "ZF_V2_OPT_EXIT=$?" >> /tmp/zf_v2_opt7_run.log
