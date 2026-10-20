#!/bin/bash
# V2 50GB 优化版 v3：小分区目标（8MB）+ epoch 覆盖全部分区（128MB）+ 批上限 16
# 依据：安装锁把"批次执行"串行化 ⇒ 并行度 = 批内分区任务数。让每轮 epoch 覆盖
# 全部 16 个分区（每分区 8MB）⇒ 批内 16 个并行任务、每任务输入小（低延迟），
# 从而在不改协议的前提下拿到 16 路并行。
set -u
cd /home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/build_rel
export LD_LIBRARY_PATH=$PWD:/usr/lib/x86_64-linux-gnu
export ZF_L0_TRIGGER=100000000
export ZF_SINK_L2_CLEAR_MB=65536
export ZF_DBG_DRAINWORK=1

DB=/tmp/zf_v2_opt3
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
  --zf_partitions=16 --zf_partition_target_mb=8 \
  --zf_epoch_target_mb=128 --zf_max_batch_partitions=16 \
  --zf_materialize_parallelism=16 \
  --zf_base_merge=true --zf_value_cache_mb=64 \
  --db="$DB" > /tmp/zf_v2_opt3_run.log 2>&1
echo "ZF_V2_OPT3_EXIT=$?" >> /tmp/zf_v2_opt3_run.log
