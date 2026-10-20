#!/bin/bash
set -u
cd /home/embed/hyl/metadata_offload/output/zeroflush_m5_perf/sink_tuning
./run_arm.sh base "ZF_SINK_WHOLE_RANGE=1"
./run_arm.sh A ""                              # 默认：按 L1 单文件下沉
./run_arm.sh B "ZF_SINK_WHOLE_RANGE=1 ZF_SINK_L2_CLEAR_MB=8192"
./run_arm.sh AB "ZF_SINK_L2_CLEAR_MB=8192"     # 默认 sink + 8GB 清场
echo MATRIX_DONE
