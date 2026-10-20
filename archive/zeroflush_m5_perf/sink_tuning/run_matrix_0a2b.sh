#!/bin/bash
# 补充运行：交错复跑 base/B1，分离运行顺序与热状态对吞吐的干扰
set -u
cd "$(dirname "$0")"
./run_arm.sh base2b "ZF_SINK_WHOLE_RANGE=1"
./run_arm.sh B1b    "ZF_SINK_BATCH=1"
./run_arm.sh base2c "ZF_SINK_WHOLE_RANGE=1"
echo MATRIX_0A2B_DONE
