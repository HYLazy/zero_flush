#!/bin/bash
# step 0b-1 验证：默认（整分区）+ 排空阶段 + 末态记录；并复核 ZF_SINK_NARROW 开关
set -u
cd "$(dirname "$0")"
./run_arm.sh base3 ""                    # 默认 = 整分区（新默认，无 env）
./run_arm.sh A0d "ZF_SINK_NARROW=1"      # 单文件窄下沉（新开关名）
echo MATRIX_0B1_DONE
