#!/bin/bash
# step 0a 第二轮：修复后二进制上的三臂对照（用户 2026-09-15 指定）
#   base2 = 整分区下沉（重建可信基线，替换旧 base 历史结果）
#   A0c   = 单文件窄下沉（复核当前策略）
#   B1    = 按 L2 重叠集合合批（ZF_SINK_BATCH=1，新策略）
# 其他参数固定（含 L2 清场阈值 4GB）。每臂结束打印 ZEROFLUSH PROPERTIES 摘要。
set -u
cd "$(dirname "$0")"
./run_arm.sh base2 "ZF_SINK_WHOLE_RANGE=1"
./run_arm.sh A0c   ""
./run_arm.sh B1    "ZF_SINK_BATCH=1"
echo MATRIX_0A2_DONE
