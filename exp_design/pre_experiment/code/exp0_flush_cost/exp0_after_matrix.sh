#!/bin/bash
# exp0_after_matrix.sh - 矩阵完成后自动执行 YCSB-A 与报告阶段
set -x
cd /home/embed/hyl/metadata_offload/exp_design/pre_experiment/code/exp0_flush_cost
LOG=/home/embed/hyl/metadata_offload/output/pre_exp/exp0/after_matrix.log
echo "[$(date '+%F %T')] 矩阵已结束, 开始 YCSB-A 阶段" >> "$LOG"
bash run_exp0.sh ycsb >> "$LOG" 2>&1
echo "[$(date '+%F %T')] YCSB 阶段退出码=$?, 开始 report 阶段" >> "$LOG"
bash run_exp0.sh report >> "$LOG" 2>&1
echo "[$(date '+%F %T')] report 阶段退出码=$?, 开始自检" >> "$LOG"
python3 self_check.py /home/embed/hyl/metadata_offload/exp_design/pre_experiment/report/exp0_report.json >> "$LOG" 2>&1
echo "[$(date '+%F %T')] 全部完成 self_check 退出码=$?" >> "$LOG"
