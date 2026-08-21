#!/bin/bash
# run_exp2.sh - 实验 2「分区开销门」编排：编译 + 全部真实运行 + 聚合 + 判定 + 报告
#
# 产物：
#   output/pre_exp/exp2/raw/          每次运行的原始 JSON
#   output/pre_exp/exp2/logs/         每次运行的 stdout/stderr 日志
#   output/pre_exp/exp2/aggregated.json / decision.json
#   exp_design/pre_experiment/report/exp2_report.json
#
# 断点续跑：已存在的原始 JSON 默认跳过（FULL_RERUN=1 强制全部重跑）。

set -euo pipefail

CODE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CODE_ROOT="$(dirname "${CODE_DIR}")"
WS_ROOT="$(cd "${CODE_ROOT}/../../.." && pwd)"
BIN_DIR="${CODE_DIR}/bin"
OUT_DIR="${WS_ROOT}/output/pre_exp/exp2"
RAW_DIR="${OUT_DIR}/raw"
LOG_DIR="${OUT_DIR}/logs"
WAL_BASE="${OUT_DIR}/wal_data"
REPORT_DIR="${WS_ROOT}/exp_design/pre_experiment/report"
REPORT="${REPORT_DIR}/exp2_report.json"

REPS=3
CXX="${CXX:-g++}"
CXXFLAGS="-std=c++17 -O2 -Wall -Wextra -pthread -I${CODE_ROOT}"
RANGE_CPU=5        # range_lookup 测量线程绑核
WAL_CPU_START=6    # multi_wal 写线程绑核起点（commit 线程不绑核）
FULL_RERUN="${FULL_RERUN:-0}"

mkdir -p "${BIN_DIR}" "${RAW_DIR}" "${LOG_DIR}" "${WAL_BASE}" "${REPORT_DIR}"

echo "[exp2] $(date '+%F %T') start"
echo "[exp2] workspace=${WS_ROOT}"

# ---------- 1. 编译 ----------
echo "[build] range_lookup_bench"
${CXX} ${CXXFLAGS} "${CODE_DIR}/range_lookup_bench.cc" \
    -o "${BIN_DIR}/range_lookup_bench"
echo "[build] multi_wal_fsync_bench"
${CXX} ${CXXFLAGS} "${CODE_DIR}/multi_wal_fsync_bench.cc" \
    -o "${BIN_DIR}/multi_wal_fsync_bench"

need_run() {  # 断点续跑：目标 JSON 已存在且非空则跳过
  [[ "${FULL_RERUN}" == "1" ]] && return 0
  [[ -s "$1" ]] && return 1
  return 0
}

# ---------- 2. Part 1: range lookup（6 组合 × 3 次） ----------
for p in 64 128 256; do
  for impl in binary_search small_btree; do
    for rep in $(seq 1 ${REPS}); do
      out="${RAW_DIR}/range_p${p}_${impl}_rep${rep}.json"
      log="${LOG_DIR}/range_p${p}_${impl}_rep${rep}.log"
      if need_run "${out}"; then
        echo "[range] p=${p} impl=${impl} rep=${rep}"
        "${BIN_DIR}/range_lookup_bench" \
            --p "${p}" --impl "${impl}" --reps 1 \
            --queries 10000000 --warmup 1000000 \
            --cpu "${RANGE_CPU}" --out "${out}" >"${log}" 2>&1
      else
        echo "[range] skip existing ${out}"
      fi
    done
  done
done

# ---------- 3. Part 2: multi-WAL fsync（12 组合 × 3 次） ----------
# P=1 & mode=A 即 baseline_single_wal（sync true/false 两档），无需额外运行。
# 顺序：sync=true 全部先跑（判定关键档），避免 nosync 大量脏页回写干扰。
for sync in true false; do
  for p in 1 16 64; do
    for mode in A B; do
      for rep in $(seq 1 ${REPS}); do
        out="${RAW_DIR}/wal_p${p}_${mode}_${sync}_rep${rep}.json"
        log="${LOG_DIR}/wal_p${p}_${mode}_${sync}_rep${rep}.log"
        waldir="${WAL_BASE}/p${p}_${mode}_${sync}_rep${rep}"
        if need_run "${out}"; then
          echo "[wal] p=${p} mode=${mode} sync=${sync} rep=${rep}"
          # 干扰控制：每轮前 sync 落盘（harness 内部另有负载/脏页等待）
          sync
          rm -rf "${waldir}"
          "${BIN_DIR}/multi_wal_fsync_bench" \
              --p "${p}" --mode "${mode}" --sync "${sync}" \
              --writers 4 --base-batch 512 \
              --cpu-start "${WAL_CPU_START}" \
              --outdir "${waldir}" --out "${out}" >"${log}" 2>&1
          rm -rf "${waldir}"   # 清理 WAL 数据文件，仅保留 JSON
        else
          echo "[wal] skip existing ${out}"
        fi
      done
    done
  done
done

# ---------- 4. 聚合 + 判定 + 报告 ----------
echo "[aggregate]"
python3 "${CODE_DIR}/aggregate_exp2.py" \
    --raw-dir "${RAW_DIR}" --out "${OUT_DIR}/aggregated.json"

echo "[decision]"
python3 "${CODE_DIR}/decision.py" \
    --agg "${OUT_DIR}/aggregated.json" --out "${OUT_DIR}/decision.json"

echo "[report]"
python3 "${CODE_DIR}/build_report.py" \
    --agg "${OUT_DIR}/aggregated.json" \
    --decision "${OUT_DIR}/decision.json" \
    --out "${REPORT}"

echo "[exp2] $(date '+%F %T') done"
echo "[exp2] report: ${REPORT}"
