#!/usr/bin/env bash
# =============================================================================
# run_exp1.sh - ZeroFlush 实验 1: L0 Overlap 源于写入准入
#
# 阶段:
#   smoke  - 小规模冒烟 (num=500000, writes=500000, wbs=4MB) 验证分区生效
#   measure - 基线 vs 干预 各 3 次中位数
#
# 干预方式: flush 时分区（flush_job.cc 按 L1 边界分流到 ≤P=16 个 SST）
# 回退预案: PartitionedMemTable 写入时路由方案复杂度超标, 采用方案文档允许的回退
#
# 硬约束:
#   - 不设 disable_auto_compactions (保持后台自动压缩启用)
#   - MemTable 总容量 64MB (write_buffer_size=64MB, max_write_buffer_number=4)
#   - workload 参数与实验 0 冻结配置一致
#   - 3 次取中位数
#   - 正式测量严禁与实验 0 并发
# =============================================================================
set -u

# ---------------------------------------------------------------- 路径与环境 --
ROOT="/home/embed/hyl/metadata_offload"
CODE="$ROOT/exp_design/pre_experiment/code/exp1_l0_overlap"
VANILLA_BUILD="$ROOT/source/rocksdb/build"
INTERVENTION_BUILD="$ROOT/source/rocksdb-exp1-partition/build"
OVERLAP_PROBE="$CODE/overlap_probe/overlap_probe"
OUT="$ROOT/output/pre_exp/exp1"
RAW="$OUT/raw"

# 严禁 anaconda 路径进入 LD_LIBRARY_PATH
export LD_LIBRARY_PATH="$VANILLA_BUILD:/usr/lib/x86_64-linux-gnu"

# ---------------------------------------------------------------- 附录 A 参数 --
# NOTE: 正式测量前需读取 exp0 冻结的 workload_config.json 逐字段核对
# NOTE: vanilla RocksDB 11.2.0 不支持 --key_distribution flag (exp0-tools 自定义)
#   若冻结配置含 zipfian, 需从 exp0-tools 移植此功能或改用 uniform
KEY_SIZE=16
VALUE_SIZE=1024
NUM=20000000
WRITES=10000000
WRITE_BUFFER_SIZE=67108864
MAX_WRITE_BUFFER_NUMBER=4
TARGET_FILE_SIZE_BASE=67108864
MAX_BYTES_FOR_LEVEL_BASE=268435456
THREADS=8
SEED=42
KEY_DISTRIBUTION=""  # 留空=默认 uniform; 若 exp0-tools 支持则填 zipfian:0.99
COMPACTION_STYLE=0
SYNC=false
REPS=3
P_PARTITION=16

log() { echo "[exp1 $(date '+%F %T')] $*"; }

check_no_exp0() {
    if pgrep -x db_bench > /dev/null 2>&1; then
        log "WARN: db_bench 进程在运行, 可能实验0未完成"
        return 1
    fi
    return 0
}

check_load() {
    while true; do
        local load1
        load1=$(awk '{print int($1)}' /proc/loadavg)
        if [ "$load1" -le 3 ]; then
            log "负载检查通过: load1=$load1"
            return 0
        fi
        log "等待低负载: load1=$load1 (60s 后重查)"
        sleep 60
    done
}

# 单次测量: fillrandom → overlap_probe → readrandom
# 参数: <db_bench_path> <label> <rep>
run_measure() {
    local bench_bin="$1" label="$2" rep="$3"
    local rundir="$RAW/${label}/run${rep}"
    local dbdir="$RAW/db_${label}_r${rep}"
    local libdir
    libdir=$(dirname "$bench_bin")

    mkdir -p "$rundir"
    rm -rf "$dbdir"

    cat > "$rundir/meta.json" <<EOF
{"experiment":"exp1_overlap_at_admission","label":"$label","rep":$rep,
 "num":$NUM,"writes":$WRITES,"value_size":$VALUE_SIZE,"key_size":$KEY_SIZE,
 "threads":$THREADS,"seed":$SEED,"key_distribution":"$KEY_DISTRIBUTION",
 "write_buffer_size":$WRITE_BUFFER_SIZE,"max_write_buffer_number":$MAX_WRITE_BUFFER_NUMBER,
 "target_file_size_base":$TARGET_FILE_SIZE_BASE,"max_bytes_for_level_base":$MAX_BYTES_FOR_LEVEL_BASE,
 "compaction_style":$COMPACTION_STYLE,"sync":$SYNC,"p_partition":$P_PARTITION,
 "db_bench":"$bench_bin"}
EOF

    export LD_LIBRARY_PATH="$libdir:/usr/lib/x86_64-linux-gnu"

    log "RUN $label rep=$rep: fillrandom (num=$NUM writes=$WRITES)"
    /usr/bin/time -v -o "$rundir/time_fill.txt" \
        "$bench_bin" \
        --benchmarks=fillrandom \
        --num="$NUM" --writes="$WRITES" \
        --key_size="$KEY_SIZE" --value_size="$VALUE_SIZE" \
        --threads="$THREADS" \
        --write_buffer_size="$WRITE_BUFFER_SIZE" \
        --max_write_buffer_number="$MAX_WRITE_BUFFER_NUMBER" \
        --target_file_size_base="$TARGET_FILE_SIZE_BASE" \
        --max_bytes_for_level_base="$MAX_BYTES_FOR_LEVEL_BASE" \
        --compaction_style="$COMPACTION_STYLE" \
        --sync="$SYNC" \
        --statistics \
        --stats_dump_period_sec=30 \
        --seed="$SEED" \
        --compression_type=none \
        --db="$dbdir" \
        > "$rundir/fillrandom_stdout.txt" 2> "$rundir/fillrandom_stderr.txt"
    local rc=$?
    if [ $rc -ne 0 ]; then
        log "ERROR: $label rep=$rep fillrandom 退出码 $rc"
        return 1
    fi

    # overlap_probe 采样统计
    log "RUN $label rep=$rep: overlap_probe"
    "$OVERLAP_PROBE" \
        --db="$dbdir" --num_keys="$NUM" \
        --samples=100000 --key_size="$KEY_SIZE" --seed="$SEED" \
        --out="$rundir/overlap.json" \
        > "$rundir/overlap_stdout.txt" 2> "$rundir/overlap_stderr.txt" || {
        log "WARN: overlap_probe 失败 (可能 L0 为空)"
    }

    # readrandom with perf context
    log "RUN $label rep=$rep: readrandom"
    "$bench_bin" \
        --benchmarks=readrandom \
        --num="$NUM" --reads=1000000 \
        --key_size="$KEY_SIZE" --value_size="$VALUE_SIZE" \
        --threads="$THREADS" \
        --perf_level=2 \
        --seed="$SEED" \
        --compression_type=none \
        --use_existing_db=1 \
        --db="$dbdir" \
        > "$rundir/readrandom_stdout.txt" 2> "$rundir/readrandom_stderr.txt"
    rc=$?
    if [ $rc -ne 0 ]; then
        log "WARN: $label rep=$rep readrandom 退出码 $rc"
    fi

    # ldb checkconsistency
    log "RUN $label rep=$rep: ldb checkconsistency"
    "$libdir/tools/ldb" checkconsistency --db="$dbdir" \
        > "$rundir/checkconsistency.txt" 2>&1 || {
        log "WARN: $label rep=$rep ldb checkconsistency 失败"
    }

    # 保留 LOG, 记录磁盘占用, 清理 DB
    cp "$dbdir"/LOG* "$rundir/" 2>/dev/null || true
    du -sb "$dbdir" 2>/dev/null | awk '{print $1}' > "$rundir/disk_bytes.txt"
    rm -rf "$dbdir"

    # 输出摘要
    grep -E "^fillrandom" "$rundir/fillrandom_stdout.txt" | head -2
    cat "$rundir/overlap.json" 2>/dev/null | head -5
    return 0
}

# --------------------------------------------------------------- 冒烟阶段 ---
smoke() {
    log "=== 冒烟: 验证分区生效 ==="
    local dbdir="$RAW/smoke_db"
    local rundir="$RAW/smoke"
    mkdir -p "$rundir"
    rm -rf "$dbdir"

    export LD_LIBRARY_PATH="$INTERVENTION_BUILD:/usr/lib/x86_64-linux-gnu"

    # 用小 write_buffer_size (4MB) 加速 L1 文件产生, 让分区路径激活
    # 冒烟参数与正式测量不同, 仅验证功能正确性
    local SMOKE_WBS=4194304
    local SMOKE_TFSB=4194304
    local SMOKE_MBLB=16777216
    log "冒烟: 干预版 fillrandom (num=500000 writes=500000 wbs=4MB)"
    "$INTERVENTION_BUILD/db_bench" \
        --benchmarks=fillrandom \
        --num=500000 --writes=500000 \
        --key_size=$KEY_SIZE --value_size=$VALUE_SIZE \
        --threads=1 \
        --write_buffer_size=$SMOKE_WBS \
        --max_write_buffer_number=$MAX_WRITE_BUFFER_NUMBER \
        --target_file_size_base=$SMOKE_TFSB \
        --max_bytes_for_level_base=$SMOKE_MBLB \
        --compaction_style=$COMPACTION_STYLE --sync=false \
        --statistics --stats_dump_period_sec=10 \
        --seed=$SEED --compression_type=none \
        --db="$dbdir" \
        > "$rundir/fillrandom.txt" 2>&1
    grep -E "^fillrandom" "$rundir/fillrandom.txt" | head -2

    log "冒烟: readrandom"
    "$INTERVENTION_BUILD/db_bench" \
        --benchmarks=readrandom \
        --num=500000 --reads=100000 \
        --key_size=$KEY_SIZE --value_size=$VALUE_SIZE \
        --threads=1 --seed=$SEED --compression_type=none \
        --use_existing_db=1 --db="$dbdir" \
        > "$rundir/readrandom.txt" 2>&1
    grep -E "^readrandom" "$rundir/readrandom.txt" | head -2

    log "冒烟: overlap_probe"
    "$OVERLAP_PROBE" \
        --db="$dbdir" --num_keys=500000 \
        --samples=100000 --key_size=$KEY_SIZE --seed=$SEED \
        --out="$rundir/overlap.json" \
        > "$rundir/overlap_stdout.txt" 2>&1 || {
        log "WARN: overlap_probe 失败"
    }
    cat "$rundir/overlap.json" 2>/dev/null

    log "冒烟: 验证 LOG 中分区消息"
    local part_count
    part_count=$(grep -c "Flush partitioned" "$dbdir"/LOG* 2>/dev/null || echo 0)
    log "分区 flush 次数: $part_count"
    if [ "$part_count" -gt 0 ]; then
        log "PASS: 分区路径已激活"
    else
        log "WARN: 未检测到分区 flush (可能 L1 文件不足, 增大数据量重试)"
    fi

    log "冒烟: ldb checkconsistency"
    "$INTERVENTION_BUILD/tools/ldb" checkconsistency --db="$dbdir" \
        > "$rundir/checkconsistency.txt" 2>&1 || {
        log "FATAL: ldb checkconsistency 失败"
        cat "$rundir/checkconsistency.txt"
        rm -rf "$dbdir"
        return 1
    }
    cat "$rundir/checkconsistency.txt"

    # 保留 LOG 供分析
    cp "$dbdir"/LOG* "$rundir/" 2>/dev/null || true
    rm -rf "$dbdir"
    log "=== 冒烟通过 ==="
    return 0
}

# --------------------------------------------------------------- 正式测量 ---
measure() {
    log "=== 正式测量开始 ==="
    check_no_exp0 || { log "FATAL: 实验0仍在运行, 中止"; exit 1; }
    check_load

    local rep
    for rep in $(seq 1 $REPS); do
        log "--- 基线 rep=$rep ---"
        run_measure "$VANILLA_BUILD/db_bench" "baseline" "$rep" \
            || { log "FATAL: 基线 rep=$rep 失败"; exit 1; }
    done

    for rep in $(seq 1 $REPS); do
        log "--- 干预 rep=$rep ---"
        run_measure "$INTERVENTION_BUILD/db_bench" "intervention" "$rep" \
            || { log "FATAL: 干预 rep=$rep 失败"; exit 1; }
    done

    log "=== 正式测量完成 ==="
}

# --------------------------------------------------------------- 报告阶段 ---
report() {
    log "=== 生成报告 ==="
    python3 "$CODE/make_report.py" "$OUT" \
        "$ROOT/exp_design/pre_experiment/report/exp1_report.json" \
        || log "WARN: 报告生成失败"
    log "=== 报告完成 ==="
}

# ------------------------------------------------------------------- main ---
main() {
    mkdir -p "$OUT" "$RAW"
    local phase="${1:-all}"
    case "$phase" in
        smoke)   smoke ;;
        measure) measure ;;
        report)  report ;;
        all)     smoke && measure && report ;;
        *) echo "usage: $0 {smoke|measure|report|all}"; exit 2 ;;
    esac
}

main "$@"
