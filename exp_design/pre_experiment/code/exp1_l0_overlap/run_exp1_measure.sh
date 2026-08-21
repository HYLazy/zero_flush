#!/usr/bin/env bash
# =============================================================================
# run_exp1_measure.sh - ZeroFlush 实验 1 阶段 B 正式测量
#
# 基线组: source/rocksdb-exp0-tools/build/db_bench (vanilla 引擎 + zipfian 工具层)
# 干预组: source/rocksdb-exp1-partition/build/db_bench (zipfian + flush 时分区补丁)
#
# 指标: overlap_factor_mean/p99, l0_file_count, l0_l1_compaction_read_mb,
#       point_query_l0_files_checked_mean, write_throughput_ops_per_s
# 3 次取中位数, 每次独立 DB 目录, 每 run 后清理
# =============================================================================
set -u

# ---------------------------------------------------------------- 路径与环境 --
ROOT="/home/embed/hyl/metadata_offload"
OUT="$ROOT/output/pre_exp/exp1"
RAW="$OUT/raw"

BASELINE_BENCH="$ROOT/source/rocksdb-exp0-tools/build/db_bench"
BASELINE_LIB="$ROOT/source/rocksdb-exp0-tools/build"

INTERVENTION_BENCH="$ROOT/source/rocksdb-exp1-partition/build/db_bench"
INTERVENTION_LIB="$ROOT/source/rocksdb-exp1-partition/build"

VANILLA_LIB="$ROOT/source/rocksdb/build"
OVERLAP_PROBE="$ROOT/exp_design/pre_experiment/code/exp1_l0_overlap/overlap_probe/overlap_probe"

# ---------------------------------------------------------------- 附录参数 --
KEY_SIZE=16
NUM=20000000
WRITES=10000000
WRITE_BUFFER_SIZE=67108864
MAX_WRITE_BUFFER_NUMBER=4
TARGET_FILE_SIZE_BASE=67108864
MAX_BYTES_FOR_LEVEL_BASE=268435456
THREADS=8
SEED=42
VALUE_SIZE=1024
REPS="${REPS:-3}"
DISTRIBUTIONS=("zipfian:0.99" "uniform")
MIN_FREE_GB=200

log() { echo "[exp1 $(date '+%F %T')] $*"; }

check_disk() {
    local free_gb
    free_gb=$(df --output=avail -BG "$OUT" | tail -1 | tr -dc '0-9')
    if [ "$free_gb" -lt "$MIN_FREE_GB" ]; then
        log "FATAL: 磁盘剩余 ${free_gb}G < ${MIN_FREE_GB}G, 中止"
        exit 1
    fi
    log "磁盘剩余 ${free_gb}G (阈值 ${MIN_FREE_GB}G)"
}

check_clear() {
    local load1
    load1=$(awk '{print int($1)}' /proc/loadavg)
    if [ "$load1" -gt 5 ]; then
        log "WARN: load1=$load1 > 5, 等待 60s"
        sleep 60
    fi
    # 确认无 db_bench 进程
    if pgrep -x db_bench >/dev/null 2>&1; then
        log "FATAL: db_bench 进程仍在运行, 中止"
        exit 1
    fi
}

# 单次测量运行
# 参数: <group> <distribution> <rep>
run_measure() {
    local group="$1" kd="$2" rep="$3"
    local dist="${kd%%:*}"
    local combo="${group}_${dist}_vs${VALUE_SIZE}"
    local rundir="$RAW/${combo}/run${rep}"
    local dbdir="$RAW/db_${combo}_r${rep}"

    local bench lib
    if [ "$group" = "baseline" ]; then
        bench="$BASELINE_BENCH"
        lib="$BASELINE_LIB"
    else
        bench="$INTERVENTION_BENCH"
        lib="$INTERVENTION_LIB"
    fi

    check_disk
    check_clear
    mkdir -p "$rundir"
    rm -rf "$dbdir"

    cat > "$rundir/meta.json" <<EOF
{"experiment":"exp1","group":"$group","combo":"$combo",
 "distribution":"$dist","key_distribution_flag":"$kd",
 "value_size":$VALUE_SIZE,"num":$NUM,"writes":$WRITES,
 "threads":$THREADS,"seed":$SEED,"rep":$rep,
 "db_bench":"$bench"}
EOF

    log "RUN $group $dist rep=$rep (num=$NUM writes=$WRITES vs=$VALUE_SIZE)"

    # 1. fillrandom
    LD_LIBRARY_PATH="$lib:/usr/lib/x86_64-linux-gnu" \
    /usr/bin/time -v -o "$rundir/time.txt" \
        "$bench" \
        --benchmarks=fillrandom \
        --num="$NUM" --writes="$WRITES" \
        --key_size="$KEY_SIZE" --value_size="$VALUE_SIZE" \
        --threads="$THREADS" \
        --write_buffer_size="$WRITE_BUFFER_SIZE" \
        --max_write_buffer_number="$MAX_WRITE_BUFFER_NUMBER" \
        --target_file_size_base="$TARGET_FILE_SIZE_BASE" \
        --max_bytes_for_level_base="$MAX_BYTES_FOR_LEVEL_BASE" \
        --compaction_style=0 \
        --sync=false \
        --statistics \
        --stats_dump_period_sec=30 \
        --seed="$SEED" \
        --compression_type=none \
        --key_distribution="$kd" \
        --db="$dbdir" \
        > "$rundir/stdout.txt" 2> "$rundir/stderr.txt"
    local rc=$?

    if [ $rc -ne 0 ]; then
        log "ERROR: $combo rep=$rep db_bench 退出码 $rc"
        cp "$dbdir"/LOG "$rundir/LOG" 2>/dev/null || true
        rm -rf "$dbdir"
        return 1
    fi

    # 2a. 保存 fillrandom LOG (必须在 overlap_probe 之前, 因为 overlap_probe 打开 DB 会创建新 LOG 覆盖 fillrandom LOG)
    cp "$dbdir"/LOG "$rundir/LOG_fillrandom" 2>/dev/null || true
    cp "$dbdir"/LOG "$rundir/LOG" 2>/dev/null || true
    cp "$dbdir"/LOG.old.* "$rundir/" 2>/dev/null || true

    # 2. overlap_probe (用 vanilla librocksdb 只读打开)
    log "overlap_probe on $dbdir"
    LD_LIBRARY_PATH="$VANILLA_LIB:/usr/lib/x86_64-linux-gnu" \
        "$OVERLAP_PROBE" \
        --db="$dbdir" \
        --num_keys="$NUM" \
        --samples=100000 \
        --key_size="$KEY_SIZE" \
        --seed="$SEED" \
        --out="$rundir/overlap_probe.json" 2>"$rundir/overlap_probe_stderr.txt"
    local prc=$?
    if [ $prc -ne 0 ]; then
        log "WARN: overlap_probe 退出码 $prc, 见 $rundir/overlap_probe_stderr.txt"
        cat "$rundir/overlap_probe_stderr.txt"
    fi

    # 2b. (LOG 已在 2a 步骤保存, overlap_probe 可能已覆盖 dbdir/LOG)

    # 3. readrandom with perf_level for PerfContext (点查 L0 文件数交叉验证)
    log "readrandom perf_context on $dbdir"
    LD_LIBRARY_PATH="$lib:/usr/lib/x86_64-linux-gnu" \
        "$bench" \
        --benchmarks=readrandom \
        --num="$NUM" \
        --reads=100000 \
        --key_size="$KEY_SIZE" --value_size="$VALUE_SIZE" \
        --threads="$THREADS" \
        --write_buffer_size="$WRITE_BUFFER_SIZE" \
        --max_write_buffer_number="$MAX_WRITE_BUFFER_NUMBER" \
        --target_file_size_base="$TARGET_FILE_SIZE_BASE" \
        --max_bytes_for_level_base="$MAX_BYTES_FOR_LEVEL_BASE" \
        --compaction_style=0 \
        --sync=false \
        --statistics \
        --perf_level=4 \
        --seed="$SEED" \
        --compression_type=none \
        --key_distribution="$kd" \
        --use_existing_db=1 \
        --db="$dbdir" \
        > "$rundir/readrandom_stdout.txt" 2> "$rundir/readrandom_stderr.txt"
    local rrc=$?
    if [ $rrc -ne 0 ]; then
        log "WARN: readrandom perf_context 退出码 $rrc"
    fi

    # 4. 保存磁盘占用, 然后清理 DB (LOG 已在 2b 步骤保存)
    du -sb "$dbdir" 2>/dev/null | awk '{print $1}' > "$rundir/disk_bytes.txt"
    rm -rf "$dbdir"

    # 5. 提取关键指标
    # write_throughput
    grep -E "^fillrandom" "$rundir/stdout.txt" | head -1 \
        > "$rundir/fillrandom_result.txt"

    # compact.read.bytes ticker (精确匹配, 排除 remote.compact.read.bytes)
    grep "^rocksdb.compact.read.bytes " "$rundir/stdout.txt" | tail -1 \
        > "$rundir/compact_read_bytes.txt"

    # Compaction Stats L1 行 (L0->L1 compaction read)
    grep -A 5 "^\*\* Compaction Stats \[default\] \*\*$" "$rundir/LOG" \
        | grep "^  L1" | tail -1 \
        > "$rundir/l1_compaction_stats.txt"

    # 最终 Level summary (L0 文件数)
    grep "Level summary" "$rundir/LOG" | tail -1 \
        > "$rundir/final_level_summary.txt"

    # flush partition 激活次数 (仅干预组)
    if [ "$group" = "intervention" ]; then
        grep -c "Flush partitioned" "$rundir/LOG" \
            > "$rundir/flush_partition_count.txt"
    fi

    log "DONE $combo rep=$rep"
    grep -E "^fillrandom" "$rundir/stdout.txt" | head -1
    cat "$rundir/overlap_probe.json" 2>/dev/null
    echo "---"

    echo "{\"group\":\"$group\",\"combo\":\"$combo\",\"distribution\":\"$dist\",\"run_dir\":\"$rundir\"}" \
        >> "$OUT/manifest_entries.jsonl"
    return 0
}

# --------------------------------------------------------------- main ---
main() {
    mkdir -p "$OUT" "$RAW"
    local phase="${1:-all}"
    case "$phase" in
        baseline)
            for kd in "${DISTRIBUTIONS[@]}"; do
                for rep in $(seq 1 $REPS); do
                    run_measure "baseline" "$kd" "$rep" || {
                        log "FATAL: baseline $kd rep=$rep 失败"; exit 1; }
                done
            done
            ;;
        intervention)
            for kd in "${DISTRIBUTIONS[@]}"; do
                for rep in $(seq 1 $REPS); do
                    run_measure "intervention" "$kd" "$rep" || {
                        log "FATAL: intervention $kd rep=$rep 失败"; exit 1; }
                done
            done
            ;;
        all)
            for g in baseline intervention; do
                for kd in "${DISTRIBUTIONS[@]}"; do
                    for rep in $(seq 1 $REPS); do
                        run_measure "$g" "$kd" "$rep" || {
                            log "FATAL: $g $kd rep=$rep 失败"; exit 1; }
                    done
                done
            done
            ;;
        baseline_zipfian)
            for rep in $(seq 1 $REPS); do
                run_measure "baseline" "zipfian:0.99" "$rep" || {
                    log "FATAL: baseline zipfian rep=$rep 失败"; exit 1; }
            done
            ;;
        intervention_zipfian)
            for rep in $(seq 1 $REPS); do
                run_measure "intervention" "zipfian:0.99" "$rep" || {
                    log "FATAL: intervention zipfian rep=$rep 失败"; exit 1; }
            done
            ;;
        baseline_uniform)
            for rep in $(seq 1 $REPS); do
                run_measure "baseline" "uniform" "$rep" || {
                    log "FATAL: baseline uniform rep=$rep 失败"; exit 1; }
            done
            ;;
        intervention_uniform)
            for rep in $(seq 1 $REPS); do
                run_measure "intervention" "uniform" "$rep" || {
                    log "FATAL: intervention uniform rep=$rep 失败"; exit 1; }
            done
            ;;
        *) echo "usage: $0 {all|baseline|intervention|baseline_zipfian|intervention_zipfian|baseline_uniform|intervention_uniform}"; exit 2 ;;
    esac
    log "=== 测量阶段完成 ==="
}

main "$@"
