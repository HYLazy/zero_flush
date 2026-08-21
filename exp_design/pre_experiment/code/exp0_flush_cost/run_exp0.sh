#!/usr/bin/env bash
# =============================================================================
# run_exp0.sh - ZeroFlush 实验 0: Flush 成本分解 (db_bench fillrandom)
#
# 阶段:
#   smoke  - 小规模冒烟 (value=100, num=100000, writes=50000) 全流程验证
#   matrix - 全量矩阵: value_size ∈ {100,1024,4096} × dist ∈ {zipfian,uniform}
#            × 3 次, 串行, 每次独立 DB 目录, 每 run 后删除
#   ycsb   - YCSB-A 交叉验证 (见 ycsb 段; 失败则回退 readwhilewriting)
#   report - 聚合 + wa_check + make_report 产出最终 JSON
#
# 硬约束:
#   - 不修改 source/rocksdb/ (只读 vanilla), 使用隔离副本 rocksdb-exp0-tools
#   - LD_LIBRARY_PATH 绝不包含 anaconda 路径
#   - 串行执行, 禁止并发 db_bench
#   - 每 run 前检查 df, 剩余 <250G 中止
# =============================================================================
set -u

# ---------------------------------------------------------------- 路径与环境 --
ROOT="/home/embed/hyl/metadata_offload"
CODE="$ROOT/exp_design/pre_experiment/code/exp0_flush_cost"
TOOLS_BUILD="$ROOT/source/rocksdb-exp0-tools/build"
DB_BENCH="$TOOLS_BUILD/db_bench"
YCSB="$ROOT/source/YCSB/bin/ycsb"
# binding 发行包 (含全部依赖 jar; source/YCSB/bin/ycsb 启动器需 mvn, 不可用)
YCSB_DIST="/tmp/ycsb-rocksdb-binding-0.18.0-SNAPSHOT"
OUT="$ROOT/output/pre_exp/exp0"
RAW="$OUT/raw"

# 严禁 anaconda 路径进入 LD_LIBRARY_PATH
export LD_LIBRARY_PATH="$TOOLS_BUILD:/usr/lib/x86_64-linux-gnu"

# ---------------------------------------------------------------- 附录 A 参数 --
KEY_SIZE=16
NUM=20000000
WRITES=10000000
WRITE_BUFFER_SIZE=67108864
MAX_WRITE_BUFFER_NUMBER=4
TARGET_FILE_SIZE_BASE=67108864
MAX_BYTES_FOR_LEVEL_BASE=268435456
THREADS=8
SEED=42
VALUE_SIZES=(100 1024 4096)
DISTRIBUTIONS=("zipfian:0.99" "uniform")
MIN_FREE_GB=250

log() { echo "[exp0 $(date '+%F %T')] $*"; }

check_disk() {
    local free_gb
    free_gb=$(df --output=avail -BG "$OUT" | tail -1 | tr -dc '0-9')
    if [ "$free_gb" -lt "$MIN_FREE_GB" ]; then
        log "FATAL: 磁盘剩余 ${free_gb}G < ${MIN_FREE_GB}G, 中止"
        exit 1
    fi
    log "磁盘剩余 ${free_gb}G (阈值 ${MIN_FREE_GB}G)"
}

check_load() {
    # 正式全量前检查系统负载; 高 IO 负载时等待 (另一工程师并行实验 2)
    while true; do
        local load1 util
        load1=$(awk '{print int($1)}' /proc/loadavg)
        util=$(iostat -x 1 2 2>/dev/null | awk \
            '$1 ~ /^(nvme|sd|vd)/ {u=$NF} END {print int(u)}')
        util=${util:-0}
        if [ "$load1" -le 18 ] && [ "$util" -le 60 ]; then
            log "负载检查通过: load1=$load1 disk_util=${util}%"
            return 0
        fi
        log "等待低负载: load1=$load1 disk_util=${util}% (60s 后重查)"
        sleep 60
    done
}

# 单次 db_bench fillrandom 运行
# 参数: <combo名> <value_size> <key_distribution> <num> <writes> <run序号>
run_fillrandom() {
    local combo="$1" vs="$2" kd="$3" num="$4" writes="$5" rep="$6"
    local rundir="$RAW/${combo}/run${rep}"
    local dbdir="$RAW/db_${combo}_r${rep}"

    check_disk
    mkdir -p "$rundir"
    rm -rf "$dbdir"

    cat > "$rundir/meta.json" <<EOF
{"benchmark":"fillrandom","combo":"$combo","value_size":$vs,
 "distribution":"${kd%%:*}","key_distribution_flag":"$kd",
 "num":$num,"writes":$writes,"threads":$THREADS,"seed":$SEED,"rep":$rep,
 "db_bench":"$DB_BENCH"}
EOF

    log "RUN $combo rep=$rep (num=$num writes=$writes)"
    /usr/bin/time -v -o "$rundir/time.txt" \
        "$DB_BENCH" \
        --benchmarks=fillrandom \
        --num="$num" --writes="$writes" \
        --key_size="$KEY_SIZE" --value_size="$vs" \
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

    # 保留原始输出与 LOG, 记录最终磁盘占用与驻留 WAL 字节, 然后清理 DB
    cp "$dbdir"/LOG "$rundir/LOG" 2>/dev/null || true
    du -sb "$dbdir" 2>/dev/null | awk '{print $1}' > "$rundir/disk_bytes.txt"
    # 驻留 WAL (RocksDB WAL 文件为 *.log; 旧 WAL 在 flush 后会被删除,
    # 磁盘交叉核对需要此值)
    du -cb "$dbdir"/*.log 2>/dev/null | tail -1 | awk '{print $1}' \
        > "$rundir/wal_resident_bytes.txt"
    rm -rf "$dbdir"

    if [ $rc -ne 0 ]; then
        log "ERROR: $combo rep=$rep db_bench 退出码 $rc (见 $rundir/stderr.txt)"
        return 1
    fi
    grep -E "^(fillrandom|Stalls)" "$rundir/stdout.txt" | head -3
    echo "{\"combo\":\"$combo\",\"run_dir\":\"$rundir\"}" \
        >> "$OUT/manifest_entries.jsonl"
    return 0
}

# --------------------------------------------------------------- 冒烟阶段 ---
smoke() {
    log "=== 冒烟: value=100 num=100000 writes=50000 ==="
    for kd in "${DISTRIBUTIONS[@]}"; do
        local combo="smoke_${kd%%:*}_vs100"
        run_fillrandom "$combo" 100 "$kd" 100000 50000 1 || return 1
    done
    # 冒烟也走完整解析链, 验证管道可用
    python3 "$CODE/smoke_check.py" "$OUT" 50000 || {
        log "FATAL: 冒烟解析/校验失败"; exit 1; }
    log "=== 冒烟通过 ==="
}

# ------------------------------------------------------------- 全量矩阵 -----
matrix() {
    check_load
    log "=== 全量矩阵开始 (6 组合 × 3 次, 串行) ==="
    for vs in "${VALUE_SIZES[@]}"; do
        for kd in "${DISTRIBUTIONS[@]}"; do
            local dist="${kd%%:*}"
            local combo="${dist}_vs${vs}"
            for rep in 1 2 3; do
                run_fillrandom "$combo" "$vs" "$kd" "$NUM" "$WRITES" "$rep" \
                    || { log "FATAL: 矩阵运行失败 $combo rep=$rep"; exit 1; }
            done
        done
    done
    log "=== 全量矩阵完成 ==="
}

# ------------------------------------------------------- 矩阵断点续跑 -------
# 读取 manifest_entries.jsonl, 跳过已完成的 (combo, rep), 只补剩余 run
resume_matrix() {
    check_load
    log "=== 矩阵断点续跑 ==="
    local entries="$OUT/manifest_entries.jsonl"
    declare -A done_set
    if [ -f "$entries" ]; then
        while IFS= read -r line; do
            local combo run_dir rep
            combo=$(echo "$line" | python3 -c \
                'import json,sys; print(json.loads(sys.stdin.read())["combo"])')
            run_dir=$(echo "$line" | python3 -c \
                'import json,sys; print(json.loads(sys.stdin.read())["run_dir"])')
            rep="${run_dir##*run}"
            [ -d "$run_dir" ] && done_set["${combo}_${rep}"]=1
        done < "$entries"
    fi
    for vs in "${VALUE_SIZES[@]}"; do
        for kd in "${DISTRIBUTIONS[@]}"; do
            local dist="${kd%%:*}"
            local combo="${dist}_vs${vs}"
            for rep in 1 2 3; do
                if [ "${done_set[${combo}_${rep}]:-0}" = "1" ]; then
                    log "SKIP $combo rep=$rep (已完成)"
                    continue
                fi
                run_fillrandom "$combo" "$vs" "$kd" "$NUM" "$WRITES" "$rep" \
                    || { log "FATAL: 续跑失败 $combo rep=$rep"; exit 1; }
            done
        done
    done
    log "=== 矩阵续跑完成 ==="
}

# ------------------------------------------------------------ YCSB-A 阶段 ---
ycsb_phase() {
    check_load
    log "=== YCSB-A 交叉验证 ==="
    # 准备 binding 发行包 (从已构建的 tar.gz 解包, 避免 mvn)
    if [ ! -x "$YCSB_DIST/bin/ycsb" ] && [ ! -f "$YCSB_DIST/bin/ycsb" ]; then
        tar xzf "$ROOT/source/YCSB/rocksdb/target/"\
"ycsb-rocksdb-binding-0.18.0-SNAPSHOT.tar.gz" -C /tmp || {
            log "ERROR: 解包 YCSB binding 失败"; return 1; }
    fi
    local YRUN="python3 $YCSB_DIST/bin/ycsb"
    local ydir="$RAW/ycsb-a"
    mkdir -p "$ydir"

    local wl="$ROOT/source/YCSB/workloads/workloada"

    # 小规模确认 binding 可运行
    log "YCSB 冒烟 (recordcount=10000 operationcount=10000)"
    rm -rf "$ydir/db_smoke"
    $YRUN load rocksdb -P "$wl" \
        -p rocksdb.dir="$ydir/db_smoke" \
        -p recordcount=10000 -p operationcount=10000 \
        -threads "$THREADS" > "$ydir/smoke_load.txt" 2>&1 || {
        log "ERROR: YCSB 冒烟 load 失败"; return 1; }
    grep -q "\[OVERALL\], Throughput(ops/sec), 0.0" "$ydir/smoke_load.txt" && {
        log "ERROR: YCSB 冒烟 load 吞吐为 0"; return 1; }
    $YRUN run rocksdb -P "$wl" \
        -p rocksdb.dir="$ydir/db_smoke" \
        -p recordcount=10000 -p operationcount=10000 \
        -threads "$THREADS" > "$ydir/smoke_run.txt" 2>&1 || {
        log "ERROR: YCSB 冒烟 run 失败"; return 1; }
    grep -E "\[OVERALL\]" "$ydir/smoke_run.txt"
    rm -rf "$ydir/db_smoke"

    # 正式运行: 2M records load (不计时) + 2M ops run (time -v 计时)
    local RC=2000000 OC=2000000
    check_disk
    rm -rf "$ydir/db"
    log "YCSB 正式 load: recordcount=$RC"
    $YRUN load rocksdb -P "$wl" \
        -p rocksdb.dir="$ydir/db" \
        -p recordcount=$RC \
        -threads "$THREADS" > "$ydir/load.txt" 2>&1 || {
        log "ERROR: YCSB load 失败"; return 1; }
    grep -E "\[OVERALL\]" "$ydir/load.txt"

    log "YCSB 正式 run: operationcount=$OC"
    /usr/bin/time -v -o "$ydir/time.txt" \
        $YRUN run rocksdb -P "$wl" \
        -p rocksdb.dir="$ydir/db" \
        -p recordcount=$RC -p operationcount=$OC \
        -threads "$THREADS" > "$ydir/run.txt" 2> "$ydir/stderr.txt"
    local rc=$?
    cp "$ydir/db"/LOG "$ydir/LOG" 2>/dev/null || true
    du -sb "$ydir/db" 2>/dev/null | awk '{print $1}' > "$ydir/disk_bytes.txt"
    grep -E "\[OVERALL\]" "$ydir/run.txt"
    rm -rf "$ydir/db"
    [ $rc -ne 0 ] && { log "ERROR: YCSB run 失败"; return 1; }

    python3 "$CODE/parse_ycsb.py" "$ydir" "$OUT/ycsb_result.json" $RC $OC
    log "=== YCSB-A 完成 ==="
}

# readwhilewriting 回退 (YCSB 不可用时): uniform 50/50
ycsb_fallback() {
    log "=== 回退: db_bench readwhilewriting (uniform 50/50) ==="
    local combo="fallback_rww"
    check_disk
    run_fillrandom_rww() {
        local rundir="$RAW/$combo/run1"
        local dbdir="$RAW/db_${combo}_r1"
        mkdir -p "$rundir"
        cat > "$rundir/meta.json" <<EOF
{"benchmark":"ycsb-a","combo":"$combo","value_size":1024,
 "distribution":"uniform","note":"YCSB 不可用, 回退 db_bench readwhilewriting",
 "num":2000000,"writes":1000000,"threads":$THREADS,"seed":$SEED}
EOF
        /usr/bin/time -v -o "$rundir/time.txt" \
            "$DB_BENCH" \
            --benchmarks=readwhilewriting \
            --num=2000000 --writes=1000000 --reads=1000000 \
            --key_size=16 --value_size=1024 \
            --threads="$THREADS" \
            --write_buffer_size="$WRITE_BUFFER_SIZE" \
            --max_write_buffer_number="$MAX_WRITE_BUFFER_NUMBER" \
            --target_file_size_base="$TARGET_FILE_SIZE_BASE" \
            --max_bytes_for_level_base="$MAX_BYTES_FOR_LEVEL_BASE" \
            --compaction_style=0 --sync=false --statistics \
            --seed="$SEED" --compression_type=none \
            --key_distribution=uniform \
            --use_existing_db=1 \
            --benchmarks=fillrandom,stats,readwhilewriting,stats \
            --db="$dbdir" \
            > "$rundir/stdout.txt" 2> "$rundir/stderr.txt"
        cp "$dbdir"/LOG "$rundir/LOG" 2>/dev/null || true
        du -sb "$dbdir" 2>/dev/null | awk '{print $1}' \
            > "$rundir/disk_bytes.txt"
        rm -rf "$dbdir"
        echo "{\"combo\":\"$combo\",\"run_dir\":\"$rundir\"}" \
            >> "$OUT/manifest_entries.jsonl"
    }
    run_fillrandom_rww
}

# --------------------------------------------------------------- 报告阶段 ---
report() {
    log "=== 聚合与报告 ==="
    python3 "$CODE/build_manifest.py" "$OUT/manifest_entries.jsonl" \
        "$OUT/manifest.json"
    python3 "$CODE/parse_stats.py" aggregate "$OUT/manifest.json" \
        "$OUT/aggregated.json"
    python3 "$CODE/wa_check.py" "$OUT/aggregated.json" "$OUT/wa_summary.json" \
        || log "WARN: wa_check 报告失败项, 见 wa_summary.json"
    python3 "$CODE/make_report.py" "$OUT" \
        "$ROOT/exp_design/pre_experiment/report/exp0_report.json"
    log "=== 报告产出完成 ==="
}

# ------------------------------------------------------------------- main ---
main() {
    mkdir -p "$OUT" "$RAW"
    local phase="${1:-all}"
    case "$phase" in
        smoke)  smoke ;;
        matrix) matrix ;;
        resume) resume_matrix ;;
        ycsb)   ycsb_phase || ycsb_fallback ;;
        report) report ;;
        all)
            smoke
            matrix
            ycsb_phase || ycsb_fallback
            report
            ;;
        *) echo "usage: $0 {smoke|matrix|resume|ycsb|report|all}"; exit 2 ;;
    esac
}

main "$@"
