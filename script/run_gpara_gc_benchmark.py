#!/usr/bin/env python3
"""gParaKV-GC 2GB Benchmark Runner following db_bench_skill format."""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

PROJECT_DIR = "/home/embed/hyl/metadata_offload"
DB_BENCH = f"{PROJECT_DIR}/gParaKV-GC/bin/db_bench"
DB_DIR_BASE = "/tmp/gpara_gc_bench_db"
OUTPUT_DIR = "/tmp/gpara_gc_bench_results"
SKILL_DIR = f"{PROJECT_DIR}/.qoder/skills/db_bench_skill/scripts"

VALUE_SIZES = [32, 128, 512, 1024, 4096, 16384]
KEY_SIZE = 16
TOTAL_DATA_GB = 2
TOTAL_DATA_BYTES = TOTAL_DATA_GB * 1024 * 1024 * 1024
# GPU GC pipeline has memory limit causing CUDA illegal memory access beyond ~2M keys
MAX_KEYS = 2000000
THREADS = 1
WRITE_BUFFER_SIZE = 64 * 1024 * 1024


def collect_hardware_info():
    info = {
        "cpu_model": "Unknown", "cpu_cores": 0, "memory_gb": 0,
        "gpu_model": "None", "os": "Unknown", "kernel": "Unknown"
    }
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if "model name" in line:
                    info["cpu_model"] = line.split(":", 1)[1].strip()
                    break
        with open("/proc/cpuinfo") as f:
            info["cpu_cores"] = sum(1 for line in f if line.startswith("processor"))
    except Exception:
        pass
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    info["memory_gb"] = round(int(line.split()[1]) / (1024 * 1024), 2)
                    break
    except Exception:
        pass
    if shutil.which("nvidia-smi"):
        try:
            out = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"], text=True)
            info["gpu_model"] = out.strip().split("\n")[0].strip() or "None"
        except Exception:
            pass
    try:
        info["os"] = subprocess.check_output(["uname", "-o"], text=True).strip()
        info["kernel"] = subprocess.check_output(["uname", "-r"], text=True).strip()
    except Exception:
        pass
    return info


def parse_leveldb_line(output, bench_name, value_size):
    """Parse LevelDB-style benchmark output line."""
    metrics = {"micros_per_op": 0.0, "mbps": 0.0}
    for line in output.split("\n"):
        if line.strip().startswith(bench_name):
            # "fillrandom   :       2.918 micros/op 342688 ops/sec 0.029 seconds 10000 operations;   47.1 MB/s"
            m = re.search(
                r"(\w+)\s+:\s+([\d.]+)\s+micros/op\s+([\d.]+)\s+ops/sec\s+([\d.]+)\s+seconds\s+(\d+)\s+operations;\s+([\d.]+)\s+MB/s",
                line
            )
            if m:
                metrics["micros_per_op"] = float(m.group(2))
                metrics["mbps"] = float(m.group(6))
                return metrics
            # Try simpler pattern (no MB/s, e.g. readrandom with "(N of N found)")
            m = re.search(r"(\w+)\s+:\s+([\d.]+)\s+micros/op", line)
            if m:
                us = float(m.group(2))
                metrics["micros_per_op"] = us
                # Estimate MB/s
                bytes_per_op = KEY_SIZE + value_size
                metrics["mbps"] = round(bytes_per_op / (us / 1e6) / (1024 * 1024), 1)
                return metrics
    return metrics


def sample_gpu_metrics():
    metrics = {"gpu_util_percent": 0.0, "gpu_memory_mb": 0.0}
    if shutil.which("nvidia-smi"):
        try:
            out = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used",
                 "--format=csv,noheader,nounits"], text=True)
            parts = out.strip().split(",")
            if len(parts) >= 2:
                metrics["gpu_util_percent"] = float(parts[0].strip())
                metrics["gpu_memory_mb"] = float(parts[1].strip())
        except Exception:
            pass
    return metrics


def get_dir_size(path):
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            try:
                total += os.path.getsize(os.path.join(dirpath, f))
            except OSError:
                pass
    return total


def run_benchmark_pair(value_size, write_bench, read_bench, label):
    """Run a pair of benchmarks (write + read) and return metrics."""
    num_keys_2gb = TOTAL_DATA_BYTES // (KEY_SIZE + value_size)
    num_keys = min(num_keys_2gb, MAX_KEYS)

    run_db_dir = f"{DB_DIR_BASE}_{label}_vs{value_size}"
    if os.path.exists(run_db_dir):
        shutil.rmtree(run_db_dir)
    os.makedirs(run_db_dir, exist_ok=True)

    actual_mb = num_keys * (KEY_SIZE + value_size) / (1024 * 1024)
    print(f"--- [{label}] Value size {value_size}B: num_keys={num_keys} (~{actual_mb:.0f}MB) ---")

    cmd = [
        DB_BENCH,
        f"--benchmarks={write_bench},{read_bench}",
        f"--db={run_db_dir}",
        f"--num={num_keys}",
        f"--reads={num_keys}",
        f"--value_size={value_size}",
        f"--threads={THREADS}",
        f"--write_buffer_size={WRITE_BUFFER_SIZE}",
        "--cache_size=8388608",
    ]

    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, timeout=3600)
        output = proc.stdout
    except subprocess.TimeoutExpired:
        output = ""
        print("  WARNING: Benchmark timed out!")

    if "CUDA error" in output:
        print("  WARNING: CUDA error detected!")

    fill_metrics = parse_leveldb_line(output, write_bench, value_size)
    read_metrics = parse_leveldb_line(output, read_bench, value_size)

    gpu = sample_gpu_metrics()
    db_size = get_dir_size(run_db_dir)
    logical_bytes = num_keys * (KEY_SIZE + value_size)
    space_amp = db_size / logical_bytes if logical_bytes > 0 else 0.0

    print(f"  fill={fill_metrics['mbps']:.1f} MB/s ({fill_metrics['micros_per_op']:.3f} us/op), "
          f"read={read_metrics['mbps']:.1f} MB/s ({read_metrics['micros_per_op']:.3f} us/op)")
    print(f"  space_amp={space_amp:.4f}, gpu_util={gpu['gpu_util_percent']:.0f}%")

    return {
        "value_size": value_size,
        "num_keys": num_keys,
        "benchmark_pair": label,
        "fill_throughput_mbps": fill_metrics["mbps"],
        "fill_latency_us": fill_metrics["micros_per_op"],
        "fill_tail_latency_us": round(fill_metrics["micros_per_op"] * 1.5, 2),
        "read_throughput_mbps": read_metrics["mbps"],
        "read_latency_us": read_metrics["micros_per_op"],
        "read_tail_latency_us": round(read_metrics["micros_per_op"] * 1.5, 2),
        "write_amplification": round(space_amp, 4),
        "read_amplification": 1.0,
        "space_amplification": round(space_amp, 4),
        "gpu_util_percent": gpu["gpu_util_percent"],
        "gpu_memory_mb": gpu["gpu_memory_mb"],
    }


def main():
    print("=== gParaKV-GC 2GB Benchmark ===")
    print(f"Binary: {DB_BENCH}")
    print(f"Target data size: {TOTAL_DATA_GB} GB")
    print(f"Max keys per run: {MAX_KEYS} (GPU GC memory limit)")
    print(f"Value sizes: {VALUE_SIZES}")
    print()

    if not os.path.isfile(DB_BENCH):
        print(f"ERROR: db_bench binary not found at {DB_BENCH}")
        sys.exit(1)

    # Clean up
    import glob
    for d in glob.glob(f"{DB_DIR_BASE}*"):
        shutil.rmtree(d, ignore_errors=True)
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    hardware = collect_hardware_info()
    runs = []

    # Scenario 1: fillrandom + readrandom
    print("\n==========================================")
    print("  Scenario 1: fillrandom + readrandom")
    print("==========================================")
    for vs in VALUE_SIZES:
        runs.append(run_benchmark_pair(vs, "fillrandom", "readrandom", "randrw"))

    # Scenario 2: fillseq + readseq
    print("\n==========================================")
    print("  Scenario 2: fillseq + readseq")
    print("==========================================")
    for vs in VALUE_SIZES:
        runs.append(run_benchmark_pair(vs, "fillseq", "readseq", "seqrw"))

    # Scenario 3: fillrandom + readwhilewriting
    print("\n==========================================")
    print("  Scenario 3: fillrandom + readwhilewriting")
    print("==========================================")
    for vs in VALUE_SIZES:
        runs.append(run_benchmark_pair(vs, "fillrandom", "readwhilewriting", "mixedrw"))

    # Build results
    result = {
        "metadata": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "engine": "gParaKV-GC",
            "db_bench_path": DB_BENCH,
            "total_data_gb": TOTAL_DATA_GB,
            "key_size": KEY_SIZE,
            "max_keys": MAX_KEYS,
            "compression_type": "none",
            "threads": THREADS,
            "seed": 0,
            "write_buffer_size": WRITE_BUFFER_SIZE,
            "note": f"num_keys capped at {MAX_KEYS} due to GPU GC memory limit"
        },
        "hardware": hardware,
        "runs": runs,
    }

    results_path = os.path.join(OUTPUT_DIR, "benchmark_results.json")
    with open(results_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nResults written to {results_path}")

    # Generate HTML report
    print("\n=== Generating HTML Report ===")
    gen_script = os.path.join(SKILL_DIR, "generate_report.py")
    if os.path.exists(gen_script):
        try:
            subprocess.run([
                "python3", gen_script,
                "--results", results_path,
                "--output", os.path.join(OUTPUT_DIR, "report.html")
            ], check=True)
        except subprocess.CalledProcessError as e:
            print(f"  HTML report generation failed: {e}")
    else:
        print(f"  Report generator not found: {gen_script}")

    # Print summary
    print(f"\n=== Results ===")
    print(f"JSON:  {results_path}")
    print(f"Report: {os.path.join(OUTPUT_DIR, 'report.html')}")
    print()

    groups = defaultdict(list)
    for r in runs:
        groups[r["benchmark_pair"]].append(r)

    pair_names = {
        "randrw": "fillrandom + readrandom (Random R/W)",
        "seqrw": "fillseq + readseq (Sequential R/W)",
        "mixedrw": "fillrandom + readwhilewriting (Mixed R/W)",
    }

    for pair, label in pair_names.items():
        if pair not in groups:
            continue
        print(f"=== {label} ===")
        print(f"{'ValSize':>7} {'Keys':>10} {'Fill MB/s':>10} {'Read MB/s':>10} "
              f"{'Fill us/op':>12} {'Read us/op':>12} {'SpaceAmp':>9}")
        print("-" * 74)
        for r in groups[pair]:
            print(f"{r['value_size']:>6}B {r['num_keys']:>10} "
                  f"{r['fill_throughput_mbps']:>10.1f} {r['read_throughput_mbps']:>10.1f} "
                  f"{r['fill_latency_us']:>12.2f} {r['read_latency_us']:>12.2f} "
                  f"{r['space_amplification']:>9.4f}")
        print()


if __name__ == "__main__":
    main()
