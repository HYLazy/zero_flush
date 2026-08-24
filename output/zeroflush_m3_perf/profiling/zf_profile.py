#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ZeroFlush 写路径剖析实验驱动（profiling W0-W3）。

变体：
  W0 native    — 原生 rocksdb 参照
  W1 zf-base   — ZF 基线（与 50GB 基准同参：P=64, stop=72, slowdown=64, K=8）
  W2 zf-nostall— ZF 无停写（slowdown/stop=10^6）→ 定量停写等待占比
  W3 zf-k1     — ZF K=1（--zf_materialize_parallelism=1）→ 定量物化并行收益

规模：vs256 / num=8M keys（≈2.2GB 逻辑，~9 epoch）/ 16 线程 / fillrandom only。
采样：每秒 /proc/<pid>/stat + /proc/diskstats → samples.csv（iostat/pidstat 缺席的替代）。
perf 因 perf_event_paranoid=4 不可用，跳过（记录于 result.json）。

用法：python3 zf_profile.py --variants W0,W1,W2,W3
"""

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = "/home/embed/hyl/metadata_offload"
NATIVE_BIN = f"{ROOT}/source/rocksdb/build/db_bench"
ZF_BIN = f"{ROOT}/source/rocksdb-zeroflush/build/db_bench"
NATIVE_BUILD = f"{ROOT}/source/rocksdb/build"
ZF_BUILD = f"{ROOT}/source/rocksdb-zeroflush/build"

NUM_KEYS = 8_000_000
THREADS = 16
WRITES_PER_THREAD = NUM_KEYS // THREADS  # P0 修正：每线程配额
TIMEOUT_SEC = 90 * 60
DISK_DEV = "nvme0n1p3"  # 根分区，数据落盘处

RE_MAIN = re.compile(
    r"^(\w+)\s*:\s*([\d.]+)\s*micros/op\s+(\d+)\s*ops/sec\s+-*([\d.]+)\s*"
    r"seconds\s+(\d+)\s*operations;\s*([\d.]+)\s*MB/s", re.M)
RE_PCT = re.compile(
    r"Percentiles:\s*P50:\s*([\d.]+)\s*P75:\s*([\d.]+)\s*P99:\s*([\d.]+)"
    r"\s*P99.9:\s*([\d.]+)\s*P99.99:\s*([\d.]+)")
RE_ZFPROP = re.compile(r"^zf\.([a-z0-9_]+)\s*:\s*([\d.]+)", re.M)


def common_flags(db_dir):
    return [
        "--benchmarks=fillrandom",
        f"--db={db_dir}",
        f"--num={NUM_KEYS}",
        "--key_size=16",
        "--value_size=256",
        f"--threads={THREADS}",
        f"--writes={WRITES_PER_THREAD}",
        "--compression_type=none",
        "--disable_wal=false",
        "--histogram=true",
        "--statistics=true",
        "--cache_size=8388608",
        "--write_buffer_size=268435456",
        "--max_background_jobs=16",
        # 剖析专用：周期性 stats dump（含 Stalls 段按原因停写秒数）
        "--stats_interval_seconds=30",
        "--stats_per_interval=1",
    ]


def zf_flags(stop, slowdown, parallelism):
    return [
        "--zeroflush",
        "--zf_partitions=64",
        f"--zf_materialize_parallelism={parallelism}",
        f"--level0_slowdown_writes_trigger={slowdown}",
        f"--level0_stop_writes_trigger={stop}",
    ]


VARIANTS = {
    "W0": dict(bin=NATIVE_BIN, build=NATIVE_BUILD, extra=[]),
    "W1": dict(bin=ZF_BIN, build=ZF_BUILD, extra=zf_flags(72, 64, 8)),
    "W2": dict(bin=ZF_BIN, build=ZF_BUILD, extra=zf_flags(10**6, 10**6, 8)),
    "W3": dict(bin=ZF_BIN, build=ZF_BUILD, extra=zf_flags(72, 64, 1)),
    "W2b": dict(bin=ZF_BIN, build=ZF_BUILD,
                extra=zf_flags(10**6, 10**6, 8)
                + ["--level0_file_num_compaction_trigger=1000000"]),
    # ---- M4.0：范围路由变体（vs W1 hash 基线对照） ----
    # R1: sampled 学习边界 + M3.2 直装（不融合）
    "R1": dict(bin=ZF_BIN, build=ZF_BUILD,
               extra=zf_flags(72, 64, 8) + ["--zf_routing=sampled"]),
    # R2: R1 + M3.3 融合归并（终态预演：L0 缺席）
    "R2": dict(bin=ZF_BIN, build=ZF_BUILD,
               extra=zf_flags(72, 64, 8)
               + ["--zf_routing=sampled", "--zf_base_merge"]),
    # R3: R2 + P=16（批量参数最优解，M4.0 实测 +11% vs hash）
    "R3": dict(bin=ZF_BIN, build=ZF_BUILD,
               extra=zf_flags(72, 64, 8)
               + ["--zf_routing=sampled", "--zf_base_merge", "--zf_partitions=16"]),
    # R4: R3 的 50GB 版（--num 由 r4_50gb.py 覆盖）
    "R4": dict(bin=ZF_BIN, build=ZF_BUILD,
               extra=zf_flags(72, 64, 8)
               + ["--zf_routing=sampled", "--zf_base_merge", "--zf_partitions=16"]),
}


def read_pid_stat(pid):
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            parts = f.read().split()
        # 字段序（1 起）：3=state 14=utime 15=stime 20=num_threads
        return dict(state=parts[2].decode(),
                    utime=int(parts[13]), stime=int(parts[14]),
                    nthreads=int(parts[19]))
    except (OSError, IndexError, ValueError):
        return None


def read_diskstats(dev):
    try:
        with open("/proc/diskstats") as f:
            for line in f:
                p = line.split()
                if len(p) > 13 and p[2] == dev:
                    return dict(r_sect=int(p[5]), w_sect=int(p[9]),
                                io_ms=int(p[12]))
    except OSError:
        pass
    return None


class Sampler(threading.Thread):
    """1s 轮询进程 CPU/线程数与磁盘扇区 → CSV。"""

    def __init__(self, pid, out_path):
        super().__init__(daemon=True)
        self.pid = pid
        self.out_path = out_path
        self.stop_flag = threading.Event()
        self.t0 = time.time()

    def run(self):
        with open(self.out_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["t_s", "state", "nthreads", "utime_s", "stime_s",
                        "disk_r_mb", "disk_w_mb", "disk_io_ms"])
            while not self.stop_flag.is_set():
                st = read_pid_stat(self.pid)
                dk = read_diskstats(DISK_DEV)
                if st and dk:
                    w.writerow([
                        f"{time.time() - self.t0:.1f}", st["state"],
                        st["nthreads"],
                        f"{st['utime'] / 100:.2f}", f"{st['stime'] / 100:.2f}",
                        f"{dk['r_sect'] * 512 / 2**20:.1f}",
                        f"{dk['w_sect'] * 512 / 2**20:.1f}",
                        dk["io_ms"]])
                    f.flush()
                self.stop_flag.wait(1.0)


def du(path):
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total


def parse_stdout(text):
    out = {}
    m = RE_MAIN.search(text)
    if m:
        out.update(bench=m.group(1), us_per_op=float(m.group(2)),
                   ops_per_sec=int(m.group(3)), seconds=float(m.group(4)),
                   operations=int(m.group(5)), mb_per_s=float(m.group(6)))
    p = RE_PCT.search(text)
    if p:
        out.update(p50=float(p.group(1)), p75=float(p.group(2)),
                   p99=float(p.group(3)), p999=float(p.group(4)),
                   p9999=float(p.group(5)))
    out["zf_props"] = {k: float(v) for k, v in RE_ZFPROP.findall(text)}
    return out


def run_variant(name, cfg, outdir, force=False):
    vdir = os.path.join(outdir, name)
    os.makedirs(vdir, exist_ok=True)
    result_path = os.path.join(vdir, "result.json")
    if os.path.exists(result_path) and not force:
        print(f"[{name}] result.json 已存在，跳过（--force 重跑）")
        return
    db_dir = f"/tmp/zf_prof_db_{name}"
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir)

    cmd = [cfg["bin"]] + common_flags(db_dir) + cfg["extra"]
    env = dict(os.environ)
    env["LD_LIBRARY_PATH"] = "/usr/lib/x86_64-linux-gnu:" + cfg["build"]
    result = dict(variant=name, cmd=" ".join(cmd), started=time.strftime("%F %T"))

    print(f"[{name}] 启动（完整命令见 result.json）", flush=True)
    stdout_path = os.path.join(vdir, "stdout.txt")
    t0 = time.time()
    with open(stdout_path, "w") as sf:
        proc = subprocess.Popen(cmd, stdout=sf, stderr=subprocess.STDOUT,
                                env=env)
        sampler = Sampler(proc.pid, os.path.join(vdir, "samples.csv"))
        sampler.start()
        try:
            rc = proc.wait(timeout=TIMEOUT_SEC)
            result["timeout"] = False
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            rc = -9
            result["timeout"] = True
        sampler.stop_flag.set()
        sampler.join()
    result["wall_sec"] = round(time.time() - t0, 1)
    result["rc"] = rc

    with open(stdout_path, errors="replace") as f:
        result.update(parse_stdout(f.read()))
    result["db_size_bytes"] = du(db_dir)
    result["db_dir"] = db_dir
    # LOG 保留在 db_dir，记录路径供解析器使用
    result["log_files"] = sorted(
        os.path.join(db_dir, n) for n in os.listdir(db_dir)
        if n.startswith("LOG"))
    result["perf_note"] = "perf_event_paranoid=4，perf record 不可用，已跳过"
    with open(result_path, "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    ops = result.get("ops_per_sec")
    print(f"[{name}] 完成 rc={rc} wall={result['wall_sec']}s "
          f"ops/s={ops} p50={result.get('p50')}us", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--variants", default="W0,W1,W2,W3")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--num", type=int, default=0,
                    help="覆盖 key 数（默认 8M=2.2GB 剖析规模；50GB 验证传 197379011）")
    ap.add_argument("--timeout-min", type=int, default=90)
    args = ap.parse_args()
    global NUM_KEYS, WRITES_PER_THREAD, TIMEOUT_SEC
    if args.num > 0:
        NUM_KEYS = args.num
        WRITES_PER_THREAD = NUM_KEYS // THREADS
    TIMEOUT_SEC = args.timeout_min * 60
    for name in args.variants.split(","):
        name = name.strip()
        if name not in VARIANTS:
            sys.exit(f"未知变体 {name}，可选 {list(VARIANTS)}")
        run_variant(name, VARIANTS[name], HERE, force=args.force)
    print("全部变体完成")


if __name__ == "__main__":
    main()
