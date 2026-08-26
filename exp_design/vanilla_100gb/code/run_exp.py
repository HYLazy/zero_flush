#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""run_exp.py - 原版 RocksDB 100GB fillrandom 基线实验驱动。

职责：
  1. 以与 ZeroFlush R42 对齐（去 ZF flag）的参数启动 db_bench（fillrandom，
     100M × 1KB = 100GiB，16 线程，stats 每 30s dump 一次）；
  2. 1s 采样器记录进程状态与磁盘扇区（/proc）→ samples.csv；
  3. 运行结束后收集 LOG*、DB 目录大小，写 cmd.json / result.json；
  4. 可选：dd 标定设备峰值写带宽 → peak_bw.json。

用法：
  python3 run_exp.py --out <run_dir>
      [--num 100000000] [--value-size 1024] [--threads 16]
      [--timeout-min 900] [--stats-interval 30]
      [--keep-db] [--no-peak-bw] [--force]

产物（run_dir）：
  stdout.txt  cmd.json  result.json  samples.csv  LOG*  peak_bw.json(可选)
"""

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = "/home/embed/hyl/metadata_offload"
BIN = f"{ROOT}/source/rocksdb/build/db_bench"
BUILD = f"{ROOT}/source/rocksdb/build"
DEFAULT_DB = "/tmp/vanilla_db"

# 与 ZeroFlush 50GB 终验 R42 对齐（去掉 zf_* / zeroflush 专属 flag）
DEFAULT_NUM = 100_000_000
DEFAULT_VALUE = 1024
DEFAULT_THREADS = 16
DEFAULT_STATS_INTERVAL = 30
DEFAULT_TIMEOUT_MIN = 900
CACHE_SIZE = 536870912      # 512MB
WRITE_BUF_SIZE = 268435456  # 256MB
MAX_BG_JOBS = 24
SUBCOMPACTIONS = 16


def detect_disk_dev(path):
    """返回 path 所在文件系统的块设备名（如 nvme0n1p3）。"""
    try:
        out = subprocess.run(["df", "-P", path], capture_output=True,
                             text=True, timeout=5)
        for line in out.stdout.strip().splitlines()[1:]:
            parts = line.split()
            if parts:
                return os.path.basename(parts[0])
    except Exception:
        pass
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


def read_pid_stat(pid):
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            parts = f.read().split()
        return dict(state=parts[2].decode(),
                    utime=int(parts[13]), stime=int(parts[14]),
                    nthreads=int(parts[19]))
    except (OSError, IndexError, ValueError):
        return None


class Sampler(threading.Thread):
    """周期轮询进程 CPU/线程数与磁盘扇区 → CSV（值均为累计量，差分在解析端）。"""

    def __init__(self, pid, out_path, dev, interval=1.0):
        super().__init__(daemon=True)
        self.pid, self.out_path, self.dev = pid, out_path, dev
        self.interval = interval
        self.stop_flag = threading.Event()
        self.t0 = time.time()
        self.rows = 0

    def run(self):
        with open(self.out_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["t_s", "state", "nthreads", "utime_s", "stime_s",
                        "disk_r_mb", "disk_w_mb", "disk_io_ms"])
            while not self.stop_flag.is_set():
                st = read_pid_stat(self.pid)
                dk = read_diskstats(self.dev)
                if st and dk:
                    w.writerow([
                        f"{time.time() - self.t0:.1f}", st["state"],
                        st["nthreads"],
                        f"{st['utime'] / 100:.2f}", f"{st['stime'] / 100:.2f}",
                        f"{dk['r_sect'] * 512 / 2**20:.1f}",
                        f"{dk['w_sect'] * 512 / 2**20:.1f}",
                        dk["io_ms"]])
                    f.flush()
                    self.rows += 1
                self.stop_flag.wait(self.interval)


def build_cmd(num, value_size, threads, stats_interval, db_dir):
    writes = num // threads
    return [
        BIN,
        "--benchmarks=fillrandom",
        f"--db={db_dir}",
        f"--num={num}",
        "--key_size=16",
        f"--value_size={value_size}",
        f"--threads={threads}",
        f"--writes={writes}",
        "--compression_type=none",
        "--disable_wal=false",
        "--histogram=true",
        "--statistics=true",
        f"--cache_size={CACHE_SIZE}",
        f"--write_buffer_size={WRITE_BUF_SIZE}",
        f"--max_background_jobs={MAX_BG_JOBS}",
        f"--subcompactions={SUBCOMPACTIONS}",
        f"--stats_interval_seconds={stats_interval}",
        "--stats_per_interval=1",
    ]


def run_peak_bw(out_dir, dev, size_mb=8192):
    """dd 写 size_mb MB，期间 0.2s 采样 diskstats，返回峰值写 MB/s。"""
    path = os.path.join(out_dir, "peak_bw_test.bin")
    result = {"dev": dev, "size_mb": size_mb}
    try:
        shutil.rmtree(path, ignore_errors=True)
        samples = []
        dk0 = read_diskstats(dev)
        flags = ["dd", f"if=/dev/zero", f"of={path}", "bs=1M",
                 f"count={size_mb}", "conv=fdatasync", "oflag=direct"]
        proc = subprocess.Popen(flags, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL)
        t0 = time.time()
        stop = threading.Event()

        def collect():
            while not stop.is_set():
                dk = read_diskstats(dev)
                if dk:
                    samples.append((time.time() - t0, dk["w_sect"]))
                time.sleep(0.2)

        th = threading.Thread(target=collect, daemon=True)
        th.start()
        proc.wait(timeout=600)
        stop.set()
        th.join()
        elapsed = time.time() - t0
        if len(samples) >= 2:
            w0 = dk0["w_sect"] if dk0 else samples[0][1]
            w1 = samples[-1][1]
            total_mb = (w1 - w0) * 512 / 2**20
            result["write_mb"] = round(total_mb, 1)
            result["elapsed_s"] = round(elapsed, 2)
            result["avg_write_mb_s"] = round(total_mb / max(elapsed, 0.01), 1)
            # 2s 滑窗峰值（瞬时平均易低估 NVMe，总平均又偏保守）
            win = 2.0
            j = 0
            peak = 0.0
            for i in range(len(samples)):
                while samples[i][0] - samples[j][0] > win:
                    j += 1
                dt = samples[i][0] - samples[j][0]
                if dt > 0.2:
                    mb = (samples[i][1] - samples[j][1]) * 512 / 2**20
                    peak = max(peak, mb / dt)
            result["peak_write_mb_s"] = round(peak, 1)
            result["direct_ok"] = True
        else:
            result["error"] = "no diskstats samples"
    except subprocess.TimeoutExpired:
        result["error"] = "dd timeout"
    except Exception as e:  # noqa: BLE001
        result["error"] = str(e)
    finally:
        shutil.rmtree(path, ignore_errors=True)
    with open(os.path.join(out_dir, "peak_bw.json"), "w") as f:
        json.dump(result, f, indent=2)
    return result


def collect_hw():
    """轻量硬件信息（与 exp_common.collect_hw_json 同口径的简化版）。"""
    hw = {"cpu_cores": os.cpu_count()}
    try:
        out = subprocess.run(["nproc"], capture_output=True, text=True)
        hw["nproc"] = int(out.stdout.strip())
    except Exception:
        pass
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    hw["mem_total_kb"] = int(line.split()[1])
                    break
    except OSError:
        pass
    try:
        out = subprocess.run(["uname", "-r"], capture_output=True, text=True)
        hw["kernel"] = out.stdout.strip()
    except Exception:
        pass
    return hw


def du(path):
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True, help="run 目录（自动创建）")
    ap.add_argument("--num", type=int, default=DEFAULT_NUM)
    ap.add_argument("--value-size", type=int, default=DEFAULT_VALUE)
    ap.add_argument("--threads", type=int, default=DEFAULT_THREADS)
    ap.add_argument("--timeout-min", type=int, default=DEFAULT_TIMEOUT_MIN)
    ap.add_argument("--stats-interval", type=int, default=DEFAULT_STATS_INTERVAL)
    ap.add_argument("--keep-db", action="store_true", help="结束后保留 DB 目录")
    ap.add_argument("--no-peak-bw", action="store_true", help="跳过峰值带宽标定")
    ap.add_argument("--force", action="store_true", help="run_dir 已存在时覆盖")
    args = ap.parse_args()

    out_dir = os.path.abspath(args.out)
    if os.path.exists(out_dir):
        if args.force:
            shutil.rmtree(out_dir)
        else:
            sys.exit(f"run_dir 已存在: {out_dir}（--force 覆盖）")
    os.makedirs(out_dir)

    db_dir = f"{DEFAULT_DB}_{os.path.basename(out_dir)}"
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir)
    dev = detect_disk_dev(db_dir) or "nvme0n1p3"

    cmd = build_cmd(args.num, args.value_size, args.threads,
                    args.stats_interval, db_dir)
    cmd_json = {
        "bin": BIN,
        "cmd": cmd,
        "num": args.num, "value_size": args.value_size,
        "threads": args.threads,
        "db_dir": db_dir, "disk_dev": dev,
        "stats_interval_seconds": args.stats_interval,
        "timeout_min": args.timeout_min,
        "started": time.strftime("%F %T"),
        "hardware": collect_hw(),
        "note": "vanilla RocksDB 11.2.0, 参数对齐 ZeroFlush R42 去掉 zf flag",
    }
    with open(os.path.join(out_dir, "cmd.json"), "w") as f:
        json.dump(cmd_json, f, indent=2, ensure_ascii=False)

    if not args.no_peak_bw:
        print(f"[peak-bw] dd 8GB 标定 {dev} 峰值写带宽...", flush=True)
        pb = run_peak_bw(out_dir, dev)
        print(f"[peak-bw] {pb.get('peak_write_mb_s')} MB/s", flush=True)

    print(f"[run] {cmd_json['started']} 启动 db_bench（{args.num} keys × "
          f"{args.value_size}B，timeout {args.timeout_min}min）", flush=True)
    stdout_path = os.path.join(out_dir, "stdout.txt")
    result = {"cmd": cmd_json, "started": cmd_json["started"],
              "disk_dev": dev}
    env = dict(os.environ)
    env["LD_LIBRARY_PATH"] = "/usr/lib/x86_64-linux-gnu:" + BUILD
    t0 = time.time()
    with open(stdout_path, "w") as sf:
        proc = subprocess.Popen(cmd, stdout=sf, stderr=subprocess.STDOUT,
                                env=env)
        sampler = Sampler(proc.pid, os.path.join(out_dir, "samples.csv"), dev)
        sampler.start()
        try:
            rc = proc.wait(timeout=args.timeout_min * 60)
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
    result["samples_rows"] = sampler.rows

    # 收集 LOG 与 DB 大小
    log_files = [os.path.join(db_dir, n) for n in os.listdir(db_dir)
                 if n.startswith("LOG")]
    for lf in log_files:
        shutil.copy2(lf, out_dir)
    result["log_files"] = [os.path.basename(lf) for lf in log_files]
    result["db_size_bytes"] = du(db_dir)
    result["db_size_gb"] = round(result["db_size_bytes"] / 2**30, 2)

    with open(os.path.join(out_dir, "result.json"), "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    if not args.keep_db:
        shutil.rmtree(db_dir, ignore_errors=True)
    print(f"[run] 完成 rc={rc} wall={result['wall_sec']}s "
          f"db={result['db_size_gb']}GB → {out_dir}", flush=True)


if __name__ == "__main__":
    main()
