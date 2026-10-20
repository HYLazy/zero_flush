#!/usr/bin/env python3
"""ZeroFlush M3.3 vs 原生 RocksDB — 50GB 性能对比基准测试。

用法：
  # 先测原生
  python3 run_benchmark.py --engine native
  # 再测 ZeroFlush
  python3 run_benchmark.py --engine zeroflush
  # 生成报告（两个引擎都跑完后）
  python3 run_benchmark.py --report
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ── 路径 ──────────────────────────────────────────────
NATIVE_DB_BENCH = "/home/embed/hyl/metadata_offload/source/rocksdb/build/db_bench"
ZF_DB_BENCH = "/home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/build/db_bench"
OUT_DIR = Path("/home/embed/hyl/metadata_offload/output/zeroflush_m3_perf")
TMP_DB_BASE = "/tmp/zf_m3_perf_db"

# ── 测试参数 ──────────────────────────────────────────
KEY_SIZE = 16
TOTAL_BYTES = 50 * 1024 * 1024 * 1024  # 50 GB（逻辑数据）
READS_LIMIT = 1_000_000                  # readrandom 读取次数
THREADS = 16
VALUE_SIZES = [256, 1024]         # 两种 value 尺寸（64B 已取消：7层LSM下写入过慢）
ZF_VALUE_SIZES = [256, 1024]      # P1 修复 stall 触发器后 vs256 可跑（旧限制已解除）
WRITE_BUFFER_SIZE = 64 * 1024 * 1024    # 64 MB
COMPRESSION = "none"

# ── 正则（解析 db_bench 输出）────────────────────────
RE_MAIN = re.compile(
    r"(?P<name>\S+)\s*:\s*(?P<us_per_op>[\d.]+)\s*micros/op\s+"
    r"(?P<ops_per_sec>[\d.]+)\s+ops/sec\s+"
    r"(?P<seconds>[\d.]+)\s+seconds\s+"
    r"(?P<operations>\d+)\s+operations;\s*"
    r"(?P<mb_per_sec>[\d.]+)\s+MB/s"
)

RE_PERCENTILES = re.compile(
    r"Percentiles:\s+P50:\s+(?P<p50>[\d.]+)"
    r"\s+P75:\s+(?P<p75>[\d.]+)"
    r"\s+P99:\s+(?P<p99>[\d.]+)"
    r"\s+P99\.9:\s+(?P<p999>[\d.]+)"
    r"\s+P99\.99:\s+(?P<p9999>[\d.]+)"
)

RE_MICROSEC = re.compile(
    r"Microseconds per (?:write|read):\s*"
    r"Count:\s+(?P<count>\d+)\s+"
    r"Average:\s+(?P<avg>[\d.]+)\s+"
    r"StdDev:\s+(?P<stddev>[\d.]+)\s*"
    r"Min:\s+(?P<min>\d+)\s+"
    r"Median:\s+(?P<median>[\d.]+)\s+"
    r"Max:\s+(?P<max>\d+)"
)


def parse_bench_output(output: str) -> Dict:
    """解析 db_bench stdout，返回指标字典。"""
    metrics: Dict = {}
    for line in output.splitlines():
        m = RE_MAIN.search(line)
        if m:
            metrics["us_per_op"] = float(m.group("us_per_op"))
            metrics["ops_per_sec"] = float(m.group("ops_per_sec"))
            metrics["mb_per_sec"] = float(m.group("mb_per_sec"))
            metrics["seconds"] = float(m.group("seconds"))
            metrics["operations"] = int(m.group("operations"))
        pm = RE_PERCENTILES.search(line)
        if pm:
            metrics["p50_us"] = float(pm.group("p50"))
            metrics["p75_us"] = float(pm.group("p75"))
            metrics["p99_us"] = float(pm.group("p99"))
            metrics["p999_us"] = float(pm.group("p999"))
            metrics["p9999_us"] = float(pm.group("p9999"))
        mm = RE_MICROSEC.search(line)
        if mm:
            metrics["avg_us"] = float(mm.group("avg"))
            metrics["stddev_us"] = float(mm.group("stddev"))
            metrics["min_us"] = int(mm.group("min"))
            metrics["median_us"] = float(mm.group("median"))
            metrics["max_us"] = int(mm.group("max"))
    return metrics


def calc_num_keys(value_size: int) -> int:
    """50GB 逻辑数据对应的 key 数。"""
    return TOTAL_BYTES // (KEY_SIZE + value_size)


def get_db_size(db_dir: str) -> int:
    total = 0
    for dirpath, _dirnames, filenames in os.walk(db_dir):
        for f in filenames:
            try:
                total += os.path.getsize(os.path.join(dirpath, f))
            except OSError:
                pass
    return total


def collect_hardware() -> Dict:
    info: Dict = {}
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if "model name" in line:
                    info["cpu_model"] = line.split(":", 1)[1].strip()
                    break
        with open("/proc/cpuinfo") as f:
            info["cpu_cores"] = sum(1 for line in f if line.startswith("processor"))
    except Exception:
        info["cpu_model"] = "Unknown"
        info["cpu_cores"] = 0
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    info["memory_gb"] = round(int(line.split()[1]) / (1024 * 1024), 2)
                    break
    except Exception:
        info["memory_gb"] = 0
    try:
        info["os"] = subprocess.check_output(["uname", "-o"], text=True).strip()
        info["kernel"] = subprocess.check_output(["uname", "-r"], text=True).strip()
    except Exception:
        info["os"] = info["kernel"] = ""
    return info


def run_db_bench(engine: str, value_size: int, benchmark: str, db_dir: str,
                 num_keys: int) -> Tuple[str, int, float]:
    """执行一次 db_bench 调用，返回 (stdout, returncode, wall_clock_sec)。

    num_keys 是 key 空间总数（--num 决定 KeyGenerator 的 key 范围）。
    ⚠️ P0 修正：db_bench 的 --writes/--reads 是【每线程】配额，总操作数 = 配额 × THREADS。
    故传 --writes=num_keys//THREADS 使总写入 = num_keys（≈50GB 逻辑数据），
    而非旧版误传 num_keys 导致 16 线程写入 16 倍（800GB）。
    """
    if engine == "zeroflush":
        db_bench_path = ZF_DB_BENCH
        # P1 修复：ZF 每分区产生一个 L0 文件，L0 恒 ≈ zf_partitions=64，
        # 默认 slowdown=20/stop=36 会触发持续 write stop，必须抬升到分区数之上
        extra = ["--zeroflush", "--zf_partitions=64",
                 "--level0_slowdown_writes_trigger=64",
                 "--level0_stop_writes_trigger=72"]
    else:
        db_bench_path = NATIVE_DB_BENCH
        extra = []

    cmd = [
        db_bench_path,
        f"--benchmarks={benchmark}",
        f"--db={db_dir}",
        f"--num={num_keys}",
        f"--key_size={KEY_SIZE}",
        f"--value_size={value_size}",
        f"--threads={THREADS}",
        f"--compression_type={COMPRESSION}",
        "--disable_wal=false",
        "--histogram=true",
        "--statistics=true",
        "--cache_size=8388608",
        "--write_buffer_size=268435456",
        "--max_background_jobs=16",
    ] + extra

    if benchmark == "fillrandom":
        # --writes 为每线程配额：总写入 = num_keys//THREADS × THREADS ≈ num_keys
        cmd.append(f"--writes={num_keys // THREADS}")
    elif benchmark == "readrandom":
        # --reads 同样为每线程配额：总读取 = READS_LIMIT
        cmd.append(f"--reads={READS_LIMIT // THREADS}")
        cmd.append("--use_existing_db=true")

    env = os.environ.copy()
    if engine == "native":
        # 原生 RocksDB 需要 LD_LIBRARY_PATH 避免 anaconda libstdc++ 冲突
        native_build = os.path.dirname(NATIVE_DB_BENCH)
        env["LD_LIBRARY_PATH"] = f"/usr/lib/x86_64-linux-gnu:{native_build}"

    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env,
                          timeout=86400)  # 24h 超时（P0 修正后单场景预期 ≤2h，留足余量）
    elapsed = time.time() - t0
    return proc.stdout, proc.returncode, elapsed


def run_engine(engine: str) -> Dict:
    """跑一组 value_size 的 fillrandom + readrandom。"""
    hardware = collect_hardware()
    runs = []

    value_sizes = ZF_VALUE_SIZES if engine == "zeroflush" else VALUE_SIZES
    for vs in value_sizes:
        try:
            num_keys = calc_num_keys(vs)
            logical_gb = (num_keys * (KEY_SIZE + vs)) / (1024 ** 3)
            run_dir = f"{TMP_DB_BASE}_{engine}_vs{vs}"

            print(f"\n{'='*60}")
            print(f"[{engine}] value_size={vs}B  keys={num_keys:,}  data={logical_gb:.1f}GB  "
                  f"({THREADS} threads × {num_keys // THREADS:,} writes/thread)")
            print(f"{'='*60}")

            # ── 清理旧数据 ──
            if os.path.exists(run_dir):
                shutil.rmtree(run_dir)
            os.makedirs(run_dir, exist_ok=True)

            entry = {
                "value_size": vs,
                "num_keys": num_keys,
                "logical_data_gb": round(logical_gb, 2),
                "fill": {},
                "read": {},
            }

            # ── fillrandom ──
            print(f"  [fillrandom] writing {num_keys:,} keys total ({num_keys // THREADS:,}/thread)...", flush=True)
            out, rc, wall = run_db_bench(engine, vs, "fillrandom", run_dir, num_keys)
            if rc != 0:
                print(f"  [FAIL] fillrandom exit={rc}")
                entry["fill"]["error"] = f"exit={rc}"
                entry["fill"]["raw_tail"] = out[-500:]
            else:
                m = parse_bench_output(out)
                entry["fill"] = m | {"wall_clock_sec": round(wall, 2)}
                print(f"    ops/s={m.get('ops_per_sec', 0):,.0f}  "
                      f"MB/s={m.get('mb_per_sec', 0):.1f}  "
                      f"us/op={m.get('us_per_op', 0):.2f}  "
                      f"P99={m.get('p99_us', 0):.2f}us")

            # ── DB 尺寸（fill 后） ──
            db_size = get_db_size(run_dir)
            amp = db_size / (num_keys * (KEY_SIZE + vs)) if num_keys * (KEY_SIZE + vs) > 0 else 0
            entry["disk_size_gb"] = round(db_size / (1024 ** 3), 2)
            entry["write_amplification"] = round(amp, 2)
            print(f"    db_size={entry['disk_size_gb']:.1f}GB  w-amp={entry['write_amplification']:.2f}")

            # ── readrandom ──
            print(f"  [readrandom] reading {READS_LIMIT:,} keys total ({READS_LIMIT // THREADS:,}/thread)...", flush=True)
            out, rc, wall = run_db_bench(engine, vs, "readrandom", run_dir, num_keys)
            if rc != 0:
                print(f"  [FAIL] readrandom exit={rc}")
                entry["read"]["error"] = f"exit={rc}"
                entry["read"]["raw_tail"] = out[-500:]
            else:
                m = parse_bench_output(out)
                entry["read"] = m | {"wall_clock_sec": round(wall, 2)}
                print(f"    ops/s={m.get('ops_per_sec', 0):,.0f}  "
                      f"MB/s={m.get('mb_per_sec', 0):.1f}  "
                      f"us/op={m.get('us_per_op', 0):.2f}  "
                      f"P99={m.get('p99_us', 0):.2f}us")

        finally:
            # ── 清理（每次测试后清除过程文件，异常路径也执行）──
            if os.path.exists(run_dir):
                shutil.rmtree(run_dir)
        runs.append(entry)
        print(f"  [DONE] vs={vs}B")

    result = {
        "engine": engine,
        "db_bench_path": ZF_DB_BENCH if engine == "zeroflush" else NATIVE_DB_BENCH,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": {
            "total_data_gb": TOTAL_BYTES / (1024 ** 3),
            "key_size": KEY_SIZE,
            "threads": THREADS,
            "reads_limit": READS_LIMIT,
            "write_buffer_size": WRITE_BUFFER_SIZE,
            "compression": COMPRESSION,
        },
        "hardware": hardware,
        "runs": runs,
    }
    return result


# ═══════════════════════════════════════════════════════
#  报告生成
# ═══════════════════════════════════════════════════════

def generate_report(native_data: Dict, zf_data: Dict):
    """从两个引擎的 JSON 数据生成 HTML 对比报告。"""
    native_runs = {r["value_size"]: r for r in native_data["runs"]}
    zf_runs = {r["value_size"]: r for r in zf_data["runs"]}
    vs_list = sorted(native_runs.keys())

    # ── 构造表格行 ──
    table_rows = ""
    for bench_name, bench_cn in [("fill", "fillrandom (随机写)"), ("read", "readrandom (随机读)")]:
        table_rows += f'<tr><th colspan="10" style="background:#eef">{bench_cn}</th></tr>\n'
        table_rows += (
            '<tr style="font-size:0.9em">'
            '<th>vs</th>'
            '<th>Native<br>ops/s</th><th>Native<br>MB/s</th><th>Native<br>us/op</th>'
            '<th>ZF<br>ops/s</th><th>ZF<br>MB/s</th><th>ZF<br>us/op</th>'
            '<th>ZF/Native</th>'
            '<th>P50 (N/Z)</th>'
            '<th>P99 (N/Z)</th>'
            '</tr>\n'
        )
        for vs in vs_list:
            n = native_runs[vs].get(bench_name, {})
            z = zf_runs[vs].get(bench_name, {})
            if not n or not z:
                continue
            n_ops = n.get("ops_per_sec", 0)
            z_ops = z.get("ops_per_sec", 0)
            ratio = z_ops / n_ops * 100 if n_ops else 0
            color = "#0a0" if ratio >= 100 else "#a00"
            faster = "ZeroFlush" if ratio >= 100 else "Native"
            # 附注：重写字节比 W-amp 衡量
            n_p50 = f'{n.get("p50_us", 0):.1f}/{z.get("p50_us", 0):.1f}'
            n_p99 = f'{n.get("p99_us", 0):.1f}/{z.get("p99_us", 0):.1f}'
            table_rows += f'''
            <tr>
              <td>{vs}B</td>
              <td align="right">{n_ops:,.0f}</td>
              <td align="right">{n.get("mb_per_sec", 0):.1f}</td>
              <td align="right">{n.get("us_per_op", 0):.2f}</td>
              <td align="right">{z_ops:,.0f}</td>
              <td align="right">{z.get("mb_per_sec", 0):.1f}</td>
              <td align="right">{z.get("us_per_op", 0):.2f}</td>
              <td align="right" style="color:{color};font-weight:bold">{ratio:.1f}%</td>
              <td align="right">{n_p50} us</td>
              <td align="right">{n_p99} us</td>
              <td align="left">{faster}</td>
            </tr>'''

    # ── 写放大表 ──
    amp_rows = ""
    for vs in vs_list:
        n = native_runs[vs]
        z = zf_runs[vs]
        amp_rows += f'''
        <tr>
          <td>{vs}B</td>
          <td align="right">{n.get("logical_data_gb", 0):.1f}</td>
          <td align="right">{n.get("disk_size_gb", 0):.1f}</td>
          <td align="right">{n.get("write_amplification", 0):.2f}</td>
          <td align="right">{z.get("disk_size_gb", 0):.1f}</td>
          <td align="right">{z.get("write_amplification", 0):.2f}</td>
        </tr>'''

    # ── Chart.js 数据 ──
    import json as _j
    chart_fill = _j.dumps({
        "labels": [f"{vs}B" for vs in vs_list],
        "datasets": [
            {"label": "Native fillrandom", "data": [native_runs[vs].get("fill", {}).get("ops_per_sec", 0) for vs in vs_list],
             "backgroundColor": "#4caf50", "borderColor": "#388e3c"},
            {"label": "ZeroFlush fillrandom", "data": [zf_runs[vs].get("fill", {}).get("ops_per_sec", 0) for vs in vs_list],
             "backgroundColor": "#81c784", "borderColor": "#4caf50"},
        ]
    })
    chart_read = _j.dumps({
        "labels": [f"{vs}B" for vs in vs_list],
        "datasets": [
            {"label": "Native readrandom", "data": [native_runs[vs].get("read", {}).get("ops_per_sec", 0) for vs in vs_list],
             "backgroundColor": "#2196f3", "borderColor": "#1565c0"},
            {"label": "ZeroFlush readrandom", "data": [zf_runs[vs].get("read", {}).get("ops_per_sec", 0) for vs in vs_list],
             "backgroundColor": "#64b5f6", "borderColor": "#2196f3"},
        ]
    })
    chart_latency = _j.dumps({
        "labels": [f"{vs}B" for vs in vs_list],
        "datasets": [
            {"label": "Native fill P99", "data": [native_runs[vs].get("fill", {}).get("p99_us", 0) for vs in vs_list],
             "backgroundColor": "#e53935", "borderColor": "#b71c1c"},
            {"label": "ZeroFlush fill P99", "data": [zf_runs[vs].get("fill", {}).get("p99_us", 0) for vs in vs_list],
             "backgroundColor": "#ff7043", "borderColor": "#e64a19"},
            {"label": "Native read P99", "data": [native_runs[vs].get("read", {}).get("p99_us", 0) for vs in vs_list],
             "backgroundColor": "#5c6bc0", "borderColor": "#283593"},
            {"label": "ZeroFlush read P99", "data": [zf_runs[vs].get("read", {}).get("p99_us", 0) for vs in vs_list],
             "backgroundColor": "#9fa8da", "borderColor": "#5c6bc0"},
        ]
    })
    hw = native_data.get("hardware", {})
    cfg = native_data.get("config", {})

    html = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>ZeroFlush M3.3 vs 原生 RocksDB — 50GB 性能对比报告</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0"></script>
<style>
  body {{ font-family: -apple-system, "Segoe UI", "PingFang SC", sans-serif;
         max-width: 1200px; margin: 30px auto; padding: 0 20px; color: #222; }}
  h1 {{ border-bottom: 3px solid #1976d2; padding-bottom: 8px; }}
  h2 {{ color: #1976d2; margin-top: 32px; }}
  .meta {{ background: #f5f5f5; padding: 12px 16px; border-left: 4px solid #1976d2; }}
  table {{ border-collapse: collapse; width: 100%; margin: 16px 0; }}
  th, td {{ padding: 6px 10px; border: 1px solid #ddd; font-size: 0.95em; }}
  th {{ background: #f0f0f0; }}
  .chart-container {{ position: relative; height: 360px; margin: 24px 0; }}
  .finding {{ background: #fff8e1; border-left: 4px solid #ffb300; padding: 12px 16px;
              margin: 12px 0; }}
  .conclusion {{ background: #e8f5e9; border-left: 4px solid #4caf50; padding: 12px 16px;
                 margin: 16px 0; }}
  code {{ background: #f0f0f0; padding: 2px 6px; border-radius: 3px; }}
</style>
</head>
<body>

<h1>ZeroFlush M3.3 vs 原生 RocksDB — 50GB 性能对比报告</h1>

<div class="meta">
  <strong>生成时间</strong>：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}<br>
  <strong>工作负载</strong>：fillrandom（50GB 逻辑写入）+ readrandom（{READS_LIMIT:,} 次随机读）<br>
  <strong>线程数</strong>：{cfg.get("threads", THREADS)} &nbsp;|&nbsp;
  <strong>Key 大小</strong>：{cfg.get("key_size", KEY_SIZE)}B &nbsp;|&nbsp;
  <strong>压缩</strong>：{cfg.get("compression", COMPRESSION)}<br>
  <strong>CPU</strong>：{hw.get("cpu_model", "?")}（{hw.get("cpu_cores", "?")} 核） &nbsp;|&nbsp;
  <strong>内存</strong>：{hw.get("memory_gb", "?")}GB &nbsp;|&nbsp;
  <strong>OS</strong>：{hw.get("os", "?")} {hw.get("kernel", "")}<br>
  <strong>原生 DB</strong>：{native_data.get("db_bench_path", "")}<br>
  <strong>ZeroFlush DB</strong>：{zf_data.get("db_bench_path", "")} --zeroflush --zf_partitions=64
</div>

<h2>1. fillrandom 随机写吞吐量 (ops/s)</h2>
<div class="chart-container"><canvas id="fillChart"></canvas></div>

<h2>2. readrandom 随机读吞吐量 (ops/s)</h2>
<div class="chart-container"><canvas id="readChart"></canvas></div>

<h2>3. 尾延迟 P99 对比 (μs, 越低越好)</h2>
<div class="chart-container"><canvas id="latChart"></canvas></div>

<h2>4. 详细吞吐量 &amp; 延迟数据</h2>
<table>
  <tr>
    <th rowspan="2" style="background:#fafafa">vs</th>
    <th colspan="3">原生 RocksDB</th>
    <th colspan="3">ZeroFlush M3.3</th>
    <th rowspan="2" style="background:#fafafa">ZF/Native</th>
    <th rowspan="2" colspan="2" style="background:#fafafa">延迟分布 us (N/Z)</th>
    <th rowspan="2" style="background:#fafafa">更优</th>
  </tr>
  <tr>
    <th>ops/s</th><th>MB/s</th><th>μs/op</th>
    <th>ops/s</th><th>MB/s</th><th>μs/op</th>
  </tr>
  {table_rows}
</table>

<h2>5. 写放大 (DB 磁盘 / 逻辑数据)</h2>
<table>
  <tr><th>vs</th><th>逻辑 (GB)</th><th colspan="2">原生</th><th colspan="2">ZeroFlush</th></tr>
  <tr><th></th><th></th><th>磁盘 GB</th><th>W-Amp</th><th>磁盘 GB</th><th>W-Amp</th></tr>
  {amp_rows}
</table>

<h2>6. 关键发现</h2>

<div class="finding" id="findings">
  <p><em>数据分析将在两引擎测试完成后填充。</em></p>
</div>

<div class="conclusion">
  <p><strong>测试配置摘要</strong>：50GB 逻辑数据集，{THREADS} 线程，无压缩，8MB block cache，{cfg.get('write_buffer_size', 67108864) // 1048576}MB write_buffer。</p>
  <p>ZeroFlush 配置：P=64、kHash 路由、partition_target_bytes=64MB、epoch_target_bytes=256MB、install_below_l0=true、merge_into_base_level=false（默认）。</p>
</div>

<script>
const fillCtx = document.getElementById('fillChart').getContext('2d');
new Chart(fillCtx, {{ type: 'bar', data: {chart_fill},
  options: {{ responsive: true, maintainAspectRatio: false,
    scales: {{ y: {{ type: 'logarithmic', title: {{ display: true, text: 'ops/sec (log)' }} }} }},
    plugins: {{ legend: {{ position: 'top' }}, title: {{ display: true, text: 'fillrandom 吞吐量对比' }} }}
  }}
}});
const readCtx = document.getElementById('readChart').getContext('2d');
new Chart(readCtx, {{ type: 'bar', data: {chart_read},
  options: {{ responsive: true, maintainAspectRatio: false,
    scales: {{ y: {{ type: 'logarithmic', title: {{ display: true, text: 'ops/sec (log)' }} }} }},
    plugins: {{ legend: {{ position: 'top' }}, title: {{ display: true, text: 'readrandom 吞吐量对比' }} }}
  }}
}});
const latCtx = document.getElementById('latChart').getContext('2d');
new Chart(latCtx, {{ type: 'bar', data: {chart_latency},
  options: {{ responsive: true, maintainAspectRatio: false,
    scales: {{ y: {{ type: 'logarithmic', title: {{ display: true, text: 'μs (log)' }} }} }},
    plugins: {{ legend: {{ position: 'top' }}, title: {{ display: true, text: 'P99 尾延迟对比' }} }}
  }}
}});
</script>

</body>
</html>'''
    out_path = OUT_DIR / "report.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"\n=> Report saved to {out_path}")
    return out_path


# ═══════════════════════════════════════════════════════
#  主入口
# ═══════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="ZeroFlush M3.3 vs 原生 RocksDB 性能对比")
    ap.add_argument("--engine", choices=["native", "zeroflush", "report"], default="report",
                    help="运行哪个引擎的 benchmark (native/zeroflush) 或 --engine report 生成报告")
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.engine == "report":
        native_path = OUT_DIR / "benchmark_native.json"
        zf_path = OUT_DIR / "benchmark_zeroflush.json"
        if not native_path.exists():
            print(f"ERROR: {native_path} not found. Run --engine native first.")
            return 1
        if not zf_path.exists():
            print(f"ERROR: {zf_path} not found. Run --engine zeroflush first.")
            return 1
        native_data = json.loads(native_path.read_text())
        zf_data = json.loads(zf_path.read_text())
        generate_report(native_data, zf_data)
        return 0

    result = run_engine(args.engine)
    out_path = OUT_DIR / f"benchmark_{args.engine}.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"\n=> Results saved to {out_path}")

    # 摘要
    for r in result["runs"]:
        fill = r.get("fill", {})
        read = r.get("read", {})
        print(f"  vs={r['value_size']}B: "
              f"fill={fill.get('ops_per_sec', 0):,.0f}ops/s "
              f"read={read.get('ops_per_sec', 0):,.0f}ops/s "
              f"w-amp={r.get('write_amplification', 0):.2f}")
    return 0


if __name__ == "__main__":
    main()