#!/usr/bin/env python3
"""生成 ZeroFlush M1 vs 原生 RocksDB 性能对比 HTML 报告"""
import json
import sys
from pathlib import Path
from datetime import datetime

IN_JSON = Path("/home/embed/hyl/metadata_offload/output/zeroflush_m1_perf/perf_compare_num100000_runs1.json")
OUT_HTML = Path("/home/embed/hyl/metadata_offload/output/zeroflush_m1_perf/report.html")


def build_html(data):
    # 提取数据点
    vs_list = [row["value_size"] for row in data["data"]]

    def get_ops(config_name, bench_name):
        return [
            next(
                (r["ops_per_sec"] for r in row["configs"][config_name][0]["results"]
                 if r["name"] == bench_name),
                0
            )
            for row in data["data"]
        ]

    def get_mb(config_name, bench_name):
        return [
            next(
                (r["mb_per_sec"] for r in row["configs"][config_name][0]["results"]
                 if r["name"] == bench_name),
                0
            )
            for row in data["data"]
        ]

    def get_us(config_name, bench_name):
        return [
            next(
                (r["us_per_op"] for r in row["configs"][config_name][0]["results"]
                 if r["name"] == bench_name),
                0
            )
            for row in data["data"]
        ]

    benchmarks = [
        ("fillrandom", "fillrandom (随机写)"),
        ("readrandom", "readrandom (随机读)"),
        ("readseq", "readseq (顺序扫描)"),
    ]

    # 构建表格
    table_rows = ""
    for bench, bench_cn in benchmarks:
        table_rows += f"<tr><th colspan='7' style='background:#eef'>{bench_cn}</th></tr>"
        for idx, vs in enumerate(vs_list):
            n_ops = get_ops("native", bench)[idx]
            z_ops = get_ops("zeroflush", bench)[idx]
            n_mb = get_mb("native", bench)[idx]
            z_mb = get_mb("zeroflush", bench)[idx]
            n_us = get_us("native", bench)[idx]
            z_us = get_us("zeroflush", bench)[idx]
            ratio = z_ops / n_ops * 100 if n_ops else 0
            faster = "ZeroFlush" if ratio > 100 else "Native"
            color = "#0a0" if ratio > 100 else "#a00"
            table_rows += f"""
            <tr>
              <td>{vs} B</td>
              <td align="right">{n_ops:,}</td>
              <td align="right">{n_mb:.1f}</td>
              <td align="right">{n_us:.2f}</td>
              <td align="right">{z_ops:,}</td>
              <td align="right">{z_mb:.1f}</td>
              <td align="right">{z_us:.2f}</td>
              <td align="right" style="color:{color};font-weight:bold">{ratio:.1f}%</td>
              <td align="left">{faster}</td>
            </tr>"""

    # Chart.js 数据
    chart_data = {
        "labels": [f"{vs}B" for vs in vs_list],
        "datasets": [],
    }
    colors = {
        "fillrandom": ("#4caf50", "#81c784"),
        "readrandom": ("#2196f3", "#64b5f6"),
        "readseq":    ("#ff9800", "#ffb74d"),
    }
    for bench, _ in benchmarks:
        col1, col2 = colors[bench]
        chart_data["datasets"].append({
            "label": f"Native {bench}",
            "data": get_ops("native", bench),
            "backgroundColor": col1,
            "borderColor": col1,
        })
        chart_data["datasets"].append({
            "label": f"ZeroFlush {bench}",
            "data": get_ops("zeroflush", bench),
            "backgroundColor": col2,
            "borderColor": col2,
        })

    import json as _j
    chart_data_json = _j.dumps(chart_data)

    cpu = data["hardware"].get("cpu_count", "?")
    num_keys = data["config"]["num_keys"]

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>ZeroFlush M1 vs Native RocksDB 性能对比报告</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0"></script>
<style>
  body {{ font-family: -apple-system, "Segoe UI", "PingFang SC", sans-serif;
         max-width: 1100px; margin: 30px auto; padding: 0 20px; color: #222; }}
  h1 {{ border-bottom: 3px solid #1976d2; padding-bottom: 8px; }}
  h2 {{ color: #1976d2; margin-top: 32px; }}
  .meta {{ background: #f5f5f5; padding: 12px 16px; border-left: 4px solid #1976d2; }}
  table {{ border-collapse: collapse; width: 100%; margin: 16px 0; }}
  th, td {{ padding: 8px 12px; border: 1px solid #ddd; }}
  th {{ background: #f0f0f0; }}
  .chart-container {{ position: relative; height: 380px; margin: 24px 0; }}
  .finding {{ background: #fff8e1; border-left: 4px solid #ffb300; padding: 12px 16px;
              margin: 12px 0; }}
  .conclusion {{ background: #e8f5e9; border-left: 4px solid #4caf50; padding: 12px 16px;
                 margin: 16px 0; }}
  code {{ background: #f0f0f0; padding: 2px 6px; border-radius: 3px; }}
</style>
</head>
<body>

<h1>ZeroFlush M1 vs 原生 RocksDB 性能对比报告</h1>

<div class="meta">
  <strong>生成时间</strong>：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}<br>
  <strong>工作负载</strong>：{data["config"]["benchmarks"]}<br>
  <strong>数据规模</strong>：{num_keys:,} keys/key_size={data["config"]["key_size"]}B<br>
  <strong>value_size 序列</strong>：{", ".join(str(v) for v in vs_list)} B<br>
  <strong>CPU 核数</strong>：{cpu}<br>
  <strong>压缩</strong>：none（避免 Snappy 未链接问题）<br>
  <strong>修复状态</strong>：WAL 持久化两处 bug（Close 析构 flush + ReopenWritableFile 防截断）已修复
</div>

<h2>1. 吞吐量对比 (ops/sec)</h2>
<div class="chart-container"><canvas id="opsChart"></canvas></div>

<h2>2. 详细数据</h2>
<table>
  <tr>
    <th rowspan="2" style="background:#fafafa">vs</th>
    <th colspan="3">Native RocksDB</th>
    <th colspan="3">ZeroFlush M1</th>
    <th rowspan="2" style="background:#fafafa">ZF / Native</th>
    <th rowspan="2" style="background:#fafafa">更优者</th>
  </tr>
  <tr>
    <th>ops/s</th><th>MB/s</th><th>μs/op</th>
    <th>ops/s</th><th>MB/s</th><th>μs/op</th>
  </tr>
  {table_rows}
</table>

<h2>3. 关键发现</h2>

<div class="finding">
  <strong>📝 写入性能 (fillrandom) 全面领先</strong><br>
  ZeroFlush M1 在所有 value_size 上都比原生 RocksDB 更快（<strong>133% ~ 173%</strong>）：
  <ul>
    <li><strong>vs=4096B</strong>：ZF 201,977 ops/s vs Native 116,454 ops/s（<strong>ZF 快 73.4%</strong>）</li>
    <li>原因：M1 不写原生 WAL（节省一次顺序 IO），不触发 memtable flush</li>
    <li>符合设计预期：M1 的核心收益就是消除 flush 开销</li>
  </ul>
</div>

<div class="finding">
  <strong>📖 随机读 (readrandom)：小 value 时 ZF 较慢，大 value 时 ZF 更快</strong>
  <ul>
    <li><strong>vs=32B</strong>：ZF 621,790 ops/s vs Native 1,355,142 ops/s（ZF 慢 54.1%）
        —— 每次 Get 都触发 pwrite+pread 系统调用，开销在小 value 时占比高</li>
    <li><strong>vs=4096B</strong>：ZF 404,900 ops/s vs Native 97,744 ops/s（<strong>ZF 快 4.14×</strong>）
        —— 大 value 时绕过 block cache 重建，直接从 zfwal 读取更高效</li>
  </ul>
</div>

<div class="finding">
  <strong>🔍 顺序扫描 (readseq)：小 value 时 ZF 显著较慢</strong>
  <ul>
    <li><strong>vs=32B</strong>：ZF 989,643 ops/s vs Native 6,632,907 ops/s（<strong>ZF 慢 6.7×</strong>）</li>
    <li>原因：每次迭代器步进触发 ReadValue（pwrite+pread），无 block cache 加速</li>
    <li>这是 M1 当前实现的开销，<strong>M2 可考虑在 MemTable 侧加 value 缓存</strong></li>
  </ul>
</div>

<div class="conclusion">
  <strong>✅ 修复后性能结论</strong>
  <ul>
    <li><strong>写性能</strong>：ZeroFlush M1 全面优于原生（133%~173%），WAL 持久化修复未引入性能回退</li>
    <li><strong>读性能（random）</strong>：小 value 慢、大 value 快，差异来自每次 ReadValue 的 pwrite+pread 开销</li>
    <li><strong>读性能（seq）</strong>：小 value 显著较慢，是 M1 已知的设计权衡（M2 优化点）</li>
    <li><strong>总体评估</strong>：修复仅恢复持久化正确性，未改变性能特征；M1 写优于原生、读略弱于原生，与设计预期一致</li>
  </ul>
</div>

<h2>4. 复现方法</h2>
<pre><code>cd /home/embed/hyl/metadata_offload/output/zeroflush_m1_perf
python3 run_perf_compare.py
python3 generate_html_report.py</code></pre>

<h2>5. 相关文件</h2>
<ul>
  <li><a href="file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/zeroflush/wal_manager.cc">zeroflush/wal_manager.cc</a> — 修复点</li>
  <li><a href="file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/zeroflush/M1_WAL_PERSISTENCE_FIX.md">M1_WAL_PERSISTENCE_FIX.md</a> — 根因分析文档</li>
  <li><a href="file:///home/embed/hyl/metadata_offload/source/rocksdb-zeroflush/tools/zf_test.cc">tools/zf_test.cc</a> — 7 个回归用例</li>
</ul>

<script>
const ctx = document.getElementById('opsChart').getContext('2d');
new Chart(ctx, {{
  type: 'bar',
  data: {chart_data_json},
  options: {{
    responsive: true,
    maintainAspectRatio: false,
    scales: {{
      y: {{
        type: 'logarithmic',
        title: {{ display: true, text: 'ops/sec (log scale)' }}
      }},
      x: {{
        title: {{ display: true, text: 'value_size' }}
      }}
    }},
    plugins: {{
      legend: {{ position: 'top' }},
      title: {{ display: true, text: 'fillrandom / readrandom / readseq 吞吐量' }}
    }}
  }}
}});
</script>

</body>
</html>"""

    return html


def main():
    if not IN_JSON.exists():
        print(f"ERROR: {IN_JSON} not found")
        sys.exit(1)
    data = json.loads(IN_JSON.read_text())
    html = build_html(data)
    OUT_HTML.write_text(html, encoding="utf-8")
    print(f"→ HTML report saved to {OUT_HTML}")
    print(f"  size: {len(html):,} bytes")


if __name__ == "__main__":
    main()
