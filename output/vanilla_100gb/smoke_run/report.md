# 原版 RocksDB 100GB fillrandom 基线实验报告

- 数据集：3.87GiB（4000000 × 1024B），线程 4，RocksDB 11.2.0 (vanilla)
- 吞吐：50943.0 ops/s，50.5 MB/s，78.481 µs/op；P99 32.53 µs
- 墙钟：79.5 s（超时：False）

## 1. 写放大分解
- 用户数据 3.87GB；设备实测总写 3.975×
- WAL 1.01×，Flush 0.978×，Compaction 1.738×（ticker 合计 3.726×）
- 逐层写量 (GB) / 次数 / W-Amp：{'L0': {'write_gb': 3.8, 'read_gb': 0.0, 'cnt': 16, 'wamp': 1.0, 'wr_mb_s': 31.6}, 'L1': {'write_gb': 3.9, 'read_gb': 4.3, 'cnt': 5, 'wamp': 1.2, 'wr_mb_s': 101.3}, 'L2': {'write_gb': 1.9, 'read_gb': 2.1, 'cnt': 15, 'wamp': 2.0, 'wr_mb_s': 14.8}}
- 逐层对写量 (GB)：{'0->1': 0.5, '0,1->1': 3.5, '1,2->2': 2.0}
- flush 次数：16（表 16）

## 2. 写停顿
- 累计停写 0h00m59s，占比 75.5%
- ticker stall.micros = 59193835.0 µs（核对差 -0.0 s）
- 分原因计数：{'cf-l0-file-count-limit-delays-with-ongoing-compaction': 0, 'cf-l0-file-count-limit-stops-with-ongoing-compaction': 0, 'l0-file-count-limit-delays': 0, 'l0-file-count-limit-stops': 0, 'memtable-limit-delays': 0, 'memtable-limit-stops': 20, 'pending-compaction-bytes-delays': 0, 'pending-compaction-bytes-stops': 0, 'total-delays': 0, 'total-stops': 20, 'write-buffer-manager-limit-stops': 0}

## 3. Compaction 位置与次数
- 总次数 36；按输出层 {'L0': 16, 'L1': 5, 'L2': 15}
- 按层对：{'0->1': 1, '0,1->1': 4, '1,2->2': 31}
- 60s 事件分布见 metrics.json `compaction.bins_60s`

## 4. 带宽使用率
- 设备峰值写 None MB/s；运行均值 223.3 MB/s（利用率 None%），峰值 631.3 MB/s；设备忙均值 96.0%
- 30s 组件带宽（compaction/flush/ingest）见 metrics.json `bandwidth.component_series`

## 交叉核对
- {'flush_bytes_vs_l0_write_gb': -0.01, 'compact_bytes_vs_sum_minus_flush_gb': 0.82, 'stall_us_vs_cum_sec': -0.0, 'pair_bytes_vs_ticker_gb': -0.78}

- 生成时间：2026-08-25 19:23:19
