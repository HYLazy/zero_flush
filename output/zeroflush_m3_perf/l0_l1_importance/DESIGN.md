# 论文实验：L0→L1 compaction 影响整体写路径性能的证明

## 机制假设（H1）

原生 RocksDB 的 L0 文件是**全 key 范围**的（每次 memtable flush 覆盖随机写负载的整个
keyspace）。L0→L1 compaction 的输入 = 触发的 L0 文件 + **与其 key 范围重叠的全部 L1
文件**——随机负载下即整个 L1。因此：

> 每次 L0→L1 的重写字节 ≈ O(|L1|)，与触发它的 L0 数据量（~4×flush）无关。

推论（可验证预测）：
- **P1**：L1 越大（max_bytes_for_level_base 越大），L0→L1 单 job 耗时/字节线性增长，
  L0→L1 在全部 compaction 字节/时间中的占比上升，成为写路径主导成分；
- **P2**：L0→L1 变慢后，L0 文件清空不及时 → **L0-file-count-limit 写停顿从 0 变为
  主导之一**（基线实验中恒为 0——之前 16/8/4/2 线程实验中停顿 100% 是 memtable stop）；
- **P3**：L0→L1 因全部 L0 文件互相重叠而**无法并行**（同时只有一个 L0→L1 在跑），
  成为串行瓶颈——写吞吐上限 = L0→L1 排空速率；
- **P4**：总写放大可能反而下降（数据更少层穿越），即"L0→L1 的重要性"不是写放大而是
  **吞吐节流与停顿结构**。

## 参数旋钮

`level_compaction_dynamic_level_bytes=false`（固定层级，隔离旋钮）+
`max_bytes_for_level_base`（L1 容量）：**A=256MB / B=1GB / C=4GB**。
其余与 100GB 系列完全一致（1KB 值、16 线程、subcompactions=16、bg=24、cache 512MB）。

## 度量

1. 各层 Write(GB)（L0→L1 字节占比 = L1 行 Write / 全部行合计）；
2. L0→L1 单 job 耗时分布（平均/P50/P99/max）与累计时间（compaction_finished 事件）；
3. 三类写停顿计数（关注 L0-file-count-limit 是否从 0 出现）；
4. 吞吐（ops/s）；readrandom（L0 文件数与层级深度变化的读放大效应）。

## 与已有实验的关系

16/8/4/2 线程实验（dynamic 默认开）已证明：默认配置下停顿 100% 是 memtable stop、
L0→L1 仅占 wall 21-23%、深层 compaction 是瓶颈。本实验通过放大 L1 使 L0→L1 成为
主导成分，证明**L0→L1 的成本结构（O(|L1|) 重写 + 不可并行）本身即构成写路径的
关键路径**——是 ZeroFlush 分区化物化直装（分区范围文件 + 绕过 L0→L1 全范围归并）
的设计依据。
