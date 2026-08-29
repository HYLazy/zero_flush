# 原生 RocksDB 100GB/1KB fillrandom：写放大 · 写停顿 · Compaction 规模

配置：key 16B + value 1024B、16 线程、num=103,244,288（用户数据 100GB）、compression=none、
cache 512MB、max_background_jobs=24、subcompactions=16、stats_dump 60s。
结果：**13,978 ops/s（13.9 MB/s），wall 7,386s（123 分钟）**，Cumulative stall 94.7%。

## 1. 各层写放大（cumulative Compaction Stats，最终快照）

W-Amp 列 = 该层 Write(GB) / Rn(GB)（db_bench 原生定义）；「/用户数据」= 该层写出字节 ÷ 100GB 用户写入。
Rn = 下一层（更浅）输入、Rnp1 = 本层输入、Write = 本层输出。

| 层 | 当前文件/大小 | Rn(GB) | Rnp1(GB) | Write(GB) | W-Amp | Write/用户数据 | Comp 次数 |
|---|---|---|---|---|---|---|---|
| L0 | 8 / 0.5GB | 0.0 | 83.9 | 184.8 | 1.80 | 1.85x | 1950 |
| L1 | 26 / 1.1GB | 100.4 | 22.2 | 121.9 | 1.20 | 1.22x | 99 |
| L2 | 66 / 3.4GB | 89.3 | 198.6 | 285.7 | 3.20 | 2.86x | 1702 |
| L3 | 410 / 25.7GB | 84.2 | 303.9 | 371.0 | 4.40 | 3.71x | 1505 |
| L4 | 826 / 47.0GB | 11.9 | 8.2 | 16.9 | 1.40 | 0.17x | 135 |
| **合计（SST 写出）** | 1336 / 77.6GB | 285.8 | 616.8 | 980.3 | - | **9.80x** | 5391 |

- **总写放大 ≈ 9.8×**：写入 100GB 用户数据，SST 侧累计写出 980GB；
- **L2/L3 是写放大主力**（L2 2.86×、L3 3.71×）——dynamic level 下 L3 为数据主层（26GB），L2→L3 与 L1→L2 反复重写；
- L0（flush）1.85×：100GB 用户数据 flush 出 184.8GB SST（覆盖写产生的多版本随 flush 落盘）；
- L4 仅 0.17×（实验结束时深层下沉尚在初期）。

## 2. 三类写停顿（WriteStallCause 计数 + 总时长）

| 停顿类型 | cause | delayed | stopped |
|---|---|---|---|
| **内存（memtable 满）** | memtable-limit | 0 | **2605** |
| **L0 文件数** | L0-file-count-limit | 6 | 0 |
| **L1 以下（pending compaction 字节）** | pending-compaction-bytes | 0 | 0 |
| 合计 | - | 6 | 2605 |

- **停顿总时长 116 分钟 = 运行时间的 94.6%**；
- **内存停顿（memtable stop）绝对主导（2605 次）**——compaction 消费速度低于 flush 速度，
  L0 文件被并行 compaction（24 后台）及时消费（L0 停顿仅 6 次 delay），但 L2/L3 深层重写拖慢整体出清，
  mutable memtable 迟迟无法切换 → 写线程在 memtable 满上全停；
- pending-compaction-bytes 停顿为 0（L1+ 预估 compaction 字节未触发 soft/hard limit——
  dynamic level bytes 下主层 26GB 远低于限制）。

## 3. Compaction 涉及的 SSTable 数量（compaction_started 输入文件，per subcompaction job）

| 输出层 | 类型 | jobs | 输入 SST 累计 | L0 输入 | 更深输入 | 平均 | 中位 | 最大 |
|---|---|---|---|---|---|---|---|---|
| L0 | Intra-L0 | 290 | 1393 | 1393 | 0 | 4.8 | 4 | 10 |
| L1 | L0→L1 | 99 | 1052 | 561 | 491 | 10.63 | 10 | 21 |
| L2 | L1→L2 | 1719 | 5879 | 0 | 5879 | 3.42 | 3 | 10 |
| L3 | L2→L3 | 1513 | 6809 | 0 | 6809 | 4.5 | 4 | 10 |
| L4 | L3→L4 | 141 | 305 | 0 | 305 | 2.16 | 2 | 3 |
| 合计 | - | 3762 | 15438 | 1954 | 13484 | 4.10 | - | - |

- **L0→L1 仅 99 次 job、平均 10.6 个输入 SST**（561 个 L0 + 491 个 L1）——L0→L1 把 ~30 个小 L0 合并成
  大 L1 输入；单次最大 21；
- **深层 compaction（L1→L2/L2→L3）是 job 数主力**（1719 + 1513 次），单次小（平均 3.4-4.5 个 SST）——
  L1/L2 文件大（target_file_size 64MB），每次下沉只涉及少数重叠文件；
- Intra-L0 290 次（平均 4.8 个 L0 文件）——L0 小文件过多时先内部归并。

## 4. 结论

1. **原生 fillrandom 100GB 的写停顿 = memtable 停顿**（94.6% 时间在 stop 等待）：写停顿不是 L0 也不是
   L1+ 字节限制，而是深层 compaction 消费不及导致 memtable 无法出清；
2. **总写放大 9.8×**，其中 L2+L3 贡献 6.6×（68%）；ZF 的物化（无多版本重写）+ L1 直装正是绕开这段的开销；
3. **L0→L1 compaction 单次平均 10.6 个 SST**（vs 深层 2-4.5 个）——L0 小文件合并特性；
4. 13,978 ops/s 中实际写线程活跃仅 ~5%（停顿 94.6%）——compaction 消费能力（24 后台线程）是上限。
