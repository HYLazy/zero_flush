# 原生 RocksDB 100GB/1KB fillrandom · 8 线程：三类写停顿复测

与 16 线程基线（../native_wa_stall_100gb/）唯一差异：**threads=8**（writes 12,905,536/线程 × 8 = 103,244,288 键，用户数据 100GB 不变）。
其余参数不变：compression=none、cache 512MB、max_background_jobs=24、subcompactions=16、stats_dump 60s。

对比问题：写线程减半（写压力减半、后台 compaction 能力不变）时，三类写停顿
（memtable-limit / L0-file-count-limit / pending-compaction-bytes）如何变化？
