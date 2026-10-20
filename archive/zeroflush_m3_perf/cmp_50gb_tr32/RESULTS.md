# trigger=32 实验（2026-09-03）：50GB 1KB fillrandom 同窗

参数：16 线程、1KB、sampled/16 分区、base_merge、skip_batching=false、value_cache 64MB、
subcompactions=16、compression=none、cache 512MB、bg=24。ZF 侧附加
--level0_file_num_compaction_trigger=32 --level0_slowdown_writes_trigger=100
--level0_stop_writes_trigger=200；native 默认。

## 结果

| 引擎 | fill ops/s | read 命中 | 备注 |
|---|---|---|---|
| ZF trigger=32 | 19,056 | 633,070/1M (63.3%) | lvl1=1281 lvl0=1017, l0busy=689 |
| native 默认 | 26,146 | 632,543/1M | |

对照：ZF 默认 trigger=4 的 50GB 历史基线 27,066 ops/s（≈ native 26,409）；
5GB 下 trigger=32 曾显示 +49%（65.9K vs 44.1K）。

## 结论

trigger=32 的 5GB 优势不迁移到 50GB——小规模下原生频繁小消费的固定开销占比高；
大规模下 L0 延迟到 32 文件才触发的大批量原生 L0→L1 消费产出全范围 L1 文件，
反而加重直装受阻。默认 trigger=4 仍是最优。HEAD（cdcbdac）保持最佳状态：
50GB ZF 27.1K ≈ native 26.4K（持平）；100GB ZF 19.7K vs native 21.7K（-9%，此前 r59_rerun）。

配套实验（同日）：R59X 越界 L0 吸收（插桩验证：吸收与原生消费竞争 21/21 失败；
关闭原生消费后 11/11 成功但 L0 增长无界）→ 净收益为负，已回滚。
