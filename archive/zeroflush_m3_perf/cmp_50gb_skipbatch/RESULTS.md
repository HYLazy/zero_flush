# skip_batching=true 50GB（2026-09-03）：同窗参数格 1

参数：16 线程、1KB、sampled/16、base_merge、value_cache 64MB、subcompactions=16、
compression=none、cache 512MB、bg=24、默认 trigger=4；skip_batching=true（默认值）。

## 结果

| 配置 | fill ops/s | lvl1/lvl0 | 读命中 | skip 分布 |
|---|---|---|---|---|
| skip_batching=true | 22,012 | 686/1209 (64% L0) | 631,919/1M (63.2%) | busy=487 l0busy=2073 ratio=29 |
| skip_batching=false（历史 27,066） | 27,066 | — | 63.3% | l0busy=689 |

## 结论

证伪：skip_batching=true 比 false 差 19%。skip 数据收养后多代物化输出更大，
撞 L0/base 冲突更频（l0busy 2073、busy 487 vs false 的 689/0）→ 装 L0 反升至 64%。
零丢失。数据完整性全绿。

## 附带：ratio 5GB 快筛（默认 trigger、skip_batching=false）

| ratio | fill ops/s | lvl1/lvl0 |
|---|---|---|
| 0.25（默认） | 44.1K | 47/312 |
| 0.15 | 42.0K | 33/335 |
| 0.10 | 50.2K (+14%) | 37/315 |

5GB 弱正信号但 scale 不可靠（trigger=32 前车之鉴）；且 ratio 不控制装 L0 主路径
（kDirect 安装期回落不经融合判据）→ 不作为优先验证格。
