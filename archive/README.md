# archive/ —— m 系列（ZeroFlush V0）存档区（与 `output/` 同级）

**归档时间**：2026-09-25　**归档人**：实验编目流程
**归档原因**：m3 / m4 / m5 是 ZeroFlush V0 这条线（分区物化 + 直装 + 治理）的实验；
当前主线已转到 `bench/`（原生 RocksDB 的 L0 调查 E1–E10 与瓶颈判定战役）。这些实验
**以后基本不再使用**，但作为历史证据保留，故集中存放并且**不进入主线目录与默认导出**。

## 存了什么

| 路径 | 内容 | 体量 |
|---|---|---|
| `zeroflush_m3_perf/` | M3 里程碑：对比基准、诊断（写放大/停顿/compaction 耗时归因、L0→L1 结构性瓶颈）、profiling、矩阵 | 12 MB |
| `zeroflush_m4_perf/` | M4 里程碑：动态治理验收、M4.9 并发重叠系列（REPORT.md + logs/） | 2.6 MB |
| `zeroflush_m5_perf/` | M5 里程碑：sink/ledger/materialize/merge-on-accept/b4b17b3/l0_closure/bench100g 等 | 197 MB |
| `report/{benchmark,progress,study}/` | 4 篇该线的文档（基准分析报告、开发进展、V0 问题研究、V0 路线图） | 120 KB |
| `catalog/` | **存档作用域**的元数据目录（7 个 JSON：78 个数据集、862 条结果、1456 条带出处引用） | 3.6 MB |
| `bundles/` | 78 个单文件实验包（`<id>.zexp` / `.metaonly.zexp`）+ `index.json` | 19 MB |

## 怎么用（需要时）

```bash
# 看存档里有什么、各多大
python3 output/report/catalog/export_bundle.py list --scope archived

# 按 id 导出单个实验包（自动落到本目录的 bundles/）
python3 output/report/catalog/export_bundle.py export m5/merge_on_accept_v2#report --scope archived

# 批量重导 / 校验
python3 output/report/catalog/export_bundle.py export --all --scope archived
python3 output/report/catalog/export_bundle.py verify archive/bundles/*.zexp

# 重新生成存档元数据（与主线共用同一生成器与校验器）
python3 output/report/catalog/build_catalog.py     # 同时重建 active 与 archived 两套目录
```

## 两条需要知道的约定

1. **存档目录内的脚本/日志里的路径是归档前的原样**（形如 `output/zeroflush_m5_perf/...`），
   指的是这些数据**归档前**的位置；对应关系是加一层 `archive/`：

   | 归档前 | 现在 |
   |---|---|
   | `output/zeroflush_m5_perf/…` | `archive/zeroflush_m5_perf/…` |
   | `output/zeroflush_m3_perf/…` | `archive/zeroflush_m3_perf/…` |
   | `output/report/{benchmark,progress,study}/…` | `archive/report/{benchmark,progress,study}/…` |

   日志（`*.log`/`*.LOG`）保持原样是刻意的——它们是当时过程的历史记录，不做改写。
   若要让某个存档脚本重新可跑，用：

   ```bash
   sed -i 's#output/zeroflush_m#archive/zeroflush_m#g' <脚本>
   ```

   **存档区之外**的所有引用（元数据目录的 1456 条出处、4 篇文档、`EXPERIMENT_DATA_SUMMARY.md`、
   源码树里的 5 个文件）**已全部改写到新路径**，并经校验器确认逐条可解析。

2. **解档**（若这条线要重启）：把三个数据目录与 `report/` 下的三份移回 `output/` 原位（`git mv`），
   然后跑一次 `build_catalog.py`——它同时重建两套目录；再把 `SCOPES` 的划分（`build_catalog.py`
   顶部）按需要调整即可。移动一律用 `git mv` 以保留历史。
