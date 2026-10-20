# output/report/catalog —— 实验数据 JSON 目录

把仓库里的实验数据编成**自描述的 JSON**：每个数据集都带 **实验环境 / 实验目的 / 结果条目 /
结果评估 / 限制**，并且**每一条事实都带 `file:LINE` 出处**，可逐条回溯到原始报告。

重新生成（幂等，会覆盖本目录的 7 个 json）：

```bash
python3 output/report/catalog/build_catalog.py
```

## 文件（按用途分）

| 文件 | 内容 | 体量 |
|---|---|---|
| `index.json` | **总索引**：数据集 id 列表、按里程碑/类别计数、注解覆盖率、校验报告、阅读约定 | 小 |
| `experiments.json` | **注解层**：本目录 33 个数据集条目（24 个 v1 + 9 个 **v2**；环境/目的/结果/评估/限制/出处/产物概览）——主体 | 最大 |
| `environment.json` | 共享环境参考：主机与设备、两套引擎与构建产物、仪器与探针、测量注意（回放方差、常驻负载） | 小 |
| `runs.json` | 逐运行身份索引：bench 8 个 stage 的 `manifest.jsonl`（144 次运行，含二进制 md5 / git HEAD / rc）+ 报告 JSON 里的 `runs[]` | 中 |
| `artifacts.json` | 每个数据集的原始产物清单（本目录 64,511 个文件：路径、格式、字节、**是否入库**） | 大 |
| `documents.json` | 报告/协议/设计/说明类文档索引（404 篇 .md：标题、类别、行数、里程碑、是否入库） | 中 |
| `schema.json` | 字段定义与填写约定（含"未记录填 null、禁止推断""数值保持原文写法"等） | 小 |

`_src/` 是注解层的**分来源中间产物**（7 组，按里程碑/战役与报告来源切分：`bench.json`（v1）、
`perf_v2.json`（v2 新版本）、`m3_*`/`m4`/`m5_*`（存档）），便于局部重跑与人工复核；
`build_catalog.py` 读取它们并做校验与合并。数据集条目可带 `generation` 字段（`v2` / 缺省 `v1`）。

## 单文件导出（发给别人分析）

`export_bundle.py` 把**每次实验**导出成一个自包含文件（数据 + 环境 + 目的 + 结果 + 评估 + 限制 +
复现命令 + 数据字典 + 校验），格式规格见 [EXPORT_FORMAT.md](EXPORT_FORMAT.md)：

```bash
python3 output/report/catalog/export_bundle.py list                     # 哪些实验可导出、各多大
python3 output/report/catalog/export_bundle.py export --all             # 导出全部到 output/bundles/
python3 output/report/catalog/export_bundle.py export --family m5       # 按族导出
python3 output/report/catalog/export_bundle.py verify output/bundles/*.zexp
```

已导出全量 111 个包（本目录 33 + 存档 78；压缩后 ~208 MiB；69 `full` / 42 `partial`），
清单见 `output/bundles/index.json`（active）与 `archive/bundles/index.json`（archived）。
收件人只需 `unzip`。只导主线（含 9 个 v2）：`export_bundle.py export --all --scope active`。

## 字段速览（`experiments.json` 的一个条目）

```json
{
 "id": "bench/I#noc",
 "title_zh": "noc 消融：移除压实产生的全部设备工作",
 "engine": ["native"],
 "category": "diagnosis",
 "purpose_zh": "...本实验回答什么问题...",
 "environment": { "host": null, "cpu": "18 核", "device": "单 NVMe（WD_BLACK SN750）",
                  "workload": "...", "rocksdb_options": "...", "other": "..." },
 "results": [ { "metric": "前台吞吐（到达相位，主指标）",
                "value": "49,049 ± 14,972 → 499,169 ± 20,462，10.2×",
                "unit": "ops/s", "condition": "default → noc",
                "source": "bench/BOTTLENECK_REPORT.md:533" } ],
 "evaluation_zh": "报告自身的裁决……",
 "caveats_zh": ["报告自述的限制……"],
 "sources": ["bench/BOTTLENECK_REPORT.md:52-57", "..."],
 "artifacts": { "dirs": ["bench/out/I"], "n_files": ..., "bytes": ..., "by_format": {...} }
}
```

约定要点：`environment` 只写与 `environment.json` 基线**不同**的部分，未记录则 `null`；
`value` 保持报告原文写法（不换算单位）；`source` 为仓库根相对路径 + 1 基行号，允许 `A-B` 区间与
`A,B-C` 逗号列表。

## 校验（每次生成时自动执行）

`build_catalog.py` 对注解层做两类校验，结果写入 `index.json` 的 `validation`：

1. **schema**：必填键齐全、`category` 取值合法、`engine` 非空、`environment` 键名受限、
   每条 `results` 必须带 `source`、id 不重复；
2. **引用**：每条 `source` 必须解析到**存在的文件**、**行号在文件范围内**、且**该行非空**
   （区间/逗号列表逐段检查）——用于抓 `Read` 显示行号与实际行号偏移一类的引用陷阱。

当前结果（active + archived 合计）：**111 个数据集 / 1,089 条结果 / 2,158 条引用 /
0 schema 问题 / 0 引用问题**；其中 active = 33 个数据集（24 v1 + 9 **v2**）/ 227 条结果 /
702 条引用，archived = 78 个数据集 / 862 条结果 / 1,456 条引用。

## 覆盖范围

111 个数据集的分布：`m3` 21、`m4` 16、`m5` 41、`bench` 24（v1）、**`perf_v2` 9（v2 新版本）**。

**v2（新版本，2026-09-25 起）**：`perf_v2/P0`（工具验收：直方图编码、计数守恒、窗口不变性）、
`perf_v2/M1`–`M5`（纯写到达率扫描、纯点读矩阵、混合读写、L0 几何、线程/扫描/缓存）、
`perf_v2/LAYOUT_BLOOM`（L0-only 布局 × Bloom 代价）、`perf_v2/BACKLOG`（L0 积压：布局 × 大小 ×
三组实验）、`perf_v2/KSWEEP`（固定 B 只扫 K）。口径与前代不同：直方图编码 `lower_bound_v2`（桶下界）、
逐运行 §6 有效性门与工具 sha256、冻结读的页缓存升温逐重复记录——引用 v2 数字前请先读
`bench/out/perf_v2/RESULTS_SUMMARY.md` 的「限制」与 `bench/v2/IMPLEMENTATION_REPORT.md` §15/§16。
覆盖 m3 的对比基准与诊断（写放大/停顿/compaction 耗时归因、L0→L1 结构性瓶颈、profiling）、
m4 的动态治理验收与 M4.9 并发重叠系列、m5 的 sink/ledger/materialize/merge-on-accept/b4b17b3/
l0_closure/bench100g 等，以及 bench 的 L0 调查（E1–E10、到达次序对照）与瓶颈判定战役（含 `noc`
消融、`zipf` 敏感性、深层几何、设备争用与设备上限探针、隔离实验）。

**未纳入**：上游 RocksDB 文档（`source/rocksdb-zeroflush/{README,HISTORY,INSTALL,…}.md`、`docs/`）；
设计文档（`zeroflush/M{3,4,4_DG,5}_DESIGN.md`、`P1B_DESIGN.md`、`RECOVERY_FENCE_DESIGN.md`）——
它们与所述代码同目录，保留在源码树内，仅在 `documents.json` 中登记。
