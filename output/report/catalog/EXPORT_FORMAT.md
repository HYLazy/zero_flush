# 实验导出包格式 `zexp/1.0` —— 一次实验 = 一个自包含文件

**目标**：每次实验都能导出一个**单文件**，既带**原始数据**，又带**实验环境 / 实验目的 / 结果评估 /
限制 / 出处 / 复现命令 / 数据字典**，可以直接发给别人分析，且**不需要我们的任何脚本**就能打开。

---

## 1. 三层结构（数据在哪里、元数据在哪里、导出物在哪里）

```
①  原始数据：原地保留，不重组
    bench/out/<stage>/<run>/…                    主线：原生 RocksDB 的 L0 调查与瓶颈判定战役
    archive/zeroflush_m{3,4,5}_perf/…     存档：ZeroFlush V0（m3/m4/m5），2026-09-25 归档

②  元数据目录（单一事实来源）：按作用域各一套
    output/report/catalog/        scope=active   33 个数据集（24 个 v1 bench/* + 9 个 v2 perf_v2/*）
    archive/catalog/       scope=archived 78 个数据集（m3/m4/m5）
    experiments.json 环境/目的/结果/评估/限制 + file:LINE 出处；environment.json 共享基线；
    runs.json 逐运行身份；artifacts.json 产物清单；documents.json 文档索引
    build_catalog.py  生成器 + 校验器（schema 与引用双重校验），一次重建两套

③  导出物（一次实验一个文件）：按作用域各自落盘
    output/bundles/<slug>.zexp + index.json             scope=active（33 个，~180 MiB）
    archive/bundles/<slug>.zexp + index.json     scope=archived（78 个，~29 MiB）
```

数据不搬家：导出器**只读**①，用②做元数据，写出③。所以导出可重复、可增量、可批量，
也不会因为导出而改动任何原始证据。

**作用域**：默认只操作 `active`（主线）；m 系列（ZeroFlush V0）在 `archived` 作用域内，
**不进入主线目录与默认导出**。要取用存档：`--scope archived` 或 `--scope all`。

## 2. 单文件包的内部编排（固定路径，收件人按此定位）

```
<slug>.zexp                          zip（deflate），unzip / 7z / python -m zipfile 均可打开
<slug>.metaonly.zexp                 该实验在本机没有数据产物时使用（仅元数据，文件名即警告）
├── EXPERIMENT.json                  ★ 机器可读主清单（唯一权威，见 §3）
├── README.md                        ★ 人读摘要：目的 / 环境 / 结果 / 评估 / 限制 / 怎么用这些数据
├── checksums.sha256                 载荷校验（sha256sum -c 可直接跑）
├── data/                            原始数据，路径 = 仓库内路径原样镜像（避免同名文件冲突）
│   └── <repo 相对路径>/<原文件名>    例：data/bench/out/I/bn_noc_r1/bn_noc_r1.samples.csv
└── derived/                         分析产物（分析表/图）镜像同规则，可选
    └── <repo 相对路径>/<原文件名>
```

为什么 `data/` 下镜像仓库路径：一次实验常跨多个目录（运行目录 + 分析表 + 清单），
镜像路径可避免 `run.log` 之类的重名，收件人也能一眼看出每个文件来自哪里、与 `payload.source` 对上。

## 3. `EXPERIMENT.json` 字段契约

| 字段 | 含义 | 来源 |
|---|---|---|
| `bundle_schema` | 固定 `"zexp/1.0"` | 导出器 |
| `id` / `title_zh` / `engine` / `category` | 实验标识与分类 | 元数据目录 |
| `purpose_zh` | **实验目的**：这个实验回答什么问题 | 元数据目录（带出处） |
| `environment` | **实验环境**：负载参数、RocksDB/ZeroFlush 选项、二进制、其他 | 元数据目录 |
| `environment_baseline` | 共享环境快照（主机/设备/引擎/测量注意），使包**不依赖外部文件**也自洽 | `catalog/environment.json` |
| `results[]` | 结果条目：metric / value / unit / condition（哪条臂） | 元数据目录 |
| `evaluation_zh` | **结果评估**：报告自身的裁决（支持/否证/作废/已修正） | 元数据目录 |
| `caveats_zh[]` | 限制、方差、作废与更正 | 元数据目录 |
| `citations[]` | 每条事实的 `file:LINE` 出处（指向仓库路径） | 元数据目录 |
| `reproduce` | 复现命令（来自运行的 `*.config.txt` argv 与战役脚本调用）与注意事项 | 逐运行产物 + 战役脚本 |
| `provenance` | git HEAD/脏状态、导出时间与导出器版本、host、二进制 md5、逐运行身份（manifest 记录） | `manifest.jsonl` / `config.txt` |
| `data_dictionary[]` | **数据字典**：包内每个文件的格式、行数、列名；已知列给出含义 | 自动扫描 + 内置列义表 |
| `payload[]` | 包内文件清单：bundle 内路径、来源仓库路径、字节、sha256、格式 | 导出时计算 |
| `omitted[]` | **未打入的文件**：来源、字节、原因（`bulk-log` / `over-budget`）、sha256 | 导出时计算 |
| `completeness` | **完整性声明**：`mode`（`full`/`partial`/`metadata-only`）、`has_payload`、`complete`（仅 `full` 为真）、包含/省略计数与判据 | 导出器 |
| `how_to_analyze[]` | 上手提示：先读什么、用什么读、单位与口径陷阱 | 导出器 + 数据集类别 |
| `annotation_gaps[]` | 该实验哪些注解字段在来源里没有（显式登记，不补写） | 元数据目录 |

## 4. 三条硬规则

1. **自包含**：`EXPERIMENT.json` + `README.md` 里不出现"见另一个文件"的外链依赖；
   共享环境以快照形式内联。所有 `citations` 是**出处标注**（便于回溯原文），不是运行依赖。
2. **完整性必须声明**：任何未打入的文件都要出现在 `omitted[]` 里并写明原因与字节数。
   `mode` 三态各有明确含义：`full`=该实验产物全部在包内；`partial`=有省略（逐条登记）；
   `metadata-only`=本机没有该实验的数据产物，包内只有元数据，**文件名带 `.metaonly` 后缀**、
   README 首屏给出警告、并附「数据在哪里」。收件人永远不应误以为"包内即全部"。
3. **不推断**：元数据里来源没写的字段保持 `null`/省略，并在 `annotation_gaps[]` 登记；
   数值一律保持报告原文写法，不做单位换算。

## 5. 精简策略（默认省流量，但可关）

| 规则 | 默认 | 开关 |
|---|---|---|
| 排除体积大的 RocksDB info log（`*.LOG`、`LOG.old.*`、`*.workload` > 1 MiB） | 开 | `--full` 关闭 |
| 总量上限（超过则从最大文件开始省略并全部登记） | 64 MiB | `--max-total-bytes N` / `--full` |
| 排除零长度文件 | 开 | `--full` 关闭 |
| 无数据产物的实验（如协议文档、跨提交记录）导出为 `.metaonly.zexp` | 开 | `--skip-empty` 改为跳过 |

排除项在 `omitted[]` 里有 sha256，收件人可核对"我们讨论的是同一份日志"。

## 6. 用法

```bash
# 看有哪些可导出、各多大
python3 output/report/catalog/export_bundle.py list [--family m5|m3|m4|bench] [--min-bytes N]

# 导出单个实验 / 整族 / 全部
python3 output/report/catalog/export_bundle.py export bench/I#noc
python3 output/report/catalog/export_bundle.py export --family m5 --out /tmp/bundles
python3 output/report/catalog/export_bundle.py export --all --full        # 含大日志
python3 output/report/catalog/export_bundle.py export --all --skip-empty  # 只要带数据的包

# 只看元数据（不打包）
python3 output/report/catalog/export_bundle.py manifest bench/I#noc

# 收件人侧（也可以什么都不装，直接用 unzip）
python3 output/report/catalog/export_bundle.py show  output/bundles/bench-I-noc.zexp
python3 output/report/catalog/export_bundle.py verify output/bundles/*.zexp
python3 output/report/catalog/export_bundle.py extract <bundle> <dir>
```

`output/bundles/index.json` 记录每次导出的 id、字节、sha256、包含/省略统计，便于对账。
当前仓库已导出全部 111 个数据集（active 33 + archived 78）：69 个 `full`、42 个 `partial`，
压缩后合计约 176 MiB（即"把整套实验数据发出去"的传输量级）。

## 6b. 收件人侧最小依赖

只需要 `unzip` 或任何语言的 zip 库。已实测（标准库 `zipfile`+`json`+`hashlib`，不含本项目代码）：
解包 → 读 `EXPERIMENT.json` 拿到环境/目的/结果/评估 → 用 `checksums.sha256` 校验每个文件 →
从 `data/**/<run>.samples.csv` 自行复算（例如 `noc` 包内数据算得 U 率 222.3 MB/s，
与包内 `results` 记的 221.8 MB/s 一致）。

## 6c. 产物来源两种方式（`artifacts.derived_from`）

| 取值 | 含义 | 导出时 |
|---|---|---|
| `directory` | 该数据集有自己的数据目录（如 `bench/out/I/bn_noc_r*`） | 遍历该目录（深度受限） |
| `citations` | 该条目是**某文档的章节**（如 `m4/gov_50gb` 是 `REPORT.md` 的一节），没有独立目录 | **只取它引用到的文件**，不遍历父目录（否则会把整个里程碑树拉进包） |

例：`m4/gov_50gb` 的包内只有 `data/…/zeroflush_m4_perf/REPORT.md` + 元数据，3.8 万字节。

## 7. 选择器（哪些文件属于哪次实验）

多数数据集 = 一个目录（`artifacts.dirs`）。但 `bench/out/I` 一个目录里装了整个瓶颈判定战役
（15 个实验、87 次运行），所以这里有**显式选择器表**（`export_bundle.py` 的 `SELECTORS`）：
按实验 id 指定 glob，例如 `bench/I#noc → bench/out/I/bn_noc_r* + bench/out/analysis/BN_decisive.txt`。
新增实验时，若数据与其它实验混放，在 `SELECTORS` 里加一条即可；否则自动回落到目录。

## 8. 已知取舍

- **容器选 zip 而非 tar.zst**：zip 在任何平台/语言的标准库都能读，且支持"只看清单不解压"；
  代价是压缩率略低于 zstd。实验包常见 0.1–25 MiB，压缩率差异不构成理由。
- **不把 CSV 转成 JSON**：转换会牺牲列式可读性、放大体积、并让 `pandas.read_csv` 失去用武之地；
  收件人拿到的是与分析工具直接兼容的原生文件。
- **不做"每次运行一个包"**：一次实验通常含多条臂/多次重复（例如 `noc` 臂 5 次重复），
  单臂导出会丢掉对照关系；粒度定在**数据集（一组相关运行 + 其分析产物）**。
