# output/report —— 主线报告与元数据（当前活跃，含 v1 / v2 两代）

本目录只放**当前主线**：同一台机器、同一个未修改的 `librocksdb.so` 之上跑的两代原生 RocksDB 战役。

| 世代 | 数据集 | 时期 | 数据与报告在哪 | 口径要点 |
|---|---|---:|---|---|
| **v2（新版本）** | `perf_v2/*` 共 9 个（P0 工具验收、M1–M5 矩阵、L0-only 布局×Bloom、L0 积压、积压 K 扫描） | 2026-09-25 → 进行中 | 数据 `bench/out/perf_v2/**`；汇总 [../../bench/out/perf_v2/RESULTS_SUMMARY.md](../../bench/out/perf_v2/RESULTS_SUMMARY.md)；实现/缺陷 [../../bench/v2/IMPLEMENTATION_REPORT.md](../../bench/v2/IMPLEMENTATION_REPORT.md) | 直方图编码 `lower_bound_v2`（桶下界）；逐运行 §6 有效性门 + 工具 sha256；冻结读受页缓存升温影响（逐重复轨迹已记录） |
| v1 | `bench/*` 共 24 个（E1–E10 L0 调查 + 瓶颈判定战役） | 2026-09-21 → 2026-09-25 | 数据 `bench/out/{A..I}/**`；报告 `bench/L0_INVESTIGATION_REPORT.md`、`bench/BOTTLENECK_REPORT.md` | 延迟百分位是 **v1 编码的占位值**，引用前须按桶下界重标（勘误见 `bench/out/hist_erratum/`，v2 侧证据见 `perf_v2/P0`） |
| 存档 | `m3/* m4/* m5/*` 共 78 个（ZeroFlush V0） | → 2026-09-25 | `archive/zeroflush_m*_perf/**`、`archive/report/` | 不进入本目录与默认导出（`--scope archived` 才取） |

> **m 系列（m3 / m4 / m5，ZeroFlush V0）已于 2026-09-25 归档**到 `archive/`：
> 数据在 `archive/zeroflush_m*_perf/`，4 篇该线文档在 `archive/report/`，
> 元数据在 `archive/catalog/`，导出包在 `archive/bundles/`。
> 归档说明见 [../../archive/README.md](../../archive/README.md)。归档区**不进入**本目录与默认导出。

**怎么一眼认出新版本**：包名与数据集 id 以 `perf_v2/` 开头、下表「版本」列标 **v2** 的即为新版本结果；
其 `EXPERIMENT.json.generation` 字段为 `"v2"`（v1 为 `"v1"`），包内 README 也应写着 `版本世代：**v2（新版本）**`。

## v2（新版本）结果速览

9 个 v2 数据集的一句话结论（完整证据与限制见各包 `EXPERIMENT.json` 与
[../../bench/out/perf_v2/RESULTS_SUMMARY.md](../../bench/out/perf_v2/RESULTS_SUMMARY.md)）：

| 数据集 | 一句话结论 |
|---|---|
| `perf_v2/P0` | 工具验收通过：直方图编码 `lower_bound_v2` 的桶边界/标签可被独立重算；v1 导出的延迟百分位必须按桶下界重标（默认臂 p99 6553.6 → 2359.3 µs） |
| `perf_v2/M1` | 纯写的膝点在 **0.75–0.90 × 闭环速率**之间，且 60 s 窗会低估它：300 s 窗里 ≥0.75 档后半段吞吐只剩前半段的 0.40–0.71×、pending 仍在上升；`noc` 消融把尾延迟与停顿归因到后台压实 |
| `perf_v2/M2` | 点读在 **16 线程**饱和（32 线程仅 +1% 且尾延迟翻倍）；8 GiB 块缓存相对 512 MiB 快 41%（1 线程）/12–23%（16 线程），低于预注册的 1.5× 阈值 → 记未判定；missing 读对缓存不敏感 |
| `perf_v2/M3` | 混合负载在读占比 ≥50% 时严格线性（achieved=offered）；写重端（25% 读）在满容量处总吞吐仍有 0.93× 时 Put p99 已从 147 µs 涨到 1 442 µs —— **尾延迟先动** |
| `perf_v2/M4` | L0 几何（K∈{1,8,16} × narrow/wide × 冻结/在线）在本工作集下**没有可分辨影响**（1.03–1.13× < 1.10–1.17× 重复离散）；可分辨的只有读哪些键（deep vs recent，命中 p99 7.2 vs 13.3 µs） |
| `perf_v2/M5` | 独立复核 get 的 16 线程膝点；Put 不随线程扩展（52.7–63.0 K ops/s，未判定）；扫描每键成本随长度摊销（1k→1M 键，吞吐降 1646×） |
| `perf_v2/LAYOUT_BLOOM` | 整库在 L0 时，布局把每请求探测从 **1.000 变到 16.000** Bloom checks：982 K vs 291 K ops/s、p99 2.46 vs 6.55 µs；16-bit 把 FPR 从 0.0098 降到 0.0008 而不改探测次数；关 Bloom 的 wide 达 27 924 ns/Get（加载扩展组首轮被判无效，重跑中） |
| `perf_v2/BACKLOG` | 排空 L0 积压的单价 **6.4–11.1 GiB 写入 / GiB 积压**（随 B 增大而降）；drain 14/45/159 s、峰值 43.7/49.4/63.0 GiB（B=0.5/2/8 GiB）；冻结读里布局只改探测次数（4.245 vs 19.244） |
| `perf_v2/KSWEEP` | 进行中（K=1、K=2 各 20/20 有效）：固定 B=2 GiB 只扫 K，分『压实冻结』与『自动压实』两组；同一臂 5 次重复可差 6.7×（页缓存升温），跨 K 对照因共享重复结构仍有效 |

## 本目录有什么

| 路径 | 内容 |
|---|---|
| [catalog/](catalog/) | 主线元数据目录：7 个 JSON（**33 个数据集**＝24 个 v1 + 9 个 v2、227 条结果、702 条带 `file:LINE` 出处的引用）+ 生成器/校验器 + 导出器 + 格式规格 |
| [../bundles/](../bundles/) | **33 个**主线实验的单文件导出包（`<id>.zexp`）+ `index.json`（其中 9 个是 v2 新版本） |

主线数据与报告**不在本目录**，各在自处：

- **v2**：`bench/out/perf_v2/{p0,m1..m5,layout_bloom,l0_backlog,l0_backlog_kscan_K*}` —— 逐运行的
  CSV/JSON/LOG；身份（工具 sha256 / rc / argv / 有效性判定）在逐运行 `config.txt` 与各 stage 的
  `manifest.jsonl`；汇总在 `bench/out/perf_v2/RESULTS_SUMMARY.md`；harness 与缺陷登记在
  `bench/v2/IMPLEMENTATION_REPORT.md`（§15 是「哪个矩阵由哪版工具产生」）
- **v1**：`bench/**` —— L0 调查（E1–E10）与瓶颈判定战役的协议（`CAMPAIGN.md` / `CAMPAIGN_BN.md`）、
  两份报告（`L0_INVESTIGATION_REPORT.md` / `BOTTLENECK_REPORT.md`）、驱动与分析工具；
  `bench/out/**` 逐运行的原始 CSV/JSON/LOG 与各 stage `manifest.jsonl`
- `EXPERIMENT_DATA_SUMMARY.md`（仓库根）—— 跨里程碑实验数据总表（含 m 系列历史数字）

## 单文件实验包（发给他人）

`bundles/` 每次实验一个自包含文件（`<id>.zexp`，zip 容器）：内含 `EXPERIMENT.json`
（机器可读：环境/目的/结果/评估/限制/出处/复现/数据字典/完整性声明）、`README.md`（人读摘要，9 节）、
`data/`（原始数据，路径镜像仓库布局）、`derived/`、`checksums.sha256`。
收件人只需 `unzip`，用标准库即可分析——已实测可由包内 `samples.csv` 复算出报告里的数字。

| 入口 | 说明 |
|---|---|
| [catalog/EXPORT_FORMAT.md](catalog/EXPORT_FORMAT.md) | 格式规格：三层结构、字段契约、三条硬规则（自包含 / 完整性必须声明 / 不推断）、精简策略、选择器 |
| [catalog/export_bundle.py](catalog/export_bundle.py) | 导出器：`list` / `manifest` / `export` / `verify` / `show` / `extract`，支持 `--scope active\|archived\|all` |
| [../bundles/index.json](../bundles/index.json) | 主线导出清单（33 个包，压缩后 179.7 MiB；3 `full` / 30 `partial`，其中 9 个是 v2 新版本） |

```bash
python3 output/report/catalog/export_bundle.py list                    # 主线：哪些实验可导出、各多大
python3 output/report/catalog/export_bundle.py export --all            # 导出全部主线实验
python3 output/report/catalog/export_bundle.py list --scope archived   # 看存档区（m 系列，78 个数据集）
python3 output/report/catalog/export_bundle.py verify output/bundles/*.zexp
```

<!-- BUNDLES-INDEX:BEGIN -->

**33 个主线实验包**（`output/bundles/<id>.zexp`），其中 **9 个是 v2 新版本**（`perf_v2/*`，2026-09-25 起的 runbook 战役：直方图编码 lower_bound_v2、逐运行有效性门、工具版本逐运行记录）。版本列标 `**v2**` 的即为新版本；其余为 v1 早期战役（其延迟百分位是 v1 编码的占位值，引用前须按桶下界重标）。起始时间取各运行自带的 `start_wall`（驱动进程启动时刻）；没有该时间戳的实验（协议文档、单张分析表）留空。

| # | 包（id → 文件） | 版本 | 做了什么实验 | 起始时间 | 运行目录 | 包大小 | 数据完整性 |
|---:|---|---|---|---|---:|---:|---|
| 1 | `perf_v2/BACKLOG` → `perf_v2/BACKLOG.zexp` | **v2** | v2 L0 积压：布局 × 大小 × {冻结读 / 只压实 / 混合}（90 次运行）。把『L0 里堆着多少数据』与『它按什么布局堆着』分开：在约 50 GiB 底库上做同值覆盖写，制造 B∈{0.5, 2, 8} GiB 的 L0 积压、layout∈{narrow, wi… | 2026-09-29 03:39 起（2 个运行目录，至 2026-09-29 18:40） | 2 | 6.2 MiB | 有省略（见包内 omitted） |
| 2 | `perf_v2/KSWEEP` → `perf_v2/KSWEEP.zexp` | **v2** | v2 L0 积压：固定 B=2 GiB 只扫 K（1/2/4/8/16）。把『积压文件数 K』从『积压字节 B』和『布局』里单独分出来：固定 B≈2 GiB，K∈{1,2,4,8,16}，每个 K 分两组——冻结压实（文件选择、Bloom、扫描、元数据代价）与自… | 2026-09-30 13:02 起（1 个运行目录，至 2026-09-30 15:26） | 1 | 4.6 MiB | 有省略（见包内 omitted） |
| 3 | `perf_v2/LAYOUT_BLOOM` → `perf_v2/LAYOUT_BLOOM.zexp` | **v2** | v2 L0-only 布局 × Bloom 代价（narrow/wide × 位数）。当整个工作集就在 L0 里时，文件分组方式（narrow=连续序数块 / wide=i mod K）与 Bloom 位数（0/10/16）如何改变点读的过滤器探测次数与 CPU 成本？用只… | 2026-09-29 11:58 起（2 个运行目录，至 2026-09-30 16:48） | 2 | 4.7 MiB | 有省略（见包内 omitted） |
| 4 | `perf_v2/M1` → `perf_v2/M1.zexp` | **v2** | v2 M1 —— 纯写开环到达率扫描（写路径的膝点与可持续性）。写路径在多少 offered 速率上开始跪？用纯覆盖写（overwrite，shuffle/uniform，16 线程）扫 0.25–1.10 × 闭环速率的开环臂，把『吞吐掉』与『尾延迟… | 2026-09-28 17:57 起（1 个运行目录，至 2026-09-28 20:50） | 1 | 6.5 MiB | 有省略（见包内 omitted） |
| 5 | `perf_v2/M2` → `perf_v2/M2.zexp` | **v2** | v2 M2 —— 纯点读矩阵（线程 × 块缓存 × 请求构成）。点读吞吐在多少线程上饱和？块缓存大小（8 GiB / 512 MiB / 64 MiB）在页缓存已经很热的情况下还能不能改变吞吐与尾延迟？用 84 个臂 × 3 次重复 = 252 次运行… | 2026-09-28 10:56 起（1 个运行目录，至 2026-09-28 20:53） | 1 | 9.0 MiB | 有省略（见包内 omitted） |
| 6 | `perf_v2/M3` → `perf_v2/M3.zexp` | **v2** | v2 M3 —— 混合读写（25/50/75% 读）在开环分数下的线性度与写尾。在 M1 得到写路径膝点之后，把读混进来：固定读比例（25/50/75%）并扫描 offered 速率（0.25–1.00 × 写路径闭环速率），看混合负载在何处、以何种指标先偏离线性。 | 2026-09-28 20:55 起（1 个运行目录，至 2026-09-28 22:02） | 1 | 3.2 MiB | 有省略（见包内 omitted） |
| 7 | `perf_v2/M4` → `perf_v2/M4.zexp` | **v2** | v2 M4 —— L0 几何（文件数 × 区间布局 × 冻结/在线）对读的影响。在固定工作集上，L0 的文件数 K（1/8/16）、区间几何（narrow=连续序数块 / wide=i mod K）与是否在线压实，是否改变点读吞吐与尾延迟？每个几何跑 3 次重复，并分… | 2026-09-29 09:40 起（1 个运行目录，至 2026-09-29 11:57） | 1 | 3.9 MiB | 有省略（见包内 omitted） |
| 8 | `perf_v2/M5` → `perf_v2/M5.zexp` | **v2** | v2 M5 —— 线程 / 扫描长度 / 块缓存（读、写、扫描三组）。在同一个 47 GiB 快照上独立复核三件事：点读随线程的扩展（与 M2 交叉验证）、Put 随线程是否扩展、定长扫描的每键成本如何随扫描长度变化；附带块缓存对照。 | 2026-09-29 00:18 起（1 个运行目录，至 2026-09-29 02:24） | 1 | 3.8 MiB | 有省略（见包内 omitted） |
| 9 | `perf_v2/P0` → `perf_v2/P0.zexp` | **v2** | v2 P0 —— 工具验收（直方图编码 / 计数守恒 / 窗口不变性）。在正式测量之前，用可独立重算的验收用例证明 v2 测量工具测的是 runbook 要求的东西：对数线性直方图的桶边界与标签、计数守恒、每类直方图、开环到达的 cohort 口径、progr… | 2026-09-25 22:16（8 个运行目录） | 8 | 33.3 MiB | 有省略（见包内 omitted） |
| 10 | `bench/CAMPAIGN` → `bench/CAMPAIGN.zexp` | v1 | L0 调查实验协议（原生 RocksDB 11.2）。固定 L0 调查实验的可复现协议：环境、负载、主配置 P、指标定义、运行矩阵，并明确列出这些运行不测量的内容。适用于 E1–E10 的 RocksDB 部分。 | — | — | 0.1 MiB | 全部产物 |
| 11 | `bench/CAMPAIGN_BN` → `bench/CAMPAIGN_BN.zexp` | v1 | 瓶颈判定实验协议（bn_*，继承 CAMPAIGN.md）。固定瓶颈判定实验（bn_*）的协议：只记录相对 bench/CAMPAIGN.md 的差异与新增——驱动新增选项、干预到达过程、容量臂表、决定性干预臂、敏感性臂、隔离实验与新增限制。 | — | — | 0.0 MiB | 全部产物 |
| 12 | `bench/E1` → `bench/E1.zexp` | v1 | E1 独立 L0 物化的写流量分来源账本。独立 L0 物化是否有可观成本？用“来源×层级”互斥归因的写流量账本，把 flush、intra-L0、L0→base 与深层重写分开记账。 | 2026-09-21 18:34 起（15 个运行目录，至 2026-09-21 22:53） † | 15 | 11.9 MiB | 有省略（见包内 omitted） |
| 13 | `bench/E10` → `bench/E10.zexp` | v1 | E10 Flush/L0 安装与 WAL 保留、恢复边界。Flush/L0 安装如何改变日志保留与恢复工作？在 6 个生命周期点位用 kill -9 注入故障，恢复后回读整个键空间校验已确认写入是否丢失。 | 2026-09-21 23:51（6 个运行目录） | 6 | 0.2 MiB | 有省略（见包内 omitted） |
| 14 | `bench/E2` → `bench/E2.zexp` | v1 | E2 到达次序与 L0 文件键区间宽度。到达组批是否产生宽范围重叠并扩大合并工作？用同一键多重集合、同一数据量、同一配置、只改变按键到达次序的匹配对照回答。 | 2026-09-21 18:34 起（23 个运行目录，至 2026-09-21 23:50） † | 23 | 11.5 MiB | 有省略（见包内 omitted） |
| 15 | `bench/E3` → `bench/E3.zexp` | v1 | E3 受控 L0 积压对点读成本的影响。L0 积压如何影响点读？用 8 个受控臂（K∈{1,4,8,16} 个近期层文件 × narrow/wide 区间几何 × hot/cold/online 三种读相）分离“文件数”“区间重… | 2026-09-21 23:40（8 个运行目录） † | 8 | 10.5 MiB | 有省略（见包内 omitted） |
| 16 | `bench/E4` → `bench/E4.zexp` | v1 | E4 键区间重叠是否限制后台有效并行。重叠是否限制后台有效并行？测量压实并发度与同层并发文件对的键区间重叠比例，并扫描 max_subcompactions 与 max_background_jobs 看能否加速排空。 | 2026-09-22 00:58 起（13 个运行目录，至 2026-09-22 02:13） † | 13 | 2.9 MiB | 有省略（见包内 omitted） |
| 17 | `bench/E5` → `bench/E5.zexp` | v1 | E5 L0 与写停顿的因果关系。L0 是写停顿的因果环节，还是伴随现象？用停顿区间的并集时长时间线与 LOG 中每次停顿原因行的逐条归类回答。 | 2026-09-21 18:34 起（15 个运行目录，至 2026-09-21 22:53） † | 15 | 11.9 MiB | 有省略（见包内 omitted） |
| 18 | `bench/E6` → `bench/E6.zexp` | v1 | E6 L0 是否提供有界突发缓冲。L0 是否提供有界缓冲？在 50 GB 已压实布局副本上做开环定速写，600 s 基线速率加中间 60 s 突发（倍率 8×），逐秒记录确认写、L0 文件数、immutable 数、待压实… | 2026-09-22 00:58 起（13 个运行目录，至 2026-09-22 02:13） † | 13 | 2.9 MiB | 有省略（见包内 omitted） |
| 19 | `bench/E7` → `bench/E7.zexp` | v1 | E7 L0 是否把有序访问状态从 DRAM 转到磁盘。L0 是否把有序访问状态从 DRAM 转到磁盘？用 stage B 的受控积压构造相（逐个 flush 出 16 个 L0 文件、期间不压实只读不写）测量 RSS、memtable 与 t… | 2026-09-21 23:40（8 个运行目录） † | 8 | 10.5 MiB | 有省略（见包内 omitted） |
| 20 | `bench/E8` → `bench/E8.zexp` | v1 | E8 触发阈值扫描能否扩大批量并摊销目标层重写。L0 的批处理是否摊销目标层重写与版本淘汰？5 个臂从同一 50 GB 初始布局副本出发跑同一段 12.8 M 次更新轨迹，只改 level0_file_num_compaction_tr… | 2026-09-21 23:54 起（5 个运行目录，至 2026-09-22 00:58） | 5 | 2.2 MiB | 有省略（见包内 omitted） |
| 21 | `bench/E9` → `bench/E9.zexp` | v1 | E9 空间成本的来源（文件数 / 冗余版本 / 暂态输出）。空间成本来自文件数、冗余版本，还是暂态输出？用逐区间目录扫描（区分 .sst/.log/其它、allocated 与 apparent 字节）与 ColumnFamilyMetaData… | 2026-09-21 18:34 起（15 个运行目录，至 2026-09-21 22:53） † | 15 | 11.9 MiB | 有省略（见包内 omitted） |
| 22 | `bench/G#arrival` → `bench/G-arrival.zexp` | v1 | stage G 到达次序对照臂（monotonic / locality / zipf）。用到达形状控制臂（升序、1 M 键窗口内随机、zipf θ=0.99 偏斜）检验“键区间几何”对写放大与重叠的作用，并给出各臂的账本与吞吐。 | 2026-09-21 22:53（4 个运行目录） | 4 | 1.5 MiB | 有省略（见包内 omitted） |
| 23 | `bench/H#intervention` → `bench/H-intervention.zexp` | v1 | stage H 定向干预（提升待压实字节上限 / 降 max_background_jobs）。把“待压实字节上限”这一环拆掉会怎样？在 50 GB 填库上于 t≈62 s 用 SetOptions 把 soft_pending_compaction_bytes_limit 从 64… | 2026-09-22 02:31（3 个运行目录） | 3 | 1.3 MiB | 有省略（见包内 omitted） |
| 24 | `bench/I#comp_scan` → `bench/I-comp_scan.zexp` | v1 | stage I 压实容量扫描（C=2/6/12、SC=1/8/16）与 F×C 矩阵。提高 L0→base / 深层压实的 worker 数与单 job 内并行度（C∈{2,6,12}、SC∈{1,8,16} 及 F×C 矩阵）能否降低积压、改善尾延迟并提高端到端吞吐？ | 2026-09-23 12:18 起（36 个运行目录，至 2026-09-24 21:45） † | 36 | 12.5 MiB | 有省略（见包内 omitted） |
| 25 | `bench/I#devceil` → `bench/I-devceil.zexp` | v1 | stage I 设备随机 IO 服务能力上限探针（devbench）。在无其它负载时直接测量这台设备的随机 IO 服务能力上限（8 GiB 文件、O_DIRECT、4 KiB 随机、16 线程、每次 30 s），为设备争用干预提供基准并解释注入量的绝对上限。 | — | — | 0.0 MiB | 全部产物 |
| 26 | `bench/I#devload` → `bench/I-devload.zexp` | v1 | stage I 设备争用干预（devload2：注入可核验的背景随机写）。注入受控背景设备负载（O_DIRECT 4 KiB 随机写、16 线程，与压实设备流量同量级），让 devbench 周期打印注入速率以核验注入量，检验设备服务容量是否为共享约束。 | 2026-09-25 01:23（3 个运行目录） † | 3 | 1.5 MiB | 有省略（见包内 omitted） |
| 27 | `bench/I#flush_scan` → `bench/I-flush_scan.zexp` | v1 | stage I Flush 容量扫描（F = 1 / 2 / 4）。降低或提高 Flush 线程数（F∈{1,2,4}，C 固定 6）是否使 immutable 队列增长、或改善前台吞吐与尾延迟？并先用单流隔离实验给出 flush 的服务率上限与可证伪预测。 | 2026-09-23 11:33 起（18 个运行目录，至 2026-09-24 22:19） † | 18 | 9.0 MiB | 有省略（见包内 omitted） |
| 28 | `bench/I#geom` → `bench/I-geom.zexp` | v1 | stage I 深层几何干预（mc8g / mult20）。用 max_compaction_bytes=8G（扩大批量）与 max_bytes_for_level_multiplier=20（加宽层级）两种“目标层大小/调度”杠杆，检验深层级联工… | 2026-09-25 00:09（6 个运行目录） † | 6 | 3.1 MiB | 有省略（见包内 omitted） |
| 29 | `bench/I#iso` → `bench/I-iso.zexp` | v1 | stage I 隔离实验（flush 隔离 / L0→base 固定输入重放）。每个部件本身有多贵？分别隔离 flush（单写线程、关闭自动压实、后台只剩 flush）与 L0→base 合并（frozen 副本构造 K 个 L0 文件后无前台写、让自动压实把积压排空… | 2026-09-23 10:41（9 个运行目录） † | 9 | 0.7 MiB | 有省略（见包内 omitted） |
| 30 | `bench/I#ledger` → `bench/I-ledger.zexp` | v1 | stage I 修正 listener 槽位错标后的分阶段工作量账本。谁做了最多工作？先用 base_input_level/output_level 修正 OnCompactionCompleted 的 listener 槽位错标，再按“来源×层级”重算稳… | 2026-09-23 11:33 起（2 个运行目录，至 2026-09-24 22:23） † | 2 | 0.6 MiB | 有省略（见包内 omitted） |
| 31 | `bench/I#noc` → `bench/I-noc.zexp` | v1 | stage I 决定性干预 noc（移除压实工作量）。移除压实产生的全部设备工作（disable_auto_compactions=1，同时解除 L0 限流并放宽 memtable 数），检验端到端吞吐的限制是来自压实工作量还是写路径本身。 | 2026-09-24 22:23（5 个运行目录） † | 5 | 0.4 MiB | 有省略（见包内 omitted） |
| 32 | `bench/I#profile` → `bench/I-profile.zexp` | v1 | stage I 默认配置画像（队列先后与三条时间线）。默认配置下“哪个阶段先出队、队列在哪里积压”？用三条统一时间线（前台完成率/延迟、Flush 队列与 immutable、L0/深层队列与设备）与队列越限时刻确定候选瓶颈。 | 2026-09-23 11:33 † | 1 | 0.5 MiB | 有省略（见包内 omitted） |
| 33 | `bench/I#zipf` → `bench/I-zipf.zexp` | v1 | stage I zipf 敏感性（θ=0.99）。到达几何由均匀改为 zipf 偏斜（θ=0.99）时，设备工作量、flush 条目数与到达相位吞吐如何变化？这是计划 §一 要求的敏感性实验（而非主对照）。 | 2026-09-24 22:30（5 个运行目录） † | 5 | 1.0 MiB | 有省略（见包内 omitted） |

† 该实验与其它实验**共用同一个 stage 目录**（`bench/out/{A,B,C,D}`）：起始时间与运行数是那个 stage 的，不是该实验独有的。stage↔实验是多对多映射（见 `bench/CAMPAIGN.md` §5），逐运行的实验归属由包内 `EXPERIMENT.json.reproduce` 与各 stage 的 `manifest.jsonl` 决定。

<!-- BUNDLES-INDEX:END -->

## JSON 数据目录

`catalog/` 把实验数据编成自描述的 JSON（每个数据集带实验环境 / 实验目的 / 结果条目 / 结果评估 / 限制，
每条事实带 `file:LINE` 出处）：

| 文件 | 内容 |
|---|---|
| [catalog/index.json](catalog/index.json) | 总索引：数据集 id、按类别计数、注解覆盖率、校验报告、阅读约定 |
| [catalog/experiments.json](catalog/experiments.json) | 注解层主体（33 个数据集条目：24 个 v1 + 9 个 v2） |
| [catalog/environment.json](catalog/environment.json) | 共享环境参考：主机 / 设备 / 两套引擎与构建 / 仪器 / 测量注意 |
| [catalog/runs.json](catalog/runs.json) | 逐运行身份索引（bench 144 次运行的 manifest + 报告 JSON 的 runs[]） |
| [catalog/artifacts.json](catalog/artifacts.json) | 每个数据集的原始产物清单（格式 / 字节 / 是否入库） |
| [catalog/documents.json](catalog/documents.json) | 报告 / 协议 / 设计 / 说明类文档索引 |
| [catalog/schema.json](catalog/schema.json) | 字段定义与填写约定 |

再生成（同时重建 active 与 archived 两套目录，含 schema 与引用双重校验）：

```bash
python3 output/report/catalog/build_catalog.py
```
