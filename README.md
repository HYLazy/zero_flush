# metadata_offload / ZeroFlush

存储元数据卸载研究仓库：**ZeroFlush**（基于 RocksDB 9.x 的分区 WAL 存储引擎，
V0 → V2）与**原生 RocksDB L0 写路径调查**（bench 战役 E1–E10、perf_v2 矩阵、
写 CPU / 资源预算战役）。

## 核心思想

原生 RocksDB 的 L0 文件覆盖全 key 范围，L0→L1 compaction 每次重写整个 L1
（O(|L1|)）且并发恒为 1 —— 它是写路径上每字节必经的唯一串行汇流点。ZeroFlush
把这一汇流点拆解为 P 个独立分区：

```
写路径：        KV → 分区 WAL（P 个分片） + Slim 索引（key → locator）
封存（seal）：  活跃字节达阈值 → 冻结分区索引 + 切表
物化：          读 WAL + 排序 + BuildTable → 分区范围文件 → 直装 L1（禁 L0）
读路径：        分区索引命中读 WAL（未物化数据）；否则原生 SST 查找
```

- **V0（已存档于 `archive/`）**：动态治理（split/merge）、范围路由、sink/L2 等
  全机制线，里程碑 M3/M4/M5 实验与结论见 `archive/` 与 `EXPERIMENT_DATA_SUMMARY.md`
- **V2（当前档，2026-10-16 交付）**：V0 的子集与收敛——固定范围分区（无动态
  分区）、保留"取消 memtable→flush、数据直写分区 WAL"、物化后端抽象
  （host CPU / CSD 设备两路，`csd-sim` 仿真与 `csd-plugin` ABI v1）。
  设计与交付说明：[source/rocksdb-zeroflush/zeroflush/V2_DESIGN.md](source/rocksdb-zeroflush/zeroflush/V2_DESIGN.md)

## 目录结构

| 目录 | 内容 |
|---|---|
| `source/rocksdb-zeroflush/` | ZeroFlush 引擎（RocksDB 9.x 源码 + ZF 改动）：`zeroflush/` 核心实现，`tools/zf_test.cc` 回归套件，`tools/zf_valcheck.cc` 验收工具（含 V2 `v2` / `backendeq` 用例） |
| `source/rocksdb/` | 未改动的原生 RocksDB 基线（bench 战役的测量对象） |
| `source/CSD-CoKV/`、`source/gParaKV-GC-*/` | 相关项目（CSD KV / GPU 参数 KV-GC） |
| `bench/` | 原生 RocksDB L0 调查战役：`l0bench` 驱动、E1–E10 冻结协议（`CAMPAIGN.md`）、perf_v2 矩阵（M1–M5/backlog/kscan/layout_bloom，runbook 见 `bench/v2/README.md`）、写 CPU（fillcpu/flushchar）与资源预算（resbudget）战役；`out/` 内为蒸馏层（报告/图表/运行身份），原始测量流在 `output/bundles/*.zexp` |
| `report/` | 《RocksDB 1KB/100GB 写性能分析报告》PDF 及其图表/数据/重建脚本（`report/build_report.py`；字体未入库，见 `.gitignore`） |
| `output/report/catalog/` | 实验数据编目（33 个活跃数据集、702 条带出处引用，全量校验通过）；`output/bundles/` 为 `.zexp` 数据包清单（包本体按约定留本地） |
| `archive/` | V0 线（m3/m4/m5）存档：数据、报告、存档编目（78 数据集/1456 引用）与存档包清单，说明见 `archive/README.md` |
| `exp_design/` | 设计计划（含 CSD 协同设计规划） |
| `EXPERIMENT_DATA_SUMMARY.md` | 全项目实验数据汇总（2026-08-04 → 09-12 V0 线，逐条给出处） |

## 构建与验证

```bash
# ZeroFlush（V0/V2 同一代码树，V2 由选项开启：--zf_v2 --zf_materialize_backend=csd-sim）
cd source/rocksdb-zeroflush && cmake -B build_rel -DCMAKE_BUILD_TYPE=Release && cmake --build build_rel -j
build_rel/zf_valcheck <db> v2 <n> <threads>          # V2 档验收
build_rel/zf_valcheck <db> backendeq <n> <threads>   # host vs CSD 等价验收

# bench 战役（原生 RocksDB 测量）
bash bench/build.sh && bash bench/campaign.sh <stage> ...
```

## 数据约定

- 原始测量流不入库；持久副本是 `.zexp` 数据包（本地 `output/bundles/` 与
  `archive/bundles/`，清单 `index.json` 入库），元数据与逐条出处见两级编目
  （`output/report/catalog/`、`archive/catalog/`）。
- 重演与校验：`python3 output/report/catalog/export_bundle.py verify output/bundles/*.zexp`。
