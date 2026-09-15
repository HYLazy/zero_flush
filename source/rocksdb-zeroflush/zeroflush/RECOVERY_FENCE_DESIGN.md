# V0.6 恢复可见性 fence：实现规格（方案 1）

> 决策（用户 2026-09-15）：**方案 1（VersionEdit 安装覆盖元数据）作为 V0.6 主路径**；
> reconciliation 只用于旧库兼容或诊断，不做正常恢复路径。
> 背景与证据：`output/zeroflush_m5_perf/ledger/REPORT_0B.md` §9/§10。
> 目标门禁：`B3_in_L0=0`、`B3_in_L1plus=0`、`lost_overwrite=0`、`other=0`、
> `active_gen` 无跨 physical part 误匹配、fence 只在真实 AddFile 后推进、空 edit 不推进 fence。

## 0. 身份分离（贯穿全部改动）

```text
WalGenerationId = (physical_wal_part, generation)   // 物理身份，locator/ledger/fence 一律用它
IndexPart       = RoutePartIdWithVersion(key, ver)  // 逻辑身份，只决定"查询哪个索引分区"
```

- `installed_fence` **按 physical_wal_part 记录**；`gen=1 @ part4` 与 `gen=1 @ part10` 不可比（已实测）。
- 新静态库可断言 `physical_part == index_part`；旧库/历史路由重放允许不同，但仍不得跨身份比较。
- locator 已携带 `part_id`（物理）✓ 保持；任何"用 gen 号相等判断同一代"的代码都要改成按
  `(physical_part, gen)` 判定（`InsertCreate` 的同分区约束是第 1 步已落地的部分）。

## 1. 子提交 1：VersionEdit 安装覆盖元数据

### 1.1 新增 tag 与编码

`db/version_edit.h`：`enum Tag`（现止于 `kBlobFileGarbage=401` 一带）新增

```cpp
kZfInstallFence = 500,   // 每项一条记录；由 ZF 安装路径写入
```

`db/version_edit.cc`：`EncodeTo` 尾部（`kInAtomicGroup` 附近）追加；`DecodeFrom` 的
`case kInAtomicGroup:` 同处新增 case。**每条记录字段**（全 varint，便于向前兼容）：

```text
physical_part          varint32
first_generation       varint32   // 连续区间的下界（含）
last_generation        varint32   // 上界（含）；first==last 表示单代
consumed_record_count  varint64   // 该区间内被真实消费的 WAL 记录数
consumed_bytes         varint64   // 该区间内被真实消费的 WAL 字节
install_sequence       varint64   // AddFile 所属安装批次序号（单调）
```

一个 VersionEdit 可含多条（多分区同批安装）。

### 1.2 存储与快照

- `VersionStorageInfo`（`db/version_set.{h,cc}`）新增
  `std::unordered_map<uint32_t, ZfInstallFence> zf_install_fences_;`
  与 `RecordZfInstallFence(...)` / `GetZfInstallFence(part)`。
- `VersionEdit::Apply`：把解出的 fence 项写入目标 `VersionStorageInfo`。
- **快照路径**：`VersionBuilder::SaveTo`（`db/version_builder.{h,cc}`）必须把这些项
  按同格式写回 MANIFEST 快照，否则 MANIFEST 轮换后 fence 丢失（非损坏、但协议失效）。
- 从旧 MANIFEST 读入时无此 tag ⇒ map 为空 ⇒ 触发 §3 的 legacy 判定。

### 1.3 推进规则（**唯一允许的推进点**）

在 ZF 安装路径（`db/flush_job.cc` 组装 `edit_` 处，紧随 `MarkEpochInstalled`）：

```text
仅当该分区本批的全部待物化代都被"完整消费"（无 kSkip、无 conflict、无回收孤儿）
且本次真实 AddFile 成功（同一 VersionEdit 内）时：
    推进 fence[physical_part] = 连续区间上界
空 edit（无 AddFile）      → 不写 tag、不推进、last_materialized 不变、pending 不变
部分 kSkip / conflict 批次 → 只推进"可安装分区"自己的连续区间
```

"完整消费"的判据来自物化任务：该 `(physical_part, gen)` 的 WAL 记录被全部读入并写入
输出（`consumed_record_count` / `consumed_bytes` 由任务累计，调试构建下与 WAL 扫描数断言一致）。

### 1.4 原子性论证

fence 与 `AddFile` 处于**同一个 `VersionEdit`**，由 `VersionSet::LogAndApply` 作为
单条 MANIFEST 记录落盘 ⇒ 崩溃后要么两者都在、要么都不在（复用 RocksDB 既有恢复原子性）。
**禁止**先写 ZFPROPS 再单独更新 fence，也禁止仅凭 WAL 文件存在判断安装状态。

## 2. 子提交 2：恢复消费 fence

```text
加载 VersionSet → 取每 physical_part 的 installed_fence
→ 恢复 WAL 管理器 active_gen（optional；缺失即 nullopt，不用 gen=0）
→ 枚举 zfwal 文件（保持 (part, gen) 物理身份）
→ gen <= fence[part] : 只作为物理保留文件，**不进入读索引**
→ gen >  fence[part] : 进入 recovery/pending 索引
→ 按当前路由得 IndexPart；locator 保留 physical_wal_part
→ 同分区 FIFO 保持（旧代 pending 时新代不得越过安装）
```

要求：
- `EnsureActive` 仅在 `ActiveGenOpt` 有值时调用（第 1 步已落地）。
- `InsertCreate` 保持"同物理分区 + 同活跃代才进 active"（第 1 步已落地）。
- 被 fence 过滤掉的代**不得**登记进 `pending_materialize_bytes`（否则水位虚高），
  它们只保留物理文件（审计/引用），其数据由已安装 SST 承载。

## 3. 子提交 3：legacy 策略

- 检测：恢复时 `zf_install_fences_` 为空且存在 zfwal/SST ⇒ **旧格式**。
- 第一版：`zfo_.require_install_fence=true` 时**拒绝打开**（错误信息指明需迁移或走
  legacy 工具）；默认值在 fence 全绿后翻为 true。
- 后续可选：一次性 reconciliation（必须输出每代被 SST 覆盖的 key/sequence 范围、
  未覆盖代、无法判定的 Merge/Delete/snapshot 情形；**无法证明安全即拒绝打开**）。

## 4. 临时 debug 读保护（保留到回归完成）

索引命中时若存在更高可见 sequence 的已安装版本（L0 与 L1+ 分别取），
debug 模式记录错误并返回更高版本；生产路径在 fence + FIFO 回归通过前不得关闭该保护。
实现前置：本版本 `GetContext` 不在 `db/*.h`（`db/table_cache.h` 仅前向声明），
需先定位其定义与可用 seq 统计，或退化为"索引命中时并行做一次 LSM 读并比较版本"。

## 5. 验收顺序（严格）

```text
A. 新建带 fence 库：写 seed1 → 物化（AddFile+fence）→ 写 seed2 → 再物化
   → 保留旧 WAL（ZF_VC_KEEP_WAL=1）→ 重启 → 逐键必须返回 seed2
B. 旧库兼容路径（reconciliation 或拒绝打开）单独验证
C. 目标模式：禁止 L0 fallback + 空 edit + 保留 WAL + 重启
D. 门禁：B3_in_L0=0  B3_in_L1plus=0  lost_overwrite=0  other=0
        + 断言 active_gen 无跨 physical part 误匹配、空 edit 不推进 fence
```

## 5b. 开工清单（已完成/待完成的锚点盘点）

已确认（本会话 grep 实证）：

| 用途 | 位置 |
|---|---|
| Tag 枚举（新增 `kZfInstallFence = 500`） | `db/version_edit.h:37` |
| EncodeTo 追加点 | `db/version_edit.cc` 的 `kInAtomicGroup` / `kFullHistoryTsLow` 块之后（约 196–215 行） |
| DecodeFrom case 表 | `db/version_edit.cc:548`（case 列表在 835 行 `kInAtomicGroup` 附近） |
| 增量编辑消费入口 | `VersionBuilder::Apply(const VersionEdit*)` → `rep_->Apply`（`db/version_builder.cc`，约 1733 行） |
| 版本状态复制入口 | `VersionBuilder::SaveTo(VersionStorageInfo*)` → `rep_->SaveTo`（`db/version_builder.cc:1738` / `1617`）；另有 `version_set.cc:6063` 的 `builder->SaveTo(versions[i]->storage_info())` 调用点 |

待定位（下次开工前 30 分钟内完成）：

1. `VersionBuilder::Rep` 的类定义与成员名（`Apply`/`SaveTo` 的**主体**在 `db/version_builder.h` 或 cc 内联，
   本次按 `Status VersionBuilder::Rep::Apply` 与 `vstorage_ / base_vstorage_` 搜索均未命中）
   —— 需要先打印其成员，确认"新版本存储"与"基版本存储"的实际字段名。
2. **MANIFEST 快照写出者**（`WriteSnapshot` / `GetVersionEdit` 在 `version_set.{h,cc}` 中未命中，
   命名与预期不同）：必须在快照路径把这些 fence 写回，否则 MANIFEST 轮换后 fence 静默丢失。
   建议从 `VersionSet::LogAndApply` 上游与其 `LogWriter` 使用点反查。
3. `VersionStorageInfo` 是否已有可复用的"自定义元数据"承载点；没有则新增
   `std::unordered_map<uint32_t, VersionEdit::ZfInstallFence>` 与 `Record/Get` 访问器。

执行顺序（每个子提交都必须能独立编译并通过构建门）：

```
1a. version_edit.{h,cc}：tag + 结构 + 编解码（此时无人写、无人读，格式中立、可安全落地）
1b. VersionStorageInfo + VersionBuilder::{Apply,SaveTo} + 快照路径：承载与复制
1c. ZF 物化侧填充：连续完整消费 + 真实 AddFile 时 AddZfInstallFence（空 edit 不写）
2.  恢复消费：gen <= fence 不入读索引、不计 pending；gen > fence 进 recovery
3.  legacy：无 fence 库拒绝打开（require_install_fence 后翻 true）
4.  debug 读保护 + 两组验收（新建带 fence 库重启返回 seed2；禁止 L0 fallback 场景）
```

## 6. 不要做的事（用户明确边界）

- 不先做 generation 编号重排/压缩；编号保持 `(physical_part, gen)` 原样。
- 不把全库 reconciliation 当 V0.6 正常恢复路径。
- 不靠遍历顺序（active/frozen 排序）掩盖错误——归属必须由身份判定保证。
- 不在同一轮混入性能改造（第 7、10 项与指数退避都排在其后）。
