# zf_v2_evidence —— ZeroFlush V2 50GB 战役证据留档（2026-10-16 → 10-17）

> 收档时间：2026-10-20。来源：`/tmp`（原始路径见 V2_DESIGN.md 的历史叙述与各文件头部）。
> 本目录是 V2_DESIGN.md §5（50GB 验收、R1/R2 规则由来、opt 优化战役、Zipfian 负载）
> 所引用/依托的运行证据；实验 DB 本体（`/tmp/zf_v2_50gb` 34G、`/tmp/zf_v2_def` 30G）
> 为一次性数据，未收档。

## 文件对照

| 组 | 文件 | 说明 |
|---|---|---|
| 主线 50GB | `zf_v2_50gb_run.log` | 最终成功运行的 stdout+诊断（V2_DESIGN §5 引用） |
| | `zf_v2_50gb_db.LOG` | 上述运行 DB info log（原 `/tmp/zf_v2_50gb/LOG`） |
| | `zf_v2_progress.log` | 逐 2 分钟进度 |
| R1 由来 | `zf_v2_50gb_stall18pct.LOG` / `zf_v2_50gb_run_deadlock_stacks.log` / `zf_deadlock_stacks_ledger_vs_threshold.txt` / `zf_v2_stall_bt.txt` | 修复前 18–19% 停滞与 ledger/阈值死锁现场（栈与 DB LOG） |
| | `zf_v2_50gb_deadlock31pct.LOG` / `zf_v2_50gb_run_deadlock31pct.log` | 31% 处死锁运行 |
| | `zf_v2_50gb_l1fix_35pct.LOG` / `zf_v2_50gb_run_l1fix_35pct_slow.log` | R1（L1 独占）修复过程中的 35% 停滞 |
| | `zf_v2_50gb_run_defaultcfg_partial.log` / `zf_v2_def_run.log` | 默认配置（无 R1/R2）对照，停滞/未完成 |
| 崩溃 | `zf_crash_bt.txt` / `zf_v2_opt5_crash_db.LOG` | §5.5 ④ 间歇 SIGSEGV（~1/8，skip 代收养路径，未修）的栈与 DB LOG |
| native 基线 | `native_50gb_baseline.log` | 同窗原生 50GB 对照 |
| opt 战役 | `zf_v2_opt{,2..7}.sh` + `zf_v2_opt*_run/_killed/_crashed/_progress.log` | 10-17 逐档优化（opt7 达 23,194 ops/s：`zf_v2_opt7_result_23194.log`） |
| Zipfian | `zipf_zf.sh` / `zipf_native.sh` / `zipf_{zf,native}_run.log` / `zipf_zf_progress.log` | §5.6：V2 1.65× 写反超 + **关库 86% 键丢失问题（未修，最高优先级）** |
| 回归 | `zf_test_out.txt` / `zf_test_final.txt` | zf_test 套件输出 |

## Zipfian 丢失问题复现目录的说明

原复现目录 `/tmp/zipf_repro2`（含命中问题的 SST）已于 2026-10-20 随 /tmp 清理删除。
问题为确定性复现（2/2，步骤详见 `archive/zeroflush_m5_perf/RUN_MANIFEST.md` D-142…D-144），
用已入库工具可重建：

```bash
# tools/zf_zipf_bench.cc：同 key 流 V2/原生对照、确定性 value、读回精确校验
# skewed(alpha=0.99) 负载 + 反复"打开→小写→关闭"即可触发关闭期物化失败
# （Corruption: ZF merge scan: gen neither registered nor provably installed）
```
