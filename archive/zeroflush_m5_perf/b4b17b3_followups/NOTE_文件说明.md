# 本目录文件说明（避免误读取证）

- `readpartial50.bt_all.txt` / `readpartial100.bt_all.txt`、`readpartial50|100.hang.LOG`、
  `read_partial_50|100.log`、`readpartial50|100.watch.log`
  → **误判产物，不可当作"读停滞"证据。** 这两相被看门狗按"DB 体积 + LOG 不变"判据终止，
  但该判据对读负载无效（读不改变 DB 体积、LOG 也几乎不写）。线程栈显示
  16 个读线程当时正在 `DBImpl::GetImpl` / `InlineSkipList` / `Version::Get` / `TableCache::Get`
  里正常工作，并非等待。
- `reads50_retest.log` / `reads100_retest.log`（及对应 `.watch.log`）
  → **修正后的正式读数**。改用进程 CPU 时间（`/proc/PID/stat` utime+stime）作进展信号，
  两相均 rc=0 正常完成。判据见 `read_retest.sh`。
- `fill_smoke.log` → 停滞点以下的 ZF 写 smoke（1GB，非对照口径），完整跑通。
- `fill_repeat.log` / `repeat.LOG` → 50GB 口径复跑，同样停滞（停于 2.41GB，账本 pending=1110.7MB）。
- `run.log` → 各相起止与退出码（`*_watch=91` 表示被看门狗判停滞后终止；`=0` 表示正常完成）。

完整报告见 `../B4B17B3_50GB_100GB_REPORT.md`。
