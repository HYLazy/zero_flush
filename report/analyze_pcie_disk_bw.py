#!/usr/bin/env python3
"""100GB 写入流程：PCIe 带宽使用率 + 硬盘带宽使用率。

输入
  * /tmp/pcie_probe/dev100.samples.csv   fillcpu（本次已加设备采样）逐 250ms 行；
     设备列来自 /proc/diskstats（nvme0n1）：sectors(512B) / IOs / io_ticks(ms)
  * /tmp/pcie_probe/iio_during_run.txt   perf stat -a 的 uncore_iio_free_running 逐 5s 间隔
  * 链路能力从 sysfs 读：PCIe 3.0 x4（8.0 GT/s × 4 lane，128b/130b 编码）
  * 天花板探针：本会话实测（顺序 O_DIRECT 写 910 MB/s、顺序读 1.7 GB/s、
     随机 4K 写 57 MB/s、随机 4K 读 2423 MB/s、随机 50/50 114 MB/s）

口径
  * 硬盘带宽使用率 = 实测设备带宽 ÷ 同会话实测天花板（分母随访问形态给出多档）
  * PCIe 带宽使用率 = 过链路的负载字节 ÷ 链路单向有效容量；
     负载字节取块层实测（NVMe 的 payload 之外只有命令/完成 TLP，大块 I/O 下 ≈1–2%）
"""
import csv
import re
import statistics as st
import sys

P = "/tmp/pcie_probe"
LINK_GT_S = 8.0 * 4                     # PCIe 3.0 x4 = 32 GT/s
LINK_GB_S = LINK_GT_S * 128 / 130 / 8    # 128b/130b 后的有效载荷上限

CEIL = {  # 本会话实测（O_DIRECT，16 线程，8 GiB 文件）
    "顺序写（1M 块）": 910.0,
    "顺序读（1M 块）": 1700.0,
    "随机 4K 写": 57.0,
    "随机 4K 读": 2423.0,
    "随机 4K 50/50": 114.0,
}


def load_samples(path):
    rows = list(csv.DictReader(open(path)))
    S = lambda r, k: int(r[k])
    out = []
    for a, b in zip(rows, rows[1:]):
        dt = float(b["elapsed"]) - float(a["elapsed"])
        if dt <= 0:
            continue
        w = (S(b, "dev_wr_sectors") - S(a, "dev_wr_sectors")) * 512 / 1e6 / dt
        r = (S(b, "dev_rd_sectors") - S(a, "dev_rd_sectors")) * 512 / 1e6 / dt
        t = (S(b, "dev_io_ticks_ms") - S(a, "dev_io_ticks_ms")) / dt / 10.0   # %
        app = (S(b, "ops_cum") - S(a, "ops_cum")) * 1040 / 1e6 / dt
        out.append(dict(ts=float(b["ts"]), el=float(b["elapsed"]), phase=int(b["phase"]),
                        wr=w, rd=r, busy=t, app=app,
                        stall=(float(b["stall_s"]) - float(a["stall_s"])) / dt * 100))
    return rows, out


def summarize(win, label):
    if not win:
        print(f"  {label}: 无样本")
        return None
    f = lambda k: [x[k] for x in win]
    m = lambda k: st.mean(f(k))
    med = lambda k: st.median(f(k))
    q = lambda k, p: sorted(f(k))[int(len(win) * p)]
    tot = sum(x["wr"] + x["rd"] for x in win)
    print(f"  {label}（{len(win)} 窗，{win[-1]['el']-win[0]['el']:.0f}s）")
    print(f"    设备写  均值 {m('wr'):6.0f}  中位 {med('wr'):6.0f}  p95 {q('wr',0.95):6.0f}  max {max(f('wr')):6.0f} MB/s")
    print(f"    设备读  均值 {m('rd'):6.0f}  中位 {med('rd'):6.0f}  p95 {q('rd',0.95):6.0f}  max {max(f('rd')):6.0f} MB/s")
    print(f"    设备合计均值 {m('wr')+m('rd'):6.0f} MB/s；设备忙率 均值 {m('busy'):5.1f}%  中位 {med('busy'):5.1f}%")
    print(f"    应用带宽均值 {m('app'):6.0f} MB/s；写线程 stall 占比均值 {m('stall'):5.1f}%")
    return dict(wr=m("wr"), rd=m("rd"), tot=m("wr") + m("rd"), busy=m("busy"),
                app=m("app"), stall=m("stall"), totbytes=tot * (win[-1]["el"] - win[0]["el"]))


def iio_series(path):
    ser = {}
    for line in open(path):
        m = re.match(r"\s*([0-9.]+)\s+([0-9.]+) MiB\s+\S*?bw_(in|out)_port(\d)/", line)
        if m:
            ser.setdefault(f"{m.group(3)}_{m.group(4)}", []).append((float(m.group(1)), float(m.group(2))))
    return ser


def main():
    rows, win = load_samples(f"{P}/dev100.samples.csv")
    fill = [x for x in win if x["phase"] == 1]
    drain = [x for x in win if x["phase"] == 2]
    print("=" * 78)
    print("100GB 写入流程：设备带宽与忙率（fillcpu 默认配置，96M×1KB，16 线程）")
    print("=" * 78)
    s_fill = summarize(fill, "fill 阶段")
    s_drain = summarize(drain, "drain 阶段")
    s_all = summarize(win, "全程")

    # 累计设备字节
    tot_wr = (int(rows[-1]["dev_wr_sectors"]) - int(rows[0]["dev_wr_sectors"])) * 512 / 1e9
    tot_rd = (int(rows[-1]["dev_rd_sectors"]) - int(rows[0]["dev_rd_sectors"])) * 512 / 1e9
    app_gb = 99.84
    print(f"\n  累计设备写 {tot_wr:.1f} GB、读 {tot_rd:.1f} GB（应用 {app_gb} GB）"
          f" → 设备写放大 {tot_wr/app_gb:.2f}×")
    print(f"  设备忙率（全程，io_ticks 口径）: {s_all['busy']:.1f}%")

    # ---- PCIe ----
    print("\n" + "=" * 78)
    print("PCIe 带宽使用率")
    print("=" * 78)
    print(f"  链路协商：PCIe 3.0 x4（8.0 GT/s × 4）= {LINK_GT_S:.0f} GT/s")
    print(f"  单向有效载荷上限（扣 128b/130b）= {LINK_GB_S:.3f} GB/s = {LINK_GB_S*1000:.0f} MB/s")
    for lbl, s in (("fill", s_fill), ("drain", s_drain), ("全程", s_all)):
        pw = s["wr"] / (LINK_GB_S * 1000) * 100
        pr = s["rd"] / (LINK_GB_S * 1000) * 100
        print(f"  {lbl:>4s}: 写向 {s['wr']:6.0f} MB/s = {pw:5.2f}% 链路   "
              f"读向 {s['rd']:6.0f} MB/s = {pr:5.2f}%   "
              f"载荷合计 {s['tot']/(LINK_GB_S*1000)*100:5.2f}%")

    # IIO 计数器：经隔离实验归因确认 port1 = 该 NVMe（走 PCH/DMI）
    #   写 6GiB O_DIRECT → bw_out_port1 897–909 MiB/s（dd 944 MB/s）
    #   读 6GiB O_DIRECT → bw_in_port1 1703–1731 MiB/s（dd 1.8 GB/s）
    ser = iio_series(f"{P}/iio_during_run.txt")
    if ser:
        print("\n  CPU 侧 uncore_iio port1（正身即该盘的 PCIe 通道，隔离实验已归因）:")
        o = [x for x in ser.get("out_1", []) if x[0] >= 0]
        i = [x for x in ser.get("in_1", []) if x[0] >= 0]
        span = o[-1][0] - o[0][0] + 5.0
        gb = lambda v: v * 1.048576 / 1024          # MiB -> GiB
        out_gib, in_gib = gb(sum(x[1] for x in o)), gb(sum(x[1] for x in i))
        print(f"    写向 out_port1: 累计 {out_gib:8.1f} GiB / {span:.0f}s = {out_gib*1.0737e3/span:6.0f} MB/s"
              f"  → {out_gib*1.0737e3/span/(LINK_GB_S*1000)*100:5.2f}% 链路")
        print(f"    读向 in_port1 : 累计 {in_gib:8.1f} GiB / {span:.0f}s = {in_gib*1.0737e3/span:6.0f} MB/s"
              f"  → {in_gib*1.0737e3/span/(LINK_GB_S*1000)*100:5.2f}% 链路")
        print(f"    双向合计 {out_gib+in_gib:8.1f} GiB = {((out_gib+in_gib)*1.0737e3/span)/1000:.2f} GB/s"
              f"，占链路 {((out_gib+in_gib)*1.0737e3/span)/(LINK_GB_S*1000)*100:.2f}%")
        dev_wr_gib, dev_rd_gib = tot_wr * 1e9 / 2**30, tot_rd * 1e9 / 2**30
        print(f"    交叉校验（独立仪器：块层）: 写 {out_gib/dev_wr_gib:.3f}×、读 {in_gib/dev_rd_gib:.3f}×"
              f"（>1 的部分 = NVMe 命令/完成 TLP 与链路帧开销）")

    # ---- 硬盘使用率 ----
    print("\n" + "=" * 78)
    print("硬盘带宽使用率（分母 = 本会话同盘实测天花板）")
    print("=" * 78)
    print(f"  天花板：顺序写 {CEIL['顺序写（1M 块）']:.0f} MB/s | 顺序读 {CEIL['顺序读（1M 块）']:.0f} MB/s | "
          f"随机4K写 {CEIL['随机 4K 写']:.0f} | 随机4K读 {CEIL['随机 4K 读']:.0f} | 随机50/50 {CEIL['随机 4K 50/50']:.0f}")
    for lbl, s in (("fill", s_fill), ("全程", s_all)):
        print(f"\n  {lbl}: 设备写 {s['wr']:.0f} MB/s，读 {s['rd']:.0f} MB/s")
        print(f"    对顺序写天花板 : {s['wr']/CEIL['顺序写（1M 块）']*100:5.1f}%")
        print(f"    对顺序读天花板 : {s['rd']/CEIL['顺序读（1M 块）']*100:5.1f}%")
        print(f"    对随机4K写天花板: {s['wr']/CEIL['随机 4K 写']*100:5.1f}%")
    print(f"\n  说明：RocksDB 写的是 64MB 级顺序 SST，对应天花板是顺序写那一档；")
    print(f"       随机 4K 那一档只在小块随机访问时才适用，不是本负载的对照。")


if __name__ == "__main__":
    main()
