#!/usr/bin/env python3
"""L0 闭包分布实验解析器（2026-09-12）

输入：results/<arm>.LOG（RocksDB LOG，含 "compacted to:" 完成行）、
     results/<arm>.stderr（含 ZFDBG-install / ZFDBG-sched / ZFDBG-frz）。
输出：每个 arm 的事件级统计 + 闭包分布 + 分层字节账目。

用法：python3 parse_closure.py [results_dir]
"""
import re
re_extra = re.compile(r"out\(([\d.]+)")
import sys
import os
from collections import defaultdict

# --- LOG 解析 -------------------------------------------------------------
# [default] compacted to: files[L0 L1 L2 ...] max score 0.97, ..., level 1,
#   files in(6, 1) filtered(0, 0) out(2 +0 blob)
#   MB in(68.7, 3.2 +0.0 blob) filtered(0.0, 0.0) out(30.2 +0.0 blob),
#   read-write-amplify(1.5) write-amplify(0.4) OK, records in: 71390,
#   records dropped: 41391
RE_TS = re.compile(r"^(\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d+)")
RE_FILES = re.compile(r"files\[([\d ]+)\]")
RE_OUT_LEVEL = re.compile(r"level (\d+),")
RE_FILES_IN = re.compile(r"files in\((\d+), (\d+)\)")
RE_MB_IN = re.compile(r"MB in\(([\d.]+), ([\d.]+)")
RE_MB_OUT = re.compile(r"blob\), .*?out\(([\d.]+)")
RE_RWA = re.compile(r"read-write-amplify\(([\d.]+)\)")
RE_WA = re.compile(r"write-amplify\(([\d.]+)\)")
RE_REC_IN = re.compile(r"records in: (\d+)")
RE_REC_DROP = re.compile(r"records dropped: (\d+)")

# ZF stderr
RE_INSTALL = re.compile(
    r"ZFDBG-install lvl=(\d+) file=(\d+) lo=\S+ hi=\S+ bytes=(\d+) part=(\d+) dec=(\d+)")
RE_FRZ = re.compile(r"ZFDBG-frz part=(\d+) gen=(\d+) size=(\d+)")
RE_SCHED = re.compile(
    r"ZFDBG-sched unsched=(\d+) bgc=(\d+) l1score=([\d.]+) l2score=([\d.]+) "
    r"l1n=(\d+) l2n=(\d+) l2bytes=(\d+)")
RE_SKIP = re.compile(r"ZFDBG-skip no_merge=(\d+) busy=(\d+).*sink=(\d+)")


def pct(vals, q):
    if not vals:
        return 0.0
    s = sorted(vals)
    return s[min(int(len(s) * q), len(s) - 1)]


def parse_log(path):
    events = []
    last_levels = None
    if not os.path.exists(path):
        return events, last_levels
    with open(path, errors="replace") as f:
        for line in f:
            if "compacted to:" not in line:
                continue
            m_in = RE_MB_IN.search(line)
            _head, _sep, _tail = line.partition("MB in(")
            m_out = re_extra.search(_tail) if _sep else None
            if not (m_in and m_out):
                continue
            lv = RE_OUT_LEVEL.search(line)
            rwa, wa = RE_RWA.search(line), RE_WA.search(line)
            fi = RE_FILES_IN.search(line)
            files = RE_FILES.search(line)
            ts = RE_TS.match(line)
            rec = RE_REC_IN.search(line)
            drop = RE_REC_DROP.search(line)
            ev = {
                "ts": ts.group(1) if ts else "",
                "out_level": int(lv.group(1)) if lv else -1,
                "in_l0": float(m_in.group(1)),
                "in_l1": float(m_in.group(2)),
                "mb_out": float(m_out.group(1)),
                "rwa": float(rwa.group(1)) if rwa else 0.0,
                "wa": float(wa.group(1)) if wa else 0.0,
                "files_in": (int(fi.group(1)), int(fi.group(2))) if fi else (0, 0),
                "rec_in": int(rec.group(1)) if rec else 0,
                "rec_drop": int(drop.group(1)) if drop else 0,
            }
            events.append(ev)
            if files:
                last_levels = [int(x) for x in files.group(1).split()]
    return events, last_levels


def parse_stderr(path):
    installs = defaultdict(lambda: [0, 0, 0])  # part -> [n, bytes, max]
    frz, sched, skip_last = [], [], None
    total_install_bytes = 0
    n_install = 0
    if not os.path.exists(path):
        return installs, frz, sched, skip_last, 0, 0
    with open(path, errors="replace") as f:
        for raw in f:
            line = raw.replace("\r", "\n")
            for seg in line.split("\n"):
                if "ZFDBG-install" in seg:
                    m = RE_INSTALL.search(seg)
                    if m:
                        lvl, num, by, part = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
                        key = (lvl, part if lvl == 0 else -lvl)
                        installs[key][0] += 1
                        installs[key][1] += by
                        installs[key][2] = max(installs[key][2], by)
                        total_install_bytes += by
                        n_install += 1
                elif "ZFDBG-frz" in seg:
                    m = RE_FRZ.search(seg)
                    if m:
                        frz.append((int(m.group(1)), int(m.group(3))))
                elif "ZFDBG-sched" in seg:
                    m = RE_SCHED.search(seg)
                    if m:
                        sched.append({
                            "bgc": int(m.group(2)), "l1score": float(m.group(3)),
                            "l2score": float(m.group(4)), "l1n": int(m.group(5)),
                            "l2n": int(m.group(6)), "l2bytes": int(m.group(7)),
                        })
                elif "ZFDBG-skip" in seg:
                    m = RE_SKIP.search(seg)
                    if m:
                        skip_last = {"no_merge": int(m.group(1)), "busy": int(m.group(2)),
                                     "sink": int(m.group(3))}
    return installs, frz, sched, skip_last, n_install, total_install_bytes


def report(rdir, arm):
    log = os.path.join(rdir, f"{arm}.LOG")
    err = os.path.join(rdir, f"{arm}.stderr")
    events, levels = parse_log(log)
    installs, frz, sched, skip_last, n_install, install_bytes = parse_stderr(err)

    print(f"\n{'='*72}\nARM {arm}\n{'='*72}")

    # 用户写入量（从 stderr 的 fillrandom 结果行）
    user_bytes = None
    if os.path.exists(err):
        with open(err, errors="replace") as f:
            for line in f:
                if "fillrandom" in line and "MB/s" in line:
                    mm = re.search(r"(\d+) operations;", line)
                    if mm:
                        user_bytes = int(mm.group(1)) * 1024  # 1KB value
    print(f"用户写入: {user_bytes/2**30:.2f} GiB ({user_bytes//(1024*1024)} MB)" if user_bytes else "用户写入: 未解析")

    if not events:
        print("无 compaction 事件")
        return

    # 按输入层分类
    by_type = defaultdict(list)
    for e in events:
        l0m, l1m = e["in_l0"], e["in_l1"]
        if l0m > 0 and l1m > 0:
            by_type[f"L0({l0m:.0f}MB)+L1"].append(e)
        elif l0m > 0:
            by_type["L0 only"].append(e)
        else:
            by_type[f"L1+ (to L{e['out_level']})"].append(e)

    print(f"\n总事件数: {len(events)}")
    tot_in = sum(e["in_l0"] + e["in_l1"] for e in events)
    tot_out = sum(e["mb_out"] for e in events)
    print(f"输入合计: {tot_in/1024:.2f} GiB   输出合计: {tot_out/1024:.2f} GiB")
    if user_bytes:
        print(f"输入/用户 = {tot_in*2**20/user_bytes:.2f}x   输出/用户 = {tot_out*2**20/user_bytes:.2f}x")

    print(f"\n事件类型分布:")
    for t, evs in sorted(by_type.items(), key=lambda kv: -len(kv[1])):
        ins = [e["in_l0"] + e["in_l1"] for e in evs]
        outs = [e["mb_out"] for e in evs]
        print(f"  {t:16s} n={len(evs):5d}  "
              f"输入 p50={pct(ins,.5):7.1f}MB p90={pct(ins,.9):7.1f}MB max={max(ins):7.1f}MB  "
              f"输出 p50={pct(outs,.5):6.1f}MB max={max(outs):6.1f}MB  "
              f"sum_in={sum(ins)/1024:6.2f}GB sum_out={sum(outs)/1024:6.2f}GB")

    print(f"\n闭包分布（L0→L1 事件，MB）：")
    l0_to_l1 = [e for e in events if e["out_level"] == 1 and e["in_l0"] > 0]
    clos = [e["in_l0"] + e["in_l1"] for e in l0_to_l1]
    if clos:
        for q, name in [(0.1, "p10"), (0.5, "p50"), (0.9, "p90"), (0.99, "p99")]:
            print(f"  {name}: {pct(clos,q):8.1f}")
        print(f"  max: {max(clos):8.1f}   mean: {sum(clos)/len(clos):8.1f}")

    # ZF 侧
    print(f"\nZF 安装: n={n_install} 总字节={install_bytes/2**30:.2f} GiB"
          + (f"  (安装/用户={install_bytes/user_bytes:.2f}x)" if user_bytes else ""))
    for k in sorted(installs, key=lambda k: (k[0], k[1])):
        lvl, part = k
        n, b, mx = installs[k]
        print(f"  lvl={lvl} part={part if part>=0 else 'all'}: n={n} "
              f"total={b/2**20:.1f}MB avg={b/n/2**20:.2f}MB max={mx/2**20:.2f}MB"
              if False else "", end="")
    print()
    agg = defaultdict(lambda: [0, 0, 0])
    for (lvl, part), v in installs.items():
        agg[lvl][0] += v[0]; agg[lvl][1] += v[1]; agg[lvl][2] = max(agg[lvl][2], v[2])
    for lvl in sorted(agg):
        n, b, mx = agg[lvl]
        print(f"  lvl={lvl}: n={n} total={b/2**20:.1f}MB avg={b/n/2**20:.2f}MB max={mx/2**20:.2f}MB")

    if frz:
        sizes = [s for _, s in frz]
        print(f"\n封存 epoch: n={len(frz)} size p50={pct(sizes,.5)/2**20:.1f}MB "
              f"p90={pct(sizes,.9)/2**20:.1f}MB max={max(sizes)/2**20:.1f}MB")
    if sched:
        print(f"调度快照 n={len(sched)}: l1n max={max(s['l1n'] for s in sched)} "
              f"l2n max={max(s['l2n'] for s in sched)} "
              f"l1score max={max(s['l1score'] for s in sched):.2f} "
              f"l2bytes max={max(s['l2bytes'] for s in sched)/2**30:.2f}GiB "
              f"bgc max={max(s['bgc'] for s in sched)}")
    if skip_last:
        print(f"skip 汇总（末次）: {skip_last}")
    if levels:
        print(f"终态 files[L0..Ln]: {levels} (L0={levels[0]} L1={levels[1]})")


if __name__ == "__main__":
    rdir = sys.argv[1] if len(sys.argv) > 1 else "results"
    arms = sorted(f[:-4] for f in os.listdir(rdir) if f.endswith(".LOG"))
    if not arms:
        print(f"未找到 {rdir}/*.LOG")
        sys.exit(1)
    for arm in arms:
        report(rdir, arm)
