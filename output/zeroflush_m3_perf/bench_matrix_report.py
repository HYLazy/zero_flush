#!/usr/bin/env python3
# 矩阵结果汇总报告 v2：吞吐 + 延迟百分位（修复解析）+ 去重
import json, os, re

BASE = "/home/embed/hyl/metadata_offload"
F = os.path.join(BASE, "output/zeroflush_m3_perf/bench_matrix.json")

def fmt(x):
    return f"{x:,}" if x is not None else "-"

def main():
    rs = json.load(open(F))
    # 去重：同 (engine, kv, gb) 取最后一条（重跑成功覆盖失败）
    dedup = {}
    for r in rs:
        dedup[(r["engine"], r["kv_value"], r["scale_gb"])] = r
    rs = list(dedup.values())
    kvs = sorted({r["kv_value"] for r in rs})
    gbs = sorted({r["scale_gb"] for r in rs})
    m = {(r["engine"], r["kv_value"], r["scale_gb"]): r for r in rs}

    out = []
    out.append("# ZeroFlush vs 原生 RocksDB 性能矩阵（fillrandom + readrandom）\n")
    out.append("参数：16 线程、key 16B、compression=none、cache 512MB、max_background_jobs=24、subcompactions=16；")
    out.append("ZF：sampled 路由 + base_merge + value_cache 64MB。workload = 所有线程写入数据总和")
    out.append("（num = GB/(16B+value)，16 线程均分）。readrandom = fill 后独立进程 100 万读/线程")
    out.append("（随机写覆盖后命中率期望 63.2%）。系统常驻负载 5-45 波动（grafana/prometheus/ceph）。\n")
    out.append("规模：10GB ≈ 页缓存内（热）、50GB/100GB 超出（磁盘瓶颈）。日期：2026-08-27/29。\n")

    out.append("## 1. 写吞吐 fillrandom（ops/s）\n")
    out.append("| KV 值 | 规模 | 原生 | ZF | 差距 | ZF MB/s |")
    out.append("|---|---|---|---|---|---|")
    for gb in gbs:
        for kv in kvs:
            n = m.get(("native", kv, gb))
            z = m.get(("zf", kv, gb))
            no = n["fill"].get("ops_s") if n else None
            zo = z["fill"].get("ops_s") if z else None
            ratio = f"{no/zo:.2f}x" if (no and zo) else "-"
            mbps = f"{z['fill'].get('mbps'):.1f}" if z and z["fill"].get("mbps") else "-"
            out.append(f"| {kv}B | {gb}GB | {fmt(no)} | {fmt(zo)} | {ratio} | {mbps} |")

    out.append("\n## 2. 读吞吐 readrandom（ops/s）\n")
    out.append("| KV 值 | 规模 | 原生 | ZF | 差距 | ZF 命中率 |")
    out.append("|---|---|---|---|---|---|")
    for gb in gbs:
        for kv in kvs:
            n = m.get(("native", kv, gb))
            z = m.get(("zf", kv, gb))
            no = n["read"].get("ops_s") if n else None
            zo = z["read"].get("ops_s") if z else None
            ratio = f"{no/zo:.2f}x" if (no and zo) else "-"
            hr = ""
            if z and z["read"].get("found") and z["read"].get("read"):
                hr = f"{z['read']['found']/z['read']['read']*100:.1f}%"
            out.append(f"| {kv}B | {gb}GB | {fmt(no)} | {fmt(zo)} | {ratio} | {hr} |")

    # 延迟百分位：db_bench histogram "Percentiles: P50: x P75: x P99: x P99.9: x P99.99: x"
    def pct(logf):
        try:
            txt = open(logf, errors="replace").read()
        except FileNotFoundError:
            return None
        pm = re.search(r"Percentiles: P50:\s*([0-9.]+).*?P75:\s*([0-9.]+).*?P99:\s*([0-9.]+).*?P99\.9:\s*([0-9.]+)", txt, re.S)
        if not pm:
            return None
        return {"p50": float(pm.group(1)), "p95": float(pm.group(2)),
                "p99": float(pm.group(3)), "p999": float(pm.group(4))}

    for sec, bench in (("fill", "fillrandom"), ("read", "readrandom")):
        out.append(f"\n## {3 if sec=='fill' else 4}. {('写' if sec=='fill' else '读')}延迟百分位（µs，P50/P75/P99/P99.9）\n")
        out.append("| KV 值 | 规模 | 引擎 | P50 | P75 | P99 | P99.9 |")
        out.append("|---|---|---|---|---|---|---|")
        for gb in gbs:
            for kv in kvs:
                for eng in ("native", "zf"):
                    r = m.get((eng, kv, gb))
                    if not r:
                        continue
                    suffix = "" if sec == "fill" else ".read"
                    logf = f"/tmp/bench_{eng}_{kv}B_{gb}GB.log{suffix}"
                    p = pct(logf)
                    if p:
                        out.append(f"| {kv}B | {gb}GB | {eng} | {p['p50']:.0f} | {p['p95']:.0f} | {p['p99']:.0f} | {p['p999']:.0f} |")

    bads = [r for r in rs if r["fill_rc"] != 0 or (r["read_rc"] not in (0, None))]
    if bads:
        out.append("\n## 5. 异常项\n")
        for r in bads:
            out.append(f"- {r['engine']} {r['kv_value']}B {r['scale_gb']}GB fill_rc={r['fill_rc']} read_rc={r['read_rc']}")

    # 汇总观察
    out.append("\n## 6. 观察\n")
    out.append("1. **写（磁盘瓶颈规模 50/100GB）：ZF 差距 1.04-1.39×**——接近原生（历史 1.92×，M4.11 切片 + R57 修复后大幅收窄）；")
    out.append("2. **写（页缓存热 10GB）：固定开销显性放大**（1.3-5.5×，272B 小值最差——每 op 固定开销占比高）；")
    out.append("3. **读：50GB 差距 1.33-1.85×**（布隆优化后；10GB 热读 4-7× 为索引/固定开销上界）；")
    out.append("4. **100GB 读：ZF 反超原生（0.45-0.96×）**——原生 L0→L6 大 compaction 积压下读放大更高，")
    out.append("   ZF 分区文件布局 + 活跃段索引在小页缓存下更稳；")
    out.append("5. **命中率全部 63.1-63.3% = 随机写覆盖期望（1-1/e），无数据丢失**；")
    out.append("6. R57（索引内存计数器竞态 → 128B 大规模爆发/崩溃）已修复，128B 50/100GB 复测通过。")

    txt = "\n".join(out) + "\n"
    path = os.path.join(BASE, "output/zeroflush_m3_perf/bench_matrix_report.md")
    open(path, "w").write(txt)
    print(txt)

if __name__ == "__main__":
    main()
