#!/usr/bin/env python3
"""Compaction 耗时拆解：I/O vs 计算，以及计算内部构成。

用法:
  python3 bench/analyze_compaction_cost.py <out_prefix> [<perf_dir>]
    1) 用 comp_jobs.csv 的 iostats 字段给出 I/O/计算总账（覆盖全部作业）
    2) 可选：用 perf 的 *.flat.txt（perf report -g none --no-children，不加
       --percent-limit，否则长尾内核地址会被截掉）给出计算内部构成
"""
import csv, glob, re, sys
from collections import defaultdict

CAT = [
 ("输入块读取与解析（BlockFetcher/DataBlockIter/IndexBlockIter/块缓存/文件读取封装）",
  ["BlockFetcher","BlockIter","ParseNextKey","ParseNextIndexKey","NewDataBlockIterator",
   "RandomAccessFileReader","clock_cache","Block_kData","BlockHandle","FilePrefetch",
   "ReadAhead","BlockBasedTable::Get","Uncompress","GetBlock","ReadBlock"]),
 ("归并/迭代框架（CompactionIterator/MergingIterator/表迭代器/Seek-Next 调度）",
  ["CompactionIterator","CompactionMergingIterator","MergingIterator","IteratorBase","LevelIterator",
   "BlockBasedTableIterator","NextAndGetResult","FindBlockForward","Seek","MergeHelper",
   "FindNextUserEntry","ProcessKeyValue","SubcompactionState"]),
 ("输出块构建与写出（BlockBuilder 前缀压缩/EmitBlock/表构建/WritableFileWriter）",
  ["BlockBuilder","WriteMaybeCompressedBlock","EmitBlock","WriteBlock","BlockBasedTableBuilder",
   "CompactionOutputs","UpdateGrandparentBoundaryInfo","WritableFileWriter","TableProperties","Finish"]),
 ("校验和（XXH3/crc32）", ["XXH3","crc32","Checksum"]),
 ("数据搬运（memmove/memcpy）", ["memmove","memcpy"]),
 ("键比较（memcmp/Comparator）", ["memcmp","Comparator"]),
 ("内存分配（malloc/free/new/delete/Arena）", ["malloc","_int_malloc","_int_free","free","operator new","operator delete","Arena"]),
 ("TLS/动态链接/vdso", ["__tls_get_addr","ld-linux","vdso"]),
]
def cat(sym):
    for name, keys in CAT:
        if any(k in sym for k in keys): return name
    if ("ffffffff" in sym) or sym.startswith("0x") or sym == "0000000000000000":
        return "内核帧（系统调用/页缓存拷贝与写回/调度）"
    return "其他用户态（未归类小函数）"

def io_accounting(prefix):
    rows = list(csv.DictReader(open(prefix + ".comp_jobs.csv")))
    g = lambda k: sum(float(r[k]) for r in rows if r.get(k))
    wall, cpu = g("wall_us")/1e6, g("cpu_us_self")/1e6
    rd, wr, fs = g("io_read_ns")/1e9, g("io_write_ns")/1e9, g("io_fsync_ns")/1e9
    cr, cw = g("io_cpu_read_ns")/1e9, g("io_cpu_write_ns")/1e9
    gin = g("in_bytes")/1e9
    print("== %s：%d 个 compaction 作业（fill+drain），输入 %.0f GB ==" % (prefix, len(rows), gin))
    print("墙钟 %.0f s = 线程 CPU %.0f s（%.1f%%）+ 非 CPU 等待 %.0f s（%.1f%%）" % (
        wall, cpu, 100*cpu/wall, wall-cpu, 100*(wall-cpu)/wall))
    print("  CPU 内：read/write 系统调用中 %.0f s（占 CPU %.0f%%）→ 纯逻辑计算 %.0f s" % (
        cr+cw, 100*(cr+cw)/cpu, cpu-cr-cw))
    print("  等待侧系统调用墙钟：fsync %.0f s（%.1f%% 墙钟）read %.0f s（%.1f%%）write %.0f s（%.1f%%）" % (
        fs, 100*fs/wall, rd, 100*rd/wall, wr, 100*wr/wall))
    print("  强度：墙钟 %.2f s/GB输入，CPU %.2f s/GB，纯计算 %.2f s/GB，syscall-CPU %.2f s/GB，等待 %.2f s/GB" % (
        wall/gin, cpu/gin, (cpu-cr-cw)/gin, (cr+cw)/gin, (wall-cpu)/gin))
    return wall, cpu, cr+cw

def perf_breakdown(perf_dir, user_cpu_s):
    files = sorted(glob.glob(perf_dir.rstrip("/") + "/*.flat.txt")) or sorted(glob.glob(perf_dir.rstrip("/") + "/*.txt"))
    tot, N = defaultdict(float), 0.0
    for f in files:
        n = 0.0
        for line in open(f):
            m = re.match(r"#\s*Samples:\s*([0-9.]+)\s*([KM]?)", line)
            if m:
                n = float(m.group(1)) * (1e3 if m.group(2) == "K" else 1e6); break
        N += n
        for line in open(f):
            mm = re.match(r"\s+([0-9.]+)%\s+\S+\s+\S+\s+\[.\] (.*)", line)
            if mm: tot[cat(mm.group(2).strip())] += float(mm.group(1)) * n / 100.0
    if N == 0:
        print("(未找到 perf 报告)"); return
    user = N - tot["内核帧（系统调用/页缓存拷贝与写回/调度）"]
    print("\n== 计算内部构成（perf，%d 样本）==" % N)
    print("%-56s %10s %11s %12s" % ("类别", "占全部样本", "占用户态", "折算秒数"))
    for k, v in sorted(tot.items(), key=lambda kv: -kv[1]):
        if k.startswith("内核"):
            print("%-56s %9.2f%% %10s %11s" % (k, 100*v/N, "—", "—"))
        else:
            print("%-56s %9.2f%% %9.2f%% %10.0f s" % (
                k, 100*v/N, 100*v/user, user_cpu_s*v/user))

if __name__ == "__main__":
    p = sys.argv[1]
    w, c, syscpu = io_accounting(p)
    if len(sys.argv) > 2:
        perf_breakdown(sys.argv[2], c - syscpu)
