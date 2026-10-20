#!/usr/bin/env python3
"""Decompose fdatasync latency on this exact machine (ext4, internal journal,
data=ordered, delalloc, NVMe with volatile write cache).

Probes:
  clean     : fdatasync on a file with zero dirty pages  -> syscall/VFS floor
  fresh-N   : buffered-write N MiB to a NEW file, then fdatasync (the
              RocksDB-SST pattern: delayed allocation + extent setup +
              journal commit + writeback + device FLUSH)
  prealloc-N: same but into an fallocate()d region        -> no delalloc/alloc
  total duration ~1 min, peak disk usage ~200 MiB.
"""
import os, time, statistics as st, subprocess, sys

BASE = "/tmp/fsprobe"
os.makedirs(BASE, exist_ok=True)
MB = 1 << 20


def med_ms(v):
    return st.median(v) * 1e3


def probe_clean(n=200):
    p = f"{BASE}/clean.bin"
    fd = os.open(p, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
    os.write(fd, b"x" * MB)
    os.fdatasync(fd)
    lat = []
    for _ in range(n):
        t0 = time.perf_counter()
        os.fdatasync(fd)  # nothing dirty since last sync
        lat.append(time.perf_counter() - t0)
    os.close(fd); os.unlink(p)
    return med_ms(lat)


def probe_dirty(mib, n=15, prealloc=False):
    p = f"{BASE}/d{'p' if prealloc else 'f'}{mib}.bin"
    lat = []
    for i in range(n):
        fd = os.open(p, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
        if prealloc:
            os.posix_fallocate(fd, 0, mib * MB)
        os.write(fd, bytes([65 + (i % 26)]) * (mib * MB))
        t0 = time.perf_counter()
        os.fdatasync(fd)
        lat.append(time.perf_counter() - t0)
        os.close(fd); os.unlink(p)
    return med_ms(lat)


def worker(path, mib, iters, out=None):
    lat = []
    for i in range(iters):
        fd = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
        os.write(fd, bytes([97 + (i % 26)]) * (mib * MB))
        t0 = time.perf_counter()
        os.fdatasync(fd)
        lat.append(time.perf_counter() - t0)
        os.close(fd)
    return lat


def probe_concurrent(mib=32, iters=12):
    import multiprocessing as mp
    # single stream reference
    single = med_ms(worker(f"{BASE}/c0.bin", mib, iters))
    os.unlink(f"{BASE}/c0.bin")
    # two concurrent streams
    with mp.Pool(2) as pool:
        r1 = pool.apply_async(worker, (f"{BASE}/c1.bin", mib, iters))
        r2 = pool.apply_async(worker, (f"{BASE}/c2.bin", mib, iters))
        l1, l2 = r1.get(), r2.get()
        pool.close(); pool.join()
    os.unlink(f"{BASE}/c1.bin"); os.unlink(f"{BASE}/c2.bin")
    return single, med_ms(l1), med_ms(l2), max(max(l1), max(l2)) * 1e3


if __name__ == "__main__":
    print(f"clean-file fdatasync (no dirty pages)      : {probe_clean():8.2f} ms")
    for mib in (1, 4, 16, 64):
        print(f"fresh file  write {mib:3d} MiB + fdatasync   : {probe_dirty(mib):8.2f} ms"
              f"  ({mib/ (probe_dirty(mib)/1e3) /1024:5.2f} GiB/s effective)")
    for mib in (16, 64):
        print(f"preallocated write {mib:3d} MiB + fdatasync : {probe_dirty(mib, prealloc=True):8.2f} ms")
    s, a, b, w = probe_concurrent()
    print(f"single stream  32 MiB sync                  : {s:8.2f} ms")
    print(f"two streams    32 MiB sync (each)           : {a:8.2f} / {b:8.2f} ms  (worst {w:.2f})")
