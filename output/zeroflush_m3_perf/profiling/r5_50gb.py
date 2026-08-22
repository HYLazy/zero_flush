#!/usr/bin/env python3
"""M4.0 50GB 写放大验证：R4 = sampled + base_merge + P=16（R3 的 50GB 版）。
对照基线：output/zeroflush_m3_perf/benchmark_zeroflush.json 的 hash 50GB
（3,387 ops/s，35.16GB，w-amp 0.70）与 benchmark_native.json（124,125 ops/s）。
"""
import zf_profile as z

z.NUM_KEYS = 197_379_011          # vs256 50GB 逻辑量（与既有基准同口径）
z.WRITES_PER_THREAD = z.NUM_KEYS // z.THREADS
z.TIMEOUT_SEC = 900 * 60          # 15h 上限

z.VARIANTS["R5"] = dict(
    bin=z.ZF_BIN, build=z.ZF_BUILD,
    extra=z.zf_flags(72, 64, 8)
    + ["--zf_routing=sampled", "--zf_base_merge", "--zf_partitions=16"])
z.run_variant("R5", z.VARIANTS["R5"], z.HERE)
