#!/usr/bin/env python3
"""R8：M4.2b 50GB 验收——align_l1 + base_merge + P=16 + subcompactions=8。
对照：hash 50GB 基线（3,387 ops/s / 35.16GB / w-amp 0.70）。
"""
import zf_profile as z

z.NUM_KEYS = 197_379_011
z.WRITES_PER_THREAD = z.NUM_KEYS // z.THREADS
z.TIMEOUT_SEC = 600 * 60

z.VARIANTS["R8"] = dict(
    bin=z.ZF_BIN, build=z.ZF_BUILD,
    extra=z.zf_flags(72, 64, 8)
    + ["--zf_routing=align_l1", "--zf_base_merge", "--zf_partitions=16",
       "--subcompactions=8"])
z.run_variant("R8", z.VARIANTS["R8"], z.HERE)
