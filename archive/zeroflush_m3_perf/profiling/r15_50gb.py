#!/usr/bin/env python3
"""R15：M4.4 终态 50GB 验收——align_l1 + 批次 freeze（默认新路径）。
对照：hash 50GB 基线（3,387 ops/s / w-amp 0.70）、R8 align_l1 50GB（22.75K）。
"""
import zf_profile as z

z.NUM_KEYS = 197_379_011
z.WRITES_PER_THREAD = z.NUM_KEYS // z.THREADS
z.TIMEOUT_SEC = 600 * 60

z.VARIANTS["R15"] = dict(
    bin=z.ZF_BIN, build=z.ZF_BUILD,
    extra=z.zf_flags(72, 64, 8)
    + ["--zf_routing=align_l1", "--zf_base_merge", "--zf_partitions=16",
       "--subcompactions=8"])
z.run_variant("R15", z.VARIANTS["R15"], z.HERE, force=True)
