#!/usr/bin/env python3
"""R19：M4 收尾 50GB 终态验收——upper_conflict 删除 + cache 256MB。
对照：R8（22.75K/257 停写）、R17（21GB 停滞）。
"""
import zf_profile as z

z.NUM_KEYS = 197_379_011
z.WRITES_PER_THREAD = z.NUM_KEYS // z.THREADS
z.TIMEOUT_SEC = 600 * 60

z.VARIANTS["R19"] = dict(
    bin=z.ZF_BIN, build=z.ZF_BUILD,
    extra=z.zf_flags(72, 64, 8)
    + ["--zf_routing=align_l1", "--zf_base_merge", "--zf_partitions=16",
       "--subcompactions=8", "--cache_size=268435456"])
z.run_variant("R19", z.VARIANTS["R19"], z.HERE, force=True)
