#!/usr/bin/env python3
"""R17：M4.5 50GB 验证——无 upper_conflict 回落（L0 循环打破）。
对照：R8（22.75K、257 停写）、R15（L0 卡死终止）。
"""
import zf_profile as z

z.NUM_KEYS = 197_379_011
z.WRITES_PER_THREAD = z.NUM_KEYS // z.THREADS
z.TIMEOUT_SEC = 600 * 60

z.VARIANTS["R17"] = dict(
    bin=z.ZF_BIN, build=z.ZF_BUILD,
    extra=z.zf_flags(72, 64, 8)
    + ["--zf_routing=align_l1", "--zf_base_merge", "--zf_partitions=16",
       "--subcompactions=8"])
z.run_variant("R17", z.VARIANTS["R17"], z.HERE, force=True)
