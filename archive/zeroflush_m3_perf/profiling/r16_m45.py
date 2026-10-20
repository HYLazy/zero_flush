#!/usr/bin/env python3
"""R16：M4.5 无 L0 回落验证——终态路径默认（zf_global_index 已翻转）2.2GB。
对照 R12（批次 freeze）：104.9K ops/s；R7（旧路径）：111K。
"""
import zf_profile as z

z.VARIANTS["R16"] = dict(
    bin=z.ZF_BIN, build=z.ZF_BUILD,
    extra=z.zf_flags(72, 64, 8)
    + ["--zf_routing=align_l1", "--zf_base_merge", "--zf_partitions=16",
       "--subcompactions=8"])
z.run_variant("R16", z.VARIANTS["R16"], z.HERE, force=True)
