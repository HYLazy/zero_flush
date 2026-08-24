#!/usr/bin/env python3
"""R11：M4.3c 调优（partition_target=64MB）——终态路径（分区索引 + 单分区 freeze）2.2GB。
对照 R7（旧路径 align_l1+subcomp）：111K ops/s、零停写。
"""
import zf_profile as z

z.VARIANTS["R11"] = dict(
    bin=z.ZF_BIN, build=z.ZF_BUILD,
    extra=z.zf_flags(72, 64, 8)
    + ["--zf_routing=align_l1", "--zf_base_merge", "--zf_partitions=16",
       "--subcompactions=8", "--zf_global_index=false", "--zf_partition_target_mb=64"])
z.run_variant("R11", z.VARIANTS["R11"], z.HERE, force=True)
