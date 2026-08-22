#!/usr/bin/env python3
"""R6：L0 compaction 加速验证——max_subcompactions=8（原生并行归并，纯配置）。
对照 R3（同配置无 subcompactions）：38.1K ops/s、24 作业 199s、P50 138us。
"""
import zf_profile as z

z.VARIANTS["R6"] = dict(
    bin=z.ZF_BIN, build=z.ZF_BUILD,
    extra=z.zf_flags(72, 64, 8)
    + ["--zf_routing=sampled", "--zf_base_merge", "--zf_partitions=16",
       "--subcompactions=8"])
z.run_variant("R6", z.VARIANTS["R6"], z.HERE, force=True)
