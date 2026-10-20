#!/usr/bin/env python3
"""R9：M4.2b 50GB 验证 2——base_merge_min_ratio=0.05（P=16 每分区 16MB vs
L1 文件 100MB+，默认 0.25 太高导致融合全被拒、全部回落 L0 恶性循环）。
对照 R8（ratio=0.25）：22.75K ops/s、fallback 3199/3207、stall 52%。
"""
import zf_profile as z

z.NUM_KEYS = 197_379_011
z.WRITES_PER_THREAD = z.NUM_KEYS // z.THREADS
z.TIMEOUT_SEC = 600 * 60

z.VARIANTS["R9"] = dict(
    bin=z.ZF_BIN, build=z.ZF_BUILD,
    extra=z.zf_flags(72, 64, 8)
    + ["--zf_routing=align_l1", "--zf_base_merge", "--zf_partitions=16",
       "--subcompactions=8", "--zf_merge_ratio=0.05"])
z.run_variant("R9", z.VARIANTS["R9"], z.HERE)
