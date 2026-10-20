#!/usr/bin/env python3
"""R26：50GB 稳定性/完成度测试——最终配置 + 强化后台（针对 L0 消费）。
配置：align_l1 + 批次 freeze + cache 512MB + subcompactions=16 +
max_background_jobs=24（R20 基线 28.8K/247 停写；R26 预期消费端加速 → 吞吐提升、停写减少）。
"""
import zf_profile as z

z.NUM_KEYS = 197_379_011
z.WRITES_PER_THREAD = z.NUM_KEYS // z.THREADS
z.TIMEOUT_SEC = 900 * 60  # 15h

z.VARIANTS["R26"] = dict(
    bin=z.ZF_BIN, build=z.ZF_BUILD,
    extra=z.zf_flags(72, 64, 8)
    + ["--zf_routing=align_l1", "--zf_base_merge", "--zf_partitions=16",
       "--subcompactions=16", "--cache_size=536870912",
       "--max_background_jobs=24"])
z.run_variant("R26", z.VARIANTS["R26"], z.HERE, force=True)
