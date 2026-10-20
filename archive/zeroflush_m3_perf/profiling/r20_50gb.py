#!/usr/bin/env python3
"""R20：50GB 稳定性/完成度测试——最终配置 + 强化后台（针对 L0 消费）。
配置：align_l1 + 批次 freeze + cache 512MB + subcompactions=16 +
max_background_jobs=24（R19 在 34GB 停滞，强化消费端观察能否完成）。
"""
import zf_profile as z

z.NUM_KEYS = 197_379_011
z.WRITES_PER_THREAD = z.NUM_KEYS // z.THREADS
z.TIMEOUT_SEC = 900 * 60  # 15h

z.VARIANTS["R20"] = dict(
    bin=z.ZF_BIN, build=z.ZF_BUILD,
    extra=z.zf_flags(72, 64, 8)
    + ["--zf_routing=align_l1", "--zf_base_merge", "--zf_partitions=16",
       "--subcompactions=16", "--cache_size=536870912",
       "--max_background_jobs=24"])
z.run_variant("R20", z.VARIANTS["R20"], z.HERE, force=True)
