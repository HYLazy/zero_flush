#!/usr/bin/env python3
"""R22：50GB 稳定性/完成度测试——M4.5b-2 kSkip + 强制融合 + 孤儿直装替换版。
配置同 R20（align_l1 + base_merge + P=16 + subcomp=16 + jobs=24 + cache 512MB，
可比对照），差异仅 kSkip：ratio 拒绝的分区不再回落 L0，而是攒批到下一
epoch 收养后多代合并——目标是消除 R20 的 247 次停写（R21 验证强制融合回落仍退化，R22 加孤儿直装替换）与 L0 循环。
验收：完整跑通 + 停写显著减少+ 吞吐 ≥ 28.8K + 数据完整。
"""
import zf_profile as z

z.NUM_KEYS = 197_379_011
z.WRITES_PER_THREAD = z.NUM_KEYS // z.THREADS
z.TIMEOUT_SEC = 900 * 60  # 15h

z.VARIANTS["R22"] = dict(
    bin=z.ZF_BIN, build=z.ZF_BUILD,
    extra=z.zf_flags(72, 64, 8)
    + ["--zf_routing=align_l1", "--zf_base_merge", "--zf_partitions=16",
       "--subcompactions=16", "--cache_size=536870912",
       "--max_background_jobs=24"])
z.run_variant("R22", z.VARIANTS["R22"], z.HERE, force=True)
