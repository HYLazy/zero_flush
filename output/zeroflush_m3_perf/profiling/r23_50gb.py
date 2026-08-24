#!/usr/bin/env python3
"""R23：50GB 稳定性/完成度测试——M4.5b kSkip 默认开（50GB 终验：完成度 + 数据完整性）。
配置同 R20（align_l1 + base_merge + P=16 + subcomp=16 + jobs=24 + cache 512MB，
可比对照），差异仅 kSkip：ratio 拒绝的分区不再回落 L0，而是攒批到下一
epoch 收养后多代合并——目标是消除 R20 的 247 次停写；R21/R22 验证 kSkip 在 50GB 写密集下受 L0 消费瓶颈限制（吞吐 ~20K 但可完成）与 L0 循环。
验收：完整跑通 + 停写显著减少（< 247）+ 吞吐 ≥ 28.8K + 数据完整。
"""
import zf_profile as z

z.NUM_KEYS = 197_379_011
z.WRITES_PER_THREAD = z.NUM_KEYS // z.THREADS
z.TIMEOUT_SEC = 900 * 60  # 15h

z.VARIANTS["R23"] = dict(
    bin=z.ZF_BIN, build=z.ZF_BUILD,
    extra=z.zf_flags(72, 64, 8)
    + ["--zf_routing=align_l1", "--zf_base_merge", "--zf_partitions=16",
       "--subcompactions=16", "--cache_size=536870912",
       "--max_background_jobs=24"])
z.run_variant("R23", z.VARIANTS["R23"], z.HERE, force=True)
