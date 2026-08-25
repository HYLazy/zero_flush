"""R36：50GB 终验——M4.6 L0 并行消费（subcompactions + l0_parallelism=8 多 job + ZF 恒并行）+ CURRENT 一致性修复 + kSkip 默认关。对比 R20（28.8K/247 停写/1.9h）。"""
"""R36：50GB 稳定性/完成度测试——最终配置 + 强化后台（针对 L0 消费）。
配置：align_l1 + 批次 freeze + cache 512MB + subcompactions=16 +
max_background_jobs=24（R19 在 34GB 停滞，强化消费端观察能否完成）。
"""
import zf_profile as z

z.NUM_KEYS = 197_379_011
z.WRITES_PER_THREAD = z.NUM_KEYS // z.THREADS
z.TIMEOUT_SEC = 900 * 60  # 15h

z.VARIANTS["R36"] = dict(
    bin=z.ZF_BIN, build=z.ZF_BUILD,
    extra=z.zf_flags(72, 64, 8)
    + ["--zf_routing=align_l1", "--zf_base_merge", "--zf_partitions=16",
       "--subcompactions=16", "--cache_size=536870912",
       "--max_background_jobs=24"])
z.run_variant("R36", z.VARIANTS["R36"], z.HERE, force=True)
