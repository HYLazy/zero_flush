#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""verify_dist.py - 抽查 db_bench --key_distribution 补丁生成的 key 分布。

用法:
  verify_dist.py <db_dir> <num_keys>

db_bench 的 key 编码: 16 字节 = 8 字节大端整数序号 + 8 字节 '0' 填充。
用 ldb scan --hex 枚举 DB 中全部唯一 key，统计序号分布:
  - unique 键数量
  - 落在 [0, 1% N) / [0, 10% N) 区间的键占比
zipfian(theta=0.99) 下头部区间占比应远高于 uniform 的 1% / 10%。
"""

import re
import subprocess
import sys
import os


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    db_dir = sys.argv[1]
    n = int(sys.argv[2])

    tools_build = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "..", "..", "..", "..", "source",
                               "rocksdb-exp0-tools", "build")
    tools_build = os.path.abspath(tools_build)
    ldb = os.path.join(tools_build, "tools", "ldb")
    env = dict(os.environ)
    env["LD_LIBRARY_PATH"] = tools_build + ":/usr/lib/x86_64-linux-gnu"

    out = subprocess.run(
        [ldb, "--db=" + db_dir, "scan", "--hex"],
        capture_output=True, text=True, env=env, timeout=600)
    if out.returncode != 0:
        print("ldb scan failed:", out.stderr[:500])
        return 1

    key_re = re.compile(r"^0x([0-9A-Fa-f]+) ==>")
    idxs = []
    for line in out.stdout.splitlines():
        m = key_re.match(line.strip())
        if not m:
            continue
        hx = m.group(1)
        if len(hx) < 16:
            continue
        idxs.append(int(hx[:16], 16))

    if not idxs:
        print("no keys parsed from ldb scan output")
        return 1

    uniq = sorted(set(idxs))
    total = len(uniq)
    in1 = sum(1 for i in uniq if i < n * 0.01)
    in10 = sum(1 for i in uniq if i < n * 0.10)
    print(f"unique_keys={total} (keyspace={n})")
    print(f"frac_unique_idx<1%N={in1 / total:.4f}  "
          f"(uniform expectation ~0.01)")
    print(f"frac_unique_idx<10%N={in10 / total:.4f}  "
          f"(uniform expectation ~0.10)")
    print(f"min_idx={uniq[0]} median_idx={uniq[total // 2]} max_idx={uniq[-1]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
