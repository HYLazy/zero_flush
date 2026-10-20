#!/usr/bin/env python3
"""Summarise one layout/Bloom run: the two windows and the per-class mechanism counts.

Used both as a smoke check while building the tool and as a single-run view during the
campaign. Prints what the plan defines:

    Bloom checks per Get = (positive + negative) / Gets of that class
    false positive rate  = positive / (positive + negative)   (absent queries only)

Usage: layout_show.py <out-prefix>
"""
import json
import sys


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    p = sys.argv[1]
    mp, sp = p + ".measure_phase.json", p + ".summary.json"
    M = json.load(open(mp))
    try:
        S = json.load(open(sp))
    except Exception:
        S = {}
    print(f"layout={M.get('layout')} K={M.get('layout_files')} M={M.get('layout_keys')} "
          f"bloom_bits={M.get('bloom_bits')} read_class={M.get('read_class')!r}")
    print(f"  offered={M.get('offered_ops_s')} svc_throughput={M.get('throughput_service_ops_s', 0):.0f} ops/s "
          f"censored={M.get('censored')} device_read={S.get('device_read_bytes', 0)/2**30:.2f} GiB")
    for name in ("perf_window", "diag_window"):
        w = M.get(name) or {}
        print(f"  {name}: ops={w.get('ops', 0)} threads={w.get('threads')} "
              f"cpu_ns/Get={w.get('cpu_ns_per_get')} cycles/Get={w.get('cycles_per_get')} "
              f"insn/Get={w.get('instructions_per_get')} "
              f"insn_errno={w.get('instructions_unavailable_errno')}")
        for k, v in w.items():
            if isinstance(v, dict) and "bloom_checks_per_get" in v:
                print(f"    [{k}] ops={v['ops']} pos={v['bloom_positive']} neg={v['bloom_negative']} "
                      f"checks/Get={v['bloom_checks_per_get']:.3f} fpr={v['false_positive_rate']} "
                      f"cache_hits/Get={v['cache_hits_per_get']:.3f} "
                      f"block_reads/Get={v['block_reads_per_get']:.4f} "
                      f"filter_reads/Get={v['filter_block_reads_per_get']:.4f} "
                      f"index_reads/Get={v['index_block_reads_per_get']:.4f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
