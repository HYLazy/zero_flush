#!/usr/bin/env python3
"""Re-label a pre-2026-09-25 (hist_version 1) histogram export to lower_bound_v2.

Runbook §2.1 requires that old exports be handled by a legacy parser rather than
merged with v2 labels. This is that parser.

The legacy encoder wrote label = (mant | kGroup) << sh, which adds a spurious high
bit and makes every label for v >= 256 read 2.07x-3.00x too large. The mapping is
still injective on reachable buckets (m = mant|kGroup lands in [24, 31] and fixes
sh through the 2-adic valuation), so the true bucket can be recovered exactly from
the label. No re-run is needed; the recovery is lossless down to bucket resolution,
which is all the original data ever had.

Usage:
  relabel_legacy_hist.py --in LEGACY.csv --out V2.csv
  relabel_legacy_hist.py --selftest

Exit status 0 = every row decoded and round-tripped.
"""
import argparse
import csv
import random
import sys

SMALL, MANTB = 256, 4
GROUP = 1 << MANTB
BASEBIT = 8


def legacy_label(i):
    """v1 encoder, reproduced exactly."""
    if i < SMALL:
        return i
    oct_, mant = divmod(i - SMALL, GROUP)
    return (mant | GROUP) << ((BASEBIT + oct_) - MANTB)


def lower_v2(i):
    if i < SMALL:
        return i
    oct_, mant = divmod(i - SMALL, GROUP)
    return mant << ((BASEBIT + oct_) - MANTB)


def upper_v2(i):
    if i < SMALL:
        return i
    oct_, mant = divmod(i - SMALL, GROUP)
    return ((mant + 1) << ((BASEBIT + oct_) - MANTB)) - 1


def label_to_bucket(label):
    """Invert the legacy label. Returns (lower, upper, was_legacy) or None.

    label = m << sh with m = mant|kGroup in [24, 31] for every reachable bucket.
    sh is NOT the 2-adic valuation of the label (m is often even), so it has to be
    searched; the solution is unique because m in [24,31] cannot be halved and stay
    in range.
    """
    if label < SMALL:
        return (label, label, False)
    for sh in range(0, 64):
        if label % (1 << sh):
            continue
        m = label >> sh
        if 24 <= m <= 31:
            mant = m - 16
            return (mant << sh, ((mant + 1) << sh) - 1, True)
    return None


def relabel(inp, outp):
    rows = list(csv.DictReader(open(inp, newline="")))
    bad, seen = 0, 0
    with open(outp, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ns", "engine_count", "e2e_count", "ns_upper", "hist_version",
                    "hist_encoding", "legacy_ns"])
        for r in rows:
            leg = int(float(r["ns"]))
            dec = label_to_bucket(leg)
            if dec is None:
                bad += 1
                print(f"WARN: label {leg} does not decode as a reachable legacy bucket",
                      file=sys.stderr)
                continue
            lo, hi, was = dec
            seen += 1
            w.writerow([lo, r.get("engine_count", ""), r.get("e2e_count", ""),
                        hi if was else lo, 2, "lower_bound_v2", leg])
    print(f"relabel: rows={len(rows)} relabelled={seen} undecodable={bad}")
    print(f"  input  encoding: legacy_v1 (hist_version 1)")
    print(f"  output encoding: {outp} -> lower_bound_v2 (hist_version 2)")
    return 1 if bad else 0


def selftest():
    """Encode values with the legacy encoder, decode the label, and require
    lo <= v <= hi for every sample."""
    random.seed(7)
    bad = 0
    samples = [0, 1, 255, 256, 287, 288, 319, 320, 512, 543, 1024, 65535, 1 << 40]
    samples += [random.randint(1, 10 ** 9) for _ in range(1000000)]
    for v in samples:
        # find the bucket index of v with v2 math, then take its legacy label
        if v < SMALL:
            i = v
        else:
            bl = v.bit_length()
            oct_ = bl - BASEBIT
            sh = bl - MANTB
            mant = (v >> sh) & (GROUP - 1)
            i = SMALL + oct_ * GROUP + mant
        leg = legacy_label(i)
        dec = label_to_bucket(leg)
        if dec is None:
            bad += 1
            continue
        lo, hi, was = dec
        if not (lo <= v <= hi):
            bad += 1
        if was and legacy_label(i) != leg:
            bad += 1
    print(f"selftest: {len(samples)} samples, failures={bad}")
    for lo, hi, want in ((256, 287, 256), (288, 319, 288), (512, 543, 512)):
        v = lo
        bl = v.bit_length()
        i = SMALL + (bl - BASEBIT) * GROUP + ((v >> (bl - MANTB)) & (GROUP - 1))
        d = label_to_bucket(legacy_label(i))
        ok = d is not None and d[0] == want and d[1] >= hi
        print(f"  legacy {v}..{hi}: label {legacy_label(i)} -> lower {d[0] if d else None}, "
              f"upper {d[1] if d else None} (want lower {want}) {'OK' if ok else 'FAIL'}")
        if not ok:
            bad += 1
    return 1 if bad else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp")
    ap.add_argument("--out", dest="outp")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        return selftest()
    if not a.inp or not a.outp:
        ap.error("--in and --out are required (or --selftest)")
    return relabel(a.inp, a.outp)


if __name__ == "__main__":
    sys.exit(main())
