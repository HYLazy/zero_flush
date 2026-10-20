#!/usr/bin/env python3
"""Merge per-K backlog roots into one read-only root for analysis.

The K scan writes every K into its own output root (so a failure cannot touch another
K's evidence), and the analyzer expects one root with `runs/` and `manifest.jsonl`.
This builds that root out of SYMLINKS to the per-run artifacts -- nothing is copied and
nothing in the source roots is touched, so the analysis root can be deleted and rebuilt
at any time.

Usage:
  l0_kscan_merge.py --out DIR --from ROOT [ROOT ...]
"""
import argparse
import glob
import os
import shutil
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--from", dest="srcs", nargs="+", required=True)
    ap.add_argument("--force", action="store_true", help="delete an existing analysis root first")
    a = ap.parse_args()

    missing = [s for s in a.srcs if not os.path.isdir(s)]
    if missing:
        print("missing source root(s): " + ", ".join(missing), file=sys.stderr)
        return 2
    if os.path.exists(a.out):
        if not a.force:
            print(f"{a.out} exists; pass --force to rebuild it", file=sys.stderr)
            return 2
        shutil.rmtree(a.out)
    runs = os.path.join(a.out, "runs")
    os.makedirs(runs)

    n_runs = 0
    with open(os.path.join(a.out, "manifest.jsonl"), "w") as out:
        for src in a.srcs:
            man = os.path.join(src, "manifest.jsonl")
            if not os.path.exists(man):
                print(f"  {src}: no manifest.jsonl, skipped")
                continue
            n_src = 0
            for line in open(man):
                line = line.strip()
                if not line:
                    continue
                out.write(line + "\n")
                n_src += 1
            # link every artifact of every run named by this root's manifest
            import json
            for line in open(man):
                line = line.strip()
                if not line:
                    continue
                rid = json.loads(line)["run_id"]
                for f in sorted(glob.glob(os.path.join(src, "runs", rid + ".*"))):
                    link = os.path.join(runs, os.path.basename(f))
                    if os.path.lexists(link):
                        print(f"  duplicate artifact {os.path.basename(f)}; keeping the first",
                              file=sys.stderr)
                        continue
                    os.symlink(os.path.abspath(f), link)
            n_runs += n_src
            print(f"  {src}: {n_src} runs")

    per_k = {}
    with open(os.path.join(a.out, "manifest.jsonl")) as f:
        import json
        for line in f:
            r = json.loads(line)
            per_k.setdefault(r["K"], set()).add((r["kind"], r["layout"], r["B"]))
    with open(os.path.join(a.out, "SOURCES.txt"), "w") as f:
        for src, k in zip(a.srcs, [None] * len(a.srcs)):
            f.write(f"{src}\n")
        f.write("\nsources above; runs are symlinks, the per-K roots remain authoritative\n")
        for k in sorted(per_k):
            f.write(f"K={k}: {len(per_k[k])} (kind, layout, B) combos\n")
    print(f"merged {n_runs} runs into {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
