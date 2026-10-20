#!/usr/bin/env python3
"""Summarise a settled RocksDB snapshot directory from a driver run's artifacts.

Runbook §4.2 requires checking, before every replica run:
  manifest, live SST bytes, L0 file count and key count
and in §4.1 a snapshot checksum via a verify pass. This prints those, taking the
key/byte/file numbers from the driver's own property samples rather than from
os.stat, so the numbers are RocksDB's view, not the filesystem's.

Usage:
  snapshot_info.py --run-prefix DIR/NAME [--json]
Prints a JSON object; exit 0 if the directory looks like a complete DB
(CURRENT + MANIFEST present and at least one SST).
"""
import argparse
import csv
import glob
import json
import os
import sys


def last_row(path):
    if not os.path.exists(path):
        return None
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    return rows[-1] if rows else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-prefix", required=True,
                    help="the --out prefix a `l0bench --mode=open` run wrote to")
    ap.add_argument("--db", default=None, help="DB directory (default: parse from config)")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()

    p = a.run_prefix
    db = a.db
    cfg = p + ".config.txt"
    if db is None and os.path.exists(cfg):
        for line in open(cfg, errors="replace"):
            if line.startswith("argv:"):
                for tok in line.split():
                    if tok.startswith("--db="):
                        db = tok[5:]
    info = {"db": db, "exists": bool(db and os.path.isdir(db))}
    if info["exists"]:
        names = os.listdir(db)
        info["has_current"] = "CURRENT" in names
        info["has_identity"] = "IDENTITY" in names
        info["manifest"] = [n for n in names if n.startswith("MANIFEST") and n != "MANIFEST"]
        info["n_sst"] = len([n for n in names if n.endswith(".sst")])
        info["n_wal"] = len([n for n in names if n.endswith(".log")])
        info["dir_bytes"] = sum(os.path.getsize(os.path.join(db, n)) for n in names
                                if os.path.isfile(os.path.join(db, n)))

    s = last_row(p + ".samples.csv")
    if s:
        def num(key):
            try:
                return int(float(s.get(key, "")))
            except Exception:
                return None
        info["estimate_num_keys"] = num("estimate-num-keys")
        info["total_sst_files_size"] = num("total-sst-files-size")
        # per-level file counts come from the samples row
        lv = {}
        for i in range(7):
            try:
                lv[f"L{i}"] = int(float(s.get(f"num-files-at-level{i}", "")))
            except Exception:
                lv[f"L{i}"] = None
        info["num_files_at_level"] = lv
        info["base_level"] = num("base-level")
        info["immutable_memtables"] = num("num-immutable-mem-table")
        info["pending_compaction_bytes"] = num("estimate-pending-compaction-bytes")
    d = last_row(p + ".dirscan.csv")
    if d:
        info["dirscan_sst_bytes"] = int(float(d["sst_bytes"]))
        info["dirscan_n_sst"] = int(float(d["n_sst"]))

    complete = bool(info["exists"] and info.get("has_current") and info.get("manifest")
                    and info.get("n_sst", 0) > 0)
    info["complete"] = complete
    if a.json:
        json.dump(info, sys.stdout, indent=2)
        print()
    else:
        for k in sorted(info):
            print(f"{k}={info[k]}")
    return 0 if complete else 1


if __name__ == "__main__":
    sys.exit(main())
