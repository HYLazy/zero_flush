#!/usr/bin/env python3
"""Plots for the resbudget experiment (run after analyze_resbudget.py)."""
import csv
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = "/home/embed/hyl/metadata_offload"
OUT = os.path.join(ROOT, "bench/out/resbudget")
CAPW = 200.0
CAPR = 400.0


def rows(path):
    try:
        with open(path) as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return []


def device_series(path):
    r = rows(path)
    if not r:
        return np.array([]), np.array([]), np.array([])
    t = np.array([float(x["elapsed_s"]) for x in r])
    w = np.array([float(x["dev_wr_mb_s"]) for x in r])
    rd = np.array([float(x["dev_rd_mb_s"]) for x in r])
    return t, w, rd


def ops_series(path):
    r = rows(path)
    if len(r) < 2:
        return np.array([]), np.array([])
    t = np.array([float(x["elapsed"]) for x in r])
    o = np.array([float(x["ops_cum"]) for x in r])
    dt = np.diff(t)
    rate = np.where(dt > 0, np.diff(o) / np.maximum(dt, 1e-9), 0)
    return t[1:], rate


def main():
    s = json.load(open(os.path.join(OUT, "summary.json")))
    a, b = s.get("A1", {}), s.get("B", {})
    if a.get("status") != "ok" or b.get("status") != "ok":
        print("summary incomplete; need A1 and B")
        return
    bp = s.get("Bpipe", {})
    have_bp = bp.get("status") == "ok"
    plt.rcParams.update({"font.size": 10, "axes.grid": True, "grid.alpha": 0.3})

    # ---- Figure 1: phase bars ------------------------------------------------
    fig, ax = plt.subplots(figsize=(9, 3.2))
    y = [1, 0]
    a_fill, a_drain = a["ingest_wall_s"], a["materialize_wall_s"]
    b_in, b_fill, b_drain = b["ingest_wall_s"], b["materialize_fill_wall_s"], b["materialize_drain_wall_s"]
    ax.barh(y[0], a_fill, color="#4C78A8", label="fill (foreground)")
    ax.barh(y[0], a_drain, left=a_fill, color="#A6CEE3", label="drain (flush+compaction)")
    ax.barh(y[1], b_in, color="#F58518", label="WAL-only ingest")
    ax.barh(y[1], b_fill, left=b_in, color="#54A24B", label="materialization replay (fill)")
    ax.barh(y[1], b_drain, left=b_in + b_fill, color="#B5CF6B", label="replay drain")
    if have_bp:
        bp_in = bp["ingest_wall_s"]
        bp_fill = bp["materialize_fill_wall_s"]
        bp_drain = bp["materialize_drain_wall_s"]
        y.append(-1)
        ax.barh(y[2], bp_in, color="#F58518", label="WAL ingest (paced)")
        ax.barh(y[2], bp_fill, left=bp_in, color="#54A24B", label="materialization replay (overlapped)")
        ax.barh(y[2], bp_drain, left=bp_in + bp_fill, color="#B5CF6B")
        ax.set_yticks(y, ["A standard", "B WAL + deferred", "B-pipe overlap"])
        ax.text(bp_in + bp_fill + bp_drain + 20, y[2],
                "%.0f s" % bp["total_wall_s"], va="center")
    else:
        ax.set_yticks(y, ["A standard", "B WAL + deferred"])
    ax.set_xlabel("wall time (s)")
    ax.set_title("Foreground vs total completion under the same 200/400 MB/s budget")
    for yy, total in ((y[0], a_fill + a_drain), (y[1], b_in + b_fill + b_drain)):
        ax.text(total + 20, yy, "%.0f s" % total, va="center")
    ax.legend(loc="lower right", fontsize=8, ncol=2)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fig_phases.png"), dpi=160)
    fig.savefig(os.path.join(OUT, "fig_phases.svg"))
    plt.close(fig)

    # ---- Figure 2: device I/O rates -----------------------------------------
    fig, axes = plt.subplots(2, 1, figsize=(10, 6), sharex=False)
    for ax, label, key in ((axes[0], "write", 1), (axes[1], "read", 2)):
        t, w, rd = device_series(os.path.join(OUT, "A1/samples.csv"))
        vals = w if key == 1 else rd
        if len(t):
            ax.plot(t, vals, color="#4C78A8", lw=1, label="A standard")
        if have_bp:
            t, w, rd = device_series(os.path.join(OUT, "Bpipe/samples.csv"))
            vals = w if key == 1 else rd
            if len(t):
                ax.plot(t, vals, color="#E45756", lw=1, label="B-pipe (overlap)")
        t, w, rd = device_series(os.path.join(OUT, "B/replay/samples.csv"))
        vals = w if key == 1 else rd
        if len(t):
            ax.plot(t, vals, color="#54A24B", lw=1, label="B materialization")
        cap = CAPW if key == 1 else CAPR
        ax.axhline(cap, color="black", ls="--", lw=1, label="budget")
        ax.set_ylabel("device %s (MB/s)" % label)
        ax.set_ylim(0, max(cap * 1.5, 50))
        ax.legend(loc="upper right", fontsize=8)
    axes[1].set_xlabel("seconds from workload start")
    axes[0].set_title("Device-level rates (whole NVMe; includes the other process's I/O)")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fig_io.png"), dpi=160)
    fig.savefig(os.path.join(OUT, "fig_io.svg"))
    plt.close(fig)

    # ---- Figure 3: A foreground ops/s ---------------------------------------
    fig, ax = plt.subplots(figsize=(10, 3.4))
    t, rate = ops_series(os.path.join(OUT, "A1/fill.samples.csv"))
    if len(t):
        ax.plot(t, rate, color="#4C78A8", lw=0.8, label="A foreground ops/s")
        ax.axvline(a["ingest_wall_s"], color="gray", ls=":", label="fill end")
    t2, rate2 = ops_series(os.path.join(OUT, "B/replay/fill.samples.csv"))
    if len(t2):
        ax.plot(t2 + b["ingest_wall_s"], rate2, color="#54A24B", lw=0.8,
                label="B replay ops/s")
    ax.set_xlabel("wall time (s)")
    ax.set_ylabel("ops/s")
    ax.set_title("Confirmed write rate: A foreground vs B materialization")
    ax.legend(loc="upper right", fontsize=8)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fig_foreground.png"), dpi=160)
    fig.savefig(os.path.join(OUT, "fig_foreground.svg"))
    plt.close(fig)
    print("wrote figures under", OUT)


if __name__ == "__main__":
    main()
