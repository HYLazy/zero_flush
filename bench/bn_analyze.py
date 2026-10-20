#!/usr/bin/env python3
"""bn_analyze.py -- bottleneck-campaign analysis (native RocksDB, no source patches).

Serves the intervention plan (瓶颈判定):
  queue   <run_dir>         per-run queue timeline: onset ordering (计划 §三),
                            per-stage service rates & pool utilisation, stall
                            attribution, device bandwidth/busy, steady-state queues
  arm     [pattern]         aggregate out/I/bn_<arm>_r<N> runs by arm (mean +/- sd)
  matrix                    Flush x Compaction capacity matrix table (计划 §五)
  isoflush                  flush isolation summary (计划 §六)
  isol0                     L0->base fixed-input replay summary (计划 §六)
All numbers are derived from the per-run CSVs written by l0bench plus the DB LOG copy.
"""
import csv, json, math, os, re, sys
from collections import defaultdict

BENCH = os.path.dirname(os.path.abspath(__file__))
OUT_I = os.path.join(BENCH, "out", "I")
GIB = 1 << 30
SOFT_LIMIT = 64 * GIB          # rocksdb default soft_pending_compaction_bytes_limit


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def load_csv(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def find_prefix(run_dir):
    """run_dir contains <rid>.samples.csv — return (rid, prefix)."""
    for fn in os.listdir(run_dir):
        if fn.endswith(".samples.csv"):
            rid = fn[: -len(".samples.csv")]
            return rid, os.path.join(run_dir, rid)
    return None, None


class Run:
    def __init__(self, run_dir):
        self.dir = run_dir
        self.rid, self.prefix = find_prefix(run_dir)
        if not self.rid:
            raise SystemExit(f"no .samples.csv in {run_dir}")
        self.samples = load_csv(self.prefix + ".samples.csv")
        self.compactions = load_csv(self.prefix + ".compactions.csv")
        self.events = load_csv(self.prefix + ".events.csv")
        self.levels = load_csv(self.prefix + ".layout_levels.csv")
        self.summary = {}
        sj = self.prefix + ".summary.json"
        if os.path.exists(sj):
            self.summary = json.load(open(sj))
        self.F, self.C = self._pools()
        self.t0 = num(self.samples[0]["ts"]) if self.samples else 0

    def _pools(self):
        eff = {}
        p = os.path.join(self.dir, self.rid + ".effective.txt")
        if os.path.exists(p):
            for line in open(p):
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    eff[k] = v
        jobs = int(num(eff.get("max_background_jobs", 8)))
        f = eff.get("max_background_flushes", "-1")
        c = eff.get("max_background_compactions", "-1")
        if int(num(f)) >= 1 and int(num(c)) >= 1:
            return int(num(f)), int(num(c))
        # derived branch: flush = max(1, jobs/4), compaction = jobs - flush
        fl = max(1, jobs // 4)
        return fl, max(1, jobs - fl)

    # --- raw series helpers -------------------------------------------------
    def series(self, key):
        return [(num(r["ts"]), num(r.get(key, 0))) for r in self.samples]

    def delta_series(self, key):
        s = self.series(key)
        out = []
        for (ta, a), (tb, b) in zip(s, s[1:]):
            dt = tb - ta
            if dt > 0:
                out.append((tb, (b - a) / dt))
        return out

    def pct(self, series, p):
        if not series:
            return float("nan")
        v = sorted(x[1] for x in series)
        return v[min(len(v) - 1, int(p * len(v)))]

    def slope(self, key, t_from=None, t_to=None, absolute=False):
        """least-squares slope of KEY vs t (per second); absolute=True returns (b-a)/(tb-ta)."""
        s = [(t, v) for t, v in self.series(key)]
        if t_from is not None:
            s = [(t, v) for t, v in s if t - self.t0 >= t_from]
        if t_to is not None:
            s = [(t, v) for t, v in s if t - self.t0 <= t_to]
        if len(s) < 3:
            return 0.0, 0.0
        if absolute:
            return (s[-1][1] - s[0][1]) / (s[-1][0] - s[0][0]), 0.0
        n = len(s)
        sx = sum(t for t, _ in s); sy = sum(v for _, v in s)
        sxx = sum(t * t for t, _ in s); sxy = sum(t * v for t, v in s)
        denom = n * sxx - sx * sx
        if denom == 0:
            return 0.0, 0.0
        b = (n * sxy - sx * sy) / denom
        a = (sy - b * sx) / n
        return b, a   # slope per second, intercept

    # --- first-crossing detection (sustained) --------------------------------
    def sustained_cross(self, key, thresh, hold_s=10.0):
        """first elapsed t where key >= thresh continuously for hold_s; None if never."""
        start = None
        for t, v in self.series(key):
            e = t - self.t0
            if v >= thresh:
                if start is None:
                    start = e
                if e - start >= hold_s:
                    return start
            else:
                start = None
        return None

    # --- stage service from job records --------------------------------------
    # NOTE on level attribution: RocksDB's CompactionJobInfo.input_file_infos[].level
    # carries the compaction's input-SLOT index (0..num_input_levels-1), not the LSM
    # level (db_impl_compaction_flush.cc records `static_cast<int>(i)` of the slot
    # loop). For leveled compactions slot0 = start-level files, slot1 = output-level
    # files; the real levels are base_input_level / output_level (also in the CSV).
    # in_bytes_0 is therefore "bytes at base_input_level", in_bytes_1 "bytes at
    # output_level". All aggregates below use this corrected mapping.
    def job_stats(self):
        rows = self.compactions
        tot_wall = sum(num(r["elapsed_us"]) for r in rows) / 1e6
        tot_cpu = sum(num(r["cpu_us"]) for r in rows) / 1e6
        agg = dict(n=0, wall_s=0.0, cpu_s=0.0,
                   in_L0=0, in_L0_base_side=0, in_deep_start=0, in_deep_out_side=0,
                   out=0, n_intraL0=0, n_L0base=0, n_deep=0, n_trivial=0,
                   out_intraL0=0, out_L0base=0, out_deep=0,
                   in_L0_L0base=0, in_base_L0base=0, in_start_deep=0, in_outside_deep=0)
        for r in rows:
            start = int(num(r["base_input_level"])); outl = int(num(r["output_level"]))
            b0 = num(r["in_bytes_0"]); b1 = num(r["in_bytes_1"])
            wall = num(r["elapsed_us"]) / 1e6
            elapsed_us = num(r["elapsed_us"])
            o = num(r["output_bytes"])
            agg["n"] += 1; agg["wall_s"] += wall; agg["cpu_s"] += num(r["cpu_us"]) / 1e6
            agg["out"] += o
            trivial = elapsed_us < 1000 and o <= max(b0 + b1, 1) * 1.001
            if trivial:
                # a trivial move installs the same files at a deeper level: stats
                # still report output_bytes, but no bytes are written. Count only
                # the job, exclude its bytes from every ledger.
                agg["n_trivial"] += 1
                agg["n"] -= 1; agg["wall_s"] -= wall
                continue
            if outl == 0:                      # intra-L0
                agg["n_intraL0"] += 1; agg["out_intraL0"] += o
                agg["in_L0"] += b0
            elif start == 0:                   # true L0->base merge
                agg["n_L0base"] += 1; agg["out_L0base"] += o
                agg["in_L0"] += b0; agg["in_L0_base_side"] += b1
                agg["in_L0_L0base"] += b0; agg["in_base_L0base"] += b1
            else:                              # deep cascade start->out
                agg["n_deep"] += 1; agg["out_deep"] += o
                agg["in_deep_start"] += b0; agg["in_deep_out_side"] += b1
                agg["in_start_deep"] += b0; agg["in_outside_deep"] += b1
        return agg

    def flush_stats(self):
        """events.csv flush_installed rows carry wall us + bytes + entries."""
        wall = 0.0; byt = 0; n = 0; entries = 0
        for r in self.events:
            if r["kind"] == "flush_installed":
                wall += num(r["wall_us"]) / 1e6
                byt += num(r["output_bytes"])
                entries += num(r["num_input_records"])
                n += 1
        return dict(n=n, wall_s=wall, bytes=byt, entries=entries)


    def flush_queue_delay(self):
        """Flush queueing delay: pair each memtable_sealed with the next flush_begin.
        RocksDB exposes no eligible->scheduled timestamp, but in this driver flushes
        are the only consumer of sealed memtables and the pool is F-wide, so the
        pairing is (sealed_i -> begin_i) in event order. Returns per-pair seconds."""
        seals = sorted([(num(e["ts"]), int(num(e["job"]))) for e in self.events
                        if e["kind"] == "memtable_sealed"])
        begins = sorted([(num(e["ts"]), int(num(e["job"]))) for e in self.events
                         if e["kind"] == "flush_begin"])
        # align by order (drop extras at either end)
        n = min(len(seals), len(begins))
        out = []
        for i in range(n):
            d = begins[i][0] - seals[i][0]
            if d >= -0.001:
                out.append(d)
        return out

    def stall_lines(self):
        """db.LOG stall lines attributed to THIS run only. The LOG is a copy of the
        whole DB directory's LOG (may include earlier phases); scan every LOG copy
        we have (db.LOG, LOG.workload) and filter by the run's start_wall."""
        import datetime
        start = None
        cfg = os.path.join(self.dir, self.rid + ".config.txt")
        if os.path.exists(cfg):
            for line in open(cfg):
                if line.startswith("start_wall="):
                    start = float(line.split("=", 1)[1])
        t0 = datetime.datetime.fromtimestamp(start) if start else None
        cls = defaultdict(int)
        seen = set()
        for base in ("db.LOG", "LOG.workload"):
            p = os.path.join(self.dir, base)
            if not os.path.exists(p):
                continue
            for line in open(p, errors="replace"):
                if "Stalling writes" not in line and "Stopping writes" not in line:
                    continue
                m = re.match(r"(\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2})", line)
                key = m.group(1) if m else line[:64]
                if key in seen:      # same line captured in both copies
                    continue
                if t0 is not None and m:
                    try:
                        ts = datetime.datetime.strptime(m.group(1), "%Y/%m/%d-%H:%M:%S")
                        if ts < t0.replace(microsecond=0):
                            continue
                        seen.add(key)
                    except ValueError:
                        pass
                if "immutable memtable" in line:
                    cls["imm"] += 1
                elif "estimated pending compaction bytes" in line:
                    cls["pending"] += 1
                elif "level-0 file" in line:
                    cls["l0files"] += 1
                else:
                    cls["other"] += 1
        return cls

    def latency_pct_from_hist(self):
        """hist.csv: ns,engine_count,e2e_count cumulative -> percentiles (us)."""
        p = self.prefix + ".hist.csv"
        if not os.path.exists(p):
            return {}
        rows = load_csv(p)
        out = {}
        for col, tag in (("engine_count", "eng"), ("e2e_count", "e2e")):
            tot = sum(num(r[col]) for r in rows)
            if tot <= 0:
                continue
            c = 0
            ps = {}
            targets = [0.5, 0.99, 0.999]
            ti = 0
            for r in rows:
                c += num(r[col])
                while ti < len(targets) and c >= targets[ti] * tot:
                    ps[targets[ti]] = num(r["ns"]) / 1000.0
                    ti += 1
            for k, v in ps.items():
                name = {0.5: "p50", 0.99: "p99", 0.999: "p999"}[k]
                out[f"{name}_{tag}_us"] = v
        return out


def queue_report(run_dir, verbose=True):
    r = Run(run_dir)
    T = num(r.samples[-1]["elapsed"]) if r.samples else 0
    # workload-end: last t where confirmed ops still grew (arrival phase);
    # after that the run is only draining (l0/pend decay, no lambda).
    uops = r.series("u_ops")
    t_work_end = T
    for (ta, a), (tb, b) in zip(uops, uops[1:]):
        if b > a:
            t_work_end = tb - r.t0
    S = r.summary
    F, C = r.F, r.C

    # ---- onset ordering (计划 §三) -----------------------------------------
    onset = {
        "imm>=2 (10s)": r.sustained_cross("num-immutable-mem-table", 2),
        "imm>=3 (10s)": r.sustained_cross("num-immutable-mem-table", 3),
        "L0>=trigger8 (10s)": r.sustained_cross("num-files-at-level0", 8),
        "pending>=16G": r.sustained_cross("estimate-pending-compaction-bytes", 16 * GIB, 5),
        "pending>=32G": r.sustained_cross("estimate-pending-compaction-bytes", 32 * GIB, 5),
        "pending>=64G(soft)": r.sustained_cross("estimate-pending-compaction-bytes", SOFT_LIMIT, 5),
    }
    # ---- growth-phase slopes ------------------------------------------------
    # pending growth window: first contiguous climb from 5G up to 60G
    b_pend, _ = r.slope("estimate-pending-compaction-bytes", t_from=None, t_to=None)
    pts = []
    climbing = False
    for t, v in r.series("estimate-pending-compaction-bytes"):
        if not climbing and v >= 5 * GIB:
            climbing = True
        if climbing:
            pts.append((t - r.t0, v))
        if climbing and v >= 60 * GIB:
            break
    if len(pts) >= 5:
        n = len(pts); sx = sum(t for t, _ in pts); sy = sum(v for _, v in pts)
        sxx = sum(t * t for t, _ in pts); sxy = sum(t * v for t, v in pts)
        d = n * sxx - sx * sx
        pend_slope = (n * sxy - sx * sy) / d / GIB if d else 0.0
        pend_win = (pts[0][0], pts[-1][0])
    else:
        pend_slope, pend_win = 0.0, None
    lam_b, _ = r.slope("u_bytes", t_from=pend_win[0] if pend_win else None,
                       t_to=pend_win[1] if pend_win else None, absolute=True)

    # ---- steady state (arrival phase, from 30% of workload end) ------------
    ss_from = 0.3 * t_work_end
    def ss(key):
        s = [(t - r.t0, v) for t, v in r.series(key) if ss_from <= t - r.t0 <= t_work_end]
        return s
    def med(key): return r.pct(ss(key), 0.5)
    def p90(key): return r.pct(ss(key), 0.9)

    def rate_in(key, scale=1.0):
        """bytes/s (or ms/s) over the arrival window [ss_from, t_work_end]."""
        s = [(t, v) for t, v in r.series(key)
             if ss_from <= t - r.t0 <= t_work_end]
        if len(s) < 3:
            return float("nan")
        dt = s[-1][0] - s[0][0]
        return (s[-1][1] - s[0][1]) / dt * scale if dt > 0 else float("nan")

    thr_ss, _ = r.slope("u_ops", t_from=ss_from, t_to=t_work_end, absolute=True)
    # tickers are byte counters; device counters are sectors (x512)
    flush_byt_rate = rate_in("flush_write_bytes")
    comp_byt_rate = rate_in("compact_write_bytes")
    comp_rd_rate = rate_in("compact_read_bytes")
    dev_wr = rate_in("dev_wr_sectors")
    dev_rd = rate_in("dev_rd_sectors")
    dev_busy = None
    if r.samples[-1].get("dev_io_ticks_ms"):
        b = rate_in("dev_io_ticks_ms")
        dev_busy = b / 1000.0 * 100.0 if not math.isnan(b) else None

    # L0 bytes over time from the layout sampler (level 0 rows)
    l0b = [(num(x["ts"]) - r.t0, num(x["total_bytes"])) for x in r.levels
           if int(num(x["level"])) == 0]
    l0b_ss = [v for t, v in l0b if ss_from <= t <= t_work_end]
    l0_med_bytes = sorted(l0b_ss)[len(l0b_ss) // 2] if l0b_ss else 0

    js = r.job_stats(); fs = r.flush_stats()
    fqd = r.flush_queue_delay()
    fqd_med = sorted(fqd)[len(fqd)//2]*1000 if fqd else float("nan")
    fqd_p90 = sorted(fqd)[int(0.9*len(fqd))]*1000 if fqd else float("nan")
    fqd_mean = sum(fqd)/len(fqd)*1000 if fqd else float("nan")
    comp_util = js["wall_s"] / (C * T) * 100 if T else float("nan")
    flush_util = fs["wall_s"] / (F * T) * 100 if T else float("nan")
    # pool busy restricted to the arrival phase (job windows inside [ss_from, t_work_end])
    comp_wall_arr = sum(num(row["elapsed_us"]) / 1e6 for row in r.compactions
                        if ss_from <= num(row["ts_begin"]) - r.t0 <= t_work_end)
    flush_wall_arr = sum(num(e["wall_us"]) / 1e6 for e in r.events
                         if e["kind"] == "flush_installed"
                         and ss_from <= num(e["ts"]) - r.t0 <= t_work_end)
    arr_span = max(1e-9, t_work_end - ss_from)
    comp_util_arr = comp_wall_arr / (C * arr_span) * 100
    flush_util_arr = flush_wall_arr / (F * arr_span) * 100
    runcomp_mean = sum(v for _, v in ss("num-running-compactions")) / max(1, len(ss("num-running-compactions")))
    runflush_mean = sum(v for _, v in ss("num-running-flushes")) / max(1, len(ss("num-running-flushes")))

    # stall union: samples with stall_micros advancing
    st = r.delta_series("stall_micros")
    st_union = sum(1 for _, v in st if v > 0) / max(1, len(st)) * 100
    stalls = r.stall_lines()
    lat = r.latency_pct_from_hist()

    U = num(S.get("U_bytes", 0)) or (num(S.get("ops_ok", 0)) * 1040)
    wa = (num(S.get("device_write_bytes", 0)) / U) if U else float("nan")

    res = dict(
        rid=r.rid, pools=f"F={F}/C={C}", duration_s=T,
        ops_ok=S.get("ops_ok"), throughput=S.get("throughput_ops_s"),
        thr_steady=thr_ss,
        onset=onset, pend_slope_gib_s=pend_slope, lam_gib_s=lam_b / GIB,
        ss_median=dict(imm=med("num-immutable-mem-table"),
                       l0_files=med("num-files-at-level0"),
                       pending_gib=med("estimate-pending-compaction-bytes") / GIB,
                       runcomp=runcomp_mean, runflush=runflush_mean),
        l0_median_bytes=l0_med_bytes,
        flush_queue=dict(n=len(fqd), mean_ms=fqd_mean, p50_ms=fqd_med, p90_ms=fqd_p90),
        ss_p90=dict(imm=p90("num-immutable-mem-table"),
                    l0_files=p90("num-files-at-level0"),
                    pending_gib=p90("estimate-pending-compaction-bytes") / GIB),
        service=dict(flush_MB_s=flush_byt_rate / 2**20,
                     comp_wr_MB_s=comp_byt_rate / 2**20,
                     comp_rd_MB_s=comp_rd_rate / 2**20,
                     dev_wr_MB_s=dev_wr * 512 / 2**20,
                     dev_rd_MB_s=dev_rd * 512 / 2**20,
                     dev_busy_pct=dev_busy),
        util=dict(comp_pool_pct=comp_util, flush_pool_pct=flush_util,
                  comp_pool_arr_pct=comp_util_arr, flush_pool_arr_pct=flush_util_arr),
        t_work_end=t_work_end,
        jobs=dict(comp_n=js["n"], comp_wall_s=js["wall_s"], comp_cpu_s=js["cpu_s"],
                  comp_cpu_per_wall=js["cpu_s"] / js["wall_s"] if js["wall_s"] else 0,
                  n_intraL0=js["n_intraL0"], n_L0base=js["n_L0base"], n_deep=js["n_deep"],
                  n_trivial=js["n_trivial"],
                  out_intraL0_gib=js["out_intraL0"] / GIB, out_L0base_gib=js["out_L0base"] / GIB,
                  out_deep_gib=js["out_deep"] / GIB,
                  in_L0_gib=js["in_L0"] / GIB,
                  in_L0base_base_side_gib=js["in_base_L0base"] / GIB,
                  in_deep_start_gib=js["in_deep_start"] / GIB,
                  in_deep_out_side_gib=js["in_deep_out_side"] / GIB,
                  out_gib=js["out"] / GIB,
                  flush_n=fs["n"], flush_wall_s=fs["wall_s"],
                  flush_MB_s_job=fs["bytes"] / fs["wall_s"] / 2**20 if fs["wall_s"] else 0),
        stalls=dict(lines=dict(stalls), union_pct=st_union),
        device_wa=wa, **lat,
    )
    if verbose:
        print(f"== {r.rid}  pools {res['pools']}  T={T:.0f}s ops={res['ops_ok']} "
              f"thr={res['throughput']:.0f} ops/s (steady {thr_ss:.0f})")
        print("-- onset ordering (elapsed s; None = never crossed sustained):")
        for k, v in onset.items():
            print(f"   {k:<22} {('%.1f' % v) if v is not None else 'never'}")
        print(f"-- pending slope {pend_slope:.2f} GiB/s over window {pend_win}; "
              f"lambda {lam_b/GIB:.2f} GiB/s")
        print("-- steady state (last 70%%): median imm=%.2f L0=%.1f pending=%.1f GiB; "
              "runcomp=%.2f runflush=%.2f L0bytes=%.2fG" % (
                  res["ss_median"]["imm"], res["ss_median"]["l0_files"],
                  res["ss_median"]["pending_gib"], runcomp_mean, runflush_mean,
                  l0_med_bytes / GIB))
        print(f"-- service (arrival phase, ends t={t_work_end:.0f}s): flush %.0f MB/s (pool busy %.0f%%) | comp wr %.0f rd %.0f MB/s (pool busy %.0f%%, cpu/wall %.2f) | dev wr %.0f rd %.0f MB/s busy %s" % (
                  res["service"]["flush_MB_s"], res["util"]["flush_pool_arr_pct"],
                  res["service"]["comp_wr_MB_s"], res["service"]["comp_rd_MB_s"],
                  res["util"]["comp_pool_arr_pct"], res["jobs"]["comp_cpu_per_wall"],
                  res["service"]["dev_wr_MB_s"], res["service"]["dev_rd_MB_s"],
                  ("%.1f%%" % dev_busy) if dev_busy is not None else "n/a"))
        print("-- jobs (corrected slot->level mapping): comp n=%d wall=%.0fs cpu=%.0fs | intraL0 n=%d(%.1fG out) L0->base n=%d(%.1fG out, base-side in %.1fG) deep n=%d(%.1fG out) trivial n=%d | L0 in %.1fG | flush n=%d wall=%.0fs (%.0f MB/s/stream)"
              % (js["n"], js["wall_s"], js["cpu_s"],
                 js["n_intraL0"], res["jobs"]["out_intraL0_gib"],
                 js["n_L0base"], res["jobs"]["out_L0base_gib"], res["jobs"]["in_L0base_base_side_gib"],
                 js["n_deep"], res["jobs"]["out_deep_gib"], js["n_trivial"],
                 res["jobs"]["in_L0_gib"],
                 fs["n"], fs["wall_s"], res["jobs"]["flush_MB_s_job"]))
        print("-- stalls: lines %s | union %.1f%% | device WA %.2fx" % (dict(stalls), st_union, wa))
        if lat:
            print("-- latency (us): " + " ".join(f"{k}={v:.1f}" for k, v in sorted(lat.items())))
    return res


def arm_name(rid):
    m = re.match(r"bn_(.+)_r(\d+)$", rid)
    return m.group(1) if m else None


def cmd_arm(pattern=None):
    groups = defaultdict(list)
    for d in sorted(os.listdir(OUT_I)):
        p = os.path.join(OUT_I, d)
        if not os.path.isdir(p) or not d.startswith("bn_"):
            continue
        rid = find_prefix(p)[0]
        a = arm_name(rid or "")
        if a and (pattern is None or pattern in a):
            groups[a].append(p)
    rows = []
    for a, dirs in sorted(groups.items()):
        reps = [queue_report(d, verbose=False) for d in dirs]
        def agg(keyfn, fmt):
            vals = [keyfn(x) for x in reps if keyfn(x) is not None and not math.isnan(keyfn(x))]
            if not vals:
                return "n/a"
            m = sum(vals) / len(vals)
            if len(vals) > 1:
                sd = math.sqrt(sum((v - m) ** 2 for v in vals) / (len(vals) - 1))
            else:
                sd = 0.0
            return fmt % (m, sd)
        n = len(reps)
        rows.append((a, n,
                     agg(lambda x: x["throughput"], "%.0f±%.0f"),
                     agg(lambda x: x["thr_steady"], "%.0f±%.0f"),
                     agg(lambda x: x["ss_median"]["pending_gib"], "%.1f±%.1f"),
                     agg(lambda x: x["ss_median"]["l0_files"], "%.1f±%.1f"),
                     agg(lambda x: x["ss_median"]["imm"], "%.2f±%.2f"),
                     agg(lambda x: x["stalls"]["union_pct"], "%.1f±%.1f"),
                     agg(lambda x: x["stalls"]["lines"].get("imm", 0), "%.0f±%.0f"),
                     agg(lambda x: x["util"]["comp_pool_arr_pct"], "%.0f±%.0f"),
                     agg(lambda x: x["util"]["flush_pool_arr_pct"], "%.0f±%.0f"),
                     agg(lambda x: x["service"]["dev_busy_pct"] if x["service"]["dev_busy_pct"] is not None else float("nan"), "%.0f±%.0f"),
                     agg(lambda x: x["device_wa"], "%.2f±%.2f")))
    hdr = ["arm", "n", "thr_ops_s", "thr_ss", "pend_GiB", "L0", "imm", "stall_un%",
           "imm_lines", "compUtil%", "flushUtil%", "devBusy%", "devWA"]
    print(("%-11s %2s " + "%-16s " * 3 + "%-12s " * 7) % tuple(hdr))
    for row in rows:
        print(("%-11s %2d " + "%-16s " * 3 + "%-12s " * 7) % row)


def cmd_matrix():
    cmd_arm()  # the arm table IS the matrix; print header note
    print("\nmatrix reading: f*=flush scan (C=6), c*=compaction scan (F=2), corners = §五 first round")


def cmd_isoflush():
    for name in ("wb64m", "wb256m", "wb1g", "wb8g"):
        d = os.path.join(OUT_I, f"bn_isoflush_{name}")
        if not os.path.isdir(d):
            continue
        r = Run(d)
        fs = r.flush_stats()
        T = num(r.samples[-1]["elapsed"])
        ut = r.series("utime_ticks"); st = r.series("stime_ticks")
        cpu_s = ((ut[-1][1] - ut[0][1]) + (st[-1][1] - st[0][1])) / 100.0 if ut else 0
        U = num(r.summary.get("U_bytes", 0))
        dw = num(r.summary.get("device_write_bytes", 0))
        dr = num(r.summary.get("device_read_bytes", 0))
        print(f"{name:8s} T={T:6.0f}s flushes={fs['n']:4d} flush_wall={fs['wall_s']:7.1f}s "
              f"flush_bytes={fs['bytes']/GIB:6.2f}GiB entries={fs['entries']/1e6:6.2f}M "
              f"per_flush_p50_wall_us="
              f"{fs['wall_s']/max(1,fs['n'])*1e6:8.0f} "
              f"flush_MB_s={fs['bytes']/max(1e-9,fs['wall_s'])/2**20:7.1f} "
              f"proc_cpu={cpu_s:7.1f}s cpu_per_GiB_in={cpu_s/ (U/GIB) if U else 0:6.2f} "
              f"devWR={dw/1e9:5.2f}GB devRD={dr/1e9:4.2f}GB devWA={dw/U if U else 0:5.2f} "
              f"U={U/GIB:5.2f}GiB")


def cmd_isol0():
    for name in ("k8narrow", "k8wide", "k8narrow_sc1", "k16wide", "k16wide_sc1"):
        d = os.path.join(OUT_I, f"bn_isol0_{name}")
        if not os.path.isdir(d):
            continue
        drain_pref = None
        for fn in os.listdir(d):
            if fn.endswith(".drain.samples.csv"):
                drain_pref = os.path.join(d, fn[: -len(".samples.csv")])
        if not drain_pref:
            print(f"{name}: no drain samples")
            continue
        comp = load_csv(drain_pref + ".compactions.csv")
        sm = load_csv(drain_pref + ".samples.csv")
        # actual time-to-quiesce: first t with L0==0 and pending==0 (drain start = t of
        # first compaction job or first sample with running compactions)
        t_start = None
        t_quiet = None
        for r in sm:
            l0 = num(r["num-files-at-level0"]); pend = num(r["estimate-pending-compaction-bytes"])
            if t_start is None and (num(r["num-running-compactions"]) > 0):
                t_start = num(r["elapsed"])
            if t_start is not None and l0 == 0 and pend == 0:
                t_quiet = num(r["elapsed"]); break
        tquiesce = (t_quiet - t_start) if (t_start is not None and t_quiet is not None) else float("nan")

        def classify(j):
            wall = num(j["elapsed_us"]) / 1e6
            start = int(num(j["base_input_level"])); outl = int(num(j["output_level"]))
            b0 = num(j["in_bytes_0"])      # slot0 = bytes AT start level (real)
            b1 = num(j["in_bytes_1"])      # slot1 = bytes AT output level (real)
            if wall < 0.01:                # trivial move: no bytes rewritten
                return "move", b0, b1, 0.0, 0.0
            if start == 0 and outl > 0:
                return "l0merge", b0, b1, wall, num(j["cpu_us"]) / 1e6
            if outl == 0:
                return "intraL0", b0, 0.0, wall, num(j["cpu_us"]) / 1e6
            return f"deep{start}->{outl}", b0, b1, wall, num(j["cpu_us"]) / 1e6

        agg = defaultdict(lambda: [0, 0.0, 0.0, 0.0, 0.0])  # n, in0, inb, wall, cpu
        for j in comp:
            c, in0, inb, wall, cpu = classify(j)
            a = agg[c]; a[0] += 1; a[1] += in0; a[2] += inb; a[3] += wall; a[4] += cpu
        nsub = sum(1 for e in load_csv(drain_pref + ".events.csv")
                   if e["kind"] == "subcompaction_begin")
        parts = " | ".join(
            f"{c}:n={a[0]} startIn={a[1]/GIB:.2f}G outIn={a[2]/GIB:.2f}G wall={a[3]:.1f}s cpu={a[4]:.1f}s"
            for c, a in sorted(agg.items()))
        print(f"{name:14s} t_quiesce={tquiesce:5.1f}s jobs={len(comp):3d} subs={nsub:3d} :: {parts}")


def cmd_ledger(run_dirs):
    print("%-22s %8s %6s %7s %7s %7s %7s %7s %6s %6s" % (
        "run", "U(GiB)", "WA", "flush", "intraL0", "L0->base", "deep", "totSST", "jobs", "triv"))
    for d in run_dirs:
        try:
            r = Run(d)
        except SystemExit:
            continue
        S = r.summary
        U = num(S.get("U_bytes", 0))
        if not U:
            continue
        fs = r.flush_stats(); js = r.job_stats()
        flush_g = fs["bytes"] / GIB
        intra = js["out_intraL0"] / GIB
        l0b = js["out_L0base"] / GIB
        deep = js["out_deep"] / GIB
        tot = flush_g + intra + l0b + deep
        print("%-22s %8.2f %6.2f %7.3f %7.3f %7.3f %7.3f %7.2f %6d %6d" % (
            r.rid, U / GIB, tot * GIB / U,
            flush_g * GIB / U, intra * GIB / U, l0b * GIB / U, deep * GIB / U,
            tot, js["n"], js["n_trivial"]))



def cmd_report():
    """Full campaign report: per-arm aggregate + effect vs default."""
    def get(x, *path):
        v = x
        for p in path:
            if not isinstance(v, dict):
                return None
            v = v.get(p)
        return v

    groups = defaultdict(list)
    for d in sorted(os.listdir(OUT_I)):
        p = os.path.join(OUT_I, d)
        if not os.path.isdir(p) or not d.startswith("bn_"):
            continue
        rid = find_prefix(p)[0]
        a = arm_name(rid or "")
        if a:
            try:
                groups[a].append(queue_report(p, verbose=False))
            except Exception as e:
                print(f"# skip {rid}: {e}")
    base = [x["throughput"] for x in groups.get("default", []) if x["throughput"]]
    if not base:
        print("no completed default-arm runs yet")
        return
    bm = sum(base) / len(base)

    def agg(reps, path, fmt="{:.1f}"):
        v = [get(x, *path) for x in reps]
        v = [x for x in v if x is not None and not (isinstance(x, float) and math.isnan(x))]
        return fmt.format(sum(v) / len(v)) if v else "n/a"

    hdr = ["arm", "n", "thr", "+-sd", "ratio", "[min-max]/def", "pendG", "L0", "imm",
           "stall%", "immL", "compU%", "flU%", "devB%", "devWA", "t_arr"]
    print(("%-10s %2s %7s %7s %5s %-13s %6s %5s %5s %6s %5s %6s %5s %5s %5s %6s") % tuple(hdr))
    for a, reps in sorted(groups.items()):
        tv = [x["throughput"] for x in reps if x["throughput"]]
        if not tv:
            continue
        m = sum(tv) / len(tv)
        sd = (math.sqrt(sum((v - m) ** 2 for v in tv) / max(1, len(tv) - 1))
              if len(tv) > 1 else 0.0)
        tarr = [x.get("t_work_end") for x in reps if x.get("t_work_end") is not None]
        print(("%-10s %2d %7.0f %7.0f %5.2f %-13s %6s %5s %5s %6s %5d %6s %5s %5s %5s %6s") % (
            a, len(reps), m, sd, m / bm,
            "[%4.2f-%4.2f]" % (min(tv) / bm, max(tv) / bm),
            agg(reps, ("ss_median", "pending_gib")),
            agg(reps, ("ss_median", "l0_files")),
            agg(reps, ("ss_median", "imm"), "{:.2f}"),
            agg(reps, ("stalls", "union_pct")),
            sum(x["stalls"]["lines"].get("imm", 0) for x in reps),
            agg(reps, ("util", "comp_pool_arr_pct"), "{:.0f}"),
            agg(reps, ("util", "flush_pool_arr_pct"), "{:.0f}"),
            agg(reps, ("service", "dev_busy_pct"), "{:.0f}"),
            agg(reps, ("device_wa",), "{:.2f}"),
            "{:.0f}".format(sum(tarr) / len(tarr)) if tarr else "n/a"))




def cmd_criteria():
    """计划 §四 支持条件的逐条对照：以 default 臂为参照，输出每个扫描臂的判据证据行。"""
    groups = defaultdict(list)
    for d in sorted(os.listdir(OUT_I)):
        p = os.path.join(OUT_I, d)
        if not os.path.isdir(p) or not d.startswith("bn_"):
            continue
        rid = find_prefix(p)[0]
        a = arm_name(rid or "")
        if a:
            try:
                groups[a].append(queue_report(p, verbose=False))
            except Exception:
                pass
    if not groups.get("default"):
        print("no default arm")
        return

    def m(reps, fn):
        vals = [fn(x) for x in reps]
        vals = [v for v in vals if v is not None and not (isinstance(v, float) and math.isnan(v))]
        return (sum(vals) / len(vals)) if vals else float("nan")

    def row(name, reps):
        def arr_rate(x):
            ops = x.get("ops_ok") or 0
            t = x.get("t_work_end") or 0
            return ops / t if (ops and t) else float("nan")
        return dict(
            n=len(reps),
            thr=m(reps, lambda x: x["throughput"]),
            arr=m(reps, arr_rate),
            t_arr=m(reps, lambda x: x.get("t_work_end") or 0),
            pend=m(reps, lambda x: x["ss_median"]["pending_gib"]),
            l0f=m(reps, lambda x: x["ss_median"]["l0_files"]),
            imm=m(reps, lambda x: x["ss_median"]["imm"]),
            stall=m(reps, lambda x: x["stalls"]["union_pct"]),
            immL=m(reps, lambda x: x["stalls"]["lines"].get("imm", 0)),
            pendL=m(reps, lambda x: x["stalls"]["lines"].get("pending", 0)),
            l0L=m(reps, lambda x: x["stalls"]["lines"].get("l0files", 0)),
            cU=m(reps, lambda x: x["util"]["comp_pool_arr_pct"]),
            fU=m(reps, lambda x: x["util"]["flush_pool_arr_pct"]),
            devB=m(reps, lambda x: x["service"]["dev_busy_pct"] if x["service"]["dev_busy_pct"] is not None else float("nan")),
            wa=m(reps, lambda x: x["device_wa"]),
            p99=m(reps, lambda x: x.get("p99_e2e_us") or float("nan")),
        )
    rows = {a: row(a, reps) for a, reps in groups.items() if reps}
    base = rows["default"]

    def onset_med(reps, key):
        vals = [x["onset"].get(key) for x in reps]
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else None
    print("onset ordering (median sustained-crossing seconds; None=never):")
    keys = ["imm>=2 (10s)", "imm>=3 (10s)", "L0>=trigger8 (10s)", "pending>=16G", "pending>=32G", "pending>=64G(soft)"]
    print("%-10s " % "arm" + " ".join("%12s" % k.split()[0].split(">=")[0][:6] for k in keys))
    for a in sorted(rows):
        reps = groups[a]
        cells = []
        for k in keys:
            v = onset_med(reps, k)
            cells.append("%12s" % ("%.0f" % v if v is not None else "-"))
        print("%-10s " % a + " ".join(cells))
    print("(columns: imm2 imm3 L0 pend16 pend32 pend64; '-': never crossed sustained)")
    print()
    hdr = ("%-10s %2s %8s %8s %6s %8s %5s %5s %6s %5s %5s %5s %5s %5s %5s %6s %7s"
           % ("arm", "n", "thr", "arr/s", "t_arr", "pendG", "L0", "imm", "stall%",
              "immL", "pendL", "l0L", "cU%", "fU%", "devB%", "devWA", "p99us"))
    print(hdr)
    for a in sorted(rows):
        r = rows[a]
        print("%-10s %2d %8.0f %8.0f %6.1f %8.1f %5.1f %5.2f %6.1f %5.0f %5.0f %5.0f %5.0f %5.0f %5.0f %6.2f %7.0f" % (
            a, r["n"], r["thr"], r["arr"], r["t_arr"], r["pend"], r["l0f"], r["imm"], r["stall"],
            r["immL"], r["pendL"], r["l0L"], r["cU"], r["fU"], r["devB"], r["wa"], r["p99"]))
    print()
    print("flush-scan criteria (计划 §四.1): f1 vs default —— imm 队列/MemTable stop 应恶化; f4 —— 应好转且吞吐升")
    for a in ("f1", "f4"):
        if a in rows:
            r = rows[a]
            print(f"  {a}: thr x{r['thr']/base['thr']:.3f}  imm {base['imm']:.2f}->{r['imm']:.2f}  "
                  f"immL {base['immL']:.0f}->{r['immL']:.0f}  fU {base['fU']:.0f}%->{r['fU']:.0f}%  "
                  f"stall {base['stall']:.1f}%->{r['stall']:.1f}%")
    print("compaction-scan criteria (计划 §四.2/四.3): c2 —— L0/债务应先涨、随后 imm 堆积; c12 —— 应好转")
    for a in ("c2", "c12"):
        if a in rows:
            r = rows[a]
            print(f"  {a}: thr x{r['thr']/base['thr']:.3f}  L0 {base['l0f']:.1f}->{r['l0f']:.1f}  "
                  f"pend {base['pend']:.1f}->{r['pend']:.1f}  stall {base['stall']:.1f}%->{r['stall']:.1f}%  "
                  f"cU {base['cU']:.0f}%->{r['cU']:.0f}%  devB {base['devB']:.0f}%->{r['devB']:.0f}%")




def cmd_paired(ref="default"):
    """Blocked (paired) analysis: within each rep, ratio arm/ref, then mean over reps.
    All arms of a rep run in the same interleaved time window, so machine-state drift
    (page cache, device temperature) largely cancels in the ratio."""
    per_rep = defaultdict(dict)   # arm -> rep -> metrics
    for d in sorted(os.listdir(OUT_I)):
        p = os.path.join(OUT_I, d)
        if not os.path.isdir(p) or not d.startswith("bn_"):
            continue
        rid = find_prefix(p)[0] or ""
        mm = re.match(r"bn_(.+)_r(\d+)$", rid)
        if not mm:
            continue
        arm, rep = mm.group(1), int(mm.group(2))
        try:
            r = queue_report(p, verbose=False)
        except Exception:
            continue
        ops = r.get("ops_ok") or 0
        t = r.get("t_work_end") or 0
        if not (ops and t):
            continue
        per_rep[arm][rep] = dict(
            arr=ops / t, thr=r["throughput"], imm=r["ss_median"]["imm"],
            l0=r["ss_median"]["l0_files"], pend=r["ss_median"]["pending_gib"],
            stall=r["stalls"]["union_pct"], immL=r["stalls"]["lines"].get("imm", 0),
            cU=r["util"]["comp_pool_arr_pct"], fU=r["util"]["flush_pool_arr_pct"],
            devB=r["service"]["dev_busy_pct"], wa=r["device_wa"],
            fq=r["flush_queue"]["p50_ms"], t=t)
    refmap = per_rep.get(ref, {})
    if not refmap:
        print(f"no reference arm '{ref}'"); return
    print(f"reference arm = {ref}; per-rep ratio arm/ref (arrival-phase throughput)")
    print("%-11s %3s %-34s %8s %8s %8s %8s" % ("arm", "n", "per-rep ratios", "mean", "sd", "min", "max"))
    for a in sorted(per_rep):
        ratios, pairs = [], []
        for rep, m in sorted(per_rep[a].items()):
            if rep in refmap and refmap[rep]["arr"]:
                rr = m["arr"] / refmap[rep]["arr"]
                ratios.append(rr); pairs.append(f"r{rep}:{rr:.2f}")
        if not ratios:
            continue
        mean = sum(ratios) / len(ratios)
        sd = (math.sqrt(sum((x - mean) ** 2 for x in ratios) / (len(ratios) - 1))
              if len(ratios) > 1 else 0.0)
        print("%-11s %3d %-34s %8.3f %8.3f %8.3f %8.3f" % (
            a, len(ratios), " ".join(pairs), mean, sd, min(ratios), max(ratios)))
    print()
    print("paired queue/stall comparison (arm value / ref value, mean over reps):")
    print("%-11s %10s %10s %10s %10s %10s %10s" % ("arm", "imm", "L0", "pend", "stall%", "flushQ_ms", "compU%"))
    for a in sorted(per_rep):
        cells = []
        for k in ("imm", "l0", "pend", "stall", "fq", "cU"):
            rt = []
            for rep, m in per_rep[a].items():
                if rep in refmap and refmap[rep][k]:
                    rt.append(m[k] / refmap[rep][k])
            cells.append(("%10.2f" % (sum(rt) / len(rt))) if rt else "%10s" % "-")
        print("%-11s %s" % (a, " ".join(cells)))




def cmd_levels(run_dirs):
    """Per (start_level -> output_level) job breakdown: count, input bytes at each
    side, output bytes, wall, cpu, subcompactions. Serves plan 四.3 deep-stage metrics."""
    for d in run_dirs:
        try:
            r = Run(d)
        except SystemExit:
            continue
        agg = defaultdict(lambda: dict(n=0, in0=0, in1=0, out=0, wall=0.0, cpu=0.0, triv=0))
        for j in r.compactions:
            k = (int(num(j["base_input_level"])), int(num(j["output_level"])))
            a = agg[k]
            a["n"] += 1
            a["in0"] += num(j["in_bytes_0"]); a["in1"] += num(j["in_bytes_1"])
            a["out"] += num(j["output_bytes"])
            a["wall"] += num(j["elapsed_us"]) / 1e6
            a["cpu"] += num(j["cpu_us"]) / 1e6
            if num(j["elapsed_us"]) < 1000:
                a["triv"] += 1
        print(f"== {r.rid}  F={r.F} C={r.C} time={r.samples[-1]['elapsed'] if r.samples else 0}s")
        print("   %-9s %6s %10s %10s %10s %9s %9s %6s" % (
            "start->out", "jobs", "in@start", "in@out", "out_bytes", "wall_s", "cpu_s", "triv"))
        for k in sorted(agg):
            a = agg[k]
            print("   %-9s %6d %10.2f %10.2f %10.2f %9.1f %9.1f %6d" % (
                f"L{k[0]}->L{k[1]}", a["n"], a["in0"] / GIB, a["in1"] / GIB, a["out"] / GIB,
                a["wall"], a["cpu"], a["triv"]))
        # device traffic in the arrival phase (service capacity the device delivered)
        rp = queue_report(d, verbose=False)
        print("   device: wr %.0f MB/s + rd %.0f MB/s, busy %.1f%%, devWA %.2f | arr %.0f ops/s, t_arr %.0fs" % (
            rp["service"]["dev_wr_MB_s"], rp["service"]["dev_rd_MB_s"],
            rp["service"]["dev_busy_pct"] or float("nan"), rp["device_wa"],
            (rp.get("ops_ok") or 0) / (rp.get("t_work_end") or 1), rp.get("t_work_end") or 0))
        print()




def cmd_decisive():
    """Decisive-intervention comparison: noc / deep (nl5,nl4) / devload vs default.
    Prints the two physical invariants (work volume, retirement rate) plus throughput,
    so the causal reading does not depend on the noisy arrival-phase metric alone."""
    def collect(pattern):
        out = []
        for d in sorted(os.listdir(OUT_I)):
            if not os.path.isdir(os.path.join(OUT_I, d)) or not re.match(pattern, d):
                continue
            try:
                r = Run(os.path.join(OUT_I, d))
            except Exception:
                continue
            if not r.summary:
                continue
            dur = num(r.summary.get("duration_s", 0))
            if not dur:
                continue
            js = r.job_stats(); fs = r.flush_stats()
            U = num(r.summary.get("U_bytes", 0))
            dw = num(r.summary.get("device_write_bytes", 0))
            rp = queue_report(os.path.join(OUT_I, d), verbose=False)
            out.append(dict(rid=r.rid, dur=dur, U_rate=(U / 1e9) / dur * 1000,
                            devWR_gB=dw / 1e9, devWA=dw / U if U else float("nan"),
                            sst_rate=(js["out"] + fs["bytes"]) / 2**20 / dur,
                            n_jobs=js["n"], deep=js["n_deep"],
                            deep_gB=js["out_deep"] / GIB, l0base_gB=js["out_L0base"] / GIB,
                            stall=rp["stalls"]["union_pct"],
                            stalls_tot=sum(rp["stalls"]["lines"].values()),
                            busy=rp["service"]["dev_busy_pct"]))
        return out

    def line(tag, rs):
        if not rs:
            print(f"{tag:<12} (no runs yet)"); return
        n = len(rs)
        def mean(k, f="{:.2f}"):
            v = [x[k] for x in rs if x[k] is not None and not (isinstance(x[k], float) and math.isnan(x[k]))]
            return f.format(sum(v) / len(v)) if v else "n/a"
        print("%-12s n=%-2d U_rate=%6s MB/s dur=%6s s devWR=%6s GB devWA=%5s "
              "sstRetire=%6s MB/s jobs=%-5s deep=%-5s deepOut=%6s GiB L0baseOut=%5s GiB "
              "stallUnion=%5s%% stallLines=%-5s devBusy=%5s%%" % (
                  tag, n, mean("U_rate", "{:.1f}"), mean("dur", "{:.0f}"), mean("devWR_gB", "{:.1f}"),
                  mean("devWA"), mean("sst_rate", "{:.0f}"), mean("n_jobs", "{:.0f}"),
                  mean("deep", "{:.0f}"), mean("deep_gB"), mean("l0base_gB"),
                  mean("stall", "{:.1f}"), mean("stalls_tot", "{:.0f}"), mean("busy", "{:.1f}")))

    print("=== decisive interventions (physical invariants + throughput) ===")
    line("default", collect(r"bn_default_r\d+$"))
    line("noc", collect(r"bn_noc_r\d+$"))
    line("geom mc8g", collect(r"bn_mc8g_r\d+$"))
    line("geom mult20", collect(r"bn_mult20_r\d+$"))
    line("devload2", collect(r"bn_devload2_r\d+$"))
    line("zipf", collect(r"bn_zipf_r\d+$"))
    print()
    print("reading: work volume (devWR GB / devWA) is the invariant that capacity knobs do NOT change;")
    print("         noc should collapse devWA toward ~1 and drop job counts;")
    print("         deep should cut deepOut/job counts at equal U;")
    print("         devload should cut U_rate without changing devWA.")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "arm"
    if cmd == "queue":
        for d in sys.argv[2:]:
            queue_report(d)
    elif cmd == "ledger":
        cmd_ledger(sys.argv[2:])
    elif cmd == "arm":
        cmd_arm(sys.argv[2] if len(sys.argv) > 2 else None)
    elif cmd == "matrix":
        cmd_matrix()
    elif cmd == "report":
        cmd_report()
    elif cmd == "criteria":
        cmd_criteria()
    elif cmd == "paired":
        cmd_paired(sys.argv[2] if len(sys.argv) > 2 else "default")
    elif cmd == "levels":
        cmd_levels(sys.argv[2:])
    elif cmd == "decisive":
        cmd_decisive()
    elif cmd == "isoflush":
        cmd_isoflush()
    elif cmd == "isol0":
        cmd_isol0()
    else:
        print(__doc__)
        sys.exit(2)
