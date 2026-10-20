#!/usr/bin/env python3
"""Analysis for the L0 investigation campaign (native RocksDB).

Reads the outputs written by bench/l0bench.cc (and, where useful, the DB LOG via
bench/logparser.py) and produces the quantities each experiment in the plan asks for.

Every number this script prints is traceable to a named input file; where a quantity
cannot be measured from the captured data it is reported as `n/a` rather than guessed.

Usage:
  analyze.py ledger   <rundir> [<rundir> ...]     E1 write-flow ledger (per run)
  analyze.py timeline <rundir>                    E5 stall/queueing time series
  analyze.py overlap  <rundir>                    E2 L0 range-overlap O(k)
  analyze.py readcost <rundir>                    E3 per-request probe/IO cost
  analyze.py parallel <rundir>                    E4 job/subcompaction concurrency
  analyze.py memory   <rundir>                    E7 memory vs recent-layer size
  analyze.py batch    <rundir> ...                E8 batching amortisation across arms
  analyze.py space    <rundir>                    E9 space accounting
  analyze.py recovery <rundir>                    E10 recovery boundary
  analyze.py compare  <rundir> ...                write-throughput comparison across runs
  analyze.py all      <rundir>                    everything applicable
"""
import csv
import json
import math
import os
import statistics
import sys

MAXL = 7
LEVELS = list(range(MAXL))


# ---------------------------------------------------------------------------
# loading
# ---------------------------------------------------------------------------
def rundir_paths(d):
    """A run directory may contain several l0bench prefixes; discover them."""
    if os.path.isdir(d):
        names = sorted(f[:-len('.summary.json')] for f in os.listdir(d)
                       if f.endswith('.summary.json'))
        pref = [os.path.join(d, n) for n in names]
        if not pref:
            # allow a stage dir: recurse one level
            sub = []
            for e in sorted(os.listdir(d)):
                p = os.path.join(d, e)
                if os.path.isdir(p):
                    sub += rundir_paths(p)
            return sub
        return pref
    if d.endswith('.json'):
        return [d[:-len('.summary.json')]]
    return [d]


def load_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline='') as f:
        return list(csv.DictReader(f))


def num(x, default=0.0):
    if x is None or x == '':
        return default
    try:
        v = float(x)
        return v
    except ValueError:
        return default


def inum(x, default=-1):
    v = num(x, None)
    return default if v is None else int(v)


class Run:
    def __init__(self, prefix):
        self.prefix = prefix
        self.dir = os.path.dirname(prefix)
        self.name = os.path.basename(prefix)
        self.summary = {}
        if os.path.exists(prefix + '.summary.json'):
            with open(prefix + '.summary.json') as f:
                self.summary = json.load(f)
        self.config = {}
        txt = prefix + '.config.txt'
        if os.path.exists(txt):
            for line in open(txt):
                if line.startswith('argv:'):
                    self.config['argv'] = line[5:].strip().split()
        self.effective = {}
        eff = prefix + '.effective.txt'
        if os.path.exists(eff):
            for line in open(eff):
                if '=' in line:
                    k, v = line.strip().split('=', 1)
                    self.effective[k] = v
        self.samples = load_csv(prefix + '.samples.csv')
        self.persec = load_csv(prefix + '.per_sec.csv')
        self.compactions = load_csv(prefix + '.compactions.csv')
        self.events = load_csv(prefix + '.events.csv')
        self.event_files = load_csv(prefix + '.event_files.csv')
        self.layout = load_csv(prefix + '.layout_files.csv')
        self.layout_levels = load_csv(prefix + '.layout_levels.csv')
        self.dirscan = load_csv(prefix + '.dirscan.csv')
        self.detail = load_csv(prefix + '.detail.csv')
        self.reads = load_csv(prefix + '.read_keys.csv')
        self.stalls = load_csv(prefix + '.stalls.csv')
        self.extra = load_csv(prefix + '.summary_extra.csv')

    # -- convenience -------------------------------------------------------
    @property
    def U(self):
        """Application-confirmed logical write bytes (key+value)."""
        return num(self.summary.get('U_bytes'))

    def ticker_series(self, name):
        out = []
        for r in self.samples:
            if name in r:
                out.append((num(r['elapsed']), num(r[name])))
        return out

    def ticker_delta(self, name):
        s = self.ticker_series(name)
        if len(s) < 2:
            return None
        return s[-1][1] - s[0][1]

    def prop_series(self, name):
        col = name.replace('rocksdb.', '')
        out = []
        for r in self.samples:
            if col in r:
                v = num(r[col], -1)
                out.append((num(r['elapsed']), v))
        return out

    def device_write_bytes(self):
        s = self.ticker_series('dev_wr_sectors')
        if len(s) < 2:
            return None
        return (s[-1][1] - s[0][1]) * 512.0

    def device_read_bytes(self):
        s = self.ticker_series('dev_rd_sectors')
        if len(s) < 2:
            return None
        return (s[-1][1] - s[0][1]) * 512.0

    def peak_sst_alloc(self):
        if not self.dirscan:
            return 0.0
        return max(num(r['sst_alloc_bytes']) for r in self.dirscan)

    def peak_l0_files(self):
        s = self.prop_series('rocksdb.num-files-at-level0')
        return max([v for _, v in s if v >= 0], default=0)

    def classify_jobs(self):
        """Return dict class -> list of job rows (E1/E2)."""
        out = {'intra_L0': [], 'L0_to_base': [], 'deep': [], 'other': []}
        for j in self.compactions:
            inl = [l for l in LEVELS if inum(j[f'in_files_{l}']) > 0]
            olev = inum(j['output_level'])
            if 0 in inl:
                cls = 'intra_L0' if olev == 0 else 'L0_to_base'
            else:
                cls = 'deep' if olev > 0 else 'other'
            out[cls].append(j)
        return out


# ---------------------------------------------------------------------------
# E1 -- write-flow ledger
# ---------------------------------------------------------------------------
def fmt_gb(b):
    return f"{b/2**30:.3f} GiB" if b is not None else "n/a"


def ledger(runs, quiet=False):
    rows = []
    for r in runs:
        U = r.U
        # --- output bytes: SST file bytes actually created (OnTableFileCreated) ---
        created_total = sum(num(f['data_size']) for f in r.event_files if f['dir'] == 'created')
        # job id -> class
        cls_of = {}
        for cls, jobs in r.classify_jobs().items():
            for j in jobs:
                cls_of[inum(j['job'])] = cls
        # flush jobs come from the flush_installed events
        flush_jobs = {inum(e['job']) for e in r.events if e['kind'] == 'flush_installed'}
        W_flush_created = sum(num(f['data_size']) for f in r.event_files
                              if f['dir'] == 'created' and inum(f['job']) in flush_jobs)
        out_by_cls = {'intra_L0': 0.0, 'L0_to_base': 0.0, 'deep': 0.0, 'other': 0.0}
        for j in r.compactions:
            inl = [l for l in LEVELS if inum(j[f'in_files_{l}']) > 0]
            olev = inum(j['output_level'])
            cls = ('intra_L0' if olev == 0 else 'L0_to_base') if 0 in inl else ('deep' if olev > 0 else 'other')
            out_by_cls[cls] += num(j['output_bytes'])      # data-block bytes from table properties
        # same split using created file bytes (join by job)
        out_by_cls_file = {'intra_L0': 0.0, 'L0_to_base': 0.0, 'deep': 0.0, 'other': 0.0}
        for f in r.event_files:
            if f['dir'] != 'created':
                continue
            jb = inum(f['job'])
            if jb in flush_jobs:
                continue
            out_by_cls_file[cls_of.get(jb, 'other')] += num(f['data_size'])
        # trivial moves: jobs that produced no table file. Their 'output_bytes' in the
        # job table is the *size of the moved files*, which must never be counted as a
        # rewrite (the plan calls this out explicitly).
        created_by_job = {}
        for f in r.event_files:
            if f['dir'] == 'created':
                created_by_job[inum(f['job'])] = created_by_job.get(inum(f['job']), 0.0) + num(f['data_size'])
        triv = [j for j in r.compactions if created_by_job.get(inum(j['job']), 0.0) == 0.0]
        trivial_moved_bytes = sum(num(j['output_bytes']) for j in triv)
        # compaction input bytes per source level
        in_by_lvl = {l: sum(num(j[f"in_bytes_{l}"]) for j in r.compactions) for l in LEVELS}
        # --- independent cross-checks: statistics counters and the DB LOG ---
        st_flush = r.ticker_delta('flush_write_bytes')
        st_compact = r.ticker_delta('compact_write_bytes')
        st_compact_read = r.ticker_delta('compact_read_bytes')
        st_wal = r.ticker_delta('wal_file_bytes')
        dev_w = r.device_write_bytes()
        dev_r = r.device_read_bytes()
        struct_w = (W_flush_created + sum(out_by_cls_file.values()))
        row = {
            'run': r.name,
            'mode': r.summary.get('mode'),
            'duration_s': num(r.summary.get('duration_s')),
            'ops_ok': num(r.summary.get('ops_ok')),
            'writes': num(r.summary.get('writes_ok')),
            'reads': num(r.summary.get('reads_issued')),
            'U_bytes': U,
            'U_gib': U / 2**30,
            'wal_bytes_stat': st_wal,
            'W_flush_created': W_flush_created,
            'W_flush_stat': st_flush,
            'W_intraL0': out_by_cls_file['intra_L0'],
            'W_L0base': out_by_cls_file['L0_to_base'],
            'W_deep': out_by_cls_file['deep'],
            'W_other': out_by_cls_file['other'],
            'W_compact_total': sum(out_by_cls_file.values()),
            'W_compact_stat': st_compact,
            'sst_written_struct': struct_w,
            'in_bytes_L0': in_by_lvl[0],
            'in_bytes_L1': in_by_lvl[1],
            'in_bytes_L2plus': sum(in_by_lvl[l] for l in LEVELS if l >= 2),
            'compact_read_stat': st_compact_read,
            'device_write_bytes': dev_w,
            'device_read_bytes': dev_r,
            'n_flushes': num(r.summary.get('flush_installed')),
            'n_compactions': num(r.summary.get('compaction_installed')),
            'peak_l0_files': r.peak_l0_files(),
            'n_intra_L0': len(r.classify_jobs()['intra_L0']),
            'n_L0_to_base': len(r.classify_jobs()['L0_to_base']),
            'n_deep': len(r.classify_jobs()['deep']),
            'n_trivial_moves': len(triv),
            'trivial_moved_bytes': trivial_moved_bytes,
            'trivial_share_of_job_output': (trivial_moved_bytes /
                                            max(1.0, trivial_moved_bytes + sum(out_by_cls.values()))),
        }
        # write amplification ratios (all anchored to U)
        def ratio(x):
            return (x / U) if (x is not None and U > 0) else None
        row['WA_sst'] = ratio(struct_w)
        row['WA_engine_files'] = ratio((st_wal or 0) + struct_w)
        row['WA_device'] = ratio(dev_w)
        row['WA_wal'] = ratio(st_wal)
        row['WA_flush'] = ratio(W_flush_created)
        row['WA_intraL0'] = ratio(out_by_cls_file['intra_L0'])
        row['WA_L0base'] = ratio(out_by_cls_file['L0_to_base'])
        row['WA_deep'] = ratio(out_by_cls_file['deep'])
        row['share_flush'] = (W_flush_created / struct_w) if struct_w else None
        row['share_intraL0'] = (out_by_cls_file['intra_L0'] / struct_w) if struct_w else None
        row['share_L0base'] = (out_by_cls_file['L0_to_base'] / struct_w) if struct_w else None
        row['share_deep'] = (out_by_cls_file['deep'] / struct_w) if struct_w else None
        # CPU accounting from the compaction job table
        row['compact_cpu_s'] = sum(num(j['cpu_us']) for j in r.compactions) / 1e6
        row['compact_wall_s_files'] = sum((num(j['ts_end']) - num(j['ts_begin'])) for j in r.compactions)
        row['compact_wall_s_summed'] = sum(num(j['elapsed_us']) for j in r.compactions) / 1e6
        row['compact_file_write_s'] = sum(num(j['file_write_ns']) for j in r.compactions) / 1e9
        row['compact_fsync_s'] = sum(num(j['file_fsync_ns']) for j in r.compactions) / 1e9
        row['records_in'] = sum(num(j['num_input_records']) for j in r.compactions)
        row['records_out'] = sum(num(j['num_output_records']) for j in r.compactions)
        row['records_replaced'] = sum(num(j['num_records_replaced']) for j in r.compactions)
        row['skipped_input_bytes'] = sum(num(j['skipped_input_bytes']) for j in r.compactions)
        rows.append(row)

    if not quiet:
        hdr = ['run', 'mode', 'U_gib', 'WA_sst', 'WA_device', 'WA_wal', 'WA_flush',
               'WA_intraL0', 'WA_L0base', 'WA_deep', 'share_flush', 'share_intraL0',
               'share_L0base', 'share_deep', 'peak_l0_files', 'n_flushes',
               'n_intra_L0', 'n_L0_to_base', 'n_deep', 'n_trivial_moves']
        print('== E1 write-flow ledger (all ratios are bytes / application-confirmed byte U) ==')
        print(','.join(hdr))
        for row in rows:
            def f(k, p=3):
                v = row.get(k)
                return f"{v:.{p}f}" if isinstance(v, (int, float)) else 'n/a'
            print(','.join([str(row['run']), str(row['mode']), f('U_gib', 2), f('WA_sst'),
                            f('WA_device'), f('WA_wal'), f('WA_flush'), f('WA_intraL0'),
                            f('WA_L0base'), f('WA_deep'), f('share_flush'), f('share_intraL0'),
                            f('share_L0base'), f('share_deep'), str(int(row['peak_l0_files'])),
                            str(int(row['n_flushes'])), str(row['n_intra_L0']),
                            str(row['n_L0_to_base']), str(row['n_deep'])]))
        print()
        print('== cross-checks (same quantity from independent sources) ==')
        print('run,flush_listener_GiB,flush_stat_GiB,rel_diff,compact_listener_GiB,compact_stat_GiB,rel_diff,'
              'wall_sum_s,cpu_sum_s,cpu_per_wall,file_write_s,fsync_s,records_replaced,skipped_GiB,'
              'trivial_moves,trivial_moved_GiB')
        for row in rows:
            def rel(a, b):
                if not a or not b:
                    return 'n/a'
                return f"{(a-b)/max(a,b):.4f}"
            print(','.join([
                str(row['run']),
                f"{row['W_flush_created']/2**30:.3f}", f"{(row['W_flush_stat'] or 0)/2**30:.3f}",
                rel(row['W_flush_created'], row['W_flush_stat']),
                f"{row['W_compact_total']/2**30:.3f}", f"{(row['W_compact_stat'] or 0)/2**30:.3f}",
                rel(row['W_compact_total'], row['W_compact_stat']),
                f"{row['compact_wall_s_summed']:.1f}", f"{row['compact_cpu_s']:.1f}",
                f"{(row['compact_cpu_s']/row['compact_wall_s_summed'] if row['compact_wall_s_summed'] else 0):.3f}",
                f"{row['compact_file_write_s']:.1f}", f"{row['compact_fsync_s']:.1f}",
                f"{row['records_replaced']:.0f}", f"{row['skipped_input_bytes']/2**30:.3f}",
                str(int(row['n_trivial_moves'])), f"{row['trivial_moved_bytes']/2**30:.3f}"]))
        print()
    return rows


# ---------------------------------------------------------------------------
# E5 -- timeline / stall causality
# ---------------------------------------------------------------------------
def timeline(runs, quiet=False):
    summary = []
    for r in runs:
        peaks = {
            'peak_l0_files': r.peak_l0_files(),
            'peak_immutable': max([v for _, v in r.prop_series('rocksdb.num-immutable-mem-table')], default=0),
            'peak_pending_gib': max([v for _, v in r.prop_series('rocksdb.estimate-pending-compaction-bytes')], default=0) / 2**30,
            'peak_memtable_gib': max([v for _, v in r.prop_series('rocksdb.size-all-mem-tables')], default=0) / 2**30,
        }
        stall_us = r.ticker_delta('stall_micros')
        dur = num(r.summary.get('duration_s'))
        stoppers = [e for e in r.stalls if e['condition'] == 'stopped']
        delayed = [e for e in r.stalls if e['condition'] == 'delayed']
        # Union duration of the intervals in which the CF was in a delayed/stopped state.
        # This is the plan's "区间并集持续时间"; stall_micros alone is thread-aggregated
        # and must be divided by the writer-thread count before being called a fraction
        # of wall time.
        ev = sorted((num(e['ts']), 1 if e['condition'] in ('delayed', 'stopped') else 0)
                    for e in r.stalls)
        union, n_int, start = 0.0, 0, None
        for ts, kind in ev:
            if kind == 1 and start is None:
                start = ts
            elif kind == 0 and start is not None:
                union += ts - start
                n_int += 1
                start = None
        # fraction of sampler ticks with L0 at/above the slowdown-relevant trigger
        l0 = [(e, v) for e, v in r.prop_series('rocksdb.num-files-at-level0') if v >= 0]
        trig = inum(r.effective.get('level0_file_num_compaction_trigger'), 8)
        slow = inum(r.effective.get('level0_slowdown_writes_trigger'), 20)
        above = (sum(1 for _, v in l0 if v >= trig) / len(l0)) if l0 else 0.0
        above_slow = (sum(1 for _, v in l0 if v >= slow) / len(l0)) if l0 else 0.0
        row = {
            'run': r.name, 'mode': r.summary.get('mode'), 'duration_s': dur,
            'throughput': num(r.summary.get('throughput_ops_s')),
            'stall_us_total': stall_us,
            'stall_thread_time_over_wall': (stall_us / 1e6 / dur) if (stall_us and dur) else None,
            'stall_union_s': union,
            'stall_union_frac_of_wall': (union / dur) if dur else None,
            'stall_intervals': n_int,
            'stall_us_per_write': (stall_us / max(1.0, num(r.summary.get('writes_ok')))) if stall_us else None,
            'stall_begin_events': num(r.summary.get('stall_begin_events')),
            'n_delayed_events': len(delayed), 'n_stopped_events': len(stoppers),
            'frac_ticks_l0_ge_trigger': above,
            'frac_ticks_l0_ge_slowdown': above_slow,
            'l0_trigger': trig,
            'p50_l0_files': statistics.median([v for _, v in l0]) if l0 else 0,
            'first_delayed_ts': (num(delayed[0]['ts']) if delayed else None),
            'first_stopped_ts': (num(stoppers[0]['ts']) if stoppers else None),
            **peaks,
        }
        if r.persec:
            row['eng_p50_us'] = statistics.median([num(x['eng_p50_us']) for x in r.persec])
            row['eng_p99_us'] = statistics.median([num(x['eng_p99_us']) for x in r.persec])
            row['eng_p999_us'] = statistics.median([num(x['eng_p999_us']) for x in r.persec])
            row['worst_sec_p99_us'] = max(num(x['eng_p99_us']) for x in r.persec)
        if r.extra:
            for e in r.extra:
                if e['name'] == 'total_hist':
                    row['run_p50_us'] = num(e['p50_ns']) / 1000
                    row['run_p99_us'] = num(e['p99_ns']) / 1000
                    row['run_p999_us'] = num(e['p999_ns']) / 1000
                    row['run_p9999_us'] = num(e['p9999_ns']) / 1000
                    row['run_max_us'] = num(e['max_ns']) / 1000
                if e['name'] == 'e2e_hist':
                    row['e2e_p99_us'] = num(e['p99_ns']) / 1000
        summary.append(row)
    if not quiet:
        print('== E5 timeline summary ==')
        keys = ['run', 'mode', 'duration_s', 'throughput', 'stall_us_total',
                'stall_thread_time_over_wall', 'stall_union_s', 'stall_union_frac_of_wall',
                'stall_intervals', 'stall_us_per_write',
                'stall_begin_events', 'n_delayed_events', 'n_stopped_events',
                'l0_trigger', 'p50_l0_files', 'frac_ticks_l0_ge_trigger',
                'frac_ticks_l0_ge_slowdown', 'peak_l0_files', 'peak_immutable',
                'peak_pending_gib', 'peak_memtable_gib', 'run_p50_us', 'run_p99_us',
                'run_p999_us', 'run_p9999_us', 'run_max_us']
        print(','.join(keys))
        for row in summary:
            out = []
            for k in keys:
                v = row.get(k)
                out.append(f"{v:.3f}" if isinstance(v, float) else (str(v) if v is not None else 'n/a'))
            print(','.join(out))
        print()
    return summary


# ---------------------------------------------------------------------------
# E2 -- range overlap O(k)
# ---------------------------------------------------------------------------
def _rng(f):
    """Decode a 16-byte key hex string into (tag, id)."""
    try:
        h = f['smallest_hex']
        lo = int(h[16:32], 16)
        h2 = f['largest_hex']
        hi = int(h2[16:32], 16)
        return lo, hi
    except Exception:
        return None, None


def overlap(runs, quiet=False):
    out = {}
    for r in runs:
        # take the last full layout snapshot before the read phase, per level
        if not r.layout:
            continue
        ts_all = sorted({f['ts'] for f in r.layout}, key=lambda x: num(x))
        res = []
        for ts in ts_all:
            files_l0 = [f for f in r.layout if f['ts'] == ts and f['level'] == '0']
            files_l1 = [f for f in r.layout if f['ts'] == ts and f['level'] == '1']
            if not files_l0:
                continue
            ranges0 = [_rng(f) for f in files_l0]
            ranges0 = [(a, b) for a, b in ranges0 if a is not None]
            if not ranges0:
                continue
            # sample keys uniformly over the L0-covered span and count covering files
            lo = min(a for a, _ in ranges0)
            hi = max(b for _, b in ranges0)
            n = 2000
            step = max(1, (hi - lo) // n)
            counts = []
            for k in range(lo, hi + 1, step):
                counts.append(sum(1 for a, b in ranges0 if a <= k <= b))
            res.append({
                'ts': num(ts), 'n_l0': len(ranges0),
                'l0_bytes': sum(num(f['file_size']) for f in files_l0),
                'n_l1': len(files_l1),
                'mean_ok': statistics.mean(counts) if counts else 0,
                'max_ok': max(counts) if counts else 0,
                'p50_ok': statistics.median(counts) if counts else 0,
                'p95_ok': sorted(counts)[int(0.95 * (len(counts) - 1))] if counts else 0,
                'frac_ok_ge_2': (sum(1 for c in counts if c >= 2) / len(counts)) if counts else 0,
            })
        out[r.name] = res
    if not quiet:
        print('== E2 L0 range overlap O(k) over time (k = number of L0 files whose range covers a key) ==')
        for name, res in out.items():
            if not res:
                continue
            print(f"-- {name}")
            print('ts,n_l0,l0_GiB,mean_ok,max_ok,p95_ok,frac_ok_ge_2')
            for x in res[::max(1, len(res)//12)]:
                print(f"{x['ts']:.0f},{x['n_l0']},{x['l0_bytes']/2**30:.3f},{x['mean_ok']:.2f},"
                      f"{x['max_ok']},{x['p95_ok']},{x['frac_ok_ge_2']:.3f}")
        print()
    return out


def overlap_vs_reads(runs, quiet=False):
    """Join sampled read keys with the nearest layout snapshot: the O(k) the request
    actually faced, split by read component and outcome."""
    rows = []
    for r in runs:
        if not r.reads or not r.layout:
            continue
        snaps = {}
        for f in r.layout:
            if f['level'] != '0':
                continue
            snaps.setdefault(f['ts'], []).append(_rng(f))
        ts_sorted = sorted(snaps.keys(), key=lambda x: num(x))
        if not ts_sorted:
            continue
        def snap_at(t):
            lo, hi = 0, len(ts_sorted) - 1
            best = ts_sorted[0]
            while lo <= hi:
                mid = (lo + hi) // 2
                if num(ts_sorted[mid]) <= t:
                    best = ts_sorted[mid]; lo = mid + 1
                else:
                    hi = mid - 1
            return snaps[best]
        agg = {}
        for x in r.reads:
            t = num(x['ts'])
            k = int(x['key_id'])
            comp = x['component']
            rngs = [(a, b) for a, b in snap_at(t) if a is not None]
            ok = sum(1 for a, b in rngs if a <= k <= b)
            key = (comp, x['status'], min(ok, 20))
            agg[key] = agg.get(key, 0) + 1
        for (comp, status, ok), c in sorted(agg.items()):
            rows.append({'run': r.name, 'component': comp, 'status': status, 'ok': ok, 'n': c})
    if not quiet:
        print('== E2/E3 analytic O(k) for the keys actually read (nearest layout snapshot) ==')
        print('run,component,status,O(k)=capped20,count')
        for x in rows:
            print(f"{x['run']},{x['component']},{x['status']},{x['ok']},{x['n']}")
        print()
    return rows


# ---------------------------------------------------------------------------
# E3 -- read cost per request
# ---------------------------------------------------------------------------
def readcost(runs, quiet=False):
    out = []
    for r in runs:
        rows = [d for d in r.detail if d['op'] in ('read', 'mixread')]
        if not rows:
            continue
        by = {}
        for d in rows:
            key = (d['status'], d['op'])
            by.setdefault(key, []).append(d)
        for (status, op), drs in by.items():
            def mean(col):
                return statistics.mean(num(x.get(col)) for x in drs)
            lat = [ (num(x['done_ns']) - num(x['submit_ns'])) / 1000.0 for x in drs ]
            lat.sort()
            e2e = [ (num(x['done_ns']) - num(x['sched_ns'])) / 1000.0 for x in drs if num(x['sched_ns']) > 0 ]
            e2e.sort()
            rec = {
                'run': r.name, 'status': status, 'n': len(drs),
                'mean_block_reads': mean('block_read_count'),
                'mean_block_read_bytes': mean('block_read_byte'),
                'mean_cache_hits': mean('block_cache_hit'),
                'mean_filter_reads': mean('filter_block_read'),
                'mean_index_reads': mean('index_block_read'),
                'mean_bloom_sst_hit': mean('bloom_sst_hit'),
                'mean_bloom_sst_miss': mean('bloom_sst_miss'),
                'mean_memtable_probes': mean('get_from_memtable_count'),
                'mean_internal_skipped': mean('internal_key_skipped'),
                'mean_user_key_cmp': mean('user_key_cmp'),
                'p50_us': lat[len(lat)//2] if lat else None,
                'p99_us': lat[int(0.99*(len(lat)-1))] if lat else None,
                'p999_us': lat[int(0.999*(len(lat)-1))] if lat else None,
                'mean_us': statistics.mean(lat) if lat else None,
                'e2e_p99_us': e2e[int(0.99*(len(e2e)-1))] if e2e else None,
            }
            rec['mean_device_reads_if_all_miss'] = rec['mean_block_reads']
            out.append(rec)
    if not quiet:
        print('== E3 per-request read cost (PerfContext deltas, sampled ops) ==')
        keys = ['run', 'status', 'n', 'mean_block_reads', 'mean_block_read_bytes',
                'mean_cache_hits', 'mean_filter_reads', 'mean_index_reads',
                'mean_bloom_sst_hit', 'mean_bloom_sst_miss', 'mean_memtable_probes',
                'mean_internal_skipped', 'mean_user_key_cmp', 'p50_us', 'mean_us',
                'p99_us', 'p999_us']
        print(','.join(keys))
        for x in out:
            print(','.join(
                f"{x[k]:.3f}" if isinstance(x.get(k), float) else str(x.get(k)) for k in keys))
        print()
    return out


# ---------------------------------------------------------------------------
# E4 -- parallelism
# ---------------------------------------------------------------------------
def parallel(runs, quiet=False):
    out = []
    for r in runs:
        if not r.compactions:
            continue
        # job concurrency: sweep event timestamps
        ivs = sorted(((num(j['ts_begin']), num(j['ts_end']), inum(j['job']),
                       [l for l in LEVELS if inum(j[f'in_files_{l}']) > 0], inum(j['output_level']))
                      for j in r.compactions))
        events = []
        for a, b, jb, inl, ol in ivs:
            events.append((a, 1)); events.append((b, -1))
        events.sort()
        cur = 0; peak = 0; area = 0.0; last = None
        samples = []
        for t, d in events:
            if last is not None:
                area += cur * (t - last)
            cur += d
            peak = max(peak, cur)
            last = t
            samples.append((t, cur))
        span = (ivs[-1][1] - ivs[0][0]) if ivs else 0
        # subcompaction events
        sub_begin = [e for e in r.events if e['kind'] == 'subcompaction_begin']
        sub_end = [e for e in r.events if e['kind'] == 'subcompaction_end']
        # overlap between L0-involving jobs and deep jobs in time
        l0_jobs = [(a, b) for a, b, jb, inl, ol in ivs if 0 in inl]
        deep_jobs = [(a, b) for a, b, jb, inl, ol in ivs if 0 not in inl]
        def overlap_area(A, B):
            tot = 0.0
            for a1, b1 in A:
                for a2, b2 in B:
                    tot += max(0.0, min(b1, b2) - max(a1, a2))
            return tot
        L0_deep_overlap = overlap_area(l0_jobs, deep_jobs)
        out.append({
            'run': r.name,
            'n_jobs': len(ivs),
            'peak_concurrent_jobs': peak,
            'mean_concurrent_jobs': (area / span) if span else None,
            'wall_span_s': span,
            'sum_job_wall_s': sum(num(j['elapsed_us']) for j in r.compactions) / 1e6,
            'effective_parallelism': (sum(num(j['elapsed_us']) for j in r.compactions) / 1e6 / span) if span else None,
            'n_l0_jobs': len(l0_jobs),
            'l0_job_wall_s': sum(b - a for a, b in l0_jobs),
            'n_deep_jobs': len(deep_jobs),
            'deep_job_wall_s': sum(b - a for a, b in deep_jobs),
            'l0_deep_time_overlap_s': L0_deep_overlap,
            'n_subcompaction_begin': len(sub_begin),
            'n_subcompaction_end': len(sub_end),
            'mean_subcompaction_us': statistics.mean([num(e['wall_us']) for e in sub_end]) if sub_end else None,
            'max_subcompactions_seen': max([num(e['num_input_files']) for e in sub_end], default=0),
            'n_deep_while_l0_active': None,
        })
        # how often was a deep job running while L0 had >=1 job active
        cnt = 0
        for a, b, jb, inl, ol in ivs:
            if 0 in inl:
                continue
            if any(max(a1, a) < min(b1, b) for a1, b1 in l0_jobs):
                cnt += 1
        out[-1]['n_deep_while_l0_active'] = cnt
    if not quiet:
        print('== E4 background parallelism ==')
        keys = ['run', 'n_jobs', 'peak_concurrent_jobs', 'mean_concurrent_jobs',
                'sum_job_wall_s', 'wall_span_s', 'effective_parallelism', 'n_l0_jobs',
                'l0_job_wall_s', 'n_deep_jobs', 'deep_job_wall_s', 'l0_deep_time_overlap_s',
                'n_deep_while_l0_active', 'n_subcompaction_begin', 'n_subcompaction_end']
        print(','.join(keys))
        for x in out:
            print(','.join(f"{x[k]:.3f}" if isinstance(x.get(k), float) else str(x.get(k)) for k in keys))
        print()
    return out


# ---------------------------------------------------------------------------
# E4b -- range conflict between concurrently compacted files
# ---------------------------------------------------------------------------
def range_conflict(runs, quiet=False):
    """From the layout snapshots: among the files marked being_compacted at the same
    instant and in the same level, how often do their key ranges overlap? Overlapping
    inputs are what forces compaction jobs to serialise even when job slots are free.
    (Only snapshots that caught files in flight are informative; coverage is reported.)"""
    out = []
    for r in runs:
        if not r.layout:
            continue
        by_ts = {}
        for f in r.layout:
            by_ts.setdefault(f['ts'], []).append(f)
        n_snap = len(by_ts)
        n_snap_with_busy = 0
        pairs = 0
        overlap_pairs = 0
        busy_counts = []
        for ts, files in by_ts.items():
            busy = [f for f in files if f['being_compacted'] == '1']
            if busy:
                n_snap_with_busy += 1
            busy_counts.append(len(busy))
            for lvl in sorted({f['level'] for f in busy}):
                fl = [f for f in busy if f['level'] == lvl]
                rngs = [_rng(f) for f in fl]
                rngs = [(a, b) for a, b in rngs if a is not None]
                for i in range(len(rngs)):
                    for j in range(i + 1, len(rngs)):
                        pairs += 1
                        a1, b1 = rngs[i]
                        a2, b2 = rngs[j]
                        if max(a1, a2) <= min(b1, b2):
                            overlap_pairs += 1
        out.append({
            'run': r.name, 'n_snapshots': n_snap,
            'frac_snapshots_with_busy': (n_snap_with_busy / n_snap) if n_snap else None,
            'max_busy_files': max(busy_counts) if busy_counts else 0,
            'mean_busy_files': statistics.mean(busy_counts) if busy_counts else 0,
            'concurrent_busy_pairs': pairs,
            'overlapping_pairs': overlap_pairs,
            'overlap_pair_frac': (overlap_pairs / pairs) if pairs else None,
        })
    if not quiet:
        print('== E4b concurrent-compaction range conflict (from being_compacted flags) ==')
        keys = ['run', 'n_snapshots', 'frac_snapshots_with_busy', 'max_busy_files',
                'mean_busy_files', 'concurrent_busy_pairs', 'overlapping_pairs',
                'overlap_pair_frac']
        print(','.join(keys))
        for x in out:
            print(','.join(f"{x[k]:.4f}" if isinstance(x.get(k), float) else str(x.get(k)) for k in keys))
        print()
    return out


# ---------------------------------------------------------------------------
# E7 -- memory vs recent-layer size
# ---------------------------------------------------------------------------
def memory(runs, quiet=False):
    """E7: resident memory vs the size of the recent (L0) layer.

    `l0_bytes` is a proxy: the number of L0 files in the 1 s property sample times the
    mean L0 file size observed in the layout samples for the same run. It is labelled a
    proxy because the property sampler and the layout sampler are not synchronised; the
    per-file layout table is the exact source and is used directly in the backlog runs.
    """
    out = []
    for r in runs:
        if not r.samples:
            continue
        l0_sizes = [num(f['file_size']) for f in r.layout if f['level'] == '0']
        l0_file_bytes = (sum(l0_sizes) / len(l0_sizes)) if l0_sizes else 0.0
        pts = []
        for s in r.samples:
            l0 = num(s.get('num-files-at-level0'), -1)
            if l0 < 0:
                continue
            tr = num(s.get('estimate-table-readers-mem'), -1)
            bc = num(s.get('block-cache-usage'), -1)
            pts.append({
                'elapsed': num(s['elapsed']),
                'l0_files': l0,
                'l0_bytes': l0 * l0_file_bytes,
                'rss_mib': num(s['rss_bytes']) / 2**20,
                'memtable_mib': num(s.get('size-all-mem-tables'), -1) / 2**20,
                'immutable': num(s.get('num-immutable-mem-table')),
                'table_readers_mib': (tr / 2**20) if tr >= 0 else None,
                'block_cache_mib': (bc / 2**20) if bc >= 0 else None,
            })
        if not pts:
            continue
        curve = []
        for b in sorted({min(p['l0_files'], 24) for p in pts}):
            g = [p for p in pts if min(p['l0_files'], 24) == b]
            def med(col):
                v = [x[col] for x in g if x[col] is not None]
                return statistics.median(v) if v else None
            curve.append({
                'l0_files': b, 'n': len(g),
                'rss_mib_median': med('rss_mib'),
                'memtable_mib_median': med('memtable_mib'),
                'table_readers_mib_median': med('table_readers_mib'),
                'block_cache_mib_median': med('block_cache_mib'),
            })
        # least-squares slope of memory against L0 bytes (per GiB of L0)
        slopes = {}
        for key, col in (('rss_mib_per_l0_gib', 'rss_mib'),
                         ('readers_mib_per_l0_gib', 'table_readers_mib')):
            xs = [p['l0_bytes'] / 2**30 for p in pts if p[col] is not None]
            ys = [p[col] for p in pts if p[col] is not None]
            if len(xs) < 3 or max(xs) == min(xs):
                slopes[key] = None
                continue
            mx = sum(xs) / len(xs); my = sum(ys) / len(ys)
            den = sum((x - mx) ** 2 for x in xs)
            slopes[key] = (sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den) if den else None
        out.append({'run': r.name, 'curve': curve,
                    'mean_l0_file_mib': l0_file_bytes / 2**20,
                    'max_l0_files_observed': max(p['l0_files'] for p in pts),
                    'max_l0_gib_observed': max(p['l0_bytes'] for p in pts) / 2**30,
                    'rss_at_zero_l0_mib': (curve[0]['rss_mib_median']
                                           if curve and curve[0]['l0_files'] == 0 else None),
                    **slopes})
    if not quiet:
        print('== E7 resident memory vs recent-layer size ==')
        for x in out:
            print(f"-- {x['run']}: mean L0 file {x['mean_l0_file_mib']:.1f} MiB, "
                  f"max L0 {x['max_l0_files_observed']:.0f} files / {x['max_l0_gib_observed']:.2f} GiB, "
                  f"RSS slope {x['rss_mib_per_l0_gib']} MiB/GiB, "
                  f"table-readers slope {x['readers_mib_per_l0_gib']} MiB/GiB")
            print('l0_files,n,rss_mib,memtable_mib,table_readers_mib,block_cache_mib')
            for c in x['curve']:
                def g(k):
                    v = c[k]
                    return f"{v:.1f}" if isinstance(v, float) else 'n/a'
                print(f"{c['l0_files']:.0f},{c['n']},{g('rss_mib_median')},{g('memtable_mib_median')},"
                      f"{g('table_readers_mib_median')},{g('block_cache_mib_median')}")
        print()
    return out


# ---------------------------------------------------------------------------
# E8 -- batching amortisation
# ---------------------------------------------------------------------------
def batch(runs, quiet=False):
    rows = []
    for r in runs:
        jobs = r.classify_jobs()
        l0_jobs = jobs['L0_to_base']
        if not l0_jobs:
            continue
        trig = inum(r.effective.get('level0_file_num_compaction_trigger'), -1)
        in_l0 = sum(num(j['in_bytes_0']) for j in l0_jobs)
        in_base = sum(num(j[f'in_bytes_{l}']) for j in l0_jobs for l in range(1, MAXL))
        out_b = sum(num(j['output_bytes']) for j in l0_jobs)
        rows.append({
            'run': r.name,
            'trigger': trig,
            'n_L0base_jobs': len(l0_jobs),
            'files_per_job_mean': statistics.mean([num(j['in_files_0']) for j in l0_jobs]),
            'in_L0_GiB': in_l0 / 2**30,
            'in_base_GiB': in_base / 2**30,
            'out_GiB': out_b / 2**30,
            'base_rewrite_per_L0_input': (in_base / in_l0) if in_l0 else None,
            'out_per_L0_input': (out_b / in_l0) if in_l0 else None,
            'records_in': sum(num(j['num_input_records']) for j in l0_jobs),
            'records_out': sum(num(j['num_output_records']) for j in l0_jobs),
            'records_replaced': sum(num(j['num_records_replaced']) for j in l0_jobs),
            'version_drop_rate': (sum(num(j['num_records_replaced']) for j in l0_jobs) /
                                  max(1.0, sum(num(j['num_input_records']) for j in l0_jobs))),
            'cpu_s': sum(num(j['cpu_us']) for j in l0_jobs) / 1e6,
            'U_GiB': r.U / 2**30,
            'WA_L0base': (out_b / r.U) if r.U else None,
        })
    if not quiet:
        print('== E8 L0->base batching: cost per unit of newly arrived L0 byte ==')
        keys = ['run', 'trigger', 'n_L0base_jobs', 'files_per_job_mean', 'in_L0_GiB',
                'in_base_GiB', 'out_GiB', 'base_rewrite_per_L0_input', 'out_per_L0_input',
                'records_in', 'records_out', 'records_replaced', 'version_drop_rate', 'cpu_s']
        print(','.join(keys))
        for x in rows:
            print(','.join(f"{x[k]:.4f}" if isinstance(x.get(k), float) else str(x.get(k)) for k in keys))
        print()
    return rows


# ---------------------------------------------------------------------------
# E8b -- within-run batch effect (immune to cross-arm threshold differences)
# ---------------------------------------------------------------------------
def batch_within(runs, quiet=False):
    """Bucket L0->base jobs by how many L0 files they pulled in, and report the base bytes
    rewritten per byte of L0 input for each bucket. Because every arm uses its own trigger,
    this within-run view is the part of the batching question that is not confounded by the
    slowdown/stop thresholds moving with the trigger."""
    out = []
    for r in runs:
        jobs = r.classify_jobs()['L0_to_base']
        if not jobs:
            continue
        buckets = {}
        for j in jobs:
            k = inum(j['in_files_0'])
            if k <= 0:
                continue
            inb = sum(num(j[f'in_bytes_{l}']) for l in range(1, MAXL))
            b = min(k, 16)
            d = buckets.setdefault(b, {'n': 0, 'l0': 0.0, 'base': 0.0, 'out': 0.0,
                                       'repl': 0.0, 'inrec': 0.0})
            d['n'] += 1
            d['l0'] += num(j['in_bytes_0'])
            d['base'] += inb
            d['out'] += num(j['output_bytes'])
            d['repl'] += num(j['num_records_replaced'])
            d['inrec'] += num(j['num_input_records'])
        for b in sorted(buckets):
            d = buckets[b]
            out.append({'run': r.name, 'batch_L0_files': b, 'n_jobs': d['n'],
                        'base_per_L0': (d['base'] / d['l0']) if d['l0'] else None,
                        'out_per_L0': (d['out'] / d['l0']) if d['l0'] else None,
                        'version_drop': (d['repl'] / d['inrec']) if d['inrec'] else None})
    if not quiet:
        print('== E8b within-run batch effect (base bytes rewritten per L0 input byte, by batch size) ==')
        print('run,batch_L0_files,n_jobs,base_per_L0,out_per_L0,version_drop')
        for x in out:
            def f(k, p=4):
                v = x[k]
                return f"{v:.{p}f}" if isinstance(v, float) else 'n/a'
            print(f"{x['run']},{x['batch_L0_files']},{x['n_jobs']},{f('base_per_L0')},{f('out_per_L0')},{f('version_drop')}")
        print()
    return out


# ---------------------------------------------------------------------------
# E9 -- space
# ---------------------------------------------------------------------------
def space(runs, quiet=False):
    out = []
    for r in runs:
        if not r.dirscan:
            continue
        peak_sst = max(num(x['sst_bytes']) for x in r.dirscan)
        peak_alloc = max(num(x['sst_alloc_bytes']) for x in r.dirscan)
        peak_wal = max(num(x['wal_bytes']) for x in r.dirscan)
        peak_total = max(num(x['db_total_bytes']) for x in r.dirscan)
        final_live = 0
        if r.layout_levels:
            ts = max(f['ts'] for f in r.layout_levels)
            final_live = sum(num(f['total_bytes']) for f in r.layout_levels if f['ts'] == ts)
        out.append({
            'run': r.name, 'mode': r.summary.get('mode'),
            'U_GiB': r.U / 2**30,
            'peak_sst_GiB': peak_sst / 2**30,
            'peak_sst_alloc_GiB': peak_alloc / 2**30,
            'peak_wal_GiB': peak_wal / 2**30,
            'peak_db_total_GiB': peak_total / 2**30,
            'final_live_sst_GiB': final_live / 2**30,
            'space_amp_peak_over_live': (peak_alloc / final_live) if final_live else None,
            'wal_over_sst_at_peak': (peak_wal / peak_sst) if peak_sst else None,
            'peak_n_sst': max(num(x['n_sst']) for x in r.dirscan),
            'rsst_peak_mib': max((num(s['rss_bytes']) / 2**20) for s in r.samples) if r.samples else 0,
        })
    if not quiet:
        print('== E9 space accounting ==')
        keys = ['run', 'mode', 'U_GiB', 'peak_sst_GiB', 'peak_sst_alloc_GiB', 'peak_wal_GiB',
                'peak_db_total_GiB', 'final_live_sst_GiB', 'space_amp_peak_over_live',
                'wal_over_sst_at_peak', 'peak_n_sst', 'rsst_peak_mib']
        print(','.join(keys))
        for x in out:
            print(','.join(f"{x[k]:.3f}" if isinstance(x.get(k), float) else str(x.get(k)) for k in keys))
        print()
    return out


# ---------------------------------------------------------------------------
# E10 -- recovery
# ---------------------------------------------------------------------------
def recovery(runs, quiet=False):
    rows = []
    for r in runs:
        d = r.dir
        pre = os.path.join(d, 'recovery_pre.txt')
        pre_wal_n = pre_wal_b = None
        if os.path.exists(pre):
            lines = [l.strip() for l in open(pre) if l.strip()]
            for i, l in enumerate(lines):
                if l.isdigit():
                    pre_wal_n = int(l)
                elif l.endswith('total'):
                    pre_wal_b = num(l.split()[0])
        open_s = None
        for e in r.extra:
            if e['name'] == 'open_seconds':
                open_s = num(e['p1_ns'] if 'p1_ns' in e else e.get('p50_ns'))
        # reopen log
        reopen = os.path.join(d, r.name.replace('.reopen', '') + '.reopen.log')
        reopen_txt = open(reopen).read() if os.path.exists(reopen) else ''
        rows.append({
            'run': r.name,
            'killed_at_ops': num(r.summary.get('killed_at_ops')),
            'wal_files_before_recovery': pre_wal_n,
            'wal_bytes_before_recovery': pre_wal_b,
            'open_seconds': open_s,
            'reopen_line': reopen_txt.strip().split('\n')[0] if reopen_txt else None,
        })
    if not quiet:
        print('== E10 recovery ==')
        for x in rows:
            print(x)
        print()
    return rows


# ---------------------------------------------------------------------------
# compare
# ---------------------------------------------------------------------------
def compare(runs, quiet=False):
    rows = []
    for r in runs:
        row = {
            'run': r.name, 'mode': r.summary.get('mode'),
            'throughput_ops_s': num(r.summary.get('throughput_ops_s')),
            'duration_s': num(r.summary.get('duration_s')),
            'ops_ok': num(r.summary.get('ops_ok')),
            'U_GiB': r.U / 2**30,
            'writes': num(r.summary.get('writes_ok')),
            'reads': num(r.summary.get('reads_issued')),
            'read_hit_rate': (num(r.summary.get('reads_hit')) / num(r.summary.get('reads_issued')))
                             if num(r.summary.get('reads_issued')) else None,
            'device_write_GiB': (r.device_write_bytes() or 0) / 2**30,
        }
        if r.extra:
            for e in r.extra:
                if e['name'] == 'total_hist':
                    row['p50_us'] = num(e['p50_ns']) / 1000
                    row['p99_us'] = num(e['p99_ns']) / 1000
                    row['p999_us'] = num(e['p999_ns']) / 1000
                    row['max_us'] = num(e['max_ns']) / 1000
                if e['name'] == 'e2e_hist':
                    row['e2e_p99_us'] = num(e['p99_ns']) / 1000
        rows.append(row)
    if not quiet:
        print('== run comparison ==')
        keys = ['run', 'mode', 'throughput_ops_s', 'duration_s', 'writes', 'reads',
                'read_hit_rate', 'U_GiB', 'device_write_GiB', 'p50_us', 'p99_us',
                'p999_us', 'max_us', 'e2e_p99_us']
        print(','.join(keys))
        for x in rows:
            out = []
            for k in keys:
                v = x.get(k)
                if isinstance(v, (int, float)):
                    out.append(f"{v:.4f}")
                elif v is None:
                    out.append('n/a')
                else:
                    out.append(str(v))
            print(','.join(out))
        print()
    return rows


# ---------------------------------------------------------------------------
def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    cmd = sys.argv[1]
    dirs = sys.argv[2:]
    runs = []
    for d in dirs:
        for p in rundir_paths(d):
            runs.append(Run(p))
    if not runs:
        print('no runs found', file=sys.stderr)
        return 1
    res = {}
    if cmd in ('ledger', 'all'):
        res['ledger'] = ledger(runs)
    if cmd in ('timeline', 'all'):
        res['timeline'] = timeline(runs)
    if cmd in ('overlap', 'all'):
        res['overlap'] = overlap(runs)
        res['overlap_vs_reads'] = overlap_vs_reads(runs)
    if cmd in ('readcost', 'all'):
        res['readcost'] = readcost(runs)
    if cmd in ('parallel', 'all'):
        res['parallel'] = parallel(runs)
    if cmd in ('memory', 'all'):
        res['memory'] = memory(runs)
    if cmd in ('conflict', 'all'):
        res['range_conflict'] = range_conflict(runs)
    if cmd in ('batch', 'all'):
        res['batch'] = batch(runs)
        res['batch_within'] = batch_within(runs)
    if cmd in ('space', 'all'):
        res['space'] = space(runs)
    if cmd in ('recovery', 'all'):
        res['recovery'] = recovery(runs)
    if cmd in ('compare', 'all'):
        res['compare'] = compare(runs)
    return 0


if __name__ == '__main__':
    sys.exit(main())
