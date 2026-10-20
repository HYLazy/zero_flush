# perf_v2 campaign: results

Server-side RocksDB measurement campaign, 2026-09-25 → 2026-09-30, on one host
(`embed-System-Product-Name`, 62 GiB RAM, NVMe `/home` partition, unmodified
`librocksdb.so` 11.2.0, repo HEAD `ff27eda`). Everything below is measured on this host;
nothing is extrapolated from another machine.

Nine experiment families were run. Each has its own analysis file next to its data:

| family | root | runs | validity | analysis |
|---|---|---:|---|---|
| P0 instrument acceptance | `p0/` | 16 | pass | `p0/P0_SUMMARY.md` |
| M1 pure write, offered-load fractions | `m1/` | 51 (+12 extended windows) | 0 invalid | `m1/REPORT.md`, `m1/REPORT_EXTENDED.md` |
| M2 pure point reads, 84 arms × 3 reps | `m2/` | 252 | 0 invalid | `m2/REPORT.md` |
| M3 mixed read/write at open-loop fractions | `m3/` | 39 | 0 invalid | `m3/REPORT.md` |
| M4 L0 geometry (K × narrow/wide × frozen/online) | `m4/` | 108 | 0 invalid | `m4/REPORT.md` |
| M5 threads / scans / block cache | `m5/` | 75 | 0 invalid | `m5/REPORT.md` |
| L0-only layout × Bloom | `layout_bloom/` | 174 | 138 valid, 36 invalid (re-run queued) | `layout_bloom/REPORT_FIRST.md`, `REPORT_EXTEND.md` |
| L0 backlog: layout × size × experiments A/B/C | `l0_backlog/` | 90 | 90 valid | `l0_backlog/REPORT.md` |
| L0 backlog: K sweep at B = 2 GiB | `l0_backlog_kscan_K*` | 100 | in progress | this file, filled when the sweep lands |

Latency percentiles are **bucket lower bounds** from the per-class cohort histograms
(`hist_version 2`); the bucket width is in the run's `hist.csv`. Throughput is the
measurement cohort's *service* rate (requests whose completion fell inside the window),
with censored/in-flight requests reported separately in every run's
`measure_phase.json`.

## What the campaign establishes

**1. The instrument measures what the runbook says it must, and the acceptance gate
holds.** P0 passes: the histogram encoder was rebuilt (`lower_bound_v2`) after the old
one was found to inflate buckets 2.0–3.0×; every run carries its own validity file and
the gate's checks (count conservation, cohort arithmetic, timestamp order,
no-conflicting-flags, no-background-interference, tool-unchanged) fail runs rather than
silently averaging them. P0's two "identical throughput" runs were explained from raw
monotonic bounds (relative difference 2.8e-8) instead of being assumed equal.

**2. The write path's knee is at ~0.75-0.90 of its closed-loop rate, and a 60 s window
hides it.** (M1, 51 runs + 12 extension windows; pure overwrite from the 47 GiB snapshot,
16 threads, one `monotonic_ns` window per run, no drain.) Closed loop: 82.3 K ops/s, but
that rate is not *sustainable*: 17 stall events in the 60 s window, write amplification
4.5×, p99 2.36 ms. Open loop, fraction of that closed-loop rate (first 60 s window):

| offered fraction | offered ops/s | achieved | achieved/offered | p99 µs | p99 / 3× low-load baseline | stalls | write amp |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.25 | 20 809 | 20 809 | 1.00 | 115 | 1.27 | 0 | 10.40 |
| 0.50 | 41 618 | 41 618 | 1.00 | 123 | 1.36 | 0 | 8.74 |
| 0.75 | 62 427 | 62 430 | 1.00 | 131 | 1.46 | 1 | 6.74 |
| 0.90 | 74 912 | 70 652 | 0.94 | 1 179 | **13.09** | 5 | 5.27 |
| 1.00 | 83 236 | 82 779 | 0.99 | 1 442 | **16.00** | 8 | 4.94 |
| 1.10 | 91 559 | 70 657 | 0.77 | 2 359 | **26.19** | 10 | 4.91 |

The knee rule (`p99 > 3× the low-load baseline`) is crossed between 0.75 and 0.90. The
300 s extension pass shows why the 60 s column is not enough: at **every** fraction from
0.75 up, the second half of a 300 s window delivers only 0.40–0.71× the first half's
throughput, p99 grows 10–21×, and pending bytes are still rising at ~0.22 GiB/s when the
window ends — the runs are not at steady state, they are inside a slow deterioration that
a short window cannot see. The `m1_noc10` ablation (auto-compaction disabled, same
offered rate as the closed loop) is the causal control: 82 336 ops/s achieved, p99
107 µs, **0 stalls**, write amp 1.0. Both the tail and the stalls belong to background
compaction, not to the foreground write path — and that ablation is a bound, not a
configuration (L0 then grows without limit).

**3. Distant point reads scale to 16 threads and stop; the block cache is consistently
worth something, but not enough to clear the pre-registered bar.** (M2, 252 runs, and M5,
75 runs.) Deep point reads (working set spread over the whole 47 GiB DB), medians of 3
reps, 8 GiB block cache:

| threads | 1 | 2 | 4 | 8 | 16 | 32 |
|---|---:|---:|---:|---:|---:|---:|
| deep, ops/s | 182 392 | 320 952 | 588 247 | 1 098 974 | 1 707 925 | 1 725 455 |
| hit p99 µs (recent mix) | 4.6 | 5.1 | 6.1 | 7.2 | 7.7 | 10.2 |

Sixteen threads is the knee: 32 threads adds 1 % throughput and a worse tail. The M5 get
arms reproduce the same shape (163 K → 1.75 M at 16 → 1.72 M at 32). Cache size moves
deep-read throughput the same way in both matrices — 8 GiB beats 512 MiB/64 MiB by
+41 % at 1 thread and +12…23 % at 16 threads — which is *below* the pre-registered 1.5×
rule, so it is recorded as **unresolved, consistently positive**, not as an effect.
Misses (keys that cannot exist) are cache-insensitive (1.29–1.30 M ops/s at 1 thread).
232 of 234 read arms served under 0.05 GiB from the device: these are CPU-bound numbers
on a page-cache-resident database (§8c of the implementation report).

**4. Mixed load is linear in the offered rate until the write tail breaks — and it breaks
at the write-heavy end only.** (M3, 39 runs.) At read shares ≥ 50 %, achieved equals
offered exactly (within 0.01 %) up to the full write-path capacity of 82 287 ops/s. At a
25 % read share the same offered rate yields 0.93× and Put p99 jumps 147 µs → 1 442 µs;
at 25 % and 75 % of capacity it still tracks. Put p99 by arm: 90–147 µs below the knee,
1.4 ms at it.

**5. L0 geometry does not matter at this working set; which keys are read does.** (M4,
108 runs.) K ∈ {1, 8, 16} files × narrow/wide × frozen compaction/online moves read
throughput by at most 1.03–1.13×, inside the within-arm spread of 1.10–1.17× → the
contrast is **unresolved**. The one resolved difference is *deep vs recent* key
selection: 1.70–1.73 M vs 1.90–1.92 M ops/s with hit p99 13.3 µs vs 7.2 µs — i.e. what
the query touches, not how the L0 files were grouped.

**6. When the whole working set is L0-resident, layout chooses between one Bloom probe
and sixteen — and that is worth 3.4×.** (L0-only layout × Bloom, 138 valid runs.) With
2 M keys entirely in L0:

| K=16 | checks/Get (in-range miss) | ops/s | p99 µs | CPU ns/Get | FPR at 10 bits |
|---|---:|---:|---:|---:|---:|
| narrow (contiguous blocks) | 1.000 | 982 K | 2.46 | 1 022 | 0.0096 |
| wide (i mod K) | 16.000 | 291 K | 6.55 | 3 320 | 0.0096 |

Sixteen-bit filters cut the false-positive rate to 0.0008 without changing the probe
count, and with Bloom disabled the wide layout collapses to 27 924 ns/Get. The read cost
is set by how many files a query's key range can match, and the two experiments together
bound where it matters: **L0-resident (Bl overlay) → 3.4×; L0 on top of a 46 GiB base
(M4) → unresolved.** The load-extension block of this family (threads 1/4/16, open-loop
rates, cache 512M/64M, `use_direct_reads` 0/1) is being re-run: its first pass was
rejected by the validity gate (24 arms carried a swept flag twice, and 12 direct-read
arms died on an unsupported flag — see the implementation report's defect list).

**7. Repaying an L0 backlog costs ~6–11 GiB of compaction work per GiB of backlog, and
the per-GiB cost falls as the backlog grows.** (L0 backlog, 90/90 valid.) Same data,
same value, only the grouping differs. Per-run medians over 5 reps:

| layout | B GiB | drain s | written GiB | written per GiB backlog | peak GiB |
|---|---:|---:|---:|---:|---:|
| narrow | 0.5 | 14 | 5.6 | 11.11 | 43.7 |
| narrow | 2 | 45 | 17.1 | 8.57 | 49.4 |
| narrow | 8 | 159 | 51.1 | 6.39 | 63.0 |
| wide | 0.5 | 14 | 5.0 | 10.07 | 43.8 |
| wide | 2 | 40 | 16.2 | 8.10 | 48.2 |
| wide | 8 | 142 | 52.1 | 6.51 | 61.8 |

Drain time and space both grow with B; work *per GiB* falls by ~40 % from 0.5 to 8 GiB.
In the frozen-read experiment the layout contrast is visible in the probe count exactly
as the L0-only experiment predicts (4.245 vs 19.244 checks/Get at K=16, i.e. the same 15
extra candidate files), and in the per-class latencies — but not in throughput, because on
this 46 GiB base the dominant class is device-bound. Medians over the 5 reps:

| layout | B GiB | queries answered from L0 | base hit p50 / p99 µs | in-range miss p50 / p99 µs | total ops/s |
|---|---:|---:|---:|---:|---:|
| narrow | 0.5 | 0.48 % | 32.8 / 180.2 | 7.7 / 49.2 | 28 424 |
| narrow | 2 | 1.87 % | 32.8 / 180.2 | 7.7 / 36.9 | 29 386 |
| narrow | 8 | 7.47 % | 26.6 / 147.5 | 6.7 / 36.9 | 42 910 |
| wide | 0.5 | 0.47 % | 28.7 / 196.6 | 12.3 / 36.9 | 36 528 |
| wide | 2 | 1.86 % | 41.0 / 229.4 | 15.4 / 180.2 | 23 702 |
| wide | 8 | 7.49 % | 45.1 / 327.7 | 18.4 / 294.9 | 19 349 |

**Read that table with the cache caveat below**: its numbers are medians over reps that
were not in the same cache state, so the *latencies* are regime mixtures. The across-K
and across-layout contrasts inside one sweep are unaffected (every K sees the same rep
structure), the absolute microseconds are not.

The base-hit class (keys answered from L1-L6) costs 29-45 µs at p50 — that is the device
and cold-memory cost, not the filter. The class whose cost the layout changes is the *cheap* one: in-range
misses never touch data, and wide's 15 extra probes double their p50 (7.7 → 12.3-18.4 µs,
p99 37 → 295 µs at B = 8 GiB). The total column therefore moves two ways at once: the
share of queries answered from the freshly-written, cached L0 rises with B (0.5 % → 7.5 %,
which speeds the average up for narrow) while wide pays a growing per-query filter bill,
and device reads dominate everything else. The claim the two experiments support jointly
is: the probe-count difference is real and measured in two independent ways; its
*throughput* consequence is large exactly when the answering data is L0-resident (family
6) and is masked when it is not.

**8. On this host a frozen-read number is a function of when it was measured.** The
protocol cannot drop the page cache (no root), and a 46 GiB database does not fit in a
62 GiB page cache alongside everything else, so a sweep *warms the cache as it runs*.
Measured directly, K = 1 frozen arm, five reps of the same arm:

| rep | 1 | 2 | 3 | 4 | 5 |
|---|---:|---:|---:|---:|---:|
| ops/s (narrow) | 15 568 | 17 210 | 19 476 | 35 061 | 104 574 |
| CPU ns/Get | 29 044 | 26 978 | 25 168 | 14 638 | 6 829 |
| device GiB read in the run | 3.5 | 3.2 | 2.8 | 2.5 | 1.5 |

The last rep is 6.7× faster than the first and reads 2.3× less from the device: the reps
are not exchangeable, and a median over five of them mixes a cold regime with a warm one.
Every analysis file now prints this trajectory (`A rep by rep`) so the regime is visible
per run instead of being averaged away. Consequences: (a) inside one sweep the effect
moves all arms together, which is why the K scan's across-K comparisons survive it; (b)
across matrices or across days the absolute numbers of frozen arms are **not** comparable
- which is also the honest reading of the first phase's A table; (c) the numbers that are
cache-insensitive (checks/Get, per-class *ratios*, device GiB per op) are the ones to
compare across campaigns.

**9. Everything above is instrument-versioned.** Each run records the tool's sha256 in
its own `config.txt`/manifest row, and the matrices were built by three revisions of
`bench/tools/l0bench` plus the two experiment-specific tools. The revision boundaries are
listed in the implementation report's §15; the only mid-matrix rebuild (backlog tool,
09-29 17:29) added two optional flags that no arm passed, which its per-run config
artifacts prove directly.

## What is not established

* **The 100 GB snapshot arms** (§4.2 base100) were built but the M-matrix arms were not
  re-run on it; the campaign's 100 GB claims would need ~11 h of machine time.
* **The layout/Bloom load-extension block**: re-run queued; its first pass is invalid.
* **The K sweep** (`l0_backlog_kscan_K*`): 100 runs, in progress; its frozen (A) and
  auto-compaction (B) groups are analysed separately and cross-checked against the
  K=16 arms the backlog matrix already ran (replication check).
* **Write-path scaling beyond 82 K ops/s** is where M1 stops; the campaign did not test
  whether the knee is a policy threshold or a device limit (that is the 50 GB campaign's
  subject, not this one).
