# L0 overlap x Bloom cost — results

Source `/home/embed/hyl/metadata_offload/bench/out/perf_v2/layout_bloom`, phase `first`. Percentiles are bucket lower bounds from each run's cohort histogram; CIs are over the run means (5 runs per group).

## Per-group metrics

| K | layout | bits | query | runs | checks/Get | FPR | cpu ns/Get | cycles/Get | insn/Get | p50 µs | p95 µs | p99 µs | ops/s | cache hits/Get | block reads/Get |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | narrow | 0 | hit | 5 | 0.000 | N/A | 3043.7 | 9131.8 | n/a | 2.71 | 3.23 | 3.58 | 317568 | 1.000 | 0.0000 |
| 1 | narrow | 0 | missing_inrange | 5 | 0.000 | N/A | 2998.1 | 8995.1 | n/a | 2.56 | 3.58 | 4.10 | 323120 | 1.250 | 0.0000 |
| 1 | narrow | 10 | hit | 5 | 1.000 | 1.0000 | 3291.0 | 9873.6 | n/a | 2.92 | 3.48 | 3.84 | 292845 | 1.000 | 0.0000 |
| 1 | narrow | 10 | missing_inrange | 5 | 1.000 | 0.0097 | 801.6 | 2404.8 | n/a | 0.56 | 0.61 | 2.15 | 1267875 | 0.012 | 0.0000 |
| 16 | narrow | 0 | hit | 5 | 0.000 | N/A | 3085.1 | 9256.1 | n/a | 2.71 | 3.28 | 3.58 | 313454 | 1.000 | 0.0000 |
| 16 | narrow | 0 | missing_inrange | 5 | 0.000 | N/A | 3110.5 | 9332.2 | n/a | 2.56 | 3.74 | 4.10 | 311419 | 1.250 | 0.0000 |
| 16 | narrow | 0 | missing_outrange | 5 | 0.000 | N/A | 792.9 | 2378.7 | n/a | 0.58 | 0.60 | 0.64 | 1279546 | 0.000 | 0.0000 |
| 16 | narrow | 10 | hit | 5 | 1.000 | 1.0000 | 3321.2 | 9964.2 | n/a | 2.92 | 3.53 | 3.89 | 290952 | 1.000 | 0.0000 |
| 16 | narrow | 10 | missing_inrange | 5 | 1.000 | 0.0098 | 1022.1 | 3066.4 | n/a | 0.78 | 0.84 | 2.46 | 982327 | 0.012 | 0.0000 |
| 16 | narrow | 10 | missing_outrange | 5 | 0.000 | N/A | 786.4 | 2359.3 | n/a | 0.58 | 0.61 | 0.65 | 1291838 | 0.000 | 0.0000 |
| 16 | wide | 0 | hit | 5 | 0.000 | N/A | 14942.8 | 44831.6 | n/a | 13.52 | 26.62 | 29.49 | 64121 | 8.495 | 0.0000 |
| 16 | wide | 0 | missing_inrange | 5 | 0.000 | N/A | 27924.1 | 83777.8 | n/a | 26.21 | 35.23 | 40.96 | 34811 | 17.189 | 0.0000 |
| 16 | wide | 0 | missing_outrange | 5 | 0.000 | N/A | 789.3 | 2367.9 | n/a | 0.58 | 0.60 | 0.65 | 1287786 | 0.000 | 0.0000 |
| 16 | wide | 10 | hit | 5 | 8.501 | 0.1261 | 4518.3 | 13555.9 | n/a | 4.04 | 6.04 | 7.37 | 212496 | 1.072 | 0.0000 |
| 16 | wide | 10 | missing_inrange | 5 | 16.000 | 0.0096 | 3319.9 | 9960.1 | n/a | 2.56 | 5.02 | 6.55 | 290551 | 0.166 | 0.0000 |
| 16 | wide | 10 | missing_outrange | 5 | 0.000 | N/A | 822.0 | 2465.9 | n/a | 0.60 | 0.64 | 0.67 | 1239272 | 0.000 | 0.0000 |

## Pre-registered contrasts (by run, 95% CI)

### hit: wide vs narrow, and Bloom off vs on (K=16, 8 GiB cache, 1 thread)

| contrast | metric | A | B | ratio B/A | 95% CI | verdict |
|---|---|---:|---:|---:|---|---|
| K=16 bits=0 hit | checks/Get | 0.000 | 0.000 | - | ?..? | insufficient reps for an interval |
| K=16 bits=0 hit | cpu ns/Get | 3085.1 | 14942.8 | 4.843 | 4.689..5.005 | checks AND performance both move: a measurable read-path cost; profiling is needed to attribute the part that is Bloom itself |
| K=16 bits=0 hit | p99 µs | 3.58 | 29.49 | 8.229 | 7.869..8.588 | checks AND performance both move: a measurable read-path cost; profiling is needed to attribute the part that is Bloom itself |
| K=16 bits=0 hit | ops/s | 313454 | 64121 | 0.205 | 0.198..0.212 | performance difference resolvable (checks unchanged) |
| K=16 bits=10 hit | checks/Get | 1.000 | 8.501 | 8.501 | 8.500..8.501 | checks AND performance both move: a measurable read-path cost; profiling is needed to attribute the part that is Bloom itself |
| K=16 bits=10 hit | cpu ns/Get | 3321.2 | 4518.3 | 1.360 | 1.306..1.417 | checks AND performance both move: a measurable read-path cost; profiling is needed to attribute the part that is Bloom itself |
| K=16 bits=10 hit | p99 µs | 3.89 | 7.37 | 1.895 | 1.753..2.047 | checks AND performance both move: a measurable read-path cost; profiling is needed to attribute the part that is Bloom itself |
| K=16 bits=10 hit | ops/s | 290952 | 212496 | 0.730 | 0.703..0.759 | performance difference resolvable (checks unchanged) |
| K=16 narrow hit | cpu ns/Get | 3085.1 | 3321.2 | 1.077 | 1.035..1.120 | performance difference resolvable (checks unchanged) |
| K=16 narrow hit | cache hits/Get | 1.000 | 1.000 | 1.000 | 1.000..1.000 | performance ratio interval lies entirely inside 0.95-1.05: under these conditions the effect is below the preset 5% |
| K=16 narrow hit | p99 µs | 3.58 | 3.89 | 1.086 | 1.049..1.122 | performance difference resolvable (checks unchanged) |
| K=16 narrow hit | ops/s | 313454 | 290952 | 0.928 | 0.894..0.964 | performance difference resolvable (checks unchanged) |
| K=16 wide hit | cpu ns/Get | 14942.8 | 4518.3 | 0.302 | 0.292..0.313 | performance difference resolvable (checks unchanged) |
| K=16 wide hit | cache hits/Get | 8.495 | 1.072 | 0.126 | 0.126..0.126 | performance difference resolvable (checks unchanged) |
| K=16 wide hit | p99 µs | 29.49 | 7.37 | 0.250 | 0.229..0.273 | performance difference resolvable (checks unchanged) |
| K=16 wide hit | ops/s | 64121 | 212496 | 3.314 | 3.197..3.434 | performance difference resolvable (checks unchanged) |

### missing_inrange: wide vs narrow, and Bloom off vs on (K=16, 8 GiB cache, 1 thread)

| contrast | metric | A | B | ratio B/A | 95% CI | verdict |
|---|---|---:|---:|---:|---|---|
| K=16 bits=0 missing_inrange | checks/Get | 0.000 | 0.000 | - | ?..? | insufficient reps for an interval |
| K=16 bits=0 missing_inrange | cpu ns/Get | 3110.5 | 27924.1 | 8.977 | 8.782..9.177 | checks AND performance both move: a measurable read-path cost; profiling is needed to attribute the part that is Bloom itself |
| K=16 bits=0 missing_inrange | p99 µs | 4.10 | 40.96 | 10.000 | 10.000..10.000 | checks AND performance both move: a measurable read-path cost; profiling is needed to attribute the part that is Bloom itself |
| K=16 bits=0 missing_inrange | ops/s | 311419 | 34811 | 0.112 | 0.110..0.114 | performance difference resolvable (checks unchanged) |
| K=16 bits=10 missing_inrange | checks/Get | 1.000 | 16.000 | 16.000 | 16.000..16.000 | checks AND performance both move: a measurable read-path cost; profiling is needed to attribute the part that is Bloom itself |
| K=16 bits=10 missing_inrange | cpu ns/Get | 1022.1 | 3319.9 | 3.248 | 3.040..3.477 | checks AND performance both move: a measurable read-path cost; profiling is needed to attribute the part that is Bloom itself |
| K=16 bits=10 missing_inrange | p99 µs | 2.46 | 6.55 | 2.667 | 2.402..2.969 | checks AND performance both move: a measurable read-path cost; profiling is needed to attribute the part that is Bloom itself |
| K=16 bits=10 missing_inrange | ops/s | 982327 | 290551 | 0.296 | 0.276..0.317 | performance difference resolvable (checks unchanged) |
| K=16 narrow missing_inrange | cpu ns/Get | 3110.5 | 1022.1 | 0.329 | 0.310..0.348 | performance difference resolvable (checks unchanged) |
| K=16 narrow missing_inrange | cache hits/Get | 1.250 | 0.012 | 0.010 | 0.010..0.010 | performance difference resolvable (checks unchanged) |
| K=16 narrow missing_inrange | p99 µs | 4.10 | 2.46 | 0.600 | 0.561..0.639 | performance difference resolvable (checks unchanged) |
| K=16 narrow missing_inrange | ops/s | 311419 | 982327 | 3.154 | 2.972..3.341 | performance difference resolvable (checks unchanged) |
| K=16 wide missing_inrange | cpu ns/Get | 27924.1 | 3319.9 | 0.119 | 0.115..0.123 | performance difference resolvable (checks unchanged) |
| K=16 wide missing_inrange | cache hits/Get | 17.189 | 0.166 | 0.010 | 0.010..0.010 | performance difference resolvable (checks unchanged) |
| K=16 wide missing_inrange | p99 µs | 40.96 | 6.55 | 0.160 | 0.154..0.166 | performance difference resolvable (checks unchanged) |
| K=16 wide missing_inrange | ops/s | 34811 | 290551 | 8.346 | 8.098..8.600 | performance difference resolvable (checks unchanged) |

## Notes on what is and is not claimable

* `instructions/Get` is `n/a` when the hardware counter could not be opened (`instructions_unavailable_errno`, 13 = EACCES from `perf_event_paranoid`). The plan allows this; claiming that the Bloom function itself is expensive needs separate CPU profiling, which this environment cannot provide.
* `cpu_ns/Get` is thread CPU time over the window divided by the window's operations, so on a saturated single-threaded arm it equals wall time per Get; it becomes an independent quantity only when the arm is not CPU-bound.
* Device reads are reported separately: on this host a 2 GiB L0-only database is fully cache-resident, so `block reads` here are cache misses, not device I/O.
* The false positive rate is only defined for queries that do not exist; it is `N/A` for hit arms and for arms with the filter switched off.

Charts: `/home/embed/hyl/metadata_offload/bench/out/perf_v2/layout_bloom/charts_first` (checks vs K, CPU vs checks, bits/key facets).
