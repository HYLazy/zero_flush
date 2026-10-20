# L0 backlog: layout x size x file count

Source `bench/out/perf_v2/l0_backlog`. 90 runs. Latency percentiles are bucket lower bounds from the per-class cohort histograms; per-level work comes from the compaction events with trivial moves excluded from written bytes.

## A — direct read cost (compaction frozen)

| layout | B GiB | K | runs | total ops/s | CPU ns/Get | hit_base ops | hit_l0 ops | missing_in_range ops | missing_out_of_range ops | hit_base checks/Get | missing_in_range checks/Get |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| narrow | 0.50 | 16 | 5 | 28424 | 21326.4 | 231155 | 2667 | 232978 | 116674 | 4.124 | 4.245 |
| narrow | 2.00 | 16 | 5 | 29386 | 21737.1 | 222239 | 10807 | 233272 | 116657 | 4.121 | 4.245 |
| narrow | 8.00 | 16 | 5 | 42910 | 17112.9 | 237701 | 54470 | 292533 | 146252 | 4.098 | 4.245 |
| wide | 0.50 | 16 | 5 | 36528 | 19393.8 | 220022 | 2594 | 221869 | 111567 | 19.123 | 19.244 |
| wide | 2.00 | 16 | 5 | 23702 | 28494.9 | 186584 | 9118 | 196541 | 98290 | 19.120 | 19.244 |
| wide | 8.00 | 16 | 5 | 19349 | 34035.7 | 133729 | 30975 | 164875 | 82368 | 19.099 | 19.244 |

### A per class (median over the group's runs)

Bucket lower bounds from the cohort histograms. `L0 share` is the fraction of queries the engine answered out of L0; the last two columns are the classes whose latency the layout changes, because they never touch base data.

| layout | B GiB | K | L0 share | base hit p50/p99 µs | L0 hit p50 µs | miss-in p50/p99 µs | miss-out p50/p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|
| narrow | 0.50 | 16 | 0.48% | 32.8 / 180.2 | 131.1 | 7.7 / 49.2 | 3.6 / 5.6 |
| narrow | 2.00 | 16 | 1.87% | 32.8 / 180.2 | 28.7 | 7.7 / 36.9 | 3.8 / 5.6 |
| narrow | 8.00 | 16 | 7.47% | 26.6 / 147.5 | 28.7 | 6.7 / 36.9 | 3.1 / 5.6 |
| wide | 0.50 | 16 | 0.47% | 28.7 / 196.6 | 11.3 | 12.3 / 36.9 | 2.8 / 5.6 |
| wide | 2.00 | 16 | 1.86% | 41.0 / 229.4 | 32.8 | 15.4 / 180.2 | 3.6 / 5.6 |
| wide | 8.00 | 16 | 7.49% | 45.1 / 327.7 | 147.5 | 18.4 / 294.9 | 3.8 / 6.1 |

## B — repaying the backlog (compaction on, no foreground load)

Byte columns are PER RUN (median over the group's runs). An earlier version summed them across runs, which folded the repetition count into a column a reader compares against one backlog: the per-GiB ratios came out 5x too large.

| layout | B GiB | K | runs | drain s | healthy | peak GiB | written GiB | read GiB | written per GiB backlog | read per GiB backlog | trivial moves |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| narrow | 0.50 | 16 | 5 | 14 | 5/5 | 43.7 | 5.6 | 5.6 | 11.11 | 11.11 | 3 |
| narrow | 2.00 | 16 | 5 | 45 | 5/5 | 49.4 | 17.1 | 17.3 | 8.57 | 8.65 | 7 |
| narrow | 8.00 | 16 | 5 | 159 | 5/5 | 63.0 | 51.1 | 55.5 | 6.39 | 6.93 | 18 |
| wide | 0.50 | 16 | 5 | 14 | 5/5 | 43.8 | 5.0 | 5.0 | 10.07 | 10.08 | 2 |
| wide | 2.00 | 16 | 5 | 40 | 5/5 | 48.2 | 16.2 | 16.4 | 8.10 | 8.20 | 7 |
| wide | 8.00 | 16 | 5 | 142 | 5/5 | 61.8 | 52.1 | 56.5 | 6.51 | 7.06 | 17 |

## C — business load (90% get / 10% put, common absolute rate)

| layout | B GiB | K | runs | total ops/s | achieved/offered | Get p99 µs | Put p99 µs | Get ops | Put ops | censored | drain s | healthy |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| narrow | 0.50 | 16 | 5 | 63277 | 0.872 | 147.5 | 41.0 | 5374999 | 597274 | 0 | 2 | 5/5 |
| narrow | 2.00 | 16 | 5 | 36745 | 0.507 | 163.8 | 49.2 | 2912827 | 324289 | 0 | 2 | 5/5 |
| narrow | 8.00 | 16 | 5 | 12545 | 0.173 | 917.5 | 81.9 | 1488544 | 166047 | 0 | 77 | 5/5 |
| wide | 0.50 | 16 | 5 | 28786 | 0.397 | 180.2 | 57.3 | 3112980 | 346314 | 0 | 2 | 5/5 |
| wide | 2.00 | 16 | 5 | 32122 | 0.443 | 180.2 | 57.3 | 1901403 | 211909 | 0 | 2 | 5/5 |
| wide | 8.00 | 16 | 5 | 7479 | 0.103 | 1310.7 | 90.1 | 1296739 | 144702 | 0 | 87 | 5/5 |

## The trade-off: did a larger L0 buy throughput and pay elsewhere?

| layout | B GiB | K | A read ops/s | B drain s | C in-window ops/s | C written GiB | C peak GiB | C drain s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| narrow | 0.50 | 16 | 28424 | 14 | 63277 | 10.0 | 44.8 | 2 |
| narrow | 2.00 | 16 | 29386 | 45 | 36745 | 17.3 | 48.9 | 2 |
| narrow | 8.00 | 16 | 42910 | 159 | 12545 | 51.3 | 63.9 | 77 |
| wide | 0.50 | 16 | 36528 | 14 | 28786 | 5.0 | 44.0 | 2 |
| wide | 2.00 | 16 | 23702 | 40 | 32122 | 16.2 | 47.6 | 2 |
| wide | 8.00 | 16 | 19349 | 142 | 7479 | 52.2 | 63.1 | 87 |

## Evidence chains (§6)

Filled in from the tables above; each line names the comparison it rests on:

* *overlap → checks/CPU → read latency*: A's `hit_base` / `missing_in_range` columns against the `checks/Get` columns, narrow vs wide at equal B and K.
* *overlap → compaction bytes → foreground tail*: B's written/read GiB per GiB of backlog (narrow vs wide) against C's Put p99.
* *larger B → deferred backpressure but higher drain/space*: B's drain seconds and peak GiB across the three sizes.
* *larger B → less work per GiB*: B's `written GiB per GiB backlog` across sizes (falling values support better reorganising efficiency).
* *more probing, no performance change*: A's ops/s intervals across layouts at equal B, with checks/Get differing.

Work per GiB is the number that decides whether a bigger buffer helps or merely moves the cost: absolute work always grows with B, so only the per-GiB column can distinguish the two.

Charts: `bench/out/perf_v2/l0_backlog/charts` (layout x size contrast, business+drain timeline, front-end vs total write volume / peak space).
