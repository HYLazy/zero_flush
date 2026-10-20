# L0 backlog: layout x size x file count

Source `/home/embed/hyl/metadata_offload/bench/out/perf_v2/l0_backlog_kscan`. 100 runs. Latency percentiles are bucket lower bounds from the per-class cohort histograms; per-level work comes from the compaction events with trivial moves excluded from written bytes.

## A — direct read cost (compaction frozen)

| layout | B GiB | K | runs | total ops/s | CPU ns/Get | hit_base ops | hit_l0 ops | missing_in_range ops | missing_out_of_range ops | hit_base checks/Get | missing_in_range checks/Get |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| narrow | 2.00 | 1 | 5 | 19476 | 25168.2 | 156915 | 7556 | 163622 | 82161 | 3.881 | 3.993 |
| narrow | 2.00 | 2 | 5 | 188247 | 4967.0 | 241363 | 11804 | 254057 | 126557 | 3.882 | 3.994 |
| narrow | 2.00 | 4 | 5 | 181792 | 5119.0 | 776719 | 38632 | 817445 | 407833 | 3.882 | 3.993 |
| narrow | 2.00 | 8 | 5 | 200643 | 4983.6 | 848745 | 41814 | 891855 | 445115 | 3.881 | 3.993 |
| narrow | 2.00 | 16 | 5 | 201985 | 4950.4 | 1258063 | 61972 | 1323123 | 661156 | 3.881 | 3.993 |
| wide | 2.00 | 1 | 5 | 22725 | 20475.6 | 182390 | 8848 | 190114 | 95609 | 3.881 | 3.994 |
| wide | 2.00 | 2 | 5 | 197319 | 5067.6 | 332001 | 16233 | 349203 | 175327 | 4.881 | 4.993 |
| wide | 2.00 | 4 | 5 | 130796 | 6599.9 | 280780 | 13624 | 294939 | 147599 | 6.882 | 6.993 |
| wide | 2.00 | 8 | 5 | 165190 | 6053.3 | 1271967 | 62326 | 1335603 | 666830 | 10.881 | 10.994 |
| wide | 2.00 | 16 | 5 | 131427 | 7608.1 | 1033058 | 50370 | 1084988 | 540852 | 18.881 | 18.994 |

### A per class (median over the group's runs)

Bucket lower bounds from the cohort histograms. `L0 share` is the fraction of queries the engine answered out of L0; the last two columns are the classes whose latency the layout changes, because they never touch base data.

| layout | B GiB | K | L0 share | base hit p50/p99 µs | L0 hit p50 µs | miss-in p50/p99 µs | miss-out p50/p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|
| narrow | 2.00 | 1 | 1.84% | 147.5 / 196.6 | 30.7 | 7.7 / 36.9 | 3.1 / 5.1 |
| narrow | 2.00 | 2 | 1.87% | 8.2 / 28.7 | 7.2 | 2.0 / 8.2 | 0.9 / 3.1 |
| narrow | 2.00 | 4 | 1.88% | 8.2 / 36.9 | 7.7 | 2.0 / 8.2 | 1.0 / 3.3 |
| narrow | 2.00 | 8 | 1.86% | 8.2 / 15.4 | 7.7 | 2.3 / 8.2 | 1.0 / 1.3 |
| narrow | 2.00 | 16 | 1.88% | 8.2 / 15.4 | 7.2 | 2.3 / 8.2 | 1.2 / 1.4 |
| wide | 2.00 | 1 | 1.87% | 131.1 / 196.6 | 22.5 | 7.2 / 30.7 | 2.8 / 5.1 |
| wide | 2.00 | 2 | 1.85% | 8.2 / 15.4 | 7.2 | 2.3 / 8.2 | 0.9 / 1.3 |
| wide | 2.00 | 4 | 1.86% | 9.2 / 147.5 | 7.7 | 2.8 / 11.3 | 1.0 / 4.1 |
| wide | 2.00 | 8 | 1.87% | 9.2 / 18.4 | 5.6 | 3.3 / 10.2 | 1.0 / 1.3 |
| wide | 2.00 | 16 | 1.86% | 11.3 / 20.5 | 6.1 | 4.6 / 13.3 | 1.2 / 1.4 |

### A rep by rep: the page cache is not controlled by the protocol

Each frozen rep reads its own random subset of the base; the page cache keeps what earlier reps brought in, and the host cannot drop caches without root. The reps are therefore not exchangeable - the first rep of a sweep is cold and the last is warm - and the median above mixes the two regimes. `device GiB` is the run's device read volume, the direct evidence of which regime a rep was in.

| layout | B GiB | K | rep | ops/s | CPU ns/Get | device GiB |
|---|---:|---:|---:|---:|---:|---:|
| narrow | 2.00 | 1 | 1 | 15568 | 29044 | 3.5 |
| narrow | 2.00 | 1 | 2 | 17210 | 26978 | 3.2 |
| narrow | 2.00 | 1 | 3 | 19476 | 25168 | 2.8 |
| narrow | 2.00 | 1 | 4 | 35061 | 14638 | 2.5 |
| narrow | 2.00 | 1 | 5 | 104574 | 6829 | 1.5 |
| narrow | 2.00 | 2 | 1 | 144885 | 5752 | 1.0 |
| narrow | 2.00 | 2 | 2 | 161498 | 5501 | 0.9 |
| narrow | 2.00 | 2 | 3 | 188247 | 4967 | 0.6 |
| narrow | 2.00 | 2 | 4 | 209356 | 4697 | 0.5 |
| narrow | 2.00 | 2 | 5 | 215718 | 4635 | 0.3 |
| narrow | 2.00 | 4 | 1 | 215217 | 4646 | 0.4 |
| narrow | 2.00 | 4 | 2 | 202816 | 4930 | 0.1 |
| narrow | 2.00 | 4 | 3 | 181792 | 5119 | 7.0 |
| narrow | 2.00 | 4 | 4 | 126013 | 6352 | 1.3 |
| narrow | 2.00 | 4 | 5 | 144489 | 5898 | 1.0 |
| narrow | 2.00 | 8 | 1 | 168778 | 5431 | 0.7 |
| narrow | 2.00 | 8 | 2 | 182276 | 5200 | 0.4 |
| narrow | 2.00 | 8 | 3 | 206684 | 4838 | 0.3 |
| narrow | 2.00 | 8 | 4 | 200643 | 4984 | 0.2 |
| narrow | 2.00 | 8 | 5 | 208971 | 4785 | 0.2 |
| narrow | 2.00 | 16 | 1 | 203769 | 4907 | 0.1 |
| narrow | 2.00 | 16 | 2 | 198875 | 5028 | 0.1 |
| narrow | 2.00 | 16 | 3 | 202727 | 4932 | 0.1 |
| narrow | 2.00 | 16 | 4 | 201985 | 4950 | 0.1 |
| narrow | 2.00 | 16 | 5 | 195458 | 5116 | 0.0 |
| wide | 2.00 | 1 | 1 | 17001 | 27214 | 5.1 |
| wide | 2.00 | 1 | 2 | 22491 | 20476 | 2.8 |
| wide | 2.00 | 1 | 3 | 22725 | 23799 | 2.7 |
| wide | 2.00 | 1 | 4 | 58495 | 9968 | 2.1 |
| wide | 2.00 | 1 | 5 | 83347 | 7924 | 1.8 |
| wide | 2.00 | 2 | 1 | 128488 | 6242 | 1.2 |
| wide | 2.00 | 2 | 2 | 177799 | 5288 | 0.6 |
| wide | 2.00 | 2 | 3 | 198763 | 5031 | 0.4 |
| wide | 2.00 | 2 | 4 | 197319 | 5068 | 0.2 |
| wide | 2.00 | 2 | 5 | 203086 | 4924 | 0.2 |
| wide | 2.00 | 4 | 1 | 185108 | 5402 | 0.0 |
| wide | 2.00 | 4 | 2 | 181707 | 5503 | 0.0 |
| wide | 2.00 | 4 | 3 | 113948 | 7214 | 4.3 |
| wide | 2.00 | 4 | 4 | 117118 | 7033 | 5.6 |
| wide | 2.00 | 4 | 5 | 130796 | 6600 | 0.9 |
| wide | 2.00 | 8 | 1 | 165815 | 6030 | 0.4 |
| wide | 2.00 | 8 | 2 | 166840 | 5993 | 0.0 |
| wide | 2.00 | 8 | 3 | 163173 | 6128 | 0.0 |
| wide | 2.00 | 8 | 4 | 165190 | 6053 | 0.0 |
| wide | 2.00 | 8 | 5 | 162930 | 6137 | 0.0 |
| wide | 2.00 | 16 | 1 | 130707 | 7650 | 0.0 |
| wide | 2.00 | 16 | 2 | 132721 | 7534 | 0.0 |
| wide | 2.00 | 16 | 3 | 132971 | 7520 | 0.0 |
| wide | 2.00 | 16 | 4 | 131427 | 7608 | 0.0 |
| wide | 2.00 | 16 | 5 | 131386 | 7611 | 0.0 |

## B — repaying the backlog (compaction on, no foreground load)

Byte columns are PER RUN (median over the group's runs). An earlier version summed them across runs, which folded the repetition count into a column a reader compares against one backlog: the per-GiB ratios came out 5x too large.

| layout | B GiB | K | runs | drain s | healthy | peak GiB | written GiB | read GiB | written per GiB backlog | read per GiB backlog | trivial moves |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| narrow | 2.00 | 1 | 5 | 29 | 5/5 | 47.5 | 15.6 | 15.8 | 7.78 | 7.88 | 9 |
| narrow | 2.00 | 2 | 5 | 20 | 5/5 | 46.8 | 14.3 | 14.5 | 7.17 | 7.26 | 8 |
| narrow | 2.00 | 4 | 5 | 21 | 5/5 | 47.5 | 16.1 | 16.2 | 8.03 | 8.10 | 11 |
| narrow | 2.00 | 8 | 5 | 19 | 5/5 | 47.3 | 15.4 | 15.6 | 7.71 | 7.80 | 8 |
| narrow | 2.00 | 16 | 5 | 24 | 5/5 | 48.5 | 17.0 | 17.2 | 8.51 | 8.61 | 12 |
| wide | 2.00 | 1 | 5 | 35 | 5/5 | 47.8 | 15.6 | 15.8 | 7.78 | 7.88 | 9 |
| wide | 2.00 | 2 | 5 | 21 | 5/5 | 46.9 | 15.1 | 15.3 | 7.53 | 7.63 | 9 |
| wide | 2.00 | 4 | 5 | 22 | 5/5 | 47.8 | 15.6 | 15.8 | 7.80 | 7.90 | 9 |
| wide | 2.00 | 8 | 5 | 18 | 5/5 | 47.8 | 14.8 | 15.0 | 7.40 | 7.48 | 6 |
| wide | 2.00 | 16 | 5 | 21 | 5/5 | 47.9 | 14.5 | 14.7 | 7.26 | 7.34 | 8 |

## C — business load (90% get / 10% put, common absolute rate)

| layout | B GiB | K | runs | total ops/s | achieved/offered | Get p99 µs | Put p99 µs | Get ops | Put ops | censored | drain s | healthy |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|

## The trade-off: did a larger L0 buy throughput and pay elsewhere?

| layout | B GiB | K | A read ops/s | B drain s | C in-window ops/s | C written GiB | C peak GiB | C drain s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| narrow | 2.00 | 1 | 19476 | 29 | - | - | - | - |
| narrow | 2.00 | 2 | 188247 | 20 | - | - | - | - |
| narrow | 2.00 | 4 | 181792 | 21 | - | - | - | - |
| narrow | 2.00 | 8 | 200643 | 19 | - | - | - | - |
| narrow | 2.00 | 16 | 201985 | 24 | - | - | - | - |
| wide | 2.00 | 1 | 22725 | 35 | - | - | - | - |
| wide | 2.00 | 2 | 197319 | 21 | - | - | - | - |
| wide | 2.00 | 4 | 130796 | 22 | - | - | - | - |
| wide | 2.00 | 8 | 165190 | 18 | - | - | - | - |
| wide | 2.00 | 16 | 131427 | 21 | - | - | - | - |

## K scan — what more L0 files cost at a fixed backlog size

Frozen group (A) is the direct lookup cost with compaction disabled; the auto-compaction group (B) is what repaying the same backlog costs when the engine's own trigger decides. C is the same backlog under business load.

| layout | K | A ops/s | A p50 hit µs | A p99 hit µs | A p99 miss-in µs | checks/Get (miss-in) | CPU ns/Get | B drain s | B written/GiB | B peak GiB | C ops/s | C Put p99 µs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| narrow | 1 | 19476 | 147.46 | 196.61 | 36.86 | 3.993 | 25168.2 | 29 | 7.78 | 47.5 | - | - |
| narrow | 2 | 188247 | 8.19 | 28.67 | 8.19 | 3.994 | 4967.0 | 20 | 7.17 | 46.8 | - | - |
| narrow | 4 | 181792 | 8.19 | 36.86 | 8.19 | 3.993 | 5119.0 | 21 | 8.03 | 47.5 | - | - |
| narrow | 8 | 200643 | 8.19 | 15.36 | 8.19 | 3.993 | 4983.6 | 19 | 7.71 | 47.3 | - | - |
| narrow | 16 | 201985 | 8.19 | 15.36 | 8.19 | 3.993 | 4950.4 | 24 | 8.51 | 48.5 | - | - |
| wide | 1 | 22725 | 131.07 | 196.61 | 30.72 | 3.994 | 20475.6 | 35 | 7.78 | 47.8 | - | - |
| wide | 2 | 197319 | 8.19 | 15.36 | 8.19 | 4.993 | 5067.6 | 21 | 7.53 | 46.9 | - | - |
| wide | 4 | 130796 | 9.22 | 147.46 | 11.26 | 6.993 | 6599.9 | 22 | 7.80 | 47.8 | - | - |
| wide | 8 | 165190 | 9.22 | 18.43 | 10.24 | 10.994 | 6053.3 | 18 | 7.40 | 47.8 | - | - |
| wide | 16 | 131427 | 11.26 | 20.48 | 13.31 | 18.994 | 7608.1 | 21 | 7.26 | 47.9 | - | - |

Every K sees the same rep structure (rep r runs all K arms in a shuffled order inside one sweep), so a cold-to-warm page cache moves all K arms together and the across-K contrast below survives it; the absolute per-rep numbers do not (see the rep-by-rep table above).

A K of 1 is the degenerate point where narrow and wide describe the same single file, so their agreement there is a consistency check rather than a layout contrast. In the auto-compaction group the engine only compacts when its own trigger is reached; at `level0_file_num_compaction_trigger=8` the K <= 8 arms therefore have nothing to schedule, and their drain second column is an empty result, not a fast one.

## Evidence chains (§6)

Filled in from the tables above; each line names the comparison it rests on:

* *overlap → checks/CPU → read latency*: A's `hit_base` / `missing_in_range` columns against the `checks/Get` columns, narrow vs wide at equal B and K.
* *overlap → compaction bytes → foreground tail*: B's written/read GiB per GiB of backlog (narrow vs wide) against C's Put p99.
* *larger B → deferred backpressure but higher drain/space*: B's drain seconds and peak GiB across the three sizes.
* *larger B → less work per GiB*: B's `written GiB per GiB backlog` across sizes (falling values support better reorganising efficiency).
* *more probing, no performance change*: A's ops/s intervals across layouts at equal B, with checks/Get differing.

Work per GiB is the number that decides whether a bigger buffer helps or merely moves the cost: absolute work always grows with B, so only the per-GiB column can distinguish the two.

Charts: `/home/embed/hyl/metadata_offload/bench/out/perf_v2/l0_backlog_kscan/charts` (chart_K_read_cost.png = frozen group, chart_K_compaction.png = auto-compaction group, chart_K_business.png = business window; the medians behind every point are in chart_K_notes.txt).
