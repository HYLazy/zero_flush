# L0 backlog: layout × size × file count — experiment plan

> Persisted on the server (the local save did not complete). Design: **vary layout, backlog
> bytes and file count independently → separate direct read cost from background
> reorganising cost → validate under a business load.** The goal is not only to find a
> difference but to explain where it comes from, and whether the cost was merely deferred
> past the measurement window.
>
> Tooling: `bench/backlogbench.cc` → `bench/tools/l0bench_backlog` (separate source and
> binary, so the other campaigns' instrument identities stay frozen);
> campaign `bench/v2/l0_backlog.sh`; analysis `bench/v2/l0_backlog_analyze.py`.

## 1. Three variables, so the old E3 confound does not repeat

| Variable | Meaning | Contrast |
|---|---|---|
| layout | how much SST key ranges overlap | narrow: low overlap; wide: high |
| size B | total data staged in L0 | ≈ 0.5, 2, 8 GiB |
| file count K | how many SSTs the same data is split into | scanned separately, never with B |

Main experiment fixes **K = 16** and crosses **2 layouts × 3 sizes = 6 initial states**,
which answers: does overlap matter at equal size and file count, and does more L0 backlog
matter at equal layout and file count. Then fix **B ≈ 2 GiB** and scan K alone.

## 2. Building comparable, runnable initial states

Start from a verified ~50 GB base with its L0 and memtable empty; every experiment group
starts from the same copy. Overwrite existing base keys **with the value they already
have**, auto-compaction off, keeping the updates in L0:

* the rewritten value equals the base value, so every group presents identical
  user-visible data;
* for different B, choose a deterministic **nested** subset of keys, all inside the same
  global key interval;
* at the same B, narrow and wide use exactly the same update key set.

After sorting the update keys:

| Layout | Grouping |
|---|---|
| narrow | split into K contiguous segments, one flush per segment |
| wide | group by sorted index `i mod K`, one flush per group |

Verify file count, key ranges, entry counts and sizes from the database's own metadata —
not from the build parameters — and size the memtable so no intended flush gets split.

**What this measures is "the effect of the amount of L0 overwrite backlog on a fixed
base".** Raising B also changes the number of overwritten versions and the L0 hit ratio,
so the result must not be described as a pure byte effect; but that is exactly the
situation a system faces under a real backlog.

## 3. Three experiments per initial state

| Experiment | Operation | Question |
|---|---|---|
| A: compaction frozen, read only | keep the initial layout, run a fixed query trace | direct read-path effect of layout and size |
| B: compaction enabled, no foreground load | settle naturally from the frozen state | how much background work the backlog costs |
| C: compaction enabled, mixed read/write | replay the same business requests, then stop writing and watch settling | effect on service quality and the full processing cost |

Each experiment uses its own copy: A cannot be followed by B on the same database and
then be treated as the same initial condition.

**A — direct read cost.** Fixed Bloom = 10 bits/key, same cache budget and thread count,
running: existing-key gets; missing gets *inside the file ranges*; fixed-length range
scans. The main trace always targets the same whole-database key space; it must not
degenerate into "read only this group's recent data" as B grows. Extra breakdown by L0
hit / deeper hit / missing. Record pure Get/Scan latency, CPU/Get, throughput, Bloom
checks, cache hits, device reads — first with a uniformly well-warmed cache, then a
separate cache-limited extension.

**B — the cost of repaying the backlog.** From the moment auto-compaction is enabled,
record: per-level bytes newly written and read, CPU time; time series of L0 size, file
count, pending and per-level occupancy; time to reach the predefined healthy state; peak
disk space. Report both **absolute work** and **work per GiB of initial L0 data** —
otherwise "8 GiB takes longer than 0.5 GiB" only says there is more data. This project's
base level may be L3, so record the actual level rather than assuming L0→L1, and do not
count trivial moves as newly written bytes.

**C — overall business impact.** First round a clear business load: **90% Get + 10% Put**;
a 50/50 point later. All groups keep the same request trace, read/write ratio, key
distribution and per-key write order; the same **absolute arrival rate**, foreground
thread count, cache and background configuration; the same commit window and request
count. Pick a medium arrival rate from a common stable baseline, then add a point near
capacity — never "80% of each group's own capacity", which is not the same load.
Report Get and Put completion and p99 separately, plus queueing, backpressure, CPU, I/O
and pending. **Equal total ops/s can still hide a worse write tail.**

## 4. Separate "temporarily fast" from "actually cheaper"

After the business window, keep recording until committed requests complete and the
background settles:

```text
initial backlog
  -> identical business window
  -> in-flight requests complete
  -> background settling to a common health standard
```

Define the health standard in advance: no in-flight writes, memtable processed, no
running compaction, per-level sizes and pending inside an allowed band and stable.
Report the residual layout and debt; groups that time out are marked incomplete. Do not
use `L0 == 0` as the completion test and do not require all data at the last level; a
forced whole-database `CompactRange` adds work of its own and must not be the common
endpoint.

Specifically check: **did a larger L0 raise in-window throughput while increasing drain
time, total bytes written or peak space?** Report the build cost of the initial backlog
separately; when comparing total write cost, put the user bytes and device bytes of both
the build and the business window on the same footing — the preparation is not free.

## 5. File-count threshold and layout cost kept apart

At fixed B ≈ 2 GiB, scan K in two separate groups:

* **compaction frozen**: file selection, Bloom, scans, metadata cost;
* **auto-compaction on**: how file count reaches overall performance through scheduling
  and slowdown/stop.

Experiments that cross the default slowdown = 20 and stop = 36 are listed separately: a
threshold-triggered drop is a real system effect but is not "the pure per-file lookup
cost". Also, **compaction changes the initial layout**, so the layout must be plotted as
it evolves; performance after the initial difference has vanished cannot be explained as
a steady-state property of that initial layout. Verifying long-run parameter benefits
needs a separate continuous-buffering experiment.

## 6. Minimum scale and decision criteria

First full matrix: **6 initial states × 3 experiments × 5 paired reps = 90 runs.**
Randomise the experiment order inside a rep, keep the same seed, and treat independent
runs as the statistical unit. **Fix the project's known timing, histogram and synchronous
logging problems before formal measurement** (the histogram encoder and the measurement
window were fixed and accepted in this campaign; the synchronous detail path was replaced
by an asynchronous ring, so this precondition is already met — see
`IMPLEMENTATION_REPORT.md` §3).

At least one of these evidence chains must hold:

| Observation combination | Conclusion it supports |
|---|---|
| more overlap → more Bloom/CPU → worse read latency or capacity | layout has a measurable direct read cost |
| more overlap → more compaction read/write bytes → worse foreground tail | layout affects the business through background contention |
| larger B → backpressure deferred, but drain and space cost rise | a larger buffer mainly moves when the cost is paid |
| larger B → less compaction work per GiB, lower total cost | larger batches really do improve reorganising efficiency |
| more probing, but the performance interval stays near the baseline | mechanism work increased, overall impact limited under these conditions |

Charts, in priority order: **layout × size performance contrast**, the **combined
business-and-drain timeline**, and the **front-end performance vs total write volume /
peak space trade-off** — which is what makes it possible to say how much was affected,
through which mechanism, and whether the cost was deferred, rather than just showing that
L0 size correlates with performance.

---

## Status

* Plan persisted here. **Not started**: the campaign queue runs the 50 GB matrices
  (M1-M5), the 100 GB snapshot and the layout/Bloom experiment first.
* The tool is a separate file and binary from both `l0bench.cc` and `layoutbench.cc`, so
  the instrument identity recorded in the running campaigns' manifests stays valid.
