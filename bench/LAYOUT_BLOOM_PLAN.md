# L0 overlap × Bloom filter cost — experiment plan

> Persisted on the server because the local save failed (auto-approval error). This is
> the plan as given; the status line at the end says what has and has not been run.
> Implementation: `bench/layoutbench.cc` (a copy of the frozen `l0bench.cc`, extended),
> built as `bench/tools/l0bench_layout`; campaign `bench/v2/layout_bloom.sh`;
> analysis `bench/v2/layout_bloom_analyze.py`.

Three steps: **prove more checks happen, then measure the CPU cost, then test whether it
reaches service capability.** The main experiment uses keys that do not exist *inside the
envelope*, because that is what exposes multi-file filtering most clearly.

## 1. What the experiment answers

| Question | Key contrast | Main metric |
|---|---|---|
| How many more checks does L0 overlap cost? | fixed data volume, file count, Bloom config; narrow vs wide | Bloom checks per Get |
| Does checking more cost CPU? | data and metadata resident, single thread, no compaction | CPU ns/Get, cycles/Get, pure Get latency |
| How much does Bloom avoid? | same layout, Bloom off vs 10 bits/key | data block cache accesses, block reads, CPU |
| Does more precision help? | same layout, 10 vs 16 bits/key | false positive rate, checks, CPU |
| When does it reach the user? | add concurrency, limit cache, fix arrival rate | throughput, p99, device read volume |

Two mechanisms must be kept apart: **raising bits/key reduces false positives but normally
cannot remove the filter checks against multiple candidate SSTs.**

## 2. Rebuild a controlled data layout

Phase 1 uses an **L0-only database** — no 50 GB base — so SST Bloom counts are
unambiguously L0's.

Fixed:

* total keys `M = 2,097,152`, 16 B key, 1 KiB value → about **2.03 GiB** logical;
* same key set, value content and query trace for every arm;
* single column family, no compression, no deletions, no older versions;
* automatic compaction off; no writes during the read phase;
* first round `K = 16`; later extend to `K = 1, 8, 16`.

Keys share a prefix and use big-endian integers, and **only even keys `2i` are written**:

| Layout | What each file contains | Property |
|---|---|---|
| narrow | one contiguous ordinal range | file key ranges do not overlap |
| wide | every key with `i mod K = file number` | each file spans most of the global range |

Each key is written once and each group is explicitly flushed. After the build, verify:
**exactly K L0 SSTs, other levels empty, memtable empty, logical content identical.** Unlike
the earlier experiment, total data volume must not grow with K.

## 3. Three query classes, counted separately

| Class | How it is built | Purpose |
|---|---|---|
| **in-envelope missing read** | query the never-written odd key `2i+1`, inside the file envelope | main experiment: the full filtering path |
| **hit read** | uniform over written even keys | cost of a real successful lookup |
| **out-of-envelope missing read** | same prefix, IDs above every SST's largest key | negative control; should be excluded by range checks |

In-envelope missing keys must avoid the gaps between narrow shards and fall inside the
intersection of all wide files' envelopes. Verify item by item against the real SST ranges:

* narrow: about **1 candidate file**.
* wide: about **K candidate files**.

Simplified predictions to be tested (not a substitute for counting):

| Request | narrow Bloom checks | wide Bloom checks |
|---|---:|---:|
| in-envelope missing | ~1 | ~K |
| uniform hit | ~1 | ~(K+1)/2 |

Hit reads may terminate early. **Do not keep using the old experiment's different-prefix
`neg` requests as a stand-in for in-envelope missing reads.**

## 4. First-round matrix: 80 runs

Fixed `K = 16`, 8 GiB block cache, single thread, compaction frozen:

> **2 layouts × 2 Bloom configs × 2 query classes × 5 reps = 40 runs**

* layouts: narrow / wide;
* Bloom: off / 10 bits/key;
* queries: hit / in-envelope missing.

Two control groups:

| Control | Runs |
|---|---:|
| K=16, both layouts, both Bloom configs, out-of-envelope missing, 5 reps | 20 |
| K=1, single layout, both Bloom configs, hit / in-envelope missing, 5 reps | 20 |

**First round totals 80 runs.** At K=1 narrow and wide coincide, so it is not repeated.

The Bloom configuration must take effect **at build time**: off, 10 bits and 16 bits each
require rebuilding the SSTs. Changing the option when opening the database cannot prove
that existing filters were disabled or changed.

## 5. Separate performance timing from mechanism counting

Each run:

1. **warm up at least 30 s**, confirming data and metadata caches are stable;
2. **performance window 60 s**, with per-request PerfContext and synchronous logging off;
3. **diagnostic window about 20 s**, replaying the same request distribution, collecting
   Bloom and other counters.

Paired arms use the same query seed and a pre-generated trace; record the executed counts.
The diagnostic window may use a fixed request count so that counted arms cover the same
batch of queries.

The performance window times only around `DB::Get`, recording:

* Get p50, p95, p99;
* thread CPU ns/Get, cycles/Get, instructions/Get;
* completion throughput.

The diagnostic window records:

* Bloom positive, negative;
* data block cache hits/misses;
* `block_read_count`, `block_read_byte`;
* filter/index block reads;
* device reads, RSS, background task state.

**Fully resident in memory does not mean no data block access**: accesses may all hit
cache. So `block_read_count` alone is not enough — cache accesses and CPU must be read
too. Counting definitions:

```text
Bloom checks per Get = (positive + negative) / sampled Gets of the same class
False positive rate for genuinely missing in-envelope queries = positive / (positive + negative)
```

The second formula applies only to queries that do not exist. With the filter off, the
false positive rate is **N/A**. The whole-Get CPU difference also includes file selection,
index and cache operations; claiming "the Bloom function itself is expensive" needs
separate CPU profiling.

## 6. Extensions after the first round

Two small matrices first:

* **K=8, bits=10**: both layouts × both query classes × 5 reps = 20 runs, to see the trend in K;
* **K=16, bits=16**: both layouts × both query classes × 5 reps = 20 runs, to separate false positives from the cost of checking itself.

Then the actual performance boundary:

| Extension | Design focus |
|---|---|
| multithreaded | 1/4/16 threads; does per-Get CPU cost turn into lost throughput |
| open-loop | same **absolute** arrival rate for both layouts; compare p99 and queueing |
| cache limited | fixed total cache budget; report filter/index usage, which squeezes data cache |
| device reads | use and verify `use_direct_reads`, measure device reads; a small block cache does not by itself mean cold device reads |

The existing driver's `use_direct_io` applies to flush/compaction and **must not be treated
as `use_direct_reads`**.

Finally add the 50 GB base validation: the base must reserve missing keys, and
"base hit, not in L0" queries must also fall inside the L0 envelope. Global Bloom counts
then include deeper levels and can no longer be labelled L0 checks.

## 7. Conclusion criteria, fixed in advance

At least 5 independent runs per group, paired arms randomly interleaved, confidence
intervals computed **by run**, not treating millions of requests as independent repeats.
The first-round engineering threshold is **5%**:

| Outcome | How to read it |
|---|---|
| more checks, wide CPU/latency interval | checking work increased, performance effect unresolved |
| performance ratio interval entirely inside 0.95–1.05 | under these conditions the effect is below the preset 5% |
| checks, CPU and latency all rise together | a measurable read-path cost; use profiling to judge Bloom's contribution |
| 16 bits lowers false positives but not checks or CPU | better filter precision did not remove multi-file check cost |
| only worse with a small cache or high load | the effect has a definite cache or load condition |

Differences near 1% may need more reps. Independent confirmation batches should be sized
from the first round's variance — not by adding samples until something is significant.

Charts, in priority order: **checks vs K**, **CPU cost vs checks**, and a
**false-positive / block-read / CPU facet plot by bits/key**; a throughput–p99 curve comes
after the load extensions.

---

## Status

* Plan persisted here. **No part of these experiments has been run yet** — the first
  round is queued behind the M1–M5 50 GB campaign.
* The tool is `bench/layoutbench.cc` → `bench/tools/l0bench_layout`, deliberately a
  **separate file and binary** from the frozen `l0bench.cc` / `tools/l0bench`, so the
  in-flight campaign's instrument identity stays exactly as recorded in its manifest.
