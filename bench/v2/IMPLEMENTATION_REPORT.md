# perf_v2 implementation report

Server-side implementation of `SERVER_EXPERIMENT_RUNBOOK.md`, 2026-09-25.
Driver under test: `bench/tools/l0bench`, built from `bench/l0bench.cc` with
`bench/build.sh`; repo HEAD `ff27eda`. The binary was rebuilt three times during the
campaign (the last time 09-29 09:33) and **`c741cca157d6e554dbd4a687110650c1` is not the
current build**; §15 lists which revision produced which matrix, with the evidence.

---

## 1. Status in one paragraph

The measurement tool (§2.1/§2.2/§2.3) is implemented and accepted, the §3 P0 gate
passes against the frozen binary, the §4.1 snapshot has been verified in place, and
the whole §1→§7 chain runs end to end at smoke scale (M1 18/18 and M2 84/84 runs
valid, §7 bundle produced and sha256-verified). **The formal M1/M2 runs are launched
and in progress** (`campaign_v2.sh formal --to-m2`); their results are not in this
document. What is established here is that the instrument measures what the runbook
says it must, that the campaign needs no further code changes, and that the four
pre-flight checks in the review are answered with raw evidence.

## 1a. The four pre-flight checks

**Check 1 — why the P0 invariance throughputs are nearly identical.**
`bench/out/perf_v2/p0/P0_THROUGHPUT_EXPLAINED.md`. The seven runs are real RocksDB
processes, not replay. Each row carries the raw monotonic nanosecond bounds
(`warmup_end`, `last_submit`, `cutoff`) and a service throughput recomputed in the
checker from those raw numbers, independent of the driver's own arithmetic: the two
agree to a relative difference of ≤ 2.8e-8 across all seven, and the recomputed
values span **0.0041%**. They agree because the arms are open-loop at a fixed offered
rate with `completed ≈ offered × window`; the client-side queue is separately reported
(p99 98 µs – 1.97 ms) and the whole-run throughput of the same seven runs spans 2.4×,
which is what shows the identity is a property of open-loop arrival rather than of
synthesized data. Collection overhead was measured too, in a round-robin interleaved
design: detail sampling at 1e-3/1e-2 and per-bucket bound tracking move throughput by
3.49% (min/max) or 2.49% (medians) against a 1.98% same-setting noise floor, with zero
dropped rows — **no overhead is resolvable**, so sampling settings are comparable.

**Check 2 — the latency correction is an interval, not a lossless point.**
`bench/out/hist_erratum/LATENCY_ERRATUM.md`. The v1 label determines the *bucket*, so
the interval `[lower, upper]` is recovered exactly and nothing inside the bucket is:
`2359.3 µs` is the **mean bucket lower bound** over 5 reps, and the request was
somewhere in `2359.3–2621.4 µs` (bucket width 262.1 µs, ~11%). Every value in the
erratum carries its width. The 87 manifest entries break down as **81 direct runs + 5
composite isolation arms (each with `.build`/`.drain` sub-runs = 10 files) + 1
snapshot-construction entry**, so no latency data is missing: 91 files, 405 percentile
cells, **0 undecodable**, and 10 files have no `hist.csv` (their interval is
recoverable from the label, the neighbouring bucket density is not).

**Check 3 — the old throughput denominators are not sound either.**
`bench/out/hist_erratum/THROUGHPUT_CAVEAT.md`. The counts are consistent (the
per-second series sums exactly to `ops_ok` for all 81 runs), but `throughput_ops_s`
divided by the whole process duration: 86/87 configs set `--drain_observe_s`
(10–120 s), 74/87 set `--wait_for_drain`, the main arms polled at
`--progress_ms=15000`, and no config had a warmup. Measured work fraction: the
denominator inflation is **1.11×–31.42×, median 2.08×, and differs per arm**
(`devload` 1.14×, `default` 2.12×, `noc` 2.31×, `zipf` 30.46×). Because it differs per
arm, old cross-arm throughput rankings are not merely scaled — they can be reordered,
and the earlier report mixed this convention with a post-hoc "arrival phase" one.

**Check 4 — freeze identity carries full hashes.**
Every manifest line now carries a `sha256` object (binary, source) and an `identity`
object with the binary SHA-256, source SHA-256, `build.sh` SHA-256, the SHA-256 of the
whole frozen script set, the build command, the compiler and the RocksDB library hash;
`campaign_v2.sh` writes the same to `$OUT/instrument_identity.json` and freezes all 15
harness scripts plus `l0bench.cc` into `$OUT/scripts_<timestamp>/` with `SHA256SUMS`.
The short md5 remains as a display convenience only.

## 1b. Review-table requirements

| review item | implementation | evidence |
|---|---|---|
| warmup / measurement / request finish / background drain accounted separately | `measure_phase.json` reports `open_*`, `warmup_*`, `measure_*`, `drain_*`, `final_flush_*` | every run dir |
| open-loop: offered, submitted, completed **and client queueing** | `offered_ops_s`, `cohort_submitted`, `cohort_completed`, `submitted_over_offered`, `client_wait_p50/p99/max_us` + `hist_cohort_client_wait.csv` | `measure_phase.json` |
| overload is a result, not INVALID | `check_run.py` no longer rejects on `censored > 0`; it audits `cohort_submitted == cohort_completed + censored`, reports censoring, and never rejects for queueing/backpressure/plateau | `*.validity.txt` |
| steady state | `steady_state()` compares second vs first half of the window (throughput, P99) plus the pending slope; non-converged and at-knee arms are written to `<stage>/needs_extension.json` and `extend_arms.sh` re-runs exactly those, from their recorded argv, on a fresh replica at 300 s (or `--measure-s=600`) | `REPORT.md`, `needs_extension.json` |
| latency per class + unfinished at cutoff | `cohort_eng_get_hit` / `_get_miss` / `_put` / `_scan` rows, `cohort_*` counts, `censored`, `inflight_at_cutoff` | `summary_extra.csv`, `measure_phase.json` |
| repeats interleaved, failures kept | rep-major loops; failed and invalid runs stay in their directories with `validity.txt` and a manifest line, and `analyze_v2.py` counts them as excluded rather than deleting them | `manifest.jsonl` |
| knee rule is P99 > 3× low-load baseline | both runbook criteria implemented; `knee.json` records which one fired, and the M1 table reports `p99_vs_3x_baseline` and `p99_crossed_3x` so "≤ 3×" reads as "the latency threshold was not crossed", not as a finding | `m1/knee.json` |
| erratum kept separate, originals preserved, `f1c12` claim withdrawn | corrections written to `bench/out/hist_erratum/` only; the retraction is explicit and the corrected ≈2% is stated as *within one bucket width* (2.1 µs gap vs 4.1 µs width), i.e. **not** evidence of a difference | `LATENCY_ERRATUM.md` §4 |

## 2. Runbook section -> artifact -> status

| § | Requirement | Artifact | Status |
|---|---|---|---|
| 1 | preflight record | `preflight.sh` -> `$OUT/logs/host_start.txt` | implemented, exercised |
| 2.1 | bucket fix, `hist_version`/`hist_encoding`/`upper_ns`, legacy parser | `l0bench.cc` `HistT`; `relabel_legacy_hist.py` | **done + accepted** |
| 2.2 | open->warmup->measure->drain, cohort timestamps, service throughput | `--warmup_s/--measure_s`, `*.measure_phase.json`, `summary.json` | **done + accepted** |
| 2.3 | per-class hists, full timestamps, async ring detail, independent sampling, count invariants | `hist_*.csv`, `detail_*.csv`, `detail_meta.json`, `summary.json.invariant_*` | **done + accepted** |
| — | `--seed` was documented but never parsed | `l0bench.cc` ParseArgs | **bug fixed** |
| 3.1–3.4 | P0 tool acceptance | `p0_acceptance.sh` + `p0_verify.py` | **PASS** |
| 4.1 | 50 GB settled snapshot | `$BENCH/base/base50` adopted + `base50.manifest.txt` | **verified (481/481 keys, 0 bad)** |
| 4.2 | 100 GB snapshot | `snapshot.sh 100` | not run (needs ~300 GB free; not started) |
| 5 M1 | pure-write offered-rate sweep | `run_matrix.sh smoke m1` + `analyze_v2.py` | pipeline **proven**, 18/18 valid; formal run not started |
| 5 M2 | pure point reads | `run_matrix.sh m2` | pipeline **proven**, 84/84 valid; formal run not started |
| 5 M3 | mixed read/write | `run_matrix.sh m3` | implemented, not run |
| 5 M4 | fixed-working-set L0 geometry | `run_matrix.sh m4` | implemented, not run |
| 5 M5 | threads / scans / cache | `run_matrix.sh m5` | implemented, not run |
| 6 | monitoring, invalid-run rules, disk-low stop | `lib.sh`, `check_run.py` | implemented; gate active on every run |
| 7 | return content + first shipment | `export_run.sh`, `campaign_v2.sh` | implemented |
| 8 | conclusions under the unresolved-effect rule | `analyze_v2.py` | implemented |

## 3. Acceptance evidence (§3)

`bash bench/v2/p0_acceptance.sh` -> **P0_ACCEPTANCE=PASS**, with the §6 gate also
required over every P0 workload run (`all P0 workload runs VALID=1`).

**§3.1 buckets** (`p0/hist`, verified by `p0_verify.py hist`):
`count=1000096`, `bound_violations=0`, `nonempty=143`, `p99=92274688 <= max=99999922`.
The three cases the runbook names, from the driver's own records:

| values | driver label | independent label | upper | legacy label |
|---|---|---|---|---|
| 256..287 | 256 | 256 | 287 | 768 |
| 288..319 | 288 | 288 | 319 | 800 |
| 512..543 | 512 | 512 | 575 | 1536 |

The verifier re-derives the layout from the runbook text, then checks the driver's
`idx -> label/upper`, checks `lo <= sample_min` and `sample_max <= hi` on every
non-empty bucket, and confirms the v1 and v2 encodings differ (76 divergent buckets)
so they cannot be merged.

**§3.2 counts and classes** (`p0/mix`): `put` 10000 done / 10000 writes / 0 err;
`get` 10000 done / 0 writes, 8026 hit + 1974 NotFound; `mix50` 5001 reads + 4999
writes. Every run: `ops_done == ops_ok + ops_err`, `reads_done + writes_done ==
ops_done`, `sum(per_sec cohort) == cohort`. Detail shows both classes non-empty
(`put` 198 and `get_hit` 154 + `get_miss` 32 sampled), `dropped=0`, `queue_depth_max=2`.
Note NotFound is counted as a completed read and separated from a hit, per §2.3.

**§3.3 scan** (`p0/scan`): `keys=100 bytes=102400 checksum=5258279921698457600
badvalue=0`, and the verifier recomputes bytes and checksum from its own
implementation of the value generator — the numbers match exactly. This checks
content, length and count in one shot.

**§3.4 polling/drain invariance** (`p0/inv_open`, the gated arm):
`progress_ms in {0,1,2000} x wait_for_drain in {0,1}`, plus one arm carrying a
deliberate 30 s observer phase after the measurement. Seven runs, cohort throughput
**336,359.8 .. 336,361.1 ops/s, spread 0.000%** (< 1% required), while the same seven
runs' wall time spans **22.0 .. 52.0 s** and their whole-run throughput spans
**142,279 .. 336,340 ops/s**. That single comparison is the runbook's §2.2
requirement demonstrated: drain and final flush move wall time by 2.36x and move
service throughput not at all. (The long-drain arm is there on purpose — a grid where
every drain returns instantly could pass trivially, so it would not test anything.)

Diagnostics recorded alongside, because the first attempt at this test was
misleading and the record should say so:

* `p0/inv` (closed loop, `--num_ops`-bounded): spread 3.879%.
* `p0/ctrl` (one configuration, 3 repeats): spread 1.692%.
* `p0/ctrl7` (one configuration, 7 repeats): spread **12.854%**.

The closed-loop spread is therefore below the same-config repeat spread at equal
sample count: it is host drift on a cache-resident workload, not a polling effect.
That is also why the gate is applied to the open-loop grid, where the cohort is
fixed by construction (offered rate x window) and the only remaining input is
polling/drain.

## 4. §4 snapshot

`bench/base/base50` is a 47.05 GiB settled snapshot (847 SST files, 50,519,839,341
bytes) inherited from the earlier campaign. `snapshot.sh adopt 50` verified it in
place — `VERIFY checked=481 ok=481 missing=0 badvalue=0` — and wrote
`base50.manifest.txt` (identity, SST bytes, file counts, verify line, binary md5, git
head). The builder is now non-destructive: an existing snapshot directory is adopted
and verified, never silently re-filled.

## 5. Pipeline proof (smoke scale)

`bash bench/v2/campaign_v2.sh smoke --to-m2` -> **rc=0**, exercising §1 preflight,
§2 build + instrument freeze, §3 P0 gate (PASS), §4 snapshot (adopted and verified),
§5 M1 (**18/18 runs `VALID=1`**), §5 M2 (**84/84 runs `VALID=1`**), per-stage
analysis, and the §7 first shipment
(`bundles/perf_v2_all_*.tar.gz`, sha256 verified), stopping at the §7 staging point.
The driver md5 is identical to the P0 run above; the only change afterwards was one
extra arm in the P0 invariance grid.

M1 arm rows (service throughput, cohort per-class percentiles):

| arm | offered | achieved | ratio | p50 µs | p99 µs | p99 bucket width | stalls | WA |
|---|---|---|---|---|---|---|---|---|
| `m1_ov_f0.25_r1` | 98,132 | 98,136 | 1.00 | 61.4 | 147.5 | 16.4 µs | 0 | 1.13 |
| `m1_ov_f0.90_r1` | 353,276 | 351,920 | 1.00 | 28.7 | 61.4 | 4.1 µs | 0 | 1.11 |
| `m1_ov_f1.10_r2` | 347,995 | 151,410 | **0.44** | 28.7 | 294.9 | 32.8 µs | **2** | 1.01 |
| `m1_noc10_r1` | 392,529 | 393,848 | 1.00 | 26.6 | 45.1 | 2.0 µs | 0 | 0.80 |

The knee rule fired on `m1_ov_f1.10_r2` (achieved/offered < 0.95 with pending
rising), which is the expected degenerate outcome at 200k keys where everything is
cache-resident; the point of the smoke pass is that the rule, the gate, the
per-class histograms and the analyzer all work on real runs, and that `knee.json`
(which M3 consumes) is produced. M2's read arms are dominated by cache residency at
this scale and are not results.

## 6. Defects found and fixed during implementation

Found by writing the acceptance tests, which is what they are for. The first four
are in the instrument and change what it reports; the last is a crash.

1. **`HistT::Val` labelled v>=256 at 2.07x..3.00x (known, §2.1).** Fixed to
   `lower_bound_v2`; legacy reproducible via `--hist_encoding=legacy`.
2. **`ops_ok` was incremented even for failed operations**, so
   `ops_done == ops_ok + ops_err` could not hold by construction. Accounting is now
   done in one place from a single success flag.
3. **`--seed` was documented and printed in `--help` but had no parse case**, so
   every run ever made used the compiled-in default 20260914 and the runbook's
   "fixed seed" could not be varied per arm. Parse case added.
4. **`overwrite` mode silently defaulted `--num_ops` to the universe**, which
   truncated measurement runs before warmup ended and produced empty cohorts with a
   negative window. The implicit cap now applies only when no measurement window is
   requested.
5. **`--progress_ms` leaked into the measurement.** The cutoff was detected only
   once per progress period, so a large `progress_ms` moved the measurement window
   (6.00 s vs 5.42 s). Detection now runs at <=50 ms independent of the reporting
   cadence, and the throughput denominator is the actual last-submission time.
6. **Intermittent SIGSEGV at process exit (rc=139 after correct output).** The `DB*`
   was never closed, so RocksDB background threads outlived global destruction and
   fired listener callbacks into reclaimed sink mutexes. The DB is now closed
   explicitly, with a `g_closing` guard on the sinks. Reproduced 4/~40 before the
   fix; 0/30 after.
7. **The histogram writer emitted v2 labels under a `legacy_v1` tag**, so a legacy
   file could not actually be used to reproduce old data. Labels now follow the
   selected encoding.
8. **The drain criterion required `L0==0`**, which §6 explicitly forbids as the
   stopping condition ("`L0==0` 只能作为物理排空附加条件，不能作为唯一停止条件"). A
   level can legitimately hold a file while deeper compaction settles, so the old
   condition could burn the whole `wait_for_drain_max_s` timeout for no reason. The
   drain is now the logical condition (no pending compaction bytes, no immutable
   memtable, no pending or running compaction); `L0==0` is opt-in via
   `--drain_require_l0_zero=1`, which the snapshot builder sets because a frozen
   starting state should have an empty L0.
9. Harness bugs found by running it: `BASE` unbound outside `snapshot.sh`; float
   compared with `-lt`; `§7` inventory globs missed the run-id prefix, so every run
   looked like it was missing its per-class histograms; the empty-DB fill control
   was op-capped before warmup ended and was correctly rejected by the §6 gate;
   `return $(...)` captured a function's stdout instead of its status; a stray quote
   in `snapshot.sh`; and the "online compaction" arms originally started a second
   process on the same DB directory, which RocksDB's LOCK refuses — they are now
   single-process mix arms, with the achieved write rate reported so the "0.5x / 1.0x
   of the knee" label is checkable rather than assumed.

One hazard was self-inflicted and is now detected rather than prevented: the driver
was rebuilt while a campaign was in flight, which is exactly the accident the
runbook records from the original campaign. `v2_run` now records the binary md5
before and after each run, writes `tool_changed.txt`, and `check_run.py` marks the
run invalid (§6 "input copy inconsistent").

## 8. Formal M1 results (first batch)

27 runs, **27/27 valid** (including the three arms that were heavily overloaded),
3 reps per offered fraction, 30 s warmup + 60 s measurement, `overwrite` from the
adopted 47 GiB base50, on a fresh hard-link replica per arm. Full table:
`out/perf_v2/m1/REPORT.md`; per-run artifacts in `out/perf_v2/m1/`.

| offered fraction | median offered ops/s | reps | median Put P99 µs | per-rep P99 µs | P99 vs baseline | median achieved/offered | median pending GiB/s |
|---|---:|---:|---:|---|---:|---:|---:|
| 0.25 | 14,988 | 3 | 98.3 | 114.7, 90.1, 98.3 | 1.00× | 1.000 | +0.04 |
| 0.50 | 29,977 | 3 | 147.5 | 163.8, 147.5, 147.5 | 1.50× | 1.000 | +0.10 |
| **0.75** | **44,966** | 3 | **1572.9** | 163.8, 1572.9, 1572.9 | **16.0×** | 0.984 | +0.29 |
| 0.90 | 53,960 | 3 | 1572.9 | 1441.8, 1572.9, 2359.3 | 16.0× | **0.888** | +0.36 |
| 1.00 | 59,955 | 3 | 1572.9 | 1572.9 ×3 | 16.0× | 0.752 | +0.30 |
| 1.10 | 65,951 | 3 | 2359.3 | 2359.3 ×3 | 24.0× | 0.766 | +0.31 |

The knee rule is evaluated on the **median across reps at each offered fraction**, and
the fraction is the controlled factor: each rep normalises against its own closed-loop
ceiling, and those ceilings differed by ~28% between reps (13,486 → 17,156 ops/s at
0.25×), so bucketing by absolute rate would leave one rep per bucket and let an
outlier rep decide the answer.

* **Tail-latency knee at 0.75× ≈ 45,000 ops/s offered.** Median Put P99 jumps from
  147.5 µs to 1572.9 µs — 16× the low-load baseline and ~10.7× the 0.50× level. It is
  present in 2 of 3 reps at 0.75× and in 3 of 3 from 0.90× up, so it is an onset rather
  than a step; the per-rep values are in the table so that is visible.
* **The throughput knee comes later, at 0.90×.** The saturation criterion
  (achieved/offered < 0.95 with pending rising) first holds at 0.90× (0.888). So the
  tail degrades roughly 9,000 ops/s *before* the delivered rate starts to sag.
* **Put p50 never moves.** 26.6–32.8 µs across the whole sweep, including 1.10×, while
  P99 rises 20×. A p50-only view would have missed the entire phenomenon.
* **`submitted/offered` is a clean saturation signature.** At 1.10× it is 0.766 ≈
  46/60 s: the client queue was ~14 s deep, so the last 14 s of scheduled arrivals were
  still waiting for their slot when the deadline hit and were never admitted
  (reported as `submitted_over_offered`; nothing admitted was lost — `censored = 0`).
  At 1.10× the reported client-side queueing is p50 4.03 s, p99 15.03 s, max 15.73 s.
* **Device write amplification falls monotonically with offered rate** (10.39 → 3.55):
  at low rates compaction has time to rewrite the same data repeatedly; at high rates
  the writes stall before they are compacted as much. This is a count-based quantity,
  so it is not affected by the erratum in §7.
* **22 of 27 arms were still moving inside 60 s** (second half ≠ first half, or pending
  rising). The 12 arms the decision depends on — one low-load reference and every arm at
  or above the knee — were re-run on fresh replicas with a 300 s window; those results
  are in `m1/REPORT.md` under "Extended-window arms" and are excluded from the knee
  aggregation above. Arms well below the knee were flagged but not extended: they are
  pacer-limited and no conclusion depends on them.

### 8.1 What the 300 s window changed (and what it did not resolve)

| offered fraction | median Put P99, 60 s | median Put P99, 300 s | ratio |
|---|---:|---:|---:|
| 0.75 | 1572.9 µs | 1441.8 µs | 0.92× |
| 0.90 | 1572.9 µs | 2359.3 µs | 1.50× |
| 1.00 | 1572.9 µs | 2359.3 µs | 1.50× |
| 1.10 | 2359.3 µs | 2359.3 µs | 1.00× |

Three things follow, and they qualify the knee claim:

1. **No extended arm converged, even at 300 s.** All twelve report `pending still
   rising` at 0.20–0.23 GiB/s, and at >= 0.90x the second half of the window delivers
   0.68–0.78x the throughput of the first half with P99 12–13x higher. This workload
   does not settle inside 300 s; `extend_arms.sh m1 --measure-s=600` is the next step if
   a stationary rate is required.
2. **Saturation is worse than the 60 s window showed.** At 1.10x the 300 s window ends
   with a client queue p99 of **137 s** and `submitted/offered = 0.509`: only half of the
   scheduled arrivals were ever admitted. The queue is unbounded, not merely deep.
3. **The knee is rep-dependent, because each rep's own ceiling drifts.** The closed-loop
   ceiling varied by ~28% between reps (a 0.75x arm ranged 40,459–51,468 ops/s), and the
   response follows the *absolute* offered rate as much as the fraction: at 0.75x one rep
   sat at achieved/offered 0.998 with P99 262 µs while another sat at 0.780 with P99
   2359 µs. The defensible statement is therefore a **band, not a point**: the sustainable
   open-loop write rate on this 47 GiB settled snapshot is roughly **40,000–50,000
   ops/s**, above which the client queue grows and Put P99 leaves the ~300 µs bucket.
   `knee.json`'s 0.75x ~= 45,000 ops/s is the middle of that band, and **M3 must not
   treat it as a sharp knee** — its rate fractions inherit this uncertainty.

## 8a. Deviations from the runbook, and why

Recorded here rather than left for a reader to discover:

1. **M1/M2/M3 arms are bounded by the measurement window, not by an op count.**
   The implicit `--num_ops = universe` that `overwrite` used to apply is disabled
   whenever a measurement window is requested, because it silently ended runs inside
   the warmup and produced empty cohorts. §4.1's build commands pass their op count
   explicitly, so nothing else changes.
2. **M2 arms are closed-loop saturation measurements, not open-loop rates.**
   §5 M2 asks for per-thread-count normalisation of open-loop rates; the arms as run
   measure each configuration's *ceiling* instead, which is what M2's own questions
   ("does read degradation come from cache size, request mix, L0 geometry or online
   compaction?") need. The per-thread-count closed-loop ceilings are measured and
   recorded (`m2_cb_t{1,4,16}_r*`), so an open-loop pass at normalised fractions is a
   mechanical follow-up. Consequence to keep in mind: `offered_ops_s = 0` and
   `client_wait_*` is null on these arms by construction — a closed-loop arm has no
   scheduled arrival, so the saturation signature is `achieved`, not `submitted/offered`.
3. **"Online compaction" is generated in-process**, by a mixed arm at
   `--read_pct=50`, not by a second writer process. Two processes cannot open one DB
   directory (RocksDB takes an exclusive LOCK; verified on this host), and §0 wants one
   measurement process at a time regardless. The achieved write rate is reported, so the
   "0.5×/1.0× of the knee" label is checkable rather than assumed.
4. **The drain criterion is the logical one** (no pending compaction bytes, no immutable
   memtable, no pending or running compaction); `L0 == 0` is opt-in via
   `--drain_require_l0_zero=1`, which only the snapshot builder sets. §6 forbids `L0 == 0`
   as the stopping condition.
5. **Latency percentiles are reported as bucket lower bounds with their bucket width.**
   The instrument's resolution is ~6–12% relative at the tail (a 2.6 ms bucket is 262 µs
   wide), so differences smaller than a bucket are not reported as differences.

## 8b. Defect found in the first formal M2 pass (and its guard)

The first formal M2 matrix completed 252 runs, all `VALID=1`, and was **wrong**.
`OPT_COMMON` was appended after each arm's own flags and carried `--threads=16` and
`--cache_size=8G`; the driver assigns on every occurrence, so the last one won and
**every swept read arm ran at 16 threads with an 8 GiB cache**. The thread-count and
cache-size sweeps collapsed into a single configuration while every run still looked
healthy — the clearest instance in this campaign of a run being valid and meaningless
at the same time.

* **Detected by** `python3 bench/v2/check_run.py --scan-stage bench/out/perf_v2/m2`:
  219 of 252 run configs contained a flag given twice with different values
  (`--threads` in 195, `--cache_size` in 144). `m1` has 0 — its results are unaffected,
  which is why the defect did not contaminate the M1 findings.
* **Fixed** by removing both flags from `OPT_COMMON`; arms that want the default now
  state it themselves. Validated on a throw-away smoke pass: 0 conflicts, and the
  thread sweep now separates cleanly (1 thread 523 K ops/s → 4 threads 1.34 M →
  16 threads 1.81 M on the smoke database).
* **Guarded** so it cannot recur silently: every run's argv is checked for
  conflicting duplicate flags and a run carrying one is now **invalid** at the §6 gate
  (`check_run.py`, check `no_conflicting_flags`), with a stage-wide scan available for
  reviewing a whole matrix before believing it.
* **The 252 affected runs are kept** in `out/perf_v2/m2_INVALID_flagconflict/` with a
  `WHY_INVALID.md` explaining the defect — the runbook asks for failed runs to be
  retained with their reason — and **no number from that directory may be used**. M2 was
  re-run with the corrected matrix; the §7 bundle dated 2026-09-26T074200 contains the
  invalid M2 data and is superseded by the bundle produced after the re-run.
* **Consequence for the report's earlier claims:** the "M2 84/84 valid" pipeline proof
  in §5 was a *pipeline* check on the smoke matrix, which had the same defect; it proved
  the harness runs end to end, not that the sweeps varied. Nothing in §8 (M1) depends on it.

## 8c. A measurement caveat found while checking M2: the page cache

When the archived and corrected `m2_cb_t1_r1` arms were compared to explain a 15x
difference in single-thread read throughput (13,928 vs 211,431 ops/s), the difference
turned out to be **the operating-system page cache, not the flags**: the archived arm
read 3.0 GiB from the device, the corrected one read 0.0 GiB.

This host has 62 GiB of RAM and the database is 47 GiB, so after warm-up a read arm can
be answered entirely from the page cache. Dropping it needs root
(`/proc/sys/vm/drop_caches`), which this campaign does not have — the same permission
that made `fstrim` fail in the preflight. So the page-cache state is **reported per arm
rather than controlled**: the M2 table now carries `dev_read_gib` and
`read_bytes_per_op`, and the M2 report states how many arms were served under 0.05 GiB
from the device. Read results from this host describe *RocksDB's block cache on top of a
warm page cache*; they are not device-bound read performance, and any arm with
`read_bytes_per_op ≈ 0` should not be quoted as an NVMe result.

## 9. Formal M2 results

252 runs, 84 arms x 3 reps, 0 invalid (`out/perf_v2/m2/REPORT.md`; the first pass is in
`m2_INVALID_flagconflict/` and is not used, §8b). Deep point reads over the whole 47 GiB
snapshot, medians of 3 reps, 8 GiB block cache:

| threads | 1 | 2 | 4 | 8 | 16 | 32 |
|---|---:|---:|---:|---:|---:|---:|
| ops/s | 182 392 | 320 952 | 588 247 | 1 098 974 | 1 707 925 | 1 725 455 |
| hit p99 µs (recent-heavy mix) | 4.6 | 5.1 | 6.1 | 7.2 | 7.7 | 10.2 |

* **Sixteen threads is the knee**: doubling to 32 adds 1 % throughput and doubles the
  hit p99. The same shape appears independently in M5's get arms (163 K -> 1.75 M at
  16 threads -> 1.72 M at 32), which are a different matrix on the same snapshot.
* **The block cache is consistently worth something and never reaches the pre-registered
  bar.** At 1 thread, 8 GiB beats 512 MiB/64 MiB by 41 % (182 K vs 129 K); at 16 threads
  by 12-23 %. Both are below the 1.5x rule, so the M2 and M5 tables record it as
  *unresolved* rather than as an effect. 512 MiB and 64 MiB are indistinguishable from
  each other, i.e. this is a threshold ("enough cache for index/filter blocks"), not a
  size curve.
* **Misses are cache-insensitive** (1.29-1.30 M ops/s at 1 thread for every cache size),
  which is the expected signature of a query that must consult filters but not data.
* **232 of 234 read arms served under 0.05 GiB from the device** - the matrix measures
  the block cache on top of a warm page cache, not the device (§8c). The report says so
  in its own header; the read_bytes_per_op column is in every run's summary.
* The throughput medians moved by up to 1.17x between the *first* M2 pass and this one
  on the same nominal arms, which is the run-to-run noise floor quoted in the
  unresolved-effect rule (1.5x) rather than evidence of anything.


**Where the numbers live.** The per-arm tables are `<matrix>/REPORT.md`; the synthesized
cross-matrix results (knees, scaling, cache effects, the three follow-on experiments) are
in `out/perf_v2/RESULTS_SUMMARY.md`, with `out/perf_v2/synthesis/*.txt` holding the
group-by-axis medians each statement rests on (produced by `bench/v2/matrix_synthesis.py`).

## 10. Impact on previously published results

Because §2.1's encoder was used for the whole 87-run bottleneck campaign, every
latency percentile in that campaign's reports is a v1 label. All 81 runs with a
`summary_extra.csv` decode cleanly (0 undecodable), so the historical data can be
corrected without re-running:

```bash
python3 bench/v2/relabel_legacy_hist.py --in <run>.hist.csv --out <run>.v2.csv
```

| reported | corrected (bucket lower bound) | source |
|---|---|---|
| `default` p50 69.6 µs | **30.3 µs** | `out/I/bn_default_r5` |
| `default` p99 6553.6 µs | **2359.3 µs** | `out/I/bn_default_r5` |
| `noc` p99 113.0 µs | **47.5 µs** | `out/I/bn_noc_r*` |
| "p99 pinned at 6.55–6.61 ms" | **2.36–2.41 ms** | 8 capacity arms |
| "`noc` p99 58x better" | **~50x** | ratio of corrected values |
| "`f1c12` p50 +41%" | **~+2% (within one bucket)** | `out/I/bn_f1c12_r*` |

Throughput, write amplification, stall counts, device counters and L0 geometry came
from counters, not from this histogram, and are unaffected. The `f1c12` p50 row is a
qualitative change: an arm was called the single significantly worse combination on
a +41% p50 that was an artifact of which bucket the value landed in.

## 11. Harness defects found while preparing M3-M5

Two more were caught by a **dry-run mode** added for exactly this purpose
(`V2_DRYRUN=1` prints each arm's exact command and records a config the conflict
scanner reads, without cloning a replica or opening a database):

* **§5 M4 had a single rep** where the runbook asks for three screening reps
  ("每臂 3 次筛选"). A rep loop was added: M4 is now 12 geometries x 3 reps x
  (build + two reads) = 108 runs.
* **M4's loop variable `rc` shadowed the script's exit code**, so the matrix would have
  reported `exit: deep: numeric argument required` and a failed stage even after a
  successful run, stopping the chained campaign there. Renamed to `rmix`.

Dry-run summaries before committing the runs, all with **0 conflicting flags**:
M3 39 arms, M4 108 arms, M5 75 arms.

## 11b. Incident: a running matrix was spliced by a mid-flight edit

While the corrected M2 sweep was running, `bench/v2/run_matrix.sh` was edited. **bash
reads a script incrementally**, so the running process resumed at a byte offset that no
longer matched the text it had read, hit garbage (`line 336: t: command not found`) and
then fell into the M1 sweep: a process whose log says `target=m2` finished its 252 M2
arms and started re-running **M1 arms into the M1 result tree** (8 arms between 17:39
and 17:53, overwriting the rep-1 files of the M1 first round).

This is the hazard the runbook names in §2 ("运行期间禁止编辑脚本") and which the
harness already guarded for the *binary* (`tool_changed.txt`); the scripts had no such
guard, so the rule rested on discipline and the discipline failed.

Actions taken:

* the spliced process tree was stopped; all other measurement processes were confirmed
  stopped before anything else was started, so no two measurements overlapped;
* **structural fix**: `run_matrix.sh`, `layout_bloom.sh` and `campaign_v2.sh` now re-exec
  themselves from a private copy taken at startup (`V2_SELF_FROZEN`, copy in
  `/tmp/v2run.*`, original script directory carried in `V2_SCRIPT_DIR` so path
  resolution still points at the real tree). Editing the live file can no longer affect
  a running campaign;
* **M1 is being re-run in full** (all three reps, then the 300 s extension), because its
  rep-1 files were replaced by runs of unknown provenance and mixing the two would leave
  the M1 dataset internally inconsistent. Today's closed-loop ceiling for the same arm
  is 83,236 ops/s against 59,955 on 2026-09-25 — a 1.39x difference on unchanged flags —
  which is itself worth recording as a machine-state effect (the 47 GiB database's page
  cache is far warmer after the M2 sweep than it was before it);
* **the M2 sweep is being sample-verified rather than trusted**: its arms look
  self-consistent (thread and cache sweeps move in the right direction, 0 conflicting
  flags, correct configs) but the process that produced them had executed spliced text,
  so three of its arms are re-run with the same frozen binary and flags and compared
  directly (`bench/v2/m2_verify_sample.sh`). If they disagree, the M2 sweep is re-run.

Nothing else was running at the time, so no two measurements overlapped: the damage is
confined to the M1 result tree plus the loss of the recovery time.

## 12. Test suites run

| suite | what it covers | result |
|---|---|---|
| `bench/tests/test_logparser.py` (pre-existing) | the LOG parser used by every analysis script | **752 checks, 752 passed, 0 failed** |
| P0 acceptance (`p0_acceptance.sh` + `p0_verify.py`) | buckets, counts/classes, scan checksum, polling/drain invariance | **PASS** (§3) |
| `relabel_legacy_hist.py --selftest` | legacy label encode/decode round-trip over reachable buckets | 1,000,013 samples, **0 failures** (§1a check 2) |
| `check_run.py --scan-stage` | conflicting duplicate flags across a whole matrix | m1 0, m3 0, m4 0, m5 0, m2 first pass 219 (§8b) |
| dry-run of M3/M4/M5 | arm command construction before committing the runs | 39 / 108 / 75 arms, 0 conflicts (§11) |

**One interference had to be checked.** The pre-existing suite generates its fixtures by
running `db_bench` (two 400k-op workloads), and that overlapped one M2 arm
(`m2_mixed_t1_c512M_r1`, started 11:09:22). Interference would show up as a rep outlier,
so the arm was compared against its cache-size siblings in the same rep block: c8G
215,445 → c512M 155,920 → and the same ~30% step appears for `recent` (284,920 →
156,605) and `deep` (182,392 → 123,393) while `negative` is unaffected (bloom-dominated,
1.29M vs 1.33M). The arm follows the expected cache-size pattern, so no M2 arm was
perturbed; the overlap is recorded here rather than left implicit. Later suites were
queued behind the matrices instead of run alongside them.

Every arm above also reports **0.0 GiB of device reads** at 1 thread, which confirms §8c
independently: on this host the read matrices measure caches, not the device.

## 13. What is left

**Status at 2026-09-30 13:20.** M1-M5 are complete (51 + 252 + 39 + 108 + 75 runs, 0
invalid) and analysed; the L0-only layout x Bloom experiment is complete except for its
load-extension block, whose first pass the gate rejected and which is being re-run; the
L0 backlog matrix is complete (90/90 valid); the backlog K sweep (100 runs) is running and
the `finalize_chain.sh` that follows it will merge/analyse it, run the replication check,
re-run the load block and rebuild the bundle. `base100`'s M-matrix arms remain deferred
(~11 h).

Two more were caught by a **dry-run mode** added for exactly this purpose
(`V2_DRYRUN=1` prints each arm's exact command and records a config the conflict
scanner reads, without cloning a replica or opening a database):

* **§5 M4 had a single rep** where the runbook asks for three screening reps
  ("每臂 3 次筛选"). The rep loop was added; M4 is now 12 geometries x 3 reps x
  (build + two reads) = 108 runs.
* **M4's loop variable `rc` shadowed the script's exit code**, so the matrix would
  have reported `exit: deep: numeric argument required` and a failed stage even after
  a successful run — the chained campaign would have stopped there. Renamed to `rmix`.

Dry-run summaries before committing the runs, all with **0 conflicting flags**:
M3 39 arms, M4 108 arms, M5 75 arms.


```bash
bash bench/v2/campaign_v2.sh formal            # M1, M2, then M3-M5 and the full export
V2_BASE100=1 bash bench/v2/campaign_v2.sh formal   # include the 100 GB snapshot (§4.2)
```

* **M3–M5 are implemented but not started.** §5 and §7 both say to run them only after
  M1/M2 hold up, so they wait on this first batch's review.
* **base100 is deferred** (`V2_BASE100=1`): §4.2 wants it after the 50 GB round is
  accepted, and nothing in M1–M5 at 50 GB needs it.
* **Replicas are hard-linked, not copied.** A 47 GiB `cp -a` per arm would need ~1.4 TB
  for M1 alone; `bench/mkreplica.sh` hard-links the immutable SSTs (~1 GB of extra
  space per arm) and each single-use replica is deleted once its run and verification
  are recorded. Without this the matrix cannot run on this volume at all — it was the
  first thing the formal run exposed.
* **Runtime.** M1 (27 runs at 30 s + 60 s plus a replica and a verification each) took
  ~55 minutes; the 12-arm extension pass ~80 minutes; M2 is 126 runs of the same shape.
  The wall-clock is dominated by the number of arms, not by the window length.

## 14. The three follow-on experiments (layout/Bloom, backlog, K sweep)

Their data lives beside the M matrices and each has its own analysis file. Headlines and
the state of each:

* **L0-only layout x Bloom** (`layout_bloom/`, 174 runs; 138 valid): with 2 M keys
  entirely in L0, `narrow` costs 1.000 Bloom checks per in-range miss and `wide` 16.000
  (K=16, one per file). That is worth 982 K vs 291 K ops/s and p99 2.46 vs 6.55 µs;
  16-bit filters cut the false-positive rate from 0.0096 to 0.0008 without changing the
  probe count, and with Bloom disabled the wide layout costs 27 924 ns/Get.
  **Its load-extension block (threads/open-loop/cache/direct-read) was rejected by the
  validity gate and is being re-run**: 24 arms passed a swept flag twice
  (`--cache_size=['8G','512M']`) because `arm_bloom` hard-coded defaults ahead of the
  swept value, and all 12 direct-read arms exited rc=2 because the tool had no
  `--use_direct_reads` switch at all (`use_direct_io` is the flush/compaction option and
  the plan says explicitly not to read one as the other). Both are fixed; the re-run
  freezes the first pass's rate (35 955 ops/s) so its open-loop arms are paired, and the
  first pass's 18 valid arms are kept in `layout_bloom/load_rev1/` as a cross-revision
  control.
* **L0 backlog: layout x size x {frozen, compaction-only, mixed}** (`l0_backlog/`,
  90/90 valid): repaying a backlog costs 6.4-11.1 GiB of compaction work per GiB of
  backlog, falling as the backlog grows; drain 14 / 45 / 159 s and peak 43.7 / 49.4 /
  63.0 GiB for 0.5 / 2 / 8 GiB on the narrow layout. The layout contrast shows up exactly
  where the L0-only experiment predicts - 4.12 vs 19.12 checks/Get at K=16, i.e. the same
  15 extra candidate files - but *not* in frozen-read throughput on this 46 GiB base,
  where the reads are device-bound and the per-class composition moves with B. See
  `out/perf_v2/RESULTS_SUMMARY.md` §7 for the full caveat.
* **L0 backlog: K sweep at B = 2 GiB** (`l0_backlog_kscan_K{1,2,4,8,16}/`, 100 runs):
  in progress. One base instance, three groups per K (A frozen reads, B auto-compaction,
  C business load at the shared absolute rate 72 526 ops/s), 5 paired reps, every run id
  carrying its K. The K=16 arms repeat arms the backlog matrix already ran, so the two
  sets also serve as a replication check (`l0_kscan_replicate.py`).

Two analysis defects were found and fixed while writing this section, both of which had
already produced numbers in earlier drafts:

* the analyzer built its record list from every `manifest.jsonl` row, and a re-run appends
  a second row for the same run id, so a repaired rep was counted twice (run counts
  doubled; every byte total doubled). It now keeps the last row per run id - the one whose
  artifacts are on disk.
* the byte columns summed over the group's runs while the per-GiB columns divided that sum
  by a single backlog, folding the repetition count into a ratio a reader compares against
  one backlog (5x for 5 reps). They are now per-run medians.

## 15. Instrument identity per matrix

This is reconstructed from artifact timestamps and the recorded per-run configuration,
because the binaries themselves were overwritten by rebuilds (defect D8: bundles do not
carry them). The current `bench/tools/l0bench` is md5 **c1241e4f5019dbd739d0845aa3a04d3e**,
mtime 09-29 09:33.

| matrix | run window (artifact mtimes) | tool revision | evidence |
|---|---|---|---|
| M2 | 09-28 10:58 .. 20:55 | l0bench rev A | ran before the 09-29 09:33 rebuild |
| M1 | 09-28 17:58 .. 20:50 | l0bench rev A | same; the two windows overlap in the evening and the overlap is recorded in §12 |
| M3 | 09-28 20:56 .. 22:02 | l0bench rev A | same |
| M5 | 09-29 00:20 .. 02:24 | l0bench rev A | same |
| M4 | 09-29 09:40 .. 11:57 | l0bench **rev B** = c1241e4f | the 09:33 rebuild precedes every M4 run; it was made for the M4 fixes (mixed-mode op accounting and the two-pass salted verify, §8b/§11) |
| layout/Bloom, first pass | 09-28 14:29 .. 09-29 15:45 | `l0bench_layout` fafe148c | the binary is unchanged on disk; its first pass's configs all name it |
| L0 backlog, first phase | 09-29 04:01 .. 18:40 | `l0bench_backlog` rev A then rev B | 18 runs (rep 1) before the 17:29 rebuild, 72 after; the two builds differ by two added, unpassed options (§14) |
| L0 backlog, K sweep | 09-30 13:02 .. | `l0bench_backlog` rev B (612cd91a / 2908281f) | recorded in `KSCAN_STATUS.log` and in every run's manifest row |

Rev A of l0bench no longer exists on this host and cannot be diffed; what can be said
without it is bounded by the artifacts:

* every M1/M2/M3/M5 run passed the §6 gate with `count_conservation` and
  `read_write_split` PASS, i.e. the per-class counters the reports quote are internally
  consistent, whatever the `ops_ok` bookkeeping defect did on screen;
* no run anywhere has a `tool_changed.txt` (the flag `v2_run` writes when the binary's
  hash changes while a run is in flight), so no single run straddles a rebuild;
* rev B was built for M4's defects. M4 quotes throughput from the cohort counters and
  reports the mixed-mode op counts, so its *absolute* numbers carry "rev B" as part of
  their identity - which is why §15 exists at all.

## 16. Defects found after the §6 list

| # | defect | effect on data | disposition |
|---|---|---|---|
| D5 | `build_state` used `cp -al`, so the state's WAL/MANIFEST inodes were shared with the base | the base's empty WAL `002824.log` shows an mtime 18:21 (last state build); MANIFEST/OPTIONS/CURRENT untouched, and all 36 state builds reported `base_l0_files=0` | fixed: `mkreplica.sh` (SSTs hardlinked, mutable files copied); the K sweep runs on it |
| D6 | analyzer counted every manifest row (re-runs duplicated) and summed bytes across runs | run counts and byte totals doubled in the backlog report; per-GiB ratios 5x too large | fixed; both reports regenerated (§14) |
| D7 | `arm_bloom` passed swept flags twice; the layout tool had no `--use_direct_reads` | 36 of the 174 layout/Bloom runs invalid (24 ambiguous config, 12 rc=2) | fixed in code; re-run queued (§14) |
| D8 | `export_run.sh` bundles do not include the tool binaries | provenance is by hash only; a bundle cannot be re-run without the matching build | open: add `bench/tools/*` (or their sources + build script) to future bundles |
| D9 | a leaked `proc_sampler` subshell survived P0 for 4 days | holds a deleted log fd and the P0 `flock` (fd 9), so a new `p0_acceptance.sh` would refuse to start; no measurement effect | killed; the sampler should be reaped by its pid file on every exit path |
| D12 | the protocol cannot drop the page cache (no root), so a frozen-read sweep **warms its own cache as it runs**: the same arm's reps measured 15.6 K → 104.6 K ops/s and 3.5 → 1.5 GiB of device reads (K scan, K=1, 5 reps) | a median over such reps mixes a cold and a warm regime; the first phase's A table has the same property, which is why M2's arms (run later on an already-warm snapshot) look different from the backlog arms' | disclosed, not fixable without root: the analyzer prints a per-rep trajectory with device-read volume for every frozen arm, and the K sweep's design (every K sees every rep in a shuffled order) keeps the across-K contrast intact |
| D10 | `export_run.sh --all` enumerated only `m1`–`m5` and `p0`, so the three follow-on experiments (layout/Bloom, backlog, K sweep) were absent from every bundle | the bundles that were handed over contain 16 k files, none of them from those experiments; the data was never at risk, only its shipment | fixed: the roots, the tool binaries, the build scripts and the current analyzers are all in the list; the bundle is rebuilt at the end of the campaign |
