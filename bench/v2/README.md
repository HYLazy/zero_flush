# perf_v2 — server-side RocksDB read/write experiment harness

This directory implements `SERVER_EXPERIMENT_RUNBOOK.md` on this host. It is the
server-side half of the runbook: the measurement tool, its acceptance tests, the
snapshot builders, the M1–M5 matrix, the validity gate and the return bundle.

Everything writes under `$BENCH/out/perf_v2` (`$OUT`), matching the runbook's
`OUT=$BENCH/out/perf_v2`. Set `ROOT=/srv/rocksdbexp` to run from a restored
deployment instead of this repo; otherwise the repo containing these scripts is used.

## Run it

```bash
bash bench/v2/campaign_v2.sh smoke            # whole chain at toy scale, minutes
bash bench/v2/campaign_v2.sh formal --to-m2   # runbook parameters, stop at the §7 staging point
bash bench/v2/campaign_v2.sh formal           # full M1..M5 (hours to days of NVMe time)
```

Individual pieces:

```bash
bash bench/v2/preflight.sh                    # §1 host/device/mount/bg-task record
bash bench/v2/p0_acceptance.sh                # §3 tool acceptance (the gate)
bash bench/v2/snapshot.sh 50                  # §4.1 base50  (48M keys)
bash bench/v2/snapshot.sh 100                 # §4.2 base100 (96M keys)
bash bench/v2/run_matrix.sh formal m1         # §5 one matrix
python3 bench/v2/analyze_v2.py --out bench/out/perf_v2 --stage m1 --md m1.md
bash bench/v2/export_run.sh --all             # §7 return bundle
python3 bench/v2/check_run.py --run-dir DIR --run-id ID --rc 0   # §6 validity of one run
```

## Runbook section -> implementation

| § | Requirement | Where | Evidence |
|---|---|---|---|
| 1 | preflight, device/mount/temperature, background tasks | `preflight.sh` | `$OUT/logs/host_start.txt` |
| 2.1 | latency bucket fix + `hist_version`/`hist_encoding`/`upper_ns`, legacy parser | `l0bench.cc` `HistT` | `.hist_selfcheck.json`, `.hist_buckets.csv`, `.hist.csv`/`.hist_legacy.csv` |
| 2.1 | re-label old exports without re-running | `relabel_legacy_hist.py` | `--selftest`, plus a real 87-run export |
| 2.2 | `open->warmup->measure->drain`, cohort timestamps, service throughput | `l0bench.cc` (`--warmup_s/--measure_s`), `measure_phase.json` | `*.measure_phase.json`, `summary.json` |
| 2.3 | per-class histograms, full timestamp set, async ring detail, independent sampling, count invariants | `l0bench.cc` worker + `DetailRing` | `hist_*.csv`, `detail_*.csv`, `detail_meta.json`, `summary.json.invariant_*` |
| 2.x | `--seed` (documented but unparsed; fixed seed was impossible) | `l0bench.cc` ParseArgs | `.config.txt` argv |
| 3.1 | bucket ranges, P99<=max, v1/v2 parsers | `p0_acceptance.sh` step 1, `p0_verify.py hist` | `$OUT/p0/hist/` |
| 3.2 | 10k Put + 10k Get + 50/50, counts, both classes in detail | step 2, `p0_verify.py mix` | `$OUT/p0/mix/` |
| 3.3 | fixed scan checksum | step 3, `p0_verify.py scan` (independent FNV over the value generator) | `$OUT/p0/scan/` |
| 3.4 | `progress_ms`/`wait_for_drain` must not move cohort throughput (<1%) | step 4, `p0_verify.py invariance` | `$OUT/p0/inv_open/` (gate), `$OUT/p0/inv/` + `$OUT/p0/ctrl/` (diagnostics) |
| 4.1/4.2 | 50/100 GB settled snapshots, verified and frozen, replica per overwrite arm | `snapshot.sh`, `lib.sh v2_prep_replica` | `$BENCH/base/base50.manifest.txt`, `*.replica_check.txt` |
| 5 M1 | pure-write offered-rate sweep, 0.25–1.10 × closed-loop, noc upper bound, fill control | `run_matrix.sh m1` | `$OUT/m1/`, `m1/knee.json` |
| 5 M2 | pure point reads: threads × cache × request mix, frozen vs online | `run_matrix.sh m2` | `$OUT/m2/` |
| 5 M3 | mixed 25/50/75 at fractions of the M1 knee, 50/50 frozen contrast | `run_matrix.sh m3` | `$OUT/m3/` |
| 5 M4 | fixed working set, K∈{1,8,16} × narrow/wide × frozen/online | `run_matrix.sh m4` | `$OUT/m4/` |
| 5 M5 | threads 1–32, scan lengths 1K–1M keys, random vs sequential start, cache, online background | `run_matrix.sh m5` | `$OUT/m5/` |
| 6 | monitoring; invalid-run rules; stop on disk-low | `lib.sh v2_monitors_start`, `v2_run`, `check_run.py` | `*.mon/`, `*.validity.txt`, `manifest.jsonl` |
| 7 | per-run return content, first shipment P0+M1+M2 | `export_run.sh`, `campaign_v2.sh` | `$OUT/bundles/*.tar.gz` |
| 8 | conclusions only under the unresolved-effect rule | `analyze_v2.py` | `<stage>/REPORT.md` |

## What the acceptance test actually proves

`p0_verify.py` re-derives every expected value from the runbook's written
specification rather than reading the driver's own bookkeeping:

* **buckets** – re-implements the log-linear layout from the text, then checks the
  driver's `idx -> label/upper` against it, checks `lo <= sample_min` and
  `sample_max <= hi` on every non-empty bucket, and checks the three label cases
  the runbook names (`256..287 -> 256`, `288..319 -> 288`, `512..543 -> 512`).
* **values** – re-implements the xorshift value generator, so the scan check
  compares two independent computations of the same bytes, not one.
* **invariance** – the open-loop grid fixes the cohort by construction (offered
  rate × window), so anything that moves it is polling/drain and nothing else.

## Caveats a reader should keep

* Latency percentiles are bucket **lower bounds**; `bucket_ns` in the analysis
  tables gives the width of the bucket the percentile landed in. Report
  resolution alongside the number.
* `smoke` scale is not a result: at 200k keys everything is cache-resident.
* The `online compaction` M2/M5 arms generate compaction with a background writer
  process; the runbook only says "default online compaction", and this is how it is
  made reproducible here.
* `p0/inv` (closed-loop) and `p0/ctrl` are reported as diagnostics, with the
  same-config noise floor, so a spread can be read against what "no effect" looks
  like on this host.

## Follow-on experiments (beyond the runbook's M1-M5)

Three experiments were added after the matrices, each with its own plan, tool, driver,
analyzer and report. They live under the same `$OUT` and are shipped by
`export_run.sh --all`.

| experiment | plan | tool / driver | analyze |
|---|---|---|---|
| L0-only layout x Bloom | `LAYOUT_BLOOM_PLAN.md` | `l0bench_layout` / `layout_bloom.sh first|extend|load` | `layout_bloom_analyze.py` |
| L0 backlog: layout x size x {A,B,C} | `L0_BACKLOG_PLAN.md` | `l0bench_backlog` / `l0_backlog.sh smoke|first|prep` | `l0_backlog_analyze.py` |
| L0 backlog: K sweep at fixed B | same plan, §5 | `l0_backlog.sh kscan` via `l0_kscan_chain.sh` | `l0_backlog_analyze.py` on a merged root |

Supporting pieces added for them:

```bash
bash bench/v2/l0_kscan_chain.sh            # one invocation per K, all sharing one base
python3 bench/v2/l0_kscan_merge.py --out DIR --from ROOT... [--force]   # symlink merge for analysis
python3 bench/v2/l0_kscan_replicate.py --a DIR --b DIR --md FILE        # cross-root replication check
python3 bench/v2/matrix_synthesis.py --report m/REPORT.md --metric ops_s --group-by threads,cache
bash bench/v2/layout_load_retry.sh         # re-run the layout/Bloom load block on one revision
bash bench/v2/finalize_chain.sh            # waits for the K sweep, analyses it, re-runs the load
                                           # block, re-exports the bundle
```

Notes that matter when reading their results:

* `l0_backlog.sh` takes `LB_BASE` to point at a shared base and `V2_KLIST` to sweep K
  inside one invocation; `V2_DRYRUN=1` prints the planned arm order without touching a
  database. The ordering hash includes K, so a sweep never pools two K values into one
  state.
* `l0_backlog_analyze.py` groups by (layout, B, **K**); it de-duplicates manifest rows
  (a re-run appends a second row for the same run id) and reports byte columns **per
  run**, not summed over a group.
* `layout_bloom_analyze.py --phase load` reads the current `runs/load_*`; the first
  pass's artifacts are parked in `layout_bloom/load_rev1/` and are not mixed in.
* The replication check is not a controlled A/B: the base instance and (for the first
  phase's rep 1) the tool revision differ. It bounds run-to-run movement; it does not
  isolate anything.
