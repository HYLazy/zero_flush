# L0 investigation campaign — run protocol (native RocksDB 11.2)

This file fixes the reproducible protocol the experiment plan asks for: environment,
workload, configuration, measurement definitions, run matrix, and — explicitly — what
these runs do **not** measure. Results live in `bench/out/<stage>/` and are summarised in
`L0_INVESTIGATION_REPORT.md`.

Scope: **vanilla RocksDB only.** No ZeroFlush code, no patched tree. Every number comes
from public RocksDB APIs used by a separate client program (`bench/l0bench.cc`) plus the
OS. The RocksDB shared library and `db_bench` used here are pre-existing build artifacts
of `source/rocksdb` (unmodified since 2026-06-24).

## 1. Environment (recorded per run in `<prefix>.config.txt`)

| item | value |
|---|---|
| CPU | Intel i9-10980XE, 18 cores / 36 threads, 1 socket, NUMA node0 = all CPUs, 3.0 GHz base |
| Memory | 62 GiB total; available ≥ 50 GiB; swap 1 GiB |
| Device | WD_BLACK SN750 2 TB NVMe (single device, no RAID), `/dev/nvme0n1p3` |
| Filesystem | ext4, `rw,relatime,commit=60`, ext4 default allocation |
| Kernel | 6.8.0-124-generic |
| RocksDB | 11.2.0, `source/rocksdb/build/librocksdb.so.11`, built 2026-06-24, `-O3 -DNDEBUG -march=native -fno-rtti -std=gnu++20` |
| db_bench (cross-check only) | `source/rocksdb/build/db_bench`, md5 recorded in the run log |
| Driver | `bench/tools/l0bench`, md5 recorded per run in `manifest.jsonl` |
| Instrumentation | `bench/logparser.py` (LOG event stream, independent path) |

Machine load is recorded implicitly through the sampler (process CPU, `nvme0n1` counters)
and by `nice -n 5` on every run. Runs are **sequential** (one DB at a time) so that the
single NVMe is not shared between measured runs. File-system cache state: each measured
workload starts from whatever cache the previous phase left; the cold/hot contrast is
produced explicitly by the block-cache setting (see §4), not by assuming a cold start.

## 2. Workload

- Key: 16 B (`8 B space tag ‖ 8 B big-endian key id`). Two spaces: main (`0x0101…`) and a
  non-existent space (`0x0202…`) used for negative lookups.
- Value: 1 KiB, deterministic incompressible pattern (`FillValue`), so `--compression=none`
  byte counts are directly interpretable. A compressed variant (lz4) exists as a variant run.
- Key universes (fixed key sets, "50 GB / 100 GB at 1 KiB"):
  - 50 GB scale: `universe = 48,000,000` keys → 48.0 M × 1040 B = 49.9 GB logical.
  - 100 GB scale: `universe = 96,000,000` keys → 99.8 GB logical.
- Arrival: `--dist=uniform --order=shuffle` = a fixed-seed random permutation of the key
  universe, so a pass of `universe` writes touches **every key exactly once** (verified:
  `--mode=verify` reads all keys back). `monotonic`, `locality`, `zipf`, `hotspot` are the
  arrival-shape controls.
- Request generation: closed-loop (`--arrival=closed`, saturation) or open-loop
  (`--arrival=open --rate=R`, fixed arrival rate with an optional burst schedule). Both
  record scheduled / submit / completion timestamps.
- Read components: `recent` (top `recent_span` keys of the universe = the region a recent
  layer updates), `deep` (bottom `deep_span`), `neg` (non-existent key space), `scan`.
  Default read mix `recent:0.4,deep:0.4,neg:0.2`.
- Durability semantics: `--sync=0` (async WAL, default) for throughput work;
  `--sync=1` is used only in the E10 recovery arms, and the two are never mixed in one
  comparison.

## 3. Primary configuration ("P")

```
threads=16  value_size=1024  write_buffer_size=256M  max_write_buffer_number=4
level0_file_num_compaction_trigger=8  slowdown=20  stop=36
target_file_size_base=64M  max_background_jobs=8  max_subcompactions=8
level_compaction_dynamic_level_bytes=1  block cache=8 GiB  bloom=10 bits
compression=none  stats_dump_period_sec=60 (LOG statistics block, independent check)
sampler: properties 1 s, LSM layout 10 s, directory scan 10 s
```

Rationale: a server-plausible setting (multi-GB write buffer, several background jobs)
that is *not* tuned to make L0 look good or bad. Variants actually run:
`dynamic_level_bytes=0` (static levelled, plan §术语), `compression=lz4`,
`level0_trigger ∈ {2,4,8,16,32}` (E8), `max_subcompactions ∈ {1,4,8,16}` and
`max_background_jobs ∈ {2,4,8,16}` (E4), 10 GB scale for configuration sweeps.

## 4. Metric definitions (what each number is, and its source)

Bytes in, per measurement window:

- `U` — application-confirmed logical bytes = Σ(key+value) over **successful** writes
  (`bytes_written_logical`). Repeated versions are included, by design.
- `W_wal` — bytes appended to WAL (`rocksdb.wal_file_bytes` delta; cross-checked by the
  directory scan's `wal_bytes`).
- `W_flush` — SST bytes created by flush jobs (`OnTableFileCreated.file_size`, joined to
  flush job ids). Cross-checked against `rocksdb.flush.write.bytes` and the LOG.
- `W_intra_L0` / `W_L0base` / `W_deep` — SST bytes created by compaction jobs split by
  input levels: L0-only inputs→L0, any L0 input→level>0, no L0 input→level>0. This is the
  plan's mutually exclusive attribution; `base level` is taken from the run's actual
  output level, not assumed to be L1.
- `SST_WA = (W_flush+W_intra_L0+W_L0base+W_deep)/U`; `engine WA = (W_wal + SST)/U`;
  `device WA = nvme0n1 sectors written × 512 / U`. Only the device number is a host
  block-layer quantity; **no NAND-level figure is claimed** (the device exposes none).
- Trivial moves appear as files whose `in`/`out` file number sets coincide with no byte
  rewrite; they are excluded from the byte ledger and counted separately (LOG
  `trivial_move` events).
- Compaction read side: input bytes per source level (`in_bytes_0..6`, from
  `table_properties` of each input file). A sum of file sizes is **not** a device-read
  count; device reads are reported separately from `nvme0n1`.

Reads:

1. Range-candidate file count `O(k)` — reconstructed analytically by intersecting the read
   key with every L0 file's `[smallest,largest]` range captured in the layout sampler
   (`GetColumnFamilyMetaData`). Labelled *analytic* everywhere it appears.
2. Actual probes per request — `PerfContext` deltas on sampled ops only
   (`block_read_count`, `block_read_byte`, `block_cache_hit/filter_hit`,
   `filter_block_read`, `index_block_read`, `bloom_sst_hit/miss`,
   `get_from_memtable_count`, `internal_key_skipped`, `get_read_bytes`). PerfLevel is
   `kEnableCount` for sampled ops and disabled otherwise, so unsampled ops carry no
   overhead. **Level attribution of probes is not available from public APIs**; it is
   reported as an aggregate per request and joined with (1) for the level view.
3. Cache hits/misses — statistics tickers and PerfContext.
4. Device reads — `nvme0n1` read sectors; never attributed to a single `Get`.

Latency and resources: engine latency (submit→done) and end-to-end latency
(scheduled→done) as in-memory histograms (exact below 256 ns, 4-bit mantissa per octave
above) plus per-second series; completion rate; queueing is visible only in the open-loop
runs. Stalls: `rocksdb.stall.micros`, `OnStallConditionsChanged` transitions,
`is-write-stopped`, `actual-delayed-write-rate`. Background: per-job/per-subcompaction
begin-end timestamps, wall and CPU time, `file_write_nanos`, `file_fsync_nanos`, per-job
input/output levels and file lists, `records_replaced` (version elimination).

Space: per-interval directory scan separating `.sst`, `.log`, other, with **allocated**
bytes (`st_blocks×512`) as well as apparent size, plus `ColumnFamilyMetaData` live bytes,
plus peak RSS and memtable/immutable/cache accounting. Space amplification is quoted
against the live SST size at the same instant.

## 5. Run matrix

| stage | runs | experiment |
|---|---|---|
| A | `50gb_fill_r1`, `50gb_read`, `base50_snapshot`, `50gb_overwrite`, `50gb_mix`, `100gb_fill_r5`, `100gb_read`, `100gb_overwrite`, repeats `50gb_fill_r11/r12`, variants `50gb_fill_static`, `50gb_fill_lz4` | E1 ledger, E5 timeline, E9 space, E2/E4 inputs |
| B | `b_k{1,4,8,16}_{wide,narrow}` (build + hot read + cold read + online read each) | E2 overlap, E3 read cost, E7 memory |
| C | `c_trig{2,4,8,16,32}` on identical base replicas | E8 batching/version elimination |
| D | `d_e4_sc{1,4,8,16}`, `d_e4_job{2,4,8,16}`, `d_e6_r*`, `d_e5_*` | E4 parallelism, E6 burst buffer, E5 intervention |
| E | `e_kflush`, `e_kflush_sync`, `e_kcomp`, `e_ktime`, `e_ktime_sync` | E10 recovery boundary |

Stage B/C/D build their starting layout by **hard-linking** the `.sst` files of the
settled 50 GB fill (`bench/mkreplica.sh`; small files are copied), so every arm starts from
a byte-identical initial layout at no extra space cost — the plan's "same initial layout
replica" requirement.

Controlled recent-layer construction (stage B): the recent region is the contiguous top
`M = K × 131,072` keys. Each arm writes the **same** `M` key/value pairs (each key updated
exactly once with a deterministic value) and forces one flush per subset, so the final
logical state is identical across arms. `K` files are produced by either
- `narrow`: file *i* holds rank slice `[i·M/K,(i+1)·M/K)` → contiguous, disjoint ranges, or
- `wide`: file *i* holds ranks `≡ i (mod K)` → every file spans the whole recent region.
Both arms therefore match on file count, bytes, entries and target level, and differ only
in range overlap.

## 6. Known limits (stated up front, repeated in the report)

1. Single platform, single device, single column family, one process. No cross-device
   generality is claimed.
2. No NAND/device-internal write accounting; no power-loss (only SIGKILL) fault injection.
3. Per-level probe counts inside a `Get` are not observable without patching RocksDB, so the
   level view of read cost is analytic (`O(k)` from file ranges) joined with aggregate
   per-request PerfContext counters.
4. Block-cache-resident vs non-resident is controlled by cache size, not by dropping OS
   page cache; "cold" means the RocksDB block cache cannot hold the recent layer.
5. Background job scheduling delays (eligible→scheduled) are not exposed by public APIs;
   job wall time, concurrency and per-subcompaction timelines are reported instead.
6. Instrumentation overhead: the sampler, layout sampling and per-job listener logging cost
   CPU; the ledger is computed from listener/statistics data, so 100 % of flush/compaction
   bytes are accounted, but throughput numbers include instrumentation overhead. Sampled
   runs with `--detail_rate=0` vs sampled PerfContext runs are compared to bound this.
7. `rocksdb.estimate-table-readers-mem` is absent in this build (`n/a` where referenced).
