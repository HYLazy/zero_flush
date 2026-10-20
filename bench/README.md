# bench/ — L0 investigation harness (native RocksDB)

Everything here measures **unmodified** RocksDB. `l0bench` is a client program (it links
`source/rocksdb/build/librocksdb.so.11`); nothing in `source/` is changed by this campaign.

| file | role |
|---|---|
| `l0bench.cc` | instrumented workload driver: closed/open arrival, bursts, controlled recent-layer construction, fault injection, per-request PerfContext deltas, EventListener capture, periodic sampling of LSM layout / properties / tickers / RSS / CPU / device / directory blocks |
| `build.sh` | builds `tools/l0bench` with the library's own flags (`-O3 -DNDEBUG -march=native -fno-rtti -std=gnu++20`) |
| `logparser.py` | independent parser for the DB `LOG` event stream (flush/compaction jobs, per-level inputs, table files, stalls, statistics dumps) with a self-validator (`--validate`) |
| `analyze.py` | turns the CSVs into the quantities each experiment needs (`ledger`, `timeline`, `overlap`, `readcost`, `parallel`, `memory`, `batch`, `space`, `recovery`, `compare`) |
| `mkreplica.sh` | cheap byte-identical replica of a closed DB directory (hard-links `.sst`, copies the rest) |
| `campaign.sh` | stage driver with progress-based watchdog, stall forensics (`gdb` backtrace), disk guard and a JSONL manifest of every run |
| `CAMPAIGN.md` | the frozen protocol: environment, workload, config, metric definitions, run matrix, known limits |
| `tools/sst_dump` | built from `tools/sst_dump.cc` against the same library, for independent SST inspection |
| `out/<stage>/<run>/` | per-run outputs (CSVs, summary JSON, effective options, DB `LOG` copy, run log) |
| `out/<stage>/manifest.jsonl` | run identity: binary md5, git head, host, config, rc |

## Quick start

```bash
bash bench/build.sh                                  # build the driver
bash bench/campaign.sh A 1 2 10 3                    # 50GB fill -> read -> snapshot -> overwrite
bash bench/campaign.sh status                         # what has completed
python3 bench/analyze.py all bench/out/A/50gb_fill_r1 # ledger/timeline/... for one run
python3 bench/logparser.py bench/out/A/50gb_fill_r1/db.LOG --validate
```

`campaign.sh` keeps `bench/db/<run_id>` only when `KEEP_DB=1` (or for runs that later stages
depend on) and aborts a run if free space drops below `FREE_MIN_GB` (default 150 GiB).

## Measurement notes

* `U` (the denominator of every write-amplification ratio) is application-confirmed
  key+value bytes, counted only for successful writes.
* Flush/compaction bytes are attributed twice from independent sources — the in-process
  `EventListener` and the DB `LOG` parsed by `logparser.py` — and compared in
  `analyze.py ledger`'s cross-check table, together with the `Statistics` tickers.
* Read-path per-request counters come from `PerfContext` deltas enabled only for sampled
  operations; `O(k)` (range-candidate L0 files) is reconstructed analytically from the
  sampled LSM layout and is labelled analytic wherever it is reported.
