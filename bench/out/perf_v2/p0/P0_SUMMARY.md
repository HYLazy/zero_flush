# P0 acceptance

- binary md5: d063d79446c992a1a69c531d6492c86b
- rocksdb: /home/embed/hyl/metadata_offload/source/rocksdb
- date: 2026-09-25T22:45:27+08:00
- overall: PASS
- §6 gate over P0 workload runs: all-VALID

Artifacts: p0/hist (buckets), p0/mix (counts+classes), p0/scan (checksum),
p0/inv_open (gated polling/drain invariance), p0/inv + p0/ctrl (diagnostics).
