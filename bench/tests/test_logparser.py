#!/usr/bin/env python3
"""Self-contained validation suite for ``bench/logparser.py``.

Run it directly::

    python3 bench/tests/test_logparser.py

It does three things:

1. parses the checked-in smoke log (``bench/smoke/db/LOG``, a 2 s ``db_bench`` fillrandom
   run) and asserts concrete numbers plus the cross-check identities between parsed sums
   and the log's own statistics counters;
2. generates two *fresh* logs with the native ``db_bench`` (one ``fillrandom`` with a
   small write buffer and a small level-1 budget so that both L0->base and deep L1->L2
   compactions happen, and one ``overwrite --use_existing_db=1`` so that non-L0-input
   compactions dominate), parses them and asserts that more than one ``class`` appears;
3. checks the failure modes: a log truncated in the middle of an ``EVENT_LOG_v1`` JSON
   object, a truncated plain line, an empty file, binary garbage, an unknown event type,
   and the CSV/JSON artifact contract.

Everything it creates lives under ``bench/tests/_scratch`` (or under ``bench/`` via
``__pycache__``) and is removed at the end unless ``KEEP_SCRATCH=1`` is set.  Nothing
outside ``bench/`` is written.

Environment overrides:
  ``DB_BENCH``  path to the db_bench binary (default ``source/rocksdb/build/db_bench``)
  ``KEEP_SCRATCH=1``  keep the generated databases and logs for inspection

Exit code 0 when every check passes, 1 otherwise.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH_DIR = os.path.dirname(HERE)
REPO_ROOT = os.path.dirname(BENCH_DIR)
sys.path.insert(0, BENCH_DIR)

import logparser  # noqa: E402  (import after sys.path setup)

SMOKE_LOG = os.path.join(BENCH_DIR, "smoke", "db", "LOG")
SCRATCH = os.path.join(HERE, "_scratch")
DB_BENCH = os.environ.get(
    "DB_BENCH", os.path.join(REPO_ROOT, "source", "rocksdb", "build", "db_bench"))
LD_LIBRARY_PATH = os.path.join(REPO_ROOT, "source", "rocksdb", "build") + \
    ":/usr/lib/x86_64-linux-gnu"

# db_bench flags for the generated logs.  The small write buffer and small level-1 budget
# are what make the run produce *several* compactions quickly: with an 8 MiB memtable and
# level0_file_num_compaction_trigger=2 the L0 compactions start within the first second, and
# max_bytes_for_level_base=16 MiB (instead of the 256 MiB default) makes L1 overflow into L2
# almost immediately, which is what yields the non-L0-input ("deep") compactions.
GEN_COMMON = [
    "--value_size=1024",
    "--threads=4",
    "--statistics=true",
    "--stats_dump_period_sec=1",
    "--compression_type=none",
    "--write_buffer_size=8388608",
    "--level0_file_num_compaction_trigger=2",
    "--max_bytes_for_level_base=16777216",
    "--target_file_size_base=4194304",
]
GEN_FILL = ["--benchmarks=fillrandom", "--num=400000"]
GEN_OVERWRITE = ["--benchmarks=overwrite", "--use_existing_db=1",
                 "--num=400000", "--writes=250000"]

RESULTS = []          # [(name, ok, detail)]
CURRENT = [None]      # name of the test function being executed


def check(ok, detail=""):
    """Record one assertion."""
    RESULTS.append((CURRENT[0], bool(ok), detail))
    if not ok:
        print("  !! FAIL: %s" % (detail or CURRENT[0]))


def eq(got, want, what):
    check(got == want, "%s: got %r, want %r" % (what, got, want))


def note(text):
    print("  .. %s" % text)


def test(fn):
    """Decorator: run one test function, turning exceptions into failures."""
    fn._is_test = True
    return fn


# --------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------

def parse(path, **kwargs):
    parser = logparser.LogParser(**kwargs)
    return parser.parse_file(path), parser


def db_bench_available():
    return os.path.isfile(DB_BENCH) and os.access(DB_BENCH, os.X_OK)


def run_db_bench(db_dir, extra):
    """Run the native db_bench once; returns the LOG path it produced."""
    env = dict(os.environ)
    env["LD_LIBRARY_PATH"] = LD_LIBRARY_PATH
    cmd = [DB_BENCH] + extra + ["--db=" + db_dir]
    note("db_bench " + " ".join(extra))
    proc = subprocess.run(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          timeout=600)
    if proc.returncode != 0:
        raise RuntimeError("db_bench failed with rc=%d:\n%s"
                           % (proc.returncode,
                              proc.stdout.decode("utf-8", "replace")[-2000:]))
    log = os.path.join(db_dir, "LOG")
    if not os.path.isfile(log) or os.path.getsize(log) == 0:
        raise RuntimeError("db_bench produced no LOG at %s" % log)
    return log


def generate_logs():
    """Generate the two logs; returns (fillrandom_log, overwrite_log) paths."""
    os.makedirs(SCRATCH, exist_ok=True)
    db_dir = os.path.join(SCRATCH, "db")
    shutil.rmtree(db_dir, ignore_errors=True)

    log1 = run_db_bench(db_dir, GEN_FILL + GEN_COMMON)
    saved1 = os.path.join(SCRATCH, "fillrandom.LOG")
    shutil.copyfile(log1, saved1)

    # Reopening rolls the LOG (RocksDB renames it to LOG.old.<ts>), so the second run's log
    # is a fresh file -- copy it out before the database is deleted.
    log2 = run_db_bench(db_dir, GEN_OVERWRITE + GEN_COMMON)
    saved2 = os.path.join(SCRATCH, "overwrite.LOG")
    shutil.copyfile(log2, saved2)
    return saved1, saved2


def expected_class(input_levels, output_level):
    """Independent re-implementation of the documented classification rule."""
    if 0 in input_levels:
        if output_level is not None and output_level > 0:
            return "L0_to_base"
        if output_level == 0:
            return "intra_L0"
        return "other"
    if output_level is not None and output_level > 0:
        return "deep"
    return "other"


def by_job_sizes(result, job_id):
    """(number of table_file_creation events, sum of their file_size) for one job."""
    created = [f for f in result["files_created"] if f["job"] == job_id]
    return len(created), sum(f["file_size"] or 0 for f in created)


def assert_class_rule(result, label):
    bad = []
    for job in result["jobs"]:
        want = expected_class(job["input_levels"], job["output_level"])
        if job["class"] != want:
            bad.append((job["job"], job["class"], want))
        if job["input_levels"] != sorted(job["input_levels"]):
            bad.append((job["job"], "unsorted input_levels", job["input_levels"]))
        if job["output_level"] is not None and job["output_level"] < 0:
            bad.append((job["job"], "negative output_level", job["output_level"]))
    eq(bad, [], "%s: class rule / input_levels ordering for every job" % label)


def assert_validation_passes(parser, label):
    """All non-informational checks must pass and the exit code must be 0."""
    checks, snapshot, code = logparser.validate(parser)
    check(snapshot is not None, "%s: no statistics dump found, cannot validate" % label)
    failed = [(c.id, c.name, c.detail) for c in checks if c.status == "FAIL"]
    eq(failed, [], "%s: validation failures" % label)
    eq(code, logparser.EXIT_OK, "%s: validation exit code" % label)
    return {c.id: c for c in checks}


# --------------------------------------------------------------------------------------
# 1. the checked-in smoke log
# --------------------------------------------------------------------------------------

@test
def test_smoke_identity():
    check(os.path.isfile(SMOKE_LOG), "smoke log missing: %s" % SMOKE_LOG)
    result, parser = parse(SMOKE_LOG)
    ident = result["run_identity"]
    eq(ident["rocksdb_version"], "11.2.0", "smoke: rocksdb_version")
    eq(ident["db_session_id"], "5KXKVS53C8VEKHPV9O2P", "smoke: db_session_id")
    eq(ident["db_id"], "9c3654a0-974a-46cf-9e0d-b64da2da7cfb", "smoke: db_id")
    eq(ident["lines"], 1351, "smoke: line count")
    check(abs(ident["end_ts"] - ident["start_ts"] - 2.369658) < 1e-6,
          "smoke: wall clock span %r" % (ident["end_ts"] - ident["start_ts"],))
    # the log was produced on this host; the recovered UTC offset must be 0 then
    eq(ident["detected_utc_offset_s"], 0.0, "smoke: detected utc offset")
    rec = result["recovery"]
    eq(rec["recovered_from_manifest"], True, "smoke: recovered_from_manifest")
    eq(rec["manifest_file_number"], 1, "smoke: manifest_file_number")
    eq(rec["last_sequence"], 0, "smoke: last_sequence")
    eq(rec["log_number"], 0, "smoke: log_number")
    eq(rec["recovered_from_wal"], False, "smoke: no WAL replay on a fresh DB")


@test
def test_smoke_event_counts():
    result, _parser = parse(SMOKE_LOG)
    counts = result["counts"]
    eq(counts["compaction_started"], 2, "smoke: compaction_started events")
    eq(counts["compaction_finished"], 2, "smoke: compaction_finished events")
    eq(counts["flush_started"], 12, "smoke: flush_started events")
    eq(counts["flush_finished"], 12, "smoke: flush_finished events")
    eq(counts["table_file_creation"], 20, "smoke: table_file_creation events")
    eq(counts["table_file_deletion"], 9, "smoke: table_file_deletion events")
    eq(counts["jobs"], 2, "smoke: compaction jobs")
    eq(counts["flushes"], 12, "smoke: flush records")
    eq(counts["stats_dumps"], 2, "smoke: statistics dumps")
    eq(counts["trivial_moves"], 0, "smoke: trivial moves")
    eq(counts["unknown_events"], 0, "smoke: unknown events")
    check(len(result["files_created"]) > 0, "smoke: files_created is non-empty")
    eq(len(result["files_created"]), 20, "smoke: files_created")


@test
def test_smoke_lsm_states_and_classes():
    result, _parser = parse(SMOKE_LOG)
    by_job = dict((j["job"], j) for j in result["jobs"])
    eq(by_job[6]["lsm_state_after"], [4, 4, 0, 0, 0, 0, 0], "smoke: job 6 lsm_state_after")
    eq(by_job[12]["lsm_state_after"], [7, 4, 0, 0, 0, 0, 0], "smoke: job 12 lsm_state_after")
    eq(by_job[6]["input_levels"], [0], "smoke: job 6 input_levels")
    eq(by_job[12]["input_levels"], [0, 1], "smoke: job 12 input_levels")
    eq(by_job[6]["class"], "L0_to_base", "smoke: job 6 class")
    eq(by_job[12]["class"], "L0_to_base", "smoke: job 12 class")
    classes = set(j["class"] for j in result["jobs"])
    check("L0_to_base" in classes, "smoke: at least one job classified L0_to_base")
    assert_class_rule(result, "smoke")

    # flush records carry the lsm state of their flush_finished event
    flush_by_job = dict((f["job"], f) for f in result["flushes"])
    eq(flush_by_job[2]["lsm_state_after"], [1, 0, 0, 0, 0, 0, 0],
       "smoke: flush job 2 lsm_state_after")
    eq(flush_by_job[2]["output_file_numbers"], [9], "smoke: flush job 2 output files")
    # the last LSM summary of the log must be the state after the last compaction
    last = result["lsm_summaries"][-1]
    eq(last["files"], [7, 4, 0, 0, 0, 0, 0], "smoke: last lsm summary")
    sources = set(s["source"] for s in result["lsm_summaries"])
    eq(sources, {"level_summary", "flush_finished", "compaction_finished"},
       "smoke: all three lsm_summaries sources present")


@test
def test_smoke_job_details_and_sums():
    result, _parser = parse(SMOKE_LOG)
    by_job = dict((j["job"], j) for j in result["jobs"])
    job = by_job[6]
    eq(job["reason"], "LevelL0FilesNum", "smoke: job 6 reason")
    eq(job["num_input_files"], 4, "smoke: job 6 num_input_files")
    eq(job["num_input_files_per_level"], {"0": 4}, "smoke: job 6 files per level")
    eq(job["input_files_per_level"], {"0": [15, 13, 11, 9]}, "smoke: job 6 input files")
    eq(job["input_data_size"], 258030666, "smoke: job 6 input_data_size")
    eq(job["output_bytes"], 246454976, "smoke: job 6 output_bytes")
    eq(job["num_output_files"], 4, "smoke: job 6 num_output_files")
    eq(job["output_file_numbers"], [16, 20, 22, 26], "smoke: job 6 output_file_numbers")
    eq(job["num_input_records"], 244561, "smoke: job 6 num_input_records")
    eq(job["num_output_records"], 233600, "smoke: job 6 num_output_records")
    eq(job["num_subcompactions"], 1, "smoke: job 6 num_subcompactions")
    eq(job["wall_micros"], 791256, "smoke: job 6 wall_micros")
    eq(job["cpu_micros"], 530672, "smoke: job 6 cpu_micros")
    eq(job["cf_name"], "default", "smoke: job 6 cf_name")
    eq(job["finished"], True, "smoke: job 6 finished")

    # every finished job must satisfy the internal count/size identities
    files_by_job = {}
    for rec in result["files_created"]:
        files_by_job.setdefault(rec["job"], []).append(rec)
    for job in result["jobs"]:
        created = files_by_job.get(job["job"], [])
        eq(len(created), job["num_output_files"],
           "smoke: job %d #table_file_creation vs num_output_files" % job["job"])
        eq(sum(f["file_size"] for f in created), job["output_bytes"],
           "smoke: job %d sum(file_size) vs total_output_size" % job["job"])

    # flush totals
    eq(sum(f["output_bytes"] for f in result["flushes"]), 768640963,
       "smoke: total flushed bytes")
    eq(sum(j["output_bytes"] for j in result["jobs"]), 448756325,
       "smoke: total compaction output bytes")
    eq(sum(j["input_data_size"] for j in result["jobs"]), 758969601,
       "smoke: total compaction input bytes")
    # flush table_properties are captured per created file
    first = result["files_created"][0]
    eq(first["file_number"], 9, "smoke: first created file")
    eq(first["file_size"], 64666688, "smoke: first created file size")
    eq(first["smallest_seqno"], 1, "smoke: first file smallest_seqno")
    eq(first["largest_seqno"], 62207, "smoke: first file largest_seqno")
    eq(first["num_entries"], 61291, "smoke: first file num_entries")
    eq(first["num_data_blocks"], 15323, "smoke: first file num_data_blocks")
    # a deleted file's size is back-filled by joining on the file number
    deleted = dict((d["file_number"], d) for d in result["files_deleted"])
    eq(deleted[9]["file_size"], 64666688, "smoke: deleted file 9 size joined from creation")
    eq(deleted[9]["type"], "table_file", "smoke: deleted file type")
    eq(deleted[9]["job"], 6, "smoke: deleted file 9 job")
    check(all(d["file_size"] is not None for d in result["files_deleted"]),
          "smoke: every deleted file has a size")

    # The last compaction of this run (job 12) was cancelled by the shutdown: its fourth
    # output file is logged with file_size 0 and the literal status "Shutdown in progress:
    # Database shutdown", and is deleted immediately afterwards.  That is the documented
    # aborted-at-shutdown shape.
    aborted = [f for f in result["files_created"] if f["file_number"] == 39]
    eq(len(aborted), 1, "smoke: file 39 creation event")
    eq(aborted[0]["file_size"], 0, "smoke: file 39 is empty")
    eq(aborted[0]["status"], "Shutdown in progress: Database shutdown",
       "smoke: file 39 status marks the shutdown abort")
    eq(deleted[39]["file_size"], 0, "smoke: file 39 was deleted")
    aborted_warnings = [w for w in result["warnings"]
                        if w["kind"] == "aborted_compaction_output"]
    eq(len(aborted_warnings), 1, "smoke: one aborted-compaction warning")
    check("job 12" in aborted_warnings[0]["text"], "smoke: the warning names job 12")
    # the report of the job itself stays intact (total_output_size is authoritative)
    eq(by_job_sizes(result, 12), (4, 202301349),
       "smoke: job 12 output count and total size")


@test
def test_smoke_options_stalls_dumps():
    result, parser = parse(SMOKE_LOG)
    opts = result["options"]
    check(len(opts) > 100, "smoke: option count %d" % len(opts))
    eq(opts["write_buffer_size"], 67108864, "smoke: write_buffer_size")
    eq(opts["level0_file_num_compaction_trigger"], 4, "smoke: level0 trigger")
    eq(opts["max_bytes_for_level_base"], 268435456, "smoke: max_bytes_for_level_base")
    eq(opts["compression"], "NoCompression", "smoke: compression option")
    eq(opts["num_levels"], 7, "smoke: num_levels")

    kinds = {}
    for stall in result["stalls"]:
        kinds[stall["kind"]] = kinds.get(stall["kind"], 0) + 1
    eq(kinds, {"begin": 4, "message": 4}, "smoke: stall kinds")
    check(all(s["text"].strip() for s in result["stalls"]), "smoke: stall text non-empty")
    begins = [s for s in result["stalls"] if s["kind"] == "begin"]
    check(all(s["text"].startswith("Stopping writes") for s in begins),
          "smoke: stall begin messages")

    eq(len(result["stats_dumps"]), 2, "smoke: stats dump count")
    dump = result["stats_dumps"][-1]
    eq(dump["tickers"]["rocksdb.flush.write.bytes"], 641739720, "smoke: flush.write.bytes")
    eq(dump["tickers"]["rocksdb.compact.write.bytes"], 379949071, "smoke: compact.write.bytes")
    eq(dump["tickers"]["rocksdb.compact.read.bytes"], 403147484, "smoke: compact.read.bytes")
    eq(dump["histograms"]["rocksdb.db.flush.micros"]["count"], 10, "smoke: db.flush.micros COUNT")
    eq(dump["histograms"]["rocksdb.db.flush.micros"]["sum"], 1485353, "smoke: db.flush.micros SUM")
    eq(dump["cumulative_stall_s"], 0.132, "smoke: cumulative stall seconds")
    eq(dump["interval_stall_s"], 0.132, "smoke: interval stall seconds")
    # the dump's ts is the 'STATISTICS:' line (the counter snapshot instant), which comes
    # ~5 ms after the 'DUMPING STATS' line that opens the block
    check(dump["ts"] - dump["dump_start_ts"] > 0.004,
          "smoke: dump ts is the STATISTICS line, not the DUMPING STATS line")

    warn_kinds = {}
    for warning in result["warnings"]:
        warn_kinds[warning["kind"]] = warn_kinds.get(warning["kind"], 0) + 1
    eq(warn_kinds, {"warn_line": 4, "aborted_compaction_output": 1},
       "smoke: warning kinds")
    check(all(w["level"] == "WARN" for w in result["warnings"]
              if w["kind"] == "warn_line"), "smoke: warning levels")

    # --stats-dump-limit keeps the first dump and the most recent ones, and never drops the
    # last one (validation depends on it)
    limited, _p2 = parse(SMOKE_LOG, stats_dump_limit=1)
    eq(len(limited["stats_dumps"]), 1, "smoke: --stats-dump-limit 1 keeps one dump")
    eq(limited["stats_dumps"][0]["tickers"]["rocksdb.flush.write.bytes"], 641739720,
       "smoke: the retained dump is the last one")


@test
def test_smoke_crosscheck_identities():
    """The cross-check identities required by the task, with explicit tolerances.

    Tolerances and their justification:

    * flush bytes -- ``rocksdb.flush.write.bytes`` is accumulated by
      ``FlushJob::RecordFlushIOStats()`` from the thread-local ``IOSTATS(bytes_written)``
      counter, which is reset in ``ReportStartedFlush()`` and read back *after* the flush has
      installed its version edit.  The MANIFEST record written by that install is therefore
      counted as flush bytes, so the counter is expected to be slightly LARGER than the sum
      of the output SST sizes.  Tolerance: 0 <= diff <= max(64 KiB, 1e-4 * counter); on this
      log the diff is +1378 B over 10 flushes (~138 B per flush, one VersionEdit record).
    * compaction bytes -- ``COMPACT_WRITE_BYTES``/``COMPACT_READ_BYTES`` are added both
      incrementally (``UpdateSubcompactionJobStatsIncrementally``) and at job end, so the
      snapshot also contains partial progress of compactions that were still running.  Only
      a one-sided bound can hold: sum(finished jobs) <= counter, with 0 tolerance on the
      bound plus a 1-byte rounding allowance.
    * counts and durations -- exact, but the counter block is read counter-by-counter over a
      millisecond, so a job finishing inside a 10 ms window around the snapshot may or may
      not be included; the checks accept the interval [count(ts <= snap), count(ts <= snap +
      10 ms)] and both bounds coincide on this log.
    * per-job file identities -- exact, tolerance 0.
    """
    result, parser = parse(SMOKE_LOG)
    snapshot = result["stats_dumps"][-1]
    cut = snapshot["ts"]
    flushes = result["flushes"]
    jobs = result["jobs"]

    # 1. flush.write.bytes vs sum(flush.output_bytes), restricted to the snapshot instant
    counter = snapshot["tickers"]["rocksdb.flush.write.bytes"]
    parsed = sum(f["output_bytes"] for f in flushes if f["end_ts"] <= cut)
    diff = counter - parsed
    eq(parsed, 641738342, "smoke: sum(flush output_bytes) up to the snapshot")
    check(0 <= diff <= max(logparser.BYTE_SLACK_ABS, logparser.BYTE_SLACK_REL * counter),
          "smoke: flush.write.bytes - sum(flushes) = %d within tolerance" % diff)
    note("flush.write.bytes %d vs parsed %d -> +%d B (%.2e)"
         % (counter, parsed, diff, diff / counter))

    # 2. compact.write.bytes >= sum(finished job output)
    counter = snapshot["tickers"]["rocksdb.compact.write.bytes"]
    parsed = sum(j["output_bytes"] for j in jobs if j["end_ts"] <= cut)
    eq(parsed, 246454976, "smoke: sum(finished job output_bytes) up to the snapshot")
    check(parsed <= counter, "smoke: compact.write.bytes >= finished job output")
    check(sum(j["output_bytes"] for j in jobs) > counter,
          "smoke: whole-log job output exceeds the snapshot counter (dump precedes run end)")
    note("compact.write.bytes %d vs parsed %d -> +%d B of in-flight compaction output"
         % (counter, parsed, counter - parsed))

    # 3. compact.read.bytes >= sum(finished job input_data_size)
    counter = snapshot["tickers"]["rocksdb.compact.read.bytes"]
    parsed = sum(j["input_data_size"] for j in jobs if j["end_ts"] <= cut)
    eq(parsed, 258030666, "smoke: sum(finished job input_data_size) up to the snapshot")
    check(parsed <= counter, "smoke: compact.read.bytes >= finished job input")

    # 4. counts and durations, all exact at this snapshot
    eq(snapshot["histograms"]["rocksdb.db.flush.micros"]["count"],
       sum(1 for f in flushes if f["end_ts"] <= cut),
       "smoke: db.flush.micros COUNT vs flush_finished count")
    eq(snapshot["histograms"]["rocksdb.db.flush.micros"]["sum"],
       sum(f["wall_micros"] for f in flushes if f["end_ts"] <= cut),
       "smoke: db.flush.micros SUM vs sum of 'Flush lasted' micros")
    eq(snapshot["histograms"]["rocksdb.compaction.times.micros"]["count"],
       sum(1 for j in jobs if j["end_ts"] <= cut),
       "smoke: compaction.times.micros COUNT vs compaction_finished count")
    eq(snapshot["histograms"]["rocksdb.compaction.times.micros"]["sum"],
       sum(j["wall_micros"] for j in jobs if j["end_ts"] <= cut),
       "smoke: compaction.times.micros SUM vs sum(compaction_time_micros)")
    eq(snapshot["histograms"]["rocksdb.compaction.times.cpu_micros"]["sum"],
       sum(j["cpu_micros"] for j in jobs if j["end_ts"] <= cut),
       "smoke: compaction.times.cpu_micros SUM vs sum(compaction_time_cpu_micros)")

    # 5. the whole validation report must be clean
    checks = assert_validation_passes(parser, "smoke")
    check(checks["1"].kind == "near-exact", "smoke: check 1 is documented near-exact")
    check(checks["2a"].kind == "bounded", "smoke: check 2a is documented bounded")
    check(checks["3a"].kind == "approx", "smoke: check 3a is documented two-sided approximate")
    for cid in ("4a", "4c", "4d", "4e", "4f", "5a", "5b", "6a", "6b", "6c", "7"):
        eq(checks[cid].status, "PASS", "smoke: check %s status" % cid)
    # check 5c is informational and must surface the shutdown-aborted job 12
    eq(checks["5c"].kind, "info", "smoke: check 5c is informational")
    eq(checks["5c"].expected, 1, "smoke: check 5c counts the one aborted-at-shutdown job")


@test
def test_streaming_and_text_paths_agree():
    """parse_file (streaming) and parse_text must produce the same result."""
    with open(SMOKE_LOG, "r", encoding="utf-8", errors="replace") as handle:
        text = handle.read()
    parser_text = logparser.LogParser()
    from_text = parser_text.parse_text(text)
    parser_file = logparser.LogParser()
    from_file = parser_file.parse_file(SMOKE_LOG)
    for key in ("jobs", "flushes", "files_created", "files_deleted", "lsm_summaries",
                "stalls", "warnings", "unknown_events", "counts", "options", "recovery"):
        eq(json.dumps(from_text[key], sort_keys=True),
           json.dumps(from_file[key], sort_keys=True),
           "smoke: parse_text == parse_file for %r" % key)
    # the module-level convenience API must agree with the class API
    from_api = logparser.parse_log(SMOKE_LOG)
    eq(from_api["counts"], from_file["counts"], "smoke: parse_log() convenience API")


@test
def test_unknown_event_type_is_recorded_not_fatal():
    header = "2026/09/21-17:29:04.225616 1159926 RocksDB version: 11.2.0\n"
    synth = header + (
        '2026/09/21-17:29:04.500000 1159926 (Original Log Time 2026/09/21-17:29:04.499999) '
        'EVENT_LOG_v1 {"time_micros": 1789982944499999, "job": 3, "event": "some_future_event", '
        '"payload": 42}\n'
        '2026/09/21-17:29:04.600000 1159926 EVENT_LOG_v1 {"time_micros": 1789982944600000, '
        '"job": 4, "event": "flush_started", "num_memtables": 1}\n'
    )
    parser = logparser.LogParser()
    result = parser.parse_text(synth)
    eq(len(result["unknown_events"]), 1, "synthetic: unknown event recorded")
    eq(result["unknown_events"][0]["event"], "some_future_event", "synthetic: unknown event name")
    eq(result["unknown_events"][0]["job"], 3, "synthetic: unknown event job")
    eq(result["counts"]["flushes"], 1, "synthetic: the known event after it still parsed")
    check(all(w["kind"] != "line_parse_error" for w in result["warnings"]),
          "synthetic: no line errors")


@test
def test_truncation_never_raises():
    with open(SMOKE_LOG, "rb") as handle:
        raw = handle.read()

    # (a) truncated in the middle of a table_file_creation JSON object
    marker = b'EVENT_LOG_v1 {"time_micros": 1789982944555851'
    idx = raw.find(marker)
    check(idx > 0, "smoke: found a JSON event to truncate")
    cases = {
        "mid-json": raw[:idx + 400],
        "mid-json-late": raw[:raw.find(b'"event": "flush_finished"') - 200],
        "no-trailing-newline": raw[:raw.rindex(b"\n")] + b"partial line without newline",
        "half-line": raw[:raw.rindex(b"\n") // 2],
    }
    for name, blob in cases.items():
        path = os.path.join(SCRATCH, "trunc_" + name + ".LOG")
        os.makedirs(SCRATCH, exist_ok=True)
        with open(path, "wb") as handle:
            handle.write(blob)
        parser = logparser.LogParser()
        result = parser.parse_file(path)   # must not raise
        check(len(result["warnings"]) > 0,
              "truncated(%s): a warning was recorded" % name)
        kinds = set(w["kind"] for w in result["warnings"])
        check(bool(kinds), "truncated(%s): warning kinds %s" % (name, sorted(kinds)))
        # everything parsed before the cut is still there and consistent
        check(result["counts"]["lines"] > 0, "truncated(%s): lines parsed" % name)
        check(all(isinstance(w["ts"], (int, float, type(None))) for w in result["warnings"]),
              "truncated(%s): warning timestamps are numeric or null" % name)
        note("truncated(%s): %d lines, warnings %s"
             % (name, result["counts"]["lines"], sorted(kinds)))

    # (b) the mid-json case must specifically report the broken event payload
    path = os.path.join(SCRATCH, "trunc_mid-json.LOG")
    parser = logparser.LogParser()
    result = parser.parse_file(path)
    kinds = set(w["kind"] for w in result["warnings"])
    check("truncated_event_line" in kinds or "event_json_error" in kinds,
          "truncated(mid-json): reports the unparsable EVENT_LOG payload, got %s" % sorted(kinds))
    check("truncated_final_line" in kinds, "truncated(mid-json): reports the missing newline")
    # and it must still have recorded the events that came before the cut
    eq(result["counts"]["flush_started"], 1, "truncated(mid-json): earlier events kept")
    eq(result["counts"]["flush_finished"], 0, "truncated(mid-json): no flush_finished yet")
    eq([w["kind"] for w in result["warnings"]].count("unfinished_job"), 1,
       "truncated(mid-json): the in-flight flush is flagged")

    # (c) an empty file and binary garbage must not raise either
    empty = os.path.join(SCRATCH, "empty.LOG")
    with open(empty, "wb"):
        pass
    garbage = os.path.join(SCRATCH, "garbage.LOG")
    with open(garbage, "wb") as handle:
        handle.write(b"\x00\xff\xfe not a log\n\nstill not a log\n")
    for path in (empty, garbage):
        parser = logparser.LogParser()
        result = parser.parse_file(path)
        eq(result["counts"]["jobs"], 0, "%s: no jobs" % os.path.basename(path))
        eq(result["jobs"], [], "%s: empty jobs" % os.path.basename(path))


@test
def test_json_contract_and_csvs():
    result, _parser = parse(SMOKE_LOG)
    required_top = ["log_path", "run_identity", "options", "recovery", "jobs", "flushes",
                    "files_created", "files_deleted", "lsm_summaries", "stalls",
                    "stats_dumps", "unknown_events", "warnings", "counts"]
    for key in required_top:
        check(key in result, "json: top-level key %r present" % key)
    for key in ("rocksdb_version", "db_session_id", "db_id", "start_ts", "end_ts"):
        check(key in result["run_identity"], "json: run_identity.%s" % key)
    for key in ("recovered_from_manifest", "last_sequence", "log_number",
                "manifest_file_number"):
        check(key in result["recovery"], "json: recovery.%s" % key)
    for key in ("jobs", "flushes", "files_created", "files_deleted", "stalls", "stats_dumps"):
        check(key in result["counts"], "json: counts.%s" % key)
    job_keys = {"job", "start_ts", "end_ts", "wall_micros", "cpu_micros", "reason", "score",
                "input_levels", "num_input_files_per_level", "input_files_per_level",
                "num_input_files", "input_data_size", "output_level", "num_output_files",
                "output_bytes", "output_file_numbers", "num_input_records",
                "num_output_records", "num_subcompactions", "lsm_state_after", "class"}
    for job in result["jobs"]:
        missing = job_keys - set(job)
        check(not missing, "json: job record has %s" % (sorted(missing) or "all required keys"))
    flush_keys = {"job", "start_ts", "end_ts", "wall_micros", "reason", "num_memtables",
                  "num_input_entries", "num_deletes", "input_data_size", "memory_usage",
                  "num_range_deletes", "output_file_numbers", "output_bytes",
                  "lsm_state_after", "immutable_memtables"}
    for flush in result["flushes"]:
        missing = flush_keys - set(flush)
        check(not missing, "json: flush record has %s" % (sorted(missing) or "all required keys"))

    # the result must survive a JSON round trip unchanged (no tuples/sets/NaN)
    blob = json.dumps(result)
    check(json.loads(blob) == result, "json: round trip is lossless")

    # CSVs
    csv_dir = os.path.join(SCRATCH, "csv")
    shutil.rmtree(csv_dir, ignore_errors=True)
    written = logparser.write_csv_dir(result, csv_dir)
    eq(len(written), 8, "csv: eight files written")
    expected_rows = {
        "jobs.csv": len(result["jobs"]),
        "flushes.csv": len(result["flushes"]),
        "files_created.csv": len(result["files_created"]),
        "files_deleted.csv": len(result["files_deleted"]),
        "lsm_summaries.csv": len(result["lsm_summaries"]),
        "stalls.csv": len(result["stalls"]),
        "stats_dumps.csv": len(result["stats_dumps"]),
        "warnings.csv": len(result["warnings"]),
    }
    for name, rows in expected_rows.items():
        path = os.path.join(csv_dir, name)
        check(os.path.isfile(path), "csv: %s exists" % name)
        with open(path, "rb") as handle:
            blob = handle.read()
        check(blob.endswith(b"\r\n"), "csv: %s uses RFC4180 CRLF line endings" % name)
        text = blob.decode("utf-8")
        eq(len(text.rstrip("\r\n").split("\r\n")), rows + 1,
           "csv: %s has a header plus one row per record" % name)
    with open(os.path.join(csv_dir, "jobs.csv"), "r", encoding="utf-8") as handle:
        header = handle.readline().rstrip("\r\n").split(",")
    for column in ("job", "class", "output_bytes", "input_files_per_level"):
        check(column in header, "csv: jobs.csv column %r" % column)
    with open(os.path.join(csv_dir, "stats_dumps.csv"), "r", encoding="utf-8") as handle:
        stats_header = handle.readline()
    check("rocksdb.flush.write.bytes" in stats_header,
          "csv: stats_dumps.csv has per-ticker columns")
    check("rocksdb.db.flush.micros [COUNT]" in stats_header,
          "csv: stats_dumps.csv has per-histogram COUNT columns")
    # RFC4180 quoting: nested values are JSON text containing commas/quotes, so they must be
    # quoted and the quotes doubled
    with open(os.path.join(csv_dir, "jobs.csv"), "r", encoding="utf-8") as handle:
        handle.readline()
        first_row = handle.readline()
    check('"{""0"":[15,13,11,9]}"' in first_row,
          "csv: nested JSON field is RFC4180-quoted with doubled quotes: %r" % first_row[:200])


@test
def test_cli_end_to_end():
    """The documented CLI must work, including --out-json/--csv-dir/--validate/--quiet."""
    out_json = os.path.join(SCRATCH, "cli.json")
    csv_dir = os.path.join(SCRATCH, "cli_csv")
    shutil.rmtree(csv_dir, ignore_errors=True)
    cmd = [sys.executable, os.path.join(BENCH_DIR, "logparser.py"), SMOKE_LOG,
           "--out-json", out_json, "--csv-dir", csv_dir, "--validate", "--quiet"]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    eq(proc.returncode, logparser.EXIT_OK,
       "cli: --validate exit code (output tail: %r)"
       % proc.stdout.decode("utf-8", "replace")[-400:])
    check(os.path.isfile(out_json), "cli: --out-json wrote the file")
    with open(out_json, "r", encoding="utf-8") as handle:
        written = json.load(handle)
    eq(written["counts"]["jobs"], 2, "cli: JSON has the jobs")
    check(b"VALIDATION" in proc.stdout, "cli: --validate printed the report")
    check(b"RESULT: PASS" in proc.stdout, "cli: validation report says PASS")
    check(b"EXACT-MATCH" in proc.stdout, "cli: report states which checks are exact")
    # absence of a statistics dump => exit code 2 (nothing could be validated)
    no_stats = os.path.join(SCRATCH, "no_stats.LOG")
    with open(no_stats, "w", encoding="utf-8") as handle:
        handle.write("2026/09/21-17:29:04.225616 1159926 RocksDB version: 11.2.0\n")
    proc2 = subprocess.run([sys.executable, os.path.join(BENCH_DIR, "logparser.py"),
                            no_stats, "--validate", "--quiet"],
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    eq(proc2.returncode, logparser.EXIT_NOT_VALIDATABLE,
       "cli: --validate without a statistics dump exits 2")


# --------------------------------------------------------------------------------------
# 2. freshly generated logs
# --------------------------------------------------------------------------------------

@test
def test_generated_logs_multiple_classes():
    if not db_bench_available():
        check(False, "db_bench not found or not executable at %s (override with $DB_BENCH)"
              % DB_BENCH)
        return
    fill_log, overwrite_log = generate_logs()
    all_classes = set()
    generated = []
    for label, path in (("fillrandom", fill_log), ("overwrite", overwrite_log)):
        size = os.path.getsize(path)
        note("%s: %s (%.1f MiB)" % (label, path, size / 1048576.0))
        check(size > 0, "%s: log is not empty" % label)
        result, parser = parse(path)
        generated.append((label, path, result, parser))
        all_classes |= set(j["class"] for j in result["jobs"])

    # (a) each generated log must satisfy the same cross-check identities
    for label, _path, result, parser in generated:
        eq(result["counts"]["jobs"], result["counts"]["compaction_finished"],
           "%s: every compaction job terminated" % label)
        check(result["counts"]["jobs"] >= 3,
              "%s: at least 3 compaction jobs (%d)" % (label, result["counts"]["jobs"]))
        check(result["counts"]["flushes"] >= 3,
              "%s: at least 3 flushes (%d)" % (label, result["counts"]["flushes"]))
        check(len(result["files_created"]) > 0, "%s: files_created non-empty" % label)
        check(len(result["stats_dumps"]) >= 2,
              "%s: several statistics dumps (%d)" % (label, len(result["stats_dumps"])))
        eq(result["counts"]["unknown_events"], 0,
           "%s: no unknown event types (recovery_started/recovery_finished/trivial_move "
           "must all be handled)" % label)
        # The only warnings a clean db_bench log may carry are genuine WARN lines plus the
        # documented aborted-at-shutdown anomaly (a compaction cancelled by Close() still
        # logs compaction_finished with a size from the in-memory statistics while its last
        # output file is never installed and is deleted immediately).  Anything else -- a
        # line parse error, an unparsable payload, an unfinished job -- must not appear.
        kinds = sorted(set(w["kind"] for w in result["warnings"]))
        check(set(kinds) <= {"warn_line", "aborted_compaction_output"},
              "%s: no unexpected structural warnings, got %s" % (label, kinds))
        for warning in result["warnings"]:
            if warning["kind"] == "aborted_compaction_output":
                check("compaction_finished" in warning["text"]
                      and "never installed" in warning["text"],
                      "%s: aborted-output warning explains itself" % label)
        assert_class_rule(result, label)
        assert_validation_passes(parser, label)

    # (b) more than one class must appear
    check(len(all_classes) > 1,
          "generated logs: more than one class appears, got %s" % sorted(all_classes))
    check("deep" in all_classes,
          "generated logs: a non-L0-input (deep) compaction appears, classes %s"
          % sorted(all_classes))
    check("L0_to_base" in all_classes,
          "generated logs: an L0-to-base compaction appears, classes %s" % sorted(all_classes))
    for label, _path, result, _parser in generated:
        for job in result["jobs"]:
            if job["class"] == "deep":
                check(0 not in job["input_levels"],
                      "%s: deep job %d has no L0 input" % (label, job["job"]))
                check(job["output_level"] > 0,
                      "%s: deep job %d writes to a level > 0" % (label, job["job"]))
            if job["class"] == "L0_to_base":
                check(0 in job["input_levels"] and job["output_level"] > 0,
                      "%s: L0_to_base job %d has L0 input and a level>0 output"
                      % (label, job["job"]))

    # (c) the overwrite run reopens the database, so it replays WAL records
    overwrite = [g for g in generated if g[0] == "overwrite"][0][2]
    eq(overwrite["recovery"]["recovered_from_manifest"], True,
       "overwrite: recovered from manifest on reopen")
    check(overwrite["counts"]["recovery_started"] == overwrite["counts"]["recovery_finished"],
          "overwrite: recovery_started/finished are paired (%d/%d)"
          % (overwrite["counts"]["recovery_started"],
             overwrite["counts"]["recovery_finished"]))
    if overwrite["counts"]["recovery_started"]:
        check(overwrite["recovery"]["recovery_finished_ts"]
              >= overwrite["recovery"]["recovery_started_ts"],
              "overwrite: recovery finished after it started")
        eq(overwrite["recovery"]["recovery_status"], "OK", "overwrite: recovery status")
    # the fillrandom run started from an empty directory
    fill = [g for g in generated if g[0] == "fillrandom"][0][2]
    eq(fill["recovery"]["last_sequence"], 0, "fillrandom: fresh DB, last_sequence 0")

    # (d) trivial moves are a real event class; if any appear they must be reported
    moves = sum(g[2]["counts"]["trivial_moves"] for g in generated)
    note("generated logs: %d trivial_move events captured" % moves)
    for _label, _path, result, _parser in generated:
        for move in result["trivial_moves"]:
            check(move["destination_level"] is not None and move["total_files_size"] is not None,
                  "trivial_move records destination_level and total_files_size")


# --------------------------------------------------------------------------------------
# runner
# --------------------------------------------------------------------------------------

def main():
    # declaration order, not alphabetical: cheap smoke-log checks first, the two db_bench
    # runs last
    tests = [fn for fn in (test_smoke_identity, test_smoke_event_counts,
                           test_smoke_lsm_states_and_classes, test_smoke_job_details_and_sums,
                           test_smoke_options_stalls_dumps, test_smoke_crosscheck_identities,
                           test_streaming_and_text_paths_agree,
                           test_unknown_event_type_is_recorded_not_fatal,
                           test_truncation_never_raises, test_json_contract_and_csvs,
                           test_cli_end_to_end, test_generated_logs_multiple_classes)]
    os.makedirs(SCRATCH, exist_ok=True)
    print("=" * 90)
    print("logparser test suite   (parser: %s)" % logparser.__file__)
    print("scratch: %s" % SCRATCH)
    print("db_bench: %s%s" % (DB_BENCH, "" if db_bench_available() else "   [NOT FOUND]"))
    print("=" * 90)
    failures = []
    for fn in tests:
        CURRENT[0] = fn.__name__
        before = len(RESULTS)
        print("\n-- %s" % fn.__name__)
        try:
            fn()
        except Exception:
            check(False, "raised %s" % traceback.format_exc().strip().splitlines()[-1])
            traceback.print_exc()
        added = RESULTS[before:]
        bad = [r for r in added if not r[1]]
        print("   %d checks, %d failed" % (len(added), len(bad)))
        if bad:
            failures.append(fn.__name__)

    total = len(RESULTS)
    failed = [r for r in RESULTS if not r[1]]
    print("\n" + "=" * 90)
    if failed:
        print("FAILED CHECKS (%d of %d):" % (len(failed), total))
        for name, _ok, detail in failed:
            print("  - %s: %s" % (name, detail))
    print("SUMMARY: %d checks, %d passed, %d failed, %d test function(s) with failures"
          % (total, total - len(failed), len(failed), len(failures)))
    print("RESULT: %s" % ("FAIL" if failed else "PASS"))
    print("=" * 90)

    if os.environ.get("KEEP_SCRATCH") != "1":
        shutil.rmtree(SCRATCH, ignore_errors=True)
    else:
        print("scratch kept at %s" % SCRATCH)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
