#!/usr/bin/env python3
"""Streaming parser for RocksDB ``LOG`` files (native/vanilla RocksDB 11.2).

This module is the evidence backbone of the metadata_offload experiment ledger:
it turns a raw RocksDB ``LOG`` into a structured, cross-checkable record of every
compaction, flush, table-file creation/deletion, LSM-state change, stall and
periodic statistics dump, and it can validate its own sums against the counters
RocksDB itself printed into the same log.

Usage
-----
    python3 bench/logparser.py <path/to/LOG> [--out-json OUT.json] [--csv-dir DIR]
                                        [--validate] [--quiet] [--stats-dump-limit N]

Standard library only.  The file is read line by line (never ``read()`` in full),
so multi-GB logs parse in constant memory *for the file*, but note that the
structured result itself grows with the number of events (see KNOWN LIMITS).

Log line shapes understood (all verified against real logs, not assumed)
-----------------------------------------------------------------------
Plain line::

    2026/09/21-17:29:04.225896 1159926 [db/version_set.cc:6694] message

Warning line::

    2026/09/21-17:29:04.706442 1159961 [WARN] [db/column_family.cc:1068] [default] Stopping writes ...

Event line (the ``(Original Log Time ...)`` prefix is optional; everything after
the literal ``EVENT_LOG_v1 `` is one complete JSON object)::

    2026/09/21-17:29:04.557716 1159928 (Original Log Time 2026/09/21-17:29:04.557546) EVENT_LOG_v1 {"time_micros": ..., "job": 2, "event": "flush_finished", ...}

Statistics dump (``--statistics=true``): a ``------- DUMPING STATS -------`` line
opens a dump, the closing ``STATISTICS:`` line is followed by ~500 *untimestamped*
counter lines.  Counters are ``rocksdb.<name> COUNT : N``; histograms are
``rocksdb.<name> P50 : f P95 : f P99 : f P100 : f COUNT : N SUM : N``.  The dump
also carries ``Cumulative stall: HH:MM:SS.mmm H:M:S, P percent`` and
``Interval stall: ...`` lines.

Event types actually handled: ``flush_started``, ``flush_finished``,
``table_file_creation``, ``table_file_deletion``, ``compaction_started``,
``compaction_finished``, ``trivial_move``, ``recovery_started`` and
``recovery_finished``.  Two of those are *not* in the task's list and were found in real
logs produced for this parser:

* ``trivial_move`` -- emitted by ``DBImpl::BackgroundCompaction`` when a compaction
  degenerates into a metadata-only file move (``db/db_impl/db_impl_compaction_flush.cc``,
  ``PerformTrivialMove``).  It has no ``compaction_started``/``compaction_finished`` pair,
  writes no bytes and therefore never shows up in the ``compact.*.bytes`` counters.  It is
  reported in the additive top-level ``trivial_moves`` array.
* ``recovery_started`` / ``recovery_finished`` -- logged when WAL records are replayed on
  reopen.  They are folded into the existing ``recovery`` object (``recovery_started_ts``,
  ``recovery_finished_ts``, ``recovery_status``, ``wal_files_on_recovery_start``).

Neither is dumped into ``unknown_events``: we do understand them.

Plain-text compaction lines (parsed for cross-checking the JSON events, never as
the primary source)::

    [default] [JOB 6] Compacting 4@0 files to L1, score 1.00
    [default] [JOB 6] Compacted 4@0 files to L1 => 246454976 bytes
    [default] [JOB 12] Generated table #33: 63917 keys, 67433770 bytes, temperature: kUnknown
    [default] [JOB 2] Flush lasted 137831 microseconds, and 92647 cpu microseconds.
    [default] Level summary: files[1 0 0 0 0 0 0] max score 0.25, estimated pending compaction bytes 0

Timestamps
----------
``EVENT_LOG_v1`` lines carry an exact ``time_micros`` epoch, which is used verbatim.
Every other line only carries a wall clock with no timezone, which is interpreted in
the *parsing* machine's local timezone and then corrected by an offset that is
auto-detected from the log itself (median of ``wall_clock - time_micros`` over the
first few hundred event lines).  On the machine that produced the log the detected
offset is 0; on a host in a different timezone it recovers the producing host's UTC
offset, which keeps ``start_ts``/``end_ts`` comparable across a multi-node campaign.
The applied offset is reported in ``run_identity.detected_utc_offset_s``.

Validation semantics (``--validate``)
-------------------------------------
``--validate`` compares the parser's own sums against the counters RocksDB printed
into the log.  Two facts about the counters are load-bearing and are why some checks
are exact while others are only one-sided bounds:

1. **The counters are a snapshot, not an end-of-run total.**  The periodic dump is
   the only place counters appear; a run that keeps working after its last dump (the
   normal case -- RocksDB never dumps counters at shutdown) leaves everything after
   that instant unaccounted for.  All counter comparisons are therefore restricted to
   events with ``ts <= snapshot_ts``.  ``snapshot_ts`` is the timestamp of the
   ``STATISTICS:`` line (the moment the counter section was produced), not the
   ``DUMPING STATS`` line, because the two are several milliseconds apart.
2. **The counter values trail that timestamp.**  The ``STATISTICS:`` section is
   formatted -- each of the ~500 counters is read individually -- *before* the line
   that carries the timestamp is written, so an event that landed in between is
   missing from the counter while its own log line is already present.  Measured on a
   loaded machine: up to ~130 ms (13 of ~930 deletion lines at ~100 deletions/s),
   which is why every check is evaluated over the interval
   ``[ts <= snapshot_ts - COUNTER_LAG_S, ts <= snapshot_ts]`` (``COUNTER_LAG_S = 0.25``),
   with the conservative end used for "must not exceed the counter" bounds and the
   inclusive end for "must not be exceeded by the counter" bounds.

Checks and their classification:

========================  ==============  ==================================================
check                     class           why
========================  ==============  ==================================================
flush.write.bytes         near-exact      ``FLUSH_WRITE_BYTES`` is added by
  vs sum(flush output)                    ``FlushJob::RecordFlushIOStats()`` from the
                                          thread-local ``IOSTATS(bytes_written)`` counter,
                                          which is reset in ``ReportStartedFlush()`` and
                                          read back *after* the flush has installed its
                                          version edit -- so the MANIFEST record written
                                          while installing the flush is counted as flush
                                          bytes.  Expected direction: counter >=
                                          sum(SST sizes), by ~one VersionEdit per flush.
                                          Tolerance: 0 <= counter - parsed <= max(64 KiB,
                                          1e-4 * counter).
compact.write.bytes       bounded         ``COMPACT_WRITE_BYTES`` is added incrementally
  vs sum(job output)                      (``CompactionJob::UpdateSubcompactionJobStats
                                          Incrementally``) as well as at job end, so the
                                          snapshot includes partial progress of compactions
                                          that are still running.  Expected direction:
                                          counter >= sum over *finished* jobs; the
                                          residual is in-flight output.
compact.read.bytes        approx          same accounting, from ``IOSTATS(bytes_read)``.
  vs sum(job input_data_size)             ``input_data_size`` is the sum of input *file
                                          sizes* (``Compaction::CalculateTotalInputSize``),
                                          whereas the counter counts bytes actually read
                                          (data + index + filter + footer blocks, block-cache
                                          hits excluded).  Expected direction: the counter is
                                          the LARGER side whenever a compaction is in flight
                                          at the snapshot (its partial progress is counted);
                                          for finished jobs a small deficit is normal too
                                          (skipped data blocks, an early-terminating
                                          compaction), so this is a *two-sided* band:
                                          parsed <= counter or |diff| <= max(8 MiB, 5 %).
                                          Measured residuals: -0.1 % to +7.6 %.
flush count               exact           ``rocksdb.db.flush.micros`` COUNT vs the number of
  vs db.flush.micros COUNT                ``flush_finished`` events, within the counter-lag
                                          window.
flush wall time           exact           ``rocksdb.db.flush.micros`` SUM vs the sum of the
                                          ``Flush lasted N microseconds`` plain lines.
compaction count          exact           ``rocksdb.compaction.times.micros`` COUNT vs the
  vs compaction.times.micros COUNT        number of ``compaction_finished`` events, within
                                          the counter-lag window.  Trivial moves do not reach
                                          this histogram.
compaction wall/cpu       exact           ``compaction.times.micros`` / ``...cpu_micros``
                                          SUM vs the ``compaction_time_*_micros`` fields of
                                          the ``compaction_finished`` events.
per-job file count        exact           number of ``table_file_creation`` events per job vs
  and size                                the job's ``num_output_files``, and the sum of
                                          their ``file_size`` vs ``total_output_size``.
                                          Jobs whose output was aborted at shutdown (an
                                          output file that was never installed -- non-OK
                                          status or file_size 0 -- and was deleted
                                          immediately) are reported separately as check 5c
                                          instead of failing 5a/5b.
text vs JSON              exact           the plain ``Compacting`` (input summary + output
                                          level), ``Compacted ... => N bytes`` and score
                                          lines vs the ``compaction_started`` /
                                          ``compaction_finished`` fields; the score is
                                          compared with |diff| <= 0.0051 because the text
                                          prints it with two decimals and exact ties may
                                          round either way.  Contradictions fail,
                                          missing text lines are informational.
files.deleted.immediately near-exact     that ticker is bumped once per
  vs delete_scheduler lines               ``file/delete_scheduler.cc`` "Deleted file ...
                                          immediately" line, for WAL and SST files alike
                                          (both log formats: the normal one and the bare
                                          trash-move-failure one), and the bump precedes the
                                          log line.  Evaluated over the same
                                          ``[snapshot - COUNTER_LAG_S, snapshot]`` interval
                                          as every other counter comparison (see point 2
                                          above), plus one line of slack for the
                                          bump-then-timestamp asymmetry: the line's timestamp
                                          is taken inside ``Logger::Logv``, i.e. after
                                          acquiring the info-log mutex.  Both bounds are
                                          printed with the check.
flushes in flight         info            ``count(flush_started)`` minus
  at the snapshot                         ``count(flush_finished)`` up to the snapshot; > 0
                                          is normal and explains why the flushed bytes of
                                          the last flush are absent from both sides.
whole-log sums vs         info            the last dump precedes the end of the run, so the
  snapshot counters                       whole-log sums include jobs the snapshot cannot
                                          contain (expected direction: whole-log >= counter
                                          for input bytes, and generally so for output).
========================  ==============  ==================================================

Exit codes: ``0`` all performed checks passed, ``1`` at least one check failed,
``2`` no counter-based check could be performed (no statistics dump in the log).

Result schema
-------------
The JSON object contains exactly the keys required by the task brief --
``log_path``, ``run_identity``, ``options``, ``recovery``, ``jobs``, ``flushes``,
``files_created``, ``files_deleted``, ``lsm_summaries``, ``stalls``, ``stats_dumps``,
``unknown_events``, ``warnings``, ``counts`` -- plus the additive keys documented
below.  Additive keys were only added where the log carries real information that
the required schema has no home for; nothing listed in the brief was renamed,
dropped or re-typed.

EXTENSIONS BEYOND THE REQUIRED SCHEMA
-------------------------------------
``trivial_moves``            top level: ``[{ts, job, destination_level, num_files,
                             total_files_size}]`` for ``trivial_move`` events.
``counts``                   extra counters: ``compaction_started``, ``compaction_finished``,
                             ``flush_started``, ``flush_finished``, ``trivial_moves``,
                             ``recovery_started``, ``recovery_finished``,
                             ``table_file_creation``, ``table_file_deletion``,
                             ``unknown_events``, ``warnings``, ``orphan_counter_lines``,
                             ``deleted_immediately_lines``, ``lines`` (the six required
                             keys keep their meaning).
``run_identity``             ``detected_utc_offset_s``, ``git_sha``, ``host``, ``lines``.
``recovery``                 ``prev_log_number``, ``next_file_number``, ``max_column_family``,
                             ``min_log_number_to_keep``, ``manifest_path``, ``db_session_id``,
                             ``recovered_from_wal``, ``recovery_started_ts``,
                             ``recovery_finished_ts``, ``recovery_status``,
                             ``wal_files_on_recovery_start``.
``stats_dumps[]``            ``histograms`` (``{name: {p50, p95, p99, p100, count, sum}}``;
                             the required ``tickers`` map keeps only ``COUNT``-style
                             counters), ``dump_start_ts`` and ``uptime_total_s``.
``flushes[]``                ``cpu_micros`` (from the ``Flush lasted`` line), ``cf_name``.
``jobs[]``                   ``cf_name``, ``oldest_snapshot_seqno``, ``finished``
                             (``false`` when the terminating event is missing, e.g. a
                             truncated log; ``output_level`` is then backfilled from the
                             plain ``Compacting ... to L<n>`` line when possible).
``files_created[]``          ``cf_name``, ``raw_key_size``, ``raw_value_size``,
                             ``num_deletions``, ``num_range_deletions``, ``status``.
``files_deleted[]``          ``status`` (the raw event field).
``lsm_summaries[]``          ``cf_name``.
``unknown_events[]``         ``raw`` (first 500 chars of the JSON payload), ``job``.
``warnings[]``               ``level`` (``WARN``/``ERROR``/... when the line carried a
                             severity tag) and ``raw`` (the complete original line).  The
                             ``kind`` values are: ``warn_line`` (a WARN/ERROR/FATAL log
                             line), ``log_message`` (an info line whose text matched a
                             corruption/failure pattern), ``aborted_compaction_output``,
                             ``unfinished_job``, ``job_id_reuse``,
                             ``truncated_final_line``, ``truncated_event_line``,
                             ``event_json_error``, ``event_not_object``,
                             ``event_dispatch_error`` and ``line_parse_error``.

KNOWN LIMITS
------------
* ``files_deleted[].file_size`` is not part of the ``table_file_deletion`` event
  (RocksDB 11.2 emits only ``job``, ``file_number`` and ``status``).  It is
  back-filled by joining on ``file_number`` with the ``table_file_creation`` events of
  the same log; it is ``null`` when the file was created before this log file starts.
  Likewise ``files_deleted[].type`` is not in the event: the parser sets it to the
  constant ``"table_file"`` (the event can only ever report table files) and keeps the
  real event field in ``status``.  There is no way to distinguish "deleted immediately"
  from "scheduled for trash" per event; the parallel ``delete_scheduler.cc`` text line
  is not paired because it also covers WAL deletions.
* A compaction that is cancelled by shutdown still logs ``compaction_finished`` with
  ``num_output_files`` and a ``total_output_size`` taken from the in-memory statistics, while
  its last output file was never installed: its ``table_file_creation`` event carries a
  non-OK ``status`` (``"Shutdown in progress: Database shutdown"``) or ``file_size: 0``, and
  the file number is deleted immediately afterwards.  For that one job
  ``sum(file_size) < total_output_size`` legitimately, so such jobs are reported through the
  additive warning kind ``aborted_compaction_output`` and informational check 5c instead of
  failing the per-job identity checks 5a/5b.  It is the last compaction of any run that
  closes the DB without letting background work finish (every ``db_bench`` run, for example).
* ``lsm_summaries`` entries sourced from ``flush_finished``/``compaction_finished``
  carry the event's ``lsm_state`` but have ``max_score`` and
  ``pending_compaction_bytes`` set to ``null`` (the events do not report them).  Only
  ``Level summary: ...`` lines carry those two fields.  The plain ``compacted to:`` and
  ``Moved #N files to level-N ...: files[...]`` lines also embed an LSM state and are
  deliberately *not* emitted, so that ``source`` stays within the three documented
  values.
* ``stalls[]`` ``begin`` entries come from the ``Stopping writes because ...`` /
  ``Stalling writes because ...`` WARN lines.  RocksDB 11.2 emits no matching "resume"
  message, so ``kind == "end"`` is only produced if a future version starts logging one
  (the message patterns are recognised); the observable end signal is the falling
  interval-stall value in the next statistics dump.  ``Cumulative stall:`` /
  ``Interval stall:`` lines are recorded as ``kind == "message"`` with the parsed
  seconds.  ``Write Stall (count): ...`` dump lines are skipped as noise.
* ``stalls[]``/``lsm_summaries[]``/``stats_dumps[]`` are DB-wide except for the
  ``cf_name`` tag; the ``STATISTICS:`` counters themselves are process-wide and cannot
  be attributed per column family.
* Memory: the file is streamed, but the result object is accumulated in full (that is
  the contract).  ``stats_dumps`` is the only unbounded-in-time part: with
  ``--stats_dump_period_sec=2`` an hour-long run produces 1800 dumps x ~500 counters.
  ``--stats-dump-limit N`` keeps the first dump plus the most recent ``N-1`` dumps, so
  memory stays bounded at the cost of the middle of the timeline; the default ``0``
  keeps everything.
* Unknown ``EVENT_LOG_v1`` event types are recorded in ``unknown_events`` and never
  crash the parser; unknown plain-text lines are ignored silently (a real log contains
  hundreds of line shapes that carry no ledger-relevant information, e.g. the block
  cache report or the per-level read-latency histograms).
"""

from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import re
import sys

# --------------------------------------------------------------------------------------
# tunables / documented tolerances
# --------------------------------------------------------------------------------------

#: How far the counter values can trail the timestamp of the ``STATISTICS:`` line they are
#: printed under, in seconds.  The counter section is formatted (each of the ~500 counters
#: is read individually) *before* that line is written, so an event that landed between the
#: read and the line is missing from the counter while its own log line is already there.
#: Measured on a loaded machine: up to ~130 ms (13 out of ~930 deletion lines at a rate of
#: ~100 deletions/s).  Every counter comparison therefore uses the conservative cut-off
#: ``snapshot_ts - COUNTER_LAG_S`` for the values that must not exceed the counter.
COUNTER_LAG_S = 0.25

#: Relative and absolute slack for the flush-byte comparison.  ``FLUSH_WRITE_BYTES``
#: also counts the MANIFEST bytes written while installing each flush, so the counter
#: is legitimately a few hundred bytes above the sum of the output SST sizes.
BYTE_SLACK_REL = 1e-4
BYTE_SLACK_ABS = 64 * 1024

#: The plain ``Compacting ... score %.2f`` line prints the compaction score rounded to two
#: decimals; exact ``.xx5`` ties may round either way, hence the slightly-widened bound.
SCORE_TOL = 0.0051

#: Two-sided sanity band for the ``compact.read.bytes`` comparison.  The counter is
#: expected to be the LARGER side while a compaction is in flight at the snapshot (its
#: partial progress is counted), but once the timeline is quiescent the two can differ in
#: either direction: index/filter/footer re-reads push the counter up, while block-cache
#: hits, skipped data blocks and an early-terminating compaction push it down.  Observed
#: residuals on real logs: -0.1 % to +7.6 %.
READ_SLACK_REL = 0.05
READ_SLACK_ABS = 8 * 1024 * 1024


EXIT_OK = 0
EXIT_CHECK_FAILED = 1
EXIT_NOT_VALIDATABLE = 2

# --------------------------------------------------------------------------------------
# regexes
# --------------------------------------------------------------------------------------

TS_FMT = "%Y/%m/%d-%H:%M:%S.%f"
_TS = r"\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d{6}"

LINE_RE = re.compile(r"^(?P<ts>" + _TS + r")\s+(?P<tid>\d+)\s+(?P<rest>.*)$")
LEVEL_RE = re.compile(r"^\[(?P<level>WARN|WARNING|ERROR|FATAL)\] (?P<rest>.*)$")
SRC_RE = re.compile(r"^\[(?P<src>[A-Za-z0-9_./-]+\.(?:cc|h):\d+)\] ?(?P<rest>.*)$")
ORIG_RE = re.compile(r"^\(Original Log Time " + _TS + r"\) (?P<rest>.*)$")
CF_PREFIX_RE = re.compile(r"^\[(?P<cf>[^\]]{1,64})\] (?P<rest>.*)$")

# statistics dump structure
DUMPING_STATS_RE = re.compile(r"^------- DUMPING STATS -------\s*$")
STATISTICS_RE = re.compile(r"^STATISTICS:\s*$")
COUNTER_RE = re.compile(r"^\s*(?P<name>rocksdb\.[A-Za-z0-9_.]+)\s+COUNT\s*:\s*(?P<count>\d+)\s*$")
HISTOGRAM_RE = re.compile(
    r"^\s*(?P<name>rocksdb\.[A-Za-z0-9_.]+)\s+"
    r"P50\s*:\s*(?P<p50>[0-9.eE+-]+)\s+"
    r"P95\s*:\s*(?P<p95>[0-9.eE+-]+)\s+"
    r"P99\s*:\s*(?P<p99>[0-9.eE+-]+)\s+"
    r"P100\s*:\s*(?P<p100>[0-9.eE+-]+)\s+"
    r"COUNT\s*:\s*(?P<count>\d+)\s+"
    r"SUM\s*:\s*(?P<sum>\d+)\s*$"
)
CUM_STALL_RE = re.compile(
    r"^Cumulative stall: (?P<hms>\d+:\d+:\d+\.\d+) H:M:S, (?P<percent>[0-9.]+) percent\s*$"
)
INT_STALL_RE = re.compile(
    r"^Interval stall: (?P<hms>\d+:\d+:\d+\.\d+) H:M:S, (?P<percent>[0-9.]+) percent\s*$"
)
UPTIME_RE = re.compile(r"^Uptime\(secs\): (?P<total>[0-9.eE+-]+) total, (?P<interval>[0-9.eE+-]+) interval\s*$")

# plain-text ledger lines
LEVEL_SUMMARY_RE = re.compile(
    r"^Level summary: files\[(?P<files>[0-9 ]+)\] max score (?P<score>[0-9.eE+-]+), "
    r"estimated pending compaction bytes (?P<pending>\d+)\s*$"
)
TXT_COMPACTING_RE = re.compile(
    r"^\[JOB (?P<job>\d+)\] Compacting (?P<summary>.+?), score (?P<score>[0-9.eE+-]+)\s*$"
)
TXT_COMPACTED_RE = re.compile(
    r"^\[JOB (?P<job>\d+)\] Compacted (?P<summary>.+?) => (?P<bytes>\d+) bytes\s*$"
)
TXT_FLUSH_LONG_RE = re.compile(
    r"^\[JOB (?P<job>\d+)\] Flush lasted (?P<wall>\d+) microseconds, and (?P<cpu>\d+) cpu microseconds\.\s*$"
)
TXT_GEN_TABLE_RE = re.compile(
    r"^\[JOB (?P<job>\d+)\] Generated table #(?P<file>\d+): (?P<keys>\d+) keys, (?P<bytes>\d+) bytes"
)
#: ``file/delete_scheduler.cc`` deletion lines.  These cover *all* file types (SST and
#: WAL), unlike the ``table_file_deletion`` events, and each one bumps
#: ``rocksdb.files.deleted.immediately`` -- which makes them a cross-check.  Two formats
#: exist: the normal one ends the ``immediately`` token with a comma before
#: ``rate_bytes_per_sec``, the trash-move-failure path logs the bare form.
TXT_DELETED_RE = re.compile(
    r"^Deleted file (?P<path>\S+) immediately(?:,|$)(?P<rest>.*)$"
)
TXT_JOB_RE = re.compile(r"^\[JOB \d+\] ")

# startup / recovery
VERSION_RE = re.compile(r"^RocksDB version: (?P<version>\S+)")
GIT_SHA_RE = re.compile(r"^Git sha (?P<sha>\S+)")
DB_SESSION_RE = re.compile(r"^DB Session ID:\s*(?P<sid>\S+)")
DB_ID_RE = re.compile(r"^DB ID: (?P<dbid>\S+)")
HOST_RE = re.compile(r"^Host name \(Env\):\s*(?P<host>.*?)\s*$")
RECOVERING_RE = re.compile(r"^Recovering from manifest file: (?P<path>.*?)\s*$")
RECOVERED_RE = re.compile(
    r"^Recovered from manifest file:(?P<path>.*?) succeeded,"
)
RECOVERED_KV_RE = re.compile(r"(?P<key>[A-Za-z_]+) is (?P<val>-?\d+)")
OPTION_RE = re.compile(r"Options\.(?P<name>[A-Za-z0-9_.\[\]]+):[ ]?(?P<value>.*?)\s*$")
WARNISH_RE = re.compile(
    r"(?i)(\bcorrupt(?:ed|ion)?\b|\bchecksum mismatch\b|\bunrecoverable\b|\bError:|\bI/O error\b|\bFailed to\b)"
)

#: ``table_file_creation`` fields promoted into ``files_created``.
_CREATION_TP_FIELDS = (
    "data_size",
    "index_size",
    "filter_size",
    "num_data_blocks",
    "num_entries",
    "raw_key_size",
    "raw_value_size",
    "num_deletions",
    "num_range_deletions",
)

#: histogram percentile fields kept per histogram name.
_HISTO_FIELDS = ("p50", "p95", "p99", "p100", "count", "sum")


# --------------------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------------------

def _ts_to_epoch(text, offset=0.0):
    """Wall-clock (naive, local) -> epoch seconds, shifted by ``offset``.

    ``offset`` is ``local_parse(wall_clock) - time_micros`` as detected from the log's
    own event lines, so subtracting it recovers the producing host's timeline even when
    this process runs in another timezone.

    The timestamp layout is fixed width (``YYYY/MM/DD-HH:MM:SS.ffffff``), so it is sliced
    by hand: ``datetime.strptime`` costs about a third of the whole parse on a real log,
    while this is a few ``int()`` calls, and the identical result is checked by falling back
    to ``strptime`` whenever the slices are not valid numbers.
    """
    try:
        return datetime.datetime(
            int(text[0:4]), int(text[5:7]), int(text[8:10]),
            int(text[11:13]), int(text[14:16]), int(text[17:19]),
            int(text[20:26])).timestamp() - offset
    except (ValueError, IndexError):
        pass
    try:
        dt = datetime.datetime.strptime(text, TS_FMT)
    except ValueError:
        return None
    return dt.timestamp() - offset


def _hms_to_seconds(text):
    """``00:00:0.132`` -> ``0.132``."""
    try:
        hours, minutes, seconds = text.split(":")
        return int(hours) * 3600 + int(minutes) * 60 + float(seconds)
    except ValueError:
        return None


def _typed(value):
    """Best-effort conversion of an ``Options.*`` value."""
    text = value.strip()
    if text == "":
        return ""
    if text in ("true", "false"):
        return text == "true"
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        return text


def _jsonable(value):
    """CSV cell rendering: scalars stay scalars, containers become compact JSON."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, str)):
        return value
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _fmt_ts(ts):
    if ts is None:
        return "-"
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S.%f")


def _fmt_bytes(num):
    if num is None:
        return "-"
    step = 1000.0
    val = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(val) < step or unit == "TB":
            return "%.1f %s" % (val, unit)
        val /= step
    return "%.1f TB" % val


def _histo_count(dump, name):
    if not dump:
        return None
    histo = dump["histograms"].get(name)
    if not histo:
        return None
    return histo["count"]


def _histo_sum(dump, name):
    if not dump:
        return None
    histo = dump["histograms"].get(name)
    if not histo:
        return None
    return histo["sum"]


def _ticker(dump, name):
    if not dump:
        return None
    return dump["tickers"].get(name)


def classify(input_levels, output_level):
    """The task's exact classification rule.

    ``input_levels`` must be sorted ascending.  ``None`` output level yields ``other``
    (neither ``> 0`` nor ``== 0``), which is what an unterminated job gets.
    """
    if 0 in input_levels:
        if output_level is not None and output_level > 0:
            return "L0_to_base"
        if output_level == 0:
            return "intra_L0"
        return "other"
    if output_level is not None and output_level > 0:
        return "deep"
    return "other"


def _input_summary(num_per_level):
    """``{0: 4, 1: 4}`` -> ``4@0 + 4@1``.

    Matches ``Compaction::InputLevelSummary()``, which *skips empty input levels* -- so a
    level that is an input level but has no file (the JSON logs ``files_L<i>: []`` for it)
    is deliberately left out here.
    """
    return " + ".join("%d@%d" % (num_per_level[lvl], lvl)
                      for lvl in sorted(num_per_level) if num_per_level[lvl])


# --------------------------------------------------------------------------------------
# the parser
# --------------------------------------------------------------------------------------

class LogParser(object):
    """Single-pass, streaming RocksDB LOG parser.

    Feed complete lines to :meth:`feed` (in order) and call :meth:`finish`, or just use
    :meth:`parse_file` / :meth:`parse_text`.
    """

    def __init__(self, stats_dump_limit=0):
        #: keep at most this many stats dumps (first + most recent N-1); 0 = keep all.
        self.stats_dump_limit = int(stats_dump_limit)

        self.line_no = 0
        self.lines = 0
        self.first_ts = None
        self.last_ts = None
        self.offset = 0.0
        self._offset_detected = False
        self.log_path = None
        self.reached_eof = False

        # run identity / config
        self.rocksdb_version = None
        self.git_sha = None
        self.db_session_id = None
        self.db_id = None
        self.host = None
        self.options = {}
        self.recovery = {
            "recovered_from_manifest": False,
            "last_sequence": None,
            "log_number": None,
            "manifest_file_number": None,
            "prev_log_number": None,
            "next_file_number": None,
            "max_column_family": None,
            "min_log_number_to_keep": None,
            "manifest_path": None,
            "db_session_id": None,
            "recovered_from_wal": False,
            "recovery_started_ts": None,
            "recovery_finished_ts": None,
            "recovery_status": None,
            "wal_files_on_recovery_start": None,
        }

        # records
        self.jobs = {}            # job id -> internal compaction record
        self.flushes = []         # internal flush records
        self._flushes_by_job = {}
        self.files_created = []
        self.files_deleted = []
        self.lsm_summaries = []
        self.stalls = []
        self.stats_dumps = []
        self.trivial_moves = []
        self.unknown_events = []
        self.warnings = []

        # text lines used only for cross-checking the JSON events
        self.text_compacting = {}   # job -> {summary, score, ts}
        self.text_compacted = {}    # job -> {summary, bytes, ts}
        self.text_generated = {}    # job -> [(file, keys, bytes)]
        self.text_flush_long = {}   # job -> {wall, cpu, ts}
        self.text_deleted_immediate = []  # [(ts, path)] from delete_scheduler.cc

        # bookkeeping
        self.counter_events = {}
        self._dump = None
        self._in_stats = False
        self._dumps_seen = 0
        self._orphan_counter_lines = 0
        self._file_sizes = {}       # file_number -> (size, job)
        self._files_by_job = {}     # job -> [files_created record]
        self._finalized = False
        self._sorted_jobs = []
        self._sorted_flushes = []

    # -- public API -------------------------------------------------------------------

    def feed(self, line, is_partial=False):
        """Consume one line (without its trailing newline).

        ``is_partial`` marks the final line of a file that had no terminating newline
        (truncated log tail).
        """
        self.lines += 1
        self.line_no += 1
        if line.endswith("\r"):
            line = line[:-1]
        try:
            self._feed(line, is_partial)
        except Exception as exc:  # never let one bad line kill a parse
            self.warnings.append({
                "ts": self.last_ts,
                "kind": "line_parse_error",
                "text": "%s: %s" % (type(exc).__name__, exc),
                "level": None,
                "raw": line[:500],
            })

    def finish(self):
        """Post-pass: resolve joins that need the whole log."""
        if self._finalized:
            return
        self._finalized = True
        for rec in self.files_deleted:
            info = self._file_sizes.get(rec["file_number"])
            if info is not None:
                rec["file_size"] = info[0]
        # index created files by job once (the identity checks are O(jobs + files))
        self._files_by_job = {}
        for frec in self.files_created:
            self._files_by_job.setdefault(frec["job"], []).append(frec)
        for job_id, frec in self._flushes_by_job.items():
            if frec["end_ts"] is None:
                self.warn(frec["start_ts"], "unfinished_job",
                          "flush job %d has a flush_started event but no flush_finished "
                          "event (truncated log or flush still running at the end)" % job_id)
        for rec in self.jobs.values():
            if rec["finished"] or rec["end_ts"] is not None:
                continue
            txt = self.text_compacting.get(rec["job"])
            if txt is not None and rec.get("output_level") is None:
                rec["_output_level_from_text"] = txt.get("output_level")
            self.warn(rec["start_ts"], "unfinished_job",
                      "compaction job %d has a compaction_started event but no "
                      "compaction_finished/trivial_move event" % rec["job"])
        # flush_started/flush_finished carry no cf_name; recover it from the flush's own
        # table_file_creation event (always present in practice).
        for job_id, frec in self._flushes_by_job.items():
            if frec["cf_name"] is None:
                for created in self._files_by_job.get(job_id) or ():
                    if created["cf_name"] is not None:
                        frec["cf_name"] = created["cf_name"]
                        break
        deleted_numbers = set(d["file_number"] for d in self.files_deleted)
        for rec in self.jobs.values():
            if not rec["finished"]:
                continue
            created = self._files_by_job.get(rec["job"])
            bad = _uninstalled_output_files(created, deleted_numbers, rec["output_bytes"])
            if bad:
                self.warn(rec["end_ts"], "aborted_compaction_output",
                          "job %d reports %d output byte(s) in compaction_finished but %d of "
                          "its %d output file(s) were never installed and were deleted "
                          "immediately (file_number %s, status %s, file_size %s): the "
                          "compaction was cancelled by shutdown and the reported size comes "
                          "from the in-memory statistics"
                          % (rec["job"], rec["output_bytes"], len(bad), len(created),
                             [f["file_number"] for f in bad],
                             [f["status"] for f in bad],
                             [f["file_size"] for f in bad]))
        self._finalize_ordering()

    def parse_file(self, path):
        """Stream ``path``; tolerates a truncated final line."""
        self.log_path = path
        self.offset = _detect_utc_offset(path)
        self._offset_detected = True
        with open(path, "rb", buffering=1 << 20) as handle:
            for raw in handle:
                if raw.endswith(b"\n"):
                    self.feed(raw[:-1].decode("utf-8", "replace"))
                else:
                    self.feed(raw.decode("utf-8", "replace"), is_partial=True)
                    self.warn(self.last_ts, "truncated_final_line",
                              "final line of the log has no terminating newline "
                              "(truncated tail)")
        self.reached_eof = True
        self.finish()
        return self.result()

    def parse_text(self, text):
        """Convenience wrapper for tests / small inputs (not for multi-GB logs)."""
        self.offset = 0.0
        lines = text.split("\n")
        last = len(lines) - 1
        for idx, line in enumerate(lines):
            if idx == last and line == "":
                break  # trailing newline
            partial = idx == last
            if partial:
                self.warn(self.last_ts, "truncated_final_line",
                          "final line of the log has no terminating newline "
                          "(truncated tail)")
            self.feed(line, is_partial=partial)
        self.reached_eof = True
        self.finish()
        return self.result()

    def result(self):
        """Build the JSON-serializable result object."""
        self.finish()
        last_dump = self.stats_dumps[-1] if self.stats_dumps else None
        out = {
            "log_path": self.log_path,
            "run_identity": {
                "rocksdb_version": self.rocksdb_version,
                "db_session_id": self.db_session_id,
                "db_id": self.db_id,
                "start_ts": self.first_ts,
                "end_ts": self.last_ts,
                "detected_utc_offset_s": round(self.offset, 3),
                "git_sha": self.git_sha,
                "host": self.host,
                "lines": self.lines,
            },
            "options": self.options,
            "recovery": self.recovery,
            "jobs": [self._job_out(j) for j in self._sorted_jobs],
            "flushes": [self._flush_out(f) for f in self._sorted_flushes],
            "files_created": self.files_created,
            "files_deleted": self.files_deleted,
            "lsm_summaries": self.lsm_summaries,
            "stalls": self.stalls,
            "stats_dumps": self.stats_dumps,
            "trivial_moves": self.trivial_moves,
            "unknown_events": self.unknown_events,
            "warnings": self.warnings,
            "counts": {
                "jobs": len(self.jobs),
                "flushes": len(self._flushes_by_job),
                "files_created": len(self.files_created),
                "files_deleted": len(self.files_deleted),
                "stalls": len(self.stalls),
                "stats_dumps": len(self.stats_dumps),
                "compaction_started": self.counter_events.get("compaction_started", 0),
                "compaction_finished": self.counter_events.get("compaction_finished", 0),
                "flush_started": self.counter_events.get("flush_started", 0),
                "flush_finished": self.counter_events.get("flush_finished", 0),
                "table_file_creation": self.counter_events.get("table_file_creation", 0),
                "table_file_deletion": self.counter_events.get("table_file_deletion", 0),
                "trivial_moves": len(self.trivial_moves),
                "recovery_started": self.counter_events.get("recovery_started", 0),
                "recovery_finished": self.counter_events.get("recovery_finished", 0),
                "unknown_events": len(self.unknown_events),
                "warnings": len(self.warnings),
                "orphan_counter_lines": self._orphan_counter_lines,
                "deleted_immediately_lines": len(self.text_deleted_immediate),
                "lines": self.lines,
            },
        }
        return out

    # -- record shaping ---------------------------------------------------------------

    def _job_out(self, rec):
        levels = sorted(rec["input_files_per_level"])
        num_per_level = {str(lvl): len(rec["input_files_per_level"][lvl]) for lvl in levels}
        output_level = rec["output_level"]
        if output_level is None and rec.get("_output_level_from_text") is not None:
            output_level = rec["_output_level_from_text"]
        out = {
            "job": rec["job"],
            "start_ts": rec["start_ts"],
            "end_ts": rec["end_ts"],
            "wall_micros": rec["wall_micros"],
            "cpu_micros": rec["cpu_micros"],
            "reason": rec["reason"],
            "score": rec["score"],
            "input_levels": levels,
            "num_input_files_per_level": num_per_level,
            "input_files_per_level": {str(lvl): list(rec["input_files_per_level"][lvl]) for lvl in levels},
            "num_input_files": sum(len(v) for v in rec["input_files_per_level"].values()),
            "input_data_size": rec["input_data_size"],
            "output_level": output_level,
            "num_output_files": rec["num_output_files"],
            "output_bytes": rec["output_bytes"],
            "output_file_numbers": sorted(rec["output_file_numbers"]),
            "num_input_records": rec["num_input_records"],
            "num_output_records": rec["num_output_records"],
            "num_subcompactions": rec["num_subcompactions"],
            "lsm_state_after": rec["lsm_state_after"],
            "class": classify(levels, output_level),
            "cf_name": rec["cf_name"],
            "oldest_snapshot_seqno": rec["oldest_snapshot_seqno"],
            "finished": rec["finished"],
        }
        return out

    def _flush_out(self, rec):
        out = {
            "job": rec["job"],
            "start_ts": rec["start_ts"],
            "end_ts": rec["end_ts"],
            "wall_micros": rec["wall_micros"],
            "reason": rec["reason"],
            "num_memtables": rec["num_memtables"],
            "num_input_entries": rec["num_input_entries"],
            "num_deletes": rec["num_deletes"],
            "input_data_size": rec["input_data_size"],
            "memory_usage": rec["memory_usage"],
            "num_range_deletes": rec["num_range_deletes"],
            "output_file_numbers": sorted(rec["output_file_numbers"]),
            "output_bytes": rec["output_bytes"],
            "lsm_state_after": rec["lsm_state_after"],
            "immutable_memtables": rec["immutable_memtables"],
            "cpu_micros": rec["cpu_micros"],
            "cf_name": rec["cf_name"],
        }
        return out

    # -- warnings ---------------------------------------------------------------------

    def warn(self, ts, kind, text, level=None, raw=None):
        self.warnings.append({
            "ts": ts,
            "kind": kind,
            "text": text,
            "level": level,
            "raw": raw,
        })

    # -- line handling ----------------------------------------------------------------

    def _feed(self, line, is_partial):
        m = LINE_RE.match(line)
        ts = None
        rest = line
        if m:
            ts = _ts_to_epoch(m.group("ts"), self.offset)
            rest = m.group("rest")
            if ts is not None:
                self.last_ts = ts
                if self.first_ts is None:
                    self.first_ts = ts
            if EVENT_LOG_MARKER not in rest:
                self._in_stats = False

        # ---- EVENT_LOG_v1: everything after the marker is one JSON object -----------
        if EVENT_LOG_MARKER in rest:
            self._on_event_line(rest, ts, is_partial)
            return

        if m is None:
            # untimestamped content: the statistics counter block and dump text
            self._on_untimestamped(line)
            return

        # strip the optional decorations, in any order:
        #   '(Original Log Time ...)', '[WARN]', '[db/some_file.cc:123]'
        level = None
        src = None
        body = rest
        for _ in range(4 if body[:1] in ("(", "[") else 0):
            om = ORIG_RE.match(body)
            if om:
                body = om.group("rest")
                continue
            lm = LEVEL_RE.match(body)
            if lm:
                level = lm.group("level")
                body = lm.group("rest")
                continue
            sm = SRC_RE.match(body)
            if sm:
                src = sm.group("src")
                body = sm.group("rest")
                continue
            break

        self._on_message(ts, level, src, body, line)

    def _on_untimestamped(self, line):
        """Untimestamped lines: the statistics counter block, mostly."""
        stripped = line.strip()
        if stripped == "":
            return
        if self._dump is not None:
            hm = HISTOGRAM_RE.match(stripped)
            if hm:
                self._in_stats = True
                name = hm.group("name")
                self._dump["histograms"][name] = {
                    "p50": float(hm.group("p50")),
                    "p95": float(hm.group("p95")),
                    "p99": float(hm.group("p99")),
                    "p100": float(hm.group("p100")),
                    "count": int(hm.group("count")),
                    "sum": int(hm.group("sum")),
                }
                return
            cm = COUNTER_RE.match(stripped)
            if cm:
                self._in_stats = True
                self._dump["tickers"][cm.group("name")] = int(cm.group("count"))
                return
            if self._in_stats and stripped.startswith("rocksdb."):
                # a counter-ish line we do not recognise: keep it out of the ledger but
                # make the omission observable
                self._orphan_counter_lines += 1
            self._on_dump_text(stripped)
            return
        self._on_dump_text(stripped)

    def _on_dump_text(self, stripped):
        """Lines inside a dump block that are not counters (stalls, uptime)."""
        if self._dump is None:
            return
        m = CUM_STALL_RE.match(stripped)
        if m:
            secs = _hms_to_seconds(m.group("hms"))
            self._dump["cumulative_stall_s"] = secs
            entry = {
                "ts": self._dump["ts"],
                "kind": "message",
                "text": stripped,
                "cumulative_stall_s": secs,
                "interval_stall_s": self._dump["interval_stall_s"],
            }
            self.stalls.append(entry)
            return
        m = INT_STALL_RE.match(stripped)
        if m:
            secs = _hms_to_seconds(m.group("hms"))
            self._dump["interval_stall_s"] = secs
            entry = {
                "ts": self._dump["ts"],
                "kind": "message",
                "text": stripped,
                "cumulative_stall_s": self._dump["cumulative_stall_s"],
                "interval_stall_s": secs,
            }
            self.stalls.append(entry)
            # 'Cumulative stall:' precedes 'Interval stall:' in the same block, so the
            # previous message entry could not know the interval yet; patch it here rather
            # than holding a reference to the dump (which would defeat --stats-dump-limit).
            for prev in reversed(self.stalls[:-1]):
                if prev["kind"] != "message":
                    continue
                if prev["interval_stall_s"] is None:
                    prev["interval_stall_s"] = secs
                break
            return
        m = UPTIME_RE.match(stripped)
        if m:
            self._dump["uptime_total_s"] = float(m.group("total"))

    def _on_message(self, ts, level, src, body, full_line):
        # --- statistics dump boundaries
        if DUMPING_STATS_RE.match(body):
            self._start_dump(ts)
            return
        if STATISTICS_RE.match(body):
            if self._dump is not None and ts is not None:
                self._dump["ts"] = ts
            self._in_stats = True
            return
        self._in_stats = False

        # --- severity-driven warnings
        if level in ("WARN", "WARNING", "ERROR", "FATAL"):
            self.warn(ts, "warn_line", body, level=level, raw=full_line)
        elif WARNISH_RE.search(body):
            self.warn(ts, "log_message", body, level=level, raw=full_line)

        # --- column-family decoration: '[default] Msg' -> cf='default', inner='Msg'
        cf = None
        inner = body
        cm = CF_PREFIX_RE.match(body)
        if cm and cm.group("cf") not in ("WARN", "WARNING", "ERROR", "FATAL", "INFO"):
            cf = cm.group("cf")
            inner = cm.group("rest")

        # --- stalls (the message always carries the cf tag first)
        if inner.startswith("Stopping writes") or inner.startswith("Stalling writes"):
            self.stalls.append({
                "ts": ts,
                "kind": "begin",
                "text": inner,
                "cumulative_stall_s": (self._dump or {}).get("cumulative_stall_s"),
                "interval_stall_s": (self._dump or {}).get("interval_stall_s"),
            })
        elif inner.startswith("Resuming writes") or inner.startswith("Resumed writes") \
                or inner.startswith("Continued writes"):
            self.stalls.append({
                "ts": ts,
                "kind": "end",
                "text": inner,
                "cumulative_stall_s": (self._dump or {}).get("cumulative_stall_s"),
                "interval_stall_s": (self._dump or {}).get("interval_stall_s"),
            })

        # --- run identity / recovery / options (these carry no cf decoration)
        if self.rocksdb_version is None:
            mm = VERSION_RE.match(body)
            if mm:
                self.rocksdb_version = mm.group("version")
        if self.git_sha is None:
            mm = GIT_SHA_RE.match(body)
            if mm:
                self.git_sha = mm.group("sha")
        if self.host is None:
            mm = HOST_RE.match(body)
            if mm:
                self.host = mm.group("host")
        if self.db_session_id is None:
            mm = DB_SESSION_RE.match(body)
            if mm:
                self.db_session_id = mm.group("sid")
                self.recovery["db_session_id"] = mm.group("sid")
        if self.db_id is None:
            mm = DB_ID_RE.match(body)
            if mm:
                self.db_id = mm.group("dbid")

        if "manifest" in body:      # cheap gate: these regexes are only worth running then
            mm = RECOVERING_RE.match(body)
            if mm:
                self.recovery["recovered_from_manifest"] = True
                self.recovery["manifest_path"] = mm.group("path")
            mm = RECOVERED_RE.match(body)
            if mm:
                self.recovery["recovered_from_manifest"] = True
                if self.recovery["manifest_path"] is None:
                    self.recovery["manifest_path"] = mm.group("path")
                for km in RECOVERED_KV_RE.finditer(body):
                    key = km.group("key")
                    if key in self.recovery:
                        self.recovery[key] = int(km.group("val"))

        if "Options." in body:
            om = OPTION_RE.search(body)
            if om:
                name = om.group("name")
                if name not in self.options:
                    self.options[name] = _typed(om.group("value"))

        # --- plain per-level state
        for candidate in ((inner, body) if inner != body else (body,)):
            mm = LEVEL_SUMMARY_RE.match(candidate)
            if mm:
                self.lsm_summaries.append({
                    "ts": ts,
                    "files": [int(x) for x in mm.group("files").split()],
                    "max_score": float(mm.group("score")),
                    "pending_compaction_bytes": int(mm.group("pending")),
                    "source": "level_summary",
                    "cf_name": cf,
                })
                return

        # --- plain compaction/flush text lines (cross-checks only, never primary)
        self._on_text_line(ts, body, inner)

    def _on_text_line(self, ts, body, inner):
        candidates = [body]
        if inner != body and TXT_JOB_RE.match(inner):
            candidates.append(inner)
        for candidate in candidates:
            mm = TXT_COMPACTING_RE.match(candidate)
            if mm:
                self.text_compacting[int(mm.group("job"))] = {
                    "summary": mm.group("summary"),
                    "score": float(mm.group("score")),
                    "output_level": _output_level_from_summary(mm.group("summary")),
                    "ts": ts,
                }
                return
            mm = TXT_COMPACTED_RE.match(candidate)
            if mm:
                self.text_compacted[int(mm.group("job"))] = {
                    "summary": mm.group("summary"),
                    "bytes": int(mm.group("bytes")),
                    "output_level": _output_level_from_summary(mm.group("summary")),
                    "ts": ts,
                }
                return
            mm = TXT_FLUSH_LONG_RE.match(candidate)
            if mm:
                self.text_flush_long[int(mm.group("job"))] = {
                    "wall": int(mm.group("wall")),
                    "cpu": int(mm.group("cpu")),
                    "ts": ts,
                }
                return
            mm = TXT_GEN_TABLE_RE.match(candidate)
            if mm:
                self.text_generated.setdefault(int(mm.group("job")), []).append(
                    (int(mm.group("file")), int(mm.group("keys")), int(mm.group("bytes"))))
                return
            mm = TXT_DELETED_RE.match(candidate)
            if mm:
                self.text_deleted_immediate.append((ts, mm.group("path")))
                return


    # -- statistics dumps -------------------------------------------------------------

    def _start_dump(self, ts):
        dump = {
            "ts": ts,
            "dump_start_ts": ts,
            "tickers": {},
            "histograms": {},
            "cumulative_stall_s": None,
            "interval_stall_s": None,
            "uptime_total_s": None,
        }
        self._dumps_seen += 1
        limit = self.stats_dump_limit
        if limit <= 0 or self._dumps_seen <= 1:
            self.stats_dumps.append(dump)
        elif limit == 1:
            self.stats_dumps = [dump]
        else:
            # first dump + the most recent (limit - 1) dumps
            if len(self.stats_dumps) >= limit:
                del self.stats_dumps[1]
            self.stats_dumps.append(dump)
        self._dump = dump

    # -- events -----------------------------------------------------------------------

    def _on_event_line(self, rest, line_ts, is_partial):
        idx = rest.find("EVENT_LOG_v1 ") + len("EVENT_LOG_v1 ")
        payload = rest[idx:].strip()
        try:
            obj, _end = json.JSONDecoder().raw_decode(payload)
        except ValueError as exc:
            kind = "truncated_event_line" if is_partial else "event_json_error"
            self.warn(line_ts, kind,
                      "could not parse EVENT_LOG_v1 payload: %s" % (exc,),
                      raw=rest[:500])
            return
        if not isinstance(obj, dict):
            self.warn(line_ts, "event_not_object",
                      "EVENT_LOG_v1 payload is not a JSON object", raw=rest[:500])
            return
        try:
            self._dispatch_event(obj, line_ts)
        except Exception as exc:
            self.warn(line_ts, "event_dispatch_error",
                      "%s: %s" % (type(exc).__name__, exc), raw=rest[:500])

    def _event_ts(self, obj, line_ts):
        micros = obj.get("time_micros")
        if isinstance(micros, (int, float)) and micros > 0:
            return micros / 1e6
        return line_ts

    def _dispatch_event(self, obj, line_ts):
        event = obj.get("event")
        ts = self._event_ts(obj, line_ts)
        job_id = obj.get("job")
        if isinstance(job_id, float):
            job_id = int(job_id)
        self.counter_events[event] = self.counter_events.get(event, 0) + 1

        if event == "flush_started":
            rec = self._flush(job_id, ts)
            rec["start_ts"] = ts
            rec["reason"] = obj.get("flush_reason")
            rec["num_memtables"] = obj.get("num_memtables")
            rec["num_input_entries"] = obj.get("total_num_input_entries")
            rec["num_deletes"] = obj.get("num_deletes")
            rec["input_data_size"] = obj.get("total_data_size")
            rec["memory_usage"] = obj.get("memory_usage")
            rec["num_range_deletes"] = obj.get("num_range_deletes")
            rec["cf_name"] = obj.get("cf_name", rec["cf_name"])
            return

        if event == "flush_finished":
            rec = self._flush(job_id, ts)
            rec["end_ts"] = ts
            rec["lsm_state_after"] = obj.get("lsm_state")
            rec["immutable_memtables"] = obj.get("immutable_memtables")
            rec["cf_name"] = obj.get("cf_name", rec["cf_name"])
            return

        if event == "table_file_creation":
            cf = obj.get("cf_name")
            props = obj.get("table_properties") or {}
            rec = {
                "ts": ts,
                "job": job_id,
                "file_number": obj.get("file_number"),
                "file_size": obj.get("file_size"),
                "smallest_seqno": obj.get("smallest_seqno"),
                "largest_seqno": obj.get("largest_seqno"),
                "data_size": props.get("data_size"),
                "index_size": props.get("index_size"),
                "filter_size": props.get("filter_size"),
                "num_data_blocks": props.get("num_data_blocks"),
                "num_entries": props.get("num_entries"),
                "raw_key_size": props.get("raw_key_size"),
                "raw_value_size": props.get("raw_value_size"),
                "num_deletions": props.get("num_deletions"),
                "num_range_deletions": props.get("num_range_deletions"),
                "cf_name": cf,
                "status": obj.get("status"),
            }
            self.files_created.append(rec)
            if rec["file_number"] is not None and rec["file_size"] is not None:
                self._file_sizes[rec["file_number"]] = (rec["file_size"], job_id)
            jrec = self.jobs.get(job_id)
            if jrec is not None and jrec["_kind"] == "compaction":
                jrec["_created_bytes"] += rec["file_size"] or 0
                if rec["file_number"] is not None:
                    jrec["output_file_numbers"].append(rec["file_number"])
            fjob = self._flushes_by_job.get(job_id)
            if fjob is not None:
                if rec["file_number"] is not None:
                    fjob["output_file_numbers"].append(rec["file_number"])
                fjob["output_bytes"] += rec["file_size"] or 0
            return

        if event == "table_file_deletion":
            self.files_deleted.append({
                "ts": ts,
                "file_number": obj.get("file_number"),
                "file_size": None,
                "type": "table_file",
                "job": job_id,
                "status": obj.get("status"),
            })
            return

        if event == "compaction_started":
            rec = self._job(job_id, ts)
            rec["start_ts"] = ts
            rec["reason"] = obj.get("compaction_reason")
            rec["score"] = obj.get("score")
            rec["input_data_size"] = obj.get("input_data_size")
            rec["oldest_snapshot_seqno"] = obj.get("oldest_snapshot_seqno")
            rec["cf_name"] = obj.get("cf_name", rec["cf_name"])
            for key, value in obj.items():
                if key.startswith("files_L") and isinstance(value, list):
                    try:
                        level = int(key[len("files_L"):])
                    except ValueError:
                        continue
                    rec["input_files_per_level"][level] = list(value)
            return

        if event == "compaction_finished":
            rec = self._job(job_id, ts)
            rec["end_ts"] = ts
            rec["finished"] = True
            rec["output_level"] = obj.get("output_level")
            rec["num_output_files"] = obj.get("num_output_files")
            rec["output_bytes"] = obj.get("total_output_size")
            rec["num_input_records"] = obj.get("num_input_records")
            rec["num_output_records"] = obj.get("num_output_records")
            rec["num_subcompactions"] = obj.get("num_subcompactions")
            rec["lsm_state_after"] = obj.get("lsm_state")
            rec["wall_micros"] = obj.get("compaction_time_micros")
            rec["cpu_micros"] = obj.get("compaction_time_cpu_micros")
            rec["cf_name"] = obj.get("cf_name", rec["cf_name"])
            if rec["lsm_state_after"] is not None:
                self.lsm_summaries.append({
                    "ts": ts,
                    "files": rec["lsm_state_after"],
                    "max_score": None,
                    "pending_compaction_bytes": None,
                    "source": "compaction_finished",
                    "cf_name": rec["cf_name"],
                })
            return

        if event == "trivial_move":
            self.trivial_moves.append({
                "ts": ts,
                "job": job_id,
                "destination_level": obj.get("destination_level"),
                "num_files": obj.get("files"),
                "total_files_size": obj.get("total_files_size"),
            })
            return

        # WAL recovery on reopen emits a recovery_started/recovery_finished pair; it has no
        # home in the required schema, so it is folded into the existing 'recovery' object.
        if event == "recovery_started":
            self.recovery["recovery_started_ts"] = ts
            wals = obj.get("wal_files")
            if isinstance(wals, list):
                self.recovery["wal_files_on_recovery_start"] = list(wals)
            return
        if event == "recovery_finished":
            self.recovery["recovery_finished_ts"] = ts
            self.recovery["recovery_status"] = obj.get("status")
            self.recovery["recovered_from_wal"] = True
            return

        self.unknown_events.append({
            "ts": ts,
            "event": event,
            "job": job_id,
            "raw": json.dumps(obj, separators=(",", ":"))[:500],
        })

    # -- record creation --------------------------------------------------------------

    def _job(self, job_id, ts):
        rec = self.jobs.get(job_id)
        if rec is None:
            rec = {
                "job": job_id,
                "_kind": "compaction",
                "start_ts": None,
                "end_ts": None,
                "wall_micros": None,
                "cpu_micros": None,
                "reason": None,
                "score": None,
                "input_files_per_level": {},
                "input_data_size": None,
                "output_level": None,
                "num_output_files": None,
                "output_bytes": None,
                "output_file_numbers": [],
                "num_input_records": None,
                "num_output_records": None,
                "num_subcompactions": None,
                "lsm_state_after": None,
                "cf_name": None,
                "oldest_snapshot_seqno": None,
                "finished": False,
                "_created_bytes": 0,
                "_output_level_from_text": None,
            }
            self.jobs[job_id] = rec
        elif rec["_kind"] != "compaction":
            self.warn(ts, "job_id_reuse",
                      "job id %s used by both a flush and a compaction" % (job_id,))
        return rec

    def _flush(self, job_id, ts):
        rec = self._flushes_by_job.get(job_id)
        if rec is None:
            rec = {
                "job": job_id,
                "start_ts": None,
                "end_ts": None,
                "wall_micros": None,
                "cpu_micros": None,
                "reason": None,
                "num_memtables": None,
                "num_input_entries": None,
                "num_deletes": None,
                "input_data_size": None,
                "memory_usage": None,
                "num_range_deletes": None,
                "output_file_numbers": [],
                "output_bytes": 0,
                "lsm_state_after": None,
                "immutable_memtables": None,
                "cf_name": None,
            }
            self._flushes_by_job[job_id] = rec
        return rec

    # -- ordering / flush post-processing ---------------------------------------------

    def _finalize_ordering(self):
        def sort_key(rec):
            return (rec["start_ts"] if rec["start_ts"] is not None
                    else (rec["end_ts"] if rec["end_ts"] is not None else 0.0))

        for rec in self._flushes_by_job.values():
            txt = self.text_flush_long.get(rec["job"])
            if txt is not None:
                rec["wall_micros"] = txt["wall"]
                rec["cpu_micros"] = txt["cpu"]
            elif rec["start_ts"] is not None and rec["end_ts"] is not None:
                rec["wall_micros"] = int(round((rec["end_ts"] - rec["start_ts"]) * 1e6))
            if rec["lsm_state_after"] is not None:
                self.lsm_summaries.append({
                    "ts": rec["end_ts"],
                    "files": rec["lsm_state_after"],
                    "max_score": None,
                    "pending_compaction_bytes": None,
                    "source": "flush_finished",
                    "cf_name": rec["cf_name"],
                })
        self.lsm_summaries.sort(key=lambda r: (r["ts"] if r["ts"] is not None else 0.0,
                                               {"level_summary": 0, "flush_finished": 1,
                                                "compaction_finished": 2}[r["source"]]))

        for rec in self.jobs.values():
            if rec["output_bytes"] is None:
                rec["output_bytes"] = rec["_created_bytes"]
        for rec in self._flushes_by_job.values():
            if rec["output_bytes"] is None:
                rec["output_bytes"] = 0

        self.files_created.sort(key=lambda r: (r["ts"] if r["ts"] is not None else 0.0,
                                               -1 if r["file_number"] is None else r["file_number"]))
        self.files_deleted.sort(key=lambda r: r["ts"] if r["ts"] is not None else 0.0)
        self.stalls.sort(key=lambda r: r["ts"] if r["ts"] is not None else 0.0)
        self.unknown_events.sort(key=lambda r: r["ts"] if r["ts"] is not None else 0.0)
        self._sorted_jobs = sorted(self.jobs.values(), key=sort_key)
        self._sorted_flushes = sorted(self._flushes_by_job.values(), key=sort_key)


EVENT_LOG_MARKER = "EVENT_LOG_v1 "


def _output_level_from_summary(summary):
    """``'4@0 files to L1'`` -> ``1``."""
    m = re.search(r"\bto L(\d+)\b", summary or "")
    if m:
        return int(m.group(1))
    return None


def _detect_utc_offset(path, max_bytes=8 << 20, max_samples=400):
    """Median ``local_parse(wall_clock) - time_micros`` over the log's event lines.

    Returns 0.0 when the log has no event lines (or cannot be read): the wall clock is
    then taken at face value in the local timezone.
    """
    deltas = []
    consumed = 0
    try:
        with open(path, "rb", buffering=1 << 20) as handle:
            for raw in handle:
                consumed += len(raw)
                if consumed > max_bytes or len(deltas) >= max_samples:
                    break
                if EVENT_LOG_MARKER.encode("ascii") not in raw:
                    continue
                text = raw.decode("utf-8", "replace")
                m = LINE_RE.match(text)
                if not m:
                    continue
                local = _ts_to_epoch(m.group("ts"), 0.0)
                if local is None:
                    continue
                rest = m.group("rest")
                idx = rest.find(EVENT_LOG_MARKER)
                payload = rest[idx + len(EVENT_LOG_MARKER):].strip()
                try:
                    obj, _ = json.JSONDecoder().raw_decode(payload)
                except ValueError:
                    continue
                micros = obj.get("time_micros") if isinstance(obj, dict) else None
                if not isinstance(micros, (int, float)) or micros <= 0:
                    continue
                deltas.append(local - micros / 1e6)
    except OSError:
        return 0.0
    if not deltas:
        return 0.0
    deltas.sort()
    median = deltas[len(deltas) // 2]
    # guard against a handful of outliers (clock steps mid-run): keep the mode-ish mass
    close = [d for d in deltas if abs(d - median) <= 1.0]
    if close:
        median = close[len(close) // 2]
    return round(median, 3)


def parse_log(path, stats_dump_limit=0):
    """Parse ``path`` and return the result object.

    Validation needs the parser itself (it holds the ``EVENT_LOG_v1`` payloads' data
    model); use :meth:`LogParser.parse_file` directly when you want both.
    """
    parser = LogParser(stats_dump_limit=stats_dump_limit)
    parser.parse_file(path)
    return parser.result()


# --------------------------------------------------------------------------------------
# CSV output
# --------------------------------------------------------------------------------------

def _write_records_csv(path, records, extra_leading=None):
    columns = list(extra_leading or [])
    for rec in records:
        for key in rec:
            if key not in columns:
                columns.append(key)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
        writer.writerow(columns)
        for rec in records:
            writer.writerow([_jsonable(rec.get(col)) for col in columns])
    return columns


def write_csv_dir(result, csv_dir):
    """Write the eight required CSVs (RFC4180 quoting, CRLF line endings)."""
    os.makedirs(csv_dir, exist_ok=True)
    written = []

    for name, records in (
        ("jobs.csv", result["jobs"]),
        ("flushes.csv", result["flushes"]),
        ("files_created.csv", result["files_created"]),
        ("files_deleted.csv", result["files_deleted"]),
        ("lsm_summaries.csv", result["lsm_summaries"]),
        ("stalls.csv", result["stalls"]),
        ("warnings.csv", result["warnings"]),
    ):
        target = os.path.join(csv_dir, name)
        _write_records_csv(target, records)
        written.append(target)

    # stats_dumps: one row per dump, one column per counter plus per histogram stat
    dumps = result["stats_dumps"]
    ticker_names = []
    histo_names = []
    for dump in dumps:
        for key in dump["tickers"]:
            if key not in ticker_names:
                ticker_names.append(key)
        for key in dump["histograms"]:
            if key not in histo_names:
                histo_names.append(key)
    ticker_names.sort()
    histo_names.sort()
    fixed = ["ts", "dump_start_ts", "cumulative_stall_s", "interval_stall_s", "uptime_total_s"]
    columns = fixed + ticker_names + [n + " [COUNT]" for n in histo_names] \
        + [n + " [SUM]" for n in histo_names]
    target = os.path.join(csv_dir, "stats_dumps.csv")
    with open(target, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
        writer.writerow(columns)
        for dump in dumps:
            row = [dump.get("ts"), dump.get("dump_start_ts"),
                   dump.get("cumulative_stall_s"), dump.get("interval_stall_s"),
                   dump.get("uptime_total_s")]
            row.extend(dump["tickers"].get(name) for name in ticker_names)
            row.extend(dump["histograms"].get(name, {}).get("count") for name in histo_names)
            row.extend(dump["histograms"].get(name, {}).get("sum") for name in histo_names)
            writer.writerow([_jsonable(cell) for cell in row])
    written.append(target)
    return written


# --------------------------------------------------------------------------------------
# validation
# --------------------------------------------------------------------------------------

class Check(object):
    """One validation check."""

    def __init__(self, cid, name, kind, expected, parsed, status, tolerance,
                 detail="", unit=""):
        self.id = cid
        self.name = name
        self.kind = kind
        self.expected = expected
        self.parsed = parsed
        self.status = status
        self.tolerance = tolerance
        self.detail = detail
        self.unit = unit

    @property
    def abs_diff(self):
        if isinstance(self.expected, (int, float)) and isinstance(self.parsed, (int, float)):
            return self.expected - self.parsed
        return None

    @property
    def rel_diff(self):
        diff = self.abs_diff
        if diff is None or not isinstance(self.expected, (int, float)) or self.expected == 0:
            return None
        return diff / float(abs(self.expected))


def _count_before(records, key, limit):
    return sum(1 for r in records if r[key] is not None and r[key] <= limit)


def _sum_before(records, value_key, ts_key, limit):
    """Sum ``value_key`` over the records whose ``ts_key`` is at or before ``limit``."""
    return sum(r[value_key] or 0 for r in records
               if r[ts_key] is not None and r[ts_key] <= limit)


def validate(parser):
    """Run the counter cross-checks; returns ``(checks, snapshot, exit_code)``.

    Every comparison is scoped to the snapshot instant ``cut`` (the timestamp of the
    ``STATISTICS:`` line, which is where the counter values were produced).  Two cut-offs
    are used for every check:

    ``cut``
        an event at or before it *may* already be in the counter (inclusive bound);
    ``cut - COUNTER_LAG_S``
        an event at or before it *must* be in the counter (conservative bound).

    The lag is real and measurable: the counter section is formatted -- each of the ~500
    counters is read individually -- *before* the ``STATISTICS:`` line carrying ``cut`` is
    written, so the values trail that timestamp by the time needed to build and write the
    dump.  Measured on a loaded machine: a deficit of up to 13 ``Deleted file ...
    immediately`` lines out of ~930 (the deletion rate is all the logs give us), i.e. of
    the order of 100 ms.  Verdicts that must not be exceeded use the conservative bound,
    verdicts the counter must not exceed use the inclusive one; the printed table always
    shows the inclusive numbers, and the detail line states which set decided the verdict.
    """
    result = parser.result()
    checks = []
    dumps = result["stats_dumps"]
    if not dumps:
        return checks, None, EXIT_NOT_VALIDATABLE
    snapshot = dumps[-1]
    cut = snapshot["ts"]
    if cut is None:
        return checks, snapshot, EXIT_NOT_VALIDATABLE
    lag_cut = cut - COUNTER_LAG_S

    flushes = result["flushes"]
    jobs = result["jobs"]
    basis = "verdict basis: events with ts <= snapshot - %g s" % COUNTER_LAG_S

    # ---- 1. flush.write.bytes vs sum(flush output_bytes) ---------------------------
    expected = _ticker(snapshot, "rocksdb.flush.write.bytes")
    lo = _sum_before(flushes, "output_bytes", "end_ts", lag_cut)
    hi = _sum_before(flushes, "output_bytes", "end_ts", cut)
    if expected is None:
        checks.append(Check("1", "flush.write.bytes vs sum(flush output_bytes)", "near-exact",
                            None, hi, "SKIP", None, "no rocksdb.flush.write.bytes ticker"))
    else:
        slack = max(BYTE_SLACK_ABS, BYTE_SLACK_REL * expected)
        ok = (lo <= expected + 1) and (expected <= hi + slack)
        detail = ("counter - inclusive sum = %+d B (rel %+.2e); the excess is expected because "
                  "FLUSH_WRITE_BYTES also counts the MANIFEST version edit written while each "
                  "flush installs itself. %s (conservative sum %d B, inclusive sum %d B)."
                  % (expected - hi, (expected - hi) / float(expected) if expected else 0.0,
                     basis, lo, hi))
        checks.append(Check("1", "flush.write.bytes vs sum(flush output_bytes)", "near-exact",
                            expected, hi, "PASS" if ok else "FAIL",
                            "0 <= diff <= max(%d B, %g x expected)" % (BYTE_SLACK_ABS,
                                                                       BYTE_SLACK_REL),
                            detail, "bytes"))

    # ---- 2. compact.write.bytes vs sum(job output_bytes) ---------------------------
    expected = _ticker(snapshot, "rocksdb.compact.write.bytes")
    lo = _sum_before(jobs, "output_bytes", "end_ts", lag_cut)
    hi = _sum_before(jobs, "output_bytes", "end_ts", cut)
    all_jobs = sum(j["output_bytes"] or 0 for j in jobs)
    inflight = [j["job"] for j in jobs
                if j["start_ts"] is not None and j["start_ts"] <= cut
                and (j["end_ts"] is None or j["end_ts"] > cut)]
    if expected is None:
        checks.append(Check("2a", "compact.write.bytes >= sum(finished job output)", "bounded",
                            None, hi, "SKIP", None, "no rocksdb.compact.write.bytes ticker"))
    else:
        ok = lo <= expected + 1
        detail = ("completed jobs account for %d B (conservative %d B); the residual %+d B is "
                  "partial output of compaction job(s) %s that were still running when the "
                  "counter was read. %s."
                  % (hi, lo, expected - hi,
                     ",".join(str(x) for x in inflight) if inflight else "none", basis))
        checks.append(Check("2a", "compact.write.bytes >= sum(finished job output)", "bounded",
                            expected, hi, "PASS" if ok else "FAIL",
                            "conservative parsed <= counter", detail, "bytes"))
        checks.append(Check("2b", "compact.write.bytes vs whole-log job output (info)", "info",
                            expected, all_jobs, "INFO", "-",
                            "the last dump precedes the end of the run, so jobs that finished "
                            "after the snapshot are NOT in this counter (expected: whole-log "
                            "sum >= counter; counter includes partial in-flight writes)",
                            "bytes"))

    # ---- 3. compact.read.bytes vs sum(job input_data_size) -------------------------
    expected = _ticker(snapshot, "rocksdb.compact.read.bytes")
    lo = _sum_before(jobs, "input_data_size", "end_ts", lag_cut)
    hi = _sum_before(jobs, "input_data_size", "end_ts", cut)
    all_in = sum(j["input_data_size"] or 0 for j in jobs)
    if expected is None:
        checks.append(Check("3a", "compact.read.bytes vs sum(finished job input)", "approx",
                            None, hi, "SKIP", None, "no rocksdb.compact.read.bytes ticker"))
    else:
        slack = max(READ_SLACK_ABS, READ_SLACK_REL * expected)
        diff = expected - hi
        ok = (lo <= expected) or (abs(expected - lo) <= slack)
        detail = (
            "input_data_size is the sum of input FILE sizes; the counter counts bytes "
            "actually read (data+index+filter+footer, block-cache hits excluded). Direction: "
            "the counter is the LARGER side while a compaction is in flight at the snapshot "
            "(residual %+d B against the inclusive sum, partial progress of %s), and for "
            "finished jobs a small deficit is normal too (skipped data blocks, early "
            "termination), hence the two-sided band. %s (conservative sum %d B)."
            % (diff, ",".join("job %s" % j for j in inflight) if inflight else "no job",
               basis, lo))
        checks.append(Check("3a", "compact.read.bytes vs sum(finished job input)", "approx",
                            expected, hi, "PASS" if ok else "FAIL",
                            "conservative parsed <= counter, or |diff| <= max(%d B, %g x counter)"
                            % (READ_SLACK_ABS, READ_SLACK_REL), detail, "bytes"))
        checks.append(Check("3b", "compact.read.bytes vs whole-log job input (info)", "info",
                            expected, all_in, "INFO", "-",
                            "same snapshot truncation as 2b; expected direction: whole-log sum "
                            ">= counter", "bytes"))

    # ---- 4. event counts and durations vs histograms -------------------------------
    checks.extend(_count_checks(result, snapshot, cut, lag_cut))

    # ---- 5. per-job file count / size identities -----------------------------------
    checks.extend(_file_identity_checks(result))

    # ---- 6. plain-text cross-check -------------------------------------------------
    checks.extend(_text_cross_checks(parser, result))

    # ---- 7. delete_scheduler text lines vs files.deleted.immediately ---------------
    ticker = _ticker(snapshot, "rocksdb.files.deleted.immediately")
    lo = sum(1 for ts, _p in parser.text_deleted_immediate if ts is not None and ts <= lag_cut)
    hi = sum(1 for ts, _p in parser.text_deleted_immediate if ts is not None and ts <= cut)
    if ticker is None:
        checks.append(Check("7", "files.deleted.immediately vs delete_scheduler lines",
                            "near-exact", None, hi, "SKIP", None,
                            "no rocksdb.files.deleted.immediately ticker"))
    else:
        # +1 for the asymmetric timestamping: a deletion bumps the ticker and only then logs
        # its line, and that line's timestamp is taken after the info-log mutex is acquired,
        # so in principle one line can carry a timestamp past the snapshot while its bump was
        # already counted.
        ok = lo <= ticker <= hi + 1
        checks.append(Check("7", "files.deleted.immediately vs delete_scheduler lines",
                            "near-exact", ticker, hi, "PASS" if ok else "FAIL",
                            "[%d, %d] (+1 timestamp-lag line)" % (lo, hi),
                            "each 'Deleted file ... immediately' line bumps this ticker, for "
                            "WAL and SST files alike (both log formats). The row is +1-tolerant "
                            "because the bump precedes a line whose own timestamp is taken "
                            "after the info-log mutex. %s; %d table_file_deletion events in the "
                            "whole log." % (basis, len(result["files_deleted"])), "files"))

    failed = [c for c in checks if c.status == "FAIL"]
    return checks, snapshot, (EXIT_CHECK_FAILED if failed else EXIT_OK)


def _count_checks(result, snapshot, cut, lag_cut):
    checks = []
    flushes = result["flushes"]
    jobs = result["jobs"]
    lag_note = "the counter block is formatted before the STATISTICS line is written"

    # 4a flush count vs db.flush.micros COUNT
    expected = _histo_count(snapshot, "rocksdb.db.flush.micros")
    lo = _count_before(flushes, "end_ts", lag_cut)
    hi = _count_before(flushes, "end_ts", cut)
    if expected is None:
        checks.append(Check("4a", "count(flush_finished) vs db.flush.micros COUNT", "exact",
                            None, hi, "SKIP", None, "no rocksdb.db.flush.micros histogram"))
    else:
        ok = lo <= expected <= hi
        checks.append(Check("4a", "count(flush_finished) vs db.flush.micros COUNT", "exact",
                            expected, hi, "PASS" if ok else "FAIL",
                            "[%d, %d] (snapshot - %g s .. snapshot)" % (lo, hi, COUNTER_LAG_S),
                            "flush_started before snapshot: %d; %s"
                            % (_count_before(flushes, "start_ts", cut), lag_note), "flushes"))

    # 4b informational: how many flushes straddle the snapshot
    started = _count_before(flushes, "start_ts", cut)
    finished = _count_before(flushes, "end_ts", cut)
    in_flight = [f["job"] for f in flushes
                 if f["start_ts"] is not None and f["start_ts"] <= cut
                 and (f["end_ts"] is None or f["end_ts"] > cut)]
    checks.append(Check("4b", "count(flush_started) - count(flush_finished) at snapshot", "info",
                        started, finished, "INFO", "-",
                        "%d flush(es) were in flight when the snapshot was taken%s; they are "
                        "correctly absent from db.flush.micros COUNT and from the flush byte "
                        "sum" % (len(in_flight),
                                 (" (job %s)" % ",".join(str(x) for x in in_flight))
                                 if in_flight else ""), "events"))

    # 4c compaction count vs compaction.times.micros COUNT
    expected = _histo_count(snapshot, "rocksdb.compaction.times.micros")
    lo = _count_before(jobs, "end_ts", lag_cut)
    hi = _count_before(jobs, "end_ts", cut)
    if expected is None:
        checks.append(Check("4c", "count(compaction_finished) vs compaction.times.micros COUNT",
                            "exact", None, hi, "SKIP", None,
                            "no rocksdb.compaction.times.micros histogram"))
    else:
        ok = lo <= expected <= hi
        checks.append(Check("4c", "count(compaction_finished) vs compaction.times.micros COUNT",
                            "exact", expected, hi, "PASS" if ok else "FAIL",
                            "[%d, %d] (snapshot - %g s .. snapshot)" % (lo, hi, COUNTER_LAG_S),
                            "trivial moves never reach this histogram", "compactions"))

    # 4d flush wall time sums
    expected = _histo_sum(snapshot, "rocksdb.db.flush.micros")
    lo_sum = _sum_before(flushes, "wall_micros", "end_ts", lag_cut)
    hi_sum = _sum_before(flushes, "wall_micros", "end_ts", cut)
    if expected is None:
        checks.append(Check("4d", "db.flush.micros SUM vs sum(flush wall_micros)", "exact",
                            None, hi_sum, "SKIP", None, "no db.flush.micros histogram"))
    else:
        ok = lo_sum <= expected <= hi_sum
        checks.append(Check("4d", "db.flush.micros SUM vs sum(flush wall_micros)", "exact",
                            expected, hi_sum, "PASS" if ok else "FAIL",
                            "[%d, %d] us" % (lo_sum, hi_sum),
                            "wall_micros comes from the '[JOB n] Flush lasted N microseconds' "
                            "plain lines", "micros"))

    # 4e/4f compaction wall and cpu sums
    expected_wall = _histo_sum(snapshot, "rocksdb.compaction.times.micros")
    expected_cpu = _histo_sum(snapshot, "rocksdb.compaction.times.cpu_micros")
    lo_wall = _sum_before(jobs, "wall_micros", "end_ts", lag_cut)
    hi_wall = _sum_before(jobs, "wall_micros", "end_ts", cut)
    lo_cpu = _sum_before(jobs, "cpu_micros", "end_ts", lag_cut)
    hi_cpu = _sum_before(jobs, "cpu_micros", "end_ts", cut)
    if expected_wall is None:
        checks.append(Check("4e", "compaction.times.micros SUM vs sum(compaction_time_micros)",
                            "exact", None, hi_wall, "SKIP", None,
                            "no rocksdb.compaction.times.micros histogram"))
    else:
        ok = lo_wall <= expected_wall <= hi_wall
        checks.append(Check("4e", "compaction.times.micros SUM vs sum(compaction_time_micros)",
                            "exact", expected_wall, hi_wall, "PASS" if ok else "FAIL",
                            "[%d, %d] us" % (lo_wall, hi_wall),
                            "compaction_time_micros from the compaction_finished events", "micros"))
    if expected_cpu is not None:
        ok = lo_cpu <= expected_cpu <= hi_cpu
        checks.append(Check("4f", "compaction.times.cpu_micros SUM vs "
                                  "sum(compaction_time_cpu_micros)",
                            "exact", expected_cpu, hi_cpu, "PASS" if ok else "FAIL",
                            "[%d, %d] us" % (lo_cpu, hi_cpu),
                            "compaction_time_cpu_micros from the compaction_finished events",
                            "micros"))
    return checks


def _uninstalled_output_files(created, deleted_numbers, reported_bytes):
    """Output files of a job that were never successfully installed *and* are gone.

    Observed shape (verified in real logs, twice with different symptoms): the database is
    shut down while a compaction is running.  The job still logs ``compaction_finished``
    with ``num_output_files`` and a ``total_output_size`` taken from the in-memory
    statistics, but the last output file was abandoned:

    * its ``table_file_creation`` event carries a non-OK ``status`` -- literally
      ``"Shutdown in progress: Database shutdown"`` -- and ``file_size`` 0, or
    * it was produced empty and removed as an empty output (``RemoveLastEmptyOutput``).

    Either way the file number shows up in ``table_file_deletion`` shortly afterwards, and
    the bytes already written to it are still counted in ``total_output_size``, so the sum
    of the reported ``file_size`` values is legitimately smaller for that one job.  This is
    a log shape, not a parser error, so those jobs are reported through check 5c and a
    warning instead of failing the identity checks 5a/5b.
    """
    bad = []
    if not created or not reported_bytes:
        return bad
    for frec in created:
        if frec.get("file_number") not in deleted_numbers:
            continue
        status = frec.get("status")
        if (status is not None and status != "OK") or (frec.get("file_size") or 0) == 0:
            bad.append(frec)
    return bad


def _aborted_output_jobs(result, files_by_job):
    """Jobs with at least one uninstalled output file (see the helper above)."""
    deleted = set(d["file_number"] for d in result["files_deleted"])
    out = []
    for job in result["jobs"]:
        if not job["finished"]:
            continue
        created = files_by_job.get(job["job"]) or []
        bad = _uninstalled_output_files(created, deleted, job["output_bytes"])
        if bad:
            out.append((job["job"], job["output_bytes"], len(created),
                        [f["status"] for f in bad]))
    return out


def _file_identity_checks(result):
    """Per-job: #table_file_creation == num_output_files and sum(file_size) == total_output_size."""
    count_bad = []
    size_bad = []
    files_by_job = {}
    for frec in result["files_created"]:
        files_by_job.setdefault(frec["job"], []).append(frec)
    explained = _aborted_output_jobs(result, files_by_job)
    explained_jobs = set(item[0] for item in explained)
    checked = 0
    for job in result["jobs"]:
        if not job["finished"] or job["end_ts"] is None:
            continue
        created = files_by_job.get(job["job"]) or []
        checked += 1
        if job["num_output_files"] is not None and len(created) != job["num_output_files"] \
                and job["job"] not in explained_jobs:
            count_bad.append((job["job"], job["num_output_files"], len(created)))
        created_bytes = sum(f["file_size"] or 0 for f in created)
        if job["output_bytes"] is not None and created_bytes != job["output_bytes"] \
                and job["job"] not in explained_jobs:
            size_bad.append((job["job"], job["output_bytes"], created_bytes))
    notes = ""
    if explained:
        notes = ("%d job(s) excluded as aborted-at-shutdown (an output file was never "
                 "installed -- non-OK status or file_size 0 -- and was deleted "
                 "immediately): %s"
                 % (len(explained),
                    "; ".join("job %d reported %d B over %d file(s), uninstalled status %s"
                              % item for item in explained)))
    checks = [Check("5a", "per-job #table_file_creation == num_output_files", "exact",
                    0, len(count_bad), "PASS" if not count_bad else "FAIL", "0",
                    ("all %d finished jobs agree" % checked) if not count_bad else
                    "; ".join("job %d: reported %d, saw %d" % item for item in count_bad),
                    "jobs")]
    checks.append(Check("5b", "per-job sum(file_size) == total_output_size", "exact",
                        0, len(size_bad), "PASS" if not size_bad else "FAIL", "0",
                        ("all %d finished jobs agree" % checked) if not size_bad else
                        "; ".join("job %d: reported %d B, files total %d B" % item
                                  for item in size_bad),
                        "jobs"))
    checks.append(Check("5c", "aborted-at-shutdown jobs (output file uninstalled)", "info",
                        len(explained), 0, "INFO", "-",
                        notes or "none", "jobs"))
    return checks


def _text_cross_checks(parser, result):
    """Plain-text compaction lines vs the JSON events.

    A *contradiction* (both present, different content) is a failure; a missing text line
    is informational, because the tail of a killed log can drop one.  The score is printed
    with two decimals, so it is compared with a 0.005 tolerance.
    """
    summary_bad = []
    score_bad = []
    bytes_bad = []
    missing_compacted = 0
    compared = 0
    for job in result["jobs"]:
        job_id = job["job"]
        txt = parser.text_compacting.get(job_id)
        if txt is not None:
            compared += 1
            want = _input_summary({int(k): v for k, v in
                                   job["num_input_files_per_level"].items()})
            got = re.sub(r"\s*files to L\d+$", "", txt["summary"])
            if want != got:
                summary_bad.append((job_id, want, got))
            if job["output_level"] is not None and txt.get("output_level") is not None \
                    and job["output_level"] != txt["output_level"]:
                summary_bad.append((job_id, "output L%d" % job["output_level"],
                                    "output L%d" % txt["output_level"]))
            if job["score"] is not None and abs(job["score"] - txt["score"]) > SCORE_TOL:
                score_bad.append((job_id, job["score"], txt["score"]))
        compacted = parser.text_compacted.get(job_id)
        if compacted is None:
            if txt is not None:
                missing_compacted += 1
        elif job["output_bytes"] is not None and compacted["bytes"] != job["output_bytes"]:
            bytes_bad.append((job_id, job["output_bytes"], compacted["bytes"]))
    checks = [
        Check("6a", "'Compacting ...' text vs compaction_started fields", "exact",
              0, len(summary_bad), "PASS" if not summary_bad else "FAIL", "0",
              ("%d jobs cross-checked" % compared) if not summary_bad else
              "; ".join("job %d: json %s vs text %s" % item for item in summary_bad),
              "jobs"),
        Check("6b", "'Compacting ... score' text vs JSON score", "exact",
              0, len(score_bad), "PASS" if not score_bad else "FAIL",
              "|diff| <= %g (the text prints score with 2 decimals)" % SCORE_TOL,
              "; ".join("job %d: json %s vs text %s" % item for item in score_bad)
              if score_bad else "%d scores agree" % compared, "jobs"),
        Check("6c", "'Compacted ... => N bytes' text vs total_output_size", "exact",
              0, len(bytes_bad), "PASS" if not bytes_bad else "FAIL", "0",
              "; ".join("job %d: json %d B vs text %d B" % item for item in bytes_bad)
              if bytes_bad else "text line agrees with the JSON event for every job",
              "jobs"),
        Check("6d", "'Compacted' lines present for every 'Compacting' line", "info",
              compared, compared - missing_compacted, "INFO", "-",
              "%d job(s) have a 'Compacting' line but no 'Compacted' line (killed tail)"
              % missing_compacted, "jobs"),
    ]
    return checks


# --------------------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------------------

_HEADER = ("#", "check", "class", "expected", "parsed", "abs diff", "rel diff",
           "tolerance", "status")

#: short suffix printed after numeric check values, so the table stays readable
_UNIT_ABBR = {"bytes": "B", "micros": "us", "jobs": "jobs", "flushes": "flushes",
              "compactions": "comps", "events": "events", "files": "files"}


def print_validation(checks, snapshot, result, stream=sys.stdout):
    """Human-readable validation report; mirrors the docstring's classification."""
    print("", file=stream)
    print("=" * 118, file=stream)
    print("VALIDATION against the log's own statistics counters", file=stream)
    print("=" * 118, file=stream)
    if snapshot is None:
        print("no statistics dump found in this log (run db_bench with --statistics=true",
              file=stream)
        print("and --stats_dump_period_sec=N); no counter-based check could be performed.",
              file=stream)
        print("RESULT: NOT VALIDATABLE", file=stream)
        return EXIT_NOT_VALIDATABLE
    print("snapshot          : %s (statistics dump #%d, line ts of 'STATISTICS:')"
          % (_fmt_ts(snapshot["ts"]), len(result["stats_dumps"])))
    print("counter lag       : the counter block is formatted before the 'STATISTICS:' line is")
    print("                    written, so values trail that timestamp; every comparison uses")
    print("                    [ts <= snapshot - %g s, ts <= snapshot]" % COUNTER_LAG_S)
    for ticker in ("rocksdb.flush.write.bytes", "rocksdb.compact.write.bytes",
                   "rocksdb.compact.read.bytes"):
        print("  %-32s %s" % (ticker, snapshot["tickers"].get(ticker)))
    for histo in ("rocksdb.db.flush.micros", "rocksdb.compaction.times.micros",
                  "rocksdb.compaction.times.cpu_micros"):
        info = snapshot["histograms"].get(histo)
        if info:
            print("  %-32s COUNT %d SUM %d" % (histo, info["count"], info["sum"]))
    print("")
    widths = (4, 52, 11, 16, 16, 15, 11, 26, 6)
    fmt = "  ".join("%%-%ds" % w for w in widths)
    print(fmt % _HEADER, file=stream)
    print("-" * 118, file=stream)
    for chk in checks:
        print(fmt % (
            chk.id, chk.name[:widths[1]], chk.kind,
            _fmt_num(chk.expected, chk.unit), _fmt_num(chk.parsed, chk.unit),
            _fmt_num(chk.abs_diff, chk.unit), _fmt_rel(chk.rel_diff),
            str(chk.tolerance)[:widths[7]], chk.status), file=stream)
    print("", file=stream)
    for chk in checks:
        if chk.detail and chk.status != "PASS":
            print("  [%s] %s" % (chk.id, chk.detail), file=stream)
        elif chk.detail and chk.kind in ("near-exact", "bounded", "info"):
            print("  [%s] %s" % (chk.id, chk.detail), file=stream)
    print("", file=stream)
    print("EXACT-MATCH checks (tolerance 0, modulo the counter-read lag window): 4a, 4c, 4d, "
          "4e, 4f, 5a, 5b, 6a, 6b, 6c", file=stream)
    print("APPROXIMATE checks:", file=stream)
    print("  near-exact   1  flush.write.bytes; the counter also counts the MANIFEST version", file=stream)
    print("                 edit written inside the flush window, so it is expected to be a", file=stream)
    print("                 few hundred bytes LARGER than the sum of the SST sizes.", file=stream)
    print("  near-exact   7  files.deleted.immediately; interval [snapshot - %g s, snapshot] plus" % COUNTER_LAG_S, file=stream)
    print("                 one line for the bump-then-timestamp asymmetry of a deletion.", file=stream)
    print("  bounded      2a compact.write.bytes >= sum(finished job output); the residual is", file=stream)
    print("                 partial output of compactions still running at snapshot time.", file=stream)
    print("  approx       3a compact.read.bytes vs sum(finished job input_data_size); two-sided", file=stream)
    print("                 band because reads can skip blocks or hit the cache, and the counter", file=stream)
    print("                 also carries in-flight progress.", file=stream)
    print("INFORMATIONAL (never affects the exit code): 2b, 3b, 4b, 5c, 6d", file=stream)
    failed = [c for c in checks if c.status == "FAIL"]
    skipped = [c for c in checks if c.status == "SKIP"]
    print("", file=stream)
    print("RESULT: %s  (%d checks, %d failed, %d skipped)"
          % ("FAIL" if failed else "PASS", len(checks), len(failed), len(skipped)),
          file=stream)
    return EXIT_CHECK_FAILED if failed else EXIT_OK


def _fmt_val(value):
    if value is None:
        return "-"
    if isinstance(value, float):
        return "%.6g" % value
    return str(value)


def _fmt_num(value, unit=None):
    text = _fmt_val(value)
    if text != "-" and unit:
        text = "%s %s" % (text, _UNIT_ABBR.get(unit, unit))
    return text


def _fmt_rel(value):
    if value is None:
        return "-"
    return "%.3e" % value


def print_summary(result, stream=sys.stdout):
    ident = result["run_identity"]
    counts = result["counts"]
    print("LOG        : %s" % result["log_path"])
    print("lines      : %d | rocksdb %s | session %s | db %s"
          % (ident["lines"], ident["rocksdb_version"], ident["db_session_id"], ident["db_id"]))
    span = None
    if ident["start_ts"] is not None and ident["end_ts"] is not None:
        span = ident["end_ts"] - ident["start_ts"]
    print("window     : %s -> %s (%.3f s)%s"
          % (_fmt_ts(ident["start_ts"]), _fmt_ts(ident["end_ts"]), span or 0.0,
             "  [tz offset %+.3f s recovered from event lines]" % ident["detected_utc_offset_s"]
             if abs(ident["detected_utc_offset_s"]) > 1e-6 else ""))
    rec = result["recovery"]
    print("recovery   : from_manifest=%s manifest_file_number=%s last_sequence=%s log_number=%s"
          % (rec["recovered_from_manifest"], rec["manifest_file_number"],
             rec["last_sequence"], rec["log_number"]))
    print("options    : %d keys%s" % (len(result["options"]),
          "  write_buffer_size=%s" % result["options"]["write_buffer_size"]
          if "write_buffer_size" in result["options"] else ""))

    jobs = result["jobs"]
    flushes = result["flushes"]
    classes = {}
    for job in jobs:
        classes[job["class"]] = classes.get(job["class"], 0) + 1
    print("compactions: %d  (%s)" % (len(jobs), ", ".join(
        "%s=%d" % (k, classes[k]) for k in sorted(classes)) or "-"))
    print("flushes    : %d | files created %d deleted %d | lsm summaries %d | stalls %d"
          % (len(flushes), counts["files_created"], counts["files_deleted"],
             len(result["lsm_summaries"]), len(result["stalls"])))
    print("trivial    : %d moves | unknown events %d | warnings %d | stats dumps %d"
          % (counts["trivial_moves"], counts["unknown_events"], counts["warnings"],
             counts["stats_dumps"]))

    job_out = sum(j["output_bytes"] or 0 for j in jobs)
    job_in = sum(j["input_data_size"] or 0 for j in jobs)
    flush_out = sum(f["output_bytes"] or 0 for f in flushes)
    wall = sum(j["wall_micros"] or 0 for j in jobs)
    cpu = sum(j["cpu_micros"] or 0 for j in jobs)
    print("bytes      : compaction in %s out %s | flush out %s"
          % (_fmt_bytes(job_in), _fmt_bytes(job_out), _fmt_bytes(flush_out)))
    print("job time   : compaction wall %.3f s cpu %.3f s"
          % (wall / 1e6, cpu / 1e6))

    last_lsm = None
    for entry in reversed(result["lsm_summaries"]):
        last_lsm = entry
        break
    if last_lsm:
        print("lsm state  : %s  (%s @ %s)"
              % ("files" + str(last_lsm["files"]), last_lsm["source"],
                 _fmt_ts(last_lsm["ts"])))
    if result["stats_dumps"]:
        dump = result["stats_dumps"][-1]
        print("last dump  : %s  flush.write.bytes=%s compact.write.bytes=%s "
              "compact.read.bytes=%s"
              % (_fmt_ts(dump["ts"]), dump["tickers"].get("rocksdb.flush.write.bytes"),
                 dump["tickers"].get("rocksdb.compact.write.bytes"),
                 dump["tickers"].get("rocksdb.compact.read.bytes")))
        if dump["cumulative_stall_s"] is not None:
            print("stall      : cumulative %.3f s, interval %.3f s at last dump"
                  % (dump["cumulative_stall_s"], dump["interval_stall_s"] or 0.0))
    kinds = {}
    for warning in result["warnings"]:
        kinds[warning["kind"]] = kinds.get(warning["kind"], 0) + 1
    if kinds:
        print("warn kinds : %s" % ", ".join("%s=%d" % kv for kv in sorted(kinds.items())))


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------

def build_arg_parser():
    ap = argparse.ArgumentParser(
        prog="logparser.py",
        description="Streaming parser and self-validator for RocksDB LOG files.",
        epilog="Exit codes with --validate: 0 all checks passed, 1 a check failed, "
               "2 no statistics dump was found so nothing could be validated. "
               "See the module docstring for the exact tolerance of every check.")
    ap.add_argument("log", help="path to a RocksDB LOG file")
    ap.add_argument("--out-json", metavar="PATH", default=None,
                    help="write the full structured result as JSON")
    ap.add_argument("--csv-dir", metavar="DIR", default=None,
                    help="additionally write jobs.csv, flushes.csv, files_created.csv, "
                         "files_deleted.csv, lsm_summaries.csv, stalls.csv, "
                         "stats_dumps.csv and warnings.csv into DIR")
    ap.add_argument("--validate", action="store_true",
                    help="print a validation report comparing parser sums with the log's "
                         "own statistics counters; exits non-zero on failure")
    ap.add_argument("--quiet", action="store_true",
                    help="suppress the human-readable summary (the validation report is "
                         "still printed)")
    ap.add_argument("--stats-dump-limit", type=int, default=0, metavar="N",
                    help="keep at most N statistics dumps (the first one plus the most "
                         "recent N-1) to bound memory on huge logs; 0 = keep all (default)")
    return ap


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    if not os.path.isfile(args.log):
        print("logparser: no such file: %s" % args.log, file=sys.stderr)
        return 2

    parser = LogParser(stats_dump_limit=args.stats_dump_limit)
    result = parser.parse_file(args.log)

    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2, sort_keys=False)
            handle.write("\n")
        if not args.quiet:
            print("wrote %s" % args.out_json)
    if args.csv_dir:
        written = write_csv_dir(result, args.csv_dir)
        if not args.quiet:
            print("wrote %d CSVs into %s" % (len(written), args.csv_dir))

    if not args.quiet:
        print_summary(result)

    if args.validate:
        checks, snapshot, code = validate(parser)
        print_validation(checks, snapshot, result)
        return code
    return 0


if __name__ == "__main__":
    sys.exit(main())
