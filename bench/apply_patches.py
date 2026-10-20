#!/usr/bin/env python3
"""Post-run patches to bench/campaign.sh.

bash reads a running script lazily, so these edits are deliberately deferred until no
campaign.sh process is alive. Run with:  python3 bench/apply_patches.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
p = os.path.join(HERE, 'campaign.sh')
s = open(p).read()
if 'STAGE_G_APPLIED' in s:
    print('already patched')
    sys.exit(0)


def sub(old, new, why):
    global s
    assert old in s, 'anchor missing: ' + why
    s = s.replace(old, new, 1)


# 1) keep the 50GB DB after the overwrite so the mix run can use the same state
sub("""    local rc=$?; finish_run "$rdir" "$db" 0
    record "$rid" "\\"phase\\":\\"overwrite\\",\\"scale\\":\\"50gb\\",\\"rc\\":$rc\"""",
    """    local rc=$?; finish_run "$rdir" "$db" 1
    record "$rid" "\\"phase\\":\\"overwrite\\",\\"scale\\":\\"50gb\\",\\"rc\\":$rc\"""",
    'overwrite keep DB')

# 2) mix run: fall back to a fresh replica of base50 when the working DB is gone
sub("""  if wants 4; then   # online mixed window at 50GB (read+write interleaved)
    require_db "$BENCH/db/$B50" || continue
    local rid=50gb_mix; local db=$BENCH/db/$B50; local rdir=$OUT/$rid
    mkdir -p "$rdir\"""",
    """  if wants 4; then   # online mixed window at 50GB (read+write interleaved)
    local rid=50gb_mix; local rdir=$OUT/$rid
    local db=$BENCH/db/$B50
    if [ ! -d "$db" ]; then
      [ -d "$BENCH/base/base50" ] || continue
      echo "### mix: working DB gone, using a replica of base50" >&2
      db=$BENCH/db/50gb_mix_w
      rm -rf "$db"
      mkdir -p "$rdir"
      bash "$MKREP" "$BENCH/base/base50" "$db" > "$rdir/replica.log" 2>&1 || continue
    fi
    mkdir -p "$rdir\"""",
    'mix fallback')

# 3) E10: single writer + monotonic arrival so the confirmed op count is exactly the
#    confirmed key prefix [0, confirmed); add a memtable-sealed arm
sub("""      --num_ops=20000000 --threads=1 --value_size=1024 --write_buffer_size=16M \\""",
    """      --num_ops=20000000 --dist=monotonic --order=identity --threads=1 --value_size=1024 \\
      --write_buffer_size=16M \\""",
    'E10 monotonic')
sub("""  local U=2000000           # 2 GB at 1 KiB values; many flush cycles with 16 MiB WB""",
    """  local U=20000000          # ids never wrap before the kill: confirmed set is a prefix""",
    'E10 universe')
sub("""  for spec in "kflush:flush_installed:1:0" "kflush_sync:flush_installed:1:1" \\
              "kcomp:compaction_installed:1:0" "ktime:time:8:0" "ktime_sync:time:8:1"; do""",
    """  for spec in "kseal:memtable_sealed:3:0" "kflush:flush_installed:1:0" \\
              "kflush_sync:flush_installed:1:1" "kcomp:compaction_installed:1:0" \\
              "ktime:time:8:0" "ktime_sync:time:8:1"; do""",
    'E10 arms')
sub("""      flush_installed|compaction_installed) kargs="--kill_at_event=$ev --kill_after_event_n=$n" ;;""",
    """      flush_installed|compaction_installed|memtable_sealed) kargs="--kill_at_event=$ev --kill_after_event_n=$n" ;;""",
    'E10 kill event')

# 4) extra stage-A runs (cold reads, scans, 100GB snapshot)
frag = open(os.path.join(HERE, 'extra_runs.fragment')).read()
frag = '\n'.join(l[2:] if l.startswith('  ') else l for l in frag.split('\n'))
sub("  if wants 8; then   # variant: static levelled",
    frag + "  if wants 8; then   # variant: static levelled", 'extra runs')

# 5) stage G -- arrival order at 50GB (E2 core test)
stage_g = open(os.path.join(HERE, 'stage_g.fragment')).read()
sub("""# ---------------------------------------------------------------------------
# Stage E -- E10: recovery""",
    stage_g + """# ---------------------------------------------------------------------------
# Stage E -- E10: recovery""", 'stage G insert')

# 6) dispatcher
sub("""  E) stage_E ;;""", """  E) stage_E ;;
  F) stage_A ;;
  G) stage_G ;;""", 'dispatch F/G')
sub("""    for st in A B C D E; do""", """    for st in A B C D E F G; do""", 'status F/G')
s = s.replace('set -uo pipefail', 'set -uo pipefail\n# STAGE_G_APPLIED', 1)

open(p, 'w').write(s)
print('patched campaign.sh')
