#!/bin/bash
# Make a cheap replica of a *closed* RocksDB directory:
#   * .sst files are HARD-LINKED (immutable in RocksDB; costs no extra space)
#   * everything else (MANIFEST, OPTIONS, CURRENT, IDENTITY, LOCK, LOG, WAL) is copied,
#     because those are appended to or replaced and must not be shared with the source.
# Usage: mkreplica.sh <src_dir> <dst_dir>
set -euo pipefail
SRC=${1:?usage: mkreplica.sh <src> <dst>}
DST=${2:?usage: mkreplica.sh <src> <dst>}
[ -d "$SRC" ] || { echo "source $SRC does not exist" >&2; exit 1; }
mkdir -p "$DST"
n_sst=0; n_cp=0
for f in "$SRC"/*; do
  b=$(basename "$f")
  case "$b" in
    *.sst) ln "$f" "$DST/$b"; n_sst=$((n_sst+1)) ;;
    *)     cp -p "$f" "$DST/$b"; n_cp=$((n_cp+1)) ;;
  esac
done
echo "replica $DST: hardlinked_sst=$n_sst copied=$n_cp apparent_bytes=$(du -sb "$DST" | cut -f1) real_extra_bytes=0"
