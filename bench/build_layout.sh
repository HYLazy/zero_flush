#!/bin/bash
# Build the L0-layout / Bloom-cost driver.
#
# Deliberately a SEPARATE binary from tools/l0bench: the 50 GB campaign's manifest
# records the exact identity of its instrument, and rebuilding that binary mid-campaign
# is the accident the harness exists to catch. This tool links the same unmodified
# RocksDB shared library with the same flags as build.sh.
set -euo pipefail
ROOT=/home/embed/hyl/metadata_offload
RS=$ROOT/source/rocksdb
OUT=$ROOT/bench/tools/l0bench_layout

DEFS="-DGFLAGS=1 -DGFLAGS_IS_A_DLL=0 -DOS_LINUX -DROCKSDB_AUXV_GETAUXVAL_PRESENT \
-DROCKSDB_FALLOCATE_PRESENT -DROCKSDB_LIB_IO_POSIX -DROCKSDB_MALLOC_USABLE_SIZE \
-DROCKSDB_NO_DYNAMIC_EXTENSION -DROCKSDB_PLATFORM_POSIX -DROCKSDB_PTHREAD_ADAPTIVE_MUTEX \
-DROCKSDB_RANGESYNC_PRESENT -DROCKSDB_SCHED_GETCPU_PRESENT"

g++ -O3 -DNDEBUG -march=native -std=gnu++20 -fno-rtti -fno-omit-frame-pointer -pthread \
  $DEFS -I$RS -I$RS/include -I/usr/include \
  $ROOT/bench/layoutbench.cc -o "$OUT" \
  -L$RS/build -lrocksdb -lgflags -lpthread -ldl -lz

echo "built: $OUT"
md5sum "$OUT" | cut -d' ' -f1
sha256sum "$OUT" | cut -c1-24
