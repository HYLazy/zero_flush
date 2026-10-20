#!/bin/bash
# Build fillcpu against the pristine native RocksDB shared library.
# Flags mirror the library's own build flags (see bench/build.sh) so ABI and
# code generation settings match.
set -euo pipefail
ROOT=/home/embed/hyl/metadata_offload
RS=$ROOT/source/rocksdb
OUT=$ROOT/bench/tools/fillcpu

DEFS="-DGFLAGS=1 -DGFLAGS_IS_A_DLL=0 -DOS_LINUX -DROCKSDB_AUXV_GETAUXVAL_PRESENT \
-DROCKSDB_FALLOCATE_PRESENT -DROCKSDB_LIB_IO_POSIX -DROCKSDB_MALLOC_USABLE_SIZE \
-DROCKSDB_NO_DYNAMIC_EXTENSION -DROCKSDB_PLATFORM_POSIX -DROCKSDB_PTHREAD_ADAPTIVE_MUTEX \
-DROCKSDB_RANGESYNC_PRESENT -DROCKSDB_SCHED_GETCPU_PRESENT"

g++ -O3 -DNDEBUG -march=native -std=gnu++20 -fno-rtti -fno-omit-frame-pointer -pthread \
  $DEFS -I$RS -I$RS/include -I/usr/include \
  $ROOT/bench/fillcpu.cc -o "$OUT" \
  -L$RS/build -lrocksdb -lpthread -ldl -lz

echo "built: $OUT"
md5sum "$OUT"
