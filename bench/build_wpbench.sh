#!/bin/bash
# Build wpbench against the pristine native RocksDB shared library (same flags as build.sh).
set -euo pipefail
ROOT=/home/embed/hyl/metadata_offload
RS=$ROOT/source/rocksdb
OUT=$ROOT/bench/tools/wpbench
DEFS="-DGFLAGS=1 -DGFLAGS_IS_A_DLL=0 -DOS_LINUX -DROCKSDB_AUXV_GETAUXVAL_PRESENT \
-DROCKSDB_FALLOCATE_PRESENT -DROCKSDB_LIB_IO_POSIX -DROCKSDB_MALLOC_USABLE_SIZE \
-DROCKSDB_NO_DYNAMIC_EXTENSION -DROCKSDB_PLATFORM_POSIX -DROCKSDB_PTHREAD_ADAPTIVE_MUTEX \
-DROCKSDB_RANGESYNCS_PRESENT -DROCKSDB_SCHED_GETCPU_PRESENT"
g++ -O3 -DNDEBUG -march=native -std=gnu++20 -fno-rtti -fno-omit-frame-pointer -pthread \
  $DEFS -I$RS -I$RS/include -I/usr/include \
  $ROOT/bench/wpbench.cc -o "$OUT" \
  -L$RS/build -lrocksdb -lpthread -ldl -lz
echo "built: $OUT"; md5sum "$OUT"
