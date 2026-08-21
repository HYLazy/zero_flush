#!/usr/bin/env bash
# build_overlap_probe.sh - 编译 overlap_probe (链接 vanilla librocksdb.so)
#
# 用法: ./build_overlap_probe.sh
# 输出: 同目录 overlap_probe 可执行文件。
# 运行时: LD_LIBRARY_PATH=<rocksdb build 目录>:/usr/lib/x86_64-linux-gnu
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CODE_DIR="$(cd "$HERE/../.." && pwd)"          # .../pre_experiment/code
ROOT="/home/embed/hyl/metadata_offload"
ROCKSDB="$ROOT/source/rocksdb"
ROCKSDB_BUILD="$ROCKSDB/build"

CXX="${CXX:-g++}"
"${CXX}" -std=c++20 -O2 -Wall -Wextra -pthread \
    -I"${ROCKSDB}/include" \
    -I"${CODE_DIR}" \
    "${HERE}/overlap_probe.cc" \
    -o "${HERE}/overlap_probe" \
    -L"${ROCKSDB_BUILD}" -lrocksdb \
    -Wl,-rpath,"${ROCKSDB_BUILD}"

echo "[build] overlap_probe -> ${HERE}/overlap_probe"
