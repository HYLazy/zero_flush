//  Copyright (c) 2013-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#else
#include "rocksdb/db_bench_tool.h"
#include <malloc.h>
int main(int argc, char** argv) {
  // ZeroFlush M5：限制 glibc malloc arena 数——多线程（36+）下默认
  // arena 池（8×核）膨胀至数十 GB 且不还 OS（50GB 实测 44GB at 32GB
  // 数据 → OOM；MALLOC_ARENA_MAX=4 后 7.6GB 且吞吐 +~2×）。
#if defined(__GLIBC__)
  mallopt(M_ARENA_MAX, 4);
#endif

  return ROCKSDB_NAMESPACE::db_bench_tool(argc, argv);
}
#endif  // GFLAGS
