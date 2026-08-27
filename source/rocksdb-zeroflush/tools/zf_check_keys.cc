// ZeroFlush 数据完整性检查：枚举 db_bench 格式的 key（小端 v 8B + 0 填充），
// 逐个 Get，输出 miss 的连续区间分布（定位学习切换/分区边界导致的丢失）。
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "zeroflush/zeroflush_db.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;

static std::string MakeKey(uint64_t v) {
  std::string key(16, '0');
  char* pos = key.data();
  if (true /* little endian */) {
    for (int i = 0; i < 8; ++i) {
      pos[i] = (v >> ((8 - i - 1) << 3)) & 0xFF;
    }
  }
  return key;
}

int main(int argc, char** argv) {
  if (argc < 3) {
    fprintf(stderr, "usage: %s <dbdir> <num_keys> [routing]\n", argv[0]);
    return 2;
  }
  const uint64_t n = strtoull(argv[2], nullptr, 10);
  rocksdb::Options opt;
  opt.create_if_missing = false;
  opt.allow_concurrent_memtable_write = true;
  opt.write_buffer_size = 256 << 20;
  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 16;
  zfo.merge_into_base_level = true;
  zfo.skip_batching = false;
  zfo.value_cache_bytes = 64ull << 20;
  if (argc >= 4 && strcmp(argv[3], "hash") == 0) {
    zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kHash;
  } else {
    zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kSampled;
  }
  std::unique_ptr<DB> db;
  Status s = zeroflush::Open(opt, zfo, argv[1], &db);
  if (!s.ok()) {
    fprintf(stderr, "open failed: %s\n", s.ToString().c_str());
    return 1;
  }
  ReadOptions ro;
  uint64_t miss = 0, hit = 0;
  uint64_t run_start = 0;
  bool in_run = false;
  std::vector<std::pair<uint64_t, uint64_t>> runs;
  for (uint64_t v = 0; v < n; ++v) {
    std::string key = MakeKey(v);
    std::string val;
    s = db->Get(ro, key, &val);
    if (s.ok()) {
      ++hit;
      if (in_run) {
        runs.emplace_back(run_start, v - 1);
        in_run = false;
      }
    } else {
      ++miss;
      if (!in_run) {
        run_start = v;
        in_run = true;
      }
    }
  }
  if (in_run) {
    runs.emplace_back(run_start, n - 1);
  }
  printf("total=%llu hit=%llu miss=%llu (%.2f%%)\n", (unsigned long long)n,
         (unsigned long long)hit, (unsigned long long)miss,
         miss * 100.0 / n);
  const size_t kMaxRuns = 40;
  printf("miss runs=%zu:\n", runs.size());
  for (size_t i = 0; i < runs.size() && i < kMaxRuns; ++i) {
    printf("  [%llu, %llu] len=%llu\n", (unsigned long long)runs[i].first,
           (unsigned long long)runs[i].second,
           (unsigned long long)(runs[i].second - runs[i].first + 1));
  }
  if (runs.size() > kMaxRuns) {
    printf("  ... (%zu more)\n", runs.size() - kMaxRuns);
  }
  return 0;
}
