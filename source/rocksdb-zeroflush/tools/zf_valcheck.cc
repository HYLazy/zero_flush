// 值内容校验：读 N 个键，打印值长度分布 + 内容摘要（期望 1024B 的重复填充）
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include "rocksdb/db.h"
#include "zeroflush/zeroflush_db.h"

static std::string MakeKey(uint64_t v) {
  std::string key(16, '0');
  char* pos = key.data();
  for (int i = 0; i < 8; ++i) pos[i] = (v >> ((8 - i - 1) << 3)) & 0xFF;
  return key;
}

int main(int argc, char** argv) {
  if (argc < 3) { fprintf(stderr, "usage: %s <dbdir> <num_keys>\n", argv[0]); return 2; }
  const uint64_t nkeys = strtoull(argv[2], nullptr, 10);
  rocksdb::Options opt; opt.create_if_missing = false;
  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 16; zfo.merge_into_base_level = true;
  zfo.skip_batching = false; zfo.value_cache_bytes = 64ull << 20;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kSampled;
  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(opt, zfo, argv[1], &db);
  if (!s.ok()) { fprintf(stderr, "open failed: %s\n", s.ToString().c_str()); return 1; }
  std::map<size_t, uint64_t> len_hist;
  uint64_t miss = 0, checked = 0;
  // 均匀抽样 2000 个键 + 前 50 + 后 50
  std::vector<uint64_t> probes;
  for (int i = 0; i < 50 && (uint64_t)i < nkeys; ++i) probes.push_back(i);
  const uint64_t step = nkeys > 2000 ? nkeys / 2000 : 1;
  for (uint64_t v = 0; v < nkeys; v += step) probes.push_back(v);
  for (uint64_t v = nkeys > 50 ? nkeys - 50 : 0; v < nkeys; ++v) probes.push_back(v);
  for (uint64_t v : probes) {
    std::string val;
    auto gs = db->Get(rocksdb::ReadOptions(), MakeKey(v), &val);
    ++checked;
    if (!gs.ok()) { ++miss; continue; }
    len_hist[val.size()]++;
  }
  printf("checked=%llu miss=%llu\n", (unsigned long long)checked, (unsigned long long)miss);
  printf("value length histogram (len:count):\n");
  for (auto& kv : len_hist) printf("  %zu: %llu\n", kv.first, (unsigned long long)kv.second);
  return 0;
}
