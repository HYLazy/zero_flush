// overlap_probe.cc - ZeroFlush 实验 1 基线探针工具
//
// 功能:
//   打开指定 RocksDB (只读) -> GetLiveFilesMetaData -> 过滤 level==0 的 L0 文件,
//   在 key 空间 [0, num_keys) 上均匀随机采样 samples 个 key
//   (key 编码与 db_bench GenerateKeyFromInt 完全一致:
//    大端 8 字节索引 + '0' 填充至 key_size), 统计每个 key 被多少 L0 文件的
//   [smallest_key, largest_key] 覆盖, 输出 overlap_factor_mean / p99 /
//   l0_file_count (JSON)。
//
// 读路径代理指标:
//   point_query_l0_files_checked_mean: 点查一个 key 时, L0 层中 key range
//   覆盖该 key 的文件数(即最坏情况下必须探测的 L0 文件数)。用
//   GetLiveFilesMetaData 的 range 覆盖数作为代理, 与 overlap_factor_mean
//   同源; 交叉验证由 db_bench readrandom + PerfContext 在编排脚本中完成。
//
// 链接: source/rocksdb/build/librocksdb.so (vanilla 树, 只读, 不修改)。
// 运行: LD_LIBRARY_PATH=<rocksdb build 目录>:/usr/lib/x86_64-linux-gnu

#include <algorithm>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

#include "common/cpp/json_out.h"

namespace {

struct Args {
  std::string db;
  int64_t num_keys = 0;
  int64_t samples = 100000;
  int key_size = 16;
  uint64_t seed = 42;
  std::string out;  // 为空则输出到 stdout
};

void Usage() {
  fprintf(stderr,
          "usage: overlap_probe --db=<path> --num_keys=<N> "
          "[--samples=100000] [--key_size=16] [--seed=42] [--out=<json>]\n");
}

bool ParseArgs(int argc, char** argv, Args* a) {
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto val = [&](const char* prefix) -> std::string {
      if (arg.rfind(prefix, 0) == 0) return arg.substr(strlen(prefix));
      return "";
    };
    std::string v;
    if (!(v = val("--db=")).empty()) {
      a->db = v;
    } else if (!(v = val("--num_keys=")).empty()) {
      a->num_keys = std::stoll(v);
    } else if (!(v = val("--samples=")).empty()) {
      a->samples = std::stoll(v);
    } else if (!(v = val("--key_size=")).empty()) {
      a->key_size = std::stoi(v);
    } else if (!(v = val("--seed=")).empty()) {
      a->seed = std::stoull(v);
    } else if (!(v = val("--out=")).empty()) {
      a->out = v;
    } else if (arg == "--help" || arg == "-h") {
      Usage();
      exit(0);
    } else {
      fprintf(stderr, "unknown arg: %s\n", arg.c_str());
      return false;
    }
  }
  if (a->db.empty() || a->num_keys <= 0) {
    Usage();
    return false;
  }
  return true;
}

// 与 db_bench GenerateKeyFromInt 一致 (keys_per_prefix=0):
// 大端写入 8 字节 v, 其余填充 '0'。key_size >= 8。
std::string GenerateKey(uint64_t v, int key_size) {
  std::string key(static_cast<size_t>(key_size), '0');
  int bytes_to_fill = std::min(key_size, 8);
  for (int i = 0; i < bytes_to_fill; ++i) {
    key[i] = static_cast<char>((v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF);
  }
  return key;
}

}  // namespace

int main(int argc, char** argv) {
  Args args;
  if (!ParseArgs(argc, argv, &args)) return 2;

  ROCKSDB_NAMESPACE::Options options;
  options.create_if_missing = false;
  std::unique_ptr<ROCKSDB_NAMESPACE::DB> db;
  ROCKSDB_NAMESPACE::Status s =
      ROCKSDB_NAMESPACE::DB::OpenForReadOnly(options, args.db, &db);
  if (!s.ok()) {
    fprintf(stderr, "FATAL: OpenForReadOnly(%s) failed: %s\n",
            args.db.c_str(), s.ToString().c_str());
    return 1;
  }

  std::vector<ROCKSDB_NAMESPACE::LiveFileMetaData> all_files;
  db->GetLiveFilesMetaData(&all_files);

  // 过滤 L0 (level==0); key range 用 BytewiseComparator 比较
  // (db_bench 默认 BytewiseComparator)。
  const ROCKSDB_NAMESPACE::Comparator* cmp =
      ROCKSDB_NAMESPACE::BytewiseComparator();
  struct L0Range {
    std::string smallest;
    std::string largest;
    uint64_t file_number;
  };
  std::vector<L0Range> l0;
  for (const auto& f : all_files) {
    if (f.level == 0) {
      l0.push_back(L0Range{f.smallestkey, f.largestkey, f.file_number});
    }
  }

  // 均匀随机采样 key 并统计覆盖数
  std::mt19937_64 rng(args.seed);
  std::uniform_int_distribution<uint64_t> dist(
      0, static_cast<uint64_t>(args.num_keys - 1));
  std::vector<int> coverage;
  coverage.reserve(static_cast<size_t>(args.samples));
  uint64_t total_cov = 0;
  int max_cov = 0;
  for (int64_t i = 0; i < args.samples; ++i) {
    std::string key = GenerateKey(dist(rng), args.key_size);
    ROCKSDB_NAMESPACE::Slice k(key);
    int cov = 0;
    for (const auto& r : l0) {
      if (cmp->Compare(k, ROCKSDB_NAMESPACE::Slice(r.smallest)) >= 0 &&
          cmp->Compare(k, ROCKSDB_NAMESPACE::Slice(r.largest)) <= 0) {
        ++cov;
      }
    }
    coverage.push_back(cov);
    total_cov += static_cast<uint64_t>(cov);
    if (cov > max_cov) max_cov = cov;
  }

  std::sort(coverage.begin(), coverage.end());
  double mean = coverage.empty()
                    ? 0.0
                    : static_cast<double>(total_cov) /
                          static_cast<double>(coverage.size());
  double p99 = 0.0;
  if (!coverage.empty()) {
    size_t idx = static_cast<size_t>(
        (static_cast<double>(coverage.size()) * 0.99));
    if (idx >= coverage.size()) idx = coverage.size() - 1;
    p99 = static_cast<double>(coverage[idx]);
  }

  pre_exp::JsonWriter w;
  w.BeginObject();
  w.Key("tool").String("overlap_probe");
  w.Key("db").String(args.db);
  w.Key("num_keys").Number(args.num_keys);
  w.Key("samples").Number(args.samples);
  w.Key("key_size").Number(static_cast<int64_t>(args.key_size));
  w.Key("seed").Number(args.seed);
  w.Key("total_live_sst_count").Number(static_cast<int64_t>(all_files.size()));
  w.Key("l0_file_count").Number(static_cast<int64_t>(l0.size()));
  w.Key("overlap_factor_mean").Number(mean);
  w.Key("overlap_factor_p99").Number(p99);
  w.Key("overlap_factor_max").Number(static_cast<int64_t>(max_cov));
  w.Key("point_query_l0_files_checked_mean").Number(mean);
  w.Key("measurement_method").String(
      "uniform-random sample keys over [0,num_keys) with db_bench "
      "GenerateKeyFromInt encoding (big-endian 8B index + '0' padding); "
      "per-key coverage = number of L0 files (GetLiveFilesMetaData, level==0) "
      "whose [smallest_key,largest_key] covers the key, compared via "
      "BytewiseComparator; point_query_l0_files_checked_mean is the same "
      "coverage count used as proxy for L0 files a point query must check; "
      "cross-checked against db_bench readrandom + PerfContext in run_exp1");
  w.EndObject();

  std::string out = w.Dump() + "\n";
  if (args.out.empty()) {
    fputs(out.c_str(), stdout);
  } else {
    FILE* fp = fopen(args.out.c_str(), "w");
    if (!fp) {
      fprintf(stderr, "FATAL: cannot write %s\n", args.out.c_str());
      return 1;
    }
    fwrite(out.data(), 1, out.size(), fp);
    fclose(fp);
  }
  return 0;
}
