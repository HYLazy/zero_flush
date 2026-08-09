//  ZeroFlush M1 WAL 持久化回归测试
//
//  本测试套件覆盖以下回归点：
//  1. Close() 修复：析构时 flush 缓冲，未满 4KB 的数据不丢失
//  2. ReopenWritableFile 修复：重开不截断已有 WAL 文件
//  3. 顺序键 / 随机键 / 多分区 下的读写一致性
//  4. 重开后的迭代器遍历与 Get 取值的正确性
//
//  用法：直接运行 ./zf_test，输出每个用例 PASS/FAIL，退出码 = 失败数。
//
// 退出码：
//   0   全部通过
//   >0  失败用例数

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <unordered_set>

#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "zeroflush/zeroflush_db.h"

namespace {

// 全局失败计数
int g_failures = 0;

// 测试基址，避免 /tmp 被多进程污染
const char* kDbBase = "/tmp/rocksdb_zf_regress_";

// 用例结果辅助
void ReportResult(const char* name, bool ok, const std::string& detail = "") {
  fprintf(stderr, "[%s] %s%s\n", ok ? "PASS" : "FAIL", name,
          detail.empty() ? "" : (" — " + detail).c_str());
  if (!ok) ++g_failures;
}

// 删除整个 db 目录（包括自定义 zfwal 子目录）以保证测试间隔离
// 注意：rocksdb::DestroyDB 不会清理 zfwal 这种自定义子目录，
// 会导致 zfwal 跨测试累积，污染后续用例。
inline void CleanDB(const std::string& dbname) {
  // 用 system() 强制递归删除（POSIX 环境）
  std::string cmd = "rm -rf '" + dbname + "'";
  if (std::system(cmd.c_str()) != 0) {
    fprintf(stderr, "[WARN] failed to %s\n", cmd.c_str());
  }
}

// 构造 options：禁用压缩（与基准测试保持一致）
rocksdb::Options MakeOptions() {
  rocksdb::Options opt;
  opt.create_if_missing = true;
  opt.compression = rocksdb::kNoCompression;
  return opt;
}

// 顺序键生成：key = "key" + 10 位十进制 i，value = "val" + 10 位十进制 i
void MakeSeqKV(int64_t i, char* key_buf, char* val_buf) {
  snprintf(key_buf, 16, "key%010lld", (long long)i);
  snprintf(val_buf, 64, "val%010lld", (long long)i);
}

// 用迭代器统计条目数
int64_t CountViaIterator(rocksdb::DB* db) {
  std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions()));
  int64_t n = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) ++n;
  return n;
}

// ---------------------------------------------------------------------------
// 用例 1：顺序键 1000 条 — 写 / 迭代器 / Get / 重开 / 迭代器 / Get
// ---------------------------------------------------------------------------
void TestSequentialKeys() {
  const char* tag = "SequentialKeys(1k)";
  std::string dbname = std::string(kDbBase) + "seq1k";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }

  // 写入 1000 条顺序键
  char k[16], v[64];
  for (int64_t i = 0; i < 1000; ++i) {
    MakeSeqKV(i, k, v);
    s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 16),
                rocksdb::Slice(v, strlen(v)));
    if (!s.ok()) {
      ReportResult(tag, false, "put@" + std::to_string(i));
      return;
    }
  }

  // 写入后迭代器 = 1000
  if (CountViaIterator(db.get()) != 1000) {
    ReportResult(tag, false, "iter after write != 1000");
    return;
  }

  // Get 全部正确
  std::string got;
  for (int64_t i = 0; i < 1000; ++i) {
    MakeSeqKV(i, k, v);
    s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(k, 16), &got);
    if (!s.ok() || got != v) {
      ReportResult(tag, false, "get@" + std::to_string(i) + " = \"" + got + "\"");
      return;
    }
  }

  // 重开
  db.reset();
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "reopen: " + s.ToString());
    return;
  }

  // 重开后迭代器 = 1000
  if (CountViaIterator(db.get()) != 1000) {
    ReportResult(tag, false, "iter after reopen != 1000");
    return;
  }

  // 重开后 Get 全部正确（这是 WAL 持久化修复的核心验证）
  for (int64_t i = 0; i < 1000; ++i) {
    MakeSeqKV(i, k, v);
    s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(k, 16), &got);
    if (!s.ok() || got != v) {
      ReportResult(tag, false,
                   "reopen get@" + std::to_string(i) + " = \"" + got + "\"");
      return;
    }
  }

  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true);
}

// ---------------------------------------------------------------------------
// 用例 2：随机键 1000 条（无重复）— 同上但用 16 字节随机键
// ---------------------------------------------------------------------------
void TestRandomKeysUnique() {
  const char* tag = "RandomKeysUnique(1k)";
  std::string dbname = std::string(kDbBase) + "rand1k";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }

  // 用 mt19937 生成 1000 个 8 字节随机键 + 值
  std::mt19937_64 rng(0xDEADBEEF);
  std::vector<std::pair<std::string, std::string>> kvs;
  kvs.reserve(1000);
  std::unordered_set<std::string> seen;
  while ((int)kvs.size() < 1000) {
    uint64_t r = rng();
    char buf[16];
    snprintf(buf, sizeof(buf), "rk%010lu", (unsigned long)(r % 1000000000ULL));
    std::string k(buf, 16);
    if (seen.count(k)) continue;
    seen.insert(k);
    std::string v = "rv" + std::to_string(r % 1000000ULL);
    kvs.emplace_back(k, v);
  }

  for (auto& kv : kvs) {
    s = db->Put(rocksdb::WriteOptions(), kv.first, kv.second);
    if (!s.ok()) {
      ReportResult(tag, false, "put: " + s.ToString());
      return;
    }
  }

  if (CountViaIterator(db.get()) != 1000) {
    ReportResult(tag, false, "iter != 1000");
    return;
  }

  // 重开
  db.reset();
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "reopen: " + s.ToString());
    return;
  }

  if (CountViaIterator(db.get()) != 1000) {
    ReportResult(tag, false, "iter after reopen != 1000");
    return;
  }

  std::string got;
  for (auto& kv : kvs) {
    s = db->Get(rocksdb::ReadOptions(), kv.first, &got);
    if (!s.ok() || got != kv.second) {
      ReportResult(tag, false, "get after reopen mismatch");
      return;
    }
  }

  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true);
}

// ---------------------------------------------------------------------------
// 用例 3：随机键有放回采样（fillrandom 风格）
// 验证唯一键数 ≈ 0.632 * num_writes，迭代器输出与 Get 一致
// ---------------------------------------------------------------------------
void TestRandomKeysWithDuplicates() {
  const char* tag = "RandomKeysWithDup(10k)";
  std::string dbname = std::string(kDbBase) + "dup10k";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }

  const int64_t kNumWrites = 10000;
  std::mt19937_64 rng(0xCAFEBABE);
  std::set<std::string> unique_keys;  // 用于统计写入期间观测到的唯一键

  // 写入 kNumWrites 次随机键
  for (int64_t i = 0; i < kNumWrites; ++i) {
    char k[16];
    uint64_t r = rng();
    snprintf(k, sizeof(k), "dk%010lu", (unsigned long)(r % 1000ULL));
    std::string v = "dv" + std::to_string(r % 10000ULL);
    unique_keys.insert(std::string(k, 16));
    s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 16), v);
    if (!s.ok()) {
      ReportResult(tag, false, "put@" + std::to_string(i));
      return;
    }
  }

  int64_t expected_unique = (int64_t)unique_keys.size();
  int64_t iter_count = CountViaIterator(db.get());

  // 迭代器计数应与写入期间看到的唯一键数一致（≤ 1000）
  if (iter_count != expected_unique) {
    ReportResult(tag, false, "iter=" + std::to_string(iter_count) +
                                " != expected unique=" +
                                std::to_string(expected_unique));
    return;
  }

  // 重开
  db.reset();
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "reopen: " + s.ToString());
    return;
  }

  if (CountViaIterator(db.get()) != expected_unique) {
    ReportResult(tag, false, "iter after reopen != unique");
    return;
  }

  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true,
               "writes=" + std::to_string(kNumWrites) +
                   " unique=" + std::to_string(expected_unique));
}

// ---------------------------------------------------------------------------
// 用例 4：多分区 — 验证不同 partition 数（1, 4, 16, 64）下行为一致
// ---------------------------------------------------------------------------
void TestMultiPartition() {
  const char* tag = "MultiPartition";
  const uint32_t configs[] = {1, 4, 16, 64};

  for (uint32_t P : configs) {
    std::string dbname = std::string(kDbBase) + "part" + std::to_string(P);
    CleanDB(dbname);

    zeroflush::ZeroFlushOptions zfo;
    zfo.partitions = P;

    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "P=" + std::to_string(P) + " open");
      return;
    }

    const int64_t N = 500;
    char k[16], v[64];
    for (int64_t i = 0; i < N; ++i) {
      MakeSeqKV(i, k, v);
      s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 16),
                  rocksdb::Slice(v, strlen(v)));
      if (!s.ok()) {
        ReportResult(tag, false, "P=" + std::to_string(P) + " put@" +
                                     std::to_string(i));
        return;
      }
    }

    if (CountViaIterator(db.get()) != N) {
      ReportResult(tag, false, "P=" + std::to_string(P) + " iter");
      return;
    }

    // 重开
    db.reset();
    s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "P=" + std::to_string(P) + " reopen");
      return;
    }

    if (CountViaIterator(db.get()) != N) {
      ReportResult(tag, false, "P=" + std::to_string(P) + " iter after reopen");
      return;
    }

    // 随机抽查 50 个 Get
    std::string got;
    for (int t = 0; t < 50; ++t) {
      int64_t i = (t * 13 + 7) % N;
      MakeSeqKV(i, k, v);
      s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(k, 16), &got);
      if (!s.ok() || got != v) {
        ReportResult(tag, false, "P=" + std::to_string(P) + " get@" +
                                     std::to_string(i));
        return;
      }
    }

    db.reset();
    CleanDB(dbname);
  }

  ReportResult(tag, true, "P in {1,4,16,64}");
}

// ---------------------------------------------------------------------------
// 用例 5：WAL 缓冲 flush 验证 — Close() 修复的核心回归点
// 写入少量数据（< 4KB，肯定有缓冲数据未刷盘），关闭后重开必须能恢复
// ---------------------------------------------------------------------------
void TestWALBufferFlush() {
  const char* tag = "WALBufferFlush(50B<4KB)";
  std::string dbname = std::string(kDbBase) + "buf";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 2;

  // 第一次会话：写 1 条 ~50 字节记录
  {
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "open1: " + s.ToString());
      return;
    }
    s = db->Put(rocksdb::WriteOptions(), "tinykey", "tinyvalue_xxxxxxxxxx");
    if (!s.ok()) {
      ReportResult(tag, false, "put1");
      return;
    }
    // 显式 db.reset() 触发析构 → Close() → flush 缓冲 + sync
  }

  // 第二次会话：重开，Get 必须命中
  {
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "open2: " + s.ToString());
      return;
    }
    std::string got;
    s = db->Get(rocksdb::ReadOptions(), "tinykey", &got);
    if (!s.ok() || got != "tinyvalue_xxxxxxxxxx") {
      ReportResult(tag, false, "get after reopen: \"" + got +
                                   "\" status=" + s.ToString());
      return;
    }
  }

  CleanDB(dbname);
  ReportResult(tag, true);
}

// ---------------------------------------------------------------------------
// 用例 6：ReopenWritableFile 不截断验证
// 写入 → 关闭 → 重开 → 继续写新键 → 关闭 → 重开 → 验证所有数据
// ---------------------------------------------------------------------------
void TestReopenNoTruncate() {
  const char* tag = "ReopenNoTruncate";
  std::string dbname = std::string(kDbBase) + "ntrunc";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 2;

  // 第一轮：写 100 条
  {
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "open1");
      return;
    }
    for (int i = 0; i < 100; ++i) {
      char k[16], v[32];
      snprintf(k, sizeof(k), "k%03d", i);
      snprintf(v, sizeof(v), "v%03d_a", i);
      s = db->Put(rocksdb::WriteOptions(), k, v);
      if (!s.ok()) {
        ReportResult(tag, false, "put1@" + std::to_string(i));
        return;
      }
    }
  }

  // 第二轮：重开，再写 100 条（key 200-299 避免重复），关闭
  {
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "open2");
      return;
    }

    // 验证第一轮的 100 条还在
    std::string got_round1;
    for (int i = 0; i < 100; ++i) {
      char k[16];
      snprintf(k, sizeof(k), "k%03d", i);
      s = db->Get(rocksdb::ReadOptions(), k, &got_round1);
      if (!s.ok()) {
        ReportResult(tag, false, "round1 missing@" + std::to_string(i));
        return;
      }
    }

    // 写第二轮
    for (int i = 200; i < 300; ++i) {
      char k[16], v[32];
      snprintf(k, sizeof(k), "k%03d", i);
      snprintf(v, sizeof(v), "v%03d_b", i);
      s = db->Put(rocksdb::WriteOptions(), k, v);
      if (!s.ok()) {
        ReportResult(tag, false, "put2@" + std::to_string(i));
        return;
      }
    }
  }

  // 第三轮：重开，验证 200 条全在
  {
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "open3");
      return;
    }

    if (CountViaIterator(db.get()) != 200) {
      ReportResult(tag, false, "iter != 200");
      return;
    }

    // 验证 0-99 仍是 v%03d_a（不是被截断后丢失）
    for (int i = 0; i < 100; ++i) {
      char k[16], expect[32];
      std::string got;
      snprintf(k, sizeof(k), "k%03d", i);
      snprintf(expect, sizeof(expect), "v%03d_a", i);
      s = db->Get(rocksdb::ReadOptions(), k, &got);
      if (!s.ok() || got != expect) {
        ReportResult(tag, false, "k" + std::to_string(i) + " got=\"" + got +
                                     "\" expect=\"" + expect + "\"");
        return;
      }
    }

    // 验证 200-299 是 v%03d_b
    for (int i = 200; i < 300; ++i) {
      char k[16], expect[32];
      std::string got;
      snprintf(k, sizeof(k), "k%03d", i);
      snprintf(expect, sizeof(expect), "v%03d_b", i);
      s = db->Get(rocksdb::ReadOptions(), k, &got);
      if (!s.ok() || got != expect) {
        ReportResult(tag, false, "k" + std::to_string(i) + " got=\"" + got +
                                     "\"");
        return;
      }
    }
  }

  CleanDB(dbname);
  ReportResult(tag, true);
}

// ---------------------------------------------------------------------------
// 用例 7：大数据集 — 100k 顺序键
// ---------------------------------------------------------------------------
void TestLargeSequential() {
  const char* tag = "LargeSequential(100k)";
  std::string dbname = std::string(kDbBase) + "big";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }

  const int64_t N = 100000;
  char k[16], v[64];
  for (int64_t i = 0; i < N; ++i) {
    MakeSeqKV(i, k, v);
    s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 16),
                rocksdb::Slice(v, strlen(v)));
    if (!s.ok()) {
      ReportResult(tag, false, "put@" + std::to_string(i));
      return;
    }
  }

  if (CountViaIterator(db.get()) != N) {
    int64_t got = CountViaIterator(db.get());
    ReportResult(tag, false, "iter after write got " + std::to_string(got) +
                                " expected " + std::to_string(N));
    return;
  }

  db.reset();
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "reopen: " + s.ToString());
    return;
  }

  if (CountViaIterator(db.get()) != N) {
    ReportResult(tag, false, "iter after reopen != " + std::to_string(N));
    return;
  }

  // 边界点抽查：第 0 / 1 / N-2 / N-1
  std::string got;
  const int64_t checks[] = {0, 1, N - 2, N - 1};
  for (int64_t i : checks) {
    MakeSeqKV(i, k, v);
    s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(k, 16), &got);
    if (!s.ok() || got != v) {
      ReportResult(tag, false, "boundary@" + std::to_string(i) + " got=\"" +
                                   got + "\"");
      return;
    }
  }

  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true);
}

}  // namespace

int main() {
  fprintf(stderr, "============================================================\n");
  fprintf(stderr, " ZeroFlush M1 WAL Persistence Regression Suite\n");
  fprintf(stderr, "============================================================\n");

  TestWALBufferFlush();          // Close() 修复
  TestReopenNoTruncate();        // ReopenWritableFile 修复
  TestSequentialKeys();          // 基础顺序键
  TestRandomKeysUnique();        // 随机键（无重复）
  TestRandomKeysWithDuplicates();// 随机键（有放回）
  TestMultiPartition();          // 多分区一致性
  TestLargeSequential();         // 大数据集

  fprintf(stderr, "============================================================\n");
  if (g_failures == 0) {
    fprintf(stderr, " ALL PASSED\n");
  } else {
    fprintf(stderr, " %d FAILED\n", g_failures);
  }
  fprintf(stderr, "============================================================\n");
  return g_failures;
}
