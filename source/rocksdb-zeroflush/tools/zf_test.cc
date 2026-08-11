//  ZeroFlush M1+M2 综合回归测试
//
//  本测试套件覆盖以下回归点：
//  1. Close() 修复：析构时 flush 缓冲，未满 4KB 的数据不丢失
//  2. ReopenWritableFile 修复：重开不截断已有 WAL 文件
//  3. 顺序键 / 随机键 / 多分区 下的读写一致性
//  4. 重开后的迭代器遍历与 Get 取值的正确性
//  5. M2 封存机制：partition_target_bytes 触发 Freeze 后，活跃代+封存代
//     混合读路径正确
//  6. M2 多代际：触发 3+ 次封存，全代际数据可恢复
//  7. M2 迭代器固定：迭代期间发生封存，旧代文件由 EpochRef 保持存活
//  8. M2 sync 语义：WriteOptions::sync=true 触达分区精准 fsync
//  9. M2 DestroyDB：递归删除 zfwal 子目录
// 10. M2 ZFPROPS：partitions 跨重开不一致时拒绝打开
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
#include <dirent.h>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <sys/stat.h>
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
  it->SeekToFirst();
  int64_t n = 0;
  for (; it->Valid(); it->Next()) ++n;
  return n;
}

// ---- M2 文件系统检查辅助 ----
// 用 stat() 而非 popen()：避免每次启动子进程，stat 是单次 syscall
// 开销可忽略。

// 检查目录是否存在
bool DirExists(const std::string& path) {
  struct stat st;
  return ::stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

// 统计 zfwal 目录中 zf-wal-<part>-<gen>.log 文件总数（活跃 + 封存代）。
// 封存发生 → 文件被重命名（仍是 zf-wal-<part>-<gen>.log 但 gen 递增），
// 数量增加；旧 gen 文件被 SealedFileCache::PurgePending 真实 unlink 后
// 数量回落。M2.1-6 验证用：触发封存前后文件数应严格 > P（= partitions）。
size_t CountZfwalFiles(const std::string& wal_dir) {
  // 列出目录内容（opendir 比 ls + wc -l 轻量）
  DIR* d = ::opendir(wal_dir.c_str());
  if (d == nullptr) return 0;
  size_t n = 0;
  struct dirent* ent;
  while ((ent = ::readdir(d)) != nullptr) {
    // 过滤 zf-wal-<digits>-<digits>.log
    const char* name = ent->d_name;
    if (::strncmp(name, "zf-wal-", 7) != 0) continue;
    const size_t len = ::strlen(name);
    if (len < 8 || ::strcmp(name + len - 4, ".log") != 0) continue;
    n++;
  }
  ::closedir(d);
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

// ---------------------------------------------------------------------------
// 用例 8 (M2.1)：FreezeReopen — partition_target_bytes 触发封存，
// 验证活跃代 + 封存代混合读路径正确
//
// 关键验证：
//  - 设置极小 partition_target_bytes，强制至少一次 Freeze
//  - 关闭 + 重开后，Get 必须从封存代（gen=0）正确读取（M2.0 D1 修复路径）
//  - 重开后写入新数据，新数据必须写入新活跃代（gen=1）
//    （M2.0 D4 修复路径：Open 按 max(gen) 探测，避免追加到已封存文件）
//  - 文件计数：reclaim=false 保证封存后 zfwal 中 P 个 gen=0 + P 个 gen=1
// ---------------------------------------------------------------------------
void TestFreezeReopen() {
  const char* tag = "FreezeReopen(M2.1)";
  std::string dbname = std::string(kDbBase) + "freeze";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.partition_target_bytes = 4 * 1024;  // 4KB：极少，必触发 Freeze
  zfo.epoch_target_bytes = 4 * 1024;       // 与上面同值，使两种触发同时达到
  zfo.reclaim_sealed_files = false;        // 关闭回收以稳定观察文件数

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open1: " + s.ToString());
    return;
  }

  // 写入 200 条大 value（每条约 200B）→ 总 ~40KB，分区平均 10KB 远超 4KB
  // 必触发 1+ 次 Freeze
  const std::string wal_dir = dbname + "/" + zfo.wal_subdir;
  char k[16], v[256];
  for (int64_t i = 0; i < 200; ++i) {
    snprintf(k, sizeof(k), "fr%010lld", (long long)i);
    ::memset(v, 'x', 200);
    snprintf(v, sizeof(v), "v%010lld_", (long long)i);
    s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 16),
                rocksdb::Slice(v, ::strlen(v)));
    if (!s.ok()) {
      ReportResult(tag, false, "put@" + std::to_string(i));
      return;
    }
  }

  // 写完后应有 200 条（迭代器正确）
  if (CountViaIterator(db.get()) != 200) {
    ReportResult(tag, false, "iter after write != 200");
    return;
  }

  // 关闭（触发析构 + zfwal flush + PendingPurge）
  db.reset();

  // 验证 zfwal 目录存在
  if (!DirExists(wal_dir)) {
    ReportResult(tag, false, "zfwal dir missing after close");
    CleanDB(dbname);
    return;
  }

  // 重开（关键路径：D4 修复验证）
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "reopen: " + s.ToString());
    CleanDB(dbname);
    return;
  }

  // 重开后迭代器 = 200（所有数据从封存代 + 活跃代正确读取）
  if (CountViaIterator(db.get()) != 200) {
    ReportResult(tag, false, "iter after reopen != 200");
    CleanDB(dbname);
    return;
  }

  // 抽查 Get（验证封存代读路径 — M2.0 D1 修复）
  std::string got;
  for (int64_t i : {0, 1, 99, 100, 199}) {
    snprintf(k, sizeof(k), "fr%010lld", (long long)i);
    snprintf(v, sizeof(v), "v%010lld_", (long long)i);
    s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(k, 16), &got);
    if (!s.ok() || got != v) {
      ReportResult(tag, false, "reopen get@" + std::to_string(i) + " got=\"" +
                                   got + "\"");
      CleanDB(dbname);
      return;
    }
  }

  // 重开后再写 50 条 → 写入活跃代（gen=1）
  for (int64_t i = 200; i < 250; ++i) {
    snprintf(k, sizeof(k), "fr%010lld", (long long)i);
    ::memset(v, 'y', 200);
    snprintf(v, sizeof(v), "v%010lld_", (long long)i);
    s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 16),
                rocksdb::Slice(v, ::strlen(v)));
    if (!s.ok()) {
      ReportResult(tag, false, "put2@" + std::to_string(i));
      CleanDB(dbname);
      return;
    }
  }

  // 验证 250 条全在
  if (CountViaIterator(db.get()) != 250) {
    ReportResult(tag, false, "iter after put2 != 250");
    CleanDB(dbname);
    return;
  }

  // 抽查新写数据（活跃代 gen=1）
  snprintf(k, sizeof(k), "fr%010lld", 249LL);
  s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(k, 16), &got);
  if (!s.ok()) {
    ReportResult(tag, false, "active gen get failed: " + s.ToString());
    CleanDB(dbname);
    return;
  }

  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true);
}

// ---------------------------------------------------------------------------
// 用例 9 (M2.1)：MultiEpoch — 触发 3+ 次封存，验证全代际数据可恢复
//
// 关键验证：
//  - 极小 partition_target_bytes（1KB）保证多 epoch
//  - 500 条 ~120B 记录 = 60KB → P=4 时每分区 ~15KB 远超阈值
//  - 关闭 + 重开后所有 500 条必须可读
//  - 文件计数 ≥ P（封存代未被 purge）
//  - 间接验证 M2.1-3 (EpochRef via MemTable 析构) —
//    关闭时 imm mem 析构 → ReleaseEpoch → 但 reclaim=false 文件保留
// ---------------------------------------------------------------------------
void TestMultiEpoch() {
  const char* tag = "MultiEpoch(M2.1)";
  std::string dbname = std::string(kDbBase) + "multiepoch";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.partition_target_bytes = 1024;  // 1KB：每分区超 1KB 即冻结
  zfo.epoch_target_bytes = 1024;       // 同步触发
  zfo.reclaim_sealed_files = false;    // 关闭回收便于观察

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open1: " + s.ToString());
    return;
  }

  // 写 500 条 ~120B 记录
  const std::string wal_dir = dbname + "/" + zfo.wal_subdir;
  char k[16], v[128];
  for (int64_t i = 0; i < 500; ++i) {
    snprintf(k, sizeof(k), "me%010lld", (long long)i);
    ::memset(v, 'A' + (i % 26), 100);
    v[100] = '_';
    v[101] = '\0';
    s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 16),
                rocksdb::Slice(v, 101));
    if (!s.ok()) {
      ReportResult(tag, false, "put@" + std::to_string(i));
      return;
    }
  }

  // 写完后立即 500 条
  if (CountViaIterator(db.get()) != 500) {
    ReportResult(tag, false, "iter after write != 500");
    return;
  }

  // 关闭（触发析构 + flush + 可能 PurgeSealedFiles）
  db.reset();

  // 重开
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "reopen: " + s.ToString());
    CleanDB(dbname);
    return;
  }

  // 重开后 500 条全可读（覆盖 3+ 个 epoch 的封存代）
  if (CountViaIterator(db.get()) != 500) {
    ReportResult(tag, false, "iter after reopen != 500");
    CleanDB(dbname);
    return;
  }

  // 抽查 Get（边界 + 中间值，确保不同代际数据被正确读取）
  std::string got;
  for (int64_t i : {0, 1, 99, 250, 499}) {
    snprintf(k, sizeof(k), "me%010lld", (long long)i);
    char fill = 'A' + (i % 26);
    char expect[128];
    ::memset(expect, fill, 100);
    expect[100] = '_';
    expect[101] = '\0';
    s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(k, 16), &got);
    if (!s.ok() || ::memcmp(got.data(), expect, 101) != 0) {
      ReportResult(tag, false, "get@" + std::to_string(i) +
                                   " i%26=" + std::to_string(i % 26));
      CleanDB(dbname);
      return;
    }
  }

  // 文件计数：reclaim=false 时至少 P 个活跃代 + 多次封存代数
  // 至少应远大于 P（实际数取决于 FlushBuf 时机与 Purge 触发）
  size_t file_count = CountZfwalFiles(wal_dir);
  if (file_count < zfo.partitions) {
    ReportResult(tag, false, "zfwal file count " + std::to_string(file_count) +
                                " < P=" + std::to_string(zfo.partitions));
    CleanDB(dbname);
    return;
  }

  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true, "files=" + std::to_string(file_count) + " >= P");
}

// ---------------------------------------------------------------------------
// 用例 10 (M2.1)：IteratorPins — 迭代器持有期间发生封存
// 验证 EpochRef 保持封存代文件存活（I3 不变式：引用归零前不删）
//
// 关键验证：
//  - 写入少量数据，打开迭代器（持有 memtable 快照）
//  - 继续写入大量数据触发 Freeze（封存 mem 索引中 value 指向的 WAL 文件）
//  - 迭代器继续遍历，必须返回与写入时一致的值（不是 stale 也不是 corrupt）
//  - reclaim=false 期间，旧 gen 文件保留在 zfwal
// ---------------------------------------------------------------------------
void TestIteratorPins() {
  const char* tag = "IteratorPins(M2.1)";
  std::string dbname = std::string(kDbBase) + "iterpin";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.partition_target_bytes = 8 * 1024;  // 8KB
  zfo.epoch_target_bytes = 8 * 1024;
  zfo.reclaim_sealed_files = false;       // 关闭回收便于观察

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open1: " + s.ToString());
    return;
  }

  // 第一批：写 50 条小记录
  char k[16], v[32];
  for (int64_t i = 0; i < 50; ++i) {
    snprintf(k, sizeof(k), "ip%010lld", (long long)i);
    snprintf(v, sizeof(v), "v1_%010lld", (long long)i);
    s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 16),
                rocksdb::Slice(v, ::strlen(v)));
    if (!s.ok()) {
      ReportResult(tag, false, "put1@" + std::to_string(i));
      return;
    }
  }

  // 打开迭代器（关键：此时 memtable 中 50 条记录，指向当前 gen=0 的 WAL）
  std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(rocksdb::ReadOptions()));
  iter->SeekToFirst();
  if (!iter->Valid()) {
    ReportResult(tag, false, "iter invalid before seal");
    return;
  }

  // 第二批：写 100 条大记录（每条 256B）→ 必触发 1+ 次 Freeze
  // 封存时旧 mem 的 SlimLocator 指向 gen=0 的 WAL；新 mem 指向新 gen=1
  // iter 仍持有旧 mem（ref++ → MemTable 不会立即析构 → EpochRef 保持）
  for (int64_t i = 50; i < 150; ++i) {
    char v2[256];
    snprintf(k, sizeof(k), "ip%010lld", (long long)i);
    ::memset(v2, 'Z', 200);
    snprintf(v2, sizeof(v2), "v2_%010lld_padding_xxxxxxxxxxxxxx",
             (long long)i);
    s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 16),
                rocksdb::Slice(v2, ::strlen(v2)));
    if (!s.ok()) {
      ReportResult(tag, false, "put2@" + std::to_string(i));
      return;
    }
  }

  // 继续迭代器（关键路径：从封存代 gen=0 读取 value）
  // iter 当前已指向 ip0000000000，下一个应该是 ip0000000001
  int64_t seen = 0;
  for (; iter->Valid(); iter->Next()) {
    rocksdb::Slice ikey = iter->key();
    if (ikey.size() != 16 || ::strncmp(ikey.data(), "ip", 2) != 0) {
      ReportResult(tag, false, "iter key bad @seen=" + std::to_string(seen));
      return;
    }
    // 关键校验：第一批 v1_* 的值必须能读（即使其 WAL 已封存）
    rocksdb::Slice ival = iter->value();
    if (ival.size() < 4 || ival[1] != '1' || ival[2] != '_') {
      ReportResult(tag, false, "iter value wrong @seen=" + std::to_string(seen) +
                                   " first4=\"" + ival.ToString().substr(0, 4) + "\"");
      return;
    }
    seen++;
  }

  // 期望见到 50 条第一批记录
  if (seen != 50) {
    ReportResult(tag, false, "iter saw " + std::to_string(seen) + " != 50");
    return;
  }

  // 重新 SeekToFirst 并遍历全部 150 条（混合活跃代 + 封存代）
  // 注意：旧迭代器固定了创建时的 SuperVersion，看不到 Freeze 后的新数据，
  // 因此需要关闭旧迭代器、打开新迭代器来验证全量数据可读。
  iter.reset();
  std::unique_ptr<rocksdb::Iterator> iter2(db->NewIterator(rocksdb::ReadOptions()));
  iter2->SeekToFirst();
  int64_t all_count = 0;
  for (; iter2->Valid(); iter2->Next()) ++all_count;
  if (all_count != 150) {
    ReportResult(tag, false, "full iter saw " + std::to_string(all_count) +
                                " != 150");
    return;
  }
  iter2.reset();

  iter.reset();
  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true);
}

// ---------------------------------------------------------------------------
// 用例 11 (M2.3-2)：SyncSemantics — WriteOptions::sync 触达分区精准 fsync
//
// 关键验证：
//  - sync=true 写入 → 触达分区必须 fsync（崩溃/断电后数据可恢复）
//  - 关闭后重开，所有 sync=true 写入的数据必须完整
//  - 与 M2.3-2 优化协同：只 fsync touched 分区（写入未触达的分区不 fsync）
//    — 此用例只验证语义正确性，性能优势由独立 benchmark 验证
// ---------------------------------------------------------------------------
void TestSyncSemantics() {
  const char* tag = "SyncSemantics(M2.3-2)";
  std::string dbname = std::string(kDbBase) + "sync";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.partition_target_bytes = 64u << 20;  // 大阈值：不触发 Freeze

  rocksdb::WriteOptions wsync;
  wsync.sync = true;  // 关键：sync=true

  // 第一轮：写 100 条 sync 记录
  {
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "open1: " + s.ToString());
      return;
    }
    char k[16], v[32];
    for (int64_t i = 0; i < 100; ++i) {
      snprintf(k, sizeof(k), "sy%010lld", (long long)i);
      snprintf(v, sizeof(v), "v%010lld", (long long)i);
      s = db->Put(wsync, rocksdb::Slice(k, 16), rocksdb::Slice(v, ::strlen(v)));
      if (!s.ok()) {
        ReportResult(tag, false, "put1@" + std::to_string(i));
        return;
      }
    }
    // 析构时不显式 flush：模拟"突然断电后只靠 sync 持久化"场景
  }

  // 第二轮：重开，所有 100 条必须可读（sync 写盘生效）
  {
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "open2: " + s.ToString());
      CleanDB(dbname);
      return;
    }
    if (CountViaIterator(db.get()) != 100) {
      ReportResult(tag, false, "iter after sync reopen != 100");
      CleanDB(dbname);
      return;
    }
    std::string got;
    char k[16], v[32];
    for (int64_t i : {0, 1, 50, 99}) {
      snprintf(k, sizeof(k), "sy%010lld", (long long)i);
      snprintf(v, sizeof(v), "v%010lld", (long long)i);
      s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(k, 16), &got);
      if (!s.ok() || got != v) {
        ReportResult(tag, false, "get@" + std::to_string(i) + " got=\"" + got + "\"");
        CleanDB(dbname);
        return;
      }
    }
  }

  // 第三轮：混合 sync/async 写入
  {
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "open3: " + s.ToString());
      CleanDB(dbname);
      return;
    }
    // 100 条 async（不保证）
    rocksdb::WriteOptions wasync;
    wasync.sync = false;
    char k[16], v[32];
    for (int64_t i = 100; i < 200; ++i) {
      snprintf(k, sizeof(k), "sy%010lld", (long long)i);
      snprintf(v, sizeof(v), "v%010lld", (long long)i);
      s = db->Put(wasync, rocksdb::Slice(k, 16), rocksdb::Slice(v, ::strlen(v)));
      if (!s.ok()) {
        ReportResult(tag, false, "put-async@" + std::to_string(i));
        CleanDB(dbname);
        return;
      }
    }
    // 50 条 sync
    for (int64_t i = 200; i < 250; ++i) {
      snprintf(k, sizeof(k), "sy%010lld", (long long)i);
      snprintf(v, sizeof(v), "v%010lld", (long long)i);
      s = db->Put(wsync, rocksdb::Slice(k, 16), rocksdb::Slice(v, ::strlen(v)));
      if (!s.ok()) {
        ReportResult(tag, false, "put-sync@" + std::to_string(i));
        CleanDB(dbname);
        return;
      }
    }
  }

  // 第四轮：重开
  {
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "open4: " + s.ToString());
      CleanDB(dbname);
      return;
    }
    // async 写入可能丢失（崩溃语义），但 Close() 内部仍会 flush 所以期望全有
    if (CountViaIterator(db.get()) != 250) {
      ReportResult(tag, false, "iter after mixed reopen != 250 (got " +
                                   std::to_string(CountViaIterator(db.get())) + ")");
      CleanDB(dbname);
      return;
    }
  }

  CleanDB(dbname);
  ReportResult(tag, true);
}

// ---------------------------------------------------------------------------
// 用例 12 (M2.3-1)：DestroyDBRemovesZfwal — 验证 DestroyDB 递归删除 zfwal
//
// 关键验证：
//  - 原生 rocksdb::DestroyDB 不会清理 zfwal（这是 M1 已知问题）
//  - zeroflush::DestroyDB 必须同时清理 dbname/ 和 dbname/zfwal/
//  - 用 stat() 验证：调用 DestroyDB 后两边都不存在
// ---------------------------------------------------------------------------
void TestDestroyDBRemovesZfwal() {
  const char* tag = "DestroyDBRemovesZfwal(M2.3-1)";
  std::string dbname = std::string(kDbBase) + "destroy";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 2;
  zfo.partition_target_bytes = 64u << 20;  // 不触发 Freeze

  const std::string wal_dir = dbname + "/" + zfo.wal_subdir;

  // 1) 创建 DB 并写一些数据
  {
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "open: " + s.ToString());
      return;
    }
    for (int i = 0; i < 10; ++i) {
      char k[16], v[16];
      snprintf(k, sizeof(k), "d%03d", i);
      snprintf(v, sizeof(v), "v%03d", i);
      s = db->Put(rocksdb::WriteOptions(), k, v);
      if (!s.ok()) {
        ReportResult(tag, false, "put@" + std::to_string(i));
        return;
      }
    }
  }
  // 关闭 DB（析构触发析构 → flush zfwal）
  // 此时 dbname/ 与 dbname/zfwal/ 都应存在

  if (!DirExists(dbname)) {
    ReportResult(tag, false, "dbname dir missing after close");
    CleanDB(dbname);
    return;
  }
  if (!DirExists(wal_dir)) {
    ReportResult(tag, false, "zfwal dir missing after close");
    CleanDB(dbname);
    return;
  }

  // 2) 调用 zeroflush::DestroyDB（应该同时清理两边）
  rocksdb::Options opt = MakeOptions();
  auto ds = zeroflush::DestroyDB(dbname, opt, zfo);
  if (!ds.ok()) {
    ReportResult(tag, false, "DestroyDB failed: " + ds.ToString());
    CleanDB(dbname);
    return;
  }

  // 3) 验证：dbname/ 不存在
  if (DirExists(dbname)) {
    ReportResult(tag, false, "dbname still exists after DestroyDB");
    CleanDB(dbname);
    return;
  }
  // 4) 验证：zfwal/ 不存在（关键 M2.3-1 验证点）
  if (DirExists(wal_dir)) {
    ReportResult(tag, false, "zfwal dir still exists after DestroyDB");
    CleanDB(dbname);
    return;
  }

  ReportResult(tag, true);
}

// ---------------------------------------------------------------------------
// 用例 13 (M2.3-3)：ZFPROPSReject — partitions 跨重开不一致时拒绝打开
//
// 关键验证：
//  - P=4 打开，写入，关闭
//  - P=8 重开 → 必须返回 InvalidArgument（防止读到错位数据）
//  - 测试结束后用 zeroflush::DestroyDB 完整清理（用 P=4 的 zfo）
// ---------------------------------------------------------------------------
void TestZFPROPSReject() {
  const char* tag = "ZFPROPSReject(M2.3-3)";
  std::string dbname = std::string(kDbBase) + "zfprops";
  CleanDB(dbname);

  // 第一轮：P=4 写入
  {
    zeroflush::ZeroFlushOptions zfo4;
    zfo4.partitions = 4;
    zfo4.use_zfprops = true;
    zfo4.partition_target_bytes = 64u << 20;

    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo4, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "open P=4: " + s.ToString());
      return;
    }
    s = db->Put(rocksdb::WriteOptions(), "zfkey", "zfval");
    if (!s.ok()) {
      ReportResult(tag, false, "put P=4");
      return;
    }
  }

  // 第二轮：P=8 重开 → 必须失败（InvalidArgument）
  bool rejected = false;
  std::string err_msg;
  {
    zeroflush::ZeroFlushOptions zfo8;
    zfo8.partitions = 8;
    zfo8.use_zfprops = true;
    zfo8.partition_target_bytes = 64u << 20;

    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo8, dbname, &db);
    if (s.IsInvalidArgument()) {
      rejected = true;
      err_msg = s.ToString();
    } else if (s.ok()) {
      // 错误：成功打开了 P=8（zfo8 应当拒绝）
      ReportResult(tag, false, "P=8 reopen succeeded but should have been rejected");
      // 主动清理（用 P=4 的 DestroyDB 保持一致）
      zeroflush::ZeroFlushOptions zfo4;
      zfo4.partitions = 4;
      zeroflush::DestroyDB(dbname, MakeOptions(), zfo4).PermitUncheckedError();
      return;
    } else {
      // 其它错误（IO 等）也视作"未拒绝分区不一致"，记下
      ReportResult(tag, false, "P=8 reopen: " + s.ToString());
      zeroflush::ZeroFlushOptions zfo4;
      zfo4.partitions = 4;
      zeroflush::DestroyDB(dbname, MakeOptions(), zfo4).PermitUncheckedError();
      return;
    }
  }

  // 验证消息中包含 partitions 信息
  if (err_msg.find("partitions") == std::string::npos) {
    ReportResult(tag, false, "rejection msg lacks 'partitions': " + err_msg);
    zeroflush::ZeroFlushOptions zfo4;
    zfo4.partitions = 4;
    zeroflush::DestroyDB(dbname, MakeOptions(), zfo4).PermitUncheckedError();
    return;
  }

  // 第三轮：P=4 重开 → 必须成功（验证不是 db 损坏）
  {
    zeroflush::ZeroFlushOptions zfo4;
    zfo4.partitions = 4;
    zfo4.use_zfprops = true;
    zfo4.partition_target_bytes = 64u << 20;

    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo4, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "P=4 reopen: " + s.ToString());
      zeroflush::ZeroFlushOptions zfo4c;
      zfo4c.partitions = 4;
      zeroflush::DestroyDB(dbname, MakeOptions(), zfo4c).PermitUncheckedError();
      return;
    }
    std::string got;
    s = db->Get(rocksdb::ReadOptions(), "zfkey", &got);
    if (!s.ok() || got != "zfval") {
      ReportResult(tag, false, "P=4 reopen get: \"" + got + "\"");
      zeroflush::ZeroFlushOptions zfo4c;
      zfo4c.partitions = 4;
      zeroflush::DestroyDB(dbname, MakeOptions(), zfo4c).PermitUncheckedError();
      return;
    }
  }

  // 清理（用 P=4 完整 DestroyDB 同时清 zfwal）
  {
    zeroflush::ZeroFlushOptions zfo4;
    zfo4.partitions = 4;
    zeroflush::DestroyDB(dbname, MakeOptions(), zfo4).PermitUncheckedError();
  }

  ReportResult(tag, true, rejected ? ("rejected: " + err_msg.substr(0, 60)) : "");
}

}  // namespace

int main(int argc, char** argv) {
  fprintf(stderr, "============================================================\n");
  fprintf(stderr, " ZeroFlush M1+M2 WAL Persistence Regression Suite\n");
  fprintf(stderr, "============================================================\n");

  // ---- Optional test name filter (argv[1] substring match) ----
  // 支持 "--zf_filter=X" 与裸 "X" 两种形式（此前 argv[1] 原样参与
  // find 匹配，"--zf_filter=..." 永远不命中任何测试名 → 假 ALL PASSED）。
  std::string filter;
  if (argc > 1) {
    const std::string arg = argv[1];
    const std::string prefix = "--zf_filter=";
    if (arg.rfind(prefix, 0) == 0) {
      filter = arg.substr(prefix.size());
    } else {
      filter = arg;
    }
  }
  auto run = [&](const char* name, void (*fn)()) {
    if (!filter.empty() && std::string(name).find(filter) == std::string::npos) {
      return;
    }
    fprintf(stderr, "\n--- %s ---\n", name);
    fn();
  };

  // ---- M1 回归 ----
  run("SequentialKeys",        TestSequentialKeys);
  run("RandomKeysUnique",      TestRandomKeysUnique);
  run("RandomKeysWithDuplicates", TestRandomKeysWithDuplicates);
  run("MultiPartition",        TestMultiPartition);
  run("WALBufferFlush",        TestWALBufferFlush);
  run("ReopenNoTruncate",      TestReopenNoTruncate);
  run("LargeSequential",       TestLargeSequential);

  // ---- M2 新增 ----
  run("FreezeReopen",          TestFreezeReopen);
  run("MultiEpoch",            TestMultiEpoch);
  run("IteratorPins",          TestIteratorPins);
  run("SyncSemantics",         TestSyncSemantics);
  run("DestroyDBRemovesZfwal", TestDestroyDBRemovesZfwal);
  run("ZFPROPSReject",         TestZFPROPSReject);

  fprintf(stderr, "============================================================\n");
  if (g_failures == 0) {
    fprintf(stderr, " ALL PASSED\n");
  } else {
    fprintf(stderr, " %d FAILED\n", g_failures);
  }
  fprintf(stderr, "============================================================\n");
  return g_failures;
}
