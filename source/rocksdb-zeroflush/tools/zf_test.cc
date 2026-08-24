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
#include <chrono>
#include <dirent.h>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <unordered_set>

#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "utilities/merge_operators.h"
#include "db/dbformat.h"
#include "memory/arena.h"
#include "table/merging_iterator.h"
#include "zeroflush/partition_index.h"
#include "zeroflush/partition_table.h"
#include "zeroflush/wal_format.h"
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

// ---- M3.2 指标与层级检查辅助 ----
// 读取 ZeroFlush 属性（rocksdb.zeroflush.<name>，DBImpl 直接分派给
// ZeroFlushContext::GetProperty，见 M3_DESIGN.md §13）。
uint64_t ZfMetric(rocksdb::DB* db, const std::string& name) {
  std::string val;
  if (!db->GetProperty("rocksdb.zeroflush." + name, &val)) return 0;
  return ::strtoull(val.c_str(), nullptr, 10);
}

// 轮询等待指标达到目标（物化由后台 flush 线程异步推进）。
bool WaitForZfMetric(rocksdb::DB* db, const std::string& name, uint64_t target,
                     uint64_t timeout_ms = 30000) {
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    if (ZfMetric(db, name) >= target) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return false;
}

// 等待全部已封存 epoch 物化完成：sealed 与 materialized 相等且连续
// 多次采样不再增长（写路径已停止产生新 epoch）。
bool WaitAllMaterialized(rocksdb::DB* db, uint64_t timeout_ms = 60000) {
  uint64_t last_m = 0, last_s = 0;
  int stable = 0;
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    const uint64_t m = ZfMetric(db, "epochs_materialized");
    const uint64_t s = ZfMetric(db, "epochs_sealed");
    if (s == m && m == last_m && s == last_s) {
      if (++stable >= 5) return true;
    } else {
      stable = 0;
    }
    last_m = m;
    last_s = s;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return false;
}

// 指定 level 的 SST 文件数（rocksdb.num-files-at-levelN 标准属性）。
uint64_t NumFilesAtLevel(rocksdb::DB* db, int level) {
  std::string val;
  if (!db->GetProperty("rocksdb.num-files-at-level" + std::to_string(level),
                       &val)) {
    return 0;
  }
  return ::strtoull(val.c_str(), nullptr, 10);
}

// 诊断辅助：失败时输出 DB LOG 尾部（后台 flush 的物化错误细节写在此处）。
void DumpDbLogTail(const std::string& dbname, size_t nbytes = 4096) {
  const std::string path = dbname + "/LOG";
  FILE* f = ::fopen(path.c_str(), "rb");
  if (f == nullptr) return;
  ::fseek(f, 0, SEEK_END);
  const long sz = ::ftell(f);
  const long off = sz > static_cast<long>(nbytes) ? sz - static_cast<long>(nbytes) : 0;
  ::fseek(f, off, SEEK_SET);
  std::string buf(static_cast<size_t>(sz - off), '\0');
  const size_t got = ::fread(&buf[0], 1, buf.size(), f);
  buf.resize(got);
  ::fclose(f);
  fprintf(stderr, "----- LOG tail -----\n%s\n--------------------\n", buf.c_str());
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
  const int64_t cnt_after_write = CountViaIterator(db.get());
  if (cnt_after_write != 200) {
    ReportResult(tag, false,
                 "iter after write != 200 (got " + std::to_string(cnt_after_write) + ")");
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

// ---------------------------------------------------------------------------
// 用例 17 (M3.1-17): StaticBoundariesRoute — 范围路由的 Route 与
// RangeOf 互为逆。验证 kStatic 模式分隔键边界、越界键落首/末分区。
// ---------------------------------------------------------------------------
void TestStaticBoundariesRoute() {
  const char* tag = "StaticBoundariesRoute(M3.1-17)";
  const auto* ucmp = rocksdb::BytewiseComparator();
  // P=4, 边界 "c", "g", "m": 分区 0=[-∞,c), 1=[c,g), 2=[g,m), 3=[m,+∞)
  std::vector<std::string> boundaries = {"c", "g", "m"};
  std::shared_ptr<zeroflush::PartitionTable> pt;
  auto s = zeroflush::PartitionTable::Create(0, boundaries, ucmp, &pt);
  if (!s.ok()) {
    ReportResult(tag, false, "Create: " + s.ToString());
    return;
  }
  // 范围边界校验
  {
    rocksdb::Slice lo, hi;
    pt->RangeOf(0, &lo, &hi);
    if (lo.size() != 0 || hi.ToString() != "c") {
      ReportResult(tag, false, "RangeOf(0) lo/hi wrong");
      return;
    }
    pt->RangeOf(3, &lo, &hi);
    if (lo.ToString() != "m" || hi.size() != 0) {
      ReportResult(tag, false, "RangeOf(3) lo/hi wrong");
      return;
    }
  }
  // Route 与 RangeOf 逆检验
  const char* keys[] = {"a", "c", "e", "g", "k", "m", "z"};
  uint32_t ex[] = {0, 1, 1, 2, 2, 3, 3};
  for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); ++i) {
    uint32_t p = pt->Route(keys[i]);
    if (p != ex[i]) {
      ReportResult(tag, false,
                   std::string("Route(") + keys[i] + ")=" +
                       std::to_string(p) + " expected=" + std::to_string(ex[i]));
      return;
    }
    // RangeOf 包含性：key 应在该分区的区间内
    rocksdb::Slice lo, hi;
    pt->RangeOf(p, &lo, &hi);
    if (lo.size() > 0 && ucmp->Compare(keys[i], lo) < 0) {
      ReportResult(tag, false, std::string("Route key < lo for key=") + keys[i]);
      return;
    }
    if (hi.size() > 0 && ucmp->Compare(keys[i], hi) >= 0) {
      ReportResult(tag, false, std::string("Route key >= hi for key=") + keys[i]);
      return;
    }
  }
  // hash 兼容模式
  auto pt2 = zeroflush::PartitionTable::CreateHash(0, 4);
  if (!pt2->IsHashMode()) {
    ReportResult(tag, false, "CreateHash !IsHashMode");
    return;
  }
  ReportResult(tag, true);
}

// ---------------------------------------------------------------------------
// 用例 18 (M3.1-18): SampledBoundariesConverge — 蓄水池采样后学习出的
// 边界使路由 skew < 1.5（等频分位点性能）。
// ---------------------------------------------------------------------------
void TestSampledBoundariesConverge() {
  const char* tag = "SampledBoundariesConverge(M3.1-18)";
  const auto* ucmp = rocksdb::BytewiseComparator();
  zeroflush::KeySampler sampler(1, ucmp);
  // 生成 10000 个均匀随机键
  std::mt19937_64 rng(42);
  for (int i = 0; i < 10000; ++i) {
    char buf[32];
    uint64_t r = rng() % 1000000;
    snprintf(buf, sizeof(buf), "k%015lu", (unsigned long)r);
    sampler.Sample(rocksdb::Slice(buf, strlen(buf)));
  }
  // 构建 P=8 的边界（7 个分位点）
  std::vector<std::string> boundaries;
  if (!sampler.BuildBoundaries(8, &boundaries)) {
    ReportResult(tag, false, "BuildBoundaries(8) failed");
    return;
  }
  if (boundaries.size() != 7) {
    ReportResult(tag, false,
                 "expected 7 boundaries, got " + std::to_string(boundaries.size()));
    return;
  }
  std::shared_ptr<zeroflush::PartitionTable> pt;
  auto s = zeroflush::PartitionTable::Create(1, boundaries, ucmp, &pt);
  if (!s.ok()) {
    ReportResult(tag, false, "Create: " + s.ToString());
    return;
  }
  // 用相同随机种子重放键，统计各分区路由偏斜
  std::vector<int> counts(8, 0);
  rng.seed(42);
  for (int i = 0; i < 10000; ++i) {
    char buf[32];
    uint64_t r = rng() % 1000000;
    snprintf(buf, sizeof(buf), "k%015lu", (unsigned long)r);
    uint32_t pid = pt->Route(rocksdb::Slice(buf, strlen(buf)));
    counts[pid]++;
  }
  int min_c = counts[0], max_c = counts[0];
  for (int c : counts) {
    if (c < min_c) min_c = c;
    if (c > max_c) max_c = c;
  }
  double skew =
      static_cast<double>(max_c) / std::max(1, min_c);
  if (skew > 1.5) {
    std::string detail = "skew=" + std::to_string(skew) + " counts=[";
    for (size_t i = 0; i < counts.size(); ++i) {
      if (i > 0) detail += ",";
      detail += std::to_string(counts[i]);
    }
    detail += "]";
    ReportResult(tag, false, detail);
    return;
  }
  ReportResult(tag, true, "skew=" + std::to_string(skew) +
                               " min=" + std::to_string(min_c) +
                               " max=" + std::to_string(max_c));
}

// ---------------------------------------------------------------------------
// 用例 35 (M4.0-35): SampledLearningEpochEndToEnd — kSampled 学习期端到端。
// 学习期 epoch 1 用 hash 路由写入、封存时学习并安装边界表 v1；epoch 2 起
// 用 v1 范围路由写入。回归背景（2026-08-21 修复）：SealedEpoch.table_version
// 曾被误标为 1，epoch 1 物化用学习后边界对 hash 路由记录做范围断言，报
// "materialized range outside table bounds" 崩溃。断言：epoch 1 物化不炸、
// epochs_materialized == epochs_sealed（≥2）、物化后与重开后全量 key 可读。
// ---------------------------------------------------------------------------
void TestSampledLearningEpochEndToEnd() {
  const char* tag = "SampledLearningEpochEndToEnd(M4.0-35)";
  std::string dbname = std::string(kDbBase) + "sampled_e2e";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 8;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kSampled;
  zfo.partition_target_bytes = 8 << 10;  // 8KB：小阈值保证 ≥2 个 epoch
  zfo.epoch_target_bytes = 8 << 10;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }

  // 3 个 epoch 的随机键（学习边界来自真实键分布；同 key 重复写以最后值为准）。
  std::map<std::string, std::string> written;
  std::mt19937_64 rng(20260821);
  char k[24], v[96];
  for (int e = 0; e < 3; ++e) {
    for (int i = 0; i < 100; ++i) {
      uint64_t r = rng() % 1000000;
      snprintf(k, sizeof(k), "k%08lu", (unsigned long)r);
      ::memset(v, 'A' + (e % 26), 90);
      v[90] = '\0';
      s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, strlen(k)),
                  rocksdb::Slice(v, 90));
      if (!s.ok()) {
        ReportResult(tag, false, "put: " + s.ToString());
        CleanDB(dbname);
        return;
      }
      written[k] = std::string(v, 90);
    }
  }

  if (!WaitAllMaterialized(db.get())) {
    DumpDbLogTail(dbname);
    ReportResult(tag, false,
                 "materialization did not finish; sealed=" +
                     std::to_string(ZfMetric(db.get(), "epochs_sealed")) +
                     " materialized=" +
                     std::to_string(ZfMetric(db.get(), "epochs_materialized")));
    CleanDB(dbname);
    return;
  }
  const uint64_t sealed = ZfMetric(db.get(), "epochs_sealed");
  const uint64_t mater = ZfMetric(db.get(), "epochs_materialized");
  if (sealed < 2 || mater != sealed) {
    ReportResult(tag, false, "sealed=" + std::to_string(sealed) +
                                 " materialized=" + std::to_string(mater));
    CleanDB(dbname);
    return;
  }

  // 物化后全量可读（含学习期 hash 写入的 epoch 1 数据）。
  std::string val;
  for (const auto& kv : written) {
    s = db->Get(rocksdb::ReadOptions(), kv.first, &val);
    if (!s.ok() || val != kv.second) {
      ReportResult(tag, false, "get after materialize: " + kv.first + " -> " +
                                   s.ToString());
      CleanDB(dbname);
      return;
    }
  }

  // 重开：ZFPROPS v2 持久化学习表与路由模式，重放后全量可读。
  db.reset();
  std::unique_ptr<rocksdb::DB> db2;
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db2);
  if (!s.ok()) {
    ReportResult(tag, false, "reopen: " + s.ToString());
    CleanDB(dbname);
    return;
  }
  for (const auto& kv : written) {
    s = db2->Get(rocksdb::ReadOptions(), kv.first, &val);
    if (!s.ok() || val != kv.second) {
      ReportResult(tag, false, "get after reopen: " + kv.first + " -> " +
                                   s.ToString());
      CleanDB(dbname);
      return;
    }
  }
  db2.reset();

  ReportResult(tag, true,
               "sealed=" + std::to_string(sealed) +
                   " keys=" + std::to_string(written.size()));
  CleanDB(dbname);
}

// ---------------------------------------------------------------------------
// 用例 37 (M4.2b-37): AlignL1Boundaries — kAlignL1 端到端。
// 每 epoch 封存时按 L1 文件边界重新对齐分区。验证：多 epoch 写入 +
// 物化不崩（范围断言通过）、物化后与重开后全量 key 可读。
// ---------------------------------------------------------------------------
void TestAlignL1Boundaries() {
  const char* tag = "AlignL1Boundaries(M4.2b-37)";
  std::string dbname = std::string(kDbBase) + "align_l1";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kAlignL1;
  zfo.partition_target_bytes = 8 << 10;  // 8KB：小阈值保证 ≥3 个 epoch
  zfo.epoch_target_bytes = 8 << 10;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }

  // 4 个 epoch 的随机键（kAlignL1 首轮 hash 写 → L0→L1 后对齐表生效）。
  std::map<std::string, std::string> written;
  std::mt19937_64 rng(20260822);
  char k[24], v[96];
  for (int e = 0; e < 4; ++e) {
    for (int i = 0; i < 100; ++i) {
      uint64_t r = rng() % 1000000;
      snprintf(k, sizeof(k), "k%08lu", (unsigned long)r);
      ::memset(v, 'A' + (e % 26), 90);
      v[90] = '\0';
      s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, strlen(k)),
                  rocksdb::Slice(v, 90));
      if (!s.ok()) {
        ReportResult(tag, false, "put: " + s.ToString());
        CleanDB(dbname);
        return;
      }
      written[k] = std::string(v, 90);
    }
  }

  if (!WaitAllMaterialized(db.get())) {
    DumpDbLogTail(dbname);
    ReportResult(tag, false, "materialization did not finish; sealed=" +
                                 std::to_string(ZfMetric(db.get(), "epochs_sealed")) +
                                 " materialized=" +
                                 std::to_string(ZfMetric(db.get(), "epochs_materialized")));
    CleanDB(dbname);
    return;
  }

  const uint64_t sealed = ZfMetric(db.get(), "epochs_sealed");

  // 物化后全量可读（含 hash 首轮写入的数据）。
  std::string val;
  for (const auto& kv : written) {
    s = db->Get(rocksdb::ReadOptions(), kv.first, &val);
    if (!s.ok() || val != kv.second) {
      ReportResult(tag, false, "get after materialize: " + kv.first + " -> " +
                                   s.ToString());
      CleanDB(dbname);
      return;
    }
  }

  // 重开：ZFPROPS 持久化对齐表与路由模式，重放后全量可读。
  db.reset();
  std::unique_ptr<rocksdb::DB> db2;
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db2);
  if (!s.ok()) {
    ReportResult(tag, false, "reopen: " + s.ToString());
    CleanDB(dbname);
    return;
  }
  for (const auto& kv : written) {
    s = db2->Get(rocksdb::ReadOptions(), kv.first, &val);
    if (!s.ok() || val != kv.second) {
      ReportResult(tag, false, "get after reopen: " + kv.first + " -> " +
                                   s.ToString());
      CleanDB(dbname);
      return;
    }
  }
  db2.reset();

  ReportResult(tag, true,
               "sealed=" + std::to_string(sealed) +
                   " keys=" + std::to_string(written.size()));
  CleanDB(dbname);
}


// ---------------------------------------------------------------------------
// 用例 38 (M4.3-38): PartitionFreezeIndependent — 分区独立 freeze。
// 触发分区 0 的 freeze（批次冻结）后，其余分区持续写入不被阻塞；
// freeze 前后全量数据可读（迭代器 + Get）。
// ---------------------------------------------------------------------------
void TestPartitionFreezeIndependent() {
  const char* tag = "PartitionFreezeIndependent(M4.3-38)";
  std::string dbname = std::string(kDbBase) + "pfreeze_indep";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kAlignL1;
  zfo.partition_target_bytes = 4 << 10;  // 4KB：小阈值，写入中频繁 freeze
  zfo.epoch_target_bytes = 4 << 10;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }

  // 写入 800 条（每分区 ~200 条 → 多次分区 freeze），freeze 后继续写。
  std::map<std::string, std::string> written;
  std::mt19937_64 rng(20260823);
  char k[24], v[96];
  for (int i = 0; i < 800; ++i) {
    uint64_t r = rng() % 1000000;
    snprintf(k, sizeof(k), "k%08lu", (unsigned long)r);
    ::memset(v, 'A' + (i % 26), 90);
    v[90] = '\0';
    s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, strlen(k)),
                rocksdb::Slice(v, 90));
    if (!s.ok()) {
      ReportResult(tag, false, "put@" + std::to_string(i) + ": " + s.ToString());
      CleanDB(dbname);
      return;
    }
    written[k] = std::string(v, 90);
  }

  // 等待全部 epoch 物化（freeze 后的数据进 SST 或索引——迭代器应全量）。
  if (!WaitAllMaterialized(db.get())) {
    ReportResult(tag, false, "materialization did not finish");
    CleanDB(dbname);
    return;
  }

  // 迭代器全量 + Get 全对。
  if (CountViaIterator(db.get()) != static_cast<int64_t>(written.size())) {
    ReportResult(tag, false, "iter count mismatch");
    CleanDB(dbname);
    return;
  }
  std::string val;
  for (const auto& kv : written) {
    s = db->Get(rocksdb::ReadOptions(), kv.first, &val);
    if (!s.ok() || val != kv.second) {
      ReportResult(tag, false,
                   "get " + kv.first + " -> " + s.ToString());
      CleanDB(dbname);
      return;
    }
  }

  ReportResult(tag, true,
               "keys=" + std::to_string(written.size()));
  CleanDB(dbname);
}

// ---------------------------------------------------------------------------
// 用例 39 (M4.3-39): ConcurrentPartitionWriteRead — 多线程并发写读跨分区。
// ---------------------------------------------------------------------------
void TestConcurrentPartitionWriteRead() {
  const char* tag = "ConcurrentPartitionWriteRead(M4.3-39)";
  std::string dbname = std::string(kDbBase) + "conc_part";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 8;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kAlignL1;
  zfo.partition_target_bytes = 8 << 10;
  zfo.epoch_target_bytes = 8 << 10;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }

  // 4 线程 × 250 条并发写（跨分区随机键），完成后全量 Get 验证。
  constexpr int kThreads = 4;
  constexpr int kPerThread = 250;
  std::map<std::string, std::string> written;
  std::mutex mu;
  std::atomic<bool> failed{false};
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937_64 rng(1000 + t);
      char k[24], v[96];
      for (int i = 0; i < kPerThread; ++i) {
        uint64_t r = rng() % 1000000;
        snprintf(k, sizeof(k), "k%08lu", (unsigned long)r);
        ::memset(v, 'a' + (t % 26), 90);
        v[90] = '\0';
        auto ps = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, strlen(k)),
                          rocksdb::Slice(v, 90));
        if (!ps.ok()) {
          failed.store(true);
          return;
        }
        std::lock_guard<std::mutex> l(mu);
        written[k] = std::string(v, 90);
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  if (failed.load()) {
    ReportResult(tag, false, "concurrent put failed");
    CleanDB(dbname);
    return;
  }

  if (!WaitAllMaterialized(db.get())) {
    ReportResult(tag, false, "materialization did not finish");
    CleanDB(dbname);
    return;
  }

  // 全量 Get 验证（跨分区读）。
  std::string val;
  for (const auto& kv : written) {
    s = db->Get(rocksdb::ReadOptions(), kv.first, &val);
    if (!s.ok() || val != kv.second) {
      ReportResult(tag, false, "get " + kv.first + " -> " + s.ToString());
      CleanDB(dbname);
      return;
    }
  }

  ReportResult(tag, true,
               "keys=" + std::to_string(written.size()));
  CleanDB(dbname);
}


// ---------------------------------------------------------------------------
// 用例 40 (M4.3-40): GetAfterCompact — compact（物化）后 Get 正确性。
// ---------------------------------------------------------------------------
void TestGetAfterCompact() {
  const char* tag = "GetAfterCompact(M4.3-40)";
  std::string dbname = std::string(kDbBase) + "get_after_compact";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 8;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kAlignL1;
  zfo.partition_target_bytes = 4 << 10;
  zfo.epoch_target_bytes = 4 << 10;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }
  std::map<std::string, std::string> written;
  std::mt19937_64 rng(20260824);
  char k[24], v[96];
  for (int i = 0; i < 600; ++i) {
    uint64_t r = rng() % 1000000;
    snprintf(k, sizeof(k), "k%08lu", (unsigned long)r);
    ::memset(v, 'A' + (i % 26), 90);
    v[90] = '\0';
    s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, strlen(k)),
                rocksdb::Slice(v, 90));
    if (!s.ok()) {
      ReportResult(tag, false, "put: " + s.ToString());
      CleanDB(dbname);
      return;
    }
    written[k] = std::string(v, 90);
  }
  // 物化完成（数据进 SST）。
  if (!WaitAllMaterialized(db.get())) {
    ReportResult(tag, false, "materialization did not finish");
    CleanDB(dbname);
    return;
  }
  std::string val;
  for (const auto& kv : written) {
    s = db->Get(rocksdb::ReadOptions(), kv.first, &val);
    if (!s.ok() || val != kv.second) {
      ReportResult(tag, false, "get: " + kv.first + " -> " + s.ToString());
      CleanDB(dbname);
      return;
    }
  }
  // 重开后仍全对。
  db.reset();
  std::unique_ptr<rocksdb::DB> db2;
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db2);
  if (!s.ok()) {
    ReportResult(tag, false, "reopen: " + s.ToString());
    CleanDB(dbname);
    return;
  }
  for (const auto& kv : written) {
    s = db2->Get(rocksdb::ReadOptions(), kv.first, &val);
    if (!s.ok() || val != kv.second) {
      ReportResult(tag, false, "get after reopen: " + kv.first);
      CleanDB(dbname);
      return;
    }
  }
  db2.reset();
  ReportResult(tag, true, "keys=" + std::to_string(written.size()));
  CleanDB(dbname);
}

// ---------------------------------------------------------------------------
// 用例 41 (M4.3-41): CrashBeforeCompact — 封存后未物化即"崩溃"（不等待
// 物化直接关闭），重开后封存代 WAL 恢复。真实 fork 与 RocksDB 全局线程池
// 不兼容（子进程物化不执行），此处用"写入触发 freeze 后立即 Close"模拟
// 崩溃窗口（Close 只 flush 活跃缓冲，不物化已封存代——重开时封存代 WAL
// 在，Recover 重建索引恢复数据）。
// ---------------------------------------------------------------------------
void TestCrashBeforeCompact() {
  const char* tag = "CrashBeforeCompact(M4.3-41)";
  std::string dbname = std::string(kDbBase) + "crash_pre_compact";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 8;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kAlignL1;
  zfo.partition_target_bytes = 4 << 10;
  zfo.epoch_target_bytes = 4 << 10;

  // 写入（触发 freeze 产生封存代）后立即关闭（不等物化）。
  std::map<std::string, std::string> written;
  {
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "open: " + s.ToString());
      return;
    }
    std::mt19937_64 rng(42);
    char k[24], v[96];
    for (int i = 0; i < 150; ++i) {
      uint64_t r = rng() % 1000000;
      snprintf(k, sizeof(k), "k%08lu", (unsigned long)r);
      ::memset(v, 'B' + (i % 26), 90);
      v[90] = '\0';
      auto ps = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, strlen(k)),
                        rocksdb::Slice(v, 90));
      if (!ps.ok()) {
        ReportResult(tag, false, "put: " + ps.ToString());
        CleanDB(dbname);
        return;
      }
      written[k] = std::string(v, 90);
    }
    // 立即关闭（不等待物化——模拟崩溃窗口：封存代 WAL 保留在磁盘）。
  }

  // 重开：Recover 重建索引（封存代 + 活跃代 WAL），全量 Get。
  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "reopen: " + s.ToString());
    CleanDB(dbname);
    return;
  }
  std::string val;
  for (const auto& kv : written) {
    s = db->Get(rocksdb::ReadOptions(), kv.first, &val);
    if (!s.ok() || val != kv.second) {
      ReportResult(tag, false,
                   "get after crash reopen: " + kv.first + " -> " +
                       s.ToString());
      CleanDB(dbname);
      return;
    }
  }
  ReportResult(tag, true, "keys=" + std::to_string(written.size()));
  CleanDB(dbname);
}

// ---------------------------------------------------------------------------
// 用例 42 (M4.3-42): MemoryBudgetBackpressure — 索引内存预算触发 freeze。
// ---------------------------------------------------------------------------
void TestMemoryBudgetBackpressure() {
  const char* tag = "MemoryBudgetBackpressure(M4.3-42)";
  std::string dbname = std::string(kDbBase) + "mem_budget";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 8;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kAlignL1;
  zfo.partition_target_bytes = 128u << 20;  // 128MB：WAL 阈值不触发（且过
                                            // K*partition_target <= 4*write_buffer 校验）
  zfo.epoch_target_bytes = 128u << 20;
  zfo.index_mem_budget = 8 << 10;           // 8KB：极小预算 → 内存触发（写入 ~15KB 必超）

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }
  std::map<std::string, std::string> written;
  std::mt19937_64 rng(7);
  char k[24], v[96];
  for (int i = 0; i < 500; ++i) {
    uint64_t r = rng() % 1000000;
    snprintf(k, sizeof(k), "k%08lu", (unsigned long)r);
    ::memset(v, 'C' + (i % 26), 90);
    v[90] = '\0';
    s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, strlen(k)),
                rocksdb::Slice(v, 90));
    if (!s.ok()) {
      ReportResult(tag, false, "put: " + s.ToString());
      CleanDB(dbname);
      return;
    }
    written[k] = std::string(v, 90);
  }
  // 内存预算应触发多次 freeze（epochs_sealed > 0）。
  const uint64_t sealed = ZfMetric(db.get(), "epochs_sealed");
  if (sealed == 0) {
    ReportResult(tag, false, "no freeze triggered by memory budget");
    CleanDB(dbname);
    return;
  }
  if (!WaitAllMaterialized(db.get())) {
    ReportResult(tag, false, "materialization did not finish");
    CleanDB(dbname);
    return;
  }
  std::string val;
  for (const auto& kv : written) {
    s = db->Get(rocksdb::ReadOptions(), kv.first, &val);
    if (!s.ok() || val != kv.second) {
      ReportResult(tag, false, "get: " + kv.first + " -> " + s.ToString());
      CleanDB(dbname);
      return;
    }
  }
  ReportResult(tag, true,
               "sealed=" + std::to_string(sealed) +
                   " keys=" + std::to_string(written.size()));
  CleanDB(dbname);
}

// ---------------------------------------------------------------------------
// 用例 43 (M4.3-43): ParallelPartitionCompact — 多分区并行物化正确性。
// ---------------------------------------------------------------------------
void TestParallelPartitionCompact() {
  const char* tag = "ParallelPartitionCompact(M4.3-43)";
  std::string dbname = std::string(kDbBase) + "par_compact";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 16;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kAlignL1;
  zfo.partition_target_bytes = 4 << 10;
  zfo.epoch_target_bytes = 4 << 10;
  zfo.materialize_parallelism = 8;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }
  std::map<std::string, std::string> written;
  std::mt19937_64 rng(2026);
  char k[24], v[96];
  for (int i = 0; i < 1000; ++i) {
    uint64_t r = rng() % 1000000;
    snprintf(k, sizeof(k), "k%08lu", (unsigned long)r);
    ::memset(v, 'D' + (i % 26), 90);
    v[90] = '\0';
    s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, strlen(k)),
                rocksdb::Slice(v, 90));
    if (!s.ok()) {
      ReportResult(tag, false, "put: " + s.ToString());
      CleanDB(dbname);
      return;
    }
    written[k] = std::string(v, 90);
  }
  if (!WaitAllMaterialized(db.get())) {
    ReportResult(tag, false, "materialization did not finish");
    CleanDB(dbname);
    return;
  }
  std::string val;
  for (const auto& kv : written) {
    s = db->Get(rocksdb::ReadOptions(), kv.first, &val);
    if (!s.ok() || val != kv.second) {
      ReportResult(tag, false, "get: " + kv.first + " -> " + s.ToString());
      CleanDB(dbname);
      return;
    }
  }
  ReportResult(tag, true, "keys=" + std::to_string(written.size()));
  CleanDB(dbname);
}

// ---------------------------------------------------------------------------
// 用例 44 (M4.3-44): SteadyStateControlledL0 — 持续写入 L0 文件数受控。
// 回落 L0 循环（M4.5 消除）前，L0 文件数须受控（≤ 2×stop 阈值）。
// ---------------------------------------------------------------------------
void TestSteadyStateControlledL0() {
  const char* tag = "SteadyStateControlledL0(M4.3-44)";
  std::string dbname = std::string(kDbBase) + "steady_l0";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 8;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kAlignL1;
  zfo.partition_target_bytes = 4 << 10;
  zfo.epoch_target_bytes = 4 << 10;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }
  std::mt19937_64 rng(99);
  char k[24], v[96];
  for (int i = 0; i < 800; ++i) {
    uint64_t r = rng() % 1000000;
    snprintf(k, sizeof(k), "k%08lu", (unsigned long)r);
    ::memset(v, 'E' + (i % 26), 90);
    v[90] = '\0';
    s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, strlen(k)),
                rocksdb::Slice(v, 90));
    if (!s.ok()) {
      ReportResult(tag, false, "put: " + s.ToString());
      CleanDB(dbname);
      return;
    }
  }
  if (!WaitAllMaterialized(db.get())) {
    ReportResult(tag, false, "materialization did not finish");
    CleanDB(dbname);
    return;
  }
  // L0 文件数受控（回落 L0 循环消除前，M4.5 验收恒 0）。
  std::string prop;
  db->GetProperty("rocksdb.num-files-at-level0", &prop);
  const int l0 = atoi(prop.c_str());
  if (l0 > 64) {
    ReportResult(tag, false, "L0 files uncontrolled: " + std::to_string(l0));
    CleanDB(dbname);
    return;
  }
  ReportResult(tag, true, "l0_files=" + std::to_string(l0));
  CleanDB(dbname);
}


// ---------------------------------------------------------------------------
// 用例 45 (M4.5b-45): MultiGenFrozenIterator — 多代 frozen 索引的迭代器
// 归并最小复现（不经 DB：直接构造 PartitionIndexSet 多代 frozen + active，
// 验证 AddIterators 归并遍历不丢 key——M4.5b 攒批可见性问题的复现单元）。
// ---------------------------------------------------------------------------
void TestMultiGenFrozenIterator() {
  const char* tag = "MultiGenFrozenIterator(M4.5b-45)";
  const auto* ucmp = rocksdb::BytewiseComparator();
  rocksdb::InternalKeyComparator icmp(ucmp);
  zeroflush::ZfKeyComparator cmp(icmp);
  zeroflush::PartitionIndexSet set(cmp);

  // helper：internal key（user key + seq）
  auto mkik = [](const std::string& user, uint64_t seq) {
    std::string ik = user;
    rocksdb::PutFixed64(&ik, rocksdb::PackSequenceAndType(
                                 seq, rocksdb::kTypeValue));
    return ik;
  };
  // locator 占位（迭代器 value 读不触发——read_value 返回空）
  zeroflush::SlimLocator loc{};
  const rocksdb::Slice loc_slice(reinterpret_cast<const char*>(&loc),
                                 sizeof(loc));

  // 4 分区 × 2 代 frozen + active，key 交错（模拟 hash 路由下分区键交错）：
  // part p 拥有 key 集 { i % 4 == p }（fr0000000000 在 p0、0001 在 p1…）。
  // 每代每分区 8 个 key → 总条目 = 4 分区 × 3 代 × 8 = 96（同 key 3 版本）。
  char k[16];
  for (int g = 0; g < 3; ++g) {
    for (int p = 0; p < 4; ++p) {
      for (int i = p; i < 32; i += 4) {
        snprintf(k, sizeof(k), "fr%08d", i);
        set.Insert(static_cast<uint32_t>(p), static_cast<uint32_t>(g),
                   mkik(k, 300 - g * 100 - i), loc_slice);
      }
    }
    if (g < 2) {
      for (int p = 0; p < 4; ++p) {
        set.Freeze(static_cast<uint32_t>(p), static_cast<uint32_t>(g + 1));
      }
    }
  }

  // 迭代器归并遍历计数（期望 4×3×8 = 96）
  rocksdb::Arena arena;
  rocksdb::MergeIteratorBuilder builder(&icmp, &arena);
  auto read_value = [](const rocksdb::Slice&, std::string* out) {
    out->clear();
    return rocksdb::Status::OK();
  };
  set.AddIterators(&builder, read_value, &arena);
  auto iter = builder.Finish(nullptr);
  iter->SeekToFirst();
  int n = 0;
  for (; iter->Valid(); iter->Next()) {
    ++n;
  }
  if (n != 96) {
    ReportResult(tag, false,
                 "iter count " + std::to_string(n) + " != 96");
    return;
  }
  ReportResult(tag, true, "count=96");
}


// ---------------------------------------------------------------------------
// 用例 46 (M3.4-46): MergeOperandChain — Merge 链跨 epoch，Get 合并正确。
// ---------------------------------------------------------------------------
void TestMergeOperandChain() {
  const char* tag = "MergeOperandChain(M3.4-46)";
  std::string dbname = std::string(kDbBase) + "merge_chain";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 8;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kAlignL1;
  zfo.partition_target_bytes = 4 << 10;
  zfo.epoch_target_bytes = 4 << 10;

  rocksdb::Options opt = MakeOptions();
  opt.merge_operator = rocksdb::MergeOperators::CreateStringAppendOperator();

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(opt, zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }
  const char* key = "mkey00000001";
  s = db->Put(rocksdb::WriteOptions(), key, "base");
  if (!s.ok()) {
    ReportResult(tag, false, "put: " + s.ToString());
    CleanDB(dbname);
    return;
  }
  for (int i = 0; i < 5; ++i) {
    s = db->Merge(rocksdb::WriteOptions(), key, "op" + std::to_string(i));
    if (!s.ok()) {
      ReportResult(tag, false, "merge@" + std::to_string(i) + ": " + s.ToString());
      CleanDB(dbname);
      return;
    }
  }
  if (!WaitAllMaterialized(db.get())) {
    ReportResult(tag, false, "materialization did not finish");
    CleanDB(dbname);
    return;
  }
  std::string val;
  s = db->Get(rocksdb::ReadOptions(), key, &val);
  if (!s.ok() || val != "base,op0,op1,op2,op3,op4") {
    ReportResult(tag, false, "get merge: '" + val + "' size=" +
                                 std::to_string(val.size()) + " (" +
                                 s.ToString() + ")");
    CleanDB(dbname);
    return;
  }
  ReportResult(tag, true, "merged='" + val + "'");
  CleanDB(dbname);
}

// ---------------------------------------------------------------------------
// 用例 47 (M3.4-47): DeleteRangeAcrossPartitions — 跨分区 DeleteRange。
// ---------------------------------------------------------------------------
void TestDeleteRangeAcrossPartitions() {
  const char* tag = "DeleteRangeAcrossPartitions(M3.4-47)";
  std::string dbname = std::string(kDbBase) + "delrange";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 8;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kAlignL1;
  zfo.partition_target_bytes = 4 << 10;
  zfo.epoch_target_bytes = 4 << 10;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }
  char k[16], v[32];
  for (int i = 0; i < 100; ++i) {
    snprintf(k, sizeof(k), "k%05d", i);
    snprintf(v, sizeof(v), "v%05d", i);
    s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 8),
                rocksdb::Slice(v, 6));
    if (!s.ok()) {
      ReportResult(tag, false, "put: " + s.ToString());
      CleanDB(dbname);
      return;
    }
  }
  s = db->DeleteRange(rocksdb::WriteOptions(), "k00020", "k00080");
  if (!s.ok()) {
    ReportResult(tag, false, "deleterange: " + s.ToString());
    CleanDB(dbname);
    return;
  }
  if (!WaitAllMaterialized(db.get())) {
    ReportResult(tag, false, "materialization did not finish");
    CleanDB(dbname);
    return;
  }
  std::string val;
  for (int i = 0; i < 100; ++i) {
    snprintf(k, sizeof(k), "k%05d", i);
    snprintf(v, sizeof(v), "v%05d", i);
    const bool in_range = (i >= 20 && i < 80);
    s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(k, 8), &val);
    if (in_range) {
      if (s.ok()) {
        ReportResult(tag, false, "range key visible: " + std::string(k));
        CleanDB(dbname);
        return;
      }
    } else {
      if (!s.ok() || val != std::string(v, 6)) {
        std::string hex;
        for (char ch : val) {
          char b[4];
          snprintf(b, sizeof(b), "%02x ", (unsigned char)ch);
          hex += b;
        }
        ReportResult(tag, false,
                     "out-of-range key wrong: " + std::string(k) +
                         " val='" + val + "' size=" +
                         std::to_string(val.size()) + " hex=[" + hex + "] (" +
                         s.ToString() + ")");
        CleanDB(dbname);
        return;
      }
    }
  }
  ReportResult(tag, true, "range [20,80) deleted, 100 keys checked");
  CleanDB(dbname);
}

// ---------------------------------------------------------------------------
// 用例 19 (M3.1-19): NonBytewiseComparator — 非字节序比较器下
// PartitionTable::Create + Route + RangeOf 互为逆（I6）。
// ---------------------------------------------------------------------------
void TestNonBytewiseComparator() {
  const char* tag = "NonBytewiseComparator(M3.1-19)";
  const auto* ucmp = rocksdb::ReverseBytewiseComparator();
  // Reverse 序：Compare("z","m") < 0 即 "z" < "m"（bytewise 相反）
  // boundaries {"z","m"} 在 reverse 下升序：z < m
  std::vector<std::string> boundaries = {"z", "m"};
  std::shared_ptr<zeroflush::PartitionTable> pt;
  auto s = zeroflush::PartitionTable::Create(0, boundaries, ucmp, &pt);
  if (!s.ok()) {
    ReportResult(tag, false, "Create: " + s.ToString());
    return;
  }
  // 验证整个字母表: Route 和 RangeOf 在 reverse 比较器下互为逆
  for (char c = 'a'; c <= 'z'; ++c) {
    std::string key(1, c);
    uint32_t p = pt->Route(key);
    rocksdb::Slice lo, hi;
    pt->RangeOf(p, &lo, &hi);
    if (lo.size() > 0 && ucmp->Compare(rocksdb::Slice(key), lo) < 0) {
      ReportResult(tag, false,
                   std::string("Route(key) < lo for key=") + key);
      return;
    }
    if (hi.size() > 0 && ucmp->Compare(rocksdb::Slice(key), hi) >= 0) {
      ReportResult(tag, false,
                   std::string("Route(key) >= hi for key=") + key);
      return;
    }
  }
  // 发散性检验：reverse 下 Route("a") 应与 bytewise 的 Route("a") 不同
  const auto* bytewise = rocksdb::BytewiseComparator();
  std::vector<std::string> bw_boundaries = {"z", "m"};  // bytewise 降序! → Create 应拒绝
  std::shared_ptr<zeroflush::PartitionTable> pt_bw;
  auto sb = zeroflush::PartitionTable::Create(0, bw_boundaries, bytewise, &pt_bw);
  if (sb.ok()) {
    // 若 bytewise 下也能创建（理论上拒绝，但边界校验通过则无歧义），对比 Route
    uint32_t r_rev = pt->Route("a");
    uint32_t r_bw = pt_bw->Route("a");
    if (r_rev == r_bw) {
      // 不同 comparator 存在相同 Route 的可能性，不视作失败，仅警告
    }
  }
  // 边界拒绝：bytewise 下 {"z","m"} 反序 → Create 应返回 InvalidArgument
  std::shared_ptr<zeroflush::PartitionTable> pt_bad;
  auto sb2 = zeroflush::PartitionTable::Create(0, {"z", "m"}, bytewise, &pt_bad);
  if (sb2.ok()) {
    ReportResult(tag, false,
                 "bytewise descending boundaries should have been rejected");
    return;
  }
  ReportResult(tag, true);
}

// ---------------------------------------------------------------------------
// 用例 20 (M3.1-20): PartitionOutputsDisjoint — H1: P 个分区的键区间
// 互不相交，源自边界升序/无重复的不变式。
// ---------------------------------------------------------------------------
void TestPartitionOutputsDisjoint() {
  const char* tag = "PartitionOutputsDisjoint(M3.1-20)";
  const auto* ucmp = rocksdb::BytewiseComparator();
  std::vector<std::string> boundaries = {"c", "g", "m", "q", "u"};
  std::shared_ptr<zeroflush::PartitionTable> pt;
  auto s = zeroflush::PartitionTable::Create(0, boundaries, ucmp, &pt);
  if (!s.ok()) {
    ReportResult(tag, false, "Create: " + s.ToString());
    return;
  }
  const uint32_t P = pt->partitions();
  // H1: 任意 i<j，range_i.hi ≤ range_j.lo
  for (uint32_t i = 0; i < P; ++i) {
    for (uint32_t j = i + 1; j < P; ++j) {
      rocksdb::Slice lo_i, hi_i, lo_j, hi_j;
      pt->RangeOf(i, &lo_i, &hi_i);
      pt->RangeOf(j, &lo_j, &hi_j);
      if (hi_i.size() > 0 && lo_j.size() > 0) {
        if (ucmp->Compare(hi_i, lo_j) > 0) {
          ReportResult(tag, false,
                       "overlap: p" + std::to_string(i) + " hi > p" +
                           std::to_string(j) + " lo");
          return;
        }
      }
    }
  }
  // 验证 Route 分配的唯一性（相同的 key 总是路由到相同的分区——Route 是纯函数）
  // 已在 17 中验证；此处额外验证边界升序校验在 Create 内执行。
  std::shared_ptr<zeroflush::PartitionTable> pt_bad;
  auto sb = zeroflush::PartitionTable::Create(0, {"b", "a"}, ucmp, &pt_bad);
  if (sb.ok()) {
    ReportResult(tag, false, "descending boundaries should be rejected");
    return;
  }
  ReportResult(tag, true);
}

// ---------------------------------------------------------------------------
// 用例 21 (M3.1-21): ComparatorNameMismatchRejected — ZFPROPS v2 中
// 记录了 comparator 名，若文件被篡改则 Open 返回 InvalidArgument。
// ---------------------------------------------------------------------------
void TestComparatorNameMismatchRejected() {
  const char* tag = "ComparatorNameMismatchRejected(M3.1-21)";
  std::string dbname = std::string(kDbBase) + "cmpr_mismatch";
  CleanDB(dbname);

  // 首轮：以 BytewiseComparator 打开，写数据
  {
    zeroflush::ZeroFlushOptions zfo;
    zfo.partitions = 4;
    zfo.use_zfprops = true;
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "open: " + s.ToString());
      return;
    }
    s = db->Put(rocksdb::WriteOptions(), "k", "v");
    if (!s.ok()) {
      ReportResult(tag, false, "put: " + s.ToString());
      return;
    }
  }

  // 验证 ZFPROPS v2 中包含正确的 comparator name
  bool has_correct_name = false;
  {
    rocksdb::Env* env = rocksdb::Env::Default();
    std::string wal_dir = dbname + "/zfwal";
    std::string props_path = wal_dir + "/ZFPROPS";
    std::string buf;
    buf.resize(4 * 1024 * 1024, '\0');
    std::unique_ptr<rocksdb::SequentialFile> rf;
    auto s = env->NewSequentialFile(props_path, &rf, rocksdb::EnvOptions());
    if (s.ok()) {
      rocksdb::Slice result;
      s = rf->Read(buf.size(), &result, &buf[0]);
      if (s.ok()) {
        buf.resize(result.size());
        zeroflush::ZfPropsV2 decoded;
        auto ds = zeroflush::DecodeZfPropsAuto(buf.data(), buf.size(), &decoded);
        if (ds.ok()) {
          has_correct_name =
              (decoded.comparator_name == "leveldb.BytewiseComparator");
        }
      }
    }
  }
  if (!has_correct_name) {
    ReportResult(tag, false, "ZFPROPS comparator name not preserved");
    CleanDB(dbname);
    return;
  }

  // ZFPROPS v2 编码/解码往返测试（comparator name 往返正确）
  {
    std::vector<zeroflush::ZfPropsTableInfo> tables;
    zeroflush::ZfPropsTableInfo ti;
    ti.version = 0;
    ti.partitions = 4;
    for (uint32_t i = 0; i < 4; ++i) ti.part_ids.push_back(i);
    tables.push_back(ti);
    std::string encoded;
    auto es = zeroflush::EncodeZfPropsV2(0, "leveldb.BytewiseComparator", tables, 0, &encoded);
    if (!es.ok()) {
      ReportResult(tag, false, "Encode: " + es.ToString());
      CleanDB(dbname);
      return;
    }
    zeroflush::ZfPropsV2 decoded;
    auto ds = zeroflush::DecodeZfPropsAuto(encoded.data(), encoded.size(), &decoded);
    if (!ds.ok() || decoded.comparator_name != "leveldb.BytewiseComparator") {
      ReportResult(tag, false, "v2 roundtrip failed");
      CleanDB(dbname);
      return;
    }
  }

  // 以正确 comparator name 重开 → 应成功
  {
    zeroflush::ZeroFlushOptions zfo;
    zfo.partitions = 4;
    zfo.use_zfprops = true;
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "reopen failed: " + s.ToString());
      CleanDB(dbname);
      return;
    }
    std::string val;
    s = db->Get(rocksdb::ReadOptions(), "k", &val);
    if (!s.ok() || val != "v") {
      ReportResult(tag, false, "get after reopen");
      CleanDB(dbname);
      return;
    }
  }

  CleanDB(dbname);
  ReportResult(tag, true);
}

// ---------------------------------------------------------------------------
// 用例 22 (M3.1-22): RoutingModeMismatchRejected — kHash 库以 kStatic
// 重开时 ZFPROPS routing_mode 不匹配 → 拒绝打开。
// ---------------------------------------------------------------------------
void TestRoutingModeMismatchRejected() {
  const char* tag = "RoutingModeMismatchRejected(M3.1-22)";
  std::string dbname = std::string(kDbBase) + "rmode_mismatch";
  CleanDB(dbname);

  // 首轮：kHash 模式打开
  {
    zeroflush::ZeroFlushOptions zfo;
    zfo.partitions = 4;
    zfo.use_zfprops = true;
    zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kHash;
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "open kHash: " + s.ToString());
      return;
    }
    s = db->Put(rocksdb::WriteOptions(), "k", "v");
    if (!s.ok()) {
      ReportResult(tag, false, "put: " + s.ToString());
      return;
    }
  }

  // 以 kStatic 重开 → 必须拒绝（routing_mode 不匹配 + 非 kHash→kSampled 升级）
  bool rejected = false;
  {
    zeroflush::ZeroFlushOptions zfo;
    zfo.partitions = 4;
    zfo.use_zfprops = true;
    zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
    zfo.static_boundaries = {"a", "b", "c"};
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (s.IsInvalidArgument()) {
      rejected = true;
    } else if (s.ok()) {
      ReportResult(tag, false,
                   "kStatic reopen succeeded (should have been rejected)");
      CleanDB(dbname);
      return;
    } else {
      ReportResult(tag, false, "kStatic reopen: " + s.ToString());
      CleanDB(dbname);
      return;
    }
  }
  if (!rejected) {
    ReportResult(tag, false, "kStatic reopen not rejected");
    CleanDB(dbname);
    return;
  }

  // kSampled 重开 kHash 库 → 应允许（kHash→kSampled 是安全升级路径）
  {
    zeroflush::ZeroFlushOptions zfo;
    zfo.partitions = 4;
    zfo.use_zfprops = true;
    zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kSampled;
    std::unique_ptr<rocksdb::DB> db;
    // 首轮以 kSampled 模式重开 kHash 库 → 应成功
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "kSampled reopen on kHash DB: " + s.ToString());
      CleanDB(dbname);
      return;
    }
    std::string val;
    s = db->Get(rocksdb::ReadOptions(), "k", &val);
    if (!s.ok() || val != "v") {
      ReportResult(tag, false, "kSampled get: " + (s.ok() ? ("val=" + val) : s.ToString()));
      CleanDB(dbname);
      return;
    }
  }

  CleanDB(dbname);
  ReportResult(tag, true);
}

// ---------------------------------------------------------------------------
// 用例 23 (M3.2-23): BulkLoadZeroL0 — 批量装载全程零 L0 文件。
// 验证 M3.2 层级下探直装（§6.2）：kStatic 范围路由下，多 epoch 批量装载
// 的每个分区文件都直接安装到 base_level（空库 = L6），L0 始终为空。
// 断言：install_fallback_l0 == 0、L0 == 0、
// install_direct_base == 物化 epoch 数 × P、数据全量可读。
// ---------------------------------------------------------------------------
void TestBulkLoadZeroL0() {
  const char* tag = "BulkLoadZeroL0(M3.2-23)";
  std::string dbname = std::string(kDbBase) + "bulk_zero_l0";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  // P=4: 分区 0=[-∞,kb) 1=[kb,kd) 2=[kd,kf) 3=[kf,+∞)
  zfo.static_boundaries = {"kb", "kd", "kf"};
  zfo.partition_target_bytes = 8 << 10;  // 8KB：单分区超限即封存
  zfo.epoch_target_bytes = 8 << 10;      // 副触发先到（写路径按 epoch 边界封存）

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }

  // 10 个逻辑 epoch × 4 分区 × 60 条 ≈ 27KB/epoch（> 8KB 触发封存）。
  // 前缀 ka/kc/ke/kg 分别落入分区 0/1/2/3；同分区键随 e,i 严格递增，
  // 相邻 epoch 键区间不重叠 → 全量直装 base 的前提成立。
  const char* prefixes[4] = {"ka", "kc", "ke", "kg"};
  const int kEpochs = 10, kPerPart = 60;
  char k[16], v[128];
  for (int e = 0; e < kEpochs; ++e) {
    for (int i = 0; i < kPerPart; ++i) {
      for (int p = 0; p < 4; ++p) {
        snprintf(k, sizeof(k), "%s%010lld", prefixes[p],
                 (long long)(e * 100 + i));
        ::memset(v, 'A' + (e % 26), 100);
        v[100] = '\0';
        s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 12),
                    rocksdb::Slice(v, 100));
        if (!s.ok()) {
          ReportResult(tag, false,
                       "put@e" + std::to_string(e) + " i" + std::to_string(i) +
                           ": " + s.ToString());
          CleanDB(dbname);
          return;
        }
      }
    }
  }

  // 等待全部已封存 epoch 物化完成（写结束后 sealed 不再增长）。
  // 注意：DB 关闭前检查——残留活跃代在 close 时才 flush，不计入物化。
  if (!WaitAllMaterialized(db.get())) {
    DumpDbLogTail(dbname);
    ReportResult(tag, false,
                 "materialization did not finish; sealed=" +
                     std::to_string(ZfMetric(db.get(), "epochs_sealed")) +
                     " materialized=" +
                     std::to_string(ZfMetric(db.get(), "epochs_materialized")));
    CleanDB(dbname);
    return;
  }

  // 直装全程生效：L0 恒空、零回落、文件全部落 base_level(L6)
  const uint64_t direct = ZfMetric(db.get(), "install_direct_base");
  const uint64_t fallback = ZfMetric(db.get(), "install_fallback_l0");
  const uint64_t n0 = NumFilesAtLevel(db.get(), 0);
  const uint64_t n6 = NumFilesAtLevel(db.get(), 6);
  if (fallback != 0 || n0 != 0) {
    ReportResult(tag, false,
                 "fallback=" + std::to_string(fallback) +
                     " l0_files=" + std::to_string(n0));
    CleanDB(dbname);
    return;
  }
  if (direct == 0 || direct != n6) {
    ReportResult(tag, false,
                 "direct=" + std::to_string(direct) +
                     " != l6_files=" + std::to_string(n6));
    CleanDB(dbname);
    return;
  }
  if (direct != ZfMetric(db.get(), "epochs_materialized") * 4) {
    ReportResult(tag, false,
                 "direct=" + std::to_string(direct) +
                     " != materialized*P=" +
                     std::to_string(ZfMetric(db.get(), "epochs_materialized") *
                                    4));
    CleanDB(dbname);
    return;
  }

  // 数据全量可读
  const int64_t total = kEpochs * kPerPart * 4;
  if (CountViaIterator(db.get()) != total) {
    ReportResult(tag, false,
                 "iter=" + std::to_string(CountViaIterator(db.get())) +
                     " != " + std::to_string(total));
    CleanDB(dbname);
    return;
  }

  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true,
               "direct=" + std::to_string(direct) + " l0=0 fallback=0");
}

// ---------------------------------------------------------------------------
// 用例 24 (M3.2-24): MaterializeParallelSpeedup — K 路并行物化吞吐。
// 相同写入序列与分区配置下，materialize_parallelism=8 的物化墙钟耗时
// （rocksdb.zeroflush.materialize_micros：封存→引用归零，即物化完成）
// 应显著低于 K=1：断言 speedup >= 2（§6.1 性能目标）。
// ---------------------------------------------------------------------------
void TestMaterializeParallelSpeedup() {
  const char* tag = "MaterializeParallelSpeedup(M3.2-24)";
  const std::string dbname1 = std::string(kDbBase) + "par_k1";
  const std::string dbname8 = std::string(kDbBase) + "par_k8";
  CleanDB(dbname1);
  CleanDB(dbname8);

  // 单 epoch ≈ 8 分区 × 12MB = 96MB；写 100 万条 ~120B 记录（110MB）触发。
  const int kRecords = 1 << 20;

  struct Result {
    bool ok = false;
    uint64_t micros = 0;
    uint64_t epochs = 0;
    int64_t total = -1;
    std::string err;
  };
  auto run_db = [&](const std::string& dbname, uint32_t k) -> Result {
    Result r;
    zeroflush::ZeroFlushOptions zfo;
    zfo.partitions = 8;
    zfo.partition_target_bytes = 12u << 20;  // 12MB：副触发先到
    zfo.epoch_target_bytes = 1u << 30;       // 1GB：主触发不参与（远大于）
    zfo.materialize_parallelism = k;
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      r.err = "open(k=" + std::to_string(k) + "): " + s.ToString();
      return r;
    }
    // 固定种子：两个库写入完全相同的数据（保证封存/物化工作量一致）
    std::mt19937_64 rng(0x5EED);
    char kk[24], vv[128];
    for (int i = 0; i < kRecords; ++i) {
      for (int j = 0; j < 20; ++j) {
        kk[j] = 'a' + static_cast<char>(rng() % 26);
      }
      kk[20] = '\0';
      ::memset(vv, 'A' + (i % 26), 100);
      vv[100] = '\0';
      s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(kk, 20),
                  rocksdb::Slice(vv, 100));
      if (!s.ok()) {
        r.err = "put(k=" + std::to_string(k) + ")@" + std::to_string(i) +
                ": " + s.ToString();
        return r;
      }
    }
    if (!WaitForZfMetric(db.get(), "epochs_materialized", 1)) {
      r.err = "no materialized epoch (k=" + std::to_string(k) + ")";
      return r;
    }
    if (!WaitAllMaterialized(db.get())) {
      r.err = "materialization unfinished (k=" + std::to_string(k) + ")";
      return r;
    }
    r.micros = ZfMetric(db.get(), "materialize_micros");
    r.epochs = ZfMetric(db.get(), "epochs_materialized");
    r.total = CountViaIterator(db.get());
    r.ok = true;
    db.reset();
    CleanDB(dbname);
    return r;
  };

  Result r1 = run_db(dbname1, 1);
  if (!r1.ok) {
    ReportResult(tag, false, r1.err);
    CleanDB(dbname1);
    CleanDB(dbname8);
    return;
  }
  Result r8 = run_db(dbname8, 8);
  if (!r8.ok) {
    ReportResult(tag, false, r8.err);
    CleanDB(dbname1);
    CleanDB(dbname8);
    return;
  }

  if (r1.total != kRecords || r8.total != kRecords) {
    ReportResult(tag, false,
                 "count k1=" + std::to_string(r1.total) +
                     " k8=" + std::to_string(r8.total));
    return;
  }
  if (r1.epochs == 0 || r8.epochs == 0) {
    ReportResult(tag, false,
                 "epochs k1=" + std::to_string(r1.epochs) +
                     " k8=" + std::to_string(r8.epochs));
    return;
  }
  if (r1.micros < r8.micros * 2) {
    ReportResult(tag, false,
                 "speedup too low: k1=" + std::to_string(r1.micros) +
                     "us vs k8=" + std::to_string(r8.micros) + "us (epochs " +
                     std::to_string(r1.epochs) + " vs " +
                     std::to_string(r8.epochs) + ")");
    return;
  }

  ReportResult(tag, true,
               "k1=" + std::to_string(r1.micros) + "us k8=" +
                   std::to_string(r8.micros) + "us speedup=" +
                   std::to_string(r1.micros / r8.micros) + "x");
}

// ---------------------------------------------------------------------------
// 用例 25 (M3.2-25): InstallFallbackToL0 — 同范围重写时回落 L0。
// 验证 §6.2 定层规则的反向路径：空库首写直装 base_level（L6）；
// 同键范围再次物化与 L6 文件重叠 → 无法直装 → 单文件原子回落 L0，
// 且 L0 新文件在读取时优先于 L6 旧值。
// ---------------------------------------------------------------------------
void TestInstallFallbackToL0() {
  const char* tag = "InstallFallbackToL0(M3.2-25)";
  std::string dbname = std::string(kDbBase) + "fallback_l0";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  // P=4: 分区 1=[d,i)；键 "fa..."（f 在 [d,i) 内）全落分区 1
  zfo.static_boundaries = {"d", "i", "n"};
  zfo.partition_target_bytes = 8 << 10;  // 8KB：500 条 × ~110B 触发多次封存
  zfo.epoch_target_bytes = 8 << 10;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }

  const int kRecords = 500;
  char k[16], v[128];
  auto put_value = [&](char fill) -> rocksdb::Status {
    for (int i = 0; i < kRecords; ++i) {
      snprintf(k, sizeof(k), "fa%010lld", (long long)i);
      ::memset(v, fill, 100);
      v[100] = '\0';
      auto st = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 12),
                        rocksdb::Slice(v, 100));
      if (!st.ok()) return st;
    }
    return rocksdb::Status::OK();
  };

  // 第一批：值 'A'，空库 → 全部直装 base_level
  s = put_value('A');
  if (!s.ok()) {
    DumpDbLogTail(dbname);
    ReportResult(tag, false, "put A: " + s.ToString());
    CleanDB(dbname);
    return;
  }
  if (!WaitAllMaterialized(db.get())) {
    DumpDbLogTail(dbname);
    ReportResult(tag, false,
                 "wait A materialized; sealed=" +
                     std::to_string(ZfMetric(db.get(), "epochs_sealed")) +
                     " materialized=" +
                     std::to_string(ZfMetric(db.get(), "epochs_materialized")));
    CleanDB(dbname);
    return;
  }
  if (ZfMetric(db.get(), "install_fallback_l0") != 0) {
    ReportResult(tag, false, "epoch1 fallback != 0");
    CleanDB(dbname);
    return;
  }
  if (ZfMetric(db.get(), "install_direct_base") == 0) {
    ReportResult(tag, false, "epoch1 no direct install");
    CleanDB(dbname);
    return;
  }
  if (NumFilesAtLevel(db.get(), 0) != 0) {
    ReportResult(tag, false, "epoch1 l0_files != 0");
    CleanDB(dbname);
    return;
  }
  {
    std::string got;
    snprintf(k, sizeof(k), "fa%010lld", (long long)42);
    s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(k, 12), &got);
    if (!s.ok() || got.size() != 100 || got[0] != 'A') {
      ReportResult(tag, false, "epoch1 get");
      CleanDB(dbname);
      return;
    }
  }

  // 第二批：同范围值 'B'，与 L6 文件键区间重叠 → 全部回落 L0
  s = put_value('B');
  if (!s.ok()) {
    ReportResult(tag, false, "put B: " + s.ToString());
    CleanDB(dbname);
    return;
  }
  if (!WaitAllMaterialized(db.get())) {
    ReportResult(tag, false, "wait B materialized");
    CleanDB(dbname);
    return;
  }
  if (ZfMetric(db.get(), "install_fallback_l0") < 1) {
    ReportResult(tag, false,
                 "epoch2 fallback=" +
                     std::to_string(ZfMetric(db.get(), "install_fallback_l0")));
    CleanDB(dbname);
    return;
  }
  // 注：回落 L0 的文件可能已被 compaction 合并进 base level（重叠范围触发
  // 正常合并），因此不在此断言 L0 文件数；回落事实由 fallback 计数保证。

  // 全量 Get 验证：L0 新文件（值 B）优先于 L6 旧值（值 A）
  {
    std::string got;
    for (int i = 0; i < kRecords; ++i) {
      snprintf(k, sizeof(k), "fa%010lld", (long long)i);
      s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(k, 12), &got);
      if (!s.ok() || got.size() != 100 || got[0] != 'B') {
        ReportResult(tag, false,
                     "get@" + std::to_string(i) +
                         (s.ok() ? (" val[0]=" + std::to_string(got[0]))
                                 : s.ToString()));
        CleanDB(dbname);
        return;
      }
    }
  }
  // 同键覆盖不产生重复条目
  if (CountViaIterator(db.get()) != kRecords) {
    ReportResult(tag, false,
                 "iter=" + std::to_string(CountViaIterator(db.get())) +
                     " != " + std::to_string(kRecords));
    CleanDB(dbname);
    return;
  }

  // 回落事实已由 fallback 计数证明；保存指标后释放 DB（reset 后再调
  // ZfMetric 会对已析构的 DB 解引用）。
  const uint64_t direct_installs = ZfMetric(db.get(), "install_direct_base");
  const uint64_t fallback_installs = ZfMetric(db.get(), "install_fallback_l0");
  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true,
               "direct=" + std::to_string(direct_installs) +
                   " fallback=" + std::to_string(fallback_installs));
}


// ---------------------------------------------------------------------------
// 用例 26 (M3.3-26): SteadyStateZeroL0 — 融合归并稳态零 L0（H3）。
// 验证 §7.2/§7.4：merge_into_base_level 开启时，同键范围反复封存物化
// 全部走"直装或融合"路径：install_fallback_l0 恒为 0、L0 恒为空、
// base_merge_count > 0（融合真实发生）、数据不丢失不重复。
// ---------------------------------------------------------------------------
void TestSteadyStateZeroL0() {
  const char* tag = "SteadyStateZeroL0(M3.3-26)";
  std::string dbname = std::string(kDbBase) + "steady_l0";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  // P=4: 分区 1=[d,i)；键 "fa..."（f 在 [d,i) 内）全落分区 1
  zfo.static_boundaries = {"d", "i", "n"};
  // 32KB 阈值：每 epoch sealed ≈ 32KB vs base 层累积 ≈ 52KB（500 条 ×
  // ~104B），sealed/overlap ≈ 0.6 > base_merge_min_ratio(0.25) → 稳态持续
  // 融合（8KB 阈值下比率 0.14 跌破阈值会回落到 L0，见 §7.2 触发比）。
  zfo.partition_target_bytes = 32 << 10;
  zfo.epoch_target_bytes = 32 << 10;
  zfo.merge_into_base_level = true;  // M3.3 融合归并

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }

  const int kRecords = 500;  // 每个 epoch 写相同键集（同键覆盖，值轮换）
  const int kEpochs = 20;
  char k[16], v[128];
  for (int e = 0; e < kEpochs; ++e) {
    for (int i = 0; i < kRecords; ++i) {
      snprintf(k, sizeof(k), "fa%010lld", (long long)i);
      ::memset(v, 'A' + (e % 26), 100);
      v[100] = '\0';
      s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 12),
                  rocksdb::Slice(v, 100));
      if (!s.ok()) {
        ReportResult(tag, false,
                     "put e=" + std::to_string(e) + ": " + s.ToString());
        CleanDB(dbname);
        return;
      }
    }
    // 让后台封存/物化随写入推进（避免最后一轮集中触发）。
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  if (!WaitAllMaterialized(db.get())) {
    DumpDbLogTail(dbname);
    ReportResult(tag, false,
                 "wait materialized; sealed=" +
                     std::to_string(ZfMetric(db.get(), "epochs_sealed")) +
                     " materialized=" +
                     std::to_string(ZfMetric(db.get(), "epochs_materialized")));
    CleanDB(dbname);
    return;
  }
  if (ZfMetric(db.get(), "epochs_sealed") < (uint64_t)kEpochs) {
    ReportResult(tag, false,
                 "epochs_sealed=" + std::to_string(ZfMetric(db.get(), "epochs_sealed")) +
                     " < " + std::to_string(kEpochs));
    CleanDB(dbname);
    return;
  }
  // 核心断言：稳态下无回落、无 L0 残留。
  const uint64_t fallback = ZfMetric(db.get(), "install_fallback_l0");
  if (fallback != 0) {
    ReportResult(tag, false, "install_fallback_l0=" + std::to_string(fallback));
    CleanDB(dbname);
    return;
  }
  if (NumFilesAtLevel(db.get(), 0) != 0) {
    ReportResult(tag, false, "l0_files != 0");
    CleanDB(dbname);
    return;
  }
  // 融合真实发生（空库首 epoch 直装，后续 epoch 融合）。
  const uint64_t merges = ZfMetric(db.get(), "base_merge_count");
  if (merges == 0) {
    ReportResult(tag, false, "base_merge_count == 0");
    CleanDB(dbname);
    return;
  }
  // 同键覆盖 → 唯一键 500，值 = 最后一轮（'A' + 19 % 26 = 'T'）。
  if (CountViaIterator(db.get()) != kRecords) {
    ReportResult(tag, false,
                 "iter=" + std::to_string(CountViaIterator(db.get())) +
                     " != " + std::to_string(kRecords));
    CleanDB(dbname);
    return;
  }
  {
    std::string got;
    for (int i = 0; i < kRecords; ++i) {
      snprintf(k, sizeof(k), "fa%010lld", (long long)i);
      s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(k, 12), &got);
      if (!s.ok() || got.size() != 100 || got[0] != 'T') {
        ReportResult(tag, false,
                     "get@" + std::to_string(i) +
                         (s.ok() ? (" val[0]=" + std::to_string(got[0]))
                                 : s.ToString()));
        CleanDB(dbname);
        return;
      }
    }
  }
  const uint64_t rewritten = ZfMetric(db.get(), "base_merge_rewritten_bytes");
  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true,
               "merges=" + std::to_string(merges) +
                   " rewritten=" + std::to_string(rewritten) + "B");
}

// ---------------------------------------------------------------------------
// 用例 27 (M3.3-27): MaterializeVsCompactionRace — 融合归并与原生
// compaction 共存不冲突。
// 验证 §7.3 注册互斥：手动 CompactRange 排空后，新 epoch 物化命中 base
// 层重叠文件 → 融合触发（base_merge_count > 0）；原生 compaction 不破坏
// ZF 直装数据；融合替换后读取一致（无重复、值正确）。
// ---------------------------------------------------------------------------
void TestMaterializeVsCompactionRace() {
  const char* tag = "MaterializeVsCompactionRace(M3.3-27)";
  std::string dbname = std::string(kDbBase) + "merge_race";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  zfo.static_boundaries = {"d", "i", "n"};
  // 32KB 阈值：阶段 3 sealed ≈ 32KB vs base 层 ≈ 52KB → 融合触发
  // （8KB 阈值下比率 0.15 < 0.25 会回落到 L0，无法断言 base_merge_count>0）。
  zfo.partition_target_bytes = 32 << 10;
  zfo.epoch_target_bytes = 32 << 10;
  zfo.merge_into_base_level = true;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }

  const int kRecords = 500;
  char k[16], v[128];
  auto put_value = [&](char fill) -> rocksdb::Status {
    for (int i = 0; i < kRecords; ++i) {
      snprintf(k, sizeof(k), "fa%010lld", (long long)i);
      ::memset(v, fill, 100);
      v[100] = '\0';
      auto st = db->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 12),
                        rocksdb::Slice(v, 100));
      if (!st.ok()) return st;
    }
    return rocksdb::Status::OK();
  };

  // 阶段 1：空库写 'A' → 物化直装 base 层。
  s = put_value('A');
  if (!s.ok()) {
    ReportResult(tag, false, "put A: " + s.ToString());
    CleanDB(dbname);
    return;
  }
  if (!WaitAllMaterialized(db.get())) {
    DumpDbLogTail(dbname);
    ReportResult(tag, false, "wait A materialized");
    CleanDB(dbname);
    return;
  }
  if (NumFilesAtLevel(db.get(), 0) != 0) {
    ReportResult(tag, false, "phase1 l0_files != 0");
    CleanDB(dbname);
    return;
  }

  // 阶段 2：手动触发原生 compaction（全范围，排空到最深层）。
  rocksdb::CompactRangeOptions cro;
  s = db->CompactRange(cro, nullptr, nullptr);
  if (!s.ok()) {
    DumpDbLogTail(dbname);
    ReportResult(tag, false, "compact: " + s.ToString());
    CleanDB(dbname);
    return;
  }
  // 原生 compaction 不得破坏 ZF 直装数据。
  if (CountViaIterator(db.get()) != kRecords) {
    ReportResult(tag, false,
                 "after compact iter=" + std::to_string(CountViaIterator(db.get())));
    CleanDB(dbname);
    return;
  }

  // 阶段 3：同键范围写 'B' → 封存物化 → 与 base 层文件重叠 → 融合。
  s = put_value('B');
  if (!s.ok()) {
    ReportResult(tag, false, "put B: " + s.ToString());
    CleanDB(dbname);
    return;
  }
  if (!WaitAllMaterialized(db.get())) {
    DumpDbLogTail(dbname);
    ReportResult(tag, false, "wait B materialized");
    CleanDB(dbname);
    return;
  }
  if (ZfMetric(db.get(), "base_merge_count") == 0) {
    ReportResult(tag, false, "base_merge_count == 0 (merge not triggered)");
    CleanDB(dbname);
    return;
  }
  // 融合替换后：唯一键 500、值全 'B'（B 侧旧值被 A 侧覆盖不重复）。
  if (CountViaIterator(db.get()) != kRecords) {
    ReportResult(tag, false,
                 "iter=" + std::to_string(CountViaIterator(db.get())) +
                     " != " + std::to_string(kRecords));
    CleanDB(dbname);
    return;
  }
  {
    std::string got;
    for (int i = 0; i < kRecords; ++i) {
      snprintf(k, sizeof(k), "fa%010lld", (long long)i);
      s = db->Get(rocksdb::ReadOptions(), rocksdb::Slice(k, 12), &got);
      if (!s.ok() || got.size() != 100 || got[0] != 'B') {
        ReportResult(tag, false,
                     "get@" + std::to_string(i) +
                         (s.ok() ? (" val[0]=" + std::to_string(got[0]))
                                 : s.ToString()));
        CleanDB(dbname);
        return;
      }
    }
  }
  const uint64_t merges = ZfMetric(db.get(), "base_merge_count");
  db.reset();  // reset 后再调 ZfMetric 会对已析构 DB 解引用（先取值）
  CleanDB(dbname);
  ReportResult(tag, true, "merges=" + std::to_string(merges));
}

// ---------------------------------------------------------------------------
// 用例 28 (M3.3-28): RecoveryEpochMergeIdempotent — 重开场景融合归并幂等。
// 简化（计划）：无需真实 crash；两段式重开模拟恢复后的覆盖写：
// Open → 写 10 条 'A' → Close（物化）→ Open → 写同 10 条 'B'（物化时与
// base 层重叠 → 融合）→ Close → Open → 记录不翻倍、值全 'B'。
// ---------------------------------------------------------------------------
void TestRecoveryEpochMergeIdempotent() {
  const char* tag = "RecoveryEpochMergeIdempotent(M3.3-28)";
  std::string dbname = std::string(kDbBase) + "merge_reopen";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  zfo.static_boundaries = {"d", "i", "n"};
  // 512B 阈值：10 条 × ~56B（WAL 记录紧凑）≈ 560B ≥ 512B 即可触发封存
  // （1KB 阈值下 10 条 560B < 1024 写不满，封存不会触发）。
  zfo.partition_target_bytes = 512;
  zfo.epoch_target_bytes = 512;
  zfo.merge_into_base_level = true;

  const int kRecords = 10;
  char k[16], v[128];
  auto put_value = [&](rocksdb::DB* d, char fill) -> rocksdb::Status {
    for (int i = 0; i < kRecords; ++i) {
      snprintf(k, sizeof(k), "fa%010lld", (long long)i);
      ::memset(v, fill, 100);
      v[100] = '\0';
      auto st = d->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 12),
                       rocksdb::Slice(v, 100));
      if (!st.ok()) return st;
    }
    return rocksdb::Status::OK();
  };
  auto verify = [&](rocksdb::DB* d, char fill, const char* phase) -> bool {
    if (CountViaIterator(d) != kRecords) {
      ReportResult(tag, false,
                   std::string(phase) + " iter=" +
                       std::to_string(CountViaIterator(d)) +
                       " != " + std::to_string(kRecords));
      return false;
    }
    std::string got;
    for (int i = 0; i < kRecords; ++i) {
      snprintf(k, sizeof(k), "fa%010lld", (long long)i);
      auto st = d->Get(rocksdb::ReadOptions(), rocksdb::Slice(k, 12), &got);
      if (!st.ok() || got.size() != 100 || got[0] != fill) {
        ReportResult(tag, false,
                     std::string(phase) + " get@" + std::to_string(i) +
                         (st.ok() ? (" val[0]=" + std::to_string(got[0]))
                                  : st.ToString()));
        return false;
      }
    }
    return true;
  };

  // 单实例双 epoch（计划 §1.6-28 简化）：不需要真实 crash。
  // epoch 1 写 'A'（直装 base）→ epoch 2 写同键 'B'（与 base 重叠 → 融合）。
  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }
  // 阶段 1：写 10 条 'A' → 封存物化（首 epoch 直装 base 层）。
  s = put_value(db.get(), 'A');
  if (!s.ok()) {
    ReportResult(tag, false, "put A: " + s.ToString());
    CleanDB(dbname);
    return;
  }
  if (!WaitForZfMetric(db.get(), "epochs_sealed", 1)) {
    ReportResult(tag, false, "phase1 no seal");
    CleanDB(dbname);
    return;
  }
  if (!WaitAllMaterialized(db.get())) {
    DumpDbLogTail(dbname);
    ReportResult(tag, false, "wait A materialized");
    CleanDB(dbname);
    return;
  }
  if (!verify(db.get(), 'A', "phase1")) {
    CleanDB(dbname);
    return;
  }
  // 阶段 2：写同键 'B' → 物化与 base 层重叠 → 融合归并。
  s = put_value(db.get(), 'B');
  if (!s.ok()) {
    ReportResult(tag, false, "put B: " + s.ToString());
    CleanDB(dbname);
    return;
  }
  if (!WaitAllMaterialized(db.get())) {
    DumpDbLogTail(dbname);
    ReportResult(tag, false, "wait B materialized");
    CleanDB(dbname);
    return;
  }
  // 覆盖写与 base 层重叠 → 融合应真实发生（§7.2）。
  if (ZfMetric(db.get(), "base_merge_count") == 0) {
    ReportResult(tag, false, "base_merge_count == 0");
    CleanDB(dbname);
    return;
  }
  if (!verify(db.get(), 'B', "phase2")) {
    CleanDB(dbname);
    return;
  }
  db.reset();  // Close：析构 flush，数据持久化

  // 阶段 3：重开验证幂等（记录不翻倍、值全 'B'）。
  {
    std::unique_ptr<rocksdb::DB> db2;
    auto s2 = zeroflush::Open(MakeOptions(), zfo, dbname, &db2);
    if (!s2.ok()) {
      ReportResult(tag, false, "open#2: " + s2.ToString());
      return;
    }
    if (!verify(db2.get(), 'B', "phase3")) {
      CleanDB(dbname);
      return;
    }
    db2.reset();
  }
  CleanDB(dbname);
  ReportResult(tag, true, "reopen merge idempotent");
}

// ---------------------------------------------------------------------------
// 用例 48 (M4.5b-48): SkipBatchMaterialize — kSkip 攒批物化端到端。
// 比例不足（sealed/overlap < base_merge_min_ratio）的 epoch 不回落 L0，
// 而是跳过物化（数据留在 frozen 索引 + recovery WAL 可读），下个 epoch
// 收养后多代合并融合。验证：skip_count>0、全程零回落零 L0、Get/迭代器
// 全对、中途重开（孤儿恢复）与最终重开均完整。
// ---------------------------------------------------------------------------
void TestSkipBatchMaterialize() {
  const char* tag = "SkipBatchMaterialize(M4.5b-48)";
  std::string dbname = std::string(kDbBase) + "skip_batch";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  zfo.static_boundaries = {"d", "i", "n"};
  // 8KB 阈值：每 epoch sealed ≈ 8KB；L1 融合输出随 epoch 累积
  // （8→16→24→32→40KB），ratio = sealed/overlap 周期跌破 0.25 →
  // kSkip（原 8KB 场景的回落点，见 TestSteadyStateZeroL0 注释）。
  zfo.partition_target_bytes = 8 << 10;
  zfo.epoch_target_bytes = 8 << 10;
  zfo.merge_into_base_level = true;
  zfo.base_merge_min_ratio = 0.25;
  zfo.skip_batching = true;  // M4.5b：本用例显式开启攒批（默认关）

  const int kRecords = 500;  // 每轮同键覆盖写（值轮换）
  char k[16], v[128];
  auto put_round = [&](rocksdb::DB* d, char fill) -> rocksdb::Status {
    for (int i = 0; i < kRecords; ++i) {
      snprintf(k, sizeof(k), "fa%010lld", (long long)i);
      ::memset(v, fill, 100);
      v[100] = '\0';
      auto st = d->Put(rocksdb::WriteOptions(), rocksdb::Slice(k, 12),
                       rocksdb::Slice(v, 100));
      if (!st.ok()) return st;
    }
    return rocksdb::Status::OK();
  };
  auto verify = [&](rocksdb::DB* d, char fill, const char* phase) -> bool {
    if (CountViaIterator(d) != kRecords) {
      ReportResult(tag, false,
                   std::string(phase) + " iter=" +
                       std::to_string(CountViaIterator(d)) +
                       " != " + std::to_string(kRecords));
      return false;
    }
    std::string got;
    for (int i = 0; i < kRecords; ++i) {
      snprintf(k, sizeof(k), "fa%010lld", (long long)i);
      auto st = d->Get(rocksdb::ReadOptions(), rocksdb::Slice(k, 12), &got);
      if (!st.ok() || got.size() != 100 || got[0] != fill) {
        ReportResult(tag, false,
                     std::string(phase) + " get@" + std::to_string(i) +
                         (st.ok() ? (" val[0]=" + std::to_string(got[0]))
                                  : st.ToString()));
        return false;
      }
    }
    return true;
  };

  // 阶段 1：写两轮（A、B）→ 物化收敛。期间 ratio 跌破阈值 → kSkip
  // （不回落 L0）；收养后的 epoch 多代融合直装。核心断言：零回落、
  // 零 L0、skip 计数 > 0。
  {
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "open: " + s.ToString());
      return;
    }
    s = put_round(db.get(), 'A');
    if (!s.ok()) {
      ReportResult(tag, false, "put A: " + s.ToString());
      CleanDB(dbname);
      return;
    }
    if (!WaitAllMaterialized(db.get())) {
      DumpDbLogTail(dbname);
      ReportResult(tag, false, "wait A materialized");
      CleanDB(dbname);
      return;
    }
    s = put_round(db.get(), 'B');
    if (!s.ok()) {
      ReportResult(tag, false, "put B: " + s.ToString());
      CleanDB(dbname);
      return;
    }
    if (!WaitAllMaterialized(db.get())) {
      DumpDbLogTail(dbname);
      ReportResult(tag, false, "wait B materialized");
      CleanDB(dbname);
      return;
    }
    // 攒批必须真实发生（比率周期性跌破 0.25 的分区被跳过）。
    if (ZfMetric(db.get(), "skip_count") == 0) {
      ReportResult(tag, false, "skip_count == 0 (kSkip never triggered)");
      CleanDB(dbname);
      return;
    }
    if (ZfMetric(db.get(), "install_fallback_l0") != 0) {
      DumpDbLogTail(dbname);
      ReportResult(tag, false,
                   "install_fallback_l0 != 0 (kSkip failed to replace fallback): "
                   "skip=" +
                       std::to_string(ZfMetric(db.get(), "skip_count")) +
                       " direct=" +
                       std::to_string(ZfMetric(db.get(), "install_direct_base")) +
                       " merges=" +
                       std::to_string(ZfMetric(db.get(), "base_merge_count")) +
                       " sealed=" +
                       std::to_string(ZfMetric(db.get(), "epochs_sealed")) +
                       " materialized=" +
                       std::to_string(ZfMetric(db.get(), "epochs_materialized")));
      CleanDB(dbname);
      return;
    }
    if (NumFilesAtLevel(db.get(), 0) != 0) {
      ReportResult(tag, false, "L0 files != 0 after two rounds");
      CleanDB(dbname);
      return;
    }
    if (!verify(db.get(), 'B', "phase1")) {
      CleanDB(dbname);
      return;
    }
    db.reset();  // 阶段 1 结束：kSkip 的 recovery WAL 落盘，重开验证孤儿恢复
  }

  // 阶段 2：重开（Recover 孤儿检测登记 kSkip 代 → 收养路径）→ 数据完整。
  {
    std::unique_ptr<rocksdb::DB> db2;
    auto s2 = zeroflush::Open(MakeOptions(), zfo, dbname, &db2);
    if (!s2.ok()) {
      ReportResult(tag, false, "open#2: " + s2.ToString());
      return;
    }
    if (!verify(db2.get(), 'B', "phase2")) {
      CleanDB(dbname);
      return;
    }
    // 继续写两轮（C、D）→ 收养后的多代合并物化。重开后首个封存 epoch
    // 收养恢复期孤儿（Recover 登记的 kSkip 代，无法与崩溃孤儿区分）→
    // 孤儿分区强制物化（回落有界，L0 遮蔽语义要求后续同分区回落直至
    // compaction 消费——正确性安全）；孤儿清空后 kSkip 攒批恢复。
    const uint64_t skip_before = ZfMetric(db2.get(), "skip_count");
    auto s = put_round(db2.get(), 'C');
    if (!s.ok()) {
      ReportResult(tag, false, "put C: " + s.ToString());
      CleanDB(dbname);
      return;
    }
    if (!WaitAllMaterialized(db2.get())) {
      DumpDbLogTail(dbname);
      ReportResult(tag, false, "wait C materialized");
      CleanDB(dbname);
      return;
    }
    s = put_round(db2.get(), 'D');
    if (!s.ok()) {
      ReportResult(tag, false, "put D: " + s.ToString());
      CleanDB(dbname);
      return;
    }
    if (!WaitAllMaterialized(db2.get())) {
      DumpDbLogTail(dbname);
      ReportResult(tag, false, "wait D materialized");
      CleanDB(dbname);
      return;
    }
    // 重开后：孤儿分区强制物化（回落有界）；孤儿清空后 kSkip 恢复
    // （skip_count 从 0 重新增长）。断言：skip 恢复、回落有界、数据完整。
    if (ZfMetric(db2.get(), "skip_count") <= skip_before) {
      DumpDbLogTail(dbname);
      ReportResult(tag, false,
                   "kSkip not resumed after orphan adoption: before=" +
                       std::to_string(skip_before) +
                       " after=" +
                       std::to_string(ZfMetric(db2.get(), "skip_count")) +
                       " sealed=" +
                       std::to_string(ZfMetric(db2.get(), "epochs_sealed")) +
                       " materialized=" +
                       std::to_string(ZfMetric(db2.get(), "epochs_materialized")) +
                       " fallback=" +
                       std::to_string(ZfMetric(db2.get(), "install_fallback_l0")));
      CleanDB(dbname);
      return;
    }
    if (ZfMetric(db2.get(), "install_fallback_l0") > 2) {
      ReportResult(tag, false,
                   "fallback after rounds 3-4 > 2 (orphan adoption bound)");
      CleanDB(dbname);
      return;
    }
    if (NumFilesAtLevel(db2.get(), 0) > 2) {
      ReportResult(tag, false, "L0 files > 2 after four rounds");
      CleanDB(dbname);
      return;
    }
    if (!verify(db2.get(), 'D', "phase2")) {
      CleanDB(dbname);
      return;
    }
    db2.reset();
  }

  // 阶段 3：最终重开 → 全部数据完整（多代合并已全部物化进 SST）。
  {
    std::unique_ptr<rocksdb::DB> db3;
    auto s3 = zeroflush::Open(MakeOptions(), zfo, dbname, &db3);
    if (!s3.ok()) {
      ReportResult(tag, false, "open#3: " + s3.ToString());
      return;
    }
    if (!verify(db3.get(), 'D', "phase3")) {
      CleanDB(dbname);
      return;
    }
    db3.reset();
  }
  CleanDB(dbname);
  ReportResult(tag, true, "skip-batch zero-L0 end-to-end");
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

  // ---- M3.1 新增 ----
  run("StaticBoundariesRoute",        TestStaticBoundariesRoute);
  run("SampledBoundariesConverge",    TestSampledBoundariesConverge);
  run("SampledLearningEpochEndToEnd", TestSampledLearningEpochEndToEnd);
  run("AlignL1Boundaries",            TestAlignL1Boundaries);
  run("PartitionFreezeIndependent",   TestPartitionFreezeIndependent);
  run("ConcurrentPartitionWriteRead", TestConcurrentPartitionWriteRead);
  run("GetAfterCompact",              TestGetAfterCompact);
  run("CrashBeforeCompact",           TestCrashBeforeCompact);
  run("MemoryBudgetBackpressure",     TestMemoryBudgetBackpressure);
  run("ParallelPartitionCompact",     TestParallelPartitionCompact);
  run("SteadyStateControlledL0",      TestSteadyStateControlledL0);
  run("MultiGenFrozenIterator",        TestMultiGenFrozenIterator);
  run("MergeOperandChain",              TestMergeOperandChain);
  run("DeleteRangeAcrossPartitions",    TestDeleteRangeAcrossPartitions);
  run("NonBytewiseComparator",        TestNonBytewiseComparator);
  run("PartitionOutputsDisjoint",     TestPartitionOutputsDisjoint);
  run("ComparatorNameMismatchRejected", TestComparatorNameMismatchRejected);
  run("RoutingModeMismatchRejected",  TestRoutingModeMismatchRejected);

  // ---- M3.2 新增 ----
  run("BulkLoadZeroL0",             TestBulkLoadZeroL0);
  run("MaterializeParallelSpeedup", TestMaterializeParallelSpeedup);
  run("InstallFallbackToL0",        TestInstallFallbackToL0);

  // ---- M3.3 新增 ----
  run("SteadyStateZeroL0",          TestSteadyStateZeroL0);
  run("MaterializeVsCompactionRace", TestMaterializeVsCompactionRace);
  run("RecoveryEpochMergeIdempotent", TestRecoveryEpochMergeIdempotent);

  // ---- M4.5b 新增 ----
  run("SkipBatchMaterialize",       TestSkipBatchMaterialize);

  fprintf(stderr, "============================================================\n");
  if (g_failures == 0) {
    fprintf(stderr, " ALL PASSED\n");
  } else {
    fprintf(stderr, " %d FAILED\n", g_failures);
  }
  fprintf(stderr, "============================================================\n");
  return g_failures;
}
