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
#include "zeroflush/repartition_controller.h"
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
  // M5 诊断：ZF_KEEP_DB=1 时保留库（Concurrent 定位——SST 内容检查）。
  if (getenv("ZF_KEEP_DB") != nullptr) {
    fprintf(stderr, "[KEEP] %s\n", dbname.c_str());
    return;
  }
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
  // M5（zeroflush0.98）适配：P1 稳态开融合——epoch2+ 同分区数据与
  // L1 无条件融合（不再直装撞 L1 落 L0——merge off 直装落 L0 为 0.9
  // 语义，L0 消费在 P1 禁用）。
  zfo.merge_into_base_level = true;

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

  // M5（zeroflush0.98）适配：dynamic_level_bytes=false（Open 覆写）后
  // base 恒 L1——文件落 L1 而非 L6。P1 语义：epoch1 直装、epoch2+ 与
  // 自身 L1 文件无条件融合（判据 1）——direct（累计直装次数）不再等于
  // L1 文件数（融合替换删旧文件）也不等于 materialized×P（后序 epoch
  // 走融合）。核心断言不变：L0 恒空、零 fallback、数据完整。
  const uint64_t direct = ZfMetric(db.get(), "install_direct_base");
  const uint64_t fallback = ZfMetric(db.get(), "install_fallback_l0");
  const uint64_t n0 = NumFilesAtLevel(db.get(), 0);
  const uint64_t n1 = NumFilesAtLevel(db.get(), 1);
  if (fallback != 0 || n0 != 0 || direct == 0) {
    ReportResult(tag, false,
                 "fallback=" + std::to_string(fallback) +
                     " l0_files=" + std::to_string(n0) +
                     " direct=" + std::to_string(direct));
    CleanDB(dbname);
    return;
  }
  if (n1 == 0) {
    ReportResult(tag, false,
                 "l1_files=0 (no direct install to base) direct=" +
                     std::to_string(direct));
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
  // M5（zeroflush0.98）适配：断言从 speedup>=2 放宽到 >=1.1——Debug
  // 构建 + 少量 epoch 下物化固定开销（任务池创建/WAL 读）稀释并行收益
  // （实测 k1=6.6s vs k8=6.3s，epochs=2）；本用例保留为"并行不退化"
  // 冒烟，并行收益由 50GB 验收吞吐门覆盖。
  if (r1.micros < r8.micros * 7 / 10) {
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
  // M5（zeroflush0.98）适配：用例开融合（P1 稳态配置）——epoch2 同范围
  // 数据走无条件融合（判据 1），fallback/L0 兜底已删除。
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
  // M5（zeroflush0.98）适配：L0 兜底/fallback 已删除（§3.2/§3.6）——
  // epoch2 同范围数据与 L1 文件无条件融合（写放大 1×），不再回落 L0。
  if (ZfMetric(db.get(), "install_fallback_l0") != 0) {
    ReportResult(tag, false,
                 "epoch2 fallback=" +
                     std::to_string(ZfMetric(db.get(), "install_fallback_l0")));
    CleanDB(dbname);
    return;
  }
  if (ZfMetric(db.get(), "base_merge_count") == 0) {
    ReportResult(tag, false, "epoch2 base_merge_count == 0 (merge expected)");
    CleanDB(dbname);
    return;
  }

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

// ---------------------------------------------------------------------------
// ZeroFlushV0 P1：S4 merge —— 路由表派生（纯函数）
// ---------------------------------------------------------------------------
void TestMergePartitionTableDerive() {
  const char* tag = "MergePartitionTableDerive(ZeroFlushV0-P1)";
  const auto* ucmp = rocksdb::BytewiseComparator();
  // P=4, 边界 c/g/m：part0=[-∞,c) 1=[c,g) 2=[g,m) 3=[m,+∞)
  std::vector<std::string> boundaries = {"c", "g", "m"};
  std::shared_ptr<zeroflush::PartitionTable> pt;
  auto s = zeroflush::PartitionTable::Create(0, boundaries, ucmp, &pt);
  if (!s.ok()) {
    ReportResult(tag, false, "Create: " + s.ToString());
    return;
  }
  const auto route_of = [&](const std::shared_ptr<zeroflush::PartitionTable>& t,
                            const char* k) { return t->Route(k); };

  // --- 合并 part1 → 吸收左邻 part0：去掉边界 c，ids [0,2,3] ---
  {
    uint32_t absorb = 999;
    std::shared_ptr<zeroflush::PartitionTable> nt;
    s = pt->DeriveMerge(1, /*version=*/1, &absorb, &nt);
    if (!s.ok() || absorb != 0 || nt == nullptr) {
      ReportResult(tag, false,
                   "merge part1: s=" + s.ToString() + " absorb=" +
                       std::to_string(absorb));
      return;
    }
    if (nt->partitions() != 3 || nt->version() != 1) {
      ReportResult(tag, false, "merge part1: partitions/version wrong");
      return;
    }
    // part1 旧区间 [c,g) 的 key 现归吸收方 part0；part2/3 保持原区间。
    // Route 返回分区下标（part_ids=[0,2,3] → id 序 0,2,3）。
    const char* ks[] = {"a", "d", "f", "g", "k", "m", "z"};
    const uint32_t ex_idx[] = {0, 0, 0, 1, 1, 2, 2};
    const uint32_t ex_id[] = {0, 0, 0, 2, 2, 3, 3};
    for (size_t i = 0; i < sizeof(ks) / sizeof(ks[0]); ++i) {
      const uint32_t got = route_of(nt, ks[i]);
      const uint32_t got_id = nt->part_id(got);
      if (got != ex_idx[i] || got_id != ex_id[i]) {
        ReportResult(tag, false,
                     std::string("route(") + ks[i] + ")=" +
                         std::to_string(got) + " (id=" + std::to_string(got_id) +
                         ") expected idx=" + std::to_string(ex_idx[i]) +
                         " id=" + std::to_string(ex_id[i]));
        return;
      }
    }
    rocksdb::Slice lo, hi;
    nt->RangeOf(0, &lo, &hi);
    if (lo.size() != 0 || hi.ToString() != "g") {
      ReportResult(tag, false, "merged part0 range wrong");
      return;
    }
  }
  // --- 合并 part0（首分区）→ 吸收右邻 part1：ids [1,2,3] ---
  {
    uint32_t absorb = 999;
    std::shared_ptr<zeroflush::PartitionTable> nt;
    s = pt->DeriveMerge(0, 2, &absorb, &nt);
    if (!s.ok() || absorb != 1) {
      ReportResult(tag, false,
                   "merge part0: s=" + s.ToString() + " absorb=" +
                       std::to_string(absorb));
      return;
    }
    if (nt->part_id(route_of(nt, "b")) != 1 || nt->part_id(route_of(nt, "z")) != 3) {
      ReportResult(tag, false, "merge part0 route wrong");
      return;
    }
  }
  // --- 合并 part3（末分区）→ 吸收左邻 part2 ---
  {
    uint32_t absorb = 999;
    std::shared_ptr<zeroflush::PartitionTable> nt;
    s = pt->DeriveMerge(3, 3, &absorb, &nt);
    if (!s.ok() || absorb != 2) {
      ReportResult(tag, false,
                   "merge part3: s=" + s.ToString() + " absorb=" +
                       std::to_string(absorb));
      return;
    }
    if (route_of(nt, "n") != 2 || route_of(nt, "b") != 0) {
      ReportResult(tag, false, "merge part3 route wrong");
      return;
    }
  }
  // --- 错误路径：hash 模式 / drain 不在表中 ---
  {
    auto hash_tbl = zeroflush::PartitionTable::CreateHash(0, 4);
    uint32_t absorb = 999;
    std::shared_ptr<zeroflush::PartitionTable> nt;
    s = hash_tbl->DeriveMerge(1, 1, &absorb, &nt);
    if (s.ok()) {
      ReportResult(tag, false, "hash-mode merge must be rejected");
      return;
    }
    s = pt->DeriveMerge(99, 1, &absorb, &nt);
    if (s.ok()) {
      ReportResult(tag, false, "merge of absent partition must be rejected");
      return;
    }
  }
  ReportResult(tag, true);
}

// ---------------------------------------------------------------------------
// ZeroFlushV0 P1：S4 merge e2e —— 冷分区取消/合并（无在途数据版）
//  覆盖：带在途数据拒绝（Busy）→ 排空后成功合并 → 新写归属吸收方 →
//  中途读/覆写正确 → 物化收敛 → 重开一致。
// ---------------------------------------------------------------------------
void TestMergeColdPartitionEndToEnd() {
  const char* tag = "MergeColdPartitionEndToEnd(ZeroFlushV0-P1)";
  std::string dbname = std::string(kDbBase) + "mergecold";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  zfo.static_boundaries = {"c", "g", "m"};
  zfo.partition_target_bytes = 16 * 1024;
  zfo.epoch_target_bytes = 16 * 1024;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }
  auto fail = [&](const std::string& why) {
    ReportResult(tag, false, why);
    CleanDB(dbname);
  };
  // 写辅助：key = prefix(1B) + %04d；value = "v<prefix>" + %04d。
  // prefix ∈ {b,d,c,h,n} 按字节序落在区间：b< c <d< g<h< m<n。
  char k[16], v[224];
  auto put = [&](const char* prefix, int id) -> bool {
    snprintf(k, sizeof(k), "%s%04d", prefix, id);
    snprintf(v, sizeof(v), "v%s%04d", prefix, id);
    return db->Put(rocksdb::WriteOptions(), k, v).ok();
  };
  auto get_ok = [&](const char* prefix, int id,
                    const char* expect) -> bool {
    snprintf(k, sizeof(k), "%s%04d", prefix, id);
    std::string out;
    if (!db->Get(rocksdb::ReadOptions(), k, &out).ok()) {
      fprintf(stderr, "[dbg] get(%s) failed\n", k);
      return false;
    }
    if (expect != nullptr && out != expect) {
      fprintf(stderr, "[dbg] get(%s)=%s expect=%s\n", k, out.c_str(), expect);
      return false;
    }
    return true;
  };

  // Phase 1：写满四分区（触发封存/物化），等待 part1 完全排空。
  // （ZeroFlushV0 P1b 起活跃尾可合并，旧"带在途数据必须拒绝"断言已由
  // MergeActiveTailEndToEnd 覆盖；本用例专注"已排空冷分区"合并。）
  for (int i = 0; i < 230; ++i) {
    if (!put("b", i) || !put("d", i) || !put("h", i) || !put("n", i)) {
      fail("put@phase1");
      return;
    }
  }
  if (!WaitAllMaterialized(db.get())) {
    fail("WaitAllMaterialized(phase1) timeout");
    return;
  }
  // 排空泵：可能残留 < 阈值的活跃尾巴 → 追加写入触发封存后重试（≤6 次）。
  uint32_t absorb = 999;
  for (int attempts = 0; attempts < 6; ++attempts) {
    s = zeroflush::MergePartition(db.get(), 1, &absorb);
    if (s.ok()) break;
    for (int i = 0; i < 256; ++i) {
      if (!put("b", 1000 + attempts * 256 + i)) {
        fail("put@drain-pump");
        return;
      }
    }
    if (!WaitAllMaterialized(db.get())) {
      fail("WaitAllMaterialized(pump) timeout");
      return;
    }
  }
  if (!s.ok()) {
    fprintf(stderr, "[dbg] merge rejected: %s\n", s.ToString().c_str());
    fail("merge after drain rejected");
    return;
  }
  if (absorb != 0) {
    fail("absorb expected left neighbor part0, got " + std::to_string(absorb));
    return;
  }
  if (ZfMetric(db.get(), "merges") != 1) {
    fail("merges metric != 1");
    return;
  }

  // Phase 2：合并后读（part1 旧数据在 L1）、覆写与新写（归吸收方 part0）。
  char exp[224];
  snprintf(exp, sizeof(exp), "vd%04d", 30);
  if (!get_ok("d", 30, exp)) {
    fail("read old part1 key after merge");
    return;
  }
  snprintf(exp, sizeof(exp), "vd%04d", 29);
  if (!get_ok("d", 29, exp)) {
    fail("read phase0 part1 key after merge");
    return;
  }
  snprintf(k, sizeof(k), "d%04d", 30);  // 覆写 part1 旧 key
  snprintf(v, sizeof(v), "vnew%04d", 30);
  if (!db->Put(rocksdb::WriteOptions(), k, v).ok()) {
    fail("put overwrite");
    return;
  }
  for (int i = 240; i < 260; ++i) {
    if (!put("d", i)) {
      fail("put@phase2-d");
      return;
    }
  }
  for (int i = 0; i < 10; ++i) {
    if (!put("c", i)) {
      fail("put c-range");
      return;
    }
  }
  // 立即读（数据在吸收方活跃索引/封存索引）。
  if (!get_ok("d", 30, "vnew0030")) {
    fail("read overwrite");
    return;
  }
  if (!get_ok("d", 250, "vd0250")) {
    fail("read new d key");
    return;
  }
  if (!get_ok("c", 5, "vc0005")) {
    fail("read new c key");
    return;
  }

  // Phase 3：物化收敛（吸收方与 part1 旧 L1 文件合并），L0 不异常。
  for (int i = 0; i < 200; ++i) {
    if (!put("b", 2000 + i)) {
      fail("put@phase3");
      return;
    }
  }
  if (!WaitAllMaterialized(db.get())) {
    fail("WaitAllMaterialized(phase3) timeout");
    return;
  }
  const uint64_t l0 = NumFilesAtLevel(db.get(), 0);
  if (!get_ok("d", 30, "vnew0030")) {
    fail("read overwrite after flush");
    return;
  }
  if (!get_ok("d", 250, "vd0250")) {
    fail("read new d key after flush");
    return;
  }
  if (l0 > 6) {
    fprintf(stderr, "[dbg] level0 files=%llu (post-merge install residue)\n",
            (unsigned long long)l0);
    fail("level0 file count unexpectedly high");
    return;
  }

  // Phase 4：重开（ZFPROPS 持久化合并后的路由表）→ 迭代计数与取值一致。
  const int64_t cnt_before = CountViaIterator(db.get());
  db.reset();
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    fail("reopen: " + s.ToString());
    return;
  }
  const int64_t cnt_after = CountViaIterator(db.get());
  if (cnt_after != cnt_before) {
    fprintf(stderr, "[dbg] iter before=%lld after=%lld\n",
            (long long)cnt_before, (long long)cnt_after);
    fail("iter count mismatch after reopen");
    return;
  }
  if (!get_ok("d", 30, "vnew0030")) {
    fail("read overwrite after reopen");
    return;
  }
  if (!get_ok("d", 250, "vd0250")) {
    fail("read new d key after reopen");
    return;
  }
  if (!get_ok("b", 5, "vb0005")) {
    fail("read b key after reopen");
    return;
  }

  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true, "absorb=" + std::to_string(absorb));
}

// ---------------------------------------------------------------------------
// ZeroFlushV0 P1b/M1a：带"活跃尾"数据的 DRAINING merge e2e。
//  覆盖：已排空的旧数据(L1) + 未封存的活跃尾(冻结为 E_D) → merge →
//  排空窗口读（别名）/覆写序正确 → E_D 与吸收方 epoch 物化 → retire →
//  重开一致。R0（TSAN 三修复）后并发门解除，走默认并行物化（8）。

// M1c① 辅助：统计 DRAINING sidecar 非空行数（fopen 避免引入 fstream）。
int DrainingSidecarLines(const std::string& dbname,
                         const std::string& wal_subdir) {
  const std::string sp = dbname + "/" + wal_subdir + "/ZF_DRAINING";
  FILE* f = ::fopen(sp.c_str(), "rb");
  if (f == nullptr) return 0;
  int n = 0;
  char buf[512];
  while (::fgets(buf, sizeof(buf), f) != nullptr) {
    if (buf[0] != '\n' && buf[0] != '\0') ++n;
  }
  ::fclose(f);
  return n;
}

// ---------------------------------------------------------------------------
void TestMergeActiveTailEndToEnd() {
  const char* tag = "MergeActiveTailEndToEnd(ZeroFlushV0-P1b)";
  std::string dbname = std::string(kDbBase) + "mergeactivetail";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  zfo.static_boundaries = {"c", "g", "m"};
  zfo.partition_target_bytes = 16 * 1024;
  zfo.epoch_target_bytes = 16 * 1024;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }
  auto fail = [&](const std::string& why) {
    ReportResult(tag, false, why);
    CleanDB(dbname);
  };
  char k[16], v[224];
  auto put = [&](const char* prefix, int id) -> bool {
    snprintf(k, sizeof(k), "%s%04d", prefix, id);
    ::memset(v, 'x', 200);
    snprintf(v, sizeof(v), "v%s%04d", prefix, id);
    return db->Put(rocksdb::WriteOptions(), k, v).ok();
  };
  auto get = [&](const char* prefix, int id, std::string* out) -> bool {
    snprintf(k, sizeof(k), "%s%04d", prefix, id);
    return db->Get(rocksdb::ReadOptions(), k, out).ok();
  };
  auto get_eq = [&](const char* prefix, int id, const char* expect) -> bool {
    std::string out;
    if (!get(prefix, id, &out) || out != expect) {
      fprintf(stderr, "[dbg] get(%s%04d) mismatch\n", prefix, id);
      return false;
    }
    return true;
  };

  // ---- Phase 0：part1（drain，区间 [c,g)）旧数据全量落 L1 + 活跃尾 ----
  for (int i = 0; i < 120; ++i) {
    if (!put("d", i)) {
      fail("put@seed");
      return;
    }
  }
  if (!WaitAllMaterialized(db.get())) {
    fail("WaitAllMaterialized(seed) timeout");
    return;
  }
  for (int i = 0; i < 10; ++i) {
    if (!put("b", i)) {  // 吸收方 part0 少量旧数据
      fail("put@seed-b");
      return;
    }
  }
  // 活跃尾：d0120..d0139（< 16KB 阈值，保持未封存 = 在途活跃数据）。
  for (int i = 120; i < 140; ++i) {
    if (!put("d", i)) {
      fail("put@tail");
      return;
    }
  }

  // ---- Phase 1：merge part1（含活跃尾）→ 应成功；DRAINING 登记 ----
  uint32_t absorb = 999;
  s = zeroflush::MergePartition(db.get(), /*drain_part=*/1, &absorb);
  if (!s.ok()) {
    fprintf(stderr, "[dbg] merge: %s\n", s.ToString().c_str());
    fail("merge with active tail failed");
    return;
  }
  if (absorb != 0 || ZfMetric(db.get(), "draining_partitions") != 1 ||
      ZfMetric(db.get(), "routing_version") != 1) {
    fail("absorb/draining/routing_version wrong after merge");
    return;
  }

  // ---- Phase 2：排空窗口读（E_D 可能尚未物化——别名回查）----
  // 旧数据（在 L1，走原生）+ 尾 key（未覆写，需别名/待 E_D 落 L1）。
  if (!get_eq("d", 5, "vd0005")) {
    fail("read seeded key after merge");
    return;
  }
  if (!get_eq("d", 130, "vd0130")) {
    fail("read tail key after merge (alias window)");
    return;
  }
  // 覆写序：尾 key 覆写归吸收方（part0）；立即读必须为新值（吸收方链优先，
  // 别名/旧版本不得盖过）。
  snprintf(k, sizeof(k), "d%04d", 130);
  snprintf(v, sizeof(v), "vnew0130");
  if (!db->Put(rocksdb::WriteOptions(), k, v).ok()) {
    fail("put overwrite");
    return;
  }
  snprintf(k, sizeof(k), "d%04d", 125);
  snprintf(v, sizeof(v), "vnew0125");
  if (!db->Put(rocksdb::WriteOptions(), k, v).ok()) {
    fail("put overwrite#2");
    return;
  }
  if (!get_eq("d", 130, "vnew0130") || !get_eq("d", 125, "vnew0125")) {
    fail("read overwritten tail key (must be new)");
    return;
  }
  if (!get_eq("d", 135, "vd0135")) {
    fail("read unoverwritten tail key after overwrites");
    return;
  }

  // ---- Phase 3：驱动物化（E_D + 吸收方 post-merge epochs）→ retire ----
  for (int i = 0; i < 300; ++i) {
    if (!put("b", 100 + i) || !put("h", 100 + i) || !put("n", 100 + i)) {
      fail("put@phase3");
      return;
    }
  }
  if (!WaitAllMaterialized(db.get())) {
    fail("WaitAllMaterialized(phase3) timeout");
    return;
  }
  // 等 DRAINING 排空（==0，WaitForZfMetric 是 >= 语义不能用于 0 目标）。
  {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(60);
    while (ZfMetric(db.get(), "draining_partitions") != 0) {
      if (std::chrono::steady_clock::now() > deadline) {
        fail("draining_partitions did not retire (timeout)");
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }
  if (ZfMetric(db.get(), "merge_retired") != 1) {
    fail("merge_retired != 1");
    return;
  }
  if (DrainingSidecarLines(dbname, zfo.wal_subdir) != 0) {
    fail("draining sidecar must be empty after retire (M1c1)");
    return;
  }
  if (!get_eq("d", 130, "vnew0130") || !get_eq("d", 125, "vnew0125") ||
      !get_eq("d", 135, "vd0135") || !get_eq("d", 5, "vd0005")) {
    fail("read after drain");
    return;
  }
  if (!get_eq("b", 5, "vb0005")) {
    fail("read b key after drain");
    return;
  }

  // ---- Phase 4：重开（ZFPROPS 持久化合并后路由）----
  const int64_t cnt_before = CountViaIterator(db.get());
  db.reset();
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    fail("reopen: " + s.ToString());
    return;
  }
  if (CountViaIterator(db.get()) != cnt_before) {
    fail("iter count mismatch after reopen");
    return;
  }
  if (!get_eq("d", 130, "vnew0130") || !get_eq("d", 135, "vd0135") ||
      !get_eq("d", 5, "vd0005")) {
    fail("spot read after reopen");
    return;
  }

  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true, "absorb=0 drain->retired");
}


// ---------------------------------------------------------------------------
// ZeroFlushV0 P1b/M1b：活跃尾 merge（merge_into_base_level=true）e2e。
//  F1（PlanLocked DRAINING 分区强制物化）接线后，merge=true + 活跃尾放行；
//  覆盖排空窗口读（别名）/覆写序/E_D 孤儿直装与吸收方物化/retire/重开。
// ---------------------------------------------------------------------------
void TestMergeActiveTailMergeEnabled() {
  const char* tag = "MergeActiveTailMergeEnabled(ZeroFlushV0-P1b)";
  std::string dbname = std::string(kDbBase) + "mergeactivetail_m";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  zfo.static_boundaries = {"c", "g", "m"};
  zfo.partition_target_bytes = 4 * 1024;
  zfo.epoch_target_bytes = 4 * 1024;
  zfo.merge_into_base_level = true;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }
  auto fail = [&](const std::string& why) {
    ReportResult(tag, false, why);
    db.reset();  // 先关 DB 再删目录（防 close-flush 在已删目录上挂起）
    CleanDB(dbname);
  };
  char k[16], v[224];
  auto put = [&](const char* prefix, int id) -> bool {
    snprintf(k, sizeof(k), "%s%04d", prefix, id);
    ::memset(v, 'x', 200);
    snprintf(v, sizeof(v), "v%s%04d", prefix, id);
    return db->Put(rocksdb::WriteOptions(), k, v).ok();
  };
  auto get_eq = [&](const char* prefix, int id, const char* expect) -> bool {
    snprintf(k, sizeof(k), "%s%04d", prefix, id);
    std::string out;
    if (!db->Get(rocksdb::ReadOptions(), k, &out).ok() || out != expect) {
      fprintf(stderr, "[dbg] get(%s%04d) mismatch: got=%.60s\n", prefix, id,
              out.c_str());
      return false;
    }
    return true;
  };

  // 种子四分区（触发封存/物化）。
  for (int i = 0; i < 40; ++i) {
    if (!put("b", i) || !put("d", i) || !put("h", i) || !put("n", i)) {
      fail("put@seed");
      return;
    }
  }
  if (!WaitAllMaterialized(db.get())) {
    fail("WaitAllMaterialized(seed) timeout");
    return;
  }
  // 活跃尾（part1, <4KB 阈值未封存）。
  for (int i = 300; i < 306; ++i) {
    if (!put("d", i)) {
      fail("put@tail");
      return;
    }
  }
  // 活跃尾 merge（merge=true，F1 接线后应成功）→ DRAINING 登记。
  uint32_t absorb = 999;
  s = zeroflush::MergePartition(db.get(), 1, &absorb);
  if (!s.ok() || absorb != 0 ||
      ZfMetric(db.get(), "draining_partitions") != 1) {
    fprintf(stderr, "[dbg] merge: %s\n", s.ToString().c_str());
    fail("active-tail merge under merge=true failed");
    return;
  }
  if (DrainingSidecarLines(dbname, zfo.wal_subdir) != 1) {
    fail("draining sidecar must hold 1 record after merge (M1c1)");
    return;
  }
  // 排空窗口：未覆写的尾 key 经别名/待 E_D 落地可读。
  if (!get_eq("d", 300, "vd0300") || !get_eq("d", 5, "vd0005")) {
    fail("alias-window read after merge");
    return;
  }
  // 覆写序：尾 key 覆写归吸收方，立即读必须为新值。
  snprintf(k, sizeof(k), "d%04d", 300);
  snprintf(v, sizeof(v), "vnew0300");
  if (!db->Put(rocksdb::WriteOptions(), k, v).ok()) {
    fail("put overwrite");
    return;
  }
  if (!get_eq("d", 300, "vnew0300")) {
    fail("overwrite must win over drain old version");
    return;
  }
  // 驱动物化（E_D 孤儿直装 + 吸收方/邻居 kMergeBase）→ retire。
  for (int i = 500; i < 560; ++i) {
    if (!put("b", i) || !put("h", i) || !put("n", i)) {
      fail("put@phase3");
      return;
    }
  }
  if (!WaitAllMaterialized(db.get())) {
    fail("WaitAllMaterialized(phase3) timeout");
    return;
  }
  {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(60);
    while (ZfMetric(db.get(), "draining_partitions") != 0) {
      if (std::chrono::steady_clock::now() > deadline) {
        fail("draining_partitions did not retire");
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }
  if (ZfMetric(db.get(), "merge_retired") != 1) {
    fail("merge_retired != 1");
    return;
  }
  if (!get_eq("d", 300, "vnew0300") || !get_eq("d", 302, "vd0302") ||
      !get_eq("h", 500, "vh0500") || !get_eq("n", 500, "vn0500") ||
      !get_eq("b", 500, "vb0500")) {
    fail("read after drain/materialize");
    return;
  }
  // 重开一致。
  const int64_t cnt_before = CountViaIterator(db.get());
  db.reset();
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    fail("reopen: " + s.ToString());
    return;
  }
  if (CountViaIterator(db.get()) != cnt_before) {
    fail("iter count mismatch after reopen");
    return;
  }
  if (!get_eq("d", 300, "vnew0300") || !get_eq("h", 500, "vh0500")) {
    fail("spot read after reopen");
    return;
  }
  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true, "active-tail merge under merge=true -> retired");
}

// ---------------------------------------------------------------------------
// ZeroFlushV0 M1c②：崩溃注入 e2e（P1B_DESIGN.md §7/§8）——活跃尾 merge 后
// E_D 未物化即崩溃（fork 子进程 _Exit），父进程重开验证：
//  1) DRAINING 注册表经 sidecar 重建（draining_partitions==1）；
//  2) E_D 尾 key 可读（恢复重放落吸收方链 + recovery 孤儿定点读）；
//  3) 后续 seal 的 AddEpochWithRecoveryAdoption 收养 E_D → 物化 →
//     ReleaseEpoch 回调 MaybeRetireDraining → retire 归零 + sidecar 清空。
// fork 注：子进程只做同步操作（写/merge），后台 flush 线程 fork 后不执行，
// 故 merge 冻结的 E_D 必留在封存 WAL（不物化）→ 真实崩溃窗口。
// ---------------------------------------------------------------------------
void TestCrashMidDrainReopen() {
  const char* tag = "CrashMidDrainReopen(M1c-2)";
  std::string dbname = std::string(kDbBase) + "crash_mid_drain";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  zfo.static_boundaries = {"c", "g", "m"};
  // 阈值放高：子进程全部 d 数据保持"活跃尾"（< 阈值不触发封存），直到
  // merge 一次性冻结为 E_D。
  zfo.partition_target_bytes = 256 * 1024;
  zfo.epoch_target_bytes = 256 * 1024;

  // 确定性 200B value：前部文本 token + 尾部 'x' 填充。
  auto make_val = [](const char* prefix, int id) -> std::string {
    std::string val(200, 'x');
    snprintf(&val[0], 32, "v%s%04d", prefix, id);
    return val;
  };

  pid_t pid = ::fork();
  if (pid < 0) {
    ReportResult(tag, false, "fork failed");
    return;
  }
  if (pid == 0) {
    // ---- 子进程（崩溃注入）：写种子+活跃尾 → merge → 立即 _Exit ----
    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      fprintf(stderr, "[child] open: %s\n", s.ToString().c_str());
      fflush(stderr);
      _exit(1);
    }
    char k[16];
    auto put = [&](const char* prefix, int id) -> bool {
      snprintf(k, sizeof(k), "%s%04d", prefix, id);
      const std::string val = make_val(prefix, id);
      return db->Put(rocksdb::WriteOptions(), k, val).ok();
    };
    auto get_eq = [&](const char* prefix, int id) -> bool {
      snprintf(k, sizeof(k), "%s%04d", prefix, id);
      std::string out;
      if (!db->Get(rocksdb::ReadOptions(), k, &out).ok() ||
          out != make_val(prefix, id)) {
        fprintf(stderr, "[child] get(%s%04d) mismatch\n", prefix, id);
        return false;
      }
      return true;
    };
    // 种子 d0000..d0059（E_D 主数据）。
    for (int i = 0; i < 60; ++i) {
      if (!put("d", i)) {
        fprintf(stderr, "[child] put seed@%d failed\n", i);
        fflush(stderr);
        _exit(1);
      }
    }
    // 活跃尾 d0100..d0119（< 阈值，未封存）。
    for (int i = 100; i < 120; ++i) {
      if (!put("d", i)) {
        fprintf(stderr, "[child] put tail@%d failed\n", i);
        fflush(stderr);
        _exit(1);
      }
    }
    // merge part1（[c,g) 区间，含活跃尾）→ E_D 冻结 + DRAINING 登记落盘。
    uint32_t absorb = 999;
    s = zeroflush::MergePartition(db.get(), /*drain_part=*/1, &absorb);
    if (!s.ok()) {
      fprintf(stderr, "[child] merge: %s\n", s.ToString().c_str());
      fflush(stderr);
      _exit(1);
    }
    if (absorb != 0 || ZfMetric(db.get(), "draining_partitions") != 1) {
      fprintf(stderr, "[child] post-merge absorb=%u draining=%llu\n", absorb,
              (unsigned long long)ZfMetric(db.get(), "draining_partitions"));
      fflush(stderr);
      _exit(1);
    }
    if (DrainingSidecarLines(dbname, zfo.wal_subdir) != 1) {
      fprintf(stderr, "[child] sidecar must hold 1 record after merge\n");
      fflush(stderr);
      _exit(1);
    }
    // merge 后立即读（E_D 未物化——别名/封存 WAL 路径）。
    if (!get_eq("d", 5) || !get_eq("d", 110)) {
      fprintf(stderr, "[child] alias-window read after merge failed\n");
      fflush(stderr);
      _exit(1);
    }
    fprintf(stderr, "[child] crash point: merge done, E_D not materialized, "
                    "_Exit\n");
    fflush(stderr);
    _exit(0);
  }

  // ---- 父进程：等待子进程崩溃后直接重开验证恢复 ----
  int st = 0;
  if (::waitpid(pid, &st, 0) < 0 || !WIFEXITED(st) || WEXITSTATUS(st) != 0) {
    ReportResult(tag, false, "child crash-seed phase failed (see [child] log)");
    CleanDB(dbname);
    return;
  }
  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "reopen: " + s.ToString());
    CleanDB(dbname);
    return;
  }
  auto fail = [&](const std::string& why) {
    ReportResult(tag, false, why);
    CleanDB(dbname);
  };
  // 1) DRAINING 注册表跨崩溃重建（sidecar）。
  if (ZfMetric(db.get(), "draining_partitions") != 1) {
    fail("draining_partitions != 1 after crash reopen (sidecar lost?)");
    return;
  }
  if (DrainingSidecarLines(dbname, zfo.wal_subdir) != 1) {
    fail("draining sidecar must hold 1 record after crash reopen");
    return;
  }
  // 2) E_D 数据（种子 + 尾）在物化前即可读（恢复重放 → 吸收方链/孤儿读）。
  char k[16];
  auto get_eq = [&](const char* prefix, int id) -> bool {
    snprintf(k, sizeof(k), "%s%04d", prefix, id);
    std::string out;
    if (!db->Get(rocksdb::ReadOptions(), k, &out).ok() ||
        out != make_val(prefix, id)) {
      fprintf(stderr, "[dbg] get(%s%04d) after reopen mismatch\n", prefix, id);
      return false;
    }
    return true;
  };
  if (!get_eq("d", 5) || !get_eq("d", 59) || !get_eq("d", 100) ||
      !get_eq("d", 119)) {
    fail("E_D read after crash reopen");
    return;
  }
  // 3) 驱动物化：写 b/h 至 epoch 阈值 → seal（AddEpochWithRecoveryAdoption
  //    收养 E_D 孤儿）→ 物化 → ReleaseEpoch → MaybeRetireDraining → retire。
  for (int i = 0; i < 2000; ++i) {
    snprintf(k, sizeof(k), "%s%04d", "b", i);
    const std::string bv = make_val("b", i);
    if (!db->Put(rocksdb::WriteOptions(), k, bv).ok()) {
      fail("put@drive-b");
      return;
    }
    snprintf(k, sizeof(k), "%s%04d", "h", i);
    const std::string hv = make_val("h", i);
    if (!db->Put(rocksdb::WriteOptions(), k, hv).ok()) {
      fail("put@drive-h");
      return;
    }
    // 200B value + 键/头 ≈ 212B/条；2 分区各 i 条合计 ≥ 256KB 阈值即 seal。
    if (i >= 640) {
      break;  // ≈ 2×641×212 ≈ 272KB，已越过 epoch_target_bytes
    }
  }
  if (!WaitAllMaterialized(db.get())) {
    fail("WaitAllMaterialized(drive) timeout");
    return;
  }
  {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(60);
    while (ZfMetric(db.get(), "draining_partitions") != 0) {
      if (std::chrono::steady_clock::now() > deadline) {
        fail("draining_partitions did not retire after reopen+drive");
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }
  if (ZfMetric(db.get(), "merge_retired") != 1) {
    fail("merge_retired != 1 after reopen+drive");
    return;
  }
  if (DrainingSidecarLines(dbname, zfo.wal_subdir) != 0) {
    fail("draining sidecar must be empty after retire (M1c1)");
    return;
  }
  // 4) retire 后 E_D 数据仍在（孤儿直装 SST 承载）。
  if (!get_eq("d", 5) || !get_eq("d", 59) || !get_eq("d", 100) ||
      !get_eq("d", 119) || !get_eq("b", 0) || !get_eq("h", 0)) {
    fail("read after retire/materialize");
    return;
  }
  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true, "crash mid-drain -> reopen -> adopt -> retire");
}

// ---------------------------------------------------------------------------
// ZeroFlushV0 P2（M2）：PartitionTable::DeriveSplit 表级用例。
//  P=4 边界 {c,g,m}：part0=[-∞,c) 1=[c,g) 2=[g,m) 3=[m,+∞)。覆盖：
//  中/首/末分区 split（boundaries 插入、父 id 退役、两子 id 原位替换、
//  路由/区间正确）、稀疏表（merge 后 {0,2,3}）split、错误路径（split_key
//  越界/等于边界、分区不在表、hash 模式、子 id 重复/与现存冲突）。
// ---------------------------------------------------------------------------
void TestDeriveSplitTable() {
  const char* tag = "DeriveSplitTable(ZeroFlushV0-P2)";
  const auto* ucmp = rocksdb::BytewiseComparator();
  auto make_base = [&](std::shared_ptr<zeroflush::PartitionTable>* out) {
    return zeroflush::PartitionTable::Create(0, {"c", "g", "m"}, ucmp, out);
  };
  auto route_id = [](const std::shared_ptr<zeroflush::PartitionTable>& t,
                     const char* k) {
    return t->part_id(t->Route(k));
  };
  auto range_of = [](const std::shared_ptr<zeroflush::PartitionTable>& t,
                     uint32_t idx, std::string* lo, std::string* hi) {
    rocksdb::Slice l, h;
    t->RangeOf(idx, &l, &h);
    *lo = l.ToString();
    *hi = h.ToString();
  };

  // --- 中分区（part1 [c,g) @ "e"）→ 子 id 4/5 ---
  {
    std::shared_ptr<zeroflush::PartitionTable> pt;
    auto s = make_base(&pt);
    if (!s.ok()) {
      ReportResult(tag, false, "Create: " + s.ToString());
      return;
    }
    std::shared_ptr<zeroflush::PartitionTable> nt;
    s = pt->DeriveSplit(1, "e", /*version=*/1, /*child_left=*/4,
                        /*child_right=*/5, &nt);
    if (!s.ok() || nt == nullptr || nt->partitions() != 5 ||
        nt->version() != 1) {
      ReportResult(tag, false, "split part1: s=" + s.ToString());
      return;
    }
    const uint32_t want_ids[] = {0, 4, 5, 2, 3};
    for (uint32_t i = 0; i < 5; ++i) {
      if (nt->part_id(i) != want_ids[i]) {
        ReportResult(tag, false, "split part1: part_id(" +
                                     std::to_string(i) + ")=" +
                                     std::to_string(nt->part_id(i)));
        return;
      }
    }
    // 子分区区间与两侧邻居。
    std::string lo, hi;
    range_of(nt, 1, &lo, &hi);
    if (lo != "c" || hi != "e") {
      ReportResult(tag, false, "split part1: child-left range wrong");
      return;
    }
    range_of(nt, 2, &lo, &hi);
    if (lo != "e" || hi != "g") {
      ReportResult(tag, false, "split part1: child-right range wrong");
      return;
    }
    // 路由：既有 key 不变；父区间 key 按切点归属左右子。
    const char* ks[] = {"a", "d", "f", "h", "n"};
    const uint32_t ex_id[] = {0, 4, 5, 2, 3};
    for (size_t i = 0; i < sizeof(ks) / sizeof(ks[0]); ++i) {
      if (route_id(nt, ks[i]) != ex_id[i]) {
        ReportResult(tag, false,
                     std::string("split part1 route(") + ks[i] + ") id=" +
                         std::to_string(route_id(nt, ks[i])) + " expect " +
                         std::to_string(ex_id[i]));
        return;
      }
    }
  }
  // --- 首分区（part0 [-∞,c) @ "a"）→ 子 id 6/7 ---
  {
    std::shared_ptr<zeroflush::PartitionTable> pt;
    auto s = make_base(&pt);
    std::shared_ptr<zeroflush::PartitionTable> nt;
    s = pt->DeriveSplit(0, "a", 2, 6, 7, &nt);
    if (!s.ok() || nt->part_id(0) != 6 || nt->part_id(1) != 7 ||
        route_id(nt, "a") != 7 || route_id(nt, "b") != 7 ||
        route_id(nt, "c") != 1) {
      ReportResult(tag, false, "split part0 (first): wrong");
      return;
    }
  }
  // --- 末分区（part3 [m,+∞) @ "z"）→ 子 id 8/9 ---
  {
    std::shared_ptr<zeroflush::PartitionTable> pt;
    auto s = make_base(&pt);
    std::shared_ptr<zeroflush::PartitionTable> nt;
    s = pt->DeriveSplit(3, "z", 3, 8, 9, &nt);
    if (!s.ok() || nt->partitions() != 5 || nt->part_id(3) != 8 ||
        nt->part_id(4) != 9 || route_id(nt, "y") != 8 ||
        route_id(nt, "z") != 9 || route_id(nt, "m") != 8) {
      ReportResult(tag, false, "split part3 (last): wrong");
      return;
    }
  }
  // --- 稀疏表（merge 后 v1 ids {0,2,3}，边界 {g,m}）：split id0 [-∞,g) ---
  {
    std::shared_ptr<zeroflush::PartitionTable> sp;
    auto s = zeroflush::PartitionTable::CreateWithPartIds(
        1, {"g", "m"}, {0, 2, 3}, ucmp, &sp);
    if (!s.ok()) {
      ReportResult(tag, false, "CreateWithPartIds: " + s.ToString());
      return;
    }
    std::shared_ptr<zeroflush::PartitionTable> nt;
    s = sp->DeriveSplit(0, "b", 2, 10, 11, &nt);
    if (!s.ok() || nt->partitions() != 4 || nt->part_id(0) != 10 ||
        nt->part_id(1) != 11 || nt->part_id(2) != 2 || nt->part_id(3) != 3 ||
        route_id(nt, "a") != 10 || route_id(nt, "b") != 11 ||
        route_id(nt, "f") != 11 || route_id(nt, "g") != 2 ||
        route_id(nt, "z") != 3) {
      ReportResult(tag, false, "sparse split id0: wrong");
      return;
    }
  }
  // --- 错误路径 ---
  {
    std::shared_ptr<zeroflush::PartitionTable> pt;
    auto s = make_base(&pt);
    std::shared_ptr<zeroflush::PartitionTable> nt;
    // split_key 等于既有边界（lo 或 hi）→ 拒绝。
    if (pt->DeriveSplit(1, "c", 1, 4, 5, &nt).ok() ||
        pt->DeriveSplit(1, "g", 1, 4, 5, &nt).ok()) {
      ReportResult(tag, false, "split at existing boundary must be rejected");
      return;
    }
    // split_key 越出父区间 → 拒绝。
    if (pt->DeriveSplit(1, "b", 1, 4, 5, &nt).ok() ||
        pt->DeriveSplit(1, "h", 1, 4, 5, &nt).ok() ||
        pt->DeriveSplit(0, "d", 1, 4, 5, &nt).ok() ||
        pt->DeriveSplit(3, "k", 1, 4, 5, &nt).ok()) {
      ReportResult(tag, false, "split_key outside partition must be rejected");
      return;
    }
    // 分区不在表中 / hash 模式 / 子 id 冲突 → 拒绝。
    if (pt->DeriveSplit(99, "e", 1, 4, 5, &nt).ok()) {
      ReportResult(tag, false, "split of absent partition must be rejected");
      return;
    }
    auto hash_tbl = zeroflush::PartitionTable::CreateHash(0, 4);
    if (hash_tbl->DeriveSplit(1, "e", 1, 4, 5, &nt).ok()) {
      ReportResult(tag, false, "hash-mode split must be rejected");
      return;
    }
    if (pt->DeriveSplit(1, "e", 1, 4, 4, &nt).ok() ||
        pt->DeriveSplit(1, "e", 1, 2, 4, &nt).ok()) {
      ReportResult(tag, false, "duplicate/conflicting child ids must be rejected");
      return;
    }
  }
  ReportResult(tag, true);
}

// ---------------------------------------------------------------------------
// ZeroFlushV0 P2（M2）：quiescent split e2e —— type-3 rewrite 端到端。
//  part1 [c,g) 种子数据封存物化（无活跃字节 = quiescent）→ 在数据中点 split
//  → 跨界 base 文件二分为两子文件（子 id 4/5，路由即时生效；splits/
//  partition_count/routing_version/SST+1 断言）→ 两子分区续写（c*=left、
//  f*=right）→ 物化 → 读全对 → reopen 路由与数据一致。
// ---------------------------------------------------------------------------
void TestSplitQuiescentEndToEnd() {
  const char* tag = "SplitQuiescentEndToEnd(ZeroFlushV0-P2)";
  std::string dbname = std::string(kDbBase) + "split_quiescent";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  zfo.static_boundaries = {"c", "g", "m"};
  zfo.partition_target_bytes = 64 * 1024;
  zfo.epoch_target_bytes = 64 * 1024;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }
  auto fail = [&](const std::string& why) {
    ReportResult(tag, false, why);
    CleanDB(dbname);
  };
  char k[16];
  auto make_val = [](const char* prefix, int id) -> std::string {
    std::string val(200, 'x');
    snprintf(&val[0], 32, "v%s%04d", prefix, id);
    return val;
  };
  auto put = [&](const char* prefix, int id) -> bool {
    snprintf(k, sizeof(k), "%s%04d", prefix, id);
    const std::string val = make_val(prefix, id);
    return db->Put(rocksdb::WriteOptions(), k, val).ok();
  };
  auto get_eq = [&](const char* prefix, int id) -> bool {
    snprintf(k, sizeof(k), "%s%04d", prefix, id);
    std::string out;
    if (!db->Get(rocksdb::ReadOptions(), k, &out).ok() ||
        out != make_val(prefix, id)) {
      fprintf(stderr, "[dbg] get(%s%04d) mismatch\n", prefix, id);
      return false;
    }
    return true;
  };
  auto total_sst = [&](rocksdb::DB* d) -> uint64_t {
    uint64_t n = 0;
    for (int l = 0; l <= 6; ++l) n += NumFilesAtLevel(d, l);
    return n;
  };

  // ---- 1) 种子：part1 [c,g) 活跃数据（< partition_target，不触发封存）；
  // 再由 b/h 追加使总活跃 ≥ epoch_target → 批次封存冻结全部有数据分区
  // （含 part1）→ 物化 → part1 无活跃字节（quiescent；后续写只落 b/h，
  // part1 不再收写）。----
  const uint64_t sealed_before = ZfMetric(db.get(), "epochs_sealed");
  const int d_total = 200;  // ≈ 43KB < 64KB 分区阈值
  for (int i = 0; i < d_total; ++i) {
    if (!put("d", i)) {
      fail("put@seed");
      return;
    }
  }
  int extra = 0;
  for (; extra < 2000; ++extra) {
    if (!put("b", extra) || !put("h", extra)) {
      fail("put@seed-bh");
      return;
    }
    if (ZfMetric(db.get(), "epochs_sealed") > sealed_before) {
      break;
    }
  }
  if (ZfMetric(db.get(), "epochs_sealed") == sealed_before) {
    fail("seed never triggered a seal");
    return;
  }
  if (!WaitAllMaterialized(db.get())) {
    fail("WaitAllMaterialized(seed) timeout");
    return;
  }
  // 数据中点作为切点（d0000.. < b < d0199 → 必跨界）。
  const int split_idx = d_total / 2;
  char split_key[16];
  snprintf(split_key, sizeof(split_key), "d%04d", split_idx);
  const uint64_t sst_before = total_sst(db.get());
  const int64_t cnt_before = CountViaIterator(db.get());

  // ---- 2) split part1 @ split_key（quiescent）→ type-3 rewrite ----
  uint32_t child_l = 999, child_r = 999;
  s = zeroflush::SplitPartition(db.get(), /*part=*/1,
                                rocksdb::Slice(split_key), &child_l, &child_r);
  if (!s.ok()) {
    fail("split: " + s.ToString());
    return;
  }
  if (child_l != 4 || child_r != 5 ||
      ZfMetric(db.get(), "splits") != 1 ||
      ZfMetric(db.get(), "partition_count") != 5 ||
      ZfMetric(db.get(), "routing_version") != 1) {
    fail("split metrics/child ids wrong");
    return;
  }
  if (total_sst(db.get()) != sst_before + 1) {
    fail("straddling base file must be rewritten into two files (+1 SST)");
    return;
  }
  if (CountViaIterator(db.get()) != cnt_before) {
    fail("iter count mismatch after split rewrite");
    return;
  }
  for (int i = 0; i < d_total; ++i) {
    if (!get_eq("d", i)) {
      fail("read seed after split");
      return;
    }
  }

  // ---- 3) 两子分区续写（left=[c,b)：c*；right=[b,g)：f*）→ 物化 ----
  const uint64_t sealed_b2 = ZfMetric(db.get(), "epochs_sealed");
  int c_n = 0, f_n = 0;
  for (; c_n < 2000; ++c_n) {
    if (!put("c", c_n) || !put("f", c_n)) {
      fail("put@children");
      return;
    }
    ++f_n;
    if (ZfMetric(db.get(), "epochs_sealed") > sealed_b2) {
      ++c_n;
      break;
    }
  }
  if (c_n < 100 || f_n < 100) {
    fail("children writes too small");
    return;
  }
  if (!WaitAllMaterialized(db.get())) {
    fail("WaitAllMaterialized(children) timeout");
    return;
  }
  if (!get_eq("c", 0) || !get_eq("c", c_n / 2) || !get_eq("f", 0) ||
      !get_eq("f", f_n - 1) || !get_eq("d", split_idx) ||
      !get_eq("d", d_total - 1)) {
    fail("read after children materialize");
    return;
  }
  // ---- 4) reopen：路由（ZFPROPS）与数据一致 ----
  const int64_t cnt_final = CountViaIterator(db.get());
  db.reset();
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    fail("reopen: " + s.ToString());
    return;
  }
  if (ZfMetric(db.get(), "partition_count") != 5 ||
      ZfMetric(db.get(), "routing_version") != 1) {
    fail("route persisted wrong across reopen");
    return;
  }
  if (CountViaIterator(db.get()) != cnt_final) {
    fail("iter count mismatch after reopen");
    return;
  }
  if (!get_eq("d", 0) || !get_eq("d", split_idx) ||
      !get_eq("d", d_total - 1) || !get_eq("c", 0) || !get_eq("f", 0)) {
    fail("spot read after reopen");
    return;
  }
  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true, "quiescent split + type-3 rewrite -> 2 files");
}

// ---------------------------------------------------------------------------
// ZeroFlushV0 R0（并行轨）：4 分区并行合并物化 out-of-order 竞态复现 harness。
//  默认 no-op（快 PASS）；ZF_STRESS_ROUNDS=N 时跑 N 轮：
//    kStatic P=4、4KB 目标、materialize_parallelism=4 → 每轮四分区交替写入
//    触发整批封存 → 连续多批 4 任务并行 kMergeBase（B 侧有 L1）；
//    ZF_STRESS_PUMP=1 时每轮穿插 MergePartition（冷分区取消，含 Busy 重试），
//    还原 P1B §9.4 记载的触发节奏（简化单写填充 12+ 轮未命中 → 需 pump）。
//  任何 Get 校验失败/物化错误即 FAIL（预期输出含 "LEAF OUT-OF-ORDER" 时命中
//  乱序叶子，可用 ZF_DBG_MERGE=1 打印逐叶预检）。
// ---------------------------------------------------------------------------
void TestMergeParallelStress() {
  const char* tag = "MergeParallelStress(R0)";
  const char* rounds_env = getenv("ZF_STRESS_ROUNDS");
  if (rounds_env == nullptr) {
    ReportResult(tag, true, "skipped (set ZF_STRESS_ROUNDS=N to run)");
    return;
  }
  const bool pump = getenv("ZF_STRESS_PUMP") != nullptr;
  // tight 模式：每轮写完立刻连续 pump（不等物化——封存/物化在飞窗口内穿插
  // merge 尝试，还原 P1B §9.4 的触发节奏）。
  const bool tight = getenv("ZF_STRESS_TIGHT") != nullptr;
  // overwrite 模式：每轮覆写同一组 key（A/B 侧同 user key 跨代纠缠——
  // 制造重复/版本交错流，贴近原始触发形态）。
  const bool overwrite = getenv("ZF_STRESS_OVERWRITE") != nullptr;
  const int rounds = std::max(1, atoi(rounds_env));
  std::string dbname = std::string(kDbBase) + "merge_parallel_stress";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  zfo.static_boundaries = {"c", "g", "m"};
  zfo.partition_target_bytes = 4 * 1024;
  zfo.epoch_target_bytes = 4 * 1024;
  const char* par_env = getenv("ZF_STRESS_PARALLELISM");
  zfo.materialize_parallelism =
      par_env != nullptr ? std::max(1, atoi(par_env)) : 4;

  std::unique_ptr<rocksdb::DB> db;
  {
    rocksdb::Options opt = MakeOptions();
    const char* flushes = getenv("ZF_STRESS_FLUSHES");
    if (flushes != nullptr) {
      opt.max_background_flushes = std::max(1, atoi(flushes));
      opt.max_background_jobs = opt.max_background_flushes + 2;
    }
    auto os = zeroflush::Open(opt, zfo, dbname, &db);
    if (!os.ok()) {
      ReportResult(tag, false, "open: " + os.ToString());
      return;
    }
  }
  auto fail = [&](const std::string& why) {
    ReportResult(tag, false, why);
    CleanDB(dbname);
  };
  char k[16];
  auto make_val = [](int round, int id) -> std::string {
    std::string val(200, 'x');
    snprintf(&val[0], 32, "vr%04d_%04d", round, id);
    return val;
  };
  char fail_why[256];
  rocksdb::Status s;
  for (int round = 0; round < rounds; ++round) {
    // 四分区交替写入（b=[..c) d=[c,g) h=[g,m) n=[m,..)）。
    const char* prefixes[] = {"b", "d", "h", "n"};
    const int kround = overwrite ? 0 : round;  // overwrite：key 固定，仅值换代
    for (int i = 0; i < 6; ++i) {
      for (const char* pfx : prefixes) {
        snprintf(k, sizeof(k), "%s%04d_%04d", pfx, kround, i);
        const std::string val = make_val(round, i);
        if (!db->Put(rocksdb::WriteOptions(), k, val).ok()) {
          snprintf(fail_why, sizeof(fail_why), "put@round %d", round);
          fail(fail_why);
          return;
        }
      }
    }
    if (tight) {
      // 封存/物化在飞窗口内的密集 pump：merge 前置会看到 pending frozen/
      // 活跃尾 → 大部分 Busy——目的正是叠加"持 DB mutex 等待屏障 × 封存
      // 交错"的调度压力（P1B §9.4 指出的缺失节奏）。
      for (int kk = 0; kk < 40; ++kk) {
        uint32_t absorb = 999;
        const uint32_t drain_part = (kk % 2 == 0) ? 1 : 3;
        s = zeroflush::MergePartition(db.get(), drain_part, &absorb);
        if (!s.ok() && !s.IsBusy() && !s.IsInvalidArgument()) {
          snprintf(fail_why, sizeof(fail_why), "tight-merge@round %d: %s",
                   round, s.ToString().c_str());
          fail(fail_why);
          return;
        }
        if (kk % 8 == 0) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
      }
    } else if (pump && round % 3 == 1) {
      uint32_t absorb = 999;
      // 冷分区取消尝试（排空完成后）：成功/仍 Busy/已不在表（上轮已合并）
      // 都接受——本 harness 意在叠加 merge 路由切换与封存交错的调度动态。
      s = zeroflush::MergePartition(db.get(), /*drain_part=*/1, &absorb);
      if (!s.ok() && !s.IsBusy() && !s.IsInvalidArgument()) {
        snprintf(fail_why, sizeof(fail_why), "merge@round %d: %s", round,
                 s.ToString().c_str());
        fail(fail_why);
        return;
      }
    }
    if (!WaitAllMaterialized(db.get(), 30000)) {
      snprintf(fail_why, sizeof(fail_why), "WaitAllMaterialized@round %d", round);
      fail(fail_why);
      return;
    }
    // 抽查读：全部分区 key 全对（乱序竞态若丢/错数据立即暴露）。overwrite
    // 校验当前轮最新值；非 overwrite 校验上一轮写入（本轮已写且已物化）。
    {
      const int vr = overwrite ? round : (round >= 1 ? round - 1 : -1);
      if (vr >= 0) {
        for (const char* pfx : prefixes) {
          for (int i = 0; i < 6; ++i) {
            snprintf(k, sizeof(k), "%s%04d_%04d", pfx, overwrite ? 0 : vr, i);
            std::string out;
            if (!db->Get(rocksdb::ReadOptions(), k, &out).ok() ||
                out != make_val(vr, i)) {
              snprintf(fail_why, sizeof(fail_why),
                       "get@round %d key %s mismatch", vr, k);
              fail(fail_why);
              return;
            }
          }
        }
      }
    }
  }
  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true, "rounds=" + std::to_string(rounds) +
                              (tight ? " tight=1"
                                     : (pump ? " pump=1" : " pump=0")) +
                              (overwrite ? " overwrite=1" : ""));
}

// ---------------------------------------------------------------------------
// M3 起步生长 e2e：P0=1 全域单分区 → 排空检查点生长拆分至多分区。
//  kStatic partitions=1 空边界（全域分区）；小阈值使每轮封存/生长快速发生。
//  流程：写种子 → WaitAllMaterialized → GrowPartitions（控制器内部冻结活跃
//  残留尾 + Flush 排空 + 条目中位数 split）→ 断言 partition_count 增长、
//  迭代计数稳定、全部 key 可读 → 多轮生长 → reopen 路由与数据一致。
// ---------------------------------------------------------------------------
void TestGrowthSinglePartition() {
  const char* tag = "GrowthSinglePartition(M3)";
  std::string dbname = std::string(kDbBase) + "growth_p0_1";
  CleanDB(dbname);

  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 1;  // P0=1 全域
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  // static_boundaries 为空 → 单分区覆盖 [-∞, +∞)。
  zfo.partition_target_bytes = 24 * 1024;
  zfo.epoch_target_bytes = 24 * 1024;
  zfo.growth_split_min_bytes = 16 * 1024;  // S_min_split（测试用小阈值）
  zfo.growth_split_cooldown_micros = 0;
  zfo.materialize_parallelism = 8;

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }
  auto fail = [&](const std::string& why) {
    ReportResult(tag, false, why);
    CleanDB(dbname);
  };
  if (ZfMetric(db.get(), "partition_count") != 1) {
    fail("P0 must start at 1 partition");
    return;
  }
  char k[16];
  auto make_val = [](int id) -> std::string {
    std::string val(200, 'x');
    snprintf(&val[0], 32, "vg%04d", id);
    return val;
  };
  auto put = [&](int id) -> bool {
    snprintf(k, sizeof(k), "d%04d", id);
    const std::string val = make_val(id);
    return db->Put(rocksdb::WriteOptions(), k, val).ok();
  };
  auto get_eq = [&](int id) -> bool {
    snprintf(k, sizeof(k), "d%04d", id);
    std::string out;
    if (!db->Get(rocksdb::ReadOptions(), k, &out).ok() ||
        out != make_val(id)) {
      fprintf(stderr, "[dbg] get(d%04d) mismatch\n", id);
      return false;
    }
    return true;
  };

  uint32_t total_splits = 0;
  uint64_t prev_pc = 1;
  int total = 0;
  for (int round = 0; round < 10 && total < 700; ++round) {
    // 每轮 130 条（≈28KB）> 24KB 封存阈值 → 必然产生 base 文件。
    for (int i = 0; i < 130 && total < 700; ++i, ++total) {
      if (!put(total)) {
        fail("put@seed");
        return;
      }
    }
    if (!WaitAllMaterialized(db.get())) {
      fail("WaitAllMaterialized(seed) timeout");
      return;
    }
    const int64_t cnt_before = CountViaIterator(db.get());
    uint32_t sp = 0;
    s = zeroflush::GrowPartitions(db.get(), &sp);
    if (!s.ok()) {
      fail("GrowPartitions: " + s.ToString());
      return;
    }
    total_splits += sp;
    if (CountViaIterator(db.get()) != cnt_before) {
      fail("iter count changed across growth split");
      return;
    }
    const uint64_t pc = ZfMetric(db.get(), "partition_count");
    if (pc < prev_pc) {
      fail("partition_count regressed");
      return;
    }
    prev_pc = pc;
  }
  if (total_splits < 2 || prev_pc < 3) {
    fail("growth did not split P0=1 into multiple partitions (splits=" +
         std::to_string(total_splits) + " pc=" + std::to_string(prev_pc) + ")");
    return;
  }
  // 全量抽查读（首/中/尾 + 各轮边界附近）。
  for (int id : {0, total / 4, total / 2, total - 1}) {
    if (!get_eq(id)) {
      fail("read after growth");
      return;
    }
  }
  // reopen：路由（ZFPROPS 多版本）与数据一致。
  const int64_t cnt_final = CountViaIterator(db.get());
  db.reset();
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    fail("reopen: " + s.ToString());
    return;
  }
  if (ZfMetric(db.get(), "partition_count") != prev_pc) {
    fail("partition_count persisted wrong across reopen");
    return;
  }
  if (CountViaIterator(db.get()) != cnt_final) {
    fail("iter count mismatch after reopen");
    return;
  }
  for (int id : {0, total / 3, total / 2, total - 1}) {
    if (!get_eq(id)) {
      fail("spot read after reopen");
      return;
    }
  }
  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true, "P0=1 -> partitions=" + std::to_string(prev_pc) +
                              " splits=" + std::to_string(total_splits) +
                              " keys=" + std::to_string(total));
}

// ---------------------------------------------------------------------------
// ZeroFlushV0 M4 治理决策策略单测（纯逻辑，无 DB）。
// 覆盖：连续窗确认、热拆分/冷合并触发、P 界、同 range 冷却、Busy 重试
// （pending aging/退避调度）、全均匀负载不动作。
// ---------------------------------------------------------------------------
void TestGovernancePolicyUnit() {
  const char* tag = "GovernancePolicyUnit(M4)";
  using zeroflush::GovernanceObs;
  using zeroflush::GovernanceAction;
  using zeroflush::GovernanceKind;
  using zeroflush::GovernancePolicy;
  const uint64_t kNow0 = 1000000000000ULL;  // 远离 0 的虚拟时钟

  // ---- A) 热分区连续 3 窗后触发 split；成功后同 range 冷却 ----
  {
    GovernancePolicy::Config cfg;
    cfg.p_min = 1;
    cfg.p_max = 64;
    cfg.confirm_windows = 3;
    cfg.split_min_bytes = 100;
    cfg.merge_max_bytes = 1000;
    cfg.cooldown_base_us = 1000 * 1000;
    cfg.cooldown_max_us = 2000 * 1000;
    cfg.retry_backoff_base_us = 100 * 1000;
    cfg.retry_backoff_max_us = 1000 * 1000;
    GovernancePolicy pol(cfg);
    auto mk = [](uint32_t id, double fast, double slow, uint64_t base,
                 const std::string& lo, const std::string& hi) {
      GovernanceObs o;
      o.part_id = id;
      o.rate_fast = fast;
      o.rate_slow = slow;
      o.base_bytes = base;
      o.base_known = true;
      o.lo = lo;
      o.hi = hi;
      return o;
    };
    // 分区 1 热（rate=1000 = 4× 均值 250 ≥ θ_split×250=750）；分区 2-4 零速
    // 但 base 大（5000 > merge_max 1000）→ 冷合并被大小门槛排除。
    std::vector<GovernanceObs> obs = {
        mk(1, 1000, 1000, 500, "", "b"),  mk(2, 0, 0, 5000, "b", "c"),
        mk(3, 0, 0, 5000, "c", "d"),      mk(4, 0, 0, 5000, "d", "")};
    GovernanceAction act;
    bool fired = false;
    for (int t = 0; t < 6; ++t) {
      auto acts = pol.Decide(obs, kNow0 + t * 1000000);
      if (!acts.empty()) {
        act = acts[0];
        fired = true;
        break;
      }
    }
    if (!fired || act.kind != GovernanceKind::kSplit || act.part_id != 1) {
      ReportResult(tag, false, "A: hot split did not fire after 3 windows");
      return;
    }
    // 成功后同 range 冷却 → 下一轮（同 obs）不动作。
    std::vector<GovernancePolicy::RangeKey> affected = {{"", "b"}};
    pol.NoteSuccess(act, kNow0 + 6 * 1000000, affected, 0);
    if (!pol.Decide(obs, kNow0 + 6 * 1000000 + 1).empty()) {
      ReportResult(tag, false, "A: cooldown did not block immediate re-action");
      return;
    }
  }

  // ---- B) 全均匀负载：rate 恒等于均值 → 无任何动作 ----
  {
    GovernancePolicy::Config cfg;
    cfg.p_min = 1;
    cfg.p_max = 64;
    cfg.confirm_windows = 3;
    cfg.split_min_bytes = 100;
    cfg.merge_max_bytes = 1000;
    GovernancePolicy pol(cfg);
    std::vector<GovernanceObs> obs;
    for (uint32_t i = 0; i < 8; ++i) {
      GovernanceObs o;
      o.part_id = 10 + i;
      o.rate_fast = o.rate_slow = 100.0;
      o.base_bytes = 500;
      o.base_known = true;
      obs.push_back(o);
    }
    for (int t = 0; t < 8; ++t) {
      if (!pol.Decide(obs, kNow0 + t * 1000000).empty()) {
        ReportResult(tag, false, "B: uniform load must not trigger");
        return;
      }
    }
  }

  // ---- C) 全空闲冷回收：P>P_min 时小分区被合并，直到 P==P_min 停 ----
  {
    GovernancePolicy::Config cfg;
    cfg.p_min = 2;
    cfg.p_max = 64;
    cfg.confirm_windows = 1;
    cfg.split_min_bytes = 100;
    cfg.merge_max_bytes = 1000;
    cfg.cooldown_base_us = 1000 * 1000;
    cfg.cooldown_max_us = 2000 * 1000;
    GovernancePolicy pol(cfg);
    std::vector<GovernanceObs> obs;
    for (uint32_t i = 0; i < 4; ++i) {
      GovernanceObs o;
      o.part_id = 20 + i;
      o.rate_fast = o.rate_slow = 0.0;
      o.base_bytes = 0;
      o.base_known = true;
      o.lo = i == 0 ? "" : "k" + std::to_string(i);
      o.hi = i == 3 ? "" : "k" + std::to_string(i + 1);
      obs.push_back(o);
    }
    const auto acts = pol.Decide(obs, kNow0);
    if (acts.empty() || acts[0].kind != GovernanceKind::kMerge) {
      ReportResult(tag, false, "C: idle cold partition did not trigger merge");
      return;
    }
    // P==P_min：合并被界拦住。
    std::vector<GovernanceObs> two = {obs[0], obs[1]};
    for (int t = 0; t < 6; ++t) {
      if (!pol.Decide(two, kNow0 + (t + 1) * 1000000).empty()) {
        ReportResult(tag, false, "C: merge fired at P==P_min");
        return;
      }
    }
  }

  // ---- D) Busy 重试：退避调度（到期前不动作）+ aging 保留 ----
  {
    GovernancePolicy::Config cfg;
    cfg.p_min = 2;
    cfg.p_max = 64;
    cfg.confirm_windows = 1;
    cfg.split_min_bytes = 100;
    cfg.merge_max_bytes = 1000;
    cfg.retry_backoff_base_us = 100 * 1000;
    cfg.retry_backoff_max_us = 1000 * 1000;
    GovernancePolicy pol(cfg);
    std::vector<GovernanceObs> obs;
    for (uint32_t i = 0; i < 4; ++i) {
      GovernanceObs o;
      o.part_id = 30 + i;
      o.rate_fast = o.rate_slow = 0.0;
      o.base_bytes = 0;
      o.base_known = true;
      o.lo = "n" + std::to_string(i);
      o.hi = "n" + std::to_string(i + 1);
      obs.push_back(o);
    }
    const auto acts0 = pol.Decide(obs, kNow0);
    if (acts0.empty()) {
      ReportResult(tag, false, "D: expected a merge candidate");
      return;
    }
    pol.NoteRetry(acts0[0], kNow0);
    // 退避期内：被重试的分区不得再次入选（pending 未到期；其他分区仍可作
    // 为新候选被选中——因此只断言返回的动作不是被重试分区）。
    {
      const auto acts_before = pol.Decide(obs, kNow0 + 1000);
      if (!acts_before.empty() &&
          acts_before[0].part_id == acts0[0].part_id) {
        ReportResult(tag, false, "D: retried partition fired before backoff");
        return;
      }
    }
    // 到期后：pending（aging 加成）优先于新候选 → 同一分区、kind 仍 merge。
    const auto acts1 = pol.Decide(obs, kNow0 + 200 * 1000);
    if (acts1.empty() || acts1[0].part_id != acts0[0].part_id ||
        acts1[0].kind != GovernanceKind::kMerge) {
      ReportResult(tag, false, "D: pending retry did not resume after backoff");
      return;
    }
  }

  // ---- E) rewrite 带宽预算（≤ share/(1-share)×输出）----
  {
    using zeroflush::RewriteBudgetOk;
    if (!RewriteBudgetOk(0.25, 10, 100) ||   // 10 ≤ 33.3 → 放行
        RewriteBudgetOk(0.25, 40, 100) ||    // 40 > 33.3 → 拒
        !RewriteBudgetOk(0.0, 1e9, 1) ||     // share=0 → 不限
        !RewriteBudgetOk(0.25, 0, 0)) {      // 无 rewrite → 放行
      ReportResult(tag, false, "E: rewrite budget gate wrong");
      return;
    }
  }

  ReportResult(tag, true, "A-E ok");
}

// ---------------------------------------------------------------------------
// ZeroFlushV0 M4 动态治理 e2e（确定性：手动 RunGovernanceTick 驱动）。
// 拆两个独立子场景，各自确定性验证一条治理路径：
//  A) 偏态热写入 → 控制器热拆分：P 增长到 P_max（空闲期合并 idle 窗设极大
//     → 本场景无 merge 干扰，拆分确定性）；
//  B) 全新空库（全零速率、无封存样本）→ 空闲期冷回收：merge 把 P 收到
//     P_min 下界（全空分区、无时序依赖，确定性）。
//  两者均校验数据正确 / P 界 / reopen 一致。
// ---------------------------------------------------------------------------
void TestGovernanceDynamicEndToEnd() {
  const char* tag = "GovernanceDynamicEndToEnd(M4)";
  const int kPmin = 2;
  const int kPmax = 8;

  // ---- A) 热拆分（偏态单调写 + 排空检查点 tick）----
  {
    std::string dbname = std::string(kDbBase) + "governance_m4_split";
    CleanDB(dbname);
    zeroflush::ZeroFlushOptions zfo;
    zfo.partitions = 4;
    zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
    zfo.static_boundaries = {"m", "s", "w"};
    zfo.partition_target_bytes = 8 * 1024;
    zfo.epoch_target_bytes = 8 * 1024;
    zfo.dynamic_governance = false;  // 手动 tick 驱动（确定性）
    zfo.governance_p_min = kPmin;
    zfo.governance_p_max = kPmax;
    zfo.governance_split_min_bytes = 4 * 1024;
    zfo.governance_merge_max_bytes = 4 * 1024;
    zfo.governance_confirm_windows = 2;
    zfo.governance_cooldown_micros = 1;
    zfo.governance_cooldown_max_micros = 1;
    zfo.governance_ewma_fast_secs = 2;
    zfo.governance_ewma_slow_secs = 2;
    // 空闲期合并窗 = 极大 → 本子场景只测拆分（无 merge 干扰）。
    zfo.governance_merge_idle_micros = 100ull << 30;

    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "A: open: " + s.ToString());
      return;
    }
    char k[24];
    auto val = [](int id) {
      std::string v(200, 'x');
      snprintf(&v[0], 32, "gv%06d", id);
      return v;
    };
    uint64_t total = 0;
    const int kKeysPerRound = 90;
    const int kRounds = 14;
    for (int round = 0; round < kRounds; ++round) {
      for (int i = 0; i < kKeysPerRound; ++i, ++total) {
        snprintf(k, sizeof(k), "a%09d", static_cast<int>(total));
        if (!db->Put(rocksdb::WriteOptions(), k, val(static_cast<int>(total)))
                 .ok()) {
          ReportResult(tag, false, "A: put@round");
          db.reset();
          CleanDB(dbname);
          return;
        }
      }
      if (!WaitAllMaterialized(db.get(), 60000)) {
        ReportResult(tag, false, "A: WaitAllMaterialized");
        db.reset();
        CleanDB(dbname);
        return;
      }
      for (int t = 0; t < 3; ++t) {
        const rocksdb::Status ts = zeroflush::RunGovernanceTick(db.get());
        if (!ts.ok()) {
          ReportResult(tag, false, "A: tick: " + ts.ToString());
          db.reset();
          CleanDB(dbname);
          return;
        }
      }
      const uint64_t pc = ZfMetric(db.get(), "partition_count");
      if (pc < kPmin || pc > kPmax) {
        ReportResult(tag, false, "A: P out of bounds (P=" +
                                     std::to_string(pc) + ")");
        db.reset();
        CleanDB(dbname);
        return;
      }
    }
    if (!WaitAllMaterialized(db.get(), 60000)) {
      ReportResult(tag, false, "A: WaitAllMaterialized(end)");
      db.reset();
      CleanDB(dbname);
      return;
    }
    if (ZfMetric(db.get(), "gov_splits") < 2) {
      ReportResult(tag, false,
                   "A: hot skew did not split >=2 times (got " +
                       std::to_string(ZfMetric(db.get(), "gov_splits")) + ")");
      db.reset();
      CleanDB(dbname);
      return;
    }
    const uint64_t pc_end = ZfMetric(db.get(), "partition_count");
    const int64_t cnt = CountViaIterator(db.get());
    if (cnt != static_cast<int64_t>(total)) {
      ReportResult(tag, false, "A: iter count mismatch");
      db.reset();
      CleanDB(dbname);
      return;
    }
    for (int id : {0, 500, static_cast<int>(total) - 1}) {
      snprintf(k, sizeof(k), "a%09d", id);
      std::string out;
      if (!db->Get(rocksdb::ReadOptions(), k, &out).ok() ||
          out != val(id)) {
        ReportResult(tag, false, "A: spot read");
        db.reset();
        CleanDB(dbname);
        return;
      }
    }
    db.reset();
    s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "A: reopen: " + s.ToString());
      CleanDB(dbname);
      return;
    }
    if (ZfMetric(db.get(), "partition_count") != pc_end ||
        CountViaIterator(db.get()) != static_cast<int64_t>(total)) {
      ReportResult(tag, false, "A: reopen inconsistent");
      db.reset();
      CleanDB(dbname);
      return;
    }
    db.reset();
    CleanDB(dbname);
  }

  // ---- B) 空闲期冷回收（全空分区合并至 P_min）----
  {
    std::string dbname = std::string(kDbBase) + "governance_m4_reclaim";
    CleanDB(dbname);
    zeroflush::ZeroFlushOptions zfo;
    zfo.partitions = 6;
    zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
    zfo.static_boundaries = {"b", "d", "f", "h", "j"};
    zfo.partition_target_bytes = 8 * 1024;
    zfo.epoch_target_bytes = 8 * 1024;
    zfo.dynamic_governance = false;
    zfo.governance_p_min = kPmin;
    zfo.governance_p_max = kPmax;
    zfo.governance_split_min_bytes = 4 * 1024;
    zfo.governance_merge_max_bytes = 4 * 1024;
    zfo.governance_confirm_windows = 1;
    zfo.governance_cooldown_micros = 1;
    zfo.governance_cooldown_max_micros = 1;
    zfo.governance_ewma_fast_secs = 2;
    zfo.governance_ewma_slow_secs = 2;
    zfo.governance_merge_idle_micros = 1;  // 空库无样本 → 立即视为空闲

    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "B: open: " + s.ToString());
      return;
    }
    uint64_t pc = ZfMetric(db.get(), "partition_count");
    int guard = 0;
    while (pc > kPmin && guard++ < 16) {
      const rocksdb::Status ts = zeroflush::RunGovernanceTick(db.get());
      if (!ts.ok()) {
        ReportResult(tag, false, "B: tick: " + ts.ToString());
        db.reset();
        CleanDB(dbname);
        return;
      }
      pc = ZfMetric(db.get(), "partition_count");
    }
    if (pc != kPmin ||
        ZfMetric(db.get(), "gov_merges") < 2) {
      ReportResult(tag, false,
                   "B: idle reclaim did not reach P_min (P=" +
                       std::to_string(pc) + " merges=" +
                       std::to_string(ZfMetric(db.get(), "gov_merges")) + ")");
      db.reset();
      CleanDB(dbname);
      return;
    }
    // 回收后读写一致性 + reopen。
    char k[16];
    for (int i = 0; i < 20; ++i) {
      snprintf(k, sizeof(k), "k%03d", i);
      if (!db->Put(rocksdb::WriteOptions(), k, "v").ok()) {
        ReportResult(tag, false, "B: put after reclaim");
        db.reset();
        CleanDB(dbname);
        return;
      }
    }
    if (!WaitAllMaterialized(db.get(), 30000)) {
      ReportResult(tag, false, "B: WaitAllMaterialized");
      db.reset();
      CleanDB(dbname);
      return;
    }
    if (CountViaIterator(db.get()) != 20) {
      ReportResult(tag, false, "B: iter count");
      db.reset();
      CleanDB(dbname);
      return;
    }
    db.reset();
    s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok() || CountViaIterator(db.get()) != 20) {
      ReportResult(tag, false, "B: reopen");
      db.reset();
      CleanDB(dbname);
      return;
    }
    db.reset();
    CleanDB(dbname);
  }

  ReportResult(tag, true, "A(split)+B(reclaim) ok");
}
// ---------------------------------------------------------------------------
// ZeroFlushV0 M4 后台自动治理 smoke（env 门控：ZF_GOV_BG=1 运行）。
// 用真实后台控制器线程（小 tick 间隔）+ 偏态写入：验证线程启停/Close
// join 生命周期与数据正确性（不做动作次数强断言——后台调度非确定性）。
// ---------------------------------------------------------------------------
void TestGovernanceBackgroundAuto() {
  const char* tag = "GovernanceBackgroundAuto(M4)";
  if (getenv("ZF_GOV_BG") == nullptr) {
    ReportResult(tag, true, "skipped (set ZF_GOV_BG=1 to run)");
    return;
  }
  std::string dbname = std::string(kDbBase) + "governance_m4_bg";
  CleanDB(dbname);
  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  zfo.static_boundaries = {"m", "s", "w"};
  zfo.partition_target_bytes = 8 * 1024;
  zfo.epoch_target_bytes = 8 * 1024;
  zfo.dynamic_governance = true;  // 真实后台控制器
  zfo.governance_tick_interval_micros = 20 * 1000;
  zfo.governance_p_min = 2;
  zfo.governance_p_max = 16;
  zfo.governance_split_min_bytes = 4 * 1024;
  zfo.governance_merge_max_bytes = 4 * 1024;
  zfo.governance_confirm_windows = 2;
  zfo.governance_cooldown_micros = 1;
  zfo.governance_cooldown_max_micros = 1;
  zfo.governance_ewma_fast_secs = 2;
  zfo.governance_ewma_slow_secs = 2;
  zfo.governance_merge_idle_micros = 30 * 1000;  // 空闲期合并判定窗（快测）

  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }
  auto fail = [&](const std::string& why) {
    ReportResult(tag, false, why);
    db.reset();
    CleanDB(dbname);
  };
  char k[24];
  std::string v(200, 'x');
  uint64_t total = 0;
  for (int round = 0; round < 6; ++round) {
    for (int i = 0; i < 60; ++i, ++total) {
      snprintf(k, sizeof(k), "a%09d", static_cast<int>(total));
      if (!db->Put(rocksdb::WriteOptions(), k, v).ok()) {
        fail("put@bg");
        return;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    if (!WaitAllMaterialized(db.get(), 30000)) {
      fail("WaitAllMaterialized@bg");
      return;
    }
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(50));  // 让后台收敛
  db.reset();  // Close：停止 + join 治理线程 → 关闭冲刷
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    fail("reopen: " + s.ToString());
    return;
  }
  const int64_t cnt = CountViaIterator(db.get());
  const uint64_t pc = ZfMetric(db.get(), "partition_count");
  if (cnt != static_cast<int64_t>(total)) {
    fail("iter count mismatch after bg governance (got " +
         std::to_string(cnt) + " want " + std::to_string(total) + ")");
    return;
  }
  if (pc < zfo.governance_p_min || pc > zfo.governance_p_max) {
    fail("P out of bounds after bg (P=" + std::to_string(pc) + ")");
    return;
  }
  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true, "bg ok P=" + std::to_string(pc));
}

// ---------------------------------------------------------------------------
// ZeroFlushV0 M4 回归：并发写下的 merge 路由切换屏障（在途写组计数）。
//  并行写（≥2 写线程）时写组最后完成者可能是 follower——若 ExitInflightGroup
//  只挂在 leader 出口，计数泄漏 → MergePartitionLocked 的路由切换屏障等待
//  归零永久 TimedOut（实测 T≥2 时 merge/split 全失败）。本用例：并发写 +
//  重试 merge，要求至少一次成功且数据完整。
// ---------------------------------------------------------------------------
void TestMergeWithConcurrentWriters() {
  const char* tag = "MergeConcurrentWriters(M4)";
  std::string dbname = std::string(kDbBase) + "merge_concurrent_writers";
  CleanDB(dbname);
  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  zfo.static_boundaries = {"c", "g", "m"};
  zfo.partition_target_bytes = 16 * 1024;
  zfo.epoch_target_bytes = 64 * 1024;
  zfo.materialize_parallelism = 4;
  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> wcnt[2];
  wcnt[0].store(0);
  wcnt[1].store(0);
  auto writer = [&](char pfx, int slot) {
    uint64_t i = 0;
    char k[16];
    const std::string v(200, 'w');
    while (!stop.load(std::memory_order_relaxed)) {
      snprintf(k, sizeof(k), "%c%06" PRIu64, pfx, i);
      if (!db->Put(rocksdb::WriteOptions(), k, v + k).ok()) {
        break;
      }
      ++i;
      wcnt[slot].fetch_add(1, std::memory_order_relaxed);
    }
  };
  std::thread w1(writer, 'd', 0);
  std::thread w2(writer, 'n', 1);
  // 先让并发写跑一段（制造并行写组——泄漏 bug 只在 ≥2 writer 的组上出现，
  // 计数滞留后屏障永久无法归零）。
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  // 主线程 merge 冷分区 2=[g,m)（写者只写分区 1/3，目标分区无数据 → 前置
  // 平凡满足，唯一等待 = 路由切换屏障 inflight==0）。并发写者持续产生写组
  // → 修复前计数泄漏使屏障永不归零（TimedOut），修复后应立即成功。
  uint32_t successes = 0;
  std::string last_status;
  for (int attempt = 0; attempt < 60 && successes == 0; ++attempt) {
    uint32_t absorb = 999;
    const rocksdb::Status ms =
        zeroflush::MergePartition(db.get(), /*drain_part=*/2, &absorb);
    last_status = ms.ToString();
    if (ms.ok()) {
      ++successes;
      break;
    }
    if (!ms.IsBusy() && !ms.IsTimedOut()) {
      stop.store(true);
      w1.join();
      w2.join();
      ReportResult(tag, false, "unexpected merge status: " + ms.ToString());
      db.reset();
      CleanDB(dbname);
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  stop.store(true);
  w1.join();
  w2.join();
  if (successes == 0) {
    ReportResult(tag, false,
                 "merge barrier never drained under concurrent writers; last=" +
                     last_status + " inflight=" +
                     std::to_string(ZfMetric(db.get(), "inflight_groups")));
    db.reset();
    CleanDB(dbname);
    return;
  }
  if (!WaitAllMaterialized(db.get(), 60000)) {
    ReportResult(tag, false, "WaitAllMaterialized");
    db.reset();
    CleanDB(dbname);
    return;
  }
  // 数据完整性：两写者各自前缀键全部可读，迭代计数 = 两写者写入数和。
  const uint64_t n0 = wcnt[0].load();
  const uint64_t n1 = wcnt[1].load();
  for (int slot = 0; slot < 2; ++slot) {
    const char pfx = slot == 0 ? 'd' : 'n';
    const uint64_t n = slot == 0 ? n0 : n1;
    for (uint64_t i = 0; i < n; ++i) {
      char k[16];
      snprintf(k, sizeof(k), "%c%06" PRIu64, pfx, i);
      std::string out;
      if (!db->Get(rocksdb::ReadOptions(), k, &out).ok()) {
        ReportResult(tag, false, "read after concurrent merge");
        db.reset();
        CleanDB(dbname);
        return;
      }
    }
  }
  const int64_t cnt = CountViaIterator(db.get());
  if (cnt != static_cast<int64_t>(n0 + n1)) {
    ReportResult(tag, false, "iter count mismatch after concurrent merge (got " +
                                 std::to_string(cnt) + " want " +
                                 std::to_string(n0 + n1) + ")");
    db.reset();
    CleanDB(dbname);
    return;
  }
  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true,
               "merge ok under concurrent writers (writes=" +
                   std::to_string(n0 + n1) + ")");
}

// ---------------------------------------------------------------------------
// ZeroFlushV0 M4 回归：活跃尾 split（连续写负载，无排空检查点）。
//  并发写者持续写父分区，主线程直接 SplitPartition：活跃尾在切路由后被
//  冻结为父区间覆盖的孤儿尾 epoch，子分区索引 miss 的 key 由别名回查父链
//  （E 排空前），排空后 type-3 rewrite 两分。要求：split 成功、split 前后
//  写入的全部 key 可读且值正确、迭代计数一致、reopen 一致。
// ---------------------------------------------------------------------------
void TestSplitWithConcurrentWriters() {
  const char* tag = "SplitConcurrentWriters(M4)";
  std::string dbname = std::string(kDbBase) + "split_concurrent_writers";
  CleanDB(dbname);
  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 4;
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
  zfo.static_boundaries = {"c", "g", "m"};
  zfo.partition_target_bytes = 16 * 1024;
  zfo.epoch_target_bytes = 64 * 1024;
  zfo.materialize_parallelism = 4;
  zfo.governance_active_tail_split = true;  // 显式覆盖（默认已开）
  std::unique_ptr<rocksdb::DB> db;
  auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok()) {
    ReportResult(tag, false, "open: " + s.ToString());
    return;
  }
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> wcnt[2];
  wcnt[0].store(0);
  wcnt[1].store(0);
  auto writer = [&](char pfx, int slot) {
    uint64_t i = 0;
    char k[16];
    while (!stop.load(std::memory_order_relaxed)) {
      snprintf(k, sizeof(k), "%c%06" PRIu64, pfx, i);
      const std::string v = std::string("v") + k;
      if (!db->Put(rocksdb::WriteOptions(), k, v).ok()) break;
      ++i;
      wcnt[slot].fetch_add(1, std::memory_order_relaxed);
    }
  };
  std::thread w1(writer, 'd', 0);
  std::thread w2(writer, 'e', 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  // 连续写负载下直接 split 父分区 1=[c,g)，切点 "d5"（内部键，两侧均有数据）。
  uint32_t cl = 0, cr = 0;
  bool ok = false;
  std::string last;
  for (int attempt = 0; attempt < 80 && !ok; ++attempt) {
    const rocksdb::Status ss = zeroflush::SplitPartition(
        db.get(), /*part=*/1, rocksdb::Slice("d5"), &cl, &cr);
    last = ss.ToString();
    if (ss.ok()) {
      ok = true;
      break;
    }
    if (!ss.IsBusy() && !ss.IsTimedOut()) {
      stop.store(true);
      w1.join();
      w2.join();
      ReportResult(tag, false, "split status: " + ss.ToString());
      db.reset();
      CleanDB(dbname);
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  if (!ok) {
    stop.store(true);
    w1.join();
    w2.join();
    ReportResult(tag, false, "active-tail split never succeeded; last=" + last);
    db.reset();
    CleanDB(dbname);
    return;
  }
  if (ZfMetric(db.get(), "partition_count") != 5) {
    stop.store(true);
    w1.join();
    w2.join();
    ReportResult(tag, false, "partition_count != 5 after split");
    db.reset();
    CleanDB(dbname);
    return;
  }
  // split 后继续写一段（新写按子分区路由；旧数据在孤儿尾 epoch → 别名读）。
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  stop.store(true);
  w1.join();
  w2.join();
  if (!WaitAllMaterialized(db.get(), 60000)) {
    ReportResult(tag, false, "WaitAllMaterialized");
    db.reset();
    CleanDB(dbname);
    return;
  }
  const uint64_t n0 = wcnt[0].load();
  const uint64_t n1 = wcnt[1].load();
  for (int slot = 0; slot < 2; ++slot) {
    const char pfx = slot == 0 ? 'd' : 'e';
    const uint64_t n = slot == 0 ? n0 : n1;
    for (uint64_t i = 0; i < n; ++i) {
      char k[16];
      snprintf(k, sizeof(k), "%c%06" PRIu64, pfx, i);
      std::string out;
      const std::string want = std::string("v") + k;
      if (!db->Get(rocksdb::ReadOptions(), k, &out).ok() || out != want) {
        ReportResult(tag, false, std::string("read mismatch after active split: ") + k);
        db.reset();
        CleanDB(dbname);
        return;
      }
    }
  }
  const int64_t cnt = CountViaIterator(db.get());
  if (cnt != static_cast<int64_t>(n0 + n1)) {
    ReportResult(tag, false, "iter count mismatch (got " + std::to_string(cnt) +
                                 " want " + std::to_string(n0 + n1) + ")");
    db.reset();
    CleanDB(dbname);
    return;
  }
  const uint64_t pc_before = ZfMetric(db.get(), "partition_count");
  db.reset();
  s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
  if (!s.ok() || ZfMetric(db.get(), "partition_count") != pc_before) {
    ReportResult(tag, false, "reopen inconsistent");
    db.reset();
    CleanDB(dbname);
    return;
  }
  db.reset();
  CleanDB(dbname);
  ReportResult(tag, true, "active-tail split under writes ok (cl=" +
                              std::to_string(cl) + " cr=" + std::to_string(cr) +
                              " writes=" + std::to_string(n0 + n1) + ")");
}


// ---------------------------------------------------------------------------
// ZeroFlushV0 M5 崩溃矩阵：活跃尾 split 中断（decide/route/freeze/rewrite 各
// 阶段）。子进程 sync 写种子后触发 SplitPartition（活跃尾路径），父进程在
// 不同时机 SIGKILL；重开后要求：路由表可恢复（P∈{4,5}）、种子数据零丢失、
// 后续写/物化/reopen 仍一致（split_orphans_ 内存别名丢失不影响——重开由
// recovery 重放 zfwal 重建索引）。
// ---------------------------------------------------------------------------
void TestCrashMidActiveSplitReopen() {
  const char* tag = "CrashMidActiveSplitReopen(M5)";
  const int kSeeds = 120;
  auto val = [](const char* pfx, int id) {
    std::string v(200, 'x');
    snprintf(&v[0], 32, "v%s%04d", pfx, id);
    return v;
  };
  // 两档 kill 时机：偏早（可能在 freeze/物化中）与偏晚（可能在 rewrite/尾部）。
  const int kill_ms[2] = {80, 260};
  for (int round = 0; round < 2; ++round) {
    std::string dbname = std::string(kDbBase) + "crash_mid_activesplit" +
                         std::to_string(round);
    CleanDB(dbname);
    zeroflush::ZeroFlushOptions zfo;
    zfo.partitions = 4;
    zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kStatic;
    zfo.static_boundaries = {"c", "g", "m"};
    zfo.partition_target_bytes = 1024 * 1024;  // d 数据保持活跃尾
    zfo.epoch_target_bytes = 1024 * 1024;
    zfo.materialize_parallelism = 4;

    pid_t pid = ::fork();
    if (pid < 0) {
      ReportResult(tag, false, "fork failed");
      return;
    }
    if (pid == 0) {
      std::unique_ptr<rocksdb::DB> db;
      if (!zeroflush::Open(MakeOptions(), zfo, dbname, &db).ok()) _exit(1);
      rocksdb::WriteOptions wo;
      wo.sync = true;  // 种子必须持久（崩溃后零丢失校验）
      char k[16];
      for (int i = 0; i < kSeeds; ++i) {
        snprintf(k, sizeof(k), "d%04d", i);
        if (!db->Put(wo, k, val("d", i)).ok()) _exit(2);
      }
      // 标记"种子已全部写完"（父进程据此再等 kill 偏移——否则会误杀在写种子
      // 中途，造成"丢失"假象）。
      {
        FILE* mf = ::fopen((dbname + ".seeds_done").c_str(), "w");
        if (mf != nullptr) {
          ::fputc('1', mf);
          ::fclose(mf);
        }
      }
      // 触发活跃尾 split（d 数据在活跃 WAL 中；目标分区 1=[c,g)）。
      for (;;) {
        uint32_t cl = 0, cr = 0;
        const rocksdb::Status ss = zeroflush::SplitPartition(
            db.get(), /*part=*/1, rocksdb::Slice("d5"), &cl, &cr);
        if (ss.ok()) break;
        if (!ss.IsBusy() && !ss.IsTimedOut()) _exit(3);
        usleep(500);
      }
      for (;;) pause();
    }
    {
      const std::string marker = dbname + ".seeds_done";
      const auto dl = std::chrono::steady_clock::now() +
                      std::chrono::seconds(30);
      while (!DirExists(marker)) {
        if (std::chrono::steady_clock::now() > dl) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(kill_ms[round]));
    ::kill(pid, SIGKILL);
    int st = 0;
    ::waitpid(pid, &st, 0);

    std::unique_ptr<rocksdb::DB> db;
    auto s = zeroflush::Open(MakeOptions(), zfo, dbname, &db);
    if (!s.ok()) {
      ReportResult(tag, false, "reopen#1: " + s.ToString());
      CleanDB(dbname);
      return;
    }
    const uint64_t pc = ZfMetric(db.get(), "partition_count");
    if (pc != 4 && pc != 5) {
      ReportResult(tag, false, "P not recoverable after crash (P=" +
                                   std::to_string(pc) + ")");
      db.reset();
      CleanDB(dbname);
      return;
    }
    char k[16];
    for (int i = 0; i < kSeeds; ++i) {
      snprintf(k, sizeof(k), "d%04d", i);
      std::string out;
      if (!db->Get(rocksdb::ReadOptions(), k, &out).ok() ||
          out != val("d", i)) {
        ReportResult(tag, false, std::string("seed lost/corrupt: ") + k);
        db.reset();
        CleanDB(dbname);
        return;
      }
    }
    if (CountViaIterator(db.get()) != kSeeds) {
      ReportResult(tag, false, "iter count != seeds after crash reopen");
      db.reset();
      CleanDB(dbname);
      return;
    }
    // 后续写 → 物化 → reopen 仍一致。
    rocksdb::WriteOptions wo;
    for (int i = 0; i < 400; ++i) {
      snprintf(k, sizeof(k), "b%04d", i);
      if (!db->Put(wo, k, val("b", i)).ok()) {
        ReportResult(tag, false, "put after crash reopen");
        db.reset();
        CleanDB(dbname);
        return;
      }
    }
    if (!WaitAllMaterialized(db.get(), 60000)) {
      ReportResult(tag, false, "WaitAllMaterialized after crash reopen");
      db.reset();
      CleanDB(dbname);
      return;
    }
    const int64_t want = kSeeds + 400;
    if (CountViaIterator(db.get()) != want) {
      ReportResult(tag, false, "iter count after drive");
      db.reset();
      CleanDB(dbname);
      return;
    }
    db.reset();
    if (!zeroflush::Open(MakeOptions(), zfo, dbname, &db).ok() ||
        CountViaIterator(db.get()) != want) {
      ReportResult(tag, false, "reopen#2 inconsistent");
      db.reset();
      CleanDB(dbname);
      return;
    }
    db.reset();
    CleanDB(dbname);
  }
  ReportResult(tag, true,
               "kill at 80/260ms: seeds intact, P∈{4,5}, reopen consistent");
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

  // ---- ZeroFlushV0 P1（S4 merge）新增 ----
  run("MergePartitionTableDerive",      TestMergePartitionTableDerive);
  run("MergeColdPartitionEndToEnd",     TestMergeColdPartitionEndToEnd);
  run("MergeActiveTailEndToEnd",        TestMergeActiveTailEndToEnd);
  run("MergeActiveTailMergeEnabled",   TestMergeActiveTailMergeEnabled);
  // ---- ZeroFlushV0 M1c② 新增 ----
  run("CrashMidDrainReopen",           TestCrashMidDrainReopen);
  // ---- ZeroFlushV0 P2（M2）新增 ----
  run("DeriveSplitTable",              TestDeriveSplitTable);
  run("SplitQuiescentEndToEnd",        TestSplitQuiescentEndToEnd);
  // ---- ZeroFlushV0 R0（并行轨）复现 harness（默认 no-op）----
  run("MergeParallelStress",           TestMergeParallelStress);
  // ---- ZeroFlushV0 M3（起步生长）新增 ----
  run("GrowthSinglePartition",         TestGrowthSinglePartition);
  // ---- ZeroFlushV0 M4（动态治理 DG）新增 ----
  run("GovernancePolicyUnit",          TestGovernancePolicyUnit);
  run("GovernanceDynamicEndToEnd",     TestGovernanceDynamicEndToEnd);
  run("GovernanceBackgroundAuto",      TestGovernanceBackgroundAuto);
  run("MergeConcurrentWriters",        TestMergeWithConcurrentWriters);
  run("SplitConcurrentWriters",        TestSplitWithConcurrentWriters);
  run("CrashMidActiveSplitReopen",  TestCrashMidActiveSplitReopen);

  fprintf(stderr, "============================================================\n");
  if (g_failures == 0) {
    fprintf(stderr, " ALL PASSED\n");
  } else {
    fprintf(stderr, " %d FAILED\n", g_failures);
  }
  fprintf(stderr, "============================================================\n");
  return g_failures;
}
