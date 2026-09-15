// ZeroFlush 严格正确性校验器（step 0b-3 第三阶段前置）
//
// 与 db_bench **共用**键生成（tools/zf_keygen.h），并自带参考值模型，
// 因此能证明"读到了正确版本"，而不仅是"请求返回了结果"。
//
// 用法：
//   zf_valcheck <dbdir> all    <N> [threads] [per_thread]  # 写→快照→覆盖→删除→校验
//   zf_valcheck <dbdir> verify <N> [threads] [per_thread]  # 仅校验（重启后）
//   zf_valcheck <dbdir> walhit <N> [threads] [per_thread]  # 不 Flush：读应走 WAL 命中
//   zf_valcheck <dbdir> l1hit  <N> [threads] [per_thread]  # 先 Flush：读走 L1/SST
//
// 覆盖的检查项：
//   1) 写入后随机 Get（参考值精确比对）
//   2) 重启后的 Get（verify 子命令，进程级重启）
//   3) 物化前 WAL 命中 / 物化后 L1 命中（walhit / l1hit 两个子命令对照）
//   4) 覆盖写（每 100 个键覆盖，seed=2）
//   5) Delete（每 1000 个键删除一个）→ 必须 NotFound
//   6) snapshot 可见性（覆盖前的快照必须看到 seed1）
//   7) 不存在的键 → NotFound
//   8) WAL/目录索引与 L1 结果一致（两条命中路径下同一批键结果相同）
#include <algorithm>
#include <atomic>
#include <map>
#include <cstring>
#include <dirent.h>
#include <fstream>
#include <iterator>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/write_batch.h"
#include "tools/zf_keygen.h"
#include "zeroflush/wal_format.h"
#include "zeroflush/zeroflush_db.h"

namespace {

constexpr int kKeySize = 16;
constexpr int kValueSize = 1024;

struct Stats {
  std::atomic<uint64_t> checked{0};
  std::atomic<uint64_t> mismatch{0};
  std::atomic<uint64_t> miss{0};
  std::atomic<uint64_t> unexpected_found{0};
  std::atomic<uint64_t> errors{0};
};

bool IsDeleted(uint64_t v) { return v % 1000 == 0; }

// 覆盖规则必须与写入端一致：第二阶段对 **[0, half) 的全部非删除键**写入 seed=2
// （不是"每 100 个覆盖一次"——校验器首版模型写错，导致 9990 例假不匹配；
// probe 模式证实数据本身正确）。
// 最新视图期望值。返回 false = 期望 NotFound。
bool ExpectLatest(uint64_t v, uint64_t half, std::string* out) {
  if (IsDeleted(v)) {
    return false;
  }
  *out = zf_keygen::ValueFor(v, v < half ? 2 : 1, kValueSize);
  return true;
}

void CheckRange(rocksdb::DB* db, const rocksdb::Snapshot* snap, uint64_t lo,
                uint64_t hi, uint64_t half, Stats* st) {
  rocksdb::ReadOptions ro;
  ro.snapshot = snap;
  for (uint64_t v = lo; v < hi; ++v) {
    const std::string key = zf_keygen::KeyFromInt(v, kKeySize);
    std::string got;
    const rocksdb::Status s = db->Get(ro, key, &got);
    std::string expect;
    const bool expect_ok = ExpectLatest(v, half, &expect);
    if (snap == nullptr) {
      // ---- 最新视图 ----
      if (expect_ok) {
        if (!s.ok()) {
          st->miss.fetch_add(1);
        } else if (got != expect) {
          st->mismatch.fetch_add(1);
        }
      } else if (!s.IsNotFound()) {
        s.ok() ? st->unexpected_found.fetch_add(1) : st->errors.fetch_add(1);
      }
    } else {
      // ---- 快照视图（创建于覆盖写之前）：一律 seed1 ----
      if (IsDeleted(v)) {
        if (!s.IsNotFound()) {
          s.ok() ? st->unexpected_found.fetch_add(1)
                 : st->errors.fetch_add(1);
        }
      } else {
        const std::string want = zf_keygen::ValueFor(v, 1, kValueSize);
        if (!s.ok()) {
          st->miss.fetch_add(1);
        } else if (got != want) {
          st->mismatch.fetch_add(1);
        }
      }
    }
    st->checked.fetch_add(1);
  }
}

std::atomic<uint64_t> g_write_errors{0};

void WriteRange(rocksdb::DB* db, uint64_t lo, uint64_t hi, uint32_t seed) {
  constexpr int kBatch = 64;
  rocksdb::WriteBatch batch;
  int n = 0;
  auto flush_batch = [&]() {
    const rocksdb::Status ws = db->Write(rocksdb::WriteOptions(), &batch);
    if (!ws.ok()) {
      // 关键：写状态必须检查——首版丢弃返回值，导致"写丢失"被误判为读缺陷。
      if (g_write_errors.fetch_add(1) < 8) {
        fprintf(stderr, "WRITE_ERROR seed=%u lo=%llu: %s\n", seed,
                (unsigned long long)lo, ws.ToString().c_str());
      }
    }
    batch.Clear();
    n = 0;
  };
  for (uint64_t v = lo; v < hi; ++v) {
    if (IsDeleted(v)) {
      batch.Delete(zf_keygen::KeyFromInt(v, kKeySize));
    } else {
      batch.Put(zf_keygen::KeyFromInt(v, kKeySize),
                zf_keygen::ValueFor(v, seed, kValueSize));
    }
    if (++n >= kBatch) {
      flush_batch();
    }
  }
  if (n > 0) {
    flush_batch();
  }
}

std::unique_ptr<rocksdb::DB> OpenDb(const std::string& dbdir, bool create) {
  rocksdb::Options opt;
  opt.create_if_missing = create;
  zeroflush::ZeroFlushOptions zfo;
  zfo.partitions = 16;
  // 降低封存阈值：小规模数据也能走到"封存→物化"路径，从而分别验证
  // WAL 命中（walhit）与 L1 命中（l1hit）两条读路径。
  zfo.epoch_target_bytes = 16ull << 20;      // 16MB 触发封存
  zfo.partition_target_bytes = 4ull << 20;   // 单分区 4MB
  // 诊断运行必须关闭 WAL 回收：否则"物化后 unlink"会让"WAL 里没有 seed2"
  // 无法区分"从未写入"与"已物化后删除"（用户 2026-09-15 指定）。
  if (getenv("ZF_VC_KEEP_WAL") != nullptr) {
    zfo.reclaim_sealed_files = false;
  }
  zfo.routing_mode = zeroflush::ZeroFlushOptions::RoutingMode::kSampled;
  zfo.merge_into_base_level = true;
  std::unique_ptr<rocksdb::DB> db;
  const auto s = zeroflush::Open(opt, zfo, dbdir, &db);
  if (!s.ok()) {
    fprintf(stderr, "open failed: %s\n", s.ToString().c_str());
    return nullptr;
  }
  return db;
}

// zfwal 文件命名：zf-wal-<part>-<gen>.log（与 wal_manager.cc 的
// MakeFileName/ParseFileName 一致；那两个函数在匿名命名空间里未导出）。
bool ParseZfWalName(const char* name, uint32_t* part, uint32_t* gen) {
  const char* kPrefix = "zf-wal-";
  if (strncmp(name, kPrefix, 7) != 0) {
    return false;
  }
  char* end = nullptr;
  const unsigned long pp = strtoul(name + 7, &end, 10);
  if (end == name + 7 || *end != '-') {
    return false;
  }
  const char* q = end + 1;
  const unsigned long gg = strtoul(q, &end, 10);
  if (end == q || strncmp(end, ".log", 4) != 0) {
    return false;
  }
  *part = static_cast<uint32_t>(pp);
  *gen = static_cast<uint32_t>(gg);
  return true;
}

std::string ZfWalPath(const std::string& dir, uint32_t part, uint32_t gen) {
  return dir + "/zf-wal-" + std::to_string(part) + "-" +
         std::to_string(gen) + ".log";
}

// ---- 证据采集：SST（全部层）与 WAL ----
enum SeedClass { kSeed1 = 1, kSeed2 = 2, kOther = 3 };

SeedClass ClassifyValue(const std::string& v, uint64_t key_v, int value_size) {
  if (v == zf_keygen::ValueFor(key_v, 1, value_size)) return kSeed1;
  if (v == zf_keygen::ValueFor(key_v, 2, value_size)) return kSeed2;
  return kOther;
}

struct SstHit {
  int level = -1;
  uint64_t file = 0;
  uint64_t seq = 0;
  SeedClass cls = kOther;
};

// 扫全部 live SST（所有层）：定位该 user key 的全部版本。
// 用 GetLiveFilesMetaData 取"文件号→层"，用 SstFileReader 迭代内部键。
void ScanSstsFor(rocksdb::DB* db, uint64_t key_v, int value_size,
                 std::vector<SstHit>* out) {
  std::vector<rocksdb::LiveFileMetaData> files;
  db->GetLiveFilesMetaData(&files);
  const std::string key = zf_keygen::KeyFromInt(key_v, 16);
  for (const auto& f : files) {
    if (f.name.empty()) continue;
    const std::string path =
        db->GetName() + "/" + f.name;  // LiveFileMetaData::name 为文件名
    rocksdb::Options opt;
    opt.comparator = rocksdb::BytewiseComparator();
    rocksdb::SstFileReader reader(opt);
    if (!reader.Open(path).ok()) continue;
    // NewTableIterator：按 internal key 迭代（含 seq），才能比较版本新旧。
    std::unique_ptr<rocksdb::Iterator> it = reader.NewTableIterator();
    if (it == nullptr) continue;
    // 目标 internal key = user_key + kMaxSequenceNumber + kValueTypeForSeek
    std::string target = key;
    rocksdb::PutFixed64(&target, rocksdb::PackSequenceAndType(
                                     rocksdb::kMaxSequenceNumber,
                                     rocksdb::kValueTypeForSeek));
    it->Seek(rocksdb::Slice(target));
    if (!it->Valid()) continue;
    rocksdb::ParsedInternalKey pik;
    if (!rocksdb::ParseInternalKey(it->key(), &pik, false /* log_err */).ok()) {
      continue;
    }
    if (pik.user_key.ToString() != key) continue;
    SstHit h;
    h.level = f.level;
    h.file = f.file_number;
    h.seq = pik.sequence;
    h.cls = ClassifyValue(it->value().ToString(), key_v, value_size);
    out->push_back(h);
  }
}

uint64_t Prop(rocksdb::DB* db, const char* name) {
  std::string v;
  if (!db->GetProperty(std::string("rocksdb.zeroflush.") + name, &v)) {
    return 0;
  }
  return strtoull(v.c_str(), nullptr, 10);
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 4) {
    fprintf(stderr,
            "usage: %s <dbdir> <all|verify|walhit|l1hit> <num_keys> "
            "[threads] [per_thread]\n",
            argv[0]);
    return 2;
  }
  const std::string dbdir = argv[1];
  const std::string mode = argv[2];
  const uint64_t num_keys = strtoull(argv[3], nullptr, 10);
  const int threads = argc > 4 ? atoi(argv[4]) : 4;
  const uint64_t per_thread = argc > 5 ? strtoull(argv[5], nullptr, 10) : 20000;
  const uint64_t total = num_keys > 0 ? num_keys : per_thread * threads;

  const bool do_write = (mode == "all" || mode == "write");
  const bool do_flush = (mode == "all" || mode == "l1hit");
  const bool write_only = (mode == "write");

  Stats st;
  std::unique_ptr<rocksdb::DB> db = OpenDb(dbdir, do_write);
  if (db == nullptr) {
    return 1;
  }

  if (mode == "triage") {
    // 三分类判决：seed2 是否在 SST（任意层）/ 是否在 WAL。
    // 诊断前提：ZF_VC_KEEP_WAL=1（关闭 WAL 回收），否则 WAL 侧证据不可信。
    const uint64_t half = total / 2;
    const std::string wal_dir = dbdir + "/zfwal";
    std::vector<std::pair<uint32_t, uint32_t>> wfiles;
    if (DIR* d = opendir(wal_dir.c_str())) {
      while (struct dirent* e = readdir(d)) {
        uint32_t p = 0, g = 0;
        if (ParseZfWalName(e->d_name, &p, &g)) wfiles.emplace_back(p, g);
      }
      closedir(d);
    }
    std::sort(wfiles.begin(), wfiles.end());
    // WAL 全量读入一次（诊断规模小；关闭回收后文件保留）
    std::vector<std::pair<std::pair<uint32_t, uint32_t>, std::string>> walbuf;
    for (const auto& pg : wfiles) {
      std::ifstream in(ZfWalPath(wal_dir, pg.first, pg.second), std::ios::binary);
      if (!in.good()) continue;
      walbuf.emplace_back(pg, std::string((std::istreambuf_iterator<char>(in)),
                                          std::istreambuf_iterator<char>()));
    }
    uint64_t n_fail = 0, b1 = 0, b2 = 0, b3 = 0, seq_inversion = 0;
    // B3 拆分（用户 2026-09-15）：L0 命中说明走的是 ZF 的 L0 fallback 路径，
    // 与"无 L0 模式"下的可见性不能混为一谈——无 L0 上线前必须重测。
    uint64_t b3_l0 = 0, b3_l1plus = 0;
    int printed = 0;
    FILE* keylist = fopen("triage_fail_keys.txt", "w");
    for (uint64_t v = 0; v < half; ++v) {
      if (v % 1000 == 0) continue;
      const std::string key = zf_keygen::KeyFromInt(v, kKeySize);
      std::string got;
      const rocksdb::Status s0 = db->Get(rocksdb::ReadOptions(), key, &got);
      if (s0.ok() && got == zf_keygen::ValueFor(v, 2, kValueSize)) continue;
      ++n_fail;
      if (keylist != nullptr) fprintf(keylist, "%llu\n", (unsigned long long)v);
      // WAL 侧
      std::vector<SeedClass> wal_cls;
      std::vector<uint64_t> wal_seq;
      for (const auto& [pg, buf] : walbuf) {
        size_t off = 0;
        while (off + zeroflush::kZfHeaderSize <= buf.size()) {
          const uint32_t klen = *reinterpret_cast<const uint32_t*>(buf.data() + off + 8);
          const uint32_t vlen = *reinterpret_cast<const uint32_t*>(buf.data() + off + 12);
          const uint32_t total_len = zeroflush::ZfRecordLength(klen, vlen);
          if (off + total_len > buf.size()) break;
          zeroflush::ZfRecordHeader h;
          rocksdb::Slice k, val;
          if (zeroflush::DecodeZfRecord(buf.data() + off, total_len, &h, &k, &val).ok() &&
              k.size() == key.size() && memcmp(k.data(), key.data(), k.size()) == 0) {
            wal_cls.push_back(ClassifyValue(val.ToString(), v, kValueSize));
            wal_seq.push_back(h.seq);
          }
          off += total_len;
        }
      }
      // SST 侧（全部层）
      std::vector<SstHit> sst;
      ScanSstsFor(db.get(), v, kValueSize, &sst);
      bool seed2_sst = false, seed2_wal = false;
      uint64_t seq2_sst = 0, seq1_sst = 0, seq2_wal = 0, seq1_wal = 0;
      for (const auto& h : sst) {
        if (h.cls == kSeed2) { seed2_sst = true; seq2_sst = std::max(seq2_sst, h.seq); }
        if (h.cls == kSeed1) { seq1_sst = std::max(seq1_sst, h.seq); }
      }
      for (size_t i = 0; i < wal_cls.size(); ++i) {
        if (wal_cls[i] == kSeed2) { seed2_wal = true; seq2_wal = std::max(seq2_wal, wal_seq[i]); }
        if (wal_cls[i] == kSeed1) { seq1_wal = std::max(seq1_wal, wal_seq[i]); }
      }
      const char* bucket;
      if (seed2_sst) {
        bucket = "B3_visible_in_sst";
        ++b3;
        if (seq2_sst > seq1_sst) ++seq_inversion;
        int min_lvl = 99;
        for (const auto& h : sst) {
          if (h.cls == kSeed2 && h.level < min_lvl) min_lvl = h.level;
        }
        if (min_lvl <= 0) {
          ++b3_l0;   // ZF L0 fallback 安装的输出
        } else {
          ++b3_l1plus;
        }
      } else if (seed2_wal) {
        bucket = "B2_in_wal_not_materialized";
        ++b2;
      } else {
        bucket = "B1_not_written";
        ++b1;
      }
      if (printed < 6) {
        printf("%s v=%llu read=%s read_seq=%llu | sst_hits=%zu (seq2=%llu "
               "seq1=%llu) | wal_hits=%zu (seq2=%llu seq1=%llu)\n",
               bucket, (unsigned long long)v,
               !s0.ok() ? "NOTFOUND"
                        : (got == zf_keygen::ValueFor(v, 1, kValueSize) ? "seed1"
                                                                      : "other"),
               (unsigned long long)db->GetLatestSequenceNumber(), sst.size(),
               (unsigned long long)seq2_sst, (unsigned long long)seq1_sst,
               wal_cls.size(), (unsigned long long)seq2_wal,
               (unsigned long long)seq1_wal);
        for (const auto& h : sst) {
          printf("    sst L%d file=%llu seq=%llu cls=%d\n", h.level,
                 (unsigned long long)h.file, (unsigned long long)h.seq,
                 (int)h.cls);
        }
        // 索引链：按读路径顺序（active 起、frozen 新→旧）
        std::string chain;
        if (zeroflush::DumpKeyIndexChain(db.get(), key, &chain).ok()) {
          printf("%s", chain.c_str());
        }
        ++printed;
      }
    }
    if (keylist != nullptr) fclose(keylist);
    printf("triage: fail=%llu B1_not_written=%llu B2_in_wal=%llu "
           "B3_visible_in_sst=%llu (B3 中 seq2>seq1 = %llu) "
           "| B3 拆分: in_L0=%llu in_L1plus=%llu\n",
           (unsigned long long)n_fail, (unsigned long long)b1,
           (unsigned long long)b2, (unsigned long long)b3,
           (unsigned long long)seq_inversion, (unsigned long long)b3_l0,
           (unsigned long long)b3_l1plus);
    printf("wal_files=%zu (reclaim=%s)\n", wfiles.size(),
           getenv("ZF_VC_KEEP_WAL") != nullptr ? "off" : "on");
    return 0;
  }

  if (mode == "deep") {
    // 二分定位：对不匹配键直接扫 zfwal，判定是"seed2 根本不在 WAL"（写路径）
    // 还是"在 WAL 但读不到"（索引/读视图）。用户 2026-09-15 指定的分类法。
    const uint64_t half = total / 2;
    const std::string wal_dir = dbdir + "/zfwal";
    // 收集 zfwal 文件
    std::vector<std::pair<uint32_t, uint32_t>> files;  // (part, gen)
    {
      DIR* d = opendir(wal_dir.c_str());
      if (d == nullptr) {
        printf("deep: cannot open %s\n", wal_dir.c_str());
        return 1;
      }
      struct dirent* e;
      while ((e = readdir(d)) != nullptr) {
        uint32_t p = 0, g = 0;
        if (ParseZfWalName(e->d_name, &p, &g)) {
          files.emplace_back(p, g);
        }
      }
      closedir(d);
    }
    std::sort(files.begin(), files.end());
    int printed = 0;
    uint64_t n_mismatch = 0, n_in_wal = 0, n_not_in_wal = 0;
    for (uint64_t v = 0; v < total && printed < 12; ++v) {
      if (v % 1000 == 0) continue;
      if (v >= half) continue;  // 只查"应被覆盖"的键
      const std::string key = zf_keygen::KeyFromInt(v, kKeySize);
      std::string got;
      const rocksdb::Status s0 =
          db->Get(rocksdb::ReadOptions(), key, &got);
      const std::string s1 = zf_keygen::ValueFor(v, 1, kValueSize);
      const std::string s2 = zf_keygen::ValueFor(v, 2, kValueSize);
      if (s0.ok() && got == s2) continue;  // 正确
      ++n_mismatch;
      // 扫 WAL：列出该键的全部记录
      std::vector<std::string> hits;
      for (const auto& [part, gen] : files) {
        const std::string path = ZfWalPath(wal_dir, part, gen);
        std::ifstream in(path, std::ios::binary);
        if (!in.good()) continue;
        std::string buf((std::istreambuf_iterator<char>(in)),
                        std::istreambuf_iterator<char>());
        size_t off = 0;
        while (off + zeroflush::kZfHeaderSize <= buf.size()) {
          zeroflush::ZfRecordHeader h;
          rocksdb::Slice k, val;
          const uint32_t total_len =
              zeroflush::ZfRecordLength(
                  *reinterpret_cast<const uint32_t*>(buf.data() + off + 8),
                  *reinterpret_cast<const uint32_t*>(buf.data() + off + 12));
          if (off + total_len > buf.size()) break;
          if (zeroflush::DecodeZfRecord(buf.data() + off, total_len, &h, &k,
                                        &val)
                  .ok() &&
              k.size() == key.size() && memcmp(k.data(), key.data(), k.size()) == 0) {
            const bool is1 = (val.ToString() == s1);
            const bool is2 = (val.ToString() == s2);
            char tmp[160];
            snprintf(tmp, sizeof(tmp),
                     "part=%u gen=%u seq=%llu off=%zu %s%s", part, gen,
                     (unsigned long long)h.seq, off, is1 ? "seed1" : "",
                     is2 ? "seed2" : "");
            hits.emplace_back(tmp);
          }
          off += total_len;
        }
      }
      bool seed2_in_wal = false;
      for (const auto& hh : hits) {
        if (hh.find("seed2") != std::string::npos) seed2_in_wal = true;
      }
      if (hits.empty()) {
        ++n_not_in_wal;
      } else {
        ++n_in_wal;
      }
      printf("FAIL v=%llu read=%s wal_hits=%zu seed2_in_wal=%d\n",
             (unsigned long long)v,
             !s0.ok() ? "NOTFOUND" : (got == s1 ? "seed1" : "other"),
             hits.size(), (int)seed2_in_wal);
      for (const auto& hh : hits) {
        printf("      %s\n", hh.c_str());
      }
      ++printed;
    }
    printf("deep: sample_mismatch=%llu (wal_has_key=%llu wal_no_key=%llu) "
           "files=%zu\n",
           (unsigned long long)n_mismatch, (unsigned long long)n_in_wal,
           (unsigned long long)n_not_in_wal, files.size());
    return 0;
  }

  if (mode == "scan") {
    // 扫描全部键，打印前 N 个不匹配样本（定位"错版本"还是"损坏"）。
    const uint64_t half = total / 2;
    int printed = 0;
    uint64_t n_mismatch = 0, n_seed1_where2 = 0, n_seed2_where1 = 0, n_other = 0;
    for (uint64_t v = 0; v < total; ++v) {
      if (v % 1000 == 0) continue;  // 删除键跳过
      std::string got;
      const rocksdb::Status st0 =
          db->Get(rocksdb::ReadOptions(), zf_keygen::KeyFromInt(v, kKeySize),
                  &got);
      const std::string s1 = zf_keygen::ValueFor(v, 1, kValueSize);
      const std::string s2 = zf_keygen::ValueFor(v, 2, kValueSize);
      const bool want2 = (v < half);
      const bool got_ok = st0.ok() && ((want2 && got == s2) || (!want2 && got == s1));
      if (got_ok) continue;
      ++n_mismatch;
      if (!st0.ok()) {
        ++n_other;
      } else if (got == s1 && want2) {
        ++n_seed1_where2;   // 覆盖写丢失
      } else if (got == s2 && !want2) {
        ++n_seed2_where1;   // 不该被覆盖的键被覆盖
      } else {
        ++n_other;          // 两者都不是 = 值损坏
      }
      if (printed < 8) {
        printf("MISMATCH v=%llu want=%s got_len=%zu eq1=%d eq2=%d st=%s\n",
               (unsigned long long)v, want2 ? "seed2" : "seed1", got.size(),
               (int)(got == s1), (int)(got == s2), st0.ToString().c_str());
        ++printed;
      }
    }
    printf("index_insert_dropped=%llu bloom_slowpath=%llu\n",
           (unsigned long long)Prop(db.get(), "index_insert_dropped"),
           (unsigned long long)Prop(db.get(), "bloom_slowpath"));
    printf("scan: mismatch=%llu (lost_overwrite=%llu unexpected_overwrite=%llu "
           "other=%llu) / total=%llu\n",
           (unsigned long long)n_mismatch,
           (unsigned long long)n_seed1_where2,
           (unsigned long long)n_seed2_where1, (unsigned long long)n_other,
           (unsigned long long)total);
    return n_mismatch == 0 ? 0 : 1;
  }

  if (mode == "probe") {
    // 诊断模式：直接对比实际值与参考值种子（定位不匹配是模型问题还是数据问题）
    for (uint64_t v : {0ull, 1ull, 50ull, 100ull, 200ull, 10000ull, 10001ull,
                       15000ull, total > 0 ? total - 1 : 0ull}) {
      std::string got;
      const rocksdb::Status s0 =
          db->Get(rocksdb::ReadOptions(), zf_keygen::KeyFromInt(v, kKeySize),
                  &got);
      const std::string s1 = zf_keygen::ValueFor(v, 1, kValueSize);
      const std::string s2 = zf_keygen::ValueFor(v, 2, kValueSize);
      printf("v=%llu status=%s len=%zu eq_seed1=%d eq_seed2=%d got0=%02x "
             "s1_0=%02x s2_0=%02x\n",
             (unsigned long long)v, s0.ToString().c_str(), got.size(),
             (int)(got == s1), (int)(got == s2),
             got.empty() ? 0 : (unsigned char)got[0], (unsigned char)s1[0],
             (unsigned char)s2[0]);
    }
    return 0;
  }

  const rocksdb::Snapshot* snap = nullptr;
  if (do_write) {
    std::vector<std::thread> ts;
    const uint64_t chunk = total / threads + 1;
    for (int i = 0; i < threads; ++i) {
      const uint64_t lo = static_cast<uint64_t>(i) * chunk;
      const uint64_t hi = std::min<uint64_t>(lo + chunk, total);
      if (lo >= hi) break;
      ts.emplace_back(WriteRange, db.get(), lo, hi, 1);
    }
    for (auto& t : ts) t.join();

    snap = db->GetSnapshot();  // 覆盖写之前的读视图

    std::vector<std::thread> ts2;
    const uint64_t half = total / 2;
    const uint64_t chunk2 = half / threads + 1;
    for (int i = 0; i < threads; ++i) {
      const uint64_t lo = static_cast<uint64_t>(i) * chunk2;
      const uint64_t hi = std::min<uint64_t>(lo + chunk2, half);
      if (lo >= hi) break;
      ts2.emplace_back(WriteRange, db.get(), lo, hi, 2);
    }
    for (auto& t : ts2) t.join();
  }

  if (write_only) {
    // 只写不 Flush：留给 walhit 模式验证"物化前读走 WAL"这条路径。
    printf("write-only done: keys=%llu pending=%.1fMB drained=%.1fMB\n",
           (unsigned long long)total,
           (double)Prop(db.get(), "pending_materialize_bytes") / 1048576.0,
           (double)Prop(db.get(), "drained_materialize_bytes") / 1048576.0);
    return 0;
  }
  if (do_flush) {
    rocksdb::FlushOptions fo;
    fo.wait = true;
    db->Flush(fo);
  }

  const uint64_t wal_before = Prop(db.get(), "sealed_read_count");
  const uint64_t miss_before = Prop(db.get(), "sealed_cache_miss");

  {
    std::vector<std::thread> ts;
    const uint64_t chunk = total / threads + 1;
    for (int i = 0; i < threads; ++i) {
      const uint64_t lo = static_cast<uint64_t>(i) * chunk;
      const uint64_t hi = std::min<uint64_t>(lo + chunk, total);
      if (lo >= hi) break;
      ts.emplace_back(CheckRange, db.get(), nullptr, lo, hi, total / 2, &st);
    }
    for (auto& t : ts) t.join();
  }
  if (snap != nullptr) {
    CheckRange(db.get(), snap, 0, total / 2, total / 2, &st);
    db->ReleaseSnapshot(snap);
  }
  {
    rocksdb::ReadOptions ro;
    for (uint64_t i = 0; i < 64; ++i) {
      std::string got;
      const rocksdb::Status s =
          db->Get(ro, zf_keygen::KeyFromInt(total + 1000000 + i, kKeySize),
                  &got);
      if (!s.IsNotFound()) {
        ++st.errors;
      }
      st.checked.fetch_add(1);
    }
  }

  const uint64_t wal_delta =
      Prop(db.get(), "sealed_read_count") - wal_before;
  const uint64_t miss_delta =
      Prop(db.get(), "sealed_cache_miss") - miss_before;
  const uint64_t bad = st.mismatch.load() + st.miss.load() +
                       st.unexpected_found.load() + st.errors.load();

  printf("write_errors=%llu\n",
         (unsigned long long)g_write_errors.load());
  printf("mode=%s keys=%llu threads=%d\n", mode.c_str(),
         (unsigned long long)total, threads);
  printf("checked=%llu mismatch=%llu miss=%llu unexpected_found=%llu "
         "errors=%llu\n",
         (unsigned long long)st.checked.load(),
         (unsigned long long)st.mismatch.load(),
         (unsigned long long)st.miss.load(),
         (unsigned long long)st.unexpected_found.load(),
         (unsigned long long)st.errors.load());
  printf("wal_read_delta=%llu sealed_miss_delta=%llu pending=%.1fMB "
         "drained=%.1fMB reclaimed=%.1fMB gens=%zu\n",
         (unsigned long long)wal_delta, (unsigned long long)miss_delta,
         (double)Prop(db.get(), "pending_materialize_bytes") / 1048576.0,
         (double)Prop(db.get(), "drained_materialize_bytes") / 1048576.0,
         (double)Prop(db.get(), "reclaimed_bytes") / 1048576.0,
         (size_t)Prop(db.get(), "registered_gens"));
  // 命中路径断言：walhit 模式必须真的从 WAL 读到（否则"WAL 命中"未被验证）
  bool path_ok = true;
  if (mode == "walhit" && wal_delta == 0) {
    printf("path_assert: FAIL (walhit 模式但 WAL 读数为 0——数据可能已物化)\n");
    path_ok = false;
  }
  if (mode == "l1hit" && wal_delta != 0) {
    printf("path_assert: WARN (l1hit 模式仍发生 %llu 次 WAL 读)\n",
           (unsigned long long)wal_delta);
  }
  const bool ok = (bad == 0) && path_ok;
  printf("%s\n", ok ? "VALCHECK PASS" : "VALCHECK FAIL");
  return ok ? 0 : 1;
}
