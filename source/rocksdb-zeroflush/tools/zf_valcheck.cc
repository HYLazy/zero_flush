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
#include "db/db_impl/db_impl.h"
#include "zeroflush/partition_index.h"
#include "zeroflush/partition_table.h"
#include "zeroflush/wal_manager.h"
#include "zeroflush/zeroflush_db.h"

namespace {
// 测试用：毫秒级 sleep（避免依赖 <chrono> 之外的实现细节）。
void env_sleep_ms(int ms) {
  rocksdb::Env::Default()->SleepForMicroseconds(ms * 1000);
}


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

// 按**完整属性名**取数（用于 zeroflush 前缀之外的名字，如 gen_read_refs:p:g）。
uint64_t Prop2(rocksdb::DB* db, const char* full_name) {
  std::string v;
  if (!db->GetProperty(full_name, &v)) {
    return 0;
  }
  return strtoull(v.c_str(), nullptr, 10);
}

uint64_t Prop(rocksdb::DB* db, const char* name) {
  std::string v;
  if (!db->GetProperty(std::string("rocksdb.zeroflush.") + name, &v)) {
    return 0;
  }
  return strtoull(v.c_str(), nullptr, 10);
}

void DiagKeyState(rocksdb::DB* db,
                  const std::shared_ptr<zeroflush::ZeroFlushContext>& ctx,
                  const std::string& dbdir, uint64_t key_v, int key_size,
                  int val_size);

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
          // step 1z（用户 2026-09-16 第 2 步）：**期望存在却 NotFound** 的键
          // 必须当场取证，不能只记"排空停滞"——未安装数据应继续可读。
          static std::atomic<int> zf_miss_diag{0};
          if (zf_miss_diag.fetch_add(1) < 3) {
            auto* impl = static_cast<rocksdb::DBImpl*>(db);
            auto ctx = impl->GetZeroFlushContext();
            if (ctx != nullptr) {
              fprintf(stderr,
                      "ZFDBG-missdiag v=%llu read_seq=%llu expect=seed%d "
                      "status=%s\n",
                      (unsigned long long)v,
                      (unsigned long long)db->GetLatestSequenceNumber(),
                      v < half ? 2 : 1, s.ToString().c_str());
              DiagKeyState(db, ctx, impl->GetName(), v, kKeySize, kValueSize);
            }
          }
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
          // 快照视图期望 seed1 却 NotFound：当场取证（排除删除键，见上方分支）。
          static std::atomic<int> zf_snapmiss_diag{0};
          if (s.IsNotFound() && zf_snapmiss_diag.fetch_add(1) < 3) {
            auto* impl = static_cast<rocksdb::DBImpl*>(db);
            auto ctx = impl->GetZeroFlushContext();
            if (ctx != nullptr) {
              fprintf(stderr,
                      "ZFDBG-snapmissdiag v=%llu snap_seq=%llu latest=%llu\n",
                      (unsigned long long)v,
                      (unsigned long long)(snap != nullptr
                                               ? snap->GetSequenceNumber()
                                               : 0),
                      (unsigned long long)db->GetLatestSequenceNumber());
              DiagKeyState(db, ctx, impl->GetName(), v, kKeySize, kValueSize);
            }
          }
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
  // 压力用例需要频繁触发封存：允许按 KB/MB 下调阈值（默认不变）。
  if (const char* kb = getenv("ZF_VC_PARTITION_TARGET_KB")) {
    const uint64_t v = strtoull(kb, nullptr, 10);
    if (v > 0) {
      zfo.partition_target_bytes = v << 10;
      // epoch 目标独立控制（见 ZF_VC_EPOCH_TARGET_MB）；未显式指定时
      // 保持与分区目标同量级，以便小规模数据也能触发"封存→物化"。
      if (getenv("ZF_VC_EPOCH_TARGET_MB") == nullptr &&
          zfo.epoch_target_bytes > zfo.partition_target_bytes) {
        zfo.epoch_target_bytes = zfo.partition_target_bytes;
      }
    }
  }
  if (const char* mb = getenv("ZF_VC_PARTITION_TARGET_MB")) {
    const uint64_t v = strtoull(mb, nullptr, 10);
    if (v > 0) {
      zfo.partition_target_bytes = v << 20;
      if (zfo.epoch_target_bytes > zfo.partition_target_bytes) {
        zfo.epoch_target_bytes = zfo.partition_target_bytes;
      }
    }
  }
  // 诊断运行必须关闭 WAL 回收：否则"物化后 unlink"会让"WAL 里没有 seed2"
  // 无法区分"从未写入"与"已物化后删除"（用户 2026-09-15 指定）。
  if (getenv("ZF_VC_KEEP_WAL") != nullptr) {
    zfo.reclaim_sealed_files = false;
  }
  // 压力组 4：把 epoch 目标撑大，使"分区已封存但 epoch 未满"→ 记录保持
  // **已封存且未安装**，从而真正覆盖 sealed 代读路径（否则很快被物化安装）。
  if (const char* em = getenv("ZF_VC_EPOCH_TARGET_MB")) {
    const uint64_t v = strtoull(em, nullptr, 10);
    if (v > 0) zfo.epoch_target_bytes = v << 20;
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

// step 1z：失败键的**同一时点状态**（用户 2026-09-16 第 2 步）。
// 用于区分"未安装数据的读依赖被撤掉"与"已安装数据不在当前读视图"。
void DiagKeyState(rocksdb::DB* db,
                  const std::shared_ptr<zeroflush::ZeroFlushContext>& ctx,
                  const std::string& dbdir, uint64_t key_v, int key_size,
                  int val_size) {
  const std::string key = zf_keygen::KeyFromInt(key_v, key_size);
  const rocksdb::Slice uk(key);
  std::string fx;
  db->GetProperty("rocksdb.zeroflush.installed_fences", &fx);
  fprintf(stderr, "  DIAG routed_part=%u fences=%s\n", ctx->RoutePartId(uk),
          fx.empty() ? "(none)" : fx.c_str());
  std::vector<std::pair<uint32_t, uint64_t>> allp;
  ctx->index_set()->ChainParts(&allp);
  for (const auto& [p, ub] : allp) {
    std::string o;
    ctx->index_set()->DumpKeyChain(p, uk, &o);
    if (o.find("seq=") != std::string::npos) {
      fprintf(stderr, "  DIAG index logical_part=%u upper=%llu\n%s", p,
              (unsigned long long)ub, o.c_str());
    }
  }
  const std::string wdir = dbdir + "/zfwal";
  std::vector<std::pair<uint32_t, uint32_t>> wfs;
  if (DIR* d = opendir(wdir.c_str())) {
    while (struct dirent* e = readdir(d)) {
      uint32_t pp = 0, gg = 0;
      if (ParseZfWalName(e->d_name, &pp, &gg)) wfs.emplace_back(pp, gg);
    }
    closedir(d);
  }
  std::sort(wfs.begin(), wfs.end());
  for (const auto& [pp, gg] : wfs) {
    std::ifstream in(ZfWalPath(wdir, pp, gg), std::ios::binary);
    if (!in.good()) continue;
    const std::string buf((std::istreambuf_iterator<char>(in)),
                          std::istreambuf_iterator<char>());
    size_t off = 0;
    while (off + zeroflush::kZfHeaderSize <= buf.size()) {
      const uint32_t klen =
          *reinterpret_cast<const uint32_t*>(buf.data() + off + 8);
      const uint32_t vlen =
          *reinterpret_cast<const uint32_t*>(buf.data() + off + 12);
      const uint32_t tl = zeroflush::ZfRecordLength(klen, vlen);
      if (off + tl > buf.size()) break;
      zeroflush::ZfRecordHeader h;
      rocksdb::Slice k, val;
      if (zeroflush::DecodeZfRecord(buf.data() + off, tl, &h, &k, &val).ok() &&
          klen == key.size() && memcmp(k.data(), key.data(), key.size()) == 0) {
        const SeedClass cls = ClassifyValue(val.ToString(), key_v, val_size);
        std::string d;
        db->GetProperty("rocksdb.zeroflush.describe_gen:" +
                            std::to_string(pp) + ":" + std::to_string(gg),
                        &d);
        fprintf(stderr, "  DIAG wal part=%u gen=%u off=%zu seq=%llu cls=%s | %s\n",
                pp, gg, off, (unsigned long long)h.seq,
                cls == kSeed2 ? "seed2" : (cls == kSeed1 ? "seed1" : "other"),
                d.c_str());
      }
      off += tl;
    }
  }
}


// 文件元信息声明的 [smallest, largest] 是否覆盖 user key（兼容 internal key：
// 尾 8B 为 PackSequenceAndType，去掉后按 user key 比较）。
static bool ZfKeyRangeCovers(const std::string& smallest,
                             const std::string& largest, const std::string& uk) {
  auto strip = [](const std::string& s) {
    if (s.size() >= 8) {
      const uint64_t packed =
          rocksdb::DecodeFixed64(s.data() + s.size() - 8);
      const uint8_t type = static_cast<uint8_t>(packed & 0xFF);
      if (type == 0 || type == 1 || type == 15 || type == 17 || type == 22) {
        return s.substr(0, s.size() - 8);
      }
    }
    return s;
  };
  const std::string lo = strip(smallest);
  const std::string hi = strip(largest);
  const rocksdb::Comparator* cmp = rocksdb::BytewiseComparator();
  return cmp->Compare(uk, lo) >= 0 && cmp->Compare(uk, hi) <= 0;
}

struct SstHit {
  int level = -1;
  uint64_t file = 0;
  uint64_t seq = 0;
  SeedClass cls = kOther;
  // 文件级 seq 元信息：用于判定 L0 的"文件号序 = seq 序"不变量是否被破坏
  // （ZF 把旧记录重新物化成更高文件号时，RocksDB 的 L0 读序会返回旧值）。
  uint64_t file_smallest_seqno = 0;
  uint64_t file_largest_seqno = 0;
  std::string file_smallest_key;
  std::string file_largest_key;
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
    h.file_smallest_seqno = f.smallest_seqno;
    h.file_largest_seqno = f.largest_seqno;
    h.file_smallest_key = f.smallestkey;
    h.file_largest_key = f.largestkey;
    out->push_back(h);
  }
}

}  // namespace

// step 1r（用户 2026-09-16 第 3 项）：失败键的**同一时点完整状态**——写入 seq、
// registry（归属/installed/引用）、索引（哪些链里有该键）、WAL 文件与该记录是否
// 可解码、当前 fence 与读序列。用于按证据分类（索引撤除 / 读视图交接 / 候选发现），
// 不做事后猜测。

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

  std::map<int, uint64_t> ok_by_src;  // step 1f：读来源分布（walread 用）
  // step 1f：walread/selftest 自包含（先写后读，同一进程，不 Flush）
  const bool do_write =
      (mode == "all" || mode == "write" || mode == "walread" ||
       mode == "selftest" || mode == "semtest" || mode == "stress" ||
       mode == "baccept");
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
    std::map<int, uint64_t> fail_by_source;      // 全部失败键的来源分布
    std::map<int, uint64_t> b3_by_source;        // B3 内按来源（A vs B）
    std::map<int, uint64_t> b2_by_source;
    std::map<int, uint64_t> ok_by_source;        // 读正确的键走的那条路
    // B3 拆分（用户 2026-09-15）：L0 命中说明走的是 ZF 的 L0 fallback 路径，
    // 与"无 L0 模式"下的可见性不能混为一谈——无 L0 上线前必须重测。
    uint64_t b3_l0 = 0, b3_l1plus = 0;
    // step 1c-2 验收：按 (part, gen) × bucket 归属，并与 fence_last 对照。
    // 判据（用户 2026-09-15）：任何 gen <= fence_last(part) 的行都不得再出现
    // "WAL-only seed2"（B2）——那意味着 fence 过早推进、该代数据无处可读。
    std::map<std::pair<uint32_t, uint32_t>, uint64_t> b2_by_gen, b3_by_gen;
    // B3（数据已在 SST、读返回陈旧 WAL 记录）中"陈旧记录的 seq 上界仍低于
    // SST 的 seed2 seq"的键数——这些键只要做一次序列号比较就会返回正确值
    // （即：读路径缺的不是数据，而是与已安装层的 seq 比较）。
    std::map<std::pair<uint32_t, uint32_t>, uint64_t> b3_stale_by_gen;
    uint64_t b3_would_fix_by_seqcmp = 0;
    uint64_t b3_index_hit = 0, b3_index_miss = 0;
    std::map<uint32_t, uint32_t> fence_last;  // part → fence.last_generation
    {
      std::string fences;
      if (db->GetProperty("rocksdb.zeroflush.installed_fences", &fences)) {
        size_t pos = 0;
        while (pos < fences.size()) {
          if (fences[pos] != 'p') { ++pos; continue; }
          unsigned p = 0, lo = 0, hi = 0;
          if (sscanf(fences.c_str() + pos, "p%u=[%u,%u]", &p, &lo, &hi) == 3) {
            fence_last[p] = hi;
          }
          ++pos;
        }
      }
    }
    int printed = 0;
    FILE* keylist = fopen("triage_fail_keys.txt", "w");
    // step 1d：按桶分文件留档，便于对"某类失败"直接取样做单键追踪。
    FILE* keylist_b1 = fopen("triage_fail_keys_b1.txt", "w");
    FILE* keylist_b2 = fopen("triage_fail_keys_b2.txt", "w");
    FILE* keylist_b3 = fopen("triage_fail_keys_b3.txt", "w");
    for (uint64_t v = 0; v < half; ++v) {
      if (v % 1000 == 0) continue;
      const std::string key = zf_keygen::KeyFromInt(v, kKeySize);
      std::string got;
      const rocksdb::Status s0 = db->Get(rocksdb::ReadOptions(), key, &got);
      if (s0.ok() && got == zf_keygen::ValueFor(v, 2, kValueSize)) {
        ++ok_by_source[(int)zeroflush::get_trace::LastSourceTls()];
        continue;
      }
      ++n_fail;
      // A/B 按**实际返回来源**统计（用户 2026-09-15）：索引返回旧值 = A，
      // LSM 自身返回旧值 = B。此前用"新值所在层"统计属错误口径。
      const zeroflush::get_trace::GetSource src =
          zeroflush::get_trace::LastSourceTls();
      ++fail_by_source[(int)src];
      if (keylist != nullptr) fprintf(keylist, "%llu\n", (unsigned long long)v);
      // WAL 侧
      std::vector<SeedClass> wal_cls;
      std::vector<uint64_t> wal_seq;
      // step 1c-2 验收：记录 seed2 记录命中的 (physical_part, gen) 文件——
      // 用于判定"该 generation 是否已被 fence 声明安装"（gen <= fence_last）。
      std::vector<std::pair<uint32_t, uint32_t>> wal_seed2_pg;
      std::vector<std::pair<uint32_t, uint32_t>> wal_seed1_pg;
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
            const SeedClass cls = ClassifyValue(val.ToString(), v, kValueSize);
            wal_cls.push_back(cls);
            wal_seq.push_back(h.seq);
            if (cls == kSeed2) {
              wal_seed2_pg.push_back(pg);
            } else if (cls == kSeed1) {
              wal_seed1_pg.push_back(pg);
            }
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
        ++b3_by_source[(int)src];
        for (const auto& pg : wal_seed2_pg) ++b3_by_gen[pg];
        // 陈旧 WAL 记录（seed1）所在代：读路径实际返回的是它。
        for (const auto& pg : wal_seed1_pg) ++b3_stale_by_gen[pg];
        // 该键最早可被读到的 stale 记录 seq 上界 = WAL 中 seed1 的 max seq
        // （被返回的那个必然 <= 它）。若它仍 < SST 的 seed2 seq，说明数据面
        // 无歧义：一次序列号比较即可返回正确值。
        if (seq1_wal < seq2_sst) ++b3_would_fix_by_seqcmp;
        // 读路径 provenance：索引链是否命中该键（命中 = 读返回的是 WAL 索引
        // 记录；未命中 = 读返回的是 LSM 里的某个更旧版本）。
        std::string ch;
        if (zeroflush::DumpKeyIndexChain(db.get(), key, &ch).ok()) {
          if (ch.find("seq=") != std::string::npos) {
            ++b3_index_hit;
          } else {
            ++b3_index_miss;
          }
        }
      } else if (seed2_wal) {
        bucket = "B2_in_wal_not_materialized";
        ++b2;
        ++b2_by_source[(int)src];
        for (const auto& pg : wal_seed2_pg) ++b2_by_gen[pg];
        if (keylist_b2 != nullptr) fprintf(keylist_b2, "%llu\n", (unsigned long long)v);
      } else {
        bucket = "B1_not_written";
        ++b1;
        if (keylist_b1 != nullptr) fprintf(keylist_b1, "%llu\n", (unsigned long long)v);
      }
      if (keylist_b3 != nullptr && seed2_sst) {
        fprintf(keylist_b3, "%llu\n", (unsigned long long)v);
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
          // 该文件元信息声明的 key 范围是否覆盖本键（不含 seq 尾 8B）：
          // 未覆盖 = 文件里虽有记录、LSM 读却按元信息跳过（范围元数据缺陷）。
          const bool covers =
              !h.file_smallest_key.empty() && !h.file_largest_key.empty() &&
              ZfKeyRangeCovers(h.file_smallest_key, h.file_largest_key, key);
          printf("    sst L%d file=%llu seq=%llu cls=%d fseq=[%llu,%llu] "
                 "covers=%d\n",
                 h.level, (unsigned long long)h.file,
                 (unsigned long long)h.seq, (int)h.cls,
                 (unsigned long long)h.file_smallest_seqno,
                 (unsigned long long)h.file_largest_seqno, covers ? 1 : 0);
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
    if (keylist_b1 != nullptr) fclose(keylist_b1);
    if (keylist_b2 != nullptr) fclose(keylist_b2);
    if (keylist_b3 != nullptr) fclose(keylist_b3);
    printf("triage: fail=%llu B1_not_written=%llu B2_in_wal=%llu "
           "B3_visible_in_sst=%llu (B3 中 seq2>seq1 = %llu) "
           "| B3 拆分: in_L0=%llu in_L1plus=%llu\n",
           (unsigned long long)n_fail, (unsigned long long)b1,
           (unsigned long long)b2, (unsigned long long)b3,
           (unsigned long long)seq_inversion, (unsigned long long)b3_l0,
           (unsigned long long)b3_l1plus);
    printf("wal_files=%zu (reclaim=%s)\n", wfiles.size(),
           getenv("ZF_VC_KEEP_WAL") != nullptr ? "off" : "on");
    // step 1c-2 验收汇总：种子2 仅存于 WAL（B2）的行按 (part,gen) 归属，
    // 对照该 part 的 fence_last；gen <= fence 视为"fence 过早推进"。
    uint64_t b2_over = 0, b2_under = 0;
    printf("fences:");
    for (const auto& [p, hi] : fence_last) {
      printf(" p%u<=%u", p, hi);
    }
    printf("%s\n", fence_last.empty() ? " (none)" : "");
    for (const auto& [pg, cnt] : b2_by_gen) {
      auto fit = fence_last.find(pg.first);
      const bool has_f = (fit != fence_last.end());
      const bool over = has_f && pg.second <= fit->second;
      if (over) {
        b2_over += cnt;
      } else {
        b2_under += cnt;
      }
      printf("B2_by_gen (part=%u gen=%u): n=%llu fence_last=%s gen_le_fence=%d\n",
             pg.first, pg.second, (unsigned long long)cnt,
             has_f ? std::to_string(fit->second).c_str() : "none",
             over ? 1 : 0);
    }
    for (const auto& [pg, cnt] : b3_stale_by_gen) {
      auto fit = fence_last.find(pg.first);
      const bool has_f = (fit != fence_last.end());
      printf("B3_stale_seed1_by_gen (part=%u gen=%u): n=%llu fence_last=%s "
             "gen_le_fence=%d\n",
             pg.first, pg.second, (unsigned long long)cnt,
             has_f ? std::to_string(fit->second).c_str() : "none",
             (has_f && pg.second <= fit->second) ? 1 : 0);
    }
    auto print_src = [](const char* tag, const std::map<int, uint64_t>& m) {
      printf("%s:", tag);
      for (const auto& [k, c] : m) {
        printf(" %s=%llu", zeroflush::get_trace::SourceName(
                               static_cast<zeroflush::get_trace::GetSource>(k)),
               (unsigned long long)c);
      }
      printf("\n");
    };
    print_src("SOURCE_all_fail", fail_by_source);
    print_src("SOURCE_B2", b2_by_source);
    print_src("SOURCE_B3(A=INDEX返回旧值 B=LSM自身返回旧值)", b3_by_source);
    print_src("SOURCE_read_ok", ok_by_source);
    printf("B3_stale_seq_below_sst_seed2=%llu (这些键只需 seq 比较即可修正)\n",
           (unsigned long long)b3_would_fix_by_seqcmp);
    printf("B3_provenance: index_chain_hit=%llu index_chain_miss=%llu\n",
           (unsigned long long)b3_index_hit, (unsigned long long)b3_index_miss);
    for (const auto& [pg, cnt] : b3_by_gen) {
      auto fit = fence_last.find(pg.first);
      const bool has_f = (fit != fence_last.end());
      printf("B3_by_gen (part=%u gen=%u): n=%llu fence_last=%s gen_le_fence=%d\n",
             pg.first, pg.second, (unsigned long long)cnt,
             has_f ? std::to_string(fit->second).c_str() : "none",
             (has_f && pg.second <= fit->second) ? 1 : 0);
    }
    printf("ACCEPTANCE: B2_records_with_gen_le_fence=%llu "
           "B2_records_with_gen_gt_fence=%llu -> %s\n",
           (unsigned long long)b2_over, (unsigned long long)b2_under,
           b2_over == 0 ? "PASS(no over-advanced fence on WAL-only data)"
                        : "FAIL(fence advanced for WAL-only generation)");
    return 0;
  }

  if (mode == "agg") {
    // 按 (physical_part, gen) 聚合：seed1/seed2 计数、seq 区间、文件大小。
    // 判定（用户 2026-09-15）：
    //   同 (part,gen) 同时有 seed1+seed2 且 seq 跨阶段 ⇒ 代内追加/复用
    //   只有 seed2 且 fence_last < gen ⇒ 恢复过滤/索引登记问题
    //   WAL/fence/分配器三者一致仍读旧值   ⇒ 转 R2（索引链缺高 seq 条目 / L0 保护）
    const std::string wal_dir = dbdir + "/zfwal";
    std::vector<std::pair<uint32_t, uint32_t>> files;
    if (DIR* d = opendir(wal_dir.c_str())) {
      while (struct dirent* e = readdir(d)) {
        uint32_t p = 0, g = 0;
        if (ParseZfWalName(e->d_name, &p, &g)) files.emplace_back(p, g);
      }
      closedir(d);
    }
    std::sort(files.begin(), files.end());
    struct Agg {
      uint64_t s1 = 0, s2 = 0, other = 0;
      uint64_t seq_min = 0, seq_max = 0;
      uint64_t size = 0;
      bool have_seq = false;
    };
    std::map<std::pair<uint32_t, uint32_t>, Agg> tab;
    for (const auto& [part, gen] : files) {
      std::ifstream in(ZfWalPath(wal_dir, part, gen), std::ios::binary);
      if (!in.good()) continue;
      std::string buf((std::istreambuf_iterator<char>(in)),
                      std::istreambuf_iterator<char>());
      Agg& a = tab[{part, gen}];
      a.size = buf.size();
      size_t off = 0;
      while (off + zeroflush::kZfHeaderSize <= buf.size()) {
        const uint32_t klen =
            *reinterpret_cast<const uint32_t*>(buf.data() + off + 8);
        const uint32_t vlen =
            *reinterpret_cast<const uint32_t*>(buf.data() + off + 12);
        const uint32_t total_len = zeroflush::ZfRecordLength(klen, vlen);
        if (off + total_len > buf.size()) break;
        zeroflush::ZfRecordHeader h;
        rocksdb::Slice k, val;
        if (zeroflush::DecodeZfRecord(buf.data() + off, total_len, &h, &k,
                                      &val)
                .ok()) {
          // 键 = BE64(v) + '0' 填充 ⇒ 前 8 字节反解出 v
          uint64_t v = 0;
          if (k.size() >= 8) {
            for (int i = 0; i < 8; ++i) {
              v = (v << 8) | static_cast<unsigned char>(k.data()[i]);
            }
          }
          const SeedClass cls = ClassifyValue(val.ToString(), v, kValueSize);
          if (cls == kSeed1) {
            ++a.s1;
          } else if (cls == kSeed2) {
            ++a.s2;
          } else {
            ++a.other;
          }
          if (!a.have_seq) {
            a.seq_min = a.seq_max = h.seq;
            a.have_seq = true;
          } else {
            a.seq_min = std::min<uint64_t>(a.seq_min, h.seq);
            a.seq_max = std::max<uint64_t>(a.seq_max, h.seq);
          }
        }
        off += total_len;
      }
    }
    uint64_t both_phases = 0, only2 = 0, only1 = 0, empty = 0;
    printf("%-14s %8s %8s %8s %10s %10s %10s %s\n", "(part,gen)", "seed1",
           "seed2", "other", "seq_min", "seq_max", "wal_size", "class");
    for (const auto& [pg, a] : tab) {
      const char* cls = "-";
      if (a.s1 == 0 && a.s2 == 0) {
        cls = "empty/all-other";
        ++empty;
      } else if (a.s1 > 0 && a.s2 > 0) {
        cls = "BOTH_PHASES";
        ++both_phases;
      } else if (a.s2 > 0) {
        cls = "ONLY_SEED2";
        ++only2;
      } else {
        cls = "ONLY_SEED1";
        ++only1;
      }
      printf("(%2u,%2u)      %8llu %8llu %8llu %10llu %10llu %10llu %s\n",
             pg.first, pg.second, (unsigned long long)a.s1,
             (unsigned long long)a.s2, (unsigned long long)a.other,
             (unsigned long long)a.seq_min, (unsigned long long)a.seq_max,
             (unsigned long long)a.size, cls);
    }
    printf("agg: files=%zu both_phases=%llu only_seed2=%llu only_seed1=%llu "
           "empty=%llu\n",
           tab.size(), (unsigned long long)both_phases,
           (unsigned long long)only2, (unsigned long long)only1,
           (unsigned long long)empty);
    return 0;
  }

  if (mode == "baccept") {
    // step 1v（用户 2026-09-16 第 3 步）：**B 的强制冲突验收**。
    //   写入并封存 → 物化完成 → 提交前强制冲突 → 空 edit → 释放 epoch
    //   → 验证 WAL/registry/索引/pending 均保留
    //   → 解除冲突 → 重规划并成功安装 → 释放旧读引用 → 验证能回收
    //   → 重启逐键校验
    // 断言：让位批次 AddFile=0、fence/drained 不变；成功后 fence 推进、
    //       pending 归零、可回收（保护不是永久保留）；重启后逐键零错误。
    const uint64_t n = total > 0 ? total : 20000;
    int fails = 0;
    auto prop = [&](const char* k) { return Prop(db.get(), k); };
    auto sst_count = [&]() {
      long c = 0;
      if (DIR* d = opendir(dbdir.c_str())) {
        while (struct dirent* e = readdir(d)) {
          if (strstr(e->d_name, ".sst") != nullptr) ++c;
        }
        closedir(d);
      }
      return c;
    };
    // 阶段 1：写入 + 封存 + 完整安装（基线）
    for (uint64_t v = 0; v < n; ++v) {
      const std::string k = zf_keygen::KeyFromInt(v, kKeySize);
      db->Put(rocksdb::WriteOptions(), k,
              zf_keygen::ValueFor(v, 1, kValueSize));
    }
    db->Flush(rocksdb::FlushOptions());
    {
      const uint64_t deadline =
          rocksdb::Env::Default()->NowMicros() + 30ull * 1000 * 1000;
      while (rocksdb::Env::Default()->NowMicros() < deadline &&
             prop("pending_materialize_bytes") != 0) {
        db->Flush(rocksdb::FlushOptions());
        env_sleep_ms(20);
      }
    }
    const uint64_t drained_base = prop("drained_materialize_bytes");
    std::string fx0;
    db->GetProperty("rocksdb.zeroflush.installed_fences", &fx0);
    const long sst_base = sst_count();
    printf("baccept phase1 baseline: pending=%llu drained=%llu fences=%s "
           "sst=%ld\n",
           (unsigned long long)prop("pending_materialize_bytes"),
           (unsigned long long)drained_base, fx0.c_str(), sst_base);
    // 阶段 2：强制冲突——注入**多次**（后台 flush 会立刻重试，注入 1 次会在
    // 观测窗口内被重试覆盖；注入 N 次让"让位状态"持续到我们测量完为止）。
    // 注入**足够多**次：phase2 的写入会产生多个物化批次，只挡前几次其余会
    // 正常安装（实测挡 8 次仍有 50 个 SST 落盘）。窗口内让"全部让位"。
    setenv("ZF_TEST_FORCE_INSTALL_CONFLICT", "-1000000", 1);
    for (uint64_t v = n; v < 2 * n; ++v) {
      const std::string k = zf_keygen::KeyFromInt(v, kKeySize);
      db->Put(rocksdb::WriteOptions(), k,
              zf_keygen::ValueFor(v, 1, kValueSize));
    }
    db->Flush(rocksdb::FlushOptions());
    env_sleep_ms(200);
    unsetenv("ZF_TEST_FORCE_INSTALL_CONFLICT");
    const uint64_t pend_yield = prop("pending_materialize_bytes");
    const uint64_t drained_yield = prop("drained_materialize_bytes");
    const long sst_yield = sst_count();
    std::string fx1;
    db->GetProperty("rocksdb.zeroflush.installed_fences", &fx1);
    // 判据用**引擎侧逐批断言**（聚合窗口会被"未被注入的后台批次"干扰：
    // 那些批次照常安装，不能用来判断让位批次是否合规）。
    std::string fy;
    db->GetProperty("rocksdb.zeroflush.forced_yield_batches", &fy);
    unsigned fy_b = 0, fy_d = 0, fy_p = 0;
    unsigned long long fy_out = 0;
    sscanf(fy.c_str(), "%u/%u/%u outputs=%llu", &fy_b, &fy_d, &fy_p, &fy_out);
    const bool yield_ok = (fy_b > 0) && (fy_d == fy_b) && (fy_p == fy_b) &&
                          (fy_out > 0);
    printf("baccept phase2 forced-conflict: batches=%u drained_flat=%u "
           "pending_flat=%u outputs_discarded=%llu (fences_unchanged=%d) -> %s\n",
           fy_b, fy_d, fy_p, fy_out, fx1 == fx0 ? 1 : 0,
           yield_ok ? "PASS" : "FAIL");
    if (!yield_ok) ++fails;
    // 阶段 3：解除冲突 → 重规划并安装 → pending 归零、drained 增加
    {
      uint64_t tries = 0;
      while (prop("pending_materialize_bytes") != 0 && tries < 60) {
        db->Flush(rocksdb::FlushOptions());
        env_sleep_ms(200);
        ++tries;
      }
    }
    const uint64_t pend_after = prop("pending_materialize_bytes");
    const uint64_t drained_after = prop("drained_materialize_bytes");
    const bool install_ok = (pend_after == 0) && (drained_after > drained_base);
    printf("baccept phase3 install-after-release: pending=%llu drained=%llu "
           "(>baseline=%d) -> %s\n",
           (unsigned long long)pend_after, (unsigned long long)drained_after,
           drained_after > drained_base ? 1 : 0, install_ok ? "PASS" : "FAIL");
    if (!install_ok) ++fails;
    // 阶段 4：保护不是永久保留——索引代与 generation 最终释放/回收
    const uint64_t reg = prop("registered_gens");
    const uint64_t recl = prop("reclaimed_bytes");
    const bool reclaim_ok = (reg == 0) && (recl > 0);
    printf("baccept phase4 reclaim: registered_gens=%llu reclaimed=%llu -> %s\n",
           (unsigned long long)reg, (unsigned long long)recl,
           reclaim_ok ? "PASS" : "FAIL");
    if (!reclaim_ok) ++fails;
    printf("baccept l0_install=%llu fence_no_committed_output=%llu "
           "gen_lost_before_install=%llu\n",
           (unsigned long long)prop("l0_install_count"),
           (unsigned long long)prop("fence_no_committed_output"),
           (unsigned long long)prop("gen_lost_before_install"));
    return fails == 0 ? 0 : 1;
  }

  if (mode == "stress") {
    // step 1n（用户 2026-09-16 第 2 项）：把 case5 扩成可重复压力测试。
    //   stress <group> <iterations>   group ∈ {1 基础跨 Freeze, 2 安装后回收,
    //                                          3 snapshot/delete 混合}
    // 每轮：**独立新库**（fresh DB dir）→ 写活跃代记录 → 选择后暂停 →
    //       触发 Freeze（确认 gen 变化）→ 等安装完成 → （组 2）unlink 回收 →
    //       放行读 → 严格判定。固定随机种子；30s watchdog 防死锁无输出。
    // 判定（任一即失败）：status != OK（期望 NotFound 的删除轮除外）、返回旧值、
    //   NotFound、Corruption/IOError、readfail_propagated > 0。
    // 通过条件（每组）：failures=0 silent_stale=0 notfound=0 corruption=0
    //   rotations > 0。
    const int group = static_cast<int>(total);
    const uint64_t iters = threads > 0 ? static_cast<uint64_t>(threads) : 100;
    const uint64_t seed = 20260916;
    std::srand(static_cast<unsigned>(seed));
    const std::string base_dir = dbdir;
    uint64_t failures = 0, silent_stale = 0, notfound = 0, corruption = 0;
    uint64_t rotations = 0, rounds = 0, propagated_total = 0, fellback_total = 0;
    uint64_t pin_ok_total = 0, pin_active_total = 0;
    // 保留契约守门的触发次数（证明目标路径被走到，而不是"没触发所以没失败"）
    uint64_t kept_total = 0, rel_before_install_total = 0, lost_total = 0,
             ho_release_total = 0;
    std::atomic<int> wd_stage{0};
    std::atomic<uint64_t> wd_iter{0};
    std::atomic<bool> wd_abort{false};
    // watchdog：30s 无进展即打印现场并硬退出（避免死锁静默）
    std::thread wd([&]() {
      uint64_t last_iter = 0;
      int last_stage = -1;
      uint64_t last_change = rocksdb::Env::Default()->NowMicros();
      while (!wd_abort.load()) {
        env_sleep_ms(200);
        const uint64_t it = wd_iter.load();
        const int stg = wd_stage.load();
        if (it != last_iter || stg != last_stage) {
          last_iter = it;
          last_stage = stg;
          last_change = rocksdb::Env::Default()->NowMicros();
        } else if (rocksdb::Env::Default()->NowMicros() - last_change >
                   30ull * 1000 * 1000) {
          printf("WATCHDOG: stalled at iter=%llu stage=%d (group=%d) — abort\n",
                 (unsigned long long)it, stg, group);
          fflush(stdout);
          std::_Exit(3);
        }
      }
    });
    for (uint64_t it = 0; it < iters; ++it) {
      wd_iter.store(it);
      wd_stage.store(1);
      const uint64_t base = 1000000 + it * 1000;
      const uint64_t kv = base + 7;
      const std::string key = zf_keygen::KeyFromInt(kv, kKeySize);
      const std::string want = zf_keygen::ValueFor(kv, 1, kValueSize);
      // 组 3：交替 值 / 删除 / 快照
      const int variant = (group == 3) ? static_cast<int>(it % 3) : 0;
      const std::string iter_dir =
          base_dir + "/it_" + std::to_string(group) + "_" + std::to_string(it);
      // 组 4/5：epoch 目标撑大 → 只封存不物化，覆盖 sealed 代（未安装）读路径
      if (group == 4 || group == 5) {
        setenv("ZF_VC_EPOCH_TARGET_MB", "64", 1);
      } else {
        unsetenv("ZF_VC_EPOCH_TARGET_MB");
      }
      std::remove((iter_dir + "/ZF_SENTINEL").c_str());
      std::unique_ptr<rocksdb::DB> idb = OpenDb(iter_dir, /*create=*/true);
      if (idb == nullptr) {
        printf("stress iter=%llu: open failed — abort\n",
               (unsigned long long)it);
        ++failures;
        break;
      }
      rocksdb::DBImpl* iimpl = static_cast<rocksdb::DBImpl*>(idb.get());
      std::shared_ptr<zeroflush::ZeroFlushContext> ictx =
          iimpl->GetZeroFlushContext();
      // 写目标键（variant 1 走删除标记；variant 2 取快照后覆盖写）
      const rocksdb::Snapshot* snap3 = nullptr;
      if (variant == 1) {
        idb->Put(rocksdb::WriteOptions(), key, want);
        idb->Delete(rocksdb::WriteOptions(), key);
      } else if (variant == 2) {
        idb->Put(rocksdb::WriteOptions(), key, want);
        snap3 = idb->GetSnapshot();
        idb->Put(rocksdb::WriteOptions(), key,
                 zf_keygen::ValueFor(kv, 2, kValueSize));
      } else {
        idb->Put(rocksdb::WriteOptions(), key, want);
      }
      const uint32_t part = ictx->RoutePartId(rocksdb::Slice(key));
      // 组 4：先让目标键落入 **sealed 代**（bulk 写触发封存后再选 locator），
      // 覆盖"sealed 代 locator"路径（前三组是活跃代 locator）。
      if (group == 4) {
        for (int i = 0; i < 400; ++i) {
          const uint64_t vv = base + 100 + i;
          idb->Put(rocksdb::WriteOptions(),
                   zf_keygen::KeyFromInt(vv, kKeySize),
                   zf_keygen::ValueFor(vv, 1, kValueSize));
        }
        env_sleep_ms(30);
      }
      // 记录所在代（写之后；组 5 会把它封存掉）
      const uint32_t rec_gen = ictx->wal()->ActiveGenOpt(part).value_or(0);
      uint32_t gen_before = rec_gen;
      // 组 5：**先换代再选 locator** → 选择时记录已在 sealed（未安装）代，
      // 覆盖"sealed 代 locator 被 pin"路径（组 4 覆盖"选择时活跃、
      // 暂停期间换代"）。lambda 定义在下方，故此处用等价的内联循环。
      if (group == 5) {
        for (int round = 0; round < 12; ++round) {
          if (ictx->wal()->ActiveGenOpt(part).value_or(0) != rec_gen) break;
          for (int i = 0; i < 400; ++i) {
            const uint64_t vv = base + 500 + i;
            idb->Put(rocksdb::WriteOptions(),
                     zf_keygen::KeyFromInt(vv, kKeySize),
                     zf_keygen::ValueFor(vv, 1, kValueSize));
          }
          env_sleep_ms(5);
        }
        env_sleep_ms(30);
        gen_before = ictx->wal()->ActiveGenOpt(part).value_or(0);
      }
      const uint64_t rseq = idb->GetLatestSequenceNumber();
      const std::string sentinel = iter_dir + "/ZF_SENTINEL";
      setenv("ZF_TEST_PAUSE_AFTER_SELECT", sentinel.c_str(), 1);
      std::string got;
      rocksdb::Status sts = rocksdb::Status::OK();
      rocksdb::ReadOptions ro;
      if (variant == 2 && snap3 != nullptr) ro.snapshot = snap3;
      // 注意：LastSourceTls 是 thread_local —— 必须在读线程内取，主线程读不到。
      const char* src_tls = "NONE";
      std::thread reader([&]() {
        sts = idb->Get(ro, key, &got);
        src_tls = zeroflush::get_trace::SourceName(
            zeroflush::get_trace::LastSourceTls());
      });
      env_sleep_ms(60);
      wd_stage.store(2);
      // 触发 Freeze：写足量数据，**直到该分区真的换代**（`ActiveGenOpt(part)`
      // 变化）——写路由与读路由可能不同，固定条数不一定命中该分区（实测
      // group4 曾出现 rotations=0）。有界重试，避免偶发不换代。
      uint64_t rot_w = 0;
      auto force_rotation = [&](uint32_t baseline) {
        for (int round = 0; round < 12; ++round) {
          if (ictx->wal()->ActiveGenOpt(part).value_or(0) != baseline) break;
          for (int i = 0; i < 400; ++i, ++rot_w) {
            const uint64_t vv = base + 100 + rot_w;
            idb->Put(rocksdb::WriteOptions(),
                     zf_keygen::KeyFromInt(vv, kKeySize),
                     zf_keygen::ValueFor(vv, 1, kValueSize));
          }
          env_sleep_ms(5);
        }
      };

      force_rotation(gen_before);  // 暂停期间换代（组 5 是第二次换代）
      const uint32_t gen_after = ictx->wal()->ActiveGenOpt(part).value_or(0);
      const bool rotated =
          (gen_after != gen_before) || (group == 5 && gen_after != rec_gen);
      if (rotated) ++rotations;
      // 等安装完成（组 1/2/3）；组 4 **不等**（要读尚未安装的 sealed 代）
      wd_stage.store(3);
      if (group == 4) {
        env_sleep_ms(30);
      } else {
        const uint64_t deadline =
            rocksdb::Env::Default()->NowMicros() + 10ull * 1000 * 1000;
        while (rocksdb::Env::Default()->NowMicros() < deadline &&
               Prop(idb.get(), "pending_materialize_bytes") != 0) {
          idb->Flush(rocksdb::FlushOptions());
          env_sleep_ms(5);
        }
      }
      bool unlink_done = false;
      // 组 4 不 unlink：组 4 读的是**未安装**的 sealed 代，删掉就是删唯一副本
      // （实测引擎会正确传播 IO error —— 那是"未安装代不可读"的预期行为，
      // 不是本组要测的路径）。"安装后回收"由组 2 覆盖。
      if (group == 2 || (group == 3 && variant == 1)) {
        wd_stage.store(4);
        // 只在"该代**已安装**（或已排空）"时 unlink（模拟"安装后回收"）：
        // 目标模式关闭 L0 后安装可能让位/等待；若在未安装时 unlink 就是删掉
        // 唯一副本——引擎会正确传播 IO error（缺口 2 协议），但那不是本组要
        // 测的回收生命周期。
        bool gen_installed = false;
        {
          std::string fx;
          idb->GetProperty("rocksdb.zeroflush.installed_fences", &fx);
          size_t pos = 0;
          while (pos < fx.size()) {
            unsigned fp = 0, lo = 0, hi = 0;
            if (fx[pos] == 'p' &&
                sscanf(fx.c_str() + pos, "p%u=[%u,%u]", &fp, &lo, &hi) == 3) {
              if (fp == part && gen_before <= hi) {
                gen_installed = true;
              }
            }
            ++pos;
          }
        }
        // 判据只看 **fence 覆盖该代**（= 已安装）。注意不能用 pending==0 代替：
        // 让位/移交会把代移出 pending 记账（recovery 集合不计入），pending=0
        // 并不代表数据已在 LSM——那样 unlink 等于删唯一副本（实测 group2 6/8）。
        if (gen_installed) {
          const std::string path = ictx->wal()->GenFilePath(part, gen_before);
          std::remove(path.c_str());
          unlink_done = true;
        }
      }
      wd_stage.store(5);
      {
        FILE* f = fopen(sentinel.c_str(), "w");
        if (f != nullptr) fclose(f);
      }
      reader.join();
      unsetenv("ZF_TEST_PAUSE_AFTER_SELECT");
      // 期望值：variant1=NotFound；variant2=快照下的 seed1；否则 seed1
      const uint64_t propagated =
          Prop(idb.get(), "read_readfail_propagated");
      propagated_total += propagated;
      fellback_total += Prop(idb.get(), "read_readfail_fallen_back");
      pin_ok_total += Prop(idb.get(), "wal_pin_acquired");
      pin_active_total += Prop(idb.get(), "wal_pin_active_unpinnable");
      kept_total += Prop(idb.get(), "index_kept_uninstalled");
      rel_before_install_total += Prop(idb.get(), "gen_release_before_install");
      lost_total += Prop(idb.get(), "gen_lost_before_install");
      ho_release_total += Prop(idb.get(), "handoff_at_release");
      const char* src = src_tls;
      const bool exp_nf = (variant == 1);
      const std::string exp_val =
          (variant == 2) ? want : zf_keygen::ValueFor(kv, 1, kValueSize);
      bool fail = false;
      const char* why = "-";
      if (propagated > 0) {
        fail = true;
        why = "readfail_propagated>0";
      } else if (sts.IsCorruption() || sts.IsIOError()) {
        fail = true;
        ++corruption;
        why = "corruption/ioerror";
      } else if (exp_nf) {
        if (!sts.IsNotFound()) {
          fail = true;
          why = "expected NotFound";
          ++silent_stale;
        }
      } else if (sts.IsNotFound()) {
        fail = true;
        ++notfound;
        why = "NotFound";
      } else if (!sts.ok()) {
        fail = true;
        why = "status!=OK";
      } else if (got != exp_val) {
        fail = true;
        ++silent_stale;
        why = "silent_stale";
      }
      if (fail) {
        ++failures;
        // 失败当场取完整状态（同一时点：读已返回、库仍打开）
        DiagKeyState(idb.get(), ictx, iter_dir, kv, kKeySize, kValueSize);
      }
      // 逐轮记录（用户指定字段）
      printf("stress group=%d iter=%llu variant=%d gen_before=%u gen_after=%u "
             "rotated=%d rseq=%llu snap=%d src=%s status=%s got_len=%zu "
             "exp=%s pin_ok=%llu pin_active=%llu propagated=%llu fellback=%llu "
             "unlink=%d %s\n",
             group, (unsigned long long)it, variant, gen_before, gen_after,
             rotated ? 1 : 0, (unsigned long long)rseq, snap3 != nullptr ? 1 : 0,
             src, sts.ok() ? "OK" : (sts.IsNotFound() ? "NOTFOUND"
                                                    : sts.ToString().c_str()),
             got.size(), exp_nf ? "NOTFOUND" : "VALUE",
             (unsigned long long)Prop(idb.get(), "wal_pin_acquired"),
             (unsigned long long)Prop(idb.get(), "wal_pin_active_unpinnable"),
             (unsigned long long)propagated,
             (unsigned long long)Prop(idb.get(), "read_readfail_fallen_back"),
             unlink_done ? 1 : 0,
             fail ? (std::string("FAIL(") + why + ")").c_str() : "PASS");
      if (snap3 != nullptr) idb->ReleaseSnapshot(snap3);
      ++rounds;
      idb.reset();  // 关闭本轮库（独立副本语义：下轮另起新库）
      if (fail && failures > 5) break;  // 连续失败即停，便于定位
    }
    wd_abort.store(true);
    wd.join();
    const bool pass = (failures == 0 && silent_stale == 0 && notfound == 0 &&
                       corruption == 0 && rotations > 0);
    printf("stress gate: index_kept_uninstalled=%llu "
           "gen_release_before_install=%llu handoff_at_release=%llu "
           "gen_lost_before_install=%llu\n",
           (unsigned long long)kept_total,
           (unsigned long long)rel_before_install_total,
           (unsigned long long)ho_release_total, (unsigned long long)lost_total);
    printf("stress group=%d rounds=%llu failures=%llu silent_stale=%llu "
           "notfound=%llu corruption=%llu rotations=%llu propagated=%llu "
           "fellback=%llu pin_ok=%llu pin_active=%llu -> %s\n",
           group, (unsigned long long)rounds, (unsigned long long)failures,
           (unsigned long long)silent_stale, (unsigned long long)notfound,
           (unsigned long long)corruption, (unsigned long long)rotations,
           (unsigned long long)propagated_total,
           (unsigned long long)fellback_total, (unsigned long long)pin_ok_total,
           (unsigned long long)pin_active_total, pass ? "PASS" : "FAIL");
    return pass ? 0 : 1;
  }

  int setup_incomplete = 0;  // fixture 未建成：计"未完成"，不得显示为通过
  if (mode == "semtest") {
    // step 1j（用户 2026-09-15 指定的四个确定性用例）：活跃读语义。
    // 1) 路由分区命中旧值，其他候选持有新值   → 必须返回新值
    // 2) 路由分区命中旧删除标记，其他候选有新值 → 必须返回新值
    // 3) 最新删除标记位于其他候选             → 定位到的胜出者必须是删除标记
    // 4) 选中 locator 后暂停（安装/换代/回收）→ 继续读取必须正确或明确报错
    //    （不得静默返回旧值/NotFound）
    // 1-3 用真实 ZeroFlushContext 的索引集构造受控状态（写入真实 locator 的
    // 合成 internal key），选择走**与读路径同一份** SelectBestIndexRecord。
    rocksdb::DBImpl* impl = static_cast<rocksdb::DBImpl*>(db.get());
    std::shared_ptr<zeroflush::ZeroFlushContext> ctx =
        impl->GetZeroFlushContext();
    if (ctx == nullptr || ctx->index_set() == nullptr) {
      printf("semtest: not a ZeroFlush DB\n");
      return 1;
    }
    const uint32_t pa = 1000;  // 路由分区（候选列表第一个）
    const uint32_t pb = 1001;  // 历史分区（写时路由版本不同）
    const std::string k = zf_keygen::KeyFromInt(total, kKeySize);
    // locator：指向一个真实存在的小 WAL 文件（用例 1-3 不读 value，只验证选择）
    auto loc_of = [&](uint32_t part, uint32_t gen, uint64_t off) {
      zeroflush::SlimLocator sl{};
      sl.part_id = part;
      sl.gen = gen;
      sl.wal_offset = off;
      return std::string(reinterpret_cast<const char*>(&sl), sizeof(sl));
    };
    auto ik = [&](uint64_t v, int type, uint64_t seq) {
      std::string out = zf_keygen::KeyFromInt(v, kKeySize);
      const uint64_t packed =
          rocksdb::PackSequenceAndType(seq, static_cast<rocksdb::ValueType>(type));
      rocksdb::PutFixed64(&out, packed);
      return out;
    };
    int fails = 0;
    // ---- 用例 1：路由分区旧值(seq 100) vs 其他候选新值(seq 200) ----
    {
      ctx->index_set()->Insert(pa, 1, ik(total, 1, 100), loc_of(pa, 1, 0));
      ctx->index_set()->Insert(pb, 2, ik(total, 1, 200), loc_of(pb, 2, 0));
      std::vector<uint32_t> cands{pa, pb};
      uint32_t lp = 0, pp = 0, pg = 0;
      uint64_t off = 0;
      rocksdb::ValueType t = rocksdb::kTypeValue;
      rocksdb::SequenceNumber sq = 0;
      const bool ok = ctx->SelectBestIndexRecord(
          rocksdb::Slice(k), 1000, &cands, &lp, &pp, &pg, &off, &t, &sq, nullptr);
      const bool pass = ok && lp == pb && sq == 200 && pp == pb && pg == 2;
      printf("case1 routed_old_vs_other_new: %s (sel_part=%u sel_seq=%llu "
             "phys=%u/%u; 期望 part=%u seq=200)\n",
             pass ? "PASS" : "FAIL", lp, (unsigned long long)sq, pp, pg, pb);
      (void)pass;
      if (!pass) ++fails;
    }
    // ---- 用例 2：路由分区旧删除标记(seq 100) vs 其他候选新值(seq 200) ----
    {
      // 注意：Insert(part, gen, ...) 只写入**已存在**的该代段（无段则丢弃，
      // 见 insert_dropped 计数）；因此三个用例都复用 pa gen1 / pb gen2。
      ctx->index_set()->Insert(pa, 1, ik(total, 0, 300), loc_of(pa, 1, 0));
      ctx->index_set()->Insert(pb, 2, ik(total, 1, 400), loc_of(pb, 2, 0));
      std::vector<uint32_t> cands{pa, pb};
      uint32_t lp = 0, pp = 0, pg = 0;
      uint64_t off = 0;
      rocksdb::ValueType t = rocksdb::kTypeValue;
      rocksdb::SequenceNumber sq = 0;
      const bool ok = ctx->SelectBestIndexRecord(
          rocksdb::Slice(k), 1000, &cands, &lp, &pp, &pg, &off, &t, &sq, nullptr);
      const bool pass = ok && lp == pb && sq == 400 && t == rocksdb::kTypeValue;
      printf("case2 routed_old_tombstone_vs_other_new_value: %s (sel_part=%u "
             "sel_seq=%llu type=%d; 期望 part=%u seq=400 type=VAL)\n",
             pass ? "PASS" : "FAIL", lp, (unsigned long long)sq, (int)t, pb);
      if (!pass) ++fails;
    }
    // ---- 用例 3：最新删除标记在其他候选(seq 600) → 胜出者必须是删除标记 ----
    {
      ctx->index_set()->Insert(pa, 1, ik(total, 1, 500), loc_of(pa, 1, 0));
      ctx->index_set()->Insert(pb, 2, ik(total, 0, 600), loc_of(pb, 2, 0));
      std::vector<uint32_t> cands{pa, pb};
      uint32_t lp = 0, pp = 0, pg = 0;
      uint64_t off = 0;
      rocksdb::ValueType t = rocksdb::kTypeValue;
      rocksdb::SequenceNumber sq = 0;
      const bool ok = ctx->SelectBestIndexRecord(
          rocksdb::Slice(k), 1000, &cands, &lp, &pp, &pg, &off, &t, &sq, nullptr);
      const bool pass = ok && lp == pb && sq == 600 && t == rocksdb::kTypeDeletion;
      printf("case3 newest_tombstone_in_other_candidate: %s (sel_part=%u "
             "sel_seq=%llu type=%d; 期望 part=%u seq=600 type=DEL⇒NotFound)\n",
             pass ? "PASS" : "FAIL", lp, (unsigned long long)sq, (int)t, pb);
      if (!pass) ++fails;
    }
    // ---- 用例 5（用户 2026-09-17 重写）：**真实回收验收** ----
    // 选中活跃记录（选择时已取 generation 读引用）→ 暂停 → 换代 + 安装该代
    // → 调用正常回收：读引用存在 ⇒ 回收被延迟 → 放行读取（值/read_sequence 正确、无错误）
    // → 释放引用 ⇒ 正常回收最终完成。不做手工 unlink；不以 pending==0 代替；
    // 直接看安装状态（fence）与引用计数、文件长度、deferred 计数。
    {
      const uint64_t v5 = total + 2;
      const std::string k5 = zf_keygen::KeyFromInt(v5, kKeySize);
      const std::string want5 = zf_keygen::ValueFor(v5, 1, kValueSize);
      rocksdb::DBImpl* impl5 = static_cast<rocksdb::DBImpl*>(db.get());
      std::shared_ptr<zeroflush::ZeroFlushContext> ctx5 =
          impl5->GetZeroFlushContext();
      const uint32_t part5 = ctx5->RoutePartId(rocksdb::Slice(k5));
      db->Put(rocksdb::WriteOptions(), k5, want5);
      ctx5->wal()->Sync(part5).PermitUncheckedError();
      const uint32_t gen5 = ctx5->wal()->ActiveGenOpt(part5).value_or(0);
      const std::string path5 = ctx5->wal()->GenFilePath(part5, gen5);
      const std::string sentinel5 = dbdir + "/ZF_PAUSE5";
      std::remove(sentinel5.c_str());
      setenv("ZF_TEST_PAUSE_AFTER_SELECT", sentinel5.c_str(), 1);
      std::string got5;
      rocksdb::Status st5 = rocksdb::Status::OK();
      const char* src5 = "NONE";
      std::thread reader5([&]() {
        st5 = db->Get(rocksdb::ReadOptions(), k5, &got5);
        src5 = zeroflush::get_trace::SourceName(
            zeroflush::get_trace::LastSourceTls());
      });
      env_sleep_ms(300);  // 等读线程进入"已选择（已取读引用）、未读"窗口
      char refprop[96];
      snprintf(refprop, sizeof(refprop), "rocksdb.zeroflush.gen_read_refs:%u:%u",
               part5, gen5);
      const uint64_t refs5 = Prop2(db.get(), refprop);
      // 换代 + 安装该代（写足量数据 + 显式排空）
      // 定向把 **part5** 写到换代（否则该代不封存、不可安装；随机铺开不保证命中）。
      {
        uint64_t w = 0;
        for (uint64_t round = 0; round < 12; ++round) {
          if (ctx5->wal()->ActiveGenOpt(part5).value_or(0) != gen5) break;
          for (int i = 0; i < 400; ++i, ++w) {
            const uint64_t vv = total + 20000 + w;
            const std::string wk = zf_keygen::KeyFromInt(vv, kKeySize);
            db->Put(rocksdb::WriteOptions(), wk,
                    zf_keygen::ValueFor(vv, 1, kValueSize));
          }
          env_sleep_ms(5);
        }
      }
      {
        rocksdb::DBImpl* bimpl5 = static_cast<rocksdb::DBImpl*>(db.get());
        uint64_t tries = 0;
        while (Prop2(db.get(), "rocksdb.zeroflush.pending_materialize_bytes") != 0 &&
               tries < 20) {
          bimpl5->ZfDrainNow();
          db->Flush(rocksdb::FlushOptions());
          env_sleep_ms(150);
          ++tries;
        }
      }
      std::string fx5;
      db->GetProperty("rocksdb.zeroflush.installed_fences", &fx5);
      uint64_t fence_last5 = 0;
      bool has_fence5 = false;
      {
        size_t pos = 0;
        while (pos < fx5.size()) {
          unsigned fp = 0, lo = 0, hi = 0;
          if (fx5[pos] == 'p' &&
              sscanf(fx5.c_str() + pos, "p%u=[%u,%u]", &fp, &lo, &hi) == 3) {
            if (fp == part5) {
              fence_last5 = hi;
              has_fence5 = true;  // 必须区分"无条目"与"条目=[0,0]"
            }
          }
          ++pos;
        }
      }
      const bool installed5 = has_fence5 && fence_last5 >= gen5;
      const uint64_t deferred5 =
          Prop2(db.get(), "rocksdb.zeroflush.reclaim_deferred_by_reader");
      uint64_t fsize_during = 0;
      rocksdb::Env::Default()
          ->GetFileSize(path5, &fsize_during)
          .PermitUncheckedError();
      {
        FILE* f = fopen(sentinel5.c_str(), "w");
        if (f != nullptr) fclose(f);
      }
      reader5.join();
      unsetenv("ZF_TEST_PAUSE_AFTER_SELECT");
      const bool read_ok5 = st5.ok() && got5 == want5;
      env_sleep_ms(200);
      uint64_t fsize_after = 0;
      const rocksdb::Status gs5 =
          rocksdb::Env::Default()->GetFileSize(path5, &fsize_after);
      const uint64_t reclaimed5 = Prop2(db.get(), "rocksdb.zeroflush.reclaimed_bytes");
      const bool keep_wal = getenv("ZF_VC_KEEP_WAL") != nullptr;
      const bool ref_ok = refs5 > 0;
      // 回收不提前：读引用存在期间未被回收（deferred 计数或文件仍在，未安装时天然不回收）
      const bool delayed_ok = deferred5 > 0 || !installed5 || fsize_during > 0;
      // 不永久推迟：回收开启时最终应消失（或已记入 reclaimed）
      // 最终回收只在"该代确已安装"时才有意义（未安装的代本就不可回收）。
      const bool final_reclaim_ok =
          keep_wal || !installed5 ? true
                                  : (fsize_after == 0 || !gs5.ok() || reclaimed5 > 0);
      // **真实回收验收的前提是该代确已安装**；未安装时不能说"回收被延迟/最终回收"，
      // 只能报 INCOMPLETE（避免"未安装 ⇒ 不适用"被当成通过）。
      const bool pass5 = installed5 && ref_ok && read_ok5 && delayed_ok &&
                         final_reclaim_ok;
      if (!installed5) {
        ++setup_incomplete;
      }
      printf("case5 real-reclaim: %s (refs_during=%llu installed=%d(gen=%u "
             "fence_last=%llu) deferred_by_reader=%llu file_during=%llu "
             "file_after=%llu reclaimed=%llu reclaim=%s | read status=%s "
             "correct=%d src=%s)\n",
             installed5 ? (pass5 ? "PASS" : "FAIL")
                        : "INCOMPLETE(该代未安装 ⇒ 回收验收不适用)",
             (unsigned long long)refs5,
             installed5 ? 1 : 0, gen5, (unsigned long long)fence_last5,
             (unsigned long long)deferred5, (unsigned long long)fsize_during,
             (unsigned long long)fsize_after, (unsigned long long)reclaimed5,
             keep_wal ? "off" : "on", st5.ok() ? "OK" : st5.ToString().c_str(),
             read_ok5 ? 1 : 0, src5);
      if (installed5 && !pass5) ++fails;
    }
    // ---- 用例 7（用户 2026-09-17 指令：**组件级隔离 fixture**）----
    // 不依赖路由/后台物化/大量写入：直接用 WAL 管理器的 Append 接口在**两个显式
    // physical part** 上写受控记录（返回真实 locator），再把它们插入**各自的索引段**
    // （每个条目只插一次），然后调用**生产选择逻辑**并断言：
    //   访问 A → 访问 B；A 命中 seq=200（胜出）；B 命中 seq=100（落选）；
    //   最终 locator 与**读引用**都属于 A；读回 A 的值。
    // 旧共享槽实现（ZF_TEST_LEGACY_PIN_SLOT=1）在同一 fixture 下必须失败。
    {
      const uint64_t vK = total + 5000;   // 目标键：**从未被写过**（索引里只有本 fixture 的条目）
      const std::string kK = zf_keygen::KeyFromInt(vK, kKeySize);
      const std::string val_old = zf_keygen::ValueFor(vK, 1, kValueSize);
      const std::string val_new = zf_keygen::ValueFor(vK, 2, kValueSize);
      rocksdb::DBImpl* impl7 = static_cast<rocksdb::DBImpl*>(db.get());
      std::shared_ptr<zeroflush::ZeroFlushContext> ctx7 =
          impl7->GetZeroFlushContext();
      const uint32_t PA = 2, PB = 3;      // 显式指定两个 physical part
      // 1) 受控 WAL 追加（真实 locator；按 seq 顺序）
      zeroflush::WalRecordRef r_old{}, r_new{}, r_fb{}, r_fa{};
      const uint64_t vFB = total + 5001, vFA = total + 5002;
      const std::string kFB = zf_keygen::KeyFromInt(vFB, kKeySize);
      const std::string kFA = zf_keygen::KeyFromInt(vFA, kKeySize);
      const rocksdb::Status a1 = ctx7->wal()->Append(
          PB, rocksdb::Slice(kK), rocksdb::Slice(val_old),
          static_cast<uint8_t>(rocksdb::kTypeValue), 100, &r_old);
      const rocksdb::Status a2 = ctx7->wal()->Append(
          PA, rocksdb::Slice(kK), rocksdb::Slice(val_new),
          static_cast<uint8_t>(rocksdb::kTypeValue), 200, &r_new);
      const rocksdb::Status a3 = ctx7->wal()->Append(
          PB, rocksdb::Slice(kFB), rocksdb::Slice(val_old),
          static_cast<uint8_t>(rocksdb::kTypeValue), 300, &r_fb);
      const rocksdb::Status a4 = ctx7->wal()->Append(
          PA, rocksdb::Slice(kFA), rocksdb::Slice(val_old),
          static_cast<uint8_t>(rocksdb::kTypeValue), 100000, &r_fa);
      const bool appends_ok = a1.ok() && a2.ok() && a3.ok() && a4.ok();
      ctx7->wal()->Sync(PA).PermitUncheckedError();
      ctx7->wal()->Sync(PB).PermitUncheckedError();
      // 确保两个 part 有索引段（Active 会创建），再把四个条目各插一次
      ctx7->index_set()->Active(PA);
      ctx7->index_set()->Active(PB);
      const uint32_t gA = ctx7->wal()->ActiveGenOpt(PA).value_or(0);
      const uint32_t gB = ctx7->wal()->ActiveGenOpt(PB).value_or(0);
      auto loc_of_ref = [](const zeroflush::WalRecordRef& r) {
        zeroflush::SlimLocator sl{};
        sl.part_id = r.part_id;
        sl.gen = r.gen;
        sl.wal_offset = r.offset;
        return std::string(reinterpret_cast<const char*>(&sl), sizeof(sl));
      };
      const uint64_t i1 = ctx7->index_set()->Insert(PA, gA, ik(vK, 1, 200),
                                                    loc_of_ref(r_new));
      const uint64_t i2 = ctx7->index_set()->Insert(PB, gB, ik(vK, 1, 100),
                                                    loc_of_ref(r_old));
      const uint64_t i3 = ctx7->index_set()->Insert(PA, gA, ik(vFA, 1, 100000),
                                                    loc_of_ref(r_fa));
      const uint64_t i4 = ctx7->index_set()->Insert(PB, gB, ik(vFB, 1, 300),
                                                    loc_of_ref(r_fb));
      const bool ins_ok = i1 > 0 && i2 > 0 && i3 > 0 && i4 > 0;
      // 2) 分别验证 A/B：索引找得到预期版本，且 locator 能解码出**一致的 key/seq/type/值**
      auto verify_cand = [&](uint32_t part, uint64_t expect_seq,
                             const zeroflush::WalRecordRef& ref,
                             const std::string& expect_val) {
        std::string lb;
        rocksdb::ValueType t = rocksdb::kTypeValue;
        rocksdb::SequenceNumber sq = 0;
        if (!ctx7->index_set()->GetCopy(part, rocksdb::Slice(kK), 99999999ULL,
                                        &lb, &t, &sq, nullptr, nullptr) ||
            sq != expect_seq) {
          return false;
        }
        // locator ↔ 记录一致性：按 locator 读回并核对 key/seq/type/值
        std::string v;
        if (!ctx7->ReadValue(rocksdb::Slice(lb), &v).ok()) return false;
        return v == expect_val && ref.gen ==
                                      reinterpret_cast<const zeroflush::SlimLocator*>(
                                          lb.data())
                                          ->gen;
      };
      const bool candA_ok = verify_cand(PA, 200, r_new, val_new);
      const bool candB_ok = verify_cand(PB, 100, r_old, val_old);
      // 2b) **真实 sealed pin 变体**：把 A/B 走组件级封存（WAL 换代 + 索引换代 +
      // registry 登记；不含 memtable/epoch 归属，工具无法构造那部分），
      // 使选择时确实取得 **sealed 句柄**（pinned=1），并**经由该句柄读取 A**。
      bool sealed_ok = false, pin_read_ok = false;
      {
        const uint32_t bytesA = static_cast<uint32_t>(
            ctx7->wal()->ActiveSize(PA));
        const uint32_t bytesB = static_cast<uint32_t>(
            ctx7->wal()->ActiveSize(PB));
        // WAL 换代（production Freeze：FlushBuf + Sync + 长度校验 + 换代）
        const zeroflush::FreezeResult frA = ctx7->wal()->Freeze(PA);
        const zeroflush::FreezeResult frB = ctx7->wal()->Freeze(PB);
        if (frA.old_gen == gA && frB.old_gen == gB && frA.sealed_bytes > 0) {
          // 索引换代（新活跃段接替，旧段冻结）
          ctx7->index_set()->Freeze(PA, frA.old_gen + 1);
          ctx7->index_set()->Freeze(PB, frB.old_gen + 1);
          // registry 登记（sealed && !installed ⇒ 计入 pending；Get 校验要求登记）
          ctx7->sealed_cache()->RegisterGen(PA, frA.old_gen, frA.sealed_bytes);
          ctx7->sealed_cache()->RegisterGen(PB, frB.old_gen, frB.sealed_bytes);
          zeroflush::SealedEpoch seA;
          seA.epoch = 900001;
          seA.total_bytes = frA.sealed_bytes;
          seA.gens.emplace_back(PA, frA.old_gen);
          seA.part_bytes[PA] = frA.sealed_bytes;
          zeroflush::SealedEpoch seB;
          seB.epoch = 900002;
          seB.total_bytes = frB.sealed_bytes;
          seB.gens.emplace_back(PB, frB.old_gen);
          seB.part_bytes[PB] = frB.sealed_bytes;
          ctx7->sealed_cache()->AddEpochWithRecoveryAdoption(seA);
          ctx7->sealed_cache()->AddEpochWithRecoveryAdoption(seB);
          sealed_ok = true;
        }
      }
      // 3) 生产选择 + 执行记录 + 读引用身份
      zeroflush::get_trace::ClearCandidateVisits();
      uint32_t sel_part = 0, sel_phys = 0, sel_gen = 0;
      uint64_t sel_off = 0, sel_seq = 0;
      rocksdb::ValueType sel_type = rocksdb::kTypeValue;
      // 传入**真实 pin 槽**（组件级入口）：胜出候选的 locator 与读引用必须成对。
      zeroflush::ZeroFlushContext::ZfPinSlot pin_slot;
      pin_slot.ctx = ctx7.get();
      const bool sel_ok = ctx7->SelectBestIndexRecord(
          rocksdb::Slice(kK), 99999999ULL, nullptr, &sel_part, &sel_phys,
          &sel_gen, &sel_off, &sel_type, &sel_seq, &pin_slot);
      const auto& pin7 = zeroflush::get_trace::LastPinIdentity();
      bool visited_a = false, visited_b = false, b_hit = false, b_lost = false;
      for (const auto& v : zeroflush::get_trace::CandidateVisits()) {
        if (v.part == PA) visited_a = true;
        if (v.part == PB) {
          visited_b = true;
          if (v.hit) {
            b_hit = true;
            b_lost = (v.seq == 100);
          }
        }
      }
      const bool loc_ok = sel_ok && sel_seq == 200 && sel_phys == r_new.part_id &&
                          sel_gen == r_new.gen && sel_off == r_new.offset;
      const bool pin_ok = pin_slot.part == r_new.part_id &&
                          pin_slot.gen == r_new.gen;
      const bool sealed_pin = pin_slot.pinned && pin_slot.handle != nullptr;
      // 经**该 pin 的句柄**读取：key/seq/type/value 必须全部匹配 A
      if (sealed_pin && loc_ok) {
        zeroflush::SlimLocator sl{};
        sl.part_id = (uint32_t)sel_phys;
        sl.gen = sel_gen;
        sl.wal_offset = sel_off;
        const std::string lb(reinterpret_cast<const char*>(&sl), sizeof(sl));
        std::string v2;
        if (ctx7->ReadValuePinned(rocksdb::Slice(lb), pin_slot.handle.get(), &v2)
                .ok() &&
            v2 == val_new) {
          // （记录头与 value 直接从句柄原始读取核对，见下）
          // 经 pin：先确认 value 一致（ReadValuePinned 已在上方核对），
          // 再**直接从句柄读原始记录头**解出 key/seq/type 与 A 的记录核对
          // （注意：ReadFromSealed 只回填 value，不能拿它解记录头）。
          char hdrbuf[24];
          rocksdb::Slice hres;
          (void)hres;
          if (pin_slot.handle->Read(r_new.offset, 24, &hres, hdrbuf).ok() &&
              hres.size() == 24) {
            uint32_t kl2 = 0, vl2 = 0;
            std::memcpy(&kl2, hres.data() + 8, 4);
            std::memcpy(&vl2, hres.data() + 12, 4);
            const uint32_t tl2 = zeroflush::ZfRecordLength(kl2, vl2);
            std::string rec2;
            rec2.resize(tl2);
            std::memcpy(&rec2[0], hres.data(), 24);
            rocksdb::Slice rest2;
            const bool ok2 =
                pin_slot.handle
                    ->Read(r_new.offset + 24, tl2 - 24, &rest2, &rec2[0] + 24)
                    .ok() &&
                rest2.size() == tl2 - 24;
            zeroflush::ZfRecordHeader h2;
            rocksdb::Slice k2, v2s;
            const bool dec = ok2 && zeroflush::DecodeZfRecord(
                                        rec2.data(), tl2, &h2, &k2, &v2s)
                                        .ok();
            pin_read_ok = dec && v2s == rocksdb::Slice(val_new) &&
                          h2.seq == 200 &&
                          h2.type == static_cast<uint8_t>(rocksdb::kTypeValue) &&
                          k2 == rocksdb::Slice(kK);
          }
        }
      }
      const bool setup_ok = appends_ok && ins_ok && candA_ok && candB_ok;
      // sealed 变体（封存成功时）必须 pinned=1 且经 pin 读回 A；未封存成功则记 INCOMPLETE。
      const bool sealed_ok_all = sealed_ok && sealed_pin && pin_read_ok;
      const bool pass7 = setup_ok && loc_ok && pin_ok && visited_a &&
                         visited_b && b_hit && b_lost && sealed_ok_all;
      printf("case7 fixture: appends=%d ins=%d candA=%d candB=%d | sel=(part=%u "
             "gen=%u off=%llu seq=%llu) expect=(part=%u gen=%u off=%llu seq=200) "
             "| pin=(part=%u gen=%u pinned=%d active=%d) | exec[A=%d B=%d "
             "B_hit=%d B_lost=%d] | legacy=%s => %s\n",
             appends_ok ? 1 : 0, ins_ok ? 1 : 0, candA_ok ? 1 : 0,
             candB_ok ? 1 : 0, sel_phys, sel_gen,
             (unsigned long long)sel_off, (unsigned long long)sel_seq,
             r_new.part_id, r_new.gen, (unsigned long long)r_new.offset,
             pin7.part, pin7.gen, pin7.pinned ? 1 : 0, pin7.is_active ? 1 : 0,
             visited_a ? 1 : 0, visited_b ? 1 : 0, b_hit ? 1 : 0, b_lost ? 1 : 0,
             getenv("ZF_TEST_LEGACY_PIN_SLOT") != nullptr ? "ON" : "off",
             pass7 ? "PASS" : "FAIL");
      printf("case7 sealed: sealed=%d pinned=%d pin_read_A=%d\n", sealed_ok ? 1 : 0,
             sealed_pin ? 1 : 0, pin_read_ok ? 1 : 0);
      if (!setup_ok || !sealed_ok) {
        ++setup_incomplete;
      } else {
        // 旧共享槽模式下**必须失败**（否则说明用例区分不出该缺陷）。
        const bool legacy = getenv("ZF_TEST_LEGACY_PIN_SLOT") != nullptr;
        if (legacy) {
          const bool legacy_check_ok = !pass7;  // 期望失败
          printf("case7 legacy-check: %s（legacy 模式：用例%s）\n",
                 legacy_check_ok ? "PASS" : "FAIL",
                 legacy_check_ok ? "按预期失败 ⇒ 用例有区分力"
                                 : "竟然通过 ⇒ 用例无区分力");
          if (!legacy_check_ok) ++fails;
        } else if (!pass7) {
          ++fails;
        }
      }
    }
    printf("semtest per-case: case1/2/3=%s  case5=%s  case6=N/A(前提失效)  "
           "case7=%s\n",
           (fails == 0) ? "见上（各自 PASS/FAIL）" : "见上",
           setup_incomplete > 0 ? "INCOMPLETE(未完成)" : "见上",
           getenv("ZF_TEST_LEGACY_PIN_SLOT") != nullptr
               ? "legacy-check 见上"
               : "见上");
    printf("semtest cases1-7: %s (fails=%d setup_incomplete=%d)\n",
           (fails == 0 && setup_incomplete == 0)
               ? "PASS"
               : (setup_incomplete > 0 ? "SEMTEST_INCOMPLETE" : "FAIL"),
           fails, setup_incomplete);
    if (setup_incomplete > 0) {
      return 3;  // 独立非成功状态：不得当作通过
    }
    return fails == 0 ? 0 : 1;
  }

  if (mode == "l0merge") {
    // step 1g（用户 2026-09-15，B-3 实验）：**既有库的安全读取路径**。
    // ZF 把"旧记录重新物化"的输出装进 L0（epoch 序 = 安装序），而各文件
    // 的 seq 区间交错 ⇒ 原生 L0 首命中即停会返回旧值。重排文件号/按
    // seq 排序都不能保证逐键正确；可行的是**把 L0 归并进 base 层**：
    // 归并后每层 key 不重叠、跨层按 seq 比较 ⇒ 读恢复正确。
    // 本模式只做一次范围 compaction（change_level → target_level），
    // 然后打印层分布供后续 triage 复核。
    rocksdb::CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 1;
    cro.exclusive_manual_compaction = false;
    const rocksdb::Status cs = db->CompactRange(cro, nullptr, nullptr);
    printf("l0merge CompactRange(change_level->L1): %s\n",
           cs.ToString().c_str());
    std::vector<rocksdb::LiveFileMetaData> files;
    db->GetLiveFilesMetaData(&files);
    std::map<int, int> per_level;
    for (const auto& f : files) ++per_level[f.level];
    printf("l0merge 层分布:");
    for (const auto& [lv, n] : per_level) {
      printf(" L%d=%d", lv, n);
    }
    printf("\n");
    return cs.ok() ? 0 : 1;
  }

  if (mode == "recdiag") {
    // step 1e（用户 2026-09-15）：**单键逐记录**证据链，用于定位"未安装的新版本
    // 丢在哪一步"。输出：
    //   1) 该 key 在全部 WAL 里的每一版记录：physical_part / gen / offset / seq
    //      / type / value_class；
    //   2) 每个涉及 part 的 fence（gen<=fence 表示恢复期会把它当"已安装"过滤）；
    //   3) 读路由：当前表算出的 logical_part（读路径实际查询的分区）；
    //   4) 每个候选 part 的索引链实例（part/gen/frozen + 该 key 的 seq/type/locator）
    //      —— 判定记录被登记进了哪个索引实例；
    //   5) 同一 read_sequence 下 Get 的结果（来源由 ZF_DBG_GET 追踪给出）。
    const uint64_t diag_v = total;  // 复用位置参数：待诊断的 key 整数
    const std::string key = zf_keygen::KeyFromInt(diag_v, kKeySize);
    const std::string want2 = zf_keygen::ValueFor(diag_v, 2, kValueSize);
    printf("recdiag key_v=%llu key_len=%zu\n", (unsigned long long)diag_v,
           key.size());
    rocksdb::DBImpl* impl = static_cast<rocksdb::DBImpl*>(db.get());
    std::shared_ptr<zeroflush::ZeroFlushContext> ctx =
        impl->GetZeroFlushContext();
    if (ctx == nullptr) {
      printf("recdiag: not a ZeroFlush DB\n");
      return 1;
    }
    const rocksdb::Slice uk(key);
    const uint32_t routed = ctx->RoutePartId(uk);
    printf("step3 routed_logical_part=%u (当前路由表)\n", routed);
    // ---- 1) WAL 里的每一版 ----
    const std::string wal_dir = dbdir + "/zfwal";
    std::vector<std::pair<uint32_t, uint32_t>> wfiles;
    if (DIR* d = opendir(wal_dir.c_str())) {
      while (struct dirent* e = readdir(d)) {
        uint32_t pp = 0, gg = 0;
        if (ParseZfWalName(e->d_name, &pp, &gg)) wfiles.emplace_back(pp, gg);
      }
      closedir(d);
    }
    std::sort(wfiles.begin(), wfiles.end());
    std::vector<uint32_t> involved;
    for (const auto& [pp, gg] : wfiles) {
      std::ifstream in(ZfWalPath(wal_dir, pp, gg), std::ios::binary);
      if (!in.good()) continue;
      const std::string buf((std::istreambuf_iterator<char>(in)),
                            std::istreambuf_iterator<char>());
      size_t off = 0;
      while (off + zeroflush::kZfHeaderSize <= buf.size()) {
        const uint32_t klen =
            *reinterpret_cast<const uint32_t*>(buf.data() + off + 8);
        const uint32_t vlen =
            *reinterpret_cast<const uint32_t*>(buf.data() + off + 12);
        const uint32_t total_len = zeroflush::ZfRecordLength(klen, vlen);
        if (off + total_len > buf.size()) break;
        zeroflush::ZfRecordHeader h;
        rocksdb::Slice k, val;
        if (zeroflush::DecodeZfRecord(buf.data() + off, total_len, &h, &k,
                                      &val)
                .ok() &&
            k.size() == key.size() && memcmp(k.data(), key.data(), k.size()) == 0) {
          const SeedClass cls = ClassifyValue(val.ToString(), diag_v, kValueSize);
          printf("step1 wal_rec phys_part=%u gen=%u offset=%llu seq=%llu "
                 "type=%d class=%s\n",
                 pp, gg, (unsigned long long)off, (unsigned long long)h.seq,
                 (int)h.type,
                 cls == kSeed2 ? "seed2" : (cls == kSeed1 ? "seed1" : "other"));
          if (std::find(involved.begin(), involved.end(), pp) == involved.end()) {
            involved.push_back(pp);
          }
        }
        off += total_len;
      }
    }
    // ---- 2) fence ----
    std::string fences;
    db->GetProperty("rocksdb.zeroflush.installed_fences", &fences);
    printf("step2 fences=%s\n", fences.empty() ? "(none)" : fences.c_str());
    for (uint32_t pp : involved) {
      char tag[64] = "(none)";
      size_t pos = 0;
      while (pos < fences.size()) {
        unsigned fp = 0, lo = 0, hi = 0;
        if (fences[pos] == 'p' &&
            sscanf(fences.c_str() + pos, "p%u=[%u,%u]", &fp, &lo, &hi) == 3) {
          if (fp == pp) snprintf(tag, sizeof(tag), "last=%u", hi);
        }
        ++pos;
      }
      printf("step2 part=%u fence_%s\n", pp, tag);
    }
    // ---- 4) 每个候选 part 的索引链实例 ----
    std::vector<uint32_t> all_parts = ctx->wal()->AllPartitionIds();
    if (std::find(all_parts.begin(), all_parts.end(), routed) ==
        all_parts.end()) {
      all_parts.push_back(routed);
    }
    for (uint32_t pp : involved) {
      if (std::find(all_parts.begin(), all_parts.end(), pp) ==
          all_parts.end()) {
        all_parts.push_back(pp);
      }
    }
    std::sort(all_parts.begin(), all_parts.end());
    for (uint32_t pp : all_parts) {
      std::string out;
      ctx->index_set()->DumpKeyChain(pp, uk, &out);
      if (out.find("seq=") != std::string::npos ||
          out.find("(no version)") != std::string::npos) {
        printf("step4 chain logical_part=%u:\n%s", pp, out.c_str());
      }
    }
    // ---- 5) 同一读视图下的 Get ----
    rocksdb::ReadOptions ro;
    std::string got;
    const rocksdb::Status rec_st = db->Get(ro, key, &got);
    printf("step5 get status=%s len=%zu eq_seed2=%d eq_seed1=%d "
           "read_seq=%llu\n",
           rec_st.ok()
               ? "OK"
               : (rec_st.IsNotFound() ? "NOTFOUND" : rec_st.ToString().c_str()),
           got.size(), rec_st.ok() && got == want2 ? 1 : 0,
           rec_st.ok() && got == zf_keygen::ValueFor(diag_v, 1, kValueSize) ? 1
                                                                           : 0,
           (unsigned long long)db->GetLatestSequenceNumber());
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

  if (mode == "selftest") {
    // step 1f（用户 2026-09-15，C 补充检查 1）：快照/删除语义的**定向断言**。
    //   C1-1 快照读必须选 seq <= read_sequence 的最高版本（含删除标记）；
    //   C1-2 删除标记必须表现为 NotFound（不是拿到旧 value）；
    //   C1-3 覆盖"新值/旧值/删除标记"三种形态在同一 key 上的先后关系。
    // 全部走真实读路径（同一 read_view），断言直接打 PASS/FAIL 并计入退出码。
    const std::string skey = zf_keygen::KeyFromInt(total, kKeySize);
    int fails = 0;
    auto put = [&](int seed) {
      return db->Put(rocksdb::WriteOptions(), skey,
                     zf_keygen::ValueFor(total, seed, kValueSize));
    };
    auto take_snap = [&]() { return db->GetSnapshot(); };
    auto rd = [&](const rocksdb::Snapshot* sn, std::string* out) {
      rocksdb::ReadOptions ro;
      if (sn != nullptr) ro.snapshot = sn;
      return db->Get(ro, skey, out);
    };
    const rocksdb::Status s1 = put(1);
    const rocksdb::Snapshot* snap_after_seed1 = take_snap();
    const rocksdb::Status s2 = put(2);
    const rocksdb::Snapshot* snap_after_seed2 = take_snap();
    const rocksdb::Status sd = db->Delete(rocksdb::WriteOptions(), skey);
    std::string v;
    // 1) 最新视图：删除标记 ⇒ NotFound（不是旧值）
    const rocksdb::Status st_now = rd(nullptr, &v);
    const bool ok_del = st_now.IsNotFound();
    if (!ok_del) {
      printf("selftest C1-2 FAIL: delete 后最新读应 NotFound, got %s\n",
             st_now.ToString().c_str());
      ++fails;
    }
    // 2) 快照（seed2 已发布）：必须拿到 seed2
    const rocksdb::Status st_s2 = rd(snap_after_seed2, &v);
    const bool ok_s2 = st_s2.ok() && v == zf_keygen::ValueFor(total, 2, kValueSize);
    if (!ok_s2) {
      printf("selftest C1-1 FAIL: snap(seed2) 应见 seed2, status=%s len=%zu\n",
             st_s2.ToString().c_str(), v.size());
      ++fails;
    }
    // 3) 快照（仅 seed1 已发布）：必须拿到旧值 seed1（不是 NotFound，也不是 seed2）
    const rocksdb::Status st_s1 = rd(snap_after_seed1, &v);
    const bool ok_s1 = st_s1.ok() && v == zf_keygen::ValueFor(total, 1, kValueSize);
    if (!ok_s1) {
      printf("selftest C1-1 FAIL: snap(seed1) 应见 seed1, status=%s len=%zu\n",
             st_s1.ToString().c_str(), v.size());
      ++fails;
    }
    // 4) 重新 Put 覆盖删除：最新视图必须可见新值（删除后写入的 seq 更高）
    const rocksdb::Status s3 = put(2);
    const rocksdb::Status st_re = rd(nullptr, &v);
    const bool ok_re = st_re.ok() && v == zf_keygen::ValueFor(total, 2, kValueSize);
    if (!ok_re) {
      printf("selftest C1-3 FAIL: 删除后重写应见新值, status=%s len=%zu\n",
             st_re.ToString().c_str(), v.size());
      ++fails;
    }
    db->ReleaseSnapshot(snap_after_seed1);
    db->ReleaseSnapshot(snap_after_seed2);
    printf("selftest put/delete/snapshot: %s (fails=%d; del_now=%d snap_seed2=%d "
           "snap_seed1=%d re_put=%d; st=%s/%s/%s/%s)\n",
           fails == 0 ? "PASS" : "FAIL", fails, ok_del ? 1 : 0, ok_s2 ? 1 : 0,
           ok_s1 ? 1 : 0, ok_re ? 1 : 0, s1.ToString().c_str(),
           s2.ToString().c_str(), sd.ToString().c_str(),
           s3.ToString().c_str());
    return fails == 0 ? 0 : 1;
  }

  if (mode == "walread" || mode == "selftest") {
    // step 1f（用户 2026-09-15）：**明确断言 WAL 命中**的回归用例。
    // 背景：`all` 模式读到的是已物化数据（wal_read_delta=0 属正常），不能
    // 用作"WAL 读路径可用"的证据。本模式：写 → **同进程立刻读**（不 Flush、
    // 不等待物化）→ 断言 WAL 读计数 > 0 且正确读有 INDEX 来源。
    const uint64_t half = total / 2;
    uint64_t bad = 0;
    uint64_t trace_left = 3;   // 前 3 个失败键当场取证（活跃窗口内）
    uint64_t read_seq_at_fail = 0;
    for (uint64_t v = 0; v < total; ++v) {
      if (v % 1000 == 0) continue;
      const std::string key = zf_keygen::KeyFromInt(v, kKeySize);
      const std::string want =
          v < half ? zf_keygen::ValueFor(v, 2, kValueSize)
                   : zf_keygen::ValueFor(v, 1, kValueSize);
      std::string got;
      const rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &got);
      const bool ok = s.ok() && got == want;
      if (!ok) {
        ++bad;
        if (trace_left > 0) {
          --trace_left;
          // 失败当场：重放该键（动态追踪键），打印期望/实得/读序列 +
          // 引擎内部时序（索引选择、ReadValue 原始状态、LSM 访问顺序）。
          const uint64_t rseq0 = db->GetLatestSequenceNumber();
          printf("LIVE_FAIL v=%llu expect=%s got_len=%zu status=%s "
                 "read_seq=%llu (write 已返回 OK；read_seq>=write_seq=%d)\n",
                 (unsigned long long)v,
                 v < half ? "seed2" : "seed1", got.size(),
                 s.ok() ? "OK" : (s.IsNotFound() ? "NOTFOUND" : s.ToString().c_str()),
                 (unsigned long long)rseq0, 1);
          zeroflush::get_trace::SetDynamicKey(key);
          std::string got2;
          const rocksdb::Status s2 = db->Get(rocksdb::ReadOptions(), key, &got2);
          printf("LIVE_FAIL retrace status=%s src=%s len=%zu\n",
                 s2.ok() ? "OK" : (s2.IsNotFound() ? "NOTFOUND"
                                                   : s2.ToString().c_str()),
                 zeroflush::get_trace::SourceName(
                     zeroflush::get_trace::LastSourceTls()),
                 got2.size());
          // 记录到底在哪：全部 WAL 文件里的每一版 + 涉及 part 的 fence +
          // 该键在各逻辑分区链里的实例（活跃窗口内即时采集）。
          const std::string wdir = dbdir + "/zfwal";
          std::vector<std::pair<uint32_t, uint32_t>> wfs;
          if (DIR* d = opendir(wdir.c_str())) {
            while (struct dirent* e = readdir(d)) {
              uint32_t pp = 0, gg = 0;
              if (ParseZfWalName(e->d_name, &pp, &gg)) wfs.emplace_back(pp, gg);
            }
            closedir(d);
          }
          std::sort(wfs.begin(), wfs.end());
          for (const auto& [pp, gg] : wfs) {
            std::ifstream in(ZfWalPath(wdir, pp, gg), std::ios::binary);
            if (!in.good()) continue;
            const std::string buf((std::istreambuf_iterator<char>(in)),
                                  std::istreambuf_iterator<char>());
            size_t off = 0;
            while (off + zeroflush::kZfHeaderSize <= buf.size()) {
              const uint32_t klen =
                  *reinterpret_cast<const uint32_t*>(buf.data() + off + 8);
              const uint32_t vlen =
                  *reinterpret_cast<const uint32_t*>(buf.data() + off + 12);
              const uint32_t tl = zeroflush::ZfRecordLength(klen, vlen);
              if (off + tl > buf.size()) break;
              zeroflush::ZfRecordHeader h;
              rocksdb::Slice k, val;
              if (zeroflush::DecodeZfRecord(buf.data() + off, tl, &h, &k, &val)
                      .ok() &&
                  k.size() == key.size() &&
                  memcmp(k.data(), key.data(), k.size()) == 0) {
                printf("LIVE_FAIL wal_rec phys_part=%u gen=%u offset=%zu "
                       "seq=%llu type=%d class=%s\n",
                       pp, gg, off, (unsigned long long)h.seq, (int)h.type,
                       ClassifyValue(val.ToString(), v, kValueSize) == kSeed2
                           ? "seed2"
                           : (ClassifyValue(val.ToString(), v, kValueSize) == kSeed1
                                  ? "seed1"
                                  : "other"));
              }
              off += tl;
            }
          }
          rocksdb::DBImpl* impl2 = static_cast<rocksdb::DBImpl*>(db.get());
          std::shared_ptr<zeroflush::ZeroFlushContext> ctx2 =
              impl2->GetZeroFlushContext();
          if (ctx2 != nullptr) {
            std::string fx;
            db->GetProperty("rocksdb.zeroflush.installed_fences", &fx);
            printf("LIVE_FAIL fences=%s\n", fx.empty() ? "(none)" : fx.c_str());
            std::vector<uint32_t> pids = ctx2->wal()->AllPartitionIds();
            for (uint32_t pp : pids) {
              std::string o;
              ctx2->index_set()->DumpKeyChain(pp, rocksdb::Slice(key), &o);
              if (o.find("seq=") != std::string::npos) {
                printf("LIVE_FAIL chain_has_key logical_part=%u\n%s", pp,
                       o.c_str());
              }
            }
          }
          zeroflush::get_trace::SetDynamicKey(std::string());
          read_seq_at_fail = rseq0;
        }
      }
      ++ok_by_src[(int)zeroflush::get_trace::LastSourceTls()];
    }
    (void)read_seq_at_fail;
    const uint64_t active_reads = Prop(db.get(), "active_wal_read_count");
    const uint64_t sealed_reads = Prop(db.get(), "sealed_read_count");
    printf("walread index_insert_dropped=%llu readfail_propagated=%llu "
           "union_overflow=%llu index_parts_scanned=%llu\n",
           (unsigned long long)Prop(db.get(), "index_insert_dropped"),
           (unsigned long long)Prop(db.get(), "read_readfail_propagated"),
           (unsigned long long)Prop(db.get(), "read_union_overflow"),
           (unsigned long long)Prop(db.get(), "index_parts_scanned"));
    printf("walread active_wal_reads=%llu sealed_wal_reads=%llu\n",
           (unsigned long long)active_reads,
           (unsigned long long)sealed_reads);
    printf("walread sources:");
    for (const auto& [k, c] : ok_by_src) {
      printf(" %s=%llu",
             zeroflush::get_trace::SourceName(
                 static_cast<zeroflush::get_trace::GetSource>(k)),
             (unsigned long long)c);
    }
    printf("\nwalread mismatch=%llu / %llu\n", (unsigned long long)bad,
           (unsigned long long)(total - total / 1000 - 1));
    const bool wal_hit = (active_reads + sealed_reads) > 0;
    printf("WAL_HIT_ASSERT: %s (active=%llu sealed=%llu)\n",
           wal_hit ? "PASS" : "FAIL", (unsigned long long)active_reads,
           (unsigned long long)sealed_reads);
    // 该模式的验收口径（用户 2026-09-15）：逐键零错误（含 NotFound）+ 明确
    // WAL 命中。两者任一不满足即失败——活跃窗口的可见性缺口进主门禁。
    printf("LIVE_WINDOW_ASSERT: %s (mismatch=%llu)\n",
           bad == 0 ? "PASS" : "FAIL", (unsigned long long)bad);
    return (wal_hit && bad == 0) ? 0 : 1;
  }

  if (write_only) {
    // 只写不 Flush：留给 walhit 模式验证"物化前读走 WAL"这条路径。
    printf("write-only fifo_blocked=%llu fifo_bypass=%llu "
           "fence_advance=%llu fence_gap=%llu fence_hole=%llu "
           "fence_input_incomplete=%llu\n",
           (unsigned long long)Prop(db.get(), "fifo_blocked"),
           (unsigned long long)Prop(db.get(), "fifo_bypass"),
           (unsigned long long)Prop(db.get(), "fence_advance_count"),
           (unsigned long long)Prop(db.get(), "fence_gap_count"),
           (unsigned long long)Prop(db.get(), "fence_hole_advance_count"),
           (unsigned long long)Prop(db.get(), "fence_input_incomplete_count"));
    printf("write-only pending=%llu pending_recomputed=%llu drift_events=%llu\n",
           (unsigned long long)Prop(db.get(), "pending_materialize_bytes"),
           (unsigned long long)Prop(db.get(), "pending_recomputed"),
           (unsigned long long)Prop(db.get(), "pending_drift_events"));
    printf("write-only gen_lost_before_install=%llu unregistered_fd_fallback=%llu "
           "gen_handoff=%llu\n",
           (unsigned long long)Prop(db.get(), "gen_lost_before_install"),
           (unsigned long long)Prop(db.get(), "gen_unregistered_fd_fallback"),
           (unsigned long long)Prop(db.get(), "gen_handoff"));
    printf("write-only l0_install=%llu l0_blocked=%llu fallback_l0=%llu "
           "forbid_l0=%d\n",
           (unsigned long long)Prop(db.get(), "l0_install_count"),
           (unsigned long long)Prop(db.get(), "l0_install_blocked"),
           (unsigned long long)Prop(db.get(), "install_fallback_l0"),
           Prop(db.get(), "forbid_l0_install") > 0 ? 1 : 0);
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
