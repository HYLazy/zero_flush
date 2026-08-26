//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M1: 分区 WAL 管理器实现。

#include "zeroflush/wal_manager.h"

#include <algorithm>
#include <cassert>
#include <cstring>

#include "db/dbformat.h"
#include "logging/logging.h"
#include "rocksdb/env.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace zeroflush {

// ROCKS_LOG_* 宏在内联命名空间（ROCKSDB_NAMESPACE）中展开时使用
// InfoLogLevel 而不加命名空间前缀，在 zeroflush 命名空间内需要显式借用。
using ROCKSDB_NAMESPACE::InfoLogLevel;

namespace {

// 文件名 <dir>/zf-wal-<part>-<gen>.log
std::string MakeFileName(const std::string& dir, uint32_t part, uint32_t gen) {
  return dir + "/zf-wal-" + std::to_string(part) + "-" +
         std::to_string(gen) + ".log";
}

bool ParseFileName(const std::string& name, uint32_t* part, uint32_t* gen) {
  // zf-wal-<part>-<gen>.log
  constexpr const char* kPrefix = "zf-wal-";
  if (name.rfind(kPrefix, 0) != 0) {
    return false;
  }
  std::string rest = name.substr(std::strlen(kPrefix));
  size_t dash = rest.find('-');
  if (dash == std::string::npos) {
    return false;
  }
  size_t dot = rest.rfind(".log");
  if (dot == std::string::npos || dot <= dash) {
    return false;
  }
  std::string p_str = rest.substr(0, dash);
  std::string g_str = rest.substr(dash + 1, dot - dash - 1);
  if (p_str.empty() || g_str.empty()) {
    return false;
  }
  for (char c : p_str) {
    if (c < '0' || c > '9') return false;
  }
  for (char c : g_str) {
    if (c < '0' || c > '9') return false;
  }
  *part = static_cast<uint32_t>(std::stoul(p_str));
  *gen = static_cast<uint32_t>(std::stoul(g_str));
  return true;
}

// 从帧头中取 key_len / val_len（帧头固定偏移：key_len@8, val_len@12）。
void DecodeKeyValLen(const char* hdr, uint32_t* key_len, uint32_t* val_len) {
  *key_len = rocksdb::DecodeFixed32(hdr + 8);
  *val_len = rocksdb::DecodeFixed32(hdr + 12);
}

}  // namespace

// ---------------------------------------------------------------------------
// WalScanner
// ---------------------------------------------------------------------------

WalScanner::WalScanner(rocksdb::Env* env, const std::string& dir,
                       uint32_t part, uint32_t gen, rocksdb::Logger* info_log)
    : env_(env), path_(MakeFileName(dir, part, gen)), info_log_(info_log) {
  rocksdb::Status s =
      env_->NewSequentialFile(path_, &file_, rocksdb::EnvOptions());
  if (!s.ok()) {
    if (info_log_ != nullptr) {
      ROCKSDB_NAMESPACE::Error(info_log_,  "ZeroFlush: open scanner file %s failed: %s",
                      path_.c_str(), s.ToString().c_str());
    }
  }
}

WalScanner::~WalScanner() = default;

bool WalScanner::Next(ZfRecordHeader* h, rocksdb::Slice* key,
                      rocksdb::Slice* value) {
  if (!file_) {
    status_ = rocksdb::Status::IOError("ZeroFlush: scanner file not open");
    return false;
  }
  // 确保缓冲剩余 ≥ header（不足则整段块读——M4.7，减少 syscall）。
  if (buf_pos_ + kZfHeaderSize > buf_.size()) {
    if (!Refill()) {
      return false;
    }
    if (buf_.size() < kZfHeaderSize) {
      // size == 0：干净 EOF（正好落在记录边界）；0 < size < header：
      // 头部被截断——对已封存文件意味着数据丢失，标记 Corruption。
      if (buf_.size() > 0) {
        status_ = rocksdb::Status::Corruption(
            "ZeroFlush: truncated record header in sealed WAL");
      }
      return false;
    }
  }
  const char* p = buf_.data() + buf_pos_;
  uint32_t key_len = 0, val_len = 0;
  DecodeKeyValLen(p, &key_len, &val_len);
  const uint32_t total = ZfRecordLength(key_len, val_len);

  // 确保整条记录在缓冲内（不足则块读；EOF 仍不足 = 尾部截断）。
  if (buf_pos_ + total > buf_.size()) {
    if (!Refill()) {
      return false;
    }
    if (buf_pos_ + total > buf_.size()) {
      // 尾部记录不完整：对已封存文件意味着数据丢失（封存时已完整刷盘）。
      status_ = rocksdb::Status::Corruption(
          "ZeroFlush: truncated record in sealed WAL");
      return false;
    }
  }

  ZfRecordHeader hdr;
  rocksdb::Slice k, v;
  rocksdb::Status st =
      DecodeZfRecord(buf_.data() + buf_pos_, total, &hdr, &k, &v);
  if (!st.ok()) {
    if (info_log_ != nullptr) {
      ROCKSDB_NAMESPACE::Error(info_log_, 
          "ZeroFlush: scanner decode record @%llu failed: %s",
          static_cast<unsigned long long>(offset_), st.ToString().c_str());
    }
    status_ = st;
    return false;
  }
  offset_ += total;
  buf_pos_ += total;
  if (h) *h = hdr;
  if (key) *key = k;
  if (value) *value = v;
  return true;
}

// M4.7：整段块读——压缩未消费缓冲到头部，再读入 kScanChunkSize。
// 返回 false 仅表示 IO 错误（EOF 通过"读入 0 字节"表达，不置错误）。
bool WalScanner::Refill() {
  if (buf_pos_ > 0) {
    const size_t keep = buf_.size() - buf_pos_;
    std::memmove(&buf_[0], &buf_[buf_pos_], keep);
    buf_.resize(keep);
    buf_pos_ = 0;
  }
  const size_t old = buf_.size();
  buf_.resize(old + kScanChunkSize);
  rocksdb::Slice result;
  rocksdb::Status s = file_->Read(kScanChunkSize, &result, &buf_[old]);
  if (!s.ok()) {
    if (info_log_ != nullptr) {
      ROCKSDB_NAMESPACE::Error(info_log_,
                        "ZeroFlush: scanner read failed: %s",
                        s.ToString().c_str());
    }
    status_ = s;
    return false;
  }
  buf_.resize(old + result.size());
  return true;
}

// ---------------------------------------------------------------------------
// PartitionedWalManager
// ---------------------------------------------------------------------------

PartitionedWalManager::PartitionedWalManager(rocksdb::Env* env,
                                             const std::string& dir,
                                             uint32_t partitions,
                                             uint64_t partition_target_bytes)
    : env_(env),
      dir_(dir),
      partitions_(partitions),
      partition_target_bytes_(partition_target_bytes) {
  parts_.reserve(partitions);
  for (uint32_t i = 0; i < partitions; ++i) {
    auto p = std::unique_ptr<Partition>(new Partition());
    p->part_id = i;
    parts_[i] = std::move(p);
  }
}

PartitionedWalManager::~PartitionedWalManager() {
  // 析构时确保所有缓冲数据已刷盘并同步，避免未满 4KB 的缓冲数据丢失。
  // 注意：Close() 返回的 Status 在析构中无法传播，只能忽略。
  Close().PermitUncheckedError();
}

rocksdb::Status PartitionedWalManager::Close() {
  rocksdb::Status s;
  for (auto& [pid, p] : parts_) {
    (void)pid;
    rocksdb::MutexLock l(&p->mu);
    s = FlushBuf(p.get());
    if (!s.ok()) {
      return s;
    }
    if (p->wfile) {
      s = p->wfile->Sync();
      if (!s.ok()) {
        return s;
      }
    }
  }
  return rocksdb::Status::OK();
}

std::string PartitionedWalManager::FileName(uint32_t part,
                                            uint32_t gen) const {
  return MakeFileName(dir_, part, gen);
}

rocksdb::Status PartitionedWalManager::Open() {
  rocksdb::Status s = env_->CreateDirIfMissing(dir_);
  if (!s.ok()) {
    return s;
  }
  // 修 D4：先列出全部分区文件，按 max(gen) 探测每个分区的活跃代，
  // 避免重开后 Append 追加到已封存的旧代文件。
  std::vector<std::pair<uint32_t, uint32_t>> all;
  s = ListFiles(&all);
  if (!s.ok()) {
    return s;
  }
  for (uint32_t i = 0; i < partitions_; ++i) {
    auto it = parts_.find(i);
    if (it == parts_.end()) {
      continue;  // 该分区在初始化时已被创建，但防御性跳过
    }
    Partition* p = it->second.get();
    // MaxGen 在"该分区无任何代文件"时返回 0，但我们用 gen=0 作第一个有效代，
    // 两者歧义。所以用"all 中是否存在 (i, *)"判别"是否有文件"。
    bool has_file = false;
    for (const auto& fg : all) {
      if (fg.first == i) { has_file = true; break; }
    }
    if (!has_file) {
      // 该分区无任何代文件（首次运行）
      p->gen = 0;
      p->flushed_size = 0;
      p->total_size = 0;
      p->buf.clear();
      continue;
    }
    const uint32_t mg = MaxGen(i, all);
    p->gen = mg;
    const std::string fname = MakeFileName(dir_, i, mg);
    rocksdb::EnvOptions opts;
    s = env_->NewRandomAccessFile(fname, &p->rfile, opts);
    if (!s.ok()) {
      return s;
    }
    uint64_t sz = 0;
    s = env_->GetFileSize(fname, &sz);
    if (!s.ok()) {
      return s;
    }
    p->flushed_size = sz;
    p->total_size = sz;
    // 修 D5：残留活跃代字节同步进 O(1) 封存判定计数。此前 total_size
    // 被设为残留大小而 total_active_bytes_/active_bytes 仍从 0 起，
    // Freeze 时 fetch_sub(old_sealed_size) 多减残留字节 → 计数下溢 →
    // ShouldSeal 恒真 → 每写组封存（重开后封存风暴，M4.5b-48 实测）。
    p->active_bytes.store(sz, std::memory_order_relaxed);
    total_active_bytes_.fetch_add(sz, std::memory_order_relaxed);
    // 残留数据在首次写组即触发封存物化（语义正确：遗留 WAL 尽快落地）。
    p->buf.clear();
  }
  return rocksdb::Status::OK();
}

uint32_t PartitionedWalManager::MaxGen(
    uint32_t part,
    const std::vector<std::pair<uint32_t, uint32_t>>& all) const {
  uint32_t mx = 0;
  bool found = false;
  for (const auto& p : all) {
    if (p.first == part && p.second > mx) {
      mx = p.second;
      found = true;
    }
  }
  return found ? mx : 0;
}

uint64_t PartitionedWalManager::GetFileSize(
    uint32_t part, uint32_t gen,
    const std::vector<std::pair<uint32_t, uint32_t>>& all,
    rocksdb::Env* env) const {
  const std::string fname = MakeFileName(dir_, part, gen);
  (void)all;  // 不需遍历 all，文件名即标识
  uint64_t sz = 0;
  if (env->GetFileSize(fname, &sz).ok()) {
    return sz;
  }
  return 0;
}

uint32_t PartitionedWalManager::ActiveGen(uint32_t part) const {
  auto it = parts_.find(part);
  if (it == parts_.end()) return 0;
  Partition* p = it->second.get();
  rocksdb::MutexLock l(&p->mu);
  return p->gen;
}

rocksdb::Status PartitionedWalManager::ReadFromSealed(
    rocksdb::RandomAccessFile* rf, const WalRecordRef& ref,
    std::string* buf, rocksdb::Slice* value) {
  // 从已封存文件定点读（M4.7b：单次读优化——一次 Read 拉取
  // kZfHeaderSize + 内联缓冲，覆盖 1KB 级记录的整条；超长记录补读）。
  constexpr size_t kInlineRead = kZfHeaderSize + 4096;
  char scratch[kInlineRead];
  rocksdb::Slice result;
  rocksdb::Status s = rf->Read(ref.offset, kInlineRead, &result, scratch);
  if (!s.ok()) {
    return s;
  }
  if (result.size() < kZfHeaderSize) {
    return rocksdb::Status::Corruption("ZF sealed record header truncated");
  }
  uint32_t key_len = 0, val_len = 0;
  DecodeKeyValLen(result.data(), &key_len, &val_len);
  uint32_t total = ZfRecordLength(key_len, val_len);
  std::string rec;
  if (total <= result.size()) {
    // 整条在一次 Read 内（1KB 级记录常态）——免第二次 syscall。
    rec.assign(result.data(), total);
  } else {
    // 超长记录：补齐 body。
    rec.resize(total);
    std::memcpy(&rec[0], result.data(), result.size());
    rocksdb::Slice rest;
    s = rf->Read(ref.offset + result.size(), total - result.size(), &rest,
                 &rec[0] + result.size());
    if (!s.ok()) {
      return s;
    }
    if (rest.size() < total - result.size()) {
      return rocksdb::Status::Corruption("ZF sealed record truncated");
    }
  }
  ZfRecordHeader h;
  rocksdb::Slice k, v;
  s = DecodeZfRecord(rec.data(), total, &h, &k, &v);
  if (!s.ok()) {
    return s;
  }
  if (h.type == rocksdb::kTypeDeletion) {
    buf->clear();
    *value = rocksdb::Slice();
    return rocksdb::Status::OK();
  }
  buf->assign(v.data(), v.size());
  *value = rocksdb::Slice(*buf);
  return rocksdb::Status::OK();
}

rocksdb::Status PartitionedWalManager::OpenGen(Partition* p) const {
  rocksdb::EnvOptions opts;
  rocksdb::Status s =
      env_->NewWritableFile(FileName(p->part_id, p->gen), &p->wfile, opts);
  if (!s.ok()) {
    return s;
  }
  return env_->NewRandomAccessFile(FileName(p->part_id, p->gen), &p->rfile,
                                   opts);
}

rocksdb::Status PartitionedWalManager::EnsureOpenForWrite(
    Partition* p) const {
  if (p->wfile) {
    return rocksdb::Status::OK();
  }
  rocksdb::EnvOptions opts;
  // 使用 ReopenWritableFile（O_APPEND 而非 O_TRUNC）避免截断已有文件。
  // 恢复场景：Recover 已从文件读取旧数据，首次 Append 必须追加而非覆盖。
  rocksdb::Status s =
      env_->ReopenWritableFile(FileName(p->part_id, p->gen), &p->wfile, opts);
  if (!s.ok()) {
    return s;
  }
  // 确保 rfile 也可用（首次运行时 Open 中 rfile 可能为空）
  if (!p->rfile) {
    s = env_->NewRandomAccessFile(FileName(p->part_id, p->gen), &p->rfile,
                                  opts);
  }
  return s;
}

rocksdb::Status PartitionedWalManager::Append(uint32_t part,
                                              const rocksdb::Slice& key,
                                              const rocksdb::Slice& value,
                                              uint8_t type, uint64_t seq,
                                              WalRecordRef* out) {
  auto it = parts_.find(part);
  if (it == parts_.end()) {
    if (part == kRangeDelPartId) {
      auto np = std::unique_ptr<Partition>(new Partition());
      np->part_id = part;
      {
        rocksdb::MutexLock l(&parts_mu_);
        parts_[part] = std::move(np);
      }
      it = parts_.find(part);
    } else {
      return rocksdb::Status::InvalidArgument(
          "ZF Append: unknown partition " + std::to_string(part));
    }
  }
  Partition* p = it->second.get();

  // M4.1d：锁外编码（thread_local 缓冲复用，热路径避免每记录一次 string
  // 分配；并发下各线程独立 scratch）。分区锁内只做 memcpy + 计数，
  // 缩短临界区——并行写路径下 p->mu 是分区级串行点。
  ZfRecordHeader h;
  h.magic = kZfMagic;
  h.cf_id = 0;
  h.type = type;
  h.flags = 0;
  h.key_len = static_cast<uint32_t>(key.size());
  h.val_len = static_cast<uint32_t>(value.size());
  h.seq = seq;
  thread_local std::string zf_append_scratch;
  zf_append_scratch.clear();
  EncodeZfRecord(h, key, value, &zf_append_scratch);

  rocksdb::MutexLock l(&p->mu);

  // 延迟打开写句柄（首次 Append 时创建文件）
  rocksdb::Status s = EnsureOpenForWrite(p);
  if (!s.ok()) {
    return s;
  }

  const uint64_t offset = p->total_size;
  p->buf.append(zf_append_scratch);
  p->total_size += zf_append_scratch.size();
  // M4.1a：维护 O(1) 封存判定计数（Append 在 p->mu 内，原子仅供锁外读）。
  const uint64_t active = p->active_bytes.fetch_add(
                              zf_append_scratch.size(),
                              std::memory_order_relaxed) +
                          zf_append_scratch.size();
  total_active_bytes_.fetch_add(zf_append_scratch.size(),
                                std::memory_order_relaxed);
  if (active >= partition_target_bytes_) {
    any_over_target_.store(true, std::memory_order_relaxed);
  }

  // 缓冲达到 4KB 边界即刷盘（对齐非强制，记录可跨界）。
  if (p->buf.size() >= 4096) {
    s = FlushBuf(p);
    if (!s.ok()) {
      return s;
    }
  }
  if (out) {
    out->part_id = part;
    out->gen = p->gen;
    out->offset = offset;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status PartitionedWalManager::FlushBuf(Partition* p) const {
  if (p->buf.empty()) {
    return rocksdb::Status::OK();
  }
  rocksdb::Status s = p->wfile->Append(rocksdb::Slice(p->buf));
  if (!s.ok()) {
    return s;
  }
  p->flushed_size = p->total_size;
  p->buf.clear();
  return rocksdb::Status::OK();
}

rocksdb::Status PartitionedWalManager::Sync(uint32_t part) {
  auto it = parts_.find(part);
  if (it == parts_.end()) {
    return rocksdb::Status::InvalidArgument(
        "ZF Sync: unknown partition " + std::to_string(part));
  }
  Partition* p = it->second.get();
  rocksdb::MutexLock l(&p->mu);
  rocksdb::Status s = EnsureOpenForWrite(p);
  if (!s.ok()) {
    return s;
  }
  s = FlushBuf(p);
  if (!s.ok()) {
    return s;
  }
  return p->wfile->Sync();
}

rocksdb::Status PartitionedWalManager::SyncAll() {
  rocksdb::Status s;
  for (auto& [pid, p] : parts_) {
    (void)pid;
    s = Sync(pid);
    if (!s.ok()) {
      return s;
    }
  }
  return rocksdb::Status::OK();
}

uint64_t PartitionedWalManager::ActiveSize(uint32_t part) const {
  auto it = parts_.find(part);
  if (it == parts_.end()) return 0;
  Partition* p = it->second.get();
  rocksdb::MutexLock l(&p->mu);
  return p->total_size;
}

FreezeResult PartitionedWalManager::Freeze(uint32_t part) {
  auto it = parts_.find(part);
  if (it == parts_.end()) {
    FreezeResult empty;
    return empty;  // 未知分区返回空结果
  }
  Partition* p = it->second.get();
  ROCKS_LOG_DEBUG(info_log_,
                  "ZeroFlush Freeze: part=%u gen=%u acquiring partition mutex",
                  part, p->gen);
  rocksdb::MutexLock l(&p->mu);
  // 修 D3：wfile 是延迟打开的，未写过的分区此处为 nullptr。
  if (p->wfile) {
    FlushBuf(p).PermitUncheckedError();
    p->wfile->Sync().PermitUncheckedError();
    p->wfile.reset();
  }
  // 旧代读句柄移交 SealedFileCache（M2.1 启用时）；若未启用，本句
  // 释放文件描述符但保留物理文件（与原 M1 行为一致）。
  p->rfile.reset();
  const uint64_t old_sealed_size = p->total_size;
  const uint32_t old_gen = p->gen;
  FreezeResult fr;
  fr.old_gen = old_gen;
  fr.sealed_bytes = old_sealed_size;
  fr.sealed_path = MakeFileName(dir_, part, old_gen);
  ++p->gen;
  // 修 D2：新代文件从 0 写起；否则 Append 会用旧代总长作 offset 写错位置。
  p->flushed_size = 0;
  p->total_size = 0;
  p->buf.clear();
  // M4.1a：维护 O(1) 封存判定计数——旧代活跃字节归还给总计数；
  // 清零分区计数与超限标志（新代从 0 起，超限由后续 Append 重新置位）。
  total_active_bytes_.fetch_sub(old_sealed_size, std::memory_order_relaxed);
  p->active_bytes.store(0, std::memory_order_relaxed);
  OpenGen(p).PermitUncheckedError();
  ROCKS_LOG_DEBUG(info_log_,
                  "ZeroFlush Freeze: part=%u done, new_gen=%u releasing "
                  "partition mutex",
                  part, p->gen);
  return fr;
}

rocksdb::Status PartitionedWalManager::ReadRecord(const WalRecordRef& ref,
                                                  std::string* value) const {
  rocksdb::Slice v;
  return ReadRecord(ref, value, &v);
}

rocksdb::Status PartitionedWalManager::ReadRecord(const WalRecordRef& ref,
                                                  std::string* buf,
                                                  rocksdb::Slice* value) const {
  auto it = parts_.find(ref.part_id);
  if (it == parts_.end()) {
    return rocksdb::Status::InvalidArgument(
        "ZF ReadRecord: unknown partition " + std::to_string(ref.part_id));
  }
  Partition* p = it->second.get();
  rocksdb::MutexLock l(&p->mu);

  // 记录可能在未刷盘缓冲中（offset >= flushed_size）
  if (ref.offset >= p->flushed_size) {
    uint64_t in_buf = ref.offset - p->flushed_size;
    if (in_buf + kZfHeaderSize > p->buf.size()) {
      return rocksdb::Status::Corruption("ZF record offset beyond buffer");
    }
    const char* base = p->buf.data() + in_buf;
    uint32_t key_len = 0, val_len = 0;
    DecodeKeyValLen(base, &key_len, &val_len);
    uint32_t total = ZfRecordLength(key_len, val_len);
    if (in_buf + total > p->buf.size()) {
      return rocksdb::Status::Corruption("ZF record truncated in buffer");
    }
    ZfRecordHeader h;
    rocksdb::Slice k, v;
    rocksdb::Status s = DecodeZfRecord(base, total, &h, &k, &v);
    if (!s.ok()) {
      return s;
    }
    if (h.type == rocksdb::kTypeDeletion) {
      buf->clear();
      *value = rocksdb::Slice();
      return rocksdb::Status::OK();
    }
    buf->assign(v.data(), v.size());
    *value = rocksdb::Slice(*buf);
    return rocksdb::Status::OK();
  }

  // 从文件定点读：先读 header 求长度，再读整条。
  char scratch[kZfHeaderSize];
  rocksdb::Slice result;
  rocksdb::Status s =
      p->rfile->Read(ref.offset, kZfHeaderSize, &result, scratch);
  if (!s.ok()) {
    return s;
  }
  if (result.size() < kZfHeaderSize) {
    return rocksdb::Status::Corruption("ZF record header truncated");
  }
  uint32_t key_len = 0, val_len = 0;
  DecodeKeyValLen(result.data(), &key_len, &val_len);
  uint32_t total = ZfRecordLength(key_len, val_len);
  std::string rec;
  rec.resize(total);
  std::memcpy(&rec[0], result.data(), kZfHeaderSize);
  rocksdb::Slice rest;
  s = p->rfile->Read(ref.offset + kZfHeaderSize, total - kZfHeaderSize, &rest,
                     &rec[0] + kZfHeaderSize);
  if (!s.ok()) {
    return s;
  }
  if (rest.size() < total - kZfHeaderSize) {
    return rocksdb::Status::Corruption("ZF record truncated");
  }
  ZfRecordHeader h;
  rocksdb::Slice k, v;
  s = DecodeZfRecord(rec.data(), total, &h, &k, &v);
  if (!s.ok()) {
    return s;
  }
  if (h.type == rocksdb::kTypeDeletion) {
    buf->clear();
    *value = rocksdb::Slice();
    return rocksdb::Status::OK();
  }
  buf->assign(v.data(), v.size());
  *value = rocksdb::Slice(*buf);
  return rocksdb::Status::OK();
}

rocksdb::Status PartitionedWalManager::ListFiles(
    std::vector<std::pair<uint32_t, uint32_t>>* out) const {
  out->clear();
  std::vector<std::string> children;
  rocksdb::Status s = env_->GetChildren(dir_, &children);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return rocksdb::Status::OK();
    }
    return s;
  }
  for (const auto& name : children) {
    uint32_t part, gen;
    if (ParseFileName(name, &part, &gen)) {
      out->emplace_back(part, gen);
    }
  }
  std::sort(out->begin(), out->end());
  return rocksdb::Status::OK();
}

// ---------------------------------------------------------------------------
// M3.1：EnsurePartition / HasPartition / AllPartitionIds
// ---------------------------------------------------------------------------

void PartitionedWalManager::EnsurePartition(uint32_t part_id) {
  if (parts_.count(part_id) > 0) return;
  auto p = std::unique_ptr<Partition>(new Partition());
  p->part_id = part_id;
  p->gen = 0;
  p->flushed_size = 0;
  p->total_size = 0;
  parts_[part_id] = std::move(p);
}

bool PartitionedWalManager::HasPartition(uint32_t part_id) const {
  return parts_.count(part_id) > 0;
}

std::vector<uint32_t> PartitionedWalManager::AllPartitionIds() const {
  std::vector<uint32_t> ids;
  ids.reserve(parts_.size());
  for (const auto& [id, _] : parts_) {
    (void)_;
    ids.push_back(id);
  }
  std::sort(ids.begin(), ids.end());
  return ids;
}

}  // namespace zeroflush
