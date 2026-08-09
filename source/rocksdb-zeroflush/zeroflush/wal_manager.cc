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
    return false;
  }
  // 缓冲剩余不足 header 时读入 header（覆盖式）
  if (buf_pos_ + kZfHeaderSize > buf_.size()) {
    buf_.resize(kZfHeaderSize);
    rocksdb::Slice result;
    rocksdb::Status s = file_->Read(kZfHeaderSize, &result, &buf_[0]);
    if (!s.ok()) {
      if (info_log_ != nullptr) {
        ROCKSDB_NAMESPACE::Error(info_log_, 
                        "ZeroFlush: scanner read header failed: %s",
                        s.ToString().c_str());
      }
      return false;
    }
    buf_.resize(result.size());
    buf_pos_ = 0;
    if (buf_.size() < kZfHeaderSize) {
      return false;  // EOF 或损坏尾部，截断处理
    }
  }
  const char* p = buf_.data() + buf_pos_;
  uint32_t key_len = 0, val_len = 0;
  DecodeKeyValLen(p, &key_len, &val_len);
  uint32_t total = ZfRecordLength(key_len, val_len);

  // 缓冲剩余不足整条记录时读入整条
  if (buf_pos_ + total > buf_.size()) {
    std::string rec;
    rec.resize(total);
    size_t have = buf_.size() - buf_pos_;
    std::memcpy(&rec[0], buf_.data() + buf_pos_, have);
    rocksdb::Slice result;
    rocksdb::Status s = file_->Read(total - have, &result, &rec[0] + have);
    if (!s.ok()) {
      if (info_log_ != nullptr) {
        ROCKSDB_NAMESPACE::Error(info_log_, 
                        "ZeroFlush: scanner read record failed: %s",
                        s.ToString().c_str());
      }
      return false;
    }
    if (result.size() < total - have) {
      return false;  // 尾部记录不完整，截断
    }
    buf_ = std::move(rec);
    buf_pos_ = 0;
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
    return false;
  }
  offset_ += total;
  buf_pos_ += total;
  if (h) *h = hdr;
  if (key) *key = k;
  if (value) *value = v;
  return true;
}

// ---------------------------------------------------------------------------
// PartitionedWalManager
// ---------------------------------------------------------------------------

PartitionedWalManager::PartitionedWalManager(rocksdb::Env* env,
                                             const std::string& dir,
                                             uint32_t partitions)
    : env_(env), dir_(dir), partitions_(partitions) {
  parts_.reserve(partitions);
  for (uint32_t i = 0; i < partitions; ++i) {
    auto p = std::unique_ptr<Partition>(new Partition());
    p->part_id = i;
    parts_.push_back(std::move(p));
  }
}

PartitionedWalManager::~PartitionedWalManager() {
  // 析构时确保所有缓冲数据已刷盘并同步，避免未满 4KB 的缓冲数据丢失。
  // 注意：Close() 返回的 Status 在析构中无法传播，只能忽略。
  Close().PermitUncheckedError();
}

rocksdb::Status PartitionedWalManager::Close() {
  rocksdb::Status s;
  for (uint32_t i = 0; i < partitions_; ++i) {
    Partition* p = parts_[i].get();
    rocksdb::MutexLock l(&p->mu);
    s = FlushBuf(p);
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
  // 只创建目录 + 打开读句柄（不截断文件），写句柄由 EnsureOpenForWrite 延迟打开。
  // 恢复场景：Recover 扫描读到已有数据后，Append 首次写入时再打开写句柄。
  for (uint32_t i = 0; i < partitions_; ++i) {
    Partition* p = parts_[i].get();
    std::string fname = FileName(i, p->gen);
    if (env_->FileExists(fname).ok()) {
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
    } else {
      // 文件不存在（首次运行）
      p->flushed_size = 0;
      p->total_size = 0;
    }
  }
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
  assert(part < partitions_);
  Partition* p = parts_[part].get();
  rocksdb::MutexLock l(&p->mu);

  // 延迟打开写句柄（首次 Append 时创建文件）
  rocksdb::Status s = EnsureOpenForWrite(p);
  if (!s.ok()) {
    return s;
  }

  ZfRecordHeader h;
  h.magic = kZfMagic;
  h.cf_id = 0;
  h.type = type;
  h.flags = 0;
  h.key_len = static_cast<uint32_t>(key.size());
  h.val_len = static_cast<uint32_t>(value.size());
  h.seq = seq;

  std::string rec;
  EncodeZfRecord(h, key, value, &rec);
  const uint64_t offset = p->total_size;
  p->buf.append(rec);
  p->total_size += rec.size();

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
  Partition* p = parts_[part].get();
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
  for (uint32_t i = 0; i < partitions_; ++i) {
    s = Sync(i);
    if (!s.ok()) {
      return s;
    }
  }
  return rocksdb::Status::OK();
}

uint64_t PartitionedWalManager::ActiveSize(uint32_t part) const {
  assert(part < partitions_);
  Partition* p = parts_[part].get();
  rocksdb::MutexLock l(&p->mu);
  return p->total_size;
}

uint32_t PartitionedWalManager::Freeze(uint32_t part) {
  assert(part < partitions_);
  Partition* p = parts_[part].get();
  rocksdb::MutexLock l(&p->mu);
  FlushBuf(p).PermitUncheckedError();
  p->wfile->Sync().PermitUncheckedError();
  p->wfile.reset();
  p->rfile.reset();  // M1：旧代读句柄由 M2 的封存索引管理
  const uint32_t old_gen = p->gen;
  ++p->gen;
  OpenGen(p).PermitUncheckedError();
  return old_gen;
}

rocksdb::Status PartitionedWalManager::ReadRecord(const WalRecordRef& ref,
                                                  std::string* value) const {
  rocksdb::Slice v;
  return ReadRecord(ref, value, &v);
}

rocksdb::Status PartitionedWalManager::ReadRecord(const WalRecordRef& ref,
                                                  std::string* buf,
                                                  rocksdb::Slice* value) const {
  assert(ref.part_id < partitions_);
  Partition* p = parts_[ref.part_id].get();
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

}  // namespace zeroflush
