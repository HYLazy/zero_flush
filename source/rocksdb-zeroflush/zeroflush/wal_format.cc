//  Copyright (c) 2026, ZeroFlush-RocksDB.
//  ZeroFlush M1: WAL 分区记录帧编码/解码实现。

#include "zeroflush/wal_format.h"

#include "rocksdb/slice.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace zeroflush {

uint32_t ZfRecordLength(uint32_t key_len, uint32_t val_len) {
  return kZfHeaderSize + key_len + val_len + kZfCrcSize;
}

void EncodeZfRecord(const ZfRecordHeader& h, const rocksdb::Slice& key,
                    const rocksdb::Slice& value, std::string* out) {
  out->clear();
  out->reserve(ZfRecordLength(h.key_len, h.val_len));
  // header（小端）
  rocksdb::PutFixed32(out, h.magic);
  rocksdb::PutFixed16(out, h.cf_id);
  out->push_back(static_cast<char>(h.type));
  out->push_back(static_cast<char>(h.flags));
  rocksdb::PutFixed32(out, h.key_len);
  rocksdb::PutFixed32(out, h.val_len);
  rocksdb::PutFixed64(out, h.seq);
  // body
  out->append(key.data(), key.size());
  out->append(value.data(), value.size());
  // trailer: crc32c 覆盖 header+body
  const char* hdr = out->data();
  uint32_t crc = rocksdb::crc32c::Value(hdr, out->size());
  rocksdb::PutFixed32(out, crc);
}

rocksdb::Status DecodeZfRecord(const char* data, size_t len,
                               ZfRecordHeader* h, rocksdb::Slice* key,
                               rocksdb::Slice* value) {
  if (len < kZfHeaderSize + kZfCrcSize) {
    return rocksdb::Status::Corruption("ZF record too short");
  }
  const char* p = data;
  h->magic = rocksdb::DecodeFixed32(p);
  p += 4;
  h->cf_id = rocksdb::DecodeFixed16(p);
  p += 2;
  h->type = static_cast<uint8_t>(*p++);
  h->flags = static_cast<uint8_t>(*p++);
  h->key_len = rocksdb::DecodeFixed32(p);
  p += 4;
  h->val_len = rocksdb::DecodeFixed32(p);
  p += 4;
  h->seq = rocksdb::DecodeFixed64(p);
  p += 8;
  assert(p - data == kZfHeaderSize);

  if (h->magic != kZfMagic) {
    return rocksdb::Status::Corruption("ZF record bad magic");
  }
  const uint32_t body_len = h->key_len + h->val_len;
  if (len != kZfHeaderSize + body_len + kZfCrcSize) {
    return rocksdb::Status::Corruption("ZF record length mismatch");
  }
  const char* trailer = data + kZfHeaderSize + body_len;
  uint32_t stored_crc = rocksdb::DecodeFixed32(trailer);
  uint32_t calc_crc =
      rocksdb::crc32c::Value(data, kZfHeaderSize + body_len);
  if (stored_crc != calc_crc) {
    return rocksdb::Status::Corruption("ZF record crc mismatch");
  }
  *key = rocksdb::Slice(p, h->key_len);
  p += h->key_len;
  *value = rocksdb::Slice(p, h->val_len);
  return rocksdb::Status::OK();
}

void EncodeZfProps(uint32_t partitions, std::string* out) {
  out->clear();
  out->reserve(kZfPropsSize);
  rocksdb::PutFixed32(out, kZfPropsMagic);
  rocksdb::PutFixed32(out, /*version=*/1);
  rocksdb::PutFixed32(out, partitions);
  // crc32c 覆盖前 12B（magic+version+partitions）
  const uint32_t crc = rocksdb::crc32c::Value(out->data(), 12);
  rocksdb::PutFixed32(out, crc);
}

rocksdb::Status DecodeZfProps(const char* data, size_t len, ZfProps* out) {
  if (len != kZfPropsSize) {
    return rocksdb::Status::Corruption("ZFPROPS: bad size");
  }
  const char* p = data;
  out->magic = rocksdb::DecodeFixed32(p);
  p += 4;
  out->version = rocksdb::DecodeFixed32(p);
  p += 4;
  out->partitions = rocksdb::DecodeFixed32(p);
  p += 4;
  out->crc = rocksdb::DecodeFixed32(p);
  if (out->magic != kZfPropsMagic) {
    return rocksdb::Status::Corruption("ZFPROPS: bad magic");
  }
  if (out->version != 1) {
    return rocksdb::Status::Corruption("ZFPROPS: unsupported version");
  }
  const uint32_t calc = rocksdb::crc32c::Value(data, 12);
  if (calc != out->crc) {
    return rocksdb::Status::Corruption("ZFPROPS: crc mismatch");
  }
  return rocksdb::Status::OK();
}

}  // namespace zeroflush
