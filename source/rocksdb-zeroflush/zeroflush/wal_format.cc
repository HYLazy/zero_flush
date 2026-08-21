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

// ---------------------------------------------------------------------------
// ZFPROPS v2
// ---------------------------------------------------------------------------

rocksdb::Status EncodeZfPropsV2(uint8_t routing_mode,
                                const std::string& comparator_name,
                                const std::vector<ZfPropsTableInfo>& tables,
                                uint32_t current_version,
                                std::string* out) {
  out->clear();
  std::string buf;

  // Header: magic + format_version + routing_mode + pad
  rocksdb::PutFixed32(&buf, kZfPropsMagicV2);
  rocksdb::PutFixed32(&buf, 2);  // format_version
  buf.push_back(static_cast<char>(routing_mode));
  buf.append(3, '\0');  // pad

  // Comparator name
  rocksdb::PutFixed32(&buf, static_cast<uint32_t>(comparator_name.size()));
  buf.append(comparator_name);

  // Table count
  rocksdb::PutFixed32(&buf, static_cast<uint32_t>(tables.size()));

  // Per-table data
  for (const auto& t : tables) {
    rocksdb::PutFixed32(&buf, t.version);
    rocksdb::PutFixed32(&buf, t.partitions);
    for (uint32_t pid : t.part_ids) {
      rocksdb::PutFixed32(&buf, pid);
    }
    rocksdb::PutFixed32(&buf, static_cast<uint32_t>(t.boundaries.size()));
    for (const auto& b : t.boundaries) {
      rocksdb::PutFixed32(&buf, static_cast<uint32_t>(b.size()));
      buf.append(b);
    }
  }

  // Current version
  rocksdb::PutFixed32(&buf, current_version);

  // CRC32C over entire payload
  const uint32_t crc = rocksdb::crc32c::Value(buf.data(), buf.size());
  rocksdb::PutFixed32(&buf, crc);

  *out = std::move(buf);
  return rocksdb::Status::OK();
}

rocksdb::Status DecodeZfPropsAuto(const char* data, size_t len,
                                  ZfPropsV2* out) {
  if (len < 8) {
    return rocksdb::Status::Corruption("ZFPROPS: too short");
  }
  const uint32_t magic = rocksdb::DecodeFixed32(data);
  if (magic == kZfPropsMagic) {
    // v1 格式：向前兼容包装为 v2 结构。
    if (len < kZfPropsSize) {
      return rocksdb::Status::Corruption("ZFPROPS: v1 too short");
    }
    ZfProps v1;
    {
      rocksdb::Status s = DecodeZfProps(data, len, &v1);
      if (!s.ok()) return s;
    }
    out->magic = kZfPropsMagic;
    out->format_version = 1;
    out->routing_mode = 0;  // kHash
    out->comparator_name.clear();  // v1 无 comparator 信息
    ZfPropsTableInfo ti;
    ti.version = 0;
    ti.partitions = v1.partitions;
    ti.part_ids.clear();
    for (uint32_t i = 0; i < v1.partitions; ++i) {
      ti.part_ids.push_back(i);
    }
    ti.boundaries.clear();  // hash 模式无边界
    out->tables = {ti};
    out->current_version = 0;
    out->crc = v1.crc;
    return rocksdb::Status::OK();
  }
  if (magic != kZfPropsMagicV2) {
    return rocksdb::Status::Corruption("ZFPROPS: bad magic");
  }
  // v2 解码。
  const char* p = data;
  const char* end = data + len;
  if (end - p < 12) {  // magic(4) + ver(4) + rmode(1) + pad(3)
    return rocksdb::Status::Corruption("ZFPROPS v2: header too short");
  }
  out->magic = kZfPropsMagicV2;
  p += 4;
  out->format_version = rocksdb::DecodeFixed32(p);
  p += 4;
  if (out->format_version != 2) {
    return rocksdb::Status::Corruption(
        "ZFPROPS v2: unsupported format_version");
  }
  out->routing_mode = static_cast<uint8_t>(*p);
  p += 4;  // routing_mode(1) + pad(3)

  // Comparator name
  if (end - p < 4) {
    return rocksdb::Status::Corruption("ZFPROPS v2: cname_len truncated");
  }
  const uint32_t cname_len = rocksdb::DecodeFixed32(p);
  p += 4;
  if (static_cast<size_t>(end - p) < cname_len) {
    return rocksdb::Status::Corruption("ZFPROPS v2: cname truncated");
  }
  out->comparator_name.assign(p, cname_len);
  p += cname_len;

  // Table count
  if (end - p < 4) {
    return rocksdb::Status::Corruption("ZFPROPS v2: table_count truncated");
  }
  const uint32_t table_count = rocksdb::DecodeFixed32(p);
  p += 4;

  out->tables.clear();
  out->tables.reserve(table_count);
  for (uint32_t ti = 0; ti < table_count; ++ti) {
    ZfPropsTableInfo t;
    if (end - p < 8) {  // version(4) + partitions(4)
      return rocksdb::Status::Corruption("ZFPROPS v2: table header truncated");
    }
    t.version = rocksdb::DecodeFixed32(p);
    p += 4;
    t.partitions = rocksdb::DecodeFixed32(p);
    p += 4;

    // part_ids P × 4B
    if (static_cast<size_t>(end - p) < t.partitions * 4) {
      return rocksdb::Status::Corruption("ZFPROPS v2: part_ids truncated");
    }
    t.part_ids.resize(t.partitions);
    for (uint32_t i = 0; i < t.partitions; ++i) {
      t.part_ids[i] = rocksdb::DecodeFixed32(p);
      p += 4;
    }

    // boundary_count
    if (end - p < 4) {
      return rocksdb::Status::Corruption("ZFPROPS v2: boundary count truncated");
    }
    const uint32_t bc = rocksdb::DecodeFixed32(p);
    p += 4;

    t.boundaries.reserve(bc);
    for (uint32_t bi = 0; bi < bc; ++bi) {
      if (end - p < 4) {
        return rocksdb::Status::Corruption("ZFPROPS v2: boundary len truncated");
      }
      const uint32_t blen = rocksdb::DecodeFixed32(p);
      p += 4;
      if (static_cast<size_t>(end - p) < blen) {
        return rocksdb::Status::Corruption("ZFPROPS v2: boundary data truncated");
      }
      t.boundaries.emplace_back(p, blen);
      p += blen;
    }
    out->tables.push_back(std::move(t));
  }

  // Current version
  if (end - p < 4) {
    return rocksdb::Status::Corruption("ZFPROPS v2: current_version truncated");
  }
  out->current_version = rocksdb::DecodeFixed32(p);
  p += 4;

  // CRC (last 4 bytes)
  if (end - p < 4) {
    return rocksdb::Status::Corruption("ZFPROPS v2: crc truncated");
  }
  out->crc = rocksdb::DecodeFixed32(p);

  // Verify CRC over the payload (everything before crc itself).
  const size_t payload_len = static_cast<size_t>(p - data);
  const uint32_t calc = rocksdb::crc32c::Value(data, payload_len);
  if (calc != out->crc) {
    return rocksdb::Status::Corruption("ZFPROPS v2: crc mismatch");
  }

  return rocksdb::Status::OK();
}

}  // namespace zeroflush
