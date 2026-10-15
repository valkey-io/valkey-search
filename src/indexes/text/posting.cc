/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/indexes/text/posting.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>

#include "absl/base/optimization.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "src/indexes/text/text_index.h"
#include "src/utils/doc_id_map.h"

namespace valkey_search::indexes::text {

namespace {

struct DocRecordInfo {
  size_t start_offset{0};
  size_t end_offset{0};
  DocId doc_id{0};
  size_t num_pos{0};
  size_t term_freq{0};
  uint32_t doc_len{0};
};

static bool ReadDocRecordHeaderFromChunk(const PostingChunk *chunk,
                                         size_t offset, DocRecordInfo &info) {
  if (!chunk || offset >= chunk->size) {
    return false;
  }
  info.start_offset = offset;
  const uint8_t *data = chunk->data + offset;
  size_t remain = chunk->size - offset;
  uint64_t doc_id_64 = 0, tf_64 = 0, doc_len_64 = 0, num_pos_64 = 0;
  size_t b1 = ReadVarint(data, remain, doc_id_64);
  if (b1 == 0) {
    return false;
  }
  size_t b2 = ReadVarint(data + b1, remain - b1, tf_64);
  if (b2 == 0) {
    return false;
  }
  size_t b3 = ReadVarint(data + b1 + b2, remain - b1 - b2, doc_len_64);
  if (b3 == 0) {
    return false;
  }
  size_t b4 =
      ReadVarint(data + b1 + b2 + b3, remain - b1 - b2 - b3, num_pos_64);
  if (b4 == 0) {
    return false;
  }
  info.doc_id = static_cast<DocId>(doc_id_64);
  info.term_freq = static_cast<size_t>(tf_64);
  info.doc_len = static_cast<uint32_t>(doc_len_64);
  info.num_pos = static_cast<size_t>(num_pos_64);
  size_t header_bytes = b1 + b2 + b3 + b4;

  if (info.num_pos == 0) {
    info.end_offset = offset + header_bytes;
    return true;
  }

  uint64_t payload_len = 0;
  size_t payload_len_bytes =
      ReadVarint(data + header_bytes, remain - header_bytes, payload_len);
  if (payload_len_bytes == 0 ||
      payload_len > remain - header_bytes - payload_len_bytes) {
    return false;
  }
  info.end_offset = offset + header_bytes + payload_len_bytes + payload_len;
  return true;
}

}  // namespace

// FieldMask Implementation

FieldMask::FieldMask(size_t num_fields) {
  CHECK(num_fields > 0 && num_fields <= 64)
      << "num_fields must be between 1 and 64";
  num_fields_ = static_cast<uint8_t>(num_fields);
}

void FieldMask::SetField(size_t field_index) {
  CHECK(field_index < num_fields_) << "Field index out of range";
  mask_ |= (1ULL << field_index);
}

size_t FieldMask::CountSetFields() const { return __builtin_popcountll(mask_); }

uint64_t FieldMask::GetMask() const { return mask_; }

// Postings Implementation

Postings::~Postings() {
  PostingChunk *curr = head_;
  while (curr != nullptr) {
    PostingChunk *nxt = curr->next;
    PostingChunk::Destroy(curr);
    curr = nxt;
  }
  head_ = nullptr;
  tail_ = nullptr;
}

Postings::Postings(Postings &&other) noexcept
    : head_(other.head_),
      tail_(other.tail_),
      skip_index_(std::move(other.skip_index_)),
      key_count_(other.key_count_),
      total_positions_(other.total_positions_),
      total_term_frequency_(other.total_term_frequency_),
      last_doc_id_(other.last_doc_id_) {
  other.head_ = nullptr;
  other.tail_ = nullptr;
  other.key_count_ = 0;
  other.total_positions_ = 0;
  other.total_term_frequency_ = 0;
  other.last_doc_id_ = 0;
}

Postings &Postings::operator=(Postings &&other) noexcept {
  if (this != &other) {
    this->~Postings();
    head_ = other.head_;
    tail_ = other.tail_;
    skip_index_ = std::move(other.skip_index_);
    key_count_ = other.key_count_;
    total_positions_ = other.total_positions_;
    total_term_frequency_ = other.total_term_frequency_;
    last_doc_id_ = other.last_doc_id_;
    other.head_ = nullptr;
    other.tail_ = nullptr;
    other.key_count_ = 0;
    other.total_positions_ = 0;
    other.total_term_frequency_ = 0;
    other.last_doc_id_ = 0;
  }
  return *this;
}

bool Postings::IsEmpty() const { return key_count_ == 0; }

void Postings::InsertKey(const Key &key, const PositionMap *pos_map,
                         uint32_t tf, uint32_t doc_len) {
  DocId doc_id = DocIdMap::Instance().GetOrAssign(key);
  if (ABSL_PREDICT_FALSE(doc_id == kInvalidDocId)) {
    return;
  }
  InsertKey(doc_id, pos_map, tf, doc_len);
}

void Postings::InsertKey(DocId doc_id, const PositionMap *pos_map, uint32_t tf,
                         uint32_t doc_len) {
  if (ABSL_PREDICT_FALSE(doc_id == kInvalidDocId)) {
    return;
  }
  InsertKey(EncodedDocId::Encode(doc_id), pos_map, tf, doc_len);
}

void Postings::InsertKey(const EncodedDocId &enc_doc_id,
                         const PositionMap *pos_map, uint32_t tf,
                         uint32_t doc_len) {
  if (ABSL_PREDICT_FALSE(enc_doc_id.doc_id == kInvalidDocId)) {
    return;
  }

  size_t num_pos = pos_map ? pos_map->size() : 0;
  size_t term_freq = tf;
  if (term_freq == 0 && pos_map) {
    for (const auto &[pos, mask] : *pos_map) {
      term_freq += mask.CountSetFields();
    }
  }

  // Worst-case per position: 5-byte uint32 delta + 10-byte uint64 mask = 15B.
  // Reserve 16 bytes per position to guarantee no buffer overrun.
  size_t max_pos_bytes = num_pos * 16;
  uint8_t stack_buf[512];
  uint8_t *pdest = stack_buf;
  std::unique_ptr<uint8_t[]> heap_buf;
  if (pos_map && num_pos > 0) {
    if (ABSL_PREDICT_FALSE(max_pos_bytes > sizeof(stack_buf))) {
      heap_buf = std::make_unique<uint8_t[]>(max_pos_bytes);
      pdest = heap_buf.get();
    }
    uint32_t last_pos = 0;
    for (const auto &[pos, mask] : *pos_map) {
      WriteVarint(pdest, pos - last_pos);
      WriteVarint(pdest, mask.GetMask());
      last_pos = pos;
    }
  }
  uint8_t *pstart = heap_buf ? heap_buf.get() : stack_buf;
  size_t payload_len = static_cast<size_t>(pdest - pstart);

  // If this DocId is <= last_doc_id_ (e.g. slight out-of-order arrival across
  // concurrent ingestion threads, recycled DocId, or re-indexing an existing
  // DocId), locate the position in a single pass, jumping directly to tail_
  // when the DocId belongs in the tail chunk.
  if (ABSL_PREDICT_FALSE(key_count_ > 0 && enc_doc_id.doc_id <= last_doc_id_)) {
    uint8_t header_buf[48];
    uint8_t *hptr = header_buf;
    std::memcpy(hptr, enc_doc_id.bytes, enc_doc_id.len);
    hptr += enc_doc_id.len;
    WriteVarint(hptr, term_freq);
    WriteVarint(hptr, doc_len);
    WriteVarint(hptr, num_pos);
    if (num_pos > 0) {
      WriteVarint(hptr, payload_len);
    }
    size_t header_len = static_cast<size_t>(hptr - header_buf);
    size_t record_len = header_len + payload_len;

    PostingChunk *chunk = head_;
    PostingChunk *prev_chunk = nullptr;

    // Fast path for concurrent ingestion: if the incoming DocId is strictly
    // greater than the first DocId of tail_, it belongs inside tail_ after
    // offset 0. Jump directly to tail_ in O(1) without scanning earlier chunks.
    if (head_ != tail_ && tail_ != nullptr && tail_->size > 0) {
      uint64_t tail_first_id = 0;
      if (ReadVarint(tail_->data, tail_->size, tail_first_id) > 0 &&
          enc_doc_id.doc_id > tail_first_id) {
        chunk = tail_;
      }
    }

    while (chunk != nullptr) {
      // Skip entire chunks whose next chunk starts at or before enc_doc_id.
      if (chunk != tail_ && chunk->next != nullptr && chunk->next->size > 0) {
        uint64_t next_first_id = 0;
        if (ReadVarint(chunk->next->data, chunk->next->size, next_first_id) >
                0 &&
            enc_doc_id.doc_id >= next_first_id) {
          prev_chunk = chunk;
          chunk = chunk->next;
          continue;
        }
      }

      uint32_t offset = 0;
      while (offset < chunk->size) {
        DocRecordInfo existing_info;
        if (!ReadDocRecordHeaderFromChunk(chunk, offset, existing_info)) {
          break;
        }
        if (existing_info.doc_id >= enc_doc_id.doc_id) {
          const bool is_replace = (existing_info.doc_id == enc_doc_id.doc_id);
          const size_t old_len =
              is_replace
                  ? (existing_info.end_offset - existing_info.start_offset)
                  : 0;
          const uint32_t tail_src =
              is_replace ? existing_info.end_offset : offset;
          const uint32_t tail_len = chunk->size - tail_src;

          if (chunk->size - old_len + record_len <= chunk->capacity) {
            if (tail_len > 0 && record_len != old_len) {
              std::memmove(chunk->data + offset + record_len,
                           chunk->data + tail_src, tail_len);
            }
            std::memcpy(chunk->data + offset, header_buf, header_len);
            if (payload_len > 0) {
              std::memcpy(chunk->data + offset + header_len, pstart,
                          payload_len);
            }
            chunk->size =
                static_cast<uint32_t>(chunk->size - old_len + record_len);
          } else if (offset > 0) {
            // Split chunk at offset so we never need prev_chunk (safe when we
            // jumped directly to tail_).
            uint32_t new_cap =
                std::max(PostingChunk::kDefaultCapacity,
                         static_cast<uint32_t>(record_len + tail_len + 32));
            PostingChunk *new_chunk = PostingChunk::Create(new_cap);
            std::memcpy(new_chunk->data, header_buf, header_len);
            if (payload_len > 0) {
              std::memcpy(new_chunk->data + header_len, pstart, payload_len);
            }
            if (tail_len > 0) {
              std::memcpy(new_chunk->data + record_len, chunk->data + tail_src,
                          tail_len);
            }
            new_chunk->size = static_cast<uint32_t>(record_len + tail_len);
            chunk->size = offset;
            new_chunk->next = chunk->next;
            chunk->next = new_chunk;
            if (tail_ == chunk) {
              tail_ = new_chunk;
            }
          } else {
            uint32_t new_cap =
                std::max(PostingChunk::kDefaultCapacity,
                         static_cast<uint32_t>(record_len + tail_len + 32));
            PostingChunk *new_chunk = PostingChunk::Create(new_cap);
            std::memcpy(new_chunk->data, header_buf, header_len);
            if (payload_len > 0) {
              std::memcpy(new_chunk->data + header_len, pstart, payload_len);
            }
            if (tail_len > 0) {
              std::memcpy(new_chunk->data + record_len, chunk->data + tail_src,
                          tail_len);
            }
            new_chunk->size = static_cast<uint32_t>(record_len + tail_len);
            new_chunk->next = chunk->next;
            if (prev_chunk != nullptr) {
              prev_chunk->next = new_chunk;
            } else {
              head_ = new_chunk;
            }
            if (tail_ == chunk) {
              tail_ = new_chunk;
            }
            PostingChunk::Destroy(chunk);
          }

          skip_index_.clear();
          if (is_replace) {
            if (total_positions_ >= existing_info.num_pos) {
              total_positions_ -= existing_info.num_pos;
            }
            if (total_term_frequency_ >= existing_info.term_freq) {
              total_term_frequency_ -= existing_info.term_freq;
            }
          } else {
            key_count_++;
          }
          total_positions_ += num_pos;
          total_term_frequency_ += term_freq;
          return;
        }
        offset = existing_info.end_offset;
      }
      prev_chunk = chunk;
      chunk = chunk->next;
    }
  }

  // Fast path: append to tail_ in ascending DocId order.
  size_t est_bytes_needed = 36 + payload_len;
  if (tail_ == nullptr ||
      (tail_->size + est_bytes_needed > tail_->capacity && tail_->size > 0)) {
    uint32_t chunk_cap = std::max(PostingChunk::kDefaultCapacity,
                                  static_cast<uint32_t>(est_bytes_needed + 32));
    auto *new_chunk = PostingChunk::Create(chunk_cap);
    if (tail_ != nullptr) {
      tail_->next = new_chunk;
      tail_ = new_chunk;
    } else {
      head_ = tail_ = new_chunk;
    }
  }

  uint8_t *dest = tail_->data + tail_->size;
  uint8_t *record_start = dest;

  // Layout per document record in PostingChunk:
  // [Encoded DocId (varint)] [tf (varint)] [doc_len (varint)] [num_pos
  // (varint)] [payload_len (varint)] [delta1, mask1, delta2, mask2, ...]

  // 1. Append pre-encoded doc_id bytes (cached to avoid repeated varint
  // encoding)
  std::memcpy(dest, enc_doc_id.bytes, enc_doc_id.len);
  dest += enc_doc_id.len;

  // 2. Append term frequency and document length (varint encoded)
  WriteVarint(dest, term_freq);
  WriteVarint(dest, doc_len);

  // 3. Append number of positions (varint encoded)
  WriteVarint(dest, num_pos);

  // 4. Append serialized positions & field bitmasks if present
  if (num_pos > 0) {
    WriteVarint(dest, payload_len);
    std::memcpy(dest, pstart, payload_len);
    dest += payload_len;
  }

  uint32_t offset_in_chunk = static_cast<uint32_t>(record_start - tail_->data);
  tail_->size = static_cast<uint32_t>(dest - tail_->data);

  if (key_count_ > 0 && (key_count_ % kBlockSkipInterval == 0)) {
    skip_index_.push_back(BlockSkipEntry{last_doc_id_, tail_, offset_in_chunk});
  }
  last_doc_id_ = enc_doc_id.doc_id;

  key_count_++;
  total_positions_ += num_pos;
  total_term_frequency_ += term_freq;
}

void Postings::RemoveKey(const Key &key, TextIndexMetadata *metadata) {
  DocId target_id = DocIdMap::Instance().GetDocId(key);
  if (target_id == kInvalidDocId) {
    return;
  }
  RemoveKey(target_id, metadata);
}

void Postings::RemoveKey(DocId target_id, TextIndexMetadata *metadata) {
  if (key_count_ == 0 || target_id == kInvalidDocId ||
      target_id > last_doc_id_) {
    return;
  }

  PostingChunk *chunk = head_;
  PostingChunk *prev_chunk = nullptr;

  if (head_ != tail_ && tail_ != nullptr && tail_->size > 0) {
    uint64_t tail_first_id = 0;
    if (ReadVarint(tail_->data, tail_->size, tail_first_id) > 0 &&
        target_id > tail_first_id) {
      chunk = tail_;
    }
  }

  while (chunk != nullptr) {
    if (chunk != tail_ && chunk->next != nullptr && chunk->next->size > 0) {
      uint64_t next_first_id = 0;
      if (ReadVarint(chunk->next->data, chunk->next->size, next_first_id) > 0 &&
          target_id >= next_first_id) {
        prev_chunk = chunk;
        chunk = chunk->next;
        continue;
      }
    }

    uint32_t offset = 0;
    DocId prev_doc_id_in_chunk = 0;
    while (offset < chunk->size) {
      DocRecordInfo existing_info;
      if (!ReadDocRecordHeaderFromChunk(chunk, offset, existing_info)) {
        break;
      }
      if (existing_info.doc_id > target_id) {
        return;
      }
      if (existing_info.doc_id == target_id) {
        size_t deleted_bytes =
            existing_info.end_offset - existing_info.start_offset;
        std::memmove(chunk->data + existing_info.start_offset,
                     chunk->data + existing_info.end_offset,
                     chunk->size - existing_info.end_offset);
        chunk->size -= deleted_bytes;

        if (key_count_ > 0) {
          key_count_--;
        }
        if (total_positions_ >= existing_info.num_pos) {
          total_positions_ -= existing_info.num_pos;
        }
        if (total_term_frequency_ >= existing_info.term_freq) {
          total_term_frequency_ -= existing_info.term_freq;
        }
        if (metadata) {
          if (metadata->total_positions >= existing_info.num_pos) {
            metadata->total_positions -= existing_info.num_pos;
          }
          if (metadata->total_term_frequency >= existing_info.term_freq) {
            metadata->total_term_frequency -= existing_info.term_freq;
          }
        }

        // Invalidate skip_index_ to avoid stale chunk offsets after deletion
        skip_index_.clear();

        if (chunk->size == 0 && (head_ != tail_)) {
          if (prev_chunk) {
            prev_chunk->next = chunk->next;
          } else {
            head_ = chunk->next;
          }
          if (chunk == tail_) {
            tail_ = prev_chunk;
          }
          PostingChunk::Destroy(chunk);
        }

        if (key_count_ == 0) {
          last_doc_id_ = 0;
          if (head_ != nullptr) {
            PostingChunk::Destroy(head_);
            head_ = nullptr;
            tail_ = nullptr;
          }
        } else if (target_id == last_doc_id_) {
          if (prev_doc_id_in_chunk != 0) {
            last_doc_id_ = prev_doc_id_in_chunk;
          } else {
            PostingChunk *c = (prev_chunk != nullptr) ? prev_chunk : head_;
            DocId l_id = 0;
            while (c != nullptr) {
              uint32_t o = 0;
              DocRecordInfo r_info;
              while (o < c->size &&
                     ReadDocRecordHeaderFromChunk(c, o, r_info)) {
                l_id = r_info.doc_id;
                o = r_info.end_offset;
              }
              c = c->next;
            }
            last_doc_id_ = l_id;
          }
        }
        return;
      }
      prev_doc_id_in_chunk = existing_info.doc_id;
      offset = existing_info.end_offset;
    }
    prev_chunk = chunk;
    chunk = chunk->next;
  }
}

size_t Postings::GetKeyCount() const { return key_count_; }

size_t Postings::GetPositionCount() const { return total_positions_; }

size_t Postings::GetTotalTermFrequency() const { return total_term_frequency_; }

std::optional<PostingDocStats> Postings::GetPostingDocStats(
    BorrowedInternedStringPtr key, uint64_t field_mask) const {
  DocId target_id = DocIdMap::Instance().GetDocId(key.AsOwned());
  if (target_id == kInvalidDocId) {
    return std::nullopt;
  }
  KeyIterator iter = GetKeyIterator();
  if (!iter.SkipForwardDocId(target_id)) {
    return std::nullopt;
  }
  if (!iter.ContainsFields(field_mask)) {
    return std::nullopt;
  }
  return PostingDocStats{static_cast<uint32_t>(iter.GetTermFrequency()),
                         iter.GetDocLen()};
}

Postings *Postings::Defrag() { return this; }

// Iterators Implementation

Postings::KeyIterator::KeyIterator() = default;

Postings::KeyIterator Postings::GetKeyIterator() const {
  KeyIterator iterator;
  iterator.postings_ = this;
  iterator.current_chunk_ = head_;
  iterator.byte_offset_ = 0;
  if (head_ != nullptr && head_->size > 0) {
    iterator.DecodeDocRecordAtOffset();
  }
  return iterator;
}

void Postings::KeyIterator::DecodeDocRecordAtOffset() {
  while (current_chunk_ != nullptr && byte_offset_ >= current_chunk_->size) {
    current_chunk_ = current_chunk_->next;
    byte_offset_ = 0;
  }

  if (!current_chunk_) {
    current_doc_id_ = kInvalidDocId;
    current_tf_ = 0;
    current_doc_len_ = 0;
    current_pos_count_ = 0;
    return;
  }

  const uint8_t *data = current_chunk_->data + byte_offset_;
  size_t remain = current_chunk_->size - byte_offset_;
  uint64_t id = 0, tf = 0, doc_len = 0, pos_cnt = 0;
  size_t b1 = ReadVarint(data, remain, id);
  if (b1 == 0) {
    current_chunk_ = nullptr;
    current_doc_id_ = kInvalidDocId;
    current_tf_ = 0;
    current_doc_len_ = 0;
    current_pos_count_ = 0;
    return;
  }
  size_t b2 = ReadVarint(data + b1, remain - b1, tf);
  if (b2 == 0) {
    current_chunk_ = nullptr;
    current_doc_id_ = kInvalidDocId;
    current_tf_ = 0;
    current_doc_len_ = 0;
    current_pos_count_ = 0;
    return;
  }
  size_t b3 = ReadVarint(data + b1 + b2, remain - b1 - b2, doc_len);
  if (b3 == 0) {
    current_chunk_ = nullptr;
    current_doc_id_ = kInvalidDocId;
    current_tf_ = 0;
    current_doc_len_ = 0;
    current_pos_count_ = 0;
    return;
  }
  size_t b4 = ReadVarint(data + b1 + b2 + b3, remain - b1 - b2 - b3, pos_cnt);
  if (b4 == 0) {
    current_chunk_ = nullptr;
    current_doc_id_ = kInvalidDocId;
    current_tf_ = 0;
    current_doc_len_ = 0;
    current_pos_count_ = 0;
    return;
  }
  current_doc_id_ = static_cast<DocId>(id);
  current_tf_ = static_cast<uint32_t>(tf);
  current_doc_len_ = static_cast<uint32_t>(doc_len);
  current_pos_count_ = pos_cnt;

  size_t header_len = b1 + b2 + b3 + b4;
  if (pos_cnt == 0) {
    pos_data_ptr_ = data + header_len;
    next_doc_offset_ = byte_offset_ + header_len;
  } else {
    uint64_t payload_len = 0;
    size_t payload_bytes_read =
        ReadVarint(data + header_len, remain - header_len, payload_len);
    if (payload_bytes_read == 0 ||
        payload_len > remain - header_len - payload_bytes_read) {
      current_chunk_ = nullptr;
      current_doc_id_ = kInvalidDocId;
      current_tf_ = 0;
      current_doc_len_ = 0;
      current_pos_count_ = 0;
      return;
    }
    pos_data_ptr_ = data + header_len + payload_bytes_read;
    next_doc_offset_ =
        byte_offset_ + header_len + payload_bytes_read + payload_len;
  }
}

bool Postings::KeyIterator::IsValid() const {
  return current_chunk_ != nullptr && current_doc_id_ != kInvalidDocId;
}

void Postings::KeyIterator::NextKey() {
  if (!IsValid()) {
    return;
  }
  byte_offset_ = next_doc_offset_;
  DecodeDocRecordAtOffset();
}

bool Postings::KeyIterator::ContainsFields(uint64_t field_mask) const {
  if (!IsValid() || current_pos_count_ == 0 || !pos_data_ptr_) {
    return false;
  }
  if (field_mask == ~0ULL) {
    return true;
  }
  const uint8_t *data = pos_data_ptr_;
  size_t remain = next_doc_offset_ - (pos_data_ptr_ - current_chunk_->data);
  size_t idx = 0;
  for (size_t i = 0; i < current_pos_count_; ++i) {
    uint64_t delta = 0, mask = 0;
    size_t b1 = ReadVarint(data + idx, remain - idx, delta);
    if (b1 == 0) {
      return false;
    }
    idx += b1;
    size_t b2 = ReadVarint(data + idx, remain - idx, mask);
    if (b2 == 0) {
      return false;
    }
    idx += b2;
    if ((mask & field_mask) != 0) {
      return true;
    }
  }
  return false;
}

bool Postings::KeyIterator::SkipForwardDocId(DocId target_id) {
  if (!IsValid() || current_doc_id_ >= target_id) {
    return IsValid() && current_doc_id_ == target_id;
  }

  const auto &skip_index = postings_->skip_index_;
  if (!skip_index.empty()) {
    auto it = std::lower_bound(skip_index.begin(), skip_index.end(), target_id,
                               [](const BlockSkipEntry &entry, DocId val) {
                                 return entry.max_doc_id < val;
                               });

    if (it != skip_index.begin()) {
      const auto &entry = *(it - 1);
      if (entry.chunk != current_chunk_ || entry.byte_offset > byte_offset_) {
        current_chunk_ = entry.chunk;
        byte_offset_ = entry.byte_offset;
        DecodeDocRecordAtOffset();
      }
    }
  }

  while (IsValid() && current_doc_id_ < target_id) {
    NextKey();
  }
  return IsValid() && current_doc_id_ == target_id;
}

bool Postings::KeyIterator::SkipForwardKey(const Key &key) {
  DocId target_id = DocIdMap::Instance().GetDocId(key);
  if (target_id == kInvalidDocId) {
    current_chunk_ = nullptr;
    current_doc_id_ = kInvalidDocId;
    current_tf_ = 0;
    current_doc_len_ = 0;
    current_pos_count_ = 0;
    return false;
  }
  return SkipForwardDocId(target_id);
}

DocId Postings::KeyIterator::GetDocId() const {
  CHECK(IsValid()) << "KeyIterator is invalid or exhausted";
  return current_doc_id_;
}

const Key &Postings::KeyIterator::GetKey() const {
  CHECK(IsValid()) << "KeyIterator is invalid or exhausted";
  return DocIdMap::Instance().GetKey(current_doc_id_);
}

PositionIterator Postings::KeyIterator::GetPositionIterator() const {
  if (!IsValid() || current_pos_count_ == 0 || !pos_data_ptr_) {
    return {};
  }
  return {pos_data_ptr_,
          static_cast<size_t>(next_doc_offset_ -
                              (pos_data_ptr_ - current_chunk_->data)),
          current_pos_count_};
}

size_t Postings::KeyIterator::GetTermFrequency() const {
  CHECK(IsValid()) << "KeyIterator is invalid or exhausted";
  return current_tf_;
}

uint32_t Postings::KeyIterator::GetDocLen() const {
  CHECK(IsValid()) << "KeyIterator is invalid or exhausted";
  return current_doc_len_;
}

PositionIterator::PositionIterator(const uint8_t *data, size_t max_bytes,
                                   size_t num_positions)
    : stream_data_(data),
      stream_max_bytes_(max_bytes),
      stream_num_positions_(num_positions) {
  if (stream_num_positions_ > 0 && stream_data_ != nullptr &&
      stream_max_bytes_ > 0) {
    DecodeStreamPosition();
  }
}

void PositionIterator::DecodeStreamPosition() {
  if (stream_byte_offset_ >= stream_max_bytes_) {
    stream_pos_index_ = stream_num_positions_;
    return;
  }
  uint64_t delta = 0, mask = 0;
  size_t delta_bytes_read = ReadVarint(
      reinterpret_cast<const uint8_t *>(stream_data_) + stream_byte_offset_,
      stream_max_bytes_ - stream_byte_offset_, delta);
  if (delta_bytes_read == 0) {
    stream_pos_index_ = stream_num_positions_;
    return;
  }
  stream_byte_offset_ += delta_bytes_read;
  size_t mask_bytes_read = ReadVarint(
      reinterpret_cast<const uint8_t *>(stream_data_) + stream_byte_offset_,
      stream_max_bytes_ - stream_byte_offset_, mask);
  if (mask_bytes_read == 0) {
    stream_pos_index_ = stream_num_positions_;
    return;
  }
  stream_byte_offset_ += mask_bytes_read;
  stream_cumulative_pos_ += static_cast<Position>(delta);
  stream_field_mask_ = mask;
}

bool PositionIterator::IsValid() const {
  return stream_data_ != nullptr && stream_pos_index_ < stream_num_positions_;
}

void PositionIterator::NextPosition() {
  if (stream_data_ != nullptr && stream_pos_index_ < stream_num_positions_) {
    stream_pos_index_++;
    if (stream_pos_index_ < stream_num_positions_) {
      DecodeStreamPosition();
    }
  }
}

bool PositionIterator::SkipForwardPosition(Position target) {
  while (IsValid() && GetPosition() < target) {
    NextPosition();
  }
  return IsValid() && GetPosition() == target;
}

Position PositionIterator::GetPosition() const {
  return stream_cumulative_pos_;
}

uint64_t PositionIterator::GetFieldMask() const { return stream_field_mask_; }

}  // namespace valkey_search::indexes::text
