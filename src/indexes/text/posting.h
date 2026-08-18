/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_POSTING_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_POSTING_H_

#include <cstdint>

#include "absl/base/optimization.h"
#include "absl/container/btree_map.h"
#include "absl/container/inlined_vector.h"
#include "src/indexes/text/flat_position_map.h"
#include "src/utils/doc_id_map.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

// Forward declaration
struct TextIndexMetadata;

using Key = InternedStringPtr;
using Position = uint32_t;
using FieldMaskPredicate = uint64_t;

struct PostingChunk {
  static constexpr uint32_t kDefaultCapacity = 512;
  PostingChunk *next{nullptr};
  uint32_t size{0};
  uint32_t capacity{0};
  uint8_t data[1];

  static PostingChunk *Create(uint32_t cap = kDefaultCapacity) {
    size_t total = sizeof(PostingChunk) - 1 + cap;
    auto *p = static_cast<PostingChunk *>(::operator new(total));
    p->next = nullptr;
    p->size = 0;
    p->capacity = cap;
    return p;
  }

  static void Destroy(PostingChunk *p) {
    if (p) {
      ::operator delete(p);
    }
  }
};

struct BlockSkipEntry {
  DocId max_doc_id{0};
  PostingChunk *chunk{nullptr};
  uint32_t byte_offset{0};
};

struct EncodedDocId {
  uint8_t bytes[5];
  uint8_t len{0};
  DocId doc_id{0};

  static inline EncodedDocId Encode(DocId id) {
    EncodedDocId enc;
    enc.doc_id = id;
    uint64_t v = id;
    while (v >= 0x80) {
      enc.bytes[enc.len++] = static_cast<uint8_t>((v & 0x7F) | 0x80);
      v >>= 7;
    }
    enc.bytes[enc.len++] = static_cast<uint8_t>(v & 0x7F);
    return enc;
  }
};

static constexpr size_t kBlockSkipInterval = 64;

static inline size_t ReadVarint(const uint8_t *src, size_t max_len,
                                uint64_t &val) {
  if (ABSL_PREDICT_TRUE(max_len > 0)) {
    uint8_t b0 = src[0];
    if (ABSL_PREDICT_TRUE((b0 & 0x80) == 0)) {
      val = b0;
      return 1;
    }
    if (ABSL_PREDICT_TRUE(max_len > 1)) {
      uint8_t b1 = src[1];
      if (ABSL_PREDICT_TRUE((b1 & 0x80) == 0)) {
        val = (b0 & 0x7F) | (static_cast<uint64_t>(b1) << 7);
        return 2;
      }
      if (ABSL_PREDICT_TRUE(max_len > 2)) {
        uint8_t b2 = src[2];
        if (ABSL_PREDICT_TRUE((b2 & 0x80) == 0)) {
          val = (b0 & 0x7F) | (static_cast<uint64_t>(b1 & 0x7F) << 7) |
                (static_cast<uint64_t>(b2) << 14);
          return 3;
        }
      }
    }
  }

  val = 0;
  int shift = 0;
  size_t idx = 0;
  while (idx < max_len && shift < 64) {
    uint8_t byte = src[idx++];
    val |= static_cast<uint64_t>(byte & 0x7F) << shift;
    if ((byte & 0x80) == 0) {
      return idx;
    }
    shift += 7;
  }
  val = 0;
  return 0;
}

template <typename BufferT>
static inline void AppendVarint(BufferT &buf, uint64_t val) {
  while (val >= 0x80) {
    buf.push_back(static_cast<uint8_t>((val & 0x7F) | 0x80));
    val >>= 7;
  }
  buf.push_back(static_cast<uint8_t>(val & 0x7F));
}

struct FieldMask {
  // Constructors
  FieldMask() = default;
  explicit FieldMask(size_t num_fields);

  // FieldMask functions
  void SetField(size_t field_index);
  size_t CountSetFields() const;
  uint64_t GetMask() const;

 private:
  uint64_t mask_ = 0;
  uint8_t num_fields_ = 0;
};

static_assert(sizeof(FieldMask) == 16, "FieldMask should exactly be 16 bytes");

using PositionMap = absl::btree_map<Position, FieldMask>;

struct Postings {
  struct KeyIterator;

  static void *operator new(size_t size) { return ::operator new(size); }
  static void operator delete(void *ptr, size_t size) {
    ::operator delete(ptr);
  }

  Postings() = default;
  // Destructor
  ~Postings();

  // Non-copyable
  Postings(const Postings &) = delete;
  Postings &operator=(const Postings &) = delete;

  // Movable
  Postings(Postings &&other) noexcept;
  Postings &operator=(Postings &&other) noexcept;

  // Are there any postings in this object?
  bool IsEmpty() const;

  // Insert the key with PositionMap (direct, no FlatPositionMap allocation)
  void InsertKey(const Key &key, const PositionMap *pos_map);
  void InsertKey(DocId doc_id, const PositionMap *pos_map);
  void InsertKey(const EncodedDocId &enc_doc_id, const PositionMap *pos_map);

  // Insert the key with FlatPositionMap
  void InsertKey(const Key &key, FlatPositionMap *flat_map);
  void InsertKey(DocId doc_id, FlatPositionMap *flat_map);

  // Remove a key and all positions for it
  void RemoveKey(const Key &key, TextIndexMetadata *metadata);
  void RemoveKey(DocId doc_id, TextIndexMetadata *metadata);

  // Total number of keys
  size_t GetKeyCount() const;

  // Total number of positions for all keys
  size_t GetPositionCount() const;

  // Total frequency of the term across all keys and positions
  size_t GetTotalTermFrequency() const;

  // Defrag this contents of this object. Returns the updated "this" pointer.
  Postings *Defrag();

  // Get a Key iterator.
  KeyIterator GetKeyIterator() const;

  // The Key Iterator
  struct KeyIterator {
    KeyIterator();
    ~KeyIterator() = default;
    KeyIterator(const KeyIterator &other) = default;
    KeyIterator &operator=(const KeyIterator &other) = default;
    KeyIterator(KeyIterator &&other) noexcept = default;
    KeyIterator &operator=(KeyIterator &&other) noexcept = default;

    // Is valid?
    bool IsValid() const;

    // Advance to next key
    void NextKey();

    // Skip forward to next key that is equal to or greater than.
    // return true if it lands on an equal key, false otherwise.
    bool SkipForwardKey(const Key &key);
    bool SkipForwardDocId(DocId target_id);

    // Get Current doc id
    DocId GetDocId() const;

    // Get Current key
    const Key &GetKey() const;

    // Check if word is present in any of the fields specified by field_mask for
    // current key
    bool ContainsFields(uint64_t field_mask) const;

    // Get Position Iterator
    PositionIterator GetPositionIterator() const;

   private:
    friend struct Postings;
    void DecodeDocRecordAtOffset();

    const Postings *postings_{nullptr};
    const PostingChunk *current_chunk_{nullptr};
    uint32_t byte_offset_{0};
    DocId current_doc_id_{kInvalidDocId};
    uint64_t current_pos_count_{0};
    const uint8_t *pos_data_ptr_{nullptr};
    uint32_t next_doc_offset_{0};
  };

 private:
  PostingChunk *head_{nullptr};
  PostingChunk *tail_{nullptr};
  absl::InlinedVector<BlockSkipEntry, 4> skip_index_;
  size_t key_count_{0};
  size_t total_positions_{0};
  size_t total_term_frequency_{0};
  DocId last_doc_id_{0};
};

}  // namespace valkey_search::indexes::text

#endif
