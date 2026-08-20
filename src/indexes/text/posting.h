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

// ============================================================================
// Variable-Byte (Varint / LEB128) Bit Manipulation Constants and Macros
// ============================================================================
// The posting list encodes integer sequences (DocId, position counts, delta-encoded
// positions, and field bitmasks) using variable-length byte encoding (Varint/LEB128).
//
// Byte Layout:
//   - Bit 7 (MSB): Continuation flag.
//       1 = More bytes follow for this integer.
//       0 = Terminal (last) byte of this integer.
//   - Bits 0..6: 7-bit payload data (stored in little-endian order, 7 bits per byte).
// ============================================================================

#define VARINT_DATA_MASK        0x7FU  // 0b01111111: extracts 7 bits of payload data
#define VARINT_CONTINUE_BIT     0x80U  // 0b10000000: flag indicating continuation byte
#define VARINT_BITS_PER_BYTE    7U     // Number of data bits per byte

// Macro helpers for varint byte encoding and inspection:
#define VARINT_ENCODE_MORE(val) (static_cast<uint8_t>(((val) & VARINT_DATA_MASK) | VARINT_CONTINUE_BIT))
#define VARINT_ENCODE_LAST(val) (static_cast<uint8_t>((val) & VARINT_DATA_MASK))
#define VARINT_IS_LAST(byte)    (((byte) & VARINT_CONTINUE_BIT) == 0)
#define VARINT_PAYLOAD(byte)    (static_cast<uint64_t>((byte) & VARINT_DATA_MASK))

// Fast-path bit-shift constants for branch-predicted varint decoding:
#define VARINT_SHIFT_BYTE_1     7U
#define VARINT_SHIFT_BYTE_2     14U
#define VARINT_SHIFT_BYTE_3     21U
#define VARINT_SHIFT_BYTE_4     28U

struct BlockSkipEntry {
  DocId max_doc_id{0};
  PostingChunk *chunk{nullptr};
  uint32_t byte_offset{0};
};

// Encodes an integer using varint (LEB128) into a destination byte pointer,
// advancing dest past the written bytes.
template <typename T>
static inline void WriteVarint(uint8_t *&dest, T value) {
  uint64_t v = static_cast<uint64_t>(value);
  while (v >= VARINT_CONTINUE_BIT) {
    *dest++ = VARINT_ENCODE_MORE(v);
    v >>= VARINT_BITS_PER_BYTE;
  }
  *dest++ = VARINT_ENCODE_LAST(v);
}

// EncodedDocId caches the pre-computed varint representation of a DocId.
// Documents typically appear across hundreds of tokens; pre-encoding the DocId
// once per document avoids repeated varint encoding during ingestion.
struct EncodedDocId {
  uint8_t bytes[5];
  uint8_t len{0};
  DocId doc_id{0};

  static inline EncodedDocId Encode(DocId id) {
    EncodedDocId enc;
    enc.doc_id = id;
    uint8_t *ptr = enc.bytes;
    WriteVarint(ptr, id);
    enc.len = static_cast<uint8_t>(ptr - enc.bytes);
    return enc;
  }
};

static constexpr size_t kBlockSkipInterval = 64;

// Reads a 64-bit unsigned integer from a varint-encoded byte stream.
// Returns the number of bytes consumed (or 0 if buffer is malformed or exhausted).
// Branch-predicted fast-paths handle 1-byte, 2-byte, and 3-byte integers.
static inline size_t ReadVarint(const uint8_t *src, size_t max_len,
                                uint64_t &val) {
  if (ABSL_PREDICT_TRUE(max_len > 0)) {
    uint8_t b0 = src[0];
    // Fast-path 1: Single-byte varint (value in [0, 127])
    if (ABSL_PREDICT_TRUE(VARINT_IS_LAST(b0))) {
      val = VARINT_PAYLOAD(b0);
      return 1;
    }
    if (ABSL_PREDICT_TRUE(max_len > 1)) {
      uint8_t b1 = src[1];
      // Fast-path 2: Two-byte varint (value in [128, 16,383])
      if (ABSL_PREDICT_TRUE(VARINT_IS_LAST(b1))) {
        val = VARINT_PAYLOAD(b0) | (VARINT_PAYLOAD(b1) << VARINT_SHIFT_BYTE_1);
        return 2;
      }
      if (ABSL_PREDICT_TRUE(max_len > 2)) {
        uint8_t b2 = src[2];
        // Fast-path 3: Three-byte varint (value in [16,384, 2,097,151])
        if (ABSL_PREDICT_TRUE(VARINT_IS_LAST(b2))) {
          val = VARINT_PAYLOAD(b0) |
                (VARINT_PAYLOAD(b1) << VARINT_SHIFT_BYTE_1) |
                (VARINT_PAYLOAD(b2) << VARINT_SHIFT_BYTE_2);
          return 3;
        }
      }
    }
  }

  // General fallback loop for 4+ bytes
  val = 0;
  int shift = 0;
  size_t idx = 0;
  while (idx < max_len && shift < 64) {
    uint8_t byte = src[idx++];
    val |= VARINT_PAYLOAD(byte) << shift;
    if (VARINT_IS_LAST(byte)) {
      return idx;
    }
    shift += VARINT_BITS_PER_BYTE;
  }
  val = 0;
  return 0;
}

template <typename BufferT>
static inline void AppendVarint(BufferT &buf, uint64_t val) {
  while (val >= VARINT_CONTINUE_BIT) {
    buf.push_back(VARINT_ENCODE_MORE(val));
    val >>= VARINT_BITS_PER_BYTE;
  }
  buf.push_back(VARINT_ENCODE_LAST(val));
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

using PositionMap = absl::btree_map<Position, FieldMask>;

// Iterator for Position data direct from chunk varint stream
class PositionIterator {
 public:
  PositionIterator() = default;
  PositionIterator(const uint8_t *data, size_t max_bytes, size_t num_positions);

  bool IsValid() const;
  void NextPosition();
  bool SkipForwardPosition(Position target);
  Position GetPosition() const;
  uint64_t GetFieldMask() const;

 private:
  void DecodeStreamPosition();

  // Stream-direct members
  const uint8_t *stream_data_{nullptr};
  size_t stream_max_bytes_{0};
  size_t stream_byte_offset_{0};
  size_t stream_num_positions_{0};
  size_t stream_pos_index_{0};
  Position stream_cumulative_pos_{0};
  uint64_t stream_field_mask_{0};
};

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

  // Insert the key with PositionMap (direct, zero intermediate allocation)
  void InsertKey(const Key &key, const PositionMap *pos_map);
  void InsertKey(DocId doc_id, const PositionMap *pos_map);
  void InsertKey(const EncodedDocId &enc_doc_id, const PositionMap *pos_map);

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
