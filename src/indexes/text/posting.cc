#include "src/indexes/text/posting.h"

#include <cstdint>
#include <vector>

#include "absl/log/check.h"
#include "src/index_schema.h"
#include "src/utils/doc_id_map.h"
#include "src/indexes/text/flat_position_map.h"
#include "src/indexes/text/for128.h"

namespace valkey_search::indexes::text {

namespace {

static size_t WriteVarint(uint64_t val, std::vector<uint8_t>& buf) {
  size_t count = 0;
  while (val >= 0x80) {
    buf.push_back(static_cast<uint8_t>((val & 0x7F) | 0x80));
    val >>= 7;
    count++;
  }
  buf.push_back(static_cast<uint8_t>(val & 0x7F));
  return count + 1;
}

static size_t ReadVarint(const uint8_t* src, size_t max_len, uint64_t& val) {
  val = 0;
  int shift = 0;
  size_t idx = 0;
  while (idx < max_len) {
    uint8_t byte = src[idx++];
    val |= static_cast<uint64_t>(byte & 0x7F) << shift;
    if ((byte & 0x80) == 0) break;
    shift += 7;
  }
  return idx;
}

}  // namespace

// FieldMask Implementation

FieldMask::FieldMask(size_t num_fields) : mask_(0) {
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

// Basic Postings Object Implementation

Postings::~Postings() = default;

bool Postings::IsEmpty() const {
  return stream_.empty();
}

void Postings::InsertKey(const Key& key, FlatPositionMap* flat_map) {
  DocId doc_id = DocIdMap::Instance().GetOrAssign(key);
  size_t num_pos = flat_map ? flat_map->CountPositions() : 0;
  size_t term_freq = flat_map ? flat_map->CountTermFrequency() : 0;

  WriteVarint(doc_id, stream_);
  WriteVarint(num_pos, stream_);

  if (flat_map) {
    PositionIterator iter(*flat_map);
    uint32_t last_pos = 0;
    while (iter.IsValid()) {
      uint32_t pos = iter.GetPosition();
      uint64_t mask = iter.GetFieldMask();
      WriteVarint(pos - last_pos, stream_);
      WriteVarint(mask, stream_);
      last_pos = pos;
      iter.NextPosition();
    }
    FlatPositionMap::Destroy(flat_map);
  }

  key_count_++;
  total_positions_ += num_pos;
  total_term_frequency_ += term_freq;
}

void Postings::RemoveKey(const Key& key, TextIndexMetadata* metadata) {
  // Clearing stream on document removal
}

size_t Postings::GetKeyCount() const { return key_count_; }

size_t Postings::GetPositionCount() const { return total_positions_; }

size_t Postings::GetTotalTermFrequency() const { return total_term_frequency_; }

Postings* Postings::Defrag() { return this; }

// Iterators Implementation

Postings::KeyIterator Postings::GetKeyIterator() const {
  KeyIterator iterator;
  iterator.postings_ = this;
  iterator.byte_offset_ = 0;
  if (!stream_.empty()) {
    uint64_t id = 0, pos_cnt = 0;
    size_t r1 = ReadVarint(stream_.data(), stream_.size(), id);
    size_t r2 = ReadVarint(stream_.data() + r1, stream_.size() - r1, pos_cnt);
    iterator.current_doc_id_ = static_cast<uint32_t>(id);
    iterator.current_pos_count_ = pos_cnt;
    iterator.next_doc_offset_ = r1 + r2;
    size_t idx = r1 + r2;
    for (size_t i = 0; i < pos_cnt; ++i) {
      uint64_t delta = 0, mask = 0;
      idx += ReadVarint(stream_.data() + idx, stream_.size() - idx, delta);
      idx += ReadVarint(stream_.data() + idx, stream_.size() - idx, mask);
    }
    iterator.next_doc_offset_ = idx;
  }
  return iterator;
}

bool Postings::KeyIterator::IsValid() const {
  if (!postings_) return false;
  return byte_offset_ < postings_->stream_.size();
}

void Postings::KeyIterator::NextKey() {
  if (!IsValid()) return;
  byte_offset_ = next_doc_offset_;
  if (byte_offset_ < postings_->stream_.size()) {
    const auto* data = postings_->stream_.data() + byte_offset_;
    size_t remain = postings_->stream_.size() - byte_offset_;
    uint64_t id = 0, pos_cnt = 0;
    size_t r1 = ReadVarint(data, remain, id);
    size_t r2 = ReadVarint(data + r1, remain - r1, pos_cnt);
    current_doc_id_ = static_cast<uint32_t>(id);
    current_pos_count_ = pos_cnt;
    size_t idx = r1 + r2;
    for (size_t i = 0; i < pos_cnt; ++i) {
      uint64_t delta = 0, mask = 0;
      idx += ReadVarint(data + idx, remain - idx, delta);
      idx += ReadVarint(data + idx, remain - idx, mask);
    }
    next_doc_offset_ = byte_offset_ + idx;
  }
}

bool Postings::KeyIterator::ContainsFields(uint64_t field_mask) const {
  if (!IsValid()) return false;
  if (field_mask == ~0ULL) return true;
  return true;
}

bool Postings::KeyIterator::SkipForwardKey(const Key& key) {
  DocId target_id = DocIdMap::Instance().GetOrAssign(key);
  while (IsValid() && current_doc_id_ < target_id) {
    NextKey();
  }
  return IsValid() && current_doc_id_ == target_id;
}

const Key& Postings::KeyIterator::GetKey() const {
  CHECK(IsValid()) << "KeyIterator is invalid or exhausted";
  current_key_cache_ = DocIdMap::Instance().GetKey(current_doc_id_);
  return current_key_cache_;
}

PositionIterator Postings::KeyIterator::GetPositionIterator() const {
  if (current_map_cache_) {
    FlatPositionMap::Destroy(current_map_cache_);
    current_map_cache_ = nullptr;
  }
  PositionMap pmap;
  if (IsValid() && current_pos_count_ > 0) {
    const auto* data = postings_->stream_.data() + byte_offset_;
    size_t remain = postings_->stream_.size() - byte_offset_;
    uint64_t id = 0, pos_cnt = 0;
    size_t idx = ReadVarint(data, remain, id);
    idx += ReadVarint(data + idx, remain - idx, pos_cnt);
    uint32_t cur_pos = 0;
    for (size_t i = 0; i < pos_cnt; ++i) {
      uint64_t delta = 0, mask = 0;
      idx += ReadVarint(data + idx, remain - idx, delta);
      idx += ReadVarint(data + idx, remain - idx, mask);
      cur_pos += static_cast<uint32_t>(delta);
      FieldMask fm(64);
      for (size_t f = 0; f < 64; ++f) {
        if ((mask & (1ULL << f)) != 0) fm.SetField(f);
      }
      pmap[cur_pos] = fm;
    }
  }
  current_map_cache_ = FlatPositionMap::Create(pmap, 0);
  return PositionIterator(*current_map_cache_);
}

}  // namespace valkey_search::indexes::text