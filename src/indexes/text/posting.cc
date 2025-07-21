/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/posting.h"

#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <vector>

namespace valkey_search::text {

// Internal FieldMask classes - not part of external interface
// Field mask interface optimized for different field counts
class FieldMask {
public:
  static std::unique_ptr<FieldMask> Create(size_t num_fields);
  virtual ~FieldMask() = default;
  virtual void SetField(size_t field_index) = 0;
  virtual void ClearField(size_t field_index) = 0;
  virtual bool HasField(size_t field_index) const = 0;
  virtual void SetAllFields() = 0;
  virtual void ClearAllFields() = 0;
  virtual size_t CountSetFields() const = 0;
  virtual uint64_t AsUint64() const = 0;
  virtual std::unique_ptr<FieldMask> Clone() const = 0;
  virtual size_t MaxFields() const = 0;
};

// Template implementation for field mask with optimized storage
template<typename MaskType, size_t MAX_FIELDS>
class FieldMaskImpl : public FieldMask {
public:
  explicit FieldMaskImpl(size_t num_fields = MAX_FIELDS);
  void SetField(size_t field_index) override;
  void ClearField(size_t field_index) override;
  bool HasField(size_t field_index) const override;
  void SetAllFields() override;
  void ClearAllFields() override;
  size_t CountSetFields() const override;
  uint64_t AsUint64() const override;
  std::unique_ptr<FieldMask> Clone() const override;
  size_t MaxFields() const override { return MAX_FIELDS; }
private:
  MaskType mask_;
  size_t num_fields_;
};

// Empty placeholder type that takes no space
struct EmptyFieldMask {};

// Optimized implementations for different field counts
using SingleFieldMask = FieldMaskImpl<EmptyFieldMask, 1>;
using ByteFieldMask = FieldMaskImpl<uint8_t, 8>;
using Uint64FieldMask = FieldMaskImpl<uint64_t, 64>;

// Field Mask Implementation
  
// Factory method to create optimal field mask based on field count
std::unique_ptr<FieldMask> FieldMask::Create(size_t num_fields) {
  if (num_fields == 0) {
    throw std::invalid_argument("num_fields must be greater than 0");
  }
  
  // Select most memory-efficient implementation
  if (num_fields <= 1) {
    return std::make_unique<SingleFieldMask>();  // EmptyFieldMask (no storage)
  } else if (num_fields <= 8) {
    return std::make_unique<ByteFieldMask>(num_fields);  // uint8_t (1 byte)
  } else if (num_fields <= 64) {
    return std::make_unique<Uint64FieldMask>(num_fields);  // uint64_t (8 bytes)
  } else {
    throw std::invalid_argument("Too many text fields (max 64 supported)");
  }
}

// Initialize field mask with specified field count
template<typename MaskType, size_t MAX_FIELDS>
FieldMaskImpl<MaskType, MAX_FIELDS>::FieldMaskImpl(size_t num_fields) 
    : num_fields_(num_fields) {
  if (num_fields > MAX_FIELDS) {
    throw std::invalid_argument("Field count exceeds maximum for this mask type");
  }
  
  if constexpr (!std::is_same_v<MaskType, EmptyFieldMask>) {
    mask_ = MaskType{};
  }
}

// Set a specific field bit to true
template<typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::SetField(size_t field_index) {
  if (field_index >= num_fields_) {
    throw std::out_of_range("Field index out of range");
  }
  
  if constexpr (!std::is_same_v<MaskType, EmptyFieldMask>) {
    mask_ |= (MaskType(1) << field_index);
  }
}

// Clear a specific field bit
template<typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::ClearField(size_t field_index) {
  if (field_index >= num_fields_) {
    throw std::out_of_range("Field index out of range");
  }
  
  if constexpr (!std::is_same_v<MaskType, EmptyFieldMask>) {
    mask_ &= ~(MaskType(1) << field_index);
  }
}

// Check if a specific field bit is set
template<typename MaskType, size_t MAX_FIELDS>
bool FieldMaskImpl<MaskType, MAX_FIELDS>::HasField(size_t field_index) const {
  if (field_index >= num_fields_) {
    return false;
  }
  
  if constexpr (std::is_same_v<MaskType, EmptyFieldMask>) {
    return true;  // Single field case: presence of object implies field is set
  } else {
    return (mask_ & (MaskType(1) << field_index)) != 0;
  }
}

// Set all field bits to true
template<typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::SetAllFields() {
  if constexpr (std::is_same_v<MaskType, EmptyFieldMask>) {
    // No-op: field is already implicitly set by object presence
  } else if (num_fields_ == MAX_FIELDS && MAX_FIELDS == 64) {
    mask_ = ~MaskType(0);  // Special case: avoid undefined behavior for 64-bit shift
  } else {
    mask_ = (MaskType(1) << num_fields_) - 1;  // Set num_fields_ bits
  }
}

// Clear all field bits
template<typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::ClearAllFields() {
  if constexpr (!std::is_same_v<MaskType, EmptyFieldMask>) {
    mask_ = MaskType{};  // Zero-initialize
  }
}

// Count number of set field bits
template<typename MaskType, size_t MAX_FIELDS>
size_t FieldMaskImpl<MaskType, MAX_FIELDS>::CountSetFields() const {
  if constexpr (std::is_same_v<MaskType, EmptyFieldMask>) {
    return 1;  // Single field case: presence of object implies field is set
  } else if constexpr (std::is_same_v<MaskType, uint8_t>) {
    return __builtin_popcount(static_cast<unsigned int>(mask_));  // Count bits in uint8_t
  } else if constexpr (std::is_same_v<MaskType, uint64_t>) {
    return __builtin_popcountll(mask_);  // Count bits in uint64_t
  } else {
    throw std::invalid_argument("Unsupported mask type for CountSetFields");
  }
}

// Convert field mask to standard uint64_t representation
template<typename MaskType, size_t MAX_FIELDS>
uint64_t FieldMaskImpl<MaskType, MAX_FIELDS>::AsUint64() const {
  if constexpr (std::is_same_v<MaskType, EmptyFieldMask>) {
    return 1ULL;  // Single field case: presence of object implies field is set
  } else {
    return static_cast<uint64_t>(mask_);  // Cast integer types to uint64_t
  }
}

// Create deep copy of field mask
template<typename MaskType, size_t MAX_FIELDS>
std::unique_ptr<FieldMask> FieldMaskImpl<MaskType, MAX_FIELDS>::Clone() const {
  auto clone = std::make_unique<FieldMaskImpl<MaskType, MAX_FIELDS>>(num_fields_);
  if constexpr (!std::is_same_v<MaskType, EmptyFieldMask>) {
    clone->mask_ = mask_;
  }
  return clone;
}

// Explicit template instantiations
template class FieldMaskImpl<EmptyFieldMask, 1>;
template class FieldMaskImpl<uint8_t, 8>;
template class FieldMaskImpl<uint64_t, 64>;

// Basic Postings Object Implementation

// Position map type alias - maps position to optimized FieldMask objects
using PositionMap = std::map<Position, std::unique_ptr<FieldMask>>;

class Postings::Impl {
public:
  bool save_positions_;
  size_t num_text_fields_;
  std::map<Key, PositionMap> key_to_positions_;
  
  Impl(bool save_positions, size_t num_text_fields)
      : save_positions_(save_positions), num_text_fields_(num_text_fields) {}
  
  Impl(const Impl& other) 
      : save_positions_(other.save_positions_), num_text_fields_(other.num_text_fields_) {
    for (const auto& [key, pos_map] : other.key_to_positions_) {
      PositionMap cloned_pos_map;
      for (const auto& [pos, field_mask] : pos_map) {
        cloned_pos_map[pos] = field_mask->Clone();
      }
      key_to_positions_[key] = std::move(cloned_pos_map);
    }
  }
  
  Impl& operator=(const Impl& other) {
    if (this != &other) {
      save_positions_ = other.save_positions_;
      num_text_fields_ = other.num_text_fields_;
      key_to_positions_.clear();
      
      for (const auto& [key, pos_map] : other.key_to_positions_) {
        PositionMap cloned_pos_map;
        for (const auto& [pos, field_mask] : pos_map) {
          cloned_pos_map[pos] = field_mask->Clone();
        }
        key_to_positions_[key] = std::move(cloned_pos_map);
      }
    }
    return *this;
  }
};

Postings::Postings(bool save_positions, size_t num_text_fields) 
    : impl_(std::make_unique<Impl>(save_positions, num_text_fields)) {
}

// Automatic cleanup via unique_ptr
Postings::~Postings() = default;

// Deep copy of all posting data
Postings::Postings(const Postings& other) 
    : impl_(std::make_unique<Impl>(*other.impl_)) {}

// Safe replacement with deep copy
Postings& Postings::operator=(const Postings& other) {
  if (this != &other) {
    impl_ = std::make_unique<Impl>(*other.impl_);
  }
  return *this;
}

// Check if posting list contains any documents
bool Postings::IsEmpty() const {
  return impl_->key_to_positions_.empty();
}

// Insert a posting entry for a key and field
void Postings::InsertPosting(const Key& key, size_t field_index, Position position) {
  if (field_index >= impl_->num_text_fields_) {
    throw std::out_of_range("Field index out of range");
  }
  
  Position effective_position;
  
  if (impl_->save_positions_) {
    // In positional mode, position must be explicitly provided
    if (position == UINT32_MAX) {
      throw std::invalid_argument("Position must be provided in positional mode");
    }
    effective_position = position;
  } else {
    // For boolean search mode, always use position 0 regardless of input
    effective_position = 0;
  }
  
  auto& pos_map = impl_->key_to_positions_[key];
  
  // Check if position already exists
  auto it = pos_map.find(effective_position);
  
  if (it != pos_map.end()) {
    // Position exists - add field to existing FieldMask object
    it->second->SetField(field_index);
  } else {
    // New position - create entry with this field
    auto field_mask = FieldMask::Create(impl_->num_text_fields_);
    field_mask->SetField(field_index);
    pos_map[effective_position] = std::move(field_mask);
  }
}


// Remove a document key and all its positions
void Postings::RemoveKey(const Key& key) {
  impl_->key_to_positions_.erase(key);
}

// Get total number of document keys
size_t Postings::GetKeyCount() const {
  return impl_->key_to_positions_.size();
}

// Get total number of position entries across all keys
size_t Postings::GetPostingCount() const {
  size_t total = 0;
  for (const auto& [key, positions] : impl_->key_to_positions_) {
    total += positions.size();
  }
  return total;
}

// Get total term frequency (sum of field occurrences across all positions)
size_t Postings::GetTotalTermFrequency() const {
  size_t total_frequency = 0;
  for (const auto& [key, positions] : impl_->key_to_positions_) {
    for (const auto& [position, field_mask] : positions) {
      // Use efficient bit manipulation to count set fields (much faster!)
      total_frequency += field_mask->CountSetFields();
    }
  }
  return total_frequency;
}

// Defragment posting list
Postings* Postings::Defrag() {
  return this;
}

// Iterators Implementation

// Placeholder for GetKeyIterator - will be implemented when iterators are added
Postings::KeyIterator Postings::GetKeyIterator() const {
  // TODO: Implement when KeyIterator is added
  static KeyIterator dummy;
  return dummy;
}

// Placeholder implementations for KeyIterator
bool Postings::KeyIterator::IsValid() const {
  // TODO: Implement when KeyIterator is added
  return false;
}

void Postings::KeyIterator::NextKey() {
  // TODO: Implement when KeyIterator is added
}

bool Postings::KeyIterator::SkipForwardKey(const Key& key) {
  // TODO: Implement when KeyIterator is added
  return false;
}

const Key& Postings::KeyIterator::GetKey() const {
  // TODO: Implement when KeyIterator is added
  static Key dummy_key;
  return dummy_key;
}

Postings::PositionIterator Postings::KeyIterator::GetPositionIterator() const {
  // TODO: Implement when KeyIterator is added
  static PositionIterator dummy;
  return dummy;
}

// Placeholder implementations for PositionIterator
bool Postings::PositionIterator::IsValid() const {
  // TODO: Implement when PositionIterator is added
  return false;
}

void Postings::PositionIterator::NextPosition() {
  // TODO: Implement when PositionIterator is added
}

bool Postings::PositionIterator::SkipForwardPosition(const Position& position) {
  // TODO: Implement when PositionIterator is added
  return false;
}

const Position& Postings::PositionIterator::GetPosition() const {
  // TODO: Implement when PositionIterator is added
  static Position dummy_position = 0;
  return dummy_position;
}

uint64_t Postings::PositionIterator::GetFieldMask() const {
  // TODO: Implement when PositionIterator is added
  return 0;
}

// Placeholder implementations for Posting struct
const Key& Posting::GetKey() const {
  static Key dummy_key;
  return dummy_key;
}

uint64_t Posting::GetFieldMask() const {
  return 0;
}

uint32_t Posting::GetPosition() const {
  return 0;
}

}
