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

namespace valkey_search::text {

// Field Mask Implementation
  
// Factory method to create optimal field mask based on field count
std::unique_ptr<FieldMask> FieldMask::Create(size_t num_fields) {
  if (num_fields == 0) {
    throw std::invalid_argument("num_fields must be greater than 0");
  }
  
  // Select most memory-efficient implementation
  if (num_fields <= 1) {
    return std::make_unique<SingleFieldMask>();  // bool (1 byte)
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
    : mask_(MaskType{}), num_fields_(num_fields) {
  if (num_fields > MAX_FIELDS) {
    throw std::invalid_argument("Field count exceeds maximum for this mask type");
  }
}

// Set a specific field bit to true
template<typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::SetField(size_t field_index) {
  if (field_index >= num_fields_) {
    throw std::out_of_range("Field index out of range");
  }
  
  if constexpr (std::is_same_v<MaskType, bool>) {
    mask_ = true;
  } else {
    mask_ |= (MaskType(1) << field_index);
  }
}

// Clear a specific field bit
template<typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::ClearField(size_t field_index) {
  if (field_index >= num_fields_) {
    throw std::out_of_range("Field index out of range");
  }
  
  if constexpr (std::is_same_v<MaskType, bool>) {
    mask_ = false;
  } else {
    mask_ &= ~(MaskType(1) << field_index);
  }
}

// Check if a specific field bit is set
template<typename MaskType, size_t MAX_FIELDS>
bool FieldMaskImpl<MaskType, MAX_FIELDS>::HasField(size_t field_index) const {
  if (field_index >= num_fields_) {
    return false;
  }
  
  if constexpr (std::is_same_v<MaskType, bool>) {
    return mask_;
  } else {
    return (mask_ & (MaskType(1) << field_index)) != 0;
  }
}

// Set all field bits to true
template<typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::SetAllFields() {
  if constexpr (std::is_same_v<MaskType, bool>) {
    mask_ = true;
  } else {
    if (num_fields_ == MAX_FIELDS && MAX_FIELDS == 64) {
      mask_ = ~MaskType(0);  // Special case: avoid undefined behavior for 64-bit shift
    } else {
      mask_ = (MaskType(1) << num_fields_) - 1;  // Set num_fields_ bits
    }
  }
}

// Clear all field bits
template<typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::ClearAllFields() {
  mask_ = MaskType{};  // Zero-initialize
}

// Count number of set field bits
template<typename MaskType, size_t MAX_FIELDS>
size_t FieldMaskImpl<MaskType, MAX_FIELDS>::CountSetFields() const {
  if constexpr (std::is_same_v<MaskType, bool>) {
    return mask_ ? 1 : 0;  // Single field case: either 0 or 1
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
  if constexpr (std::is_same_v<MaskType, bool>) {
    return mask_ ? 1ULL : 0ULL;  // Convert bool to 0/1
  } else {
    return static_cast<uint64_t>(mask_);  // Cast integer types to uint64_t
  }
}

// Create deep copy of field mask
template<typename MaskType, size_t MAX_FIELDS>
std::unique_ptr<FieldMask> FieldMaskImpl<MaskType, MAX_FIELDS>::Clone() const {
  auto clone = std::make_unique<FieldMaskImpl<MaskType, MAX_FIELDS>>(num_fields_);
  clone->mask_ = mask_;
  return clone;
}

// Explicit template instantiations
template class FieldMaskImpl<bool, 1>;
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

// Add document key for boolean search (no position tracking)
void Postings::SetKey(const Key& key) {
  // This function is designed for boolean search (save_positions=false mode)
  if (impl_->save_positions_) {
    throw std::invalid_argument("SetKey() is only for boolean search mode (save_positions=false). Use AddPositionForField() instead.");
  }
  
  // Just record document presence with assumed position 0 and empty field mask
  auto& pos_map = impl_->key_to_positions_[key];
  pos_map.clear();
  
  // Store single dummy position with empty field mask - now creates actual FieldMask object!
  pos_map[0] = FieldMask::Create(impl_->num_text_fields_);  // Position 0, no fields set
}

// Add term occurrence at specific position and field
void Postings::AddPositionForField(const Key& key, Position position, size_t field_index) {
  if (field_index >= impl_->num_text_fields_) {
    throw std::out_of_range("Field index out of range");
  }
  
  auto& pos_map = impl_->key_to_positions_[key];
  
  // Check if position already exists
  auto it = pos_map.find(position);
  
  if (it != pos_map.end()) {
    // Position exists - directly add field to existing FieldMask object (much cleaner!)
    it->second->SetField(field_index);
  } else {
    // New position - create entry with only this field
    auto field_mask = FieldMask::Create(impl_->num_text_fields_);
    field_mask->SetField(field_index);
    pos_map[position] = std::move(field_mask);
  }
}

// Replace all positions for a key with new position/field pairs
void Postings::SetKeyWithFieldPositions(const Key& key, std::span<std::pair<Position, size_t>> position_field_pairs) {
  auto& pos_map = impl_->key_to_positions_[key];
  pos_map.clear();  // Replace existing positions
  
  // Group positions by position value
  std::map<Position, std::unique_ptr<FieldMask>> position_masks;
  
  for (const auto& [pos, field_idx] : position_field_pairs) {
    if (field_idx >= impl_->num_text_fields_) {
      throw std::out_of_range("Field index out of range");
    }
    
    if (position_masks.find(pos) == position_masks.end()) {
      position_masks[pos] = FieldMask::Create(impl_->num_text_fields_);
    }
    position_masks[pos]->SetField(field_idx);
  }
  
  // Move FieldMask objects to PositionMap (automatically maintains sorted order)
  for (auto& [pos, mask] : position_masks) {
    pos_map[pos] = std::move(mask);
  }
}

// Merge new position/field pairs with existing positions for a key
void Postings::UpdateKeyWithFieldPositions(const Key& key, std::span<std::pair<Position, size_t>> position_field_pairs) {
  auto& pos_map = impl_->key_to_positions_[key];
  // NO pos_map.clear() - preserve existing positions!
  
  for (const auto& [pos, field_idx] : position_field_pairs) {
    if (field_idx >= impl_->num_text_fields_) {
      throw std::out_of_range("Field index out of range");
    }
    
    // Check if position already exists
    auto it = pos_map.find(pos);
    
    if (it != pos_map.end()) {
      // Position exists - add field to existing FieldMask
      it->second->SetField(field_idx);
    } else {
      // New position - create entry with only this field
      auto field_mask = FieldMask::Create(impl_->num_text_fields_);
      field_mask->SetField(field_idx);
      pos_map[pos] = std::move(field_mask);
    }
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
