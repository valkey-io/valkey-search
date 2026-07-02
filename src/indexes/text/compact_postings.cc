/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/indexes/text/compact_postings.h"

#include <cstddef>

#include "src/indexes/text/flat_position_map.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

size_t CompactPostings::GetKeyCount() const {
  if (storage_ == 0) return 0;
  switch (storage_ & kTagMask) {
    case kSingleTag:
      return 1;
    case kSmallVecTag:
      return GetSmallVec()->count;
    case kMapTag:
      return GetMap()->size();
    default:
      return 0;
  }
}

bool CompactPostings::InsertKey(const InternedStringPtr& key,
                                FlatPositionMap* flat_map) {
  if (storage_ == 0) {
    auto* single = new SingleEntry();
    single->entry.key = key;
    single->entry.positions = flat_map;
    SetSingle(single);
    return true;
  }

  switch (storage_ & kTagMask) {
    case kSingleTag: {
      auto* single = GetSingle();
      if (single->entry.key == key) {
        return false;
      }
      auto* vec = new SmallVec();
      if (key < single->entry.key) {
        vec->entries[0].key = key;
        vec->entries[0].positions = flat_map;
        vec->entries[1].key = std::move(single->entry.key);
        vec->entries[1].positions = single->entry.positions;
      } else {
        vec->entries[0].key = std::move(single->entry.key);
        vec->entries[0].positions = single->entry.positions;
        vec->entries[1].key = key;
        vec->entries[1].positions = flat_map;
      }
      vec->count = 2;
      delete single;
      SetSmallVec(vec);
      return true;
    }
    case kSmallVecTag: {
      auto* vec = GetSmallVec();
      size_t pos = SmallVecLowerBound(vec, key);
      if (pos < vec->count && vec->entries[pos].key == key) {
        return false;
      }
      if (vec->count < kSmallVecCap) {
        for (size_t i = vec->count; i > pos; --i) {
          vec->entries[i].key = std::move(vec->entries[i - 1].key);
          vec->entries[i].positions = vec->entries[i - 1].positions;
        }
        vec->entries[pos].key = key;
        vec->entries[pos].positions = flat_map;
        ++vec->count;
        return true;
      }
      PromoteToMap(key, flat_map);
      return true;
    }
    case kMapTag: {
      auto* map = GetMap();
      auto [_, inserted] = map->emplace(key, flat_map);
      return inserted;
    }
    default:
      return false;
  }
}

FlatPositionMap* CompactPostings::RemoveKey(const InternedStringPtr& key) {
  if (storage_ == 0) return nullptr;

  switch (storage_ & kTagMask) {
    case kSingleTag: {
      auto* single = GetSingle();
      if (single->entry.key != key) return nullptr;
      FlatPositionMap* result = single->entry.positions;
      delete single;
      storage_ = 0;
      return result;
    }
    case kSmallVecTag: {
      auto* vec = GetSmallVec();
      size_t pos = SmallVecLowerBound(vec, key);
      if (pos >= vec->count || vec->entries[pos].key != key) return nullptr;
      FlatPositionMap* result = vec->entries[pos].positions;
      for (size_t i = pos; i < vec->count - 1; ++i) {
        vec->entries[i].key = std::move(vec->entries[i + 1].key);
        vec->entries[i].positions = vec->entries[i + 1].positions;
      }
      vec->entries[vec->count - 1].key = InternedStringPtr();
      vec->entries[vec->count - 1].positions = nullptr;
      --vec->count;
      if (vec->count == 1) {
        auto* single = new SingleEntry();
        single->entry.key = std::move(vec->entries[0].key);
        single->entry.positions = vec->entries[0].positions;
        delete vec;
        SetSingle(single);
      } else if (vec->count == 0) {
        delete vec;
        storage_ = 0;
      }
      return result;
    }
    case kMapTag: {
      auto* map = GetMap();
      auto it = map->find(key);
      if (it == map->end()) return nullptr;
      FlatPositionMap* result = it->second;
      map->erase(it);
      if (map->size() <= kSmallVecCap) {
        DemoteToSmallVec();
      }
      return result;
    }
    default:
      return nullptr;
  }
}

FlatPositionMap* CompactPostings::Find(const InternedStringPtr& key) const {
  if (storage_ == 0) return nullptr;

  switch (storage_ & kTagMask) {
    case kSingleTag: {
      auto* single = GetSingle();
      return single->entry.key == key ? single->entry.positions : nullptr;
    }
    case kSmallVecTag: {
      auto* vec = GetSmallVec();
      size_t pos = SmallVecLowerBound(vec, key);
      if (pos < vec->count && vec->entries[pos].key == key) {
        return vec->entries[pos].positions;
      }
      return nullptr;
    }
    case kMapTag: {
      auto* map = GetMap();
      auto it = map->find(key);
      return it != map->end() ? it->second : nullptr;
    }
    default:
      return nullptr;
  }
}

size_t CompactPostings::SmallVecLowerBound(const SmallVec* vec,
                                           const InternedStringPtr& key) {
  size_t lo = 0, hi = vec->count;
  while (lo < hi) {
    size_t mid = lo + (hi - lo) / 2;
    if (vec->entries[mid].key < key) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

void CompactPostings::PromoteToMap(const InternedStringPtr& key,
                                   FlatPositionMap* flat_map) {
  auto* vec = GetSmallVec();
  auto* map = new MapType();
  for (size_t i = 0; i < vec->count; ++i) {
    map->emplace(std::move(vec->entries[i].key), vec->entries[i].positions);
  }
  map->emplace(key, flat_map);
  delete vec;
  SetMap(map);
}

void CompactPostings::DemoteToSmallVec() {
  auto* map = GetMap();
  auto* vec = new SmallVec();
  size_t i = 0;
  for (auto& [k, v] : *map) {
    vec->entries[i].key = k;
    vec->entries[i].positions = v;
    ++i;
    if (i >= kSmallVecCap) break;
  }
  vec->count = i;
  delete map;
  SetSmallVec(vec);
}

void CompactPostings::Destroy() {
  if (storage_ == 0) return;

  switch (storage_ & kTagMask) {
    case kSingleTag: {
      auto* single = GetSingle();
      if (single->entry.positions) {
        FlatPositionMap::Destroy(single->entry.positions);
      }
      delete single;
      break;
    }
    case kSmallVecTag: {
      auto* vec = GetSmallVec();
      for (size_t i = 0; i < vec->count; ++i) {
        if (vec->entries[i].positions) {
          FlatPositionMap::Destroy(vec->entries[i].positions);
        }
      }
      delete vec;
      break;
    }
    case kMapTag: {
      auto* map = GetMap();
      for (auto& [_, flat_map] : *map) {
        if (flat_map) {
          FlatPositionMap::Destroy(flat_map);
        }
      }
      delete map;
      break;
    }
  }
  storage_ = 0;
}

// --- KeyIterator ---

CompactPostings::KeyIterator CompactPostings::GetKeyIterator() const {
  KeyIterator it;
  if (storage_ == 0) {
    it.mode_ = KeyIterator::Mode::kEmpty;
    return it;
  }

  switch (storage_ & kTagMask) {
    case kSingleTag: {
      it.mode_ = KeyIterator::Mode::kSingle;
      it.single_entry_ = &GetSingle()->entry;
      it.single_consumed_ = false;
      break;
    }
    case kSmallVecTag: {
      auto* vec = GetSmallVec();
      it.mode_ = KeyIterator::Mode::kSmallVec;
      it.vec_data_ = vec->entries;
      it.vec_count_ = vec->count;
      it.vec_idx_ = 0;
      break;
    }
    case kMapTag: {
      auto* map = GetMap();
      it.mode_ = KeyIterator::Mode::kMap;
      it.map_ = map;
      it.map_iter_ = map->begin();
      it.map_end_ = map->end();
      break;
    }
    default:
      it.mode_ = KeyIterator::Mode::kEmpty;
      break;
  }
  return it;
}

bool CompactPostings::KeyIterator::IsValid() const {
  switch (mode_) {
    case Mode::kEmpty:
      return false;
    case Mode::kSingle:
      return !single_consumed_;
    case Mode::kSmallVec:
      return vec_idx_ < vec_count_;
    case Mode::kMap:
      return map_iter_ != map_end_;
  }
  return false;
}

void CompactPostings::KeyIterator::Next() {
  switch (mode_) {
    case Mode::kEmpty:
      break;
    case Mode::kSingle:
      single_consumed_ = true;
      break;
    case Mode::kSmallVec:
      if (vec_idx_ < vec_count_) ++vec_idx_;
      break;
    case Mode::kMap:
      if (map_iter_ != map_end_) ++map_iter_;
      break;
  }
}

bool CompactPostings::KeyIterator::SkipForward(const InternedStringPtr& key) {
  switch (mode_) {
    case Mode::kEmpty:
      return false;
    case Mode::kSingle:
      if (single_consumed_) return false;
      if (single_entry_->key < key) {
        single_consumed_ = true;
        return false;
      }
      return single_entry_->key == key;
    case Mode::kSmallVec: {
      while (vec_idx_ < vec_count_ && vec_data_[vec_idx_].key < key) {
        ++vec_idx_;
      }
      return vec_idx_ < vec_count_ && vec_data_[vec_idx_].key == key;
    }
    case Mode::kMap:
      map_iter_ = map_->lower_bound(key);
      return map_iter_ != map_end_ && map_iter_->first == key;
  }
  return false;
}

const InternedStringPtr& CompactPostings::KeyIterator::GetKey() const {
  switch (mode_) {
    case Mode::kSingle:
      return single_entry_->key;
    case Mode::kSmallVec:
      return vec_data_[vec_idx_].key;
    case Mode::kMap:
      return map_iter_->first;
    default:
      static InternedStringPtr empty;
      return empty;
  }
}

FlatPositionMap* CompactPostings::KeyIterator::GetPositions() const {
  switch (mode_) {
    case Mode::kSingle:
      return single_entry_->positions;
    case Mode::kSmallVec:
      return vec_data_[vec_idx_].positions;
    case Mode::kMap:
      return map_iter_->second;
    default:
      return nullptr;
  }
}

}  // namespace valkey_search::indexes::text
