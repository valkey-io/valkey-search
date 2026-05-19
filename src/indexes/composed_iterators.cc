/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/indexes/composed_iterators.h"

namespace valkey_search::indexes {

// --- AndIterator ---

AndIterator::AndIterator(
    std::vector<std::unique_ptr<EntriesFetcherIteratorBase>> children,
    std::vector<std::unique_ptr<EntriesFetcherIteratorBase>> excluded)
    : children_(std::move(children)), excluded_(std::move(excluded)) {
  if (!children_.empty()) {
    FindCommonKey();
  }
}

bool AndIterator::Done() const { return !current_key_; }

const InternedStringPtr& AndIterator::operator*() const {
  return current_key_;
}

void AndIterator::Next() {
  if (!current_key_) return;
  // Advance all children past current key.
  for (auto& child : children_) {
    child->Next();
  }
  FindCommonKey();
}

bool AndIterator::SeekForwardKey(const InternedStringPtr& target) {
  if (!current_key_) return false;
  if (current_key_ >= target) return true;
  for (auto& child : children_) {
    if (!child->SeekForwardKey(target)) {
      current_key_ = {};
      return false;
    }
  }
  FindCommonKey();
  return !Done();
}

bool AndIterator::FindCommonKey() {
  // Find intersection: all children must be on the same key.
  while (true) {
    // Check if any child is exhausted.
    for (auto& child : children_) {
      if (child->Done()) {
        current_key_ = {};
        return false;
      }
    }
    // Find max key across all children.
    InternedStringPtr max_key = **children_[0];
    for (size_t i = 1; i < children_.size(); ++i) {
      if (**children_[i] > max_key) {
        max_key = **children_[i];
      }
    }
    // Seek all children to max_key.
    bool all_match = true;
    for (auto& child : children_) {
      if (**child < max_key) {
        if (!child->SeekForwardKey(max_key)) {
          current_key_ = {};
          return false;
        }
        if (**child != max_key) {
          all_match = false;
        }
      }
    }
    if (all_match) {
      // All on same key — check exclusions.
      if (!IsExcluded(max_key)) {
        current_key_ = max_key;
        return true;
      }
      // Excluded — advance all past this key.
      for (auto& child : children_) {
        child->Next();
      }
    }
    // Not all match yet — loop again with new max.
  }
}

bool AndIterator::IsExcluded(const InternedStringPtr& key) {
  for (auto& ex : excluded_) {
    if (!ex->Done()) {
      ex->SeekForwardKey(key);
      if (!ex->Done() && **ex == key) {
        return true;
      }
    }
  }
  return false;
}

// --- OrIterator ---

OrIterator::OrIterator(
    std::vector<std::unique_ptr<EntriesFetcherIteratorBase>> children)
    : children_(std::move(children)) {
  for (size_t i = 0; i < children_.size(); ++i) {
    if (!children_[i]->Done()) {
      heap_.push_back_unsorted(HeapEntry{i, &children_});
    }
  }
  heap_.heapify();
  Advance();
}

bool OrIterator::Done() const { return !current_key_; }

const InternedStringPtr& OrIterator::operator*() const {
  return current_key_;
}

void OrIterator::Next() {
  if (!current_key_) return;
  // Advance all children that are on current_key_.
  while (!heap_.empty()) {
    const auto& top = heap_.min();
    if (**children_[top.idx] == current_key_) {
      HeapEntry entry = top;
      heap_.pop_min();
      children_[entry.idx]->Next();
      if (!children_[entry.idx]->Done()) {
        heap_.emplace(HeapEntry{entry.idx, &children_});
      }
    } else {
      break;
    }
  }
  Advance();
}

bool OrIterator::SeekForwardKey(const InternedStringPtr& target) {
  if (!current_key_) return false;
  if (current_key_ >= target) return true;
  heap_.clear();
  for (size_t i = 0; i < children_.size(); ++i) {
    if (!children_[i]->Done()) {
      children_[i]->SeekForwardKey(target);
      if (!children_[i]->Done()) {
        heap_.push_back_unsorted(HeapEntry{i, &children_});
      }
    }
  }
  heap_.heapify();
  Advance();
  return !Done();
}

void OrIterator::Advance() {
  if (heap_.empty()) {
    current_key_ = {};
    return;
  }
  current_key_ = **children_[heap_.min().idx];
}

}  // namespace valkey_search::indexes
