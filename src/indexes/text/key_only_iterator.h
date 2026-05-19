/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_KEY_ONLY_ITERATOR_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_KEY_ONLY_ITERATOR_H_

#include <memory>

#include "src/indexes/index_base.h"
#include "src/indexes/text/text_iterator.h"

namespace valkey_search::indexes::text {

// Adapts an EntriesFetcherIteratorBase (tag/numeric) into a TextIterator.
// Key-level only — positions are always "done" (no positional data).
class KeyOnlyTextIterator : public TextIterator {
 public:
  KeyOnlyTextIterator(std::unique_ptr<indexes::EntriesFetcherIteratorBase> iter,
                      std::unique_ptr<indexes::EntriesFetcherBase> fetcher)
      : iter_(std::move(iter)), fetcher_(std::move(fetcher)) {}

  FieldMaskPredicate QueryFieldMask() const override { return ~0ULL; }

  bool DoneKeys() const override { return iter_->Done(); }
  const Key& CurrentKey() const override { return **iter_; }
  bool NextKey() override {
    iter_->Next();
    return !iter_->Done();
  }
  bool SeekForwardKey(const Key& target_key) override {
    return iter_->SeekForwardKey(target_key);
  }

  // No positions — always done.
  bool DonePositions() const override { return true; }
  const PositionRange& CurrentPosition() const override {
    static PositionRange dummy{0, 0};
    return dummy;
  }
  bool NextPosition() override { return false; }
  bool SeekForwardPosition(Position) override { return false; }
  FieldMaskPredicate CurrentFieldMask() const override { return ~0ULL; }
  bool HasPositions() const override { return false; }
  bool IsIteratorValid() const override { return !iter_->Done(); }

 private:
  std::unique_ptr<indexes::EntriesFetcherIteratorBase> iter_;
  std::unique_ptr<indexes::EntriesFetcherBase> fetcher_;  // keeps data alive
};

// Emits keys from source that are NOT present in excluded.
// Both must iterate in the same sorted order.
class ExcludeIterator : public TextIterator {
 public:
  ExcludeIterator(std::unique_ptr<TextIterator> source,
                  std::unique_ptr<TextIterator> excluded)
      : source_(std::move(source)), excluded_(std::move(excluded)) {
    SkipExcluded();
  }

  FieldMaskPredicate QueryFieldMask() const override {
    return source_->QueryFieldMask();
  }
  bool DoneKeys() const override { return source_->DoneKeys(); }
  const Key& CurrentKey() const override { return source_->CurrentKey(); }
  bool NextKey() override {
    source_->NextKey();
    SkipExcluded();
    return !source_->DoneKeys();
  }
  bool SeekForwardKey(const Key& target_key) override {
    if (!source_->SeekForwardKey(target_key)) return false;
    SkipExcluded();
    return !source_->DoneKeys();
  }
  bool DonePositions() const override { return true; }
  const PositionRange& CurrentPosition() const override {
    static PositionRange dummy{0, 0};
    return dummy;
  }
  bool NextPosition() override { return false; }
  bool SeekForwardPosition(Position) override { return false; }
  FieldMaskPredicate CurrentFieldMask() const override { return ~0ULL; }
  bool HasPositions() const override { return false; }
  bool IsIteratorValid() const override { return !source_->DoneKeys(); }

 private:
  std::unique_ptr<TextIterator> source_;
  std::unique_ptr<TextIterator> excluded_;

  void SkipExcluded() {
    while (!source_->DoneKeys()) {
      if (excluded_->DoneKeys()) return;  // nothing left to exclude
      // Check if current source key is excluded.
      if (!excluded_->SeekForwardKey(source_->CurrentKey())) return;
      if (excluded_->CurrentKey() == source_->CurrentKey()) {
        source_->NextKey();  // skip this key
      } else {
        return;  // current key not excluded
      }
    }
  }
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_KEY_ONLY_ITERATOR_H_
