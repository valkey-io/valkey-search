/*
 * Copyright (c) 2025, ValkeySearch contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_H_

// Commenting out because this file does not compile correctly yet.
// #include "src/indexes/text/text.h"

namespace valkey_search::indexes {

class Text : public IndexBase {
 public:
  explicit Text(const data_model::TextIndex& text_index_proto);
  absl::StatusOr<bool> AddRecord(const InternedStringPtr& key,
                                 absl::string_view data) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  absl::StatusOr<bool> RemoveRecord(
      const InternedStringPtr& key,
      DeletionType deletion_type = DeletionType::kNone) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  absl::StatusOr<bool> ModifyRecord(const InternedStringPtr& key,
                                    absl::string_view data) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  int RespondWithInfo(RedisModuleCtx* ctx) const override;
  bool IsTracked(const InternedStringPtr& key) const override;
  absl::Status SaveIndex(RDBOutputStream& rdb_stream) const override {
    return absl::OkStatus();
  }

  private:
  // Each text field is assigned a unique number within the containing index, this is used
  // by the Postings object to identify fields.
  size_t text_field_number;
  std::shared_ptr<TextIndex> text_; // 2


  inline void ForEachTrackedKey(
      absl::AnyInvocable<void(const InternedStringPtr&)> fn) const override {
    absl::MutexLock lock(&index_mutex_);
    for (const auto& [key, _] : tracked_tags_by_keys_) {
      fn(key);
    }
  }
  uint64_t GetRecordCount() const override;
  std::unique_ptr<data_model::Index> ToProto() const override;

  InternedStringPtr GetRawValue(const InternedStringPtr& key) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

  // Abstract for Text. Every text type will have a specific implementation.
  class EntriesFetcherIterator : public EntriesFetcherIteratorBase {
   public:
    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
  };

  // Common for all Text types.
  class EntriesFetcher : public EntriesFetcherBase {
   public:
    EntriesFetcher(size_t size,
                const InternedStringSet* untracked_keys = nullptr)
    : size_(size), untracked_keys_(untracked_keys) {}

    size_t Size() const override { return size_; }

    // Factory method that creates the appropriate iterator
    std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

   private:
      size_t size_;
      const InternedStringSet* untracked_keys_;
      TextPredicate::Operation operation_;
      absl::string_view data_;
      bool no_field_{false};
  };

  // TODO: Handle TextPredicate in the .c file.
  // This is needed for the FT.SEARCH command's core search fn.
  virtual std::unique_ptr<EntriesFetcher> Search(
      const query::TextPredicate& predicate,
      bool negate) const ABSL_NO_THREAD_SAFETY_ANALYSIS;

 private:
  mutable absl::Mutex index_mutex_;
};
}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_H_

// WIP below:

std::unique_ptr<Text::EntriesFetcher> Text::Search(
    const query::TextPredicate& predicate,
    bool negate) const {
  // TODO: Calculate Size based on number of document key names found.
  auto fetcher = std::make_unique<EntriesFetcher>(
    CalculateSize(predicate),
    negate ? &untracked_keys_ : nullptr);
  fetcher->operation_ = predicate.GetOperation();
  // Currently, we support a single word exact match.
  fetcher->data_ = predicate.GetTextString();
  return fetcher;
}

size_t Text::CalculateSize(const query::TextPredicate& predicate) const {
  switch (predicate.GetOperation()) {
    case TextPredicate::Operation::kExact: {
      // TODO: Handle phrase matching.
      std::vector<WordIterator> iterVec = {iter};
      auto word = predicate.GetTextString();
      if (words.empty()) return 0;
      return 0;
    }
    // Other operations...
    default:
      return 0;
  }
}


size_t Text::EntriesFetcher::Size() const { return size_; }

std::unique_ptr<EntriesFetcherIteratorBase> Text::EntriesFetcher::Begin() {
  // Numeric.
  // auto itr = std::make_unique<EntriesFetcherIterator>(
  //     entries_range_, additional_entries_range_, untracked_keys_);
  // itr->Next();
  // return itr;
  switch (operation_) {
    case TextPredicate::Operation::kExact:
      auto iter = text_.prefix_.GetWordIterator(data_);
      std::vector<WordIterator> iterVec = {iter};
      bool slop = 0;
      bool in_order = true;
      // TODO: Implement PhraseIterator in the .cc and .h files.
      auto itr = std::make_unique<text::PhraseIterator>(iterVec, slop, in_order, untracked_keys_);
      itr->Next();
      return itr;
    default:
      CHECK(false) << "Unsupported TextPredicate operation: " << static_cast<int>(operation_);
      return nullptr;  // Should never reach here.
  }
}
