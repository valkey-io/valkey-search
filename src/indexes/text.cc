/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text.h"

#include <stdexcept>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/posting.h"

namespace valkey_search::indexes {

Text::Text(const data_model::TextIndex& text_index_proto,
           std::shared_ptr<text::TextIndex> text_index)
    : IndexBase(IndexerType::kText), text_index_(text_index) {
  // TODO: Initialize text_field_number_
  // Right now the data_model is as follows. Do we need a text field number
  // here?
  // data_model::TextIndex {
  //   bool with_suffix_trie = 1;
  //   bool no_stem = 2;
  //   int32 min_stem_size = 3;
  // }
}

absl::StatusOr<bool> Text::AddRecord(const InternedStringPtr& key,
                                     absl::string_view data) {
  // TODO: Replace this tokenizing with the proper lexer functionality when it's
  // implemented
  int prev_pos = 0;
  for (int i = 0; i <= data.size(); i++) {
    if (i == data.size() || data[i] == ' ') {
      if (i > prev_pos) {
        absl::string_view word = data.substr(prev_pos, i - prev_pos);
        text_index_->prefix_.Mutate(
            word,
            [&](std::optional<std::shared_ptr<text::Postings>> existing)
                -> std::optional<std::shared_ptr<text::Postings>> {
              // TODO: Mutate the postings object
              // Note that we'll have to create a new Postings object if one
              // doesn't exist and I'm not sure if we should be passing a whole
              // IndexSchema object to the constructor.
              throw std::runtime_error("Mutate lambda not implemented");
            });
      }
      prev_pos = i + 1;
    }
  }
  return true;
}

absl::StatusOr<bool> Text::RemoveRecord(const InternedStringPtr& key,
                                        DeletionType deletion_type) {
  throw std::runtime_error("Text::RemoveRecord not implemented");
}

absl::StatusOr<bool> Text::ModifyRecord(const InternedStringPtr& key,
                                        absl::string_view data) {
  throw std::runtime_error("Text::ModifyRecord not implemented");
}

int Text::RespondWithInfo(ValkeyModuleCtx* ctx) const {
  throw std::runtime_error("Text::RespondWithInfo not implemented");
}

bool Text::IsTracked(const InternedStringPtr& key) const {
  throw std::runtime_error("Text::IsTracked not implemented");
}

uint64_t Text::GetRecordCount() const {
  throw std::runtime_error("Text::GetRecordCount not implemented");
}

std::unique_ptr<data_model::Index> Text::ToProto() const {
  throw std::runtime_error("Text::ToProto not implemented");
}

size_t Text::CalculateSize(const query::TextPredicate& predicate) const {
  switch (predicate.GetOperation()) {
    case query::TextPredicate::Operation::kExact: {
      // TODO: Handle phrase matching.
      auto word = predicate.GetTextString();
      if (word.empty()) return 0;
      // TODO: Check the number of documents in the postings obj for the word.
      return 0;
    }
    default:
      return 0;
  }
}

std::unique_ptr<Text::EntriesFetcher> Text::Search(
    const query::TextPredicate& predicate,
    bool negate) const {
  auto fetcher = std::make_unique<EntriesFetcher>(
    CalculateSize(predicate),
    text_index_,
    negate ? &untracked_keys_ : nullptr);
  fetcher->operation_ = predicate.GetOperation();
  // Currently, we support a single word exact match.
  fetcher->data_ = predicate.GetTextString();
  return fetcher;
}


size_t Text::EntriesFetcher::Size() const { return size_; }

std::unique_ptr<EntriesFetcherIteratorBase> Text::EntriesFetcher::Begin() {
  switch (operation_) {
    case query::TextPredicate::Operation::kExact: {
      auto iter = text_index_->prefix_.GetWordIterator(data_);
      std::vector<WordIterator> iterVec = {iter};
      bool slop = 0;
      bool in_order = true;
      auto itr = std::make_unique<text::PhraseIterator>(iterVec, slop, in_order, untracked_keys_);
      itr->Next();
      return itr;
    }
    default:
      CHECK(false) << "Unsupported TextPredicate operation: " << static_cast<int>(operation_);
      return nullptr;  // Should never reach here.
  }
  return nullptr;
}

}  // namespace valkey_search::indexes
