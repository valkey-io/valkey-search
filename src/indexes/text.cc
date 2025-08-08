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
           std::shared_ptr<text::TextIndexSchema> text_index_schema,
           size_t text_field_number)
    : IndexBase(IndexerType::kText), 
      text_field_number_(text_field_number),
      text_index_schema_(text_index_schema),
      with_suffix_trie_(text_index_proto.with_suffix_trie()),
      no_stem_(text_index_proto.no_stem()),
      min_stem_size_(text_index_proto.min_stem_size()) {   
}

absl::StatusOr<bool> Text::AddRecord(const InternedStringPtr& key,
                                     absl::string_view data) {
  // TODO: Replace this tokenizing with the proper lexer functionality when it's
  // implemented
  int prev_pos = 0;
  uint32_t position = 0;
  
  for (int i = 0; i <= data.size(); i++) {
    if (i == data.size() || data[i] == ' ') {
      if (i > prev_pos) {
        absl::string_view word = data.substr(prev_pos, i - prev_pos);
        text_index_schema_->text_index_->prefix_.Mutate(
            word,
            [&](std::optional<std::shared_ptr<text::Postings>> existing)
                -> std::optional<std::shared_ptr<text::Postings>> {
              std::shared_ptr<text::Postings> postings;
              if (existing.has_value()) {
                postings = existing.value();
              } else {
                // Create new Postings object with schema configuration
                // TODO: Get save_positions from IndexSchema, for now assume true
                bool save_positions = true;
                uint8_t num_text_fields = text_index_schema_->num_text_fields_;
                postings = std::make_shared<text::Postings>(save_positions, num_text_fields);
              }
              
              // Add the key and position to postings
              postings->InsertPosting(key, text_field_number_, position);
              return postings;
            });
        position++;
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
  return false;
}

uint64_t Text::GetRecordCount() const {
  throw std::runtime_error("Text::GetRecordCount not implemented");
}

std::unique_ptr<data_model::Index> Text::ToProto() const {
  auto index_proto = std::make_unique<data_model::Index>();
  auto text_index = std::make_unique<data_model::TextIndex>();
  text_index->set_with_suffix_trie(with_suffix_trie_);
  text_index->set_no_stem(no_stem_);
  text_index->set_min_stem_size(min_stem_size_);
  index_proto->set_allocated_text_index(text_index.release());
  return index_proto;
}

std::unique_ptr<Text::EntriesFetcher> Text::Search(
    const TextPredicate& predicate, bool negate) const {
  throw std::runtime_error("Text::Search not implemented");
}

}  // namespace valkey_search::indexes
