/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/text_index.h"

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "libstemmer.h"
#include "vmsdk/src/memory_allocation.h"

namespace valkey_search::indexes::text {

// Track current TextIndexSchema for accessing metadata
thread_local TextIndexSchema* current_schema_ = nullptr;

TextIndexSchema* GetTextIndexSchema() {
  CHECK(current_schema_ != nullptr) << "No TextIndexSchema context set";
  return current_schema_;
}

namespace {

std::optional<std::shared_ptr<Postings>> AddKeyToPostings(
    std::optional<std::shared_ptr<Postings>> existing,
    const InternedStringPtr& key, PositionMap&& pos_map) {
  std::shared_ptr<Postings> postings;
  if (existing.has_value()) {
    postings = existing.value();
  } else {
    current_schema_->GetMetadata().num_unique_terms++;
    postings = std::make_shared<Postings>();
  }
  postings->InsertKey(key, std::move(pos_map));
  return postings;
}

std::optional<std::shared_ptr<Postings>> RemoveKeyFromPostings(
    std::optional<std::shared_ptr<Postings>> existing,
    const InternedStringPtr& key) {
  CHECK(existing.has_value()) << "Per-key tree became unaligned";
  auto postings = existing.value();
  postings->RemoveKey(key);

  if (!postings->IsEmpty()) {
    return postings;
  } else {
    current_schema_->GetMetadata().num_unique_terms--;
    return std::nullopt;
  }
}

}  // namespace

TextIndexSchema::TextIndexSchema(data_model::Language language,
                                 const std::string& punctuation,
                                 bool with_offsets,
                                 const std::vector<std::string>& stop_words)
    : with_offsets_(with_offsets), lexer_(language, punctuation, stop_words) {}

absl::StatusOr<bool> TextIndexSchema::StageAttributeData(
    const InternedStringPtr& key, absl::string_view data,
    size_t text_field_number, bool stem, size_t min_stem_size, bool suffix) {
  current_schema_ = this;
  NestedMemoryScope scope{metadata_.text_index_memory_pool_};

  auto tokens = lexer_.Tokenize(data, stem, min_stem_size);

  if (!tokens.ok()) {
    if (tokens.status().code() == absl::StatusCode::kInvalidArgument) {
      return false;  // UTF-8 errors → hash_indexing_failures
    }
    return tokens.status();
  }

  // Map tokens -> positions -> field-masks
  TokenPositions* token_positions;
  {
    std::lock_guard<std::mutex> guard(in_progress_key_updates_mutex_);
    token_positions = &in_progress_key_updates_[key];
  }
  for (uint32_t i = 0; i < tokens->size(); ++i) {
    const auto& token = tokens.value()[i];
    uint32_t position =
        with_offsets_ ? i
                      : 0;  // If positional info is disabled we default to 0
    auto& [positions, suffix_eligible] = (*token_positions)[token];
    if (suffix) suffix_eligible = true;
    auto [pos_it, _] =
        positions.try_emplace(position, FieldMask::Create(num_text_fields_));
    pos_it->second->SetField(text_field_number);
  }

  return true;
}

void TextIndexSchema::CommitKeyData(const InternedStringPtr& key) {
  current_schema_ = this;
  NestedMemoryScope scope{metadata_.text_index_memory_pool_};

  // Retrieve the key's staged data
  TokenPositions token_positions;
  {
    std::lock_guard<std::mutex> guard(in_progress_key_updates_mutex_);
    auto node = in_progress_key_updates_.extract(key);
    // Exit early if the key contains no new text updates
    if (node.empty()) {
      return;
    }
    token_positions = std::move(node.mapped());
  }

  TextIndex key_index{};

  // Index the key's tokens
  for (auto& entry : token_positions) {
    const std::string& token = entry.first;
    auto& [pos_map, suffix] = entry.second;

    const std::optional<std::string> reverse_token =
        suffix ? std::optional<std::string>(
                     std::string(token.rbegin(), token.rend()))
               : std::nullopt;

    std::optional<std::shared_ptr<Postings>> updated_target;

    // Update the postings object for the token in the schema-level index with
    // the key and position map
    {
      std::lock_guard<std::mutex> schema_guard(text_index_mutex_);
      updated_target =
          text_index_->prefix_.MutateTarget(token, [&](auto existing) {
            NestedMemoryScope scope{metadata_.posting_memory_pool_};
            // Note: Right now this won't include the position map memory since
            // it's already allocated and moved into the postings object. Once
            // we start creating a serialized version instead then it will be
            // tracked. At that point stop moving the pos_map and just pass a
            // reference so that it doesn't get cleaned up in the memory scope.
            return AddKeyToPostings(existing, key, std::move(pos_map));
          });
      if (suffix) {
        if (!text_index_->suffix_.has_value()) {
          text_index_->suffix_.emplace();
        }
        text_index_->suffix_.value().SetTarget(*reverse_token, updated_target);
      }
    }

    // Put the token in the per-key index pointing to the same shared postings
    // object
    key_index.prefix_.SetTarget(token, updated_target);
    if (suffix) {
      if (!key_index.suffix_.has_value()) {
        key_index.suffix_.emplace();
      }
      key_index.suffix_.value().SetTarget(*reverse_token, updated_target);
    }
  }

  // Map the key to the newly created per-key index
  {
    std::lock_guard<std::mutex> per_key_guard(per_key_text_indexes_mutex_);
    per_key_text_indexes_.emplace(key, std::move(key_index));
  }
}

void TextIndexSchema::DeleteKeyData(const InternedStringPtr& key) {
  current_schema_ = this;
  NestedMemoryScope scope{metadata_.text_index_memory_pool_};

  // Extract the per-key index
  TextIndex key_index;
  {
    std::lock_guard<std::mutex> per_key_guard(per_key_text_indexes_mutex_);
    auto node = per_key_text_indexes_.extract(key);
    if (node.empty()) {
      return;
    }
    key_index = std::move(node.mapped());
  }

  auto iter = key_index.prefix_.GetWordIterator("");

  // Cleanup schema-level text index
  std::lock_guard<std::mutex> schema_guard(text_index_mutex_);
  while (!iter.Done()) {
    std::string_view word = iter.GetWord();
    std::optional<std::shared_ptr<Postings>> new_target =
        text_index_->prefix_.MutateTarget(word, [&](auto existing) {
          NestedMemoryScope scope{metadata_.posting_memory_pool_};
          return RemoveKeyFromPostings(existing, key);
        });
    if (text_index_->suffix_.has_value()) {
      std::string reverse_word(word.rbegin(), word.rend());
      text_index_->suffix_.value().SetTarget(reverse_word, new_target);
    }
    iter.Next();
  }
}

uint64_t TextIndexSchema::GetTotalPositions() const {
  return metadata_.total_positions.load();
}

uint64_t TextIndexSchema::GetNumUniqueTerms() const {
  return metadata_.num_unique_terms.load();
}

uint64_t TextIndexSchema::GetTotalTermFrequency() const {
  return metadata_.total_term_frequency.load();
}

uint64_t TextIndexSchema::GetPostingsMemoryUsage() const {
  if (!text_index_) {
    return 0;
  }
  return metadata_.posting_memory_pool_.GetUsage();
}

uint64_t TextIndexSchema::GetRadixTreeMemoryUsage() const {
  // TODO: properly implement
  return GetTotalTextIndexMemoryUsage() - GetPostingsMemoryUsage();
}

// Note: This is a subset of the memory reported by GetPostingsMemoryUsage(),
uint64_t TextIndexSchema::GetPositionMemoryUsage() const {
  uint64_t total_positions = GetTotalPositions();
  return total_positions * sizeof(uint32_t);
}

uint64_t TextIndexSchema::GetTotalTextIndexMemoryUsage() const {
  return metadata_.text_index_memory_pool_.GetUsage();
}

}  // namespace valkey_search::indexes::text
