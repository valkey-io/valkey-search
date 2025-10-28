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

namespace valkey_search::indexes::text {

TextIndexMetadata& GetTextIndexMetadata() {
  static TextIndexMetadata metadata;
  return metadata;
}

namespace {

std::optional<std::shared_ptr<text::Postings>> AddWordToPostings(
    std::optional<std::shared_ptr<text::Postings>> existing,
    const InternedStringPtr& key, size_t text_field_number, uint32_t position,
    bool save_positions, uint8_t num_text_fields) {
  std::shared_ptr<text::Postings> postings;
  bool is_new_term = false;
  if (existing.has_value()) {
    postings = existing.value();
  } else {
    postings =
        std::make_shared<text::Postings>(save_positions, num_text_fields);
    is_new_term = true;
  }
  postings->InsertPosting(key, text_field_number, position);
  
  auto& metadata = GetTextIndexMetadata();
  std::lock_guard<std::mutex> lock(metadata.mtx);
  if (is_new_term) {
    metadata.num_unique_terms++;
  }
  metadata.total_term_frequency++;
  
  return postings;
}

std::optional<std::shared_ptr<text::Postings>> RemoveKeyFromPostings(
    std::optional<std::shared_ptr<text::Postings>> existing,
    const InternedStringPtr& key) {
  CHECK(existing.has_value()) << "Per-key tree became unaligned";
  auto postings = existing.value();
  postings->RemoveKey(key);
  
  if (!postings->IsEmpty()) {
    return postings;
  } else {
    auto& metadata = GetTextIndexMetadata();
    std::lock_guard<std::mutex> lock(metadata.mtx);
    metadata.num_unique_terms--;
    return std::nullopt;
  }
}

}  // namespace

TextIndexSchema::TextIndexSchema(data_model::Language language,
                                 const std::string& punctuation,
                                 bool with_offsets,
                                 const std::vector<std::string>& stop_words)
    : with_offsets_(with_offsets), lexer_(language, punctuation, stop_words) {}

absl::StatusOr<bool> TextIndexSchema::IndexAttributeData(
    const InternedStringPtr& key, absl::string_view data,
    size_t text_field_number, bool stem, size_t min_stem_size, bool suffix) {
  auto tokens = lexer_.Tokenize(data, stem, min_stem_size);

  if (!tokens.ok()) {
    if (tokens.status().code() == absl::StatusCode::kInvalidArgument) {
      return false;  // UTF-8 errors → hash_indexing_failures
    }
    return tokens.status();
  }

  TextIndex* key_index;  // Key-specific index
  {
    std::lock_guard<std::mutex> per_key_guard(per_key_text_indexes_mutex_);
    key_index = &per_key_text_indexes_[key];
  }

  // TODO: Once we optimize the postings object for space efficiency, it won't
  // be cheap to incrementally update. We likely want to build the position map
  // structure up front for each word in the key and then merge them into the
  // trees' posting objects at the end of the key ingestion.
  for (uint32_t position = 0; position < tokens->size(); ++position) {
    const auto& token = (*tokens)[position];
    const std::optional<std::string> reverse_token =
        suffix ? std::optional<std::string>(
                     std::string(token.rbegin(), token.rend()))
               : std::nullopt;

    // Mutate key index
    {
      std::lock_guard<std::mutex> key_guard(key_index->mutex_);
      std::optional<std::shared_ptr<Postings>> new_target =
          key_index->prefix_.MutateTarget(token, [&](auto existing) {
            return AddWordToPostings(existing, key, text_field_number, position,
                                     with_offsets_, num_text_fields_);
          });

      if (suffix) {
        if (!key_index->suffix_.has_value()) {
          key_index->suffix_.emplace();
        }
        key_index->suffix_.value().SetTarget(*reverse_token, new_target);
      }
    }

    // Mutate schema index
    {
      std::lock_guard<std::mutex> schema_guard(text_index_->mutex_);
      std::optional<std::shared_ptr<Postings>> new_target =
          text_index_->prefix_.MutateTarget(token, [&](auto existing) {
            return AddWordToPostings(existing, key, text_field_number, position,
                                     with_offsets_, num_text_fields_);
          });

      if (suffix) {
        if (!text_index_->suffix_.has_value()) {
          text_index_->suffix_.emplace();
        }
        text_index_->suffix_.value().SetTarget(*reverse_token, new_target);
      }
    }
  }

  return true;
}

void TextIndexSchema::DeleteKeyData(const InternedStringPtr& key) {
  TextIndex* key_index = nullptr;
  {
    std::lock_guard<std::mutex> per_key_guard(per_key_text_indexes_mutex_);
    auto it = per_key_text_indexes_.find(key);
    if (it != per_key_text_indexes_.end()) {
      key_index = &it->second;
    }
  }

  if (key_index) {
    std::lock_guard<std::mutex> main_tree_guard(text_index_->mutex_);

    // Cleanup schema-level text index
    auto iter = key_index->prefix_.GetWordIterator("");
    while (!iter.Done()) {
      std::string_view word = iter.GetWord();
      std::optional<std::shared_ptr<Postings>> new_target =
          text_index_->prefix_.MutateTarget(word, [&](auto existing) {
            return RemoveKeyFromPostings(existing, key);
          });
      if (text_index_->suffix_.has_value()) {
        std::string reverse_word(word.rbegin(), word.rend());
        text_index_->suffix_.value().SetTarget(reverse_word, new_target);
      }
      iter.Next();
    }
  }

  {
    std::lock_guard<std::mutex> per_key_guard(per_key_text_indexes_mutex_);
    per_key_text_indexes_.erase(key);
  }
}

uint64_t TextIndexSchema::GetTotalPositions() const {
  auto& metadata = GetTextIndexMetadata();
  std::lock_guard<std::mutex> lock(metadata.mtx);
  return metadata.total_positions;
}

uint64_t TextIndexSchema::GetNumUniqueTerms() const {
  auto& metadata = GetTextIndexMetadata();
  std::lock_guard<std::mutex> lock(metadata.mtx);
  return metadata.num_unique_terms;
}

uint64_t TextIndexSchema::GetTotalTermFrequency() const {
  auto& metadata = GetTextIndexMetadata();
  std::lock_guard<std::mutex> lock(metadata.mtx);
  return metadata.total_term_frequency;
}

uint64_t TextIndexSchema::GetPostingsMemoryUsage() const {
  if (!text_index_) {
    return 0;
  }
  return Postings::GetMemoryUsage();
}

uint64_t TextIndexSchema::GetRadixTreeMemoryUsage() const {
  if (!text_index_) {
    return 0;
  }
  return RadixTree<std::shared_ptr<Postings>, false>::GetMemoryUsage();
}

uint64_t TextIndexSchema::GetPositionMemoryUsage() const {
  uint64_t total_positions = GetTotalPositions();
  return total_positions * sizeof(uint32_t);
}

uint64_t TextIndexSchema::GetTotalTextIndexMemoryUsage() const {
  return GetPostingsMemoryUsage() + GetRadixTreeMemoryUsage();
}

double TextIndexSchema::GetTotalTermsPerDocAvg(uint64_t num_docs) const {
  if (num_docs == 0) {
    return 0.0;
  }
  return static_cast<double>(GetTotalTermFrequency()) / num_docs;
}

double TextIndexSchema::GetTotalTextIndexSizePerDocAvg(
    uint64_t num_docs) const {
  if (num_docs == 0) {
    return 0.0;
  }
  return static_cast<double>(GetTotalTextIndexMemoryUsage()) / num_docs;
}

double TextIndexSchema::GetPositionSizePerTermAvg() const {
  uint64_t num_total_terms = GetTotalTermFrequency();
  if (num_total_terms == 0) {
    return 0.0;
  }
  return static_cast<double>(GetPositionMemoryUsage()) / num_total_terms;
}

double TextIndexSchema::GetTotalTextIndexSizePerTermAvg() const {
  uint64_t num_terms = GetNumUniqueTerms();
  if (num_terms == 0) {
    return 0.0;
  }
  return static_cast<double>(GetTotalTextIndexMemoryUsage()) / num_terms;
}

}  // namespace valkey_search::indexes::text
