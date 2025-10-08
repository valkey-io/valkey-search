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

namespace {

bool IsWhitespace(unsigned char c) {
  return std::isspace(c) || std::iscntrl(c);
}

PunctuationBitmap BuildPunctuationBitmap(const std::string& punctuation) {
  PunctuationBitmap bitmap;
  bitmap.reset();

  for (int i = 0; i < 256; ++i) {
    if (IsWhitespace(static_cast<unsigned char>(i))) {
      bitmap.set(i);
    }
  }

  for (char c : punctuation) {
    bitmap.set(static_cast<unsigned char>(c));
  }

  return bitmap;
}

absl::flat_hash_set<std::string> BuildStopWordsSet(
    const std::vector<std::string>& stop_words) {
  absl::flat_hash_set<std::string> stop_words_set;
  for (const auto& word : stop_words) {
    stop_words_set.insert(absl::AsciiStrToLower(word));
  }
  return stop_words_set;
}

std::optional<std::shared_ptr<text::Postings>> AddWordToPostings(
    std::optional<std::shared_ptr<text::Postings>> existing,
    const InternedStringPtr& key, size_t text_field_number, uint32_t position,
    bool save_positions, uint8_t num_text_fields) {
  std::shared_ptr<text::Postings> postings;
  if (existing.has_value()) {
    postings = existing.value();
  } else {
    postings =
        std::make_shared<text::Postings>(save_positions, num_text_fields);
  }
  postings->InsertPosting(key, text_field_number, position);
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
    return std::nullopt;
  }
}

}  // namespace

TextIndexSchema::TextIndexSchema(data_model::Language language,
                                 const std::string& punctuation,
                                 bool with_offsets,
                                 const std::vector<std::string>& stop_words)
    : language_(language),
      punct_bitmap_(BuildPunctuationBitmap(punctuation)),
      stop_words_set_(BuildStopWordsSet(stop_words)),
      with_offsets_(with_offsets),
      lexer_(Lexer(GetLanguageString())) {}

const char* TextIndexSchema::GetLanguageString() const {
  switch (language_) {
    case data_model::LANGUAGE_ENGLISH:
      return "english";
    default:
      return "english";
  }
}

absl::StatusOr<bool> TextIndexSchema::IndexAttributeData(
    const InternedStringPtr& key, absl::string_view data,
    size_t text_field_number, bool stem, size_t min_stem_size, bool suffix) {
  auto tokens = lexer_.Tokenize(data, GetPunctuationBitmap(), stem,
                                min_stem_size, GetStopWordsSet());

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
                                     GetWithOffsets(), GetNumTextFields());
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
                                     GetWithOffsets(), GetNumTextFields());
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
        std::optional<std::shared_ptr<Postings>> new_target = text_index_->prefix_.MutateTarget(word, [&](auto existing) {
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

}  // namespace valkey_search::indexes::text
