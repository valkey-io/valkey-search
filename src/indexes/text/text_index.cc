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
    postings = std::make_shared<text::Postings>(save_positions, num_text_fields);
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

absl::StatusOr<bool> TextIndexSchema::IndexAttributeData(const InternedStringPtr& key,
                                absl::string_view data, size_t text_field_number, bool stem,
                                size_t min_stem_size, bool suffix) {
  std::lock_guard<std::mutex> per_key_guard(per_key_text_indexes_mutex_);

  auto tokens =
      lexer_.Tokenize(data, GetPunctuationBitmap(), stem, min_stem_size, GetStopWordsSet());

  if (!tokens.ok()) {
    if (tokens.status().code() == absl::StatusCode::kInvalidArgument) {
      return false;  // UTF-8 errors → hash_indexing_failures
    }
    return tokens.status();
  }

  // Be smart about how we update in main trees since don't want to traverse
  // twice to same PO

  for (uint32_t position = 0; position < tokens->size(); ++position) {
    const auto& token = (*tokens)[position];
    text_index_->prefix_.Mutate(
        token,
        [&](auto existing) {
          return AddWordToPostings(existing, key, text_field_number, position,
                              GetWithOffsets(), GetNumTextFields());
        });
  }

  return true;
}

void TextIndexSchema::DeleteKeyData(const InternedStringPtr& key) {
  std::optional<TextIndex> key_index;
  {
    std::lock_guard<std::mutex> per_key_guard(per_key_text_indexes_mutex_);
    auto it = per_key_text_indexes_.find(key);
    if (it != per_key_text_indexes_.end()) {
      key_index.emplace(std::move(it->second));
    }
  }

  if (key_index.has_value()) {
    std::lock_guard<std::mutex> main_tree_guard(text_index_->mutex_);

    // Cleanup prefix tree
    auto iter = key_index->prefix_.GetWordIterator("");
    while (!iter.Done()) {
      std::string_view word = iter.GetWord();
      text_index_->prefix_.Mutate(word, [&](auto existing) {
        return RemoveKeyFromPostings(existing, key);
      });
    }

    if (text_index_->suffix_.has_value()) {
      // TODO: Cleanup suffix tree
    }
  }
}

}  // namespace valkey_search::indexes::text
