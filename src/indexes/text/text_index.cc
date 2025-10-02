/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/text_index.h"

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

}  // namespace

TextIndexSchema::TextIndexSchema(data_model::Language language,
                                 const std::string& punctuation,
                                 bool with_offsets,
                                 const std::vector<std::string>& stop_words)
    : language_(language),
      punct_bitmap_(BuildPunctuationBitmap(punctuation)),
      stop_words_set_(BuildStopWordsSet(stop_words)),
      with_offsets_(with_offsets),
      stemmer_(sb_stemmer_new(GetLanguageString(), "UTF_8")) {}

TextIndexSchema::~TextIndexSchema() {
  if (stemmer_) {
    sb_stemmer_delete(stemmer_);
  }
}

const char* TextIndexSchema::GetLanguageString() const {
  switch (language_) {
    case data_model::LANGUAGE_ENGLISH:
      return "english";
    default:
      return "english";
  }
}

uint64_t TextIndexSchema::GetTotalPositions() const {
  if (!text_index_) {
    return 0;
  }
  
  uint64_t total_positions = 0;
  
  auto word_iter = text_index_->prefix_.GetWordIterator("");
  
  while (!word_iter.Done()) {
    auto postings = word_iter.GetTarget();
    if (postings) {
      total_positions += postings->GetPositionCount();
    }
    word_iter.Next();
  }
  
  return total_positions;
}

uint64_t TextIndexSchema::GetNumTerms() const {
  if (!text_index_) {
    return 0;
  }
  
  uint64_t num_terms = 0;
  
  auto word_iter = text_index_->prefix_.GetWordIterator("");
  
  while (!word_iter.Done()) {
    num_terms++;
    word_iter.Next();
  }
  
  return num_terms;
}

uint64_t TextIndexSchema::GetTotalTermFrequency() const {
  if (!text_index_) {
    return 0;
  }
  
  uint64_t total_term_freq = 0;
  
  auto word_iter = text_index_->prefix_.GetWordIterator("");
  
  while (!word_iter.Done()) {
    auto postings = word_iter.GetTarget();
    if (postings) {
      total_term_freq += postings->GetTotalTermFrequency();
    }
    word_iter.Next();
  }
  
  return total_term_freq;
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

double TextIndexSchema::GetTotalTextIndexSizePerDocAvg(uint64_t num_docs) const {
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
  uint64_t num_terms = GetNumTerms();
  if (num_terms == 0) {
    return 0.0;
  }
  return static_cast<double>(GetTotalTextIndexMemoryUsage()) / num_terms;
}

}  // namespace valkey_search::indexes::text
