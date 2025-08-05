/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/lexer.h"

#include <bitset>
#include <cstddef>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"

namespace valkey_search::text {

namespace {
// ASCII character bitmap for fast punctuation lookup
using PunctuationBitmap = std::bitset<256>;

// Check if a character is valid UTF-8 continuation byte
bool IsUtf8Continuation(char c) {
  return (static_cast<unsigned char>(c) & 0xC0) == 0x80;
}

// Get the length of a UTF-8 character from its first byte
int GetUtf8CharLength(char c) {
  unsigned char byte = static_cast<unsigned char>(c);
  if (byte < 0x80) return 1;  // ASCII
  if ((byte & 0xE0) == 0xC0) return 2;  // 110xxxxx
  if ((byte & 0xF0) == 0xE0) return 3;  // 1110xxxx
  if ((byte & 0xF8) == 0xF0) return 4;  // 11110xxx
  return -1;  // Invalid UTF-8
}

// Validate UTF-8 sequence starting at position
bool IsValidUtf8Sequence(absl::string_view text, size_t pos, int expected_len) {
  if (pos + expected_len > text.size()) return false;
  
  for (int i = 1; i < expected_len; ++i) {
    if (!IsUtf8Continuation(text[pos + i])) {
      return false;
    }
  }
  return true;
}

// Check if entire string is valid UTF-8
bool IsValidUtf8(absl::string_view text) {
  size_t pos = 0;
  while (pos < text.size()) {
    int char_len = GetUtf8CharLength(text[pos]);
    if (char_len < 0 || !IsValidUtf8Sequence(text, pos, char_len)) {
      return false;
    }
    pos += char_len;
  }
  return true;
}

// Create punctuation bitmap from string
PunctuationBitmap CreatePunctuationBitmap(const std::string& punctuation) {
  PunctuationBitmap bitmap;
  for (char c : punctuation) {
    bitmap.set(static_cast<unsigned char>(c));
  }
  return bitmap;
}

// No defaults here - parser is the source of truth

}  // namespace

// Initialize lexer from schema proto
Lexer::Lexer(const data_model::IndexSchema& schema_proto) {
  ConfigureFromSchemaProto(schema_proto);
}

// Initialize lexer from text index and schema protos
Lexer::Lexer(const data_model::TextIndex& text_index_proto,
             const data_model::IndexSchema& schema_proto) {
  // First configure from schema-level settings
  ConfigureFromSchemaProto(schema_proto);
  
  // Then apply field-specific overrides
  ApplyFieldOverrides(text_index_proto);
}

// Configure lexer from schema proto
void Lexer::ConfigureFromSchemaProto(const data_model::IndexSchema& schema_proto) {
  // Get schema-level text parameters - with no fallbacks
  punctuation_ = schema_proto.punctuation();
  bool nostem = schema_proto.nostem();
  case_conversion_enabled_ = !nostem;
  stemming_enabled_ = !nostem;
  min_stem_size_ = schema_proto.min_stem_size();
  language_ = schema_proto.language();
  
  // Initialize stop words from schema parameters
  SetStopWords(std::vector<std::string>(schema_proto.stop_words().begin(), schema_proto.stop_words().end()));
}

// Apply field-specific overrides
void Lexer::ApplyFieldOverrides(const data_model::TextIndex& text_index_proto) {
  // Field-specific parameters override schema parameters
  if (text_index_proto.nostem()) {
    case_conversion_enabled_ = false;
    stemming_enabled_ = false;
  }
  
  // Field-specific min stem size overrides schema if non-zero
  if (text_index_proto.min_stem_size() > 0) {
    min_stem_size_ = text_index_proto.min_stem_size();
  }
}
      
// TODO: Add these methods once we implement stop words functionality
void Lexer::SetStopWords(const std::vector<std::string>& stop_words) {
  // No-op for now, will be implemented in future
  stop_words_.clear();
  for (const auto& word : stop_words) {
    stop_words_.insert(absl::AsciiStrToLower(word));
  }
}

const std::unordered_set<std::string>& Lexer::GetStopWords() const {
  return stop_words_;
}

// TODO: Add these methods once we implement stemming functionality
void Lexer::SetStemmingEnabled(bool enabled) {
  stemming_enabled_ = enabled;
}

bool Lexer::IsStemmingEnabled() const {
  return stemming_enabled_;
}

void Lexer::SetMinStemSize(uint32_t size) {
  min_stem_size_ = size;
}

uint32_t Lexer::GetMinStemSize() const {
  return min_stem_size_;
}

void Lexer::SetLanguage(data_model::Language language) {
  language_ = language;
}

data_model::Language Lexer::GetLanguage() const {
  return language_;
}

void Lexer::SetCaseConversionEnabled(bool enabled) {
  case_conversion_enabled_ = enabled;
}

bool Lexer::IsCaseConversionEnabled() const {
  return case_conversion_enabled_;
}

void Lexer::ApplyCaseConversion(std::vector<std::string>& words) const {
  for (auto& word : words) {
    word = absl::AsciiStrToLower(word);
  }
}

// TODO: Implement stop word removal
void Lexer::ApplyStopWordRemoval(std::vector<std::string>& words, std::vector<size_t>& positions) const {
  // No-op for now - will be implemented in future stages
  // This function should remove common words (like "the", "a", "is") that don't contribute to search relevance
}

// TODO: Implement stemming using Snowball stemmer library
void Lexer::ApplyStemming(std::vector<std::string>& words) const {
  // No-op for now - will be implemented in future stages
  // This function should reduce words to their root form (e.g., "running" -> "run")
}

// TODO: Implement Porter stemming algorithm
std::string Lexer::ApplyPorterStemming(const std::string& word) const {
  // No-op for now - will be implemented in future stages
  // Returns the word unchanged until stemming is implemented
  return word;
}

absl::Status Lexer::SetPunctuation(const std::string& new_punctuation) {
  // Validate that all characters are ASCII
  for (char c : new_punctuation) {
    if (static_cast<unsigned char>(c) >= 128) {
      return absl::InvalidArgumentError(
          "Punctuation characters must be ASCII");
    }
  }
  
  punctuation_ = new_punctuation;
  return absl::OkStatus();
}

std::string Lexer::GetPunctuation() const {
  return punctuation_;
}

absl::StatusOr<LexerOutput> Lexer::ProcessString(absl::string_view s) const {
  // Validate UTF-8
  if (!IsValidUtf8(s)) {
    return absl::InvalidArgumentError("Invalid UTF-8 sequence in input");
  }
  
  LexerOutput output;
  PunctuationBitmap punct_bitmap = CreatePunctuationBitmap(punctuation_);
  
  // Track positions for all words
  output.has_positions_ = true;
  
  size_t pos = 0;
  while (pos < s.size()) {
    // Skip punctuation/whitespace, but handle escape sequences
    while (pos < s.size()) {
      char current = s[pos];
      
      // Handle escape sequences - don't skip them as punctuation
      if (current == '\\' && pos + 1 < s.size()) {
        break;  // This is the start of a word with escaped characters
      }
      
      // If not punctuation, we've found the start of a word
      if (!punct_bitmap[static_cast<unsigned char>(current)]) {
        break;  // Found non-punctuation character - start of word
      }
      
      // Skip regular punctuation
      pos++;
    }
    
    if (pos >= s.size()) break;
    
    // Found start of a word
    size_t word_start = pos;
    std::string escaped_word;
    bool has_escaped_chars = false;
    
    // Process the word
    while (pos < s.size()) {
      char current = s[pos];
      
      // Handle escape sequences
      if (current == '\\' && pos + 1 < s.size()) {
        // Next character is escaped - treat as literal
        has_escaped_chars = true;
        if (escaped_word.empty()) {
          // First escaped char - copy everything so far
          escaped_word = std::string(s.substr(word_start, pos - word_start));
        }
        escaped_word += s[pos + 1];  // Add the escaped character
        pos += 2;  // Skip both '\' and the escaped character
        continue;
      }
      
      // Check if current character is punctuation
      if (punct_bitmap[static_cast<unsigned char>(current)]) {
        break;  // End of word
      }
      
      // Regular character - add to escaped word if we're building one
      if (has_escaped_chars) {
        escaped_word += current;
      }
      
      pos++;
    }
    
    // Create word entry
    LexerOutput::Word word;
    word.location.start = word_start;
    word.location.end = pos;
    
    if (has_escaped_chars) {
      // Store escaped word and reference it
      output.escaped_words_.push_back(std::move(escaped_word));
      word.word = output.escaped_words_.back();
    } else {
      // Use string_view of original text
      word.word = s.substr(word_start, pos - word_start);
    }
    
    output.words_.push_back(word);
  }
  
  // Process words for case conversion and other operations
  if (!output.words_.empty()) {
    // Extract words as strings for further processing
    std::vector<std::string> word_strings;
    word_strings.reserve(output.words_.size());
    
    // Set up positions if tracking is enabled
    if (output.has_positions_) {
      output.positions_.reserve(output.words_.size());
    }
    
    for (size_t i = 0; i < output.words_.size(); ++i) {
      word_strings.push_back(std::string(output.words_[i].word));
      if (output.has_positions_) {
        output.positions_.push_back(i);
      }
    }
    
    // Apply case conversion if enabled
    if (case_conversion_enabled_) {
      ApplyCaseConversion(word_strings);
    }
    
    // TODO: Apply stop word removal (when implemented)
    // if (!stop_words_.empty()) {
    //   ApplyStopWordRemoval(word_strings, output.positions_);
    // }
    
    // TODO: Apply stemming (when implemented)
    // if (stemming_enabled_) {
    //   ApplyStemming(word_strings);
    // }
    
    // Store processed terms
    output.processed_terms_ = std::move(word_strings);
  }
  
  return output;
}

const std::vector<LexerOutput::Word>& LexerOutput::GetWords() const {
  return words_;
}

const std::vector<std::string>& LexerOutput::GetProcessedTerms() const {
  return processed_terms_;
}

const std::vector<size_t>& LexerOutput::GetPositions() const {
  return positions_;
}

bool LexerOutput::HasPositions() const {
  return has_positions_;
}

}  // namespace valkey_search::text
