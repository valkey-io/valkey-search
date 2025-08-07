#ifndef _VALKEY_SEARCH_INDEXES_TEXT_LEXER_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_LEXER_H_

/*

The Lexer takes in a UTF-8 string and outputs a vector of processed terms.
Each word output is decorated with metadata like it's physical location, etc.

1. The lexer treats configurable punctuation as whitespace.
2. The lexer supports escaping of punctuation to force its treatment as a
   natural character that is part of a word.
3. Words are defined as sequences of utf-8 characters separated by whitespace.
4. The lexer converts text to lowercase (configurable).
5. The lexer provides APIs for stop word removal and stemming (future implementation).

*/

#include <string>
#include <unordered_set>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"

namespace valkey_search {
namespace text {

// Forward declaration
struct LexerOutput;

/*

Configuration for Lexer operation. Typically, there's one of these for each
index-field. The lexer handles the text processing pipeline:
1. Tokenization (breaking text into words)
2. Case conversion (converting to lowercase, configurable) 
3. APIs for advanced processing (stemming, stop words) for future implementation

*/

struct Lexer {
  //
  // Set/Get current punctuation characters.
  //
  // Since every scanned input character is subject to this table, we want to
  // make the implementation efficient, so by restricting punctuation characters
  // to ASCII characters a bitmap implementation becomes feasible and is
  // preferred.
  //
  // Rejected if we find some non-ASCII characters
  absl::Status SetPunctuation(const std::string& new_punctuation);

  std::string GetPunctuation() const;

  // Enable/disable case conversion (enabled by default)
  void SetCaseConversionEnabled(bool enabled);
  
  bool IsCaseConversionEnabled() const;
  
  // Set/get stop words
  void SetStopWords(const std::vector<std::string>& stop_words);
  const std::unordered_set<std::string>& GetStopWords() const;
  
  // Enable/disable stemming (enabled by default)
  void SetStemmingEnabled(bool enabled);
  bool IsStemmingEnabled() const;
  
  // Set/get minimum stem size
  void SetMinStemSize(uint32_t size);
  uint32_t GetMinStemSize() const;

  // Set/get language for stemming
  void SetLanguage(data_model::Language language);
  data_model::Language GetLanguage() const;

  // Instantiate with schema proto parameters
  explicit Lexer(const data_model::IndexSchema& schema_proto);
  
  // Instantiate with text index and schema protos (field-specific configuration)
  Lexer(const data_model::TextIndex& text_index_proto,
        const data_model::IndexSchema& schema_proto);

  //
  // Process an input string - performs text processing:
  // 1. Tokenization
  // 2. Case conversion (if enabled)
  // 3. Future: Stop word removal and stemming (when implemented)
  //
  // May fail if there are non-UTF-8 characters present.
  //
  absl::StatusOr<LexerOutput> ProcessString(absl::string_view s) const;

 private:
  // Apply case conversion to words (convert to lowercase)
  void ApplyCaseConversion(std::vector<std::string>& words) const;
  
  // Apply stop word removal - filters out common words
  void ApplyStopWordRemoval(std::vector<std::string>& words, std::vector<size_t>& positions) const;
  
  // Apply stemming - reduces words to their root form
  void ApplyStemming(std::vector<std::string>& words) const;
  
  // Helper to apply Porter stemming algorithm
  std::string ApplyPorterStemming(const std::string& word) const;
  
  // Configure lexer from schema proto
  void ConfigureFromSchemaProto(const data_model::IndexSchema& schema_proto);
  
  // Apply field-specific overrides
  void ApplyFieldOverrides(const data_model::TextIndex& text_index_proto);
  
  std::string punctuation_;
  bool case_conversion_enabled_;
  std::unordered_set<std::string> stop_words_;
  bool stemming_enabled_;
  uint32_t min_stem_size_;
  data_model::Language language_;
};

//
// The primary output of the lexer. most words are string-views of the original
// input string. But for escaped words, the actual storage will be within this
// object. Hence clients of this object don't have to worry about whether a word
// is escaped or not. The position of a word is the index into the word vector.
//
struct LexerOutput {
  struct Word {
    std::string_view word;
    struct Location {
      unsigned start;  // Byte Offset of start within original text
      unsigned end;    // Byte Offset of end+1
    } location;
  };

  const std::vector<Word>& GetWords() const;
  
  // Get the processed terms after all text processing steps
  const std::vector<std::string>& GetProcessedTerms() const;
  
  // Get term positions, only valid if positions are tracked
  const std::vector<size_t>& GetPositions() const;
  
  // Check if positions are tracked
  bool HasPositions() const;

 private:
  friend struct Lexer;
  
  // Storage for escaped words
  std::vector<std::string> escaped_words_;
  
  // Vector of words
  std::vector<Word> words_;
  
  // Final processed terms after all processing steps
  std::vector<std::string> processed_terms_;
  
  // Term positions
  std::vector<size_t> positions_;
  
  // Whether positions are tracked
  bool has_positions_ = false;
};

}  // namespace text
}  // namespace valkey_search

#endif
