/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_LANGUAGE_H_
#define VALKEY_SEARCH_INDEXES_TEXT_LANGUAGE_H_

#include <bitset>
#include <cstdint>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/unicode_normalizer.h"
#include "src/utils/scanner.h"
#include "unicode/uniset.h"
#include "unicode/utypes.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes::text {

// Base ASCII punctuation used as the default for English and
// LANGUAGE_UNSPECIFIED, and for attribute alias validation.
inline const std::string kAsciiPunctuation = ",.<>{}[]\"':;!@#$%^&*()-+=~/\\|?";

// Punctuation lookup set. ASCII code points use a bitset; non-ASCII code
// points (e.g. Arabic ، U+060C) use a hash set.
struct PunctuationSet {
  std::bitset<128> ascii;                   // Code points 0x00..0x7F
  absl::flat_hash_set<uint32_t> non_ascii;  // Code points >= 0x80

  bool Contains(uint32_t cp) const {
    if (utils::Scanner::IsAscii(cp)) return ascii[cp];
    return non_ascii.contains(cp);
  }
};

// Build a PunctuationSet from a punctuation string. Iterates as code points
// (not bytes) so multi-byte chars like U+060C are stored correctly.
// ASCII whitespace/control characters and Unicode White_Space code points are
// always included as word boundaries.
inline PunctuationSet BuildPunctuationSet(const std::string& punctuation) {
  PunctuationSet result;
  // ASCII whitespace and control characters (0x00..0x7F).
  for (int i = 0; i < 128; ++i) {
    if (std::isspace(static_cast<unsigned char>(i)) ||
        std::iscntrl(static_cast<unsigned char>(i))) {
      result.ascii.set(i);
    }
  }

  // Non-ASCII Unicode White_Space code points (NBSP U+00A0, NNBSP U+202F,
  // typographic spaces U+2000..U+200A, ideographic space U+3000, etc.).
  // Sourced from ICU's property data via UnicodeSet so it stays in sync with
  // the linked Unicode version without maintaining a hand-coded list.
  UErrorCode ec = U_ZERO_ERROR;
  icu::UnicodeSet ws(UNICODE_STRING_SIMPLE("[\\p{White_Space}]"), ec);
  CHECK(U_SUCCESS(ec)) << "ICU UnicodeSet for White_Space failed: "
                       << u_errorName(ec);
  for (int32_t i = 0; i < ws.getRangeCount(); ++i) {
    UChar32 start = ws.getRangeStart(i);
    UChar32 end = ws.getRangeEnd(i);
    for (UChar32 cp = start; cp <= end; ++cp) {
      if (cp >= 0x80) {
        result.non_ascii.insert(static_cast<uint32_t>(cp));
      }
    }
  }

  // Language-specific punctuation characters from the punctuation string.
  utils::Scanner scanner(punctuation);
  utils::Scanner::Char cp;
  while ((cp = scanner.NextUtf8()) != utils::Scanner::kEOF) {
    if (utils::Scanner::IsAscii(cp)) {
      result.ascii.set(cp);
    } else {
      result.non_ascii.insert(cp);
    }
  }
  return result;
}

constexpr size_t kInProgressStemVariantsInlineCapacity = 4;

using InProgressStemMap = absl::flat_hash_map<
    std::string,
    absl::InlinedVector<std::string, kInProgressStemVariantsInlineCapacity>>;

/// Abstract interface for stemming.
///
/// Provides direct access to stemming operations for query expansion,
/// delete path, and stem map building during ingestion.
///
/// Concrete implementations: SnowballStemFilter (Snowball algorithm for
/// European languages).
class Stemmer {
 public:
  virtual ~Stemmer() = default;

  /// Compute the stem root of a token.
  /// Returns the input unchanged if the word is too short to stem.
  virtual std::string GetStemRoot(absl::string_view token,
                                  uint32_t min_stem_size = 0) const = 0;

  /// Build stem map from already-processed tokens.
  /// For each token, if its stem differs from the original, adds the mapping
  /// stem_root -> original_token.
  virtual void BuildStemMap(const std::vector<std::string>& tokens,
                            uint32_t min_stem_size,
                            InProgressStemMap& stem_mappings) const = 0;
};

/// Abstract interface for language-specific text processing behavior.
///
/// Each supported language implements this interface to provide its own
/// punctuation rules, stop words, normalization, stemming, and tokenization.
/// Callers program against Language* — concrete type selection happens at
/// index creation time.
class Language {
 public:
  virtual ~Language() = default;

  /// Returns the protobuf enum identifying this language.
  virtual data_model::Language Id() const = 0;

  /// Returns the lowercase language name (e.g., "english", "french").
  virtual absl::string_view Name() const = 0;

  /// Default punctuation characters used as word boundaries.
  virtual const std::string& GetDefaultPunctuation() const = 0;

  /// Default stop words filtered out during tokenization.
  virtual const std::vector<std::string>& GetDefaultStopWords() const = 0;

  /// Unicode normalization form (NFC for most languages, NFKC for Arabic).
  virtual NormalizationForm GetNormalizationForm() const = 0;

  /// ICU locale for case folding. Empty string means generic Unicode folding.
  virtual absl::string_view CaseFoldLocale() const = 0;

  /// Full ingestion pipeline: segment + normalize + stop word removal.
  virtual absl::StatusOr<std::vector<std::string>> Tokenize(
      absl::string_view text) const = 0;

  /// Tokenize and build stem map in one pass (ingestion with stemming).
  virtual absl::StatusOr<std::vector<std::string>> TokenizeWithStemMap(
      absl::string_view text, uint32_t min_stem_size,
      InProgressStemMap& stem_mappings) const = 0;

  /// Returns true if the codepoint is a word boundary in query text.
  virtual bool IsQueryDelimiter(uint32_t codepoint) const = 0;

  /// Direct access to the punctuation set for hot-loop usage in the filter
  /// parser. Avoids virtual dispatch per character.
  virtual const PunctuationSet& GetPunctuationSet() const = 0;

  /// Unicode normalization + case fold on a single token in place.
  virtual void NormalizeInPlace(std::string& token) const = 0;

  /// Returns true if the word is a stop word (input must be normalized).
  virtual bool IsStopWord(absl::string_view word) const = 0;

  /// Returns the stemmer, or nullptr if this language has no stemming.
  virtual Stemmer* GetStemmer() const = 0;

  /// Whether this language is usable with the current module version.
  virtual bool IsSupported() const = 0;

  /// Minimum module version required to use this language.
  virtual vmsdk::ValkeyVersion MinRequiredVersion() const = 0;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_LANGUAGE_H_
