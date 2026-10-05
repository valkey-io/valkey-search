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

  // True if every code point of the UTF-8 `text` is in the set.
  bool ContainsAll(absl::string_view text) const {
    utils::Scanner scanner(text);
    utils::Scanner::Char cp;
    while ((cp = scanner.NextUtf8()) != utils::Scanner::kEOF) {
      if (!Contains(cp)) return false;
    }
    return true;
  }

  void Insert(uint32_t cp) {
    if (utils::Scanner::IsAscii(cp)) {
      ascii.set(cp);
    } else {
      non_ascii.insert(cp);
    }
  }
};

// Which word boundaries a language adds on top of its listed punctuation.
//   kAscii:   ASCII whitespace and control characters only. This is the 1.2
//             tokenization, and matches Redis, so languages available before
//             1.3 (English) keep it.
//   kUnicode: also Unicode White_Space (NBSP, U+3000, ...) and every code point
//             that normalizes to punctuation in the set.
enum class DelimiterScope { kAscii, kUnicode };

// Build a PunctuationSet from a punctuation string. Iterates as code points
// (not bytes) so multi-byte chars like U+060C are stored correctly; listed
// characters are always honored, whatever the scope. ASCII whitespace/control
// characters are always word boundaries. With DelimiterScope::kUnicode, so are
// Unicode White_Space code points, and the set is closed under `form`: a code
// point that normalizes to punctuation is punctuation too (e.g. under NFKC,
// U+FF0C FULLWIDTH COMMA -> ','), so splitting raw text yields the same tokens
// as normalizing first. Tokens can then be normalized one at a time.
inline PunctuationSet BuildPunctuationSet(const std::string& punctuation,
                                          NormalizationForm form,
                                          DelimiterScope scope) {
  PunctuationSet result;
  // ASCII whitespace and control characters (0x00..0x7F).
  for (int i = 0; i < 128; ++i) {
    if (std::isspace(static_cast<unsigned char>(i)) ||
        std::iscntrl(static_cast<unsigned char>(i))) {
      result.ascii.set(i);
    }
  }

  // Language-specific punctuation characters from the punctuation string.
  utils::Scanner scanner(punctuation);
  utils::Scanner::Char cp;
  while ((cp = scanner.NextUtf8()) != utils::Scanner::kEOF) {
    result.Insert(cp);
  }

  if (scope == DelimiterScope::kAscii) {
    return result;
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
    for (UChar32 ws_cp = start; ws_cp <= end; ++ws_cp) {
      if (ws_cp >= 0x80) {
        result.non_ascii.insert(static_cast<uint32_t>(ws_cp));
      }
    }
  }

  // Close under normalization. A normalized form is never itself changed by
  // the form, so the code points added here cannot affect later checks.
  UnicodeNormalizer::ForEachChangedByNormalization(
      form, [&result](uint32_t cp, absl::string_view normalized) {
        if (result.ContainsAll(normalized)) {
          result.Insert(cp);
        }
      });
  return result;
}

// Per-index tokenization settings: the word boundaries and stop words from
// FT.CREATE PUNCTUATION / STOPWORDS, or the language's defaults. Built by
// Language::MakeTokenizerConfig, which applies the language's rules, and owned
// by the text index. The Language itself is shared by every index using it.
struct TokenizerConfig {
  PunctuationSet punct_set;
  absl::flat_hash_set<std::string> stop_words;  // Normalized by the language.

  // `word` must already be normalized.
  bool IsStopWord(absl::string_view word) const {
    return stop_words.contains(word);
  }
};

// Resolves a backslash escape at text[0..] (position AFTER the backslash).
// Both the ingestion tokenizer (SegmentInternal) and the query filter parser
// (HandleBackslashEscape) share this logic so that ASCII and non-ASCII escaped
// punctuation are handled identically.
//
// Returns:
//   > 0 : number of bytes consumed; caller should append text[0..return_value)
//   == 0 : break the current token (backslash acted as word boundary)
inline uint8_t ResolveBackslashEscape(absl::string_view text,
                                      const PunctuationSet& punct) {
  if (text.empty()) return 0;

  uint8_t lead = static_cast<uint8_t>(text[0]);
  if (lead < 0x80) {
    // ASCII: escaped backslash or escaped punctuation → append 1 byte.
    if (lead == '\\' || punct.Contains(lead)) return 1;
    // Non-punct after backslash: if backslash itself is punct → break token.
    return punct.Contains(static_cast<unsigned char>('\\')) ? 0 : 1;
  }

  // Non-ASCII: decode full codepoint so multi-byte punctuation (e.g. Arabic
  // ، U+060C) is recognized — same as the ASCII path above.
  utils::Scanner s(text);
  auto cp = s.NextUtf8();
  uint8_t len = s.LastUtf8ByteLen();
  // A malformed byte is treated as non-punctuation, same as 1.2.
  if (cp != utils::Scanner::kInvalidCp &&
      (cp == '\\' || punct.Contains(static_cast<uint32_t>(cp)))) {
    return len;
  }
  return punct.Contains(static_cast<unsigned char>('\\')) ? 0 : len;
}

// Unit in which word lengths are measured for MINSTEMSIZE and fuzzy edit
// distance.
enum class LengthUnit { kCodePoints, kBytes };

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
  /// Returns the input unchanged if the word is shorter than min_stem_size,
  /// measured in `unit`.
  virtual std::string GetStemRoot(
      absl::string_view token, uint32_t min_stem_size = 0,
      LengthUnit unit = LengthUnit::kCodePoints) const = 0;

  /// Build stem map from already-processed tokens.
  /// For each token, if its stem differs from the original, adds the mapping
  /// stem_root -> original_token.
  virtual void BuildStemMap(const std::vector<std::string>& tokens,
                            uint32_t min_stem_size, LengthUnit unit,
                            InProgressStemMap& stem_mappings) const = 0;
};

/// Abstract interface for language-specific text processing behavior.
///
/// Each supported language implements this interface to provide its own
/// punctuation rules, stop words, normalization, stemming, and tokenization.
/// Callers program against Language* — concrete type selection happens at
/// index creation time.
///
/// Instances are shared (one per language, see LanguageRegistry) and hold no
/// per-index state. Index-specific settings live in a TokenizerConfig that
/// the index owns and passes to Tokenize.
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

  /// Builds an index's tokenization settings from its punctuation and stop
  /// words, applying this language's rules (delimiter scope, normalization of
  /// the stop words).
  virtual TokenizerConfig MakeTokenizerConfig(
      const std::string& punctuation,
      const std::vector<std::string>& stop_words) const = 0;

  /// Full ingestion pipeline: segment + normalize + stop word removal, using
  /// the index's settings.
  virtual absl::StatusOr<std::vector<std::string>> Tokenize(
      absl::string_view text, const TokenizerConfig& config) const = 0;

  /// Tokenize and build stem map in one pass (ingestion with stemming).
  virtual absl::StatusOr<std::vector<std::string>> TokenizeWithStemMap(
      absl::string_view text, const TokenizerConfig& config,
      uint32_t min_stem_size, LengthUnit unit,
      InProgressStemMap& stem_mappings) const = 0;

  /// Unicode normalization + case fold on a single token in place.
  virtual void NormalizeInPlace(std::string& token) const = 0;

  /// Returns the stemmer, or nullptr if this language has no stemming.
  virtual Stemmer* GetStemmer() const = 0;

  /// Minimum module version required to use this language.
  virtual vmsdk::ValkeyVersion MinRequiredVersion() const = 0;

  /// Unit for MINSTEMSIZE and fuzzy edit distance under the current
  /// search.emulate-release (see COMPATIBILITY.md).
  virtual LengthUnit GetLengthUnit() const = 0;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_LANGUAGE_H_
