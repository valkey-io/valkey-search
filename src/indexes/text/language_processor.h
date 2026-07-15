/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_LANGUAGE_PROCESSOR_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_LANGUAGE_PROCESSOR_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/functional/function_ref.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/punctuation.h"
#include "src/indexes/text/unicode_normalizer.h"

namespace valkey_search::indexes::text {

// ============================================================================
// Interfaces
// ============================================================================

/// Abstract interface for word boundary detection.
///
/// A Segmenter converts a chunk of text into multiple tokens (1→many).
/// It is the "cut" step — it splits raw text into tokens without applying
/// any normalization, filtering, or stop-word removal.
///
/// Escape contract: Segmenters handle `\<char>` in the input text as "include
/// <char> literally, do not treat it as a word boundary." This supports the
/// query language escape convention.
class Segmenter {
 public:
  virtual ~Segmenter() = default;

  /// Segment a single UTF-8 input string into tokens.
  /// Returns an error if the input contains invalid UTF-8.
  virtual absl::StatusOr<std::vector<std::string>> Segment(
      absl::string_view text) const = 0;
};

/// Abstract interface for a single post-segmentation processing step.
///
/// A TokenFilter mutates a single token in place and returns whether the token
/// should be kept (true) or dropped (false). It operates on individual tokens
/// in isolation — it has no access to the surrounding token list.
///
/// TokenFilters are stateless and idempotent.
///
/// Examples:
///   - NormalizeCaseFoldFilter: normalizes and lowercases a token (always
///   keeps)
///   - StopWordFilter: drops stop words, keeps everything else unchanged
///   - SnowballStemFilter: applies Snowball stemming (always keeps)
class TokenFilter {
 public:
  virtual ~TokenFilter() = default;

  /// Apply this filter to a single token in place.
  /// Returns true if the token should be kept, false if it should be removed.
  /// The token string may be mutated regardless of the return value.
  virtual bool Apply(std::string &token) const = 0;
};

/// Abstract interface for text normalization, extending TokenFilter.
///
/// A Normalizer is a TokenFilter that performs Unicode normalization and
/// case folding. It is exposed via LanguageProcessor::GetNormalizer() for
/// O(1) access by callers that need normalization independent of the full
/// pipeline (e.g., wildcard/fuzzy predicates).
///
/// Concrete implementations: NormalizeCaseFoldFilter (NFC/NFKC + case fold).
class Normalizer : public TokenFilter {
 public:
  ~Normalizer() override = default;

  /// Normalize a token in place. Equivalent to Apply() but expresses the
  /// intent that normalization never drops tokens.
  virtual void NormalizeInPlace(std::string &token) const = 0;
};

// Inline capacity for per-document stem mapping
constexpr size_t kInProgressStemVariantsInlineCapacity = 4;

// Per-document stem mappings: stemmed_word -> list of original words that stem
// to it
using InProgressStemMap = absl::flat_hash_map<
    std::string,
    absl::InlinedVector<std::string, kInProgressStemVariantsInlineCapacity>>;

/// Abstract interface for stemming, extending TokenFilter.
///
/// A Stemmer is a TokenFilter that also provides direct access to stemming
/// operations for use outside the pipeline (delete path, query expansion,
/// stem map building).
///
/// Concrete implementations: SnowballStemFilter (Snowball algorithm for
/// European languages).
class Stemmer : public TokenFilter {
 public:
  ~Stemmer() override = default;

  /// Compute the stem root of a token.
  /// Used by delete path and query expansion for stem tree lookups.
  /// Returns the input unchanged if the word is too short to stem.
  virtual std::string GetStemRoot(absl::string_view token,
                                  uint32_t min_stem_size = 0) const = 0;

  /// Build stem map from already-processed tokens.
  /// Ingestion-specific: maps stem roots to their original surface forms.
  /// For each token, if its stem differs from the original, adds the mapping
  /// stem_root -> original_token.
  virtual void BuildStemMap(const std::vector<std::string> &tokens,
                            uint32_t min_stem_size,
                            InProgressStemMap &stem_mappings) const = 0;
};

/// Abstract interface for query-path tokenization strategy.
///
/// The parser uses this to extract tokens from raw text spans between
/// query syntax characters. Two strategies exist:
///   - Punctuation-based (European): walk codepoints, break on punctuation
///   - Segmenter-delegated (CJK): extract span, delegate to Segment()
///
/// The parser handles query-syntax detection (|, @, (, ), ", *, %, etc.)
/// and delegates word-boundary logic and escape handling to this interface.
///
/// Standalone, testable, stateless — consistent with Segmenter/TokenFilter.
class QueryTokenizer {
 public:
  virtual ~QueryTokenizer() = default;

  struct Token {
    std::string content;    // the extracted token text (escapes resolved)
    size_t bytes_consumed;  // total bytes consumed from text[pos] onward
  };

  /// Extract the next token from quoted text starting at `text[pos]`.
  ///
  /// In quoted context, the only external boundary is the closing `"`.
  /// The tokenizer handles word-boundary detection, escape resolution, and
  /// invalid UTF-8 replacement within the quoted text span.
  ///
  /// The tokenizer MUST stop (without consuming) when it sees `"`.
  ///
  /// Returns:
  ///   - A Token with non-empty content if a token was extracted
  ///   - A Token with empty content if only punctuation/skippable bytes were
  ///     consumed (bytes_consumed > 0 tells the caller to advance)
  ///   - nullopt if nothing could be consumed (pos is at `"` or end)
  ///   - Error status for malformed escape sequences
  virtual absl::StatusOr<std::optional<Token>> NextQuotedToken(
      absl::string_view text, size_t pos) const = 0;

  /// Extract the next token from unquoted text starting at `text[pos]`.
  ///
  /// In unquoted context, the caller has already handled query syntax chars
  /// (|, @, (, ), ", *, %, -, {, }, [, ], :, ;, $). The tokenizer handles
  /// word-boundary detection and escape resolution for the remaining
  /// text-content codepoints.
  ///
  /// The tokenizer MUST stop (without consuming) when it encounters any
  /// character for which `IsQuerySyntax` returns true.
  ///
  /// `hit_query_syntax` is set to true if the tokenizer stopped because it
  /// encountered a query-syntax character.
  ///
  /// Returns:
  ///   - A Token with non-empty content if a token was extracted
  ///   - A Token with empty content + bytes_consumed > 0 if only
  ///     punctuation/skippable bytes were consumed
  ///   - nullopt if nothing could be consumed (pos at query syntax or end)
  ///   - Error status for malformed escape sequences
  virtual absl::StatusOr<std::optional<Token>> NextUnquotedToken(
      absl::string_view text, size_t pos, bool &hit_query_syntax,
      absl::FunctionRef<bool(uint32_t cp)> is_query_syntax) const = 0;
};

// ============================================================================
// Language-agnostic concrete implementations
// ============================================================================

/// Segmenter that splits text on punctuation/whitespace boundaries.
///
/// Used by Snowball languages (English, French, German, etc.) where word
/// boundaries are defined by punctuation and whitespace characters.
///
/// Handles backslash escapes: `\<char>` means "include <char> literally,
/// don't treat it as punctuation."
class PunctuationSegmenter : public Segmenter {
 public:
  explicit PunctuationSegmenter(const std::string &punctuation);
  explicit PunctuationSegmenter(PunctuationSet punct_set);

  absl::StatusOr<std::vector<std::string>> Segment(
      absl::string_view text) const override;

  /// Returns true if the given code point is treated as a word boundary
  /// (punctuation) by this segmenter. Used by PunctuationQueryTokenizer and
  /// internally by Segment().
  bool IsPunctuation(uint32_t cp) const;

 private:
  PunctuationSet punct_set_;
};

/// Concrete Normalizer that performs Unicode normalization and case folding.
///
/// For ASCII-only tokens, applies simple ASCII lowercasing (unless a locale
/// is configured that requires special handling, e.g. Turkish).
/// For tokens containing non-ASCII characters, applies the configured
/// Unicode normalization form (NFC or NFKC) followed by Unicode case folding.
///
/// When a locale is set, locale-aware case folding is used instead of generic
/// Unicode case folding. This is required for Turkish where I→ı and İ→i
/// (dotted/dotless I) mappings differ from the default Unicode rules.
class NormalizeCaseFoldFilter : public Normalizer {
 public:
  explicit NormalizeCaseFoldFilter(
      NormalizationForm form = NormalizationForm::NFC,
      const std::string &locale = "");

  bool Apply(std::string &token) const override;
  void NormalizeInPlace(std::string &token) const override;

  /// Get the normalization form used by this filter.
  NormalizationForm GetNormForm() const { return norm_form_; }

 private:
  NormalizationForm norm_form_;
  std::string locale_;  // Empty = generic folding; "tr" = Turkish-aware
};

/// TokenFilter that removes stop words.
///
/// Returns false for tokens that are in the configured stop word set,
/// effectively removing them from the token stream.
class StopWordFilter : public TokenFilter {
 public:
  explicit StopWordFilter(const std::vector<std::string> &stop_words);
  explicit StopWordFilter(absl::flat_hash_set<std::string> stop_words_set);

  bool Apply(std::string &token) const override;

  /// Returns true if the given word is a stop word.
  bool IsStopWord(absl::string_view word) const;

 private:
  absl::flat_hash_set<std::string> stop_words_set_;
};

/// Punctuation-based query tokenizer for European/Snowball languages.
///
/// Walks codepoints one at a time, breaking on punctuation characters.
/// This contains the exact escape-handling and word-boundary logic that was
/// previously inline in the filter parser's token extraction methods.
///
/// Escape contract (mirrors PunctuationSegmenter::Segment):
///   - `\\` -> include literal `\` in token, continue same token
///   - `\<punctuation>` -> include the punctuation char literally, continue
///   - `\<non-punctuation>` where `\` is punctuation -> end current token
///   - `\<non-punctuation>` where `\` is NOT punctuation -> include char,
///   continue
///   - `\` at end of input -> error
///   - `\` + invalid UTF-8 -> replace with U+FFFD, continue
class PunctuationQueryTokenizer : public QueryTokenizer {
 public:
  explicit PunctuationQueryTokenizer(const PunctuationSegmenter &segmenter)
      : segmenter_(segmenter) {}

  absl::StatusOr<std::optional<Token>> NextQuotedToken(
      absl::string_view text, size_t pos) const override;

  absl::StatusOr<std::optional<Token>> NextUnquotedToken(
      absl::string_view text, size_t pos, bool &hit_query_syntax,
      absl::FunctionRef<bool(uint32_t cp)> is_query_syntax) const override;

 private:
  /// Result of handling a backslash escape.
  enum class EscapeResult {
    kContinue,   // Escape handled, continue building token
    kBreakToken  // Backslash was punctuation before non-punctuation -> end
                 // token
  };

  /// Handles a backslash escape sequence at text[cursor].
  absl::StatusOr<EscapeResult> HandleEscape(absl::string_view text,
                                            size_t &cursor,
                                            std::string &content) const;

  const PunctuationSegmenter &segmenter_;
};

// ============================================================================
// LanguageProcessor — composed pipeline
// ============================================================================

/// Composed pipeline of Segmenters and TokenFilters.
///
/// The LanguageProcessor is a stateless, idempotent composition that owns
/// its segmenters and token filters. It provides:
///   - Process(): full pipeline execution (segment then filter)
///   - Segment(): segmentation only
///   - ApplyFilters(): apply only the filter chain
///   - O(1) accessors for components needed outside the pipeline
///
/// Adding a new language means composing different segmenters/filters via
/// the Create() factory — no interface changes required.
class LanguageProcessor {
 public:
  /// Builder for composing language-specific pipelines without requiring
  /// friend access. New languages use the Builder — no changes to this
  /// header needed.
  class Builder {
   public:
    Builder &AddSegmenter(std::shared_ptr<Segmenter> segmenter);
    Builder &AddFilter(std::shared_ptr<TokenFilter> filter);
    Builder &SetQueryTokenizer(std::shared_ptr<QueryTokenizer> tokenizer);
    Builder &SetNormalizer(std::shared_ptr<Normalizer> normalizer);
    Builder &SetStopWordFilter(std::shared_ptr<StopWordFilter> filter);
    Builder &SetStemmer(std::shared_ptr<Stemmer> stemmer);
    std::shared_ptr<LanguageProcessor> Build();

   private:
    std::shared_ptr<LanguageProcessor> processor_ =
        std::make_shared<LanguageProcessor>();
  };

  virtual ~LanguageProcessor() = default;

  /// Full pipeline: segment then filter. Stateless and idempotent.
  absl::StatusOr<std::vector<std::string>> Process(
      absl::string_view text) const;

  /// Apply segmenters sequentially to text.
  absl::StatusOr<std::vector<std::string>> Segment(
      absl::string_view text) const;

  /// Apply the filter chain to a list of tokens.
  /// Tokens that any filter eliminates are removed from the result.
  std::vector<std::string> ApplyFilters(std::vector<std::string> tokens) const;

  /// Apply all filters to a single token (same chain as ingestion).
  /// Returns true if the token should be kept, false if any filter drops it.
  /// Use for query tokens that need the full filter pipeline without
  /// re-segmentation (e.g., unquoted plain terms).
  bool ProcessWord(std::string &token) const;

  /// Get the normalizer. Always non-null — every language has normalization.
  /// O(1) access.
  Normalizer *GetNormalizer() const { return normalizer_.get(); }

  /// Get the query tokenizer strategy. Used by the filter parser for
  /// determining word boundaries in query text. O(1) access.
  const QueryTokenizer *GetQueryTokenizer() const {
    return query_tokenizer_.get();
  }

  /// Get the stop word filter, or nullptr if this processor has no stop words.
  /// O(1) access. Use for direct stop-word checks on already-normalized
  /// tokens (avoids redundant normalization from ApplyFilters).
  StopWordFilter *GetStopWordFilter() const { return stop_word_filter_.get(); }

  /// Get the stemmer, or nullptr if this processor doesn't support stemming.
  /// O(1) access.
  Stemmer *GetStemmer() const { return stemmer_.get(); }

  /// Factory: create a LanguageProcessor for the given language with
  /// the appropriate segmenter and filter composition.
  static std::shared_ptr<LanguageProcessor> Create(
      data_model::Language language, const std::string &punctuation,
      const std::vector<std::string> &stop_words);

 private:
  std::vector<std::shared_ptr<Segmenter>> segmenters_;
  std::vector<std::shared_ptr<TokenFilter>> filters_;
  std::shared_ptr<QueryTokenizer> query_tokenizer_;

  // O(1) accessors for components used outside the pipeline.
  // Normalizer is also in the filter chain but exposed for direct use
  // (e.g., wildcard/fuzzy normalization without stopword removal).
  std::shared_ptr<Normalizer> normalizer_;
  // Stop word filter is also in the filter chain but exposed for direct
  // IsStopWord() checks on already-normalized tokens in query parser.
  std::shared_ptr<StopWordFilter> stop_word_filter_;
  // Stemmer is not part of the main filter chain — tokens are indexed
  // unstemmed. Nullptr for languages without stemming support.
  std::shared_ptr<Stemmer> stemmer_;
};

}  // namespace valkey_search::indexes::text

#endif  // _VALKEY_SEARCH_INDEXES_TEXT_LANGUAGE_PROCESSOR_H_
