#pragma once
#include <bitset>
#include <cstdint>
#include <string>
#include <vector>

#include "absl/functional/function_ref.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace valkey_search::indexes::text {

// Unicode normalization forms for multi-language text processing
enum class NormalizationForm {
  NFC,   // Canonical decomposition, then canonical composition
  NFKC,  // Compatibility decomposition, then canonical composition
  NFD,   // Canonical decomposition
  NFKD   // Compatibility decomposition
};

class UnicodeNormalizer {
 public:
  /// Performs Unicode case folding in-place on an existing string.
  /// Minimizes heap allocations by reusing the provided string's buffer.
  /// This is preferred for high-throughput tokenization loops.
  /// @param str The string to be modified; its capacity is reused while its
  /// content is replaced with the folded version.
  static void CaseFoldInPlace(std::string& str);

  /// Unicode normalization for consistent text comparison across languages.
  /// Uses ICU Normalizer2 (UTF-8-native normalizeUTF8) for diacritic handling
  /// and text standardization, e.g. so canonically-equivalent forms compare
  /// equal.
  /// @param text Input text. Precondition: well-formed UTF-8. The ICU UTF-8
  ///   APIs do not substitute U+FFFD for malformed input, so callers must
  ///   validate/sanitize upstream (the tokenization and query paths do).
  /// @param form Normalization form (NFC, NFKC, NFD, NFKD)
  /// @return Normalized text string
  /// @example Normalize("résumé", NormalizationForm::NFD) decomposes diacritics
  static std::string Normalize(absl::string_view text, NormalizationForm form);

  /// In-place variant of Normalize for per-token use: leaves `text` untouched,
  /// with no allocation, when it is already in `form` (the common case).
  static void NormalizeInPlace(std::string& text, NormalizationForm form);

  /// Returns `text` with malformed UTF-8 replaced by U+FFFD, one per maximal
  /// invalid subsequence (ICU's UnicodeString::fromUTF8 policy). This is the
  /// conversion 1.2 applied to every non-ASCII query term, so the < 1.3.0
  /// tolerate path uses it to reproduce 1.2 exactly. Well-formed input is
  /// returned unchanged.
  static std::string ReplaceInvalidUtf8(absl::string_view text);

  /// Calls `fn(cp, normalized)` for every code point that `form` changes, with
  /// its normalized form as UTF-8. Lets a caller close a set of code points
  /// under normalization. Enumerates only code points ICU marks as possibly
  /// changing (the form's quick-check property), not all of Unicode.
  static void ForEachChangedByNormalization(
      NormalizationForm form,
      absl::FunctionRef<void(uint32_t cp, absl::string_view normalized)> fn);

  // Planned multi-language support APIs (declared but not yet implemented).
  // These show reviewers exactly which ICU functionality later tasks will use.

  /// Word boundary detection for CJK and complex script languages
  /// Uses ICU BreakIterator with built-in dictionaries (cjdict.dict ~2MB for
  /// CJK) Handles Chinese, Japanese, Korean word segmentation without spaces
  /// @param text Input text for word segmentation
  /// @param locale Language locale (e.g., "zh", "ja", "ko", "" for auto-detect)
  /// @return Vector of word boundary positions
  /// @example FindWordBoundaries("北京大学", "zh") returns positions for "北京"
  /// + "大学"
  static std::vector<size_t> FindWordBoundaries(absl::string_view text,
                                                const std::string& locale = "");

  /// Locale-aware case folding for language-specific rules
  /// Handles special cases like Turkish i/İ distinction
  /// @param text Input text to fold
  /// @param locale Language locale for locale-specific rules
  /// @return Locale-aware case-folded text
  /// @example LocaleAwareCaseFold("İSTANBUL", "tr") handles Turkish correctly
  static std::string LocaleAwareCaseFold(absl::string_view text,
                                         const std::string& locale);
};

/// Concrete normalizer: Unicode normalization + case folding.
///
/// Applies the configured normalization form followed by case folding
/// (generic, or locale-aware when a locale is set). An ASCII token takes a
/// fast path of plain ASCII lowercasing when that gives the same result, i.e.
/// when it contains no ASCII character the locale lowercases differently
/// (Turkish: `I` -> `ı`).
class NormalizeCaseFoldFilter {
 public:
  explicit NormalizeCaseFoldFilter(
      NormalizationForm form = NormalizationForm::NFC,
      const std::string& locale = "");

  void NormalizeInPlace(std::string& token) const;

  NormalizationForm GetNormalizationForm() const { return norm_form_; }
  const std::string& GetLocale() const { return locale_; }

 private:
  bool CanLowerAsAscii(absl::string_view token) const;

  NormalizationForm norm_form_;
  std::string locale_;
  // ASCII characters whose lowercase under `locale_` differs from ASCII
  // lowercasing. Derived from ICU at construction; empty without a locale.
  std::bitset<128> locale_sensitive_ascii_;
};

}  // namespace valkey_search::indexes::text
