#pragma once
#include <string>
#include <vector>

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
  /// Initialize ICU for module-wide usage (call once at module startup)
  static void Initialize();

  /// Basic case folding for case-insensitive search
  /// Currently implemented using ICU UnicodeString::foldCase()
  /// @param text Input text to fold
  /// @return Case-folded text string
  static std::string CaseFold(absl::string_view text);

  // Planned multi-language support APIs (ICU-backed, not implemented yet)
  // These show reviewers exactly which ICU functionality we will use

  /// Unicode normalization for consistent text comparison across languages
  /// Uses ICU Normalizer2 for diacritic handling and text standardization
  /// @param text Input text to normalize
  /// @param form Normalization form (NFC, NFKC, NFD, NFKD)
  /// @return Normalized text string
  /// @example Normalize("résumé", NormalizationForm::NFD) removes diacritics
  static std::string Normalize(absl::string_view text, NormalizationForm form);

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

}  // namespace valkey_search::indexes::text
