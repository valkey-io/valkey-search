#include "src/indexes/text/unicode_normalizer.h"

#include <cstdint>
#include <limits>
#include <string>

#include "absl/algorithm/container.h"
#include "absl/log/check.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "unicode/bytestream.h"
#include "unicode/casemap.h"
#include "unicode/normalizer2.h"
#include "unicode/stringpiece.h"
#include "unicode/uniset.h"
#include "unicode/unistr.h"
#include "unicode/utypes.h"

namespace valkey_search::indexes::text {

namespace {

// Map a NormalizationForm to ICU's shared, immutable, thread-safe Normalizer2
// singleton. ICU owns these instances; callers must not delete, copy, or cache
// their own. A failure status / null result means the ICU normalization data is
// not linked, which is a build problem (see Normalize()).
const icu::Normalizer2 *InstanceFor(NormalizationForm form, UErrorCode &ec) {
  switch (form) {
    case NormalizationForm::NFC:
      return icu::Normalizer2::getNFCInstance(ec);
    case NormalizationForm::NFD:
      return icu::Normalizer2::getNFDInstance(ec);
    case NormalizationForm::NFKC:
      return icu::Normalizer2::getNFKCInstance(ec);
    case NormalizationForm::NFKD:
      return icu::Normalizer2::getNFKDInstance(ec);
  }
  return nullptr;  // Unreachable: all enum values are handled above.
}

// Code points that `form` may change: those not "Yes" in its quick-check
// property.
const char *QuickCheckNotYesPattern(NormalizationForm form) {
  switch (form) {
    case NormalizationForm::NFC:
      return "[:^NFC_QC=Yes:]";
    case NormalizationForm::NFD:
      return "[:^NFD_QC=Yes:]";
    case NormalizationForm::NFKC:
      return "[:^NFKC_QC=Yes:]";
    case NormalizationForm::NFKD:
      return "[:^NFKD_QC=Yes:]";
  }
  return nullptr;  // Unreachable: all enum values are handled above.
}

icu::StringPiece ToStringPiece(absl::string_view s) {
  // ICU's StringPiece length is int32_t. Guard against silent truncation of an
  // oversized input rather than passing a wrong length into ICU. Not reachable
  // with real query/document sizes; a hit indicates a caller-side problem.
  CHECK_LE(s.size(), static_cast<size_t>(std::numeric_limits<int32_t>::max()))
      << "Text too large for ICU (" << s.size() << " bytes)";
  return icu::StringPiece(s.data(), static_cast<int32_t>(s.size()));
}

}  // namespace

std::string UnicodeNormalizer::Normalize(absl::string_view text,
                                         NormalizationForm form) {
  if (text.empty()) {
    return std::string();
  }

  UErrorCode ec = U_ZERO_ERROR;
  const icu::Normalizer2 *normalizer = InstanceFor(form, ec);
  // A failure or null instance here means ICU normalization data is not linked.
  // With our static data packaging this can only happen in a broken build, not
  // from user input, so fail loudly rather than silently skip normalization.
  CHECK(U_SUCCESS(ec) && normalizer != nullptr)
      << "ICU Normalizer2 unavailable (static data not linked?): "
      << u_errorName(ec);

  std::string out;
  icu::StringByteSink<std::string> sink(&out);
  // normalizeUTF8 consumes UTF-8 and writes UTF-8 to the sink. ICU performs any
  // UTF-16 work internally and only on spans that actually need normalizing, so
  // there is no manual UnicodeString round-trip on our side.
  //
  // NOTE: the NFx normalizeUTF8 fast path does NOT substitute U+FFFD for
  // malformed input (it decodes via a trie that yields an error value and
  // advances). Callers that must tolerate malformed bytes are expected to
  // sanitize first (UnicodeNormalizer::ReplaceInvalidUtf8). On well-formed
  // input this never sets an error code.
  normalizer->normalizeUTF8(/*options=*/0, ToStringPiece(text), sink,
                            /*edits=*/nullptr, ec);
  // A failure here (e.g. out-of-memory) is unrecoverable and unrelated to the
  // input bytes; returning the un-normalized string would silently desync the
  // index and query paths and corrupt search results.
  CHECK(U_SUCCESS(ec)) << "ICU normalizeUTF8 failed: " << u_errorName(ec);

  return out;
}

void UnicodeNormalizer::NormalizeInPlace(std::string &text,
                                         NormalizationForm form) {
  UErrorCode ec = U_ZERO_ERROR;
  const icu::Normalizer2 *normalizer = InstanceFor(form, ec);
  CHECK(U_SUCCESS(ec) && normalizer != nullptr)
      << "ICU Normalizer2 unavailable (static data not linked?): "
      << u_errorName(ec);
  if (normalizer->isNormalizedUTF8(ToStringPiece(text), ec)) {
    return;
  }
  CHECK(U_SUCCESS(ec)) << "ICU isNormalizedUTF8 failed: " << u_errorName(ec);
  text = Normalize(text, form);
}

std::string UnicodeNormalizer::ReplaceInvalidUtf8(absl::string_view text) {
  std::string out;
  icu::UnicodeString::fromUTF8(ToStringPiece(text)).toUTF8String(out);
  return out;
}

void UnicodeNormalizer::ForEachChangedByNormalization(
    NormalizationForm form,
    absl::FunctionRef<void(uint32_t cp, absl::string_view normalized)> fn) {
  UErrorCode ec = U_ZERO_ERROR;
  icu::UnicodeSet candidates(
      icu::UnicodeString(QuickCheckNotYesPattern(form), -1, US_INV), ec);
  CHECK(U_SUCCESS(ec)) << "ICU UnicodeSet for quick-check failed: "
                       << u_errorName(ec);
  std::string original;
  for (int32_t i = 0; i < candidates.getRangeCount(); ++i) {
    for (UChar32 cp = candidates.getRangeStart(i);
         cp <= candidates.getRangeEnd(i); ++cp) {
      original.clear();
      icu::UnicodeString(cp).toUTF8String(original);
      std::string normalized = Normalize(original, form);
      if (normalized != original) {
        fn(static_cast<uint32_t>(cp), normalized);
      }
    }
  }
}

void UnicodeNormalizer::CaseFoldInPlace(std::string &str) {
  // UTF-8-native case folding via CaseMap::utf8Fold (no manual UTF-16
  // round-trip). Like normalizeUTF8, this does not substitute U+FFFD for
  // malformed input; callers needing that must sanitize first
  // (UnicodeNormalizer::ReplaceInvalidUtf8).
  std::string out;
  icu::StringByteSink<std::string> sink(&out);
  UErrorCode ec = U_ZERO_ERROR;
  icu::CaseMap::utf8Fold(/*options=*/0, ToStringPiece(str), sink,
                         /*edits=*/nullptr, ec);
  CHECK(U_SUCCESS(ec)) << "ICU utf8Fold failed: " << u_errorName(ec);
  str = std::move(out);
}

std::string UnicodeNormalizer::LocaleAwareCaseFold(absl::string_view text,
                                                   const std::string &locale) {
  if (text.empty()) {
    return std::string();
  }

  // Turkish (and Azerbaijani) require locale-aware case mapping because:
  //   - U+0049 (I) must map to U+0131 (ı) — dotless lowercase i
  //   - U+0130 (İ) must map to U+0069 (i) — regular lowercase i
  // Generic Unicode case folding maps both I and İ to i, which breaks
  // Turkish text retrieval (queries for "ılık" won't match "ILIK").
  //
  // We use ICU's utf8ToLower with the Turkish locale rather than utf8Fold,
  // because utf8Fold is locale-independent by design (Unicode CaseFolding.txt
  // does not have Turkish-specific mappings).
  std::string out;
  icu::StringByteSink<std::string> sink(&out);
  UErrorCode ec = U_ZERO_ERROR;
  icu::CaseMap::utf8ToLower(locale.c_str(), /*options=*/0, ToStringPiece(text),
                            sink, /*edits=*/nullptr, ec);
  CHECK(U_SUCCESS(ec)) << "ICU utf8ToLower failed for locale '" << locale
                       << "': " << u_errorName(ec);
  return out;
}

// ============================================================================
// NormalizeCaseFoldFilter
// ============================================================================

NormalizeCaseFoldFilter::NormalizeCaseFoldFilter(NormalizationForm form,
                                                 const std::string &locale)
    : norm_form_(form), locale_(locale) {
  if (locale_.empty()) {
    return;
  }
  for (int c = 0; c < 128; ++c) {
    const std::string ch(1, static_cast<char>(c));
    if (UnicodeNormalizer::LocaleAwareCaseFold(ch, locale_) !=
        std::string(1, absl::ascii_tolower(static_cast<unsigned char>(c)))) {
      locale_sensitive_ascii_.set(c);
    }
  }
}

bool NormalizeCaseFoldFilter::CanLowerAsAscii(absl::string_view token) const {
  return absl::c_all_of(token, [this](char ch) {
    const auto c = static_cast<unsigned char>(ch);
    return absl::ascii_isascii(c) && !locale_sensitive_ascii_[c];
  });
}

void NormalizeCaseFoldFilter::NormalizeInPlace(std::string &token) const {
  if (CanLowerAsAscii(token)) {
    absl::AsciiStrToLower(&token);
    return;
  }
  UnicodeNormalizer::NormalizeInPlace(token, norm_form_);
  if (locale_.empty()) {
    UnicodeNormalizer::CaseFoldInPlace(token);
  } else {
    token = UnicodeNormalizer::LocaleAwareCaseFold(token, locale_);
  }
}

}  // namespace valkey_search::indexes::text
