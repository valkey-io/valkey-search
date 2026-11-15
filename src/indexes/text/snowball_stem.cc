/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/snowball_stem.h"

#include <algorithm>
#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "libstemmer.h"
#include "src/utils/scanner.h"
#include "vmsdk/src/info.h"

namespace valkey_search::indexes::text {

namespace {

struct StemmerDeleter {
  void operator()(sb_stemmer* stemmer) const { sb_stemmer_delete(stemmer); }
};

using StemmerPtr = std::unique_ptr<sb_stemmer, StemmerDeleter>;

thread_local absl::flat_hash_map<data_model::Language, StemmerPtr> stemmers_;

// INFO counter for the min_stem_size code-point compatibility defect (see
// COMPATIBILITY.md). Statically constructed because stemming runs on worker
// threads. Only counts words where the byte and code point lengths disagree.
vmsdk::info_field::Integer min_stem_size_code_points_compat_counter(
    "compatibility", "compatibility-min_stem_size_code_points",
    vmsdk::info_field::IntegerBuilder().App());

bool IsKnownAlgorithm(absl::string_view name) {
  for (const char** algorithm = sb_stemmer_list(); *algorithm != nullptr;
       ++algorithm) {
    if (name == *algorithm) {
      return true;
    }
  }
  return false;
}

}  // namespace

SnowballStemFilter::SnowballStemFilter(data_model::Language language,
                                       absl::string_view algorithm_name)
    : language_(language), algorithm_name_(algorithm_name) {
  // Fail when the language is registered, not on its first stem: the
  // per-thread stemmer itself is still created lazily (see GetStemmer).
  CHECK(IsKnownAlgorithm(algorithm_name_))
      << "Unknown Snowball algorithm: " << algorithm_name_;
}

sb_stemmer* SnowballStemFilter::GetStemmer() const {
  auto it = stemmers_.find(language_);
  if (it == stemmers_.end()) {
    StemmerPtr stemmer(sb_stemmer_new(algorithm_name_.c_str(), "UTF_8"));
    CHECK(stemmer) << "Failed to create stemmer for algorithm: "
                   << algorithm_name_;
    sb_stemmer* raw_ptr = stemmer.get();
    stemmers_[language_] = std::move(stemmer);
    return raw_ptr;
  }
  return it->second.get();
}

std::string_view SnowballStemFilter::DoStemming(absl::string_view word,
                                                sb_stemmer* stemmer,
                                                uint32_t min_stem_size,
                                                LengthUnit unit) const {
  if (word.empty()) {
    return word;
  }
  if (!utils::Scanner::AtLeastNCodepoints(word, min_stem_size)) {
    if (unit != LengthUnit::kBytes || word.size() < min_stem_size) {
      return word;
    }
    // Long enough only when counted in bytes (1.2 behavior).
    min_stem_size_code_points_compat_counter.Increment();
  }
  CHECK(stemmer) << "Stemmer is not initialized";
  const sb_symbol* stemmed = sb_stemmer_stem(
      stemmer, reinterpret_cast<const sb_symbol*>(word.data()), word.length());
  CHECK(stemmed) << "Stemming failed";
  int stemmed_length = sb_stemmer_length(stemmer);
  // The Turkish Snowball stemmer can legitimately reduce a word to zero length
  // due to its aggressive r_remove_proper_noun_suffix step combined with noun/
  // verb suffix chains. This is a valid outcome, not an error — return the
  // original word unchanged when it happens.
  if (stemmed_length <= 0) {
    return word;
  }
  return {reinterpret_cast<const char*>(stemmed),
          static_cast<std::string_view::size_type>(stemmed_length)};
}

std::string SnowballStemFilter::GetStemRoot(absl::string_view token,
                                            uint32_t min_stem_size,
                                            LengthUnit unit) const {
  sb_stemmer* stemmer = GetStemmer();
  std::string_view stemmed = DoStemming(token, stemmer, min_stem_size, unit);
  return std::string(stemmed);
}

void SnowballStemFilter::BuildStemMap(const std::vector<std::string>& tokens,
                                      uint32_t min_stem_size, LengthUnit unit,
                                      InProgressStemMap& stem_mappings) const {
  sb_stemmer* stemmer = GetStemmer();
  for (const auto& token : tokens) {
    std::string_view stemmed_view =
        DoStemming(token, stemmer, min_stem_size, unit);
    if (stemmed_view != token) {
      auto it = stem_mappings.find(stemmed_view);
      if (it == stem_mappings.end()) {
        it = stem_mappings.try_emplace(std::string(stemmed_view)).first;
      }
      auto& variants = it->second;
      if (std::find(variants.begin(), variants.end(), token) ==
          variants.end()) {
        variants.emplace_back(token);
      }
    }
  }
}

}  // namespace valkey_search::indexes::text
