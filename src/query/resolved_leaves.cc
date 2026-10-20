/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/resolved_leaves.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/strings/ascii.h"
#include "absl/types/span.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/tag.h"
#include "src/indexes/text/rax_wrapper.h"
#include "src/indexes/text/term.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/debug.h"

namespace valkey_search::query {

namespace {

// Collapses an all-fields mask (what the parser builds for an unscoped query)
// to the `~0ULL` sentinel so posting probes skip the per-position scan. Field
// numbers are dense from 0 (TextIndexSchema::AllocateTextFieldNumber).
uint64_t ScoringFieldMask(uint64_t field_mask, uint8_t num_fields) {
  if (num_fields == 0 || num_fields >= 64) return field_mask;
  const uint64_t all_fields = (1ULL << num_fields) - 1;
  return (field_mask & all_fields) == all_fields ? ~0ULL : field_mask;
}

}  // namespace

ResolvedLeafCache::ResolvedLeafCache(
    const indexes::text::TextIndexSchema *text_index_schema, CorpusStats stats,
    const indexes::scoring::Scorer *scorer, LockMode mode)
    : text_index_schema_(text_index_schema),
      stats_(stats),
      scorer_(scorer),
      mode_(mode),
      needs_doc_len_(scorer->NeedsDocumentLength()),
      avg_doc_len_(needs_doc_len_ && stats.total_docs > 0
                       ? static_cast<float>(stats.total_doc_len) /
                             static_cast<float>(stats.total_docs)
                       : 0.0f) {}

size_t ResolvedLeafCache::KeyCount(const WordPostings &word) const {
  auto get = [&] { return word.postings->GetKeyCount(); };
  return MainThread() ? text_index_schema_->WithWordLock(*word.word, get)
                      : get();
}

std::optional<indexes::text::PostingValue> ResolvedLeafCache::Probe(
    const WordPostings &word, BorrowedInternedStringPtr key,
    uint64_t field_mask) const {
  auto get = [&] { return word.postings->GetPostingValue(key, field_mask); };
  return MainThread() ? text_index_schema_->WithWordLock(*word.word, get)
                      : get();
}

float ResolvedLeafCache::Idf(size_t dt) const {
  return scorer_->PrecomputeIDF(
      {stats_.total_docs,
       static_cast<uint32_t>(std::min<size_t>(dt, stats_.total_docs))});
}

std::optional<std::string> ResolvedLeafCache::LockWord(
    absl::string_view word) const {
  if (MainThread()) return std::string(word);
  return std::nullopt;
}

WordPostings ResolvedLeafCache::Lookup(absl::string_view word) const {
  auto find = [&](const indexes::text::TextIndex &index) {
    return index.GetPrefix().FindPostingsTarget(word);
  };
  return {LockWord(word), MainThread()
                              ? text_index_schema_->WithTextIndexLock(find)
                              : find(*text_index_schema_->GetTextIndex())};
}

ResolvedLeaf &ResolvedLeafCache::GetOrResolve(const Predicate *predicate) {
  CHECK(predicate != nullptr);
  auto it = leaves_.find(predicate);
  if (it == leaves_.end()) {
    it = leaves_.emplace(predicate, Resolve(predicate)).first;
  }
  return it->second;
}

ResolvedLeaf ResolvedLeafCache::Resolve(const Predicate *predicate) const {
  switch (predicate->GetType()) {
    case PredicateType::kText:
      return ResolveText(static_cast<const TextPredicate *>(predicate));
    case PredicateType::kTag:
      return ResolveTag(predicate);
    default:
      return std::monostate{};
  }
}

float ResolvedLeafCache::OfferExpansionTerm(ExpansionLeaf &leaf,
                                            const ExpansionMatch &match) const {
  if (leaf.representative && leaf.representative->df >= match.df) {
    return Idf(match.df);
  }
  leaf.representative.emplace(
      ExpansionLeaf::Term{match.term, match.df, Idf(match.df)});
  return leaf.representative->idf;
}

std::optional<ExpansionMatch> ResolvedLeafCache::FindExpansionMatch(
    const ExpansionPredicate &predicate,
    const indexes::text::TextIndex &per_key_index,
    const InternedStringPtr &key) const {
  const uint64_t field_mask = predicate.GetFieldMask();
  std::optional<ExpansionMatch> found;
  predicate.ForEachMatch(
      per_key_index,
      [&](absl::string_view word,
          const indexes::text::InvasivePtr<indexes::text::Postings> &postings) {
        auto get = [&] {
          return std::pair{postings->GetPostingValue(
                               BorrowedInternedStringPtr(key), field_mask),
                           postings->GetKeyCount()};
        };
        auto [entry, df] =
            MainThread() ? text_index_schema_->WithWordLock(word, get) : get();
        if (!entry) return true;
        found.emplace(ExpansionMatch{{LockWord(word), postings}, *entry, df});
        return false;
      });
  return found;
}

ResolvedLeaf ResolvedLeafCache::ResolveText(
    const TextPredicate *predicate) const {
  CHECK(predicate->GetTextIndexSchema().get() == text_index_schema_);
  const uint8_t num_text_fields = text_index_schema_->GetNumTextFields();

  if (dynamic_cast<const ExpansionPredicate *>(predicate)) {
    ExpansionLeaf leaf;
    leaf.field_mask =
        ScoringFieldMask(predicate->GetFieldMask(), num_text_fields);
    return leaf;
  }
  auto term_pred = dynamic_cast<const TermPredicate *>(predicate);
  if (term_pred == nullptr) return std::monostate{};

  TermLeaf leaf;

  // A single-word group; an absent word adds none.
  auto add_word_group = [&](absl::string_view word, uint64_t field_mask) {
    WordPostings found = Lookup(word);
    if (!found.postings) return;
    TermGroup group;
    group.idf = Idf(KeyCount(found));
    group.field_mask = field_mask;
    group.words.push_back(std::move(found));
    leaf.groups.push_back(std::move(group));
  };

  // Fires once per query here rather than per key; test_cancel.py pauses on it.
  BACKGROUND_PAUSEPOINT("search_term_predicate");
  const absl::string_view word = term_pred->GetTextString();
  // Group 1: the exact word.
  add_word_group(word,
                 ScoringFieldMask(term_pred->GetFieldMask(), num_text_fields));

  const uint64_t stem_field_mask =
      term_pred->GetFieldMask() & text_index_schema_->GetStemTextFieldMask();
  if (!term_pred->IsExact() && stem_field_mask != 0) {
    const std::string stemmed = text_index_schema_->GetLexer().StemWord(word);
    auto add_stem_groups = [&](const indexes::text::Rax &stem_tree) {
      // Group 2: the root, when it differs from the word.
      if (stemmed != word) {
        add_word_group(stemmed,
                       ScoringFieldMask(stem_field_mask, num_text_fields));
      }
      // Group 3: the root's inflections, with dt the distinct-doc count kept
      // at ingestion (summing the words' dfs would double-count).
      const auto root = stem_tree.FindStemParentsTarget(stemmed);
      if (!root) return;
      TermGroup stem;
      const size_t max_words = options::GetMaxTermExpansions().GetValue();
      for (const auto &parent :
           absl::MakeConstSpan(root->parents)
               .first(std::min(root->parents.size(), max_words))) {
        WordPostings found = Lookup(parent);
        if (found.postings) stem.words.push_back(std::move(found));
      }
      if (!stem.words.empty()) {
        // The whole group's df, even when max expansions truncates the words.
        stem.idf = Idf(root->distinct_docs);
        stem.field_mask = ScoringFieldMask(stem_field_mask, num_text_fields);
        leaf.groups.push_back(std::move(stem));
      }
    };
    MainThread() ? text_index_schema_->WithStemTreeLock(add_stem_groups)
                 : add_stem_groups(text_index_schema_->GetStemTree());
  }
  return leaf;
}

ResolvedLeaf ResolvedLeafCache::ResolveTag(const Predicate *predicate) const {
  // Only a real TagPredicate carries the index + values needed to score; a
  // non-TagPredicate kTag leaf (e.g. a test mock) contributes 0.
  auto tag_pred = dynamic_cast<const TagPredicate *>(predicate);
  if (tag_pred == nullptr) return std::monostate{};
  const indexes::Tag *tag_index = tag_pred->GetIndex();
  if (tag_index == nullptr) return std::monostate{};

  TagLeaf leaf;
  const bool case_sensitive = tag_index->IsCaseSensitive();
  absl::flat_hash_set<std::string> seen;
  for (const auto &value : tag_pred->GetTags()) {
    std::string norm = case_sensitive ? value : absl::AsciiStrToLower(value);
    if (!seen.insert(norm).second) continue;
    if (!value.empty() && value.back() == '*') {
      leaf.tag_prefixes.push_back(value);
      continue;
    }
    TagLeaf::Value resolved{value};
    size_t dt = 0;
    // The main thread takes the index mutex only to read dt; see
    // TagLeaf::Value.
    if (MainThread()) {
      dt = tag_index->GetTagValueDocCount(value, /*lock=*/true);
    } else {
      resolved.bag = tag_index->LookupValue(value);
      if (resolved.bag) dt = resolved.bag->size();
    }
    if (dt == 0) continue;
    resolved.idf = Idf(dt);
    leaf.tag_values.push_back(std::move(resolved));
  }
  return leaf;
}

EvaluationResult EvaluateText(
    ResolvedLeafCache &cache, const TextPredicate &predicate,
    const InternedStringPtr &key, bool require_positions,
    absl::FunctionRef<const indexes::text::TextIndex *()> per_key_index) {
  if (const auto *leaf =
          std::get_if<TermLeaf>(&cache.GetOrResolve(&predicate))) {
    return static_cast<const TermPredicate &>(predicate).Evaluate(
        *leaf, key, require_positions);
  }
  const auto *index = per_key_index();
  if (index == nullptr) return EvaluationResult(false);
  return static_cast<const ExpansionPredicate &>(predicate).Evaluate(
      *index, key, require_positions, cache.MainThread());
}

}  // namespace valkey_search::query
