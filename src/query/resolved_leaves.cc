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

uint64_t ScoringFieldMask(uint64_t field_mask, uint8_t num_fields) {
  if (num_fields == 0 || num_fields >= 64) return field_mask;
  const uint64_t all_fields = (1ULL << num_fields) - 1;
  return (field_mask & all_fields) == all_fields ? ~0ULL : field_mask;
}

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
  return MainThread() ? text_index_schema_->WithWordLock(word.word, get)
                      : get();
}

std::optional<indexes::text::PostingValue> ResolvedLeafCache::Probe(
    const WordPostings &word, BorrowedInternedStringPtr key,
    uint64_t field_mask) const {
  auto get = [&] { return word.postings->GetPostingValue(key, field_mask); };
  return MainThread() ? text_index_schema_->WithWordLock(word.word, get)
                      : get();
}

float ResolvedLeafCache::Idf(size_t dt) const {
  return scorer_->PrecomputeIDF(
      {stats_.total_docs,
       static_cast<uint32_t>(std::min<size_t>(dt, stats_.total_docs))});
}

WordPostings ResolvedLeafCache::Lookup(absl::string_view word) const {
  auto find = [&](const indexes::text::TextIndex &index) {
    return index.GetPrefix().FindPostingsTarget(word);
  };
  return {std::string(word), MainThread()
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
  if (leaf.representative &&
      leaf.representative->key_count >= match.key_count) {
    return Idf(match.key_count);
  }
  leaf.representative.emplace(
      ExpansionLeaf::Term{match.term, match.key_count, Idf(match.key_count)});
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
        auto [entry, key_count] =
            MainThread() ? text_index_schema_->WithWordLock(word, get) : get();
        if (!entry) return true;
        found.emplace(
            ExpansionMatch{{std::string(word), postings}, *entry, key_count});
        return false;
      });
  return found;
}

ResolvedLeaf ResolvedLeafCache::ResolveText(
    const TextPredicate *predicate) const {
  // Resolved once per query, so the per-document walk never pays a
  // dynamic_cast.
  auto text_index_schema = predicate->GetTextIndexSchema();
  CHECK(text_index_schema.get() == text_index_schema_);
  const uint8_t num_text_fields = text_index_schema->GetNumTextFields();

  if (dynamic_cast<const ExpansionPredicate *>(predicate)) {
    ExpansionLeaf leaf;
    leaf.field_mask =
        ScoringFieldMask(predicate->GetFieldMask(), num_text_fields);
    return leaf;
  }
  auto term_pred = dynamic_cast<const TermPredicate *>(predicate);
  if (term_pred == nullptr) return std::monostate{};

  TermLeaf leaf;

  // A single-word BM25 term (the exact surface term or the stem root
  // literal): one posting list, IDF from that word's own df. An absent word
  // adds no group.
  auto add_word_group = [&](absl::string_view word, uint64_t field_mask,
                            TermGroup::Kind kind) {
    WordPostings found = Lookup(word);
    if (!found.postings) return;
    TermGroup group;
    group.kind = kind;
    group.idf = Idf(KeyCount(found));
    group.field_mask = field_mask;
    group.words.push_back(std::move(found));
    leaf.groups.push_back(std::move(group));
  };

  // Fires once per query here rather than per key; test_cancel.py pauses on it.
  BACKGROUND_PAUSEPOINT("search_term_predicate");
  const absl::string_view word = term_pred->GetTextString();
  // Group 1: the exact surface term. For a stemmed term this same word is
  // scored again in the inflection group below (it is one of its parents), the
  // deliberate exact-match boost.
  add_word_group(word,
                 ScoringFieldMask(term_pred->GetFieldMask(), num_text_fields),
                 TermGroup::Kind::kOriginal);

  const uint64_t stem_field_mask =
      term_pred->GetFieldMask() & text_index_schema->GetStemTextFieldMask();
  if (!term_pred->IsExact() && stem_field_mask != 0) {
    // Parents of the stem root: every surface word that stems to it with
    // surface != root (a self-stemming word is never added to the stem tree,
    // so the root literal is not among them). Includes the query word.
    const std::string stemmed = text_index_schema->GetLexer().StemWord(word);
    auto add_stem_groups = [&](const indexes::text::Rax &stem_tree) {
      // Group 2: the stem root literal, only when it differs from the
      // query word (else it is group 1) and is itself indexed.
      if (stemmed != word) {
        add_word_group(stemmed,
                       ScoringFieldMask(stem_field_mask, num_text_fields),
                       TermGroup::Kind::kStemRoot);
      }
      const auto root = stem_tree.FindStemParentsTarget(stemmed);
      if (!root) return;

      // Group 3: the stem inflections. F sums the per-doc frequencies of
      // every inflection; dt is the distinct doc count counted at ingestion.
      TermGroup stem;
      stem.kind = TermGroup::Kind::kInflections;
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
    MainThread() ? text_index_schema->WithStemTreeLock(add_stem_groups)
                 : add_stem_groups(text_index_schema->GetStemTree());
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

  // A tag value is scored as a BM25 term with F ≡ 1: IDF over the number of
  // documents carrying that value (dt). A union (`{red|blue}`) resolves several
  // values, each contributing its own term.
  TagLeaf leaf;
  leaf.tag_index = tag_index;
  // Dedupe query values that collapse to the same tag under the index's case
  // rules (e.g. `{red|Red}` on a case-insensitive index).
  const bool case_sensitive = tag_index->IsCaseSensitive();
  absl::flat_hash_set<std::string> seen;
  for (const auto &value : tag_pred->GetTags()) {
    std::string norm = case_sensitive ? value : absl::AsciiStrToLower(value);
    if (!seen.insert(norm).second) continue;
    // A prefix value (`foo*`) credits a single representative matched value
    // per document, which depends on the document, so dt/IDF resolve per
    // candidate.
    if (!value.empty() && value.back() == '*') {
      leaf.tag_prefixes.push_back(value);
      continue;
    }
    // A value absent from the index has no matching document and never
    // contributes a term.
    TagLeaf::Value resolved{value};
    size_t dt = 0;
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
        *leaf, key, require_positions, cache.MainThread());
  }
  const auto *index = per_key_index();
  if (index == nullptr) return EvaluationResult(false);
  return static_cast<const ExpansionPredicate &>(predicate).Evaluate(
      *index, key, require_positions, cache.MainThread());
}

}  // namespace valkey_search::query
