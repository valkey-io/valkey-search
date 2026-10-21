/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/query_context.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/strings/ascii.h"
#include "absl/types/span.h"
#include "src/index_schema.h"
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

// Corpus-wide scoring inputs, under the locks `mode` requires. The analyzer
// cannot see the caller's read phase through the runtime branch.
CorpusStats ReadCorpusStats(const IndexSchema &index_schema,
                            LockMode mode) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  const auto text = index_schema.GetTextIndexSchema();
  return {
      .total_docs =
          static_cast<uint32_t>(mode == LockMode::kShortLocks
                                    ? index_schema.GetIndexKeyInfoSizeLocked()
                                    : index_schema.GetIndexKeyInfoSize()),
      // Atomic, so readable without the time-sliced mutex.
      .total_doc_len = text ? text->GetMetadata().total_doc_len.load() : 0,
      .has_score_field = index_schema.HasScoreField(),
      .default_document_score = index_schema.GetScore(),
  };
}

}  // namespace

QueryContext::QueryContext(const IndexSchema &index_schema,
                           const indexes::scoring::Scorer *scorer,
                           LockMode mode)
    : index_schema_(index_schema),
      text_index_schema_(index_schema.GetTextIndexSchema().get()),
      stats_(ReadCorpusStats(index_schema, mode)),
      scorer_(scorer),
      mode_(mode),
      needs_doc_len_(scorer->NeedsDocumentLength()),
      avg_doc_len_(needs_doc_len_ && stats_.total_docs > 0
                       ? static_cast<float>(stats_.total_doc_len) /
                             static_cast<float>(stats_.total_docs)
                       : 0.0f) {}

const indexes::text::TextIndex *QueryContext::PerKeyTextIndex(
    const InternedStringPtr &key) const {
  if (text_index_schema_ == nullptr) return nullptr;
  // Under kShortLocks the map lock guards the find; the tree itself is stable
  // because the revalidated key has no in-flight mutation.
  return text_index_schema_->GetPerKeyTextIndex(key, ShortLocks());
}

uint32_t QueryContext::DocLen(BorrowedInternedStringPtr key) const {
  if (!needs_doc_len_ || text_index_schema_ == nullptr) return 0;
  return text_index_schema_->GetKeyDocLen(key, ShortLocks());
}

float QueryContext::DocScore(BorrowedInternedStringPtr key) const
    ABSL_NO_THREAD_SAFETY_ANALYSIS {
  if (!stats_.has_score_field) return stats_.default_document_score;
  return ShortLocks() ? index_schema_.GetDocumentScoreLocked(key)
                      : index_schema_.GetDocumentScore(key);
}

size_t QueryContext::TagPrefixDocCount(const indexes::Tag &index,
                                       absl::string_view prefix,
                                       BorrowedInternedStringPtr key) const {
  return index.GetPrefixMatchDocCount(prefix, key, ShortLocks());
}

size_t QueryContext::KeyCount(const WordPostings &word) const {
  auto get = [&] { return word.postings->GetKeyCount(); };
  return ShortLocks() ? text_index_schema_->WithWordLock(*word.word, get)
                      : get();
}

std::optional<indexes::text::PostingValue> QueryContext::Probe(
    const WordPostings &word, BorrowedInternedStringPtr key,
    uint64_t field_mask) const {
  auto get = [&] { return word.postings->GetPostingValue(key, field_mask); };
  return ShortLocks() ? text_index_schema_->WithWordLock(*word.word, get)
                      : get();
}

float QueryContext::ScoreLeaf(indexes::scoring::LeafScoreInput input) const {
  input.avg_doc_len = avg_doc_len_;
  return scorer_->ScoreLeaf(input);
}

float QueryContext::Idf(size_t dt) const {
  return scorer_->PrecomputeIDF(
      {stats_.total_docs,
       static_cast<uint32_t>(std::min<size_t>(dt, stats_.total_docs))});
}

std::optional<std::string> QueryContext::LockWord(
    absl::string_view word) const {
  if (ShortLocks()) return std::string(word);
  return std::nullopt;
}

WordPostings QueryContext::Lookup(absl::string_view word) const {
  auto find = [&](const indexes::text::TextIndex &index) {
    return index.GetPrefix().FindPostingsTarget(word);
  };
  return {LockWord(word), ShortLocks()
                              ? text_index_schema_->WithTextIndexLock(find)
                              : find(*text_index_schema_->GetTextIndex())};
}

ResolvedLeaf &QueryContext::GetOrResolve(const Predicate *predicate) {
  CHECK(predicate != nullptr);
  auto it = leaf_cache_.find(predicate);
  if (it == leaf_cache_.end()) {
    it = leaf_cache_.emplace(predicate, Resolve(predicate)).first;
  }
  return it->second;
}

ResolvedLeaf QueryContext::Resolve(const Predicate *predicate) const {
  switch (predicate->GetType()) {
    case PredicateType::kText:
      return ResolveText(static_cast<const TextPredicate *>(predicate));
    case PredicateType::kTag:
      return ResolveTag(static_cast<const TagPredicate *>(predicate));
    default:
      return std::monostate{};
  }
}

std::optional<ExpansionMatch> QueryContext::FindExpansionMatch(
    ExpansionLeaf &leaf, const ExpansionPredicate &predicate,
    const indexes::text::TextIndex &per_key_index,
    const InternedStringPtr &key) const {
  const uint64_t field_mask = predicate.GetFieldMask();
  std::optional<ExpansionMatch> found;
  predicate.ForEachMatch(
      per_key_index,
      [&](absl::string_view word,
          const indexes::text::InvasivePtr<indexes::text::Postings> &postings) {
        // Entry and df under one bucket hold.
        auto get = [&] {
          return std::pair{postings->GetPostingValue(
                               BorrowedInternedStringPtr(key), field_mask),
                           postings->GetKeyCount()};
        };
        auto [entry, df] =
            ShortLocks() ? text_index_schema_->WithWordLock(word, get) : get();
        if (!entry) return true;
        const float idf = Idf(df);
        if (!leaf.representative || leaf.representative->df < df) {
          leaf.representative.emplace(
              ExpansionLeaf::Term{{LockWord(word), postings}, df, idf});
        }
        found.emplace(ExpansionMatch{idf, *entry});
        return false;
      });
  return found;
}

ResolvedLeaf QueryContext::ResolveText(const TextPredicate *predicate) const {
  CHECK(predicate->GetTextIndexSchema().get() == text_index_schema_);
  const uint8_t num_text_fields = text_index_schema_->GetNumTextFields();

  if (dynamic_cast<const ExpansionPredicate *>(predicate)) {
    ExpansionLeaf leaf;
    leaf.field_mask =
        ScoringFieldMask(predicate->GetFieldMask(), num_text_fields);
    return leaf;
  }
  auto term_pred = static_cast<const TermPredicate *>(predicate);

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
    ShortLocks() ? text_index_schema_->WithStemTreeLock(add_stem_groups)
                 : add_stem_groups(text_index_schema_->GetStemTree());
  }
  return leaf;
}

ResolvedLeaf QueryContext::ResolveTag(const TagPredicate *tag_pred) const {
  const indexes::Tag *tag_index = tag_pred->GetIndex();
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
    // kShortLocks takes the index mutex only to read dt; see
    // TagLeaf::Value.
    if (ShortLocks()) {
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

EvaluationResult EvaluateTextLeaf(
    QueryContext &query_ctx, const TextPredicate &predicate,
    const InternedStringPtr &key, bool require_positions,
    absl::FunctionRef<const indexes::text::TextIndex *()> per_key_index) {
  if (const auto *leaf =
          std::get_if<TermLeaf>(&query_ctx.GetOrResolve(&predicate))) {
    return static_cast<const TermPredicate &>(predicate).Evaluate(
        *leaf, key, require_positions);
  }
  const auto *index = per_key_index();
  if (index == nullptr) return EvaluationResult(false);
  return static_cast<const ExpansionPredicate &>(predicate).Evaluate(
      *index, key, require_positions, query_ctx.ShortLocks());
}

}  // namespace valkey_search::query
