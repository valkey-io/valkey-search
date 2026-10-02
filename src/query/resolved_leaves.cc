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
#include "absl/synchronization/mutex.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/tag.h"
#include "src/indexes/text/fuzzy.h"
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

ResolvedLeafCache::ResolvedLeafCache(CorpusStats stats,
                                     const indexes::scoring::Scorer *scorer,
                                     LockMode mode)
    : stats_(stats),
      scorer_(scorer),
      mode_(mode),
      needs_doc_len_(scorer->NeedsDocumentLength()),
      avg_doc_len_(needs_doc_len_ && stats.total_docs > 0
                       ? static_cast<float>(stats.total_doc_len) /
                             static_cast<float>(stats.total_docs)
                       : 0.0f) {}

size_t KeyCount(const WordPostings &word) {
  std::optional<absl::MutexLock> lock;
  if (word.lock != nullptr) lock.emplace(word.lock);
  return word.postings->GetKeyCount();
}

std::optional<indexes::text::PostingDocStats> ProbeDocStats(
    const WordPostings &word, BorrowedInternedStringPtr key,
    uint64_t field_mask) {
  std::optional<absl::MutexLock> lock;
  if (word.lock != nullptr) lock.emplace(word.lock);
  return word.postings->GetPostingDocStats(key, field_mask);
}

float ResolvedLeafCache::Idf(size_t dt) const {
  return scorer_->PrecomputeIDF(
      {stats_.total_docs,
       static_cast<uint32_t>(std::min<size_t>(dt, stats_.total_docs))});
}

WordPostings ResolvedLeafCache::Lookup(
    const indexes::text::TextIndexSchema &schema,
    absl::string_view word) const {
  WordPostings result{std::string(word), nullptr};
  if (mode_ == LockMode::kBackground) {
    result.postings =
        schema.GetTextIndex()->GetPrefix().FindPostingsTarget(word);
    return result;
  }
  result.postings = schema.LookupGlobalPostings(word);
  if (mode_ == LockMode::kMainThread)
    result.lock = &schema.GetWordLocks().Get(word);
  return result;
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
                                            const WordPostings &term) const {
  const size_t count = KeyCount(term);
  if (leaf.representative && KeyCount(leaf.representative->term) >= count) {
    return Idf(count);
  }
  leaf.representative.emplace(ExpansionLeaf::Term{term, Idf(count)});
  return leaf.representative->idf;
}

std::optional<WordPostings> FindExpansionMatch(
    const TextPredicate &predicate, ExpansionLeaf::Kind kind,
    const indexes::text::TextIndex &per_key_index, const InternedStringPtr &key,
    indexes::text::RaxTargetMutexPool *word_locks) {
  const uint64_t field_mask = predicate.GetFieldMask();
  const uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  std::optional<WordPostings> found;
  // True once `found` is set, so the walks stop at the first match.
  auto probe =
      [&](absl::string_view word,
          indexes::text::InvasivePtr<indexes::text::Postings> postings) {
        if (!postings) return false;
        absl::Mutex *bucket = word_locks ? &word_locks->Get(word) : nullptr;
        std::optional<absl::MutexLock> lock;
        if (bucket != nullptr) lock.emplace(bucket);
        auto key_iter = postings->GetKeyIterator();
        if (!key_iter.SkipForwardKey(key) ||
            !key_iter.ContainsFields(field_mask)) {
          return false;
        }
        found = WordPostings{std::string(word), std::move(postings), bucket};
        return true;
      };
  const absl::string_view term = predicate.GetTextString();
  switch (kind) {
    case ExpansionLeaf::Kind::kPrefix: {
      auto it = per_key_index.GetPrefix().GetWordIterator(term);
      for (uint32_t n = 0; !it.Done() && n < max_words; ++n, it.Next()) {
        if (probe(it.GetWord(), it.GetPostingsTarget())) break;
      }
      break;
    }
    case ExpansionLeaf::Kind::kSuffix: {
      // The suffix trie stores reversed words; without WITHSUFFIXTRIE there
      // are no matched terms.
      auto suffix = per_key_index.GetSuffix();
      if (!suffix.has_value()) break;
      auto it = suffix->get().GetWordIterator(
          std::string(term.rbegin(), term.rend()));
      for (uint32_t n = 0; !it.Done() && n < max_words; ++n, it.Next()) {
        const absl::string_view reversed = it.GetWord();
        if (probe(std::string(reversed.rbegin(), reversed.rend()),
                  it.GetPostingsTarget())) {
          break;
        }
      }
      break;
    }
    case ExpansionLeaf::Kind::kFuzzy: {
      auto expansion = indexes::text::FuzzySearch::Search(
          per_key_index.GetPrefix(), term,
          static_cast<const FuzzyPredicate &>(predicate).GetDistance(),
          max_words, /*words_only=*/true);
      for (size_t i = 0; i < expansion.postings.size(); ++i) {
        if (probe(expansion.words[i], std::move(expansion.postings[i]))) break;
      }
      break;
    }
  }
  return found;
}

ResolvedLeaf ResolvedLeafCache::ResolveText(
    const TextPredicate *predicate) const {
  // The concrete kind is established once here so the per-document walk never
  // pays a dynamic_cast. Infix is unimplemented (its Evaluate CHECKs), so it
  // never reaches this point; a stray one resolves to monostate.
  auto text_index_schema = predicate->GetTextIndexSchema();
  CHECK(text_index_schema != nullptr);
  const uint8_t num_text_fields = text_index_schema->GetNumTextFields();

  auto expansion = [&](ExpansionLeaf::Kind kind) {
    ExpansionLeaf leaf;
    leaf.kind = kind;
    leaf.field_mask =
        ScoringFieldMask(predicate->GetFieldMask(), num_text_fields);
    return leaf;
  };
  if (dynamic_cast<const PrefixPredicate *>(predicate)) {
    return expansion(ExpansionLeaf::Kind::kPrefix);
  }
  if (dynamic_cast<const SuffixPredicate *>(predicate)) {
    return expansion(ExpansionLeaf::Kind::kSuffix);
  }
  if (dynamic_cast<const FuzzyPredicate *>(predicate)) {
    return expansion(ExpansionLeaf::Kind::kFuzzy);
  }
  auto term_pred = dynamic_cast<const TermPredicate *>(predicate);
  if (term_pred == nullptr) return std::monostate{};

  TermLeaf leaf;

  // A single-word BM25 term (the exact surface term or the stem root
  // literal): one posting list, IDF from that word's own df. An absent word
  // adds no group.
  auto add_word_group = [&](absl::string_view word, uint64_t field_mask,
                            TermGroup::Kind kind) {
    WordPostings found = Lookup(*text_index_schema, word);
    if (!found.postings) return;
    TermGroup group;
    group.kind = kind;
    group.idf = Idf(KeyCount(found));
    group.field_mask = field_mask;
    group.words.push_back(std::move(found));
    leaf.groups.push_back(std::move(group));
  };

  // Mirrors TermPredicate::Evaluate, which this lookup replaces for a cached
  // leaf; test_cancel.py expects the pausepoint to fire on either route.
  BACKGROUND_PAUSEPOINT("search_term_predicate");
  const absl::string_view word = term_pred->GetTextString();
  // Leaf 1: the exact surface term. For a stemmed term this same word is scored
  // again in the inflection group below (it is one of its parents), the
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
    absl::InlinedVector<absl::string_view,
                        indexes::text::kStemVariantsInlineCapacity>
        stem_variants;
    uint32_t stem_distinct_docs = 0;
    const std::string stemmed = text_index_schema->GetAllStemVariants(
        word, stem_variants, stem_field_mask, /*lock_needed=*/true,
        &stem_distinct_docs);

    // Leaf 2: the stem root literal, only when it differs from the query word
    // (else it is Leaf 1) and is itself indexed.
    if (stemmed != word) {
      add_word_group(stemmed,
                     ScoringFieldMask(stem_field_mask, num_text_fields),
                     TermGroup::Kind::kStemRoot);
    }

    // Leaf 3: the stem inflection group. F sums the per-doc frequencies of
    // every inflection; dt is the distinct doc count counted at ingestion.
    TermGroup stem;
    stem.kind = TermGroup::Kind::kInflections;
    for (const auto &variant : stem_variants) {
      WordPostings found = Lookup(*text_index_schema, variant);
      if (found.postings) stem.words.push_back(std::move(found));
    }
    if (!stem.words.empty()) {
      stem.idf = Idf(stem_distinct_docs);
      stem.field_mask = ScoringFieldMask(stem_field_mask, num_text_fields);
      leaf.groups.push_back(std::move(stem));
    }
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
    if (mode_ == LockMode::kBackground) {
      resolved.handle = tag_index->LookupValue(value);
      if (resolved.handle) dt = resolved.handle->DocCount();
    } else {
      dt = tag_index->GetTagValueDocCount(value, /*lock=*/true);
    }
    if (dt == 0) continue;
    resolved.idf = Idf(dt);
    leaf.tag_values.push_back(std::move(resolved));
  }
  return leaf;
}

EvaluationResult EvaluateTagLeaf(const TagPredicate &predicate,
                                 const TagLeaf &leaf,
                                 const InternedStringPtr &key) {
  for (const TagLeaf::Value &value : leaf.tag_values) {
    CHECK(value.handle.has_value()) << "tag bags are background-only";
    if (value.handle->Contains(BorrowedInternedStringPtr(key))) {
      return EvaluationResult(true);
    }
  }
  if (leaf.tag_prefixes.empty()) return EvaluationResult(false);
  bool case_sensitive = true;
  auto tags = predicate.GetIndex()->GetValue(key, case_sensitive);
  return predicate.Evaluate(tags ? &*tags : nullptr, case_sensitive);
}

EvaluationResult EvaluateTermLeaf(const TermPredicate &predicate,
                                  const TermLeaf &leaf,
                                  const InternedStringPtr &key,
                                  bool require_positions) {
  // Raw predicate masks, not the collapsed scoring masks: TermIterator
  // intersects QueryFieldMask() across AND children.
  const uint64_t field_mask = predicate.GetFieldMask();
  const uint64_t stem_field_mask =
      field_mask & predicate.GetTextIndexSchema()->GetStemTextFieldMask();
  absl::InlinedVector<indexes::text::Postings::KeyIterator,
                      indexes::text::kWordExpansionInlineCapacity>
      key_iterators;
  bool found_original = false;
  // Groups are stored in the order TermPredicate::Evaluate probes (original,
  // stem root, inflections), which TermIterator relies on to partition them.
  for (const TermGroup &group : leaf.groups) {
    const bool original = group.kind == TermGroup::Kind::kOriginal;
    const uint64_t mask = original ? field_mask : stem_field_mask;
    for (const WordPostings &word : group.words) {
      // A retained iterator would outlive a per-word lock; positional queries
      // run under LockMode::kMainThreadWordLocksHeld, where `lock` is unset.
      CHECK(word.lock == nullptr || !require_positions);
      std::optional<absl::MutexLock> lock;
      if (word.lock != nullptr) lock.emplace(word.lock);
      auto key_iter = word.postings->GetKeyIterator();
      if (!key_iter.SkipForwardKey(key) || !key_iter.ContainsFields(mask)) {
        continue;
      }
      if (!require_positions) return EvaluationResult(true);
      found_original |= original;
      key_iterators.emplace_back(std::move(key_iter));
    }
  }
  if (key_iterators.empty()) return EvaluationResult(false);
  auto iterator = std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), field_mask, require_positions, stem_field_mask,
      found_original);
  if (!iterator->IsIteratorValid()) return EvaluationResult(false);
  return {true, std::move(iterator)};
}

EvaluationResult EvaluateTextLeaf(
    ResolvedLeafCache &cache, const TextPredicate &predicate,
    const InternedStringPtr &key, bool require_positions,
    absl::FunctionRef<const indexes::text::TextIndex *()> per_key_index) {
  if (const auto *term =
          std::get_if<TermLeaf>(&cache.GetOrResolve(&predicate))) {
    return EvaluateTermLeaf(static_cast<const TermPredicate &>(predicate),
                            *term, key, require_positions);
  }
  const auto *index = per_key_index();
  if (index == nullptr) return EvaluationResult(false);
  return predicate.Evaluate(*index, key, require_positions,
                            cache.WalkLocks(*predicate.GetTextIndexSchema()));
}

}  // namespace valkey_search::query
