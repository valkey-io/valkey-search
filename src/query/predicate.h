/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_PREDICATE_H_
#define VALKEYSEARCH_SRC_QUERY_PREDICATE_H_
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/strings/string_view.h"
#include "src/indexes/text/text_iterator.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search::indexes {
class Text;
class Numeric;
class Tag;
}  // namespace valkey_search::indexes

namespace valkey_search::indexes::text {
class TextIterator;
class TextIndexSchema;
class TextIndex;
}  // namespace valkey_search::indexes::text

namespace valkey_search::query {

enum class PredicateType {
  kTag,
  kNumeric,
  kComposedAnd,
  kComposedOr,
  kNegate,
  kText,
  kNone
};

class TextPredicate;
class TagPredicate;
class NumericPredicate;

struct EvaluationResult {
  bool matches;
  std::unique_ptr<valkey_search::indexes::text::TextIterator> filter_iterator;

  // Constructor 1: For non-text predicates (no iterator)
  explicit EvaluationResult(bool result)
      : matches(result), filter_iterator(nullptr) {}

  // Constructor 2: For text predicates (with iterator)
  EvaluationResult(
      bool result,
      std::unique_ptr<valkey_search::indexes::text::TextIterator> iterator)
      : matches(result), filter_iterator(std::move(iterator)) {}

  // Helper function to build EvaluationResult for text predicates
  EvaluationResult BuildTextEvaluationResult(
      const std::unique_ptr<indexes::text::TextIterator>& iterator,
      bool requires_position);
};

class Evaluator {
 public:
  virtual ~Evaluator() = default;
  virtual EvaluationResult EvaluateText(const TextPredicate& predicate,
                                        bool require_positions) = 0;
  virtual EvaluationResult EvaluateTags(const TagPredicate& predicate) = 0;
  virtual EvaluationResult EvaluateNumeric(
      const NumericPredicate& predicate) = 0;
  // Access target key for proximity validation (only for Text)
  virtual const InternedStringPtr& GetTargetKey() const = 0;
  virtual bool IsPrefilterEvaluator() const { return false; }
};

class Predicate;
struct EstimatedQualifiedEntries {
  size_t estimated_qualified_entries;
  std::vector<Predicate*> predicates;
};

class Predicate {
 public:
  explicit Predicate(PredicateType type) : type_(type) {}
  virtual EvaluationResult Evaluate(Evaluator& evaluator) const = 0;
  virtual ~Predicate() = default;
  PredicateType GetType() const { return type_; }

 private:
  PredicateType type_;
};

class NegatePredicate : public Predicate {
 public:
  explicit NegatePredicate(std::unique_ptr<Predicate> predicate)
      : Predicate(PredicateType::kNegate), predicate_(std::move(predicate)) {}
  EvaluationResult Evaluate(Evaluator& evaluator) const override;
  const Predicate* GetPredicate() const { return predicate_.get(); }

 private:
  std::unique_ptr<Predicate> predicate_;
};

class NumericPredicate : public Predicate {
 public:
  NumericPredicate(const indexes::Numeric* index, absl::string_view alias,
                   absl::string_view identifier, double start,
                   bool is_inclusive_start, double end, bool is_inclusive_end);
  const indexes::Numeric* GetIndex() const { return index_; }
  absl::string_view GetIdentifier() const {
    return vmsdk::ToStringView(identifier_.get());
  }
  vmsdk::UniqueValkeyString GetRetainedIdentifier() const {
    return vmsdk::RetainUniqueValkeyString(identifier_.get());
  }
  absl::string_view GetAlias() const { return alias_; }
  double GetStart() const { return start_; }
  bool IsStartInclusive() const { return is_inclusive_start_; }
  double GetEnd() const { return end_; }
  bool IsEndInclusive() const { return is_inclusive_end_; }
  EvaluationResult Evaluate(Evaluator& evaluator) const override;
  EvaluationResult Evaluate(const double* value) const;

 private:
  const indexes::Numeric* index_;
  std::string alias_;
  vmsdk::UniqueValkeyString identifier_;
  double start_;
  bool is_inclusive_start_;
  double end_;
  bool is_inclusive_end_;
};

class TagPredicate : public Predicate {
 public:
  TagPredicate(const indexes::Tag* index, absl::string_view alias,
               absl::string_view identifier, absl::string_view raw_tag_string,
               const absl::flat_hash_set<absl::string_view>& tags);
<<<<<<< HEAD
  EvaluationResult Evaluate(Evaluator& evaluator) const override;
  EvaluationResult Evaluate(const absl::flat_hash_set<absl::string_view>* tags,
                            bool case_sensitive) const;
=======
  bool Evaluate(Evaluator& evaluator) const override;
  // Evaluate against tags (string_view set from indexed data or parsed query)
  bool Evaluate(const absl::flat_hash_set<absl::string_view>* tags,
                bool case_sensitive) const;
>>>>>>> upstream/main
  const indexes::Tag* GetIndex() const { return index_; }
  absl::string_view GetAlias() const { return alias_; }
  absl::string_view GetIdentifier() const {
    return vmsdk::ToStringView(identifier_.get());
  }
  vmsdk::UniqueValkeyString GetRetainedIdentifier() const {
    return vmsdk::RetainUniqueValkeyString(identifier_.get());
  }
  const std::string& GetTagString() const { return raw_tag_string_; }
  const absl::flat_hash_set<std::string>& GetTags() const { return tags_; }

 private:
  const indexes::Tag* index_;
  vmsdk::UniqueValkeyString identifier_;
  std::string alias_;
  std::string raw_tag_string_;
  absl::flat_hash_set<std::string> tags_;
};

using FieldMaskPredicate = uint64_t;

class TextPredicate : public Predicate {
 public:
  TextPredicate() : Predicate(PredicateType::kText) {}
  virtual ~TextPredicate() = default;
  virtual EvaluationResult Evaluate(Evaluator& evaluator) const = 0;
  // Evaluate against per-key TextIndex
  virtual EvaluationResult Evaluate(
      const valkey_search::indexes::text::TextIndex& text_index,
      const InternedStringPtr& target_key, bool require_positions) const = 0;
  virtual std::shared_ptr<indexes::text::TextIndexSchema> GetTextIndexSchema()
      const = 0;
  virtual const FieldMaskPredicate GetFieldMask() const = 0;
  virtual void* Search(bool negate) const;
  virtual std::unique_ptr<indexes::text::TextIterator> BuildTextIterator(
      const void* fetcher) const = 0;
  virtual size_t EstimateSize() const = 0;
};

class TermPredicate : public TextPredicate {
 public:
  TermPredicate(
      std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
      FieldMaskPredicate field_mask, std::string term, bool exact);
  std::shared_ptr<indexes::text::TextIndexSchema> GetTextIndexSchema() const {
    return text_index_schema_;
  }
  absl::string_view GetTextString() const { return term_; }
  EvaluationResult Evaluate(Evaluator& evaluator) const override;
  // Evaluate against per-key TextIndex
  EvaluationResult Evaluate(
      const valkey_search::indexes::text::TextIndex& text_index,
      const InternedStringPtr& target_key,
      bool require_positions) const override;
  std::unique_ptr<indexes::text::TextIterator> BuildTextIterator(
      const void* fetcher) const override;
  const FieldMaskPredicate GetFieldMask() const override { return field_mask_; }
  bool IsExact() const { return exact_; }
  size_t EstimateSize() const override;

 private:
  std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema_;
  FieldMaskPredicate field_mask_;
  std::string term_;
  bool exact_;
};

class PrefixPredicate : public TextPredicate {
 public:
  PrefixPredicate(
      std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
      FieldMaskPredicate field_mask, std::string term);
  std::shared_ptr<indexes::text::TextIndexSchema> GetTextIndexSchema() const {
    return text_index_schema_;
  }
  absl::string_view GetTextString() const { return term_; }
  EvaluationResult Evaluate(Evaluator& evaluator) const override;
  // Evaluate against per-key TextIndex
  EvaluationResult Evaluate(
      const valkey_search::indexes::text::TextIndex& text_index,
      const InternedStringPtr& target_key,
      bool require_positions) const override;
  std::unique_ptr<indexes::text::TextIterator> BuildTextIterator(
      const void* fetcher) const override;
  const FieldMaskPredicate GetFieldMask() const override { return field_mask_; }
  size_t EstimateSize() const override;

 private:
  std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema_;
  FieldMaskPredicate field_mask_;
  std::string term_;
};

class SuffixPredicate : public TextPredicate {
 public:
  SuffixPredicate(
      std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
      FieldMaskPredicate field_mask, std::string term);
  std::shared_ptr<indexes::text::TextIndexSchema> GetTextIndexSchema() const {
    return text_index_schema_;
  }
  absl::string_view GetTextString() const { return term_; }
  EvaluationResult Evaluate(Evaluator& evaluator) const override;
  // Evaluate against per-key TextIndex
  EvaluationResult Evaluate(
      const valkey_search::indexes::text::TextIndex& text_index,
      const InternedStringPtr& target_key,
      bool require_positions) const override;
  std::unique_ptr<indexes::text::TextIterator> BuildTextIterator(
      const void* fetcher) const override;
  const FieldMaskPredicate GetFieldMask() const override { return field_mask_; }
  size_t EstimateSize() const override;

 private:
  std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema_;
  FieldMaskPredicate field_mask_;
  std::string term_;
};

class InfixPredicate : public TextPredicate {
 public:
  InfixPredicate(
      std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
      FieldMaskPredicate field_mask, std::string term);
  std::shared_ptr<indexes::text::TextIndexSchema> GetTextIndexSchema() const {
    return text_index_schema_;
  }
  absl::string_view GetTextString() const { return term_; }
  EvaluationResult Evaluate(Evaluator& evaluator) const override;
  // Evaluate against per-key TextIndex
  EvaluationResult Evaluate(
      const valkey_search::indexes::text::TextIndex& text_index,
      const InternedStringPtr& target_key,
      bool require_positions) const override;
  std::unique_ptr<indexes::text::TextIterator> BuildTextIterator(
      const void* fetcher) const override;
  const FieldMaskPredicate GetFieldMask() const override { return field_mask_; }
  size_t EstimateSize() const override;

 private:
  std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema_;
  FieldMaskPredicate field_mask_;
  std::string term_;
};

class FuzzyPredicate : public TextPredicate {
 public:
  FuzzyPredicate(
      std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
      FieldMaskPredicate field_mask, std::string term, uint32_t distance);
  std::shared_ptr<indexes::text::TextIndexSchema> GetTextIndexSchema() const {
    return text_index_schema_;
  }
  absl::string_view GetTextString() const { return term_; }
  uint32_t GetDistance() const { return distance_; }
  EvaluationResult Evaluate(Evaluator& evaluator) const override;
  // Evaluate against per-key TextIndex
  EvaluationResult Evaluate(
      const valkey_search::indexes::text::TextIndex& text_index,
      const InternedStringPtr& target_key,
      bool require_positions) const override;
  std::unique_ptr<indexes::text::TextIterator> BuildTextIterator(
      const void* fetcher) const override;
  const FieldMaskPredicate GetFieldMask() const override { return field_mask_; }
  size_t EstimateSize() const override;

 private:
  std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema_;
  FieldMaskPredicate field_mask_;
  std::string term_;
  uint32_t distance_;
};

enum class LogicalOperator { kAnd, kOr };
// Composed Predicate (AND/OR) - N-ary structure
class ComposedPredicate : public Predicate {
 public:
  // N-ary constructor
  ComposedPredicate(LogicalOperator logical_op,
                    std::vector<std::unique_ptr<Predicate>> children,
                    std::optional<uint32_t> slop = std::nullopt,
                    bool inorder = false);

  EvaluationResult Evaluate(Evaluator& evaluator) const override;
  std::optional<uint32_t> GetSlop() const { return slop_; }
  bool GetInorder() const { return inorder_; }

  // N-ary interface
  const std::vector<std::unique_ptr<Predicate>>& GetChildren() const {
    return children_;
  }
  size_t GetChildCount() const { return children_.size(); }
  // Add a child predicate (for building N-ary trees)
  void AddChild(std::unique_ptr<Predicate> child);
  // Release children (transfer ownership of children)
  std::vector<std::unique_ptr<Predicate>> ReleaseChildren() {
    return std::move(children_);
  }

 private:
  std::vector<std::unique_ptr<Predicate>> children_;
  std::optional<uint32_t> slop_;
  bool inorder_;
};

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_PREDICATE_H_
