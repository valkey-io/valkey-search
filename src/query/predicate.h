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
#include "absl/strings/string_view.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search::indexes {
class Text;
class Numeric;
class Tag;
}  // namespace valkey_search::indexes

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
class Evaluator {
 public:
  virtual ~Evaluator() = default;
  virtual bool EvaluateText(const TextPredicate& predicate) = 0;
  virtual bool EvaluateTags(const TagPredicate& predicate) = 0;
  virtual bool EvaluateNumeric(const NumericPredicate& predicate) = 0;
};

class Predicate;
struct EstimatedQualifiedEntries {
  size_t estimated_qualified_entries;
  std::vector<Predicate*> predicates;
};

class Predicate {
 public:
  explicit Predicate(PredicateType type) : type_(type) {}
  virtual bool Evaluate(Evaluator& evaluator) const = 0;
  virtual ~Predicate() = default;
  PredicateType GetType() const { return type_; }

 private:
  PredicateType type_;
};

class NegatePredicate : public Predicate {
 public:
  explicit NegatePredicate(std::unique_ptr<Predicate> predicate)
      : Predicate(PredicateType::kNegate), predicate_(std::move(predicate)) {}
  bool Evaluate(Evaluator& evaluator) const override;
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
  bool Evaluate(Evaluator& evaluator) const override;
  bool Evaluate(const double* value) const;

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
  bool Evaluate(Evaluator& evaluator) const override;
  bool Evaluate(const absl::flat_hash_set<absl::string_view>* tags,
                bool case_sensitive) const;
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

class TextPredicate : public Predicate {
 public:
  enum class Operation {
    kExact,      // Exact term / exact phrase match.
    kPrefix,     // Prefix Wildcard match
    kSuffix,     // kSuffix Wildcard match
    kInfix,      // kInfix Wildcard match
    kFuzzy,      // Fuzzy match
  };

  TextPredicate(const indexes::Text* index, absl::string_view alias,
                absl::string_view identifier,
                absl::string_view raw_text_string,
                Operation op = Operation::kExact,
                uint32_t fuzzy_distance = 0);

  bool Evaluate(Evaluator& evaluator) const override;
  bool Evaluate(absl::string_view raw_text_string) const;
  const indexes::Text* GetIndex() const { return index_; }
  absl::string_view GetAlias() const { return alias_; }
  absl::string_view GetIdentifier() const {
    return vmsdk::ToStringView(identifier_.get());
  }
  vmsdk::UniqueValkeyString GetRetainedIdentifier() const {
    return vmsdk::RetainUniqueValkeyString(identifier_.get());
  }
  const std::string& GetTextString() const { return raw_text_string_; }
  Operation GetOperation() const { return operation_; }
  uint32_t GetFuzzyDistance() const { return fuzzy_distance_; }

 private:
  const indexes::Text* index_;
  vmsdk::UniqueValkeyString identifier_;
  std::string alias_; // Attribute alias will be NULL for default text fields.
  std::string raw_text_string_;
  Operation operation_;
  uint32_t fuzzy_distance_;

  // Private evaluation methods
  bool EvaluateExact(absl::string_view text) const;
  bool EvaluatePrefix(absl::string_view text) const;
  bool EvaluateSuffix(absl::string_view text) const;
  bool EvaluateInfix(absl::string_view text) const;
  bool EvaluateFuzzy(absl::string_view text) const;
};

enum class LogicalOperator { kAnd, kOr };
// Composed Predicate (AND/OR)
class ComposedPredicate : public Predicate {
 public:
  ComposedPredicate(std::unique_ptr<Predicate> lhs_predicate,
                    std::unique_ptr<Predicate> rhs_predicate,
                    LogicalOperator logical_op);

  bool Evaluate(Evaluator& evaluator) const override;
  const Predicate* GetLhsPredicate() const { return lhs_predicate_.get(); }
  const Predicate* GetRhsPredicate() const { return rhs_predicate_.get(); }

 private:
  std::unique_ptr<Predicate> lhs_predicate_;
  std::unique_ptr<Predicate> rhs_predicate_;
};

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_PREDICATE_H_
