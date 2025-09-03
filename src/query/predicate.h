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
  TextPredicate() : Predicate(PredicateType::kText) {}
  virtual ~TextPredicate() = default;
  virtual bool Evaluate(Evaluator& evaluator) const = 0;
  virtual bool Evaluate(const std::string_view& text) const = 0;
  virtual const indexes::Text* GetIndex() const = 0;
};

class TermPredicate : public TextPredicate {
 public:
  TermPredicate(const indexes::Text* index, absl::string_view identifier,
                absl::string_view alias, std::string term);
  const indexes::Text* GetIndex() const { return index_; }
  absl::string_view GetAlias() const { return alias_; }
  absl::string_view GetIdentifier() const {
    return vmsdk::ToStringView(identifier_.get());
  }
  vmsdk::UniqueValkeyString GetRetainedIdentifier() const {
    return vmsdk::RetainUniqueValkeyString(identifier_.get());
  }
  absl::string_view GetTextString() const { return term_; }
  bool Evaluate(Evaluator& evaluator) const override;
  bool Evaluate(const std::string_view& text) const override;

 private:
  const indexes::Text* index_;
  vmsdk::UniqueValkeyString identifier_;
  absl::string_view alias_;
  std::string term_;
};

class PrefixPredicate : public TextPredicate {
 public:
  PrefixPredicate(const indexes::Text* index, absl::string_view identifier,
                  absl::string_view alias, std::string term);
  const indexes::Text* GetIndex() const { return index_; }
  absl::string_view GetAlias() const { return alias_; }
  absl::string_view GetIdentifier() const {
    return vmsdk::ToStringView(identifier_.get());
  }
  vmsdk::UniqueValkeyString GetRetainedIdentifier() const {
    return vmsdk::RetainUniqueValkeyString(identifier_.get());
  }
  absl::string_view GetTextString() const { return term_; }
  bool Evaluate(Evaluator& evaluator) const override;
  bool Evaluate(const std::string_view& text) const override;

 private:
  const indexes::Text* index_;
  vmsdk::UniqueValkeyString identifier_;
  absl::string_view alias_;
  std::string term_;
};

class SuffixPredicate : public TextPredicate {
 public:
  SuffixPredicate(const indexes::Text* index, absl::string_view identifier,
                  absl::string_view alias, std::string term);
  const indexes::Text* GetIndex() const { return index_; }
  absl::string_view GetAlias() const { return alias_; }
  absl::string_view GetIdentifier() const {
    return vmsdk::ToStringView(identifier_.get());
  }
  vmsdk::UniqueValkeyString GetRetainedIdentifier() const {
    return vmsdk::RetainUniqueValkeyString(identifier_.get());
  }
  absl::string_view GetTextString() const { return term_; }
  bool Evaluate(Evaluator& evaluator) const override;
  bool Evaluate(const std::string_view& text) const override;

 private:
  const indexes::Text* index_;
  vmsdk::UniqueValkeyString identifier_;
  absl::string_view alias_;
  std::string term_;
};

class InfixPredicate : public TextPredicate {
 public:
  InfixPredicate(const indexes::Text* index, absl::string_view identifier,
                 absl::string_view alias, std::string term);
  const indexes::Text* GetIndex() const { return index_; }
  absl::string_view GetAlias() const { return alias_; }
  absl::string_view GetIdentifier() const {
    return vmsdk::ToStringView(identifier_.get());
  }
  vmsdk::UniqueValkeyString GetRetainedIdentifier() const {
    return vmsdk::RetainUniqueValkeyString(identifier_.get());
  }
  absl::string_view GetTextString() const { return term_; }
  bool Evaluate(Evaluator& evaluator) const override;
  bool Evaluate(const std::string_view& text) const override;

 private:
  const indexes::Text* index_;
  vmsdk::UniqueValkeyString identifier_;
  absl::string_view alias_;
  std::string term_;
};

class FuzzyPredicate : public TextPredicate {
 public:
  FuzzyPredicate(const indexes::Text* index, absl::string_view identifier,
                 absl::string_view alias, std::string term, uint32_t distance);
  const indexes::Text* GetIndex() const { return index_; }
  absl::string_view GetAlias() const { return alias_; }
  absl::string_view GetIdentifier() const {
    return vmsdk::ToStringView(identifier_.get());
  }
  vmsdk::UniqueValkeyString GetRetainedIdentifier() const {
    return vmsdk::RetainUniqueValkeyString(identifier_.get());
  }
  absl::string_view GetTextString() const { return term_; }
  uint32_t GetDistance() const { return distance_; }
  bool Evaluate(Evaluator& evaluator) const override;
  bool Evaluate(const std::string_view& text) const override;

 private:
  const indexes::Text* index_;
  vmsdk::UniqueValkeyString identifier_;
  absl::string_view alias_;
  std::string term_;
  uint32_t distance_;
};

class ProximityPredicate : public TextPredicate {
 public:
  ProximityPredicate(std::vector<std::unique_ptr<TextPredicate>> terms,
                     uint32_t slop = 0, bool inorder = true);
  uint32_t GetSlop() const { return slop_; }
  bool IsInOrder() const { return inorder_; }
  bool Evaluate(Evaluator& evaluator) const override;
  bool Evaluate(const std::string_view& text) const override { return false; }
  const indexes::Text* GetIndex() const override {
    return terms_[0]->GetIndex();
  }
  const std::vector<std::unique_ptr<TextPredicate>>& GetTerms() const {
    return terms_;
  }

 private:
  std::vector<std::unique_ptr<TextPredicate>> terms_;
  bool inorder_;
  uint32_t slop_;
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
