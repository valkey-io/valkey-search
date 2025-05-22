/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
  kNone
};

class TagPredicate;
class NumericPredicate;
class Evaluator {
 public:
  virtual ~Evaluator() = default;
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
  NumericPredicate(const indexes::Numeric* index, absl::string_view identifier,
                   double start, bool is_inclusive_start, double end,
                   bool is_inclusive_end);
  const indexes::Numeric* GetIndex() const { return index_; }
  absl::string_view GetIdentifier() const {
    return vmsdk::ToStringView(identifier_.get());
  }
  vmsdk::UniqueRedisString GetRetainedIdentifier() const {
    return vmsdk::RetainUniqueRedisString(identifier_.get());
  }
  double GetStart() const { return start_; }
  bool IsStartInclusive() const { return is_inclusive_start_; }
  double GetEnd() const { return end_; }
  bool IsEndInclusive() const { return is_inclusive_end_; }
  bool Evaluate(Evaluator& evaluator) const override;
  bool Evaluate(const double* value) const;

 private:
  const indexes::Numeric* index_;
  vmsdk::UniqueRedisString identifier_;
  double start_;
  bool is_inclusive_start_;
  double end_;
  bool is_inclusive_end_;
};

class TagPredicate : public Predicate {
 public:
  TagPredicate(const indexes::Tag* index, absl::string_view identifier,
               absl::string_view raw_tag_string,
               const absl::flat_hash_set<absl::string_view>& tags);
  bool Evaluate(Evaluator& evaluator) const override;
  bool Evaluate(const absl::flat_hash_set<absl::string_view>* tags,
                bool case_sensitive) const;
  const indexes::Tag* GetIndex() const { return index_; }
  absl::string_view GetIdentifier() const {
    return vmsdk::ToStringView(identifier_.get());
  }
  vmsdk::UniqueRedisString GetRetainedIdentifier() const {
    return vmsdk::RetainUniqueRedisString(identifier_.get());
  }
  const std::string& GetTagString() const { return raw_tag_string_; }
  const absl::flat_hash_set<std::string>& GetTags() const { return tags_; }

 private:
  const indexes::Tag* index_;
  vmsdk::UniqueRedisString identifier_;
  std::string raw_tag_string_;
  absl::flat_hash_set<std::string> tags_;
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
