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

#include "src/query/predicate.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::query {

bool NegatePredicate::Evaluate(Evaluator& evaluator) const {
  return !predicate_->Evaluate(evaluator);
}

NumericPredicate::NumericPredicate(const indexes::Numeric* index,
                                   absl::string_view identifier, double start,
                                   bool is_inclusive_start, double end,
                                   bool is_inclusive_end)
    : Predicate(PredicateType::kNumeric),
      index_(index),
      identifier_(vmsdk::MakeUniqueRedisString(identifier)),
      start_(start),
      is_inclusive_start_(is_inclusive_start),
      end_(end),
      is_inclusive_end_(is_inclusive_end) {}

bool NumericPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateNumeric(*this);
}

bool NumericPredicate::Evaluate(const double* value) const {
  if (!value) {
    return false;
  }
  return (((*value > start_ || (is_inclusive_start_ && *value == start_)) &&
           (*value < end_)) ||
          (is_inclusive_end_ && *value == end_));
}

TagPredicate::TagPredicate(const indexes::Tag* index,
                           absl::string_view identifier,
                           absl::string_view raw_tag_string,
                           const absl::flat_hash_set<absl::string_view>& tags)
    : Predicate(PredicateType::kTag),
      index_(index),
      identifier_(vmsdk::MakeUniqueRedisString(identifier)),
      raw_tag_string_(raw_tag_string),
      tags_(tags.begin(), tags.end()) {}

bool TagPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateTags(*this);
}

bool TagPredicate::Evaluate(
    const absl::flat_hash_set<absl::string_view>* in_tags,
    bool case_sensitive) const {
  if (!in_tags) {
    return false;
  }

  for (const auto& in_tag : *in_tags) {
    for (const auto& tag : tags_) {
      absl::string_view left_hand_side = in_tag;
      absl::string_view right_hand_side = tag;
      if (right_hand_side.back() == '*') {
        if (left_hand_side.length() < right_hand_side.length() - 1) {
          continue;
        }
        left_hand_side = left_hand_side.substr(0, right_hand_side.length() - 1);
        right_hand_side =
            right_hand_side.substr(0, right_hand_side.length() - 1);
      }
      if (case_sensitive) {
        if (left_hand_side == right_hand_side) {
          return true;
        }
      } else {
        if (absl::EqualsIgnoreCase(left_hand_side, right_hand_side)) {
          return true;
        }
      }
    }
  }
  return false;
}

ComposedPredicate::ComposedPredicate(std::unique_ptr<Predicate> lhs_predicate,
                                     std::unique_ptr<Predicate> rhs_predicate,
                                     LogicalOperator logical_op)
    : Predicate(logical_op == LogicalOperator::kAnd
                    ? PredicateType::kComposedAnd
                    : PredicateType::kComposedOr),
      lhs_predicate_(std::move(lhs_predicate)),
      rhs_predicate_(std::move(rhs_predicate)) {}

bool ComposedPredicate::Evaluate(Evaluator& evaluator) const {
  if (GetType() == PredicateType::kComposedAnd) {
    auto lhs = lhs_predicate_->Evaluate(evaluator);
    VMSDK_LOG(DEBUG, nullptr) << "Inline evaluate AND predicate lhs: " << lhs;
    auto rhs = rhs_predicate_->Evaluate(evaluator);
    VMSDK_LOG(DEBUG, nullptr) << "Inline evaluate AND predicate rhs: " << rhs;
    return lhs && rhs;
  }

  auto lhs = lhs_predicate_->Evaluate(evaluator);
  VMSDK_LOG(DEBUG, nullptr) << "Inline evaluate OR predicate lhs: " << lhs;
  auto rhs = rhs_predicate_->Evaluate(evaluator);
  VMSDK_LOG(DEBUG, nullptr) << "Inline evaluate OR predicate rhs: " << rhs;
  return lhs || rhs;
}

}  // namespace valkey_search::query
