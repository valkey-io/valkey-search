/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_HYBRID_COMBINE_H_
#define VALKEYSEARCH_SRC_COMMANDS_FT_HYBRID_COMBINE_H_

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "src/expr/expr.h"
#include "src/expr/value.h"

namespace valkey_search::query {

// Evaluation record for a COMBINE FUNCTION expression: holds the per-arm score
// for the document currently being scored, indexed by arm position.
struct ArmScoreRecord : public expr::Expression::Record {
  std::vector<expr::Value> scores;
};

// Compiled reference to one arm's score, resolved by CombineFunctionContext.
// At evaluation time it reads the corresponding slot of an ArmScoreRecord.
class ArmScoreRef : public expr::Expression::AttributeReference {
 public:
  ArmScoreRef(size_t arm_index, std::string name)
      : arm_index_(arm_index), name_(std::move(name)) {}
  expr::Value GetValue(expr::Expression::EvalContext& /*ctx*/,
                       const expr::Expression::Record& record) const override {
    const auto& rec = static_cast<const ArmScoreRecord&>(record);
    if (arm_index_ >= rec.scores.size()) {
      return expr::Value();  // Nil
    }
    return rec.scores[arm_index_];
  }
  void Dump(std::ostream& os) const override { os << '@' << name_; }

 private:
  size_t arm_index_;
  std::string name_;
};

// CompileContext for a COMBINE FUNCTION expression. Resolves `@<alias>`
// references to per-arm score slots. Each arm is reachable via its
// YIELD_SCORE_AS alias (if any) and via the positional default
// `@__arm<i>_score`; the two-arm FT.HYBRID also exposes `@__search_score`
// (arm 0) and `@__vector_score` (arm 1). PARAMS are not supported.
class CombineFunctionContext : public expr::Expression::CompileContext {
 public:
  absl::flat_hash_map<std::string, size_t> alias_to_arm;

  absl::StatusOr<std::unique_ptr<expr::Expression::AttributeReference>>
  MakeReference(absl::string_view s, bool /*create*/) override {
    auto it = alias_to_arm.find(s);
    if (it == alias_to_arm.end()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "COMBINE FUNCTION references unknown arm score `@", s, "`"));
    }
    return std::make_unique<ArmScoreRef>(it->second, std::string(s));
  }

  absl::StatusOr<expr::Value> GetParam(absl::string_view /*s*/) const override {
    return absl::InvalidArgumentError(
        "PARAMS are not available inside COMBINE FUNCTION");
  }
};

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_COMMANDS_FT_HYBRID_COMBINE_H_
