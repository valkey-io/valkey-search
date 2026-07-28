/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_FILTER_EXPR_H_
#define VALKEYSEARCH_SRC_FILTER_EXPR_H_

#include <string>

#include "src/expr/expr.h"
#include "src/expr/value.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"

namespace valkey_search {

// A compiled reference to an attribute field for filter evaluation.
// Holds the alias used to look up values in MutatedAttributes.
class FilterAttributeReference : public expr::Expression::AttributeReference {
 public:
  FilterAttributeReference(std::string alias, indexes::IndexerType type,
                           data_model::AttributeDataType data_type)
      : alias_(std::move(alias)), type_(type), data_type_(data_type) {}

  expr::Value GetValue(expr::Expression::EvalContext& ctx,
                       const expr::Expression::Record& record) const override;

  void Dump(std::ostream& os) const override;

 private:
  std::string alias_;
  indexes::IndexerType type_;
  data_model::AttributeDataType data_type_;
};

// A compiled reference to a HASH field that is NOT declared in the index
// schema. At evaluation time it reads the field's value directly off the open
// key (obtained from the FilterEvalContext), returning a missing Nil when the
// field is absent on the key. Because an undeclared field has no schema type,
// the raw string value is returned and the comparison operators promote it to
// a double when both operands are numeric.
//
// Only generated for HASH indexes; JSON indexes reject references to
// undeclared fields at FT.CREATE time (there is no path to resolve them).
class UnindexedHashFieldReference
    : public expr::Expression::AttributeReference {
 public:
  explicit UnindexedHashFieldReference(std::string field)
      : field_(std::move(field)) {}

  expr::Value GetValue(expr::Expression::EvalContext& ctx,
                       const expr::Expression::Record& record) const override;

  void Dump(std::ostream& os) const override;

 private:
  std::string field_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_FILTER_EXPR_H_
