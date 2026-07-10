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

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_FILTER_EXPR_H_
