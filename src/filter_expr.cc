/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/filter_expr.h"

#include <string>
#include <utility>

#include "absl/strings/numbers.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search {

// Fetch a single field's raw value off the open key by identifier. Returns
// nullptr when the key is absent, the field is missing, or the lookup fails.
vmsdk::UniqueValkeyString FilterEvalContext::GetKeyField(
    absl::string_view identifier) const {
  if (open_key_ == nullptr || data_type_ == nullptr) {
    return nullptr;
  }
  auto record = data_type_->GetAttribute(ctx_, open_key_, key_, identifier);
  if (!record.ok()) {
    return nullptr;
  }
  return std::move(record).value();
}

expr::Value FilterAttributeReference::GetValue(
    expr::Expression::EvalContext& ctx,
    const expr::Expression::Record& record) const {
  const auto& filter_record = static_cast<const FilterRecord&>(record);
  const auto& attrs = filter_record.GetMutatedAttributes();
  auto itr = attrs.find(alias_);
  if (itr == attrs.end() || itr->second.IsNull()) {
    return expr::Value(expr::Value::Nil("Field Missing"));
  }
  auto data_view = itr->second.GetStringView();
  // Whether a NUMERIC field is a *runtime number* to a FILTER depends on the
  // key type, and Redisearch draws the same line:
  //
  //   JSON - the document carries real types, so a NUMERIC field is a number.
  //          `@price > @rating` compares numerically.
  //   HASH - every value is bytes, so a NUMERIC field is not. `@price >
  //   @rating`
  //          compares byte by byte, and only a numeric literal or a
  //          number-returning function makes a comparison numeric.
  //
  // Parsing unconditionally would make the HASH case numeric; never parsing
  // would make the JSON case a string comparison. Both diverge.
  if (type_ == indexes::IndexerType::kNumeric &&
      data_type_ == data_model::ATTRIBUTE_DATA_TYPE_JSON) {
    double d;
    if (absl::SimpleAtod(data_view, &d)) {
      return expr::Value(d);
    }
  }
  return expr::Value(data_view);
}

void FilterAttributeReference::Dump(std::ostream& os) const {
  os << "@" << alias_;
}

expr::Value UnindexedHashFieldReference::GetValue(
    expr::Expression::EvalContext& ctx,
    const expr::Expression::Record& record) const {
  const auto& eval_ctx = static_cast<const FilterEvalContext&>(ctx);
  auto value = eval_ctx.GetKeyField(field_);
  if (!value) {
    return expr::Value(expr::Value::Nil("Field Missing"));
  }
  // Undeclared fields have no schema type, so the value is returned as a string
  // that the comparison operators promote to a double when numeric. The bytes
  // are copied into an owning Value because `value` is released when this call
  // returns.
  return expr::Value(std::string(vmsdk::ToStringView(value.get())));
}

void UnindexedHashFieldReference::Dump(std::ostream& os) const {
  os << "@" << field_;
}

}  // namespace valkey_search
