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
  if (type_ == indexes::IndexerType::kNumeric) {
    double d;
    if (absl::SimpleAtod(data_view, &d)) {
      return expr::Value(d);
    }
    // The NUMERIC field's raw value is not a parseable number. Count it
    // (surfaced as FT.INFO filter_numeric_conversion_failures) and fall
    // through, treating the raw bytes as a string value.
    filter_record.RecordNumericConversionFailure();
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
