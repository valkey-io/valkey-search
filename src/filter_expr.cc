/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/filter_expr.h"

#include "absl/strings/numbers.h"
#include "src/index_schema.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search {

expr::Value FilterAttributeReference::GetValue(
    expr::Expression::EvalContext& ctx,
    const expr::Expression::Record& record) const {
  const auto& filter_record = static_cast<const FilterRecord&>(record);
  const auto* attrs = static_cast<const IndexSchema::MutatedAttributes*>(
      filter_record.GetMutatedAttributes());
  auto itr = attrs->find(alias_);
  if (itr == attrs->end() || !itr->second.data) {
    return expr::Value(expr::Value::Nil("Field Missing"));
  }
  auto data_view = vmsdk::ToStringView(itr->second.data.get());
  if (type_ == indexes::IndexerType::kNumeric) {
    double d;
    if (absl::SimpleAtod(data_view, &d)) {
      return expr::Value(d);
    }
    VMSDK_LOG(WARNING, nullptr)
        << "FILTER: Failed to parse numeric field '" << alias_
        << "' as double, raw value: [" << data_view
        << "] (length=" << data_view.size() << ")";
  }
  return expr::Value(data_view);
}

void FilterAttributeReference::Dump(std::ostream& os) const {
  os << "@" << alias_;
}

}  // namespace valkey_search
