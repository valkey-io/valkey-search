/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/filter_expr.h"

#include <atomic>
#include <cstdint>

#include "absl/strings/numbers.h"
#include "src/index_schema.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search {

namespace {

// Adapter that exposes an IndexSchema::MutatedAttributes map (the fields of the
// document being indexed) through the expression Record interface, and carries
// a counter for numeric-conversion failures observed during evaluation.
//
// FilterRecord is confined to this translation unit:
// IndexSchema::EvaluateFilter (defined below) is its only constructor and
// FilterAttributeReference its only consumer, so it can hold a fully-typed
// pointer to MutatedAttributes without forcing that type into filter_expr.h
// (which would couple it to index_schema.h).
class FilterRecord : public expr::Expression::Record {
 public:
  FilterRecord(const IndexSchema::MutatedAttributes* mutated_attributes,
               std::atomic<uint64_t>* numeric_conversion_failures)
      : mutated_attributes_(mutated_attributes),
        numeric_conversion_failures_(numeric_conversion_failures) {}

  const IndexSchema::MutatedAttributes* GetMutatedAttributes() const {
    return mutated_attributes_;
  }

  void RecordNumericConversionFailure() const {
    if (numeric_conversion_failures_ != nullptr) {
      numeric_conversion_failures_->fetch_add(1, std::memory_order_relaxed);
    }
  }

 private:
  const IndexSchema::MutatedAttributes* mutated_attributes_;
  std::atomic<uint64_t>* numeric_conversion_failures_;
};

}  // namespace

expr::Value FilterAttributeReference::GetValue(
    expr::Expression::EvalContext& ctx,
    const expr::Expression::Record& record) const {
  const auto& filter_record = static_cast<const FilterRecord&>(record);
  const auto* attrs = filter_record.GetMutatedAttributes();
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

bool IndexSchema::EvaluateFilter(
    const MutatedAttributes& mutated_attributes) const {
  FilterRecord record(&mutated_attributes,
                      &stats_.filter_numeric_conversion_failures);
  expr::Expression::EvalContext ctx;
  auto result = compiled_filter_->Evaluate(ctx, record);
  // A Nil ("unknown") result means the filter referenced a missing field;
  // matching Redisearch, such a document is kept. Only a definite false
  // excludes it.
  return result.IsNil() || result.IsTrue();
}

}  // namespace valkey_search
