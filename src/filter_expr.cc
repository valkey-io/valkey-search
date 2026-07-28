/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/filter_expr.h"

#include <cstdint>
#include <string>
#include <utility>

#include "absl/strings/numbers.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

// Adapter that exposes an IndexSchema::MutatedAttributes map (the declared
// fields already fetched for the document being indexed) through the
// expression Record interface, and carries a counter for numeric-conversion
// failures observed during evaluation.
//
// FilterRecord is confined to this translation unit:
// IndexSchema::EvaluateFilter (defined below) is its only constructor and
// FilterAttributeReference its only consumer, so it can hold a fully-typed
// pointer to MutatedAttributes without forcing that type into filter_expr.h
// (which would couple it to index_schema.h). The counter is a plain uint64_t
// because the filter is evaluated only on the main thread.
class FilterRecord : public expr::Expression::Record {
 public:
  FilterRecord(const IndexSchema::MutatedAttributes* mutated_attributes,
               uint64_t* numeric_conversion_failures)
      : mutated_attributes_(mutated_attributes),
        numeric_conversion_failures_(numeric_conversion_failures) {}

  const IndexSchema::MutatedAttributes* GetMutatedAttributes() const {
    return mutated_attributes_;
  }

  void RecordNumericConversionFailure() const {
    if (numeric_conversion_failures_ != nullptr) {
      ++(*numeric_conversion_failures_);
    }
  }

 private:
  const IndexSchema::MutatedAttributes* mutated_attributes_;
  uint64_t* numeric_conversion_failures_;
};

// Per-evaluation context that carries the still-open key of the document being
// indexed, so that references to fields NOT in the index schema
// (UnindexedHashFieldReference) can read those fields directly off the key.
//
// Only meaningful for HASH indexes; open_key_ is valid for the duration of a
// single synchronous EvaluateFilter call on the main thread.
class FilterEvalContext : public expr::Expression::EvalContext {
 public:
  FilterEvalContext(ValkeyModuleCtx* ctx, ValkeyModuleKey* open_key,
                    absl::string_view key, const AttributeDataType* data_type)
      : ctx_(ctx), open_key_(open_key), key_(key), data_type_(data_type) {}

  // Fetch a single field's raw value off the open key by identifier. Returns
  // nullptr when the key is absent, the field is missing, or the lookup fails.
  vmsdk::UniqueValkeyString GetKeyField(absl::string_view identifier) const {
    if (open_key_ == nullptr || data_type_ == nullptr) {
      return nullptr;
    }
    auto record = data_type_->GetRecord(ctx_, open_key_, key_, identifier);
    if (!record.ok()) {
      return nullptr;
    }
    return std::move(record).value();
  }

 private:
  ValkeyModuleCtx* ctx_;
  ValkeyModuleKey* open_key_;
  absl::string_view key_;
  const AttributeDataType* data_type_;
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

bool IndexSchema::EvaluateFilter(const MutatedAttributes& mutated_attributes,
                                 ValkeyModuleCtx* ctx,
                                 ValkeyModuleKey* open_key,
                                 absl::string_view key) const {
  FilterRecord record(&mutated_attributes,
                      &stats_.filter_numeric_conversion_failures);
  FilterEvalContext eval_ctx(ctx, open_key, key, attribute_data_type_.get());
  auto result = compiled_filter_->Evaluate(eval_ctx, record);
  // A Nil ("unknown") result means the filter referenced a missing field;
  // matching Redisearch, such a document is kept. Only a definite false
  // excludes it.
  return result.IsNil() || result.IsTrue();
}

}  // namespace valkey_search
