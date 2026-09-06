/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_FILTER_EXPR_H_
#define VALKEYSEARCH_SRC_FILTER_EXPR_H_

#include <string>

#include "absl/strings/string_view.h"
#include "src/expr/expr.h"
#include "src/expr/value.h"
#include "src/index_schema.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

class AttributeDataType;

// Adapter exposing an IndexSchema::MutatedAttributes map (the declared fields
// already fetched for the document being indexed) through the expression
// Record interface, and carrying the index's stats so evaluation can count
// numeric-conversion failures.
//
// Declared here rather than kept private to filter_expr.cc because
// IndexSchema::EvaluateFilter, which is its only constructor, lives in
// index_schema.cc; FilterAttributeReference downcasts to it, so the two
// cannot be separated. That is what couples this header to index_schema.h.
class FilterRecord : public expr::Expression::Record {
 public:
  FilterRecord(const IndexSchema::MutatedAttributes& mutated_attributes,
               IndexSchema::Stats& stats)
      : mutated_attributes_(mutated_attributes), stats_(stats) {}

  const IndexSchema::MutatedAttributes& GetMutatedAttributes() const {
    return mutated_attributes_;
  }

  // Counts a NUMERIC field whose raw value would not parse as a double.
  // A plain increment: the filter is evaluated only on the main thread.
  void RecordNumericConversionFailure() const {
    ++stats_.filter_numeric_conversion_failures;
  }

 private:
  const IndexSchema::MutatedAttributes& mutated_attributes_;
  IndexSchema::Stats& stats_;
};

// Per-evaluation context carrying the still-open key of the document being
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
  vmsdk::UniqueValkeyString GetKeyField(absl::string_view identifier) const;

 private:
  ValkeyModuleCtx* ctx_;
  ValkeyModuleKey* open_key_;
  absl::string_view key_;
  const AttributeDataType* data_type_;
};

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
