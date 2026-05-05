/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_COMMANDS_FT_AGGREGATE_EXEC
#define VALKEYSEARCH_COMMANDS_FT_AGGREGATE_EXEC

#include <deque>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "src/commands/ft_aggregate_parser.h"
#include "src/expr/expr.h"
#include "src/expr/value.h"
#include "src/indexes/vector_base.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace aggregate {

// Runs the full aggregate reply pipeline on the supplied neighbors:
//   1. Process query setup (key/score record indices)
//   2. Convert neighbors -> Record objects (LOAD pass)
//   3. Execute aggregation stages (APPLY/FILTER/GROUPBY/SORTBY/LIMIT)
//   4. Emit the response
//
// Used by aggregate::AggregateParameters::SendReply and by FT.HYBRID's
// MultiSearchParameters::SendReply (which feeds the post-fusion neighbor list).
absl::Status RunAggregatePipeline(ValkeyModuleCtx* ctx,
                                  std::vector<indexes::Neighbor>& neighbors,
                                  AggregateParameters& parameters);

class Record : public expr::Expression::Record {
 public:
  Record(size_t fields) : fields_(fields) {}
  std::vector<expr::Value> fields_;
  std::vector<std::pair<std::string, expr::Value>> extra_fields_;
  bool operator==(const Record& r) const {
    return fields_ == r.fields_ && extra_fields_ == r.extra_fields_;
  }
  void Dump(std::ostream& os, const AggregateParameters* agg_params) const;
};

using RecordPtr = std::unique_ptr<Record>;

class RecordSet : public std::deque<RecordPtr> {
 public:
  RecordSet(const AggregateParameters* agg_params) : agg_params_(agg_params) {}
  RecordPtr pop_front() {  // NOLINT: needs to follow STL naming convention
    auto p = this->front().release();
    this->std::deque<RecordPtr>::pop_front();
    return RecordPtr(p);
  }
  RecordPtr pop_back() {  // NOLINT: needs to follow the STL naming convention
    auto p = this->back().release();
    this->std::deque<RecordPtr>::pop_back();
    return RecordPtr(p);
  }
  void push_back(
      RecordPtr&& p) {  // NOLINT: needs to follow the STL naming convention
    this->deque<RecordPtr>::emplace_back(std::move(p));
  }
  friend std::ostream& operator<<(std::ostream& os, const RecordSet& rs);

  const AggregateParameters* agg_params_;
};

struct GroupKey {
  absl::InlinedVector<expr::Value, 4> keys_;
  template <typename H>
  friend H AbslHashValue(H h, const GroupKey& k) {
    return H::combine(std::move(h), k.keys_);
  }
  friend bool operator==(const GroupKey& l, const GroupKey& r) {
    return l.keys_ == r.keys_;
  }
  friend std::ostream& operator<<(std::ostream& os, const GroupKey& gk) {
    for (auto& k : gk.keys_) {
      if (&k != &gk.keys_[0]) {
        os << ',';
      }
      os << k;
    }
    return os;
  }
};

inline std::ostream& operator<<(std::ostream& os, const Record& r) {
  for (auto& f : r.fields_) {
    if (&f != &r.fields_[0]) {
      os << ',';
    }
    os << f;
  }
  if (!r.extra_fields_.empty()) {
    os << " : ";
    for (auto& p : r.extra_fields_) {
      if (&p != &r.extra_fields_[0]) {
        os << ',';
      }
      os << p.first << ":" << p.second;
    }
  }
  return os;
}

inline std::ostream& operator<<(std::ostream& os, const Record* r) {
  return os << *r;
}

inline std::ostream& operator<<(std::ostream& os, std::unique_ptr<Record> r) {
  return os << r.get();
}

}  // namespace aggregate
}  // namespace valkey_search

#endif
