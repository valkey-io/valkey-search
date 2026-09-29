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
  // Slots start out missing: only the attributes the key actually has get
  // assigned, and the reply leaves the rest out.
  Record(size_t fields) : fields_(fields, expr::Value::Missing()) {}
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
    // Not expr::Value's `==`, which reads an unordered comparison as a match
    // where the caller has asked for one. Grouping needs identity: two
    // missing keys are the same group, a missing key and a present one never
    // are. Going through the operator put every record lacking the field into
    // whichever group the hash map happened to compare it against.
    if (l.keys_.size() != r.keys_.size()) {
      return false;
    }
    for (size_t i = 0; i < l.keys_.size(); ++i) {
      const bool l_nil = l.keys_[i].IsNil();
      const bool r_nil = r.keys_[i].IsNil();
      if (l_nil || r_nil) {
        if (l_nil != r_nil) {
          return false;
        }
        continue;  // both missing: the same group.
      }
      if (expr::Compare(l.keys_[i], r.keys_[i]) != expr::Ordering::kEQUAL) {
        return false;
      }
    }
    return true;
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
