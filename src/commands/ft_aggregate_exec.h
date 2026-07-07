/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_COMMANDS_FT_AGGREGATE_EXEC
#define VALKEYSEARCH_COMMANDS_FT_AGGREGATE_EXEC

#include <deque>
#include <memory>

#include "absl/container/inlined_vector.h"
#include "src/commands/ft_aggregate_parser.h"
#include "src/expr/expr.h"
#include "src/expr/value.h"

namespace valkey_search {
namespace aggregate {

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

// GK algorithm error bound for the QUANTILE reducer.
// An epsilon of 0.01 guarantees the returned quantile is within 1% of the
// true rank.
constexpr double kQuantileEpsilon = 0.01;

// Instrumentation counters for the QUANTILE reducer's GK algorithm.
// Exposed for testing to verify all internal paths are exercised.
struct QuantileStats {
  size_t flush_initial_count{0};  // First flush (empty samples)
  size_t flush_merge_count{0};    // Merge flush (into existing samples)
  size_t compress_count{0};       // Compression passes performed
  size_t insert_count{0};         // Total values inserted
  size_t samples_merged{0};       // Samples removed during compression
};

// Creates a QuantileReducer configured with the given quantile value.
// The returned reducer owns its stats, `stats_out` points into the reducer
// and remains valid for the reducer's lifetime.
std::unique_ptr<GroupBy::Reducer> MakeQuantileReducer(
    double quantile, QuantileStats*& stats_out);

}  // namespace aggregate
}  // namespace valkey_search

#endif
