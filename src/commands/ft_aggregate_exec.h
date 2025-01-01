#ifndef VALKEYSEARCH_COMMANDS_FT_AGGREGATE_EXEC
#define VALKEYSEARCH_COMMANDS_FT_AGGREGATE_EXEC

#include "src/expr/value.h"
#include "src/expr/expr.h"

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/hash/hash.h"

#include <compare>
#include <deque>

namespace valkey_search {
namespace aggregate {

class Record : public expr::Expression::Record {
  public:
    Record(size_t referenced) : referenced_(referenced) {}
    std::vector<expr::Value> referenced_;
    std::vector<std::pair<std::string, expr::Value>> unreferenced_;
    bool operator==(const Record& r) const { 
      return referenced_ == r.referenced_ && unreferenced_ == r.unreferenced_;
    }
};

using RecordPtr = std::unique_ptr<Record>;

class RecordSet : public std::deque<RecordPtr> {
  public:
  RecordPtr pop_front() { // NOLINT: needs to follow STL naming convention
    auto p = this->front().release();
    this->std::deque<RecordPtr>::pop_front();
    return RecordPtr(p);
  }
  RecordPtr pop_back() { // NOLINT: needs to follow the STL naming convention
    auto p = this->back().release();
    this->std::deque<RecordPtr>::pop_back();
    return RecordPtr(p);
  }
  void push_back(RecordPtr&& p) { // NOLINT: needs to follow the STL naming convention
    this->deque<RecordPtr>::emplace_back(std::move(p));
  }
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
      if (&k == &gk.keys_[0]) {
        os << ',';
      }
      os << k;
    }
    return os;
  }
};

std::ostream& operator<<(std::ostream& os, const Record& r) {
  for (auto& f : r.referenced_) {
    if (&f != &r.referenced_[0]) {
      os << ',';
    }
    os << f;
  }
  if (!r.unreferenced_.empty()) {
    os << " : ";
    for (auto& p : r.unreferenced_) {
      if (&p != &r.unreferenced_[0]) {
        os << ',';
      }
      os << p.first << ":" << p.second;
    }
  }
  return os;
}

std::ostream& operator<<(std::ostream& os, const Record *r) { return os << *r; }
std::ostream& operator<<(std::ostream& os, std::unique_ptr<Record> r) { return os << r.get(); }

}
}

#endif
