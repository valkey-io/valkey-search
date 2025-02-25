#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_AGGREGATE_PARSER_H
#define VALKEYSEARCH_SRC_COMMANDS_FT_AGGREGATE_PARSER_H

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "src/expr/expr.h"
#include "src/expr/value.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search {
namespace aggregate {

class Command;
class Record;
class RecordSet;
class Stage;

struct IndexInterface {
  virtual absl::StatusOr<indexes::IndexerType> GetFieldType(
      absl::string_view s) const = 0;
};

struct AggregateParameters : public expr::Expression::CompileContext,
                             public query::VectorSearchParameters {
  bool loadall_{false};
  std::vector<vmsdk::UniqueRedisString> loads_;
  bool addscores_{false};
  std::vector<std::unique_ptr<Stage>> stages_;

  absl::StatusOr<std::unique_ptr<expr::Expression::AttributeReference>>
  MakeReference(const absl::string_view s, bool create) override;

  absl::StatusOr<expr::Value> GetParam(
      const absl::string_view s) const override {
    auto it = parse_vars.params.find(s);
    if (it != parse_vars.params.end()) {
      it->second.first++;
      return expr::Value(it->second.second);
    } else {
      return absl::NotFoundError(absl::StrCat("parameter ", s, " not found."));
    }
  }

  absl::flat_hash_map<std::string, size_t> attr_record_indexes_;
  std::vector<std::string> attr_record_names_;
  static constexpr size_t kKeyIndex = 0;
  static constexpr size_t kScoreIndex = 1;

  size_t AddRecordAttribute(absl::string_view attr) {
    size_t new_index = attr_record_names_.size();
    auto [itr, inserted] =
        attr_record_indexes_.try_emplace(std::string(attr), new_index);
    if (!inserted) {
      return itr->second;
    } else {
      attr_record_names_.push_back(std::string(attr));
      return new_index;
    }
  }

  struct {
    // Variables here are only used during parsing and are cleared at the end.

    // For testing
    IndexInterface* index_interface_;

  } parse_vars_;
  void ClearAtEndOfParse() {
    parse_vars_.index_interface_ = nullptr;
    parse_vars.ClearAtEndOfParse();
  }

  AggregateParameters(IndexInterface* index_interface) {
    parse_vars_.index_interface_ = index_interface;
  }

  friend std::ostream& operator<<(std::ostream& os,
                                  const AggregateParameters& agg);
};

class Stage {
 public:
  virtual ~Stage() = default;
  virtual absl::Status Execute(RecordSet& records) const = 0;

  virtual void Dump(std::ostream& os) const = 0;
  friend std::ostream& operator<<(std::ostream& os, const Stage& s) {
    s.Dump(os);
    return os;
  }

 private:
  // Common per-stage stats.
};

struct Attribute : expr::Expression::AttributeReference {
  Attribute(absl::string_view name, size_t ix)
      : expr::Expression::AttributeReference(),
        name_(name),
        record_index_(ix) {}
  std::string name_;
  size_t record_index_;
  void Dump(std::ostream& os) const override { os << name_; }
  expr::Value GetValue(expr::Expression::EvalContext& ctx,
                       const expr::Expression::Record& record) const override;
};

class Limit : public Stage {
 public:
  size_t offset_;
  size_t limit_;
  void Dump(std::ostream& os) const override {
    os << "LIMIT: " << offset_ << " " << limit_;
  }
  absl::Status Execute(RecordSet& records) const override;
};

class Apply : public Stage {
 public:
  std::unique_ptr<Attribute> name_;
  std::unique_ptr<expr::Expression> expr_;
  absl::Status Execute(RecordSet& records) const override;
  void Dump(std::ostream& os) const override {
    os << "APPLY: ";
    name_->Dump(os);
    os << " := ";
    expr_->Dump(os);
  }
};

class Filter : public Stage {
 public:
  std::unique_ptr<expr::Expression> expr_;
  absl::Status Execute(RecordSet& records) const override;
  void Dump(std::ostream& os) const override {
    os << "FILTER: " << expr_.get();
  }
};

class GroupBy : public Stage {
 public:
  absl::Status Execute(RecordSet& records) const override;
  struct ReducerInstance {
    virtual ~ReducerInstance() = default;
    virtual void ProcessRecord(absl::InlinedVector<expr::Value, 4>& values) = 0;
    virtual expr::Value GetResult() const = 0;
  };
  struct ReducerInfo {
    std::string name_;
    size_t min_nargs_{0};
    size_t max_nargs_{0};
    std::unique_ptr<ReducerInstance> (*make_instance)();
  };
  static absl::flat_hash_map<std::string, ReducerInfo> reducerTable;

  struct Reducer {
    std::unique_ptr<Attribute> output_;
    std::vector<std::unique_ptr<expr::Expression>> args_;
    ReducerInfo* info_;
    friend std::ostream& operator<<(std::ostream& os, const Reducer& r) {
      os << r.info_->name_ << '(';
      for (auto& a : r.args_) {
        if (&a != &r.args_[0]) {
          os << ',';
        }
        os << a.get();
      }
      return os << ')';
    }
  };

  absl::InlinedVector<std::unique_ptr<Attribute>, 4> groups_;
  absl::InlinedVector<Reducer, 4> reducers_;

  void Dump(std::ostream& os) const override {
    os << "GROUPBY ";
    for (auto& g : groups_) {
      if (&g != &groups_[0]) {
        os << ',';
      }
      os << '@' << g.get();
    }
    for (auto& r : reducers_) {
      if (&r != &reducers_[0]) {
        os << ',';
      }
      os << ' ' << r << " => " << r.output_->name_;
    }
  }
};

class SortBy : public Stage {
 public:
  absl::Status Execute(RecordSet& records) const override;
  enum Direction { kASC, kDESC };
  struct SortKey {
    Direction direction_;
    std::unique_ptr<expr::Expression> expr_;
  };
  std::optional<size_t> max_;
  absl::InlinedVector<SortKey, 4> sortkeys_;
  void Dump(std::ostream& os) const override {
    os << "SORTBY:";
    for (auto& k : sortkeys_) {
      switch (k.direction_) {
        case Direction::kASC:
          os << " ASC:";
          break;
        case Direction::kDESC:
          os << " DESC:";
          break;
        default:
          RedisModule_Assert(false);
      }
      os << k.expr_.get();
    }
    if (max_) {
      os << " MAX:" << *max_;
    }
  }
};

absl::StatusOr<std::unique_ptr<AggregateParameters>> ParseAggregateParameters(
    RedisModuleCtx* ctx, RedisModuleString** argv, int argc,
    const SchemaManager& schema_manager);

//
// Only here for unit tests
//
vmsdk::KeyValueParser<AggregateParameters> CreateAggregateParser();

}  // namespace aggregate
}  // namespace valkey_search
#endif