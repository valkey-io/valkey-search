/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FT_AGGREGATE_PARSER_H
#define VALKEYSEARCH_SRC_COMMANDS_FT_AGGREGATE_PARSER_H

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "src/commands/commands.h"
#include "src/expr/expr.h"
#include "src/expr/value.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "vmsdk/src/command_parser.h"

namespace valkey_search::query {
struct SearchResult;
}

namespace valkey_search {
namespace aggregate {

constexpr absl::string_view kAsParam{"AS"};

class Command;
class Record;
class RecordSet;
class Stage;
class SortBy;

using ArgVector = absl::InlinedVector<expr::Value, 4>;

// A single entry of the LOAD clause. `identifier` is the field/path named in
// the command (with any leading '@' stripped); `alias` is the output name.
// When the LOAD clause has no `AS` alias for the entry, `alias == identifier`.
// Member names mirror query::ReturnAttribute (identifier + alias).
// The name a record column is emitted under in the FT.AGGREGATE reply.
//
// Redisearch echoes the field token as the query wrote it: `@price` comes back
// as `price`, a JSON path `$.price` comes back as `$.price`. valkey-search
// used to emit the schema identifier instead, so on a JSON index -- where the
// identifier is the path and the attribute name is not -- every column came
// back as `$.price`. See issue #1243. Corrected in 1.3.0, gated because the
// old naming is well-defined and an application may be reading it.
std::string OutputNameFor(absl::string_view written_name,
                          absl::string_view schema_identifier);

struct LoadField {
  // The field to fetch. A JSON path is resolved to its attribute name here.
  std::string identifier;
  // The name the column is emitted under: the field token as written, or the
  // `AS` argument when one was given.
  std::string alias;
  // Whether `alias` came from an `AS` clause. Recorded rather than derived
  // from `alias != identifier`, which no longer implies a rename now that a
  // resolved JSON path leaves the two legitimately different.
  bool renamed{false};
};

struct IndexInterface {
  virtual absl::StatusOr<indexes::IndexerType> GetFieldType(
      absl::string_view s) const = 0;
  virtual absl::StatusOr<std::string> GetIdentifier(
      absl::string_view alias) const = 0;
  virtual absl::StatusOr<std::string> GetAlias(
      absl::string_view identifier) const = 0;
};

struct AggregateParameters : public expr::Expression::CompileContext,
                             public QueryCommand {
  ~AggregateParameters() override = default;
  AggregateParameters(int db_num) : QueryCommand(db_num){};
  absl::Status ParseCommand(vmsdk::ArgsIterator& itr) override;
  void SendReply(ValkeyModuleCtx* ctx, query::SearchResult& result) override;
  bool loadall_{false};
  std::vector<LoadField> loads_;
  bool load_key{false};
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

  // Determine if we need full results or if we can optimize with trimming via
  // LIMIT offset & count.
  bool RequiresCompleteResults() const override;
  //
  // Number of records required as output of the query phase.
  // If all records are required, then it will be
  // [0..std::numeric_limits<size_t>::max()]
  //
  query::SerializationRange GetSerializationRange() const;

  //
  // Information for each index position in a Record
  //
  struct AttributeRecordInfo {
    std::string identifier_;  // The identifier of the attribute
    std::string alias_;
    // The name this attribute is emitted under in the FT.AGGREGATE reply.
    // Defaults to identifier_ (preserving legacy behavior); a LOAD ... AS
    // rename overrides it with the requested alias.
    std::string output_name_;
    indexes::IndexerType data_type_;
  };

  friend std::ostream& operator<<(std::ostream& os,
                                  const AttributeRecordInfo& info) {
    os << info.identifier_;
    if (info.identifier_ != info.alias_) {
      os << '(' << info.alias_ << ")";
    }
    return os << ":" << int(info.data_type_);
  }
  //
  // Maps attribute names to their index in the Record.
  //
  absl::flat_hash_map<std::string, size_t> record_indexes_by_alias_;
  //
  // The set of identifiers sourced by some column of the Record. Used during
  // the RecordsMap -> Record conversion to tell a fetched field that feeds a
  // column from one that must be passed through as an extra field. A column
  // finds its own value by identifier, so no identifier -> index map is
  // needed -- and unlike such a map this stays correct when several columns
  // source the same identifier.
  //
  absl::flat_hash_set<std::string> record_identifiers_;
  //
  // Maps indexes in a record into metadata for that index
  //
  std::vector<AttributeRecordInfo> record_info_by_index_;

  // A column is identified by the name it is emitted under, NOT by the field
  // it reads: one LOAD clause may name the same field several times under
  // different output names, and each of those needs a column of its own.
  // Collapsing on the identifier instead would leave only the last-registered
  // output name, silently dropping the others.
  //
  // `record_indexes_by_alias_` can hold a name a column used to be emitted
  // under before a rename, so a hit is only a match when the column still
  // emits that name.
  size_t AddRecordAttribute(absl::string_view identifier,
                            absl::string_view alias,
                            absl::string_view output_name,
                            indexes::IndexerType data_type) {
    if (auto itr = record_indexes_by_alias_.find(output_name);
        itr != record_indexes_by_alias_.end() &&
        record_info_by_index_[itr->second].output_name_ == output_name) {
      return itr->second;
    }
    size_t new_index = record_info_by_index_.size();
    // The attribute name keeps resolving to the first column that carried it;
    // the output name always resolves to the column that emits it.
    record_indexes_by_alias_.emplace(std::string(alias), new_index);
    record_indexes_by_alias_[std::string(output_name)] = new_index;
    record_identifiers_.emplace(std::string(identifier));
    record_info_by_index_.push_back(
        AttributeRecordInfo{.identifier_ = std::string(identifier),
                            .alias_ = std::string(alias),
                            .output_name_ = std::string(output_name),
                            .data_type_ = data_type});
    return new_index;
  }

  // Adds a column reading the same field as `source` but emitted under
  // `output_name`. Used when one LOAD clause renames a field that an earlier
  // entry in the same clause has already claimed an output name for.
  size_t AddRecordAttributeAlias(size_t source, absl::string_view output_name) {
    AttributeRecordInfo info = record_info_by_index_[source];
    info.output_name_ = std::string(output_name);
    size_t new_index = record_info_by_index_.size();
    record_info_by_index_.push_back(std::move(info));
    record_indexes_by_alias_[std::string(output_name)] = new_index;
    return new_index;
  }

  // __key and the score are seeded as the first two columns by ParseCommand so
  // that later stages can find them even after a LOAD ... AS rename has
  // changed the name they are emitted under.
  static constexpr size_t kKeyColumn = 0;
  static constexpr size_t kScoreColumn = 1;

  struct {
    // Variables here are only used during parsing and are cleared at the end.

    // For testing
    IndexInterface* index_interface_;

  } parse_vars_;
  void ClearAtEndOfParse() {
    parse_vars_.index_interface_ = nullptr;
    parse_vars.ClearAtEndOfParse();
  }

  friend std::ostream& operator<<(std::ostream& os,
                                  const AggregateParameters& agg);
};

class Stage {
 public:
  virtual ~Stage() = default;
  virtual absl::Status Execute(RecordSet& records) const = 0;
  virtual void Dump(std::ostream& os) const = 0;
  // std::nullopt means this stage doesn't require a specific number of input
  // records, otherwise it is the number required.
  // For ALL records std::numeric_limits<size_t>::max() is returned.
  virtual std::optional<query::SerializationRange> GetSerializationRange()
      const = 0;
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
  std::optional<query::SerializationRange> GetSerializationRange()
      const override {
    return query::SerializationRange{offset_, offset_ + limit_};
  }
};

class Apply : public Stage {
 public:
  std::unique_ptr<Attribute> name_;
  std::unique_ptr<expr::Expression> expr_;
  absl::Status Execute(RecordSet& records) const override;
  std::optional<query::SerializationRange> GetSerializationRange()
      const override {
    return query::SerializationRange::All();
  }
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
  std::optional<query::SerializationRange> GetSerializationRange()
      const override {
    return query::SerializationRange::All();
  }
  void Dump(std::ostream& os) const override {
    os << "FILTER: " << expr_.get();
  }
};

class GroupBy : public Stage {
 public:
  absl::Status Execute(RecordSet& records) const override;
  std::optional<query::SerializationRange> GetSerializationRange()
      const override {
    return query::SerializationRange::All();
  }
  struct ReducerInstance {
    virtual ~ReducerInstance() = default;
    virtual void ProcessRecord(const ArgVector& value) = 0;
    virtual expr::Value GetResult() const = 0;
  };

  struct Reducer {
    std::string name_;
    std::unique_ptr<Attribute> output_;
    std::vector<std::unique_ptr<expr::Expression>> args_;

    virtual ~Reducer() = default;
    virtual std::unique_ptr<ReducerInstance> MakeInstance() = 0;

    friend std::ostream& operator<<(std::ostream& os, const Reducer& r) {
      os << r.name_ << '(';
      for (auto& a : r.args_) {
        if (&a != &r.args_[0]) {
          os << ',';
        }
        os << a.get();
      }
      return os << ')';
    }
  };

  using ReducerInfo = absl::StatusOr<std::unique_ptr<Reducer>> (*)(
      std::string_view name, AggregateParameters&, vmsdk::ArgsIterator&);
  static absl::flat_hash_map<std::string, ReducerInfo> reducerTable;

  absl::InlinedVector<std::unique_ptr<Attribute>, 4> groups_;
  absl::InlinedVector<std::unique_ptr<Reducer>, 4> reducers_;

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
      os << ' ' << *r << " => " << r->output_->name_;
    }
  }
};

class SortBy : public Stage {
 public:
  absl::Status Execute(RecordSet& records) const override;
  std::optional<query::SerializationRange> GetSerializationRange()
      const override {
    return query::SerializationRange::All();
  }
  enum Direction { kASC, kDESC };
  struct SortKey {
    Direction direction_;
    std::unique_ptr<expr::Expression> expr_;
  };
  size_t max_{10};
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
          CHECK(false);
      }
      os << k.expr_.get();
    }
    if (max_) {
      os << " MAX:" << max_;
    }
  }
};

absl::StatusOr<std::unique_ptr<QueryCommand>> ParseAggregateParameters(
    ValkeyModuleCtx* ctx, ValkeyModuleString** argv, int argc,
    const SchemaManager& schema_manager);

//
// Only here for unit tests
//
vmsdk::KeyValueParser<AggregateParameters> CreateAggregateParser();

}  // namespace aggregate
}  // namespace valkey_search
#endif
