/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/commands/ft_aggregate_parser.h"

#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"

// #define DBG std::cerr
#define DBG 0 && std::cerr

namespace valkey_search {
namespace aggregate {

constexpr absl::string_view kAddScoresParam{"ADDSCORES"};
constexpr absl::string_view kApplyParam{"APPLY"};
constexpr absl::string_view kAscParam{"ASC"};
constexpr absl::string_view kDescParam{"DESC"};
constexpr absl::string_view kDialectParam{"DIALECT"};
constexpr absl::string_view kFilterParam{"FILTER"};
constexpr absl::string_view kGroupByParam{"GROUPBY"};
constexpr absl::string_view kLimitParam{"LIMIT"};
constexpr absl::string_view kLoadParam{"LOAD"};
constexpr absl::string_view kMaxParam{"MAX"};
constexpr absl::string_view kParamsParam{"PARAMS"};
constexpr absl::string_view kReduceParam{"REDUCE"};
constexpr absl::string_view kSortByParam{"SORTBY"};
constexpr absl::string_view kTimeoutParam{"TIMEOUT"};
constexpr absl::string_view kSlopParam{"SLOP"};
constexpr absl::string_view kInorder{"INORDER"};
constexpr absl::string_view kVerbatim{"VERBATIM"};

std::unique_ptr<vmsdk::ParamParser<AggregateParameters>> ConstructLoadParser() {
  return std::make_unique<vmsdk::ParamParser<AggregateParameters>>(
      [](AggregateParameters &parameters,
         vmsdk::ArgsIterator &itr) -> absl::Status {
        VMSDK_ASSIGN_OR_RETURN(auto count_string, itr.Get());
        itr.Next();
        if (vmsdk::ToStringView(count_string) == "*") {
          parameters.loadall_ = true;
          return absl::OkStatus();
        }
        uint32_t cnt{0};
        VMSDK_ASSIGN_OR_RETURN(cnt, vmsdk::To<uint32_t>(count_string));
        // `cnt` is a token budget. Each LOAD entry is a field/path, optionally
        // followed by `AS <alias>`; the `AS` keyword and its alias count
        // against the budget (matching RediSearch).
        uint32_t consumed = 0;
        // Output names already assigned by an `AS` rename in this clause. Two
        // renames targeting the same alias are rejected (see below).
        absl::flat_hash_set<std::string> renamed_aliases;
        while (consumed < cnt) {
          std::string load;
          VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, load));
          ++consumed;
          if (load.empty() || load == "@") {
            return absl::InvalidArgumentError(
                "Empty argument in LOAD clause not allowed");
          }
          std::string identifier = load[0] == '@' ? load.substr(1) : load;
          std::string alias = identifier;
          // Honoring `AS <alias>` in the LOAD clause is a compatibility fix
          // introduced in 1.3. Older emulated releases treat `AS` as just
          // another field name (the legacy behavior preserved below).
          absl::Status as_status = VALKEY_SEARCH_COMPATIBILITY_FIX(
              1, 3, 0, "ft_aggregate_load_as",
              [&]() -> absl::Status {
                // If the entry is a schema identifier (e.g. a JSON path) rather
                // than an alias, resolve it to the alias so it can be fetched.
                // With no AS clause the default output name then remains the
                // identifier, matching how single-field loads already behave.
                auto &index = *parameters.parse_vars_.index_interface_;
                if (!index.GetFieldType(identifier).ok()) {
                  if (auto resolved = index.GetAlias(identifier);
                      resolved.ok()) {
                    identifier = *std::move(resolved);
                    alias = identifier;
                  }
                }
                if (consumed < cnt && itr.PopIfNextIgnoreCase(kAsParam)) {
                  ++consumed;
                  std::string rename;
                  if (consumed >= cnt) {
                    return absl::InvalidArgumentError(
                        "`AS` argument to LOAD clause is missing/invalid");
                  }
                  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, rename));
                  ++consumed;
                  if (rename.empty()) {
                    return absl::InvalidArgumentError(
                        "`AS` argument to LOAD clause is missing/invalid");
                  }
                  alias = std::move(rename);
                }
                return absl::OkStatus();
              },
              []() -> absl::Status { return absl::OkStatus(); });
          VMSDK_RETURN_IF_ERROR(as_status);
          if (alias != identifier) {
            if (!renamed_aliases.insert(alias).second) {
              // Intentionally stricter than RediSearch (which keeps the first
              // and silently drops the rest): reject two LOAD ... AS renames
              // that target the same alias.
              return absl::InvalidArgumentError(absl::StrCat(
                  "Duplicate `AS` alias `", alias, "` in LOAD clause"));
            }
            // LOAD precedes APPLY/SORTBY/FILTER in the command, so register
            // the rename now to make `@alias` resolvable in those later
            // stages. Entries that can't be resolved against the index here
            // (e.g. __key, the score field) are renamed afterwards in
            // ManipulateReturnsClause.
            auto ref = parameters.MakeReference(identifier, false);
            if (ref.ok()) {
              if (auto *attr = dynamic_cast<Attribute *>(ref->get())) {
                parameters.record_info_by_index_[attr->record_index_]
                    .output_name_ = alias;
                parameters.record_indexes_by_alias_.emplace(
                    alias, attr->record_index_);
              }
            }
          }
          parameters.loads_.emplace_back(LoadField{
              .identifier = std::move(identifier), .alias = std::move(alias)});
        }
        return absl::OkStatus();
      });
}

std::unique_ptr<vmsdk::ParamParser<AggregateParameters>>
ConstructApplyParser() {
  return std::make_unique<vmsdk::ParamParser<AggregateParameters>>(
      [](AggregateParameters &parameters,
         vmsdk::ArgsIterator &itr) -> absl::Status {
        auto apply = std::make_unique<Apply>();
        VMSDK_ASSIGN_OR_RETURN(auto expr_string, itr.PopNext());
        VMSDK_ASSIGN_OR_RETURN(
            apply->expr_, expr::Expression::Compile(
                              parameters, vmsdk::ToStringView(expr_string)));
        if (!itr.PopIfNextIgnoreCase(kAsParam)) {
          return absl::InvalidArgumentError(
              "`AS` argument to APPLY clause is missing/invalid");
        }
        VMSDK_ASSIGN_OR_RETURN(auto name_string, itr.PopNext());
        VMSDK_ASSIGN_OR_RETURN(
            auto name,
            parameters.MakeReference(vmsdk::ToStringView(name_string), true));
        apply->name_ = std::unique_ptr<Attribute>(
            dynamic_cast<Attribute *>(name.release()));
        DBG << *apply << "\n";
        parameters.stages_.emplace_back(std::move(apply));
        return absl::OkStatus();
      });
}

std::unique_ptr<vmsdk::ParamParser<AggregateParameters>>
ConstructFilterParser() {
  return std::make_unique<vmsdk::ParamParser<AggregateParameters>>(
      [](AggregateParameters &parameters,
         vmsdk::ArgsIterator &itr) -> absl::Status {
        auto filter = std::make_unique<Filter>();
        VMSDK_ASSIGN_OR_RETURN(auto expr_string, itr.PopNext());
        VMSDK_ASSIGN_OR_RETURN(
            filter->expr_, expr::Expression::Compile(
                               parameters, vmsdk::ToStringView(expr_string)));
        parameters.stages_.emplace_back(std::move(filter));
        return absl::OkStatus();
      });
}

std::unique_ptr<vmsdk::ParamParser<AggregateParameters>>
ConstructLimitParser() {
  return std::make_unique<vmsdk::ParamParser<AggregateParameters>>(
      [](AggregateParameters &parameters,
         vmsdk::ArgsIterator &itr) -> absl::Status {
        auto stage = std::make_unique<Limit>();
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, stage->offset_));
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, stage->limit_));
        parameters.stages_.emplace_back(std::move(stage));
        return absl::OkStatus();
      });
}

std::unique_ptr<vmsdk::ParamParser<AggregateParameters>>
ConstructParamsParser() {
  return std::make_unique<vmsdk::ParamParser<AggregateParameters>>(
      [](AggregateParameters &parameters,
         vmsdk::ArgsIterator &itr) -> absl::Status {
        uint32_t cnt{0};
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, cnt));
        for (auto i = 0; i < cnt; i += 2) {
          VMSDK_ASSIGN_OR_RETURN(auto name, itr.PopNext());
          VMSDK_ASSIGN_OR_RETURN(auto value, itr.PopNext());
          for (auto c : vmsdk::ToStringView(name)) {
            if (!std::isalnum(c) && c != '_') {
              return absl::InvalidArgumentError(
                  absl::StrCat("Parameter name `", vmsdk::ToStringView(name),
                               "` contains an invalid character."));
            }
          }
          parameters.parse_vars.params[vmsdk::ToStringView(name)] =
              std::make_pair(0, vmsdk::ToStringView(value));
        }
        DBG << "After params: " << parameters << "\n";
        return absl::OkStatus();
      });
}

std::unique_ptr<vmsdk::ParamParser<AggregateParameters>>
ConstructSortByParser() {
  return std::make_unique<vmsdk::ParamParser<AggregateParameters>>(
      [](AggregateParameters &parameters,
         vmsdk::ArgsIterator &itr) -> absl::Status {
        auto sortby = std::make_unique<SortBy>();
        uint32_t cnt{0};
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, cnt));
        DBG << "Parsing sortby " << cnt << "\n";
        for (auto i = 0; i < cnt; ++i) {
          VMSDK_ASSIGN_OR_RETURN(auto expr_string, itr.PopNext(),
                                 _ << " in SORTBY stage");
          DBG << "Parsing field " << vmsdk::ToStringView(expr_string) << "\n";
          VMSDK_ASSIGN_OR_RETURN(
              auto expr,
              expr::Expression::Compile(parameters,
                                        vmsdk::ToStringView(expr_string)),
              _ << " in SORTBY stage");
          SortBy::Direction direction = SortBy::Direction::kASC;
          if (itr.PopIfNextIgnoreCase(kAscParam)) {
            direction = SortBy::Direction::kASC;
            i++;
          } else if (itr.PopIfNextIgnoreCase(kDescParam)) {
            direction = SortBy::Direction::kDESC;
            i++;
          }
          DBG << "Got Sortby field: " << *expr << "\n";
          sortby->sortkeys_.emplace_back(
              SortBy::SortKey{direction, std::move(expr)});
        }
        if (itr.PopIfNextIgnoreCase(kMaxParam)) {
          size_t max{0};
          VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, max));
          sortby->max_ = max;
        }
        parameters.stages_.emplace_back(std::move(sortby));
        DBG << "After sortby: " << parameters << "\n";
        return absl::OkStatus();
      });
}

std::unique_ptr<vmsdk::ParamParser<AggregateParameters>>
ConstructGroupByParser() {
  return std::make_unique<vmsdk::ParamParser<AggregateParameters>>(
      [](AggregateParameters &parameters,
         vmsdk::ArgsIterator &itr) -> absl::Status {
        auto groupby = std::make_unique<GroupBy>();
        uint32_t cnt{0};
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, cnt));
        if (cnt == 0) {
          return absl::OutOfRangeError("Groupby requires arguments");
        }
        for (auto i = 0; i < cnt; ++i) {
          VMSDK_ASSIGN_OR_RETURN(auto group_string, itr.PopNext());
          auto group_string_view = vmsdk::ToStringView(group_string);
          if (group_string_view.empty() || group_string_view[0] != '@') {
            return absl::InvalidArgumentError(
                absl::StrCat("Group field reference must start with '@'"));
          }
          group_string_view.remove_prefix(1);
          VMSDK_ASSIGN_OR_RETURN(
              auto group, parameters.MakeReference(group_string_view, false));
          groupby->groups_.emplace_back(std::unique_ptr<Attribute>(
              dynamic_cast<Attribute *>(group.release())));
        }
        while (itr.PopIfNextIgnoreCase(kReduceParam)) {
          VMSDK_ASSIGN_OR_RETURN(auto name, itr.PopNext(),
                                 _ << "Missing Reducer name");
          auto uc_name =
              expr::FuncUpper(expr::Value(vmsdk::ToStringView(name)));
          auto reducer_itr = GroupBy::reducerTable.find(uc_name.AsStringView());
          if (reducer_itr == GroupBy::reducerTable.end()) {
            return absl::NotFoundError(absl::StrCat("reducer function `",
                                                    vmsdk::ToStringView(name),
                                                    "` not found"));
          }

          VMSDK_ASSIGN_OR_RETURN(
              std::unique_ptr<GroupBy::Reducer> r,
              reducer_itr->second(uc_name.AsStringView(), parameters, itr));
          groupby->reducers_.emplace_back(std::move(r));
        }
        parameters.stages_.emplace_back(std::move(groupby));
        DBG << "After groupby: " << parameters << "\n";
        return absl::OkStatus();
      });
}

vmsdk::KeyValueParser<AggregateParameters> CreateAggregateParser() {
  vmsdk::KeyValueParser<AggregateParameters> parser;
  parser.AddParamParser(kDialectParam,
                        GENERATE_VALUE_PARSER(AggregateParameters, dialect));
  parser.AddParamParser(kTimeoutParam,
                        GENERATE_VALUE_PARSER(AggregateParameters, timeout_ms));
  parser.AddParamParser(kAddScoresParam,
                        GENERATE_FLAG_PARSER(AggregateParameters, addscores_));
  parser.AddParamParser(kSlopParam,
                        GENERATE_VALUE_PARSER(AggregateParameters, slop));
  parser.AddParamParser(kInorder,
                        GENERATE_FLAG_PARSER(AggregateParameters, inorder));
  parser.AddParamParser(kVerbatim,
                        GENERATE_FLAG_PARSER(AggregateParameters, verbatim));
  parser.AddParamParser(kLoadParam, ConstructLoadParser());
  parser.AddParamParser(kApplyParam, ConstructApplyParser());
  parser.AddParamParser(kFilterParam, ConstructFilterParser());
  parser.AddParamParser(kGroupByParam, ConstructGroupByParser());
  parser.AddParamParser(kLimitParam, ConstructLimitParser());
  parser.AddParamParser(kParamsParam, ConstructParamsParser());
  parser.AddParamParser(kSortByParam, ConstructSortByParser());
  return parser;
}

absl::StatusOr<std::unique_ptr<expr::Expression::AttributeReference>>
AggregateParameters::MakeReference(const absl::string_view name, bool create) {
  DBG << "MakeReference : " << name << " Create:" << create << "\n";
  auto it = record_indexes_by_identifier_.find(name);
  if (it != record_indexes_by_identifier_.end()) {
    return std::make_unique<Attribute>(name, it->second);
  }
  it = record_indexes_by_alias_.find(name);
  if (it != record_indexes_by_alias_.end()) {
    return std::make_unique<Attribute>(name, it->second);
  }
  indexes::IndexerType fieldType = indexes::IndexerType::kNone;
  if (!create) {
    VMSDK_ASSIGN_OR_RETURN(fieldType,
                           parse_vars_.index_interface_->GetFieldType(name));
    switch (fieldType) {
      case indexes::IndexerType::kTag:
      case indexes::IndexerType::kNumeric:
      case indexes::IndexerType::kVector:
      case indexes::IndexerType::kFlat:
      case indexes::IndexerType::kHNSW:
        break;
      default:
        return absl::InvalidArgumentError(
            absl::StrCat("Invalid data type for @", name));
    }
  }
  auto identifier = parse_vars_.index_interface_->GetIdentifier(name);
  size_t new_index;
  if (identifier.ok()) {
    // DBG << "Adding Record Attribute: " << name << " with alias "
    //     << identifier.value() << "\n";
    new_index = AddRecordAttribute(*identifier, name, fieldType);
  } else {
    // DBG << "Adding Record Attribute: " << name
    //     << " with synthetic alias (no index schema)\n";
    new_index = AddRecordAttribute(name, name, indexes::IndexerType::kNone);
  }
  return std::make_unique<Attribute>(name, new_index);
}

std::ostream &operator<<(std::ostream &os, const AggregateParameters &agg) {
  os << "\nAggregate command Parameters: " << "\n";
  for (const auto &[key, value] : agg.parse_vars.params) {
    DBG << "Parameter " << key << " And Value: " << value.first << ":"
        << value.second << "\n";
  }
  for (const auto &c : agg.stages_) {
    os << *c << "\n";
  }
  return os;
}

}  // namespace aggregate
}  // namespace valkey_search
