/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/commands/ft_aggregate_parser.h"

#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"

// #define DBG std::cerr
#define DBG 0 && std::cerr

namespace valkey_search {
namespace aggregate {

constexpr absl::string_view kAddScoresParam{"ADDSCORES"};
constexpr absl::string_view kApplyParam{"APPLY"};
constexpr absl::string_view kAsParam{"AS"};
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

std::unique_ptr<vmsdk::ParamParser<AggregateParameters>> ConstructLoadParser() {
  return std::make_unique<vmsdk::ParamParser<AggregateParameters>>(
      [](AggregateParameters &parameters,
         vmsdk::ArgsIterator &itr) -> absl::Status {
        VMSDK_ASSIGN_OR_RETURN(auto count_string, itr.Get());
        itr.Next();
        if (vmsdk::ToStringView(count_string) == "*") {
          parameters.loadall_ = true;
        } else {
          uint32_t cnt{0};
          VMSDK_ASSIGN_OR_RETURN(cnt, vmsdk::To<uint32_t>(count_string));
          for (uint32_t i = 0; i < cnt; ++i) {
            std::string load;
            VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, load));
            if (load.empty() || load == "@") {
              return absl::InvalidArgumentError(
                  "Empty argument in LOAD clause not allowed");
            }
            if (load[0] == '@') {
              parameters.loads_.emplace_back(load.substr(1));
            } else {
              parameters.loads_.emplace_back(std::move(load));
            }
          }
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
        parameters.has_limit_stage = true;
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
          GroupBy::Reducer r;
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
          r.info_ = &reducer_itr->second;
          uint32_t cnt{0};
          VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, cnt));
          if (cnt < r.info_->min_nargs_ || cnt > r.info_->max_nargs_) {
            return absl::OutOfRangeError(
                absl::StrCat("incorrect number of arguments (", cnt,
                             ") to reducer ", uc_name.AsStringView()));
          }
          for (int i = 0; i < cnt; ++i) {
            VMSDK_ASSIGN_OR_RETURN(auto arg, itr.PopNext(),
                                   _ << "Missing Reducer argument " << i);
            VMSDK_ASSIGN_OR_RETURN(
                auto expr,
                expr::Expression::Compile(parameters, vmsdk::ToStringView(arg)),
                _ << " in GROUPBY stage");
            r.args_.emplace_back(std::move(expr));
          }
          if (itr.PopIfNextIgnoreCase(kAsParam)) {
            VMSDK_ASSIGN_OR_RETURN(auto alias, itr.PopNext(),
                                   _ << "Missing Reducer alias");
            VMSDK_ASSIGN_OR_RETURN(
                auto output,
                parameters.MakeReference(vmsdk::ToStringView(alias), true));
            r.output_ = std::unique_ptr<Attribute>(
                dynamic_cast<Attribute *>(output.release()));
          } else {
            std::ostringstream os;
            os << r;
            VMSDK_ASSIGN_OR_RETURN(auto output,
                                   parameters.MakeReference(os.str(), true));
            r.output_ = std::unique_ptr<Attribute>(
                dynamic_cast<Attribute *>(output.release()));
          }
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
