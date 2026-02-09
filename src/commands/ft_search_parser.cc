/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/ft_search_parser.h"

#include <cctype>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "ft_create_parser.h"
#include "ft_search_parser.h"
#include "src/query/search.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/info.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"

// Query operation counters
DEV_INTEGER_COUNTER(query_stats, query_text_term_count);
DEV_INTEGER_COUNTER(query_stats, query_text_prefix_count);
DEV_INTEGER_COUNTER(query_stats, query_text_suffix_count);
DEV_INTEGER_COUNTER(query_stats, query_text_fuzzy_count);
DEV_INTEGER_COUNTER(query_stats, query_text_proximity_count);
DEV_INTEGER_COUNTER(query_stats, query_numeric_count);
DEV_INTEGER_COUNTER(query_stats, query_tag_count);

namespace valkey_search {

namespace {
// Increment query operation metrics based on query operations flags.
// File-internal helper function.
void IncrementQueryOperationMetrics(QueryOperations query_operations) {
  // High-level query type metrics
  if (query_operations & QueryOperations::kContainsText) {
    ++Metrics::GetStats().query_text_requests_cnt;
  }
  if (query_operations & QueryOperations::kContainsNumeric) {
    query_numeric_count.Increment();
  }
  if (query_operations & QueryOperations::kContainsTag) {
    query_tag_count.Increment();
  }
  // Text operation type metrics
  if (query_operations & QueryOperations::kContainsTextTerm) {
    query_text_term_count.Increment();
  }
  if (query_operations & QueryOperations::kContainsTextPrefix) {
    query_text_prefix_count.Increment();
  }
  if (query_operations & QueryOperations::kContainsTextSuffix) {
    query_text_suffix_count.Increment();
  }
  if (query_operations & QueryOperations::kContainsTextFuzzy) {
    query_text_fuzzy_count.Increment();
  }
  if (query_operations & QueryOperations::kContainsProximity) {
    query_text_proximity_count.Increment();
  }
}
}  // namespace

constexpr absl::string_view kMaxKnnConfig{"max-vector-knn"};
constexpr int kDefaultKnnLimit{10000};
constexpr int kMaxKnn{100000};

/// Register the "--max-knn" flag. Controls the max KNN parameter for vector
/// search.
static auto max_knn =
    vmsdk::config::NumberBuilder(kMaxKnnConfig,     // name
                                 kDefaultKnnLimit,  // default size
                                 1,                 // min size
                                 kMaxKnn)           // max size
        .WithValidationCallback(CHECK_RANGE(1, kMaxKnn, kMaxKnnConfig))
        .Build();

namespace options {
vmsdk::config::Number &GetMaxKnn() {
  return dynamic_cast<vmsdk::config::Number &>(*max_knn);
}

}  // namespace options

namespace {

absl::Status Verify(query::SearchParameters &parameters) {
  // Only verify the vector KNN parameters for vector based queries.
  if (!parameters.IsNonVectorQuery()) {
    if (parameters.query.empty()) {
      return absl::InvalidArgumentError("Invalid Query Syntax");
    }
    if (parameters.ef.has_value()) {
      auto max_ef_runtime_value = options::GetMaxEfRuntime().GetValue();
      VMSDK_RETURN_IF_ERROR(
          vmsdk::VerifyRange(parameters.ef.value(), 1, max_ef_runtime_value))
          << "`EF_RUNTIME` must be a positive integer greater than 0 and "
             "cannot "
             "exceed "
          << max_ef_runtime_value << ".";
    }
    auto max_knn_value = options::GetMaxKnn().GetValue();
    VMSDK_RETURN_IF_ERROR(vmsdk::VerifyRange(parameters.k, 1, max_knn_value))
        << "KNN parameter must be a positive integer greater than 0 and cannot "
           "exceed "
        << max_knn_value << ".";
  }
  if (parameters.timeout_ms > query::kMaxTimeoutMs) {
    return absl::InvalidArgumentError(
        absl::StrCat(query::kTimeoutParam,
                     " must be a positive integer greater than 0 and "
                     "cannot exceed ",
                     query::kMaxTimeoutMs, "."));
  }
  if (parameters.dialect < 2 || parameters.dialect > 4) {
    return absl::InvalidArgumentError(
        "DIALECT requires a non negative integer >=2 and <= 4");
  }

  // Validate all parameters used, nuke the map to avoid dangling pointers
  while (!parameters.parse_vars.params.empty()) {
    auto begin = parameters.parse_vars.params.begin();
    if (begin->second.first == 0) {
      return absl::NotFoundError(
          absl::StrCat("Parameter `", begin->first, "` not used."));
    }
    parameters.parse_vars.params.erase(begin);
  }
  return absl::OkStatus();
}

std::unique_ptr<vmsdk::ParamParser<SearchCommand>> ConstructLimitParser() {
  return std::make_unique<vmsdk::ParamParser<SearchCommand>>(
      [](SearchCommand &parameters, vmsdk::ArgsIterator &itr) -> absl::Status {
        VMSDK_RETURN_IF_ERROR(
            vmsdk::ParseParamValue(itr, parameters.limit.first_index));
        VMSDK_RETURN_IF_ERROR(
            vmsdk::ParseParamValue(itr, parameters.limit.number));
        return absl::OkStatus();
      });
}

std::unique_ptr<vmsdk::ParamParser<SearchCommand>> ConstructParamsParser() {
  return std::make_unique<vmsdk::ParamParser<SearchCommand>>(
      [](SearchCommand &parameters, vmsdk::ArgsIterator &itr) -> absl::Status {
        unsigned count{0};
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, count));
        if (count & 1) {
          return absl::InvalidArgumentError(
              "Parameter count must be an even number.");
        }
        while (count > 0) {
          VMSDK_ASSIGN_OR_RETURN(auto key_str, itr.Get());
          itr.Next();
          VMSDK_ASSIGN_OR_RETURN(auto value_str, itr.Get());
          itr.Next();
          absl::string_view key = vmsdk::ToStringView(key_str);
          absl::string_view value = vmsdk::ToStringView(value_str);
          auto [_, inserted] = parameters.parse_vars.params.insert(
              std::make_pair(key, std::make_pair(0, value)));
          if (!inserted) {
            return absl::InvalidArgumentError(
                absl::StrCat("Parameter ", key, " is already defined."));
          }
          count -= 2;
        }
        return absl::OkStatus();
      });
}
std::unique_ptr<vmsdk::ParamParser<SearchCommand>> ConstructSortByParser() {
  return std::make_unique<vmsdk::ParamParser<SearchCommand>>(
      [](SearchCommand &parameters, vmsdk::ArgsIterator &itr) -> absl::Status {
        vmsdk::UniqueValkeyString field;
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, field));
        query::SortByParameter sortbyparams;
        sortbyparams.field = vmsdk::ToStringView(field.get());

        // Check for optional ASC/DESC parameter
        if (itr.DistanceEnd() > 0) {
          auto next_arg = itr.Get();
          if (next_arg.ok()) {
            absl::string_view order_str = vmsdk::ToStringView(next_arg.value());
            if (absl::EqualsIgnoreCase(order_str, "ASC")) {
              sortbyparams.order = query::SortOrder::kAscending;
              itr.Next();
            } else if (absl::EqualsIgnoreCase(order_str, "DESC")) {
              sortbyparams.order = query::SortOrder::kDescending;
              itr.Next();
            }
            // If it's neither ASC nor DESC, leave it for the next parser
          }
        }
        parameters.sortby = std::make_optional(sortbyparams);
        return absl::OkStatus();
      });
}
std::unique_ptr<vmsdk::ParamParser<SearchCommand>> ConstructReturnParser() {
  return std::make_unique<vmsdk::ParamParser<SearchCommand>>(
      [](SearchCommand &parameters, vmsdk::ArgsIterator &itr) -> absl::Status {
        uint32_t cnt{0};
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, cnt));
        if (cnt == 0) {
          parameters.no_content = true;
          return absl::OkStatus();
        }
        for (uint32_t i = 0; i < cnt; ++i) {
          vmsdk::UniqueValkeyString identifier;
          VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, identifier));
          auto as_property = vmsdk::RetainUniqueValkeyString(identifier.get());
          VMSDK_ASSIGN_OR_RETURN(
              auto res,
              vmsdk::ParseParam(query::kAsParam, false, itr, as_property));
          if (res) {
            i += 2;
            if (i > cnt) {
              return absl::InvalidArgumentError("Unexpected parameter `AS` ");
            }
          }
          auto schema_identifier = parameters.index_schema->GetIdentifier(
              vmsdk::ToStringView(identifier.get()));
          vmsdk::UniqueValkeyString attribute_alias;
          if (schema_identifier.ok()) {
            attribute_alias = vmsdk::RetainUniqueValkeyString(identifier.get());
            identifier = vmsdk::MakeUniqueValkeyString(*schema_identifier);
          }
          parameters.return_attributes.emplace_back(query::ReturnAttribute{
              std::move(identifier), std::move(attribute_alias),
              std::move(as_property)});
        }
        return absl::OkStatus();
      });
}

vmsdk::KeyValueParser<SearchCommand> CreateSearchParser() {
  vmsdk::KeyValueParser<SearchCommand> parser;
  parser.AddParamParser(query::kDialectParam,
                        GENERATE_VALUE_PARSER(SearchCommand, dialect));
  parser.AddParamParser(query::kLocalOnly,
                        GENERATE_FLAG_PARSER(SearchCommand, local_only));
  parser.AddParamParser(
      query::kAllShards,
      GENERATE_NEGATIVE_FLAG_PARSER(SearchCommand, enable_partial_results));
  parser.AddParamParser(
      query::kSomeShards,
      GENERATE_FLAG_PARSER(SearchCommand, enable_partial_results));
  parser.AddParamParser(
      query::kConsistent,
      GENERATE_FLAG_PARSER(SearchCommand, enable_consistency));
  parser.AddParamParser(
      query::kInconsistent,
      GENERATE_NEGATIVE_FLAG_PARSER(SearchCommand, enable_consistency));
  parser.AddParamParser(query::kTimeoutParam,
                        GENERATE_VALUE_PARSER(SearchCommand, timeout_ms));
  parser.AddParamParser(query::kLimitParam, ConstructLimitParser());
  parser.AddParamParser(query::kNoContentParam,
                        GENERATE_FLAG_PARSER(SearchCommand, no_content));
  parser.AddParamParser(query::kReturnParam, ConstructReturnParser());
  parser.AddParamParser(query::kSortByParam, ConstructSortByParser());
  parser.AddParamParser(query::kParamsParam, ConstructParamsParser());
  parser.AddParamParser(query::kInorder,
                        GENERATE_FLAG_PARSER(SearchCommand, inorder));
  parser.AddParamParser(query::kVerbatim,
                        GENERATE_FLAG_PARSER(SearchCommand, verbatim));
  parser.AddParamParser(query::kSlop,
                        GENERATE_VALUE_PARSER(SearchCommand, slop));

  return parser;
}

static vmsdk::KeyValueParser<SearchCommand> SearchParser = CreateSearchParser();

}  // namespace

absl::Status SearchCommand::PostParseQueryString() {
  VMSDK_RETURN_IF_ERROR(query::SearchParameters::PostParseQueryString());

  if (sortby.has_value()) {
    // Validate sortby field exists in the index schema
    VMSDK_RETURN_IF_ERROR(index_schema->GetIdentifier(sortby->field).status());
  }

  return absl::OkStatus();
}

absl::Status VerifyQueryString(query::SearchParameters &parameters) {
  // Only verify the vector KNN parameters for vector based queries.
  if (!parameters.IsNonVectorQuery()) {
    if (parameters.query.empty()) {
      return absl::InvalidArgumentError("Invalid Query Syntax");
    }
    if (parameters.ef.has_value()) {
      auto max_ef_runtime_value = options::GetMaxEfRuntime().GetValue();
      VMSDK_RETURN_IF_ERROR(
          vmsdk::VerifyRange(parameters.ef.value(), 1, max_ef_runtime_value))
          << "`EF_RUNTIME` must be a positive integer greater than 0 and "
             "cannot "
             "exceed "
          << max_ef_runtime_value << ".";
    }
    auto max_knn_value = options::GetMaxKnn().GetValue();
    VMSDK_RETURN_IF_ERROR(vmsdk::VerifyRange(parameters.k, 1, max_knn_value))
        << "KNN parameter must be a positive integer greater than 0 and cannot "
           "exceed "
        << max_knn_value << ".";
  }
  if (parameters.timeout_ms > query::kMaxTimeoutMs) {
    return absl::InvalidArgumentError(
        absl::StrCat(query::kTimeoutParam,
                     " must be a positive integer greater than 0 and "
                     "cannot exceed ",
                     query::kMaxTimeoutMs, "."));
  }
  if (parameters.dialect < 2 || parameters.dialect > 4) {
    return absl::InvalidArgumentError(
        "DIALECT requires a non negative integer >=2 and <= 4");
  }

  // Validate all parameters used, nuke the map to avoid dangling pointers
  while (!parameters.parse_vars.params.empty()) {
    auto begin = parameters.parse_vars.params.begin();
    if (begin->second.first == 0) {
      return absl::NotFoundError(
          absl::StrCat("Parameter `", begin->first, "` not used."));
    }
    parameters.parse_vars.params.erase(begin);
  }
  return absl::OkStatus();
}

absl::Status SearchCommand::ParseCommand(vmsdk::ArgsIterator &itr) {
  VMSDK_RETURN_IF_ERROR(SearchParser.Parse(*this, itr));
  if (itr.DistanceEnd() > 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected parameter at position ", (itr.Position() + 1),
                     ":", vmsdk::ToStringView(itr.Get().value())));
  }
  VMSDK_RETURN_IF_ERROR(PreParseQueryString());
  VMSDK_RETURN_IF_ERROR(PostParseQueryString());
  VMSDK_RETURN_IF_ERROR(VerifyQueryString(*this));
  return absl::OkStatus();
}

}  // namespace valkey_search
