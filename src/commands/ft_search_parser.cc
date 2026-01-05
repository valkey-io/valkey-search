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

#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "ft_create_parser.h"
#include "src/commands/filter_parser.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/metrics.h"
#include "src/query/search.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search {

constexpr absl::string_view kMaxKnnConfig{"max-vector-knn"};
constexpr int kDefaultKnnLimit{128};
constexpr int kMaxKnn{1000};

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

constexpr absl::string_view kParamsParam{"PARAMS"};
constexpr absl::string_view kDialectParam{"DIALECT"};
constexpr absl::string_view kLimitParam{"LIMIT"};
constexpr absl::string_view kNoContentParam{"NOCONTENT"};
constexpr absl::string_view kReturnParam{"RETURN"};
constexpr absl::string_view kTimeoutParam{"TIMEOUT"};
constexpr absl::string_view kAsParam{"AS"};
constexpr absl::string_view kLocalOnly{"LOCALONLY"};
constexpr absl::string_view kAllShards{"ALLSHARDS"};
constexpr absl::string_view KSomeShards{"SOMESHARDS"};
constexpr absl::string_view kConsistent{"CONSISTENT"};
constexpr absl::string_view kInconsistent{"INCONSISTENT"};
constexpr absl::string_view kVectorFilterDelimiter = "=>";
constexpr absl::string_view kSlop{"SLOP"};
constexpr absl::string_view kInorder{"INORDER"};
constexpr absl::string_view kVerbatim{"VERBATIM"};

absl::StatusOr<absl::string_view> SubstituteParam(
    query::SearchParameters &parameters, absl::string_view source) {
  if (source.empty() || source[0] != '$') {
    return source;
  } else {
    source.remove_prefix(1);
    auto itr = parameters.parse_vars.params.find(source);
    if (itr == parameters.parse_vars.params.end()) {
      return absl::NotFoundError(
          absl::StrCat("Parameter ", source, " not found."));
    } else {
      itr->second.first++;
      return itr->second.second;
    }
  }
}

absl::Status ParseKnnInner(query::SearchParameters &parameters,
                           std::string_view filter) {
  absl::InlinedVector<absl::string_view, 8> params =
      absl::StrSplit(filter, ' ', absl::SkipEmpty());
  if (params.empty()) {
    return absl::InvalidArgumentError("Missing parameters");
  }
  // TODO - need some investment to consolidate this with the common parsing
  // functionality
  if (!absl::EqualsIgnoreCase(params[0], "KNN")) {
    return absl::InvalidArgumentError(
        absl::StrCat("`", params[0], "`. Expecting `KNN`"));
  }
  if (params.size() == 1) {
    return absl::InvalidArgumentError("KNN argument is missing");
  }
  parameters.parse_vars.k_string = params[1];
  if (params.size() == 2) {
    return absl::InvalidArgumentError("Vector field argument is missing");
  }
  if (params[2].data()[0] != '@' || params[2].size() == 1) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected argument `", params[2],
                     "`. Expecting a vector field name, starting with '@'"));
  }
  parameters.attribute_alias =
      absl::string_view(params[2].data() + 1, params[2].size() - 1);
  if (params.size() == 3) {
    return absl::InvalidArgumentError("Blob attribute argument is missing");
  }
  parameters.parse_vars.query_vector_string = params[3];

  size_t i = 4;
  while (i < params.size()) {
    if (absl::EqualsIgnoreCase(params[i], "EF_RUNTIME")) {
      i++;
      if (i == params.size()) {
        return absl::InvalidArgumentError("EF_RUNTIME argument is missing");
      }
      parameters.parse_vars.ef_string = params[i++];
    } else if (absl::EqualsIgnoreCase(params[i], kAsParam)) {
      i++;
      if (i == params.size()) {
        return absl::InvalidArgumentError("AS argument is missing");
      }
      parameters.parse_vars.score_as_string = params[i++];
    } else {
      return absl::InvalidArgumentError(
          absl::StrCat("Unexpected argument `", params[i], "`"));
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<size_t> FindOpenSquareBracket(absl::string_view input) {
  for (size_t position = 0; position < input.size(); ++position) {
    if (input[position] == '[') {
      return position;
    }
    if (!std::isspace(input[position])) {
      return absl::InvalidArgumentError(
          absl::StrCat("Expecting '[' got '", input.substr(position, 1), "'"));
    }
  }
  return absl::InvalidArgumentError("Missing opening bracket");
}

absl::StatusOr<size_t> FindCloseSquareBracket(absl::string_view input) {
  for (auto position = input.size(); position > 0; --position) {
    if (input[position - 1] == ']') {
      return position - 1;
    }
    if (!std::isspace(input[position - 1])) {
      return absl::InvalidArgumentError(absl::StrCat(
          "Expecting ']' got '", input.substr(position - 1, 1), "'"));
    }
  }
  if (input[0] == ']') {
    return 0;
  }
  return absl::InvalidArgumentError("Missing closing bracket");
}

absl::StatusOr<FilterParseResults> ParsePreFilter(
    const IndexSchema &index_schema, absl::string_view pre_filter,
    const query::SearchParameters &search_params) {
  TextParsingOptions options{.verbatim = search_params.verbatim,
                             .inorder = search_params.inorder,
                             .slop = search_params.slop};
  FilterParser parser(index_schema, pre_filter, options);
  return parser.Parse();
}

absl::Status ParseKNN(query::SearchParameters &parameters,
                      absl::string_view filter_str) {
  if (filter_str.empty()) {
    return absl::InvalidArgumentError("Vector query clause is missing");
  }
  VMSDK_ASSIGN_OR_RETURN(auto close_position,
                         FindCloseSquareBracket(filter_str));
  size_t position = 0;
  VMSDK_ASSIGN_OR_RETURN(
      auto open_position,
      FindOpenSquareBracket(absl::string_view(filter_str.data() + position,
                                              close_position - position)));
  position += open_position;
  return ParseKnnInner(parameters,
                       absl::string_view(filter_str.data() + position + 1,
                                         close_position - position - 1));
}

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
        absl::StrCat(kTimeoutParam,
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

std::unique_ptr<vmsdk::ParamParser<query::SearchParameters>>
ConstructLimitParser() {
  return std::make_unique<vmsdk::ParamParser<query::SearchParameters>>(
      [](query::SearchParameters &parameters,
         vmsdk::ArgsIterator &itr) -> absl::Status {
        VMSDK_RETURN_IF_ERROR(
            vmsdk::ParseParamValue(itr, parameters.limit.first_index));
        VMSDK_RETURN_IF_ERROR(
            vmsdk::ParseParamValue(itr, parameters.limit.number));
        return absl::OkStatus();
      });
}

std::unique_ptr<vmsdk::ParamParser<query::SearchParameters>>
ConstructParamsParser() {
  return std::make_unique<vmsdk::ParamParser<query::SearchParameters>>(
      [](query::SearchParameters &parameters,
         vmsdk::ArgsIterator &itr) -> absl::Status {
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

std::unique_ptr<vmsdk::ParamParser<query::SearchParameters>>
ConstructReturnParser() {
  return std::make_unique<vmsdk::ParamParser<query::SearchParameters>>(
      [](query::SearchParameters &parameters,
         vmsdk::ArgsIterator &itr) -> absl::Status {
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
              auto res, vmsdk::ParseParam(kAsParam, false, itr, as_property));
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

vmsdk::KeyValueParser<query::SearchParameters> CreateSearchParser() {
  vmsdk::KeyValueParser<query::SearchParameters> parser;
  parser.AddParamParser(
      kDialectParam, GENERATE_VALUE_PARSER(query::SearchParameters, dialect));
  parser.AddParamParser(
      kLocalOnly, GENERATE_FLAG_PARSER(query::SearchParameters, local_only));
  parser.AddParamParser(kAllShards,
                        GENERATE_NEGATIVE_FLAG_PARSER(query::SearchParameters,
                                                      enable_partial_results));
  parser.AddParamParser(
      KSomeShards,
      GENERATE_FLAG_PARSER(query::SearchParameters, enable_partial_results));
  parser.AddParamParser(
      kConsistent,
      GENERATE_FLAG_PARSER(query::SearchParameters, enable_consistency));
  parser.AddParamParser(kInconsistent,
                        GENERATE_NEGATIVE_FLAG_PARSER(query::SearchParameters,
                                                      enable_consistency));
  parser.AddParamParser(
      kTimeoutParam,
      GENERATE_VALUE_PARSER(query::SearchParameters, timeout_ms));
  parser.AddParamParser(kLimitParam, ConstructLimitParser());
  parser.AddParamParser(
      kNoContentParam,
      GENERATE_FLAG_PARSER(query::SearchParameters, no_content));
  parser.AddParamParser(kReturnParam, ConstructReturnParser());
  parser.AddParamParser(kParamsParam, ConstructParamsParser());
  parser.AddParamParser(kInorder,
                        GENERATE_FLAG_PARSER(query::SearchParameters, inorder));
  parser.AddParamParser(
      kVerbatim, GENERATE_FLAG_PARSER(query::SearchParameters, verbatim));
  parser.AddParamParser(kSlop,
                        GENERATE_VALUE_PARSER(query::SearchParameters, slop));
  return parser;
}

static vmsdk::KeyValueParser<query::SearchParameters> SearchParser =
    CreateSearchParser();

}  // namespace

//
// We don't have values for the $ substitution yet. so we break the parsing into
// two pieces
//
absl::Status PreParseQueryString(query::SearchParameters &parameters) {
  // Validate the query string's length.
  if (parameters.parse_vars.query_string.length() >
      options::GetQueryStringBytes()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Query string is too long, max length is ",
                     options::GetQueryStringBytes(), " bytes."));
  }
  auto filter_expression =
      absl::string_view(parameters.parse_vars.query_string);
  VMSDK_LOG(DEBUG, nullptr)
      << "Query: '" << parameters.parse_vars.query_string << "'";
  auto pos = filter_expression.find(kVectorFilterDelimiter);
  absl::string_view pre_filter;
  absl::string_view vector_filter;
  // If the delimiter is not found (ie - non vector query), treat the whole
  // string as pre-filter.
  if (pos == absl::string_view::npos) {
    pre_filter = absl::StripAsciiWhitespace(filter_expression);
  } else {
    pre_filter = absl::StripAsciiWhitespace(filter_expression.substr(0, pos));
    vector_filter = absl::StripAsciiWhitespace(
        filter_expression.substr(pos + kVectorFilterDelimiter.size()));
  }
  // If INORDER OR SLOP, but the index schema does not support offsets, we
  // reject the query.
  if ((parameters.inorder || parameters.slop.has_value()) &&
      !parameters.index_schema->HasTextOffsets()) {
    return absl::InvalidArgumentError("Index does not support offsets");
  }
  VMSDK_ASSIGN_OR_RETURN(
      parameters.filter_parse_results,
      ParsePreFilter(*parameters.index_schema, pre_filter, parameters),
      _.SetPrepend() << "Invalid filter expression: `" << pre_filter << "`. ");
  if (!parameters.filter_parse_results.root_predicate &&
      vector_filter.empty()) {
    // Return an error if no valid pre-filter and no vector filter is provided.
    return absl::InvalidArgumentError("Vector query clause is missing");
  }
  // Optionally parse the vector filter if it exists.
  if (!vector_filter.empty()) {
    if (parameters.filter_parse_results.root_predicate) {
      ++Metrics::GetStats().query_hybrid_requests_cnt;
    }
    VMSDK_RETURN_IF_ERROR(ParseKNN(parameters, vector_filter)).SetPrepend()
        << "Error parsing vector similarity parameters: `" << vector_filter
        << "`. ";
    // Validate the index exists and is a vector index.
    VMSDK_ASSIGN_OR_RETURN(auto index, parameters.index_schema->GetIndex(
                                           parameters.attribute_alias));
    if (index->GetIndexerType() != indexes::IndexerType::kHNSW &&
        index->GetIndexerType() != indexes::IndexerType::kFlat) {
      return absl::InvalidArgumentError(
          absl::StrCat("Index field `", parameters.attribute_alias,
                       "` is not a Vector index "));
    }
    if (parameters.parse_vars.score_as_string.empty()) {
      VMSDK_ASSIGN_OR_RETURN(parameters.score_as,
                             parameters.index_schema->DefaultReplyScoreAs(
                                 parameters.attribute_alias));
    } else {
      parameters.score_as =
          vmsdk::MakeUniqueValkeyString(parameters.parse_vars.score_as_string);
    }
  }
  // TODO: Return Temp Error for unsupported predicates.
  return absl::OkStatus();
}

absl::Status PostParseVectorParameters(query::SearchParameters &parameters) {
  VMSDK_ASSIGN_OR_RETURN(
      auto k_string,
      SubstituteParam(parameters, parameters.parse_vars.k_string));
  VMSDK_ASSIGN_OR_RETURN(parameters.k, vmsdk::To<unsigned>(k_string));

  VMSDK_ASSIGN_OR_RETURN(
      parameters.query,
      SubstituteParam(parameters, parameters.parse_vars.query_vector_string));

  if (!parameters.parse_vars.ef_string.empty()) {
    VMSDK_ASSIGN_OR_RETURN(
        auto ef_string,
        SubstituteParam(parameters, parameters.parse_vars.ef_string));
    VMSDK_ASSIGN_OR_RETURN(parameters.ef, vmsdk::To<unsigned>(ef_string));
  }

  if (!parameters.parse_vars.score_as_string.empty()) {
    VMSDK_ASSIGN_OR_RETURN(
        parameters.parse_vars.score_as_string,
        SubstituteParam(parameters, parameters.parse_vars.score_as_string));
  }
  return absl::OkStatus();
}

absl::Status PostParseQueryString(query::SearchParameters &parameters) {
  if (parameters.IsVectorQuery()) {
    VMSDK_RETURN_IF_ERROR(PostParseVectorParameters(parameters)).SetPrepend()
        << "Error parsing vector similarity parameters: ";
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
  VMSDK_RETURN_IF_ERROR(PreParseQueryString(*this));
  VMSDK_RETURN_IF_ERROR(PostParseQueryString(*this));
  VMSDK_RETURN_IF_ERROR(Verify(*this));
  return absl::OkStatus();
}

}  // namespace valkey_search
