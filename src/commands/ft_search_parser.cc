/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
#include "src/commands/filter_parser.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/metrics.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace {

constexpr absl::string_view kParamsParam{"PARAMS"};
constexpr absl::string_view kDialectParam{"DIALECT"};
constexpr absl::string_view kLimitParam{"LIMIT"};
constexpr absl::string_view kNoContentParam{"NOCONTENT"};
constexpr absl::string_view kReturnParam{"RETURN"};
constexpr absl::string_view kTimeoutParam{"TIMEOUT"};
constexpr absl::string_view kAsParam{"AS"};
constexpr absl::string_view kLocalOnly{"LOCALONLY"};
constexpr absl::string_view kVectorFilterDelimiter = "=>";

absl::StatusOr<absl::string_view> SubstituteParam(
    query::VectorSearchParameters &parameters, absl::string_view source) {
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

absl::Status ParseKnnInner(query::VectorSearchParameters &parameters,
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
  VMSDK_ASSIGN_OR_RETURN(auto k_string, SubstituteParam(parameters, params[1]));
  VMSDK_ASSIGN_OR_RETURN(parameters.k, vmsdk::To<unsigned>(k_string));
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
  VMSDK_ASSIGN_OR_RETURN(parameters.query,
                         SubstituteParam(parameters, params[3]));
  size_t i = 4;
  while (i < params.size()) {
    if (absl::EqualsIgnoreCase(params[i], "EF_RUNTIME")) {
      i++;
      if (i == params.size()) {
        return absl::InvalidArgumentError("EF_RUNTIME argument is missing");
      }
      VMSDK_ASSIGN_OR_RETURN(auto ef_string,
                             SubstituteParam(parameters, params[i++]));
      VMSDK_ASSIGN_OR_RETURN(parameters.ef, vmsdk::To<unsigned>(ef_string));
    } else if (absl::EqualsIgnoreCase(params[i], kAsParam)) {
      i++;
      if (i == params.size()) {
        return absl::InvalidArgumentError("AS argument is missing");
      }
      VMSDK_ASSIGN_OR_RETURN(parameters.parse_vars.score_as_string,
                             SubstituteParam(parameters, params[i++]));
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
    const IndexSchema &index_schema, absl::string_view pre_filter) {
  FilterParser parser(index_schema, pre_filter);
  return parser.Parse();
}

absl::Status ParseKNN(query::VectorSearchParameters &parameters,
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

absl::Status Verify(query::VectorSearchParameters &parameters) {
  // Only verify the vector KNN parameters for vector based queries.
  // if (parameters.query.empty()) {
  //   return absl::InvalidArgumentError("missing vector parameter");
  // }
  if (parameters.ef.has_value() && parameters.ef <= 0) {
    return absl::InvalidArgumentError("`EF` value must be positive");
  }
  // if (parameters.k <= 0) {
  //   return absl::InvalidArgumentError("k must be positive");
  // }
  if (parameters.timeout_ms > kMaxTimeoutMs) {
    return absl::InvalidArgumentError(
        absl::StrCat(kTimeoutParam,
                     " must be a positive integer greater than 0 and "
                     "cannot exceed ",
                     kMaxTimeoutMs, "."));
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

std::unique_ptr<vmsdk::ParamParser<query::VectorSearchParameters>>
ConstructLimitParser() {
  return std::make_unique<vmsdk::ParamParser<query::VectorSearchParameters>>(
      [](query::VectorSearchParameters &parameters,
         vmsdk::ArgsIterator &itr) -> absl::Status {
        VMSDK_RETURN_IF_ERROR(
            vmsdk::ParseParamValue(itr, parameters.limit.first_index));
        VMSDK_RETURN_IF_ERROR(
            vmsdk::ParseParamValue(itr, parameters.limit.number));
        return absl::OkStatus();
      });
}

std::unique_ptr<vmsdk::ParamParser<query::VectorSearchParameters>>
ConstructParamsParser() {
  return std::make_unique<vmsdk::ParamParser<query::VectorSearchParameters>>(
      [](query::VectorSearchParameters &parameters,
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

std::unique_ptr<vmsdk::ParamParser<query::VectorSearchParameters>>
ConstructReturnParser() {
  return std::make_unique<vmsdk::ParamParser<query::VectorSearchParameters>>(
      [](query::VectorSearchParameters &parameters,
         vmsdk::ArgsIterator &itr) -> absl::Status {
        uint32_t cnt{0};
        VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, cnt));
        if (cnt == 0) {
          parameters.no_content = true;
          return absl::OkStatus();
        }
        for (uint32_t i = 0; i < cnt; ++i) {
          vmsdk::UniqueRedisString identifier;
          VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, identifier));
          auto as_property = vmsdk::RetainUniqueRedisString(identifier.get());
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
          vmsdk::UniqueRedisString attribute_alias;
          if (schema_identifier.ok()) {
            attribute_alias = vmsdk::RetainUniqueRedisString(identifier.get());
            identifier = vmsdk::MakeUniqueRedisString(*schema_identifier);
          }
          parameters.return_attributes.emplace_back(query::ReturnAttribute{
              std::move(identifier), std::move(attribute_alias),
              std::move(as_property)});
        }
        return absl::OkStatus();
      });
}

vmsdk::KeyValueParser<query::VectorSearchParameters> CreateSearchParser() {
  vmsdk::KeyValueParser<query::VectorSearchParameters> parser;
  parser.AddParamParser(
      kDialectParam,
      GENERATE_VALUE_PARSER(query::VectorSearchParameters, dialect));
  parser.AddParamParser(
      kLocalOnly,
      GENERATE_FLAG_PARSER(query::VectorSearchParameters, local_only));
  parser.AddParamParser(
      kTimeoutParam,
      GENERATE_VALUE_PARSER(query::VectorSearchParameters, timeout_ms));
  parser.AddParamParser(kLimitParam, ConstructLimitParser());
  parser.AddParamParser(
      kNoContentParam,
      GENERATE_FLAG_PARSER(query::VectorSearchParameters, no_content));
  parser.AddParamParser(kReturnParam, ConstructReturnParser());
  parser.AddParamParser(kParamsParam, ConstructParamsParser());
  return parser;
}

static vmsdk::KeyValueParser<query::VectorSearchParameters> SearchParser =
    CreateSearchParser();

absl::Status ParseQueryString(query::VectorSearchParameters &parameters) {
  auto filter_expression =
      absl::string_view(parameters.parse_vars.query_string);
  auto pos = filter_expression.find(kVectorFilterDelimiter);
  absl::string_view pre_filter;
  absl::string_view vector_filter;
  if (pos == absl::string_view::npos) {
    pre_filter = absl::StripAsciiWhitespace(filter_expression);
  } else {
    pre_filter = absl::StripAsciiWhitespace(filter_expression.substr(0, pos));
    vector_filter = absl::StripAsciiWhitespace(
      filter_expression.substr(pos + kVectorFilterDelimiter.size()));
  }
  VMSDK_ASSIGN_OR_RETURN(
      parameters.filter_parse_results,
      ParsePreFilter(*parameters.index_schema, pre_filter),
      _.SetPrepend() << "Invalid filter expression: `" << pre_filter << "`. ");
  if (parameters.filter_parse_results.root_predicate) {
    ++Metrics::GetStats().query_hybrid_requests_cnt;
  }

  if (!vector_filter.empty()) {
    VMSDK_RETURN_IF_ERROR(ParseKNN(parameters, vector_filter)).SetPrepend()
      << "Error parsing vector similarity parameters: `" << vector_filter
      << "`. ";
      // Validate the index exists and is a vector index.
    VMSDK_ASSIGN_OR_RETURN(auto index, parameters.index_schema->GetIndex(
                                          parameters.attribute_alias));
    if (index->GetIndexerType() != indexes::IndexerType::kHNSW &&
        index->GetIndexerType() != indexes::IndexerType::kFlat) {
      return absl::InvalidArgumentError(absl::StrCat("Index field `",
                                                    parameters.attribute_alias,
                                                    "` is not a Vector index "));
    }
    if (parameters.parse_vars.score_as_string.empty()) {
      VMSDK_ASSIGN_OR_RETURN(parameters.score_as,
                            parameters.index_schema->DefaultReplyScoreAs(
                                parameters.attribute_alias));
    } else {
      parameters.score_as =
          vmsdk::MakeUniqueRedisString(parameters.parse_vars.score_as_string);
    }
  }
  return absl::OkStatus();
}
}  // namespace

absl::StatusOr<std::unique_ptr<query::VectorSearchParameters>>
ParseVectorSearchParameters(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc, const SchemaManager &schema_manager) {
  vmsdk::ArgsIterator itr{argv, argc};
  auto parameters = std::make_unique<query::VectorSearchParameters>();
  VMSDK_RETURN_IF_ERROR(
      vmsdk::ParseParamValue(itr, parameters->index_schema_name));
  VMSDK_ASSIGN_OR_RETURN(
      parameters->index_schema,
      SchemaManager::Instance().GetIndexSchema(RedisModule_GetSelectedDb(ctx),
                                               parameters->index_schema_name));
  VMSDK_RETURN_IF_ERROR(
      vmsdk::ParseParamValue(itr, parameters->parse_vars.query_string));
  VMSDK_RETURN_IF_ERROR(SearchParser.Parse(*parameters, itr));
  if (itr.DistanceEnd() > 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected parameter at position ", (itr.Position() + 1),
                     ":", vmsdk::ToStringView(itr.Get().value())));
  }
  VMSDK_RETURN_IF_ERROR(ParseQueryString(*parameters));
  VMSDK_RETURN_IF_ERROR(Verify(*parameters));
  parameters->parse_vars.ClearAtEndOfParse();
  return parameters;
}
}  // namespace valkey_search
