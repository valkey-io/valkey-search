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
#include "src/commands/ft_create_parser.h"

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <string>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace {
constexpr absl::string_view kInitialCapParam{"INITIAL_CAP"};
constexpr absl::string_view kBlockSizeParam{"BLOCK_SIZE"};
constexpr absl::string_view kMParam{"M"};
constexpr absl::string_view kEfConstructionParam{"EF_CONSTRUCTION"};
constexpr absl::string_view kEfRuntimeParam{"EF_RUNTIME"};
constexpr absl::string_view kDimensionsParam{"DIM"};
constexpr absl::string_view kDistanceMetricParam{"DISTANCE_METRIC"};
constexpr absl::string_view kDataTypeParam{"TYPE"};
constexpr absl::string_view kPrefixParam{"PREFIX"};
constexpr absl::string_view kFilterParam{"FILTER"};
constexpr absl::string_view kLanguageParam{"LANGUAGE"};
constexpr absl::string_view kLanguageFieldParam{"LANGUAGE_FIELD"};
constexpr absl::string_view kScoreFieldParam{"SCORE_FIELD"};
constexpr absl::string_view kPayloadFieldParam{"PAYLOAD_FIELD"};
const absl::string_view kAsParam{"AS"};
const absl::string_view kOnParam{"ON"};
const absl::string_view kSeparatorParam{"SEPARATOR"};
const absl::string_view kCaseSensitiveParam{"CASESENSITIVE"};
const absl::string_view kScoreParam{"SCORE"};
constexpr absl::string_view kSchemaParam{"SCHEMA"};
constexpr size_t kMaxAttributes{50};
constexpr int kMaxDimensions{32768};
constexpr int kMaxM{2000000};
constexpr int kMaxEfConstruction{4096};
constexpr int kMaxEfRuntime{4096};
const absl::NoDestructor<
    absl::flat_hash_map<absl::string_view, data_model::Language>>
    kLanguageByStr({{"ENGLISH", data_model::LANGUAGE_ENGLISH}});
const absl::NoDestructor<
    absl::flat_hash_map<absl::string_view, data_model::AttributeDataType>>
    kOnDataTypeByStr({{"HASH", data_model::ATTRIBUTE_DATA_TYPE_HASH},
                      {"JSON", data_model::ATTRIBUTE_DATA_TYPE_JSON}});
absl::Status ParsePrefixes(vmsdk::ArgsIterator &itr,
                           data_model::IndexSchema &index_schema_proto) {
  uint32_t prefixes_cnt{0};
  VMSDK_ASSIGN_OR_RETURN(
      auto res, vmsdk::ParseParam(kPrefixParam, false, itr, prefixes_cnt));
  if (!res) {
    return absl::OkStatus();
  }
  if (prefixes_cnt > (uint32_t)itr.DistanceEnd()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Bad arguments for PREFIX: `", prefixes_cnt,
                     "` is outside acceptable bounds"));
  }
  for (uint32_t i = 0; i < prefixes_cnt; ++i) {
    VMSDK_ASSIGN_OR_RETURN(auto itr_arg, itr.Get());
    if (vmsdk::ParseHashTag(vmsdk::ToStringView(itr_arg))) {
      return absl::InvalidArgumentError(
          "PREFIX argument(s) must not contain a hash tag");
    }
    index_schema_proto.add_subscribed_key_prefixes(
        std::string(vmsdk::ToStringView(itr_arg)));
    itr.Next();
  }
  return absl::OkStatus();
}
std::string NotSupportedParamErrorMsg(absl::string_view param) {
  return absl::StrCat("The parameter `", param, "` is not supported");
}
absl::Status ParseLanguage(vmsdk::ArgsIterator &itr,
                           data_model::IndexSchema &index_schema_proto) {
  data_model::Language language{data_model::Language::LANGUAGE_ENGLISH};
  VMSDK_ASSIGN_OR_RETURN(auto res, ParseParam(kLanguageParam, false, itr,
                                              language, *kLanguageByStr));
  VMSDK_ASSIGN_OR_RETURN(
      res, vmsdk::IsParamKeyMatch(kLanguageFieldParam, false, itr));
  if (res) {
    return absl::InvalidArgumentError(
        NotSupportedParamErrorMsg(kLanguageFieldParam));
  }
  return absl::OkStatus();
}
absl::Status ParseScore(vmsdk::ArgsIterator &itr,
                        data_model::IndexSchema &index_schema_proto) {
  float score{1.0};
  VMSDK_ASSIGN_OR_RETURN(auto res,
                         vmsdk::ParseParam(kScoreParam, false, itr, score));
  if (res && score != 1.0) {
    return absl::InvalidArgumentError(
        absl::StrCat("`", kScoreParam, "` parameter with a value `", score,
                     "` is not supported. The only supported value is `1.0`"));
  }
  VMSDK_ASSIGN_OR_RETURN(res,
                         vmsdk::IsParamKeyMatch(kScoreFieldParam, false, itr));
  if (res) {
    return absl::InvalidArgumentError(
        NotSupportedParamErrorMsg(kScoreFieldParam));
  }
  index_schema_proto.set_score(score);
  return absl::OkStatus();
}
vmsdk::KeyValueParser<HNSWParameters> CreateHNSWParser() {
  vmsdk::KeyValueParser<HNSWParameters> parser;
  parser.AddParamParser(kDimensionsParam,
                        GENERATE_VALUE_PARSER(HNSWParameters, dimensions));
  parser.AddParamParser(kDataTypeParam,
                        GENERATE_ENUM_PARSER(HNSWParameters, vector_data_type,
                                             *indexes::kVectorDataTypeByStr));
  parser.AddParamParser(kDistanceMetricParam,
                        GENERATE_ENUM_PARSER(HNSWParameters, distance_metric,
                                             *indexes::kDistanceMetricByStr));
  parser.AddParamParser(kInitialCapParam,
                        GENERATE_VALUE_PARSER(HNSWParameters, initial_cap));
  parser.AddParamParser(kMParam, GENERATE_VALUE_PARSER(HNSWParameters, m));
  parser.AddParamParser(kEfConstructionParam,
                        GENERATE_VALUE_PARSER(HNSWParameters, ef_construction));
  parser.AddParamParser(kEfRuntimeParam,
                        GENERATE_VALUE_PARSER(HNSWParameters, ef_runtime));
  return parser;
}
vmsdk::KeyValueParser<FlatParameters> CreateFlatParamParser() {
  vmsdk::KeyValueParser<FlatParameters> parser;
  parser.AddParamParser(kDimensionsParam,
                        GENERATE_VALUE_PARSER(FlatParameters, dimensions));
  parser.AddParamParser(kDataTypeParam,
                        GENERATE_ENUM_PARSER(FlatParameters, vector_data_type,
                                             *indexes::kVectorDataTypeByStr));
  parser.AddParamParser(kDistanceMetricParam,
                        GENERATE_ENUM_PARSER(FlatParameters, distance_metric,
                                             *indexes::kDistanceMetricByStr));
  parser.AddParamParser(kInitialCapParam,
                        GENERATE_VALUE_PARSER(FlatParameters, initial_cap));
  parser.AddParamParser(kBlockSizeParam,
                        GENERATE_VALUE_PARSER(FlatParameters, block_size));
  return parser;
}
absl::Status ParseVector(vmsdk::ArgsIterator &itr,
                         data_model::Index &index_proto) {
  absl::string_view algo_str;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, algo_str));
  data_model::VectorIndex::AlgorithmCase algo{
      data_model::VectorIndex::ALGORITHM_NOT_SET};
  VMSDK_ASSIGN_OR_RETURN(algo,
                         vmsdk::ToEnum(algo_str, *indexes::kVectorAlgoByStr));
  uint32_t params_num;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, params_num));
  if (params_num > static_cast<uint32_t>(itr.DistanceEnd() + 1)) {
    return absl::InvalidArgumentError(
        absl::StrCat("Expected ", params_num, " parameters for ",
                     absl::AsciiStrToUpper(algo_str), " but got ",
                     itr.DistanceEnd(), " parameters."));
  }
  VMSDK_ASSIGN_OR_RETURN(auto vector_itr, itr.SubIterator(params_num));
  if (algo == data_model::VectorIndex::kHnswAlgorithm) {
    static auto parser = CreateHNSWParser();
    HNSWParameters parameters;
    VMSDK_RETURN_IF_ERROR(parser.Parse(parameters, vector_itr));
    VMSDK_RETURN_IF_ERROR(parameters.Verify());
    index_proto.set_allocated_vector_index(parameters.ToProto().release());
  } else {
    static auto parser = CreateFlatParamParser();
    FlatParameters parameters;
    VMSDK_RETURN_IF_ERROR(parser.Parse(parameters, vector_itr));
    VMSDK_RETURN_IF_ERROR(parameters.Verify());
    index_proto.set_allocated_vector_index(parameters.ToProto().release());
  }
  itr.Next(params_num);
  return absl::OkStatus();
}
absl::Status ParseNumeric(vmsdk::ArgsIterator &itr,
                          data_model::Index &index_proto) {
  auto numeric_index_proto = std::make_unique<data_model::NumericIndex>();
  index_proto.set_allocated_numeric_index(numeric_index_proto.release());
  return absl::OkStatus();
}
vmsdk::KeyValueParser<FTCreateTagParameters> CreateTagParser() {
  vmsdk::KeyValueParser<FTCreateTagParameters> parser;
  parser.AddParamParser(
      kSeparatorParam, GENERATE_VALUE_PARSER(FTCreateTagParameters, separator));
  parser.AddParamParser(
      kCaseSensitiveParam,
      GENERATE_FLAG_PARSER(FTCreateTagParameters, case_sensitive));
  return parser;
}
absl::Status ParseTag(vmsdk::ArgsIterator &itr,
                      data_model::Index &index_proto) {
  auto tag_index_proto = std::make_unique<data_model::TagIndex>();
  static auto parser = CreateTagParser();
  FTCreateTagParameters parameters;
  VMSDK_RETURN_IF_ERROR(parser.Parse(parameters, itr, false));
  if (parameters.separator.length() != 1) {
    if (parameters.separator.length() == 3 &&
        ((parameters.separator[0] == '"' && parameters.separator[2] == '"') ||
         (parameters.separator[0] == '\'' &&
          parameters.separator[2] == '\''))) {
      parameters.separator = parameters.separator.substr(1, 1);
    } else {
      return absl::InvalidArgumentError(
          absl::StrCat("The separator must be a single character, but got `",
                       parameters.separator, "`"));
    }
  }
  tag_index_proto->set_separator(parameters.separator);
  tag_index_proto->set_case_sensitive(parameters.case_sensitive);
  index_proto.set_allocated_tag_index(tag_index_proto.release());
  return absl::OkStatus();
}
absl::StatusOr<indexes::IndexerType> ParseIndexerType(
    vmsdk::ArgsIterator &itr) {
  absl::string_view index_type_str;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, index_type_str));
  VMSDK_ASSIGN_OR_RETURN(auto index_type,
                         vmsdk::ToEnum<indexes::IndexerType>(
                             index_type_str, *indexes::kIndexerTypeByStr));
  return index_type;
}
absl::StatusOr<data_model::Attribute *> ParseAttributeArgs(
    vmsdk::ArgsIterator &itr, absl::string_view attribute_identifier,
    data_model::IndexSchema &index_schema_proto) {
  auto attribute_proto = index_schema_proto.add_attributes();
  attribute_proto->set_identifier(attribute_identifier);
  VMSDK_ASSIGN_OR_RETURN(auto res,
                         vmsdk::ParseParam(kAsParam, false, itr,
                                           *attribute_proto->mutable_alias()));
  if (!res) {
    attribute_proto->set_alias(attribute_proto->identifier());
  }
  VMSDK_ASSIGN_OR_RETURN(auto index_type, ParseIndexerType(itr));
  auto index_proto = std::make_unique<data_model::Index>();
  if (index_type == indexes::IndexerType::kVector) {
    VMSDK_RETURN_IF_ERROR(ParseVector(itr, *index_proto));
  } else if (index_type == indexes::IndexerType::kTag) {
    VMSDK_RETURN_IF_ERROR(ParseTag(itr, *index_proto));
  } else if (index_type == indexes::IndexerType::kNumeric) {
    VMSDK_RETURN_IF_ERROR(ParseNumeric(itr, *index_proto));
  } else {
    CHECK(false);
  }
  attribute_proto->set_allocated_index(index_proto.release());
  return attribute_proto;
}

bool HasVectorIndex(const data_model::IndexSchema &index_schema_proto) {
  for (const auto &attribute : index_schema_proto.attributes()) {
    const auto &index = attribute.index();
    if (index.index_type_case() ==
        data_model::Index::IndexTypeCase::kVectorIndex) {
      return true;
    }
  }
  return false;
}

}  // namespace
absl::StatusOr<data_model::IndexSchema> ParseFTCreateArgs(
    RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  data_model::IndexSchema index_schema_proto;
  vmsdk::ArgsIterator itr{argv, argc};
  VMSDK_RETURN_IF_ERROR(
      vmsdk::ParseParamValue(itr, *index_schema_proto.mutable_name()));
  if (vmsdk::ParseHashTag(index_schema_proto.name())) {
    return absl::InvalidArgumentError("Index name must not contain a hash tag");
  }
  data_model::AttributeDataType on_data_type{
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH};
  VMSDK_ASSIGN_OR_RETURN(auto res, ParseParam(kOnParam, false, itr,
                                              on_data_type, *kOnDataTypeByStr));
  if (on_data_type == data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON &&
      !IsJsonModuleLoaded(ctx)) {
    return absl::InvalidArgumentError("JSON module is not loaded.");
  }
  index_schema_proto.set_attribute_data_type(on_data_type);
  VMSDK_RETURN_IF_ERROR(ParsePrefixes(itr, index_schema_proto));
  VMSDK_ASSIGN_OR_RETURN(res, vmsdk::IsParamKeyMatch(kFilterParam, false, itr));
  if (res) {
    return absl::InvalidArgumentError(NotSupportedParamErrorMsg(kFilterParam));
  }
  VMSDK_RETURN_IF_ERROR(ParseLanguage(itr, index_schema_proto));
  VMSDK_RETURN_IF_ERROR(ParseScore(itr, index_schema_proto));
  VMSDK_ASSIGN_OR_RETURN(
      res, vmsdk::IsParamKeyMatch(kPayloadFieldParam, false, itr));
  if (res) {
    return absl::InvalidArgumentError(
        NotSupportedParamErrorMsg(kPayloadFieldParam));
  }
  absl::string_view schema;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, schema));
  if (!absl::EqualsIgnoreCase(schema, kSchemaParam)) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Unexpected parameter `", schema, "`, expecting `", kSchemaParam, "`"));
  }
  if (!itr.HasNext()) {
    return absl::InvalidArgumentError(
        "Index schema must have at least one attribute");
  }
  std::set<absl::string_view> identifier_names;
  while (itr.HasNext()) {
    absl::string_view attribute_identifier;
    VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, attribute_identifier));
    VMSDK_ASSIGN_OR_RETURN(
        auto attribute,
        ParseAttributeArgs(itr, attribute_identifier, index_schema_proto),
        _.SetPrepend() << "Invalid field type for field `"
                       << attribute_identifier << "`: ");
    if (identifier_names.find(attribute->identifier()) !=
        identifier_names.end()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "Duplicate field in schema - ", attribute->identifier()));
    }
    if (identifier_names.size() >= kMaxAttributes) {
      return absl::InvalidArgumentError(
          absl::StrCat("The maximum number of attributes cannot exceed ",
                       kMaxAttributes, "."));
    }
    identifier_names.insert(attribute->identifier());
  }
  // if (!HasVectorIndex(index_schema_proto)) {
  //   return absl::InvalidArgumentError(
  //       "At least one attribute must be indexed as a vector");
  // }
  return index_schema_proto;
}
std::unique_ptr<data_model::VectorIndex> FTCreateVectorParameters::ToProto()
    const {
  auto vector_index_proto = std::make_unique<data_model::VectorIndex>();
  vector_index_proto->set_dimension_count(dimensions.value());
  vector_index_proto->set_distance_metric(distance_metric);
  vector_index_proto->set_vector_data_type(vector_data_type);
  vector_index_proto->set_initial_cap(initial_cap);
  return vector_index_proto;
}
absl::Status FTCreateVectorParameters::Verify() const {
  if (!dimensions) {
    return absl::InvalidArgumentError("Missing dimensions parameter.");
  }
  if (dimensions.value() <= 0 || dimensions.value() >= kMaxDimensions) {
    return absl::InvalidArgumentError(absl::StrCat(
        "The dimensions value must be a positive integer greater than 0 and "
        "less than or equal to ",
        kMaxDimensions, "."));
  }
  if (initial_cap <= 0) {
    return absl::InvalidArgumentError(
        "INITIAL_CAP must be a positive integer greater than 0.");
  }
  FTCreateVectorParameters default_values;
  if (vector_data_type == default_values.vector_data_type) {
    return absl::InvalidArgumentError("Missing vector TYPE parameter.");
  }
  if (distance_metric == default_values.distance_metric) {
    return absl::InvalidArgumentError("Missing DISTANCE_METRIC parameter.");
  }
  return absl::OkStatus();
}
std::unique_ptr<data_model::VectorIndex> HNSWParameters::ToProto() const {
  auto vector_index_proto = FTCreateVectorParameters::ToProto();
  auto hnsw_algorithm_proto = std::make_unique<data_model::HNSWAlgorithm>();
  hnsw_algorithm_proto->set_m(m);
  hnsw_algorithm_proto->set_ef_construction(ef_construction);
  hnsw_algorithm_proto->set_ef_runtime(ef_runtime);
  vector_index_proto->set_allocated_hnsw_algorithm(
      hnsw_algorithm_proto.release());
  return vector_index_proto;
}
absl::Status HNSWParameters::Verify() const {
  VMSDK_RETURN_IF_ERROR(FTCreateVectorParameters::Verify());
  if (m <= 0 || m > kMaxM) {
    return absl::InvalidArgumentError(absl::StrCat(
        kMParam,
        " must be a positive integer greater than 0 and cannot exceed ", kMaxM,
        "."));
  }
  if (ef_construction <= 0 || ef_construction > kMaxEfConstruction) {
    return absl::InvalidArgumentError(
        absl::StrCat(kEfConstructionParam,
                     " must be a positive integer greater than 0 "
                     "and cannot exceed ",
                     kMaxEfConstruction, "."));
  }
  if (ef_runtime == 0 || ef_runtime > kMaxEfRuntime) {
    return absl::InvalidArgumentError(
        absl::StrCat(kEfRuntimeParam,
                     " must be a positive integer greater than 0 and "
                     "cannot exceed ",
                     kMaxEfRuntime, "."));
  }
  return absl::OkStatus();
}
std::unique_ptr<data_model::VectorIndex> FlatParameters::ToProto() const {
  auto vector_index_proto = FTCreateVectorParameters::ToProto();
  auto flat_algorithm_proto = std::make_unique<data_model::FlatAlgorithm>();
  flat_algorithm_proto->set_block_size(block_size);
  vector_index_proto->set_allocated_flat_algorithm(
      flat_algorithm_proto.release());
  return vector_index_proto;
}
}  // namespace valkey_search
