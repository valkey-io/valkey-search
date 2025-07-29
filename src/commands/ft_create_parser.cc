/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#include "src/commands/ft_create_parser.h"

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>  // For std::once_flag
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
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace {

// Default stop words set
const std::vector<std::string> kDefaultStopWords{
    "a", "is", "the", "an", "and", "are", "as", "at", "be", "but", "by", "for",
    "if", "in", "into", "it", "no", "not", "of", "on", "or", "such", "that", "their",
    "then", "there", "these", "they", "this", "to", "was", "will", "with"
};

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
constexpr size_t kDefaultAttributesCountLimit{50};
constexpr int kDefaultDimensionsCountLimit{32768};
constexpr int kDefaultPrefixesCountLimit{8};
constexpr int kDefaultTagFieldLenLimit{256};
constexpr int kDefaultNumericFieldLenLimit{128};
constexpr size_t kMaxAttributesCount{100};
constexpr int kMaxDimensionsCount{64000};
constexpr int kMaxM{2000000};
constexpr int kMaxEfConstruction{4096};
constexpr int kMaxEfRuntime{4096};
constexpr int kMaxPrefixesCount{16};
constexpr int kMaxTagFieldLen{10000};
constexpr int kMaxNumericFieldLen{256};
constexpr int kTimeoutMs{50000};
constexpr int kMinTimeoutMs{1};
constexpr int kMaxTimeoutMs{60000};

constexpr absl::string_view kMaxPrefixesConfig{"max-prefixes"};
constexpr absl::string_view kMaxTagFieldLenConfig{"max-tag-field-length"};
constexpr absl::string_view kMaxNumericFieldLenConfig{
    "max-numeric-field-length"};
constexpr absl::string_view kMaxAttributesConfig{"max-vector-attributes"};
constexpr absl::string_view kMaxDimensionsConfig{"max-vector-dimensions"};
constexpr absl::string_view kMaxMConfig{"max-vector-m"};
constexpr absl::string_view kMaxEfConstructionConfig{
    "max-vector-ef-construction"};
constexpr absl::string_view kMaxEfRuntimeConfig{"max-vector-ef-runtime"};
constexpr absl::string_view kDefaultTimeoutMs{"default-timeout-ms"};

// FullText variables
constexpr absl::string_view kTextParam{"TEXT"};
constexpr absl::string_view kPunctuationParam{"PUNCTUATION"};
constexpr absl::string_view kWithOffsetsParam{"WITHOFFSETS"};
constexpr absl::string_view kNoOffsetsParam{"NOOFFSETS"};
constexpr absl::string_view kWithSuffixTrieParam{"WITHSUFFIXTRIE"};
constexpr absl::string_view kNoSuffixTrieParam{"NOSUFFIXTRIE"};
constexpr absl::string_view kNoStopWordsParam{"NOSTOPWORDS"};
constexpr absl::string_view kStopWordsParam{"STOPWORDS"};
constexpr absl::string_view kNoStemParam{"NOSTEM"};
constexpr absl::string_view kMinStemSizeParam{"MINSTEMSIZE"};

/// Register the "--max-prefixes" flag. Controls the max number of prefixes per
/// index.
static auto max_prefixes =
    vmsdk::config::NumberBuilder(kMaxPrefixesConfig,          // name
                                 kDefaultPrefixesCountLimit,  // default size
                                 1,                           // min size
                                 kMaxPrefixesCount)           // max size
        .WithValidationCallback(
            CHECK_RANGE(1, kMaxPrefixesCount, kMaxPrefixesConfig))
        .Build();

/// Register the "--max-tag-field-length" flag. Controls the max length of a tag
/// field.
static auto max_tag_field_len =
    vmsdk::config::NumberBuilder(kMaxTagFieldLenConfig,     // name
                                 kDefaultTagFieldLenLimit,  // default size
                                 1,                         // min size
                                 kMaxTagFieldLen)           // max size
        .WithValidationCallback(
            CHECK_RANGE(1, kMaxTagFieldLen, kMaxTagFieldLenConfig))
        .Build();

/// Register the "--max-numeric-field-length" flag. Controls the max length of a
/// numeric field.
static auto max_numeric_field_len =
    vmsdk::config::NumberBuilder(kMaxNumericFieldLenConfig,     // name
                                 kDefaultNumericFieldLenLimit,  // default size
                                 1,                             // min size
                                 kMaxNumericFieldLen)           // max size
        .WithValidationCallback(
            CHECK_RANGE(1, kMaxNumericFieldLen, kMaxNumericFieldLenConfig))
        .Build();

/// Register the "--max-attributes" flag. Controls the max number of attributes
/// per index.
static auto max_attributes =
    vmsdk::config::NumberBuilder(kMaxAttributesConfig,          // name
                                 kDefaultAttributesCountLimit,  // default size
                                 1,                             // min size
                                 kMaxAttributesCount)           // max size
        .WithValidationCallback(
            CHECK_RANGE(1, kMaxAttributesCount, kMaxAttributesConfig))
        .Build();

/// Register the "--max-dimensions" flag. Controls the max dimensions for vector
/// indices.
static auto max_dimensions =
    vmsdk::config::NumberBuilder(kMaxDimensionsConfig,          // name
                                 kDefaultDimensionsCountLimit,  // default size
                                 1,                             // min size
                                 kMaxDimensionsCount)           // max size
        .WithValidationCallback(
            CHECK_RANGE(1, kMaxDimensionsCount, kMaxDimensionsConfig))
        .Build();

/// Register the "--max-m" flag. Controls the max M parameter for HNSW
/// algorithm.
static auto max_m =
    vmsdk::config::NumberBuilder(kMaxMConfig,  // name
                                 kMaxM,        // default size
                                 1,            // min size
                                 kMaxM)        // max size
        .WithValidationCallback(CHECK_RANGE(1, kMaxM, kMaxMConfig))
        .Build();

/// Register the "--max-ef-construction" flag. Controls the max EF construction
/// parameter for HNSW algorithm.
static auto max_ef_construction =
    vmsdk::config::NumberBuilder(kMaxEfConstructionConfig,  // name
                                 kMaxEfConstruction,        // default size
                                 1,                         // min size
                                 kMaxEfConstruction)        // max size
        .WithValidationCallback(
            CHECK_RANGE(1, kMaxEfConstruction, kMaxEfConstructionConfig))
        .Build();

/// Register the "--max-ef-runtime" flag. Controls the max EF runtime parameter
/// for HNSW algorithm.
static auto max_ef_runtime =
    vmsdk::config::NumberBuilder(kMaxEfRuntimeConfig,  // name
                                 kMaxEfRuntime,        // default size
                                 1,                    // min size
                                 kMaxEfRuntime)        // max size
        .WithValidationCallback(
            CHECK_RANGE(1, kMaxEfRuntime, kMaxEfRuntimeConfig))
        .Build();

/// Register the "--default-timeout-ms" flag. Controls the default timeout
/// in milliseconds for FT.SEARCH.
static auto default_timeout_ms =
    vmsdk::config::NumberBuilder(kDefaultTimeoutMs,  // name
                                 kTimeoutMs,            // default timeout
                                 kMinTimeoutMs,         // min timeout
                                 kMaxTimeoutMs)         // max timeout
        .Build();

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
  // Check if the number of prefixes exceeds the configured maximum
  const auto max_prefixes = options::GetMaxPrefixes().GetValue();
  VMSDK_RETURN_IF_ERROR(
      vmsdk::VerifyRange(prefixes_cnt, std::nullopt, max_prefixes))
      << "Number of prefixes (" << prefixes_cnt
      << ") exceeds the maximum allowed (" << max_prefixes << ")";
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
                          data_model::Index &index_proto,
                          absl::string_view attribute_identifier) {
  const auto max_numeric_identifier_len =
      options::GetMaxNumericFieldLen().GetValue();
  VMSDK_RETURN_IF_ERROR(vmsdk::VerifyRange(
      attribute_identifier.length(), std::nullopt, max_numeric_identifier_len))
      << "A numeric field can have a maximum length of "
      << max_numeric_identifier_len << ".";
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
absl::Status ParseTag(vmsdk::ArgsIterator &itr, data_model::Index &index_proto,
                      absl::string_view attribute_identifier) {
  const auto max_tag_identifier_len = options::GetMaxTagFieldLen().GetValue();
  VMSDK_RETURN_IF_ERROR(vmsdk::VerifyRange(
      attribute_identifier.length(), std::nullopt, max_tag_identifier_len))
      << "A tag field can have a maximum length of " << max_tag_identifier_len
      << ".";
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


vmsdk::KeyValueParser<FTCreateTextParameters> CreateTextFieldParser() {
  vmsdk::KeyValueParser<FTCreateTextParameters> parser;
  // Field-level parameters only: WITHSUFFIXTRIE, NOSUFFIXTRIE, NOSTEM, MINSTEMSIZE
  parser.AddParamParser(
      kWithSuffixTrieParam, GENERATE_FLAG_PARSER(FTCreateTextParameters, with_suffix_trie));
  parser.AddParamParser(
      kNoSuffixTrieParam, 
      GENERATE_NEGATIVE_FLAG_PARSER(FTCreateTextParameters, with_suffix_trie));
  parser.AddParamParser(
      kNoStemParam, GENERATE_FLAG_PARSER(FTCreateTextParameters, no_stem));
  parser.AddParamParser(
      kMinStemSizeParam, 
      std::make_unique<vmsdk::ParamParser<FTCreateTextParameters>>(
          [](FTCreateTextParameters &params, vmsdk::ArgsIterator &itr) -> absl::Status {
            int value;
            VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, value));
            if (value <= 0) {
              return absl::InvalidArgumentError("MINSTEMSIZE must be positive");
            }
            params.min_stem_size = value;
            return absl::OkStatus();
          }));
  return parser;
}

absl::Status ParseStopWords(vmsdk::ArgsIterator &itr, GlobalTextParameters &params) {
  uint32_t count;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, count));
  if (count == 0) {
    params.stop_words.clear();
    return absl::OkStatus();
  }
  
  // Check if we have enough arguments remaining
  if (static_cast<uint32_t>(itr.DistanceEnd()) < count) {
    return absl::OutOfRangeError("Missing argument for STOPWORDS. The count does not match the number of arguments provided for STOPWORDS");
  }
  
  params.stop_words.clear();
  for (uint32_t i = 0; i < count; ++i) {
    // Check if the next argument is a reserved keyword that should not be consumed
    // VMSDK_ASSIGN_OR_RETURN(auto next_arg, itr.Get());
    // absl::string_view next_word = vmsdk::ToStringView(next_arg);
    // if (absl::EqualsIgnoreCase(next_word, kSchemaParam)) {
    //   return absl::OutOfRangeError("Missing argument for STOPWORDS. The count does not match the number of arguments provided for STOPWORDS");
    // }
    
    std::string word;
    VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, word));
    params.stop_words.push_back(word);
  }
  return absl::OkStatus();
}

absl::Status ParseText(vmsdk::ArgsIterator &itr, data_model::Index &index_proto,
                       const GlobalTextParameters &global_defaults) {
  // Start with field-specific defaults, then parse field-level parameters
  FTCreateTextParameters field_params;
  field_params.with_suffix_trie = false;
  field_params.no_stem = global_defaults.no_stem;  // Can be overridden
  field_params.min_stem_size = global_defaults.min_stem_size;  // Can be overridden
  
  // Parse field-level parameters (WITHSUFFIXTRIE, NOSUFFIXTRIE, NOSTEM, MINSTEMSIZE)
  static auto field_parser = CreateTextFieldParser();
  VMSDK_RETURN_IF_ERROR(field_parser.Parse(field_params, itr, false));
  
  // Create and populate the TextIndex object (field-specific parameters only)
  auto text_index_proto = std::make_unique<data_model::TextIndex>();
  text_index_proto->set_with_suffix_trie(field_params.with_suffix_trie);
  text_index_proto->set_no_stem(field_params.no_stem);
  text_index_proto->set_min_stem_size(field_params.min_stem_size);
  
  // Set the text_index in the index_proto
  index_proto.set_allocated_text_index(text_index_proto.release());
  
  return absl::OkStatus();
}

vmsdk::KeyValueParser<GlobalTextParameters> CreateGlobalTextParser() {
  vmsdk::KeyValueParser<GlobalTextParameters> parser;
  parser.AddParamParser(
      kPunctuationParam, GENERATE_VALUE_PARSER(GlobalTextParameters, punctuation));
  parser.AddParamParser(
      kWithOffsetsParam, GENERATE_FLAG_PARSER(GlobalTextParameters, with_offsets));
  parser.AddParamParser(
      kNoOffsetsParam, 
      GENERATE_NEGATIVE_FLAG_PARSER(GlobalTextParameters, with_offsets));
  parser.AddParamParser(
      kNoStemParam, GENERATE_FLAG_PARSER(GlobalTextParameters, no_stem));
  parser.AddParamParser(
      kNoStopWordsParam, 
      GENERATE_CLEAR_CONTAINER_PARSER(GlobalTextParameters, stop_words));
  return parser;
}

absl::Status ParseGlobalTextDefaults(vmsdk::ArgsIterator &itr, GlobalTextParameters &defaults) {
  static auto parser = CreateGlobalTextParser();
  VMSDK_RETURN_IF_ERROR(parser.Parse(defaults, itr, false));
  
  VMSDK_ASSIGN_OR_RETURN(auto res, vmsdk::IsParamKeyMatch(kStopWordsParam, false, itr));
  if (res) {
    VMSDK_RETURN_IF_ERROR(ParseStopWords(itr, defaults));
  }
  
  // Handle LANGUAGE parameter for global text defaults
  VMSDK_ASSIGN_OR_RETURN(res, vmsdk::ParseParam(kLanguageParam, false, itr,
                                               defaults.language, *kLanguageByStr));
  
  // Validate punctuation is not empty after parsing
  if (defaults.punctuation.length() >= 2 && 
      ((defaults.punctuation.front() == '"' && defaults.punctuation.back() == '"') ||
       (defaults.punctuation.front() == '\'' && defaults.punctuation.back() == '\''))) {
    defaults.punctuation = defaults.punctuation.substr(1, defaults.punctuation.length() - 2);
  }
  
  // Check if punctuation is empty after quote removal
  if (defaults.punctuation.empty()) {
    return absl::InvalidArgumentError("PUNCTUATION string cannot be empty");
  }
  
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
    data_model::IndexSchema &index_schema_proto,
    const GlobalTextParameters &global_text_defaults) {
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
    VMSDK_RETURN_IF_ERROR(ParseTag(itr, *index_proto, attribute_identifier));
  } else if (index_type == indexes::IndexerType::kNumeric) {
    VMSDK_RETURN_IF_ERROR(
        ParseNumeric(itr, *index_proto, attribute_identifier));
  } else if (index_type == indexes::IndexerType::kText) {
    VMSDK_RETURN_IF_ERROR(ParseText(itr, *index_proto, global_text_defaults));
  } else {
    CHECK(false);
  }
  attribute_proto->set_allocated_index(index_proto.release());
  return attribute_proto;
}

// bool HasVectorIndex(const data_model::IndexSchema &index_schema_proto) {
//   for (const auto &attribute : index_schema_proto.attributes()) {
//     const auto &index = attribute.index();
//     if (index.index_type_case() ==
//         data_model::Index::IndexTypeCase::kVectorIndex) {
//       return true;
//     }
//   }
//   return false;
// }

// bool HasTextIndex(const data_model::IndexSchema &index_schema_proto) {
//   for (const auto &attribute : index_schema_proto.attributes()) {
//     const auto &index = attribute.index();
//     if (index.index_type_case() ==
//         data_model::Index::IndexTypeCase::kTextIndex) {
//       return true;
//     }
//   }
//   return false;
// }

}  // namespace
absl::StatusOr<data_model::IndexSchema> ParseFTCreateArgs(
    ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  // Get configuration values
  const auto max_attributes_value = options::GetMaxAttributes().GetValue();

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
  
  // Parse global text parameters before SCHEMA
  GlobalTextParameters global_text_defaults;
  // Initialize with defaults for each parse call
  global_text_defaults.punctuation = ",.<>{}[]\"':;!@#$%^&*()-+=~/\\|";
  global_text_defaults.min_stem_size = 4;
  global_text_defaults.with_offsets = true;
  global_text_defaults.no_stem = false;
  global_text_defaults.language = data_model::LANGUAGE_ENGLISH;
  global_text_defaults.stop_words = kDefaultStopWords;
  
  VMSDK_RETURN_IF_ERROR(ParseGlobalTextDefaults(itr, global_text_defaults));
  
  // Set global text parameters in IndexSchema
  index_schema_proto.set_language(global_text_defaults.language);
  index_schema_proto.set_punctuation(global_text_defaults.punctuation);
  index_schema_proto.set_with_offsets(global_text_defaults.with_offsets);
  index_schema_proto.set_nostem(global_text_defaults.no_stem);
  index_schema_proto.set_min_stem_size(global_text_defaults.min_stem_size);
  
  // Add stop words to the schema
  for (const auto& word : global_text_defaults.stop_words) {
    index_schema_proto.add_stop_words(word);
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
        ParseAttributeArgs(itr, attribute_identifier, index_schema_proto, global_text_defaults),
        _.SetPrepend() << "Invalid field type for field `"
                       << attribute_identifier << "`: ");
    if (identifier_names.find(attribute->identifier()) !=
        identifier_names.end()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "Duplicate field in schema - ", attribute->identifier()));
    }
    VMSDK_RETURN_IF_ERROR(vmsdk::VerifyRange(
        identifier_names.size() + 1, std::nullopt, max_attributes_value))
        << "The maximum number of attributes cannot exceed "
        << max_attributes_value << ".";

    identifier_names.insert(attribute->identifier());
  }
  
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
  const auto max_dimensions_value = options::GetMaxDimensions().GetValue();
  VMSDK_RETURN_IF_ERROR(
      vmsdk::VerifyRange(dimensions.value(), 1, max_dimensions_value))
      << "The dimensions value must be a positive integer greater than 0 and "
         "less than or equal to "
      << max_dimensions_value << ".";

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
  const auto max_m_value = options::GetMaxM().GetValue();
  VMSDK_RETURN_IF_ERROR(vmsdk::VerifyRange(m, 1, max_m_value))
      << kMParam
      << " must be a positive integer greater than 0 and cannot exceed "
      << max_m_value << ".";

  const auto max_ef_construction_value =
      options::GetMaxEfConstruction().GetValue();
  VMSDK_RETURN_IF_ERROR(
      vmsdk::VerifyRange(ef_construction, 1, max_ef_construction_value))
      << kEfConstructionParam
      << " must be a positive integer greater than 0 and cannot exceed "
      << max_ef_construction_value << ".";
  const auto max_ef_runtime_value = options::GetMaxEfRuntime().GetValue();
  VMSDK_RETURN_IF_ERROR(vmsdk::VerifyRange(ef_runtime, 1, max_ef_runtime_value))
      << kEfRuntimeParam
      << " must be a positive integer greater than 0 and cannot exceed "
      << max_ef_runtime_value << ".";
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

namespace options {

vmsdk::config::Number &GetMaxPrefixes() {
  return dynamic_cast<vmsdk::config::Number &>(*max_prefixes);
}

vmsdk::config::Number &GetMaxTagFieldLen() {
  return dynamic_cast<vmsdk::config::Number &>(*max_tag_field_len);
}

vmsdk::config::Number &GetMaxNumericFieldLen() {
  return dynamic_cast<vmsdk::config::Number &>(*max_numeric_field_len);
}

vmsdk::config::Number &GetMaxAttributes() {
  return dynamic_cast<vmsdk::config::Number &>(*max_attributes);
}

vmsdk::config::Number &GetMaxDimensions() {
  return dynamic_cast<vmsdk::config::Number &>(*max_dimensions);
}

vmsdk::config::Number &GetMaxM() {
  return dynamic_cast<vmsdk::config::Number &>(*max_m);
}

vmsdk::config::Number &GetMaxEfConstruction() {
  return dynamic_cast<vmsdk::config::Number &>(*max_ef_construction);
}

vmsdk::config::Number &GetMaxEfRuntime() {
  return dynamic_cast<vmsdk::config::Number &>(*max_ef_runtime);
}

vmsdk::config::Number &GetDefaultTimeoutMs() {
  return dynamic_cast<vmsdk::config::Number &>(*default_timeout_ms);
}
}  // namespace options
}  // namespace valkey_search
