/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/ft_create_parser.h"

#include <algorithm>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search {

namespace {

using testing::TestParamInfo;
using testing::ValuesIn;
struct AttributeParameters {
  absl::string_view identifier;
  absl::string_view attribute_alias;
  indexes::IndexerType indexer_type{indexes::IndexerType::kNone};
};

// Default stop words
const std::vector<std::string> kDefStopWords{
    "a", "is", "the", "an", "and", "are", "as", "at", "be", "but", "by", "for",
    "if", "in", "into", "it", "no", "not", "of", "on", "or", "such", "that", "their",
    "then", "there", "these", "they", "this", "to", "was", "will", "with"
};

struct ExpectedPerIndexTextParameters {
  std::string punctuation = ",.<>{}[]\"':;!@#$%^&*()-+=~/\\|";  // Default punctuation
  std::vector<std::string> stop_words = {kDefStopWords};  // Default stop words
  data_model::Language language = data_model::Language::LANGUAGE_ENGLISH;
  bool with_offsets = true;
};

struct FTCreateParameters {
  absl::string_view index_schema_name;
  data_model::AttributeDataType on_data_type{
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH};
  std::vector<absl::string_view> prefixes;
  float score{1.0};
  absl::string_view score_field;
  absl::string_view payload_field;
  std::vector<AttributeParameters> attributes;
  ExpectedPerIndexTextParameters per_index_text_params;
};

struct FTCreateParserTestCase {
  std::string test_name;
  bool success{false};
  absl::string_view command_str;
  bool too_many_attributes{false};
  std::vector<HNSWParameters> hnsw_parameters;
  std::vector<FlatParameters> flat_parameters;
  std::vector<FTCreateTagParameters> tag_parameters;
  std::vector<PerFieldTextParams> text_parameters;
  FTCreateParameters expected;
  std::string expected_error_message;
};

class FTCreateParserTest
    : public vmsdk::ValkeyTestWithParam<FTCreateParserTestCase> {};

void VerifyVectorParams(const data_model::VectorIndex &vector_index_proto,
                        const FTCreateVectorParameters *expected_params) {
  EXPECT_EQ(vector_index_proto.dimension_count(), expected_params->dimensions);
  EXPECT_EQ(vector_index_proto.distance_metric(),
            expected_params->distance_metric);
  EXPECT_EQ(vector_index_proto.vector_data_type(),
            expected_params->vector_data_type);
  EXPECT_EQ(vector_index_proto.initial_cap(), expected_params->initial_cap);
}

TEST_P(FTCreateParserTest, ParseParams) {
  const FTCreateParserTestCase &test_case = GetParam();
  auto command_str = std::string(test_case.command_str);
  if (test_case.too_many_attributes) {
    for (int i = 0; i < 50; ++i) {
      absl::StrAppend(&command_str, " hash_field", std::to_string(i + 2),
                      " vector hnsw 6 TYPE FLOAT32 DIM 3 "
                      "DISTANCE_METRIC IP ");
    }
  }
  auto args = vmsdk::ToValkeyStringVector(command_str);
  auto index_schema_proto =
      ParseFTCreateArgs(nullptr, args.data(), args.size());
  EXPECT_EQ(index_schema_proto.ok(), test_case.success);
  if (index_schema_proto.ok()) {
    auto params = index_schema_proto.value();
    EXPECT_EQ(index_schema_proto->name(), test_case.expected.index_schema_name);
    EXPECT_EQ(index_schema_proto->attribute_data_type(),
              test_case.expected.on_data_type);
    std::vector<absl::string_view> prefixes;
    prefixes.reserve(index_schema_proto->subscribed_key_prefixes().size());
    std::transform(index_schema_proto->subscribed_key_prefixes().begin(),
                   index_schema_proto->subscribed_key_prefixes().end(),
                   std::back_inserter(prefixes), [](const auto &prefix) {
                     return absl::string_view(prefix);
                   });
    EXPECT_EQ(prefixes, test_case.expected.prefixes);
    EXPECT_EQ(index_schema_proto->attributes().size(),
              test_case.expected.attributes.size());
              
    // Verify schema-level text parameters if we have text fields
    bool has_text_fields = false;
    for (const auto& attr : test_case.expected.attributes) {
      if (attr.indexer_type == indexes::IndexerType::kText) {
        has_text_fields = true;
        break;
      }
    }
    
    // Verify global text parameters in IndexSchema proto
    if (has_text_fields && test_case.success) {
      // Verify punctuation
      EXPECT_EQ(index_schema_proto->punctuation(), test_case.expected.per_index_text_params.punctuation);
      
      // Verify language
      EXPECT_EQ(index_schema_proto->language(), test_case.expected.per_index_text_params.language);
      
      // Verify with_offsets
      EXPECT_EQ(index_schema_proto->with_offsets(), test_case.expected.per_index_text_params.with_offsets);
      
      // Verify stop words
      std::vector<std::string> actual_stop_words;
      for (const auto& word : index_schema_proto->stop_words()) {
        actual_stop_words.push_back(word);
      }
      EXPECT_EQ(actual_stop_words, test_case.expected.per_index_text_params.stop_words);
    }
    
    auto hnsw_index = 0;
    auto flat_index = 0;
    auto tag_index = 0;
    auto text_index = 0;
    for (auto i = 0; i < index_schema_proto->attributes().size(); ++i) {
      EXPECT_EQ(index_schema_proto->attributes(i).identifier(),
                test_case.expected.attributes[i].identifier);
      EXPECT_EQ(index_schema_proto->attributes(i).alias(),
                test_case.expected.attributes[i].attribute_alias);
      if (test_case.expected.attributes[i].indexer_type ==
          indexes::IndexerType::kFlat) {
        EXPECT_TRUE(index_schema_proto->attributes(i)
                        .index()
                        .vector_index()
                        .has_flat_algorithm());
        VerifyVectorParams(
            index_schema_proto->attributes(i).index().vector_index(),
            &test_case.flat_parameters[flat_index]);
        EXPECT_EQ(index_schema_proto->attributes(i)
                      .index()
                      .vector_index()
                      .flat_algorithm()
                      .block_size(),
                  test_case.flat_parameters[flat_index].block_size);
        ++flat_index;
      } else if (test_case.expected.attributes[i].indexer_type ==
                 indexes::IndexerType::kHNSW) {
        EXPECT_TRUE(index_schema_proto->attributes(i)
                        .index()
                        .vector_index()
                        .has_hnsw_algorithm());
        VerifyVectorParams(
            index_schema_proto->attributes(i).index().vector_index(),
            &test_case.hnsw_parameters[hnsw_index]);
        auto hnsw_proto = index_schema_proto->attributes(i)
                              .index()
                              .vector_index()
                              .hnsw_algorithm();
        EXPECT_EQ(hnsw_proto.ef_construction(),
                  test_case.hnsw_parameters[hnsw_index].ef_construction);
        EXPECT_EQ(hnsw_proto.ef_runtime(),
                  test_case.hnsw_parameters[hnsw_index].ef_runtime);
        EXPECT_EQ(hnsw_proto.m(), test_case.hnsw_parameters[hnsw_index].m);
        ++hnsw_index;
      } else if (test_case.expected.attributes[i].indexer_type ==
                 indexes::IndexerType::kNumeric) {
        EXPECT_TRUE(
            index_schema_proto->attributes(i).index().has_numeric_index());
      } else if (test_case.expected.attributes[i].indexer_type ==
                 indexes::IndexerType::kTag) {
        EXPECT_TRUE(index_schema_proto->attributes(i).index().has_tag_index());
        auto tag_proto = index_schema_proto->attributes(i).index().tag_index();
        EXPECT_EQ(tag_proto.separator(),
                  test_case.tag_parameters[tag_index].separator);
        EXPECT_EQ(tag_proto.case_sensitive(),
                  test_case.tag_parameters[tag_index].case_sensitive);
        ++tag_index;
      } else if (test_case.expected.attributes[i].indexer_type ==
                 indexes::IndexerType::kText) {
        EXPECT_TRUE(index_schema_proto->attributes(i).index().has_text_index());
        if (text_index < test_case.text_parameters.size()) {
          auto text_proto = index_schema_proto->attributes(i).index().text_index();
          const auto& expected_text = test_case.text_parameters[text_index];
          EXPECT_EQ(text_proto.with_suffix_trie(), expected_text.with_suffix_trie);
          EXPECT_EQ(text_proto.no_stem(), expected_text.no_stem);
          EXPECT_EQ(text_proto.min_stem_size(), expected_text.min_stem_size);
        }
        ++text_index;
      } else {
        EXPECT_FALSE(index_schema_proto->attributes(i)
                         .index()
                         .vector_index()
                         .has_flat_algorithm());
        EXPECT_FALSE(index_schema_proto->attributes(i)
                         .index()
                         .vector_index()
                         .has_hnsw_algorithm());
      }
    }
  } else {
    if (!test_case.expected_error_message.empty()) {
      EXPECT_EQ(index_schema_proto.status().message(),
                test_case.expected_error_message);
    }
    std::cerr << "Failed to parse command: " << index_schema_proto.status()
              << "\n";
  }
  for (const auto &arg : args) {
    TestValkeyModule_FreeString(nullptr, arg);
  }
}

INSTANTIATE_TEST_SUITE_P(
    FTCreateParserTests, FTCreateParserTest,
    ValuesIn<FTCreateParserTestCase>(
        {
         {
             .test_name = "happy_path_hnsw",
             .success = true,
             .command_str = " idx1 on HASH PREFIx 3 abc def ghi LANGUAGe "
                            "ENGLISh SCORE 1.0 SChema hash_field1 as "
                            "hash_field11 vector hnsw 14 TYPE  FLOAT32 DIM 3  "
                            "DISTANCE_METRIC IP M 1 EF_CONSTRUCTION 5 "
                            " INITIAL_CAP 15000 EF_RUNTIME 25 ",
             .hnsw_parameters = {{
                 {
                     .dimensions = 3,
                     .distance_metric = data_model::DISTANCE_METRIC_IP,
                     .vector_data_type = data_model::VECTOR_DATA_TYPE_FLOAT32,
                     .initial_cap = 15000,
                 },
                 /* .m =*/1,
                 /* .ef_construction =*/5,
                 /* .ef_runtime =*/25,
             }},
             .expected = {.index_schema_name = "idx1",
                          .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                          .prefixes = {"abc", "def", "ghi"},
                          .attributes = {{
                              .identifier = "hash_field1",
                              .attribute_alias = "hash_field11",
                              .indexer_type = indexes::IndexerType::kHNSW,
                          }}},
         },
         {
             .test_name = "happy_path_flat",
             .success = true,
             .command_str = " idx1 on HASH PREFIx 3 abc def ghi LANGUAGe "
                            "ENGLISh SCORE 1.0 SChema hash_field1 as "
                            "hash_field11 vector flat 10 TYPE  FLOAT32 DIM 3  "
                            "DISTANCE_METRIC IP  "
                            " INITIAL_CAP 15000 BLOCK_SIZE 25 ",
             .flat_parameters = {{
                 {
                     .dimensions = 3,
                     .distance_metric = data_model::DISTANCE_METRIC_IP,
                     .vector_data_type = data_model::VECTOR_DATA_TYPE_FLOAT32,
                     .initial_cap = 15000,
                 },
                 /*.block_size =*/25,
             }},
             .expected = {.index_schema_name = "idx1",
                          .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                          .prefixes = {"abc", "def", "ghi"},
                          .attributes = {{
                              .identifier = "hash_field1",
                              .attribute_alias = "hash_field11",
                              .indexer_type = indexes::IndexerType::kFlat,
                          }}},
         },
         {
             .test_name = "happy_path_hnsw_and_numeric",
             .success = true,
             .command_str = " idx1 on HASH PREFIx 3 abc def ghi LANGUAGe "
                            "ENGLISh SCORE 1.0 SChema hash_field10 as "
                            "hash_field10 numeric hash_field1 as "
                            "hash_field11 vector hnsw 14 TYPE  FLOAT32 DIM 3  "
                            "DISTANCE_METRIC IP M 1 EF_CONSTRUCTION 5 "
                            " INITIAL_CAP 15000 EF_RUNTIME 25 ",
             .hnsw_parameters = {{
                 {
                     .dimensions = 3,
                     .distance_metric = data_model::DISTANCE_METRIC_IP,
                     .vector_data_type = data_model::VECTOR_DATA_TYPE_FLOAT32,
                     .initial_cap = 15000,
                 },
                 /* .m =*/1,
                 /* .ef_construction =*/5,
                 /* .ef_runtime =*/25,
             }},
             .expected =
                 {.index_schema_name = "idx1",
                  .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                  .prefixes = {"abc", "def", "ghi"},
                  .attributes =
                      {
                          {
                              .identifier = "hash_field10",
                              .attribute_alias = "hash_field10",
                              .indexer_type = indexes::IndexerType::kNumeric,
                          },
                          {
                              .identifier = "hash_field1",
                              .attribute_alias = "hash_field11",
                              .indexer_type = indexes::IndexerType::kHNSW,
                          },
                      }},
         },
         {
             .test_name = "happy_path_hnsw_and_tag_1",
             .success = true,
             .command_str =
                 " idx1 on HASH PREFIx 3 abc def ghi LANGUAGe "
                 "ENGLISh SCORE 1.0 SChema hash_field10 as "
                 "hash_field10 tag SEPARATOR '|' CASESENSITIVE hash_field1 as "
                 "hash_field11 vector hnsw 14 TYPE  FLOAT32 DIM 3  "
                 "DISTANCE_METRIC IP M 1 EF_CONSTRUCTION 5 "
                 " INITIAL_CAP 15000 EF_RUNTIME 25 ",
             .hnsw_parameters = {{
                 {
                     .dimensions = 3,
                     .distance_metric = data_model::DISTANCE_METRIC_IP,
                     .vector_data_type = data_model::VECTOR_DATA_TYPE_FLOAT32,
                     .initial_cap = 15000,
                 },
                 /* .m =*/1,
                 /* .ef_construction =*/5,
                 /* .ef_runtime =*/25,
             }},
             .tag_parameters = {{
                 .separator = "|",
                 .case_sensitive = true,
             }},
             .expected =
                 {.index_schema_name = "idx1",
                  .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                  .prefixes = {"abc", "def", "ghi"},
                  .attributes =
                      {
                          {
                              .identifier = "hash_field10",
                              .attribute_alias = "hash_field10",
                              .indexer_type = indexes::IndexerType::kTag,
                          },
                          {
                              .identifier = "hash_field1",
                              .attribute_alias = "hash_field11",
                              .indexer_type = indexes::IndexerType::kHNSW,
                          },
                      }},
         },
         {
             .test_name = "happy_path_hnsw_and_tag_2",
             .success = true,
             .command_str =
                 " idx1 on HASH PREFIx 3 abc def ghi LANGUAGe "
                 "ENGLISh SCORE 1.0 SChema hash_field20 as "
                 "hash_field20 tag SEPARATOR '|' CASESENSITIVE hash_field21 as "
                 "hash_field21 tag SEPARATOR $ hash_field22 as "
                 "hash_field22 tag  hash_field1 as "
                 "hash_field11 vector hnsw 14 TYPE  FLOAT32 DIM 3  "
                 "DISTANCE_METRIC IP M 1 EF_CONSTRUCTION 5 "
                 " INITIAL_CAP 15000 EF_RUNTIME 25 ",
             .hnsw_parameters = {{
                 {
                     .dimensions = 3,
                     .distance_metric = data_model::DISTANCE_METRIC_IP,
                     .vector_data_type = data_model::VECTOR_DATA_TYPE_FLOAT32,
                     .initial_cap = 15000,
                 },
                 /* .m =*/1,
                 /* .ef_construction =*/5,
                 /* .ef_runtime =*/25,
             }},
             .tag_parameters = {{
                                    .separator = "|",
                                    .case_sensitive = true,
                                },
                                {
                                    .separator = "$",
                                    .case_sensitive = false,
                                },
                                {
                                    .separator = ",",
                                    .case_sensitive = false,
                                }},
             .expected =
                 {.index_schema_name = "idx1",
                  .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                  .prefixes = {"abc", "def", "ghi"},
                  .attributes =
                      {
                          {
                              .identifier = "hash_field20",
                              .attribute_alias = "hash_field20",
                              .indexer_type = indexes::IndexerType::kTag,
                          },
                          {
                              .identifier = "hash_field21",
                              .attribute_alias = "hash_field21",
                              .indexer_type = indexes::IndexerType::kTag,
                          },
                          {
                              .identifier = "hash_field22",
                              .attribute_alias = "hash_field22",
                              .indexer_type = indexes::IndexerType::kTag,
                          },
                          {
                              .identifier = "hash_field1",
                              .attribute_alias = "hash_field11",
                              .indexer_type = indexes::IndexerType::kHNSW,
                          },
                      }},
         },
         {
             .test_name = "happy_path_flat_and_numeric",
             .success = true,
             .command_str = " idx1 on HASH PREFIx 3 abc def ghi LANGUAGe "
                            "ENGLISh SCORE 1.0 SChema hash_field1 as "
                            "hash_field11 vector flat 10 TYPE  FLOAT32 DIM 3  "
                            "DISTANCE_METRIC IP  "
                            " INITIAL_CAP 15000 BLOCK_SIZE 25 hash_field10 as "
                            "hash_field10 numeric ",
             .flat_parameters = {{
                 {
                     .dimensions = 3,
                     .distance_metric = data_model::DISTANCE_METRIC_IP,
                     .vector_data_type = data_model::VECTOR_DATA_TYPE_FLOAT32,
                     .initial_cap = 15000,
                 },
                 /*.block_size =*/25,
             }},
             .expected =
                 {.index_schema_name = "idx1",
                  .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                  .prefixes = {"abc", "def", "ghi"},
                  .attributes =
                      {
                          {
                              .identifier = "hash_field1",
                              .attribute_alias = "hash_field11",
                              .indexer_type = indexes::IndexerType::kFlat,
                          },
                          {
                              .identifier = "hash_field10",
                              .attribute_alias = "hash_field10",
                              .indexer_type = indexes::IndexerType::kNumeric,
                          },
                      }},
         },
         {
             .test_name = "happy_path_flat_and_tag_1",
             .success = true,
             .command_str = " idx1 on HASH PREFIx 3 abc def ghi LANGUAGe "
                            "ENGLISh SCORE 1.0 SChema hash_field1 as "
                            "hash_field11 vector flat 10 TYPE  FLOAT32 DIM 3  "
                            "DISTANCE_METRIC IP  "
                            " INITIAL_CAP 15000 BLOCK_SIZE 25 hash_field10 as "
                            "hash_field10 tag SEPARATOR \"@\"",
             .flat_parameters = {{
                 {
                     .dimensions = 3,
                     .distance_metric = data_model::DISTANCE_METRIC_IP,
                     .vector_data_type = data_model::VECTOR_DATA_TYPE_FLOAT32,
                     .initial_cap = 15000,
                 },
                 /*.block_size =*/25,
             }},
             .tag_parameters = {{
                 .separator = "@",
                 .case_sensitive = false,
             }},
             .expected =
                 {.index_schema_name = "idx1",
                  .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                  .prefixes = {"abc", "def", "ghi"},
                  .attributes =
                      {
                          {
                              .identifier = "hash_field1",
                              .attribute_alias = "hash_field11",
                              .indexer_type = indexes::IndexerType::kFlat,
                          },
                          {
                              .identifier = "hash_field10",
                              .attribute_alias = "hash_field10",
                              .indexer_type = indexes::IndexerType::kTag,
                          },
                      }},
         },
         {
             .test_name = "happy_path_hnsw_3_attributes",
             .success = true,
             .command_str =
                 "idx1 on HASH SChema hash_field1 as "
                 "hash_field11 vector hnsw 12 TYPE  FLOAT32 DIM 3  "
                 "DISTANCE_METRIC IP EF_CONSTRUCTION 5 "
                 " INITIAL_CAP 15000  EF_RUNTIME 25 "
                 "hash_field3 vecTor hnsw 6 DISTANCE_METRIC COSINE TYPE "
                 "FLOAT32 DIM 5 "
                 "hash_field4 Vector Hnsw 8 DISTANCE_METRIc cOSINE tYPE "
                 "FLOAt32 dIM 15 m 12 ",
             .hnsw_parameters =
                 {{
                      FTCreateVectorParameters{
                          .dimensions = 3,
                          .distance_metric = data_model::DISTANCE_METRIC_IP,
                          .vector_data_type =
                              data_model::VECTOR_DATA_TYPE_FLOAT32,
                          .initial_cap = 15000,
                      },
                      /* .m = */ kDefaultM,
                      /* .ef_construction = */ 5,
                      /* .ef_runtime = */ 25,
                  },
                  {
                      {
                          .dimensions = 5,
                          .distance_metric = data_model::DISTANCE_METRIC_COSINE,
                          .vector_data_type =
                              data_model::VECTOR_DATA_TYPE_FLOAT32,
                          .initial_cap = kDefaultInitialCap,
                      },
                      /* .m = */ kDefaultM,
                      /* .ef_construction = */
                      kDefaultEFConstruction,
                      /* .ef_runtime = */
                      kDefaultEFRuntime,
                  },
                  {
                      {
                          .dimensions = 15,
                          .distance_metric = data_model::DISTANCE_METRIC_COSINE,
                          .vector_data_type =
                              data_model::VECTOR_DATA_TYPE_FLOAT32,
                          .initial_cap = kDefaultInitialCap,
                      },
                      /* .m = */ 12,
                      /* .ef_construction = */
                      kDefaultEFConstruction,
                      /* .ef_runtime = */
                      kDefaultEFRuntime,
                  }},
             .expected = {.index_schema_name = "idx1",
                          .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                          .attributes =
                              {{
                                   .identifier = "hash_field1",
                                   .attribute_alias = "hash_field11",
                                   .indexer_type = indexes::IndexerType::kHNSW,
                               },
                               {
                                   .identifier = "hash_field3",
                                   .attribute_alias = "hash_field3",
                                   .indexer_type = indexes::IndexerType::kHNSW,
                               },
                               {
                                   .identifier = "hash_field4",
                                   .attribute_alias = "hash_field4",
                                   .indexer_type = indexes::IndexerType::kHNSW,
                               }}},
         },
         {
             .test_name = "happy_path_hnsw_default_on_hash",
             .success = true,
             .command_str = " idx1 SChema hash_field1 as "
                            "hash_field11 vector hnsw 6 TYPE  FLOAT32 DIM 3 "
                            "DISTANCE_METRIC IP ",
             .hnsw_parameters = {{
                 {
                     .dimensions = 3,
                     .distance_metric = data_model::DISTANCE_METRIC_IP,
                     .vector_data_type = data_model::VECTOR_DATA_TYPE_FLOAT32,
                 },
             }},
             .expected = {.index_schema_name = "idx1",
                          .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                          .attributes = {{
                              .identifier = "hash_field1",
                              .attribute_alias = "hash_field11",
                              .indexer_type = indexes::IndexerType::kHNSW,
                          }}},
         },
         {
             .test_name = "happy_path_numeric_index_on_hash",
             .success = true,
             .command_str = "idx1 on HASH SChema hash_field1 as "
                            "hash_field11 numeric ",
             .expected = {.index_schema_name = "idx1",
                          .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                          .attributes = {{
                              .identifier = "hash_field1",
                              .attribute_alias = "hash_field11",
                              .indexer_type = indexes::IndexerType::kNumeric,
                          }}},
         },
         {
             .test_name = "happy_path_tag_index_on_hash",
             .success = true,
             .command_str = "idx1 on HASH SCHEMA hash_field1 as "
                            "hash_field11 tag ",
             .tag_parameters = {{
                 .separator = ",",
                 .case_sensitive = false,
             }},
             .expected = {.index_schema_name = "idx1",
                          .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                          .attributes = {{
                              .identifier = "hash_field1",
                              .attribute_alias = "hash_field11",
                              .indexer_type = indexes::IndexerType::kTag,
                          }}},
         },
         {
             .test_name = "invalid_separator",
             .success = false,
             .command_str =
                 " idx1 on HASH PREFIx 3 abc def ghi LANGUAGe "
                 "ENGLISh SCORE 1.0 SChema hash_field10 as "
                 "hash_field10 tag SEPARATOR @@ CASESENSITIVE hash_field1 as "
                 "hash_field11 vector hnsw 14 TYPE  FLOAT32 DIM 3  "
                 "DISTANCE_METRIC IP M 1 EF_CONSTRUCTION 5 "
                 " INITIAL_CAP 15000 EF_RUNTIME 25 ",
             .tag_parameters = {{
                 .separator = "@@",
             }},
             .expected_error_message =
                 "Invalid field type for field `hash_field10`: The separator "
                 "must be a single character, but got `@@`",
         },
         {
             .test_name = "duplicate_identifier",
             .success = false,
             .command_str =
                 "idx1 on HASH SChema hash_field1 vector hnsw 6 TYPE "
                 "FLOAT32 DIM 3  DISTANCE_METRIC Ip "
                 "hash_field1 vector hnsw 6 TYPE FLOAT32 DIM 3 "
                 "DISTANCE_METRIC Ip",
             .expected_error_message =
                 "Duplicate field in schema - hash_field1",
         },
         {
             .test_name = "trailing_invalid_token_at_the_end",
             .success = false,
             .command_str =
                 " idx1 on HASH PREFIx 3 abc def ghi LANGUAGe "
                 "ENGLISh SCORE 1.0 SChema hash_field1 as "
                 "hash_field11 vector hnsw 14 TYPE  FLOAT32 DIM 3  "
                 "DISTANCE_METRIC IP M 1 EF_CONSTRUCTION 5 "
                 " INITIAL_CAP 15000 EF_RUNTIME 25 random_token_at_the_end",
             .expected_error_message =
                 "Invalid field type for field `random_token_at_the_end`: "
                 "Missing argument",
         },
         {
             .test_name = "invalid_ef_runtime_negative",
             .success = false,
             .command_str = "idx1 SChema hash_field1 as "
                            "hash_field11 vector hnsw 8 TYPE  FLOAT32 DIM 3 "
                            "DISTANCE_METRIC IP EF_RUNTIME -100",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Error parsing "
                 "value for the parameter `EF_RUNTIME` - `-100` is outside "
                 "acceptable "
                 "bounds",
         },
         {
             .test_name = "invalid_ef_runtime_zero",
             .success = false,
             .command_str = "idx1 SChema hash_field1 as "
                            "hash_field11 vector hnsw 8 TYPE  FLOAT32 DIM 3 "
                            "DISTANCE_METRIC IP EF_RUNTIME 0",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Invalid range: "
                 "Value below minimum; EF_RUNTIME must be a positive integer "
                 "greater than 0 and cannot exceed 4096.",
         },
         {
             .test_name = "invalid_m_negative",
             .success = false,
             .command_str = "idx1 SChema hash_field1 as "
                            "hash_field11 vector hnsw 8 TYPE  FLOAT32 DIM 3 "
                            "DISTANCE_METRIC IP M -10",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Invalid range: "
                 "Value below minimum; M must be a positive integer greater "
                 "than 0 and cannot exceed 2000000.",
         },
         {
             .test_name = "invalid_m_too_big",
             .success = false,
             .command_str = "idx1 SChema hash_field1 as "
                            "hash_field11 vector hnsw 8 TYPE  FLOAT32 DIM 3 "
                            "DISTANCE_METRIC IP M 3000000",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Invalid range: "
                 "Value above maximum; M must be a positive integer greater "
                 "than 0 and cannot exceed 2000000.",
         },
         {
             .test_name = "invalid_ef_construction_zero",
             .success = false,
             .command_str = "idx1 SChema hash_field1 as "
                            "hash_field11 vector hnsw 8 TYPE  FLOAT32 DIM 3 "
                            "DISTANCE_METRIC IP EF_CONSTRUCTIOn 0",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Invalid range: "
                 "Value below minimum; EF_CONSTRUCTION must be a positive "
                 "integer greater than 0 and cannot exceed 4096.",
         },
         {
             .test_name = "invalid_ef_construction_negative",
             .success = false,
             .command_str = "idx1 SChema hash_field1 as "
                            "hash_field11 vector hnsw 8 TYPE  FLOAT32 DIM 3 "
                            "DISTANCE_METRIC IP EF_CONSTRUCTIOn -100",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Invalid range: "
                 "Value below minimum; EF_CONSTRUCTION must be a positive "
                 "integer greater than 0 and cannot exceed 4096.",
         },
         {
             .test_name = "invalid_as",
             .success = false,
             .command_str = "idx1 SChema hash_field1 asa "
                            "hash_field11 vector hnsw 6 TYPE  FLOAT32 DIM 3 "
                            "DISTANCE_METRIC IP ",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Unknown argument "
                 "`asa`",
         },
         {.test_name = "invalid_negative_prefix_cnt",
          .success = false,
          .command_str =
              "idx1 prefix -2 SChema hash_field1 vector1 hnsw 6 TYPE "
              "FLOAT32 DIM 3 DISTANCE_METRIC IP ",
          .expected_error_message =
              "Bad arguments for PREFIX: `-2` is outside acceptable bounds"},
         {.test_name = "invalid_too_bit_prefix_cnt",
          .success = false,
          .command_str = "idx1 prefix 20 SChema hash_field1vector1 hnsw 6 TYPE "
                         "FLOAT32 DIM 3 DISTANCE_METRIC IP ",
          .expected_error_message =
              "Bad arguments for PREFIX: `20` is outside acceptable bounds"},
         {
             .test_name = "invalid_vector",
             .success = false,
             .command_str = "idx1 SChema hash_field1 vector1 hnsw 6 TYPE "
                            "FLOAT32 DIM 3 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Unknown argument "
                 "`vector1`",
         },
         {.test_name = "invalid_hnsw",
          .success = false,
          .command_str = "idx1 SChema hash_field1 vector hnsw1 6 TYPE "
                         "FLOAT32 DIM 3 DISTANCE_METRIC IP ",
          .expected_error_message =
              "Invalid field type for field `hash_field1`: Unknown argument "
              "`hnsw1`"},
         {
             .test_name = "invalid_too_many_attributes",
             .success = false,
             .command_str = "idx1 SChema "
                            "hash_field1 vector hnsw 6 TYPE FLOAT32 DIM 3 "
                            "DISTANCE_METRIC IP ",
             .too_many_attributes = true,
             .expected_error_message =
                 "Invalid range: Value above maximum; The maximum number of "
                 "attributes cannot exceed 50.",
         },
         {
             .test_name = "invalid_param_num_1",
             .success = false,
             .command_str = "idx1 SChema hash_field1 vector hnsw 8 TYPE "
                            "FLOAT32 DIM 3 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Expected 8 "
                 "parameters for HNSW but got 6 parameters.",
         },
         {
             .test_name = "invalid_param_num_2",
             .success = false,
             .command_str = " idx1 SChema hash_field1 vector hnsw 5 TYPE "
                            "FLOAT32 DIM 3 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Error parsing "
                 "value for the parameter `DISTANCE_METRIC` - Missing argument",
         },
         {
             .test_name = "invalid_param_num_3",
             .success = false,
             .command_str = "idx1 SChema hash_field1 vector hnsw -6 TYPE "
                            "FLOAT32 DIM 3 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: `-6` is outside "
                 "acceptable bounds",
         },
         {
             .test_name = "invalid_flat_param_num_1",
             .success = false,
             .command_str = "idx1 SChema hash_field1 vector flat 8 TYPE "
                            "FLOAT32 DIM 3 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Expected 8 "
                 "parameters for FLAT but got 6 parameters.",
         },
         {
             .test_name = "invalid_flat_param_num_2",
             .success = false,
             .command_str = " idx1 SChema hash_field1 vector FLAT 5 TYPE "
                            "FLOAT32 DIM 3 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Error parsing "
                 "value for the parameter `DISTANCE_METRIC` - Missing argument",
         },
         {
             .test_name = "invalid_flat_param_num_3",
             .success = false,
             .command_str = "idx1 SChema hash_field1 vector flat -6 TYPE "
                            "FLOAT32 DIM 3 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: `-6` is outside "
                 "acceptable bounds",
         },
         {
             .test_name = "invalid_type_1",
             .success = false,
             .command_str = " idx1 SChema hash_field1 vector hnsw 6 TYPE1 "
                            "FLOAT32 DIM 3 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Unexpected "
                 "argument `TYPE1`",
         },
         {
             .test_name = "invalid_type_2",
             .success = false,
             .command_str = " idx1 SChema hash_field1 vector hnsw 6 TYPE "
                            "FLOAT321 DIM 3 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Error parsing "
                 "value for the parameter `TYPE` - Unknown argument `FLOAT321`",
         },
         {
             .test_name = "invalid_dim_1",
             .success = false,
             .command_str = " idx1 SChema hash_field1 vector hnsw 6 TYPE1 "
                            "FLOAT32 DIM1 3 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Unexpected "
                 "argument `TYPE1`",
         },
         {
             .test_name = "invalid_dim_2",
             .success = false,
             .command_str = " idx1 SChema hash_field1 vector hnsw 6 TYPE "
                            "FLOAT321 DIM a DISTANCE_METRIC IP ",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Error parsing "
                 "value for the parameter `TYPE` - Unknown argument `FLOAT321`",
         },
         {
             .test_name = "invalid_dim_3",
             .success = false,
             .command_str = " idx1 SChema hash_field1 vector hnsw 6 TYPE "
                            "FLOAT321 DIM -5 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Error parsing "
                 "value for the parameter `TYPE` - Unknown argument `FLOAT321`",
         },
         {
             .test_name = "invalid_distance_1",
             .success = false,
             .command_str = " idx1 SChema hash_field1 vector hnsw 6 TYPE1 "
                            "FLOAT32 DIM 3 DISTANCE_METRIC1 IP ",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Unexpected "
                 "argument `TYPE1`",
         },
         {
             .test_name = "invalid_distance_2",
             .success = false,
             .command_str = " idx1 SChema hash_field1 vector hnsw 6 TYPE "
                            "FLOAT321 DIM 3 DISTANCE_METRIC IP1 ",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: Error parsing "
                 "value for the parameter `TYPE` - Unknown argument `FLOAT321`",
         },
         {
             .test_name = "unexpected_filter",
             .success = false,
             .command_str =
                 " idx1 filter aa SChema hash_field1 vector hnsw 6 TYPE "
                 "FLOAT321 DIM 5 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "The parameter `FILTER` is not supported",
         },
         {
             .test_name = "invalid_language_parameter_value",
             .success = false,
             .command_str = " idx1 LANGUAGE hebrew SChema hash_field1 vector "
                            "hnsw 6 TYPE FLOAT321 DIM 5 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "Bad arguments for LANGUAGE: Unknown argument `hebrew`",
         },
         {
             .test_name = "unexpected_language_field",
             .success = false,
             .command_str = " idx1 LANGUAGE_FIELD aa SChema hash_field1 vector "
                            "hnsw 6 TYPE FLOAT321 DIM 5 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "The parameter `LANGUAGE_FIELD` is not supported",
         },
         {
             .test_name = "invalid_score_parameter_value",
             .success = false,
             .command_str =
                 " idx1 SCORE 2 SChema hash_field1 vector hnsw 6 TYPE "
                 "FLOAT321 DIM 5 DISTANCE_METRIC IP ",
             .expected_error_message = "`SCORE` parameter with a value `2` is "
                                       "not supported. The only "
                                       "supported value is `1.0`",
         },
         {
             .test_name = "unexpected_score_field",
             .success = false,
             .command_str =
                 " idx1 SCORE_FIELD SChema hash_field1 vector hnsw 6 TYPE "
                 "FLOAT321 DIM 5 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "The parameter `SCORE_FIELD` is not supported",
         },
         {
             .test_name = "invalid_parameter_before_schema",
             .success = false,
             .command_str =
                 " idx1 SCOREa 2 SChema hash_field1 vector hnsw 6 TYPE "
                 "FLOAT321 DIM 5 DISTANCE_METRIC IP ",
             .expected_error_message =
                 "Unexpected parameter `SCOREa`, expecting `SCHEMA`",
         },
         {
             .test_name = "missing_schema",
             .success = false,
             .command_str = "idx prefix 1 x",
             .expected_error_message = "Missing argument",
         },
         {
             .test_name = "missing_schema_2",
             .success = false,
             .command_str = "idx",
             .expected_error_message = "Missing argument",
         },
         {
             .test_name = "invalid_index_name",
             .success = false,
             .command_str = "idx{a}",
             .expected_error_message = "Index name must not contain a hash tag",
         },
         {
             .test_name = "invalid_index_prefix",
             .success = false,
             .command_str = "idx on hash prefix 1 a{b}",
             .expected_error_message =
                 "PREFIX argument(s) must not contain a hash tag",
         },
         // TEXT field tests
         {
             .test_name = "happy_path_text_basic",
             .success = true,
             .command_str = "idx1 on HASH SCHEMA text_field TEXT",
             .too_many_attributes = false,
             .hnsw_parameters = {},
             .flat_parameters = {},
             .tag_parameters = {},
             .text_parameters = {{
                 .with_suffix_trie = false,
                 .no_stem = false,
                 .min_stem_size = 4,
             }},
             .expected = {
                 .index_schema_name = "idx1",
                 .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                 .attributes = {{
                     .identifier = "text_field",
                     .attribute_alias = "text_field",
                     .indexer_type = indexes::IndexerType::kText,
                 }}
             },
             .expected_error_message = "",
         },
         {
             .test_name = "happy_path_text_with_field_parameters",
             .success = true,
             .command_str = "idx1 on HASH SCHEMA text_field TEXT WITHSUFFIXTRIE MINSTEMSIZE 2",
             .too_many_attributes = false,
             .hnsw_parameters = {},
             .flat_parameters = {},
             .tag_parameters = {},
             .text_parameters = {{
                 .with_suffix_trie = true,
                 .no_stem = false,
                 .min_stem_size = 2,
             }},
             .expected = {
                 .index_schema_name = "idx1",
                 .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                 .attributes = {{
                     .identifier = "text_field",
                     .attribute_alias = "text_field",
                     .indexer_type = indexes::IndexerType::kText,
                 }}
             },
             .expected_error_message = "",
         },
         {
             .test_name = "happy_path_text_with_per_index_parameters",
             .success = true,
             .command_str = "idx1 on HASH PUNCTUATION \",.;\" WITHOFFSETS NOSTEM STOPWORDS 3 the and or SCHEMA text_field TEXT",
             .too_many_attributes = false,
             .hnsw_parameters = {},
             .flat_parameters = {},
             .tag_parameters = {},
             .text_parameters = {{
                 .with_suffix_trie = false,
                 .no_stem = true,
                 .min_stem_size = 4,
             }},
             .expected = {
                 .index_schema_name = "idx1",
                 .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                 .attributes = {{
                     .identifier = "text_field",
                     .attribute_alias = "text_field",
                     .indexer_type = indexes::IndexerType::kText,
                 }},
                 .per_index_text_params = {
                     .punctuation = ",.;",
                     .stop_words = {"the", "and", "or"},
                     .language = data_model::Language::LANGUAGE_ENGLISH,
                     .with_offsets = true,
                 }
             },
             .expected_error_message = "",
         },
         {
             .test_name = "happy_path_text_per_index_nostopwords",
             .success = true,
             .command_str = "idx1 on HASH NOSTOPWORDS SCHEMA text_field TEXT",
             .too_many_attributes = false,
             .hnsw_parameters = {},
             .flat_parameters = {},
             .tag_parameters = {},
             .text_parameters = {{
                 .with_suffix_trie = false,
                 .no_stem = false,
                 .min_stem_size = 4,
             }},
             .expected = {
                 .index_schema_name = "idx1",
                 .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                 .attributes = {{
                     .identifier = "text_field",
                     .attribute_alias = "text_field",
                     .indexer_type = indexes::IndexerType::kText,
                 }},
                 .per_index_text_params = {
                     .punctuation = ",.<>{}[]\"':;!@#$%^&*()-+=~/\\|",
                     .stop_words = {},  // Empty due to NOSTOPWORDS
                     .language = data_model::Language::LANGUAGE_ENGLISH,
                     .with_offsets = true,
                 }
             },
             .expected_error_message = "",
         },
         {
             .test_name = "happy_path_text_per_index_stopwords_zero",
             .success = true,
             .command_str = "idx1 on HASH STOPWORDS 0 SCHEMA text_field TEXT",
             .too_many_attributes = false,
             .hnsw_parameters = {},
             .flat_parameters = {},
             .tag_parameters = {},
             .text_parameters = {{
                 .with_suffix_trie = false,
                 .no_stem = false,
                 .min_stem_size = 4,
             }},
             .expected = {
                 .index_schema_name = "idx1",
                 .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                 .attributes = {{
                     .identifier = "text_field",
                     .attribute_alias = "text_field",
                     .indexer_type = indexes::IndexerType::kText,
                 }},
                 .per_index_text_params = {
                     .punctuation = ",.<>{}[]\"':;!@#$%^&*()-+=~/\\|",
                     .stop_words = {},  // Empty due to STOPWORDS 0
                     .language = data_model::Language::LANGUAGE_ENGLISH,
                     .with_offsets = true,
                 }
             },
             .expected_error_message = "",
         },
         {
             .test_name = "happy_path_text_with_vector",
             .success = true,
             .command_str = "idx1 on HASH SCHEMA text_field TEXT vector_field VECTOR HNSW 6 TYPE FLOAT32 DIM 3 DISTANCE_METRIC IP",
             .too_many_attributes = false,
             .hnsw_parameters = {{
                 {
                     .dimensions = 3,
                     .distance_metric = data_model::DISTANCE_METRIC_IP,
                     .vector_data_type = data_model::VECTOR_DATA_TYPE_FLOAT32,
                     .initial_cap = kDefaultInitialCap,
                 },
                 /* .m = */ kDefaultM,
                 /* .ef_construction = */ kDefaultEFConstruction,
                 /* .ef_runtime = */ kDefaultEFRuntime,
             }},
             .flat_parameters = {},
             .tag_parameters = {},
             .text_parameters = {{
                 .with_suffix_trie = false,
                 .no_stem = false,
                 .min_stem_size = 4,
             }},
             .expected = {
                 .index_schema_name = "idx1",
                 .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                 .attributes = {
                     {
                         .identifier = "text_field",
                         .attribute_alias = "text_field",
                         .indexer_type = indexes::IndexerType::kText,
                     },
                     {
                         .identifier = "vector_field",
                         .attribute_alias = "vector_field",
                         .indexer_type = indexes::IndexerType::kHNSW,
                     }
                 }
             },
             .expected_error_message = "",
         },
         {
             .test_name = "text_field_nostopwords_invalid",
             .success = false,
             .command_str = "idx1 on HASH SCHEMA text_field TEXT NOSTOPWORDS",
             .too_many_attributes = false,
             .hnsw_parameters = {},
             .flat_parameters = {},
             .tag_parameters = {},
             .text_parameters = {},
             .expected = {},
             .expected_error_message = "Invalid field type for field `NOSTOPWORDS`: Missing argument",
         },
         {
             .test_name = "invalid_text_empty_punctuation_per_index",
             .success = false,
             .command_str = "idx1 on HASH PUNCTUATION \"\" SCHEMA text_field TEXT",
             .too_many_attributes = false,
             .hnsw_parameters = {},
             .flat_parameters = {},
             .tag_parameters = {},
             .text_parameters = {},
             .expected = {},
             .expected_error_message = "PUNCTUATION string cannot be empty",
         },
         {
             .test_name = "invalid_text_negative_minstemsize",
             .success = false,
             .command_str = "idx1 on HASH SCHEMA text_field TEXT MINSTEMSIZE -1",
             .too_many_attributes = false,
             .hnsw_parameters = {},
             .flat_parameters = {},
             .tag_parameters = {},
             .text_parameters = {},
             .expected = {},
             .expected_error_message = "Invalid field type for field `text_field`: Error parsing value for the parameter `MINSTEMSIZE` - MINSTEMSIZE must be positive",
         },
         {
             .test_name = "invalid_text_zero_minstemsize",
             .success = false,
             .command_str = "idx1 on HASH SCHEMA text_field TEXT MINSTEMSIZE 0",
             .too_many_attributes = false,
             .hnsw_parameters = {},
             .flat_parameters = {},
             .tag_parameters = {},
             .text_parameters = {},
             .expected = {},
             .expected_error_message = "Invalid field type for field `text_field`: Error parsing value for the parameter `MINSTEMSIZE` - MINSTEMSIZE must be positive",
         },
         {
             .test_name = "invalid_per_index_stopwords_before_schema",
             .success = false,
             .command_str = "idx1 on HASH STOPWORDS -1 SCHEMA text_field TEXT",
             .too_many_attributes = false,
             .hnsw_parameters = {},
             .flat_parameters = {},
             .tag_parameters = {},
             .text_parameters = {},
             .expected = {},
             .expected_error_message = "Error parsing value for the parameter `STOPWORDS` - `-1` is outside acceptable bounds",
         },
         {
             .test_name = "invalid_per_index_stopwords_missing_words",
             .success = false,
             .command_str = "idx1 on HASH STOPWORDS 3 the and SCHEMA text_field TEXT",
             .too_many_attributes = false,
             .hnsw_parameters = {},
             .flat_parameters = {},
             .tag_parameters = {},
             .text_parameters = {},
             .expected = {},
             .expected_error_message = "Unexpected parameter `text_field`, expecting `SCHEMA`",
         },
         // Additional TEXT field edge case tests
         {
             .test_name = "text_field_punctuation_single_quote_invalid",
             .success = false,
             .command_str = "idx1 on HASH SCHEMA text_field TEXT PUNCTUATION '.,;'",
             .too_many_attributes = false,
             .hnsw_parameters = {},
             .flat_parameters = {},
             .tag_parameters = {},
             .text_parameters = {},
             .expected = {},
             .expected_error_message = "Invalid field type for field `PUNCTUATION`: Unknown argument `.,;`",
         },
         {
             .test_name = "text_field_punctuation_unquoted_invalid",
             .success = false,
             .command_str = "idx1 on HASH SCHEMA text_field TEXT PUNCTUATION .,;",
             .text_parameters = {},
             .expected = {},
             .expected_error_message = "Invalid field type for field `PUNCTUATION`: Unknown argument `.,;`",
         },
         {
            .test_name = "text_nooffsets_flag",
            .success = true,
            .command_str = "idx1 on HASH NOOFFSETS SCHEMA text_field TEXT",
            .text_parameters = {{
                .with_suffix_trie = false,
                .no_stem = false,
                .min_stem_size = 4,
            }},
            .expected = {
                .index_schema_name = "idx1",
                .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                .attributes = {{
                    .identifier = "text_field",
                    .attribute_alias = "text_field",
                    .indexer_type = indexes::IndexerType::kText,
                }},
                .per_index_text_params = {
                    .punctuation = ",.<>{}[]\"':;!@#$%^&*()-+=~/\\|",
                    .stop_words = {kDefStopWords},
                    .language = data_model::Language::LANGUAGE_ENGLISH,
                    .with_offsets = false,  // NOOFFSETS should set this to false
                }
            },
        },
         {
             .test_name = "text_withsuffixtrie_flag",
             .success = true,
             .command_str = "idx1 on HASH SCHEMA text_field TEXT WITHSUFFIXTRIE",
             .text_parameters = {{
                 .with_suffix_trie = true,
                 .no_stem = false,
                 .min_stem_size = 4,
             }},
             .expected = {
                 .index_schema_name = "idx1",
                 .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                 .attributes = {{
                     .identifier = "text_field",
                     .attribute_alias = "text_field",
                     .indexer_type = indexes::IndexerType::kText,
                 }}
             },
         },
         {
             .test_name = "text_nosuffixtrie_flag",
             .success = true,
             .command_str = "idx1 on HASH SCHEMA text_field TEXT NOSUFFIXTRIE",
             .text_parameters = {{
                 .with_suffix_trie = false,
                 .no_stem = false,
                 .min_stem_size = 4,
             }},
             .expected = {
                 .index_schema_name = "idx1",
                 .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                 .attributes = {{
                     .identifier = "text_field",
                     .attribute_alias = "text_field",
                     .indexer_type = indexes::IndexerType::kText,
                 }}
             },
         },
         {
             .test_name = "text_combined_per_index_and_field_flags",
             .success = true,
             .command_str = "idx1 on HASH NOOFFSETS NOSTEM LANGUAGE ENGLISH SCHEMA text_field TEXT WITHSUFFIXTRIE MINSTEMSIZE 2",
             .text_parameters = {{
                 .with_suffix_trie = true,
                 .no_stem = true,
                 .min_stem_size = 2,
             }},
             .expected = {
                 .index_schema_name = "idx1",
                 .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                 .attributes = {{
                     .identifier = "text_field",
                     .attribute_alias = "text_field",
                     .indexer_type = indexes::IndexerType::kText,
                 }},
                 .per_index_text_params = {
                     .punctuation = ",.<>{}[]\"':;!@#$%^&*()-+=~/\\|",
                     .stop_words = {kDefStopWords},
                     .language = data_model::Language::LANGUAGE_ENGLISH,
                     .with_offsets = false,
                 }
             }
         },
         {
             .test_name = "text_large_stopwords_list_field",
             .success = false,
             .command_str = "idx1 on HASH SCHEMA text_field TEXT STOPWORDS 10 a an and are as at be but by for",
             .text_parameters = {},
             .expected = {},
             .expected_error_message = "Invalid field type for field `STOPWORDS`: Unknown argument `10`",
         },
         {
            .test_name = "text_large_stopwords_list_per_index",
            .success = true, 
            .command_str = "idx1 on HASH STOPWORDS 10 a an and are as at be but by for SCHEMA text_field TEXT",
            .text_parameters = {{
                .with_suffix_trie = false,
                .no_stem = false,
                .min_stem_size = 4,
            }},
            .expected = {
                .index_schema_name = "idx1",
                .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                .attributes = {{
                    .identifier = "text_field",
                    .attribute_alias = "text_field",
                    .indexer_type = indexes::IndexerType::kText,
                }},
                .per_index_text_params = {
                    .punctuation = ",.<>{}[]\"':;!@#$%^&*()-+=~/\\|",
                    .stop_words = {"a", "an", "and", "are", "as", "at", "be", "but", "by", "for"},
                    .language = data_model::Language::LANGUAGE_ENGLISH,
                    .with_offsets = true,
                }
            },
        },
         {
             .test_name = "text_max_minstemsize",
             .success = true,
             .command_str = "idx1 on HASH SCHEMA text_field TEXT MINSTEMSIZE 100",
             .text_parameters = {{
                 .with_suffix_trie = false,
                 .no_stem = false,
                 .min_stem_size = 100,
             }},
             .expected = {
                 .index_schema_name = "idx1",
                 .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                 .attributes = {{
                     .identifier = "text_field",
                     .attribute_alias = "text_field",
                     .indexer_type = indexes::IndexerType::kText,
                 }}
             },
         },
         {
             .test_name = "text_field_special_characters_punctuation_invalid",
             .success = false,
             .command_str = "idx1 on HASH SCHEMA text_field TEXT PUNCTUATION \"!@#$%^&*()_+-=[]{}|;':,.<>?\"",
             .text_parameters = {},
             .expected = {},
             .expected_error_message = "Invalid field type for field `PUNCTUATION`: Unknown argument `!@#$%^&*()_+-=[]{}|;':,.<>?`",
         },
         {
            .test_name = "text_special_characters_punctuation_per_index",
            .success = true,
            .command_str = "idx1 on HASH PUNCTUATION \"!@#$%^&*()_+-=[]{}|;':,.<>?\" SCHEMA text_field TEXT",
            .text_parameters = {{
                .with_suffix_trie = false,
                .no_stem = false,
                .min_stem_size = 4,
            }},
            .expected = {
                .index_schema_name = "idx1",
                .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                .attributes = {{
                    .identifier = "text_field",
                    .attribute_alias = "text_field",
                    .indexer_type = indexes::IndexerType::kText,
                }},
                .per_index_text_params = {
                    .punctuation = "!@#$%^&*()_+-=[]{}|;':,.<>?",
                    .stop_words = {kDefStopWords},
                    .language = data_model::Language::LANGUAGE_ENGLISH,
                    .with_offsets = true,
                }
            },
        },
         {
             .test_name = "text_multiple_fields_different_configs",
             .success = true,
             .command_str = "idx1 on HASH NOSTOPWORDS PUNCTUATION '.,;' SCHEMA text1 TEXT text2 TEXT MINSTEMSIZE 2",
             .text_parameters = {
                 {
                     .with_suffix_trie = false,
                     .no_stem = false,
                     .min_stem_size = 4,
                 },
                 {
                     .with_suffix_trie = false,
                     .no_stem = false,
                     .min_stem_size = 2,
                 }
             },
             .expected = {
                 .index_schema_name = "idx1",
                 .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                 .attributes = {
                     {
                         .identifier = "text1",
                         .attribute_alias = "text1",
                         .indexer_type = indexes::IndexerType::kText,
                     },
                     {
                         .identifier = "text2",
                         .attribute_alias = "text2",
                         .indexer_type = indexes::IndexerType::kText,
                     }
                 },
                 .per_index_text_params = {
                     .punctuation = ".,;",
                     .stop_words = {},  // Empty due to NOSTOPWORDS
                     .language = data_model::Language::LANGUAGE_ENGLISH,
                     .with_offsets = true,
                 }
             },
         },
         // Error cases for TEXT fields
         {
            .test_name = "invalid_text_single_quote_empty_per_index",
            .success = false,
            .command_str = "idx1 on HASH PUNCTUATION '' SCHEMA text_field TEXT",  // Moved PUNCTUATION before SCHEMA
            .expected_error_message = "PUNCTUATION string cannot be empty",  // Simplified error message
        },
         {
            .test_name = "invalid_text_stopwords_negative_count_per_index",
            .success = false,
            .command_str = "idx1 on HASH STOPWORDS -1 SCHEMA text_field TEXT",  // Moved STOPWORDS before SCHEMA
            .expected_error_message = "Error parsing value for the parameter `STOPWORDS` - `-1` is outside acceptable bounds",  // Simplified error message
        },
         {
             .test_name = "invalid_text_stopwords_missing_words_field",
             .success = false,
             .command_str = "idx1 on HASH SCHEMA text_field TEXT STOPWORDS 3 the and",
             .expected_error_message = "Invalid field type for field `STOPWORDS`: Unknown argument `3`",
         },
         {
            .test_name = "invalid_text_stopwords_missing_words_per_index",
            .success = false,
            .command_str = "idx1 on HASH STOPWORDS 3 the and SCHEMA text_field TEXT",
            .expected_error_message = "Unexpected parameter `text_field`, expecting `SCHEMA`",
        },
        {
            .test_name = "invalid_text_field_parameters_per_index",
            .success = false,
            .command_str = "idx1 on HASH WITHSUFFIXTRIE MINSTEMSIZE 2 SCHEMA text_field TEXT",  // Moved parameters before SCHEMA
            .expected_error_message = "Unexpected parameter `WITHSUFFIXTRIE`, expecting `SCHEMA`",  // Error for unsupported global parameter
        },
         {
             .test_name = "valid_text_minstemsize_too_large",
             .success = true,  // Should succeed as there's no upper limit defined
             .command_str = "idx1 on HASH SCHEMA text_field TEXT MINSTEMSIZE 999999",
             .text_parameters = {{
                 .with_suffix_trie = false,
                 .no_stem = false,
                 .min_stem_size = 999999,
             }},
             .expected = {
                 .index_schema_name = "idx1",
                 .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                 .attributes = {{
                     .identifier = "text_field",
                     .attribute_alias = "text_field",
                     .indexer_type = indexes::IndexerType::kText,
                 }}
             },
         },
         {
             .test_name = "invalid_text_unknown_parameter",
             .success = false,
             .command_str = "idx1 on HASH SCHEMA text_field TEXT UNKNOWN_PARAM value",
             .expected_error_message = "Invalid field type for field `UNKNOWN_PARAM`: Unknown argument `value`",
         },
         {
             .test_name = "text_case_insensitive_parameters",
             .success = true,
             .command_str = "idx1 on HASH punctuation '.,;' withoffsets nostem SCHEMA text_field text",
             .text_parameters = {{
                 .with_suffix_trie = false,
                 .no_stem = true,
                 .min_stem_size = 4,
             }},
             .expected = {
                 .index_schema_name = "idx1",
                 .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                 .attributes = {{
                     .identifier = "text_field",
                     .attribute_alias = "text_field",
                     .indexer_type = indexes::IndexerType::kText,
                 }},
                 .per_index_text_params = {
                     .punctuation = ".,;",
                     .stop_words = {kDefStopWords},
                     .language = data_model::Language::LANGUAGE_ENGLISH,
                     .with_offsets = true,
                 }
             },
         },
         {
             .test_name = "text_per_index_and_field_parameters_mixed",
             .success = true,
             .command_str = "idx1 on HASH LANGUAGE english PUNCTUATION '.,;' SCHEMA text_field TEXT WITHSUFFIXTRIE",
             .text_parameters = {{
                 .with_suffix_trie = true,
                 .no_stem = false,
                 .min_stem_size = 4,
             }},
             .expected = {
                 .index_schema_name = "idx1",
                 .on_data_type = data_model::ATTRIBUTE_DATA_TYPE_HASH,
                 .attributes = {{
                     .identifier = "text_field",
                     .attribute_alias = "text_field",
                     .indexer_type = indexes::IndexerType::kText,
                 }},
                 .per_index_text_params = {
                     .punctuation = ".,;",
                     .stop_words = {kDefStopWords},
                     .language = data_model::Language::LANGUAGE_ENGLISH,
                     .with_offsets = true,
                 }
             },
         }}),
    [](const TestParamInfo<FTCreateParserTestCase> &info) {
      return info.param.test_name;
    });

}  // namespace

}  // namespace valkey_search
