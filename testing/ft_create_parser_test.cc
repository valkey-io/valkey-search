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

struct FTCreateParameters {
  absl::string_view index_schema_name;
  data_model::AttributeDataType on_data_type{
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH};
  std::vector<absl::string_view> prefixes;
  float score{1.0};
  absl::string_view score_field;
  absl::string_view payload_field;
  std::vector<AttributeParameters> attributes;
};

struct FTCreateParserTestCase {
  std::string test_name;
  bool success{false};
  absl::string_view command_str;
  bool too_many_attributes{false};
  std::vector<HNSWParameters> hnsw_parameters;
  std::vector<FlatParameters> flat_parameters;
  std::vector<FTCreateTagParameters> tag_parameters;
  FTCreateParameters expected;

  std::string expected_error_message;
};

class FTCreateParserTest
    : public vmsdk::RedisTestWithParam<FTCreateParserTestCase> {};

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
  auto args = vmsdk::ToRedisStringVector(command_str);
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
    auto hnsw_index = 0;
    auto flat_index = 0;
    auto tag_index = 0;
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
    TestRedisModule_FreeString(nullptr, arg);
  }
}

INSTANTIATE_TEST_SUITE_P(
    FTCreateParserTests, FTCreateParserTest,
    ValuesIn<FTCreateParserTestCase>(
        {{
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
             .test_name = "missing_vector_index",
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
                 "Invalid field type for field `hash_field1`: EF_RUNTIME must "
                 "be a positive integer greater than 0 and cannot exceed 4096.",
         },
         {
             .test_name = "invalid_m_negative",
             .success = false,
             .command_str = "idx1 SChema hash_field1 as "
                            "hash_field11 vector hnsw 8 TYPE  FLOAT32 DIM 3 "
                            "DISTANCE_METRIC IP M -10",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: M must be a "
                 "positive integer greater than 0 and cannot exceed 2000000.",
         },
         {
             .test_name = "invalid_m_too_big",
             .success = false,
             .command_str = "idx1 SChema hash_field1 as "
                            "hash_field11 vector hnsw 8 TYPE  FLOAT32 DIM 3 "
                            "DISTANCE_METRIC IP M 3000000",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: M must be a "
                 "positive integer greater than 0 and cannot exceed 2000000.",
         },
         {
             .test_name = "invalid_ef_construction_zero",
             .success = false,
             .command_str = "idx1 SChema hash_field1 as "
                            "hash_field11 vector hnsw 8 TYPE  FLOAT32 DIM 3 "
                            "DISTANCE_METRIC IP EF_CONSTRUCTIOn 0",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: EF_CONSTRUCTION "
                 "must "
                 "be a positive integer greater than 0 and cannot exceed 4096.",
         },
         {
             .test_name = "invalid_ef_construction_negative",
             .success = false,
             .command_str = "idx1 SChema hash_field1 as "
                            "hash_field11 vector hnsw 8 TYPE  FLOAT32 DIM 3 "
                            "DISTANCE_METRIC IP EF_CONSTRUCTIOn -100",
             .expected_error_message =
                 "Invalid field type for field `hash_field1`: EF_CONSTRUCTION "
                 "must be a positive integer greater than 0 and cannot exceed "
                 "4096.",
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
                 "The maximum number of attributes cannot exceed 50.",
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
         }}),
    [](const TestParamInfo<FTCreateParserTestCase> &info) {
      return info.param.test_name;
    });

}  // namespace

}  // namespace valkey_search
