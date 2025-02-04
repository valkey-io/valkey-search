/*
 * Copyright (c) 2025, ValkeySearch contributors
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

#include "src/query/response_generator.h"

#include <cstddef>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/attribute_data_type.h"
#include "src/indexes/vector_base.h"
#include "src/query/predicate.h"
#include "src/query/search.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

using testing::TestParamInfo;
using testing::ValuesIn;

class MockPredicate : public query::Predicate {
 public:
  explicit MockPredicate(query::PredicateType type) : query::Predicate(type) {}
  MOCK_METHOD(bool, Evaluate, (query::Evaluator & evaluator),
              (override, const));
};

struct ResponseGeneratorTestCase {
  std::string test_name;
  data_model::AttributeDataType data_type;
  std::deque<std::string> external_id_neighbors;
  std::vector<TestReturnAttribute> return_attributes;
  absl::flat_hash_set<std::string> filter_identifiers;
  int filter_evaluate_not_match_index{-1};
  std::unordered_map<std::string, std::string> returned_records_map;
  absl::flat_hash_set<std::string> missing_keys;
  absl::flat_hash_set<std::string> expected_fetched_identifiers;
  std::vector<std::string> expected_neighbors;
  std::vector<std::unordered_map<std::string, std::string>> expected_contents;
};

class ResponseGeneratorTest
    : public ValkeySearchTestWithParam<ResponseGeneratorTestCase> {};

RecordsMap ToRecordsMap(
    const std::unordered_map<std::string, std::string> &record_map) {
  RecordsMap records_map;
  for (const auto &[key, value] : record_map) {
    records_map.emplace(key,
                        RecordsMapValue(vmsdk::MakeUniqueRedisString(key),
                                        vmsdk::MakeUniqueRedisString(value)));
  }
  return records_map;
}

TEST_P(ResponseGeneratorTest, ProcessNeighborsForReply) {
  auto &params = GetParam();
  RedisModuleCtx fake_ctx;

  std::deque<indexes::Neighbor> expected_neighbors;
  for (const auto &external_id : params.external_id_neighbors) {
    auto string_interned_external_id = StringInternStore::Intern(external_id);
    expected_neighbors.push_back(
        indexes::Neighbor(string_interned_external_id, 0));
  }
  std::vector<RecordsMap> expected_contents;
  for (const auto &expected_content : params.expected_contents) {
    expected_contents.push_back(ToRecordsMap(expected_content));
  }
  query::VectorSearchParameters parameters;
  for (const auto &return_attribute : params.return_attributes) {
    parameters.return_attributes.push_back(
        {.identifier =
             vmsdk::MakeUniqueRedisString(return_attribute.identifier),
         .alias = vmsdk::MakeUniqueRedisString(return_attribute.alias)});
  }
  parameters.filter_parse_results.filter_identifiers =
      params.filter_identifiers;
  int filter_evaluate_cnt = -1;
  auto predicate =
      std::make_unique<MockPredicate>(query::PredicateType::kNumeric);
  EXPECT_CALL(*predicate, Evaluate(testing::_))
      .WillRepeatedly([&params, &filter_evaluate_cnt](
                          [[maybe_unused]] query::Evaluator &evaluator) {
        if (params.filter_evaluate_not_match_index == -1) {
          return true;
        }
        ++filter_evaluate_cnt;
        return (filter_evaluate_cnt != params.filter_evaluate_not_match_index);
      });

  parameters.filter_parse_results.root_predicate = std::move(predicate);
  parameters.attribute_alias = "some_addribute_name";

  MockAttributeDataType data_type;
  EXPECT_CALL(data_type, ToProto()).WillRepeatedly([&params]() {
    return params.data_type;
  });
  absl::flat_hash_set<absl::string_view> expected_fetched_identifiers;
  for (const auto &id : params.expected_fetched_identifiers) {
    expected_fetched_identifiers.insert(id);
  }
  for (const auto &neighbor : expected_neighbors) {
    EXPECT_CALL(data_type,
                FetchAllRecords(&fake_ctx, parameters.attribute_alias,
                                absl::string_view(*neighbor.external_id),
                                expected_fetched_identifiers))
        .WillOnce([&params](
                      RedisModuleCtx *ctx,
                      const std::string &query_attribute_alias,
                      absl::string_view key,
                      const absl::flat_hash_set<absl::string_view> &identifiers)
                      -> absl::StatusOr<RecordsMap> {
          if (params.missing_keys.contains(key)) {
            return absl::NotFoundError("not found");
          }
          auto returned_records_map = ToRecordsMap(params.returned_records_map);
          return std::move(returned_records_map);
        });
  }
  ProcessNeighborsForReply(&fake_ctx, data_type, expected_neighbors, parameters,
                           parameters.attribute_alias);
  EXPECT_EQ(expected_neighbors.size(), params.expected_neighbors.size());
  for (size_t i = 0; i < params.expected_neighbors.size(); ++i) {
    EXPECT_EQ(std::string(*expected_neighbors[i].external_id),
              params.expected_neighbors[i]);
    EXPECT_EQ(ToStringMap(expected_neighbors[i].attribute_contents.value()),
              ToStringMap(expected_contents[i]));
  }
}

INSTANTIATE_TEST_SUITE_P(
    ResponseGeneratorTests, ResponseGeneratorTest,
    ValuesIn<ResponseGeneratorTestCase>(
        {{
             .test_name = "json_with_filter_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = -1,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id1",
                     "external_id2",
                 },
             .expected_contents = {{{"id1", "id1_value"}},
                                   {{"id1", "id1_value"}}},
         },

         {
             .test_name =
                 "json_with_filter_with_return_missing_key_external_id1",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = -1,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {"external_id1"},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id2",
                 },
             .expected_contents =
                 {
                     {{"id1", "id1_value"}},
                 },
         },
         {
             .test_name = "json_filter_not_match_first_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = 0,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id2",
                 },
             .expected_contents = {{{"id1", "id1_value"}}},
         },
         {
             .test_name = "json_filter_not_match_second_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = 1,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id1",
                 },
             .expected_contents = {{{"id1", "id1_value"}}},
         },
         {
             .test_name = "json_no_filter_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .returned_records_map =
                 {
                     {"id1", "id1_value"},
                 },
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1"},
             .expected_neighbors =
                 {
                     "external_id1",
                     "external_id2",
                 },
             .expected_contents = {{{"id1", "id1_value"}},
                                   {{"id1", "id1_value"}}},
         },
         {
             .test_name = "json_no_filter_no_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .returned_records_map =
                 {
                     {std::string(kJsonRootElementQuery), "id1_value"},
                 },
             .missing_keys = {},
             .expected_fetched_identifiers = {std::string(
                 kJsonRootElementQuery)},
             .expected_neighbors =
                 {
                     "external_id1",
                     "external_id2",
                 },
             .expected_contents =
                 {{{std::string(kJsonRootElementQuery), "id1_value"}},
                  {{std::string(kJsonRootElementQuery), "id1_value"}}},
         },
         {
             .test_name = "json_with_filter_with_no_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = -1,
             .returned_records_map = {{std::string(kJsonRootElementQuery),
                                       "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers =
                 {std::string(kJsonRootElementQuery), "id2"},
             .expected_neighbors =
                 {
                     "external_id1",
                     "external_id2",
                 },
             .expected_contents =
                 {{{std::string(kJsonRootElementQuery), "id1_value"}},
                  {{std::string(kJsonRootElementQuery), "id1_value"}}},
         },
         {
             .test_name = "hash_with_filter_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = -1,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id1",
                     "external_id2",
                 },
             .expected_contents = {{{"id1", "id1_value"}},
                                   {{"id1", "id1_value"}}},
         },
         {
             .test_name =
                 "hash_with_filter_with_return_missing_key_external_id1",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = -1,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {"external_id2"},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id1",
                 },
             .expected_contents =
                 {
                     {{"id1", "id1_value"}},
                 },
         },
         {
             .test_name = "hash_filter_not_match_first_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = 0,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id2",
                 },
             .expected_contents = {{{"id1", "id1_value"}}},
         },
         {
             .test_name = "hash_filter_not_match_second_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = 1,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1", "id2"},
             .expected_neighbors =
                 {
                     "external_id1",
                 },
             .expected_contents = {{{"id1", "id1_value"}}},
         },
         {
             .test_name = "hash_no_filter_with_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .return_attributes = {{.identifier = "id1", .alias = "id1_alias"}},
             .returned_records_map =
                 {
                     {"id1", "id1_value"},
                 },
             .missing_keys = {},
             .expected_fetched_identifiers = {"id1"},
             .expected_neighbors =
                 {
                     "external_id1",
                     "external_id2",
                 },
             .expected_contents = {{{"id1", "id1_value"}},
                                   {{"id1", "id1_value"}}},
         },
         {
             .test_name = "hash_with_filter_with_no_return",
             .data_type =
                 data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH,
             .external_id_neighbors = {"external_id1", "external_id2"},
             .filter_identifiers = {"id2"},
             .filter_evaluate_not_match_index = -1,
             .returned_records_map = {{"id1", "id1_value"},
                                      {"id2", "id2_value"}},
             .missing_keys = {},
             .expected_fetched_identifiers = {},
             .expected_neighbors =
                 {
                     "external_id1",
                     "external_id2",
                 },
             .expected_contents = {{{"id1", "id1_value"}, {"id2", "id2_value"}},
                                   {{"id1", "id1_value"},
                                    {"id2", "id2_value"}}},
         }}),
    [](const TestParamInfo<ResponseGeneratorTestCase> &info) {
      return info.param.test_name;
    });

}  // namespace

}  // namespace valkey_search
