/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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
#include "src/commands/filter_parser.h"
#include "src/indexes/text.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_flat.h"
#include "src/metrics.h"
#include "src/query/predicate.h"
#include "src/query/search.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/time_sliced_mrmw_mutex.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

using testing::TestParamInfo;
using testing::ValuesIn;

class MockPredicate : public query::Predicate {
 public:
  explicit MockPredicate(query::PredicateType type) : query::Predicate(type) {}
  MOCK_METHOD(query::EvaluationResult, Evaluate, (query::Evaluator & evaluator),
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
                        RecordsMapValue(vmsdk::MakeUniqueValkeyString(key),
                                        vmsdk::MakeUniqueValkeyString(value)));
  }
  return records_map;
}

TEST_P(ResponseGeneratorTest, ProcessNeighborsForReply) {
  auto &params = GetParam();
  ValkeyModuleCtx fake_ctx;

  EXPECT_CALL(*kMockValkeyModule, GetExpire(testing::_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_NO_EXPIRE));

  std::vector<indexes::Neighbor> expected_neighbors;
  for (const auto &external_id : params.external_id_neighbors) {
    auto string_interned_external_id = StringInternStore::Intern(external_id);
    expected_neighbors.push_back(
        indexes::Neighbor(string_interned_external_id, 0));
    expected_neighbors.back().sequence_number = 0;
  }
  std::vector<RecordsMap> expected_contents;
  expected_contents.reserve(params.expected_contents.size());
  for (const auto &expected_content : params.expected_contents) {
    expected_contents.push_back(ToRecordsMap(expected_content));
  }
  UnitTestSearchParameters parameters;
  parameters.index_schema = CreateIndexSchema("index").value();
  for (const auto &n : expected_neighbors) {
    parameters.index_schema->SetIndexMutationSequenceNumber(n.external_id,
                                                            n.sequence_number);
    parameters.index_schema->SetDbMutationSequenceNumber(
        n.external_id,
        n.sequence_number + 1);  // + 1 forces call to filter.
  }

  for (const auto &return_attribute : params.return_attributes) {
    parameters.return_attributes.push_back(
        {.identifier =
             vmsdk::MakeUniqueValkeyString(return_attribute.identifier),
         .alias = vmsdk::MakeUniqueValkeyString(return_attribute.alias)});
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
          return query::EvaluationResult(true);
        }
        ++filter_evaluate_cnt;
        return query::EvaluationResult(filter_evaluate_cnt !=
                                       params.filter_evaluate_not_match_index);
      });

  parameters.filter_parse_results.root_predicate = std::move(predicate);
  parameters.attribute_alias = "some_attribute_name";

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
                FetchAllAttributes(
                    &fake_ctx, std::make_optional(parameters.attribute_alias),
                    testing::_, absl::string_view(*neighbor.external_id),
                    expected_fetched_identifiers))
        .WillOnce([&params](
                      ValkeyModuleCtx *ctx,
                      const std::optional<std::string> &query_attribute_alias,
                      ValkeyModuleKey *open_key, absl::string_view key,
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

TEST_F(ResponseGeneratorTest, ProcessNeighborsForReplyContentLimits) {
  ValkeyModuleCtx fake_ctx;

  EXPECT_CALL(*kMockValkeyModule, GetExpire(testing::_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_NO_EXPIRE));

  // Set up a small content size limit for testing
  const size_t test_size_limit = 100;
  VMSDK_EXPECT_OK(
      options::GetMaxSearchResultRecordSize().SetValue(test_size_limit));

  // Set up a small content fields limit for testing
  const size_t test_fields_limit = 2;
  VMSDK_EXPECT_OK(
      options::GetMaxSearchResultFieldsCount().SetValue(test_fields_limit));

  // Create neighbors with different content sizes and field counts
  std::vector<indexes::Neighbor> neighbors;
  auto small_external_id = StringInternStore::Intern("small_content_id");
  auto large_external_id = StringInternStore::Intern("large_content_id");
  auto many_fields_id = StringInternStore::Intern("many_fields_id");

  neighbors.push_back(indexes::Neighbor(small_external_id, 0));
  neighbors.push_back(indexes::Neighbor(large_external_id, 0));
  neighbors.push_back(indexes::Neighbor(many_fields_id, 0));

  // Set up parameters
  UnitTestSearchParameters parameters;
  parameters.return_attributes.push_back(
      {.identifier = vmsdk::MakeUniqueValkeyString("content"),
       .alias = vmsdk::MakeUniqueValkeyString("content_alias")});
  parameters.return_attributes.push_back(
      {.identifier = vmsdk::MakeUniqueValkeyString("field1"),
       .alias = vmsdk::MakeUniqueValkeyString("field1_alias")});
  parameters.return_attributes.push_back(
      {.identifier = vmsdk::MakeUniqueValkeyString("field2"),
       .alias = vmsdk::MakeUniqueValkeyString("field2_alias")});
  parameters.attribute_alias = "test_attribute";

  // Mock data type
  MockAttributeDataType data_type;
  EXPECT_CALL(data_type, ToProto()).WillRepeatedly([]() {
    return data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH;
  });

  // Mock FetchAllAttributes to return different sized content
  EXPECT_CALL(
      data_type,
      FetchAllAttributes(
          &fake_ctx, std::make_optional(parameters.attribute_alias), testing::_,
          absl::string_view("small_content_id"), testing::_))
      .WillOnce([](ValkeyModuleCtx *ctx,
                   const std::optional<std::string> &query_attribute_alias,
                   ValkeyModuleKey *open_key, absl::string_view key,
                   const absl::flat_hash_set<absl::string_view> &identifiers)
                    -> absl::StatusOr<RecordsMap> {
        // Return small content (within both size and field limits)
        RecordsMap small_content;
        small_content.emplace(
            "content", RecordsMapValue(vmsdk::MakeUniqueValkeyString("content"),
                                       vmsdk::MakeUniqueValkeyString("small")));
        small_content.emplace(
            "field1", RecordsMapValue(vmsdk::MakeUniqueValkeyString("field1"),
                                      vmsdk::MakeUniqueValkeyString("value1")));
        return small_content;
      });

  EXPECT_CALL(
      data_type,
      FetchAllAttributes(
          &fake_ctx, std::make_optional(parameters.attribute_alias), testing::_,
          absl::string_view("large_content_id"), testing::_))
      .WillOnce([test_size_limit](
                    ValkeyModuleCtx *ctx,
                    const std::optional<std::string> &query_attribute_alias,
                    ValkeyModuleKey *open_key, absl::string_view key,
                    const absl::flat_hash_set<absl::string_view> &identifiers)
                    -> absl::StatusOr<RecordsMap> {
        // Return large content (exceeds size limit)
        RecordsMap large_content;
        std::string large_value(test_size_limit + 10,
                                'x');  // Exceed the size limit by 10 bytes
        large_content.emplace(
            "content",
            RecordsMapValue(vmsdk::MakeUniqueValkeyString("content"),
                            vmsdk::MakeUniqueValkeyString(large_value)));
        return large_content;
      });

  EXPECT_CALL(data_type,
              FetchAllAttributes(
                  &fake_ctx, std::make_optional(parameters.attribute_alias),
                  testing::_, absl::string_view("many_fields_id"), testing::_))
      .WillOnce([](ValkeyModuleCtx *ctx,
                   const std::optional<std::string> &query_attribute_alias,
                   ValkeyModuleKey *open_key, absl::string_view key,
                   const absl::flat_hash_set<absl::string_view> &identifiers)
                    -> absl::StatusOr<RecordsMap> {
        // Return content with many fields (exceeds field count limit)
        RecordsMap many_fields_content;
        many_fields_content.emplace(
            "content", RecordsMapValue(vmsdk::MakeUniqueValkeyString("content"),
                                       vmsdk::MakeUniqueValkeyString("data")));
        many_fields_content.emplace(
            "field1", RecordsMapValue(vmsdk::MakeUniqueValkeyString("field1"),
                                      vmsdk::MakeUniqueValkeyString("value1")));
        many_fields_content.emplace(
            "field2", RecordsMapValue(vmsdk::MakeUniqueValkeyString("field2"),
                                      vmsdk::MakeUniqueValkeyString("value2")));
        return many_fields_content;
      });

  ProcessNeighborsForReply(&fake_ctx, data_type, neighbors, parameters,
                           parameters.attribute_alias);

  // Verify that only the neighbor with small content remains
  // (both large content and many fields neighbors should be filtered out)
  EXPECT_EQ(neighbors.size(), 1);
  EXPECT_EQ(std::string(*neighbors[0].external_id), "small_content_id");
  EXPECT_TRUE(neighbors[0].attribute_contents.has_value());

  // Verify the content is correct
  auto content_map = ToStringMap(neighbors[0].attribute_contents.value());
  EXPECT_EQ(content_map["content"], "small");
  EXPECT_EQ(content_map["field1"], "value1");
  EXPECT_EQ(content_map.size(), 2);

  // Verify that the metric was incremented correctly
  // Should be incremented by 2: once for large content, once for many fields
  EXPECT_EQ(Metrics::GetStats().query_result_record_dropped_cnt, 2);
}

// --- Score recompute on the main-thread content-fetch path ---
//
// When a document mutates between shard-side scoring and content fetch
// (db_seq != sequence_number), VerifyFilter recomputes its relevance score
// through the same Scorer seam ScoreTextQuery uses (search.cc
// SingleDocumentScorer) and ProcessNeighborsForReply writes it to
// Neighbor.score, then re-ranks the survivors. These tests exercise that
// wiring end-to-end with weight leaves; the per-leaf scoring math (including
// text via Scorer::ScoreLeaf and AND/OR composition) is covered by
// ScoreNodeTest in search_test.cc, and SingleDocumentScorer reuses that exact
// ScoreNode walk, so a matched leaf recomputes to the same value here.

namespace {
// Builds a single-neighbor scenario, runs ProcessNeighborsForReply, and returns
// the surviving neighbors. `mutated` toggles the db/sequence mismatch that
// drives VerifyFilter's recompute walk.
void RunSingleNeighborRecompute(
    ValkeyModuleCtx *fake_ctx, UnitTestSearchParameters &parameters,
    MockAttributeDataType &data_type, absl::string_view key, bool mutated,
    query::PredicateType leaf_type, float weight, float initial_neighbor_score,
    const std::optional<std::string> &vector_identifier,
    std::vector<indexes::Neighbor> &neighbors) {
  parameters.index_schema = CreateIndexSchema("index").value();
  parameters.filter_parse_results.filter_identifiers = {"id2"};

  auto predicate = std::make_unique<MockPredicate>(leaf_type);
  predicate->SetWeight(weight);
  EXPECT_CALL(*predicate, Evaluate(testing::_))
      .WillRepeatedly([]([[maybe_unused]] query::Evaluator &evaluator) {
        return query::EvaluationResult(true);
      });
  parameters.filter_parse_results.root_predicate = std::move(predicate);

  auto id = StringInternStore::Intern(std::string(key));
  neighbors.push_back(indexes::Neighbor(id, initial_neighbor_score));
  neighbors.back().sequence_number = 0;
  // index_key_info_ must contain the key so GetIndexKeyInfoSize() (the corpus
  // size SingleDocumentScorer sources) is non-zero, matching ScoreTextQuery.
  parameters.index_schema->SetIndexMutationSequenceNumber(id, 0);
  parameters.index_schema->SetDbMutationSequenceNumber(id, mutated ? 1 : 0);

  EXPECT_CALL(data_type, ToProto())
      .WillRepeatedly(testing::Return(
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH));
  EXPECT_CALL(data_type, FetchAllAttributes(fake_ctx, testing::_, testing::_,
                                            absl::string_view(key), testing::_))
      .WillOnce([](ValkeyModuleCtx *, const std::optional<std::string> &,
                   ValkeyModuleKey *, absl::string_view,
                   const absl::flat_hash_set<absl::string_view> &)
                    -> absl::StatusOr<RecordsMap> {
        RecordsMap m;
        m.emplace("id2", RecordsMapValue(vmsdk::MakeUniqueValkeyString("id2"),
                                         vmsdk::MakeUniqueValkeyString("v")));
        return m;
      });

  ProcessNeighborsForReply(fake_ctx, data_type, neighbors, parameters,
                           vector_identifier);
}
}  // namespace

// A neighbor that did NOT mutate (db_seq == sequence_number) keeps its carried
// score untouched — VerifyFilter returns on the fast path with no recomputed
// score. (The magnitude of a recomputed numeric/tag score is covered by the
// query-level TagScoreTest in search_test.cc, which uses real indexes.)
TEST_F(ResponseGeneratorTest, NoRecomputeWhenNeighborNotMutated) {
  ValkeyModuleCtx fake_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetExpire(testing::_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_NO_EXPIRE));

  UnitTestSearchParameters parameters;
  MockAttributeDataType data_type;
  std::vector<indexes::Neighbor> neighbors;
  RunSingleNeighborRecompute(&fake_ctx, parameters, data_type, "k1",
                             /*mutated=*/false, query::PredicateType::kNumeric,
                             /*weight=*/3.0f, /*initial_neighbor_score=*/7.0f,
                             /*vector_identifier=*/std::nullopt, neighbors);

  ASSERT_EQ(neighbors.size(), 1);
  // Untouched: still the shard-side score, not the numeric weight (3.0).
  EXPECT_FLOAT_EQ(neighbors[0].score, 7.0f);
}

// A vector query's Neighbor.score is a KNN distance, so the Scorer must never
// be turned loose on it: the relevance recompute is for non-vector queries
// only. Here the schema carries no index under the alias, so the distance
// cannot be recomputed either and the carried one has to survive untouched.
// The case where it CAN be recomputed is below.
TEST_F(ResponseGeneratorTest, VectorQueryIsNeverRelevanceRescored) {
  ValkeyModuleCtx fake_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetExpire(testing::_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_NO_EXPIRE));

  UnitTestSearchParameters parameters;
  parameters.attribute_alias = "vec";  // -> vector query
  MockAttributeDataType data_type;
  std::vector<indexes::Neighbor> neighbors;
  RunSingleNeighborRecompute(
      &fake_ctx, parameters, data_type, "k1",
      /*mutated=*/true, query::PredicateType::kNumeric, /*weight=*/3.0f,
      /*initial_neighbor_score=*/0.5f,
      /*vector_identifier=*/std::make_optional<std::string>("vec"), neighbors);

  ASSERT_EQ(neighbors.size(), 1);
  // The KNN distance-as-score is preserved, not recomputed.
  EXPECT_FLOAT_EQ(neighbors[0].score, 0.5f);
}

// The other half: with the vector index reachable, a mutated document's
// distance IS refreshed against the vector it now holds, and the reply is put
// back in order. The arithmetic across every storage type and metric is
// covered by RecomputeDistanceTest in vector_test.cc; this is about the
// plumbing -- that the reply path notices the mutation, finds the bytes, and
// re-sorts on the result.
TEST_F(ResponseGeneratorTest, VectorQueryDistanceRecomputedAndReordered) {
  ValkeyModuleCtx fake_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetExpire(testing::_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_NO_EXPIRE));

  auto floats = [](std::vector<float> v) {
    return std::string(reinterpret_cast<const char *>(v.data()),
                       v.size() * sizeof(float));
  };
  // The origin, so a distance is the squared length of the stored vector.
  const std::string query = floats({0.0f, 0.0f, 0.0f, 0.0f});
  // k1 is rewritten to sit far away; k2 is not touched.
  const std::string k1_now = floats({9.0f, 0.0f, 0.0f, 0.0f});
  const std::string k2_now = floats({2.0f, 0.0f, 0.0f, 0.0f});

  UnitTestSearchParameters parameters;
  parameters.index_schema = CreateIndexSchema("index").value();
  parameters.attribute_alias = "vec";
  parameters.query = query;
  auto vector_index = indexes::VectorFlat<float>::Create(
      CreateFlatVectorIndexProto(4, data_model::DISTANCE_METRIC_L2, 10, 10),
      "vec", data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0);
  ASSERT_TRUE(vector_index.ok());
  VMSDK_EXPECT_OK(
      parameters.index_schema->AddIndex("vec", "vec", vector_index.value()));

  // k1 carries the distance its old vector earned, which put it first.
  std::vector<indexes::Neighbor> neighbors;
  auto k1 = StringInternStore::Intern("k1");
  auto k2 = StringInternStore::Intern("k2");
  neighbors.push_back(indexes::Neighbor(k1, 1.0f));
  neighbors.push_back(indexes::Neighbor(k2, 4.0f));
  for (auto &n : neighbors) {
    n.sequence_number = 0;
    parameters.index_schema->SetIndexMutationSequenceNumber(n.external_id, 0);
  }
  // Only k1 was rewritten.
  parameters.index_schema->SetDbMutationSequenceNumber(k1, 1);
  parameters.index_schema->SetDbMutationSequenceNumber(k2, 0);

  MockAttributeDataType data_type;
  EXPECT_CALL(data_type, ToProto())
      .WillRepeatedly(testing::Return(
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH));
  EXPECT_CALL(data_type, FetchAllAttributes(&fake_ctx, testing::_, testing::_,
                                            testing::_, testing::_))
      .WillRepeatedly([&k1_now, &k2_now](
                          ValkeyModuleCtx *, const std::optional<std::string> &,
                          ValkeyModuleKey *, absl::string_view key,
                          const absl::flat_hash_set<absl::string_view> &)
                          -> absl::StatusOr<RecordsMap> {
        RecordsMap m;
        m.emplace("vec", RecordsMapValue(vmsdk::MakeUniqueValkeyString("vec"),
                                         vmsdk::MakeUniqueValkeyString(
                                             key == "k1" ? k1_now : k2_now)));
        return m;
      });

  ProcessNeighborsForReply(&fake_ctx, data_type, neighbors, parameters,
                           std::make_optional<std::string>("vec"));

  ASSERT_EQ(neighbors.size(), 2);
  // k1 now sits at 81 and therefore last; k2 keeps the 4 it came in with.
  EXPECT_EQ(neighbors[0].external_id->Str(), "k2");
  EXPECT_FLOAT_EQ(neighbors[0].distance, 4.0f);
  EXPECT_EQ(neighbors[1].external_id->Str(), "k1");
  EXPECT_FLOAT_EQ(neighbors[1].distance, 81.0f);
  // A query ranked on its distance reports the same number as its score.
  EXPECT_FLOAT_EQ(neighbors[1].score, 81.0f);
}

// A VSIM arm carrying a text FILTER. The FT.HYBRID parser rewrites it into
// `(<filter>)=>[KNN ...]`, so on the wire it carries a text predicate and is
// shaped exactly like a `text=>[KNN ...]` hybrid query -- but its filter only
// decides membership, so Neighbor::score stays the KNN distance. That is what
// SearchParameters::vector_score_only says, and in a cluster it only reaches
// the shard because coordinator.proto now serializes it.
//
// Without the flag consulted here, the refreshed distance lands in
// Neighbor::distance while Neighbor::score keeps the stale pre-mutation
// distance, and the arm is left in its stale order.
TEST_F(ResponseGeneratorTest, VectorScoreOnlyArmRefreshesScoreWithDistance) {
  ValkeyModuleCtx fake_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetExpire(testing::_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_NO_EXPIRE));

  auto floats = [](std::vector<float> v) {
    return std::string(reinterpret_cast<const char *>(v.data()),
                       v.size() * sizeof(float));
  };
  const std::string query = floats({0.0f, 0.0f, 0.0f, 0.0f});
  const std::string k1_now = floats({9.0f, 0.0f, 0.0f, 0.0f});
  const std::string k2_now = floats({2.0f, 0.0f, 0.0f, 0.0f});

  UnitTestSearchParameters parameters;
  parameters.index_schema = CreateIndexSchema("index").value();
  parameters.attribute_alias = "vec";
  parameters.query = query;
  // The two things that together describe the arm: it carries a text predicate,
  // and its score is nonetheless the raw distance.
  parameters.filter_parse_results.query_operations =
      QueryOperations::kContainsText;
  parameters.vector_score_only = true;
  ASSERT_TRUE(query::QueryHasTextPredicate(parameters));

  auto vector_index = indexes::VectorFlat<float>::Create(
      CreateFlatVectorIndexProto(4, data_model::DISTANCE_METRIC_L2, 10, 10),
      "vec", data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0);
  ASSERT_TRUE(vector_index.ok());
  VMSDK_EXPECT_OK(
      parameters.index_schema->AddIndex("vec", "vec", vector_index.value()));

  // As they arrive from the shard-side search: score == distance, because
  // ApplyHybridTextScore skips a vector_score_only arm.
  std::vector<indexes::Neighbor> neighbors;
  auto k1 = StringInternStore::Intern("k1");
  auto k2 = StringInternStore::Intern("k2");
  neighbors.push_back(indexes::Neighbor(k1, 1.0f));
  neighbors.push_back(indexes::Neighbor(k2, 4.0f));
  for (auto &n : neighbors) {
    n.sequence_number = 0;
    parameters.index_schema->SetIndexMutationSequenceNumber(n.external_id, 0);
  }
  // Only k1 was rewritten between the search and this reply.
  parameters.index_schema->SetDbMutationSequenceNumber(k1, 1);
  parameters.index_schema->SetDbMutationSequenceNumber(k2, 0);

  MockAttributeDataType data_type;
  EXPECT_CALL(data_type, ToProto())
      .WillRepeatedly(testing::Return(
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH));
  EXPECT_CALL(data_type, FetchAllAttributes(&fake_ctx, testing::_, testing::_,
                                            testing::_, testing::_))
      .WillRepeatedly([&k1_now, &k2_now](
                          ValkeyModuleCtx *, const std::optional<std::string> &,
                          ValkeyModuleKey *, absl::string_view key,
                          const absl::flat_hash_set<absl::string_view> &)
                          -> absl::StatusOr<RecordsMap> {
        RecordsMap m;
        m.emplace("vec", RecordsMapValue(vmsdk::MakeUniqueValkeyString("vec"),
                                         vmsdk::MakeUniqueValkeyString(
                                             key == "k1" ? k1_now : k2_now)));
        return m;
      });

  ProcessNeighborsForReply(&fake_ctx, data_type, neighbors, parameters,
                           std::make_optional<std::string>("vec"));

  ASSERT_EQ(neighbors.size(), 2);
  // Re-sorted distance-ascending: k1 moved from 1 to 81.
  EXPECT_EQ(neighbors[0].external_id->Str(), "k2");
  EXPECT_EQ(neighbors[1].external_id->Str(), "k1");
  EXPECT_FLOAT_EQ(neighbors[1].distance, 81.0f);
  // The invariant this arm shape must hold everywhere: the score the shard
  // serializes is the distance, refreshed included -- never the stale 1.0f.
  for (const auto &n : neighbors) {
    EXPECT_FLOAT_EQ(n.score, n.distance) << n.external_id->Str();
  }
}

// --- Text revalidation on the main-thread content-fetch path ---
//
// A document that mutated between shard-side scoring and content fetch is
// re-checked by VerifyFilter, and a text predicate is re-checked against the
// key's per-key TextIndex rather than the fetched records. That per-key index
// shares its Postings objects with the per-index trees (text_index.h), so the
// walk runs under the schema's time-sliced reader lock -- which must be
// released again before the score recompute, because SingleDocumentScorer
// acquires the same non-reentrant mutex itself.

namespace {
// A schema with one TEXT field carrying a single document, ready for the reply
// path: the key is registered in index_key_info_ (so the corpus size the
// scorer sources is non-zero) and its db sequence number is set to make the
// document look mutated, or not.
std::shared_ptr<MockIndexSchema> BuildSingleTextDocSchema(
    const std::string &key, const std::string &content, bool mutated) {
  auto schema = CreateIndexSchema("index").value();
  EXPECT_CALL(*schema, GetIdentifier(testing::_)).Times(testing::AnyNumber());
  schema->CreateTextIndexSchema();
  auto text_schema = schema->GetTextIndexSchema();
  auto text = std::make_shared<indexes::Text>(
      CreateTextIndexProto(/*with_suffix_trie=*/true, /*no_stem=*/true, 1.0),
      text_schema);
  EXPECT_TRUE(schema->AddIndex("text", "text", text).ok());

  auto interned = StringInternStore::Intern(key);
  EXPECT_TRUE(
      text->AddRecord(interned,
                      AttributeData(vmsdk::MakeUniqueValkeyString(content)))
          .ok());
  text_schema->CommitKeyData(interned);
  schema->SetIndexMutationSequenceNumber(interned, 0);
  schema->SetDbMutationSequenceNumber(interned, mutated ? 1 : 0);
  return schema;
}

// Runs the reply path for one neighbor of that schema against `filter`.
// Non-vector (no attribute alias, no vector identifier), so a surviving
// mutated document also gets its relevance score recomputed -- i.e. the
// recompute that must NOT run under the reader lock.
void RunTextReplyPath(ValkeyModuleCtx *fake_ctx,
                      UnitTestSearchParameters &parameters,
                      MockAttributeDataType &data_type, const std::string &key,
                      const std::string &content, const std::string &filter,
                      bool mutated, float initial_neighbor_score,
                      std::vector<indexes::Neighbor> &neighbors) {
  parameters.index_schema = BuildSingleTextDocSchema(key, content, mutated);
  ASSERT_NE(parameters.index_schema->GetTextIndexSchema(), nullptr);

  auto interned = StringInternStore::Intern(key);
  // The branch under test needs this to exist; without it the evaluation would
  // silently fall back to "no match" and the assertions below would be vacuous.
  ASSERT_NE(parameters.index_schema->GetTextIndexSchema()->GetPerKeyTextIndex(
                interned, /*lock=*/true),
            nullptr);

  TextParsingOptions options{};
  FilterParser parser(*parameters.index_schema, filter, options);
  auto parsed = parser.Parse();
  ASSERT_TRUE(parsed.ok()) << parsed.status();
  parameters.filter_parse_results = std::move(parsed).value();
  ASSERT_TRUE(query::QueryHasTextPredicate(parameters));

  neighbors.push_back(indexes::Neighbor(interned, initial_neighbor_score));
  neighbors.back().sequence_number = 0;

  EXPECT_CALL(data_type, ToProto())
      .WillRepeatedly(testing::Return(
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH));
  EXPECT_CALL(data_type, FetchAllAttributes(fake_ctx, testing::_, testing::_,
                                            testing::_, testing::_))
      .WillRepeatedly([&content](ValkeyModuleCtx *,
                                 const std::optional<std::string> &,
                                 ValkeyModuleKey *, absl::string_view,
                                 const absl::flat_hash_set<absl::string_view> &)
                          -> absl::StatusOr<RecordsMap> {
        RecordsMap m;
        m.emplace("text",
                  RecordsMapValue(vmsdk::MakeUniqueValkeyString("text"),
                                  vmsdk::MakeUniqueValkeyString(content)));
        return m;
      });

  query::ProcessNeighborsForReply(fake_ctx, data_type, neighbors, parameters,
                                  /*vector_identifier=*/std::nullopt);
}
}  // namespace

// The matching half: a mutated document whose text predicate still matches
// survives the revalidation and comes out with a freshly recomputed relevance
// score. A match can only be produced by the text branch -- it is the only one
// that hands the evaluator a per-key TextIndex and a target key -- so this also
// says the branch ran with the reader lock held, and the non-zero score says
// the lock was released again in time for SingleDocumentScorer to take it.
TEST_F(ResponseGeneratorTest, MutatedTextDocumentRevalidatedAndRescored) {
  ValkeyModuleCtx fake_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetExpire(testing::_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_NO_EXPIRE));

  UnitTestSearchParameters parameters;
  MockAttributeDataType data_type;
  std::vector<indexes::Neighbor> neighbors;
  RunTextReplyPath(&fake_ctx, parameters, data_type, "k1", "hello world",
                   "@text:hello", /*mutated=*/true,
                   /*initial_neighbor_score=*/7.0f, neighbors);

  ASSERT_EQ(neighbors.size(), 1u);
  EXPECT_EQ(neighbors[0].external_id->Str(), "k1");
  // Rescored through SingleDocumentScorer, which runs after the reader lock is
  // released: the stale 7.0 the shard carried is gone.
  EXPECT_NE(neighbors[0].score, 7.0f);
  EXPECT_GT(neighbors[0].score, 0.0f);
}

// The non-matching half, and the teeth of the pair: the same mutated document
// against a word it does not contain is dropped. That verdict can only come
// from evaluating the predicate against the per-key text index, so the text
// branch is provably executed rather than skipped.
TEST_F(ResponseGeneratorTest, MutatedTextDocumentDroppedWhenItNoLongerMatches) {
  ValkeyModuleCtx fake_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetExpire(testing::_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_NO_EXPIRE));

  UnitTestSearchParameters parameters;
  MockAttributeDataType data_type;
  std::vector<indexes::Neighbor> neighbors;
  RunTextReplyPath(&fake_ctx, parameters, data_type, "k1", "hello world",
                   "@text:zebra", /*mutated=*/true,
                   /*initial_neighbor_score=*/7.0f, neighbors);

  EXPECT_TRUE(neighbors.empty());
}

// The control: the identical non-matching filter on an UNmutated document
// keeps it, because the sequence-number fast path returns before any of this.
// This is what makes the drop above attributable to the revalidation branch --
// and it is also why the lock costs nothing on the common path.
TEST_F(ResponseGeneratorTest, UnmutatedTextDocumentSkipsRevalidation) {
  ValkeyModuleCtx fake_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetExpire(testing::_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_NO_EXPIRE));

  UnitTestSearchParameters parameters;
  MockAttributeDataType data_type;
  std::vector<indexes::Neighbor> neighbors;
  RunTextReplyPath(&fake_ctx, parameters, data_type, "k1", "hello world",
                   "@text:zebra", /*mutated=*/false,
                   /*initial_neighbor_score=*/7.0f, neighbors);

  ASSERT_EQ(neighbors.size(), 1u);
  EXPECT_FLOAT_EQ(neighbors[0].score, 7.0f);
}

// The reader lock VerifyFilter takes must be gone by the time the reply path
// returns -- a leaked or unbalanced reader would leave the schema mutex stuck
// in read mode and starve every ingestion worker. Taking the writer lock here
// would block forever if that happened.
TEST_F(ResponseGeneratorTest, TextRevalidationLeavesSchemaMutexFree) {
  ValkeyModuleCtx fake_ctx;
  EXPECT_CALL(*kMockValkeyModule, GetExpire(testing::_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_NO_EXPIRE));

  UnitTestSearchParameters parameters;
  MockAttributeDataType data_type;
  std::vector<indexes::Neighbor> neighbors;
  RunTextReplyPath(&fake_ctx, parameters, data_type, "k1", "hello world",
                   "@text:hello", /*mutated=*/true,
                   /*initial_neighbor_score=*/7.0f, neighbors);
  ASSERT_EQ(neighbors.size(), 1u);

  vmsdk::WriterMutexLock lock(&parameters.index_schema->GetTimeSlicedMutex());
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

class ResponseGeneratorDbParamTest
    : public ValkeySearchTestWithParam<data_model::AttributeDataType> {};

TEST_P(ResponseGeneratorDbParamTest, ProcessNeighborsForReplySelectsCorrectDB) {
  ValkeyModuleCtx fake_ctx;
  int target_db = 5;
  int original_db = 0;
  data_model::AttributeDataType type = GetParam();
  EXPECT_CALL(*kMockValkeyModule, GetExpire(testing::_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_NO_EXPIRE));
  UnitTestSearchParameters parameters;
  parameters.db_num = target_db;
  parameters.return_attributes.push_back(
      {.identifier = vmsdk::MakeUniqueValkeyString("field"),
       .alias = vmsdk::MakeUniqueValkeyString("field")});
  parameters.attribute_alias = "attr";

  std::vector<indexes::Neighbor> neighbors;
  auto external_id = StringInternStore::Intern("key");
  neighbors.push_back(indexes::Neighbor(external_id, 0));

  MockAttributeDataType data_type;
  EXPECT_CALL(data_type, ToProto()).WillRepeatedly(testing::Return(type));

  {
    // Expect DB selection sequence
    testing::InSequence s;
    EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx))
        .WillOnce(testing::Return(original_db));
    EXPECT_CALL(*kMockValkeyModule, SelectDb(&fake_ctx, target_db))
        .WillOnce(testing::Return(VALKEYMODULE_OK));

    // Expect fetch
    EXPECT_CALL(data_type,
                FetchAllAttributes(
                    &fake_ctx, std::make_optional(parameters.attribute_alias),
                    testing::_, absl::string_view("key"), testing::_))
        .WillOnce(testing::Return(RecordsMap{}));

    // Expect restore DB
    EXPECT_CALL(*kMockValkeyModule, SelectDb(&fake_ctx, original_db))
        .WillOnce(testing::Return(VALKEYMODULE_OK));
  }

  query::ProcessNeighborsForReply(&fake_ctx, data_type, neighbors, parameters,
                                  parameters.attribute_alias);
}

TEST_P(ResponseGeneratorDbParamTest, ProcessNeighborsForReplyNoContent) {
  ValkeyModuleCtx fake_ctx;
  int target_db = 5;
  int original_db = 0;
  data_model::AttributeDataType type = GetParam();

  EXPECT_CALL(*kMockValkeyModule, GetExpire(testing::_))
      .WillRepeatedly(testing::Return(VALKEYMODULE_NO_EXPIRE));

  UnitTestSearchParameters parameters;
  parameters.db_num = target_db;
  parameters.no_content = true;
  parameters.attribute_alias = "attr";

  std::vector<indexes::Neighbor> neighbors;
  auto external_id = StringInternStore::Intern("key");
  neighbors.push_back(indexes::Neighbor(external_id, 0));

  MockAttributeDataType data_type;
  EXPECT_CALL(data_type, ToProto()).WillRepeatedly(testing::Return(type));

  absl::flat_hash_set<absl::string_view> expected_identifiers;
  if (type == data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON) {
    expected_identifiers.insert(kJsonRootElementQuery);
  }

  {
    testing::InSequence s;
    EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx))
        .WillOnce(testing::Return(original_db));
    EXPECT_CALL(*kMockValkeyModule, SelectDb(&fake_ctx, target_db))
        .WillOnce(testing::Return(VALKEYMODULE_OK));

    EXPECT_CALL(data_type,
                FetchAllAttributes(
                    &fake_ctx, std::make_optional(parameters.attribute_alias),
                    testing::_, absl::string_view("key"), expected_identifiers))
        .WillOnce(testing::Return(RecordsMap{}));

    EXPECT_CALL(*kMockValkeyModule, SelectDb(&fake_ctx, original_db))
        .WillOnce(testing::Return(VALKEYMODULE_OK));
  }

  query::ProcessNeighborsForReply(&fake_ctx, data_type, neighbors, parameters,
                                  parameters.attribute_alias);
  EXPECT_EQ(neighbors.size(), 1);
}

INSTANTIATE_TEST_SUITE_P(
    ResponseGeneratorDbTests, ResponseGeneratorDbParamTest,
    testing::Values(data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH,
                    data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON),
    [](const testing::TestParamInfo<data_model::AttributeDataType> &info) {
      return info.param ==
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH
                 ? "Hash"
                 : "Json";
    });

}  // namespace

}  // namespace valkey_search
