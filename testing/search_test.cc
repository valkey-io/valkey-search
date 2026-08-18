/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/search.h"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/attribute_data_type.h"
#include "src/commands/filter_parser.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/indexes/numeric.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/tag.h"
#include "src/indexes/text.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_flat.h"
#include "src/indexes/vector_hnsw.h"
#include "src/query/predicate.h"
#include "src/utils/patricia_tree.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search {

namespace {

using testing::_;
using testing::ByMove;
using testing::Return;
using testing::TestParamInfo;
using testing::ValuesIn;
using ::valkey_search::indexes::IndexerType;

const char kIndexSchemaName[] = "index_schema_name";
const char kVectorAttributeAlias[] = "vector";
const uint32_t kDialect = 2;
const char kScoreAs[] = "__vector_score";
const int kVectorDimensions = 100;
const size_t kEfRuntime = 30;

auto VectorToStr = [](const std::vector<float> &v) {
  return absl::string_view((char *)v.data(), v.size() * sizeof(float));
};

class MockNumeric : public indexes::Numeric {
 public:
  MockNumeric(const data_model::NumericIndex &numeric_index_proto)
      : indexes::Numeric(numeric_index_proto) {}
  MOCK_METHOD(std::unique_ptr<indexes::Numeric::EntriesFetcher>, Search,
              (const query::NumericPredicate &predicate, bool negate),
              (const, override));
};

class TestedNumericEntriesFetcherIterator
    : public indexes::EntriesFetcherIteratorBase {
 public:
  TestedNumericEntriesFetcherIterator(std::vector<InternedStringPtr> &keys)
      : keys_(std::move(keys)), it_(keys_.begin()) {}
  bool Done() const override { return it_ == keys_.end(); }
  void Next() override { ++it_; }
  const InternedStringPtr &operator*() const override { return *it_; }

 private:
  std::vector<InternedStringPtr> keys_;
  std::vector<InternedStringPtr>::const_iterator it_;
};
// Mock Numeric EntriesFetcher.
// It fetches keys in the range provided at construction time. For example, when
// key_range <1, 3> is provided, it will fetch keys "1", "2", "3".
class TestedNumericEntriesFetcher : public indexes::Numeric::EntriesFetcher {
 public:
  TestedNumericEntriesFetcher(indexes::Numeric::EntriesRange &entries_range,
                              std::pair<size_t, size_t> key_range)
      : indexes::Numeric::EntriesFetcher(
            entries_range, key_range.second - key_range.first + 1),
        key_range_(key_range) {}
  TestedNumericEntriesFetcher(indexes::Numeric::EntriesRange &entries_range,
                              size_t size)
      : indexes::Numeric::EntriesFetcher(entries_range, size) {
    key_range_ = std::make_pair(0, size - 1);
  }
  size_t Size() const override {
    return key_range_.second - key_range_.first + 1;
  }
  size_t GetId() const { return Size(); }

  std::unique_ptr<indexes::EntriesFetcherIteratorBase> Begin() override {
    std::vector<InternedStringPtr> keys;
    for (size_t i = key_range_.first; i <= key_range_.second; ++i) {
      auto interned_key = StringInternStore::Intern(std::to_string(i));
      keys.push_back(interned_key);
    }

    return std::make_unique<TestedNumericEntriesFetcherIterator>(keys);
  }

 private:
  std::pair<size_t, size_t> key_range_;
};

class MockTag : public indexes::Tag {
 public:
  MockTag(const data_model::TagIndex &tag_index_proto)
      : indexes::Tag(tag_index_proto) {}
  MOCK_METHOD(std::unique_ptr<indexes::EntriesFetcherBase>, Search,
              (const query::TagPredicate &predicate, bool negate),
              (const, override));
};

class TestedTagEntriesFetcher : public indexes::Tag::EntriesFetcher {
 public:
  explicit TestedTagEntriesFetcher(size_t size)
      : indexes::Tag::EntriesFetcher(/*matched_slots=*/{},
                                     /*extras=*/{},
                                     /*size=*/size),
        size_(size) {}

  size_t Size() const override { return size_; }
  size_t GetId() const { return size_; }

 private:
  size_t size_;
};

struct EvaluateFilterAsPrimaryTestCase {
  std::string test_name;
  std::string filter;
  size_t evaluate_size{0};
  std::vector<size_t> fetcher_ids;
  std::string expected_tree_structure;
};

class EvaluateFilterAsPrimaryTest
    : public ValkeySearchTestWithParam<EvaluateFilterAsPrimaryTestCase> {};

void InitIndexSchema(MockIndexSchema *index_schema) {
  data_model::NumericIndex numeric_index_proto;

  EXPECT_CALL(*index_schema, GetIdentifier(::testing::_))
      .Times(::testing::AnyNumber());

  auto numeric_index_100_10 =
      std::make_shared<MockNumeric>(numeric_index_proto);
  auto numeric_index_100_30 =
      std::make_shared<MockNumeric>(numeric_index_proto);
  VMSDK_EXPECT_OK(index_schema->AddIndex(
      "numeric_index_100_10", "numeric_index_100_10", numeric_index_100_10));
  VMSDK_EXPECT_OK(index_schema->AddIndex(
      "numeric_index_100_30", "numeric_index_100_30", numeric_index_100_30));
  auto numeric_index_100_20 =
      std::make_shared<MockNumeric>(numeric_index_proto);
  auto numeric_index_100_40 =
      std::make_shared<MockNumeric>(numeric_index_proto);
  VMSDK_EXPECT_OK(index_schema->AddIndex(
      "numeric_index_100_20", "numeric_index_100_20", numeric_index_100_20));
  VMSDK_EXPECT_OK(index_schema->AddIndex(
      "numeric_index_100_40", "numeric_index_100_40", numeric_index_100_40));
  static indexes::Numeric::EntriesRange entries_range;

  EXPECT_CALL(*numeric_index_100_10, Search(_, false)).WillRepeatedly([]() {
    return std::make_unique<TestedNumericEntriesFetcher>(entries_range, 10);
  });
  EXPECT_CALL(*numeric_index_100_10, Search(_, true)).WillRepeatedly([]() {
    return std::make_unique<TestedNumericEntriesFetcher>(entries_range, 90);
  });
  EXPECT_CALL(*numeric_index_100_30, Search(_, false)).WillRepeatedly([]() {
    return std::make_unique<TestedNumericEntriesFetcher>(entries_range, 30);
  });
  EXPECT_CALL(*numeric_index_100_30, Search(_, true)).WillRepeatedly([]() {
    return std::make_unique<TestedNumericEntriesFetcher>(entries_range, 70);
  });
  EXPECT_CALL(*numeric_index_100_20, Search(_, false)).WillRepeatedly([]() {
    return std::make_unique<TestedNumericEntriesFetcher>(entries_range, 20);
  });
  EXPECT_CALL(*numeric_index_100_20, Search(_, true)).WillRepeatedly([]() {
    return std::make_unique<TestedNumericEntriesFetcher>(entries_range, 80);
  });
  EXPECT_CALL(*numeric_index_100_40, Search(_, false)).WillRepeatedly([]() {
    return std::make_unique<TestedNumericEntriesFetcher>(entries_range, 40);
  });
  EXPECT_CALL(*numeric_index_100_40, Search(_, true)).WillRepeatedly([]() {
    return std::make_unique<TestedNumericEntriesFetcher>(entries_range, 60);
  });

  data_model::TagIndex tag_index_proto;
  tag_index_proto.set_separator(",");
  tag_index_proto.set_case_sensitive(false);
  auto tag_index_100_15 = std::make_shared<MockTag>(tag_index_proto);

  VMSDK_EXPECT_OK(index_schema->AddIndex("tag_index_100_15", "tag_index_100_15",
                                         tag_index_100_15));
  EXPECT_CALL(*tag_index_100_15, Search(_, false)).WillRepeatedly([]() {
    return std::make_unique<TestedTagEntriesFetcher>(15);
  });
  EXPECT_CALL(*tag_index_100_15, Search(_, true)).WillRepeatedly([]() {
    return std::make_unique<TestedTagEntriesFetcher>(85);
  });
}

TEST_P(EvaluateFilterAsPrimaryTest, ParseParams) {
  const EvaluateFilterAsPrimaryTestCase &test_case = GetParam();
  auto index_schema = CreateIndexSchema(kIndexSchemaName).value();
  InitIndexSchema(index_schema.get());
  TextParsingOptions options{};
  FilterParser parser(*index_schema, test_case.filter, options);
  auto filter_parse_results = parser.Parse();

  // Generate the actual predicate tree structure
  std::string actual_tree =
      PrintPredicateTree(filter_parse_results.value().root_predicate.get());

  // Compare expected vs actual tree structure
  EXPECT_EQ(actual_tree, test_case.expected_tree_structure)
      << "Tree structure mismatch for filter: " << test_case.filter;

  std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> entries_fetchers;
  UnitTestSearchParameters params;
  params.index_schema = index_schema;
  params.filter_parse_results = std::move(filter_parse_results.value());
  params.attribute_alias = "";  // Makes is_vec_query = false
  EXPECT_EQ(EvaluateFilterAsPrimary(
                params, params.filter_parse_results.root_predicate.get(),
                entries_fetchers, false),
            test_case.evaluate_size);

  EXPECT_EQ(entries_fetchers.size(), test_case.fetcher_ids.size());
  std::vector<size_t> actual_fetcher_ids;
  while (!entries_fetchers.empty()) {
    auto entry_fetcher = std::move(entries_fetchers.front());
    entries_fetchers.pop();
    auto numeric_fetcher =
        dynamic_cast<const TestedNumericEntriesFetcher *>(entry_fetcher.get());
    if (numeric_fetcher) {
      actual_fetcher_ids.push_back(numeric_fetcher->GetId());
    } else {
      auto tag_fetcher =
          dynamic_cast<const TestedTagEntriesFetcher *>(entry_fetcher.get());
      if (tag_fetcher) {
        actual_fetcher_ids.push_back(tag_fetcher->GetId());
      } else {
        FAIL();
      }
    }
  }
  std::sort(actual_fetcher_ids.begin(), actual_fetcher_ids.end());
  std::vector<size_t> expected_fetcher_ids = test_case.fetcher_ids;
  std::sort(expected_fetcher_ids.begin(), expected_fetcher_ids.end());
  EXPECT_EQ(actual_fetcher_ids, expected_fetcher_ids);
}

INSTANTIATE_TEST_SUITE_P(
    EvaluateFilterAsPrimaryTests, EvaluateFilterAsPrimaryTest,
    ValuesIn<EvaluateFilterAsPrimaryTestCase>({
        {
            .test_name = "single_numeric_10",
            .filter = "@numeric_index_100_10:[1.0 2.0]",
            .evaluate_size = 10,
            .fetcher_ids = {10},
            .expected_tree_structure = "NUMERIC(numeric_index_100_10)\n",
        },
        {
            .test_name = "single_numeric_30",
            .filter = "@numeric_index_100_30:[1.0 2.0]",
            .evaluate_size = 30,
            .fetcher_ids = {30},
            .expected_tree_structure = "NUMERIC(numeric_index_100_30)\n",
        },
        {
            .test_name = "two_numerics_and",
            .filter = "@numeric_index_100_30:[1.0 2.0] "
                      "@numeric_index_100_10:[3.0 4.0]",
            .evaluate_size = 10,
            .fetcher_ids = {10},
            .expected_tree_structure = "AND{\n"

                                       "  NUMERIC(numeric_index_100_30)\n"
                                       "  NUMERIC(numeric_index_100_10)\n"
                                       "}\n",
        },
        {
            .test_name = "two_numerics_or",
            .filter = "@numeric_index_100_30:[1.0 2.0] |"
                      "@numeric_index_100_10:[3.0 4.0]",
            .evaluate_size = 40,
            .fetcher_ids = {10, 30},
            .expected_tree_structure = "OR{\n"
                                       "  NUMERIC(numeric_index_100_30)\n"
                                       "  NUMERIC(numeric_index_100_10)\n"
                                       "}\n",
        },
        {
            .test_name = "single_numeric_negate_10",
            .filter = "-@numeric_index_100_10:[1.0 2.0]",
            .evaluate_size = 90,
            .fetcher_ids = {90},
            .expected_tree_structure = "NOT{\n"
                                       "  NUMERIC(numeric_index_100_10)\n"
                                       "}\n",
        },
        {
            .test_name = "single_numeric_negate_30",
            .filter = "-@numeric_index_100_30:[1.0 2.0]",
            .evaluate_size = 70,
            .fetcher_ids = {70},
            .expected_tree_structure = "NOT{\n"
                                       "  NUMERIC(numeric_index_100_30)\n"
                                       "}\n",
        },
        {
            .test_name = "negate_two_numerics_or",
            .filter = "-(@numeric_index_100_30:[1.0 2.0] |"
                      "@numeric_index_100_10:[3.0 4.0])",
            .evaluate_size = 70,
            .fetcher_ids = {70},
            .expected_tree_structure = "NOT{\n"
                                       "  OR{\n"
                                       "    NUMERIC(numeric_index_100_30)\n"
                                       "    NUMERIC(numeric_index_100_10)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "negate_two_numerics_and",
            .filter = "-(@numeric_index_100_30:[1.0 2.0] "
                      "@numeric_index_100_10:[3.0 4.0])",
            .evaluate_size = 160,
            .fetcher_ids = {70, 90},
            .expected_tree_structure = "NOT{\n"
                                       "  AND{\n"
                                       "    NUMERIC(numeric_index_100_30)\n"
                                       "    NUMERIC(numeric_index_100_10)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "double_negate_two_numerics_or",
            .filter = "-(-(@numeric_index_100_30:[1.0 2.0] |"
                      "@numeric_index_100_10:[3.0 4.0]))",
            .evaluate_size = 40,
            .fetcher_ids = {10, 30},
            .expected_tree_structure = "NOT{\n"
                                       "  NOT{\n"
                                       "    OR{\n"
                                       "      NUMERIC(numeric_index_100_30)\n"
                                       "      NUMERIC(numeric_index_100_10)\n"
                                       "    }\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "highly_nested_and_or_mix",
            .filter = "@tag_index_100_15:{tag1} @tag_index_100_15:{tag2} "
                      "(@numeric_index_100_10:[1.0 2.0] | "
                      "@tag_index_100_15:{tag3} | "
                      "@numeric_index_100_30:[3.0 4.0] | "
                      "@numeric_index_100_20:[3.0 4.0] | "
                      "@numeric_index_100_40:[3.0 4.0] )",
            .evaluate_size = 15,
            .fetcher_ids = {15},
            .expected_tree_structure = "AND{\n"
                                       "  TAG(tag_index_100_15)\n"
                                       "  TAG(tag_index_100_15)\n"
                                       "  OR{\n"
                                       "    NUMERIC(numeric_index_100_10)\n"
                                       "    TAG(tag_index_100_15)\n"
                                       "    NUMERIC(numeric_index_100_30)\n"
                                       "    NUMERIC(numeric_index_100_20)\n"
                                       "    NUMERIC(numeric_index_100_40)\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "deeply_nested_with_negation",
            .filter = "@numeric_index_100_30:[1.0 2.0] "
                      "(-(@tag_index_100_15:{tag1} | "
                      "(@numeric_index_100_10:[3.0 4.0] "
                      "@tag_index_100_15:{tag2})))",
            .evaluate_size = 30,
            .fetcher_ids = {30},
            .expected_tree_structure = "AND{\n"
                                       "  NUMERIC(numeric_index_100_30)\n"
                                       "  NOT{\n"
                                       "    OR{\n"
                                       "      TAG(tag_index_100_15)\n"
                                       "      AND{\n"
                                       "        NUMERIC(numeric_index_100_10)\n"
                                       "        TAG(tag_index_100_15)\n"
                                       "      }\n"
                                       "    }\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "triple_nested_or_and_mix",
            .filter = "(@tag_index_100_15:{tag1} | "
                      "(@numeric_index_100_30:[1.0 2.0] "
                      "(@tag_index_100_15:{tag2} | "
                      "@numeric_index_100_10:[5.0 6.0])))",
            .evaluate_size = 40,
            .fetcher_ids = {15, 15, 10},
            .expected_tree_structure = "OR{\n"
                                       "  TAG(tag_index_100_15)\n"
                                       "  AND{\n"
                                       "    NUMERIC(numeric_index_100_30)\n"
                                       "    OR{\n"
                                       "      TAG(tag_index_100_15)\n"
                                       "      NUMERIC(numeric_index_100_10)\n"
                                       "    }\n"
                                       "  }\n"
                                       "}\n",
        },
        {
            .test_name = "complex_multilevel_nesting",
            .filter = "-(@tag_index_100_15:{tag1} "
                      "(-(@numeric_index_100_30:[1.0 2.0] | "
                      "@numeric_index_100_10:[3.0 4.0]) | "
                      "@tag_index_100_15:{tag2}))",
            .evaluate_size = 125,
            .fetcher_ids = {10, 30, 85},
            .expected_tree_structure =
                "NOT{\n"
                "  AND{\n"
                "    TAG(tag_index_100_15)\n"
                "    OR{\n"
                "      NOT{\n"
                "        OR{\n"
                "          NUMERIC(numeric_index_100_30)\n"
                "          NUMERIC(numeric_index_100_10)\n"
                "        }\n"
                "      }\n"
                "      TAG(tag_index_100_15)\n"
                "    }\n"
                "  }\n"
                "}\n",
        },
    }),
    [](const TestParamInfo<EvaluateFilterAsPrimaryTestCase> &info) {
      return info.param.test_name;
    });

std::shared_ptr<MockIndexSchema> CreateIndexSchemaWithMultipleAttributes(
    const IndexerType vector_indexer_type = indexes::IndexerType::kHNSW,
    data_model::DistanceMetric distance_metric =
        data_model::DISTANCE_METRIC_L2) {
  auto index_schema = CreateIndexSchema(kIndexSchemaName).value();
  EXPECT_CALL(*index_schema, GetIdentifier(::testing::_))
      .Times(::testing::AnyNumber());

  // Add vector index
  std::shared_ptr<indexes::IndexBase> vector_index;
  if (vector_indexer_type == IndexerType::kHNSW) {
    vector_index =
        indexes::VectorHNSW<float>::Create(
            CreateHNSWVectorIndexProto(kVectorDimensions, distance_metric, 1000,
                                       10, 300, 30),
            "vector_attribute_identifier",
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
            .value();
  } else {
    vector_index = indexes::VectorFlat<float>::Create(
                       CreateFlatVectorIndexProto(kVectorDimensions,
                                                  distance_metric, 1000, 250),
                       "vector_attribute_identifier",
                       data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
                       .value();
  }
  VMSDK_EXPECT_OK(index_schema->AddIndex(kVectorAttributeAlias,
                                         kVectorAttributeAlias, vector_index));

  // Add numeric index
  data_model::NumericIndex numeric_index_proto;
  auto numeric_index = std::make_shared<indexes::Numeric>(numeric_index_proto);
  VMSDK_EXPECT_OK(index_schema->AddIndex("numeric", "numeric", numeric_index));

  // Add tag index
  data_model::TagIndex tag_index_proto;
  tag_index_proto.set_separator(",");
  tag_index_proto.set_case_sensitive(false);
  auto tag_index = std::make_shared<indexes::Tag>(tag_index_proto);
  VMSDK_EXPECT_OK(index_schema->AddIndex("tag", "tag", tag_index));

  // Add records
  size_t num_records = 10000;
#ifdef SAN_BUILD
  num_records = 100;
#endif
  auto vectors =
      DeterministicallyGenerateVectors(num_records, kVectorDimensions, 10.0);
  for (size_t i = 0; i < num_records; ++i) {
    auto key = std::to_string(i);

    // Add record to vector index
    std::string vector = std::string((char *)vectors[i].data(),
                                     vectors[i].size() * sizeof(float));
    auto interned_key = StringInternStore::Intern(key);
    index_schema->SetIndexMutationSequenceNumber(interned_key, i);

    VMSDK_EXPECT_OK(vector_index->AddRecord(interned_key, vector));

    // Add record to numeric index
    auto numeric_value = std::to_string(i);
    VMSDK_EXPECT_OK(numeric_index->AddRecord(interned_key, numeric_value));

    // Add record to tag index
    std::string tag_value = "LT10000";
    if (i < 5) {
      tag_value += ",LT5";
    }
    if (i < 3) {
      tag_value += ",LT3";
    }
    VMSDK_EXPECT_OK(tag_index->AddRecord(interned_key, tag_value));
  }

  return index_schema;
}

struct LocalSearchTestCase {
  std::string test_name;
  int k;  // The number of neighbors to return.
  std::string filter;
  size_t expected_neighbors_size;
  bool is_vector_search_query = true;
};

class LocalSearchTest
    : public ValkeySearchTestWithParam<
          std::tuple<data_model::DistanceMetric, LocalSearchTestCase>> {};

TEST_P(LocalSearchTest, LocalSearchTest) {
  const auto &param = GetParam();
  data_model::DistanceMetric distance_metric = std::get<0>(param);
  const LocalSearchTestCase &test_case = std::get<1>(param);
  auto index_schema = CreateIndexSchemaWithMultipleAttributes(
      indexes::IndexerType::kFlat, distance_metric);
  UnitTestSearchParameters params;
  params.index_schema_name = kIndexSchemaName;
  if (test_case.is_vector_search_query) {
    params.attribute_alias = kVectorAttributeAlias;
  }
  params.score_as = vmsdk::MakeUniqueValkeyString(kScoreAs);
  params.dialect = kDialect;
  params.k = test_case.k;
  params.ef = kEfRuntime;
  std::vector<float> query_vector(kVectorDimensions, 1.0);
  params.query = VectorToStr(query_vector);
  TextParsingOptions options{};
  FilterParser parser(*index_schema, test_case.filter, options);
  params.filter_parse_results = std::move(parser.Parse().value());
  params.index_schema = index_schema;

  auto prefiltering_requests =
      Metrics::GetStats().query_prefiltering_requests_cnt.load();
  auto time_slice_queries = Metrics::GetStats().time_slice_queries.load();
  auto status = Search(params, valkey_search::query::SearchMode::kLocal);

  if (test_case.is_vector_search_query) {
    EXPECT_EQ(prefiltering_requests + 1,
              Metrics::GetStats().query_prefiltering_requests_cnt.load());
  }

  EXPECT_EQ(time_slice_queries + 1,
            Metrics::GetStats().time_slice_queries.load());
  VMSDK_EXPECT_OK(status);
  EXPECT_EQ(params.search_result.neighbors.size(),
            test_case.expected_neighbors_size);
  if (test_case.is_vector_search_query &&
      !params.search_result.neighbors.empty()) {
    if (distance_metric == data_model::DISTANCE_METRIC_COSINE) {
      for (const auto &neighbor : params.search_result.neighbors) {
        EXPECT_GE(neighbor.distance, 0.0f);
        EXPECT_LE(neighbor.distance, 2.0f);
      }
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    LocalSearchTests, LocalSearchTest,
    testing::Combine(
        testing::ValuesIn({data_model::DISTANCE_METRIC_L2,
                           data_model::DISTANCE_METRIC_COSINE}),
        testing::ValuesIn<LocalSearchTestCase>({
            {
                .test_name = "numeric_filter_k_eligible_candidates",
                .k = 10,
                .filter = "@numeric:[10 20]",
                .expected_neighbors_size = 10,
            },
            {
                .test_name = "numeric_filter_less_than_k_eligible_candidates",
                .k = 10,
                .filter = "@numeric:[99 99]",
                .expected_neighbors_size = 1,
            },
            {
                .test_name = "numeric_filter_no_eligible_candidates",
                .k = 10,
                .filter = "@numeric:[10000 20000]",
                .expected_neighbors_size = 0,
            },
            {
                .test_name = "tag_filter_k_eligible_candidates",
                .k = 5,
                .filter = "@tag:{LT5}",
                .expected_neighbors_size = 5,
            },
            {
                .test_name = "tag_filter_less_than_k_eligible_candidates",
                .k = 5,
                .filter = "@tag:{LT3}",
                .expected_neighbors_size = 3,
            },
            {
                .test_name = "tag_filter_all_candidates_eligible",
                .k = 10,
                .filter = "@tag:{Lt*}",
                .expected_neighbors_size = 10,
            },
            {
                .test_name = "tag_filter_no_eligible_candidates",
                .k = 10,
                .filter = "@tag:{random}",
                .expected_neighbors_size = 0,
            },
            {
                .test_name = "non_vector_numeric_filter_eligible_candidates",
                .filter = "@numeric:[1 10]",
                .expected_neighbors_size = 10,
                .is_vector_search_query = false,
            },
            {
                .test_name = "non_vector_numeric_and_tag_filter",
                .filter = "@numeric:[1 10] @tag:{LT5}",
                .expected_neighbors_size = 4,
                .is_vector_search_query = false,
            },
            {
                .test_name = "non_vector_numeric_or_numeric_filter",
                .filter = "@numeric:[1 10] | @numeric:[21 25]",
                .expected_neighbors_size = 15,
                .is_vector_search_query = false,
            },
        })),
    [](const testing::TestParamInfo<
        std::tuple<data_model::DistanceMetric, LocalSearchTestCase>> &info) {
      return (std::get<0>(info.param) == data_model::DISTANCE_METRIC_L2
                  ? "L2_"
                  : "COSINE_") +
             std::get<1>(info.param).test_name;
    });

// A hybrid `text=>[KNN]` query must rank by the text relevance score, not by
// the vector distance. Both docs contain "cat": "short" is a one-word document
// (high BM25) but far from the query vector; "long" buries "cat" in a long
// document (low BM25) yet sits exactly on the query vector (distance 0). By KNN
// distance alone "long" would rank first; by text score "short" wins. The
// result order must follow the text score.
TEST_F(ValkeySearchTest, HybridQueryRanksByTextScoreNotVectorDistance) {
  auto schema = CreateIndexSchema(kIndexSchemaName).value();
  EXPECT_CALL(*schema, GetIdentifier(::testing::_))
      .Times(::testing::AnyNumber());

  auto vector_index =
      indexes::VectorFlat<float>::Create(
          CreateFlatVectorIndexProto(kVectorDimensions,
                                     data_model::DISTANCE_METRIC_L2, 1000, 250),
          "vector_attribute_identifier",
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
          .value();
  VMSDK_EXPECT_OK(schema->AddIndex(kVectorAttributeAlias, kVectorAttributeAlias,
                                   vector_index));

  schema->CreateTextIndexSchema();
  auto text_schema = schema->GetTextIndexSchema();
  auto text = std::make_shared<indexes::Text>(
      CreateTextIndexProto(/*with_suffix_trie=*/true, /*no_stem=*/true, 1.0),
      text_schema);
  VMSDK_EXPECT_OK(schema->AddIndex("text", "text", text));

  auto add_doc = [&](const std::string &key, const std::string &content,
                     float vec_value) {
    auto interned = StringInternStore::Intern(key);
    std::vector<float> vec(kVectorDimensions, vec_value);
    VMSDK_EXPECT_OK(vector_index->AddRecord(
        interned, std::string((char *)vec.data(), vec.size() * sizeof(float))));
    VMSDK_EXPECT_OK(text->AddRecord(interned, content));
    text_schema->CommitKeyData(interned);
    schema->SetIndexMutationSequenceNumber(interned, 0);
  };
  // "short": short doc (high BM25 for "cat"), far from the query vector.
  add_doc("short", "cat", 5.0f);
  // "long": long doc (low BM25 for "cat"), exactly on the query vector.
  add_doc("long", "cat dog bird fish tree stone river cloud", 1.0f);

  UnitTestSearchParameters params;
  params.index_schema_name = kIndexSchemaName;
  params.index_schema = schema;
  params.attribute_alias = kVectorAttributeAlias;
  params.score_as = vmsdk::MakeUniqueValkeyString(kScoreAs);
  params.dialect = kDialect;
  params.k = 2;
  params.ef = kEfRuntime;
  std::vector<float> query_vector(kVectorDimensions, 1.0f);
  params.query = VectorToStr(query_vector);
  TextParsingOptions options{};
  FilterParser parser(*schema, "@text:cat", options);
  params.filter_parse_results = std::move(parser.Parse().value());

  VMSDK_EXPECT_OK(Search(params, valkey_search::query::SearchMode::kLocal));

  const auto &neighbors = params.search_result.neighbors;
  ASSERT_EQ(neighbors.size(), 2u);
  // Ranked by text score: "short" (higher BM25) before "long".
  EXPECT_EQ(neighbors[0].external_id->Str(), "short");
  EXPECT_EQ(neighbors[1].external_id->Str(), "long");
  // The winner has the higher text score but the LARGER vector distance,
  // confirming distance is not the ranking key.
  EXPECT_GT(neighbors[0].score, neighbors[1].score);
  EXPECT_GT(neighbors[0].distance, neighbors[1].distance);
}

struct FetchFilteredKeysTestCase {
  std::string test_name;
  std::string filter;
  std::vector<std::pair<size_t, size_t>> fetched_key_ranges;
  std::unordered_set<std::string> expected_keys;
};

class FetchFilteredKeysTest
    : public ValkeySearchTestWithParam<FetchFilteredKeysTestCase> {};

TEST_P(FetchFilteredKeysTest, ParseParams) {
  auto index_schema = CreateIndexSchemaWithMultipleAttributes();
  auto vector_index = dynamic_cast<indexes::VectorBase *>(
      index_schema->GetIndex(kVectorAttributeAlias)->get());
  const FetchFilteredKeysTestCase &test_case = GetParam();
  UnitTestSearchParameters params;
  TextParsingOptions options{};
  FilterParser parser(*index_schema, test_case.filter, options);
  params.filter_parse_results = std::move(parser.Parse().value());
  params.k = 100;
  auto vectors = DeterministicallyGenerateVectors(1, kVectorDimensions, 10.0);
  params.query =
      std::string((char *)vectors[0].data(), vectors[0].size() * sizeof(float));
  std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> entries_fetchers;
  indexes::Numeric::EntriesRange entries_range;
  for (auto key_range : test_case.fetched_key_ranges) {
    entries_fetchers.push(std::make_unique<TestedNumericEntriesFetcher>(
        entries_range, std::make_pair(key_range.first, key_range.second)));
  }
  auto results = CalcBestMatchingPrefilteredKeys(params, entries_fetchers,
                                                 vector_index, 0);
  auto neighbors = vector_index->CreateReply(results).value();
  EXPECT_EQ(neighbors.size(), test_case.expected_keys.size());
  for (auto it = neighbors.begin(); it != neighbors.end(); ++it) {
    EXPECT_TRUE(
        test_case.expected_keys.contains(std::string(*it->external_id)));
  }
}

INSTANTIATE_TEST_SUITE_P(
    FetchFilteredKeysTests, FetchFilteredKeysTest,
    ValuesIn<FetchFilteredKeysTestCase>({
        {
            .test_name = "base_predicate",
            .filter = "@numeric:[0 4]",
            .fetched_key_ranges = {{0, 4}},
            .expected_keys = {"0", "1", "2", "3", "4"},
        },
        {
            .test_name = "or_predicate",
            .filter = "@numeric:[0 4] | @numeric:[1 6]",
            .fetched_key_ranges = {{0, 4}, {1, 6}},
            .expected_keys = {"0", "1", "2", "3", "4", "5", "6"},
        },
        {
            .test_name = "and_predicate",
            .filter = "@numeric:[0 4] @numeric:[1 6]",
            // Only the entries_fetcher for the smaller set is returned from
            // EvaluateFilterAsPrimary.
            .fetched_key_ranges = {{0, 4}},
            .expected_keys = {"1", "2", "3", "4"},
        },
        // Cases that should not happen but would still work.
        {
            .test_name = "base_predicate_mismatch_with_fetched_key_range",
            .filter = "@numeric:[1 5]",
            .fetched_key_ranges = {{0, 4}},
            .expected_keys = {"1", "2", "3", "4"},
        },
    }),
    [](const TestParamInfo<FetchFilteredKeysTestCase> &info) {
      return info.param.test_name;
    });

struct SearchTestCase {
  std::string test_name;
  std::string filter;
  int k;  // The number of neighbors to return.
  std::unordered_set<std::string> expected_keys;
};

class SearchTest : public ValkeySearchTestWithParam<
                       std::tuple<IndexerType, SearchTestCase>> {};

TEST_P(SearchTest, ParseParams) {
  const auto &param = GetParam();
  IndexerType indexer_type = std::get<0>(param);
  SearchTestCase test_case = std::get<1>(param);
  UnitTestSearchParameters params;
  params.index_schema = CreateIndexSchemaWithMultipleAttributes(indexer_type);
  params.index_schema_name = kIndexSchemaName;
  params.attribute_alias = kVectorAttributeAlias;
  params.score_as = vmsdk::MakeUniqueValkeyString(kScoreAs);
  params.dialect = kDialect;
  params.k = test_case.k;
  params.ef = kEfRuntime;
  std::vector<float> query_vector(kVectorDimensions, 0.0);
  params.query = VectorToStr(query_vector);
  if (!test_case.filter.empty()) {
    TextParsingOptions options{};
    FilterParser parser(*params.index_schema, test_case.filter, options);
    params.filter_parse_results = std::move(parser.Parse().value());
  }
  auto status = Search(params, query::SearchMode::kLocal);
  VMSDK_EXPECT_OK(status);
#ifndef SAN_BUILD
  EXPECT_EQ(params.search_result.neighbors.size(),
            test_case.expected_keys.size());
#endif

  for (auto &neighbor : params.search_result.neighbors) {
    EXPECT_TRUE(
        test_case.expected_keys.contains(std::string(*neighbor.external_id)));
  }
}

INSTANTIATE_TEST_SUITE_P(
    SearchTests, SearchTest,
    testing::Combine(
        ValuesIn({IndexerType::kHNSW, IndexerType::kFlat}),
        ValuesIn<SearchTestCase>(
            // Note that the vectors are generated such that vectors with lower
            // indices are closer to the query vector. Hence, the nearest
            // neighbors are expected to be the first k vectors that match the
            // filter.
            {{
                 .test_name = "no_filter",
                 .filter = "",
                 .k = 5,
                 .expected_keys = {"0", "1", "2", "3", "4"},
             },
             {
                 .test_name = "prefix_match_filter",
                 .filter = "@tag:{lT*}",
                 .k = 5,
                 .expected_keys = {"0", "1", "2", "3", "4"},
             },
             {
                 .test_name = "numeric_filter_all_candidates_eligible",
                 .filter = "@numeric:[0 10000]",
                 .k = 5,
                 .expected_keys = {"0", "1", "2", "3", "4"},
             },
             {
                 .test_name = "numeric_filter_k_eligible_candidates",
                 .filter = "@numeric:[0 4]",
                 .k = 5,
                 .expected_keys = {"0", "1", "2", "3", "4"},
             },
             {
                 .test_name = "numeric_filter_less_than_k_eligible_candidates",
                 .filter = "@numeric:[0 2]",
                 .k = 5,
                 .expected_keys = {"0", "1", "2"},
             },
             {
                 .test_name = "numeric_filter_no_eligible_candidates",
                 .filter = "@numeric:[10000 20000]",
                 .k = 5,
                 .expected_keys = {},
             },
             {
                 .test_name = "tag_filter_all_candidates_eligible",
                 .filter = "@tag:{LT10000}",
                 .k = 5,
                 .expected_keys = {"0", "1", "2", "3", "4"},
             },
             {
                 .test_name = "tag_filter_k_eligible_candidates",
                 .filter = "@tag:{LT5}",
                 .k = 5,
                 .expected_keys = {"0", "1", "2", "3", "4"},
             },
             {
                 .test_name = "tag_filter_less_than_k_eligible_candidates",
                 .filter = "@tag:{LT3}",
                 .k = 5,
                 .expected_keys = {"0", "1", "2"},
             },
             {
                 .test_name = "tag_filter_no_eligible_candidates",
                 .filter = "@tag:{random}",
                 .k = 5,
                 .expected_keys = {},
             },
             {
                 .test_name = "or_filter",
                 .filter = "@numeric:[4 100] | @tag:{LT5}",
                 .k = 5,
                 .expected_keys = {"0", "1", "2", "3", "4"},
             },
             {
                 .test_name = "and_filter",
                 .filter = "@numeric:[4 100] @tag:{LT5}",
                 .k = 5,
                 .expected_keys = {"4"},
             },
             // TODO: Add tests where vector, numeric and tag
             // indexes are not aligned.
             {
                 .test_name = "numeric_negate_filter",
                 .filter = "-@numeric:[0 100]",
                 .k = 5,
                 .expected_keys = {"101", "102", "103", "104", "105"},
             },
             {
                 .test_name = "tag_negate_filter",
                 .filter = "-@tag:{LT5}",
                 .k = 5,
                 .expected_keys = {"5", "6", "7", "8", "9"},
             },
             {
                 .test_name = "composite_filter_with_negate",
                 .filter = "-@numeric:[4 100] @tag:{LT5}",
                 .k = 5,
                 .expected_keys = {"0", "1", "2", "3"},
             }})),
    [](const TestParamInfo<std::tuple<IndexerType, SearchTestCase>> &info) {
      std::string test_name = std::get<1>(info.param).test_name;
      test_name +=
          (std::get<0>(info.param) == IndexerType::kHNSW) ? "_hnsw" : "_flat";
      return test_name;
    });

struct IndexedContentTestCase {
  struct TestReturnAttribute {
    std::string identifier;
    std::string alias;
  };
  struct TestIndex {
    std::string attribute_alias;
    std::string attribute_identifier;
    IndexerType indexer_type;
    absl::flat_hash_map<std::string, std::string> contents;
  };
  struct TestNeighbor {
    std::string external_id;
    float score;
    std::optional<absl::flat_hash_map<std::string, std::string>>
        attribute_contents;
    indexes::Neighbor ToIndexesNeighbor() const {
      auto string_interned_external_id = StringInternStore::Intern(external_id);
      auto result = indexes::Neighbor{string_interned_external_id, score};
      if (attribute_contents.has_value()) {
        result.attribute_contents = RecordsMap();
        for (auto &attribute : *attribute_contents) {
          result.attribute_contents->emplace(
              attribute.first,
              RecordsMapValue(vmsdk::MakeUniqueValkeyString(attribute.first),
                              vmsdk::MakeUniqueValkeyString(attribute.second)));
        }
      }
      return result;
    }
    static TestNeighbor FromIndexesNeighbor(const indexes::Neighbor &neighbor) {
      TestNeighbor result;
      result.external_id = std::string(*neighbor.external_id);
      result.score = neighbor.score;
      if (neighbor.attribute_contents.has_value()) {
        result.attribute_contents =
            absl::flat_hash_map<std::string, std::string>();
        for (auto &attribute : *neighbor.attribute_contents) {
          result.attribute_contents->emplace(
              attribute.first,
              vmsdk::ToStringView(attribute.second.value.get()));
        }
      }
      return result;
    }
    bool operator==(const TestNeighbor &other) const {
      if (external_id != other.external_id || score != other.score) {
        return false;
      }
      if (attribute_contents.has_value() !=
          other.attribute_contents.has_value()) {
        return false;
      }
      if (!attribute_contents.has_value()) {
        return true;
      }
      if (attribute_contents->size() != other.attribute_contents->size()) {
        return false;
      }
      for (auto &attribute : *attribute_contents) {
        auto it = other.attribute_contents->find(attribute.first);
        if (it == other.attribute_contents->end() ||
            it->second != attribute.second) {
          return false;
        }
      }
      return true;
    }
  };
  std::string test_name;
  bool no_content;
  std::vector<TestReturnAttribute> return_attributes;
  std::vector<TestIndex> indexes;
  absl::StatusOr<std::vector<TestNeighbor>> input;
  absl::StatusOr<std::vector<TestNeighbor>> expected_output;
};

class IndexedContentTest
    : public ValkeySearchTestWithParam<
          std::tuple<data_model::DistanceMetric, IndexedContentTestCase>> {};

TEST_P(IndexedContentTest, MaybeAddIndexedContentTest) {
  auto index_schema = CreateIndexSchema("test_schema").value();
  auto distance_metric = std::get<0>(GetParam());
  auto test_case = std::get<1>(GetParam());
  for (auto &index : test_case.indexes) {
    std::shared_ptr<indexes::IndexBase> index_base;
    switch (index.indexer_type) {
      case IndexerType::kHNSW: {
        data_model::VectorIndex vector_index_proto = CreateHNSWVectorIndexProto(
            kVectorDimensions, distance_metric, 1000, 10, 300, 30);
        auto vector_index =
            indexes::VectorHNSW<float>::Create(
                vector_index_proto, "attribute_identifier_1",
                data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
                .value();
        VMSDK_EXPECT_OK(index_schema->AddIndex(
            index.attribute_alias, index.attribute_identifier, vector_index));
        index_base = vector_index;
        break;
      }
      case IndexerType::kFlat: {
        data_model::VectorIndex vector_index_proto = CreateFlatVectorIndexProto(
            kVectorDimensions, distance_metric, 1000, 250);
        auto flat_index =
            indexes::VectorFlat<float>::Create(
                vector_index_proto, "attribute_identifier_1",
                data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
                .value();
        VMSDK_EXPECT_OK(index_schema->AddIndex(
            index.attribute_alias, index.attribute_identifier, flat_index));
        index_base = flat_index;
        break;
      }
      case IndexerType::kTag: {
        data_model::TagIndex tag_index_proto;
        tag_index_proto.set_separator(",");
        tag_index_proto.set_case_sensitive(false);
        auto tag_index = std::make_shared<indexes::Tag>(tag_index_proto);
        VMSDK_EXPECT_OK(index_schema->AddIndex(
            index.attribute_alias, index.attribute_identifier, tag_index));
        index_base = tag_index;
        break;
      }
      case IndexerType::kNumeric: {
        data_model::NumericIndex numeric_index_proto;
        auto numeric_index =
            std::make_shared<indexes::Numeric>(numeric_index_proto);
        VMSDK_EXPECT_OK(index_schema->AddIndex(
            index.attribute_alias, index.attribute_identifier, numeric_index));
        index_base = numeric_index;
        break;
      }
      default:
        CHECK(false);
    }
    for (auto &content : index.contents) {
      auto key = StringInternStore::Intern(content.first);
      auto value = content.second;
      VMSDK_EXPECT_OK(index_base->AddRecord(key, value));
    }
  }

  UnitTestSearchParameters parameters;
  parameters.index_schema = index_schema;
  for (auto &attribute : test_case.return_attributes) {
    auto identifier = vmsdk::MakeUniqueValkeyString(attribute.identifier);
    auto alias = vmsdk::MakeUniqueValkeyString(attribute.alias);
    parameters.return_attributes.push_back(query::ReturnAttribute{
        .identifier = std::move(identifier), .alias = std::move(alias)});
  }
  parameters.no_content = test_case.no_content;

  absl::StatusOr<std::vector<indexes::Neighbor>> got;
  if (test_case.input.ok()) {
    absl::StatusOr<std::vector<indexes::Neighbor>> neighbors =
        std::vector<indexes::Neighbor>();
    for (auto &neighbor : test_case.input.value()) {
      neighbors->push_back(neighbor.ToIndexesNeighbor());
    }
    got = query::MaybeAddIndexedContent(std::move(neighbors), parameters);
  } else {
    got = query::MaybeAddIndexedContent(test_case.input.status(), parameters);
  }

  if (!got.ok()) {
    EXPECT_EQ(got.status(), test_case.expected_output.status());
  } else {
    VMSDK_EXPECT_OK(test_case.expected_output);
    EXPECT_EQ(got->size(), test_case.expected_output->size());
#ifndef TESTING_TMP_DISABLED
    for (size_t i = 0; i < got->size(); ++i) {
      EXPECT_EQ(IndexedContentTestCase::TestNeighbor::FromIndexesNeighbor(
                    got.value()[i]),
                test_case.expected_output.value()[i]);
    }
#endif  // TESTING_TMP_DISABLED
  }
}

static const char kTestVector0[401] =
    "00000000000000000000000000000000000000000000000000"
    "00000000000000000000000000000000000000000000000000"
    "00000000000000000000000000000000000000000000000000"
    "00000000000000000000000000000000000000000000000000"
    "00000000000000000000000000000000000000000000000000"
    "00000000000000000000000000000000000000000000000000"
    "00000000000000000000000000000000000000000000000000"
    "00000000000000000000000000000000000000000000000000";

static const char kTestVector1[401] =
    "11111111111111111111111111111111111111111111111111"
    "11111111111111111111111111111111111111111111111111"
    "11111111111111111111111111111111111111111111111111"
    "11111111111111111111111111111111111111111111111111"
    "11111111111111111111111111111111111111111111111111"
    "11111111111111111111111111111111111111111111111111"
    "11111111111111111111111111111111111111111111111111"
    "11111111111111111111111111111111111111111111111111";

INSTANTIATE_TEST_SUITE_P(
    IndexedContentTests, IndexedContentTest,
    testing::Combine(
        testing::Values(data_model::DISTANCE_METRIC_L2,
                        data_model::DISTANCE_METRIC_COSINE),
        testing::ValuesIn<IndexedContentTestCase>(
            {
                {
                    .test_name = "no_return_attributes",
                    .input = {{{.external_id = "1",
                                .score = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{.external_id = "1",
                                          .score = 0.1,
                                          .attribute_contents = std::nullopt}}},
                },
                {
                    .test_name = "all_non_indexed_return_attributes",
                    .return_attributes = {{.identifier = "1", .alias = "a1"},
                                          {.identifier = "2", .alias = "a2"}},
                    .input = {{{.external_id = "1",
                                .score = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{.external_id = "1",
                                          .score = 0.1,
                                          .attribute_contents = std::nullopt}}},
                },
                {
                    .test_name = "some_non_indexed_return_attributes",
                    .return_attributes = {{.identifier = "a1", .alias = "as1"},
                                          {.identifier = "a2", .alias = "as2"}},
                    .indexes =
                        {
                            {
                                .attribute_alias = "a1",
                                .attribute_identifier = "i1",
                                .indexer_type = IndexerType::kTag,
                                .contents = {{"1", "1"}},
                            },
                        },
                    .input = {{{.external_id = "1",
                                .score = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{.external_id = "1",
                                          .score = 0.1,
                                          .attribute_contents = std::nullopt}}},
                },
                {
                    .test_name = "no_content",
                    .no_content = true,
                    .indexes =
                        {
                            {
                                .attribute_alias = "a1",
                                .attribute_identifier = "i1",
                                .indexer_type = IndexerType::kTag,
                                .contents = {{"1", "1"}},
                            },
                        },
                    .input = {{{.external_id = "1",
                                .score = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{.external_id = "1",
                                          .score = 0.1,
                                          .attribute_contents = std::nullopt}}},
                },
                {
                    .test_name = "tag_indexed_return_attributes",
                    .return_attributes = {{.identifier = "a1", .alias = "as1"},
                                          {.identifier = "a2", .alias = "as2"}},
                    .indexes =
                        {
                            {
                                .attribute_alias = "a1",
                                .attribute_identifier = "i1",
                                .indexer_type = IndexerType::kTag,
                                .contents = {{"1", "1"}},
                            },
                            {
                                .attribute_alias = "a2",
                                .attribute_identifier = "i2",
                                .indexer_type = IndexerType::kTag,
                                .contents = {{"1", "2, abc ,ABC    "}},
                            },
                        },
                    .input = {{{.external_id = "1",
                                .score = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{
                        .external_id = "1",
                        .score = 0.1,
                        .attribute_contents =
                            absl::flat_hash_map<std::string, std::string>{
                                {"as1", "1"}, {"as2", "2, abc ,ABC    "}},
                    }}},
                },
                {
                    .test_name = "numeric_indexed_return_attributes",
                    .return_attributes = {{.identifier = "a1", .alias = "as1"},
                                          {.identifier = "a2", .alias = "as2"}},
                    .indexes =
                        {
                            {
                                .attribute_alias = "a1",
                                .attribute_identifier = "i1",
                                .indexer_type = IndexerType::kNumeric,
                                .contents = {{"1", "1.0"}},
                            },
                            {
                                .attribute_alias = "a2",
                                .attribute_identifier = "i2",
                                .indexer_type = IndexerType::kNumeric,
                                .contents = {{"1", "2.0"}},
                            },
                        },
                    .input = {{{.external_id = "1",
                                .score = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{
                        .external_id = "1",
                        .score = 0.1,
                        .attribute_contents =
                            absl::flat_hash_map<std::string, std::string>{
                                {"as1", "1"}, {"as2", "2"}},
                    }}},
                },
                {
                    .test_name = "hnsw_indexed_return_attributes",
                    .return_attributes = {{.identifier = "a1", .alias = "as1"},
                                          {.identifier = "a2", .alias = "as2"}},
                    .indexes =
                        {
                            {
                                .attribute_alias = "a1",
                                .attribute_identifier = "i1",
                                .indexer_type = IndexerType::kHNSW,
                                .contents = {{"1", kTestVector0}},
                            },
                            {
                                .attribute_alias = "a2",
                                .attribute_identifier = "i2",
                                .indexer_type = IndexerType::kHNSW,
                                .contents = {{"1", kTestVector1}},
                            },
                        },
                    .input = {{{.external_id = "1",
                                .score = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{
                        .external_id = "1",
                        .score = 0.1,
                        .attribute_contents =
                            absl::flat_hash_map<std::string, std::string>{
                                {"as1", kTestVector0}, {"as2", kTestVector1}},
                    }}},
                },
                {
                    .test_name = "flat_indexed_return_attributes",
                    .return_attributes = {{.identifier = "a1", .alias = "as1"},
                                          {.identifier = "a2", .alias = "as2"}},
                    .indexes =
                        {
                            {
                                .attribute_alias = "a1",
                                .attribute_identifier = "i1",
                                .indexer_type = IndexerType::kFlat,
                                .contents = {{"1", kTestVector0}},
                            },
                            {
                                .attribute_alias = "a2",
                                .attribute_identifier = "i2",
                                .indexer_type = IndexerType::kFlat,
                                .contents = {{"1", kTestVector1}},
                            },
                        },
                    .input = {{{.external_id = "1",
                                .score = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{
                        .external_id = "1",
                        .score = 0.1,
                        .attribute_contents =
                            absl::flat_hash_map<std::string, std::string>{
                                {"as1", kTestVector0}, {"as2", kTestVector1}},
                    }}},
                },
                {
                    .test_name = "not_ok_input",
                    .return_attributes = {{.identifier = "a1", .alias = "as1"},
                                          {.identifier = "a2", .alias = "as2"}},
                    .input = absl::InternalError("test error"),
                    .expected_output = absl::InternalError("test error"),
                },
                {
                    .test_name = "content_already_exists",
                    .return_attributes = {{.identifier = "a1", .alias = "as1"},
                                          {.identifier = "a2", .alias = "as2"}},
                    .indexes =
                        {
                            {
                                .attribute_alias = "a1",
                                .attribute_identifier = "i1",
                                .indexer_type = IndexerType::kTag,
                                .contents = {{"2", "1"}},
                            },
                            {
                                .attribute_alias = "a2",
                                .attribute_identifier = "i2",
                                .indexer_type = IndexerType::kTag,
                                .contents = {{"2", "2"}},
                            },
                        },
                    .input = {{{.external_id = "1",
                                .score = 0.1,
                                .attribute_contents = absl::flat_hash_map<
                                    std::string, std::string>{{"as1", "1"},
                                                              {"as2", "2"}}},
                               {.external_id = "2",
                                .score = 0.2,
                                .attribute_contents = std::nullopt}}},
                    .expected_output =
                        {{{
                              .external_id = "1",
                              .score = 0.1,
                              .attribute_contents =
                                  absl::flat_hash_map<std::string, std::string>{
                                      {"as1", "1"}, {"as2", "2"}},
                          },
                          {.external_id = "2",
                           .score = 0.2,
                           .attribute_contents =
                               absl::flat_hash_map<std::string, std::string>{
                                   {"as1", "1"}, {"as2", "2"}}}}},
                },
                {
                    .test_name = "index_content_not_exists",
                    .return_attributes = {{.identifier = "a1", .alias = "as1"},
                                          {.identifier = "a2", .alias = "as2"}},
                    .indexes =
                        {
                            {
                                .attribute_alias = "a1",
                                .attribute_identifier = "i1",
                                .indexer_type = IndexerType::kTag,
                                .contents = {{"1", "1"}},
                            },
                            {
                                .attribute_alias = "a2",
                                .attribute_identifier = "i2",
                                .indexer_type = IndexerType::kTag,
                                .contents = {{"1", "2"}, {"2", "2"}},
                            },
                        },
                    .input = {{{.external_id = "1",
                                .score = 0.1,
                                .attribute_contents = std::nullopt},
                               {.external_id = "2",
                                .score = 0.2,
                                .attribute_contents = std::nullopt}}},
                    .expected_output =
                        {{{
                              .external_id = "1",
                              .score = 0.1,
                              .attribute_contents =
                                  absl::flat_hash_map<std::string, std::string>{
                                      {"as1", "1"}, {"as2", "2"}},
                          },
                          {.external_id = "2",
                           .score = 0.2,
                           .attribute_contents = std::nullopt}}},
                },
            })),
    [](const TestParamInfo<IndexedContentTest::ParamType> &info) {
      std::string distance_metric =
          std::get<0>(info.param) == data_model::DISTANCE_METRIC_L2 ? "L2"
                                                                    : "COSINE";
      std::string test_name =
          absl::StrCat(distance_metric, "_", std::get<1>(info.param).test_name);
      return test_name;
    });

class ScoreTextQueryTestBase : public ValkeySearchTest {
 protected:
  // Schema with a text field "text", a case-insensitive tag field "color", and
  // a numeric field "rating", so ScoreTextQuery sees real posting lists and a
  // non-zero corpus. Each doc is (key, text, color); an empty color leaves the
  // doc untracked by the tag index, and the numeric field needs no records
  // (ScoreNode's numeric case returns 0 without touching the index).
  std::shared_ptr<MockIndexSchema> BuildTextTagSchema(
      const std::vector<std::tuple<std::string, std::string, std::string>>
          &docs) {
    auto schema = CreateIndexSchema(kIndexSchemaName).value();
    EXPECT_CALL(*schema, GetIdentifier(::testing::_))
        .Times(::testing::AnyNumber());
    schema->CreateTextIndexSchema();
    auto text_schema = schema->GetTextIndexSchema();
    auto text = std::make_shared<indexes::Text>(
        CreateTextIndexProto(/*with_suffix_trie=*/true, /*no_stem=*/true, 1.0),
        text_schema);
    VMSDK_EXPECT_OK(schema->AddIndex("text", "text", text));
    auto tag = std::make_shared<indexes::Tag>(
        CreateTagIndexProto(/*separator=*/",", /*case_sensitive=*/false));
    VMSDK_EXPECT_OK(schema->AddIndex("color", "color", tag));
    // A numeric field so text+numeric composition can be scored. ScoreNode's
    // kNumeric case returns 0 without touching the index (the pre-filter admits
    // range membership), so the numeric index needs no records for scoring.
    auto numeric =
        std::make_shared<indexes::Numeric>(CreateNumericIndexProto());
    VMSDK_EXPECT_OK(schema->AddIndex("rating", "rating", numeric));
    for (const auto &[k, content, color] : docs) {
      auto key = StringInternStore::Intern(k);
      VMSDK_EXPECT_OK(text->AddRecord(key, content));
      text_schema->CommitKeyData(key);
      if (!color.empty()) {
        VMSDK_EXPECT_OK(tag->AddRecord(key, color));
      }
      schema->SetIndexMutationSequenceNumber(key, 0);
    }
    return schema;
  }

  // Schema with ONLY a case-insensitive tag field "color" (no text field), for
  // the degenerate text-less case. Each doc is (key, color).
  std::shared_ptr<MockIndexSchema> BuildTagOnlySchema(
      const std::vector<std::pair<std::string, std::string>> &docs) {
    auto schema = CreateIndexSchema(kIndexSchemaName).value();
    EXPECT_CALL(*schema, GetIdentifier(::testing::_))
        .Times(::testing::AnyNumber());
    auto tag = std::make_shared<indexes::Tag>(
        CreateTagIndexProto(/*separator=*/",", /*case_sensitive=*/false));
    VMSDK_EXPECT_OK(schema->AddIndex("color", "color", tag));
    for (const auto &[k, color] : docs) {
      auto key = StringInternStore::Intern(k);
      VMSDK_EXPECT_OK(tag->AddRecord(key, color));
      schema->SetIndexMutationSequenceNumber(key, 0);
    }
    return schema;
  }

  // Score `key` against `filter`; nullopt when the predicate did not match.
  std::optional<float> Score(MockIndexSchema &schema, absl::string_view filter,
                             const std::string &key) {
    TextParsingOptions options{};
    auto parsed = FilterParser(schema, filter, options).Parse();
    EXPECT_TRUE(parsed.ok()) << parsed.status();
    auto interned = StringInternStore::Intern(key);
    std::vector<indexes::BorrowedNeighbor> cands{
        {BorrowedInternedStringPtr(interned), 0.0f, 0.0f}};
    vmsdk::ReaderMutexLock lock(&schema.GetTimeSlicedMutex());
    query::ScoreTextQuery(
        schema, parsed.value().root_predicate.get(),
        indexes::scoring::GetScorer(indexes::scoring::ScorerType::kBm25Std),
        cands);
    if (cands.empty()) return std::nullopt;
    return cands[0].score;
  }

  // Score `key` for `filter` through the IN-ITERATOR path
  // (TextIterator::GetScore), which is the path pure-text prefix/suffix/fuzzy
  // queries run on -- unlike Score() above, which uses the extra-step
  // ScoreTextQuery. Mirrors DoSearchNonVector's per-key scoring
  // (GetScore() * GetWeight()). `filter` must be a single text predicate.
  // Returns nullopt when the document does not match.
  std::optional<float> ScoreViaIterator(MockIndexSchema &schema,
                                        absl::string_view filter,
                                        const std::string &key) {
    TextParsingOptions options{};
    auto parsed = FilterParser(schema, filter, options).Parse();
    EXPECT_TRUE(parsed.ok()) << parsed.status();
    auto *text_pred = dynamic_cast<query::TextPredicate *>(
        parsed.value().root_predicate.get());
    EXPECT_NE(text_pred, nullptr) << "not a single text predicate: " << filter;
    if (text_pred == nullptr) return std::nullopt;
    text_pred->SetScorer(
        indexes::scoring::GetScorer(indexes::scoring::ScorerType::kBm25Std));
    auto text_index = schema.GetTextIndexSchema()->GetTextIndex();
    auto interned = StringInternStore::Intern(key);
    vmsdk::ReaderMutexLock lock(&schema.GetTimeSlicedMutex());
    auto iter =
        text_pred->BuildTextIterator(text_index, text_pred->GetFieldMask(),
                                     /*require_positions=*/false);
    if (!iter->SeekForwardKey(interned) || iter->DoneKeys())
      return std::nullopt;
    if (iter->CurrentKey()->Str() != key) return std::nullopt;
    return iter->GetScore() * iter->GetWeight();
  }
};

// Every case has the same shape: score `filter` against `key`, compare to a
// linear combination of baseline single-filter scores on the same key.
// `expected` receives the baseline scores in the order they appear in
// `baselines`. Optional `zero_score_keys` asserts the filter retains those
// keys (already admitted by the pre-filter, but re-deriving a non-match here)
// with a zero score rather than dropping them. Docs are (key, text, color);
// pass "" for either field a case does not use.
struct ScoreCase {
  std::string test_name;
  std::vector<std::tuple<std::string, std::string, std::string>> docs;
  std::string key;
  std::vector<std::string> baselines;
  std::string filter;
  std::function<float(const std::vector<float> &)> expected;
  std::vector<std::string> zero_score_keys{};
};

class ScoreTextQueryTest : public ScoreTextQueryTestBase,
                           public testing::WithParamInterface<ScoreCase> {};

TEST_P(ScoreTextQueryTest, ScoreMatchesFormula) {
  const auto &c = GetParam();
  auto schema = BuildTextTagSchema(c.docs);
  std::vector<float> bases;
  for (const auto &f : c.baselines) {
    auto s = Score(*schema, f, c.key);
    ASSERT_TRUE(s.has_value()) << "baseline did not match: " << f;
    bases.push_back(*s);
  }
  auto got = Score(*schema, c.filter, c.key);
  ASSERT_TRUE(got.has_value()) << "filter did not match: " << c.filter;
  EXPECT_NEAR(*got, c.expected(bases), 1e-4f);
  for (const auto &k : c.zero_score_keys) {
    auto s = Score(*schema, c.filter, k);
    ASSERT_TRUE(s.has_value())
        << "candidate dropped instead of retained: " << k;
    EXPECT_EQ(*s, 0.0f) << "expected zero score for non-matching key: " << k;
  }
}

INSTANTIATE_TEST_SUITE_P(
    ScoreTextQueryTests, ScoreTextQueryTest,
    ValuesIn<ScoreCase>({
        // --- Text leaf weight + AND/OR composition ---
        // Leaf weight scales the score linearly.
        {.test_name = "text_weight_0_1",
         .docs = {{"key1", "hello world", ""}},
         .key = "key1",
         .baselines = {"@text:hello"},
         .filter = "(@text:hello) => { $weight: 0.1; }",
         .expected = [](const auto &b) { return 0.1f * b[0]; }},
        {.test_name = "text_weight_100",
         .docs = {{"key1", "hello world", ""}},
         .key = "key1",
         .baselines = {"@text:hello"},
         .filter = "(@text:hello) => { $weight: 100; }",
         .expected = [](const auto &b) { return 100.0f * b[0]; }},
        // AND with default weight sums matching children; a doc missing a child
        // term re-derives a non-match here, but stays admitted with a zero
        // score
        // rather than being dropped.
        {.test_name = "AndPredicateScoreSumsChildWeights",
         .docs = {{"key1", "hello world", ""}, {"key2", "hello there", ""}},
         .key = "key1",
         .baselines = {"@text:hello", "@text:world"},
         .filter = "@text:hello @text:world",
         .expected = [](const auto &b) { return b[0] + b[1]; },
         .zero_score_keys = {"key2"}},
        {.test_name = "AndPredicateOwnWeightMultipliesSum",
         .docs = {{"key1", "hello world", ""}},
         .key = "key1",
         .baselines = {"@text:hello @text:world"},
         .filter = "(@text:hello @text:world) => { $weight: 4.0; }",
         .expected = [](const auto &b) { return 4.0f * b[0]; }},
        {.test_name = "AndDefaultWeightChildrenSumToCount",
         .docs = {{"key1", "hello brave new world", ""}},
         .key = "key1",
         .baselines = {"@text:hello", "@text:brave", "@text:world"},
         .filter = "@text:hello @text:brave @text:world",
         .expected = [](const auto &b) { return b[0] + b[1] + b[2]; }},
        {.test_name = "OrPredicateDefaultWeightChildrenSum",
         .docs = {{"key1", "hello world", ""}},
         .key = "key1",
         .baselines = {"@text:hello", "@text:world"},
         .filter = "@text:hello | @text:world",
         .expected = [](const auto &b) { return b[0] + b[1]; }},
        {.test_name = "NestedAndWeightsComposeMultiplicatively",
         .docs = {{"key1", "hello brave new world", ""}},
         .key = "key1",
         .baselines = {"@text:hello", "@text:brave", "@text:world"},
         .filter = "(@text:hello @text:brave) => { $weight: 4.0; } @text:world",
         .expected = [](const auto &b) { return 4.0f * (b[0] + b[1]) + b[2]; }},
        // Negation is a filter, not a scoring clause: it contributes 0.
        {.test_name = "NegateContributesZero",
         .docs = {{"key1", "hello world", ""}},
         .key = "key1",
         .baselines = {"@text:hello"},
         .filter = "@text:hello -@text:missing",
         .expected = [](const auto &b) { return b[0]; }},
        // Same, even for a doc that contains the negated term: candidate
        // filtering happens before scoring, so ScoreNode still adds zero for
        // the negation.
        {.test_name = "NegateDoesNotInflateEnclosingAnd",
         .docs = {{"key1", "hello world", ""}, {"key2", "hello there", ""}},
         .key = "key2",
         .baselines = {"@text:hello"},
         .filter = "@text:hello -@text:there",
         .expected = [](const auto &b) { return b[0]; }},

        // --- Numeric: a filter, never a ranker (contributes 0) ---
        // Adding a numeric clause to a text query changes nothing.
        {.test_name = "NumericClauseAddsZeroToText",
         .docs = {{"key1", "hello world", ""}},
         .key = "key1",
         .baselines = {"@text:hello"},
         .filter = "@text:hello @rating:[0 100]",
         .expected = [](const auto &b) { return b[0]; }},
        // The enclosing group weight multiplies the text term; numeric adds 0.
        {.test_name = "NumericInWeightedGroupContributesZero",
         .docs = {{"key1", "hello world", ""}},
         .key = "key1",
         .baselines = {"@text:hello"},
         .filter = "(@text:hello @rating:[0 100]) => { $weight: 4.0; }",
         .expected = [](const auto &b) { return 4.0f * b[0]; }},

        // --- Tag: BM25 term with F ≡ 1, IDF over per-tag-value doc count ---
        // $weight scales the tag term linearly.
        {.test_name = "TagWeightScalesTerm",
         .docs = {{"d1", "aa bb", "red"}, {"d2", "aa bb", "blue"}},
         .key = "d1",
         .baselines = {"@color:{red}"},
         .filter = "(@color:{red}) => { $weight: 3.0; }",
         .expected = [](const auto &b) { return 3.0f * b[0]; }},
        // Text + tag AND sums the text term and the tag term.
        {.test_name = "TextAndTagTermsSum",
         .docs = {{"d1", "hello world", "red"}, {"d2", "hello there", "blue"}},
         .key = "d1",
         .baselines = {"@text:hello", "@color:{red}"},
         .filter = "@text:hello @color:{red}",
         .expected = [](const auto &b) { return b[0] + b[1]; }},
        // A union {red|blue} on a doc carrying both sums both value terms.
        {.test_name = "TagUnionSumsMatchedValues",
         .docs = {{"d1", "aa bb", "red"},
                  {"d2", "aa bb", "blue"},
                  {"d3", "aa bb", "red,blue"}},
         .key = "d3",
         .baselines = {"@color:{red}", "@color:{blue}"},
         .filter = "@color:{red|blue}",
         .expected = [](const auto &b) { return b[0] + b[1]; }},
        // A union of values that collapse to the same tag under the index's
        // case rules ({red|Red} on a case-insensitive index) must score the
        // value ONCE, not once per query spelling.
        {.test_name = "TagUnionCaseVariantsScoreOnce",
         .docs = {{"d1", "aa bb", "red"}, {"d2", "aa bb", "blue"}},
         .key = "d1",
         .baselines = {"@color:{red}"},
         .filter = "@color:{red|Red}",
         .expected = [](const auto &b) { return b[0]; }},
    }),
    [](const TestParamInfo<ScoreCase> &info) { return info.param.test_name; });

// --- Tag scoring: relationships not expressible as a same-key formula --------
//
// The formula-shaped cases ($weight, union sum, text+tag sum, numeric adds 0)
// live in the ScoreTextQueryTest param suite above. These remaining tests
// compare scores ACROSS keys / schemas, so they stay as standalone TEST_Fs on
// the shared ScoreTextQueryTestBase fixture. Semantics:
// docs/redis_numeric_tag_scoring.md.

// A rarer tag value scores higher (IDF). Text length is held equal across docs,
// so ordering is driven purely by tag-value document frequency:
// red -> 3 docs, blue -> 2, green -> 1, hence IDF(green) > IDF(blue) >
// IDF(red).
TEST_F(ScoreTextQueryTestBase, RarerTagValueScoresHigher) {
  auto schema = BuildTextTagSchema({
      {"d1", "aa bb", "red"},
      {"d2", "aa bb", "red,blue"},
      {"d3", "aa bb", "red,blue,green"},
  });
  auto red = Score(*schema, "@color:{red}", "d1");
  auto blue = Score(*schema, "@color:{blue}", "d2");
  auto green = Score(*schema, "@color:{green}", "d3");
  ASSERT_TRUE(red && blue && green);
  EXPECT_GT(*green, *blue);
  EXPECT_GT(*blue, *red);
  EXPECT_GT(*red, 0.0f);
}

// Tag term frequency is NOT counted (F ≡ 1): a value repeated within a document
// scores the same as a single occurrence. doc_len also counts TEXT tokens only,
// not tags, so the same holds for differing tag counts. Text is held equal.
TEST_F(ScoreTextQueryTestBase, TagFrequencyAndTagCountDoNotAffectScore) {
  auto schema = BuildTextTagSchema({
      {"d1", "aa bb", "red"},
      {"d2", "aa bb", "red,red,red"},
      {"d3", "aa bb", "red,blue,green,yellow,purple"},
  });
  auto single = Score(*schema, "@color:{red}", "d1");
  auto repeated = Score(*schema, "@color:{red}", "d2");   // F still 1
  auto many_tags = Score(*schema, "@color:{red}", "d3");  // doc_len still 2
  ASSERT_TRUE(single && repeated && many_tags);
  EXPECT_FLOAT_EQ(*single, *repeated);
  EXPECT_FLOAT_EQ(*single, *many_tags);
}

// Shorter TEXT gets a mild boost on its tag term (doc-length normalization uses
// the TEXT length).
TEST_F(ScoreTextQueryTestBase, ShorterTextBoostsTagTerm) {
  auto schema = BuildTextTagSchema({
      {"d1", "aa", "red"},           // text length 1
      {"d2", "aa bb cc dd", "red"},  // text length 4
  });
  auto short_doc = Score(*schema, "@color:{red}", "d1");
  auto long_doc = Score(*schema, "@color:{red}", "d2");
  ASSERT_TRUE(short_doc && long_doc);
  EXPECT_GT(*short_doc, *long_doc);
}

// On a text-less index every doc has TEXT length 0, so avg_doc_len is 0 and the
// scorer returns a well-defined 0 — NOT Redis's nan. The candidate is kept
// (matched), just contributes nothing to relevance.
TEST_F(ScoreTextQueryTestBase, TextLessIndexScoresZeroNotNan) {
  auto schema = BuildTagOnlySchema({{"d1", "red"}, {"d2", "blue"}});
  auto score = Score(*schema, "@color:{red}", "d1");
  ASSERT_TRUE(score.has_value());
  EXPECT_FALSE(std::isnan(*score));
  EXPECT_FLOAT_EQ(*score, 0.0f);
}

// The recompute path (SingleDocumentScorer) must land on the same scale as the
// shard-side extra-step path (ScoreTextQuery) — both walk the same ScoreNode.
// Pinned at a real NON-ZERO value (text + tag terms) so a magnitude divergence
// in either path is caught; the numeric clause adds 0 and must not perturb it.
TEST_F(ScoreTextQueryTestBase, RecomputePathMatchesExtraStepAtNonZero) {
  auto schema = BuildTextTagSchema({
      {"d1", "hello world", "red"},
      {"d2", "hello there", "blue"},
  });
  const auto *scorer =
      indexes::scoring::GetScorer(indexes::scoring::ScorerType::kBm25Std);
  const std::string filter = "@text:hello @color:{red} @rating:[0 100]";

  // Extra-step path: Score() takes the reader lock internally.
  auto extra_step = Score(*schema, filter, "d1");
  ASSERT_TRUE(extra_step.has_value());
  EXPECT_GT(*extra_step, 0.0f);

  // Recompute path: SingleDocumentScorer takes the lock itself, so construct
  // and call it WITHOUT the reader lock held.
  TextParsingOptions options{};
  auto parsed = FilterParser(*schema, filter, options).Parse();
  ASSERT_TRUE(parsed.ok()) << parsed.status();
  query::SingleDocumentScorer document_scorer(
      *schema, parsed.value().root_predicate.get(), scorer);
  auto recomputed = document_scorer.Score(StringInternStore::Intern("d1"));
  ASSERT_TRUE(recomputed.has_value());
  EXPECT_FLOAT_EQ(*recomputed, *extra_step);
}

// --- Prefix / suffix / fuzzy expansion scoring (in-iterator path) ------------
//
// Contract (docs/redis_prefix_suffix_fuzzy_scoring.md): an expansion
// contributes exactly ONE matched term's BM25 (its own IDF + own F), never the
// sum over matched terms. A doc matching a single expansion term therefore
// scores identically to the exact-term query for that term.

// A doc matching the prefix via a single term scores like the exact term.
TEST_F(ScoreTextQueryTestBase, PrefixSingleMatchEqualsExactTerm) {
  auto schema = BuildTextTagSchema({
      {"d_cat", "cat", ""},
      {"d_cats", "cats", ""},
      {"d_dog", "dog", ""},
  });
  auto prefix = ScoreViaIterator(*schema, "@text:cat*", "d_cat");
  auto exact = ScoreViaIterator(*schema, "@text:cat", "d_cat");
  ASSERT_TRUE(prefix && exact);
  EXPECT_GT(*prefix, 0.0f);
  EXPECT_FLOAT_EQ(*prefix, *exact);
}

// A doc matching the prefix via SEVERAL terms is scored on ONE of them, never
// their sum (category df=3, catalog df=1 -> distinct IDFs, so the pick is
// observable). Which term wins is an unspecified union artifact, so assert only
// the invariant: score == one candidate's BM25 and strictly below their sum.
TEST_F(ScoreTextQueryTestBase, PrefixMultiMatchScoresOneTermNotSum) {
  auto schema = BuildTextTagSchema({
      {"d_cat", "cat", ""},
      {"d_multi", "category catalog", ""},
      {"d_cat2", "category", ""},
      {"d_cat3", "category", ""},
  });
  auto prefix = ScoreViaIterator(*schema, "@text:cat*", "d_multi");
  auto only_category = ScoreViaIterator(*schema, "@text:category", "d_multi");
  auto only_catalog = ScoreViaIterator(*schema, "@text:catalog", "d_multi");
  ASSERT_TRUE(prefix && only_category && only_catalog);
  EXPECT_LT(*prefix, *only_category + *only_catalog);
  EXPECT_TRUE(std::fabs(*prefix - *only_category) < 1e-4f ||
              std::fabs(*prefix - *only_catalog) < 1e-4f)
      << "prefix=" << *prefix << " category=" << *only_category
      << " catalog=" << *only_catalog;
}

// A doc matching the suffix via a single term scores like the exact term.
TEST_F(ScoreTextQueryTestBase, SuffixSingleMatchEqualsExactTerm) {
  auto schema = BuildTextTagSchema({
      {"d_run", "running", ""},
      {"d_jog", "jogging", ""},
      {"d_dog", "dog", ""},
  });
  auto suffix = ScoreViaIterator(*schema, "@text:*ing", "d_run");
  auto exact = ScoreViaIterator(*schema, "@text:running", "d_run");
  ASSERT_TRUE(suffix && exact);
  EXPECT_GT(*suffix, 0.0f);
  EXPECT_FLOAT_EQ(*suffix, *exact);
}

// A doc matching the fuzzy pattern via a single term scores like the exact
// term.
TEST_F(ScoreTextQueryTestBase, FuzzySingleMatchEqualsExactTerm) {
  auto schema = BuildTextTagSchema({
      {"d_cat", "cat", ""},
      {"d_dog", "dog", ""},
      {"d_bird", "bird", ""},
  });
  auto fuzzy = ScoreViaIterator(*schema, "@text:%cat%", "d_cat");
  auto exact = ScoreViaIterator(*schema, "@text:cat", "d_cat");
  ASSERT_TRUE(fuzzy && exact);
  EXPECT_GT(*fuzzy, 0.0f);
  EXPECT_FLOAT_EQ(*fuzzy, *exact);
}

// --- Expansion scoring: extra-step path (ScoreTextQuery / ScoreNode) ---------
//
// The tests above drive the in-iterator path (pure-text queries). Score() below
// always takes the extra-step ScoreNode path -- the one combined (text +
// numeric/tag/negate) queries use. Same contract: one matched term, not the
// sum. The representative pick (first present expansion term) can differ from
// the in-iterator heap pick on multi-match docs, so cross-path equality is
// asserted only on single-match docs.

TEST_F(ScoreTextQueryTestBase, ExtraStepPrefixSingleMatchEqualsExactTerm) {
  auto schema = BuildTextTagSchema({
      {"d_cat", "cat", ""},
      {"d_cats", "cats", ""},
      {"d_dog", "dog", ""},
  });
  auto prefix = Score(*schema, "@text:cat*", "d_cat");
  auto exact = Score(*schema, "@text:cat", "d_cat");
  ASSERT_TRUE(prefix && exact);
  EXPECT_GT(*prefix, 0.0f);
  EXPECT_FLOAT_EQ(*prefix, *exact);
  // Single-match: extra-step and in-iterator pick the same (only) term.
  auto in_iter = ScoreViaIterator(*schema, "@text:cat*", "d_cat");
  ASSERT_TRUE(in_iter);
  EXPECT_FLOAT_EQ(*prefix, *in_iter);
}

TEST_F(ScoreTextQueryTestBase, ExtraStepPrefixMultiMatchScoresOneTermNotSum) {
  auto schema = BuildTextTagSchema({
      {"d_cat", "cat", ""},
      {"d_multi", "category catalog", ""},
      {"d_cat2", "category", ""},
      {"d_cat3", "category", ""},
  });
  auto prefix = Score(*schema, "@text:cat*", "d_multi");
  auto only_category = Score(*schema, "@text:category", "d_multi");
  auto only_catalog = Score(*schema, "@text:catalog", "d_multi");
  ASSERT_TRUE(prefix && only_category && only_catalog);
  EXPECT_LT(*prefix, *only_category + *only_catalog);
  EXPECT_TRUE(std::fabs(*prefix - *only_category) < 1e-4f ||
              std::fabs(*prefix - *only_catalog) < 1e-4f)
      << "prefix=" << *prefix << " category=" << *only_category
      << " catalog=" << *only_catalog;
}

TEST_F(ScoreTextQueryTestBase, ExtraStepSuffixSingleMatchEqualsExactTerm) {
  auto schema = BuildTextTagSchema({
      {"d_run", "running", ""},
      {"d_jog", "jogging", ""},
      {"d_dog", "dog", ""},
  });
  auto suffix = Score(*schema, "@text:*ing", "d_run");
  auto exact = Score(*schema, "@text:running", "d_run");
  ASSERT_TRUE(suffix && exact);
  EXPECT_GT(*suffix, 0.0f);
  EXPECT_FLOAT_EQ(*suffix, *exact);
}

TEST_F(ScoreTextQueryTestBase, ExtraStepFuzzySingleMatchEqualsExactTerm) {
  auto schema = BuildTextTagSchema({
      {"d_cat", "cat", ""},
      {"d_dog", "dog", ""},
      {"d_bird", "bird", ""},
  });
  auto fuzzy = Score(*schema, "@text:%cat%", "d_cat");
  auto exact = Score(*schema, "@text:cat", "d_cat");
  ASSERT_TRUE(fuzzy && exact);
  EXPECT_GT(*fuzzy, 0.0f);
  EXPECT_FLOAT_EQ(*fuzzy, *exact);
}

// The real reason the extra-step path matters: a prefix combined with a numeric
// clause. The numeric contributes 0, so the combined score equals the prefix
// alone -- and must be non-zero (the expansion IS scored here, unlike before).
TEST_F(ScoreTextQueryTestBase, ExtraStepPrefixInCombinedQueryScored) {
  auto schema = BuildTextTagSchema({
      {"d_cat", "cat", ""},
      {"d_cats", "cats", ""},
  });
  auto combined = Score(*schema, "@text:cat* @rating:[0 100]", "d_cat");
  auto prefix_only = Score(*schema, "@text:cat*", "d_cat");
  ASSERT_TRUE(combined && prefix_only);
  EXPECT_GT(*combined, 0.0f);
  EXPECT_FLOAT_EQ(*combined, *prefix_only);
}

// --- Tag prefix expansion scoring (extra-step path) --------------------------
//
// A tag prefix (`@color:{re*}`) is scored like a text expansion: it contributes
// exactly ONE matched value's BM25 (F ≡ 1, its own IDF), never the sum over the
// values it expands to -- while an explicit union (`{red|reef}`) still sums.
// Verified empirically against Redis 8.6 (tag prefix scores a single
// representative value; a union sums its members).

// A doc whose only matching value is one tag scores like the exact-value query.
TEST_F(ScoreTextQueryTestBase, TagPrefixSingleMatchEqualsExactValue) {
  auto schema = BuildTextTagSchema({
      {"d_red", "aa", "red"},
      {"d_red2", "aa", "red"},
      {"d_reef", "aa", "reef"},
  });
  auto prefix = Score(*schema, "@color:{re*}", "d_red");
  auto exact = Score(*schema, "@color:{red}", "d_red");
  ASSERT_TRUE(prefix && exact);
  EXPECT_GT(*prefix, 0.0f);
  EXPECT_FLOAT_EQ(*prefix, *exact);
}

// A doc carrying SEVERAL values matching the prefix is scored on exactly ONE of
// them, never their sum (red df=3, reef df=1 -> distinct IDFs, so the pick is
// observable). Which value wins is unspecified, so assert only the invariant:
// score == one candidate value's BM25 and strictly below the union of both.
TEST_F(ScoreTextQueryTestBase, TagPrefixMultiMatchScoresOneValueNotSum) {
  auto schema = BuildTextTagSchema({
      {"d_red", "aa", "red"},
      {"d_red2", "aa", "red"},
      {"d_multi", "aa", "red,reef"},
  });
  auto prefix = Score(*schema, "@color:{re*}", "d_multi");
  auto only_red = Score(*schema, "@color:{red}", "d_multi");
  auto only_reef = Score(*schema, "@color:{reef}", "d_multi");
  auto both = Score(*schema, "@color:{red|reef}", "d_multi");
  ASSERT_TRUE(prefix && only_red && only_reef && both);
  EXPECT_FLOAT_EQ(*both, *only_red + *only_reef);  // union sums
  EXPECT_LT(*prefix, *both);                       // prefix picks one
  EXPECT_TRUE(std::fabs(*prefix - *only_red) < 1e-4f ||
              std::fabs(*prefix - *only_reef) < 1e-4f)
      << "prefix=" << *prefix << " red=" << *only_red << " reef=" << *only_reef;
}

// A tag prefix combined with a numeric clause is scored (the numeric adds 0),
// proving the expansion is not silently dropped on the combined-query path.
TEST_F(ScoreTextQueryTestBase, TagPrefixInCombinedQueryScored) {
  auto schema = BuildTextTagSchema({
      {"d_red", "aa", "red"},
      {"d_reef", "aa", "reef"},
  });
  auto combined = Score(*schema, "@color:{re*} @rating:[0 100]", "d_red");
  auto prefix_only = Score(*schema, "@color:{re*}", "d_red");
  ASSERT_TRUE(combined && prefix_only);
  EXPECT_GT(*combined, 0.0f);
  EXPECT_FLOAT_EQ(*combined, *prefix_only);
}

}  // namespace
}  // namespace valkey_search
