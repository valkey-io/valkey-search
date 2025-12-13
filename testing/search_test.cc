/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/search.h"

#include <cstddef>
#include <cstdint>
#include <deque>
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
#include "src/indexes/tag.h"
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
  MOCK_METHOD(std::unique_ptr<indexes::Tag::EntriesFetcher>, Search,
              (const query::TagPredicate &predicate, bool negate),
              (const, override));
};

class TestedTagEntriesFetcher : public indexes::Tag::EntriesFetcher {
 public:
  TestedTagEntriesFetcher(
      size_t size,
      PatriciaTree<InternedStringPtr, InternedStringPtrHash,
                   InternedStringPtrEqual> &tree,
      absl::flat_hash_set<PatriciaNode<InternedStringPtr, InternedStringPtrHash,
                                       InternedStringPtrEqual> *> &entries,
      bool negate, InternedStringSet &untracked_keys)
      : indexes::Tag::EntriesFetcher(tree, entries, size, negate,
                                     untracked_keys),
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
  std::unordered_set<size_t> fetcher_ids;
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
  indexes::Numeric::EntriesRange entries_range;

  EXPECT_CALL(*numeric_index_100_10, Search(_, false))
      .WillRepeatedly(Return(ByMove(
          std::make_unique<TestedNumericEntriesFetcher>(entries_range, 10))));
  EXPECT_CALL(*numeric_index_100_10, Search(_, true))
      .WillRepeatedly(Return(ByMove(
          std::make_unique<TestedNumericEntriesFetcher>(entries_range, 90))));
  EXPECT_CALL(*numeric_index_100_30, Search(_, false))
      .WillRepeatedly(Return(ByMove(
          std::make_unique<TestedNumericEntriesFetcher>(entries_range, 30))));
  EXPECT_CALL(*numeric_index_100_30, Search(_, true))
      .WillRepeatedly(Return(ByMove(
          std::make_unique<TestedNumericEntriesFetcher>(entries_range, 70))));

  data_model::TagIndex tag_index_proto;
  tag_index_proto.set_separator(",");
  tag_index_proto.set_case_sensitive(false);
  auto tag_index_100_15 = std::make_shared<MockTag>(tag_index_proto);

  VMSDK_EXPECT_OK(index_schema->AddIndex("tag_index_100_15", "tag_index_100_15",
                                         tag_index_100_15));
  PatriciaTree<InternedStringPtr, InternedStringPtrHash, InternedStringPtrEqual>
      tree(false);
  absl::flat_hash_set<PatriciaNode<InternedStringPtr, InternedStringPtrHash,
                                   InternedStringPtrEqual> *>
      entries;
  InternedStringSet untracked_keys;
  EXPECT_CALL(*tag_index_100_15, Search(_, false))
      .WillRepeatedly(Return(ByMove(std::make_unique<TestedTagEntriesFetcher>(
          15, tree, entries, false, untracked_keys))));
  EXPECT_CALL(*tag_index_100_15, Search(_, true))
      .WillRepeatedly(Return(ByMove(std::make_unique<TestedTagEntriesFetcher>(
          85, tree, entries, false, untracked_keys))));
}

TEST_P(EvaluateFilterAsPrimaryTest, ParseParams) {
  const EvaluateFilterAsPrimaryTestCase &test_case = GetParam();
  auto index_schema = CreateIndexSchema(kIndexSchemaName).value();
  InitIndexSchema(index_schema.get());
  FilterParser parser(*index_schema, test_case.filter);
  auto filter_parse_results = parser.Parse();
  std::queue<std::unique_ptr<indexes::EntriesFetcherBase>> entries_fetchers;
  EXPECT_EQ(
      EvaluateFilterAsPrimary(filter_parse_results.value().root_predicate.get(),
                              entries_fetchers, false),
      test_case.evaluate_size);

  EXPECT_EQ(entries_fetchers.size(), test_case.fetcher_ids.size());
  while (!entries_fetchers.empty()) {
    auto entry_fetcher = std::move(entries_fetchers.front());
    entries_fetchers.pop();
    auto numeric_fetcher =
        dynamic_cast<const TestedNumericEntriesFetcher *>(entry_fetcher.get());
    if (numeric_fetcher) {
      EXPECT_TRUE(test_case.fetcher_ids.contains(numeric_fetcher->GetId()));
    } else {
      auto tag_fetcher =
          dynamic_cast<const TestedTagEntriesFetcher *>(entry_fetcher.get());
      if (tag_fetcher) {
        EXPECT_TRUE(test_case.fetcher_ids.contains(tag_fetcher->GetId()));
      } else {
        FAIL();
      }
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    EvaluateFilterAsPrimaryTests, EvaluateFilterAsPrimaryTest,
    ValuesIn<EvaluateFilterAsPrimaryTestCase>({
        {
            .test_name = "single_numeric_10",
            .filter = "@numeric_index_100_10:[1.0 2.0]",
            .evaluate_size = 10,
            .fetcher_ids = {10},
        },
        {
            .test_name = "single_numeric_30",
            .filter = "@numeric_index_100_30:[1.0 2.0]",
            .evaluate_size = 30,
            .fetcher_ids = {30},
        },
        {
            .test_name = "two_numerics_and",
            .filter = "@numeric_index_100_30:[1.0 2.0] "
                      "@numeric_index_100_10:[3.0 4.0]",
            .evaluate_size = 10,
            .fetcher_ids = {10},
        },
        {
            .test_name = "two_numerics_or",
            .filter = "@numeric_index_100_30:[1.0 2.0] |"
                      "@numeric_index_100_10:[3.0 4.0]",
            .evaluate_size = 40,
            .fetcher_ids = {10, 30},
        },
        {
            .test_name = "single_numeric_negate_10",
            .filter = "-@numeric_index_100_10:[1.0 2.0]",
            .evaluate_size = 90,
            .fetcher_ids = {90},
        },
        {
            .test_name = "single_numeric_negate_30",
            .filter = "-@numeric_index_100_30:[1.0 2.0]",
            .evaluate_size = 70,
            .fetcher_ids = {70},
        },
        {
            .test_name = "negate_two_numerics_or",
            .filter = "-(@numeric_index_100_30:[1.0 2.0] |"
                      "@numeric_index_100_10:[3.0 4.0])",
            .evaluate_size = 70,
            .fetcher_ids = {70},
        },
        {
            .test_name = "negate_two_numerics_and",
            .filter = "-(@numeric_index_100_30:[1.0 2.0] "
                      "@numeric_index_100_10:[3.0 4.0])",
            .evaluate_size = 160,
            .fetcher_ids = {70, 90},
        },
        {
            .test_name = "double_negate_two_numerics_or",
            .filter = "-(-(@numeric_index_100_30:[1.0 2.0] |"
                      "@numeric_index_100_10:[3.0 4.0]))",
            .evaluate_size = 40,
            .fetcher_ids = {10, 30},
        },
    }),
    [](const TestParamInfo<EvaluateFilterAsPrimaryTestCase> &info) {
      return info.param.test_name;
    });

std::shared_ptr<MockIndexSchema> CreateIndexSchemaWithMultipleAttributes(
    const IndexerType vector_indexer_type = indexes::IndexerType::kHNSW) {
  auto index_schema = CreateIndexSchema(kIndexSchemaName).value();
  EXPECT_CALL(*index_schema, GetIdentifier(::testing::_))
      .Times(::testing::AnyNumber());

  // Add vector index
  std::shared_ptr<indexes::IndexBase> vector_index;
  if (vector_indexer_type == IndexerType::kHNSW) {
    vector_index = indexes::VectorHNSW<float>::Create(
                       CreateHNSWVectorIndexProto(
                           kVectorDimensions, data_model::DISTANCE_METRIC_L2,
                           1000, 10, 300, 30),
                       "vector_attribute_identifier",
                       data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
                       .value();
  } else {
    vector_index =
        indexes::VectorFlat<float>::Create(
            CreateFlatVectorIndexProto(
                kVectorDimensions, data_model::DISTANCE_METRIC_L2, 1000, 250),
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

class LocalSearchTest : public ValkeySearchTestWithParam<LocalSearchTestCase> {
};

TEST_P(LocalSearchTest, LocalSearchTest) {
  auto index_schema = CreateIndexSchemaWithMultipleAttributes();
  const LocalSearchTestCase &test_case = GetParam();
  query::SearchParameters params(100000, nullptr, 0);
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
  FilterParser parser(*index_schema, test_case.filter);
  params.filter_parse_results = std::move(parser.Parse().value());
  params.index_schema = index_schema;
  auto time_slice_queries = Metrics::GetStats().time_slice_queries.load();
  auto neighbors = Search(params, valkey_search::query::SearchMode::kLocal);
  EXPECT_EQ(time_slice_queries + 1,
            Metrics::GetStats().time_slice_queries.load());
  VMSDK_EXPECT_OK(neighbors);
  EXPECT_EQ(neighbors.value().neighbors.size(),
            test_case.expected_neighbors_size);
}

INSTANTIATE_TEST_SUITE_P(
    LocalSearchTests, LocalSearchTest,
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
    }),
    [](const testing::TestParamInfo<LocalSearchTestCase> &info) {
      return info.param.test_name;
    });

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
  query::SearchParameters params(100000, nullptr, 0);
  FilterParser parser(*index_schema, test_case.filter);
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
  auto results =
      CalcBestMatchingPrefilteredKeys(params, entries_fetchers, vector_index);
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
        {
            .test_name = "and_predicate_both_sets_retrieved",
            .filter = "@numeric:[0 4] @numeric:[1 6]",
            .fetched_key_ranges = {{0, 4}, {1, 6}},
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
  query::SearchParameters params(100000, nullptr, 0);
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
    FilterParser parser(*params.index_schema, test_case.filter);
    params.filter_parse_results = std::move(parser.Parse().value());
  }
  auto neighbors = Search(params, query::SearchMode::kLocal);
  VMSDK_EXPECT_OK(neighbors);
#ifndef SAN_BUILD
  EXPECT_EQ(neighbors->neighbors.size(), test_case.expected_keys.size());
#endif

  for (auto &neighbor : neighbors->neighbors) {
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
    float distance;
    std::optional<absl::flat_hash_map<std::string, std::string>>
        attribute_contents;
    indexes::Neighbor ToIndexesNeighbor() const {
      auto string_interned_external_id = StringInternStore::Intern(external_id);
      auto result = indexes::Neighbor{string_interned_external_id, distance};
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
      result.distance = neighbor.distance;
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
      if (external_id != other.external_id || distance != other.distance) {
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
  absl::StatusOr<std::deque<TestNeighbor>> input;
  absl::StatusOr<std::deque<TestNeighbor>> expected_output;
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

  auto parameters = query::SearchParameters(100000, nullptr, 0);
  parameters.index_schema = index_schema;
  for (auto &attribute : test_case.return_attributes) {
    auto identifier = vmsdk::MakeUniqueValkeyString(attribute.identifier);
    auto alias = vmsdk::MakeUniqueValkeyString(attribute.alias);
    parameters.return_attributes.push_back(query::ReturnAttribute{
        .identifier = std::move(identifier), .alias = std::move(alias)});
  }
  parameters.no_content = test_case.no_content;

  absl::StatusOr<std::deque<indexes::Neighbor>> got;
  if (test_case.input.ok()) {
    absl::StatusOr<std::deque<indexes::Neighbor>> neighbors =
        std::deque<indexes::Neighbor>();
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
                                .distance = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{.external_id = "1",
                                          .distance = 0.1,
                                          .attribute_contents = std::nullopt}}},
                },
                {
                    .test_name = "all_non_indexed_return_attributes",
                    .return_attributes = {{.identifier = "1", .alias = "a1"},
                                          {.identifier = "2", .alias = "a2"}},
                    .input = {{{.external_id = "1",
                                .distance = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{.external_id = "1",
                                          .distance = 0.1,
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
                                .distance = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{.external_id = "1",
                                          .distance = 0.1,
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
                                .distance = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{.external_id = "1",
                                          .distance = 0.1,
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
                                .distance = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{
                        .external_id = "1",
                        .distance = 0.1,
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
                                .distance = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{
                        .external_id = "1",
                        .distance = 0.1,
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
                                .distance = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{
                        .external_id = "1",
                        .distance = 0.1,
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
                                .distance = 0.1,
                                .attribute_contents = std::nullopt}}},
                    .expected_output = {{{
                        .external_id = "1",
                        .distance = 0.1,
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
                                .distance = 0.1,
                                .attribute_contents = absl::flat_hash_map<
                                    std::string, std::string>{{"as1", "1"},
                                                              {"as2", "2"}}},
                               {.external_id = "2",
                                .distance = 0.2,
                                .attribute_contents = std::nullopt}}},
                    .expected_output =
                        {{{
                              .external_id = "1",
                              .distance = 0.1,
                              .attribute_contents =
                                  absl::flat_hash_map<std::string, std::string>{
                                      {"as1", "1"}, {"as2", "2"}},
                          },
                          {.external_id = "2",
                           .distance = 0.2,
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
                                .distance = 0.1,
                                .attribute_contents = std::nullopt},
                               {.external_id = "2",
                                .distance = 0.2,
                                .attribute_contents = std::nullopt}}},
                    .expected_output =
                        {{{
                              .external_id = "1",
                              .distance = 0.1,
                              .attribute_contents =
                                  absl::flat_hash_map<std::string, std::string>{
                                      {"as1", "1"}, {"as2", "2"}},
                          },
                          {.external_id = "2",
                           .distance = 0.2,
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
}  // namespace
}  // namespace valkey_search
