/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/indexes/index_base.h"
#include "src/indexes/numeric.h"
#include "src/query/predicate.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::indexes {

namespace {

class NumericIndexTest : public vmsdk::ValkeyTest {
 protected:
  data_model::NumericIndex numeric_index_proto;
  IndexTeser<NumericField, data_model::NumericIndex> index{numeric_index_proto};
};

std::vector<std::string> Fetch(
    valkey_search::indexes::EntriesFetcherBase& fetcher) {
  std::vector<std::string> keys;
  auto itr = fetcher.Begin();
  while (!itr->Done()) {
    keys.push_back(std::string(***itr));
    itr->Next();
  }
  return keys;
}

TEST_F(NumericIndexTest, SimpleAddModifyRemove) {
  EXPECT_TRUE(index.AddRecord("key1", "1.5").value());
  EXPECT_TRUE(index.AddRecord("key2", "2.0").value());
  std::string attribute_id = "attribute_id";
  std::string attribute_alias = "attribute_alias";

  query::NumericPredicate predicate1(&index, attribute_alias, attribute_id, 1.0,
                                     true, 2.0, true);
  auto fetcher = index.Search(predicate1, false);
  EXPECT_EQ(fetcher->Size(), 2);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre("key1", "key2"));

  EXPECT_EQ(index.AddRecord("key2", "2.0").status().code(),
            absl::StatusCode::kAlreadyExists);
  EXPECT_TRUE(index.ModifyRecord("key2", "2.1").value());

  auto predicate2 = query::NumericPredicate(
      &index, attribute_alias, attribute_id, 2.05, true, 2.2, true);
  fetcher = index.Search(predicate2, false);
  EXPECT_EQ(fetcher->Size(), 1);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre("key2"));

  EXPECT_EQ(index.ModifyRecord("key5", "2.1").status().code(),
            absl::StatusCode::kNotFound);
  EXPECT_FALSE(index.IsTracked("key3"));
  EXPECT_TRUE(index.AddRecord("key3", "3.0").value());
  EXPECT_TRUE(index.IsTracked("key3"));
  EXPECT_TRUE(index.RemoveRecord("key3").value());
  auto predicate3 = query::NumericPredicate(&index, attribute_alias,
                                            attribute_id, 2.5, true, 3.5, true);
  fetcher = index.Search(predicate3, false);
  EXPECT_EQ(fetcher->Size(), 0);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre());

  EXPECT_FALSE(index.IsTracked("key3"));
  auto res = index.RemoveRecord("key3");
  EXPECT_TRUE(res.ok());
  EXPECT_FALSE(res.value());
  EXPECT_FALSE(index.AddRecord("key5", "aaa").value());
  EXPECT_FALSE(index.ModifyRecord("key5", "aaa").value());
}

TEST_F(NumericIndexTest, SimpleAddModifyRemove1) {
  VMSDK_EXPECT_OK(index.AddRecord("key1", "1.5"));
  VMSDK_EXPECT_OK(index.AddRecord("key2", "2.0"));
  auto predicate = query::NumericPredicate(&index, "attribute1", "id1", 1.0,
                                           true, 2.1, true);
  auto fetcher = index.Search(predicate, false);
  EXPECT_EQ(fetcher->Size(), 2);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre("key1", "key2"));
  VMSDK_EXPECT_OK(index.ModifyRecord("key2", "2.1"));

  predicate = query::NumericPredicate(&index, "attribute1", "id1", 2.05, true,
                                      2.2, true);
  fetcher = index.Search(predicate, false);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre("key2"));
  VMSDK_EXPECT_OK(index.AddRecord("key3", "3.0"));
  VMSDK_EXPECT_OK(index.RemoveRecord("key3"));

  predicate = query::NumericPredicate(&index, "attribute1", "id1", 2.5, true,
                                      3.5, true);
  fetcher = index.Search(predicate, false);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre());
}

TEST_F(NumericIndexTest, ModifyWithNonNumericString) {
  VMSDK_EXPECT_OK(index.AddRecord("key1", "1.5"));
  auto predicate = query::NumericPredicate(&index, "attribute1", "id1", 1.0,
                                           true, 2.1, true);
  auto fetcher = index.Search(predicate, false);
  EXPECT_EQ(fetcher->Size(), 1);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre("key1"));
  VMSDK_EXPECT_OK(index.ModifyRecord("key1", "abcde"));

  fetcher = index.Search(predicate, false);
  EXPECT_EQ(Fetch(*fetcher).size(), 0);
  EXPECT_EQ(index.GetRecordCount(), 0);
}

TEST_F(NumericIndexTest, RangeSearchInclusiveExclusive) {
  EXPECT_TRUE(index.AddRecord("key1", "1.0").value());
  EXPECT_TRUE(index.AddRecord("key2", "2.0").value());
  EXPECT_TRUE(index.AddRecord("key3", "2.2").value());
  EXPECT_TRUE(index.AddRecord("key4", "3.2").value());
  EXPECT_TRUE(index.AddRecord("key5", "2.0").value());
  EXPECT_TRUE(index.AddRecord("key6", "2.1").value());

  std::string attribute_id = "attribute_id";
  std::string attribute_alias = "attribute_alias";

  query::NumericPredicate predicate(&index, attribute_alias, attribute_id, 1.0,
                                    true, 2.1, true);
  auto fetcher = index.Search(predicate, false);
  EXPECT_EQ(fetcher->Size(), 4);
  EXPECT_THAT(Fetch(*fetcher),
              testing::UnorderedElementsAre("key1", "key2", "key5", "key6"));

  predicate = query::NumericPredicate(&index, attribute_alias, attribute_id,
                                      1.0, false, 2.1, true);
  fetcher = index.Search(predicate, false);
  EXPECT_EQ(fetcher->Size(), 3);
  EXPECT_THAT(Fetch(*fetcher),
              testing::UnorderedElementsAre("key2", "key5", "key6"));

  predicate = query::NumericPredicate(&index, attribute_alias, attribute_id,
                                      1.0, false, 2.1, false);
  fetcher = index.Search(predicate, false);
  EXPECT_EQ(fetcher->Size(), 2);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre("key2", "key5"));

  predicate = query::NumericPredicate(&index, attribute_alias, attribute_id,
                                      1.0, false, 3.5, false);
  fetcher = index.Search(predicate, false);
  EXPECT_EQ(fetcher->Size(), 5);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre(
                                   "key2", "key3", "key4", "key5", "key6"));

  predicate = query::NumericPredicate(&index, attribute_alias, attribute_id,
                                      0.0, false, 2.1, false);
  fetcher = index.Search(predicate, false);
  EXPECT_EQ(fetcher->Size(), 3);
  EXPECT_THAT(Fetch(*fetcher),
              testing::UnorderedElementsAre("key1", "key2", "key5"));
}

TEST_F(NumericIndexTest, RangeSearchInclusiveExclusive1) {
  VMSDK_EXPECT_OK(index.AddRecord("key1", "1.0"));
  VMSDK_EXPECT_OK(index.AddRecord("key2", "2.1"));
  VMSDK_EXPECT_OK(index.AddRecord("key3", "3.0"));
  VMSDK_EXPECT_OK(index.AddRecord("key4", "5.0"));
  VMSDK_EXPECT_OK(index.AddRecord("key5", "7.0"));
  VMSDK_EXPECT_OK(index.AddRecord("key6", "9.0"));
  auto predicate = query::NumericPredicate(&index, "attribute1", "id1", 1.0,
                                           true, 3.0, true);
  auto fetcher = index.Search(predicate, false);
  EXPECT_EQ(fetcher->Size(), 3);
  EXPECT_THAT(Fetch(*fetcher),
              testing::UnorderedElementsAre("key1", "key2", "key3"));

  predicate = query::NumericPredicate(&index, "attribute1", "id1", 1.0, false,
                                      3.0, true);
  fetcher = index.Search(predicate, false);
  EXPECT_EQ(fetcher->Size(), 2);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre("key2", "key3"));

  predicate = query::NumericPredicate(&index, "attribute1", "id1", 1.0, true,
                                      3.0, false);
  fetcher = index.Search(predicate, false);
  EXPECT_EQ(fetcher->Size(), 2);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre("key1", "key2"));

  predicate = query::NumericPredicate(&index, "attribute1", "id1", 1.0, false,
                                      3.0, false);
  fetcher = index.Search(predicate, false);
  EXPECT_EQ(fetcher->Size(), 1);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre("key2"));

  predicate = query::NumericPredicate(&index, "attribute1", "id1", 2.0, true,
                                      4.0, true);
  fetcher = index.Search(predicate, false);
  EXPECT_EQ(fetcher->Size(), 2);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre("key2", "key3"));
  predicate = query::NumericPredicate(&index, "attribute1", "id1", 2.0, false,
                                      4.0, false);
  fetcher = index.Search(predicate, false);
  EXPECT_EQ(fetcher->Size(), 2);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre("key2", "key3"));
}

TEST_F(NumericIndexTest, RangeSearchNegate) {
  VMSDK_EXPECT_OK(index.AddRecord("key1", "1.0"));
  VMSDK_EXPECT_OK(index.AddRecord("key2", "2.1"));
  VMSDK_EXPECT_OK(index.AddRecord("key3", "3.0"));
  VMSDK_EXPECT_OK(index.AddRecord("key4", "5.0"));
  VMSDK_EXPECT_OK(index.AddRecord("key5", "7.0"));
  VMSDK_EXPECT_OK(index.AddRecord("key6", "9.0"));
  auto predicate = query::NumericPredicate(&index, "attribute1", "id1", 1.0,
                                           true, 3.0, true);
  auto fetcher = index.Search(predicate, true);
  EXPECT_EQ(fetcher->Size(), 3);
  EXPECT_THAT(Fetch(*fetcher),
              testing::UnorderedElementsAre("key4", "key5", "key6"));

  predicate = query::NumericPredicate(&index, "attribute1", "id1", 1.0, false,
                                      3.0, false);
  fetcher = index.Search(predicate, true);
  EXPECT_EQ(fetcher->Size(), 5);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre(
                                   "key1", "key3", "key4", "key5", "key6"));

  predicate = query::NumericPredicate(&index, "attribute1", "id1", 1.0, false,
                                      3.0, true);
  fetcher = index.Search(predicate, true);
  EXPECT_EQ(fetcher->Size(), 4);
  EXPECT_THAT(Fetch(*fetcher),
              testing::UnorderedElementsAre("key1", "key4", "key5", "key6"));

  predicate = query::NumericPredicate(&index, "attribute1", "id1", 1.0, true,
                                      3.0, false);
  fetcher = index.Search(predicate, true);
  EXPECT_EQ(fetcher->Size(), 4);
  EXPECT_THAT(Fetch(*fetcher),
              testing::UnorderedElementsAre("key3", "key4", "key5", "key6"));

  predicate = query::NumericPredicate(&index, "attribute1", "id1", 2.0, true,
                                      4.0, true);
  fetcher = index.Search(predicate, true);
  EXPECT_EQ(fetcher->Size(), 4);
  EXPECT_THAT(Fetch(*fetcher),
              testing::UnorderedElementsAre("key1", "key4", "key5", "key6"));
  predicate = query::NumericPredicate(&index, "attribute1", "id1", 2.0, false,
                                      4.0, false);
  fetcher = index.Search(predicate, true);
  EXPECT_EQ(fetcher->Size(), 4);
  EXPECT_THAT(Fetch(*fetcher),
              testing::UnorderedElementsAre("key1", "key4", "key5", "key6"));

  predicate = query::NumericPredicate(&index, "attribute1", "id1", 2.0, false,
                                      4.0, true);
  fetcher = index.Search(predicate, true);
  EXPECT_EQ(fetcher->Size(), 4);
  EXPECT_THAT(Fetch(*fetcher),
              testing::UnorderedElementsAre("key1", "key4", "key5", "key6"));

  predicate = query::NumericPredicate(&index, "attribute1", "id1", 2.1, true,
                                      2.1, true);
  fetcher = index.Search(predicate, true);
  EXPECT_EQ(fetcher->Size(), 5);
  EXPECT_THAT(Fetch(*fetcher), testing::UnorderedElementsAre(
                                   "key1", "key3", "key4", "key5", "key6"));

  VMSDK_EXPECT_OK(index.RemoveRecord("key6"));
  predicate = query::NumericPredicate(&index, "attribute1", "id1", 1.0, true,
                                      3.0, true);
  fetcher = index.Search(predicate, true);
  EXPECT_EQ(fetcher->Size(), 3);
  EXPECT_THAT(Fetch(*fetcher),
              testing::UnorderedElementsAre("key4", "key5", "key6"));

  VMSDK_EXPECT_OK(index.AddRecord("key6", "9.0"));
  predicate = query::NumericPredicate(&index, "attribute1", "id1", 1.0, true,
                                      3.0, true);
  fetcher = index.Search(predicate, true);
  EXPECT_EQ(fetcher->Size(), 3);
  EXPECT_THAT(Fetch(*fetcher),
              testing::UnorderedElementsAre("key4", "key5", "key6"));
}

TEST_F(NumericIndexTest, DeletedKeysNegativeSearchTest) {
  EXPECT_TRUE(index.AddRecord("doc0", "-100").value());

  // Test 1: soft delete
  EXPECT_TRUE(index.AddRecord("doc1", "-200").value());
  EXPECT_TRUE(index.RemoveRecord("doc1", DeletionType::kNone)
                  .value());  // remove a field
  auto entries_fetcher =
      index.Search(query::NumericPredicate(&index, "attribute1", "id1", 1.0,
                                           true, 3.0, true),
                   true);
  EXPECT_THAT(Fetch(*entries_fetcher),
              testing::UnorderedElementsAre("doc0", "doc1"));

  // Test 2: hard delete
  EXPECT_FALSE(
      index.RemoveRecord("doc1", DeletionType::kRecord).value());  // delete key
  entries_fetcher =
      index.Search(query::NumericPredicate(&index, "attribute1", "id1", 1.0,
                                           true, 3.0, true),
                   true);
  EXPECT_THAT(Fetch(*entries_fetcher), testing::UnorderedElementsAre("doc0"));
}

}  // namespace

}  // namespace valkey_search::indexes
