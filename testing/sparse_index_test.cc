/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#include <bit>
#include <limits>

#include "gtest/gtest.h"
#include "src/attribute_data.h"
#include "src/indexes/sparse.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::indexes {
namespace {
std::string Encode(std::initializer_list<Sparse::Entry> entries) {
  std::string blob;
  for (const auto &entry : entries) {
    const auto dimension = entry.dimension;
    const auto weight = entry.weight;
    for (uint32_t word : {dimension, std::bit_cast<uint32_t>(weight)}) {
      for (int shift = 0; shift < 32; shift += 8) {
        blob.push_back(static_cast<char>(word >> shift));
      }
    }
  }
  return blob;
}

class SparseIndexTest : public vmsdk::ValkeyTest {
 protected:
  Sparse index;
  struct Token : cancel::Base {
    bool cancelled = false;
    bool IsCancelled() override { return cancelled; }
    void Cancel() override { cancelled = true; }
  };
  cancel::Token token = std::make_shared<Token>();
  absl::StatusOr<RecordResult> Add(absl::string_view key,
                                   absl::string_view blob) {
    return index.AddRecord(StringInternStore::Intern(key),
                           AttributeData(vmsdk::MakeUniqueValkeyString(blob)));
  }
  absl::StatusOr<std::vector<Neighbor>> Search(absl::string_view query,
                                               size_t k = 10) {
    return index.Search(query, k, token, [](const auto &) { return true; });
  }
};

TEST_F(SparseIndexTest, RejectsMalformedValuesIncludingUnderFastMath) {
  EXPECT_FALSE(Sparse::Decode("x").ok());
  EXPECT_FALSE(Sparse::Decode(Encode({{3, 1}, {3, 2}})).ok());
  EXPECT_FALSE(Sparse::Decode(Encode({{4, 1}, {2, 2}})).ok());
  EXPECT_FALSE(
      Sparse::Decode(Encode({{0, std::numeric_limits<float>::infinity()}}))
          .ok());
  EXPECT_FALSE(
      Sparse::Decode(Encode({{0, std::numeric_limits<float>::quiet_NaN()}}))
          .ok());
  EXPECT_TRUE(Sparse::Decode(Encode({{0, -1}, {UINT32_MAX, 2}})).ok());
  EXPECT_TRUE(Sparse::Decode(Encode({{1, 0}}))->empty());
}

TEST_F(SparseIndexTest, SignedDotProductsTiesFilterAndUpdates) {
  ASSERT_EQ(*Add("a", Encode({{1, -2}, {5, 1}})), RecordResult::kAdded);
  ASSERT_EQ(*Add("b", Encode({{1, 2}, {5, -1}})), RecordResult::kAdded);
  ASSERT_EQ(*Add("c", Encode({{1, 2}})), RecordResult::kAdded);
  ASSERT_EQ(*Add("d", Encode({{9, 100}})), RecordResult::kAdded);
  const auto query = Encode({{1, 1}});
  auto result = Search(query, 2);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 2);
  EXPECT_EQ((*result)[0].external_id->Str(), "b");
  EXPECT_EQ((*result)[1].external_id->Str(), "c");
  EXPECT_EQ((*result)[0].distance, -2);
  auto filtered = index.Search(
      query, 1, token, [](const auto &key) { return key->Str() == "a"; });
  ASSERT_EQ(filtered->size(), 1);
  EXPECT_EQ((*filtered)[0].distance, 2);
  auto b = StringInternStore::Intern("b");
  ASSERT_TRUE(index
                  .ModifyRecord(b, AttributeData(vmsdk::MakeUniqueValkeyString(
                                       Encode({{9, 4}}))))
                  .ok());
  ASSERT_TRUE(
      index.RemoveRecord(StringInternStore::Intern("c"), DeletionType::kRecord)
          .ok());
  result = Search(query);
  ASSERT_EQ(result->size(), 1);
  EXPECT_EQ((*result)[0].external_id->Str(), "a");
  EXPECT_EQ(*index.ModifyRecord(
                b, AttributeData(vmsdk::MakeUniqueValkeyString("bad"))),
            RecordResult::kInvalidData);
  EXPECT_FALSE(index.IsTracked(b));
  EXPECT_TRUE(index.IsUnTracked(b));
  EXPECT_EQ(index.GetTrackedKeyCount(), 2);
  EXPECT_TRUE(index.ToProto()->has_sparse_index());
  token->Cancel();
  EXPECT_EQ(Search(query).status().code(), absl::StatusCode::kDeadlineExceeded);
}

TEST_F(SparseIndexTest, RescoringAndOverflow) {
  auto record = Encode({{1, -2}, {3, 4}});
  EXPECT_EQ(*Sparse::RecomputeDistance(record, Encode({{1, 2}, {3, 3}})), -8);
  EXPECT_FALSE(Sparse::RecomputeDistance(record, Encode({{5, 2}})).ok());
  auto huge = Encode({{1, std::numeric_limits<float>::max()}});
  ASSERT_TRUE(Add("a", huge).ok());
  EXPECT_FALSE(Search(huge).ok());
}

TEST_F(SparseIndexTest, RemovalRepairReusedIdsAndZeroScoreOverlap) {
  // Multiple shared postings exercise both swap removal and ID reuse.
  ASSERT_TRUE(Add("z", Encode({{1, 3}, {2, -3}, {4, 1}})).ok());
  ASSERT_TRUE(Add("b", Encode({{1, 2}, {2, -2}, {4, 1}})).ok());
  ASSERT_TRUE(Add("c", Encode({{1, 5}, {2, -5}, {4, 1}})).ok());
  auto b = StringInternStore::Intern("b");
  ASSERT_TRUE(index.RemoveRecord(b, DeletionType::kRecord).ok());
  ASSERT_TRUE(Add("a", Encode({{1, 4}, {2, -4}})).ok());
  auto c = StringInternStore::Intern("c");
  ASSERT_TRUE(index
                  .ModifyRecord(c, AttributeData(vmsdk::MakeUniqueValkeyString(
                                       Encode({{1, 7}, {2, -7}, {5, 2}}))))
                  .ok());
  ASSERT_TRUE(
      index.RemoveRecord(StringInternStore::Intern("z"), DeletionType::kRecord)
          .ok());
  ASSERT_TRUE(
      index.ModifyRecord(c, AttributeData(vmsdk::MakeUniqueValkeyString("bad")))
          .ok());
  ASSERT_TRUE(Add("b", Encode({{1, 8}, {2, -8}})).ok());
  auto result = Search(Encode({{1, 1}, {2, 1}}));
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 2);
  EXPECT_EQ((*result)[0].external_id->Str(), "a");
  EXPECT_EQ((*result)[1].external_id->Str(), "b");
  EXPECT_EQ((*result)[0].distance, 0);
  EXPECT_EQ((*result)[1].distance, 0);
  EXPECT_TRUE(Search(Encode({{4, 1}}))->empty());
  EXPECT_TRUE(Search(Encode({{5, 1}}))->empty());
  EXPECT_EQ(index.GetTrackedKeyCount(), 2);
}
}  // namespace
}  // namespace valkey_search::indexes
