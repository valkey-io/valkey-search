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
    auto entries = Sparse::Decode(query, token.get());
    if (!entries.ok()) {
      return entries.status();
    }
    return index.Search(*entries, k, token, [](const auto &) { return true; });
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
  auto filtered =
      index.Search(*Sparse::Decode(query), 1, token,
                   [](const auto &key) { return key->Str() == "a"; });
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

TEST_F(SparseIndexTest, DenseBitmapPreservesBoundaryIdsZeroSumsAndTies) {
  for (uint32_t id = 0; id <= 128; ++id) {
    ASSERT_TRUE(Add(std::to_string(id), Encode({{1, 1}, {2, -1}})).ok());
  }
  auto hits = index.Search({{1, 1}, {2, 1}}, 3, token, [](const auto &key) {
    auto name = key->Str();
    return name == "63" || name == "64" || name == "127" || name == "128";
  });
  ASSERT_TRUE(hits.ok());
  ASSERT_EQ(hits->size(), 3);
  EXPECT_EQ((*hits)[0].external_id->Str(), "127");
  EXPECT_EQ((*hits)[1].external_id->Str(), "128");
  EXPECT_EQ((*hits)[2].external_id->Str(), "63");
  for (const auto &hit : *hits) {
    EXPECT_EQ(hit.distance, 0);
  }
}

TEST_F(SparseIndexTest, ScoreCutoffPreservesFilteredOverflow) {
  const auto huge = std::numeric_limits<float>::max();
  ASSERT_TRUE(Add("finite", Encode({{1, 1}})).ok());
  ASSERT_TRUE(Add("overflow", Encode({{2, -huge}})).ok());
  const auto q = Encode({{1, 1}, {2, huge}});
  auto hits = index.Search(*Sparse::Decode(q), 1, token, [](const auto &key) {
    return key->Str() == "finite";
  });
  ASSERT_TRUE(hits.ok());
  ASSERT_EQ(hits->size(), 1);
  EXPECT_EQ(hits->front().external_id->Str(), "finite");
  EXPECT_FALSE(Search(q, 1).ok());
}

TEST_F(SparseIndexTest, RareDimensionsRetainZeroSumAfterChurn) {
  for (int i = 0; i < 64; ++i) {
    ASSERT_TRUE(Add(std::to_string(i), Encode({{100, 1}})).ok());
  }
  for (int i = 0; i < 64; ++i) {
    ASSERT_TRUE(index
                    .RemoveRecord(StringInternStore::Intern(std::to_string(i)),
                                  DeletionType::kRecord)
                    .ok());
  }
  ASSERT_TRUE(Add("only", Encode({{1, 1}, {2, -1}})).ok());
  auto hits = Search(Encode({{1, 1}, {2, 1}, {3, 2}}));
  ASSERT_TRUE(hits.ok());
  ASSERT_EQ(hits->size(), 1);
  EXPECT_EQ(hits->front().external_id->Str(), "only");
  EXPECT_EQ(hits->front().distance, 0);
}

TEST_F(SparseIndexTest, CancellationKeepsOnlyFullyScoredCandidates) {
  const auto key = StringInternStore::Intern("a");
  ASSERT_TRUE(Add("a", Encode({{1, 4}})).ok());
  std::vector<InternedStringPtr> candidates{key};
  auto run = [&](bool partial) {
    token = std::make_shared<Token>();
    return index.Search(
        {{1, 2}}, 1, token,
        [&](const auto &) {
          token->Cancel();
          return true;
        },
        &candidates, partial);
  };
  EXPECT_EQ(run(false).status().code(), absl::StatusCode::kDeadlineExceeded);
  auto hits = run(true);
  ASSERT_TRUE(hits.ok());
  ASSERT_EQ(hits->size(), 1);
  EXPECT_EQ(hits->front().distance, -8);
}

TEST_F(SparseIndexTest, PollsLongDecodesAndEstimatesWithAbsentDimensions) {
  struct Budget : cancel::Base {
    int remaining;
    explicit Budget(int polls) : remaining(polls) {}
    bool IsCancelled() override { return --remaining <= 0; }
    void Cancel() override { remaining = 0; }
  };
  std::string query;
  for (uint32_t i = 0; i < 1024; ++i) {
    query += Encode({{i, 1}});
  }
  Budget decode(2), estimate(2);
  EXPECT_EQ(Sparse::Decode(query, &decode).status().code(),
            absl::StatusCode::kDeadlineExceeded);
  EXPECT_EQ(index.PreferPrefilter(*Sparse::Decode(query), 1, &estimate)
                .status()
                .code(),
            absl::StatusCode::kDeadlineExceeded);
  token = std::make_shared<Budget>(6);
  EXPECT_EQ(Search(query).status().code(), absl::StatusCode::kDeadlineExceeded);
  ASSERT_TRUE(Add("long", query).ok());
  std::vector<InternedStringPtr> candidates{StringInternStore::Intern("long")};
  token = std::make_shared<Budget>(2);
  bool reached_filter = false;
  auto hits = index.Search(
      {{1023, 1}}, 1, token,
      [&](const auto &) {
        reached_filter = true;
        return true;
      },
      &candidates);
  EXPECT_EQ(hits.status().code(), absl::StatusCode::kDeadlineExceeded);
  EXPECT_FALSE(reached_filter);
}

TEST_F(SparseIndexTest, PlanEstimateTracksReplacementAndRemoval) {
  ASSERT_TRUE(Add("a", Encode({{1, 2}, {2, 1}})).ok());
  ASSERT_TRUE(Add("b", Encode({{1, 3}, {2, 1}})).ok());
  const std::vector<Sparse::Entry> query{{1, 1}};
  EXPECT_TRUE(*index.PreferPrefilter(query, 0));
  EXPECT_FALSE(*index.PreferPrefilter(query, 2));
  auto a = StringInternStore::Intern("a");
  ASSERT_TRUE(index
                  .ModifyRecord(a, AttributeData(vmsdk::MakeUniqueValkeyString(
                                       Encode({{1, 2}}))))
                  .ok());
  ASSERT_TRUE(
      index.RemoveRecord(StringInternStore::Intern("b"), DeletionType::kRecord)
          .ok());
  ASSERT_TRUE(Add("c", Encode({{1, 4}})).ok());
  // Two postings now cost as much as one two-pointer forward scan. A stale
  // count left by replacement/removal would choose the other plan.
  EXPECT_TRUE(*index.PreferPrefilter(query, 1));
  EXPECT_FALSE(*index.PreferPrefilter(query, 2));
  ASSERT_TRUE(index
                  .ModifyRecord(a, AttributeData(vmsdk::MakeUniqueValkeyString(
                                       "invalid")))
                  .ok());
  EXPECT_FALSE(*index.PreferPrefilter(query, 1));
}

TEST_F(SparseIndexTest, RescoringAndOverflow) {
  auto record = Encode({{1, -2}, {3, 4}});
  EXPECT_EQ(*Sparse::RecomputeDistance(record, {{1, 2}, {3, 3}}), -8);
  EXPECT_FALSE(Sparse::RecomputeDistance(record, {{5, 2}}).ok());
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
