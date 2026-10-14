/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/sparse_search.h"

#include <algorithm>
#include <bit>
#include <cstdint>
#include <limits>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "absl/base/internal/endian.h"
#include "gtest/gtest.h"
#include "src/indexes/fp16.h"

namespace valkey_search::query {
namespace {

void Append(std::string &data, uint32_t id, uint16_t weight_bits) {
  const size_t offset = data.size();
  data.resize(offset + kSparsePostingBytes);
  absl::little_endian::Store32(data.data() + offset, id);
  absl::little_endian::Store16(data.data() + offset + sizeof(id), weight_bits);
}

TEST(SparseSearchTest, AccumulatesAcrossListsAndBlocks) {
  std::string a;
  std::string b;
  Append(a, 1, 0x3c00);           // 1
  Append(a, 8191, 0x4000);        // 2
  Append(a, 8192, 0x4200);        // 3
  Append(a, UINT32_MAX, 0x4400);  // 4
  Append(b, 1, 0x4000);
  Append(b, 8192, 0x3c00);
  auto result = SparseSearch({{a, 2}, {b, 3}}, 3);
  ASSERT_TRUE(result.ok()) << result.status();
  ASSERT_EQ(result->size(), 3);
  EXPECT_EQ((*result)[0].id, 8192);
  EXPECT_EQ((*result)[0].score, 9);
  EXPECT_EQ((*result)[1].id, 1);
  EXPECT_EQ((*result)[1].score, 8);
  EXPECT_EQ((*result)[2].id, UINT32_MAX);
  EXPECT_EQ((*result)[2].score, 8);
}

TEST(SparseSearchTest, EmptyListsAndZeroScores) {
  auto empty = SparseSearch({{{}, 1}}, 10);
  ASSERT_TRUE(empty.ok());
  EXPECT_TRUE(empty->empty());
  std::string a;
  Append(a, 0, 0);
  Append(a, 8192, 0x3c00);
  auto result = SparseSearch({{a, 0}}, 1);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result->size(), 1);
  EXPECT_EQ(result->front().id, 0);
  EXPECT_EQ(result->front().score, 0);
}

TEST(SparseSearchTest, HandlesAllFiniteNonnegativeHalfValues) {
  std::string data;
  for (uint16_t bits = 0; bits < 0x7c00; ++bits) {
    Append(data, bits, bits);
  }
  auto result = SparseSearch({{data, 1}}, 2);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->front().id, 0x7bff);
  EXPECT_EQ(result->front().score, 65504);
  std::string subnormal;
  Append(subnormal, 0, 1);
  result = SparseSearch({{subnormal, 1}}, 1);
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->front().score, 0x1p-24);
}

TEST(SparseSearchTest, RejectsMalformedListsAndWeights) {
  EXPECT_FALSE(SparseSearch({}, 1).ok());
  EXPECT_FALSE(SparseSearch({{{}, 1}}, 0).ok());
  EXPECT_FALSE(SparseSearch({{{}, 1}}, kSparseMaxTopK + 1).ok());
  std::vector<SparsePostingList> too_many(kSparseMaxDimensions + 1, {{}, 1});
  EXPECT_FALSE(SparseSearch(too_many, 1).ok());
  for (size_t length = 1; length < kSparsePostingBytes; ++length) {
    EXPECT_FALSE(SparseSearch({{std::string(length, '\0'), 1}}, 1).ok());
  }
  for (double weight : {-1.0, std::numeric_limits<double>::infinity(),
                        std::numeric_limits<double>::quiet_NaN()}) {
    EXPECT_FALSE(SparseSearch({{{}, weight}}, 1).ok());
  }
  for (uint16_t bits : {0x7c00, 0x7fff, 0x8000, 0xbc00, 0xfc00}) {
    std::string data;
    Append(data, 1, bits);
    EXPECT_FALSE(SparseSearch({{data, 1}}, 1).ok());
  }
  for (uint32_t second : {0, 1}) {
    std::string data;
    Append(data, 1, 0x3c00);
    Append(data, second, 0x3c00);
    EXPECT_FALSE(SparseSearch({{data, 1}}, 1).ok());
  }
  std::string across_blocks;
  Append(across_blocks, 8192, 0x3c00);
  Append(across_blocks, 1, 0x3c00);
  EXPECT_FALSE(SparseSearch({{across_blocks, 1}}, 1).ok());
  std::string overflow;
  Append(overflow, 1, 0x4000);
  EXPECT_FALSE(
      SparseSearch({{overflow, std::numeric_limits<double>::max()}}, 1).ok());
}

TEST(SparseSearchTest, PreservesAccumulationOrderForNearlyTiedScores) {
  std::string candidate;
  std::string competitor;
  Append(candidate, 2, 0x3c00);  // 1
  Append(competitor, 3, 0x3c00);
  // Each +1 rounds away after 2^53. Regrouping the two small terms would
  // instead tie point 3 and incorrectly promote the smaller point ID 2.
  const std::vector<SparsePostingList> lists = {
      {candidate, 0x1p53},
      {candidate, 1},
      {candidate, 1},
      {competitor, 0x1.0000000000001p53}};
  for (size_t k : {1, 2}) {
    auto result = SparseSearch(lists, k);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(result->size(), k);
    EXPECT_EQ((*result)[0].id, 3);
    EXPECT_EQ(std::bit_cast<uint64_t>((*result)[0].score),
              std::bit_cast<uint64_t>(0x1.0000000000001p53));
    if (k == 2) {
      EXPECT_EQ((*result)[1].id, 2);
      EXPECT_EQ(std::bit_cast<uint64_t>((*result)[1].score),
                std::bit_cast<uint64_t>(0x1p53));
    }
  }
}

TEST(SparseSearchTest, RoundsMultiplyAndAddSeparatelyForNearlyTiedScores) {
  std::string unit;
  std::string fraction;
  std::string competitor;
  Append(unit, 1, 0x3c00);      // 1
  Append(fraction, 1, 0x3c01);  // 1 + 2^-10
  Append(competitor, 0, 0x3c00);
  // The rounded product is 1 + 2^-10 + 2^-51. Adding 2^-53 rounds
  // back to that even value; FMA retains the product's 2^-61 remainder
  // and rounds one ULP higher, incorrectly promoting point 1 over point 0.
  constexpr double expected = 0x1.0040000000002p0;
  const std::vector<SparsePostingList> lists = {
      {unit, 0x1p-53}, {fraction, 0x1.0000000000002p0}, {competitor, expected}};
  for (size_t k : {1, 2}) {
    auto result = SparseSearch(lists, k);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(result->size(), k);
    for (size_t i = 0; i < k; ++i) {
      EXPECT_EQ((*result)[i].id, i);
      EXPECT_EQ(std::bit_cast<uint64_t>((*result)[i].score),
                std::bit_cast<uint64_t>(expected));
    }
  }
}

TEST(SparseSearchTest, MatchesReferenceDotProduct) {
  std::mt19937 rng(42);
  for (size_t dimensions : {4, 8, 16, 64}) {
    std::vector<std::string> data(dimensions);
    std::vector<SparsePostingList> lists;
    std::map<uint32_t, double> reference;
    for (size_t t = 0; t < dimensions; ++t) {
      const double query_weight = (rng() % 17) / 4.0;
      for (uint32_t id = 0; id < 25000; id += 1 + rng() % 300) {
        const float16 weight = float_to_float16((rng() % 1000) / 32.0f);
        Append(data[t], id, std::bit_cast<uint16_t>(weight));
        reference[id] += query_weight * float16_to_float(weight);
      }
      lists.push_back({data[t], query_weight});
    }
    std::vector<SparseSearchHit> expected;
    expected.reserve(reference.size());
    for (const auto &[id, score] : reference) {
      expected.push_back({id, score});
    }
    std::sort(expected.begin(), expected.end(),
              [](const auto &a, const auto &b) {
                return a.score > b.score || (a.score == b.score && a.id < b.id);
              });
    for (size_t k : {1, 10, 100, 10000}) {
      auto actual = SparseSearch(lists, k);
      ASSERT_TRUE(actual.ok()) << actual.status();
      ASSERT_EQ(actual->size(), std::min(k, expected.size()));
      for (size_t i = 0; i < actual->size(); ++i) {
        EXPECT_EQ((*actual)[i].id, expected[i].id);
        EXPECT_DOUBLE_EQ((*actual)[i].score, expected[i].score);
      }
    }
  }
}

}  // namespace
}  // namespace valkey_search::query
