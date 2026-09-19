/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 */

#include "src/coordinator/server.h"

#include <vector>

#include "gtest/gtest.h"
#include "src/indexes/vector_base.h"
#include "src/query/fanout.h"
#include "src/utils/string_interning.h"

namespace valkey_search::coordinator {
namespace {

TEST(NeighborEntryCompatibilityTest, FallsBackToLegacyFloatFields) {
  NeighborEntry entry;
  entry.set_distance(0.125f);
  entry.set_score(0.875f);

  EXPECT_FALSE(entry.has_distance_fp64());
  EXPECT_FALSE(entry.has_score_fp64());
  EXPECT_DOUBLE_EQ(query::fanout::GetPreferredNeighborDistance(entry), 0.125f);
  EXPECT_DOUBLE_EQ(query::fanout::GetPreferredNeighborScore(entry), 0.875f);
}

TEST(NeighborEntryCompatibilityTest, PrefersFP64FieldsWhenPresent) {
  NeighborEntry entry;
  entry.set_distance(0.125f);
  entry.set_score(0.875f);
  entry.set_distance_fp64(0.12500000000000003);
  entry.set_score_fp64(0.87500000000000011);

  EXPECT_DOUBLE_EQ(query::fanout::GetPreferredNeighborDistance(entry),
                   0.12500000000000003);
  EXPECT_DOUBLE_EQ(query::fanout::GetPreferredNeighborScore(entry),
                   0.87500000000000011);
}

TEST(NeighborEntryCompatibilityTest, SerializesLegacyAndFP64Fields) {
  const double distance = 1.0e40;
  const double score = 0.12345678901234567;
  std::vector<indexes::Neighbor> neighbors;
  neighbors.emplace_back(StringInternStore::Intern("doc"), distance, score);
  SearchIndexPartitionResponse response;

  SerializeNeighbors(&response, neighbors);

  ASSERT_EQ(response.neighbors_size(), 1);
  const NeighborEntry& entry = response.neighbors(0);
  EXPECT_FLOAT_EQ(entry.distance(), static_cast<float>(distance));
  EXPECT_FLOAT_EQ(entry.score(), static_cast<float>(score));
  ASSERT_TRUE(entry.has_distance_fp64());
  ASSERT_TRUE(entry.has_score_fp64());
  EXPECT_DOUBLE_EQ(entry.distance_fp64(), distance);
  EXPECT_DOUBLE_EQ(entry.score_fp64(), score);
}

}  // namespace
}  // namespace valkey_search::coordinator
