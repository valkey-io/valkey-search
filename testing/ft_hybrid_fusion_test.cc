/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

// Unit tests for the fusion driver's distance-to-similarity conversion.
//
// This is the one point where every vector arm is put onto the footing fusion
// reads, and the mapping differs per metric. Two of the three inputs that
// matter cannot be produced by any index an integration test is likely to
// build: a distance at or below -1, which only inner product reaches, and a
// distance of exactly -1, which nothing reaches but which sits on the pole of
// the L2 formula.

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/vector_base.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"

namespace valkey_search {
namespace query {

// Defined in src/commands/ft_hybrid.cc.
void ConvertVectorArmScoresToSimilarity(std::vector<indexes::Neighbor> &ns,
                                        data_model::DistanceMetric metric);

namespace {

// StringInternStore::Intern requires the main-thread context this fixture
// establishes.
class FusionConversionTest : public ValkeySearchTest {};

std::vector<indexes::Neighbor> NeighborsAt(
    const std::vector<float> &distances) {
  std::vector<indexes::Neighbor> out;
  out.reserve(distances.size());
  for (size_t i = 0; i < distances.size(); ++i) {
    out.push_back(indexes::Neighbor(
        StringInternStore::Intern(absl::StrCat("k", i)), distances[i]));
  }
  return out;
}

// L2 distances are non-negative, so this is the plain reciprocal.
TEST_F(FusionConversionTest, L2IsTheReciprocalOfOnePlusD) {
  auto ns = NeighborsAt({0.0f, 1.0f, 3.0f, 99.0f});
  ConvertVectorArmScoresToSimilarity(ns, data_model::DISTANCE_METRIC_L2);
  EXPECT_FLOAT_EQ(ns[0].score, 1.0f);
  EXPECT_FLOAT_EQ(ns[1].score, 0.5f);
  EXPECT_FLOAT_EQ(ns[2].score, 0.25f);
  EXPECT_FLOAT_EQ(ns[3].score, 0.01f);
}

// The pole. An L2 distance should never be negative, so what matters is that a
// float error near it cannot produce an infinity.
TEST_F(FusionConversionTest, L2GuardsThePole) {
  auto ns = NeighborsAt({-1.0f, -1.5f, -100.0f});
  ConvertVectorArmScoresToSimilarity(ns, data_model::DISTANCE_METRIC_L2);
  for (const auto &n : ns) {
    EXPECT_FLOAT_EQ(n.score, 0.0f);
  }
}

// Inner-product distance is `1 - dot` and so unbounded below. The map is affine
// because no bounded one exists over an unbounded domain -- and crucially it
// stays injective far below -1, where the reciprocal formula collapsed every
// value onto the guard.
TEST_F(FusionConversionTest, InnerProductIsAffine) {
  auto ns = NeighborsAt({0.0f, -9.0f, -49.0f, -499.0f, 1.0f, 2.0f});
  ConvertVectorArmScoresToSimilarity(ns, data_model::DISTANCE_METRIC_IP);
  EXPECT_FLOAT_EQ(ns[0].score, 0.5f);
  EXPECT_FLOAT_EQ(ns[1].score, -4.0f);
  EXPECT_FLOAT_EQ(ns[2].score, -24.0f);
  EXPECT_FLOAT_EQ(ns[3].score, -249.0f);
  EXPECT_FLOAT_EQ(ns[4].score, 1.0f);
  EXPECT_FLOAT_EQ(ns[5].score, 1.5f);
}

TEST_F(FusionConversionTest, InnerProductKeepsDistinctDistancesDistinct) {
  // The regression this exists for: three distances below -1 used to map onto
  // one value, so the arm could no longer order its own best matches.
  auto ns = NeighborsAt({-9.0f, -49.0f, -499.0f});
  ConvertVectorArmScoresToSimilarity(ns, data_model::DISTANCE_METRIC_IP);
  EXPECT_GT(ns[0].score, ns[1].score);
  EXPECT_GT(ns[1].score, ns[2].score);
}

// Cosine distance runs [0, 2], so the map is the affine one that lands on
// [0, 1] -- not the reciprocal, which would put an opposed vector at 1/3.
TEST_F(FusionConversionTest, CosineSpansZeroToOne) {
  auto ns = NeighborsAt({0.0f, 0.5f, 1.0f, 2.0f});
  ConvertVectorArmScoresToSimilarity(ns, data_model::DISTANCE_METRIC_COSINE);
  EXPECT_FLOAT_EQ(ns[0].score, 1.0f);
  EXPECT_FLOAT_EQ(ns[1].score, 0.75f);
  EXPECT_FLOAT_EQ(ns[2].score, 0.5f);
  EXPECT_FLOAT_EQ(ns[3].score, 0.0f);
}

TEST_F(FusionConversionTest, CosineSelfDistanceLandsJustAboveOne) {
  // What a real index produces for a document matching the query: the
  // arithmetic does not cancel exactly, so the distance is slightly negative
  // and the similarity slightly above 1. It must not be clamped.
  auto ns = NeighborsAt({-1.6391277e-07f});
  ConvertVectorArmScoresToSimilarity(ns, data_model::DISTANCE_METRIC_COSINE);
  EXPECT_GT(ns[0].score, 1.0f);
  EXPECT_LT(ns[0].score, 1.001f);
}

// Each mapping is strictly monotone, so no two distances are conflated -- but
// the DIRECTION is not the same for all three, and that is deliberate.
//
// L2 and cosine decrease in the distance, so a nearer document scores higher.
// Inner product increases in it. Since an inner-product distance is `1 - dot`,
// where a smaller distance means a better match, that means an inner-product
// arm reports its best matches with its lowest similarity. That is what the
// reference engine does -- measured, not assumed -- and matching it is the
// point of these numbers, so the oddity is inherited on purpose and pinned
// here so nobody "fixes" it into an incompatibility.
TEST_F(FusionConversionTest, EachMetricIsStrictlyMonotoneInItsOwnDirection) {
  struct Case {
    data_model::DistanceMetric metric;
    std::vector<float> ascending_distances;
    bool similarity_decreases;
  };
  const std::vector<Case> cases = {
      {data_model::DISTANCE_METRIC_L2, {0.0f, 0.25f, 1.0f, 9.0f}, true},
      {data_model::DISTANCE_METRIC_COSINE, {0.0f, 0.5f, 1.0f, 2.0f}, true},
      {data_model::DISTANCE_METRIC_IP, {-499.0f, -9.0f, 0.0f, 1.0f}, false},
  };
  for (const auto &c : cases) {
    auto ns = NeighborsAt(c.ascending_distances);
    ConvertVectorArmScoresToSimilarity(ns, c.metric);
    for (size_t i = 1; i < ns.size(); ++i) {
      if (c.similarity_decreases) {
        EXPECT_LT(ns[i].score, ns[i - 1].score)
            << "metric " << c.metric << " at distance "
            << c.ascending_distances[i];
      } else {
        EXPECT_GT(ns[i].score, ns[i - 1].score)
            << "metric " << c.metric << " at distance "
            << c.ascending_distances[i];
      }
    }
  }
}

// An unset metric is the shape a non-vector arm would present. It must not
// crash or produce a non-finite score; falling back to the L2 mapping is the
// conservative answer.
TEST_F(FusionConversionTest, UnspecifiedMetricFallsBackToL2) {
  auto ns = NeighborsAt({0.0f, 1.0f, 3.0f});
  ConvertVectorArmScoresToSimilarity(ns,
                                     data_model::DISTANCE_METRIC_UNSPECIFIED);
  EXPECT_FLOAT_EQ(ns[0].score, 1.0f);
  EXPECT_FLOAT_EQ(ns[1].score, 0.5f);
  EXPECT_FLOAT_EQ(ns[2].score, 0.25f);
}

TEST_F(FusionConversionTest, EmptyArmIsLeftAlone) {
  std::vector<indexes::Neighbor> ns;
  ConvertVectorArmScoresToSimilarity(ns, data_model::DISTANCE_METRIC_IP);
  EXPECT_TRUE(ns.empty());
}

}  // namespace
}  // namespace query
}  // namespace valkey_search
