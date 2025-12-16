/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/flat_position_map.h"

#include <algorithm>
#include <map>
#include <memory>
#include <random>
#include <vector>

#include "gtest/gtest.h"
#include "src/indexes/text/posting.h"

namespace valkey_search::indexes::text {

class FlatPositionMapTest : public ::testing::Test {
 protected:
  std::map<Position, std::unique_ptr<FieldMask>> CreatePositionMap(
      const std::vector<std::pair<Position, uint64_t>>& positions,
      size_t num_fields) {
    std::map<Position, std::unique_ptr<FieldMask>> position_map;
    for (const auto& [pos, mask] : positions) {
      auto field_mask = FieldMask::Create(num_fields);
      for (size_t i = 0; i < num_fields; ++i) {
        if (mask & (1ULL << i)) {
          field_mask->SetField(i);
        }
      }
      position_map[pos] = std::move(field_mask);
    }
    return position_map;
  }

  // Helper to verify iteration correctness
  void VerifyIteration(
      const FlatPositionMap& flat_map,
      const std::vector<std::pair<Position, uint64_t>>& expected) {
    PositionIterator iter(flat_map);
    size_t idx = 0;
    while (iter.IsValid()) {
      ASSERT_LT(idx, expected.size()) << "More positions than expected";
      EXPECT_EQ(iter.GetPosition(), expected[idx].first)
          << "Position mismatch at index " << idx;
      EXPECT_EQ(iter.GetFieldMask(), expected[idx].second)
          << "Field mask mismatch at index " << idx;
      iter.NextPosition();
      idx++;
    }
    EXPECT_EQ(idx, expected.size()) << "Fewer positions than expected";
  }
};

//=============================================================================
// Core Functionality Tests
//=============================================================================

TEST_F(FlatPositionMapTest, EmptyMap) {
  std::map<Position, std::unique_ptr<FieldMask>> empty_map;
  EXPECT_DEATH(FlatPositionMap flat_map(empty_map, 1),
               "Cannot create FlatPositionMap from empty position_map");
}

TEST_F(FlatPositionMapTest, SinglePositionSingleField) {
  auto position_map = CreatePositionMap({{100, 1}}, 1);
  FlatPositionMap flat_map(position_map, 1);

  PositionIterator iter(flat_map);
  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 100);
  EXPECT_EQ(iter.GetFieldMask(), 1ULL);

  iter.NextPosition();
  EXPECT_FALSE(iter.IsValid());

  EXPECT_EQ(flat_map.CountPositions(), 1);
  EXPECT_EQ(flat_map.CountTermFrequency(), 1);
}

TEST_F(FlatPositionMapTest, MultiplePositionsIteration) {
  auto position_map =
      CreatePositionMap({{10, 1}, {25, 1}, {50, 1}, {75, 1}}, 1);
  FlatPositionMap flat_map(position_map, 1);

  PositionIterator iter(flat_map);
  EXPECT_EQ(iter.GetPosition(), 10);
  iter.NextPosition();
  EXPECT_EQ(iter.GetPosition(), 25);
  iter.NextPosition();
  EXPECT_EQ(iter.GetPosition(), 50);
  iter.NextPosition();
  EXPECT_EQ(iter.GetPosition(), 75);
  iter.NextPosition();
  EXPECT_FALSE(iter.IsValid());
}

TEST_F(FlatPositionMapTest, LargeDeltaEncoding) {
  auto position_map = CreatePositionMap({{1, 1}, {1000, 1}, {100000, 1}}, 1);
  FlatPositionMap flat_map(position_map, 1);

  PositionIterator iter(flat_map);
  EXPECT_EQ(iter.GetPosition(), 1);
  iter.NextPosition();
  EXPECT_EQ(iter.GetPosition(), 1000);
  iter.NextPosition();
  EXPECT_EQ(iter.GetPosition(), 100000);
}

//=============================================================================
// Field Mask Tests
//=============================================================================

TEST_F(FlatPositionMapTest, MultipleFields) {
  auto position_map =
      CreatePositionMap({{10, 0b001}, {20, 0b010}, {30, 0b100}}, 3);
  FlatPositionMap flat_map(position_map, 3);

  PositionIterator iter(flat_map);
  EXPECT_EQ(iter.GetPosition(), 10);
  EXPECT_EQ(iter.GetFieldMask(), 0b001ULL);
  iter.NextPosition();

  EXPECT_EQ(iter.GetPosition(), 20);
  EXPECT_EQ(iter.GetFieldMask(), 0b010ULL);
  iter.NextPosition();

  EXPECT_EQ(iter.GetPosition(), 30);
  EXPECT_EQ(iter.GetFieldMask(), 0b100ULL);
}

TEST_F(FlatPositionMapTest, SingleFieldOptimization) {
  // Single field maps don't store field masks
  auto position_map = CreatePositionMap({{10, 1}, {20, 1}, {30, 1}}, 1);
  FlatPositionMap flat_map(position_map, 1);

  PositionIterator iter(flat_map);
  while (iter.IsValid()) {
    EXPECT_EQ(iter.GetFieldMask(), 1ULL);
    iter.NextPosition();
  }
}

TEST_F(FlatPositionMapTest, AllFieldsSet) {
  uint64_t all_fields = ~0ULL;
  auto position_map = CreatePositionMap({{100, all_fields}}, 64);
  FlatPositionMap flat_map(position_map, 64);

  PositionIterator iter(flat_map);
  EXPECT_EQ(iter.GetFieldMask(), all_fields);
}

TEST_F(FlatPositionMapTest, TermFrequencyCalculation) {
  // Position 10: 1 field, Position 20: 2 fields, Position 30: 3 fields
  auto position_map =
      CreatePositionMap({{10, 0b001}, {20, 0b011}, {30, 0b111}}, 3);
  FlatPositionMap flat_map(position_map, 3);

  EXPECT_EQ(flat_map.CountTermFrequency(), 6);  // 1+2+3
}

//=============================================================================
// SkipForward Tests
//=============================================================================

TEST_F(FlatPositionMapTest, SkipToExistingPosition) {
  auto position_map =
      CreatePositionMap({{10, 1}, {20, 2}, {30, 4}, {40, 8}}, 4);
  FlatPositionMap flat_map(position_map, 4);

  PositionIterator iter(flat_map);
  EXPECT_TRUE(iter.SkipForwardPosition(30));
  EXPECT_EQ(iter.GetPosition(), 30);
  EXPECT_EQ(iter.GetFieldMask(), 4ULL);
}

TEST_F(FlatPositionMapTest, SkipToNonExistingPosition) {
  auto position_map = CreatePositionMap({{10, 1}, {30, 2}, {50, 4}}, 3);
  FlatPositionMap flat_map(position_map, 3);

  PositionIterator iter(flat_map);
  EXPECT_FALSE(iter.SkipForwardPosition(20));
  EXPECT_EQ(iter.GetPosition(), 30);  // Next position >= target
}

TEST_F(FlatPositionMapTest, SkipBeyondEnd) {
  auto position_map = CreatePositionMap({{10, 1}, {20, 2}}, 2);
  FlatPositionMap flat_map(position_map, 2);

  PositionIterator iter(flat_map);
  EXPECT_FALSE(iter.SkipForwardPosition(100));
  EXPECT_FALSE(iter.IsValid());
}

//=============================================================================
// Partition Tests
//=============================================================================

TEST_F(FlatPositionMapTest, LargeMapWithPartitions) {
  std::vector<std::pair<Position, uint64_t>> positions;
  for (int i = 0; i < 200; ++i) {
    positions.push_back({i * 10, 1ULL << (i % 4)});
  }
  auto position_map = CreatePositionMap(positions, 4);
  FlatPositionMap flat_map(position_map, 4);

  EXPECT_EQ(flat_map.CountPositions(), 200);

  // Verify iteration
  PositionIterator iter(flat_map);
  for (int i = 0; i < 200; ++i) {
    EXPECT_TRUE(iter.IsValid());
    EXPECT_EQ(iter.GetPosition(), i * 10);
    iter.NextPosition();
  }
  EXPECT_FALSE(iter.IsValid());
}

TEST_F(FlatPositionMapTest, SkipForwardWithPartitions) {
  std::vector<std::pair<Position, uint64_t>> positions;
  for (int i = 0; i < 300; ++i) {
    positions.push_back({i * 5, 1ULL});
  }
  auto position_map = CreatePositionMap(positions, 1);
  FlatPositionMap flat_map(position_map, 1);

  PositionIterator iter(flat_map);
  EXPECT_TRUE(iter.SkipForwardPosition(750));
  EXPECT_EQ(iter.GetPosition(), 750);
}

//=============================================================================
// Move Semantics Tests
//=============================================================================

TEST_F(FlatPositionMapTest, MoveConstructor) {
  auto position_map = CreatePositionMap({{10, 1}, {20, 2}}, 2);
  FlatPositionMap map1(position_map, 2);
  const char* data = map1.data();

  FlatPositionMap map2(std::move(map1));

  EXPECT_EQ(map2.data(), data);
  EXPECT_EQ(map1.data(), nullptr);
  EXPECT_EQ(map2.CountPositions(), 2);
}

TEST_F(FlatPositionMapTest, MoveAssignment) {
  auto position_map1 = CreatePositionMap({{10, 1}}, 1);
  auto position_map2 = CreatePositionMap({{20, 2}}, 1);

  FlatPositionMap map1(position_map1, 1);
  FlatPositionMap map2(position_map2, 1);
  const char* data2 = map2.data();

  map1 = std::move(map2);

  EXPECT_EQ(map1.data(), data2);
  EXPECT_EQ(map2.data(), nullptr);
  EXPECT_EQ(map1.CountPositions(), 1);
}

//=============================================================================
// Stress Test
//=============================================================================

TEST_F(FlatPositionMapTest, StressTest) {
  std::vector<std::pair<Position, uint64_t>> positions;
  Position pos = 0;
  for (int i = 0; i < 1000; ++i) {
    pos += (i % 10) + 1;
    positions.push_back({pos, 1ULL << (i % 8)});
  }

  auto position_map = CreatePositionMap(positions, 8);
  FlatPositionMap flat_map(position_map, 8);

  EXPECT_EQ(flat_map.CountPositions(), 1000);

  PositionIterator iter(flat_map);
  for (size_t i = 0; i < positions.size(); ++i) {
    ASSERT_TRUE(iter.IsValid()) << "Failed at index " << i;
    EXPECT_EQ(iter.GetPosition(), positions[i].first);
    EXPECT_EQ(iter.GetFieldMask(), positions[i].second);
    iter.NextPosition();
  }
  EXPECT_FALSE(iter.IsValid());
}

//=============================================================================
// Random Test Generation - Edge Case Focused
//=============================================================================

class RandomPositionMapGenerator {
 public:
  explicit RandomPositionMapGenerator(uint32_t seed) : rng_(seed) {}

  // Generate random number of elements with distribution focused on edge cases
  size_t GenerateNumElements() {
    // Distribution: 30% small (1-10), 30% medium (11-50), 20% large (51-200),
    // 20% very large (201-500)
    std::uniform_int_distribution<int> category_dist(0, 9);
    int category = category_dist(rng_);

    if (category < 3) {  // 30% small
      return std::uniform_int_distribution<size_t>(1, 10)(rng_);
    } else if (category < 6) {  // 30% medium
      return std::uniform_int_distribution<size_t>(11, 50)(rng_);
    } else if (category < 8) {  // 20% large
      return std::uniform_int_distribution<size_t>(51, 200)(rng_);
    } else {  // 20% very large
      return std::uniform_int_distribution<size_t>(201, 500)(rng_);
    }
  }

  // Generate position delta with edge case distribution
  // Focuses on partition boundaries (128 bytes ~= 40-50 positions with small
  // deltas)
  Position GenerateDelta() {
    std::uniform_int_distribution<int> category_dist(0, 99);
    int category = category_dist(rng_);

    if (category < 40) {  // 40% tiny deltas (1-5)
      return std::uniform_int_distribution<Position>(1, 5)(rng_);
    } else if (category < 70) {  // 30% small deltas (6-63, fit in 1 byte)
      return std::uniform_int_distribution<Position>(6, 63)(rng_);
    } else if (category < 85) {  // 15% medium deltas (64-4095, fit in 2 bytes)
      return std::uniform_int_distribution<Position>(64, 4095)(rng_);
    } else if (category < 95) {  // 10% large deltas (4096-65535)
      return std::uniform_int_distribution<Position>(4096, 65535)(rng_);
    } else {  // 5% very large deltas
      return std::uniform_int_distribution<Position>(65536, 1000000)(rng_);
    }
  }

  // Generate field mask with edge cases
  uint64_t GenerateFieldMask(size_t num_fields) {
    if (num_fields == 0) return 0;  // Should never happen
    if (num_fields == 1) return 1;

    std::uniform_int_distribution<int> category_dist(0, 9);
    int category = category_dist(rng_);

    if (category < 2) {  // 20% all bits set
      // Handle num_fields=64 specially to avoid UB with (1ULL << 64)
      if (num_fields >= 64) {
        return ~0ULL;  // All bits set
      }
      return (1ULL << num_fields) - 1;
    } else if (category < 4) {  // 20% single bit
      size_t bit =
          std::uniform_int_distribution<size_t>(0, num_fields - 1)(rng_);
      return 1ULL << bit;
    } else {  // 60% random multiple bits
      uint64_t mask = 0;
      size_t num_bits =
          std::uniform_int_distribution<size_t>(1, num_fields)(rng_);
      for (size_t i = 0; i < num_bits; ++i) {
        size_t bit = std::uniform_int_distribution<size_t>(
            0, std::min(num_fields, size_t{64}) - 1)(rng_);
        mask |= (1ULL << bit);
      }
      return mask ? mask : 1ULL;  // Ensure at least one bit set
    }
  }

  // Generate number of fields with edge case focus
  size_t GenerateNumFields() {
    std::uniform_int_distribution<int> category_dist(0, 9);
    int category = category_dist(rng_);

    if (category < 5) {  // 50% single field (important optimization case)
      return 1;
    } else if (category < 7) {  // 20% two fields
      return 2;
    } else if (category < 9) {  // 20% small number (3-8)
      return std::uniform_int_distribution<size_t>(3, 8)(rng_);
    } else {  // 10% larger number (9-64)
      return std::uniform_int_distribution<size_t>(9, 64)(rng_);
    }
  }

  // Generate complete random position map
  std::vector<std::pair<Position, uint64_t>> GeneratePositionMap(
      size_t num_elements, size_t num_fields) {
    std::vector<std::pair<Position, uint64_t>> positions;
    Position current_pos = 0;

    for (size_t i = 0; i < num_elements; ++i) {
      current_pos += GenerateDelta();
      uint64_t mask = GenerateFieldMask(num_fields);
      positions.push_back({current_pos, mask});
    }

    return positions;
  }

  // Generate random skip targets for testing
  std::vector<Position> GenerateSkipTargets(
      const std::vector<std::pair<Position, uint64_t>>& positions,
      size_t num_targets) {
    std::vector<Position> targets;
    if (positions.empty()) return targets;

    Position min_pos = positions.front().first;
    Position max_pos = positions.back().first;

    for (size_t i = 0; i < num_targets; ++i) {
      std::uniform_int_distribution<int> category_dist(0, 9);
      int category = category_dist(rng_);

      if (category < 5 && !positions.empty()) {  // 50% exact position
        size_t idx = std::uniform_int_distribution<size_t>(
            0, positions.size() - 1)(rng_);
        targets.push_back(positions[idx].first);
      } else if (category < 8) {  // 30% random position in range (>= min_pos)
        targets.push_back(
            std::uniform_int_distribution<Position>(min_pos, max_pos)(rng_));
      } else {  // 20% beyond end
        targets.push_back(
            max_pos + std::uniform_int_distribution<Position>(1, 1000)(rng_));
      }
    }

    std::sort(targets.begin(), targets.end());
    return targets;
  }

 private:
  std::mt19937 rng_;
};

// Parameterized test for random position maps
TEST_F(FlatPositionMapTest, RandomMapGeneration_1000Tests) {
  const size_t kNumTests = 1000;
  RandomPositionMapGenerator gen(42);  // Fixed seed for reproducibility

  for (size_t test_num = 0; test_num < kNumTests; ++test_num) {
    // Generate random parameters
    size_t num_elements = gen.GenerateNumElements();
    size_t num_fields = gen.GenerateNumFields();

    // Generate random position map
    auto positions = gen.GeneratePositionMap(num_elements, num_fields);
    auto position_map = CreatePositionMap(positions, num_fields);

    // Create flat map
    FlatPositionMap flat_map(position_map, num_fields);

    // Verify basic properties
    ASSERT_EQ(flat_map.CountPositions(), num_elements)
        << "Test " << test_num << " failed: position count mismatch";

    // Verify complete iteration
    VerifyIteration(flat_map, positions);

    // Test random skip forwards - create new iterator for each target
    size_t num_skip_tests = std::min(size_t{20}, num_elements);
    auto skip_targets = gen.GenerateSkipTargets(positions, num_skip_tests);

    for (const auto& target : skip_targets) {
      // Skip targets that are before the first position (iterator starts there)
      if (target < positions[0].first) continue;

      PositionIterator iter(flat_map);  // Fresh iterator for each target
      bool exact_match = iter.SkipForwardPosition(target);

      if (iter.IsValid()) {
        EXPECT_GE(iter.GetPosition(), target)
            << "Test " << test_num
            << " failed: skip forward didn't reach target";

        // If exact match claimed, verify it
        if (exact_match) {
          EXPECT_EQ(iter.GetPosition(), target)
              << "Test " << test_num
              << " failed: exact match claimed but position differs";
        }

        // Verify we can still iterate from here
        Position prev_pos = iter.GetPosition();
        iter.NextPosition();
        if (iter.IsValid()) {
          EXPECT_GT(iter.GetPosition(), prev_pos)
              << "Test " << test_num
              << " failed: positions not monotonic after skip";
        }
      }
    }
  }
}

// Edge case focused tests
TEST_F(FlatPositionMapTest, EdgeCase_ZeroPartitions) {
  // Generate maps that will have exactly 0 partitions (< 128 bytes)
  std::mt19937 rng(100);

  for (int test = 0; test < 100; ++test) {
    // Small number of elements with tiny deltas to stay under 128 bytes
    size_t num_elements = std::uniform_int_distribution<size_t>(1, 10)(rng);
    size_t num_fields = 1;  // Single field to minimize size

    std::vector<std::pair<Position, uint64_t>> positions;
    Position pos = 0;
    for (size_t i = 0; i < num_elements; ++i) {
      pos += std::uniform_int_distribution<Position>(1, 5)(rng);
      positions.push_back({pos, 1});
    }

    auto position_map = CreatePositionMap(positions, num_fields);
    FlatPositionMap flat_map(position_map, num_fields);

    VerifyIteration(flat_map, positions);
  }
}

TEST_F(FlatPositionMapTest, EdgeCase_OnePartition) {
  // Generate maps that will have exactly 1 partition (128-256 bytes)
  std::mt19937 rng(200);

  for (int test = 0; test < 100; ++test) {
    // Aim for size that creates exactly 1 partition
    size_t num_elements = std::uniform_int_distribution<size_t>(40, 80)(rng);
    size_t num_fields = 1;

    std::vector<std::pair<Position, uint64_t>> positions;
    Position pos = 0;
    for (size_t i = 0; i < num_elements; ++i) {
      pos += std::uniform_int_distribution<Position>(1, 10)(rng);
      positions.push_back({pos, 1});
    }

    auto position_map = CreatePositionMap(positions, num_fields);
    FlatPositionMap flat_map(position_map, num_fields);

    VerifyIteration(flat_map, positions);

    // Test skip forward across partition
    if (positions.size() > 1) {
      PositionIterator iter(flat_map);
      Position target = positions[positions.size() / 2].first;
      iter.SkipForwardPosition(target);
      EXPECT_GE(iter.GetPosition(), target);
    }
  }
}

TEST_F(FlatPositionMapTest, EdgeCase_TwoPartitions) {
  // Generate maps that will have exactly 2 partitions (256-384 bytes)
  std::mt19937 rng(300);

  for (int test = 0; test < 100; ++test) {
    size_t num_elements = std::uniform_int_distribution<size_t>(80, 120)(rng);
    size_t num_fields = 1;

    std::vector<std::pair<Position, uint64_t>> positions;
    Position pos = 0;
    for (size_t i = 0; i < num_elements; ++i) {
      pos += std::uniform_int_distribution<Position>(1, 10)(rng);
      positions.push_back({pos, 1});
    }

    auto position_map = CreatePositionMap(positions, num_fields);
    FlatPositionMap flat_map(position_map, num_fields);

    VerifyIteration(flat_map, positions);
  }
}

TEST_F(FlatPositionMapTest, EdgeCase_LastPartitionSingleEntry) {
  // Create maps where last partition has exactly 1 entry
  std::mt19937 rng(400);

  for (int test = 0; test < 50; ++test) {
    // Create enough entries to fill N partitions, then add exactly 1 more
    size_t base_elements = std::uniform_int_distribution<size_t>(40, 80)(rng);
    size_t num_fields = 1;

    std::vector<std::pair<Position, uint64_t>> positions;
    Position pos = 0;

    // Fill base partitions
    for (size_t i = 0; i < base_elements; ++i) {
      pos += std::uniform_int_distribution<Position>(1, 5)(rng);
      positions.push_back({pos, 1});
    }

    // Add one more position with large delta to force new partition
    pos += std::uniform_int_distribution<Position>(10000, 100000)(rng);
    positions.push_back({pos, 1});

    auto position_map = CreatePositionMap(positions, num_fields);
    FlatPositionMap flat_map(position_map, num_fields);

    VerifyIteration(flat_map, positions);

    // Verify we can skip to the last position
    PositionIterator iter(flat_map);
    EXPECT_TRUE(iter.SkipForwardPosition(pos));
    EXPECT_EQ(iter.GetPosition(), pos);
  }
}

TEST_F(FlatPositionMapTest, EdgeCase_MixedFieldMaskChanges) {
  // Test field mask changes at partition boundaries and within partitions
  RandomPositionMapGenerator gen(500);
  std::mt19937 rng(500);

  for (int test = 0; test < 100; ++test) {
    size_t num_elements = std::uniform_int_distribution<size_t>(50, 200)(rng);
    size_t num_fields = std::uniform_int_distribution<size_t>(2, 8)(rng);

    std::vector<std::pair<Position, uint64_t>> positions;
    Position pos = 0;
    uint64_t prev_mask = 0;

    for (size_t i = 0; i < num_elements; ++i) {
      pos += gen.GenerateDelta();

      // Sometimes keep same mask, sometimes change it
      uint64_t mask;
      if (i > 0 && std::uniform_int_distribution<int>(0, 2)(rng) == 0) {
        mask = prev_mask;  // 33% keep same
      } else {
        mask = gen.GenerateFieldMask(num_fields);  // 67% change
      }

      positions.push_back({pos, mask});
      prev_mask = mask;
    }

    auto position_map = CreatePositionMap(positions, num_fields);
    FlatPositionMap flat_map(position_map, num_fields);

    VerifyIteration(flat_map, positions);
  }
}

TEST_F(FlatPositionMapTest, RandomSkipForwardPatterns) {
  // Test various skip forward patterns with fresh iterators
  RandomPositionMapGenerator gen(600);
  std::mt19937 rng(600);

  for (int test = 0; test < 100; ++test) {
    size_t num_elements = std::uniform_int_distribution<size_t>(50, 200)(rng);
    size_t num_fields = gen.GenerateNumFields();

    auto positions = gen.GeneratePositionMap(num_elements, num_fields);
    auto position_map = CreatePositionMap(positions, num_fields);
    FlatPositionMap flat_map(position_map, num_fields);

    // Test skip forwards to various positions with fresh iterators
    size_t num_skips = std::min(size_t{10}, num_elements);

    for (size_t i = 0; i < num_skips; ++i) {
      // Pick random target from available positions
      size_t target_idx =
          std::uniform_int_distribution<size_t>(0, num_elements - 1)(rng);
      Position target = positions[target_idx].first;

      // Use fresh iterator for each skip
      PositionIterator iter(flat_map);
      bool exact = iter.SkipForwardPosition(target);

      ASSERT_TRUE(iter.IsValid())
          << "Test " << test << " skip " << i << " failed";
      EXPECT_GE(iter.GetPosition(), target);

      if (exact) {
        EXPECT_EQ(iter.GetPosition(), target);
        EXPECT_EQ(iter.GetFieldMask(), positions[target_idx].second);
      }
    }
  }
}

TEST_F(FlatPositionMapTest, RandomMapWithTermFrequencyVerification) {
  // Verify term frequency calculation across random maps
  RandomPositionMapGenerator gen(700);
  std::mt19937 rng(700);

  for (int test = 0; test < 100; ++test) {
    size_t num_elements = std::uniform_int_distribution<size_t>(10, 100)(rng);
    size_t num_fields = std::uniform_int_distribution<size_t>(1, 16)(rng);

    auto positions = gen.GeneratePositionMap(num_elements, num_fields);
    auto position_map = CreatePositionMap(positions, num_fields);
    FlatPositionMap flat_map(position_map, num_fields);

    // Calculate expected term frequency
    size_t expected_freq = 0;
    for (const auto& [pos, mask] : positions) {
      expected_freq += __builtin_popcountll(mask);
    }

    EXPECT_EQ(flat_map.CountTermFrequency(), expected_freq)
        << "Test " << test << " failed: term frequency mismatch";
  }
}

TEST_F(FlatPositionMapTest, VeryLargeMapScenarios) {
  // Test very large position maps (100K-500K positions) across multiple
  // scenarios
  RandomPositionMapGenerator gen(800);
  std::mt19937 rng(800);

  // Test different size categories
  std::vector<size_t> test_sizes = {100000, 150000, 250000, 500000};

  for (size_t target_size : test_sizes) {
    // Scenario 1: Single field (best compression)
    {
      std::vector<std::pair<Position, uint64_t>> positions;
      Position pos = 0;
      for (size_t i = 0; i < target_size; ++i) {
        pos += std::uniform_int_distribution<Position>(1, 10)(rng);
        positions.push_back({pos, 1});
      }

      auto position_map = CreatePositionMap(positions, 1);
      FlatPositionMap flat_map(position_map, 1);

      ASSERT_EQ(flat_map.CountPositions(), target_size);
      EXPECT_EQ(flat_map.CountTermFrequency(), target_size);

      // Test skip to various positions
      PositionIterator iter1(flat_map);
      iter1.SkipForwardPosition(positions[target_size / 4].first);
      EXPECT_TRUE(iter1.IsValid());
      EXPECT_GE(iter1.GetPosition(), positions[target_size / 4].first);

      PositionIterator iter2(flat_map);
      iter2.SkipForwardPosition(positions[target_size / 2].first);
      EXPECT_TRUE(iter2.IsValid());
      EXPECT_GE(iter2.GetPosition(), positions[target_size / 2].first);

      PositionIterator iter3(flat_map);
      iter3.SkipForwardPosition(positions[target_size * 3 / 4].first);
      EXPECT_TRUE(iter3.IsValid());
      EXPECT_GE(iter3.GetPosition(), positions[target_size * 3 / 4].first);
    }

    // Scenario 2: Multiple fields with varying masks
    if (target_size <= 250000) {  // Limit for time
      std::vector<std::pair<Position, uint64_t>> positions;
      Position pos = 0;
      for (size_t i = 0; i < target_size; ++i) {
        pos += std::uniform_int_distribution<Position>(1, 5)(rng);
        uint64_t mask = 1ULL << (i % 8);  // Cycle through 8 different masks
        positions.push_back({pos, mask});
      }

      auto position_map = CreatePositionMap(positions, 8);
      FlatPositionMap flat_map(position_map, 8);

      ASSERT_EQ(flat_map.CountPositions(), target_size);

      // Verify first position and skip to end
      PositionIterator iter(flat_map);
      EXPECT_EQ(iter.GetPosition(), positions[0].first);
      EXPECT_EQ(iter.GetFieldMask(), positions[0].second);

      iter.SkipForwardPosition(positions[target_size - 1].first);
      EXPECT_TRUE(iter.IsValid());
      EXPECT_GE(iter.GetPosition(), positions[target_size - 1].first);
    }

    // Scenario 3: Large deltas (sparse positions)
    if (target_size == 100000) {
      std::vector<std::pair<Position, uint64_t>> positions;
      Position pos = 0;
      for (size_t i = 0; i < target_size; ++i) {
        pos += std::uniform_int_distribution<Position>(10, 100)(rng);
        positions.push_back({pos, 1});
      }

      auto position_map = CreatePositionMap(positions, 1);
      FlatPositionMap flat_map(position_map, 1);

      ASSERT_EQ(flat_map.CountPositions(), target_size);

      // Test partition-crossing skips
      for (size_t skip_test = 0; skip_test < 10; ++skip_test) {
        size_t target_idx =
            std::uniform_int_distribution<size_t>(0, target_size - 1)(rng);
        PositionIterator iter(flat_map);
        iter.SkipForwardPosition(positions[target_idx].first);
        EXPECT_TRUE(iter.IsValid());
        EXPECT_GE(iter.GetPosition(), positions[target_idx].first);
      }
    }
  }
}

}  // namespace valkey_search::indexes::text
