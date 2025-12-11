/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/flat_position_map.h"

#include <map>
#include <memory>

#include "gtest/gtest.h"
#include "src/indexes/text/posting.h"

namespace valkey_search::indexes::text {

class FlatPositionMapTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void TearDown() override { allocated_maps_.clear(); }

  // Helper to create a position map with field masks
  std::map<Position, std::unique_ptr<FieldMask>> CreatePositionMap(
      const std::vector<std::pair<Position, uint64_t>>& positions,
      size_t num_fields) {
    std::map<Position, std::unique_ptr<FieldMask>> position_map;
    for (const auto& [pos, mask] : positions) {
      auto field_mask = FieldMask::Create(num_fields);
      // Set fields based on mask bits
      for (size_t i = 0; i < num_fields; ++i) {
        if (mask & (1ULL << i)) {
          field_mask->SetField(i);
        }
      }
      position_map[pos] = std::move(field_mask);
    }
    return position_map;
  }

  // Helper to track allocated maps for cleanup
  FlatPositionMap TrackMap(FlatPositionMap map) {
    allocated_maps_.push_back(std::move(map));
    return std::move(allocated_maps_.back());
  }

  std::vector<FlatPositionMap> allocated_maps_;
};

//=============================================================================
// Basic Serialization and Iteration Tests
//=============================================================================

TEST_F(FlatPositionMapTest, BasicSerializationWithSmallDeltas) {
  auto position_map = CreatePositionMap({{5, 1}, {10, 1}, {15, 1}, {20, 1}}, 1);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 1));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 5);

  iter.NextPosition();
  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 10);

  iter.NextPosition();
  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 15);

  iter.NextPosition();
  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 20);

  iter.NextPosition();
  EXPECT_FALSE(iter.IsValid());
}

TEST_F(FlatPositionMapTest, MultipleFieldsWithVariableDeltas) {
  auto position_map =
      CreatePositionMap({{10, 1}, {50, 2}, {150, 4}, {500, 7}}, 3);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 3));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 10);
  EXPECT_EQ(iter.GetFieldMask(), 1ULL);

  iter.NextPosition();
  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 50);
  EXPECT_EQ(iter.GetFieldMask(), 2ULL);

  iter.NextPosition();
  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 150);
  EXPECT_EQ(iter.GetFieldMask(), 4ULL);

  iter.NextPosition();
  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 500);
  EXPECT_EQ(iter.GetFieldMask(), 7ULL);

  iter.NextPosition();
  EXPECT_FALSE(iter.IsValid());
}

TEST_F(FlatPositionMapTest, LargePositionMapWithPartitions) {
  // Create a large position map to trigger partition creation (128+ bytes)
  std::vector<std::pair<Position, uint64_t>> positions;
  for (int i = 0; i < 150; ++i) {
    positions.push_back({i * 10, 1ULL << (i % 5)});
  }
  auto position_map = CreatePositionMap(positions, 5);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 5));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  // Verify all positions are correct
  for (int i = 0; i < 150; ++i) {
    EXPECT_TRUE(iter.IsValid());
    EXPECT_EQ(iter.GetPosition(), i * 10);
    EXPECT_EQ(iter.GetFieldMask(), 1ULL << (i % 5));
    iter.NextPosition();
  }

  EXPECT_FALSE(iter.IsValid());
}

//=============================================================================
// SkipForward Tests
//=============================================================================

TEST_F(FlatPositionMapTest, SkipForwardToExactPosition) {
  auto position_map =
      CreatePositionMap({{10, 1}, {20, 2}, {30, 4}, {40, 8}, {50, 16}}, 5);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 5));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  // Skip to exact position
  EXPECT_TRUE(iter.SkipForwardPosition(30));
  EXPECT_EQ(iter.GetPosition(), 30);
  EXPECT_EQ(iter.GetFieldMask(), 4ULL);

  // Continue from skipped position
  iter.NextPosition();
  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 40);
  EXPECT_EQ(iter.GetFieldMask(), 8ULL);
}

TEST_F(FlatPositionMapTest, SkipForwardToNonExistentPosition) {
  auto position_map = CreatePositionMap({{10, 1}, {30, 2}, {50, 4}}, 3);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 3));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  // Skip to position between existing positions
  EXPECT_FALSE(iter.SkipForwardPosition(25));
  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 30);  // Should land on next position >= target
}

TEST_F(FlatPositionMapTest, SkipForwardBeyondEnd) {
  auto position_map = CreatePositionMap({{10, 1}, {20, 2}, {30, 4}}, 3);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 3));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  // Skip beyond all positions
  EXPECT_FALSE(iter.SkipForwardPosition(100));
  EXPECT_FALSE(iter.IsValid());
}

TEST_F(FlatPositionMapTest, SkipForwardWithPartitionOptimization) {
  // Create enough positions to have multiple partitions
  std::vector<std::pair<Position, uint64_t>> positions;
  for (int i = 0; i < 200; ++i) {
    positions.push_back({i * 5, 1ULL << (i % 4)});
  }
  auto position_map = CreatePositionMap(positions, 4);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 4));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  // Skip to a position far into the map (should use partition map)
  EXPECT_TRUE(iter.SkipForwardPosition(500));
  EXPECT_EQ(iter.GetPosition(), 500);
  EXPECT_EQ(iter.GetFieldMask(), 1ULL << (100 % 4));
}

//=============================================================================
// Edge Cases
//=============================================================================

TEST_F(FlatPositionMapTest, EmptyPositionMap) {
  std::map<Position, std::unique_ptr<FieldMask>> empty_map;

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(empty_map, 1));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);
  EXPECT_FALSE(iter.IsValid());
}

TEST_F(FlatPositionMapTest, SinglePosition) {
  auto position_map = CreatePositionMap({{42, 1}}, 1);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 1));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 42);
  EXPECT_EQ(iter.GetFieldMask(), 1ULL);

  iter.NextPosition();
  EXPECT_FALSE(iter.IsValid());
}

TEST_F(FlatPositionMapTest, ConsecutivePositionsMinimalDeltas) {
  auto position_map =
      CreatePositionMap({{1, 1}, {2, 1}, {3, 1}, {4, 1}, {5, 1}}, 1);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 1));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  for (Position expected = 1; expected <= 5; ++expected) {
    EXPECT_TRUE(iter.IsValid());
    EXPECT_EQ(iter.GetPosition(), expected);
    iter.NextPosition();
  }

  EXPECT_FALSE(iter.IsValid());
}

TEST_F(FlatPositionMapTest, LargeDeltasRequiringMultipleBytes) {
  // Test with deltas that require multiple bytes in variable-length encoding
  auto position_map =
      CreatePositionMap({{100, 1}, {1000, 2}, {10000, 4}, {100000, 8}}, 4);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 4));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  EXPECT_EQ(iter.GetPosition(), 100);
  iter.NextPosition();
  EXPECT_EQ(iter.GetPosition(), 1000);
  iter.NextPosition();
  EXPECT_EQ(iter.GetPosition(), 10000);
  iter.NextPosition();
  EXPECT_EQ(iter.GetPosition(), 100000);
}

//=============================================================================
// Multiple Field Tests
//=============================================================================

TEST_F(FlatPositionMapTest, MultipleFieldsAtSamePosition) {
  // Field mask with multiple bits set (fields 0, 2, 4)
  auto position_map = CreatePositionMap({{10, 0b10101}}, 5);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 5));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 10);
  EXPECT_EQ(iter.GetFieldMask(), 0b10101ULL);
}

TEST_F(FlatPositionMapTest, MaximumFieldCount) {
  // Test with 64 fields (maximum supported)
  uint64_t all_fields_mask = ~0ULL;  // All 64 bits set
  auto position_map = CreatePositionMap({{10, all_fields_mask}}, 64);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 64));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 10);
  EXPECT_EQ(iter.GetFieldMask(), all_fields_mask);
}

TEST_F(FlatPositionMapTest, FieldMaskOptimizationSingleField) {
  // When num_fields=1, field masks should not be stored
  auto position_map =
      CreatePositionMap({{10, 1}, {20, 1}, {30, 1}, {40, 1}}, 1);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 1));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  // All positions should return field mask 1 (implicit)
  for (Position expected : {10, 20, 30, 40}) {
    EXPECT_TRUE(iter.IsValid());
    EXPECT_EQ(iter.GetPosition(), expected);
    EXPECT_EQ(iter.GetFieldMask(), 1ULL);
    iter.NextPosition();
  }
}

TEST_F(FlatPositionMapTest, FieldMaskOptimizationUnchanged) {
  // Field masks should only be stored when they change
  // All positions have same mask (field 0)
  auto position_map =
      CreatePositionMap({{10, 1}, {20, 1}, {30, 1}, {40, 1}}, 3);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 3));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  for (Position expected : {10, 20, 30, 40}) {
    EXPECT_TRUE(iter.IsValid());
    EXPECT_EQ(iter.GetPosition(), expected);
    EXPECT_EQ(iter.GetFieldMask(), 1ULL);
    iter.NextPosition();
  }
}

TEST_F(FlatPositionMapTest, FieldMaskChanges) {
  // Field masks should be stored when they change
  auto position_map =
      CreatePositionMap({{10, 1}, {20, 2}, {30, 4}, {40, 2}}, 3);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 3));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  EXPECT_EQ(iter.GetPosition(), 10);
  EXPECT_EQ(iter.GetFieldMask(), 1ULL);
  iter.NextPosition();

  EXPECT_EQ(iter.GetPosition(), 20);
  EXPECT_EQ(iter.GetFieldMask(), 2ULL);
  iter.NextPosition();

  EXPECT_EQ(iter.GetPosition(), 30);
  EXPECT_EQ(iter.GetFieldMask(), 4ULL);
  iter.NextPosition();

  EXPECT_EQ(iter.GetPosition(), 40);
  EXPECT_EQ(iter.GetFieldMask(), 2ULL);
}

//=============================================================================
// Iterator Tests
//=============================================================================

TEST_F(FlatPositionMapTest, MultipleIndependentIterators) {
  auto position_map = CreatePositionMap({{5, 1}, {10, 2}, {15, 4}}, 3);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 3));
  ASSERT_NE(flat_map.data(), nullptr);

  // First iteration
  FlatPositionMapIterator iter1(flat_map);
  EXPECT_EQ(iter1.GetPosition(), 5);
  iter1.NextPosition();
  EXPECT_EQ(iter1.GetPosition(), 10);

  // Second iteration (independent)
  FlatPositionMapIterator iter2(flat_map);
  EXPECT_EQ(iter2.GetPosition(), 5);
  iter2.NextPosition();
  EXPECT_EQ(iter2.GetPosition(), 10);
  iter2.NextPosition();
  EXPECT_EQ(iter2.GetPosition(), 15);

  // First iterator should still be at position 10
  EXPECT_EQ(iter1.GetPosition(), 10);
}

TEST_F(FlatPositionMapTest, CumulativePositionAccuracy) {
  // Create a map with varying deltas
  auto position_map = CreatePositionMap(
      {{7, 1}, {15, 2}, {28, 4}, {50, 8}, {100, 16}, {200, 32}}, 6);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 6));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  std::vector<Position> expected_positions = {7, 15, 28, 50, 100, 200};

  for (size_t i = 0; i < expected_positions.size(); ++i) {
    EXPECT_TRUE(iter.IsValid());
    EXPECT_EQ(iter.GetPosition(), expected_positions[i])
        << "Failed at index " << i;
    iter.NextPosition();
  }

  EXPECT_FALSE(iter.IsValid());
}

//=============================================================================
// Public Method Tests
//=============================================================================

TEST_F(FlatPositionMapTest, CountPositions) {
  auto position_map =
      CreatePositionMap({{10, 1}, {20, 2}, {30, 4}, {40, 8}, {50, 16}}, 5);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 5));
  ASSERT_NE(flat_map.data(), nullptr);

  EXPECT_EQ(flat_map.CountPositions(), 5);
}

TEST_F(FlatPositionMapTest, CountPositionsEmpty) {
  std::map<Position, std::unique_ptr<FieldMask>> empty_map;

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(empty_map, 1));
  ASSERT_NE(flat_map.data(), nullptr);

  EXPECT_EQ(flat_map.CountPositions(), 0);
}

TEST_F(FlatPositionMapTest, CountTermFrequencySingleField) {
  // Single field means frequency = position count
  auto position_map =
      CreatePositionMap({{10, 1}, {20, 1}, {30, 1}, {40, 1}}, 1);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 1));
  ASSERT_NE(flat_map.data(), nullptr);

  EXPECT_EQ(flat_map.CountTermFrequency(), 4);
}

TEST_F(FlatPositionMapTest, CountTermFrequencyMultipleFields) {
  // Frequency = sum of popcount of all field masks
  // Position 10: field 0 (1 field)
  // Position 20: fields 0,1 (2 fields)
  // Position 30: fields 0,1,2 (3 fields)
  // Position 40: fields 0,2 (2 fields)
  // Total: 1+2+3+2 = 8
  auto position_map = CreatePositionMap(
      {{10, 0b001}, {20, 0b011}, {30, 0b111}, {40, 0b101}}, 3);

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 3));
  ASSERT_NE(flat_map.data(), nullptr);

  EXPECT_EQ(flat_map.CountTermFrequency(), 8);
}

TEST_F(FlatPositionMapTest, CountTermFrequencyEmpty) {
  std::map<Position, std::unique_ptr<FieldMask>> empty_map;

  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(empty_map, 1));
  ASSERT_NE(flat_map.data(), nullptr);

  EXPECT_EQ(flat_map.CountTermFrequency(), 0);
}

//=============================================================================
// Move Semantics Tests
//=============================================================================

TEST_F(FlatPositionMapTest, MoveConstructor) {
  auto position_map = CreatePositionMap({{10, 1}, {20, 2}, {30, 4}}, 3);

  FlatPositionMap flat_map1 =
      FlatPositionMap::SerializePositionMap(position_map, 3);
  const char* original_data = flat_map1.data();
  ASSERT_NE(original_data, nullptr);

  // Move construct
  FlatPositionMap flat_map2(std::move(flat_map1));

  // Verify ownership transferred
  EXPECT_EQ(flat_map2.data(), original_data);
  EXPECT_EQ(flat_map1.data(), nullptr);

  // Verify data is still valid
  FlatPositionMapIterator iter(flat_map2);
  EXPECT_EQ(iter.GetPosition(), 10);

  allocated_maps_.push_back(std::move(flat_map2));
}

TEST_F(FlatPositionMapTest, MoveAssignment) {
  auto position_map1 = CreatePositionMap({{10, 1}, {20, 2}}, 2);
  auto position_map2 = CreatePositionMap({{100, 4}, {200, 8}}, 2);

  FlatPositionMap flat_map1 =
      FlatPositionMap::SerializePositionMap(position_map1, 2);
  FlatPositionMap flat_map2 =
      FlatPositionMap::SerializePositionMap(position_map2, 2);

  const char* map2_data = flat_map2.data();
  ASSERT_NE(map2_data, nullptr);

  // Move assign
  flat_map1 = std::move(flat_map2);

  // Verify ownership transferred
  EXPECT_EQ(flat_map1.data(), map2_data);
  EXPECT_EQ(flat_map2.data(), nullptr);

  // Verify data is still valid
  FlatPositionMapIterator iter(flat_map1);
  EXPECT_EQ(iter.GetPosition(), 100);

  allocated_maps_.push_back(std::move(flat_map1));
}

TEST_F(FlatPositionMapTest, MoveAssignmentSelfAssignment) {
  auto position_map = CreatePositionMap({{10, 1}, {20, 2}}, 2);

  FlatPositionMap flat_map =
      FlatPositionMap::SerializePositionMap(position_map, 2);
  const char* original_data = flat_map.data();

  // Self-assignment should be a no-op
  flat_map = std::move(flat_map);

  EXPECT_EQ(flat_map.data(), original_data);

  allocated_maps_.push_back(std::move(flat_map));
}

//=============================================================================
// Stress Tests
//=============================================================================

TEST_F(FlatPositionMapTest, StressTestManyPositions) {
  // Create 1000 positions with varying deltas and field masks
  std::vector<std::pair<Position, uint64_t>> positions;
  Position current_pos = 0;
  for (int i = 0; i < 1000; ++i) {
    current_pos += (i % 10) + 1;  // Varying deltas 1-10
    positions.push_back({current_pos, 1ULL << (i % 8)});
  }

  auto position_map = CreatePositionMap(positions, 8);
  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 8));
  ASSERT_NE(flat_map.data(), nullptr);

  // Verify count
  EXPECT_EQ(flat_map.CountPositions(), 1000);

  // Verify all positions are correct
  FlatPositionMapIterator iter(flat_map);
  for (size_t i = 0; i < positions.size(); ++i) {
    EXPECT_TRUE(iter.IsValid());
    EXPECT_EQ(iter.GetPosition(), positions[i].first)
        << "Failed at index " << i;
    EXPECT_EQ(iter.GetFieldMask(), positions[i].second)
        << "Failed at index " << i;
    iter.NextPosition();
  }

  EXPECT_FALSE(iter.IsValid());
}

TEST_F(FlatPositionMapTest, StressTestSkipForwardInLargeMap) {
  // Create a large map and test skip forward multiple times
  std::vector<std::pair<Position, uint64_t>> positions;
  for (int i = 0; i < 500; ++i) {
    positions.push_back({i * 100, 1ULL << (i % 6)});
  }

  auto position_map = CreatePositionMap(positions, 6);
  FlatPositionMap flat_map =
      TrackMap(FlatPositionMap::SerializePositionMap(position_map, 6));
  ASSERT_NE(flat_map.data(), nullptr);

  FlatPositionMapIterator iter(flat_map);

  // Skip to various positions
  EXPECT_TRUE(iter.SkipForwardPosition(10000));
  EXPECT_EQ(iter.GetPosition(), 10000);

  EXPECT_TRUE(iter.SkipForwardPosition(25000));
  EXPECT_EQ(iter.GetPosition(), 25000);

  EXPECT_TRUE(iter.SkipForwardPosition(40000));
  EXPECT_EQ(iter.GetPosition(), 40000);
}

}  // namespace valkey_search::indexes::text
