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

  void TearDown() override {
    // Clean up any allocated flat maps
    for (auto* flat_map : allocated_maps_) {
      FreeFlatPositionMap(flat_map);
    }
    allocated_maps_.clear();
  }

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
    allocated_maps_.push_back(map);
    return map;
  }

  std::vector<FlatPositionMap> allocated_maps_;
};

// Test SIMPLE encoding with cumulative position tracking
TEST_F(FlatPositionMapTest, SimpleEncodingCumulativePosition) {
  // Create a simple position map (single field, small deltas)
  auto position_map = CreatePositionMap({{5, 1}, {10, 1}, {15, 1}, {20, 1}}, 1);

  FlatPositionMap flat_map = TrackMap(SerializePositionMap(position_map, 1));
  ASSERT_NE(flat_map, nullptr);

  FlatPositionMapIterator iter(flat_map);

  // Test that GetPosition returns correct cumulative positions
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

// Test EXPANDABLE encoding with cumulative position tracking
TEST_F(FlatPositionMapTest, ExpandableEncodingCumulativePosition) {
  // Create position map with multiple fields and larger deltas
  auto position_map =
      CreatePositionMap({{10, 1}, {50, 2}, {150, 4}, {500, 7}}, 3);

  FlatPositionMap flat_map = TrackMap(SerializePositionMap(position_map, 3));
  ASSERT_NE(flat_map, nullptr);

  FlatPositionMapIterator iter(flat_map);

  // Test cumulative positions with variable-length encoding
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

// Test BINARY_SEARCH encoding with cumulative position tracking
TEST_F(FlatPositionMapTest, BinarySearchEncodingCumulativePosition) {
  // Create a large position map to trigger binary search encoding
  std::vector<std::pair<Position, uint64_t>> positions;
  for (int i = 0; i < 150; ++i) {
    positions.push_back({i * 10, 1ULL << (i % 5)});
  }
  auto position_map = CreatePositionMap(positions, 5);

  FlatPositionMap flat_map = TrackMap(SerializePositionMap(position_map, 5));
  ASSERT_NE(flat_map, nullptr);

  FlatPositionMapIterator iter(flat_map);

  // Test cumulative positions throughout the map
  for (int i = 0; i < 150; ++i) {
    EXPECT_TRUE(iter.IsValid());
    EXPECT_EQ(iter.GetPosition(), i * 10);
    EXPECT_EQ(iter.GetFieldMask(), 1ULL << (i % 5));
    iter.NextPosition();
  }

  EXPECT_FALSE(iter.IsValid());
}

// Test SkipForward with cumulative position tracking
TEST_F(FlatPositionMapTest, SkipForwardWithCumulativePosition) {
  auto position_map =
      CreatePositionMap({{10, 1}, {20, 2}, {30, 4}, {40, 8}, {50, 16}}, 5);

  FlatPositionMap flat_map = TrackMap(SerializePositionMap(position_map, 5));
  ASSERT_NE(flat_map, nullptr);

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

// Test SkipForward to non-existent position
TEST_F(FlatPositionMapTest, SkipForwardNonExistentPosition) {
  auto position_map = CreatePositionMap({{10, 1}, {30, 2}, {50, 4}}, 3);

  FlatPositionMap flat_map = TrackMap(SerializePositionMap(position_map, 3));
  ASSERT_NE(flat_map, nullptr);

  FlatPositionMapIterator iter(flat_map);

  // Skip to position between existing positions
  EXPECT_FALSE(iter.SkipForwardPosition(25));
  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 30);  // Should land on next position
}

// Test multiple iterations over the same map
TEST_F(FlatPositionMapTest, MultipleIterations) {
  auto position_map = CreatePositionMap({{5, 1}, {10, 2}, {15, 4}}, 3);

  FlatPositionMap flat_map = TrackMap(SerializePositionMap(position_map, 3));
  ASSERT_NE(flat_map, nullptr);

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

// Test large deltas in EXPANDABLE encoding
TEST_F(FlatPositionMapTest, LargeDeltasExpandableEncoding) {
  // Test with deltas that require multiple bytes in variable-length encoding
  auto position_map =
      CreatePositionMap({{100, 1}, {1000, 2}, {10000, 4}, {100000, 8}}, 4);

  FlatPositionMap flat_map = TrackMap(SerializePositionMap(position_map, 4));
  ASSERT_NE(flat_map, nullptr);

  FlatPositionMapIterator iter(flat_map);

  EXPECT_EQ(iter.GetPosition(), 100);
  iter.NextPosition();
  EXPECT_EQ(iter.GetPosition(), 1000);
  iter.NextPosition();
  EXPECT_EQ(iter.GetPosition(), 10000);
  iter.NextPosition();
  EXPECT_EQ(iter.GetPosition(), 100000);
}

// Test edge case: single position
TEST_F(FlatPositionMapTest, SinglePosition) {
  auto position_map = CreatePositionMap({{42, 1}}, 1);

  FlatPositionMap flat_map = TrackMap(SerializePositionMap(position_map, 1));
  ASSERT_NE(flat_map, nullptr);

  FlatPositionMapIterator iter(flat_map);

  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 42);
  EXPECT_EQ(iter.GetFieldMask(), 1ULL);

  iter.NextPosition();
  EXPECT_FALSE(iter.IsValid());
}

// Test edge case: empty position map
TEST_F(FlatPositionMapTest, EmptyPositionMap) {
  std::map<Position, std::unique_ptr<FieldMask>> empty_map;

  FlatPositionMap flat_map = TrackMap(SerializePositionMap(empty_map, 1));
  ASSERT_NE(flat_map, nullptr);

  FlatPositionMapIterator iter(flat_map);
  EXPECT_FALSE(iter.IsValid());
}

// Test consecutive positions (minimal deltas)
TEST_F(FlatPositionMapTest, ConsecutivePositions) {
  auto position_map =
      CreatePositionMap({{1, 1}, {2, 1}, {3, 1}, {4, 1}, {5, 1}}, 1);

  FlatPositionMap flat_map = TrackMap(SerializePositionMap(position_map, 1));
  ASSERT_NE(flat_map, nullptr);

  FlatPositionMapIterator iter(flat_map);

  for (Position expected = 1; expected <= 5; ++expected) {
    EXPECT_TRUE(iter.IsValid());
    EXPECT_EQ(iter.GetPosition(), expected);
    iter.NextPosition();
  }

  EXPECT_FALSE(iter.IsValid());
}

// Test multiple fields at same position
TEST_F(FlatPositionMapTest, MultipleFieldsSamePosition) {
  // Field mask with multiple bits set (fields 0, 2, 4)
  auto position_map = CreatePositionMap({{10, 0b10101}}, 5);

  FlatPositionMap flat_map = TrackMap(SerializePositionMap(position_map, 5));
  ASSERT_NE(flat_map, nullptr);

  FlatPositionMapIterator iter(flat_map);

  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 10);
  EXPECT_EQ(iter.GetFieldMask(), 0b10101ULL);
}

// Test cumulative position accuracy after multiple Next() calls
TEST_F(FlatPositionMapTest, CumulativePositionAccuracy) {
  // Create a map with varying deltas
  auto position_map = CreatePositionMap(
      {{7, 1}, {15, 2}, {28, 4}, {50, 8}, {100, 16}, {200, 32}}, 6);

  FlatPositionMap flat_map = TrackMap(SerializePositionMap(position_map, 6));
  ASSERT_NE(flat_map, nullptr);

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

// Test SkipForward beyond last position
TEST_F(FlatPositionMapTest, SkipForwardBeyondEnd) {
  auto position_map = CreatePositionMap({{10, 1}, {20, 2}, {30, 4}}, 3);

  FlatPositionMap flat_map = TrackMap(SerializePositionMap(position_map, 3));
  ASSERT_NE(flat_map, nullptr);

  FlatPositionMapIterator iter(flat_map);

  // Skip beyond all positions
  EXPECT_FALSE(iter.SkipForwardPosition(100));
  EXPECT_FALSE(iter.IsValid());
}

// Test with maximum field count
TEST_F(FlatPositionMapTest, MaximumFieldCount) {
  // Test with 64 fields (maximum supported)
  uint64_t all_fields_mask = ~0ULL;  // All 64 bits set
  auto position_map = CreatePositionMap({{10, all_fields_mask}}, 64);

  FlatPositionMap flat_map = TrackMap(SerializePositionMap(position_map, 64));
  ASSERT_NE(flat_map, nullptr);

  FlatPositionMapIterator iter(flat_map);

  EXPECT_TRUE(iter.IsValid());
  EXPECT_EQ(iter.GetPosition(), 10);
  EXPECT_EQ(iter.GetFieldMask(), all_fields_mask);
}

// Stress test: many positions with cumulative tracking
TEST_F(FlatPositionMapTest, StressTestCumulativePosition) {
  // Create 1000 positions with varying deltas
  std::vector<std::pair<Position, uint64_t>> positions;
  Position current_pos = 0;
  for (int i = 0; i < 1000; ++i) {
    current_pos += (i % 10) + 1;  // Varying deltas
    positions.push_back({current_pos, 1ULL << (i % 8)});
  }

  auto position_map = CreatePositionMap(positions, 8);
  FlatPositionMap flat_map = TrackMap(SerializePositionMap(position_map, 8));
  ASSERT_NE(flat_map, nullptr);

  FlatPositionMapIterator iter(flat_map);

  // Verify all positions are correct
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

}  // namespace valkey_search::indexes::text
