/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/flat_position_map.h"

#include <map>
#include <memory>
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
};

//=============================================================================
// Core Functionality Tests
//=============================================================================

TEST_F(FlatPositionMapTest, EmptyMap) {
  std::map<Position, std::unique_ptr<FieldMask>> empty_map;
  FlatPositionMap flat_map =
      FlatPositionMap::SerializePositionMap(empty_map, 1);

  FlatPositionMapIterator iter(flat_map);
  EXPECT_FALSE(iter.IsValid());
  EXPECT_EQ(flat_map.CountPositions(), 0);
  EXPECT_EQ(flat_map.CountTermFrequency(), 0);
}

TEST_F(FlatPositionMapTest, SinglePositionSingleField) {
  auto position_map = CreatePositionMap({{100, 1}}, 1);
  FlatPositionMap flat_map =
      FlatPositionMap::SerializePositionMap(position_map, 1);

  FlatPositionMapIterator iter(flat_map);
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
  FlatPositionMap flat_map =
      FlatPositionMap::SerializePositionMap(position_map, 1);

  FlatPositionMapIterator iter(flat_map);
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
  FlatPositionMap flat_map =
      FlatPositionMap::SerializePositionMap(position_map, 1);

  FlatPositionMapIterator iter(flat_map);
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
  FlatPositionMap flat_map =
      FlatPositionMap::SerializePositionMap(position_map, 3);

  FlatPositionMapIterator iter(flat_map);
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
  FlatPositionMap flat_map =
      FlatPositionMap::SerializePositionMap(position_map, 1);

  FlatPositionMapIterator iter(flat_map);
  while (iter.IsValid()) {
    EXPECT_EQ(iter.GetFieldMask(), 1ULL);
    iter.NextPosition();
  }
}

TEST_F(FlatPositionMapTest, AllFieldsSet) {
  uint64_t all_fields = ~0ULL;
  auto position_map = CreatePositionMap({{100, all_fields}}, 64);
  FlatPositionMap flat_map =
      FlatPositionMap::SerializePositionMap(position_map, 64);

  FlatPositionMapIterator iter(flat_map);
  EXPECT_EQ(iter.GetFieldMask(), all_fields);
}

TEST_F(FlatPositionMapTest, TermFrequencyCalculation) {
  // Position 10: 1 field, Position 20: 2 fields, Position 30: 3 fields
  auto position_map =
      CreatePositionMap({{10, 0b001}, {20, 0b011}, {30, 0b111}}, 3);
  FlatPositionMap flat_map =
      FlatPositionMap::SerializePositionMap(position_map, 3);

  EXPECT_EQ(flat_map.CountTermFrequency(), 6);  // 1+2+3
}

//=============================================================================
// SkipForward Tests
//=============================================================================

TEST_F(FlatPositionMapTest, SkipToExistingPosition) {
  auto position_map =
      CreatePositionMap({{10, 1}, {20, 2}, {30, 4}, {40, 8}}, 4);
  FlatPositionMap flat_map =
      FlatPositionMap::SerializePositionMap(position_map, 4);

  FlatPositionMapIterator iter(flat_map);
  EXPECT_TRUE(iter.SkipForwardPosition(30));
  EXPECT_EQ(iter.GetPosition(), 30);
  EXPECT_EQ(iter.GetFieldMask(), 4ULL);
}

TEST_F(FlatPositionMapTest, SkipToNonExistingPosition) {
  auto position_map = CreatePositionMap({{10, 1}, {30, 2}, {50, 4}}, 3);
  FlatPositionMap flat_map =
      FlatPositionMap::SerializePositionMap(position_map, 3);

  FlatPositionMapIterator iter(flat_map);
  EXPECT_FALSE(iter.SkipForwardPosition(20));
  EXPECT_EQ(iter.GetPosition(), 30);  // Next position >= target
}

TEST_F(FlatPositionMapTest, SkipBeyondEnd) {
  auto position_map = CreatePositionMap({{10, 1}, {20, 2}}, 2);
  FlatPositionMap flat_map =
      FlatPositionMap::SerializePositionMap(position_map, 2);

  FlatPositionMapIterator iter(flat_map);
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
  FlatPositionMap flat_map =
      FlatPositionMap::SerializePositionMap(position_map, 4);

  EXPECT_EQ(flat_map.CountPositions(), 200);

  // Verify iteration
  FlatPositionMapIterator iter(flat_map);
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
  FlatPositionMap flat_map =
      FlatPositionMap::SerializePositionMap(position_map, 1);

  FlatPositionMapIterator iter(flat_map);
  EXPECT_TRUE(iter.SkipForwardPosition(750));
  EXPECT_EQ(iter.GetPosition(), 750);
}

//=============================================================================
// Move Semantics Tests
//=============================================================================

TEST_F(FlatPositionMapTest, MoveConstructor) {
  auto position_map = CreatePositionMap({{10, 1}, {20, 2}}, 2);
  FlatPositionMap map1 = FlatPositionMap::SerializePositionMap(position_map, 2);
  const char* data = map1.data();

  FlatPositionMap map2(std::move(map1));

  EXPECT_EQ(map2.data(), data);
  EXPECT_EQ(map1.data(), nullptr);
  EXPECT_EQ(map2.CountPositions(), 2);
}

TEST_F(FlatPositionMapTest, MoveAssignment) {
  auto position_map1 = CreatePositionMap({{10, 1}}, 1);
  auto position_map2 = CreatePositionMap({{20, 2}}, 1);

  FlatPositionMap map1 =
      FlatPositionMap::SerializePositionMap(position_map1, 1);
  FlatPositionMap map2 =
      FlatPositionMap::SerializePositionMap(position_map2, 1);
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
  FlatPositionMap flat_map =
      FlatPositionMap::SerializePositionMap(position_map, 8);

  EXPECT_EQ(flat_map.CountPositions(), 1000);

  FlatPositionMapIterator iter(flat_map);
  for (size_t i = 0; i < positions.size(); ++i) {
    ASSERT_TRUE(iter.IsValid()) << "Failed at index " << i;
    EXPECT_EQ(iter.GetPosition(), positions[i].first);
    EXPECT_EQ(iter.GetFieldMask(), positions[i].second);
    iter.NextPosition();
  }
  EXPECT_FALSE(iter.IsValid());
}

}  // namespace valkey_search::indexes::text
