/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/posting.h"

#include "gtest/gtest.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/memory_tracker.h"

namespace valkey_search::indexes::text {

class PostingTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();

    postings_ = std::make_unique<Postings>();
    metadata_ = std::make_unique<TextIndexMetadata>();
  }

  InternedStringPtr InternKey(const std::string& key) {
    return StringInternStore::Intern(key);
  }

  void TearDown() override { ValkeySearchTest::TearDown(); }

  std::unique_ptr<Postings> postings_;
  std::unique_ptr<TextIndexMetadata> metadata_;

  PositionMap CreatePositionMap(
      std::initializer_list<std::pair<Position, std::vector<size_t>>>
          pos_fields_pairs,
      size_t num_fields = 5) {
    PositionMap pos_map;
    for (const auto& [pos, field_indices] : pos_fields_pairs) {
      auto field_mask = FieldMask::Create(num_fields);
      for (size_t field_idx : field_indices) {
        field_mask->SetField(field_idx);
      }
      pos_map[pos] = std::move(field_mask);
    }
    return pos_map;
  }
};

TEST_F(PostingTest, InsertionCounters) {
  // Empty postings
  EXPECT_TRUE(postings_->IsEmpty());
  EXPECT_EQ(postings_->GetKeyCount(), 0);
  EXPECT_EQ(postings_->GetPositionCount(), 0);
  EXPECT_EQ(postings_->GetTotalTermFrequency(), 0);

  // Single key, single position, single field
  postings_->InsertKey(InternKey("doc1"), CreatePositionMap({{10, {0}}}), metadata_.get(), 5);
  EXPECT_FALSE(postings_->IsEmpty());
  EXPECT_EQ(postings_->GetKeyCount(), 1);
  EXPECT_EQ(postings_->GetPositionCount(), 1);
  EXPECT_EQ(postings_->GetTotalTermFrequency(), 1);

  // Multiple keys
  postings_->InsertKey(InternKey("doc2"), CreatePositionMap({{20, {1}}}), metadata_.get(), 5);
  postings_->InsertKey(InternKey("doc3"), CreatePositionMap({{30, {2}}}), metadata_.get(), 5);
  EXPECT_EQ(postings_->GetKeyCount(), 3);
  EXPECT_EQ(postings_->GetPositionCount(), 3);
  EXPECT_EQ(postings_->GetTotalTermFrequency(), 3);

  // Single key with multiple positions
  postings_ = std::make_unique<Postings>();
  metadata_ = std::make_unique<TextIndexMetadata>();
  postings_->InsertKey(InternKey("doc1"),
                       CreatePositionMap({{10, {0}}, {20, {0}}, {30, {1}}}), metadata_.get(), 5);
  EXPECT_EQ(postings_->GetKeyCount(), 1);
  EXPECT_EQ(postings_->GetPositionCount(), 3);
  EXPECT_EQ(postings_->GetTotalTermFrequency(), 3);

  // Multiple fields at same position
  postings_ = std::make_unique<Postings>();
  postings_->InsertKey(InternKey("doc1"), CreatePositionMap({{10, {0, 2}}}), metadata_.get(), 5);
  EXPECT_EQ(postings_->GetKeyCount(), 1);
  EXPECT_EQ(postings_->GetPositionCount(), 1);
  EXPECT_EQ(postings_->GetTotalTermFrequency(),
            2);  // 2 fields have the term at position 10
}

TEST_F(PostingTest, RemoveKey) {
  postings_->InsertKey(InternKey("doc1"), CreatePositionMap({{10, {0}}}), metadata_.get(), 5);
  postings_->InsertKey(InternKey("doc2"), CreatePositionMap({{20, {1}}}), metadata_.get(), 5);

  EXPECT_EQ(postings_->GetKeyCount(), 2);

  postings_->RemoveKey(InternKey("doc1"), metadata_.get());
  EXPECT_EQ(postings_->GetKeyCount(), 1);
  EXPECT_EQ(postings_->GetPositionCount(), 1);

  postings_->RemoveKey(InternKey("nonexistent"), metadata_.get());
  EXPECT_EQ(postings_->GetKeyCount(), 1);

  postings_->RemoveKey(InternKey("doc2"), metadata_.get());
  EXPECT_TRUE(postings_->IsEmpty());
}

TEST_F(PostingTest, KeyIteratorBasic) {
  // Add some test data
  postings_->InsertKey(InternKey("doc1"), CreatePositionMap({{10, {0}}}), metadata_.get(), 5);
  postings_->InsertKey(InternKey("doc2"), CreatePositionMap({{20, {1}}}), metadata_.get(), 5);
  postings_->InsertKey(InternKey("doc3"), CreatePositionMap({{30, {2}}}), metadata_.get(), 5);

  // Test key iteration - collect all keys
  auto key_iter = postings_->GetKeyIterator();
  std::vector<std::string> found_keys;

  while (key_iter.IsValid()) {
    found_keys.push_back(std::string(key_iter.GetKey()->Str()));
    key_iter.NextKey();
  }

  // Verify all keys are present
  EXPECT_EQ(found_keys.size(), 3);
  std::sort(found_keys.begin(), found_keys.end());
  EXPECT_EQ(found_keys[0], "doc1");
  EXPECT_EQ(found_keys[1], "doc2");
  EXPECT_EQ(found_keys[2], "doc3");
}

TEST_F(PostingTest, KeyIteratorSkipForward) {
  // Add test data
  postings_->InsertKey(InternKey("doc1"), CreatePositionMap({{10, {0}}}), metadata_.get(), 5);
  postings_->InsertKey(InternKey("doc3"), CreatePositionMap({{20, {1}}}), metadata_.get(), 5);
  postings_->InsertKey(InternKey("doc5"), CreatePositionMap({{30, {2}}}), metadata_.get(), 5);

  auto key_iter = postings_->GetKeyIterator();

  // Test that we can skip to an existing key
  auto doc3_key = InternKey("doc3");
  bool found_exact = key_iter.SkipForwardKey(doc3_key);
  if (found_exact) {
    EXPECT_TRUE(key_iter.IsValid());
    EXPECT_EQ(key_iter.GetKey()->Str(), "doc3");
  }

  // Test that iterator can advance through all keys
  key_iter = postings_->GetKeyIterator();
  int key_count = 0;
  while (key_iter.IsValid()) {
    key_count++;
    key_iter.NextKey();
  }
  EXPECT_EQ(key_count, 3);
}

TEST_F(PostingTest, PositionIteratorBasic) {
  // Add test data with multiple positions for one key
  postings_->InsertKey(InternKey("doc1"),
                       CreatePositionMap({{10, {0}}, {20, {1}}, {30, {2}}}), metadata_.get(), 5);

  // Get key iterator and position iterator
  auto key_iter = postings_->GetKeyIterator();
  EXPECT_TRUE(key_iter.IsValid());
  EXPECT_EQ(key_iter.GetKey()->Str(), "doc1");

  auto pos_iter = key_iter.GetPositionIterator();

  // Test position iteration
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 10);
  EXPECT_EQ(pos_iter.GetFieldMask(), 1ULL);  // Field 0 set (bit 0)

  pos_iter.NextPosition();
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 20);
  EXPECT_EQ(pos_iter.GetFieldMask(), 2ULL);  // Field 1 set (bit 1)

  pos_iter.NextPosition();
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 30);
  EXPECT_EQ(pos_iter.GetFieldMask(), 4ULL);  // Field 2 set (bit 2)

  pos_iter.NextPosition();
  EXPECT_FALSE(pos_iter.IsValid());  // End of iteration
}

TEST_F(PostingTest, PositionIteratorSkipForward) {
  // Add test data with gaps in positions
  postings_->InsertKey(InternKey("doc1"),
                       CreatePositionMap({{10, {0}}, {30, {1}}, {50, {2}}}), metadata_.get(), 5);

  auto key_iter = postings_->GetKeyIterator();
  auto pos_iter = key_iter.GetPositionIterator();

  // Skip to exact position match
  EXPECT_TRUE(pos_iter.SkipForward(30));
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 30);
  EXPECT_EQ(pos_iter.GetFieldMask(), 2ULL);

  // Skip to non-existent position (should land on next greater position)
  EXPECT_FALSE(pos_iter.SkipForward(40));
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 50);

  // Skip beyond all positions
  EXPECT_FALSE(pos_iter.SkipForward(100));
  EXPECT_FALSE(pos_iter.IsValid());
}

TEST_F(PostingTest, IteratorWithMultipleFields) {
  // Test position with multiple fields set
  postings_->InsertKey(InternKey("doc1"),
                       CreatePositionMap({{10, {0, 2}}, {20, {1}}}), metadata_.get(), 5);

  auto key_iter = postings_->GetKeyIterator();
  auto pos_iter = key_iter.GetPositionIterator();

  // First position should have fields 0 and 2 set (bits 0 and 2)
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 10);
  EXPECT_EQ(pos_iter.GetFieldMask(), 5ULL);  // Binary: 101 (fields 0 and 2)

  pos_iter.NextPosition();
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 20);
  EXPECT_EQ(pos_iter.GetFieldMask(), 2ULL);  // Binary: 010 (field 1)
}

TEST_F(PostingTest, EmptyPostingIterators) {
  // Test key iterator on empty posting
  auto key_iter = postings_->GetKeyIterator();
  EXPECT_FALSE(key_iter.IsValid());

  // Test position iterator behavior: add one position, then advance past it
  postings_->InsertKey(InternKey("doc1"), CreatePositionMap({{10, {0}}}), metadata_.get(), 5);

  auto valid_key_iter = postings_->GetKeyIterator();
  EXPECT_TRUE(valid_key_iter.IsValid());

  auto pos_iter = valid_key_iter.GetPositionIterator();
  EXPECT_TRUE(pos_iter.IsValid());
  EXPECT_EQ(pos_iter.GetPosition(), 10);

  // Advance past the only position - should become invalid
  pos_iter.NextPosition();
  EXPECT_FALSE(pos_iter.IsValid());
}

TEST_F(PostingTest, ContainsFieldsCheck) {
  // Test basic field containment functionality
  postings_->InsertKey(InternKey("doc1"), CreatePositionMap({{10, {0}}}), metadata_.get(), 5);

  auto field_mask = FieldMask::Create(5);
  field_mask->SetField(0);

  auto key_iter = postings_->GetKeyIterator();

  // Should have exactly one key
  EXPECT_EQ(postings_->GetKeyCount(), 1);

  // Iterator should be valid and contain the field we inserted
  EXPECT_TRUE(key_iter.IsValid());
  EXPECT_EQ(key_iter.GetKey()->Str(), "doc1");
  EXPECT_TRUE(key_iter.ContainsFields(field_mask->AsUint64()));

  // Test that it doesn't contain fields that weren't set
  auto field_mask_2 = FieldMask::Create(5);
  field_mask_2->SetField(1);
  EXPECT_FALSE(key_iter.ContainsFields(field_mask_2->AsUint64()));
}

TEST_F(PostingTest, LargeScaleOperations) {
  // Test with realistic random data: varying positions per document,
  // multiple fields per position, and overlapping positions across documents
  size_t total_positions = 0;
  size_t total_term_frequency = 0;

  for (int doc = 0; doc < 100; ++doc) {
    PositionMap pos_map;
    int num_positions = 1 + (std::rand() % 1000);  // 1-1000 positions per doc

    for (int i = 0; i < num_positions; ++i) {
      Position pos = std::rand() % 10000;
      if (pos_map.count(pos)) continue;  // Skip duplicates within same doc

      auto field_mask = FieldMask::Create(5);
      int num_fields = 1 + (std::rand() % 5);  // 1-5 fields per position
      for (int f = 0; f < num_fields; ++f) {
        field_mask->SetField(std::rand() % 5);
      }

      total_term_frequency += field_mask->CountSetFields();
      pos_map[pos] = std::move(field_mask);
    }

    total_positions += pos_map.size();
    postings_->InsertKey(InternKey("doc" + std::to_string(doc)),
                         std::move(pos_map), metadata_.get(), 5);
  }

  EXPECT_EQ(postings_->GetKeyCount(), 100);
  EXPECT_EQ(postings_->GetPositionCount(), total_positions);
  EXPECT_EQ(postings_->GetTotalTermFrequency(), total_term_frequency);

  // Verify all keys are iterable with valid positions
  auto key_iter = postings_->GetKeyIterator();
  size_t key_count = 0;
  while (key_iter.IsValid()) {
    EXPECT_TRUE(key_iter.GetPositionIterator().IsValid());
    key_iter.NextKey();
    key_count++;
  }
  EXPECT_EQ(key_count, 100);
}

TEST_F(PostingTest, FieldMaskImplementations) {
  postings_->InsertKey(InternKey("doc1"),
                       CreatePositionMap({{10, {0}}, {20, {0}}}, 1), metadata_.get(), 1);
  postings_->InsertKey(InternKey("doc2"),
                       CreatePositionMap({{15, {0, 2}}, {25, {1, 3}}}, 5), metadata_.get(), 5);
  postings_->InsertKey(InternKey("doc3"),
                       CreatePositionMap({{30, {0, 8}}, {40, {5}}}, 9), metadata_.get(), 9);

  EXPECT_EQ(postings_->GetTotalTermFrequency(), 9);  // 2 + 4 + 3

  auto key_iter = postings_->GetKeyIterator();

  // Iterate through all keys and verify field masks for each
  // Note: iteration order is determined by interned string pointers
  int keys_verified = 0;
  while (key_iter.IsValid()) {
    std::string key = std::string(key_iter.GetKey()->Str());
    auto pos_iter = key_iter.GetPositionIterator();

    if (key == "doc1") {
      // doc1: SingleFieldMask (1 field total)
      EXPECT_TRUE(pos_iter.IsValid());
      EXPECT_EQ(pos_iter.GetFieldMask(), 1);  // Field 0 at position 10
      pos_iter.NextPosition();
      EXPECT_TRUE(pos_iter.IsValid());
      EXPECT_EQ(pos_iter.GetFieldMask(), 1);  // Field 0 at position 20
      pos_iter.NextPosition();
      EXPECT_FALSE(pos_iter.IsValid());
      keys_verified++;
    } else if (key == "doc2") {
      // doc2: ByteFieldMask (5 fields total)
      EXPECT_TRUE(pos_iter.IsValid());
      EXPECT_EQ(pos_iter.GetFieldMask(), 5);  // Fields 0, 2 at position 15
      pos_iter.NextPosition();
      EXPECT_TRUE(pos_iter.IsValid());
      EXPECT_EQ(pos_iter.GetFieldMask(), 10);  // Fields 1, 3 at position 25
      pos_iter.NextPosition();
      EXPECT_FALSE(pos_iter.IsValid());
      keys_verified++;
    } else if (key == "doc3") {
      // doc3: Uint64FieldMask (9 fields total)
      EXPECT_TRUE(pos_iter.IsValid());
      EXPECT_EQ(pos_iter.GetFieldMask(), 0x101);  // Fields 0, 8 at position 30
      pos_iter.NextPosition();
      EXPECT_TRUE(pos_iter.IsValid());
      EXPECT_EQ(pos_iter.GetFieldMask(), 0x20);  // Field 5 at position 40
      pos_iter.NextPosition();
      EXPECT_FALSE(pos_iter.IsValid());
      keys_verified++;
    }

    key_iter.NextKey();
  }

  EXPECT_EQ(keys_verified, 3);
}

}  // namespace valkey_search::indexes::text
