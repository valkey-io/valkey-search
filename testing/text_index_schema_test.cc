/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text.h"
#include "src/indexes/text/text_index.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search {
namespace indexes {
namespace text {

class TextIndexSchemaTest : public vmsdk::ValkeyTest {
 protected:
  void SetUp() override { vmsdk::ValkeyTest::SetUp(); }

  std::shared_ptr<TextIndexSchema> CreateSchema() {
    std::vector<std::string> empty_stop_words;
    return std::make_shared<TextIndexSchema>(
        data_model::LANGUAGE_ENGLISH,
        " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~", false, empty_stop_words,
        4);
  }
};

// Concurrent CommitKeyData calls with overlapping words must
//  not crash or corrupt the index.
TEST_F(TextIndexSchemaTest, ConcurrentCommitKeyData) {
  auto schema = CreateSchema();
  data_model::TextIndex proto;
  auto text = std::make_shared<Text>(proto, schema);

  // 4 documents with overlapping words ("the" appears in all).
  const std::vector<std::pair<std::string, std::string>> docs = {
      {"key:1", "the cat sat"},
      {"key:2", "the cat ran"},
      {"key:3", "the dog barked"},
      {"key:4", "the dog howled"},
  };

  std::vector<std::thread> threads;
  for (const auto &[key_str, content] : docs) {
    threads.emplace_back([&schema, key_str, content]() {
      auto key = StringInternStore::Intern(key_str);
      auto result = schema->StageAttributeData(key, content, 0, false, false);
      EXPECT_TRUE(result.ok());
      schema->CommitKeyData(key);
    });
  }
  for (auto &t : threads) t.join();

  // Unique words across all documents: the, cat, sat, ran, dog, barked, howled
  EXPECT_EQ(schema->GetNumUniqueTerms(), 7);
}

TEST_F(TextIndexSchemaTest, FieldAllocationAcrossMultipleTexts) {
  // Test that TextIndexSchema properly manages field number allocation
  // across multiple Text instances - this is the key schema responsibility
  auto schema = CreateSchema();

  // Initial state - no fields allocated
  EXPECT_EQ(0, schema->GetNumTextFields());

  // Create multiple Text instances sharing the same schema
  data_model::TextIndex proto;

  auto text1 = std::make_shared<Text>(proto, schema);
  EXPECT_EQ(1, schema->GetNumTextFields());

  auto text2 = std::make_shared<Text>(proto, schema);
  EXPECT_EQ(2, schema->GetNumTextFields());

  auto text3 = std::make_shared<Text>(proto, schema);
  EXPECT_EQ(3, schema->GetNumTextFields());

  // Schema correctly tracks field allocation for posting list identification
}

// Validate first-byte sharding routes words correctly
TEST_F(TextIndexSchemaTest, ShardRoutingByFirstByte) {
  auto schema = CreateSchema();
  auto text_index = schema->GetTextIndex();

  // Verify different first bytes go to different shards
  EXPECT_NE(text_index->GetShardIndex("apple"),
            text_index->GetShardIndex("banana"));
  EXPECT_NE(text_index->GetShardIndex("cat"), text_index->GetShardIndex("dog"));
  EXPECT_NE(text_index->GetShardIndex("test"),
            text_index->GetShardIndex("zebra"));

  // Same first byte = same shard
  EXPECT_EQ(text_index->GetShardIndex("apple"),
            text_index->GetShardIndex("avocado"));
  EXPECT_EQ(text_index->GetShardIndex("test"),
            text_index->GetShardIndex("tennis"));

  // Verify deterministic routing
  EXPECT_EQ(text_index->GetShardIndex("word"),
            text_index->GetShardIndex("word"));
}

// Validate cross-shard suffix indexing (prefix and suffix in different shards)
TEST_F(TextIndexSchemaTest, CrossShardSuffixIndexing) {
  auto schema = CreateSchema();
  schema->EnableSuffix();

  // Allocate field number before staging data
  uint8_t field_num = schema->AllocateTextFieldNumber();

  auto text_index = schema->GetTextIndex();
  auto key = StringInternStore::Intern("test_key");

  // Index word where reversed word routes to different shard
  // "forest" → shard['f'], reversed "tserof" → shard['t']
  auto result =
      schema->StageAttributeData(key, "forest", field_num, false, true);
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(*result);
  schema->CommitKeyData(key);

  // Verify word exists in prefix tree
  size_t prefix_shard = text_index->GetShardIndex("forest");
  auto prefix_iter =
      text_index->GetPrefixShard(prefix_shard).GetWordIterator("forest");
  EXPECT_FALSE(prefix_iter.Done());
  EXPECT_EQ(prefix_iter.GetWord(), "forest");

  // Verify reversed word exists in suffix tree
  std::string reversed("tserof");
  size_t suffix_shard = text_index->GetShardIndex(reversed);
  auto suffix_shard_opt = text_index->GetSuffixShard(suffix_shard);
  ASSERT_TRUE(suffix_shard_opt.has_value());
  auto suffix_iter = suffix_shard_opt->get().GetWordIterator(reversed);
  EXPECT_FALSE(suffix_iter.Done());
  EXPECT_EQ(suffix_iter.GetWord(), reversed);

  // Verify they're in different shards (key property of first-byte sharding)
  EXPECT_NE(prefix_shard, suffix_shard);
}

}  // namespace text
}  // namespace indexes
}  // namespace valkey_search
