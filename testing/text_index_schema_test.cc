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

TEST_F(TextIndexSchemaTest, CommitKeyDataReturnsDocLen) {
  auto schema = CreateSchema();
  data_model::TextIndex proto;
  auto text = std::make_shared<Text>(proto, schema);

  // With with_offsets=false, all tokens get position 0.
  // "hello world hello" has 2 unique tokens -> doc_len = 2
  // (duplicate "hello" merges into same position entry)
  auto key = StringInternStore::Intern("doc1");
  auto result =
      schema->StageAttributeData(key, "hello world hello", 0, false, false);
  ASSERT_TRUE(result.ok());
  auto commit = schema->CommitKeyData(key);
  EXPECT_EQ(commit.doc_len, 2);
  EXPECT_EQ(commit.norm, 1);  // Each unique token appears once (position 0)
}

TEST_F(TextIndexSchemaTest, CommitKeyDataReturnsZeroForEmptyStage) {
  auto schema = CreateSchema();

  // CommitKeyData without staging returns 0
  auto key = StringInternStore::Intern("no_data");
  auto commit = schema->CommitKeyData(key);
  EXPECT_EQ(commit.doc_len, 0);
  EXPECT_EQ(commit.norm, 0);
}

TEST_F(TextIndexSchemaTest, TotalDocLenAccumulatesAcrossDocuments) {
  auto schema = CreateSchema();
  data_model::TextIndex proto;
  auto text = std::make_shared<Text>(proto, schema);

  // doc1: "hello world" -> 2 terms
  auto key1 = StringInternStore::Intern("doc1");
  auto r1 = schema->StageAttributeData(key1, "hello world", 0, false, false);
  ASSERT_TRUE(r1.ok());
  schema->CommitKeyData(key1);

  // doc2: "foo bar baz" -> 3 terms
  auto key2 = StringInternStore::Intern("doc2");
  auto r2 = schema->StageAttributeData(key2, "foo bar baz", 0, false, false);
  ASSERT_TRUE(r2.ok());
  schema->CommitKeyData(key2);

  // total_doc_len should be 2 + 3 = 5
  EXPECT_EQ(schema->GetMetadata().total_doc_len.load(), 5);
}

TEST_F(TextIndexSchemaTest, TotalDocLenDecrementsOnDelete) {
  auto schema = CreateSchema();
  data_model::TextIndex proto;
  auto text = std::make_shared<Text>(proto, schema);

  // doc1: "hello world foo" -> 3 terms
  auto key1 = StringInternStore::Intern("doc1");
  auto r1 =
      schema->StageAttributeData(key1, "hello world foo", 0, false, false);
  ASSERT_TRUE(r1.ok());
  uint32_t doc_len = schema->CommitKeyData(key1).doc_len;
  EXPECT_EQ(doc_len, 3);
  EXPECT_EQ(schema->GetMetadata().total_doc_len.load(), 3);

  // DeleteKeyData decrements total_doc_len internally
  schema->DeleteKeyData(key1);
  EXPECT_EQ(schema->GetMetadata().total_doc_len.load(), 0);
}

TEST_F(TextIndexSchemaTest, DocLenWithMultipleFields) {
  auto schema = CreateSchema();
  data_model::TextIndex proto;

  // Create two Text instances to allocate field numbers 0 and 1
  auto text1 = std::make_shared<Text>(proto, schema);
  auto text2 = std::make_shared<Text>(proto, schema);

  auto key = StringInternStore::Intern("doc1");

  // Field 0: "hello world" -> 2 terms
  auto r1 = schema->StageAttributeData(key, "hello world", 0, false, false);
  ASSERT_TRUE(r1.ok());

  // Field 1: "foo bar baz" -> 3 terms
  auto r2 = schema->StageAttributeData(key, "foo bar baz", 1, false, false);
  ASSERT_TRUE(r2.ok());

  // doc_len should be total across both fields = 5
  auto commit = schema->CommitKeyData(key);
  EXPECT_EQ(commit.doc_len, 5);
  // norm = max token freq; each token appears once per field = 1
  EXPECT_EQ(commit.norm, 1);
}

TEST_F(TextIndexSchemaTest, NormReflectsMaxTermFrequency) {
  // with_offsets=true so duplicate tokens get distinct positions
  std::vector<std::string> empty_stop_words;
  auto schema = std::make_shared<TextIndexSchema>(
      data_model::LANGUAGE_ENGLISH, " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
      true, empty_stop_words, 4);
  data_model::TextIndex proto;
  auto text = std::make_shared<Text>(proto, schema);

  // "hello hello hello world" -> "hello" appears 3 times, "world" 1 time
  auto key = StringInternStore::Intern("doc1");
  auto r = schema->StageAttributeData(key, "hello hello hello world", 0, false,
                                      false);
  ASSERT_TRUE(r.ok());
  auto commit = schema->CommitKeyData(key);
  // doc_len = 4 (total tokens), norm = 3 (max freq is "hello" with 3)
  EXPECT_EQ(commit.doc_len, 4);
  EXPECT_EQ(commit.norm, 3);
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

}  // namespace text
}  // namespace indexes
}  // namespace valkey_search
