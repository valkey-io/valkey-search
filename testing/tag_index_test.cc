/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/indexes/index_base.h"
#include "src/indexes/tag.h"
#include "src/query/predicate.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::indexes {

namespace {

class TagIndexTest : public vmsdk::ValkeyTest {
 public:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    data_model::TagIndex tag_index_proto;
    tag_index_proto.set_separator(",");
    tag_index_proto.set_case_sensitive(false);
    index = std::make_unique<IndexTeser<Tag, data_model::TagIndex>>(
        tag_index_proto);
  }
  std::unique_ptr<IndexTeser<Tag, data_model::TagIndex>> index;
};

std::vector<std::string> Fetch(
    valkey_search::indexes::EntriesFetcherBase& fetcher) {
  std::vector<std::string> keys;
  auto itr = fetcher.Begin();
  while (!itr->Done()) {
    keys.push_back(std::string(***itr));
    itr->Next();
  }
  return keys;
}

TEST_F(TagIndexTest, AddRecordAndSearchTest) {
  EXPECT_FALSE(index->AddRecord("key1", "    ").value());
  EXPECT_TRUE(index->AddRecord("key1", "tag1").value());

  EXPECT_TRUE(index->AddRecord("key2", "tag2").value());
  EXPECT_EQ(index->AddRecord("key2", "tag2").status().code(),
            absl::StatusCode::kAlreadyExists);

  std::string identifier = "attribute_name";
  std::string raw_tag_string = "tag1";
  auto parsed_tags =
      indexes::Tag::ParseSearchTags(raw_tag_string, index->GetSeparator())
          .value();
  query::TagPredicate predicate(index.get(), identifier, raw_tag_string,
                                parsed_tags);
  auto entries_fetcher = index->Search(predicate, false);
  EXPECT_EQ(entries_fetcher->Size(), 1);
  EXPECT_THAT(Fetch(*entries_fetcher), testing::UnorderedElementsAre("key1"));
}

TEST_F(TagIndexTest, RemoveRecordAndSearchTest) {
  EXPECT_TRUE(index->AddRecord("key1", "tag1").value());
  EXPECT_TRUE(index->AddRecord("key2", "tag2").value());
  EXPECT_TRUE(index->RemoveRecord("key1").value());

  std::string identifier = "attribute_name";
  std::string raw_tag_string = "tag1";
  auto parsed_tags =
      indexes::Tag::ParseSearchTags(raw_tag_string, index->GetSeparator())
          .value();
  query::TagPredicate predicate(index.get(), identifier, raw_tag_string,
                                parsed_tags);
  auto entries_fetcher = index->Search(predicate, false);

  EXPECT_EQ(entries_fetcher->Size(), 0);
}

TEST_F(TagIndexTest, ModifyRecordAndSearchTest) {
  EXPECT_TRUE(index->AddRecord("key1", "tag2").value());
  EXPECT_TRUE(index->ModifyRecord("key1", "tag2.1,tag2.2").value());

  std::string identifier = "attribute_name";
  std::string raw_tag_string = "tag2.1";
  auto parsed_tags =
      indexes::Tag::ParseSearchTags(raw_tag_string, index->GetSeparator())
          .value();
  query::TagPredicate predicate(index.get(), identifier, raw_tag_string,
                                parsed_tags);
  auto entries_fetcher = index->Search(predicate, false);

  EXPECT_EQ(entries_fetcher->Size(), 1);
  EXPECT_THAT(Fetch(*entries_fetcher), testing::UnorderedElementsAre("key1"));
  EXPECT_EQ(index->ModifyRecord("key5", "tag5").status().code(),
            absl::StatusCode::kNotFound);
}

TEST_F(TagIndexTest, ModifyRecordWithEmptyString) {
  EXPECT_TRUE(index->AddRecord("key1", "tag2").value());
  EXPECT_FALSE(index->ModifyRecord("key1", "").value());

  std::string identifier = "attribute_name";
  std::string raw_tag_string = "tag2";
  auto parsed_tags =
      indexes::Tag::ParseSearchTags(raw_tag_string, index->GetSeparator())
          .value();
  query::TagPredicate predicate(index.get(), identifier, raw_tag_string,
                                parsed_tags);
  auto entries_fetcher = index->Search(predicate, false);

  EXPECT_EQ(entries_fetcher->Size(), 0);
  EXPECT_EQ(index->GetRecordCount(), 0);
}

TEST_F(TagIndexTest, KeyTrackingTest) {
  EXPECT_TRUE(index->AddRecord("key1", "tag1").value());
  EXPECT_TRUE(index->AddRecord("key2", "tag2").value());
  EXPECT_FALSE(index->IsTracked("key3"));
  EXPECT_TRUE(index->AddRecord("key3", "tag3").value());
  EXPECT_TRUE(index->IsTracked("key3"));
  EXPECT_TRUE(index->RemoveRecord("key3").value());
  EXPECT_FALSE(index->IsTracked("key3"));
  auto res = index->RemoveRecord("key3");
  EXPECT_TRUE(res.ok());
  EXPECT_FALSE(res.value());
  EXPECT_FALSE(index->AddRecord("key5", "  ").value());
  EXPECT_FALSE(index->ModifyRecord("key5", " ").value());
  EXPECT_TRUE(index->AddRecord("key6", " tag6 , tag7 ").value());
}

TEST_F(TagIndexTest, PrefixSearchHappyTest) {
  EXPECT_TRUE(index->AddRecord("doc1", "disagree").value());
  EXPECT_TRUE(index->AddRecord("doc2", "disappear").value());
  EXPECT_TRUE(index->AddRecord("doc3", "dislike").value());
  EXPECT_TRUE(index->AddRecord("doc4", "disadvantage").value());
  EXPECT_TRUE(index->AddRecord("doc5", "preschool").value());

  std::string raw_tag_string = "dis*";
  std::string identifier = "attribute_name";
  auto parsed_tags =
      indexes::Tag::ParseSearchTags(raw_tag_string, index->GetSeparator())
          .value();
  EXPECT_THAT(parsed_tags, testing::UnorderedElementsAre("dis*"));
  auto entries_fetcher = index->Search(
      query::TagPredicate(index.get(), identifier, raw_tag_string, parsed_tags),
      false);
  EXPECT_THAT(Fetch(*entries_fetcher),
              testing::UnorderedElementsAre("doc1", "doc2", "doc3", "doc4"));
}

TEST_F(TagIndexTest, PrefixSearchCaseInsensitiveTest) {
  EXPECT_TRUE(index->AddRecord("doc1", "disagree").value());
  EXPECT_TRUE(index->AddRecord("doc2", "disappear").value());
  EXPECT_TRUE(index->AddRecord("doc3", "dislike").value());
  EXPECT_TRUE(index->AddRecord("doc4", "disadvantage").value());
  EXPECT_TRUE(index->AddRecord("doc5", "preschool").value());

  std::string raw_tag_string = "dIs*";
  std::string identifier = "attribute_name";
  auto parsed_tags =
      indexes::Tag::ParseSearchTags(raw_tag_string, index->GetSeparator())
          .value();
  EXPECT_THAT(parsed_tags, testing::UnorderedElementsAre("dIs*"));
  auto entries_fetcher = index->Search(
      query::TagPredicate(index.get(), identifier, raw_tag_string, parsed_tags),
      false);
  EXPECT_THAT(Fetch(*entries_fetcher),
              testing::UnorderedElementsAre("doc1", "doc2", "doc3", "doc4"));
}

TEST_F(TagIndexTest, PrefixSearchInvalidTagTest) {
  auto status = indexes::Tag::ParseSearchTags("dis**", index->GetSeparator());
  EXPECT_EQ(status.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_F(TagIndexTest, PrefixSearchMinLengthNotSatisfiedTest) {
  EXPECT_TRUE(index->AddRecord("doc1", "disagree").value());
  EXPECT_TRUE(index->AddRecord("doc2", "disappear").value());

  // No results because the prefix length is less than 2.
  std::string identifier = "attribute_name";
  std::string raw_tag_string = "d*";
  auto parsed_tags =
      indexes::Tag::ParseSearchTags(raw_tag_string, index->GetSeparator())
          .value();
  EXPECT_TRUE(parsed_tags.empty());

  auto entries_fetcher = index->Search(
      query::TagPredicate(index.get(), identifier, raw_tag_string, parsed_tags),
      false);
  EXPECT_EQ(entries_fetcher->Size(), 0);
}

TEST_F(TagIndexTest, PrefixSearchMinLengthSatisfiedTest) {
  EXPECT_TRUE(index->AddRecord("doc1", "disagree").value());
  EXPECT_TRUE(index->AddRecord("doc2", "disappear").value());

  // Results are returned because the prefix length is greater than 2.

  std::string identifier = "attribute_name";
  std::string raw_tag_string = "dis*";
  auto parsed_tags =
      indexes::Tag::ParseSearchTags(raw_tag_string, index->GetSeparator())
          .value();
  EXPECT_EQ(parsed_tags.size(), 1);

  auto entries_fetcher = index->Search(
      query::TagPredicate(index.get(), identifier, raw_tag_string, parsed_tags),
      false);
  EXPECT_EQ(entries_fetcher->Size(), 2);
}

TEST_F(TagIndexTest, NegativeSearchTest) {
  EXPECT_TRUE(index->AddRecord("doc1", "disagree").value());
  EXPECT_TRUE(index->AddRecord("doc2", "distance").value());
  EXPECT_TRUE(index->RemoveRecord("doc1").value());  // now untracked
  EXPECT_TRUE(index->RemoveRecord("doc2").value());  // now untracked`
  EXPECT_TRUE(index->AddRecord("doc3", "decorum").value());
  EXPECT_TRUE(index->AddRecord("doc4", "dismiss").value());
  EXPECT_FALSE(index->RemoveRecord("doc5").value());  // removed, never added
  EXPECT_TRUE(index->AddRecord("doc6", "demand").value());
  EXPECT_TRUE(index->RemoveRecord("doc6").value());
  EXPECT_TRUE(
      index->AddRecord("doc6", "demand2").value());  // removed then added

  std::string identifier = "attribute_name";
  std::string raw_tag_string = "dis*";
  auto parsed_tags =
      indexes::Tag::ParseSearchTags(raw_tag_string, index->GetSeparator())
          .value();
  EXPECT_EQ(parsed_tags.size(), 1);

  auto entries_fetcher = index->Search(
      query::TagPredicate(index.get(), identifier, raw_tag_string, parsed_tags),
      true);
  EXPECT_THAT(
      Fetch(*entries_fetcher),
      testing::UnorderedElementsAre("doc1", "doc2", "doc3", "doc5", "doc6"));
}

TEST_F(TagIndexTest, DeletedKeysNegativeSearchTest) {
  EXPECT_TRUE(index->AddRecord("doc0", "ambiance").value());

  // Test 1: soft delete
  EXPECT_TRUE(index->AddRecord("doc1", "demand").value());
  EXPECT_TRUE(index->RemoveRecord("doc1", DeletionType::kNone)
                  .value());  // remove a field

  std::string identifier = "attribute_name";
  std::string raw_tag_string = "dis*";
  auto entries_fetcher = index->Search(
      query::TagPredicate(
          index.get(), identifier, raw_tag_string,
          indexes::Tag::ParseSearchTags(raw_tag_string, index->GetSeparator())
              .value()),
      true);
  EXPECT_THAT(Fetch(*entries_fetcher),
              testing::UnorderedElementsAre("doc0", "doc1"));

  // Test 2: hard delete
  EXPECT_FALSE(index->RemoveRecord("doc1", DeletionType::kRecord)
                   .value());  // delete key
  entries_fetcher = index->Search(
      query::TagPredicate(
          index.get(), identifier, raw_tag_string,
          indexes::Tag::ParseSearchTags(raw_tag_string, index->GetSeparator())
              .value()),
      true);
  EXPECT_THAT(Fetch(*entries_fetcher), testing::UnorderedElementsAre("doc0"));
}
}  // namespace

}  // namespace valkey_search::indexes
