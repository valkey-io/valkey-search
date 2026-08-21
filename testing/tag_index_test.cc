/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/commands/filter_parser.h"
#include "src/indexes/index_base.h"
#include "src/indexes/tag.h"
#include "src/indexes/vector_base.h"
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
  std::string identifier = "attribute_id";
  std::string alias = "attribute_alias";
};

std::vector<std::string> Fetch(
    valkey_search::indexes::EntriesFetcherBase &fetcher) {
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

  // Parsing filter string from query: uses '|' separator (query language OR
  // syntax)
  std::string filter_tag_string = "tag1";
  auto parsed_tags = FilterParser::ParseQueryTags(filter_tag_string).value();
  query::TagPredicate predicate(index.get(), alias, identifier,
                                filter_tag_string, parsed_tags);
  auto entries_fetcher = index->Search(predicate, false);
  EXPECT_EQ(entries_fetcher->Size(), 1);
  EXPECT_THAT(Fetch(*entries_fetcher), testing::UnorderedElementsAre("key1"));
}

TEST_F(TagIndexTest, RemoveRecordAndSearchTest) {
  EXPECT_TRUE(index->AddRecord("key1", "tag1").value());
  EXPECT_TRUE(index->AddRecord("key2", "tag2").value());
  EXPECT_TRUE(index->RemoveRecord("key1").value());

  // Parsing filter string from query: uses '|' separator (query language OR
  // syntax)
  std::string filter_tag_string = "tag1";
  auto parsed_tags = FilterParser::ParseQueryTags(filter_tag_string).value();
  query::TagPredicate predicate(index.get(), alias, identifier,
                                filter_tag_string, parsed_tags);
  auto entries_fetcher = index->Search(predicate, false);

  EXPECT_EQ(entries_fetcher->Size(), 0);
}

TEST_F(TagIndexTest, ModifyRecordAndSearchTest) {
  EXPECT_TRUE(index->AddRecord("key1", "tag2").value());
  EXPECT_TRUE(index->ModifyRecord("key1", "tag2.1,tag2.2").value());

  // Parsing filter string from query: uses '|' separator (query language OR
  // syntax)
  std::string filter_tag_string = "tag2.1";
  auto parsed_tags = FilterParser::ParseQueryTags(filter_tag_string).value();
  query::TagPredicate predicate(index.get(), alias, identifier,
                                filter_tag_string, parsed_tags);
  auto entries_fetcher = index->Search(predicate, false);

  EXPECT_EQ(entries_fetcher->Size(), 1);
  EXPECT_THAT(Fetch(*entries_fetcher), testing::UnorderedElementsAre("key1"));
  EXPECT_EQ(index->ModifyRecord("key5", "tag5").status().code(),
            absl::StatusCode::kNotFound);
}

TEST_F(TagIndexTest, ModifyRecordWithEmptyString) {
  EXPECT_TRUE(index->AddRecord("key1", "tag2").value());
  EXPECT_FALSE(index->ModifyRecord("key1", "").value());

  // Parsing filter string from query: uses '|' separator (query language OR
  // syntax)
  std::string filter_tag_string = "tag2";
  auto parsed_tags = FilterParser::ParseQueryTags(filter_tag_string).value();
  query::TagPredicate predicate(index.get(), alias, identifier,
                                filter_tag_string, parsed_tags);
  auto entries_fetcher = index->Search(predicate, false);

  EXPECT_EQ(entries_fetcher->Size(), 0);
  EXPECT_EQ(index->GetTrackedKeyCount(), 0);
}

TEST_F(TagIndexTest, ModifyShortInlineTagStringRegressionTest) {
  EXPECT_TRUE(index->AddRecord("key1", "a").value());

  {
    std::string filter_tag_string = "a";
    auto parsed_tags = FilterParser::ParseQueryTags(filter_tag_string).value();
    query::TagPredicate predicate(index.get(), alias, identifier,
                                  filter_tag_string, parsed_tags);
    auto entries_fetcher = index->Search(predicate, false);
    EXPECT_EQ(entries_fetcher->Size(), 1);
    EXPECT_THAT(Fetch(*entries_fetcher), testing::UnorderedElementsAre("key1"));
  }

  EXPECT_TRUE(index->ModifyRecord("key1", "b").value());

  {
    std::string filter_tag_string = "a";
    auto parsed_tags = FilterParser::ParseQueryTags(filter_tag_string).value();
    query::TagPredicate predicate(index.get(), alias, identifier,
                                  filter_tag_string, parsed_tags);
    auto entries_fetcher = index->Search(predicate, false);
    EXPECT_EQ(entries_fetcher->Size(), 0);
  }

  {
    std::string filter_tag_string = "b";
    auto parsed_tags = FilterParser::ParseQueryTags(filter_tag_string).value();
    query::TagPredicate predicate(index.get(), alias, identifier,
                                  filter_tag_string, parsed_tags);
    auto entries_fetcher = index->Search(predicate, false);
    EXPECT_EQ(entries_fetcher->Size(), 1);
    EXPECT_THAT(Fetch(*entries_fetcher), testing::UnorderedElementsAre("key1"));
  }
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

  // Parsing filter string from query: uses '|' separator (query language OR
  // syntax)
  std::string filter_tag_string = "dis*";
  auto parsed_tags = FilterParser::ParseQueryTags(filter_tag_string).value();
  EXPECT_THAT(parsed_tags, testing::UnorderedElementsAre("dis*"));
  auto entries_fetcher =
      index->Search(query::TagPredicate(index.get(), alias, identifier,
                                        filter_tag_string, parsed_tags),
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

  // Parsing filter string from query: uses '|' separator (query language OR
  // syntax)
  std::string filter_tag_string = "dIs*";
  auto parsed_tags = FilterParser::ParseQueryTags(filter_tag_string).value();
  EXPECT_THAT(parsed_tags, testing::UnorderedElementsAre("dIs*"));
  auto entries_fetcher =
      index->Search(query::TagPredicate(index.get(), alias, identifier,
                                        filter_tag_string, parsed_tags),
                    false);
  EXPECT_THAT(Fetch(*entries_fetcher),
              testing::UnorderedElementsAre("doc1", "doc2", "doc3", "doc4"));
}

TEST_F(TagIndexTest, PrefixSearchInvalidTagTest) {
  auto status = indexes::Tag::ParseSearchTags("dis**", index->GetSeparator());
  EXPECT_EQ(status.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_F(TagIndexTest, PrefixSearchMinLengthNotSatisfiedReturnsError) {
  // Parsing filter string from query: uses '|' separator (query language OR
  // syntax)
  std::string filter_tag_string = "d*";
  auto parsed_tags = FilterParser::ParseQueryTags(filter_tag_string);
  EXPECT_EQ(parsed_tags.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_F(TagIndexTest, PrefixSearchMinLengthSatisfiedTest) {
  EXPECT_TRUE(index->AddRecord("doc1", "disagree").value());
  EXPECT_TRUE(index->AddRecord("doc2", "disappear").value());

  // Results are returned because the prefix length is greater than 2.

  // Parsing filter string from query: uses '|' separator (query language OR
  // syntax)
  std::string filter_tag_string = "dis*";
  auto parsed_tags = FilterParser::ParseQueryTags(filter_tag_string).value();
  EXPECT_EQ(parsed_tags.size(), 1);

  auto entries_fetcher =
      index->Search(query::TagPredicate(index.get(), alias, identifier,
                                        filter_tag_string, parsed_tags),
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

  // Parsing filter string from query: uses '|' separator (query language OR
  // syntax)
  std::string filter_tag_string = "dis*";
  auto parsed_tags = FilterParser::ParseQueryTags(filter_tag_string).value();
  EXPECT_EQ(parsed_tags.size(), 1);

  auto entries_fetcher =
      index->Search(query::TagPredicate(index.get(), alias, identifier,
                                        filter_tag_string, parsed_tags),
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
  // Parsing filter string from query: uses '|' separator (query language OR
  // syntax)
  std::string filter_tag_string = "dis*";
  auto entries_fetcher = index->Search(
      query::TagPredicate(
          index.get(), alias, identifier, filter_tag_string,
          FilterParser::ParseQueryTags(filter_tag_string).value()),
      true);
  EXPECT_THAT(Fetch(*entries_fetcher),
              testing::UnorderedElementsAre("doc0", "doc1"));

  // Test 2: hard delete
  EXPECT_FALSE(index->RemoveRecord("doc1", DeletionType::kRecord)
                   .value());  // delete key
  entries_fetcher = index->Search(
      query::TagPredicate(
          index.get(), alias, identifier, filter_tag_string,
          FilterParser::ParseQueryTags(filter_tag_string).value()),
      true);
  EXPECT_THAT(Fetch(*entries_fetcher), testing::UnorderedElementsAre("doc0"));
}

// Tests for escaped separator handling in ParseSearchTags and UnescapeTag
// Per Redis spec: \| should be treated as literal pipe, not a separator

// Helper function to parse and unescape tags (simulating full query flow)
static absl::flat_hash_set<std::string> ParseAndUnescapeTags(
    absl::string_view raw_tag_string, char separator) {
  auto parsed = indexes::Tag::ParseSearchTags(raw_tag_string, separator);
  if (!parsed.ok()) {
    return {};
  }
  absl::flat_hash_set<std::string> result;
  for (const auto &tag : parsed.value()) {
    result.insert(indexes::Tag::UnescapeTag(tag));
  }
  return result;
}

TEST_F(TagIndexTest, ParseSearchTagsEscapedSeparator) {
  // Query: "foo\|bar" should parse as single tag "foo|bar"
  // (backslash escapes the pipe, so it's not a separator)
  std::string raw_tag_string = R"(foo\|bar)";
  auto result = ParseAndUnescapeTags(raw_tag_string, '|');
  // Should be ONE tag: "foo|bar" (with the pipe as part of the value)
  EXPECT_THAT(result, testing::UnorderedElementsAre("foo|bar"));
}

TEST_F(TagIndexTest, ParseSearchTagsEscapedSeparatorWithMultipleTags) {
  // Query: "a\|b|c" should parse as two tags: "a|b" and "c"
  std::string raw_tag_string = R"(a\|b|c)";
  auto result = ParseAndUnescapeTags(raw_tag_string, '|');
  EXPECT_THAT(result, testing::UnorderedElementsAre("a|b", "c"));
}

TEST_F(TagIndexTest, ParseSearchTagsEscapedBackslash) {
  // Query: "foo\\|bar" - double backslash is escaped backslash, then pipe
  // is a separator. Should parse as: "foo\" and "bar"
  std::string raw_tag_string = R"(foo\\|bar)";
  auto result = ParseAndUnescapeTags(raw_tag_string, '|');
  EXPECT_THAT(result, testing::UnorderedElementsAre(R"(foo\)", "bar"));
}

TEST_F(TagIndexTest, ParseSearchTagsEscapedBackslashFollowedByEscapedPipe) {
  // Query: "foo\\\|bar" - escaped backslash + escaped pipe = literal "foo\|bar"
  std::string raw_tag_string = R"(foo\\\|bar)";
  auto result = ParseAndUnescapeTags(raw_tag_string, '|');
  EXPECT_THAT(result, testing::UnorderedElementsAre(R"(foo\|bar)"));
}

TEST_F(TagIndexTest, ParseSearchTagsMultipleEscapedSeparators) {
  // Query: "a\|b\|c|d\|e" should parse as: "a|b|c" and "d|e"
  std::string raw_tag_string = R"(a\|b\|c|d\|e)";
  auto result = ParseAndUnescapeTags(raw_tag_string, '|');
  EXPECT_THAT(result, testing::UnorderedElementsAre("a|b|c", "d|e"));
}

TEST_F(TagIndexTest, ParseSearchTagsEscapedBackslashOnly) {
  // Query: "foo\\" (escaped backslash, no separator)
  // Should unescape to single backslash: "foo\"
  std::string raw_tag_string = R"(foo\\)";
  auto result = ParseAndUnescapeTags(raw_tag_string, '|');
  EXPECT_THAT(result, testing::UnorderedElementsAre(R"(foo\)"));
}

TEST_F(TagIndexTest, ParseSearchTagsEscapedPipeOnly) {
  // Query: "foo\|" (escaped pipe at end, no separator)
  // Should unescape to: "foo|"
  std::string raw_tag_string = R"(foo\|)";
  auto result = ParseAndUnescapeTags(raw_tag_string, '|');
  EXPECT_THAT(result, testing::UnorderedElementsAre("foo|"));
}

// =============================================================================
// UnescapeTag unit tests - direct testing of the unescape function
// =============================================================================

TEST_F(TagIndexTest, UnescapeTagEmptyString) {
  EXPECT_EQ(indexes::Tag::UnescapeTag(""), "");
}

TEST_F(TagIndexTest, UnescapeTagNoEscapeSequences) {
  EXPECT_EQ(indexes::Tag::UnescapeTag("simple"), "simple");
  EXPECT_EQ(indexes::Tag::UnescapeTag("hello world"), "hello world");
}

TEST_F(TagIndexTest, UnescapeTagEscapedPipe) {
  EXPECT_EQ(indexes::Tag::UnescapeTag(R"(a\|b)"), "a|b");
}

TEST_F(TagIndexTest, UnescapeTagEscapedBackslash) {
  EXPECT_EQ(indexes::Tag::UnescapeTag(R"(a\\b)"), R"(a\b)");
}

TEST_F(TagIndexTest, UnescapeTagTrailingBackslash) {
  // Trailing backslash with no following char is preserved literally
  EXPECT_EQ(indexes::Tag::UnescapeTag(R"(abc\)"), R"(abc\)");
}

TEST_F(TagIndexTest, UnescapeTagOnlyBackslash) {
  EXPECT_EQ(indexes::Tag::UnescapeTag(R"(\)"), R"(\)");
}

TEST_F(TagIndexTest, UnescapeTagMixedEscapes) {
  // Multiple different escape sequences
  EXPECT_EQ(indexes::Tag::UnescapeTag(R"(a\|b\\c)"), R"(a|b\c)");
}

TEST_F(TagIndexTest, UnescapeTagConsecutiveBackslashes) {
  // Four backslashes → two backslashes
  EXPECT_EQ(indexes::Tag::UnescapeTag(R"(\\\\)"), R"(\\)");
}

TEST_F(TagIndexTest, UnescapeTagEscapedRegularChar) {
  // Escaping a regular character (permissive: \x → x)
  EXPECT_EQ(indexes::Tag::UnescapeTag(R"(test\value)"), "testvalue");
}

// =============================================================================
// ParseSearchTags edge case tests
// =============================================================================

TEST_F(TagIndexTest, ParseSearchTagsEmptyBetweenSeparators) {
  // Empty tags between separators should be ignored
  auto result = ParseAndUnescapeTags("a||b", '|');
  EXPECT_THAT(result, testing::UnorderedElementsAre("a", "b"));
}

TEST_F(TagIndexTest, ParseSearchTagsWhitespaceOnlyTag) {
  // Whitespace-only tags should be ignored
  auto result = ParseAndUnescapeTags("a|   |b", '|');
  EXPECT_THAT(result, testing::UnorderedElementsAre("a", "b"));
}

TEST_F(TagIndexTest, ParseSearchTagsSingleCharacterPrefixReturnsError) {
  auto result = indexes::Tag::ParseSearchTags("b*", '|');
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_F(TagIndexTest, ParseSearchTagsWildcardOnlyReturnsError) {
  auto result = indexes::Tag::ParseSearchTags("*", '|');
  EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
}

TEST_F(TagIndexTest, ParseSearchTagsTrailingBackslash) {
  // Backslash at end with no following character
  auto result = indexes::Tag::ParseSearchTags(R"(tag\)", '|');
  ASSERT_TRUE(result.ok());
  // Raw result contains trailing backslash
  EXPECT_THAT(result.value(), testing::UnorderedElementsAre(R"(tag\)"));
}

TEST_F(TagIndexTest, ParseSearchTagsOnlyBackslash) {
  auto result = indexes::Tag::ParseSearchTags(R"(\)", '|');
  ASSERT_TRUE(result.ok());
  EXPECT_THAT(result.value(), testing::UnorderedElementsAre(R"(\)"));
}

TEST_F(TagIndexTest, ParseSearchTagsUnicodePreserved) {
  auto result = ParseAndUnescapeTags("日本語|中文", '|');
  EXPECT_THAT(result, testing::UnorderedElementsAre("日本語", "中文"));
}

TEST_F(TagIndexTest, ParseSearchTagsEmptyString) {
  auto result = indexes::Tag::ParseSearchTags("", '|');
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result.value().empty());
}

TEST_F(TagIndexTest, ParseSearchTagsWhitespaceOnly) {
  auto result = indexes::Tag::ParseSearchTags("   ", '|');
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result.value().empty());
}

TEST_F(TagIndexTest, GetRawTagStringTest) {
  auto key1 = StringInternStore::Intern("key1");
  auto missing_key = StringInternStore::Intern("non_existent_key");
  EXPECT_TRUE(index->AddRecord("key1", "tag1, tag2").value());

  auto raw_tags = index->GetRawTagString(key1);
  ASSERT_TRUE(raw_tags.has_value());
  EXPECT_EQ(raw_tags.value(), "tag1, tag2");
  EXPECT_FALSE(index->IsCaseSensitive());
  EXPECT_EQ(index->GetSeparator(), ',');

  auto missing_raw_tags = index->GetRawTagString(missing_key);
  EXPECT_FALSE(missing_raw_tags.has_value());
}

TEST_F(TagIndexTest, RawTagStringPredicateEvaluateTest) {
  std::string filter_tag_string = "tech|scien*";
  auto parsed_tags = FilterParser::ParseQueryTags(filter_tag_string).value();
  query::TagPredicate predicate(index.get(), alias, identifier,
                                filter_tag_string, parsed_tags);

  EXPECT_TRUE(predicate.Evaluate("news, TECH, sport", ',', false).matches);
  EXPECT_TRUE(predicate.Evaluate("news, scientific", ',', false).matches);
  EXPECT_TRUE(predicate.Evaluate("news; TECH; sport", ';', false).matches);
  EXPECT_TRUE(predicate.Evaluate("news| scientific", '|', false).matches);
  EXPECT_FALSE(predicate.Evaluate("news, TECH, sport", ',', true).matches);
  EXPECT_FALSE(predicate.Evaluate("news, sport, politics", ',', false).matches);
  EXPECT_FALSE(predicate.Evaluate("", ',', false).matches);
}

TEST_F(TagIndexTest, PrefilterEvaluatorEvaluateTagsTest) {
  auto key1 = StringInternStore::Intern("key1");
  auto key2 = StringInternStore::Intern("key2");
  auto key3 = StringInternStore::Intern("key3");

  EXPECT_TRUE(index->AddRecord("key1", "news, tech").value());
  EXPECT_TRUE(index->AddRecord("key2", "sports, health").value());

  std::string filter_tag_string = "tech";
  auto parsed_tags = FilterParser::ParseQueryTags(filter_tag_string).value();
  query::TagPredicate predicate(index.get(), alias, identifier,
                                filter_tag_string, parsed_tags);

  PrefilterEvaluator evaluator(nullptr, QueryOperations::kNone);

  EXPECT_TRUE(evaluator.Evaluate(predicate, key1));
  EXPECT_FALSE(evaluator.Evaluate(predicate, key2));
  EXPECT_FALSE(evaluator.Evaluate(predicate, key3));
}

TEST_F(TagIndexTest, PrefilterEvaluatorCustomSeparatorTest) {
  data_model::TagIndex tag_index_proto;
  tag_index_proto.set_separator(";");
  tag_index_proto.set_case_sensitive(false);
  auto custom_index =
      std::make_unique<IndexTeser<Tag, data_model::TagIndex>>(tag_index_proto);

  auto key1 = StringInternStore::Intern("key1");
  auto key2 = StringInternStore::Intern("key2");

  EXPECT_TRUE(custom_index->AddRecord("key1", "news; tech").value());
  EXPECT_TRUE(custom_index->AddRecord("key2", "sports; health").value());

  std::string filter_tag_string = "tech";
  auto parsed_tags = FilterParser::ParseQueryTags(filter_tag_string).value();
  query::TagPredicate predicate(custom_index.get(), alias, identifier,
                                filter_tag_string, parsed_tags);

  PrefilterEvaluator evaluator(nullptr, QueryOperations::kNone);

  EXPECT_TRUE(evaluator.Evaluate(predicate, key1));
  EXPECT_FALSE(evaluator.Evaluate(predicate, key2));
}

TEST_F(TagIndexTest, PrefilterEvaluatorWildcardTest) {
  auto key1 = StringInternStore::Intern("key1");
  auto key2 = StringInternStore::Intern("key2");

  EXPECT_TRUE(index->AddRecord("key1", "news, technology").value());
  EXPECT_TRUE(index->AddRecord("key2", "sports, health").value());

  PrefilterEvaluator evaluator(nullptr, QueryOperations::kNone);

  // 1. Prefix wildcard match "tech*"
  {
    std::string query_str = "tech*";
    auto parsed = FilterParser::ParseQueryTags(query_str).value();
    query::TagPredicate pred(index.get(), alias, identifier, query_str, parsed);
    EXPECT_TRUE(evaluator.Evaluate(pred, key1));
    EXPECT_FALSE(evaluator.Evaluate(pred, key2));
  }

  // 2. Wildcard prefix longer than tag "technology_extra*" -> no match
  {
    std::string query_str = "technology_extra*";
    auto parsed = FilterParser::ParseQueryTags(query_str).value();
    query::TagPredicate pred(index.get(), alias, identifier, query_str, parsed);
    EXPECT_FALSE(evaluator.Evaluate(pred, key1));
    EXPECT_FALSE(evaluator.Evaluate(pred, key2));
  }

  // 3. Multi-tag query with wildcards "sport*|tech*"
  {
    std::string query_str = "sport*|tech*";
    auto parsed = FilterParser::ParseQueryTags(query_str).value();
    query::TagPredicate pred(index.get(), alias, identifier, query_str, parsed);
    EXPECT_TRUE(evaluator.Evaluate(pred, key1));
    EXPECT_TRUE(evaluator.Evaluate(pred, key2));
  }
}

}  // namespace

}  // namespace valkey_search::indexes
