/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/text.h"
#include "src/indexes/text/text_index.h"
#include "src/indexes/vector_base.h"
#include "src/query/predicate.h"
#include "src/query/search.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"
#include "vmsdk/src/time_sliced_mrmw_mutex.h"

namespace valkey_search {
namespace {

using ::testing::ElementsAre;

// Builds a scorable IndexSchema with a single text field, populated with the
// supplied {key, content} documents. Both the text postings (TF/doc-frequency)
// and the index_key_info_ map (total_docs/document_score) are populated so the
// scoring walker can read everything it needs.
class TextScoringTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    std::vector<absl::string_view> prefixes = {"doc:"};
    // with_offsets=true so repeated tokens get distinct positions and term
    // frequency reflects real counts; no_stem so query terms match raw tokens.
    index_schema_ =
        MockIndexSchema::Create(&fake_ctx_, "idx", prefixes,
                                std::make_unique<HashAttributeDataType>(),
                                /*mutations_thread_pool=*/nullptr,
                                data_model::LANGUAGE_ENGLISH,
                                " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
                                /*with_offsets=*/true)
            .value();
    index_schema_->CreateTextIndexSchema();
    text_index_ = std::make_shared<indexes::Text>(
        CreateTextIndexProto(/*with_suffix_trie=*/false, /*no_stem=*/true, 1.0),
        index_schema_->GetTextIndexSchema());
    VMSDK_EXPECT_OK(index_schema_->AddIndex("body", "body", text_index_));
  }

  // The schema unsubscribes from the KeyspaceEventManager on destruction, so it
  // must be released before the base fixture tears that singleton down.
  void TearDown() override {
    text_index_.reset();
    index_schema_.reset();
    ValkeySearchTest::TearDown();
  }

  // Inserts one document: stages/commits the text data and registers the key in
  // index_key_info_ (default document_score = 1.0).
  void AddDoc(const std::string& key_name, absl::string_view content) {
    auto key = StringInternStore::Intern(key_name);
    auto added = text_index_->AddRecord(key, content);
    ASSERT_TRUE(added.ok()) << added.status();
    index_schema_->GetTextIndexSchema()->CommitKeyData(key);

    vmsdk::WriterMutexLock lock(&index_schema_->GetTimeSlicedMutex());
    index_schema_->SetIndexMutationSequenceNumber(key, ++sequence_number_);
  }

  std::unique_ptr<query::TermPredicate> Term(const std::string& term) {
    return std::make_unique<query::TermPredicate>(
        index_schema_->GetTextIndexSchema(),
        /*field_mask=*/1ULL << text_index_->GetTextFieldNumber(), term,
        /*exact=*/true);
  }

  // Runs ScoreTextQuery over all currently-indexed keys and returns the ranked
  // results (already sorted by score desc, key asc).
  std::vector<indexes::BorrowedNeighbor> Score(const query::Predicate* root) {
    std::vector<indexes::BorrowedNeighbor> candidates;
    {
      vmsdk::ReaderMutexLock lock(&index_schema_->GetTimeSlicedMutex());
      for (const auto& [key, _] : index_schema_->GetIndexKeyInfo()) {
        candidates.push_back(
            {BorrowedInternedStringPtr(key), 0.0f, indexes::kDefaultScore});
      }
    }
    const auto* scorer =
        indexes::scoring::GetScorer(indexes::scoring::ScorerType::kBm25Std);
    vmsdk::ReaderMutexLock lock(&index_schema_->GetTimeSlicedMutex());
    query::ScoreTextQuery(*index_schema_, root, scorer, candidates);
    return candidates;
  }

  static std::vector<std::string> Order(
      const std::vector<indexes::BorrowedNeighbor>& ranked) {
    std::vector<std::string> keys;
    keys.reserve(ranked.size());
    for (const auto& n : ranked) keys.push_back(std::string(n.key.Str()));
    return keys;
  }

  static float ScoreOf(const std::vector<indexes::BorrowedNeighbor>& ranked,
                       absl::string_view key) {
    for (const auto& n : ranked) {
      if (n.key.Str() == key) return n.score;
    }
    ADD_FAILURE() << "key " << key << " not in ranked results";
    return 0.0f;
  }

  std::shared_ptr<MockIndexSchema> index_schema_;
  std::shared_ptr<indexes::Text> text_index_;
  uint64_t sequence_number_ = 0;
};

// A rarer term (lower doc-frequency) outranks a common one, and the doc with
// the higher term frequency ranks first within a single term.
TEST_F(TextScoringTest, SingleTermRanksByTfAndIdf) {
  AddDoc("doc:1", "apple apple banana");
  AddDoc("doc:2", "apple banana");
  AddDoc("doc:3", "banana cherry");

  auto term = Term("apple");
  auto ranked = Score(term.get());

  // Only doc:1 and doc:2 contain "apple"; doc:1 has TF=2 vs doc:2 TF=1.
  EXPECT_THAT(Order(ranked), ElementsAre("doc:1", "doc:2"));
  EXPECT_GT(ScoreOf(ranked, "doc:1"), ScoreOf(ranked, "doc:2"));
}

// Docs not containing the term are dropped from the scored result set.
TEST_F(TextScoringTest, NonMatchingDocsExcluded) {
  AddDoc("doc:1", "apple banana");
  AddDoc("doc:2", "cherry");

  auto term = Term("apple");
  auto ranked = Score(term.get());

  EXPECT_THAT(Order(ranked), ElementsAre("doc:1"));
}

// An AND group sums the per-term contributions; a doc matching both terms
// outranks one matching only the shared term.
TEST_F(TextScoringTest, AndGroupSumsTermContributions) {
  AddDoc("doc:1", "apple banana");
  AddDoc("doc:2", "apple cherry");

  std::vector<std::unique_ptr<query::Predicate>> children;
  children.push_back(Term("apple"));
  children.push_back(Term("banana"));
  auto group = std::make_unique<query::ComposedPredicate>(
      query::LogicalOperator::kAnd, std::move(children));

  auto ranked = Score(group.get());

  // doc:1 matches both apple+banana; doc:2 matches only apple.
  EXPECT_THAT(Order(ranked), ElementsAre("doc:1", "doc:2"));
  EXPECT_GT(ScoreOf(ranked, "doc:1"), ScoreOf(ranked, "doc:2"));
}

// An empty candidate set yields no scores and does not crash.
TEST_F(TextScoringTest, EmptyCandidatesYieldsEmpty) {
  AddDoc("doc:1", "apple");
  auto term = Term("apple");
  std::vector<indexes::BorrowedNeighbor> candidates;
  const auto* scorer =
      indexes::scoring::GetScorer(indexes::scoring::ScorerType::kBm25Std);
  vmsdk::ReaderMutexLock lock(&index_schema_->GetTimeSlicedMutex());
  query::ScoreTextQuery(*index_schema_, term.get(), scorer, candidates);
  EXPECT_TRUE(candidates.empty());
}

}  // namespace
}  // namespace valkey_search
