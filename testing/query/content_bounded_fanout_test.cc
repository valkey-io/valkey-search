/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/content_bounded_fanout.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gmock/gmock.h"
#include "grpcpp/support/status.h"
#include "gtest/gtest.h"
#include "src/coordinator/client.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/util.h"
#include "src/query/fanout_content_limit.h"
#include "src/query/search.h"
#include "src/utils/cancel.h"
#include "src/valkey_search_options.h"
#include "testing/common.h"
#include "testing/coordinator/common.h"
#include "vmsdk/src/cluster_map.h"

namespace valkey_search::query::fanout {
namespace {

using ::testing::_;
using ::testing::ElementsAre;

// Records the completion instead of replying to a client.
class CapturingSearch : public UnitTestSearchParameters {
 public:
  explicit CapturingSearch(std::unique_ptr<SearchParameters> *sink)
      : sink_(sink) {}
  bool SupportsBoundedContent() const override { return true; }
  void QueryCompleteMainThread(
      std::unique_ptr<SearchParameters> self) override {
    *sink_ = std::move(self);
  }
  void QueryCompleteBackground(
      std::unique_ptr<SearchParameters> self) override {
    *sink_ = std::move(self);
  }

 private:
  std::unique_ptr<SearchParameters> *sink_;
};

void AddResult(coordinator::SearchIndexPartitionResponse &response,
               const std::string &key, float score, bool with_content,
               float distance = 0.0f, uint64_t sequence_number = 0) {
  auto *entry = response.add_neighbors();
  entry->set_key(key);
  entry->set_score(score);
  entry->set_distance(distance);
  entry->set_sequence_number(sequence_number);
  if (with_content) {
    auto *content = entry->add_attribute_contents();
    content->set_identifier("f");
    content->set_content(key);
  } else {
    entry->set_content_omitted(true);
  }
}

// The keys a fetch request asks for.
std::vector<std::string> FetchKeys(const coordinator::FetchContentRequest &r) {
  std::vector<std::string> keys;
  for (const auto &candidate : r.candidates()) {
    keys.push_back(candidate.key());
  }
  return keys;
}

std::vector<std::string> Keys(const SearchResult &result) {
  std::vector<std::string> keys;
  for (const auto &neighbor : result.neighbors) {
    keys.emplace_back(neighbor.external_id->Str());
  }
  return keys;
}

class BoundedContentFanoutTest : public ValkeySearchTest {
 protected:
  static constexpr int kShards = 2;

  void SetUp() override {
    ValkeySearchTest::SetUp();
    for (int shard = 0; shard < kShards; ++shard) {
      vmsdk::cluster_map::NodeInfo node;
      node.socket_address.primary_endpoint = "10.0.0." + std::to_string(shard);
      node.socket_address.port = 6379;
      targets_.push_back(node);
      clients_[shard] = std::make_shared<coordinator::MockClient>();
      const std::string address = coordinator::FormatAddressWithPort(
          node.socket_address.primary_endpoint,
          coordinator::GetCoordinatorPort(node.socket_address.port));
      ON_CALL(pool_, GetClient(absl::string_view(address)))
          .WillByDefault(testing::Return(clients_[shard]));
      ON_CALL(*clients_[shard], SearchIndexPartition(_, _))
          .WillByDefault([this, shard](auto request, auto done) {
            searches_[shard].push_back({std::move(request), std::move(done)});
          });
      ON_CALL(*clients_[shard], FetchContent(_, _))
          .WillByDefault([this, shard](auto request, auto done) {
            fetches_[shard].push_back({std::move(request), std::move(done)});
          });
    }
  }

  // LIMIT `offset` `count`, every shard bounded to `content_limit`.
  void Start(uint64_t offset, uint64_t count, uint64_t content_limit,
             bool partial_results = false) {
    auto search = std::make_unique<CapturingSearch>(&completed_);
    search->limit = LimitParameter{offset, count};
    search->enable_partial_results = partial_results;
    cancellation_token_ = search->cancellation_token;
    BoundedContentPlan plan{offset + count,
                            ShardLimits(kShards, content_limit)};
    VMSDK_EXPECT_OK(PerformBoundedContentFanoutAsync(
        targets_, &pool_, std::move(search), std::move(plan), nullptr));
  }

  // KNN `k`, every shard bounded to `content_limit`.
  void StartKnn(int k, uint64_t content_limit, uint64_t offset = 0) {
    auto search = std::make_unique<CapturingSearch>(&completed_);
    search->attribute_alias = "vec";
    search->k = k;
    search->limit = LimitParameter{offset, static_cast<uint64_t>(k)};
    BoundedContentPlan plan{static_cast<uint64_t>(k),
                            ShardLimits(kShards, content_limit)};
    VMSDK_EXPECT_OK(PerformBoundedContentFanoutAsync(
        targets_, &pool_, std::move(search), std::move(plan), nullptr));
  }

  template <typename Request>
  struct Pending {
    std::unique_ptr<Request> request;
    coordinator::SearchIndexPartitionCallback done;
  };

  template <typename Request>
  static void Reply(std::vector<Pending<Request>> &pending,
                    coordinator::SearchIndexPartitionResponse response,
                    grpc::Status status = grpc::Status::OK) {
    ASSERT_FALSE(pending.empty());
    auto call = std::move(pending.front());
    pending.erase(pending.begin());
    call.done(status, response);
  }

  testing::NiceMock<coordinator::MockClientPool> pool_;
  std::shared_ptr<coordinator::MockClient> clients_[kShards];
  std::vector<vmsdk::cluster_map::NodeInfo> targets_;
  std::vector<Pending<coordinator::SearchIndexPartitionRequest>>
      searches_[kShards];
  std::vector<Pending<coordinator::FetchContentRequest>> fetches_[kShards];
  std::unique_ptr<SearchParameters> completed_;
  cancel::Token cancellation_token_;
};

TEST_F(BoundedContentFanoutTest, EvenDataCompletesInOneRound) {
  Start(/*offset=*/0, /*count=*/2, /*content_limit=*/1);
  ASSERT_EQ(searches_[0].size(), 1u);
  EXPECT_EQ(searches_[0][0].request->limit().number(), 2u);
  EXPECT_EQ(searches_[0][0].request->content_limit(), 1u);

  coordinator::SearchIndexPartitionResponse r0, r1;
  AddResult(r0, "a", 9, true);
  AddResult(r0, "c", 1, false);  // Key-only, but ranks below the top 2.
  r0.set_total_count(2);
  AddResult(r1, "b", 8, true);
  r1.set_total_count(1);
  Reply(searches_[0], r0);
  EXPECT_EQ(completed_, nullptr);
  Reply(searches_[1], r1);

  EXPECT_TRUE(fetches_[0].empty());
  EXPECT_TRUE(fetches_[1].empty());
  ASSERT_NE(completed_, nullptr);
  EXPECT_TRUE(completed_->search_result.status.ok());
  // "c" is key-only past the window, so it is not kept as backfill.
  EXPECT_THAT(Keys(completed_->search_result), ElementsAre("a", "b"));
  EXPECT_EQ(completed_->search_result.total_count, 3u);
}

TEST_F(BoundedContentFanoutTest, SkewFetchesOnlyMissingWinnersFromTheirShard) {
  Start(0, 2, 1);
  coordinator::SearchIndexPartitionResponse r0, r1;
  AddResult(r0, "a", 9, true);
  AddResult(r0, "b", 8, false, /*distance=*/0.25f,  // Winner, no content.
            /*sequence_number=*/42);
  AddResult(r1, "x", 1, true);
  Reply(searches_[0], r0);
  Reply(searches_[1], r1);

  EXPECT_EQ(completed_, nullptr);
  EXPECT_TRUE(fetches_[1].empty());
  ASSERT_EQ(fetches_[0].size(), 1u);
  const auto &fetch = *fetches_[0][0].request;
  EXPECT_THAT(FetchKeys(fetch), ElementsAre("b"));
  EXPECT_FALSE(fetch.query().has_content_limit());
  // The shard keeps the carried score unless the document changed since the
  // first round, which it tells by the carried sequence number.
  EXPECT_EQ(fetch.candidates(0).score(), 8.0f);
  EXPECT_EQ(fetch.candidates(0).distance(), 0.25f);
  EXPECT_EQ(fetch.candidates(0).sequence_number(), 42u);
  EXPECT_TRUE(fetch.candidates(0).content_omitted());

  coordinator::SearchIndexPartitionResponse fetched;
  AddResult(fetched, "b", 8, true);
  Reply(fetches_[0], fetched);

  ASSERT_NE(completed_, nullptr);
  EXPECT_TRUE(completed_->search_result.status.ok());
  const auto &neighbors = completed_->search_result.neighbors;
  ASSERT_GE(neighbors.size(), 2u);
  EXPECT_EQ(neighbors[1].external_id->Str(), "b");
  EXPECT_TRUE(neighbors[1].attribute_contents.has_value());
}

// Regression: key-only candidates past the window are neither winners nor
// usable backfill; left in, the reply path counted them as removed results.
TEST_F(BoundedContentFanoutTest, KeyOnlyCandidatesPastTheWindowAreDropped) {
  Start(0, 2, 1);
  coordinator::SearchIndexPartitionResponse r0, r1;
  AddResult(r0, "a", 9, true);
  AddResult(r0, "b", 8, false);  // Winner: fetched.
  AddResult(r0, "c", 7, false);  // Past the window: dropped, not fetched.
  r0.set_total_count(3);
  AddResult(r1, "x", 1, true);  // Past the window, with content: kept.
  r1.set_total_count(1);
  Reply(searches_[0], r0);
  Reply(searches_[1], r1);
  ASSERT_EQ(fetches_[0].size(), 1u);
  EXPECT_THAT(FetchKeys(*fetches_[0][0].request), ElementsAre("b"));
  coordinator::SearchIndexPartitionResponse fetched;
  AddResult(fetched, "b", 8, true);
  Reply(fetches_[0], fetched);

  ASSERT_NE(completed_, nullptr);
  EXPECT_THAT(Keys(completed_->search_result), ElementsAre("a", "b", "x"));
  EXPECT_EQ(completed_->search_result.total_count, 4u);
}

TEST_F(BoundedContentFanoutTest, CandidatesInsideTheOffsetAreValidated) {
  // LIMIT 1 1: "a" is skipped by the offset, but must still be valid to
  // take up its place.
  Start(/*offset=*/1, /*count=*/1, /*content_limit=*/1);
  coordinator::SearchIndexPartitionResponse r0, r1;
  AddResult(r0, "a", 9, false);
  AddResult(r0, "b", 8, false);
  r0.set_total_count(2);
  AddResult(r1, "c", 7, true);
  r1.set_total_count(1);
  Reply(searches_[0], r0);
  Reply(searches_[1], r1);
  ASSERT_EQ(fetches_[0].size(), 1u);
  EXPECT_THAT(FetchKeys(*fetches_[0][0].request), ElementsAre("a", "b"));

  // "a" no longer matches: "b" is now the one the offset skips, and "c" is
  // the reply.
  coordinator::SearchIndexPartitionResponse fetched;
  AddResult(fetched, "b", 8, true);
  Reply(fetches_[0], fetched);
  ASSERT_NE(completed_, nullptr);
  EXPECT_THAT(Keys(completed_->search_result), ElementsAre("c"));
  EXPECT_EQ(completed_->search_result.total_count, 2u);  // 3 less "a".
}

TEST_F(BoundedContentFanoutTest, WinnerThatNoLongerMatchesIsReplacedByTheNext) {
  Start(0, 2, 1);
  coordinator::SearchIndexPartitionResponse r0, r1;
  AddResult(r0, "a", 9, true);
  AddResult(r0, "b", 8, false);
  AddResult(r0, "c", 7, false);
  r0.set_total_count(3);
  AddResult(r1, "x", 1, true);
  r1.set_total_count(1);
  Reply(searches_[0], r0);
  Reply(searches_[1], r1);
  ASSERT_EQ(fetches_[0].size(), 1u);
  EXPECT_THAT(FetchKeys(*fetches_[0][0].request), ElementsAre("b"));
  Reply(fetches_[0], {});  // "b" no longer matches.

  // "c" moved into the window without content: a second fetch round.
  EXPECT_EQ(completed_, nullptr);
  ASSERT_EQ(fetches_[0].size(), 1u);
  EXPECT_THAT(FetchKeys(*fetches_[0][0].request), ElementsAre("c"));
  coordinator::SearchIndexPartitionResponse fetched;
  AddResult(fetched, "c", 7, true);
  Reply(fetches_[0], fetched);

  ASSERT_NE(completed_, nullptr);
  EXPECT_TRUE(completed_->search_result.status.ok());
  EXPECT_THAT(Keys(completed_->search_result), ElementsAre("a", "c", "x"));
  for (const auto &neighbor : completed_->search_result.neighbors) {
    EXPECT_TRUE(neighbor.attribute_contents.has_value());
  }
  EXPECT_EQ(completed_->search_result.total_count, 3u);  // 4 less "b".
}

TEST_F(BoundedContentFanoutTest, RoundsEndWhenCandidatesRunOut) {
  Start(0, 2, 1);
  coordinator::SearchIndexPartitionResponse r0, r1;
  AddResult(r0, "a", 9, true);
  AddResult(r0, "b", 8, false);
  AddResult(r0, "c", 7, false);
  r0.set_total_count(3);
  Reply(searches_[0], r0);
  Reply(searches_[1], r1);
  Reply(fetches_[0], {});  // "b" gone.
  Reply(fetches_[0], {});  // "c" gone: nothing left to fetch.

  ASSERT_NE(completed_, nullptr);
  EXPECT_TRUE(fetches_[0].empty());
  EXPECT_THAT(Keys(completed_->search_result), ElementsAre("a"));
  EXPECT_EQ(completed_->search_result.total_count, 1u);
}

TEST_F(BoundedContentFanoutTest, FailedFetchFailsWithoutPartialResults) {
  Start(0, 2, 1);
  coordinator::SearchIndexPartitionResponse r0, r1;
  AddResult(r0, "a", 9, true);
  AddResult(r0, "b", 8, false);
  Reply(searches_[0], r0);
  Reply(searches_[1], r1);
  Reply(fetches_[0], {}, grpc::Status(grpc::StatusCode::UNAVAILABLE, "down"));

  ASSERT_NE(completed_, nullptr);
  EXPECT_EQ(completed_->search_result.status.code(),
            absl::StatusCode::kUnavailable);
}

TEST_F(BoundedContentFanoutTest, FailedFetchMovesOnToOtherShardsUnderPartial) {
  Start(0, 2, 1, /*partial_results=*/true);
  coordinator::SearchIndexPartitionResponse r0, r1;
  AddResult(r0, "a", 9, true);
  AddResult(r0, "b", 8, false);
  AddResult(r0, "c", 7, false);
  r0.set_total_count(3);
  AddResult(r1, "x", 5, true);
  AddResult(r1, "y", 4, false);
  r1.set_total_count(2);
  Reply(searches_[0], r0);
  Reply(searches_[1], r1);
  ASSERT_EQ(fetches_[0].size(), 1u);
  Reply(fetches_[0], {}, grpc::Status(grpc::StatusCode::UNAVAILABLE, "down"));

  // Shard 0's key-only candidates cannot be resolved: "x" takes "b"'s place
  // and has content, so no further round is needed.
  ASSERT_NE(completed_, nullptr);
  EXPECT_TRUE(fetches_[0].empty());
  EXPECT_TRUE(fetches_[1].empty());
  EXPECT_TRUE(completed_->search_result.status.ok());
  EXPECT_THAT(Keys(completed_->search_result), ElementsAre("a", "x"));
  EXPECT_EQ(completed_->search_result.total_count, 4u);  // 5 less "b".
}

TEST_F(BoundedContentFanoutTest,
       TimeoutBetweenRoundsRepliesOnlyResolvedCandidates) {
  Start(0, 2, 1, /*partial_results=*/true);
  coordinator::SearchIndexPartitionResponse r0, r1;
  AddResult(r0, "a", 9, true);
  AddResult(r0, "b", 8, false);
  r0.set_total_count(2);
  AddResult(r1, "c", 7, true);
  r1.set_total_count(1);
  Reply(searches_[0], r0);
  cancellation_token_->Cancel();  // The query times out before round 1 ends.
  Reply(searches_[1], r1);

  // No fetch round: "b" is left out of the partial reply, but it matched, so
  // it is still counted.
  EXPECT_TRUE(fetches_[0].empty());
  ASSERT_NE(completed_, nullptr);
  EXPECT_TRUE(completed_->search_result.status.ok());
  EXPECT_THAT(Keys(completed_->search_result), ElementsAre("a", "c"));
  for (const auto &neighbor : completed_->search_result.neighbors) {
    EXPECT_TRUE(neighbor.attribute_contents.has_value());
  }
  EXPECT_EQ(completed_->search_result.total_count, 3u);
}

TEST_F(BoundedContentFanoutTest, FetchCarriesTheTimeLeftNotTheFullTimeout) {
  Start(0, 2, 1);
  const uint64_t timeout_ms = searches_[0][0].request->timeout_ms();
  coordinator::SearchIndexPartitionResponse r0, r1;
  AddResult(r0, "a", 9, true);
  AddResult(r0, "b", 8, false);
  Reply(searches_[0], r0);
  absl::SleepFor(absl::Milliseconds(100));
  Reply(searches_[1], r1);

  ASSERT_EQ(fetches_[0].size(), 1u);
  const uint64_t fetch_timeout_ms =
      fetches_[0][0].request->query().timeout_ms();
  EXPECT_GT(fetch_timeout_ms, 0u);
  EXPECT_LE(fetch_timeout_ms, timeout_ms - 100);
}

TEST_F(BoundedContentFanoutTest, RecomputedScoreReranksAndFetchesTheNext) {
  Start(0, 2, 1);
  coordinator::SearchIndexPartitionResponse r0, r1;
  AddResult(r0, "a", 9, true);
  AddResult(r0, "b", 8, false);
  AddResult(r0, "c", 7, false);
  AddResult(r1, "x", 1, true);
  Reply(searches_[0], r0);
  Reply(searches_[1], r1);
  ASSERT_THAT(FetchKeys(*fetches_[0][0].request), ElementsAre("b"));

  // "b" changed and still matches, but now scores below "c".
  coordinator::SearchIndexPartitionResponse fetched;
  AddResult(fetched, "b", 0.5f, true);
  Reply(fetches_[0], fetched);
  EXPECT_EQ(completed_, nullptr);
  ASSERT_EQ(fetches_[0].size(), 1u);
  EXPECT_THAT(FetchKeys(*fetches_[0][0].request), ElementsAre("c"));

  coordinator::SearchIndexPartitionResponse fetched_c;
  AddResult(fetched_c, "c", 7, true);
  Reply(fetches_[0], fetched_c);
  ASSERT_NE(completed_, nullptr);
  const auto &neighbors = completed_->search_result.neighbors;
  ASSERT_GE(neighbors.size(), 2u);
  EXPECT_EQ(neighbors[0].external_id->Str(), "a");
  EXPECT_EQ(neighbors[1].external_id->Str(), "c");
}

TEST_F(BoundedContentFanoutTest, KnnReplacesADroppedWinnerWithTheNextNearest) {
  StartKnn(/*k=*/2, /*content_limit=*/1);
  coordinator::SearchIndexPartitionResponse r0, r1;
  AddResult(r0, "near", 0, true, /*distance=*/0.1f);
  AddResult(r0, "mid", 0, false, /*distance=*/0.5f);
  AddResult(r1, "far", 0, true, /*distance=*/0.9f);
  Reply(searches_[0], r0);
  Reply(searches_[1], r1);
  ASSERT_THAT(FetchKeys(*fetches_[0][0].request), ElementsAre("mid"));

  Reply(fetches_[0], {});  // "mid" no longer matches.
  ASSERT_NE(completed_, nullptr);
  EXPECT_THAT(Keys(completed_->search_result), ElementsAre("near", "far"));
}

TEST_F(BoundedContentFanoutTest, KnnWithAnOffsetFetchesNothingPastK) {
  // KNN 2 LIMIT 1 2: the reply is "mid" alone. "far" is kept only to replace
  // a winner that drops out, so it is not fetched while none does.
  StartKnn(/*k=*/2, /*content_limit=*/1, /*offset=*/1);
  coordinator::SearchIndexPartitionResponse r0, r1;
  AddResult(r0, "near", 0, true, /*distance=*/0.1f);
  AddResult(r0, "mid", 0, false, /*distance=*/0.5f);
  AddResult(r1, "far", 0, false, /*distance=*/0.9f);
  Reply(searches_[0], r0);
  Reply(searches_[1], r1);
  ASSERT_EQ(fetches_[0].size(), 1u);
  EXPECT_THAT(FetchKeys(*fetches_[0][0].request), ElementsAre("mid"));
  EXPECT_TRUE(fetches_[1].empty());

  coordinator::SearchIndexPartitionResponse fetched;
  AddResult(fetched, "mid", 0, true, /*distance=*/0.5f);
  Reply(fetches_[0], fetched);
  ASSERT_NE(completed_, nullptr);
  EXPECT_THAT(Keys(completed_->search_result), ElementsAre("mid"));
}

TEST_F(BoundedContentFanoutTest, PlanningSkipsBoundsBeyondTheResultsCap) {
  auto &enabled =
      const_cast<vmsdk::config::Boolean &>(options::GetFanoutBoundedContent());
  VMSDK_EXPECT_OK(enabled.SetValue(true));
  CapturingSearch search(&completed_);
  search.limit = LimitParameter{0, 10};
  EXPECT_TRUE(PlanBoundedContentFanout(search, targets_).has_value());
  search.limit = LimitParameter{
      0, options::GetMaxNonVectorSearchResultsFetched().GetValue() + 1ull};
  EXPECT_FALSE(PlanBoundedContentFanout(search, targets_).has_value());
  VMSDK_EXPECT_OK(enabled.SetValue(false));
}

TEST_F(BoundedContentFanoutTest, FailedSearchSkipsTheFetchRound) {
  Start(0, 2, 1);
  coordinator::SearchIndexPartitionResponse r1;
  AddResult(r1, "b", 8, false);
  Reply(searches_[0], {}, grpc::Status(grpc::StatusCode::UNAVAILABLE, "down"));
  Reply(searches_[1], r1);

  EXPECT_TRUE(fetches_[1].empty());
  ASSERT_NE(completed_, nullptr);
  EXPECT_FALSE(completed_->search_result.status.ok());
}

}  // namespace
}  // namespace valkey_search::query::fanout
