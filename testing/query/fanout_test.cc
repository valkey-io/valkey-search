/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/fanout.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/synchronization/notification.h"
#include "absl/time/time.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/util.h"
#include "src/query/search.h"
#include "testing/common.h"
#include "testing/coordinator/common.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/testing_infra/module.h"

namespace valkey_search::query::fanout {
namespace {

// Hands the merged SearchResult to the test instead of a blocked client. The
// tracker may finish on either the main or a background thread.
class CapturingSearchParameters : public SearchParameters {
 public:
  CapturingSearchParameters(SearchResult *out, absl::Notification *done)
      : out_(out), done_(done) {
    timeout_ms = 10000;
    db_num = 0;
    cancellation_token = cancel::Make(timeout_ms, nullptr);
  }
  void QueryCompleteBackground(
      std::unique_ptr<SearchParameters> self) override {
    Complete();
  }
  void QueryCompleteMainThread(
      std::unique_ptr<SearchParameters> self) override {
    Complete();
  }

 private:
  void Complete() {
    *out_ = std::move(search_result);
    done_->Notify();
  }
  SearchResult *out_;
  absl::Notification *done_;
};

struct ShardHit {
  std::string key;
  float distance;
};

class FanoutTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    InitThreadPools(/*readers=*/1, /*writers=*/1, /*utility=*/1);
    index_schema_ = CreateVectorHNSWSchema("idx", &fake_ctx_).value();
    auto pool = std::make_unique<coordinator::MockClientPool>();
    client_pool_ = pool.get();
    ValkeySearch::Instance().SetCoordinatorClientPool(std::move(pool));
  }

  void TearDown() override {
    // The schema unsubscribes from the KeyspaceEventManager on destruction,
    // so it must go before the base fixture tears that manager down.
    index_schema_.reset();
    ValkeySearchTest::TearDown();
  }

  // One remote primary per shard, each answering with the given hits.
  vmsdk::cluster_map::NodeInfo AddRemoteShard(int port,
                                              std::vector<ShardHit> hits,
                                              uint64_t total_count) {
    auto client = std::make_shared<coordinator::MockClient>();
    EXPECT_CALL(*client_pool_,
                GetClient(testing::StrEq(coordinator::FormatAddressWithPort(
                    "127.0.0.1", coordinator::GetCoordinatorPort(port)))))
        .WillRepeatedly(testing::Return(client));
    EXPECT_CALL(*client, SearchIndexPartition(testing::_, testing::_))
        .WillOnce([hits = std::move(hits), total_count](
                      std::unique_ptr<coordinator::SearchIndexPartitionRequest>,
                      coordinator::SearchIndexPartitionCallback done) {
          coordinator::SearchIndexPartitionResponse response;
          response.set_total_count(total_count);
          for (const auto &hit : hits) {
            auto *entry = response.add_neighbors();
            entry->set_key(hit.key);
            entry->set_distance(hit.distance);
            entry->set_score(hit.distance);
            // Remote hits always carry content; NOCONTENT sends it empty.
            auto *content = entry->add_attribute_contents();
            content->set_identifier("vector");
            content->set_content("");
          }
          done(grpc::Status::OK, response);
        });
    clients_.push_back(client);
    return vmsdk::cluster_map::NodeInfo{
        .node_id = std::string(VALKEYMODULE_NODE_ID_LEN, 'a' + port),
        .is_primary = true,
        .is_local = false,
        .socket_address = {.primary_endpoint = "127.0.0.1",
                           .port = static_cast<uint16_t>(port)},
    };
  }

  std::unique_ptr<CapturingSearchParameters> MakeKnnParameters(int k) {
    auto params = std::make_unique<CapturingSearchParameters>(&result_, &done_);
    params->index_schema_name = "idx";
    params->index_schema = index_schema_;
    params->attribute_alias = "vector";
    params->score_as = vmsdk::MakeUniqueValkeyString("score");
    params->k = k;
    params->limit = {.first_index = 0, .number = 10};
    params->no_content = true;
    params->enable_consistency = false;
    query_vector_.assign(100, 1.0f);
    params->query = VectorToStr(query_vector_);
    return params;
  }

  std::unique_ptr<CapturingSearchParameters> MakeNonVectorParameters() {
    auto params = std::make_unique<CapturingSearchParameters>(&result_, &done_);
    params->index_schema_name = "idx";
    params->index_schema = index_schema_;
    // Empty attribute_alias => IsNonVectorQuery(). A high limit keeps the
    // merge from trimming so the assertions see the full merged set.
    params->limit = {.first_index = 0, .number = 1000};
    params->no_content = true;
    params->enable_consistency = false;
    return params;
  }

  SearchResult RunFanout(std::vector<vmsdk::cluster_map::NodeInfo> targets,
                         std::unique_ptr<CapturingSearchParameters> params) {
    VMSDK_EXPECT_OK(PerformSearchFanoutAsync(
        &fake_ctx_, targets, client_pool_, std::move(params),
        ValkeySearch::Instance().GetReaderThreadPool()));
    // All mocked shards answer synchronously on this thread and there is no
    // local target, so the merge normally finishes before the call returns;
    // the bounded wait turns a regression into a failure instead of a hang.
    EXPECT_TRUE(done_.WaitForNotificationWithTimeout(absl::Seconds(10)))
        << "fan-out never completed";
    return std::move(result_);
  }

  std::shared_ptr<MockIndexSchema> index_schema_;
  coordinator::MockClientPool *client_pool_ = nullptr;
  std::vector<std::shared_ptr<coordinator::MockClient>> clients_;
  std::vector<float> query_vector_;
  SearchResult result_;
  absl::Notification done_;
};

std::vector<std::string> Keys(const SearchResult &result) {
  std::vector<std::string> keys;
  for (const auto &neighbor : result.neighbors) {
    keys.emplace_back(neighbor.external_id->Str());
  }
  return keys;
}

// A key indexed on two shards (its slot mid-migration) is returned by both.
// The merge must keep one copy, let a genuine neighbor take the freed slot,
// and not count the key twice.
TEST_F(FanoutTest, KnnMergeDropsDuplicateKeyAcrossShards) {
  std::vector<vmsdk::cluster_map::NodeInfo> targets{
      AddRemoteShard(1, {{"vec:1", 0.1f}, {"vec:2", 0.2f}}, 2),
      AddRemoteShard(2, {{"vec:1", 0.1f}, {"vec:3", 0.3f}}, 2),
  };
  auto result = RunFanout(std::move(targets), MakeKnnParameters(/*k=*/3));

  VMSDK_EXPECT_OK(result.status);
  EXPECT_THAT(Keys(result), testing::ElementsAre("vec:1", "vec:2", "vec:3"));
  EXPECT_EQ(result.total_count, 3u);
}

// Distinct keys from different shards are merged untouched: the dedup must not
// collapse anything that is not the same key.
TEST_F(FanoutTest, KnnMergeKeepsDistinctKeys) {
  std::vector<vmsdk::cluster_map::NodeInfo> targets{
      AddRemoteShard(1, {{"vec:1", 0.1f}, {"vec:2", 0.2f}}, 2),
      AddRemoteShard(2, {{"vec:3", 0.3f}, {"vec:4", 0.4f}}, 2),
  };
  auto result = RunFanout(std::move(targets), MakeKnnParameters(/*k=*/3));

  VMSDK_EXPECT_OK(result.status);
  EXPECT_THAT(Keys(result), testing::ElementsAre("vec:1", "vec:2", "vec:3"));
  EXPECT_EQ(result.total_count, 4u);
}

// The non-vector merge path (no k, direct emplace) must dedup by key too, and
// discount the duplicate once from the summed total.
TEST_F(FanoutTest, NonVectorMergeDropsDuplicateKeyAcrossShards) {
  std::vector<vmsdk::cluster_map::NodeInfo> targets{
      AddRemoteShard(1, {{"doc:1", 0.0f}, {"doc:2", 0.0f}}, 2),
      AddRemoteShard(2, {{"doc:1", 0.0f}, {"doc:3", 0.0f}}, 2),
  };
  auto result = RunFanout(std::move(targets), MakeNonVectorParameters());

  VMSDK_EXPECT_OK(result.status);
  EXPECT_THAT(Keys(result),
              testing::UnorderedElementsAre("doc:1", "doc:2", "doc:3"));
  // 2 + 2 summed, minus one for the single dropped duplicate.
  EXPECT_EQ(result.total_count, 3u);
}

// When the shards' summed total is smaller than the number of dropped
// duplicates, the adjusted total saturates at zero rather than underflowing
// the unsigned counter.
TEST_F(FanoutTest, DuplicateAdjustmentSaturatesAtZero) {
  std::vector<vmsdk::cluster_map::NodeInfo> targets{
      AddRemoteShard(1, {{"vec:1", 0.1f}}, 0),
      AddRemoteShard(2, {{"vec:1", 0.1f}}, 0),
  };
  auto result = RunFanout(std::move(targets), MakeKnnParameters(/*k=*/3));

  VMSDK_EXPECT_OK(result.status);
  EXPECT_THAT(Keys(result), testing::ElementsAre("vec:1"));
  EXPECT_EQ(result.total_count, 0u);
}

}  // namespace
}  // namespace valkey_search::query::fanout
