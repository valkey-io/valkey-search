/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/content_resolution.h"

#include <memory>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "src/index_schema.h"
#include "src/indexes/vector_base.h"
#include "src/query/search.h"
#include "src/utils/cancel.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"

namespace valkey_search {
namespace {

struct CapturedFinalState {
  bool completed{false};
  std::vector<indexes::Neighbor> neighbors;
  size_t total_count{0};
};

// SearchParameters subclass that snapshots the post-DispatchValidatedQuery
// neighbor vector and total_count into an external CapturedFinalState before
// the unique_ptr<self> is dropped. This avoids any self-referential
// ownership of the params object.
class CapturingSearchParameters : public query::SearchParameters {
 public:
  explicit CapturingSearchParameters(CapturedFinalState* sink) : sink_(sink) {
    timeout_ms = 10000;
    db_num = 0;
    cancellation_token = cancel::Make(timeout_ms, nullptr);
  }
  void QueryCompleteBackground(
      std::unique_ptr<SearchParameters> self) override {
    FAIL() << "QueryCompleteBackground should not be called";
  }
  void QueryCompleteMainThread(
      std::unique_ptr<SearchParameters> self) override {
    sink_->completed = true;
    sink_->neighbors = std::move(self->search_result.neighbors);
    sink_->total_count = self->search_result.total_count;
    // self is dropped here; no self-reference retained.
  }

 private:
  CapturedFinalState* sink_;
};

class DispatchValidatedQueryTest : public ValkeySearchTest {};

// T13: DispatchValidatedQuery removes kFail neighbors, refreshes
// sequence_number for kPass neighbors, and adjusts total_count. Verified
// by short-circuiting through ResolveContent's cancellation branch (which
// calls QueryCompleteMainThread before any content fetch).
TEST_F(DispatchValidatedQueryTest,
       RefreshesKPassRemovesKFailAdjustsTotalCount) {
  auto schema = CreateIndexSchema("idx").value();

  auto k_pass_id = StringInternStore::Intern("doc:pass");
  auto k_fail_id = StringInternStore::Intern("doc:fail");
  auto k_unchecked_id = StringInternStore::Intern("doc:unchecked");

  // The kPass neighbor recorded an old sequence number at search time;
  // the schema's current db sequence number is higher. After dispatch,
  // the neighbor's sequence_number must be refreshed.
  schema->SetDbMutationSequenceNumber(k_pass_id, /*sequence_number=*/42);

  CapturedFinalState captured;
  auto params = std::make_unique<CapturingSearchParameters>(&captured);
  params->index_schema = schema;

  auto& neighbors = params->search_result.neighbors;
  neighbors.emplace_back(k_pass_id, /*distance=*/0.0f);
  neighbors.back().sequence_number = 7;
  neighbors.back().validation_state = indexes::ValidationState::kPass;

  neighbors.emplace_back(k_fail_id, /*distance=*/0.0f);
  neighbors.back().sequence_number = 8;
  neighbors.back().validation_state = indexes::ValidationState::kFail;

  neighbors.emplace_back(k_unchecked_id, /*distance=*/0.0f);
  neighbors.back().sequence_number = 9;
  neighbors.back().validation_state = indexes::ValidationState::kNotChecked;

  params->search_result.total_count = 3;

  // Cancel the search so ResolveContent's first branch fires and we can
  // inspect the post-cleanup state without exercising the content-fetch
  // path (which needs a real Valkey context).
  params->cancellation_token->Cancel();

  auto ctx = std::make_shared<query::PendingValidationContext>();
  ctx->params = std::move(params);

  query::DispatchValidatedQuery(ctx);

  ASSERT_TRUE(captured.completed);
  // kFail removed.
  EXPECT_EQ(captured.neighbors.size(), 2u);
  EXPECT_EQ(captured.neighbors[0].external_id, k_pass_id);
  EXPECT_EQ(captured.neighbors[1].external_id, k_unchecked_id);
  // kPass sequence_number refreshed to current db seq.
  EXPECT_EQ(captured.neighbors[0].sequence_number, 42u);
  // kNotChecked untouched.
  EXPECT_EQ(captured.neighbors[1].sequence_number, 9u);
  EXPECT_EQ(captured.neighbors[1].validation_state,
            indexes::ValidationState::kNotChecked);
  // total_count decremented by exactly the kFail count.
  EXPECT_EQ(captured.total_count, 2u);
}

// kFail removal must clamp total_count at zero, never wrap.
TEST_F(DispatchValidatedQueryTest, KFailClampsTotalCountAtZero) {
  auto schema = CreateIndexSchema("idx").value();
  auto k_fail_id = StringInternStore::Intern("doc:fail");

  CapturedFinalState captured;
  auto params = std::make_unique<CapturingSearchParameters>(&captured);
  params->index_schema = schema;

  auto& neighbors = params->search_result.neighbors;
  neighbors.emplace_back(k_fail_id, /*distance=*/0.0f);
  neighbors.back().validation_state = indexes::ValidationState::kFail;

  // total_count smaller than removal count — must clamp.
  params->search_result.total_count = 0;
  params->cancellation_token->Cancel();

  auto ctx = std::make_shared<query::PendingValidationContext>();
  ctx->params = std::move(params);

  query::DispatchValidatedQuery(ctx);

  ASSERT_TRUE(captured.completed);
  EXPECT_EQ(captured.total_count, 0u);
  EXPECT_TRUE(captured.neighbors.empty());
}

}  // namespace
}  // namespace valkey_search
