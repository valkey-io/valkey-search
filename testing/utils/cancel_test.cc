/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/cancel.h"

#include <atomic>
#include <stop_token>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search {
namespace {

class CancelTest : public vmsdk::ValkeyTest {
 protected:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    auto timeout_poll_frequency =
        vmsdk::debug::ControlledGet("TimeoutPollFrequency");
    ASSERT_TRUE(timeout_poll_frequency.ok()) << timeout_poll_frequency.status();
    saved_timeout_poll_frequency_ = *timeout_poll_frequency;

    auto force_timeout = vmsdk::debug::ControlledGet("ForceTimeout");
    ASSERT_TRUE(force_timeout.ok()) << force_timeout.status();
    saved_force_timeout_ = *force_timeout;
  }

  void TearDown() override {
    VMSDK_EXPECT_OK(vmsdk::debug::ControlledSet("TimeoutPollFrequency",
                                                saved_timeout_poll_frequency_));
    VMSDK_EXPECT_OK(
        vmsdk::debug::ControlledSet("ForceTimeout", saved_force_timeout_));
    vmsdk::ValkeyTest::TearDown();
  }

  std::string saved_timeout_poll_frequency_;
  std::string saved_force_timeout_;
};

// Check cancellation visibility across threads.
TEST_F(CancelTest, CancellationIsVisibleAcrossThreads) {
  auto token = cancel::Make(60 * 60 * 1000, nullptr);
  std::atomic_bool ready{false};
  std::atomic_bool observed{false};

  std::thread observer([&] {
    ready.store(true, std::memory_order_release);
    for (;;) {
      if (token->IsCancelled()) {
        observed.store(true, std::memory_order_relaxed);
        return;
      }
      std::this_thread::yield();
    }
  });

  while (!ready.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  token->Cancel();
  observer.join();

  EXPECT_TRUE(observed.load(std::memory_order_relaxed));
}

// Check that Cancel invokes a stop callback once.
TEST_F(CancelTest, StopCallbackFiresOnCancel) {
  auto token = cancel::Make(60 * 60 * 1000, nullptr);
  int fired = 0;
  std::stop_callback stop_callback(token->GetStopToken(), [&] { ++fired; });
  EXPECT_EQ(fired, 0);
  token->Cancel();
  EXPECT_EQ(fired, 1);
  // Cancel is idempotent.
  token->Cancel();
  EXPECT_EQ(fired, 1);
  EXPECT_TRUE(token->IsCancelled());
}

// Check that a stopped token invokes a new callback immediately.
TEST_F(CancelTest, StopCallbackFiresImmediatelyIfAlreadyCancelled) {
  auto token = cancel::Make(60 * 60 * 1000, nullptr);
  token->Cancel();
  bool fired = false;
  // A stop callback runs at registration if the token is stopped.
  std::stop_callback stop_callback(token->GetStopToken(),
                                   [&] { fired = true; });
  EXPECT_TRUE(fired);
}

// Check that an expired deadline requests stop.
TEST_F(CancelTest, TimeoutRequestsStop) {
  auto token = cancel::Make(0, nullptr);  // Deadline has already passed.
  bool fired = false;
  std::stop_callback stop_callback(token->GetStopToken(),
                                   [&] { fired = true; });
  // Poll until the deadline check runs.
  VMSDK_EXPECT_OK(vmsdk::debug::ControlledSet("TimeoutPollFrequency", "1"));
  bool cancelled = false;
  for (int i = 0; i < 1000 && !cancelled; ++i) {
    cancelled = token->IsCancelled();
  }
  EXPECT_TRUE(cancelled);
  EXPECT_TRUE(fired);
}

// Check that a forced timeout requests stop.
TEST_F(CancelTest, ForceTimeoutRequestsStop) {
  auto token = cancel::Make(60 * 60 * 1000, nullptr);
  bool fired = false;
  std::stop_callback stop_callback(token->GetStopToken(),
                                   [&] { fired = true; });
  VMSDK_EXPECT_OK(vmsdk::debug::ControlledSet("TimeoutPollFrequency", "1"));
  VMSDK_EXPECT_OK(vmsdk::debug::ControlledSet("ForceTimeout", "yes"));
  bool cancelled = false;
  for (int i = 0; i < 1000 && !cancelled; ++i) {
    cancelled = token->IsCancelled();
  }
  EXPECT_TRUE(cancelled);
  EXPECT_TRUE(fired);
}

}  // namespace
}  // namespace valkey_search
