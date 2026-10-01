/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/epoch_reclamation.h"

#include <cstdint>
#include <limits>
#include <thread>
#include <vector>

#include "absl/synchronization/notification.h"
#include "gtest/gtest.h"

namespace vmsdk::epoch {

namespace {

constexpr uint64_t kNoActive = std::numeric_limits<uint64_t>::max();

TEST(EpochReclamationTest, NoGuardMeansNoActiveEpoch) {
  EXPECT_FALSE(InGuard());
  EXPECT_EQ(MinActiveEpoch(), kNoActive);
}

TEST(EpochReclamationTest, NestedGuardsAnnounceOnce) {
  {
    EpochGuard outer;
    EXPECT_TRUE(InGuard());
    const uint64_t announced = MinActiveEpoch();
    EXPECT_NE(announced, kNoActive);

    // The epoch advances, but a nested guard keeps the outer announcement.
    RetireEpoch();
    {
      EpochGuard inner;
      EXPECT_TRUE(InGuard());
      EXPECT_EQ(MinActiveEpoch(), announced);
    }
    // Leaving the inner guard does not end the outer one.
    EXPECT_TRUE(InGuard());
    EXPECT_EQ(MinActiveEpoch(), announced);
  }
  EXPECT_FALSE(InGuard());
  EXPECT_EQ(MinActiveEpoch(), kNoActive);
}

TEST(EpochReclamationTest, RetiredTagBlockedOnlyByEarlierGuards) {
  absl::Notification earlier_entered;
  absl::Notification release_earlier;
  std::thread earlier([&]() {
    EpochGuard guard;
    earlier_entered.Notify();
    release_earlier.WaitForNotification();
  });
  earlier_entered.WaitForNotification();

  // A guard that started before the retire may still read the old record.
  const uint64_t tag = RetireEpoch();
  EXPECT_LE(MinActiveEpoch(), tag);

  // A guard that starts after the retire can only see the new record, so it
  // does not block reclamation.
  absl::Notification later_entered;
  absl::Notification release_later;
  std::thread later([&]() {
    EpochGuard guard;
    later_entered.Notify();
    release_later.WaitForNotification();
  });
  later_entered.WaitForNotification();

  release_earlier.Notify();
  earlier.join();
  EXPECT_GT(MinActiveEpoch(), tag);
  EXPECT_NE(MinActiveEpoch(), kNoActive);

  release_later.Notify();
  later.join();
  EXPECT_EQ(MinActiveEpoch(), kNoActive);
}

TEST(EpochReclamationTest, ExitedThreadsAreNotScanned) {
  // Each thread registers its announcement on first use and must unregister it
  // when it exits; scanning a dangling announcement would read freed TLS.
  for (int round = 0; round < 4; ++round) {
    std::vector<std::thread> threads;
    threads.reserve(16);
    for (int i = 0; i < 16; ++i) {
      threads.emplace_back([]() { EpochGuard guard; });
    }
    for (auto &thread : threads) {
      thread.join();
    }
    EXPECT_EQ(MinActiveEpoch(), kNoActive);
  }
}

}  // namespace

}  // namespace vmsdk::epoch
