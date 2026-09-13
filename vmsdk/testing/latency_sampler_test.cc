/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/latency_sampler.h"

#include <memory>
#include <thread>  // NOLINT(build/c++11)
#include <vector>

#include "absl/synchronization/blocking_counter.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace vmsdk {
namespace {

class LatencySamplerTest : public vmsdk::ValkeyTest {};

TEST_F(LatencySamplerTest, ValidSampler_RecordsSamples) {
  LatencySampler sampler(1, 1000000000, 2);
  EXPECT_FALSE(sampler.HasSamples());

  sampler.SubmitSample(absl::Milliseconds(5));
  EXPECT_TRUE(sampler.HasSamples());

  std::string stats = sampler.GetStatsString();
  EXPECT_FALSE(stats.empty());
  EXPECT_NE(stats.find("p50="), std::string::npos);
}

TEST_F(LatencySamplerTest, SampleEveryN_OnlySubmitsEveryNth) {
  LatencySampler sampler(1, 1000000000, 2);

  for (int i = 0; i < 99; ++i) {
    auto sw = SAMPLE_EVERY_N(100);
    if (i == 0) {
      EXPECT_NE(sw, nullptr);
    }
    sampler.SubmitSample(std::move(sw));
  }
  EXPECT_TRUE(sampler.HasSamples());
}

TEST_F(LatencySamplerTest, NullStopWatch_IsNoOp) {
  LatencySampler sampler(1, 1000000000, 2);
  sampler.SubmitSample(nullptr);
  EXPECT_FALSE(sampler.HasSamples());
}

TEST_F(LatencySamplerTest, ConcurrentAccess) {
  LatencySampler sampler(1, 1000000000, 2);

  constexpr int kNumThreads = 8;
  constexpr int kSamplesPerThread = 100;
  absl::BlockingCounter counter(kNumThreads);

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&sampler, &counter]() {
      for (int i = 0; i < kSamplesPerThread; ++i) {
        sampler.SubmitSample(absl::Milliseconds(i + 1));
      }
      counter.DecrementCount();
    });
  }

  counter.Wait();
  for (auto &t : threads) {
    t.join();
  }

  EXPECT_TRUE(sampler.HasSamples());
  EXPECT_NE(sampler.GetStatsString().find("p50="), std::string::npos);
}

// Verifies graceful handling when hdr_init fails due to invalid parameters.
// Without the fix, these tests crash with SIGSEGV (NULL histogram dereference).
TEST_F(LatencySamplerTest, HdrInitFailure_InvalidPrecision) {
  // precision=0 is invalid (must be >= 1), causes hdr_init to return EINVAL.
  LatencySampler sampler(1, 1000000000, 0);
  sampler.SubmitSample(absl::Milliseconds(5));

  EXPECT_FALSE(sampler.HasSamples());
  EXPECT_EQ(sampler.GetStatsString(), "p50=0.000,p99=0.000,p99.9=0.000");
}

TEST_F(LatencySamplerTest, HdrInitFailure_InvalidMinValue) {
  // lowest_trackable_value < 1 triggers EINVAL.
  LatencySampler sampler(0, 1000000000, 2);
  sampler.SubmitSample(absl::Milliseconds(5));

  EXPECT_FALSE(sampler.HasSamples());
}

TEST_F(LatencySamplerTest, HdrInitFailure_MinExceedsHalfMax) {
  // lowest_trackable_value * 2 > highest_trackable_value triggers EINVAL.
  LatencySampler sampler(100, 150, 2);
  sampler.SubmitSample(absl::Milliseconds(5));

  EXPECT_FALSE(sampler.HasSamples());
}

}  // namespace
}  // namespace vmsdk
