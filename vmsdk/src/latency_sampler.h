/*
 * Copyright (c) 2025, ValkeySearch contributors
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

#ifndef VMSDK_SRC_LATENCY_SAMPLER_H_
#define VMSDK_SRC_LATENCY_SAMPLER_H_

#include <cstdint>
#include <memory>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "third_party/hdrhistogram_c/src/hdr_histogram.h"
#include "vmsdk/src/utils.h"

namespace vmsdk {

// LatencySampler provides a mechanism for tracking latency samples in a
// histogram. It is lazily allocated so it will not take any memory unless
// samples are added.
class LatencySampler {
 public:
  LatencySampler(int64_t min_value, int64_t max_value, int precision)
      : min_value_(min_value),
        max_value_(max_value),
        precision_(precision),
        sample_unit_(absl::Nanoseconds(1)),
        reporting_unit_(absl::Microseconds(1)) {}
  LatencySampler(int64_t min_value, int64_t max_value, int precision,
                 absl::Duration sample_unit, absl::Duration reporting_unit)
      : min_value_(min_value),
        max_value_(max_value),
        precision_(precision),
        sample_unit_(sample_unit),
        reporting_unit_(reporting_unit) {}
  ~LatencySampler() {
    if (initialized_) {
      hdr_close(histogram_);
    }
  }

  void SubmitSample(std::unique_ptr<vmsdk::StopWatch> sample) {
    if (!sample) {
      return;
    }
    SubmitSample(sample->Duration());
  }
  void SubmitSample(absl::Duration latency) {
    absl::MutexLock lock(&histogram_lock_);
    if (!initialized_) {
      hdr_init(min_value_, max_value_, precision_, &histogram_);
      initialized_ = true;
    }
    hdr_record_value(histogram_, absl::ToInt64Nanoseconds(latency) /
                                     absl::ToInt64Nanoseconds(sample_unit_));
  }
  bool HasSamples() {
    absl::MutexLock lock(&histogram_lock_);
    return initialized_;
  }
  std::string GetStatsString() {
    absl::MutexLock lock(&histogram_lock_);
    double p50 = 0;
    double p99 = 0;
    double p999 = 0;
    if (initialized_) {
      double sample_to_reporting_unit =
          absl::ToDoubleMicroseconds(sample_unit_) /
          absl::ToDoubleMicroseconds(reporting_unit_);
      p50 = hdr_value_at_percentile(histogram_, 50) * sample_to_reporting_unit;
      p99 = hdr_value_at_percentile(histogram_, 99) * sample_to_reporting_unit;
      p999 =
          hdr_value_at_percentile(histogram_, 99.9) * sample_to_reporting_unit;
    }
    return absl::StrFormat("p50=%.3f,p99=%.3f,p99.9=%.3f", p50, p99, p999);
  }

 private:
  mutable absl::Mutex histogram_lock_;
  int64_t min_value_;
  int64_t max_value_;
  int precision_;
  absl::Duration sample_unit_;
  absl::Duration reporting_unit_;
  bool initialized_ ABSL_GUARDED_BY(histogram_lock_) = false;
  hdr_histogram *histogram_ ABSL_GUARDED_BY(histogram_lock_);
};

#define SAMPLE_EVERY_N(interval)                   \
  []() -> std::unique_ptr<vmsdk::StopWatch> {      \
    thread_local uint64_t counter = 0;             \
    if (counter++ % interval == 0) {               \
      return std::make_unique<vmsdk::StopWatch>(); \
    }                                              \
    return nullptr;                                \
  }()

}  // namespace vmsdk

#endif  // VMSDK_SRC_LATENCY_SAMPLER_H_
