// svs/test_05_incremental_add_latency.cc
//
// Purpose:
//   Twin of hnsw/test_05_incremental_add_latency.cc. Measures per-vector
//   SVS add() latency as the index grows. This is the test that most
//   directly shows why valkey-search buffers 10 000 inserts internally
//   before calling add().
//
// valkey-search code paths this mimics:
//   Hypothetical future state where VectorSVS::AddRecordImpl calls
//   idx->add(1, &label, v) directly instead of buffering into
//   pending_buffer_ at src/indexes/vector_svs.cc:230-254. The client
//   wants this to be fast enough that the buffer can be removed.
//
// Spec section: §3.2, §4.1.
//
// Reproduction:
//   (Assumes valkey-search is already built so libsvs_runtime.so and
//   headers exist under .build-release/_deps/svs-src/. If not, see
//   svs_integration_tests/README.md 'One-time setup'.)
//
//   ./build_test.sh svs/test_05_incremental_add_latency
//   ./svs/test_05_incremental_add_latency
//
// Expected output on current SVS runtime 0.2.0:
//   Per-vector add is in the milliseconds-to-seconds range (varies
//   dramatically with N). This is what motivates the pending_buffer_
//   workaround.
//
// Expected output on the new SVS runtime:
//   Per-vector add stays in the low milliseconds as N grows, so
//   valkey-search can delete pending_buffer_ entirely.

#include <algorithm>
#include <climits>
#include <cstdio>
#include <vector>

#include "svs_common.h"
#include "test_common.h"

using namespace svstest;
using svstest::svs_::DVamana;

constexpr size_t kDim      = 128;
constexpr size_t kTotalN   = 5000;  // smaller than HNSW because SVS is slow
constexpr size_t kBucketN  = 500;

int main() {
  std::printf("svs/test_05_incremental_add_latency: dim=%zu total=%zu\n",
              kDim, kTotalN);

  DVamana* idx = nullptr;
  auto st = svs_::build_svs(&idx, kDim);
  if (!st.ok()) { fail("build", st.message()); return 1; }

  rng(1);
  auto data = random_vecs(kTotalN, kDim);

  section("per-vector add latency");
  std::vector<double> bucket;
  bucket.reserve(kBucketN);
  std::printf("  %-8s %-10s %-10s %-10s\n",
              "after", "avg_us", "p50_us", "p99_us");
  for (size_t i = 0; i < kTotalN; ++i) {
    size_t label = i;
    auto t0 = clock_t_::now();
    auto s = idx->add(1, &label, data.data() + i * kDim);
    if (!s.ok()) {
      fail("add",
           "i=" + std::to_string(i) + ": " + s.message());
      DVamana::destroy(idx); return 1;
    }
    bucket.push_back(ms_since(t0) * 1000.0);
    if ((i + 1) % kBucketN == 0) {
      std::sort(bucket.begin(), bucket.end());
      double sum = 0; for (double x : bucket) sum += x;
      double avg = sum / bucket.size();
      double p50 = bucket[bucket.size() / 2];
      double p99 = bucket[(size_t)(bucket.size() * 0.99)];
      std::printf("  %-8zu %-10.1f %-10.1f %-10.1f\n",
                  i + 1, avg, p50, p99);
      bucket.clear();
    }
  }

  DVamana::destroy(idx);
  pass("svs/test_05_incremental_add_latency");
  return 0;
}
