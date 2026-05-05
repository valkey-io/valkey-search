// hnsw/test_05_incremental_add_latency.cc
//
// Purpose:
//   Measure per-vector add() latency on hnswlib as the index grows.
//   Reports average, p50, and p99 across buckets of 1000 inserts.
//
// valkey-search code path this mimics:
//   VectorHNSW::AddRecordImpl  src/indexes/vector_hnsw.cc:182-204 —
//   one addPoint() per call. valkey-search expects this to be
//   sub-millisecond so HSET can return synchronously.
//
// Spec section: §3.2 AddRecordImpl — per-vector add must be cheap.
//
// Reproduction:
//   (Assumes valkey-search is already built so libsvs_runtime.so and
//   headers exist under .build-release/_deps/svs-src/. If not, see
//   svs_integration_tests/README.md 'One-time setup'.)
//
//   ./build_test.sh hnsw/test_05_incremental_add_latency
//   ./hnsw/test_05_incremental_add_latency
//
// Expected output:
//   Per-vector add latency stays roughly log(N) with N, in the tens of
//   microseconds range for a 128-dim index up to 50k elements.

#include <algorithm>
#include <cstdio>
#include <vector>

#include "hnsw_common.h"
#include "test_common.h"

using namespace svstest;

constexpr size_t kDim      = 128;
constexpr size_t kTotalN   = 20000;
constexpr size_t kBucketN  = 1000;

int main() {
  std::printf("hnsw/test_05_incremental_add_latency: dim=%zu total=%zu\n",
              kDim, kTotalN);

  auto idx = hnsw::make_hnsw(kDim, kTotalN + 1000);
  rng(1);
  auto data = random_vecs(kTotalN, kDim);

  section("per-vector add latency");
  std::vector<double> bucket;
  bucket.reserve(kBucketN);
  std::printf("  %-8s %-10s %-10s %-10s\n", "after", "avg_us", "p50_us", "p99_us");
  for (size_t i = 0; i < kTotalN; ++i) {
    auto t0 = clock_t_::now();
    idx.algo->addPoint(data.data() + i * kDim, i);
    bucket.push_back(ms_since(t0) * 1000.0);  // microseconds
    if ((i + 1) % kBucketN == 0) {
      std::sort(bucket.begin(), bucket.end());
      double sum = 0;
      for (double x : bucket) sum += x;
      double avg = sum / bucket.size();
      double p50 = bucket[bucket.size() / 2];
      double p99 = bucket[(size_t)(bucket.size() * 0.99)];
      std::printf("  %-8zu %-10.1f %-10.1f %-10.1f\n",
                  i + 1, avg, p50, p99);
      bucket.clear();
    }
  }

  pass("hnsw/test_05_incremental_add_latency");
  return 0;
}
