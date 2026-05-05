// hnsw/test_06_save_load.cc
//
// Purpose:
//   Build an index, serialize to disk, load into a fresh index, and
//   verify that searches return the same top-K after the round-trip.
//
// valkey-search code path this mimics:
//   VectorHNSW::SaveIndexImpl  src/indexes/vector_hnsw.cc:237-241
//   VectorHNSW::LoadFromRDB    src/indexes/vector_hnsw.cc:140-173
//   valkey-search wraps hnswlib's SaveIndex/loadIndex inside its chunked
//   RDB streams; here we use hnswlib's direct file API for simplicity.
//
// Spec section: §3.9 SaveIndexImpl / LoadFromRDB.
//
// Reproduction:
//   (Assumes valkey-search is already built so libsvs_runtime.so and
//   headers exist under .build-release/_deps/svs-src/. If not, see
//   svs_integration_tests/README.md 'One-time setup'.)
//
//   ./build_test.sh hnsw/test_06_save_load
//   ./hnsw/test_06_save_load
//
// Expected output:
//   100% match on top-10 search results before and after reload.

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include <vector>

#include "hnsw_common.h"
#include "test_common.h"

using namespace svstest;

constexpr size_t kDim = 128;
constexpr size_t kN   = 5000;

int main() {
  std::printf("hnsw/test_06_save_load: dim=%zu N=%zu\n", kDim, kN);

  auto path = std::string("/tmp/hnsw_test_06_") +
              std::to_string((unsigned long)getpid()) + ".bin";

  rng(1);
  auto data = random_vecs(kN, kDim);
  auto q    = random_vec(kDim);

  // Build + save.
  std::vector<hnswlib::labeltype> pre_labels(10);
  {
    auto idx = hnsw::make_hnsw(kDim, kN + 100);
    for (size_t i = 0; i < kN; ++i)
      idx.algo->addPoint(data.data() + i * kDim, i);
    auto res = idx.algo->searchKnn(q.data(), 10);
    while (!res.empty()) {
      pre_labels[res.size() - 1] = res.top().second;
      res.pop();
    }
    section("save");
    idx.algo->saveIndex(path);
    std::printf("  saved: %s\n", path.c_str());
  }

  // Load + search.
  section("load");
  hnswlib::L2Space space(kDim);
  hnswlib::HierarchicalNSW<float> reloaded(&space, path);
  reloaded.setEf(50);
  std::printf("  loaded: %zu elements\n", reloaded.getCurrentElementCount());

  auto res = reloaded.searchKnn(q.data(), 10);
  std::vector<hnswlib::labeltype> post_labels(10);
  while (!res.empty()) {
    post_labels[res.size() - 1] = res.top().second;
    res.pop();
  }

  section("verify");
  size_t matches = 0;
  for (size_t i = 0; i < 10; ++i)
    if (pre_labels[i] == post_labels[i]) ++matches;
  std::printf("  top-10 match after reload: %zu/10\n", matches);

  std::remove(path.c_str());
  if (matches == 10) { pass("hnsw/test_06_save_load"); return 0; }
  fail("hnsw/test_06_save_load",
       "top-10 mismatch: " + std::to_string(matches));
  return 1;
}
