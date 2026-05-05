// svs/test_06_save_load.cc
//
// Purpose:
//   Twin of hnsw/test_06_save_load.cc. Build a DynamicVamanaIndex,
//   serialize to a file, load into a fresh index, verify top-K
//   stability.
//
// valkey-search code paths this mimics:
//   VectorSVS::SaveIndexImpl  src/indexes/vector_svs.cc:738-742
//       (currently returns UnimplementedError — this test exercises
//        the SVS runtime's save/load directly to see whether the
//        runtime itself supports the round-trip.)
//
// Spec section: §3.9 — stable save/load format.
//
// Reproduction:
//   (Assumes valkey-search is already built so libsvs_runtime.so and
//   headers exist under .build-release/_deps/svs-src/. If not, see
//   svs_integration_tests/README.md 'One-time setup'.)
//
//   ./build_test.sh svs/test_06_save_load
//   ./svs/test_06_save_load
//
// Expected output on current SVS runtime 0.2.0:
//   **FAIL: top-10 match is typically 1-3 / 10**, even though save and
//   load both return OK. This indicates the reload produces a
//   semantically different graph (different search params, different
//   entry point, or missing state). Surfacing this is the point of the
//   test — spec §3.9 asks for a stable round-trip.
//
// Expected output on the new SVS runtime:
//   100% top-10 match; version-stable across patch releases.

#include <algorithm>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <unistd.h>
#include <vector>

#include "svs_common.h"
#include "test_common.h"

using namespace svstest;
using svstest::svs_::DVamana;

constexpr size_t kDim = 128;
constexpr size_t kN   = 5000;

int main() {
  std::printf("svs/test_06_save_load: dim=%zu N=%zu\n", kDim, kN);

  auto path = std::string("/tmp/svs_test_06_") +
              std::to_string((unsigned long)getpid()) + ".bin";

  rng(1);
  auto data = random_vecs(kN, kDim);
  auto q    = random_vec(kDim);

  std::vector<size_t> pre_labels(10, SIZE_MAX);

  // Build, save.
  {
    DVamana* idx = nullptr;
    auto st = svs_::build_svs(&idx, kDim);
    if (!st.ok()) { fail("build", st.message()); return 1; }

    std::vector<size_t> labels(kN);
    for (size_t i = 0; i < kN; ++i) labels[i] = i;
    st = idx->add(kN, labels.data(), data.data());
    if (!st.ok()) { fail("add", st.message()); DVamana::destroy(idx); return 1; }

    std::vector<float> dists(10);
    std::vector<size_t> slabels(10, SIZE_MAX);
    st = idx->search(1, q.data(), 10, dists.data(), slabels.data(),
                     nullptr, nullptr);
    if (!st.ok()) { fail("search-pre-save", st.message()); DVamana::destroy(idx); return 1; }
    pre_labels = slabels;

    section("save");
    std::ofstream out(path, std::ios::binary);
    if (!out) { fail("open-save", path); DVamana::destroy(idx); return 1; }
    st = idx->save(out);
    if (!st.ok()) {
      fail("save", st.message());
      DVamana::destroy(idx);
      return 1;
    }
    out.close();
    std::printf("  saved: %s\n", path.c_str());
    DVamana::destroy(idx);
  }

  // Load.
  section("load");
  DVamana* reloaded = nullptr;
  {
    std::ifstream in(path, std::ios::binary);
    if (!in) { fail("open-load", path); return 1; }
    auto st = DVamana::load(&reloaded, in, ::svs::runtime::v0::MetricType::L2,
                            ::svs::runtime::v0::StorageKind::FP32);
    if (!st.ok()) { fail("load", st.message()); return 1; }
  }

  // Search.
  std::vector<float> dists(10);
  std::vector<size_t> post_labels(10, SIZE_MAX);
  {
    auto st = reloaded->search(1, q.data(), 10, dists.data(),
                               post_labels.data(), nullptr, nullptr);
    if (!st.ok()) { fail("search-post-load", st.message()); DVamana::destroy(reloaded); return 1; }
  }

  section("verify");
  size_t matches = 0;
  for (size_t i = 0; i < 10; ++i)
    if (pre_labels[i] == post_labels[i] && pre_labels[i] != SIZE_MAX)
      ++matches;
  std::printf("  top-10 match after reload: %zu/10\n", matches);

  DVamana::destroy(reloaded);
  std::remove(path.c_str());
  if (matches == 10) { pass("svs/test_06_save_load"); return 0; }
  fail("svs/test_06_save_load",
       "top-10 mismatch: " + std::to_string(matches));
  return 1;
}
