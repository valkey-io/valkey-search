// test_06b_incremental_save.cc
//
// Mirrors the module's actual add pattern: build(0 elements) then add
// one vector at a time, then save.  Contrasts with test_06_save_load
// which adds all N vectors in a single bulk call.
//
// Runs three sub-cases:
//   A) N=5000, incremental add (one vector at a time) — module's actual pattern
//   B) N=5000, bulk add (single add(N,...) call) — standalone baseline
//   C) N=10,   incremental add — small count, similar to early module tests

#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <vector>

#include "svs_common.h"
#include "test_common.h"

using namespace svstest;
using svstest::svs_::DVamana;

static int run_case(const char* label, size_t dim, size_t n, bool incremental) {
  std::printf("\n=== %s (dim=%zu N=%zu incremental=%s) ===\n", label, dim, n,
              incremental ? "yes" : "no");

  auto path = std::string("/tmp/svs_test_06b_") +
              std::to_string((unsigned long)getpid()) + "_" + label + ".bin";

  rng(42);
  auto data = random_vecs(n, dim);
  auto q = random_vec(dim);

  DVamana* idx = nullptr;
  auto st = svs_::build_svs(&idx, dim);
  if (!st.ok()) {
    fail("build", st.message());
    return 1;
  }

  if (incremental) {
    for (size_t i = 0; i < n; ++i) {
      size_t label_i = i;
      const float* vec = data.data() + i * dim;
      st = idx->add(1, &label_i, vec);
      if (!st.ok()) {
        std::printf("  add(%zu) failed: %s\n", i, st.message());
        DVamana::destroy(idx);
        return 1;
      }
    }
    std::printf("  incremental add: %zu vectors OK\n", n);
  } else {
    std::vector<size_t> labels(n);
    for (size_t i = 0; i < n; ++i) labels[i] = i;
    st = idx->add(n, labels.data(), data.data());
    if (!st.ok()) {
      fail("add-bulk", st.message());
      DVamana::destroy(idx);
      return 1;
    }
    std::printf("  bulk add: %zu vectors OK\n", n);
  }

  std::printf("  calling save()...\n");
  {
    std::ofstream out(path, std::ios::binary);
    st = idx->save(out);
    if (!st.ok()) {
      std::printf("  [SAVE FAILED] %s\n", st.message());
      DVamana::destroy(idx);
      return 1;
    }
  }
  std::printf("  save() returned OK\n");
  DVamana::destroy(idx);

  // Load and check recall
  DVamana* reloaded = nullptr;
  {
    std::ifstream in(path, std::ios::binary);
    st = DVamana::load(&reloaded, in, ::svs::runtime::v0::MetricType::L2,
                       ::svs::runtime::v0::StorageKind::FP32);
    if (!st.ok()) {
      std::printf("  [LOAD FAILED] %s\n", st.message());
      return 1;
    }
  }
  std::vector<float> dists(10);
  std::vector<size_t> result_labels(10, SIZE_MAX);
  st = reloaded->search(1, q.data(), 10, dists.data(), result_labels.data(),
                        nullptr, nullptr);
  DVamana::destroy(reloaded);
  std::remove(path.c_str());

  if (!st.ok()) {
    std::printf("  [SEARCH FAILED] %s\n", st.message());
    return 1;
  }
  std::printf("  load+search OK (first result label=%zu)\n", result_labels[0]);
  return 0;
}

int main() {
  std::printf("test_06b_incremental_save\n");

  int rc = 0;

  // Case A: module's actual pattern — incremental, 5000 vectors
  rc |= run_case("A_incremental_5000", 128, 5000, /*incremental=*/true);

  // Case B: standalone baseline — bulk, 5000 vectors
  rc |= run_case("B_bulk_5000", 128, 5000, /*incremental=*/false);

  // Case C: incremental, small count (like our early module tests)
  rc |= run_case("C_incremental_10", 128, 10, /*incremental=*/true);

  // Case D: incremental, 1 vector (the minimal crashing case in the module)
  rc |= run_case("D_incremental_1", 128, 1, /*incremental=*/true);

  return rc;
}
