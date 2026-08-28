// test_06c_add_patterns.cc — isolate exactly where single-vector incremental
// add fails vs bulk add succeeds.

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

int main() {
  rng(42);
  constexpr size_t kDim = 4;
  constexpr size_t kN = 10;
  auto data = random_vecs(kN, kDim);

  // --- Case 1: single add(1) to empty graph ---
  std::printf("Case 1: add(1) to empty graph\n");
  {
    DVamana* idx = nullptr;
    auto st = svs_::build_svs(&idx, kDim);
    std::printf("  build(): %s\n", st.ok() ? "OK" : st.message());

    if (st.ok()) {
      size_t label = 0;
      st = idx->add(1, &label, data.data());
      std::printf("  add(1, label=0): %s\n", st.ok() ? "OK" : st.message());
      if (idx) DVamana::destroy(idx);
    }
  }

  // --- Case 2: bulk add(N) to empty graph ---
  std::printf("Case 2: bulk add(%zu) to empty graph\n", kN);
  {
    DVamana* idx = nullptr;
    auto st = svs_::build_svs(&idx, kDim);
    std::printf("  build(): %s\n", st.ok() ? "OK" : st.message());

    if (st.ok()) {
      std::vector<size_t> labels(kN);
      for (size_t i = 0; i < kN; ++i) labels[i] = i;
      st = idx->add(kN, labels.data(), data.data());
      std::printf("  add(%zu): %s\n", kN, st.ok() ? "OK" : st.message());
      if (idx) DVamana::destroy(idx);
    }
  }

  // --- Case 3: add(1) after bulk add (non-empty graph) ---
  std::printf("Case 3: add(1) after initial bulk add (non-empty graph)\n");
  {
    DVamana* idx = nullptr;
    auto st = svs_::build_svs(&idx, kDim);

    // First: bulk add kN vectors to populate the graph
    std::vector<size_t> labels(kN);
    for (size_t i = 0; i < kN; ++i) labels[i] = i;
    st = idx->add(kN, labels.data(), data.data());
    std::printf("  initial bulk add(%zu): %s\n", kN,
                st.ok() ? "OK" : st.message());

    if (st.ok()) {
      // Now add one more vector
      auto extra = random_vec(kDim);
      size_t extra_label = kN;
      st = idx->add(1, &extra_label, extra.data());
      std::printf("  add(1) to non-empty: %s\n", st.ok() ? "OK" : st.message());

      // Save
      if (st.ok()) {
        auto path = std::string("/tmp/svs_06c_") +
                    std::to_string((unsigned long)getpid()) + ".bin";
        std::ofstream out(path, std::ios::binary);
        st = idx->save(out);
        std::printf("  save(): %s\n", st.ok() ? "OK" : st.message());
        std::remove(path.c_str());
      }
    }
    if (idx) DVamana::destroy(idx);
  }

  return 0;
}
