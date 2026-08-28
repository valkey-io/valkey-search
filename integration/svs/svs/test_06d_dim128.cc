// test_06d_dim128.cc — isolate save() crash with dim=128, incremental adds.
// Reports exactly which operation (add or save) fails.

#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

#include "svs_common.h"
#include "test_common.h"

using namespace svstest;
using svstest::svs_::DVamana;

static void run(const char* label, size_t dim, size_t n, bool incremental) {
  std::printf("\n[%s] dim=%zu n=%zu incremental=%s\n", label, dim, n,
              incremental ? "yes" : "no");
  rng(42);
  auto data = random_vecs(n, dim);

  DVamana* idx = nullptr;
  auto st = svs_::build_svs(&idx, dim);
  if (!st.ok()) {
    std::printf("  build FAIL: %s\n", st.message());
    return;
  }

  if (incremental) {
    for (size_t i = 0; i < n; ++i) {
      size_t lbl = i;
      st = idx->add(1, &lbl, data.data() + i * dim);
      if (!st.ok()) {
        std::printf("  add(%zu) FAIL: %s\n", i, st.message());
        DVamana::destroy(idx);
        return;
      }
    }
  } else {
    std::vector<size_t> lbls(n);
    for (size_t i = 0; i < n; ++i) lbls[i] = i;
    st = idx->add(n, lbls.data(), data.data());
    if (!st.ok()) {
      std::printf("  bulk add FAIL: %s\n", st.message());
      DVamana::destroy(idx);
      return;
    }
  }
  std::printf("  all adds OK (%zu vectors)\n", n);

  auto path = std::string("/tmp/svs_06d_") +
              std::to_string((unsigned long)getpid()) + "_" + label + ".bin";
  {
    std::ofstream out(path, std::ios::binary);
    st = idx->save(out);
  }
  DVamana::destroy(idx);
  std::remove(path.c_str());
  std::printf("  save(): %s\n", st.ok() ? "OK" : st.message());
}

int main() {
  run("inc_dim4_n10", 4, 10, true);
  run("inc_dim4_n200", 4, 200, true);
  run("inc_dim128_n1", 128, 1, true);
  run("inc_dim128_n10", 128, 10, true);
  run("inc_dim128_n200", 128, 200, true);
  run("bulk_dim128_n200", 128, 200, false);
  run("inc_dim128_n5000", 128, 5000, true);
  run("bulk_dim128_n5000", 128, 5000, false);
}
