// Pure hnswlib (upstream) memory benchmark.
//
// Upstream hnswlib vs. valkey-search's fork: the only memory-relevant
// difference is pointer-vs-memcpy storage. Upstream memcpys the vector
// bytes into its internal slab. The valkey-search fork stores an 8-byte
// pointer per node instead. Either way, the full vector bytes have to
// live *somewhere* in the process: in the fork they live in the intern
// store, in upstream they live in hnswlib's slab. Total process RSS is
// identical; only the *attribution* differs.
//
// For this benchmark (no external intern store), using upstream produces
// the same total-process RSS that the fork would, with a much simpler
// build. The point of this benchmark is to establish a pure-library
// baseline to compare against libsvs — not to debate fork-vs-upstream.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "common.h"
#include "hnswlib/hnswlib.h"

int main(int argc, char** argv) {
  size_t N = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 100000;
  size_t DIM = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 768;
  size_t M = 32;
  size_t EF_CONSTRUCTION = 128;

  std::string out_dir = "/tmp/pure_lib_bench";
  if (const char* env = std::getenv("OUT_DIR")) out_dir = env;
  out_dir += "/" + std::to_string(N) + "/hnsw";

  std::printf("=== pure hnswlib N=%zu DIM=%zu M=%zu ===\n", N, DIM, M);

  snapshot(out_dir + "/stage0_startup", "stage0_startup");

  hnswlib::L2Space space(DIM);
  std::unique_ptr<hnswlib::HierarchicalNSW<float>> index(
      new hnswlib::HierarchicalNSW<float>(&space, N, M, EF_CONSTRUCTION));

  snapshot(out_dir + "/stage1_empty_index", "stage1_empty_index");

  auto data = generate_vectors(N, DIM);
  snapshot(out_dir + "/stage2_data_generated", "stage2_data_generated");

  for (size_t i = 0; i < N; ++i) {
    index->addPoint(data.data() + i * DIM, i);
    if ((i + 1) % 10000 == 0) {
      std::printf("  added %zu\n", i + 1);
      std::fflush(stdout);
    }
  }

  snapshot(out_dir + "/stage3_indexed", "stage3_indexed");

  // Free the input vector buffer. In valkey-search, the intern store holds
  // the equivalent memory permanently. In upstream hnswlib, the library
  // already copied every vector into its slab, so we can free our buffer
  // and re-measure — revealing what hnswlib alone costs.
  std::vector<float>().swap(data);
  snapshot(out_dir + "/stage4_after_drop_input", "stage4_after_drop_input");

  return 0;
}
