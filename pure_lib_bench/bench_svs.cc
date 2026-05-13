// Pure libsvs memory benchmark.
//
// Instantiates svs::runtime::v0::DynamicVamanaIndex directly against the
// libsvs_runtime.so that valkey-search uses. No Valkey, no module, no
// intern store, no raw_vectors_. Just the library and a std::vector<float>
// of input data.
//
// Usage:
//   ./bench_svs N DIM [storage_kind]
// storage_kind: "fp32" (default), "lvq4x8", "leanvec4x8"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common.h"
#include "svs/runtime/api_defs.h"
#include "svs/runtime/dynamic_vamana_index.h"
#include "svs/runtime/vamana_index.h"

using svs::runtime::v0::DynamicVamanaIndex;
using svs::runtime::v0::MetricType;
using svs::runtime::v0::StorageKind;
using svs::runtime::v0::VamanaIndex;

static StorageKind parse_kind(const std::string& s) {
  if (s == "fp32") return StorageKind::FP32;
  if (s == "lvq4x8") return StorageKind::LVQ4x8;
  if (s == "lvq4x4") return StorageKind::LVQ4x4;
  if (s == "leanvec4x8") return StorageKind::LeanVec4x8;
  std::fprintf(stderr, "unknown storage kind: %s\n", s.c_str());
  std::exit(1);
}

int main(int argc, char** argv) {
  size_t N = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 100000;
  size_t DIM = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 768;
  std::string kind_str = argc > 3 ? argv[3] : "fp32";
  StorageKind kind = parse_kind(kind_str);

  // Block size is the strongest lever on SVS mmap amplification. Default
  // 30 = 1 GiB blocks. Take an override from env so we can demonstrate
  // the effect.
  size_t blocksize_exp = 30;
  if (const char* env = std::getenv("BLOCKSIZE_EXP")) {
    blocksize_exp = std::strtoull(env, nullptr, 10);
  }

  std::string out_dir = "/tmp/pure_lib_bench";
  if (const char* env = std::getenv("OUT_DIR")) out_dir = env;
  out_dir += "/" + std::to_string(N) + "/svs_" + kind_str;
  if (blocksize_exp != 30) {
    out_dir += "_block" + std::to_string(blocksize_exp);
  }

  std::printf("=== pure libsvs N=%zu DIM=%zu kind=%s block_exp=%zu ===\n", N,
              DIM, kind_str.c_str(), blocksize_exp);

  snapshot(out_dir + "/stage0_startup", "stage0_startup");

  VamanaIndex::BuildParams bp;
  bp.graph_max_degree = 32;
  bp.prune_to = 32;
  bp.alpha = 1.2f;
  bp.construction_window_size = 128;
  bp.max_candidate_pool_size = 200;
  bp.use_full_search_history = true;

  VamanaIndex::SearchParams sp;
  sp.search_window_size = 50;
  sp.search_buffer_capacity = 50;

  VamanaIndex::DynamicIndexParams dip;
  dip.blocksize_exp = blocksize_exp;

  DynamicVamanaIndex* index = nullptr;
  auto st = DynamicVamanaIndex::build(&index, DIM, MetricType::L2, kind, bp,
                                      sp, dip);
  if (st.code != svs::runtime::v0::ErrorCode::SUCCESS || index == nullptr) {
    const char* m = st.message();
    std::fprintf(stderr, "build failed: %s\n", m ? m : "<no msg>");
    return 1;
  }

  snapshot(out_dir + "/stage1_empty_index", "stage1_empty_index");

  auto data = generate_vectors(N, DIM);
  snapshot(out_dir + "/stage2_data_generated", "stage2_data_generated");

  // Add in a single batch to match SVS's natural bulk-ingest. Pass all
  // labels [0..N) and all data at once.
  std::vector<size_t> labels(N);
  for (size_t i = 0; i < N; ++i) labels[i] = i;

  // SVS's add signature is (n, labels, data). Chunk it into moderate
  // batches so we don't hit a single-call limit, but stay bulk to match
  // the real add path.
  const size_t BATCH = 10000;
  for (size_t i = 0; i < N; i += BATCH) {
    size_t n = std::min(BATCH, N - i);
    auto add_st = index->add(n, labels.data() + i, data.data() + i * DIM);
    if (add_st.code != svs::runtime::v0::ErrorCode::SUCCESS) {
      const char* m = add_st.message();
      std::fprintf(stderr, "add failed at %zu: %s\n", i, m ? m : "<no msg>");
      return 1;
    }
    std::printf("  added %zu / %zu\n", i + n, N);
    std::fflush(stdout);
  }

  snapshot(out_dir + "/stage3_indexed", "stage3_indexed");

  // SVS copies data into its own storage during add(). We can now free
  // the input buffer and re-measure — that shows the true SVS-only cost.
  std::vector<float>().swap(data);
  std::vector<size_t>().swap(labels);
  snapshot(out_dir + "/stage4_after_drop_input", "stage4_after_drop_input");

  // Report SVS's self-reported block size too, for the record.
  std::printf("  blocksize_bytes=%zu (block_exp=%zu)\n",
              index->blocksize_bytes(), blocksize_exp);

  DynamicVamanaIndex::destroy(index);
  return 0;
}
