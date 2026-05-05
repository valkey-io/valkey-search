// hnsw_common.h — tiny wrapper for building the upstream hnswlib index
// the same way valkey-search's VectorHNSW does (see
// src/indexes/vector_hnsw.cc:82-108).

#pragma once

#include <memory>

// Vendored upstream hnswlib 0.8.0. Header-only.
#include "third_party/hnswlib/hnswlib.h"
#include "third_party/hnswlib/hnswalg.h"
#include "third_party/hnswlib/space_l2.h"

namespace svstest::hnsw {

struct HnswIndex {
  std::unique_ptr<hnswlib::L2Space> space;
  std::unique_ptr<hnswlib::HierarchicalNSW<float>> algo;
};

// Mirrors valkey-search's HNSW-index construction (M=16, efC=200 are
// the defaults the VectorHNSW wrapper seeds from the proto). We pick a
// modest initial_cap so tests stay light; resize-if-full isn't exercised
// here because the asks are about threading/API, not capacity.
inline HnswIndex make_hnsw(size_t dim, size_t initial_cap,
                           size_t M = 16, size_t ef_construction = 200,
                           size_t ef_runtime = 50) {
  HnswIndex idx;
  idx.space = std::make_unique<hnswlib::L2Space>(dim);
  idx.algo = std::make_unique<hnswlib::HierarchicalNSW<float>>(
      idx.space.get(), initial_cap, M, ef_construction);
  idx.algo->setEf(ef_runtime);
  // Aligned with VectorHNSW::Create (src/indexes/vector_hnsw.cc:101).
  idx.algo->allow_replace_deleted_ = false;
  return idx;
}

}  // namespace svstest::hnsw
