// svs_common.h — tiny wrapper for building a DynamicVamanaIndex the
// same way valkey-search's VectorSVS does (see
// src/indexes/vector_svs.cc:158-205).

#pragma once

#include <svs/runtime/api_defs.h>
#include <svs/runtime/dynamic_vamana_index.h>

namespace svstest::svs_ {

using DVamana = ::svs::runtime::v0::DynamicVamanaIndex;

// Default parameters match the "HNSW-parity" config we use in benchmarks
// (GMD=32 ~ M=16 edges-per-layer equivalent; CWS=128 mirrors EF_CONSTRUCTION=128).
inline ::svs::runtime::v0::Status build_svs(
    DVamana** out,
    size_t dim,
    ::svs::runtime::v0::MetricType metric =
        ::svs::runtime::v0::MetricType::L2,
    ::svs::runtime::v0::StorageKind storage =
        ::svs::runtime::v0::StorageKind::FP32,
    size_t graph_max_degree = 32,
    size_t construction_window_size = 128,
    size_t search_window_size = 50) {
  ::svs::runtime::v0::VamanaIndex::BuildParams build{};
  build.graph_max_degree = graph_max_degree;
  build.construction_window_size = construction_window_size;
  build.alpha = 1.2f;
  ::svs::runtime::v0::VamanaIndex::SearchParams search{};
  search.search_window_size = search_window_size;
  return DVamana::build(out, dim, metric, storage, build, search);
}

}  // namespace svstest::svs
