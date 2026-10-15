/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

// Force-included into the SVS C binding sources (src/indexes/CMakeLists.txt),
// which valkey-search compiles without OpenMP. The bindings name
// svs::threads::OMPThreadPool in one unguarded switch case
// (bindings/c/src/threadpool.hpp, SVS_THREADPOOL_KIND_OMP); this stub makes
// that compile and throws if it is ever selected. Interim: delete once
// upstream guards the case with SVS_OMP.

#ifndef VALKEYSEARCH_SRC_INDEXES_SVS_SHIM_SVS_OMP_SHIM_H_
#define VALKEYSEARCH_SRC_INDEXES_SVS_SHIM_SVS_OMP_SHIM_H_

#include <cstddef>
#include <functional>
#include <stdexcept>

#include "svs/lib/preprocessor.h"
#include "svs/lib/threads/threadpool.h"

#if !SVS_OMP
namespace svs::threads {
class OMPThreadPool {
 public:
  explicit OMPThreadPool(size_t /*num_threads*/) {
    throw std::runtime_error(
        "SVS_THREADPOOL_KIND_OMP is unavailable: SVS was built without OpenMP");
  }
  size_t size() const { return 1; }
  void parallel_for(std::function<void(size_t)> f, size_t n) {
    for (size_t i = 0; i < n; ++i) f(i);
  }
  void resize(size_t /*num_threads*/) {}
};
}  // namespace svs::threads
#endif  // !SVS_OMP

#endif  // VALKEYSEARCH_SRC_INDEXES_SVS_SHIM_SVS_OMP_SHIM_H_
