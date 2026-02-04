#include <cstdlib>

#include "iostream.h"

#pragma once

#ifdef VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES
#include "vmsdk/src/memory_allocation_overrides.h"  // IWYU pragma: keep
#endif

// https://github.com/nmslib/hnswlib/pull/508
// This allows others to provide their own error stream (e.g. RcppHNSW)
#ifndef HNSWLIB_ERR_OVERRIDE
#define HNSWERR std::cerr
#else
#define HNSWERR HNSWLIB_ERR_OVERRIDE
#endif

#define USE_PREFETCH
#ifndef NO_MANUAL_VECTORIZATION
#if (defined(__SSE__) || _M_IX86_FP > 0 || defined(_M_AMD64) || defined(_M_X64))
#define USE_SSE
#ifdef __AVX__
#define USE_AVX
#ifdef __AVX512F__
#define USE_AVX512
#endif
#endif
#endif
#endif

#if defined(USE_AVX) || defined(USE_SSE)
#ifdef _MSC_VER
#include <intrin.h>

#include <stdexcept>
static void cpuid(int32_t out[4], int32_t eax, int32_t ecx) {
  __cpuidex(out, eax, ecx);
}
static __int64 xgetbv(unsigned int x) { return _xgetbv(x); }
#else
#include <cpuid.h>
#include <stdint.h>
#include <x86intrin.h>
static void cpuid(int32_t cpuInfo[4], int32_t eax, int32_t ecx) {
  __cpuid_count(eax, ecx, cpuInfo[0], cpuInfo[1], cpuInfo[2], cpuInfo[3]);
}
static uint64_t xgetbv(unsigned int index) {
  uint32_t eax, edx;
  __asm__ __volatile__("xgetbv" : "=a"(eax), "=d"(edx) : "c"(index));
  return ((uint64_t)edx << 32) | eax;
}
#endif

#if defined(USE_AVX512)
#include <immintrin.h>
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
#if defined(__GNUC__)
#define PORTABLE_ALIGN32 __attribute__((aligned(32)))
#define PORTABLE_ALIGN64 __attribute__((aligned(64)))
#else
#define PORTABLE_ALIGN32 __declspec(align(32))
#define PORTABLE_ALIGN64 __declspec(align(64))
#endif

// Adapted from https://github.com/Mysticial/FeatureDetector
#define _XCR_XFEATURE_ENABLED_MASK 0

static bool AVXCapable() {
  int cpuInfo[4];

  // CPU support
  cpuid(cpuInfo, 0, 0);
  int nIds = cpuInfo[0];

  bool HW_AVX = false;
  if (nIds >= 0x00000001) {
    cpuid(cpuInfo, 0x00000001, 0);
    HW_AVX = (cpuInfo[2] & ((int)1 << 28)) != 0;
  }

  // OS support
  cpuid(cpuInfo, 1, 0);

  bool osUsesXSAVE_XRSTORE = (cpuInfo[2] & (1 << 27)) != 0;
  bool cpuAVXSupport = (cpuInfo[2] & (1 << 28)) != 0;

  bool avxSupported = false;
  if (osUsesXSAVE_XRSTORE && cpuAVXSupport) {
    uint64_t xcrFeatureMask = xgetbv(_XCR_XFEATURE_ENABLED_MASK);
    avxSupported = (xcrFeatureMask & 0x6) == 0x6;
  }
  return HW_AVX && avxSupported;
}

static bool AVX512Capable() {
  if (!AVXCapable()) return false;

  int cpuInfo[4];

  // CPU support
  cpuid(cpuInfo, 0, 0);
  int nIds = cpuInfo[0];

  bool HW_AVX512F = false;
  if (nIds >= 0x00000007) {  //  AVX512 Foundation
    cpuid(cpuInfo, 0x00000007, 0);
    HW_AVX512F = (cpuInfo[1] & ((int)1 << 16)) != 0;
  }

  // OS support
  cpuid(cpuInfo, 1, 0);

  bool osUsesXSAVE_XRSTORE = (cpuInfo[2] & (1 << 27)) != 0;
  bool cpuAVXSupport = (cpuInfo[2] & (1 << 28)) != 0;

  bool avx512Supported = false;
  if (osUsesXSAVE_XRSTORE && cpuAVXSupport) {
    uint64_t xcrFeatureMask = xgetbv(_XCR_XFEATURE_ENABLED_MASK);
    avx512Supported = (xcrFeatureMask & 0xe6) == 0xe6;
  }
  return HW_AVX512F && avx512Supported;
}
#endif

#include <string.h>

#include <iostream>
#include <queue>
#include <vector>

namespace hnswlib {
typedef size_t labeltype;

// This can be extended to store state for filtering (e.g. from a std::set)
class BaseFilterFunctor {
 public:
  virtual bool operator()(hnswlib::labeltype id) { return true; }
  virtual ~BaseFilterFunctor(){};
};

// VALKEYSEARCH BEGIN
//
// When true, early cancellation is requested
//
class BaseCancellationFunctor {
 public:
  virtual bool isCancelled() { return false; }
  virtual ~BaseCancellationFunctor(){};
};
// VALKEYSEARCH END

template <typename dist_t>
class BaseSearchStopCondition {
 public:
  virtual void add_point_to_result(labeltype label, const void *datapoint,
                                   dist_t dist) = 0;

  virtual void remove_point_from_result(labeltype label, const void *datapoint,
                                        dist_t dist) = 0;

  virtual bool should_stop_search(dist_t candidate_dist, dist_t lowerBound) = 0;

  virtual bool should_consider_candidate(dist_t candidate_dist,
                                         dist_t lowerBound) = 0;

  virtual bool should_remove_extra() = 0;

  virtual void filter_results(
      std::vector<std::pair<dist_t, labeltype>> &candidates) = 0;

  virtual ~BaseSearchStopCondition() {}
};

template <typename T>
class pairGreater {
 public:
  bool operator()(const T &p1, const T &p2) { return p1.first > p2.first; }
};

template <typename T>
static void writeBinaryPOD(std::ostream &out, const T &podRef) {
  out.write((char *)&podRef, sizeof(T));
}

template <typename T>
static void readBinaryPOD(std::istream &in, T &podRef) {
  in.read((char *)&podRef, sizeof(T));
}

template <typename MTYPE>
using DISTFUNC = MTYPE (*)(const void *, const void *, const void *);

template <typename MTYPE>
class DistFuncWrapper {
 public:
  DistFuncWrapper() : func_(nullptr) {}

  // Support assignment from a raw function pointer
  DistFuncWrapper &operator=(DISTFUNC<MTYPE> f) {
    assert(f != nullptr && "Assigned distance function cannot be null.");
    func_ = f;
    return *this;
  }
  void SetShouldNormalize(bool should_normalize) {
    should_normalize_ = should_normalize;
  }

  inline MTYPE operator()(const void *pVect1v, const void *pVect2v,
                          const void *qty_ptr) const {
    size_t qty = *((size_t *)qty_ptr);
    MTYPE *mag1 = should_normalize_
                      ? (MTYPE *)((char *)pVect1v + qty * sizeof(MTYPE))
                      : 1;
    MTYPE *mag2 = should_normalize_
                      ? (MTYPE *)((char *)pVect2v + qty * sizeof(MTYPE))
                      : 1;
    return func_(pVect1v, pVect2v, qty_ptr) / (*mag1 * *mag2);
  }

 private:
  DISTFUNC<MTYPE> func_;
  bool should_normalize_;
};

template <typename MTYPE>
class SpaceInterface {
 public:
  SpaceInterface(bool should_normalize) : should_normalize_(should_normalize) {}
  // virtual void search(void *);
  virtual size_t get_data_size() = 0;

  DistFuncWrapper<MTYPE> get_dist_func() {
    auto dist_func = inner_get_dist_func();
    dist_func.SetShouldNormalize(should_normalize_);
    return dist_func;
  }

  virtual void *get_dist_func_param() = 0;

  virtual ~SpaceInterface() {}

 protected:
  virtual DistFuncWrapper<MTYPE> inner_get_dist_func() = 0;

 private:
  bool should_normalize_;
};

template <typename dist_t>
class AlgorithmInterface {
 public:
  virtual void addPoint(const void *datapoint, labeltype label,
                        bool replace_deleted = false) = 0;

  virtual std::priority_queue<std::pair<dist_t, labeltype>> searchKnn(
      const void *, size_t, BaseFilterFunctor *isIdAllowed = nullptr,
      BaseCancellationFunctor *isCancelled = nullptr  // VALKEYSEARCH
  ) const = 0;

  // Return k nearest neighbor in the order of closer fist
  virtual std::vector<std::pair<dist_t, labeltype>> searchKnnCloserFirst(
      const void *query_data, size_t k,
      BaseFilterFunctor *isIdAllowed = nullptr,
      BaseCancellationFunctor *isCancelled = nullptr  // VALKEYSEARCH
  ) const;

  virtual absl::Status SaveIndex(OutputStream &output) = 0;
  virtual ~AlgorithmInterface() {}
};

template <typename dist_t>
std::vector<std::pair<dist_t, labeltype>>
AlgorithmInterface<dist_t>::searchKnnCloserFirst(
    const void *query_data, size_t k, BaseFilterFunctor *isIdAllowed,
    BaseCancellationFunctor *isCancelled  // VALKEYSEARCH
) const {
  std::vector<std::pair<dist_t, labeltype>> result;

  // here searchKnn returns the result in the order of further first
  auto ret =
      searchKnn(query_data, k, isIdAllowed, isCancelled);  // VALKEYSEARCH
  {
    size_t sz = ret.size();
    result.resize(sz);
    while (!ret.empty()) {
      result[--sz] = ret.top();
      ret.pop();
    }
  }

  return result;
}

class ChunkedArray {
 public:
  ChunkedArray(size_t element_byte_size, size_t elements_per_chunk,
               size_t element_count)
      : element_byte_size_(element_byte_size),
        elements_per_chunk_(elements_per_chunk),
        element_count_(0) {
    resize(element_count);
  }

  ~ChunkedArray() { clear(); }

  ChunkedArray(const ChunkedArray &) = delete;
  ChunkedArray &operator=(const ChunkedArray &) = delete;

  size_t getCapacity() const { return element_count_; }

  size_t getSizePerElement() const { return element_byte_size_; }

  size_t getSizePerChunk() const {
    return elements_per_chunk_ * element_byte_size_;
  }

  char *operator[](size_t i) const {
    assert(i < getCapacity());
    if (i >= getCapacity()) return nullptr;
    size_t chunk_index = i / elements_per_chunk_;
    size_t index_in_chunk = i % elements_per_chunk_;
    return chunks_[chunk_index] + element_byte_size_ * index_in_chunk;
  }

  void clear() {
    for (auto chunk : chunks_) {
      delete[] chunk;
    }
    chunks_.clear();
    element_count_ = 0;
  }

  void resize(size_t new_element_count) {
    size_t chunk_count = getChunkCount(element_count_);
    size_t new_chunk_count = getChunkCount(new_element_count);

    chunks_.resize(new_chunk_count);
    for (size_t i = chunk_count; i < new_chunk_count; i++) {
      chunks_[i] = new char[elements_per_chunk_ * element_byte_size_];
      // Note that we don't initialize the memory on purpose. The caller
      // is expected to track the initialization state.
    }
    element_count_ = new_element_count;
  }

 private:
  size_t getChunkCount(size_t element_count) const {
    return (element_count + elements_per_chunk_ - 1) / elements_per_chunk_;
  }

  size_t element_byte_size_;
  size_t elements_per_chunk_;
  size_t element_count_;
  std::deque<char *> chunks_;
};

}  // namespace hnswlib

#pragma GCC diagnostic pop

#include "bruteforce.h"
#include "hnswalg.h"
#include "space_ip.h"
#include "space_l2.h"
#include "stop_condition.h"
