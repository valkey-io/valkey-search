#pragma once

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <stdexcept>

#ifdef __linux__
#include <sys/mman.h>
#include <unistd.h>
#endif

#include "absl/status/status.h"
#include "iostream.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

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
// Always enable SimSIMD distance kernels. Defined here (before space_l2.h /
// space_ip.h / stop_condition.h test `#if defined(USE_SIMSIMD)`, all of which
// include this header first) rather than via a build flag. SimSIMD does its
// own runtime CPU-feature dispatch, so this stays portable.
#define USE_SIMSIMD
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

// When true, early cancellation is requested
class BaseCancellationFunctor {
 public:
  virtual bool isCancelled() { return false; }
  virtual ~BaseCancellationFunctor(){};
};

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
using DISTFUNC = MTYPE (*)(const void *, const void *, const void *,
                           MTYPE magnitude);
template <typename MTYPE>
using ProductFUNC = MTYPE (*)(const void *, const void *, const void *);

template <typename MTYPE>
class SpaceInterface {
 public:
  // virtual void search(void *);
  virtual size_t get_data_size() = 0;

  virtual DISTFUNC<MTYPE> get_dist_func() = 0;

  virtual void *get_dist_func_param() = 0;

  virtual ~SpaceInterface() {}
};

template <typename dist_t, typename QueryVectorT, typename StoredVectorT>
class AlgorithmInterface {
 public:
  virtual void addPoint(QueryVectorT &&datapoint, labeltype label,
                        bool replace_deleted = false) = 0;

  virtual std::priority_queue<std::pair<dist_t, labeltype>> searchKnn(
      const QueryVectorT &query_data, size_t k,
      BaseFilterFunctor *isIdAllowed = nullptr,
      BaseCancellationFunctor *isCancelled = nullptr) const = 0;

  // Return k nearest neighbor in the order of closer fist
  virtual std::vector<std::pair<dist_t, labeltype>> searchKnnCloserFirst(
      const QueryVectorT &query_data, size_t k,
      BaseFilterFunctor *isIdAllowed = nullptr,
      BaseCancellationFunctor *isCancelled = nullptr) const;

  virtual ~AlgorithmInterface() = default;
};

template <typename dist_t, typename QueryVectorT, typename StoredVectorT>
std::vector<std::pair<dist_t, labeltype>>
AlgorithmInterface<dist_t, QueryVectorT, StoredVectorT>::searchKnnCloserFirst(
    const QueryVectorT &query_data, size_t k, BaseFilterFunctor *isIdAllowed,
    BaseCancellationFunctor *isCancelled) const {
  std::vector<std::pair<dist_t, labeltype>> result;

  // here searchKnn returns the result in the order of further first
  auto ret = searchKnn(query_data, k, isIdAllowed, isCancelled);
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
               size_t element_count, bool use_large_pages = false)
      : element_byte_size_(element_byte_size),
        elements_per_chunk_(elements_per_chunk),
        use_large_pages_(use_large_pages && CanUseLargePages()),
        element_count_(0) {
    resize(element_count);
  }

  ~ChunkedArray() { clear(); }

  ChunkedArray(const ChunkedArray &) = delete;
  ChunkedArray &operator=(const ChunkedArray &) = delete;

  size_t getCapacity() const {
    return element_count_.load(std::memory_order_relaxed);
  }

  size_t getSizePerElement() const { return element_byte_size_; }

  size_t getSizePerChunk() const {
    return elements_per_chunk_ * element_byte_size_;
  }

  // Keeps large-page chunks fully covered by 2 MiB THP candidates without
  // padding their allocation. The extra space holds more elements instead.
  static size_t GetElementsPerChunkForLargePages(
      size_t element_byte_size, size_t default_elements_per_chunk) {
    if (!CanUseLargePages()) {
      return default_elements_per_chunk;
    }
    if (element_byte_size == 0 || default_elements_per_chunk == 0 ||
        default_elements_per_chunk >
            std::numeric_limits<size_t>::max() / element_byte_size) {
      throw std::overflow_error("Chunk allocation size overflow");
    }
    const size_t default_chunk_size =
        default_elements_per_chunk * element_byte_size;
    if (default_chunk_size >
        std::numeric_limits<size_t>::max() - (kLargePageSize - 1)) {
      throw std::overflow_error("Chunk allocation size overflow");
    }
    const size_t large_page_chunk_size =
        ((default_chunk_size + kLargePageSize - 1) / kLargePageSize) *
        kLargePageSize;
    return large_page_chunk_size / element_byte_size;
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
      if (use_large_pages_) {
        FreeLargePageChunk(chunk);
      } else {
        delete[] chunk;
      }
    }
    chunks_.clear();
    element_count_.store(0, std::memory_order_relaxed);
  }

  void resize(size_t new_element_count) {
    size_t chunk_count =
        getChunkCount(element_count_.load(std::memory_order_relaxed));
    size_t new_chunk_count = getChunkCount(new_element_count);

    chunks_.resize(new_chunk_count);
    size_t allocated_chunk_count = chunk_count;
    try {
      for (size_t i = chunk_count; i < new_chunk_count; i++) {
        chunks_[i] = use_large_pages_
                         ? AllocateLargePageChunk()
                         : new char[elements_per_chunk_ * element_byte_size_];
        allocated_chunk_count = i + 1;
        // Note that we don't initialize the memory on purpose. The caller
        // is expected to track the initialization state.
      }
    } catch (...) {
      for (size_t i = chunk_count; i < allocated_chunk_count; i++) {
        if (use_large_pages_) {
          FreeLargePageChunk(chunks_[i]);
        } else {
          delete[] chunks_[i];
        }
      }
      chunks_.resize(chunk_count);
      throw;
    }
    element_count_.store(new_element_count, std::memory_order_relaxed);
  }

 private:
  static constexpr size_t kLargePageSize = 2 * 1024 * 1024;

  static bool CanUseLargePages() {
#ifdef __linux__
    // On older Valkey versions these APIs are absent and the pointers remain
    // null after ValkeyModule_Init. Preserve the ordinary allocation path.
    return ValkeyModule_IncrExternalMemory != nullptr &&
           ValkeyModule_DecrExternalMemory != nullptr;
#else
    return false;
#endif
  }

// `getpagesize()` and the mapping helpers below are Linux-specific. Keep this
// definition behind the same guard as their headers so non-Linux builds do not
// need a transitive declaration for `getpagesize()`.
#ifdef __linux__
  size_t GetLargePageMappingSize() const {
    if (elements_per_chunk_ != 0 &&
        element_byte_size_ > std::numeric_limits<size_t>::max() /
                                 elements_per_chunk_) {
      throw std::overflow_error("Chunk allocation size overflow");
    }
    const size_t chunk_size = getSizePerChunk();
    const size_t page_size = static_cast<size_t>(getpagesize());
    if (chunk_size > std::numeric_limits<size_t>::max() - (page_size - 1)) {
      throw std::overflow_error("Chunk allocation size overflow");
    }
    return ((chunk_size + page_size - 1) / page_size) * page_size;
  }
#endif

  char *AllocateLargePageChunk() {
#ifdef __linux__
    const size_t mapping_size = GetLargePageMappingSize();
    if (mapping_size > std::numeric_limits<size_t>::max() - kLargePageSize) {
      throw std::overflow_error("Chunk allocation size overflow");
    }
    void *mapping = mmap(nullptr, mapping_size + kLargePageSize,
                         PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS,
                         -1, 0);
    if (mapping == MAP_FAILED) {
      throw std::bad_alloc();
    }
    const uintptr_t address = reinterpret_cast<uintptr_t>(mapping);
    const uintptr_t aligned_address =
        (address + kLargePageSize - 1) & ~(kLargePageSize - 1);
    const size_t prefix_size = aligned_address - address;
    const size_t suffix_size = kLargePageSize - prefix_size;
    if (prefix_size != 0) {
      munmap(mapping, prefix_size);
    }
    auto *chunk = reinterpret_cast<char *>(aligned_address);
    if (suffix_size != 0) {
      munmap(chunk + mapping_size, suffix_size);
    }
    // Fully covered 2 MiB ranges can be promoted to THP; the partial tail
    // remains base-page backed, avoiding up to nearly 2 MiB of padding/chunk.
    madvise(chunk, mapping_size, MADV_HUGEPAGE);
    if (ValkeyModule_IncrExternalMemory(mapping_size) != VALKEYMODULE_OK) {
      munmap(chunk, mapping_size);
      throw std::bad_alloc();
    }
    return chunk;
#else
    throw std::bad_alloc();
#endif
  }

  void FreeLargePageChunk(char *chunk) {
#ifdef __linux__
    if (chunk == nullptr) {
      return;
    }
    const size_t mapping_size = GetLargePageMappingSize();
    const int result = ValkeyModule_DecrExternalMemory(mapping_size);
    assert(result == VALKEYMODULE_OK);
    (void)result;
    munmap(chunk, mapping_size);
#endif
  }

  size_t getChunkCount(size_t element_count) const {
    return (element_count + elements_per_chunk_ - 1) / elements_per_chunk_;
  }

  size_t element_byte_size_;
  size_t elements_per_chunk_;
  bool use_large_pages_;
  std::atomic<size_t> element_count_;
  std::deque<char *> chunks_;
};

}  // namespace hnswlib

#pragma GCC diagnostic pop

#include "bruteforce.h"
#include "hnswalg.h"
#include "space_ip.h"
#include "space_l2.h"
#include "stop_condition.h"
