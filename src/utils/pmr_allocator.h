/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_PMR_ALLOCATOR_H_
#define VALKEYSEARCH_SRC_UTILS_PMR_ALLOCATOR_H_

#include <cstddef>
#include <cstring>
#include <memory>
#include <memory_resource>
#include <utility>

namespace valkey_search::utils {

// PmrDeleter for use with std::unique_ptr allocated from a memory_resource.
template <typename T>
struct PmrDeleter {
  std::pmr::memory_resource *res = nullptr;

  void operator()(T *ptr) const {
    if (ptr) {
      ptr->~T();
      if (res) {
        res->deallocate(ptr, sizeof(T), alignof(T));
      } else {
        ::operator delete(reinterpret_cast<void *>(ptr));
      }
    }
  }
};

template <typename T, typename... Args>
std::unique_ptr<T, PmrDeleter<T>> MakePmrUnique(std::pmr::memory_resource *res,
                                                Args &&...args) {
  if (res) {
    void *mem = res->allocate(sizeof(T), alignof(T));
    T *ptr = new (mem) T(std::forward<Args>(args)...);
    return std::unique_ptr<T, PmrDeleter<T>>(ptr, PmrDeleter<T>{res});
  } else {
    return std::unique_ptr<T, PmrDeleter<T>>(new T(std::forward<Args>(args)...),
                                             PmrDeleter<T>{nullptr});
  }
}

// CRTP base class providing memory_resource pooling via an allocation header
// prefix. Enables using standard new (res) T(...), delete ptr, and
// std::unique_ptr<T>.
template <typename Derived>
class PmrAllocated {
  struct alignas(alignof(std::max_align_t)) Header {
    std::pmr::memory_resource *res;
  };

 public:
  static constexpr size_t AllocationSize() {
    return sizeof(Header) + sizeof(Derived);
  }
  static void *operator new(size_t size, void *ptr) noexcept { return ptr; }
  static void *operator new(size_t size,
                            std::pmr::memory_resource *res = nullptr) {
    size_t total_size = sizeof(Header) + size;
    void *raw = res ? res->allocate(total_size, alignof(Derived))
                    : ::operator new(total_size);
    static_cast<Header *>(raw)->res = res;
    return static_cast<char *>(raw) + sizeof(Header);
  }

  static void operator delete(void *ptr,
                              std::pmr::memory_resource *res) noexcept {
    if (!ptr) {
      return;
    }
    Header *header =
        reinterpret_cast<Header *>(static_cast<char *>(ptr) - sizeof(Header));
    if (header->res) {
      header->res->deallocate(header, sizeof(Header) + sizeof(Derived),
                              alignof(Derived));
    } else {
      ::operator delete(header);
    }
  }

  static void operator delete(void *ptr) noexcept {
    if (!ptr) {
      return;
    }
    Header *header =
        reinterpret_cast<Header *>(static_cast<char *>(ptr) - sizeof(Header));
    if (header->res) {
      header->res->deallocate(header, sizeof(Header) + sizeof(Derived),
                              alignof(Derived));
    } else {
      ::operator delete(header);
    }
  }
};

template <typename T, typename... Args>
std::unique_ptr<T> MakePmrAllocated(std::pmr::memory_resource *res,
                                    Args &&...args) {
  return std::unique_ptr<T>(new (res) T(std::forward<Args>(args)...));
}

// C-style hooks for Rax radix tree
#ifdef __cplusplus
extern "C" {
#endif
extern void *RaxPmrMalloc(size_t size);
extern void *RaxPmrRealloc(void *ptr, size_t size);
extern void RaxPmrFree(void *ptr);
extern int RaxPmrUsableSize(void *ptr);
#ifdef __cplusplus
}
#endif

extern thread_local std::pmr::memory_resource *t_rax_res;

// RAII guard to route rax allocations to a specific memory_resource on the
// current thread.
class RaxPmrGuard {
 public:
  explicit RaxPmrGuard(std::pmr::memory_resource *res);
  ~RaxPmrGuard();

 private:
  std::pmr::memory_resource *prev_res_;
};

#define USE_CUSTOM_RAX_ALLOCATOR 1

#if defined(USE_CUSTOM_RAX_ALLOCATOR) && USE_CUSTOM_RAX_ALLOCATOR
#define RAX_PMR_GUARD(res) valkey_search::utils::RaxPmrGuard _rax_pmr_guard(res)
#else
#define RAX_PMR_GUARD(res) \
  do {                     \
  } while (0)
#endif

// Custom PMR memory resource wrapper that combines synchronized_pool_resource
// for block recycling and space reclaiming after free with an upstream
// monotonic_buffer_resource that starts at 10 KB chunk size and grows
// exponentially (doubles) on every memory allocation required from system
// malloc.
class TreePmrAllocator : public std::pmr::memory_resource {
 public:
  TreePmrAllocator() : monotonic_(10 * 1024), pool_(&monotonic_) {}

 protected:
  void *do_allocate(size_t bytes, size_t alignment) override {
    return pool_.allocate(bytes, alignment);
  }
  void do_deallocate(void *p, size_t bytes, size_t alignment) override {
    pool_.deallocate(p, bytes, alignment);
  }
  bool do_is_equal(
      const std::pmr::memory_resource &other) const noexcept override {
    return this == &other;
  }

 private:
  std::pmr::monotonic_buffer_resource monotonic_;
  std::pmr::synchronized_pool_resource pool_;
};

}  // namespace valkey_search::utils

#endif  // VALKEYSEARCH_SRC_UTILS_PMR_ALLOCATOR_H_
