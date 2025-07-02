/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_INTRUSIVE_REF_COUNT_H_
#define VALKEYSEARCH_SRC_UTILS_INTRUSIVE_REF_COUNT_H_
#include <atomic>
#include <memory>

#define DEFINE_UNIQUE_PTR_TYPE(Type) \
  using Unique##Type##Ptr = std::unique_ptr<Type, void (*)(Type*)>;

class IntrusiveRefCount {
 public:
  void IncrementRef() { ref_count_.fetch_add(1, std::memory_order_relaxed); }

  void DecrementRef() {
    if (ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      delete this;
    }
  }

  template <typename T, typename... Args>
  static std::unique_ptr<T, void (*)(T*)> Create(Args&&... args) {
    T* obj = new T(std::forward<Args>(args)...);
    obj->IncrementRef();  // Start with a ref count of 1

    // Return a unique_ptr with a custom deleter
    return std::unique_ptr<T, void (*)(T*)>(
        obj, [](T* ptr) { ptr->DecrementRef(); });
  }

 protected:
  virtual ~IntrusiveRefCount() = default;

 private:
  std::atomic<int> ref_count_ = 0;
};

#define CREATE_UNIQUE_PTR(Type, ...) \
  IntrusiveRefCount::Create<Type>(__VA_ARGS__)

#endif  // VALKEYSEARCH_SRC_UTILS_INTRUSIVE_REF_COUNT_H_
