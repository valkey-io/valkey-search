/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_INVASIVE_PTR_H_
#define VALKEY_SEARCH_INDEXES_TEXT_INVASIVE_PTR_H_

#include <atomic>
#include <cstdint>
#include <utility>

namespace valkey_search::indexes::text {

namespace detail {
  template <typename T>
  struct InvasivePtrStorage {
    template <typename... Args>
    explicit InvasivePtrStorage(Args&&... args)
        : data_(std::forward<Args>(args)...) {}

    std::atomic<uint32_t> refcount_ = 1;
    T data_;
  };
}

// Raw invasive pointer opaque alias
template <typename T>
using InvasivePtrRaw = detail::InvasivePtrStorage<T>*;

/**
 * @brief A memory-efficient shared pointer.
 *
 * InvasivePtr manages the lifetime of objects through atomic reference
 * counting, storing the reference count alongside the managed object.
 *
 * Thread-safety: Reference counting operations are atomic and thread-safe.
 * The managed object itself is not protected by this class.
 *
 * @tparam T The type of object to manage
 *
 * Example usage:
 * @code
 *   auto ptr = InvasivePtr<MyClass>::Make(arg1, arg2);
 *   InvasivePtr<MyClass> copy = ptr;  // Increments refcount
 *   ptr->method();                    // Access managed object
 * @endcode
 */
template <typename T>
class InvasivePtr {
 public:
  InvasivePtr() = default;

  InvasivePtr(std::nullptr_t) noexcept : ptr_(nullptr) {}

  // Factory constructor
  template <typename... Args>
  static InvasivePtr Make(Args&&... args) {
    InvasivePtr result;
    result.ptr_ = new detail::InvasivePtrStorage<T>(std::forward<Args>(args)...);
    return result;
  }

  ~InvasivePtr() { ReleaseRef(); }

  // Copy semantics
  InvasivePtr(const InvasivePtr& other) : ptr_(other.ptr_) { AddRef(); }

  InvasivePtr& operator=(const InvasivePtr& other) {
    if (this != &other) {
      ReleaseRef();
      ptr_ = other.ptr_;
      AddRef();
    }
    return *this;
  }

  InvasivePtr& operator=(std::nullptr_t) noexcept {
    Clear();
    return *this;
  }

  // Move semantics
  InvasivePtr(InvasivePtr&& other) noexcept : ptr_(other.ptr_) {
    other.ptr_ = nullptr;
  }

  InvasivePtr& operator=(InvasivePtr&& other) noexcept {
    if (this != &other) {
      ReleaseRef();
      ptr_ = other.ptr_;
      other.ptr_ = nullptr;
    }
    return *this;
  }

  // Raw move/copy semantics

  // Transfers ownership to caller without decrementing refcount.
  // Caller must reconstruct via AdoptRaw() to restore memory management.
  // Freeing the memory directly is very dangerous - you must be certain there
  // are no other references.
  InvasivePtrRaw<T> ReleaseRaw() && {
    InvasivePtrRaw<T> result = ptr_;
    ptr_ = nullptr;
    return result;
  }

  // Every ReleaseRaw() should be paired with a corresponding AdoptRaw() later to
  // restore safe memory management.
  static InvasivePtr AdoptRaw(InvasivePtrRaw<T> raw_ptr) {
    return InvasivePtr(raw_ptr);
  }

  // Creates a new shared reference from a raw pointer, incrementing the
  // reference count. Use this when copying from void* storage (like Rax tree
  // targets) where you need a new managed reference.
  static InvasivePtr CopyRaw(InvasivePtrRaw<T> raw_ptr) {
    if (!raw_ptr) {
      return InvasivePtr{};
    }
    InvasivePtr result;
    result.ptr_ = raw_ptr;
    result.AddRef();
    return result;
  }

  // Access operators
  T& operator*() const { return ptr_->data_; }
  T* operator->() const { return &ptr_->data_; }

  // Boolean conversion
  explicit operator bool() const { return ptr_ != nullptr; }

  // Comparison operators
  auto operator<=>(const InvasivePtr&) const = default;

  // Resets to the default nullptr state
  void Clear() {
    ReleaseRef();
    ptr_ = nullptr;
  }

 private:
  explicit InvasivePtr(InvasivePtrRaw<T> raw) : ptr_(raw) {}

  void ReleaseRef() {
    if (ptr_ && ptr_->refcount_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      delete ptr_;
    }
  }

  void AddRef() {
    if (ptr_) {
      ptr_->refcount_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  detail::InvasivePtrStorage<T>* ptr_ = nullptr;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_INVASIVE_PTR_H_
