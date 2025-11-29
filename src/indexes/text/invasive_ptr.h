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
    result.ptr_ = new RefCountWrapper(std::forward<Args>(args)...);
    return result;
  }

  ~InvasivePtr() { Release(); }

  // Copy semantics
  InvasivePtr(const InvasivePtr& other) : ptr_(other.ptr_) { Acquire(); }

  InvasivePtr& operator=(const InvasivePtr& other) {
    if (this != &other) {
      Release();
      ptr_ = other.ptr_;
      Acquire();
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
      Release();
      ptr_ = other.ptr_;
      other.ptr_ = nullptr;
    }
    return *this;
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
    Release();
    ptr_ = nullptr;
  }

 private:
  struct RefCountWrapper {
    template <typename... Args>
    explicit RefCountWrapper(Args&&... args)
        : data_(std::forward<Args>(args)...) {}

    std::atomic<uint32_t> refcount_ = 1;
    T data_;
  };

  void Release() {
    if (ptr_ && ptr_->refcount_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      delete ptr_;
    }
  }

  void Acquire() {
    if (ptr_) {
      ptr_->refcount_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  RefCountWrapper* ptr_ = nullptr;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_INVASIVE_PTR_H_
