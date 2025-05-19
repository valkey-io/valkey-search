/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
