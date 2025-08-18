/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/string_interning.h"

#include <cstring>
#include <memory>

#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/utils/allocator.h"
#include "vmsdk/src/memory_tracker.h"
#include "vmsdk/src/memory_allocation_overrides.h"

namespace valkey_search {

MemoryPool StringInternStore::memory_pool_{0};

InternedString::InternedString(absl::string_view str, bool shared)
    : length_(str.length()), is_shared_(shared), is_data_owner_(true) {
  IsolatedMemoryScope scope { StringInternStore::memory_pool_ };

  data_ = new char[length_ + 1];
  memcpy(data_, str.data(), length_);
  data_[length_] = '\0';
}

InternedString::InternedString(char* data, size_t length)
    : data_(data), length_(length), is_shared_(true), is_data_owner_(false) {}

InternedString::~InternedString() {
  // NOTE: isolate memory tracking for deallocation.
  IsolatedMemoryScope scope {StringInternStore::memory_pool_};

  if (is_shared_) {
    StringInternStore::Instance().Release(this);
  }

  if (is_data_owner_) {
    delete[] data_;
  } else {
    Allocator::Free(data_);
  }
}

void StringInternStore::Release(InternedString* str) {
  absl::MutexLock lock(&mutex_);
  auto it = str_to_interned_.find(*str);
  if (it == str_to_interned_.end()) {
    return;
  }
  auto locked = it->second.lock();
  // During `StringIntern` destruction, a new `StringIntern` may be stored,
  // so we check if the `StringIntern` being released is the one currently
  // stored before removing it.
  if (!locked || locked.get() == str) {
    str_to_interned_.erase(*str);
  }
}

std::shared_ptr<InternedString> StringInternStore::Intern(
    absl::string_view str, Allocator* allocator) {
  return Instance().InternImpl(str, allocator);
}

std::shared_ptr<InternedString> StringInternStore::InternImpl(
    absl::string_view str, Allocator* allocator) {
  IsolatedMemoryScope scope {memory_pool_};

  absl::MutexLock lock(&mutex_);
  auto it = str_to_interned_.find(str);
  if (it != str_to_interned_.end()) {
    if (auto locked = it->second.lock()) {
      return locked;
    }
  }

  std::shared_ptr<InternedString> interned_string;
  if (allocator) {
    auto buffer = allocator->Allocate(str.size() + 1);
    memcpy(buffer, str.data(), str.size());
    buffer[str.size()] = '\0';
    interned_string =
        std::shared_ptr<InternedString>(new InternedString(buffer, str.size()));
  } else {
    interned_string =
        std::shared_ptr<InternedString>(new InternedString(str, true));
  }
  str_to_interned_.insert({*interned_string, interned_string});
  return interned_string;
}

int64_t StringInternStore::GetMemoryUsage() {
  return memory_pool_.GetUsage();
}

}  // namespace valkey_search
