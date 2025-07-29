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
#include "src/indexes/global_metrics.h"

namespace valkey_search {

InternedString::InternedString(absl::string_view str, bool shared, indexes::MetricType metric_type)
    : length_(str.length()), is_shared_(shared), is_data_owner_(true), metadata_flags_(0) {
  data_ = new char[length_ + 1];
  memcpy(data_, str.data(), length_);
  data_[length_] = '\0';
  indexes::OnInternedStringAlloc(this, metric_type);
}

InternedString::InternedString(char* data, size_t length, indexes::MetricType metric_type)
    : data_(data), length_(length), is_shared_(true), is_data_owner_(false), metadata_flags_(0) {
  indexes::OnInternedStringAlloc(this, metric_type);
}

InternedString::~InternedString() {
  if (is_shared_) {
    StringInternStore::Instance().Release(this);
  }
  indexes::OnInternedStringDealloc(this);
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
    absl::string_view str, Allocator* allocator, indexes::MetricType metric_type) {
  return Instance().InternImpl(str, allocator, metric_type);
}

bool StringInternStore::MarkDelete(absl::string_view str) {
  absl::MutexLock lock(&mutex_);
  auto it = str_to_interned_.find(str);
  if (it == str_to_interned_.end()) {
    return false;
  }
  auto locked = it->second.lock();
  if (!locked) {
    return false;
  }

  return indexes::OnInternedStringMarkUnused(locked);
}

bool StringInternStore::UnmarkDelete(absl::string_view str) {
  absl::MutexLock lock(&mutex_);
  auto it = str_to_interned_.find(str);
  if (it == str_to_interned_.end()) {
    return false;
  }
  auto locked = it->second.lock();
  if (!locked) {
    return false;
  }
  
  return indexes::OnInternedStringIncrUsed(locked);
}

std::shared_ptr<InternedString> StringInternStore::InternImpl(
    absl::string_view str, Allocator* allocator, indexes::MetricType metric_type) {
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
        std::shared_ptr<InternedString>(new InternedString(buffer, str.size(), metric_type));
  } else {
    interned_string =
        std::shared_ptr<InternedString>(new InternedString(str, true, metric_type));
  }
  str_to_interned_.insert({*interned_string, interned_string});
  return interned_string;
}

}  // namespace valkey_search
