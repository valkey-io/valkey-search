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

InternedString::InternedString(absl::string_view str, bool shared, StringType string_type)
    : length_(str.length()), use_count_(INIT_USE_COUNT), string_type_(string_type), 
      is_shared_(shared), is_data_owner_(true) {
  data_ = new char[length_ + 1];
  memcpy(data_, str.data(), length_);
  data_[length_] = '\0';
  StringInternStore::Instance().UpdateCounters(string_type_, length_, 1);
}

InternedString::InternedString(char* data, size_t length, StringType string_type)
    : data_(data), length_(length), use_count_(INIT_USE_COUNT), string_type_(string_type),
      is_shared_(true), is_data_owner_(false) {
  StringInternStore::Instance().UpdateCounters(string_type_, length_, 1);
}

InternedString::~InternedString() {
  if (is_shared_) {
    StringInternStore::Instance().Release(this);
  }
  if (is_data_owner_) {
    delete[] data_;
  } else {
    Allocator::Free(data_);
  }
  StringInternStore::Instance().UpdateCounters(string_type_, -length_, -1);

  if (string_type_ == StringType::VECTOR) {
    uint16_t use_count = use_count_.load(std::memory_order_relaxed);
    if (use_count == 0) {
      StringInternStore::Instance().marked_deleted_.Update(-length_, -1);
    }
  }
}

void InternedString::IncrementUseCount() {
  if (string_type_ != StringType::VECTOR) return;
  uint16_t old_count = use_count_.load(std::memory_order_relaxed);
    
  if (old_count == 0) {
    // Reuse after marked deleted - transitioning from 0 to 1
    use_count_.store(1, std::memory_order_relaxed);
    StringInternStore::Instance().marked_deleted_.Update(-length_, -1);
  } else if (old_count == INIT_USE_COUNT) {
    // First time ever using this string - transitioning from INIT_USE_COUNT to 1
    use_count_.store(1, std::memory_order_relaxed);
  } else {
    // Normal increment - just add 1
    use_count_.fetch_add(1, std::memory_order_relaxed);
  }
}

void InternedString::DecrementUseCount() {
  if (string_type_ != StringType::VECTOR) return;
  uint16_t old_count = use_count_.load(std::memory_order_relaxed);

  if (old_count == INIT_USE_COUNT ||old_count == 0) {
    return;
  }

  old_count = use_count_.fetch_sub(1, std::memory_order_relaxed);
  // If transitioning from 1 to 0, increment marked deleted counters.
  if (old_count == 1) {
    StringInternStore::Instance().marked_deleted_.Update(length_, 1);
  }
}

void StringInternStore::UpdateCounters(StringType string_type, int64_t memory_delta, int objects_delta) {
  const size_t index = static_cast<size_t>(string_type);
  if (index >= static_cast<size_t>(StringType::kStringTypeCount)) return;
  counters_[index].Update(memory_delta, objects_delta);
}

void StringInternStore::Release(InternedString* str) {
  absl::MutexLock lock(&mutex_);

  const size_t index = static_cast<size_t>(str->string_type_);
  if (index >= static_cast<size_t>(StringType::kStringTypeCount)) return;
  auto& pool = pools_[index];

  auto it = pool.find(*str);
  if (it == pool.end()) {
    return;
  }
  auto locked = it->second.lock();
  // During `StringIntern` destruction, a new `StringIntern` may be stored,
  // so we check if the `StringIntern` being released is the one currently
  // stored before removing it.
  if (!locked || locked.get() == str) {
    pool.erase(*str);
  }
}

std::shared_ptr<InternedString> StringInternStore::Intern(
    absl::string_view str, StringType string_type, Allocator* allocator) {
  return Instance().InternImpl(str, string_type, allocator);
}

bool StringInternStore::SetDeleteMark(const char* data, size_t length, bool mark_delete) {
  std::shared_ptr<InternedString> locked;
  {
    absl::MutexLock lock(&mutex_);

    // SetDeleteMark is only relevant for vectors
    const size_t index = static_cast<size_t>(StringType::VECTOR);
    auto& pool = pools_[index];

    absl::string_view str(data, length);
    auto it = pool.find(str);
    if (it == pool.end()) {
      return false;
    }
    locked = it->second.lock();
    if (!locked) {
      return false;
    }

    if (mark_delete) {
      locked->DecrementUseCount();
    } else {
      locked->IncrementUseCount();
    }
  }
  return true;
}

std::shared_ptr<InternedString> StringInternStore::InternImpl(
    absl::string_view str, StringType string_type, Allocator* allocator) {
  absl::MutexLock lock(&mutex_);
  const size_t index = static_cast<size_t>(string_type);
  if (index >= static_cast<size_t>(StringType::kStringTypeCount)) {
    return nullptr;
  }

  auto& pool = pools_[index];
  auto it = pool.find(str);
  if (it != pool.end()) {
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
        std::shared_ptr<InternedString>(new InternedString(buffer, str.size(), string_type));
  } else {
    interned_string =
        std::shared_ptr<InternedString>(new InternedString(str, true, string_type));
  }
  pool.insert({*interned_string, interned_string});
  return interned_string;
}

}  // namespace valkey_search
