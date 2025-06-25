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

#include "src/utils/string_interning.h"

#include <cstring>
#include <memory>

#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/utils/allocator.h"
#include "vmsdk/src/memory_tracker.h"

namespace valkey_search {

std::atomic<int64_t> StringInternStore::memory_pool_{0};
absl::flat_hash_map<const void*, std::set<std::string>> StringInternStore::index_usage_map_{};
absl::flat_hash_map<const void*, int64_t> StringInternStore::index_usage_cache_{};
absl::Mutex StringInternStore::usage_mutex_;

InternedString::InternedString(absl::string_view str, bool shared)
    : length_(str.length()), is_shared_(shared), is_data_owner_(true) {
  data_ = new char[length_ + 1];
  memcpy(data_, str.data(), length_);
  data_[length_] = '\0';
}

InternedString::InternedString(char* data, size_t length)
    : data_(data), length_(length), is_shared_(true), is_data_owner_(false) {}

InternedString::~InternedString() {
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
  absl::MutexLock lock(&mutex_);
  auto it = str_to_interned_.find(str);
  if (it != str_to_interned_.end()) {
    if (auto locked = it->second.lock()) {
      return locked;
    }
  }

  // NOTE: isolate memory tracking.
  MemoryTrackingScope scope {&memory_pool_};

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

void StringInternStore::RegisterIndexUsage(const void* index_id, absl::string_view str) {
  absl::MutexLock lock(&usage_mutex_);
  
  bool is_new = index_usage_map_[index_id].insert(std::string(str)).second;
  
  if (is_new) {
    int64_t string_size = str.size() + 1;
    index_usage_cache_[index_id] += string_size;
  }
}

void StringInternStore::UnregisterIndexUsage(const void* index_id, absl::string_view str) {
  absl::MutexLock lock(&usage_mutex_);
  auto it = index_usage_map_.find(index_id);
  if (it != index_usage_map_.end()) {
    
    bool was_removed = it->second.erase(std::string(str)) > 0;
    
    if (was_removed) {
      int64_t string_size = str.size() + 1;
      index_usage_cache_[index_id] -= string_size;
    }
    
    if (it->second.empty()) {
      index_usage_map_.erase(it);
      index_usage_cache_.erase(index_id);
    }
  }
}

int64_t StringInternStore::GetIndexUsage(const void* index_id) {
  absl::MutexLock lock(&usage_mutex_);
  auto it = index_usage_cache_.find(index_id);
  return (it != index_usage_cache_.end()) ? it->second : 0;
}

int64_t StringInternStore::GetMemoryUsage() {
  return memory_pool_.load();
}

}  // namespace valkey_search
