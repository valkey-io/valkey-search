/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/rax/optimized_rax.h"

#include <algorithm>
#include <cstring>

namespace valkey_search {

namespace {
thread_local OptimizedRax *t_active_rax = nullptr;

constexpr uint64_t kOptRaxMagic = 0x4F50545F52415821ULL;

struct alignas(16) OptRaxHeader {
  uint64_t magic;
  size_t size;
};

class ActiveRaxGuard {
 public:
  explicit ActiveRaxGuard(OptimizedRax *rax) : prev_(t_active_rax) {
    t_active_rax = rax;
  }
  ~ActiveRaxGuard() { t_active_rax = prev_; }

 private:
  OptimizedRax *prev_;
};
}  // namespace

size_t OptimizedRax::GetBucketIndex(size_t size) {
  if (size <= 32) {
    return 0;
  }
  if (size <= 64) {
    return 1;
  }
  if (size <= 128) {
    return 2;
  }
  if (size <= 256) {
    return 3;
  }
  if (size <= 512) {
    return 4;
  }
  if (size <= 1024) {
    return 5;
  }
  if (size <= 2048) {
    return 6;
  }
  return 7;
}

size_t OptimizedRax::GetBucketSize(size_t bucket_idx) {
  return static_cast<size_t>(32) << bucket_idx;
}

OptimizedRax::OptimizedRax() {
  ActiveRaxGuard guard(this);
  rax_ = RaxNew();
}

OptimizedRax::~OptimizedRax() {
  if (rax_) {
    ActiveRaxGuard guard(this);
    RaxFree(rax_);
  }
  for (int i = 0; i < kNumBuckets; ++i) {
    for (void *ptr : free_lists_[i]) {
      valkey_search::GetDefaultSegregatedAllocator().Free(static_cast<char*>(ptr));
    }
    free_lists_[i].clear();
  }
}

void OptimizedRax::FreeWithCallback(void (*free_callback)(void *)) {
  if (rax_) {
    ActiveRaxGuard guard(this);
    RaxFreeWithCallback(rax_, free_callback);
    rax_ = nullptr;
  }
}

void *OptimizedRax::AllocateNode(size_t size) {
  size_t total_size = size + sizeof(OptRaxHeader);
  size_t bucket_idx = GetBucketIndex(total_size);
  size_t alloc_size = GetBucketSize(bucket_idx);
  void *mem = nullptr;
  if (!free_lists_[bucket_idx].empty()) {
    mem = free_lists_[bucket_idx].back();
    free_lists_[bucket_idx].pop_back();
  } else {
    mem = valkey_search::GetDefaultSegregatedAllocator().Allocate(alloc_size);
  }
  OptRaxHeader *hdr = reinterpret_cast<OptRaxHeader *>(mem);
  hdr->magic = kOptRaxMagic;
  hdr->size = size;
  return reinterpret_cast<char *>(mem) + sizeof(OptRaxHeader);
}

void OptimizedRax::FreeNode(void *ptr) {
  if (!ptr) {
    return;
  }
  OptRaxHeader *hdr = reinterpret_cast<OptRaxHeader *>(
      reinterpret_cast<char *>(ptr) - sizeof(OptRaxHeader));
  if (hdr->magic != kOptRaxMagic) {
    return;
  }
  size_t total_size = hdr->size + sizeof(OptRaxHeader);
  size_t bucket_idx = GetBucketIndex(total_size);
  if (free_lists_[bucket_idx].size() < 64) {
    free_lists_[bucket_idx].push_back(hdr);
    return;
  }
  valkey_search::GetDefaultSegregatedAllocator().Free(reinterpret_cast<char*>(hdr));
}

void *OptimizedRax::ReallocateNode(void *ptr, size_t new_size) {
  if (!ptr) {
    return AllocateNode(new_size);
  }
  if (new_size == 0) {
    FreeNode(ptr);
    return nullptr;
  }
  OptRaxHeader *hdr = reinterpret_cast<OptRaxHeader *>(
      reinterpret_cast<char *>(ptr) - sizeof(OptRaxHeader));
  if (hdr->magic != kOptRaxMagic) {
    return AllocateNode(new_size);
  }
  size_t old_size = hdr->size;
  size_t old_bucket = GetBucketIndex(old_size + sizeof(OptRaxHeader));
  size_t new_bucket = GetBucketIndex(new_size + sizeof(OptRaxHeader));
  // Optimization 1: Capacity Quantization - If new size still fits in the same
  // bucket capacity, return pointer immediately without reallocating or
  // copying!
  if (old_bucket == new_bucket &&
      (new_size + sizeof(OptRaxHeader)) <= GetBucketSize(old_bucket)) {
    hdr->size = new_size;
    return ptr;
  }
  void *new_ptr = AllocateNode(new_size);
  std::memcpy(new_ptr, ptr, std::min(old_size, new_size));
  FreeNode(ptr);
  return new_ptr;
}

int OptimizedRax::UsableSize(void *ptr) {
  if (!ptr) {
    return 0;
  }
  OptRaxHeader *hdr = reinterpret_cast<OptRaxHeader *>(
      reinterpret_cast<char *>(ptr) - sizeof(OptRaxHeader));
  if (hdr->magic == kOptRaxMagic) {
    return static_cast<int>(hdr->size);
  }
  return 0;
}

int OptimizedRax::Insert(std::string_view key, void *data, void **old_data) {
  ActiveRaxGuard guard(this);
  return RaxInsert(
      rax_, reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
      key.size(), data, old_data);
}

int OptimizedRax::Remove(std::string_view key, void **old_data) {
  ActiveRaxGuard guard(this);
  return RaxRemove(
      rax_, reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
      key.size(), old_data);
}

void *OptimizedRax::Find(std::string_view key) const {
  ActiveRaxGuard guard(const_cast<OptimizedRax *>(this));
  void *val = nullptr;
  RaxFind(rax_,
             reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
             key.size(), &val);
  return val;
}

int OptimizedRax::Mutate(std::string_view key, RaxMutateCallback mutate,
                         void *caller_context, item_count_op op) {
  ActiveRaxGuard guard(this);
  return RaxMutate(
      rax_, reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
      key.size(), mutate, caller_context, op);
}

size_t OptimizedRax::GetSubtreeItemCount(std::string_view prefix) const {
  ActiveRaxGuard guard(const_cast<OptimizedRax *>(this));
  return RaxGetSubtreeItemCount(
      rax_,
      reinterpret_cast<unsigned char *>(const_cast<char *>(prefix.data())),
      prefix.size());
}

uint64_t OptimizedRax::Size() const { return rax_ ? rax_->numele : 0; }

extern "C" {
bool IsOptRaxActive() { return t_active_rax != nullptr; }
void *OptRaxMalloc(size_t size) {
  return t_active_rax ? t_active_rax->AllocateNode(size) : nullptr;
}
void OptRaxFree(void *ptr) {
  if (t_active_rax) {
    t_active_rax->FreeNode(ptr);
  }
}
void *OptRaxRealloc(void *ptr, size_t size) {
  return t_active_rax ? t_active_rax->ReallocateNode(ptr, size) : nullptr;
}
int OptRaxUsableSize(void *ptr) {
  return t_active_rax ? t_active_rax->UsableSize(ptr) : 0;
}
}

}  // namespace valkey_search

