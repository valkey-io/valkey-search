/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/rax/rax.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <utility>

extern "C" {
// NOLINTNEXTLINE(readability-identifier-naming)
void *__wrap_malloc(size_t size);
// NOLINTNEXTLINE(readability-identifier-naming)
void __wrap_free(void *ptr);
// NOLINTNEXTLINE(readability-identifier-naming)
void *__wrap_realloc(void *ptr, size_t size);
// NOLINTNEXTLINE(readability-identifier-naming)
int __wrap_malloc_usable_size(void *ptr);
}

namespace valkey_search {

namespace {
thread_local RaxTree *t_active_rax = nullptr;

constexpr uint64_t kRaxMagic = 0x5241585F54524545ULL;  // "RAX_TREE"

struct alignas(16) RaxHeader {
  uint64_t magic;
  size_t size;
};

struct FreeBlock {
  FreeBlock *next;
};

class ActiveRaxGuard {
 public:
  explicit ActiveRaxGuard(RaxTree *rax) : prev_(t_active_rax) {
    t_active_rax = rax;
  }
  ~ActiveRaxGuard() { t_active_rax = prev_; }

 private:
  RaxTree *prev_;
};
}  // namespace

size_t RaxTree::GetBucketIndex(size_t size) {
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

size_t RaxTree::GetBucketSize(size_t bucket_idx) {
  return static_cast<size_t>(32) << bucket_idx;
}

RaxTree::RaxTree(std::pmr::memory_resource *res) : res_(res) {
  ActiveRaxGuard guard(this);
  rax_ = vs_raxNew();
}

RaxTree::~RaxTree() {
  if (rax_) {
    ActiveRaxGuard guard(this);
    vs_raxFree(rax_);
  }
  if (res_) {
    for (int i = 0; i < kNumBuckets; ++i) {
      size_t block_size = GetBucketSize(i);
      FreeBlock *curr = static_cast<FreeBlock *>(free_lists_[i]);
      while (curr != nullptr) {
        FreeBlock *next = curr->next;
        res_->deallocate(curr, block_size, alignof(std::max_align_t));
        curr = next;
      }
      free_lists_[i] = nullptr;
      free_counts_[i] = 0;
    }
  }
}

RaxTree::RaxTree(RaxTree &&other) noexcept
    : rax_(other.rax_), res_(other.res_) {
  other.rax_ = nullptr;
  for (int i = 0; i < kNumBuckets; ++i) {
    free_lists_[i] = other.free_lists_[i];
    free_counts_[i] = other.free_counts_[i];
    other.free_lists_[i] = nullptr;
    other.free_counts_[i] = 0;
  }
}

RaxTree &RaxTree::operator=(RaxTree &&other) noexcept {
  if (this != &other) {
    if (rax_) {
      ActiveRaxGuard guard(this);
      vs_raxFree(rax_);
    }
    if (res_) {
      for (int i = 0; i < kNumBuckets; ++i) {
        size_t block_size = GetBucketSize(i);
        FreeBlock *curr = static_cast<FreeBlock *>(free_lists_[i]);
        while (curr != nullptr) {
          FreeBlock *next = curr->next;
          res_->deallocate(curr, block_size, alignof(std::max_align_t));
          curr = next;
        }
        free_lists_[i] = nullptr;
        free_counts_[i] = 0;
      }
    }
    rax_ = other.rax_;
    res_ = other.res_;
    other.rax_ = nullptr;
    for (int i = 0; i < kNumBuckets; ++i) {
      free_lists_[i] = other.free_lists_[i];
      free_counts_[i] = other.free_counts_[i];
      other.free_lists_[i] = nullptr;
      other.free_counts_[i] = 0;
    }
  }
  return *this;
}

void RaxTree::FreeWithCallback(void (*free_callback)(void *)) {
  if (rax_) {
    ActiveRaxGuard guard(this);
    vs_raxFreeWithCallback(rax_, free_callback);
    rax_ = nullptr;
  }
}

void *RaxTree::AllocateNode(size_t size) {
  if (!res_) {
    return __wrap_malloc(size);
  }
  size_t total_size = size + sizeof(RaxHeader);
  void *mem = nullptr;
  if (total_size <= 4096) {
    size_t bucket_idx = GetBucketIndex(total_size);
    size_t alloc_size = GetBucketSize(bucket_idx);
    if (free_lists_[bucket_idx] != nullptr) {
      FreeBlock *fn = static_cast<FreeBlock *>(free_lists_[bucket_idx]);
      free_lists_[bucket_idx] = fn->next;
      --free_counts_[bucket_idx];
      mem = fn;
    } else {
      mem = res_->allocate(alloc_size, alignof(std::max_align_t));
    }
  } else {
    mem = res_->allocate(total_size, alignof(std::max_align_t));
  }
  RaxHeader *hdr = reinterpret_cast<RaxHeader *>(mem);
  hdr->magic = kRaxMagic;
  hdr->size = size;
  return reinterpret_cast<char *>(mem) + sizeof(RaxHeader);
}

void RaxTree::FreeNode(void *ptr) {
  if (!ptr) {
    return;
  }
  if (!res_) {
    __wrap_free(ptr);
    return;
  }
  RaxHeader *hdr = reinterpret_cast<RaxHeader *>(reinterpret_cast<char *>(ptr) -
                                                 sizeof(RaxHeader));
  if (hdr->magic != kRaxMagic) {
    __wrap_free(ptr);
    return;
  }
  size_t total_size = hdr->size + sizeof(RaxHeader);
  if (total_size <= 4096) {
    size_t bucket_idx = GetBucketIndex(total_size);
    if (free_counts_[bucket_idx] < 64) {
      FreeBlock *fn = reinterpret_cast<FreeBlock *>(hdr);
      fn->next = static_cast<FreeBlock *>(free_lists_[bucket_idx]);
      free_lists_[bucket_idx] = fn;
      ++free_counts_[bucket_idx];
      return;
    }
    res_->deallocate(hdr, GetBucketSize(bucket_idx), alignof(std::max_align_t));
  } else {
    res_->deallocate(hdr, total_size, alignof(std::max_align_t));
  }
}

void *RaxTree::ReallocateNode(void *ptr, size_t new_size) {
  if (!ptr) {
    return AllocateNode(new_size);
  }
  if (new_size == 0) {
    FreeNode(ptr);
    return nullptr;
  }
  if (!res_) {
    return __wrap_realloc(ptr, new_size);
  }
  RaxHeader *hdr = reinterpret_cast<RaxHeader *>(reinterpret_cast<char *>(ptr) -
                                                 sizeof(RaxHeader));
  if (hdr->magic != kRaxMagic) {
    void *new_ptr = AllocateNode(new_size);
    std::memcpy(new_ptr, ptr, new_size);
    return new_ptr;
  }
  size_t old_size = hdr->size;
  size_t old_total = old_size + sizeof(RaxHeader);
  size_t new_total = new_size + sizeof(RaxHeader);
  if (old_total <= 4096 && new_total <= 4096) {
    size_t old_bucket = GetBucketIndex(old_total);
    size_t new_bucket = GetBucketIndex(new_total);
    if (old_bucket == new_bucket && new_total <= GetBucketSize(old_bucket)) {
      hdr->size = new_size;
      return ptr;
    }
  }
  void *new_ptr = AllocateNode(new_size);
  std::memcpy(new_ptr, ptr, std::min(old_size, new_size));
  FreeNode(ptr);
  return new_ptr;
}

int RaxTree::UsableSize(void *ptr) {
  if (!ptr) {
    return 0;
  }
  if (!res_) {
    return __wrap_malloc_usable_size(ptr);
  }
  RaxHeader *hdr = reinterpret_cast<RaxHeader *>(reinterpret_cast<char *>(ptr) -
                                                 sizeof(RaxHeader));
  if (hdr->magic == kRaxMagic) {
    return static_cast<int>(hdr->size);
  }
  return 0;
}

int RaxTree::Insert(std::string_view key, void *data, void **old_data) {
  ActiveRaxGuard guard(this);
  return vs_raxInsert(
      rax_, reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
      key.size(), data, old_data);
}

int RaxTree::Remove(std::string_view key, void **old_data) {
  ActiveRaxGuard guard(this);
  return vs_raxRemove(
      rax_, reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
      key.size(), old_data);
}

void *RaxTree::Find(std::string_view key) const {
  ActiveRaxGuard guard(const_cast<RaxTree *>(this));
  void *val = nullptr;
  vs_raxFind(rax_,
             reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
             key.size(), &val);
  return val;
}

int RaxTree::Mutate(std::string_view key, vs_raxMutateCallback mutate,
                    void *caller_context, item_count_op op) {
  ActiveRaxGuard guard(this);
  return vs_raxMutate(
      rax_, reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
      key.size(), mutate, caller_context, op);
}

size_t RaxTree::GetSubtreeItemCount(std::string_view prefix) const {
  ActiveRaxGuard guard(const_cast<RaxTree *>(this));
  return vs_raxGetSubtreeItemCount(
      rax_,
      reinterpret_cast<unsigned char *>(const_cast<char *>(prefix.data())),
      prefix.size());
}

uint64_t RaxTree::Size() const { return rax_ ? rax_->numele : 0; }

size_t RaxTree::GetAllocSize() const { return rax_ ? rax_->alloc_size : 0; }

extern "C" {
bool IsRaxActive() { return t_active_rax != nullptr; }
void *RaxMalloc(size_t size) {
  return t_active_rax ? t_active_rax->AllocateNode(size) : nullptr;
}
void RaxFree(void *ptr) {
  if (!ptr) {
    return;
  }
  if (t_active_rax) {
    t_active_rax->FreeNode(ptr);
    return;
  }
  RaxHeader *hdr = reinterpret_cast<RaxHeader *>(reinterpret_cast<char *>(ptr) -
                                                 sizeof(RaxHeader));
  if (hdr->magic == kRaxMagic) {
    size_t total_size = hdr->size + sizeof(RaxHeader);
    std::pmr::get_default_resource()->deallocate(
        hdr,
        total_size <= 4096
            ? RaxTree::GetBucketSize(RaxTree::GetBucketIndex(total_size))
            : total_size,
        alignof(std::max_align_t));
  } else {
    __wrap_free(ptr);
  }
}
void *RaxRealloc(void *ptr, size_t size) {
  return t_active_rax ? t_active_rax->ReallocateNode(ptr, size) : nullptr;
}
int RaxUsableSize(void *ptr) {
  return t_active_rax ? t_active_rax->UsableSize(ptr) : 0;
}
}

}  // namespace valkey_search
