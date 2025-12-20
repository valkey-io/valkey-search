/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/memory_pool.h"

#include <absl/log/check.h>
#include <execinfo.h>

#include <functional>

// clang-format off
// We put this at the end since it will otherwise mangle the malloc symbols in
// the dependencies.
#include "vmsdk/src/memory_allocation_overrides.h"

// RawSystemAllocator implements an allocator that will not go through
// the SystemAllocTracker, for use by the SystemAllocTracker to prevent
// infinite recursion when tracking pointers.
template <typename T>
struct RawSystemAllocator {
  // NOLINTNEXTLINE
  typedef T value_type;

  RawSystemAllocator() = default;
  template <typename U>
  constexpr RawSystemAllocator(const RawSystemAllocator<U>&) noexcept {}
  // NOLINTNEXTLINE
  T* allocate(std::size_t n) {
    return static_cast<T*>(__real_malloc(n * sizeof(T)));
  }
  // NOLINTNEXTLINE
  void deallocate(T* p, std::size_t) {
    __real_free(p);
  }
};

namespace valkey_search {

bool thread_local MemoryPool::CaptureEnabled = false;
bool MemoryPool::CaptureRequested = false;

void MemoryPool::NewChunk(size_t data_size) {
  auto this_chunk_size = std::max(data_size, chunk_size_);
  auto chunk =
      reinterpret_cast<Chunk*>(new char[this_chunk_size + sizeof(Chunk)]);
  chunk->size_ = this_chunk_size;
  chunk->leftoff_ = 0;
  allocated_ += this_chunk_size;
  chunks_.emplace_back(chunk);
}

MemoryPool::MemoryPool(size_t chunk_size) {
  // Externally chunks are sized to fit efficiently into slabs, so our
  // internal chunk size gets reduced accordingly.
  CHECK(chunk_size > sizeof(Chunk));
  chunk_size_ = chunk_size - sizeof(Chunk);
  NewChunk(0);
}

size_t ComputeBytes(size_t bytes, size_t alignment) {
  CHECK(alignment <= 16);  // Only currently supported value
  // Round up the bytes to the alignment,
  return (bytes + 15ul) & ~(15ul);
}

void* MemoryPool::do_allocate(size_t bytes, size_t alignment) {
  CHECK(!chunks_.empty());
  size_t this_bytes = ComputeBytes(bytes, alignment);
  Chunk* chunk = chunks_.back();
  if (chunk->leftoff_ + this_bytes > chunk->size_) {
    NewChunk(this_bytes);
    chunk = chunks_.back();
  }
  CHECK(chunk->leftoff_ + this_bytes <= chunk->size_);
  void* p = chunk->data_ + chunk->leftoff_;
  chunk->leftoff_ += this_bytes;
  inuse_ += this_bytes;
  return p;
}

void MemoryPool::do_deallocate(void* p, size_t bytes, size_t alignment) {
  //
  // Note, because of the way things get destructed, we might get called to deallocate
  // after the pool itself has been destroyed. So be care here....
  //
  alignment = std::max(alignment, 16ul);
  bytes = (bytes + alignment - 1) & ~(alignment - 1);
  CHECK(inuse_ >= bytes);
  inuse_ -= bytes;
}

MemoryPool::~MemoryPool() {
  while (!chunks_.empty()) {
    auto chunk = chunks_.back();
    allocated_ -= chunk->size_;
    chunks_.pop_back();
    delete[] reinterpret_cast<char*>(chunk);
  }
  CHECK(allocated_ == 0);
}

static absl::Mutex pool_debug_mutex;
static absl::flat_hash_map<vmsdk::Backtrace, size_t, std::hash<vmsdk::Backtrace>, std::equal_to<vmsdk::Backtrace>, RawSystemAllocator<std::pair<const vmsdk::Backtrace, size_t>>> backtraces;

//
// This is called out of the malloc chain.
// If pool debugging is enabled, then it captures the current call stack
//
void MemoryPool::DoCapture() {
  vmsdk::Backtrace backtrace;
  backtrace.Capture();
  absl::MutexLock lock(&pool_debug_mutex);
  auto itr = backtraces.find(backtrace);
  if (itr == backtraces.end()) {
    backtraces.emplace(std::move(backtrace), 1);
    return;
  } else {
    itr->second++;
  }
}

absl::Status MemoryPool::DebugCmd(ValkeyModuleCtx* ctx, vmsdk::ArgsIterator& itr) {
  std::string keyword;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, keyword));
  keyword = absl::AsciiStrToUpper(keyword);
  if (keyword == "ENABLE") {
    CaptureRequested = true;
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  } else if (keyword == "DISABLE") {
    CaptureRequested = false;
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  } else if (keyword == "RESET") {
    absl::MutexLock lock(&pool_debug_mutex);
    backtraces.clear();
  } else if (keyword == "DUMP") {
    absl::MutexLock lock(&pool_debug_mutex);
    std::multimap<size_t, const vmsdk::Backtrace *> sorted;
    for (auto& [backtrace, count] : backtraces) {
      sorted.insert(std::make_pair<size_t, const vmsdk::Backtrace *>(backtrace.stack_.size(), &backtrace));
    }
    ValkeyModule_ReplyWithArray(ctx, backtraces.size());
    for (const auto& [count, backtrace] : sorted) {
      auto symbols = backtrace->Symbolize();
      ValkeyModule_ReplyWithArray(ctx, backtrace->stack_.size() + 1);
      ValkeyModule_ReplyWithLongLong(ctx, count);
      for (int i = 0; i < backtrace->stack_.size(); i++) {
        ValkeyModule_ReplyWithSimpleString(ctx, symbols[i].data());
      }
    }
  } else {
    return absl::InvalidArgumentError(absl::StrCat(
        "Unknown subcommand: ", keyword));
  }
  return absl::OkStatus();
}


}
