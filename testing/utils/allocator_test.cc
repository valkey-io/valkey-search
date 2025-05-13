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

#include "src/utils/allocator.h"

#include <cstddef>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"
#include "src/utils/intrusive_ref_count.h"
#include "vmsdk/src/testing_infra/utils.h"

#ifndef ASAN_BUILD
namespace valkey_search {

namespace {

class AllocatorTest : public vmsdk::RedisTestWithParam<bool> {};

TEST_P(AllocatorTest, BasicFixedSizeAllocator) {
  const size_t size = 11;
  auto memory_alignment = GetParam();

  auto allocator =
      CREATE_UNIQUE_PTR(FixedSizeAllocator, size, memory_alignment);
  EXPECT_EQ(allocator->ActiveAllocations(), 0);
  EXPECT_EQ(allocator->ChunkCount(), 0);
  char *ptr = allocator->Allocate(size);
  EXPECT_EQ(allocator->ActiveAllocations(), 1);
  EXPECT_EQ(allocator->ChunkCount(), 1);
  Allocator::Free(ptr);
  EXPECT_EQ(allocator->ActiveAllocations(), 0);
  EXPECT_EQ(allocator->ChunkCount(), 0);
  EXPECT_EQ(EntriesFitInChunk(size, kChunkBufferPages), 3723);
}

TEST_P(AllocatorTest, FixedSizeAllocatorMultipleChunks) {
  const size_t size = 128;
  const size_t chunks = 3;
  auto memory_alignment = GetParam();

  auto allocator =
      CREATE_UNIQUE_PTR(FixedSizeAllocator, size, memory_alignment);
  {
    auto entries_fit_in_chunk = EntriesFitInChunk(size, kChunkBufferPages);
    std::vector<char *> buffers;
    buffers.reserve(chunks * entries_fit_in_chunk);
    for (size_t i = 0; i < chunks * entries_fit_in_chunk; ++i) {
      buffers.push_back(allocator->Allocate(size));
    }
    EXPECT_EQ(allocator->ChunkCount(), chunks);
    EXPECT_EQ(allocator->ActiveAllocations(), chunks * entries_fit_in_chunk);
    for (auto &buffer : buffers) {
      Allocator::Free(buffer);
    }
    EXPECT_EQ(allocator->ChunkCount(), 0);
    EXPECT_EQ(allocator->ActiveAllocations(), 0);
  }
}

TEST_P(AllocatorTest, FixedSizeAllocatorMultipleChunksWithFree) {
  const size_t size = 256;
  const size_t chunks = 3;
  auto memory_alignment = GetParam();

  auto allocator =
      CREATE_UNIQUE_PTR(FixedSizeAllocator, size, memory_alignment);
  {
    auto entries_fit_in_chunk = EntriesFitInChunk(size, kChunkBufferPages);
    std::vector<absl::flat_hash_set<char *>> buffers;
    buffers.reserve(chunks);
    for (size_t j = 0; j < chunks; ++j) {
      buffers.push_back(absl::flat_hash_set<char *>());
      for (size_t i = 0; i < entries_fit_in_chunk; ++i) {
        buffers[j].insert(allocator->Allocate(size));
      }
    }
    auto ptr = const_cast<char *>(*buffers[chunks - 1].begin());
    Allocator::Free(ptr);
    EXPECT_EQ(ptr, allocator->Allocate(size));
    EXPECT_EQ(allocator->ChunkCount(), chunks);
    EXPECT_EQ(allocator->ActiveAllocations(), chunks * entries_fit_in_chunk);
    for (auto &chunk : buffers) {
      for (auto &buffer : chunk) {
        Allocator::Free(buffer);
      }
    }
    EXPECT_EQ(allocator->ChunkCount(), 0);
    EXPECT_EQ(allocator->ActiveAllocations(), 0);
  }
}

size_t FreeBuffer(std::vector<absl::flat_hash_set<char *>> &buffers, int chunk,
                  size_t cnt) {
  auto it = buffers[chunk].begin();
  size_t freed = 0;
  for (size_t i = 0; i < cnt; ++i) {
    auto ptr = const_cast<char *>(*it);
    Allocator::Free(ptr);
    it++;
    freed++;
  }
  return freed;
}

void VerifyAllocationChunk(FixedSizeAllocator &allocator,
                           std::vector<absl::flat_hash_set<char *>> &buffers,
                           int chunk, size_t cnt) {
  for (size_t i = 0; i < cnt; ++i) {
    auto ptr = allocator.Allocate();
    EXPECT_TRUE(buffers[chunk].contains(ptr));
  }
}

TEST_P(AllocatorTest, FixedSizeAllocatorMultipleChunksWithDeleteChunks) {
  const size_t size = 256;
  const size_t chunks = 4;
  auto memory_alignment = GetParam();
  auto allocator =
      CREATE_UNIQUE_PTR(FixedSizeAllocator, size, memory_alignment);
  {
    auto entries_fit_in_chunk = EntriesFitInChunk(size, kChunkBufferPages);
    std::vector<absl::flat_hash_set<char *>> buffers;
    buffers.reserve(chunks);
    for (size_t j = 0; j < chunks; ++j) {
      buffers.push_back(absl::flat_hash_set<char *>());
      for (size_t i = 0; i < entries_fit_in_chunk; ++i) {
        buffers[j].insert(allocator->Allocate(size));
      }
    }
    EXPECT_EQ(allocator->ChunkCount(), chunks);
    EXPECT_EQ(allocator->ActiveAllocations(), chunks * entries_fit_in_chunk);
    size_t freed = FreeBuffer(buffers, 0, 3);
    freed += FreeBuffer(buffers, 2, entries_fit_in_chunk);
    freed += FreeBuffer(buffers, 1, 2);
    freed += FreeBuffer(buffers, 3, entries_fit_in_chunk);

    EXPECT_EQ(allocator->ChunkCount(), chunks - 2);
    EXPECT_EQ(allocator->ActiveAllocations(),
              chunks * entries_fit_in_chunk - freed);

    VerifyAllocationChunk(*allocator, buffers, 1, 2);
    VerifyAllocationChunk(*allocator, buffers, 0, 3);

    EXPECT_EQ(allocator->ChunkCount(), chunks - 2);
    EXPECT_EQ(allocator->ActiveAllocations(),
              (chunks - 2) * entries_fit_in_chunk);
    EXPECT_NE(allocator->Allocate(size), nullptr);
    EXPECT_EQ(allocator->ChunkCount(), chunks - 1);
    EXPECT_EQ(allocator->ActiveAllocations(),
              (chunks - 2) * entries_fit_in_chunk + 1);
    for (auto &chunk : buffers) {
      for (auto &buffer : chunk) {
        Allocator::Free(buffer);
      }
    }
    EXPECT_EQ(allocator->ChunkCount(), 0);
    EXPECT_EQ(allocator->ActiveAllocations(), 0);
  }
}

TEST_P(AllocatorTest, FixedSizeAllocatorMultipleChunksWithFreeEntries) {
  auto memory_alignment = GetParam();
  const size_t size = 256;
  const size_t chunks = 4;
  auto allocator =
      CREATE_UNIQUE_PTR(FixedSizeAllocator, size, memory_alignment);
  {
    auto entries_fit_in_chunk = EntriesFitInChunk(size, kChunkBufferPages);
    std::vector<absl::flat_hash_set<char *>> buffers;
    buffers.reserve(chunks);
    for (size_t j = 0; j < chunks; ++j) {
      buffers.push_back(absl::flat_hash_set<char *>());
      for (size_t i = 0; i < entries_fit_in_chunk; ++i) {
        buffers[j].insert(allocator->Allocate(size));
      }
    }
    EXPECT_EQ(allocator->ChunkCount(), chunks);
    EXPECT_EQ(allocator->ActiveAllocations(), chunks * entries_fit_in_chunk);
    size_t freed = FreeBuffer(buffers, 0, 3);
    freed += FreeBuffer(buffers, 2, 1);
    freed += FreeBuffer(buffers, 1, 2);
    freed += FreeBuffer(buffers, 3, 4);

    EXPECT_EQ(allocator->ChunkCount(), chunks);
    EXPECT_EQ(allocator->ActiveAllocations(),
              chunks * entries_fit_in_chunk - freed);

    VerifyAllocationChunk(*allocator, buffers, 2, 1);
    VerifyAllocationChunk(*allocator, buffers, 1, 2);
    VerifyAllocationChunk(*allocator, buffers, 0, 3);
    VerifyAllocationChunk(*allocator, buffers, 3, 4);

    EXPECT_EQ(allocator->ChunkCount(), chunks);
    EXPECT_EQ(allocator->ActiveAllocations(), chunks * entries_fit_in_chunk);
    for (auto &chunk : buffers) {
      for (auto &buffer : chunk) {
        Allocator::Free(buffer);
      }
    }
    EXPECT_EQ(allocator->ChunkCount(), 0);
    EXPECT_EQ(allocator->ActiveAllocations(), 0);
  }
}

INSTANTIATE_TEST_SUITE_P(AllocatorTests, AllocatorTest,
                         ::testing::Values(true, false),
                         [](const testing::TestParamInfo<bool> &info) {
                           return std::to_string(info.param);
                         });

}  // namespace

}  // namespace valkey_search
#endif
