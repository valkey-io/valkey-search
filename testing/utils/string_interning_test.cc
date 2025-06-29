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
#include <string>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/memory_tracker.h"
#include "src/utils/allocator.h"
#include "src/utils/intrusive_ref_count.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/memory_allocation_overrides.h"

namespace valkey_search {

using testing::TestParamInfo;

namespace {

class MockAllocator : public Allocator {
 public:
  explicit MockAllocator() = default;

  ~MockAllocator() override {
    if (buffer_) {
      free(buffer_);
    }
  }

  char* Allocate(size_t size) override {
    buffer_ = (char*)malloc(size);

    // simulate the memory allocation
    vmsdk::ReportAllocMemorySize(size);

    return buffer_;
  }

  size_t ChunkSize() const override {
    return 1024;
  }
  
protected:
  void Free(AllocatorChunk* chunk, char* ptr) override {
    free(ptr);
  }

 private:
  char* buffer_ = nullptr;
};

class StringInterningTest : public vmsdk::RedisTestWithParam<bool> {};

TEST_F(StringInterningTest, BasicTest) {
  EXPECT_EQ(StringInternStore::Instance().Size(), 0);
  {
    auto interned_key_1 = StringInternStore::Intern("key1");
    auto interned_key_2 = StringInternStore::Intern("key2");
    auto interned_key_2_1 = StringInternStore::Intern("key2");
    auto interned_key_3 = std::make_shared<InternedString>("key3");

    EXPECT_EQ(std::string(*interned_key_1), "key1");
    EXPECT_EQ(std::string(*interned_key_2), "key2");
    EXPECT_EQ(std::string(*interned_key_3), "key3");
    EXPECT_EQ(interned_key_2.get(), interned_key_2_1.get());
    EXPECT_EQ(StringInternStore::Instance().Size(), 2);
  }
  EXPECT_EQ(StringInternStore::Instance().Size(), 0);
}

TEST_P(StringInterningTest, WithAllocator) {
  bool require_ptr_alignment = GetParam();
  auto allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, strlen("prefix_key1") + 1, require_ptr_alignment);
  {
    EXPECT_EQ(StringInternStore::Instance().Size(), 0);
    EXPECT_EQ(allocator->ActiveAllocations(), 0);
    {
      auto interned_key_1 =
          StringInternStore::Intern("prefix_key1", allocator.get());
      EXPECT_EQ(allocator->ActiveAllocations(), 1);
      auto interned_key_2 =
          StringInternStore::Intern("prefix_key2", allocator.get());
      auto interned_key_2_1 = StringInternStore::Intern("prefix_key2");
      EXPECT_EQ(allocator->ActiveAllocations(), 2);
      auto interned_key_2_2 =
          StringInternStore::Intern("prefix_key2", allocator.get());
      EXPECT_EQ(allocator->ActiveAllocations(), 2);

      EXPECT_EQ(std::string(*interned_key_1), "prefix_key1");
      EXPECT_EQ(std::string(*interned_key_2), "prefix_key2");
      EXPECT_EQ(std::string(*interned_key_2_1), "prefix_key2");
      EXPECT_EQ(interned_key_2.get(), interned_key_2_1.get());
      EXPECT_EQ(StringInternStore::Instance().Size(), 2);
    }
    EXPECT_EQ(StringInternStore::Instance().Size(), 0);
    EXPECT_EQ(allocator->ActiveAllocations(), 0);
  }
}

INSTANTIATE_TEST_SUITE_P(StringInterningTests, StringInterningTest,
                         ::testing::Values(true, false),
                         [](const TestParamInfo<bool> &info) {
                           return std::to_string(info.param);
                         });

class StringInterningMemoryTest : public vmsdk::RedisTest {
 protected:
  void SetUp() override {
    vmsdk::RedisTest::SetUp();
    vmsdk::ResetValkeyAlloc();
  }

  void TearDown() override {
    vmsdk::RedisTest::TearDown();
    vmsdk::ResetValkeyAlloc();
  }
};

TEST_F(StringInterningMemoryTest, BasicUsageTracking) {
  const void* index = reinterpret_cast<const void*>(0x1000);

  EXPECT_EQ(StringInternStore::GetIndexUsage(index), 0);

  StringInternStore::RegisterIndexUsage(index, "test_key_1");
  StringInternStore::RegisterIndexUsage(index, "test_key_2");

  // "test_key_1" = 10 bytes + 1 (null) = 11
  // "test_key_2" = 10 bytes + 1 (null) = 11
  // Total = 22
  EXPECT_EQ(StringInternStore::GetIndexUsage(index), 22);

  // Register same string again - should not double count
  StringInternStore::RegisterIndexUsage(index, "test_key_1");
  EXPECT_EQ(StringInternStore::GetIndexUsage(index), 22);

  // Duplicate Unregister.
  StringInternStore::UnregisterIndexUsage(index, "test_key_1");
  EXPECT_EQ(StringInternStore::GetIndexUsage(index), 11);
  StringInternStore::UnregisterIndexUsage(index, "test_key_1");
  EXPECT_EQ(StringInternStore::GetIndexUsage(index), 11);

  // Clean up.
  StringInternStore::UnregisterIndexUsage(index, "test_key_2");
  EXPECT_EQ(StringInternStore::GetIndexUsage(index), 0);
}


TEST_F(StringInterningMemoryTest, MemoryTrackingIsolation) {
  std::atomic<int64_t> caller_pool{0};
  std::shared_ptr<InternedString> interned_str;

  MemoryTrackingScope scope{&caller_pool};
  auto allocator = std::make_unique<MockAllocator>();
  interned_str = StringInternStore::Intern("test_string", allocator.get());

  EXPECT_EQ(caller_pool.load(), 0);
  EXPECT_EQ(StringInternStore::GetMemoryUsage(), 12);

  interned_str.reset();
}

TEST_F(StringInterningMemoryTest, IsUsedByIndex) {
  const void* index = reinterpret_cast<const void*>(0x1000);

  StringInternStore::RegisterIndexUsage(index, "test_key_1");
  EXPECT_TRUE(StringInternStore::IsUsedByIndex(index, "test_key_1"));
  EXPECT_FALSE(StringInternStore::IsUsedByIndex(index, "test_key_2"));

  StringInternStore::UnregisterIndexUsage(index, "test_key_1");
}

TEST_F(StringInterningMemoryTest, EdgeCases) {
  const void* index1 = reinterpret_cast<const void*>(0x1000);

  // Empty string
  StringInternStore::RegisterIndexUsage(index1, "");
  EXPECT_EQ(StringInternStore::GetIndexUsage(index1), 1);  // Just null terminator
  StringInternStore::UnregisterIndexUsage(index1, "");

  // Non-existent index
  const void* non_existent = reinterpret_cast<const void*>(0x9999);
  EXPECT_EQ(StringInternStore::GetIndexUsage(non_existent), 0);

  // Unregister non-existent string
  StringInternStore::UnregisterIndexUsage(index1, "non_existent");
  EXPECT_EQ(StringInternStore::GetIndexUsage(index1), 0);
}

}  // namespace

}  // namespace valkey_search
