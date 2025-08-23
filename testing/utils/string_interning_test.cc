/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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
  explicit MockAllocator() : chunk_(this, 1024) {}

  ~MockAllocator() override = default;

  char* Allocate(size_t size) override {
    // simulate the memory allocation in the current tracking scope
    vmsdk::ReportAllocMemorySize(size);
    
    if (!chunk_.free_list.empty()) {
      auto ptr = chunk_.free_list.top();
      chunk_.free_list.pop();
      allocated_size_ = size;
      return ptr;
    }
    return nullptr;  // Out of memory
  }

  size_t ChunkSize() const override {
    return 1024;
  }
  
protected:
  void Free(AllocatorChunk* chunk, char* ptr) override {
    // Report memory deallocation to balance the allocation
    vmsdk::ReportFreeMemorySize(allocated_size_);
    
    chunk->free_list.push(ptr);
  }

 private:
  AllocatorChunk chunk_;
  size_t allocated_size_ = 0;
};

class StringInterningTest : public vmsdk::ValkeyTestWithParam<bool> {};

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

TEST_F(StringInterningTest, StringInternStoreTracksMemoryInternally) {
  MemoryPool caller_pool{0};
  std::shared_ptr<InternedString> interned_str;
  auto allocator = std::make_unique<MockAllocator>();

  {
    NestedMemoryScope scope{caller_pool};
    interned_str = StringInternStore::Intern("test_string", allocator.get());
  }

  EXPECT_EQ(caller_pool.GetUsage(), 0);
  EXPECT_EQ(StringInternStore::GetMemoryUsage(), 12);

  interned_str.reset();

  EXPECT_EQ(StringInternStore::GetMemoryUsage(), 0);
}

TEST_F(StringInterningTest, NonInternedStringDoesAffectStoreMemory) {
  static auto track_malloc_size = [](void* ptr) -> size_t {
    return 21;
  };
  
  vmsdk::test_utils::SetTestSystemMallocSizeFunction(track_malloc_size);
  
  const char* test_str = "non_interned_string";
  size_t expected_size = strlen(test_str) + 1;

  int64_t initial_memory = StringInternStore::GetMemoryUsage();
  
  auto non_interned = std::make_shared<InternedString>(test_str);
  
  EXPECT_EQ(StringInternStore::GetMemoryUsage(), initial_memory+21);
  
  non_interned.reset();
  
  EXPECT_EQ(StringInternStore::GetMemoryUsage(), initial_memory);
  
  vmsdk::test_utils::ClearTestSystemMallocSizeFunction();
}

INSTANTIATE_TEST_SUITE_P(StringInterningTests, StringInterningTest,
                         ::testing::Values(true, false),
                         [](const TestParamInfo<bool> &info) {
                           return std::to_string(info.param);
                         });
}  // namespace

}  // namespace valkey_search
