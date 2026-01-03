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
#include <thread>

#include "gtest/gtest.h"
#include "src/utils/allocator.h"
#include "src/utils/intrusive_ref_count.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/testing_infra/utils.h"

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

  size_t ChunkSize() const override { return 1024; }

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
  EXPECT_EQ(StringInternStore::Instance().UniqueStrings(), 0);
  {
    auto interned_key_1 = StringInternStore::Intern("key1");
    EXPECT_EQ(interned_key_1.RefCount(), 1);

    EXPECT_EQ(interned_key_1->Str(), "key1");
    EXPECT_EQ(StringInternStore::Instance().UniqueStrings(), 1);
    auto interned_key_2 = StringInternStore::Intern("key2");
    EXPECT_EQ(interned_key_2.RefCount(), 1);
    EXPECT_EQ(interned_key_2->Str(), "key2");
    EXPECT_EQ(StringInternStore::Instance().UniqueStrings(), 2);
    auto interned_key_2_1 = StringInternStore::Intern("key2");
    EXPECT_EQ(interned_key_2.RefCount(), 2);
    EXPECT_EQ(interned_key_2_1.RefCount(), 2);
    EXPECT_EQ(interned_key_2->Str().data(), interned_key_2_1->Str().data());
    EXPECT_EQ(interned_key_2, interned_key_2_1);
    EXPECT_EQ(StringInternStore::Instance().UniqueStrings(), 2);
  }
  EXPECT_EQ(StringInternStore::Instance().UniqueStrings(), 0);
}

TEST_P(StringInterningTest, WithAllocator) {
  bool require_ptr_alignment = GetParam();
  auto allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, strlen("prefix_key1") + 1, require_ptr_alignment);
  {
    EXPECT_EQ(StringInternStore::Instance().UniqueStrings(), 0);
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
      EXPECT_EQ(interned_key_2->Str().data(), interned_key_2_1->Str().data());
      EXPECT_EQ(StringInternStore::Instance().UniqueStrings(), 2);
    }
    EXPECT_EQ(StringInternStore::Instance().UniqueStrings(), 0);
    EXPECT_EQ(allocator->ActiveAllocations(), 0);
  }
}
/*
TEST_F(StringInterningTest, StringInternStoreTracksMemoryInternally) {
  MemoryPool caller_pool{0};
  InternedStringPtr interned_str;
  auto allocator = std::make_unique<MockAllocator>();

  {
    NestedMemoryScope scope{caller_pool};
    interned_str = StringInternStore::Intern("test_string", allocator.get());
  }

  EXPECT_EQ(caller_pool.GetUsage(), 0);
  EXPECT_EQ(StringInternStore::GetMemoryUsage(), 12);

  interned_str.reset();
}
*/
INSTANTIATE_TEST_SUITE_P(StringInterningTests, StringInterningTest,
                         ::testing::Values(true, false),
                         [](const TestParamInfo<bool>& info) {
                           return std::to_string(info.param);
                         });

class StringInterningMultithreadTest : public vmsdk::ValkeyTest {};

TEST_F(StringInterningMultithreadTest, Simple) {
  const std::string test_string = "concurrent_test_string";
  auto interned_str1 = StringInternStore::Intern(test_string);
  auto interned_str2 = StringInternStore::Intern(test_string);
  EXPECT_EQ(interned_str1.RefCount(), 2);
  interned_str1 = InternedStringPtr();
  EXPECT_EQ(interned_str2.RefCount(), 1);
  interned_str2 = InternedStringPtr();
  EXPECT_EQ(StringInternStore::Instance().UniqueStrings(), 0);
}

TEST_F(StringInterningMultithreadTest, ConcurrentInterning) {
  const int kNumThreads = 32;
  const int kNumIterations = 1000000;
  const std::string test_string = "concurrent_test_string";

  auto intern_function = [&]() {
    for (int i = 0; i < kNumIterations; ++i) {
      auto interned_str = StringInternStore::Intern(test_string);
      SYNCOUT("CREATED : " << (void*)(interned_str->Str().data() - 8)
                           << " (REFCOUNT : " << interned_str.RefCount()
                           << ")");

      EXPECT_EQ(interned_str->Str(), test_string);
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back(intern_function);
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (auto& thread : threads) {
    EXPECT_EQ(thread.joinable(), false);
  }

  std::cout << "Final string count: "
            << StringInternStore::Instance().UniqueStrings() << std::endl;

  EXPECT_EQ(StringInternStore::Instance().UniqueStrings(), 0);
}
}  // namespace

}  // namespace valkey_search
