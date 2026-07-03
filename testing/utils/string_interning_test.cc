/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/string_interning.h"

#include <algorithm>
#include <cstring>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
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

class BorrowedInternedStringPtrTest : public vmsdk::ValkeyTest {};

TEST_F(BorrowedInternedStringPtrTest, BasicBorrowAndAccess) {
  auto owned = StringInternStore::Intern("borrowed_test");
  EXPECT_EQ(owned.RefCount(), 1);

  BorrowedInternedStringPtr borrowed(owned);
  // Borrowing does not increment ref count.
  EXPECT_EQ(owned.RefCount(), 1);
  EXPECT_EQ(borrowed.Str(), "borrowed_test");
  EXPECT_TRUE(borrowed);
}

TEST_F(BorrowedInternedStringPtrTest, CopyDoesNotRefCount) {
  auto owned = StringInternStore::Intern("copy_test");
  BorrowedInternedStringPtr b1(owned);
  BorrowedInternedStringPtr b2 = b1;
  BorrowedInternedStringPtr b3;
  b3 = b2;
  // Three borrowed copies, ref count still 1.
  EXPECT_EQ(owned.RefCount(), 1);
  EXPECT_EQ(b1.Str(), "copy_test");
  EXPECT_EQ(b2.Str(), "copy_test");
  EXPECT_EQ(b3.Str(), "copy_test");
}

TEST_F(BorrowedInternedStringPtrTest, DestroyDoesNotDecrementRefCount) {
  auto owned = StringInternStore::Intern("destroy_test");
  {
    BorrowedInternedStringPtr borrowed(owned);
    EXPECT_EQ(owned.RefCount(), 1);
  }
  // After borrowed goes out of scope, ref count unchanged.
  EXPECT_EQ(owned.RefCount(), 1);
  EXPECT_EQ(owned->Str(), "destroy_test");
}

TEST_F(BorrowedInternedStringPtrTest, MaterializeIncrementsRefCount) {
  auto owned = StringInternStore::Intern("materialize_test");
  EXPECT_EQ(owned.RefCount(), 1);

  BorrowedInternedStringPtr borrowed(owned);
  InternedStringPtr materialized = borrowed.Materialize();

  EXPECT_EQ(owned.RefCount(), 2);
  EXPECT_EQ(materialized->Str(), "materialize_test");
  EXPECT_EQ(materialized, owned);
}

TEST_F(BorrowedInternedStringPtrTest, MaterializedPtrKeepsStringAlive) {
  InternedStringPtr materialized;
  {
    auto owned = StringInternStore::Intern("lifetime_test");
    BorrowedInternedStringPtr borrowed(owned);
    materialized = borrowed.Materialize();
    EXPECT_EQ(owned.RefCount(), 2);
  }
  // Original owned is gone, but materialized keeps it alive.
  EXPECT_EQ(materialized.RefCount(), 1);
  EXPECT_EQ(materialized->Str(), "lifetime_test");
  EXPECT_EQ(StringInternStore::Instance().UniqueStrings(), 1);
}

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
  const int kNumIterations = 100000;
  const std::string test_string = "concurrent_test_string";

  auto intern_function = [&]() {
    for (int i = 0; i < kNumIterations; ++i) {
      auto interned_str = StringInternStore::Intern(test_string);
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
// ---------------------------------------------------------------------------
// BagOfInternedStringPtrs tests
// ---------------------------------------------------------------------------

class BagOfInternedStringPtrsTest : public vmsdk::ValkeyTest {};

TEST_F(BagOfInternedStringPtrsTest, FitsIn8Bytes) {
  static_assert(sizeof(BagOfInternedStringPtrs) == 8);
  EXPECT_EQ(sizeof(BagOfInternedStringPtrs), 8u);
}

TEST_F(BagOfInternedStringPtrsTest, DefaultEmpty) {
  BagOfInternedStringPtrs bag;
  EXPECT_TRUE(bag.empty());
  EXPECT_EQ(bag.size(), 0u);
  EXPECT_EQ(bag.begin(), bag.end());
  EXPECT_FALSE(bag.contains(StringInternStore::Intern("nope")));
  EXPECT_EQ(bag.find(StringInternStore::Intern("nope")), bag.end());
}

TEST_F(BagOfInternedStringPtrsTest, SingleElementLifecycle) {
  auto k = StringInternStore::Intern("k");
  EXPECT_EQ(k.RefCount(), 1);
  {
    BagOfInternedStringPtrs bag;
    auto [it, inserted] = bag.insert(k);
    EXPECT_TRUE(inserted);
    EXPECT_EQ(k.RefCount(), 2);
    EXPECT_FALSE(bag.empty());
    EXPECT_EQ(bag.size(), 1u);
    EXPECT_TRUE(bag.contains(k));
    EXPECT_NE(bag.find(k), bag.end());
    EXPECT_EQ(it->Hash(), k.Hash());
    EXPECT_EQ((*it)->Str(), "k");
  }
  EXPECT_EQ(k.RefCount(), 1);
}

TEST_F(BagOfInternedStringPtrsTest, InsertDuplicateInSingleMode) {
  auto k = StringInternStore::Intern("dup");
  BagOfInternedStringPtrs bag;
  bag.insert(k);
  EXPECT_EQ(k.RefCount(), 2);
  EXPECT_EQ(bag.TestModeForTesting(),
            BagOfInternedStringPtrs::TestMode::kSingle);
  auto [it, inserted] = bag.insert(k);
  EXPECT_FALSE(inserted);
  EXPECT_EQ(bag.size(), 1u);
  EXPECT_EQ(k.RefCount(), 2);
  EXPECT_NE(it, bag.end());
  // Sanity: the duplicate insert must not have promoted out of Single mode,
  // and the bag itself remains 8 bytes regardless of representation.
  EXPECT_EQ(bag.TestModeForTesting(),
            BagOfInternedStringPtrs::TestMode::kSingle);
  static_assert(sizeof(BagOfInternedStringPtrs) == 8);
}

TEST_F(BagOfInternedStringPtrsTest, PromoteSingleToMulti) {
  auto a = StringInternStore::Intern("a");
  auto b = StringInternStore::Intern("b");
  BagOfInternedStringPtrs bag;
  bag.insert(a);
  EXPECT_EQ(bag.TestModeForTesting(),
            BagOfInternedStringPtrs::TestMode::kSingle);
  bag.insert(b);
  EXPECT_EQ(bag.TestModeForTesting(),
            BagOfInternedStringPtrs::TestMode::kArray4);
  EXPECT_EQ(bag.size(), 2u);
  EXPECT_TRUE(bag.contains(a));
  EXPECT_TRUE(bag.contains(b));
  EXPECT_EQ(a.RefCount(), 2);
  EXPECT_EQ(b.RefCount(), 2);
}

TEST_F(BagOfInternedStringPtrsTest, MultiInsertEraseFindContains) {
  std::vector<InternedStringPtr> keys;
  for (int i = 0; i < 8; ++i) {
    keys.push_back(StringInternStore::Intern("multi_" + std::to_string(i)));
  }
  BagOfInternedStringPtrs bag;
  for (const auto& k : keys) {
    auto [it, inserted] = bag.insert(k);
    EXPECT_TRUE(inserted);
    EXPECT_NE(it, bag.end());
  }
  EXPECT_EQ(bag.size(), keys.size());
  for (const auto& k : keys) {
    EXPECT_TRUE(bag.contains(k));
    EXPECT_NE(bag.find(k), bag.end());
    EXPECT_EQ(k.RefCount(), 2);
  }
  // erase a missing key
  auto missing = StringInternStore::Intern("missing");
  EXPECT_EQ(bag.erase(missing), 0u);
  // erase real keys (down to 2 so we stay in multi mode)
  for (size_t i = 0; i < keys.size() - 2; ++i) {
    EXPECT_EQ(bag.erase(keys[i]), 1u);
    EXPECT_EQ(keys[i].RefCount(), 1);
  }
  EXPECT_EQ(bag.size(), 2u);
}

TEST_F(BagOfInternedStringPtrsTest, DemoteMultiToSingleOnErase) {
  auto a = StringInternStore::Intern("demoteA");
  auto b = StringInternStore::Intern("demoteB");
  auto c = StringInternStore::Intern("demoteC");
  BagOfInternedStringPtrs bag;
  bag.insert(a);
  bag.insert(b);
  bag.insert(c);
  EXPECT_EQ(bag.size(), 3u);
  EXPECT_EQ(bag.TestModeForTesting(),
            BagOfInternedStringPtrs::TestMode::kArray4);
  EXPECT_EQ(bag.erase(b), 1u);
  EXPECT_EQ(bag.TestModeForTesting(),
            BagOfInternedStringPtrs::TestMode::kArray4);
  EXPECT_EQ(bag.erase(c), 1u);
  EXPECT_EQ(bag.size(), 1u);
  // Demote happens at count==1: Array4 -> Single.
  EXPECT_EQ(bag.TestModeForTesting(),
            BagOfInternedStringPtrs::TestMode::kSingle);
  EXPECT_TRUE(bag.contains(a));
  EXPECT_FALSE(bag.contains(b));
  EXPECT_FALSE(bag.contains(c));
  EXPECT_EQ(a.RefCount(), 2);
  EXPECT_EQ(b.RefCount(), 1);
  EXPECT_EQ(c.RefCount(), 1);
  // After demotion the surviving element is reachable through normal
  // single-mode iteration.
  EXPECT_EQ(std::distance(bag.begin(), bag.end()), 1);
  EXPECT_EQ((*bag.begin())->Str(), "demoteA");
  // Removing the lone element returns to empty.
  EXPECT_EQ(bag.erase(a), 1u);
  EXPECT_TRUE(bag.empty());
  EXPECT_EQ(bag.TestModeForTesting(),
            BagOfInternedStringPtrs::TestMode::kEmpty);
  EXPECT_EQ(a.RefCount(), 1);
}

TEST_F(BagOfInternedStringPtrsTest, EraseByIterator) {
  // Single mode.
  {
    auto k = StringInternStore::Intern("eraseit_single");
    BagOfInternedStringPtrs bag;
    bag.insert(k);
    auto next = bag.erase(bag.begin());
    EXPECT_EQ(next, bag.end());
    EXPECT_TRUE(bag.empty());
    EXPECT_EQ(k.RefCount(), 1);
  }
  // Multi mode (erase one of three; demotion happens at size==1, not here).
  {
    auto a = StringInternStore::Intern("eit_a");
    auto b = StringInternStore::Intern("eit_b");
    auto c = StringInternStore::Intern("eit_c");
    BagOfInternedStringPtrs bag;
    bag.insert(a);
    bag.insert(b);
    bag.insert(c);
    auto it = bag.find(b);
    ASSERT_NE(it, bag.end());
    auto next = bag.erase(it);
    EXPECT_EQ(next, bag.end());  // matches absl::flat_hash_set::erase semantics
    EXPECT_FALSE(bag.contains(b));
    EXPECT_EQ(bag.size(), 2u);
    EXPECT_EQ(b.RefCount(), 1);
  }
}

TEST_F(BagOfInternedStringPtrsTest, ClearAllStates) {
  auto a = StringInternStore::Intern("clear_a");
  auto b = StringInternStore::Intern("clear_b");
  // Empty -> empty.
  {
    BagOfInternedStringPtrs bag;
    bag.clear();
    EXPECT_TRUE(bag.empty());
  }
  // Single -> empty.
  {
    BagOfInternedStringPtrs bag;
    bag.insert(a);
    EXPECT_EQ(a.RefCount(), 2);
    bag.clear();
    EXPECT_TRUE(bag.empty());
    EXPECT_EQ(a.RefCount(), 1);
  }
  // Multi -> empty.
  {
    BagOfInternedStringPtrs bag;
    bag.insert(a);
    bag.insert(b);
    EXPECT_EQ(a.RefCount(), 2);
    EXPECT_EQ(b.RefCount(), 2);
    bag.clear();
    EXPECT_TRUE(bag.empty());
    EXPECT_EQ(a.RefCount(), 1);
    EXPECT_EQ(b.RefCount(), 1);
  }
}

TEST_F(BagOfInternedStringPtrsTest, NonCopyable) {
  static_assert(!std::is_copy_constructible_v<BagOfInternedStringPtrs>);
  static_assert(!std::is_copy_assignable_v<BagOfInternedStringPtrs>);
  static_assert(std::is_move_constructible_v<BagOfInternedStringPtrs>);
  static_assert(std::is_move_assignable_v<BagOfInternedStringPtrs>);
}

TEST_F(BagOfInternedStringPtrsTest, MoveCtorAndAssign) {
  auto a = StringInternStore::Intern("mv_a");
  auto b = StringInternStore::Intern("mv_b");
  // Single.
  {
    BagOfInternedStringPtrs src;
    src.insert(a);
    EXPECT_EQ(a.RefCount(), 2);
    BagOfInternedStringPtrs dst(std::move(src));
    EXPECT_TRUE(src.empty());
    EXPECT_EQ(dst.size(), 1u);
    EXPECT_EQ(a.RefCount(), 2);  // no churn
  }
  // Multi via move-assign over an existing single.
  {
    BagOfInternedStringPtrs src;
    src.insert(a);
    src.insert(b);
    BagOfInternedStringPtrs dst;
    dst.insert(a);
    EXPECT_EQ(a.RefCount(), 3);
    dst = std::move(src);
    EXPECT_TRUE(src.empty());
    EXPECT_EQ(dst.size(), 2u);
    EXPECT_EQ(a.RefCount(), 2);  // dst dropped its own ref before adopting
    EXPECT_EQ(b.RefCount(), 2);
  }
  EXPECT_EQ(a.RefCount(), 1);
  EXPECT_EQ(b.RefCount(), 1);
  // Self-move is a no-op (does not crash, contents preserved).
  {
    BagOfInternedStringPtrs bag;
    bag.insert(a);
    auto& bag_ref = bag;
    bag = std::move(bag_ref);
    EXPECT_EQ(bag.size(), 1u);
    EXPECT_TRUE(bag.contains(a));
  }
}

TEST_F(BagOfInternedStringPtrsTest, IterationVisitsEverythingExactlyOnce) {
  auto check_iteration = [](const std::vector<std::string>& strings) {
    std::vector<InternedStringPtr> keys;
    for (const auto& s : strings) {
      keys.push_back(StringInternStore::Intern(s));
    }
    BagOfInternedStringPtrs bag;
    for (const auto& k : keys) {
      bag.insert(k);
    }
    absl::flat_hash_set<std::string> seen;
    for (const auto& v : bag) {
      seen.insert(std::string(v->Str()));
    }
    EXPECT_EQ(seen.size(), strings.size());
    for (const auto& s : strings) {
      EXPECT_TRUE(seen.contains(s)) << s;
    }
    EXPECT_EQ(static_cast<size_t>(std::distance(bag.begin(), bag.end())),
              strings.size());
  };
  check_iteration({});
  check_iteration({"only"});
  check_iteration({"two_a", "two_b"});
  check_iteration({"many_1", "many_2", "many_3", "many_4", "many_5"});
}

TEST_F(BagOfInternedStringPtrsTest, IteratorIsStlForwardIterator) {
  using It = BagOfInternedStringPtrs::const_iterator;
  static_assert(std::is_same_v<std::iterator_traits<It>::iterator_category,
                               std::forward_iterator_tag>);
  static_assert(
      std::is_same_v<std::iterator_traits<It>::value_type, InternedStringPtr>);
  // Usable with STL algorithms.
  auto a = StringInternStore::Intern("stl_a");
  auto b = StringInternStore::Intern("stl_b");
  BagOfInternedStringPtrs bag;
  bag.insert(a);
  bag.insert(b);
  auto found = std::find_if(
      bag.begin(), bag.end(),
      [&](const InternedStringPtr& p) { return p->Str() == "stl_b"; });
  ASSERT_NE(found, bag.end());
  EXPECT_EQ((*found)->Str(), "stl_b");
  std::vector<InternedStringPtr> copied(bag.begin(), bag.end());
  EXPECT_EQ(copied.size(), 2u);
}

TEST_F(BagOfInternedStringPtrsTest, SwapAcrossAllModeCombinations) {
  auto a = StringInternStore::Intern("sw_a");
  auto b = StringInternStore::Intern("sw_b");
  auto c = StringInternStore::Intern("sw_c");
  auto make = [&](int mode) {
    BagOfInternedStringPtrs bag;
    if (mode == 1) {
      bag.insert(a);
    } else if (mode == 2) {
      bag.insert(b);
      bag.insert(c);
    }
    return bag;
  };
  for (int lhs_mode = 0; lhs_mode <= 2; ++lhs_mode) {
    for (int rhs_mode = 0; rhs_mode <= 2; ++rhs_mode) {
      auto lhs = make(lhs_mode);
      auto rhs = make(rhs_mode);
      auto lhs_size = lhs.size();
      auto rhs_size = rhs.size();
      lhs.swap(rhs);
      EXPECT_EQ(lhs.size(), rhs_size);
      EXPECT_EQ(rhs.size(), lhs_size);
    }
  }
}

TEST_F(BagOfInternedStringPtrsTest, RvalueInsertAdoptsRefCount) {
  auto k = StringInternStore::Intern("rv_k");
  EXPECT_EQ(k.RefCount(), 1);
  BagOfInternedStringPtrs bag;
  {
    InternedStringPtr local = k;
    EXPECT_EQ(k.RefCount(), 2);
    bag.insert(std::move(local));
    // local was emptied; the ref count moved into the bag.
    EXPECT_EQ(k.RefCount(), 2);
  }
  EXPECT_EQ(k.RefCount(), 2);
  bag.clear();
  EXPECT_EQ(k.RefCount(), 1);
}

TEST_F(BagOfInternedStringPtrsTest, NoLeaksUnderRepeatedChurn) {
  // Repeatedly insert and erase to exercise promote/demote transitions and
  // confirm we always settle back to baseline ref counts.
  auto a = StringInternStore::Intern("churn_a");
  auto b = StringInternStore::Intern("churn_b");
  auto c = StringInternStore::Intern("churn_c");
  BagOfInternedStringPtrs bag;
  for (int i = 0; i < 100; ++i) {
    bag.insert(a);
    bag.insert(b);
    bag.insert(c);
    bag.erase(c);
    bag.erase(b);
    bag.erase(a);
    EXPECT_TRUE(bag.empty());
  }
  EXPECT_EQ(a.RefCount(), 1);
  EXPECT_EQ(b.RefCount(), 1);
  EXPECT_EQ(c.RefCount(), 1);
}

// ---------------------------------------------------------------------------
// Representation transition tests
// Modes: Empty -> Single -> Array4 -> Array8 -> Set
// Insert promotes one mode up at the lower-bound boundary.
// Erase demotes one mode down at the lower-bound boundary.
// ---------------------------------------------------------------------------

namespace {
// Pre-intern N keys "trkN_0" ... "trkN_{N-1}" with a unique prefix per call
// so concurrent tests don't collide on ref counts.
std::vector<InternedStringPtr> MakeKeys(int n, const std::string& prefix) {
  std::vector<InternedStringPtr> keys;
  keys.reserve(n);
  for (int i = 0; i < n; ++i) {
    keys.push_back(StringInternStore::Intern(prefix + std::to_string(i)));
  }
  return keys;
}

// Walk the bag and verify it contains exactly the first `expected_count` keys
// from `keys` (regardless of iteration order), and nothing else.
void ExpectBagContains(const BagOfInternedStringPtrs& bag,
                       const std::vector<InternedStringPtr>& keys,
                       size_t expected_count) {
  EXPECT_EQ(bag.size(), expected_count);
  for (size_t i = 0; i < expected_count; ++i) {
    EXPECT_TRUE(bag.contains(keys[i])) << "missing keys[" << i << "]";
  }
  for (size_t i = expected_count; i < keys.size(); ++i) {
    EXPECT_FALSE(bag.contains(keys[i])) << "unexpected keys[" << i << "]";
  }
  // Iteration must visit each expected key exactly once.
  absl::flat_hash_set<size_t> seen;
  for (const auto& v : bag) {
    for (size_t i = 0; i < expected_count; ++i) {
      if (v == keys[i]) {
        EXPECT_TRUE(seen.insert(i).second) << "key " << i << " visited twice";
        break;
      }
    }
  }
  EXPECT_EQ(seen.size(), expected_count);
}
}  // namespace

TEST_F(BagOfInternedStringPtrsTest, InsertProgressionEmptyToSet) {
  using Mode = BagOfInternedStringPtrs::TestMode;
  // Insert keys one at a time and verify size + content + ref counts + mode
  // at every boundary.
  auto keys = MakeKeys(10, "ipe_");

  BagOfInternedStringPtrs bag;
  EXPECT_EQ(bag.size(), 0u);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kEmpty);

  // Empty -> Single (1 element).
  bag.insert(keys[0]);
  ExpectBagContains(bag, keys, 1);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kSingle);
  EXPECT_EQ(keys[0].RefCount(), 2);

  // Single -> Array4 (2 elements).
  bag.insert(keys[1]);
  ExpectBagContains(bag, keys, 2);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kArray4);
  EXPECT_EQ(keys[1].RefCount(), 2);

  // Array4 stays Array4 (3, 4 elements).
  bag.insert(keys[2]);
  ExpectBagContains(bag, keys, 3);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kArray4);
  bag.insert(keys[3]);
  ExpectBagContains(bag, keys, 4);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kArray4);

  // Array4 -> Array8 (5 elements).
  bag.insert(keys[4]);
  ExpectBagContains(bag, keys, 5);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kArray8);
  EXPECT_EQ(keys[4].RefCount(), 2);

  // Array8 stays Array8 (6, 7, 8 elements).
  bag.insert(keys[5]);
  bag.insert(keys[6]);
  bag.insert(keys[7]);
  ExpectBagContains(bag, keys, 8);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kArray8);

  // Array8 -> Set (9 elements).
  bag.insert(keys[8]);
  ExpectBagContains(bag, keys, 9);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kSet);

  // Set stays Set.
  bag.insert(keys[9]);
  ExpectBagContains(bag, keys, 10);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kSet);
  for (const auto& k : keys) {
    EXPECT_EQ(k.RefCount(), 2);
  }
}

TEST_F(BagOfInternedStringPtrsTest, EraseProgressionSetToEmpty) {
  using Mode = BagOfInternedStringPtrs::TestMode;
  // Inverse of the previous test: starting from 10 elements (Set mode),
  // erase one at a time and verify mode-boundary demotions.
  auto keys = MakeKeys(10, "ese_");
  BagOfInternedStringPtrs bag;
  for (const auto& k : keys) {
    bag.insert(k);
  }
  ExpectBagContains(bag, keys, 10);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kSet);
  for (const auto& k : keys) {
    EXPECT_EQ(k.RefCount(), 2);
  }

  // Set -> Set (10 -> 9).
  bag.erase(keys[9]);
  EXPECT_EQ(keys[9].RefCount(), 1);
  ExpectBagContains(bag, keys, 9);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kSet);

  // Set -> Array8 (9 -> 8).
  bag.erase(keys[8]);
  EXPECT_EQ(keys[8].RefCount(), 1);
  ExpectBagContains(bag, keys, 8);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kArray8);

  // Array8 -> Array8 (8 -> 7, 6, 5).
  bag.erase(keys[7]);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kArray8);
  bag.erase(keys[6]);
  bag.erase(keys[5]);
  ExpectBagContains(bag, keys, 5);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kArray8);

  // Array8 -> Array4 (5 -> 4).
  bag.erase(keys[4]);
  EXPECT_EQ(keys[4].RefCount(), 1);
  ExpectBagContains(bag, keys, 4);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kArray4);

  // Array4 -> Array4 (4 -> 3, 2).
  bag.erase(keys[3]);
  bag.erase(keys[2]);
  ExpectBagContains(bag, keys, 2);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kArray4);

  // Array4 -> Single (2 -> 1).
  bag.erase(keys[1]);
  EXPECT_EQ(keys[1].RefCount(), 1);
  ExpectBagContains(bag, keys, 1);
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kSingle);

  // Single -> Empty.
  bag.erase(keys[0]);
  EXPECT_EQ(keys[0].RefCount(), 1);
  EXPECT_TRUE(bag.empty());
  EXPECT_EQ(bag.TestModeForTesting(), Mode::kEmpty);
}

TEST_F(BagOfInternedStringPtrsTest, ArrayPacksToFrontAfterMidErase) {
  // Erase from the middle of each array mode and verify subsequent elements
  // shift left to keep slots [0, count) populated and no nulls in the middle.
  auto keys = MakeKeys(8, "pack_");

  // Array4: erase middle.
  {
    BagOfInternedStringPtrs bag;
    for (int i = 0; i < 4; ++i) bag.insert(keys[i]);
    bag.erase(keys[1]);
    EXPECT_EQ(bag.size(), 3u);
    EXPECT_TRUE(bag.contains(keys[0]));
    EXPECT_FALSE(bag.contains(keys[1]));
    EXPECT_TRUE(bag.contains(keys[2]));
    EXPECT_TRUE(bag.contains(keys[3]));
    // Iterate; should yield exactly 3 distinct elements with no null views.
    int n = 0;
    for (const auto& v : bag) {
      EXPECT_TRUE(v);
      ++n;
    }
    EXPECT_EQ(n, 3);
  }

  // Array8: erase from middle.
  {
    BagOfInternedStringPtrs bag;
    for (int i = 0; i < 7; ++i) bag.insert(keys[i]);
    bag.erase(keys[3]);
    EXPECT_EQ(bag.size(), 6u);
    EXPECT_FALSE(bag.contains(keys[3]));
    int n = 0;
    for (const auto& v : bag) {
      EXPECT_TRUE(v);
      ++n;
    }
    EXPECT_EQ(n, 6);
  }
}

TEST_F(BagOfInternedStringPtrsTest, EraseLastElementOfArray) {
  // Edge case: erase the highest-indexed element in an array (no shifts).
  auto keys = MakeKeys(6, "last_");

  {
    BagOfInternedStringPtrs bag;
    for (int i = 0; i < 4; ++i) bag.insert(keys[i]);
    bag.erase(keys[3]);
    EXPECT_EQ(bag.size(), 3u);
    EXPECT_FALSE(bag.contains(keys[3]));
    EXPECT_EQ(keys[3].RefCount(), 1);
  }
  {
    BagOfInternedStringPtrs bag;
    for (int i = 0; i < 6; ++i) bag.insert(keys[i]);
    bag.erase(keys[5]);
    EXPECT_EQ(bag.size(), 5u);
    EXPECT_FALSE(bag.contains(keys[5]));
    EXPECT_EQ(keys[5].RefCount(), 1);
  }
}

TEST_F(BagOfInternedStringPtrsTest, DuplicateInsertNoOpAcrossModes) {
  auto keys = MakeKeys(6, "dup_");
  BagOfInternedStringPtrs bag;
  // Single
  bag.insert(keys[0]);
  EXPECT_FALSE(bag.insert(keys[0]).second);
  EXPECT_EQ(keys[0].RefCount(), 2);
  // Array4
  bag.insert(keys[1]);
  bag.insert(keys[2]);
  EXPECT_FALSE(bag.insert(keys[1]).second);
  EXPECT_EQ(keys[1].RefCount(), 2);
  // Array8
  bag.insert(keys[3]);
  bag.insert(keys[4]);
  EXPECT_FALSE(bag.insert(keys[3]).second);
  EXPECT_EQ(keys[3].RefCount(), 2);
}

TEST_F(BagOfInternedStringPtrsTest, DestructorReleasesAllElementsInEveryMode) {
  // For each mode, build a bag that holds the keys, then destroy the bag and
  // confirm every key's ref count returns to 1.
  auto keys = MakeKeys(10, "drel_");
  for (size_t n : {0, 1, 2, 4, 5, 8, 9, 10}) {
    {
      BagOfInternedStringPtrs bag;
      for (size_t i = 0; i < n; ++i) {
        bag.insert(keys[i]);
        EXPECT_EQ(keys[i].RefCount(), 2) << "n=" << n << " i=" << i;
      }
      EXPECT_EQ(bag.size(), n);
    }
    // Bag destructed; all owned refs should be released.
    for (size_t i = 0; i < keys.size(); ++i) {
      EXPECT_EQ(keys[i].RefCount(), 1)
          << "leak: bag with n=" << n << " left key " << i << " elevated";
    }
  }
}

TEST_F(BagOfInternedStringPtrsTest, MoveCtorAcrossEveryMode) {
  // Build a source bag in each mode, move-construct into a destination, and
  // verify the destination has the contents and the source is empty.
  auto keys = MakeKeys(10, "mv_");
  for (size_t n : {0, 1, 2, 5, 9}) {
    BagOfInternedStringPtrs src;
    for (size_t i = 0; i < n; ++i) src.insert(keys[i]);
    BagOfInternedStringPtrs dst(std::move(src));
    EXPECT_TRUE(src.empty()) << "n=" << n;
    EXPECT_EQ(dst.size(), n) << "n=" << n;
    for (size_t i = 0; i < n; ++i) {
      EXPECT_TRUE(dst.contains(keys[i])) << "n=" << n << " i=" << i;
    }
    // No ref-count churn during a move.
    for (size_t i = 0; i < n; ++i) {
      EXPECT_EQ(keys[i].RefCount(), 2);
    }
    // dst goes out of scope; refs should drop back.
  }
  for (const auto& k : keys) {
    EXPECT_EQ(k.RefCount(), 1);
  }
}

TEST_F(BagOfInternedStringPtrsTest, EraseByIteratorAllArrayModes) {
  // erase(iterator) must work in Array4, Array8, and Set.
  {
    auto keys = MakeKeys(3, "eia4_");
    BagOfInternedStringPtrs bag;
    for (const auto& k : keys) bag.insert(k);  // Array4 with 3
    auto it = bag.find(keys[1]);
    ASSERT_NE(it, bag.end());
    bag.erase(it);
    EXPECT_EQ(bag.size(), 2u);
    EXPECT_FALSE(bag.contains(keys[1]));
    EXPECT_EQ(keys[1].RefCount(), 1);
  }
  {
    auto keys = MakeKeys(6, "eia8_");
    BagOfInternedStringPtrs bag;
    for (const auto& k : keys) bag.insert(k);  // Array8 with 6
    auto it = bag.find(keys[2]);
    ASSERT_NE(it, bag.end());
    bag.erase(it);
    EXPECT_EQ(bag.size(), 5u);
    EXPECT_FALSE(bag.contains(keys[2]));
    EXPECT_EQ(keys[2].RefCount(), 1);
  }
  {
    auto keys = MakeKeys(10, "eiaset_");
    BagOfInternedStringPtrs bag;
    for (const auto& k : keys) bag.insert(k);  // Set with 10
    auto it = bag.find(keys[4]);
    ASSERT_NE(it, bag.end());
    bag.erase(it);
    EXPECT_EQ(bag.size(), 9u);
    EXPECT_FALSE(bag.contains(keys[4]));
    EXPECT_EQ(keys[4].RefCount(), 1);
  }
}

TEST_F(BagOfInternedStringPtrsTest, RvalueInsertIntoArrayAndSetModes) {
  // Walk a moving InternedStringPtr into the bag at every mode boundary and
  // confirm the rvalue path adopts the ref count rather than copying it.
  auto keys = MakeKeys(10, "rva_");
  BagOfInternedStringPtrs bag;
  // Single mode (Empty -> Single).
  {
    InternedStringPtr local = keys[0];
    EXPECT_EQ(keys[0].RefCount(), 2);
    bag.insert(std::move(local));
    // local was emptied; ref count moved to bag.
    EXPECT_EQ(keys[0].RefCount(), 2);
    EXPECT_EQ(bag.TestModeForTesting(),
              BagOfInternedStringPtrs::TestMode::kSingle);
  }
  // Array4 promotion (Single -> Array4).
  {
    InternedStringPtr local = keys[1];
    EXPECT_EQ(keys[1].RefCount(), 2);
    bag.insert(std::move(local));
    EXPECT_EQ(keys[1].RefCount(), 2);
    EXPECT_EQ(bag.TestModeForTesting(),
              BagOfInternedStringPtrs::TestMode::kArray4);
  }
  // Array4 fill via rvalue (still Array4).
  for (int i = 2; i < 4; ++i) {
    InternedStringPtr local = keys[i];
    bag.insert(std::move(local));
    EXPECT_EQ(keys[i].RefCount(), 2);
  }
  EXPECT_EQ(bag.TestModeForTesting(),
            BagOfInternedStringPtrs::TestMode::kArray4);
  // Array4 -> Array8 promotion via rvalue.
  {
    InternedStringPtr local = keys[4];
    bag.insert(std::move(local));
    EXPECT_EQ(keys[4].RefCount(), 2);
    EXPECT_EQ(bag.TestModeForTesting(),
              BagOfInternedStringPtrs::TestMode::kArray8);
  }
  // Array8 fill via rvalue.
  for (int i = 5; i < 8; ++i) {
    InternedStringPtr local = keys[i];
    bag.insert(std::move(local));
    EXPECT_EQ(keys[i].RefCount(), 2);
  }
  EXPECT_EQ(bag.TestModeForTesting(),
            BagOfInternedStringPtrs::TestMode::kArray8);
  // Array8 -> Set promotion via rvalue.
  {
    InternedStringPtr local = keys[8];
    bag.insert(std::move(local));
    EXPECT_EQ(keys[8].RefCount(), 2);
    EXPECT_EQ(bag.TestModeForTesting(),
              BagOfInternedStringPtrs::TestMode::kSet);
  }
  // Set insert via rvalue (no mode change).
  {
    InternedStringPtr local = keys[9];
    bag.insert(std::move(local));
    EXPECT_EQ(keys[9].RefCount(), 2);
    EXPECT_EQ(bag.TestModeForTesting(),
              BagOfInternedStringPtrs::TestMode::kSet);
  }
  EXPECT_EQ(bag.size(), 10u);
}

TEST_F(BagOfInternedStringPtrsTest, EraseNonexistentInEveryMode) {
  auto present = MakeKeys(10, "enpr_");
  auto missing = StringInternStore::Intern("enpr_missing");
  // Single mode.
  {
    BagOfInternedStringPtrs bag;
    bag.insert(present[0]);
    EXPECT_EQ(bag.erase(missing), 0u);
    EXPECT_EQ(bag.size(), 1u);
    EXPECT_EQ(bag.TestModeForTesting(),
              BagOfInternedStringPtrs::TestMode::kSingle);
  }
  // Array4 mode.
  {
    BagOfInternedStringPtrs bag;
    for (int i = 0; i < 3; ++i) bag.insert(present[i]);
    EXPECT_EQ(bag.erase(missing), 0u);
    EXPECT_EQ(bag.size(), 3u);
    EXPECT_EQ(bag.TestModeForTesting(),
              BagOfInternedStringPtrs::TestMode::kArray4);
  }
  // Array8 mode.
  {
    BagOfInternedStringPtrs bag;
    for (int i = 0; i < 6; ++i) bag.insert(present[i]);
    EXPECT_EQ(bag.erase(missing), 0u);
    EXPECT_EQ(bag.size(), 6u);
    EXPECT_EQ(bag.TestModeForTesting(),
              BagOfInternedStringPtrs::TestMode::kArray8);
  }
  // Set mode.
  {
    BagOfInternedStringPtrs bag;
    for (int i = 0; i < 10; ++i) bag.insert(present[i]);
    EXPECT_EQ(bag.erase(missing), 0u);
    EXPECT_EQ(bag.size(), 10u);
    EXPECT_EQ(bag.TestModeForTesting(),
              BagOfInternedStringPtrs::TestMode::kSet);
  }
  // Erasing the absent key must not have inadvertently bumped its ref count.
  EXPECT_EQ(missing.RefCount(), 1);
}

TEST_F(BagOfInternedStringPtrsTest, ReservePicksCorrectMode) {
  using Mode = BagOfInternedStringPtrs::TestMode;
  // reserve(n) on a fresh bag should pre-pick a representation that fits n
  // elements, so subsequent inserts don't trigger any promotions.
  struct Case {
    size_t n;
    Mode expected_after_reserve;
    Mode expected_after_insert_all;
  };
  for (Case c :
       {Case{0, Mode::kEmpty, Mode::kEmpty},
        Case{1, Mode::kEmpty, Mode::kSingle},  // <=1 stays lazy
        Case{2, Mode::kArray4, Mode::kArray4},
        Case{4, Mode::kArray4, Mode::kArray4},
        Case{5, Mode::kArray8, Mode::kArray8},
        Case{8, Mode::kArray8, Mode::kArray8}, Case{9, Mode::kSet, Mode::kSet},
        Case{20, Mode::kSet, Mode::kSet}}) {
    BagOfInternedStringPtrs bag;
    bag.reserve(c.n);
    EXPECT_EQ(bag.TestModeForTesting(), c.expected_after_reserve)
        << "n=" << c.n;
    auto keys =
        MakeKeys(static_cast<int>(c.n), "rsv_" + std::to_string(c.n) + "_");
    for (const auto& k : keys) {
      bag.insert(k);
    }
    EXPECT_EQ(bag.size(), c.n) << "n=" << c.n;
    EXPECT_EQ(bag.TestModeForTesting(), c.expected_after_insert_all)
        << "n=" << c.n;
  }
}

TEST_F(BagOfInternedStringPtrsTest, ReserveIsNoopOnNonEmptyBag) {
  // reserve() should not corrupt a bag that already has elements.
  auto keys = MakeKeys(3, "rsvne_");
  BagOfInternedStringPtrs bag;
  for (const auto& k : keys) bag.insert(k);
  auto mode_before = bag.TestModeForTesting();
  bag.reserve(100);  // would promote to Set if we honored this; we don't.
  EXPECT_EQ(bag.TestModeForTesting(), mode_before);
  EXPECT_EQ(bag.size(), 3u);
  for (const auto& k : keys) {
    EXPECT_TRUE(bag.contains(k));
  }
}

TEST_F(BagOfInternedStringPtrsTest, ChurnAcrossAllModes) {
  // Stress: cycle a bag through every mode many times in both directions and
  // verify no ref-count leaks.
  auto keys = MakeKeys(10, "fchurn_");
  for (int rep = 0; rep < 25; ++rep) {
    BagOfInternedStringPtrs bag;
    for (const auto& k : keys) bag.insert(k);  // grow through all modes
    EXPECT_EQ(bag.size(), 10u);
    for (auto it = keys.rbegin(); it != keys.rend(); ++it) {
      bag.erase(*it);  // shrink through all modes
    }
    EXPECT_TRUE(bag.empty());
  }
  for (const auto& k : keys) {
    EXPECT_EQ(k.RefCount(), 1);
  }
}

}  // namespace

}  // namespace valkey_search
