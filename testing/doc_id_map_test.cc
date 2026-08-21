/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/utils/doc_id_map.h"

#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "src/utils/string_interning.h"

namespace valkey_search {

class DocIdMapTest : public ::testing::Test {
 protected:
  void SetUp() override {
    DocIdMap::Instance().Clear();
  }

  void TearDown() override {
    DocIdMap::Instance().Clear();
  }
};

TEST_F(DocIdMapTest, BasicGetOrAssignWithInternedPtr) {
  auto& map = DocIdMap::Instance();
  EXPECT_EQ(map.Size(), 0);

  DocId id1 = map.GetOrAssign(StringInternStore::Intern("doc:1"));
  EXPECT_NE(id1, kInvalidDocId);
  EXPECT_EQ(map.Size(), 1);

  DocId id2 = map.GetOrAssign(StringInternStore::Intern("doc:2"));
  EXPECT_NE(id2, kInvalidDocId);
  EXPECT_NE(id1, id2);
  EXPECT_EQ(map.Size(), 2);

  // Idempotency check with direct InternedStringPtr
  InternedStringPtr ptr1 = StringInternStore::Intern("doc:1");
  DocId id1_repeat = map.GetOrAssign(ptr1);
  EXPECT_EQ(id1, id1_repeat);
  EXPECT_EQ(map.Size(), 2);
}

TEST_F(DocIdMapTest, ReverseLookupReturnsInternedPtr) {
  auto& map = DocIdMap::Instance();

  DocId id1 = map.GetOrAssign(StringInternStore::Intern("doc:alpha"));
  DocId id2 = map.GetOrAssign(StringInternStore::Intern("doc:beta"));

  const InternedStringPtr &key1 = map.GetKey(id1);
  const InternedStringPtr &key2 = map.GetKey(id2);

  EXPECT_TRUE(key1);
  EXPECT_EQ(key1->Str(), "doc:alpha");
  EXPECT_TRUE(key2);
  EXPECT_EQ(key2->Str(), "doc:beta");

  // Verify interning address identity
  InternedStringPtr expected_key1 = StringInternStore::Intern("doc:alpha");
  EXPECT_EQ(key1.RawPtr(), expected_key1.RawPtr());

  // Invalid ID check
  const InternedStringPtr &empty = map.GetKey(999999);
  EXPECT_FALSE(empty);
}

TEST_F(DocIdMapTest, RemoveAndRecycleWithBatches) {
  auto& map = DocIdMap::Instance();

  InternedStringPtr doc1 = StringInternStore::Intern("doc:1");
  InternedStringPtr doc2 = StringInternStore::Intern("doc:2");

  DocId id1 = map.GetOrAssign(doc1);
  DocId id2 = map.GetOrAssign(doc2);

  EXPECT_EQ(map.Size(), 2);
  EXPECT_EQ(map.GetDocId(doc1), id1);

  // Remove doc1 -> should unmap doc1 and recycle id1 into local batch
  EXPECT_TRUE(map.Remove(doc1));
  EXPECT_EQ(map.Size(), 1);
  EXPECT_EQ(map.GetDocId(doc1), kInvalidDocId);
  EXPECT_FALSE(map.GetKey(id1));

  // Assign new doc:3 -> should recycle id1!
  InternedStringPtr doc3 = StringInternStore::Intern("doc:3");
  DocId id3 = map.GetOrAssign(doc3);
  EXPECT_EQ(id3, id1);  // Recycled from local batch!
  EXPECT_EQ(map.Size(), 2);
  EXPECT_EQ(map.GetKey(id3)->Str(), "doc:3");
}

TEST_F(DocIdMapTest, CrossThreadBatchTransfer) {
  auto& map = DocIdMap::Instance();

  constexpr size_t total_deletions = 150; // Exceeds 128 (active + spare batch cap)
  std::vector<InternedStringPtr> doc_ptrs;
  doc_ptrs.reserve(total_deletions);

  for (size_t i = 0; i < total_deletions; ++i) {
    std::string key = "del_doc:" + std::to_string(i);
    InternedStringPtr ptr = StringInternStore::Intern(key);
    map.GetOrAssign(ptr);
    doc_ptrs.push_back(ptr);
  }
  EXPECT_EQ(map.Size(), total_deletions);

  // Thread 1: Delete all 150 documents (flushes 64-item batch to global stack)
  std::thread t1([&]() {
    for (size_t i = 0; i < total_deletions; ++i) {
      EXPECT_TRUE(map.Remove(doc_ptrs[i]));
    }
  });
  t1.join();

  EXPECT_EQ(map.Size(), 0);
  // Global stack should have received at least 1 full batch
  EXPECT_GT(map.GlobalFreeBatchesCount(), 0);

  // Thread 2: Allocate new documents -> should consume from global lock-free stack!
  std::thread t2([&]() {
    for (size_t i = 0; i < 50; ++i) {
      std::string new_key = "new_doc:" + std::to_string(i);
      DocId id = map.GetOrAssign(StringInternStore::Intern(new_key));
      EXPECT_NE(id, kInvalidDocId);
      EXPECT_LE(id, total_deletions); // Reused an existing freed DocId!
    }
  });
  t2.join();
}

TEST_F(DocIdMapTest, CrossChunkAllocationWithInterning) {
  auto& map = DocIdMap::Instance();

  constexpr size_t total_docs = DocIdMap::kChunkSize + 100;
  std::vector<DocId> ids;
  ids.reserve(total_docs);

  for (size_t i = 0; i < total_docs; ++i) {
    std::string key = "doc:" + std::to_string(i);
    DocId id = map.GetOrAssign(StringInternStore::Intern(key));
    ids.push_back(id);
  }

  EXPECT_EQ(map.Size(), total_docs);

  for (size_t i = 0; i < total_docs; ++i) {
    std::string expected_key = "doc:" + std::to_string(i);
    const InternedStringPtr &key_ptr = map.GetKey(ids[i]);
    EXPECT_EQ(key_ptr->Str(), expected_key);
  }
}

TEST_F(DocIdMapTest, ConcurrentGetOrAssignAndRecycle) {
  auto& map = DocIdMap::Instance();

  constexpr int num_threads = 16;
  constexpr int docs_per_thread = 500;
  std::vector<std::thread> threads;

  threads.reserve(num_threads);
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([t, &map]() {
      for (int i = 0; i < docs_per_thread; ++i) {
        std::string key = "thread_" + std::to_string(t) + "_doc_" + std::to_string(i);
        InternedStringPtr ptr = StringInternStore::Intern(key);
        DocId id = map.GetOrAssign(ptr);
        EXPECT_NE(id, kInvalidDocId);
        EXPECT_EQ(map.GetKey(id)->Str(), key);

        if (i % 2 == 0) {
          EXPECT_TRUE(map.Remove(ptr));
        }
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }
}

TEST_F(DocIdMapTest, ClearResetsMap) {
  auto& map = DocIdMap::Instance();

  map.GetOrAssign(StringInternStore::Intern("doc:1"));
  map.GetOrAssign(StringInternStore::Intern("doc:2"));
  EXPECT_EQ(map.Size(), 2);

  map.Clear();
  EXPECT_EQ(map.Size(), 0);

  DocId new_id = map.GetOrAssign(StringInternStore::Intern("doc:1"));
  EXPECT_EQ(new_id, 1);
}

}  // namespace valkey_search
