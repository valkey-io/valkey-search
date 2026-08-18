#include "src/utils/doc_id_map.h"

#include <atomic>
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

  DocId id1 = map.GetOrAssign("doc:1");
  EXPECT_NE(id1, kInvalidDocId);
  EXPECT_EQ(map.Size(), 1);

  DocId id2 = map.GetOrAssign("doc:2");
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

  DocId id1 = map.GetOrAssign("doc:alpha");
  DocId id2 = map.GetOrAssign("doc:beta");

  const InternedStringPtr& key1 = map.GetKey(id1);
  const InternedStringPtr& key2 = map.GetKey(id2);

  EXPECT_TRUE(key1);
  EXPECT_EQ(key1->Str(), "doc:alpha");
  EXPECT_TRUE(key2);
  EXPECT_EQ(key2->Str(), "doc:beta");

  // Verify interning address identity: key1 raw pointer matches StringInternStore address!
  InternedStringPtr expected_key1 = StringInternStore::Intern("doc:alpha");
  EXPECT_EQ(key1.RawPtr(), expected_key1.RawPtr());

  // Invalid ID check
  const InternedStringPtr& empty = map.GetKey(999999);
  EXPECT_FALSE(empty);
}

TEST_F(DocIdMapTest, GetDocIdReadOnly) {
  auto& map = DocIdMap::Instance();

  EXPECT_EQ(map.GetDocId("non_existent"), kInvalidDocId);

  DocId id = map.GetOrAssign("doc:existing");
  EXPECT_EQ(map.GetDocId("doc:existing"), id);
}

TEST_F(DocIdMapTest, CrossChunkAllocationWithInterning) {
  auto& map = DocIdMap::Instance();

  // Populate more than one chunk (kChunkSize = 4096)
  constexpr size_t total_docs = DocIdMap::kChunkSize + 100;
  std::vector<DocId> ids;
  ids.reserve(total_docs);

  for (size_t i = 0; i < total_docs; ++i) {
    std::string key = "doc:" + std::to_string(i);
    DocId id = map.GetOrAssign(key);
    ids.push_back(id);
  }

  EXPECT_EQ(map.Size(), total_docs);

  // Verify all keys reverse lookup correctly across chunk boundary
  for (size_t i = 0; i < total_docs; ++i) {
    std::string expected_key = "doc:" + std::to_string(i);
    const InternedStringPtr& key_ptr = map.GetKey(ids[i]);
    EXPECT_EQ(key_ptr->Str(), expected_key);
  }
}

TEST_F(DocIdMapTest, ConcurrentGetOrAssign) {
  auto& map = DocIdMap::Instance();

  constexpr int num_threads = 16;
  constexpr int docs_per_thread = 1000;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([t, &map]() {
      for (int i = 0; i < docs_per_thread; ++i) {
        std::string key = "thread_" + std::to_string(t) + "_doc_" + std::to_string(i);
        DocId id = map.GetOrAssign(key);
        EXPECT_NE(id, kInvalidDocId);
        EXPECT_EQ(map.GetKey(id)->Str(), key);
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  EXPECT_EQ(map.Size(), num_threads * docs_per_thread);
}

TEST_F(DocIdMapTest, ClearResetsMap) {
  auto& map = DocIdMap::Instance();

  map.GetOrAssign("doc:1");
  map.GetOrAssign("doc:2");
  EXPECT_EQ(map.Size(), 2);

  map.Clear();
  EXPECT_EQ(map.Size(), 0);
  EXPECT_EQ(map.GetDocId("doc:1"), kInvalidDocId);

  DocId new_id = map.GetOrAssign("doc:1");
  EXPECT_EQ(new_id, 1);
}

}  // namespace valkey_search
