/*
 * Copyright (c) 2025, ValkeySearch contributors
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

#include "src/utils/patricia_tree.h"

#include <unordered_set>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace valkey_search {

namespace {

class PatriciaTreeSetTest : public testing::Test {
 protected:
  void SetUp() override {
    tree_ = new PatriciaTree<int>(false);
    tree_case_sensitive_ = new PatriciaTree<int>(true);
  }
  void TearDown() override {
    delete tree_;
    delete tree_case_sensitive_;
  }
  PatriciaTree<int> *tree_;
  PatriciaTree<int> *tree_case_sensitive_;
};

TEST_F(PatriciaTreeSetTest, SimpleAddRemoveModify) {
  tree_->AddKeyValue("", 0);
  tree_->AddKeyValue("appLe", 1);
  tree_->AddKeyValue("APp", 2);
  tree_->AddKeyValue("aPricot", 4);
  tree_->AddKeyValue("baNAna", 3);
  tree_->AddKeyValue("A", 5);
  EXPECT_THAT(*tree_->GetValue("", true), testing::UnorderedElementsAre(0));
  EXPECT_THAT(*tree_->GetValue("APPLE", true),
              testing::UnorderedElementsAre(1));
  EXPECT_THAT(*tree_->GetValue("APP", true), testing::UnorderedElementsAre(2));
  EXPECT_EQ(tree_->GetValue("no-EXIST", true), nullptr);
  EXPECT_FALSE(tree_->HasKey("NO-exist"));
  EXPECT_FALSE(tree_->Remove("no-eXIst", -1));
  EXPECT_THAT(*tree_->GetValue("APp", true), testing::UnorderedElementsAre(2));
  EXPECT_TRUE(tree_->Remove("aPP", 2));
  EXPECT_FALSE(tree_->Remove("APp", 2));
}

TEST_F(PatriciaTreeSetTest, SimpleAddRemoveModifyCaseSensitive) {
  tree_case_sensitive_->AddKeyValue("", 0);
  tree_case_sensitive_->AddKeyValue("apple", 1);
  tree_case_sensitive_->AddKeyValue("app", 2);
  tree_case_sensitive_->AddKeyValue("apricot", 4);
  tree_case_sensitive_->AddKeyValue("banana", 3);
  tree_case_sensitive_->AddKeyValue("a", 5);
  EXPECT_THAT(*tree_case_sensitive_->GetValue("", true),
              testing::UnorderedElementsAre(0));
  EXPECT_THAT(*tree_case_sensitive_->GetValue("apple", true),
              testing::UnorderedElementsAre(1));
  EXPECT_FALSE(tree_case_sensitive_->HasKey("apPLE"));
  EXPECT_THAT(*tree_case_sensitive_->GetValue("app", true),
              testing::UnorderedElementsAre(2));
  EXPECT_FALSE(tree_case_sensitive_->HasKey("apP"));
  EXPECT_EQ(tree_case_sensitive_->GetValue("no-exist", true), nullptr);
  EXPECT_FALSE(tree_case_sensitive_->HasKey("no-exist"));
  EXPECT_FALSE(tree_case_sensitive_->Remove("no-exist", -1));
  EXPECT_FALSE(tree_case_sensitive_->Remove("APP", -1));
  EXPECT_THAT(*tree_case_sensitive_->GetValue("app", true),
              testing::UnorderedElementsAre(2));
  EXPECT_TRUE(tree_case_sensitive_->Remove("app", 2));
  EXPECT_FALSE(tree_case_sensitive_->Remove("app", 2));
}

TEST_F(PatriciaTreeSetTest, PrefixMatcher) {
  tree_->AddKeyValue("apb", 1);
  tree_->AddKeyValue("apple", 1);
  tree_->AddKeyValue("app", 1);
  tree_->AddKeyValue("", 1);

  auto cnt = 0;
  auto itr = tree_->PrefixMatcher("appl");
  while (!itr.Done()) {
    EXPECT_THAT(*itr.Value()->value, testing::UnorderedElementsAre(1));
    itr.Next();
    cnt++;
  }
  EXPECT_EQ(cnt, 1);

  cnt = 0;
  itr = tree_->PrefixMatcher("a");
  while (!itr.Done()) {
    itr.Next();
    cnt++;
  }
  EXPECT_EQ(cnt, 3);

  cnt = 0;
  itr = tree_->PrefixMatcher("ab");
  while (!itr.Done()) {
    itr.Next();
    cnt++;
  }
  EXPECT_EQ(cnt, 0);

  cnt = 0;
  itr = tree_->PrefixMatcher("");
  while (!itr.Done()) {
    itr.Next();
    cnt++;
  }
  EXPECT_EQ(cnt, 4);
}

TEST_F(PatriciaTreeSetTest, GetValue) {
  tree_->AddKeyValue("apple", 2);
  tree_->AddKeyValue("app", 1);

  EXPECT_EQ(tree_->GetValue("ap", true), nullptr);
  EXPECT_THAT(*tree_->GetValue("ap", false), testing::UnorderedElementsAre(1));
  EXPECT_EQ(tree_->GetValue("ab", false), nullptr);
  EXPECT_THAT(*tree_->GetValue("apple", true),
              testing::UnorderedElementsAre(2));
  EXPECT_THAT(*tree_->GetValue("apple", false),
              testing::UnorderedElementsAre(2));
  EXPECT_EQ(tree_->GetValue("apples", false), nullptr);
  EXPECT_EQ(tree_->GetValue("apples", true), nullptr);
  EXPECT_EQ(tree_->GetValue("", true), nullptr);
  EXPECT_EQ(tree_->GetValue("", false), nullptr);
}

TEST_F(PatriciaTreeSetTest, GetQualifiedElementsCountSimple) {
  tree_->AddKeyValue("", 0);
  tree_->AddKeyValue("apple", 1);
  tree_->AddKeyValue("app", 2);
  EXPECT_EQ(tree_->GetQualifiedElementsCount("", false), 3);
  EXPECT_EQ(tree_->GetQualifiedElementsCount("", true), 1);
  EXPECT_EQ(tree_->GetQualifiedElementsCount("app", true), 1);
  EXPECT_EQ(tree_->GetQualifiedElementsCount("app", false), 2);
  EXPECT_EQ(tree_->GetQualifiedElementsCount("ab", true), 0);
  EXPECT_EQ(tree_->GetQualifiedElementsCount("ap", true), 0);
  EXPECT_EQ(tree_->GetQualifiedElementsCount("ap", false), 2);
  EXPECT_EQ(tree_->GetQualifiedElementsCount("apples", true), 0);
  EXPECT_EQ(tree_->GetQualifiedElementsCount("apples", false), 0);
  tree_->Remove("app", 2);
  EXPECT_EQ(tree_->GetQualifiedElementsCount("", false), 2);
  EXPECT_EQ(tree_->GetQualifiedElementsCount("app", true), 0);
  EXPECT_EQ(tree_->GetQualifiedElementsCount("app", false), 1);
}

TEST_F(PatriciaTreeSetTest, GetQualifiedElementsCountRemoveFailed) {
  tree_->AddKeyValue("apple", 1);
  tree_->AddKeyValue("app", 2);
  tree_->Remove("ap", 1);
  EXPECT_EQ(tree_->GetQualifiedElementsCount("", false), 2);
  EXPECT_EQ(tree_->GetQualifiedElementsCount("app", false), 2);
}

TEST_F(PatriciaTreeSetTest, TriePathIterator) {
  tree_->AddKeyValue("apple", 1);
  tree_->AddKeyValue("app", 2);
  tree_->AddKeyValue("ap", 3);
  tree_->AddKeyValue("ap", 4);
  tree_->AddKeyValue("a", 5);

  std::unordered_set<int> expected = {1, 2, 3, 4, 5};
  std::unordered_set<int> got;
  for (auto itr = tree_->PathIterator("apple"); !itr.Done(); itr.Next()) {
    got.insert(itr.Value().value->begin(), itr.Value().value->end());
  }
  EXPECT_EQ(got, expected);
}
}  // namespace
}  // namespace valkey_search
