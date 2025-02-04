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

#include "src/utils/segment_tree.h"

#include "gtest/gtest.h"

namespace valkey_search::utils {

class SegmentTreeTest : public testing::Test {
 protected:
  void SetUp() override { tree_ = new SegmentTree(); }
  void TearDown() override { delete tree_; }
  SegmentTree *tree_;
};

TEST_F(SegmentTreeTest, SimpleAdd) {
  tree_->Add(1.0);
  EXPECT_EQ(tree_->Count(0.0, 2.0), 1);
  tree_->Add(0.0);
  tree_->Add(2.0);
  EXPECT_EQ(tree_->Count(0.0, 2.0, false, false), 1);
  EXPECT_EQ(tree_->Count(0.0, 2.0, true, true), 3);
}

TEST_F(SegmentTreeTest, SimpleRemove) {
  tree_->Add(1.0);
  tree_->Add(0.0);
  tree_->Add(2.0);
  EXPECT_EQ(tree_->Count(0.0, 2.0, false, false), 1);
  EXPECT_EQ(tree_->Count(0.0, 2.0, true, true), 3);
  tree_->Remove(1.0);
  EXPECT_EQ(tree_->Count(0.0, 2.0, false, false), 0);
  EXPECT_EQ(tree_->Count(0.0, 2.0, true, true), 2);
  tree_->Remove(0.0);
  EXPECT_EQ(tree_->Count(0.0, 2.0, false, false), 0);
  EXPECT_EQ(tree_->Count(0.0, 2.0, true, true), 1);
  tree_->Remove(2.0);
  EXPECT_EQ(tree_->Count(0.0, 2.0, false, false), 0);
  EXPECT_EQ(tree_->Count(0.0, 2.0, true, true), 0);
}

TEST_F(SegmentTreeTest, AddUnbalanced) {
  for (int i = 100; i < 200; i++) {
    tree_->Add(i);
  }
  EXPECT_EQ(tree_->GetHeight(), 8);
  EXPECT_EQ(tree_->Count(100.0, 150.0, false, false), 49);
  EXPECT_EQ(tree_->Count(100.0, 150.0, true, true), 51);
  for (int i = -200; i < -100; i++) {
    tree_->Add(i);
  }
  EXPECT_EQ(tree_->GetHeight(), 9);
  for (int i = -100; i < 0; i++) {
    tree_->Add(i);
  }
  EXPECT_EQ(tree_->GetHeight(), 10);
  for (int i = 0; i < 100; i++) {
    tree_->Add(i);
  }
  EXPECT_EQ(tree_->GetHeight(), 11);
  EXPECT_EQ(tree_->Count(100.0, 150.0, false, false), 49);
  EXPECT_EQ(tree_->Count(100.0, 150.0, true, true), 51);
  EXPECT_EQ(tree_->Count(-150.0, -100.0, false, false), 49);
  EXPECT_EQ(tree_->Count(-150.0, -100.0, true, true), 51);
}

TEST_F(SegmentTreeTest, AddRemoveUnbalanced) {
  for (int i = 0; i < 100; i++) {
    tree_->Add(i);
  }
  for (int i = 0; i < 100; i++) {
    tree_->Remove(i);
  }
  EXPECT_EQ(tree_->GetHeight(), 0);
  EXPECT_EQ(tree_->Count(0.0, 50.0, false, false), 0);
  EXPECT_EQ(tree_->Count(0.0, 50.0, true, true), 0);
}

TEST_F(SegmentTreeTest, AddRemoveAdd) {
  tree_->Add(1.9);
  tree_->Remove(1.9);
  tree_->Add(2.1);
  EXPECT_EQ(tree_->Count(2.0, 3, false, false), 1);
}

TEST_F(SegmentTreeTest, LeftLeftUnbalance) {
  EXPECT_EQ(tree_->GetHeight(), 0);

  tree_->Add(0.0);

  // Tree: [0]

  EXPECT_EQ(tree_->GetHeight(), 1);

  tree_->Add(-1.0);

  // Tree: [-1,0]
  //        /  \
  //      [-1] [0]

  EXPECT_EQ(tree_->GetHeight(), 2);

  tree_->Add(-2.0);

  // Tree: [-2,0]
  //       /     \
  //   [-2, -1] [0]
  //     /   \
  //   [-2] [-1]

  EXPECT_EQ(tree_->GetHeight(), 3);

  tree_->Add(-3.0);

  // Tree: [-3,0]
  //     /        \
  // [-3, -2]  [-1, 0]
  //   /  \     /   \
  // [-3] [-2] [-1] [0]

  EXPECT_EQ(tree_->GetHeight(), 3);
}

TEST_F(SegmentTreeTest, RightRightUnbalance) {
  EXPECT_EQ(tree_->GetHeight(), 0);

  tree_->Add(0.0);

  // Tree: [0]

  EXPECT_EQ(tree_->GetHeight(), 1);

  tree_->Add(1.0);

  // Tree: [0,1]
  //        /  \
  //      [0] [1]

  EXPECT_EQ(tree_->GetHeight(), 2);
  tree_->Add(2.0);

  // Tree: [0,2]
  //       /    \
  //     [0]   [1,2]
  //           /   \
  //         [1]   [2]

  EXPECT_EQ(tree_->GetHeight(), 3);

  tree_->Add(3.0);

  // Tree:  [0,3]
  //      /       \
  //   [0, 1]    [2, 3]
  //   /    \    /     \
  // [0]    [1] [2]    [3]

  EXPECT_EQ(tree_->GetHeight(), 3);
}

TEST_F(SegmentTreeTest, RightLeftUnbalance) {
  EXPECT_EQ(tree_->GetHeight(), 0);

  tree_->Add(0.0);

  // Tree: [0]

  EXPECT_EQ(tree_->GetHeight(), 1);

  tree_->Add(1.0);

  // Tree: [0,1]
  //        /  \
  //      [0] [1]

  EXPECT_EQ(tree_->GetHeight(), 2);

  tree_->Add(2.0);

  // Tree: [0,2]
  //       /    \
  //     [0]   [1,2]
  //           /   \
  //         [1]   [2]

  EXPECT_EQ(tree_->GetHeight(), 3);

  tree_->Add(1.5);

  // Tree:  [0,3]
  //      /       \
  //   [0, 1.5]  [2, 3]
  //   /    \    /     \
  // [0]  [1.5] [2]    [3]

  EXPECT_EQ(tree_->GetHeight(), 3);
}

TEST_F(SegmentTreeTest, LeftRightUnbalance) {
  tree_->Add(0.0);

  // Tree: [0]

  EXPECT_EQ(tree_->GetHeight(), 1);

  tree_->Add(-1.0);

  // Tree: [-1,0]
  //        /  \
  //      [-1] [0]

  EXPECT_EQ(tree_->GetHeight(), 2);

  tree_->Add(-2.0);

  // Tree: [-2,0]
  //       /     \
  //   [-2, -1] [0]
  //     /   \
  //   [-2] [-1]

  EXPECT_EQ(tree_->GetHeight(), 3);

  tree_->Add(1.0);

  // Tree: [-2,1]
  //     /        \
  // [-2, -1]  [0, 1]
  //   /  \     /   \
  // [-2] [-1] [0] [1]

  EXPECT_EQ(tree_->GetHeight(), 3);

  tree_->Add(-0.1);

  // Tree:    [-2,1]
  //        /        \
  // [-2, -0.1]       [0, 1]
  //   /  \            /   \
  // [-2] [-1, -0.1]  [0] [1]
  //         /   \
  //      [-1] [-0.1]

  EXPECT_EQ(tree_->GetHeight(), 4);

  tree_->Add(-0.2);

  // Tree:      [-2,1]
  //        /            \
  // [-2, -0.1]             [0, 1]
  //   /       \             /   \
  // [-2, -1] [-0.2, -0.1]  [0] [1]
  //   /  \     /     \
  // [-2] [-1] [-0.2] [-0.1]

  EXPECT_EQ(tree_->GetHeight(), 4);

  tree_->Add(-0.15);

  // Tree:            [-2,1]
  //            /               \
  //   [-2, -0.15]              [-0.1, 1]
  //   /          \              /      \
  // [-2, -1]  [-0.2, -0.15]  [-0.1]    [0,1]
  //   /   \       /     \              /  \
  // [-2] [-1]  [-0.2]   [-0.15]      [0]   [1]

  EXPECT_EQ(tree_->GetHeight(), 4);
}

TEST_F(SegmentTreeTest, AddSameValue) {
  tree_->Add(0.0);
  tree_->Add(0.0);
  tree_->Add(0.0);
  EXPECT_EQ(tree_->Count(0.0, 0.0, true, true), 3);
  EXPECT_EQ(tree_->GetHeight(), 1);
  EXPECT_TRUE(tree_->Remove(0.0));
  EXPECT_EQ(tree_->Count(0.0, 0.0, true, true), 2);
  EXPECT_EQ(tree_->GetHeight(), 1);
  EXPECT_TRUE(tree_->Remove(0.0));
  EXPECT_TRUE(tree_->Remove(0.0));
  EXPECT_EQ(tree_->Count(0.0, 0.0, true, true), 0);
  EXPECT_EQ(tree_->GetHeight(), 0);
}

TEST_F(SegmentTreeTest, RemoveEmpty) { EXPECT_FALSE(tree_->Remove(0.0)); }

TEST_F(SegmentTreeTest, RemoveNotAdded) {
  tree_->Add(0.0);
  EXPECT_FALSE(tree_->Remove(1.0));
  EXPECT_FALSE(tree_->Remove(-1.0));
  tree_->Add(1.0);
  EXPECT_FALSE(tree_->Remove(0.5));
  EXPECT_EQ(tree_->Count(0.0, 1.0, true, true), 2);
}

TEST_F(SegmentTreeTest, RemoveRightSubtree) {
  tree_->Add(0.0);
  tree_->Add(1.0);
  EXPECT_TRUE(tree_->Remove(1.0));
  EXPECT_EQ(tree_->Count(0.0, 1.0, true, true), 1);
  EXPECT_EQ(tree_->GetHeight(), 1);
}

TEST_F(SegmentTreeTest, RemoveLeftSubtree) {
  tree_->Add(0.0);
  tree_->Add(1.0);
  EXPECT_TRUE(tree_->Remove(0.0));
  EXPECT_EQ(tree_->Count(0.0, 1.0, true, true), 1);
  EXPECT_EQ(tree_->GetHeight(), 1);
}

}  // namespace valkey_search::utils
