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

#ifndef VALKEYSEARCH_SRC_UTILS_SEGMENT_TREE_H_
#define VALKEYSEARCH_SRC_UTILS_SEGMENT_TREE_H_

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>

namespace valkey_search::utils {

class SegmentTree {
 private:
  struct SegmentTreeNode {
    uint64_t count;
    uint32_t height;
    double min_value;
    double max_value;
    std::unique_ptr<SegmentTreeNode> left_node;
    std::unique_ptr<SegmentTreeNode> right_node;
    bool IsLeaf() { return min_value == max_value; }
    SegmentTreeNode(uint64_t count, double min_value, double max_value,
                    std::unique_ptr<SegmentTreeNode> left_node,
                    std::unique_ptr<SegmentTreeNode> right_node, int height = 1)
        : count(count),
          height(height),
          min_value(min_value),
          max_value(max_value),
          left_node(std::move(left_node)),
          right_node(std::move(right_node)) {}
  };

 public:
  SegmentTree() = default;
  void Add(double value) { Add(value, root_); }
  bool Remove(double value) { return Remove(value, root_); }
  uint64_t CountGreaterThan(double value, bool inclusive) {
    return CountGreaterThan(value, inclusive, root_.get());
  }
  uint64_t Count(double start, double end, bool inclusive_start = false,
                 bool inclusive_end = false) {
    uint64_t count_greater_than_start =
        CountGreaterThan(start, inclusive_start);
    // Since we subtract the count of the end, we need to flip the inclusive
    // flag. This way, if the end is excluded, we include it in the count that
    // we subtract from the result.
    uint64_t count_greater_than_end = CountGreaterThan(end, !inclusive_end);
    return count_greater_than_start - count_greater_than_end;
  }
  int GetHeight() { return GetHeight(root_); }

  // Testing only
  void GetTreeString(const std::unique_ptr<SegmentTreeNode>& node,
                     std::stringstream& ss) {
    if (node == nullptr) {
      return;
    }
    ss << "Node: " << node->min_value << " " << node->max_value
       << " count: " << node->count << " height: " << node->height << std::endl;
    GetTreeString(node->left_node, ss);
    GetTreeString(node->right_node, ss);
  }
  std::string GetTreeString() {
    std::stringstream ss;
    ss << "Tree height: " << GetHeight() << std::endl;
    GetTreeString(root_, ss);
    return ss.str();
  }

 private:
  static int GetHeight(const std::unique_ptr<SegmentTreeNode>& node) {
    return node ? (node->height) : 0;
  }

  static uint64_t CountGreaterThan(double value, bool inclusive,
                                   SegmentTreeNode* node) {
    if (node == nullptr) {
      return 0;
    }
    if (inclusive ? value > node->max_value : value >= node->max_value) {
      return 0;
    }
    if (inclusive ? value <= node->min_value : value < node->min_value) {
      return node->count;
    }
    return CountGreaterThan(value, inclusive, node->left_node.get()) +
           CountGreaterThan(value, inclusive, node->right_node.get());
  }

  static void Add(double value, std::unique_ptr<SegmentTreeNode>& parent) {
    if (parent == nullptr) {
      parent =
          std::make_unique<SegmentTreeNode>(1, value, value, nullptr, nullptr);
      return;
    }
    if (parent->IsLeaf()) {
      if (parent->min_value == value) {
        parent->count++;
        return;
      }
      // Split the parent node.
      if (value < parent->min_value) {
        // Insert new parent with empty left node.
        auto new_parent = std::make_unique<SegmentTreeNode>(
            parent->count, value, parent->max_value, nullptr, std::move(parent),
            parent->height + 1);
        parent = std::move(new_parent);
        // Insert value into left node spot.
        Add(value, parent->left_node);
      } else if (value > parent->max_value) {
        // Insert new parent with empty right node.
        auto new_parent = std::make_unique<SegmentTreeNode>(
            parent->count, parent->min_value, value, std::move(parent), nullptr,
            parent->height + 1);
        parent = std::move(new_parent);
        // Insert value into right node spot.
        Add(value, parent->right_node);
      }
    } else {
      if (parent->left_node->max_value >= value) {
        Add(value, parent->left_node);
      } else if (parent->right_node->min_value <= value) {
        Add(value, parent->right_node);
      } else {
        // In some cases, we can go on either side. We choose the side based on
        // height to help with balancing.
        if (parent->left_node->height <= parent->right_node->height) {
          Add(value, parent->left_node);
        } else {
          Add(value, parent->right_node);
        }
      }
    }

    // Successfully added the value. Update the parent node.
    parent->count++;
    parent->min_value = std::min(parent->min_value, value);
    parent->max_value = std::max(parent->max_value, value);
    Rebalance(parent);
  }

  static bool Remove(double value, std::unique_ptr<SegmentTreeNode>& parent) {
    if (parent == nullptr) {
      return false;
    }
    if (parent->min_value == value && parent->max_value == value) {
      parent->count--;
      if (parent->count == 0) {
        parent = nullptr;
      }
      return true;
    }
    if (value < parent->min_value) {
      return false;
    }
    if (value > parent->max_value) {
      return false;
    }
    bool removed = false;
    if (parent->left_node != nullptr && value <= parent->left_node->max_value) {
      removed = Remove(value, parent->left_node);
    } else if (parent->right_node != nullptr &&
               value >= parent->right_node->min_value) {
      removed = Remove(value, parent->right_node);
    }
    if (!removed) {
      return false;
    }

    // Cleanup empty intermediate nodes.
    if (parent->left_node == nullptr) {
      parent = std::move(parent->right_node);
    } else if (parent->right_node == nullptr) {
      parent = std::move(parent->left_node);
    } else {
      parent->count--;
      Rebalance(parent);
    }
    return true;
  }

  static void Rebalance(std::unique_ptr<SegmentTreeNode>& parent) {
    int right_height = GetHeight(parent->right_node);
    int left_height = GetHeight(parent->left_node);
    if (left_height > right_height + 1) {
      int left_left_height = GetHeight(parent->left_node->left_node);
      int left_right_height = GetHeight(parent->left_node->right_node);
      if (left_left_height > left_right_height) {
        // Left left imbalance. Rotate right.
        RotateRight(parent);
      } else {
        // Left right imbalance. Rotate left subtree to the right, then rotate
        // right.
        RotateLeft(parent->left_node);
        RotateRight(parent);
      }
    } else if (left_height < right_height - 1) {
      int right_left_height = GetHeight(parent->right_node->left_node);
      int right_right_height = GetHeight(parent->right_node->right_node);
      if (right_left_height > right_right_height) {
        // Right left imbalance. Rotate right subtree to the right, then rotate
        // left.
        RotateRight(parent->right_node);
        RotateLeft(parent);
      } else {
        // Right right imbalance. Rotate left.
        RotateLeft(parent);
      }
    } else {
      parent->height = std::max(left_height, right_height) + 1;
    }
  }

  static void RotateRight(std::unique_ptr<SegmentTreeNode>& parent) {
    std::unique_ptr<SegmentTreeNode> new_right_left_subtree = nullptr;
    std::unique_ptr<SegmentTreeNode> new_right_right_subtree = nullptr;
    std::unique_ptr<SegmentTreeNode> new_left_subtree = nullptr;
    if (parent->left_node != nullptr) {
      if (parent->left_node->left_node != nullptr) {
        new_left_subtree = std::move(parent->left_node->left_node);
      }
      if (parent->left_node->right_node != nullptr) {
        new_right_left_subtree = std::move(parent->left_node->right_node);
      }
    }
    if (parent->right_node != nullptr) {
      new_right_right_subtree = std::move(parent->right_node);
    }
    auto new_right = std::move(parent->left_node);
    parent->left_node = std::move(new_left_subtree);
    // Repurpose the old left node as the new right node.
    new_right->count =
        new_right_left_subtree->count + new_right_right_subtree->count;
    new_right->min_value = new_right_left_subtree->min_value;
    new_right->max_value = new_right_right_subtree->max_value;
    new_right->left_node = std::move(new_right_left_subtree);
    new_right->right_node = std::move(new_right_right_subtree);
    new_right->height =
        std::max(new_right->left_node->height, new_right->right_node->height) +
        1;
    parent->right_node = std::move(new_right);
  }

  static void RotateLeft(std::unique_ptr<SegmentTreeNode>& parent) {
    std::unique_ptr<SegmentTreeNode> new_left_left_subtree = nullptr;
    std::unique_ptr<SegmentTreeNode> new_left_right_subtree = nullptr;
    std::unique_ptr<SegmentTreeNode> new_right_subtree = nullptr;
    if (parent->right_node != nullptr) {
      if (parent->right_node->right_node != nullptr) {
        new_right_subtree = std::move(parent->right_node->right_node);
      }
      if (parent->right_node->left_node != nullptr) {
        new_left_right_subtree = std::move(parent->right_node->left_node);
      }
    }
    if (parent->left_node != nullptr) {
      new_left_left_subtree = std::move(parent->left_node);
    }
    auto new_left = std::move(parent->right_node);
    parent->right_node = std::move(new_right_subtree);
    // Repurpose the old right node as the new left node.
    new_left->count =
        new_left_left_subtree->count + new_left_right_subtree->count;
    new_left->min_value = new_left_left_subtree->min_value;
    new_left->max_value = new_left_right_subtree->max_value;
    new_left->left_node = std::move(new_left_left_subtree);
    new_left->right_node = std::move(new_left_right_subtree);
    new_left->height =
        std::max(new_left->left_node->height, new_left->right_node->height) + 1;
    parent->left_node = std::move(new_left);
  }

  std::unique_ptr<SegmentTreeNode> root_;
};

}  // namespace valkey_search::utils

#endif  // VALKEYSEARCH_SRC_UTILS_SEGMENT_TREE_H_
