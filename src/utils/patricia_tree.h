/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_PATRICIA_TREE_H_
#define VALKEYSEARCH_SRC_UTILS_PATRICIA_TREE_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stack>
#include <string>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"

namespace valkey_search {

template <typename T, typename Hasher = absl::Hash<T>,
          typename Equaler = std::equal_to<T>>
class PatriciaNode {
 public:
  PatriciaNode() = default;
  absl::flat_hash_map<std::string,
                      std::unique_ptr<PatriciaNode<T, Hasher, Equaler>>>
      children;
  int64_t subtree_values_count = 0;
  std::optional<absl::flat_hash_set<T, Hasher, Equaler>> value;
  void PrintValue() {}
};

template <typename T, typename Hasher = absl::Hash<T>,
          typename Equaler = std::equal_to<T>>
class PatriciaTree {
 public:
  using SetType = absl::flat_hash_set<T, Hasher, Equaler>;
  using PatriciaNodeType = PatriciaNode<T, Hasher, Equaler>;
  PatriciaTree(bool case_sensitive)
      : root_(std::make_unique<PatriciaNodeType>()),
        case_sensitive_(case_sensitive) {}
  void AddKeyValue(absl::string_view key, const T &value) {
    PatriciaNodeType *node = root_.get();
    absl::string_view remaining_key = key;
    while (true) {
      node->subtree_values_count++;
      if (remaining_key.empty()) {
        if (!node->value.has_value()) {
          node->value.emplace(std::move(SetType{value}));
        } else {
          node->value.value().insert(value);
        }
        return;
      }
      bool found = false;
      for (auto &[child_key, child_node] : node->children) {
        absl::string_view common_prefix =
            GetCommonPrefix(remaining_key, child_key, case_sensitive_);
        if (!common_prefix.empty()) {
          if (common_prefix.size() == child_key.size()) {
            // Continue in this branch
            node = child_node.get();
            remaining_key = remaining_key.substr(common_prefix.size());
            found = true;
            break;
          }
          // Split the edge
          auto new_node = std::make_unique<PatriciaNodeType>();
          new_node->subtree_values_count = child_node->subtree_values_count;
          new_node
              ->children[std::string(child_key.substr(common_prefix.size()))] =
              std::move(child_node);
          auto common_prefix_str = std::string(common_prefix);
          node->children.erase(std::string(child_key));
          node->children[common_prefix_str] = std::move(new_node);
          node = node->children[common_prefix_str].get();
          remaining_key = remaining_key.substr(common_prefix.size());
          found = true;
          break;
        }
      }

      if (!found) {
        auto new_node = std::make_unique<PatriciaNodeType>();
        auto new_node_ptr = new_node.get();
        node->children[std::string(remaining_key)] = std::move(new_node);
        node = new_node_ptr;
        remaining_key = "";
      }
    }
  }

  // Returns the set of values for the given key. If exact_match is false,
  // returns the set of values for the longest prefix of the key.
  SetType *GetValue(absl::string_view key, bool exact_match) const {
    PatriciaNodeType *node = GetLeafNodeForKey(key, exact_match);
    if (node == nullptr) {
      return nullptr;
    }
    return !node->value.has_value() ? nullptr : &node->value.value();
  }

  int64_t GetQualifiedElementsCount(absl::string_view key,
                                    bool exact_match) const {
    PatriciaNodeType *node = GetLeafNodeForKey(key, exact_match);
    if (node == nullptr) {
      return 0;
    }
    if (exact_match) {
      return !node->value.has_value() ? 0 : node->value.value().size();
    }
    return node->subtree_values_count;
  }

  bool HasKey(absl::string_view key) const {
    return GetValue(key, true) != nullptr;
  }

  bool Remove(absl::string_view key, const T &value) {
    auto res = RemoveHelper(root_.get(), key, value);
    return res;
  }

  // This iterator is used to iterate over all the values of a given prefix and
  // it's subtrees. In short, it will return all values that starts with a given
  // prefix.
  class PrefixSubTreeIterator {
   public:
    PrefixSubTreeIterator(PatriciaNodeType *node) {
      if (node == nullptr) {
        return;
      }
      DfsHelper(node);
    }

    bool Done() const { return values_.empty(); }

    void Next() {
      if (!values_.empty()) {
        values_.pop();
      }
    }

    PatriciaNodeType *Value() const { return values_.top(); }

   private:
    std::stack<PatriciaNodeType *> values_;

    void DfsHelper(PatriciaNodeType *node) {
      if (node->value.has_value()) {
        values_.push(node);
      }
      for (const auto &[child_key, child_node] : node->children) {
        DfsHelper(child_node.get());
      }
    }
  };

  PrefixSubTreeIterator PrefixMatcher(absl::string_view key) const {
    return PrefixSubTreeIterator(GetLeafNodeForKey(key, false));
  }

  PrefixSubTreeIterator RootIterator() const {
    return PrefixSubTreeIterator(GetLeafNodeForKey("", false));
  }

  PatriciaNodeType *ExactMatcher(absl::string_view key) const {
    return GetLeafNodeForKey(key, true);
  }

  // This iterator is used to iterate over all the values of a given prefix from
  // root to leaf.
  class TriePathIterator {
   public:
    TriePathIterator(PatriciaNodeType *root, absl::string_view str,
                     bool case_sensitive) {
      case_sensitive_ = case_sensitive;
      if (root->value != std::nullopt) {
        values_.push(root);
      }
      while (!str.empty()) {
        bool found = false;
        for (const auto &child : root->children) {
          if (str.compare(0, child.first.size(), child.first) == 0) {
            found = true;
            if (child.second->value != std::nullopt) {
              values_.push(child.second.get());
            }
            str = str.substr(child.first.size());
            root = child.second.get();
            break;
          }
        }
        if (!found) {
          return;
        }
      }
    }

    bool Done() const { return values_.empty(); }

    void Next() {
      if (!values_.empty()) {
        values_.pop();
      }
    }

    const PatriciaNodeType &Value() const { return *values_.top(); }

   private:
    std::stack<PatriciaNodeType *> values_;
    bool case_sensitive_;
  };

  TriePathIterator PathIterator(absl::string_view str) {
    return TriePathIterator(root_.get(), str, case_sensitive_);
  }

 private:
  std::unique_ptr<PatriciaNodeType> root_;
  bool case_sensitive_;

  static absl::string_view GetCommonPrefix(absl::string_view str1,
                                           absl::string_view str2,
                                           bool case_sensitive) {
    if (case_sensitive) {
      return absl::FindLongestCommonPrefix(str1, str2);
    } else {
      size_t min_length = std::min(str1.size(), str2.size());
      size_t i = 0;
      while (i < min_length &&
             (absl::ascii_tolower(str1[i]) == absl::ascii_tolower(str2[i]))) {
        ++i;
      }
      return str1.substr(0, i);
    }
  }

  bool RemoveHelper(PatriciaNodeType *node, absl::string_view key,
                    const T &value) {
    if (key.empty()) {
      if (!node->value) {
        return false;  // Key not found
      }
      auto itr = node->value.value().find(value);
      if (itr != node->value.value().end()) {
        node->value.value().erase(itr);
        node->subtree_values_count--;
        return true;
      }
      return false;
    }

    for (auto it = node->children.begin(); it != node->children.end(); ++it) {
      absl::string_view common_prefix =
          GetCommonPrefix(key, it->first, case_sensitive_);
      PatriciaNodeType *child_node = it->second.get();
      if (!common_prefix.empty() && common_prefix.size() == it->first.size()) {
        bool found =
            RemoveHelper(child_node, key.substr(common_prefix.size()), value);
        if (found) {
          node->subtree_values_count--;
          if (child_node->children.empty() &&
              (!child_node->value.has_value() ||
               child_node->value.value().empty())) {
            node->children.erase(it);
          }
          return found;
        }
      }
    }
    return false;  // Key not found
  }

  // Returns leaf node of the prefix of the key, nullptr otherwise.
  PatriciaNodeType *GetLeafNodeForKey(absl::string_view key,
                                      bool exact_match) const {
    PatriciaNodeType *node = root_.get();
    absl::string_view remaining_key = key;
    while (!remaining_key.empty()) {
      bool found = false;
      for (const auto &[child_key, child_node] : node->children) {
        auto common_prefix =
            GetCommonPrefix(remaining_key, child_key, case_sensitive_);
        if (!exact_match && common_prefix.size() == remaining_key.size()) {
          // This takes care of case where key is a prefix of a node
          // e.g. "a" matches "abc"
          DCHECK(child_key.size() >= remaining_key.size());
          node = child_node.get();
          remaining_key = "";
          found = true;
          break;
        } else if (common_prefix.size() == child_key.size()) {
          // For key "abc" This takes of care of going to node "abc"
          node = child_node.get();
          remaining_key = remaining_key.substr(common_prefix.size());
          found = true;
          break;
        }
      }
      if (!found) {
        return nullptr;
      }
    }
    return node;
  }
};
}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_PATRICIA_TREE_H_
