/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_RADIX_TREE_H
#define _VALKEY_SEARCH_INDEXES_TEXT_RADIX_TREE_H

/*

A radix tree, but with path compression. This data
structure is functionally similar to a BTree but more space and time efficient
when dealing with common prefixes of keys.

While the RadixTree operates on a word basis for the text search case the target
of the RadixTree is a Postings object which itself has multiple Keys and
Positions within it.

In addition to normal insert/delete operations, the RadixTree has a WordIterator
that supports iteration across multiple word entries that share a common prefix.
Iteration is always done in lexical order.

A Path iterator is provided that operates at the path level. It provides
iteration capabilities for interior sub-trees of the RadixTree. Functionally,
the Path iterator is provided a prefix which identifies the sub-tree to be
iterated over. The iteration is over the set of next valid characters present in
the subtree in lexical order. This iterator can be used to visit all words with
a common prefix while intelligently skipping subsets (subtrees) of words --
ideal for fuzzy matching.

Another feature of a RadixTree is the ability to provide a count of the entries
that have a common prefix in O(len(prefix)) time. This is useful in query
planning.

Even though the description of the RadixTree consistently refers to prefixes,
this implementation also supports a suffix RadixTree. A suffix RadixTree is
simply a RadixTree built by reversing the order of the characters in a string.
For suffix RadixTrees, the external interface for the strings is the same, i.e.,
it is the responsibility of the RadixTree object itself to perform any reverse
ordering that might be required, clients of this interface need not reverse
their strings.

Note that unlike most other search objects, this object is explicitly
multi-thread aware. The multi-thread usage of this object is designed to match
the time-sliced mutex, in other words, during write operations, only a small
subset of the methods are allowed. External iterators are not valid across a
write operation. Conversely, during the read cycle, all non-mutation operations
are allowed and don't require any locking.

Ideally, detection of mutation violations, stale iterators, etc. would be built
into the codebase efficiently enough to be deployed in production code.

*/

#include <deque>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <sstream>
#include <tuple>
#include <variant>

#include "absl/functional/function_ref.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"

namespace valkey_search::indexes::text {

// Needed for std::visit
template <class... Ts>
struct overloaded : Ts... {
  using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

using Byte = uint8_t;
using BytePath = std::string;

template <typename Target, bool reverse>
struct RadixTree {
  struct WordIterator;
  struct PathIterator;
  RadixTree() = default;

  //
  // This function is the only way to mutate the RadixTree, all other functions
  // are read-only. This function is explicitly multi-thread safe and is
  // designed to allow other mutations to be performed on other words and
  // targets simultaneously, with minimal collisions.
  //
  // In all cases, the mutate function is invoked once under the locking
  // provided by the RadixTree itself, so if the target objects are disjoint
  // (which is normal) then no locking is required within the mutate function
  // itself.
  //
  // The input parameter to the mutate function will be nullopt if there is no
  // entry for this word. Otherwise it will contain the value for this word. The
  // return value of the mutate function is the new value for this word. if the
  // return value is nullopt then this word is deleted from the RadixTree.
  //
  //
  void Mutate(
      absl::string_view word,
      absl::FunctionRef<std::optional<Target>(std::optional<Target>)> mutate);

  // Get the number of words that have the specified prefix in O(len(prefix))
  // time.
  size_t GetWordCount(absl::string_view prefix) const;

  // Get the length of the longest word in the RadixTree, this can be used to
  // pre-size arrays and strings that are used when iterating on this RadixTree.
  size_t GetLongestWord() const;

  // Create a word Iterator over the sequence of words that start with the
  // prefix. The iterator will automatically be positioned to the lexically
  // smallest word and will end with the last word that shares the suffix.
  WordIterator GetWordIterator(absl::string_view prefix) const;

  // Create a Path iterator at a specific starting prefix
  PathIterator GetPathIterator(absl::string_view prefix) const;

  // Returns tree structure as vector of strings
  std::vector<std::string> DebugGetTreeStrings() const;

  // Prints tree structure
  void DebugPrintTree(const std::string& label = "") const;

 private:
  /*
   * This is the first iteration of a RadixTree. It will be optimized in the
   * future, likely with multiple different representations.
   *
   * Right now there are three types of nodes:
   *    1) Leaf node that has a target and no children.
   *    2) Branching node that has between 2 and 256 children and may or may not
   *       have a target.
   *    3) Compressed node that has a single child of one or more bytes and may
   *       or may not have a target.
   *
   * Differentiating nodes that have multiple children vs a single child takes
   * inspiration from Rax in the Valkey core. An alternative would be to merge
   * compressed and branching nodes into one:
   *
   * using NodeChildren = std::variant<
   *     std::monostate,                            // Leaf node
   *     std::map<BytePath, std::unique_ptr<Node>>  // Internal node
   * >;
   *
   * For example,
   *
   *                  [compressed]
   *                  "te" |
   *                   [branching]
   *                "s" /     \ "a"
   *          [compressed]   [compressed]
   *          "ting" /           \ "m"
   *   Target <- [leaf]           [leaf] -> Target
   *
   *  would become...
   *
   *                     [node]
   *                  "te" |
   *                     [node]
   *             "sting" /     \ "am"
   *       Target <- [leaf]    [leaf] -> Target
   *
   * There is one less level to the graph, but the complexity at the internal
   * nodes has increased and will be tricky to compress into a performant,
   * compact format given the varying sized outgoing edges. We'll consider the
   * implementations carefully when we return to optimize.
   *
   */
  struct Node;
  using NodeChildren =
      std::variant<std::monostate,                             // Leaf node
                   std::map<Byte, std::unique_ptr<Node>>,      // Branching node
                   std::pair<BytePath, std::unique_ptr<Node>>  // Compressed
                                                               // node
                   >;
  struct Node {
    uint32_t sub_tree_count;
    std::optional<Target> target;
    NodeChildren children;
  };

  Node root_;

  /*
   * Trims a branch from the tree when a word ending at a leaf node is deleted
   * from the tree.
   *
   * For example, consider the tree with "xtest" and "xabc":
   *
   *                  [compressed]
   *                   "x" |
   *                   [branching]
   *                "a" /     \ "t"
   *          [compressed]   [compressed]
   *          "bc" /           \ "est"
   *   Target <- [leaf]           [leaf] -> Target
   *
   *  It would become the following after deleting "xabc"...
   *
   *                  [compressed]
   *              "xtest" |
   *                   [leaf] -> Target
   */
  void TrimBranchFromTree(absl::string_view word, std::deque<Node*>& node_path);

  // Recursive helper for DebugGetTreeStrings
  std::vector<std::string> DebugGetTreeString(const Node* node,
                                              const std::string& path,
                                              int depth, bool is_last,
                                              const std::string& prefix) const;

 public:
  //
  // The Word Iterator provides access to sequences of Words and the associated
  // Postings Object in lexical order. Currently the word iterator assumes the
  // radix tree is not mutated for the life of the iterator.
  //
  struct WordIterator {
    // Is iterator valid?
    bool Done() const;

    // Advance to next word in lexical order
    void Next();

    // Seek forward to the next word that's greater or equal to the specified
    // word. If the prefix of this word doesn't match the prefix that created
    // this iterator, then it immediately becomes invalid. The return boolean
    // indicates if the landing spot is equal to the specified word (true) or
    // greater (false).
    bool SeekForward(absl::string_view word);

    // Access the current location, asserts if !IsValid()
    absl::string_view GetWord() const;
    const Target& GetTarget() const;

   private:
    friend struct RadixTree;
    explicit WordIterator(const Node* node, absl::string_view prefix);

    using MapIterator = std::map<Byte, std::unique_ptr<Node>>::const_iterator;
    std::deque<std::tuple<uint32_t,     // depth
                          MapIterator,  // next sibling iterator
                          MapIterator   // end iterator
                          >>
        stack_;
    const Node* curr_;
    std::string word_;
  };

  //
  // The Path iterator is initialized with a prefix. It allows
  // iteration over the set of next valid characters for the prefix.
  // For each of those valid characters, the presence of a word or
  // a subtree can be interrogated.
  //
  struct PathIterator {
    // Is the iterator itself pointing to a valid node?
    bool Done() const;

    // Is there a word at the current position?
    bool IsWord() const;

    // Advance to the next character at this level of the RadixTree
    void Next();

    // Seek to the char that's greater than or equal
    // returns true if target char is present, false otherwise
    bool SeekForward(char target);

    // Is there a node under the current path?
    bool CanDescend() const;

    // Create a new PathIterator automatically descending from the current
    // position asserts if !CanDescend()
    PathIterator DescendNew() const;

    // get current Path. If IsWord is true, then there's a word here....
    absl::string_view GetPath();

    // Get the target for this word, will assert if !IsWord()
    const Target& GetTarget() const;

    // Defrag the current Node and then defrag the Postings if this points to
    // one.
    void Defrag();
  };
};

template <typename Target, bool reverse>
void RadixTree<Target, reverse>::Mutate(
    absl::string_view word,
    absl::FunctionRef<std::optional<Target>(std::optional<Target>)> mutate) {
  CHECK(!word.empty()) << "Can't mutate the target at an empty word";
  Node* n = &root_;
  absl::string_view remaining = word;
  std::deque<Node*> node_path{n};
  while (!remaining.empty()) {
    Node* next;
    std::visit(
        overloaded{
            [&](std::monostate&) {
              // Leaf case - we're at a leaf and still have more of the word
              // remaining. Create a compressed path to a new leaf node.
              std::unique_ptr<Node> new_leaf = std::make_unique<Node>(
                  Node{0, std::nullopt, std::monostate{}});
              next = new_leaf.get();
              n->children = std::pair{BytePath(remaining), std::move(new_leaf)};
              remaining.remove_prefix(remaining.length());
            },
            [&](std::map<Byte, std::unique_ptr<Node>>& children) {
              // Branch case - look for branch matching the first byte
              const auto it = children.find(remaining[0]);
              if (it != children.end()) {
                next = it->second.get();
              } else {
                // No match - create an edge to a new empty leaf node
                std::unique_ptr<Node> new_leaf = std::make_unique<Node>(
                    Node{0, std::nullopt, std::monostate{}});
                next = new_leaf.get();
                children[remaining[0]] = std::move(new_leaf);
              }
              remaining.remove_prefix(1);
            },
            [&](std::pair<BytePath, std::unique_ptr<Node>>& child) {
              // Compressed case - check amount of the path that matches
              const BytePath& path = child.first;
              size_t match = 0;
              size_t max_match = std::min(path.length(), remaining.length());
              while (match < max_match && path[match] == remaining[match]) {
                match++;
              }

              if (match == path.length()) {
                // Full match - descend into the next node
                next = child.second.get();
                remaining.remove_prefix(match);
              } else if (match == 0) {
                // No match - convert the current node to a branch node
                auto new_branches = std::map<Byte, std::unique_ptr<Node>>();

                // Create a branch for the existing path
                if (path.length() == 1) {
                  // Branch directly to the leaf
                  new_branches[path[0]] = std::move(child.second);
                } else {
                  // Create an intermediate compressed node to the leaf
                  new_branches[path[0]] = std::make_unique<Node>(
                      Node{0, std::nullopt,
                           std::pair{path.substr(1), std::move(child.second)}});
                }
                n->children = std::move(new_branches);
                // Next iteration will hit the branching path for the same node
                next = n;
              } else {
                // Partial match - split the compressed node into two at the
                // branching point
                std::unique_ptr<Node> new_node = std::make_unique<Node>(Node{
                    0, std::nullopt,
                    std::pair{path.substr(match), std::move(child.second)}});
                child.first = path.substr(0, match);
                child.second = std::move(new_node);

                // Continue from the new compressed node
                remaining.remove_prefix(match);
                next = child.second.get();
              }
            }},
        n->children);
    if (next != n) {
      n = next;
      node_path.push_back(n);
    }
  }

  std::optional<Target> new_target = mutate(n->target);

  if (new_target) {
    n->target = new_target;
  } else {
    // Delete the word from the tree
    n->target = std::nullopt;
    node_path.pop_back();

    // Reconstruct the tree
    std::visit(overloaded{
                   [&](const std::monostate&) {
                     // Leaf case - We need to trim the branch from the tree
                     TrimBranchFromTree(word, node_path);
                   },
                   [&](const std::map<Byte, std::unique_ptr<Node>>& children) {
                     // Branch case - Target node is a branching node and still
                     // belongs in the tree as is
                   },
                   [&](std::pair<BytePath, std::unique_ptr<Node>>& child) {
                     // Compressed case - If the target's parent is also a
                     // compressed node, it can now point directly to the
                     // target's child
                     const auto& parent = node_path.back();
                     if (std::holds_alternative<
                             std::pair<BytePath, std::unique_ptr<Node>>>(
                             parent->children)) {
                       auto& parent_child =
                           std::get<std::pair<BytePath, std::unique_ptr<Node>>>(
                               parent->children);
                       parent_child.first += child.first;
                       parent_child.second = std::move(child.second);
                     }
                   },
               },
               n->children);
  }
}

template <typename Target, bool reverse>
void RadixTree<Target, reverse>::TrimBranchFromTree(
    absl::string_view word, std::deque<Node*>& node_path) {
  absl::string_view remaining = word;
  bool done = false;
  while (!node_path.empty() && !done) {
    // Start at the target node's parent
    Node* n = node_path.back();
    node_path.pop_back();
    std::visit(
        overloaded{
            [&](const std::monostate&) {
              CHECK(false) << "We don't expect to hit a leaf while "
                              "traversing up the tree";
            },
            [&](std::map<Byte, std::unique_ptr<Node>>& children) {
              children.erase(remaining.back());
              if (children.size() > 1) {
                // Keep the branching node as is
              } else if (children.size() == 1) {
                // Transform into compressed node if there is now only one child
                n->children = std::pair{
                    BytePath{static_cast<char>(children.begin()->first)},
                    std::move(children.begin()->second)};

                // Now let's see if we have a new chain of compressed nodes we
                // can merge
                BytePath new_edge;

                // Find the new parent
                // Move to the next parent if the current parent doesn't have
                // a target and the next parent is a compressed node
                // (In reality we should only go up at most one level)
                Node* parent = n;
                Node* next_parent;
                while (parent->target == std::nullopt && !node_path.empty()) {
                  next_parent = node_path.back();
                  node_path.pop_back();
                  if (!std::holds_alternative<
                          std::pair<BytePath, std::unique_ptr<Node>>>(
                          next_parent->children)) {
                    break;
                  }
                  new_edge.insert(
                      0, std::get<std::pair<BytePath, std::unique_ptr<Node>>>(
                             next_parent->children)
                             .first);
                  parent = next_parent;
                }

                // Find the new child
                // Move to the next child if the current child doesn't have
                // a target and is a compressed node
                // (In reality we should only go down at most one level)
                Node* child_parent = n;
                auto& children =
                    std::get<std::pair<BytePath, std::unique_ptr<Node>>>(
                        n->children);
                new_edge += children.first;
                Node* child = children.second.get();
                while (child->target == std::nullopt &&
                       std::holds_alternative<
                           std::pair<BytePath, std::unique_ptr<Node>>>(
                           child->children)) {
                  auto& children =
                      std::get<std::pair<BytePath, std::unique_ptr<Node>>>(
                          child->children);
                  new_edge += children.first;
                  child_parent = child;
                  child = children.second.get();
                }

                // Connect the parent to the child, moving ownership of the
                // child Node to the parent
                parent->children = std::pair{
                    new_edge,
                    std::move(
                        std::get<std::pair<BytePath, std::unique_ptr<Node>>>(
                            child_parent->children)
                            .second)};
              } else {
                CHECK(false) << "We shouldn't have a branching node with "
                                "zero children";
              }
              done = true;
            },
            [&](const std::pair<BytePath, std::unique_ptr<Node>>& child) {
              if (n->target != std::nullopt || n == &root_) {
                // A compressed node with a target or the root becomes a leaf
                n->children = std::monostate{};
                done = true;
              } else {
                // We can trim the tree branch higher up
                remaining.remove_suffix(child.first.length());
              }
            },
        },
        n->children);
  }
}

template <typename Target, bool reverse>
size_t RadixTree<Target, reverse>::GetWordCount(
    absl::string_view prefix) const {
  // TODO: Implement word counting
  return 0;
}

template <typename Target, bool reverse>
size_t RadixTree<Target, reverse>::GetLongestWord() const {
  // TODO: Implement longest word calculation
  return 0;
}

template <typename Target, bool reverse>
typename RadixTree<Target, reverse>::WordIterator
RadixTree<Target, reverse>::GetWordIterator(absl::string_view prefix) const {
  const Node* n = &root_;
  absl::string_view remaining = prefix;
  bool no_match = false;
  std::string actual_prefix = std::string(prefix);

  // Find the highest node in the sub-branch that matches the prefix
  while (!remaining.empty()) {
    std::visit(
        overloaded{
            [&](const std::monostate&) { no_match = true; },
            [&](const std::map<Byte, std::unique_ptr<Node>>& children) {
              const auto it = children.find(remaining[0]);
              if (it != children.end()) {
                n = it->second.get();
                remaining.remove_prefix(1);
              } else {
                no_match = true;
                return;
              }
            },
            [&](const std::pair<BytePath, std::unique_ptr<Node>>& child) {
              const BytePath& path = child.first;
              if (remaining.starts_with(path)) {
                remaining.remove_prefix(path.length());
              } else if (path.starts_with(remaining)) {
                // The prefix is a sub-path of the current path, we need to
                // reconstruct prefix to be passed to the iterator and return
                // word found so far
                actual_prefix =
                    actual_prefix.substr(
                        0, actual_prefix.length() - remaining.length()) +
                    path;
                remaining.remove_prefix(remaining.length());
              } else {
                no_match = true;
                return;
              }
              n = child.second.get();
            }},
        n->children);
    if (no_match) {
      n = nullptr;
      break;
    }
  }
  return WordIterator(n, actual_prefix);
}

template <typename Target, bool reverse>
RadixTree<Target, reverse>::WordIterator::WordIterator(const Node* node,
                                                       absl::string_view prefix)
    : curr_(node), word_(prefix) {
  if (curr_ && !curr_->target.has_value()) {
    Next();
  }
}

template <typename Target, bool reverse>
bool RadixTree<Target, reverse>::WordIterator::Done() const {
  return curr_ == nullptr;
}

template <typename Target, bool reverse>
void RadixTree<Target, reverse>::WordIterator::Next() {
  do {
    std::visit(
        overloaded{
            [&](const std::monostate&) {
              if (stack_.empty()) {
                curr_ = nullptr;
                return;
              }
              auto const [depth, it, end_it] = stack_.back();
              stack_.pop_back();
              if (std::next(it) != end_it) {
                // There are more siblings to search
                stack_.push_back({1, std::next(it), end_it});
              } else if (!stack_.empty()) {
                std::get<0>(stack_.back()) += 1;
              }
              curr_ = it->second.get();
              word_.resize(word_.size() - depth);
              word_ += it->first;
            },
            [&](const std::map<Byte, std::unique_ptr<Node>>& children) {
              auto it = children.begin();
              if (std::next(it) != children.end()) {
                // There are more siblings to search
                stack_.push_back({1, std::next(it), children.end()});
              } else if (!stack_.empty()) {
                std::get<0>(stack_.back()) += 1;
              }
              curr_ = it->second.get();
              word_ += it->first;
            },
            [&](const std::pair<BytePath, std::unique_ptr<Node>>& child) {
              curr_ = child.second.get();
              word_ += child.first;
              if (!stack_.empty()) {
                std::get<0>(stack_.back()) += child.first.length();
              }
            }},
        curr_->children);
  } while (curr_ && !curr_->target.has_value());
}

template <typename Target, bool reverse>
bool RadixTree<Target, reverse>::WordIterator::SeekForward(
    absl::string_view word) {
  throw std::logic_error("TODO");
}

template <typename Target, bool reverse>
absl::string_view RadixTree<Target, reverse>::WordIterator::GetWord() const {
  return word_;
}

template <typename Target, bool reverse>
const Target& RadixTree<Target, reverse>::WordIterator::GetTarget() const {
  return curr_->target.value();
}

/*** PathIterator ***/

template <typename Target, bool reverse>
bool RadixTree<Target, reverse>::PathIterator::Done() const {
  throw std::logic_error("TODO");
}

template <typename Target, bool reverse>
bool RadixTree<Target, reverse>::PathIterator::IsWord() const {
  throw std::logic_error("TODO");
}

template <typename Target, bool reverse>
void RadixTree<Target, reverse>::PathIterator::Next() {
  throw std::logic_error("TODO");
}

template <typename Target, bool reverse>
bool RadixTree<Target, reverse>::PathIterator::SeekForward(char target) {
  throw std::logic_error("TODO");
}

template <typename Target, bool reverse>
bool RadixTree<Target, reverse>::PathIterator::CanDescend() const {
  throw std::logic_error("TODO");
}

template <typename Target, bool reverse>
typename RadixTree<Target, reverse>::PathIterator
RadixTree<Target, reverse>::PathIterator::DescendNew() const {
  throw std::logic_error("TODO");
}

template <typename Target, bool reverse>
absl::string_view RadixTree<Target, reverse>::PathIterator::GetPath() {
  throw std::logic_error("TODO");
}

template <typename Target, bool reverse>
const Target& RadixTree<Target, reverse>::PathIterator::GetTarget() const {
  throw std::logic_error("TODO");
}

template <typename Target, bool reverse>
void RadixTree<Target, reverse>::PathIterator::Defrag() {
  throw std::logic_error("TODO");
}

template <typename Target, bool reverse>
std::vector<std::string> RadixTree<Target, reverse>::DebugGetTreeString(
    const Node* node, const std::string& path, int depth, bool is_last,
    const std::string& prefix) const {
  std::vector<std::string> result;

  // Build tree connector: └── for last child, ├── for others
  std::string connector =
      depth == 0 ? "" : prefix + (is_last ? "└── " : "├── ");
  std::string line = connector + "\"" + path + "\"";

  std::visit(
      overloaded{[&](const std::monostate&) {
                   line += " LEAF";
                   if (node->target.has_value()) line += " [T]";
                   result.push_back(line);
                 },
                 [&](const std::map<Byte, std::unique_ptr<Node>>& children) {
                   line += " BRANCH(" + std::to_string(children.size()) + ")";
                   if (node->target.has_value()) line += " [T]";
                   result.push_back(line);
                   // Prepare prefix for children: spaces for last, │ for
                   // continuing
                   std::string child_prefix =
                       depth == 0 ? "" : prefix + (is_last ? "    " : "│   ");
                   auto it = children.begin();
                   for (size_t i = 0; i < children.size(); ++i, ++it) {
                     auto child_result = DebugGetTreeString(
                         it->second.get(), path + char(it->first), depth + 1,
                         i == children.size() - 1, child_prefix);
                     result.insert(result.end(), child_result.begin(),
                                   child_result.end());
                   }
                 },
                 [&](const std::pair<BytePath, std::unique_ptr<Node>>& child) {
                   line += " COMPRESSED";
                   if (node->target.has_value()) line += " [T]";
                   result.push_back(line);
                   std::string child_prefix =
                       depth == 0 ? "" : prefix + (is_last ? "    " : "│   ");
                   // Compressed nodes have only one child, so it's always last
                   auto child_result = DebugGetTreeString(
                       child.second.get(), path + child.first, depth + 1, true,
                       child_prefix);
                   result.insert(result.end(), child_result.begin(),
                                 child_result.end());
                 }},
      node->children);

  return result;
}

template <typename Target, bool reverse>
std::vector<std::string> RadixTree<Target, reverse>::DebugGetTreeStrings()
    const {
  return DebugGetTreeString(&root_, "", 0, true, "");
}

template <typename Target, bool reverse>
void RadixTree<Target, reverse>::DebugPrintTree(
    const std::string& label) const {
  std::cout << "\n=== Tree Structure" << (label.empty() ? "" : (" - " + label))
            << " ===" << std::endl;

  auto structure_lines = DebugGetTreeStrings();
  for (const auto& line : structure_lines) {
    std::cout << line << std::endl;
  }

  std::cout << "=== End Structure ===\n" << std::endl;
}

}  // namespace valkey_search::indexes::text

#endif
