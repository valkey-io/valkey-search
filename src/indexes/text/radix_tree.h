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
    n = next;
  }

  std::optional<Target> new_target = mutate(n->target);

  if (new_target) {
    n->target = new_target;
  } else {
    // TODO: delete word from the tree
    assert(false);
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
  return WordIterator(n, prefix);
}

template <typename Target, bool reverse>
typename RadixTree<Target, reverse>::PathIterator
RadixTree<Target, reverse>::GetPathIterator(absl::string_view prefix) const {
  // TODO: Implement GetPathIterator
  return PathIterator();
}

/*** WordIterator ***/

template <typename Target, bool reverse>
RadixTree<Target, reverse>::WordIterator::WordIterator(const Node* node, absl::string_view prefix)
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

}  // namespace valkey_search::indexes::text

#endif

