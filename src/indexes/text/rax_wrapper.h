/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_RAX_WRAPPER_H
#define _VALKEY_SEARCH_INDEXES_TEXT_RAX_WRAPPER_H

/*

Wrapper for Rax, a memory-efficient radix tree.

In addition to normal insert/delete operations, the WordIterator supports
iteration across multiple word entries that share a common prefix. Iteration is
always done in lexical order.

A PathIterator API is also provided to enable fuzzy searching.

TODO: Another feature of a RadixTree is the ability to provide a count of the
entries that have a common prefix in O(len(prefix)) time. This is useful in
query planning.

*/

#include <concepts>
#include <cstdint>
#include <deque>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <sstream>
#include <tuple>
#include <type_traits>
#include <variant>

#include "absl/container/flat_hash_set.h"
#include "absl/functional/function_ref.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "rax/rax.h"
#include "src/indexes/text/invasive_ptr.h"
#include "posting.h"

namespace valkey_search::indexes::text {

// Stem tree target: maps stem root to set of parent words that stem to it
// Example: "happi" → {"happy", "happiness", "happily"}
using StemParents = absl::flat_hash_set<std::string>;

class Rax {
 public:
  class WordIterator;
  class PathIterator;

  // Constructor with optional deletion callback for targets.
  // If provided, callback will be invoked for each target during destruction.
  explicit Rax(void (*free_callback)(void *) = nullptr);
  ~Rax();

  // Move constructor and assignment
  Rax(Rax &&other) noexcept;
  Rax &operator=(Rax &&other) noexcept;

  // Delete copy constructor and assignment (Rax owns its internal state)
  Rax(const Rax &) = delete;
  Rax &operator=(const Rax &) = delete;

  //
  // Applies the mutation function to the current target of the word to generate
  // a new target. If the word doesn't already exist, a path for it will be
  // first added to the tree with a default-constructed target. The new target
  // is returned to the caller.
  //
  // The input parameter to the mutate function will be nullopt if there is no
  // entry for this word. Otherwise it will contain the value for this word. The
  // return value of the mutate function is the new value for this word. if the
  // return value is nullopt then this word is deleted from the RadixTree.
  //
  // (TODO) This function is explicitly multi-thread safe and is
  // designed to allow other mutations to be performed on other words and
  // targets simultaneously, with minimal collisions.
  //
  // In all cases, the mutate function is invoked once under the locking
  // provided by the RadixTree itself, so if the target objects are disjoint
  // (which is normal) then no locking is required within the mutate function
  // itself.
  //
  void MutateTarget(absl::string_view word,
                    absl::FunctionRef<void *(void *)> mutate);

  // Get the total number of unique words in the RadixTree (i.e. total number of
  // entries).
  size_t GetTotalUniqueWordCount() const;

  // Get the number of words that have the specified prefix in O(len(prefix))
  // time.
  size_t GetWordCount(absl::string_view prefix) const;

  // Get the length of the longest word in the RadixTree, this can be used to
  // pre-size arrays and strings that are used when iterating on this RadixTree.
  size_t GetLongestWord() const;

  // Get the allocation size tracked by rax internally.
  size_t GetAllocSize() const;

  // Check if the Rax tree is valid (not moved-from or null)
  bool IsValid() const;

  // Create a word Iterator over the sequence of words that start with the
  // prefix. The iterator will automatically be positioned to the lexically
  // smallest word and will end with the last word that shares the suffix.
  WordIterator GetWordIterator(absl::string_view prefix) const;

  // Create a Path iterator at a specific starting prefix
  PathIterator GetPathIterator(absl::string_view prefix) const;

  // Returns tree structure as vector of strings
  std::vector<std::string> DebugGetTreeStrings() const;

  // Prints tree structure
  void DebugPrintTree(const std::string &label = "") const;

 private:
  rax *rax_;  // Note: We can embed it directly to save the pointer memory if
              // the per-key text index overhead is still an issue
  void (*free_callback_)(void *);  // Optional callback for freeing targets

 public:
  //
  // The Word Iterator provides access to sequences of Words and the associated
  // Postings Object in lexical order. Currently the word iterator assumes the
  // radix tree is not mutated for the life of the iterator.
  //
  class WordIterator {
   public:
    // Constructor - seeks to prefix
    explicit WordIterator(rax *rax, absl::string_view prefix);

    // Destructor - cleans up iterator
    ~WordIterator();

    // Disabling copy and move semantics until we need them
    WordIterator(const WordIterator &) = delete;
    WordIterator &operator=(const WordIterator &) = delete;
    WordIterator(WordIterator &&other) noexcept = delete;
    WordIterator &operator=(WordIterator &&other) noexcept = delete;

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

    // Access the current location, asserts if !Done()
    absl::string_view GetWord() const;
    void *GetTarget() const;

    // Postings-specific accessor. Caller is responsible for tracking the type.
    InvasivePtr<Postings> GetPostingsTarget() const;

    // StemParents-specific accessor. Caller is responsible for tracking the type.
    InvasivePtr<StemParents> GetStemParentsTarget() const;

   private:
    friend class Rax;

    raxIterator iter_;
    std::string prefix_;
    bool done_ = false;
  };

  //
  // The Path iterator is initialized with a prefix. It allows
  // iteration over the set of next valid characters for the prefix.
  // For each of those valid characters, the presence of a word or
  // a subtree can be interrogated.
  //
  class PathIterator {
   public:
    // Constructor - navigates to prefix
    PathIterator(rax *rax, absl::string_view prefix);

    // Destructor
    ~PathIterator();

    // Default copy and move semantics
    PathIterator(const PathIterator &) = default;
    PathIterator &operator=(const PathIterator &) = default;
    PathIterator(PathIterator &&other) noexcept = default;
    PathIterator &operator=(PathIterator &&other) noexcept = default;

    // Is the iterator itself pointing to a valid node?
    bool Done() const;

    // Is there a word at the current position?
    bool IsWord() const;

    // Advance to the next character at this level of the RadixTree
    void NextChild();

    // Seek to the char that's greater than or equal
    // returns true if target char is present, false otherwise
    bool SeekForward(char target);

    // Is there a node under the current path?
    bool CanDescend() const;

    // Create a new PathIterator automatically descending from the current
    // position asserts if !CanDescend()
    PathIterator DescendNew() const;

    // Get current Path. If IsWord is true, then there's a word here....
    absl::string_view GetPath() const;

    // Get the edge label for the current child being iterated
    absl::string_view GetChildEdge();

    // Get the target for this word, will assert if !IsWord()
    void *GetTarget() const;

    // Postings-specific accessor. Caller is responsible for tracking the type.
    InvasivePtr<Postings> GetPostingsTarget() const;

    // Defrag the current Node and then defrag the Postings if this points to
    // one.
    void Defrag();

   private:
    friend class Rax;

    // Private constructor for DescendNew - directly positions at a node
    PathIterator(rax *rax, raxNode *node, std::string path);

    rax *rax_;                // Reference to the rax tree
    raxNode *node_;           // Current node we're at
    std::string path_;        // Path to current node
    size_t child_index_;      // Current child index (for branching nodes)
    bool exhausted_;          // True when all children visited
    std::string child_edge_;  // Cached edge for GetChildEdge()
  };
};

}  // namespace valkey_search::indexes::text

#endif
