#ifndef _VALKEY_SEARCH_INDEXES_TEXT_RADIX_TREE_H
#define _VALKEY_SEARCH_INDEXES_TEXT_RADIX_TREE_H

/*

A radix tree, but with path compression. This data
structure is functionally similar to a BTree but more space and time efficient
when dealing with common prefixes of keys.

While the RadixTree operates on a word basis for the text search case the target of
the RadixTree is a Postings object which itself has multiple Keys and Positions within it.

In addition to normal insert/delete operations, the RadixTree has a WordIterator that
supports iteration across multiple word entries that share a common prefix.
Iteration is always done in lexical order.

A Path iterator is provided that operates at the path level. It provides iteration
capabilities for interior sub-trees of the RadixTree. Functionally, the Path iterator
is provided a prefix which identifies the sub-tree to be iterated over. The 
iteration is over the set of next valid characters present in the subtree in lexical order.
This iterator can be used to visit all words with a common prefix while intelligently skipping 
subsets (subtrees) of words -- ideal for fuzzy matching.

Another feature of a RadixTree is the ability to provide a count of the entries that
have a common prefix in O(len(prefix)) time. This is useful in query planning.

Even though the description of the RadixTree consistently refers to prefixes, this
implementation also supports a suffix RadixTree. A suffix RadixTree is simply a RadixTree built
by reversing the order of the characters in a string. For suffix RadixTrees, the
external interface for the strings is the same, i.e., it is the responsibilty of
the RadixTree object itself to perform any reverse ordering that might be required, clients
of this interface need not reverse their strings.

Note that unlike most other search objects, this object is explicitly multi-thread aware.
The multi-thread usage of this object is designed to match the time-sliced mutex, in other words,
during write operations, only a small subset of the methods are allowed. External iterators are
not valid across a write operation. Conversely, during the read cycle, all non-mutation operations
are allowed and don't require any locking.

Ideally, detection of mutation violations, stale iterators, etc. would be built into the codebase
efficiently enough to be deployed in production code.

*/

#include <memory>
#include <span>

#include "absl/strings/string_view.h"
#include "src/text/text.h"

template<typename Target, bool reverse>
struct RadixTree {
  struct WordIterator;
  struct PathIterator;
  RadixTree();

  //
  // This function is the only way to mutate the RadixTree, all other functions are read-only.
  // This function is explicitly multi-thread safe and is designed to allow other mutations to
  // be performed on other words and targets simultaneously, with minimal collisions.
  //
  // In all cases, the mutate function is invoked once under the locking provided by the RadixTree
  // itself, so if the target objects are disjoint (which is normal) then no locking is required
  // within the mutate function itself.
  //
  // The input parameter to the mutate function will be nullopt if there is no entry for this word.
  // Otherwise it will contain the value for this word.
  // The return value of the mutate function is the new value for this word. if the return
  // value is nullopt then this word is deleted from the RadixTree.
  // 
  //
  void Mutate(absl::string_view word, absl::invokeable<std::option<Target>> (std::option<Target>)> mutate);

  // Get the number of words that have the specified prefix in O(len(prefix))
  // time.
  size_t GetWordCount(absl::string_view prefix) const;

  // Get the length of the longest word in the RadixTree, this can be used to pre-size
  // arrays and strings that are used when iterating on this RadixTree.
  size_t GetLongestWord() const;

  // Create a word Iterator over the sequence of words that start with the prefix.
  // The iterator will automatically be positioned to the lexically smallest
  // word and will end with the last word that shares the suffix.
  WordIterator GetWordIterator(absl::string_view prefix) const;

  // Create a Path iterator at a specific starting prefix
  PathIterator GetPathIterator(absl::string_view prefix) const;

  //
  // The Word Iterator provides access to sequences of Words and the associated Postings Object in lexical order.
  //
  struct WordIterator {
    // Is iterator valid?
    bool Done() const;

    // Advance to next word in lexical order
    void Next();

    // Seek forward to the next word that's greater or equal to the specified word.
    // If the prefix of this word doesn't match the prefix that created this
    // iterator, then it immediately becomes invalid. The return boolean indicates if the landing spot
    // is equal to the specified word (true) or greater (false).
    bool SeekForward(absl::string_view word);

    // Access the current location, asserts if !IsValid()
    absl::string_view GetWord() const;
    Postings& GetPostings() const;

  };

  //
  // The Path iterator is initialized with a prefix. It allows
  // iteration over the set of next valid characters for the prefix.
  // For each of those valid characters, the presence of a word or
  // a subtree can be interrogated.
  //
  struct PathIterator {
    // Is the iterator iself pointing to a valid node?
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

    // Create a new PathIterator automatically descending from the current position
    // asserts if !CanDescend()
    PathIterator DescendNew() const;

    // get current Path. If IsWord is true, then there's a word here....
    absl::string_view GetPath();

    // Get Postings for this word, will assert if !IsWord()
    Postings& GetPostings() const;

    // Defrag the current Node and then defrag the Postings if this points to one.
    void Defrag();

  }
};

#endif
