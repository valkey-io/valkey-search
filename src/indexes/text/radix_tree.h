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
iteration is over the set of next valid characters present in the subtree.

Another feature of ART is the ability to provide a count of the entries that
have a common prefix in O(len(prefix)) time. This is useful in query planning.

Even though the description of the RadixTree consistently refers to prefixes, this
implementation also supports a suffix RadixTree. A suffix RadixTree is simply an RadixTree built
by reversing the order of the characters in a string. For suffix RadixTrees, the
external interface for the strings is the same, i.e., it is the responsibilty of
the RadixTree object to perform any reverse ordering that might be required, clients
of this interface need not reverse their strings.

Note that unlike may other objects, this object is multi-thread aware. In particular
the mutation primitives AddKey and RemoveKey collapse the 

*/

#include <memory>
#include <span>

#include "absl/strings/string_view.h"
#include "src/text/text.h"

struct RadixTree {
  struct WordIterator;
  struct PathIterator;
  // Construct an RadixTree. Use either prefix or suffix ordering
  RadixTree(bool suffix_ordered);

  // If the desginated word doesn't exist in the RadixTree it is created with an empty
  // Postings object.
  // 
  // The mutate function is called with the Postings object for this word.
  //
  // This function is mutation multi-thread safe both for mutations of the RadixTree 
  // as well as mutations of the target Postings object. 
  //
  void AddPostings(absl::string_view word, absl::invokeable<void (Postings&)> mutate);

  // Similar to AddPostings, this is multi-thread safe. Except that if the word doesn't exist
  // then it will assert out. The return value of the mutate function indicates whether
  // the Postings object has become empty and thus the word & Postings object should be deleted.
  void RemovePostings(absl::string_view word, absl::invokeable<bool (Postings&)> mutate);

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
