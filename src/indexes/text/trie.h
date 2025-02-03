#ifndef _VALKEY_SEARCH_TEXT_TRIE_H
#define _VALKEY_SEARCH_TEXT_TRIE_H

/*

Adaptive Radix Tree => Art. A radix tree, but with path compression. This data
structure is functionally similar to a BTree but more space and time efficient
when the keys have identical prefixes, which happens a lot in many languages.

In addition to normal insert/delete operations on a word basis, the Trie
supports iteration across multiple word entries that share a common prefix.
Iteration is done in lexical order.

Another feature of ART is the ability to provide a count of the entries that
have a common prefix in O(len(prefix)) time. This is useful in query planning.

TODO: Need to think through the locking semantics of updates to the Trie and
Postings.

*/

#include <memory>

#include "absl/strings/string_view.h"
#include "src/text/text.h"

struct ArtIterator;

struct Art : public std::enable_shared_from_this<Art> {
  // Map a word into a posting. Create a new posting if the word doesn't exist.
  std::shared_ptr<Posting> FindPosting(absl::string_view word);

  // Remove a word from the index. If the posting for this word isn't empty, it
  // will assert.
  void RemovePosting(absl::string_view word);

  // Get the number of words that have the specified prefix in O(len(prefix))
  // time.
  size_t CountPostings(absl::string_view prefix) const;

  // Create an Iterator over the sequence of words that start with the prefix.
  // The iterator will automatically be positioned to the lexically smallest
  // word and will end with the last word that shares the suffix.
  ArtIterator GetIterator(absl::string_view prefix) const;
};

//
// The Trie iterator is constructed to sequence over the subset of entries in
// the Trie that share a common prefix.

struct ArtIterator {
  // Does Iterator point to a Valid Posting
  bool IsValid() const;

  // Advance to next word in lexical order
  void NextWord();

  // Seek to the next word that's greater or equal to the specified word.
  // If the prefix of this word doesn't match the prefix that created this
  // iterator, then it immediately becomes invalid.
  // The return boolean indicates if the landing spot
  // is equal to the specified word (true) or greater (false).
  bool Seek(absl::string_view word);

  // Get the current Posting. Asserts if !IsValid()
  const Posting& GetPosting() const;

 private:
  friend class Art;
  ArtIterator(std::shared_ptr<Art> art);
  std::shared_ptr<Art> art_;
};

#endif
