#ifndef _VALKEY_SEARCH_TEXT_TRIE_H
#define _VALKEY_SEARCH_TEXT_TRIE_H

/*

A Trie provides a mapping from a string to a postings object (a shared
pointer).

In addition to normal insert/delete operations on a word basis, the Trie
supports iteration across multiple word entries that share a common prefix.
Iteration is always done in lexical order.

TODO: Need to think through the locking semantics of updates to the Trie and
Suffix Trie

*/

#include <memory>

#include "absl/strings/string_view.h"
#include "src/text/text.h"

struct TrieIterator;

struct Trie {
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
  TrieIterator GetIterator(absl::string_view prefix) const;
};

//
// The Trie iterator is constructed to sequence over the subset of entries in
// the Trie that share the same prefix (or suffix for reverse Tries).
struct TrieIterator {
  // Does Iterator point to a Valid Posting
  bool IsValid() const;

  // Advance to next posting within the current prefix
  void Next();

  // Advance to the specified word, or if that word doesn't exist advance to the
  // next word that's greater than the specified word. It's permitted that the
  // specified word be beyond the current prefix range. In which case the
  // iterator becomes Invalid. The return boolean indicates if the landing spot
  // is equal to the specified word (true) or greater (false).
  bool Next(absl::string_view word);

  // Get the current word. Asserts if !IsValid()
  const std::string& GetWord() const;

  // Get the posting for the current word. Asserts if !IsValid()
  std::shared_ptr<Posting> Get() const;

 private:
  // The trie for this iterator
  std::shared_ptr<Trie> trie_;
};

//
// The suffix trie is conceptually a trie implemented on the characters
// which are a reversal of the prefix trie. This allows efficient iteration over
// all words with a common suffix. However, that iteration is not performed in
// lexical order, so it's not a natural fit into the iteration hierarchy. So
// when creating an iterator over a prefix of the suffix trie, we construct a
// temporary Trie with the words and shared postings.
//
struct SuffixTrie {
  // Map a word into a posting. Create a new posting if the word doesn't exist.
  std::shared_ptr<Posting> FindPosting(absl::string_view word);

  // Remove a word from the index. If the posting for this word isn't empty, it
  // will assert.
  void RemovePosting(absl::string_view word);

  // Get the number of words that have the specified suffix in O(len(prefix))
  // time.
  size_t CountPostings(absl::string_view suffix) const;

  // Create an Iterator over the sequence of words that end with the specified
  // suffix. The words are filtered by the specified prefix and inserted into a
  // new Trie which is attached to the resulting interator. When that iterator
  // is destroyed the new Trie will also be destroyed.
  TrieIterator GetIterator(absl::string_view prefix,
                           absl::string_view suffix) const;
};

#endif
