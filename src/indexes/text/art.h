#ifndef _VALKEY_SEARCH_INDEXES_TEXT_ART_H
#define _VALKEY_SEARCH_INDEXES_TEXT_ART_H

/*

Adaptive Radix Tree => Art. A radix tree, but with path compression. This data
structure is functionally similar to a BTree but more space and time efficient
when the keys have identical prefixes, which happens a lot in many languages.

While the Art operates on a word basis for the text search case the target of
the Art is a Postings object which itself has multiple Postings within it. To
plan for future fine grain locking, the insert/remove functions are done on an
individual posting, this allows the Art to properly manage simultaneous
insert/delete operations that might cause the entire Postings container to be
created/destroyed.

In addition to normal insert/delete operations, the Art
supports iteration across multiple word entries that share a common prefix.
Iteration is always done in lexical order.

Another feature of ART is the ability to provide a count of the entries that
have a common prefix in O(len(prefix)) time. This is useful in query planning.

Even though the description of the Art consistently refers to prefixes, this
implementation also supports a suffix Art. A suffix Art is simply an Art built
by reversing the order of the characters in a string. For suffix Arts, the
external interface for the strings is the same, i.e., it is the responsibilty of
the Art object to perform any reverse ordering that might be required, clients
of this interface need not reverse their strings.

*/

#include <memory>

#include "absl/strings/string_view.h"
#include "src/text/text.h"

template <PostingsContainer Postings>
struct ArtIterator;

template <PostingsContainer Postings>
struct Art : public std::enable_shared_from_this<Art<Postings>> {
  use Posting = typename Postings::Posting;
  // Construct an Art. Use either prefix or suffix ordering
  Art(bool suffix_ordered);

  // Find the postings for a word, will create one if necessary
  void AddPosting(absl::string_view word, const Posting& new_posting);

  // Remove a word from the index.
  void RemovePosting(absl::string_view word, const Posting& posting);

  // Get the number of words that have the specified prefix in O(len(prefix))
  // time.
  size_t Count(absl::string_view prefix) const;

  // Create an Iterator over the sequence of words that start with the prefix.
  // The iterator will automatically be positioned to the lexically smallest
  // word and will end with the last word that shares the suffix.
  ArtIterator GetIterator(absl::string_view prefix) const;

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

    // Access the current location, asserts if !IsValid()
    absl::string_view GetWord() const;
    Postings& operator*() const;
    Postings* operator->() const;

   private:
    friend class Art<Postings>;
    ArtIterator(std::shared_ptr<Art<Postings>> art);
    std::shared_ptr<Art<Postings>> art_;
  };
};

#endif
