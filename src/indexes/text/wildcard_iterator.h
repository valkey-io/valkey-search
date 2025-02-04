#ifndef VALKEY_SEARCH_INDEXES_TREE_WILDCARD_ITERATOR_H_
#define VALKEY_SEARCH_INDEXES_TREE_WILDCARD_ITERATOR_H_

/*

The WildCard iterator provides an iterator to words (and their postings) that
match any pattern with a single wildcard, i.e., pattern*, *pattern, or pat*tern.

Words are iterated in lexical order.

The Wildcard iterator has two underlying algorithms and it selects between the
two algorithms based on the constructor form used and/or run-time sizing
information.

Algorithm 1: Is used when there is no suffix tree OR the number of
prefix-matching words is small (below a fixed threshold).

This algorithm iterates over a candidate list defined only by the prefix. As
each candidate is visited, the suffix is compared.

Algorithm 2: Is used when a suffix tree is present and the number of
suffix-matching words is a substantially less than the number of prefix-matching
words.

This algorithm operates by constructing a temporary Art. The suffix art is used
to generate suffix-matching candidates. These candidates are filtered by their
prefix with the survivors being inserted into the temporary Art which
essentially serves to sort them since the suffix-matching candidates won't be
iterated in lexical order.

*/

#include "absl/string/string_view.h"
#include "src/text/text.h"

namespace valkey_search {
namespace text {

struct WildCardIterator {
  // Use this form when there's no suffix tree available.
  WildCardIterator(absl::string_view prefix, absl::string_view suffix,
                   const Art& prefix_tree, );

  // Use this form when a suffix tree IS available.
  WildCardIterator(absl::string_view prefix, absl::string_view suffix,
                   const Art& prefix_tree, const Art& suffix_tree, );

  // Points to valid element
  bool IsValid() const;

  // Go to next word
  void NextWord();

  // Seek to word that's equal or greater
  // returns true => found equal word, false => didn't find equal word
  bool Seek(absl::string_view word);

  // Access the iterator, will assert if !IsValid()
  const Posting& GetPosting() const;
  const std::string& GetWord() const;

 private:
  std::shared_ptr<Art> art_;
  ArtIterator itr_;
};

}  // namespace text
}  // namespace valkey_search

#endif
