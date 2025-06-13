#ifndef VALKEY_SEARCH_INDEXES_TREE_WILDCARD_ITERATOR_H_
#define VALKEY_SEARCH_INDEXES_TREE_WILDCARD_ITERATOR_H_

/*

The WildCard iterator provides an iterator to words (and their postings) that
match any pattern with a single wildcard, i.e., pattern*, *pattern, or pat*tern.

Words are iterated in lexical order.

The Wildcard iterator has two underlying algorithms and it selects between the
two algorithms based on the constructor form used and/or runtime sizing
information.

Algorithm 1: Is used when there is no suffix tree OR the number of
prefix-matching words is small (exact algo here is TBD, probably some ratio w.r.t. 
the size of the suffix tree).

This algorithm iterates over a candidate list defined only by the prefix. As
each candidate is visited, the suffix is compared and if not present, the iterator
advances until the next valid suffix is found. This algorithm operates in time
O(#PrefixMatches)

Algorithm 2: Is used when a suffix tree is present and the number of
suffix-matching words is a less than the number of prefix-matching
words.

This algorithm operates by constructing a temporary RadixTree. The suffix RadixTree is used
to generate suffix-matching candidates. These candidates are filtered by their
prefix with the survivors being inserted into the temporary RadixTree which
essentially serves to sort them since the suffix-matching candidates won't be
iterated in lexical order.

This algorithm operates in time O(#SuffixMatches)

*/

#include "absl/string/string_view.h"
#include "src/text/text.h"

namespace valkey_search {
namespace text {

struct WildCardIterator : public WordIterator {
  using Posting = typename Postings::Posting;
  // Use this form when there's no suffix tree available.
  WildCardIterator(absl::string_view prefix, absl::string_view suffix,
                   const RadixTree<Postings>& prefix_tree, );

  // Use this form when a suffix tree IS available.
  WildCardIterator(absl::string_view prefix, absl::string_view suffix,
                   const RadixTree<Postings>& prefix_tree,
                   const RadixTree<Postings>& suffix_tree, );

  // Points to valid Word?
  bool Done() const override;

  // Go to next word
  void Next() override;

  // Seek forward to word that's equal or greater
  // returns true => found equal word, false => didn't find equal word
  bool SeekForward(absl::string_view word);

  // Access the iterator, will assert if !IsValid()
  absl::string_view GetWord() const override;
  Posting& GetPosting() const;

  absl::string_view GetPrefix() const { return prefix_; }
  absl::string_view GetSuffix() const { return suffix_; }

 private:
  absl::string_view prefix_;
  absl::string_view suffix_;
  // the one to iterator over, could be temporary or not....
  std::shared_ptr<RadixTree<Postings *>> radix_tree_;
};

}  // namespace text
}  // namespace valkey_search

#endif
