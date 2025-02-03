#ifndef VALKEY_SEARCH_INDEXES_TREE_H_
#define VALKEY_SEARCH_INDEXES_TREE_H_

/*

The WildCard iterator provides an iterator to words (and their postings) that match any pattern
with a single wildcard, i.e., pattern*, *pattern, or pat*tern.

Words are iterated in lexical order.

*/

#include "src/text/text.h"
#include "absl/string/string_view.h"

namespace valkey_search {
namespace text {

struct WildCardIterator {
  // Use this form when there's no suffix tree available. It's slower because it may crawl over 
  // a lot of unused words
  WildCardIterator(
    absl::string_view prefix,
    absl::string_view suffix,
    const Art& prefix_tree,
  );

  // Use this form when a suffix tree IS available.
  WildCardIterator(
    absl::string_view prefix,
    absl::string_view suffix,
    const Art& prefix_tree,
    const Art& suffix_tree,
  );

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

};


}
}

#endif
