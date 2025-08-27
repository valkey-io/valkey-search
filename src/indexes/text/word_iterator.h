#ifndef VALKEY_SEARCH_INDEXES_TEXT_WORD_ITERATOR_H_
#define VALKEY_SEARCH_INDEXES_TEXT_WORD_ITERATOR_H_

#include <memory>

#include "absl/strings/string_view.h"

namespace valkey_search {
namespace text {

/*

Base Class for all Word Iterators. currently this includes WildCard and Fuzzy,
more may come in the future.

*/
struct WordIterator {
  virtual bool Done() const = 0;
  virtual void Next() = 0;
  virtual absl::string_view GetWord() const = 0;
  virtual std::unique_ptr<WordIterator> Clone() const = 0;

  absl::string_view operator*() const = {return GetWord();
}
};  // namespace text

}  // namespace valkey_search
}
#endif
