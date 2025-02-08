#ifndef _VALKEY_SEARCH_INDEXES_TEXT_FUZZY_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_FUZZY_H_

namespace valkey_search {
namespace text {

//
// Fuzzy iterator that returns keys
//
// two stage iterator. Does FuzzyWordIterator then maps that into keys
//
struct FuzzyKeyIterator : public public indexes::EntriesFetcherIteratorBase {
  FuzzyIterator(indexes::Text& index, absl::string_view word, size_t distance);
  virtual bool Done() const override;
  virtual void Next() = override;
  virtual const Key& operator*() const override;
  virtual std::unique_ptr<WordIterator> clone() const override;
};

//
// Takes a root word and iterates over the words that are a fuzzy match
//
struct FuzzyWordIterator : public WordIterator {
  FuzzyWordIterator(indexes::Text& index, absl::string_view word, size_t distance);
  virtual bool Done() const override;
  virtual void Next() override;
  virtual absl::string_view GetWord() const override;
  virtual std::unique_ptr<WordIterator> Clone() const override;

  absl::string_view operator*() const = { return GetWord(); }

}

}
}
#endif
