#ifndef _VALKEY_SEARCH_INDEXES_TEXT_POSTING_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_POSTING_H_

/*

For each entry in the inverted term index, there is shared_ptr to an instance of
this structure which is used to contain the key/position information for each
word. It is expected that there will be a very large number of these objects
most of which will have only a small nmber of key/position entries. However,
there will be a small number of instances where the number of key/position
entries is quite large. Thus it's likely that the fully optimized version of
this object will have two or more encodings for its contents. This optimization
is hidden from view.

This object is NOT multi-thread safe, it's expected that the caller performs
locking for mutation operations.

Conceptually, this object is an ordered sequence of pairs: (Key, Position).
Where the ordering is lexical for keys followed by numeric for positions with
the same key. To read the contents of a posting, an iterator is used. The
general usage pattern for the iterator is:

auto itr = porting.GetIterator(); // Or GetIteratorByKey
...
while (itr.IsValid()) {
  ...
  ...itr.Get().xxx
  ...
  itr.Next();  // Or Seek, Or NextKey, or NextPositionInKey etc.
}

*/

#include "src/indexes/text/lexer.h"
#include "src/text/text.h"

namespace valkey_search::text {

struct PostingsIterator;

struct Posting {
  const Key& key_;
  Position position_;
}

struct Postings : public std::enable_shared_from_this<Postings> {
  // Construct a posting. If save_positions is off, then any keys that
  // are inserted have an assumed single position of 0.
  Postings(bool save_positions);

  // Are there any postings in this object?
  bool IsEmpty() const;

  // Add a posting
  void Add(const Posting& posting);

  // Remove a key and all positions for it
  void RemoveKey(const Key& key);

  // Total number of postings
  size_t GetPostingCount() const;

  // Total number of unique keys
  size_t GetKeyCount() const;

  // Get an iterator. At construction this will point to
  // the first entry in the posting sequence.
  PostingsIterator GetIterator() const;
};

//
// The Posting Iterator.
//
struct PostingsIterator {
  // Indicates that the iterator points to a valid place in the sequence.
  // Generally, processing continues while this is true and terminates if
  // it becomes false;
  bool IsValid() const;

  // Advance to the next position of the sequence.
  void Next();

  // Advance to the next position within the same key. If there are no
  // more positions within this key, then become invalid.
  void NextPositionInKey();

  // Advance to the first position of the next key. If there are
  // no more unique keys, then becom invalid.
  void NextKey();

  // Seek to the first key that's equal or greater than the provided key.
  // returns true if the provided key was found.
  // returns false if the provided key was NOT found and the iterator is
  // positioned to the next lexically large key.
  bool Seek(const Key& k);

  // Fetch the Posting at the current location.
  const Posting& GetPositing() const;

 private:
  friend class Postings;
  PostingsInterator(std::shard_ptr<Postings> postings);

  std::shared_ptr<Postings> postings_;
};

}  // namespace valkey_search::text

#endif