#ifndef _VALKEY_SEARCH_INDEXES_TEXT_TEXT_H
#define _VALKSY_SEARCH_INDEXES_TEXT_TEXT_H

/*

External API for text subsystem

*/

#include <concepts>
#include <memory>

#include "src/utils/string_interning.h"

namespace valkey_search {
namespace text {

using Key = InternedString;
using Position = uint32_t;

using Byte = uint8_t;
using Char = uint32_t;

//
// Most of the Text index works over the abstract type "Postings".
// While currently there is only a single concrete implementation of Postings,
// in the future there could be two implementations.  One for inter-key index
// and a different type for intra-key index.
//
// This concept spells out the requirements for Postings
//
template <typename Postings>
concept PostingsContainer = requires(Postings c, typename Postings::Posting p) {
  // Postings is default constructable
  c{};
  // Posting is copy constructable
  typename Postings::Posting{p};
  // Postings can add a posting
  void(c.AddPosting(p));
  // Can remove a posting
  c.RemovePosting(p);
  // Can test for empty
  c.Empty()->std::same_as<bool>;
};

}  // namespace text
}  // namespace valkey_search

#endif