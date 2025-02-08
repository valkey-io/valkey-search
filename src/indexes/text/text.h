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

using Key = vmsdk::InternedStringPtr;
using Position = uint32_t;

using Byte = uint8_t;
using Char = uint32_t;

}  // namespace text
}  // namespace valkey_search

#endif