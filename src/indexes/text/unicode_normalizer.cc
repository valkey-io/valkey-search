#include "src/indexes/text/unicode_normalizer.h"

#include <limits.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <vector>

#include "unicode/brkiter.h"
#include "unicode/casemap.h"
#include "unicode/locid.h"
#include "unicode/normalizer2.h"
#include "unicode/uchar.h"
#include "unicode/uclean.h"
#include "unicode/udata.h"
#include "unicode/unistr.h"
#include "unicode/uscript.h"

namespace valkey_search::indexes::text {

void UnicodeNormalizer::CaseFoldInPlace(std::string& str) {
  icu::UnicodeString input = icu::UnicodeString::fromUTF8(str);
  input.foldCase();
  // Clear the original string and let ICU export directly into it
  str.clear();
  input.toUTF8String(str);
}
}  // namespace valkey_search::indexes::text
