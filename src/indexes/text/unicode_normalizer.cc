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

std::string UnicodeNormalizer::CaseFold(absl::string_view text) {
  icu::UnicodeString input = icu::UnicodeString::fromUTF8(text);
  icu::UnicodeString folded = input.foldCase();

  std::string result;
  folded.toUTF8String(result);
  return result;
}
}  // namespace valkey_search::indexes::text
