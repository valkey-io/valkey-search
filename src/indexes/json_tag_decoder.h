/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_JSON_TAG_DECODER_H_
#define VALKEYSEARCH_SRC_INDEXES_JSON_TAG_DECODER_H_

#include <optional>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"

namespace valkey_search::indexes {

struct JsonTags {
  enum class Status { kOk, kMissing, kInvalid };
  Status status{Status::kOk};
  std::vector<std::string> tags;
};

// Decodes the JSON module's result for a TAG field's JSONPath (the array of
// matches) the way Redis does. A single match may be a string, a boolean,
// null, or an array of strings and nulls; with several matches, each must be
// a string or null. Strings are split on `separator` with each piece trimmed,
// or kept as-is without one. Empty values produce no tag.
JsonTags DecodeJsonTags(absl::string_view json, std::optional<char> separator);

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_JSON_TAG_DECODER_H_
