/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef THIRD_PARTY_SNOWBALL_LANGUAGE_MAPPING_H_
#define THIRD_PARTY_SNOWBALL_LANGUAGE_MAPPING_H_

#include <unordered_map>
#include "src/index_schema.pb.h"

namespace snowball {

// Map from Language enum to snowball language strings
static const std::unordered_map<valkey_search::data_model::Language, const char*> language_map = {
  {valkey_search::data_model::LANGUAGE_UNSPECIFIED, "english"},
  {valkey_search::data_model::LANGUAGE_ENGLISH, "english"},
};

}  // namespace snowball

#endif  // THIRD_PARTY_SNOWBALL_LANGUAGE_MAPPING_H_
