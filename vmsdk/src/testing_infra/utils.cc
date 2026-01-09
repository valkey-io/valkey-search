/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <cstddef>
#include <vector>

#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

std::vector<ValkeyModuleString *> ToValkeyStringVector(
    absl::string_view params_str, absl::string_view exclude) {
  std::vector<absl::string_view> params =
      absl::StrSplit(params_str, ' ', absl::SkipEmpty());
  std::vector<ValkeyModuleString *> ret;
  for (size_t i = 0; i < params.size(); i += 2) {
    if (exclude == params[i]) {
      continue;
    }
    ret.push_back(
        ValkeyModule_CreateString(nullptr, params[i].data(), params[i].size()));
    if (i + 1 == params.size()) {
      break;
    }
    ret.push_back(ValkeyModule_CreateString(nullptr, params[i + 1].data(),
                                            params[i + 1].size()));
  }
  return ret;
}

}  // namespace vmsdk
