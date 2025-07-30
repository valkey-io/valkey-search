/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <cstddef>
#include <string>
#include <vector>

#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

std::vector<ValkeyModuleString*> ToValkeyStringVector(
    absl::string_view params_str, absl::string_view exclude) {
  std::vector<absl::string_view> params =
      absl::StrSplit(params_str, ' ', absl::SkipEmpty());
  std::vector<ValkeyModuleString*> ret;
  for (size_t i = 0; i < params.size(); ++i) {
    if (exclude == params[i]) {
      continue;
    }
    
    // Handle quote stripping (mimics CLI behavior)
    absl::string_view param = params[i];
    std::string processed_param;
    
    if (param.length() >= 2 && 
        ((param.front() == '"' && param.back() == '"') ||
         (param.front() == '\'' && param.back() == '\''))) {
          
      processed_param = std::string(param.substr(1, param.length() - 2));
      ret.push_back(ValkeyModule_CreateString(nullptr, processed_param.data(), processed_param.size()));
    } else {
      ret.push_back(ValkeyModule_CreateString(nullptr, param.data(), param.size()));
    }
  }
  return ret;
}

}  // namespace vmsdk
