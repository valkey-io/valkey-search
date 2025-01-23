/*
 * Copyright (c) 2025, ValkeySearch contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <cstddef>
#include <vector>

#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

std::vector<RedisModuleString *> ToRedisStringVector(
    absl::string_view params_str, absl::string_view exclude = "") {
  std::vector<absl::string_view> params =
      absl::StrSplit(params_str, ' ', absl::SkipEmpty());
  std::vector<RedisModuleString *> ret;
  for (size_t i = 0; i < params.size(); i += 2) {
    if (exclude == params[i]) {
      continue;
    }
    ret.push_back(
        RedisModule_CreateString(nullptr, params[i].data(), params[i].size()));
    if (i + 1 == params.size()) {
      break;
    }
    ret.push_back(RedisModule_CreateString(nullptr, params[i + 1].data(),
                                           params[i + 1].size()));
  }
  return ret;
}

}  // namespace vmsdk
