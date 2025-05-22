/*
 * Copyright (c) 2025, valkey-search contributors
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

#ifndef VMSDK_SRC_MODULETYPE_H_
#define VMSDK_SRC_MODULETYPE_H_

#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

class ModuleType {
 public:
  ModuleType(RedisModuleCtx *ctx, absl::string_view key,
             RedisModuleType *module_type);
  virtual ~ModuleType() = default;
  absl::Status Register(RedisModuleCtx *ctx);
  absl::Status Deregister(RedisModuleCtx *ctx);

  static absl::Status Register(RedisModuleCtx *ctx, absl::string_view key,
                               void *ptr, RedisModuleType *module_type);
  static absl::Status Deregister(RedisModuleCtx *ctx, absl::string_view key);

 protected:
  RedisModuleType *module_type_{nullptr};
  vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx_;
  std::string key_;
};

}  // namespace vmsdk
#endif  // VMSDK_SRC_MODULETYPE_H_
