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

#ifndef VALKEYSEARCH_SRC_COMMANDS_COMMANDS_H_
#define VALKEYSEARCH_SRC_COMMANDS_COMMANDS_H_

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

enum FTCommand {
  kCreate,
  kDropIndex,
  kInfo,
  kList,
  kSearch,
};

constexpr absl::string_view kSearchCategory{"@search"};
constexpr absl::string_view kReadCategory{"@read"};
constexpr absl::string_view kWriteCategory{"@write"};
constexpr absl::string_view kFastCategory{"@fast"};
constexpr absl::string_view kSlowCategory{"@slow"};
constexpr absl::string_view kAdminCategory{"@admin"};

constexpr absl::string_view kCreateCommand{"FT.CREATE"};
constexpr absl::string_view kDropIndexCommand{"FT.DROPINDEX"};
constexpr absl::string_view kInfoCommand{"FT.INFO"};
constexpr absl::string_view kListCommand{"FT._LIST"};
constexpr absl::string_view kSearchCommand{"FT.SEARCH"};
constexpr absl::string_view kAggregateCommand{"FT.AGGREGATE"};

const absl::flat_hash_set<absl::string_view> kCreateCmdPermissions{
    kSearchCategory, kWriteCategory, kFastCategory};
const absl::flat_hash_set<absl::string_view> kDropIndexCmdPermissions{
    kSearchCategory, kWriteCategory, kFastCategory};
const absl::flat_hash_set<absl::string_view> kSearchCmdPermissions{
    kSearchCategory, kReadCategory, kSlowCategory};
const absl::flat_hash_set<absl::string_view> kInfoCmdPermissions{
    kSearchCategory, kReadCategory, kFastCategory};
const absl::flat_hash_set<absl::string_view> kListCmdPermissions{
    kSearchCategory, kReadCategory, kSlowCategory, kAdminCategory};

inline absl::flat_hash_set<absl::string_view> PrefixACLPermissions(
    const absl::flat_hash_set<absl::string_view> &cmd_permissions,
    absl::string_view command) {
  absl::flat_hash_set<absl::string_view> ret = cmd_permissions;
  ret.insert(command);
  return ret;
}

absl::Status FTCreateCmd(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
absl::Status FTDropIndexCmd(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc);
absl::Status FTInfoCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
absl::Status FTListCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
absl::Status FTSearchCmd(RedisModuleCtx *ctx, RedisModuleString **argv,
                         int argc);
absl::Status FTAggregateCmd(RedisModuleCtx *ctx, RedisModuleString **argv,
                            int argc);
}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_COMMANDS_COMMANDS_H_
