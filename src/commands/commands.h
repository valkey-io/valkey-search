/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_COMMANDS_H_
#define VALKEYSEARCH_SRC_COMMANDS_COMMANDS_H_

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "src/query/search.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

enum FTCommand {
  kCreate,
  kDropIndex,
  kInfo,
  kList,
  kSearch,
  kDebug,
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
constexpr absl::string_view kDebugCommand{"FT._DEBUG"};
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
const absl::flat_hash_set<absl::string_view> kDebugCmdPermissions{
    kSearchCategory, kReadCategory, kSlowCategory, kAdminCategory};

inline absl::flat_hash_set<absl::string_view> PrefixACLPermissions(
    const absl::flat_hash_set<absl::string_view> &cmd_permissions,
    absl::string_view command) {
  absl::flat_hash_set<absl::string_view> ret = cmd_permissions;
  ret.insert(command);
  return ret;
}

absl::Status FTCreateCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                         int argc);
absl::Status FTDropIndexCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                            int argc);
absl::Status FTInfoCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                       int argc);
absl::Status FTListCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                       int argc);
absl::Status FTSearchCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                         int argc);
absl::Status FTDebugCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                        int argc);
absl::Status FTAggregateCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                            int argc);

//
// Common stuff for FT.SEARCH and FT.AGGREGATE command
//
struct QueryCommand : public query::SearchParameters {
  QueryCommand() : query::SearchParameters(0, nullptr, 0) {}
  //
  // Start of command.
  //
  static absl::Status Execute(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                              int argc, std::unique_ptr<QueryCommand> cmd);

  //
  // Parse command (after index and query string)
  //
  virtual absl::Status ParseCommand(vmsdk::ArgsIterator &itr) = 0;
  //
  // Executed on Main Thread after merge
  //
  virtual void SendReply(ValkeyModuleCtx *ctx,
                         std::deque<indexes::Neighbor> &neighbors) = 0;
};

namespace async {

int Reply(ValkeyModuleCtx *ctx, [[maybe_unused]] ValkeyModuleString **argv,
          [[maybe_unused]] int argc);

int Timeout(ValkeyModuleCtx *ctx, [[maybe_unused]] ValkeyModuleString **argv,
            [[maybe_unused]] int argc);

void Free(ValkeyModuleCtx * /*ctx*/, void *privdata);

}  // namespace async

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_COMMANDS_COMMANDS_H_
