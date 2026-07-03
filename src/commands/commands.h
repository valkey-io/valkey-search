/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_COMMANDS_H_
#define VALKEYSEARCH_SRC_COMMANDS_COMMANDS_H_

#include <memory>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "src/coordinator/client_pool.h"
#include "src/query/search.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/thread_pool.h"
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
constexpr absl::string_view kDangerousCategory{"@dangerous"};

constexpr absl::string_view kCreateCommand{"FT.CREATE"};
constexpr absl::string_view kDropIndexCommand{"FT.DROPINDEX"};
constexpr absl::string_view kInfoCommand{"FT.INFO"};
constexpr absl::string_view kListCommand{"FT._LIST"};
constexpr absl::string_view kSearchCommand{"FT.SEARCH"};
constexpr absl::string_view kDebugCommand{"FT._DEBUG"};
constexpr absl::string_view kAggregateCommand{"FT.AGGREGATE"};
constexpr absl::string_view kHybridCommand{"FT.HYBRID"};
constexpr absl::string_view kInternalUpdateCommand{"FT.INTERNAL_UPDATE"};

const absl::flat_hash_set<absl::string_view> kCreateCmdPermissions{
    kSearchCategory, kWriteCategory, kFastCategory};
const absl::flat_hash_set<absl::string_view> kDropIndexCmdPermissions{
    kSearchCategory, kWriteCategory, kFastCategory};
const absl::flat_hash_set<absl::string_view> kInternalUpdateCmdPermissions{
    kAdminCategory, kSearchCategory, kWriteCategory, kFastCategory};
const absl::flat_hash_set<absl::string_view> kSearchCmdPermissions{
    kSearchCategory, kReadCategory, kSlowCategory};
const absl::flat_hash_set<absl::string_view> kInfoCmdPermissions{
    kSearchCategory, kReadCategory, kFastCategory};
const absl::flat_hash_set<absl::string_view> kListCmdPermissions{
    kSearchCategory, kReadCategory, kSlowCategory, kAdminCategory};
const absl::flat_hash_set<absl::string_view> kDebugCmdPermissions{
    kSearchCategory, kSlowCategory, kAdminCategory, kDangerousCategory};

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
absl::Status FTHybridCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                         int argc);
absl::Status FTInternalUpdateCmd(ValkeyModuleCtx *ctx,
                                 ValkeyModuleString **argv, int argc);

// Generic dispatch entry shared by FT.SEARCH, FT.AGGREGATE, FT.HYBRID. The
// `Cmd` type must satisfy the duck-typed contract documented in commands.cc:
//   - Field access: index_schema_name, db_num, index_schema,
//   cancellation_token,
//     timeout_ms, enable_partial_results, local_only,
//     index_fingerprint_version, slot_fingerprint, blocked_client.
//   - Static methods:
//       static absl::Status Cmd::ParseAfterIndex(Cmd&, vmsdk::ArgsIterator&);
//       static absl::Status Cmd::ExecuteSyncLocal(ValkeyModuleCtx*,
//                                                 std::unique_ptr<Cmd>);
//       static absl::Status Cmd::DispatchLocalAsync(
//           ValkeyModuleCtx*, std::unique_ptr<Cmd>, vmsdk::ThreadPool*);
//       static absl::Status Cmd::DispatchFanoutAsync(
//           ValkeyModuleCtx*, std::unique_ptr<Cmd>,
//           std::vector<vmsdk::cluster_map::NodeInfo>&,
//           coordinator::ClientPool*, vmsdk::ThreadPool*);
// The dispatch hooks own BlockedClient setup (different commands need
// different async::Reply/Timeout/Free callbacks).
template <typename Cmd>
absl::Status ExecuteCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                            int argc, std::unique_ptr<Cmd> parameters);

//
// Common stuff for FT.SEARCH and FT.AGGREGATE command
//
struct QueryCommand : public query::SearchParameters {
  QueryCommand(int db_num) : query::SearchParameters(0, nullptr, db_num) {}
  //
  // Start of command. Thin wrapper around ExecuteCommand<QueryCommand> kept
  // for source-compat with FTSearchCmd / FTAggregateCmd call sites.
  //
  static absl::Status Execute(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                              int argc, std::unique_ptr<QueryCommand> cmd);

  // ----- Hooks consumed by ExecuteCommand<QueryCommand> -----

  // Parse the leading bare query string and forward the rest of `itr` to the
  // subclass-specific ParseCommand. Called by ExecuteCommand after index name.
  static absl::Status ParseAfterIndex(QueryCommand &cmd,
                                      vmsdk::ArgsIterator &itr);

  // Synchronous local execution path used inside MULTI/EXEC or when the
  // server does not support parallel queries. Runs query::Search inline,
  // sends the reply, and returns OkStatus.
  static absl::Status ExecuteSyncLocal(ValkeyModuleCtx *ctx,
                                       std::unique_ptr<QueryCommand> cmd);

  // Asynchronous local dispatch on the reader thread pool. Sets up the
  // blocked-client callbacks before scheduling.
  static absl::Status DispatchLocalAsync(ValkeyModuleCtx *ctx,
                                         std::unique_ptr<QueryCommand> cmd,
                                         vmsdk::ThreadPool *pool);

  // Asynchronous cluster-fanout dispatch. Sets up the blocked-client callbacks
  // before fanning out.
  static absl::Status DispatchFanoutAsync(
      ValkeyModuleCtx *ctx, std::unique_ptr<QueryCommand> cmd,
      std::vector<vmsdk::cluster_map::NodeInfo> &search_targets,
      coordinator::ClientPool *client_pool, vmsdk::ThreadPool *pool);

  //
  // Parse command (after index and query string)
  //
  virtual absl::Status ParseCommand(vmsdk::ArgsIterator &itr) = 0;
  //
  // Executed on Main Thread after merge
  //
  virtual void SendReply(ValkeyModuleCtx *ctx,
                         query::SearchResult &search_result) = 0;
  //
  // Determine if we need full results or if we can optimize with trimming
  //
  bool RequiresCompleteResults() const override = 0;
  //
  // Called when query completes.
  //
  void QueryCompleteBackground(std::unique_ptr<SearchParameters> self) override;
  void QueryCompleteMainThread(std::unique_ptr<SearchParameters> self) override;

  std::optional<vmsdk::BlockedClient> blocked_client;

 private:
  void QueryCompleteImpl(std::unique_ptr<SearchParameters> parameters);
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
