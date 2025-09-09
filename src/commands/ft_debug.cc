/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <absl/base/no_destructor.h>
#include <absl/strings/ascii.h>

#include "module_config.h"
#include "src/commands/commands.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/info.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"

extern vmsdk::module::Options options;  // Declared in module_loader.cc
namespace valkey_search {

absl::Status CheckEndOfArgs(vmsdk::ArgsIterator &itr) {
  if (itr.HasNext()) {
    return absl::InvalidArgumentError("Extra arguments found on command line");
  } else {
    return absl::OkStatus();
  }
}

namespace {
static std::atomic<bool> g_fanout_force_remote_fail{false};
}

bool GetFanoutForceRemoteFail() { return g_fanout_force_remote_fail.load(); }

//
// FT._DEBUG PAUSEPOINT [ SET | RESET | TEST | LIST] <pausepoint>
//
// Connects to the vmsdk::debug mechanism
// A pausepoint is a mechanism to pause a thread at a specific location for
// testing purposes.
//
// Individual pausepoints are labelled by a unique string. No checking for the
// uniqueness is done. A pause point in the code is enabled by calling
// vmsdk::debug::PausePoint("<string>"); If that pausepoint is not set, then
// this call does nothing. But if the pausepoint is set then the calling thread
// "hangs" at that point.
//
// The TEST option can be used to determine if one or more threads are paused at
// a particular pausepoint, the number of paused threads is returned by that
// command, with 0 indicating that no threads are paused. If threads are paused,
// then by RESETing the pausepoint they are released.
//
// A typical test scenario is to enable a pause point and then trigger some
// background activity, i.e.,
//  a query, ingestion or other background activity. The test program then waits
//  until the background
// thread reaches the pause point, which the test detects by polling the pause
// point with the TEST subcommand. Once the background thread is paused, the
// test can proceed by clearing the pausepoint with the RESET subcommand.
//
// Note, a pausepoint cannot be attempted by the main thread. It's also
// recommended that it not be done while holding a mutex/lock.
//
absl::Status PausePointControlCmd(ValkeyModuleCtx *ctx,
                                  vmsdk::ArgsIterator &itr) {
  std::string keyword;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, keyword));
  keyword = absl::AsciiStrToUpper(keyword);
  if (keyword == "LIST") {
    vmsdk::debug::PausePointList(ctx);
    return absl::OkStatus();
  }
  std::string point;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, point));
  VMSDK_RETURN_IF_ERROR(CheckEndOfArgs(itr));
  if (keyword == "TEST") {
    auto result = vmsdk::debug::PausePointWaiters(point);
    if (result.ok()) {
      ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(*result));
    } else {
      ValkeyModule_ReplyWithSimpleString(ctx, result.status().message().data());
    }
  } else if (keyword == "SET" || keyword == "RESET") {
    vmsdk::debug::PausePointControl(point, keyword == "SET");
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  } else {
    ValkeyModule_ReplyWithError(
        ctx, absl::StrCat("Unknown keyword", keyword).data());
  }
  return absl::OkStatus();
}

//
// FT._DEBUG CONTROLLED_VARIABLE SET <test_control> <value>
// FT._DEBUG CONTROLLED_VARIABLE GET <test_control>
// FT._DEBUG CONTROLLED_VARIABLE LIST
//
// Connects to the vmsdk::debug CONTROLLED_VARIABLE mechanism.
//
// Note, one quirk of this command is that GET and LIST return values as
// strings, not numbers.
//
// Controlled Variables are NOT replicated
//
absl::Status ControlledCmd(ValkeyModuleCtx *ctx, vmsdk::ArgsIterator &itr) {
  std::string keyword;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, keyword));
  keyword = absl::AsciiStrToUpper(keyword);
  if (keyword == "LIST") {
    VMSDK_RETURN_IF_ERROR(CheckEndOfArgs(itr));
    auto results = vmsdk::debug::ControlledGetValues();
    ValkeyModule_ReplyWithArray(ctx, 2 * results.size());
    for (auto &r : results) {
      ValkeyModule_ReplyWithCString(ctx, r.first.data());
      ValkeyModule_ReplyWithCString(ctx, r.second.data());
    }
    return absl::OkStatus();
  }
  std::string test_control_name;
  std::string value;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, test_control_name));
  if (keyword == "GET") {
    VMSDK_ASSIGN_OR_RETURN(value,
                           vmsdk::debug::ControlledGet(test_control_name));
    VMSDK_RETURN_IF_ERROR(CheckEndOfArgs(itr));
    ValkeyModule_ReplyWithCString(ctx, value.data());
  } else if (keyword == "SET") {
    VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, value));
    VMSDK_RETURN_IF_ERROR(CheckEndOfArgs(itr));
    VMSDK_RETURN_IF_ERROR(
        vmsdk::debug::ControlledSet(test_control_name, value));
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  } else {
    ValkeyModule_ReplyWithError(
        ctx, absl::StrCat("Unknown keyword", keyword).data());
  }
  return absl::OkStatus();
}

absl::Status FanoutForceRemoteFailCmd(ValkeyModuleCtx *ctx,
                                      vmsdk::ArgsIterator &itr) {
  std::string keyword;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, keyword));
  keyword = absl::AsciiStrToUpper(keyword);
  if (keyword == "GET") {
    ValkeyModule_ReplyWithSimpleString(
        ctx, g_fanout_force_remote_fail.load() ? "yes" : "no");
  } else if (keyword == "YES" || keyword == "TRUE") {
    g_fanout_force_remote_fail.store(true);
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  } else if (keyword == "NO" || keyword == "FALSE") {
    g_fanout_force_remote_fail.store(false);
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  } else {
    ValkeyModule_ReplyWithError(
        ctx, absl::StrCat("Unknown keyword", keyword).data());
  }
  return absl::OkStatus();
}

absl::Status HelpCmd(ValkeyModuleCtx *ctx, vmsdk::ArgsIterator &itr) {
  VMSDK_RETURN_IF_ERROR(CheckEndOfArgs(itr));
  static std::vector<std::pair<std::string, std::string>> help_text{
      {"FT._DEBUG SHOW_INFO", "Show Info Variable Information"},
      {"FT._DEBUG CONTROLLED_VARIABLE SET <variable> <value>",
       "Set a controlled variable"},
      {"FT._DEBUG CONTROLLED_VARIABLE GET <variable>",
       "Get a controlled variable"},
      {"FT._DEBUG CONTROLLED_VARIABLE LIST",
       "list all controlled variables and their values"},
      {"FT._DEBUG PAUSEPOINT [ SET | RESET | TEST | LIST] <pausepoint>",
       "control pause points"},
  };
  ValkeyModule_ReplySetArrayLength(ctx, 2 * help_text.size());
  for (auto &pair : help_text) {
    ValkeyModule_ReplyWithCString(ctx, pair.first.data());
    ValkeyModule_ReplyWithCString(ctx, pair.second.data());
  }
  return absl::OkStatus();
}

absl::Status FTDebugCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                        int argc) {
  if (!vmsdk::config::IsDebugModeEnabled()) {
    // Pretend like we don't exist
    std::ostringstream msg;
    msg << "ERR unknown command '" << vmsdk::ToStringView(argv[0])
        << "', with args beginning with:";
    for (int i = 1; i < argc; ++i) {
      msg << " '" << vmsdk::ToStringView(argv[i]) << "'";
    }
    ValkeyModule_ReplyWithError(ctx, msg.str().data());
    return absl::OkStatus();
  }
  std::string msg;
  for (int i = 1; i < argc; ++i) {
    msg += " ";
    msg += std::string(vmsdk::ToStringView(argv[i]));
  }
  VMSDK_LOG(WARNING, ctx) << "FT._DEBUG" << msg;
  vmsdk::ArgsIterator itr{argv, argc};
  itr.Next();  // Skip the command name
  std::string keyword;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, keyword));
  keyword = absl::AsciiStrToUpper(keyword);
  if (keyword == "SHOW_INFO") {
    return vmsdk::info_field::ShowInfo(ctx, itr, options);
  } else if (keyword == "PAUSEPOINT") {
    return PausePointControlCmd(ctx, itr);
  } else if (keyword == "CONTROLLED_VARIABLE") {
    return ControlledCmd(ctx, itr);
  } else if (keyword == "HELP") {
    return HelpCmd(ctx, itr);
  } else if (keyword == "FANOUT_FORCE_REMOTE_FAIL") {
    return FanoutForceRemoteFailCmd(ctx, itr);
  } else {
    return absl::InvalidArgumentError(absl::StrCat(
        "Unknown subcommand: ", *itr.GetStringView(), " try HELP subcommand"));
  }
}

}  // namespace valkey_search