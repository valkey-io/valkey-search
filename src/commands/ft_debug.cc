/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <absl/base/no_destructor.h>
#include <absl/strings/ascii.h>

#include "module_config.h"
#include "src/index_schema.h"
#include "src/utils/string_interning.h"
#include "src/coordinator/metadata_manager.h"
#include "src/schema_manager.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/info.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/module_config.h"
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

/*
FT._DEBUG STRINGPOOLSTATS

Output is an array with elements:
[0] is one bucket of stats for all Inline Strings (essentially non-vectors)
[1] is one bucket of status for Out-of-Line Strings (essentially vectors)
[2] is a Histogram of all Strings by reference count.
[3] is a Histogram of all Strings by size.

A histogram is an array of entries. Each entry is an array of 2 elements.
The first element is the bucket-designator, the second element is a bucket of
strings that satisfy the bucket-designator.

The bucket-designator is a signed number. If the number is negative then this
bucket is for out-of-line strings. If the number is positive then this bucket is
for inline strings. The magnitude of the number is either the reference count or
string size.

A bucket is an array of key/value pairs:

Count: Number of strings in this bucket
Bytes: Total number of "Active" bytes (excludes allocated, but unused space)
AvgSize: Bytes / Count
Allocated: Total number of bytes allocated (including unused space)
AvgAllocated: Allocated / Count
Utilization: AvgSize / AvgAllocated

*/
void DumpBucket(ValkeyModuleCtx *ctx,
                const StringInternStore::Stats::BucketStats &bucket) {
  ValkeyModule_ReplyWithArray(ctx, 12);
  ValkeyModule_ReplyWithCString(ctx, "Count");
  ValkeyModule_ReplyWithLongLong(ctx, bucket.count_);
  ValkeyModule_ReplyWithCString(ctx, "Bytes");
  ValkeyModule_ReplyWithLongLong(ctx, bucket.bytes_);
  ValkeyModule_ReplyWithCString(ctx, "AvgSize");
  auto avg_size =
      bucket.count_ == 0 ? 0 : bucket.bytes_ / double(bucket.count_);
  ValkeyModule_ReplyWithDouble(ctx, avg_size);
  ValkeyModule_ReplyWithCString(ctx, "Allocated");
  ValkeyModule_ReplyWithLongLong(ctx, bucket.allocated_);
  ValkeyModule_ReplyWithCString(ctx, "AvgAllocated");
  auto avg_allocated =
      bucket.count_ == 0 ? 0 : bucket.allocated_ / double(bucket.count_);
  ValkeyModule_ReplyWithDouble(ctx, avg_allocated);
  auto utilization =
      (avg_size == 0 || avg_allocated == 0) ? 0 : avg_size / avg_allocated;
  ValkeyModule_ReplyWithCString(ctx, "Utilization");
  ValkeyModule_ReplyWithLongLong(ctx, int(100.0 * utilization));
}

std::ostream &operator<<(std::ostream &os,
                         const StringInternStore::Stats::BucketStats &bucket) {
  auto avg_size =
      bucket.count_ == 0 ? 0 : bucket.bytes_ / double(bucket.count_);
  auto avg_allocated =
      bucket.count_ == 0 ? 0 : bucket.allocated_ / double(bucket.count_);
  auto utilization =
      (avg_size == 0 || avg_allocated == 0) ? 0 : avg_size / avg_allocated;
  return os << "Count: " << bucket.count_ << " Bytes: " << bucket.bytes_
            << " AvgSize: " << avg_size << " Allocated: " << bucket.allocated_
            << " AvgAllocated: " << avg_allocated
            << " Utilization: " << int(100.0 * utilization) << '%';
}

absl::Status StringPoolStats(ValkeyModuleCtx *ctx, vmsdk::ArgsIterator &itr) {
  VMSDK_RETURN_IF_ERROR(CheckEndOfArgs(itr));
  auto stats = StringInternStore::Instance().GetStats();
  ValkeyModule_ReplyWithArray(ctx, 4);
  // Reply[0] -> GlobalStats
  DumpBucket(ctx, stats.inline_total_stats_);
  DumpBucket(ctx, stats.out_of_line_total_stats_);
  // Reply[1] -> ByRefcount
  ValkeyModule_ReplyWithArray(ctx, stats.by_ref_stats_.size());
  for (auto &hist : stats.by_ref_stats_) {
    ValkeyModule_ReplyWithArray(ctx, 2);
    ValkeyModule_ReplyWithLongLong(ctx, hist.first);
    DumpBucket(ctx, hist.second);
  }
  ValkeyModule_ReplyWithArray(ctx, stats.by_size_stats_.size());
  for (auto &hist : stats.by_size_stats_) {
    ValkeyModule_ReplyWithArray(ctx, 2);
    ValkeyModule_ReplyWithLongLong(ctx, hist.first);
    DumpBucket(ctx, hist.second);
  }
  //
  // Put the stats into the log
  //
  VMSDK_LOG(NOTICE, ctx) << "<<<< Start InternStringPool Stats >>>>>";
  VMSDK_LOG(NOTICE, ctx) << "Inline Total: " << stats.inline_total_stats_;
  VMSDK_LOG(NOTICE, ctx) << "OutOfLine Total: "
                         << stats.out_of_line_total_stats_;
  VMSDK_LOG(NOTICE, ctx) << "ByRefCount Buckets: "
                         << stats.by_ref_stats_.size();
  for (auto &hist : stats.by_ref_stats_) {
    if (hist.first > 0) {
      VMSDK_LOG(NOTICE, ctx)
          << "InlineRef: " << hist.first << " " << hist.second;
    } else {
      VMSDK_LOG(NOTICE, ctx)
          << "OutOfLineRef: " << -hist.first << " " << hist.second;
    }
  }
  VMSDK_LOG(NOTICE, ctx) << "BySize Buckets: " << stats.by_size_stats_.size();
  for (auto &hist : stats.by_size_stats_) {
    if (hist.first > 0) {
      VMSDK_LOG(NOTICE, ctx)
          << "InlineSize: " << hist.first << " " << hist.second;
    } else {
      VMSDK_LOG(NOTICE, ctx)
          << "OutOfLineSize: " << -hist.first << " " << hist.second;
    }
  }
  VMSDK_LOG(NOTICE, ctx) << "<<<< End InternStringPool Stats >>>>>";
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
      {"FT._DEBUG TEXTINFO <index> ...", "show info about schema-level text"},
      {"FT._DEBUG STRINGPOOLSTATS", "Show InternStringPool Stats"},
      {"FT_DEBUG SHOW_METADATA",
       "list internal metadata manager table namespace"},
      {"FT_DEBUG SHOW_INDEXSCHEMAS", "list internal index schema tables"},
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
  std::string msg;
  if (!vmsdk::config::IsDebugModeEnabled()) {
    // Pretend like we don't exist
    msg = "ERR unknown command '";
    msg += vmsdk::ToStringView(argv[0]);
    msg += "', with args beginning with:";
    for (int i = 1; i < argc; ++i) {
      msg += " '";
      msg += vmsdk::ToStringView(argv[i]);
      msg += "'";
    }
    ValkeyModule_ReplyWithError(ctx, msg.data());
    return absl::OkStatus();
  }
  for (int i = 1; i < argc; ++i) {
    msg += " ";
    msg += vmsdk::ToStringView(argv[i]);
  }
  VMSDK_LOG(WARNING, ctx) << "FT._DEBUG: " << msg;
  vmsdk::ArgsIterator itr{argv, argc};
  itr.Next();  // Skip the command name
  std::string keyword;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, keyword));
  keyword = absl::AsciiStrToUpper(keyword);
  if (keyword == "SHOW_INFO") {
    return vmsdk::info_field::ShowInfo(ctx, itr, ::options);
  } else if (keyword == "PAUSEPOINT") {
    return PausePointControlCmd(ctx, itr);
  } else if (keyword == "CONTROLLED_VARIABLE") {
    return ControlledCmd(ctx, itr);
  } else if (keyword == "STRINGPOOLSTATS") {
    return StringPoolStats(ctx, itr);
  } else if (keyword == "TEXTINFO") {
    return IndexSchema::TextInfoCmd(ctx, itr);
  } else if (keyword == "SHOW_METADATA") {
    return valkey_search::coordinator::MetadataManager::Instance().ShowMetadata(
        ctx, itr);
  } else if (keyword == "SHOW_INDEXSCHEMAS") {
    return valkey_search::SchemaManager::Instance().ShowIndexSchemas(ctx, itr);
  } else if (keyword == "HELP") {
    return HelpCmd(ctx, itr);
  } else {
    return absl::InvalidArgumentError(absl::StrCat(
        "Unknown subcommand: ", *itr.GetStringView(), " try HELP subcommand"));
  }
}

}  // namespace valkey_search
