/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "src/acl.h"
#include "src/commands/commands.h"
#include "src/cursor.h"
#include "src/schema_manager.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/utils.h"

namespace valkey_search {

// FT.CURSOR READ <index> <cursor_id> [COUNT <count>]
// FT.CURSOR DEL <index> <cursor_id>
absl::Status FTCursorCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                         int argc) {
  if (argc < 4) {
    return absl::InvalidArgumentError(vmsdk::WrongArity(kCursorCommand));
  }
  vmsdk::ArgsIterator itr{argv, argc};
  itr.Next();  // Skip the command name
  std::string subcommand;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, subcommand));
  subcommand = absl::AsciiStrToUpper(subcommand);
  const bool is_read = subcommand == "READ";
  if (!is_read && subcommand != "DEL") {
    return absl::InvalidArgumentError(
        absl::StrCat("Unknown subcommand: ", subcommand));
  }
  std::string index_name;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, index_name));
  uint64_t id;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, id)).SetPrepend()
      << "Bad cursor id: ";
  std::optional<int64_t> count;
  if (is_read && itr.PopIfNextIgnoreCase("COUNT")) {
    VMSDK_ASSIGN_OR_RETURN(count, ParseCursorCount(itr));
  }
  if (itr.HasNext()) {
    VMSDK_ASSIGN_OR_RETURN(auto extra, itr.GetStringView());
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected argument `", extra, "`"));
  }

  // As in Redis, the named index must exist but need not be the one the
  // cursor was created on.
  const int db_num = ValkeyModule_GetSelectedDb(ctx);
  VMSDK_RETURN_IF_ERROR(
      SchemaManager::Instance().GetIndexSchema(db_num, index_name).status());

  auto &table = CursorTable::Instance();
  Cursor *cursor = table.Lookup(id);
  if (cursor == nullptr || table.GetDbNum(id) != db_num) {
    return absl::NotFoundError(is_read
                                   ? absl::StrCat("Cursor not found, id: ", id)
                                   : "Cursor does not exist");
  }
  // The rows of a cursor are the rows of its own index, so reading or
  // releasing one needs the same key permissions FT.SEARCH and FT.AGGREGATE
  // require of that index.
  auto index_schema =
      SchemaManager::Instance().GetIndexSchema(db_num, cursor->GetIndexName());
  if (index_schema.ok()) {
    VMSDK_RETURN_IF_ERROR(AclPrefixCheck(ctx, acl::KeyAccess::kRead,
                                         (*index_schema)->GetKeyPrefixes()));
  }
  if (!is_read) {
    table.Erase(id);
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    return absl::OkStatus();
  }
  if (!index_schema.ok() || !cursor->IsSameIndex(*index_schema)) {
    table.Erase(id);
    return absl::NotFoundError(
        "The index was dropped while the cursor was idle");
  }

  table.Touch(id, absl::Now());
  if (count.has_value()) {
    cursor->SetReadCount(*count);
  }
  ValkeyModule_ReplyWithArray(ctx, 2);
  cursor->ReplyRows(ctx, *index_schema, cursor->GetReadCount());
  if (cursor->RemainingRows() > 0) {
    ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(id));
  } else {
    ValkeyModule_ReplyWithLongLong(ctx, 0);
    table.Erase(id);
  }
  return absl::OkStatus();
}

// FT._DEBUG SHOW_CURSORS: an [id, milliseconds until expiration] pair per
// cursor.
absl::Status ShowCursorsCmd(ValkeyModuleCtx *ctx) {
  auto &table = CursorTable::Instance();
  const auto now = absl::Now();
  ValkeyModule_ReplyWithArray(ctx, table.Size());
  table.ForEach([&](uint64_t id, absl::Time expiration) {
    ValkeyModule_ReplyWithArray(ctx, 2);
    ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(id));
    ValkeyModule_ReplyWithLongLong(
        ctx, std::max<int64_t>(0, absl::ToInt64Milliseconds(expiration - now)));
  });
  return absl::OkStatus();
}

}  // namespace valkey_search
