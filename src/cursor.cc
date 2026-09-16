/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/cursor.h"

#include "absl/base/no_destructor.h"
#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "src/valkey_search.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/info.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/utils.h"

namespace valkey_search {

static absl::NoDestructor<std::unique_ptr<CursorTable>> cursor_table_instance;

static vmsdk::info_field::Integer num_cursors(
    "cursors", "num_cursors",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return CursorTable::HasInstance() ? CursorTable::Instance().Size() : 0;
    }));

static absl::StatusOr<int64_t> ParseBoundedValue(vmsdk::ArgsIterator &itr,
                                                 absl::string_view name,
                                                 int64_t max) {
  VMSDK_ASSIGN_OR_RETURN(auto text, itr.GetStringView());
  VMSDK_ASSIGN_OR_RETURN(auto value, vmsdk::ToNumeric<int64_t>(text),
                         _.SetPrepend() << "Bad " << name << " value: ");
  itr.Next();
  if (value < 1 || value > max) {
    return absl::InvalidArgumentError(
        absl::StrCat(name, " must be between 1 and ", max));
  }
  return value;
}

absl::StatusOr<int64_t> ParseCursorCount(vmsdk::ArgsIterator &itr) {
  return ParseBoundedValue(itr, "COUNT",
                           options::GetCursorMaxCount().GetValue());
}

absl::StatusOr<CursorOptions> ParseCursorOptions(vmsdk::ArgsIterator &itr) {
  CursorOptions cursor_options;
  const int64_t max_idle_ms = options::GetCursorMaxIdleMs().GetValue();
  cursor_options.max_idle =
      absl::Milliseconds(std::min(kDefaultCursorMaxIdleMs, max_idle_ms));
  while (true) {
    if (itr.PopIfNextIgnoreCase("COUNT")) {
      VMSDK_ASSIGN_OR_RETURN(cursor_options.count, ParseCursorCount(itr));
    } else if (itr.PopIfNextIgnoreCase("MAXIDLE")) {
      VMSDK_ASSIGN_OR_RETURN(auto ms,
                             ParseBoundedValue(itr, "MAXIDLE", max_idle_ms));
      cursor_options.max_idle = absl::Milliseconds(ms);
    } else {
      return cursor_options;
    }
  }
}

void CursorTable::InitInstance(std::unique_ptr<CursorTable> instance) {
  *cursor_table_instance = std::move(instance);
}

bool CursorTable::HasInstance() { return *cursor_table_instance != nullptr; }

CursorTable &CursorTable::Instance() {
  CHECK(HasInstance());
  return **cursor_table_instance;
}

uint32_t CursorTable::ComputeIdCrc(ValkeyModuleCtx *ctx) {
  auto *info = ValkeyModule_GetServerInfo(ctx, "server");
  const char *run_id =
      info ? ValkeyModule_ServerInfoGetFieldC(info, "run_id") : nullptr;
  uint32_t crc = vmsdk::Crc32(run_id ? run_id : "");
  if (info) {
    ValkeyModule_FreeServerInfo(ctx, info);
  }
  return crc;
}

CursorTable::~CursorTable() { Clear(); }

CursorTable::DbCursors &CursorTable::DbCursorsFor(int db_num) {
  auto &db = by_db_[db_num];
  if (db == nullptr) {
    db = std::make_unique<DbCursors>();
    db->db_num = db_num;
  }
  return *db;
}

uint64_t CursorTable::Insert(std::unique_ptr<Cursor> cursor, int db_num,
                             absl::Time now) {
  vmsdk::VerifyMainThread();
  uint64_t id;
  do {
    // Ids are replied as RESP integers, which are signed: the 31 bit counter
    // keeps them positive.
    counter_ = (counter_ + 1) & 0x7FFFFFFF;
    id = (uint64_t{counter_} << 32) | id_crc_;
  } while (id == 0 || cursors_.contains(id));
  auto &db = DbCursorsFor(db_num);
  db.by_index[cursor->GetIndexName()].insert(id);
  auto expiration = by_expiration_.emplace(now + cursor->GetMaxIdle(), id);
  cursors_.emplace(id, Entry{std::move(cursor), expiration, &db});
  return id;
}

Cursor *CursorTable::Lookup(uint64_t id) const {
  vmsdk::VerifyMainThread();
  auto itr = cursors_.find(id);
  return itr == cursors_.end() ? nullptr : itr->second.cursor.get();
}

int CursorTable::GetDbNum(uint64_t id) const {
  vmsdk::VerifyMainThread();
  auto itr = cursors_.find(id);
  CHECK(itr != cursors_.end());
  return itr->second.db->db_num;
}

void CursorTable::Touch(uint64_t id, absl::Time now) {
  vmsdk::VerifyMainThread();
  auto itr = cursors_.find(id);
  CHECK(itr != cursors_.end());
  auto &entry = itr->second;
  by_expiration_.erase(entry.expiration);
  entry.expiration =
      by_expiration_.emplace(now + entry.cursor->GetMaxIdle(), id);
}

void CursorTable::Erase(uint64_t id) {
  vmsdk::VerifyMainThread();
  auto itr = cursors_.find(id);
  CHECK(itr != cursors_.end());
  by_expiration_.erase(itr->second.expiration);
  auto &by_index = itr->second.db->by_index;
  auto index_itr = by_index.find(itr->second.cursor->GetIndexName());
  CHECK(index_itr != by_index.end());
  index_itr->second.erase(id);
  if (index_itr->second.empty()) {
    by_index.erase(index_itr);
  }
  auto cursor = std::move(itr->second.cursor);
  cursors_.erase(itr);
  Destroy(std::move(cursor));
}

void CursorTable::EraseIndex(int db_num, absl::string_view index_name) {
  vmsdk::VerifyMainThread();
  auto db_itr = by_db_.find(db_num);
  if (db_itr == by_db_.end()) {
    return;
  }
  auto index_itr = db_itr->second->by_index.find(index_name);
  if (index_itr == db_itr->second->by_index.end()) {
    return;
  }
  // Erase() removes entries from this set, so work from a copy of the ids.
  std::vector<uint64_t> ids(index_itr->second.begin(), index_itr->second.end());
  for (auto id : ids) {
    Erase(id);
  }
}

void CursorTable::SwapDb(int first, int second) {
  vmsdk::VerifyMainThread();
  if (first == second) {
    return;
  }
  auto &first_db = DbCursorsFor(first);
  auto &second_db = DbCursorsFor(second);
  // The entries point at the DbCursors, so swap the databases the two carry
  // rather than their contents.
  std::swap(first_db.db_num, second_db.db_num);
  std::swap(by_db_[first], by_db_[second]);
}

size_t CursorTable::ExpireIdle(absl::Time now) {
  vmsdk::VerifyMainThread();
  size_t expired = 0;
  while (!by_expiration_.empty() && by_expiration_.begin()->first <= now) {
    Erase(by_expiration_.begin()->second);
    ++expired;
  }
  return expired;
}

void CursorTable::Clear() {
  while (!by_expiration_.empty()) {
    Erase(by_expiration_.begin()->second);
  }
}

void CursorTable::ForEach(
    absl::FunctionRef<void(uint64_t id, absl::Time expiration)> fn) const {
  vmsdk::VerifyMainThread();
  for (const auto &[expiration, id] : by_expiration_) {
    fn(id, expiration);
  }
}

void CursorTable::Destroy(std::unique_ptr<Cursor> cursor) {
  cursor->ReleaseMainThreadState();
  if (ValkeySearch::HasInstance()) {
    ValkeySearch::Instance().ScheduleSearchResultCleanup(
        [cursor = std::move(cursor)]() mutable { cursor.reset(); });
  }
}

}  // namespace valkey_search
