/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_CURSOR_H_
#define VALKEYSEARCH_SRC_CURSOR_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/functional/function_ref.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

class IndexSchema;

constexpr absl::string_view kWithCursorParam{"WITHCURSOR"};
constexpr int64_t kDefaultCursorCount{1000};
constexpr int64_t kDefaultCursorMaxIdleMs{300000};

struct CursorOptions {
  int64_t count{kDefaultCursorCount};
  absl::Duration max_idle{absl::Milliseconds(kDefaultCursorMaxIdleMs)};
};

// Parses a COUNT value, which must be in [1, cursor-max-count].
absl::StatusOr<int64_t> ParseCursorCount(vmsdk::ArgsIterator &itr);
// Parses the optional COUNT and MAXIDLE clauses that follow WITHCURSOR.
absl::StatusOr<CursorOptions> ParseCursorOptions(vmsdk::ArgsIterator &itr);

//
// The saved output of a query (FT.AGGREGATE / FT.SEARCH ... WITHCURSOR) that
// is handed back to the client in pieces by FT.CURSOR READ.
//
class Cursor {
 public:
  Cursor(uint32_t db_num, std::string index_name,
         std::weak_ptr<IndexSchema> index_schema, absl::Duration max_idle)
      : db_num_(db_num),
        index_name_(std::move(index_name)),
        index_schema_(std::move(index_schema)),
        max_idle_(max_idle) {}
  virtual ~Cursor() = default;

  virtual size_t RemainingRows() const = 0;
  // Emits `[n, row...]` for n = min(count, RemainingRows()) and consumes those
  // rows. `index_schema` is the live schema the cursor was created against.
  virtual void ReplyRows(ValkeyModuleCtx *ctx,
                         const std::shared_ptr<IndexSchema> &index_schema,
                         size_t count) = 0;
  // Releases whatever may only be released on the main thread. The cursor
  // itself is then destroyed on a utility thread.
  virtual void ReleaseMainThreadState() {}

  uint32_t GetDbNum() const { return db_num_; }
  const std::string &GetIndexName() const { return index_name_; }
  // True if `index_schema` is the same index the cursor was created against,
  // i.e. it has not been dropped (and possibly recreated) since.
  bool IsSameIndex(const std::shared_ptr<IndexSchema> &index_schema) const {
    return index_schema_.lock() == index_schema;
  }
  absl::Duration GetMaxIdle() const { return max_idle_; }

 private:
  uint32_t db_num_;
  std::string index_name_;
  std::weak_ptr<IndexSchema> index_schema_;
  absl::Duration max_idle_;
};

//
// All outstanding cursors, indexed by id and by expiration time. Main thread
// only.
//
class CursorTable {
 public:
  // Cursor ids are (counter << 32) | id_crc, where the counter is 31 bits so
  // that ids are positive as RESP integers.
  CursorTable(uint32_t id_crc, uint32_t counter)
      : id_crc_(id_crc), counter_(counter) {}
  ~CursorTable();

  static void InitInstance(std::unique_ptr<CursorTable> instance);
  static bool HasInstance();
  static CursorTable &Instance();
  // The CRC-32 of the server's run_id.
  static uint32_t ComputeIdCrc(ValkeyModuleCtx *ctx);

  // Takes ownership of the cursor, returns its (non-zero) id.
  uint64_t Insert(std::unique_ptr<Cursor> cursor, absl::Time now);
  Cursor *Lookup(uint64_t id) const;
  // Recomputes the cursor's expiration from its max idle time.
  void Touch(uint64_t id, absl::Time now);
  void Erase(uint64_t id);
  // Destroys every cursor whose expiration is at or before `now`.
  size_t ExpireIdle(absl::Time now);
  void Clear();
  size_t Size() const { return cursors_.size(); }
  void ForEach(
      absl::FunctionRef<void(uint64_t id, absl::Time expiration)> fn) const;

 private:
  using ExpirationMap = std::multimap<absl::Time, uint64_t>;
  struct Entry {
    std::unique_ptr<Cursor> cursor;
    // std::multimap iterators stay valid across other inserts and erases.
    ExpirationMap::iterator expiration;
  };
  void Destroy(std::unique_ptr<Cursor> cursor);

  uint32_t id_crc_;
  uint32_t counter_;
  absl::flat_hash_map<uint64_t, Entry> cursors_;
  ExpirationMap by_expiration_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_CURSOR_H_
