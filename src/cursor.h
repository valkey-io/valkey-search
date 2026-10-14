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
#include "absl/container/flat_hash_set.h"
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
  Cursor(std::string index_name, std::weak_ptr<IndexSchema> index_schema,
         const CursorOptions &options)
      : index_name_(std::move(index_name)),
        index_schema_(std::move(index_schema)),
        max_idle_(options.max_idle),
        read_count_(options.count) {}
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

  const std::string &GetIndexName() const { return index_name_; }
  // The number of rows an FT.CURSOR READ that gives no COUNT returns. It
  // starts as the COUNT of the WITHCURSOR clause that created the cursor, and,
  // as in Redis, a COUNT given to a READ replaces it for later reads.
  int64_t GetReadCount() const { return read_count_; }
  void SetReadCount(int64_t count) { read_count_ = count; }
  // True if `index_schema` is the same index the cursor was created against,
  // i.e. it has not been dropped (and possibly recreated) since.
  bool IsSameIndex(const std::shared_ptr<IndexSchema> &index_schema) const {
    return index_schema_.lock() == index_schema;
  }
  absl::Duration GetMaxIdle() const { return max_idle_; }

 private:
  std::string index_name_;
  std::weak_ptr<IndexSchema> index_schema_;
  absl::Duration max_idle_;
  int64_t read_count_;
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
  uint64_t Insert(std::unique_ptr<Cursor> cursor, int db_num, absl::Time now);
  Cursor *Lookup(uint64_t id) const;
  // The database the cursor was created in. The cursor must exist.
  int GetDbNum(uint64_t id) const;
  // Recomputes the cursor's expiration from its max idle time.
  void Touch(uint64_t id, absl::Time now);
  void Erase(uint64_t id);
  // Destroys every cursor of an index that is being removed.
  void EraseIndex(int db_num, absl::string_view index_name);
  // Moves the cursors of two databases, following their index schemas. O(1).
  void SwapDb(int first, int second);
  // Destroys every cursor whose expiration is at or before `now`.
  size_t ExpireIdle(absl::Time now);
  void Clear();
  size_t Size() const { return cursors_.size(); }
  void ForEach(
      absl::FunctionRef<void(uint64_t id, absl::Time expiration)> fn) const;

 private:
  using ExpirationMap = std::multimap<absl::Time, uint64_t>;
  // The cursors of one database, by index name. Held by pointer so that
  // SwapDb only has to exchange two pointers and the database numbers.
  struct DbCursors {
    int db_num;
    absl::flat_hash_map<std::string, absl::flat_hash_set<uint64_t>> by_index;
  };
  struct Entry {
    std::unique_ptr<Cursor> cursor;
    // std::multimap iterators stay valid across other inserts and erases.
    ExpirationMap::iterator expiration;
    DbCursors *db;
  };
  void Destroy(std::unique_ptr<Cursor> cursor);
  DbCursors &DbCursorsFor(int db_num);

  uint32_t id_crc_;
  uint32_t counter_;
  absl::flat_hash_map<uint64_t, Entry> cursors_;
  ExpirationMap by_expiration_;
  absl::flat_hash_map<int, std::unique_ptr<DbCursors>> by_db_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_CURSOR_H_
