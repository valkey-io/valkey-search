/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_INDEX_BASE_H
#define VALKEYSEARCH_SRC_INDEXES_INDEX_BASE_H

#include <cstddef>
#include <cstdint>
#include <memory>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/rdb_serialization.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/memory_tracker.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {
enum class IndexerType { kHNSW, kFlat, kNumeric, kTag, kVector, kNone };

enum class DeletionType {
  kRecord,      // The record was deleted from the index.
  kIdentifier,  // One or more fields of the record were deleted.
  kNone         // No deletion occurred.
};

const absl::NoDestructor<absl::flat_hash_map<absl::string_view, IndexerType>>
    kIndexerTypeByStr({{"VECTOR", IndexerType::kVector},
                       {"TAG", IndexerType::kTag},
                       {"NUMERIC", IndexerType::kNumeric}});

class IndexBase {
 public:
  explicit IndexBase(IndexerType indexer_type, MemoryPool& memory_pool)
      : indexer_type_(indexer_type), memory_pool_(memory_pool) {}
  virtual ~IndexBase() = default;

  // Add/Remove/Modify will return true if the operation was successful, false
  // if it was skipped.  Returns an error status if there is an unexpected
  // failure.
  virtual absl::StatusOr<bool> AddRecord(const InternedStringPtr& key,
                                         absl::string_view data) = 0;
  virtual absl::StatusOr<bool> RemoveRecord(const InternedStringPtr& key,
                                            DeletionType deletion_type) = 0;
  virtual absl::StatusOr<bool> ModifyRecord(const InternedStringPtr& key,
                                            absl::string_view data) = 0;
  virtual int RespondWithInfo(ValkeyModuleCtx* ctx) const = 0;
  virtual bool IsTracked(const InternedStringPtr& key) const = 0;
  IndexerType GetIndexerType() const { return indexer_type_; }
  virtual absl::Status SaveIndex(RDBChunkOutputStream chunked_out) const = 0;

  virtual std::unique_ptr<data_model::Index> ToProto() const = 0;
  virtual void ForEachTrackedKey(
      absl::AnyInvocable<void(const InternedStringPtr&)> fn) const {}

  virtual vmsdk::UniqueValkeyString NormalizeStringRecord(
      vmsdk::UniqueValkeyString input) const {
    return input;
  }
  virtual uint64_t GetRecordCount() const = 0;

  MemoryPool& GetMemoryPool() { return memory_pool_; }

 protected:
  MemoryPool& memory_pool_;

 private:
  IndexerType indexer_type_{IndexerType::kNone};
};

class EntriesFetcherIteratorBase {
 public:
  virtual bool Done() const = 0;
  virtual void Next() = 0;
  virtual const InternedStringPtr& operator*() const = 0;
  virtual ~EntriesFetcherIteratorBase() = default;
};

class EntriesFetcherBase {
 public:
  virtual size_t Size() const = 0;
  virtual ~EntriesFetcherBase() = default;
  virtual std::unique_ptr<EntriesFetcherIteratorBase> Begin() = 0;
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_INDEX_BASE_H
