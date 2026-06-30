/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_INDEX_BASE_H
#define VALKEYSEARCH_SRC_INDEXES_INDEX_BASE_H

#include <cstddef>
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
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {
enum class IndexerType { kHNSW, kFlat, kNumeric, kTag, kVector, kNone, kText };

enum class DeletionType {
  kRecord,      // The record was deleted from the index.
  kIdentifier,  // One or more fields of the record were deleted.
  kNone         // No deletion occurred.
};

// Result of an AddRecord/ModifyRecord operation on a field. This is the
// "expected" outcome channel; genuinely unexpected failures are still reported
// through the absl::Status error channel (see IndexBase::AddRecord).
enum class RecordResult {
  kAdded,        // The field value was successfully indexed.
  kMissing,      // The field has no indexable value (the only allowed
                 // non-valid field value, e.g. an empty tag/text or an
                 // unchanged vector). Treated as if the field were absent.
  kInvalidData,  // The field value does not conform to the field's type (e.g.
                 // a NUMERIC field whose value is not a number, or a VECTOR
                 // field with the wrong byte length). Redisearch drops the
                 // entire key in this case; see COMPATIBILITY.md.
};

const absl::NoDestructor<absl::flat_hash_map<absl::string_view, IndexerType>>
    kIndexerTypeByStr({{"VECTOR", IndexerType::kVector},
                       {"TAG", IndexerType::kTag},
                       {"NUMERIC", IndexerType::kNumeric},
                       {"TEXT", IndexerType::kText}});

class IndexBase {
 public:
  explicit IndexBase(IndexerType indexer_type) : indexer_type_(indexer_type) {}
  virtual ~IndexBase() = default;

  // Add/Modify return a RecordResult describing the outcome (indexed, missing,
  // or invalid data). Remove returns true if a record was removed, false if it
  // was skipped. All three return an error status if there is an unexpected
  // failure.
  virtual absl::StatusOr<RecordResult> AddRecord(const InternedStringPtr& key,
                                                 absl::string_view data) = 0;
  virtual absl::StatusOr<bool> RemoveRecord(const InternedStringPtr& key,
                                            DeletionType deletion_type) = 0;
  virtual absl::StatusOr<RecordResult> ModifyRecord(
      const InternedStringPtr& key, absl::string_view data) = 0;
  virtual int RespondWithInfo(ValkeyModuleCtx* ctx) const = 0;
  IndexerType GetIndexerType() const { return indexer_type_; }
  virtual absl::Status SaveIndex(RDBChunkOutputStream chunked_out) const = 0;

  virtual std::unique_ptr<data_model::Index> ToProto() const = 0;

  virtual size_t GetTrackedKeyCount() const = 0;
  virtual size_t GetUnTrackedKeyCount() const = 0;
  virtual bool IsTracked(const InternedStringPtr& key) const = 0;
  virtual bool IsUnTracked(const InternedStringPtr& key) const = 0;
  virtual void UnTrack(const InternedStringPtr& key) = 0;
  virtual absl::Status ForEachTrackedKey(
      absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn) const = 0;
  virtual absl::Status ForEachUnTrackedKey(
      absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn) const = 0;

  virtual vmsdk::UniqueValkeyString NormalizeStringRecord(
      vmsdk::UniqueValkeyString input) const {
    return input;
  }

  /// Returns the mutation weight for this index type
  virtual uint32_t GetMutationWeight() const = 0;

  virtual bool IsVectorIndex() const { return false; }

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
