/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#ifndef VALKEYSEARCH_SRC_INDEXES_SPARSE_H_
#define VALKEYSEARCH_SRC_INDEXES_SPARSE_H_

#include <cstdint>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/utils/cancel.h"

namespace valkey_search::indexes {

// A managed inverted index. HASH values contain sorted (uint32, float32)
// little-endian pairs, with no header. Unlike dense vectors, absent dimensions
// consume no space and searches only return documents sharing a dimension.
class Sparse : public IndexBase {
 public:
  struct Entry {
    uint32_t dimension;
    float weight;
    uint32_t posting_offset = 0;
  };
  Sparse() : IndexBase(IndexerType::kSparse) {}

  // Rejects non-finite values, duplicate/out-of-order IDs and truncated pairs.
  static absl::StatusOr<std::vector<Entry>> Decode(absl::string_view blob);
  // Re-scores a HASH value after a concurrent mutation, without reading index
  // state. A document with no overlap is no longer a sparse search candidate.
  static absl::StatusOr<float> RecomputeDistance(absl::string_view record,
                                                 absl::string_view query);

  absl::StatusOr<RecordResult> AddRecord(const InternedStringPtr &key,
                                         AttributeData &&data) override;
  absl::StatusOr<RecordResult> ModifyRecord(const InternedStringPtr &key,
                                            AttributeData &&data) override;
  absl::StatusOr<bool> RemoveRecord(
      const InternedStringPtr &key,
      DeletionType deletion_type = DeletionType::kNone) override;
  int RespondWithInfo(ValkeyModuleCtx *ctx) const override;
  // Like TAG/NUMERIC, rebuild postings from HASH values during RDB loading.
  absl::Status SaveIndex(RDBChunkOutputStream) const override {
    return absl::OkStatus();
  }
  std::unique_ptr<data_model::Index> ToProto() const override;
  uint32_t GetMutationWeight() const override;
  size_t GetTrackedKeyCount() const override;
  size_t GetUnTrackedKeyCount() const override;
  bool IsTracked(const InternedStringPtr &key) const override;
  bool IsUnTracked(const InternedStringPtr &key) const override;
  void UnTrack(const InternedStringPtr &key) override;
  absl::Status ForEachTrackedKey(
      absl::AnyInvocable<absl::Status(const InternedStringPtr &)> fn)
      const override;
  absl::Status ForEachUnTrackedKey(
      absl::AnyInvocable<absl::Status(const InternedStringPtr &)> fn)
      const override;

  // Called during the schema's reader phase: mutations cannot run concurrently.
  // Returns exact top-K by negative dot product (lower distance is better),
  // breaking ties by key ascending. Filter runs before top-K truncation.
  absl::StatusOr<std::vector<Neighbor>> Search(
      absl::string_view query, size_t count, cancel::Token &cancellation,
      absl::AnyInvocable<bool(const InternedStringPtr &)> filter,
      const std::vector<InternedStringPtr> *candidates = nullptr) const;

 private:
  using DocId = uint32_t;
  struct Posting {
    DocId id;
    float weight;
  };
  struct Record {
    InternedStringPtr key;
    std::vector<Entry> entries;
  };
  void AddPostings(DocId id) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void ErasePostings(DocId id) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  mutable absl::Mutex mutex_;
  InternedStringHashMap<DocId> ids_ ABSL_GUARDED_BY(mutex_);
  std::vector<Record> records_ ABSL_GUARDED_BY(mutex_);
  std::vector<DocId> free_ids_ ABSL_GUARDED_BY(mutex_);
  BagOfInternedStringPtrs untracked_ ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<uint32_t, std::vector<Posting>> postings_
      ABSL_GUARDED_BY(mutex_);
};
}  // namespace valkey_search::indexes
#endif  // VALKEYSEARCH_SRC_INDEXES_SPARSE_H_
