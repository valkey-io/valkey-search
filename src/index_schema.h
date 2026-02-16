/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEX_SCHEMA_H_
#define VALKEYSEARCH_SRC_INDEX_SCHEMA_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/blocking_counter.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "gtest/gtest_prod.h"
#include "src/attribute.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/indexes/text/text_index.h"
#include "src/indexes/vector_base.h"
#include "src/keyspace_event_manager.h"
#include "src/rdb_serialization.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/blocked_client.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/time_sliced_mrmw_mutex.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query {
struct SearchParameters;
}  // namespace valkey_search::query

namespace valkey_search {
bool ShouldBlockClient(ValkeyModuleCtx *ctx, bool inside_multi_exec,
                       bool from_backfill);

using Key = InternedStringPtr;
using MutationSequenceNumber = uint64_t;
using RDBLoadFunc = void *(*)(ValkeyModuleIO *, int);
using FreeFunc = void (*)(void *);
using FieldMaskPredicate = uint64_t;

struct AttributeInfo {
  explicit AttributeInfo(uint16_t pos, uint64_t size)
      : position_(pos), size_(size) {}
  ~AttributeInfo() = default;

  inline uint16_t GetPosition() const {
    return static_cast<uint16_t>(position_);
  }
  inline void SetPosition(uint16_t pos) { position_ = pos; }
  inline uint64_t GetSize() const { return size_; }
  inline void SetSize(uint64_t size) { size_ = size; }

 private:
  // Attribute position within the IndexSchema::attribute_size_vec_ property.
  uint64_t position_ : 16;
  uint64_t size_ : 48;
};

class IndexSchema : public KeyspaceEventSubscription,
                    public std::enable_shared_from_this<IndexSchema> {
 public:
  struct IndexKeyInfo {
    MutationSequenceNumber mutation_sequence_number_{0};
  };

  using IndexKeyInfoMap = absl::flat_hash_map<Key, IndexKeyInfo>;

  struct InfoIndexPartitionData {
    uint64_t num_docs;
    uint64_t num_records;
    uint64_t hash_indexing_failures;
    uint64_t backfill_scanned_count;
    uint64_t backfill_db_size;
    uint64_t backfill_inqueue_tasks;
    float backfill_complete_percent;
    bool backfill_in_progress;
    uint64_t mutation_queue_size;
    uint64_t recent_mutations_queue_delay;
    std::string state;
  };

  struct Stats {
    template <typename T>
    struct ResultCnt {
      T failure_cnt{0};
      T success_cnt{0};
      T skipped_cnt{0};
    };
    ResultCnt<std::atomic<uint64_t>> subscription_remove;
    ResultCnt<std::atomic<uint64_t>> subscription_modify;
    ResultCnt<std::atomic<uint64_t>> subscription_add;
    std::atomic<uint32_t> document_cnt{0};
    std::atomic<uint32_t> backfill_inqueue_tasks{0};
    uint64_t mutation_queue_size_ ABSL_GUARDED_BY(mutex_){0};
    absl::Duration mutations_queue_delay_ ABSL_GUARDED_BY(mutex_);
    mutable absl::Mutex mutex_;

    // Single interface to get all stats data
    InfoIndexPartitionData GetStats() const;
  };
  std::shared_ptr<IndexSchema> GetSharedPtr() { return shared_from_this(); }
  std::weak_ptr<IndexSchema> GetWeakPtr() { return weak_from_this(); }

  static absl::StatusOr<std::shared_ptr<IndexSchema>> Create(
      ValkeyModuleCtx *ctx, const data_model::IndexSchema &index_schema_proto,
      vmsdk::ThreadPool *mutations_thread_pool, bool skip_attributes,
      bool reload);
  ~IndexSchema() override;
  absl::StatusOr<std::shared_ptr<indexes::IndexBase>> GetIndex(
      absl::string_view attribute_alias) const;
  inline bool HasTextOffsets() const { return with_offsets_; }
  inline uint32_t GetMinStemSize() const { return min_stem_size_; }
  inline FieldMaskPredicate GetStemTextFieldMask() const {
    return stem_text_field_mask_;
  }
  const absl::flat_hash_set<std::string> &GetAllTextIdentifiers(
      bool with_suffix) const;
  FieldMaskPredicate GetAllTextFieldMask(bool with_suffix) const;
  void UpdateTextFieldMasksForIndex(const std::string &identifier,
                                    indexes::IndexBase *index);
  absl::flat_hash_set<std::string> GetTextIdentifiersByFieldMask(
      FieldMaskPredicate field_mask) const;
  virtual absl::StatusOr<std::string> GetIdentifier(
      absl::string_view attribute_alias) const;
  absl::StatusOr<AttributePosition> GetAttributePositionByAlias(
      absl::string_view attribute_alias) const;
  absl::StatusOr<AttributePosition> GetAttributePositionByIdentifier(
      absl::string_view identifier) const;
  absl::StatusOr<std::string> GetAlias(absl::string_view identifier) const;
  absl::StatusOr<vmsdk::UniqueValkeyString> DefaultReplyScoreAs(
      absl::string_view attribute_alias) const;
  absl::Status AddIndex(absl::string_view attribute_alias,
                        absl::string_view identifier,
                        std::shared_ptr<indexes::IndexBase> index);

  void RespondWithInfo(ValkeyModuleCtx *ctx) const;

  inline const AttributeDataType &GetAttributeDataType() const override {
    return *attribute_data_type_;
  }

  inline const std::vector<std::string> &GetKeyPrefixes() const override {
    return subscribed_key_prefixes_;
  }

  inline const std::string &GetName() const { return name_; }
  inline std::uint32_t GetDBNum() const { return db_num_; }

  void CreateTextIndexSchema() {
    text_index_schema_ = std::make_shared<indexes::text::TextIndexSchema>(
        language_, punctuation_, with_offsets_, stop_words_, min_stem_size_);
  }
  std::shared_ptr<indexes::text::TextIndexSchema> GetTextIndexSchema() const {
    return text_index_schema_;
  }
  inline uint64_t GetFingerprint() const { return fingerprint_; }
  inline uint32_t GetVersion() const { return version_; }

  inline void SetFingerprint(uint64_t fingerprint) {
    fingerprint_ = fingerprint;
  }
  inline void SetVersion(uint32_t version) { version_ = version; }

  void OnKeyspaceNotification(ValkeyModuleCtx *ctx, int type, const char *event,
                              ValkeyModuleString *key) override;

  uint32_t PerformBackfill(ValkeyModuleCtx *ctx, uint32_t batch_size);

  bool IsBackfillInProgress() const {
    auto &backfill_job = backfill_job_.Get();
    return backfill_job.has_value() &&
           (!backfill_job->IsScanDone() || stats_.backfill_inqueue_tasks > 0);
  }

  float GetBackfillPercent() const;
  absl::string_view GetStateForInfo() const;
  uint64_t CountRecords() const;

  int GetAttributeCount() const { return attributes_.size(); }
  int GetTagAttributeCount() const;
  int GetNumericAttributeCount() const;
  int GetVectorAttributeCount() const;
  int GetTextAttributeCount() const;
  int GetTextItemCount() const;

  virtual absl::Status RDBSave(SafeRDB *rdb) const;
  absl::Status SaveIndexExtension(RDBChunkOutputStream output) const;
  absl::Status LoadIndexExtension(ValkeyModuleCtx *ctx,
                                  RDBChunkInputStream input);
  static absl::StatusOr<vmsdk::ValkeyVersion> GetMinVersion(
      const google::protobuf::Any &metadata);
  absl::Status ValidateIndex() const;

  static absl::StatusOr<std::shared_ptr<IndexSchema>> LoadFromRDB(
      ValkeyModuleCtx *ctx, vmsdk::ThreadPool *mutations_thread_pool,
      std::unique_ptr<data_model::IndexSchema> index_schema_proto,
      SupplementalContentIter &&supplemental_iter);

  bool IsInCurrentDB(ValkeyModuleCtx *ctx) const;

  virtual void OnSwapDB(ValkeyModuleSwapDbInfo *swap_db_info);
  virtual void OnLoadingEnded(ValkeyModuleCtx *ctx);

  inline const Stats &GetStats() const { return stats_; }
  void ProcessSingleMutationAsync(ValkeyModuleCtx *ctx, bool from_backfill,
                                  const Key &key,
                                  vmsdk::StopWatch *delay_capturer);
  std::unique_ptr<data_model::IndexSchema> ToProto() const;
  struct DocumentMutation {
    struct AttributeData {
      vmsdk::UniqueValkeyString data;
      indexes::DeletionType deletion_type{indexes::DeletionType::kNone};
    };
    std::optional<absl::flat_hash_map<std::string, AttributeData>> attributes;
    std::vector<vmsdk::BlockedClient> blocked_clients;
    // Queries waiting for this mutation to complete
    std::vector<std::unique_ptr<query::SearchParameters>> waiting_queries;
    MutationSequenceNumber sequence_number{0};
    bool consume_in_progress{false};
    bool from_backfill{false};
    bool from_multi{false};
  };
  using MutatedAttributes =
      absl::flat_hash_map<std::string, DocumentMutation::AttributeData>;
  vmsdk::TimeSlicedMRMWMutex &GetTimeSlicedMutex() {
    return time_sliced_mutex_;
  }
  void MarkAsDestructing();
  void ProcessMultiQueue();
  void SubscribeToVectorExternalizer(absl::string_view attribute_identifier,
                                     indexes::VectorBase *vector_index);
  uint64_t GetBackfillScannedKeyCount() const;
  uint64_t GetBackfillDbSize() const;
  InfoIndexPartitionData GetInfoIndexPartitionData() const;
  // Check neighbors for contention with in-flight mutations by comparing
  // sequence numbers. Only neighbors whose db and index sequence numbers
  // differ are checked against the mutation queue. If contention is found,
  // params is moved into the mutation queue and true is returned.
  // Otherwise params is untouched and false is returned.
  bool PerformKeyContentionCheck(
      const std::vector<indexes::Neighbor> &neighbors,
      std::unique_ptr<query::SearchParameters> &&params)
      ABSL_LOCKS_EXCLUDED(mutated_records_mutex_);

  static absl::Status TextInfoCmd(ValkeyModuleCtx *ctx,
                                  vmsdk::ArgsIterator &itr);
  struct DbKeyInfo {
    MutationSequenceNumber mutation_sequence_number_{0};
    std::vector<AttributeInfo> attr_info_vec_;

    inline std::vector<AttributeInfo> &GetAttributeInfoVec() {
      return attr_info_vec_;
    }
  };

  MutationSequenceNumber GetIndexMutationSequenceNumber(const Key &key) const {
    auto itr = index_key_info_.find(key);
    CHECK(itr != index_key_info_.end()) << "Key not found: " << key->Str();
    return itr->second.mutation_sequence_number_;
  }

  MutationSequenceNumber GetDbMutationSequenceNumber(const Key &key) const {
    vmsdk::VerifyMainThread();
    auto itr = db_key_info_.Get().find(key);
    CHECK(itr != db_key_info_.Get().end()) << "Key not found: " << key->Str();
    return itr->second.mutation_sequence_number_;
  }

  // Accessor for global key map (for negation queries)
  // Safe to call from reader threads - protected by mutated_records_mutex_
  const IndexKeyInfoMap &GetIndexKeyInfo() const { return index_key_info_; }

  size_t GetIndexKeyInfoSize() const { return index_key_info_.size(); }

  // Unit test only
  void SetDbMutationSequenceNumber(const Key &key,
                                   MutationSequenceNumber sequence_number) {
    db_key_info_.Get()[key].mutation_sequence_number_ = sequence_number;
  }
  // Unit test only
  void SetIndexMutationSequenceNumber(const Key &key,
                                      MutationSequenceNumber sequence_number) {
    absl::MutexLock lock(&mutated_records_mutex_);
    index_key_info_[key].mutation_sequence_number_ = sequence_number;
  }

  /**
   * @brief Computes the total size of attributes, optionally filtered by
   * indexer type.
   */
  inline uint64_t GetSize(std::optional<indexes::IndexerType>
                              indexer_type_filter = std::nullopt) const {
    if (!indexer_type_filter.has_value()) {
      // No filter
      return std::accumulate(attributes_indexed_data_size_.begin(),
                             attributes_indexed_data_size_.end(), 0);
    }

    uint64_t total_size{0};
    for (const auto &[_, attr] : attributes_) {
      if (attr.GetIndex()->GetIndexerType() == indexer_type_filter.value()) {
        total_size += attributes_indexed_data_size_[attr.GetPosition()];
      }
    }
    return total_size;
  }
  /**
   * @brief Returns the total size of attributes matching the specified filter.
   *
   * Return the size of the first attribute that satisfies the given
   * filter criteria. If the filter is empty, returns the total size of all
   * attributes.
   *
   * @return The total size of matching attributes, or 0 if no attributes match
   *         the filter.
   */
  inline uint64_t GetSize(absl::string_view attribute_alias_filter = "") const {
    if (attribute_alias_filter.empty()) {
      return GetSize(std::nullopt);
    }

    for (const auto &[_, attr] : attributes_) {
      if (attr.GetAlias() == attribute_alias_filter) {
        return attributes_indexed_data_size_[attr.GetPosition()];
      }
    }
    return 0;
  }

 protected:
  IndexSchema(ValkeyModuleCtx *ctx,
              const data_model::IndexSchema &index_schema_proto,
              std::unique_ptr<AttributeDataType> attribute_data_type,
              vmsdk::ThreadPool *mutations_thread_pool, bool reload);
  absl::Status Init(ValkeyModuleCtx *ctx);

 private:
  vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx_;
  absl::flat_hash_map<std::string, Attribute> attributes_;
  absl::flat_hash_map<std::string, std::string> identifier_to_alias_;
  KeyspaceEventManager *keyspace_event_manager_;
  std::vector<std::string> subscribed_key_prefixes_;
  std::unique_ptr<AttributeDataType> attribute_data_type_;
  std::string name_;
  uint32_t db_num_{0};
  data_model::Language language_{data_model::LANGUAGE_ENGLISH};
  std::string punctuation_;
  bool with_offsets_{true};
  std::vector<std::string> stop_words_;
  uint32_t min_stem_size_{4};
  std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema_;
  // Precomputed text field information for searches
  uint64_t all_text_field_mask_{0ULL};
  uint64_t suffix_text_field_mask_{0ULL};
  uint64_t stem_text_field_mask_{0ULL};  // Tracks fields with stemming enabled
  absl::flat_hash_set<std::string> all_text_identifiers_;
  absl::flat_hash_set<std::string> suffix_text_identifiers_;
  bool loaded_v2_{false};
  uint64_t fingerprint_{0};
  uint32_t version_{0};

  vmsdk::ThreadPool *mutations_thread_pool_{nullptr};
  std::vector<uint64_t> attributes_indexed_data_size_;

  InternedStringHashMap<DocumentMutation> tracked_mutated_records_
      ABSL_GUARDED_BY(mutated_records_mutex_);
  bool is_destructing_ ABSL_GUARDED_BY(mutated_records_mutex_){false};
  mutable absl::Mutex mutated_records_mutex_;

  MutationSequenceNumber schema_mutation_sequence_number_{0};
  vmsdk::MainThreadAccessGuard<absl::flat_hash_map<Key, DbKeyInfo>>
      db_key_info_;  // Mainthread.
  IndexKeyInfoMap index_key_info_ ABSL_GUARDED_BY(
      mutated_records_mutex_);  // updates are guarded by mutated_records_mutex_
  struct BackfillJob {
    BackfillJob() = delete;
    BackfillJob(ValkeyModuleCtx *ctx, absl::string_view name, int db_num);
    bool IsScanDone() const { return scan_ctx.get() == nullptr; }
    void MarkScanAsDone() {
      scan_ctx.reset();
      cursor.reset();
    }
    vmsdk::UniqueValkeyDetachedThreadSafeContext scan_ctx;
    vmsdk::UniqueValkeyScanCursor cursor;
    uint64_t scanned_key_count{0};
    uint64_t db_size;
    vmsdk::StopWatch stopwatch;
    bool paused_by_oom{false};
  };

  vmsdk::MainThreadAccessGuard<std::optional<BackfillJob>> backfill_job_;
  absl::flat_hash_map<std::string, indexes::VectorBase *>
      vector_externalizer_subscriptions_;
  void VectorExternalizer(const Key &key,
                          absl::string_view attribute_identifier,
                          vmsdk::UniqueValkeyString &record);

  mutable Stats stats_;

  void ProcessKeyspaceNotification(ValkeyModuleCtx *ctx,
                                   ValkeyModuleString *key, bool from_backfill);

  void ProcessMutation(ValkeyModuleCtx *ctx,
                       MutatedAttributes &mutated_attributes,
                       const Key &interned_key, bool from_backfill,
                       bool is_delete);
  bool ScheduleMutation(bool from_backfill, const Key &key,
                        vmsdk::ThreadPool::Priority priority,
                        absl::BlockingCounter *blocking_counter);
  void EnqueueMultiMutation(const Key &key);
  void DrainMutationQueue(ValkeyModuleCtx *ctx) const
      ABSL_LOCKS_EXCLUDED(mutated_records_mutex_);

  void SyncProcessMutation(ValkeyModuleCtx *ctx,
                           MutatedAttributes &mutated_attributes,
                           const Key &key);
  void ProcessAttributeMutation(ValkeyModuleCtx *ctx,
                                const Attribute &attribute, const Key &key,
                                vmsdk::UniqueValkeyString data,
                                indexes::DeletionType deletion_type);
  static void BackfillScanCallback(ValkeyModuleCtx *ctx,
                                   ValkeyModuleString *keyname,
                                   ValkeyModuleKey *key, void *privdata);
  bool DeleteIfNotInValkeyDict(ValkeyModuleCtx *ctx, ValkeyModuleString *key,
                               const Attribute &attribute);
  vmsdk::BlockedClientCategory GetBlockedCategoryFromProto() const;
  bool InTrackedMutationRecords(const Key &key,
                                const std::string &identifier) const;
  bool TrackMutatedRecord(ValkeyModuleCtx *ctx, const Key &key,
                          MutatedAttributes &&mutated_attributes,
                          MutationSequenceNumber sequence_number,
                          bool from_backfill, bool block_client,
                          bool from_multi)
      ABSL_LOCKS_EXCLUDED(mutated_records_mutex_);
  std::optional<MutatedAttributes> ConsumeTrackedMutatedAttribute(
      const Key &key, bool first_time)
      ABSL_LOCKS_EXCLUDED(mutated_records_mutex_);
  size_t GetMutatedRecordsSize() const
      ABSL_LOCKS_EXCLUDED(mutated_records_mutex_);
  /**
   * @brief Updates the database key information entry for a given key.
   *
   * This method updates or removes an entry in the internal database key
   * information map, tracking attribute sizes and maintaining the global index
   * size. It increments the schema mutation sequence number for versioning and
   * ensures thread safety by verifying execution on the main thread.
   *
   * @param ctx Pointer to the ValkeyModuleCtx (Valkey module context) for
   * accessing module operations.
   * @param mutated_attributes A collection of attributes that have been
   * modified, where each attribute consists of a name and associated data.
   * @param interned_key The interned key object representing the database key
   * being updated.
   * @param from_backfill Boolean flag indicating whether this update originates
   * from a backfill operation (currently unused in the implementation but may
   * affect future behavior).
   * @param is_delete Boolean flag indicating whether this is a delete
   * operation; if true, the key entry is removed from the map.
   *
   * @return MutationSequenceNumber The sequence number assigned to this
   * mutation, representing the order in which this update occurred within the
   * schema.
   */
  MutationSequenceNumber UpdateDbInfoKey(
      ValkeyModuleCtx *ctx, const MutatedAttributes &mutated_attributes,
      const Key &interned_key, bool from_backfill, bool is_delete);
  mutable vmsdk::TimeSlicedMRMWMutex time_sliced_mutex_;
  vmsdk::MainThreadAccessGuard<std::deque<Key>> multi_mutations_keys_;
  vmsdk::MainThreadAccessGuard<bool> schedule_multi_exec_processing_{false};

  FRIEND_TEST(IndexSchemaRDBTest, SaveAndLoad);
  FRIEND_TEST(IndexSchemaRDBTest, ComprehensiveSkipLoadTest);
  FRIEND_TEST(IndexSchemaFriendTest, ConsistencyTest);
  FRIEND_TEST(IndexSchemaFriendTest, MutatedAttributes);
  FRIEND_TEST(IndexSchemaFriendTest, MutatedAttributesSanity);
  FRIEND_TEST(ValkeySearchTest, Info);
  FRIEND_TEST(OnSwapDBCallbackTest, OnSwapDBCallback);
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_INDEX_SCHEMA_H_
