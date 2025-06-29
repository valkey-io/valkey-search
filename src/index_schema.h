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
#include <queue>
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
#include "src/indexes/vector_base.h"
#include "src/keyspace_event_manager.h"
#include "src/rdb_serialization.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/blocked_client.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/time_sliced_mrmw_mutex.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
bool ShouldBlockClient(ValkeyModuleCtx *ctx, bool inside_multi_exec,
                       bool from_backfill);

using RDBLoadFunc = void *(*)(ValkeyModuleIO *, int);
using FreeFunc = void (*)(void *);

class IndexSchema : public KeyspaceEventSubscription,
                    public std::enable_shared_from_this<IndexSchema> {
 public:
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
  };
  std::shared_ptr<IndexSchema> GetSharedPtr() { return shared_from_this(); }
  std::weak_ptr<IndexSchema> GetWeakPtr() { return weak_from_this(); }

  static absl::StatusOr<std::shared_ptr<IndexSchema>> Create(
      ValkeyModuleCtx *ctx, const data_model::IndexSchema &index_schema_proto,
      vmsdk::ThreadPool *mutations_thread_pool, bool skip_attributes = false);
  ~IndexSchema() override;
  absl::StatusOr<std::shared_ptr<indexes::IndexBase>> GetIndex(
      absl::string_view attribute_alias) const;
  virtual absl::StatusOr<std::string> GetIdentifier(
      absl::string_view attribute_alias) const;
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

  virtual absl::Status RDBSave(SafeRDB *rdb) const;

  static absl::StatusOr<std::shared_ptr<IndexSchema>> LoadFromRDB(
      ValkeyModuleCtx *ctx, vmsdk::ThreadPool *mutations_thread_pool,
      std::unique_ptr<data_model::IndexSchema> index_schema_proto,
      SupplementalContentIter &&supplemental_iter);

  bool IsInCurrentDB(ValkeyModuleCtx *ctx) const;

  virtual void OnSwapDB(ValkeyModuleSwapDbInfo *swap_db_info);
  virtual void OnLoadingEnded(ValkeyModuleCtx *ctx);

  inline const Stats &GetStats() const { return stats_; }
  void ProcessSingleMutationAsync(ValkeyModuleCtx *ctx, bool from_backfill,
                                  const InternedStringPtr &key,
                                  vmsdk::StopWatch *delay_capturer);
  std::unique_ptr<data_model::IndexSchema> ToProto() const;
  struct DocumentMutation {
    struct AttributeData {
      vmsdk::UniqueValkeyString data;
      indexes::DeletionType deletion_type{indexes::DeletionType::kNone};
    };
    std::optional<absl::flat_hash_map<std::string, AttributeData>> attributes;
    std::vector<vmsdk::BlockedClient> blocked_clients;
    bool consume_in_progress{false};
    bool from_backfill{false};
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

 protected:
  IndexSchema(ValkeyModuleCtx *ctx,
              const data_model::IndexSchema &index_schema_proto,
              std::unique_ptr<AttributeDataType> attribute_data_type,
              vmsdk::ThreadPool *mutations_thread_pool);
  absl::Status Init(ValkeyModuleCtx *ctx);

 private:
  vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx_;
  absl::flat_hash_map<std::string, Attribute> attributes_;
  KeyspaceEventManager *keyspace_event_manager_;
  std::vector<std::string> subscribed_key_prefixes_;
  std::unique_ptr<AttributeDataType> attribute_data_type_;
  std::string name_;
  uint32_t db_num_{0};

  vmsdk::ThreadPool *mutations_thread_pool_{nullptr};
  InternedStringMap<DocumentMutation> tracked_mutated_records_
      ABSL_GUARDED_BY(mutated_records_mutex_);
  bool is_destructing_ ABSL_GUARDED_BY(mutated_records_mutex_){false};
  mutable absl::Mutex mutated_records_mutex_;

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
  void VectorExternalizer(const InternedStringPtr &key,
                          absl::string_view attribute_identifier,
                          vmsdk::UniqueValkeyString &record);

  mutable Stats stats_;

  void ProcessKeyspaceNotification(ValkeyModuleCtx *ctx,
                                   ValkeyModuleString *key, bool from_backfill);

  void ProcessMutation(ValkeyModuleCtx *ctx,
                       MutatedAttributes &mutated_attributes,
                       const InternedStringPtr &interned_key,
                       bool from_backfill);
  void ScheduleMutation(bool from_backfill, const InternedStringPtr &key,
                        vmsdk::ThreadPool::Priority priority,
                        absl::BlockingCounter *blocking_counter);
  void EnqueueMultiMutation(const InternedStringPtr &key);

  bool IsTrackedByAnyIndex(const InternedStringPtr &key) const;
  void SyncProcessMutation(ValkeyModuleCtx *ctx,
                           MutatedAttributes &mutated_attributes,
                           const InternedStringPtr &key);
  void ProcessAttributeMutation(ValkeyModuleCtx *ctx,
                                const Attribute &attribute,
                                const InternedStringPtr &key,
                                vmsdk::UniqueValkeyString data,
                                indexes::DeletionType deletion_type);
  static void BackfillScanCallback(ValkeyModuleCtx *ctx,
                                   ValkeyModuleString *keyname,
                                   ValkeyModuleKey *key, void *privdata);
  bool DeleteIfNotInValkeyDict(ValkeyModuleCtx *ctx, ValkeyModuleString *key,
                              const Attribute &attribute);

  bool TrackMutatedRecord(ValkeyModuleCtx *ctx, const InternedStringPtr &key,
                          MutatedAttributes &&mutated_attributes,
                          bool from_backfill, bool block_client)
      ABSL_LOCKS_EXCLUDED(mutated_records_mutex_);
  std::optional<MutatedAttributes> ConsumeTrackedMutatedAttribute(
      const InternedStringPtr &key, bool first_time)
      ABSL_LOCKS_EXCLUDED(mutated_records_mutex_);
  size_t GetMutatedRecordsSize() const
      ABSL_LOCKS_EXCLUDED(mutated_records_mutex_);

  mutable vmsdk::TimeSlicedMRMWMutex time_sliced_mutex_;
  struct MultiMutations {
    std::unique_ptr<absl::BlockingCounter> blocking_counter;
    std::queue<InternedStringPtr> keys;
  };
  vmsdk::MainThreadAccessGuard<MultiMutations> multi_mutations_;
  vmsdk::MainThreadAccessGuard<bool> schedule_multi_exec_processing_{false};

  FRIEND_TEST(IndexSchemaRDBTest, SaveAndLoad);
  FRIEND_TEST(IndexSchemaFriendTest, ConsistencyTest);
  FRIEND_TEST(IndexSchemaFriendTest, MutatedAttributes);
  FRIEND_TEST(IndexSchemaFriendTest, MutatedAttributesSanity);
  FRIEND_TEST(ValkeySearchTest, Info);
  FRIEND_TEST(OnSwapDBCallbackTest, OnSwapDBCallback);
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_INDEX_SCHEMA_H_
