/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/index_schema.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/blocking_counter.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "src/attribute.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_flat.h"
#include "src/indexes/vector_hnsw.h"
#include "src/keyspace_event_manager.h"
#include "src/metrics.h"
#include "src/rdb_serialization.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "src/vector_externalizer.h"
#include "vmsdk/src/blocked_client.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/info.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/time_sliced_mrmw_mutex.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

LogLevel GetLogSeverity(bool ok) { return ok ? DEBUG : WARNING; }

//
// Controls and stats for V2 RDB file
//
static auto config_rdb_write_v2 =
    vmsdk::config::BooleanBuilder("rdb-write-v2", false).Dev().Build();
static auto config_rdb_read_v2 =
    vmsdk::config::BooleanBuilder("rdb-read-v2", false).Dev().Build();
static auto config_rdb_validate_on_write =
    vmsdk::config::BooleanBuilder("rdb-validate-on-write", false).Dev().Build();

static bool RDBReadV2() {
  return dynamic_cast<vmsdk::config::Boolean &>(*config_rdb_read_v2).GetValue();
}

static bool RDBWriteV2() {
  return dynamic_cast<vmsdk::config::Boolean &>(*config_rdb_write_v2)
      .GetValue();
}

static bool RDBValidateOnWrite() {
  return dynamic_cast<vmsdk::config::Boolean &>(*config_rdb_validate_on_write)
      .GetValue();
}

DEV_INTEGER_COUNTER(rdb_stats, rdb_save_keys);
DEV_INTEGER_COUNTER(rdb_stats, rdb_load_keys);
DEV_INTEGER_COUNTER(rdb_stats, rdb_save_sections);
DEV_INTEGER_COUNTER(rdb_stats, rdb_load_sections);
DEV_INTEGER_COUNTER(rdb_stats, rdb_load_sections_skipped);
DEV_INTEGER_COUNTER(rdb_stats, rdb_save_multi_exec_entries);
DEV_INTEGER_COUNTER(rdb_stats, rdb_load_multi_exec_entries);
DEV_INTEGER_COUNTER(rdb_stats, rdb_save_mutation_entries);
DEV_INTEGER_COUNTER(rdb_stats, rdb_load_mutation_entries);

IndexSchema::BackfillJob::BackfillJob(ValkeyModuleCtx *ctx,
                                      absl::string_view name, int db_num)
    : cursor(vmsdk::MakeUniqueValkeyScanCursor()) {
  scan_ctx = vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(ctx);
  ValkeyModule_SelectDb(scan_ctx.get(), db_num);
  db_size = ValkeyModule_DbSize(scan_ctx.get());
  VMSDK_LOG(NOTICE, ctx) << "Starting backfill for index schema in DB "
                         << db_num << ": " << name << " (size: " << db_size
                         << ")";
}

absl::StatusOr<std::shared_ptr<indexes::IndexBase>> IndexFactory(
    ValkeyModuleCtx *ctx, IndexSchema *index_schema,
    const data_model::Attribute &attribute,
    std::optional<SupplementalContentChunkIter> iter) {
  const auto &index = attribute.index();
  switch (index.index_type_case()) {
    case data_model::Index::IndexTypeCase::kTagIndex: {
      return std::make_shared<indexes::Tag>(index.tag_index());
    }
    case data_model::Index::IndexTypeCase::kNumericIndex: {
      return std::make_shared<indexes::Numeric>(index.numeric_index());
    }
    case data_model::Index::IndexTypeCase::kVectorIndex: {
      switch (index.vector_index().algorithm_case()) {
        case data_model::VectorIndex::kHnswAlgorithm: {
          switch (index.vector_index().vector_data_type()) {
            case data_model::VECTOR_DATA_TYPE_FLOAT32: {
              VMSDK_ASSIGN_OR_RETURN(
                  auto index,
                  (iter.has_value())
                      ? indexes::VectorHNSW<float>::LoadFromRDB(
                            ctx, &index_schema->GetAttributeDataType(),
                            index.vector_index(), attribute.identifier(),
                            std::move(*iter))
                      : indexes::VectorHNSW<float>::Create(
                            index.vector_index(), attribute.identifier(),
                            index_schema->GetAttributeDataType().ToProto()));
              index_schema->SubscribeToVectorExternalizer(
                  attribute.identifier(), index.get());
              return index;
            }
            default: {
              return absl::InvalidArgumentError(
                  "Unsupported vector data type.");
            }
          }
        }
        case data_model::VectorIndex::kFlatAlgorithm: {
          switch (index.vector_index().vector_data_type()) {
            case data_model::VECTOR_DATA_TYPE_FLOAT32: {
              // TODO: Create an empty index in case of an error
              // loading the index contents from RDB.
              VMSDK_ASSIGN_OR_RETURN(
                  auto index,
                  (iter.has_value())
                      ? indexes::VectorFlat<float>::LoadFromRDB(
                            ctx, &index_schema->GetAttributeDataType(),
                            index.vector_index(), attribute.identifier(),
                            std::move(*iter))
                      : indexes::VectorFlat<float>::Create(
                            index.vector_index(), attribute.identifier(),
                            index_schema->GetAttributeDataType().ToProto()));
              index_schema->SubscribeToVectorExternalizer(
                  attribute.identifier(), index.get());
              return index;
            }
            default: {
              return absl::InvalidArgumentError(
                  "Unsupported vector data type.");
            }
          }
        }
        default: {
          return absl::InvalidArgumentError("Unsupported algorithm.");
        }
      }
      break;
    }
    default: {
      return absl::InvalidArgumentError("Unsupported index type.");
    }
  }
}

absl::StatusOr<std::shared_ptr<IndexSchema>> IndexSchema::Create(
    ValkeyModuleCtx *ctx, const data_model::IndexSchema &index_schema_proto,
    vmsdk::ThreadPool *mutations_thread_pool, bool skip_attributes,
    bool reload) {
  std::unique_ptr<AttributeDataType> attribute_data_type;
  switch (index_schema_proto.attribute_data_type()) {
    case data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH:
      attribute_data_type = std::make_unique<HashAttributeDataType>();
      break;
    case data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON:
      if (!IsJsonModuleSupported(ctx)) {
        return absl::InvalidArgumentError("JSON module is not loaded");
      }
      attribute_data_type = std::make_unique<JsonAttributeDataType>();
      break;
    default:
      return absl::InvalidArgumentError("Unsupported attribute data type.");
  }

  auto res = std::shared_ptr<IndexSchema>(
      new IndexSchema(ctx, index_schema_proto, std::move(attribute_data_type),
                      mutations_thread_pool, reload));
  VMSDK_RETURN_IF_ERROR(res->Init(ctx));
  if (!skip_attributes) {
    for (const auto &attribute : index_schema_proto.attributes()) {
      VMSDK_ASSIGN_OR_RETURN(
          std::shared_ptr<indexes::IndexBase> index,
          IndexFactory(ctx, res.get(), attribute, std::nullopt));
      VMSDK_RETURN_IF_ERROR(
          res->AddIndex(attribute.alias(), attribute.identifier(), index));
    }
  }
  return res;
}

vmsdk::MRMWMutexOptions CreateMrmwMutexOptions() {
  vmsdk::MRMWMutexOptions options;
  options.read_quota_duration = absl::Milliseconds(10);
  options.read_switch_grace_period = absl::Milliseconds(1);
  options.write_quota_duration = absl::Milliseconds(1);
  options.write_switch_grace_period = absl::Microseconds(200);
  return options;
}

IndexSchema::IndexSchema(ValkeyModuleCtx *ctx,
                         const data_model::IndexSchema &index_schema_proto,
                         std::unique_ptr<AttributeDataType> attribute_data_type,
                         vmsdk::ThreadPool *mutations_thread_pool, bool reload)
    : detached_ctx_(vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(ctx)),
      keyspace_event_manager_(&KeyspaceEventManager::Instance()),
      attribute_data_type_(std::move(attribute_data_type)),
      name_(std::string(index_schema_proto.name())),
      db_num_(index_schema_proto.db_num()),
      mutations_thread_pool_(mutations_thread_pool),
      time_sliced_mutex_(CreateMrmwMutexOptions()) {
  ValkeyModule_SelectDb(detached_ctx_.get(), db_num_);
  if (index_schema_proto.subscribed_key_prefixes().empty()) {
    subscribed_key_prefixes_.push_back("");
  } else {
    for (const auto &key_prefix :
         index_schema_proto.subscribed_key_prefixes()) {
      if (!std::any_of(subscribed_key_prefixes_.begin(),
                       subscribed_key_prefixes_.end(),
                       [&](const std::string &s) {
                         return key_prefix.starts_with(s);
                       })) {
        subscribed_key_prefixes_.push_back(std::string(key_prefix));
      }
    }
  }
  // The protobuf has volatile fields that get save/restores in the RDB. here we
  // reconcile the source of the index_schema_proto (reload or not) and restore
  // those fields
  if (reload) {
    stats_.document_cnt = index_schema_proto.stats().documents_count();
  }
}

absl::Status IndexSchema::Init(ValkeyModuleCtx *ctx) {
  VMSDK_RETURN_IF_ERROR(keyspace_event_manager_->InsertSubscription(ctx, this));
  backfill_job_ = std::make_optional<BackfillJob>(ctx, name_, db_num_);
  return absl::OkStatus();
}

IndexSchema::~IndexSchema() {
  VMSDK_LOG(NOTICE, detached_ctx_.get())
      << "Index schema " << name_ << " dropped from DB " << db_num_;

  // If we are not already destructing, make sure we perform necessary cleanup.
  // Note that this will fail on background threads, so indices should be marked
  // as destructing by the main thread.
  if (!is_destructing_) {
    MarkAsDestructing();
  }
}

absl::StatusOr<std::shared_ptr<indexes::IndexBase>> IndexSchema::GetIndex(
    absl::string_view attribute_alias) const {
  auto itr = attributes_.find(std::string{attribute_alias});
  if (ABSL_PREDICT_FALSE(itr == attributes_.end())) {
    return absl::NotFoundError(
        absl::StrCat("Index field `", attribute_alias, "` does not exist"));
  }
  return itr->second.GetIndex();
}

absl::StatusOr<std::string> IndexSchema::GetIdentifier(
    absl::string_view attribute_alias) const {
  auto itr = attributes_.find(std::string{attribute_alias});
  if (itr == attributes_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Index field `", attribute_alias, "` does not exist"));
  }
  return itr->second.GetIdentifier();
}

absl::StatusOr<std::string> IndexSchema::GetAlias(
    absl::string_view identifier) const {
  auto itr = identifier_to_alias_.find(std::string{identifier});
  if (itr == identifier_to_alias_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Index Identifier `", identifier, "` does not exist"));
  }
  return itr->second;
}

absl::StatusOr<vmsdk::UniqueValkeyString> IndexSchema::DefaultReplyScoreAs(
    absl::string_view attribute_alias) const {
  auto itr = attributes_.find(std::string{attribute_alias});
  if (ABSL_PREDICT_FALSE(itr == attributes_.end())) {
    return absl::NotFoundError(
        absl::StrCat("Index field `", attribute_alias, "` does not exist"));
  }
  return itr->second.DefaultReplyScoreAs();
}

absl::Status IndexSchema::AddIndex(absl::string_view attribute_alias,
                                   absl::string_view identifier,
                                   std::shared_ptr<indexes::IndexBase> index) {
  auto [_, res] =
      attributes_.insert({std::string(attribute_alias),
                          Attribute{attribute_alias, identifier, index}});
  if (!res) {
    return absl::AlreadyExistsError(
        absl::StrCat("Index field `", attribute_alias, "` already exists"));
  }
  identifier_to_alias_.insert(
      {std::string(identifier), std::string(attribute_alias)});
  return absl::OkStatus();
}

void TrackResults(
    ValkeyModuleCtx *ctx, const absl::StatusOr<bool> &status,
    const char *operation_str,
    IndexSchema::Stats::ResultCnt<std::atomic<uint64_t>> &counter) {
  if (ABSL_PREDICT_FALSE(!status.ok())) {
    ++counter.failure_cnt;
    // Track global ingestion failures
    Metrics::GetStats().ingest_total_failures++;
  } else if (status.value()) {
    ++counter.success_cnt;
  } else {
    ++counter.skipped_cnt;
  }
  // Separate errors and successes so that they log on different timers.
  if (ABSL_PREDICT_TRUE(status.ok())) {
    VMSDK_LOG_EVERY_N_SEC(GetLogSeverity(status.ok()), ctx, 5)
        << operation_str
        << " succeeded with result: " << status.status().ToString();
  } else {
    VMSDK_LOG_EVERY_N_SEC(GetLogSeverity(status.ok()), ctx, 1)
        << operation_str
        << " failed with result: " << status.status().ToString();
  }
}

void IndexSchema::OnKeyspaceNotification(ValkeyModuleCtx *ctx, int type,
                                         const char *event,
                                         ValkeyModuleString *key) {
  if (ABSL_PREDICT_FALSE(!IsInCurrentDB(ctx))) {
    return;
  }
  ProcessKeyspaceNotification(ctx, key, false);
}

bool AddAttributeData(IndexSchema::MutatedAttributes &mutated_attributes,
                      const Attribute &attribute,
                      AttributeDataType &attribute_data_type,
                      vmsdk::UniqueValkeyString record) {
  if (record) {
    if (attribute_data_type.RecordsProvidedAsString()) {
      auto normalized_record =
          attribute.GetIndex()->NormalizeStringRecord(std::move(record));
      if (!normalized_record) {
        return false;
      }
      mutated_attributes[attribute.GetAlias()].data =
          std::move(normalized_record);
    } else {
      mutated_attributes[attribute.GetAlias()].data = std::move(record);
    }
  } else {
    mutated_attributes[attribute.GetAlias()].data = nullptr;
  }
  return true;
}

void IndexSchema::ProcessKeyspaceNotification(ValkeyModuleCtx *ctx,
                                              ValkeyModuleString *key,
                                              bool from_backfill) {
  auto key_cstr = vmsdk::ToStringView(key);
  if (key_cstr.empty()) {
    return;
  }
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx, key, VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
  // Fail fast if the key type does not match the data type.
  if (key_obj && !GetAttributeDataType().IsProperType(key_obj.get())) {
    return;
  }
  MutatedAttributes mutated_attributes;
  bool added = false;
  auto interned_key = StringInternStore::Intern(key_cstr);
  for (const auto &attribute_itr : attributes_) {
    auto &attribute = attribute_itr.second;
    if (!key_obj) {
      added = true;
      mutated_attributes[attribute_itr.first] = {
          nullptr, indexes::DeletionType::kRecord};
      continue;
    }
    bool is_module_owned;
    vmsdk::UniqueValkeyString record = VectorExternalizer::Instance().GetRecord(
        ctx, attribute_data_type_.get(), key_obj.get(), key_cstr,
        attribute.GetIdentifier(), is_module_owned);
    // Early return on record not found just if the record not tracked.
    // Otherwise, it will be processed as a delete
    if (!record && !attribute.GetIndex()->IsTracked(interned_key) &&
        !InTrackedMutationRecords(interned_key, attribute.GetIdentifier())) {
      return;
    }
    if (!is_module_owned) {
      // A record which are owned by the module were not modified and are
      // already tracked in the vector registry.
      VectorExternalizer(interned_key, attribute.GetIdentifier(), record);
    }
    if (AddAttributeData(mutated_attributes, attribute, *attribute_data_type_,
                         std::move(record))) {
      added = true;
    }
  }
  if (added) {
    switch (attribute_data_type_->ToProto()) {
      case data_model::ATTRIBUTE_DATA_TYPE_HASH:
        if (from_backfill) {
          Metrics::GetStats().backfill_hash_keys++;
        } else {
          Metrics::GetStats().ingest_hash_keys++;
        }
        break;
      case data_model::ATTRIBUTE_DATA_TYPE_JSON:
        if (from_backfill) {
          Metrics::GetStats().backfill_json_keys++;
        } else {
          Metrics::GetStats().ingest_json_keys++;
        }
        break;
      default:
        CHECK(false);
    }
    ProcessMutation(ctx, mutated_attributes, interned_key, from_backfill);
  }
}

bool IndexSchema::IsTrackedByAnyIndex(const InternedStringPtr &key) const {
  return std::any_of(attributes_.begin(), attributes_.end(),
                     [&key](const auto &attribute) {
                       return attribute.second.GetIndex()->IsTracked(key);
                     });
}

void IndexSchema::SyncProcessMutation(ValkeyModuleCtx *ctx,
                                      MutatedAttributes &mutated_attributes,
                                      const InternedStringPtr &key) {
  vmsdk::WriterMutexLock lock(&time_sliced_mutex_);
  for (auto &attribute_data_itr : mutated_attributes) {
    const auto itr = attributes_.find(attribute_data_itr.first);
    if (itr == attributes_.end()) {
      continue;
    }
    ProcessAttributeMutation(ctx, itr->second, key,
                             std::move(attribute_data_itr.second.data),
                             attribute_data_itr.second.deletion_type);
  }
}

void IndexSchema::ProcessAttributeMutation(
    ValkeyModuleCtx *ctx, const Attribute &attribute,
    const InternedStringPtr &key, vmsdk::UniqueValkeyString data,
    indexes::DeletionType deletion_type) {
  auto index = attribute.GetIndex();
  if (data) {
    DCHECK(deletion_type == indexes::DeletionType::kNone);
    auto data_view = vmsdk::ToStringView(data.get());
    if (index->IsTracked(key)) {
      auto res = index->ModifyRecord(key, data_view);
      TrackResults(ctx, res, "Modify", stats_.subscription_modify);
      if (res.ok() && res.value()) {
        ++Metrics::GetStats().time_slice_upserts;
      }
      return;
    }
    bool was_tracked = IsTrackedByAnyIndex(key);
    auto res = index->AddRecord(key, data_view);
    TrackResults(ctx, res, "Add", stats_.subscription_add);

    if (res.ok() && res.value()) {
      ++Metrics::GetStats().time_slice_upserts;
      // Increment the hash key count if it wasn't tracked and we successfully
      // added it to the index.
      if (!was_tracked) {
        ++stats_.document_cnt;
      }

      // Track field type counters
      switch (index->GetIndexerType()) {
        case indexes::IndexerType::kVector:
        case indexes::IndexerType::kHNSW:
        case indexes::IndexerType::kFlat:
          Metrics::GetStats().ingest_field_vector++;
          break;
        case indexes::IndexerType::kNumeric:
          Metrics::GetStats().ingest_field_numeric++;
          break;
        case indexes::IndexerType::kTag:
          Metrics::GetStats().ingest_field_tag++;
          break;
        default:
          // Shouldn't happen
          break;
      }
    }
    return;
  }

  auto res = index->RemoveRecord(key, deletion_type);
  TrackResults(ctx, res, "Remove", stats_.subscription_remove);
  if (res.ok() && res.value()) {
    ++Metrics::GetStats().time_slice_deletes;
    // Reduce the hash key count if nothing is tracking the key anymore.
    if (!IsTrackedByAnyIndex(key)) {
      --stats_.document_cnt;
    }
  }
}

std::unique_ptr<vmsdk::StopWatch> CreateQueueDelayCapturer() {
  std::unique_ptr<vmsdk::StopWatch> ret;
  thread_local int cnt{0};
  ++cnt;
  if (ABSL_PREDICT_FALSE(cnt % 1000 == 0)) {
    ret = std::make_unique<vmsdk::StopWatch>();
  }
  return ret;
}

// ProcessMultiQueue is used to flush pending mutations occurring in a
// multi/exec transaction. This function is called lazily on the next FT.SEARCH
// command.
void IndexSchema::ProcessMultiQueue() {
  schedule_multi_exec_processing_ = false;
  auto &multi_mutations = multi_mutations_.Get();
  if (ABSL_PREDICT_TRUE(multi_mutations.keys.empty())) {
    return;
  }

  // Track batch metrics
  Metrics::GetStats().ingest_last_batch_size = multi_mutations.keys.size();
  Metrics::GetStats().ingest_total_batches++;

  multi_mutations.blocking_counter =
      std::make_unique<absl::BlockingCounter>(multi_mutations.keys.size());
  vmsdk::WriterMutexLock lock(&time_sliced_mutex_);
  while (!multi_mutations.keys.empty()) {
    auto key = multi_mutations.keys.front();
    multi_mutations.keys.pop_front();
    ScheduleMutation(false, key, vmsdk::ThreadPool::Priority::kMax,
                     multi_mutations.blocking_counter.get());
  }
  multi_mutations.blocking_counter->Wait();
  multi_mutations.blocking_counter.reset();
}

void IndexSchema::EnqueueMultiMutation(const InternedStringPtr &key) {
  auto &multi_mutations = multi_mutations_.Get();
  multi_mutations.keys.push_back(key);
  if (multi_mutations.keys.size() >= mutations_thread_pool_->Size() &&
      !schedule_multi_exec_processing_.Get()) {
    schedule_multi_exec_processing_.Get() = true;
    vmsdk::RunByMain(
        [weak_index_schema = GetWeakPtr()]() mutable {
          auto index_schema = weak_index_schema.lock();
          if (!index_schema) {
            return;
          }
          index_schema->ProcessMultiQueue();
        },
        true);
  }
}

void IndexSchema::ScheduleMutation(bool from_backfill,
                                   const InternedStringPtr &key,
                                   vmsdk::ThreadPool::Priority priority,
                                   absl::BlockingCounter *blocking_counter) {
  {
    absl::MutexLock lock(&stats_.mutex_);
    ++stats_.mutation_queue_size_;
    if (ABSL_PREDICT_FALSE(from_backfill)) {
      ++stats_.backfill_inqueue_tasks;
    }
  }
  mutations_thread_pool_->Schedule(
      [from_backfill, weak_index_schema = GetWeakPtr(),
       ctx = detached_ctx_.get(), delay_capturer = CreateQueueDelayCapturer(),
       key_str = std::move(key), blocking_counter]() mutable {
        PAUSEPOINT("block_mutation_queue");
        auto index_schema = weak_index_schema.lock();
        if (ABSL_PREDICT_FALSE(!index_schema)) {
          return;
        }
        index_schema->ProcessSingleMutationAsync(ctx, from_backfill, key_str,
                                                 delay_capturer.get());
        if (ABSL_PREDICT_FALSE(blocking_counter)) {
          blocking_counter->DecrementCount();
        }
      },
      priority);
}

bool ShouldBlockClient(ValkeyModuleCtx *ctx, bool inside_multi_exec,
                       bool from_backfill) {
  return !inside_multi_exec && !from_backfill && vmsdk::IsRealUserClient(ctx);
}

void IndexSchema::ProcessMutation(ValkeyModuleCtx *ctx,
                                  MutatedAttributes &mutated_attributes,
                                  const InternedStringPtr &interned_key,
                                  bool from_backfill) {
  if (ABSL_PREDICT_FALSE(!mutations_thread_pool_ ||
                         mutations_thread_pool_->Size() == 0)) {
    SyncProcessMutation(ctx, mutated_attributes, interned_key);
    return;
  }
  const bool inside_multi_exec = vmsdk::MultiOrLua(ctx);
  if (ABSL_PREDICT_FALSE(inside_multi_exec)) {
    EnqueueMultiMutation(interned_key);
  }
  const bool block_client =
      ShouldBlockClient(ctx, inside_multi_exec, from_backfill);

  if (ABSL_PREDICT_FALSE(!TrackMutatedRecord(
          ctx, interned_key, std::move(mutated_attributes), from_backfill,
          block_client, inside_multi_exec)) ||
      inside_multi_exec) {
    // Skip scheduling if the mutation key has already been tracked or is part
    // of a multi exec command.
    return;
  }
  const vmsdk::ThreadPool::Priority priority =
      from_backfill ? vmsdk::ThreadPool::Priority::kLow
                    : vmsdk::ThreadPool::Priority::kHigh;
  ScheduleMutation(from_backfill, interned_key, priority, nullptr);
}

void IndexSchema::ProcessSingleMutationAsync(ValkeyModuleCtx *ctx,
                                             bool from_backfill,
                                             const InternedStringPtr &key,
                                             vmsdk::StopWatch *delay_capturer) {
  bool first_time = true;
  do {
    auto mutation_record = ConsumeTrackedMutatedAttribute(key, first_time);
    first_time = false;
    if (!mutation_record.has_value()) {
      break;
    }
    SyncProcessMutation(ctx, mutation_record.value(), key);
  } while (true);
  absl::MutexLock lock(&stats_.mutex_);
  --stats_.mutation_queue_size_;
  if (ABSL_PREDICT_FALSE(from_backfill)) {
    --stats_.backfill_inqueue_tasks;
  }
  if (ABSL_PREDICT_FALSE(delay_capturer)) {
    stats_.mutations_queue_delay_ = delay_capturer->Duration();
  }
}

void IndexSchema::BackfillScanCallback(ValkeyModuleCtx *ctx,
                                       ValkeyModuleString *keyname,
                                       ValkeyModuleKey *key, void *privdata) {
  IndexSchema *index_schema = reinterpret_cast<IndexSchema *>(privdata);
  index_schema->backfill_job_.Get()->scanned_key_count++;
  auto key_prefixes = index_schema->GetKeyPrefixes();
  auto key_cstr = vmsdk::ToStringView(keyname);
  if (std::any_of(key_prefixes.begin(), key_prefixes.end(),
                  [&key_cstr](const auto &key_prefix) {
                    return key_cstr.starts_with(key_prefix);
                  })) {
    index_schema->ProcessKeyspaceNotification(ctx, keyname, true);
  }
}

CONTROLLED_BOOLEAN(StopBackfill, false);

uint32_t IndexSchema::PerformBackfill(ValkeyModuleCtx *ctx,
                                      uint32_t batch_size) {
  auto &backfill_job = backfill_job_.Get();
  if (!backfill_job.has_value() || backfill_job->IsScanDone()) {
    return 0;
  }

  if (StopBackfill.GetValue()) {
    VMSDK_LOG_EVERY_N_SEC(NOTICE, ctx, 1) << "Backfill stopped by request";
    return 0;
  }

  backfill_job->paused_by_oom = false;

  // We need to ensure the DB size is monotonically increasing, since it could
  // change during the backfill, in which case we may show incorrect progress.
  backfill_job->db_size =
      std::max(backfill_job->db_size,
               (uint64_t)ValkeyModule_DbSize(backfill_job->scan_ctx.get()));

  uint64_t start_scan_count = backfill_job->scanned_key_count;
  uint64_t &current_scan_count = backfill_job->scanned_key_count;
  while (current_scan_count - start_scan_count < batch_size) {
    auto ctx_flags = ValkeyModule_GetContextFlags(ctx);
    if (ctx_flags & VALKEYMODULE_CTX_FLAGS_OOM) {
      backfill_job->paused_by_oom = true;
      return 0;
    }

    // Scan will return zero if there are no more keys to scan. This could be
    // the case either if there are no keys at all or if we have reached the
    // end of the current iteration. Because of this, we use the scanned key
    // count to know how many keys we have scanned in total (either zero or
    // one).
    if (!ValkeyModule_Scan(backfill_job->scan_ctx.get(),
                           backfill_job->cursor.get(), BackfillScanCallback,
                           (void *)this)) {
      VMSDK_LOG(NOTICE, ctx)
          << "Index schema " << name_ << " finished backfill. Scanned "
          << backfill_job->scanned_key_count << " keys in "
          << absl::FormatDuration(backfill_job->stopwatch.Duration());
      uint32_t res = current_scan_count - start_scan_count;
      backfill_job->MarkScanAsDone();
      return res;
    }
  }
  return current_scan_count - start_scan_count;
}

float IndexSchema::GetBackfillPercent() const {
  const auto &backfill_job = backfill_job_.Get();
  if (!IsBackfillInProgress() || (backfill_job->db_size == 0)) {
    return 1;
  }
  DCHECK(backfill_job->scanned_key_count >= stats_.backfill_inqueue_tasks);
  auto processed_keys =
      backfill_job->scanned_key_count - stats_.backfill_inqueue_tasks;
  if (backfill_job->scanned_key_count < stats_.backfill_inqueue_tasks ||
      backfill_job->db_size < processed_keys) {
    // This is a special case. Our scanned key count could be bigger than the
    // DB size if we have resized the hash table during the scan, causing us
    // to reiterate over keys we have already processed. The number of keys
    // double counted should be relatively small. Because of this, we report
    // very close to 100% indicate we are almost done. We shouldn't be in this
    // state for long.
    return 0.99;
  }
  return (float)processed_keys / backfill_job->db_size;
}

absl::string_view IndexSchema::GetStateForInfo() const {
  if (!IsBackfillInProgress()) {
    return "ready";
  } else {
    if (backfill_job_.Get()->paused_by_oom) {
      return "backfill_paused_by_oom";
    } else {
      return "backfill_in_progress";
    }
  }
}

uint64_t IndexSchema::CountRecords() const {
  uint64_t record_cnt = 0;
  for (const auto &attribute : attributes_) {
    record_cnt += attribute.second.GetIndex()->GetTrackedKeyCount();
  }
  return record_cnt;
}

void IndexSchema::RespondWithInfo(ValkeyModuleCtx *ctx) const {
  ValkeyModule_ReplyWithArray(ctx, 22);
  ValkeyModule_ReplyWithSimpleString(ctx, "index_name");
  ValkeyModule_ReplyWithSimpleString(ctx, name_.data());

  ValkeyModule_ReplyWithSimpleString(ctx, "index_definition");
  ValkeyModule_ReplyWithArray(ctx, 6);
  ValkeyModule_ReplyWithSimpleString(ctx, "key_type");
  ValkeyModule_ReplyWithSimpleString(ctx,
                                     attribute_data_type_->ToString().c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "prefixes");
  ValkeyModule_ReplyWithArray(ctx, subscribed_key_prefixes_.size());
  for (const auto &prefix : subscribed_key_prefixes_) {
    ValkeyModule_ReplyWithSimpleString(ctx, prefix.c_str());
  }
  // hard-code default score of 1 as it's the only value we currently
  // supported.
  ValkeyModule_ReplyWithSimpleString(ctx, "default_score");
  ValkeyModule_ReplyWithCString(ctx, "1");

  ValkeyModule_ReplyWithSimpleString(ctx, "attributes");
  ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
  int attribute_array_len = 0;
  for (const auto &attribute : attributes_) {
    attribute_array_len += attribute.second.RespondWithInfo(ctx);
  }
  ValkeyModule_ReplySetArrayLength(ctx, attribute_array_len);

  ValkeyModule_ReplyWithSimpleString(ctx, "num_docs");
  ValkeyModule_ReplyWithLongLong(ctx, stats_.document_cnt);
  ValkeyModule_ReplyWithSimpleString(ctx, "num_records");
  ValkeyModule_ReplyWithLongLong(ctx, CountRecords());
  ValkeyModule_ReplyWithSimpleString(ctx, "hash_indexing_failures");
  ValkeyModule_ReplyWithCString(
      ctx, absl::StrFormat("%lu", stats_.subscription_add.skipped_cnt).c_str());

  ValkeyModule_ReplyWithSimpleString(ctx, "backfill_in_progress");
  ValkeyModule_ReplyWithCString(
      ctx, absl::StrFormat("%d", IsBackfillInProgress() ? 1 : 0).c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "backfill_complete_percent");
  ValkeyModule_ReplyWithCString(
      ctx, absl::StrFormat("%f", GetBackfillPercent()).c_str());

  absl::MutexLock lock(&stats_.mutex_);
  ValkeyModule_ReplyWithSimpleString(ctx, "mutation_queue_size");
  ValkeyModule_ReplyWithCString(
      ctx, absl::StrFormat("%lu", stats_.mutation_queue_size_).c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "recent_mutations_queue_delay");
  ValkeyModule_ReplyWithCString(
      ctx, absl::StrFormat("%lu sec", (stats_.mutation_queue_size_ > 0
                                           ? stats_.mutations_queue_delay_ /
                                                 absl::Seconds(1)
                                           : 0))
               .c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "state");
  ValkeyModule_ReplyWithSimpleString(ctx, GetStateForInfo().data());
}

bool IsVectorIndex(std::shared_ptr<indexes::IndexBase> index) {
  return index->GetIndexerType() == indexes::IndexerType::kVector ||
         index->GetIndexerType() == indexes::IndexerType::kHNSW ||
         index->GetIndexerType() == indexes::IndexerType::kFlat;
}

std::unique_ptr<data_model::IndexSchema> IndexSchema::ToProto() const {
  auto index_schema_proto = std::make_unique<data_model::IndexSchema>();
  index_schema_proto->set_name(this->name_);
  index_schema_proto->set_db_num(db_num_);
  index_schema_proto->mutable_subscribed_key_prefixes()->Add(
      subscribed_key_prefixes_.begin(), subscribed_key_prefixes_.end());
  index_schema_proto->set_attribute_data_type(attribute_data_type_->ToProto());
  auto stats = index_schema_proto->mutable_stats();
  stats->set_documents_count(stats_.document_cnt);
  std::transform(
      attributes_.begin(), attributes_.end(),
      google::protobuf::RepeatedPtrFieldBackInserter(
          index_schema_proto->mutable_attributes()),
      [](const auto &attribute) { return *attribute.second.ToProto(); });
  return index_schema_proto;
}

static absl::Status SaveSupplementalSection(
    SafeRDB *rdb, data_model::SupplementalContentType type,
    std::function<void(data_model::SupplementalContentHeader &)> init,
    absl::AnyInvocable<absl::Status(RDBChunkOutputStream)> write_section) {
  rdb_save_sections.Increment();
  auto header = std::make_unique<data_model::SupplementalContentHeader>();
  header->set_type(type);
  VMSDK_LOG(NOTICE, nullptr) << "Writing supplemental section type "
                             << data_model::SupplementalContentType_Name(type);
  init(*header);
  auto header_str = header->SerializeAsString();
  VMSDK_RETURN_IF_ERROR(rdb->SaveStringBuffer(header_str));
  return write_section(RDBChunkOutputStream(rdb));
}

absl::Status IndexSchema::RDBSave(SafeRDB *rdb) const {
  auto index_schema_proto = ToProto();
  auto rdb_section = std::make_unique<data_model::RDBSection>();
  rdb_section->set_type(data_model::RDB_SECTION_INDEX_SCHEMA);
  rdb_section->set_allocated_index_schema_contents(
      index_schema_proto.release());

  size_t supplemental_count =
      GetAttributeCount() +
      std::count_if(attributes_.begin(), attributes_.end(),
                    [](const auto &attribute) {
                      return IsVectorIndex(attribute.second.GetIndex());
                    });
  if (RDBWriteV2()) {
    supplemental_count += 1;  // For Index Extension
  }
  rdb_section->set_supplemental_count(supplemental_count);

  auto rdb_section_string = rdb_section->SerializeAsString();
  VMSDK_RETURN_IF_ERROR(rdb->SaveStringBuffer(rdb_section_string))
      << "IO error while saving IndexSchema name: " << this->name_
      << " in DB: " << this->db_num_ << " to RDB";

  for (auto &attribute : attributes_) {
    VMSDK_LOG(NOTICE, nullptr)
        << "Starting to save attribute: " << attribute.second.GetAlias();
    // Note that the serialized attribute proto is also stored as part of the
    // serialized index schema proto above. We store here again to avoid any
    // dependencies on the ordering of multiple attributes.
    // We could remove the duplication in the future.
    VMSDK_RETURN_IF_ERROR(SaveSupplementalSection(
        rdb, data_model::SUPPLEMENTAL_CONTENT_INDEX_CONTENT,
        [&](auto &header) {
          header.mutable_index_content_header()->set_allocated_attribute(
              attribute.second.ToProto().release());
        },
        std::bind_front(&indexes::IndexBase::SaveIndex,
                        attribute.second.GetIndex())));

    // Key to ID mapping is stored as a separate chunked supplemental content
    // for vector indexes.
    if (IsVectorIndex(attribute.second.GetIndex())) {
      VMSDK_RETURN_IF_ERROR(SaveSupplementalSection(
          rdb, data_model::SUPPLEMENTAL_CONTENT_KEY_TO_ID_MAP,
          [&](auto &header) {
            header.mutable_key_to_id_map_header()->set_allocated_attribute(
                attribute.second.ToProto().release());
          },
          std::bind_front(&indexes::VectorBase::SaveTrackedKeys,
                          dynamic_cast<const indexes::VectorBase *>(
                              attribute.second.GetIndex().get()))));
    }
  }

  if (RDBWriteV2()) {
    VMSDK_RETURN_IF_ERROR(SaveSupplementalSection(
        rdb, data_model::SUPPLEMENTAL_CONTENT_INDEX_EXTENSION,
        [&](auto &header) {
          header.mutable_mutation_queue_header()->set_backfilling(
              IsBackfillInProgress());
          VMSDK_LOG(NOTICE, nullptr)
              << "RDB: Saving Index Extension Backfill = "
              << header.mutation_queue_header().backfilling();
        },
        std::bind_front(&IndexSchema::SaveIndexExtension, this)));
  }

  return absl::OkStatus();
}

absl::Status IndexSchema::ValidateIndex() const {
  absl::Status status = absl::OkStatus();
 //
  // Find a non-vector index as the oracle
  // If all indexes are vector indexes, no validation is needed
  //
  std::shared_ptr<indexes::IndexBase> oracle_index = nullptr;
  std::string oracle_name;
  
  for (const auto &attribute : attributes_) {
    if (!IsVectorIndex(attribute.second.GetIndex())) {
      oracle_index = attribute.second.GetIndex();
      oracle_name = attribute.first;
      break;
    }
  }
  
  // If no non-vector index found, all indexes are vectors - no validation needed
  if (oracle_index == nullptr) {
    return absl::OkStatus();
  }
  size_t oracle_key_count =
      oracle_index->GetTrackedKeyCount() + oracle_index->GetUnTrackedKeyCount();
  //
  // Now, make sure all the other indexes have the same key count, except for
  // vector indexes which may have less keys
  //
  for (const auto &[name, attr] : attributes_) {
    auto idx = attr.GetIndex();
    size_t cnt = idx->GetTrackedKeyCount() + idx->GetUnTrackedKeyCount();
    if (IsVectorIndex(idx) ? cnt <= oracle_key_count
                           : cnt == oracle_key_count) {
      continue;
    }
    VMSDK_LOG(WARNING, nullptr)
        << "Index validation failed for index " << name
        << " expected key count " << oracle_key_count << " got " << cnt;
    //
    // Ok, do a detailed comparison
    //
    auto larger_index = (cnt > oracle_key_count) ? idx : oracle_index;
    auto larger_name = (cnt > oracle_key_count) ? name : oracle_name;
    auto smaller_index = (cnt > oracle_key_count) ? oracle_index : idx;
    auto smaller_name = (cnt > oracle_key_count) ? oracle_name : name;
    auto key_check = [&](const InternedStringPtr &key) {
      if (!smaller_index->IsTracked(key) && !smaller_index->IsUnTracked(key)) {
        VMSDK_LOG(WARNING, nullptr)
            << "Key found in " << larger_name << " not found in "
            << smaller_name << ": " << key->Str();
        status = absl::InternalError(
            absl::StrCat("Key found in ", larger_name, " not found in ",
                         smaller_name, ": ", key->Str()));
      }
      return absl::OkStatus();
    };
    auto status1 = larger_index->ForEachTrackedKey(key_check);
    if (!status1.ok()) {
      status = status1;
    }
    auto status2 = larger_index->ForEachUnTrackedKey(key_check);
    if (!status2.ok()) {
      status = status2;
    }
  }
  return status;
}

absl::Status IndexSchema::SaveIndexExtension(RDBChunkOutputStream out) const {
  if (RDBValidateOnWrite()) {
    VMSDK_RETURN_IF_ERROR(ValidateIndex());
  }
  //
  // To reconstruct an index-schema, we want to ingest all of the keys that are
  // currently within the index. If there is a non-vector index, we can use the
  // tracked and untracked key lists from that index. If there is ONLY vector
  // indexes, then this key list is not needed as there aren't any non-vector
  // indexes to ingest.
  //
  // The V1 format doesn't have this list and substitutes a backfill to rebuild.
  // In the absence of support for SKIPINITIALSCAN the backfill is sufficient to
  // determine which keys are in the index. However, once we support this option
  // it's no longer possible to determine which keys are in the index without
  // storing them explicitly. Thus the V2 format includes this key list
  // explicitly which will trivially enable the SKIPINITIALSCAN option.
  //
  std::shared_ptr<indexes::IndexBase> index;
  for (const auto &attribute : attributes_) {
    if (!IsVectorIndex(attribute.second.GetIndex())) {
      index = attribute.second.GetIndex();
      break;
    }
  }
  if (!index) {
    VMSDK_RETURN_IF_ERROR(out.SaveObject<size_t>(0));  // zero keys
  } else {
    size_t key_count =
        index->GetTrackedKeyCount() + index->GetUnTrackedKeyCount();
    VMSDK_RETURN_IF_ERROR(out.SaveObject(key_count));
    rdb_save_keys.Increment(key_count);
    VMSDK_LOG(DEBUG, nullptr)
        << "Writing Index Extension, keys = " << key_count;

    auto write_a_key = [&](const InternedStringPtr &key) {
      key_count--;
      return out.SaveString(key->Str());
    };
    VMSDK_RETURN_IF_ERROR(index->ForEachTrackedKey(write_a_key));
    VMSDK_RETURN_IF_ERROR(index->ForEachUnTrackedKey(write_a_key));
    CHECK(key_count == 0) << "Key count mismatch for index " << GetName();
  }
  //
  // Write out the mutation queue entries
  //
  VMSDK_LOG(DEBUG, nullptr) << "Writing Mutation Queue, records = "
                            << tracked_mutated_records_.size();
  VMSDK_RETURN_IF_ERROR(out.SaveObject(tracked_mutated_records_.size()));
  rdb_save_mutation_entries.Increment(tracked_mutated_records_.size());
  for (const auto &[key, value] : tracked_mutated_records_) {
    VMSDK_RETURN_IF_ERROR(out.SaveString(key->Str()));
  }
  //
  // Write out the multi/exec queued keys
  //
  VMSDK_RETURN_IF_ERROR(
      out.SaveObject<size_t>(multi_mutations_.Get().keys.size()));
  rdb_save_multi_exec_entries.Increment(multi_mutations_.Get().keys.size());
  VMSDK_LOG(DEBUG, nullptr) << "Writing Multi/Exec Queue, records = "
                            << multi_mutations_.Get().keys.size();
  for (const auto &key : multi_mutations_.Get().keys) {
    CHECK(tracked_mutated_records_.find(key) != tracked_mutated_records_.end());
    VMSDK_RETURN_IF_ERROR(out.SaveString(key->Str()));
  }
  return absl::OkStatus();
}

absl::Status IndexSchema::LoadIndexExtension(ValkeyModuleCtx *ctx,
                                             RDBChunkInputStream input) {
  VMSDK_ASSIGN_OR_RETURN(size_t key_count, input.LoadObject<size_t>());
  rdb_load_keys.Increment(key_count);
  VMSDK_LOG(DEBUG, ctx) << "Loading Index Extension, keys = " << key_count;
  for (size_t i = 0; i < key_count; ++i) {
    VMSDK_ASSIGN_OR_RETURN(auto keyname_str, input.LoadString());
    auto keyname = vmsdk::MakeUniqueValkeyString(keyname_str);
    ProcessKeyspaceNotification(ctx, keyname.get(), false);
  }
  VMSDK_ASSIGN_OR_RETURN(size_t count, input.LoadObject<size_t>());
  VMSDK_LOG(DEBUG, ctx) << "Loading Mutation Entries, entries = " << count;
  rdb_load_mutation_entries.Increment(count);
  for (size_t i = 0; i < count; ++i) {
    VMSDK_ASSIGN_OR_RETURN(auto keyname_str, input.LoadString());
    auto keyname = vmsdk::MakeUniqueValkeyString(keyname_str);
    ProcessKeyspaceNotification(ctx, keyname.get(), false);
  }
  VMSDK_ASSIGN_OR_RETURN(size_t multi_count, input.LoadObject<size_t>());
  rdb_load_multi_exec_entries.Increment(multi_count);
  VMSDK_LOG(DEBUG, ctx) << "Loading Multi/Exec Entries, entries = "
                        << multi_count;
  for (size_t i = 0; i < count; ++i) {
    VMSDK_ASSIGN_OR_RETURN(auto keyname_str, input.LoadString());
    if (tracked_mutated_records_.find(StringInternStore::Intern(keyname_str)) ==
        tracked_mutated_records_.end()) {
      VMSDK_LOG(WARNING, ctx)
          << "Multi/Exec key not found in mutation tracked records: "
          << keyname_str;
      return absl::InternalError(
          "Multi/Exec key not found in mutation tracked records");
    }
    auto keyname = StringInternStore::Intern(keyname_str);
    EnqueueMultiMutation(keyname);
  }
  loaded_v2_ = true;
  return absl::OkStatus();
}

// We need to iterate over the chunks to consume them
static absl::Status SkipSupplementalContent(
    SupplementalContentIter &supplemental_iter, std::string_view reason) {
  rdb_load_sections_skipped.Increment();
  VMSDK_LOG(NOTICE, nullptr)
      << "Skipping supplemental content section (" << reason << ")";
  auto chunk_it = supplemental_iter.IterateChunks();
  while (chunk_it.HasNext()) {
    VMSDK_ASSIGN_OR_RETURN([[maybe_unused]] auto chunk_result, chunk_it.Next());
  }
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<IndexSchema>> IndexSchema::LoadFromRDB(
    ValkeyModuleCtx *ctx, vmsdk::ThreadPool *mutations_thread_pool,
    std::unique_ptr<data_model::IndexSchema> index_schema_proto,
    SupplementalContentIter &&supplemental_iter) {
  // flag to skip loading attributes and indices
  bool skip_loading_index_data = options::GetSkipIndexLoad().GetValue();
  // When skipping index data, create attributes immediately (with empty
  // indexes)
  bool load_attributes_on_create = skip_loading_index_data;
  // Attributes will be loaded from supplemental content. if
  // !load_attributes_on_create
  VMSDK_ASSIGN_OR_RETURN(
      auto index_schema,
      IndexSchema::Create(ctx, *index_schema_proto, mutations_thread_pool,
                          !load_attributes_on_create, true));

  // Supplemental content will include indices and any content for them
  while (supplemental_iter.HasNext()) {
    rdb_load_sections.Increment();
    VMSDK_ASSIGN_OR_RETURN(auto supplemental_content, supplemental_iter.Next());
    if (skip_loading_index_data) {
      VMSDK_RETURN_IF_ERROR(
          SkipSupplementalContent(supplemental_iter, "due to configuration"));
    } else {
      switch (supplemental_content->type()) {
        case data_model::SupplementalContentType::
            SUPPLEMENTAL_CONTENT_INDEX_CONTENT: {
          auto &attribute =
              supplemental_content->index_content_header().attribute();
          VMSDK_LOG(NOTICE, nullptr)
              << "Loading Index Content for attribute: " << attribute.alias();
          VMSDK_ASSIGN_OR_RETURN(
              std::shared_ptr<indexes::IndexBase> index,
              IndexFactory(ctx, index_schema.get(), attribute,
                           supplemental_iter.IterateChunks()));
          VMSDK_RETURN_IF_ERROR(index_schema->AddIndex(
              attribute.alias(), attribute.identifier(), index));
          break;
        }
        case data_model::SupplementalContentType::
            SUPPLEMENTAL_CONTENT_KEY_TO_ID_MAP: {
          auto &attribute =
              supplemental_content->key_to_id_map_header().attribute();
          VMSDK_LOG(NOTICE, nullptr)
              << "Loading Key to ID Map Content for attribute: "
              << attribute.alias();
          VMSDK_ASSIGN_OR_RETURN(
              auto index, index_schema->GetIndex(attribute.alias()),
              _ << "Key to ID mapping found before index definition.");
          if (!IsVectorIndex(index)) {
            return absl::InternalError(
                "Key to ID mapping found for non vector index ");
          }
          auto vector_index = dynamic_cast<indexes::VectorBase *>(index.get());
          VMSDK_RETURN_IF_ERROR(vector_index->LoadTrackedKeys(
              ctx, &index_schema->GetAttributeDataType(),
              supplemental_iter.IterateChunks()));
          break;
        }
        case data_model::SupplementalContentType::
            SUPPLEMENTAL_CONTENT_INDEX_EXTENSION: {
          VMSDK_LOG(NOTICE, nullptr) << "Loading Mutation Queue";
          if (!RDBReadV2()) {
            VMSDK_RETURN_IF_ERROR(
                SkipSupplementalContent(supplemental_iter, "mutation queue"));
          } else {
            if (index_schema) {
              VMSDK_RETURN_IF_ERROR(index_schema->LoadIndexExtension(
                  ctx, RDBChunkInputStream(supplemental_iter.IterateChunks())));
              bool backfilling =
                  supplemental_content->mutation_queue_header().backfilling();
              if (!backfilling) {
                VMSDK_LOG(DEBUG, ctx) << "Backfill suppressed.";
                index_schema->backfill_job_.Get() = std::nullopt;
              }
            } else {
              return absl::InternalError(
                  "Supplemental section mutation queue out of order");
            }
          }
          break;
        }
        default:
          VMSDK_LOG(NOTICE, ctx) << "Unknown supplemental content type: "
                                 << supplemental_content->type();
          VMSDK_RETURN_IF_ERROR(
              SkipSupplementalContent(supplemental_iter, "unknown type"));
          break;
      }
    }
  }

  return index_schema;
}

bool IndexSchema::IsInCurrentDB(ValkeyModuleCtx *ctx) const {
  return ValkeyModule_GetSelectedDb(ctx) == db_num_;
}

void IndexSchema::OnSwapDB(ValkeyModuleSwapDbInfo *swap_db_info) {
  uint32_t curr_db = db_num_;
  uint32_t db_to_swap_to;
  if (curr_db == swap_db_info->dbnum_first) {
    db_to_swap_to = swap_db_info->dbnum_second;
  } else if (curr_db == swap_db_info->dbnum_second) {
    db_to_swap_to = swap_db_info->dbnum_first;
  } else {
    return;
  }
  db_num_ = db_to_swap_to;
  auto &backfill_job = backfill_job_.Get();
  if (IsBackfillInProgress() && !backfill_job->IsScanDone()) {
    ValkeyModule_SelectDb(backfill_job->scan_ctx.get(), db_to_swap_to);
  }
}

void IndexSchema::OnLoadingEnded(ValkeyModuleCtx *ctx) {
  if (loaded_v2_) {
    VMSDK_LOG(NOTICE, ctx) << "RDB load completed, "
                           << " Mutation Queue contains "
                           << tracked_mutated_records_.size() << " entries."
                           << (backfill_job_.Get().has_value()
                                   ? " Backfill still required."
                                   : " Backfill not needed.");
    return;
  }
  // Clean up any potentially stale index entries that can arise from
  // pending record deletions being lost during RDB save.
  vmsdk::StopWatch stop_watch;
  ValkeyModule_SelectDb(ctx, db_num_);  // Make sure we are in the right DB.
  absl::flat_hash_map<std::string, MutatedAttributes> deletion_attributes;
  for (const auto &attribute : attributes_) {
    const auto &index = attribute.second.GetIndex();
    std::vector<std::string> to_delete;
    uint64_t key_size = 0;
    uint64_t stale_entries = 0;
    auto status = index->ForEachTrackedKey([ctx, &deletion_attributes,
                                            &key_size, &attribute,
                                            &stale_entries](
                                               const InternedStringPtr &key) {
      auto r_str = vmsdk::MakeUniqueValkeyString(*key);
      if (!ValkeyModule_KeyExists(ctx, r_str.get())) {
        deletion_attributes[std::string(*key)][attribute.second.GetAlias()] = {
            nullptr, indexes::DeletionType::kRecord};
        stale_entries++;
      }
      key_size++;
      return absl::OkStatus();
    });
    VMSDK_LOG(NOTICE, ctx) << "Deleting " << stale_entries
                           << " stale entries of " << key_size
                           << " total keys for {Index: " << name_
                           << ", Attribute: " << attribute.first << "}";
  }
  VMSDK_LOG(NOTICE, ctx) << "Deleting " << deletion_attributes.size()
                         << " stale entries for {Index: " << name_ << "}";

  for (auto &[key, attributes] : deletion_attributes) {
    auto interned_key = std::make_shared<InternedString>(key);
    ProcessMutation(ctx, attributes, interned_key, true);
  }
  VMSDK_LOG(NOTICE, ctx) << "Scanned index schema " << name_
                         << " for stale entries in "
                         << absl::FormatDuration(stop_watch.Duration());
}

vmsdk::BlockedClientCategory IndexSchema::GetBlockedCategoryFromProto() const {
  // Determine category based on data type
  switch (attribute_data_type_->ToProto()) {
    case data_model::ATTRIBUTE_DATA_TYPE_HASH:
      return vmsdk::BlockedClientCategory::kHash;
    case data_model::ATTRIBUTE_DATA_TYPE_JSON:
      return vmsdk::BlockedClientCategory::kJson;
    default:
      return vmsdk::BlockedClientCategory::kOther;
  }
}

bool IndexSchema::InTrackedMutationRecords(
    const InternedStringPtr &key, const std::string &identifier) const {
  absl::MutexLock lock(&mutated_records_mutex_);
  auto itr = tracked_mutated_records_.find(key);
  if (ABSL_PREDICT_FALSE(itr == tracked_mutated_records_.end())) {
    return false;
  }
  if (itr->second.attributes->find(identifier) ==
      itr->second.attributes->end()) {
    return false;
  }
  return true;
}
// Returns true if the inserted key not exists otherwise false
bool IndexSchema::TrackMutatedRecord(ValkeyModuleCtx *ctx,
                                     const InternedStringPtr &key,
                                     MutatedAttributes &&mutated_attributes,
                                     bool from_backfill, bool block_client,
                                     bool from_multi) {
  absl::MutexLock lock(&mutated_records_mutex_);
  auto [itr, inserted] =
      tracked_mutated_records_.insert({key, DocumentMutation{}});
  if (ABSL_PREDICT_TRUE(inserted)) {
    itr->second.attributes = MutatedAttributes();
    itr->second.attributes.value() = std::move(mutated_attributes);
    itr->second.from_backfill = from_backfill;
    itr->second.from_multi = from_multi;
    if (ABSL_PREDICT_TRUE(block_client)) {
      vmsdk::BlockedClient blocked_client(ctx, true,
                                          GetBlockedCategoryFromProto());
      blocked_client.MeasureTimeStart();
      itr->second.blocked_clients.emplace_back(std::move(blocked_client));
    }
    return true;
  }

  if (!itr->second.from_multi && from_multi) {
    itr->second.from_multi = from_multi;
  }

  if (!itr->second.attributes.has_value()) {
    itr->second.attributes = MutatedAttributes();
  }
  for (auto &mutated_attribute : mutated_attributes) {
    itr->second.attributes.value()[mutated_attribute.first] =
        std::move(mutated_attribute.second);
  }

  if (ABSL_PREDICT_TRUE(block_client) &&
      ABSL_PREDICT_TRUE(!itr->second.from_multi)) {
    vmsdk::BlockedClient blocked_client(ctx, true,
                                        GetBlockedCategoryFromProto());
    blocked_client.MeasureTimeStart();
    itr->second.blocked_clients.emplace_back(std::move(blocked_client));
  }

  if (ABSL_PREDICT_FALSE(!from_backfill && itr->second.from_backfill)) {
    itr->second.from_backfill = false;
    return true;
  }
  return false;
}

void IndexSchema::MarkAsDestructing() {
  absl::MutexLock lock(&mutated_records_mutex_);
  auto status = keyspace_event_manager_->RemoveSubscription(this);
  if (!status.ok()) {
    VMSDK_LOG(WARNING, detached_ctx_.get())
        << "Failed to remove keyspace event subscription for index "
           "schema "
        << name_ << ": " << status.message();
  }
  backfill_job_.Get()->MarkScanAsDone();
  tracked_mutated_records_.clear();
  is_destructing_ = true;
}

std::optional<IndexSchema::MutatedAttributes>
IndexSchema::ConsumeTrackedMutatedAttribute(const InternedStringPtr &key,
                                            bool first_time) {
  absl::MutexLock lock(&mutated_records_mutex_);
  auto itr = tracked_mutated_records_.find(key);
  if (ABSL_PREDICT_FALSE(itr == tracked_mutated_records_.end())) {
    return std::nullopt;
  }
  if (ABSL_PREDICT_FALSE(first_time && itr->second.consume_in_progress)) {
    return std::nullopt;
  }
  itr->second.consume_in_progress = true;
  // Delete this tracked document if no additional mutations were tracked
  if (!itr->second.attributes.has_value()) {
    tracked_mutated_records_.erase(itr);
    return std::nullopt;
  }
  // Track entry is now first consumed
  auto mutated_attributes = std::move(itr->second.attributes.value());
  itr->second.attributes = std::nullopt;
  return mutated_attributes;
}

size_t IndexSchema::GetMutatedRecordsSize() const {
  absl::MutexLock lock(&mutated_records_mutex_);
  return tracked_mutated_records_.size();
}

void IndexSchema::SubscribeToVectorExternalizer(
    absl::string_view attribute_identifier, indexes::VectorBase *vector_index) {
  vector_externalizer_subscriptions_[attribute_identifier] = vector_index;
}

void IndexSchema::VectorExternalizer(const InternedStringPtr &key,
                                     absl::string_view attribute_identifier,
                                     vmsdk::UniqueValkeyString &record) {
  auto it = vector_externalizer_subscriptions_.find(attribute_identifier);
  if (it == vector_externalizer_subscriptions_.end()) {
    return;
  }
  if (record) {
    std::optional<float> magnitude;
    auto vector_str = vmsdk::ToStringView(record.get());
    InternedStringPtr interned_vector =
        it->second->InternVector(vector_str, magnitude);
    if (interned_vector) {
      VectorExternalizer::Instance().Externalize(
          key, attribute_identifier, attribute_data_type_->ToProto(),
          interned_vector, magnitude);
    }
    return;
  }
  VectorExternalizer::Instance().Remove(key, attribute_identifier,
                                        attribute_data_type_->ToProto());
}

IndexSchema::InfoIndexPartitionData IndexSchema::Stats::GetStats() const {
  absl::MutexLock lock(&mutex_);
  return InfoIndexPartitionData{
      .num_docs = document_cnt,
      .hash_indexing_failures = subscription_add.skipped_cnt,
      .backfill_inqueue_tasks = backfill_inqueue_tasks,
      .mutation_queue_size = mutation_queue_size_,
      .recent_mutations_queue_delay =
          mutation_queue_size_ > 0
              ? static_cast<uint64_t>(mutations_queue_delay_ / absl::Seconds(1))
              : 0};
}

// backfill scanned key count
uint64_t IndexSchema::GetBackfillScannedKeyCount() const {
  const auto &backfill_job = backfill_job_.Get();
  return backfill_job.has_value() ? backfill_job->scanned_key_count : 0;
}

// backfill database size
uint64_t IndexSchema::GetBackfillDbSize() const {
  const auto &backfill_job = backfill_job_.Get();
  return backfill_job.has_value() ? backfill_job->db_size : 0;
}

IndexSchema::InfoIndexPartitionData IndexSchema::GetInfoIndexPartitionData()
    const {
  InfoIndexPartitionData data = stats_.GetStats();
  data.num_records = CountRecords();
  data.backfill_scanned_count = GetBackfillScannedKeyCount();
  data.backfill_db_size = GetBackfillDbSize();
  data.backfill_complete_percent = GetBackfillPercent();
  data.backfill_in_progress = IsBackfillInProgress();
  data.state = std::string(GetStateForInfo());
  return data;
}

}  // namespace valkey_search
