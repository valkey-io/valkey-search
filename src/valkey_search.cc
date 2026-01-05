/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/valkey_search.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "src/attribute_data_type.h"
#include "src/coordinator/client_pool.h"
#include "src/coordinator/grpc_suspender.h"
#include "src/coordinator/metadata_manager.h"
#include "src/coordinator/server.h"
#include "src/coordinator/util.h"
#include "src/metrics.h"
#include "src/rdb_serialization.h"
#include "src/schema_manager.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "src/vector_externalizer.h"
#include "vmsdk/src/info.h"
#include "vmsdk/src/latency_sampler.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

using vmsdk::config::ModuleConfigManager;

static absl::NoDestructor<std::unique_ptr<ValkeySearch>> valkey_search_instance;

ValkeySearch &ValkeySearch::Instance() { return **valkey_search_instance; };

void ValkeySearch::InitInstance(std::unique_ptr<ValkeySearch> instance) {
  *valkey_search_instance = std::move(instance);
}

uint32_t ValkeySearch::GetHNSWBlockSize() const {
  return options::GetHNSWBlockSize().GetValue();
}

void ValkeySearch::SetHNSWBlockSize(uint32_t block_size) {
  options::GetHNSWBlockSize().SetValueOrLog(block_size, WARNING);
}

void ModuleInfo(ValkeyModuleInfoCtx *ctx, int for_crash_report) {
  ValkeySearch::Instance().Info(ctx, for_crash_report);
}

void AddLatencyStat(ValkeyModuleInfoCtx *ctx, absl::string_view stat_name,
                    vmsdk::LatencySampler &sampler) {
  // Latency stats are excluded unless they have values, following Valkey engine
  // logic.
  if (sampler.HasSamples()) {
    ValkeyModule_InfoAddFieldCString(ctx, stat_name.data(),
                                     sampler.GetStatsString().c_str());
  }
}
/* Note: ValkeySearch::Info may be invoked during a crashdump by the engine.
 * In such cases, any section deemed unsafe is skipped.
 * A section is considered unsafe if it involves any of the following:
 *   1. Acquiring locks
 *   2. Performing heap allocations
 *   3. Requiring execution on the main thread

 >>> This is being converted to the new machinery in vmsdk::info_field. Once
 that's done this section will be empty and it can be removed.
 */

static vmsdk::info_field::Integer human_used_memory(
    "memory", "used_memory_human",
    vmsdk::info_field::IntegerBuilder()
        .SIBytes()
        .App()
        .Computed(vmsdk::GetUsedMemoryCnt)
        .CrashSafe());

static vmsdk::info_field::Integer used_memory(
    "memory", "used_memory_bytes",
    vmsdk::info_field::IntegerBuilder()
        .App()
        .Computed(vmsdk::GetUsedMemoryCnt)
        .CrashSafe());

static vmsdk::info_field::Integer reclaimable_memory(
    "memory", "index_reclaimable_memory",
    vmsdk::info_field::IntegerBuilder()
        .App()
        .Computed([]() -> uint64_t {
          return Metrics::GetStats().reclaimable_memory;
        })
        .CrashSafe());

static vmsdk::info_field::String background_indexing_status(
    "indexing", "background_indexing_status",
    vmsdk::info_field::StringBuilder().App().ComputedCharPtr(
        []() -> const char * {
          return SchemaManager::Instance().IsIndexingInProgress()
                     ? "IN_PROGRESS"
                     : "NO_ACTIVITY";
        }));

static vmsdk::info_field::Float used_read_cpu(
    "thread-pool", "used_read_cpu",
    vmsdk::info_field::FloatBuilder().App().Computed([]() -> double {
      auto reader_thread_pool = ValkeySearch::Instance().GetReaderThreadPool();
      return reader_thread_pool->GetAvgCPUPercentage().value_or(-1);
    }));

static vmsdk::info_field::Float used_write_cpu(
    "thread-pool", "used_write_cpu",
    vmsdk::info_field::FloatBuilder().App().Computed([]() -> double {
      auto writer_thread_pool = ValkeySearch::Instance().GetWriterThreadPool();
      return writer_thread_pool->GetAvgCPUPercentage().value_or(-1);
    }));

static vmsdk::info_field::Integer ingest_hash_keys(
    "global_ingestion", "ingest_hash_keys",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return Metrics::GetStats().ingest_hash_keys;
    }));

static vmsdk::info_field::Integer backfill_hash_keys(
    "global_ingestion", "backfill_hash_keys",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return Metrics::GetStats().backfill_hash_keys;
    }));

static vmsdk::info_field::Integer ingest_hash_blocked(
    "global_ingestion", "ingest_hash_blocked",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return vmsdk::BlockedClientTracker::GetInstance().GetClientCount(
          vmsdk::BlockedClientCategory::kHash);
    }));

static vmsdk::info_field::Integer ingest_json_keys(
    "global_ingestion", "ingest_json_keys",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return Metrics::GetStats().ingest_json_keys;
    }));

static vmsdk::info_field::Integer backfill_json_keys(
    "global_ingestion", "backfill_json_keys",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return Metrics::GetStats().backfill_json_keys;
    }));

static vmsdk::info_field::Integer ingest_json_blocked(
    "global_ingestion", "ingest_json_blocked",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return vmsdk::BlockedClientTracker::GetInstance().GetClientCount(
          vmsdk::BlockedClientCategory::kJson);
    }));

static vmsdk::info_field::Integer ingest_field_vector(
    "global_ingestion", "ingest_field_vector",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return Metrics::GetStats().ingest_field_vector;
    }));

static vmsdk::info_field::Integer ingest_field_numeric(
    "global_ingestion", "ingest_field_numeric",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return Metrics::GetStats().ingest_field_numeric;
    }));

static vmsdk::info_field::Integer ingest_field_tag(
    "global_ingestion", "ingest_field_tag",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return Metrics::GetStats().ingest_field_tag;
    }));

static vmsdk::info_field::Integer ingest_last_batch_size(
    "global_ingestion", "ingest_last_batch_size",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return Metrics::GetStats().ingest_last_batch_size;
    }));

static vmsdk::info_field::Integer ingest_total_batches(
    "global_ingestion", "ingest_total_batches",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return Metrics::GetStats().ingest_total_batches;
    }));

static vmsdk::info_field::Integer ingest_total_failures(
    "global_ingestion", "ingest_total_failures",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return Metrics::GetStats().ingest_total_failures;
    }));

static vmsdk::info_field::Integer time_slice_read_periods(
    "time_slice_mutex", "time_slice_read_periods",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return vmsdk::GetGlobalTimeSlicedMRMWStats().read_periods;
    }));

static vmsdk::info_field::Integer time_slice_read_time(
    "time_slice_mutex", "time_slice_read_time",
    vmsdk::info_field::IntegerBuilder()
        .Dev()
        .Units(vmsdk::info_field::Units::kMicroSeconds)
        .Computed([]() -> long long {
          return vmsdk::GetGlobalTimeSlicedMRMWStats().read_time_microseconds;
        }));

static vmsdk::info_field::Integer time_slice_write_periods(
    "time_slice_mutex", "time_slice_write_periods",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return vmsdk::GetGlobalTimeSlicedMRMWStats().write_periods;
    }));

static vmsdk::info_field::Integer time_slice_write_time(
    "time_slice_mutex", "time_slice_write_time",
    vmsdk::info_field::IntegerBuilder()
        .Dev()
        .Units(vmsdk::info_field::Units::kMicroSeconds)
        .Computed([]() -> long long {
          return vmsdk::GetGlobalTimeSlicedMRMWStats().write_time_microseconds;
        }));

static vmsdk::info_field::Integer time_slice_queries(
    "time_slice_mutex", "time_slice_queries",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return Metrics::GetStats().time_slice_queries;
    }));

static vmsdk::info_field::Integer time_slice_upserts(
    "time_slice_mutex", "time_slice_upserts",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return Metrics::GetStats().time_slice_upserts;
    }));

static vmsdk::info_field::Integer time_slice_deletes(
    "time_slice_mutex", "time_slice_deletes",
    vmsdk::info_field::IntegerBuilder().Dev().Computed([]() -> long long {
      return Metrics::GetStats().time_slice_deletes;
    }));

static vmsdk::info_field::Integer query_queue_size(
    "thread-pool", "query_queue_size",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return ValkeySearch::Instance().GetReaderThreadPool()->QueueSize();
    }));

static vmsdk::info_field::Integer writer_queue_size(
    "thread-pool", "writer_queue_size",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return ValkeySearch::Instance().GetWriterThreadPool()->QueueSize();
    }));

static vmsdk::info_field::Integer worker_pool_suspend_cnt(
    "thread-pool", "worker_pool_suspend_cnt",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().worker_thread_pool_suspend_cnt;
    }));

static vmsdk::info_field::Integer writer_resumed_cnt(
    "thread-pool", "writer_resumed_cnt",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().writer_worker_thread_pool_resumed_cnt;
    }));

static vmsdk::info_field::Integer reader_resumed_cnt(
    "thread-pool", "reader_resumed_cnt",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().reader_worker_thread_pool_resumed_cnt;
    }));

static vmsdk::info_field::Integer writer_suspension_expired_cnt(
    "thread-pool", "writer_suspension_expired_cnt",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats()
          .writer_worker_thread_pool_suspension_expired_cnt;
    }));

static vmsdk::info_field::Integer rdb_load_success_cnt(
    "rdb", "rdb_load_success_cnt",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().rdb_load_success_cnt;
    }));

static vmsdk::info_field::Integer rdb_load_failure_cnt(
    "rdb", "rdb_load_failure_cnt",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().rdb_load_failure_cnt;
    }));

static vmsdk::info_field::Integer rdb_save_success_cnt(
    "rdb", "rdb_save_success_cnt",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().rdb_save_success_cnt;
    }));

static vmsdk::info_field::Integer rdb_save_failure_cnt(
    "rdb", "rdb_save_failure_cnt",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().rdb_save_failure_cnt;
    }));

static vmsdk::info_field::Integer successful_requests_count(
    "query", "successful_requests_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().query_successful_requests_cnt;
    }));

static vmsdk::info_field::Integer failure_requests_count(
    "query", "failure_requests_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().query_failed_requests_cnt;
    }));

static vmsdk::info_field::Integer result_record_dropped_count(
    "query", "result_record_dropped_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().query_result_record_dropped_cnt;
    }));

static vmsdk::info_field::Integer hybrid_requests_count(
    "query", "hybrid_requests_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().query_hybrid_requests_cnt;
    }));

static vmsdk::info_field::Integer inline_filtering_requests_count(
    "query", "inline_filtering_requests_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().query_inline_filtering_requests_cnt;
    }));

static vmsdk::info_field::Integer query_prefiltering_requests_cnt(
    "query", "query_prefiltering_requests_cnt",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().query_prefiltering_requests_cnt;
    }));

static vmsdk::info_field::Integer hnsw_add_exceptions_count(
    "hnswlib", "hnsw_add_exceptions_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().hnsw_add_exceptions_cnt;
    }));

static vmsdk::info_field::Integer hnsw_remove_exceptions_count(
    "hnswlib", "hnsw_remove_exceptions_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().hnsw_remove_exceptions_cnt;
    }));

static vmsdk::info_field::Integer hnsw_modify_exceptions_count(
    "hnswlib", "hnsw_modify_exceptions_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().hnsw_modify_exceptions_cnt;
    }));

static vmsdk::info_field::Integer hnsw_search_exceptions_count(
    "hnswlib", "hnsw_search_exceptions_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().hnsw_search_exceptions_cnt;
    }));

static vmsdk::info_field::Integer hnsw_create_exceptions_count(
    "hnswlib", "hnsw_create_exceptions_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().hnsw_create_exceptions_cnt;
    }));

static vmsdk::info_field::Integer string_interning_store_size(
    "string_interning", "string_interning_store_size",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return StringInternStore::Instance().UniqueStrings();
    }));

static vmsdk::info_field::Integer vector_externing_entry_count(
    "vector_externing", "vector_externing_entry_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return VectorExternalizer::Instance().GetStats().entry_cnt;
    }));

static vmsdk::info_field::Integer vector_externing_hash_extern_errors(
    "vector_externing", "vector_externing_hash_extern_errors",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return VectorExternalizer::Instance().GetStats().hash_extern_errors;
    }));

static vmsdk::info_field::Integer vector_externing_generated_value_cnt(
    "vector_externing", "vector_externing_generated_value_cnt",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return VectorExternalizer::Instance().GetStats().generated_value_cnt;
    }));

static vmsdk::info_field::Integer vector_externing_num_lru_entries(
    "vector_externing", "vector_externing_num_lru_entries",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return VectorExternalizer::Instance().GetStats().num_lru_entries;
    }));

static vmsdk::info_field::Integer vector_externing_lru_promote_cnt(
    "vector_externing", "vector_externing_lru_promote_cnt",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return VectorExternalizer::Instance().GetStats().lru_promote_cnt;
    }));

static vmsdk::info_field::Integer vector_externing_deferred_entry_cnt(
    "vector_externing", "vector_externing_deferred_entry_cnt",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return VectorExternalizer::Instance().GetStats().deferred_entry_cnt;
    }));

static vmsdk::info_field::Integer coordinator_server_listening_port(
    "coordinator", "coordinator_server_listening_port",
    vmsdk::info_field::IntegerBuilder()
        .App()
        .Computed([]() -> long long {
          return ValkeySearch::Instance().GetCoordinatorServer()->GetPort();
        })
        .VisibleIf([]() -> bool {
          return ValkeySearch::Instance().UsingCoordinator();
        }));

static vmsdk::info_field::Integer
    coordinator_server_get_global_metadata_success_count(
        "coordinator", "coordinator_server_get_global_metadata_success_count",
        vmsdk::info_field::IntegerBuilder()
            .App()
            .Computed([]() -> long long {
              return Metrics::GetStats()
                  .coordinator_server_get_global_metadata_success_cnt;
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator();
            }));

static vmsdk::info_field::Integer
    coordinator_server_get_global_metadata_failure_count(
        "coordinator", "coordinator_server_get_global_metadata_failure_count",
        vmsdk::info_field::IntegerBuilder()
            .App()
            .Computed([]() -> long long {
              return Metrics::GetStats()
                  .coordinator_server_get_global_metadata_failure_cnt;
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator();
            }));

static vmsdk::info_field::Integer
    coordinator_server_search_index_partition_success_count(
        "coordinator",
        "coordinator_server_search_index_partition_success_count",
        vmsdk::info_field::IntegerBuilder()
            .App()
            .Computed([]() -> long long {
              return Metrics::GetStats()
                  .coordinator_server_search_index_partition_success_cnt;
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator();
            }));

static vmsdk::info_field::Integer
    coordinator_server_search_index_partition_failure_count(
        "coordinator",
        "coordinator_server_search_index_partition_failure_count",
        vmsdk::info_field::IntegerBuilder()
            .App()
            .Computed([]() -> long long {
              return Metrics::GetStats()
                  .coordinator_server_search_index_partition_failure_cnt;
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator();
            }));

static vmsdk::info_field::Integer
    coordinator_client_get_global_metadata_success_count(
        "coordinator", "coordinator_client_get_global_metadata_success_count",
        vmsdk::info_field::IntegerBuilder()
            .App()
            .Computed([]() -> long long {
              return Metrics::GetStats()
                  .coordinator_client_get_global_metadata_success_cnt;
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator();
            }));

static vmsdk::info_field::Integer
    coordinator_client_get_global_metadata_failure_count(
        "coordinator", "coordinator_client_get_global_metadata_failure_count",
        vmsdk::info_field::IntegerBuilder()
            .App()
            .Computed([]() -> long long {
              return Metrics::GetStats()
                  .coordinator_client_get_global_metadata_failure_cnt;
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator();
            }));

static vmsdk::info_field::Integer
    coordinator_client_search_index_partition_success_count(
        "coordinator",
        "coordinator_client_search_index_partition_success_count",
        vmsdk::info_field::IntegerBuilder()
            .App()
            .Computed([]() -> long long {
              return Metrics::GetStats()
                  .coordinator_client_search_index_partition_success_cnt;
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator();
            }));

static vmsdk::info_field::Integer
    coordinator_client_search_index_partition_failure_count(
        "coordinator",
        "coordinator_client_search_index_partition_failure_count",
        vmsdk::info_field::IntegerBuilder()
            .App()
            .Computed([]() -> long long {
              return Metrics::GetStats()
                  .coordinator_client_search_index_partition_failure_cnt;
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator();
            }));

static vmsdk::info_field::Integer coordinator_bytes_out(
    "coordinator", "coordinator_bytes_out",
    vmsdk::info_field::IntegerBuilder()
        .App()
        .Computed([]() -> long long {
          return Metrics::GetStats().coordinator_bytes_out;
        })
        .VisibleIf([]() -> bool {
          return ValkeySearch::Instance().UsingCoordinator();
        }));

static vmsdk::info_field::Integer coordinator_bytes_in(
    "coordinator", "coordinator_bytes_in",
    vmsdk::info_field::IntegerBuilder()
        .App()
        .Computed([]() -> long long {
          return Metrics::GetStats().coordinator_bytes_in;
        })
        .VisibleIf([]() -> bool {
          return ValkeySearch::Instance().UsingCoordinator();
        }));

static vmsdk::info_field::Integer coordinator_last_time_since_healthy_metadata(
    "coordinator", "coordinator_last_time_since_healthy_metadata",
    vmsdk::info_field::IntegerBuilder()
        .Dev()
        .Units(vmsdk::info_field::Units::kMilliSeconds)
        .Computed([]() -> int64_t {
          return coordinator::MetadataManager::Instance()
              .GetMilliSecondsSinceLastHealthyMetadata();
        })
        .VisibleIf([]() -> bool {
          return ValkeySearch::Instance().UsingCoordinator();
        }));

static vmsdk::info_field::Integer
    coordinator_metadata_reconciliation_completed_count(
        "coordinator", "coordinator_metadata_reconciliation_completed_count",
        vmsdk::info_field::IntegerBuilder()
            .App()
            .Computed([]() -> int64_t {
              // prevent failure in unit tests
              if (!coordinator::MetadataManager::IsInitialized()) {
                return 0;
              }
              return coordinator::MetadataManager::Instance()
                  .GetMetadataReconciliationCompletedCount();
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator();
            }));

static vmsdk::info_field::String
    coordinator_client_get_global_metadata_success_latency_usec(
        "coordinator",
        "coordinator_client_get_global_metadata_success_latency_usec",
        vmsdk::info_field::StringBuilder()
            .App()
            .ComputedString([]() -> std::string {
              auto &sampler =
                  Metrics::GetStats()
                      .coordinator_client_get_global_metadata_success_latency;
              return sampler.GetStatsString();
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator() &&
                     Metrics::GetStats()
                         .coordinator_client_get_global_metadata_success_latency
                         .HasSamples();
            }));

static vmsdk::info_field::String
    coordinator_client_get_global_metadata_failure_latency_usec(
        "coordinator",
        "coordinator_client_get_global_metadata_failure_latency_usec",
        vmsdk::info_field::StringBuilder()
            .App()
            .ComputedString([]() -> std::string {
              auto &sampler =
                  Metrics::GetStats()
                      .coordinator_client_get_global_metadata_failure_latency;
              return sampler.GetStatsString();
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator() &&
                     Metrics::GetStats()
                         .coordinator_client_get_global_metadata_failure_latency
                         .HasSamples();
            }));

static vmsdk::info_field::String
    coordinator_client_search_index_partition_success_latency_usec(
        "coordinator",
        "coordinator_client_search_index_partition_success_latency_usec",
        vmsdk::info_field::StringBuilder()
            .App()
            .ComputedString([]() -> std::string {
              auto &sampler =
                  Metrics::GetStats()
                      .coordinator_client_search_index_partition_success_latency;
              return sampler.GetStatsString();
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator() &&
                     Metrics::GetStats()
                         .coordinator_client_search_index_partition_success_latency
                         .HasSamples();
            }));

static vmsdk::info_field::String
    coordinator_client_search_index_partition_failure_latency_usec(
        "coordinator",
        "coordinator_client_search_index_partition_failure_latency_usec",
        vmsdk::info_field::StringBuilder()
            .App()
            .ComputedString([]() -> std::string {
              auto &sampler =
                  Metrics::GetStats()
                      .coordinator_client_search_index_partition_failure_latency;
              return sampler.GetStatsString();
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator() &&
                     Metrics::GetStats()
                         .coordinator_client_search_index_partition_failure_latency
                         .HasSamples();
            }));

static vmsdk::info_field::String
    coordinator_server_get_global_metadata_success_latency_usec(
        "coordinator",
        "coordinator_server_get_global_metadata_success_latency_usec",
        vmsdk::info_field::StringBuilder()
            .App()
            .ComputedString([]() -> std::string {
              auto &sampler =
                  Metrics::GetStats()
                      .coordinator_server_get_global_metadata_success_latency;
              return sampler.GetStatsString();
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator() &&
                     Metrics::GetStats()
                         .coordinator_server_get_global_metadata_success_latency
                         .HasSamples();
            }));

static vmsdk::info_field::String
    coordinator_server_get_global_metadata_failure_latency_usec(
        "coordinator",
        "coordinator_server_get_global_metadata_failure_latency_usec",
        vmsdk::info_field::StringBuilder()
            .App()
            .ComputedString([]() -> std::string {
              auto &sampler =
                  Metrics::GetStats()
                      .coordinator_server_get_global_metadata_failure_latency;
              return sampler.GetStatsString();
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator() &&
                     Metrics::GetStats()
                         .coordinator_server_get_global_metadata_failure_latency
                         .HasSamples();
            }));

static vmsdk::info_field::String
    coordinator_server_search_index_partition_success_latency_usec(
        "coordinator",
        "coordinator_server_search_index_partition_success_latency_usec",
        vmsdk::info_field::StringBuilder()
            .App()
            .ComputedString([]() -> std::string {
              auto &sampler =
                  Metrics::GetStats()
                      .coordinator_server_search_index_partition_success_latency;
              return sampler.GetStatsString();
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator() &&
                     Metrics::GetStats()
                         .coordinator_server_search_index_partition_success_latency
                         .HasSamples();
            }));

static vmsdk::info_field::String
    coordinator_server_search_index_partition_failure_latency_usec(
        "coordinator",
        "coordinator_server_search_index_partition_failure_latency_usec",
        vmsdk::info_field::StringBuilder()
            .App()
            .ComputedString([]() -> std::string {
              auto &sampler =
                  Metrics::GetStats()
                      .coordinator_server_search_index_partition_failure_latency;
              return sampler.GetStatsString();
            })
            .VisibleIf([]() -> bool {
              return ValkeySearch::Instance().UsingCoordinator() &&
                     Metrics::GetStats()
                         .coordinator_server_search_index_partition_failure_latency
                         .HasSamples();
            }));

static vmsdk::info_field::String hnsw_vector_index_search_latency_usec(
    "latency", "hnsw_vector_index_search_latency_usec",
    vmsdk::info_field::StringBuilder()
        .App()
        .ComputedString([]() -> std::string {
          auto &sampler = Metrics::GetStats().hnsw_vector_index_search_latency;
          return sampler.GetStatsString();
        })
        .VisibleIf([]() -> bool {
          return Metrics::GetStats()
              .hnsw_vector_index_search_latency.HasSamples();
        }));

static vmsdk::info_field::String flat_vector_index_search_latency_usec(
    "latency", "flat_vector_index_search_latency_usec",
    vmsdk::info_field::StringBuilder()
        .App()
        .ComputedString([]() -> std::string {
          auto &sampler = Metrics::GetStats().flat_vector_index_search_latency;
          return sampler.GetStatsString();
        })
        .VisibleIf([]() -> bool {
          return Metrics::GetStats()
              .flat_vector_index_search_latency.HasSamples();
        }));

static vmsdk::info_field::Integer info_fanout_retry_count(
    "fanout", "info_fanout_retry_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().info_fanout_retry_cnt;
    }));

static vmsdk::info_field::Integer info_fanout_fail_count(
    "fanout", "info_fanout_fail_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().info_fanout_fail_cnt;
    }));

static vmsdk::info_field::Integer pause_handle_cluster_message_round_cnt(
    "fanout", "pause_handle_cluster_message_round_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().pause_handle_cluster_message_round_cnt;
    }));

static vmsdk::info_field::Integer fulltext_query_blocked_count(
    "query", "fulltext_query_blocked_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().fulltext_query_blocked_cnt;
    }));

static vmsdk::info_field::Integer fulltext_query_retry_count(
    "query", "fulltext_query_retry_count",
    vmsdk::info_field::IntegerBuilder().App().Computed([]() -> long long {
      return Metrics::GetStats().fulltext_query_retry_cnt;
    }));

#ifdef DEBUG_INFO
// Helper function to create subscription info fields with maximum deduplication
template <typename StatsSelector>
static std::array<vmsdk::info_field::Integer, 3> CreateSubscriptionInfoFields(
    const std::string &operation_name, StatsSelector stats_selector) {
  auto create_field = [&](const std::string &suffix, auto count_extractor) {
    return vmsdk::info_field::Integer(
        "subscription", operation_name + "_" + suffix,
        vmsdk::info_field::IntegerBuilder().App().Computed(
            [stats_selector, count_extractor]() -> long long {
              auto result =
                  SchemaManager::Instance().AccumulateIndexSchemaResults(
                      stats_selector);
              return count_extractor(result);
            }));
  };

  return {create_field("successful_count",
                       [](const auto &result) { return result.success_cnt; }),
          create_field("failure_count",
                       [](const auto &result) { return result.failure_cnt; }),
          create_field("skipped_count",
                       [](const auto &result) { return result.skipped_cnt; })};
}

// Create all subscription info fields using the helper function
static auto add_subscription_fields = CreateSubscriptionInfoFields(
    "add_subscription",
    [](const IndexSchema::Stats &stats)
        -> const IndexSchema::Stats::ResultCnt<std::atomic<uint64_t>> & {
      return stats.subscription_add;
    });

static auto modify_subscription_fields = CreateSubscriptionInfoFields(
    "modify_subscription",
    [](const IndexSchema::Stats &stats)
        -> const IndexSchema::Stats::ResultCnt<std::atomic<uint64_t>> & {
      return stats.subscription_modify;
    });

static auto remove_subscription_fields = CreateSubscriptionInfoFields(
    "remove_subscription",
    [](const IndexSchema::Stats &stats)
        -> const IndexSchema::Stats::ResultCnt<std::atomic<uint64_t>> & {
      return stats.subscription_remove;
    });

static vmsdk::info_field::Integer &add_subscription_successful_count =
    add_subscription_fields[0];
static vmsdk::info_field::Integer &add_subscription_failure_count =
    add_subscription_fields[1];
static vmsdk::info_field::Integer &add_subscription_skipped_count =
    add_subscription_fields[2];

static vmsdk::info_field::Integer &modify_subscription_successful_count =
    modify_subscription_fields[0];
static vmsdk::info_field::Integer &modify_subscription_failure_count =
    modify_subscription_fields[1];
static vmsdk::info_field::Integer &modify_subscription_skipped_count =
    modify_subscription_fields[2];

static vmsdk::info_field::Integer &remove_subscription_successful_count =
    remove_subscription_fields[0];
static vmsdk::info_field::Integer &remove_subscription_failure_count =
    remove_subscription_fields[1];
static vmsdk::info_field::Integer &remove_subscription_skipped_count =
    remove_subscription_fields[2];

#endif

static vmsdk::info_field::Integer string_interning_memory_bytes(
    "string_interning", "string_interning_memory_bytes",
    vmsdk::info_field::IntegerBuilder()
        .App()
        .Computed(StringInternStore::GetMemoryUsage)
        .CrashSafe());

static vmsdk::info_field::Integer string_interning_memory_human(
    "string_interning", "string_interning_memory_human",
    vmsdk::info_field::IntegerBuilder()
        .SIBytes()
        .App()
        .Computed(StringInternStore::GetMemoryUsage)
        .CrashSafe());

void ValkeySearch::Info(ValkeyModuleInfoCtx *ctx, bool for_crash_report) const {
  vmsdk::info_field::DoSections(ctx, for_crash_report);
}

// Beside the thread which initiates the fork, no other threads are present
// in the forked child process. This could lead to full sync corruption as the
// fork system-call may occur in the middle of mutating the index. In addition,
// vector insertion may lead to high amount of dirty pages which increases the
// chances to OOM during full sync. Addressing these by temporary suspending the
// writer thread pool during full sync. The writer thread pool resumes once the
// child process dies or suspension time exceeds 60 seconds. Suspending the
// workers guarantees that no thread is mutating the index while the fork is
// happening. For more details see:
// https://pubs.opengroup.org/onlinepubs/009695399/functions/pthread_atfork.html
void ValkeySearch::AtForkPrepare() {
  // Sanity: fork can occur (by example: calling to "popen") before the thread
  // pool is initialized
  if (writer_thread_pool_ == nullptr || reader_thread_pool_ == nullptr) {
    return;
  }
  Metrics::GetStats().worker_thread_pool_suspend_cnt++;
  auto status = writer_thread_pool_->SuspendWorkers();
  VMSDK_LOG(WARNING, nullptr) << "At prepare fork callback, suspend writer "
                                 "worker thread pool returned message: "
                              << status.message();
  status = reader_thread_pool_->SuspendWorkers();
  VMSDK_LOG(WARNING, nullptr) << "At prepare fork callback, suspend reader "
                                 "worker thread pool returned message: "
                              << status.message();
  status = coordinator::GRPCSuspender::Instance().Suspend();
  VMSDK_LOG(WARNING, nullptr) << "At prepare fork callback, suspend gRPC "
                                 "returned message: "
                              << status.message();
}

void ValkeySearch::AfterForkParent() {
  // Sanity: fork can occur (by example: calling to "popen") before the thread
  // pool is initialized
  if (reader_thread_pool_ == nullptr) {
    return;
  }
  auto status = reader_thread_pool_->ResumeWorkers();
  Metrics::GetStats().reader_worker_thread_pool_resumed_cnt++;
  VMSDK_LOG(WARNING, nullptr) << "After fork parent callback, resume reader "
                                 "worker thread pool returned message: "
                              << status.message();
  writer_thread_pool_suspend_watch_ = vmsdk::StopWatch();
  status = coordinator::GRPCSuspender::Instance().Resume();
  VMSDK_LOG(WARNING, nullptr) << "After fork parent callback, resume gRPC "
                                 "returned message: "
                              << status.message();
}

void ValkeySearch::OnServerCronCallback(ValkeyModuleCtx *ctx,
                                        [[maybe_unused]] ValkeyModuleEvent eid,
                                        [[maybe_unused]] uint64_t subevent,
                                        [[maybe_unused]] void *data) {
  // Clean-up after threads that exited without being "joined"
  if (writer_thread_pool_) {
    writer_thread_pool_->JoinTerminatedWorkers();
  }
  if (reader_thread_pool_) {
    reader_thread_pool_->JoinTerminatedWorkers();
  }
  // Resume worker thread pool if suspension time exceeds the max allowed
  // duration
  if (writer_thread_pool_suspend_watch_.has_value() &&
      options::GetMaxWorkerSuspensionSecs().GetValue() > 0 &&
      writer_thread_pool_suspend_watch_.value().Duration() >
          absl::Seconds(options::GetMaxWorkerSuspensionSecs().GetValue())) {
    ResumeWriterThreadPool(ctx, /*is_expired=*/true);
  }
  // refresh cluster map in cluster mode
  if (IsCluster() && UsingCoordinator()) {
    GetOrRefreshClusterMap(ctx);
  }
}

void ValkeySearch::OnForkChildCallback(ValkeyModuleCtx *ctx,
                                       [[maybe_unused]] ValkeyModuleEvent eid,
                                       uint64_t subevent,
                                       [[maybe_unused]] void *data) {
  // if max-worker-suspension-secs config > 0, we resume the workers either when
  // fork dies or when time expires (the second condition is checked on cron
  // callback).
  if (options::GetMaxWorkerSuspensionSecs().GetValue() > 0) {
    if (subevent & VALKEYMODULE_SUBEVENT_FORK_CHILD_DIED) {
      ResumeWriterThreadPool(ctx, /*is_expired=*/false);
    }
  } else {
    // max-worker-suspension-secs <= 0 - we resume the workers on a 'fork born'
    // event. We don't check if it's a 'fork born' event - in case the config
    // was modified in the middle of the fork, we want to resume the workers
    // also after 'fork died' event in case it wasn't already.
    ResumeWriterThreadPool(ctx, /*is_expired=*/false);
  }
}

absl::StatusOr<std::string> GetConfigGetReply(ValkeyModuleCtx *ctx,
                                              const char *config) {
  auto reply = vmsdk::UniquePtrValkeyCallReply(
      ValkeyModule_Call(ctx, "CONFIG", "cc", "GET", config));
  if (reply == nullptr) {
    return absl::InternalError(
        absl::StrFormat("Failed to get config: %s", config));
  }
  ValkeyModuleCallReply *config_reply =
      ValkeyModule_CallReplyArrayElement(reply.get(), 1);

  size_t reply_len;
  const char *reply_str =
      ValkeyModule_CallReplyStringPtr(config_reply, &reply_len);
  return std::string(reply_str, reply_len);
}

absl::StatusOr<int> GetValkeyLocalPort(ValkeyModuleCtx *ctx) {
  int port = -1;
  VMSDK_ASSIGN_OR_RETURN(auto tls_port_str, GetConfigGetReply(ctx, "tls-port"));
  if (!absl::SimpleAtoi(tls_port_str, &port)) {
    return absl::InternalError(
        absl::StrFormat("Failed to parse port: %s", tls_port_str));
  }
  if (port == 0) {
    VMSDK_ASSIGN_OR_RETURN(auto port_str, GetConfigGetReply(ctx, "port"));
    if (!absl::SimpleAtoi(port_str, &port)) {
      return absl::InternalError(
          absl::StrFormat("Failed to parse port: %s", port_str));
    }
  }

  if (port < 0) {
    return absl::InternalError("Valkey port is negative");
  }
  if (coordinator::GetCoordinatorPort(port) > 65535) {
    return absl::FailedPreconditionError(
        "Coordinator port is too large, Valkey port must be less than or equal "
        "to 45241 (max port of 65535 minus coordinator offset of 20294).");
  }
  return port;
}

absl::Status ValkeySearch::Startup(ValkeyModuleCtx *ctx) {
  reader_thread_pool_ = std::make_unique<vmsdk::ThreadPool>(
      "read-worker-", options::GetReaderThreadCount().GetValue(),
      options::GetThreadPoolWaitTimeSamples().GetValue());
  reader_thread_pool_->StartWorkers();
  writer_thread_pool_ = std::make_unique<vmsdk::ThreadPool>(
      "write-worker-", options::GetWriterThreadCount().GetValue(),
      options::GetThreadPoolWaitTimeSamples().GetValue());
  writer_thread_pool_->StartWorkers();

  VMSDK_LOG(NOTICE, ctx) << "use_coordinator: "
                         << options::GetUseCoordinator().GetValue()
                         << ", IsCluster: " << IsCluster();

  VMSDK_LOG(NOTICE, ctx) << "Reader workers count: "
                         << reader_thread_pool_->Size();
  VMSDK_LOG(NOTICE, ctx) << "Writer workers count: "
                         << writer_thread_pool_->Size();

  if (options::GetUseCoordinator().GetValue() && IsCluster()) {
    client_pool_ = std::make_unique<coordinator::ClientPool>(
        vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(ctx));
    coordinator::MetadataManager::InitInstance(
        std::make_unique<coordinator::MetadataManager>(ctx, *client_pool_));
    coordinator::MetadataManager::Instance().RegisterForClusterMessages(ctx);
  }
  SchemaManager::InitInstance(std::make_unique<SchemaManager>(
      ctx, server_events::SubscribeToServerEvents, writer_thread_pool_.get(),
      options::GetUseCoordinator().GetValue() && IsCluster()));
  if (options::GetUseCoordinator().GetValue()) {
    VMSDK_ASSIGN_OR_RETURN(auto valkey_port, GetValkeyLocalPort(ctx));
    auto coordinator_port = coordinator::GetCoordinatorPort(valkey_port);
    coordinator_ = coordinator::ServerImpl::Create(
        ctx, reader_thread_pool_.get(), coordinator_port);
    if (coordinator_ == nullptr) {
      return absl::InternalError("Failed to create coordinator server");
    }
  }
  return absl::OkStatus();
}

void ValkeySearch::ResumeWriterThreadPool(ValkeyModuleCtx *ctx,
                                          bool is_expired) {
  auto status = writer_thread_pool_->ResumeWorkers();
  auto msg =
      is_expired
          ? absl::StrFormat(
                "Worker thread pool suspension took more than %lu seconds",
                options::GetMaxWorkerSuspensionSecs().GetValue())
          : "Fork child died notification received";
  if (is_expired) {
    Metrics::GetStats().writer_worker_thread_pool_suspension_expired_cnt++;
  }
  Metrics::GetStats().writer_worker_thread_pool_resumed_cnt++;
  VMSDK_LOG(WARNING, ctx) << msg
                          << ". Resuming writer "
                             "worker thread pool returned message: "
                          << status.message() << " Suspend duration: "
                          << FormatDuration(writer_thread_pool_suspend_watch_
                                                .value_or(vmsdk::StopWatch())
                                                .Duration());
  writer_thread_pool_suspend_watch_ = std::nullopt;
}

absl::Status ValkeySearch::OnLoad(ValkeyModuleCtx *ctx,
                                  ValkeyModuleString **argv, int argc) {
  ctx_ = ValkeyModule_GetDetachedThreadSafeContext(ctx);

  // Register a single module type for Aux load/save callbacks.
  VMSDK_RETURN_IF_ERROR(RegisterModuleType(ctx));

  // Register all global configuration variables
  VMSDK_RETURN_IF_ERROR(ModuleConfigManager::Instance().Init(ctx));

  // Load configurations to initialize registered configs
  if (ValkeyModule_LoadConfigs(ctx) != VALKEYMODULE_OK) {
    return absl::InternalError("Failed to load configurations");
  }

  // Apply command line arguments and initialize the module
  VMSDK_RETURN_IF_ERROR(LoadAndParseArgv(ctx, argv, argc));
  VMSDK_RETURN_IF_ERROR(Startup(ctx));

  ValkeyModule_SetModuleOptions(
      ctx, VALKEYMODULE_OPTIONS_HANDLE_IO_ERRORS |
               VALKEYMODULE_OPTIONS_HANDLE_REPL_ASYNC_LOAD |
               VALKEYMODULE_OPTION_NO_IMPLICIT_SIGNAL_MODIFIED);
  VMSDK_LOG(NOTICE, ctx) << "Json "
                         << (IsJsonModuleSupported(ctx) ? "" : "not ")
                         << "supported!";
  VectorExternalizer::Instance().Init(ctx_);
  ValkeyModule_Assert(vmsdk::info_field::Validate(ctx));
  VMSDK_LOG(DEBUG, ctx) << "Search module completed initialization!";
  return absl::OkStatus();
}

absl::Status ValkeySearch::LoadAndParseArgv(ValkeyModuleCtx *ctx,
                                            ValkeyModuleString **argv,
                                            int argc) {
  VMSDK_RETURN_IF_ERROR(
      vmsdk::config::ModuleConfigManager::Instance().ParseAndLoadArgv(ctx, argv,
                                                                      argc));
  // Sanity check
  if ((options::GetReaderThreadCount().GetValue() == 0 &&
       options::GetWriterThreadCount().GetValue() != 0) ||
      (options::GetWriterThreadCount().GetValue() == 0 &&
       options::GetReaderThreadCount().GetValue() != 0)) {
    return absl::InvalidArgumentError(
        "Maintaining query integrity is only supported when both the reader "
        "and writer thread pools are either enabled or disabled "
        "simultaneously");
  }
  return absl::OkStatus();
}

bool ValkeySearch::IsChildProcess() {
  const auto flags = ValkeyModule_GetContextFlags(nullptr);
  return flags & VALKEYMODULE_CTX_FLAGS_IS_CHILD;
}

void ValkeySearch::OnUnload(ValkeyModuleCtx *ctx) {
  ValkeyModule_FreeThreadSafeContext(ctx_);
  reader_thread_pool_ = nullptr;
}

std::shared_ptr<vmsdk::cluster_map::ClusterMap>
ValkeySearch::GetOrRefreshClusterMap(ValkeyModuleCtx *ctx) {
  auto current_map = std::atomic_load(&cluster_map_);
  // Check if we need to refresh
  bool needs_refresh =
      !current_map || !current_map->IsConsistent() ||
      std::chrono::steady_clock::now() > current_map->GetExpirationTime();
  if (needs_refresh) {
    VMSDK_LOG_EVERY_N_SEC(DEBUG, nullptr, 1) << "Creating a new cluster map";
    auto new_map = vmsdk::cluster_map::ClusterMap::CreateNewClusterMap(ctx);
    std::atomic_store(&cluster_map_, new_map);
    return new_map;
  }
  return current_map;
}

}  // namespace valkey_search
