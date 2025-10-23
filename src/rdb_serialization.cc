/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/rdb_serialization.h"

#include <cstddef>
#include <cstdlib>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "src/metrics.h"
#include "src/rdb_section.pb.h"
#include "src/valkey_search.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::flat_hash_map<data_model::RDBSectionType, RDBSectionCallbacks>
    kRegisteredRDBSectionCallbacks = {};

absl::StatusOr<std::unique_ptr<data_model::SupplementalContentChunk>>
SupplementalContentChunkIter::Next() {
  if (curr_chunk_.ok()) {
    std::unique_ptr<data_model::SupplementalContentChunk> result =
        std::move(*curr_chunk_);
    ReadNextChunk();
    return result;
  }
  return curr_chunk_.status();
}

void SupplementalContentChunkIter::ReadNextChunk() {
  if (done_) {
    curr_chunk_ = absl::NotFoundError("No more elements remaining");
    return;
  }
  auto serialized_chunk = rdb_->LoadString();
  if (!serialized_chunk.ok()) {
    curr_chunk_ = absl::InternalError(
        "IO error while reading serialized SupplementalContentChunk from "
        "RDB");
    return;
  }
  curr_chunk_ = std::make_unique<data_model::SupplementalContentChunk>();
  if (!(*curr_chunk_)
           ->ParseFromString(vmsdk::ToStringView(serialized_chunk->get()))) {
    curr_chunk_ = absl::InternalError(
        "Failed to deserialize "
        "SupplementalContentChunk read from RDB");
    return;
  }
  done_ = !(*curr_chunk_)->has_binary_content();
}

absl::StatusOr<std::unique_ptr<data_model::SupplementalContentHeader>>
SupplementalContentIter::Next() {
  if (remaining_ == 0) {
    return absl::NotFoundError("No more supplemental content chunks");
  }
  VMSDK_ASSIGN_OR_RETURN(auto serialized_supplemental_content_header,
                         rdb_->LoadString(),
                         _ << "IO error while reading serialized "
                              "SupplementalContentHeader from RDB");
  auto result = std::make_unique<data_model::SupplementalContentHeader>();
  if (!result->ParseFromString(
          vmsdk::ToStringView(serialized_supplemental_content_header.get()))) {
    return absl::InternalError(
        "Failed to deserialize SupplementalContentHeader read from RDB");
  }
  remaining_--;
  return result;
}

absl::StatusOr<std::unique_ptr<data_model::RDBSection>> RDBSectionIter::Next() {
  VMSDK_ASSIGN_OR_RETURN(
      auto serialized_rdb_section, rdb_->LoadString(),
      _ << "IO error while reading serialized RDBSection from RDB");
  auto result = std::make_unique<data_model::RDBSection>();
  if (!result->ParseFromString(
          std::string(vmsdk::ToStringView(serialized_rdb_section.get())))) {
    return absl::InternalError(
        "Failed to deserialize RDBSection read from RDB");
  }
  remaining_--;
  curr_supplemental_count_ = result->supplemental_count();
  return result;
}

absl::StatusOr<std::unique_ptr<std::string>> RDBChunkInputStream::LoadChunk() {
  if (!iter_.HasNext()) {
    return absl::NotFoundError("No more elements remaining");
  }
  VMSDK_ASSIGN_OR_RETURN(auto result, iter_.Next());
  return std::unique_ptr<std::string>(result->release_binary_content());
}

absl::Status RDBChunkOutputStream::SaveChunk(const char *data, size_t len) {
  if (closed_) {
    return absl::InternalError("RDBChunkOutputStream is closed");
  }
  data_model::SupplementalContentChunk chunk;
  chunk.set_binary_content(std::string(data, len));
  std::string serialized_string;
  if (!chunk.SerializeToString(&serialized_string)) {
    return absl::InternalError("Failed to serialize chunk to string");
  }
  VMSDK_RETURN_IF_ERROR(rdb_->SaveStringBuffer(serialized_string));
  return absl::OkStatus();
}

absl::Status RDBChunkOutputStream::Close() {
  if (closed_) {
    return absl::InternalError("RDBChunkOutputStream is already closed");
  }
  /* Empty string represents an EOF */
  std::string serialized_string = "";
  VMSDK_RETURN_IF_ERROR(rdb_->SaveStringBuffer(serialized_string));
  closed_ = true;
  return absl::OkStatus();
}

void RegisterRDBCallback(data_model::RDBSectionType type,
                         RDBSectionCallbacks callbacks) {
  vmsdk::VerifyMainThread();
  kRegisteredRDBSectionCallbacks[type] = std::move(callbacks);
}
void ClearRDBCallbacks() { kRegisteredRDBSectionCallbacks.clear(); }

absl::Status PerformRDBLoad(ValkeyModuleCtx *ctx, SafeRDB *rdb, int encver) {
  // Parse the header
  if (encver != kCurrentEncVer) {
    return absl::InternalError(absl::StrFormat(
        "Unable to load RDB with encoding version %d, we only support %d",
        encver, kCurrentEncVer));
  }
  VMSDK_ASSIGN_OR_RETURN(
      auto min_semantic_version_int, rdb->LoadUnsigned(),
      _ << "IO error reading semantic version from RDB. Failing RDB load.");
  auto min_semantic_version = vmsdk::SemanticVersion(min_semantic_version_int);
  if (min_semantic_version > kCurrentSemanticVersion) {
    return absl::InternalError(absl::StrCat(
        "ValkeySearch RDB contents require minimum version ",
        min_semantic_version.ToString(), " and we are on ",
        kCurrentSemanticVersion.ToString(),
        ". If you are downgrading, ensure all feature usage on the new "
        "version of ValkeySearch is supported by this version and retry."));
  }

  VMSDK_ASSIGN_OR_RETURN(
      auto rdb_section_count, rdb->LoadUnsigned(),
      _ << "IO error reading RDB section count from RDB. Failing RDB load.");

  // Begin RDBSection iteration
  RDBSectionIter it(rdb, rdb_section_count);
  while (it.HasNext()) {
    VMSDK_ASSIGN_OR_RETURN(auto section, it.Next());

    if (kRegisteredRDBSectionCallbacks.contains(section->type())) {
      auto &load_callback =
          kRegisteredRDBSectionCallbacks.at(section->type()).load;
      VMSDK_RETURN_IF_ERROR(load_callback(ctx, std::move(section),
                                          it.IterateSupplementalContent()));
    } else {
      VMSDK_LOG(WARNING, ctx)
          << "Ignoring unknown RDB section with type "
          << data_model::RDBSectionType_Name(section->type());
      // Need to consume all supplemental data
      auto supp_it = it.IterateSupplementalContent();
      while (supp_it.HasNext()) {
        VMSDK_ASSIGN_OR_RETURN(auto _, supp_it.Next());
        auto chunk_it = supp_it.IterateChunks();
        while (chunk_it.HasNext()) {
          VMSDK_ASSIGN_OR_RETURN(auto _, chunk_it.Next());
        }
      }
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<vmsdk::UniqueValkeyDetachedThreadSafeContext>
CreateRDBDetachedContext(ValkeyModuleIO *rdb) {
  /* Wrap the RDB context in a detached context to ensure we have a client. */
  auto ctx = ValkeyModule_GetContextFromIO(rdb);
  return vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(
      ValkeyModule_GetDetachedThreadSafeContext(ctx));
}

int AuxLoadCallback(ValkeyModuleIO *rdb, int encver, int when) {
  auto ctx = CreateRDBDetachedContext(rdb);
  if (!ctx.ok()) {
    VMSDK_LOG(WARNING, nullptr)
        << "Could not create RDB load context: " << ctx.status().message();
    return VALKEYMODULE_ERR;
  }
  SafeRDB safe_rdb(rdb);
  auto result = PerformRDBLoad(ctx.value().get(), &safe_rdb, encver);
  if (result.ok()) {
    Metrics::GetStats().rdb_load_success_cnt++;

    return VALKEYMODULE_OK;
  }
  Metrics::GetStats().rdb_load_failure_cnt++;
  VMSDK_LOG_EVERY_N_SEC(WARNING, ctx.value().get(), 0.1)
      << "Failed to load ValkeySearch aux section from RDB: "
      << result.message();
  return VALKEYMODULE_ERR;
}

absl::Status PerformRDBSave(ValkeyModuleCtx *ctx, SafeRDB *rdb, int when) {
  // Aggregate header information from save callbacks first
  int rdb_section_count = 0;
  int min_semantic_version = 0;  // 0.0.0 by default
  absl::flat_hash_map<data_model::RDBSectionType, int> section_counts;
  for (auto &registeredRDBSectionCallback : kRegisteredRDBSectionCallbacks) {
    data_model::RDBSectionType rdb_section_type =
        registeredRDBSectionCallback.first;
    section_counts[rdb_section_type] =
        registeredRDBSectionCallback.second.section_count(ctx, when);

    if (section_counts[rdb_section_type] > 0) {
      min_semantic_version =
          std::max(min_semantic_version,
                   registeredRDBSectionCallback.second.minimum_semantic_version(
                       ctx, when));
    }
    rdb_section_count += section_counts[rdb_section_type];
  }

  // Do nothing to satisfy AuxSave2 if there are no RDBSections.
  if (rdb_section_count == 0) {
    return absl::OkStatus();
  }

  // Save the header
  VMSDK_RETURN_IF_ERROR(rdb->SaveUnsigned(min_semantic_version));
  VMSDK_RETURN_IF_ERROR(rdb->SaveUnsigned(rdb_section_count));

  // Now do the save of the contents
  for (auto &section_count : section_counts) {
    if (section_count.second == 0) {
      continue;
    }
    VMSDK_RETURN_IF_ERROR(
        kRegisteredRDBSectionCallbacks[section_count.first].save(ctx, rdb,
                                                                 when));
  }

  return absl::OkStatus();
}

void AuxSaveCallback(ValkeyModuleIO *rdb, int when) {
  SafeRDB safe_rdb(rdb);
  auto ctx = ValkeySearch::Instance().GetBackgroundCtx();
  auto result = PerformRDBSave(ctx, &safe_rdb, when);
  if (result.ok()) {
    Metrics::GetStats().rdb_save_success_cnt++;
    return;
  }
  Metrics::GetStats().rdb_save_failure_cnt++;
  VMSDK_LOG_EVERY_N_SEC(WARNING, ctx, 0.1)
      << "Failed to save ValkeySearch aux section to RDB: " << result.message();
}

// This module type is used purely to get aux callbacks.
absl::Status RegisterModuleType(ValkeyModuleCtx *ctx) {
  static ValkeyModuleTypeMethods tm = {
      .version = VALKEYMODULE_TYPE_METHOD_VERSION,
      .rdb_load = [](ValkeyModuleIO *io, int encver) -> void * {
        DCHECK(false) << "Attempt to load ValkeySearch module type from RDB";
        return nullptr;
      },
      .rdb_save =
          [](ValkeyModuleIO *io, void *value) {
            DCHECK(false) << "Attempt to save ValkeySearch module type to RDB";
          },
      .aof_rewrite =
          [](ValkeyModuleIO *aof, ValkeyModuleString *key, void *value) {
            DCHECK(false)
                << "Attempt to rewrite ValkeySearch module type to AOF";
          },
      .free =
          [](void *value) {
            DCHECK(false) << "Attempt to free ValkeySearch module type object";
          },
      .aux_load = AuxLoadCallback,
      .aux_save_triggers = VALKEYMODULE_AUX_AFTER_RDB,
      .aux_save2 = AuxSaveCallback,
  };

  static ValkeyModuleType *kValkeySearchModuleType = nullptr;
  kValkeySearchModuleType = ValkeyModule_CreateDataType(
      ctx, kValkeySearchModuleTypeName.data(), kCurrentEncVer, &tm);
  if (!kValkeySearchModuleType) {
    return absl::InternalError(absl::StrCat(
        "failed to create ", kValkeySearchModuleTypeName, " type"));
  }
  return absl::OkStatus();
}

}  // namespace valkey_search
