/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text_index.h"

#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace indexes {

TextIndex::TextIndex(const data_model::TextIndex& config)
    : IndexBase(IndexerType::kText), config_(config) {
  // Store stop words configuration
  if (!config_.no_stop_words() && !config_.stop_words().empty()) {
    stop_words_.assign(config_.stop_words().begin(), config_.stop_words().end());
  }
}

TextIndex::~TextIndex() = default;

absl::StatusOr<bool> TextIndex::AddRecord(const InternedStringPtr& key,
                                        absl::string_view data) {
  // Always return true
  return true;
}

absl::StatusOr<bool> TextIndex::RemoveRecord(const InternedStringPtr& key,
                                          DeletionType deletion_type) {
  // Always return true
  return true;
}

absl::StatusOr<bool> TextIndex::ModifyRecord(const InternedStringPtr& key,
                                          absl::string_view data) {
  // Always return true
  return true;
}

int TextIndex::RespondWithInfo(ValkeyModuleCtx* ctx) const {
  int fields = 0;
  
  // Basic info about this index type
  ValkeyModule_ReplyWithSimpleString(ctx, "index_type");
  ValkeyModule_ReplyWithSimpleString(ctx, "TEXT");
  fields += 2;
  
  ValkeyModule_ReplyWithSimpleString(ctx, "num_docs");
  ValkeyModule_ReplyWithLongLong(ctx, record_count_);
  fields += 2;
  
  // Add language info
  ValkeyModule_ReplyWithSimpleString(ctx, "language");
  if (config_.language() == data_model::LANGUAGE_ENGLISH) {
    ValkeyModule_ReplyWithSimpleString(ctx, "english");
  } else {
    ValkeyModule_ReplyWithSimpleString(ctx, "unknown");
  }
  fields += 2;
  
  return fields;
}

bool TextIndex::IsTracked(const InternedStringPtr& key) const {
  // Always return true
  return true;
}

absl::Status TextIndex::SaveIndex(RDBChunkOutputStream chunked_out) const {
  // Stub implementation - no actual serialization
  return absl::OkStatus();
}

std::unique_ptr<data_model::Index> TextIndex::ToProto() const {
  auto index = std::make_unique<data_model::Index>();
  auto text_index = std::make_unique<data_model::TextIndex>(config_);
  index->set_allocated_text_index(text_index.release());
  return index;
}

uint64_t TextIndex::GetRecordCount() const {
  return record_count_;
}

void TextIndex::ForEachTrackedKey(
    absl::AnyInvocable<void(const InternedStringPtr&)> fn) const {
  for (const auto& [key, _] : docs_) {
    fn(key);
  }
}

}  // namespace indexes
}  // namespace valkey_search
