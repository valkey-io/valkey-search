/*
 * Copyright (c) 2025, ValkeySearch contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/rdb_serialization.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

#include "src/indexes/text_index.h"

namespace valkey_search {
namespace indexes {

// Provide the missing constructor that text_index.h declares
Text::Text(const data_model::TextIndex& text_index_proto) : IndexBase(IndexerType::kText) {
  // Initialize the member variables declared in text_index.h
  // prefix_ = text::RadixTree();
  // suffix_ = std::nullopt;
  // reverse_ = absl::flat_hash_map<Key, text::RadixTree>();
  // untracked_keys_ = absl::flat_hash_set<Key>();
}

// Implement all pure virtual methods from IndexBase
absl::StatusOr<bool> Text::AddRecord(const InternedStringPtr& key, absl::string_view data) {
  return true;
}

absl::StatusOr<bool> Text::RemoveRecord(const InternedStringPtr& key, DeletionType deletion_type) {
  return true;
}

absl::StatusOr<bool> Text::ModifyRecord(const InternedStringPtr& key, absl::string_view data) {
  return true;
}

int Text::RespondWithInfo(ValkeyModuleCtx* ctx) const {
  ValkeyModule_ReplyWithSimpleString(ctx, "index_type");
  ValkeyModule_ReplyWithSimpleString(ctx, "TEXT");
  return 2;
}

bool Text::IsTracked(const InternedStringPtr& key) const {
  return false;
}

absl::Status Text::SaveIndex(RDBChunkOutputStream chunked_out) const {
  return absl::OkStatus();
}

std::unique_ptr<data_model::Index> Text::ToProto() const {
  auto index = std::make_unique<data_model::Index>();
  auto text_index = std::make_unique<data_model::TextIndex>();
  index->set_allocated_text_index(text_index.release());
  return index;
}

uint64_t Text::GetRecordCount() const {
  return 0;
}

}  // namespace indexes
}  // namespace valkey_search
