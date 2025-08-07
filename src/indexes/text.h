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

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_H_

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include <concepts>
#include <memory>
#include <optional>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/indexes/index_base.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text_index.h"
#include "src/rdb_serialization.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "src/indexes/text/lexer.h"

// Forward declaration
namespace valkey_search::indexes {
enum class DeletionType;
enum class IndexerType;
}

namespace valkey_search {
namespace text {

using Byte = uint8_t;
using Char = uint32_t;

struct TextFieldIndex {
    TextFieldIndex(const data_model::TextIndex& text_index_proto,
                 const data_model::IndexSchema* index_schema_proto = nullptr,
                 std::string field_identifier = "") 
      : text_index_proto_(text_index_proto),
        index_schema_proto_(index_schema_proto),
        field_identifier_(field_identifier),
        // Initialize lexer in the constructor's initialization list
        lexer_(index_schema_proto_ ? 
              Lexer(text_index_proto_, *index_schema_proto_) : 
              Lexer(text_index_proto_, data_model::IndexSchema())) {
    // Initialize text index structures
    text_ = std::make_shared<TextIndex>();
  }

  // Return the field identifier
  const std::string& GetFieldIdentifier() const {
    return field_identifier_;
  }
  ~TextFieldIndex() = default;

  absl::StatusOr<bool> AddRecord(const InternedStringPtr& key,
                                 absl::string_view data);
  absl::StatusOr<bool> RemoveRecord(const InternedStringPtr& key,
                                    indexes::DeletionType deletion_type) {
    return false; // Placeholder 
  }
  absl::StatusOr<bool> ModifyRecord(const InternedStringPtr& key,
                                    absl::string_view data) {
    return false; // Placeholder 
  }
  int RespondWithInfo(ValkeyModuleCtx* ctx) const {
    return 0; // Placeholder 
  }
  bool IsTracked(const InternedStringPtr& key) const {
    return false; // Placeholder 
  }
  absl::Status SaveIndex(RDBChunkOutputStream chunked_out) const {
    return absl::OkStatus(); // Placeholder 
  }

  std::unique_ptr<data_model::Index> ToProto() const {
    auto index_proto = std::make_unique<data_model::Index>();
    index_proto->mutable_text_index();
    return index_proto;
  }
  void ForEachTrackedKey(
      absl::AnyInvocable<void(const InternedStringPtr&)> fn) const {
    // Placeholder 
  }

  uint64_t GetRecordCount() const {
    return 0; // Placeholder 
  }

 private:
  // Each text field is assigned a unique number within the containing index
  size_t text_field_number = 0;
  // The per-index text index
  std::shared_ptr<TextIndex> text_;
  // Store references to configuration protos
  const data_model::TextIndex& text_index_proto_;
  const data_model::IndexSchema* index_schema_proto_;
  // Field identifier (name or alias)
  std::string field_identifier_;
  // Lexer for text processing
  text::Lexer lexer_;
};


}  // namespace text
}  // namespace valkey_search

namespace valkey_search::indexes {

class Text : public IndexBase {
 public:
  explicit Text(const data_model::TextIndex& text_index_proto);
  absl::StatusOr<bool> AddRecord(const InternedStringPtr& key,
                                 absl::string_view data) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  absl::StatusOr<bool> RemoveRecord(
      const InternedStringPtr& key,
      DeletionType deletion_type = DeletionType::kNone) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  absl::StatusOr<bool> ModifyRecord(const InternedStringPtr& key,
                                    absl::string_view data) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  int RespondWithInfo(RedisModuleCtx* ctx) const override;
  bool IsTracked(const InternedStringPtr& key) const override;
  absl::Status SaveIndex(RDBOutputStream& rdb_stream) const override {
    return absl::OkStatus();
  }

  private:
  // Each text field is assigned a unique number within the containing index, this is used
  // by the Postings object to identify fields.
  size_t text_field_number;
  std::shared_ptr<TextIndex> text_;


  inline void ForEachTrackedKey(
      absl::AnyInvocable<void(const InternedStringPtr&)> fn) const override {
    absl::MutexLock lock(&index_mutex_);
    for (const auto& [key, _] : tracked_tags_by_keys_) {
      fn(key);
    }
  }
  uint64_t GetRecordCount() const override;
  std::unique_ptr<data_model::Index> ToProto() const override;

  InternedStringPtr GetRawValue(const InternedStringPtr& key) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

  class EntriesFetcherIterator : public EntriesFetcherIteratorBase {
   public:
    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
  };

  class EntriesFetcher : public EntriesFetcherBase {
   public:
    size_t Size() const override;
    std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

   private:
  };

  virtual std::unique_ptr<EntriesFetcher> Search(
      const query::TextPredicate& predicate,
      bool negate) const ABSL_NO_THREAD_SAFETY_ANALYSIS;

 private:
  mutable absl::Mutex index_mutex_;
};
}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_H_
