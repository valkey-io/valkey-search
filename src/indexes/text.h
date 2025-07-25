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


namespace valkey_search::indexes {

class Text : public IndexBase {
 public:
  explicit Text(const data_model::TextIndex& text_index_proto) : IndexBase(IndexerType::kText) {
    // TODO: Parse configuration from text_index_proto
  }
  absl::StatusOr<bool> AddRecord(const InternedStringPtr& key,
                                 absl::string_view data) override
      ABSL_LOCKS_EXCLUDED(index_mutex_) {
    absl::MutexLock lock(&index_mutex_);
    tracked_keys_[key] = std::string(data);
    return true;
  }
  absl::StatusOr<bool> RemoveRecord(
      const InternedStringPtr& key,
      DeletionType deletion_type = DeletionType::kNone) override
      ABSL_LOCKS_EXCLUDED(index_mutex_) {
    absl::MutexLock lock(&index_mutex_);
    auto it = tracked_keys_.find(key);
    if (it == tracked_keys_.end()) {
      return false;
    }
    tracked_keys_.erase(it);
    return true;
  }
  absl::StatusOr<bool> ModifyRecord(const InternedStringPtr& key,
                                    absl::string_view data) override
      ABSL_LOCKS_EXCLUDED(index_mutex_) {
    absl::MutexLock lock(&index_mutex_);
    auto it = tracked_keys_.find(key);
    if (it == tracked_keys_.end()) {
      return false;
    }
    it->second = std::string(data);
    return true;
  }
  int RespondWithInfo(ValkeyModuleCtx* ctx) const override {
    ValkeyModule_ReplyWithSimpleString(ctx, "index_type");
    ValkeyModule_ReplyWithSimpleString(ctx, "TEXT");
    return 2;
  }
  bool IsTracked(const InternedStringPtr& key) const override {
    absl::MutexLock lock(&index_mutex_);
    return tracked_keys_.find(key) != tracked_keys_.end();
  }
  absl::Status SaveIndex(RDBChunkOutputStream chunked_out) const override {
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
    for (const auto& [key, _] : tracked_keys_) {
      fn(key);
    }
  }
  uint64_t GetRecordCount() const override {
    absl::MutexLock lock(&index_mutex_);
    return tracked_keys_.size();
  }
  std::unique_ptr<data_model::Index> ToProto() const override {
    auto index = std::make_unique<data_model::Index>();
    auto text_index = std::make_unique<data_model::TextIndex>();
    index->set_allocated_text_index(text_index.release());
    return index;
  }

  InternedStringPtr GetRawValue(const InternedStringPtr& key) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS {
    auto it = tracked_keys_.find(key);
    if (it == tracked_keys_.end()) {
      return nullptr;
    }
    return StringInternStore::Intern(it->second);
  }

  class EntriesFetcherIterator : public EntriesFetcherIteratorBase {
   public:
    bool Done() const override {
      return true;
    }
    void Next() override {
      // TODO: Implement iterator logic
    }
    const InternedStringPtr& operator*() const override {
      static InternedStringPtr empty_ptr;
      return empty_ptr;
    }

   private:
  };

  class EntriesFetcher : public EntriesFetcherBase {
   public:
    size_t Size() const override {
      return 0;
    }
    std::unique_ptr<EntriesFetcherIteratorBase> Begin() override {
      return std::make_unique<EntriesFetcherIterator>();
    }

   private:
  };

  virtual std::unique_ptr<EntriesFetcher> Search(
      const query::TextPredicate& predicate,
      bool negate) const ABSL_NO_THREAD_SAFETY_ANALYSIS {
    return std::make_unique<EntriesFetcher>();
  }

 private:
  mutable absl::Mutex index_mutex_;
  absl::flat_hash_map<InternedStringPtr, std::string> tracked_keys_;
};
}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_H_
