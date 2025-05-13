/*
 * Copyright (c) 2025, valkey-search contributors
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

#include "src/indexes/tag.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/index_base.h"
#include "src/query/predicate.h"
#include "src/utils/patricia_tree.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

// For performance reasons, a minimum term length is enforced. The default is 2,
// but configurable.
const int16_t kDefaultMinPrefixLength{2};

static bool IsValidPrefix(absl::string_view str) {
  return str.length() < 2 || str[str.length() - 1] != '*' ||
         str[str.length() - 2] != '*';
}

Tag::Tag(const data_model::TagIndex& tag_index_proto)
    : IndexBase(IndexerType::kTag),
      separator_(tag_index_proto.separator()[0]),
      case_sensitive_(tag_index_proto.case_sensitive()),
      tree_(case_sensitive_) {}

absl::StatusOr<bool> Tag::AddRecord(const InternedStringPtr& key,
                                    absl::string_view data) {
  auto interned_data = StringInternStore::Intern(data);
  auto parsed_tags = ParseRecordTags(*interned_data, separator_);
  absl::MutexLock lock(&index_mutex_);
  if (parsed_tags.empty()) {
    untracked_keys_.insert(key);
    return false;
  }
  auto [_, succ] = tracked_tags_by_keys_.insert(
      {key, TagInfo{.raw_tag_string = std::move(interned_data),
                    .tags = parsed_tags}});
  if (!succ) {
    return absl::AlreadyExistsError(
        absl::StrCat("Key `", key->Str(), "` already exists"));
  }
  untracked_keys_.erase(key);
  for (const auto& tag : parsed_tags) {
    tree_.AddKeyValue(tag, key);
  }
  return true;
}

absl::StatusOr<absl::flat_hash_set<absl::string_view>> Tag::ParseSearchTags(
    absl::string_view data, char separator) {
  absl::flat_hash_set<absl::string_view> parsed_tags;
  std::vector<absl::string_view> parts = absl::StrSplit(data, separator);
  for (const auto& part : parts) {
    auto tag = absl::StripAsciiWhitespace(part);
    if (tag.empty()) {
      continue;
    }

    // Prefix tag is identified by a trailing '*'.
    if (tag.back() == '*') {
      if (!IsValidPrefix(tag)) {
        return absl::InvalidArgumentError(
            absl::StrCat("Tag string `", tag, "` ends with multiple *."));
      }
      // Prefix tags that are shorter than min length are ignored.
      if (tag.length() <= kDefaultMinPrefixLength) {
        continue;
      }
    }
    parsed_tags.insert(tag);
  }
  return parsed_tags;
}

absl::flat_hash_set<absl::string_view> Tag::ParseRecordTags(
    absl::string_view data, char separator) {
  absl::flat_hash_set<absl::string_view> parsed_tags;
  for (const auto& part : absl::StrSplit(data, separator)) {
    auto tag = absl::StripAsciiWhitespace(part);
    if (!tag.empty()) {
      parsed_tags.insert(tag);
    }
  }
  return parsed_tags;
}

absl::StatusOr<bool> Tag::ModifyRecord(const InternedStringPtr& key,
                                       absl::string_view data) {
  // TODO: implement operator [] in patriciatree.
  auto interned_data = StringInternStore::Intern(data);
  auto new_parsed_tags = ParseRecordTags(*interned_data, separator_);
  if (new_parsed_tags.empty()) {
    [[maybe_unused]] auto res =
        RemoveRecord(key, indexes::DeletionType::kIdentifier);
    return false;
  }
  absl::MutexLock lock(&index_mutex_);

  auto it = tracked_tags_by_keys_.find(key);
  if (it == tracked_tags_by_keys_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Key `", key->Str(), "` not found"));
  }
  auto& tag_info = it->second;

  // insert new tags that are not present in the old tags.
  for (const auto& tag : new_parsed_tags) {
    if (!tag_info.tags.contains(tag)) {
      tree_.AddKeyValue(tag, key);
    }
  }

  // remove old tags that are not present in the new tags.
  for (const auto& tag : tag_info.tags) {
    if (!new_parsed_tags.contains(tag)) {
      tree_.Remove(tag, key);
    }
  }

  tag_info.tags = new_parsed_tags;
  tag_info.raw_tag_string = std::move(interned_data);
  return true;
}

absl::StatusOr<bool> Tag::RemoveRecord(const InternedStringPtr& key,
                                       DeletionType deletion_type) {
  absl::MutexLock lock(&index_mutex_);
  if (deletion_type == DeletionType::kRecord) {
    // If key is DELETED, remove it from untracked_keys_.
    untracked_keys_.erase(key);
  } else {
    // If key doesn't have TAG but exists, insert it to untracked_keys_.
    untracked_keys_.insert(key);
  }
  auto it = tracked_tags_by_keys_.find(key);
  if (it == tracked_tags_by_keys_.end()) {
    return false;
  }
  auto& tag_info = it->second;
  for (const auto& tag : tag_info.tags) {
    tree_.Remove(tag, key);
  }
  tracked_tags_by_keys_.erase(it);
  return true;
}

int Tag::RespondWithInfo(RedisModuleCtx* ctx) const {
  auto num_replies = 6;
  RedisModule_ReplyWithSimpleString(ctx, "type");
  RedisModule_ReplyWithSimpleString(ctx, "TAG");
  RedisModule_ReplyWithSimpleString(ctx, "SEPARATOR");
  RedisModule_ReplyWithSimpleString(
      ctx, std::string(&separator_, sizeof(char)).c_str());
  if (case_sensitive_) {
    num_replies++;
    RedisModule_ReplyWithSimpleString(ctx, "CASESENSITIVE");
  }
  RedisModule_ReplyWithSimpleString(ctx, "size");
  absl::MutexLock lock(&index_mutex_);
  RedisModule_ReplyWithCString(
      ctx, std::to_string(tracked_tags_by_keys_.size()).c_str());
  return num_replies;
}

bool Tag::IsTracked(const InternedStringPtr& key) const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_tags_by_keys_.contains(key);
}

std::unique_ptr<data_model::Index> Tag::ToProto() const {
  auto index_proto = std::make_unique<data_model::Index>();
  auto tag_index = std::make_unique<data_model::TagIndex>();
  tag_index->set_separator(absl::string_view(&separator_, 1));
  tag_index->set_case_sensitive(case_sensitive_);
  index_proto->set_allocated_tag_index(tag_index.release());
  return index_proto;
}

InternedStringPtr Tag::GetRawValue(const InternedStringPtr& key) const {
  // Note that the Tag index is not mutated while the time sliced mutex is
  // in a read mode and therefor it is safe to skip lock acquiring.
  if (auto it = tracked_tags_by_keys_.find(key);
      it != tracked_tags_by_keys_.end()) {
    return it->second.raw_tag_string;
  }
  return nullptr;
}

const absl::flat_hash_set<absl::string_view>* Tag::GetValue(
    const InternedStringPtr& key, bool& case_sensitive) const {
  // Note that the Tag index is not mutated while the time sliced mutex is
  // in a read mode and therefor it is safe to skip lock acquiring.
  if (auto it = tracked_tags_by_keys_.find(key);
      it != tracked_tags_by_keys_.end()) {
    case_sensitive = case_sensitive_;
    return &it->second.tags;
  }
  return nullptr;
}

Tag::EntriesFetcherIterator::EntriesFetcherIterator(
    const PatriciaTreeIndex& tree,
    absl::flat_hash_set<PatriciaNodeIndex*>& entries,
    const InternedStringSet& untracked_keys, bool negate)
    : tree_iter_(tree.RootIterator()),
      entries_(entries),
      untracked_keys_(untracked_keys),
      negate_(negate) {}

bool Tag::EntriesFetcherIterator::Done() const {
  if (negate_) {
    return tree_iter_.Done() &&
           (untracked_keys_.empty() ||
            (untracked_keys_iter_.has_value() &&
             untracked_keys_iter_.value() == untracked_keys_.end()));
  }
  return entries_.empty() && next_node_ == nullptr;
}

void Tag::EntriesFetcherIterator::NextNegate() {
  if (next_node_) {
    ++next_iter_;
    if (next_iter_ != next_node_->value.value().end()) {
      return;
    }
    tree_iter_.Next();
  }
  while (!tree_iter_.Done()) {
    next_node_ = tree_iter_.Value();
    if (next_node_ && !entries_.contains(next_node_) &&
        next_node_->value.has_value() && !next_node_->value.value().empty()) {
      next_iter_ = next_node_->value.value().begin();
      return;
    }
    tree_iter_.Next();
  }
  next_node_ = nullptr;
  if (!untracked_keys_iter_.has_value()) {
    untracked_keys_iter_ = untracked_keys_.begin();
    return;
  }
  ++untracked_keys_iter_.value();
}

void Tag::EntriesFetcherIterator::Next() {
  if (negate_) {
    NextNegate();
    return;
  }
  if (next_node_) {
    ++next_iter_;
    if (next_iter_ != next_node_->value.value().end()) {
      return;
    }
  }
  while (!entries_.empty()) {
    auto itr = entries_.begin();
    next_node_ = *itr;
    entries_.erase(itr);
    if (next_node_->value.has_value() && !next_node_->value.value().empty()) {
      next_iter_ = next_node_->value.value().begin();
      return;
    }
  }
  next_node_ = nullptr;
}

const InternedStringPtr& Tag::EntriesFetcherIterator::operator*() const {
  if (negate_ && tree_iter_.Done()) {
    return *untracked_keys_iter_.value();
  }
  return *next_iter_;
}

// TODO: b/357027854 - Support Suffix/Infix Search
std::unique_ptr<Tag::EntriesFetcher> Tag::Search(
    const query::TagPredicate& predicate, bool negate) const {
  absl::flat_hash_set<PatriciaNodeIndex*> entries;
  size_t size = 0;

  for (const auto& tag : predicate.GetTags()) {
    if (tag.back() == '*') {
      auto prefix_tag = tag.substr(0, tag.length() - 1);
      for (auto it = tree_.PrefixMatcher(prefix_tag); !it.Done(); it.Next()) {
        PatriciaNodeIndex* node = it.Value();
        if (node != nullptr) {
          auto res = entries.insert(node);
          if (res.second) {
            size += node->value.value().size();
          }
        }
      }
    } else {
      // exact search
      PatriciaNodeIndex* node = tree_.ExactMatcher(tag);
      if (node != nullptr) {
        auto res = entries.insert(node);
        if (res.second) {
          size += node->value.value().size();
        }
      }
    }
  }
  if (negate) {
    size = tracked_tags_by_keys_.size() > size
               ? tracked_tags_by_keys_.size() - size
               : tracked_tags_by_keys_.size();
    size += untracked_keys_.size();
  }
  return std::make_unique<Tag::EntriesFetcher>(tree_, entries, size, negate,
                                               untracked_keys_);
}

std::unique_ptr<EntriesFetcherIteratorBase> Tag::EntriesFetcher::Begin() {
  auto itr = std::make_unique<EntriesFetcherIterator>(tree_, entries_,
                                                      untracked_keys_, negate_);
  itr->Next();
  return itr;
}

size_t Tag::EntriesFetcher::Size() const { return size_; }

uint64_t Tag::GetRecordCount() const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_tags_by_keys_.size();
}

}  // namespace valkey_search::indexes
