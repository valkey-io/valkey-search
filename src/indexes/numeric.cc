/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/numeric.h"

#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <string>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/index_base.h"
#include "src/query/predicate.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {
namespace {
std::optional<double> ParseNumber(absl::string_view data) {
  double value;
  if (absl::AsciiStrToLower(data) == "nan" || !absl::SimpleAtod(data, &value)) {
    return std::nullopt;
  }
  return value;
}
}  // namespace

Numeric::Numeric(const data_model::NumericIndex& numeric_index_proto)
    : IndexBase(IndexerType::kNumeric) {
  index_ = std::make_unique<BTreeNumericIndex>();
}

absl::StatusOr<bool> Numeric::AddRecord(const InternedStringPtr& key,
                                        absl::string_view data) {
  auto value = ParseNumber(data);
  absl::MutexLock lock(&index_mutex_);
  if (!value.has_value()) {
    untracked_keys_.insert(key);
    return false;
  }
  auto [_, succ] = tracked_keys_.insert({key, *value});
  if (!succ) {
    return absl::AlreadyExistsError(
        absl::StrCat("Key `", key->Str(), "` already exists"));
  }
  untracked_keys_.erase(key);
  index_->Add(key, *value);
  return true;
}

absl::StatusOr<bool> Numeric::ModifyRecord(const InternedStringPtr& key,
                                           absl::string_view data) {
  auto value = ParseNumber(data);
  if (!value.has_value()) {
    [[maybe_unused]] auto res =
        RemoveRecord(key, indexes::DeletionType::kIdentifier);
    return false;
  }
  absl::MutexLock lock(&index_mutex_);
  auto it = tracked_keys_.find(key);
  if (it == tracked_keys_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Key `", key->Str(), "` not found"));
  }

  index_->Modify(it->first, it->second, *value);
  it->second = *value;
  return true;
}

absl::StatusOr<bool> Numeric::RemoveRecord(const InternedStringPtr& key,
                                           DeletionType deletion_type) {
  absl::MutexLock lock(&index_mutex_);
  if (deletion_type == DeletionType::kRecord) {
    // If key is DELETED, remove it from untracked_keys_.
    untracked_keys_.erase(key);
  } else {
    // If key doesn't have NUMERIC but exists, insert it to untracked_keys_.
    untracked_keys_.insert(key);
  }
  auto it = tracked_keys_.find(key);
  if (it == tracked_keys_.end()) {
    return false;
  }

  index_->Remove(it->first, it->second);
  tracked_keys_.erase(it);
  return true;
}

int Numeric::RespondWithInfo(ValkeyModuleCtx* ctx) const {
  ValkeyModule_ReplyWithSimpleString(ctx, "type");
  ValkeyModule_ReplyWithSimpleString(ctx, "NUMERIC");
  ValkeyModule_ReplyWithSimpleString(ctx, "size");
  absl::MutexLock lock(&index_mutex_);
  ValkeyModule_ReplyWithCString(ctx,
                                std::to_string(tracked_keys_.size()).c_str());
  return 4;
}

std::unique_ptr<data_model::Index> Numeric::ToProto() const {
  auto index_proto = std::make_unique<data_model::Index>();
  auto numeric_index = std::make_unique<data_model::NumericIndex>();
  index_proto->set_allocated_numeric_index(numeric_index.release());
  return index_proto;
}

uint32_t Numeric::GetMutationWeight() const {
  return options::GetMutationWeightNumeric().GetValue();
}

const double* Numeric::GetValue(const InternedStringPtr& key) const {
  // Note that the Numeric index is not mutated while the time sliced mutex is
  // in a read mode and therefor it is safe to skip lock acquiring.
  if (auto it = tracked_keys_.find(key); it != tracked_keys_.end()) {
    return &it->second;
  }
  return nullptr;
}

std::unique_ptr<Numeric::EntriesFetcher> Numeric::Search(
    const query::NumericPredicate& predicate, bool negate) const {
  EntriesRange entries_range;
  const auto& btree = index_->GetBtree();
  if (negate) {
    auto size =
        index_->GetCount(std::numeric_limits<double>::lowest(),
                         predicate.GetStart(), true,
                         !predicate.IsStartInclusive()) +
        index_->GetCount(predicate.GetEnd(), std::numeric_limits<double>::max(),
                         !predicate.IsEndInclusive(), true);
    entries_range.first = btree.begin();
    entries_range.second = predicate.IsStartInclusive()
                               ? btree.lower_bound(predicate.GetStart())
                               : btree.upper_bound(predicate.GetStart());
    EntriesRange additional_entries_range;
    additional_entries_range.first =
        predicate.IsEndInclusive() ? btree.upper_bound(predicate.GetEnd())
                                   : btree.lower_bound(predicate.GetEnd());
    ;
    additional_entries_range.second = btree.end();
    return std::make_unique<Numeric::EntriesFetcher>(
        entries_range, size + untracked_keys_.size(), additional_entries_range,
        &untracked_keys_);
  }

  entries_range.first = predicate.IsStartInclusive()
                            ? btree.lower_bound(predicate.GetStart())
                            : btree.upper_bound(predicate.GetStart());
  entries_range.second = predicate.IsEndInclusive()
                             ? btree.upper_bound(predicate.GetEnd())
                             : btree.lower_bound(predicate.GetEnd());
  size_t size = index_->GetCount(predicate.GetStart(), predicate.GetEnd(),
                                 predicate.IsStartInclusive(),
                                 predicate.IsEndInclusive());
  return std::make_unique<Numeric::EntriesFetcher>(entries_range, size);
}

Numeric::EntriesFetcherIterator::EntriesFetcherIterator(
    const EntriesRange& entries_range,
    const std::optional<EntriesRange>& additional_entries_range,
    const InternedStringSet* untracked_keys)
    : negate_(additional_entries_range.has_value()),
      untracked_keys_(untracked_keys) {
  InitHeap(entries_range);
  if (additional_entries_range.has_value()) {
    InitHeap(additional_entries_range.value());
  }
  heap_.heapify();
  AdvanceToNext();
}

void Numeric::EntriesFetcherIterator::InitHeap(const EntriesRange& range) {
  for (auto it = range.first; it != range.second; ++it) {
    if (!it->second.empty()) {
      heap_.push_back_unsorted(
          HeapEntry{it->second.begin(), it->second.end()});
    }
  }
}

void Numeric::EntriesFetcherIterator::AdvanceToNext() {
  // Skip duplicates (same key in multiple numeric buckets is impossible,
  // but keep pattern consistent).
  while (!heap_.empty()) {
    const auto& top = heap_.min();
    if (current_key_ && *top.current == current_key_) {
      HeapEntry entry = top;
      heap_.pop_min();
      ++entry.current;
      if (entry.current != entry.end) {
        heap_.emplace(entry);
      }
    } else {
      current_key_ = *top.current;
      return;
    }
  }
  // Heap exhausted — untracked keys for negate only.
  if (negate_ && untracked_keys_ && !untracked_keys_->empty()) {
    if (!untracked_keys_iter_.has_value()) {
      untracked_keys_iter_ = untracked_keys_->begin();
    } else {
      ++untracked_keys_iter_.value();
    }
    if (untracked_keys_iter_.value() != untracked_keys_->end()) {
      current_key_ = *untracked_keys_iter_.value();
      return;
    }
  }
  current_key_ = {};
}

bool Numeric::EntriesFetcherIterator::Done() const { return !current_key_; }

void Numeric::EntriesFetcherIterator::Next() {
  if (!current_key_) return;
  // Advance all heap entries on current_key_.
  while (!heap_.empty() && *heap_.min().current == current_key_) {
    HeapEntry entry = heap_.min();
    heap_.pop_min();
    ++entry.current;
    if (entry.current != entry.end) {
      heap_.emplace(entry);
    }
  }
  AdvanceToNext();
}

const InternedStringPtr& Numeric::EntriesFetcherIterator::operator*() const {
  return current_key_;
}

bool Numeric::EntriesFetcherIterator::SeekForwardKey(
    const InternedStringPtr& target) {
  if (!current_key_) return false;
  if (current_key_ >= target) return true;
  // Drain heap entries behind target.
  while (!heap_.empty() && *heap_.min().current < target) {
    HeapEntry entry = heap_.min();
    heap_.pop_min();
    while (entry.current != entry.end && *entry.current < target) {
      ++entry.current;
    }
    if (entry.current != entry.end) {
      heap_.emplace(entry);
    }
  }
  current_key_ = {};
  AdvanceToNext();
  return !Done();
}

size_t Numeric::EntriesFetcher::Size() const { return size_; }

std::unique_ptr<EntriesFetcherIteratorBase> Numeric::EntriesFetcher::Begin() {
  auto itr = std::make_unique<EntriesFetcherIterator>(
      entries_range_, additional_entries_range_, untracked_keys_);
  return itr;
}

size_t Numeric::GetTrackedKeyCount() const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_keys_.size();
}

size_t Numeric::GetUnTrackedKeyCount() const {
  absl::MutexLock lock(&index_mutex_);
  return untracked_keys_.size();
}

bool Numeric::IsTracked(const InternedStringPtr& key) const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_keys_.contains(key);
}

bool Numeric::IsUnTracked(const InternedStringPtr& key) const {
  absl::MutexLock lock(&index_mutex_);
  return untracked_keys_.contains(key);
}

void Numeric::UnTrack(const InternedStringPtr& key) {
  absl::MutexLock lock(&index_mutex_);
  CHECK(!tracked_keys_.contains(key));
  untracked_keys_.insert(key);
}

absl::Status Numeric::ForEachTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn) const {
  absl::MutexLock lock(&index_mutex_);
  for (const auto& [key, _] : tracked_keys_) {
    VMSDK_RETURN_IF_ERROR(fn(key));
  }
  return absl::OkStatus();
}

absl::Status Numeric::ForEachUnTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn) const {
  absl::MutexLock lock(&index_mutex_);
  for (const auto& key : untracked_keys_) {
    VMSDK_RETURN_IF_ERROR(fn(key));
  }
  return absl::OkStatus();
}

}  // namespace valkey_search::indexes
