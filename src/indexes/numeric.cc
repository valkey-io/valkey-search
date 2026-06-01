/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/numeric.h"

#include <cstddef>
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
  index_ = std::make_unique<TreeType>();
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
  index_->Insert(*value, key);
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

  index_->Erase(it->second, it->first);
  index_->Insert(*value, it->first);
  it->second = *value;
  return true;
}

absl::StatusOr<bool> Numeric::RemoveRecord(const InternedStringPtr& key,
                                           DeletionType deletion_type) {
  absl::MutexLock lock(&index_mutex_);
  if (deletion_type == DeletionType::kRecord) {
    untracked_keys_.erase(key);
  } else {
    untracked_keys_.insert(key);
  }
  auto it = tracked_keys_.find(key);
  if (it == tracked_keys_.end()) {
    return false;
  }

  index_->Erase(it->second, it->first);
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
  if (auto it = tracked_keys_.find(key); it != tracked_keys_.end()) {
    return &it->second;
  }
  return nullptr;
}

std::unique_ptr<Numeric::EntriesFetcher> Numeric::Search(
    const query::NumericPredicate& predicate, bool negate) const {
  TreeIterator start_iter =
      predicate.IsStartInclusive()
          ? index_->LowerBoundByValue(predicate.GetStart())
          : index_->UpperBoundByValue(predicate.GetStart());
  TreeIterator end_iter = predicate.IsEndInclusive()
                              ? index_->UpperBoundByValue(predicate.GetEnd())
                              : index_->LowerBoundByValue(predicate.GetEnd());

  size_t in_range =
      index_->Count(predicate.GetStart(), predicate.GetEnd(),
                    predicate.IsStartInclusive(), predicate.IsEndInclusive());

  if (negate) {
    EntriesRange below{index_->Begin(), start_iter};
    EntriesRange above{end_iter, index_->End()};
    size_t total = index_->TotalPostings() - in_range + untracked_keys_.size();
    return std::make_unique<Numeric::EntriesFetcher>(below, total, above,
                                                     &untracked_keys_);
  }

  EntriesRange in_range_iters{start_iter, end_iter};
  return std::make_unique<Numeric::EntriesFetcher>(in_range_iters, in_range);
}

Numeric::EntriesFetcherIterator::EntriesFetcherIterator(
    const EntriesRange& entries_range,
    const std::optional<EntriesRange>& additional_entries_range,
    const KeySet* untracked_keys)
    : entries_range_(entries_range),
      entries_iter_(entries_range_.first),
      additional_entries_range_(additional_entries_range),
      untracked_keys_(untracked_keys) {
  if (additional_entries_range_.has_value()) {
    additional_entries_iter_ = additional_entries_range_.value().first;
  }
  // If both tree ranges are empty but we have untracked keys, jump straight
  // to the untracked phase so operator*() is well-defined.
  if (entries_iter_ == entries_range_.last &&
      (!additional_entries_range_.has_value() ||
       additional_entries_iter_ == additional_entries_range_.value().last) &&
      untracked_keys_) {
    untracked_keys_iter_ = untracked_keys_->begin();
  }
}

bool Numeric::EntriesFetcherIterator::Done() const {
  if (entries_iter_ != entries_range_.last) {
    return false;
  }
  if (additional_entries_range_.has_value() &&
      additional_entries_iter_ != additional_entries_range_.value().last) {
    return false;
  }
  if (untracked_keys_ == nullptr) {
    return true;
  }
  return untracked_keys_iter_.has_value() &&
         untracked_keys_iter_.value() == untracked_keys_->end();
}

void Numeric::EntriesFetcherIterator::Next() {
  if (entries_iter_ != entries_range_.last) {
    ++entries_iter_;
    if (entries_iter_ != entries_range_.last) {
      return;
    }
    if (additional_entries_range_.has_value() &&
        additional_entries_iter_ != additional_entries_range_.value().last) {
      return;
    }
    if (untracked_keys_) {
      untracked_keys_iter_ = untracked_keys_->begin();
    }
    return;
  }
  if (additional_entries_range_.has_value() &&
      additional_entries_iter_ != additional_entries_range_.value().last) {
    ++additional_entries_iter_;
    if (additional_entries_iter_ != additional_entries_range_.value().last) {
      return;
    }
    if (untracked_keys_) {
      untracked_keys_iter_ = untracked_keys_->begin();
    }
    return;
  }
  if (untracked_keys_ && untracked_keys_iter_.has_value() &&
      untracked_keys_iter_.value() != untracked_keys_->end()) {
    ++untracked_keys_iter_.value();
  }
}

const InternedStringPtr& Numeric::EntriesFetcherIterator::operator*() const {
  if (entries_iter_ != entries_range_.last) {
    return *entries_iter_;
  }
  if (additional_entries_range_.has_value() &&
      additional_entries_iter_ != additional_entries_range_.value().last) {
    return *additional_entries_iter_;
  }
  DCHECK(untracked_keys_ && untracked_keys_iter_.has_value() &&
         untracked_keys_iter_.value() != untracked_keys_->end());
  return *untracked_keys_iter_.value();
}

size_t Numeric::EntriesFetcher::Size() const { return size_; }

std::unique_ptr<EntriesFetcherIteratorBase> Numeric::EntriesFetcher::Begin() {
  return std::make_unique<EntriesFetcherIterator>(
      entries_range_, additional_entries_range_, untracked_keys_);
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
