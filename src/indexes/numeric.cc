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

#include "src/indexes/numeric.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>

#include "absl/container/flat_hash_set.h"
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
    // If key doesn't have TAG but exists, insert it to untracked_keys_.
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

bool Numeric::IsTracked(const InternedStringPtr& key) const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_keys_.contains(key);
}

std::unique_ptr<data_model::Index> Numeric::ToProto() const {
  auto index_proto = std::make_unique<data_model::Index>();
  auto numeric_index = std::make_unique<data_model::NumericIndex>();
  index_proto->set_allocated_numeric_index(numeric_index.release());
  return index_proto;
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

bool Numeric::EntriesFetcherIterator::NextKeys(
    const Numeric::EntriesRange& range, BTreeNumericIndex::ConstIterator& iter,
    std::optional<InternedStringSet::const_iterator>& keys_iter) {
  while (iter != range.second) {
    if (!keys_iter.has_value()) {
      keys_iter = iter->second.begin();
    } else {
      ++keys_iter.value();
    }
    if (keys_iter.value() != iter->second.end()) {
      return true;
    }
    ++iter;
    keys_iter = std::nullopt;
  }
  return false;
}

Numeric::EntriesFetcherIterator::EntriesFetcherIterator(
    const EntriesRange& entries_range,
    const std::optional<EntriesRange>& additional_entries_range,
    const InternedStringSet* untracked_keys)
    : entries_range_(entries_range),
      entries_iter_(entries_range_.first),
      additional_entries_range_(additional_entries_range),
      untracked_keys_(untracked_keys) {
  if (additional_entries_range_.has_value()) {
    additional_entries_iter_ = additional_entries_range_.value().first;
  }
}

bool Numeric::EntriesFetcherIterator::Done() const {
  return entries_iter_ == entries_range_.second &&
         (!additional_entries_range_.has_value() ||
          additional_entries_iter_ ==
              additional_entries_range_.value().second) &&
         (untracked_keys_ == nullptr ||
          (untracked_keys_iter_.has_value() &&
           untracked_keys_iter_ == untracked_keys_->end()));
}

void Numeric::EntriesFetcherIterator::Next() {
  if (NextKeys(entries_range_, entries_iter_, entry_keys_iter_)) {
    return;
  }
  if (additional_entries_range_.has_value() &&
      NextKeys(additional_entries_range_.value(), additional_entries_iter_,
               additional_entry_keys_iter_)) {
    return;
  }
  if (untracked_keys_) {
    if (untracked_keys_iter_.has_value()) {
      ++untracked_keys_iter_.value();
    } else {
      untracked_keys_iter_ = untracked_keys_->begin();
    }
  }
}

const InternedStringPtr& Numeric::EntriesFetcherIterator::operator*() const {
  if (entries_iter_ != entries_range_.second) {
    DCHECK(entry_keys_iter_ != entries_iter_->second.end());
    return *entry_keys_iter_.value();
  }
  if (additional_entries_range_.has_value() &&
      additional_entries_iter_ != additional_entries_range_.value().second) {
    DCHECK(additional_entry_keys_iter_ !=
           additional_entries_iter_->second.end());
    return *additional_entry_keys_iter_.value();
  }
  DCHECK(untracked_keys_ && untracked_keys_iter_.has_value() &&
         untracked_keys_iter_ != untracked_keys_->end());
  return *untracked_keys_iter_.value();
}

size_t Numeric::EntriesFetcher::Size() const { return size_; }

std::unique_ptr<EntriesFetcherIteratorBase> Numeric::EntriesFetcher::Begin() {
  auto itr = std::make_unique<EntriesFetcherIterator>(
      entries_range_, additional_entries_range_, untracked_keys_);
  itr->Next();
  return itr;
}

uint64_t Numeric::GetRecordCount() const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_keys_.size();
}

}  // namespace valkey_search::indexes
