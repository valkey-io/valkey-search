/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/numeric.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
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
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/indexes/key_attr_value.h"
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

// Helpers that read the SlotT through the schema for `pos`. Caller holds
// the appropriate lock (index_mutex_ for writes, read-epoch invariant for
// reads).
const NumericSlot* TryGetSlot(const IndexSchema* schema, uint16_t pos,
                              const InternedStringPtr& key) {
  if (schema == nullptr) {
    return nullptr;
  }
  const KeyAttrValue* kav = schema->FindKAV(key);
  if (kav == nullptr) {
    return nullptr;
  }
  const Slot& slot = kav->slots[pos];
  if (!IsOccupied(slot)) {
    return nullptr;
  }
  return std::launder(reinterpret_cast<const NumericSlot*>(slot.storage));
}
}  // namespace

Numeric::Numeric(const data_model::NumericIndex& numeric_index_proto)
    : TypedIndex<Numeric, NumericSlot>(IndexerType::kNumeric) {
  index_ = std::make_unique<BTreeNumericIndex>();
}

absl::StatusOr<bool> Numeric::AddRecord(const InternedStringPtr& key,
                                        absl::string_view data) {
  auto value = ParseNumber(data);
  absl::MutexLock lock(&index_mutex_);
  // Defensive: production callers (ProcessAttributeMutation) dispatch through
  // IsTracked → ModifyRecord; tests can hit AddRecord on an already-occupied
  // slot, so report AlreadyExists rather than CHECK-failing in OccupySlot.
  if (KeyAttrValue* kav = schema_->FindKAV(key);
      kav != nullptr && IsOccupied(kav->slots[pos_])) {
    return absl::AlreadyExistsError(
        absl::StrCat("Key already exists in numeric index: ", key->Str()));
  }
  if (!value.has_value()) {
    // Unparseable input: slot stays empty, but the key has no numeric value,
    // so it belongs in the missing list. Link if not already linked.
    if (!schema_->IsLinked(pos_, key)) {
      schema_->LinkMissing(pos_, key);
    }
    return false;
  }
  // OccupySlot consumes the empty Missing and bumps the occupied count.
  // We placement-new the NumericSlot on top, with the SlotBase header set.
  std::byte* storage = OccupySlot(key, data.size());
  new (storage) NumericSlot{
      {/*occupied=*/1u, /*user_data_len=*/static_cast<uint32_t>(data.size())},
      *value};
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
  KeyAttrValue* kav = schema_->FindKAV(key);
  if (kav == nullptr || !IsOccupied(kav->slots[pos_])) {
    return absl::NotFoundError(
        absl::StrCat("Key not tracked in numeric index: ", key->Str()));
  }
  Slot& slot = kav->slots[pos_];
  NumericSlot& nslot = SlotAs(slot.storage);
  index_->Modify(key, nslot.value, *value);
  nslot.value = *value;
  if (nslot.user_data_len != data.size()) {
    ResizeSlot(key, data.size());
  }
  return true;
}

absl::StatusOr<bool> Numeric::RemoveRecord(const InternedStringPtr& key,
                                           DeletionType deletion_type) {
  absl::MutexLock lock(&index_mutex_);
  KeyAttrValue* kav = schema_->FindKAV(key);
  if (kav == nullptr) {
    return false;
  }
  Slot& slot = kav->slots[pos_];
  if (IsOccupied(slot)) {
    NumericSlot& nslot = SlotAs(slot.storage);
    index_->Remove(key, nslot.value);
    // VacateSlot overwrites the bytes with a fresh Missing. For kRecord
    // (whole-key delete) we don't relink — the KAV is about to be freed.
    VacateSlot(key, /*relink=*/deletion_type != DeletionType::kRecord);
    return true;
  }
  // Slot already empty. For kIdentifier/kNone, ensure it's linked into the
  // missing list (covers brand-new-key first-notification). For kRecord,
  // ensure it's unlinked (caller will free the KAV next).
  if (deletion_type == DeletionType::kRecord) {
    if (schema_->IsLinked(pos_, key)) {
      schema_->UnlinkMissing(pos_, key);
    }
  } else {
    if (!schema_->IsLinked(pos_, key)) {
      schema_->LinkMissing(pos_, key);
    }
  }
  return false;
}

void Numeric::DestructTyped(const InternedStringPtr& key, NumericSlot& slot) {
  // Whole-key delete safety net (DestroyKeyAttrValue path). Remove this key
  // from the btree/segment tree using the slot's stored value. The slot's
  // bytes are overwritten by the caller (DestroyKeyAttrValue runs the
  // Missing destructor afterward) so we don't need to touch them.
  absl::MutexLock lock(&index_mutex_);
  index_->Remove(key, slot.value);
}

int Numeric::RespondWithInfo(ValkeyModuleCtx* ctx) const {
  ValkeyModule_ReplyWithSimpleString(ctx, "type");
  ValkeyModule_ReplyWithSimpleString(ctx, "NUMERIC");
  ValkeyModule_ReplyWithSimpleString(ctx, "size");
  absl::MutexLock lock(&index_mutex_);
  ValkeyModule_ReplyWithCString(
      ctx, std::to_string(schema_->OccupiedCount(pos_)).c_str());
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
  // Note: Numeric is not mutated while time_sliced_mutex_ is in read mode,
  // so it's safe to skip lock acquisition here.
  const NumericSlot* nslot = TryGetSlot(schema_, pos_, key);
  return nslot == nullptr ? nullptr : &nslot->value;
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
    additional_entries_range.second = btree.end();
    const size_t missing_size = schema_->MissingListAt(pos_).size;
    return std::make_unique<Numeric::EntriesFetcher>(
        entries_range, size + missing_size, additional_entries_range, schema_,
        pos_);
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
    const IndexSchema* schema_for_missing, uint16_t pos)
    : entries_range_(entries_range),
      entries_iter_(entries_range_.first),
      additional_entries_range_(additional_entries_range),
      schema_for_missing_(schema_for_missing),
      pos_(pos) {
  if (additional_entries_range_.has_value()) {
    additional_entries_iter_ = additional_entries_range_.value().first;
  }
}

Numeric::EntriesFetcherIterator::~EntriesFetcherIterator() = default;

bool Numeric::EntriesFetcherIterator::Done() const {
  const bool btree_done =
      entries_iter_ == entries_range_.second &&
      (!additional_entries_range_.has_value() ||
       additional_entries_iter_ == additional_entries_range_.value().second);
  if (!btree_done) {
    return false;
  }
  if (schema_for_missing_ == nullptr) {
    return true;
  }
  // Missing-list portion: not done until we've started it AND walked off.
  return missing_started_ && (missing_iter_ == nullptr || missing_iter_->Done());
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
  if (schema_for_missing_ != nullptr) {
    if (!missing_started_) {
      missing_iter_ = std::make_unique<MissingListIterator>(
          MissingListBegin(schema_for_missing_, pos_));
      missing_started_ = true;
    } else if (missing_iter_ != nullptr && !missing_iter_->Done()) {
      missing_iter_->Next();
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
  DCHECK(missing_iter_ != nullptr && !missing_iter_->Done());
  return missing_iter_->Key();
}

size_t Numeric::EntriesFetcher::Size() const { return size_; }

std::unique_ptr<EntriesFetcherIteratorBase> Numeric::EntriesFetcher::Begin() {
  auto itr = std::make_unique<EntriesFetcherIterator>(
      entries_range_, additional_entries_range_, schema_for_missing_, pos_);
  itr->Next();
  return itr;
}

size_t Numeric::GetTrackedKeyCount() const {
  absl::MutexLock lock(&index_mutex_);
  return schema_->OccupiedCount(pos_);
}

size_t Numeric::GetUnTrackedKeyCount() const {
  absl::MutexLock lock(&index_mutex_);
  return schema_->MissingListAt(pos_).size;
}

bool Numeric::IsTracked(const InternedStringPtr& key) const {
  absl::MutexLock lock(&index_mutex_);
  const KeyAttrValue* kav = schema_->FindKAV(key);
  return kav != nullptr && IsOccupied(kav->slots[pos_]);
}

bool Numeric::IsUnTracked(const InternedStringPtr& key) const {
  absl::MutexLock lock(&index_mutex_);
  const KeyAttrValue* kav = schema_->FindKAV(key);
  return kav != nullptr && !IsOccupied(kav->slots[pos_]);
}

void Numeric::UnTrack(const InternedStringPtr& key) {
  absl::MutexLock lock(&index_mutex_);
  const KeyAttrValue* kav = schema_->FindKAV(key);
  CHECK(kav != nullptr);
  CHECK(!IsOccupied(kav->slots[pos_]));
  if (!schema_->IsLinked(pos_, key)) {
    schema_->LinkMissing(pos_, key);
  }
}

absl::Status Numeric::ForEachTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn) const {
  // schema_->ForEachKey takes schema_mutex_ reader for the duration; the
  // index's own index_mutex_ isn't needed (the slot's occupied bit is KAV
  // state, not index-specific aux structure state).
  return schema_->ForEachKey(
      [&](const InternedStringPtr& key, const KeyAttrValue& kav) {
        if (IsOccupied(kav.slots[pos_])) {
          return fn(key);
        }
        return absl::OkStatus();
      });
}

absl::Status Numeric::ForEachUnTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn) const {
  absl::MutexLock lock(&index_mutex_);
  for (auto it = MissingListBegin(schema_, pos_); !it.Done(); it.Next()) {
    VMSDK_RETURN_IF_ERROR(fn(it.Key()));
  }
  return absl::OkStatus();
}

}  // namespace valkey_search::indexes
