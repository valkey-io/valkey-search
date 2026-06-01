/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/tag.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/indexes/key_attr_value.h"
#include "src/query/predicate.h"
#include "src/utils/patricia_tree.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

static bool IsValidPrefix(absl::string_view str) {
  return str.length() < 2 || str[str.length() - 1] != '*' ||
         str[str.length() - 2] != '*';
}

namespace {
// Slot accessors — caller responsible for locking discipline.
const TagSlot* TryGetSlot(const IndexSchema* schema, uint16_t pos,
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
  return std::launder(reinterpret_cast<const TagSlot*>(slot.storage));
}
}  // namespace

Tag::Tag(const data_model::TagIndex& tag_index_proto)
    : TypedIndex<Tag, TagSlot>(IndexerType::kTag),
      separator_(tag_index_proto.separator()[0]),
      case_sensitive_(tag_index_proto.case_sensitive()),
      tree_(case_sensitive_) {}

absl::StatusOr<bool> Tag::AddRecord(const InternedStringPtr& key,
                                    absl::string_view data) {
  auto interned_data = StringInternStore::Intern(data);
  auto parsed_tags = ParseRecordTags(*interned_data, separator_);
  absl::MutexLock lock(&index_mutex_);
  // Defensive: production callers (ProcessAttributeMutation) dispatch through
  // IsTracked → ModifyRecord; tests can hit AddRecord on an already-occupied
  // slot, so report AlreadyExists rather than CHECK-failing in OccupySlot.
  if (KeyAttrValue* kav = schema_->FindKAV(key);
      kav != nullptr && IsOccupied(kav->slots[pos_])) {
    return absl::AlreadyExistsError(
        absl::StrCat("Key already exists in tag index: ", key->Str()));
  }
  if (parsed_tags.empty()) {
    if (!schema_->IsLinked(pos_, key)) {
      schema_->LinkMissing(pos_, key);
    }
    return false;
  }
  // Heap-allocate the TagInfo; the slot owns it.
  auto* info = new TagInfo{.raw_tag_string = std::move(interned_data),
                           .tags = parsed_tags};
  std::byte* storage = OccupySlot(key, data.size());
  new (storage) TagSlot{
      {/*occupied=*/1u, /*user_data_len=*/static_cast<uint32_t>(data.size())},
      info};
  for (const auto& tag : parsed_tags) {
    tree_.AddKeyValue(tag, key);
  }
  return true;
}

std::string Tag::UnescapeTag(absl::string_view tag) {
  std::string result;
  result.reserve(tag.size());
  for (size_t i = 0; i < tag.size(); ++i) {
    if (tag[i] == '\\' && i + 1 < tag.size()) {
      result += tag[++i];
    } else {
      result += tag[i];
    }
  }
  return result;
}

absl::StatusOr<absl::flat_hash_set<absl::string_view>> Tag::ParseSearchTags(
    absl::string_view data, char separator) {
  absl::flat_hash_set<absl::string_view> parsed_tags;
  auto InsertTag = [&](absl::string_view raw) -> absl::Status {
    auto tag = absl::StripAsciiWhitespace(raw);
    if (tag.empty()) {
      return absl::OkStatus();
    }
    if (tag.back() == '*') {
      if (!IsValidPrefix(tag)) {
        return absl::InvalidArgumentError(
            absl::StrCat("Tag string `", tag, "` ends with multiple *."));
      }
      const auto min_prefix_length =
          options::GetTagMinPrefixLength().GetValue();
      if (tag.length() <= min_prefix_length) {
        return absl::InvalidArgumentError(absl::StrCat(
            "Tag string `", tag, "` is too short for prefix wildcard."));
      }
      parsed_tags.insert(tag);
    } else {
      parsed_tags.insert(tag);
    }
    return absl::OkStatus();
  };

  size_t tag_start = 0;
  for (size_t i = 0; i < data.size(); ++i) {
    if (data[i] == '\\' && i + 1 < data.size()) {
      ++i;
    } else if (data[i] == separator) {
      VMSDK_RETURN_IF_ERROR(InsertTag(data.substr(tag_start, i - tag_start)));
      tag_start = i + 1;
    }
  }
  VMSDK_RETURN_IF_ERROR(InsertTag(data.substr(tag_start)));
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
  auto interned_data = StringInternStore::Intern(data);
  auto new_parsed_tags = ParseRecordTags(*interned_data, separator_);
  if (new_parsed_tags.empty()) {
    [[maybe_unused]] auto res =
        RemoveRecord(key, indexes::DeletionType::kIdentifier);
    return false;
  }
  absl::MutexLock lock(&index_mutex_);
  KeyAttrValue* kav = schema_->FindKAV(key);
  if (kav == nullptr || !IsOccupied(kav->slots[pos_])) {
    return absl::NotFoundError(
        absl::StrCat("Key not tracked in tag index: ", key->Str()));
  }
  Slot& slot = kav->slots[pos_];
  TagSlot& tslot = SlotAs(slot.storage);
  TagInfo& tag_info = *tslot.info;

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
  if (tslot.user_data_len != data.size()) {
    ResizeSlot(key, data.size());
  }
  return true;
}

absl::StatusOr<bool> Tag::RemoveRecord(const InternedStringPtr& key,
                                       DeletionType deletion_type) {
  absl::MutexLock lock(&index_mutex_);
  KeyAttrValue* kav = schema_->FindKAV(key);
  if (kav == nullptr) {
    return false;
  }
  Slot& slot = kav->slots[pos_];
  if (IsOccupied(slot)) {
    TagSlot& tslot = SlotAs(slot.storage);
    TagInfo* info = tslot.info;
    for (const auto& tag : info->tags) {
      tree_.Remove(tag, key);
    }
    delete info;
    VacateSlot(key, /*relink=*/deletion_type != DeletionType::kRecord);
    return true;
  }
  // Empty slot — link or unlink as appropriate.
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

void Tag::DestructTyped(const InternedStringPtr& key, TagSlot& slot) {
  // Safety-net path (DestroyKeyAttrValue found an occupied slot). Remove
  // this key's tags from the patricia tree, then free the TagInfo*.
  absl::MutexLock lock(&index_mutex_);
  TagInfo* info = slot.info;
  for (const auto& tag : info->tags) {
    tree_.Remove(tag, key);
  }
  delete info;
}

int Tag::RespondWithInfo(ValkeyModuleCtx* ctx) const {
  auto num_replies = 8;
  ValkeyModule_ReplyWithSimpleString(ctx, "type");
  ValkeyModule_ReplyWithSimpleString(ctx, "TAG");
  ValkeyModule_ReplyWithSimpleString(ctx, "SEPARATOR");
  ValkeyModule_ReplyWithSimpleString(
      ctx, std::string(&separator_, sizeof(char)).c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "CASESENSITIVE");
  ValkeyModule_ReplyWithSimpleString(ctx, case_sensitive_ ? "1" : "0");
  ValkeyModule_ReplyWithSimpleString(ctx, "size");
  absl::MutexLock lock(&index_mutex_);
  ValkeyModule_ReplyWithCString(
      ctx, std::to_string(schema_->OccupiedCount(pos_)).c_str());
  return num_replies;
}

std::unique_ptr<data_model::Index> Tag::ToProto() const {
  auto index_proto = std::make_unique<data_model::Index>();
  auto tag_index = std::make_unique<data_model::TagIndex>();
  tag_index->set_separator(absl::string_view(&separator_, 1));
  tag_index->set_case_sensitive(case_sensitive_);
  index_proto->set_allocated_tag_index(tag_index.release());
  return index_proto;
}

uint32_t Tag::GetMutationWeight() const {
  return options::GetMutationWeightTag().GetValue();
}

InternedStringPtr Tag::GetRawValue(const InternedStringPtr& key) const {
  const TagSlot* tslot = TryGetSlot(schema_, pos_, key);
  return tslot == nullptr ? InternedStringPtr{} : tslot->info->raw_tag_string;
}

const absl::flat_hash_set<absl::string_view>* Tag::GetValue(
    const InternedStringPtr& key, bool& case_sensitive) const {
  const TagSlot* tslot = TryGetSlot(schema_, pos_, key);
  if (tslot == nullptr) {
    return nullptr;
  }
  case_sensitive = case_sensitive_;
  return &tslot->info->tags;
}

Tag::EntriesFetcherIterator::EntriesFetcherIterator(
    const PatriciaTreeIndex& tree,
    absl::flat_hash_set<PatriciaNodeIndex*>& entries,
    const IndexSchema* schema_for_missing, uint16_t pos, bool negate)
    : tree_(tree),
      entries_(entries),
      schema_for_missing_(schema_for_missing),
      pos_(pos),
      negate_(negate) {}

Tag::EntriesFetcherIterator::~EntriesFetcherIterator() = default;

void Tag::EntriesFetcherIterator::EnsureNegateRootIter() {
  if (!negate_root_iter_.has_value()) {
    negate_root_iter_.emplace(tree_.RootIterator());
  }
}

bool Tag::EntriesFetcherIterator::Done() const {
  if (negate_) {
    const bool tree_done =
        negate_root_iter_.has_value() && negate_root_iter_->Done();
    if (!tree_done) {
      return false;
    }
    if (schema_for_missing_ == nullptr) {
      return true;
    }
    return missing_started_ &&
           (missing_iter_ == nullptr || missing_iter_->Done());
  }
  return entries_.empty() && next_node_ == nullptr;
}

void Tag::EntriesFetcherIterator::NextNegate() {
  EnsureNegateRootIter();
  if (next_node_) {
    ++next_iter_;
    if (next_iter_ != next_node_->value.value().end()) {
      return;
    }
    negate_root_iter_->Next();
  }
  while (!negate_root_iter_->Done()) {
    next_node_ = negate_root_iter_->Value();
    if (next_node_ && !entries_.contains(next_node_) &&
        next_node_->value.has_value() && !next_node_->value.value().empty()) {
      next_iter_ = next_node_->value.value().begin();
      return;
    }
    negate_root_iter_->Next();
  }
  next_node_ = nullptr;
  if (schema_for_missing_ == nullptr) {
    return;
  }
  if (!missing_started_) {
    missing_iter_ = std::make_unique<MissingListIterator>(
        MissingListBegin(schema_for_missing_, pos_));
    missing_started_ = true;
  } else if (missing_iter_ != nullptr && !missing_iter_->Done()) {
    missing_iter_->Next();
  }
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
  if (negate_ && negate_root_iter_.has_value() && negate_root_iter_->Done()) {
    DCHECK(missing_iter_ != nullptr && !missing_iter_->Done());
    return missing_iter_->Key();
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
          if (res.second && node->value.has_value()) {
            size += node->value.value().size();
          }
        }
      }
    } else {
      PatriciaNodeIndex* node = tree_.ExactMatcher(tag);
      if (node != nullptr) {
        auto res = entries.insert(node);
        if (res.second && node->value.has_value()) {
          size += node->value.value().size();
        }
      }
    }
  }
  if (negate) {
    const size_t tracked = schema_->OccupiedCount(pos_);
    size = tracked > size ? tracked - size : tracked;
    size += schema_->MissingListAt(pos_).size;
  }
  return std::make_unique<Tag::EntriesFetcher>(tree_, entries, size, negate,
                                                schema_, pos_);
}

std::unique_ptr<EntriesFetcherIteratorBase> Tag::EntriesFetcher::Begin() {
  auto itr = std::make_unique<EntriesFetcherIterator>(
      tree_, entries_, schema_for_missing_, pos_, negate_);
  itr->Next();
  return itr;
}

size_t Tag::EntriesFetcher::Size() const { return size_; }

size_t Tag::GetTrackedKeyCount() const {
  absl::MutexLock lock(&index_mutex_);
  return schema_->OccupiedCount(pos_);
}

size_t Tag::GetUnTrackedKeyCount() const {
  absl::MutexLock lock(&index_mutex_);
  return schema_->MissingListAt(pos_).size;
}

bool Tag::IsTracked(const InternedStringPtr& key) const {
  absl::MutexLock lock(&index_mutex_);
  const KeyAttrValue* kav = schema_->FindKAV(key);
  return kav != nullptr && IsOccupied(kav->slots[pos_]);
}

bool Tag::IsUnTracked(const InternedStringPtr& key) const {
  absl::MutexLock lock(&index_mutex_);
  const KeyAttrValue* kav = schema_->FindKAV(key);
  return kav != nullptr && !IsOccupied(kav->slots[pos_]);
}

void Tag::UnTrack(const InternedStringPtr& key) {
  absl::MutexLock lock(&index_mutex_);
  const KeyAttrValue* kav = schema_->FindKAV(key);
  CHECK(kav != nullptr);
  CHECK(!IsOccupied(kav->slots[pos_]));
  if (!schema_->IsLinked(pos_, key)) {
    schema_->LinkMissing(pos_, key);
  }
}

absl::Status Tag::ForEachTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn) const {
  return schema_->ForEachKey(
      [&](const InternedStringPtr& key, const KeyAttrValue& kav) {
        if (IsOccupied(kav.slots[pos_])) {
          return fn(key);
        }
        return absl::OkStatus();
      });
}

absl::Status Tag::ForEachUnTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn) const {
  absl::MutexLock lock(&index_mutex_);
  for (auto it = MissingListBegin(schema_, pos_); !it.Done(); it.Next()) {
    VMSDK_RETURN_IF_ERROR(fn(it.Key()));
  }
  return absl::OkStatus();
}

}  // namespace valkey_search::indexes
