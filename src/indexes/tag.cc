/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/indexes/tag.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/optimization.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/attribute_data_type.h"
#include "src/indexes/index_base.h"
#include "src/indexes/json_tag_decoder.h"
#include "src/indexes/text/rax/rax.h"
#include "src/query/predicate.h"
#include "src/utils/scanner.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/info.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

namespace {

// INFO counter for the non-UTF-8 TAG compatibility defect (see
// COMPATIBILITY.md). Statically constructed because AddRecord/ModifyRecord run
// on worker threads, where VALKEY_SEARCH_COMPATIBILITY_FIX's lazily
// constructed counter is unsafe.
vmsdk::info_field::Integer tag_rejects_non_utf8_compat_counter(
    "compatibility", "compatibility-tag_rejects_non_utf8",
    vmsdk::info_field::IntegerBuilder().App());

// INFO counter for the JSON TAG decoding compatibility defect (see
// COMPATIBILITY.md): JSON TAG values indexed differently than from 1.3.0.
vmsdk::info_field::Integer json_tag_values_compat_counter(
    "compatibility", "compatibility-json_tag_values",
    vmsdk::info_field::IntegerBuilder().App());

// Redisearch rejects non-UTF-8 TAG data. This is gated on
// search.emulate-release so the legacy behavior (index it) is preserved by
// default.
bool RejectNonUtf8(absl::string_view data) {
  if (ABSL_PREDICT_TRUE(utils::IsValidUtf8(data))) {
    return false;
  }
  if (options::EnabledInVersion(1, 3, 0)) {
    return true;
  }
  if (!options::compat_counters_suppressed) {
    tag_rejects_non_utf8_compat_counter.Increment();
  }
  return false;
}

inline uintptr_t SlotToStorage(void *p) {
  return reinterpret_cast<uintptr_t>(p);
}
inline void *StorageToSlot(uintptr_t s) { return reinterpret_cast<void *>(s); }

// rax free-callback — invoked once per surviving slot during raxFree.
// The slot's bits are the bag's storage; adopting them into a local bag and
// letting it destruct frees any heap payload it owns.
extern "C" void TagFreeCallback(void *p) {
  (void)BagOfInternedStringPtrs::Adopt(SlotToStorage(p));
}

// Mutation trampoline for raxMutate.
struct MutateCtx {
  const InternedStringPtr *key;
  bool insert;
};

extern "C" void *TagMutateTrampoline(void *current, void *ctx) {
  auto *a = static_cast<MutateCtx *>(ctx);
  auto bag = BagOfInternedStringPtrs::Adopt(SlotToStorage(current));
  if (a->insert) {
    bag.insert(*a->key);
  } else {
    bag.erase(*a->key);
  }
  // If the bag is empty, Release returns 0 and raxMutate then erases the rax
  // key. Otherwise the storage bits get planted back into the slot.
  return StorageToSlot(bag.Release());
}

// Below 1.3.0, a JSON TAG value is indexed as it was before arrays were
// supported.
bool DecodeJsonTagArrays() { return options::EnabledInVersion(1, 3, 0); }

template <typename Fn>
bool ForEachSplitTag(absl::string_view data, char separator, Fn &&fn) {
  for (absl::string_view part : absl::StrSplit(data, separator)) {
    auto tag = absl::StripAsciiWhitespace(part);
    if (!tag.empty() && fn(tag)) {
      return true;
    }
  }
  return false;
}

// Each tag is a varint length followed by its bytes.
void AppendPackedTag(std::string &packed, absl::string_view tag) {
  size_t n = tag.size();
  while (n >= 0x80) {
    packed.push_back(static_cast<char>((n & 0x7f) | 0x80));
    n >>= 7;
  }
  packed.push_back(static_cast<char>(n));
  packed.append(tag);
}

template <typename Fn>
void ForEachPackedTag(absl::string_view packed, Fn &&fn) {
  while (!packed.empty()) {
    size_t n = 0;
    for (int shift = 0;; shift += 7) {
      auto byte = static_cast<unsigned char>(packed.front());
      packed.remove_prefix(1);
      n |= static_cast<size_t>(byte & 0x7f) << shift;
      if ((byte & 0x80) == 0) {
        break;
      }
    }
    if (fn(packed.substr(0, n))) {
      return;
    }
    packed.remove_prefix(n);
  }
}

bool IsValidPrefix(absl::string_view str) {
  return str.length() < 2 || str[str.length() - 1] != '*' ||
         str[str.length() - 2] != '*';
}

}  // namespace

Tag::Tag(const data_model::TagIndex &tag_index_proto, bool json)
    : IndexBase(IndexerType::kTag),
      separator_(tag_index_proto.separator()[0]),
      explicit_separator_(
          tag_index_proto.has_explicit_separator()
              ? std::optional<bool>(tag_index_proto.explicit_separator())
              : std::nullopt),
      case_sensitive_(tag_index_proto.case_sensitive()),
      json_(json),
      tree_(raxNew()) {}

Tag::~Tag() { raxFreeWithCallback(tree_, &TagFreeCallback); }

std::string Tag::Normalize(absl::string_view tag) const {
  if (case_sensitive_) {
    return std::string(tag);
  }
  std::string out(tag);
  for (auto &c : out) {
    c = absl::ascii_tolower(static_cast<unsigned char>(c));
  }
  return out;
}

std::optional<char> Tag::JsonSeparator() const {
  if (explicit_separator_.has_value() && !*explicit_separator_) {
    return std::nullopt;
  }
  return separator_;
}

std::optional<char> Tag::EffectiveSeparator() const {
  return json_ && DecodeJsonTagArrays() ? JsonSeparator() : separator_;
}

std::optional<InternedStringPtr> Tag::InternRecord(
    absl::string_view data) const {
  if (RejectNonUtf8(data)) {
    return std::nullopt;
  }
  if (!json_) {
    return StringInternStore::Intern(data);
  }
  std::string packed;
  absl::flat_hash_set<absl::string_view> seen;
  auto pack = [&](absl::string_view tag) {
    if (seen.insert(tag).second) {
      AppendPackedTag(packed, tag);
    }
    return false;
  };
  auto decoded = DecodeJsonTags(data, JsonSeparator());
  if (DecodeJsonTagArrays()) {
    if (decoded.status == JsonTags::Status::kInvalid) {
      return std::nullopt;
    }
    for (const auto &tag : decoded.tags) {
      pack(tag);
    }
    return StringInternStore::Intern(packed);
  }
  vmsdk::UniqueValkeyString normalized;
  if (NormalizeJsonAttribute(data, normalized).ok()) {
    ForEachSplitTag(vmsdk::ToStringView(normalized.get()), separator_, pack);
  }
  if (!options::compat_counters_suppressed &&
      (decoded.status == JsonTags::Status::kInvalid ||
       absl::flat_hash_set<absl::string_view>(decoded.tags.begin(),
                                              decoded.tags.end()) != seen)) {
    json_tag_values_compat_counter.Increment();
  }
  return StringInternStore::Intern(packed);
}

template <typename Fn>
void Tag::ForEachStoredTag(absl::string_view stored, Fn &&fn) const {
  if (json_) {
    ForEachPackedTag(stored, fn);
  } else {
    ForEachSplitTag(stored, separator_, fn);
  }
}

absl::flat_hash_set<absl::string_view> Tag::StoredTags(
    absl::string_view stored) const {
  absl::flat_hash_set<absl::string_view> tags;
  ForEachStoredTag(stored, [&](absl::string_view tag) {
    tags.insert(tag);
    return false;
  });
  return tags;
}

std::optional<absl::flat_hash_set<absl::string_view>> Tag::ParseRecord(
    absl::string_view data, InternedStringPtr &storage) const {
  // AddRecord or ModifyRecord already counted this record.
  const bool was_suppressed =
      std::exchange(options::compat_counters_suppressed, true);
  absl::Cleanup restore = [was_suppressed] {
    options::compat_counters_suppressed = was_suppressed;
  };
  auto interned = InternRecord(data);
  if (!interned.has_value()) {
    return std::nullopt;
  }
  storage = *std::move(interned);
  return StoredTags(*storage);
}

void Tag::IndexTagForKey(absl::string_view tag, const InternedStringPtr &key) {
  std::string norm = Normalize(tag);
  MutateCtx ctx{&key, /*insert=*/true};
  raxMutate(tree_, reinterpret_cast<unsigned char *>(norm.data()), norm.size(),
            &TagMutateTrampoline, &ctx, ADD);
}

void Tag::DeindexTagForKey(absl::string_view tag,
                           const InternedStringPtr &key) {
  std::string norm = Normalize(tag);
  MutateCtx ctx{&key, /*insert=*/false};
  raxMutate(tree_, reinterpret_cast<unsigned char *>(norm.data()), norm.size(),
            &TagMutateTrampoline, &ctx, SUBTRACT);
}

absl::StatusOr<RecordResult> Tag::AddRecord(const InternedStringPtr &key,
                                            AttributeData &&data) {
  auto str = data.ConsumeString();
  auto interned_data = InternRecord(vmsdk::ToStringView(str.get()));
  if (!interned_data.has_value()) {
    absl::MutexLock lock(&index_mutex_);
    untracked_keys_.insert(key);
    return RecordResult::kInvalidData;
  }

  auto parsed_tags = StoredTags(**interned_data);
  absl::MutexLock lock(&index_mutex_);
  if (parsed_tags.empty()) {
    // An empty tag set is a missing value, not invalid data.
    untracked_keys_.insert(key);
    return RecordResult::kMissing;
  }
  auto [_, succ] = tracked_tags_by_keys_.insert(
      {key, TagInfo{.stored_tags = *std::move(interned_data)}});
  if (!succ) {
    return absl::AlreadyExistsError(
        absl::StrCat("Key `", key->Str(), "` already exists"));
  }
  untracked_keys_.erase(key);
  for (const auto &tag : parsed_tags) {
    IndexTagForKey(tag, key);
  }
  return RecordResult::kAdded;
}

std::string Tag::UnescapeTag(absl::string_view tag) {
  std::string result;
  result.reserve(tag.size());
  for (size_t i = 0; i < tag.size(); ++i) {
    if (tag[i] == '\\' && i + 1 < tag.size()) {
      // Escape sequence: consume next character literally
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

  // Helper: validate and insert a single tag (handles prefix wildcards)
  auto InsertTag = [&](absl::string_view raw) -> absl::Status {
    auto tag = absl::StripAsciiWhitespace(raw);
    if (tag.empty()) {
      return absl::OkStatus();  // Empty tags are silently ignored
    }
    if (tag.back() == '*') {
      if (!IsValidPrefix(tag)) {
        return absl::InvalidArgumentError(
            absl::StrCat("Tag string `", tag, "` ends with multiple *."));
      }
      const auto min_prefix_length =
          options::GetTagMinPrefixLength().GetValue();

      // Prefix tags shorter than min length are rejected.
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

  // Parse respecting escape sequences:
  //   - \<separator> is NOT a real separator
  //   - \\ is an escaped backslash (not an escape prefix)
  //   - Unescaped separator splits tags
  // Returns string_view positions; unescaping happens later at TagPredicate.
  size_t tag_start = 0;
  for (size_t i = 0; i < data.size(); ++i) {
    if (data[i] == '\\' && i + 1 < data.size()) {
      // Skip escaped character
      ++i;
    } else if (data[i] == separator) {
      // Unescaped separator: extract tag
      VMSDK_RETURN_IF_ERROR(InsertTag(data.substr(tag_start, i - tag_start)));
      tag_start = i + 1;
    }
  }
  // Handle the last tag (after final separator or if no separator)
  VMSDK_RETURN_IF_ERROR(InsertTag(data.substr(tag_start)));
  return parsed_tags;
}

absl::flat_hash_set<absl::string_view> Tag::ParseRecordTags(
    absl::string_view data, char separator) {
  absl::flat_hash_set<absl::string_view> parsed_tags;
  for (const auto &part : absl::StrSplit(data, separator)) {
    auto tag = absl::StripAsciiWhitespace(part);
    if (!tag.empty()) {
      parsed_tags.insert(tag);
    }
  }
  return parsed_tags;
}

absl::StatusOr<RecordResult> Tag::ModifyRecord(const InternedStringPtr &key,
                                               AttributeData &&data) {
  auto str = data.ConsumeString();
  auto interned_data = InternRecord(vmsdk::ToStringView(str.get()));
  if (!interned_data.has_value()) {
    [[maybe_unused]] auto res =
        RemoveRecord(key, indexes::DeletionType::kIdentifier);
    return RecordResult::kInvalidData;
  }

  auto new_parsed_tags = StoredTags(**interned_data);
  if (new_parsed_tags.empty()) {
    [[maybe_unused]] auto res =
        RemoveRecord(key, indexes::DeletionType::kIdentifier);
    return RecordResult::kMissing;
  }
  absl::MutexLock lock(&index_mutex_);

  auto it = tracked_tags_by_keys_.find(key);
  if (it == tracked_tags_by_keys_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Key `", key->Str(), "` not found"));
  }
  auto &tag_info = it->second;
  auto old_parsed_tags = StoredTags(*tag_info.stored_tags);

  // insert new tags that are not present in the old tags.
  for (const auto &tag : new_parsed_tags) {
    if (!old_parsed_tags.contains(tag)) {
      IndexTagForKey(tag, key);
    }
  }
  // remove old tags that are not present in the new tags.
  for (const auto &tag : old_parsed_tags) {
    if (!new_parsed_tags.contains(tag)) {
      DeindexTagForKey(tag, key);
    }
  }

  tag_info.stored_tags = *std::move(interned_data);
  return RecordResult::kAdded;
}

absl::StatusOr<bool> Tag::RemoveRecord(const InternedStringPtr &key,
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
  auto &tag_info = it->second;
  auto parsed_tags = StoredTags(*tag_info.stored_tags);
  for (const auto &tag : parsed_tags) {
    DeindexTagForKey(tag, key);
  }
  tracked_tags_by_keys_.erase(it);
  return true;
}

int Tag::RespondWithInfo(ValkeyModuleCtx *ctx) const {
  auto num_replies = 8;
  ValkeyModule_ReplyWithSimpleString(ctx, "type");
  ValkeyModule_ReplyWithSimpleString(ctx, "TAG");
  ValkeyModule_ReplyWithSimpleString(ctx, "SEPARATOR");
  auto separator = EffectiveSeparator();
  ValkeyModule_ReplyWithSimpleString(
      ctx, separator.has_value() ? std::string(1, *separator).c_str() : "");
  ValkeyModule_ReplyWithSimpleString(ctx, "CASESENSITIVE");
  ValkeyModule_ReplyWithSimpleString(ctx, case_sensitive_ ? "1" : "0");
  ValkeyModule_ReplyWithSimpleString(ctx, "size");
  absl::MutexLock lock(&index_mutex_);
  ValkeyModule_ReplyWithCString(
      ctx, std::to_string(tracked_tags_by_keys_.size()).c_str());
  return num_replies;
}

std::unique_ptr<data_model::Index> Tag::ToProto() const {
  auto index_proto = std::make_unique<data_model::Index>();
  auto tag_index = std::make_unique<data_model::TagIndex>();
  tag_index->set_separator(absl::string_view(&separator_, 1));
  if (explicit_separator_.has_value()) {
    tag_index->set_explicit_separator(*explicit_separator_);
  }
  tag_index->set_case_sensitive(case_sensitive_);
  index_proto->set_allocated_tag_index(tag_index.release());
  return index_proto;
}

uint32_t Tag::GetMutationWeight() const {
  return options::GetMutationWeightTag().GetValue();
}

InternedStringPtr Tag::GetRawValue(const InternedStringPtr &key) const {
  // Note that the Tag index is not mutated while the time sliced mutex is
  // in a read mode and therefore it is safe to skip lock acquiring.
  DCHECK(!json_) << "A JSON TAG index stores decoded tags";
  if (auto it = tracked_tags_by_keys_.find(key);
      it != tracked_tags_by_keys_.end()) {
    return it->second.stored_tags;
  }
  return {};
}

std::optional<absl::flat_hash_set<absl::string_view>> Tag::GetValue(
    const InternedStringPtr &key, bool &case_sensitive) const {
  // Note that the Tag index is not mutated while the time sliced mutex is
  // in a read mode and therefore it is safe to skip lock acquiring.
  if (auto it = tracked_tags_by_keys_.find(key);
      it != tracked_tags_by_keys_.end()) {
    case_sensitive = case_sensitive_;
    return StoredTags(*it->second.stored_tags);
  }
  return std::nullopt;
}

vmsdk::info_field::Integer tag_value_lookups(
    "tag", "tag_value_lookups", vmsdk::info_field::IntegerBuilder().Dev());

std::optional<BorrowedBagOfInternedStringPtrs> Tag::LookupValue(
    absl::string_view value) const {
  // Lock-free by the same read-side invariant GetValue relies on: the index is
  // not mutated while the time-sliced mutex is held in read mode.
  tag_value_lookups.Increment();
  std::string norm = Normalize(value);
  void *slot = nullptr;
  if (raxFind(tree_, reinterpret_cast<unsigned char *>(norm.data()),
              norm.size(), &slot) != 1) {
    return std::nullopt;
  }
  return BorrowedBagOfInternedStringPtrs(SlotToStorage(slot));
}

// -- Search / EntriesFetcher / EntriesFetcherIterator --------------------

Tag::EntriesFetcherIterator::EntriesFetcherIterator(
    const std::vector<void *> &slots,
    const std::vector<InternedStringPtr> &extras)
    : slots_(slots), extras_(extras) {
  AdvanceToNextNonEmpty();
}

Tag::EntriesFetcherIterator::~EntriesFetcherIterator() {
  // The bag's storage lives in the rax slot — Release so our local copy's
  // destructor doesn't free it.
  (void)bag_.Release();
}

bool Tag::EntriesFetcherIterator::Done() const {
  return slots_done_ && extras_idx_ >= extras_.size();
}

void Tag::EntriesFetcherIterator::Next() {
  if (!slots_done_) {
    ++bag_it_;
    if (bag_it_ != bag_end_) {
      current_ = *bag_it_;
      return;
    }
    (void)bag_.Release();
    ++slot_idx_;
    AdvanceToNextNonEmpty();
    return;
  }
  ++extras_idx_;
  if (extras_idx_ < extras_.size()) {
    current_ = extras_[extras_idx_];
  }
}

const InternedStringPtr &Tag::EntriesFetcherIterator::operator*() const {
  return current_;
}

void Tag::EntriesFetcherIterator::AdvanceToNextNonEmpty() {
  while (slot_idx_ < slots_.size()) {
    bag_ = BagOfInternedStringPtrs::Adopt(
        reinterpret_cast<uintptr_t>(slots_[slot_idx_]));
    bag_it_ = bag_.begin();
    bag_end_ = bag_.end();
    if (bag_it_ != bag_end_) {
      current_ = *bag_it_;
      return;
    }
    (void)bag_.Release();
    ++slot_idx_;
  }
  slots_done_ = true;
  if (extras_idx_ < extras_.size()) {
    current_ = extras_[extras_idx_];
  }
}

std::unique_ptr<EntriesFetcherIteratorBase> Tag::EntriesFetcher::Begin() {
  return std::make_unique<EntriesFetcherIterator>(matched_slots_, extras_);
}

// TODO: b/357027854 - Support Suffix/Infix Search
std::unique_ptr<EntriesFetcherBase> Tag::Search(
    const query::TagPredicate &predicate, bool negate) const {
  // Collect matched rax slots (each slot's 8 bytes encode a bag) without
  // iterating their postings; the iterator yields lazily during Begin().
  absl::flat_hash_set<void *> seen;
  std::vector<void *> matched_slots;
  size_t total = 0;

  auto collect_slot = [&](void *slot) {
    if (!slot || !seen.insert(slot).second) {
      return;
    }
    matched_slots.push_back(slot);
    auto bag = BagOfInternedStringPtrs::Adopt(SlotToStorage(slot));
    total += bag.size();
    (void)bag.Release();
  };

  for (absl::string_view tag : predicate.GetTags()) {
    const bool is_prefix = !tag.empty() && tag.back() == '*';
    absl::string_view q = is_prefix ? tag.substr(0, tag.size() - 1) : tag;
    std::string norm = Normalize(q);
    auto *qbytes =
        reinterpret_cast<unsigned char *>(const_cast<char *>(norm.data()));

    if (!is_prefix) {
      // exact search
      void *p = nullptr;
      if (raxFind(tree_, qbytes, norm.size(), &p) == 1) {
        collect_slot(p);
      }
    } else {
      raxIterator it;
      raxStart(&it, tree_);
      raxSeekSubTree(&it, qbytes, norm.size());
      while (raxNext(&it)) {
        collect_slot(it.data);
      }
      raxStop(&it);
    }
  }

  std::vector<InternedStringPtr> extras;
  size_t out_size = total;

  if (negate) {
    // Yield every posting NOT in `seen`, plus every untracked key.
    std::vector<void *> negate_slots;
    size_t negate_total = 0;
    raxIterator it;
    raxStart(&it, tree_);
    unsigned char empty = 0;
    raxSeekSubTree(&it, &empty, 0);
    while (raxNext(&it)) {
      if (it.data && !seen.contains(it.data)) {
        negate_slots.push_back(it.data);
        auto bag = BagOfInternedStringPtrs::Adopt(SlotToStorage(it.data));
        negate_total += bag.size();
        (void)bag.Release();
      }
    }
    raxStop(&it);
    extras.reserve(untracked_keys_.size());
    for (const auto &k : untracked_keys_) {
      extras.push_back(k);
    }
    out_size = negate_total + extras.size();
    return std::make_unique<EntriesFetcher>(std::move(negate_slots),
                                            std::move(extras), out_size);
  }

  return std::make_unique<EntriesFetcher>(std::move(matched_slots),
                                          std::move(extras), out_size);
}

size_t Tag::GetTrackedKeyCount() const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_tags_by_keys_.size();
}

size_t Tag::GetUnTrackedKeyCount() const {
  absl::MutexLock lock(&index_mutex_);
  return untracked_keys_.size();
}

size_t Tag::GetTagValueDocCount(absl::string_view value, bool lock) const {
  std::optional<absl::MutexLock> guard;
  if (lock) guard.emplace(&index_mutex_);
  auto bag = LookupValue(value);
  return bag ? bag->size() : 0;
}

size_t Tag::GetPrefixMatchDocCount(absl::string_view prefix_value,
                                   BorrowedInternedStringPtr key,
                                   bool lock) const {
  if (prefix_value.empty() || prefix_value.back() != '*') return 0;
  const absl::string_view prefix =
      prefix_value.substr(0, prefix_value.size() - 1);
  std::optional<absl::MutexLock> guard;
  if (lock) guard.emplace(&index_mutex_);

  // Scan the doc's own tags rather than the prefix's rax subtree: a doc carries
  // a handful of tags while a prefix can match an unbounded slice of the index.
  // Lock-free by the read-side invariant GetValue relies on.
  auto it = tracked_tags_by_keys_.find(key);
  if (it == tracked_tags_by_keys_.end()) return 0;

  // Stored tags are never empty, so a bare `*` query's empty prefix cannot
  // match one.
  size_t count = 0;
  ForEachStoredTag(it->second.stored_tags->Str(), [&](absl::string_view tag) {
    if (case_sensitive_ ? absl::StartsWith(tag, prefix)
                        : absl::StartsWithIgnoreCase(tag, prefix)) {
      auto bag = LookupValue(tag);
      count = bag ? bag->size() : 0;
      return true;
    }
    return false;
  });
  return count;
}

bool Tag::IsTracked(const InternedStringPtr &key) const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_tags_by_keys_.contains(key);
}

bool Tag::IsUnTracked(const InternedStringPtr &key) const {
  absl::MutexLock lock(&index_mutex_);
  return untracked_keys_.contains(key);
}

void Tag::UnTrack(const InternedStringPtr &key) {
  absl::MutexLock lock(&index_mutex_);
  CHECK(!tracked_tags_by_keys_.contains(key));
  untracked_keys_.insert(key);
}

absl::Status Tag::ForEachTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr &)> fn) const {
  absl::MutexLock lock(&index_mutex_);
  for (const auto &[key, _] : tracked_tags_by_keys_) {
    VMSDK_RETURN_IF_ERROR(fn(key));
  }
  return absl::OkStatus();
}

absl::Status Tag::ForEachUnTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr &)> fn) const {
  absl::MutexLock lock(&index_mutex_);
  for (const auto &key : untracked_keys_) {
    VMSDK_RETURN_IF_ERROR(fn(key));
  }
  return absl::OkStatus();
}

}  // namespace valkey_search::indexes
