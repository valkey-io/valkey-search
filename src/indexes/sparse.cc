/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#include "src/indexes/sparse.h"

#include <algorithm>
#include <bit>
#include <limits>
#include <optional>
#include <queue>

#include "src/attribute_data.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/status/status_macros.h"

namespace valkey_search::indexes {
namespace {
uint32_t ReadLE32(const char *p) {
  const auto *b = reinterpret_cast<const unsigned char *>(p);
  return uint32_t(b[0]) | (uint32_t(b[1]) << 8) | (uint32_t(b[2]) << 16) |
         (uint32_t(b[3]) << 24);
}

absl::StatusOr<float> Distance(double dot) {
  if (dot > std::numeric_limits<float>::max() ||
      dot < -std::numeric_limits<float>::max()) {
    return absl::InvalidArgumentError("Sparse dot product exceeds FLOAT32");
  }
  return -static_cast<float>(dot);
}

// Both inputs are ordered by dimension; preserve the unfiltered summation
// order.
std::optional<double> Dot(const std::vector<Sparse::Entry> &record,
                          const std::vector<Sparse::Entry> &query) {
  size_t i = 0, j = 0;
  double sum = 0;
  bool overlap = false;
  while (i < record.size() && j < query.size()) {
    if (record[i].dimension < query[j].dimension) {
      ++i;
    } else if (record[i].dimension > query[j].dimension) {
      ++j;
    } else {
      sum += double(record[i++].weight) * double(query[j++].weight);
      overlap = true;
    }
  }
  return overlap ? std::optional<double>(sum) : std::nullopt;
}
}  // namespace

absl::StatusOr<std::vector<Sparse::Entry>> Sparse::Decode(
    absl::string_view blob) {
  if (blob.size() % 8 != 0) {
    return absl::InvalidArgumentError("SPARSE requires uint32/FLOAT32 pairs");
  }
  std::vector<Entry> entries;
  entries.reserve(blob.size() / 8);
  uint32_t previous = 0;
  for (size_t offset = 0; offset < blob.size(); offset += 8) {
    uint32_t dimension = ReadLE32(blob.data() + offset);
    uint32_t bits = ReadLE32(blob.data() + offset + 4);
    // Bit validation remains correct when the module uses -ffast-math.
    if ((bits & 0x7f800000u) == 0x7f800000u ||
        (offset != 0 && dimension <= previous)) {
      return absl::InvalidArgumentError(
          "SPARSE requires finite weights and strictly increasing dimensions");
    }
    previous = dimension;
    float weight = std::bit_cast<float>(bits);
    if (weight != 0) {
      entries.push_back({dimension, weight});
    }
  }
  return entries;
}

absl::StatusOr<float> Sparse::RecomputeDistance(absl::string_view record,
                                                absl::string_view query) {
  VMSDK_ASSIGN_OR_RETURN(auto a, Decode(record));
  VMSDK_ASSIGN_OR_RETURN(auto b, Decode(query));
  size_t i = 0, j = 0;
  double dot = 0;
  bool overlap = false;
  while (i < a.size() && j < b.size()) {
    if (a[i].dimension < b[j].dimension) {
      ++i;
    } else if (b[j].dimension < a[i].dimension) {
      ++j;
    } else {
      dot += double(a[i++].weight) * double(b[j++].weight);
      overlap = true;
    }
  }
  if (!overlap) {
    return absl::NotFoundError("No sparse overlap");
  }
  return Distance(dot);
}

absl::StatusOr<RecordResult> Sparse::AddRecord(const InternedStringPtr &key,
                                               AttributeData &&data) {
  auto entries = Decode(data.GetStringView());
  absl::MutexLock lock(&mutex_);
  if (!entries.ok() || entries->empty()) {
    untracked_.insert(key);
    return entries.ok() ? RecordResult::kMissing : RecordResult::kInvalidData;
  }
  if (ids_.contains(key)) {
    return absl::AlreadyExistsError("Sparse record already exists");
  }
  DocId id;
  if (free_ids_.empty()) {
    if (records_.size() >= std::numeric_limits<DocId>::max()) {
      return absl::ResourceExhaustedError("Sparse document ID space exhausted");
    }
    id = static_cast<DocId>(records_.size());
    records_.emplace_back();
  } else {
    id = free_ids_.back();
    free_ids_.pop_back();
  }
  records_[id] = {key, std::move(*entries)};
  ids_.emplace(key, id);
  AddPostings(id);
  untracked_.erase(key);
  return RecordResult::kAdded;
}

void Sparse::AddPostings(DocId id) {
  for (auto &entry : records_[id].entries) {
    auto &posting = postings_[entry.dimension];
    entry.posting_offset = static_cast<uint32_t>(posting.size());
    posting.push_back({id, entry.weight});
  }
}

void Sparse::ErasePostings(DocId id) {
  for (const auto &entry : records_[id].entries) {
    auto it = postings_.find(entry.dimension);
    auto &posting = it->second;
    const Posting moved = posting.back();
    posting[entry.posting_offset] = moved;
    posting.pop_back();
    if (moved.id != id) {
      // Each record has one forward entry per dimension. Repair the swapped
      // posting's back-reference so a later replacement/removal stays O(nnz).
      auto &forward = records_[moved.id].entries;
      auto moved_entry =
          std::lower_bound(forward.begin(), forward.end(), entry.dimension,
                           [](const Entry &a, uint32_t dimension) {
                             return a.dimension < dimension;
                           });
      moved_entry->posting_offset = entry.posting_offset;
    }
    if (posting.empty()) postings_.erase(it);
  }
}

absl::StatusOr<RecordResult> Sparse::ModifyRecord(const InternedStringPtr &key,
                                                  AttributeData &&data) {
  auto entries = Decode(data.GetStringView());
  absl::MutexLock lock(&mutex_);
  auto it = ids_.find(key);
  if (it == ids_.end()) return absl::NotFoundError("Sparse record not found");
  const DocId id = it->second;
  ErasePostings(id);
  if (!entries.ok() || entries->empty()) {
    ids_.erase(it);
    records_[id] = {};
    free_ids_.push_back(id);
    untracked_.insert(key);
    return entries.ok() ? RecordResult::kMissing : RecordResult::kInvalidData;
  }
  records_[id].entries = std::move(*entries);
  AddPostings(id);
  return RecordResult::kAdded;
}

absl::StatusOr<bool> Sparse::RemoveRecord(const InternedStringPtr &key,
                                          DeletionType deletion_type) {
  absl::MutexLock lock(&mutex_);
  if (deletion_type == DeletionType::kRecord)
    untracked_.erase(key);
  else
    untracked_.insert(key);
  auto it = ids_.find(key);
  if (it == ids_.end()) return false;
  const DocId id = it->second;
  ErasePostings(id);
  records_[id] = {};
  ids_.erase(it);
  free_ids_.push_back(id);
  return true;
}

absl::StatusOr<std::vector<Neighbor>> Sparse::Search(
    absl::string_view query, size_t count, cancel::Token &cancellation,
    absl::AnyInvocable<bool(const InternedStringPtr &)> filter,
    const std::vector<InternedStringPtr> *candidates) const
    ABSL_NO_THREAD_SAFETY_ANALYSIS {
  VMSDK_ASSIGN_OR_RETURN(auto entries, Decode(query));
  struct Hit {
    DocId id;
    float distance;
  };
  auto better = [this](const Hit &a,
                       const Hit &b) ABSL_NO_THREAD_SAFETY_ANALYSIS {
    return a.distance != b.distance
               ? a.distance < b.distance
               : records_[a.id].key->Str() < records_[b.id].key->Str();
  };
  std::priority_queue<Hit, std::vector<Hit>, decltype(better)> heap(better);
  auto add = [&](DocId id, double score)
                 ABSL_NO_THREAD_SAFETY_ANALYSIS -> absl::Status {
    if (count == 0 || !filter(records_[id].key)) return absl::OkStatus();
    VMSDK_ASSIGN_OR_RETURN(auto distance, Distance(score));
    Hit hit{id, distance};
    if (heap.size() < count)
      heap.push(hit);
    else if (better(hit, heap.top())) {
      heap.pop();
      heap.push(hit);
    }
    return absl::OkStatus();
  };
  size_t visited = 0;
  if (candidates) {
    for (const auto &key : *candidates) {
      if ((visited++ & 255) == 0 && cancellation->IsCancelled()) {
        return absl::DeadlineExceededError("Sparse search cancelled");
      }
      auto it = ids_.find(key);
      if (it == ids_.end()) continue;
      auto score = Dot(records_[it->second].entries, entries);
      if (score) VMSDK_RETURN_IF_ERROR(add(it->second, *score));
    }
  } else {
    // Query-local arrays: independent concurrent readers, no shared scratch or
    // retained state. Marks preserve overlapping documents whose sum is zero.
    std::vector<double> scores(records_.size(), 0);
    std::vector<uint8_t> seen(records_.size(), 0);
    std::vector<DocId> touched;
    for (const auto &entry : entries) {
      auto posting = postings_.find(entry.dimension);
      if (posting == postings_.end()) continue;
      for (const auto &[id, weight] : posting->second) {
        if ((visited++ & 255) == 0 && cancellation->IsCancelled()) {
          return absl::DeadlineExceededError("Sparse search cancelled");
        }
        if (!seen[id]) {
          seen[id] = 1;
          touched.push_back(id);
        }
        scores[id] += double(weight) * double(entry.weight);
      }
    }
    for (DocId id : touched) {
      if ((visited++ & 255) == 0 && cancellation->IsCancelled()) {
        return absl::DeadlineExceededError("Sparse search cancelled");
      }
      VMSDK_RETURN_IF_ERROR(add(id, scores[id]));
    }
  }
  std::vector<Neighbor> result;
  result.reserve(heap.size());
  while (!heap.empty()) {
    result.emplace_back(records_[heap.top().id].key, heap.top().distance);
    heap.pop();
  }
  std::reverse(result.begin(), result.end());
  return result;
}

int Sparse::RespondWithInfo(ValkeyModuleCtx *ctx) const {
  ValkeyModule_ReplyWithSimpleString(ctx, "type");
  ValkeyModule_ReplyWithSimpleString(ctx, "SPARSE");
  return 2;
}

std::unique_ptr<data_model::Index> Sparse::ToProto() const {
  auto proto = std::make_unique<data_model::Index>();
  proto->mutable_sparse_index();
  return proto;
}

uint32_t Sparse::GetMutationWeight() const {
  return options::GetMutationWeightTag().GetValue();
}

size_t Sparse::GetTrackedKeyCount() const {
  absl::MutexLock lock(&mutex_);
  return ids_.size();
}
size_t Sparse::GetUnTrackedKeyCount() const {
  absl::MutexLock lock(&mutex_);
  return untracked_.size();
}
bool Sparse::IsTracked(const InternedStringPtr &key) const {
  absl::MutexLock lock(&mutex_);
  return ids_.contains(key);
}
bool Sparse::IsUnTracked(const InternedStringPtr &key) const {
  absl::MutexLock lock(&mutex_);
  return untracked_.contains(key);
}
void Sparse::UnTrack(const InternedStringPtr &key) {
  absl::MutexLock lock(&mutex_);
  untracked_.erase(key);
}
absl::Status Sparse::ForEachTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr &)> fn) const {
  absl::MutexLock lock(&mutex_);
  for (const auto &[key, id] : ids_) {
    VMSDK_RETURN_IF_ERROR(fn(key));
  }
  return absl::OkStatus();
}
absl::Status Sparse::ForEachUnTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr &)> fn) const {
  absl::MutexLock lock(&mutex_);
  for (const auto &key : untracked_) {
    VMSDK_RETURN_IF_ERROR(fn(key));
  }
  return absl::OkStatus();
}
}  // namespace valkey_search::indexes
