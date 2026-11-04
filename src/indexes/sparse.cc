/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */
#include "src/indexes/sparse.h"

#include <algorithm>
#include <bit>
#include <limits>
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
  auto [it, inserted] = records_.emplace(key, std::move(*entries));
  if (!inserted) {
    return absl::AlreadyExistsError("Sparse record already exists");
  }
  for (const auto &entry : it->second) {
    postings_[entry.dimension].emplace(key, entry.weight);
  }
  untracked_.erase(key);
  return RecordResult::kAdded;
}

void Sparse::ErasePostings(const InternedStringPtr &key,
                           const std::vector<Entry> &entries) {
  for (const auto &entry : entries) {
    auto it = postings_.find(entry.dimension);
    it->second.erase(key);
    if (it->second.empty()) {
      postings_.erase(it);
    }
  }
}

absl::StatusOr<RecordResult> Sparse::ModifyRecord(const InternedStringPtr &key,
                                                  AttributeData &&data) {
  auto entries = Decode(data.GetStringView());
  absl::MutexLock lock(&mutex_);
  auto it = records_.find(key);
  if (it == records_.end()) {
    return absl::NotFoundError("Sparse record not found");
  }
  ErasePostings(key, it->second);
  if (!entries.ok() || entries->empty()) {
    records_.erase(it);
    untracked_.insert(key);
    return entries.ok() ? RecordResult::kMissing : RecordResult::kInvalidData;
  }
  it->second = std::move(*entries);
  for (const auto &entry : it->second) {
    postings_[entry.dimension].emplace(key, entry.weight);
  }
  return RecordResult::kAdded;
}

absl::StatusOr<bool> Sparse::RemoveRecord(const InternedStringPtr &key,
                                          DeletionType deletion_type) {
  absl::MutexLock lock(&mutex_);
  if (deletion_type == DeletionType::kRecord) {
    untracked_.erase(key);
  } else {
    untracked_.insert(key);
  }
  auto it = records_.find(key);
  if (it == records_.end()) {
    return false;
  }
  ErasePostings(key, it->second);
  records_.erase(it);
  return true;
}

absl::StatusOr<std::vector<Neighbor>> Sparse::Search(
    absl::string_view query, size_t count, cancel::Token &cancellation,
    absl::AnyInvocable<bool(const InternedStringPtr &)> filter) const
    ABSL_NO_THREAD_SAFETY_ANALYSIS {
  VMSDK_ASSIGN_OR_RETURN(auto entries, Decode(query));
  InternedStringHashMap<double> scores;
  size_t visited = 0;
  for (const auto &entry : entries) {
    auto posting = postings_.find(entry.dimension);
    if (posting == postings_.end()) {
      continue;
    }
    for (const auto &[key, weight] : posting->second) {
      if ((visited++ & 255) == 0 && cancellation->IsCancelled()) {
        return absl::DeadlineExceededError("Sparse search cancelled");
      }
      scores[key] += double(weight) * double(entry.weight);
    }
  }
  struct Hit {
    InternedStringPtr key;
    float distance;
  };
  auto better = [](const Hit &a, const Hit &b) {
    return a.distance != b.distance ? a.distance < b.distance
                                    : a.key->Str() < b.key->Str();
  };
  std::priority_queue<Hit, std::vector<Hit>, decltype(better)> heap(better);
  for (const auto &[key, score] : scores) {
    if ((visited++ & 255) == 0 && cancellation->IsCancelled()) {
      return absl::DeadlineExceededError("Sparse search cancelled");
    }
    if (count == 0 || !filter(key)) {
      continue;
    }
    VMSDK_ASSIGN_OR_RETURN(auto distance, Distance(score));
    Hit hit{key, distance};
    if (heap.size() < count) {
      heap.push(std::move(hit));
    } else if (better(hit, heap.top())) {
      heap.pop();
      heap.push(std::move(hit));
    }
  }
  std::vector<Neighbor> result;
  result.reserve(heap.size());
  while (!heap.empty()) {
    result.emplace_back(heap.top().key, heap.top().distance);
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
  return records_.size();
}
size_t Sparse::GetUnTrackedKeyCount() const {
  absl::MutexLock lock(&mutex_);
  return untracked_.size();
}
bool Sparse::IsTracked(const InternedStringPtr &key) const {
  absl::MutexLock lock(&mutex_);
  return records_.contains(key);
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
  for (const auto &[key, entries] : records_) {
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
