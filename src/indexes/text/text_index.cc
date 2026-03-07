/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/text_index.h"

#include <array>
#include <cstring>
#include <numeric>

#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/synchronization/mutex.h"
#include "libstemmer.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/memory_allocation.h"
namespace valkey_search::indexes::text {

namespace {

// InvasivePtrRaw<Postings> deletion
static void FreePostingsCallback(void *target) {
  if (target) {
    auto raw = static_cast<InvasivePtrRaw<Postings>>(target);
    InvasivePtr<Postings>::AdoptRaw(raw);
  }
}

static void FreeStemParentsCallback(void *target) {
  if (target) {
    auto raw = static_cast<InvasivePtrRaw<StemParents>>(target);
    InvasivePtr<StemParents>::AdoptRaw(raw);
  }
}

InvasivePtr<Postings> AddKeyToPostings(InvasivePtr<Postings> existing_postings,
                                       const InternedStringPtr &key,
                                       FlatPositionMap *flat_map,
                                       TextIndexMetadata *metadata) {
  InvasivePtr<Postings> postings;
  if (existing_postings) {
    postings = existing_postings;
  } else {
    metadata->num_unique_terms++;
    postings = InvasivePtr<Postings>::Make();
  }

  postings->InsertKey(key, flat_map);
  return postings;
}

InvasivePtr<Postings> RemoveKeyFromPostings(
    InvasivePtr<Postings> existing_postings, const InternedStringPtr &key,
    TextIndexMetadata *metadata) {
  CHECK(existing_postings) << "Per-key tree became unaligned";

  existing_postings->RemoveKey(key, metadata);

  if (existing_postings->IsEmpty()) {
    metadata->num_unique_terms--;
    existing_postings.Clear();
  }
  return existing_postings;
}

// Factory for target set callback
template <typename Target>
std::function<void *(void *)> CreateTargetSetFn(
    const InvasivePtr<Target> &updated_target) {
  return [&updated_target](void *old_val) -> void * {
    if (old_val) {
      InvasivePtr<Target>::AdoptRaw(
          static_cast<InvasivePtrRaw<Target>>(old_val));
    }
    if (!updated_target) return nullptr;
    InvasivePtr<Target> copy = updated_target;
    return static_cast<void *>(std::move(copy).ReleaseRaw());
  };
}

// Factory for simple target mutation
template <typename Target, typename MutateFn>
std::function<void *(void *)> CreateSimpleTargetMutateFn(MutateFn mutate_fn) {
  return [mutate_fn = std::move(mutate_fn)](void *old_val) -> void * {
    // Take ownership of any existing target
    auto existing = InvasivePtr<Target>::AdoptRaw(
        static_cast<InvasivePtrRaw<Target>>(old_val));

    // Mutate the target
    InvasivePtr<Target> new_target = mutate_fn(std::move(existing));

    // Pass ownership of the new target to the tree
    return static_cast<void *>(std::move(new_target).ReleaseRaw());
  };
}

}  // namespace

/*** TextIndex ***/

TextIndex::TextIndex(bool suffix)
    : prefix_tree_(FreePostingsCallback),
      suffix_tree_(suffix ? std::make_unique<Rax>(FreePostingsCallback)
                          : nullptr) {}

void TextIndex::MutateTarget(absl::string_view word,
                             const InvasivePtr<Postings> &target,
                             item_count_op op) {
  auto target_set_fn = CreateTargetSetFn(target);
  prefix_tree_.MutateTarget(word, target_set_fn, op);
  if (suffix_tree_) {
    std::string rev(word.rbegin(), word.rend());
    suffix_tree_->MutateTarget(rev, target_set_fn, op);
  }
}

Rax &TextIndex::GetPrefix() { return prefix_tree_; }

const Rax &TextIndex::GetPrefix() const { return prefix_tree_; }

std::optional<std::reference_wrapper<Rax>> TextIndex::GetSuffix() {
  if (!suffix_tree_) {
    return std::nullopt;
  }
  return std::ref(*suffix_tree_);
}

std::optional<std::reference_wrapper<const Rax>> TextIndex::GetSuffix() const {
  if (!suffix_tree_) {
    return std::nullopt;
  }
  return std::ref(*suffix_tree_);
}

/*** TextIndexSchema ***/

#define ITEM_COUNT_TRACKING_ENABLED(op) \
  (track_subtree_item_counts_ ? op : item_count_op::NONE)

TextIndexSchema::TextIndexSchema(data_model::Language language,
                                 const std::string &punctuation,
                                 bool with_offsets,
                                 const std::vector<std::string> &stop_words,
                                 uint32_t min_stem_size)
    : with_offsets_(with_offsets),
      lexer_(language, punctuation, stop_words),
      stem_tree_(FreeStemParentsCallback),
      min_stem_size_(min_stem_size),
      rax_target_mutex_pool_(options::GetRaxTargetMutexPoolSize().GetValue()) {}

absl::StatusOr<bool> TextIndexSchema::StageAttributeData(
    const InternedStringPtr &key, absl::string_view data,
    size_t text_field_number, bool stem, bool suffix) {
  absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>>
      *stem_mappings_ptr = nullptr;
  if (stem) {
    std::lock_guard<std::mutex> stem_guard(in_progress_stem_mappings_mutex_);
    stem_mappings_ptr = &in_progress_stem_mappings_[key];
  }
  // Tokenize and collect stem mappings
  auto tokens_res =
      lexer_.Tokenize(data, stem, min_stem_size_, stem_mappings_ptr);
  if (!tokens_res.ok()) {
    if (tokens_res.status().code() == absl::StatusCode::kInvalidArgument) {
      return false;
    }
    return tokens_res.status();
  }
  const auto &tokens = tokens_res.value();
  if (tokens.empty()) return true;

  const size_t num_tokens = tokens.size();
  constexpr size_t kNumBuckets = 128;
  constexpr size_t kMask = kNumBuckets - 1;

  // 1. PRE-CALCULATE COUNTS: Avoid vector resizing entirely
  std::array<uint32_t, kNumBuckets> bucket_counts = {0};
  std::vector<size_t> hashes(num_tokens);
  for (size_t i = 0; i < num_tokens; ++i) {
    hashes[i] = absl::Hash<absl::string_view>{}(tokens[i]);
    bucket_counts[hashes[i] & kMask]++;
  }

  // 2. FLAT ALLOCATION: One single allocation for ALL bucket data
  // This drastically reduces Page Faults compared to vector<vector>
  std::vector<uint32_t> all_bucket_data(num_tokens);
  std::array<size_t, kNumBuckets> bucket_offsets;
  size_t current_offset = 0;
  for (int i = 0; i < kNumBuckets; ++i) {
    bucket_offsets[i] = current_offset;
    current_offset += bucket_counts[i];
  }

  // 3. DISTRIBUTE: Fill the flat buffer
  auto current_ptrs = bucket_offsets; // Copy for tracking
  for (uint32_t i = 0; i < num_tokens; ++i) {
    all_bucket_data[current_ptrs[hashes[i] & kMask]++] = i;
  }

  // Map tokens -> positions -> field-masks
  TokenPositions *token_positions;
  {
    std::lock_guard<std::mutex> guard(in_progress_key_updates_mutex_);
    token_positions = &in_progress_key_updates_[key];
  }

  // 4. PROCESS: Reuse the map to avoid 128 allocations
  absl::flat_hash_map<absl::string_view, std::vector<uint32_t>> local_groups;
  
  for (size_t b = 0; b < kNumBuckets; ++b) {
    size_t start = bucket_offsets[b];
    size_t end = current_ptrs[b]; // Use the pointers from the distribution step
    if (start == end) continue;

    local_groups.clear(); // Reuses memory! No malloc/free here.
    
    for (size_t i = start; i < end; ++i) {
      uint32_t pos_idx = all_bucket_data[i];
      local_groups[tokens[pos_idx]].push_back(pos_idx);
    }

    for (auto& [token_view, positions] : local_groups) {
      auto it = token_positions->find(token_view);
      if (it == token_positions->end()) {
        it = token_positions->emplace(std::string(token_view), std::make_pair(PositionMap{}, false)).first;
      }
      
      auto &[pos_map, suffix_eligible] = it->second;
      if (suffix) suffix_eligible = true;
      
      for (uint32_t pos_idx : positions) {
        uint32_t position = with_offsets_ ? pos_idx : pos_idx;
        auto [pos_it, _] = pos_map.try_emplace(position, num_text_fields_);
        pos_it->second.SetField(text_field_number);
      }
    }
  }

  return true;
}

void TextIndexSchema::CommitKeyData(const InternedStringPtr &key) {
  // Retrieve the key's staged data
  TokenPositions token_positions;
  {
    std::lock_guard<std::mutex> guard(in_progress_key_updates_mutex_);
    auto node = in_progress_key_updates_.extract(key);
    // Exit early if the key contains no new text updates
    if (node.empty()) {
      return;
    }
    token_positions = std::move(node.mapped());
  }

  // Retrieve the key's stem mappings
  absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>>
      stem_mappings;
  if (stem_text_field_mask_) {
    std::lock_guard<std::mutex> stem_guard(in_progress_stem_mappings_mutex_);
    auto stem_node = in_progress_stem_mappings_.extract(key);
    if (!stem_node.empty()) {
      stem_mappings = std::move(stem_node.mapped());
    }
  }

  TextIndex key_index{with_suffix_trie_};

  // Index the key's tokens
  for (auto &entry : token_positions) {
    const std::string &token = entry.first;
    auto &[pos_map, suffix] = entry.second;

    const std::optional<std::string> reverse_token =
        suffix ? std::optional<std::string>(
                     std::string(token.rbegin(), token.rend()))
               : std::nullopt;

    // Update metadata from PositionMap
    metadata_.total_positions += pos_map.size();
    for (const auto &[_, field_mask] : pos_map) {
      metadata_.total_term_frequency += field_mask.CountSetFields();
    }

    // Create FlatPositionMap from PositionMap
    FlatPositionMap *flat_map =
        FlatPositionMap::Create(pos_map, num_text_fields_);

    // The updated target gets set in target_add_fn and later used in
    // target_set_fn, so that all trees point to the same postings object
    InvasivePtr<Postings> updated_target;
    {
      absl::MutexLock word_lock(&rax_target_mutex_pool_.Get(token));

      InvasivePtr<Postings> existing;
      {
        // Tree read lock prevents rax node reallocation racing with FindTarget.
        absl::ReaderMutexLock tree_read(&text_index_mutex_);
        existing = text_index_->GetPrefix().FindPostingsTarget(token);
      }
      bool is_new_word = !existing;

      updated_target =
          AddKeyToPostings(std::move(existing), key, flat_map, &metadata_);

      if (is_new_word) {
        absl::WriterMutexLock tree_lock(&text_index_mutex_);
        text_index_->MutateTarget(
            token, updated_target,
            ITEM_COUNT_TRACKING_ENABLED(item_count_op::ADD));
      }
    }

    // Update per-key index (no locking needed — local to this call).
    key_index.MutateTarget(token, updated_target);
  }

  if (stem_text_field_mask_ && !stem_mappings.empty()) {
    absl::WriterMutexLock stem_lock(&stem_tree_mutex_);
    for (const auto &[stemmed, originals] : stem_mappings) {
      auto stem_mutate_fn = CreateSimpleTargetMutateFn<StemParents>(
          [&originals](InvasivePtr<StemParents> existing) {
            if (!existing) existing = InvasivePtr<StemParents>::Make();
            existing->insert(originals.begin(), originals.end());
            return existing;
          });
      stem_tree_.MutateTarget(stemmed, stem_mutate_fn);
    }
  }

  // Map the key to the newly created per-key index
  {
    std::lock_guard<std::mutex> per_key_guard(per_key_text_indexes_mutex_);
    per_key_text_indexes_.emplace(key, std::move(key_index));
  }
}

void TextIndexSchema::DeleteKeyData(const InternedStringPtr &key) {
  // Extract the per-key index
  absl::node_hash_map<Key, TextIndex>::node_type node;
  {
    std::lock_guard<std::mutex> per_key_guard(per_key_text_indexes_mutex_);
    node = per_key_text_indexes_.extract(key);
    if (node.empty()) {
      return;
    }
  }
  TextIndex &key_index = node.mapped();
  auto suffix_opt = text_index_->GetSuffix();

  std::vector<std::string> empty_words;

  auto iter = key_index.GetPrefix().GetWordIterator("");
  while (!iter.Done()) {
    std::string word_str(iter.GetWord());
    {
      absl::MutexLock word_lock(&rax_target_mutex_pool_.Get(word_str));

      InvasivePtr<Postings> existing;
      {
        absl::ReaderMutexLock tree_read(&text_index_mutex_);
        existing = text_index_->GetPrefix().FindPostingsTarget(word_str);
      }

      InvasivePtr<Postings> updated_target;
      updated_target =
          RemoveKeyFromPostings(std::move(existing), key, &metadata_);

      if (!updated_target) {
        absl::WriterMutexLock tree_lock(&text_index_mutex_);
        text_index_->MutateTarget(
            word_str, updated_target,
            ITEM_COUNT_TRACKING_ENABLED(item_count_op::SUBTRACT));
        if (stem_text_field_mask_) {
          empty_words.push_back(word_str);
        }
      }
    }
    iter.Next();
  }

  if (!empty_words.empty() && stem_text_field_mask_) {
    absl::WriterMutexLock stem_lock(&stem_tree_mutex_);
    for (const auto &word : empty_words) {
      std::string stem(word);
      lexer_.StemWordInPlace(stem, lexer_.GetStemmer(), min_stem_size_);
      if (stem != word) {
        auto stem_remove_fn = CreateSimpleTargetMutateFn<StemParents>(
            [&word](InvasivePtr<StemParents> existing) {
              // The term may not exist in the stem tree if it was only present
              // in NOSTEM fields.
              if (existing) {
                CHECK(!existing->empty())
                    << "Stem tree entry should not be empty";
                existing->erase(word);
                if (existing->empty()) existing.Clear();
              }
              return existing;
            });
        stem_tree_.MutateTarget(stem, stem_remove_fn);
      }
    }
  }
}

uint64_t TextIndexSchema::GetTotalPositions() const {
  return metadata_.total_positions.load();
}

uint64_t TextIndexSchema::GetNumUniqueTerms() const {
  return metadata_.num_unique_terms.load();
}

uint64_t TextIndexSchema::GetTotalTermFrequency() const {
  return metadata_.total_term_frequency.load();
}

std::string TextIndexSchema::GetAllStemVariants(
    absl::string_view search_term,
    absl::InlinedVector<absl::string_view, kStemVariantsInlineCapacity>
        &words_to_search,
    uint64_t stem_enabled_mask, bool lock_needed) {
  // Stem the search term
  std::string stemmed(search_term);
  lexer_.StemWordInPlace(stemmed, lexer_.GetStemmer());

  std::optional<absl::ReaderMutexLock> stem_guard;
  if (lock_needed) stem_guard.emplace(&stem_tree_mutex_);

  auto stem_iter = stem_tree_.GetWordIterator(stemmed);
  // GetWordIterator positions at the first word with this prefix, check if
  // exact match
  if (!stem_iter.Done() && stem_iter.GetWord() == stemmed) {
    const auto &parents_ptr = stem_iter.GetStemParentsTarget();
    if (parents_ptr) {
      const auto &parents = *parents_ptr;
      uint32_t max_expansions = options::GetMaxTermExpansions().GetValue();
      uint32_t count = 0;
      for (const auto &parent : parents) {
        if (++count > max_expansions) break;  // Limit parent words added
        words_to_search.push_back(parent);    // Views to tree-owned strings
      }
    }
  }

  return stemmed;  // Caller owns this and will add view to words_to_search
}

}  // namespace valkey_search::indexes::text
