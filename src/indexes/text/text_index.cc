/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/text_index.h"

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
  // Get or create stem mappings for this key if stemming is enabled
  absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>>
      *stem_mappings_ptr = nullptr;
  if (stem) {
    std::lock_guard<std::mutex> stem_guard(in_progress_stem_mappings_mutex_);
    stem_mappings_ptr = &in_progress_stem_mappings_[key];
  }

  // Tokenize and collect stem mappings
  auto tokens = lexer_.Tokenize(data, stem, min_stem_size_, stem_mappings_ptr);

  if (!tokens.ok()) {
    if (tokens.status().code() == absl::StatusCode::kInvalidArgument) {
      return false;  // UTF-8 errors → hash_indexing_failures
    }
    return tokens.status();
  }

  // Map tokens -> positions -> field-masks
  TokenPositions *token_positions;
  {
    std::lock_guard<std::mutex> guard(in_progress_key_updates_mutex_);
    token_positions = &in_progress_key_updates_[key];
  }
  for (uint32_t i = 0; i < tokens->size(); ++i) {
    const auto &token = (*tokens)[i];
    uint32_t position =
        with_offsets_ ? i
                      : 0;  // If positional info is disabled we default to 0
    auto &[positions, suffix_eligible] = (*token_positions)[token];
    if (suffix) suffix_eligible = true;
    auto [pos_it, _] =
        positions.try_emplace(position, FieldMask(num_text_fields_));
    pos_it->second.SetField(text_field_number);
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

const TextIndex *TextIndexSchema::LookupPerKeyTextIndex(const Key &key, bool lock) {
  if (!key) {
    CHECK(false) << "Invalid null key passed to LookupPerKeyTextIndex";
  }
  std::optional<std::lock_guard<std::mutex>> per_key_guard;
  if (lock) per_key_guard.emplace(per_key_text_indexes_mutex_);
  if (auto it = per_key_text_indexes_.find(key); it != per_key_text_indexes_.end()) {
    return &it->second;
  }
  // Key not found in text indexes - this is normal for keys without text data
  return nullptr;
}

}  // namespace valkey_search::indexes::text
