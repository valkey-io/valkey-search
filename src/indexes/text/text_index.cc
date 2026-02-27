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
                                       PositionMap &&pos_map,
                                       TextIndexMetadata *metadata,
                                       size_t num_text_fields) {
  InvasivePtr<Postings> postings;
  if (existing_postings) {
    postings = existing_postings;
  } else {
    metadata->num_unique_terms++;
    postings = InvasivePtr<Postings>::Make();
  }

  postings->InsertKey(key, std::move(pos_map), metadata, num_text_fields);
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

// Factory for target mutate callback with memory tracking and new target
// copying to the outer scope
template <typename Target, typename MutateFn>
std::function<void *(void *)> CreateTargetMutateFn(
    MemoryPool &memory_pool, MutateFn mutate_fn,
    InvasivePtr<Target> &updated_target) {
  return [&updated_target, &memory_pool,
          mutate_fn = std::move(mutate_fn)](void *old_val) -> void * {
    NestedMemoryScope scope{memory_pool};

    // Take ownership of the existing target reference. Nullptr is
    // handled gracefully as a no-op.
    auto existing = InvasivePtr<Target>::AdoptRaw(
        static_cast<InvasivePtrRaw<Target>>(old_val));

    // Mutate the target
    InvasivePtr<Target> new_target = mutate_fn(std::move(existing));

    // Copy the new target reference to the outer scope
    updated_target = new_target;

    // Pass ownership of the new target reference to the tree
    return static_cast<void *>(std::move(new_target).ReleaseRaw());
  };
}

// Factory for target set callback
template <typename Target>
std::function<void *(void *)> CreateTargetSetFn(
    InvasivePtr<Target> &updated_target) {
  return [&updated_target](void *old_val) -> void * {
    // Take ownership of the existing target reference if there is one.
    // It will be deconstructed as it falls out of scope.
    if (old_val) {
      InvasivePtr<Target>::AdoptRaw(
          static_cast<InvasivePtrRaw<Target>>(old_val));
    }
    // Grab a new reference to the new shared target and pass ownership
    // of it to the tree.
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
      min_stem_size_(min_stem_size) {}

absl::StatusOr<bool> TextIndexSchema::StageAttributeData(
    const InternedStringPtr &key, absl::string_view data,
    size_t text_field_number, bool stem, bool suffix) {
  NestedMemoryScope scope{metadata_.text_index_memory_pool_};

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
    const auto &token = tokens.value()[i];
    uint32_t position =
        with_offsets_ ? i
                      : 0;  // If positional info is disabled we default to 0
    auto &[positions, suffix_eligible] = (*token_positions)[token];
    if (suffix) suffix_eligible = true;
    auto [pos_it, _] =
        positions.try_emplace(position, FieldMask::Create(num_text_fields_));
    pos_it->second->SetField(text_field_number);
  }

  return true;
}

void TextIndexSchema::CommitKeyData(const InternedStringPtr &key) {
  NestedMemoryScope scope{metadata_.text_index_memory_pool_};

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

    // The updated target gets set in target_add_fn and later used in
    // target_set_fn, so that all trees point to the same postings object
    InvasivePtr<Postings> updated_target;

    // Note: Right now the memory tracking won't include the position map memory
    // since it's already allocated and moved into the postings object. Once we
    // start creating a serialized version instead then it will be tracked. At
    // that point stop moving the pos_map and just pass a reference so that it
    // doesn't get cleaned up in the memory scope.
    auto target_add_fn = CreateTargetMutateFn(
        metadata_.posting_memory_pool_,
        [&](InvasivePtr<Postings> existing) {
          return AddKeyToPostings(std::move(existing), key, std::move(pos_map),
                                  &metadata_, num_text_fields_);
        },
        updated_target);
    auto target_set_fn = CreateTargetSetFn(updated_target);

    // Update the postings object for the token in the schema-level index with
    // the key and position map
    {
      std::lock_guard<std::mutex> schema_guard(text_index_mutex_);
      text_index_->GetPrefix().MutateTarget(
          token, target_add_fn,
          ITEM_COUNT_TRACKING_ENABLED(item_count_op::ADD));
      if (suffix) {
        text_index_->GetSuffix().value().get().MutateTarget(
            *reverse_token, target_set_fn,
            ITEM_COUNT_TRACKING_ENABLED(item_count_op::ADD));
      }
    }

    // Put the token in the per-key index pointing to the same shared postings
    // object
    key_index.GetPrefix().MutateTarget(token, target_set_fn);
    if (suffix) {
      key_index.GetSuffix().value().get().MutateTarget(*reverse_token,
                                                       target_set_fn);
    }
  }

  // Populate stem tree with mappings
  if (stem_text_field_mask_) {
    std::lock_guard<std::mutex> stem_guard(stem_tree_mutex_);
    for (const auto &[stemmed, originals] : stem_mappings) {
      auto stem_mutate_fn = CreateSimpleTargetMutateFn<StemParents>(
          [&originals](InvasivePtr<StemParents> existing) {
            if (!existing) {
              existing = InvasivePtr<StemParents>::Make();
            }
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
  NestedMemoryScope scope{metadata_.text_index_memory_pool_};

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

  // The updated target gets set in target_remove_fn and later used in
  // target_set_fn, so that all trees point to the same postings object
  InvasivePtr<Postings> updated_target;

  auto target_remove_fn = CreateTargetMutateFn(
      metadata_.posting_memory_pool_,
      [&](InvasivePtr<Postings> existing) {
        return RemoveKeyFromPostings(std::move(existing), key, &metadata_);
      },
      updated_target);
  auto target_set_fn = CreateTargetSetFn(updated_target);

  // Cleanup schema-level text index and stem tree
  auto suffix_opt = text_index_->GetSuffix();
  auto iter = key_index.GetPrefix().GetWordIterator("");
  std::lock_guard<std::mutex> schema_guard(text_index_mutex_);
  while (!iter.Done()) {
    // Remove the key from the schema-level trees
    std::string_view word = iter.GetWord();
    text_index_->GetPrefix().MutateTarget(
        word, target_remove_fn,
        ITEM_COUNT_TRACKING_ENABLED(item_count_op::SUBTRACT));
    if (suffix_opt.has_value()) {
      std::string reverse_word(word.rbegin(), word.rend());
      suffix_opt.value().get().MutateTarget(
          reverse_word, target_set_fn,
          ITEM_COUNT_TRACKING_ENABLED(item_count_op::SUBTRACT));
    }

    // If the postings are now empty, remove from stem tree if it was a parent
    if (!updated_target && stem_text_field_mask_) {
      // Check if this word has a stem mapping using schema-level minimum
      std::string stemmed = lexer_.StemWord(
          std::string(word), lexer_.GetStemmer(), min_stem_size_);
      if (stemmed != word) {
        // This word was a stem parent, remove it from stem tree
        std::lock_guard<std::mutex> stem_guard(stem_tree_mutex_);
        auto stem_remove_fn = CreateSimpleTargetMutateFn<StemParents>(
            [&word](InvasivePtr<StemParents> existing) {
              // The term may not exist in the stem tree if it was only present
              // in NOSTEM fields.
              if (existing) {
                CHECK(!existing->empty())
                    << "Stem tree entry should not be empty";
                existing->erase(std::string(word));
                if (existing->empty()) {
                  existing.Clear();
                }
              }
              return existing;
            });
        stem_tree_.MutateTarget(stemmed, stem_remove_fn);
      }
    }

    iter.Next();
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

uint64_t TextIndexSchema::GetPostingsMemoryUsage() const {
  if (!text_index_) {
    return 0;
  }
  return metadata_.posting_memory_pool_.GetUsage();
}

uint64_t TextIndexSchema::GetRadixTreeMemoryUsage() const {
  // TODO: properly implement
  return GetTotalTextIndexMemoryUsage() - GetPostingsMemoryUsage();
}

// Note: This is a subset of the memory reported by GetPostingsMemoryUsage(),
uint64_t TextIndexSchema::GetPositionMemoryUsage() const {
  uint64_t total_positions = GetTotalPositions();
  return total_positions * sizeof(uint32_t);
}

uint64_t TextIndexSchema::GetTotalTextIndexMemoryUsage() const {
  return metadata_.text_index_memory_pool_.GetUsage();
}

std::string TextIndexSchema::GetAllStemVariants(
    absl::string_view search_term,
    absl::InlinedVector<absl::string_view, kStemVariantsInlineCapacity>
        &words_to_search,
    uint64_t stem_enabled_mask, bool lock_needed) {
  // Stem the search term
  std::string stemmed =
      lexer_.StemWord(std::string(search_term), lexer_.GetStemmer());

  // Conditionally acquire lock - use unique_lock with defer_lock for
  // conditional locking
  std::unique_lock<std::mutex> stem_guard(stem_tree_mutex_, std::defer_lock);
  if (lock_needed) {
    stem_guard.lock();
  }

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
