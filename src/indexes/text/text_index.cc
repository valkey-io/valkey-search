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
#include "vmsdk/src/memory_allocation.h"
namespace valkey_search::indexes::text {

// InvasivePtrRaw<Postings> deletion
static void FreePostingsCallback(void *target) {
  if (target) {
    auto raw = static_cast<InvasivePtrRaw<Postings>>(target);
    InvasivePtr<Postings>::AdoptRaw(raw);
  }
}

namespace {

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

std::function<void *(void *)> CreateTargetSetFn(
    InvasivePtr<Postings> &updated_target) {
  return [&updated_target](void *old_val) -> void * {
    // Take ownership of the existing postings object reference if there is one.
    // It will be deconstructed as it falls out of scope.
    if (old_val) {
      InvasivePtr<Postings>::AdoptRaw(
          static_cast<InvasivePtrRaw<Postings>>(old_val));
    }
    // Grab a new reference to the new shared postings object and pass ownership
    // of it to the tree.
    InvasivePtr<Postings> copy = updated_target;
    return static_cast<void *>(std::move(copy).ReleaseRaw());
  };
}

}  // namespace

/*** TextIndex ***/

TextIndex::TextIndex(bool suffix)
    : prefix_tree_(FreePostingsCallback),
      suffix_tree_(suffix ? std::make_unique<Rax>(FreePostingsCallback)
                          : nullptr) {}

RadixTree<InvasivePtr<Postings>> &TextIndex::GetPrefix() {
  return prefix_tree_;
}

const RadixTree<InvasivePtr<Postings>> &TextIndex::GetPrefix() const {
  return prefix_tree_;
}

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

TextIndexSchema::TextIndexSchema(data_model::Language language,
                                 const std::string &punctuation,
                                 bool with_offsets,
                                 const std::vector<std::string> &stop_words)
    : with_offsets_(with_offsets), lexer_(language, punctuation, stop_words) {}

absl::StatusOr<bool> TextIndexSchema::StageAttributeData(
    const InternedStringPtr &key, absl::string_view data,
    size_t text_field_number, bool stem, size_t min_stem_size, bool suffix) {
  NestedMemoryScope scope{metadata_.text_index_memory_pool_};

  auto tokens = lexer_.Tokenize(data, stem, min_stem_size);

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

    auto target_add_fn = [&](void *old_val) {
      // Note: Right now this won't include the position map memory since
      // it's already allocated and moved into the postings object. Once
      // we start creating a serialized version instead then it will be
      // tracked. At that point stop moving the pos_map and just pass a
      // reference so that it doesn't get cleaned up in the memory scope.
      NestedMemoryScope scope{metadata_.posting_memory_pool_};

      // Take ownership of the existing postings object reference if there is
      // one. It will be deconstructed at the end of this scope.
      auto existing_postings =
          old_val ? InvasivePtr<Postings>::AdoptRaw(
                        static_cast<InvasivePtrRaw<Postings>>(old_val))
                  : InvasivePtr<Postings>{};

      // Mutate the postings
      InvasivePtr<Postings> new_postings =
          AddKeyToPostings(existing_postings, key, std::move(pos_map),
                           &metadata_, num_text_fields_);

      // Copy the new postings to the outer scope
      updated_target = new_postings;

      // Pass ownership of the new postings object reference to the tree
      return static_cast<void *>(std::move(new_postings).ReleaseRaw());
    };

    auto target_set_fn = CreateTargetSetFn(updated_target);

    // Update the postings object for the token in the schema-level index with
    // the key and position map
    {
      std::lock_guard<std::mutex> schema_guard(text_index_mutex_);
      text_index_->GetPrefix().MutateTarget(token, target_add_fn);
      if (suffix) {
        text_index_->GetSuffix().value().get().MutateTarget(*reverse_token,
                                                            target_set_fn);
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

  auto target_remove_fn = [&](void *old_val) {
    NestedMemoryScope scope{metadata_.posting_memory_pool_};

    // Take ownership of the existing postings object reference if there is one.
    // It will be deconstructed at the end of this scope.
    auto existing_postings =
        old_val ? InvasivePtr<Postings>::AdoptRaw(
                      static_cast<InvasivePtrRaw<Postings>>(old_val))
                : InvasivePtr<Postings>{};

    // Mutate the postings
    InvasivePtr<Postings> new_postings =
        RemoveKeyFromPostings(existing_postings, key, &metadata_);

    // Copy the new postings to the outer scope
    updated_target = new_postings;

    // Pass ownership of the new postings object reference to the tree
    return static_cast<void *>(std::move(new_postings).ReleaseRaw());
  };

  // Cleanup schema-level text index
  auto suffix_opt = text_index_->GetSuffix();
  auto iter = key_index.GetPrefix().GetWordIterator("");
  std::lock_guard<std::mutex> schema_guard(text_index_mutex_);
  while (!iter.Done()) {
    // Remove the key from the schema-level trees
    std::string_view word = iter.GetWord();
    text_index_->GetPrefix().MutateTarget(word, target_remove_fn);
    if (suffix_opt.has_value()) {
      std::string reverse_word(word.rbegin(), word.rend());
      suffix_opt.value().get().MutateTarget(reverse_word,
                                            CreateTargetSetFn(updated_target));
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

}  // namespace valkey_search::indexes::text
