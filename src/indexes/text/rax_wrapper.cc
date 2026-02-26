/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/rax_wrapper.h"

#include <cerrno>
#include <cstring>

#include "absl/log/check.h"

namespace valkey_search::indexes::text {

namespace {

// C-compatible callback wrapper
// We can't pass the mutation callback closure directly to C APIs, so we wrap it
// in a C-style function and pass the closure as opaque caller context.
extern "C" void *MutateCallbackWrapper(void *current, void *caller_context) {
  auto *fn = static_cast<absl::FunctionRef<void *(void *)> *>(caller_context);
  return (*fn)(current);
}

}  // namespace

// Constructor
Rax::Rax(void (*free_callback)(void *))
    : rax_(raxNew()), free_callback_(free_callback) {
  CHECK(rax_ != nullptr) << "Failed to create rax tree";
}

// Destructor
Rax::~Rax() {
  if (rax_) {
    raxFreeWithCallback(rax_, free_callback_);
    rax_ = nullptr;
  }
}

// Move constructor
Rax::Rax(Rax &&other) noexcept
    : rax_(other.rax_), free_callback_(other.free_callback_) {
  other.rax_ = nullptr;
  other.free_callback_ = nullptr;
}

// Move assignment
Rax &Rax::operator=(Rax &&other) noexcept {
  if (this != &other) {
    if (rax_) {
      raxFreeWithCallback(rax_, free_callback_);
    }
    rax_ = other.rax_;
    free_callback_ = other.free_callback_;
    other.rax_ = nullptr;
    other.free_callback_ = nullptr;
  }
  return *this;
}

void Rax::MutateTarget(absl::string_view word,
                       absl::FunctionRef<void *(void *)> mutate,
                       item_count_op op) {
  CHECK(!word.empty()) << "Can't mutate the target for an empty word";

  unsigned char *c_word = const_cast<unsigned char *>(
      reinterpret_cast<const unsigned char *>(word.data()));
  void *opaque_callback = reinterpret_cast<void *>(&mutate);
  int res = raxMutate(rax_, c_word, word.size(), MutateCallbackWrapper,
                      opaque_callback, op);
  CHECK(res) << "Rax mutation failed for word: " << word << ", errno: " << errno
             << " (" << strerror(errno) << ")";
}

size_t Rax::GetTotalUniqueWordCount() const { return raxSize(rax_); }

size_t Rax::GetSubtreeKeyCount(absl::string_view prefix) const {
  return raxGetSubtreeItemCount(
      rax_,
      const_cast<unsigned char *>(
          reinterpret_cast<const unsigned char *>(prefix.data())),
      prefix.size());
}

size_t Rax::GetLongestWord() const {
  // TODO: Implement longest word calculation
  return 0;
}

size_t Rax::GetAllocSize() const { return raxAllocSize(rax_); }

bool Rax::IsValid() const { return rax_ != nullptr && raxSize(rax_) > 0; }

Rax::WordIterator Rax::GetWordIterator(absl::string_view prefix) const {
  return WordIterator(rax_, prefix);
}

/*** WordIterator ***/

Rax::WordIterator::WordIterator(rax *rax, absl::string_view prefix)
    : prefix_(prefix) {
  raxStart(&iter_, rax);

  auto raw_prefix = const_cast<unsigned char *>(
      reinterpret_cast<const unsigned char *>(prefix.data()));

  // Seek to first node matching the prefix
  CHECK(raxSeekSubTree(&iter_, raw_prefix, prefix.size()));
  raxNext(&iter_);
  // Check that we're at a node within the prefix's path
  if (raxEOF(&iter_)) {
    done_ = true;
  }
}

Rax::WordIterator::~WordIterator() { raxStop(&iter_); }

bool Rax::WordIterator::Done() const { return done_; }

void Rax::WordIterator::Next() {
  CHECK(!Done()) << "Out of range";

  raxNext(&iter_);
  if (raxEOF(&iter_)) {
    done_ = true;
    return;
  }
}

// TODO: currently unused and untested
bool Rax::WordIterator::SeekForward(absl::string_view word) {
  CHECK(!Done()) << "Out of range";

  // Check if word matches prefix
  if (!word.starts_with(prefix_)) {
    done_ = true;
    return false;
  }

  // Seek to the word
  CHECK(
      !raxSeekSubTree(&iter_,
                      const_cast<unsigned char *>(
                          reinterpret_cast<const unsigned char *>(word.data())),
                      word.size()));
  raxNext(&iter_);
  if (raxEOF(&iter_)) {
    done_ = true;
    return false;
  }

  // Return true if exact match, false if greater
  return GetWord() == word;
}

absl::string_view Rax::WordIterator::GetWord() const {
  CHECK(!Done()) << "Cannot get word from invalid iterator";
  return absl::string_view(reinterpret_cast<const char *>(iter_.key),
                           iter_.key_len);
}

void *Rax::WordIterator::GetTarget() const {
  CHECK(!Done()) << "Cannot get target from invalid iterator";
  return iter_.data;
}

InvasivePtr<Postings> Rax::WordIterator::GetPostingsTarget() const {
  return InvasivePtr<Postings>::CopyRaw(
      static_cast<InvasivePtrRaw<Postings>>(GetTarget()));
}

InvasivePtr<StemParents> Rax::WordIterator::GetStemParentsTarget() const {
  return InvasivePtr<StemParents>::CopyRaw(
      static_cast<InvasivePtrRaw<StemParents>>(GetTarget()));
}

/*** PathIterator ***/
// WARNING: The PathIterator is not productionized and needs to be cleaned up.

namespace {

// Helper to compute padding for rax node
inline size_t RaxPadding(size_t nodesize) {
  return (sizeof(void *) - ((nodesize + 4) % sizeof(void *))) &
         (sizeof(void *) - 1);
}

// Helper to get pointer to first child in a rax node
inline raxNode **RaxNodeFirstChildPtr(raxNode *n) {
  return reinterpret_cast<raxNode **>(n->data + n->size + RaxPadding(n->size));
}

// Helper to get data stored in a rax node
inline void *RaxNodeGetData(raxNode *n) {
  if (!n->iskey || n->isnull) return nullptr;
  size_t node_len =
      sizeof(raxNode) + n->size + RaxPadding(n->size) +
      (n->iscompr ? sizeof(raxNode *) : sizeof(raxNode *) * n->size) +
      sizeof(void *);
  return *reinterpret_cast<void **>(reinterpret_cast<char *>(n) + node_len -
                                    sizeof(void *));
}

// Helper to check if node is a leaf (no children)
inline bool RaxNodeIsLeaf(raxNode *n) { return n->size == 0 && !n->iscompr; }

}  // namespace

Rax::PathIterator Rax::GetPathIterator(absl::string_view prefix) const {
  return PathIterator(rax_, prefix);
}

Rax::PathIterator::PathIterator(rax *rax, absl::string_view prefix)
    : rax_(rax), node_(nullptr), child_index_(0), exhausted_(false) {
  if (!rax_ || !rax_->head) {
    exhausted_ = true;
    return;
  }

  // Navigate to the prefix, similar to raxLowWalk
  raxNode *h = rax_->head;
  size_t i = 0;

  while (i < prefix.size()) {
    if (h->iscompr) {
      // Compressed node: check how much of the path matches
      size_t match = 0;
      size_t max_match =
          std::min(static_cast<size_t>(h->size), prefix.size() - i);
      while (match < max_match &&
             h->data[match] == static_cast<unsigned char>(prefix[i + match])) {
        match++;
      }

      if (match < h->size) {
        // Partial match or no match - prefix not fully in tree
        if (match < prefix.size() - i) {
          // Prefix extends beyond what we matched, no valid node
          exhausted_ = true;
          return;
        }
        // Prefix ends in the middle of compressed path - position here
        break;
      }
      i += h->size;
      // Descend to child
      h = *reinterpret_cast<raxNode **>(h->data + h->size +
                                        RaxPadding(h->size));
    } else {
      // Branching node: find child with matching byte
      unsigned char c = static_cast<unsigned char>(prefix[i]);
      size_t pos;
      for (pos = 0; pos < h->size; pos++) {
        if (h->data[pos] >= c) break;
      }
      if (pos >= h->size || h->data[pos] != c) {
        // Character not found
        exhausted_ = true;
        return;
      }
      i++;
      // Descend to child
      raxNode **children = RaxNodeFirstChildPtr(h);
      h = children[pos];
    }
  }

  node_ = h;
  path_ = std::string(prefix);
}

Rax::PathIterator::~PathIterator() = default;

// Private constructor for DescendNew - directly positions at a node
Rax::PathIterator::PathIterator(rax *rax, raxNode *node, std::string path)
    : rax_(rax),
      node_(node),
      path_(std::move(path)),
      child_index_(0),
      exhausted_(false) {}

bool Rax::PathIterator::Done() const {
  if (!node_ || exhausted_) return true;
  // Leaf nodes have no children to iterate
  if (RaxNodeIsLeaf(node_)) return true;
  // Branching nodes: done when past all children
  if (!node_->iscompr) return child_index_ >= node_->size;
  // Compressed nodes have exactly one child edge
  return false;
}

bool Rax::PathIterator::IsWord() const { return node_ && node_->iskey; }

void Rax::PathIterator::NextChild() {
  if (!node_ || exhausted_) return;

  if (node_->iscompr || RaxNodeIsLeaf(node_)) {
    // Compressed or leaf: only one "child", mark as exhausted after first
    exhausted_ = true;
  } else {
    // Branching node: move to next child
    child_index_++;
  }
}

bool Rax::PathIterator::SeekForward(char target) {
  if (!node_ || exhausted_) return false;

  if (node_->iscompr) {
    // Compressed node: check if first char of compressed path matches
    if (node_->size > 0 &&
        node_->data[0] == static_cast<unsigned char>(target)) {
      return true;
    }
    exhausted_ = true;
    return false;
  }

  if (RaxNodeIsLeaf(node_)) {
    exhausted_ = true;
    return false;
  }

  // Branching node: binary-like search for target (children are sorted)
  unsigned char t = static_cast<unsigned char>(target);
  for (size_t i = child_index_; i < node_->size; i++) {
    if (node_->data[i] == t) {
      child_index_ = i;
      return true;
    }
    if (node_->data[i] > t) {
      child_index_ = i;
      return false;
    }
  }
  child_index_ = node_->size;  // Past end
  return false;
}

bool Rax::PathIterator::CanDescend() const {
  if (!node_ || exhausted_) return false;
  if (RaxNodeIsLeaf(node_)) return false;
  if (node_->iscompr) return true;  // Compressed always has one child
  return child_index_ < node_->size;
}

Rax::PathIterator Rax::PathIterator::DescendNew() const {
  CHECK(CanDescend()) << "Cannot descend from leaf or exhausted iterator";

  if (node_->iscompr) {
    // Compressed: descend through the compressed path to child
    std::string new_path = path_;
    new_path.append(reinterpret_cast<const char *>(node_->data), node_->size);
    raxNode *child = *reinterpret_cast<raxNode **>(node_->data + node_->size +
                                                   RaxPadding(node_->size));
    return PathIterator(rax_, child, std::move(new_path));
  }

  // Branching: descend through current child
  std::string new_path = path_;
  new_path += static_cast<char>(node_->data[child_index_]);
  raxNode **children = RaxNodeFirstChildPtr(node_);
  return PathIterator(rax_, children[child_index_], std::move(new_path));
}

absl::string_view Rax::PathIterator::GetPath() const { return path_; }

absl::string_view Rax::PathIterator::GetChildEdge() {
  if (!node_ || exhausted_) {
    child_edge_.clear();
    return child_edge_;
  }

  if (node_->iscompr) {
    // Compressed: edge is the entire compressed path
    child_edge_.assign(reinterpret_cast<const char *>(node_->data),
                       node_->size);
  } else if (!RaxNodeIsLeaf(node_) && child_index_ < node_->size) {
    // Branching: edge is single character
    child_edge_.assign(1, static_cast<char>(node_->data[child_index_]));
  } else {
    child_edge_.clear();
  }
  return child_edge_;
}

void *Rax::PathIterator::GetTarget() const {
  CHECK(IsWord()) << "Cannot get target from non-word node";
  return RaxNodeGetData(node_);
}

InvasivePtr<Postings> Rax::PathIterator::GetPostingsTarget() const {
  return InvasivePtr<Postings>::CopyRaw(
      static_cast<InvasivePtrRaw<Postings>>(GetTarget()));
}

void Rax::PathIterator::Defrag() {
  // TODO: Implement defragmentation if needed
}

}  // namespace valkey_search::indexes::text
