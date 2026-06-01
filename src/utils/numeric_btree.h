/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_NUMERIC_BTREE_H_
#define VALKEYSEARCH_SRC_UTILS_NUMERIC_BTREE_H_

#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>

#include "absl/log/check.h"
#include "src/utils/string_interning.h"

namespace valkey_search::utils {

// An order-statistic B+-tree whose leaf entries pair a unique double value
// with the BagOfInternedStringPtrs containing every key indexed at that
// value. All entries live at leaves; leaves are linked in a doubly-linked
// list. Each internal-node child link carries the count of POSTINGS (sum of
// bag sizes) in its subtree, so range counts run in O(log V) where V is the
// number of unique doubles. Range scans yield individual keys in
// (value, key) order.
class NumericBTree {
 public:
  using Bag = BagOfInternedStringPtrs;

  struct LeafEntry {
    double value{0.0};
    Bag keys;

    LeafEntry() = default;
    LeafEntry(const LeafEntry&) = delete;
    LeafEntry& operator=(const LeafEntry&) = delete;
    LeafEntry(LeafEntry&&) noexcept = default;
    LeafEntry& operator=(LeafEntry&&) noexcept = default;
  };

  NumericBTree() = default;
  NumericBTree(const NumericBTree&) = delete;
  NumericBTree& operator=(const NumericBTree&) = delete;
  ~NumericBTree() { Destroy(root_); }

  // Total postings across all bags.
  size_t TotalPostings() const { return total_postings_; }
  // Number of distinct values (== number of leaf entries).
  size_t UniqueValues() const { return unique_values_; }
  bool Empty() const { return total_postings_ == 0; }

  // Returns true if (value, key) was newly added; false if (value, key)
  // already existed.
  bool Insert(double value, const InternedStringPtr& key);

  // Returns true if (value, key) was found and removed.
  bool Erase(double value, const InternedStringPtr& key);

  // Postings with value in [start, end] respecting inclusive flags.
  uint64_t Count(double start, double end, bool start_inclusive,
                 bool end_inclusive) const;

 private:
  static constexpr int kLeafCap = 32;
  static constexpr int kInternalCap = 32;
  static constexpr int kLeafMin = kLeafCap / 2;          // 16
  static constexpr int kInternalMin = kInternalCap / 2;  // 16

  enum class Kind : uint8_t { kLeaf, kInternal };

  struct NodeBase {
    Kind kind;
    uint16_t n;
    NodeBase* parent;
    bool IsLeaf() const { return kind == Kind::kLeaf; }
  };

  struct Leaf : NodeBase {
    Leaf* prev;
    Leaf* next;
    LeafEntry entries[kLeafCap];
    Leaf() {
      this->kind = Kind::kLeaf;
      this->n = 0;
      this->parent = nullptr;
      this->prev = nullptr;
      this->next = nullptr;
    }
  };

  struct Internal : NodeBase {
    // Each value is unique to one leaf entry, so a single double per
    // separator is enough to disambiguate routing.
    double separators[kInternalCap];
    NodeBase* children[kInternalCap + 1];
    uint64_t subtree_count[kInternalCap + 1];
    Internal() {
      this->kind = Kind::kInternal;
      this->n = 0;
      this->parent = nullptr;
      for (int i = 0; i <= kInternalCap; ++i) {
        children[i] = nullptr;
        subtree_count[i] = 0;
      }
    }
  };

  static void Destroy(NodeBase* node) {
    if (!node) {
      return;
    }
    if (node->IsLeaf()) {
      delete static_cast<Leaf*>(node);
    } else {
      auto* in = static_cast<Internal*>(node);
      for (int i = 0; i <= in->n; ++i) {
        Destroy(in->children[i]);
      }
      delete in;
    }
  }

  static uint64_t SubtreePostings(NodeBase* node) {
    if (!node) {
      return 0;
    }
    if (node->IsLeaf()) {
      Leaf* l = static_cast<Leaf*>(node);
      uint64_t s = 0;
      for (int i = 0; i < l->n; ++i) {
        s += l->entries[i].keys.size();
      }
      return s;
    }
    auto* in = static_cast<Internal*>(node);
    uint64_t s = 0;
    for (int i = 0; i <= in->n; ++i) {
      s += in->subtree_count[i];
    }
    return s;
  }

  // First leaf-entry index whose value >= v.
  static int LeafLowerPos(const Leaf* l, double v) {
    int pos = 0;
    while (pos < l->n && l->entries[pos].value < v) {
      ++pos;
    }
    return pos;
  }

  // First child index whose subtree may contain v: smallest ci with
  // (separators[ci-1] <= v and v < separators[ci]). Implemented as
  // first ci such that v < separators[ci].
  static int InternalChildForValue(const Internal* in, double v) {
    int i = 0;
    while (i < in->n && in->separators[i] <= v) {
      ++i;
    }
    return i;
  }

  // For value-only RankLT (count of values strictly less than v).
  static int InternalChildForValueLT(const Internal* in, double v) {
    int i = 0;
    while (i < in->n && in->separators[i] < v) {
      ++i;
    }
    return i;
  }
  static int InternalChildForValueLE(const Internal* in, double v) {
    int i = 0;
    while (i < in->n && in->separators[i] <= v) {
      ++i;
    }
    return i;
  }

  struct InsertOut {
    bool new_posting;       // whether a new (value, key) pair was added
    NodeBase* split_right;  // non-null on split
    double split_sep;       // smallest value of split_right subtree
  };

  static InsertOut NoOp(bool added) { return InsertOut{added, nullptr, 0.0}; }

  InsertOut InsertRec(NodeBase* node, double v, const InternedStringPtr& k) {
    if (node->IsLeaf()) {
      return InsertLeaf(static_cast<Leaf*>(node), v, k);
    }
    return InsertInternal(static_cast<Internal*>(node), v, k);
  }

  InsertOut InsertLeaf(Leaf* l, double v, const InternedStringPtr& k) {
    int pos = LeafLowerPos(l, v);
    if (pos < l->n && l->entries[pos].value == v) {
      auto [_, ins] = l->entries[pos].keys.insert(k);
      return NoOp(ins);
    }
    // Need a new LeafEntry at pos.
    if (l->n < kLeafCap) {
      for (int i = l->n; i > pos; --i) {
        l->entries[i] = std::move(l->entries[i - 1]);
      }
      l->entries[pos] = LeafEntry();
      l->entries[pos].value = v;
      l->entries[pos].keys.insert(k);
      ++l->n;
      ++unique_values_;
      return NoOp(true);
    }
    // Split.
    auto* right = new Leaf();
    constexpr int mid = kLeafCap / 2;
    if (pos <= mid) {
      for (int i = mid; i < kLeafCap; ++i) {
        right->entries[i - mid] = std::move(l->entries[i]);
      }
      right->n = kLeafCap - mid;
      l->n = mid;
      for (int i = l->n; i > pos; --i) {
        l->entries[i] = std::move(l->entries[i - 1]);
      }
      l->entries[pos] = LeafEntry();
      l->entries[pos].value = v;
      l->entries[pos].keys.insert(k);
      ++l->n;
    } else {
      int j = 0;
      for (int i = mid; i < pos; ++i) {
        right->entries[j++] = std::move(l->entries[i]);
      }
      LeafEntry fresh;
      fresh.value = v;
      fresh.keys.insert(k);
      right->entries[j++] = std::move(fresh);
      for (int i = pos; i < kLeafCap; ++i) {
        right->entries[j++] = std::move(l->entries[i]);
      }
      right->n = j;
      l->n = mid;
    }
    right->prev = l;
    right->next = l->next;
    if (l->next) {
      l->next->prev = right;
    }
    l->next = right;
    right->parent = l->parent;
    ++unique_values_;
    return InsertOut{true, right, right->entries[0].value};
  }

  InsertOut InsertInternal(Internal* in, double v, const InternedStringPtr& k) {
    int ci = InternalChildForValue(in, v);
    InsertOut sub = InsertRec(in->children[ci], v, k);
    if (sub.new_posting) {
      ++in->subtree_count[ci];
    }
    if (sub.split_right == nullptr) {
      return InsertOut{sub.new_posting, nullptr, 0.0};
    }

    sub.split_right->parent = in;
    if (in->n < kInternalCap) {
      for (int i = in->n; i > ci; --i) {
        in->separators[i] = in->separators[i - 1];
      }
      for (int i = in->n + 1; i > ci + 1; --i) {
        in->children[i] = in->children[i - 1];
        in->subtree_count[i] = in->subtree_count[i - 1];
      }
      in->separators[ci] = sub.split_sep;
      in->children[ci + 1] = sub.split_right;
      // Recompute counts for the two children touched by the split.
      in->subtree_count[ci] = SubtreePostings(in->children[ci]);
      in->subtree_count[ci + 1] = SubtreePostings(sub.split_right);
      ++in->n;
      return InsertOut{sub.new_posting, nullptr, 0.0};
    }

    // Split internal node.
    constexpr int total_seps = kInternalCap + 1;
    double seps[total_seps];
    NodeBase* kids[total_seps + 1];
    uint64_t cnts[total_seps + 1];
    for (int i = 0; i < ci; ++i) {
      seps[i] = in->separators[i];
    }
    seps[ci] = sub.split_sep;
    for (int i = ci; i < kInternalCap; ++i) {
      seps[i + 1] = in->separators[i];
    }
    for (int i = 0; i <= ci; ++i) {
      kids[i] = in->children[i];
      cnts[i] = in->subtree_count[i];
    }
    kids[ci + 1] = sub.split_right;
    cnts[ci + 1] = SubtreePostings(sub.split_right);
    cnts[ci] = SubtreePostings(in->children[ci]);
    for (int i = ci + 1; i <= kInternalCap; ++i) {
      kids[i + 1] = in->children[i];
      cnts[i + 1] = in->subtree_count[i];
    }

    constexpr int mid = total_seps / 2;
    auto* right = new Internal();
    in->n = mid;
    for (int i = 0; i < mid; ++i) {
      in->separators[i] = seps[i];
    }
    for (int i = 0; i <= mid; ++i) {
      in->children[i] = kids[i];
      in->subtree_count[i] = cnts[i];
      if (in->children[i]) {
        in->children[i]->parent = in;
      }
    }
    for (int i = mid + 1; i <= kInternalCap; ++i) {
      in->children[i] = nullptr;
      in->subtree_count[i] = 0;
    }
    int rseps = total_seps - mid - 1;
    right->n = rseps;
    for (int i = 0; i < rseps; ++i) {
      right->separators[i] = seps[mid + 1 + i];
    }
    for (int i = 0; i <= rseps; ++i) {
      right->children[i] = kids[mid + 1 + i];
      right->subtree_count[i] = cnts[mid + 1 + i];
      if (right->children[i]) {
        right->children[i]->parent = right;
      }
    }
    right->parent = in->parent;
    return InsertOut{sub.new_posting, right, seps[mid]};
  }

  // Returns true iff posting was found and removed.
  bool EraseRec(NodeBase* node, double v, const InternedStringPtr& k) {
    if (node->IsLeaf()) {
      Leaf* l = static_cast<Leaf*>(node);
      int pos = LeafLowerPos(l, v);
      if (pos >= l->n || l->entries[pos].value != v) {
        return false;
      }
      auto& bag = l->entries[pos].keys;
      if (bag.erase(k) == 0) {
        return false;
      }
      if (bag.empty()) {
        for (int i = pos; i < l->n - 1; ++i) {
          l->entries[i] = std::move(l->entries[i + 1]);
        }
        l->entries[l->n - 1] = LeafEntry();
        --l->n;
        --unique_values_;
      }
      return true;
    }
    Internal* in = static_cast<Internal*>(node);
    int ci = InternalChildForValue(in, v);
    bool ok = EraseRec(in->children[ci], v, k);
    if (!ok) {
      return false;
    }
    --in->subtree_count[ci];

    NodeBase* child = in->children[ci];
    bool underflow =
        child->IsLeaf() ? (child->n < kLeafMin) : (child->n < kInternalMin);
    if (underflow) {
      RebalanceChild(in, ci);
    }
    return true;
  }

  void RebalanceChild(Internal* in, int ci) {
    NodeBase* child = in->children[ci];
    NodeBase* left = (ci > 0) ? in->children[ci - 1] : nullptr;
    NodeBase* right = (ci < in->n) ? in->children[ci + 1] : nullptr;
    int min_n = child->IsLeaf() ? kLeafMin : kInternalMin;

    if (left && left->n > min_n) {
      RotateFromLeft(in, ci);
    } else if (right && right->n > min_n) {
      RotateFromRight(in, ci);
    } else if (left) {
      MergeAdjacent(in, ci - 1);
    } else {
      CHECK(right);
      MergeAdjacent(in, ci);
    }
  }

  void RotateFromLeft(Internal* in, int ci) {
    NodeBase* child = in->children[ci];
    NodeBase* left = in->children[ci - 1];
    if (child->IsLeaf()) {
      Leaf* L = static_cast<Leaf*>(left);
      Leaf* R = static_cast<Leaf*>(child);
      uint64_t moved = L->entries[L->n - 1].keys.size();
      for (int i = R->n; i > 0; --i) {
        R->entries[i] = std::move(R->entries[i - 1]);
      }
      R->entries[0] = std::move(L->entries[L->n - 1]);
      L->entries[L->n - 1] = LeafEntry();
      --L->n;
      ++R->n;
      in->separators[ci - 1] = R->entries[0].value;
      in->subtree_count[ci - 1] -= moved;
      in->subtree_count[ci] += moved;
    } else {
      Internal* L = static_cast<Internal*>(left);
      Internal* R = static_cast<Internal*>(child);
      uint64_t moved = L->subtree_count[L->n];
      for (int i = R->n; i > 0; --i) {
        R->separators[i] = R->separators[i - 1];
      }
      for (int i = R->n + 1; i > 0; --i) {
        R->children[i] = R->children[i - 1];
        R->subtree_count[i] = R->subtree_count[i - 1];
      }
      R->separators[0] = in->separators[ci - 1];
      R->children[0] = L->children[L->n];
      R->subtree_count[0] = moved;
      if (R->children[0]) {
        R->children[0]->parent = R;
      }
      in->separators[ci - 1] = L->separators[L->n - 1];
      L->children[L->n] = nullptr;
      L->subtree_count[L->n] = 0;
      --L->n;
      ++R->n;
      in->subtree_count[ci - 1] -= moved;
      in->subtree_count[ci] += moved;
    }
  }

  void RotateFromRight(Internal* in, int ci) {
    NodeBase* child = in->children[ci];
    NodeBase* right = in->children[ci + 1];
    if (child->IsLeaf()) {
      Leaf* L = static_cast<Leaf*>(child);
      Leaf* R = static_cast<Leaf*>(right);
      uint64_t moved = R->entries[0].keys.size();
      L->entries[L->n] = std::move(R->entries[0]);
      ++L->n;
      for (int i = 0; i < R->n - 1; ++i) {
        R->entries[i] = std::move(R->entries[i + 1]);
      }
      R->entries[R->n - 1] = LeafEntry();
      --R->n;
      in->separators[ci] = R->entries[0].value;
      in->subtree_count[ci] += moved;
      in->subtree_count[ci + 1] -= moved;
    } else {
      Internal* L = static_cast<Internal*>(child);
      Internal* R = static_cast<Internal*>(right);
      uint64_t moved = R->subtree_count[0];
      L->separators[L->n] = in->separators[ci];
      L->children[L->n + 1] = R->children[0];
      L->subtree_count[L->n + 1] = moved;
      if (L->children[L->n + 1]) {
        L->children[L->n + 1]->parent = L;
      }
      in->separators[ci] = R->separators[0];
      for (int i = 0; i < R->n - 1; ++i) {
        R->separators[i] = R->separators[i + 1];
      }
      for (int i = 0; i < R->n; ++i) {
        R->children[i] = R->children[i + 1];
        R->subtree_count[i] = R->subtree_count[i + 1];
      }
      R->children[R->n] = nullptr;
      R->subtree_count[R->n] = 0;
      --R->n;
      ++L->n;
      in->subtree_count[ci] += moved;
      in->subtree_count[ci + 1] -= moved;
    }
  }

  void MergeAdjacent(Internal* in, int i) {
    NodeBase* L = in->children[i];
    NodeBase* R = in->children[i + 1];
    uint64_t merged_count = in->subtree_count[i] + in->subtree_count[i + 1];
    if (L->IsLeaf()) {
      Leaf* LL = static_cast<Leaf*>(L);
      Leaf* RR = static_cast<Leaf*>(R);
      for (int j = 0; j < RR->n; ++j) {
        LL->entries[LL->n + j] = std::move(RR->entries[j]);
      }
      LL->n += RR->n;
      LL->next = RR->next;
      if (RR->next) {
        RR->next->prev = LL;
      }
      delete RR;
    } else {
      Internal* LL = static_cast<Internal*>(L);
      Internal* RR = static_cast<Internal*>(R);
      LL->separators[LL->n] = in->separators[i];
      for (int j = 0; j < RR->n; ++j) {
        LL->separators[LL->n + 1 + j] = RR->separators[j];
      }
      for (int j = 0; j <= RR->n; ++j) {
        LL->children[LL->n + 1 + j] = RR->children[j];
        LL->subtree_count[LL->n + 1 + j] = RR->subtree_count[j];
        if (LL->children[LL->n + 1 + j]) {
          LL->children[LL->n + 1 + j]->parent = LL;
        }
        RR->children[j] = nullptr;
      }
      LL->n += RR->n + 1;
      delete RR;
    }
    for (int j = i; j < in->n - 1; ++j) {
      in->separators[j] = in->separators[j + 1];
    }
    for (int j = i + 1; j < in->n; ++j) {
      in->children[j] = in->children[j + 1];
      in->subtree_count[j] = in->subtree_count[j + 1];
    }
    in->children[in->n] = nullptr;
    in->subtree_count[in->n] = 0;
    --in->n;
    in->subtree_count[i] = merged_count;
  }

  uint64_t RankLT(double v) const { return RankRec(root_, v, true); }
  uint64_t RankLE(double v) const { return RankRec(root_, v, false); }
  uint64_t RankRec(NodeBase* node, double v, bool strict) const {
    if (!node) {
      return 0;
    }
    if (node->IsLeaf()) {
      Leaf* l = static_cast<Leaf*>(node);
      uint64_t r = 0;
      for (int i = 0; i < l->n; ++i) {
        bool ok =
            strict ? (l->entries[i].value < v) : (l->entries[i].value <= v);
        if (!ok) {
          break;
        }
        r += l->entries[i].keys.size();
      }
      return r;
    }
    Internal* in = static_cast<Internal*>(node);
    int ci = strict ? InternalChildForValueLT(in, v)
                    : InternalChildForValueLE(in, v);
    uint64_t r = 0;
    for (int j = 0; j < ci; ++j) {
      r += in->subtree_count[j];
    }
    r += RankRec(in->children[ci], v, strict);
    return r;
  }

 public:
  // Const iterator that yields one InternedStringPtr per posting in
  // (value, key) order. Implemented as a (leaf, entry-index, bag-iterator)
  // triple.
  class Iterator {
   public:
    Iterator() = default;
    bool IsEnd() const { return leaf_ == nullptr; }
    const InternedStringPtr& operator*() const {
      DCHECK(leaf_);
      return *bag_iter_;
    }
    const InternedStringPtr* operator->() const {
      DCHECK(leaf_);
      return bag_iter_.operator->();
    }
    double Value() const {
      DCHECK(leaf_);
      return leaf_->entries[entry_idx_].value;
    }

    Iterator& operator++() {
      DCHECK(leaf_);
      ++bag_iter_;
      AdvanceIfBagExhausted();
      return *this;
    }
    bool operator==(const Iterator& o) const {
      if (leaf_ != o.leaf_) {
        return false;
      }
      if (leaf_ == nullptr) {
        return true;
      }
      return entry_idx_ == o.entry_idx_ && bag_iter_ == o.bag_iter_;
    }
    bool operator!=(const Iterator& o) const { return !(*this == o); }

   private:
    friend class NumericBTree;
    Iterator(const Leaf* l, int entry_idx) : leaf_(l), entry_idx_(entry_idx) {
      if (leaf_ && entry_idx_ < leaf_->n) {
        bag_iter_ = leaf_->entries[entry_idx_].keys.begin();
        AdvanceIfBagExhausted();
      } else {
        leaf_ = nullptr;
      }
    }

    void AdvanceIfBagExhausted() {
      while (leaf_ != nullptr) {
        if (entry_idx_ >= leaf_->n) {
          leaf_ = leaf_->next;
          entry_idx_ = 0;
          if (leaf_ != nullptr && leaf_->n > 0) {
            bag_iter_ = leaf_->entries[0].keys.begin();
          }
          continue;
        }
        if (bag_iter_ != leaf_->entries[entry_idx_].keys.end()) {
          return;
        }
        ++entry_idx_;
        if (entry_idx_ < leaf_->n) {
          bag_iter_ = leaf_->entries[entry_idx_].keys.begin();
        }
      }
    }

    const Leaf* leaf_{nullptr};
    int entry_idx_{0};
    Bag::const_iterator bag_iter_{};
  };

  Iterator Begin() const {
    if (!first_leaf_) {
      return Iterator();
    }
    return Iterator(first_leaf_, 0);
  }
  Iterator End() const { return Iterator(); }

  // First posting whose value >= v.
  Iterator LowerBoundByValue(double v) const {
    return DescendForValue(v, /*strict=*/true);
  }
  // First posting whose value > v.
  Iterator UpperBoundByValue(double v) const {
    return DescendForValue(v, /*strict=*/false);
  }

 private:
  Iterator DescendForValue(double v, bool strict) const {
    NodeBase* node = root_;
    if (!node) {
      return Iterator();
    }
    while (!node->IsLeaf()) {
      Internal* in = static_cast<Internal*>(node);
      int ci = strict ? InternalChildForValueLT(in, v)
                      : InternalChildForValueLE(in, v);
      node = in->children[ci];
    }
    Leaf* l = static_cast<Leaf*>(node);
    int i = 0;
    if (strict) {
      while (i < l->n && l->entries[i].value < v) {
        ++i;
      }
    } else {
      while (i < l->n && l->entries[i].value <= v) {
        ++i;
      }
    }
    if (i >= l->n) {
      // Walk to next leaf.
      Leaf* next_leaf = l->next;
      if (!next_leaf) {
        return Iterator();
      }
      return Iterator(next_leaf, 0);
    }
    return Iterator(l, i);
  }

  NodeBase* root_{nullptr};
  Leaf* first_leaf_{nullptr};
  size_t total_postings_{0};
  size_t unique_values_{0};

  // Loose bounds on the values currently in the tree. Maintained as:
  //   cached_min_ <= min(values currently present)
  //   cached_max_ >= max(values currently present)
  // We tighten on Insert (taking min/max with the new value) and never
  // tighten on Erase, so the bounds may drift wider than the truth without
  // affecting Count's correctness -- a too-wide bound only causes the
  // whole-range early-out to fire less often, never to fire incorrectly.
  // When the tree becomes empty we reset to the neutral sentinel.
  double cached_min_{std::numeric_limits<double>::max()};
  double cached_max_{std::numeric_limits<double>::lowest()};
};

inline bool NumericBTree::Insert(double value, const InternedStringPtr& key) {
  if (!root_) {
    auto* l = new Leaf();
    l->entries[0].value = value;
    l->entries[0].keys.insert(key);
    l->n = 1;
    root_ = l;
    first_leaf_ = l;
    total_postings_ = 1;
    unique_values_ = 1;
    cached_min_ = value;
    cached_max_ = value;
    return true;
  }
  InsertOut r = InsertRec(root_, value, key);
  if (!r.new_posting) {
    return false;
  }
  if (r.split_right) {
    auto* new_root = new Internal();
    new_root->separators[0] = r.split_sep;
    new_root->children[0] = root_;
    new_root->children[1] = r.split_right;
    new_root->subtree_count[0] = SubtreePostings(root_);
    new_root->subtree_count[1] = SubtreePostings(r.split_right);
    new_root->n = 1;
    root_->parent = new_root;
    r.split_right->parent = new_root;
    root_ = new_root;
  }
  ++total_postings_;
  if (value < cached_min_) {
    cached_min_ = value;
  }
  if (value > cached_max_) {
    cached_max_ = value;
  }
  return true;
}

inline bool NumericBTree::Erase(double value, const InternedStringPtr& key) {
  if (!root_) {
    return false;
  }
  bool ok = EraseRec(root_, value, key);
  if (!ok) {
    return false;
  }
  --total_postings_;
  if (root_->IsLeaf()) {
    if (root_->n == 0) {
      delete static_cast<Leaf*>(root_);
      root_ = nullptr;
      first_leaf_ = nullptr;
      cached_min_ = std::numeric_limits<double>::max();
      cached_max_ = std::numeric_limits<double>::lowest();
    }
  } else {
    auto* in = static_cast<Internal*>(root_);
    if (in->n == 0) {
      NodeBase* new_root = in->children[0];
      in->children[0] = nullptr;
      delete in;
      root_ = new_root;
      if (root_) {
        root_->parent = nullptr;
      }
    }
  }
  return true;
}

inline uint64_t NumericBTree::Count(double start, double end,
                                    bool start_inclusive,
                                    bool end_inclusive) const {
  if (total_postings_ == 0) {
    return 0;
  }
  // Whole-tree fast path: if the query range fully covers the cached bounds
  // (which are loose lower bounds on min and upper bounds on max -- never
  // tighter than the truth), every posting is in range and we can return
  // total_postings_ in O(1) without descending the spine.
  bool covers_min =
      start_inclusive ? (start <= cached_min_) : (start < cached_min_);
  bool covers_max = end_inclusive ? (end >= cached_max_) : (end > cached_max_);
  if (covers_min && covers_max) {
    return total_postings_;
  }
  uint64_t left = start_inclusive ? RankLT(start) : RankLE(start);
  uint64_t right = end_inclusive ? RankLE(end) : RankLT(end);
  return right >= left ? right - left : 0;
}

}  // namespace valkey_search::utils

#endif  // VALKEYSEARCH_SRC_UTILS_NUMERIC_BTREE_H_
