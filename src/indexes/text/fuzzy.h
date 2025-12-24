/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_FUZZY_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_FUZZY_H_

#include <algorithm>
#include <string>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "absl/strings/string_view.h"
#include "invasive_ptr.h"
#include "posting.h"
#include "radix_tree.h"
#include "text.h"

namespace valkey_search::indexes::text {

// Fuzzy search using Damerau-Levenshtein distance on RadixTree
struct FuzzySearch {
  // Returns KeyIterators for all words within edit distance <= max_distance
  static absl::InlinedVector<Postings::KeyIterator,
                             kWordExpansionInlineCapacity>
  Search(const RadixTree<InvasivePtr<Postings>>& tree,
         absl::string_view pattern, size_t max_distance) {
    absl::InlinedVector<indexes::text::Postings::KeyIterator,
                        kWordExpansionInlineCapacity>
        key_iterators;

    // Dynamic Programming matrix rows for Damerau-Levenshtein algorithm
    // Row i-2 (for transposition)
    absl::InlinedVector<size_t, 32> prev_prev(pattern.length() + 1);
    // Row i-1 (previous row)
    absl::InlinedVector<size_t, 32> prev(pattern.length() + 1);
    // Row i (current row)
    absl::InlinedVector<size_t, 32> curr(pattern.length() + 1);

    // Initialize first row: distance from empty string to each pattern prefix.
    // Example: for pattern "race", first row is [0, 1, 2, 3, 4]
    for (size_t i = 0; i <= pattern.length(); ++i) {
      prev[i] = i;
    }

    // Start traversal from root to explore all words in the tree
    auto iter = tree.GetPathIterator("");
    SearchRecursive(iter, pattern, max_distance, "", '\0', prev_prev, prev,
                    curr, key_iterators);
    return key_iterators;
  }

 private:
  static void SearchRecursive(
      RadixTree<InvasivePtr<Postings>>::PathIterator iter,
      absl::string_view pattern, size_t max_distance,
      std::string word,   // Current word being built
      char prev_tree_ch,  // Previous character (for transposition detection)
      absl::InlinedVector<size_t, 32>&
          prev_prev,  // Row i-2 of DP matrix (for transposition)
      absl::InlinedVector<size_t, 32>&
          prev,  // Row i-1 of DP matrix (previous row)
      absl::InlinedVector<size_t, 32>&
          curr,  // Row i of DP matrix (current row being computed)
      absl::InlinedVector<indexes::text::Postings::KeyIterator,
                          kWordExpansionInlineCapacity>& key_iterators) {
    // Iterate over children at current tree level
    while (!iter.Done()) {
      absl::string_view edge = iter.GetChildEdge();
      std::string new_word = word;
      // Minimum edit distance in the current DP row after processing the edge.
      // Used for pruning: if min_dist > max_distance, skip entire subtree.
      size_t min_dist;

      // SAVE STATE: Each child must start with same parent state
      auto saved_prev_prev = prev_prev;
      auto saved_prev = prev;
      char saved_prev_tree_ch = prev_tree_ch;

      // Process each character in the edge
      for (char tree_ch : edge) {
        new_word += tree_ch;

        curr[0] = new_word.length();
        min_dist = curr[0];

        // DP matrix (example: "car" vs pattern "cra"):
        //       ""  "c"  "cr"  "cra"
        // ""   [ 0,  1,   2,   3  ]
        // "c"  [ 1,  0,   1,   2  ]
        // "ca" [ 2,  1,   1,   1  ]
        // "car"[ 3,  2,   1,   1  ]
        // Computing curr[i] from:
        //   [..prev[i-1]   prev[i] ]      <- diagonal (substitution), above
        //   (deletion)
        //   [..curr[i-1]   curr[i] ]     <- left (insertion), result
        //
        // curr[i] = minimum of (
        //   prev[i] + 1,           // Deletion (from above cell)
        //   curr[i-1] + 1,         // Insertion (from left cell)
        //   prev[i-1] + cost,      // Substitution (from diagonal cell)
        //   prev_prev[i-2] + cost  // Transposition (from 2 back diagonal)
        // )
        for (size_t i = 1; i <= pattern.length(); ++i) {
          char pattern_ch = pattern[i - 1];
          size_t cost = (tree_ch == pattern_ch) ? 0 : 1;

          curr[i] = std::min({
              prev[i] + 1,        // Deletion
              curr[i - 1] + 1,    // Insertion
              prev[i - 1] + cost  // Substitution
          });

          // Damerau-Levenshtein: transposition
          if (i > 1 && new_word.length() > 1 && tree_ch == pattern[i - 2] &&
              pattern_ch == prev_tree_ch) {
            curr[i] = std::min(curr[i], prev_prev[i - 2] + cost);
          }

          min_dist = std::min(min_dist, curr[i]);
        }
        // Shift rows for next character: curr becomes prev, prev becomes
        // prev_prev
        prev_prev.swap(prev);
        prev.swap(curr);
        prev_tree_ch = tree_ch;
      }
      // Pruning: skip subtree if minimum distance exceeds target edit distance.
      // Since distance can only increase with more characters, if we're already
      // too far, no word in this subtree can match.
      bool should_prune = (min_dist > max_distance);
      if (should_prune) {
        // Restore state for next child (each child starts from same parent
        // state)
        prev_prev = saved_prev_prev;
        prev = saved_prev;
        prev_tree_ch = saved_prev_tree_ch;
        iter.NextChild();
        continue;
      }

      // Descend to the child node at the end of this edge
      if (iter.CanDescend()) {
        auto child_iter = iter.DescendNew();
        // Check if the node has a word and edit distance is within the limit
        // The edit distance is in prev row now as we did the row swap
        // in loop above
        if (child_iter.IsWord() && prev[pattern.length()] <= max_distance) {
          key_iterators.emplace_back(child_iter.GetTarget()->GetKeyIterator());
        }

        // Recurse into child's subtree
        if (child_iter.CanDescend()) {
          SearchRecursive(child_iter, pattern, max_distance, new_word,
                          prev_tree_ch, prev_prev, prev, curr, key_iterators);
        }
      }

      // Restore state for next child (each child starts from same parent
      // state)
      prev_prev = saved_prev_prev;
      prev = saved_prev;
      prev_tree_ch = saved_prev_tree_ch;

      iter.NextChild();
    }
  }
};

}  // namespace valkey_search::indexes::text

#endif
