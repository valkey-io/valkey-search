/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_FUZZY_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_FUZZY_H_

#include <algorithm>
#include <cctype>
#include <string>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "absl/strings/string_view.h"
#include "posting.h"
#include "radix_tree.h"
#include "vmsdk/src/log.h"

namespace valkey_search::indexes::text {

// Fuzzy search using Damerau-Levenshtein distance on RadixTree
template <typename Target>
struct FuzzySearch {
  // Returns KeyIterators for all words within edit distance <= max_distance
  static std::vector<Postings::KeyIterator> Search(
      const RadixTree<Target>& tree, absl::string_view pattern,
      size_t max_distance) {
    std::vector<indexes::text::Postings::KeyIterator> key_iterators;

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
    // Remove - temporary log for the tree
    tree.DebugPrintTree("Fuzzy Search Tree");

    // Start traversal from root to explore all words in the tree
    auto iter = tree.GetPathIterator("");
    // Remove - temporary logs
    VMSDK_LOG(WARNING, nullptr) << "Starting fuzzy search for pattern='"
                                << pattern << "' max_distance=" << max_distance;
    VMSDK_LOG(WARNING, nullptr) << "Root iter.Done()='" << iter.Done()
                                << " iter.CanDescend()=" << iter.CanDescend();
    if (!iter.Done()) {
      VMSDK_LOG(WARNING, nullptr)
          << "Root iter.GetPath()='" << iter.GetPath() << "'";
    }

    SearchRecursive(iter, pattern, max_distance, "", '\0', prev_prev, prev,
                    curr, key_iterators);
    return key_iterators;
  }

 private:
  static void SearchRecursive(
      typename RadixTree<Target>::PathIterator iter, absl::string_view pattern,
      size_t max_distance,
      std::string word,  // Current word being built
      char prev_char,    // Previous character (for transposition detection)
      absl::InlinedVector<size_t, 32>&
          prev_prev,  // Row i-2 of DP matrix (for transposition)
      absl::InlinedVector<size_t, 32>&
          prev,  // Row i-1 of DP matrix (previous row)
      absl::InlinedVector<size_t, 32>&
          curr,  // Row i of DP matrix (current row being computed)
      std::vector<indexes::text::Postings::KeyIterator>& key_iterators) {
    // Iterate over siblings at current tree level
    while (!iter.Done()) {
      absl::string_view path = iter.GetPath();
      VMSDK_LOG(WARNING, nullptr)
          << "  path='" << path << "' iter.IsWord()=" << iter.IsWord()
          << " iter.CanDescend()=" << iter.CanDescend();
      std::string new_word = word;

      // SAVE STATE: Each sibling must start with same parent state
      auto saved_prev_prev = prev_prev;
      auto saved_prev = prev;
      char saved_prev_char = prev_char;

      // Process each character in the path
      for (char ch : path) {
        new_word += ch;
        char lower_ch = std::tolower(static_cast<unsigned char>(ch));

        curr[0] = new_word.length();
        size_t min_dist = curr[0];

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
          char lower_pat =
              std::tolower(static_cast<unsigned char>(pattern[i - 1]));
          size_t cost = (lower_ch == lower_pat) ? 0 : 1;

          curr[i] = std::min({
              prev[i] + 1,        // Deletion
              curr[i - 1] + 1,    // Insertion
              prev[i - 1] + cost  // Substitution
          });

          // Damerau-Levenshtein: transposition
          if (i > 1 && new_word.length() > 1 &&
              lower_ch ==
                  std::tolower(static_cast<unsigned char>(pattern[i - 2])) &&
              lower_pat ==
                  std::tolower(static_cast<unsigned char>(prev_char))) {
            curr[i] = std::min(curr[i], prev_prev[i - 2] + cost);
          }

          min_dist = std::min(min_dist, curr[i]);
        }
        // Shift rows for next character: curr becomes prev, prev becomes
        // prev_prev
        prev_prev.swap(prev);
        prev.swap(curr);
        prev_char = ch;
      }
      // Pruning: skip subtree if minimum distance exceeds target edit distance.
      // Since distance can only increase with more characters, if we're already
      // too far, no word in this subtree can match.
      size_t min_dist = *std::min_element(prev.begin(), prev.end());
      bool should_prune = (min_dist > max_distance);
      VMSDK_LOG(WARNING, nullptr) << "  should_prune='" << should_prune;
      if (should_prune) {
        // Restore state for next sibling (each sibling starts from same parent
        // state)
        prev_prev = saved_prev_prev;
        prev = saved_prev;
        prev_char = saved_prev_char;
        iter.Next();
        continue;
      }

      // Descend to the child node that this path leads to
      if (iter.CanDescend()) {
        auto child_iter = iter.DescendNew();
        VMSDK_LOG(WARNING, nullptr)
            << "     After descend: child_iter.IsWord()=" << child_iter.IsWord()
            << " new_word='" << new_word
            << "' distance=" << prev[pattern.length()];

        // Check if the child is a word
        if (child_iter.IsWord() && prev[pattern.length()] <= max_distance) {
          VMSDK_LOG(WARNING, nullptr) << "     ADDING RESULT: " << new_word;
          key_iterators.emplace_back(child_iter.GetTarget()->GetKeyIterator());
        }

        // Recursively explore children of the child
        if (child_iter.CanDescend()) {
          SearchRecursive(child_iter, pattern, max_distance, new_word,
                          prev_char, prev_prev, prev, curr, key_iterators);
        }
      }

      // Restore state for next sibling (each sibling starts from same parent
      // state)
      prev_prev = saved_prev_prev;
      prev = saved_prev;
      prev_char = saved_prev_char;

      iter.Next();
    }
  }
};

}  // namespace valkey_search::indexes::text

#endif
