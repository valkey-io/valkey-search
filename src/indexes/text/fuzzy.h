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
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "invasive_ptr.h"
#include "posting.h"
#include "rax_wrapper.h"
#include "src/utils/scanner.h"
#include "text.h"

namespace valkey_search::indexes::text {

// Vector of Unicode code points. Using a typedef to avoid repeating the
// template arguments everywhere in the fuzzy search DP matrix code.
using Codepoints = absl::InlinedVector<uint32_t, 32>;

// Fuzzy search using Damerau-Levenshtein distance on RadixTree
struct FuzzySearch {
  // Returns KeyIterators for all words within edit distance <= max_distance.
  // The DP matrix is sized by code point count (not bytes): é→è costs 1 edit.
  static absl::InlinedVector<Postings::KeyIterator,
                             kWordExpansionInlineCapacity>
  Search(const Rax& tree, absl::string_view pattern, size_t max_distance,
         uint32_t max_words) {
    absl::InlinedVector<indexes::text::Postings::KeyIterator,
                        kWordExpansionInlineCapacity>
        key_iterators;

    // Decode pattern to code points so the DP matrix is indexed per character.
    // The pattern reaches here already well-formed UTF-8. Both entry points
    // resolve malformed bytes upstream, compat-gated (>= 1.4.0 rejects with
    // InvalidArgumentError; < 1.4.0 substitutes U+FFFD so the term matches
    // nothing): client queries via FilterParser::Parse's upfront gate, and
    // inter-node requests via GRPCPredicateToPredicate in search_converter.cc.
    // kInvalidCp is therefore unreachable here, and the CHECK is a contract
    // assertion: if it fires, a caller delivered an unsanitized pattern, which
    // is a programming error.
    Codepoints pattern_cps;
    {
      utils::Scanner s(pattern);
      utils::Scanner::Char cp;
      while ((cp = s.NextUtf8()) != utils::Scanner::kEOF) {
        CHECK(cp != utils::Scanner::kInvalidCp)
            << "Fuzzy pattern contained invalid UTF-8 — the filter parser "
               "should have rejected or substituted it at the query boundary; "
               "this indicates a code path bypass";
        pattern_cps.push_back(cp);
      }
    }

    size_t pattern_len = pattern_cps.size();

    // Dynamic Programming matrix rows for Damerau-Levenshtein algorithm
    absl::InlinedVector<size_t, 32> prev_prev(pattern_len + 1);  // Row i-2
    absl::InlinedVector<size_t, 32> prev(pattern_len + 1);       // Row i-1
    absl::InlinedVector<size_t, 32> curr(pattern_len + 1);       // Row i

    // Initialize first row: distance from empty string to each pattern prefix.
    // Example: for pattern "race" (4 code points), first row is [0,1,2,3,4]
    for (size_t i = 0; i <= pattern_len; ++i) {
      prev[i] = i;
    }

    // Start traversal from root to explore all words in the tree
    auto iter = tree.GetPathIterator("");
    uint32_t word_count = 0;
    SearchRecursive(iter, pattern_cps, max_distance, "", 0 /*prev_tree_cp*/,
                    0 /*new_word_cp_count*/, 0 /*dp_byte_pos*/, prev_prev, prev,
                    curr, key_iterators, max_words, word_count);
    return key_iterators;
  }

 private:
  // Decode the code point at `pos` in `text` and advance `pos` past it.
  // Precondition: caller verified the full sequence is present (see
  // ExpectedLen) and `text` is validated stored content, so a malformed
  // sequence here is a contract violation.
  static uint32_t DecodeAndAdvance(absl::string_view text, size_t& pos) {
    utils::Scanner s(text.substr(pos));
    uint32_t cp = s.NextUtf8();
    CHECK(cp != utils::Scanner::kInvalidCp)
        << "Fuzzy decoded invalid UTF-8 from validated radix-tree edge";
    pos += s.LastUtf8ByteLen();
    return cp;
  }

  // SearchRecursive operates on code points throughout:
  //   - pattern_cps: pattern decoded to uint32_t code points
  //   - prev_tree_cp: last code point consumed from the tree (for
  //   transposition)
  //   - new_word_cp_count: code point count of new_word (for curr[0])
  //   - dp_byte_pos: byte offset in new_word up to which DP has consumed code
  //                  points. Bytes in [dp_byte_pos, new_word.size()) are a
  //                  partial UTF-8 sequence carried across the next edge —
  //                  required because radix-tree edges may split mid-codepoint.
  //   - DP matrix columns = pattern_cps.size() + 1
  static void SearchRecursive(
      Rax::PathIterator iter, const Codepoints& pattern_cps,
      size_t max_distance,
      std::string word,          // Current word being built (raw bytes)
      uint32_t prev_tree_cp,     // Previous code point (for transposition)
      size_t new_word_cp_count,  // Code point count of word
      size_t dp_byte_pos,        // Bytes of word already consumed by DP
      absl::InlinedVector<size_t, 32>&
          prev_prev,  // Row i-2 of DP matrix (for transposition)
      absl::InlinedVector<size_t, 32>&
          prev,  // Row i-1 of DP matrix (previous row)
      absl::InlinedVector<size_t, 32>&
          curr,  // Row i of DP matrix (current row being computed)
      absl::InlinedVector<indexes::text::Postings::KeyIterator,
                          kWordExpansionInlineCapacity>& key_iterators,
      uint32_t max_words, uint32_t& word_count) {
    size_t pattern_len = pattern_cps.size();

    // Iterate over children at current tree level
    while (!iter.Done() && word_count < max_words) {
      absl::string_view edge = iter.GetChildEdge();
      std::string new_word = word;
      new_word.append(edge.data(), edge.size());
      size_t edge_word_cp_count = new_word_cp_count;
      size_t edge_dp_byte_pos = dp_byte_pos;
      // Minimum edit distance in the current DP row after processing the edge.
      // Used for pruning: if min_dist > max_distance, skip entire subtree.
      // Initialize from the parent's prev row so an edge containing only
      // partial UTF-8 bytes (no DP step runs) still admits proper pruning;
      // for normal edges this initial value is overwritten by the first
      // DP step.
      size_t min_dist = *std::min_element(prev.begin(), prev.end());

      // SAVE STATE: prev_prev/prev/prev_tree_cp persist across siblings via
      // the function frame, so each child must restore parent state.
      auto saved_prev_prev = prev_prev;
      auto saved_prev = prev;
      uint32_t saved_prev_tree_cp = prev_tree_cp;

      // Drain complete code points from new_word; stop on a partial trailing
      // sequence so the missing bytes can join from the next edge.
      while (edge_dp_byte_pos < new_word.size()) {
        uint8_t b0 = static_cast<uint8_t>(new_word[edge_dp_byte_pos]);
        uint8_t need = utils::Scanner::ExpectedLen(b0);
        if (edge_dp_byte_pos + need > new_word.size()) {
          break;  // Partial UTF-8 sequence — wait for next edge to complete it.
        }
        uint32_t tree_cp = DecodeAndAdvance(new_word, edge_dp_byte_pos);
        ++edge_word_cp_count;

        // curr[0] = cost of deleting all code points of new_word so far.
        curr[0] = edge_word_cp_count;
        min_dist = curr[0];

        // DP matrix (example: "car" vs pattern "cra"):
        //       ""  "c"  "cr"  "cra"
        // ""   [ 0,  1,   2,   3  ]
        // "c"  [ 1,  0,   1,   2  ]
        // "ca" [ 2,  1,   1,   1  ]
        // "car"[ 3,  2,   1,   1  ]
        // curr[i] reads from these cells:
        //   [..prev[i-1]   prev[i] ]    <- diagonal (substitution), above
        //   (deletion)
        //   [..curr[i-1]   curr[i] ]    <- left (insertion), result
        //   [..prev_prev[i-2]      ]    <- 2-back diagonal (transposition)
        for (size_t i = 1; i <= pattern_len; ++i) {
          uint32_t pattern_cp = pattern_cps[i - 1];
          size_t cost = (tree_cp == pattern_cp) ? 0 : 1;

          curr[i] = std::min({
              prev[i] + 1,        // Deletion
              curr[i - 1] + 1,    // Insertion
              prev[i - 1] + cost  // Substitution
          });

          // Damerau-Levenshtein: transposition (swap of adjacent code points)
          if (i > 1 && edge_word_cp_count > 1 &&
              tree_cp == pattern_cps[i - 2] && pattern_cp == prev_tree_cp) {
            curr[i] = std::min(curr[i], prev_prev[i - 2] + cost);
          }

          min_dist = std::min(min_dist, curr[i]);
        }
        // Shift rows: curr → prev → prev_prev
        prev_prev.swap(prev);
        prev.swap(curr);
        prev_tree_cp = tree_cp;
      }

      // Pruning: skip subtree if minimum distance exceeds target edit distance.
      // Distance can only grow as more code points are appended, so once min
      // exceeds max no word under this subtree can match.
      if (min_dist > max_distance) {
        prev_prev = saved_prev_prev;
        prev = saved_prev;
        prev_tree_cp = saved_prev_tree_cp;
        iter.NextChild();
        continue;
      }

      // Descend to the child node at the end of this edge
      if (iter.CanDescend()) {
        auto child_iter = iter.DescendNew();
        // The edit distance is in prev row (after the swap above)
        if (child_iter.IsWord() && prev[pattern_len] <= max_distance) {
          key_iterators.emplace_back(
              child_iter.GetPostingsTarget()->GetKeyIterator());
          ++word_count;
          if (word_count >= max_words) {
            return;
          }
        }

        // Recurse into child's subtree
        if (child_iter.CanDescend()) {
          SearchRecursive(child_iter, pattern_cps, max_distance, new_word,
                          prev_tree_cp, edge_word_cp_count, edge_dp_byte_pos,
                          prev_prev, prev, curr, key_iterators, max_words,
                          word_count);
        }
      }

      // Restore state for next child
      prev_prev = saved_prev_prev;
      prev = saved_prev;
      prev_tree_cp = saved_prev_tree_cp;

      iter.NextChild();
    }
  }
};

}  // namespace valkey_search::indexes::text

#endif
