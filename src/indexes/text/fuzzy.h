// /*
//  * Copyright (c) 2025, valkey-search contributors
//  * All rights reserved.
//  * SPDX-License-Identifier: BSD 3-Clause
//  *
//  */

// #ifndef _VALKEY_SEARCH_INDEXES_TEXT_FUZZY_H_
// #define _VALKEY_SEARCH_INDEXES_TEXT_FUZZY_H_

// namespace valkey_search::indexes::text {

// //
// // Fuzzy iterator that returns keys
// //
// // two stage iterator. Does FuzzyWordIterator then maps that into keys
// //
// struct FuzzyKeyIterator : public indexes::EntriesFetcherIteratorBase {
//   FuzzyIterator(indexes::Text& index, absl::string_view word, size_t distance);
//   virtual bool Done() const override;
//   virtual void Next() = override;
//   virtual const Key& operator*() const override;
//   virtual std::unique_ptr<WordIterator> clone() const override;
// };

// //
// // Takes a root word and iterates over the words that are a fuzzy match
// //
// struct FuzzyWordIterator : public WordIterator {
//   FuzzyWordIterator(indexes::Text& index, absl::string_view word,
//                     size_t distance);
//   virtual bool Done() const override;
//   virtual void Next() override;
//   virtual absl::string_view GetWord() const override;
//   virtual std::unique_ptr<WordIterator> Clone() const override;

//   absl::string_view operator*() const { return GetWord(); }
// };

// };

// }  // namespace valkey_search::indexes::text
// #endif

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
#include "radix_tree.h"
#include "absl/strings/string_view.h"

namespace valkey_search::indexes::text {

// Fuzzy search using Damerau-Levenshtein distance on RadixTree
template <typename Target>
struct FuzzySearch {
  // Search for words within max_distance edits of pattern
  // Returns vector of (word, target) pairs
  static std::vector<std::pair<std::string, Target>> Search(
      const RadixTree<Target>& tree, 
      absl::string_view pattern, 
      size_t max_distance) {
    
    std::vector<std::pair<std::string, Target>> results;
    std::vector<size_t> prev_prev(pattern.length() + 1);  // we need this for transposition
    std::vector<size_t> prev(pattern.length() + 1);      // we need this for replacemnt and instertion ops
    std::vector<size_t> curr(pattern.length() + 1);
    
    // Initialize first row: cost of inserting pattern characters
    // For a search string like "race" , the first row would be [0 1 2 3 4]
    // where each cell represents the cost of charcter insertion to an empty string.
    for (size_t i = 0; i <= pattern.length(); ++i) {
      prev[i] = i;
    }
    
    // get path iterator from root
    auto iter = tree.GetPathIterator("");
    std::cout << "DEBUG: Starting fuzzy search for pattern='" << pattern << "' max_distance=" << max_distance << std::endl;
    std::cout << "DEBUG: Root iter.Done()=" << iter.Done() << " iter.CanDescend()=" << iter.CanDescend() << std::endl;
    if (!iter.Done()) {
      std::cout << "DEBUG: Root iter.GetPath()='" << iter.GetPath() << "'" << std::endl;
    }
    
    SearchRecursive(iter, pattern, max_distance, "", '\0', 
                  prev_prev, prev, curr, results);
    
    std::cout << "DEBUG: Search complete, found " << results.size() << " results" << std::endl;
    return results;
  }


 private:
static void SearchRecursive(
    typename RadixTree<Target>::PathIterator iter,
    absl::string_view pattern, 
    size_t max_distance,
    std::string word,  // Current word being built 
    char prev_char,    // Previous character (for transposition detection)
    std::vector<size_t>& prev_prev,  // Row i-2 of DP matrix (for transposition)
    std::vector<size_t>& prev,       // Row i-1 of DP matrix (previous row)
    std::vector<size_t>& curr,       // Row i of DP matrix (current row being computed)
    std::vector<std::pair<std::string, Target>>& results) {
  // ITERATE OVER SIBLINGS at current tree level
  while (!iter.Done()) {
    absl::string_view path = iter.GetPath();
    std::cout << "DEBUG:   path='" << path << "' iter.IsWord()=" << iter.IsWord() << " iter.CanDescend()=" << iter.CanDescend() << std::endl;
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
      
      for (size_t i = 1; i <= pattern.length(); ++i) {
        char lower_pat = std::tolower(
            static_cast<unsigned char>(pattern[i - 1]));
        size_t cost = (lower_ch == lower_pat) ? 0 : 1;
        
        curr[i] = std::min({
            prev[i] + 1,           // Deletion
            curr[i - 1] + 1,       // Insertion
            prev[i - 1] + cost     // Substitution
        });
        
        // Damerau-Levenshtein: transposition
        if (i > 1 && new_word.length() > 1 && 
            lower_ch == std::tolower(
                static_cast<unsigned char>(pattern[i - 2])) &&
            lower_pat == std::tolower(
                static_cast<unsigned char>(prev_char))) {
          curr[i] = std::min(curr[i], prev_prev[i - 2] + cost);
        }
        
        min_dist = std::min(min_dist, curr[i]);
      }
      
      prev_prev.swap(prev);
      prev.swap(curr);
      prev_char = ch;
    }
    // NOW check if we should prune this subtree
    size_t min_dist = *std::min_element(prev.begin(), prev.end());
    bool should_prune = (min_dist > max_distance);
    std::cout << "DEBUG:   should_prune=" << should_prune << std::endl;
    if (should_prune) {
      // Restore state before moving to next sibling
      prev_prev = saved_prev_prev;
      prev = saved_prev;
      prev_char = saved_prev_char;
      iter.Next();
      continue;
    }
    
    // Descend to the child node that this path leads to
    if (iter.CanDescend()) {
      auto child_iter = iter.DescendNew();
      std::cout << "DEBUG:     After descend: child_iter.IsWord()=" << child_iter.IsWord() 
                << " new_word='" << new_word << "' distance=" << prev[pattern.length()] << std::endl;
      
      // Check if the child is a word
      if (child_iter.IsWord() && prev[pattern.length()] <= max_distance) {
        std::cout << "DEBUG:     ADDING RESULT: " << new_word << std::endl;
        results.emplace_back(new_word, child_iter.GetTarget());
      }
      
      // Recursively explore children of the child
      if (child_iter.CanDescend()) {
        SearchRecursive(child_iter, pattern, max_distance,
                       new_word, prev_char, prev_prev, prev, curr, results);
      }
    }

    // Restore parent state for next sibling
    prev_prev = saved_prev_prev;
    prev = saved_prev;
    prev_char = saved_prev_char;
    
    iter.Next();
  }
}
};

}  // namespace valkey_search::indexes::text

#endif
