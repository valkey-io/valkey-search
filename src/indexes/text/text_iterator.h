/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_WORD_ITERATOR_H_
#define VALKEY_SEARCH_INDEXES_TEXT_WORD_ITERATOR_H_

#include <cstddef>

#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

/* Base Class for all Text Search Iterators.
 * This contract holds for both keys and positions.
 * // Initializes the TextIterator and primes to the first match of
 * keys/positions. TextIterator::Init();
 * // Tells the caller site if there was no initial match upon init.
 * // Post init, it tells us whether there are keys remaining that can be
 * searched for. If (!DoneKeys) {
 *   // Access the current key match.
 *   auto key = CurrentKey();
 *   // Move to the next key which matches the criteria/s. This can result in
 * moving
 *   // till the end if there are no matches.
 *   NextKey();
 * }
 */
class TextIterator {
 public:
  virtual ~TextIterator() = default;

  // The field which the iterator was initialized to search for.
  virtual uint64_t FieldMask() const = 0;

  // virtual bool DoneWords() const = 0;
  // virtual std::string CurrentWord() const = 0;
  // virtual bool NextWord() = 0;

  // Key-level iteration
  // Returns true if there is a match (i.e. `CurrentKey()` is valid) provided
  // the TextIterator is used as described above. Use `CurrentKey()` to access
  // the matching document. Otherwise, returns false. Returns false if we have
  // exhausted all keys, and there are no more search results. In this case no
  // more calls should be made to `NextKey()`.
  virtual bool DoneKeys() const = 0;
  // Returns the current key.
  // PRECONDITION: !DoneKeys()
  virtual const InternedStringPtr& CurrentKey() const = 0;
  // Advances the key iteration until there is a match OR until we have
  // exhausted all keys. Returns true when there is a match wrt constraints
  // (e.g. field, position, inorder, slop, etc). Returns false otherwise. When
  // false is returned, `CurrentKey()` should no longer be accessed.
  // PRECONDITION: !DoneKeys()
  virtual bool NextKey() = 0;
  // Seeks forward to the first key >= target_key that matches all constraints.
  // Returns true if such a key is found, false if no more matching keys exist.
  // If current key is already >= target_key, returns true without changing
  // state. PRECONDITION: !DoneKeys(). This is intended to be used after a
  // previous call to NextKey().
  virtual bool SeekForwardKey(const InternedStringPtr& target_key) = 0;

  // Position-level iteration
  // Returns true if there is a match (i.e. `CurrentPosition()` is valid)
  // provided the TextIterator is used as described above. Use
  // `CurrentPosition()` to access the matching document. Otherwise, returns
  // false. Returns false if we have exhausted all keys, and there are no more
  // search results. In this case no more calls should be made to
  // `NextPosition()`.
  virtual bool DonePositions() const = 0;
  // Returns the current position.
  // Represents start and end. start == end in all TextIterators except for the
  // OR Proximity since it can contain a nested proximity block. PRECONDITION:
  // !DonePositions()
  virtual std::pair<uint32_t, uint32_t> CurrentPosition() const = 0;
  // Moves to the next position match. Returns true if there is one.
  // Otherwise, returns false if we have exhausted all positions.
  // PRECONDITION: !DonePositions()
  virtual bool NextPosition() = 0;
};

}  // namespace valkey_search::indexes::text
#endif


// x y z

// x1 y1 z1 => this combination has 5 keys that match, and it turns out to be all the keys returned from each iterator is common
// x2 y2 z2 => this combination has 5 keys that match, and it turns out to be all the keys returned from each iterator is common

// This adds up to 10.

// 2 words per text iterator
// 5 keys per word.
// Overall, 10 unique keys that can be searched for because I am saying every key is a valid common key amongst the text iterators.


// If it was just a wildcard prefix search, that is 10 calls to NextKey.


// For Option1:

// For proximity, this is 3 calls on initialization.
// We are already on the common key.
// Move from the old key, so 3 more calls
// repeat until all keys of this word combination are done.
// This means, we have 3 * 5 calls to NextKey for the current combination.
// There is a final call to NextKey in proximity to learn there are no more keys. But it 
// needs to happen only on one iterator.
// Which is 15 + 1 = 16.


// But we need to do this across all combinations of words.
// That is 2  x 2 x 2 = 8.

// 8 * 16 = 128 calls to NextKey.


// You need to think about Option2.

// There are X word iterators.
// Why do I need to search across all the keys??
// Can't I simply check one pass across all word iterators??
// Each word iterator has keys lexically sorted.

// For Option2:

// Psuedo code in wilcard.cc:
// bool NextKey() {
//   // Find word with smallest current_key across ALL words
//   size_t min_word_idx = FindMinKeyWordIndex();
//   current_key_ = word_states_[min_word_idx].current_key;
  
//   // Advance ONLY the word that provided this key
//   AdvanceWordToNextKey(word_states_[min_word_idx]);
// }


// So, FindMinKeyWordIndex have to look over all words (2)
// and loop over all keys (5 per word). meaning 10 calls to NextKey on every call.

// So, on init, there are 3 * 10 = 30 calls across all text iterators.

// Assuming there are 10 unique keys, we need to do this 10 times.
// So, 10 * 30 = 300 NextKey calls.

// Option 1 has much less calls to NextKey in this example.

// However, we can use a SeekForward on the KeyIterator in every call to NextKey.

// What this means is that, since NextKey of every text iterator (and the key iterator) returns results in a lexical order, 
// it will be possible to use a seek forward to jump skip "done keys".

// Let's say use the same example.

// It will take 10 calls to NextKey on init in each of the 3 text iterators.

// On the second round, it will take 1 seekforward, and 9 NextKey calls in each of 3 text iterators.
// On the third round, it will take 1 seekforward, and 8 NextKey calls.
// On the fourth round, it will take 1 seekforward, and 7 NextKey calls.
// On the fifth round, it will take 1 seekforward, and 6 NextKey calls.
// On the sixth round, it will take 1 seekforward, and 5 NextKey calls.
// On the seventh round, it will take 1 seekforward, and 4 NextKey calls.
// On the eighth round, it will take 1 seekforward, and 3 NextKey calls.
// On the ninth round, it will take 1 seekforward, and 2 NextKey calls.
// On the tenth round, it will take 1 seekforward, and 1 NextKey calls.

// So, 10 + 9 + 8 + 7 + 6 + 5 + 4 + 3 + 2 + 1 = 45 calls to NextKey.

// But this happens in each of the 3 Text Iterators.

// So, it is really (10 * 3) + (9 * 3) + (8 * 3) + (7 * 3) + (6 * 3) + (5 * 3) + (4 * 3) + (3 * 3) + (2 * 3) + (1 * 3) = 165 calls to NextKey.


// then there is one final call to learn there is no more keys, but needs to happen only on one textiterator.

// 165 + 1 = 166

// So, the total number of calls to NextKey is 300 + 120 = 420 calls to NextKey.
// Which is 10% less than the previous option.

// So, the previous option is better.



// Example 2:
// 2 text iterators

// 10 words per iterator
// 2 keys per word

// X1 Y1 = 2 keys
// X2 Y2 = 2 keys
// X3 Y3 = 2 keys
// ...
// X10 Y10 = 2 keys

// This is 20 keys.

// Option 1 Analysis:
// Per word combination:

// Initialization: 2 calls

// Process 2 keys: 2 × 2 = 4 calls

// Final check: 1 call

// Total per combination: 2 + 4 + 1 = 7 calls

// Total Option 1: 100 combinations × 7 = 700 NextKey calls

// Option 2 Analysis (with SeekForward):
// Initialization:

// Each iterator scans all keys: 2 × 20 = 40 NextKey calls

// Processing 20 unique keys:

// Round 1: 20 NextKey calls per iterator = 40 calls

// Round 2: 1 SeekForward + 19 NextKey = 20 calls per iterator = 40 calls

// Round 3: 1 SeekForward + 18 NextKey = 19 calls per iterator = 38 calls

// ...

// Round 20: 1 SeekForward + 1 NextKey = 2 calls per iterator = 4 calls

// Sum for rounds 1-20:

// (20 + 19 + 18 + ... + 1) × 2 = 210 × 2 = 420 NextKey calls

// Final check: 1 call

// Total Option 2: 40 + 420 + 1 = 461 NextKey calls

// Comparison:
// Option 1: 700 NextKey calls

// Option 2: 461 NextKey calls



// These are examples where there is a 100% success in alligning the
// text iterators to a common key. 
// we have not analyzed how it will be in the real world.




// Example 3 - continuing with the same example 2.
// but lets say that some of the combinations would have resulted in already
// seen keys. Option 1 does not have deduplication. Option 2 goes in lexical
// order and skips using seekforward.
// so lets say there are only 10 unique words.

// Example 3: Same setup but only 10 unique keys (with duplicates)
// Setup:

// 2 text iterators (A, B)

// 10 words per iterator

// 2 keys per word

// Total: 20 keys per iterator, but only 10 unique keys

// Word combinations: 10 × 10 = 100

// Many combinations will produce duplicate keys

// Option 1 Analysis (NO deduplication):
// Key insight: Option 1 processes ALL 100 word combinations, even if they produce duplicate keys.

// Per word combination:

// Initialization: 2 calls

// Process 2 keys: 2 × 2 = 4 calls

// Final check: 1 call

// Total per combination: 7 calls

// Total Option 1: 100 combinations × 7 = 700 NextKey calls

// Note: Option 1 returns duplicate keys to the user - it processes all combinations regardless of whether keys were seen before.

// Option 2 Analysis (with SeekForward, automatic deduplication):
// Key insight: Option 2 processes only the 10 unique keys due to lexical ordering and SeekForward.

// Initialization:

// Each iterator scans all keys: 2 × 20 = 40 NextKey calls

// Processing 10 unique keys (automatically deduplicated):

// Round 1: 20 NextKey calls per iterator = 40 calls

// Round 2: 1 SeekForward + 19 NextKey = 20 calls per iterator = 40 calls

// Round 3: 1 SeekForward + 18 NextKey = 19 calls per iterator = 38 calls

// ...

// Round 10: 1 SeekForward + 11 NextKey = 12 calls per iterator = 24 calls

// Sum for rounds 1-10:

// (20 + 19 + 18 + ... + 11) × 2 = 155 × 2 = 310 NextKey calls

// Final check: 1 call

// Total Option 2: 40 + 310 + 1 = 351 NextKey calls

// Comparison:
// Option 1: 700 NextKey calls (returns duplicates)

// Option 2: 351 NextKey calls (no duplicates)



// Everything below is AI generated and not reviewed:



// Example 4: Few Words, Many Keys
// Setup :

// 3 text iterators

// 2 words per iterator

// 100 keys per word

// Total: 200 keys per iterator

// Word combinations: 2³ = 8

// Assume all 200 keys are unique

// Option 1 Analysis :

// Per combination: 3 + (3×100) + 1 = 304 calls

// Total: 8 × 304 = 2,432 NextKey calls

// Option 2 Analysis (with SeekForward) :

// Initialization: 3 × 200 = 600 calls

// Processing 200 keys: (200 + 199 + ... + 1) × 3 = 20,100 × 3 = 60,300 calls

// Total: 600 + 60,300 = 60,900 NextKey calls

// Winner: Option 1 (25x better!)

// Example 5: Single Word per Iterator
// Setup :

// 4 text iterators

// 1 word per iterator (no wildcards)

// 50 keys per word

// Word combinations: 1⁴ = 1

// All keys are unique

// Option 1 Analysis :

// Single combination: 4 + (4×50) + 1 = 205 calls

// Total: 1 × 205 = 205 NextKey calls

// Option 2 Analysis (with SeekForward) :

// Initialization: 4 × 50 = 200 calls

// Processing 50 keys: (50 + 49 + ... + 1) × 4 = 1,275 × 4 = 5,100 calls

// Total: 200 + 5,100 = 5,300 NextKey calls

// Winner: Option 1 (26x better!)

// Example 6: High Key Density
// Setup :

// 2 text iterators

// 3 words per iterator

// 1000 keys per word

// Word combinations: 3² = 9

// All keys unique

// Option 1 Analysis :

// Per combination: 2 + (2×1000) + 1 = 2,003 calls

// Total: 9 × 2,003 = 18,027 NextKey calls

// Option 2 Analysis (with SeekForward) :

// Initialization: 2 × 3000 = 6,000 calls

// Processing 3000 keys: (3000 + 2999 + ... + 1) × 2 = 4,501,500 × 2 = 9,003,000 calls

// Total: 9,009,000 NextKey calls

// Winner: Option 1 (500x better!)

// Pattern: When Option 1 Wins
// Option 1 is more optimal when:

// Few word combinations (small number of words per iterator)

// Many keys per word (high key density)

// Most keys are unique (low duplication)

// Few text iterators (reduces combination explosion)

// Formula : Option 1 wins when:
// Word combinations × Keys per combination < Total keys × (Total keys + 1) / 2

// The key insight is that Option 2's merge-sort approach becomes very expensive when there are many keys to scan, while Option 1's word combination approach is efficient when combinations are few.













// ####################################################################


// Corrected version:



// Looking at the corrected analysis in your file, here are the 5 examples with the proper Option 2 logic:

// Example 1: Original (3 iterators, 2 words, 5 keys per word)
// Setup: 3 text iterators, 2 words per iterator, 5 keys per word = 10 total keys per iterator

// Option 1: 2³ = 8 word combinations × 16 calls per combination = 128 NextKey calls

// Option 2 (Corrected):

// Initialization: 3 × 1 = 3 calls

// Processing 10 unique keys: 10 × 1 = 10 calls

// Total: 13 NextKey calls

// Winner: Option 2 (10x better)

// Example 2: Many Words (2 iterators, 10 words, 2 keys per word)
// Setup: 2 text iterators, 10 words per iterator, 2 keys per word = 20 total keys per iterator

// Option 1: 10² = 100 word combinations × 7 calls per combination = 700 NextKey calls

// Option 2 (Corrected):

// Initialization: 2 × 1 = 2 calls

// Processing 20 unique keys: 20 × 1 = 20 calls

// Total: 22 NextKey calls

// Winner: Option 2 (32x better)

// Example 3: With Duplicates (same as Example 2, but only 10 unique keys)
// Setup: Same as Example 2, but many word combinations produce duplicate keys

// Option 1: 100 combinations × 7 calls = 700 NextKey calls (processes duplicates)

// Option 2 (Corrected):

// Initialization: 2 × 1 = 2 calls

// Processing 10 unique keys: 10 × 1 = 10 calls

// Total: 12 NextKey calls (automatic deduplication)

// Winner: Option 2 (58x better + no duplicates)

// Example 4: Few Words, Many Keys (3 iterators, 2 words, 100 keys per word)
// Setup: 3 text iterators, 2 words per iterator, 100 keys per word = 200 total keys per iterator

// Option 1: 2³ = 8 combinations × 304 calls per combination = 2,432 NextKey calls

// Option 2 (Corrected):

// Initialization: 3 × 1 = 3 calls

// Processing 200 unique keys: 200 × 1 = 200 calls

// Total: 203 NextKey calls

// Winner: Option 2 (12x better)

// Example 5: Single Word (4 iterators, 1 word, 50 keys per word)
// Setup: 4 text iterators, 1 word per iterator, 50 keys per word

// Option 1: 1¹ = 1 combination × 205 calls = 205 NextKey calls

// Option 2 (Corrected):

// Initialization: 4 × 1 = 4 calls

// Processing 50 unique keys: 50 × 1 = 50 calls

// Total: 54 NextKey calls

// Winner: Option 2 (4x better)

// Key Insight
// The corrected Option 2 analysis shows that merge-sort processes each key exactly once (O(total_keys)), making it dramatically more efficient than the exponential word combinations in Option 1. Option 2 wins in ALL practical scenarios due to:

// Linear complexity vs exponential

// Automatic deduplication

// Minimal overhead

// No redundant processing






// Here are the time complexity formulas for both options:

// Time Complexity Formulas
// Option 1: Word Combinations Approach
// T₁ = W^I × (I + I×K + 1)

// Where:

// W = words per iterator

// I = number of iterators

// K = keys per word

// W^I = total word combinations (exponential)

// (I + I×K + 1) = cost per combination (initialization + processing + final check)

// Simplified: O(W^I × I × K)

// Option 2: Merge-Sort Approach (Corrected)
// T₂ = I + U

// Where:

// I = number of iterators (initialization cost)

// U = total unique keys across all iterators

// Simplified: O(I + U)

// Complexity Analysis
// Option 1:

// Exponential in number of words per iterator: O(W^I)

// Linear in other factors: O(I × K)

// Overall: O(W^I × I × K)

// Option 2:

// Linear in number of iterators: O(I)

// Linear in unique keys: O(U)

// Overall: O(I + U)

// When Each Option Wins
// Option 2 wins when: I + U < W^I × I × K

// This simplifies to: W^I > (I + U)/(I × K)

// Option 1 wins when: Word combinations are extremely few and keys per word are extremely few.

// In practice, Option 2 wins in virtually all real-world scenarios because:

// W^I grows exponentially (2³=8, 3³=27, 4³=64, 5³=125...)

// I + U grows linearly

// The exponential term dominates quickly

// The crossover point is typically around W=1 or W=2 with very few keys per word.