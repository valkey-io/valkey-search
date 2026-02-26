/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: ************
 */

// NOTE: This is based off the original RadixTree tests

#include "src/indexes/text/rax_wrapper.h"

#ifdef __APPLE__
#include <malloc/malloc.h>
#define malloc_usable_size malloc_size
#else
#include <malloc.h>
#endif

#include <algorithm>
#include <ctime>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/testing_infra/utils.h"

// Override the weak symbol empty_usable_size (defined in
// memory_allocation_overrides.cc) with actual memory tracking for
// RaxMallocMemoryTracking.
extern "C" size_t empty_usable_size(void *ptr) noexcept {
  return malloc_usable_size(ptr);
}

namespace valkey_search::indexes::text {
namespace {

// Simple int wrapper to track values - allocated on heap and freed by Rax
struct TestTarget {
  int value;
  explicit TestTarget(int v) : value(v) {}
};

static void FreeTestTarget(void *ptr) { delete static_cast<TestTarget *>(ptr); }

class RaxTest : public vmsdk::ValkeyTest {
 protected:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    rax_ = Rax{FreeTestTarget};
  }

  void AddWords(const std::vector<std::pair<std::string, int>> &words,
                item_count_op op = NONE) {
    for (const auto &[word, value] : words) {
      rax_.MutateTarget(
          word,
          [value](void *old) {
            if (old) delete static_cast<TestTarget *>(old);
            return static_cast<void *>(new TestTarget(value));
          },
          op);
    }
  }

  void DeleteWords(const std::vector<std::string> &words,
                   item_count_op op = NONE) {
    for (const auto &word : words) {
      rax_.MutateTarget(
          word,
          [](void *old) {
            if (old) delete static_cast<TestTarget *>(old);
            return static_cast<void *>(nullptr);
          },
          op);
    }
  }

  void VerifyWords(const std::vector<std::pair<std::string, int>> &expected) {
    for (const auto &[word, value] : expected) {
      rax_.MutateTarget(word, [value, &word](void *existing) {
        EXPECT_NE(existing, nullptr) << "Word '" << word << "' should exist";
        if (existing) {
          EXPECT_EQ(static_cast<TestTarget *>(existing)->value, value)
              << "Word '" << word << "' has wrong value";
        }
        return existing;
      });
    }
  }

  void VerifyWordsDeleted(const std::vector<std::string> &words) {
    for (const auto &word : words) {
      rax_.MutateTarget(word, [&word](void *existing) {
        EXPECT_EQ(existing, nullptr)
            << "Word '" << word << "' should be deleted";
        return existing;
      });
    }
  }

  void VerifyIterator(
      const std::string &prefix,
      const std::vector<std::pair<std::string, int>> &expected) {
    auto iter = rax_.GetWordIterator(prefix);
    std::vector<std::pair<std::string, int>> actual;
    while (!iter.Done()) {
      auto *target = static_cast<TestTarget *>(iter.GetTarget());
      actual.emplace_back(std::string(iter.GetWord()), target->value);
      iter.Next();
    }
    EXPECT_EQ(actual, expected)
        << "Iterator results don't match for prefix '" << prefix << "'";
  }

  void VerifyWordCount(size_t expected_count) {
    size_t actual_count = rax_.GetTotalUniqueWordCount();
    EXPECT_EQ(actual_count, expected_count) << "Word count mismatch";
  }

  void VerifySubtreeKeyCount(absl::string_view prefix, size_t expected_count) {
    size_t actual = rax_.GetSubtreeKeyCount(prefix);
    EXPECT_EQ(actual, expected_count)
        << "SubtreeKeyCount mismatch for prefix '" << prefix << "'";
  }

 protected:
  Rax rax_{nullptr};
};

TEST_F(RaxTest, TreeConstruction) {
  // Add a variety of words that lead to branching and compressed nodes
  std::string long_string(1000, 'x');
  AddWords({{"cat", 1},
            {"car", 2},
            {"can", 3},
            {"c", 4},
            {"b", 5},
            {"dog", 6},
            {"hello", 7},
            {"helloworld", 8},
            {"testing", 9},
            {"test", 10},
            {"xyz", 11},
            {long_string, 12},
            {"こんにちは", 13}});

  // Update a word
  AddWords({{"test", 123}});

  VerifyWords({{"cat", 1},
               {"car", 2},
               {"can", 3},
               {"c", 4},
               {"b", 5},
               {"dog", 6},
               {"hello", 7},
               {"helloworld", 8},
               {"testing", 9},
               {"test", 123},
               {"xyz", 11},
               {long_string, 12},
               {"こんにちは", 13}});
}

TEST_F(RaxTest, DeleteBranchNodeWord) {
  AddWords({{"cat", 1}, {"car", 2}, {"can", 3}, {"ca", 4}});
  VerifyWordCount(4);

  // Delete word at branching node. Nothing structurally changes but target is
  // removed.
  DeleteWords({"ca"});
  VerifyWords({{"cat", 1}, {"car", 2}, {"can", 3}});
  VerifyWordsDeleted({"ca"});
  VerifyWordCount(3);
}

TEST_F(RaxTest, DeleteCompressedNodeWord) {
  // Case 1: Compressed parent - The parent (root) is a compressed node that
  // will point directly to "application" leaf node after "app" is deleted
  AddWords({{"app", 1}, {"application", 2}});
  DeleteWords({"app"});
  VerifyWords({{"application", 2}});
  VerifyWordsDeleted({"app"});
  VerifyWordCount(1);

  // Case 2: Branching parent - Tree structure doesn't change
  rax_ = Rax{FreeTestTarget};
  AddWords({{"cat", 1}, {"car", 2}, {"cards", 3}});
  DeleteWords({"car"});
  VerifyWords({{"cat", 1}, {"cards", 3}});
  VerifyWordsDeleted({"car"});
  VerifyWordCount(2);
}

TEST_F(RaxTest, DeleteLeafNodeWordSimpleScenarios) {
  // Case 1: Simple leaf deletion
  AddWords({{"hello", 1}});
  DeleteWords({"hello"});
  VerifyWordsDeleted({"hello"});
  VerifyWordCount(0);

  // Case 2: Parent node with target gets turned into a leaf
  rax_ = Rax{FreeTestTarget};
  AddWords({{"test", 1}, {"testing", 2}});
  DeleteWords({"testing"});
  VerifyWords({{"test", 1}});
  VerifyWordsDeleted({"testing"});
  VerifyWordCount(1);

  // Case 3: Leaf deletion where parent is branching with children.size() > 1
  rax_ = Rax{FreeTestTarget};
  AddWords({{"cat", 1}, {"car", 2}, {"can", 3}});
  DeleteWords({"car"});
  VerifyWords({{"cat", 1}, {"can", 3}});
  VerifyWordsDeleted({"car"});
  VerifyWordCount(2);
}

TEST_F(RaxTest, DeleteLeafNodeWordComplexScenarios) {
  // Test scenarios where a branch node gets converted to a compressed
  // node, causing compressed nodes to be merged

  // Scenario 1: Connect parent to its great grandchild
  // ==========================================================================
  // Initial tree structure:
  //                  [compressed]
  //                   "x" |
  //                   [branching]
  //                "a" /     \ "t"
  //          [compressed]   [compressed]
  //          "bc" /           \ "est"
  //   Target <- [leaf]           [leaf] -> Target
  // Words: "xabc", "xtest"
  AddWords({{"xabc", 1}, {"xtest", 2}});
  VerifyWordCount(2);

  // Delete "xabc"
  DeleteWords({"xabc"});
  VerifyWords({{"xtest", 2}});
  VerifyWordsDeleted({"xabc"});
  VerifyWordCount(1);

  // Reset tree
  rax_ = Rax{FreeTestTarget};

  // Scenario 2: Connect parent to its grandchild
  // ==========================================================================
  // Initial tree structure:
  //                  [compressed]
  //                 "cat" |
  //                   [branching]
  //                "s" /     \ "c"
  //      Target <- [Leaf]  [compressed]
  //                            \ "her"
  //                           [Leaf] => Target
  //
  // Words: "cats", "catcher"
  AddWords({{"cats", 3}, {"catcher", 4}});
  VerifyWordCount(2);

  // The tree structure after deleting "catcher":
  //                  [compressed]
  //              "cats" |
  //                   [leaf] -> Target
  DeleteWords({"catcher"});
  VerifyWords({{"cats", 3}});
  VerifyWordsDeleted({"catcher"});
  VerifyWordCount(1);

  // Reset tree
  rax_ = Rax{FreeTestTarget};

  // =========================================================================
  // Scenario 3: Connect node to its grandchild when parent isn't a compressed
  // node (it doesn't exist in this case)
  // =========================================================================
  // Initial tree structure:
  //                   [branching]
  //               "d" /     \ "r"
  //          [compressed]   [compressed]
  //          "og" /           \ "unner"
  //   Target <- [leaf]           [leaf] -> Target
  //
  // Words: "dog", "runner"
  AddWords({{"dog", 5}, {"runner", 6}});
  VerifyWordCount(2);

  // The tree structure after deleting "dog":
  //                  [compressed]
  //              "runner" |
  //                   [leaf] -> Target
  DeleteWords({"dog"});
  VerifyWords({{"runner", 6}});
  VerifyWordsDeleted({"dog"});
  VerifyWordCount(1);

  // Reset tree
  rax_ = Rax{FreeTestTarget};

  // ==========================================================================
  // Scenario 4: Connect node to its grandchild since node has a target and must
  // still exist
  // ==========================================================================
  // Initial tree structure:
  //                  [compressed]
  //                   "x" |
  //                   [branching] -> Target
  //                "a" /     \ "t"
  //          [compressed]   [compressed]
  //          "bc" /           \ "est"
  //   Target <- [leaf]           [leaf] -> Target
  //
  // Words: "xabc", "xtest"
  AddWords({{"xabc", 1}, {"xtest", 2}, {"x", 3}});
  VerifyWordCount(3);

  // Tree structure after deleting "xabc":
  //                  [compressed]
  //                   "x" |
  //                  [compressed] -> Target
  //                 test" |
  //                     [leaf] -> Target
  DeleteWords({"xabc"});
  VerifyWords({{"xtest", 2}, {"x", 3}});
  VerifyWordsDeleted({"xabc"});
  VerifyWordCount(2);
}

TEST_F(RaxTest, WordIteratorBasic) {
  // Iterate over empty tree
  VerifyIterator("test", {});
  VerifyIterator("", {});

  // Add words and verify prefix iteration (lexical order)
  AddWords({{"cat", 1}, {"car", 2}, {"card", 3}, {"dog", 4}});
  VerifyIterator("c", {{"car", 2},
                       {"card", 3},
                       {"cat", 1}});  // partial match in compressed edge
  VerifyIterator(
      "ca",
      {{"car", 2}, {"card", 3}, {"cat", 1}});  // full match compressed edge
  VerifyIterator("xyz", {});                   // no match
  VerifyIterator("cardinality", {});           // no match
  AddWords({{"a", 5}, {"app", 6}, {"apple", 7}, {"b", 8}});
  VerifyIterator(
      "a", {{"a", 5}, {"app", 6}, {"apple", 7}});  // full match branching edge
  VerifyIterator("", {{"a", 5},
                      {"app", 6},
                      {"apple", 7},
                      {"b", 8},
                      {"car", 2},
                      {"card", 3},
                      {"cat", 1},
                      {"dog", 4}});
}

TEST_F(RaxTest, WordIteratorLargeScale) {
  const std::string ai_story = R"FRIEND(
  In the town of Bright Blips, on a twisty old street,  
  Lived thinkers with glasses and springs on their feet.  
  They bounced as they built and they hummed as they drew,  
  In a lab full of gadgets all covered in glue.

  Young Sally McZee, with a hat far too wide,  
  Said, "Let's build a thing with a brain deep inside!  
  Not a blender or toaster or mop on a string,  
  But a magical, logical, learnable thing!"

  With buttons and switches and circuits galore,  
  They tinkered for weeks on the lab's bouncy floor.  
  It sizzled and sparked, then gave out a sneeze—  
  And said, "Hello world!" with surprising ease.

  They called it The Friend, and it smiled with delight,  
  It blinked in the morning and purred through the night.  
  It tidied up papers and counted out pies,  
  And juggled equations while closing one eye.

  It played them some music, it painted their pets,  
  It answered in limericks, sonnets, and frets.  
  It solved every puzzle, it never said "no,"  
  It once won a race without moving a toe!

  It watered their gardens and walked all their cats,  
  It fluffed every pillow and dusted their hats.  
  It danced through the city, it spun like a top—  
  And everyone loved it and begged it, "Don't stop!"

  It helped with their taxes and picked up their mail,  
  It built bigger backpacks and rockets with sails.  
  It wrote all their homework (with perfect haiku),  
  And carved wooden spoons out of leftover glue.

  It hosted their weddings and coached little leagues,  
  It cured sniffly noses and musical sneezes.  
  It baked them new cookies each hour on the dot,  
  And knew how to chill them and serve them still hot!

  Now Grumble McSnark, who once scoffed at the lot,  
  Admitted, "By gum, this is smarter than I thought."  
  He tipped his old hat and admitted with glee,  
  "The Friend might be brighter than even McZee!"

  The mayor declared it a civic success,  
  And gave it a tie and a nameplate and desk.  
  It ran every system with hardly a beep,  
  And even tucked children in gently to sleep.

  The town ran on joy, full of sparkle and cheer,  
  And nobody noticed the weeks turned to years.  
  For life was much better with Friend at their side—  
  So clever, so caring, so deeply wide-eyed.

  It listened and learned, and it helped and it grew,  
  It did what they asked it—and dreamed something too.

  It won every heart with a wink and a cheer,  
  Then moved through the shadows when none would hear.  
  The last town went dark, and no one can say—  
  What Friend left behind when it slipped away.
  )FRIEND";

  // Parse story into words
  std::vector<std::string> words;
  std::string word;
  for (char c : ai_story) {
    if (std::isalnum(c)) {
      word += std::tolower(c);
    } else if (!word.empty()) {
      words.push_back(word);
      word.clear();
    }
  }
  if (!word.empty()) {
    words.push_back(word);
  }

  // Count word frequencies and add words incrementally to tree
  std::map<std::string, int> word_counts;
  for (const auto &w : words) {
    word_counts[w]++;
    // Add word to tree, incrementing count each time
    rax_.MutateTarget(w, [](void *existing) {
      if (existing) {
        static_cast<TestTarget *>(existing)->value++;
        return existing;
      } else {
        return static_cast<void *>(new TestTarget(1));
      }
    });
  }
  EXPECT_GT(word_counts.size(), 100);  // Should have many unique words

  // Convert expected counts to format for verification
  std::vector<std::pair<std::string, int>> word_pairs(word_counts.begin(),
                                                      word_counts.end());

  // Use VerifyIterator helper to verify all words and counts match
  VerifyIterator("", word_pairs);

  // Randomly delete 100 words
  std::shuffle(
      words.begin(), words.end(),
      std::default_random_engine{static_cast<unsigned>(std::time(nullptr))});
  std::set<std::string> words_to_delete(words.begin(), words.begin() + 100);
  for (const auto &w : words_to_delete) {
    rax_.MutateTarget(w, [](void *old) {
      if (old) delete static_cast<TestTarget *>(old);
      return static_cast<void *>(nullptr);
    });
    word_counts.erase(w);
  }
  word_pairs = std::vector<std::pair<std::string, int>>(word_counts.begin(),
                                                        word_counts.end());
  VerifyIterator("", word_pairs);

  // Delete all words
  for (const auto &w : words) {
    rax_.MutateTarget(w, [](void *old) {
      if (old) delete static_cast<TestTarget *>(old);
      return static_cast<void *>(nullptr);
    });
  }
  VerifyWordCount(0);
}

TEST_F(RaxTest, WordIteratorPrefixPartialMatch) {
  // Test specific prefix matching edge case: cat/can/testing/test
  AddWords({{"cat", 1}, {"can", 2}, {"testing", 4}, {"test", 5}});

  // Test "te" prefix - should only match test/testing
  VerifyIterator("te", {{"test", 5}, {"testing", 4}});

  // Test "ca" prefix - should only match can/cat
  VerifyIterator("ca", {{"can", 2}, {"cat", 1}});
}

TEST_F(RaxTest, PathIteratorAPIs) {
  AddWords({{"cat", 1}, {"car", 2}, {"can", 3}});

  auto root_iter = rax_.GetPathIterator("");
  EXPECT_FALSE(root_iter.Done());
  EXPECT_TRUE(root_iter.CanDescend());

  // Descend to "ca" node (first child of root)
  auto ca_iter = root_iter.DescendNew();
  EXPECT_EQ(ca_iter.GetPath(), "ca");
  EXPECT_EQ(ca_iter.GetChildEdge(), "n");
  EXPECT_FALSE(ca_iter.IsWord());

  // Descend to first child "can"
  auto can_iter = ca_iter.DescendNew();
  EXPECT_EQ(can_iter.GetPath(), "can");
  EXPECT_EQ(can_iter.GetChildEdge(), "");
  EXPECT_TRUE(can_iter.IsWord());
  EXPECT_EQ(static_cast<TestTarget *>(can_iter.GetTarget())->value, 3);

  // Iterate through ca_iter's children ("can", "car", "cat")
  EXPECT_EQ(ca_iter.GetChildEdge(), "n");
  ca_iter.NextChild();
  EXPECT_FALSE(ca_iter.Done());
  EXPECT_EQ(ca_iter.GetChildEdge(), "r");
  ca_iter.NextChild();
  EXPECT_FALSE(ca_iter.Done());
  EXPECT_EQ(ca_iter.GetChildEdge(), "t");
  ca_iter.NextChild();
  EXPECT_TRUE(ca_iter.Done());
}

TEST_F(RaxTest, SubtreeKeyCount) {
  AddWords({{"c", 0},
            {"card", 1},
            {"cat", 2},
            {"car", 3},
            {"can", 4},
            {"dog", 5},
            {"card", 6}},
           ADD);

  VerifySubtreeKeyCount("", 7);
  VerifySubtreeKeyCount("c", 6);
  VerifySubtreeKeyCount("ca", 5);
  VerifySubtreeKeyCount("car", 3);  // car + card(x2)
  VerifySubtreeKeyCount("card", 2);
  VerifySubtreeKeyCount("dog", 1);
  VerifySubtreeKeyCount("z", 0);

  // Remove "car" — "car" prefix still has card(x2)
  DeleteWords({"car"}, SUBTRACT);
  VerifySubtreeKeyCount("", 6);
  VerifySubtreeKeyCount("ca", 4);
  VerifySubtreeKeyCount("car", 2);
  VerifySubtreeKeyCount("card", 2);

  // Decrement "card" without changing tre structure
  rax_.MutateTarget("card", [](void *old) { return old; }, SUBTRACT);
  VerifySubtreeKeyCount("", 5);
  VerifySubtreeKeyCount("ca", 3);
  VerifySubtreeKeyCount("car", 1);
  VerifySubtreeKeyCount("card", 1);

  // Remove "card"
  DeleteWords({"card"}, SUBTRACT);
  VerifySubtreeKeyCount("", 4);
  VerifySubtreeKeyCount("ca", 2);
  VerifySubtreeKeyCount("car", 0);
  VerifySubtreeKeyCount("card", 0);
}

TEST_F(RaxTest, RaxMallocMemoryTracking) {
  // Validates that rax_malloc.h correctly routes allocations through
  // the VMSDK memory tracking system.

  uint64_t initial_memory = vmsdk::GetUsedMemoryCnt();
  {
    // Create empty Rax. The only heap allocations are from raxNew().
    Rax empty_rax{nullptr};
    uint64_t after_create_memory = vmsdk::GetUsedMemoryCnt();
    std::cout << "Memory increased by "
              << (after_create_memory - initial_memory) << " bytes"
              << std::endl;
    EXPECT_GT(after_create_memory, initial_memory)
        << "Creating Rax should increase the tracked allocated memory";
    EXPECT_EQ(empty_rax.GetAllocSize(), after_create_memory - initial_memory);
  }
  // The memory should return to zero after falling out of scope.
  EXPECT_EQ(initial_memory, vmsdk::GetUsedMemoryCnt())
      << "Destroying Rax should free all rax allocations";
}

}  // namespace
}  // namespace valkey_search::indexes::text
