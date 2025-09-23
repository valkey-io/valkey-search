/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: ************
 */

#include <map>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "src/indexes/text/radix_tree.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::indexes::text {
namespace {

struct TestTarget {
  int value;
  explicit TestTarget(int v) : value(v) {}
  bool operator==(const TestTarget& other) const {
    return value == other.value;
  }
};

class RadixTreeTest : public vmsdk::ValkeyTest {
 protected:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    prefix_tree_ = std::make_unique<RadixTree<TestTarget, false>>();
  }

  void AddWords(const std::vector<std::pair<std::string, int>>& words) {
    for (const auto& [word, value] : words) {
      prefix_tree_->Mutate(word, [value](auto) { return TestTarget(value); });
    }
  }

  void DeleteWords(const std::vector<std::string>& words) {
    for (const auto& word : words) {
      prefix_tree_->Mutate(
          word, [](auto) -> std::optional<TestTarget> { return std::nullopt; });
    }
  }

  void VerifyWords(const std::vector<std::pair<std::string, int>>& expected) {
    for (const auto& [word, value] : expected) {
      prefix_tree_->Mutate(word, [value, word](auto existing) {
        EXPECT_TRUE(existing.has_value())
            << "Word '" << word << "' should exist";
        EXPECT_EQ(existing->value, value)
            << "Word '" << word << "' has wrong value";
        return existing;
      });
    }
  }

  void VerifyWordsDeleted(const std::vector<std::string>& words) {
    for (const auto& word : words) {
      prefix_tree_->Mutate(word, [&word](auto existing) {
        EXPECT_FALSE(existing.has_value())
            << "Word '" << word << "' should be deleted";
        return existing;
      });
    }
  }

  void VerifyIterator(
      const std::string& prefix,
      const std::vector<std::pair<std::string, int>>& expected) {
    auto iter = prefix_tree_->GetWordIterator(prefix);
    std::vector<std::pair<std::string, int>> actual;
    while (!iter.Done()) {
      actual.emplace_back(std::string(iter.GetWord()), iter.GetTarget().value);
      iter.Next();
    }
    EXPECT_EQ(actual, expected)
        << "Iterator results don't match for prefix '" << prefix << "'";
  }

  std::unique_ptr<RadixTree<TestTarget, false>> prefix_tree_;
};

TEST_F(RadixTreeTest, TreeConstruction) {
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

TEST_F(RadixTreeTest, DeleteBranchNodeWord) {
  AddWords({{"cat", 1}, {"car", 2}, {"can", 3}, {"ca", 4}});

  // Delete word at branching node. Nothing structurally changes.
  DeleteWords({"ca"});
  VerifyWords({{"cat", 1}, {"car", 2}, {"can", 3}});
  VerifyWordsDeleted({"ca"});
}

TEST_F(RadixTreeTest, DeleteCompressedNodeWord) {
  // Case 1: Compressed parent - The parent (root) is a compressed node that
  // will point directly to "application" leaf node after "app" is deleted
  AddWords({{"app", 1}, {"application", 2}});
  DeleteWords({"app"});
  VerifyWords({{"application", 2}});
  VerifyWordsDeleted({"app"});

  // Case 2: Branching parent - Tree structure doesn't change
  prefix_tree_ = std::make_unique<RadixTree<TestTarget, false>>();
  AddWords({{"cat", 1}, {"car", 2}, {"cards", 3}});
  DeleteWords({"car"});
  VerifyWords({{"cat", 1}, {"cards", 3}});
  VerifyWordsDeleted({"car"});
}

TEST_F(RadixTreeTest, DeleteLeafNodeWordSimpleScenarios) {
  // Case 1: Simple leaf deletion
  AddWords({{"hello", 1}});
  DeleteWords({"hello"});
  VerifyWordsDeleted({"hello"});

  // Case 2: Parent node with target gets turned into a leaf
  prefix_tree_ = std::make_unique<RadixTree<TestTarget, false>>();
  AddWords({{"test", 1}, {"testing", 2}});
  DeleteWords({"testing"});
  VerifyWords({{"test", 1}});
  VerifyWordsDeleted({"testing"});

  // Case 3: Leaf deletion where parent is branching with children.size() > 1
  prefix_tree_ = std::make_unique<RadixTree<TestTarget, false>>();
  AddWords({{"cat", 1}, {"car", 2}, {"can", 3}});
  DeleteWords({"car"});
  VerifyWords({{"cat", 1}, {"can", 3}});
  VerifyWordsDeleted({"car"});
}

TEST_F(RadixTreeTest, DeleteLeafNodeWordComplexScenarios) {
  // Test all three scenarios where a branch node gets converted to a compressed
  // node. parent_compressed = the branch nodes parent is a compressed node
  // child_compressed = the branch nodes child is a compressed node

  // ========================================================================
  // Scenario 1: parent_compressed && child_compressed (connect parent to its
  // great grandchild)
  // ========================================================================
  // Initial tree structure:
  //                  [compressed]
  //                   "x" |
  //                   [branching]
  //                "a" /     \ "t"
  //          [compressed]   [compressed]
  //          "bc" /           \ "est"
  //   Target <- [leaf]           [leaf] -> Target
  //
  // Words: "xabc", "xtest"
  AddWords({{"xabc", 1}, {"xtest", 2}});
  prefix_tree_->DebugPrintTree(
      "Scenario 1 - Initial: both parent and child compressed");

  // Tree structure after deleting "xabc":
  //                  [compressed]
  //              "xtest" |
  //                   [leaf] -> Target
  DeleteWords({"xabc"});
  prefix_tree_->DebugPrintTree(
      "Scenario 1 - After deletion: parent connected to great-grandchild");
  VerifyWords({{"xtest", 2}});
  VerifyWordsDeleted({"xabc"});

  // Reset tree
  prefix_tree_ = std::make_unique<RadixTree<TestTarget, false>>();

  // ========================================================================
  // Scenario 2: parent_compressed (connect parent to its grandchild)
  // ========================================================================
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
  prefix_tree_->DebugPrintTree(
      "Scenario 2 - Initial: parent compressed, child branching");

  // The tree structure after deleting "catcher":
  //                  [compressed]
  //              "cats" |
  //                   [leaf] -> Target
  DeleteWords({"catcher"});
  prefix_tree_->DebugPrintTree(
      "Scenario 2 - After deletion: parent connected to grandchild");
  VerifyWords({{"cats", 3}});
  VerifyWordsDeleted({"catcher"});

  // Reset tree
  prefix_tree_ = std::make_unique<RadixTree<TestTarget, false>>();

  // ========================================================================
  // Scenario 3: child_compressed (connect node to its grandchild)
  // ========================================================================
  // Initial tree structure:
  //                   [branching]
  //               "d" /     \ "r"
  //          [compressed]   [compressed]
  //          "og" /           \ "unner"
  //   Target <- [leaf]           [leaf] -> Target
  //
  // Words: "dog", "runner"
  AddWords({{"dog", 5}, {"runner", 6}});
  prefix_tree_->DebugPrintTree(
      "Scenario 3 - Initial: parent branching, child compressed");

  // The tree structure after deleting "dog":
  //                  [compressed]
  //              "runner" |
  //                   [leaf] -> Target
  DeleteWords({"dog"});
  prefix_tree_->DebugPrintTree(
      "Scenario 3 - After deletion: node connected to grandchild");
  VerifyWords({{"runner", 6}});
  VerifyWordsDeleted({"dog"});
}

// Test WordIterator functionality
TEST_F(RadixTreeTest, WordIteratorBasic) {
  // Create tree: cat/car/card/dog, test "ca" prefix iteration (lexical order:
  // car, card, cat)
  AddWords({{"cat", 1}, {"car", 2}, {"card", 3}, {"dog", 4}});
  VerifyIterator("ca", {{"car", 2}, {"card", 3}, {"cat", 1}});
}

TEST_F(RadixTreeTest, WordIteratorEmpty) {
  // Test iterator on empty tree
  auto iter = prefix_tree_->GetWordIterator("test");
  EXPECT_TRUE(iter.Done());
}

TEST_F(RadixTreeTest, WordIteratorNoMatch) {
  prefix_tree_->Mutate("hello", [](auto) { return TestTarget(1); });

  // Test iterator with non-matching prefix
  auto iter = prefix_tree_->GetWordIterator("world");
  EXPECT_TRUE(iter.Done());
}

TEST_F(RadixTreeTest, WordIteratorSingleWord) {
  prefix_tree_->Mutate("test", [](auto) { return TestTarget(42); });

  auto iter = prefix_tree_->GetWordIterator("test");
  EXPECT_FALSE(iter.Done());
  EXPECT_EQ(iter.GetWord(), "test");
  EXPECT_EQ(iter.GetTarget().value, 42);

  iter.Next();
  EXPECT_TRUE(iter.Done());
}

TEST_F(RadixTreeTest, WordIteratorCompressedPaths) {
  // Test with compressed paths: testing/test/tester with "test" prefix
  AddWords({{"testing", 1}, {"test", 2}, {"tester", 3}});
  VerifyIterator("test", {{"test", 2}, {"tester", 3}, {"testing", 1}});
}

TEST_F(RadixTreeTest, WordIteratorRootPrefix) {
  // Test iterator with empty prefix (should get all words in lexical order)
  AddWords({{"a", 1}, {"b", 2}, {"c", 3}});
  VerifyIterator("", {{"a", 1}, {"b", 2}, {"c", 3}});
}

TEST_F(RadixTreeTest, WordIteratorComplexTree) {
  // Build complex tree: app/application/apple/apply/a, test "app" prefix
  AddWords(
      {{"app", 1}, {"application", 2}, {"apple", 3}, {"apply", 4}, {"a", 5}});
  VerifyIterator("app",
                 {{"app", 1}, {"apple", 3}, {"application", 2}, {"apply", 4}});
}

TEST_F(RadixTreeTest, WordIteratorLargeScale) {
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
  for (const auto& w : words) {
    word_counts[w]++;
    // Add word to tree, incrementing count each time
    prefix_tree_->Mutate(w, [](auto existing) {
      if (existing.has_value()) {
        return TestTarget(existing->value + 1);
      } else {
        return TestTarget(1);
      }
    });
  }
  EXPECT_GT(word_counts.size(), 100);  // Should have many unique words

  // Convert expected counts to format for verification
  std::vector<std::pair<std::string, int>> word_pairs(word_counts.begin(),
                                                      word_counts.end());

  // Use VerifyIterator helper to verify all words and counts match
  VerifyIterator("", word_pairs);
}

TEST_F(RadixTreeTest, WordIteratorPrefixPartialMatch) {
  // Test specific prefix matching edge case: cat/can/testing/test
  AddWords({{"cat", 1}, {"can", 2}, {"testing", 4}, {"test", 5}});

  // Test "te" prefix - should only match test/testing
  VerifyIterator("te", {{"test", 5}, {"testing", 4}});

  // Test "ca" prefix - should only match can/cat
  VerifyIterator("ca", {{"can", 2}, {"cat", 1}});
}

}  // namespace
}  // namespace valkey_search::indexes::text
