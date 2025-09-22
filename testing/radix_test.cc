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

// Simple test target type
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

  // Helper: Add multiple words to tree
  void AddWords(const std::vector<std::pair<std::string, int>>& words) {
    for (const auto& [word, value] : words) {
      prefix_tree_->Mutate(word, [value](auto) { return TestTarget(value); });
    }
  }

  // Helper: Verify words exist with expected values
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

  // Helper: Verify words don't exist
  void VerifyWordsDeleted(const std::vector<std::string>& words) {
    for (const auto& word : words) {
      prefix_tree_->Mutate(word, [&word](auto existing) {
        EXPECT_FALSE(existing.has_value())
            << "Word '" << word << "' should be deleted";
        return existing;
      });
    }
  }

  // Helper: Delete multiple words
  void DeleteWords(const std::vector<std::string>& words) {
    for (const auto& word : words) {
      prefix_tree_->Mutate(
          word, [](auto) -> std::optional<TestTarget> { return std::nullopt; });
    }
  }

  // Helper: Verify iterator results
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

// Test core mutation operations: add, update, and basic tree structure
TEST_F(RadixTreeTest, CoreMutationOperations) {
  // Add first word (creates compressed path from root)
  prefix_tree_->Mutate("hello", [](auto existing) {
    EXPECT_FALSE(existing.has_value());
    return TestTarget(1);
  });

  // Update existing word
  prefix_tree_->Mutate("hello", [](auto existing) {
    EXPECT_TRUE(existing.has_value());
    EXPECT_EQ(existing->value, 1);
    return TestTarget(2);
  });

  // Verify final state
  VerifyWords({{"hello", 2}});
}

// Test branch node operations: existing and new children
TEST_F(RadixTreeTest, BranchNodeOperations) {
  // Create initial branching structure: ca->t/r, then add ca->n, then separate
  // dog branch
  AddWords({{"cat", 1}, {"car", 2}, {"can", 3}, {"dog", 4}});
  VerifyWords({{"cat", 1}, {"car", 2}, {"can", 3}, {"dog", 4}});
}

// Test all compressed node scenarios: extension, branching, and splitting
TEST_F(RadixTreeTest, CompressedNodeOperations) {
  // Test 1: Full match extension (hello -> helloworld)
  // Test 2: No match branching (add completely different word)
  // Test 3: Partial match splitting (testing -> test)
  AddWords({{"hello", 1},
            {"helloworld", 2},
            {"xyz", 3},
            {"testing", 4},
            {"test", 5}});
  VerifyWords({{"hello", 1},
               {"helloworld", 2},
               {"xyz", 3},
               {"testing", 4},
               {"test", 5}});
}

// Test edge cases: single character words creating specific branch patterns
TEST_F(RadixTreeTest, EdgeCases) {
  AddWords({{"a", 1}, {"b", 2}, {"ab", 3}});
  VerifyWords({{"a", 1}, {"b", 2}, {"ab", 3}});
}

TEST_F(RadixTreeTest, DeleteBasicCases) {
  // Case 1: Delete leaf node
  prefix_tree_->Mutate("hello", [](auto) { return TestTarget(1); });
  prefix_tree_->Mutate(
      "hello", [](auto) -> std::optional<TestTarget> { return std::nullopt; });
  prefix_tree_->Mutate("hello", [](auto existing) {
    EXPECT_FALSE(existing.has_value());
    return existing;
  });

  // Case 2: Delete non-existent node
  prefix_tree_->Mutate("world", [](auto existing) -> std::optional<TestTarget> {
    EXPECT_FALSE(existing.has_value());
    return std::nullopt;
  });

  // TODO: Enable when assertion handling is clarified
  EXPECT_DEATH(prefix_tree_->Mutate("", [](auto) { return TestTarget(1); }),
               ".*");
}

TEST_F(RadixTreeTest, DeleteStructuralCases) {
  // Case 1: Delete from branch node
  prefix_tree_->Mutate("cat", [](auto) { return TestTarget(1); });
  prefix_tree_->Mutate("car", [](auto) { return TestTarget(2); });
  prefix_tree_->Mutate("can", [](auto) { return TestTarget(3); });
  prefix_tree_->DebugPrintTree("Initial: cat, car, can");

  prefix_tree_->Mutate(
      "car", [](auto) -> std::optional<TestTarget> { return std::nullopt; });

  prefix_tree_->DebugPrintTree("After deleting 'car'");

  // Verify remaining structure
  auto iter = prefix_tree_->GetWordIterator("ca");
  std::vector<std::pair<std::string, int>> actual;
  while (!iter.Done()) {
    actual.emplace_back(std::string(iter.GetWord()), iter.GetTarget().value);
    iter.Next();
  }
  std::vector<std::pair<std::string, int>> expected = {{"can", 3}, {"cat", 1}};
  EXPECT_EQ(actual, expected);

  // Case 2: Delete from compressed node
  prefix_tree_->Mutate("testing", [](auto) { return TestTarget(4); });
  prefix_tree_->Mutate("test", [](auto) { return TestTarget(5); });
  prefix_tree_->DebugPrintTree("After adding testing/test");
  prefix_tree_->Mutate(
      "test", [](auto) -> std::optional<TestTarget> { return std::nullopt; });
  prefix_tree_->DebugPrintTree("After deleting 'test'");

  // Verify compressed node structure remains correct
  auto test_iter = prefix_tree_->GetWordIterator("test");
  EXPECT_FALSE(test_iter.Done());
  EXPECT_EQ(test_iter.GetWord(), "testing");
  EXPECT_EQ(test_iter.GetTarget().value, 4);
}

TEST_F(RadixTreeTest, DeleteCausingMerge) {
  // Test app/application -> delete app leaves just application (merge scenario)
  AddWords({{"app", 1}, {"application", 2}});
  DeleteWords({"app"});
  VerifyWords({{"application", 2}});
  VerifyWordsDeleted({"app"});
}

TEST_F(RadixTreeTest, DeleteComplexScenarios) {
  // Build complex tree: a->b->c->d/e, then delete in specific order to test
  // restructuring
  AddWords({{"a", 1}, {"ab", 2}, {"abc", 3}, {"abcd", 4}, {"abce", 5}});

  // Delete nodes in specific order to test various restructuring scenarios
  DeleteWords({"abc", "a", "abce"});

  // Verify final structure: should have ab, abcd remaining
  VerifyIterator("", {{"ab", 2}, {"abcd", 4}});
  VerifyWordsDeleted({"a", "abc", "abce"});
}

// Test complex scenarios: overlapping prefixes and special strings
TEST_F(RadixTreeTest, ComplexScenarios) {
  // Overlapping prefixes with multiple lengths: te->st->ing
  // Complex tree with multiple splits: app->lication
  // Special strings: long string and Unicode
  std::string long_string(1000, 'x');
  AddWords({{"te", 1},
            {"test", 2},
            {"testing", 3},
            {"app", 4},
            {"application", 5},
            {long_string, 6},
            {"こんにちは", 7}});
  VerifyWords({{"te", 1},
               {"test", 2},
               {"testing", 3},
               {"app", 4},
               {"application", 5},
               {long_string, 6},
               {"こんにちは", 7}});
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

  // Count word frequencies and add to tree
  std::map<std::string, int> word_counts;
  for (const auto& w : words) {
    word_counts[w]++;
  }

  // Add words to tree, incrementing count each time
  for (const auto& w : words) {
    prefix_tree_->Mutate(w, [](auto existing) {
      if (existing.has_value()) {
        return TestTarget(existing->value + 1);
      } else {
        return TestTarget(1);
      }
    });
  }

  // Iterate over all words and verify counts
  auto iter = prefix_tree_->GetWordIterator("");

  std::map<std::string, int> iterated_counts;
  while (!iter.Done()) {
    std::string word = std::string(iter.GetWord());
    int count = iter.GetTarget().value;

    // Ensure we haven't seen this word before
    EXPECT_EQ(iterated_counts.count(word), 0)
        << "Word '" << word << "' encountered multiple times during iteration";

    iterated_counts[word] = count;
    iter.Next();
  }

  // Verify word counts match
  EXPECT_EQ(iterated_counts, word_counts);
  EXPECT_GT(word_counts.size(), 100);  // Should have many unique words
}

TEST_F(RadixTreeTest, WordIteratorPrefixPartialMatch) {
  // Test specific prefix matching edge case: cat/can/testing/test
  AddWords({{"cat", 1}, {"can", 2}, {"testing", 4}, {"test", 5}});

  // Test "te" prefix - should only match test/testing
  VerifyIterator("te", {{"test", 5}, {"testing", 4}});

  // Test "ca" prefix - should only match can/cat
  VerifyIterator("ca", {{"can", 2}, {"cat", 1}});
}

TEST_F(RadixTreeTest, BranchToCompressedNodeConversion) {
  // Test all three compression scenarios when branch nodes are converted.
  // These test the complex logic in the deletion code that handles:
  // 1. parent_compressed && child_compressed - connect parent to
  // great-grandchild
  // 2. parent_compressed only - connect parent to grandchild
  // 3. child_compressed only - connect node to grandchild

  // ========================================================================
  // Scenario 1: parent_compressed && child_compressed
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

  // Delete "xabc" - this triggers parent_compressed && child_compressed case
  // It becomes the following after deleting "xabc":
  //                  [compressed]
  //              "xtest" |
  //                   [leaf] -> Target
  DeleteWords({"xabc"});
  prefix_tree_->DebugPrintTree(
      "Scenario 1 - After deletion: parent connected to great-grandchild");
  VerifyIterator("x", {{"xtest", 2}});
  VerifyWordsDeleted({"xabc"});

  // Reset for scenario 2
  prefix_tree_ = std::make_unique<RadixTree<TestTarget, false>>();

  // ========================================================================
  // Scenario 2: parent_compressed only
  // ========================================================================
  // Initial tree structure:
  //                  [compressed]
  //                 "cat" |
  //                   [branching]
  //                "s" /     \ "ch"
  //      Target <- [Leaf]   [Leaf] -> Target
  //          "" /           
  //   Target <- [leaf]           
  //
  // Words: "cats", "catcher"
  AddWords({{"cats", 3}, {"catcher", 4}});
  prefix_tree_->DebugPrintTree(
      "Scenario 2 - Initial: parent compressed, child branching");

  // Delete "cats" - this triggers parent_compressed only case
  // It becomes:
  //                  [compressed]
  //              "catcher" |
  //                   [leaf] -> Target
  DeleteWords({"cats"});
  prefix_tree_->DebugPrintTree(
      "Scenario 2 - After deletion: parent connected to grandchild");
  VerifyIterator("cat", {{"catcher", 4}});
  VerifyWordsDeleted({"cats"});

  // Reset for scenario 3
  prefix_tree_ = std::make_unique<RadixTree<TestTarget, false>>();

  // ========================================================================
  // Scenario 3: child_compressed only
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

  // Delete "dog" - this triggers child_compressed only case
  // It becomes:
  //                  [compressed]
  //              "runner" |
  //                   [leaf] -> Target
  DeleteWords({"dog"});
  prefix_tree_->DebugPrintTree(
      "Scenario 3 - After deletion: node connected to grandchild");
  VerifyIterator("", {{"runner", 6}});
  VerifyWordsDeleted({"dog"});
}

}  // namespace
}  // namespace valkey_search::indexes::text
