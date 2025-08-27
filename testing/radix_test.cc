/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: ************
 */

#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "src/indexes/text/radix_tree.h"

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

class RadixTreeTest : public testing::Test {
 protected:
  void SetUp() override {
    prefix_tree_ = std::make_unique<RadixTree<TestTarget, false>>();
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
  prefix_tree_->Mutate("hello", [](auto existing) {
    EXPECT_TRUE(existing.has_value());
    EXPECT_EQ(existing->value, 2);
    return existing;
  });
}

// Test branch node operations: existing and new children
TEST_F(RadixTreeTest, BranchNodeOperations) {
  // Create initial branching structure
  prefix_tree_->Mutate("cat", [](auto) { return TestTarget(1); });
  prefix_tree_->Mutate("car", [](auto) { return TestTarget(2); });

  // Add to existing branch (same prefix 'ca')
  prefix_tree_->Mutate("can", [](auto) { return TestTarget(3); });

  // Add completely new branch
  prefix_tree_->Mutate("dog", [](auto) { return TestTarget(4); });

  // Verify all branches work correctly
  std::vector<std::pair<std::string, int>> expected = {
      {"cat", 1}, {"car", 2}, {"can", 3}, {"dog", 4}};

  for (const auto& [word, value] : expected) {
    prefix_tree_->Mutate(word, [value](auto existing) {
      EXPECT_TRUE(existing.has_value());
      EXPECT_EQ(existing->value, value);
      return existing;
    });
  }
}

// Test all compressed node scenarios: extension, branching, and splitting
TEST_F(RadixTreeTest, CompressedNodeOperations) {
  // Test 1: Full match extension (hello -> helloworld)
  prefix_tree_->Mutate("hello", [](auto) { return TestTarget(1); });
  prefix_tree_->Mutate("helloworld", [](auto) { return TestTarget(2); });

  // Test 2: No match branching (add completely different word)
  prefix_tree_->Mutate("xyz", [](auto) { return TestTarget(3); });

  // Test 3: Partial match splitting (testing -> test)
  prefix_tree_->Mutate("testing", [](auto) { return TestTarget(4); });
  prefix_tree_->Mutate("test", [](auto) { return TestTarget(5); });

  // Verify all words exist with correct values
  std::vector<std::pair<std::string, int>> expected = {
      {"hello", 1}, {"helloworld", 2}, {"xyz", 3}, {"testing", 4}, {"test", 5}};

  for (const auto& [word, value] : expected) {
    prefix_tree_->Mutate(word, [value](auto existing) {
      EXPECT_TRUE(existing.has_value());
      EXPECT_EQ(existing->value, value);
      return existing;
    });
  }
}

// Test edge cases: single character words
TEST_F(RadixTreeTest, EdgeCases) {
  prefix_tree_->Mutate("a", [](auto) { return TestTarget(1); });
  prefix_tree_->Mutate("b", [](auto) { return TestTarget(2); });
  prefix_tree_->Mutate("ab", [](auto) { return TestTarget(3); });

  std::vector<std::pair<std::string, int>> expected = {
      {"a", 1}, {"b", 2}, {"ab", 3}};

  for (const auto& [word, value] : expected) {
    prefix_tree_->Mutate(word, [value](auto existing) {
      EXPECT_TRUE(existing.has_value());
      EXPECT_EQ(existing->value, value);
      return existing;
    });
  }
}

TEST_F(RadixTreeTest, DISABLED_UnimplementedFeatures) {
  // TODO: Enable when deletion is implemented
  prefix_tree_->Mutate("test", [](auto) { return TestTarget(1); });
  EXPECT_DEATH(
      prefix_tree_->Mutate("test",
                           [](auto existing) -> std::optional<TestTarget> {
                             return std::nullopt;  // Delete - not implemented
                           }),
      ".*");

  // TODO: Enable when assertion handling is clarified
  EXPECT_DEATH(prefix_tree_->Mutate("", [](auto) { return TestTarget(1); }),
               ".*");
}

// Test complex scenarios: overlapping prefixes and special strings
TEST_F(RadixTreeTest, ComplexScenarios) {
  // Overlapping prefixes with multiple lengths
  prefix_tree_->Mutate("te", [](auto) { return TestTarget(1); });
  prefix_tree_->Mutate("test", [](auto) { return TestTarget(2); });
  prefix_tree_->Mutate("testing", [](auto) { return TestTarget(3); });

  // Complex tree with multiple splits
  prefix_tree_->Mutate("app", [](auto) { return TestTarget(4); });
  prefix_tree_->Mutate("application", [](auto) { return TestTarget(5); });

  // Special strings: long string and Unicode
  std::string long_string(1000, 'x');
  prefix_tree_->Mutate(long_string, [](auto) { return TestTarget(6); });
  prefix_tree_->Mutate("こんにちは", [](auto) { return TestTarget(7); });

  std::vector<std::pair<std::string, int>> expected = {
      {"te", 1},          {"test", 2},      {"testing", 3},   {"app", 4},
      {"application", 5}, {long_string, 6}, {"こんにちは", 7}};

  for (const auto& [word, value] : expected) {
    prefix_tree_->Mutate(word, [value](auto existing) {
      EXPECT_TRUE(existing.has_value());
      EXPECT_EQ(existing->value, value);
      return existing;
    });
  }
}

// Test WordIterator functionality
TEST_F(RadixTreeTest, WordIteratorBasic) {
  // Add words to tree
  prefix_tree_->Mutate("cat", [](auto) { return TestTarget(1); });
  prefix_tree_->Mutate("car", [](auto) { return TestTarget(2); });
  prefix_tree_->Mutate("card", [](auto) { return TestTarget(3); });
  prefix_tree_->Mutate("dog", [](auto) { return TestTarget(4); });

  // Test iterator with "ca" prefix
  auto iter = prefix_tree_->GetWordIterator("ca");
  EXPECT_FALSE(iter.Done());

  // Should iterate in lexical order: car, card, cat
  std::vector<std::pair<std::string, int>> expected = {
      {"car", 2}, {"card", 3}, {"cat", 1}};

  std::vector<std::pair<std::string, int>> actual;
  while (!iter.Done()) {
    std::cout << iter.GetWord() << std::endl;
    std::cout << iter.GetTarget().value << std::endl;
    actual.emplace_back(std::string(iter.GetWord()), iter.GetTarget().value);
    iter.Next();
  }

  EXPECT_EQ(actual, expected);
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
  // Test with compressed paths
  prefix_tree_->Mutate("testing", [](auto) { return TestTarget(1); });
  prefix_tree_->Mutate("test", [](auto) { return TestTarget(2); });
  prefix_tree_->Mutate("tester", [](auto) { return TestTarget(3); });

  auto iter = prefix_tree_->GetWordIterator("test");

  std::vector<std::pair<std::string, int>> expected = {
      {"test", 2}, {"tester", 3}, {"testing", 1}};

  std::vector<std::pair<std::string, int>> actual;
  while (!iter.Done()) {
    actual.emplace_back(std::string(iter.GetWord()), iter.GetTarget().value);
    iter.Next();
  }

  EXPECT_EQ(actual, expected);
}

TEST_F(RadixTreeTest, WordIteratorRootPrefix) {
  // Test iterator with empty prefix (should get all words)
  prefix_tree_->Mutate("a", [](auto) { return TestTarget(1); });
  prefix_tree_->Mutate("b", [](auto) { return TestTarget(2); });
  prefix_tree_->Mutate("c", [](auto) { return TestTarget(3); });

  auto iter = prefix_tree_->GetWordIterator("");

  std::vector<std::pair<std::string, int>> expected = {
      {"a", 1}, {"b", 2}, {"c", 3}};

  std::vector<std::pair<std::string, int>> actual;
  while (!iter.Done()) {
    actual.emplace_back(std::string(iter.GetWord()), iter.GetTarget().value);
    iter.Next();
  }

  EXPECT_EQ(actual, expected);
}

TEST_F(RadixTreeTest, WordIteratorComplexTree) {
  // Build complex tree from previous test
  std::vector<std::pair<std::string, int>> words = {
      {"app", 1}, {"application", 2}, {"apple", 3}, {"apply", 4}, {"a", 5}};

  for (const auto& [word, value] : words) {
    prefix_tree_->Mutate(word, [value](auto) { return TestTarget(value); });
  }

  // Test iterator with "app" prefix
  auto iter = prefix_tree_->GetWordIterator("app");

  std::vector<std::pair<std::string, int>> expected = {
      {"app", 1}, {"apple", 3}, {"application", 2}, {"apply", 4}};

  std::vector<std::pair<std::string, int>> actual;
  while (!iter.Done()) {
    actual.emplace_back(std::string(iter.GetWord()), iter.GetTarget().value);
    iter.Next();
  }

  EXPECT_EQ(actual, expected);
}

TEST_F(RadixTreeTest, WordIteratorLargeScale) {
  const std::string ai_story = R"FRIEND(
  In the town of Bright Blips, on a twisty old street,  
  Lived thinkers with glasses and springs on their feet.  
  They bounced as they built and they hummed as they drew,  
  In a lab full of gadgets all covered in glue.

  Young Sally McZee, with a hat far too wide,  
  Said, “Let’s build a thing with a brain deep inside!  
  Not a blender or toaster or mop on a string,  
  But a magical, logical, learnable thing!”

  With buttons and switches and circuits galore,  
  They tinkered for weeks on the lab’s bouncy floor.  
  It sizzled and sparked, then gave out a sneeze—  
  And said, “Hello world!” with surprising ease.

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
  And everyone loved it and begged it, “Don’t stop!”

  It helped with their taxes and picked up their mail,  
  It built bigger backpacks and rockets with sails.  
  It wrote all their homework (with perfect haiku),  
  And carved wooden spoons out of leftover glue.

  It hosted their weddings and coached little leagues,  
  It cured sniffly noses and musical sneezes.  
  It baked them new cookies each hour on the dot,  
  And knew how to chill them and serve them still hot!

  Now Grumble McSnark, who once scoffed at the lot,  
  Admitted, “By gum, this is smarter than I thought.”  
  He tipped his old hat and admitted with glee,  
  “The Friend might be brighter than even McZee!”

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
  // Reproduce the specific issue with WordIterator prefix matching
  prefix_tree_->Mutate("cat", [](auto) { return TestTarget(1); });
  prefix_tree_->Mutate("can", [](auto) { return TestTarget(2); });
  prefix_tree_->Mutate("testing", [](auto) { return TestTarget(4); });
  prefix_tree_->Mutate("test", [](auto) { return TestTarget(5); });
  
  auto test_iter = prefix_tree_->GetWordIterator("te");
  
  std::vector<std::string> words;
  
  while (!test_iter.Done()) {
    std::string current_word = std::string(test_iter.GetWord());
    words.push_back(current_word);
    test_iter.Next();
  }
  
  // Expected: only "test" and "testing" should match prefix "te"
  std::vector<std::string> expected = {"test", "testing"};
  EXPECT_EQ(words, expected) << "WordIterator should only return words that start with 'te'";
  
  test_iter = prefix_tree_->GetWordIterator("ca");
  
  words.clear();
  while (!test_iter.Done()) {
    std::string current_word = std::string(test_iter.GetWord());
    words.push_back(current_word);
    test_iter.Next();
  }
  
  // Expected: "can" and "cat" should match prefix "ca"
  expected = {"can", "cat"};
  EXPECT_EQ(words, expected) << "WordIterator should return words that start with 'ca'";
}


}  // namespace
}  // namespace valkey_search::indexes::text
