/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: ************
 */

#include <algorithm>
#include <ctime>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "src/indexes/text/fuzzy.h"
#include "src/indexes/text/radix_tree.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::indexes::text {
namespace {

struct TestTarget {
  int value;
  TestTarget() : value(-1) {}
  explicit TestTarget(int v) : value(v) {}
  bool operator==(const TestTarget& other) const {
    return value == other.value;
  }

  explicit operator bool() const { return value != -1; }
};

class RadixTreeTest : public vmsdk::ValkeyTest {
 protected:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    prefix_tree_ = RadixTree<TestTarget>{};
  }

  void AddWords(const std::vector<std::pair<std::string, int>>& words) {
    for (const auto& [word, value] : words) {
      prefix_tree_.MutateTarget(word,
                                [value](auto) { return TestTarget(value); });
    }
  }

  void DeleteWords(const std::vector<std::string>& words) {
    for (const auto& word : words) {
      prefix_tree_.MutateTarget(word, [](auto) { return TestTarget{}; });
    }
  }

  void VerifyWords(const std::vector<std::pair<std::string, int>>& expected) {
    for (const auto& [word, value] : expected) {
      prefix_tree_.MutateTarget(word, [value, word](auto existing) {
        EXPECT_TRUE(static_cast<bool>(existing))
            << "Word '" << word << "' should exist";
        EXPECT_EQ(existing.value, value)
            << "Word '" << word << "' has wrong value";
        return existing;
      });
    }
  }

  void VerifyWordsDeleted(const std::vector<std::string>& words) {
    for (const auto& word : words) {
      prefix_tree_.MutateTarget(word, [&word](auto existing) {
        EXPECT_FALSE(static_cast<bool>(existing))
            << "Word '" << word << "' should be deleted";
        return existing;
      });
    }
  }

  void VerifyIterator(
      const std::string& prefix,
      const std::vector<std::pair<std::string, int>>& expected) {
    auto iter = prefix_tree_.GetWordIterator(prefix);
    std::vector<std::pair<std::string, int>> actual;
    while (!iter.Done()) {
      actual.emplace_back(std::string(iter.GetWord()), iter.GetTarget().value);
      iter.Next();
    }
    EXPECT_EQ(actual, expected)
        << "Iterator results don't match for prefix '" << prefix << "'";
  }

  void VerifyTreeStructure(const std::vector<std::string>& expected_structure) {
    auto actual_structure = prefix_tree_.DebugGetTreeStrings();

    if (actual_structure.size() != expected_structure.size()) {
      FAIL() << "Tree structure size mismatch.\n"
             << "Expected " << expected_structure.size() << " lines, got "
             << actual_structure.size() << " lines.\n"
             << "Expected structure:\n"
             << JoinLines(expected_structure) << "\n"
             << "Actual structure:\n"
             << JoinLines(actual_structure);
      return;
    }

    for (size_t i = 0; i < expected_structure.size(); ++i) {
      EXPECT_EQ(actual_structure[i], expected_structure[i])
          << "Tree structure mismatch at line " << i << ".\n"
          << "Expected structure:\n"
          << JoinLines(expected_structure) << "\n"
          << "Actual structure:\n"
          << JoinLines(actual_structure);
    }
  }

 protected:
  RadixTree<TestTarget> prefix_tree_;

 private:
  std::string JoinLines(const std::vector<std::string>& lines) {
    std::string result;
    for (size_t i = 0; i < lines.size(); ++i) {
      result += std::to_string(i) + ": " + lines[i] + "\n";
    }
    return result;
  }
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
  // clang-format off
  VerifyTreeStructure({
      "\"\" COMPRESSED",
      "└── \"ca\" BRANCH(3) [T]",
      "    ├── \"can\" LEAF [T]",
      "    ├── \"car\" LEAF [T]",
      "    └── \"cat\" LEAF [T]"
  });
  // clang-format on

  // Delete word at branching node. Nothing structurally changes but target is
  // removed.
  DeleteWords({"ca"});
  VerifyWords({{"cat", 1}, {"car", 2}, {"can", 3}});
  VerifyWordsDeleted({"ca"});
  // clang-format off
  VerifyTreeStructure({
      "\"\" COMPRESSED",
      "└── \"ca\" BRANCH(3)",
      "    ├── \"can\" LEAF [T]",
      "    ├── \"car\" LEAF [T]",
      "    └── \"cat\" LEAF [T]"
  });
  // clang-format on
}

TEST_F(RadixTreeTest, DeleteCompressedNodeWord) {
  // Case 1: Compressed parent - The parent (root) is a compressed node that
  // will point directly to "application" leaf node after "app" is deleted
  AddWords({{"app", 1}, {"application", 2}});
  DeleteWords({"app"});
  VerifyWords({{"application", 2}});
  VerifyWordsDeleted({"app"});
  // clang-format off
  VerifyTreeStructure({
      "\"\" COMPRESSED",
      "└── \"application\" LEAF [T]"
  });
  // clang-format on

  // Case 2: Branching parent - Tree structure doesn't change
  prefix_tree_ = RadixTree<TestTarget>{};
  AddWords({{"cat", 1}, {"car", 2}, {"cards", 3}});
  DeleteWords({"car"});
  VerifyWords({{"cat", 1}, {"cards", 3}});
  VerifyWordsDeleted({"car"});
  // clang-format off
  VerifyTreeStructure({
      "\"\" COMPRESSED",
      "└── \"ca\" BRANCH(2)",
      "    ├── \"car\" COMPRESSED",
      "    │   └── \"cards\" LEAF [T]",
      "    └── \"cat\" LEAF [T]"
  });
  // clang-format on
}

TEST_F(RadixTreeTest, DeleteLeafNodeWordSimpleScenarios) {
  // Case 1: Simple leaf deletion
  AddWords({{"hello", 1}});
  DeleteWords({"hello"});
  VerifyWordsDeleted({"hello"});
  VerifyTreeStructure({"\"\" LEAF"});

  // Case 2: Parent node with target gets turned into a leaf
  prefix_tree_ = RadixTree<TestTarget>{};
  AddWords({{"test", 1}, {"testing", 2}});
  DeleteWords({"testing"});
  VerifyWords({{"test", 1}});
  VerifyWordsDeleted({"testing"});
  // clang-format off
  VerifyTreeStructure({
      "\"\" COMPRESSED",
      "└── \"test\" LEAF [T]"
  });
  // clang-format on

  // Case 3: Leaf deletion where parent is branching with children.size() > 1
  prefix_tree_ = RadixTree<TestTarget>{};
  AddWords({{"cat", 1}, {"car", 2}, {"can", 3}});
  DeleteWords({"car"});
  VerifyWords({{"cat", 1}, {"can", 3}});
  VerifyWordsDeleted({"car"});
  // clang-format off
  VerifyTreeStructure({
      "\"\" COMPRESSED",
      "└── \"ca\" BRANCH(2)",
      "    ├── \"can\" LEAF [T]",
      "    └── \"cat\" LEAF [T]"
  });
  // clang-format on
}

TEST_F(RadixTreeTest, DeleteLeafNodeWordComplexScenarios) {
  // Test scenarios where a branch node gets converted to a compressed
  // node, causing compressed nodes to be merged

  // ==========================================================================
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
  //
  // Words: "xabc", "xtest"
  AddWords({{"xabc", 1}, {"xtest", 2}});
  // clang-format off
  VerifyTreeStructure({
      "\"\" COMPRESSED",
      "└── \"x\" BRANCH(2)",
      "    ├── \"xa\" COMPRESSED",
      "    │   └── \"xabc\" LEAF [T]",
      "    └── \"xt\" COMPRESSED",
      "        └── \"xtest\" LEAF [T]"
  });
  // clang-format on

  // Tree structure after deleting "xabc":
  //                  [compressed]
  //              "xtest" |
  //                   [leaf] -> Target
  DeleteWords({"xabc"});
  VerifyWords({{"xtest", 2}});
  VerifyWordsDeleted({"xabc"});
  // clang-format off
  VerifyTreeStructure({
      "\"\" COMPRESSED",
      "└── \"xtest\" LEAF [T]"
  });
  // clang-format on

  // Reset tree
  prefix_tree_ = RadixTree<TestTarget>{};

  // ==========================================================================
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
  // clang-format off
  VerifyTreeStructure({
      "\"\" COMPRESSED",
      "└── \"cat\" BRANCH(2)",
      "    ├── \"catc\" COMPRESSED",
      "    │   └── \"catcher\" LEAF [T]",
      "    └── \"cats\" LEAF [T]"
  });
  // clang-format on

  // The tree structure after deleting "catcher":
  //                  [compressed]
  //              "cats" |
  //                   [leaf] -> Target
  DeleteWords({"catcher"});
  VerifyWords({{"cats", 3}});
  VerifyWordsDeleted({"catcher"});
  // clang-format off
  VerifyTreeStructure({
      "\"\" COMPRESSED",
      "└── \"cats\" LEAF [T]"
  });
  // clang-format on

  // Reset tree
  prefix_tree_ = RadixTree<TestTarget>{};

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
  // clang-format off
  VerifyTreeStructure({
      "\"\" BRANCH(2)",
      "├── \"d\" COMPRESSED",
      "│   └── \"dog\" LEAF [T]",
      "└── \"r\" COMPRESSED",
      "    └── \"runner\" LEAF [T]"
  });
  // clang-format on

  // The tree structure after deleting "dog":
  //                  [compressed]
  //              "runner" |
  //                   [leaf] -> Target
  DeleteWords({"dog"});
  VerifyWords({{"runner", 6}});
  VerifyWordsDeleted({"dog"});
  // clang-format off
  VerifyTreeStructure({
      "\"\" COMPRESSED",
      "└── \"runner\" LEAF [T]"
  });
  // clang-format on

  // Reset tree
  prefix_tree_ = RadixTree<TestTarget>{};

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
  // clang-format off
  VerifyTreeStructure({
      "\"\" COMPRESSED",
      "└── \"x\" BRANCH(2) [T]",
      "    ├── \"xa\" COMPRESSED",
      "    │   └── \"xabc\" LEAF [T]",
      "    └── \"xt\" COMPRESSED",
      "        └── \"xtest\" LEAF [T]"
  });
  // clang-format on

  // Tree structure after deleting "xabc":
  //                  [compressed]
  //                   "x" |
  //                  [compressed] -> Target
  //                 test" |
  //                     [leaf] -> Target
  DeleteWords({"xabc"});
  VerifyWords({{"xtest", 2}, {"x", 3}});
  VerifyWordsDeleted({"xabc"});
  // clang-format off
  VerifyTreeStructure({
      "\"\" COMPRESSED",
      "└── \"x\" COMPRESSED [T]",
      "    └── \"xtest\" LEAF [T]"
  });
  // clang-format on
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
  auto iter = prefix_tree_.GetWordIterator("test");
  EXPECT_TRUE(iter.Done());
}

TEST_F(RadixTreeTest, WordIteratorNoMatch) {
  prefix_tree_.MutateTarget("hello", [](auto) { return TestTarget(1); });

  // Test iterator with non-matching prefix
  auto iter = prefix_tree_.GetWordIterator("world");
  EXPECT_TRUE(iter.Done());
}

TEST_F(RadixTreeTest, WordIteratorSingleWord) {
  prefix_tree_.MutateTarget("test", [](auto) { return TestTarget(42); });

  auto iter = prefix_tree_.GetWordIterator("test");
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
    prefix_tree_.MutateTarget(w, [](auto existing) {
      if (existing) {
        return TestTarget(existing.value + 1);
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

  // Randomly delete 100 words
  std::shuffle(
      words.begin(), words.end(),
      std::default_random_engine{static_cast<unsigned>(std::time(nullptr))});
  std::set<std::string> words_to_delete(words.begin(), words.begin() + 100);
  for (const auto& w : words_to_delete) {
    prefix_tree_.MutateTarget(w, [](auto) { return TestTarget{}; });
    word_counts.erase(w);
  }
  word_pairs = std::vector<std::pair<std::string, int>>(word_counts.begin(),
                                                        word_counts.end());
  VerifyIterator("", word_pairs);

  // Delete all words
  for (const auto& w : words) {
    prefix_tree_.MutateTarget(w, [](auto) { return TestTarget{}; });
  }
  // clang-format off
  VerifyTreeStructure({
      "\"\" LEAF"
  });
  // clang-format on
}

TEST_F(RadixTreeTest, WordIteratorPrefixPartialMatch) {
  // Test specific prefix matching edge case: cat/can/testing/test
  AddWords({{"cat", 1}, {"can", 2}, {"testing", 4}, {"test", 5}});

  // Test "te" prefix - should only match test/testing
  VerifyIterator("te", {{"test", 5}, {"testing", 4}});

  // Test "ca" prefix - should only match can/cat
  VerifyIterator("ca", {{"can", 2}, {"cat", 1}});
}

TEST_F(RadixTreeTest, PathIteratorBasic) {
  AddWords({{"cat", 1}, {"car", 2}, {"dog", 3}});

  // Test root iterator - root should have children
  auto iter = prefix_tree_.GetPathIterator("");
  EXPECT_FALSE(iter.Done());
  EXPECT_TRUE(iter.CanDescend());

  // Descend from root - should get first child
  auto child_iter = iter.DescendNew();
  EXPECT_FALSE(child_iter.Done());

  // Should be able to get path (edge label)
  auto path = child_iter.GetPath();
  EXPECT_FALSE(path.empty());
}

TEST_F(RadixTreeTest, PathIteratorSingleWord) {
  AddWords({{"application", 1}});
  prefix_tree_.DebugPrintTree("Single Word Tree");

  auto iter = prefix_tree_.GetPathIterator("");

  std::string word;
  while (!iter.Done()) {
    word += std::string(iter.GetPath());
    if (iter.CanDescend()) {
      iter = iter.DescendNew();
    } else {
      break;
    }
  }

  EXPECT_EQ(word, "application");
  EXPECT_TRUE(iter.IsWord());
}

TEST_F(RadixTreeTest, PathIteratorTraversal) {
  AddWords({{"cat", 1}, {"car", 2}, {"can", 3}});
  prefix_tree_.DebugPrintTree("Traversal Test");

  auto iter = prefix_tree_.GetPathIterator("");

  std::vector<std::string> found_words;
  std::function<void(decltype(iter), std::string)> traverse;
  traverse = [&](auto it, std::string accumulated) {
    while (!it.Done()) {
      std::string word = accumulated + std::string(it.GetPath());
      if (it.IsWord()) {
        found_words.push_back(word);
      }
      if (it.CanDescend()) {
        traverse(it.DescendNew(), word);
      }
      it.Next();
    }
  };

  traverse(iter, "");

  EXPECT_EQ(found_words.size(), 3);
  std::sort(found_words.begin(), found_words.end());
  EXPECT_EQ(found_words[0], "can");
  EXPECT_EQ(found_words[1], "car");
  EXPECT_EQ(found_words[2], "cat");
}

}  // namespace
}  // namespace valkey_search::indexes::text
