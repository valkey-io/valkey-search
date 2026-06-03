/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/slop_calculator.h"

#include <cstdint>
#include <vector>

#include "gtest/gtest.h"

namespace valkey_search::indexes::scoring {
namespace {

// Positions in the test document:
//   apple red blue banana yellow green grape purple orange cherry pink
//   violet one two three four five six seven eight nine ten zero ...
// (single-char tokens are dropped, so only the words below have positions)
//   apple=0 red=1 blue=2 banana=3 yellow=4 green=5 grape=6 purple=7
//   orange=8 cherry=9 pink=10 violet=11 one=12 two=13 three=14 four=15
//   five=16 six=17

using Pos = std::vector<SlopPosition>;

// Drives a calculator with a single bare outermost term.
void Term(SlopCalculator& calc, const Pos& positions) {
  calc.OnTerm(positions);
}

// Case 1: single node -> guard returns 1.
TEST(SlopCalculatorTest, SingleTerm) {
  SlopCalculator calc;
  Term(calc, {0});  // apple
  EXPECT_EQ(calc.Finalize(), 1u);
}

// Case 2: flat AND, basic gap. {0},{3} -> gap 3 -> 3.
TEST(SlopCalculatorTest, FlatAndTwoTerms) {
  SlopCalculator calc;
  Term(calc, {0});  // apple
  Term(calc, {3});  // banana
  EXPECT_EQ(calc.Finalize(), 3u);
}

// Case 3: flat AND, 3 gaps. {0},{7},{12} -> 7,5 -> floor(sqrt(74))=8.
TEST(SlopCalculatorTest, FlatAndThreeTerms) {
  SlopCalculator calc;
  Term(calc, {0});   // apple
  Term(calc, {7});   // purple
  Term(calc, {12});  // one
  EXPECT_EQ(calc.Finalize(), 8u);
}

// Case 4: nested AND group collapses to union; MinGap picks closest member.
// apple red (blue orange three) five six
// {0},{1},{2,8,14},{16},{17} -> 1,1,2,1 -> floor(sqrt(7))=2.
TEST(SlopCalculatorTest, NestedAndGroupUnion) {
  SlopCalculator calc;
  Term(calc, {0});  // apple
  Term(calc, {1});  // red
  calc.EnterGroup();
  Term(calc, {2});   // blue
  Term(calc, {8});   // orange
  Term(calc, {14});  // three
  calc.ExitGroup();
  Term(calc, {16});  // five
  Term(calc, {17});  // six
  EXPECT_EQ(calc.Finalize(), 2u);
}

// Case 5: nested OR group collapses to union.
// apple (banana | green grape) purple
// {0},{3,5,6},{7} -> 3,1 -> floor(sqrt(10))=3.
TEST(SlopCalculatorTest, NestedOrGroupUnion) {
  SlopCalculator calc;
  Term(calc, {0});  // apple
  calc.EnterGroup();
  Term(calc, {3});  // banana
  Term(calc, {5});  // green
  Term(calc, {6});  // grape
  calc.ExitGroup();
  Term(calc, {7});  // purple
  EXPECT_EQ(calc.Finalize(), 3u);
}

// Case 6: flat OR root unwrapped -> distance across branches (matches Redis).
// apple | blue | green : {0},{2},{5} -> 2,3 -> floor(sqrt(13))=3.
TEST(SlopCalculatorTest, FlatOrRootUnwrapped) {
  SlopCalculator calc;
  Term(calc, {0});  // apple
  Term(calc, {2});  // blue
  Term(calc, {5});  // green
  EXPECT_EQ(calc.Finalize(), 3u);
}

// Case 7: query order preserved (no OR reordering).
// green | apple | blue : {5},{0},{2} -> 5,2 -> floor(sqrt(29))=5.
TEST(SlopCalculatorTest, QueryOrderPreserved) {
  SlopCalculator calc;
  Term(calc, {5});  // green
  Term(calc, {0});  // apple
  Term(calc, {2});  // blue
  EXPECT_EQ(calc.Finalize(), 5u);
}

// Case 8: OR groups stay in query order (intentional divergence from Redis).
// (banana|grape|purple) (cherry|violet|two) apple blue
// {3,6,7},{9,11,13},{0},{2} -> 2,9,2 -> floor(sqrt(89))=9.
TEST(SlopCalculatorTest, OrGroupsInQueryOrder) {
  SlopCalculator calc;
  calc.EnterGroup();
  Term(calc, {3});  // banana
  Term(calc, {6});  // grape
  Term(calc, {7});  // purple
  calc.ExitGroup();
  calc.EnterGroup();
  Term(calc, {9});   // cherry
  Term(calc, {11});  // violet
  Term(calc, {13});  // two
  calc.ExitGroup();
  Term(calc, {0});  // apple
  Term(calc, {2});  // blue
  EXPECT_EQ(calc.Finalize(), 9u);
}

// Case 9: repeated term shares a position -> inner gap 0, no special rule.
// apple apple blue yellow : {0},{0},{2},{4} -> 0,2,2 -> floor(sqrt(8))=2.
TEST(SlopCalculatorTest, RepeatedTermInnerGapZero) {
  SlopCalculator calc;
  Term(calc, {0});  // apple
  Term(calc, {0});  // apple
  Term(calc, {2});  // blue
  Term(calc, {4});  // yellow
  EXPECT_EQ(calc.Finalize(), 2u);
}

// Case 10: gap 0 -> final min-1 guard kicks in.
// red red, red at {0,6} : {0,6},{0,6} -> MinGap 0 -> guard -> 1.
TEST(SlopCalculatorTest, RepeatedTermGuardToOne) {
  SlopCalculator calc;
  Term(calc, {0, 6});  // red
  Term(calc, {0, 6});  // red
  EXPECT_EQ(calc.Finalize(), 1u);
}

// Case 11: absent OR branch contributes no anchor.
// zzz | apple | blue : zzz absent -> {0},{2} -> gap 2 -> 2.
TEST(SlopCalculatorTest, AbsentOrBranchDropped) {
  SlopCalculator calc;
  Term(calc, {});   // zzz (absent)
  Term(calc, {0});  // apple
  Term(calc, {2});  // blue
  EXPECT_EQ(calc.Finalize(), 2u);
}

TEST(SlopCalculatorDeathTest, ExitWithoutEnterCrashes) {
  SlopCalculator calc;
  EXPECT_DEATH(calc.ExitGroup(), "");
}

TEST(SlopCalculatorDeathTest, FinalizeWithOpenGroupCrashes) {
  SlopCalculator calc;
  calc.EnterGroup();
  EXPECT_DEATH((void)calc.Finalize(), "");
}

}  // namespace
}  // namespace valkey_search::indexes::scoring
