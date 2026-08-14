/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/scoring/scorer.h"

#include <cstdint>
#include <cstring>

#include "absl/log/check.h"
#include "src/indexes/scoring/bm25std_scorer.h"

namespace valkey_search::indexes::scoring {

// Hand-rolled inf check via the raw IEEE-754 bits: the build uses -ffast-math
// (implies -ffinite-math-only), under which the compiler assumes no inf/NaN and
// folds std::isinf to a constant false. Inspecting the bit pattern is immune to
// that assumption.
bool IsInf(float f) {
  static constexpr uint32_t kExponentMask = 0x7F800000U;
  static constexpr uint32_t kMantissaMask = 0x007FFFFFU;
  uint32_t bits;
  std::memcpy(&bits, &f, sizeof(bits));
  return (bits & kExponentMask) == kExponentMask && (bits & kMantissaMask) == 0;
}

const Scorer* GetScorer(ScorerType type) {
  static const Bm25StdScorer kBm25Std;
  switch (type) {
    case ScorerType::kBm25Std:
      return &kBm25Std;
    case ScorerType::kTfidf:
      CHECK(false) << "TFIDF scorer not yet implemented";
      return nullptr;
  }
  CHECK(false) << "Unknown scorer type";
  return nullptr;
}

}  // namespace valkey_search::indexes::scoring
