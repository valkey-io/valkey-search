/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_UTILS_BLOOM_FILTER_H_
#define VALKEYSEARCH_SRC_UTILS_BLOOM_FILTER_H_

#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"

namespace valkey_search {
namespace utils {

// Simple Bloom Filter implementation.
class BloomFilter {
 public:
  // Default configuration constants.
  static constexpr size_t kDefaultNumBits = 1024;
  static constexpr size_t kDefaultNumHashes = 3;
  static constexpr size_t kMinNumBits = 64;
  static constexpr size_t kMinNumHashes = 1;
  static constexpr size_t kBitsPerWord = 64;
  static constexpr double kDefaultFalsePositiveRate = 0.01;

  explicit BloomFilter(size_t num_bits = kDefaultNumBits,
                       size_t num_hashes = kDefaultNumHashes)
      : num_bits_(std::max(num_bits, kMinNumBits)),
        num_hashes_(std::max(num_hashes, kMinNumHashes)),
        bits_((num_bits_ + kBitsPerWord - 1) / kBitsPerWord) {
    Clear();
  }

  // Creates an optimally-sized bloom filter for expected capacity and false
  // positive rate.
  static BloomFilter CreateOptimal(
      size_t expected_capacity,
      double false_positive_rate = kDefaultFalsePositiveRate) {
    if (expected_capacity == 0) {
      expected_capacity = 1;
    }
    if (false_positive_rate <= 0.0 || false_positive_rate >= 1.0) {
      false_positive_rate = kDefaultFalsePositiveRate;
    }

    // m = -n * ln(p) / (ln(2)^2)
    static constexpr double kLn2 = 0.693147180559945;
    static constexpr double kLn2Squared = kLn2 * kLn2;
    size_t num_bits =
        static_cast<size_t>(-static_cast<double>(expected_capacity) *
                            std::log(false_positive_rate) / kLn2Squared);

    // k = (m/n) * ln(2)
    size_t num_hashes = static_cast<size_t>(
        (static_cast<double>(num_bits) / expected_capacity) * kLn2);

    num_bits = std::max(num_bits, kMinNumBits);
    num_hashes = std::max(num_hashes, kMinNumHashes);

    return BloomFilter(num_bits, num_hashes);
  }

  BloomFilter(const BloomFilter&) = delete;
  BloomFilter& operator=(const BloomFilter&) = delete;
  BloomFilter(BloomFilter&&) = default;
  BloomFilter& operator=(BloomFilter&&) = default;

  // Adds an item to the filter.
  void Insert(absl::string_view item) {
    auto [h1, h2] = ComputeHashes(item);
    for (size_t i = 0; i < num_hashes_; ++i) {
      SetBit((h1 + i * h2) % num_bits_);
    }
  }

  // Returns true if the item might be in the set, false if definitely not.
  bool MayContain(absl::string_view item) const {
    auto [h1, h2] = ComputeHashes(item);
    for (size_t i = 0; i < num_hashes_; ++i) {
      if (!GetBit((h1 + i * h2) % num_bits_)) {
        return false;
      }
    }
    return true;
  }

  // Resets all bits to zero. NOT thread-safe.
  void Clear() {
    for (auto& word : bits_) {
      word.store(0, std::memory_order_relaxed);
    }
  }

  size_t NumBits() const { return num_bits_; }
  size_t NumHashes() const { return num_hashes_; }

 private:
  std::pair<uint64_t, uint64_t> ComputeHashes(absl::string_view item) const {
    uint64_t h1 = absl::HashOf(item);
    uint64_t h2 = absl::HashOf(item.size(), item);
    return {h1, h2};
  }

  void SetBit(size_t idx) {
    bits_[idx / kBitsPerWord].fetch_or(uint64_t{1} << (idx % kBitsPerWord),
                                       std::memory_order_relaxed);
  }

  bool GetBit(size_t idx) const {
    return (bits_[idx / kBitsPerWord].load(std::memory_order_relaxed) &
            (uint64_t{1} << (idx % kBitsPerWord))) != 0;
  }

  size_t num_bits_;
  size_t num_hashes_;
  std::vector<std::atomic<uint64_t>> bits_;
};

}  // namespace utils
}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_BLOOM_FILTER_H_