// test_common.h — shared helpers for BOTH hnsw/ and svs/ tests.
// No SVS- or hnswlib-specific types here (kept out of headers so this
// file compiles in both toolchains).

#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <vector>

namespace svstest {

using clock_t_ = std::chrono::steady_clock;

inline double ms_since(clock_t_::time_point t0) {
  auto dt = clock_t_::now() - t0;
  return std::chrono::duration<double, std::milli>(dt).count();
}

// Deterministic Mersenne Twister so a given (seed, N, dim) triple
// produces the same vectors across HNSW and SVS runs. Use the same
// seed in matching hnsw/test_0X and svs/test_0X so numbers line up.
inline std::mt19937& rng(uint64_t seed = 0) {
  thread_local std::mt19937 r(0);
  if (seed != 0) r.seed(seed);
  return r;
}

inline std::vector<float> random_vec(size_t dim) {
  std::vector<float> v(dim);
  std::uniform_real_distribution<float> d(-1.0f, 1.0f);
  auto& r = rng();
  for (auto& x : v) x = d(r);
  return v;
}

inline std::vector<float> random_vecs(size_t n, size_t dim) {
  std::vector<float> out(n * dim);
  std::uniform_real_distribution<float> d(-1.0f, 1.0f);
  auto& r = rng();
  for (auto& x : out) x = d(r);
  return out;
}

inline void section(const char* name) {
  std::printf("\n--- %s ---\n", name);
}

inline void pass(const char* name, const std::string& detail = {}) {
  if (detail.empty())
    std::printf("[PASS] %s\n", name);
  else
    std::printf("[PASS] %s (%s)\n", name, detail.c_str());
}

inline void fail(const char* name, const std::string& detail) {
  std::printf("[FAIL] %s: %s\n", name, detail.c_str());
}

}  // namespace svstest
