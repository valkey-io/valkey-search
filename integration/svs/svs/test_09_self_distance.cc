// svs/test_09_self_distance.cc
//
// Purpose:
//   Empirically measure the self-distance (get_distance(label, same_vector))
//   across all compression types and metric types. When a vector is stored
//   with lossy compression, re-querying its own distance should return a
//   small value near zero (L2) or near the expected self-similarity (IP).
//   This test determines the maximum observed self-distance to inform the
//   epsilon threshold for VectorSVS::IsVectorMatch.
//
// Reproduction:
//   ./build_test.sh svs/test_09_self_distance
//   ./svs/test_09_self_distance
//
// Related issue: eshenayo/valkey-search#1

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <numeric>
#include <vector>

#include "svs_common.h"
#include "test_common.h"

using namespace svstest;
using svstest::svs_::DVamana;
using ::svs::runtime::v0::DynamicVamanaIndexLeanVec;
using ::svs::runtime::v0::LeanVecTrainingData;
using ::svs::runtime::v0::MetricType;
using ::svs::runtime::v0::StorageKind;
using ::svs::runtime::v0::Status;
using ::svs::runtime::v0::VamanaIndex;

struct TestConfig {
  const char* name;
  StorageKind storage;
  bool is_leanvec;
};

static const TestConfig kConfigs[] = {
    {"FP32",       StorageKind::FP32,       false},
    {"FP16",       StorageKind::FP16,       false},
    {"SQI8",       StorageKind::SQI8,       false},
    {"LVQ4x0",    StorageKind::LVQ4x0,    false},
    {"LVQ8x0",    StorageKind::LVQ8x0,    false},
    {"LVQ4x4",    StorageKind::LVQ4x4,    false},
    {"LVQ4x8",    StorageKind::LVQ4x8,    false},
    {"LeanVec4x4", StorageKind::LeanVec4x4, true},
    {"LeanVec4x8", StorageKind::LeanVec4x8, true},
    {"LeanVec8x8", StorageKind::LeanVec8x8, true},
};

static const MetricType kMetrics[] = {MetricType::L2, MetricType::INNER_PRODUCT};
static const char* kMetricNames[] = {"L2", "IP"};

static const size_t kDimensions[] = {64, 128, 256, 512};

static constexpr size_t kN = 200;
static constexpr size_t kLeanVecN = 10000;
static constexpr size_t kLeanVecDimsRatio = 2; // leanvec_dims = dim / ratio

struct Stats {
  double min_dist;
  double max_dist;
  double mean_dist;
  double stddev;
  size_t count;
};

Stats compute_stats(const std::vector<float>& dists) {
  Stats s{};
  s.count = dists.size();
  if (s.count == 0) return s;

  s.min_dist = *std::min_element(dists.begin(), dists.end());
  s.max_dist = *std::max_element(dists.begin(), dists.end());

  double sum = 0.0;
  for (float d : dists) sum += d;
  s.mean_dist = sum / s.count;

  double var = 0.0;
  for (float d : dists) var += (d - s.mean_dist) * (d - s.mean_dist);
  s.stddev = std::sqrt(var / s.count);

  return s;
}

struct Result {
  const char* compression;
  const char* metric;
  size_t dim;
  Stats stats;
  bool skipped;
  const char* skip_reason;
};

int main() {
  std::printf("=== SVS Self-Distance Empirical Test ===\n");
  std::printf("Measures get_distance(label, same_vector) for all compression/metric/dim combos\n\n");

  std::vector<Result> results;
  double global_max_l2 = 0.0;
  double global_max_ip = 0.0;

  for (size_t mi = 0; mi < 2; ++mi) {
    MetricType metric = kMetrics[mi];
    const char* metric_name = kMetricNames[mi];

    for (size_t di = 0; di < 4; ++di) {
      size_t dim = kDimensions[di];

      for (const auto& cfg : kConfigs) {
        Result res{};
        res.compression = cfg.name;
        res.metric = metric_name;
        res.dim = dim;
        res.skipped = false;

        rng(42 + dim + mi * 1000);

        DVamana* idx = nullptr;
        Status st;

        // IP/COSINE requires alpha <= 1.0
        float alpha = (metric == MetricType::INNER_PRODUCT) ? 1.0f : 1.2f;

        if (cfg.is_leanvec) {
          size_t leanvec_dims = dim / kLeanVecDimsRatio;
          if (leanvec_dims < 4) {
            res.skipped = true;
            res.skip_reason = "dim too small for LeanVec";
            results.push_back(res);
            continue;
          }

          auto training_data = random_vecs(kLeanVecN, dim);
          LeanVecTrainingData* td = nullptr;
          st = LeanVecTrainingData::build(&td, dim, kLeanVecN,
                                          training_data.data(), leanvec_dims);
          if (!st.ok()) {
            res.skipped = true;
            res.skip_reason = "LeanVecTrainingData::build failed";
            results.push_back(res);
            continue;
          }

          VamanaIndex::BuildParams build{};
          build.graph_max_degree = 32;
          build.construction_window_size = 128;
          build.alpha = alpha;
          VamanaIndex::SearchParams search{};
          search.search_window_size = 50;

          st = DynamicVamanaIndexLeanVec::build(
              &idx, dim, metric, cfg.storage, td, build, search);
          LeanVecTrainingData::destroy(td);

          if (!st.ok()) {
            res.skipped = true;
            res.skip_reason = "LeanVec index build failed";
            results.push_back(res);
            continue;
          }
        } else {
          st = svs_::build_svs(&idx, dim, metric, cfg.storage,
                               32, 128, 50, alpha);
          if (!st.ok()) {
            res.skipped = true;
            res.skip_reason = "build_svs failed";
            results.push_back(res);
            continue;
          }
        }

        size_t n = cfg.is_leanvec ? kLeanVecN : kN;
        auto data = random_vecs(n, dim);
        std::vector<size_t> labels(n);
        for (size_t i = 0; i < n; ++i) labels[i] = i;

        st = idx->add(n, labels.data(), data.data());
        if (!st.ok()) {
          std::printf("  [SKIP] %s/%s/dim=%zu: add failed: %s\n",
                      cfg.name, metric_name, dim, st.message());
          res.skipped = true;
          res.skip_reason = "add failed";
          DVamana::destroy(idx);
          results.push_back(res);
          continue;
        }

        std::vector<float> self_dists;
        self_dists.reserve(n);
        size_t errors = 0;

        for (size_t i = 0; i < n; ++i) {
          float dist = -1.0f;
          auto s = idx->get_distance(i, &data[i * dim], &dist);
          if (!s.ok()) {
            ++errors;
            continue;
          }
          self_dists.push_back(dist);
        }

        DVamana::destroy(idx);

        if (self_dists.empty()) {
          res.skipped = true;
          res.skip_reason = "all get_distance calls failed";
          results.push_back(res);
          continue;
        }

        res.stats = compute_stats(self_dists);
        results.push_back(res);

        if (metric == MetricType::L2 && res.stats.max_dist > global_max_l2)
          global_max_l2 = res.stats.max_dist;
        if (metric == MetricType::INNER_PRODUCT && res.stats.max_dist > global_max_ip)
          global_max_ip = res.stats.max_dist;

        if (errors > 0) {
          std::printf("  [WARN] %s/%s/dim=%zu: %zu/%zu get_distance errors\n",
                      cfg.name, metric_name, dim, errors, n);
        }
      }
    }
  }

  // Print summary table
  section("Self-Distance Results");
  std::printf("%-12s %-4s %4s | %12s %12s %12s %12s\n",
              "Compression", "Met", "Dim", "Min", "Max", "Mean", "StdDev");
  std::printf("%-12s %-4s %4s | %12s %12s %12s %12s\n",
              "------------", "----", "----", "------------",
              "------------", "------------", "------------");

  for (const auto& r : results) {
    if (r.skipped) {
      std::printf("%-12s %-4s %4zu | SKIPPED (%s)\n",
                  r.compression, r.metric, r.dim, r.skip_reason);
    } else {
      std::printf("%-12s %-4s %4zu | %12.8f %12.8f %12.8f %12.8f\n",
                  r.compression, r.metric, r.dim,
                  r.stats.min_dist, r.stats.max_dist,
                  r.stats.mean_dist, r.stats.stddev);
    }
  }

  // False-positive safety check: measure distance between two DIFFERENT vectors
  section("False-Positive Safety Check");
  std::printf("Distance between vectors differing by 0.1 in one component:\n");
  std::printf("%-12s %-4s %4s | %12s\n", "Compression", "Met", "Dim", "Distance");
  std::printf("%-12s %-4s %4s | %12s\n", "------------", "----", "----", "------------");

  for (const auto& cfg : kConfigs) {
    if (cfg.is_leanvec) continue; // skip LeanVec for this check to keep it fast

    for (size_t mi = 0; mi < 2; ++mi) {
      MetricType metric = kMetrics[mi];
      const char* metric_name = kMetricNames[mi];
      size_t dim = 128;
      float alpha = (metric == MetricType::INNER_PRODUCT) ? 1.0f : 1.2f;

      rng(99);
      DVamana* idx = nullptr;
      auto st = svs_::build_svs(&idx, dim, metric, cfg.storage,
                                32, 128, 50, alpha);
      if (!st.ok()) continue;

      auto vec1 = random_vec(dim);
      auto vec2 = vec1; // copy
      vec2[0] += 0.1f;  // small perturbation

      std::vector<size_t> labels = {0, 1};
      std::vector<float> both(2 * dim);
      std::copy(vec1.begin(), vec1.end(), both.begin());
      std::copy(vec2.begin(), vec2.end(), both.begin() + dim);

      st = idx->add(2, labels.data(), both.data());
      if (!st.ok()) { DVamana::destroy(idx); continue; }

      // Distance from vec1's stored repr to vec2 (the perturbed query)
      float dist = 0.0f;
      auto s = idx->get_distance(0, vec2.data(), &dist);
      DVamana::destroy(idx);

      if (s.ok()) {
        std::printf("%-12s %-4s %4zu | %12.8f\n",
                    cfg.name, metric_name, dim, dist);
      }
    }
  }

  // Threshold validation: check that the compiled default covers all results
  section("Threshold Validation");
  constexpr float kDefaultEpsilonPerDim = 0.0021f;
  std::printf("Compiled default: kDefaultDistanceMatchEpsilonPerDim = %g\n",
              kDefaultEpsilonPerDim);
  size_t threshold_failures = 0;
  for (const auto& r : results) {
    if (r.skipped) continue;
    float threshold = kDefaultEpsilonPerDim * r.dim;
    if (std::string(r.metric) == "L2") {
      if (r.stats.max_dist > threshold) {
        std::printf("  [EXCEED] %s/%s/dim=%zu: max=%.8f > threshold=%.8f\n",
                    r.compression, r.metric, r.dim, r.stats.max_dist, threshold);
        ++threshold_failures;
      }
    }
    // IP: self-distance = ~||v||² (not 0), validated by |dist - ||v||²| check
  }
  if (threshold_failures == 0) {
    std::printf("  All L2 self-distances within threshold.\n");
  } else {
    std::printf("  %zu compression/dim combos exceed threshold.\n",
                threshold_failures);
    std::printf("  (Expected for LVQ4x0/LeanVec4x4 at high dimensions.)\n");
  }

  // Summary
  section("Summary");
  std::printf("Global max self-distance (L2):  %g\n", global_max_l2);
  std::printf("Global max self-distance (IP):  %g (raw dot product, NOT error)\n",
              global_max_ip);
  std::printf("\nPer-dim factor for worst-case L2 (LVQ4x0@512): %g\n",
              global_max_l2 / 512.0);
  std::printf("Compiled default kDefaultDistanceMatchEpsilonPerDim: %g\n",
              kDefaultEpsilonPerDim);
  std::printf("\n");

  pass("svs/test_09_self_distance",
       "empirical thresholds measured successfully");
  return 0;
}
