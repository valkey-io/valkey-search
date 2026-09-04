/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/rank_fusion.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_format.h"
#include "src/attribute_data_type.h"
#include "src/indexes/vector_base.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::query::rank_fusion {
namespace {

// Per-doc accumulator. external_id_ptr points at the InternedStringPtr from
// whichever arm first observed this doc — those pointers remain valid for the
// lifetime of the fusion call (the caller-owned per-arm neighbor vectors are
// not mutated).
struct FusedEntry {
  const indexes::Neighbor* representative = nullptr;  // for attribute_contents
  double score = 0.0;
  // Per-arm raw scores (Neighbor::score). nullopt means "doc absent from this
  // arm". (Using optional rather than a NaN sentinel because the build uses
  // -ffast-math, under which NaN comparisons are undefined.)
  std::vector<std::optional<double>> per_arm_score;
};

// Format a double the way the rest of the code formats reply doubles.
std::string FormatScore(double v) { return absl::StrFormat("%.12g", v); }

// Add a per-arm score to a fused neighbor's attribute_contents under the given
// alias. If the neighbor doesn't yet have an attribute_contents map, allocate
// one. The alias must outlive the fused neighbor; in practice it lives on
// MultiSearchParameters::per_arm_score_alias for the duration of the reply.
void AttachArmScore(indexes::Neighbor& n, const std::string& alias,
                    double value) {
  if (!n.attribute_contents.has_value()) {
    n.attribute_contents.emplace();
  }
  auto value_str = vmsdk::MakeUniqueValkeyString(FormatScore(value));
  auto identifier_str = vmsdk::MakeUniqueValkeyString(alias);
  // attribute_contents is keyed by string_view; the view must point at the
  // owned identifier_str's bytes for lifetime safety. The existing
  // RecordsMapValue stores the identifier internally; we use that as the key.
  auto identifier_view = vmsdk::ToStringView(identifier_str.get());
  n.attribute_contents->emplace(
      identifier_view,
      RecordsMapValue(std::move(identifier_str), std::move(value_str)));
}

// Build the merged neighbor list from the accumulator. Each fused neighbor
// carries the fused score in `Neighbor::score` (higher = better), which is what
// the reply path and the aggregate post-pipeline read. `distance` is set to the
// same value so anything still keyed on it stays consistent. Per-arm score
// aliases are added to attribute_contents.
std::vector<indexes::Neighbor> AssembleResult(
    const std::vector<ArmInput>& arms,
    absl::flat_hash_map<std::string, FusedEntry>& accum) {
  std::vector<indexes::Neighbor> out;
  out.reserve(accum.size());
  for (auto& [key, entry] : accum) {
    indexes::Neighbor n;
    n.external_id = entry.representative->external_id;
    n.score = static_cast<float>(entry.score);
    n.distance = n.score;
    // Carry over the representative's attribute_contents as the base. We
    // shallow-copy via move-fresh; if we want to merge from multiple arms in
    // the future, this is the place.
    if (entry.representative->attribute_contents.has_value()) {
      // Construct a fresh RecordsMap by emplacing copies of the keys/values;
      // RecordsMapValue holds a UniqueValkeyString, so we re-create those.
      n.attribute_contents.emplace();
      for (const auto& [identifier, value] :
           *entry.representative->attribute_contents) {
        auto id_str = vmsdk::MakeUniqueValkeyString(
            vmsdk::ToStringView(value.GetIdentifier()));
        auto val_str = vmsdk::MakeUniqueValkeyString(
            vmsdk::ToStringView(value.value.get()));
        auto id_view = vmsdk::ToStringView(id_str.get());
        n.attribute_contents->emplace(
            id_view, RecordsMapValue(std::move(id_str), std::move(val_str)));
      }
    }
    // Attach per-arm aliases.
    for (size_t i = 0; i < arms.size(); ++i) {
      if (!arms[i].score_alias.has_value()) {
        continue;
      }
      const auto& d = entry.per_arm_score[i];
      if (!d.has_value()) {
        // Doc was not present in this arm — omit the alias entry.
        continue;
      }
      AttachArmScore(n, *arms[i].score_alias, *d);
    }
    out.push_back(std::move(n));
  }
  // Stable order: descending fused score, tie-break by external_id for
  // determinism.
  std::sort(out.begin(), out.end(),
            [](const indexes::Neighbor& a, const indexes::Neighbor& b) {
              if (a.score != b.score) {
                return a.score > b.score;
              }
              return a.external_id->Str() < b.external_id->Str();
            });
  return out;
}

}  // namespace

std::vector<indexes::Neighbor> RRF(std::vector<ArmInput> arms) {
  absl::flat_hash_map<std::string, FusedEntry> accum;
  for (size_t arm_i = 0; arm_i < arms.size(); ++arm_i) {
    const auto& arm = arms[arm_i];
    if (arm.neighbors == nullptr) {
      continue;
    }
    const size_t cap =
        arm.window == 0
            ? arm.neighbors->size()
            : std::min(static_cast<size_t>(arm.window), arm.neighbors->size());
    for (size_t rank = 0; rank < cap; ++rank) {
      const auto& n = (*arm.neighbors)[rank];
      auto [it, inserted] = accum.try_emplace(n.external_id->Str());
      if (inserted) {
        it->second.representative = &n;
        it->second.per_arm_score.assign(arms.size(), std::nullopt);
      }
      it->second.score += 1.0 / (static_cast<double>(arm.rrf_constant) +
                                 static_cast<double>(rank + 1));
      it->second.per_arm_score[arm_i] = static_cast<double>(n.score);
    }
  }
  return AssembleResult(arms, accum);
}

std::vector<indexes::Neighbor> Linear(std::vector<ArmInput> arms) {
  // Weighted sum of the arms' raw scores: sum over arms of
  // `weight_i * score_i`, where a doc absent from an arm contributes 0 from
  // that arm.
  //
  // The scores are used as they stand — there is no per-arm normalization.
  // Normalizing would make a document's fused score depend on which *other*
  // documents happened to come back in the same arm, so the same document
  // against the same query would score differently as the corpus around it
  // changed. Callers balance the arms with ALPHA/BETA instead. Every arm
  // reaches here higher-is-better: a vector arm's distance was already turned
  // into a similarity by the caller (see ConvertVectorArmScoresToSimilarity in
  // ft_hybrid.cc).
  absl::flat_hash_map<std::string, FusedEntry> accum;
  for (size_t arm_i = 0; arm_i < arms.size(); ++arm_i) {
    const auto& arm = arms[arm_i];
    if (arm.neighbors == nullptr) {
      continue;
    }
    const size_t cap =
        arm.window == 0
            ? arm.neighbors->size()
            : std::min(static_cast<size_t>(arm.window), arm.neighbors->size());

    for (size_t rank = 0; rank < cap; ++rank) {
      const auto& n = (*arm.neighbors)[rank];
      auto [it, inserted] = accum.try_emplace(n.external_id->Str());
      if (inserted) {
        it->second.representative = &n;
        it->second.per_arm_score.assign(arms.size(), std::nullopt);
      }
      it->second.score += arm.weight * static_cast<double>(n.score);
      it->second.per_arm_score[arm_i] = static_cast<double>(n.score);
    }
  }
  return AssembleResult(arms, accum);
}

std::vector<indexes::Neighbor> Function(
    std::vector<ArmInput> arms,
    const std::function<double(const std::vector<std::optional<double>>&)>&
        score_fn) {
  // Pass 1: gather each document's per-arm raw scores across all arms.
  absl::flat_hash_map<std::string, FusedEntry> accum;
  for (size_t arm_i = 0; arm_i < arms.size(); ++arm_i) {
    const auto& arm = arms[arm_i];
    if (arm.neighbors == nullptr) {
      continue;
    }
    const size_t cap =
        arm.window == 0
            ? arm.neighbors->size()
            : std::min(static_cast<size_t>(arm.window), arm.neighbors->size());
    for (size_t rank = 0; rank < cap; ++rank) {
      const auto& n = (*arm.neighbors)[rank];
      auto [it, inserted] = accum.try_emplace(n.external_id->Str());
      if (inserted) {
        it->second.representative = &n;
        it->second.per_arm_score.assign(arms.size(), std::nullopt);
      }
      it->second.per_arm_score[arm_i] = static_cast<double>(n.score);
    }
  }
  // Pass 2: compute the combined score for each document via the user fn.
  for (auto& [key, entry] : accum) {
    entry.score = score_fn(entry.per_arm_score);
  }
  return AssembleResult(arms, accum);
}

}  // namespace valkey_search::query::rank_fusion
